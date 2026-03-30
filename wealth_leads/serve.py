from __future__ import annotations

import base64
import csv
import html
import io
import json
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
import webbrowser
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, quote, unquote, urlencode, urljoin
from wsgiref.simple_server import make_server

from wealth_leads.config import (
    database_path,
    lead_desk_equity_only_min_usd,
    lead_desk_min_signal_usd,
    lead_desk_s1_only,
    lead_desk_us_registrant_hq_only,
)
from wealth_leads.advisor_pack import get_issuer_snapshot_dict
from wealth_leads.db import connect, get_lead_client_research
from wealth_leads.lead_research import row_to_client_research_dict
from wealth_leads.management import issuer_summary_looks_spammy, why_surfaced_line
from wealth_leads.profile_build import _effective_other_comp
from wealth_leads.management_bios import extract_age_from_bio_text
from wealth_leads.person_quality import (
    is_acceptable_lead_person_name,
    refine_lead_person_name,
)
from wealth_leads.territory import (
    hq_city_state_display,
    hq_city_state_looks_like_filing_noise,
    hq_principal_office_display_line,
    is_plausible_registrant_headquarters,
    registrant_hq_line_parses_as_united_states,
)
from wealth_leads.title_badge import advisor_title_badge

_SERVE_CHILD_ENV = "WEALTH_LEADS_SERVE_CHILD"
# New value each process start so the tab reloads after you restart the server (picks up code changes).
_DEV_BOOT = f"{time.time_ns()}-{os.getpid()}"


def _want_live_reload() -> bool:
    return os.environ.get("WEALTH_LEADS_LIVE_RELOAD", "1").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def _live_reload_snippet() -> str:
    if not _want_live_reload():
        return ""
    return """
<script>
(function(){
  var u='/__dev/state';
  function tick(){
    fetch(u,{cache:'no-store'}).then(function(r){return r.json()}).then(function(j){
      var x=String(j.db)+'|'+String(j.boot);
      if(window.__wlPulse===undefined){window.__wlPulse=x;return;}
      if(window.__wlPulse!==x)location.reload();
    }).catch(function(){});
  }
  setInterval(tick,1200);
  tick();
})();
</script>"""


def _dev_state_body() -> bytes:
    db_m = 0.0
    dbp = database_path()
    try:
        p = Path(dbp)
        if p.is_file():
            db_m = p.stat().st_mtime
    except OSError:
        pass
    payload = json.dumps({"db": db_m, "boot": _DEV_BOOT}, separators=(",", ":"))
    return payload.encode("utf-8")


def _package_py_snapshot(pkg: Path) -> dict[str, float]:
    out: dict[str, float] = {}
    if not pkg.is_dir():
        return out
    for p in pkg.rglob("*.py"):
        if p.is_file():
            try:
                out[str(p.resolve())] = p.stat().st_mtime
            except OSError:
                continue
    return out


def _spawn_reload_watch_loop(*, port: int, open_browser: bool, live: bool) -> None:
    pkg = Path(__file__).resolve().parent
    first_child = True
    print(
        "Dev reload: watching wealth_leads/*.py — server restarts on save; "
        "browser auto-refreshes when the DB or process changes.",
        file=sys.stderr,
    )
    try:
        while True:
            snap = _package_py_snapshot(pkg)
            env = os.environ.copy()
            env[_SERVE_CHILD_ENV] = "1"
            env["WEALTH_LEADS_LIVE_RELOAD"] = "1" if live else "0"
            cmd = [
                sys.executable,
                "-m",
                "wealth_leads",
                "serve",
                "--port",
                str(port),
            ]
            if not open_browser or not first_child:
                cmd.append("--no-browser")
            if not live:
                cmd.append("--no-live")
            proc = subprocess.Popen(cmd, env=env)
            first_child = False
            while proc.poll() is None:
                time.sleep(0.45)
                if _package_py_snapshot(pkg) != snap:
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                    time.sleep(0.15)
                    break
            else:
                code = proc.returncode or 0
                if code == 0:
                    return
                print(
                    f"[serve] server process exited ({code}); restarting…",
                    file=sys.stderr,
                )
                time.sleep(0.5)
                continue
    except KeyboardInterrupt:
        print("\nStopped (reload watcher).", file=sys.stderr)


def _norm_person_name(name: str) -> str:
    s = (name or "").lower().replace(".", " ")
    return " ".join(s.split())


def _is_s1_form_type(form_type: str) -> bool:
    ft = (form_type or "").strip().upper().replace(" ", "")
    return ft.startswith("S-1")


def _row_equity_usd(row: dict) -> float:
    """Grant-style equity from SCT: stock + options, or stored equity sum if columns absent."""
    s, o, ec = row.get("stock_awards"), row.get("option_awards"), row.get("equity_comp_disclosed")
    if s is not None or o is not None:
        return float(s or 0) + float(o or 0)
    if ec is not None:
        return float(ec)
    return 0.0


_NAME_SUFFIXES = frozenset({"jr", "sr", "ii", "iii", "iv", "v"})


def _first_last_name_parts(norm: str) -> tuple[str, str]:
    """First + last tokens for loose matching (handles middle initials vs roster without them)."""
    parts = [p for p in (norm or "").split() if p]
    while len(parts) >= 2 and parts[-1].rstrip(".").lower() in _NAME_SUFFIXES:
        parts = parts[:-1]
    if not parts:
        return ("", "")
    if len(parts) == 1:
        return (parts[0], parts[0])
    return (parts[0], parts[-1])


def _officer_name_match_tier(person_norm: str, officer_norm: str) -> int:
    """0 = exact normalized match, 1 = same first+last, -1 = no match."""
    if not person_norm or not officer_norm:
        return -1
    if person_norm == officer_norm:
        return 0
    pf, pl = _first_last_name_parts(person_norm)
    of_, ol = _first_last_name_parts(officer_norm)
    if pf and pl and pf == of_ and pl == ol:
        return 1
    return -1


def _filing_date_sort_key(fd: str) -> int:
    try:
        s = (fd or "").replace("-", "")[:8]
        return int(s) if s.isdigit() else 0
    except (TypeError, ValueError):
        return 0


def _parse_filing_date(s: str) -> Optional[date]:
    if not (s or "").strip():
        return None
    raw = s.strip()[:10]
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        return None


def _whole_calendar_years_elapsed(since: date, until: date) -> int:
    """Birthday-unknown approximation: count full calendar years between dates."""
    y = until.year - since.year
    if (until.month, until.day) < (since.month, since.day):
        y -= 1
    return max(0, y)


def _age_estimated_for_today(
    age_stated: Optional[int], anchor_date_str: str
) -> tuple[Optional[int], bool]:
    """
    Returns (age to display today, whether we applied a calendar-year adjustment).
    Without a parseable anchor date, returns (age_stated, False).
    """
    if age_stated is None:
        return None, False
    anchor = _parse_filing_date(anchor_date_str)
    if anchor is None:
        return age_stated, False
    extra = _whole_calendar_years_elapsed(anchor, date.today())
    return age_stated + extra, extra > 0


def _resolve_officer_extras_for_person(
    rows_for_cik: list[tuple[str, str, Optional[int], int, str]],
    *,
    pref_filing_id: int,
    person_norm: str,
) -> tuple[str, Optional[int], str]:
    """
    Match NEO person_name to officers for the same CIK.
    Returns (title, age as in filing, filing_date of that officer row).
    """
    if not person_norm or not rows_for_cik:
        return "", None, ""
    best_key: tuple[int, int, int, int, int] | None = None
    best_title = ""
    best_age: Optional[int] = None
    best_fdate = ""
    for onorm, tit, oage, fid, fdate in rows_for_cik:
        tier = _officer_name_match_tier(person_norm, onorm)
        if tier < 0:
            continue
        t = (tit or "").strip()
        if not t:
            continue
        same_f = 0 if fid == pref_filing_id else 1
        dk = _filing_date_sort_key(fdate)
        # Prefer a row with a parsed age over the headline filing row with no age
        # (common when an amended S-1 omits the age column but the prior filing had it).
        age_pref = 0 if oage is not None else 1
        cand_key = (tier, age_pref, same_f, -dk, -len(t))
        if best_key is None or cand_key < best_key:
            best_key = cand_key
            best_title = t
            best_age = oage
            best_fdate = (fdate or "").strip()
    if best_key is None:
        return "", None, ""
    return best_title, best_age, best_fdate


def _profile_key(cik: str, person_name: str) -> tuple[str, str]:
    return (str(cik or "").strip(), _norm_person_name(person_name))


def _money(v: object) -> str:
    if v is None:
        return "—"
    try:
        x = float(v)
        if x >= 1_000_000:
            return f"${x:,.0f}"
        return f"${x:,.0f}"
    except (TypeError, ValueError):
        return "—"


def _try_float(v: object) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _equity_awards_sum_profile(p: dict) -> Optional[float]:
    sa, oa = p.get("stock_awards"), p.get("option_awards")
    if sa is None and oa is None:
        return None
    try:
        return float(sa or 0) + float(oa or 0)
    except (TypeError, ValueError):
        return None


def _cash_ex_equity_awards(p: dict) -> Optional[float]:
    """
    Disclosure bundle for advisors: SCT total minus stock + option _award_ columns when both exist;
    otherwise sum salary, bonus, non-equity, pension, and effective other.
    """
    eq = _equity_awards_sum_profile(p)
    tot = _try_float(p.get("total"))
    if tot is not None and eq is not None:
        return max(0.0, tot - eq)
    s = 0.0
    n = 0
    for k in ("salary", "bonus", "non_equity_incentive", "pension_change"):
        v = _try_float(p.get(k))
        if v is not None:
            s += v
            n += 1
    oth = _effective_other_comp(p)
    if oth is not None:
        try:
            s += float(oth)
            n += 1
        except (TypeError, ValueError):
            pass
    return s if n > 0 else None


def _profile_lead_tier(p: dict) -> str:
    """
    premium — summary comp on file and meets desk pay bar (or bar is 0).
    standard — summary comp but below desk pay bar (e.g. director SCT).
    visibility — S-1 officer/director table only; no NEO/SCT rows in DB.
    """
    if p.get("has_summary_comp"):
        min_bar = lead_desk_min_signal_usd()
        equity_only = lead_desk_equity_only_min_usd()
        raw_v = p.get("equity_hwm") if equity_only else p.get("signal_hwm")
        try:
            v = float(raw_v or 0)
        except (TypeError, ValueError):
            v = 0.0
        if min_bar <= 0 or v >= min_bar:
            return "premium"
        return "standard"
    return "visibility"


def _annotate_lead_tier_fields(p: dict) -> None:
    tier = _profile_lead_tier(p)
    p["lead_tier"] = tier
    base = (p.get("why_surfaced") or "").strip()
    if tier == "premium":
        return
    if tier == "standard":
        suffix = (
            " — Not premium: disclosed S-1 summary comp is below the desk pay-signal bar; "
            "still useful for smaller RIAs / lower minimums."
        )
    else:
        suffix = (
            " — Not premium: S-1 officer/director listing only (no summary comp row in DB); "
            "visibility / referral-tier prospect."
        )
    p["why_surfaced"] = (base + suffix).strip()


def _desk_sort_tuple(p: dict) -> tuple:
    tier = p.get("lead_tier") or _profile_lead_tier(p)
    tr = {"premium": 2, "standard": 1, "visibility": 0}.get(tier, 0)
    fy = p.get("headline_year")
    try:
        fy_i = int(fy) if fy is not None else 0
    except (TypeError, ValueError):
        fy_i = 0
    try:
        tot = float(p.get("total") or 0)
    except (TypeError, ValueError):
        tot = 0.0
    return (
        tr,
        float(p.get("signal_hwm") or 0),
        _filing_date_sort_key(p.get("filing_date") or ""),
        fy_i,
        tot,
    )


def _resolve_issuer_revenue_for_cik(
    conn: sqlite3.Connection,
    cik: str,
    *,
    head_revenue: str = "",
) -> str:
    """Prefer headline filing row; else latest non-empty revenue line for CIK."""
    ck = (cik or "").strip()
    rev = (head_revenue or "").strip()
    if not ck:
        return rev
    if rev:
        return rev
    r = conn.execute(
        """
        SELECT issuer_revenue_text FROM filings
        WHERE cik = ? AND issuer_revenue_text IS NOT NULL
          AND TRIM(issuer_revenue_text) != ''
        ORDER BY COALESCE(filing_date, '') DESC, id DESC
        LIMIT 1
        """,
        (ck,),
    ).fetchone()
    return ((r[0] or "").strip()) if r else ""


def _roster_individual_dedupe_key(display_name: str) -> str:
    """
    Merge variants like "Matthew K. Morrow" vs "Matthew Morrow" for counts only.
    Uses first + last token of normalized name (same basis as officer matching elsewhere).
    """
    n = _norm_person_name(display_name)
    if not n:
        return ""
    first, last = _first_last_name_parts(n)
    if first and last and first != last:
        return f"{first}|{last}"
    return n


def management_roster_scale_stats(
    conn: sqlite3.Connection, cik: str
) -> Optional[dict[str, int]]:
    """
    Officer/director rows for CIK on S-1 filings: raw disclosure lines vs people after
    first+last dedupe (handles middle initials and spelling variants in the table).
    """
    ck = (cik or "").strip()
    if not ck:
        return None
    cur = conn.execute(
        """
        SELECT o.name
        FROM officers o
        JOIN filings f ON f.id = o.filing_id
        WHERE f.cik = ? AND UPPER(COALESCE(f.form_type, '')) LIKE 'S-1%'
          AND LENGTH(TRIM(COALESCE(o.name, ''))) > 0
          AND LENGTH(TRIM(COALESCE(o.title, ''))) > 0
        ORDER BY COALESCE(f.filing_date, '') DESC, f.id DESC
        """,
        (ck,),
    )
    raw_rows = 0
    keys: set[str] = set()
    for r in cur.fetchall():
        raw_nm = (r["name"] or "").strip()
        display_nm = raw_nm
        if not is_acceptable_lead_person_name(display_nm):
            rfn = refine_lead_person_name(raw_nm)
            if not rfn:
                continue
            display_nm = rfn
        dk = _roster_individual_dedupe_key(display_nm)
        if not dk:
            continue
        raw_rows += 1
        keys.add(dk)
    if raw_rows == 0:
        return None
    return {"raw_rows": raw_rows, "unique_people": len(keys)}


def _fetch_s1_officer_join_rows(conn: sqlite3.Connection) -> list[dict]:
    cur = conn.execute(
        """
        SELECT o.name AS person_name, o.title, o.age, o.filing_id,
               f.company_name, f.cik, f.filing_date, f.index_url, f.primary_doc_url,
               f.form_type AS filing_form_type,
               f.issuer_summary AS filing_issuer_summary,
               f.issuer_website AS filing_issuer_website,
               f.issuer_headquarters AS filing_issuer_headquarters,
               f.issuer_industry AS filing_issuer_industry,
               f.issuer_revenue_text AS filing_issuer_revenue_text,
               f.director_term_summary AS filing_director_term_summary
        FROM officers o
        JOIN filings f ON f.id = o.filing_id
        WHERE LENGTH(TRIM(COALESCE(o.title, ''))) > 0
        """
    )
    out: list[dict] = []
    for r in cur.fetchall():
        d = dict(r)
        if not _is_s1_form_type(str(d.get("filing_form_type") or "")):
            continue
        nm = (d.get("person_name") or "").strip()
        if not is_acceptable_lead_person_name(nm):
            rfn = refine_lead_person_name(nm)
            if rfn:
                d["person_name"] = rfn
            else:
                continue
        out.append(d)
    return out


def _visibility_profile_dict(
    conn: sqlite3.Connection,
    head: dict,
    officer_rows_by_cik: dict[str, list[tuple[str, str, Optional[int], int, str]]],
    narr_map: dict[tuple[int, str], dict],
) -> dict:
    pn = _norm_person_name(head["person_name"] or "")
    fid = int(head["filing_id"])
    cik_s = str(head["cik"] or "").strip()
    off_t, officer_age, officer_age_filing_date = _resolve_officer_extras_for_person(
        officer_rows_by_cik.get(cik_s, []),
        pref_filing_id=fid,
        person_norm=pn,
    )
    title_guess = (head.get("title") or "").strip() or off_t or "—"

    iss_raw = (head.get("filing_issuer_summary") or "").strip()
    if iss_raw and issuer_summary_looks_spammy(iss_raw):
        iss_raw = ""
    if not iss_raw:
        alt_rows = conn.execute(
            """
            SELECT issuer_summary FROM filings
            WHERE cik = ? AND issuer_summary IS NOT NULL
              AND TRIM(issuer_summary) != ''
            ORDER BY COALESCE(filing_date, '') DESC, id DESC
            LIMIT 25
            """,
            (cik_s,),
        ).fetchall()
        for row in alt_rows:
            cand = (row[0] or "").strip()
            if cand and not issuer_summary_looks_spammy(cand):
                iss_raw = cand
                break

    issuer_web = (head.get("filing_issuer_website") or "").strip()
    issuer_hq = _resolve_issuer_headquarters_for_profile(
        conn, cik_s, (head.get("filing_issuer_headquarters") or "").strip()
    )
    if not issuer_web and cik_s:
        rw = conn.execute(
            """
            SELECT issuer_website FROM filings
            WHERE cik = ? AND issuer_website IS NOT NULL
              AND TRIM(issuer_website) != ''
            ORDER BY COALESCE(filing_date, '') DESC LIMIT 1
            """,
            (cik_s,),
        ).fetchone()
        issuer_web = (rw[0] or "").strip() if rw else ""

    issuer_ind = (head.get("filing_issuer_industry") or "").strip()
    if not issuer_ind and cik_s:
        rind = conn.execute(
            """
            SELECT issuer_industry FROM filings
            WHERE cik = ? AND issuer_industry IS NOT NULL
              AND TRIM(issuer_industry) != ''
            ORDER BY COALESCE(filing_date, '') DESC LIMIT 1
            """,
            (cik_s,),
        ).fetchone()
        issuer_ind = (rind[0] or "").strip() if rind else ""

    rev_txt = _resolve_issuer_revenue_for_cik(
        conn,
        cik_s,
        head_revenue=str(head.get("filing_issuer_revenue_text") or ""),
    )

    mgmt_nar = narr_map.get((fid, pn))
    if not mgmt_nar and cik_s:
        altn = conn.execute(
            """
            SELECT m.person_name, m.role_heading, m.bio_text
            FROM person_management_narrative m
            JOIN filings f ON f.id = m.filing_id
            WHERE f.cik = ? AND m.person_name_norm = ?
            ORDER BY COALESCE(f.filing_date, '') DESC, f.id DESC
            LIMIT 1
            """,
            (cik_s, pn),
        ).fetchone()
        mgmt_nar = (
            {
                "person_name": altn["person_name"],
                "role_heading": altn["role_heading"],
                "bio_text": altn["bio_text"],
            }
            if altn
            else None
        )

    dts = (head.get("filing_director_term_summary") or "").strip()
    if not dts and cik_s:
        rd = conn.execute(
            """
            SELECT director_term_summary FROM filings
            WHERE cik = ? AND director_term_summary IS NOT NULL
              AND TRIM(director_term_summary) != ''
            ORDER BY COALESCE(filing_date, '') DESC, id DESC
            LIMIT 1
            """,
            (cik_s,),
        ).fetchone()
        dts = (rd[0] or "").strip() if rd else ""

    why = why_surfaced_line(
        str(head.get("filing_form_type") or ""),
        head.get("filing_date"),
    )

    bio_text_for_age = (mgmt_nar or {}).get("bio_text") or ""
    narrative_age = (
        extract_age_from_bio_text(bio_text_for_age) if bio_text_for_age else None
    )
    age_stated = officer_age if officer_age is not None else narrative_age
    age_anchor = (
        (officer_age_filing_date or (head["filing_date"] or "")).strip()
        if officer_age is not None
        else (head["filing_date"] or "").strip()
    )
    display_age, _ = _age_estimated_for_today(age_stated, age_anchor)

    return {
        "norm_name": pn,
        "display_name": head["person_name"] or "—",
        "company_name": head["company_name"] or "",
        "cik": head["cik"] or "",
        "title": title_guess,
        "headline_year": None,
        "salary": None,
        "bonus": None,
        "stock_awards": None,
        "option_awards": None,
        "total": None,
        "equity": None,
        "filing_date": head["filing_date"] or "",
        "index_url": head["index_url"] or "",
        "primary_doc_url": head["primary_doc_url"] or "",
        "filing_form_type": head.get("filing_form_type") or "",
        "issuer_summary": iss_raw,
        "why_surfaced": why,
        "years_count": 0,
        "comp_timeline": "—",
        "sum_year_totals": None,
        "year_breakdown": [],
        "equity_hwm": 0.0,
        "total_hwm": 0.0,
        "signal_hwm": 0.0,
        "has_s1_comp": False,
        "has_summary_comp": False,
        "has_s1_officer": True,
        "source_filing_ids": [fid],
        "officer_age": display_age,
        "age_stated_in_filing": age_stated,
        "age_anchor_date": age_anchor,
        "officer_age_from_table": officer_age,
        "narrative_age": narrative_age,
        "issuer_website": issuer_web,
        "issuer_headquarters": issuer_hq,
        "issuer_hq_city_state": hq_city_state_display(issuer_hq),
        "issuer_industry": issuer_ind,
        "issuer_revenue_text": rev_txt,
        "mgmt_bio_role": (mgmt_nar or {}).get("role_heading") or "",
        "mgmt_bio_text": (mgmt_nar or {}).get("bio_text") or "",
        "mgmt_bio_display_name": (mgmt_nar or {}).get("person_name") or "",
        "director_term_summary": dts,
    }


def _build_profiles(conn: sqlite3.Connection) -> list[dict]:
    """One row per (CIK, person): headline = latest fiscal year; sums + per-year breakdown for drill-down."""
    if not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='neo_compensation'"
    ).fetchone():
        return []

    cur = conn.execute(
        """
        SELECT c.person_name, c.role_hint, c.fiscal_year,
               c.salary, c.bonus, c.stock_awards, c.option_awards,
               c.non_equity_incentive, c.pension_change, c.other_comp,
               c.total, c.equity_comp_disclosed,
               f.id AS filing_id, f.company_name, f.cik, f.filing_date,
               f.index_url, f.primary_doc_url, f.form_type AS filing_form_type,
               f.issuer_summary AS filing_issuer_summary,
               f.issuer_website AS filing_issuer_website,
               f.issuer_headquarters AS filing_issuer_headquarters,
               f.issuer_industry AS filing_issuer_industry,
               f.issuer_revenue_text AS filing_issuer_revenue_text,
               f.director_term_summary AS filing_director_term_summary
        FROM neo_compensation c
        JOIN filings f ON f.id = c.filing_id
        """
    )
    raw = [dict(r) for r in cur.fetchall()]
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in raw:
        groups[_profile_key(row["cik"], row["person_name"])].append(row)

    s1_officer_rows = _fetch_s1_officer_join_rows(conn)
    s1_officer_keys = {
        _profile_key(r["cik"], r["person_name"]) for r in s1_officer_rows
    }
    ciks_neo = {
        str(r["cik"] or "").strip() for r in raw if str(r.get("cik") or "").strip()
    }
    ciks_s1 = {
        str(r["cik"] or "").strip()
        for r in s1_officer_rows
        if str(r.get("cik") or "").strip()
    }
    ciks = ciks_neo | ciks_s1
    if not groups and not s1_officer_rows:
        return []

    narr_map: dict[tuple[int, str], dict] = {}
    fids_neo = {int(r["filing_id"]) for r in raw}
    fids_vis = {int(r["filing_id"]) for r in s1_officer_rows}
    fids_narr = fids_neo | fids_vis
    if fids_narr and conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='person_management_narrative'"
    ).fetchone():
        qm = ",".join("?" * len(fids_narr))
        ncur = conn.execute(
            f"""
            SELECT filing_id, person_name_norm, person_name, role_heading, bio_text
            FROM person_management_narrative
            WHERE filing_id IN ({qm})
            """,
            tuple(fids_narr),
        )
        for nr in ncur.fetchall():
            narr_map[(int(nr["filing_id"]), nr["person_name_norm"] or "")] = dict(nr)
    officer_rows_by_cik: dict[str, list[tuple[str, str, Optional[int], int, str]]] = (
        defaultdict(list)
    )
    if ciks:
        qmarks = ",".join("?" * len(ciks))
        ocur = conn.execute(
            f"""
            SELECT o.name, o.title, o.age, o.filing_id, f.filing_date, f.cik
            FROM officers o
            JOIN filings f ON f.id = o.filing_id
            WHERE f.cik IN ({qmarks})
              AND TRIM(COALESCE(o.title, '')) != ''
            """,
            tuple(ciks),
        )
        for o in ocur.fetchall():
            onm = (o["name"] or "").strip()
            if not is_acceptable_lead_person_name(onm):
                rfn = refine_lead_person_name(onm)
                if not rfn:
                    continue
                onm = rfn
            onorm = _norm_person_name(onm)
            tit = (o["title"] or "").strip()
            ck = str(o["cik"] or "").strip()
            if not onorm or not tit or not ck:
                continue
            try:
                oage = int(o["age"]) if o["age"] is not None else None
            except (TypeError, ValueError):
                oage = None
            officer_rows_by_cik[ck].append(
                (onorm, tit, oage, int(o["filing_id"]), o["filing_date"] or "")
            )

    profiles: list[dict] = []
    for key, items in groups.items():
        items.sort(
            key=lambda r: (
                r["fiscal_year"] or 0,
                r["filing_date"] or "",
                r["filing_id"] or 0,
            ),
            reverse=True,
        )
        head = items[0]
        raw_nm = (head["person_name"] or "").strip()
        display_nm = raw_nm
        if not is_acceptable_lead_person_name(display_nm):
            rnm = refine_lead_person_name(raw_nm)
            if rnm:
                display_nm = rnm
        if not is_acceptable_lead_person_name(display_nm):
            continue
        if display_nm != raw_nm:
            head = {**dict(head), "person_name": display_nm}
        by_year: dict[int, dict] = {}
        for r in items:
            fy_raw = r["fiscal_year"]
            if fy_raw is None:
                continue
            fy = int(fy_raw)
            if fy <= 0:
                continue
            cur = by_year.get(fy)
            if cur is None:
                by_year[fy] = r
            else:
                fd = r["filing_date"] or ""
                fd0 = cur["filing_date"] or ""
                if fd > fd0 or (
                    fd == fd0 and (r.get("filing_id") or 0) > (cur.get("filing_id") or 0)
                ):
                    by_year[fy] = r

        years_sorted = sorted(by_year.keys(), reverse=True)
        timeline_parts = []
        for y in years_sorted[:8]:
            t = by_year[y].get("total")
            timeline_parts.append(f"{y} {_money(t)}")
        timeline = " · ".join(timeline_parts) if timeline_parts else "—"

        sum_year_totals: Optional[float] = None
        if years_sorted:
            acc = 0.0
            any_tot = False
            for y in years_sorted:
                t = by_year[y].get("total")
                if t is not None:
                    acc += float(t)
                    any_tot = True
            if any_tot:
                sum_year_totals = acc

        year_breakdown: list[dict] = []
        for y in years_sorted:
            r = by_year[y]
            year_breakdown.append(
                {
                    "fiscal_year": y,
                    "salary": r.get("salary"),
                    "bonus": r.get("bonus"),
                    "stock_awards": r.get("stock_awards"),
                    "option_awards": r.get("option_awards"),
                    "non_equity_incentive": r.get("non_equity_incentive"),
                    "pension_change": r.get("pension_change"),
                    "other_comp": r.get("other_comp"),
                    "total": r.get("total"),
                    "equity_comp_disclosed": r.get("equity_comp_disclosed"),
                    "filing_date": r.get("filing_date") or "",
                    "primary_doc_url": r.get("primary_doc_url") or "",
                }
            )

        pn = _norm_person_name((head["person_name"] or "").strip())
        off_t, officer_age, officer_age_filing_date = _resolve_officer_extras_for_person(
            officer_rows_by_cik.get(str(head["cik"] or "").strip(), []),
            pref_filing_id=int(head["filing_id"]),
            person_norm=pn,
        )
        rh = (head.get("role_hint") or "").strip()
        title_guess = off_t if len(off_t) >= len(rh) else (rh or off_t)

        iss_raw = (head.get("filing_issuer_summary") or "").strip()
        if iss_raw and issuer_summary_looks_spammy(iss_raw):
            iss_raw = ""
        if not iss_raw:
            alt_rows = conn.execute(
                """
                SELECT issuer_summary FROM filings
                WHERE cik = ? AND issuer_summary IS NOT NULL
                  AND TRIM(issuer_summary) != ''
                ORDER BY COALESCE(filing_date, '') DESC, id DESC
                LIMIT 25
                """,
                (str(head["cik"] or "").strip(),),
            ).fetchall()
            for row in alt_rows:
                cand = (row[0] or "").strip()
                if cand and not issuer_summary_looks_spammy(cand):
                    iss_raw = cand
                    break

        cik_s = str(head["cik"] or "").strip()
        issuer_web = (head.get("filing_issuer_website") or "").strip()
        issuer_hq = _resolve_issuer_headquarters_for_profile(
            conn, cik_s, (head.get("filing_issuer_headquarters") or "").strip()
        )
        if not issuer_web and cik_s:
            rw = conn.execute(
                """
                SELECT issuer_website FROM filings
                WHERE cik = ? AND issuer_website IS NOT NULL
                  AND TRIM(issuer_website) != ''
                ORDER BY COALESCE(filing_date, '') DESC LIMIT 1
                """,
                (cik_s,),
            ).fetchone()
            issuer_web = (rw[0] or "").strip() if rw else ""

        issuer_ind = (head.get("filing_issuer_industry") or "").strip()
        if not issuer_ind and cik_s:
            rind = conn.execute(
                """
                SELECT issuer_industry FROM filings
                WHERE cik = ? AND issuer_industry IS NOT NULL
                  AND TRIM(issuer_industry) != ''
                ORDER BY COALESCE(filing_date, '') DESC LIMIT 1
                """,
                (cik_s,),
            ).fetchone()
            issuer_ind = (rind[0] or "").strip() if rind else ""

        rev_txt = _resolve_issuer_revenue_for_cik(
            conn,
            cik_s,
            head_revenue=str(head.get("filing_issuer_revenue_text") or ""),
        )

        mgmt_nar = narr_map.get((int(head["filing_id"]), pn))
        if not mgmt_nar and cik_s:
            altn = conn.execute(
                """
                SELECT m.person_name, m.role_heading, m.bio_text
                FROM person_management_narrative m
                JOIN filings f ON f.id = m.filing_id
                WHERE f.cik = ? AND m.person_name_norm = ?
                ORDER BY COALESCE(f.filing_date, '') DESC, f.id DESC
                LIMIT 1
                """,
                (cik_s, pn),
            ).fetchone()
            mgmt_nar = (
                {
                    "person_name": altn["person_name"],
                    "role_heading": altn["role_heading"],
                    "bio_text": altn["bio_text"],
                }
                if altn
                else None
            )

        dts = (head.get("filing_director_term_summary") or "").strip()
        if not dts and cik_s:
            rd = conn.execute(
                """
                SELECT director_term_summary FROM filings
                WHERE cik = ? AND director_term_summary IS NOT NULL
                  AND TRIM(director_term_summary) != ''
                ORDER BY COALESCE(filing_date, '') DESC, id DESC
                LIMIT 1
                """,
                (cik_s,),
            ).fetchone()
            dts = (rd[0] or "").strip() if rd else ""

        why = why_surfaced_line(
            str(head.get("filing_form_type") or ""),
            head.get("filing_date"),
        )

        equity_hwm = 0.0
        total_hwm = 0.0
        for r in items:
            equity_hwm = max(equity_hwm, _row_equity_usd(r))
            t = r.get("total")
            if t is not None:
                try:
                    total_hwm = max(total_hwm, float(t))
                except (TypeError, ValueError):
                    pass
        signal_hwm = max(total_hwm, equity_hwm)
        has_s1_comp = any(
            _is_s1_form_type(str(r.get("filing_form_type") or "")) for r in items
        )

        bio_text_for_age = (mgmt_nar or {}).get("bio_text") or ""
        narrative_age = (
            extract_age_from_bio_text(bio_text_for_age) if bio_text_for_age else None
        )
        age_stated = (
            officer_age if officer_age is not None else narrative_age
        )
        age_anchor = (
            (officer_age_filing_date or (head["filing_date"] or "")).strip()
            if officer_age is not None
            else (head["filing_date"] or "").strip()
        )
        display_age, _ = _age_estimated_for_today(age_stated, age_anchor)

        prof = {
            "norm_name": pn,
            "display_name": head["person_name"] or "—",
            "company_name": head["company_name"] or "",
            "cik": head["cik"] or "",
            "title": title_guess or "—",
            "headline_year": head["fiscal_year"],
            "salary": head["salary"],
            "bonus": head["bonus"],
            "stock_awards": head["stock_awards"],
            "option_awards": head.get("option_awards"),
            "non_equity_incentive": head.get("non_equity_incentive"),
            "pension_change": head.get("pension_change"),
            "other_comp": head.get("other_comp"),
            "total": head["total"],
            "equity": head["equity_comp_disclosed"],
            "filing_date": head["filing_date"] or "",
            "index_url": head["index_url"] or "",
            "primary_doc_url": head["primary_doc_url"] or "",
            "filing_form_type": head.get("filing_form_type") or "",
            "issuer_summary": iss_raw,
            "why_surfaced": why,
            "years_count": len(by_year),
            "comp_timeline": timeline,
            "sum_year_totals": sum_year_totals,
            "year_breakdown": year_breakdown,
            "equity_hwm": equity_hwm,
            "total_hwm": total_hwm,
            "signal_hwm": signal_hwm,
            "has_s1_comp": has_s1_comp,
            "officer_age": display_age,
            "age_stated_in_filing": age_stated,
            "age_anchor_date": age_anchor,
            "officer_age_from_table": officer_age,
            "narrative_age": narrative_age,
            "issuer_website": issuer_web,
            "issuer_headquarters": issuer_hq,
            "issuer_hq_city_state": hq_city_state_display(issuer_hq),
            "issuer_industry": issuer_ind,
            "issuer_revenue_text": rev_txt,
            "mgmt_bio_role": (mgmt_nar or {}).get("role_heading") or "",
            "mgmt_bio_text": (mgmt_nar or {}).get("bio_text") or "",
            "mgmt_bio_display_name": (mgmt_nar or {}).get("person_name") or "",
            "director_term_summary": dts,
        }
        prof["source_filing_ids"] = sorted({int(r["filing_id"]) for r in items})
        prof["has_summary_comp"] = True
        prof["has_s1_officer"] = _profile_key(
            str(head["cik"] or "").strip(), head["person_name"] or ""
        ) in s1_officer_keys
        _annotate_lead_tier_fields(prof)
        profiles.append(prof)

    vis_buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in s1_officer_rows:
        vis_buckets[_profile_key(r["cik"], r["person_name"])].append(r)
    for vkey, vrows in vis_buckets.items():
        if vkey in groups:
            continue
        vrows.sort(
            key=lambda d: (
                _filing_date_sort_key(d.get("filing_date") or ""),
                int(d.get("filing_id") or 0),
            ),
            reverse=True,
        )
        vhead = vrows[0]
        vraw = (vhead.get("person_name") or "").strip()
        vnm = vraw
        if not is_acceptable_lead_person_name(vnm):
            vr = refine_lead_person_name(vraw)
            if vr:
                vnm = vr
        if not is_acceptable_lead_person_name(vnm):
            continue
        if vnm != vraw:
            vhead = {**dict(vhead), "person_name": vnm}
        vprof = _visibility_profile_dict(conn, vhead, officer_rows_by_cik, narr_map)
        _annotate_lead_tier_fields(vprof)
        profiles.append(vprof)

    profiles.sort(key=_desk_sort_tuple, reverse=True)
    return profiles


def _lead_desk_filter_profiles(profiles: list[dict]) -> list[dict]:
    """
    Desk list: S-1 context (summary comp and/or S-1 officer/director table).
    Premium pay bar no longer drops rows — low-signal SCT and officer-only profiles
    stay on the desk as standard / visibility tiers (see lead_tier).
    """
    s1_only = lead_desk_s1_only()
    us_hq_only = lead_desk_us_registrant_hq_only()
    out: list[dict] = []
    for p in profiles:
        if s1_only and not (p.get("has_s1_comp") or p.get("has_s1_officer")):
            continue
        if us_hq_only and not registrant_hq_line_parses_as_united_states(
            (p.get("issuer_headquarters") or "").strip()
        ):
            continue
        out.append(p)
    out.sort(key=_desk_sort_tuple, reverse=True)
    return out


_PAY_BAND_MILLION = 1_000_000.0
_PAY_BAND_QUARTER = 250_000.0


def _profile_pay_signal_usd(p: dict) -> float:
    """Same basis as premium tier: best FY total vs equity, per desk env (signal_hwm / equity_hwm)."""
    if lead_desk_equity_only_min_usd():
        try:
            return float(p.get("equity_hwm") or 0)
        except (TypeError, ValueError):
            return 0.0
    try:
        return float(p.get("signal_hwm") or 0)
    except (TypeError, ValueError):
        return 0.0


def filter_profiles_pay_band(profiles: list[dict], band: str) -> list[dict]:
    """
    Filter by filing-derived pay signal (summary comp / equity high-water mark in DB).
    Not personal AUM — advisors use it as a comparable disclosed-comp lens.
    """
    b = (band or "all").strip().lower()
    if b in ("", "all"):
        return list(profiles)
    out: list[dict] = []
    for p in profiles:
        s = _profile_pay_signal_usd(p)
        if b in ("million_plus", "1m", "high"):
            if s >= _PAY_BAND_MILLION:
                out.append(p)
        elif b in ("quarter_to_million", "mid", "250k"):
            if _PAY_BAND_QUARTER <= s < _PAY_BAND_MILLION:
                out.append(p)
        elif b in ("under_quarter", "low", "rest"):
            if s < _PAY_BAND_QUARTER:
                out.append(p)
        else:
            out.append(p)
    return out


def _pay_band_nav_html(*, current: str, base_path: str, extra_qs: Optional[dict] = None) -> str:
    """Segment control for desk or finder (preserve non-band query keys)."""
    cur = (current or "all").strip().lower() or "all"
    extra: dict[str, str] = {}
    for k, v in (extra_qs or {}).items():
        if v is None or k == "band":
            continue
        s = str(v).strip()
        if s:
            extra[k] = s
    pairs = [
        ("all", "All"),
        ("million_plus", "$1M+ signal"),
        ("quarter_to_million", "$250k–$1M"),
        ("under_quarter", "Under $250k"),
    ]
    links: list[str] = []
    path = (base_path or "/").split("?")[0]
    for key, label in pairs:
        href = path + "?" + urlencode({**extra, "band": key})
        active = " pay-band-tab--active" if key == cur else ""
        links.append(
            f'<a class="pay-band-tab{active}" href="{html.escape(href)}">{html.escape(label)}</a>'
        )
    return (
        "<div class='pay-band-wrap'><p class='pay-band-hint'><strong>Pay signal</strong> from "
        "parsed <abbr title='Summary compensation table'>SCT</abbr> in your DB (best fiscal year "
        "— not household AUM). Same basis as Premium vs Standard tier.</p>"
        "<nav class='pay-band-nav' aria-label='Pay signal band'>" + " · ".join(links) + "</nav></div>"
    )


def filter_profiles_geo_industry_text(
    profiles: list[dict],
    *,
    location_sub: str = "",
    industry_sub: str = "",
    text_sub: str = "",
) -> list[dict]:
    """
    Server-side filters for RIA-style lookup: registrant HQ text, SIC/NAICS line plus
    issuer summary keywords, and free-text across person / company / CIK.
    """
    loc = (location_sub or "").lower().strip()
    ind = (industry_sub or "").lower().strip()
    q = (text_sub or "").lower().strip()
    out: list[dict] = []
    for p in profiles:
        if loc:
            blob_loc = " ".join(
                [
                    str(p.get("issuer_headquarters") or ""),
                    str(p.get("company_name") or ""),
                ]
            ).lower()
            if loc not in blob_loc:
                continue
        if ind:
            blob_ind = " ".join(
                [
                    str(p.get("issuer_industry") or ""),
                    str(p.get("issuer_summary") or ""),
                ]
            ).lower()
            if ind not in blob_ind:
                continue
        if q:
            blob_q = " ".join(
                [
                    str(p.get("display_name") or ""),
                    str(p.get("company_name") or ""),
                    str(p.get("cik") or ""),
                    str(p.get("title") or ""),
                ]
            ).lower()
            if q not in blob_q:
                continue
        out.append(p)
    return out


def finder_export_csv_bytes(
    *,
    profiles_all: list[dict],
    profiles_desk: list[dict],
    hq: str,
    industry: str,
    q: str,
    all_neo: bool,
    pay_band: str = "all",
) -> bytes:
    base = profiles_all if all_neo else profiles_desk
    filtered = filter_profiles_geo_industry_text(
        base,
        location_sub=hq,
        industry_sub=industry,
        text_sub=q,
    )
    filtered = filter_profiles_pay_band(filtered, pay_band)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(
        [
            "display_name",
            "company_name",
            "cik",
            "title",
            "registrant_hq",
            "industry_sic_naics",
            "filing_date",
            "latest_total_usd",
            "max_equity_usd",
            "lead_tier",
            "profile_path",
            "index_url",
            "primary_doc_url",
        ]
    )
    for p in filtered:
        w.writerow(
            [
                p.get("display_name") or "",
                p.get("company_name") or "",
                p.get("cik") or "",
                p.get("title") or "",
                p.get("issuer_headquarters") or "",
                p.get("issuer_industry") or "",
                p.get("filing_date") or "",
                p.get("total") if p.get("total") is not None else "",
                p.get("equity_hwm") if p.get("equity_hwm") is not None else "",
                p.get("lead_tier") or _profile_lead_tier(p),
                _profile_lead_url(p),
                p.get("index_url") or "",
                p.get("primary_doc_url") or "",
            ]
        )
    return buf.getvalue().encode("utf-8")


def _reported_other_cell(y: dict) -> str:
    """Show disclosed other, or imputed remainder from total when the cell is blank."""
    o = y.get("other_comp")
    if o is not None:
        return _money(o)
    t = y.get("total")
    if t is None:
        return "—"
    try:
        tf = float(t)
    except (TypeError, ValueError):
        return "—"
    s = 0.0
    for k in (
        "salary",
        "bonus",
        "stock_awards",
        "option_awards",
        "non_equity_incentive",
        "pension_change",
    ):
        v = y.get(k)
        if v is not None:
            try:
                s += float(v)
            except (TypeError, ValueError):
                pass
    r = tf - s
    if r < -2.0:
        return "—"
    if r <= 0.01:
        return _money(0)
    return _money(r) + "<span class='dim'> *</span>"


def _headline_year_row_from_profile(p: dict) -> Optional[dict]:
    """Single fiscal-year row for headline comp (SCT), preferring headline_year in year_breakdown."""
    yb = p.get("year_breakdown") or []
    hy = p.get("headline_year")
    hi: Optional[int] = None
    if hy is not None and str(hy).strip() != "":
        try:
            hi = int(hy)
        except (TypeError, ValueError):
            hi = None
    if hi is not None:
        for y in yb:
            if y.get("fiscal_year") == hi:
                return y
    if yb:
        return yb[0]
    if not p.get("has_summary_comp"):
        return None
    return {
        "fiscal_year": p.get("headline_year"),
        "salary": p.get("salary"),
        "bonus": p.get("bonus"),
        "stock_awards": p.get("stock_awards"),
        "option_awards": p.get("option_awards"),
        "non_equity_incentive": p.get("non_equity_incentive"),
        "pension_change": p.get("pension_change"),
        "other_comp": p.get("other_comp"),
        "equity_comp_disclosed": p.get("equity"),
        "total": p.get("total"),
        "filing_date": p.get("filing_date"),
        "primary_doc_url": p.get("primary_doc_url"),
    }


def _profile_headline_comp_breakout_html(p: dict) -> str:
    """One-row SCT breakout for the headline fiscal year — primary advisor comp view."""
    if not p.get("has_summary_comp"):
        return (
            '<div class="lead-comp-breakout-wrap card">'
            "<h2 class=\"lead-section-h\">Compensation</h2>"
            "<p class=\"meta dim\" style=\"margin:0\">No summary comp row in the database for this person yet "
            "(visibility-only profile or NEO not parsed). Use an S-1 SCT lead or run sync / backfill.</p>"
            "</div>"
        )
    row = _headline_year_row_from_profile(p)
    if not row:
        return (
            '<div class="lead-comp-breakout-wrap card">'
            "<h2 class=\"lead-section-h\">Compensation</h2>"
            "<p class=\"meta dim\" style=\"margin:0\">No fiscal-year breakdown available.</p>"
            "</div>"
        )
    fy = row.get("fiscal_year")
    try:
        fy_s = str(int(fy)) if fy is not None and str(fy).strip() != "" else "—"
    except (TypeError, ValueError):
        fy_s = str(fy).strip() if fy not in (None, "") else "—"
    doc = html.escape(row.get("primary_doc_url") or "")
    doc_l = f'<a href="{doc}" target="_blank" rel="noopener">Filing doc</a>' if doc else "—"
    tbl = (
        "<div class=\"table-wrap lead-comp-table-wrap\"><table class=\"inner-comp lead-comp-breakout\">"
        "<thead><tr>"
        '<th scope="col" title="Fiscal year">FY</th>'
        '<th scope="col" title="Salary">Salary</th>'
        '<th scope="col" title="Bonus">Bonus</th>'
        '<th scope="col" title="Stock awards">Stock</th>'
        '<th scope="col" title="Option awards">Opt.</th>'
        '<th scope="col" title="Non-equity incentive plan comp">Non-eq.</th>'
        '<th scope="col" title="Change in pension value">Pens.</th>'
        '<th scope="col" title="All other compensation">Other</th>'
        '<th scope="col" title="Total">Total</th>'
        "</tr></thead><tbody><tr>"
        f"<td class=\"num strong\">{html.escape(fy_s)}</td>"
        f"<td class=\"num\">{_money(row.get('salary'))}</td>"
        f"<td class=\"num\">{_money(row.get('bonus'))}</td>"
        f"<td class=\"num\">{_money(row.get('stock_awards'))}</td>"
        f"<td class=\"num\">{_money(row.get('option_awards'))}</td>"
        f"<td class=\"num\">{_money(row.get('non_equity_incentive'))}</td>"
        f"<td class=\"num\">{_money(row.get('pension_change'))}</td>"
        f"<td class=\"num\">{_reported_other_cell(row)}</td>"
        f"<td class=\"num strong\">{_money(row.get('total'))}</td>"
        "</tr></tbody></table></div>"
        f"<p class=\"meta dim lead-comp-foot\">Grant-date / filing-disclosed SCT values — not liquid or household wealth. "
        f"Filing date <b>{html.escape(row.get('filing_date') or '—')}</b> · {doc_l}</p>"
    )
    return (
        '<div class="lead-comp-breakout-wrap card">'
        '<h2 class="lead-section-h">Compensation</h2>'
        f"{tbl}"
        "</div>"
    )


def _profile_headline_comp_summary(p: dict) -> str:
    """Deprecated for lead page; kept for any legacy callers — delegates to breakout."""
    return _profile_headline_comp_breakout_html(p)


def _profile_breakdown_table(p: dict) -> str:
    yb = p.get("year_breakdown") or []
    if not yb:
        return "<p class='bd-note'>No fiscal-year rows.</p>"
    parts: list[str] = [
        "<table class='inner-comp'><thead><tr>",
        "<th>FY</th><th>Salary</th><th>Bonus</th><th>Stock</th><th>Options</th>",
        "<th>Non-equity</th><th>Pension Δ</th><th>Other</th>",
        "<th>Equity Σ</th><th>Total</th><th>Filing</th><th>Doc</th>",
        "</tr></thead><tbody>",
    ]
    for y in yb:
        doc = html.escape(y.get("primary_doc_url") or "")
        doc_l = f'<a href="{doc}" target="_blank" rel="noopener">S-1</a>' if doc else "—"
        fy = y.get("fiscal_year")
        parts.append(
            "<tr>"
            f"<td class='num'>{html.escape(str(fy) if fy is not None else '—')}</td>"
            f"<td class='num'>{_money(y.get('salary'))}</td>"
            f"<td class='num'>{_money(y.get('bonus'))}</td>"
            f"<td class='num'>{_money(y.get('stock_awards'))}</td>"
            f"<td class='num'>{_money(y.get('option_awards'))}</td>"
            f"<td class='num'>{_money(y.get('non_equity_incentive'))}</td>"
            f"<td class='num'>{_money(y.get('pension_change'))}</td>"
            f"<td class='num'>{_reported_other_cell(y)}</td>"
            f"<td class='num'>{_money(y.get('equity_comp_disclosed'))}</td>"
            f"<td class='num strong'>{_money(y.get('total'))}</td>"
            f"<td>{html.escape(y.get('filing_date') or '')}</td>"
            f"<td>{doc_l}</td>"
            "</tr>"
        )
    parts.append("</tbody></table>")
    return "".join(parts)


def _profile_lead_compensation_card_html(p: dict) -> str:
    """
    Lead profile: show every stored fiscal year by default (full SCT history).
    Falls back to the single headline row when year_breakdown is empty but comp exists.
    """
    if not p.get("has_summary_comp"):
        return _profile_headline_comp_breakout_html(p)
    yb = p.get("year_breakdown") or []
    if not yb:
        return _profile_headline_comp_breakout_html(p)
    top = yb[0]
    doc_u = (top.get("primary_doc_url") or "").strip()
    doc_e = html.escape(doc_u)
    doc_l = (
        f'<a href="{doc_e}" target="_blank" rel="noopener">Open filing</a>' if doc_u else ""
    )
    fd = html.escape(top.get("filing_date") or "—")
    foot_bits = [
        "Grant-date / filing-disclosed summary comp (SCT) — not liquid wealth.",
        f"Newest fiscal year first · latest row filing <b>{fd}</b>",
    ]
    if doc_l:
        foot_bits.append(doc_l)
    foot = (
        "<p class=\"meta dim lead-comp-foot\">"
        + " · ".join(foot_bits)
        + "</p>"
    )
    tbl = _profile_breakdown_table(p)
    return (
        '<div class="lead-comp-breakout-wrap card">'
        '<h2 class="lead-section-h">Compensation</h2>'
        '<p class="meta dim" style="margin:0 0 0.5rem;font-size:0.78rem">'
        "All fiscal years in the database for this person (summary comp table).</p>"
        '<div class="table-wrap lead-comp-table-wrap lead-comp-history-wrap">'
        f"{tbl}"
        "</div>"
        f"{foot}"
        "</div>"
    )


def _profile_lead_url(p: dict) -> str:
    return "/lead?" + urlencode(
        {"cik": str(p.get("cik") or ""), "name": p.get("display_name") or ""}
    )


def _find_profile(profiles: list[dict], cik: str, norm_name: str) -> Optional[dict]:
    cik_s = str(cik or "").strip()
    for p in profiles:
        if str(p.get("cik") or "").strip() == cik_s and p.get("norm_name") == norm_name:
            return p
    return None


def _hq_one_line_for_maps(raw: str | None) -> str:
    """Collapse multiline SEC HQ blocks to one line for map search URLs."""
    s = (raw or "").strip()
    if not s:
        return ""
    parts = [p.strip() for p in re.split(r"[\n\r]+", s) if p.strip()]
    one = ", ".join(parts)
    return re.sub(r"[ \t]{2,}", " ", one).strip()


def _best_issuer_headquarters_for_cik(
    conn: sqlite3.Connection, cik_s: str
) -> str:
    """
    Newest plausible registrant HQ, preferring S-1/F-1 and 10-K/10-Q over 8-K and other forms
    so a random current report does not override the cover address from a registration statement.
    """
    ck = (cik_s or "").strip()
    if not ck:
        return ""
    sql = """
        SELECT issuer_headquarters FROM filings
        WHERE cik = ? AND issuer_headquarters IS NOT NULL
          AND TRIM(issuer_headquarters) != ''
        ORDER BY
          CASE
            WHEN COALESCE(form_type, '') LIKE 'S-1%'
              OR COALESCE(form_type, '') LIKE 'F-1%' THEN 0
            WHEN COALESCE(form_type, '') LIKE '10-K%' THEN 1
            WHEN COALESCE(form_type, '') LIKE '10-Q%' THEN 2
            ELSE 3
          END,
          COALESCE(filing_date, '') DESC,
          id DESC
        LIMIT 25
    """
    rows = list(conn.execute(sql, (ck,)).fetchall())
    for row in rows:
        h = (row[0] if row else "") or ""
        h = str(h).strip()
        if is_plausible_registrant_headquarters(h):
            return h
    return ""


def _resolve_issuer_headquarters_for_profile(
    conn: sqlite3.Connection,
    cik_s: str,
    from_filing_row: str,
) -> str:
    """Use filing-attached HQ if it looks real; otherwise best plausible HQ for the CIK."""
    raw = (from_filing_row or "").strip()
    if is_plausible_registrant_headquarters(raw):
        return raw
    if not (cik_s or "").strip():
        return ""
    return _best_issuer_headquarters_for_cik(conn, cik_s)


def _filings_for_profile(conn: sqlite3.Connection, cik: str, _norm_name: str) -> list[dict]:
    """
    Issuer filing timeline for cross-reference: every S-1 and 10-K (incl. /A) we have for
    this CIK, newest first — not only filings where this person has a comp row (10-K often
    parses officers without hitting NEO comp).
    """
    cik_s = str(cik or "").strip()
    cur = conn.execute(
        """
        SELECT f.id, f.accession, f.form_type, f.filing_date, f.index_url, f.primary_doc_url
        FROM filings f
        WHERE f.cik = ?
          AND (form_type LIKE '%S-1%' OR form_type LIKE '%10-K%')
        ORDER BY COALESCE(f.filing_date, '') DESC, f.id DESC
        LIMIT 50
        """,
        (cik_s,),
    )
    out: list[dict] = []
    for r in cur.fetchall():
        out.append(
            {
                "id": int(r["id"]),
                "accession": r["accession"] or "",
                "form_type": r["form_type"] or "",
                "filing_date": r["filing_date"] or "",
                "index_url": r["index_url"] or "",
                "primary_doc_url": r["primary_doc_url"] or "",
            }
        )
    return out


def _desk_table(profiles: list[dict], stats: dict) -> str:
    if not profiles:
        n_all = int(stats.get("profile_count_all") or 0)
        hidden = n_all > 0
        extra = ""
        if hidden:
            s1 = bool(stats.get("lead_desk_s1_only"))
            min_e = float(stats.get("lead_desk_min_signal_usd") or 0)
            extra = (
                f"<p class='meta'><b>{n_all}</b> profile(s) in DB but not on the desk "
                f"({'S-1 context only (SCT or S-1 officer listing); ' if s1 else ''}"
                f"or outside your snapshot). Pay bar <b>${min_e:,.0f}</b> is used for "
                f"<b>premium</b> vs <b>standard</b> tiering only — it does not hide rows. "
                f"Open a saved <code>/lead?…</code> link for any profile. "
                f"To include non–S-1: <code>WEALTH_LEADS_LEAD_DESK_S1_ONLY=0</code>.</p>"
            )
        return f"""
  <h2>Lead desk</h2>
  <p class="meta">No rows match the current desk filters — see note below.</p>
  {extra}
  <p class="meta">If the database is empty, run <code>sync</code> / <code>backfill-comp</code>, or open <b>Source rows</b> below.</p>"""

    rows: list[str] = []
    for p in profiles:
        company = html.escape(p["company_name"] or "")
        title = html.escape(p["title"] or "—")
        href = html.escape(_profile_lead_url(p))
        nm = html.escape(p["display_name"] or "")
        idx = html.escape(p["index_url"] or "")
        doc = html.escape(p["primary_doc_url"] or "")
        idx_l = f'<a href="{idx}" target="_blank" rel="noopener" onclick="event.stopPropagation()">EDGAR</a>' if idx else "—"
        doc_l = f'<a href="{doc}" target="_blank" rel="noopener" onclick="event.stopPropagation()">Doc</a>' if doc else "—"
        tier = p.get("lead_tier") or _profile_lead_tier(p)
        tier_lbl = {"premium": "Premium", "standard": "Standard", "visibility": "Visibility"}.get(
            tier, tier
        )
        tier_title = (
            "Full SCT disclosure meets desk pay-signal bar (or bar is off)."
            if tier == "premium"
            else (
                "S-1 summary comp below desk pay bar — still a sellable lead."
                if tier == "standard"
                else "S-1 officer/director table only — no SCT row in DB; referral-tier."
            )
        )
        badge = (
            f'<span class="badge badge-tier-{html.escape(tier)}" '
            f'title="{html.escape(tier_title)}">{html.escape(tier_lbl)}</span>'
        )
        why_s = html.escape((p.get("why_surfaced") or "")[:160])
        why_cell = f'<span class="lead-why">{why_s}</span>' if why_s else ""
        oa_row = p.get("officer_age")
        age_cell = "—"
        if oa_row is not None:
            try:
                age_cell = str(int(oa_row))
            except (TypeError, ValueError):
                pass
        bio_full = (p.get("mgmt_bio_text") or "").strip()
        bio_one = re.sub(r"\s+", " ", bio_full)[:220]
        age_tip = ""
        st = p.get("age_stated_in_filing")
        anch = (p.get("age_anchor_date") or "").strip()
        if st is not None and anch:
            try:
                st_i = int(st)
                disp_i = int(oa_row) if oa_row is not None else st_i
                if disp_i != st_i:
                    age_tip = (
                        f"Age ~{disp_i} est. (stated {st_i} as of filing {anch}; "
                        "calendar years to today; birthday not in filing)."
                    )
                else:
                    age_tip = f"Age {st_i} (anchor filing date {anch})."
            except (TypeError, ValueError):
                pass
        tip_parts = []
        if bio_one:
            tip_parts.append(bio_one + ("…" if len(bio_full) > 220 else ""))
        if age_tip:
            tip_parts.append(age_tip)
        combined_tip = " \u00b7 ".join(tip_parts) if tip_parts else "Open profile"
        row_tip = html.escape(combined_tip, quote=True)
        tip_attr = f' title="{row_tip}"'
        web_u = (p.get("issuer_website") or "").strip()
        co_link = _canonical_external_url(web_u)
        if not co_link and p.get("primary_doc_url"):
            co_link = _canonical_external_url((p.get("primary_doc_url") or "").strip())
        co_href = html.escape(co_link)
        if co_link:
            co_cell = (
                f'<a href="{co_href}" target="_blank" rel="noopener" '
                f'onclick="event.stopPropagation()" title="Issuer site or filing">'
                f"{company}</a>"
            )
        else:
            co_cell = company
        rows.append(
            "<tr class='desk-row' tabindex='0' role='link' "
            f"data-href='{href}'{tip_attr}>"
            f"<td class='num'>{html.escape(age_cell)}</td>"
            f"<td class='profile-name'><a href='{href}'>{nm}</a>{why_cell}</td>"
            f"<td>{title}</td>"
            f"<td class='co-name'>{co_cell}</td>"
            f"<td><span class='badge'>{badge}</span></td>"
            f"<td class='num strong'>{_money(p['total'])}</td>"
            f"<td class='num'>{_money(p.get('equity_hwm'))}</td>"
            f"<td class='num dim'>{html.escape(str(p['headline_year'] or '—'))}</td>"
            f"<td>{html.escape(p['filing_date'] or '')}</td>"
            f"<td class='cik'>{html.escape(str(p['cik'] or ''))}</td>"
            f"<td>{idx_l} · {doc_l}</td>"
            "</tr>"
        )
    inner = "".join(rows)
    return f"""
  <h2>Lead desk</h2>
  <p class="meta">
    <b>Person-first pre-IPO leads.</b> Desk = <b>S-1 family</b> profiles: <b>Premium</b> (SCT meets pay bar), <b>Standard</b> (SCT below bar, e.g. many directors), and <b>Visibility</b> (officer/director on S-1, no SCT row in DB). Pay bar (<code>WEALTH_LEADS_LEAD_DESK_MIN_SIGNAL_USD</code>) tiers only — it does not remove rows. <b>Company</b> links to issuer site when parsed, else filing doc. <b>Latest total</b> = headline FY SCT (— if none); <b>Max equity</b> = best single-year stock+options.
    <b>Age</b> = stated in the filing, then + full calendar years to today (no birthday in EDGAR text); hover a row for <b>bio + age detail</b>.
    Full narrative + HQ on the profile page.
    <b>Click the row</b> for detail. Raw rows: <b>Source rows</b> below.
  </p>
  <div class="table-wrap">
  <table id="desk">
    <thead>
      <tr>
        <th title="Stated age in the filing, plus full calendar years to today (birthday not disclosed)">Age</th><th>Person</th><th>Role</th><th>Company</th><th title="Premium vs standard vs visibility (see desk note)">Tier</th>
        <th>Latest total</th><th title='Max single-FY stock + options (SCT)'>Max equity</th>
        <th>FY</th><th>Filing</th><th>CIK</th><th>Quick source</th>
      </tr>
    </thead>
    <tbody id="desk-body">{inner}</tbody>
  </table>
  </div>"""


def _finder_table(profiles: list[dict]) -> str:
    if not profiles:
        return """
  <h2>Matching leads</h2>
  <p class="meta">No rows match these filters. Widen the search, check <b>All NEO profiles</b>, or run
  <code>python -m wealth_leads backfill-comp --force</code> to re-parse HQ and SIC/NAICS from filings.</p>"""

    rows_html: list[str] = []
    for p in profiles:
        company = html.escape(p["company_name"] or "")
        title = html.escape(p["title"] or "—")
        href = html.escape(_profile_lead_url(p))
        nm = html.escape(p["display_name"] or "")
        idx = html.escape(p["index_url"] or "")
        doc = html.escape(p["primary_doc_url"] or "")
        idx_l = f'<a href="{idx}" target="_blank" rel="noopener" onclick="event.stopPropagation()">EDGAR</a>' if idx else "—"
        doc_l = f'<a href="{doc}" target="_blank" rel="noopener" onclick="event.stopPropagation()">Doc</a>' if doc else "—"
        hq_raw = (p.get("issuer_headquarters") or "").strip()
        hq_disp = html.escape(hq_raw[:140] + ("…" if len(hq_raw) > 140 else ""))
        ind_raw = (p.get("issuer_industry") or "").strip()
        ind_disp = html.escape(ind_raw[:160] + ("…" if len(ind_raw) > 160 else ""))
        if not hq_disp:
            hq_disp = "<span class='dim'>—</span>"
        if not ind_disp:
            ind_disp = "<span class='dim'>—</span>"
        oa_row = p.get("officer_age")
        age_cell = "—"
        if oa_row is not None:
            try:
                age_cell = str(int(oa_row))
            except (TypeError, ValueError):
                pass
        rows_html.append(
            "<tr class='desk-row' tabindex='0' role='link' "
            f"data-href='{href}'>"
            f"<td class='num'>{html.escape(age_cell)}</td>"
            f"<td class='profile-name'><a href='{href}'>{nm}</a></td>"
            f"<td>{title}</td>"
            f"<td class='co-name'>{company}</td>"
            f"<td class='hq-cell'>{hq_disp}</td>"
            f"<td class='ind-cell'>{ind_disp}</td>"
            f"<td class='num strong'>{_money(p['total'])}</td>"
            f"<td>{html.escape(p['filing_date'] or '')}</td>"
            f"<td class='cik'>{html.escape(str(p['cik'] or ''))}</td>"
            f"<td>{idx_l} · {doc_l}</td>"
            "</tr>"
        )
    inner = "".join(rows_html)
    return f"""
  <h2>Matching leads</h2>
  <p class="meta"><b>{len(profiles)}</b> profile(s) match the current filters. Click a row for the full profile and filing links.</p>
  <div class="table-wrap">
  <table id="finder">
    <thead>
      <tr>
        <th>Age</th><th>Person</th><th>Role</th><th>Company</th>
        <th title="Registrant principal office / HQ from filing text">Registrant HQ</th>
        <th title="SIC or NAICS line when parsed from the filing">Industry (SIC/NAICS)</th>
        <th>Latest total</th><th>Filing</th><th>CIK</th><th>Sources</th>
      </tr>
    </thead>
    <tbody id="finder-body">{inner}</tbody>
  </table>
  </div>"""


def _finder_form(
    *,
    hq: str,
    industry: str,
    q: str,
    all_neo: bool,
    export_href: str,
    pay_band: str = "all",
    form_action: str = "/finder",
) -> str:
    hq_e = html.escape(hq)
    ind_e = html.escape(industry)
    q_e = html.escape(q)
    band_e = html.escape((pay_band or "all").strip() or "all")
    action_e = html.escape(form_action)
    return f"""
  <div class="card" style="margin-bottom:1rem">
    <h2 style="margin-top:0">Search</h2>
    <p class="meta" style="margin-top:0">Filter by <b>registrant location</b> (HQ address text) and <b>SIC/NAICS</b> (parsed code line from the filing, if any) plus issuer summary text. All matching is substring, case-insensitive.</p>
    <form method="get" action="{action_e}" style="max-width:40rem">
      <input type="hidden" name="band" value="{band_e}"/>
      <label class="sr" for="hq">Registrant HQ contains</label>
      <input type="search" id="hq" name="hq" placeholder="e.g. California, Austin, 94105" value="{hq_e}" style="width:100%;max-width:100%;margin-bottom:0.5rem"/>
      <label class="sr" for="industry">SIC / NAICS or summary contains</label>
      <input type="search" id="industry" name="industry" placeholder="e.g. NAICS 541, SIC 7372, biotech (also matches issuer summary)" value="{ind_e}" style="width:100%;max-width:100%;margin-bottom:0.5rem"/>
      <label class="sr" for="fq">Person or company contains</label>
      <input type="search" id="fq" name="q" placeholder="Person name, company, or CIK…" value="{q_e}" style="width:100%;max-width:100%;margin-bottom:0.5rem"/>
      <label style="display:flex;align-items:center;gap:0.5rem;font-size:0.8125rem;color:#8b96a3;cursor:pointer;margin:0.5rem 0">
        <input type="checkbox" name="all_neo" value="1"{' checked' if all_neo else ''}/>
        All profiles in DB (ignore lead-desk S-1-context filter; includes every tier)
      </label>
      <button type="submit" style="margin-top:0.35rem;padding:0.45rem 0.9rem;border-radius:4px;border:1px solid #238636;background:#238636;color:#fff;cursor:pointer;font:inherit">Apply filters</button>
      <a href="{html.escape(export_href)}" style="margin-left:0.75rem;font-size:0.8125rem">Download CSV</a>
    </form>
  </div>"""


def _page_finder(
    profiles: list[dict],
    *,
    stats: dict,
    rendered_at: str,
    hq: str,
    industry: str,
    q: str,
    all_neo: bool,
    base_count: int,
    pay_band: str = "all",
    nav_base_path: str = "/finder",
    form_action: str = "/finder",
    desk_href: str = "/",
    export_path: str = "/export/finder.csv",
) -> str:
    banner = _stats_banner(stats, rendered_at)
    band_nav = _pay_band_nav_html(
        current=pay_band,
        base_path=nav_base_path,
        extra_qs={
            "hq": hq,
            "industry": industry,
            "q": q,
            **({"all_neo": "1"} if all_neo else {}),
        },
    )
    css = _shared_css()
    extra_css = """
    td.hq-cell, td.ind-cell { font-size: 0.72rem; color: #a8b0ba; max-width: 14rem; }
    #filter-finder { width: 100%; max-width: 22rem; padding: 0.4rem 0.55rem; border-radius: 4px;
      border: 1px solid #2a3340; background: #0a0e12; color: #d8dee4; font: inherit; margin-bottom: 0.75rem; }
    """
    exp_q = urlencode(
        {
            "hq": hq or "",
            "industry": industry or "",
            "q": q or "",
            "band": (pay_band or "all").strip() or "all",
            **({"all_neo": "1"} if all_neo else {}),
        }
    )
    export_href = export_path + "?" + exp_q
    form = _finder_form(
        hq=hq,
        industry=industry,
        q=q,
        all_neo=all_neo,
        export_href=export_href,
        pay_band=pay_band,
        form_action=form_action,
    )
    tbl = _finder_table(profiles)
    scope = (
        f"<p class='meta'>Showing <b>{len(profiles)}</b> row(s) after HQ / SIC·NAICS·summary / text filters and pay-signal band. "
        f"Universe before filters: <b>{base_count}</b> "
        f"({'all in database' if all_neo else 'after lead-desk S-1-context filters'}).</p>"
    )
    desk_link_e = html.escape(desk_href)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>WealthPipeline — lead finder</title>
  <style>{css}{extra_css}</style>
</head>
<body class="wide">
  <h1>WealthPipeline <span class="tag">lead finder · local</span></h1>
  <p class="meta">
    Search people tied to issuers in your database. <b>HQ</b> is the registrant’s principal office line from filings (not a home address).
    <b>SIC/NAICS</b> matches the parsed code line from the filing when present; you can also match keywords in the issuer business summary.
    <a href="{desk_link_e}">Lead desk</a>
  </p>
  {banner}
  {band_nav}
  {form}
  {scope}
  {tbl}
  <label class="sr" for="filter-finder">Narrow results in this table</label>
  <input type="search" id="filter-finder" placeholder="Quick filter this table…" autocomplete="off"/>
  <script>
  (function() {{
    var input = document.getElementById('filter-finder');
    if (input) {{
      input.addEventListener('input', function() {{
        var q = (input.value || '').toLowerCase().trim();
        document.querySelectorAll('#finder-body tr').forEach(function(tr) {{
          if (!q) {{ tr.classList.remove('hidden'); return; }}
          tr.classList.toggle('hidden', (tr.textContent || '').toLowerCase().indexOf(q) < 0);
        }});
      }});
    }}
    document.querySelectorAll('#finder-body tr.desk-row').forEach(function(tr) {{
      function go() {{
        var h = tr.getAttribute('data-href');
        if (h) window.location = h;
      }}
      tr.addEventListener('click', function(e) {{
        if (e.target.closest('a')) return;
        go();
      }});
      tr.addEventListener('keydown', function(e) {{
        if (e.key === 'Enter') {{ e.preventDefault(); go(); }}
      }});
    }});
  }})();
  </script>
  {_live_reload_snippet()}
</body>
</html>"""


def _leads_table(rows: list[sqlite3.Row]) -> str:
    body_rows = []
    for r in rows:
        name = r["name"] or "—"
        title = html.escape(r["title"] or "—")
        company = html.escape(r["company_name"] or "")
        idx = html.escape(r["index_url"] or "")
        doc = html.escape(r["primary_doc_url"] or "")
        idx_link = f'<a href="{idx}" target="_blank" rel="noopener">index</a>' if idx else "—"
        doc_link = f'<a href="{doc}" target="_blank" rel="noopener">S-1</a>' if doc else "—"
        cy = r["comp_year"]
        body_rows.append(
            "<tr>"
            f"<td>{company}</td>"
            f"<td>{html.escape(str(r['cik'] or ''))}</td>"
            f"<td>{html.escape(str(r['filing_date'] or ''))}</td>"
            f"<td>{html.escape(name)}</td>"
            f"<td>{title}</td>"
            f"<td class='num'>{html.escape(str(cy)) if cy is not None else '—'}</td>"
            f"<td class='num'>{_money(r['comp_salary'])}</td>"
            f"<td class='num'>{_money(r['comp_bonus'])}</td>"
            f"<td class='num'>{_money(r['comp_stock'])}</td>"
            f"<td class='num'>{_money(r['comp_total'])}</td>"
            f"<td class='num'>{_money(r['comp_equity'])}</td>"
            f"<td>{idx_link}</td>"
            f"<td>{doc_link}</td>"
            "</tr>"
        )
    inner = (
        "".join(body_rows)
        if body_rows
        else '<tr><td colspan="13">No rows yet. Run sync first.</td></tr>'
    )
    return f"""
  <h2>Officers by filing (raw)</h2>
  <p class="meta">One row per officer line in the filing. Comp columns match the summary table by name when available.</p>
  <table>
    <thead>
      <tr>
        <th>Company</th><th title="SEC company ID, not dollars">CIK</th><th>Filed</th><th>Name</th><th>Title</th>
        <th title="Fiscal year of comp row">Comp yr</th>
        <th>Salary</th><th>Bonus</th><th>Stock</th><th>Total</th><th>Equity sum</th>
        <th>EDGAR</th><th>Doc</th>
      </tr>
    </thead>
    <tbody>{inner}</tbody>
  </table>"""


def _comp_table(rows: list[sqlite3.Row]) -> str:
    body_rows = []
    for r in rows:
        company = html.escape(r["company_name"] or "")
        doc = html.escape(r["primary_doc_url"] or "")
        doc_link = f'<a href="{doc}" target="_blank" rel="noopener">S-1</a>' if doc else "—"
        role = html.escape(r["role_hint"] or "—")
        body_rows.append(
            "<tr>"
            f"<td>{company}</td>"
            f"<td>{html.escape(r['person_name'] or '')}</td>"
            f"<td>{role}</td>"
            f"<td>{html.escape(str(r['fiscal_year'] or ''))}</td>"
            f"<td class='num'>{_money(r['salary'])}</td>"
            f"<td class='num'>{_money(r['bonus'])}</td>"
            f"<td class='num'>{_money(r['stock_awards'])}</td>"
            f"<td class='num'>{_money(r['option_awards'])}</td>"
            f"<td class='num'>{_money(r['other_comp'])}</td>"
            f"<td class='num'>{_money(r['total'])}</td>"
            f"<td class='num'>{_money(r['equity_comp_disclosed'])}</td>"
            f"<td>{doc_link}</td>"
            "</tr>"
        )
    inner = (
        "".join(body_rows)
        if body_rows
        else '<tr><td colspan="12">No summary compensation rows yet. Re-run <code>python -m wealth_leads sync --force</code> after upgrade.</td></tr>'
    )
    return f"""
  <h2>NEO summary compensation lines (raw)</h2>
  <p class="meta">Every parsed comp row — the desk rolls these up by person + company (CIK).</p>
  <table>
    <thead>
      <tr>
        <th>Company</th><th>NEO</th><th>Role (if parsed)</th><th>Year</th>
        <th>Salary</th><th>Bonus</th><th>Stock</th><th>Options</th><th>Other</th><th>Total</th>
        <th>Equity cols sum</th><th>Doc</th>
      </tr>
    </thead>
    <tbody>{inner}</tbody>
  </table>"""


def _comp_missing_callout(stats: dict) -> str:
    if stats.get("missing_db"):
        return ""
    if stats.get("comp_rows", 0) > 0:
        return ""
    if stats.get("filings", 0) == 0:
        return ""
    return """<div class="callout">
    <strong>No compensation rows in your database yet.</strong>
    CIK is only SEC’s company ID (not pay). Run <code>python -m wealth_leads sync</code> (sync now auto-runs a comp backfill), or
    <code>python -m wealth_leads backfill-comp</code>, then refresh. Use <code>backfill-comp --force</code> after upgrading the parser.
    Many small S-1s have no parseable summary table.
  </div>"""


def _stats_banner(stats: dict, rendered_at: str) -> str:
    if stats.get("missing_db"):
        return """<div class="banner warn"><strong>No database file yet.</strong> Run sync once, then refresh this page.</div>"""
    nf, no, nc = stats["filings"], stats["officers"], stats["comp_rows"]
    np = int(stats.get("profile_count", 0))
    np_all = int(stats.get("profile_count_all", np))
    latest = html.escape(str(stats.get("latest_filing_date") or "—"))
    mtime = html.escape(str(stats.get("db_file_modified") or "—"))
    rat = html.escape(rendered_at)
    desk_lbl = f"{np} desk leads"
    if np_all != np:
        desk_lbl = f"{np} desk leads <span style='color:#6b7785'>({np_all} NEO profiles before filters)</span>"
    return f"""<div class="banner">
    <strong>This database</strong>
    <span class="stats"><span>{desk_lbl}</span><span>{nf} filings</span><span>{no} officer rows</span><span>{nc} comp rows</span></span>
    <span class="sub">Lead desk row count is filtered (e.g. S-1 context); it is <b>not</b> the same as &ldquo;My leads&rdquo; for a signed-in advisor. Newest filing: <b>{latest}</b> · DB updated: <b>{mtime}</b> · Page: <b>{rat}</b></span>
  </div>"""


def _shared_css() -> str:
    return """
    :root {
      font-family: system-ui, sans-serif;
      background: #0a0e12;
      color: #d8dee4;
    }
    body { margin: 0; padding: 1rem 1.1rem; max-width: 900px; margin-inline: auto; }
    body.wide { max-width: 1480px; }
    nav.top { margin-bottom: 1rem; font-size: 0.84rem; }
    nav.top a { color: #5eb3e0; }
    h1 { font-size: 1.2rem; font-weight: 600; margin-top: 0; letter-spacing: -0.02em; }
    h1 span.tag { font-weight: 400; color: #6b7785; font-size: 0.88rem; }
    h2 { font-size: 0.95rem; margin-top: 1.5rem; margin-bottom: 0.45rem; color: #a8b0ba; font-weight: 600; }
    h2:first-of-type { margin-top: 0.5rem; }
    p.meta { color: #6b7785; font-size: 0.8125rem; margin-bottom: 0.85rem; line-height: 1.5; }
    .banner { background: #121820; border: 1px solid #2a3340; border-radius: 6px; padding: 0.75rem 0.9rem; margin-bottom: 0.85rem; }
    .banner.warn { border-color: #8b4040; background: #1f1515; }
    .banner .stats { display: flex; flex-wrap: wrap; gap: 0.5rem 1.1rem; margin: 0.4rem 0; font-size: 0.84rem; }
    .banner .stats span { color: #6b7785; }
    .banner .stats span::before { content: "· "; color: #2a3340; }
    .banner .stats span:first-child::before { content: ""; }
    .banner .sub { display: block; font-size: 0.75rem; color: #6b7785; margin-top: 0.3rem; }
    .callout { background: #1a1810; border: 1px solid #5a4f2a; border-radius: 6px; padding: 0.75rem 0.9rem; margin: 0 0 0.85rem 0; font-size: 0.8125rem; line-height: 1.45; }
    .callout strong { color: #d4b84a; }
    label.sr { display: block; font-size: 0.75rem; color: #6b7785; margin-bottom: 0.3rem; }
    #filter {
      width: 100%; max-width: 22rem; padding: 0.4rem 0.55rem; border-radius: 4px;
      border: 1px solid #2a3340; background: #0a0e12; color: #d8dee4; font: inherit;
    }
    .table-wrap { overflow-x: auto; border: 1px solid #2a3340; border-radius: 6px; margin-bottom: 0.5rem; }
    table { width: 100%; border-collapse: collapse; font-size: 0.75rem;
      font-variant-numeric: tabular-nums;
    }
    #desk .num, .detail-page .num { font-family: ui-monospace, "Cascadia Mono", "Consolas", monospace; }
    th, td { text-align: left; padding: 0.4rem 0.5rem; border-bottom: 1px solid #222a33; vertical-align: top; }
    thead th { background: #0f1318; color: #8b96a3; font-weight: 600; position: sticky; top: 0; z-index: 1; }
    th { color: #8b96a3; font-weight: 600; }
    td.num { text-align: right; }
    td.strong { font-weight: 600; color: #e8ecf0; }
    td.dim { color: #6b7785; text-align: center; }
    td.cik { color: #6b7785; font-size: 0.72rem; }
    td.profile-name { font-weight: 600; color: #e8ecf0; }
    td.profile-name a { color: #e8ecf0; }
    td.co-name a { color: #8ecfff; font-weight: 600; }
    tr.desk-row { cursor: pointer; }
    tr.desk-row:focus { outline: 1px solid #5eb3e0; outline-offset: -1px; }
    tr.desk-row:hover td { background: #121820; }
    span.badge {
      display: inline-block; font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.04em;
      padding: 0.15rem 0.4rem; border-radius: 4px; background: #1a2634; color: #8b96a3; border: 1px solid #2a3340;
    }
    .badge-tier-premium { background: #15251c; color: #56d364; border-color: #2a6a3f; }
    .badge-tier-standard { background: #1e2515; color: #d4a72c; border-color: #5c4f2a; }
    .badge-tier-visibility { background: #22252c; color: #a8b0ba; border-color: #3d424d; }
    p.lead-tier-strip { margin-top: 0.35rem; margin-bottom: 0.5rem; }
    .client-research-card { border-color: #316d9a; background: linear-gradient(180deg, #141c24 0%, #121820 100%); }
    .client-research-card h2 { color: #79c0ff; }
    .lead-outreach-panel { display: flow-root; }
    .research-photo-wrap { float: right; margin: 0 0 0.75rem 1rem; max-width: 160px; }
    .research-photo { width: 100%; max-height: 200px; object-fit: cover; border-radius: 8px; border: 1px solid #2a3340; }
    @media (max-width: 520px) {
      .lead-outreach-panel .research-photo-wrap { float: none; margin: 0 0 0.75rem 0; max-width: 100%; }
    }
    .client-research-links { display: flex; flex-wrap: wrap; gap: 0.5rem 1rem; margin-top: 0.5rem; font-size: 0.8125rem; }
    .advisor-subh { font-size: 0.78rem; text-transform: uppercase; letter-spacing: 0.06em; color: #8b96a3; margin: 0.85rem 0 0.2rem; font-weight: 600; }
    .advisor-company-snapshot { border-color: #3d4d60; }
    .outreach-email-table { width: 100%; font-size: 0.72rem; margin-top: 0.5rem; }
    .outreach-email-table td { padding: 0.3rem 0.35rem; border-bottom: 1px solid #1a2228; }
    details.lead-more { margin-top: 0.75rem; border-top: 1px solid #2a3340; padding-top: 0.65rem; }
    details.lead-more summary { cursor: pointer; color: #8b96a3; font-size: 0.8rem; }
    .person-story-prose { font-size: 0.875rem; line-height: 1.55; color: #c5ccd4; margin-top: 0.35rem; }
    .lead-why {
      display: block; font-size: 0.68rem; color: #7a8796; line-height: 1.35;
      margin-top: 0.28rem; max-width: 24rem; font-weight: 400;
    }
    span.dim, .dim { color: #6b7785; font-weight: 400; }
    a { color: #5eb3e0; text-decoration: none; }
    a:hover { text-decoration: underline; }
    tr.hidden { display: none; }
    code { font-size: 0.85em; }
    p.bd-note { margin: 0 0 0.5rem 0; font-size: 0.72rem; color: #6b7785; line-height: 1.45; max-width: 52rem; }
    table.inner-comp { width: 100%; font-size: 0.72rem; margin: 0; border-collapse: collapse; }
    table.inner-comp th { background: #0c1016; color: #8b96a3; font-weight: 600; padding: 0.35rem 0.45rem; border-bottom: 1px solid #2a3340; }
    table.inner-comp td { padding: 0.35rem 0.45rem; border-bottom: 1px solid #1a2228; }
    table.inner-comp tbody tr:hover td { background: #0f141a; }
    .hero { display: grid; gap: 0.75rem; margin-bottom: 1rem; }
    @media (min-width: 640px) { .hero { grid-template-columns: 1fr 1fr; } }
    @media (min-width: 900px) { .hero { grid-template-columns: repeat(4, 1fr); } }
    .stat-card {
      background: #121820; border: 1px solid #2a3340; border-radius: 6px; padding: 0.65rem 0.75rem;
    }
    .stat-card .lbl { font-size: 0.7rem; color: #6b7785; text-transform: uppercase; letter-spacing: 0.03em; }
    .stat-card .val { font-size: 1.05rem; font-weight: 600; margin-top: 0.2rem; font-family: ui-monospace, monospace; }
    .card {
      background: #121820; border: 1px solid #2a3340; border-radius: 6px; padding: 0.85rem 1rem; margin-bottom: 1rem;
    }
    .card.bio-placeholder { border-style: dashed; border-color: #3d4a5c; }
    .lead-comp-block { margin-bottom: 0.25rem; }
    .lead-comp-summary { margin-bottom: 0.75rem; }
    .lead-comp-dl {
      display: grid;
      grid-template-columns: minmax(7rem, max-content) 1fr;
      gap: 0.4rem 1rem;
      margin: 0;
      font-size: 0.84rem;
      line-height: 1.45;
    }
    .lead-comp-dl dt { margin: 0; color: #8b96a3; font-weight: 600; }
    .lead-comp-dd { margin: 0; }
    details.lead-comp-details { margin-top: 0.5rem; }
    details.lead-comp-details summary {
      cursor: pointer;
      color: #8b96a3;
      font-size: 0.8125rem;
      user-select: none;
    }
    details.lead-comp-details summary:hover { color: #c5ccd4; }
    .lead-mgmt-bio-wrap { margin-top: 2.25rem; }
    table.related-leads-table td.related-title {
      max-width: 14rem;
      font-size: 0.72rem;
      line-height: 1.35;
    }
    details.audit { margin-top: 1.5rem; border-top: 1px solid #2a3340; padding-top: 1rem; }
    details.audit summary {
      cursor: pointer; color: #8b96a3; font-size: 0.8125rem; user-select: none;
      margin-bottom: 0.75rem;
    }
    details.audit summary:hover { color: #c5ccd4; }
    body.lead-profile-page { max-width: 1180px; }
    .lead-page-header { margin-bottom: 1rem; padding-bottom: 0.85rem; border-bottom: 1px solid #2a3340; }
    .lead-section-h {
      font-size: 0.95rem; font-weight: 600; margin: 0 0 0.5rem 0; color: #e8ecf0; letter-spacing: -0.01em;
    }
    .lead-hero-line { font-size: 0.92rem; line-height: 1.5; color: #c5ccd4; margin: 0.25rem 0 0.65rem; }
    .lead-hero-sep { color: #5c6570; margin: 0 0.35rem; }
    .lead-hero-kv {
      display: grid; grid-template-columns: repeat(2, 1fr); gap: 0.5rem 1rem;
      margin: 0.35rem 0 0.25rem; padding: 0.65rem 0.75rem;
      background: #101820; border: 1px solid #2a3340; border-radius: 6px;
    }
    @media (min-width: 640px) { .lead-hero-kv { grid-template-columns: repeat(4, 1fr); } }
    .lead-kv { min-width: 0; }
    .lead-kv-l {
      display: block; font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.06em;
      color: #6b7785; margin-bottom: 0.15rem;
    }
    .lead-kv-v { font-size: 0.88rem; color: #e8ecf0; line-height: 1.35; word-break: break-word; }
    .lead-hero-links { margin: 0.45rem 0 0; font-size: 0.84rem; }
    .lead-comp-fullwidth {
      width: 100%;
      margin: 0 0 1.1rem 0;
    }
    .lead-comp-fullwidth .lead-comp-block { margin-bottom: 0; }
    .lead-comp-fullwidth .lead-comp-table-wrap {
      overflow-x: visible;
      max-width: 100%;
    }
    .lead-comp-fullwidth div.table-wrap.lead-comp-table-wrap {
      overflow-x: visible;
    }
    .lead-comp-fullwidth table.lead-comp-breakout {
      width: 100%;
      table-layout: fixed;
    }
    .lead-comp-fullwidth table.lead-comp-breakout th,
    .lead-comp-fullwidth table.lead-comp-breakout td {
      font-size: 0.72rem;
      padding: 0.4rem 0.28rem;
      white-space: normal;
      word-break: normal;
      overflow-wrap: anywhere;
      vertical-align: top;
    }
    .lead-comp-fullwidth table.lead-comp-breakout .num {
      text-align: right;
      font-variant-numeric: tabular-nums;
      white-space: nowrap;
    }
    .lead-comp-fullwidth table.lead-comp-breakout th:first-child,
    .lead-comp-fullwidth table.lead-comp-breakout td:first-child { width: 2.5rem; }
    @media (max-width: 480px) {
      .lead-comp-fullwidth table.lead-comp-breakout th,
      .lead-comp-fullwidth table.lead-comp-breakout td { font-size: 0.62rem; padding: 0.3rem 0.12rem; }
    }
    .lead-comp-history-wrap {
      overflow-x: auto;
      -webkit-overflow-scrolling: touch;
      max-width: 100%;
    }
    table.lead-comp-breakout th, table.lead-comp-breakout td { font-size: 0.78rem; padding: 0.4rem 0.45rem; }
    table.lead-comp-breakout .num { text-align: right; font-variant-numeric: tabular-nums; }
    .lead-comp-foot { margin: 0.5rem 0 0; font-size: 0.75rem; }
    .lead-comp-table-wrap { overflow-x: auto; -webkit-overflow-scrolling: touch; margin: 0; }
    .lead-snapshot-inner .card { margin-bottom: 0; }
    .lead-snapshot-inner { margin-top: 0.35rem; }
    details.lead-snapshot-fold { margin-bottom: 0.85rem; }
    .lead-split {
      display: grid;
      gap: 1.25rem;
      align-items: start;
      margin-bottom: 1rem;
    }
    @media (min-width: 920px) {
      .lead-split { grid-template-columns: 1fr 1fr; }
    }
    .lead-col { min-width: 0; }
    .lead-col-title {
      font-size: 0.68rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: #9aa3b0;
      margin: 0 0 0.25rem;
      font-weight: 700;
    }
    .lead-company-card .lead-company-dl {
      display: grid;
      grid-template-columns: minmax(6.5rem, max-content) 1fr;
      gap: 0.45rem 1rem;
      margin: 0;
      font-size: 0.8125rem;
      line-height: 1.5;
    }
    .lead-company-card .lead-company-dl dt {
      margin: 0;
      color: #8b96a3;
      font-weight: 600;
    }
    .lead-company-card .lead-company-dl dd {
      margin: 0;
      color: #c5ccd4;
      word-break: break-word;
      min-width: 0;
    }
    .lead-company-card .lead-company-dl dd a {
      word-break: break-all;
    }
    @media (max-width: 420px) {
      .lead-company-card .lead-company-dl {
        grid-template-columns: 1fr;
        gap: 0.15rem 0;
      }
      .lead-company-card .lead-company-dl dt { margin-top: 0.45rem; }
      .lead-company-card .lead-company-dl dt:first-child { margin-top: 0; }
      .lead-company-card .lead-company-dl dd { margin-bottom: 0.1rem; }
    }
    .lead-col-hint {
      font-size: 0.72rem;
      color: #6b7785;
      line-height: 1.45;
      margin: 0 0 0.85rem;
    }
    .lead-map-row {
      display: flex;
      flex-wrap: wrap;
      gap: 0.5rem 1rem;
      align-items: center;
      margin: 0.5rem 0 0.75rem;
      font-size: 0.8125rem;
    }
    .lead-data-note {
      background: #141c24;
      border: 1px dashed #3d4d60;
      border-radius: 6px;
      padding: 0.55rem 0.65rem;
      font-size: 0.75rem;
      color: #8b96a3;
      line-height: 1.45;
      margin: 0 0 0.85rem;
    }
    .lead-col .hero { grid-template-columns: 1fr 1fr; }
    .pay-band-wrap { margin: 0 0 1rem 0; padding: 0.65rem 0.85rem; background: #101820; border: 1px solid #2a3340; border-radius: 6px; }
    .pay-band-hint { margin: 0 0 0.5rem 0; font-size: 0.78rem; line-height: 1.45; color: #8b96a3; }
    .pay-band-nav { font-size: 0.84rem; line-height: 1.6; }
    a.pay-band-tab { color: #58a6ff; margin-right: 0.35rem; }
    a.pay-band-tab--active { font-weight: 600; color: #c5ccd4; text-decoration: underline; }
    """


def _page_desk(
    profiles: list[dict],
    leads: list[sqlite3.Row],
    comp: list[sqlite3.Row],
    stats: dict,
    rendered_at: str,
    *,
    pay_band: str = "all",
    nav_base_path: str = "/",
    desk_universe_count: Optional[int] = None,
) -> str:
    banner = _stats_banner(stats, rendered_at)
    band_nav = _pay_band_nav_html(current=pay_band, base_path=nav_base_path)
    if (
        desk_universe_count is not None
        and (pay_band or "all").strip().lower() != "all"
    ):
        band_counts = (
            f"<p class='meta'>This pay-signal band: <b>{len(profiles)}</b> of "
            f"<b>{desk_universe_count}</b> desk lead(s).</p>"
        )
    else:
        band_counts = ""
    css = _shared_css()
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>WealthPipeline — lead desk</title>
  <style>{css}</style>
</head>
<body class="wide">
  <h1>WealthPipeline <span class="tag">lead desk · local</span></h1>
  <p class="meta">
    <a href="{html.escape('/finder' if nav_base_path == '/' else '/admin/finder')}">Lead finder</a> — filter by registrant HQ and SIC/NAICS (or summary keywords).
    SEC filing–native timing: <b>S-1</b> from RSS, then <b>10-K</b> for the <b>same CIKs</b> via SEC submissions (cross-reference). Data updates when you run <code>sync</code>.
    Database: <code>{html.escape(str(Path(database_path()).resolve()))}</code>
  </p>
  {banner}
  {band_nav}
  {band_counts}
  {_comp_missing_callout(stats)}
  <label class="sr" for="filter">Filter desk + audit tables</label>
  <input type="search" id="filter" placeholder="Company, person, or CIK…" autocomplete="off"/>
  {_desk_table(profiles, stats)}
  <details class="audit">
    <summary>Source rows (audit) — officers × filings and raw NEO comp lines</summary>
    {_leads_table(leads)}
    {_comp_table(comp)}
  </details>
  <script>
  (function() {{
    var input = document.getElementById('filter');
    if (input) {{
      input.addEventListener('input', function() {{
        var q = (input.value || '').toLowerCase().trim();
        document.querySelectorAll('#desk-body tr').forEach(function(tr) {{
          if (!q) {{ tr.classList.remove('hidden'); return; }}
          tr.classList.toggle('hidden', (tr.textContent || '').toLowerCase().indexOf(q) < 0);
        }});
        document.querySelectorAll('details.audit table tbody tr').forEach(function(tr) {{
          if (!q) {{ tr.classList.remove('hidden'); return; }}
          tr.classList.toggle('hidden', (tr.textContent || '').toLowerCase().indexOf(q) < 0);
        }});
      }});
    }}
    document.querySelectorAll('#desk-body tr.desk-row').forEach(function(tr) {{
      function go() {{
        var h = tr.getAttribute('data-href');
        if (h) window.location = h;
      }}
      tr.addEventListener('click', function(e) {{
        if (e.target.closest('a')) return;
        go();
      }});
      tr.addEventListener('keydown', function(e) {{
        if (e.key === 'Enter') {{ e.preventDefault(); go(); }}
      }});
    }});
    }})();
  </script>
  {_live_reload_snippet()}
</body>
</html>"""


def _canonical_external_url(raw: str, *, base: str = "") -> str:
    """
    Normalize user/link targets for href and img src.

    Relative-looking hosts (e.g. linkedin.com/in/...) resolve against the current app
    origin and fail; bare domains (wbinfra.com) need https://.
    """
    s = (raw or "").strip()
    if not s:
        return ""
    low = s.lower()
    if low.startswith(("javascript:", "data:", "vbscript:")):
        return ""
    if low.startswith("mailto:") or low.startswith("tel:"):
        return s
    if s.startswith("//"):
        return "https:" + s
    if s.startswith(("http://", "https://")):
        return s
    if s.startswith("/"):
        b = _canonical_external_url(base) if base else ""
        if b:
            return urljoin(b if b.endswith("/") else b + "/", s)
        return ""
    if "://" in s:
        return s
    host = s.split("/", 1)[0]
    if "." in host and " " not in host and not host.startswith("."):
        return "https://" + s.lstrip("/")
    return ""


def _filings_table_html(filings: list[dict]) -> str:
    if not filings:
        return "<p class='meta'>No linked filings in DB for this person.</p>"
    rows = []
    for f in filings:
        idx = html.escape(f.get("index_url") or "")
        doc = html.escape(f.get("primary_doc_url") or "")
        idx_l = f'<a href="{idx}" target="_blank" rel="noopener">EDGAR</a>' if idx else "—"
        doc_l = f'<a href="{doc}" target="_blank" rel="noopener">Doc</a>' if doc else "—"
        rows.append(
            "<tr>"
            f"<td>{html.escape(f.get('filing_date') or '')}</td>"
            f"<td>{html.escape(f.get('form_type') or '')}</td>"
            f"<td class='cik'>{html.escape(f.get('accession') or '')}</td>"
            f"<td>{idx_l} · {doc_l}</td>"
            "</tr>"
        )
    return (
        "<div class='table-wrap'><table><thead><tr>"
        "<th>Filing date</th><th>Form</th><th>Accession</th><th>Links</th>"
        "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table></div>"
    )


def _json_leaf_display_str(v: object) -> str:
    """Issuer snapshot values come from json.loads — may be int/float/bool, not only strings."""
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return ""
    s = str(v).strip()
    return s


def _issuer_snapshot_card_html(snap: dict) -> str:
    if not snap:
        return (
            '<div class="card advisor-company-snapshot">'
            '<h3 class="advisor-subh" style="margin-top:0">At a glance (enriched)</h3>'
            '<p class="meta" style="margin-bottom:0">'
            "<span class='dim'>Not generated yet.</span> Run "
            "<code>enrich-client-research</code> after issuer summary exists."
            "</p></div>"
        )
    parts: list[str] = []
    for key, title in [
        ("headline", "At a glance"),
        ("business_plain", "What they do"),
        ("pool_angle", "Why scale matters for outreach"),
    ]:
        v = _json_leaf_display_str(snap.get(key))
        if v:
            parts.append(
                f'<h3 class="advisor-subh">{html.escape(title)}</h3>'
                f'<p class="meta" style="margin-top:0.2rem">{html.escape(v)}</p>'
            )
    cave = _json_leaf_display_str(snap.get("caveat"))
    if cave:
        parts.append(f'<p class="meta dim" style="margin-top:0.65rem">{html.escape(cave)}</p>')
    body = "".join(parts) if parts else "<p class='meta dim'>Snapshot empty — re-run enrichment.</p>"
    return (
        '<div class="card advisor-company-snapshot">'
        f"{body}"
        "</div>"
    )


def _lead_company_public_card_html(
    p: dict,
    *,
    roster_stats: Optional[dict] = None,
) -> str:
    """Company panel: HQ, website, referral breadth (no SIC/NAICS — often blank or noisy)."""
    co = html.escape(p.get("company_name") or "—")
    cik = html.escape(str(p.get("cik") or ""))
    web_raw = (p.get("issuer_website") or "").strip()
    web_canon = _canonical_external_url(web_raw)
    if web_canon:
        web_dd = (
            f'<a href="{html.escape(web_canon)}" target="_blank" rel="noopener">'
            f"{html.escape(web_canon)}</a>"
        )
    else:
        web_dd = "<span class='dim'>—</span>"

    hq_txt = (p.get("issuer_headquarters") or "").strip()
    hq_loc = hq_city_state_display(hq_txt)
    if not hq_loc:
        mat = (p.get("issuer_hq_city_state") or "").strip()
        if mat and not hq_city_state_looks_like_filing_noise(mat):
            hq_loc = mat
    hq_detail_line = hq_principal_office_display_line(hq_txt)
    if hq_detail_line:
        addr_dd = html.escape(hq_detail_line)
    elif hq_loc:
        addr_dd = html.escape(hq_loc)
    elif hq_txt:
        addr_dd = (
            "<span class='dim'>Unclear from filing text — verify in the linked SEC filing.</span>"
        )
    else:
        addr_dd = "<span class='dim'>—</span>"

    ref_dd = "<span class='dim'>—</span>"
    if roster_stats:
        u = int(roster_stats.get("unique_people") or 0)
        if u > 0:
            ref_tip = (
                "Distinct officer/director names deduped from filing tables — sizing hint only, "
                "not wealth verification."
            )
            ref_dd = (
                f'<span title="{html.escape(ref_tip, quote=True)}">'
                f"<strong>{u}</strong> potential high-value referrals</span>"
            )

    rows: list[tuple[str, str]] = [
        ("Company", f"<strong>{co}</strong>"),
        ("CIK", f'<span class="cik">{cik}</span>'),
        ("Website", web_dd),
        ("Headquarters", addr_dd),
        ("Potential referrals", ref_dd),
    ]
    dl_parts = [
        f"<dt>{html.escape(label)}</dt><dd>{inner}</dd>" for label, inner in rows
    ]
    dl = '<dl class="lead-company-dl">' + "".join(dl_parts) + "</dl>"
    foot = (
        '<p class="meta dim" style="margin:0.45rem 0 0;font-size:0.72rem;line-height:1.4">'
        "Filing-based company context for trust, tax, and wealth work—verify in EDGAR.</p>"
    )
    return (
        '<div class="card lead-company-card" aria-label="Company details">'
        f"{dl}{foot}"
        "</div>"
    )


def _outreach_email_block_html(outreach: dict) -> str:
    if not outreach or outreach.get("error"):
        return ""
    dom = (outreach.get("domain") or "").strip()
    dsrc = (outreach.get("domain_source") or "").strip()
    on_site = outreach.get("emails_on_site") or []
    cands = outreach.get("candidates") or []
    bits: list[str] = []
    if dom:
        bits.append(
            f"<p class='meta' style='margin-top:0;margin-bottom:0.35rem'><strong>Corporate mail domain (hint):</strong> "
            f"<code>{html.escape(dom)}</code>"
            f"{(' — ' + html.escape(dsrc)) if dsrc else ''}</p>"
        )
    if on_site:
        esc = ", ".join(html.escape(x) for x in on_site[:8])
        bits.append(f"<p class='meta' style='margin-bottom:0.35rem'><strong>Addresses seen on site:</strong> {esc}</p>")
    if not cands:
        return "".join(bits)
    rows = []
    for c in cands:
        em = html.escape(c.get("email") or "")
        st = html.escape(str(c.get("smtp_status") or "—"))
        det = html.escape(str(c.get("smtp_detail") or "")[:120])
        rows.append(f"<tr><td><code>{em}</code></td><td>{st}</td><td class='dim'>{det}</td></tr>")
    bits.append(
        "<p class='meta' style='margin:0.5rem 0 0.25rem'><strong>Suggested address patterns</strong> "
        "<span class='dim'>(SMTP hints are often wrong; verify before sending)</span></p>"
        '<div class="table-wrap"><table class="outreach-email-table"><thead><tr>'
        "<th>Email</th><th>Check</th><th>Detail</th></tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table></div>"
    )
    return "".join(bits)


def _client_research_card_html(
    cr: Optional[dict], *, issuer_website: str = ""
) -> str:
    if cr is None:
        return (
            '<div class="card client-research-card lead-outreach-panel">'
            '<h2 class="lead-section-h">Background &amp; outreach</h2>'
            '<p class="meta" style="margin-bottom:0">'
            "<span class='dim'>Not generated yet.</span> Run <code>enrich-client-research</code> for photo, story, links, and email hints."
            "</p></div>"
        )
    st = (cr.get("status") or "").strip()
    person_story = (cr.get("person_story") or "").strip()
    outreach: dict = {}
    raw_o = cr.get("outreach_json")
    if raw_o:
        try:
            outreach = json.loads(raw_o) if isinstance(raw_o, str) else (raw_o or {})
        except json.JSONDecodeError:
            outreach = {}

    summ = html.escape((cr.get("research_summary") or "").strip())
    bio = html.escape((cr.get("bio_website") or "").strip())
    photo = (cr.get("photo_url") or "").strip()
    lpu = (cr.get("linkedin_profile_url") or "").strip()
    lsearch = (cr.get("linkedin_search_url") or "").strip()
    lpg = (cr.get("leadership_page_url") or "").strip()
    err = (cr.get("error_message") or "").strip()

    site_base = _canonical_external_url(issuer_website)
    photo_html = ""
    blob_raw = cr.get("photo_blob")
    mime_st = (cr.get("photo_mime") or "image/jpeg").strip() or "image/jpeg"
    if blob_raw:
        try:
            b = blob_raw if isinstance(blob_raw, (bytes, bytearray)) else bytes(blob_raw)
            if b:
                safe_mime = html.escape(mime_st, quote=True)
                b64 = base64.b64encode(b).decode("ascii")
                data_uri = f"data:{safe_mime};base64,{b64}"
                photo_html = (
                    '<div class="research-photo-wrap">'
                    f'<img class="research-photo" src="{data_uri}" alt="" loading="lazy"/>'
                    "</div>"
                )
        except (TypeError, ValueError, OSError):
            photo_html = ""
    if not photo_html and photo:
        pic = _canonical_external_url(photo, base=site_base) or photo
        pe = html.escape(pic)
        photo_html = (
            '<div class="research-photo-wrap">'
            f'<img class="research-photo" src="{pe}" alt="" referrerpolicy="no-referrer" loading="lazy"/>'
            "</div>"
        )

    story_html = ""
    if person_story:
        story_html = (
            '<h3 class="advisor-subh" style="margin-top:0">Advisor-ready story</h3>'
            f'<p class="person-story-prose">{html.escape(person_story)}</p>'
        )

    body_parts: list[str] = []
    if story_html:
        body_parts.append(story_html)
    if summ:
        body_parts.append(
            f'<h3 class="advisor-subh">Website pull (short)</h3><p class="meta">{summ}</p>'
        )
    if bio and not person_story:
        body_parts.append(f"<p class='meta' style='margin-bottom:0'>{bio}</p>")

    links: list[str] = []
    if lpg:
        lh = _canonical_external_url(lpg, base=site_base) or lpg
        links.append(
            f'<a href="{html.escape(lh)}" target="_blank" rel="noopener">Leadership page</a>'
        )
    if lpu:
        lu = _canonical_external_url(lpu, base=site_base) or lpu
        links.append(
            f'<a href="{html.escape(lu)}" target="_blank" rel="noopener">LinkedIn (linked on site)</a>'
        )
    if lsearch:
        ls = _canonical_external_url(lsearch) or lsearch
        links.append(
            f'<a href="{html.escape(ls)}" target="_blank" rel="noopener">LinkedIn search</a>'
        )
    links_html = ""
    if links:
        links_html = (
            '<h3 class="advisor-subh">How to find them</h3>'
            '<div class="client-research-links">'
            + " · ".join(links)
            + "</div>"
        )

    outreach_html = _outreach_email_block_html(outreach)
    if outreach_html:
        outreach_html = '<h3 class="advisor-subh">Contact hypotheses</h3>' + outreach_html

    err_html = ""
    if err and st in ("partial", "error", "skipped"):
        err_html = f'<p class="meta"><span class="dim">{html.escape(err)}</span></p>'

    status_note = ""
    if st == "partial":
        status_note = "<p class='meta dim' style='margin-top:0.35rem'>Partial — verify on issuer site.</p>"
    elif st == "skipped" and (person_story or outreach_html):
        status_note = "<p class='meta dim' style='margin-top:0.35rem'>No company website; S-1 story / domain hints may still apply.</p>"

    inner = photo_html + "".join(body_parts) + links_html + outreach_html + err_html + status_note
    if not inner.strip():
        inner = (
            "<p class='meta dim' style='margin-bottom:0'>No website match yet.</p>" + links_html + outreach_html
        )

    return (
        '<div class="card client-research-card lead-outreach-panel">'
        '<h2 class="lead-section-h">Background &amp; outreach</h2>'
        f"{inner}"
        "</div>"
    )


def _page_lead(
    profile: Optional[dict],
    filings: list[dict],
    *,
    query_cik: str,
    query_name: str,
    stats: dict,
    rendered_at: str,
    client_research: Optional[dict] = None,
    issuer_snapshot: Optional[dict] = None,
    roster_stats: Optional[dict] = None,
) -> str:
    css = _shared_css()
    if profile is None:
        q = html.escape(urlencode({"cik": query_cik, "name": query_name}))
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Lead not found — WealthPipeline</title>
  <style>{css}</style>
</head>
<body>
  <nav class="top"><a href="/">← Lead desk</a> · <a href="/finder">Lead finder</a></nav>
  <h1>Profile not found</h1>
  <p class="meta">No matching executive + CIK in your snapshot. Check <code>cik</code> and <code>name</code> query params, run <code>sync</code>, or return to the desk.</p>
  <p class="meta">Requested: CIK <code>{html.escape(query_cik)}</code>, name <code>{html.escape(query_name)}</code></p>
  <p class="meta"><a href="/?">Back to lead desk</a> · <a href="/lead?{q}">Retry this URL</a></p>
</body>
</html>"""

    p = profile
    doc_u = p.get("primary_doc_url") or ""
    doc_e = html.escape(doc_u)
    doc_link = (
        f'<a href="{doc_e}" target="_blank" rel="noopener">Open primary filing doc</a>'
        if doc_u
        else ""
    )

    lt = p.get("lead_tier") or _profile_lead_tier(p)
    if lt == "premium":
        tier_badge = (
            '<span class="badge badge-tier-premium" title="Summary comp meets the desk pay-signal bar '
            '(or the bar is set to $0).">Premium</span>'
        )
    elif lt == "standard":
        tier_badge = (
            '<span class="badge badge-tier-standard" title="S-1 summary comp is below the desk pay bar.">'
            "Standard</span>"
        )
    else:
        tier_badge = (
            '<span class="badge badge-tier-visibility" title="Officer/director table; no summary comp row '
            "in this database.\">Visibility</span>"
        )

    dts_body = (p.get("director_term_summary") or "").strip()
    director_card = f"""
    <div class="card">
      <h2 class="lead-section-h">Director &amp; board terms</h2>
      <p class="meta" style="margin-bottom:0">{html.escape(dts_body) if dts_body else "<span class='dim'>Not extracted yet — run <code>backfill-comp --force</code> after updating the parser.</span>"}</p>
    </div>"""

    mb = (p.get("mgmt_bio_text") or "").strip()
    mb_role = (p.get("mgmt_bio_role") or "").strip()
    mb_name = (p.get("mgmt_bio_display_name") or "").strip()
    disp = (p.get("display_name") or "").strip()
    if mb:
        paras = [html.escape(x.strip()) for x in mb.split("\n\n") if x.strip()]
        paras_html = "".join(
            f"<p class='meta' style='margin-top:0.35rem; margin-bottom:0'>{x}</p>"
            for x in paras
        )
        heading_line = mb_name or disp
        role_suffix = f" — {html.escape(mb_role)}" if mb_role else ""
        mgmt_narrative_card = f"""
    <div class="card">
      <h2 class="lead-section-h">Management biography</h2>
      <p class="meta" style="margin-bottom:0"><strong>{html.escape(heading_line)}</strong>{role_suffix}</p>
      {paras_html}
    </div>"""
    else:
        mgmt_narrative_card = f"""
    <div class="card bio-placeholder">
      <h2 class="lead-section-h">Management biography</h2>
      <p class="meta" style="margin-bottom:0">
        <span class='dim'>No narrative block stored for this executive yet.</span>
        Re-sync or run <code>backfill-comp --force</code> to re-parse the <b>Executive Officers and Directors</b> prose (name, role, age context, prior positions in narrative form).
        {(' ' + doc_link) if doc_link else ''}
      </p>
    </div>"""

    bookmark_q = urlencode(
        {"cik": str(p.get("cik") or ""), "name": p.get("display_name") or ""}
    )

    oa = p.get("officer_age")
    stated = p.get("age_stated_in_filing")
    anchor = (p.get("age_anchor_date") or "").strip()
    oat = p.get("officer_age_from_table")
    nar = p.get("narrative_age")
    age_hero_val = "—"
    age_hero_title = ""
    if oa is not None:
        try:
            age_n = int(oa)
            age_hero_val = str(age_n)
            if oat is not None:
                src = "Officers table"
            elif nar is not None:
                src = "Management narrative"
            else:
                src = "Filing-derived"
            age_hero_title = src
            if stated is not None and anchor:
                try:
                    st_i = int(stated)
                    esc_a = html.escape(anchor)
                    if age_n != st_i:
                        age_hero_title = (
                            f"{src}. Filing stated {st_i} as of {esc_a}; {age_n} adds calendar years to today."
                        )
                    else:
                        age_hero_title = f"{src}. As of filing date {esc_a}."
                except (TypeError, ValueError):
                    pass
        except (TypeError, ValueError):
            age_hero_val = "—"
            age_hero_title = ""
    else:
        age_hero_title = "Not in officers table or narrative — run backfill if missing."
    hq_txt = (p.get("issuer_headquarters") or "").strip()
    hq_loc = hq_city_state_display(hq_txt)
    if not hq_loc:
        mat = (p.get("issuer_hq_city_state") or "").strip()
        if mat and not hq_city_state_looks_like_filing_noise(mat):
            hq_loc = mat
    hq_loc_esc = html.escape(hq_loc) if hq_loc else ""
    web_raw = (p.get("issuer_website") or "").strip()
    summ_body = (p.get("issuer_summary") or "").strip()
    summ_html = (
        html.escape(summ_body)
        if summ_body
        else "<span class='dim'>Not extracted — run <code>sync</code> or <code>backfill-comp --force</code>.</span>"
    )

    age_title_attr = html.escape(age_hero_title, quote=True) if age_hero_title else ""

    snap_dict = issuer_snapshot or {}
    company_intro_card = _lead_company_public_card_html(
        p, roster_stats=roster_stats
    )
    snapshot_card = _issuer_snapshot_card_html(snap_dict)
    snapshot_folded = (
        '<details class="lead-more lead-snapshot-fold">'
        "<summary>Company snapshot <span class='dim'>(enriched story, if available)</span></summary>"
        f'<div class="lead-snapshot-inner">{snapshot_card}</div></details>'
    )

    hq_one = _hq_one_line_for_maps(hq_txt)
    co_nm = (p.get("company_name") or "").strip()
    map_query = (hq_loc or hq_one or (f"{co_nm} headquarters" if co_nm else ""))
    maps_link = ""
    if map_query.strip():
        maps_url = "https://www.google.com/maps/search/?api=1&query=" + quote(
            map_query, safe=""
        )
        maps_link = (
            f'<a href="{html.escape(maps_url)}" target="_blank" rel="noopener">Map</a>'
        )

    idx_u = (p.get("index_url") or "").strip()
    idx_link = (
        f'<a href="{html.escape(idx_u)}" target="_blank" rel="noopener">EDGAR index</a>'
        if idx_u
        else ""
    )
    quick_links = " · ".join(
        x for x in (idx_link, doc_link, maps_link) if x
    )

    loc_display = (
        hq_loc_esc
        if hq_loc_esc
        else "<span class='dim'>—</span>"
    )
    age_ttl = f' title="{age_title_attr}"' if age_title_attr else ""
    age_inner = (
        html.escape(age_hero_val)
        if age_hero_val != "—"
        else "<span class='dim'>—</span>"
    )
    hero_kv = f"""
    <div class="lead-hero-kv" role="group" aria-label="Key facts">
      <div class="lead-kv"><span class="lead-kv-l">Age</span>
        <span class="lead-kv-v"{age_ttl}>{age_inner}</span></div>
      <div class="lead-kv"><span class="lead-kv-l">HQ location</span>
        <span class="lead-kv-v">{loc_display}</span></div>
      <div class="lead-kv"><span class="lead-kv-l">Latest filing</span>
        <span class="lead-kv-v">{html.escape(p.get('filing_date') or '—')}</span></div>
      <div class="lead-kv"><span class="lead-kv-l">Desk tier</span>
        <span class="lead-kv-v">{tier_badge}</span></div>
    </div>"""

    why_card = f"""
    <div class="card lead-why-card">
      <h2 class="lead-section-h">Why surfaced</h2>
      <p class="meta" style="margin-bottom:0">{html.escape(p.get('why_surfaced') or '—')}</p>
    </div>"""

    research_card = _client_research_card_html(
        client_research, issuer_website=web_raw
    )

    filings_block = f"""
    <h3 class="lead-section-h" style="margin-top:0.75rem">Issuer filings</h3>
    <p class="meta dim" style="font-size:0.78rem">S-1 / 10-K for this CIK in your DB (newest first).</p>
    {_filings_table_html(filings)}
    """

    source_details = f"""
    <details class="lead-more">
      <summary>More SEC source detail</summary>
      <p class="meta" style="margin-top:0.65rem; margin-bottom:0"><strong>Issuer summary (extracted)</strong></p>
      <p class="meta" style="margin-top:0.35rem">{summ_html}</p>
      {director_card}
      {filings_block}
    </details>"""

    col_company = f"""
    <aside class="lead-col lead-col-company" aria-labelledby="lead-col-company">
      <div class="lead-col-title" id="lead-col-company">Company</div>
      {company_intro_card}
      {snapshot_folded}
      {source_details}
    </aside>"""

    comp_fullwidth = f"""
    <section class="lead-comp-fullwidth" aria-label="Summary compensation from filing">
      <div class="lead-comp-block">
      {_profile_lead_compensation_card_html(p)}
      </div>
    </section>"""

    col_person = f"""
    <main class="lead-col lead-col-person" aria-labelledby="lead-col-person">
      <div class="lead-col-title" id="lead-col-person">Person</div>
      <p class="lead-data-note">Filings give registrant address only — not a personal home address or phone.</p>
      {why_card}
      {research_card}
      <details class="lead-more">
        <summary>Management biography (filing text)</summary>
        <div class="lead-mgmt-bio-wrap" style="margin-top:0.5rem">
      {mgmt_narrative_card}
        </div>
      </details>
    </main>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{html.escape(p.get('display_name') or 'Lead')} — WealthPipeline</title>
  <style>{css}</style>
</head>
<body class="detail-page lead-profile-page">
  <header class="lead-page-header">
    <nav class="top"><a href="/">← Lead desk</a> · <a href="/finder">Lead finder</a></nav>
    <h1>{html.escape(p.get('display_name') or '—')}</h1>
    <p class="lead-hero-line">
      <strong title="{html.escape((p.get('title') or '—').strip() or '—', quote=True)}">{html.escape(advisor_title_badge((p.get('title') or '').strip() or '—'))}</strong>
      <span class="lead-hero-sep">·</span>
      <span>{html.escape(p.get('company_name') or '—')}</span>
      <span class="lead-hero-sep">·</span>
      <span class="cik">CIK {html.escape(str(p.get('cik') or ''))}</span>
    </p>
    {hero_kv}
    <p class="lead-hero-links meta">{quick_links if quick_links else "<span class='dim'>No EDGAR links on file</span>"}</p>
  </header>
  {comp_fullwidth}
  <div class="lead-split" role="presentation">
    {col_company}
    {col_person}
  </div>
  <p class="meta dim" style="font-size:0.78rem;margin-top:1rem">Bookmark: <code>/lead?{html.escape(bookmark_q)}</code></p>
  {_live_reload_snippet()}
</body>
</html>"""


def _load_page_data() -> tuple[list[dict], list[dict], list[sqlite3.Row], list[sqlite3.Row], dict]:
    dbp = database_path()
    if not Path(dbp).is_file():
        empty = {"missing_db": True, "profile_count": 0, "profile_count_all": 0}
        return [], [], [], [], empty

    mtime = datetime.fromtimestamp(Path(dbp).stat().st_mtime).strftime("%Y-%m-%d %H:%M")

    with connect() as conn:
        profiles_all = _build_profiles(conn)
        profiles = _lead_desk_filter_profiles(profiles_all)

        if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='neo_compensation'"
        ).fetchone():
            cur = conn.execute(
                """
                SELECT f.company_name, f.cik, f.filing_date, o.name, o.title,
                       f.index_url, f.primary_doc_url,
                       NULL AS comp_year, NULL AS comp_salary, NULL AS comp_bonus,
                       NULL AS comp_stock, NULL AS comp_total, NULL AS comp_equity
                FROM filings f
                LEFT JOIN officers o ON o.filing_id = f.id
                ORDER BY f.filing_date DESC, f.company_name, o.name
                """
            )
        else:
            cur = conn.execute(
                """
                SELECT f.company_name, f.cik, f.filing_date, o.name, o.title,
                       f.index_url, f.primary_doc_url,
                       nc.fiscal_year AS comp_year,
                       nc.salary AS comp_salary,
                       nc.bonus AS comp_bonus,
                       nc.stock_awards AS comp_stock,
                       nc.total AS comp_total,
                       nc.equity_comp_disclosed AS comp_equity
                FROM filings f
                LEFT JOIN officers o ON o.filing_id = f.id
                LEFT JOIN neo_compensation nc ON nc.id = (
                    SELECT c.id FROM neo_compensation c
                    WHERE c.filing_id = f.id
                    AND o.name IS NOT NULL
                    AND lower(trim(replace(replace(c.person_name, '.', ''), '  ', ' '))) =
                        lower(trim(replace(replace(o.name, '.', ''), '  ', ' ')))
                    ORDER BY c.fiscal_year DESC LIMIT 1
                )
                ORDER BY f.filing_date DESC, f.company_name, o.name
                """
            )
        leads = list(cur.fetchall())

        comp: list[sqlite3.Row] = []
        if conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='neo_compensation'"
        ).fetchone():
            cur = conn.execute(
                """
                SELECT f.company_name, f.primary_doc_url, c.person_name, c.role_hint,
                       c.fiscal_year, c.salary, c.bonus, c.stock_awards, c.option_awards,
                       c.other_comp, c.total, c.equity_comp_disclosed
                FROM neo_compensation c
                JOIN filings f ON f.id = c.filing_id
                ORDER BY f.filing_date DESC, f.company_name, c.person_name, c.fiscal_year DESC
                """
            )
            comp = list(cur.fetchall())

        nf = int(conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0])
        no = int(conn.execute("SELECT COUNT(*) FROM officers").fetchone()[0])
        if conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='neo_compensation'"
        ).fetchone():
            nc = int(conn.execute("SELECT COUNT(*) FROM neo_compensation").fetchone()[0])
        else:
            nc = 0
        latest = conn.execute(
            "SELECT MAX(filing_date) FROM filings"
        ).fetchone()[0]

    stats = {
        "missing_db": False,
        "filings": nf,
        "officers": no,
        "comp_rows": nc,
        "profile_count": len(profiles),
        "profile_count_all": len(profiles_all),
        "lead_desk_s1_only": lead_desk_s1_only(),
        "lead_desk_min_signal_usd": lead_desk_min_signal_usd(),
        "lead_desk_equity_only_legacy": lead_desk_equity_only_min_usd(),
        "latest_filing_date": latest,
        "db_file_modified": mtime,
    }
    return profiles, profiles_all, leads, comp, stats


def _is_advisor_only_path(pi: str) -> bool:
    """Paths that exist on the FastAPI advisor app, not this legacy WSGI desk."""
    if pi in ("/login", "/register", "/my-leads", "/watchlist", "/healthz"):
        return True
    if pi.startswith("/admin") or pi.startswith("/export/my-leads"):
        return True
    return False


def _advisor_redirect_help_html() -> str:
    return """<!DOCTYPE html><html><head><meta charset="utf-8"/><title>Wrong server — WealthPipeline</title>
<style>body{font-family:system-ui;background:#0d1117;color:#e8ecf0;max-width:36rem;margin:2rem auto;padding:1rem;line-height:1.5}
a{color:#5eb3e0}code{background:#1a2634;padding:0.1rem 0.35rem;border-radius:4px}</style></head><body>
<h1>This is the legacy lead desk server</h1>
<p>It does not serve <strong>/login</strong> or the advisor UI. You are probably running
<code>python -m wealth_leads serve</code> on the same port as the advisor app.</p>
<p><strong>Fix:</strong> Close this server window, then start the advisor app:</p>
<ul>
<li>Double-click <code>Start WealthPipeline Dashboard.bat</code> in the project folder, or</li>
<li>Run <code>py -3 serve_advisor.py</code> from the project root.</li>
</ul>
<p>Then open <a href="http://127.0.0.1:8765/login">http://127.0.0.1:8765/login</a></p>
<p style="margin-top:1.5rem;font-size:0.85rem;color:#8b96a3">Tip: the legacy desk (no sign-in) defaults to port <strong>8766</strong> so it does not
take over 8765. Run <code>python -m wealth_leads serve</code> or set <code>WEALTH_LEADS_PORT</code>.</p>
</body></html>"""


def _app(environ, start_response):
    path = environ.get("PATH_INFO") or "/"
    pi = path.rstrip("/") or "/"

    if pi == "/__dev/state":
        body = _dev_state_body()
        start_response(
            "200 OK",
            [
                ("Content-Type", "application/json; charset=utf-8"),
                ("Content-Length", str(len(body))),
                ("Cache-Control", "no-store"),
            ],
        )
        return [body]

    if pi == "/export/finder.csv":
        qs = parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=True)
        hq = (qs.get("hq") or [""])[0].strip()
        industry = (qs.get("industry") or [""])[0].strip()
        qtxt = (qs.get("q") or [""])[0].strip()
        all_neo = (qs.get("all_neo") or [""])[0] in ("1", "on", "true", "True")
        band = ((qs.get("band") or ["all"])[0] or "all").strip() or "all"
        profiles, profiles_all, _leads, _comp, _stats = _load_page_data()
        body = finder_export_csv_bytes(
            profiles_all=profiles_all,
            profiles_desk=profiles,
            hq=hq,
            industry=industry,
            q=qtxt,
            all_neo=all_neo,
            pay_band=band,
        )
        start_response(
            "200 OK",
            [
                ("Content-Type", "text/csv; charset=utf-8"),
                ("Content-Length", str(len(body))),
                (
                    "Content-Disposition",
                    'attachment; filename="wealthpipeline-finder.csv"',
                ),
                ("Cache-Control", "no-store"),
            ],
        )
        return [body]

    if _is_advisor_only_path(pi):
        body = _advisor_redirect_help_html().encode("utf-8")
        start_response(
            "200 OK",
            [
                ("Content-Type", "text/html; charset=utf-8"),
                ("Content-Length", str(len(body))),
                ("Cache-Control", "no-store"),
            ],
        )
        return [body]

    if pi not in ("/", "/lead", "/finder"):
        start_response("404 Not Found", [("Content-Type", "text/plain; charset=utf-8")])
        return [b"Not Found"]

    profiles, profiles_all, leads, comp, stats = _load_page_data()
    rendered_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if pi == "/finder":
        qs = parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=True)
        hq = (qs.get("hq") or [""])[0].strip()
        industry = (qs.get("industry") or [""])[0].strip()
        qtxt = (qs.get("q") or [""])[0].strip()
        all_neo = (qs.get("all_neo") or [""])[0] in ("1", "on", "true", "True")
        band = ((qs.get("band") or ["all"])[0] or "all").strip() or "all"
        base = profiles_all if all_neo else profiles
        filtered = filter_profiles_geo_industry_text(
            base,
            location_sub=hq,
            industry_sub=industry,
            text_sub=qtxt,
        )
        filtered = filter_profiles_pay_band(filtered, band)
        html_out = _page_finder(
            filtered,
            stats=stats,
            rendered_at=rendered_at,
            hq=hq,
            industry=industry,
            q=qtxt,
            all_neo=all_neo,
            base_count=len(base),
            pay_band=band,
        )
    elif pi == "/lead":
        qs = parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=True)
        cik = (qs.get("cik") or [""])[0].strip()
        name_raw = (qs.get("name") or [""])[0]
        name_decoded = unquote(name_raw) if name_raw else ""
        norm = _norm_person_name(name_decoded)
        prof = _find_profile(profiles_all, cik, norm) if not stats.get("missing_db") else None
        filings: list[dict] = []
        cr_dict: Optional[dict] = None
        issuer_snap: Optional[dict] = None
        roster_stats_out: Optional[dict] = None
        if prof is not None and not stats.get("missing_db"):
            with connect() as conn:
                filings = _filings_for_profile(conn, cik, norm)
                cr_row = get_lead_client_research(conn, cik, norm)
                cr_dict = row_to_client_research_dict(cr_row)
                issuer_snap = get_issuer_snapshot_dict(conn, cik)
                roster_stats_out = management_roster_scale_stats(
                    conn, (cik or "").strip()
                )
        html_out = _page_lead(
            prof,
            filings,
            query_cik=cik,
            query_name=name_decoded,
            stats=stats,
            rendered_at=rendered_at,
            client_research=cr_dict,
            issuer_snapshot=issuer_snap,
            roster_stats=roster_stats_out,
        )
    else:
        qs = parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=True)
        band = ((qs.get("band") or ["all"])[0] or "all").strip() or "all"
        desk_view = filter_profiles_pay_band(profiles, band)
        html_out = _page_desk(
            desk_view,
            leads,
            comp,
            stats,
            rendered_at,
            pay_band=band,
            nav_base_path="/",
            desk_universe_count=len(profiles),
        )

    body = html_out.encode("utf-8")
    start_response(
        "200 OK",
        [
            ("Content-Type", "text/html; charset=utf-8"),
            ("Content-Length", str(len(body))),
            ("Cache-Control", "no-store, no-cache, must-revalidate"),
            ("Pragma", "no-cache"),
        ],
    )
    return [body]


def _open_browser_when_ready(url: str, delay_sec: float = 0.8) -> None:
    def _go() -> None:
        if sys.platform == "win32":
            try:
                os.startfile(url)
                return
            except OSError:
                pass
        webbrowser.open(url)

    threading.Timer(delay_sec, _go).start()


def run_localhost(
    *,
    port: int | None = None,
    open_browser: bool = True,
    live: bool = True,
    reload: bool = False,
) -> None:
    # Default 8766 so the FastAPI advisor can own 8765 (serve_advisor / Start … .bat).
    p = port or int(os.environ.get("WEALTH_LEADS_PORT", "8766"))
    if live:
        os.environ["WEALTH_LEADS_LIVE_RELOAD"] = "1"
    else:
        os.environ["WEALTH_LEADS_LIVE_RELOAD"] = "0"

    if reload and os.environ.get(_SERVE_CHILD_ENV) != "1":
        _spawn_reload_watch_loop(port=p, open_browser=open_browser, live=live)
        return

    url = f"http://127.0.0.1:{p}/"
    try:
        httpd = make_server("127.0.0.1", p, _app)
    except OSError as e:
        print(f"Could not listen on {url} (port {p}): {e}", file=sys.stderr)
        print("Another copy may be running, or the port is in use.", file=sys.stderr)
        raise SystemExit(1) from e
    print(f"WealthPipeline legacy desk (/, /lead, /finder): {url}")
    print(
        "Advisor app with /login is separate — use serve_advisor.py or Start WealthPipeline Dashboard.bat (port 8765).",
        file=sys.stderr,
    )
    if _want_live_reload():
        print(
            "Live refresh: tab reloads when the database changes or the server restarts "
            "(disable with --no-live or WEALTH_LEADS_LIVE_RELOAD=0).",
            file=sys.stderr,
        )
    print("Press Ctrl+C to stop.")
    if open_browser:
        print("Opening your browser in a moment…")
        _open_browser_when_ready(url)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
