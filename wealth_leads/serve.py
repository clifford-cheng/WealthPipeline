from __future__ import annotations

import csv
import html
import io
import json
import math
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
from urllib.parse import parse_qs, quote, unquote, urlencode
from wsgiref.simple_server import make_server

from wealth_leads.config import (
    database_path,
    lead_desk_equity_only_min_usd,
    lead_desk_min_signal_usd,
    lead_desk_s1_only,
)
from wealth_leads.db import connect
from wealth_leads.management import issuer_summary_looks_spammy, why_surfaced_line
from wealth_leads.management_bios import extract_age_from_bio_text

_SERVE_CHILD_ENV = "WEALTH_LEADS_SERVE_CHILD"
# Below this high-water SCT total, label comp as "minimal" (typical director fees, etc.).
_ROSTER_MEANINGFUL_COMP_USD = 25_000.0
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


def _build_profiles(conn: sqlite3.Connection) -> list[dict]:
    """One row per (CIK, person): headline = latest fiscal year; sums + per-year breakdown for drill-down."""
    if not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='neo_compensation'"
    ).fetchone():
        return []

    cur = conn.execute(
        """
        SELECT c.person_name, c.role_hint, c.fiscal_year,
               c.salary, c.bonus, c.stock_awards, c.option_awards, c.other_comp,
               c.total, c.equity_comp_disclosed,
               f.id AS filing_id, f.company_name, f.cik, f.filing_date,
               f.index_url, f.primary_doc_url, f.form_type AS filing_form_type,
               f.issuer_summary AS filing_issuer_summary,
               f.issuer_website AS filing_issuer_website,
               f.issuer_headquarters AS filing_issuer_headquarters,
               f.issuer_industry AS filing_issuer_industry,
               f.director_term_summary AS filing_director_term_summary
        FROM neo_compensation c
        JOIN filings f ON f.id = c.filing_id
        """
    )
    raw = [dict(r) for r in cur.fetchall()]
    if not raw:
        return []

    narr_map: dict[tuple[int, str], dict] = {}
    fids_neo = {int(r["filing_id"]) for r in raw}
    if fids_neo and conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='person_management_narrative'"
    ).fetchone():
        qm = ",".join("?" * len(fids_neo))
        ncur = conn.execute(
            f"""
            SELECT filing_id, person_name_norm, person_name, role_heading, bio_text
            FROM person_management_narrative
            WHERE filing_id IN ({qm})
            """,
            tuple(fids_neo),
        )
        for nr in ncur.fetchall():
            narr_map[(int(nr["filing_id"]), nr["person_name_norm"] or "")] = dict(nr)

    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in raw:
        groups[_profile_key(row["cik"], row["person_name"])].append(row)

    ciks = {str(r["cik"] or "").strip() for r in raw if str(r.get("cik") or "").strip()}
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
            onorm = _norm_person_name(o["name"] or "")
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
                    "other_comp": r.get("other_comp"),
                    "total": r.get("total"),
                    "equity_comp_disclosed": r.get("equity_comp_disclosed"),
                    "filing_date": r.get("filing_date") or "",
                    "primary_doc_url": r.get("primary_doc_url") or "",
                }
            )

        pn = _norm_person_name(head["person_name"] or "")
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
        issuer_hq = (head.get("filing_issuer_headquarters") or "").strip()
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
        if not issuer_hq and cik_s:
            rhq = conn.execute(
                """
                SELECT issuer_headquarters FROM filings
                WHERE cik = ? AND issuer_headquarters IS NOT NULL
                  AND TRIM(issuer_headquarters) != ''
                ORDER BY COALESCE(filing_date, '') DESC LIMIT 1
                """,
                (cik_s,),
            ).fetchone()
            issuer_hq = (rhq[0] or "").strip() if rhq else ""

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

        profiles.append(
            {
                "norm_name": key[1],
                "display_name": head["person_name"] or "—",
                "company_name": head["company_name"] or "",
                "cik": head["cik"] or "",
                "title": title_guess or "—",
                "headline_year": head["fiscal_year"],
                "salary": head["salary"],
                "bonus": head["bonus"],
                "stock_awards": head["stock_awards"],
                "option_awards": head.get("option_awards"),
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
                "issuer_industry": issuer_ind,
                "mgmt_bio_role": (mgmt_nar or {}).get("role_heading") or "",
                "mgmt_bio_text": (mgmt_nar or {}).get("bio_text") or "",
                "mgmt_bio_display_name": (mgmt_nar or {}).get("person_name") or "",
                "director_term_summary": dts,
            }
        )

    profiles.sort(
        key=lambda p: (p["filing_date"] or "", p["headline_year"] or 0, p["total"] or 0),
        reverse=True,
    )
    return profiles


def _lead_desk_filter_profiles(profiles: list[dict]) -> list[dict]:
    """
    Narrow desk to S-1-linked NEO rows and a minimum pay bar (configurable).
    Default: best single FY max(SCT total, stock+options) >= threshold.
    Legacy: if WEALTH_LEADS_LEAD_DESK_MIN_EQUITY_USD is set, only stock+options are compared.
    """
    s1_only = lead_desk_s1_only()
    min_bar = lead_desk_min_signal_usd()
    equity_only = lead_desk_equity_only_min_usd()
    out: list[dict] = []
    for p in profiles:
        if s1_only and not p.get("has_s1_comp"):
            continue
        if min_bar > 0:
            if equity_only:
                v = float(p.get("equity_hwm") or 0)
            else:
                v = float(p.get("signal_hwm") or 0)
            if v < min_bar:
                continue
        out.append(p)
    out.sort(
        key=lambda p: (
            float(p.get("signal_hwm") or 0),
            p.get("filing_date") or "",
            p.get("headline_year") or 0,
            p.get("total") or 0,
        ),
        reverse=True,
    )
    return out


def filter_profiles_geo_industry_text(
    profiles: list[dict],
    *,
    location_sub: str = "",
    industry_sub: str = "",
    text_sub: str = "",
) -> list[dict]:
    """
    Server-side filters for RIA-style lookup: registrant HQ text, SIC/NAICS/summary keywords,
    and free-text across person / company / CIK.
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
) -> bytes:
    base = profiles_all if all_neo else profiles_desk
    filtered = filter_profiles_geo_industry_text(
        base,
        location_sub=hq,
        industry_sub=industry,
        text_sub=q,
    )
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
                _profile_lead_url(p),
                p.get("index_url") or "",
                p.get("primary_doc_url") or "",
            ]
        )
    return buf.getvalue().encode("utf-8")


def _profile_breakdown_table(p: dict) -> str:
    yb = p.get("year_breakdown") or []
    if not yb:
        return "<p class='bd-note'>No fiscal-year rows.</p>"
    parts: list[str] = [
        "<p class='bd-note'><strong>Year-by-year</strong> — from the filing summary compensation table. "
        "Each FY uses the row from the <b>latest amendment</b> in your DB if duplicates exist. "
        "Equity columns are usually grant-date fair value, not cash. <strong>Total</strong> is the issuer’s SCT total for that year.</p>",
        "<table class='inner-comp'><thead><tr>",
        "<th>FY</th><th>Salary</th><th>Bonus</th><th>Stock</th><th>Options</th><th>Other</th>",
        "<th>Equity Σ</th><th>Total</th><th>Filing</th><th>Doc</th>",
        "</tr></thead><tbody>",
    ]
    for y in yb:
        doc = html.escape(y.get("primary_doc_url") or "")
        doc_l = f'<a href="{doc}" target="_blank" rel="noopener">S-1</a>' if doc else "—"
        parts.append(
            "<tr>"
            f"<td class='num'>{y['fiscal_year']}</td>"
            f"<td class='num'>{_money(y.get('salary'))}</td>"
            f"<td class='num'>{_money(y.get('bonus'))}</td>"
            f"<td class='num'>{_money(y.get('stock_awards'))}</td>"
            f"<td class='num'>{_money(y.get('option_awards'))}</td>"
            f"<td class='num'>{_money(y.get('other_comp'))}</td>"
            f"<td class='num'>{_money(y.get('equity_comp_disclosed'))}</td>"
            f"<td class='num strong'>{_money(y.get('total'))}</td>"
            f"<td>{html.escape(y.get('filing_date') or '')}</td>"
            f"<td>{doc_l}</td>"
            "</tr>"
        )
    parts.append("</tbody></table>")
    return "".join(parts)


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


def _title_suggests_director_role(title: str) -> bool:
    """Rough heuristic: title text skews toward outside / independent directors."""
    t = re.sub(r"\s+", " ", (title or "").strip().lower())
    if not t:
        return False
    needles = (
        "director",
        "board member",
        "audit committee",
        "compensation committee",
        "nominating committee",
        "chair of the board",
        "chairman of the board",
        "vice chair",
        "lead director",
    )
    if "independent" in t and "director" in t:
        return True
    return any(n in t for n in needles)


def _neo_stats_by_norm_for_cik(
    conn: sqlite3.Connection, cik: str
) -> dict[str, dict[str, Any]]:
    """Per normalized person name: NEO row count, high-water total, latest FY (any filing for CIK)."""
    ck = (cik or "").strip()
    out: dict[str, dict[str, Any]] = {}
    if not ck:
        return out
    if not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='neo_compensation'"
    ).fetchone():
        return out
    for r in conn.execute(
        """
        SELECT c.person_name, c.fiscal_year, c.total
        FROM neo_compensation c
        JOIN filings f ON f.id = c.filing_id
        WHERE f.cik = ?
        """,
        (ck,),
    ).fetchall():
        pn = _norm_person_name(r["person_name"] or "")
        if not pn:
            continue
        if pn not in out:
            out[pn] = {"max_total": None, "row_count": 0, "latest_fy": None}
        st = out[pn]
        st["row_count"] += 1
        fy = r["fiscal_year"]
        if fy is not None:
            try:
                fy_i = int(fy)
                if st["latest_fy"] is None or fy_i > st["latest_fy"]:
                    st["latest_fy"] = fy_i
            except (TypeError, ValueError):
                pass
        tot = r["total"]
        if tot is None:
            continue
        try:
            fv = float(tot)
            if math.isnan(fv):
                continue
        except (TypeError, ValueError):
            continue
        if st["max_total"] is None or fv > st["max_total"]:
            st["max_total"] = fv
    return out


def company_sidebar_for_lead(conn: sqlite3.Connection, cik: str) -> dict[str, Any]:
    """
    Company column context: officer roster from the registrant's newest filing in DB.
    (Proxy for \"who else is named\" — not full company headcount.)
    """
    ck = (cik or "").strip()
    empty: dict[str, Any] = {"officer_roster": [], "roster_source": "", "roster_filing_id": None}
    if not ck:
        return empty
    fr = conn.execute(
        """
        SELECT id, filing_date, form_type FROM filings
        WHERE cik = ?
        ORDER BY COALESCE(filing_date, '') DESC, id DESC
        LIMIT 1
        """,
        (ck,),
    ).fetchone()
    if not fr:
        return empty
    fid = int(fr["id"])
    fd = (fr["filing_date"] or "").strip()
    ft = (fr["form_type"] or "").strip()
    bits = [x for x in (fd, ft) if x]
    src = " · ".join(bits) if bits else f"filing #{fid}"
    neo_by_norm = _neo_stats_by_norm_for_cik(conn, ck)
    roster: list[dict[str, Any]] = []
    for r in conn.execute(
        """
        SELECT name, title FROM officers
        WHERE filing_id = ?
        ORDER BY name COLLATE NOCASE
        """,
        (fid,),
    ).fetchall():
        name = (r["name"] or "").strip()
        title = (r["title"] or "").strip()
        pn = _norm_person_name(name)
        ns = neo_by_norm.get(pn) if pn else None
        roster.append(
            {
                "name": name,
                "title": title,
                "person_norm": pn,
                "neo_max_total": ns["max_total"] if ns else None,
                "neo_row_count": int(ns["row_count"]) if ns else 0,
                "neo_latest_fy": ns["latest_fy"] if ns else None,
            }
        )
    return {
        "officer_roster": roster,
        "roster_source": src,
        "roster_filing_id": fid,
    }


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
            min_e = float(stats.get("lead_desk_min_signal_usd") or 0)
            leg = bool(stats.get("lead_desk_equity_only_legacy"))
            s1 = bool(stats.get("lead_desk_s1_only"))
            bar = "max-year stock+options only" if leg else "max(SCT total, stock+options) in a single FY"
            extra = (
                f"<p class='meta'><b>{n_all}</b> NEO profile(s) are hidden by desk filters"
                f" ({'S-1 only; ' if s1 else ''}{bar} <b>${min_e:,.0f}</b>). "
                f"Open a saved <code>/lead?…</code> link to view anyone still in the DB. "
                f"To widen: set <code>WEALTH_LEADS_LEAD_DESK_MIN_SIGNAL_USD=0</code> (or lower the number), "
                f"and/or <code>WEALTH_LEADS_LEAD_DESK_S1_ONLY=0</code>. "
                f"Legacy equity-only: <code>WEALTH_LEADS_LEAD_DESK_MIN_EQUITY_USD</code>.</p>"
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
        ft_raw = (p.get("filing_form_type") or "EDGAR").strip()
        ft_u = ft_raw.upper()
        if "10-K" in ft_u:
            badge = "10-K · NEO"
        elif "S-1" in ft_u:
            badge = "S-1 · NEO"
        else:
            badge = f"{html.escape(ft_raw[:14])} · NEO" if ft_raw else "EDGAR · NEO"
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
        co_link = web_u if web_u.startswith(("http://", "https://")) else ""
        if not co_link and p.get("primary_doc_url"):
            co_link = (p.get("primary_doc_url") or "").strip()
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
    <b>Person-first pre-IPO leads.</b> Desk = <b>S-1 / S-1/A NEO</b> plus a configurable pay bar (<code>WEALTH_LEADS_LEAD_DESK_MIN_SIGNAL_USD</code>). <b>Company</b> links to the issuer website when we parsed one from the filing, otherwise the primary EDGAR doc.     <b>Latest total</b> = headline fiscal year SCT; <b>Max equity</b> = best single-year stock+options.
    <b>Age</b> = stated in the filing, then + full calendar years to today (no birthday in EDGAR text); hover a row for <b>bio + age detail</b>.
    Full narrative + HQ on the profile page.
    <b>Click the row</b> for detail. Raw rows: <b>Source rows</b> below.
  </p>
  <div class="table-wrap">
  <table id="desk">
    <thead>
      <tr>
        <th title="Stated age in the filing, plus full calendar years to today (birthday not disclosed)">Age</th><th>Person</th><th>Role</th><th>Company</th><th>Form</th>
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
) -> str:
    hq_e = html.escape(hq)
    ind_e = html.escape(industry)
    q_e = html.escape(q)
    return f"""
  <div class="card" style="margin-bottom:1rem">
    <h2 style="margin-top:0">Search</h2>
    <p class="meta" style="margin-top:0">Filter by <b>registrant location</b> (HQ address text) and <b>industry</b> (parsed SIC/NAICS or business summary keywords from the filing). All matching is substring, case-insensitive.</p>
    <form method="get" action="/finder" style="max-width:40rem">
      <label class="sr" for="hq">Registrant HQ contains</label>
      <input type="search" id="hq" name="hq" placeholder="e.g. California, Austin, 94105" value="{hq_e}" style="width:100%;max-width:100%;margin-bottom:0.5rem"/>
      <label class="sr" for="industry">Industry contains</label>
      <input type="search" id="industry" name="industry" placeholder="e.g. software, NAICS, 7372, pharmaceutical" value="{ind_e}" style="width:100%;max-width:100%;margin-bottom:0.5rem"/>
      <label class="sr" for="fq">Person or company contains</label>
      <input type="search" id="fq" name="q" placeholder="Person name, company, or CIK…" value="{q_e}" style="width:100%;max-width:100%;margin-bottom:0.5rem"/>
      <label style="display:flex;align-items:center;gap:0.5rem;font-size:0.8125rem;color:#8b96a3;cursor:pointer;margin:0.5rem 0">
        <input type="checkbox" name="all_neo" value="1"{' checked' if all_neo else ''}/>
        All NEO profiles (ignore lead-desk S-1 / pay-bar filters)
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
) -> str:
    banner = _stats_banner(stats, rendered_at)
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
            **({"all_neo": "1"} if all_neo else {}),
        }
    )
    export_href = "/export/finder.csv?" + exp_q
    form = _finder_form(
        hq=hq, industry=industry, q=q, all_neo=all_neo, export_href=export_href
    )
    tbl = _finder_table(profiles)
    scope = (
        f"<p class='meta'>Universe: <b>{base_count}</b> NEO profile(s) "
        f"({'all in database' if all_neo else 'after lead-desk filters'}), then location / industry / text filters.</p>"
    )
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
    Search people tied to issuers in your snapshot. <b>HQ</b> is the registrant’s principal office line from filings (not a home address).
    <b>Industry</b> uses parsed SIC/NAICS when available, and otherwise you can match keywords in the issuer business summary.
    <a href="/">Lead desk</a>
  </p>
  {banner}
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
    <strong>Local snapshot</strong>
    <span class="stats"><span>{desk_lbl}</span><span>{nf} filings</span><span>{no} officer rows</span><span>{nc} comp rows</span></span>
    <span class="sub">Newest filing date in DB: <b>{latest}</b> · DB file updated: <b>{mtime}</b> · Page loaded: <b>{rat}</b></span>
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
    table.comp-head td { font-size: 0.8rem; }
    details.audit { margin-top: 1.5rem; border-top: 1px solid #2a3340; padding-top: 1rem; }
    details.audit summary {
      cursor: pointer; color: #8b96a3; font-size: 0.8125rem; user-select: none;
      margin-bottom: 0.75rem;
    }
    details.audit summary:hover { color: #c5ccd4; }
    body.lead-profile-page { max-width: 1180px; }
    .lead-page-header { margin-bottom: 1rem; padding-bottom: 0.85rem; border-bottom: 1px solid #2a3340; }
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
    table.roster-grid { font-size: 0.74rem; }
    table.roster-grid td.roster-comp-col { white-space: nowrap; vertical-align: top; }
    table.roster-grid td.roster-note-col {
      font-size: 0.68rem;
      color: #8b96a3;
      line-height: 1.4;
      max-width: 17rem;
      vertical-align: top;
    }
    table.roster-grid td.roster-title { max-width: 13rem; vertical-align: top; }
    .roster-pill-row { display: flex; flex-wrap: wrap; gap: 0.25rem; margin-top: 0.3rem; }
    .roster-pill {
      display: inline-block;
      font-size: 0.58rem;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      padding: 0.08rem 0.35rem;
      border-radius: 3px;
      font-weight: 600;
    }
    .roster-pill-lead { background: #1e2d3d; color: #79c0ff; border: 1px solid #316d9a; }
    .roster-pill-neo { background: #15251c; color: #56d364; border: 1px solid #2a6a3f; }
    .roster-pill-soft { background: #22252c; color: #a8b0ba; border: 1px solid #3d424d; }
    .roster-comp-num { font-family: ui-monospace, "Cascadia Mono", Consolas, monospace; }
    .roster-comp-strong { font-weight: 600; color: #e8ecf0; }
    .roster-comp-min { color: #a8b5c4; }
    .roster-note { color: #8b96a3; }
    """


def _page_desk(
    profiles: list[dict],
    leads: list[sqlite3.Row],
    comp: list[sqlite3.Row],
    stats: dict,
    rendered_at: str,
) -> str:
    banner = _stats_banner(stats, rendered_at)
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
    <a href="/finder">Lead finder</a> — filter by registrant HQ and industry.
    SEC filing–native timing: <b>S-1</b> from RSS, then <b>10-K</b> for the <b>same CIKs</b> via SEC submissions (cross-reference). Data updates when you run <code>sync</code>.
    Database: <code>{html.escape(str(Path(database_path()).resolve()))}</code>
  </p>
  {banner}
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


def _roster_money_float(v: object) -> Optional[float]:
    if v is None:
        return None
    try:
        x = float(v)
        if math.isnan(x):
            return None
        return x
    except (TypeError, ValueError):
        return None


def _lead_roster_comp_cell(o: dict[str, Any]) -> str:
    nrows = int(o.get("neo_row_count") or 0)
    mx = _roster_money_float(o.get("neo_max_total"))
    fy = o.get("neo_latest_fy")
    if nrows == 0 or mx is None:
        return "<span class='dim'>No SCT row</span>"
    fy_l = f"FY {fy}" if fy is not None else "FY in DB"
    if mx < _ROSTER_MEANINGFUL_COMP_USD:
        return (
            f"<span class='roster-comp-num roster-comp-min'>{html.escape(_money(mx))}</span>"
            f" <span class='dim'>({html.escape(fy_l)}, minimal)</span>"
        )
    return (
        f"<span class='roster-comp-num roster-comp-strong'>{html.escape(_money(mx))}</span>"
        f" <span class='dim'>({html.escape(fy_l)} high-water)</span>"
    )


def _lead_roster_note_cell(o: dict[str, Any]) -> str:
    title = o.get("title") or ""
    dirish = _title_suggests_director_role(title)
    nrows = int(o.get("neo_row_count") or 0)
    mx = _roster_money_float(o.get("neo_max_total"))
    if dirish and nrows == 0:
        txt = (
            "Director / board role — often no NEO line in SCT; cash fees may be elsewhere "
            "in the filing."
        )
    elif dirish and mx is not None and mx < _ROSTER_MEANINGFUL_COMP_USD:
        txt = "Director — small SCT totals are common (fees, retainers vs. executive pay)."
    elif nrows > 0 and mx is not None and mx >= _ROSTER_MEANINGFUL_COMP_USD:
        txt = (
            "Summary comp table in DB with larger totals — comparable executive signal to "
            "pipeline NEO profiles (confirm title and duties in the filing)."
        )
    elif nrows > 0:
        txt = "SCT row exists but amounts are low—may be director fees or partial parse."
    else:
        txt = (
            "Signature-table name only in our DB—no matching NEO row for this person "
            "on this CIK (not necessarily a non-executive; check parse / filing)."
        )
    return f"<span class='roster-note'>{html.escape(txt)}</span>"


def _officer_roster_html(
    roster: list[dict[str, Any]],
    roster_source: str,
    *,
    lead_norm: str,
) -> str:
    if not roster:
        return (
            "<p class='meta' style='margin-bottom:0'>"
            "<span class='dim'>No officer rows in DB for the latest filing on this CIK. "
            "Sync / parse may not have captured the signature table.</span>"
            "</p>"
        )

    def sort_key(o: dict[str, Any]) -> tuple:
        pn = (o.get("person_norm") or "") or _norm_person_name(o.get("name") or "")
        is_lead = bool(lead_norm and pn == lead_norm)
        nrows = int(o.get("neo_row_count") or 0)
        mx = _roster_money_float(o.get("neo_max_total"))
        has_any_neo = nrows > 0
        meaningful = has_any_neo and mx is not None and mx >= _ROSTER_MEANINGFUL_COMP_USD
        if is_lead:
            tier = 0
        elif meaningful:
            tier = 1
        elif has_any_neo:
            tier = 2
        else:
            tier = 3
        sub = -(mx if mx is not None else 0.0) if tier <= 2 else 0.0
        return (tier, sub, (o.get("name") or "").lower())

    sorted_roster = sorted(roster, key=sort_key)
    body_rows: list[str] = []
    for o in sorted_roster[:60]:
        nm = o.get("name") or "—"
        tl = o.get("title") or "—"
        pn = (o.get("person_norm") or "") or _norm_person_name(nm)
        nrows = int(o.get("neo_row_count") or 0)
        mx = _roster_money_float(o.get("neo_max_total"))
        pills: list[str] = []
        if lead_norm and pn == lead_norm:
            pills.append("<span class='roster-pill roster-pill-lead'>This page</span>")
        if nrows > 0 and mx is not None and mx >= _ROSTER_MEANINGFUL_COMP_USD:
            pills.append("<span class='roster-pill roster-pill-neo'>NEO</span>")
        elif nrows > 0:
            pills.append("<span class='roster-pill roster-pill-soft'>SCT</span>")
        pill_html = (
            "<div class='roster-pill-row'>" + "".join(pills) + "</div>"
        ) if pills else ""
        body_rows.append(
            "<tr>"
            f"<td class='profile-name'>{html.escape(nm)}{pill_html}</td>"
            f"<td class='roster-title'>{html.escape(tl)}</td>"
            f"<td class='roster-comp-col'>{_lead_roster_comp_cell(o)}</td>"
            f"<td class='roster-note-col'>{_lead_roster_note_cell(o)}</td>"
            "</tr>"
        )
    cap = ""
    if len(roster) > 60:
        cap = f"<p class='meta dim' style='margin:0.35rem 0 0;font-size:0.72rem'>Showing 60 of {len(roster)} names.</p>"
    src = html.escape(roster_source) if roster_source else "latest filing"
    expl = (
        "<strong>How to read this:</strong> The left column is everyone named on the "
        "<em>latest</em> officer/signature table we parsed. "
        "<strong>Comp (DB)</strong> is our match to a <em>summary compensation (NEO)</em> row "
        "for this CIK anywhere in your database—not every director gets an SCT line, and "
        "many only show modest fees. "
        f"High-water totals below {_money(_ROSTER_MEANINGFUL_COMP_USD)} are labeled "
        "<em>minimal</em> (common for director fees). "
        "Sort order: this profile first, then larger NEO totals, then small or missing SCT."
    )
    return (
        "<p class='meta roster-explainer' style='margin-top:0.25rem;margin-bottom:0.65rem;font-size:0.78rem;line-height:1.5;color:#9aa3b0'>"
        f"{expl}</p>"
        f"<p class='meta' style='margin-top:0;margin-bottom:0.4rem;font-size:0.72rem;color:#7a8796'>"
        f"Officer table source: <strong>{src}</strong> · not total company headcount.</p>"
        "<div class='table-wrap'><table class='roster-grid'><thead><tr>"
        "<th>Name</th><th>Title</th><th>Comp in DB</th><th>Context</th>"
        "</tr></thead><tbody>"
        + "".join(body_rows)
        + "</tbody></table></div>"
        + cap
    )


def _page_lead(
    profile: Optional[dict],
    filings: list[dict],
    *,
    query_cik: str,
    query_name: str,
    stats: dict,
    rendered_at: str,
    company_sidebar: Optional[dict[str, Any]] = None,
) -> str:
    css = _shared_css()
    banner = _stats_banner(stats, rendered_at)
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
  {banner}
</body>
</html>"""

    p = profile
    sum_sct = p.get("sum_year_totals")
    sum_disp = _money(sum_sct) if sum_sct is not None else "—"
    doc_u = p.get("primary_doc_url") or ""
    doc_e = html.escape(doc_u)
    doc_link = (
        f'<a href="{doc_e}" target="_blank" rel="noopener">Open primary filing doc</a>'
        if doc_u
        else ""
    )

    headline_tbl = f"""
    <div class="table-wrap"><table class="comp-head">
      <thead><tr>
        <th>FY</th><th>Salary</th><th>Bonus</th><th>Stock</th><th>Options</th><th>Equity Σ</th><th>Total</th>
      </tr></thead>
      <tbody><tr>
        <td class="num">{html.escape(str(p.get('headline_year') or '—'))}</td>
        <td class="num">{_money(p.get('salary'))}</td>
        <td class="num">{_money(p.get('bonus'))}</td>
        <td class="num">{_money(p.get('stock_awards'))}</td>
        <td class="num">{_money(p.get('option_awards'))}</td>
        <td class="num">{_money(p.get('equity'))}</td>
        <td class="num strong">{_money(p.get('total'))}</td>
      </tr></tbody>
    </table></div>"""

    dts_body = (p.get("director_term_summary") or "").strip()
    director_card = f"""
    <div class="card">
      <h2 style="margin-top:0">Director and board terms</h2>
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
      <h2 style="margin-top:0">Management biography (from filing)</h2>
      <p class="meta" style="margin-bottom:0"><strong>{html.escape(heading_line)}</strong>{role_suffix}</p>
      {paras_html}
    </div>"""
    else:
        mgmt_narrative_card = f"""
    <div class="card bio-placeholder">
      <h2 style="margin-top:0">Management biography (from filing)</h2>
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
    if oa is not None:
        try:
            age_n = int(oa)
            if oat is not None:
                src = "executive officers table"
            elif nar is not None:
                src = "Management narrative (prose)"
            else:
                src = "filing-derived"
            esc_a = html.escape(anchor) if anchor else ""
            bits = [src]
            if stated is not None and anchor:
                try:
                    st_i = int(stated)
                    if age_n != st_i:
                        bits.append(
                            f"filing stated <b>{st_i}</b> as of <b>{esc_a}</b>; "
                            f"<b>{age_n}</b> adds full calendar years to today (birthday not in filing)"
                        )
                    else:
                        bits.append(f"as of filing date <b>{esc_a}</b>")
                except (TypeError, ValueError):
                    pass
            detail = "<span class='dim'>(" + "; ".join(bits) + ")</span>"
            age_line = f"<strong>Age:</strong> {age_n} {detail}<br/>"
        except (TypeError, ValueError):
            age_line = (
                "<strong>Age:</strong> <span class='dim'>—</span><br/>"
            )
    else:
        age_line = (
            "<strong>Age:</strong> <span class='dim'>Not found in roster table or narrative — "
            "run <code>backfill-comp --force</code> after parser updates.</span><br/>"
        )
    hq_txt = (p.get("issuer_headquarters") or "").strip()
    hq_esc = html.escape(hq_txt) if hq_txt else ""
    web_raw = (p.get("issuer_website") or "").strip()
    web_esc = html.escape(web_raw)
    web_link = (
        f'<a href="{web_esc}" target="_blank" rel="noopener">Company website</a>'
        if web_raw.startswith(("http://", "https://"))
        else ""
    )
    summ_body = (p.get("issuer_summary") or "").strip()
    summ_html = (
        html.escape(summ_body)
        if summ_body
        else "<span class='dim'>Not extracted — run <code>sync</code> or <code>backfill-comp --force</code>.</span>"
    )

    person_card = f"""
    <div class="card">
      <h2 style="margin-top:0">Person</h2>
      <p class="meta" style="margin-bottom:0">
        {age_line}
        <strong>Location (proxy):</strong> {hq_esc or "—"}
        <span class='dim'> — usually registrant HQ from the filing, not a home address.</span>
      </p>
    </div>"""

    company_bits = [
        f"<strong>{html.escape(p.get('company_name') or '—')}</strong>",
        f"CIK <span class='cik'>{html.escape(str(p.get('cik') or ''))}</span>",
    ]
    if web_link:
        company_bits.append(web_link)
    if hq_esc:
        company_bits.append(f"HQ: {hq_esc}")
    ind_txt = (p.get("issuer_industry") or "").strip()
    if ind_txt:
        company_bits.append(f"SIC/NAICS: {html.escape(ind_txt)}")
    company_intro = " · ".join(company_bits)

    company_card = f"""
    <div class="card">
      <h2 style="margin-top:0">Company</h2>
      <p class="meta" style="margin-bottom:0">{company_intro}</p>
      <p class="meta" style="margin-top:0.65rem; margin-bottom:0"><strong>From the filing (summary)</strong></p>
      <p class="meta" style="margin-top:0.35rem; margin-bottom:0">{summ_html}</p>
    </div>"""

    sb = company_sidebar or {}
    roster = list(sb.get("officer_roster") or [])
    roster_src = (sb.get("roster_source") or "").strip()
    lead_norm = _norm_person_name(
        (p.get("display_name") or "").strip() or query_name
    )
    roster_block = _officer_roster_html(roster, roster_src, lead_norm=lead_norm)

    hq_one = _hq_one_line_for_maps(hq_txt)
    co_nm = (p.get("company_name") or "").strip()
    map_query = hq_one or (f"{co_nm} headquarters" if co_nm else "")
    maps_row = ""
    if map_query.strip():
        maps_url = "https://www.google.com/maps/search/?api=1&query=" + quote(
            map_query, safe=""
        )
        maps_row = (
            "<div class='lead-map-row'>"
            f'<a href="{html.escape(maps_url)}" target="_blank" rel="noopener">Open map</a>'
            "<span class='dim'>Google Maps search from registrant HQ (or company name). Verify on the filing.</span>"
            "</div>"
        )

    data_note = (
        "<div class='lead-data-note'>"
        "<strong>Revenue &amp; headcount:</strong> Not stored as numbers in WealthPipeline yet. "
        "They often appear in Business / MD&amp;A in the filing—use the business summary here "
        "or the primary document link in the header. The officer table lists <strong>named executives</strong> "
        "from the filing, not total employees—useful for referral paths inside the company."
        "</div>"
    )

    roster_card = f"""
    <div class="card">
      <h2 style="margin-top:0">Officer roster &amp; SCT coverage</h2>
      {roster_block}
    </div>"""

    why_card = f"""
    <div class="card">
      <h2 style="margin-top:0">Why this lead</h2>
      <p class="meta" style="margin-bottom:0">{html.escape(p.get('why_surfaced') or '—')}</p>
    </div>"""

    filings_block = f"""
    <h2>Issuer filings (S-1 / 10-K)</h2>
    <p class="meta">Same <b>CIK</b> — registration statements and annual reports in your DB (newest first, up to 50).</p>
    {_filings_table_html(filings)}
    """

    col_company = f"""
    <aside class="lead-col lead-col-company" aria-labelledby="lead-col-company">
      <div class="lead-col-title" id="lead-col-company">Company</div>
      <p class="lead-col-hint">Registrant facts, map, business text, board terms, filing list, and who else is named—context for referrals and outreach.</p>
      {company_card}
      {maps_row}
      {data_note}
      {roster_card}
      {director_card}
      {filings_block}
    </aside>"""

    col_person = f"""
    <main class="lead-col lead-col-person" aria-labelledby="lead-col-person">
      <div class="lead-col-title" id="lead-col-person">Person</div>
      <p class="lead-col-hint">Why they surfaced, demographics proxy, disclosed compensation, and management bio from the filing.</p>
      {why_card}
      {person_card}
      <div class="hero">
        <div class="stat-card"><div class="lbl">Latest FY total</div><div class="val">{_money(p.get('total'))}</div></div>
        <div class="stat-card"><div class="lbl">Max equity (any FY)</div><div class="val">{_money(p.get('equity_hwm'))}</div></div>
        <div class="stat-card"><div class="lbl">Σ SCT (years in DB)</div><div class="val">{sum_disp}</div></div>
        <div class="stat-card"><div class="lbl">Fiscal years w/ comp</div><div class="val">{int(p.get('years_count') or 0)}</div></div>
      </div>
      <h2>Headline compensation (latest fiscal year)</h2>
      {headline_tbl}
      <h2>Year-by-year (summary comp table)</h2>
      {_profile_breakdown_table(p)}
      {mgmt_narrative_card}
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
    <p class="meta">
      <b>{html.escape(p.get('title') or '—')}</b> · {html.escape(p.get('company_name') or '')}
      · CIK <span class="cik">{html.escape(str(p.get('cik') or ''))}</span>
    </p>
    <p class="meta">
      Latest filing in profile: <b>{html.escape(p.get('filing_date') or '—')}</b>.
      <a href="{html.escape(p.get('index_url') or '')}" target="_blank" rel="noopener">EDGAR index</a>
      {(' · ' + doc_link) if doc_link else ''}
    </p>
  </header>
  <div class="lead-split" role="presentation">
    {col_company}
    {col_person}
  </div>
  <p class="meta">Bookmark this page: <code>/lead?{html.escape(bookmark_q)}</code></p>
  {banner}
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
        profiles, profiles_all, _leads, _comp, _stats = _load_page_data()
        body = finder_export_csv_bytes(
            profiles_all=profiles_all,
            profiles_desk=profiles,
            hq=hq,
            industry=industry,
            q=qtxt,
            all_neo=all_neo,
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
        base = profiles_all if all_neo else profiles
        filtered = filter_profiles_geo_industry_text(
            base,
            location_sub=hq,
            industry_sub=industry,
            text_sub=qtxt,
        )
        html_out = _page_finder(
            filtered,
            stats=stats,
            rendered_at=rendered_at,
            hq=hq,
            industry=industry,
            q=qtxt,
            all_neo=all_neo,
            base_count=len(base),
        )
    elif pi == "/lead":
        qs = parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=True)
        cik = (qs.get("cik") or [""])[0].strip()
        name_raw = (qs.get("name") or [""])[0]
        name_decoded = unquote(name_raw) if name_raw else ""
        norm = _norm_person_name(name_decoded)
        prof = _find_profile(profiles_all, cik, norm) if not stats.get("missing_db") else None
        filings: list[dict] = []
        company_sidebar = None
        if prof is not None and not stats.get("missing_db"):
            with connect() as conn:
                filings = _filings_for_profile(conn, cik, norm)
                company_sidebar = company_sidebar_for_lead(conn, cik)
        html_out = _page_lead(
            prof,
            filings,
            query_cik=cik,
            query_name=name_decoded,
            stats=stats,
            rendered_at=rendered_at,
            company_sidebar=company_sidebar,
        )
    else:
        html_out = _page_desk(profiles, leads, comp, stats, rendered_at)

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
