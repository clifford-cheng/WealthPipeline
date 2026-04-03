"""
Advisor web UI: territory-based assigned leads (not global browsing), admin allocation.
Run: uvicorn wealth_leads.web_app:app --host 0.0.0.0 --port 8080
Or:   python -m wealth_leads serve-app

Env: WEALTH_LEADS_APP_SECRET (required), WEALTH_LEADS_ALLOW_SIGNUP=1 for more accounts,
     WEALTH_LEADS_APP_PORT (default 8080), WEALTH_LEADS_UVICORN_RELOAD=1 to auto-reload on code edits (serve-app).
"""
from __future__ import annotations

import csv
import html as html_module
import io
import json
import math
import os
import re
import sqlite3
import traceback
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import quote, urlencode

from fastapi import FastAPI, Form, Request, Response
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

from wealth_leads.config import (
    app_allow_public_signup,
    app_secret_key,
    database_path,
    lead_desk_us_registrant_hq_only,
    require_app_auth,
    user_may_view_pipeline_exec_bundle,
)
from wealth_leads.advisor_pack import get_issuer_snapshot_dict
from wealth_leads.allocation import assign_for_cycle, assignments_to_display_rows
from wealth_leads.crm_ui import (
    format_headquarters_for_ui,
    pipeline_cash_excl_equity_from_row,
    pipeline_hq_city_state,
    render_admin_home,
    render_my_leads_page,
    render_pipeline_company_page,
    render_pipeline_review_page,
)
from wealth_leads.db import (
    add_user_watchlist,
    app_user_count,
    connect,
    delete_lead_suppress,
    delete_user_watchlist,
    get_allocation_settings,
    get_app_user_by_email,
    get_app_user_by_id,
    get_lead_client_research,
    get_advisor_lead_outreach,
    get_lead_profile_row,
    insert_app_user,
    insert_lead_advisor_feedback,
    insert_lead_suppress,
    list_allocation_clients,
    list_lead_advisor_feedback,
    list_lead_suppress,
    list_beneficial_owner_gems_for_filing_ids,
    list_beneficial_owner_outreach_targets_for_cik,
    list_advisor_lead_outreach_status_by_norm_for_cik,
    list_lead_profiles_for_review,
    list_user_watchlist,
    update_allocation_settings,
    update_user_allocation_profile,
    upsert_advisor_lead_outreach,
)
from wealth_leads.lead_research import row_to_client_research_dict
from wealth_leads.password_util import hash_password, verify_password
from wealth_leads.profile_build import rebuild_lead_profiles
from wealth_leads.sync_runner import start_sync_subprocess, sync_state

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None  # type: ignore[misc, assignment]


def _profile_display_name_for_quality_gate(p: dict[str, Any]) -> str:
    d = (p.get("display_name") or "").strip()
    if d:
        return d
    pn = (p.get("person_norm") or "").strip()
    return " ".join(w.title() for w in pn.split()) if pn else ""


def _lead_profile_row_dict(row: sqlite3.Row) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k in row.keys():
        v = row[k]
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            out[k] = None
        else:
            out[k] = v
    return out


def _pipeline_review_params(request: Request) -> dict[str, Any]:
    """Shared query params for pipeline HTML, company drill-in, and CSV export."""
    q = (request.query_params.get("q") or "").strip()
    include_non_s1 = request.query_params.get("include_non_s1") == "1"
    try:
        months_sel = int(request.query_params.get("months", "6"))
    except ValueError:
        months_sel = 6
    raw_tier = (request.query_params.get("tier") or "").strip().lower().replace("-", "_")
    if raw_tier in ("standard", "economy", "std", "other"):
        pipeline_tier_tab = "standard"
    else:
        pipeline_tier_tab = "exec"
    return {
        "q": q,
        "include_non_s1": include_non_s1,
        "months_sel": months_sel,
        "pipeline_tier_tab": pipeline_tier_tab,
    }


def _json_safe_num(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def _json_sanitize_for_response(obj: Any) -> Any:
    """``JSONResponse`` cannot encode bytes, NaN, or Inf — strip before returning API JSON."""
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        for k, v in obj.items():
            if k == "photo_blob":
                continue
            if isinstance(v, (bytes, bytearray, memoryview)):
                continue
            out[str(k)] = _json_sanitize_for_response(v)
        return out
    if isinstance(obj, list):
        return [_json_sanitize_for_response(x) for x in obj]
    if isinstance(obj, float):
        return _json_safe_num(obj)
    return obj


def _neo_comp_rows_for_drawer(
    conn: sqlite3.Connection, filing_ids: list[int], person_norm: str
) -> list[dict[str, Any]]:
    """Per-year NEO lines for this person from linked filings (table parse + LLM)."""
    from wealth_leads.serve import _norm_person_name

    if not filing_ids:
        return []
    qm = ",".join("?" * len(filing_ids))
    cur = conn.execute(
        f"""
        SELECT c.fiscal_year, c.person_name, c.role_hint, c.salary, c.bonus,
               c.stock_awards, c.option_awards, c.non_equity_incentive, c.pension_change,
               c.other_comp, c.total, c.source,
               f.filing_date, f.form_type, f.accession
        FROM neo_compensation c
        JOIN filings f ON f.id = c.filing_id
        WHERE c.filing_id IN ({qm})
        ORDER BY c.fiscal_year DESC, COALESCE(f.filing_date, '') DESC, f.id DESC
        """,
        tuple(filing_ids),
    )
    out: list[dict[str, Any]] = []
    for r in cur.fetchall():
        if _norm_person_name(r["person_name"] or "") != person_norm:
            continue
        out.append(
            {
                "fiscal_year": r["fiscal_year"],
                "role_hint": r["role_hint"],
                "salary": _json_safe_num(r["salary"]),
                "bonus": _json_safe_num(r["bonus"]),
                "stock_awards": _json_safe_num(r["stock_awards"]),
                "option_awards": _json_safe_num(r["option_awards"]),
                "non_equity_incentive": _json_safe_num(r["non_equity_incentive"]),
                "pension_change": _json_safe_num(r["pension_change"]),
                "other_comp": _json_safe_num(r["other_comp"]),
                "total": _json_safe_num(r["total"]),
                "source": (r["source"] or "").strip(),
                "filing_date": (r["filing_date"] or "").strip(),
                "form_type": (r["form_type"] or "").strip(),
                "accession": (r["accession"] or "").strip(),
            }
        )
    return out


def _latest_filing_issuer_fallback(
    conn: sqlite3.Connection, cik: str
) -> Optional[dict[str, Any]]:
    """Latest filing row for CIK — fills gaps when lead_profile issuer fields are empty."""
    ck = (cik or "").strip()
    if not ck:
        return None
    r = conn.execute(
        """
        SELECT issuer_website, issuer_headquarters, issuer_industry, issuer_summary,
               issuer_revenue_text,
               company_name, filing_date, form_type, accession
        FROM filings
        WHERE cik = ?
        ORDER BY COALESCE(filing_date, '') DESC, id DESC
        LIMIT 1
        """,
        (ck,),
    ).fetchone()
    if not r:
        return None
    from wealth_leads.serve import _resolve_issuer_headquarters_for_profile

    d = {k: r[k] for k in r.keys()}
    raw_hq = (d.get("issuer_headquarters") or "").strip()
    d["issuer_headquarters"] = _resolve_issuer_headquarters_for_profile(
        conn, ck, raw_hq
    )
    return d


def _officer_snapshot_for_person(
    conn: sqlite3.Connection, cik: str, person_norm: str
) -> Optional[dict[str, Any]]:
    from wealth_leads.serve import _norm_person_name

    ck = (cik or "").strip()
    if not ck or not person_norm:
        return None
    cur = conn.execute(
        """
        SELECT o.name, o.title, o.age, o.source, f.filing_date, f.form_type
        FROM officers o
        JOIN filings f ON f.id = o.filing_id
        WHERE f.cik = ?
        ORDER BY COALESCE(f.filing_date, '') DESC, f.id DESC
        """,
        (ck,),
    )
    for r in cur.fetchall():
        if _norm_person_name(r["name"] or "") != person_norm:
            continue
        age = r["age"]
        try:
            age_i = int(age) if age is not None else None
        except (TypeError, ValueError):
            age_i = None
        return {
            "title": (r["title"] or "").strip(),
            "age": age_i,
            "source": (r["source"] or "").strip(),
            "filing_date": (r["filing_date"] or "").strip(),
            "form_type": (r["form_type"] or "").strip(),
        }
    return None


def _pipeline_row_drawer_enrich(conn: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
    """Extra fields for pipeline drawer: bios, issuer narrative, director terms, NEO sources."""
    from wealth_leads.serve import _norm_person_name

    cik = (row["cik"] or "").strip()
    pn = (row["person_norm"] or "").strip()
    extras: dict[str, Any] = {
        "mgmt_bio_text": "",
        "mgmt_bio_role": "",
        "issuer_summary_full": "",
        "director_term_summary": "",
        "neo_source_line": "",
        "lead_intel": None,
        "neo_comp_rows": [],
        "issuer_fallback": None,
        "officer_snapshot": None,
    }
    try:
        raw_ids = json.loads(row["neo_filing_ids_json"] or "[]")
    except (json.JSONDecodeError, TypeError):
        raw_ids = []
    if not isinstance(raw_ids, list):
        raw_ids = []
    fids_i: list[int] = []
    for x in raw_ids:
        try:
            fids_i.append(int(x))
        except (TypeError, ValueError):
            pass
    sources: list[str] = []
    if fids_i:
        qm = ",".join("?" * len(fids_i))
        qneo = (
            "SELECT DISTINCT source, person_name FROM neo_compensation "
            f"WHERE filing_id IN ({qm})"
        )
        for nr in conn.execute(qneo, tuple(fids_i)):
            if _norm_person_name(nr["person_name"] or "") != pn:
                continue
            s = (nr["source"] or "").strip()
            if s and s not in sources:
                sources.append(s)
        extras["neo_source_line"] = ", ".join(sources)
        fq_full = (
            "SELECT issuer_summary, director_term_summary, s1_llm_lead_pack FROM filings "
            f"WHERE id IN ({qm}) "
            "ORDER BY COALESCE(filing_date, '') DESC, id DESC LIMIT 1"
        )
        fq_basic = (
            "SELECT issuer_summary, director_term_summary FROM filings "
            f"WHERE id IN ({qm}) "
            "ORDER BY COALESCE(filing_date, '') DESC, id DESC LIMIT 1"
        )
        try:
            fr = conn.execute(fq_full, tuple(fids_i)).fetchone()
        except sqlite3.OperationalError:
            fr = conn.execute(fq_basic, tuple(fids_i)).fetchone()
        if fr:
            extras["issuer_summary_full"] = (fr["issuer_summary"] or "").strip()
            extras["director_term_summary"] = (fr["director_term_summary"] or "").strip()
            try:
                lp = (fr["s1_llm_lead_pack"] or "").strip()
            except (KeyError, IndexError, TypeError):
                lp = ""
            if lp:
                try:
                    extras["lead_intel"] = json.loads(lp)
                except json.JSONDecodeError:
                    extras["lead_intel"] = None
    br = conn.execute(
        """
        SELECT m.bio_text, m.role_heading FROM person_management_narrative m
        JOIN filings f ON f.id = m.filing_id
        WHERE f.cik = ? AND m.person_name_norm = ?
        ORDER BY COALESCE(f.filing_date, '') DESC, f.id DESC LIMIT 1
        """,
        (cik, pn),
    ).fetchone()
    if br:
        extras["mgmt_bio_text"] = (br["bio_text"] or "").strip()
        extras["mgmt_bio_role"] = (br["role_heading"] or "").strip()

    extras["neo_comp_rows"] = _neo_comp_rows_for_drawer(conn, fids_i, pn)
    extras["officer_snapshot"] = _officer_snapshot_for_person(conn, cik, pn)
    fb = _latest_filing_issuer_fallback(conn, cik)
    if fb:
        fb = dict(fb)
        fb["issuer_headquarters"] = format_headquarters_for_ui(
            fb.get("issuer_headquarters") or ""
        )
        extras["issuer_fallback"] = fb
    cr = get_lead_client_research(conn, cik, pn)
    extras["client_research"] = (
        {k: cr[k] for k in cr.keys()} if cr is not None else None
    )
    extras["beneficial_owner_gems"] = []
    if fids_i:
        try:
            gems = list_beneficial_owner_gems_for_filing_ids(
                conn, fids_i, min_gem=30, limit=10
            )
            extras["beneficial_owner_gems"] = [
                {k: row[k] for k in row.keys()} for row in gems
            ]
        except sqlite3.Error:
            pass
    return extras


from wealth_leads.person_quality import is_acceptable_lead_person_name
from wealth_leads.serve import (
    _build_profiles,
    _filings_for_profile,
    _find_profile,
    _latest_filing_snapshot_caveats_for_cik,
    _latest_important_people_from_s1_llm_pack,
    _lead_desk_filter_profiles,
    _lead_page_db_stats,
    _load_page_data,
    _norm_person_name,
    _desk_sort_tuple,
    _page_desk,
    _page_desk_company,
    _page_finder,
    _page_lead,
    _profile_lead_tier,
    _profile_lead_url,
    beneficial_stake_detail_for_profile,
    filter_profiles_geo_industry_text,
    desk_sales_bundle_company_counts,
    filter_profiles_sales_bundle,
    filter_rows_sales_bundle,
    profile_sales_bundle,
    finder_export_csv_bytes,
    lead_feedback_form_html,
    lead_outreach_form_html,
    lead_issuer_recency_bundle,
    normalize_sales_bundle_query,
    sales_bundle_from_lead_tier,
)
from wealth_leads.territory import registrant_hq_line_parses_as_united_states

AUTH_COOKIE = "wl_auth"
SESSION_MAX_AGE = 60 * 60 * 24 * 14

# Used when WEALTH_LEADS_REQUIRE_AUTH is off (iterate on data without login).
APP_WEB_BOOT_AT = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

_SYNTHETIC_REVIEW_USER: dict = {
    "id": 0,
    "email": "Review mode (no login)",
    "is_admin": True,
    "monthly_lead_quota": 0,
    "territory_type": "state",
    "territory_spec": "",
    "premium_s1_only": False,
    "_synthetic": True,
}

def _compliance_footer_html() -> str:
    return f"""
<footer style="margin:2rem 1rem;font-size:0.72rem;color:#6b7785;max-width:52rem;line-height:1.5;border-top:1px solid #2a3340;padding-top:1rem">
<strong>Important.</strong> WealthPipeline surfaces public SEC filing text and extracted fields for research workflows.
It is <strong>not</strong> investment advice, a recommendation, or a solicitation. Filings may be amended; numbers and narratives
are only as accurate as the underlying parser and your sync time. You are responsible for your own compliance (e.g. RIA marketing
and recordkeeping). Verify material facts in the official EDGAR filing.
<p style="margin:0.85rem 0 0;padding-top:0.75rem;border-top:1px solid #2a3340;line-height:1.45"><strong>Server</strong> started <code>{html_module.escape(APP_WEB_BOOT_AT)}</code>.
Restart the app after editing Python files (or set <code>WEALTH_LEADS_UVICORN_RELOAD=1</code> when running <code>serve-app</code>). Refresh the <strong>Pipeline</strong> materialized table with <strong>Rebuild profiles</strong> or <code>py -m wealth_leads rebuild-profiles</code>.</p>
</footer>
"""

_TOP_NAV_CSS = """
<style>
.wl-top { background:#121820;border-bottom:1px solid #2a3340;padding:0.5rem 1rem;font-size:0.8125rem;display:flex;flex-wrap:wrap;gap:0.75rem;align-items:center;justify-content:space-between }
.wl-top a { color:#5eb3e0; text-decoration:none }
.wl-top a:hover { text-decoration:underline }
.wl-top .wl-actions form { display:inline }
.wl-top button { background:#1a2634;border:1px solid #2a3340;color:#d8dee4;border-radius:4px;padding:0.2rem 0.5rem;cursor:pointer;font:inherit;font-size:0.75rem }
.wl-top button:hover { background:#243044 }
</style>
"""


def _serializer(secret: str) -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(secret, salt="wealthpipeline-auth")


def _safe_next(path: str) -> str:
    p = (path or "/").strip()
    if not p.startswith("/") or p.startswith("//"):
        return "/"
    return p


def _session_user(request: Request) -> Optional[dict]:
    if not require_app_auth():
        return dict(_SYNTHETIC_REVIEW_USER)
    secret = app_secret_key()
    if len(secret) < 16:
        return None
    raw = request.cookies.get(AUTH_COOKIE)
    if not raw:
        return None
    try:
        data = _serializer(secret).loads(raw, max_age=SESSION_MAX_AGE)
        uid = int(data["uid"])
    except (BadSignature, SignatureExpired, TypeError, ValueError, KeyError):
        return None
    with connect() as conn:
        row = get_app_user_by_id(conn, uid)
        if row is None:
            return None
        return {
            "id": int(row["id"]),
            "email": row["email"],
            "is_admin": bool(int(row["is_admin"] or 0)),
            "monthly_lead_quota": int(row["monthly_lead_quota"] or 0),
            "territory_type": (row["territory_type"] or "state").strip(),
            "territory_spec": (row["territory_spec"] or "").strip(),
            "premium_s1_only": bool(int(row["premium_s1_only"] or 0)),
        }


def _auth_redirect(request: Request) -> Optional[Response]:
    if not require_app_auth():
        return None
    if len(app_secret_key()) < 16:
        return HTMLResponse(
            "<h1>Configuration</h1><p>Set <code>WEALTH_LEADS_APP_SECRET</code> "
            "(at least 16 characters) and restart.</p>",
            status_code=503,
        )
    u = _session_user(request)
    if u is None:
        nxt = quote(str(request.url.path) + (f"?{request.url.query}" if request.url.query else ""))
        return RedirectResponse(url=f"/login?next={nxt}", status_code=302)
    return None


def _pipeline_url_path() -> str:
    return "/admin/pipeline" if require_app_auth() else "/pipeline"


def _inject_chrome(page_html: str, user: dict) -> str:
    email = html_module.escape(str(user.get("email") or ""))
    if user.get("_synthetic"):
        pp = _pipeline_url_path()
        center = (
            f'<a href="{pp}"><strong>Pipeline</strong></a> · '
            '<a href="/admin/desk">Desk</a> · <a href="/admin/finder">Finder</a> · '
            f'<a href="{pp}.csv">Pipeline CSV</a> · '
            '<a href="/admin">Sync &amp; settings</a>'
        )
        right = (
            f'<span style="color:#8b96a3">{email}</span> · '
            '<a href="/login">Sign in</a> <span style="color:#6b7785">(set WEALTH_LEADS_REQUIRE_AUTH=1)</span>'
        )
    elif user.get("is_admin"):
        center = (
            '<a href="/my-leads">My leads</a> · <a href="/admin">Admin</a> · '
            '<a href="/admin/pipeline">Pipeline</a> · '
            '<a href="/admin/desk">Desk</a> · <a href="/admin/finder">Finder</a> · '
            '<a href="/watchlist">Watchlist</a> · <a href="/export/my-leads.csv">My CSV</a>'
        )
        right = (
            f'<span style="color:#8b96a3">{email}</span> · '
            '<form method="post" action="/logout"><button type="submit">Log out</button></form>'
        )
    else:
        center = (
            '<a href="/my-leads">My leads</a> · '
            '<a href="/export/my-leads.csv">Export CSV</a>'
        )
        right = (
            f'<span style="color:#8b96a3">{email}</span> · '
            '<form method="post" action="/logout"><button type="submit">Log out</button></form>'
        )
    nav = (
        _TOP_NAV_CSS
        + f'<nav class="wl-top"><div>{center}</div>'
        f'<div class="wl-actions">{right}</div></nav>'
    )
    # Place nav inside <body>; putting it before <body> is invalid HTML and breaks some clients.
    # Callable repl avoids interpreting backslashes in `nav` as re group refs.
    out, n_sub = re.subn(
        r"(<body\b[^>]*>)",
        lambda m: m.group(1) + nav,
        page_html,
        count=1,
        flags=re.IGNORECASE,
    )
    if not n_sub:
        out = page_html
    out = out.replace("</body>", _compliance_footer_html() + "</body>", 1)
    return out


@asynccontextmanager
async def _lifespan(app: FastAPI):
    stop_ev = None
    try:
        from wealth_leads.auto_sync import start_auto_sync_background
        from wealth_leads.config import (
            auto_sync_first_delay_sec,
            auto_sync_interval_hours,
        )

        stop_ev = start_auto_sync_background()
        if stop_ev is None:
            print(
                "Auto SEC sync: disabled (WEALTH_LEADS_AUTO_SYNC_HOURS=0).",
                flush=True,
            )
        else:
            print(
                f"Auto SEC sync: enabled — first pull in ~{auto_sync_first_delay_sec():.0f}s, "
                f"then every {auto_sync_interval_hours():g}h while this server runs. "
                "Log: logs/sec-sync.log",
                flush=True,
            )
    except Exception as e:
        print(f"Auto SEC sync: could not start ({e!r}).", flush=True)
    yield
    if stop_ev is not None:
        stop_ev.set()


app = FastAPI(
    title="WealthPipeline",
    docs_url=None,
    redoc_url=None,
    lifespan=_lifespan,
)


@app.middleware("http")
async def _no_cache_html_for_dev(request: Request, call_next):
    """
    Normal refresh should not show a stale page from disk cache while iterating locally.
    Set WEALTH_LEADS_ALLOW_BROWSER_CACHE=1 to allow caching (not typical for this app).
    """
    response = await call_next(request)
    response.headers["X-WealthPipeline-Server"] = "advisor"
    if os.environ.get("WEALTH_LEADS_ALLOW_BROWSER_CACHE", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        return response
    ct = (response.headers.get("content-type") or "").lower()
    if "text/html" in ct:
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
    return response


@app.get("/login", response_class=HTMLResponse)
async def login_get(request: Request, next: str = "/my-leads") -> HTMLResponse:
    err = request.query_params.get("err", "")
    ok = request.query_params.get("ok", "")
    if err:
        msg = f'<p style="color:#c98a7a">{html_module.escape(err)}</p>'
    elif ok == "1":
        msg = '<p style="color:#7dd97d">Account created. Sign in below.</p>'
    else:
        msg = ""
    sec = app_secret_key()
    if require_app_auth() and len(sec) < 16:
        return HTMLResponse(
            "<h1>Missing secret</h1><p>Set WEALTH_LEADS_APP_SECRET (16+ chars).</p>",
            status_code=503,
        )
    data_mode = ""
    if not require_app_auth():
        data_mode = (
            '<p style="color:#8b96a3;font-size:0.88rem;line-height:1.45">You are in <strong>data review mode</strong> '
            '(no login required). Open the <a href="/pipeline">pipeline</a> or <a href="/admin/desk">desk</a>. '
            "To require sign-in later, set <code>WEALTH_LEADS_REQUIRE_AUTH=1</code> and restart.</p>"
        )
    return HTMLResponse(
        f"""<!DOCTYPE html><html><head><meta charset="utf-8"/><title>Sign in — WealthPipeline</title>
<style>body{{font-family:system-ui;background:#0d1117;color:#e8ecf0;max-width:24rem;margin:3rem auto;padding:1rem}}
label{{display:block;margin:0.5rem 0 0.2rem;color:#8b96a3;font-size:0.8rem}}
input{{width:100%;padding:0.45rem;border-radius:4px;border:1px solid #2a3340;background:#0a0e12;color:#e8ecf0}}
button{{margin-top:1rem;padding:0.5rem 1rem;border-radius:4px;border:none;background:#238636;color:#fff;cursor:pointer;width:100%}}
a{{color:#58a6ff}}</style></head><body>
<h1>WealthPipeline</h1>
{data_mode}
<p>Sign in with your advisor account.</p>
{msg}
<form method="post" action="/login">
<input type="hidden" name="next" value="{html_module.escape(next)}"/>
<label>Email</label><input name="email" type="email" autocomplete="username" required/>
<label>Password</label><input name="password" type="password" autocomplete="current-password" required/>
<button type="submit">Sign in</button>
</form>
<p style="margin-top:1.5rem;font-size:0.85rem"><a href="/register">Create account</a> (if allowed)</p>
</body></html>"""
    )


@app.post("/login")
async def login_post(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    next: str = Form("/my-leads"),
) -> RedirectResponse:
    secret = app_secret_key()
    if len(secret) < 16:
        return RedirectResponse("/login?err=" + quote("Server not configured"), status_code=302)
    with connect() as conn:
        row = get_app_user_by_email(conn, email)
    if row is None or not verify_password(password, row["password_hash"]):
        return RedirectResponse(
            "/login?err=" + quote("Invalid email or password"),
            status_code=302,
        )
    token = _serializer(secret).dumps({"uid": int(row["id"])})
    resp = RedirectResponse(url=_safe_next(next), status_code=302)
    resp.set_cookie(
        AUTH_COOKIE,
        token,
        max_age=SESSION_MAX_AGE,
        httponly=True,
        samesite="lax",
        secure=request.url.scheme == "https",
    )
    return resp


@app.get("/register", response_class=HTMLResponse)
async def register_get(request: Request) -> HTMLResponse:
    err = request.query_params.get("err", "")
    msg = f'<p style="color:#c98a7a">{html_module.escape(err)}</p>' if err else ""
    if len(app_secret_key()) < 16:
        return HTMLResponse(
            "<h1>Missing secret</h1><p>Set WEALTH_LEADS_APP_SECRET (16+ chars).</p>",
            status_code=503,
        )
    with connect() as conn:
        n = app_user_count(conn)
    allow = n == 0 or app_allow_public_signup()
    if not allow:
        return HTMLResponse(
            "<h1>Registration closed</h1><p>Set <code>WEALTH_LEADS_ALLOW_SIGNUP=1</code> or sign in.</p>"
            '<p><a href="/login">Sign in</a></p>',
            status_code=403,
        )
    return HTMLResponse(
        f"""<!DOCTYPE html><html><head><meta charset="utf-8"/><title>Register — WealthPipeline</title>
<style>body{{font-family:system-ui;background:#0d1117;color:#e8ecf0;max-width:24rem;margin:3rem auto;padding:1rem}}
label{{display:block;margin:0.5rem 0 0.2rem;color:#8b96a3;font-size:0.8rem}}
input{{width:100%;padding:0.45rem;border-radius:4px;border:1px solid #2a3340;background:#0a0e12;color:#e8ecf0}}
button{{margin-top:1rem;padding:0.5rem 1rem;border-radius:4px;border:none;background:#238636;color:#fff;cursor:pointer;width:100%}}
a{{color:#58a6ff}}</style></head><body>
<h1>Create account</h1>
{msg}
<form method="post" action="/register">
<label>Work email</label><input name="email" type="email" autocomplete="username" required/>
<label>Password</label><input name="password" type="password" autocomplete="new-password" required minlength="10"/>
<button type="submit">Register</button>
</form>
<p style="margin-top:1rem;font-size:0.85rem"><a href="/login">Sign in</a></p>
</body></html>"""
    )


@app.post("/register")
async def register_post(
    email: str = Form(...),
    password: str = Form(...),
) -> RedirectResponse:
    secret = app_secret_key()
    if len(secret) < 16:
        return RedirectResponse("/register?err=" + quote("Server not configured"), status_code=302)
    if len(password) < 10:
        return RedirectResponse("/register?err=" + quote("Password too short"), status_code=302)
    with connect() as conn:
        n = app_user_count(conn)
        if n > 0 and not app_allow_public_signup():
            return RedirectResponse("/register?err=" + quote("Signup disabled"), status_code=302)
        if get_app_user_by_email(conn, email):
            return RedirectResponse("/register?err=" + quote("Email already registered"), status_code=302)
        uid = insert_app_user(conn, email, hash_password(password))
        if n == 0:
            update_user_allocation_profile(conn, uid, is_admin=1)
    return RedirectResponse("/login?ok=1", status_code=302)


@app.post("/logout")
async def logout() -> RedirectResponse:
    resp = RedirectResponse(url="/login", status_code=302)
    resp.delete_cookie(AUTH_COOKIE)
    return resp


@app.get("/", response_model=None)
async def root(request: Request) -> Response:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    if not require_app_auth():
        return RedirectResponse("/pipeline", status_code=302)
    return RedirectResponse("/my-leads", status_code=302)


@app.get("/my-leads", response_model=None)
async def my_leads(
    request: Request,
    cycle: str = "",
    tag: str = "",
) -> Response:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    user = _session_user(request)
    assert user is not None
    cy = (cycle or "").strip() or datetime.now().strftime("%Y%m")
    if len(cy) != 6 or not cy.isdigit():
        cy = datetime.now().strftime("%Y%m")
    with connect() as conn:
        from wealth_leads.db import count_assignments_for_user_cycle

        n_delivered = count_assignments_for_user_cycle(conn, user["id"], cy)
    rows = []
    with connect() as conn:
        rows = assignments_to_display_rows(
            conn, user_id=user["id"], cycle_yyyymm=cy, tag_filter=tag
        )
    body = render_my_leads_page(
        rows=rows,
        cycle=cy,
        tag_filter=tag,
        quota=int(user.get("monthly_lead_quota") or 0),
        delivered=n_delivered,
        territory_type=user.get("territory_type") or "state",
        territory_spec=user.get("territory_spec") or "",
        premium_s1_only=bool(user.get("premium_s1_only")),
    )
    return HTMLResponse(_inject_chrome(body, user))


@app.get("/export/my-leads.csv", response_model=None)
async def export_my_leads_csv(
    request: Request,
    cycle: str = "",
    tag: str = "",
) -> Response:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    user = _session_user(request)
    assert user is not None
    cy = (cycle or "").strip() or datetime.now().strftime("%Y%m")
    with connect() as conn:
        rows = assignments_to_display_rows(
            conn, user_id=user["id"], cycle_yyyymm=cy, tag_filter=tag
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
            "estimated_equity_usd",
            "liquidity_stage",
            "tags",
            "why_summary",
            "suggested_outreach_angle",
            "email_guess",
            "email_confidence",
            "profile_path",
        ]
    )
    for r in rows:
        prof = r.get("profile") or {}
        snap = r.get("snapshot") or {}
        nm = prof.get("display_name") or snap.get("display_name") or ""
        co = prof.get("company_name") or snap.get("company_name") or ""
        title = prof.get("title") or snap.get("title") or ""
        hq = format_headquarters_for_ui(
            prof.get("issuer_headquarters") or snap.get("issuer_headquarters") or ""
        )
        eq = prof.get("equity_hwm")
        if eq is None:
            eq = snap.get("equity_hwm")
        tags = ";".join(r.get("tags") or [])
        prof_path = _profile_lead_url(
            {"cik": r.get("cik"), "display_name": nm}
        )
        w.writerow(
            [
                nm,
                co,
                r.get("cik") or "",
                title,
                hq,
                eq if eq is not None else "",
                r.get("liquidity_stage") or "",
                tags,
                r.get("why_summary") or "",
                r.get("outreach_angle") or "",
                r.get("email_guess") or "",
                r.get("email_confidence") if r.get("email_confidence") is not None else "",
                prof_path if nm else "",
            ]
        )
    data = buf.getvalue().encode("utf-8")

    def gen():
        yield data

    return StreamingResponse(
        gen(),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="my-leads-{cy}.csv"',
            "Cache-Control": "no-store",
        },
    )


@app.get("/admin", response_model=None)
async def admin_home(request: Request) -> Response:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    user = _session_user(request)
    assert user is not None
    if not user.get("is_admin"):
        return HTMLResponse("<h1>Forbidden</h1><p>Admin only.</p>", status_code=403)
    cy = datetime.now().strftime("%Y%m")
    msg = request.query_params.get("msg", "")
    with connect() as conn:
        clients = list_allocation_clients(conn)
        settings_row = get_allocation_settings(conn)
        suppress_rows = list_lead_suppress(conn, limit=80)
        advisor_feedback_rows = list_lead_advisor_feedback(conn, limit=40)
        try:
            filing_count = int(
                conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
            )
        except Exception:
            filing_count = None
    body = render_admin_home(
        clients=clients,
        settings_row=settings_row,
        cycle=cy,
        alloc_msg=msg,
        filing_count=filing_count,
        sync_info=sync_state(),
        suppress_rows=suppress_rows,
        advisor_feedback_rows=advisor_feedback_rows,
    )
    return HTMLResponse(_inject_chrome(body, user))


@app.get("/admin/pipeline", response_class=HTMLResponse)
@app.get("/pipeline", response_class=HTMLResponse)
async def admin_pipeline(request: Request) -> Response:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    user = _session_user(request)
    assert user is not None
    if not user.get("is_admin"):
        return HTMLResponse("<h1>Forbidden</h1>", status_code=403)
    msg = request.query_params.get("msg", "")
    pf = _pipeline_review_params(request)
    months_back = None if pf["months_sel"] <= 0 else pf["months_sel"]
    with connect() as conn:
        rows = list_lead_profiles_for_review(
            conn,
            s1_only=not pf["include_non_s1"],
            search=pf["q"],
            limit=800,
            months_back=months_back,
            pay_band="all",
            us_registrant_hq_only=lead_desk_us_registrant_hq_only(),
            listing_stage="all",
        )
    from wealth_leads.config import pipeline_blur_comp_columns

    body = render_pipeline_review_page(
        rows=rows,
        search=pf["q"],
        include_non_s1=pf["include_non_s1"],
        months=pf["months_sel"],
        tier_tab=pf["pipeline_tier_tab"],
        msg=msg,
        pipeline_path=_pipeline_url_path(),
        blur_comp=pipeline_blur_comp_columns(),
    )
    return HTMLResponse(_inject_chrome(body, user))


@app.get("/admin/pipeline/company", response_class=HTMLResponse)
@app.get("/pipeline/company", response_class=HTMLResponse)
async def admin_pipeline_company(request: Request) -> Response:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    user = _session_user(request)
    assert user is not None
    if not user.get("is_admin"):
        return HTMLResponse("<h1>Forbidden</h1>", status_code=403)
    cik = (request.query_params.get("cik") or "").strip()
    if not cik:
        return RedirectResponse(_pipeline_url_path(), status_code=302)
    pf = _pipeline_review_params(request)
    months_back = None if pf["months_sel"] <= 0 else pf["months_sel"]
    pp = _pipeline_url_path()
    back_qs: dict[str, str] = {
        "q": pf["q"],
        "months": str(pf["months_sel"]),
        "include_non_s1": "1" if pf["include_non_s1"] else "",
        "tier": pf["pipeline_tier_tab"],
    }
    back_href = pp + "?" + urlencode({k: v for k, v in back_qs.items() if v})

    with connect() as conn:
        rows = list_lead_profiles_for_review(
            conn,
            s1_only=not pf["include_non_s1"],
            search=pf["q"],
            limit=800,
            months_back=months_back,
            pay_band="all",
            us_registrant_hq_only=lead_desk_us_registrant_hq_only(),
            listing_stage="all",
            cik=cik,
        )
        outreach_by_norm = list_advisor_lead_outreach_status_by_norm_for_cik(
            conn, int(user["id"]), cik
        )
    from wealth_leads.config import pipeline_blur_comp_columns

    company_name = (
        (rows[0]["company_name"] or "").strip()
        if rows
        else f"CIK {cik}"
    )
    raw_sb = (request.query_params.get("sales_bundle") or "").strip()
    sb_cur = (
        normalize_sales_bundle_query(raw_sb)
        if raw_sb
        else "economy"
    )
    if sb_cur == "premium" and not user_may_view_pipeline_exec_bundle(user):
        return HTMLResponse(
            "<h1>Not included in your access</h1>"
            "<p>The executive roster is sold separately. Use the roster link from your "
            "subscription, or contact the administrator.</p>"
            '<p class="dim"><a href="'
            + html_module.escape(back_href, quote=True)
            + '">Back to pipeline</a></p>',
            status_code=403,
        )
    rows_view = filter_rows_sales_bundle(rows, sb_cur)

    body = render_pipeline_company_page(
        rows=rows_view,
        sales_bundle=sb_cur,
        company_name=company_name,
        back_href=back_href,
        blur_comp=pipeline_blur_comp_columns(),
        outreach_by_norm=outreach_by_norm,
        pipeline_path=_pipeline_url_path(),
        search=pf["q"],
        months=pf["months_sel"],
        include_non_s1=pf["include_non_s1"],
    )
    return HTMLResponse(_inject_chrome(body, user))


@app.post("/admin/pipeline/rebuild")
@app.post("/pipeline/rebuild")
async def admin_pipeline_rebuild(request: Request) -> RedirectResponse:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    user = _session_user(request)
    assert user is not None
    if not user.get("is_admin"):
        return RedirectResponse("/my-leads", status_code=302)
    with connect() as conn:
        st = rebuild_lead_profiles(conn)
    summary = f"Rebuilt lead_profile: {st['rows_written']} rows."
    return RedirectResponse(
        _pipeline_url_path() + "?msg=" + quote(summary),
        status_code=302,
    )


@app.get("/admin/pipeline.csv")
@app.get("/pipeline.csv")
async def admin_pipeline_csv(request: Request) -> Response:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    user = _session_user(request)
    assert user is not None
    if not user.get("is_admin"):
        return HTMLResponse("Forbidden", status_code=403)
    pf = _pipeline_review_params(request)
    months_back = None if pf["months_sel"] <= 0 else pf["months_sel"]
    with connect() as conn:
        rows = list_lead_profiles_for_review(
            conn,
            s1_only=not pf["include_non_s1"],
            search=pf["q"],
            limit=5000,
            months_back=months_back,
            pay_band="all",
            us_registrant_hq_only=lead_desk_us_registrant_hq_only(),
            listing_stage="all",
        )

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(
        [
            "cross_company_hint",
            "cik",
            "person_norm",
            "display_name",
            "title",
            "company_name",
            "form_type_latest",
            "filing_date_latest",
            "accession_latest",
            "issuer_headquarters",
            "issuer_hq_city_state",
            "issuer_hq_has_detail",
            "issuer_industry",
            "headline_year",
            "signal_hwm",
            "hq_city_state",
            "cash_excl_equity_headline",
            "stock_grants_headline",
            "salary_headline",
            "bonus_headline",
            "other_comp_headline",
            "equity_hwm",
            "has_s1_comp",
            "lead_tier",
            "issuer_listing_stage",
            "has_mgmt_bio",
            "has_officer_row",
            "has_beneficial_owner_stake",
            "neo_row_count",
            "comp_llm_assisted",
            "comp_timeline",
            "why_surfaced",
            "primary_doc_url",
            "index_url",
            "other_ciks_json",
            "built_at",
        ]
    )
    for r in rows:
        cash_excl = pipeline_cash_excl_equity_from_row(r)
        w.writerow(
            [
                r["cross_company_hint"],
                r["cik"],
                r["person_norm"],
                r["display_name"],
                r["title"],
                r["company_name"],
                r["form_type_latest"],
                r["filing_date_latest"],
                r["accession_latest"],
                format_headquarters_for_ui(r["issuer_headquarters"]),
                (r["issuer_hq_city_state"] if "issuer_hq_city_state" in r.keys() else "")
                or "",
                r["issuer_hq_has_detail"] if "issuer_hq_has_detail" in r.keys() else "",
                r["issuer_industry"],
                r["headline_year"] if "headline_year" in r.keys() else "",
                r["signal_hwm"],
                pipeline_hq_city_state(r),
                cash_excl if cash_excl is not None else "",
                r["stock_grants_headline"]
                if "stock_grants_headline" in r.keys()
                else "",
                r["salary_headline"] if "salary_headline" in r.keys() else "",
                r["bonus_headline"] if "bonus_headline" in r.keys() else "",
                r["other_comp_headline"] if "other_comp_headline" in r.keys() else "",
                r["equity_hwm"],
                r["has_s1_comp"],
                r["lead_tier"] if "lead_tier" in r.keys() else "premium",
                (r["issuer_listing_stage"] if "issuer_listing_stage" in r.keys() else "")
                or "unknown",
                r["has_mgmt_bio"],
                r["has_officer_row"],
                r["has_beneficial_owner_stake"]
                if "has_beneficial_owner_stake" in r.keys()
                else 0,
                r["neo_row_count"],
                r["comp_llm_assisted"],
                r["comp_timeline"],
                r["why_surfaced"],
                r["primary_doc_url"],
                r["index_url"],
                r["other_ciks_json"],
                r["built_at"],
            ]
        )
    data = buf.getvalue().encode("utf-8")
    return Response(
        content=data,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": 'attachment; filename="lead_profile_export.csv"',
            "Cache-Control": "no-store",
        },
    )


@app.get("/pipeline/row.json")
@app.get("/admin/pipeline/row.json")
async def pipeline_row_json(
    request: Request, cik: str, person_norm: str
) -> Response:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    user = _session_user(request)
    assert user is not None
    if not user.get("is_admin"):
        return JSONResponse({"error": "Forbidden"}, status_code=403)
    with connect() as conn:
        row = get_lead_profile_row(conn, cik, person_norm)
        if row is None:
            return JSONResponse({"error": "Not found"}, status_code=404)
        payload = _lead_profile_row_dict(row)
        payload.update(_pipeline_row_drawer_enrich(conn, row))
        payload["issuer_headquarters"] = format_headquarters_for_ui(
            payload.get("issuer_headquarters")
        )
    return JSONResponse(_json_sanitize_for_response(payload))


@app.get("/pipeline/geocode.json")
@app.get("/admin/pipeline/geocode.json")
async def pipeline_geocode_json(request: Request, q: str) -> Response:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    user = _session_user(request)
    assert user is not None
    if not user.get("is_admin"):
        return JSONResponse({"error": "Forbidden"}, status_code=403)
    if requests is None:
        return JSONResponse(
            {"lat": None, "lon": None, "label": None, "error": "requests not installed"},
            status_code=503,
        )
    q = (q or "").strip()
    if not q or len(q) > 500:
        return JSONResponse({"lat": None, "lon": None, "label": None})
    try:
        r = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": q, "format": "json", "limit": 1},
            headers={
                "User-Agent": "WealthPipeline/1.0 (local SEC lead desk; contact in app README)",
                "Accept-Language": "en",
            },
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
    except Exception:
        return JSONResponse({"lat": None, "lon": None, "label": None})
    if not data:
        return JSONResponse({"lat": None, "lon": None, "label": None})
    first = data[0]
    return JSONResponse(
        {
            "lat": float(first["lat"]),
            "lon": float(first["lon"]),
            "label": first.get("display_name"),
        }
    )


@app.post("/admin/sync")
async def admin_sync_post(request: Request, force: str = Form("")) -> Response:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    user = _session_user(request)
    assert user is not None
    if not user.get("is_admin"):
        return RedirectResponse("/my-leads", status_code=302)
    do_force = force.strip().lower() in ("1", "on", "true", "yes")
    _ok, user_msg = start_sync_subprocess(force=do_force)
    return RedirectResponse("/admin?msg=" + quote(user_msg), status_code=302)


@app.post("/admin/settings")
async def admin_settings_save(
    request: Request,
    max_clients_per_territory: int = Form(1),
    default_monthly_quota: int = Form(30),
    allow_shared_leads_default: str = Form(""),
) -> RedirectResponse:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    user = _session_user(request)
    assert user is not None
    if not user.get("is_admin"):
        return RedirectResponse("/my-leads", status_code=302)
    sh = 1 if allow_shared_leads_default in ("1", "on", "true") else 0
    with connect() as conn:
        update_allocation_settings(
            conn,
            max_clients_per_territory=max(1, int(max_clients_per_territory)),
            default_monthly_quota=max(1, int(default_monthly_quota)),
            allow_shared_leads_default=sh,
        )
    return RedirectResponse("/admin?msg=" + quote("Settings saved."), status_code=302)


@app.post("/admin/allocate")
async def admin_allocate(
    request: Request,
    cycle_yyyymm: str = Form(...),
    replace: str = Form(""),
) -> RedirectResponse:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    user = _session_user(request)
    assert user is not None
    if not user.get("is_admin"):
        return RedirectResponse("/my-leads", status_code=302)
    cy = (cycle_yyyymm or "").strip()
    if len(cy) != 6 or not cy.isdigit():
        return RedirectResponse("/admin?msg=" + quote("Invalid cycle."), status_code=302)
    rep = replace in ("1", "on", "true")
    with connect() as conn:
        profiles_all = _build_profiles(conn)
        desk = _lead_desk_filter_profiles(profiles_all, conn)
        filtered = [
            p
            for p in desk
            if is_acceptable_lead_person_name(_profile_display_name_for_quality_gate(p))
        ]
        stats = assign_for_cycle(
            conn,
            cycle_yyyymm=cy,
            profiles_all=filtered,
            replace=rep,
        )
    sg = int(stats.get("skipped_assignment_gate") or 0)
    summary = (
        f"Cycle {cy}: assigned {stats['assigned']} of {stats['candidates']} candidates; "
        f"skipped no-eligible {stats['skipped_no_eligible']}, exclusive {stats['skipped_exclusive']}"
        + (f", assignment rules {sg}" if sg else "")
        + "."
    )
    return RedirectResponse("/admin?msg=" + quote(summary), status_code=302)


@app.post("/admin/suppress")
async def admin_suppress_post(
    request: Request,
    action: str = Form("add"),
    cik: str = Form(""),
    person_norm: str = Form(""),
    person_name: str = Form(""),
    reason: str = Form(""),
) -> RedirectResponse:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    user = _session_user(request)
    assert user is not None
    if not user.get("is_admin"):
        return RedirectResponse("/my-leads", status_code=302)
    ck = (cik or "").strip()
    pn = (person_norm or "").strip()
    if not pn and (person_name or "").strip():
        pn = _norm_person_name(person_name)
    act = (action or "add").strip().lower()
    if not ck or not pn:
        return RedirectResponse(
            "/admin?msg=" + quote("Suppress: need CIK and person norm (or display name)."),
            status_code=302,
        )
    with connect() as conn:
        if act == "delete":
            delete_lead_suppress(conn, cik=ck, person_norm=pn)
            msg = "Removed from suppress list."
        else:
            insert_lead_suppress(conn, cik=ck, person_norm=pn, reason=reason)
            msg = "Added to suppress list."
    return RedirectResponse("/admin?msg=" + quote(msg), status_code=302)


@app.post("/admin/client/{user_id}")
async def admin_client_save(
    request: Request,
    user_id: int,
    monthly_lead_quota: int = Form(0),
    territory_type: str = Form("state"),
    territory_spec: str = Form(""),
    premium_s1_only: str = Form(""),
    allow_shared_leads: str = Form(""),
    is_admin: str = Form(""),
) -> RedirectResponse:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    admin = _session_user(request)
    assert admin is not None
    if not admin.get("is_admin"):
        return RedirectResponse("/my-leads", status_code=302)
    if user_id == admin["id"] and is_admin not in ("1", "on", "true"):
        return RedirectResponse("/admin?msg=" + quote("You cannot remove your own admin flag."), status_code=302)
    with connect() as conn:
        update_user_allocation_profile(
            conn,
            user_id,
            monthly_lead_quota=max(0, int(monthly_lead_quota)),
            territory_type=territory_type[:32],
            territory_spec=territory_spec[:2000],
            premium_s1_only=1 if premium_s1_only in ("1", "on", "true") else 0,
            allow_shared_leads=1 if allow_shared_leads in ("1", "on", "true") else 0,
            is_admin=1 if is_admin in ("1", "on", "true") else 0,
        )
    return RedirectResponse("/admin?msg=" + quote("Client updated."), status_code=302)


@app.get("/admin/desk", response_model=None)
async def admin_desk(request: Request) -> Response:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    user = _session_user(request)
    assert user is not None
    if not user.get("is_admin"):
        return HTMLResponse("<h1>Forbidden</h1>", status_code=403)
    profiles, _pa, leads, comp, stats = _load_page_data()
    n_prem_co, n_eco_co = desk_sales_bundle_company_counts(profiles)
    sb = normalize_sales_bundle_query(request.query_params.get("sales_bundle"))
    filtered = filter_profiles_sales_bundle(profiles, sb)
    body = _page_desk(
        filtered,
        leads,
        comp,
        stats,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        nav_base_path="/admin/desk",
        desk_universe_count=len(profiles),
        sales_bundle=sb,
        premium_company_count=n_prem_co,
        economy_company_count=n_eco_co,
    )
    return HTMLResponse(_inject_chrome(body, user))


@app.get("/admin/desk/company", response_model=None)
async def admin_desk_company(request: Request) -> Response:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    user = _session_user(request)
    assert user is not None
    if not user.get("is_admin"):
        return HTMLResponse("<h1>Forbidden</h1>", status_code=403)
    cik_w = (request.query_params.get("cik") or "").strip()
    if not cik_w:
        return HTMLResponse(
            "<h1>Bad request</h1><p>Missing <code>cik</code>. Open the desk and click a company.</p>",
            status_code=400,
        )
    profiles, _pa, leads, comp, stats = _load_page_data(omit_audit_tables=True)
    at_cik = [p for p in profiles if str(p.get("cik") or "").strip() == cik_w]
    filtered_cik = list(at_cik)
    filtered_cik.sort(key=_desk_sort_tuple, reverse=True)
    sb = normalize_sales_bundle_query(request.query_params.get("sales_bundle"))
    filtered_cik = filter_profiles_sales_bundle(filtered_cik, sb)
    co_name = ""
    if filtered_cik:
        co_name = (filtered_cik[0].get("company_name") or "").strip()
    elif at_cik:
        co_name = (at_cik[0].get("company_name") or "").strip()
    body = _page_desk_company(
        filtered_cik,
        leads,
        comp,
        stats,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        cik=cik_w,
        company_name=co_name,
        nav_base_path="/admin/desk",
        sales_bundle=sb,
    )
    return HTMLResponse(_inject_chrome(body, user))


@app.get("/admin/finder", response_model=None)
async def admin_finder(
    request: Request,
    hq: str = "",
    industry: str = "",
    q: str = "",
    all_neo: int = 0,
) -> Response:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    user = _session_user(request)
    assert user is not None
    if not user.get("is_admin"):
        return HTMLResponse("<h1>Forbidden</h1>", status_code=403)
    profiles, profiles_all, _leads, _comp, stats = _load_page_data(
        omit_audit_tables=True
    )
    use_all = bool(all_neo)
    base = profiles_all if use_all else profiles
    filtered = filter_profiles_geo_industry_text(
        base,
        location_sub=hq,
        industry_sub=industry,
        text_sub=q,
    )
    body = _page_finder(
        filtered,
        stats=stats,
        rendered_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        hq=hq,
        industry=industry,
        q=q,
        all_neo=use_all,
        base_count=len(base),
        nav_base_path="/admin/finder",
        form_action="/admin/finder",
        desk_href="/admin/desk",
        export_path="/export/finder.csv",
    )
    return HTMLResponse(_inject_chrome(body, user))


@app.get("/finder", response_model=None)
async def finder_legacy_redirect(request: Request) -> Response:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    user = _session_user(request)
    assert user is not None
    if user.get("is_admin"):
        q = request.url.query
        dest = "/admin/finder" + (f"?{q}" if q else "")
        return RedirectResponse(dest, status_code=302)
    return RedirectResponse("/my-leads", status_code=302)


@app.get("/lead", response_model=None)
async def lead(
    request: Request,
    cik: str = "",
    name: str = "",
    msg: str = "",
) -> Response:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    user = _session_user(request)
    assert user is not None
    from wealth_leads.serve import _norm_person_name

    norm = _norm_person_name(name)
    lead_page_msg = (msg or "").strip()
    if not user.get("is_admin"):
        with connect() as conn:
            ok = conn.execute(
                """
                SELECT 1 FROM lead_assignments
                WHERE user_id = ? AND cik = ? AND person_norm = ? LIMIT 1
                """,
                (user["id"], (cik or "").strip(), norm),
            ).fetchone()
        if not ok:
            return HTMLResponse(
                "<h1>Not assigned</h1><p>This profile is not on your assigned list. "
                "Open <a href='/my-leads'>My leads</a>.</p>",
                status_code=403,
            )
    try:
        dbp = database_path()
        if not os.path.isfile(dbp):
            stats = {
                "missing_db": True,
                "profile_count": 0,
                "profile_count_all": 0,
                "filings": 0,
                "officers": 0,
                "comp_rows": 0,
                "lead_desk_s1_only": False,
                "lead_desk_min_signal_usd": 0.0,
                "lead_desk_equity_only_legacy": False,
                "latest_filing_date": None,
                "db_file_modified": "—",
            }
            profiles_all: list[dict] = []
        else:
            mtime = datetime.fromtimestamp(os.path.getmtime(dbp)).strftime("%Y-%m-%d %H:%M")
            cik_s = (cik or "").strip()
            with connect() as conn:
                stats = _lead_page_db_stats(conn, db_mtime=mtime)
                if not conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='neo_compensation'"
                ).fetchone():
                    profiles_all = []
                elif cik_s:
                    profiles_all = _build_profiles(conn, cik_filter=cik_s)
                else:
                    profiles_all = []
        prof = _find_profile(profiles_all, cik, norm) if not stats.get("missing_db") else None
        filings: list[dict] = []
        cr_dict: dict[str, Any] | None = None
        issuer_snap: dict[str, Any] | None = None
        beneficial_rows: list[dict[str, Any]] = []
        important_llm: list[dict[str, Any]] = []
        beneficial_stake_detail: dict[str, Any] | None = None
        beneficial_filing_caveats = ""
        lead_recency: dict[str, Any] = {}
        lead_feedback_html = ""
        lead_outreach_html = ""
        if prof is not None and not stats.get("missing_db"):
            with connect() as conn:
                filings = _filings_for_profile(conn, cik, norm)
                cr_row = get_lead_client_research(conn, (cik or "").strip(), norm)
                cr_dict = row_to_client_research_dict(cr_row)
                issuer_snap = get_issuer_snapshot_dict(conn, (cik or "").strip())
                bo_only = bool(prof.get("has_s1_beneficial_owner")) and not bool(
                    prof.get("has_s1_officer")
                )
                if bo_only:
                    try:
                        beneficial_stake_detail = beneficial_stake_detail_for_profile(
                            conn, prof
                        )
                    except sqlite3.Error:
                        beneficial_stake_detail = None
                    try:
                        beneficial_filing_caveats = _latest_filing_snapshot_caveats_for_cik(
                            conn, (cik or "").strip()
                        )
                    except sqlite3.Error:
                        beneficial_filing_caveats = ""
                    try:
                        important_llm = _latest_important_people_from_s1_llm_pack(
                            conn, (cik or "").strip()
                        )
                    except sqlite3.Error:
                        important_llm = []
                else:
                    try:
                        beneficial_rows = [
                            dict(r)
                            for r in list_beneficial_owner_outreach_targets_for_cik(
                                conn, (cik or "").strip(), limit=10
                            )
                        ]
                    except sqlite3.Error:
                        beneficial_rows = []
                    try:
                        important_llm = _latest_important_people_from_s1_llm_pack(
                            conn, (cik or "").strip()
                        )
                    except sqlite3.Error:
                        important_llm = []
                try:
                    lead_recency = lead_issuer_recency_bundle(
                        conn, (cik or "").strip()
                    )
                except sqlite3.Error:
                    lead_recency = {}
            pn_fb = (prof.get("norm_name") or "").strip() or norm
            ck_fb = (cik or "").strip()
            if pn_fb and ck_fb:
                lead_feedback_html = lead_feedback_form_html(ck_fb, pn_fb)
                uid = user.get("id")
                if uid is not None:
                    with connect() as conn_o:
                        row_o = get_advisor_lead_outreach(conn_o, int(uid), ck_fb, pn_fb)
                    cur_o: dict[str, str] | None = None
                    if row_o is not None:
                        cur_o = {
                            "email": str(row_o["email"] or ""),
                            "status": str(row_o["status"] or "none"),
                            "notes": str(row_o["notes"] or ""),
                        }
                    lead_outreach_html = lead_outreach_form_html(
                        ck_fb, pn_fb, current=cur_o
                    )
        body = _page_lead(
            prof,
            filings,
            query_cik=cik,
            query_name=name,
            stats=stats,
            rendered_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            client_research=cr_dict,
            issuer_snapshot=issuer_snap,
            beneficial_gems=beneficial_rows,
            important_people_llm=important_llm,
            beneficial_stake_detail=beneficial_stake_detail,
            beneficial_filing_caveats=beneficial_filing_caveats,
            lead_recency=lead_recency,
            lead_feedback_html=lead_feedback_html,
            lead_outreach_html=lead_outreach_html,
            lead_page_msg=lead_page_msg,
        )
        return HTMLResponse(_inject_chrome(body, user))
    except Exception as e:
        traceback.print_exc()
        return HTMLResponse(
            "<h1>Profile error</h1><p>Something went wrong rendering this lead. "
            "Check the server log for a traceback.</p>"
            f"<p style='color:#8b96a3;font-size:0.85rem'>{html_module.escape(str(e))}</p>",
            status_code=500,
        )


@app.post("/lead/feedback", response_model=None)
async def lead_feedback_post(
    request: Request,
    cik: str = Form(...),
    person_norm: str = Form(...),
    category: str = Form("other"),
    note: str = Form(""),
    also_suppress: str = Form(""),
) -> Response:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    user = _session_user(request)
    assert user is not None
    ck = (cik or "").strip()
    pn = (person_norm or "").strip()
    if not ck or not pn:
        return HTMLResponse("<h1>Bad request</h1><p>Missing cik or person_norm.</p>", 400)
    if not user.get("is_admin"):
        with connect() as conn:
            ok = conn.execute(
                """
                SELECT 1 FROM lead_assignments
                WHERE user_id = ? AND cik = ? AND person_norm = ? LIMIT 1
                """,
                (user["id"], ck, pn),
            ).fetchone()
        if not ok:
            return HTMLResponse(
                "<h1>Forbidden</h1><p>Feedback only for leads assigned to you.</p>",
                status_code=403,
            )
    email = str(user.get("email") or "").strip()
    suppress = also_suppress.strip().lower() in ("1", "on", "true", "yes")
    try:
        with connect() as conn:
            insert_lead_advisor_feedback(
                conn,
                cik=ck,
                person_norm=pn,
                category=category,
                note=note,
                user_email=email,
                also_suppress=suppress,
            )
    except sqlite3.Error:
        return HTMLResponse(
            "<h1>Database error</h1><p>Could not save feedback.</p>", status_code=500
        )
    q = urlencode(
        {
            "cik": ck,
            "name": pn,
            "msg": "Feedback recorded — thank you.",
        }
    )
    return RedirectResponse(f"/lead?{q}", status_code=302)


@app.post("/lead/outreach", response_model=None)
async def lead_outreach_post(
    request: Request,
    cik: str = Form(...),
    person_norm: str = Form(...),
    email: str = Form(""),
    status: str = Form("none"),
    notes: str = Form(""),
) -> Response:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    user = _session_user(request)
    assert user is not None
    ck = (cik or "").strip()
    pn = (person_norm or "").strip()
    if not ck or not pn:
        return HTMLResponse("<h1>Bad request</h1><p>Missing cik or person_norm.</p>", 400)
    uid = user.get("id")
    if uid is None:
        return HTMLResponse(
            "<h1>Forbidden</h1><p>Sign in to save outreach.</p>", status_code=403
        )
    if not user.get("is_admin"):
        with connect() as conn:
            ok = conn.execute(
                """
                SELECT 1 FROM lead_assignments
                WHERE user_id = ? AND cik = ? AND person_norm = ? LIMIT 1
                """,
                (int(uid), ck, pn),
            ).fetchone()
        if not ok:
            return HTMLResponse(
                "<h1>Forbidden</h1><p>Outreach only for leads assigned to you.</p>",
                status_code=403,
            )
    try:
        with connect() as conn:
            upsert_advisor_lead_outreach(
                conn,
                int(uid),
                ck,
                pn,
                email=email,
                status=status,
                notes=notes,
            )
    except sqlite3.Error:
        return HTMLResponse(
            "<h1>Database error</h1><p>Could not save outreach.</p>", status_code=500
        )
    q = urlencode(
        {
            "cik": ck,
            "name": pn,
            "msg": "Outreach saved.",
        }
    )
    return RedirectResponse(f"/lead?{q}", status_code=302)


@app.get("/watchlist", response_model=None)
async def watchlist_get(request: Request) -> Response:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    user = _session_user(request)
    assert user is not None
    if user.get("_synthetic"):
        return RedirectResponse(
            _pipeline_url_path()
            + "?msg="
            + quote("Sign in (set WEALTH_LEADS_REQUIRE_AUTH=1) to use the watchlist."),
            status_code=302,
        )
    with connect() as conn:
        rows = list_user_watchlist(conn, user["id"])
    trs = []
    for r in rows:
        q = urlencode({"cik": r["cik"], "name": r["person_norm"] or ""})
        link = f"/lead?{q}"
        trs.append(
            "<tr>"
            f"<td>{html_module.escape(r['cik'])}</td>"
            f"<td>{html_module.escape(r['person_norm'] or '— (whole issuer)')}</td>"
            f"<td>{html_module.escape(r['label'] or '')}</td>"
            f"<td><a href=\"{link}\">Open</a></td>"
            f"<td><form method=\"post\" action=\"/watchlist/{int(r['id'])}/delete\" style=\"display:inline\">"
            '<button type="submit">Remove</button></form></td>'
            "</tr>"
        )
    tbl = (
        "<table style='width:100%;border-collapse:collapse;font-size:0.85rem'>"
        "<thead><tr><th>CIK</th><th>Person (norm)</th><th>Label</th><th></th><th></th></tr></thead>"
        f"<tbody>{''.join(trs) if trs else '<tr><td colspan=5 style=\"color:#6b7785\">No saved watches.</td></tr>'}</tbody></table>"
    )
    page = f"""<!DOCTYPE html><html><head><meta charset="utf-8"/><title>Watchlist</title>
<style>body{{font-family:system-ui;background:#0d1117;color:#e8ecf0;max-width:48rem;margin:1rem auto;padding:1rem}}
input,button{{font:inherit}} label{{display:block;margin-top:0.75rem;color:#8b96a3;font-size:0.8rem}}
input{{padding:0.4rem;width:100%;max-width:20rem;border-radius:4px;border:1px solid #2a3340;background:#0a0e12;color:#e8ecf0}}
button{{margin-top:0.5rem;padding:0.35rem 0.65rem;border-radius:4px;border:1px solid #2a3340;background:#1a2634;color:#d8dee4;cursor:pointer}}
a{{color:#58a6ff}}</style></head><body>
<h1>Watchlist</h1>
<p>Save CIKs or specific people to reopen their profile quickly. Person name should match the desk display name for best results.</p>
{tbl}
<h2 style="margin-top:2rem">Add</h2>
<form method="post" action="/watchlist/add">
<label>CIK</label><input name="cik" required pattern="[0-9]{{1,10}}" placeholder="2064947"/>
<label>Person name (optional)</label><input name="person_name" placeholder="Jason Long"/>
<label>Note (optional)</label><input name="label" placeholder="RSU follow-up"/>
<button type="submit">Save to watchlist</button>
</form>
<p style="margin-top:1.5rem"><a href="/my-leads">← My leads</a></p>
</body></html>"""
    return HTMLResponse(_inject_chrome(page, user))


@app.post("/watchlist/add")
async def watchlist_add(
    request: Request,
    cik: str = Form(...),
    person_name: str = Form(""),
    label: str = Form(""),
) -> RedirectResponse:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    user = _session_user(request)
    assert user is not None
    if user.get("_synthetic"):
        return RedirectResponse(
            _pipeline_url_path()
            + "?msg="
            + quote("Sign in (set WEALTH_LEADS_REQUIRE_AUTH=1) to use the watchlist."),
            status_code=302,
        )
    from wealth_leads.serve import _norm_person_name

    pn = _norm_person_name(person_name) if person_name.strip() else ""
    with connect() as conn:
        add_user_watchlist(conn, user["id"], cik=cik, person_norm=pn, label=label)
    return RedirectResponse("/watchlist", status_code=302)


@app.post("/watchlist/{item_id}/delete")
async def watchlist_del(request: Request, item_id: int) -> RedirectResponse:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    user = _session_user(request)
    assert user is not None
    if user.get("_synthetic"):
        return RedirectResponse(
            _pipeline_url_path()
            + "?msg="
            + quote("Sign in (set WEALTH_LEADS_REQUIRE_AUTH=1) to use the watchlist."),
            status_code=302,
        )
    with connect() as conn:
        delete_user_watchlist(conn, user["id"], item_id)
    return RedirectResponse("/watchlist", status_code=302)


@app.get("/export/finder.csv", response_model=None)
async def export_finder_csv(
    request: Request,
    hq: str = "",
    industry: str = "",
    q: str = "",
    all_neo: int = 0,
) -> Response:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    u = _session_user(request)
    assert u is not None
    if not u.get("is_admin"):
        return HTMLResponse("<h1>Forbidden</h1><p>Admin only.</p>", status_code=403)
    profiles, profiles_all, _l, _c, _s = _load_page_data(omit_audit_tables=True)
    data = finder_export_csv_bytes(
        profiles_all=profiles_all,
        profiles_desk=profiles,
        hq=hq,
        industry=industry,
        q=q,
        all_neo=bool(all_neo),
    )

    def gen():
        yield data

    return StreamingResponse(
        gen(),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": 'attachment; filename="wealthpipeline-finder.csv"',
            "Cache-Control": "no-store",
        },
    )


@app.get("/export/desk.csv", response_model=None)
async def export_desk_csv(
    request: Request,
    sales_bundle: str = "premium",
) -> Response:
    redir = _auth_redirect(request)
    if redir is not None:
        return redir
    u = _session_user(request)
    assert u is not None
    if not u.get("is_admin"):
        return HTMLResponse("<h1>Forbidden</h1><p>Admin only.</p>", status_code=403)
    profiles, _pa, _leads, _comp, _stats = _load_page_data(omit_audit_tables=True)
    sb_cur = normalize_sales_bundle_query(sales_bundle)
    desk_rows = filter_profiles_sales_bundle(profiles, sb_cur)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(
        [
            "sales_bundle_filter",
            "display_name",
            "company_name",
            "cik",
            "title",
            "officer_age_est",
            "latest_total_usd",
            "max_equity_usd",
            "headline_fy",
            "filing_date",
            "lead_tier",
            "issuer_listing_stage",
            "profile_path",
        ]
    )
    for p in desk_rows:
        w.writerow(
            [
                sb_cur,
                p.get("display_name") or "",
                p.get("company_name") or "",
                p.get("cik") or "",
                p.get("title") or "",
                p.get("officer_age") if p.get("officer_age") is not None else "",
                p.get("total") if p.get("total") is not None else "",
                p.get("equity_hwm") if p.get("equity_hwm") is not None else "",
                p.get("headline_year") or "",
                p.get("filing_date") or "",
                p.get("lead_tier") or _profile_lead_tier(p),
                (p.get("issuer_listing_stage") or "unknown").strip().lower(),
                _profile_lead_url(p),
            ]
        )
    data = buf.getvalue().encode("utf-8")

    def gen():
        yield data

    return StreamingResponse(
        gen(),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": 'attachment; filename="wealthpipeline-desk.csv"',
            "Cache-Control": "no-store",
        },
    )


@app.get("/healthz")
async def healthz() -> dict:
    """Cheap liveness probe (load balancers / Docker). Does not open SQLite — safe if DB is locked."""
    return {"ok": True, "db": str(database_path())}
