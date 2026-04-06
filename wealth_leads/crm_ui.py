"""HTML fragments for territory allocation / client CRM UI."""
from __future__ import annotations

import html as html_module
import math
import re
from typing import Any
from urllib.parse import quote, urlencode

from wealth_leads.config import advisor_ui_product_name
from wealth_leads.serve import (
    _norm_person_name,
    normalize_listing_stage_query,
    sales_bundle_tab_label,
)
from wealth_leads.territory import (
    hq_principal_office_display_line,
    issuer_hq_city_state_for_ui,
    issuer_hq_city_state_materialized_ok,
    normalize_registrant_hq_address_blob,
)
from wealth_leads.title_badge import advisor_title_badge


def _esc(s: Any) -> str:
    return html_module.escape(str(s) if s is not None else "")


def _attr(s: Any) -> str:
    """Escape for HTML attribute values (incl. quotes)."""
    return html_module.escape(str(s) if s is not None else "", quote=True)


def format_headquarters_for_ui(raw: str | None) -> str:
    """Collapse multiline SEC principal-office blocks into one line for UI and geocoding."""
    s = (raw if raw is not None else "").strip()
    if not s:
        return ""
    parts = [p.strip() for p in re.split(r"[\n\r]+", s) if p.strip()]
    one = ", ".join(parts)
    one = re.sub(r"[ \t]{2,}", " ", one).strip()
    return normalize_registrant_hq_address_blob(one)


def render_my_leads_page(
    *,
    rows: list[dict[str, Any]],
    cycle: str,
    tag_filter: str,
    quota: int,
    delivered: int,
    territory_type: str,
    territory_spec: str,
    premium_s1_only: bool,
) -> str:
    opt_s1 = " selected" if tag_filter == "s1" else ""
    opt_rsu = " selected" if tag_filter == "rsu" else ""
    trs: list[str] = []
    for r in rows:
        prof = r.get("profile") or {}
        snap = r.get("snapshot") or {}
        name = prof.get("display_name") or snap.get("display_name") or "—"
        co = prof.get("company_name") or snap.get("company_name") or "—"
        title = prof.get("title") or snap.get("title") or "—"
        hq_raw = prof.get("issuer_headquarters") or snap.get("issuer_headquarters") or ""
        hq = format_headquarters_for_ui(hq_raw) or "—"
        eq = prof.get("equity_hwm")
        if eq is None:
            eq = snap.get("equity_hwm")
        eq_s = f"${float(eq):,.0f}" if eq is not None else "—"
        tags = r.get("tags") or []
        tag_s = ", ".join(tags) if tags else "—"
        nm_lead = (r.get("person_norm") or "").strip() or (prof.get("norm_name") or "").strip()
        if not nm_lead:
            nm_lead = prof.get("display_name") or snap.get("display_name") or ""
        nm_lead = _norm_person_name(nm_lead)
        q = urlencode({"cik": r.get("cik") or "", "name": nm_lead})
        lead_href = f"/lead?{q}"
        ang = (r.get("outreach_angle") or "")[:140]
        trs.append(
            "<tr>"
            f"<td><strong>{_esc(name)}</strong><div class='sub'>{_esc(r.get('why_summary', '')[:200])}{'…' if len(r.get('why_summary', '')) > 200 else ''}</div></td>"
            f"<td>{_esc(co)}</td>"
            f"<td>{_esc(title)}</td>"
            f"<td class='dim'>{_esc(hq[:120] if len(hq) > 120 else hq)}</td>"
            f"<td class='num'>{_esc(eq_s)}</td>"
            f"<td>{_esc(r.get('liquidity_stage', '')[:80])}</td>"
            f"<td>{_esc(tag_s)}</td>"
            f"<td class='dim'>{_esc(ang)}{'…' if len(r.get('outreach_angle') or '') > 140 else ''}</td>"
            f"<td><a href=\"{_esc(lead_href)}\">Profile</a></td>"
            "</tr>"
        )
    body = "".join(trs) if trs else (
        "<tr><td colspan='9' class='dim'>No leads assigned for this cycle yet. "
        "Your administrator runs monthly allocation.</td></tr>"
    )
    tf = quote(tag_filter)
    cy = quote(cycle)
    export_href = f"/export/my-leads.csv?cycle={cy}&tag={tf}"
    prem = "Yes (S-1–tagged only)" if premium_s1_only else "No"
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>My assigned leads</title>
<style>
body{{font-family:system-ui;background:#0d1117;color:#e8ecf0;max-width:1200px;margin:0 auto;padding:1rem}}
h1{{font-size:1.25rem;font-weight:600}}
.meta{{color:#8b96a3;font-size:0.85rem;line-height:1.5;margin:0.5rem 0 1rem}}
.cards{{display:flex;flex-wrap:wrap;gap:0.75rem;margin-bottom:1rem}}
.card{{background:#121820;border:1px solid #2a3340;border-radius:6px;padding:0.65rem 0.85rem;min-width:10rem}}
.card .lbl{{font-size:0.7rem;color:#6b7785;text-transform:uppercase}}
.card .val{{font-size:1.05rem;font-weight:600;margin-top:0.2rem}}
table{{width:100%;border-collapse:collapse;font-size:0.8rem}}
th,td{{text-align:left;padding:0.45rem 0.5rem;border-bottom:1px solid #2a3340;vertical-align:top}}
th{{color:#8b96a3;font-weight:600}}
.num{{text-align:right;font-variant-numeric:tabular-nums}}
.dim{{color:#6b7785}}
.sub{{font-size:0.72rem;color:#6b7785;margin-top:0.35rem;line-height:1.4}}
a{{color:#58a6ff;text-decoration:none}} a:hover{{text-decoration:underline}}
select,button{{font:inherit;padding:0.35rem 0.5rem;border-radius:4px;border:1px solid #2a3340;background:#1a2634;color:#d8dee4}}
form.inline{{display:inline;margin-left:0.5rem}}
</style></head><body>
<h1>Your assigned leads</h1>
<p class="meta">Curated deal flow for this billing cycle — not a global directory. Sources are public SEC filings; verify before outreach.</p>
<div class="cards">
<div class="card"><div class="lbl">Territory</div><div class="val">{_esc(territory_type)} · {_esc(territory_spec or '—')}</div></div>
<div class="card"><div class="lbl">Monthly quota</div><div class="val">{quota}</div></div>
<div class="card"><div class="lbl">Delivered this cycle</div><div class="val">{delivered}</div></div>
<div class="card"><div class="lbl">Premium S-1 only</div><div class="val">{_esc(prem)}</div></div>
</div>
<p class="meta">
<form method="get" action="/my-leads" class="inline">
<label>Cycle </label>
<input type="text" name="cycle" value="{_esc(cycle)}" pattern="\\d{{6}}" size="8" style="background:#0a0e12;border:1px solid #2a3340;color:#e8ecf0;border-radius:4px;padding:0.25rem"/>
<label> Filter </label>
<select name="tag">
<option value="">All</option>
<option value="s1"{opt_s1}>S-1 tagged</option>
<option value="rsu"{opt_rsu}>Pre-IPO / RSU</option>
</select>
<button type="submit">Apply</button>
</form>
<a href="{_esc(export_href)}" style="margin-left:1rem">Download CSV</a>
</p>
<div style="overflow-x:auto">
<table>
<thead><tr>
<th>Why it matters / person</th><th>Company</th><th>Role</th><th>Location (registrant HQ)</th><th>Est. equity (FY)</th><th>Liquidity stage</th><th>Tags</th><th>Suggested angle</th><th></th>
</tr></thead>
<tbody>{body}</tbody>
</table>
</div>
<p class="meta">Outreach angles and email: SEC filings do not include personal email; use your CRM. Suggested angle is shown on the profile page.</p>
</body></html>"""


def _render_lead_suppress_admin_card(rows: list[Any]) -> str:
    """Manual suppress list: hide (CIK, person_norm) from desk, pipeline rebuild, and allocation."""
    trs: list[str] = []
    for r in rows:
        ck = _esc(r["cik"])
        pn = _esc(r["person_norm"])
        rs = _esc((r["reason"] or "")[:120])
        ca = _esc(r["created_at"] or "")
        raw_ck = str(r["cik"] or "")
        raw_pn = str(r["person_norm"] or "")
        trs.append(
            f"<tr><td><code>{ck}</code></td><td><code>{pn}</code></td>"
            f"<td>{rs}</td><td class='dim'>{ca}</td>"
            "<td><form method='post' action='/admin/suppress' style='margin:0'>"
            "<input type='hidden' name='action' value='delete'/>"
            f"<input type='hidden' name='cik' value='{_attr(raw_ck)}'/>"
            f"<input type='hidden' name='person_norm' value='{_attr(raw_pn)}'/>"
            "<button type='submit' style='font:inherit;font-size:0.75rem;padding:0.2rem 0.45rem'>Remove</button>"
            "</form></td></tr>"
        )
    body = "".join(trs) if trs else "<tr><td colspan='5' class='dim'>No suppressed leads.</td></tr>"
    return f"""
<div class="card">
<h2 style="margin-top:0;font-size:1rem">Suppress leads (manual)</h2>
<p class="meta">Rows here are removed from the <strong>lead desk</strong>, <strong>pipeline materialization</strong>, and <strong>monthly allocation</strong>.
Use the same <code>person_norm</code> as in URLs (e.g. <code>/lead?name=…</code>) — usually lowercase tokens.</p>
<form method="post" action="/admin/suppress" style="display:grid;gap:0.35rem;max-width:28rem;margin:0.5rem 0">
<input type="hidden" name="action" value="add"/>
<label>CIK <input name="cik" required pattern="\\d{{10}}" placeholder="0001234567" style="width:100%"/></label>
<label>Person norm <input name="person_norm" placeholder="jane doe (or leave blank and use display name below)" style="width:100%"/></label>
<label>Display name (optional — used if norm empty) <input name="person_name" placeholder="Jane Doe" style="width:100%"/></label>
<label>Reason <input name="reason" placeholder="e.g. bad lead / duplicate" style="width:100%"/></label>
<button type="submit">Add to suppress list</button>
</form>
<table>
<thead><tr><th>CIK</th><th>person_norm</th><th>Reason</th><th>Added</th><th></th></tr></thead>
<tbody>{body}</tbody>
</table>
</div>"""


def _render_advisor_feedback_admin_card(rows: list[Any]) -> str:
    """Recent advisor-submitted flags (wrong person, duplicate, etc.) from lead pages."""
    trs: list[str] = []
    for r in rows:
        ck = str(r["cik"] or "")
        pn = str(r["person_norm"] or "")
        lead_q = urlencode({"cik": ck, "name": pn})
        trs.append(
            "<tr>"
            f"<td><code>{_esc(ck)}</code></td>"
            f"<td><code>{_esc(pn)}</code></td>"
            f"<td>{_esc(r['category'] or '')}</td>"
            f"<td>{_esc((r['note'] or '')[:200])}</td>"
            f"<td class='dim'>{_esc(r['user_email'] or '')}</td>"
            f"<td>{'Y' if int(r['also_suppress'] or 0) else ''}</td>"
            f"<td class='dim'>{_esc(r['created_at'] or '')}</td>"
            f"<td><a href=\"/lead?{html_module.escape(lead_q)}\">Open</a></td>"
            "</tr>"
        )
    body = (
        "".join(trs)
        if trs
        else "<tr><td colspan='8' class='dim'>No advisor feedback yet.</td></tr>"
    )
    return f"""
<div class="card">
<h2 style="margin-top:0;font-size:1rem">Advisor feedback (recent)</h2>
<p class="meta">Submitted from the <strong>Advisor feedback</strong> panel on <code>/lead</code>. Review categories and optional suppressions.</p>
<table>
<thead><tr><th>CIK</th><th>person_norm</th><th>Category</th><th>Note</th><th>User</th><th>Supp</th><th>When</th><th></th></tr></thead>
<tbody>{body}</tbody>
</table>
</div>"""


def render_admin_home(
    *,
    clients: list[Any],
    settings_row: Any,
    cycle: str,
    alloc_msg: str,
    filing_count: int | None = None,
    sync_info: dict[str, Any] | None = None,
    suppress_rows: list[Any] | None = None,
    advisor_feedback_rows: list[Any] | None = None,
) -> str:
    mc = int(settings_row["max_clients_per_territory"] or 1)
    dq = int(settings_row["default_monthly_quota"] or 30)
    sh = int(settings_row["allow_shared_leads_default"] or 0)
    rows = []
    for c in clients:
        uid = int(c["id"])
        tt = (c["territory_type"] or "state").strip().lower()
        rows.append(
            f"""<tr>
<td>{_esc(c["email"])}</td>
<td>{"Y" if int(c["is_admin"] or 0) else ""}</td>
<td>{int(c["monthly_lead_quota"] or 0)}</td>
<td>{_esc(tt)}</td>
<td>{_esc((c["territory_spec"] or "")[:48])}</td>
<td>{"Y" if int(c.get("premium_s1_only") or 0) else ""}</td>
<td>{"Y" if int(c.get("allow_shared_leads") or 0) else ""}</td>
<td><form method="post" action="/admin/client/{uid}" style="display:grid;gap:0.25rem;max-width:18rem">
<input type="hidden" name="cycle_hint" value="{_esc(cycle)}"/>
<label class="sr">Quota</label>
<input name="monthly_lead_quota" type="number" min="0" max="500" value="{int(c["monthly_lead_quota"] or 0)}" style="width:100%"/>
<label class="sr">Territory type</label>
<select name="territory_type"><option value="state"{" selected" if tt=="state" else ""}>state</option>
<option value="zips"{" selected" if tt=="zips" else ""}>zips</option>
<option value="metro"{" selected" if tt=="metro" else ""}>metro</option></select>
<input name="territory_spec" value="{_esc(c["territory_spec"] or "")}" placeholder="CA or zip list or metro"/>
<label><input type="checkbox" name="premium_s1_only" value="1"{" checked" if int(c.get("premium_s1_only") or 0) else ""}/> S-1 leads only</label>
<label><input type="checkbox" name="allow_shared_leads" value="1"{" checked" if int(c.get("allow_shared_leads") or 0) else ""}/> Allow shared leads</label>
<label><input type="checkbox" name="is_admin" value="1"{" checked" if int(c["is_admin"] or 0) else ""}/> Admin</label>
<button type="submit">Save</button>
</form></td>
</tr>"""
        )
    tbl = "\n".join(rows) if rows else "<tr><td colspan='8' class='dim'>No users</td></tr>"
    msg = f"<p class='ok'>{_esc(alloc_msg)}</p>" if alloc_msg else ""
    si = sync_info or {}
    sph = str(si.get("phase") or "idle")
    sm = str(si.get("message") or "")
    sync_running = sph == "running"
    sync_ok = sph == "ok"
    sync_err = sph == "error"
    sync_note = ""
    if sync_running:
        sync_note = (
            "<p class='warn'><strong>SEC sync is running</strong> — this page reloads every 10s until it finishes. "
            "You can use other tabs meanwhile.</p>"
        )
    elif sync_ok and sm:
        sync_note = f"<p class='ok'>Last sync finished OK.</p><pre class='log'>{_esc(sm)}</pre>"
    elif sync_err:
        sync_note = f"<p class='err'><strong>Last sync failed.</strong></p><pre class='log'>{_esc(sm)}</pre>"
    body_attr = ' data-sync-running="1"' if sync_running else ""
    reload_js = (
        "<script>setTimeout(function(){ location.reload(); }, 10000);</script>"
        if sync_running
        else ""
    )
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/><title>Admin — allocation</title>
<style>
body{{font-family:system-ui;background:#0d1117;color:#e8ecf0;max-width:1400px;margin:0 auto;padding:1rem}}
h1{{font-size:1.2rem}}
table{{width:100%;border-collapse:collapse;font-size:0.78rem}}
th,td{{border-bottom:1px solid #2a3340;padding:0.4rem;vertical-align:top}}
th{{color:#8b96a3;text-align:left}}
.ok{{color:#7dd97d}} .err{{color:#c98a7a}} .warn{{color:#d4a72c}}
.meta{{color:#8b96a3;font-size:0.82rem;line-height:1.45;margin:0.35rem 0}}
.dim{{color:#6b7785}}
input,select,button{{font:inherit;font-size:0.8rem}}
a{{color:#58a6ff}}
code{{font-size:0.78rem;background:#0a0e12;padding:0.1rem 0.35rem;border-radius:3px}}
.card{{background:#121820;border:1px solid #2a3340;border-radius:6px;padding:1rem;margin:1rem 0;max-width:42rem}}
.sr{{position:absolute;width:1px;height:1px;overflow:hidden}}
pre.log{{white-space:pre-wrap;word-break:break-word;font-size:0.72rem;background:#0a0e12;padding:0.5rem;border-radius:4px;max-height:12rem;overflow:auto;border:1px solid #2a3340}}
</style></head><body{body_attr}>
<h1>Admin — territories & allocation</h1>
<p class="meta"><a href="/my-leads">My leads</a> · <a href="/admin/desk">Desk</a> · <a href="/admin/pipeline">Pipeline</a> · <a href="/admin/finder">Finder</a></p>
{msg}
{sync_note}
<div class="card">
<h2 style="margin-top:0;font-size:1rem">SEC data — sync from here</h2>
<p class="meta">Sync loads SEC filings and rebuilds <code>lead_profile</code>. Browse from <a href="/admin/desk">desk</a> or <a href="/admin/pipeline">pipeline</a> after.</p>
<p class="meta"><strong>Filings in DB:</strong> {_esc(filing_count) if filing_count is not None else "unknown"}</p>
<form method="post" action="/admin/sync" style="display:flex;flex-wrap:wrap;gap:0.75rem;align-items:center;margin-top:0.5rem">
<button type="submit" style="padding:0.45rem 0.9rem;border-radius:4px;border:none;background:#238636;color:#fff;cursor:pointer;font-weight:600" {"disabled" if sync_running else ""}>Run SEC sync now</button>
<label style="font-size:0.8rem;color:#8b96a3"><input type="checkbox" name="force" value="1"/> Force reprocess (slow; re-fetches filings)</label>
</form>
<p class="meta dim">Background sync (default 24h). Log: <code>logs\\sec-sync.log</code>. Disable: <code>WEALTH_LEADS_AUTO_SYNC_HOURS=0</code>. If blocked, set <code>SEC_USER_AGENT</code> with contact email.</p>
</div>
{reload_js}
<div class="card">
<h2 style="margin-top:0;font-size:1rem">Global settings</h2>
<form method="post" action="/admin/settings">
<label>Max clients per territory (state / zip bucket)</label>
<input type="number" name="max_clients_per_territory" min="1" max="50" value="{mc}" style="width:100%;max-width:8rem"/>
<label>Default monthly quota (new users)</label>
<input type="number" name="default_monthly_quota" min="1" max="200" value="{dq}" style="width:100%;max-width:8rem"/>
<label><input type="checkbox" name="allow_shared_leads_default" value="1"{" checked" if sh else ""}/> Allow same lead to multiple clients this cycle</label>
<button type="submit">Save settings</button>
</form>
</div>
<div class="card">
<h2 style="margin-top:0;font-size:1rem">Run allocation</h2>
<form method="post" action="/admin/allocate">
<label>Cycle (YYYYMM)</label>
<input name="cycle_yyyymm" value="{_esc(cycle)}" pattern="\\d{{6}}" required style="width:8rem"/>
<label><input type="checkbox" name="replace" value="1" checked/> Replace existing assignments for this cycle</label>
<button type="submit">Assign leads</button>
</form>
<p class="meta">Uses highest score first, balances fill vs quota, respects territories. Prepare <code>sync</code> / DB first.</p>
</div>
<h2>Active clients</h2>
<table>
<thead><tr><th>Email</th><th>Adm</th><th>Quota</th><th>T type</th><th>T spec</th><th>S1 only</th><th>Shared</th><th>Edit</th></tr></thead>
<tbody>{tbl}</tbody>
</table>
{_render_lead_suppress_admin_card(suppress_rows or [])}
{_render_advisor_feedback_admin_card(advisor_feedback_rows or [])}
</body></html>"""


_HQ_CUT_MARKERS = (
    " and our telephone",
    " and our telephone number is",
    " telephone number, including area code",
    " telephone number",
    "(registrant's telephone",
    "(registrant's telephone number",
    "(registrant’s telephone",  # unicode apostrophe
    "; our telephone",
    " and our facsimile",
    " including area code)",
)

_DATE_ONLY = re.compile(
    r"^[A-Z][a-z]{2,8} \d{1,2}, \d{4}\.?$|^\d{4}-\d{2}-\d{2}$"
)


def _hq_short_display(raw: str, *, max_len: int = 56) -> str:
    """Strip common SEC tail junk; drop bogus date-only lines; truncate for table cells."""
    s = format_headquarters_for_ui(raw)
    if not s:
        return "—"
    low = s.lower()
    if low.startswith("(registrant") and "telephone" in low and len(s) < 120:
        return "—"
    for m in _HQ_CUT_MARKERS:
        i = low.find(m)
        if i >= 10:
            s = s[:i].strip().rstrip(",.;")
            low = s.lower()
            break
    if _DATE_ONLY.match(s):
        return "—"
    if len(s) < 4:
        return "—"
    if len(s) <= max_len:
        return s
    chunk = s[: max_len + 1]
    if " " in chunk:
        chunk = chunk[:max_len].rsplit(" ", 1)[0]
    else:
        chunk = chunk[:max_len]
    return chunk + "…"


def _pipeline_hq_cell(raw: str) -> str:
    full = (raw or "").strip()
    short = _hq_short_display(full)
    if full:
        tattr = html_module.escape(full, quote=True)
        title = f' title="{tattr}"'
    else:
        title = ""
    return f'<td class="hq"{title}>{_esc(short)}</td>'


def _pipeline_month_label(months: int) -> str:
    if months <= 0:
        return "all filing dates"
    if months == 1:
        return "last 1 month"
    return f"last {months} months"


def _profile_row_float(r: Any, key: str) -> float | None:
    if key not in r.keys():
        return None
    v = r[key]
    if v is None:
        return None
    try:
        x = float(v)
        if math.isnan(x):
            return None
        return x
    except (TypeError, ValueError):
        return None


def _profile_row_int(r: Any, key: str, default: int = 0) -> int:
    """SQLite may return empty string or odd types for INTEGER columns."""
    if key not in r.keys():
        return default
    v = r[key]
    if v is None or v == "":
        return default
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _pipeline_headline_year_display(r: Any) -> tuple[int | None, str]:
    """Return (year or None, label for tooltips / UI). Never raises."""
    if "headline_year" not in r.keys():
        return None, "—"
    v = r["headline_year"]
    if v is None or v == "":
        return None, "—"
    try:
        y = int(v)
        if y <= 0:
            return None, "—"
        return y, str(y)
    except (TypeError, ValueError):
        return None, "—"


def pipeline_hq_city_state(r: Any) -> str:
    """City + state/region only for pipeline Location — never street or issuer-name junk."""
    raw_row = (
        (r["issuer_headquarters"] or "").strip()
        if "issuer_headquarters" in r.keys()
        else ""
    )
    if raw_row:
        live = issuer_hq_city_state_for_ui(format_headquarters_for_ui(raw_row))
        if live:
            return live
    if "issuer_hq_city_state" in r.keys():
        v = (r["issuer_hq_city_state"] or "").strip()
        if v and issuer_hq_city_state_materialized_ok(v):
            return v
    return ""


def _pipeline_row_filing_date_latest_str(r: Any) -> str:
    try:
        return str(r["filing_date_latest"] or "")
    except (KeyError, TypeError, IndexError):
        return ""


_REG_FORM_PIPELINE_HEADER = re.compile(
    r"^(S-1|F-1|S-1/A|F-1/A|S-11|S-11/A)",
    re.I,
)


def _pipeline_pick_company_filing_for_sec_link(
    rows: list[Any],
) -> dict[str, str] | None:
    """
    One representative SEC filing for the company roster header (primary document + optional index).
    Prefers S-1/F-1-family ``form_type_latest`` (or ``has_s1_comp``), then newest ``filing_date_latest``.
    """
    candidates: list[tuple[int, str, dict[str, str]]] = []
    for r in rows:
        try:
            url = (r["primary_doc_url"] or "").strip()
        except (KeyError, TypeError, IndexError):
            continue
        if not url or not url.lower().startswith("http"):
            continue
        try:
            ft = (r["form_type_latest"] or "").strip()
        except (KeyError, TypeError, IndexError):
            ft = ""
        try:
            idx = (r["index_url"] or "").strip()
        except (KeyError, TypeError, IndexError):
            idx = ""
        try:
            acc = (r["accession_latest"] or "").strip()
        except (KeyError, TypeError, IndexError):
            acc = ""
        fd = _pipeline_row_filing_date_latest_str(r)
        if _REG_FORM_PIPELINE_HEADER.match(ft):
            pri = 0
        elif _profile_row_int(r, "has_s1_comp"):
            pri = 1
        else:
            pri = 2
        meta = {
            "primary_doc_url": url,
            "index_url": idx,
            "form_type": ft,
            "accession": acc,
            "filing_date": fd,
            "has_s1_comp": "1" if _profile_row_int(r, "has_s1_comp") else "",
        }
        candidates.append((pri, fd, meta))
    if not candidates:
        return None
    best_pri = min(t[0] for t in candidates)
    tier = [t for t in candidates if t[0] == best_pri]
    _pri, _fd, meta = max(tier, key=lambda t: t[1])
    return meta


def _pipeline_company_sec_links_html(
    rows: list[Any], *, cik_fallback: str = ""
) -> str:
    """Clickable SEC primary document (and index when distinct) for the pipeline company page."""
    meta = _pipeline_pick_company_filing_for_sec_link(rows)
    if not meta:
        ck = (cik_fallback or "").strip()
        if ck.isdigit():
            try:
                cik_n = int(ck, 10)
            except ValueError:
                cik_n = 0
            if cik_n > 0:
                browse = (
                    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany"
                    f"&CIK={cik_n}&owner=exclude&count=40"
                )
                return (
                    f'<p class="pl-co-sec dim" style="margin:0.35rem 0 0 0;font-size:0.88rem">'
                    f'<span class="dim">SEC</span> · '
                    f'<a href="{_attr(browse)}" target="_blank" rel="noopener noreferrer">'
                    "Browse filings on SEC.gov</a></p>"
                )
        return ""
    doc_u = meta["primary_doc_url"]
    idx_u = (meta.get("index_url") or "").strip()
    ft = (meta.get("form_type") or "").strip()
    acc = (meta.get("accession") or "").strip()
    fd = (meta.get("filing_date") or "").strip()
    ft_u = ft.upper()
    if _REG_FORM_PIPELINE_HEADER.match(ft):
        if ft_u.startswith("F-1"):
            doc_label = "View F-1"
        else:
            doc_label = "View S-1"
    elif meta.get("has_s1_comp") == "1":
        doc_label = "View S-1 filing"
    elif ft:
        doc_label = f"View {ft}"
    else:
        doc_label = "View SEC filing"
    tip_parts = [x for x in (ft, acc, fd) if x]
    tip = html_module.escape(" · ".join(tip_parts), quote=True)
    hdoc = _attr(doc_u)
    out = (
        f'<p class="pl-co-sec dim" style="margin:0.35rem 0 0 0;font-size:0.88rem">'
        f'<span class="dim">SEC</span> · '
        f'<a href="{hdoc}" target="_blank" rel="noopener noreferrer" title="{tip}">'
        f"{_esc(doc_label)}</a>"
    )
    if (
        idx_u
        and idx_u.lower().startswith("http")
        and idx_u.rstrip("/") != doc_u.rstrip("/")
    ):
        out += (
            f' · <a href="{_attr(idx_u)}" target="_blank" rel="noopener noreferrer" '
            f'title="{tip}">Filing index</a>'
        )
    out += "</p>"
    return out


def _pl_bundle_numeric_data_attr(v: Any) -> str:
    """Format a scalar for data-pl-cash / data-pl-stock; never raises."""
    if v is None:
        return ""
    try:
        x = float(v)
        if math.isnan(x) or math.isinf(x):
            return ""
        return f"{x:.6f}"
    except (TypeError, ValueError):
        return ""


def _pipeline_row_exec_verified_bundle(r: Any) -> str:
    """
    Pipeline index grouping: ``premium`` URL = NEO / summary-comp rows (``lead_tier`` premium
    or standard). ``economy`` = visibility-only (no SCT row). See ``serve.filter_rows_sales_bundle``.
    """
    lt = ""
    try:
        lt = str(r["lead_tier"] or "").strip().lower()
    except (KeyError, TypeError, IndexError):
        pass
    return "premium" if lt in ("premium", "standard") else "economy"


def _filter_members_for_sales_bundle(members: list[Any], sales_bundle: str) -> list[Any]:
    sb = str(sales_bundle or "").strip().lower().replace("-", "_")
    want = "economy" if sb == "economy" else "premium"
    return [r for r in members if _pipeline_row_exec_verified_bundle(r) == want]


def pipeline_cash_excl_equity_from_row(r: Any) -> float | None:
    """
    Cash + non-equity-award bundle for pipeline/CSV: total_headline − stock_grants_headline
    when both exist; else sum of salary/bonus/other headline cells if any exist.
    """
    tot = _profile_row_float(r, "total_headline")
    stk = _profile_row_float(r, "stock_grants_headline")
    if tot is not None and stk is not None:
        return max(0.0, tot - stk)
    s = 0.0
    n = 0
    for k in ("salary_headline", "bonus_headline", "other_comp_headline"):
        v = _profile_row_float(r, k)
        if v is not None:
            s += v
            n += 1
    if n > 0:
        return s
    if stk is not None:
        return 0.0
    return None


def _pipeline_bundle_summary_block_html(
    rows: list[Any], *, company_name: str, cik_display: str
) -> str:
    if not rows:
        return ""
    first = rows[0]
    loc = pipeline_hq_city_state(first) or "—"
    st_l = (
        (first["issuer_listing_stage"] if "issuer_listing_stage" in first.keys() else "")
        or ""
    ).strip().lower()
    if st_l == "pre_ipo":
        listing = "Pre-IPO"
    elif st_l == "public":
        listing = "Listed"
    else:
        listing = "—"
    dates = [
        str(r["filing_date_latest"] or "").strip()
        for r in rows
        if (r["filing_date_latest"] or "").strip()
    ]
    newest = max(dates) if dates else "—"
    co = (company_name or "").strip() or "—"
    cash_sum = 0.0
    cash_n = 0
    stk_sum = 0.0
    stk_n = 0
    for r in rows:
        cv = pipeline_cash_excl_equity_from_row(r)
        if cv is not None:
            cash_sum += cv
            cash_n += 1
        rk = r.keys()
        if "stock_grants_headline" in rk and r["stock_grants_headline"] is not None:
            try:
                stk_sum += float(r["stock_grants_headline"])
                stk_n += 1
            except (TypeError, ValueError):
                pass
    cash_s = _money_cell(cash_sum if cash_n else None)
    stk_s = _money_cell(stk_sum if stk_n else None)
    return (
        '<section class="pl-bundle-card" aria-label="Company bundle summary"><dl>'
        f"<dt>Issuer</dt><dd>{_esc(co)}</dd>"
        f"<dt>CIK</dt><dd class='mono'>{_esc(cik_display)}</dd>"
        f"<dt>Listing</dt><dd>{_esc(listing)}</dd>"
        f"<dt>HQ (registrant)</dt><dd>{_esc(loc)}</dd>"
        f"<dt>Executives in bundle</dt><dd>{len(rows)}</dd>"
        f"<dt>Newest filing (row)</dt><dd class='num'>{_esc(newest)}</dd>"
        f"<dt>Σ Cash &amp; bonus</dt><dd class='num'>{cash_s}</dd>"
        f"<dt>Σ Stock + options</dt><dd class='num'>{stk_s}</dd>"
        "</dl></section>"
    )


def _pipeline_person_row_html(
    r: Any,
    *,
    blur_class: str,
    hide_company_column: bool = False,
) -> str:
    """One `<tr>` for the pipeline person grid."""
    cross_badge = (
        "<span class=\"cross\">Multi-CIK</span>"
        if _profile_row_int(r, "cross_company_hint")
        else ""
    )
    try:
        cik_s = str(r["cik"] or "").strip()
    except (KeyError, TypeError, IndexError):
        cik_s = ""
    try:
        pn_raw = r["person_norm"]
    except (KeyError, TypeError, IndexError):
        pn_raw = ""
    try:
        dn_raw = r["display_name"]
    except (KeyError, TypeError, IndexError):
        dn_raw = ""
    # Same normalization as /lead and _profile_lead_url so drill-in always matches _find_profile.
    name_for_lead = _norm_person_name(
        (pn_raw or "").strip() or (dn_raw or "").strip()
    )
    profile_q = urlencode({"cik": cik_s, "name": name_for_lead})
    profile_href = html_module.escape(f"/lead?{profile_q}", quote=True)
    try:
        title_r = (r["title"] or "").strip() or "—"
    except (KeyError, TypeError, IndexError):
        title_r = "—"
    role_badge = advisor_title_badge(title_r)
    full_title_attr = html_module.escape(title_r, quote=True)
    fy_n, fy_label = _pipeline_headline_year_display(r)
    loc_tt = html_module.escape(
        "City and state from the registrant principal office in the filing (not the person’s home). "
        "No street or ZIP in this column.",
        quote=True,
    )
    is_bo = _profile_row_int(r, "has_beneficial_owner_stake")
    cash_tt = html_module.escape(
        (
            "No NEO summary-comp row: cash column not from SCT for this profile."
            if is_bo
            else (
                f"Cash and bonus (plus other non-equity SCT cash/benefit lines): total minus stock + option "
                f"award columns when both exist; else salary + bonus + other for FY {fy_label}. "
                "Full SCT table on profile."
                if fy_n is not None
                else "Cash-side SCT bundle for headline FY; see profile for detail."
            )
        ),
        quote=True,
    )
    stock_tt = html_module.escape(
        (
            "Pre-IPO beneficial ownership: shares × net $/share from the S-1 when underwriting discounts/commissions "
            "are parsed (otherwise public price). Not SCT grant-date value. Compare to SCT equity for executives."
            if is_bo
            else (
                f"Sum of stock awards + option awards for FY {fy_label} (grant-date fair value, SCT). "
                "Illiquid pre-exit; vesting per plan."
                if fy_n is not None
                else "Stock + option awards for headline FY (grant-date value)."
            )
        ),
        quote=True,
    )
    row_keys = r.keys()
    cash_v = pipeline_cash_excl_equity_from_row(r)
    stk_v = r["stock_grants_headline"] if "stock_grants_headline" in row_keys else None
    loc_s = pipeline_hq_city_state(r)
    st_l = (
        (r["issuer_listing_stage"] if "issuer_listing_stage" in row_keys else "") or ""
    ).strip().lower()
    if st_l == "pre_ipo":
        listing_cell = "Pre-IPO"
    elif st_l == "public":
        listing_cell = "Listed"
    else:
        listing_cell = "—"
    listing_tt = html_module.escape(
        "From your DB: Pre-IPO = S-1/F-1 on file, no 10-K/10-Q yet; Listed = at least one periodic report.",
        quote=True,
    )
    row_open_hint = html_module.escape(
        "Opens full profile page (/lead). Click the row or ›. Enter when focused.",
        quote=True,
    )
    try:
        co_nm = r["company_name"]
    except (KeyError, TypeError, IndexError):
        co_nm = ""
    co_cell = "" if hide_company_column else f"<td>{_esc(co_nm)}</td>"
    lk_list = st_l if st_l in ("pre_ipo", "public", "unknown") else "unknown"
    cash_attr = f"{float(cash_v):.6f}" if cash_v is not None else ""
    stk_attr = ""
    if stk_v is not None:
        try:
            stk_attr = f"{float(stk_v):.6f}"
        except (TypeError, ValueError):
            stk_attr = ""
    return (
        "<tr class=\"pl-row\" tabindex=\"0\" role=\"link\" "
        f'data-profile-href="{profile_href}" title="{row_open_hint}" '
        f'data-pl-listing="{lk_list}" data-pl-cash="{cash_attr}" data-pl-stock="{stk_attr}">'
        + f"<td>{cross_badge}<strong>{_esc(dn_raw or '—')}</strong></td>"
        + f"<td class='pl-title-cell' title=\"{full_title_attr}\">"
        + f"<strong>{_esc(role_badge)}</strong>"
        + "</td>"
        + co_cell
        + f'<td class="dim" title="{listing_tt}">{_esc(listing_cell)}</td>'
        + f'<td class="pl-loc" title="{loc_tt}">{_esc(loc_s or "—")}</td>'
        + f"<td class='num'>{_esc(_pipeline_row_filing_date_latest_str(r))}</td>"
        + f'<td class="num{blur_class}" title="{cash_tt}">{_money_cell(cash_v)}</td>'
        + f'<td class="num{blur_class}" title="{stock_tt}">{_money_cell(stk_v)}</td>'
        + f"<td class='dim'>{_esc(_pipeline_flags(r))}</td>"
        + f'<td class="pl-row-cue"><a class="pl-row-profile" href="{profile_href}" '
        + 'aria-label="Open full profile">›</a></td>'
        + "</tr>"
    )


def _group_pipeline_rows_by_cik(rows: list[Any]) -> list[dict[str, Any]]:
    """Preserve first-seen CIK order, then sort bundles by newest filing among members."""
    buckets: dict[str, list[Any]] = {}
    order: list[str] = []
    for r in rows:
        ck = str(r["cik"] or "").strip()
        if not ck:
            continue
        if ck not in buckets:
            order.append(ck)
            buckets[ck] = []
        buckets[ck].append(r)
    out: list[dict[str, Any]] = []
    for ck in order:
        mem = buckets[ck]
        mem.sort(key=_pipeline_row_filing_date_latest_str, reverse=True)
        first = mem[0]
        cash_sum = 0.0
        cash_n = 0
        stk_sum = 0.0
        stk_n = 0
        for r in mem:
            cv = pipeline_cash_excl_equity_from_row(r)
            if cv is not None:
                cash_sum += cv
                cash_n += 1
            if "stock_grants_headline" in r.keys() and r["stock_grants_headline"] is not None:
                try:
                    stk_sum += float(r["stock_grants_headline"])
                    stk_n += 1
                except (TypeError, ValueError):
                    pass
        filed_latest = max((_pipeline_row_filing_date_latest_str(x) for x in mem), default="")
        st_l = (
            (first["issuer_listing_stage"] if "issuer_listing_stage" in first.keys() else "")
            or ""
        ).strip().lower()
        if st_l == "pre_ipo":
            listing_cell = "Pre-IPO"
        elif st_l == "public":
            listing_cell = "Listed"
        else:
            listing_cell = "—"
        out.append(
            {
                "cik": ck,
                "company_name": (first["company_name"] or "").strip() or "—",
                "members": mem,
                "people_count": len(mem),
                "listing_cell": listing_cell,
                "location": pipeline_hq_city_state(first) or "—",
                "filing_latest": filed_latest,
                "cash_sum": cash_sum if cash_n else None,
                "stock_sum": stk_sum if stk_n else None,
            }
        )
    out.sort(key=lambda b: b["filing_latest"], reverse=True)
    return out


def _pipeline_bundle_for_member_subset(
    parent: dict[str, Any], members: list[Any], *, sales_bundle: str
) -> dict[str, Any] | None:
    """One sellable SKU (Exec or Other) under a CIK; issuer-level fields copied from parent."""
    if not members:
        return None
    mem = list(members)
    mem.sort(key=_pipeline_row_filing_date_latest_str, reverse=True)
    cash_sum = 0.0
    cash_n = 0
    stk_sum = 0.0
    stk_n = 0
    for r in mem:
        cv = pipeline_cash_excl_equity_from_row(r)
        if cv is not None:
            cash_sum += cv
            cash_n += 1
        if "stock_grants_headline" in r.keys() and r["stock_grants_headline"] is not None:
            try:
                stk_sum += float(r["stock_grants_headline"])
                stk_n += 1
            except (TypeError, ValueError):
                pass
    filed_latest = max((_pipeline_row_filing_date_latest_str(x) for x in mem), default="")
    return {
        "cik": parent["cik"],
        "company_name": parent["company_name"],
        "listing_cell": parent["listing_cell"],
        "location": parent["location"],
        "members": mem,
        "people_count": len(mem),
        "filing_latest": filed_latest,
        "cash_sum": cash_sum if cash_n else None,
        "stock_sum": stk_sum if stk_n else None,
        "sales_bundle": sales_bundle,
    }


def _expand_pipeline_bundles_to_exec_other_skus(
    bundles: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Turn one row per CIK into up to two pipeline bundles: NEO/SCT (``sales_bundle=premium``)
    and visibility-only (``economy``). Preserves company order; within each company, premium then economy.
    """
    out: list[dict[str, Any]] = []
    for parent in bundles:
        mem_all = parent["members"]
        for sb in ("premium", "economy"):
            sub = _filter_members_for_sales_bundle(mem_all, sb)
            row = _pipeline_bundle_for_member_subset(parent, sub, sales_bundle=sb)
            if row is not None:
                out.append(row)
    return out


def _filter_pipeline_bundles_by_listing_stage(
    bundles: list[dict[str, Any]], list_cur: str
) -> list[dict[str, Any]]:
    """Keep bundles whose issuer listing (see ``listing_cell``) matches the tab."""
    lc = normalize_listing_stage_query(list_cur)
    if lc == "all":
        return bundles
    out: list[dict[str, Any]] = []
    for b in bundles:
        cell = (b.get("listing_cell") or "").strip()
        if lc == "pre_ipo" and cell == "Pre-IPO":
            out.append(b)
        elif lc == "public" and cell == "Listed":
            out.append(b)
        elif lc == "unknown" and cell == "—":
            out.append(b)
    return out


def pipeline_review_rows_after_company_listing_filter(
    rows: list[Any], *, listing_stage: str
) -> list[Any]:
    """
    Same company-level listing logic as the pipeline index: group by CIK, drop whole
    issuers that do not match the listing tab, flatten remaining profiles (CSV / counts).
    """
    bundles = _group_pipeline_rows_by_cik(rows)
    bundles = _filter_pipeline_bundles_by_listing_stage(bundles, listing_stage)
    flat: list[Any] = []
    for b in bundles:
        flat.extend(b["members"])
    return flat


def _pipeline_company_bundle_row_html(
    b: dict[str, Any],
    *,
    bundle_href: str,
    blur_class: str,
    show_bundle_suffix: bool = True,
) -> str:
    """One `<tr>` for a sellable company bundle; click opens the gated company roster URL."""
    bh = html_module.escape(bundle_href, quote=True)
    sb = (b.get("sales_bundle") or "").strip().lower()
    if sb == "premium":
        open_hint = html_module.escape(
            "Open the executive-network roster for this company.",
            quote=True,
        )
    elif sb == "economy":
        open_hint = html_module.escape(
            "Open roster for this company.",
            quote=True,
        )
    else:
        open_hint = html_module.escape(
            "Open this company's pipeline roster.",
            quote=True,
        )
    listing_tt = html_module.escape(
        "From your DB: Pre-IPO = S-1/F-1 on file, no 10-K/10-Q yet; Listed = at least one periodic report.",
        quote=True,
    )
    sum_cash_tt = html_module.escape(
        "Sum of per-person cash and bonus column (non-equity SCT bundle) across people in this bundle.",
        quote=True,
    )
    sum_stk_tt = html_module.escape(
        "Sum of per-person stock + options column (headline FY) across people in this bundle.",
        quote=True,
    )
    lc = (b.get("listing_cell") or "").strip()
    if lc == "Pre-IPO":
        lk = "pre_ipo"
    elif lc == "Listed":
        lk = "public"
    else:
        lk = "unknown"
    try:
        pc = int(b.get("people_count", 0) or 0)
    except (TypeError, ValueError):
        pc = 0
    cash_attr = _pl_bundle_numeric_data_attr(b.get("cash_sum"))
    stk_attr = _pl_bundle_numeric_data_attr(b.get("stock_sum"))
    sku = (b.get("sales_bundle") or "").strip().lower()
    if show_bundle_suffix:
        if sku == "premium":
            sku_bit = f" <span class='sub pl-bundle-sku'>· {_esc(sales_bundle_tab_label('premium'))}</span>"
        elif sku == "economy":
            sku_bit = " <span class='sub pl-bundle-sku'>· Standard</span>"
        else:
            sku_bit = ""
    else:
        sku_bit = ""
    return (
        "<tr class=\"pl-row pl-row-bundle\" tabindex=\"0\" role=\"link\" "
        f'data-bundle-href="{bh}" title="{open_hint}" data-pl-listing="{lk}" '
        f'data-pl-people="{pc}" data-pl-cash="{cash_attr}" '
        f'data-pl-stock="{stk_attr}">'
        f"<td><strong>{_esc(b['company_name'])}</strong>{sku_bit}</td>"
        f"<td class='num'>{pc}</td>"
        f'<td class="dim" title="{listing_tt}">{_esc(b["listing_cell"])}</td>'
        f'<td class="pl-loc">{_esc(b["location"])}</td>'
        f"<td class='num'>{_esc(b['filing_latest'])}</td>"
        f'<td class="num{blur_class}" title="{sum_cash_tt}">{_money_cell(b.get("cash_sum"))}</td>'
        f'<td class="num{blur_class}" title="{sum_stk_tt}">{_money_cell(b.get("stock_sum"))}</td>'
        "</tr>"
    )


def _pipeline_index_tab_href(
    pipeline_path: str,
    *,
    tier: str,
    q: str,
    months: int,
    include_non_s1: bool,
) -> str:
    """GET URL for pipeline index with a given tier tab (``exec`` | ``standard``)."""
    parts: dict[str, str] = {"tier": tier, "months": str(months)}
    qs = (q or "").strip()
    if qs:
        parts["q"] = qs
    if include_non_s1:
        parts["include_non_s1"] = "1"
    return f"{pipeline_path.rstrip('/')}?{urlencode(parts)}"


def _pipeline_bundle_numeric(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _pipeline_filing_latest_sort_int(s: Any) -> int:
    """YYYYMMDD as int for tie-break (newer first when combined with negation in sort key)."""
    t = str(s or "").strip()[:10]
    if len(t) == 10 and t[4] == "-" and t[7] == "-":
        try:
            return int(t[0:4]) * 10_000 + int(t[5:7]) * 100 + int(t[8:10])
        except ValueError:
            pass
    return 0


def _pipeline_bundle_sort_key_amounts(
    b: dict[str, Any],
) -> tuple[float, float, float]:
    """Descending Σ stock+options, then Σ cash+bonus, then newest filing date."""
    stk = _pipeline_bundle_numeric(b.get("stock_sum"))
    cash = _pipeline_bundle_numeric(b.get("cash_sum"))
    fd = _pipeline_filing_latest_sort_int(b.get("filing_latest"))
    return (-stk, -cash, -float(fd))


def _pipeline_single_tier_table_html(
    sku_bundles: list[dict[str, Any]],
    *,
    pipe_qs: dict[str, str],
    base_co: str,
    blur_class: str,
    thead_html: str,
    empty_message: str,
    show_bundle_suffix: bool,
) -> str:
    """One pipeline table for the active tier tab (no second tier on the page)."""
    rows = sorted(sku_bundles, key=_pipeline_bundle_sort_key_amounts)
    trs: list[str] = []
    for b in rows:
        qs_co = {
            **pipe_qs,
            "cik": b["cik"],
            "sales_bundle": (b.get("sales_bundle") or "economy").strip().lower(),
        }
        enc = urlencode({k: v for k, v in qs_co.items() if v})
        bundle_href = f"{base_co}?{enc}"
        trs.append(
            _pipeline_company_bundle_row_html(
                b,
                bundle_href=bundle_href,
                blur_class=blur_class,
                show_bundle_suffix=show_bundle_suffix,
            )
        )
    body = (
        "".join(trs)
        if trs
        else f"<tr><td colspan='7' class='dim empty'>{_esc(empty_message)}</td></tr>"
    )
    return (
        '<div class="pl-table-wrap">'
        '<table class="pl-grid pl-pipeline-tier">'
        f"{thead_html}"
        f"<tbody>{body}</tbody>"
        "</table></div>"
    )


PIPELINE_PAGE_CSS = """
:root {
  --bg: #0b0b0c;
  --panel: #121214;
  --line: #2a2a2e;
  --text: #e8e8ea;
  --muted: #8c8c93;
  --faint: #5c5c64;
  --amber: #e2a012;
  --amber-dim: #a6740a;
  --green: #3fb950;
  --mono: ui-monospace, "Cascadia Code", "SF Mono", Consolas, monospace;
  --sans: "Segoe UI", system-ui, -apple-system, sans-serif;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  font-family: var(--sans);
  font-size: 13px;
  background: var(--bg);
  color: var(--text);
  line-height: 1.45;
  min-height: 100vh;
}
.pl-shell { max-width: 1440px; margin: 0 auto; padding: 0 1rem 2rem; }
.pl-head {
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  justify-content: space-between;
  gap: 0.75rem 1.5rem;
  padding: 1rem 0 0.75rem;
  border-bottom: 1px solid var(--line);
}
.pl-brand { display: flex; align-items: baseline; gap: 0.65rem; flex-wrap: wrap; }
/* Company drill-in: SEC links — muted accent, not default browser blue */
.pl-co-sec a {
  color: #b8a882;
  text-decoration: none;
  font-weight: 500;
  border-bottom: 1px solid rgba(184, 168, 130, 0.42);
}
.pl-co-sec a:hover {
  color: var(--amber);
  border-bottom-color: rgba(226, 160, 18, 0.55);
}
.pl-co-sec a:visited {
  color: #a89870;
  border-bottom-color: rgba(168, 152, 112, 0.35);
}
.pl-co-sec a:visited:hover {
  color: var(--amber);
}
.pl-logo {
  font-weight: 650;
  letter-spacing: 0.06em;
  font-size: 0.78rem;
  color: var(--amber);
}
.pl-title { font-size: 1.15rem; font-weight: 600; letter-spacing: -0.02em; }
.pl-sub { font-size: 0.75rem; color: var(--muted); }
.pl-actions { display: flex; flex-wrap: wrap; gap: 0.5rem 1rem; align-items: center; }
.pl-actions a {
  color: var(--amber);
  text-decoration: none;
  font-size: 0.8rem;
}
.pl-actions a:hover { text-decoration: underline; color: #f0b429; }
.pl-banner {
  font-size: 0.78rem;
  color: var(--muted);
  padding: 0.65rem 0 0;
  max-width: 52rem;
  line-height: 1.5;
}
.pl-banner strong { color: var(--text); font-weight: 600; }
.pl-banner code { font-family: var(--mono); font-size: 0.72rem; background: var(--panel); padding: 0.1rem 0.35rem; border-radius: 2px; }
.pl-toolbar {
  display: flex;
  flex-wrap: wrap;
  align-items: flex-end;
  gap: 1rem 1.5rem;
  padding: 1rem 0;
  border-bottom: 1px solid var(--line);
}
.pl-filters {
  display: flex;
  flex-wrap: wrap;
  align-items: flex-end;
  gap: 1rem 1.25rem;
}
.pl-field { display: flex; flex-direction: column; gap: 0.2rem; }
.pl-field label {
  font-size: 0.65rem;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--muted);
}
.pl-field input[type="text"],
.pl-field input[type="number"] {
  background: var(--panel);
  border: 1px solid var(--line);
  color: var(--text);
  border-radius: 2px;
  padding: 0.35rem 0.5rem;
  font-family: var(--sans);
  font-size: 0.8125rem;
  min-width: 0;
}
.pl-field input[type="text"] { width: min(22rem, 88vw); }
.pl-field input[type="number"] { width: 4.25rem; font-family: var(--mono); }
.pl-check {
  display: flex;
  align-items: center;
  gap: 0.4rem;
  padding-bottom: 0.35rem;
  font-size: 0.8rem;
  color: var(--muted);
}
.pl-check input { accent-color: var(--amber); }
.pl-hint { font-size: 0.68rem; color: var(--faint); margin-top: 0.15rem; }
.pl-rebuild {
  margin-left: auto;
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.5rem 0.75rem;
}
.pl-rebuild button {
  font-family: var(--sans);
  font-size: 0.75rem;
  background: transparent;
  color: var(--muted);
  border: 1px solid var(--line);
  border-radius: 2px;
  padding: 0.35rem 0.65rem;
  cursor: pointer;
}
.pl-rebuild button:hover { border-color: var(--amber-dim); color: var(--text); }
.pl-rebuild span { font-size: 0.68rem; color: var(--faint); max-width: 14rem; line-height: 1.35; }
.pl-data-help {
  margin: 0.75rem 0 0;
  max-width: 52rem;
  font-size: 0.72rem;
  color: var(--muted);
  line-height: 1.45;
}
.pl-data-help summary { cursor: pointer; color: var(--amber); user-select: none; }
.pl-data-help dl {
  display: grid;
  grid-template-columns: minmax(5rem, 9rem) 1fr;
  gap: 0.35rem 1rem;
  margin: 0.5rem 0 0;
}
.pl-data-help dt { color: var(--faint); margin: 0; }
.pl-data-help dd { margin: 0; }
.msg-ok { color: var(--green); font-size: 0.8rem; padding: 0.5rem 0 0; margin: 0; }
.pl-table-wrap {
  overflow-x: auto;
  margin-top: 0.5rem;
  border: 1px solid var(--line);
  border-radius: 2px;
  background: var(--panel);
}
table.pl-grid {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.78rem;
}
.pl-grid thead th {
  position: sticky;
  top: 0;
  z-index: 1;
  background: #161618;
  border-bottom: 1px solid var(--line);
  padding: 0.5rem 0.6rem;
  text-align: center;
  font-weight: 600;
  font-size: 0.65rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--muted);
  white-space: nowrap;
}
.pl-grid tbody tr:nth-child(even) { background: rgba(255,255,255,0.02); }
.pl-grid tbody tr:hover { background: rgba(226,160,18,0.06); }
.pl-grid td {
  border-bottom: 1px solid var(--line);
  padding: 0.45rem 0.6rem;
  vertical-align: top;
  text-align: center;
}
.pl-grid td.empty { padding: 2rem 1rem; text-align: center; color: var(--faint); }
/* Text-heavy columns: left-aligned for scanning; numbers stay centered */
.pl-grid thead th:first-child,
.pl-grid td:first-child,
.pl-grid thead th.pl-th-text,
.pl-grid td.pl-title-cell,
.pl-grid td.pl-loc {
  text-align: left;
}
.num {
  text-align: center;
  font-family: var(--mono);
  font-variant-numeric: tabular-nums;
}
.dim { color: var(--faint); }
.mono { font-family: var(--mono); font-size: 0.72rem; }
.sub { font-size: 0.68rem; color: var(--faint); margin-top: 0.15rem; font-family: var(--mono); }
.hq { color: #c4c4ca; max-width: 14rem; cursor: default; }
.cross {
  display: inline-block;
  background: rgba(226,160,18,0.12);
  color: var(--amber);
  font-size: 0.6rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  padding: 0.12rem 0.35rem;
  border-radius: 2px;
  margin-right: 0.35rem;
  vertical-align: middle;
}
.pl-row { cursor: pointer; }
.pl-row:hover { background: rgba(226,160,18,0.08) !important; }
.pl-row:focus { outline: 1px solid var(--amber); outline-offset: -2px; }
/* Bundle row is role=link — avoid OS “hyperlink” blue on company names */
tr.pl-row-bundle {
  color: var(--text);
}
tr.pl-row-bundle td strong {
  color: var(--text);
  font-weight: 600;
}
tr.pl-row-bundle:hover td:first-child strong {
  color: var(--amber);
}
tr.pl-row-bundle td.dim {
  color: var(--faint);
}
tr.pl-row-bundle td.pl-loc {
  color: #b0b0b8;
}
.pl-col-profile {
  width: 4.5rem;
  text-align: center;
  font-size: 0.62rem;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: var(--muted);
}
.pl-row-cue {
  padding: 0 !important;
  width: 2.25rem;
  text-align: center;
  vertical-align: middle;
}
.pl-row-profile {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 2.25rem;
  color: var(--amber);
  text-decoration: none;
  font-size: 1.15rem;
  font-weight: 300;
  line-height: 1;
}
.pl-row-profile:hover { color: #f0b429; background: rgba(226,160,18,0.1); }
.pl-title-cell { max-width: 14rem; color: #c4c4ca; }
.pl-loc { max-width: 11rem; color: #b0b0b8; font-size: 0.92em; line-height: 1.35; }
.pl-comp-preview {
  max-width: 18rem;
  font-family: var(--mono);
  font-size: 0.72rem;
  color: #b8b8be;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.pay-band-wrap { margin: 0.5rem 0 0; max-width: 52rem; }
.pay-band-hint { font-size: 0.72rem; color: var(--faint); margin: 0 0 0.35rem; line-height: 1.45; }
.pay-band-nav { display: flex; flex-wrap: wrap; gap: 0.35rem 0.65rem; align-items: center; font-size: 0.78rem; }
.pay-band-tab {
  color: var(--muted);
  text-decoration: none;
  padding: 0.15rem 0.4rem;
  border-radius: 2px;
  border: 1px solid transparent;
}
.pay-band-tab:hover { color: var(--text); border-color: var(--line); }
.pay-band-tab--active {
  color: var(--amber);
  border-color: rgba(226, 160, 18, 0.35);
  background: rgba(226, 160, 18, 0.06);
}
.filter-strip {
  margin: 0.5rem 0 0;
  max-width: 52rem;
  padding: 0.45rem 0.65rem;
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 4px;
}
.filter-strip--tier { margin-bottom: 0.5rem; }
.desk-tier-row {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.5rem 0.85rem;
  margin: 0 0 0.85rem 0;
  padding: 0.55rem 0.75rem;
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 6px;
  max-width: 52rem;
}
.desk-tier-row-label {
  font-size: 0.65rem;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  font-weight: 600;
  color: var(--muted);
}
.desk-tier-tabs {
  display: flex;
  flex-wrap: wrap;
  gap: 0.4rem;
  align-items: center;
}
a.desk-tier-tab {
  display: inline-block;
  padding: 0.35rem 0.85rem;
  border-radius: 5px;
  border: 1px solid var(--line);
  background: var(--bg);
  color: var(--amber);
  font-size: 0.8125rem;
  font-weight: 500;
  text-decoration: none;
}
a.desk-tier-tab:hover {
  border-color: rgba(226, 160, 18, 0.5);
  color: #f0b429;
}
a.desk-tier-tab--active {
  border-color: rgba(226, 160, 18, 0.45);
  background: rgba(226, 160, 18, 0.08);
  color: var(--text);
  font-weight: 600;
}
.desk-tier-row--stacked {
  flex-direction: column;
  align-items: stretch;
  gap: 0.45rem;
}
.desk-tier-row--after-scope { margin-top: 0.5rem; }
.desk-tier-row-head {
  display: flex;
  flex-direction: column;
  gap: 0.12rem;
}
.desk-tier-row-title {
  font-size: 0.875rem;
  font-weight: 600;
  color: var(--text);
}
.desk-tier-hint { font-size: 0.72rem; line-height: 1.4; }
.desk-tier-sep { user-select: none; padding: 0 0.1rem; }
.pl-banner-sub { padding-top: 0; margin-top: -0.15rem; margin-bottom: 0; }
.pl-blur-comp {
  filter: blur(5px);
  user-select: none;
}
.pl-package-nav {
  font-size: 0.82rem;
  margin: 0.65rem 0 0;
  color: var(--muted);
}
.pl-package-nav a {
  color: var(--amber);
  text-decoration: none;
}
.pl-package-nav a:hover { text-decoration: underline; color: #f0b429; }
.pl-package-section {
  margin-top: 1.25rem;
  scroll-margin-top: 1rem;
}
.pl-package-section:first-of-type { margin-top: 0.55rem; }
.pl-package-h2 {
  font-size: 0.95rem;
  font-weight: 600;
  margin: 0 0 0.15rem;
  letter-spacing: -0.02em;
}
.pl-package-sub {
  font-size: 0.72rem;
  margin: 0 0 0.35rem;
  max-width: 44rem;
  line-height: 1.4;
}
.pl-pipeline-tabs {
  display: flex;
  flex-wrap: wrap;
  gap: 0.4rem;
  align-items: center;
  margin: 0.85rem 0 0;
  padding-bottom: 0.55rem;
  border-bottom: 1px solid var(--line);
}
.pl-pipeline-tab {
  display: inline-block;
  padding: 0.42rem 0.9rem;
  border-radius: 4px;
  border: 1px solid var(--line);
  color: var(--muted);
  text-decoration: none;
  font-size: 0.82rem;
  font-weight: 500;
}
.pl-pipeline-tab:hover {
  border-color: var(--amber-dim);
  color: var(--text);
}
.pl-pipeline-tab--active {
  background: var(--panel);
  border-color: var(--amber-dim);
  color: var(--amber);
}
.pl-tab-count {
  font-weight: 400;
  font-size: 0.78rem;
}
.pl-tier-active-hint {
  font-size: 0.72rem;
  margin: 0.55rem 0 0;
  max-width: 48rem;
  line-height: 1.45;
}
.pl-bundle-card {
  margin-top: 1rem;
  padding: 0.85rem 1rem;
  border: 1px solid var(--line);
  border-radius: 2px;
  background: var(--panel);
  font-size: 0.78rem;
}
.pl-bundle-card dl {
  display: grid;
  grid-template-columns: max-content 1fr;
  gap: 0.35rem 1.25rem;
  margin: 0;
  align-items: baseline;
}
.pl-bundle-card dt {
  color: var(--faint);
  font-size: 0.62rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  margin: 0;
}
.pl-bundle-card dd {
  margin: 0;
  color: var(--text);
}
thead tr.pl-filter-row th {
  padding: 0.3rem 0.45rem;
  font-weight: 400;
  border-bottom: 1px solid var(--line);
  vertical-align: bottom;
  text-align: center;
}
thead tr.pl-filter-row th:first-child {
  text-align: left;
}
.pl-th-filter {
  width: 100%;
  max-width: 7.5rem;
  box-sizing: border-box;
  font: inherit;
  font-size: 0.72rem;
  padding: 0.28rem 0.4rem;
  border-radius: 3px;
  border: 1px solid var(--line);
  background: var(--bg);
  color: var(--text);
}
thead tr.pl-filter-row select.pl-th-filter { max-width: 6.25rem; }
tr.hidden { display: none !important; }
"""

def render_pipeline_review_page(
    *,
    rows: list[Any],
    search: str,
    include_non_s1: bool,
    months: int,
    tier_tab: str,
    msg: str,
    pipeline_path: str = "/admin/pipeline",
    blur_comp: bool = False,
) -> str:
    msg_html = f'<p class="msg-ok">{_esc(msg)}</p>' if msg else ""
    win_label = _pipeline_month_label(months)
    mo_opts: list[tuple[int, str]] = [
        (3, "3 months"),
        (6, "6 months"),
        (12, "12 months"),
        (24, "24 months"),
        (0, "All time"),
    ]
    months_select = "".join(
        f'<option value="{v}"{" selected" if months == v else ""}>{lbl}</option>'
        for v, lbl in mo_opts
    )
    blur_class = " pl-blur-comp" if blur_comp else ""
    tt = (tier_tab or "exec").strip().lower()
    if tt not in ("exec", "standard"):
        tt = "exec"
    pipe_qs: dict[str, str] = {
        "q": search,
        "months": str(months),
        "include_non_s1": "1" if include_non_s1 else "",
        "tier": tt,
    }

    bundles = _group_pipeline_rows_by_cik(rows)
    sku_bundles = _expand_pipeline_bundles_to_exec_other_skus(bundles)
    sku_premium = [
        b
        for b in sku_bundles
        if (b.get("sales_bundle") or "").strip().lower() == "premium"
    ]
    sku_economy = [
        b
        for b in sku_bundles
        if (b.get("sales_bundle") or "").strip().lower() == "economy"
    ]
    n_people = 0
    for b in sku_bundles:
        try:
            n_people += int(b.get("people_count", 0) or 0)
        except (TypeError, ValueError):
            pass
    base_co = pipeline_path.split("?")[0].rstrip("/") + "/company"
    n_co = len(bundles)
    n_prem = len(sku_premium)
    n_eco = len(sku_economy)
    summary_line = (
        f"{n_prem} executive · {n_eco} standard · {n_co} issuers · "
        f"{n_people} people · {_esc(win_label)}"
    )
    href_exec = _pipeline_index_tab_href(
        pipeline_path,
        tier="exec",
        q=search,
        months=months,
        include_non_s1=include_non_s1,
    )
    href_std = _pipeline_index_tab_href(
        pipeline_path,
        tier="standard",
        q=search,
        months=months,
        include_non_s1=include_non_s1,
    )
    tab_nav = (
        '<nav class="pl-pipeline-tabs" aria-label="Pipeline product">'
        f'<a href="{_attr(href_exec)}" class="pl-pipeline-tab'
        f'{" pl-pipeline-tab--active" if tt == "exec" else ""}">'
        f'Executive · verified <span class="pl-tab-count dim">({n_prem})</span></a>'
        f'<a href="{_attr(href_std)}" class="pl-pipeline-tab'
        f'{" pl-pipeline-tab--active" if tt == "standard" else ""}">'
        f'Standard <span class="pl-tab-count dim">({n_eco})</span></a>'
        "</nav>"
    )
    if tt == "exec":
        active_hint = (
            "Showing NEO / summary-comp (verified SCT) bundles only. Open Standard for visibility-only officers."
        )
        active_bundles = sku_premium
        empty_msg = "No executive bundles in this window."
    else:
        active_hint = (
            "Showing visibility-only bundles (no SCT row). Open Executive · verified for summary comp."
        )
        active_bundles = sku_economy
        empty_msg = "No standard bundles in this window."
    tab_title = "Executive" if tt == "exec" else "Standard"
    thead_html = """<thead><tr>
<th scope="col">Company</th>
<th scope="col" class="num">People</th>
<th scope="col" title="Pre-IPO vs listed from form types in your DB">Listing</th>
<th scope="col" class="pl-th-text" title="City and state from registrant principal office (no street)">Location</th>
<th scope="col" class="num">Filed</th>
<th scope="col" class="num" title="Sum of per-person cash and bonus in this bundle">Σ Cash and bonus</th>
<th scope="col" class="num" title="Sum of per-person stock + options in this bundle">Σ Stock + options</th>
</tr><tr class="pl-filter-row">
<th><input type="search" class="pl-th-filter" data-plf-col="co" placeholder="Contains…" aria-label="Filter company"/></th>
<th><input type="number" class="pl-th-filter" data-plf-col="minpeople" min="0" step="1" placeholder="Min" aria-label="Min people"/></th>
<th><select class="pl-th-filter" data-plf-col="listing" aria-label="Filter listing">
<option value="">All</option><option value="pre_ipo">Pre-IPO</option><option value="public">Listed</option><option value="unknown">Unknown</option>
</select></th>
<th><input type="search" class="pl-th-filter" data-plf-col="loc" placeholder="Contains…" aria-label="Filter location"/></th>
<th><input type="search" class="pl-th-filter" data-plf-col="filed" placeholder="Contains…" aria-label="Filter filed"/></th>
<th><input type="number" class="pl-th-filter" data-plf-col="mincash" min="0" step="1000" placeholder="Min $" aria-label="Min cash sum"/></th>
<th><input type="number" class="pl-th-filter" data-plf-col="minstk" min="0" step="1000" placeholder="Min $" aria-label="Min stock sum"/></th>
</tr></thead>"""

    tier_block = (
        f'<p class="pl-tier-active-hint dim">{_esc(active_hint)}</p>'
        + _pipeline_single_tier_table_html(
            active_bundles,
            pipe_qs=pipe_qs,
            base_co=base_co,
            blur_class=blur_class,
            thead_html=thead_html,
            empty_message=empty_msg,
            show_bundle_suffix=False,
        )
    )
    ins = " checked" if include_non_s1 else ""
    csv_qs: dict[str, str] = {
        "q": search,
        "months": str(months),
        "include_non_s1": "1" if include_non_s1 else "",
    }
    csv_href = f"{_esc(pipeline_path)}.csv?{urlencode({k: v for k, v in csv_qs.items() if v})}"
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Company pipeline — {_esc(tab_title)} · {_esc(advisor_ui_product_name())}</title>
<style>
{PIPELINE_PAGE_CSS}</style></head><body>
<div class="pl-shell">
<header class="pl-head">
  <div class="pl-brand">
    <span class="pl-logo">{_esc(advisor_ui_product_name())}</span>
    <span class="pl-title">Company pipeline</span>
    <span class="pl-sub dim">{summary_line}</span>
  </div>
  <div class="pl-actions">
    <a href="/admin">Sync &amp; settings</a>
    <a href="{csv_href}">Export CSV</a>
  </div>
</header>
{msg_html}
<details class="pl-data-help">
  <summary>What do these columns mean?</summary>
  <dl>
    <dt>People / Filed</dt><dd>Counts and latest filing date from materialized <code>lead_profile</code> rows.</dd>
    <dt>Σ Cash / Σ Stock</dt><dd>Sums per bundle row for the active tab’s materialized tier — a practical proxy for scale alongside listing and people count.</dd>
    <dt>Tabs</dt><dd><strong>Executive · verified</strong> lists issuers with at least one NEO / summary-comp row (<code>lead_tier</code> premium or standard). <strong>Standard</strong> lists visibility-only officers (no SCT in DB). Company drill-in uses <code>sales_bundle=premium|economy</code> with the same split. Executive roster access may be restricted by configuration.</dd>
    <dt>signal_hwm</dt><dd>In CSV: headline comp signal used for desk pay-bar; see <code>WEALTH_LEADS_LEAD_DESK_MIN_SIGNAL_USD</code>.</dd>
    <dt>Search</dt><dd>Full-text on <code>search_text</code> when available; otherwise LIKE. Rebuild profiles after sync to refresh the index.</dd>
  </dl>
</details>
{tab_nav}
<div class="pl-toolbar">
  <form id="pl-filters" class="pl-filters" method="get" action="{_esc(pipeline_path)}" autocomplete="off" data-pipeline-path="{_esc(pipeline_path)}">
    <input type="hidden" name="tier" value="{_esc(tt)}"/>
    <div class="pl-field">
      <label for="pl-months">Filing window</label>
      <select id="pl-months" name="months">{months_select}</select>
      <span class="pl-hint">Companies: highest Σ stock+options first, then Σ cash+bonus, then newest filing</span>
    </div>
    <div class="pl-field">
      <label for="pl-q">Filter</label>
      <input id="pl-q" type="text" name="q" value="{_esc(search)}" placeholder="Name or company…"/>
      <span class="pl-hint">Updates as you type (short pause)</span>
    </div>
    <label class="pl-check"><input type="checkbox" name="include_non_s1" value="1"{ins}/> Include exec comp from non–S-1 filings (e.g. 10-K)</label>
  </form>
  <form class="pl-rebuild" method="post" action="{_esc(pipeline_path)}/rebuild">
    <button type="submit">Rebuild profiles</button>
    <span>No SEC fetch—rebuilds materialized rows from current DB.</span>
  </form>
</div>
{tier_block}
</div>
<script>
(function () {{
  var form = document.getElementById("pl-filters");

  function goProfile(tr) {{
    if (!tr) return;
    var href = tr.getAttribute("data-bundle-href") || tr.getAttribute("data-profile-href");
    if (href) window.location.href = href;
  }}

  function plRowFromEventTarget(t) {{
    if (!t) return null;
    var el = t.nodeType === 3 ? t.parentElement : t;
    return el && typeof el.closest === "function" ? el.closest("tr.pl-row") : null;
  }}

  function parsePF(s) {{
    if (s == null || s === "") return null;
    var x = parseFloat(String(s).replace(/,/g, ""));
    return isNaN(x) ? null : x;
  }}
  function cellLowerPL(tr, i) {{
    var td = tr.cells[i];
    return td ? (td.innerText || "").toLowerCase().trim() : "";
  }}
  function wirePipelineTierTable(plTable) {{
    var plTbody = plTable && plTable.tBodies[0];
    var plFilterRow = plTable && plTable.querySelector("thead tr.pl-filter-row");
    function applyPlBundleFilters() {{
      if (!plTbody || !plFilterRow) return;
      var fCo = "", fLoc = "", fFiled = "", fList = "";
      var minP = null, minCash = null, minStk = null;
      plFilterRow.querySelectorAll("[data-plf-col]").forEach(function (el) {{
        var k = el.getAttribute("data-plf-col");
        var v = (el.value || "").trim();
        if (k === "co") fCo = v.toLowerCase();
        else if (k === "loc") fLoc = v.toLowerCase();
        else if (k === "filed") fFiled = v.toLowerCase();
        else if (k === "listing") fList = v;
        else if (k === "minpeople") minP = parsePF(v);
        else if (k === "mincash") minCash = parsePF(v);
        else if (k === "minstk") minStk = parsePF(v);
      }});
      plTbody.querySelectorAll("tr").forEach(function (tr) {{
        if (!tr.classList.contains("pl-row-bundle")) return;
        var ok = true;
        if (fCo && cellLowerPL(tr, 0).indexOf(fCo) < 0) ok = false;
        if (ok && minP != null) {{
          var np = parseInt(tr.getAttribute("data-pl-people") || "0", 10) || 0;
          if (np < minP) ok = false;
        }}
        if (ok && fList && (tr.getAttribute("data-pl-listing") || "") !== fList) ok = false;
        if (ok && fLoc && cellLowerPL(tr, 3).indexOf(fLoc) < 0) ok = false;
        if (ok && fFiled && cellLowerPL(tr, 4).indexOf(fFiled) < 0) ok = false;
        if (ok && minCash != null) {{
          var c = parsePF(tr.getAttribute("data-pl-cash"));
          if (c == null || c < minCash) ok = false;
        }}
        if (ok && minStk != null) {{
          var s = parsePF(tr.getAttribute("data-pl-stock"));
          if (s == null || s < minStk) ok = false;
        }}
        tr.classList.toggle("hidden", !ok);
      }});
    }}
    if (plFilterRow) {{
      plFilterRow.querySelectorAll("input,select").forEach(function (el) {{
        el.addEventListener("input", applyPlBundleFilters);
        el.addEventListener("change", applyPlBundleFilters);
      }});
      applyPlBundleFilters();
    }}
    if (plTbody) {{
      plTbody.addEventListener("click", function (ev) {{
        if (ev.target.closest("a")) return;
        var tr = plRowFromEventTarget(ev.target);
        if (!tr) return;
        ev.preventDefault();
        goProfile(tr);
      }});
      plTbody.addEventListener("keydown", function (ev) {{
        if (ev.key !== "Enter" && ev.key !== " ") return;
        var tr = plRowFromEventTarget(ev.target);
        if (!tr || !plTbody.contains(tr)) return;
        ev.preventDefault();
        goProfile(tr);
      }});
    }}
  }}
  document.querySelectorAll("table.pl-pipeline-tier").forEach(wirePipelineTierTable);

  if (!form) return;
  var q = form.querySelector('[name="q"]');
  var months = form.querySelector('[name="months"]');
  var nons1 = form.querySelector('[name="include_non_s1"]');
  var t;
  function submit() {{ form.submit(); }}
  if (q) {{
    q.addEventListener("input", function () {{
      clearTimeout(t);
      t = setTimeout(submit, 750);
    }});
    q.addEventListener("keydown", function (e) {{
      if (e.key === "Enter") {{ clearTimeout(t); submit(); }}
    }});
  }}
  if (months) months.addEventListener("change", submit);
  if (nons1) nons1.addEventListener("change", submit);
}})();
</script>
</body></html>"""


def _pipeline_company_tbody_rows_html(
    rows: list[Any],
    *,
    blur_class: str,
    empty_message: str,
) -> str:
    trs: list[str] = []
    for r in rows:
        trs.append(
            _pipeline_person_row_html(
                r,
                blur_class=blur_class,
                hide_company_column=True,
            )
        )
    if trs:
        return "".join(trs)
    return f"<tr><td colspan='9' class='dim empty'>{_esc(empty_message)}</td></tr>"


def render_pipeline_company_page(
    *,
    rows: list[Any],
    sales_bundle: str,
    company_name: str,
    back_href: str,
    blur_comp: bool,
    pipeline_path: str = "/admin/pipeline",
    search: str = "",
    months: int = 6,
    include_non_s1: bool = False,
) -> str:
    """One CIK, one roster per request — tier chosen by ``sales_bundle`` (no dual view)."""
    blur_class = " pl-blur-comp" if blur_comp else ""
    sb = (sales_bundle or "").strip().lower().replace("-", "_")
    if sb != "premium":
        sb = "economy"
    if sb == "premium":
        page_h2 = "Executive roster"
        page_sub = "NEO / summary-comp profiles (verified SCT) for this issuer."
        title_suffix = "Executive roster"
        empty_msg = "No NEO / summary-comp profiles in this company for the current window."
    else:
        page_h2 = "Roster"
        page_sub = "People for this issuer in your current filing window."
        title_suffix = "Roster"
        empty_msg = "No profiles in this roster for the current window."
    hq_detail_co = ""
    if rows:
        try:
            r0 = rows[0]
            hqr = (
                (r0["issuer_headquarters"] or "").strip()
                if "issuer_headquarters" in r0.keys()
                else ""
            )
            if hqr:
                hq_detail_co = (
                    hq_principal_office_display_line(format_headquarters_for_ui(hqr))
                    or ""
                )
        except (KeyError, TypeError, IndexError):
            hq_detail_co = ""
    co_addr_html = ""
    if hq_detail_co:
        co_addr_html = (
            f'<p class="pl-co-principal dim" style="margin:0.35rem 0 0 0;font-size:0.88rem;'
            f"line-height:1.35;max-width:44rem\">{_esc(hq_detail_co)}</p>"
        )
    cik_fb = ""
    if rows:
        try:
            cik_fb = str(rows[0]["cik"] or "").strip()
        except (KeyError, TypeError, IndexError):
            cik_fb = ""
    co_sec_html = _pipeline_company_sec_links_html(rows, cik_fallback=cik_fb)
    tbody_html = _pipeline_company_tbody_rows_html(
        rows,
        blur_class=blur_class,
        empty_message=empty_msg,
    )
    thead_html = """<thead><tr>
<th scope="col">Person</th>
<th scope="col" class="pl-th-text">Role</th>
<th scope="col" title="Pre-IPO vs listed from form types in your DB">Listing</th>
<th scope="col" class="pl-th-text" title="City and state from registrant principal office (no street)">Location</th>
<th scope="col" class="num">Filed</th>
<th scope="col" class="num" title="Non-equity-award SCT bundle">Cash and bonus</th>
<th scope="col" class="num" title="Stock awards + option awards, same FY (grant-date fair value)">Stock + options</th>
<th scope="col">Sources</th>
<th scope="col" class="pl-col-profile" title="Open full profile">Profile</th>
</tr><tr class="pl-filter-row">
<th><input type="search" class="pl-th-filter" data-plp-col="person" placeholder="Contains…" aria-label="Filter person"/></th>
<th><input type="search" class="pl-th-filter" data-plp-col="role" placeholder="Contains…" aria-label="Filter role"/></th>
<th><select class="pl-th-filter" data-plp-col="listing" aria-label="Filter listing">
<option value="">All</option><option value="pre_ipo">Pre-IPO</option><option value="public">Listed</option><option value="unknown">Unknown</option>
</select></th>
<th><input type="search" class="pl-th-filter" data-plp-col="loc" placeholder="Contains…" aria-label="Filter location"/></th>
<th><input type="search" class="pl-th-filter" data-plp-col="filed" placeholder="Contains…" aria-label="Filter filed"/></th>
<th><input type="number" class="pl-th-filter" data-plp-col="mincash" min="0" step="1000" placeholder="Min $" aria-label="Min cash USD"/></th>
<th><input type="number" class="pl-th-filter" data-plp-col="minstk" min="0" step="1000" placeholder="Min $" aria-label="Min stock USD"/></th>
<th><input type="search" class="pl-th-filter" data-plp-col="src" placeholder="Contains…" aria-label="Filter sources"/></th>
<th><span class="dim" style="font-size:0.68rem">—</span></th>
</tr></thead>"""
    back_attr = html_module.escape(back_href, quote=True)
    h2_esc = _esc(page_h2)
    sub_esc = _esc(page_sub)
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>{_esc(company_name)} — {_esc(title_suffix)} · {_esc(advisor_ui_product_name())}</title>
<style>
{PIPELINE_PAGE_CSS}
</style></head><body>
<div class="pl-shell">
<header class="pl-head pl-head--co">
  <div class="pl-brand">
    <span class="pl-title">{_esc(company_name)}</span>
    {co_addr_html}
    {co_sec_html}
  </div>
  <div class="pl-actions">
    <a href="{back_attr}">← Back</a>
  </div>
</header>
<section class="pl-package-section" style="margin-top:0.75rem" aria-labelledby="pl-co-roster-h">
<h2 id="pl-co-roster-h" class="pl-package-h2">{h2_esc}</h2>
<p class="pl-package-sub dim">{sub_esc}</p>
<div class="pl-table-wrap" style="margin-top:0.35rem">
<table class="pl-grid pl-co-roster">
{thead_html}
<tbody>{tbody_html}</tbody>
</table>
</div>
</section>
</div>
<script>
(function () {{
  function goProfile(tr) {{
    if (!tr) return;
    var href = tr.getAttribute("data-bundle-href") || tr.getAttribute("data-profile-href");
    if (href) window.location.href = href;
  }}
  function plRowFromEventTarget(t) {{
    if (!t) return null;
    var el = t.nodeType === 3 ? t.parentElement : t;
    return el && typeof el.closest === "function" ? el.closest("tr.pl-row") : null;
  }}
  function parsePP(s) {{
    if (s == null || s === "") return null;
    var x = parseFloat(String(s).replace(/,/g, ""));
    return isNaN(x) ? null : x;
  }}
  function cellLowerPP(tr, i) {{
    var td = tr.cells[i];
    return td ? (td.innerText || "").toLowerCase().trim() : "";
  }}
  var plTableCo = document.querySelector("table.pl-co-roster");
  var plTbody = plTableCo && plTableCo.tBodies[0];
  var plpFilterRow = plTableCo && plTableCo.querySelector("thead tr.pl-filter-row");
  function applyPlPersonFilters() {{
    if (!plTbody || !plpFilterRow) return;
    var fp = "", fr = "", floc = "", ff = "", fs = "", flist = "";
    var minC = null, minS = null;
    plpFilterRow.querySelectorAll("[data-plp-col]").forEach(function (el) {{
      var k = el.getAttribute("data-plp-col");
      var v = (el.value || "").trim();
      if (k === "person") fp = v.toLowerCase();
      else if (k === "role") fr = v.toLowerCase();
      else if (k === "loc") floc = v.toLowerCase();
      else if (k === "filed") ff = v.toLowerCase();
      else if (k === "src") fs = v.toLowerCase();
      else if (k === "listing") flist = v;
      else if (k === "mincash") minC = parsePP(v);
      else if (k === "minstk") minS = parsePP(v);
    }});
    plTbody.querySelectorAll("tr").forEach(function (tr) {{
      if (!tr.classList.contains("pl-row") || tr.classList.contains("pl-row-bundle")) return;
      var ok = true;
      if (fp && cellLowerPP(tr, 0).indexOf(fp) < 0) ok = false;
      if (ok && fr && cellLowerPP(tr, 1).indexOf(fr) < 0) ok = false;
      if (ok && flist && (tr.getAttribute("data-pl-listing") || "") !== flist) ok = false;
      if (ok && floc && cellLowerPP(tr, 3).indexOf(floc) < 0) ok = false;
      if (ok && ff && cellLowerPP(tr, 4).indexOf(ff) < 0) ok = false;
      if (ok && fs && cellLowerPP(tr, 7).indexOf(fs) < 0) ok = false;
      if (ok && minC != null) {{
        var c = parsePP(tr.getAttribute("data-pl-cash"));
        if (c == null || c < minC) ok = false;
      }}
      if (ok && minS != null) {{
        var st = parsePP(tr.getAttribute("data-pl-stock"));
        if (st == null || st < minS) ok = false;
      }}
      tr.classList.toggle("hidden", !ok);
    }});
  }}
  if (plpFilterRow) {{
    plpFilterRow.querySelectorAll("input,select").forEach(function (el) {{
      el.addEventListener("input", applyPlPersonFilters);
      el.addEventListener("change", applyPlPersonFilters);
    }});
    applyPlPersonFilters();
  }}
  if (plTbody) {{
    plTbody.addEventListener("click", function (ev) {{
      if (ev.target.closest("a")) return;
      var tr = plRowFromEventTarget(ev.target);
      if (!tr) return;
      ev.preventDefault();
      goProfile(tr);
    }});
    plTbody.addEventListener("keydown", function (ev) {{
      if (ev.key !== "Enter" && ev.key !== " ") return;
      var tr = plRowFromEventTarget(ev.target);
      if (!tr || !plTbody.contains(tr)) return;
      ev.preventDefault();
      goProfile(tr);
    }});
  }}
}})();
</script>
</body></html>"""


def _money_cell(v: Any) -> str:
    if v is None:
        return "—"
    try:
        x = float(v)
        if math.isnan(x):
            return "—"
        return f"${x:,.0f}"
    except (TypeError, ValueError):
        return "—"


def _pipeline_flags(r: Any) -> str:
    """SCT/source hints only — listing stage has its own column."""
    parts: list[str] = []
    if _profile_row_int(r, "has_beneficial_owner_stake"):
        parts.append("5%+")
    try:
        lt = (r["lead_tier"] or "").strip().lower()
    except (KeyError, TypeError, IndexError):
        lt = ""
    if lt in ("standard", "visibility"):
        parts.append(lt[:3])
    if _profile_row_int(r, "has_s1_comp"):
        parts.append("S-1")
    if _profile_row_int(r, "has_mgmt_bio"):
        parts.append("bio")
    if _profile_row_int(r, "has_officer_row"):
        parts.append("officer")
    if _profile_row_int(r, "comp_llm_assisted"):
        parts.append("LLM")
    return ", ".join(parts) if parts else "—"
