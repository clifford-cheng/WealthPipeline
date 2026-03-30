"""HTML fragments for territory allocation / client CRM UI."""
from __future__ import annotations

import html as html_module
import math
import re
from typing import Any
from urllib.parse import quote, urlencode

from wealth_leads.serve import _pay_band_nav_html
from wealth_leads.territory import (
    hq_city_state_display,
    hq_city_state_looks_like_filing_noise,
    strip_hq_lease_and_rental_tail,
    strip_registrant_hq_contact_tail,
)
from wealth_leads.title_badge import advisor_title_badge


def _esc(s: Any) -> str:
    return html_module.escape(str(s) if s is not None else "")


def format_headquarters_for_ui(raw: str | None) -> str:
    """Collapse multiline SEC principal-office blocks into one line for UI and geocoding."""
    s = (raw if raw is not None else "").strip()
    if not s:
        return ""
    parts = [p.strip() for p in re.split(r"[\n\r]+", s) if p.strip()]
    one = ", ".join(parts)
    one = re.sub(r"[ \t]{2,}", " ", one).strip()
    one = strip_hq_lease_and_rental_tail(strip_registrant_hq_contact_tail(one))
    return one


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
        q = urlencode(
            {
                "cik": r.get("cik") or "",
                "name": prof.get("display_name") or snap.get("display_name") or "",
            }
        )
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


def render_admin_home(
    *,
    clients: list[Any],
    settings_row: Any,
    cycle: str,
    alloc_msg: str,
    filing_count: int | None = None,
    sync_info: dict[str, Any] | None = None,
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
input,select,button{{font:inherit;font-size:0.8rem}}
a{{color:#58a6ff}}
code{{font-size:0.78rem;background:#0a0e12;padding:0.1rem 0.35rem;border-radius:3px}}
.card{{background:#121820;border:1px solid #2a3340;border-radius:6px;padding:1rem;margin:1rem 0;max-width:42rem}}
.sr{{position:absolute;width:1px;height:1px;overflow:hidden}}
pre.log{{white-space:pre-wrap;word-break:break-word;font-size:0.72rem;background:#0a0e12;padding:0.5rem;border-radius:4px;max-height:12rem;overflow:auto;border:1px solid #2a3340}}
</style></head><body{body_attr}>
<h1>Admin — territories & allocation</h1>
<p class="meta"><a href="/my-leads">My assigned leads</a> · <a href="/admin/pipeline"><strong>Pipeline review</strong></a> · <a href="/admin/desk">Full lead desk</a> · <a href="/admin/finder">Finder</a></p>
{msg}
{sync_note}
<div class="card">
<h2 style="margin-top:0;font-size:1rem">SEC data — sync from here</h2>
<p class="meta">Loads filings into <strong>wealth_leads.sqlite3</strong> and rebuilds the <code>lead_profile</code> table (see <a href="/admin/pipeline">Pipeline review</a>). Then run <strong>Assign leads</strong> if you use territories, or work from the pipeline / desk.</p>
<p class="meta"><strong>Filings in database now:</strong> {_esc(filing_count) if filing_count is not None else "unknown"}</p>
<form method="post" action="/admin/sync" style="display:flex;flex-wrap:wrap;gap:0.75rem;align-items:center;margin-top:0.5rem">
<button type="submit" style="padding:0.45rem 0.9rem;border-radius:4px;border:none;background:#238636;color:#fff;cursor:pointer;font-weight:600" {"disabled" if sync_running else ""}>Run SEC sync now</button>
<label style="font-size:0.8rem;color:#8b96a3"><input type="checkbox" name="force" value="1"/> Force reprocess (slow; re-fetches filings)</label>
</form>
<p class="meta"><strong>Automatic updates:</strong> While this server is running, a background job pulls SEC data on a schedule (default every <strong>24 hours</strong>; see the black console line when you start the app). Log file: <code>logs\\sec-sync.log</code>. Turn off with <code>WEALTH_LEADS_AUTO_SYNC_HOURS=0</code> before starting.</p>
<p class="meta">If SEC blocks requests, set <code>SEC_USER_AGENT</code> with your contact email, restart the server. Optional: <code>scripts\\windows\\Run SEC sync only.bat</code> or Task Scheduler if the PC is off when the server isn’t running.</p>
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
</body></html>"""


_HQ_CUT_MARKERS = (
    " and our telephone",
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


def pipeline_hq_city_state(r: Any) -> str:
    """City, ST (no street) for pipeline — prefer live parse from full HQ so stale DB rows stay correct."""
    raw_row = (
        (r["issuer_headquarters"] or "").strip()
        if "issuer_headquarters" in r.keys()
        else ""
    )
    if raw_row:
        live = hq_city_state_display(format_headquarters_for_ui(raw_row))
        if live:
            return live
    if "issuer_hq_city_state" in r.keys():
        v = (r["issuer_hq_city_state"] or "").strip()
        if v and not hq_city_state_looks_like_filing_noise(v):
            return v
    return ""


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
    return s if n > 0 else None


def render_pipeline_review_page(
    *,
    rows: list[Any],
    total_in_db: int,
    visible_count: int,
    search: str,
    cross_only: bool,
    include_non_s1: bool,
    months: int,
    msg: str,
    built_hint: str,
    pipeline_path: str = "/admin/pipeline",
    pay_band: str = "all",
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
    row_open_hint = html_module.escape(
        "Opens full profile page (/lead). Click the row or ›. Enter when focused.",
        quote=True,
    )
    blur_class = " pl-blur-comp" if blur_comp else ""
    trs: list[str] = []
    for r in rows:
        cross_badge = (
            "<span class=\"cross\">Multi-CIK</span>" if int(r["cross_company_hint"] or 0) else ""
        )
        cik_s = str(r["cik"] or "").strip()
        name_for_lead = (r["person_norm"] or "").strip() or (r["display_name"] or "").strip()
        profile_q = urlencode({"cik": cik_s, "name": name_for_lead})
        profile_href = html_module.escape(f"/lead?{profile_q}", quote=True)
        title_r = (r["title"] or "").strip() or "—"
        role_badge = advisor_title_badge(title_r)
        full_title_attr = html_module.escape(title_r, quote=True)
        fy = r["headline_year"]
        fy_label = str(int(fy)) if fy is not None else "—"
        loc_tt = html_module.escape(
            "City and state from the registrant principal office in the filing (not the person’s home). "
            "No street or ZIP in this column.",
            quote=True,
        )
        cash_tt = html_module.escape(
            (
                f"Cash and bonus (plus other non-equity SCT cash/benefit lines): total minus stock + option "
                f"award columns when both exist; else salary + bonus + other for FY {fy_label}. "
                "Full SCT table on profile."
                if fy is not None
                else "Cash-side SCT bundle for headline FY; see profile for detail."
            ),
            quote=True,
        )
        stock_tt = html_module.escape(
            (
                f"Sum of stock awards + option awards for FY {fy_label} (grant-date fair value, SCT). "
                "Illiquid pre-exit; vesting per plan."
                if fy is not None
                else "Stock + option awards for headline FY (grant-date value)."
            ),
            quote=True,
        )
        row_keys = r.keys()
        cash_v = pipeline_cash_excl_equity_from_row(r)
        stk_v = r["stock_grants_headline"] if "stock_grants_headline" in row_keys else None
        loc_s = pipeline_hq_city_state(r)
        trs.append(
            "<tr class=\"pl-row\" tabindex=\"0\" role=\"link\" "
            f'data-profile-href="{profile_href}" title="{row_open_hint}">'
            + f"<td>{cross_badge}<strong>{_esc(r['display_name'])}</strong></td>"
            + f"<td class='pl-title-cell' title=\"{full_title_attr}\">"
            + f"<strong>{_esc(role_badge)}</strong>"
            + "</td>"
            + f"<td>{_esc(r['company_name'])}</td>"
            + f'<td class="pl-loc" title="{loc_tt}">{_esc(loc_s or "—")}</td>'
            + f"<td class='num'>{_esc(r['filing_date_latest'])}</td>"
            + f'<td class="num{blur_class}" title="{cash_tt}">{_money_cell(cash_v)}</td>'
            + f'<td class="num{blur_class}" title="{stock_tt}">{_money_cell(stk_v)}</td>'
            + f"<td class='dim'>{_esc(_pipeline_flags(r))}</td>"
            + f'<td class="pl-row-cue"><a class="pl-row-profile" href="{profile_href}" '
            + 'aria-label="Open full profile">›</a></td>'
            + "</tr>"
        )
    body = "".join(trs) if trs else "<tr><td colspan='9' class='dim empty'>No rows in this window. Widen the date range, sync SEC data, pay-signal tab, or run <code>py -m wealth_leads rebuild-profiles</code>.</td></tr>"
    cq = " checked" if cross_only else ""
    ins = " checked" if include_non_s1 else ""
    qqs = quote(search)
    band_q = quote((pay_band or "all").strip() or "all")
    csv_href = (
        f"{_esc(pipeline_path)}.csv?q={qqs}&months={months}&cross_only={1 if cross_only else 0}"
        f"{'&include_non_s1=1' if include_non_s1 else ''}&band={band_q}"
    )
    band_cur = (pay_band or "all").strip() or "all"
    band_nav = _pay_band_nav_html(
        current=band_cur,
        base_path=pipeline_path.split("?")[0],
        extra_qs={
            "q": search,
            "months": str(months),
            "cross_only": "1" if cross_only else "",
            "include_non_s1": "1" if include_non_s1 else "",
        },
    )
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Lead profile — WealthPipeline</title>
<style>
:root {{
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
}}
* {{ box-sizing: border-box; }}
body {{
  margin: 0;
  font-family: var(--sans);
  font-size: 13px;
  background: var(--bg);
  color: var(--text);
  line-height: 1.45;
  min-height: 100vh;
}}
.pl-shell {{ max-width: 1440px; margin: 0 auto; padding: 0 1rem 2rem; }}
.pl-head {{
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  justify-content: space-between;
  gap: 0.75rem 1.5rem;
  padding: 1rem 0 0.75rem;
  border-bottom: 1px solid var(--line);
}}
.pl-brand {{ display: flex; align-items: baseline; gap: 0.65rem; flex-wrap: wrap; }}
.pl-logo {{
  font-weight: 700;
  letter-spacing: 0.04em;
  font-size: 0.72rem;
  text-transform: uppercase;
  color: var(--amber);
}}
.pl-title {{ font-size: 1.15rem; font-weight: 600; letter-spacing: -0.02em; }}
.pl-sub {{ font-size: 0.75rem; color: var(--muted); }}
.pl-actions {{ display: flex; flex-wrap: wrap; gap: 0.5rem 1rem; align-items: center; }}
.pl-actions a {{
  color: var(--amber);
  text-decoration: none;
  font-size: 0.8rem;
}}
.pl-actions a:hover {{ text-decoration: underline; color: #f0b429; }}
.pl-banner {{
  font-size: 0.78rem;
  color: var(--muted);
  padding: 0.65rem 0 0;
  max-width: 52rem;
  line-height: 1.5;
}}
.pl-banner strong {{ color: var(--text); font-weight: 600; }}
.pl-banner code {{ font-family: var(--mono); font-size: 0.72rem; background: var(--panel); padding: 0.1rem 0.35rem; border-radius: 2px; }}
.pl-toolbar {{
  display: flex;
  flex-wrap: wrap;
  align-items: flex-end;
  gap: 1rem 1.5rem;
  padding: 1rem 0;
  border-bottom: 1px solid var(--line);
}}
.pl-filters {{
  display: flex;
  flex-wrap: wrap;
  align-items: flex-end;
  gap: 1rem 1.25rem;
}}
.pl-field {{ display: flex; flex-direction: column; gap: 0.2rem; }}
.pl-field label {{
  font-size: 0.65rem;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: var(--muted);
}}
.pl-field input[type="text"],
.pl-field input[type="number"] {{
  background: var(--panel);
  border: 1px solid var(--line);
  color: var(--text);
  border-radius: 2px;
  padding: 0.35rem 0.5rem;
  font-family: var(--sans);
  font-size: 0.8125rem;
  min-width: 0;
}}
.pl-field input[type="text"] {{ width: min(22rem, 88vw); }}
.pl-field input[type="number"] {{ width: 4.25rem; font-family: var(--mono); }}
.pl-check {{
  display: flex;
  align-items: center;
  gap: 0.4rem;
  padding-bottom: 0.35rem;
  font-size: 0.8rem;
  color: var(--muted);
}}
.pl-check input {{ accent-color: var(--amber); }}
.pl-hint {{ font-size: 0.68rem; color: var(--faint); margin-top: 0.15rem; }}
.pl-rebuild {{
  margin-left: auto;
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.5rem 0.75rem;
}}
.pl-rebuild button {{
  font-family: var(--sans);
  font-size: 0.75rem;
  background: transparent;
  color: var(--muted);
  border: 1px solid var(--line);
  border-radius: 2px;
  padding: 0.35rem 0.65rem;
  cursor: pointer;
}}
.pl-rebuild button:hover {{ border-color: var(--amber-dim); color: var(--text); }}
.pl-rebuild span {{ font-size: 0.68rem; color: var(--faint); max-width: 14rem; line-height: 1.35; }}
.msg-ok {{ color: var(--green); font-size: 0.8rem; padding: 0.5rem 0 0; margin: 0; }}
.pl-table-wrap {{
  overflow-x: auto;
  margin-top: 0.5rem;
  border: 1px solid var(--line);
  border-radius: 2px;
  background: var(--panel);
}}
table.pl-grid {{
  width: 100%;
  border-collapse: collapse;
  font-size: 0.78rem;
}}
.pl-grid thead th {{
  position: sticky;
  top: 0;
  z-index: 1;
  background: #161618;
  border-bottom: 1px solid var(--line);
  padding: 0.5rem 0.6rem;
  text-align: left;
  font-weight: 600;
  font-size: 0.65rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--muted);
  white-space: nowrap;
}}
.pl-grid tbody tr:nth-child(even) {{ background: rgba(255,255,255,0.02); }}
.pl-grid tbody tr:hover {{ background: rgba(226,160,18,0.06); }}
.pl-grid td {{
  border-bottom: 1px solid var(--line);
  padding: 0.45rem 0.6rem;
  vertical-align: top;
}}
.pl-grid td.empty {{ padding: 2rem 1rem; text-align: center; color: var(--faint); }}
.num {{ text-align: right; font-family: var(--mono); font-variant-numeric: tabular-nums; }}
.dim {{ color: var(--faint); }}
.mono {{ font-family: var(--mono); font-size: 0.72rem; }}
.sub {{ font-size: 0.68rem; color: var(--faint); margin-top: 0.15rem; font-family: var(--mono); }}
.hq {{ color: #c4c4ca; max-width: 14rem; cursor: default; }}
.cross {{
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
}}
.pl-row {{ cursor: pointer; }}
.pl-row:hover {{ background: rgba(226,160,18,0.08) !important; }}
.pl-row:focus {{ outline: 1px solid var(--amber); outline-offset: -2px; }}
.pl-col-profile {{
  width: 4.5rem;
  text-align: center;
  font-size: 0.62rem;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: var(--muted);
}}
.pl-row-cue {{
  padding: 0 !important;
  width: 2.25rem;
  text-align: center;
  vertical-align: middle;
}}
.pl-row-profile {{
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 2.25rem;
  color: var(--amber);
  text-decoration: none;
  font-size: 1.15rem;
  font-weight: 300;
  line-height: 1;
}}
.pl-row-profile:hover {{ color: #f0b429; background: rgba(226,160,18,0.1); }}
.pl-title-cell {{ max-width: 14rem; color: #c4c4ca; }}
.pl-loc {{ max-width: 11rem; color: #b0b0b8; font-size: 0.92em; line-height: 1.35; }}
.pl-comp-preview {{
  max-width: 18rem;
  font-family: var(--mono);
  font-size: 0.72rem;
  color: #b8b8be;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}}
.pay-band-wrap {{ margin: 0.5rem 0 0; max-width: 52rem; }}
.pay-band-hint {{ font-size: 0.72rem; color: var(--faint); margin: 0 0 0.35rem; line-height: 1.45; }}
.pay-band-nav {{ display: flex; flex-wrap: wrap; gap: 0.35rem 0.65rem; align-items: center; font-size: 0.78rem; }}
.pay-band-tab {{
  color: var(--muted);
  text-decoration: none;
  padding: 0.15rem 0.4rem;
  border-radius: 2px;
  border: 1px solid transparent;
}}
.pay-band-tab:hover {{ color: var(--text); border-color: var(--line); }}
.pay-band-tab--active {{
  color: var(--amber);
  border-color: rgba(226, 160, 18, 0.35);
  background: rgba(226, 160, 18, 0.06);
}}
.pl-blur-comp {{
  filter: blur(5px);
  user-select: none;
}}
</style></head><body>
<div class="pl-shell">
<header class="pl-head">
  <div class="pl-brand">
    <span class="pl-logo">WealthPipeline</span>
    <span class="pl-title">Pre-IPO pipeline</span>
    <span class="pl-sub">Up to {visible_count} shown · {_esc(win_label)} · {total_in_db} profiles in DB</span>
  </div>
  <div class="pl-actions">
    <a href="/admin">Sync &amp; settings</a>
    <a href="{csv_href}">Export CSV</a>
  </div>
</header>
<p class="pl-banner">{_esc(built_hint)} · <strong>Cash and bonus</strong> is the non-equity-award side of SCT (total − stock/option awards when both exist; else salary+bonus+other). <strong>Location</strong> is city + state from the registrant HQ line (no street). <strong>Rebuild profiles</strong> after code updates. <code>WEALTH_LEADS_PIPELINE_BLUR_COMP=1</code> blurs comp.</p>
{msg_html}
<div class="pl-toolbar">
  <form id="pl-filters" class="pl-filters" method="get" action="{_esc(pipeline_path)}" autocomplete="off" data-pipeline-path="{_esc(pipeline_path)}">
    <input type="hidden" name="band" value="{_esc(band_cur)}"/>
    <div class="pl-field">
      <label for="pl-months">Filing window</label>
      <select id="pl-months" name="months">{months_select}</select>
      <span class="pl-hint">Rows sorted newest first within window</span>
    </div>
    <div class="pl-field">
      <label for="pl-q">Filter</label>
      <input id="pl-q" type="text" name="q" value="{_esc(search)}" placeholder="Name, company, CIK…"/>
      <span class="pl-hint">Updates as you type (short pause)</span>
    </div>
    <label class="pl-check"><input type="checkbox" name="cross_only" value="1"{cq}/> Multi-CIK only</label>
    <label class="pl-check"><input type="checkbox" name="include_non_s1" value="1"{ins}/> Include NEO from non–S-1 filings (e.g. 10-K)</label>
  </form>
  <form class="pl-rebuild" method="post" action="{_esc(pipeline_path)}/rebuild">
    <button type="submit">Rebuild profiles</button>
    <span>No SEC fetch—rebuilds materialized rows from current DB.</span>
  </form>
</div>
{band_nav}
<div class="pl-table-wrap">
<table class="pl-grid">
<thead><tr>
<th scope="col">Person</th>
<th scope="col">Role</th>
<th scope="col">Company</th>
<th scope="col" title="City and state from registrant principal office (no street)">Location</th>
<th scope="col" class="num">Filed</th>
<th scope="col" class="num" title="Non-equity-award SCT bundle (see banner)">Cash and bonus</th>
<th scope="col" class="num" title="Stock awards + option awards, same FY (grant-date fair value)">Stock + options</th>
<th scope="col">Sources</th>
<th scope="col" class="pl-col-profile" title="Open full profile">Profile</th>
</tr></thead>
<tbody>{body}</tbody>
</table>
</div>
</div>
<script>
(function () {{
  var form = document.getElementById("pl-filters");

  function goProfile(tr) {{
    if (!tr) return;
    var href = tr.getAttribute("data-profile-href");
    if (href) window.location.href = href;
  }}

  function plRowFromEventTarget(t) {{
    if (!t) return null;
    var el = t.nodeType === 3 ? t.parentElement : t;
    return el && typeof el.closest === "function" ? el.closest("tr.pl-row") : null;
  }}

  var plTbody = document.querySelector("table.pl-grid tbody");
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

  if (!form) return;
  var q = form.querySelector('[name="q"]');
  var months = form.querySelector('[name="months"]');
  var cross = form.querySelector('[name="cross_only"]');
  var nons1 = form.querySelector('[name="include_non_s1"]');
  var t;
  function submit() {{ form.submit(); }}
  if (q) {{
    q.addEventListener("input", function () {{
      clearTimeout(t);
      t = setTimeout(submit, 450);
    }});
    q.addEventListener("keydown", function (e) {{
      if (e.key === "Enter") {{ clearTimeout(t); submit(); }}
    }});
  }}
  if (months) months.addEventListener("change", submit);
  if (cross) cross.addEventListener("change", submit);
  if (nons1) nons1.addEventListener("change", submit);
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
    parts: list[str] = []
    try:
        lt = (r["lead_tier"] or "").strip().lower()
    except (KeyError, TypeError, IndexError):
        lt = ""
    if lt in ("standard", "visibility"):
        parts.append(lt[:3])
    if int(r["has_s1_comp"] or 0):
        parts.append("S-1")
    if int(r["has_mgmt_bio"] or 0):
        parts.append("bio")
    if int(r["has_officer_row"] or 0):
        parts.append("officer")
    try:
        if int(r["comp_llm_assisted"] or 0):
            parts.append("LLM")
    except (KeyError, TypeError, IndexError):
        pass
    return ", ".join(parts) if parts else "—"
