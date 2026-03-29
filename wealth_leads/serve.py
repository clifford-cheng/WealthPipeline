from __future__ import annotations

import html
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from typing import Optional
import threading
import webbrowser
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlencode
from wsgiref.simple_server import make_server

from wealth_leads.config import database_path
from wealth_leads.db import connect


def _norm_person_name(name: str) -> str:
    s = (name or "").lower().replace(".", " ")
    return " ".join(s.split())


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
               f.index_url, f.primary_doc_url, f.form_type AS filing_form_type
        FROM neo_compensation c
        JOIN filings f ON f.id = c.filing_id
        """
    )
    raw = [dict(r) for r in cur.fetchall()]
    if not raw:
        return []

    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in raw:
        groups[_profile_key(row["cik"], row["person_name"])].append(row)

    filing_ids = {r["filing_id"] for r in raw}
    off_titles: dict[tuple[int, str], str] = {}
    if filing_ids:
        qmarks = ",".join("?" * len(filing_ids))
        ocur = conn.execute(
            f"SELECT filing_id, name, title FROM officers WHERE filing_id IN ({qmarks})",
            tuple(filing_ids),
        )
        for o in ocur.fetchall():
            k = (int(o["filing_id"]), _norm_person_name(o["name"] or ""))
            off_titles[k] = o["title"] or ""

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

        title_guess = (head.get("role_hint") or "").strip()
        if not title_guess:
            title_guess = off_titles.get(
                (int(head["filing_id"]), _norm_person_name(head["person_name"] or "")),
                "",
            )

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
                "years_count": len(by_year),
                "comp_timeline": timeline,
                "sum_year_totals": sum_year_totals,
                "year_breakdown": year_breakdown,
            }
        )

    profiles.sort(
        key=lambda p: (p["filing_date"] or "", p["headline_year"] or 0, p["total"] or 0),
        reverse=True,
    )
    return profiles


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


def _filings_for_profile(conn: sqlite3.Connection, cik: str, norm_name: str) -> list[dict]:
    cur = conn.execute(
        """
        SELECT f.id, f.accession, f.form_type, f.filing_date, f.index_url, f.primary_doc_url,
               c.person_name
        FROM filings f
        JOIN neo_compensation c ON c.filing_id = f.id
        WHERE f.cik = ?
        ORDER BY f.filing_date DESC, f.id DESC
        """,
        (str(cik or "").strip(),),
    )
    seen: set[int] = set()
    out: list[dict] = []
    for r in cur.fetchall():
        if _norm_person_name(r["person_name"] or "") != norm_name:
            continue
        fid = int(r["id"])
        if fid in seen:
            continue
        seen.add(fid)
        out.append(
            {
                "id": fid,
                "accession": r["accession"] or "",
                "form_type": r["form_type"] or "",
                "filing_date": r["filing_date"] or "",
                "index_url": r["index_url"] or "",
                "primary_doc_url": r["primary_doc_url"] or "",
            }
        )
    return out


def _desk_table(profiles: list[dict]) -> str:
    if not profiles:
        return """
  <h2>Lead desk</h2>
  <p class="meta">No NEO compensation rows yet — run sync / backfill-comp, or open <b>Source rows</b> below.</p>"""

    rows: list[str] = []
    for p in profiles:
        company = html.escape(p["company_name"] or "")
        title = html.escape(p["title"] or "—")
        href = html.escape(_profile_lead_url(p))
        nm = html.escape(p["display_name"] or "")
        sum_sct = p.get("sum_year_totals")
        sum_cell = _money(sum_sct) if sum_sct is not None else "—"
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
        rows.append(
            "<tr class='desk-row' tabindex='0' role='link' "
            f"data-href='{href}' title='Open profile'>"
            f"<td class='profile-name'><a href='{href}'>{nm}</a></td>"
            f"<td>{title}</td>"
            f"<td>{company}</td>"
            f"<td><span class='badge'>{badge}</span></td>"
            f"<td class='num strong'>{_money(p['total'])}</td>"
            f"<td class='num'>{sum_cell}</td>"
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
    <b>Timing-first.</b> Each row is one executive at one issuer (CIK). <b>Latest total</b> = most recent fiscal year in your DB; <b>Σ SCT</b> = sum of disclosed yearly totals (not take-home cash).
    <b>Click the name or row</b> for compensation detail, filings, and (soon) management bio. Raw rows: <b>Source rows</b> below.
  </p>
  <div class="table-wrap">
  <table id="desk">
    <thead>
      <tr>
        <th>Person</th><th>Role</th><th>Company</th><th>Signal</th>
        <th>Latest total</th><th title='Sum of SCT Total per FY in DB'>Σ SCT</th><th>FY</th><th>Filing</th><th>CIK</th><th>Quick source</th>
      </tr>
    </thead>
    <tbody id="desk-body">{inner}</tbody>
  </table>
  </div>"""


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
    latest = html.escape(str(stats.get("latest_filing_date") or "—"))
    mtime = html.escape(str(stats.get("db_file_modified") or "—"))
    rat = html.escape(rendered_at)
    return f"""<div class="banner">
    <strong>Local snapshot</strong>
    <span class="stats"><span>{np} lead profiles</span><span>{nf} filings</span><span>{no} officer rows</span><span>{nc} comp rows</span></span>
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
    tr.desk-row { cursor: pointer; }
    tr.desk-row:focus { outline: 1px solid #5eb3e0; outline-offset: -1px; }
    tr.desk-row:hover td { background: #121820; }
    span.badge {
      display: inline-block; font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.04em;
      padding: 0.15rem 0.4rem; border-radius: 4px; background: #1a2634; color: #8b96a3; border: 1px solid #2a3340;
    }
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
    @media (min-width: 640px) { .hero { grid-template-columns: 1fr 1fr 1fr; } }
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
    SEC filing–native lead timing (S-1 + 10-K from RSS by default). Data updates when you run <code>sync</code>.
    Database: <code>{html.escape(str(Path(database_path()).resolve()))}</code>
  </p>
  {banner}
  {_comp_missing_callout(stats)}
  <label class="sr" for="filter">Filter desk + audit tables</label>
  <input type="search" id="filter" placeholder="Company, person, or CIK…" autocomplete="off"/>
  {_desk_table(profiles)}
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


def _page_lead(
    profile: Optional[dict],
    filings: list[dict],
    *,
    query_cik: str,
    query_name: str,
    stats: dict,
    rendered_at: str,
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
  <nav class="top"><a href="/">← Lead desk</a></nav>
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

    bio_block = f"""
    <div class="card bio-placeholder">
      <h2 style="margin-top:0">Management biography</h2>
      <p class="meta" style="margin-bottom:0">
        <strong>Coming next.</strong> We have not extracted the narrative bio from the registration statement yet.
        For now, {doc_link or "open the issuer’s filing from EDGAR"} and search for <b>Management</b>,
        <b>Directors and Executive Officers</b>, or the person’s name. That section usually lists prior roles, education, and board seats — ideal for professional rapport cues.
      </p>
    </div>"""

    bookmark_q = urlencode(
        {"cik": str(p.get("cik") or ""), "name": p.get("display_name") or ""}
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{html.escape(p.get('display_name') or 'Lead')} — WealthPipeline</title>
  <style>{css}</style>
</head>
<body class="detail-page">
  <nav class="top"><a href="/">← Lead desk</a></nav>
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
  <div class="hero">
    <div class="stat-card"><div class="lbl">Latest FY total</div><div class="val">{_money(p.get('total'))}</div></div>
    <div class="stat-card"><div class="lbl">Σ SCT (years in DB)</div><div class="val">{sum_disp}</div></div>
    <div class="stat-card"><div class="lbl">Fiscal years w/ comp</div><div class="val">{int(p.get('years_count') or 0)}</div></div>
  </div>
  <h2>Headline compensation (latest fiscal year)</h2>
  {headline_tbl}
  <h2>Year-by-year (summary comp table)</h2>
  {_profile_breakdown_table(p)}
  {bio_block}
  <h2>Filings (this person + issuer)</h2>
  {_filings_table_html(filings)}
  <p class="meta">Bookmark this page: <code>/lead?{html.escape(bookmark_q)}</code></p>
  {banner}
</body>
</html>"""


def _load_page_data() -> tuple[list[dict], list[sqlite3.Row], list[sqlite3.Row], dict]:
    dbp = database_path()
    if not Path(dbp).is_file():
        return [], [], [], {"missing_db": True, "profile_count": 0}

    mtime = datetime.fromtimestamp(Path(dbp).stat().st_mtime).strftime("%Y-%m-%d %H:%M")

    with connect() as conn:
        profiles = _build_profiles(conn)

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
        "latest_filing_date": latest,
        "db_file_modified": mtime,
    }
    return profiles, leads, comp, stats


def _app(environ, start_response):
    path = environ.get("PATH_INFO") or "/"
    if path != "/" and path.rstrip("/") != "/lead":
        start_response("404 Not Found", [("Content-Type", "text/plain; charset=utf-8")])
        return [b"Not Found"]

    profiles, leads, comp, stats = _load_page_data()
    rendered_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if path.rstrip("/") == "/lead":
        qs = parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=True)
        cik = (qs.get("cik") or [""])[0].strip()
        name_raw = (qs.get("name") or [""])[0]
        name_decoded = unquote(name_raw) if name_raw else ""
        norm = _norm_person_name(name_decoded)
        prof = _find_profile(profiles, cik, norm) if not stats.get("missing_db") else None
        filings: list[dict] = []
        if prof is not None and not stats.get("missing_db"):
            with connect() as conn:
                filings = _filings_for_profile(conn, cik, norm)
        html_out = _page_lead(
            prof,
            filings,
            query_cik=cik,
            query_name=name_decoded,
            stats=stats,
            rendered_at=rendered_at,
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


def run_localhost(*, port: int | None = None, open_browser: bool = True) -> None:
    p = port or int(os.environ.get("WEALTH_LEADS_PORT", "8765"))
    url = f"http://127.0.0.1:{p}/"
    try:
        httpd = make_server("127.0.0.1", p, _app)
    except OSError as e:
        print(f"Could not listen on {url} (port {p}): {e}", file=sys.stderr)
        print("Another copy may be running, or the port is in use.", file=sys.stderr)
        raise SystemExit(1) from e
    print(f"WealthPipeline dashboard: {url}")
    print("Press Ctrl+C to stop.")
    if open_browser:
        print("Opening your browser in a moment…")
        _open_browser_when_ready(url)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
