from __future__ import annotations

import html
import os
import sqlite3
import sys
import threading
import webbrowser
from pathlib import Path
from wsgiref.simple_server import make_server

from wealth_leads.config import database_path
from wealth_leads.db import connect


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
        body_rows.append(
            "<tr>"
            f"<td>{company}</td>"
            f"<td>{html.escape(str(r['cik'] or ''))}</td>"
            f"<td>{html.escape(str(r['filing_date'] or ''))}</td>"
            f"<td>{html.escape(name)}</td>"
            f"<td>{title}</td>"
            f"<td>{idx_link}</td>"
            f"<td>{doc_link}</td>"
            "</tr>"
        )
    inner = (
        "".join(body_rows)
        if body_rows
        else '<tr><td colspan="7">No rows yet. Run sync first.</td></tr>'
    )
    return f"""
  <h2>Officers &amp; directors (signature block)</h2>
  <table>
    <thead>
      <tr>
        <th>Company</th><th>CIK</th><th>Filed</th><th>Name</th><th>Title</th><th>EDGAR</th><th>Doc</th>
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
  <h2>NEO summary compensation (parsed from S-1 tables)</h2>
  <p class="meta">Dollar amounts are <b>as disclosed</b> in the registration statement (e.g. stock awards often reflect grant-date fair value). Not tax or net-worth advice.</p>
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


def _page(leads: list[sqlite3.Row], comp: list[sqlite3.Row]) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>WealthPipeline — S-1 leads</title>
  <style>
    :root {{ font-family: system-ui, sans-serif; background: #0f1419; color: #e7e9ea; }}
    body {{ margin: 0; padding: 1.25rem; max-width: 1280px; margin-inline: auto; }}
    h1 {{ font-size: 1.25rem; font-weight: 600; margin-top: 0; }}
    h2 {{ font-size: 1.05rem; margin-top: 2rem; margin-bottom: 0.5rem; }}
    p.meta {{ color: #8b98a5; font-size: 0.875rem; margin-bottom: 1rem; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.8125rem; }}
    th, td {{ text-align: left; padding: 0.5rem 0.6rem; border-bottom: 1px solid #38444d; vertical-align: top; }}
    th {{ color: #8b98a5; font-weight: 600; }}
    td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    a {{ color: #1d9bf0; }}
    tr:hover td {{ background: #1a2228; }}
    code {{ font-size: 0.8em; }}
  </style>
</head>
<body>
  <h1>S-1 / S-1A pipeline</h1>
  <p class="meta">Local SQLite: <code>{html.escape(database_path())}</code> — run <code>python -m wealth_leads sync</code> to refresh.</p>
  {_leads_table(leads)}
  {_comp_table(comp)}
</body>
</html>"""


def _fetch_leads() -> list[sqlite3.Row]:
    dbp = database_path()
    if not Path(dbp).is_file():
        return []
    with connect() as conn:
        cur = conn.execute(
            """
            SELECT f.company_name, f.cik, f.filing_date, o.name, o.title,
                   f.index_url, f.primary_doc_url
            FROM filings f
            LEFT JOIN officers o ON o.filing_id = f.id
            ORDER BY f.filing_date DESC, f.company_name, o.name
            """
        )
        return list(cur.fetchall())


def _fetch_comp() -> list[sqlite3.Row]:
    dbp = database_path()
    if not Path(dbp).is_file():
        return []
    with connect() as conn:
        if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='neo_compensation'"
        ).fetchone():
            return []
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
        return list(cur.fetchall())


def _app(environ, start_response):
    if environ.get("PATH_INFO", "/") not in ("/", ""):
        start_response("404 Not Found", [("Content-Type", "text/plain; charset=utf-8")])
        return [b"Not Found"]

    body = _page(_fetch_leads(), _fetch_comp()).encode("utf-8")
    start_response("200 OK", [("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(body)))])
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
