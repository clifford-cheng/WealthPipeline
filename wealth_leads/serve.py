from __future__ import annotations

import html
import os
import sqlite3
import sys
from datetime import datetime
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
        <th>Company</th><th title="Central Index Key — SEC's company ID number, not dollars">CIK</th><th>Filed</th><th>Name</th><th>Title</th><th>EDGAR</th><th>Doc</th>
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


def _comp_missing_callout(stats: dict) -> str:
    if stats.get("missing_db"):
        return ""
    if stats.get("comp_rows", 0) > 0:
        return ""
    if stats.get("filings", 0) == 0:
        return ""
    return """<div class="callout">
    <strong>No compensation rows in your database yet.</strong>
    Pay data is <em>not</em> the CIK column (that is only SEC’s company ID).
    Salary / stock / bonus appear in the second table — <b>NEO summary compensation</b> — after you backfill:
    <code>python -m wealth_leads sync --force</code>
    (or run <code>Sync SEC data then open dashboard.bat</code>), then refresh this page. Many small S-1s also have no parseable summary table.
  </div>"""


def _stats_banner(stats: dict) -> str:
    if stats.get("missing_db"):
        return """<div class="banner warn"><strong>No database file yet.</strong> Run sync once, then refresh this page.</div>"""
    nf, no, nc = stats["filings"], stats["officers"], stats["comp_rows"]
    latest = html.escape(str(stats.get("latest_filing_date") or "—"))
    mtime = html.escape(str(stats.get("db_file_modified") or "—"))
    return f"""<div class="banner">
    <strong>Local snapshot</strong>
    <span class="stats"><span>{nf} filings</span><span>{no} officer rows</span><span>{nc} comp rows</span></span>
    <span class="sub">Newest filing date in DB: <b>{latest}</b> · DB file updated: <b>{mtime}</b></span>
  </div>"""


def _page(leads: list[sqlite3.Row], comp: list[sqlite3.Row], stats: dict) -> str:
    banner = _stats_banner(stats)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>WealthPipeline — local dashboard</title>
  <style>
    :root {{ font-family: system-ui, sans-serif; background: #0f1419; color: #e7e9ea; }}
    body {{ margin: 0; padding: 1.25rem; max-width: 1280px; margin-inline: auto; }}
    h1 {{ font-size: 1.35rem; font-weight: 600; margin-top: 0; }}
    h2 {{ font-size: 1.05rem; margin-top: 2rem; margin-bottom: 0.5rem; }}
    p.meta {{ color: #8b98a5; font-size: 0.875rem; margin-bottom: 1rem; line-height: 1.45; }}
    .banner {{ background: #1a2634; border: 1px solid #38444d; border-radius: 8px; padding: 0.85rem 1rem; margin-bottom: 1rem; }}
    .banner.warn {{ border-color: #c44242; background: #2a1f1f; }}
    .banner .stats {{ display: flex; flex-wrap: wrap; gap: 0.75rem 1.25rem; margin: 0.5rem 0; font-size: 0.9rem; }}
    .banner .stats span {{ color: #8b98a5; }}
    .banner .stats span::before {{ content: "· "; color: #38444d; }}
    .banner .stats span:first-child::before {{ content: ""; }}
    .banner .sub {{ display: block; font-size: 0.8rem; color: #8b98a5; margin-top: 0.35rem; }}
    .callout {{ background: #2a2518; border: 1px solid #6b5a2a; border-radius: 8px; padding: 0.85rem 1rem; margin: 0 0 1rem 0; font-size: 0.875rem; line-height: 1.45; }}
    .callout strong {{ color: #f0d060; }}
    label.sr {{ display: block; font-size: 0.8rem; color: #8b98a5; margin-bottom: 0.35rem; }}
    #filter {{
      width: 100%; max-width: 28rem; padding: 0.45rem 0.6rem; border-radius: 6px;
      border: 1px solid #38444d; background: #0f1419; color: #e7e9ea; font: inherit;
    }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.8125rem; }}
    th, td {{ text-align: left; padding: 0.5rem 0.6rem; border-bottom: 1px solid #38444d; vertical-align: top; }}
    th {{ color: #8b98a5; font-weight: 600; }}
    td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    a {{ color: #1d9bf0; }}
    tr:hover td {{ background: #1a2228; }}
    tr.hidden {{ display: none; }}
    code {{ font-size: 0.8em; }}
  </style>
</head>
<body>
  <h1>WealthPipeline — local dashboard</h1>
  <p class="meta">
    This is <b>your copy</b> of the pipeline running on <b>your PC</b> (not a public website).
    It reads the same SQLite file the <code>sync</code> command fills. After you sync, press
    <b>F5</b> here to reload. GitHub only holds code; your leads live in the DB file below.
  </p>
  {banner}
  {_comp_missing_callout(stats)}
  <label class="sr" for="filter">Filter both tables</label>
  <input type="search" id="filter" placeholder="Type company or person name…" autocomplete="off"/>
  <p class="meta">Database: <code>{html.escape(database_path())}</code></p>
  {_leads_table(leads)}
  {_comp_table(comp)}
  <script>
  (function() {{
    var input = document.getElementById('filter');
    if (!input) return;
    input.addEventListener('input', function() {{
      var q = (input.value || '').toLowerCase().trim();
      document.querySelectorAll('table tbody tr').forEach(function(tr) {{
        if (!q) {{ tr.classList.remove('hidden'); return; }}
        tr.classList.toggle('hidden', (tr.textContent || '').toLowerCase().indexOf(q) < 0);
      }});
    }});
  }})();
  </script>
</body>
</html>"""


def _load_page_data() -> tuple[list[sqlite3.Row], list[sqlite3.Row], dict]:
    dbp = database_path()
    if not Path(dbp).is_file():
        return [], [], {"missing_db": True}

    mtime = datetime.fromtimestamp(Path(dbp).stat().st_mtime).strftime("%Y-%m-%d %H:%M")

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
        "latest_filing_date": latest,
        "db_file_modified": mtime,
    }
    return leads, comp, stats


def _app(environ, start_response):
    if environ.get("PATH_INFO", "/") not in ("/", ""):
        start_response("404 Not Found", [("Content-Type", "text/plain; charset=utf-8")])
        return [b"Not Found"]

    leads, comp, stats = _load_page_data()
    body = _page(leads, comp, stats).encode("utf-8")
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
