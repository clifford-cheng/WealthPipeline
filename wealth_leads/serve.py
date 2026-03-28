from __future__ import annotations

import html
import os
import sqlite3
import webbrowser
from pathlib import Path
from wsgiref.simple_server import make_server

from wealth_leads.config import database_path
from wealth_leads.db import connect


def _page(rows: list[sqlite3.Row]) -> str:
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

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>WealthPipeline — S-1 leads</title>
  <style>
    :root {{ font-family: system-ui, sans-serif; background: #0f1419; color: #e7e9ea; }}
    body {{ margin: 0; padding: 1.25rem; max-width: 1200px; margin-inline: auto; }}
    h1 {{ font-size: 1.25rem; font-weight: 600; margin-top: 0; }}
    p.meta {{ color: #8b98a5; font-size: 0.875rem; margin-bottom: 1rem; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.8125rem; }}
    th, td {{ text-align: left; padding: 0.5rem 0.6rem; border-bottom: 1px solid #38444d; vertical-align: top; }}
    th {{ color: #8b98a5; font-weight: 600; }}
    a {{ color: #1d9bf0; }}
    tr:hover td {{ background: #1a2228; }}
  </style>
</head>
<body>
  <h1>S-1 / S-1A leads</h1>
  <p class="meta">From local SQLite (<code>{html.escape(database_path())}</code>). Run <code>python -m wealth_leads sync</code> to refresh.</p>
  <table>
    <thead>
      <tr>
        <th>Company</th><th>CIK</th><th>Filed</th><th>Name</th><th>Title</th><th>EDGAR</th><th>Doc</th>
      </tr>
    </thead>
    <tbody>
      {''.join(body_rows) if body_rows else '<tr><td colspan="7">No rows yet. Run sync first.</td></tr>'}
    </tbody>
  </table>
</body>
</html>"""


def _fetch_rows() -> list[sqlite3.Row]:
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


def _app(environ, start_response):
    if environ.get("PATH_INFO", "/") not in ("/", ""):
        start_response("404 Not Found", [("Content-Type", "text/plain; charset=utf-8")])
        return [b"Not Found"]

    rows = _fetch_rows()
    body = _page(rows).encode("utf-8")
    start_response("200 OK", [("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(body)))])
    return [body]


def run_localhost(*, port: int | None = None, open_browser: bool = True) -> None:
    p = port or int(os.environ.get("WEALTH_LEADS_PORT", "8765"))
    url = f"http://127.0.0.1:{p}/"
    httpd = make_server("127.0.0.1", p, _app)
    print(f"WealthPipeline dashboard: {url}")
    print("Press Ctrl+C to stop.")
    if open_browser:
        webbrowser.open(url)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
