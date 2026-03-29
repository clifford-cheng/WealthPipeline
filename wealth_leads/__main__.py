from __future__ import annotations

import argparse
import csv
import sys

import requests

from wealth_leads.config import database_path
from wealth_leads.compensation import NeoCompRow, extract_neo_compensation_from_s1
from wealth_leads.db import (
    connect,
    get_filing_by_accession,
    insert_filing,
    replace_neo_compensation,
    replace_officers,
    update_primary_doc_url,
)
from wealth_leads.officers import extract_officers_from_s1_html
from wealth_leads.parse_index import primary_s1_document_url
from wealth_leads.rss import fetch_current_s1_feed
from wealth_leads.sec_client import get_text


def _neo_comp_db_rows(
    filing_id: int, comps: list[NeoCompRow]
) -> list[tuple]:
    out: list[tuple] = []
    for c in comps:
        if c.stock_awards is not None or c.option_awards is not None:
            eq = 0.0
            if c.stock_awards is not None:
                eq += c.stock_awards
            if c.option_awards is not None:
                eq += c.option_awards
        else:
            eq = None
        out.append(
            (
                filing_id,
                c.person_name,
                c.role_hint,
                c.fiscal_year,
                c.salary,
                c.bonus,
                c.stock_awards,
                c.option_awards,
                c.non_equity_incentive,
                c.pension_change,
                c.other_comp,
                c.total,
                eq,
                "summary_compensation_table",
            )
        )
    return out


def sync(*, force_reprocess: bool = False) -> None:
    session = requests.Session()
    feed = fetch_current_s1_feed(session)
    with connect() as conn:
        for item in feed:
            existing = get_filing_by_accession(conn, item.accession)
            if (
                existing
                and existing["officers_extracted"]
                and existing["compensation_extracted"]
                and not force_reprocess
            ):
                continue

            filing_id = insert_filing(
                conn,
                accession=item.accession,
                cik=item.cik,
                company_name=item.company_name,
                form_type=item.form_type,
                filing_date=item.filing_date,
                index_url=item.index_url,
                primary_doc_url=existing["primary_doc_url"]
                if existing and existing["primary_doc_url"]
                else None,
            )

            row = get_filing_by_accession(conn, item.accession)
            assert row is not None
            doc_url = row["primary_doc_url"]
            if not doc_url:
                idx_html = get_text(item.index_url, session=session)
                doc_url = primary_s1_document_url(idx_html)
                if doc_url:
                    update_primary_doc_url(conn, filing_id, doc_url)
                else:
                    print(
                        f"[warn] No primary S-1 doc in index: {item.company_name} "
                        f"({item.accession})",
                        file=sys.stderr,
                    )
                    continue

            try:
                s1_html = get_text(doc_url, session=session)
            except Exception as e:
                print(
                    f"[warn] Could not fetch S-1 body {doc_url}: {e}",
                    file=sys.stderr,
                )
                continue

            officers = extract_officers_from_s1_html(s1_html)
            if not officers:
                print(
                    f"[warn] No officers parsed: {item.company_name} ({item.accession})",
                    file=sys.stderr,
                )
            replace_officers(conn, filing_id, officers)

            comps = extract_neo_compensation_from_s1(s1_html)
            replace_neo_compensation(
                conn, filing_id, _neo_comp_db_rows(filing_id, comps)
            )
            if not comps:
                print(
                    f"[warn] No NEO summary comp table: {item.company_name} "
                    f"({item.accession})",
                    file=sys.stderr,
                )
        print(f"Processed {len(feed)} RSS entries (see {database_path()}).")


def export_leads_csv() -> None:
    writer = csv.writer(sys.stdout)
    writer.writerow(
        [
            "company_name",
            "cik",
            "accession",
            "form_type",
            "filing_date",
            "officer_name",
            "officer_title",
            "index_url",
            "primary_doc_url",
        ]
    )
    with connect() as conn:
        cur = conn.execute(
            """
            SELECT f.company_name, f.cik, f.accession, f.form_type, f.filing_date,
                   o.name, o.title, f.index_url, f.primary_doc_url
            FROM officers o
            JOIN filings f ON f.id = o.filing_id
            ORDER BY f.filing_date DESC, f.company_name, o.name
            """
        )
        for r in cur:
            writer.writerow(list(r))


def export_compensation_csv() -> None:
    writer = csv.writer(sys.stdout)
    writer.writerow(
        [
            "company_name",
            "cik",
            "accession",
            "filing_date",
            "person_name",
            "role_hint",
            "fiscal_year",
            "salary",
            "bonus",
            "stock_awards",
            "option_awards",
            "non_equity_incentive",
            "pension_change",
            "other_comp",
            "total",
            "equity_comp_disclosed",
            "primary_doc_url",
        ]
    )
    with connect() as conn:
        cur = conn.execute(
            """
            SELECT f.company_name, f.cik, f.accession, f.filing_date,
                   c.person_name, c.role_hint, c.fiscal_year,
                   c.salary, c.bonus, c.stock_awards, c.option_awards,
                   c.non_equity_incentive, c.pension_change, c.other_comp,
                   c.total, c.equity_comp_disclosed, f.primary_doc_url
            FROM neo_compensation c
            JOIN filings f ON f.id = c.filing_id
            ORDER BY f.filing_date DESC, f.company_name, c.person_name, c.fiscal_year DESC
            """
        )
        for r in cur:
            writer.writerow(list(r))


def main() -> None:
    p = argparse.ArgumentParser(description="SEC S-1 lead pipeline (MVP)")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("sync", help="Pull EDGAR RSS, store filings + officers")
    s.add_argument(
        "--force-officers",
        action="store_true",
        help="(Deprecated alias for --force.)",
    )
    s.add_argument(
        "--force",
        action="store_true",
        help="Re-process filings even if officers + compensation already stored",
    )

    sub.add_parser("export", help="Print leads as CSV to stdout")
    sub.add_parser(
        "export-comp",
        help="Print NEO compensation rows (from S-1 tables) as CSV",
    )

    srv = sub.add_parser(
        "serve",
        help="Open dashboard at http://127.0.0.1:8765 (run sync first for data)",
    )
    srv.add_argument(
        "--port",
        type=int,
        default=None,
        help="Port (default 8765 or WEALTH_LEADS_PORT)",
    )
    srv.add_argument(
        "--no-browser",
        action="store_true",
        help="Do not open a browser tab automatically",
    )

    args = p.parse_args()
    if args.cmd == "sync":
        sync(
            force_reprocess=bool(
                getattr(args, "force", False) or getattr(args, "force_officers", False)
            )
        )
    elif args.cmd == "export":
        export_leads_csv()
    elif args.cmd == "export-comp":
        export_compensation_csv()
    elif args.cmd == "serve":
        from wealth_leads.serve import run_localhost

        run_localhost(port=args.port, open_browser=not args.no_browser)


if __name__ == "__main__":
    main()
