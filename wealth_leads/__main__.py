from __future__ import annotations

import argparse
import csv
import sys

import requests

from wealth_leads.config import (
    database_path,
    follow_10k_for_s1_ciks,
    submissions_10k_per_cik,
    sync_form_types,
)
from wealth_leads.compensation import NeoCompRow, extract_neo_compensation_from_s1
from wealth_leads.db import (
    connect,
    get_filing_by_accession,
    insert_filing,
    replace_neo_compensation,
    replace_officers,
    update_filing_issuer_summary,
    update_primary_doc_url,
)
from wealth_leads.management import (
    extract_executive_officers_from_filing_html,
    extract_issuer_summary_from_filing_html,
    merge_officer_rows,
)
from wealth_leads.officers import extract_officers_from_s1_html
from wealth_leads.parse_index import (
    canonical_filing_document_url,
    primary_document_url_for_form,
)
from wealth_leads.rss import RssFiling, fetch_current_feed
from wealth_leads.submissions import recent_10k_rss_filings_for_cik, s1_ciks_with_latest_name
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


def backfill_compensation(
    *, force: bool = False, conn=None, session=None
) -> int:
    """
    Fetch S-1 HTML again and parse NEO tables for filings missing comp rows
    (or all filings if force=True). Returns count of filings processed.
    """
    if session is None:
        session = requests.Session()

    n_done = 0

    def _work(c) -> None:
        nonlocal n_done
        q = """
            SELECT f.id, f.primary_doc_url, f.company_name, f.accession
            FROM filings f
            WHERE f.primary_doc_url IS NOT NULL
            """
        if not force:
            q += """
            AND (SELECT COUNT(*) FROM neo_compensation WHERE filing_id = f.id) = 0
            """
        q += " ORDER BY f.filing_date DESC"
        rows = c.execute(q).fetchall()
        if not rows:
            print("Backfill compensation: nothing to do.", file=sys.stderr)
            return
        print(
            f"Backfill compensation: processing {len(rows)} filing(s)…",
            file=sys.stderr,
        )
        for r in rows:
            fid = int(r["id"])
            raw_u = r["primary_doc_url"]
            url = canonical_filing_document_url(raw_u)
            if url and url != raw_u:
                update_primary_doc_url(c, fid, url)
            if not url:
                continue
            try:
                s1_html = get_text(url, session=session)
            except Exception as e:
                print(
                    f"[warn] backfill comp fetch {r['company_name']}: {e}",
                    file=sys.stderr,
                )
                continue
            if force:
                mgmt_b = extract_executive_officers_from_filing_html(s1_html)
                sig_b = extract_officers_from_s1_html(s1_html)
                replace_officers(c, fid, merge_officer_rows(mgmt_b, sig_b))
                summ_b = extract_issuer_summary_from_filing_html(s1_html)
                if summ_b:
                    update_filing_issuer_summary(c, fid, summ_b)
            comps = extract_neo_compensation_from_s1(s1_html)
            replace_neo_compensation(c, fid, _neo_comp_db_rows(fid, comps))
            n_done += 1
            if not comps:
                print(
                    f"[warn] backfill comp: no table parsed for {r['company_name']}",
                    file=sys.stderr,
                )

    if conn is not None:
        _work(conn)
    else:
        with connect() as c:
            _work(c)
    return n_done


def _process_rss_item(
    conn,
    item: RssFiling,
    session,
    *,
    force_reprocess: bool,
) -> None:
    existing = get_filing_by_accession(conn, item.accession)
    if (
        existing
        and existing["officers_extracted"]
        and existing["compensation_extracted"]
        and not force_reprocess
    ):
        return

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
    if doc_url:
        doc_url = canonical_filing_document_url(doc_url)
        if doc_url != row["primary_doc_url"]:
            update_primary_doc_url(conn, filing_id, doc_url)
    if not doc_url:
        idx_html = get_text(item.index_url, session=session)
        doc_url = primary_document_url_for_form(idx_html, item.form_type)
        if doc_url:
            update_primary_doc_url(conn, filing_id, doc_url)
        else:
            print(
                f"[warn] No primary doc in index ({item.form_type}): {item.company_name} "
                f"({item.accession})",
                file=sys.stderr,
            )
            return

    try:
        body_html = get_text(doc_url, session=session)
    except Exception as e:
        print(
            f"[warn] Could not fetch filing body {doc_url}: {e}",
            file=sys.stderr,
        )
        return

    mgmt_off = extract_executive_officers_from_filing_html(body_html)
    sig_off = extract_officers_from_s1_html(body_html)
    officers = merge_officer_rows(mgmt_off, sig_off)
    if not officers:
        print(
            f"[warn] No officers parsed: {item.company_name} ({item.accession})",
            file=sys.stderr,
        )
    replace_officers(conn, filing_id, officers)

    summ = extract_issuer_summary_from_filing_html(body_html)
    if summ:
        update_filing_issuer_summary(conn, filing_id, summ)

    comps = extract_neo_compensation_from_s1(body_html)
    replace_neo_compensation(conn, filing_id, _neo_comp_db_rows(filing_id, comps))
    if not comps:
        print(
            f"[warn] No NEO summary comp table: {item.company_name} "
            f"({item.accession})",
            file=sys.stderr,
        )


def sync(*, force_reprocess: bool = False) -> None:
    session = requests.Session()
    forms = sync_form_types()
    feed: list = []
    for ft in forms:
        batch = fetch_current_feed(session, form_type=ft)
        feed.extend(batch)
        print(f"RSS {ft}: {len(batch)} entr(y/ies)", file=sys.stderr)

    seen_acc: set[str] = set()
    deduped: list = []
    for it in feed:
        if it.accession in seen_acc:
            continue
        seen_acc.add(it.accession)
        deduped.append(it)
    feed = deduped

    with connect() as conn:
        for item in feed:
            _process_rss_item(conn, item, session, force_reprocess=force_reprocess)

        n_follow = 0
        per_10k = submissions_10k_per_cik()
        if follow_10k_for_s1_ciks() and per_10k > 0:
            cik_rows = s1_ciks_with_latest_name(conn)
            for cik, cname in cik_rows:
                for extra in recent_10k_rss_filings_for_cik(
                    conn, cik, cname, session, limit=per_10k
                ):
                    _process_rss_item(
                        conn, extra, session, force_reprocess=force_reprocess
                    )
                    n_follow += 1
            print(
                f"10-K follow (same CIKs as S-1): {n_follow} new filing(s) from "
                f"{len(cik_rows)} issuer(s), up to {per_10k} recent 10-K/A each",
                file=sys.stderr,
            )

        backfill_compensation(conn=conn, session=session, force=False)
        print(
            f"Processed {len(feed)} RSS entr(y/ies) across {len(forms)} form type(s) "
            f"(see {database_path()})."
        )


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
            "comp_fiscal_year",
            "comp_salary",
            "comp_bonus",
            "comp_stock_awards",
            "comp_total",
            "comp_equity_disclosed",
            "index_url",
            "primary_doc_url",
        ]
    )
    with connect() as conn:
        if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='neo_compensation'"
        ).fetchone():
            cur = conn.execute(
                """
                SELECT f.company_name, f.cik, f.accession, f.form_type, f.filing_date,
                       o.name, o.title,
                       NULL, NULL, NULL, NULL, NULL, NULL,
                       f.index_url, f.primary_doc_url
                FROM officers o
                JOIN filings f ON f.id = o.filing_id
                ORDER BY f.filing_date DESC, f.company_name, o.name
                """
            )
        else:
            cur = conn.execute(
                """
                SELECT f.company_name, f.cik, f.accession, f.form_type, f.filing_date,
                       o.name, o.title,
                       nc.fiscal_year, nc.salary, nc.bonus, nc.stock_awards,
                       nc.total, nc.equity_comp_disclosed,
                       f.index_url, f.primary_doc_url
                FROM officers o
                JOIN filings f ON f.id = o.filing_id
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
    p = argparse.ArgumentParser(
        description=(
            "SEC filing lead pipeline: S-1 RSS + 10-K cross-reference per CIK (submissions API); "
            "optional global 10-K RSS via SEC_SYNC_FORMS."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser(
        "sync",
        help="Pull EDGAR 'current' RSS (forms from SEC_SYNC_FORMS), store filings + officers + NEO comp",
    )
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
    s.epilog = (
        "Env: SEC_SYNC_FORMS (default S-1), SEC_FOLLOW_10K=1 (10-K per S-1 CIK via submissions), "
        "SEC_10K_PER_CIK=3, SEC_RSS_COUNT. Set SEC_SYNC_FORMS=S-1,10-K for a global 10-K RSS feed too."
    )

    sub.add_parser("export", help="Print leads as CSV to stdout")
    sub.add_parser(
        "export-comp",
        help="Print NEO compensation rows (from S-1 tables) as CSV",
    )
    bf = sub.add_parser(
        "backfill-comp",
        help="Re-fetch S-1 HTML and parse comp for filings missing NEO rows",
    )
    bf.add_argument(
        "--force",
        action="store_true",
        help="Re-parse compensation for every filing (slow; use after parser upgrades)",
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
    elif args.cmd == "backfill-comp":
        n = backfill_compensation(force=bool(getattr(args, "force", False)))
        print(f"Backfill finished ({n} filings processed).")
    elif args.cmd == "serve":
        from wealth_leads.serve import run_localhost

        run_localhost(port=args.port, open_browser=not args.no_browser)


if __name__ == "__main__":
    main()
