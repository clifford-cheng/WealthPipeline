from __future__ import annotations

import argparse
import csv
import sys

import requests

from wealth_leads.config import (
    database_path,
    email_smtp_verify_enabled,
    enrich_client_research_after_sync_enabled,
    enrich_client_research_after_sync_limit,
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
    replace_person_management_narratives,
    update_filing_director_term_summary,
    update_filing_issuer_industry,
    update_filing_issuer_meta,
    update_filing_issuer_revenue_text_if_empty,
    update_filing_issuer_summary,
    update_primary_doc_url,
)
from wealth_leads.territory import is_plausible_registrant_headquarters
from wealth_leads.management import (
    extract_executive_officers_from_filing_html,
    extract_issuer_headquarters_from_filing_html,
    extract_issuer_industry_from_filing_html,
    extract_issuer_revenue_line_from_filing_html,
    extract_issuer_summary_from_filing_html,
    extract_issuer_website_from_filing_html,
    merge_officer_rows,
)
from wealth_leads.management_bios import (
    extract_director_term_summary_from_filing_html,
    extract_management_biographies_from_filing_html,
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
            mgmt_b = extract_executive_officers_from_filing_html(s1_html)
            sig_b = extract_officers_from_s1_html(s1_html)
            replace_officers(c, fid, merge_officer_rows(mgmt_b, sig_b))
            summ_b = extract_issuer_summary_from_filing_html(s1_html)
            if summ_b:
                update_filing_issuer_summary(c, fid, summ_b)
            web_b = extract_issuer_website_from_filing_html(s1_html)
            hq_b = extract_issuer_headquarters_from_filing_html(s1_html)
            hq_ok = is_plausible_registrant_headquarters(hq_b) if hq_b else False
            if web_b or hq_ok:
                update_filing_issuer_meta(
                    c,
                    fid,
                    website=web_b,
                    headquarters=hq_b if hq_ok else "",
                )
            ind_b = extract_issuer_industry_from_filing_html(s1_html)
            if ind_b:
                update_filing_issuer_industry(c, fid, ind_b)
            rev_b = extract_issuer_revenue_line_from_filing_html(s1_html)
            if rev_b:
                update_filing_issuer_revenue_text_if_empty(c, fid, rev_b)
            bios_b = extract_management_biographies_from_filing_html(s1_html)
            replace_person_management_narratives(c, fid, bios_b)
            dts_b = extract_director_term_summary_from_filing_html(s1_html)
            update_filing_director_term_summary(c, fid, dts_b)
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


def refresh_issuer_hq_from_primary_docs() -> None:
    """
    Re-fetch every stored primary document and re-run website + HQ heuristics.
    Does not re-parse NEO or officers; use after upgrading extract_issuer_headquarters_from_filing_html.
    """
    session = requests.Session()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT id, primary_doc_url, company_name, accession, issuer_headquarters
            FROM filings
            WHERE primary_doc_url IS NOT NULL AND TRIM(primary_doc_url) != ''
            ORDER BY filing_date DESC
            """
        ).fetchall()
        total = len(rows)
        parsed = errors = 0
        for i, r in enumerate(rows):
            fid = int(r["id"])
            url = canonical_filing_document_url(r["primary_doc_url"])
            if url and url != r["primary_doc_url"]:
                update_primary_doc_url(conn, fid, url)
            if not url:
                continue
            try:
                html = get_text(url, session=session)
            except Exception as e:
                errors += 1
                nm = (r["company_name"] or r["accession"] or str(fid)).strip()
                print(f"[warn] refresh issuer HQ fetch {nm}: {e}", file=sys.stderr)
                continue
            web = extract_issuer_website_from_filing_html(html)
            hq = extract_issuer_headquarters_from_filing_html(html)
            hq_ok = is_plausible_registrant_headquarters(hq) if hq else False
            old_hq = (r["issuer_headquarters"] or "").strip()
            if hq_ok:
                update_filing_issuer_meta(conn, fid, website=web, headquarters=hq)
            else:
                if not is_plausible_registrant_headquarters(old_hq):
                    conn.execute(
                        """
                        UPDATE filings SET
                            issuer_headquarters = '',
                            issuer_hq_city_state = ''
                        WHERE id = ?
                        """,
                        (fid,),
                    )
                if web:
                    update_filing_issuer_meta(
                        conn, fid, website=web, headquarters=""
                    )
            parsed += 1
            if (i + 1) % 25 == 0 or (i + 1) == total:
                print(f"refresh-issuer-hq: {i + 1}/{total}…", file=sys.stderr)
        conn.commit()
    print(
        f"refresh-issuer-hq: done — parsed {parsed}, fetch errors {errors}, total rows {total}",
        file=sys.stderr,
    )


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

    web = extract_issuer_website_from_filing_html(body_html)
    hq = extract_issuer_headquarters_from_filing_html(body_html)
    hq_ok = is_plausible_registrant_headquarters(hq) if hq else False
    if web or hq_ok:
        update_filing_issuer_meta(
            conn, filing_id, website=web, headquarters=hq if hq_ok else ""
        )

    ind = extract_issuer_industry_from_filing_html(body_html)
    if ind:
        update_filing_issuer_industry(conn, filing_id, ind)

    rev_h = extract_issuer_revenue_line_from_filing_html(body_html)
    if rev_h:
        update_filing_issuer_revenue_text_if_empty(conn, filing_id, rev_h)

    bios = extract_management_biographies_from_filing_html(body_html)
    replace_person_management_narratives(conn, filing_id, bios)
    dts = extract_director_term_summary_from_filing_html(body_html)
    update_filing_director_term_summary(conn, filing_id, dts)

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

    from wealth_leads.profile_build import rebuild_lead_profiles

    with connect() as conn:
        pr_stats = rebuild_lead_profiles(conn)
    print(
        f"Lead profiles materialized: {pr_stats['rows_written']} rows "
        f"(from {pr_stats['profiles_source']} NEO profiles; "
        f"{pr_stats.get('cross_company_flagged', 0)} cross-CIK name hints).",
        file=sys.stderr,
    )

    if enrich_client_research_after_sync_enabled():
        from wealth_leads.lead_research import run_enrich_client_research

        lim = enrich_client_research_after_sync_limit()
        with connect() as conn:
            er = run_enrich_client_research(
                conn,
                limit=lim,
                force=False,
                use_llm=True,
                verify_smtp=email_smtp_verify_enabled(),
            )
        print(
            f"Website enrich after sync: enriched={er.get('enriched')} "
            f"skipped_ok={er.get('skipped_ok')} (cap {lim}; "
            f"set WEALTH_LEADS_ENRICH_WEB_AFTER_SYNC=0 to disable).",
            file=sys.stderr,
        )
        if er.get("errors"):
            for line in er["errors"][:5]:
                print(f"  [enrich] {line}", file=sys.stderr)


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
        "SEC_10K_PER_CIK=3, SEC_RSS_COUNT. Set SEC_SYNC_FORMS=S-1,10-K for a global 10-K RSS feed too. "
        "Lead desk: WEALTH_LEADS_LEAD_DESK_S1_ONLY=1 (default), "
        "WEALTH_LEADS_LEAD_DESK_MIN_SIGNAL_USD=300000 (max single-FY of SCT total vs stock+options; 0 disables). "
        "Legacy equity-only: WEALTH_LEADS_LEAD_DESK_MIN_EQUITY_USD. "
        "Optional website photos / LinkedIn hints after each sync: WEALTH_LEADS_ENRICH_WEB_AFTER_SYNC=1 "
        "and WEALTH_LEADS_ENRICH_WEB_AFTER_SYNC_LIMIT=12 (runs enrich-client-research; needs issuer site URL + "
        "OPENAI for best LLM extraction; or run `enrich-client-research` manually anytime)."
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
        help="Legacy lead desk at http://127.0.0.1:8766 (/, /lead, /finder; no /login — use serve_advisor for that)",
    )
    srv.add_argument(
        "--port",
        type=int,
        default=None,
        help="Port (default 8766 or WEALTH_LEADS_PORT; advisor app uses 8765 by default)",
    )
    srv.add_argument(
        "--no-browser",
        action="store_true",
        help="Do not open a browser tab automatically",
    )
    srv.add_argument(
        "--no-live",
        action="store_true",
        help="Disable automatic browser refresh when the DB file or server process changes",
    )
    srv.add_argument(
        "--reload",
        action="store_true",
        help="Dev: restart the server when wealth_leads Python files change (implies live refresh)",
    )

    app_cmd = sub.add_parser(
        "serve-app",
        help="Advisor app: sign-in, watchlist, CSV export (set WEALTH_LEADS_APP_SECRET)",
    )
    app_cmd.add_argument(
        "--host",
        default="127.0.0.1",
        help="Bind address (default 127.0.0.1; use 0.0.0.0 behind TLS reverse proxy)",
    )
    app_cmd.add_argument(
        "--port",
        type=int,
        default=None,
        help="Port (default WEALTH_LEADS_APP_PORT or 8080)",
    )

    alloc_cmd = sub.add_parser(
        "allocate",
        help="Run monthly territory lead assignment (writes lead_assignments; use after sync)",
    )
    alloc_cmd.add_argument(
        "--cycle",
        type=str,
        default=None,
        help="Billing cycle YYYYMM (default: current UTC month)",
    )
    alloc_cmd.add_argument(
        "--no-replace",
        action="store_true",
        help="Do not delete existing assignments for the cycle before assigning",
    )

    sub.add_parser(
        "rebuild-profiles",
        help="Refresh lead_profile table from NEO data (no SEC fetch; run after sync or parser changes)",
    )
    sub.add_parser(
        "refresh-issuer-hq",
        help="Re-fetch primary documents and refresh issuer website + headquarters heuristics (no NEO re-parse)",
    )

    erc = sub.add_parser(
        "enrich-client-research",
        help="Fetch leadership pages on issuer websites → bio, photo URL, LinkedIn hints (stores in lead_client_research)",
    )
    erc.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Max profiles to enrich (default 20, cap 500)",
    )
    erc.add_argument("--cik", type=str, default="", help="Only this CIK")
    erc.add_argument(
        "--force",
        action="store_true",
        help="Re-run even if status is already ok",
    )
    erc.add_argument(
        "--no-llm",
        action="store_true",
        help="Heuristics only (no LLM: no website summary, dossier story, or company snapshot during enrich)",
    )
    erc.add_argument(
        "--smtp",
        action="store_true",
        help="Run SMTP RCPT probes on guessed addresses (also WEALTH_LEADS_EMAIL_SMTP_VERIFY=1; outbound port 25 often blocked)",
    )
    erc.epilog = (
        "LLM (company snapshot, website card, executive story): same as enrich-s1-ai — "
        "WEALTH_LEADS_S1_AI_PROVIDER=ollama (or openai, anthropic), WEALTH_LEADS_OLLAMA_MODEL, "
        "WEALTH_LEADS_OLLAMA_URL. Downloads headshot bytes into SQLite when a photo URL is found. "
        "Requires non-empty issuer website on the filing/profile."
    )

    ai = sub.add_parser(
        "enrich-s1-ai",
        help="Use an LLM (OpenAI, Anthropic, or local Ollama) to extract NEO comp, officers, bios, HQ from S-1 HTML (not on sync)",
    )
    ai.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Max filings to process (default 5; cap 200)",
    )
    ai.add_argument(
        "--filing-id",
        type=int,
        default=None,
        help="Process a single filing by database id",
    )
    ai.add_argument(
        "--only-missing-neo",
        action="store_true",
        help="Only filings with zero neo_compensation rows",
    )
    ai.add_argument(
        "--issuer-refresh",
        action="store_true",
        help="Only filings with empty/short HQ or no s1_llm_lead_pack (use when NEO exists but address/AI never ran)",
    )
    ai.add_argument(
        "--replace-neo",
        action="store_true",
        help="Overwrite existing NEO rows when AI returns data (default: fill only if empty)",
    )
    ai.add_argument(
        "--replace-officers",
        action="store_true",
        help="Overwrite officers even if already parsed",
    )
    ai.add_argument(
        "--replace-bios",
        action="store_true",
        help="Overwrite management bios even if already stored",
    )
    ai.add_argument(
        "--allow-empty-neo",
        action="store_true",
        help="With --replace-neo: clear NEO table if AI returns no comp rows (dangerous)",
    )
    ai.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch HTML and report size; do not call the LLM",
    )
    ai.epilog = (
        "Provider: WEALTH_LEADS_S1_AI_PROVIDER=openai (default), anthropic, or ollama (alias: local). "
        "OpenAI: OPENAI_API_KEY; WEALTH_LEADS_S1_AI_MODEL (default gpt-4o-mini). "
        "Anthropic: ANTHROPIC_API_KEY; WEALTH_LEADS_ANTHROPIC_S1_MODEL. "
        "Ollama: run `ollama serve`, pull a model, set WEALTH_LEADS_OLLAMA_MODEL (default llama3.1); "
        "optional WEALTH_LEADS_OLLAMA_URL (default http://127.0.0.1:11434). "
        "Document excerpt: WEALTH_LEADS_S1_AI_DOCUMENT_MODE=windows (default), linear "
        "(read from start of plain text), or bookend (head+tail); "
        "WEALTH_LEADS_S1_AI_MAX_CHARS caps size (default 100000). "
        "Cloud APIs bill per token; local uses your RAM/GPU. Output includes lead_intel "
        "(offering, ownership, related parties, etc.) stored on the filing row and shown in the pipeline drawer. "
        "Then: py -m wealth_leads rebuild-profiles"
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

        run_localhost(
            port=args.port,
            open_browser=not args.no_browser,
            live=not bool(getattr(args, "no_live", False)),
            reload=bool(getattr(args, "reload", False)),
        )
    elif args.cmd == "serve-app":
        import uvicorn

        from wealth_leads.config import app_listen_port, uvicorn_reload_enabled

        port = getattr(args, "port", None) or app_listen_port()
        uvicorn.run(
            "wealth_leads.web_app:app",
            host=args.host,
            port=port,
            log_level="info",
            reload=uvicorn_reload_enabled(),
        )
    elif args.cmd == "allocate":
        from wealth_leads.allocation import run_allocation_from_db

        stats = run_allocation_from_db(
            cycle_yyyymm=getattr(args, "cycle", None),
            replace=not bool(getattr(args, "no_replace", False)),
        )
        print(stats, file=sys.stderr)
    elif args.cmd == "rebuild-profiles":
        from wealth_leads.profile_build import rebuild_lead_profiles

        with connect() as conn:
            st = rebuild_lead_profiles(conn)
        print(st, file=sys.stderr)
    elif args.cmd == "refresh-issuer-hq":
        refresh_issuer_hq_from_primary_docs()
    elif args.cmd == "enrich-client-research":
        from wealth_leads.lead_research import run_enrich_client_research

        with connect() as conn:
            st = run_enrich_client_research(
                conn,
                limit=int(getattr(args, "limit", 20) or 20),
                cik=(getattr(args, "cik", None) or "").strip() or None,
                force=bool(getattr(args, "force", False)),
                use_llm=not bool(getattr(args, "no_llm", False)),
                verify_smtp=bool(getattr(args, "smtp", False))
                or email_smtp_verify_enabled(),
            )
        print(st, file=sys.stderr)
    elif args.cmd == "enrich-s1-ai":
        from wealth_leads.s1_ai_extract import run_enrich_s1_ai

        n = run_enrich_s1_ai(
            limit=int(getattr(args, "limit", 5) or 5),
            filing_id=getattr(args, "filing_id", None),
            only_missing_neo=bool(getattr(args, "only_missing_neo", False)),
            issuer_refresh=bool(getattr(args, "issuer_refresh", False)),
            replace_neo=bool(getattr(args, "replace_neo", False)),
            replace_officers=bool(getattr(args, "replace_officers", False)),
            replace_bios=bool(getattr(args, "replace_bios", False)),
            allow_empty_neo=bool(getattr(args, "allow_empty_neo", False)),
            dry_run=bool(getattr(args, "dry_run", False)),
        )
        print(f"enrich-s1-ai finished ({n} filing(s) processed).", file=sys.stderr)


if __name__ == "__main__":
    main()
