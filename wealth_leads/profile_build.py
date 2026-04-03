"""
Materialize `lead_profile` rows from in-memory profiles for review and export.

Sources: summary-comp (NEO) rows plus S-1 officer/director-only profiles when there is
no SCT line in DB (`lead_tier` = visibility). `lead_tier` also marks standard vs premium
for low-signal SCT.

Rebuild runs after SEC sync (or manually via Admin / CLI). Rows are filtered with the same
rules as the lead desk (by default, beneficial-only S-1 shareholders are omitted).
Cross-company hints flag the same normalized name appearing under multiple CIKs.
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from typing import Any

import sqlite3


def _neo_filing_map(
    conn: sqlite3.Connection, ciks: set[str]
) -> dict[tuple[str, str], list[int]]:
    if not ciks:
        return {}
    qm = ",".join("?" * len(ciks))
    cur = conn.execute(
        f"""
        SELECT f.cik AS cik, c.person_name AS person_name, c.filing_id AS filing_id
        FROM neo_compensation c
        JOIN filings f ON f.id = c.filing_id
        WHERE f.cik IN ({qm})
        """,
        tuple(ciks),
    )
    from wealth_leads.serve import _norm_person_name

    out: dict[tuple[str, str], set[int]] = defaultdict(set)
    for r in cur.fetchall():
        ck = str(r["cik"] or "").strip()
        pn = _norm_person_name(r["person_name"] or "")
        if ck and pn:
            out[(ck, pn)].add(int(r["filing_id"]))
    return {k: sorted(v) for k, v in out.items()}


def _latest_filing_row(
    conn: sqlite3.Connection, filing_ids: list[int]
) -> sqlite3.Row | None:
    if not filing_ids:
        return None
    qm = ",".join("?" * len(filing_ids))
    return conn.execute(
        f"""
        SELECT accession, filing_date, primary_doc_url, form_type, index_url
        FROM filings
        WHERE id IN ({qm})
        ORDER BY COALESCE(filing_date, '') DESC, id DESC
        LIMIT 1
        """,
        tuple(filing_ids),
    ).fetchone()


def _neo_llm_assisted_for_person(
    conn: sqlite3.Connection, filing_ids: list[int], person_norm: str
) -> bool:
    """True if any NEO row for this person on these filings came from LLM extraction."""
    from wealth_leads.serve import _norm_person_name

    if not filing_ids:
        return False
    qm = ",".join("?" * len(filing_ids))
    cur = conn.execute(
        f"""
        SELECT c.source, c.person_name FROM neo_compensation c
        WHERE c.filing_id IN ({qm})
        """,
        tuple(filing_ids),
    )
    for r in cur.fetchall():
        if _norm_person_name(r["person_name"] or "") != person_norm:
            continue
        s = (r["source"] or "").lower()
        if "llm_s1_extract" in s or "openai_s1_extract" in s:
            return True
    return False


def _neo_row_count(conn: sqlite3.Connection, cik: str, person_norm: str) -> int:
    from wealth_leads.serve import _norm_person_name

    cur = conn.execute(
        """
        SELECT c.person_name
        FROM neo_compensation c
        JOIN filings f ON f.id = c.filing_id
        WHERE f.cik = ?
        """,
        (cik.strip(),),
    )
    n = 0
    for r in cur.fetchall():
        if _norm_person_name(r["person_name"] or "") == person_norm:
            n += 1
    return n


def _opt_float(v: object) -> object:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _imputed_other_from_total(p: dict) -> object:
    """When SCT 'All other' cell is missing, derive from reported total minus other components."""
    t = _opt_float(p.get("total"))
    if t is None:
        return None
    s = 0.0
    for k in (
        "salary",
        "bonus",
        "stock_awards",
        "option_awards",
        "non_equity_incentive",
        "pension_change",
    ):
        v = _opt_float(p.get(k))
        if v is not None:
            s += float(v)
    r = float(t) - s
    if r < -2.0:
        return None
    return max(0.0, r)


def _effective_other_comp(p: dict) -> object:
    direct = _opt_float(p.get("other_comp"))
    if direct is not None:
        return direct
    return _imputed_other_from_total(p)


def _materialized_search_text(
    *,
    display_name: str,
    title: str,
    company_name: str,
    person_norm: str,
    cik: str,
    issuer_headquarters: str,
    issuer_industry: str,
    why_surfaced: str,
    issuer_summary_excerpt: str,
) -> str:
    parts = [
        display_name,
        title,
        company_name,
        (person_norm or "").replace("_", " "),
        cik,
        issuer_headquarters,
        issuer_industry,
        why_surfaced,
        (issuer_summary_excerpt or "")[:320],
    ]
    return " ".join(p.lower() for p in parts if (p or "").strip())


def _headline_comp_columns(p: dict) -> tuple[object, object, object, object]:
    """
    Headline FY from SCT: salary, bonus, other compensation, and sum of stock + option awards.
    Beneficial-owner profiles use the stock column for disclosed pre-IPO stake value (no SCT FY).
    """
    if p.get("has_s1_beneficial_owner"):
        stk = _opt_float(p.get("signal_hwm"))
        if stk is None:
            stk = _opt_float(p.get("equity_hwm"))
        return None, None, None, stk
    if not p.get("has_summary_comp") or p.get("headline_year") is None:
        return None, None, None, None
    sal = _opt_float(p.get("salary"))
    bonus = _opt_float(p.get("bonus"))
    other = _effective_other_comp(p)
    grants = 0.0
    any_g = False
    for x in (p.get("stock_awards"), p.get("option_awards")):
        if x is not None:
            try:
                grants += float(x)
                any_g = True
            except (TypeError, ValueError):
                pass
    stk: object = grants if any_g else None
    return sal, bonus, other, stk


def rebuild_lead_profiles(conn: sqlite3.Connection) -> dict[str, Any]:
    from wealth_leads.crm_ui import format_headquarters_for_ui
    from wealth_leads.serve import _build_profiles, _lead_desk_filter_profiles
    from wealth_leads.territory import hq_city_state_pipeline_only, hq_has_registrant_address_detail

    profiles = _lead_desk_filter_profiles(_build_profiles(conn), conn)
    if not profiles:
        conn.execute("DELETE FROM lead_profile")
        return {"profiles_source": 0, "rows_written": 0}

    norm_ciks: dict[str, set[str]] = defaultdict(set)
    for p in profiles:
        nn = (p.get("norm_name") or "").strip()
        ck = str(p.get("cik") or "").strip()
        if nn and ck:
            norm_ciks[nn].add(ck)

    ciks = {str(p.get("cik") or "").strip() for p in profiles if p.get("cik")}
    fmap = _neo_filing_map(conn, ciks)
    rows: list[tuple] = []
    built_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    for p in profiles:
        cik = str(p.get("cik") or "").strip()
        pn = (p.get("norm_name") or "").strip()
        if not cik or not pn:
            continue
        fids = list(p.get("source_filing_ids") or [])
        if not fids:
            fids = list(fmap.get((cik, pn), []))
        lf = _latest_filing_row(conn, fids)
        acc = (lf["accession"] if lf else "") or ""
        fd_latest = (lf["filing_date"] if lf else "") or (p.get("filing_date") or "")
        pdoc = (lf["primary_doc_url"] if lf else "") or (p.get("primary_doc_url") or "")
        fform = (lf["form_type"] if lf else "") or (p.get("filing_form_type") or "")
        idx = (lf["index_url"] if lf else "") or (p.get("index_url") or "")

        nc = len(fids) if fids else _neo_row_count(conn, cik, pn)
        cross = 1 if len(norm_ciks.get(pn, set())) > 1 else 0
        others = sorted(norm_ciks.get(pn, set()) - {cik})

        has_bio = 1 if (p.get("mgmt_bio_text") or "").strip() else 0
        has_off = 1 if (
            p.get("officer_age_from_table") is not None
            or (
                p.get("has_s1_officer")
                and (p.get("title") or "").strip()
                and (p.get("title") or "").strip() != "—"
            )
        ) else 0

        summ = (p.get("issuer_summary") or "").strip()
        summ_ex = (summ[:600] + "…") if len(summ) > 600 else summ
        why_s = (p.get("why_surfaced") or "")[:500]
        search_blob = _materialized_search_text(
            display_name=(p.get("display_name") or "")[:400],
            title=(p.get("title") or "")[:400],
            company_name=(p.get("company_name") or "")[:400],
            person_norm=pn,
            cik=cik,
            issuer_headquarters=(p.get("issuer_headquarters") or "")[:500],
            issuer_industry=(p.get("issuer_industry") or "")[:500],
            why_surfaced=why_s,
            issuer_summary_excerpt=summ_ex,
        )[:4000]
        llm_comp = 1 if _neo_llm_assisted_for_person(conn, fids, pn) else 0
        sal_h, bonus_h, other_h, stk_h = _headline_comp_columns(p)
        tot_h = (
            _opt_float(p.get("total"))
            if p.get("has_summary_comp") and p.get("headline_year") is not None
            else None
        )

        hq_line = format_headquarters_for_ui((p.get("issuer_headquarters") or ""))[:500]
        hq_cs = (hq_city_state_pipeline_only(hq_line)[:120] if hq_line else "") or ""
        hq_detail = 1 if (hq_line and hq_has_registrant_address_detail(hq_line)) else 0

        rows.append(
            (
                cik,
                pn,
                (p.get("display_name") or "")[:400],
                (p.get("title") or "")[:400],
                (p.get("company_name") or "")[:400],
                fd_latest[:32],
                acc[:32],
                (pdoc or "")[:2000],
                (fform or "")[:32],
                (p.get("issuer_headquarters") or "")[:500],
                (p.get("issuer_industry") or "")[:500],
                (p.get("issuer_website") or "")[:500],
                (idx or "")[:2000],
                p.get("equity_hwm"),
                p.get("total_hwm"),
                p.get("signal_hwm"),
                int(p["headline_year"])
                if p.get("headline_year") is not None
                else None,
                tot_h,
                sal_h,
                bonus_h,
                other_h,
                stk_h,
                1 if p.get("has_s1_comp") else 0,
                has_bio,
                has_off,
                int(nc),
                (p.get("comp_timeline") or "")[:500],
                summ_ex,
                why_s,
                json.dumps(fids),
                cross,
                json.dumps(others),
                llm_comp,
                (p.get("lead_tier") or "premium")[:24],
                hq_cs,
                hq_detail,
                (p.get("issuer_listing_stage") or "unknown")[:24],
                1 if p.get("has_s1_beneficial_owner") else 0,
                (p.get("issuer_risk_level") or "none")[:24],
                json.dumps(p.get("issuer_risk_reasons") or []),
                (p.get("issuer_revenue_text") or "").strip()[:500],
                search_blob,
                built_at,
            )
        )

    conn.execute("DELETE FROM lead_profile")
    conn.executemany(
        """
        INSERT INTO lead_profile (
            cik, person_norm, display_name, title, company_name,
            filing_date_latest, accession_latest, primary_doc_url, form_type_latest,
            issuer_headquarters, issuer_industry, issuer_website, index_url,
            equity_hwm, total_hwm, signal_hwm, headline_year,
            total_headline,
            salary_headline, bonus_headline, other_comp_headline, stock_grants_headline,
            has_s1_comp, has_mgmt_bio, has_officer_row, neo_row_count,
            comp_timeline, issuer_summary_excerpt, why_surfaced,
            neo_filing_ids_json, cross_company_hint, other_ciks_json,
            comp_llm_assisted, lead_tier, issuer_hq_city_state, issuer_hq_has_detail,
            issuer_listing_stage, has_beneficial_owner_stake,
            issuer_risk_level, issuer_risk_reasons_json, issuer_revenue_text, search_text, built_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        rows,
    )
    from wealth_leads.db import rebuild_lead_profile_fts_index

    rebuild_lead_profile_fts_index(conn)
    from wealth_leads.lead_research import materialize_email_outreach_for_profiles

    eo = materialize_email_outreach_for_profiles(conn)
    return {
        "profiles_source": len(profiles),
        "rows_written": len(rows),
        "cross_company_flagged": sum(1 for r in rows if r[30] == 1),
        **eo,
    }
