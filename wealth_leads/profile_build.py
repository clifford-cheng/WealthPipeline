"""
Materialize `lead_profile` rows from in-memory profiles for review and export.

Sources: summary-comp (NEO) rows plus S-1 officer/director-only profiles when there is
no SCT line in DB (`lead_tier` = visibility). `lead_tier` also marks standard vs premium
for low-signal SCT.

Rebuild runs after SEC sync (or manually via Admin / CLI). Cross-company hints flag the
same normalized name appearing under multiple CIKs (possible repeat IPO / board moves).
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


def rebuild_lead_profiles(conn: sqlite3.Connection) -> dict[str, Any]:
    from wealth_leads.serve import _build_profiles

    profiles = _build_profiles(conn)
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
                (p.get("title") or "").strip()
                and (p.get("title") or "").strip() != "—"
            )
        ) else 0

        q = 0  # reserved; pipeline sorts by filing_date_latest
        summ = (p.get("issuer_summary") or "").strip()
        summ_ex = (summ[:600] + "…") if len(summ) > 600 else summ
        llm_comp = 1 if _neo_llm_assisted_for_person(conn, fids, pn) else 0

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
                1 if p.get("has_s1_comp") else 0,
                has_bio,
                has_off,
                int(nc),
                (p.get("comp_timeline") or "")[:500],
                summ_ex,
                (p.get("why_surfaced") or "")[:500],
                json.dumps(fids),
                int(q),
                cross,
                json.dumps(others),
                llm_comp,
                (p.get("lead_tier") or "premium")[:24],
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
            has_s1_comp, has_mgmt_bio, has_officer_row, neo_row_count,
            comp_timeline, issuer_summary_excerpt, why_surfaced,
            neo_filing_ids_json, quality_score, cross_company_hint, other_ciks_json,
            comp_llm_assisted, lead_tier, built_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        rows,
    )
    return {
        "profiles_source": len(profiles),
        "rows_written": len(rows),
        "cross_company_flagged": sum(1 for r in rows if r[26] == 1),
    }
