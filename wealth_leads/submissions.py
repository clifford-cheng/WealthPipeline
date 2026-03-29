from __future__ import annotations

import sqlite3

from wealth_leads.db import get_filing_by_accession
from wealth_leads.rss import RssFiling
from wealth_leads.sec_client import get_json

_10K_FORMS = frozenset({"10-K", "10-K/A", "FORM10-K", "FORM10-K/A"})


def archives_index_url(cik: str, accession: str) -> str:
    """Standard EDGAR index.htm URL for a filing."""
    cik_part = str(int(str(cik).strip()))
    acc_flat = accession.replace("-", "")
    return (
        f"https://www.sec.gov/Archives/edgar/data/{cik_part}/{acc_flat}/{accession}-index.htm"
    )


def recent_10k_rss_filings_for_cik(
    conn: sqlite3.Connection,
    cik: str,
    company_name: str,
    session,
    *,
    limit: int,
) -> list[RssFiling]:
    """
    Use data.sec.gov submissions JSON to find the most recent 10-K / 10-K/A
    filings for one issuer, skipping accessions already in `filings`.
    """
    if limit <= 0:
        return []

    cik_stripped = str(cik).strip()
    try:
        cik10 = f"{int(cik_stripped):010d}"
    except ValueError:
        return []

    url = f"https://data.sec.gov/submissions/CIK{cik10}.json"
    try:
        data = get_json(url, session=session)
    except Exception:
        return []

    issuer = (data.get("name") or company_name or "").strip() or company_name
    recent = (data.get("filings") or {}).get("recent") or {}
    forms = recent.get("form") or []
    accs = recent.get("accessionNumber") or []
    dates = recent.get("filingDate") or []

    out: list[RssFiling] = []
    n = min(len(forms), len(accs), len(dates))
    for i in range(n):
        if len(out) >= limit:
            break
        raw_form = (forms[i] or "").strip()
        fn = raw_form.upper().replace(" ", "")
        if fn not in _10K_FORMS:
            continue
        acc = (accs[i] or "").strip()
        if not acc:
            continue
        if get_filing_by_accession(conn, acc) is not None:
            continue
        filed = (dates[i] or "").strip() or None
        cik_out = str(int(cik_stripped))
        out.append(
            RssFiling(
                accession=acc,
                cik=cik_out,
                company_name=issuer,
                form_type=raw_form or fn,
                filing_date=filed,
                index_url=archives_index_url(cik_stripped, acc),
            )
        )
    return out


def s1_ciks_with_latest_name(conn: sqlite3.Connection) -> list[tuple[str, str]]:
    """Distinct CIKs that appear on any S-1 / S-1/A filing, with a recent company name."""
    ciks = [
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT cik FROM filings WHERE form_type LIKE '%S-1%'"
        ).fetchall()
    ]
    out: list[tuple[str, str]] = []
    for cik in ciks:
        row = conn.execute(
            """
            SELECT company_name FROM filings
            WHERE cik = ?
            ORDER BY COALESCE(filing_date, '') DESC, id DESC
            LIMIT 1
            """,
            (cik,),
        ).fetchone()
        name = (row[0] if row else "") or ""
        out.append((str(cik).strip(), name))
    return out
