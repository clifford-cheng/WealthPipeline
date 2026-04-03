from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any, Optional

from wealth_leads.db import (
    get_filing_by_accession,
    get_issuer_sec_hints_row,
    upsert_issuer_sec_hints,
)
from wealth_leads.rss import RssFiling
from wealth_leads.sec_client import get_json

_10K_FORMS = frozenset({"10-K", "10-K/A", "FORM10-K", "FORM10-K/A"})


def submission_form_is_8k(raw_form: str) -> bool:
    """True for 8-K, 8-K/A, 8-K12B, etc. (submissions ``recent.form`` values)."""
    fn = (raw_form or "").strip().upper().replace(" ", "")
    return fn.startswith("8-K")


def archives_index_url(cik: str, accession: str) -> str:
    """Standard EDGAR index.htm URL for a filing."""
    cik_part = str(int(str(cik).strip()))
    acc_flat = accession.replace("-", "")
    return (
        f"https://www.sec.gov/Archives/edgar/data/{cik_part}/{acc_flat}/{accession}-index.htm"
    )


def fetch_company_submissions_json(
    cik: str, session: Any
) -> Optional[dict[str, Any]]:
    """``data.sec.gov/submissions/CIK##########.json`` for one issuer."""
    cik_stripped = str(cik).strip()
    try:
        cik10 = f"{int(cik_stripped):010d}"
    except ValueError:
        return None
    url = f"https://data.sec.gov/submissions/CIK{cik10}.json"
    try:
        return get_json(url, session=session)
    except Exception:
        return None


def _hints_refresh_due(updated_at: Optional[str], min_days: int) -> bool:
    if min_days <= 0:
        return True
    if not updated_at:
        return True
    try:
        raw = str(updated_at).replace("Z", "").strip()[:19]
        t = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return (now - t).total_seconds() >= float(min_days) * 86400.0
    except ValueError:
        return True


def _apply_submissions_hints(
    conn: sqlite3.Connection, cik_stripped: str, data: dict[str, Any]
) -> None:
    issuer = (data.get("name") or "").strip()
    tickers = data.get("tickers") or []
    if not isinstance(tickers, list):
        tickers = []
    exchanges = data.get("exchanges") or []
    if not isinstance(exchanges, list):
        exchanges = []
    try:
        upsert_issuer_sec_hints(
            conn,
            cik=cik_stripped,
            tickers=[str(x) for x in tickers],
            exchanges=[str(x) for x in exchanges],
            company_name=issuer,
        )
    except sqlite3.Error:
        pass


def refresh_issuer_sec_hints_if_stale(
    conn: sqlite3.Connection,
    cik: str,
    company_name: str,
    session: Any,
    *,
    min_days: int = 7,
) -> bool:
    """
    Fetch submissions JSON only to refresh ``issuer_sec_hints`` (tickers / exchanges).
    Returns True if a network fetch was attempted and hints were updated.
    """
    cik_stripped = str(cik).strip()
    try:
        int(cik_stripped)
    except ValueError:
        return False
    row = get_issuer_sec_hints_row(conn, cik_stripped)
    if row and not _hints_refresh_due(str(row["updated_at"] or ""), min_days):
        return False
    data = fetch_company_submissions_json(cik_stripped, session)
    if not data:
        return False
    _apply_submissions_hints(conn, cik_stripped, data)
    return True


def recent_submission_filings_for_cik(
    conn: sqlite3.Connection,
    cik: str,
    company_name: str,
    session: Any,
    *,
    max_10k: int = 0,
    max_8k: int = 0,
) -> list[RssFiling]:
    """
    One ``submissions/CIK##########.json`` fetch per issuer; collect the most recent
    10-K / 10-K/A and/or 8-K family rows not yet in ``filings``. Always refreshes
    ``issuer_sec_hints`` when JSON loads.
    """
    if max_10k <= 0 and max_8k <= 0:
        return []

    cik_stripped = str(cik).strip()
    try:
        int(cik_stripped)
    except ValueError:
        return []

    data = fetch_company_submissions_json(cik_stripped, session)
    if not data:
        return []

    _apply_submissions_hints(conn, cik_stripped, data)

    issuer = (data.get("name") or company_name or "").strip() or company_name
    recent = (data.get("filings") or {}).get("recent") or {}
    forms = recent.get("form") or []
    accs = recent.get("accessionNumber") or []
    dates = recent.get("filingDate") or []

    out: list[RssFiling] = []
    n_10k = 0
    n_8k = 0
    n = min(len(forms), len(accs), len(dates))
    for i in range(n):
        if n_10k >= max_10k and n_8k >= max_8k:
            break
        raw_form = (forms[i] or "").strip()
        fn = raw_form.upper().replace(" ", "")
        want = False
        if max_10k > 0 and n_10k < max_10k and fn in _10K_FORMS:
            want = True
        elif max_8k > 0 and n_8k < max_8k and submission_form_is_8k(raw_form):
            want = True
        if not want:
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
        if fn in _10K_FORMS:
            n_10k += 1
        else:
            n_8k += 1
    return out


def recent_10k_rss_filings_for_cik(
    conn: sqlite3.Connection,
    cik: str,
    company_name: str,
    session: Any,
    *,
    limit: int,
) -> list[RssFiling]:
    """Backward-compatible: 10-K / 10-K/A only (one submissions fetch)."""
    return recent_submission_filings_for_cik(
        conn, cik, company_name, session, max_10k=limit, max_8k=0
    )


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
