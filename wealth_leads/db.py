from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional

from wealth_leads.config import database_path

SCHEMA = """
CREATE TABLE IF NOT EXISTS filings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    accession TEXT NOT NULL UNIQUE,
    cik TEXT NOT NULL,
    company_name TEXT NOT NULL,
    form_type TEXT NOT NULL,
    filing_date TEXT,
    index_url TEXT NOT NULL,
    primary_doc_url TEXT,
    officers_extracted INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_filings_filing_date ON filings(filing_date);
CREATE INDEX IF NOT EXISTS idx_filings_cik ON filings(cik);

CREATE TABLE IF NOT EXISTS officers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filing_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    title TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'signature_table',
    FOREIGN KEY (filing_id) REFERENCES filings(id),
    UNIQUE(filing_id, name, title)
);

CREATE INDEX IF NOT EXISTS idx_officers_filing ON officers(filing_id);
"""


@contextmanager
def connect(path: Optional[str] = None) -> Generator[sqlite3.Connection, None, None]:
    dbp = path or database_path()
    Path(dbp).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(dbp)
    conn.row_factory = sqlite3.Row
    try:
        conn.executescript(SCHEMA)
        yield conn
        conn.commit()
    finally:
        conn.close()


def insert_filing(
    conn: sqlite3.Connection,
    *,
    accession: str,
    cik: str,
    company_name: str,
    form_type: str,
    filing_date: Optional[str],
    index_url: str,
    primary_doc_url: Optional[str] = None,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO filings (
            accession, cik, company_name, form_type,
            filing_date, index_url, primary_doc_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(accession) DO UPDATE SET
            form_type = excluded.form_type,
            filing_date = COALESCE(excluded.filing_date, filings.filing_date),
            primary_doc_url = COALESCE(excluded.primary_doc_url, filings.primary_doc_url)
        RETURNING id
        """,
        (
            accession,
            cik,
            company_name,
            form_type,
            filing_date,
            index_url,
            primary_doc_url,
        ),
    )
    row = cur.fetchone()
    assert row is not None
    return int(row[0])


def get_filing_by_accession(
    conn: sqlite3.Connection, accession: str
) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM filings WHERE accession = ?", (accession,)
    ).fetchone()


def update_primary_doc_url(
    conn: sqlite3.Connection, filing_id: int, url: str
) -> None:
    conn.execute(
        "UPDATE filings SET primary_doc_url = ? WHERE id = ?", (url, filing_id)
    )


def replace_officers(
    conn: sqlite3.Connection, filing_id: int, rows: list[tuple[str, str, str]]
) -> None:
    conn.execute("DELETE FROM officers WHERE filing_id = ?", (filing_id,))
    if rows:
        conn.executemany(
            """
            INSERT INTO officers (filing_id, name, title, source)
            VALUES (?, ?, ?, ?)
            """,
            [(filing_id, n, t, s) for n, t, s in rows],
        )
    # Mark attempt complete so sync does not re-fetch forever; use --force-officers to retry.
    conn.execute(
        "UPDATE filings SET officers_extracted = 1 WHERE id = ?", (filing_id,)
    )
