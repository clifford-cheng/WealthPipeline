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

CREATE TABLE IF NOT EXISTS neo_compensation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filing_id INTEGER NOT NULL,
    person_name TEXT NOT NULL,
    role_hint TEXT,
    fiscal_year INTEGER NOT NULL,
    salary REAL,
    bonus REAL,
    stock_awards REAL,
    option_awards REAL,
    non_equity_incentive REAL,
    pension_change REAL,
    other_comp REAL,
    total REAL,
    equity_comp_disclosed REAL,
    source TEXT NOT NULL DEFAULT 'summary_compensation_table',
    FOREIGN KEY (filing_id) REFERENCES filings(id),
    UNIQUE(filing_id, person_name, fiscal_year)
);

CREATE INDEX IF NOT EXISTS idx_neo_comp_filing ON neo_compensation(filing_id);

CREATE TABLE IF NOT EXISTS person_management_narrative (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filing_id INTEGER NOT NULL,
    person_name TEXT NOT NULL,
    person_name_norm TEXT NOT NULL,
    role_heading TEXT,
    bio_text TEXT NOT NULL,
    FOREIGN KEY (filing_id) REFERENCES filings(id),
    UNIQUE(filing_id, person_name_norm)
);

CREATE INDEX IF NOT EXISTS idx_mgmt_narr_filing ON person_management_narrative(filing_id);
CREATE INDEX IF NOT EXISTS idx_mgmt_narr_norm ON person_management_narrative(person_name_norm);
"""


def _migrate_filings_compensation_column(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(filings)").fetchall()}
    if "compensation_extracted" not in cols:
        conn.execute(
            "ALTER TABLE filings ADD COLUMN compensation_extracted "
            "INTEGER NOT NULL DEFAULT 0"
        )


def _migrate_filings_issuer_summary(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(filings)").fetchall()}
    if "issuer_summary" not in cols:
        conn.execute("ALTER TABLE filings ADD COLUMN issuer_summary TEXT")


def _migrate_filings_issuer_meta(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(filings)").fetchall()}
    if "issuer_website" not in cols:
        conn.execute("ALTER TABLE filings ADD COLUMN issuer_website TEXT")
    if "issuer_headquarters" not in cols:
        conn.execute("ALTER TABLE filings ADD COLUMN issuer_headquarters TEXT")


def _migrate_officers_age(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(officers)").fetchall()}
    if "age" not in cols:
        conn.execute("ALTER TABLE officers ADD COLUMN age INTEGER")


def _migrate_filings_director_term(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(filings)").fetchall()}
    if "director_term_summary" not in cols:
        conn.execute("ALTER TABLE filings ADD COLUMN director_term_summary TEXT")


@contextmanager
def connect(path: Optional[str] = None) -> Generator[sqlite3.Connection, None, None]:
    dbp = path or database_path()
    Path(dbp).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(dbp)
    conn.row_factory = sqlite3.Row
    try:
        conn.executescript(SCHEMA)
        _migrate_filings_compensation_column(conn)
        _migrate_filings_issuer_summary(conn)
        _migrate_filings_issuer_meta(conn)
        _migrate_officers_age(conn)
        _migrate_filings_director_term(conn)
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


def update_filing_issuer_summary(
    conn: sqlite3.Connection, filing_id: int, text: str
) -> None:
    conn.execute(
        "UPDATE filings SET issuer_summary = ? WHERE id = ?",
        (text, filing_id),
    )


def update_filing_issuer_meta(
    conn: sqlite3.Connection,
    filing_id: int,
    *,
    website: str = "",
    headquarters: str = "",
) -> None:
    conn.execute(
        """
        UPDATE filings SET
            issuer_website = COALESCE(NULLIF(TRIM(?), ''), issuer_website),
            issuer_headquarters = COALESCE(NULLIF(TRIM(?), ''), issuer_headquarters)
        WHERE id = ?
        """,
        (website or "", headquarters or "", filing_id),
    )


def replace_person_management_narratives(
    conn: sqlite3.Connection, filing_id: int, rows: list[dict]
) -> None:
    conn.execute(
        "DELETE FROM person_management_narrative WHERE filing_id = ?", (filing_id,)
    )
    if not rows:
        return
    conn.executemany(
        """
        INSERT INTO person_management_narrative (
            filing_id, person_name, person_name_norm, role_heading, bio_text
        ) VALUES (?, ?, ?, ?, ?)
        """,
        [
            (
                filing_id,
                r["person_name"],
                r["person_name_norm"],
                r.get("role_heading") or "",
                r["bio_text"],
            )
            for r in rows
        ],
    )


def update_filing_director_term_summary(
    conn: sqlite3.Connection, filing_id: int, text: str
) -> None:
    if not (text or "").strip():
        return
    conn.execute(
        "UPDATE filings SET director_term_summary = ? WHERE id = ?",
        (text.strip(), filing_id),
    )


def replace_officers(
    conn: sqlite3.Connection, filing_id: int, rows: list[tuple]
) -> None:
    conn.execute("DELETE FROM officers WHERE filing_id = ?", (filing_id,))
    if rows:
        out: list[tuple] = []
        for row in rows:
            if len(row) == 3:
                n, t, s = row[0], row[1], row[2]
                out.append((filing_id, n, t, s, None))
            else:
                n, t, s, a = row[0], row[1], row[2], row[3]
                out.append((filing_id, n, t, s, a))
        conn.executemany(
            """
            INSERT INTO officers (filing_id, name, title, source, age)
            VALUES (?, ?, ?, ?, ?)
            """,
            out,
        )
    # Mark attempt complete so sync does not re-fetch forever; use --force-officers to retry.
    conn.execute(
        "UPDATE filings SET officers_extracted = 1 WHERE id = ?", (filing_id,)
    )


def replace_neo_compensation(
    conn: sqlite3.Connection,
    filing_id: int,
    rows: list[tuple],
) -> None:
    """rows: tuples matching insert order incl. equity_comp_disclosed."""
    conn.execute("DELETE FROM neo_compensation WHERE filing_id = ?", (filing_id,))
    if rows:
        conn.executemany(
            """
            INSERT INTO neo_compensation (
                filing_id, person_name, role_hint, fiscal_year,
                salary, bonus, stock_awards, option_awards,
                non_equity_incentive, pension_change, other_comp, total,
                equity_comp_disclosed, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    conn.execute(
        "UPDATE filings SET compensation_extracted = 1 WHERE id = ?", (filing_id,)
    )
