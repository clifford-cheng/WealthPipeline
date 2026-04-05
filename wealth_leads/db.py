from __future__ import annotations

import json
import re
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional

from wealth_leads.config import database_path

# Windows + uvicorn --reload can briefly open two processes on the same file; default timeout is brittle.
_SQLITE_CONNECT_TIMEOUT_SEC = 60.0
_SQLITE_BUSY_TIMEOUT_MS = 60_000
# Full open + migrations retry: refresh storms + background sync can still briefly lock.
_SQLITE_CONNECT_ATTEMPTS = 15
_SQLITE_CONNECT_RETRY_BASE_SEC = 0.1


def _sqlite_locked_exc(e: BaseException) -> bool:
    if not isinstance(e, sqlite3.OperationalError):
        return False
    msg = str(e).lower()
    return "locked" in msg or "busy" in msg


def _prepare_sqlite_connection(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except sqlite3.Error:
        pass
    try:
        conn.execute(f"PRAGMA busy_timeout = {_SQLITE_BUSY_TIMEOUT_MS}")
    except sqlite3.Error:
        pass
    conn.executescript(SCHEMA)
    _migrate_filings_compensation_column(conn)
    _migrate_filings_issuer_summary(conn)
    _migrate_filings_issuer_meta(conn)
    _migrate_filings_issuer_hq_city_state(conn)
    _migrate_officers_age(conn)
    _migrate_filings_director_term(conn)
    _migrate_filings_issuer_industry(conn)
    _migrate_filings_s1_llm_lead_pack(conn)
    _migrate_filings_issuer_scale_text(conn)
    _migrate_app_auth(conn)
    _migrate_allocation_system(conn)
    _migrate_lead_profile(conn)
    _migrate_lead_profile_llm_flag(conn)
    _migrate_lead_profile_lead_tier(conn)
    _migrate_lead_profile_headline_comp(conn)
    _migrate_lead_profile_hq_materialized(conn)
    _migrate_lead_profile_issuer_listing_stage(conn)
    _migrate_lead_profile_beneficial_flag(conn)
    _migrate_lead_profile_issuer_risk(conn)
    _migrate_lead_profile_drop_quality_index(conn)
    _migrate_lead_profile_search_text(conn)
    _migrate_lead_profile_fts5(conn)
    _migrate_lead_profile_issuer_revenue_text(conn)
    _migrate_lead_suppress(conn)
    _migrate_issuer_sec_hints(conn)
    _migrate_lead_advisor_feedback(conn)
    _migrate_advisor_lead_outreach(conn)
    _migrate_lead_client_research(conn)
    _migrate_issuer_advisor_snapshot(conn)
    _migrate_lead_client_research_advisor_cols(conn)
    _migrate_lead_client_research_photo_blob(conn)
    _migrate_lead_client_research_filing_narrative_bullets(conn)
    _migrate_beneficial_owner_stake(conn)

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


def _migrate_filings_issuer_hq_city_state(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(filings)").fetchall()}
    if "issuer_hq_city_state" not in cols:
        conn.execute(
            "ALTER TABLE filings ADD COLUMN issuer_hq_city_state TEXT NOT NULL DEFAULT ''"
        )
        from wealth_leads.territory import issuer_hq_city_state_for_ui

        for row in conn.execute("SELECT id, issuer_headquarters FROM filings"):
            hid = int(row["id"])
            hq = (row["issuer_headquarters"] or "").strip()
            cs = issuer_hq_city_state_for_ui(hq) if hq else ""
            conn.execute(
                "UPDATE filings SET issuer_hq_city_state = ? WHERE id = ?",
                (cs, hid),
            )


def _migrate_officers_age(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(officers)").fetchall()}
    if "age" not in cols:
        conn.execute("ALTER TABLE officers ADD COLUMN age INTEGER")


def _migrate_filings_director_term(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(filings)").fetchall()}
    if "director_term_summary" not in cols:
        conn.execute("ALTER TABLE filings ADD COLUMN director_term_summary TEXT")


def _migrate_filings_issuer_industry(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(filings)").fetchall()}
    if "issuer_industry" not in cols:
        conn.execute("ALTER TABLE filings ADD COLUMN issuer_industry TEXT")


def _migrate_filings_s1_llm_lead_pack(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(filings)").fetchall()}
    if "s1_llm_lead_pack" not in cols:
        conn.execute("ALTER TABLE filings ADD COLUMN s1_llm_lead_pack TEXT")


def _migrate_filings_issuer_scale_text(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(filings)").fetchall()}
    if "issuer_revenue_text" not in cols:
        conn.execute("ALTER TABLE filings ADD COLUMN issuer_revenue_text TEXT")
    if "issuer_employees_text" not in cols:
        conn.execute("ALTER TABLE filings ADD COLUMN issuer_employees_text TEXT")


def _migrate_app_auth(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE COLLATE NOCASE,
            password_hash TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_watchlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            cik TEXT NOT NULL,
            person_norm TEXT NOT NULL DEFAULT '',
            label TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(user_id, cik, person_norm),
            FOREIGN KEY (user_id) REFERENCES app_users(id)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_watchlist_user ON user_watchlist(user_id)"
    )


@contextmanager
def connect(path: Optional[str] = None) -> Generator[sqlite3.Connection, None, None]:
    dbp = path or database_path()
    Path(dbp).parent.mkdir(parents=True, exist_ok=True)
    conn: Optional[sqlite3.Connection] = None
    last_exc: Optional[BaseException] = None
    for attempt in range(_SQLITE_CONNECT_ATTEMPTS):
        c: Optional[sqlite3.Connection] = None
        try:
            c = sqlite3.connect(dbp, timeout=_SQLITE_CONNECT_TIMEOUT_SEC)
            c.row_factory = sqlite3.Row
            _prepare_sqlite_connection(c)
            conn = c
            break
        except sqlite3.OperationalError as e:
            last_exc = e
            if c is not None:
                try:
                    c.close()
                except sqlite3.Error:
                    pass
            if _sqlite_locked_exc(e) and attempt + 1 < _SQLITE_CONNECT_ATTEMPTS:
                time.sleep(_SQLITE_CONNECT_RETRY_BASE_SEC * (attempt + 1))
                continue
            raise
    if conn is None:
        if last_exc is not None:
            raise last_exc
        raise sqlite3.OperationalError("could not open database")
    try:
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
    headquarters_force: bool = False,
) -> None:
    """
    Merge website/HQ into filings. Heuristic sync uses headquarters_force=False (never
    overwrites non-empty HQ with empty). S-1 AI / Ollama uses headquarters_force=True
    so a model-extracted address replaces a bad prior HTML scrape.

    ``issuer_hq_city_state`` is always derived from the resolved full HQ line (city + state
    or region, no US ZIP, no street) for pipeline / materialized profiles.
    """
    from wealth_leads.territory import issuer_hq_city_state_for_ui

    tw = (website or "").strip()
    th = (headquarters or "").strip()
    if headquarters_force and th:
        cs = issuer_hq_city_state_for_ui(th)
        conn.execute(
            """
            UPDATE filings SET
                issuer_website = COALESCE(NULLIF(?, ''), issuer_website),
                issuer_headquarters = ?,
                issuer_hq_city_state = ?
            WHERE id = ?
            """,
            (tw, th, cs, filing_id),
        )
        return
    if th:
        cs = issuer_hq_city_state_for_ui(th)
        conn.execute(
            """
            UPDATE filings SET
                issuer_website = COALESCE(NULLIF(TRIM(?), ''), issuer_website),
                issuer_headquarters = COALESCE(NULLIF(TRIM(?), ''), issuer_headquarters),
                issuer_hq_city_state = ?
            WHERE id = ?
            """,
            (tw, th, cs, filing_id),
        )
    else:
        conn.execute(
            """
            UPDATE filings SET
                issuer_website = COALESCE(NULLIF(TRIM(?), ''), issuer_website)
            WHERE id = ?
            """,
            (tw, filing_id),
        )


def update_filing_issuer_industry(
    conn: sqlite3.Connection, filing_id: int, industry: str
) -> None:
    s = (industry or "").strip()
    if not s:
        return
    conn.execute(
        """
        UPDATE filings SET
            issuer_industry = COALESCE(NULLIF(TRIM(?), ''), issuer_industry)
        WHERE id = ?
        """,
        (s[:2000], filing_id),
    )


def update_filing_issuer_industry_if_empty(
    conn: sqlite3.Connection, filing_id: int, industry: str
) -> None:
    """LLM / secondary source: do not replace SIC/NAICS already parsed from the filing."""
    s = (industry or "").strip()
    if not s:
        return
    conn.execute(
        """
        UPDATE filings SET issuer_industry = ?
        WHERE id = ?
          AND (issuer_industry IS NULL OR TRIM(issuer_industry) = '')
        """,
        (s[:2000], filing_id),
    )


def update_filing_issuer_revenue_text(
    conn: sqlite3.Connection, filing_id: int, revenue: str
) -> None:
    """Persist revenue line (LLM or heuristic). Empty string skips."""
    rev = (revenue or "").strip()
    if not rev:
        return
    conn.execute(
        "UPDATE filings SET issuer_revenue_text = ? WHERE id = ?",
        (rev[:2000], filing_id),
    )


def update_filing_issuer_revenue_text_if_empty(
    conn: sqlite3.Connection, filing_id: int, revenue: str
) -> None:
    """Set issuer_revenue_text only when blank (heuristic sync); LLM enrich can fill or replace later."""
    rev = (revenue or "").strip()
    if not rev:
        return
    conn.execute(
        """
        UPDATE filings SET issuer_revenue_text = ?
        WHERE id = ?
          AND (issuer_revenue_text IS NULL OR TRIM(issuer_revenue_text) = '')
        """,
        (rev[:2000], filing_id),
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


def update_filing_s1_llm_lead_pack(
    conn: sqlite3.Connection, filing_id: int, payload_json: str
) -> None:
    """Store JSON object text from LLM (offering, ownership, related parties, etc.)."""
    s = (payload_json or "").strip()
    if not s:
        return
    conn.execute(
        "UPDATE filings SET s1_llm_lead_pack = ? WHERE id = ?",
        (s[:60_000], filing_id),
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


def app_user_count(conn: sqlite3.Connection) -> int:
    if not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='app_users'"
    ).fetchone():
        return 0
    return int(conn.execute("SELECT COUNT(*) FROM app_users").fetchone()[0])


def get_app_user_by_email(conn: sqlite3.Connection, email: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM app_users WHERE email = ? COLLATE NOCASE",
        (email.strip(),),
    ).fetchone()


def get_app_user_by_id(conn: sqlite3.Connection, user_id: int) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM app_users WHERE id = ?", (user_id,)).fetchone()


def insert_app_user(conn: sqlite3.Connection, email: str, password_hash: str) -> int:
    cur = conn.execute(
        "INSERT INTO app_users (email, password_hash) VALUES (?, ?)",
        (email.strip().lower(), password_hash),
    )
    return int(cur.lastrowid)


def list_user_watchlist(conn: sqlite3.Connection, user_id: int) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            """
            SELECT id, cik, person_norm, label, created_at
            FROM user_watchlist WHERE user_id = ?
            ORDER BY created_at DESC
            """,
            (user_id,),
        ).fetchall()
    )


def add_user_watchlist(
    conn: sqlite3.Connection,
    user_id: int,
    *,
    cik: str,
    person_norm: str = "",
    label: str = "",
) -> int:
    cik_s = (cik or "").strip()
    pn = " ".join((person_norm or "").lower().replace(".", " ").split())
    lb = (label or "").strip() or None
    ex = conn.execute(
        """
        SELECT id FROM user_watchlist
        WHERE user_id = ? AND cik = ? AND person_norm = ?
        """,
        (user_id, cik_s, pn),
    ).fetchone()
    if ex:
        conn.execute(
            "UPDATE user_watchlist SET label = ? WHERE id = ?",
            (lb, int(ex["id"])),
        )
        return int(ex["id"])
    cur = conn.execute(
        """
        INSERT INTO user_watchlist (user_id, cik, person_norm, label)
        VALUES (?, ?, ?, ?)
        """,
        (user_id, cik_s, pn, lb),
    )
    return int(cur.lastrowid)


def delete_user_watchlist(conn: sqlite3.Connection, user_id: int, item_id: int) -> bool:
    cur = conn.execute(
        "DELETE FROM user_watchlist WHERE id = ? AND user_id = ?",
        (item_id, user_id),
    )
    return cur.rowcount > 0


def _migrate_allocation_system(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(app_users)").fetchall()}
    if "is_admin" not in cols:
        conn.execute(
            "ALTER TABLE app_users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0"
        )
    if "monthly_lead_quota" not in cols:
        conn.execute(
            "ALTER TABLE app_users ADD COLUMN monthly_lead_quota INTEGER NOT NULL DEFAULT 30"
        )
    if "territory_type" not in cols:
        conn.execute(
            "ALTER TABLE app_users ADD COLUMN territory_type TEXT NOT NULL DEFAULT 'state'"
        )
    if "territory_spec" not in cols:
        conn.execute("ALTER TABLE app_users ADD COLUMN territory_spec TEXT NOT NULL DEFAULT ''")
    if "premium_s1_only" not in cols:
        conn.execute(
            "ALTER TABLE app_users ADD COLUMN premium_s1_only INTEGER NOT NULL DEFAULT 0"
        )
    if "allow_shared_leads" not in cols:
        conn.execute(
            "ALTER TABLE app_users ADD COLUMN allow_shared_leads INTEGER NOT NULL DEFAULT 0"
        )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS allocation_settings (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            max_clients_per_territory INTEGER NOT NULL DEFAULT 1,
            default_monthly_quota INTEGER NOT NULL DEFAULT 30,
            allow_shared_leads_default INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO allocation_settings (id, max_clients_per_territory, default_monthly_quota, allow_shared_leads_default)
        VALUES (1, 1, 30, 0)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lead_assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            cik TEXT NOT NULL,
            person_norm TEXT NOT NULL,
            cycle_yyyymm TEXT NOT NULL,
            territory_key TEXT NOT NULL DEFAULT '',
            score REAL NOT NULL DEFAULT 0,
            tags_json TEXT NOT NULL DEFAULT '[]',
            liquidity_stage TEXT NOT NULL DEFAULT '',
            why_summary TEXT NOT NULL DEFAULT '',
            outreach_angle TEXT NOT NULL DEFAULT '',
            email_guess TEXT,
            email_confidence REAL,
            profile_snapshot_json TEXT NOT NULL DEFAULT '{}',
            assigned_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES app_users(id),
            UNIQUE(user_id, cik, person_norm, cycle_yyyymm)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_lead_assign_cycle ON lead_assignments(cycle_yyyymm)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_lead_assign_user_cycle ON lead_assignments(user_id, cycle_yyyymm)"
    )

    n_users = int(conn.execute("SELECT COUNT(*) FROM app_users").fetchone()[0])
    if n_users == 1:
        conn.execute("UPDATE app_users SET is_admin = 1 WHERE id = (SELECT MIN(id) FROM app_users)")


def _migrate_lead_profile(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lead_profile (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cik TEXT NOT NULL,
            person_norm TEXT NOT NULL,
            display_name TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL DEFAULT '',
            company_name TEXT NOT NULL DEFAULT '',
            filing_date_latest TEXT NOT NULL DEFAULT '',
            accession_latest TEXT NOT NULL DEFAULT '',
            primary_doc_url TEXT NOT NULL DEFAULT '',
            form_type_latest TEXT NOT NULL DEFAULT '',
            issuer_headquarters TEXT NOT NULL DEFAULT '',
            issuer_industry TEXT NOT NULL DEFAULT '',
            issuer_website TEXT NOT NULL DEFAULT '',
            index_url TEXT NOT NULL DEFAULT '',
            equity_hwm REAL,
            total_hwm REAL,
            signal_hwm REAL,
            headline_year INTEGER,
            has_s1_comp INTEGER NOT NULL DEFAULT 0,
            has_mgmt_bio INTEGER NOT NULL DEFAULT 0,
            has_officer_row INTEGER NOT NULL DEFAULT 0,
            neo_row_count INTEGER NOT NULL DEFAULT 0,
            comp_timeline TEXT NOT NULL DEFAULT '',
            issuer_summary_excerpt TEXT NOT NULL DEFAULT '',
            why_surfaced TEXT NOT NULL DEFAULT '',
            neo_filing_ids_json TEXT NOT NULL DEFAULT '[]',
            quality_score INTEGER NOT NULL DEFAULT 0,
            cross_company_hint INTEGER NOT NULL DEFAULT 0,
            other_ciks_json TEXT NOT NULL DEFAULT '[]',
            built_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(cik, person_norm)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_lead_profile_filing_date ON lead_profile(filing_date_latest DESC)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_lead_profile_cik ON lead_profile(cik)")


def _migrate_lead_profile_llm_flag(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(lead_profile)").fetchall()}
    if "comp_llm_assisted" not in cols:
        conn.execute(
            "ALTER TABLE lead_profile ADD COLUMN comp_llm_assisted "
            "INTEGER NOT NULL DEFAULT 0"
        )


def _migrate_lead_profile_lead_tier(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(lead_profile)").fetchall()}
    if "lead_tier" not in cols:
        conn.execute(
            "ALTER TABLE lead_profile ADD COLUMN lead_tier TEXT NOT NULL DEFAULT 'premium'"
        )


def _migrate_lead_profile_headline_comp(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(lead_profile)").fetchall()}
    if "salary_headline" not in cols:
        conn.execute("ALTER TABLE lead_profile ADD COLUMN salary_headline REAL")
    if "bonus_headline" not in cols:
        conn.execute("ALTER TABLE lead_profile ADD COLUMN bonus_headline REAL")
    if "other_comp_headline" not in cols:
        conn.execute("ALTER TABLE lead_profile ADD COLUMN other_comp_headline REAL")
    if "stock_grants_headline" not in cols:
        conn.execute("ALTER TABLE lead_profile ADD COLUMN stock_grants_headline REAL")
    if "total_headline" not in cols:
        conn.execute("ALTER TABLE lead_profile ADD COLUMN total_headline REAL")


def _migrate_lead_profile_hq_materialized(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(lead_profile)").fetchall()}
    if "issuer_hq_city_state" not in cols:
        conn.execute(
            "ALTER TABLE lead_profile ADD COLUMN issuer_hq_city_state "
            "TEXT NOT NULL DEFAULT ''"
        )
    if "issuer_hq_has_detail" not in cols:
        conn.execute(
            "ALTER TABLE lead_profile ADD COLUMN issuer_hq_has_detail "
            "INTEGER NOT NULL DEFAULT 0"
        )


def _migrate_lead_profile_issuer_listing_stage(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(lead_profile)").fetchall()}
    if "issuer_listing_stage" not in cols:
        conn.execute(
            "ALTER TABLE lead_profile ADD COLUMN issuer_listing_stage "
            "TEXT NOT NULL DEFAULT 'unknown'"
        )


def _migrate_lead_profile_beneficial_flag(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(lead_profile)").fetchall()}
    if "has_beneficial_owner_stake" not in cols:
        conn.execute(
            "ALTER TABLE lead_profile ADD COLUMN has_beneficial_owner_stake "
            "INTEGER NOT NULL DEFAULT 0"
        )


def _migrate_lead_profile_issuer_risk(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(lead_profile)").fetchall()}
    if "issuer_risk_level" not in cols:
        conn.execute(
            "ALTER TABLE lead_profile ADD COLUMN issuer_risk_level "
            "TEXT NOT NULL DEFAULT 'none'"
        )
    if "issuer_risk_reasons_json" not in cols:
        conn.execute(
            "ALTER TABLE lead_profile ADD COLUMN issuer_risk_reasons_json "
            "TEXT NOT NULL DEFAULT '[]'"
        )


def _migrate_lead_profile_drop_quality_index(conn: sqlite3.Connection) -> None:
    """quality_score was always 0; same build path for all rows — index added no signal."""
    try:
        conn.execute("DROP INDEX IF EXISTS idx_lead_profile_quality")
    except sqlite3.Error:
        pass


def _migrate_lead_profile_search_text(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(lead_profile)").fetchall()}
    if "search_text" not in cols:
        try:
            conn.execute(
                "ALTER TABLE lead_profile ADD COLUMN search_text TEXT NOT NULL DEFAULT ''"
            )
        except sqlite3.Error:
            return
    try:
        conn.execute(
            """
            UPDATE lead_profile SET search_text = trim(lower(
                coalesce(display_name,'') || ' ' || coalesce(company_name,'') || ' ' ||
                coalesce(title,'') || ' ' || coalesce(person_norm,'') || ' ' ||
                coalesce(cik,'') || ' ' || coalesce(issuer_headquarters,'') || ' ' ||
                coalesce(issuer_industry,'')
            ))
            WHERE trim(coalesce(search_text,'')) = ''
            """
        )
    except sqlite3.Error:
        pass


def _migrate_lead_profile_fts5(conn: sqlite3.Connection) -> None:
    if not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='lead_profile'"
    ).fetchone():
        return
    if conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='lead_profile_fts'"
    ).fetchone():
        return
    try:
        conn.execute(
            """
            CREATE VIRTUAL TABLE lead_profile_fts USING fts5(
                search_text,
                content='lead_profile',
                content_rowid='id'
            )
            """
        )
    except sqlite3.OperationalError:
        return
    try:
        conn.execute(
            "INSERT INTO lead_profile_fts(lead_profile_fts) VALUES('rebuild')"
        )
    except sqlite3.OperationalError:
        pass


def _migrate_lead_profile_issuer_revenue_text(conn: sqlite3.Connection) -> None:
    if not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='lead_profile'"
    ).fetchone():
        return
    cols = {row[1] for row in conn.execute("PRAGMA table_info(lead_profile)").fetchall()}
    if "issuer_revenue_text" not in cols:
        conn.execute(
            "ALTER TABLE lead_profile ADD COLUMN issuer_revenue_text TEXT NOT NULL DEFAULT ''"
        )


def rebuild_lead_profile_fts_index(conn: sqlite3.Connection) -> None:
    """Refresh FTS5 shadow index after bulk lead_profile changes."""
    if not conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='lead_profile_fts'"
    ).fetchone():
        return
    try:
        conn.execute(
            "INSERT INTO lead_profile_fts(lead_profile_fts) VALUES('rebuild')"
        )
    except sqlite3.OperationalError:
        pass


def _lead_profile_fts_table_exists(conn: sqlite3.Connection) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='lead_profile_fts'"
        ).fetchone()
        is not None
    )


def _lead_profile_fts_prefix_query(raw: str) -> Optional[str]:
    toks = re.findall(r"[A-Za-z0-9][A-Za-z0-9\-]{0,63}", (raw or "").strip())
    if not toks:
        return None
    parts: list[str] = []
    for t in toks:
        tl = t.lower()
        parts.append(f"search_text:{tl}*")
    return " AND ".join(parts)


def _migrate_lead_suppress(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lead_suppress (
            cik TEXT NOT NULL,
            person_norm TEXT NOT NULL,
            reason TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (cik, person_norm)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_lead_suppress_cik ON lead_suppress(cik)"
    )


def _migrate_issuer_sec_hints(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS issuer_sec_hints (
            cik TEXT NOT NULL PRIMARY KEY,
            tickers_json TEXT NOT NULL DEFAULT '[]',
            exchanges_json TEXT NOT NULL DEFAULT '[]',
            company_name TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )


def _migrate_lead_advisor_feedback(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lead_advisor_feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cik TEXT NOT NULL,
            person_norm TEXT NOT NULL,
            category TEXT NOT NULL,
            note TEXT NOT NULL DEFAULT '',
            user_email TEXT NOT NULL DEFAULT '',
            also_suppress INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_lead_fb_cik ON lead_advisor_feedback(cik)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_lead_fb_created ON lead_advisor_feedback(created_at)"
    )


def _migrate_advisor_lead_outreach(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS advisor_lead_outreach (
            user_id INTEGER NOT NULL,
            cik TEXT NOT NULL,
            person_norm TEXT NOT NULL,
            email TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'none',
            notes TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (user_id, cik, person_norm)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_alo_user_cik ON advisor_lead_outreach(user_id, cik)"
    )


def issuer_listing_stage_map(
    conn: sqlite3.Connection, ciks: set[str]
) -> dict[str, str]:
    """
    Classify issuers from `filings.form_type` only (what is in the local DB).

    - public: any 10-K or 10-Q family form
    - pre_ipo: S-1 / F-1 family but no periodic report in DB
    - unknown: otherwise (including CIKs with no rows)

    Newly public names may lack a 10-Q/10-K in DB until the next sync.
    """
    ciks_clean = {str(ck).strip() for ck in ciks if ck and str(ck).strip()}
    out: dict[str, str] = {ck: "unknown" for ck in ciks_clean}
    if not ciks_clean:
        return {}
    qm = ",".join("?" * len(ciks_clean))
    cur = conn.execute(
        f"""
        SELECT cik AS cik,
          MAX(
            CASE
              WHEN form_type LIKE '10-K' || '%' OR form_type LIKE '10-Q' || '%'
              THEN 1 ELSE 0
            END
          ) AS has_periodic,
          MAX(
            CASE
              WHEN form_type LIKE 'S-1' || '%'
                OR form_type LIKE 'F-1' || '%'
              THEN 1 ELSE 0
            END
          ) AS has_s1_family
        FROM filings
        WHERE cik IN ({qm})
        GROUP BY cik
        """,
        tuple(ciks_clean),
    )
    for r in cur.fetchall():
        ck = str(r["cik"] or "").strip()
        if not ck:
            continue
        has_p = int(r["has_periodic"] or 0)
        has_s1 = int(r["has_s1_family"] or 0)
        if has_p:
            out[ck] = "public"
        elif has_s1:
            out[ck] = "pre_ipo"
        else:
            out[ck] = "unknown"
    return out


def _migrate_lead_client_research(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS lead_client_research (
            cik TEXT NOT NULL,
            person_norm TEXT NOT NULL,
            display_name TEXT NOT NULL DEFAULT '',
            company_name TEXT NOT NULL DEFAULT '',
            issuer_website TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending',
            bio_website TEXT,
            photo_url TEXT,
            leadership_page_url TEXT,
            linkedin_profile_url TEXT,
            linkedin_search_url TEXT,
            research_summary TEXT,
            source_excerpt TEXT,
            raw_json TEXT,
            error_message TEXT,
            enriched_at TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (cik, person_norm)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_lead_client_research_status ON lead_client_research(status)"
    )


def _migrate_issuer_advisor_snapshot(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS issuer_advisor_snapshot (
            cik TEXT NOT NULL PRIMARY KEY,
            snapshot_json TEXT NOT NULL DEFAULT '{}',
            source_excerpt TEXT,
            built_at TEXT NOT NULL DEFAULT ''
        )
        """
    )


def _migrate_lead_client_research_advisor_cols(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(lead_client_research)").fetchall()}
    if "person_story" not in cols:
        conn.execute("ALTER TABLE lead_client_research ADD COLUMN person_story TEXT")
    if "outreach_json" not in cols:
        conn.execute("ALTER TABLE lead_client_research ADD COLUMN outreach_json TEXT")


def _migrate_lead_client_research_photo_blob(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(lead_client_research)").fetchall()}
    if "photo_blob" not in cols:
        conn.execute("ALTER TABLE lead_client_research ADD COLUMN photo_blob BLOB")
    if "photo_mime" not in cols:
        conn.execute("ALTER TABLE lead_client_research ADD COLUMN photo_mime TEXT")


def _migrate_lead_client_research_filing_narrative_bullets(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(lead_client_research)").fetchall()}
    if "filing_narrative_bullets" not in cols:
        conn.execute(
            "ALTER TABLE lead_client_research ADD COLUMN filing_narrative_bullets TEXT"
        )


def _migrate_beneficial_owner_stake(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS beneficial_owner_stake (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filing_id INTEGER NOT NULL,
            holder_name TEXT NOT NULL,
            holder_kind TEXT NOT NULL,
            raw_name_cell TEXT,
            shares_before_offering REAL,
            pct_beneficial REAL,
            footnote_markers TEXT,
            footnote_text TEXT,
            mailing_address TEXT,
            notional_usd_est REAL,
            offering_price_used REAL,
            gem_score INTEGER NOT NULL DEFAULT 0,
            outreach_recommended TEXT NOT NULL DEFAULT '',
            outreach_notes TEXT,
            FOREIGN KEY (filing_id) REFERENCES filings(id)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_beneficial_owner_filing ON beneficial_owner_stake(filing_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_beneficial_owner_gem ON beneficial_owner_stake(gem_score DESC)"
    )
    bcols = {
        row[1] for row in conn.execute("PRAGMA table_info(beneficial_owner_stake)").fetchall()
    }
    if "offering_price_gross_usd" not in bcols:
        conn.execute(
            "ALTER TABLE beneficial_owner_stake ADD COLUMN offering_price_gross_usd REAL"
        )
    if "offering_underwriting_deduction_usd" not in bcols:
        conn.execute(
            "ALTER TABLE beneficial_owner_stake ADD COLUMN offering_underwriting_deduction_usd REAL"
        )
    if "offering_price_doc_anchor" not in bcols:
        conn.execute(
            "ALTER TABLE beneficial_owner_stake ADD COLUMN offering_price_doc_anchor TEXT"
        )
    if "underwriting_doc_anchor" not in bcols:
        conn.execute(
            "ALTER TABLE beneficial_owner_stake ADD COLUMN underwriting_doc_anchor TEXT"
        )
    if "mailing_footnote_doc_anchor" not in bcols:
        conn.execute(
            "ALTER TABLE beneficial_owner_stake ADD COLUMN mailing_footnote_doc_anchor TEXT"
        )
    if "beneficial_parse_build" not in bcols:
        conn.execute(
            "ALTER TABLE beneficial_owner_stake ADD COLUMN beneficial_parse_build TEXT DEFAULT ''"
        )


def replace_beneficial_owner_stakes(
    conn: sqlite3.Connection, filing_id: int, rows: list[dict]
) -> None:
    conn.execute("DELETE FROM beneficial_owner_stake WHERE filing_id = ?", (filing_id,))
    if not rows:
        return
    batch: list[tuple] = []
    for r in rows:
        batch.append(
            (
                filing_id,
                (r.get("holder_name") or "")[:400],
                (r.get("holder_kind") or "unknown")[:32],
                (r.get("raw_name_cell") or "")[:500],
                r.get("shares_before_offering"),
                r.get("pct_beneficial"),
                (r.get("footnote_markers") or "[]")[:500],
                (r.get("footnote_text") or "")[:6000],
                (r.get("mailing_address") or "")[:500],
                r.get("notional_usd_est"),
                r.get("offering_price_used"),
                r.get("offering_price_gross_usd"),
                r.get("offering_underwriting_deduction_usd"),
                (r.get("offering_price_doc_anchor") or "")[:256],
                (r.get("underwriting_doc_anchor") or "")[:256],
                (r.get("mailing_footnote_doc_anchor") or "")[:256],
                (r.get("beneficial_parse_build") or "")[:32],
                int(r.get("gem_score") or 0),
                (r.get("outreach_recommended") or "")[:64],
                (r.get("outreach_notes") or "")[:1500],
            )
        )
    conn.executemany(
        """
        INSERT INTO beneficial_owner_stake (
            filing_id, holder_name, holder_kind, raw_name_cell,
            shares_before_offering, pct_beneficial, footnote_markers, footnote_text,
            mailing_address, notional_usd_est, offering_price_used,
            offering_price_gross_usd, offering_underwriting_deduction_usd,
            offering_price_doc_anchor, underwriting_doc_anchor, mailing_footnote_doc_anchor,
            beneficial_parse_build,
            gem_score, outreach_recommended, outreach_notes
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        batch,
    )


def list_beneficial_owner_gems_for_cik(
    conn: sqlite3.Connection,
    cik: str,
    *,
    min_gem: int = 1,
    limit: int = 12,
    persons_only: bool = True,
) -> list[sqlite3.Row]:
    ck = (cik or "").strip()
    if not ck:
        return []
    kind_clause = " AND b.holder_kind = 'person' " if persons_only else ""
    try:
        return list(
            conn.execute(
                f"""
                SELECT b.*, f.filing_date AS stake_filing_date, f.company_name AS stake_company_name,
                       f.accession AS stake_accession
                FROM beneficial_owner_stake b
                JOIN filings f ON f.id = b.filing_id
                WHERE f.cik = ? AND b.gem_score >= ? {kind_clause}
                ORDER BY b.gem_score DESC, COALESCE(b.notional_usd_est, 0) DESC
                LIMIT ?
                """,
                (ck, min_gem, limit),
            ).fetchall()
        )
    except sqlite3.OperationalError:
        return []


def list_beneficial_owner_stakes_for_cik(
    conn: sqlite3.Connection,
    cik: str,
) -> list[sqlite3.Row]:
    """All person/unknown beneficial-owner rows for a CIK (caller matches to a profile by name)."""
    ck = (cik or "").strip()
    if not ck:
        return []
    try:
        return list(
            conn.execute(
                """
                SELECT b.*, f.filing_date AS stake_filing_date,
                       f.primary_doc_url AS stake_primary_doc_url,
                       f.accession AS stake_accession
                FROM beneficial_owner_stake b
                JOIN filings f ON f.id = b.filing_id
                WHERE f.cik = ? AND b.holder_kind IN ('person', 'unknown')
                ORDER BY b.gem_score DESC,
                         COALESCE(b.notional_usd_est, 0) DESC,
                         f.id DESC
                """,
                (ck,),
            ).fetchall()
        )
    except sqlite3.OperationalError:
        return []


def list_beneficial_owner_outreach_targets_for_cik(
    conn: sqlite3.Connection,
    cik: str,
    *,
    limit: int = 10,
) -> list[sqlite3.Row]:
    """
    Individuals likely worth outreach: natural persons with either a parsed mailing-style
    line from the footnote or a material estimated stake (heuristic gem / notional).
    Excludes generic cap-table noise (tiny holders with no address).
    """
    ck = (cik or "").strip()
    if not ck:
        return []
    try:
        return list(
            conn.execute(
                """
                SELECT b.*, f.filing_date AS stake_filing_date, f.company_name AS stake_company_name,
                       f.accession AS stake_accession, f.primary_doc_url AS stake_primary_doc_url
                FROM beneficial_owner_stake b
                JOIN filings f ON f.id = b.filing_id
                WHERE f.cik = ?
                  AND b.holder_kind = 'person'
                  AND (
                    TRIM(COALESCE(b.mailing_address, '')) != ''
                    OR (
                      COALESCE(b.notional_usd_est, 0) >= 500000
                      AND b.gem_score >= 55
                    )
                  )
                ORDER BY
                  CASE WHEN b.outreach_recommended = 'mail' THEN 0 ELSE 1 END,
                  b.gem_score DESC,
                  COALESCE(b.notional_usd_est, 0) DESC
                LIMIT ?
                """,
                (ck, limit),
            ).fetchall()
        )
    except sqlite3.OperationalError:
        return []


def list_beneficial_owner_gems_for_filing_ids(
    conn: sqlite3.Connection,
    filing_ids: list[int],
    *,
    min_gem: int = 1,
    limit: int = 8,
    persons_only: bool = True,
) -> list[sqlite3.Row]:
    if not filing_ids:
        return []
    qm = ",".join("?" * len(filing_ids))
    kind_clause = " AND holder_kind = 'person' " if persons_only else ""
    try:
        return list(
            conn.execute(
                f"""
                SELECT * FROM beneficial_owner_stake
                WHERE filing_id IN ({qm}) AND gem_score >= ? {kind_clause}
                ORDER BY gem_score DESC, COALESCE(notional_usd_est, 0) DESC
                LIMIT ?
                """,
                tuple(filing_ids) + (min_gem, limit),
            ).fetchall()
        )
    except sqlite3.OperationalError:
        return []


def get_lead_client_research(
    conn: sqlite3.Connection, cik: str, person_norm: str
) -> Optional[sqlite3.Row]:
    ck = (cik or "").strip()
    pn = (person_norm or "").strip()
    if not ck or not pn:
        return None
    try:
        return conn.execute(
            "SELECT * FROM lead_client_research WHERE cik = ? AND person_norm = ?",
            (ck, pn),
        ).fetchone()
    except sqlite3.OperationalError:
        return None


def get_issuer_website_for_cik(conn: sqlite3.Connection, cik: str) -> str:
    """Latest non-empty issuer website for CIK (filings)."""
    ck = (cik or "").strip()
    if not ck:
        return ""
    r = conn.execute(
        """
        SELECT issuer_website FROM filings
        WHERE cik = ? AND issuer_website IS NOT NULL
          AND TRIM(issuer_website) != ''
          AND (issuer_website LIKE 'http://%' OR issuer_website LIKE 'https://%')
        ORDER BY COALESCE(filing_date, '') DESC, id DESC
        LIMIT 1
        """,
        (ck,),
    ).fetchone()
    return (r[0] or "").strip() if r else ""


def get_allocation_settings(conn: sqlite3.Connection) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM allocation_settings WHERE id = 1").fetchone()
    assert row is not None
    return row


def update_allocation_settings(
    conn: sqlite3.Connection,
    *,
    max_clients_per_territory: int,
    default_monthly_quota: int,
    allow_shared_leads_default: int,
) -> None:
    conn.execute(
        """
        UPDATE allocation_settings SET
            max_clients_per_territory = ?,
            default_monthly_quota = ?,
            allow_shared_leads_default = ?,
            updated_at = datetime('now')
        WHERE id = 1
        """,
        (max_clients_per_territory, default_monthly_quota, allow_shared_leads_default),
    )


def list_allocation_clients(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            """
            SELECT id, email, is_admin, monthly_lead_quota, territory_type,
                   territory_spec, premium_s1_only, allow_shared_leads
            FROM app_users ORDER BY email COLLATE NOCASE
            """
        ).fetchall()
    )


def update_user_allocation_profile(
    conn: sqlite3.Connection,
    user_id: int,
    *,
    monthly_lead_quota: Optional[int] = None,
    territory_type: Optional[str] = None,
    territory_spec: Optional[str] = None,
    premium_s1_only: Optional[int] = None,
    allow_shared_leads: Optional[int] = None,
    is_admin: Optional[int] = None,
) -> None:
    fields: list[str] = []
    vals: list[object] = []
    if monthly_lead_quota is not None:
        fields.append("monthly_lead_quota = ?")
        vals.append(int(monthly_lead_quota))
    if territory_type is not None:
        fields.append("territory_type = ?")
        vals.append(territory_type.strip()[:32])
    if territory_spec is not None:
        fields.append("territory_spec = ?")
        vals.append(territory_spec.strip()[:2000])
    if premium_s1_only is not None:
        fields.append("premium_s1_only = ?")
        vals.append(int(premium_s1_only))
    if allow_shared_leads is not None:
        fields.append("allow_shared_leads = ?")
        vals.append(int(allow_shared_leads))
    if is_admin is not None:
        fields.append("is_admin = ?")
        vals.append(int(is_admin))
    if not fields:
        return
    vals.append(user_id)
    conn.execute(
        f"UPDATE app_users SET {', '.join(fields)} WHERE id = ?",
        tuple(vals),
    )


def delete_assignments_for_cycle(conn: sqlite3.Connection, cycle_yyyymm: str) -> None:
    conn.execute(
        "DELETE FROM lead_assignments WHERE cycle_yyyymm = ?",
        (cycle_yyyymm.strip(),),
    )


def list_assignments_for_cycle(
    conn: sqlite3.Connection,
    cycle_yyyymm: str,
    *,
    user_id: Optional[int] = None,
) -> list[sqlite3.Row]:
    cy = cycle_yyyymm.strip()
    if user_id is not None:
        return list(
            conn.execute(
                """
                SELECT * FROM lead_assignments
                WHERE cycle_yyyymm = ? AND user_id = ?
                ORDER BY score DESC, assigned_at DESC
                """,
                (cy, user_id),
            ).fetchall()
        )
    return list(
        conn.execute(
            """
            SELECT * FROM lead_assignments
            WHERE cycle_yyyymm = ?
            ORDER BY user_id, score DESC
            """,
            (cy,),
        ).fetchall()
    )


def count_assignments_for_user_cycle(
    conn: sqlite3.Connection, user_id: int, cycle_yyyymm: str
) -> int:
    return int(
        conn.execute(
            """
            SELECT COUNT(*) FROM lead_assignments
            WHERE user_id = ? AND cycle_yyyymm = ?
            """,
            (user_id, cycle_yyyymm.strip()),
        ).fetchone()[0]
    )


def insert_lead_assignment(
    conn: sqlite3.Connection,
    *,
    user_id: int,
    cik: str,
    person_norm: str,
    cycle_yyyymm: str,
    territory_key: str,
    score: float,
    tags: list[str],
    liquidity_stage: str,
    why_summary: str,
    outreach_angle: str,
    email_guess: str,
    email_confidence: float,
    snapshot: dict,
) -> None:
    p = snapshot.get("profile") or {}
    slim = {
        "display_name": p.get("display_name"),
        "company_name": p.get("company_name"),
        "title": p.get("title"),
        "issuer_headquarters": p.get("issuer_headquarters"),
        "issuer_industry": p.get("issuer_industry"),
        "total": p.get("total"),
        "equity_hwm": p.get("equity_hwm"),
        "filing_date": p.get("filing_date"),
        "index_url": p.get("index_url"),
        "primary_doc_url": p.get("primary_doc_url"),
        "location": snapshot.get("location"),
    }
    conn.execute(
        """
        INSERT INTO lead_assignments (
            user_id, cik, person_norm, cycle_yyyymm, territory_key, score,
            tags_json, liquidity_stage, why_summary, outreach_angle,
            email_guess, email_confidence, profile_snapshot_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id,
            cik.strip(),
            person_norm.strip(),
            cycle_yyyymm.strip(),
            (territory_key or "")[:120],
            float(score),
            json.dumps(tags),
            liquidity_stage[:2000],
            why_summary[:4000],
            outreach_angle[:2000],
            (email_guess or "")[:320],
            float(email_confidence or 0),
            json.dumps(slim),
        ),
    )


def user_is_admin(conn: sqlite3.Connection, user_id: int) -> bool:
    row = conn.execute(
        "SELECT is_admin FROM app_users WHERE id = ?", (user_id,)
    ).fetchone()
    return bool(row and int(row["is_admin"] or 0))


def count_users_with_state_territory(conn: sqlite3.Connection, state_abbr: str) -> int:
    st = (state_abbr or "").strip().upper()[:2]
    if len(st) != 2:
        return 0
    return int(
        conn.execute(
            """
            SELECT COUNT(*) FROM app_users
            WHERE territory_type = 'state'
              AND UPPER(TRIM(territory_spec)) = ?
            """,
            (st,),
        ).fetchone()[0]
    )


def count_lead_profiles(conn: sqlite3.Connection) -> int:
    try:
        return int(conn.execute("SELECT COUNT(*) FROM lead_profile").fetchone()[0])
    except sqlite3.OperationalError:
        return 0


def is_lead_suppressed(
    conn: sqlite3.Connection, cik: str, person_norm: str
) -> bool:
    ck = (cik or "").strip()
    pn = (person_norm or "").strip()
    if not ck or not pn:
        return False
    try:
        r = conn.execute(
            "SELECT 1 FROM lead_suppress WHERE cik = ? AND person_norm = ? LIMIT 1",
            (ck, pn),
        ).fetchone()
        return r is not None
    except sqlite3.OperationalError:
        return False


def lead_suppress_pair_set(conn: sqlite3.Connection) -> set[tuple[str, str]]:
    """
    All (cik, person_norm) pairs in lead_suppress — one query for desk filtering
    instead of one query per profile.
    """
    out: set[tuple[str, str]] = set()
    try:
        for row in conn.execute("SELECT cik, person_norm FROM lead_suppress"):
            ck = str(row[0] or "").strip()
            pn = str(row[1] or "").strip()
            if ck and pn:
                out.add((ck, pn))
    except sqlite3.OperationalError:
        pass
    return out


def list_lead_suppress(
    conn: sqlite3.Connection, *, limit: int = 500
) -> list[sqlite3.Row]:
    try:
        cap = max(1, min(int(limit), 2000))
        return list(
            conn.execute(
                """
                SELECT cik, person_norm, reason, created_at
                FROM lead_suppress
                ORDER BY datetime(created_at) DESC, cik, person_norm
                LIMIT ?
                """,
                (cap,),
            ).fetchall()
        )
    except sqlite3.OperationalError:
        return []


def insert_lead_suppress(
    conn: sqlite3.Connection,
    *,
    cik: str,
    person_norm: str,
    reason: str = "",
) -> None:
    ck = (cik or "").strip()
    pn = (person_norm or "").strip()
    if not ck or not pn:
        return
    conn.execute(
        """
        INSERT INTO lead_suppress (cik, person_norm, reason)
        VALUES (?, ?, ?)
        ON CONFLICT(cik, person_norm) DO UPDATE SET
            reason = excluded.reason,
            created_at = datetime('now')
        """,
        (ck, pn, (reason or "").strip()[:2000]),
    )


def delete_lead_suppress(
    conn: sqlite3.Connection, *, cik: str, person_norm: str
) -> None:
    ck = (cik or "").strip()
    pn = (person_norm or "").strip()
    if not ck or not pn:
        return
    conn.execute(
        "DELETE FROM lead_suppress WHERE cik = ? AND person_norm = ?",
        (ck, pn),
    )


def _norm_cik_key(cik: str) -> str:
    try:
        return str(int(str(cik or "").strip()))
    except ValueError:
        return str(cik or "").strip()


def upsert_issuer_sec_hints(
    conn: sqlite3.Connection,
    *,
    cik: str,
    tickers: list[str],
    exchanges: list[str],
    company_name: str = "",
) -> None:
    ck = _norm_cik_key(cik)
    if not ck:
        return
    tj = json.dumps([str(x).strip() for x in tickers if str(x).strip()][:16])
    ej = json.dumps([str(x).strip() for x in exchanges if str(x).strip()][:16])
    nm = (company_name or "").strip()[:500]
    conn.execute(
        """
        INSERT INTO issuer_sec_hints (cik, tickers_json, exchanges_json, company_name, updated_at)
        VALUES (?, ?, ?, ?, datetime('now'))
        ON CONFLICT(cik) DO UPDATE SET
            tickers_json = excluded.tickers_json,
            exchanges_json = excluded.exchanges_json,
            company_name = COALESCE(NULLIF(excluded.company_name, ''), issuer_sec_hints.company_name),
            updated_at = datetime('now')
        """,
        (ck, tj, ej, nm),
    )


def get_issuer_sec_hints_row(
    conn: sqlite3.Connection, cik: str
) -> Optional[sqlite3.Row]:
    ck = _norm_cik_key(cik)
    if not ck:
        return None
    try:
        return conn.execute(
            "SELECT * FROM issuer_sec_hints WHERE cik = ? LIMIT 1", (ck,)
        ).fetchone()
    except sqlite3.OperationalError:
        return None


def max_filing_date_for_cik(conn: sqlite3.Connection, cik: str) -> str:
    ck = (cik or "").strip()
    if not ck:
        return ""
    variants = {ck}
    try:
        n = int(ck)
        variants.add(str(n))
        variants.add(f"{n:010d}")
    except ValueError:
        pass
    best = ""
    try:
        for v in variants:
            r = conn.execute(
                "SELECT MAX(filing_date) AS m FROM filings WHERE cik = ?",
                (v,),
            ).fetchone()
            d = str(r["m"] or "").strip() if r else ""
            if d and (not best or d > best):
                best = d
        return best
    except sqlite3.OperationalError:
        return ""


def insert_lead_advisor_feedback(
    conn: sqlite3.Connection,
    *,
    cik: str,
    person_norm: str,
    category: str,
    note: str = "",
    user_email: str = "",
    also_suppress: bool = False,
) -> None:
    ck = (cik or "").strip()
    pn = (person_norm or "").strip()
    cat = (category or "other").strip().lower()[:48]
    if cat not in ("wrong_person", "duplicate", "bad_issuer", "other"):
        cat = "other"
    conn.execute(
        """
        INSERT INTO lead_advisor_feedback (
            cik, person_norm, category, note, user_email, also_suppress
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            ck,
            pn,
            cat,
            (note or "").strip()[:4000],
            (user_email or "").strip()[:320],
            1 if also_suppress else 0,
        ),
    )
    if also_suppress and ck and pn:
        insert_lead_suppress(
            conn,
            cik=ck,
            person_norm=pn,
            reason=f"advisor_feedback:{cat}",
        )


def list_lead_advisor_feedback(
    conn: sqlite3.Connection, *, limit: int = 50
) -> list[sqlite3.Row]:
    try:
        cap = max(1, min(int(limit), 500))
        return list(
            conn.execute(
                """
                SELECT id, cik, person_norm, category, note, user_email, also_suppress, created_at
                FROM lead_advisor_feedback
                ORDER BY datetime(created_at) DESC, id DESC
                LIMIT ?
                """,
                (cap,),
            ).fetchall()
        )
    except sqlite3.OperationalError:
        return []


def _normalize_advisor_outreach_status(raw: str) -> str:
    x = (raw or "none").strip().lower().replace("-", "_")
    if x in ("no_reply", "noresponse"):
        x = "no_response"
    allowed = frozenset(
        ("none", "planned", "sent", "replied", "no_response", "declined")
    )
    return x if x in allowed else "none"


def get_advisor_lead_outreach(
    conn: sqlite3.Connection, user_id: int, cik: str, person_norm: str
) -> Optional[sqlite3.Row]:
    try:
        return conn.execute(
            """
            SELECT user_id, cik, person_norm, email, status, notes, updated_at
            FROM advisor_lead_outreach
            WHERE user_id = ? AND cik = ? AND person_norm = ?
            LIMIT 1
            """,
            (int(user_id), (cik or "").strip(), (person_norm or "").strip()),
        ).fetchone()
    except sqlite3.OperationalError:
        return None


def upsert_advisor_lead_outreach(
    conn: sqlite3.Connection,
    user_id: int,
    cik: str,
    person_norm: str,
    *,
    email: str = "",
    status: str = "none",
    notes: str = "",
) -> None:
    ck = (cik or "").strip()
    pn = (person_norm or "").strip()
    st = _normalize_advisor_outreach_status(status)
    conn.execute(
        """
        INSERT INTO advisor_lead_outreach (
            user_id, cik, person_norm, email, status, notes, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(user_id, cik, person_norm) DO UPDATE SET
            email = excluded.email,
            status = excluded.status,
            notes = excluded.notes,
            updated_at = datetime('now')
        """,
        (
            int(user_id),
            ck,
            pn,
            (email or "").strip()[:320],
            st,
            (notes or "").strip()[:4000],
        ),
    )


def list_advisor_lead_outreach_status_by_norm_for_cik(
    conn: sqlite3.Connection, user_id: int, cik: str
) -> dict[str, str]:
    try:
        ck = (cik or "").strip()
        rows = conn.execute(
            """
            SELECT person_norm, status FROM advisor_lead_outreach
            WHERE user_id = ? AND cik = ?
            """,
            (int(user_id), ck),
        ).fetchall()
        out: dict[str, str] = {}
        for r in rows:
            pn = str(r["person_norm"] or "").strip()
            if pn:
                out[pn] = str(r["status"] or "none")
        return out
    except sqlite3.OperationalError:
        return {}


def get_lead_profile_row(
    conn: sqlite3.Connection, cik: str, person_norm: str
) -> Optional[sqlite3.Row]:
    try:
        return conn.execute(
            "SELECT * FROM lead_profile WHERE cik = ? AND person_norm = ? LIMIT 1",
            ((cik or "").strip(), (person_norm or "").strip()),
        ).fetchone()
    except sqlite3.OperationalError:
        return None


def list_lead_profiles_for_review(
    conn: sqlite3.Connection,
    *,
    search: str = "",
    limit: int = 500,
    s1_only: bool = True,
    months_back: Optional[int] = 6,
    pay_band: str = "all",
    us_registrant_hq_only: bool = False,
    listing_stage: str = "all",
    cik: Optional[str] = None,
) -> list[sqlite3.Row]:
    """
    Pipeline review rows. By default only NEO rows tied to an S-1-family filing
    (has_s1_comp), matching a pre-IPO lead thesis. Set s1_only=False to include e.g. 10-K NEO.
    months_back: limit to filing_date_latest within this many calendar months (approximate);
    None or <=0 means no date window.
    Sorted newest filing first.

    When ``us_registrant_hq_only`` is true, SQL fetch limits are increased in steps until
    enough U.S.-HQ rows are found (or a cap), so the newest *eligible* rows are not dropped
    by an early LIMIT.

    Text search uses FTS5 on ``search_text`` when available; otherwise LIKE across name fields.
    """
    from wealth_leads.person_quality import is_acceptable_lead_person_name
    from wealth_leads.territory import registrant_hq_line_parses_as_united_states

    limit_clamped = max(1, min(int(limit), 5000))

    def _pipeline_row_display_name(r: sqlite3.Row) -> str:
        d = (r["display_name"] or "").strip()
        if d:
            return d
        pn = (r["person_norm"] or "").strip()
        return " ".join(w.title() for w in pn.split()) if pn else ""

    def _name_filter(rows_in: list[sqlite3.Row]) -> list[sqlite3.Row]:
        return [
            r
            for r in rows_in
            if is_acceptable_lead_person_name(_pipeline_row_display_name(r))
        ]

    def _us_filter(rows_in: list[sqlite3.Row]) -> list[sqlite3.Row]:
        if not us_registrant_hq_only:
            return rows_in
        return [
            r
            for r in rows_in
            if registrant_hq_line_parses_as_united_states(
                (r["issuer_headquarters"] or "").strip()
            )
        ]

    try:
        from wealth_leads.config import lead_desk_equity_only_min_usd

        sql = "SELECT * FROM lead_profile WHERE 1=1"
        params: list[object] = []
        if s1_only:
            sql += " AND (has_s1_comp = 1 OR lead_tier = 'visibility')"
        b = (pay_band or "all").strip().lower()
        if b not in ("", "all"):
            col = "equity_hwm" if lead_desk_equity_only_min_usd() else "signal_hwm"
            if b in ("million_plus", "1m", "high"):
                sql += f" AND COALESCE({col}, 0) >= ?"
                params.append(1_000_000.0)
            elif b in ("quarter_to_million", "mid", "250k"):
                sql += (
                    f" AND COALESCE({col}, 0) >= ? AND COALESCE({col}, 0) < ?"
                )
                params.extend([250_000.0, 1_000_000.0])
            elif b in ("under_quarter", "low", "rest"):
                sql += f" AND COALESCE({col}, 0) < ?"
                params.append(250_000.0)
        if months_back is not None and months_back > 0:
            from datetime import date, timedelta

            approx_days = int(float(months_back) * 30.437)
            cutoff = (date.today() - timedelta(days=approx_days)).isoformat()
            sql += " AND filing_date_latest >= ?"
            params.append(cutoff)
        search_s = (search or "").strip()
        use_fts = bool(
            search_s
            and _lead_profile_fts_table_exists(conn)
            and _lead_profile_fts_prefix_query(search_s)
        )
        if search_s:
            fts_q = _lead_profile_fts_prefix_query(search_s) if use_fts else None
            if fts_q:
                sql += (
                    " AND id IN (SELECT rowid FROM lead_profile_fts "
                    "WHERE lead_profile_fts MATCH ?)"
                )
                params.append(fts_q)
            else:
                pat = f"%{search_s}%"
                sql += (
                    " AND (display_name LIKE ? OR company_name LIKE ? OR title LIKE ? "
                    "OR issuer_headquarters LIKE ? OR person_norm LIKE ? OR cik LIKE ? "
                    "OR search_text LIKE ?)"
                )
                params.extend([pat, pat, pat, pat, pat, pat, pat])
        lst = str(listing_stage or "all").strip().lower().replace("-", "_")
        if lst in ("pre_ipo", "public", "unknown"):
            sql += " AND issuer_listing_stage = ?"
            params.append(lst)
        ck_f = (cik or "").strip()
        if ck_f:
            variants = {ck_f}
            try:
                n = int(ck_f)
                variants.add(str(n))
                variants.add(f"{n:010d}")
            except ValueError:
                pass
            qm = ",".join("?" * len(variants))
            sql += f" AND cik IN ({qm})"
            params.extend(sorted(variants))

        order_sql = " ORDER BY filing_date_latest DESC, cik, person_norm"

        def _fetch(sql_limit: int) -> list[sqlite3.Row]:
            lim = max(1, min(int(sql_limit), 5000))
            q = sql + order_sql + " LIMIT ?"
            p = list(params) + [lim]
            return list(conn.execute(q, p).fetchall())

        if not us_registrant_hq_only:
            rows = _fetch(limit_clamped)
            rows = _name_filter(rows)
            return rows[:limit_clamped]

        sql_cap = limit_clamped
        max_cap = 5000
        while True:
            rows = _fetch(sql_cap)
            rows = _name_filter(rows)
            rows_us = _us_filter(rows)
            if len(rows_us) >= limit_clamped:
                return rows_us[:limit_clamped]
            if sql_cap >= max_cap:
                return rows_us[:limit_clamped]
            nxt = min(sql_cap * 2, max_cap)
            if nxt <= sql_cap:
                return rows_us[:limit_clamped]
            sql_cap = nxt
    except sqlite3.OperationalError:
        return []
