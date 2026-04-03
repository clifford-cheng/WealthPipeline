"""
Read-only checks for materialized lead_profile + FTS. Run:

    python -m wealth_leads.qa_lead_profile
"""
from __future__ import annotations

import re
import sqlite3
import sys


def main() -> int:
    from wealth_leads.config import database_path
    from wealth_leads.db import (
        _lead_profile_fts_table_exists,
        _lead_profile_fts_prefix_query,
        list_lead_profiles_for_review,
    )

    path = database_path()
    print(f"Database: {path}")
    try:
        conn = sqlite3.connect(str(path))
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as e:
        print(f"Could not open DB: {e}", file=sys.stderr)
        return 1

    try:
        n = int(conn.execute("SELECT COUNT(*) FROM lead_profile").fetchone()[0])
        print(f"lead_profile rows: {n}")

        bad_dates = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM lead_profile
                WHERE filing_date_latest != ''
                  AND filing_date_latest NOT GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
                """
            ).fetchone()[0]
        )
        print(f"filing_date_latest not ISO YYYY-MM-DD (non-empty): {bad_dates}")

        for col in ("lead_tier", "issuer_risk_level", "issuer_listing_stage"):
            print(f"\n{col}:")
            cur = conn.execute(
                f"SELECT {col}, COUNT(*) AS c FROM lead_profile GROUP BY {col} ORDER BY c DESC LIMIT 12"
            )
            for r in cur.fetchall():
                print(f"  {(r[0] or '').strip()!r}: {r[1]}")

        llm = int(
            conn.execute(
                "SELECT COUNT(*) FROM lead_profile WHERE comp_llm_assisted = 1"
            ).fetchone()[0]
        )
        print(f"\ncomp_llm_assisted=1: {llm}")

        st_empty = int(
            conn.execute(
                "SELECT COUNT(*) FROM lead_profile WHERE trim(coalesce(search_text,'')) = ''"
            ).fetchone()[0]
        )
        print(f"empty search_text: {st_empty} (run Rebuild profiles to fill)")

        fts = _lead_profile_fts_table_exists(conn)
        print(f"lead_profile_fts table: {fts}")
        if fts and n > 0:
            pq = _lead_profile_fts_prefix_query("a")
            if pq:
                try:
                    m = int(
                        conn.execute(
                            "SELECT COUNT(*) FROM lead_profile_fts WHERE lead_profile_fts MATCH ?",
                            (pq,),
                        ).fetchone()[0]
                    )
                    print(f"FTS smoke query (prefix 'a'): {m} hits")
                except sqlite3.OperationalError as e:
                    print(f"FTS MATCH failed: {e}", file=sys.stderr)

        sample = list_lead_profiles_for_review(conn, search="", limit=3, months_back=0)
        print(f"\nlist_lead_profiles_for_review(limit=3, months_back=0): {len(sample)} rows")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
