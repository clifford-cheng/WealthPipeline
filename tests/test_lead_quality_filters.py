"""Tests for issuer text scan, suppress list, and assignment gate helpers."""
from __future__ import annotations

import os
import sqlite3
import unittest
from unittest.mock import patch

from wealth_leads.allocation import _assignment_gate_skip_reason
from wealth_leads.issuer_lead_quality import scan_issuer_summary_text
from wealth_leads.submissions import submission_form_is_8k


class TestSubmissionForm8k(unittest.TestCase):
    def test_8k_variants(self) -> None:
        self.assertTrue(submission_form_is_8k("8-K"))
        self.assertTrue(submission_form_is_8k("8-K/A"))
        self.assertTrue(submission_form_is_8k("8-K12B"))
        self.assertFalse(submission_form_is_8k("10-K"))
        self.assertFalse(submission_form_is_8k("S-1"))


class TestIssuerTextScan(unittest.TestCase):
    def test_none_on_short_text(self) -> None:
        self.assertEqual(scan_issuer_summary_text("hi")["level"], "none")

    def test_high_bankruptcy(self) -> None:
        blob = (
            "The Company filed a voluntary petition under Chapter 11 of the Bankruptcy Code "
            "to restructure its obligations."
        )
        r = scan_issuer_summary_text(blob)
        self.assertEqual(r["level"], "high")
        self.assertTrue(any("Bankruptcy" in x for x in r["reasons"]))

    def test_elevated_reverse_split(self) -> None:
        blob = (
            "On March 1, 2024 the Board approved a 1-for-10 reverse stock split of our "
            "common stock to maintain exchange listing compliance."
        )
        r = scan_issuer_summary_text(blob)
        self.assertEqual(r["level"], "elevated")


class TestAssignmentGate(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(
            """
            CREATE TABLE lead_suppress (
                cik TEXT NOT NULL,
                person_norm TEXT NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (cik, person_norm)
            )
            """
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()

    def test_incomplete_profile(self) -> None:
        self.assertEqual(
            _assignment_gate_skip_reason(self.conn, {"cik": "", "norm_name": "a"}),
            "incomplete",
        )

    def test_visibility_when_excluded(self) -> None:
        p = {
            "cik": "0001234567",
            "norm_name": "jane doe",
            "lead_tier": "visibility",
        }
        with patch.dict(os.environ, {"WEALTH_LEADS_ASSIGN_EXCLUDE_VISIBILITY": "1"}):
            self.assertEqual(_assignment_gate_skip_reason(self.conn, p), "visibility")
        with patch.dict(os.environ, {"WEALTH_LEADS_ASSIGN_EXCLUDE_VISIBILITY": "0"}):
            self.assertIsNone(_assignment_gate_skip_reason(self.conn, p))

    def test_issuer_risk_high(self) -> None:
        p = {
            "cik": "0001234567",
            "norm_name": "jane doe",
            "lead_tier": "premium",
            "issuer_risk_level": "high",
        }
        with patch.dict(os.environ, {"WEALTH_LEADS_ASSIGN_EXCLUDE_ISSUER_RISK_HIGH": "1"}):
            self.assertEqual(
                _assignment_gate_skip_reason(self.conn, p), "issuer_risk_high"
            )

    def test_stale_filing(self) -> None:
        p = {
            "cik": "0001234567",
            "norm_name": "jane doe",
            "lead_tier": "premium",
            "issuer_risk_level": "none",
            "filing_date": "1999-01-01",
        }
        with patch.dict(
            os.environ,
            {"WEALTH_LEADS_ASSIGN_MAX_FILING_STALE_DAYS": "365"},
        ):
            self.assertEqual(
                _assignment_gate_skip_reason(self.conn, p), "stale_filing"
            )


if __name__ == "__main__":
    unittest.main()
