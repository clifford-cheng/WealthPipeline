"""
Heuristic issuer-risk signals from **stored** filing text (`filings.issuer_summary`).

Scans the latest few S-1/F-1/10-K **and 8-K** summaries per CIK (8-K rows appear after
``SEC_FOLLOW_8K`` sync). This is still not legal/financial advice — advisors verify material facts.
"""
from __future__ import annotations

import re
import sqlite3
from typing import Any

# (level, reason_label, phrase_or_regex)
# `high` should be conservative — fewer false positives than elevated.
_HIGH_RULES: tuple[tuple[str, str, str], ...] = (
    ("high", "Bankruptcy / Chapter 11", r"\bchapter\s+11\b"),
    ("high", "Bankruptcy / Chapter 7", r"\bchapter\s+7\b"),
    ("high", "Bankruptcy / Chapter 15", r"\bchapter\s+15\b"),
    ("high", "Bankruptcy filing", r"\bbankruptcy\s+court\b"),
    ("high", "Bankruptcy filing", r"\bbankruptcy\s+petition\b"),
    ("high", "Bankruptcy filing", r"\bfiled\s+for\s+bankruptcy\b"),
    ("high", "Receivership", r"\breceivership\b"),
    ("high", "Delisting", r"\bdelist"),
    ("high", "Trading suspended", r"\btrading\s+suspended\b"),
    ("high", "Ceased operations", r"\bceased\s+(all\s+)?operations\b"),
)

_ELEVATED_RULES: tuple[tuple[str, str, str], ...] = (
    ("elevated", "Material weakness", r"\bmaterial\s+weakness\b"),
    ("elevated", "Restatement", r"\brestatement\b"),
    ("elevated", "Reverse stock split", r"\breverse\s+stock\s+split\b"),
    ("elevated", "Reverse split ratio", r"\b1\s*[-–]\s*for\s*[-–]?\s*\d+"),
    ("elevated", "SEC / regulatory inquiry", r"\bsec\s+investigation\b"),
    ("elevated", "SEC / regulatory inquiry", r"\bwells\s+notice\b"),
    ("elevated", "Subpoena / inquiry", r"\bsubpoena\b"),
    ("elevated", "DOJ / criminal reference", r"\bdepartment\s+of\s+justice\b"),
    ("elevated", "Fraud / enforcement (narrative)", r"\bsecurities\s+fraud\b"),
)


def _compile_rules(
    rules: tuple[tuple[str, str, str], ...],
) -> list[tuple[str, str, re.Pattern[str]]]:
    out: list[tuple[str, str, re.Pattern[str]]] = []
    for level, label, pat in rules:
        out.append((level, label, re.compile(pat, re.I)))
    return out


_HIGH_PATTERNS = _compile_rules(_HIGH_RULES)
_ELEVATED_PATTERNS = _compile_rules(_ELEVATED_RULES)


def scan_issuer_summary_text(text: str) -> dict[str, Any]:
    """
    Return ``{"level": "none"|"elevated"|"high", "reasons": [str, ...]}``.
    Deduplicates reason labels; high wins over elevated.
    """
    blob = (text or "").strip().lower()
    if len(blob) < 40:
        return {"level": "none", "reasons": []}
    reasons: list[str] = []
    seen: set[str] = set()
    level = "none"
    for lv, label, rx in _HIGH_PATTERNS:
        if rx.search(blob):
            level = "high"
            if label not in seen:
                seen.add(label)
                reasons.append(label)
    for lv, label, rx in _ELEVATED_PATTERNS:
        if rx.search(blob):
            if level != "high":
                level = "elevated"
            if label not in seen:
                seen.add(label)
                reasons.append(label)
    return {"level": level, "reasons": reasons}


def issuer_adverse_signal_map(
    conn: sqlite3.Connection, ciks: set[str]
) -> dict[str, dict[str, Any]]:
    """
    For each CIK, scan up to three most recent non-empty ``issuer_summary`` blobs from
    S-1/F-1/10-K family rows in the local DB.
    """
    ciks_clean = {str(ck).strip() for ck in ciks if ck and str(ck).strip()}
    out: dict[str, dict[str, Any]] = {
        ck: {"level": "none", "reasons": []} for ck in ciks_clean
    }
    if not ciks_clean:
        return {}
    qm = ",".join("?" * len(ciks_clean))
    cur = conn.execute(
        f"""
        SELECT cik, issuer_summary, filing_date, form_type
        FROM filings
        WHERE cik IN ({qm})
          AND issuer_summary IS NOT NULL
          AND TRIM(issuer_summary) != ''
          AND (
            form_type LIKE 'S-1%' OR form_type LIKE 'F-1%'
            OR form_type LIKE '10-K%'
            OR form_type LIKE '8-K%'
          )
        ORDER BY cik, COALESCE(filing_date, '') DESC, id DESC
        """,
        tuple(ciks_clean),
    )
    per_cik_parts: dict[str, list[str]] = {ck: [] for ck in ciks_clean}
    counts: dict[str, int] = {ck: 0 for ck in ciks_clean}
    for r in cur.fetchall():
        ck = str(r["cik"] or "").strip()
        if ck not in per_cik_parts or counts[ck] >= 3:
            continue
        s = (r["issuer_summary"] or "").strip()
        if not s:
            continue
        per_cik_parts[ck].append(s[:120_000])
        counts[ck] += 1

    for ck, parts in per_cik_parts.items():
        if not parts:
            continue
        merged = "\n\n".join(parts)
        sig = scan_issuer_summary_text(merged)
        out[ck] = sig
    return out
