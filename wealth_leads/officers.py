from __future__ import annotations

import re
from typing import Optional

from bs4 import BeautifulSoup

# Titles we keep from signature blocks (exclude generic parentheticals-only rows).
_TITLE_HINT = re.compile(
    r"\b("
    r"chief|president|officer|director|chairman|chairperson|"
    r"treasurer|secretary|evp|svp|vp\b|vice president|"
    r"general counsel|counsel|controller|partner|"
    r"ceo|cfo|coo|cto|cio|cmo|cpo"
    r")\b",
    re.I,
)

_SKIP_NAME = re.compile(
    r"\b(LLP|LLC|Inc\.?|Corp\.?|Company|Securities|Commission|Trust)\b",
    re.I,
)


def _clean_name(raw: str) -> Optional[str]:
    s = " ".join(raw.split())
    s = re.sub(r"^/s/\s*", "", s, flags=re.I).strip()
    if len(s) < 3 or _SKIP_NAME.search(s):
        return None
    if not re.match(r"^[A-Z][a-zA-Z\.\-\s,']+$", s):
        # Allow "O'Brien", "Jean-Luc", commas in "Smith, Jr."
        if not re.match(r"^[A-Z][a-zA-Z\.\-\s,']{2,80}$", s):
            return None
    return s


def _clean_title(raw: str) -> Optional[str]:
    s = " ".join(raw.split())
    if len(s) < 2:
        return None
    if s.startswith("(") and "officer" in s.lower():
        # e.g. (Principal Executive Officer) — supplemental, skip as primary title
        return None
    if not _TITLE_HINT.search(s):
        return None
    return s


def extract_officers_from_s1_html(
    html: str,
) -> list[tuple[str, str, str, Optional[int]]]:
    """
    Best-effort extraction of signers from the standard SEC signature table
    (Signature / Title / Date). Works for S-1 registration statements and
    many 10-K / Exchange Act certification pages. Returns (name, title, source, age).
    """
    soup = BeautifulSoup(html, "html.parser")
    found: list[tuple[str, str, str, None]] = []

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        header_text = " ".join(rows[0].get_text(" ", strip=True).lower().split())
        if "signature" not in header_text or "title" not in header_text:
            continue
        table_text = table.get_text()
        tl = table_text.lower()
        filing_context = (
            "securities act" in tl
            or "registration statement" in tl
            or "exchange act" in tl
            or "annual report" in tl
            or "/s/" in table_text
        )
        if not filing_context:
            continue

        for tr in rows[1:]:
            cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            if len(cells) < 3:
                continue
            name_raw, title_raw = cells[0], cells[2]
            title = _clean_title(title_raw)
            if not title:
                continue
            name = _clean_name(name_raw)
            if name:
                found.append((name, title, "signature_table", None))

    # De-dupe by name, prefer longer / more specific titles
    by_name: dict[str, tuple[str, str, str, Optional[int]]] = {}
    for name, title, src, age in found:
        prev = by_name.get(name)
        if prev is None or len(title) > len(prev[1]):
            by_name[name] = (name, title, src, age)
    return sorted(by_name.values(), key=lambda x: (x[1], x[0]))
