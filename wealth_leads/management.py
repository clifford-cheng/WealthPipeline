from __future__ import annotations

import re
from typing import Optional

from bs4 import BeautifulSoup

from wealth_leads.officers import _clean_name, _clean_title

OfficerRow = tuple[str, str, str]  # name, title, source


def _strip_honorific(raw: str) -> str:
    return re.sub(r"^(Mr\.|Ms\.|Mrs\.|Dr\.|Prof\.)\s+", "", raw.strip(), flags=re.I)


def _clean_name_table(raw: str) -> Optional[str]:
    s = _strip_honorific(" ".join(raw.split()))
    if len(s) < 2:
        return None
    low = s.lower()
    if low in ("name", "none", "n/a", "—", "-"):
        return None
    return _clean_name(s) or (
        s if re.match(r"^[A-Z][a-zA-Z\.\-\s,']{2,80}$", s) else None
    )


def _clean_title_table(raw: str) -> Optional[str]:
    s = " ".join(raw.split())
    if len(s) < 2:
        return None
    low = s.lower()
    if low in ("position", "title", "none", "—"):
        return None
    # Roster titles: allow "Director" without other tokens if column is clearly "Position"
    if "director" in low and len(s) < 120:
        return s
    t = _clean_title(s)
    return t if t else (s if len(s) < 120 and any(
        k in low
        for k in (
            "chief",
            "president",
            "officer",
            "chair",
            "counsel",
            "treasurer",
            "secretary",
            "founder",
            "member",
            "ceo",
            "cfo",
            "coo",
            "cto",
        )
    ) else None)


def _header_blob(headers: list[str]) -> str:
    return " ".join(h.lower() for h in headers if h and h.strip())


def _is_executive_roster_table(headers: list[str]) -> bool:
    """
    Distinguish management roster from summary comp / other 'Name' tables.
    """
    blob = _header_blob(headers)
    if "name" not in blob:
        return False
    if any(
        x in blob
        for x in (
            "salary",
            "bonus",
            "stock",
            "option",
            "fiscal year",
            "fiscalyear",
            "non-equity",
            "pension",
            "grant",
            "total",
        )
    ):
        return False
    if any(x in blob for x in ("position", "title", "officer", "director", "chair")):
        return True
    if "age" in blob and ("position" in blob or "title" in blob):
        return True
    return False


def _column_map(headers: list[str]) -> tuple[Optional[int], Optional[int]]:
    name_i = pos_i = None
    for i, h in enumerate(headers):
        hl = h.lower()
        if name_i is None and "name" in hl and "company" not in hl:
            name_i = i
        if pos_i is None and any(
            x in hl for x in ("position", "title", "officer", "office held", "principal")
        ):
            pos_i = i
    if name_i is None and headers:
        name_i = 0
    if pos_i is None and len(headers) >= 2:
        pos_i = len(headers) - 1
    return name_i, pos_i


def _row_cell_texts(tr) -> list[str]:
    """SEC tables often use <th> for the name column; many insert blank <td> spacers between columns."""
    return [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]


def extract_executive_officers_from_filing_html(html: str) -> list[OfficerRow]:
    """
    Parse 'Executive Officers and Directors' style tables (Name / Age / Position, etc.).
    Does not replace signature parsing — merge with merge_officer_rows().
    """
    soup = BeautifulSoup(html, "html.parser")
    found: list[OfficerRow] = []

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        header_idx: Optional[int] = None
        headers: list[str] = []
        for hi in range(min(20, len(rows))):
            headers = _row_cell_texts(rows[hi])
            if headers and _is_executive_roster_table(headers):
                header_idx = hi
                break
        if header_idx is None:
            continue
        name_i, pos_i = _column_map(headers)
        if name_i is None or pos_i is None:
            continue
        for tr in rows[header_idx + 1 :]:
            cells = _row_cell_texts(tr)
            if len(cells) <= max(name_i, pos_i):
                continue
            name_raw = cells[name_i]
            title_raw = cells[pos_i]
            if name_raw.isdigit() or len(name_raw) <= 1:
                continue
            name = _clean_name_table(name_raw)
            title = _clean_title_table(title_raw)
            if not name or not title:
                continue
            found.append((name, title, "management_section"))

    by_name: dict[str, OfficerRow] = {}
    for name, title, src in found:
        key = " ".join(name.lower().split())
        prev = by_name.get(key)
        if prev is None or len(title) > len(prev[1]):
            by_name[key] = (name, title, src)
    return sorted(by_name.values(), key=lambda x: (x[1], x[0]))


_SOURCE_RANK = {"management_section": 0, "signature_table": 1, "merged": 2}


def merge_officer_rows(*groups: list[OfficerRow]) -> list[OfficerRow]:
    """
    One row per person; prefer management_section over signature_table; then longer title.
    Pass arguments with management list first, then signature list.
    """
    by_key: dict[str, tuple[str, str, str, int]] = {}
    for group in groups:
        for name, title, src in group:
            key = " ".join(name.lower().replace(".", " ").split())
            rank = _SOURCE_RANK.get(src, 5)
            prev = by_key.get(key)
            if prev is None:
                by_key[key] = (name, title, src, rank)
                continue
            _, ptitle, _, prank = prev
            if rank < prank:
                by_key[key] = (name, title, src, rank)
            elif rank == prank and len(title) > len(ptitle):
                by_key[key] = (name, title, src, rank)
    return sorted(
        [(n, t, s) for n, t, s, _ in by_key.values()],
        key=lambda x: (x[1], x[0]),
    )


def extract_issuer_summary_from_filing_html(
    html: str, *, max_chars: int = 650
) -> str:
    """
    First filing-sourced blurb from Prospectus Summary (registration statements).
    Plain-text heuristic; no external scraping.
    """
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    if not text:
        return ""

    patterns = [
        r"prospectus\s+summary\s*[:\s]*\n*(.+?)(?:\n\s*\n|the\s+offering|risk\s+factors|implications\s+of\s+being|$)",
        r"summary\s+of\s+the\s+offering\s*[:\s]*\n*(.+?)(?:\n\s*\n|risk\s+factors|$)",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.I | re.S)
        if m:
            chunk = re.sub(r"\s+", " ", m.group(1)).strip()
            if len(chunk) < 40:
                continue
            return (chunk[:max_chars] + "…") if len(chunk) > max_chars else chunk

    m = re.search(
        r"(?:our\s+)?company\s*[—\-]\s*(.+?)(?:\n\s*\n|business|industry|$)",
        text,
        re.I | re.S,
    )
    if m:
        chunk = re.sub(r"\s+", " ", m.group(1)).strip()
        if len(chunk) >= 40:
            return (chunk[:max_chars] + "…") if len(chunk) > max_chars else chunk
    return ""


def why_surfaced_line(form_type: str, filing_date: Optional[str]) -> str:
    """Single-line, filing-based timing context (not investment advice)."""
    ft = (form_type or "Filing").strip()
    fd = filing_date or "date unknown"
    amend = bool(re.search(r"/\s*A\b", ft, re.I))
    tail = " (amendment)" if amend else ""
    return f"{ft} filed {fd}{tail} — public registration activity in EDGAR."
