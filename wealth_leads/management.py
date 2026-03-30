from __future__ import annotations

import re
from typing import Optional

from bs4 import BeautifulSoup

from wealth_leads.officers import _clean_name, _clean_title

OfficerRow = tuple[str, str, str, Optional[int]]  # name, title, source, age (management tables)

# Phrases typical of comp/governance pages — not a business description.
_ISSUER_SUMMARY_SPAM = (
    "hedging transaction",
    "short sales",
    "company-based derivative",
    "derivative securities",
    "clawback policy",
    "covered persons",
    "covered person",
    "anti-hedging",
    "insider trading policy",
    "section 10d of the exchange act",
    "nyse listing company manual",
    "incentive compensation granted",
    "restatement of our financial statements",
)


def issuer_summary_looks_spammy(chunk: str) -> bool:
    if not chunk or len(chunk.strip()) < 40:
        return True
    low = chunk.lower()
    return any(s in low for s in _ISSUER_SUMMARY_SPAM)


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
    # Many S-1s label the person column "Executive Officer" (or similar) — not "Name".
    has_name_col = "name" in blob and "company name" not in blob
    has_age = bool(re.search(r"\bage\b", blob))
    has_pos = any(
        x in blob
        for x in (
            "position",
            "title",
            "office held",
            "principal",
            "officer",
            "director",
            "chair",
        )
    )
    has_exec_officer_hdr = any(
        h
        and len(h) < 80
        and re.search(
            r"\b(executive\s+officers?|officer\s+name|director\s+name)\b",
            h,
            re.I,
        )
        for h in headers
    )
    roster_like = (
        has_name_col
        or (has_age and has_pos)
        or (has_exec_officer_hdr and (has_age or has_pos))
    )
    if not roster_like:
        return False
    if has_name_col or (has_age and has_pos):
        return True
    if any(x in blob for x in ("position", "title", "officer", "director", "chair")):
        return True
    if has_age and ("position" in blob or "title" in blob):
        return True
    return False


def _column_header_key(h: str) -> str:
    """Lowercase header with footnote markers stripped (e.g. 'Age (1)' -> 'age')."""
    hl = h.lower().strip()
    return re.sub(r"\s*[\(\[]\s*\d+\s*[\)\]]\s*$", "", hl).strip()


def _column_map(headers: list[str]) -> tuple[Optional[int], Optional[int], Optional[int]]:
    """Map columns; avoid treating the 'Executive Officer' name column as 'position'."""
    name_i = age_i = None
    for i, h in enumerate(headers):
        hl = _column_header_key(h)
        if name_i is None and "name" in hl and "company" not in hl:
            name_i = i
        if name_i is None and re.search(
            r"\b(executive\s+officers?|officer\s+name|director\s+name)\b", hl
        ):
            name_i = i
        if age_i is None and re.match(r"^age\b", hl):
            age_i = i
    if name_i is None and headers:
        name_i = 0

    pos_i = None
    for i, h in enumerate(headers):
        if i == name_i:
            continue
        hl = _column_header_key(h)
        if re.search(
            r"\b(position|title|office\s+held|principal\s+occupation|"
            r"present\s+principal|principal\s+employment)\b",
            hl,
        ):
            pos_i = i
            break
    if pos_i is None and headers:
        others = [j for j in range(len(headers)) if j != name_i and j != age_i]
        if len(others) == 1:
            pos_i = others[0]
        elif len(headers) >= 2:
            pos_i = len(headers) - 1
    return name_i, pos_i, age_i


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

    def _parse_age(cell: str) -> Optional[int]:
        s = (cell or "").strip()
        m = re.search(r"\b(1[89]|[2-9]\d)\b", s)
        if not m:
            m = re.search(r"\b(100)\b", s)
        if not m:
            return None
        n = int(m.group(1))
        return n if 18 <= n <= 100 else None

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
        name_i, pos_i, age_i = _column_map(headers)
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
            age_val: Optional[int] = None
            if age_i is not None and len(cells) > age_i:
                age_val = _parse_age(cells[age_i])
            found.append((name, title, "management_section", age_val))

    by_name: dict[str, OfficerRow] = {}
    for name, title, src, age in found:
        key = " ".join(name.lower().split())
        prev = by_name.get(key)
        if prev is None:
            by_name[key] = (name, title, src, age)
        elif len(title) > len(prev[1]):
            by_name[key] = (name, title, src, age)
        elif len(title) == len(prev[1]) and age is not None and prev[3] is None:
            by_name[key] = (name, title, src, age)
    return sorted(by_name.values(), key=lambda x: (x[1], x[0]))


_SOURCE_RANK = {"management_section": 0, "signature_table": 1, "merged": 2}


def merge_officer_rows(*groups: list[OfficerRow]) -> list[OfficerRow]:
    """
    One row per person; prefer management_section over signature_table; then longer title.
    Pass arguments with management list first, then signature list.
    """
    by_key: dict[str, tuple[str, str, str, int, Optional[int]]] = {}
    for group in groups:
        for row in group:
            name, title, src = row[0], row[1], row[2]
            age = row[3] if len(row) > 3 else None
            key = " ".join(name.lower().replace(".", " ").split())
            rank = _SOURCE_RANK.get(src, 5)
            prev = by_key.get(key)
            if prev is None:
                by_key[key] = (name, title, src, rank, age)
                continue
            _, ptitle, _, prank, page = prev
            if rank < prank:
                by_key[key] = (name, title, src, rank, age if age is not None else page)
            elif rank == prank and len(title) > len(ptitle):
                by_key[key] = (name, title, src, rank, age if age is not None else page)
            elif rank == prank and len(title) == len(ptitle) and age is not None and page is None:
                by_key[key] = (name, title, src, rank, age)
    return sorted(
        [(n, t, s, ag) for n, t, s, _, ag in by_key.values()],
        key=lambda x: (x[1], x[0]),
    )


def extract_issuer_summary_from_filing_html(
    html: str, *, max_chars: int = 650
) -> str:
    """
    First filing-sourced blurb from Prospectus Summary / Company Overview (S-1).
    Plain-text heuristic; no external scraping.
    """
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    if not text:
        return ""

    def _take(m: re.Match[str] | None) -> Optional[str]:
        if not m:
            return None
        chunk = re.sub(r"\s+", " ", m.group(1)).strip()
        if len(chunk) < 40 or issuer_summary_looks_spammy(chunk):
            return None
        return (chunk[:max_chars] + "…") if len(chunk) > max_chars else chunk

    patterns = [
        r"prospectus\s+summary\s*[:\s]*\n*(.+?)(?:\n\s*\n|the\s+offering|risk\s+factors|implications\s+of\s+being|$)",
        r"summary\s+of\s+the\s+offering\s*[:\s]*\n*(.+?)(?:\n\s*\n|risk\s+factors|$)",
        r"company\s+overview\s*[:\s]*\n*(.+?)(?:\n\s*\n|risk\s+factors|the\s+offering|use\s+of\s+proceeds|dividend\s+policy|$)",
        r"(?:this\s+)?prospectus\s+relates\s+to\s+(.+?)(?:\n\s*\n|risk\s+factors|the\s+offering|$)",
    ]
    for pat in patterns:
        got = _take(re.search(pat, text, re.I | re.S))
        if got:
            return got

    # "Our Company — …" / "Company — …" using a real dash only (not "Company-based").
    m = re.search(
        r"(?:our\s+)?company\s+[—\u2013\u2014]\s*(.+?)(?:\n\s*\n|business|industry|$)",
        text,
        re.I | re.S,
    )
    got = _take(m)
    if got:
        return got

    # Early-document fallback: many S-1s put the story under Overview before comp sections.
    head = text[: min(len(text), 220_000)]
    m2 = re.search(
        r"company\s+overview\s*[:\s]*\n*(.+?)(?:\n\s*\n|risk\s+factors|the\s+offering|$)",
        head,
        re.I | re.S,
    )
    got = _take(m2)
    if got:
        return got
    return ""


_URL_IN_TEXT = re.compile(r"https?://[^\s\]\)\"'<>]+", re.I)
_WWW_RE = re.compile(r"\bwww\.[a-z0-9][a-z0-9.-]+\.[a-z]{2,}\b", re.I)
_HQ_RE = re.compile(
    r"(?:principal executive offices?|headquarters|headquartered|corporate offices?)\s*"
    r".{0,180}?"
    r"(?:located\s+at|located\s+in|based\s+in|situated\s+in|"
    r"are\s+located\s+at|are\s+located\s+in|are\s+at|are\s+in|"
    r"is\s+located\s+at|is\s+located\s+in|is\s+at|is\s+in|"
    r"are\s+the\s+following|is\s+the\s+following)\s*"
    r"([^.]{3,240})",
    re.I | re.S,
)
_HQ_PLACE = re.compile(
    r"(?:principal\s+place\s+of\s+business|registered\s+office|"
    r"mailing\s+address\s+of\s+our\s+principal|business\s+address)\s*[:-]?\s*"
    r"([^.]{4,240})",
    re.I | re.S,
)
_HQ_LINE_LABEL = re.compile(
    r"^(?:principal executive offices?|mailing address|business address|"
    r"address\s+of\s+principal)\s*:?\s*$",
    re.I,
)


def extract_issuer_website_from_filing_html(html: str) -> str:
    """First issuer-style external URL from filing body (heuristic; no outbound fetch)."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    skip = ("sec.gov", "edgar", "nasdaq.com", "nyse.com", "github.com")
    for m in _URL_IN_TEXT.finditer(text):
        u = m.group(0).rstrip(".,);")
        if any(s in u.lower() for s in skip):
            continue
        start = max(0, m.start() - 120)
        window = text[start : m.start()].lower()
        if any(
            k in window
            for k in (
                "website",
                "internet",
                "located at",
                "visit us",
                "visit our",
                "please visit",
                "see our",
            )
        ):
            return u[:300]
    m2 = _WWW_RE.search(text)
    if m2:
        start = max(0, m2.start() - 80)
        if "website" in text[start : m2.start()].lower():
            return "https://" + m2.group(0).lower().rstrip(".,)")
    return ""


def _hq_clean(chunk: str) -> str:
    s = re.sub(r"\s+", " ", (chunk or "").strip())
    s = re.sub(
        r"^(located\s+at|located\s+in|based\s+in|at|in)\s+",
        "",
        s,
        flags=re.I,
    )
    s = re.sub(r"\s+([\.,])", r"\1", s)
    return s[:240] if len(s) >= 4 else ""


def _extract_hq_from_cover_tables(soup: BeautifulSoup) -> str:
    """Cover / about-the-company tables often pair a label cell with the street address."""
    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            tds = [td.get_text(" ", strip=True) for td in tr.find_all(["th", "td"])]
            if len(tds) < 2:
                continue
            label = " ".join(tds[:-1]).lower()
            val = tds[-1].strip()
            if len(val) < 8:
                continue
            if (
                "principal" in label
                and ("executive" in label or "office" in label or "business" in label)
            ) or ("headquarters" in label and "address" not in label):
                if re.search(r"\d", val) or ("," in val and len(val) > 12):
                    return _hq_clean(val)
            if "address" in label and "principal" in label:
                if re.search(r"\d", val) or "," in val:
                    return _hq_clean(val)
    return ""


def extract_issuer_headquarters_from_filing_html(html: str) -> str:
    """Short HQ / principal office line from filing text and cover tables (heuristic)."""
    soup = BeautifulSoup(html, "html.parser")
    tab = _extract_hq_from_cover_tables(soup)
    if tab:
        return tab

    text_nl = soup.get_text("\n", strip=True)
    flat = re.sub(r"[\s\xa0]+", " ", soup.get_text(" ", strip=True))

    for body in (flat, text_nl):
        m = _HQ_RE.search(body)
        if m:
            got = _hq_clean(m.group(1))
            if got:
                return got
        m2 = _HQ_PLACE.search(body)
        if m2:
            got = _hq_clean(m2.group(1))
            if got:
                return got

    lines = [ln.strip() for ln in text_nl.splitlines() if ln.strip()]
    for i, ln in enumerate(lines[:-1]):
        if _HQ_LINE_LABEL.match(ln):
            nxt = lines[i + 1]
            if len(nxt) >= 8 and (re.search(r"\d", nxt) or "," in nxt):
                return _hq_clean(nxt)
    return ""


_SIC_DESC_LINE = re.compile(
    r"S\.?I\.?C\.?\s*(?:code)?\s*[:\.]?\s*(\d{3,4})\b\s*[-–—]?\s*([^\n\r<]{2,140})",
    re.I,
)
_SIC_CODE = re.compile(
    r"(?:Standard\s+Industrial\s+Classification|S\.?I\.?C\.?)\s*(?:\(?\s*SIC\s*\)?)?\s*"
    r"(?:code|number)?\s*[:\.]?\s*(\d{3,4})\b",
    re.I,
)
_NAICS_LINE = re.compile(
    r"NAICS\s*(?:code)?\s*(?:is|:|\.|,)?\s*(\d{4,6})\b\s*(?:[-–—]?\s*([^\n\r<]{2,140}))?",
    re.I,
)


def extract_issuer_industry_from_filing_html(html: str) -> str:
    """
    Best-effort SIC / NAICS line from filing body text (heuristic; many filers vary wording).
    """
    soup = BeautifulSoup(html, "html.parser")
    flat = re.sub(r"[\s\xa0]+", " ", soup.get_text(" ", strip=True))
    if len(flat) < 24:
        return ""

    m = _NAICS_LINE.search(flat)
    if m:
        code = m.group(1)
        desc = (m.group(2) or "").strip()
        if desc:
            desc = re.split(r"[.;]|\s{2,}", desc, maxsplit=1)[0].strip()
        if desc.lower().startswith("code") or "sic code" in desc.lower():
            desc = ""
        line = f"NAICS {code}"
        if desc and 3 < len(desc) < 100:
            line += f" — {desc[:120]}"
        return line[:300]

    m2 = _SIC_DESC_LINE.search(flat)
    if m2:
        code, desc = m2.group(1), (m2.group(2) or "").strip()
        if desc.lower().startswith("code"):
            desc = ""
        line = f"SIC {code}"
        if desc and len(desc) > 3:
            line += f" — {desc[:120]}"
        return line[:300]

    m3 = _SIC_CODE.search(flat)
    if m3:
        return f"SIC {m3.group(1)}"[:300]
    return ""


def why_surfaced_line(form_type: str, filing_date: Optional[str]) -> str:
    """Single-line, filing-based timing context (not investment advice)."""
    ft = (form_type or "Filing").strip()
    fd = filing_date or "date unknown"
    amend = bool(re.search(r"/\s*A\b", ft, re.I))
    tail = " (amendment)" if amend else ""
    return f"{ft} filed {fd}{tail} — public registration activity in EDGAR."
