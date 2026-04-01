from __future__ import annotations

import re
from typing import Optional

from bs4 import BeautifulSoup

from wealth_leads.officers import _clean_name, _clean_title
from wealth_leads.person_quality import (
    is_acceptable_lead_person_name,
    refine_lead_person_name,
)
from wealth_leads.territory import (
    is_plausible_registrant_headquarters,
    normalize_registrant_hq_address_blob,
)

OfficerRow = tuple[str, str, str, Optional[int]]  # name, title, source, age (management tables)

# Long TOC / inline-XBRL noise can push the cover page past 50k plain-text chars.
_HQ_EARLY_TEXT_CHARS = 120_000

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
        if age_i is None and (
            re.match(r"^years?\b", hl) or re.match(r"^approx\.?\s+age\b", hl)
        ):
            age_i = i
    if name_i is None and headers:
        name_i = 0

    pos_i = None
    for i, h in enumerate(headers):
        if i == name_i:
            continue
        hl = _column_header_key(h)
        # S-1s almost always use "Positions Held" / "Position" — \bposition\b misses "positions".
        if re.search(
            r"\b(positions?|titles?|office\s+held|principal\s+occupation|"
            r"present\s+principal|principal\s+employment)\b",
            hl,
        ):
            pos_i = i
            break
    if pos_i is None and headers:
        for i, h in enumerate(headers):
            if i == name_i or i == age_i:
                continue
            hl = _column_header_key(h)
            if "position" in hl or "title" in hl:
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
        """18–100; do not pick ``20`` out of ``2024``."""
        s = (cell or "").strip()
        if not s:
            return None
        if re.match(r"^(1[89]|[2-9]\d|100)$", s):
            return int(s)
        m_full = re.match(r"^(1[89]|[2-9]\d|100)\s*[\(\[].*[\)\]]$", s)
        if m_full:
            n = int(m_full.group(1))
            return n if 18 <= n <= 100 else None
        for m in re.finditer(r"(?<!\d)(1[89]|[2-9]\d|100)(?!\d)", s):
            n = int(m.group(1))
            if 18 <= n <= 100:
                return n
        return None

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
        if age_i is None and len(headers) <= 8:
            others = [j for j in range(len(headers)) if j != name_i and j != pos_i]
            if len(others) == 1:
                age_i = others[0]
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
            if (not name or not title) or (
                bool(name)
                and bool(title)
                and not is_acceptable_lead_person_name(name)
            ):
                alt_name = _clean_name_table(title_raw)
                alt_title = _clean_title_table(name_raw)
                if (
                    alt_name
                    and alt_title
                    and is_acceptable_lead_person_name(alt_name)
                ):
                    name, title = alt_name, alt_title
            if name and not is_acceptable_lead_person_name(name):
                refined = refine_lead_person_name(name_raw) or refine_lead_person_name(
                    title_raw
                )
                if refined:
                    name = refined
            if not name or not title:
                continue
            if not is_acceptable_lead_person_name(name):
                continue
            age_val: Optional[int] = None
            if age_i is not None and len(cells) > age_i:
                age_val = _parse_age(cells[age_i])
            if age_val is None and name_i is not None and pos_i is not None:
                candidates: list[int] = []
                for j, cell in enumerate(cells):
                    if j in (name_i, pos_i):
                        continue
                    c = (cell or "").strip()
                    if len(c) > 14:
                        continue
                    pv = _parse_age(cell)
                    if pv is not None:
                        candidates.append(pv)
                if len(candidates) == 1:
                    age_val = candidates[0]
            found.append((name, title, "management_section", age_val))

    by_name: dict[str, OfficerRow] = {}
    for name, title, src, age in found:
        key = " ".join(name.lower().split())
        prev = by_name.get(key)
        if prev is None:
            by_name[key] = (name, title, src, age)
        elif len(title) > len(prev[1]):
            keep_age = age if age is not None else prev[3]
            by_name[key] = (name, title, src, keep_age)
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
# Same heading but address immediately after colon on one line (common on cover HTML).
_HQ_RE_COLON = re.compile(
    r"(?:our\s+)?principal\s+executive\s+offices?\s*:\s*([^\n]{8,400})",
    re.I,
)
_HQ_PLACE = re.compile(
    r"(?:principal\s+place\s+of\s+business|registered\s+office|"
    r"mailing\s+address\s+of\s+our\s+principal|business\s+address)\s*[:-]?\s*"
    r"([^.]{4,240})",
    re.I | re.S,
)
_HQ_LINE_LABEL = re.compile(
    r"^(?:principal executive offices?|our\s+principal\s+executive\s+offices?|"
    r"corporate\s+headquarters|mailing address|business address|"
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
    # Sentence-style prospectus lines: "…Houston, TX, and our telephone number at that address is …"
    s = re.sub(
        r",?\s*and\s+our\s+telephone\s+number\s+at\s+(?:that|this)\s+address\s+is\s+.+$",
        "",
        s,
        flags=re.I,
    )
    s = re.sub(r",?\s*and\s+the\s+telephone\s+number\s+(?:at|there)\s+.+$", "", s, flags=re.I)
    s = normalize_registrant_hq_address_blob(s)
    s = s.rstrip(" ,")
    return s[:500] if len(s) >= 4 else ""


# S-1 cover: street / city / phone first, then this caption (label is *below* the block).
_HQ_SEC_REGISTRANT_ADDRESS_CAPTION = re.compile(
    r"\(\s*Address,\s+including\s+zip\s+code[^)]{0,260}"
    r"principal\s+executive\s+offices?\s*\)",
    re.I | re.S,
)

_HQ_COVER_PHONE_ONLY = re.compile(
    r"^[\s(]*(?:\+?1[\s.-]*)?\(?\d{3}\)?[\s.-]*\d{3}[\s.-]*\d{4}[\s)]*$"
)


def _hq_cover_line_skip(ln: str) -> bool:
    """Skip phone / fax lines and EIN-like numeric lines on SEC covers (not street numbers)."""
    s = (ln or "").replace("\xa0", " ").strip()
    if not s or len(s) <= 1:
        return True
    low = s.lower()
    if _HQ_COVER_PHONE_ONLY.match(s):
        return True
    if re.match(
        r"^(telephone|phone|fax|facsimile|tel\.|toll[\s-]?free)\s*[:.]?\s*",
        low,
    ):
        return True
    if re.match(r"^\(\s*\d{3}\s*\)\s*$", s):
        return True
    # I.R.S. Employer Identification No. value (do not treat as phone)
    if re.match(r"^\d{2}-\d{7}\s*$", s):
        return True
    return False


def _hq_cover_line_is_probable_person_name_only(ln: str) -> bool:
    """Bare 'Firstname Lastname' lines above the street on some covers (no title on same line)."""
    s = (ln or "").replace("\xa0", " ").strip()
    if not s or len(s) > 72 or re.search(r"\d", s):
        return False
    if re.search(
        r"\b(street|st\.|avenue|suite|president|director|chief|officer|secretary|treasurer)\b",
        s.lower(),
    ):
        return False
    return bool(re.match(r"^[A-Z][a-z]+(?:\s+[A-Z][a-z'.-]+){1,4}$", s))


def _hq_cover_line_is_officer_filler(ln: str) -> bool:
    """
    True for 'Name, President and CEO' lines some S-1s stack above the street on the cover.
    When scanning upward from the registrant-address caption, stop before those lines.
    When scanning downward after a 'Principal executive offices' label, skip them.
    """
    s = (ln or "").replace("\xa0", " ").strip()
    if not s:
        return False
    low = s.lower()
    if re.search(r"\d", s):
        return False
    if re.search(
        r"\b(street|st\.|avenue|ave\.|boulevard|blvd|road|rd\.|drive|dr\.|lane|ln\.|suite|ste\.|"
        r"floor|fl\.|room|rm\.|building|bldg|plaza|highway|hwy|way|circle|cir\.|"
        r"parkway|route|rt\.|county|region|province)\b",
        low,
    ):
        return False
    return bool(
        re.search(
            r"\b(?:president|chief\s+executive|chief\s+financial|chief\s+accounting|"
            r"treasurer|secretary|vice\s+president|chief\s+operating|general\s+counsel|"
            r"chairman|chairperson|director|executive\s+officer)\b",
            low,
        )
    )


def _hq_from_sec_registrant_address_caption(head_text: str) -> str:
    """
    SEC Form cover often lists the principal office street/city/phone, then the parenthetical
    '(Address, including zip code, … of registrant's principal executive offices)'.
    Our 'label above address' heuristics miss that; this reads the lines immediately above the caption.
    """
    m = _HQ_SEC_REGISTRANT_ADDRESS_CAPTION.search(head_text)
    if not m:
        return ""
    before = head_text[: m.start()].rstrip()
    lines = [ln.strip() for ln in before.splitlines() if ln.strip()]
    if not lines:
        return ""
    collected: list[str] = []
    for ln in reversed(lines[-14:]):
        if _hq_cover_line_skip(ln):
            continue
        low = ln.lower()
        if low in ("(", ")") or len(ln) == 1:
            continue
        if re.match(
            r"^(table of contents|form s-\d|united states|securities and exchange commission)\b",
            low,
        ):
            break
        if re.match(r"^\(?i\.r\.s\.", low) or "identification no" in low:
            break
        if low.startswith("(primary standard") or "classification code number" in low:
            break
        if re.match(
            r"^(exact name of registrant|state or other jurisdiction|of incorporation)\b",
            low,
        ):
            break
        if _hq_cover_line_is_officer_filler(ln):
            break
        if _hq_cover_line_is_probable_person_name_only(ln):
            continue
        collected.append(ln)
        if len(collected) >= 4:
            break
    if len(collected) < 2:
        if len(collected) == 1 and re.search(r"\d", collected[0]) and "," in collected[0]:
            got = _hq_clean(collected[0])
            return got if len(got) >= 10 else ""
        return ""
    street_city = list(reversed(collected))
    joined = ", ".join(street_city)
    got = _hq_clean(joined)
    if got and len(got) >= 10 and re.search(r"\d", got):
        return got
    return ""


# iXBRL-heavy S-1 covers: street / city / state lines, then "(Address of principal executive offices)",
# then ZIP on its own line (no "Address, including zip code…" caption in plain text).
_ADDR_OF_PRINC_EXEC_COVER = re.compile(
    r"^\(\s*Address\s+of\s+principal\s+executive\s+offices?\s*\)\s*$",
    re.I,
)


def _hq_ixbrl_cover_before_address_of_principal(head_text: str) -> str:
    lines = [ln.strip() for ln in head_text.splitlines() if ln.strip()]
    for i, ln in enumerate(lines):
        if not _ADDR_OF_PRINC_EXEC_COVER.match(ln):
            continue
        stop = re.compile(
            r"^(?:\(?(?:Exact\s+Name|I\.R\.S\.|Employer|Identification|Primary\s+Standard)|"
            r"State\s+or\s+other\s+jurisdiction|of\s+incorporation)",
            re.I,
        )
        chunks: list[str] = []
        for j in range(i - 1, max(-1, i - 22), -1):
            x = lines[j].strip()
            if _hq_cover_line_skip(x):
                continue
            if x in (",", "—", "-", "–"):
                continue
            if stop.match(x):
                break
            low = x.lower()
            if low.startswith("(exact name") or "as specified in its charter" in low:
                break
            if low.startswith("(state or other") or low.startswith("(primary standard"):
                break
            if low.startswith("(i.r.s") or "identification no" in low:
                break
            if _hq_cover_line_is_officer_filler(x):
                break
            if _hq_cover_line_is_probable_person_name_only(x):
                continue
            chunks.append(x)
        if len(chunks) < 2:
            continue
        parts = list(reversed(chunks))
        joined = ", ".join(parts)
        zip_tail = ""
        if i + 1 < len(lines) and re.match(r"^\d{5}(?:-\d{4})?$", lines[i + 1]):
            zip_tail = " " + lines[i + 1]
        got = _hq_clean(joined + zip_tail)
        if got and len(got) >= 12 and re.search(r"\d", got):
            return got
    return ""


_HQ_LABEL_INLINE = re.compile(
    r"(?im)^(principal\s+executive\s+offices?|our\s+principal\s+executive\s+offices?|"
    r"corporate\s+headquarters)\s*:\s*(.+)$",
)


def _hq_multiline_after_principal_label(head_text: str) -> str:
    """
    Cover / summary pages often use:
      Principal executive offices
      123 Main Street
      City, ST 12345
    (no 'located at' — our old regex missed that.)
    """
    lines = [ln.strip() for ln in head_text.splitlines() if ln.strip()]
    label_only = re.compile(
        r"^(principal\s+executive\s+offices?|our\s+principal\s+executive\s+offices?|"
        r"corporate\s+headquarters|address\s+of\s+principal\s+executive\s+offices?)\s*:?\s*$",
        re.I,
    )
    label_with_value = re.compile(
        r"^(principal\s+executive\s+offices?|our\s+principal\s+executive\s+offices?|"
        r"corporate\s+headquarters)\s*:\s*(.+)$",
        re.I,
    )
    stop = re.compile(
        r"^(telephone|phone|fax|toll[\s-]?free|website|e-?mail|internet\s+address|"
        r"table\s+of\s+contents|prospectus\s+summary|the\s+offering|risk\s+factors|"
        r"part\s+i\b|item\s+1\.?\b)",
        re.I,
    )
    for i, ln in enumerate(lines):
        mv = label_with_value.match(ln)
        if mv:
            got = _hq_clean(mv.group(2))
            if got and len(got) >= 10 and (re.search(r"\d", got) or "," in got):
                return got
        if not label_only.match(ln):
            continue
        parts: list[str] = []
        for j in range(i + 1, min(i + 6, len(lines))):
            nx = lines[j]
            if stop.match(nx):
                break
            if len(nx) < 2:
                continue
            if _hq_cover_line_is_officer_filler(nx):
                continue
            if nx.isupper() and len(nx) < 50 and not re.search(r"\d", nx):
                break
            parts.append(nx)
        if not parts:
            continue
        joined = " ".join(parts)
        if sum(1 for p in parts if "," in p) >= 1:
            joined = ", ".join(p.strip() for p in parts)
        got = _hq_clean(joined)
        if got and len(got) >= 10 and (re.search(r"\d", got) or "," in got):
            return got
    return ""


def _hq_from_early_document_text(text_nl: str) -> str:
    """Early plain text: SEC cover caption block, iXBRL cover, inline label, multiline label + address."""
    if not text_nl:
        return ""
    head = text_nl[: min(len(text_nl), _HQ_EARLY_TEXT_CHARS)]
    cap = _hq_from_sec_registrant_address_caption(head)
    if cap:
        return cap
    ix = _hq_ixbrl_cover_before_address_of_principal(head)
    if ix:
        return ix
    for m in _HQ_LABEL_INLINE.finditer(head):
        got = _hq_clean((m.group(2) or "").strip())
        if got and len(got) >= 10 and (re.search(r"\d", got) or "," in got):
            return got
    return _hq_multiline_after_principal_label(head)


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
            if "corporate" in label and "headquarters" in label:
                if re.search(r"\d", val) or ("," in val and len(val) > 10):
                    return _hq_clean(val)
            if "address" in label and "principal" in label:
                if re.search(r"\d", val) or "," in val:
                    return _hq_clean(val)
    return ""


def extract_issuer_headquarters_from_filing_html(html: str) -> str:
    """Short HQ / principal office line from filing text and cover tables (heuristic)."""
    soup = BeautifulSoup(html, "html.parser")
    tab = _extract_hq_from_cover_tables(soup)
    if tab and is_plausible_registrant_headquarters(tab):
        return tab

    text_nl = soup.get_text("\n", strip=True)
    early = _hq_from_early_document_text(text_nl)
    if early and is_plausible_registrant_headquarters(early):
        return early

    flat = re.sub(r"[\s\xa0]+", " ", soup.get_text(" ", strip=True))
    head_flat = flat[: min(len(flat), _HQ_EARLY_TEXT_CHARS)]
    mc = _HQ_RE_COLON.search(head_flat)
    if mc:
        got = _hq_clean(mc.group(1))
        if got and (re.search(r"\d", got) or "," in got):
            if is_plausible_registrant_headquarters(got):
                return got

    for body in (flat, text_nl):
        m = _HQ_RE.search(body)
        if m:
            got = _hq_clean(m.group(1))
            if got and is_plausible_registrant_headquarters(got):
                return got
        m2 = _HQ_PLACE.search(body)
        if m2:
            got = _hq_clean(m2.group(1))
            if got and is_plausible_registrant_headquarters(got):
                return got

    lines = [ln.strip() for ln in text_nl.splitlines() if ln.strip()]
    for i, ln in enumerate(lines[:-1]):
        if _HQ_LINE_LABEL.match(ln):
            nxt = lines[i + 1]
            if len(nxt) >= 8 and (re.search(r"\d", nxt) or "," in nxt):
                got = _hq_clean(nxt)
                if is_plausible_registrant_headquarters(got):
                    return got
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
    SIC / NAICS from registration-statement prose (standard disclosure; wording varies).

    Returns a short line like ``NAICS 541511 — …`` or ``SIC 7372`` when the HTML text
    matches common patterns; empty string when not found (no narrative guess).
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


def _revenue_amount_plausible(
    amt: str, *, from_labeled_revenue_row: bool = False
) -> bool:
    """Reject obvious per-share / EPS fragments; relax rules for table rows we labeled as revenue."""
    low = (amt or "").lower()
    if not amt or "per share" in low or re.search(r"\bper\s+common\s+share\b", low):
        return False
    nums = re.sub(r"[^\d.]", "", amt.replace(",", ""))
    try:
        v = float(nums) if nums else 0.0
    except ValueError:
        return False
    if any(x in low for x in ("million", "billion", "thousand", " mm", " bn")):
        return v > 0
    if from_labeled_revenue_row:
        # Labeled revenue row: bare $ usually full-year total; avoid tiny false positives
        return v >= 100_000
    return v >= 5_000


def _row_first_cell_is_revenue_line(label: str) -> bool:
    """First column of a financial table row — revenue / sales, not cost or subtotal."""
    t = re.sub(r"\s+", " ", (label or "").strip())
    t_low = re.sub(r"\[\d+\]|\(\d+\)", "", t.lower()).strip()
    if not t_low or len(t_low) > 120:
        return False
    if any(
        x in t_low
        for x in (
            "cost of revenue",
            "cost of sales",
            "cost of goods",
            "gross profit",
            "gross margin",
            "operating expenses",
            "operating income",
            "net loss",
            "net income",
            "ebitda",
            "adjusted ebitda",
            "interest expense",
            "income tax",
            "weighted average",
            "basic and diluted",
            "per share",
            "percentage",
            "not meaningful",
            "three months",
            "six months",
            "nine months",
        )
    ):
        return False
    if re.match(
        r"^(?:(?:total|net|gross)\s+)?revenu(?:e|es)?(?:\s*,\s*net)?$",
        t_low,
    ):
        return True
    if re.match(r"^total\s+revenu", t_low):
        return True
    if re.match(r"^net\s+revenu", t_low):
        return True
    if re.match(r"^(?:net\s+)?sales$", t_low):
        return True
    if re.match(r"^subscription\s+revenu", t_low) and "cost" not in t_low:
        return True
    if re.match(r"^(?:product|service)\s+revenu", t_low):
        return True
    if re.match(r"^operating\s+revenu", t_low):
        return True
    if re.match(r"^contract\s+revenu", t_low):
        return True
    return False


def _first_currency_amount_in_row(cells: list[str]) -> str:
    """Prefer the first data column (often most recent fiscal year in SFD)."""
    for cell in cells[1:]:
        s = re.sub(r"\[\d+\]", "", cell)
        s = re.sub(r"\s+", " ", s).strip()
        if not s or s in ("—", "-", "–", "N/A", "n/a", "*"):
            continue
        # Parentheses = negative; still revenue magnitude for young cos — skip negatives
        if re.match(r"^\([^)]+\)$", s.replace("$", "").strip()):
            continue
        m = re.search(
            r"(\$\s*[\d,]+(?:\.\d+)?(?:\s*(?:million|billion|thousand|M|B))?)",
            s,
            re.I,
        )
        if m:
            amt = re.sub(r"\s+", " ", m.group(1).strip())
            if _revenue_amount_plausible(amt, from_labeled_revenue_row=True):
                return amt
    return ""


def _extract_revenue_amount_from_s1_tables(soup: BeautifulSoup) -> str:
    """
    Selected financial data / statements of operations: row labeled Revenue or Sales with $ in another cell.
    """
    for tr in soup.find_all("tr"):
        cells = [
            re.sub(r"\s+", " ", td.get_text(" ", strip=True))
            for td in tr.find_all(["td", "th"])
        ]
        if len(cells) < 2:
            continue
        if not _row_first_cell_is_revenue_line(cells[0]):
            continue
        got = _first_currency_amount_in_row(cells)
        if got:
            return got
    return ""


def extract_issuer_revenue_line_from_filing_html(html: str) -> str:
    """
    Consolidated annual revenue from S-1 / F-1 HTML without XBRL.

    Order: (1) HTML tables with a revenue/sales row, (2) MD&A and summary prose patterns.
    Nearly all registration statements disclose revenue in prose and/or selected financial data.
    """
    soup = BeautifulSoup(html, "html.parser")
    tab = _extract_revenue_amount_from_s1_tables(soup)
    if tab:
        return f"Annual revenue {tab} (per registration filing financial data)."[:400]

    flat = re.sub(r"[\s\xa0]+", " ", soup.get_text(" ", strip=True))
    if len(flat) < 200:
        return ""
    head = flat[: min(len(flat), 620_000)]

    patterns = [
        r"(?:total\s+)?revenu(?:e|es)\s+(?:was|were)\s+"
        r"((?:approximately\s+|about\s+)?(?:\$|US\$|USD\s*)?[\d,.]+(?:\s*(?:million|billion|thousand|M|B))?)"
        r"(?:\s+for\s+the\s+(?:fiscal\s+)?year\s+ended\s+[^,\.]{4,52})?",
        r"(?:total\s+)?revenu(?:e|es)\s+of\s+"
        r"((?:approximately\s+|about\s+)?(?:\$|US\$|USD\s*)?[\d,.]+(?:\s*(?:million|billion|thousand))?)",
        r"(?:generated|recognized|reported)\s+(?:total\s+)?revenu(?:e|es)\s+of\s+"
        r"((?:\$|US\$|USD\s*)?[\d,.]+(?:\s*(?:million|billion|thousand))?)",
        r"net\s+revenu(?:e|es)\s+(?:was|were)\s+"
        r"((?:approximately\s+)?(?:\$|US\$|USD\s*)?[\d,.]+(?:\s*(?:million|billion|thousand))?)",
        r"operating\s+revenu(?:e|es)\s+(?:was|were)\s+"
        r"((?:\$|US\$|USD\s*)?[\d,.]+(?:\s*(?:million|billion|thousand))?)",
        r"(?:net\s+)?sales\s+(?:were|was)\s+"
        r"((?:approximately\s+)?(?:\$|US\$|USD\s*)?[\d,.]+(?:\s*(?:million|billion|thousand))?)",
        r"total\s+net\s+revenu(?:e|es)\s+(?:of|were|was)\s+"
        r"((?:\$|US\$|USD\s*)?[\d,.]+(?:\s*(?:million|billion|thousand))?)",
        r"(?:recorded|achieved|delivered)\s+(?:total\s+)?revenu(?:e|es)\s+of\s+"
        r"((?:\$|US\$|USD\s*)?[\d,.]+(?:\s*(?:million|billion|thousand))?)",
        # "For the year ended ... , revenue totaled $X"
        r"for\s+the\s+(?:fiscal\s+)?year\s+ended[^$]{6,72}?"
        r"(?:total\s+)?revenu(?:e|es)\s+(?:of|totaling|totaled|was|were)\s+"
        r"((?:\$|US\$|USD\s*)?[\d,.]+(?:\s*(?:million|billion|thousand))?)",
        r"for\s+the\s+years?\s+ended[^$]{8,120}?"
        r"(?:total\s+)?revenu(?:e|es)\s+of\s+"
        r"((?:\$|US\$|USD\s*)?[\d,.]+(?:\s*(?:million|billion|thousand))?)",
    ]
    for pat in patterns:
        m = re.search(pat, head, re.I)
        if not m:
            continue
        amt = re.sub(r"\s+", " ", m.group(1).strip())
        if not _revenue_amount_plausible(amt):
            continue
        return f"Annual revenue was {amt} (disclosed in registration filing)."[:400]
    return ""


def why_surfaced_line(form_type: str, filing_date: Optional[str]) -> str:
    """Single-line filing hook for the desk (not investment advice)."""
    ft = (form_type or "Filing").strip()
    fd = filing_date or "?"
    amend = bool(re.search(r"/\s*A\b", ft, re.I))
    tail = " · A" if amend else ""
    return f"{ft} · {fd}{tail}"
