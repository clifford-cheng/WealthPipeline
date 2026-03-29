"""
Parse per-executive narrative blocks under Management (many S-1 layouts).
Also extract issuer-wide director term / board election language when present.
"""
from __future__ import annotations

import re
from typing import Iterator, Optional

from bs4 import BeautifulSoup, Tag

_STOP_BEFORE_COMP = re.compile(
    r"^(summary\s+compensation|compensation\s+discussion|corporate\s+governance|"
    r"executive\s+compensation|director\s+compensation|pay\s+versus\s+performance|"
    r"outstanding\s+equity|grants\s+of\s+plan)",
    re.I,
)

_UPPER_STOP = frozenset(
    (
        "COMPENSATION",
        "GOVERNANCE",
        "RELATIONSHIPS",
        "SECURITY OWNERSHIP",
    )
)


def _norm_key(name: str) -> str:
    s = (name or "").lower().replace(".", " ")
    return " ".join(s.split())


def extract_age_from_bio_text(bio: str, *, window: int = 900) -> Optional[int]:
    """
    Ages often appear in the first paragraph: 'age 45', ', 45, has served', '(age 45)', etc.
    """
    if not bio:
        return None
    s = bio[:window]
    pats = [
        re.compile(r"\bage\s*[:\s,]+\s*(1[89]|[2-9]\d|100)\b", re.I),
        re.compile(r"\b(1[89]|[2-9]\d|100)\s+years?\s+old\b", re.I),
        re.compile(r"\(\s*age\s*(1[89]|[2-9]\d|100)\s*\)", re.I),
        re.compile(r",\s*(1[89]|[2-9]\d|100)\s*,\s*(?:has|had|was|is)\s+", re.I),
    ]
    for pat in pats:
        m = pat.search(s)
        if m:
            try:
                n = int(m.group(1))
                if 18 <= n <= 100:
                    return n
            except (TypeError, ValueError):
                continue
    return None


def _match_bio_lead(t: str) -> Optional[tuple[str, str, int]]:
    """
    Return (display_name, role_heading, start_index_of_body) where body includes honorific opener.
    """
    t = t.strip()
    if len(t) < 45:
        return None

    # 1) Name — Role . Mr./Ms. …
    m = re.match(
        r"^(.+?)\s*[—\u2013\u2014\-]\s*(.+?)\s*\.\s+(Mr\.|Ms\.|Mrs\.|Dr\.)\s*",
        t,
        re.UNICODE,
    )
    if m:
        return (m.group(1).strip(), m.group(2).strip(), m.start(3))

    # 2) Name, Role. Mr./Ms. …
    m = re.match(
        r"^(.+?),\s*([^,]{2,160}?)\.\s+(Mr\.|Ms\.|Mrs\.|Dr\.)\s*",
        t,
        re.UNICODE,
    )
    if m:
        return (m.group(1).strip(), m.group(2).strip(), m.start(3))

    # 3) Name — Role Mr./Ms. … (no period before honorific)
    m = re.match(
        r"^(.+?)\s*[—\u2013\u2014\-]\s*(.+?)\s+(Mr\.|Ms\.|Mrs\.|Dr\.)\s+",
        t,
        re.UNICODE,
    )
    if m:
        return (m.group(1).strip(), m.group(2).strip(), m.start(3))

    # 4) Mr./Ms. Name — Role. …
    m = re.match(
        r"^(Mr\.|Ms\.|Mrs\.|Dr\.)\s+([A-Za-z\.\s,'-]{2,80}?)\s*[—\u2013\u2014\-]\s*(.+?)\.\s+",
        t,
        re.UNICODE,
    )
    if m:
        name = f"{m.group(1)} {m.group(2).strip()}".strip()
        role = m.group(3).strip()
        body_start = m.end(0)
        if body_start < len(t):
            return (name, role, body_start)

    # 5) Name has served / has been / has worked as …
    m = re.match(
        r"^([A-Z][^\n—]{2,100}?)\s+has\s+(?:served|been|worked)\s+(?:as\s+)?(.+?)(?:[.;]\s+|\s+since\s+|\s+from\s+)",
        t,
        re.I | re.UNICODE,
    )
    if m:
        name = m.group(1).strip()
        role = m.group(2).strip()
        if len(name) >= 3 and len(role) >= 3:
            # Keep the full paragraph (including the lead sentence) as the bio body.
            return (name, role[:200], m.start(0))

    return None


def _should_stop_block(head: str) -> bool:
    h = head.strip()[:120]
    if _STOP_BEFORE_COMP.match(h):
        return True
    if h.isupper() and len(h) < 90 and " " in h and "TABLE" not in h:
        if any(x in h for x in _UPPER_STOP):
            return True
    return False


def _iter_management_blocks(anchor: Tag) -> Iterator[str]:
    """Walk block-level nodes after the section anchor (not only <p>)."""
    for el in anchor.find_all_next(["p", "div", "font"]):
        if el.find_parent("table"):
            continue
        if el.name == "div" and el.find("p") is not None:
            continue
        txt = el.get_text(" ", strip=True)
        if not txt:
            continue
        if el.name == "div" and len(txt) < 60:
            continue
        if el.name == "font" and len(txt) < 45:
            continue
        head = txt[:120]
        if _should_stop_block(head):
            return
        yield txt


def _find_management_anchor(soup: BeautifulSoup) -> Optional[Tag]:
    """
    Locate the real Management / roster heading — avoid TOC-only 'Management' lines.
    """
    patterns = [
        re.compile(r"executive\s+officers?\s+and\s+directors", re.I),
        re.compile(r"directors?\s+and\s+executive\s+officers", re.I),
        re.compile(r"executive\s+officers?\s+and\s+board", re.I),
    ]
    for pat in patterns:
        hit = soup.find(string=pat)
        if not hit:
            continue
        cur = hit.parent
        while cur is not None and getattr(cur, "name", None) not in ("p", "div", "font", "td", "th"):
            cur = cur.parent
        if cur is not None:
            return cur
    for p in soup.find_all("p"):
        t = p.get_text(" ", strip=True).lower()
        if "executive officers and directors" in t or "directors and executive officers" in t:
            return p
    return None


def _parse_bio_blocks(blocks: list[str]) -> list[dict]:
    results: list[dict] = []
    i = 0
    while i < len(blocks):
        t = blocks[i]
        m = _match_bio_lead(t)
        if not m:
            i += 1
            continue
        display_name, role_heading, start_body = m
        dn = display_name.strip()
        if not dn or dn.startswith(".") or len(dn) > 100:
            i += 1
            continue
        display_name = dn
        body = t[start_body:].strip()
        j = i + 1
        while j < len(blocks):
            nt = blocks[j]
            if not nt.strip():
                j += 1
                continue
            if _match_bio_lead(nt):
                break
            body = f"{body}\n\n{nt}".strip()
            j += 1
        if len(body) < 35:
            i = j
            continue
        nk = _norm_key(display_name)
        if nk:
            results.append(
                {
                    "person_name": display_name,
                    "person_name_norm": nk,
                    "role_heading": role_heading,
                    "bio_text": body,
                }
            )
        i = j
    return results


def _plaintext_bio_paragraphs(html: str) -> list[str]:
    """Fallback when iXBRL / div layout skipped <p> sequencing."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    lt = text.lower()
    starts: list[int] = []
    for needle in (
        "executive officers and directors",
        "directors and executive officers",
    ):
        i = lt.find(needle)
        if i >= 0:
            starts.append(i)
    if not starts:
        return []
    start = min(starts)
    chunk = text[start:]
    low = chunk.lower()
    end = len(chunk)
    for stop in (
        "\nsummary compensation",
        "\ncompensation discussion",
        "\ncorporate governance",
        "\npay versus performance",
        "\nsecurity ownership of certain",
        "\nsecurity ownership",
    ):
        j = low.find(stop)
        if j > 500:
            end = min(end, j)
    chunk = chunk[:end]
    out: list[str] = []
    for block in re.split(r"\n\s*\n+", chunk):
        b = block.strip()
        if len(b) >= 55 and not _should_stop_block(b[:120]):
            out.append(b)
    return out


def _merge_bio_dicts(rows: list[dict]) -> list[dict]:
    byk: dict[str, dict] = {}
    for r in rows:
        k = r.get("person_name_norm") or ""
        if not k:
            continue
        prev = byk.get(k)
        if prev is None or len(r.get("bio_text") or "") > len(prev.get("bio_text") or ""):
            byk[k] = r
    return sorted(byk.values(), key=lambda x: (x.get("role_heading") or "", x.get("person_name") or ""))


def extract_management_biographies_from_filing_html(html: str) -> list[dict]:
    """
    Returns dicts: person_name, person_name_norm, role_heading, bio_text.
    bio_text includes career / prior-role narrative (often covers “last five years” in prose).
    """
    soup = BeautifulSoup(html, "html.parser")
    anchor = _find_management_anchor(soup)
    merged: list[dict] = []
    if anchor is not None:
        merged.extend(_parse_bio_blocks(list(_iter_management_blocks(anchor))))
    plain = _parse_bio_blocks(_plaintext_bio_paragraphs(html))
    merged.extend(plain)
    out = _merge_bio_dicts(merged)
    return out


def extract_director_term_summary_from_filing_html(html: str) -> str:
    """
    Best-effort: sentences about how long directors serve, classes, annual election, etc.
    """
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    candidates: list[str] = []
    for para in text.split("\n"):
        p = para.strip()
        if len(p) < 50 or len(p) > 900:
            continue
        pl = p.lower()
        if "director" not in pl:
            continue
        if not any(
            k in pl
            for k in (
                "annual",
                "class",
                "term",
                "election",
                "re-election",
                "reelection",
                "staggered",
                "until",
                "meeting",
                "expire",
                "vacancy",
            )
        ):
            continue
        if "long-term incentive" in pl or "rsu" in pl or "stock option" in pl:
            continue
        if "cumulative voting" in pl:
            continue
        candidates.append(p)
    if not candidates:
        return ""

    def _score(para: str) -> int:
        pl = para.lower()
        s = len(para)
        if "annual" in pl and "director" in pl:
            s += 500
        if "re-election" in pl or "reelection" in pl:
            s += 200
        if "class" in pl and "director" in pl:
            s += 150
        if "staggered" in pl:
            s += 120
        if "term" in pl and "director" in pl:
            s += 80
        return s

    return max(candidates, key=_score)
