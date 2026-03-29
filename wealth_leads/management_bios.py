"""
Parse per-executive narrative blocks under Management (Name—Title . Mr./Ms. … bio …).
Also extract issuer-wide director term / board election language when present.
"""
from __future__ import annotations

import re
from typing import Optional

from bs4 import BeautifulSoup, Tag

# Name—Title . Mr.|Ms.|Mrs.|Dr. … (em dash or hyphen variants)
_BIO_HEAD = re.compile(
    r"^(.+?)\s*[—\u2013\u2014\-]\s*(.+?)\s*\.\s+(Mr\.|Ms\.|Mrs\.|Dr\.)\s*",
    re.UNICODE,
)

_STOP_BEFORE_COMP = re.compile(
    r"^(summary\s+compensation|compensation\s+discussion|corporate\s+governance|"
    r"executive\s+compensation|director\s+compensation|pay\s+versus\s+performance|"
    r"outstanding\s+equity|grants\s+of\s+plan)",
    re.I,
)


def _norm_key(name: str) -> str:
    s = (name or "").lower().replace(".", " ")
    return " ".join(s.split())


def _find_management_anchor(soup: BeautifulSoup) -> Optional[Tag]:
    """
    Locate the real Management / roster heading — avoid TOC lines that are only 'Management'.
    """
    hit = soup.find(string=re.compile(r"executive\s+officers?\s+and\s+directors", re.I))
    if hit:
        cur = hit.parent
        while cur is not None and getattr(cur, "name", None) != "p":
            cur = cur.parent
        if cur is not None:
            return cur
    for p in soup.find_all("p"):
        t = p.get_text(" ", strip=True).lower()
        if "executive officers and directors" in t:
            return p
    return None


def _paragraphs_after_anchor(anchor: Tag) -> list[Tag]:
    """Sequential <p> tags after the anchor until compensation / governance headers."""
    out: list[Tag] = []
    for p in anchor.find_all_next("p"):
        t = p.get_text(" ", strip=True)
        if not t:
            continue
        head = t.strip()[:120]
        if _STOP_BEFORE_COMP.match(head):
            break
        if head.isupper() and len(head) < 90 and " " in head and "TABLE" not in head:
            # Likely a printed section header; stop if it looks like a new article
            if any(
                x in head
                for x in (
                    "COMPENSATION",
                    "GOVERNANCE",
                    "RELATIONSHIPS",
                    "SECURITY OWNERSHIP",
                )
            ):
                break
        out.append(p)
    return out


def extract_management_biographies_from_filing_html(html: str) -> list[dict]:
    """
    Returns dicts: person_name, person_name_norm, role_heading, bio_text.
    bio_text includes career / prior-role narrative (often covers “last five years” in prose).
    """
    soup = BeautifulSoup(html, "html.parser")
    anchor = _find_management_anchor(soup)
    if anchor is None:
        return []

    paragraphs = _paragraphs_after_anchor(anchor)
    results: list[dict] = []
    i = 0
    while i < len(paragraphs):
        t = paragraphs[i].get_text(" ", strip=True)
        m = _BIO_HEAD.match(t)
        if not m:
            i += 1
            continue
        display_name = m.group(1).strip()
        role_heading = m.group(2).strip()
        start_body = m.start(3)
        body = t[start_body:].strip()
        j = i + 1
        while j < len(paragraphs):
            nt = paragraphs[j].get_text(" ", strip=True)
            if not nt:
                j += 1
                continue
            if _BIO_HEAD.match(nt):
                break
            body = f"{body}\n\n{nt}".strip()
            j += 1
        if len(body) < 40:
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
