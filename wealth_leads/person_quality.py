"""
Reject officer / NEO rows where the "name" cell is prose, a bare title, or misparsed text.

Keeps advisor-facing lists to real people (executives / directors) where the string
shape matches a person name, not a sentence or role descriptor alone.
"""
from __future__ import annotations

import re
from typing import Optional

# Governance / biography prose mistaken for a roster name.
_PROSE_HINT = re.compile(
    r"\b("
    r"serves\s+as|served\s+as|acting\s+as|appointed\s+as|"
    r"will\s+serve|has\s+served|previously\s+served|"
    r"trust\s+protector|of\s+the\s+entity|of\s+the\s+company|"
    r"member\s+of\s+the\s+board|biographical\s+information"
    r")\b",
    re.I,
)

_LEADING_NAME_BEFORE_VERB = re.compile(
    r"^("
    r"[A-Z][a-zA-Z'\-]+"  # first
    r"(?:\s+[A-Z]\.?){0,2}"  # middle / initial
    r"(?:\s+[A-Z][a-zA-Z'\-]+)+"  # last (required)
    r")\s+"
    r"(?:serves|served|has\s+served|will\s+serve|is|was|has\s+been)\s+",
    re.S,
)


def looks_like_role_only_line(s: str) -> bool:
    """Same idea as compensation._looks_like_role_line — local copy to avoid import cycles."""
    t = (s or "").strip().lower()
    keys = (
        "officer",
        "chief",
        "president",
        "director",
        "secretary",
        "treasurer",
        "chairman",
        "chairperson",
        "executive",
        "vice president",
        "cfo",
        "ceo",
        "coo",
        "general counsel",
        "counsel",
        "nominee",
        "division",
    )
    return any(k in t for k in keys)


def looks_like_prose_or_narrative_name_field(s: str) -> bool:
    t = (s or "").strip()
    if not t:
        return True
    if len(t) > 88:
        return True
    if _PROSE_HINT.search(t):
        return True
    if t.count(".") >= 2:
        return True
    if re.search(r",\s*age\s+\d{2}\b", t, re.I):
        return True
    low = t.lower()
    if "http://" in low or "https://" in low:
        return True
    return False


_TITLE_LEAD_WORDS = frozenset(
    {
        "chief",
        "executive",
        "general",
        "senior",
        "vice",
        "assistant",
        "former",
        "president",
        "chairman",
        "chairperson",
        "treasurer",
        "secretary",
        "controller",
        "director",
        "nominee",
        "evp",
        "svp",
    }
)


def is_acceptable_lead_person_name(s: str) -> bool:
    """
    True if the string is plausibly a human name for UI / lead identity.

    Rejects long titles, sentences, and single-column role descriptors.
    """
    t = " ".join((s or "").split())
    if len(t) < 3 or len(t) > 72:
        return False
    if looks_like_prose_or_narrative_name_field(t):
        return False

    parts = t.split()
    p0 = parts[0].lower().rstrip(".")

    # Multi-word title blobs ("Chief Financial Officer", long EVP lines)
    if looks_like_role_only_line(t):
        if len(parts) >= 3:
            return False
        if len(parts) == 2 and (
            p0 in _TITLE_LEAD_WORDS
            or parts[1].lower().rstrip(".") in ("nominee", "officer")
        ):
            return False

    def _is_name_token(p: str) -> bool:
        if not p:
            return False
        if p.upper() == p and len(p) <= 4 and "." not in p:
            return False
        c0 = p[0]
        if c0.isupper() and any(ch.islower() for ch in p[1:]):
            return True
        if len(p) == 2 and p[1] == "." and c0.isupper():
            return True
        if "'" in p and c0.isupper():
            return True
        return False

    name_like = sum(1 for p in re.split(r"\s+", t) if _is_name_token(p.strip(" ,")))
    if name_like < 2 and "," not in t:
        if len(parts) == 1 and _is_name_token(parts[0]):
            return len(parts[0]) >= 3
        return False
    return True


def refine_lead_person_name(raw: str) -> Optional[str]:
    """
    If the cell is 'Jane Q. Doe serves as …', return 'Jane Q. Doe'.
    """
    t = (raw or "").strip()
    m = _LEADING_NAME_BEFORE_VERB.match(t)
    if not m:
        return None
    cand = m.group(1).strip()
    if is_acceptable_lead_person_name(cand):
        return cand
    return None


