"""Map long SEC titles to short advisor-facing role labels (full title stays in tooltip)."""
from __future__ import annotations

import re


def advisor_title_badge(title: str) -> str:
    """
    Collapse titles like 'Executive Vice President - General Counsel and Director'
    to a single bucket (e.g. EVP). 'President, CMC Division' -> President.
    """
    t = (title or "").strip()
    if not t or t == "—":
        return "—"
    low = t.lower()
    u = t.upper()

    # S-1 beneficial-ownership path — not an operating executive title.
    if (
        "major shareholder" in low
        or "beneficial ownership" in low
        or ">5%" in t
        or ("shareholder" in low and "beneficial" in low)
    ):
        return "5%+ holder"

    if re.search(r"\bCEO\b", u) or "chief executive officer" in low:
        return "CEO"
    if re.search(r"\bCFO\b", u) or "chief financial officer" in low:
        return "CFO"
    if re.search(r"\bCOO\b", u) or "chief operating officer" in low:
        return "COO"
    if re.search(r"\bCTO\b", u) or "chief technology officer" in low:
        return "CTO"
    if re.search(r"\bCIO\b", u) or "chief information officer" in low:
        return "CIO"
    if re.search(r"\bCMO\b", u) or "chief marketing officer" in low:
        return "CMO"
    if "general counsel" in low or "chief legal officer" in low:
        return "General Counsel"
    if "chairman" in low or "chairperson" in low:
        return "Chair"
    if re.search(r"\bevp\b", u) or "executive vice president" in low:
        return "EVP"
    if re.search(r"\bsvp\b", u) or "senior vice president" in low:
        return "SVP"
    if "vice president" in low:
        return "VP"
    if re.search(r"\bpresident\b", low):
        return "President"
    if "treasurer" in low:
        return "Treasurer"
    if "secretary" in low:
        return "Secretary"
    if "controller" in low:
        return "Controller"
    if "director" in low:
        return "Director"
    if "partner" in low:
        return "Partner"
    if "founder" in low:
        return "Founder"
    return "Executive"
