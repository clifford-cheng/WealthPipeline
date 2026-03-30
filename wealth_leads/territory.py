"""
Geographic territory matching for lead allocation.
HQ strings come from SEC filings (registrant address), not personal addresses.
"""
from __future__ import annotations

import re
from typing import Optional

# US state abbreviation -> normalized territory key
_US_STATE_NAMES = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
    "district of columbia": "DC",
}

_ZIP_RE = re.compile(r"\b(\d{5})(?:-\d{4})?\b")
_STATE_ABB_END = re.compile(
    r",\s*([A-Z]{2})\s+(?:\d{5}|\d{4,}|[A-Z][a-z])|\b([A-Z]{2})\s+\d{5}\b",
    re.I,
)
_STATE_COMMA = re.compile(r",\s*([A-Z]{2})\s*$", re.I)
_MONTH_DAY_YEAR = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+\d{1,2},?\s*(19|20)\d{2}\b",
    re.I,
)
_SLASH_DATE = re.compile(r"\b\d{1,2}/\d{1,2}/(?:19|20)?\d{2}\b")
_STREET_NOISE = re.compile(
    r"\b(suite|ste\.?|unit|floor|fl\.?|bldg|building|attention|attn\.?|"
    r"p\.?o\.?\s*box|po\s*box|c/o)\b",
    re.I,
)
_MONTH_NAMES = (
    "January|February|March|April|May|June|July|August|September|"
    "October|November|December"
)
_MONTH_DAY_ONLY = re.compile(
    rf"^({_MONTH_NAMES})\s+\d{{1,2}}(?:st|nd|rd|th)?\s*$",
    re.I,
)
_ISO_DATE_PREFIX = re.compile(r"^(?:19|20)\d{2}-\d{2}-\d{2}\s*,?\s*")
_MDY_LEADING = re.compile(
    rf"^(?:(?:as\s+of|effective)\s+)?({_MONTH_NAMES})\s+\d{{1,2}}(?:st|nd|rd|th)?\s*,\s*(?:19|20)\d{{2}}\s*,?\s*",
    re.I,
)
_MDNY_LEADING_NO_YEAR = re.compile(
    rf"^({_MONTH_NAMES})\s+\d{{1,2}}(?:st|nd|rd|th)?\s*,\s*",
    re.I,
)


def _norm_state_token(tok: str) -> Optional[str]:
    t = (tok or "").strip().upper()
    if len(t) == 2 and t.isalpha():
        return t
    return None


def extract_territory_keys_from_hq(hq: str) -> list[str]:
    """
    Derive exclusivity / matching keys from a headquarters line.
    Returns keys like US-ST-CA, US-ZIP-94105.
    """
    raw = (hq or "").strip()
    if not raw:
        return []
    keys: list[str] = []
    seen: set[str] = set()

    def add(k: str) -> None:
        if k and k not in seen:
            seen.add(k)
            keys.append(k)

    low = raw.lower()
    for m in _ZIP_RE.finditer(raw):
        add(f"US-ZIP-{m.group(1)}")

    m2 = _STATE_ABB_END.search(raw)
    if m2:
        st = _norm_state_token(m2.group(1) or m2.group(2) or "")
        if st:
            add(f"US-ST-{st}")

    m3 = _STATE_COMMA.search(raw)
    if m3:
        st = _norm_state_token(m3.group(1))
        if st:
            add(f"US-ST-{st}")

    for name, abbr in _US_STATE_NAMES.items():
        if name in low:
            add(f"US-ST-{abbr}")

    return keys


def _strip_leading_calendar_prefixes(s: str) -> str:
    """Remove effective-date clauses often prepended to SEC address blobs (e.g. 'March 30, 2026, ')."""
    s = (s or "").strip()
    for _ in range(6):
        m = _ISO_DATE_PREFIX.match(s)
        if m:
            s = s[m.end() :].lstrip(" ,")
            continue
        m = _MDY_LEADING.match(s)
        if m:
            s = s[m.end() :].lstrip(" ,")
            continue
        m = _MDNY_LEADING_NO_YEAR.match(s)
        if m:
            s = s[m.end() :].lstrip(" ,")
            continue
        break
    return s.strip()


def _scrub_hq_for_location(hq: str) -> str:
    """Drop parenthetical asides, date prefaces, normalize whitespace."""
    s = (hq or "").strip()
    if not s:
        return ""
    s = re.sub(r"\([^)]{0,240}\)", " ", s)
    s = re.sub(r"[\n\r]+", ", ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = _strip_leading_calendar_prefixes(s)
    s = re.sub(r",\s*,", ", ", s)
    return s.strip(" ,")


def strip_registrant_hq_contact_tail(s: str) -> str:
    """Remove telephone / fax clauses often concatenated onto principal-office lines."""
    t = (s or "").strip()
    t = re.sub(
        r",?\s*(telephone|phone|fax|facsimile|tel\.)\s*:.*$",
        "",
        t,
        flags=re.I,
    )
    t = re.sub(
        r",?\s*and\s+our\s+telephone\s+number\s+at\s+(?:that|this)\s+address\s+is\s+.+$",
        "",
        t,
        flags=re.I,
    )
    return t.rstrip(" ,")


_STREET_TYPE_WORD = re.compile(
    r"\b(street|st\.|avenue|ave|boulevard|blvd|road|rd|drive|dr|lane|ln|"
    r"way|parkway|plaza|highway|hwy|court|ct\.?|circle|cir\.?|terrace|trail)\b",
    re.I,
)


def hq_has_registrant_address_detail(hq: str | None) -> bool:
    """
    True if HQ looks like a registrant principal office (not city/state/ZIP only).
    S-1 cover addresses include a street number, P.O. Box, suite, or long named thoroughfare;
    "City, ST 12345" alone is not enough (too easy to mispick from narrative text).
    """
    s = strip_registrant_hq_contact_tail(_scrub_hq_for_location(hq or "")).strip()
    if not s:
        return False
    if re.search(r"\b[Pp]\.?\s*[Oo]\.?\s*[Bb]ox\s+\d", s):
        return True
    if re.search(r"(?:^|,\s*)(?:p\.?\s*o\.?\s*)?\d{1,6}\s+[A-Za-z]", s, re.I):
        return True
    if re.search(r"\b(?:suite|ste\.?|unit|#)\s*\d", s, re.I):
        return True
    if len(s) >= 42 and _STREET_TYPE_WORD.search(s):
        return True
    return False


def _segment_is_location_noise(seg: str) -> bool:
    """True for street lines, dates, bare ZIPs, suite/floor fragments, etc."""
    s = seg.strip()
    if not s:
        return True
    low = s.lower()
    if _MONTH_DAY_YEAR.search(s) or _SLASH_DATE.search(s):
        return True
    if re.search(
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\b",
        s,
        re.I,
    ) and re.search(r"\b(19|20)\d{2}\b", s):
        return True
    if re.match(r"^(19|20)\d{2}$", s):
        return True
    if re.search(r"\b(19|20)\d{2}\b", s) and len(s) <= 36:
        return True
    if re.match(r"^(fy|as of|effective)\s", low):
        return True
    if re.match(r"^fy\s*\d{4}\b", low):
        return True
    if _MONTH_DAY_ONLY.match(s):
        return True
    if re.match(r"^\d{5}(?:-\d{4})?$", s):
        return True
    if re.match(r"^\d+", s):
        return True
    if _STREET_NOISE.search(s):
        return True
    if "®" in s and len(s) < 6:
        return True
    return False


def _us_state_abbr_from_token(tok: str) -> Optional[str]:
    t = (tok or "").strip()
    m = re.match(r"^([A-Z]{2})(?:\s+\d{5}(?:-\d{4})?)?\s*$", t, re.I)
    if m:
        return _norm_state_token(m.group(1))
    low = t.lower()
    if low in _US_STATE_NAMES:
        return _US_STATE_NAMES[low]
    return None


def hq_city_state_display(hq: str | None) -> str:
    """
    City + US state, or city + country / region — never street or ZIP.
    Parses comma-separated SEC principal-office blobs after scrubbing dates/noise.
    """
    raw = _scrub_hq_for_location(hq or "")
    if not raw:
        return ""
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    clean = [p for p in parts if not _segment_is_location_noise(p)]
    if not clean:
        return ""

    last_tok = clean[-1]
    st = _us_state_abbr_from_token(last_tok)
    if st:
        i = len(clean) - 2
        while i >= 0:
            seg = clean[i]
            if _segment_is_location_noise(seg) or re.match(r"^\d", seg):
                i -= 1
                continue
            if len(seg) < 2:
                i -= 1
                continue
            return f"{seg}, {st}"
        return st

    if len(clean) >= 2:
        a, b = clean[-2], clean[-1]
        if not _segment_is_location_noise(a) and not _segment_is_location_noise(b):
            if len(b) <= 32 and len(a) <= 80:
                return f"{a}, {b}"
    one = clean[-1]
    if len(one) <= 48 and not re.match(r"^\d", one):
        if _segment_is_location_noise(one) or _MONTH_DAY_ONLY.match(one):
            return ""
        return one
    return ""


def hq_principal_office_display_line(hq: str | None, *, max_len: int = 800) -> str:
    """
    Single-line registrant principal office for UI (cleaned filing text; may include street).
    Use alongside hq_city_state_display for city/state headline.
    """
    s = _scrub_hq_for_location(hq or "").strip(" ,")
    if not s:
        return ""
    segs = [p.strip() for p in s.split(",") if p.strip()]
    if len(segs) == 1 and _MONTH_DAY_ONLY.match(segs[0]):
        return ""
    if (
        len(segs) == 2
        and _MONTH_DAY_ONLY.match(segs[0])
        and re.match(r"^(19|20)\d{2}$", segs[1])
    ):
        return ""
    return s[:max_len]


def is_plausible_registrant_headquarters(hq: str | None) -> bool:
    """
    Filter HTML-heuristic extractions that grabbed effective dates or filing metadata
    instead of a principal-office address. LLM extractions should still pass basic
    sanity checks here when selecting among filings.
    """
    s = (hq or "").strip()
    if len(s) < 8:
        return False
    if re.search(r"\(?zip\s+code\)?", s, re.I) and len(s) <= 48 and not re.search(
        r"\b(street|st\.|avenue|ave|road|rd|drive|dr|suite|boulevard|blvd|lane)\b",
        s,
        re.I,
    ):
        return False
    first = s.split(",")[0].strip()
    if _MONTH_DAY_ONLY.match(first):
        return False
    if re.match(r"^(19|20)\d{2}-\d{2}-\d{2}\s*$", first):
        return False
    scrub = _scrub_hq_for_location(s)
    if not scrub or len(scrub) < 8:
        return False
    if hq_principal_office_display_line(s) == "" and hq_city_state_display(s) == "":
        return False
    if not hq_has_registrant_address_detail(s):
        return False
    return True


def parse_location_parts(hq: str) -> dict[str, str]:
    """Lightweight city / state / zip for display (heuristic)."""
    raw = (hq or "").strip()
    out: dict[str, str] = {"city": "", "state": "", "zip": ""}
    if not raw:
        return out
    z = _ZIP_RE.search(raw)
    if z:
        out["zip"] = z.group(1)
    m = _STATE_COMMA.search(raw) or _STATE_ABB_END.search(raw)
    if m:
        st = m.group(1) if m.lastindex else ""
        if st and len(st) == 2:
            out["state"] = st.upper()
        elif m.groups():
            g = m.group(1) or m.group(2) or ""
            st2 = _norm_state_token(g)
            if st2:
                out["state"] = st2
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    if parts and not out["city"]:
        out["city"] = parts[0][:80]
    return out


def territory_spec_to_match_rules(territory_type: str, territory_spec: str) -> dict:
    """
    Client territory configuration.
    type: state | zips | metro
    spec: state code (CA), comma zips, or metro substring.
    """
    tt = (territory_type or "state").strip().lower()
    spec = (territory_spec or "").strip()
    if tt == "state":
        ab = spec.strip().upper()[:2]
        st = _norm_state_token(ab) if len(ab) == 2 else None
        ab = st or ab
        return {"type": "state", "state": ab, "keys": [f"US-ST-{ab}"]}
    if tt == "zips":
        zips = []
        for p in spec.replace(";", ",").split(","):
            p = p.strip()
            if p.isdigit() and len(p) >= 5:
                zips.append(p[:5])
        keys = [f"US-ZIP-{z}" for z in zips]
        return {"type": "zips", "zips": zips, "keys": keys}
    return {"type": "metro", "metro_substring": spec.lower(), "keys": []}


def lead_matches_territory(
    hq: str,
    lead_keys: list[str],
    rules: dict,
) -> bool:
    """Whether a lead's HQ / keys fall inside the client's territory rules."""
    if rules["type"] == "state":
        st = rules.get("state") or ""
        lk = {k for k in lead_keys if k.startswith("US-ST-")}
        return f"US-ST-{st.upper()}" in lk
    if rules["type"] == "zips":
        want = {f"US-ZIP-{z}" for z in rules.get("zips") or []}
        return bool(want & set(lead_keys))
    if rules["type"] == "metro":
        sub = rules.get("metro_substring") or ""
        return bool(sub) and sub in (hq or "").lower()
    return False


def exclusivity_key_for_lead(lead_keys: list[str]) -> str:
    """
    Single key used for 'one client per region' default.
    Prefer state; else first zip; else METRO:hash of hq — use first key.
    """
    for k in lead_keys:
        if k.startswith("US-ST-"):
            return k
    for k in lead_keys:
        if k.startswith("US-ZIP-"):
            return k
    return ""
