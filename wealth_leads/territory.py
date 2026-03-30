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
