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
# Use ``fl\.`` only — a bare ``FL`` is almost always a U.S. state, not ``floor``.
_STREET_NOISE = re.compile(
    r"\b(suite|ste\.?|unit|floor|fl\.|bldg|building|attention|attn\.?|"
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


def registrant_hq_line_parses_as_united_states(hq: str | None) -> bool:
    """
    True when the registrant principal-office line yields U.S. state and/or ZIP keys
    (same signals as ``extract_territory_keys_from_hq``).

    U.S. SEC registrants often incorporate in Nevada/Delaware but disclose a **non-U.S.**
    principal executive office (e.g. Hong Kong). Those addresses usually contain no U.S.
    ZIP or state name, so this returns False even though the company files U.S. registration
    statements such as an S-1.
    """
    return bool(extract_territory_keys_from_hq((hq or "").strip()))


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


_FACILITY_OUR_CLAUSE = re.compile(
    r",?\s*(?:in|at)\s+our\s+"
    r"([A-Za-z][A-Za-z'\-]*(?:\s+[A-Za-z][A-Za-z'\-]*)?)\s+facility\b\.?",
    re.I,
)

_NON_US_PRINCIPAL_HINT = re.compile(
    r"\b("
    r"hong\s+kong|香港|kowloon|cayman\s+islands|grand\s+cayman|tortola|"
    r"british\s+virgin\s+islands|\bbvi\b|"
    r"people'?s\s+republic\s+of\s+china|(?<![a-z])prc(?![a-z])"
    r")\b",
    re.I,
)


def strip_hq_our_facility_clause(hq: str) -> tuple[str, Optional[str]]:
    """
    Remove trailing 'in our Vista facility' style clauses; return (cleaned, city hint if any).
    """
    t = (hq or "").strip()
    m = _FACILITY_OUR_CLAUSE.search(t)
    if not m:
        return t, None
    city = (m.group(1) or "").strip()
    t2 = (t[: m.start()] + t[m.end() :]).strip(" ,.;")
    return t2, city or None


def strip_registrant_hq_contact_tail(s: str) -> str:
    """Remove telephone / fax clauses often concatenated onto principal-office lines."""
    t = (s or "").strip()
    t = re.sub(
        r",?\s*((?:telephone|phone|fax|facsimile|tel\.)|\btel)\s*:.*$",
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
    # Common SEC cover variants without "at this/that address" or without a colon.
    t = re.sub(
        r",?\s*and\s+our\s+telephone\s+number\s+is\s+.+$",
        "",
        t,
        flags=re.I,
    )
    t = re.sub(
        r",?\s*and\s+our\s+facsimile\s+number\s+is\s+.+$",
        "",
        t,
        flags=re.I,
    )
    t = re.sub(r",?\s*and\s+our\s+telephone\b.*$", "", t, flags=re.I)
    t = re.sub(r",?\s*and\s+our\s+facsimile\b.*$", "", t, flags=re.I)
    t = re.sub(
        r",?\s*including\s+area\s+code\b.*$",
        "",
        t,
        flags=re.I,
    )
    return t.rstrip(" ,")


_LEASE_OR_RENTAL_CUT = re.compile(
    r"(?:^|[,;\s])(?:with\s+the\s+lease\s+expiring\b|at\s+a\s+monthly\s+rental\b|"
    r"monthly\s+rental\s+cost\b|rental\s+cost\s+of\b)",
    re.I,
)


# Narrative accidentally concatenated after a street/city/ZIP line on S-1 cover or summary
# (e.g. "…NY 11598 Lock-up: We, each of our officers… have agreed… hypothecate, pledge…").
_HQ_LOCKUP_AND_PROSPECTUS_TAIL = re.compile(
    r"(?is)"
    r"(?:"
    r"\block-?up\s*:"
    r"|\block\s+up\s+agreement\s+"
    r"|\bwe\s*,\s*each\s+of\s+our\s+(?:officers|directors|officers\s+and\s+directors)\b"
    r"|\beach\s+of\s+our\s+officers\s*,?\s*directors\b"
    r"|\bcertain\s+of\s+our\s+stockholders\s+have\s+agreed\b"
    r"|\b(?:have|has)\s+agreed\s*,\s*subject\s+to\b"
    r"|\bnot\s+to\s+sell\s*,\s*offer\s*,?\s*agree\s+to\s+sell\b"
    r"|\bcontract\s+to\s+sell\s*,?\s*hypothecate\b"
    r")",
)


def strip_hq_lockup_and_prospectus_tail(s: str) -> str:
    """
    Cut lock-up / selling-restriction / underwriting prose sometimes pasted after the
    registrant address in a single blob (no sentence break). Keeps the address prefix only.
    """
    t = (s or "").strip()
    if not t:
        return ""
    m = _HQ_LOCKUP_AND_PROSPECTUS_TAIL.search(t)
    if m:
        t = t[: m.start()].strip().rstrip(",.;")
    # Truncated capture after ZIP: "11598 gr" or "11598 xyz"
    t = re.sub(r"(\b\d{5}(?:-\d{4})?)\s+[A-Za-z]{1,4}\s*$", r"\1", t)
    return t.rstrip(" ,")


# Principal office line often ends with "ST 12345, (775) 902-5161", "…89703, 775 902, 5161",
# or local-only ", 725-0090" (exchange + last four, no area code). Not part of the address.
_HQ_TRAILING_PHONE_AFTER_ZIP = re.compile(
    r"\b(?P<zip>\d{5}(?:-\d{4})?)\s*,\s*"
    r"(?:\+?1[\s.-]*)?"
    r"(?:"
    r"\(?\d{3}\)?[\s.-]*\d{3}[\s.-]*\d{4}|"  # (775) 902-5161
    r"\d{3}[\s.,-]+\d{3}(?:[\s.,-]+\d{1,4})?|"  # 775 902, 5161
    r"\d{3}[\s.-]+\d{4}"  # 725-0090, 725.0090 (7-digit local)
    r")\s*$",
    re.I,
)
# Same tail when filers omit the comma before the phone: "32901 725-0090"
_HQ_TRAILING_PHONE_AFTER_ZIP_SPACE = re.compile(
    r"\b(?P<zip>\d{5}(?:-\d{4})?)\s+"
    r"(?:\+?1[\s.-]*)?"
    r"(?:"
    r"\(?\d{3}\)?[\s.-]*\d{3}[\s.-]*\d{4}|"
    r"\d{3}[\s.,-]+\d{3}(?:[\s.,-]+\d{1,4})?|"
    r"\d{3}[\s.-]+\d{4}"
    r")\s*$",
    re.I,
)


def strip_hq_phone_tail_after_zip(s: str) -> str:
    """Drop telephone digits that SEC cover/extract sometimes leaves after City, State ZIP."""
    t = (s or "").strip()
    if not t:
        return ""
    # "Texas 78735 and our telephone number is …" (no comma before "and")
    t = re.sub(
        r"(\b\d{5}(?:-\d{4})?)\s+and\s+our\s+(?:telephone|facsimile)\b.*$",
        r"\1",
        t,
        flags=re.I,
    )
    for rx in (_HQ_TRAILING_PHONE_AFTER_ZIP, _HQ_TRAILING_PHONE_AFTER_ZIP_SPACE):
        m = rx.search(t)
        if m:
            t = t[: m.end("zip")].rstrip(", ;")
            break
    return t


def normalize_registrant_hq_address_blob(s: str) -> str:
    """
    Normalize a registrant principal-office blob toward a U.S. mailing-address prefix.

    Strips phone/fax clauses, lease tail, lock-up prospectus tail, then **telephone
    digits glued after a U.S. ZIP** (10-digit, 3+3+4 chunks, or **local 7-digit**
    e.g. ``32901, 725-0090``). Use before
    city/state parsing, territory keys, or full-address display.
    """
    t = (s or "").strip()
    if not t:
        return ""
    t = strip_registrant_hq_contact_tail(t)
    t = strip_hq_lease_and_rental_tail(t)
    t = strip_hq_lockup_and_prospectus_tail(t)
    t = strip_hq_phone_tail_after_zip(t)
    return t.rstrip(" ,")


def strip_hq_lease_and_rental_tail(s: str) -> str:
    """
    Cut off lease / rent narrative sometimes concatenated to a principal-office capture
    (common in foreign subsidiary / facility descriptions). Handles clauses at the start
    of the string or after a space (no comma).
    """
    t = (s or "").strip()
    for _ in range(16):
        m = _LEASE_OR_RENTAL_CUT.search(t)
        if not m:
            break
        t = t[: m.start()].strip(" ,")
    return t


def headquarters_looks_like_lease_narrative(hq: str | None) -> bool:
    """True when the blob is clearly a rental / lease clause, not a registrant street line."""
    low = (hq or "").lower()
    if not low.strip():
        return False
    return any(
        x in low
        for x in (
            "monthly rental",
            "lease expiring",
            "with the lease",
            "rental cost of rmb",
            "at a monthly rental",
            "rental cost of",
        )
    )


def hq_city_state_looks_like_filing_noise(s: str | None) -> bool:
    """True when a city/state display string is clearly SEC boilerplate, not a location."""
    low = (s or "").strip().lower()
    if not low:
        return False
    return any(
        x in low
        for x in (
            "lease expir",
            "with the lease",
            "monthly rental",
            "at a monthly rental",
            "identification number",
            "employer identification",
            "irs employer",
            "i.r.s. employer",
            "i.r.s employer",
            "primary standard industrial",
            "classification code number",
            "lock-up",
            "lock up",
            "hypothecate",
            "stockholders have agreed",
            "agree to sell",
            "our telephone",
            "telephone number",
            "our facsimile",
            "including area code",
        )
    ) or bool(
        re.search(r",\s*\d{3}[\s.-]+\d{4}\s*$", low)
        or re.search(r"^\s*[a-z]{2}\s*,\s*\d{3}[\s.-]+\d{4}\s*$", low)
    )


def _finalize_city_state_no_zip(display: str) -> str:
    """Ensure pipeline / DB city-state field never ends with a US ZIP."""
    t = (display or "").strip()
    if not t:
        return ""
    t = re.sub(r",\s*([A-Z]{2})\s+\d{5}(?:-\d{4})?\s*$", r", \1", t, flags=re.I)
    t = re.sub(r"\s+\d{5}(?:-\d{4})?\s*$", "", t)
    return t.strip(" ,")


_STREET_TYPE_WORD = re.compile(
    r"\b(street|st\.|avenue|ave|boulevard|blvd|road|rd|drive|dr|lane|ln|"
    r"way|parkway|place|plaza|highway|hwy|court|ct\.?|circle|cir\.?|terrace|trail)\b",
    re.I,
)

# City side of "City, ST" must not look like a registrant street line or legal-entity name.
_CORP_OR_ENTITY_TAIL = re.compile(
    r"(?i)\b(?:incorporated|corporation|company|inc\.?|corp\.?|llc|l\.?l\.?c\.?|ltd\.?|plc)\s*$"
)


def _city_segment_plausible_for_city_state_field(seg: str) -> bool:
    """
    True if ``seg`` may appear as the city (left) side of a city/state pipeline label.
    Rejects numbered streets, thoroughfare tokens, suites/boxes, and obvious issuer-name tails.
    """
    s = (seg or "").strip()
    if not s or len(s) > 80:
        return False
    if re.search(r"\d", s):
        return False
    if _STREET_TYPE_WORD.search(s):
        return False
    if _STREET_NOISE.search(s):
        return False
    if re.search(r"(?i)#\s*\d", s):
        return False
    if _CORP_OR_ENTITY_TAIL.search(s):
        return False
    # "Bond St New York" (thoroughfare + city in one segment) — not a city label.
    if re.search(r"(?i)\b[A-Za-z][A-Za-z'`-]{0,30}\s+st\.?\s+[A-Z]", s):
        return False
    # Comma split left "St" + city (e.g. "… Bond St New York, NY" → "St New York") — not a city.
    if re.match(r"(?i)^st\s+[A-Z]", s) and not s.lower().startswith("st."):
        return False
    return True


def pipeline_hq_city_state_label_ok(cs: str | None) -> bool:
    """
    True if ``cs`` is safe for pipeline / materialized ``issuer_hq_city_state`` (city + state/region,
    no US ZIP, no street-level left side).
    """
    t = (cs or "").strip()
    if not t or hq_city_state_looks_like_filing_noise(t):
        return False
    if re.search(r"\b\d{5}(?:-\d{4})?\b", t):
        return False
    if len(t) > 88:
        return False
    parts = [p.strip() for p in t.split(",") if p.strip()]
    if len(parts) < 2:
        return False
    city_side = ", ".join(parts[:-1]).strip()
    st_side = parts[-1]
    if not _city_segment_plausible_for_city_state_field(city_side):
        return False
    if len(st_side) > 40 or re.match(r"^\d", st_side):
        return False
    return True


def _issuer_hq_city_state_ui_loose_ok(cs: str | None) -> bool:
    """
    Accepts labels for lead/pipeline **Location** when strict :func:`pipeline_hq_city_state_label_ok`
    rejects them but the string still looks like city + state/region (no ZIP, no obvious issuer tail).
    """
    if pipeline_hq_city_state_label_ok(cs):
        return True
    t = (cs or "").strip()
    if not t or hq_city_state_looks_like_filing_noise(t):
        return False
    if re.search(r"\b\d{5}(?:-\d{4})?\b", t):
        return False
    if len(t) > 100:
        return False
    parts = [p.strip() for p in t.split(",") if p.strip()]
    if len(parts) < 2:
        return False
    city_side = ", ".join(parts[:-1]).strip()
    st_side = parts[-1]
    if re.search(r"\d", city_side) or len(city_side) > 72:
        return False
    if _CORP_OR_ENTITY_TAIL.search(city_side):
        return False
    if re.match(r"^\d", st_side):
        return False
    if len(st_side) > 40:
        return False
    return True


def issuer_hq_city_state_materialized_ok(cs: str | None) -> bool:
    """Whether a stored ``issuer_hq_city_state`` cell is safe to show when live HQ parse is empty."""
    v = (cs or "").strip()
    if not v:
        return False
    return pipeline_hq_city_state_label_ok(v) or _issuer_hq_city_state_ui_loose_ok(v)


def _hq_pipeline_city_state_zip_tail_fallback(hq: str | None) -> str:
    """
    When :func:`hq_city_state_display` returns empty, infer **City, ST** from a US tail
    ``…, City, ST 12345`` or ``… City ST 12345`` (requires ZIP + 2-letter state for confidence).
    """
    if not (hq or "").strip():
        return ""
    s_strip, _fc = strip_hq_our_facility_clause(hq or "")
    s0 = normalize_registrant_hq_address_blob(s_strip)
    raw = _scrub_hq_for_location(s0)
    if not raw or _NON_US_PRINCIPAL_HINT.search(raw):
        return ""
    if headquarters_looks_like_lease_narrative(raw) and not re.search(
        r"(?i)\b(street|st\.|avenue|road|suite|room|floor|building|hong\s+kong)\b",
        raw,
    ):
        return ""
    work = raw.strip(" ,")
    mz = re.search(r"\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\s*$", work, re.I)
    if not mz:
        return ""
    st = _norm_state_token(mz.group(1).upper())
    if not st:
        return ""
    before = work[: mz.start()].strip(" ,")
    if not before:
        return ""

    def _strip_zip_seg(seg: str) -> str:
        return re.sub(r"\s+\d{5}(?:-\d{4})?\s*$", "", (seg or "").strip(), flags=re.I).strip()

    city = ""
    parts = [p.strip() for p in before.split(",") if p.strip()]
    for idx in range(len(parts) - 1, -1, -1):
        cand = _strip_zip_seg(parts[idx])
        if cand and _city_segment_plausible_for_city_state_field(cand):
            city = cand
            break
    if not city:
        w = before.split()
        for n in range(4, 0, -1):
            if len(w) >= n:
                cand = " ".join(w[-n:])
                if _city_segment_plausible_for_city_state_field(cand):
                    city = cand
                    break
    if not city:
        return ""
    line = _finalize_city_state_no_zip(f"{city}, {st}")
    if not line or hq_city_state_looks_like_filing_noise(line):
        return ""
    if re.search(r"\b\d{5}", line):
        return ""
    return line


def hq_city_state_pipeline_only(hq: str | None) -> str:
    """
    Strict **city + state/region** for pipeline lists and ``issuer_hq_city_state`` materialization.
    Returns ``""`` when :func:`hq_city_state_display` would include a street-like or issuer-name segment.
    """
    cs = hq_city_state_display(hq or "")
    if cs and pipeline_hq_city_state_label_ok(cs):
        return cs
    fb = _hq_pipeline_city_state_zip_tail_fallback(hq or "")
    if fb and pipeline_hq_city_state_label_ok(fb):
        return fb
    return ""


# SEC cover / MD&A sentences that sometimes remain attached to address blobs.
_ADVISOR_CITY_ST_PROSE = re.compile(
    r"\b(which\b|where\s+our|where\s+the|records\s+are|kept\s+and|"
    r"principal\s+business|executive\s+offic|mailing\s+address|registered\s+agent|"
    r"incorporated\s+in|located\s+at\s+our|business\s+address\s+for|"
    r"our\s+records\s+are)\b",
    re.I,
)


def _hq_blob_one_line(raw: str | None) -> str:
    """Same single-line collapse as crm_ui.format_headquarters_for_ui (shared here to avoid import cycles)."""
    s = (raw or "").strip()
    if not s:
        return ""
    parts = [p.strip() for p in re.split(r"[\n\r]+", s) if p.strip()]
    one = ", ".join(parts)
    one = re.sub(r"[ \t]{2,}", " ", one).strip()
    return normalize_registrant_hq_address_blob(one)


def hq_normalize_ui_line(raw: str | None) -> str:
    """Public alias: one-line HQ for UI compare / scoring (identical to format_headquarters_for_ui)."""
    return _hq_blob_one_line(raw)


def _hq_blob_trim_trailing_prose(blob: str) -> str:
    """Drop SEC narrative often glued after the address (``..., which is where our ...``)."""
    t = (blob or "").strip()
    if not t:
        return ""
    low = t.lower()
    for marker in (
        ", which",
        ", where",
        "; which",
        ", and is",
        ", and our",
    ):
        i = low.find(marker)
        if i > 16:
            t = t[:i].strip().rstrip(",;")
            low = t.lower()
    return t


def hq_advisor_city_state_only(hq: str | None) -> str:
    """
    **City, ST** only (known US state), for advisor territory / company desk — no street, ZIP,
    country suffix, or filing prose. Returns ``""`` if we cannot produce a confident US city + state.
    """
    if not (hq or "").strip():
        return ""
    blob = _hq_blob_one_line(hq)
    for sep in (" · ", " \u00b7 "):
        if sep in blob:
            blob = blob.split(sep, 1)[0].strip()
            break
    blob = _hq_blob_trim_trailing_prose(blob)
    cs = hq_city_state_display(blob)
    if not cs:
        return ""
    t = _finalize_city_state_no_zip(cs).strip()
    if not t or hq_city_state_looks_like_filing_noise(t):
        return ""
    if _ADVISOR_CITY_ST_PROSE.search(t):
        return ""
    if len(t) > 44:
        return ""
    m = re.match(r"^(.+),\s*([A-Z]{2})\s*$", t)
    if not m:
        return ""
    city = re.sub(r"\s+", " ", m.group(1).strip())
    st_tok = m.group(2).upper()
    st = _norm_state_token(st_tok)
    if not st:
        return ""
    if re.match(r"^\d", city):
        return ""
    if len(city) < 2 or len(city) > 36:
        return ""
    return f"{city}, {st}"


def hq_looks_like_street_plus_state_only(raw: str | None) -> bool:
    """
    True for strings like ``440 Stevens Ave, CA`` — street + 2-letter state (optional ZIP)
    with no city segment. These parse poorly for territory and should not be shown as a full location.
    """
    s0 = _hq_blob_one_line(raw)
    if not s0:
        return False
    s = _scrub_hq_for_location(normalize_registrant_hq_address_blob(s0)).strip(" ,")
    if not s:
        return False
    parts = [p.strip() for p in s.split(",") if p.strip()]
    if len(parts) != 2:
        return False
    street_seg, state_seg = parts[0], parts[1]
    st = re.sub(r"\s+\d{5}(?:-\d{4})?\s*$", "", state_seg, flags=re.I).strip()
    if len(st) != 2 or not st.isalpha():
        return False
    if not _norm_state_token(st.upper()):
        return False
    if not re.match(r"^\d+\s+", street_seg):
        return False
    if not _STREET_TYPE_WORD.search(street_seg):
        return False
    return True


def company_registrant_location_display(hq: str | None, *, max_len: int = 140) -> str:
    """
    One line for company rosters: validated **City, ST** when parseable; optional street
    after `` · `` when the first address segment is clearly a street line. Returns ``""``
    when the only parseable form is street+state-only junk.
    """
    if not (hq or "").strip():
        return ""
    if hq_looks_like_street_plus_state_only(hq):
        return ""
    cs = hq_city_state_display(hq)
    if cs and not hq_city_state_looks_like_filing_noise(cs):
        s_scrub = _scrub_hq_for_location(
            normalize_registrant_hq_address_blob(_hq_blob_one_line(hq))
        ).strip(" ,")
        if s_scrub and hq_has_registrant_address_detail(hq):
            segs = [p.strip() for p in s_scrub.split(",") if p.strip()]
            if segs:
                street = segs[0]
                city_part = cs.split(",")[0].strip().lower()
                if (
                    not _segment_is_location_noise(street)
                    and re.match(r"^\d+\s+", street)
                    and _STREET_TYPE_WORD.search(street)
                    and (not city_part or city_part not in street.lower())
                ):
                    return f"{cs} · {street}"[:max_len]
        return cs[:max_len]
    if headquarters_looks_like_lease_narrative(_hq_blob_one_line(hq)):
        return ""
    pl = hq_principal_office_display_line(hq, max_len=max_len)
    if pl and is_plausible_registrant_headquarters(hq):
        return pl[:max_len]
    return ""


def hq_has_registrant_address_detail(hq: str | None) -> bool:
    """
    True if HQ looks like a registrant principal office (not city/state/ZIP only).
    S-1 cover addresses include a street number, P.O. Box, suite, or long named thoroughfare;
    "City, ST 12345" alone is not enough (too easy to mispick from narrative text).
    """
    s0 = normalize_registrant_hq_address_blob(hq or "")
    s = _scrub_hq_for_location(s0).strip()
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
    if re.match(r"^\d", s):
        if _STREET_TYPE_WORD.search(s):
            return False
        if re.match(r"^\d{1,6}\s*$", s):
            return True
        if len(s) <= 6 and re.match(r"^(?:19|20)\d{2}\b", s):
            return True
        if len(s) <= 4 and re.match(r"^\d+", s) and not re.search(r"[A-Za-z]", s):
            return True
        if re.match(r"^\d+\s+[A-Za-z]", s) and len(s) >= 10:
            return False
        return False
    if _STREET_NOISE.search(s):
        return True
    if "®" in s and len(s) < 6:
        return True
    if re.search(r"(?i)lease\s+expir|with\s+the\s+lease|monthly\s+rental", s):
        return True
    if re.search(
        r"(?i)employer\s+identification|identification\s+number|i\.?\s*r\.?\s*s\.?\s*employer",
        s,
    ):
        return True
    if re.search(
        r"(?i)lock-?up(\s|:|$)|hypothecate|pledge\s*,\s*grant|stockholders?\s+have\s+agreed|"
        r"contract\s+to\s+sell|each\s+of\s+our\s+officers",
        s,
    ):
        return True
    if re.search(
        r"(?i)\bour\s+telephone\b|\bour\s+facsimile\b|\btelephone\s+number\s+is\b",
        s,
    ):
        return True
    low_seg = s.lower().strip()
    if len(low_seg) <= 3 and low_seg.isalpha() and low_seg not in (
        "usa",
        "uae",
        "pei",
    ):
        if len(low_seg) == 2 and _norm_state_token(low_seg.upper()):
            return False
        return True
    if re.match(
        r"^(?:\+?1\s*)?"
        r"(?:\(?\d{3}\)?[\s.-]*)?"
        r"\d{3}[\s.,-]+\d{3}(?:[\s.,-]+\d{1,4})?\s*$",
        s,
    ):
        return True
    if re.match(
        r"^(?:\+?1[\s.-]*)?(?:\(?\d{3}\)?[\s.-]*)?\d{3}[\s.-]+\d{4}\s*$",
        s,
    ):
        return True
    return False


def _hq_city_state_street_endswith_same_as_state_tail(clean: list[str]) -> str:
    """
    Two-part US lines where the street segment ends with the city name repeated as the state
    (full state name + ZIP), e.g. ``85 Broad Street New York, New York 10004`` → ``New York, NY``.
    """
    if len(clean) < 2:
        return ""

    def _sz(seg: str) -> str:
        return re.sub(r"\s+\d{5}(?:-\d{4})?\s*$", "", (seg or "").strip(), flags=re.I).strip()

    last = _sz(clean[-1])
    st = _us_state_abbr_from_token(last)
    if not st:
        return ""
    if len(last) < 4:
        return ""
    prev = _sz(clean[-2])
    if not prev.lower().endswith(last.lower()):
        return ""
    t = _finalize_city_state_no_zip(f"{last}, {st}")
    if not t or hq_city_state_looks_like_filing_noise(t):
        return ""
    return t


def _us_state_abbr_from_token(tok: str) -> Optional[str]:
    t = (tok or "").strip()
    m = re.match(r"^([A-Z]{2})(?:\s+\d{5}(?:-\d{4})?)?\s*$", t, re.I)
    if m:
        return _norm_state_token(m.group(1))
    t_no_zip = re.sub(r"\s+\d{5}(?:-\d{4})?\s*$", "", t, flags=re.I).strip()
    low = t_no_zip.lower()
    if low in _US_STATE_NAMES:
        return _US_STATE_NAMES[low]
    if t.lower() in _US_STATE_NAMES:
        return _US_STATE_NAMES[t.lower()]
    return None


def hq_city_state_display(hq: str | None) -> str:
    """
    City + US state (2-letter), or city + country / region — no street, no US ZIP.
    Pipeline tables and ``issuer_hq_city_state`` should use :func:`hq_city_state_pipeline_only`
    so labels never pick a street line or issuer-name segment as the city side.
    """
    def _emit(line: str) -> str:
        t = _finalize_city_state_no_zip((line or "").strip())
        if not t or hq_city_state_looks_like_filing_noise(t):
            return ""
        return t

    s_strip, facility_city = strip_hq_our_facility_clause(hq or "")
    # Same normalization as hq_principal_office_display_line (phone-after-ZIP, lock-up, etc.)
    s0 = normalize_registrant_hq_address_blob(s_strip)
    raw = _scrub_hq_for_location(s0)
    if not raw:
        return ""
    if _NON_US_PRINCIPAL_HINT.search(raw):
        return ""
    if headquarters_looks_like_lease_narrative(raw) and not re.search(
        r"(?i)\b(street|st\.|avenue|road|suite|room|floor|building|hong\s+kong)\b",
        raw,
    ):
        return ""
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    clean = [p for p in parts if not _segment_is_location_noise(p)]
    if not clean:
        return ""

    def _strip_zip_from_seg(seg: str) -> str:
        return re.sub(r"\s+\d{5}(?:-\d{4})?\s*$", "", (seg or "").strip()).strip()

    if (
        facility_city
        and len(clean) == 1
        and _ZIP_RE.search(clean[0])
    ):
        seg0 = _strip_zip_from_seg(clean[0])
        st_only = _us_state_abbr_from_token(seg0)
        if st_only:
            hit = _emit(f"{facility_city}, {st_only}")
            if hit:
                return hit

    # e.g. "Houston, Texas 77056" — strip ZIP before full state name → abbr
    last_tok = _strip_zip_from_seg(clean[-1])
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
            seg_city = _strip_zip_from_seg(seg)
            if not _city_segment_plausible_for_city_state_field(seg_city):
                i -= 1
                continue
            return _emit(f"{seg_city}, {st}")

    if len(clean) >= 2:
        a, b = clean[-2], clean[-1]
        b2 = _strip_zip_from_seg(b)
        a2 = _strip_zip_from_seg(a)
        st_b = _us_state_abbr_from_token(b2)
        if st_b and _city_segment_plausible_for_city_state_field(a2):
            return _emit(f"{a2}, {st_b}")
        if (
            not _segment_is_location_noise(a2)
            and not _segment_is_location_noise(b2)
            and _city_segment_plausible_for_city_state_field(a2)
            and len(b2) <= 36
            and len(a2) <= 80
        ):
            return _emit(f"{a2}, {b2}")
        dup_hit = _hq_city_state_street_endswith_same_as_state_tail(clean)
        if dup_hit:
            return _emit(dup_hit)
    if len(clean) == 1:
        one = _strip_zip_from_seg(clean[-1])
        # Single segment only — never treat as city if it looks like a full address fragment.
        if len(one) <= 48 and not re.match(r"^\d", one) and not re.search(r"\d", one):
            if _segment_is_location_noise(one) or _MONTH_DAY_ONLY.match(one):
                return ""
            if _us_state_abbr_from_token(one):
                return ""
            if not _city_segment_plausible_for_city_state_field(one):
                return ""
            return _emit(one)
    return ""


def hq_principal_office_display_line(hq: str | None, *, max_len: int = 800) -> str:
    """
    Single-line registrant principal office for UI (cleaned filing text; may include street + ZIP).
    Use on the lead profile for the full company address; use hq_city_state_display for city/state only.
    """
    s_strip, _fc = strip_hq_our_facility_clause(hq or "")
    s0 = normalize_registrant_hq_address_blob(s_strip)
    s = _scrub_hq_for_location(s0).strip(" ,")
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


def issuer_hq_city_state_for_ui(hq_raw: str | None) -> str:
    """
    **City + state/region** for lead headers, company card, and pipeline — always derived from the
    same canonical scrub as :func:`hq_principal_office_display_line` when that line is non-empty,
    so compact location stays aligned with the full registrant address the user sees.
    """
    r = (hq_raw or "").strip()
    if not r:
        return ""
    principal = (hq_principal_office_display_line(r) or "").strip()
    for src in (principal, r):
        if not src:
            continue
        cs = hq_city_state_pipeline_only(src)
        if cs:
            return cs
        zt = _hq_pipeline_city_state_zip_tail_fallback(src)
        if zt and _issuer_hq_city_state_ui_loose_ok(zt):
            return zt
        disp = hq_city_state_display(src)
        if disp and _issuer_hq_city_state_ui_loose_ok(disp):
            return disp
    return ""


def is_plausible_registrant_headquarters(hq: str | None) -> bool:
    """
    Filter HTML-heuristic extractions that grabbed effective dates or filing metadata
    instead of a principal-office address. LLM extractions should still pass basic
    sanity checks here when selecting among filings.
    """
    s = (hq or "").strip()
    if headquarters_looks_like_lease_narrative(s):
        return False
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
    if hq_looks_like_street_plus_state_only(s):
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
