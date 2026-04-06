"""
S-1 / registration-style disclosure: beneficial owners (>5%), principal stockholders.

Heuristic HTML parse — not iXBRL-perfect. Produces rows for ranking individuals vs entities,
footnote context, optional mailing lines from footnotes, and rough notional $ at parsed offering price.
"""
from __future__ import annotations

import json
import re
from typing import Any, Optional

from bs4 import BeautifulSoup, Tag

from wealth_leads.person_quality import is_acceptable_lead_person_name, refine_lead_person_name

# Bump when beneficial_owner_stake columns or parsing logic changes (backfill picks stale filings).
STAKE_PARSE_REVISION = "v3"

_ENTITY_HINT = re.compile(
    r"\b("
    r"LLC|L\.L\.C\.|Inc\.?|Incorporated|Corp\.?|Corporation|LP\.?|L\.P\.|"
    r"Ltd\.?|PLC|Limited\b|Trust|Partners|Partnership|Holdings?|Ventures?|Capital|"
    r"Contracting|Physician\s+Contracting|Medical\s+Group|Professional\s+Association|"
    r"P\.?C\.?|P\.A\.|L\.L\.P\.?|"
    r"Pension|401\s*\(?k\)?|Employee\s+Stock|Municipal|County\s+of|State\s+of|"
    r"United\s+States|U\.S\.|Vanguard|BlackRock|Fidelity|Bank\s+of"
    r")\b",
    re.I,
)

_FOOTNOTE_MARKERS = re.compile(r"[\(\[]\s*(\d+)\s*[\)\]]|([⁰¹²³⁴⁵⁶⁷⁸⁹]+)")
_SUP_MAP = str.maketrans("⁰¹²³⁴⁵⁶⁷⁸⁹", "0123456789")


def _row_cell_texts(tr: Tag) -> list[str]:
    return [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]


def _header_blob(headers: list[str]) -> str:
    return " ".join(h.lower() for h in headers if h and h.strip())


def _is_beneficial_owner_table(headers: list[str]) -> bool:
    blob = _header_blob(headers)
    if not blob:
        return False
    # Avoid NEO / comp tables
    if "summary compensation" in blob or "fiscal year" in blob and "salary" in blob:
        return False
    if "beneficial" in blob and (
        "owner" in blob or "stockholder" in blob or "5%" in blob or "five" in blob
    ):
        return True
    if "principal" in blob and "stockholder" in blob:
        return True
    if "security" in blob and "ownership" in blob:
        if "beneficial" in blob or "5%" in blob or "stockholder" in blob:
            return True
    if "voting" in blob and "stock" in blob and "5%" in blob:
        return True
    return False


def _collect_footnote_map_after_table(table: Tag) -> dict[int, str]:
    """
    Notes often live in the next block or in a following <table> (S-1 layout). Do not stop at
    the first sibling table — that table frequently *is* the numbered-footnote block.
    """
    parts: list[str] = []
    for sib in table.find_next_siblings():
        if hasattr(sib, "get_text"):
            parts.append(sib.get_text("\n", strip=True))
        if sum(len(p) for p in parts) > 24_000:
            break
    chunk = "\n".join(parts)
    return _parse_numbered_footnotes(chunk)


def _parse_numbered_footnotes(text: str) -> dict[int, str]:
    out: dict[int, str] = {}
    if not text or not text.strip():
        return out
    for m in re.finditer(
        r"(?:^|\n)\s*\(\s*(\d+)\s*\)\s*(.+?)(?=(?:\n\s*\(\s*\d+\s*\)\s*)|\Z)",
        text,
        re.S | re.M,
    ):
        n = int(m.group(1))
        body = re.sub(r"\s+", " ", m.group(2).strip())
        if body:
            out[n] = body[:4000]
    return out


def _footnote_refs_from_cell(cell: str) -> list[int]:
    refs: list[int] = []
    for m in re.finditer(r"\(\s*(\d+)\s*\)", cell or ""):
        refs.append(int(m.group(1)))
    for m in re.finditer(r"([⁰¹²³⁴⁵⁶⁷⁸⁹]+)", cell or ""):
        digits = m.group(1).translate(_SUP_MAP)
        for ch in digits:
            if ch.isdigit():
                refs.append(int(ch))
    return sorted(set(refs))


def _strip_footnote_marks_from_name(cell: str) -> str:
    s = re.sub(r"\s*[\(\[]\s*\d+\s*[\)\]]\s*$", "", (cell or "").strip())
    s = re.sub(r"\s+[⁰¹²³⁴⁵⁶⁷⁸⁹]+\s*$", "", s)
    return s.strip()


def _parse_us_street_line(text: str) -> str:
    """Return a single plausible U.S. mailing line from footnote prose."""
    t = (text or "").strip()
    if not t:
        return ""
    # Street number + name ... City, ST ZIP
    m = re.search(
        r"\d{1,6}\s+[A-Za-z0-9#.\-\s,]+(?:"
        r"Street|St\.|Avenue|Ave\.|Road|Rd\.|Drive|Dr\.|Lane|Ln\.|"
        r"Place|Pl\.|Court|Ct\.|Boulevard|Blvd\.|Way|Circle|Cir\."
        r")[^.\n]{0,40}[,\s]+[A-Za-z][A-Za-z\s\-]{2,28},\s*[A-Z]{2}\s*\d{5}(?:-\d{4})?",
        t,
        re.I,
    )
    if m:
        return re.sub(r"\s+", " ", m.group(0).strip())
    m2 = re.search(
        r"\d{1,6}\s+[A-Za-z0-9#.\-\s,]{6,80},\s*[A-Za-z][A-Za-z\s\-]{2,28},\s*[A-Z]{2}\s*\d{5}",
        t,
    )
    if m2:
        return re.sub(r"\s+", " ", m2.group(0).strip())
    return ""


def _parse_floatish_shares(cell: str) -> Optional[float]:
    s = (cell or "").strip().replace(",", "")
    if not s or s in ("—", "-", "–", "n/a", "N/A"):
        return None
    m = re.search(r"(\d+(?:\.\d+)?)\s*(?:million|mm|m)\b", s, re.I)
    if m:
        try:
            return float(m.group(1)) * 1_000_000
        except ValueError:
            return None
    m2 = re.search(r"(\d+(?:\.\d+)?)", s.replace("%", ""))
    if not m2:
        return None
    try:
        v = float(m2.group(1))
        return v if v < 1e12 else None
    except ValueError:
        return None


def _parse_percent(cell: str) -> Optional[float]:
    s = (cell or "").strip().replace(",", "")
    m = re.search(r"(\d+(?:\.\d+)?)\s*%", s)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def classify_holder_kind(display_name: str) -> str:
    """person | entity | unknown"""
    t = (display_name or "").strip()
    if not t:
        return "unknown"
    if t.endswith(":"):
        return "entity"
    if re.search(r"\bbeneficial\s+owners?\b", t, re.I):
        return "entity"
    if _ENTITY_HINT.search(t):
        return "entity"
    if is_acceptable_lead_person_name(t):
        return "person"
    ref = refine_lead_person_name(t)
    if ref and is_acceptable_lead_person_name(ref):
        return "person"
    if len(t.split()) >= 2 and t[0].isupper() and not _ENTITY_HINT.search(t):
        return "person"
    return "unknown"


def _try_float_str(g: str) -> Optional[float]:
    try:
        return float(g)
    except ValueError:
        return None


_FRAG_SAFE = re.compile(r"^[A-Za-z0-9_\-:.]+$")

_MAX_ANCHOR_BLOCK_CHARS = 14_000


def _dollar_needle_strings(amount: float) -> list[str]:
    varlist: list[str] = []
    for fmt in (f"${amount:.2f}", f"${amount:.1f}", f"${amount:g}"):
        if fmt not in varlist:
            varlist.append(fmt)
    if amount == int(amount):
        s = f"${int(amount)}"
        if s not in varlist:
            varlist.append(s)
    return varlist


def _add_frag_candidates(
    tag: Tag,
    candidates: list[tuple[str, int]],
    seen: set[str],
) -> None:
    tid = tag.get("id")
    if tid and isinstance(tid, str):
        t = tid.strip()
        if t and _FRAG_SAFE.match(t):
            frag = "#" + t
            if frag not in seen:
                seen.add(frag)
                candidates.append((frag, len(tag.get_text(" ", strip=True))))
    if tag.name == "a":
        nm = tag.get("name")
        if nm and isinstance(nm, str):
            tt = nm.strip()
            if tt and _FRAG_SAFE.match(tt):
                frag = "#" + tt
                if frag not in seen:
                    seen.add(frag)
                    candidates.append((frag, len(tag.get_text(" ", strip=True))))


def _nearest_doc_fragment(
    par: Optional[Tag],
    *,
    amount_hint: Optional[float] = None,
) -> str:
    """
    Prefer an HTML id closest to the matched text.

    Walking only up the tree returns huge Item/section wrappers (``div`` ids) so the
    browser lands a screen or more above the dollar figure; we first look under ``par``
    for tight ids, then pick the smallest ancestor block that still has an id.
    """
    if not isinstance(par, Tag):
        return ""
    needles = _dollar_needle_strings(float(amount_hint)) if amount_hint is not None else []

    def tag_matches_needle(tag: Tag) -> bool:
        if not needles:
            return True
        blob = tag.get_text(" ", strip=True)
        return any(n in blob for n in needles)

    cands: list[tuple[str, int]] = []
    seen_frags: set[str] = set()
    if tag_matches_needle(par):
        _add_frag_candidates(par, cands, seen_frags)
    for sub in par.find_all(id=True):
        if isinstance(sub, Tag) and tag_matches_needle(sub):
            _add_frag_candidates(sub, cands, seen_frags)
    for sub in par.find_all("a", attrs={"name": True}):
        if isinstance(sub, Tag) and tag_matches_needle(sub):
            _add_frag_candidates(sub, cands, seen_frags)
    if cands:
        cands.sort(key=lambda x: x[1])
        for frag, ln in cands:
            if ln <= _MAX_ANCHOR_BLOCK_CHARS:
                return frag
        return cands[0][0]

    anc: list[tuple[str, int]] = []
    seen_anc: set[str] = set()
    cur: Any = par
    step = 0
    while cur is not None and step < 45:
        step += 1
        if isinstance(cur, Tag):
            if tag_matches_needle(cur):
                tid = cur.get("id")
                if tid and isinstance(tid, str):
                    t = tid.strip()
                    if t and _FRAG_SAFE.match(t):
                        frag = "#" + t
                        if frag not in seen_anc:
                            seen_anc.add(frag)
                            anc.append((frag, len(cur.get_text(" ", strip=True))))
                if cur.name == "a":
                    nm = cur.get("name")
                    if nm and isinstance(nm, str):
                        tt = nm.strip()
                        if tt and _FRAG_SAFE.match(tt):
                            frag = "#" + tt
                            if frag not in seen_anc:
                                seen_anc.add(frag)
                                anc.append((frag, len(cur.get_text(" ", strip=True))))
        cur = getattr(cur, "parent", None)
    if anc:
        anc.sort(key=lambda x: x[1])
        for frag, ln in anc:
            if ln <= _MAX_ANCHOR_BLOCK_CHARS:
                return frag
        return anc[0][0]
    return ""


def _find_dollar_anchor(
    soup: BeautifulSoup,
    amount: float,
    *,
    keywords: tuple[str, ...],
) -> str:
    """Locate a tag containing a dollar string for ``amount``; prefer blocks whose text matches keywords."""
    varlist = _dollar_needle_strings(amount)
    rx = re.compile("|".join(re.escape(v) for v in varlist))
    kw = tuple(k.lower() for k in keywords if k)
    for node in soup.find_all(string=rx):
        par = node.parent
        if not isinstance(par, Tag):
            continue
        parts: list[str] = []
        cur: Any = par
        for _ in range(10):
            if not isinstance(cur, Tag):
                break
            parts.append(cur.get_text(" ", strip=True).lower())
            cur = cur.parent
        blob = " ".join(parts)
        if kw and not any(k in blob for k in kw):
            continue
        frag = _nearest_doc_fragment(par, amount_hint=amount)
        if frag:
            return frag
    return ""


def _find_footnote_anchor(soup: BeautifulSoup, fn_id: int, hint: str) -> str:
    """Anchor for numbered footnote ``(n)`` containing mailing / disclosure text."""
    if fn_id <= 0:
        return ""
    rx = re.compile(rf"\(\s*{fn_id}\s*\)")
    hint_l = (hint or "").strip().lower()
    hint_words = [w for w in re.split(r"\W+", hint_l) if len(w) > 3][:6]

    def try_nodes(require_hint: bool) -> str:
        for txt in soup.find_all(string=rx):
            par = txt.parent
            if not isinstance(par, Tag):
                continue
            block = par.get_text(" ", strip=True).lower()
            if require_hint and hint_words:
                if not any(w in block for w in hint_words):
                    continue
            frag = _nearest_doc_fragment(par)
            if frag:
                return frag
        return ""

    hit = try_nodes(require_hint=True)
    return hit or try_nodes(require_hint=False)


def _extract_gross_public_offering_price(haystacks: tuple[str, ...]) -> Optional[float]:
    """Public / assumed **primary** price per share before underwriting haircut (e.g. $5.00)."""
    range_pat = r"price\s+range\s+of\s+\$\s*(\d+(?:\.\d+)?)\s+to\s+\$\s*(\d+(?:\.\d+)?)"
    primary_patterns: list[str] = [
        # "assumed initial public offering price is $5.00" (words between assumed and offering)
        r"(?:assumed|initial)\s+[^$]{0,55}offering\s+price[^$]{0,80}\$\s*(\d+(?:\.\d+)?)",
        r"(?:assumed|initial)\s+(?:public\s+)?offering\s+price[^$]{0,80}\$\s*(\d+(?:\.\d+)?)",
        r"(?:assumed|initial)\s+[^$]{0,40}exercise\s+price\s+of\s+\$\s*(\d+(?:\.\d+)?)\s+per\s+share",
        r"(?:offering|initial)\s+price\s+of\s+\$\s*(\d+(?:\.\d+)?)",
        r"offering\s+price\s+of\s+(\d+(?:\.\d+)?)\s+per\s+share",
        r"public\s+offering\s+price.*?\$\s*(\d+(?:\.\d+)?)",
        r"initial\s+offering\s+price.*?\$\s*(\d+(?:\.\d+)?)",
    ]
    loose_pat = r"\$\s*(\d+(?:\.\d+)?)\s+per\s+share"
    min_primary_usd = 0.5
    for blob in haystacks:
        m = re.search(range_pat, blob, re.I)
        if m:
            lo = _try_float_str(m.group(1))
            hi = _try_float_str(m.group(2))
            if lo is not None and hi is not None and lo >= min_primary_usd:
                return (lo + hi) / 2.0
        for pat in primary_patterns:
            m = re.search(pat, blob, re.I | re.S)
            if m:
                v = _try_float_str(m.group(1))
                if v is not None and v >= min_primary_usd:
                    return v
        for m in re.finditer(loose_pat, blob, re.I):
            v = _try_float_str(m.group(1))
            if v is not None and v >= min_primary_usd:
                return v
    return None


def _extract_underwriting_deduction_per_share(compact: str, gross: float) -> Optional[float]:
    """
    Per-share underwriting discount + selling commissions (e.g. $0.40 when public price is $5.00).
    """
    patterns: list[str] = [
        r"(?:underwriting\s+)?(?:discounts?\s+and\s+)?commissions?\s+(?:of|are|equal\s+to)\s+(?:up\s+to\s+)?\$\s*(\d+(?:\.\d+)?)(?:\s+per\s+share)?",
        r"(?:underwriting\s+)?(?:discounts?\s+and\s+)?commissions?\s+(?:of|equal\s+to)\s+(?:up\s+to\s+)?\$\s*(\d+(?:\.\d+)?)\s+per\s+share",
        r"\$\s*(\d+(?:\.\d+)?)\s+per\s+share\s+of\s+underwriting\s+(?:discounts?\s+and\s+)?commissions?",
        r"underwriting\s+(?:discounts?\s+and\s+)?commissions?[:\s]+\$\s*(\d+(?:\.\d+)?)(?:\s+per\s+share)?",
        r"(?:discounts?\s+and\s+)(?:selling\s+)?commissions?[:\s]+\$\s*(\d+(?:\.\d+)?)(?:\s+per\s+share)?",
        r"selling\s+concessions?[:\s]+\$\s*(\d+(?:\.\d+)?)(?:\s+per\s+share)?",
    ]
    for pat in patterns:
        for m in re.finditer(pat, compact, re.I):
            v = _try_float_str(m.group(1))
            if v is None or v <= 0 or v >= gross:
                continue
            if v > gross * 0.49:
                continue
            return round(v, 6)
    return None


def _infer_deduction_from_proceeds_per_share(compact: str, gross: float) -> Optional[float]:
    m = re.search(
        r"(?:proceeds\s+to|amount\s+per\s+share\s+(?:to|before)|before\s+(?:offering\s+)?expenses)[^$]{0,120}?"
        r"\$\s*(\d+(?:\.\d+)?)(?:\s+per\s+share)?",
        compact,
        re.I,
    )
    if not m:
        return None
    net = _try_float_str(m.group(1))
    if net is None or net <= 0 or net >= gross:
        return None
    d = gross - net
    if d <= 0 or d > gross * 0.4:
        return None
    return round(d, 6)


def extract_offering_price_detail(
    html: str, soup: Optional[BeautifulSoup] = None
) -> dict[str, Any]:
    """
    Public offering price per share, optional underwriting discount+commissions per share,
    and **net** per share used for illustrative stakeholder value.

    Many S-1s state e.g. $5.00 public price and $0.40 underwriting — net to holder context ~ $4.60.
    Also returns HTML fragment hints (``#id``) so filing doc URLs can jump to the relevant block.
    """
    if soup is None:
        soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    haystacks = ((html or ""), text)
    compact = re.sub(r"\s+", " ", text)
    gross = _extract_gross_public_offering_price(haystacks)
    if gross is None:
        return {
            "gross_per_share": None,
            "deduction_per_share": None,
            "net_per_share": None,
            "doc_anchor_offering": "",
            "doc_anchor_underwriting": "",
        }
    deduction = _extract_underwriting_deduction_per_share(compact, gross)
    if deduction is None:
        deduction = _infer_deduction_from_proceeds_per_share(compact, gross)
    net = gross - deduction if deduction is not None else gross
    if net <= 0:
        net = gross
        deduction = None
    anch_o = _find_dollar_anchor(
        soup,
        gross,
        keywords=("offering", "price", "assumed", "public", "initial", "ipo"),
    )
    anch_u = ""
    if deduction is not None:
        anch_u = _find_dollar_anchor(
            soup,
            float(deduction),
            keywords=(
                "underwriting",
                "commission",
                "discount",
                "selling",
                "concession",
            ),
        )
    return {
        "gross_per_share": round(gross, 6),
        "deduction_per_share": deduction,
        "net_per_share": round(net, 6),
        "doc_anchor_offering": anch_o,
        "doc_anchor_underwriting": anch_u,
    }


def extract_assumed_offering_price_usd(html: str) -> Optional[float]:
    """Backward compat: returns **net** per share when underwriting is disclosed, else public price."""
    d = extract_offering_price_detail(html)
    return d.get("net_per_share")


def _gem_score_and_outreach(
    *,
    holder_kind: str,
    notional: Optional[float],
    mailing: str,
    footnote_blob: str,
) -> tuple[int, str, str]:
    score = 0
    notes: list[str] = []
    if holder_kind == "entity":
        # Keep a trace score for debugging; UI filters to people.
        if notional and notional >= 10_000_000:
            score += 8
        return (min(100, score), "low_priority", "Corporate / entity holder — not a primary individual lead.")

    if holder_kind == "person":
        score += 38
    else:
        score += 12

    if notional:
        if notional >= 50_000_000:
            score += 45
        elif notional >= 10_000_000:
            score += 38
        elif notional >= 1_000_000:
            score += 28
        elif notional >= 250_000:
            score += 18
        else:
            score += 8

    if mailing:
        score += 28
        channel = "mail"
        notes.append("Filing footnote includes a line that looks like a street address — mail is the strongest channel if policy allows.")
    else:
        channel = "research"
        if holder_kind == "person":
            notes.append(
                "No footnote mailing on file — stake-only context, good as a budget add-on; verify contact (LinkedIn, intros)."
            )
        else:
            notes.append(
                "No clear personal mailing line parsed — use LinkedIn / issuer counsel intro / shared counsel paths."
            )

    if footnote_blob and any(
        k in footnote_blob.lower()
        for k in ("spouse", "wife", "husband", "director", "officer", "chief executive", "former ")
    ):
        score += 7
        notes.append("Footnote ties holder to an insider / family relationship — good personalization angle.")

    if holder_kind == "person" and not mailing and (not notional or notional < 500_000):
        channel = "research"

    return (min(100, score), channel, " ".join(notes))


def extract_beneficial_owner_rows(html: str) -> list[dict[str, Any]]:
    """
    Return list of dicts ready for DB insert (caller adds filing_id).
    Keys align with beneficial_owner_stake columns (except id, filing_id).
    """
    soup = BeautifulSoup(html, "html.parser")
    price_detail = extract_offering_price_detail(html, soup=soup)
    net_price = price_detail.get("net_per_share")
    gross_price = price_detail.get("gross_per_share")
    ded_price = price_detail.get("deduction_per_share")
    price_anchor = (price_detail.get("doc_anchor_offering") or "").strip()
    uw_anchor = (price_detail.get("doc_anchor_underwriting") or "").strip()
    out: list[dict[str, Any]] = []

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        header_idx = 0
        headers = _row_cell_texts(rows[0])
        if not _is_beneficial_owner_table(headers):
            if len(rows) > 1:
                headers = _row_cell_texts(rows[1])
                if _is_beneficial_owner_table(headers):
                    header_idx = 1
                else:
                    continue
            else:
                continue

        fn_map = _collect_footnote_map_after_table(table)
        # Name column usually first with "name" or left-most non-numeric header
        name_i = 0
        for i, h in enumerate(headers):
            hl = h.lower()
            if "name" in hl and "company" not in hl:
                name_i = i
                break

        pct_i: Optional[int] = None
        share_i: Optional[int] = None
        for i, h in enumerate(headers):
            hl = h.lower()
            if i == name_i:
                continue
            if "%" in h or "percent" in hl:
                pct_i = i
            elif "share" in hl or "common" in hl or "stock" in hl:
                share_i = i
        if share_i is None:
            for i in range(len(headers)):
                if i == name_i or i == pct_i:
                    continue
                if share_i is None:
                    share_i = i
                    break

        for tr in rows[header_idx + 1 :]:
            cells = _row_cell_texts(tr)
            if len(cells) <= name_i:
                continue
            raw_name = cells[name_i]
            low = raw_name.lower()
            if not raw_name or len(raw_name) > 200:
                continue
            if low.startswith("name") and len(raw_name) < 24:
                continue
            if "total" == low.strip() and len(cells) <= 2:
                continue
            if "beneficial owners of more than" in low:
                continue

            clean = _strip_footnote_marks_from_name(raw_name)
            refs = _footnote_refs_from_cell(raw_name)
            fn_parts: list[str] = []
            for ri in refs:
                if ri in fn_map:
                    fn_parts.append(fn_map[ri])
            footnote_text = " ".join(fn_parts).strip()
            mailing = _parse_us_street_line(footnote_text)

            display = clean.strip()
            kind = classify_holder_kind(display)

            sh_val: Optional[float] = None
            if share_i is not None and len(cells) > share_i:
                sh_val = _parse_floatish_shares(cells[share_i])
            if sh_val is None:
                for j, cell in enumerate(cells):
                    if j == name_i:
                        continue
                    cand = _parse_floatish_shares(cell)
                    if cand is not None and cand >= 1_000:
                        sh_val = cand
                        break
            pct_val: Optional[float] = None
            if pct_i is not None and len(cells) > pct_i:
                pct_val = _parse_percent(cells[pct_i])

            notional: Optional[float] = None
            if net_price is not None and sh_val is not None:
                notional = sh_val * float(net_price)

            mail_anchor = ""
            if mailing and refs:
                for ri in refs:
                    mail_anchor = _find_footnote_anchor(soup, ri, mailing) or ""
                    if mail_anchor:
                        break

            gem, channel, notes = _gem_score_and_outreach(
                holder_kind=kind,
                notional=notional,
                mailing=mailing,
                footnote_blob=footnote_text,
            )

            out.append(
                {
                    "holder_name": display[:400],
                    "holder_kind": kind,
                    "raw_name_cell": raw_name[:500],
                    "shares_before_offering": sh_val,
                    "pct_beneficial": pct_val,
                    "footnote_markers": json.dumps(refs),
                    "footnote_text": footnote_text[:6000] if footnote_text else "",
                    "mailing_address": mailing[:500] if mailing else "",
                    "notional_usd_est": notional,
                    "offering_price_used": net_price,
                    "offering_price_gross_usd": gross_price,
                    "offering_underwriting_deduction_usd": ded_price,
                    "offering_price_doc_anchor": price_anchor or "",
                    "underwriting_doc_anchor": uw_anchor or "",
                    "mailing_footnote_doc_anchor": mail_anchor or "",
                    "beneficial_parse_build": STAKE_PARSE_REVISION,
                    "gem_score": gem,
                    "outreach_recommended": channel,
                    "outreach_notes": notes[:1500],
                }
            )

    # Dedupe by holder_name + kind keeping highest gem
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for row in out:
        k = (row["holder_name"].lower(), row["holder_kind"])
        prev = by_key.get(k)
        if prev is None or int(row["gem_score"]) > int(prev["gem_score"]):
            by_key[k] = row
    return sorted(by_key.values(), key=lambda r: (-int(r["gem_score"]), -(r["notional_usd_est"] or 0)))


def beneficial_owner_row_matching_display_name(
    html: str, display_name: str
) -> Optional[dict[str, Any]]:
    """
    Best *person* row from beneficial-ownership tables whose holder name matches
    ``display_name`` using the same loose match as NEO grant / option HTML parsers.
    Used when the DB officer↔stake link is missing but the primary document is loaded.
    """
    from wealth_leads.compensation import _person_cell_matches_name

    dn = (display_name or "").strip()
    if not dn:
        return None
    rows = extract_beneficial_owner_rows(html)
    best: Optional[dict[str, Any]] = None
    best_sh = -1.0
    for r in rows:
        if (r.get("holder_kind") or "") != "person":
            continue
        hn = (r.get("holder_name") or "").strip()
        if not hn or not _person_cell_matches_name(hn, dn):
            continue
        try:
            fv = float(r.get("shares_before_offering") or 0)
        except (TypeError, ValueError):
            fv = 0.0
        if fv > best_sh:
            best_sh = fv
            best = r
    return best


def beneficial_owner_rows_for_db(html: str) -> list[dict[str, Any]]:
    """Alias for sync — only individual gems are surfaced in UI; entities stored with low scores."""
    return extract_beneficial_owner_rows(html)
