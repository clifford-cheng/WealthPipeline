"""
LLM-assisted extraction from S-1 HTML when table-based heuristics miss or mangle data.

**Cost:** Cloud APIs (OpenAI, Anthropic) bill per token. **Ollama/local** has no API fee;
  you pay in GPU/RAM and time.

**Providers** (WEALTH_LEADS_S1_AI_PROVIDER):
  - openai (default): OPENAI_API_KEY; WEALTH_LEADS_S1_AI_MODEL (default gpt-4o-mini)
  - anthropic: ANTHROPIC_API_KEY; WEALTH_LEADS_ANTHROPIC_S1_MODEL
  - ollama or local: run Ollama on your PC; WEALTH_LEADS_OLLAMA_URL (default
    http://127.0.0.1:11434); WEALTH_LEADS_OLLAMA_MODEL (default llama3.1)

For cloud, read the vendor’s API / data terms. Local keeps filing text on your machine.

Run:  py -m wealth_leads enrich-s1-ai --help

This does not run automatically on sync (cost + latency). Use after sync or on
filings where neo_compensation is empty.

Ollama/OpenAI/Anthropic are **not** "fed" the corpus into the model weights each time:
each run sends a **prompt** (a plain-text excerpt of the filing) and receives **JSON**
back. By default we build that excerpt with phrase-based **windows** for long S-1s.
Set ``WEALTH_LEADS_S1_AI_DOCUMENT_MODE=linear`` to send **contiguous text from the
start** instead (cover first), or ``bookend`` for head+tail; tune
``WEALTH_LEADS_S1_AI_MAX_CHARS`` to match your model context.

The model returns a structured lead_intel block (offering, principal holders,
related parties, use of proceeds, auditor/counsel) stored as JSON on the filing
row for the pipeline drawer—plus NEO/officers/bios/issuer fields written to
existing tables.
"""
from __future__ import annotations

import json
import math
import re
import sqlite3
from typing import Any, Optional

from bs4 import BeautifulSoup

from wealth_leads.compensation import NeoCompRow
from wealth_leads.config import (
    anthropic_api_key,
    anthropic_s1_model,
    ollama_base_url,
    ollama_s1_model,
    openai_api_key,
    openai_s1_model,
    s1_ai_document_mode,
    s1_ai_max_chars,
    s1_ai_provider,
)
from wealth_leads.db import (
    replace_neo_compensation,
    replace_officers,
    replace_person_management_narratives,
    update_filing_director_term_summary,
    update_filing_issuer_industry,
    update_filing_issuer_meta,
    update_filing_issuer_summary,
    update_filing_s1_llm_lead_pack,
)
from wealth_leads.serve import _norm_person_name


def _s1_html_to_plain(html: str) -> str:
    """Strip HTML to normalized plain text (full document, not length-capped)."""
    soup = BeautifulSoup(html or "", "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text("\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text


def s1_html_to_linear_document_text(html: str, *, max_chars: int) -> str:
    """Contiguous excerpt from the beginning: cover page and prospectus flow stay in reading order."""
    return _s1_html_to_plain(html)[:max_chars]


def s1_html_to_bookend_document_text(html: str, *, max_chars: int) -> str:
    """Head + tail when the filing exceeds max_chars (summary comp often appears later)."""
    t = _s1_html_to_plain(html)
    n = len(t)
    if n <= max_chars:
        return t
    sep = "\n\n[ ... middle of filing omitted for length ... ]\n\n"
    budget = max_chars - len(sep)
    if budget < 8_000:
        return t[:max_chars]
    head_n = int(budget * 0.72)
    tail_n = budget - head_n
    return t[:head_n] + sep + t[-tail_n:]


def s1_html_to_document_text(html: str, *, max_chars: int = 100_000) -> str:
    """Strip HTML to plain text. Long filings: merge prospectus head + compensation / ownership windows."""
    text = _s1_html_to_plain(html)
    n = len(text)
    if n <= max_chars:
        return text

    low = text.casefold()
    head_len = min(24_000, max_chars // 3)
    intervals: list[tuple[int, int]] = [(0, head_len)]

    for phrase, back, fwd in (
        ("principal executive offices", 14_000, 32_000),
        ("principal executive office", 14_000, 32_000),
        ("principal place of business", 12_000, 28_000),
        ("mailing address of our principal", 10_000, 26_000),
        ("our corporate headquarters", 8_000, 24_000),
        ("summary compensation table", 18_000, 65_000),
        ("summary compensation", 18_000, 60_000),
        ("named executive officer", 14_000, 55_000),
        ("named executive officers", 14_000, 55_000),
        ("compensation discussion and analysis", 12_000, 50_000),
        ("executive compensation", 10_000, 45_000),
        ("director compensation", 10_000, 40_000),
        ("grant of plan-based awards", 8_000, 38_000),
        ("principal stockholders", 8_000, 45_000),
        ("security ownership of certain beneficial owner", 8_000, 45_000),
        ("certain relationships and related", 8_000, 40_000),
        ("underwriting", 8_000, 38_000),
        ("use of proceeds", 8_000, 38_000),
        ("plan of distribution", 6_000, 28_000),
        ("experts", 4_000, 20_000),
    ):
        pos = 0
        pl = len(phrase)
        while True:
            idx = low.find(phrase, pos)
            if idx < 0:
                break
            lo = max(0, idx - back)
            hi = min(n, idx + fwd)
            intervals.append((lo, hi))
            pos = idx + max(pl, 4)

    intervals.sort()
    merged: list[tuple[int, int]] = []
    for lo, hi in intervals:
        if not merged or lo > merged[-1][1] + 200:
            merged.append((lo, hi))
        else:
            a, b = merged[-1]
            merged[-1] = (a, max(b, hi))

    parts: list[str] = []
    for i, (lo, hi) in enumerate(merged):
        if i and lo > merged[i - 1][1]:
            parts.append("\n\n[ ... omitted ... ]\n\n")
        parts.append(text[lo:hi])
    out = "".join(parts)
    if len(out) <= max_chars:
        return out

    # Hard cap: ALWAYS keep prospectus front matter (cover + registrant HQ), then add
    # highest-scoring slices. Previously we only ranked comp-heavy windows — the model
    # often never saw headquarters_address at all on long S-1s.
    head_keep = min(16_000, n, max(8_000, max_chars // 5))
    prefix = text[0:head_keep]
    sep = "\n\n---\n\n"
    budget = max_chars - len(prefix) - len(sep) - 40
    scored: list[tuple[int, tuple[int, int]]] = []
    for span in merged:
        lo, hi = span
        slug = text[lo:hi].casefold()
        score = 0
        if lo == 0:
            score += 14
        elif lo < 10_000:
            score += 9
        if "principal executive" in slug or "principal office" in slug:
            score += 12
        if "headquarter" in slug or "mailing address" in slug:
            score += 8
        if "registrant" in slug and "address" in slug:
            score += 5
        if "summary compensation" in slug:
            score += 5
        if "compensation" in slug:
            score += 2
        if "stock" in slug and "award" in slug:
            score += 1
        if "stockholder" in slug or "ownership" in slug:
            score += 1
        scored.append((score, span))
    scored.sort(key=lambda x: (-x[0], x[1][0]))
    acc: list[str] = [prefix]
    used = len(prefix)
    for _score, (lo, hi) in scored:
        if hi <= head_keep:
            continue
        lo = max(lo, head_keep)
        if lo >= hi:
            continue
        chunk = text[lo:hi]
        need = len(chunk) + len(sep)
        if used + need <= max_chars - 20:
            acc.append(chunk)
            used += need
        elif max_chars - used > 10_000:
            room = max(2000, max_chars - used - len(sep) - 50)
            acc.append(chunk[:room] + "\n\n[... truncated ...]\n")
            break
    return sep.join(acc)[:max_chars] if acc else text[:max_chars]


def html_document_for_s1_llm(html: str) -> str:
    """
    Build the plain-text blob sent to the LLM for enrich-s1-ai.

    Controlled by WEALTH_LEADS_S1_AI_DOCUMENT_MODE (windows | linear | bookend) and
    WEALTH_LEADS_S1_AI_MAX_CHARS.
    """
    max_c = s1_ai_max_chars()
    mode = s1_ai_document_mode()
    if mode == "linear":
        return s1_html_to_linear_document_text(html, max_chars=max_c)
    if mode == "bookend":
        return s1_html_to_bookend_document_text(html, max_chars=max_c)
    return s1_html_to_document_text(html, max_chars=max_c)


_SYSTEM_PROMPT = """You are an expert at reading U.S. SEC Form S-1 registration statements for
pre-IPO / wealth-management lead research. Your job is to extract EVERYTHING in the user schema
that is supported by the document excerpt—issuer facts, people, pay, ownership, offering context,
and professional firms—so downstream software can build reviewable lead profiles.

Rules:
- Use ONLY the excerpt; do not invent names, amounts, addresses, titles, or law firms.
- Where the excerpt is silent, use null or empty arrays (do not guess).
- For money in summary_compensation, use numbers (not strings). Use fiscal_year as a number.
- Person names: plain English as in the filing (avoid ALL CAPS if the filing uses mixed case).
- lead_intel text fields: concise prose taken from or tightly summarized from the excerpt; cite
  ranges as the filing states them (e.g. price "X to Y per share" as narrative text).
- Return one JSON object with exactly the top-level keys in the user schema (no markdown)."""


_USER_SCHEMA = """Analyze this S-1 excerpt and return a single JSON object with exactly these keys:

{
  "issuer": {
    "headquarters_address": string or null,
    "website": string or null,
    "industry_description": string or null,
    "business_summary": string or null
  },
  "director_term_summary": string or null,
  "executive_officers": [
    {"name": string, "title": string, "age": number or null}
  ],
  "summary_compensation": [
    {
      "name": string,
      "title_or_role": string or null,
      "fiscal_year": number,
      "salary": number or null,
      "bonus": number or null,
      "stock_awards": number or null,
      "option_awards": number or null,
      "non_equity_incentive": number or null,
      "pension_or_deferred": number or null,
      "other_compensation": number or null,
      "total": number or null
    }
  ],
  "management_bios": [
    {"name": string, "role_heading": string or null, "bio_text": string}
  ],
  "lead_intel": {
    "offering": {
      "summary": string or null,
      "shares_narrative": string or null,
      "price_range_narrative": string or null,
      "underwriters": string or null
    },
    "ownership": {
      "principal_stockholders": [
        {
          "name": string,
          "position_or_role": string or null,
          "shares_or_percent": string or null
        }
      ]
    },
    "related_party_transactions": string or null,
    "use_of_proceeds": string or null,
    "auditor": string or null,
    "legal_counsel": string or null,
    "material_contracts_note": string or null
  },
  "notes": string
}

Section guidance:
- issuer.headquarters_address: the registrant principal executive office / mailing address exactly as in the
  filing—almost always on the cover page, inside the prospectus summary header, or Item 1 / Business within
  the first pages (street, city, state, ZIP). Do not paste filing dates, effective dates, or "as of" text
  into this field. issuer.website / industry / business_summary: Prospectus summary and Business sections.
- summary_compensation: one object per person per fiscal year in Summary Compensation Table(s); "—" → null.
- management_bios: biographies under Management / Directors (~800 chars per person max).
- executive_officers: officer / director roster with ages if listed.
- lead_intel.offering: registered offering size, share classes, price talk, underwriters (from cover,
  underwriting, MD&A or offering sections if present in excerpt).
- lead_intel.ownership.principal_stockholders: beneficial owners / >5% holders if in excerpt;
  shares_or_percent as stated (string OK, e.g. "12.3%" or "1,200,000 shares").
- lead_intel.related_party_transactions: short summary of related-party deals naming parties if in excerpt.
- lead_intel.use_of_proceeds: brief use-of-proceeds if in excerpt.
- lead_intel.auditor / legal_counsel: firm names if clearly stated for the offering or audit opinion.
- lead_intel.material_contracts_note: only if excerpt mentions key employment / change-of-control hooks.
- notes: extraction caveats (e.g. "CD&A truncated in excerpt").

If a subsection is missing from the excerpt, use null or empty arrays inside lead_intel."""


def _parse_json_from_llm_text(text: str) -> dict[str, Any]:
    """Parse model output; strip optional ```json fences."""
    t = (text or "").strip()
    if t.startswith("```"):
        t = re.sub(r"^```(?:json)?\s*", "", t, flags=re.I)
        t = re.sub(r"\s*```\s*$", "", t)
    return json.loads(t)


def _num(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        x = float(v)
        return None if math.isnan(x) else x
    if isinstance(v, str):
        t = v.strip().replace(",", "").replace("$", "")
        if not t or t in ("—", "-", "–", "N/A", "n/a", "*"):
            return None
        try:
            return float(t)
        except ValueError:
            return None
    return None


def _int_year(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        y = int(v)
        return y if 1990 <= y <= 2040 else None
    except (TypeError, ValueError):
        return None


def _neo_rows_from_ai(data: dict[str, Any]) -> list[NeoCompRow]:
    raw = data.get("summary_compensation") or []
    if not isinstance(raw, list):
        return []
    seen: set[tuple[str, int]] = set()
    out: list[NeoCompRow] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = (item.get("name") or "").strip()
        fy = _int_year(item.get("fiscal_year"))
        if not name or fy is None:
            continue
        key = (name.lower(), fy)
        if key in seen:
            continue
        seen.add(key)
        role = (item.get("title_or_role") or "").strip() or None
        sal = _num(item.get("salary"))
        bonus = _num(item.get("bonus"))
        stock = _num(item.get("stock_awards"))
        opt = _num(item.get("option_awards"))
        ne = _num(item.get("non_equity_incentive"))
        pen = _num(item.get("pension_or_deferred"))
        oth = _num(item.get("other_compensation"))
        tot = _num(item.get("total"))
        out.append(
            NeoCompRow(
                person_name=name,
                role_hint=role,
                fiscal_year=fy,
                salary=sal,
                bonus=bonus,
                stock_awards=stock,
                option_awards=opt,
                non_equity_incentive=ne,
                pension_change=pen,
                other_comp=oth,
                total=tot,
            )
        )
    out.sort(key=lambda r: (r.person_name.lower(), -r.fiscal_year))
    return out


def _neo_db_tuples(filing_id: int, comps: list[NeoCompRow]) -> list[tuple]:
    rows: list[tuple] = []
    for c in comps:
        if c.stock_awards is not None or c.option_awards is not None:
            eq = 0.0
            if c.stock_awards is not None:
                eq += c.stock_awards
            if c.option_awards is not None:
                eq += c.option_awards
        else:
            eq = None
        rows.append(
            (
                filing_id,
                c.person_name,
                c.role_hint,
                c.fiscal_year,
                c.salary,
                c.bonus,
                c.stock_awards,
                c.option_awards,
                c.non_equity_incentive,
                c.pension_change,
                c.other_comp,
                c.total,
                eq,
                "llm_s1_extract",
            )
        )
    return rows


def _officers_from_ai(data: dict[str, Any]) -> list[tuple[str, str, str, Optional[int]]]:
    from wealth_leads.person_quality import is_acceptable_lead_person_name

    raw = data.get("executive_officers") or []
    if not isinstance(raw, list):
        return []
    out: list[tuple[str, str, str, Optional[int]]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = (item.get("name") or "").strip()
        title = (item.get("title") or "").strip()
        if not name or not title:
            continue
        if not is_acceptable_lead_person_name(name):
            continue
        age = item.get("age")
        age_i: Optional[int] = None
        if age is not None:
            try:
                age_i = int(age)
                if not (18 <= age_i <= 100):
                    age_i = None
            except (TypeError, ValueError):
                age_i = None
        out.append((name, title, "llm_s1_extract", age_i))
    return out


def _bios_from_ai(data: dict[str, Any]) -> list[dict[str, str]]:
    raw = data.get("management_bios") or []
    if not isinstance(raw, list):
        return []
    out: list[dict[str, str]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = (item.get("name") or "").strip()
        bio = (item.get("bio_text") or "").strip()
        if not name or not bio:
            continue
        role = (item.get("role_heading") or "").strip()
        out.append(
            {
                "person_name": name[:400],
                "person_name_norm": _norm_person_name(name),
                "role_heading": role[:400],
                "bio_text": bio[:15_000],
            }
        )
    return out


def _empty_lead_intel_val(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str):
        return not v.strip()
    if isinstance(v, (list, dict)):
        return len(v) == 0
    return False


def _lead_intel_json_for_db(data: dict[str, Any]) -> Optional[str]:
    """Serialize lead_intel subtree for filings.s1_llm_lead_pack (omit empty keys)."""
    raw = data.get("lead_intel")
    if not isinstance(raw, dict):
        return None
    offering = raw.get("offering")
    ownership = raw.get("ownership")
    pack: dict[str, Any] = {}
    if isinstance(offering, dict):
        of = {k: v for k, v in offering.items() if not _empty_lead_intel_val(v)}
        if of:
            pack["offering"] = of
    if isinstance(ownership, dict):
        holders = ownership.get("principal_stockholders")
        if isinstance(holders, list) and holders:
            pack["ownership"] = {"principal_stockholders": holders[:80]}
    for key in (
        "related_party_transactions",
        "use_of_proceeds",
        "auditor",
        "legal_counsel",
        "material_contracts_note",
    ):
        v = raw.get(key)
        if not _empty_lead_intel_val(v):
            pack[key] = v
    if not pack:
        return None
    return json.dumps(pack, ensure_ascii=False)


def call_openai_extract(
    document_text: str,
    *,
    company_name: str,
    accession: str,
) -> dict[str, Any]:
    import requests

    key = openai_api_key()
    if not key:
        raise RuntimeError(
            "No API key: set WEALTH_LEADS_OPENAI_API_KEY or OPENAI_API_KEY"
        )
    model = openai_s1_model()
    user_msg = (
        f"Company (from index metadata, may help disambiguate): {company_name}\n"
        f"Accession: {accession}\n\n"
        f"{_USER_SCHEMA}\n\n--- DOCUMENT EXCERPT ---\n{document_text}"
    )
    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": user_msg},
            ],
            "response_format": {"type": "json_object"},
            "temperature": 0.05,
            "max_tokens": 16384,
        },
        timeout=300,
    )
    r.raise_for_status()
    payload = r.json()
    content = payload["choices"][0]["message"]["content"]
    return _parse_json_from_llm_text(content)


def call_anthropic_extract(
    document_text: str,
    *,
    company_name: str,
    accession: str,
) -> dict[str, Any]:
    import requests

    key = anthropic_api_key()
    if not key:
        raise RuntimeError(
            "No API key: set WEALTH_LEADS_ANTHROPIC_API_KEY or ANTHROPIC_API_KEY"
        )
    model = anthropic_s1_model()
    user_msg = (
        f"{_USER_SCHEMA}\n\n"
        "Respond with ONLY a valid JSON object (no markdown code fences, no commentary).\n\n"
        f"Company (from index metadata): {company_name}\n"
        f"Accession: {accession}\n\n"
        f"--- DOCUMENT EXCERPT ---\n{document_text}"
    )
    system = (
        _SYSTEM_PROMPT
        + " Your entire reply must be parseable as JSON (object at root)."
    )
    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "max_tokens": 16384,
            "system": system,
            "messages": [{"role": "user", "content": user_msg}],
        },
        timeout=300,
    )
    r.raise_for_status()
    data = r.json()
    parts: list[str] = []
    for block in data.get("content") or []:
        if isinstance(block, dict) and block.get("type") == "text":
            parts.append(block.get("text") or "")
    return _parse_json_from_llm_text("".join(parts))


def call_ollama_extract(
    document_text: str,
    *,
    company_name: str,
    accession: str,
) -> dict[str, Any]:
    import requests

    base = ollama_base_url()
    model = ollama_s1_model()
    user_msg = (
        f"{_USER_SCHEMA}\n\n"
        "Reply with a single JSON object only (no markdown fences, no explanation).\n\n"
        f"Company: {company_name}\n"
        f"Accession: {accession}\n\n"
        f"--- DOCUMENT EXCERPT ---\n{document_text}"
    )
    url = f"{base}/api/chat"
    body: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
        "stream": False,
        "options": {"temperature": 0.1},
    }
    # Ollama 0.5+ JSON mode (helps valid JSON; upgrade Ollama if you get API errors)
    body["format"] = "json"
    r = requests.post(url, json=body, timeout=900)
    r.raise_for_status()
    data = r.json()
    content = (data.get("message") or {}).get("content") or ""
    return _parse_json_from_llm_text(content)


def call_llm_extract(
    document_text: str,
    *,
    company_name: str,
    accession: str,
) -> dict[str, Any]:
    prov = s1_ai_provider()
    if prov == "anthropic":
        return call_anthropic_extract(
            document_text,
            company_name=company_name,
            accession=accession,
        )
    if prov == "ollama":
        return call_ollama_extract(
            document_text,
            company_name=company_name,
            accession=accession,
        )
    return call_openai_extract(
        document_text,
        company_name=company_name,
        accession=accession,
    )


def apply_ai_payload_to_filing(
    conn: sqlite3.Connection,
    filing_id: int,
    data: dict[str, Any],
    *,
    replace_neo: bool,
    replace_officers: bool,
    replace_bios: bool,
    allow_empty_neo: bool,
) -> dict[str, Any]:
    """Write AI JSON into existing filing tables."""
    stats: dict[str, Any] = {
        "neo_rows": 0,
        "officers": 0,
        "bios": 0,
        "issuer_updates": False,
        "lead_intel_stored": False,
    }
    issuer = data.get("issuer") if isinstance(data.get("issuer"), dict) else {}
    if issuer:
        from wealth_leads.territory import is_plausible_registrant_headquarters

        hq = (issuer.get("headquarters_address") or "").strip()
        web = (issuer.get("website") or "").strip()
        hq_ok = is_plausible_registrant_headquarters(hq) if hq else False
        if (hq_ok and hq) or web:
            update_filing_issuer_meta(
                conn,
                filing_id,
                website=web,
                headquarters=hq if hq_ok else "",
                headquarters_force=bool(hq_ok and hq),
            )
            stats["issuer_updates"] = True
        ind = (issuer.get("industry_description") or "").strip()
        if ind:
            update_filing_issuer_industry(conn, filing_id, ind)
            stats["issuer_updates"] = True
        summ = (issuer.get("business_summary") or "").strip()
        if summ:
            update_filing_issuer_summary(conn, filing_id, summ[:2000])
            stats["issuer_updates"] = True

    dts = data.get("director_term_summary")
    if isinstance(dts, str) and dts.strip():
        update_filing_director_term_summary(conn, filing_id, dts.strip()[:8000])

    neo = _neo_rows_from_ai(data)
    cur = conn.execute(
        "SELECT COUNT(*) AS n FROM neo_compensation WHERE filing_id = ?", (filing_id,)
    ).fetchone()
    have_neo = int(cur["n"] if hasattr(cur, "keys") else cur[0])

    if neo:
        if replace_neo or have_neo == 0:
            replace_neo_compensation(conn, filing_id, _neo_db_tuples(filing_id, neo))
            stats["neo_rows"] = len(neo)
        else:
            stats["skipped_neo"] = "existing NEO rows kept (use --replace-neo to overwrite)"
    elif replace_neo and allow_empty_neo:
        replace_neo_compensation(conn, filing_id, [])
        stats["neo_rows"] = 0
    elif replace_neo and not neo:
        stats["warning"] = (
            "AI returned no summary_compensation; existing NEO rows unchanged "
            "(pass --allow-empty-neo to clear)"
        )

    off = _officers_from_ai(data)
    if off:
        oc = conn.execute(
            "SELECT COUNT(*) AS n FROM officers WHERE filing_id = ?", (filing_id,)
        ).fetchone()
        have_o = int(oc["n"] if hasattr(oc, "keys") else oc[0])
        if replace_officers or have_o == 0:
            replace_officers(conn, filing_id, off)
            stats["officers"] = len(off)
        else:
            stats["skipped_officers"] = "use --replace-officers to overwrite"

    bios = _bios_from_ai(data)
    if bios:
        bc = conn.execute(
            "SELECT COUNT(*) AS n FROM person_management_narrative WHERE filing_id = ?",
            (filing_id,),
        ).fetchone()
        have_b = int(bc["n"] if hasattr(bc, "keys") else bc[0])
        if replace_bios or have_b == 0:
            replace_person_management_narratives(conn, filing_id, bios)
            stats["bios"] = len(bios)
        else:
            stats["skipped_bios"] = "use --replace-bios to overwrite"

    lj = _lead_intel_json_for_db(data)
    if lj:
        update_filing_s1_llm_lead_pack(conn, filing_id, lj)
        stats["lead_intel_stored"] = True

    return stats


def enrich_filing_with_llm(
    conn: sqlite3.Connection,
    filing_id: int,
    html: str,
    *,
    company_name: str,
    accession: str,
    replace_neo: bool,
    replace_officers: bool,
    replace_bios: bool,
    allow_empty_neo: bool,
) -> dict[str, Any]:
    text = html_document_for_s1_llm(html)
    data = call_llm_extract(text, company_name=company_name, accession=accession)
    stats = apply_ai_payload_to_filing(
        conn,
        filing_id,
        data,
        replace_neo=replace_neo,
        replace_officers=replace_officers,
        replace_bios=replace_bios,
        allow_empty_neo=allow_empty_neo,
    )
    stats["notes"] = (data.get("notes") or "")[:500]
    return stats


def run_enrich_s1_ai(
    *,
    limit: int,
    filing_id: Optional[int],
    only_missing_neo: bool,
    issuer_refresh: bool,
    replace_neo: bool,
    replace_officers: bool,
    replace_bios: bool,
    allow_empty_neo: bool,
    dry_run: bool,
) -> int:
    """
    Process up to `limit` S-1 filings. Returns count attempted.
    """
    import sys

    import requests

    from wealth_leads.db import connect
    from wealth_leads.parse_index import canonical_filing_document_url
    from wealth_leads.sec_client import get_text

    if not dry_run:
        prov = s1_ai_provider()
        if prov == "anthropic" and not anthropic_api_key():
            print(
                "Set WEALTH_LEADS_ANTHROPIC_API_KEY or ANTHROPIC_API_KEY "
                "(or WEALTH_LEADS_S1_AI_PROVIDER=openai or ollama).",
                file=sys.stderr,
            )
            return 0
        if prov == "openai" and not openai_api_key():
            print(
                "Set WEALTH_LEADS_OPENAI_API_KEY or OPENAI_API_KEY "
                "(or WEALTH_LEADS_S1_AI_PROVIDER=anthropic or ollama).",
                file=sys.stderr,
            )
            return 0

    session = requests.Session()
    q = """
        SELECT f.id, f.primary_doc_url, f.company_name, f.accession, f.form_type
        FROM filings f
        WHERE f.primary_doc_url IS NOT NULL
          AND (UPPER(f.form_type) LIKE 'S-1%')
        """
    params: list[Any] = []
    if filing_id is not None:
        q += " AND f.id = ?"
        params.append(filing_id)
    if only_missing_neo:
        q += """ AND (SELECT COUNT(*) FROM neo_compensation c WHERE c.filing_id = f.id) = 0"""
    if issuer_refresh:
        q += """ AND (
            LENGTH(TRIM(COALESCE(f.issuer_headquarters, ''))) < 12
            OR TRIM(COALESCE(f.issuer_headquarters, '')) = ''
            OR f.s1_llm_lead_pack IS NULL
            OR TRIM(COALESCE(f.s1_llm_lead_pack, '')) = ''
        )"""
    q += " ORDER BY COALESCE(f.filing_date, '') DESC, f.id DESC LIMIT ?"
    lim = 1 if filing_id is not None else max(1, min(limit, 200))
    params.append(lim)

    n_done = 0
    with connect() as conn:
        rows = list(conn.execute(q, tuple(params)).fetchall())
        if not rows:
            print("enrich-s1-ai: no filings matched.", file=sys.stderr)
            return 0
        for r in rows:
            fid = int(r["id"])
            raw_u = r["primary_doc_url"]
            url = canonical_filing_document_url(raw_u)
            if not url:
                print(f"[skip] filing {fid}: no URL", file=sys.stderr)
                continue
            try:
                html = get_text(url, session=session)
            except Exception as e:
                print(f"[warn] fetch filing {fid}: {e}", file=sys.stderr)
                continue
            co = r["company_name"] or ""
            acc = r["accession"] or ""
            print(
                f"enrich-s1-ai [{s1_ai_provider()}] mode={s1_ai_document_mode()}: "
                f"filing id={fid} {co} ({acc}) …",
                file=sys.stderr,
            )
            if dry_run:
                text = html_document_for_s1_llm(html)
                print(
                    f"  dry-run: would send ~{len(text)} chars "
                    f"(max {s1_ai_max_chars()}, mode={s1_ai_document_mode()})",
                    file=sys.stderr,
                )
                n_done += 1
                continue
            try:
                stats = enrich_filing_with_llm(
                    conn,
                    fid,
                    html,
                    company_name=co,
                    accession=acc,
                    replace_neo=replace_neo,
                    replace_officers=replace_officers,
                    replace_bios=replace_bios,
                    allow_empty_neo=allow_empty_neo,
                )
            except Exception as e:
                print(f"[error] filing {fid}: {e}", file=sys.stderr)
                continue
            print(f"  → {stats}", file=sys.stderr)
            n_done += 1
    return n_done

