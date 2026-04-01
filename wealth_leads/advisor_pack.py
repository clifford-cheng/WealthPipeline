"""
Advisor-facing packs: human-readable company scale context (from filing text via LLM),
merged executive story (S-1 + website), and corporate email pattern guessing + optional
SMTP RCPT checks (many servers are non-authoritative — results are hints only).
"""
from __future__ import annotations

import json
import re
import smtplib
import socket
from email.utils import parseaddr
from typing import Any, Optional

try:
    import dns.resolver as _dns_resolver  # type: ignore[import-untyped]
except ImportError:  # pragma: no cover - optional for SMTP MX lookups
    _dns_resolver = None
import requests

from wealth_leads.config import (
    anthropic_api_key,
    anthropic_s1_model,
    ollama_base_url,
    ollama_s1_model,
    openai_api_key,
    openai_s1_model,
    s1_ai_provider,
    user_agent,
)


def _advisor_parse_json(text: str) -> dict[str, Any]:
    t = (text or "").strip()
    if t.startswith("```"):
        t = re.sub(r"^```(?:json)?\s*", "", t, flags=re.I)
        t = re.sub(r"\s*```\s*$", "", t)
    return json.loads(t)


def advisor_llm_available() -> bool:
    """True if enrich / advisor LLM steps can run for the configured provider."""
    p = s1_ai_provider()
    if p in ("ollama", "local"):
        return True
    if p in ("anthropic", "claude"):
        return bool(anthropic_api_key())
    return bool(openai_api_key())


def _openai_advisor_json(
    *,
    system: str,
    user: str,
    temperature: float,
    max_tokens: int,
    timeout: float,
) -> dict[str, Any]:
    key = openai_api_key()
    if not key:
        raise RuntimeError(
            "Provider is OpenAI but no key: set OPENAI_API_KEY or WEALTH_LEADS_OPENAI_API_KEY"
        )
    model = openai_s1_model()
    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "response_format": {"type": "json_object"},
            "temperature": temperature,
            "max_tokens": max_tokens,
        },
        timeout=timeout,
    )
    r.raise_for_status()
    return json.loads(r.json()["choices"][0]["message"]["content"])


def _openai_advisor_text(
    *,
    system: str,
    user: str,
    temperature: float,
    max_tokens: int,
    timeout: float,
) -> str:
    key = openai_api_key()
    if not key:
        raise RuntimeError(
            "Provider is OpenAI but no key: set OPENAI_API_KEY or WEALTH_LEADS_OPENAI_API_KEY"
        )
    model = openai_s1_model()
    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        },
        timeout=timeout,
    )
    r.raise_for_status()
    return (r.json()["choices"][0]["message"]["content"] or "").strip()


def _ollama_advisor_json(
    *,
    system: str,
    user: str,
    temperature: float,
    max_tokens: int,
    timeout: float,
) -> dict[str, Any]:
    url = f"{ollama_base_url()}/api/chat"
    body: dict[str, Any] = {
        "model": ollama_s1_model(),
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "stream": False,
        "format": "json",
        "options": {"temperature": temperature, "num_predict": max_tokens},
    }
    r = requests.post(url, json=body, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    content = (data.get("message") or {}).get("content") or ""
    return _advisor_parse_json(content)


def _ollama_advisor_text(
    *,
    system: str,
    user: str,
    temperature: float,
    max_tokens: int,
    timeout: float,
) -> str:
    url = f"{ollama_base_url()}/api/chat"
    body: dict[str, Any] = {
        "model": ollama_s1_model(),
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "stream": False,
        "options": {"temperature": temperature, "num_predict": max_tokens},
    }
    r = requests.post(url, json=body, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    return ((data.get("message") or {}).get("content") or "").strip()


def _anthropic_advisor_json(
    *,
    system: str,
    user: str,
    temperature: float,
    max_tokens: int,
    timeout: float,
) -> dict[str, Any]:
    key = anthropic_api_key()
    if not key:
        raise RuntimeError(
            "Provider is Anthropic but no key: set ANTHROPIC_API_KEY or WEALTH_LEADS_ANTHROPIC_API_KEY"
        )
    model = anthropic_s1_model()
    sys2 = (
        system
        + " Your entire reply must be a single JSON object (no markdown, no commentary)."
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
            "max_tokens": max(1024, min(max_tokens * 2, 8192)),
            "system": sys2,
            "messages": [{"role": "user", "content": user}],
            "temperature": temperature,
        },
        timeout=timeout,
    )
    r.raise_for_status()
    data = r.json()
    parts: list[str] = []
    for block in data.get("content") or []:
        if isinstance(block, dict) and block.get("type") == "text":
            parts.append(block.get("text") or "")
    return _advisor_parse_json("".join(parts))


def _anthropic_advisor_text(
    *,
    system: str,
    user: str,
    temperature: float,
    max_tokens: int,
    timeout: float,
) -> str:
    key = anthropic_api_key()
    if not key:
        raise RuntimeError(
            "Provider is Anthropic but no key: set ANTHROPIC_API_KEY or WEALTH_LEADS_ANTHROPIC_API_KEY"
        )
    model = anthropic_s1_model()
    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "max_tokens": max(256, min(max_tokens, 4096)),
            "system": system,
            "messages": [{"role": "user", "content": user}],
            "temperature": temperature,
        },
        timeout=timeout,
    )
    r.raise_for_status()
    data = r.json()
    parts: list[str] = []
    for block in data.get("content") or []:
        if isinstance(block, dict) and block.get("type") == "text":
            parts.append(block.get("text") or "")
    return "".join(parts).strip()


def advisor_llm_chat_json(
    *,
    system: str,
    user: str,
    temperature: float = 0.15,
    max_tokens: int = 1800,
    timeout: float = 120.0,
) -> dict[str, Any]:
    """JSON response (object) — uses WEALTH_LEADS_S1_AI_PROVIDER like enrich-s1-ai."""
    p = s1_ai_provider()
    if p in ("ollama", "local"):
        return _ollama_advisor_json(
            system=system,
            user=user,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
        )
    if p in ("anthropic", "claude"):
        return _anthropic_advisor_json(
            system=system,
            user=user,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
        )
    return _openai_advisor_json(
        system=system,
        user=user,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
    )


def advisor_llm_chat_text(
    *,
    system: str,
    user: str,
    temperature: float = 0.2,
    max_tokens: int = 900,
    timeout: float = 120.0,
) -> str:
    p = s1_ai_provider()
    if p in ("ollama", "local"):
        return _ollama_advisor_text(
            system=system,
            user=user,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
        )
    if p in ("anthropic", "claude"):
        return _anthropic_advisor_text(
            system=system,
            user=user,
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
        )
    return _openai_advisor_text(
        system=system,
        user=user,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
    )


def gather_issuer_text_blob(conn, cik: str, *, max_chars: int = 24_000) -> tuple[str, str]:
    """
    Concatenate issuer_summary (+ optional s1_llm lead_intel snippet) from recent filings.
    Returns (blob, note on sources).
    """
    ck = (cik or "").strip()
    if not ck:
        return "", ""
    cur = conn.execute(
        """
        SELECT filing_date, issuer_summary, s1_llm_lead_pack, company_name, form_type,
               issuer_revenue_text
        FROM filings
        WHERE cik = ?
          AND (issuer_summary IS NOT NULL AND TRIM(issuer_summary) != '')
        ORDER BY COALESCE(filing_date, '') DESC, id DESC
        LIMIT 6
        """,
        (ck,),
    )
    parts: list[str] = []
    meta: list[str] = []
    total = 0
    for r in cur.fetchall():
        summ = (r["issuer_summary"] or "").strip()
        if not summ:
            continue
        hdr = f"=== Filing {r['form_type'] or ''} dated {r['filing_date'] or ''} ===\n"
        chunk = hdr + summ
        rv = (r["issuer_revenue_text"] or "").strip()
        if rv:
            chunk += f"\nRevenue (S-1 extract): {rv}\n"
        lp = ""
        try:
            raw_lp = r["s1_llm_lead_pack"]
        except (KeyError, IndexError, TypeError):
            raw_lp = None
        if raw_lp and str(raw_lp).strip():
            lp = "\n--- LLM lead_intel (JSON excerpt) ---\n" + str(raw_lp)[:4000]
        blob = chunk + lp
        if total + len(blob) > max_chars:
            blob = blob[: max(0, max_chars - total)]
        parts.append(blob)
        meta.append(str(r["filing_date"] or ""))
        total += len(blob)
        if total >= max_chars:
            break
    return "\n\n".join(parts), " · ".join(meta)[:500]


def llm_issuer_advisor_snapshot(
    *,
    company_name: str,
    cik: str,
    text_blob: str,
) -> dict[str, Any]:
    if not advisor_llm_available():
        raise RuntimeError(
            "No LLM configured: set WEALTH_LEADS_S1_AI_PROVIDER=ollama (Ollama) or provide "
            "OPENAI_API_KEY / Anthropic key — same as enrich-s1-ai."
        )
    system = (
        "You write concise briefing sections for wealth advisors reviewing a pre-IPO / public registrant. "
        "Use ONLY facts stated or clearly implied in the SEC excerpt. If revenue is "
        "not in the text, say explicitly: 'Not stated in this excerpt — check MD&A / prospectus in EDGAR.' "
        "Do not invent numbers. Respond with JSON only:\n"
        '{"headline":"","revenue":"","business_plain":"","pool_angle":"","caveat":""}\n'
        "headline = one scannable line (company + stage).\n"
        "revenue = one or two sentences (ranges, 'revenue recognition', etc.) or not-stated message.\n"
        "business_plain = what they do in plain English, 2-4 sentences max.\n"
        "pool_angle = for an advisor only: how company scale might relate to breadth of exec "
        "households / plan complexity (speculative but grounded; if unknown, say so briefly).\n"
        "caveat = one sentence: filing text only, not investment advice; ownership and share counts "
        "in registration statements are point-in-time and can be overturned by later splits, "
        "distress, or enforcement — advisors must verify current cap table and 8-Ks."
    )
    user = (
        f"Company: {company_name}\nCIK: {cik}\n\n--- SEC / extracted text ---\n"
        f"{text_blob[:20_000]}"
    )
    return advisor_llm_chat_json(
        system=system, user=user, temperature=0.1, max_tokens=1800, timeout=120.0
    )


def ensure_issuer_advisor_snapshot(
    conn,
    cik: str,
    company_name: str,
    *,
    force: bool = False,
    use_llm: bool = True,
) -> dict[str, Any]:
    """Load or build per-CIK advisor snapshot. Returns parsed JSON dict."""
    ck = (cik or "").strip()
    if not ck:
        return {}
    try:
        row = conn.execute(
            "SELECT snapshot_json, built_at FROM issuer_advisor_snapshot WHERE cik = ?",
            (ck,),
        ).fetchone()
    except Exception:
        row = None
    if row and not force and (row["snapshot_json"] or "").strip() not in ("", "{}"):
        try:
            return json.loads(row["snapshot_json"] or "{}")
        except json.JSONDecodeError:
            pass
    if not use_llm or not advisor_llm_available():
        return {}
    blob, _src = gather_issuer_text_blob(conn, ck)
    if not blob.strip():
        return {}
    snap = llm_issuer_advisor_snapshot(company_name=company_name or ck, cik=ck, text_blob=blob)
    conn.execute(
        """
        INSERT INTO issuer_advisor_snapshot (cik, snapshot_json, source_excerpt, built_at)
        VALUES (?,?,?,datetime('now'))
        ON CONFLICT(cik) DO UPDATE SET
            snapshot_json = excluded.snapshot_json,
            source_excerpt = excluded.source_excerpt,
            built_at = excluded.built_at
        """,
        (ck, json.dumps(snap, ensure_ascii=False)[:48000], blob[:8000]),
    )
    return snap


def llm_person_advisor_story(
    *,
    display_name: str,
    title: str,
    company_name: str,
    s1_bio: str,
    website_bio: str,
    website_summary: str,
) -> str:
    if not advisor_llm_available():
        return ""
    system = (
        "Write a short executive brief for a wealth advisor (outreach / discovery call prep). "
        "No home address or personal phone; do not imply private contact data. "
        "Use at most 4 tight sentences (or 4–5 lines starting with '- '), in this order when known: "
        "role at this company, career path, prior notable employers, anything liquidity or equity-related "
        "if explicitly stated. If filing vs website differ, one short clause. No marketing filler."
    )
    user = (
        f"Name: {display_name}\nTitle: {title}\nCompany: {company_name}\n\n"
        f"--- Management narrative (SEC filing) ---\n{(s1_bio or '')[:6000]}\n\n"
        f"--- Website / leadership bio ---\n{(website_bio or '')[:4000]}\n\n"
        f"--- Website research summary ---\n{(website_summary or '')[:2000]}\n"
    )
    try:
        return advisor_llm_chat_text(
            system=system, user=user, temperature=0.2, max_tokens=450, timeout=120.0
        )
    except Exception:
        return ""


_EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.I,
)


def extract_emails_from_text(text: str) -> list[str]:
    if not text:
        return []
    found = _EMAIL_RE.findall(text or "")
    out: list[str] = []
    seen: set[str] = set()
    for e in found:
        low = e.lower()
        if low in seen:
            continue
        seen.add(low)
        out.append(e.strip())
    return out[:30]


def _root_domain_from_email(email: str) -> Optional[str]:
    _, addr = parseaddr(email)
    if "@" not in addr:
        addr = email
    parts = addr.strip().lower().split("@")
    if len(parts) != 2:
        return None
    dom = parts[1].strip()
    return dom if dom else None


def infer_primary_domain(
    emails_found: list[str], website_root: str
) -> tuple[Optional[str], Optional[str]]:
    """
    Return (domain, source_note). Prefer clearest corporate mailbox domain
    (e.g. contact@wbinfra.com → wbinfra.com).
    """
    from urllib.parse import urlparse

    skip = frozenset(
        {
            "gmail.com",
            "yahoo.com",
            "outlook.com",
            "hotmail.com",
            "icloud.com",
            "sentry.io",
            "wixpress.com",
            "google.com",
        }
    )
    for e in emails_found:
        d = _root_domain_from_email(e)
        if d and d not in skip and not d.endswith(".png"):
            return d, f"from address on issuer site: {e}"
    p = urlparse(website_root if website_root.startswith("http") else "https://" + website_root)
    if p.netloc:
        host = p.netloc.lower().replace("www.", "")
        if host and "." in host:
            return host, "from website hostname (no mailbox found)"
    return None, None


def _name_parts(display_name: str) -> tuple[str, str]:
    parts = [x for x in re.split(r"[\s,]+", (display_name or "").strip()) if x]
    if not parts:
        return "", ""
    first = re.sub(r"[^a-zA-Z]", "", parts[0]).lower()
    last = re.sub(r"[^a-zA-Z]", "", parts[-1]).lower() if len(parts) > 1 else ""
    return first, last


def build_email_candidates(
    display_name: str,
    domain: str,
    *,
    max_variants: Optional[int] = None,
) -> list[dict[str, Any]]:
    from wealth_leads.config import email_hypothesis_top_n

    cap = max_variants if max_variants is not None else email_hypothesis_top_n()
    cap = max(1, min(int(cap), 12))
    first, last = _name_parts(display_name)
    if not domain or not first:
        return []
    fl = f"{first}.{last}" if last else first
    # Prefer first.last@ (most common corporate pattern), then first@, then other variants.
    if last:
        locals_ = [
            fl,
            first,
            f"{first[0]}.{last}" if first else first,
            f"{first}{last}" if last else first,
            f"{first}_{last}" if last else first,
            f"{last}",
        ]
    else:
        locals_ = [first]
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    dom = domain.lower().strip()
    for loc in locals_:
        loc = loc.strip(".")
        if not loc or loc in seen:
            continue
        seen.add(loc)
        out.append({"email": f"{loc}@{dom}", "pattern": loc})
        if len(out) >= cap:
            break
    return out


def mx_hosts(domain: str) -> list[str]:
    domain = domain.strip().lower().rstrip(".")
    if not domain:
        return []
    if _dns_resolver is None:
        return []
    try:
        ans = _dns_resolver.resolve(domain, "MX")
        ranked = sorted([(int(r.preference), str(r.exchange).rstrip(".")) for r in ans])
        return [h for _, h in ranked]
    except Exception:
        return []


def smtp_rcpt_probe(
    to_address: str,
    *,
    timeout: float = 12.0,
    mail_from: str = "",
) -> tuple[str, str]:
    """
    Return (status, detail). status in accept | reject | uncertain | error | skipped.
    Many MX return 250 for all RCPT — treat as uncertain.
    """
    if "@" not in to_address:
        return "error", "bad address"
    dom = to_address.split("@")[-1].lower()
    hosts = mx_hosts(dom)
    if not hosts:
        return "error", "no MX records"
    mf = mail_from or "wealthpipeline-verify@invalid"
    last_err = ""
    for host in hosts[:2]:
        try:
            with smtplib.SMTP(host, 25, timeout=timeout) as server:
                server.ehlo()
                try:
                    server.starttls()
                    server.ehlo()
                except (smtplib.SMTPException, socket.error):
                    pass
                server.mail(mf)
                code, reply = server.rcpt(to_address)
                rep = (reply or b"").decode("utf-8", "replace")[:400]
                if code == 250:
                    return "uncertain", f"250 {rep} (many hosts accept any RCPT)"
                if code in (550, 551, 553):
                    return "reject", rep
                return "uncertain", f"{code} {rep}"
        except (socket.timeout, OSError, smtplib.SMTPException) as e:
            last_err = str(e)
            continue
    return "error", last_err or "smtp failed"


def _fetch_html_simple(sess: requests.Session, url: str, timeout: float = 14.0):
    try:
        r = sess.get(url, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
        return r.text, None
    except requests.RequestException as e:
        return None, str(e)


def discover_site_emails(sess: requests.Session, base_root: str) -> list[str]:
    """Fetch homepage + /contact + /contact-us for mailto/text emails."""
    urls = [
        base_root.rstrip("/") + "/",
        base_root.rstrip("/") + "/contact",
        base_root.rstrip("/") + "/contact-us",
    ]
    found: list[str] = []
    seen: set[str] = set()
    for u in urls:
        html, _ = _fetch_html_simple(sess, u)
        if not html:
            continue
        for e in extract_emails_from_text(html):
            low = e.lower()
            if low not in seen:
                seen.add(low)
                found.append(e)
    return found


def apply_smtp_probes_to_candidates(
    candidates: list[dict[str, Any]],
    *,
    mail_from: str = "",
    max_probes: int = 8,
) -> list[dict[str, Any]]:
    """
    SMTP RCPT check for up to ``max_probes`` rows still marked ``skipped`` (or empty).
    Leaves rows that already have ``uncertain`` / ``reject`` / ``error`` unchanged.
    """
    n = 0
    out: list[dict[str, Any]] = []
    mf = (mail_from or "").strip()
    cap = max(1, min(int(max_probes), 50))
    for c in candidates:
        row = dict(c)
        prev = (row.get("smtp_status") or "skipped").strip().lower()
        if prev not in ("skipped", ""):
            out.append(row)
            continue
        if n >= cap:
            out.append(row)
            continue
        em = (row.get("email") or "").strip()
        if "@" not in em:
            out.append(row)
            continue
        st, det = smtp_rcpt_probe(em, mail_from=mf)
        row["smtp_status"] = st
        row["smtp_detail"] = det
        n += 1
        out.append(row)
    return out


def outreach_pattern_pack_from_website(
    display_name: str,
    website_root: str,
    *,
    verify_smtp: bool = False,
    mail_from: str = "",
    max_smtp_probes: int = 8,
) -> dict[str, Any]:
    """
    Issuer website URL from the filing only (hostname → mail domain + name patterns).

    No HTTP fetch. Optional SMTP RCPT probes (same idea as enrich ``--smtp``).
    """
    ws = (website_root or "").strip()
    if not ws:
        return {}
    if not ws.startswith("http"):
        ws = "https://" + ws
    dom, dsrc = infer_primary_domain([], ws)
    if not dom:
        return {}
    cands = build_email_candidates(display_name, dom)
    if not cands:
        return {}
    rows = [{**c, "smtp_status": "skipped", "smtp_detail": ""} for c in cands]
    if verify_smtp:
        rows = apply_smtp_probes_to_candidates(
            rows, mail_from=mail_from, max_probes=max_smtp_probes
        )
    return {
        "emails_on_site": [],
        "domain": dom,
        "domain_source": dsrc or "from website hostname",
        "candidates": rows,
    }


def run_email_ping_suite(
    sess: requests.Session,
    *,
    display_name: str,
    website_root: str,
    verify_smtp: bool,
    mail_from: str = "",
) -> dict[str, Any]:
    from wealth_leads.config import email_smtp_probe_max_candidates

    lim = email_smtp_probe_max_candidates()
    emails = discover_site_emails(sess, website_root)
    domain, dom_src = infer_primary_domain(emails, website_root)
    candidates = build_email_candidates(display_name, domain or "") if domain else []
    rows = [{**c, "smtp_status": "skipped", "smtp_detail": ""} for c in candidates]
    if verify_smtp and domain and rows:
        rows = apply_smtp_probes_to_candidates(
            rows, mail_from=mail_from, max_probes=lim
        )
    return {
        "emails_on_site": emails,
        "domain": domain,
        "domain_source": dom_src,
        "candidates": rows,
    }


def fetch_s1_bio_for_person(conn, cik: str, person_norm: str) -> str:
    r = conn.execute(
        """
        SELECT m.bio_text
        FROM person_management_narrative m
        JOIN filings f ON f.id = m.filing_id
        WHERE f.cik = ? AND m.person_name_norm = ?
        ORDER BY COALESCE(f.filing_date, '') DESC, f.id DESC
        LIMIT 1
        """,
        ((cik or "").strip(), (person_norm or "").strip()),
    ).fetchone()
    return (r["bio_text"] or "").strip() if r else ""


def get_issuer_snapshot_dict(conn, cik: str) -> dict[str, Any]:
    ck = (cik or "").strip()
    if not ck:
        return {}
    try:
        row = conn.execute(
            "SELECT snapshot_json FROM issuer_advisor_snapshot WHERE cik = ?",
            (ck,),
        ).fetchone()
    except Exception:
        return {}
    if not row or not (row["snapshot_json"] or "").strip():
        return {}
    try:
        return json.loads(row["snapshot_json"] or "{}")
    except json.JSONDecodeError:
        return {}
