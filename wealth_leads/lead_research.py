"""
Client-facing research pack layered on SEC data: company website → leadership / team
pages → executive bio, headshot URL, and LinkedIn clues.

- Uses public pages on the issuer domain only (no LinkedIn login or scraping).
- linkedin_profile_url is only set when the issuer site explicitly links to linkedin.com/in/...
- linkedin_search_url is always a DuckDuckGo query for manual verification.
"""
from __future__ import annotations

import json
import re
import sqlite3
from collections import defaultdict
from datetime import datetime
from typing import Any, Optional
from urllib.parse import quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from wealth_leads.advisor_pack import (
    advisor_llm_available,
    advisor_llm_chat_json,
    ensure_issuer_advisor_snapshot,
    fetch_s1_bio_for_person,
    llm_filing_narrative_advisor_bullets,
    llm_person_advisor_story,
    outreach_pattern_pack_from_website,
    run_email_ping_suite,
)
from wealth_leads.config import (
    email_smtp_mail_from,
    email_smtp_probe_max_candidates,
    email_smtp_verify_enabled,
    user_agent,
)
from wealth_leads.db import get_issuer_website_for_cik, get_lead_client_research


_FETCH_TIMEOUT_SEC = 14
_MAX_TEXT_PER_PAGE = 14_000
_MAX_PROMPT_CHARS = 12_000
_MAX_HEADSHOT_BYTES = 600_000

_LEADERSHIP_PATHS = (
    "/leadership",
    "/team",
    "/management",
    "/executive-team",
    "/executives",
    "/our-team",
    "/about/leadership",
    "/about/team",
    "/about/management",
    "/company/leadership",
    "/company/team",
    "/about-us/leadership",
    "/about-us/team",
    "/governance/leadership",
    "/corporate-governance",
)

_NAME_STOP = frozenset(
    {"the", "and", "for", "mr", "ms", "mrs", "dr", "ph", "d", "cpa", "mba"}
)


def _now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def normalize_site_root(url: str) -> Optional[str]:
    s = (url or "").strip()
    if not s:
        return None
    if not s.startswith(("http://", "https://")):
        s = "https://" + s
    p = urlparse(s)
    if not p.netloc:
        return None
    scheme = p.scheme or "https"
    return f"{scheme}://{p.netloc}".rstrip("/")


def _linkedin_profile_regex(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(
        r"https?://(?:[\w-]+\.)?linkedin\.com/in/[a-zA-Z0-9\-_%/]+/?",
        text,
        re.I,
    )
    if not m:
        return None
    u = m.group(0).rstrip("/")
    if "/in/" not in u.lower():
        return None
    return u


def linkedin_search_url(display_name: str, company_name: str) -> str:
    q = quote_plus(f'"{display_name}" {company_name} site:linkedin.com/in')
    return f"https://duckduckgo.com/?q={q}"


def _tokenize_name(display_name: str) -> list[str]:
    raw = re.sub(r"[^\w\s]", " ", (display_name or "").lower())
    return [x for x in raw.split() if x and x not in _NAME_STOP]


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": user_agent(),
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
    )
    return s


def _html_looks_like_bot_challenge_page(html: str) -> bool:
    """Cloudflare / similar return 200 with a JS challenge, not the real site HTML."""
    if not html or len(html) < 400:
        return False
    low = html.lower()
    if "just a moment" in low and (
        "challenges.cloudflare.com" in low
        or "/cdn-cgi/challenge" in low
        or "cf-browser-verification" in low
    ):
        return True
    if "checking your browser before accessing" in low:
        return True
    return False


def _fetch_html(sess: requests.Session, url: str) -> tuple[Optional[str], Optional[str]]:
    try:
        r = sess.get(url, timeout=_FETCH_TIMEOUT_SEC, allow_redirects=True)
        body = r.text or ""
        ct = (r.headers.get("Content-Type") or "").lower()
        if r.status_code >= 400:
            if _html_looks_like_bot_challenge_page(body):
                return None, (
                    f"bot_protection_page (Cloudflare or similar; HTTP {r.status_code}; "
                    "needs a real browser or automation, not plain requests)"
                )
            r.raise_for_status()
        if "html" not in ct and "text" not in ct:
            return None, f"non-html content-type: {ct}"
        if _html_looks_like_bot_challenge_page(body):
            return None, (
                "bot_protection_page (e.g. Cloudflare JS challenge; plain HTTP cannot load this site)"
            )
        return body, None
    except requests.RequestException as e:
        return None, str(e)


def _discover_urls_from_index(sess: requests.Session, root: str) -> list[str]:
    html, err = _fetch_html(sess, root + "/")
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    out: list[str] = []
    for a in soup.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href or href.startswith(("#", "javascript:", "mailto:")):
            continue
        low = href.lower()
        if not any(k in low for k in ("leadership", "team", "management", "executive", "director", "officer")):
            continue
        full = urljoin(root + "/", href)
        if urlparse(full).netloc == urlparse(root).netloc:
            out.append(full.split("#")[0].rstrip("/"))
    seen: set[str] = set()
    uniq: list[str] = []
    for u in out:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq[:10]


def _gather_candidate_pages(sess: requests.Session, root: str) -> list[str]:
    candidates: list[str] = []
    for path in _LEADERSHIP_PATHS:
        candidates.append(root.rstrip("/") + path)
    candidates.extend(_discover_urls_from_index(sess, root))
    seen: set[str] = set()
    out: list[str] = []
    for u in candidates:
        uu = u.split("#")[0].rstrip("/")
        if uu not in seen:
            seen.add(uu)
            out.append(uu)
    return out[:14]


def _strip_noise(soup: BeautifulSoup) -> None:
    for tag in soup(["script", "style", "noscript", "svg", "iframe"]):
        tag.decompose()


def _heuristic_person_bundle(
    html: str, page_url: str, display_name: str, title: str
) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    _strip_noise(soup)
    tokens = _tokenize_name(display_name)
    last = tokens[-1] if tokens else ""
    first = tokens[0] if tokens else ""
    if not last:
        return {}

    best_img: Optional[str] = None
    best_bio: str = ""
    best_score = 0

    for img in soup.find_all("img", src=True):
        alt = (img.get("alt") or "").lower()
        src = (img.get("src") or "").strip()
        if not src or src.startswith("data:"):
            continue
        blob = alt + " " + (img.get("title") or "").lower()
        score = 0
        if last in blob:
            score += 3
        if first and first in blob:
            score += 1
        parent = img.find_parent(["article", "section", "div", "li"])
        ctx = (parent.get_text(" ", strip=True) if parent else "")[:900].lower()
        if last in ctx and (not title or (title or "").lower()[:24] in ctx or True):
            score += 2
        if score > best_score:
            best_score = score
            best_img = urljoin(page_url, src)

    for tag in soup.find_all(["h1", "h2", "h3", "h4", "strong", "b"]):
        t = tag.get_text(" ", strip=True)
        low = t.lower()
        if last not in low:
            continue
        if first and first not in low and len(tokens) > 1:
            continue
        block = tag.find_parent(["article", "section", "div", "li"])
        if not block:
            block = tag.parent
        if block:
            txt = block.get_text("\n", strip=True)
            lines = [ln for ln in txt.split("\n") if ln.strip()]
            bio = "\n".join(lines[1:6])[:1200]
            if len(bio) > len(best_bio):
                best_bio = bio

    full_text = soup.get_text("\n", strip=True)
    li = _linkedin_profile_regex(full_text) or _linkedin_profile_regex(html)

    return {
        "photo_url": best_img if best_score >= 2 else None,
        "bio_website": best_bio.strip()[:2000] if best_bio else None,
        "linkedin_from_page": li,
    }


def _call_llm_client_pack(
    *,
    display_name: str,
    title: str,
    company_name: str,
    issuer_website: str,
    text_bundle: str,
) -> dict[str, Any]:
    if not advisor_llm_available():
        raise RuntimeError(
            "No LLM for client pack: set WEALTH_LEADS_S1_AI_PROVIDER=ollama and run Ollama, "
            "or set OPENAI_API_KEY / Anthropic key (same as enrich-s1-ai)."
        )
    system = (
        "You help wealth advisors prepare a polished, factual 'client research card' for an executive. "
        "Use ONLY the provided website text; do not invent employers, degrees, or LinkedIn URLs. "
        "If the executive is not clearly named in the text, return empty strings for fields sourced from the site. "
        "Respond with JSON only: "
        '{"research_summary":"","bio_website":"","photo_url":"","linkedin_profile_url":"","leadership_page_url":""} '
        "research_summary = 2–4 sentences for an advisor to read to a client (professional tone). "
        "bio_website = short bio taken or condensed from the site prose (empty if none). "
        "photo_url = absolute image URL only if clearly the headshot for this person in the text/markup context (empty if unsure). "
        "linkedin_profile_url = only if a full linkedin.com/in/... URL appears in the source text; else empty string. "
        "leadership_page_url = the page URL this bundle seems to draw from (one of the URLs provided in the preamble), or empty."
    )
    preamble = (
        f"Executive: {display_name}\nTitle (from SEC profile): {title}\n"
        f"Company: {company_name}\nIssuer website: {issuer_website}\n\n"
        f"--- WEBSITE TEXT (may be noisy) ---\n{text_bundle[:_MAX_PROMPT_CHARS]}"
    )
    return advisor_llm_chat_json(
        system=system, user=preamble, temperature=0.15, max_tokens=1200, timeout=120.0
    )


def _fetch_headshot_bytes(
    sess: requests.Session, url: str
) -> tuple[Optional[bytes], Optional[str]]:
    """Download a small image for local DB storage (issuer site only)."""
    if not url or not str(url).startswith(("http://", "https://")):
        return None, None
    try:
        r = sess.get(
            str(url).strip(),
            timeout=_FETCH_TIMEOUT_SEC,
            stream=True,
            allow_redirects=True,
            headers={"Accept": "image/*,*/*;q=0.8"},
        )
        r.raise_for_status()
        ct = (r.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        if ct and not ct.startswith("image/"):
            return None, None
        buf = b""
        for chunk in r.iter_content(chunk_size=65536):
            if chunk:
                buf += chunk
            if len(buf) > _MAX_HEADSHOT_BYTES:
                return None, None
        if len(buf) < 200:
            return None, None
        if not ct:
            ct = "image/jpeg"
        return buf, ct
    except requests.RequestException:
        return None, None


def _upsert_pattern_outreach_from_profile(conn: sqlite3.Connection, row: sqlite3.Row) -> str:
    """
    Persist guessed emails from ``issuer_website`` + display name (filing data only).
    No HTTP. Returns ``unchanged`` if ``outreach_json`` already populated (full enrich kept).
    """
    cik = str(row["cik"] or "").strip()
    person_norm = str(row["person_norm"] or "").strip()
    if not cik or not person_norm:
        return "bad_row"
    display_name = (row["display_name"] or "").strip() or person_norm
    company_name = (row["company_name"] or "").strip()
    site = (row["issuer_website"] or "").strip()
    if not site.startswith("http"):
        site = get_issuer_website_for_cik(conn, cik)
    if not site.startswith("http"):
        return "no_website"
    pack = outreach_pattern_pack_from_website(
        display_name,
        site,
        verify_smtp=False,
        mail_from="",
        max_smtp_probes=email_smtp_probe_max_candidates(),
    )
    if not pack.get("candidates"):
        return "no_patterns"
    lsearch = linkedin_search_url(display_name, company_name)
    now = _now_iso()
    oj = json.dumps(pack, ensure_ascii=False)[:48000]
    ex = get_lead_client_research(conn, cik, person_norm)
    ojs = (ex["outreach_json"] or "").strip() if ex else ""
    if ojs:
        try:
            prev = json.loads(ojs)
        except json.JSONDecodeError:
            return "unchanged"
        if not prev.get("error") and (prev.get("candidates") or []):
            return "unchanged"
    if ex:
        conn.execute(
            """
            UPDATE lead_client_research SET
              outreach_json = ?,
              linkedin_search_url = ?,
              enriched_at = ?,
              display_name = ?,
              company_name = ?,
              issuer_website = ?
            WHERE cik = ? AND person_norm = ?
            """,
            (
                oj,
                lsearch[:2000],
                now,
                display_name[:400],
                company_name[:400],
                site[:500],
                cik,
                person_norm,
            ),
        )
        return "updated"
    conn.execute(
        """
        INSERT INTO lead_client_research (
          cik, person_norm, display_name, company_name, issuer_website,
          status, linkedin_search_url, enriched_at, outreach_json,
          bio_website, photo_url, leadership_page_url,
          linkedin_profile_url, research_summary, source_excerpt, raw_json, error_message,
          person_story, photo_blob, photo_mime
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            cik,
            person_norm,
            display_name[:400],
            company_name[:400],
            site[:500],
            "pending",
            lsearch[:2000],
            now,
            oj,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
    )
    return "inserted"


def materialize_email_outreach_for_profiles(
    conn: sqlite3.Connection,
    *,
    limit: Optional[int] = None,
) -> dict[str, Any]:
    """
    Fill ``lead_client_research.outreach_json`` with hostname-based email patterns for
    profiles that have ``issuer_website`` (from filings). Runs after ``rebuild-profiles`` /
    ``sync``. SMTP checks run on lead view or ``enrich-client-research`` when enabled.
    """
    cap = 50_000 if limit is None else max(1, min(int(limit), 100_000))
    cur = conn.execute(
        """
        SELECT * FROM lead_profile
        WHERE issuer_website IS NOT NULL AND TRIM(issuer_website) != ''
        ORDER BY filing_date_latest DESC, cik, person_norm
        LIMIT ?
        """,
        (cap,),
    )
    rows = cur.fetchall()
    by: dict[str, int] = defaultdict(int)
    for row in rows:
        by[_upsert_pattern_outreach_from_profile(conn, row)] += 1
    return {
        "email_outreach_profiles_seen": len(rows),
        "email_outreach_inserted": by.get("inserted", 0),
        "email_outreach_updated": by.get("updated", 0),
        "email_outreach_unchanged": by.get("unchanged", 0),
        "email_outreach_no_patterns": by.get("no_patterns", 0)
        + by.get("no_website", 0)
        + by.get("bad_row", 0),
    }


def enrich_lead_profile_row(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    use_llm: bool = True,
    verify_smtp: Optional[bool] = None,
) -> dict[str, Any]:
    """Build and persist client research for one lead_profile row. Returns status dict."""
    vs = email_smtp_verify_enabled() if verify_smtp is None else bool(verify_smtp)
    cik = str(row["cik"] or "").strip()
    person_norm = str(row["person_norm"] or "").strip()
    display_name = (row["display_name"] or "").strip() or person_norm
    company_name = (row["company_name"] or "").strip()
    title = (row["title"] or "").strip()
    site = (row["issuer_website"] or "").strip()
    if not site or not site.startswith("http"):
        site = get_issuer_website_for_cik(conn, cik)

    lsearch = linkedin_search_url(display_name, company_name)
    base_root = normalize_site_root(site)

    if use_llm and advisor_llm_available():
        try:
            ensure_issuer_advisor_snapshot(
                conn, cik, company_name, force=False, use_llm=True
            )
        except Exception:
            pass

    def _save(
        status: str,
        **fields: Any,
    ) -> dict[str, Any]:
        payload = {
            "cik": cik,
            "person_norm": person_norm,
            "display_name": display_name[:400],
            "company_name": company_name[:400],
            "issuer_website": (site or "")[:500],
            "status": status,
            "linkedin_search_url": lsearch[:2000],
            "enriched_at": _now_iso(),
            **fields,
        }
        conn.execute(
            """
            INSERT INTO lead_client_research (
                cik, person_norm, display_name, company_name, issuer_website,
                status, bio_website, photo_url, leadership_page_url,
                linkedin_profile_url, linkedin_search_url, research_summary,
                source_excerpt, raw_json, error_message, enriched_at,
                person_story, filing_narrative_bullets, outreach_json, photo_blob, photo_mime
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(cik, person_norm) DO UPDATE SET
                display_name = excluded.display_name,
                company_name = excluded.company_name,
                issuer_website = excluded.issuer_website,
                status = excluded.status,
                bio_website = excluded.bio_website,
                photo_url = excluded.photo_url,
                leadership_page_url = excluded.leadership_page_url,
                linkedin_profile_url = excluded.linkedin_profile_url,
                linkedin_search_url = excluded.linkedin_search_url,
                research_summary = excluded.research_summary,
                source_excerpt = excluded.source_excerpt,
                raw_json = excluded.raw_json,
                error_message = excluded.error_message,
                enriched_at = excluded.enriched_at,
                person_story = excluded.person_story,
                filing_narrative_bullets = COALESCE(excluded.filing_narrative_bullets, filing_narrative_bullets),
                outreach_json = excluded.outreach_json,
                photo_blob = excluded.photo_blob,
                photo_mime = excluded.photo_mime
            """,
            (
                payload["cik"],
                payload["person_norm"],
                payload["display_name"],
                payload["company_name"],
                payload["issuer_website"],
                payload["status"],
                payload.get("bio_website"),
                payload.get("photo_url"),
                payload.get("leadership_page_url"),
                payload.get("linkedin_profile_url"),
                payload["linkedin_search_url"],
                payload.get("research_summary"),
                payload.get("source_excerpt"),
                payload.get("raw_json"),
                payload.get("error_message"),
                payload["enriched_at"],
                payload.get("person_story"),
                payload.get("filing_narrative_bullets"),
                payload.get("outreach_json"),
                payload.get("photo_blob"),
                payload.get("photo_mime"),
            ),
        )
        return {"cik": cik, "person_norm": person_norm, "status": status}

    s1_bio = fetch_s1_bio_for_person(conn, cik, person_norm)
    person_story_val: Optional[str] = None
    outreach_val: Optional[str] = None
    filing_narrative_bullets_val: Optional[str] = None
    _s1_strip = str(s1_bio or "").strip()
    if use_llm and advisor_llm_available() and _s1_strip:
        try:
            filing_narrative_bullets_val = (
                llm_filing_narrative_advisor_bullets(
                    display_name=display_name,
                    title=title,
                    company_name=company_name,
                    s1_bio=_s1_strip,
                ).strip()
                or None
            )
        except Exception:
            filing_narrative_bullets_val = None

    if not base_root:
        if use_llm and advisor_llm_available() and _s1_strip:
            try:
                person_story_val = llm_person_advisor_story(
                    display_name=display_name,
                    title=title,
                    company_name=company_name,
                    s1_bio=_s1_strip,
                    website_bio="",
                    website_summary="",
                )
            except Exception:
                person_story_val = None
        return _save(
            "skipped",
            error_message="No issuer website URL in profile or filings.",
            research_summary="",
            bio_website=None,
            photo_url=None,
            leadership_page_url=None,
            linkedin_profile_url=None,
            source_excerpt=None,
            raw_json=None,
            person_story=person_story_val,
            filing_narrative_bullets=filing_narrative_bullets_val,
            outreach_json=None,
        )

    sess = _session()
    pages = _gather_candidate_pages(sess, base_root)
    chunks: list[str] = []
    used_page = ""
    heuristic: dict[str, Any] = {}

    tl = display_name.lower().split()[-1] if display_name else ""
    fallback_pages: list[tuple[str, str]] = []
    used_fallback_llm_context = False

    for url in pages:
        html, err = _fetch_html(sess, url)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        _strip_noise(soup)
        text = soup.get_text("\n", strip=True)
        if len(text) > _MAX_TEXT_PER_PAGE:
            text = text[:_MAX_TEXT_PER_PAGE] + "\n…"
        low = text.lower()
        # Heuristic photo / bio / LinkedIn can use img alt + nearby DOM even when the
        # plain-text body does not include the surname (common on leadership pages).
        h = _heuristic_person_bundle(html, url, display_name, title)
        if h.get("photo_url") or h.get("bio_website") or h.get("linkedin_from_page"):
            heuristic = {**heuristic, **{k: v for k, v in h.items() if v}}
        if tl and tl in low:
            chunks.append(f"URL: {url}\n{text}\n")
            if not used_page:
                used_page = url
        elif text.strip():
            fallback_pages.append((url, text))

    if not chunks and fallback_pages:
        used_fallback_llm_context = True
        for url, text in fallback_pages[:2]:
            chunks.append(
                f"URL: {url}\n(Leadership page — name string match uncertain; "
                f"verify context for {display_name}.)\n{text}\n"
            )
            if not used_page:
                used_page = url
    text_bundle = "\n\n".join(chunks[:6])
    raw_parts: dict[str, Any] = {
        "pages_tried": pages,
        "heuristic": heuristic,
        "used_fallback_llm_context": used_fallback_llm_context,
    }

    bio = (heuristic.get("bio_website") or "").strip() or None
    photo = heuristic.get("photo_url")
    li_prof = heuristic.get("linkedin_from_page")
    summary = ""
    llm_page = ""

    if use_llm and text_bundle.strip() and advisor_llm_available():
        try:
            llm = _call_llm_client_pack(
                display_name=display_name,
                title=title,
                company_name=company_name,
                issuer_website=base_root,
                text_bundle=text_bundle,
            )
            raw_parts["llm"] = llm
            summary = (llm.get("research_summary") or "").strip()
            if (llm.get("bio_website") or "").strip():
                bio = (llm.get("bio_website") or "").strip()
            if (llm.get("photo_url") or "").strip():
                photo = (llm.get("photo_url") or "").strip()
            if (llm.get("linkedin_profile_url") or "").strip():
                li_prof = (llm.get("linkedin_profile_url") or "").strip()
            if (llm.get("leadership_page_url") or "").strip():
                llm_page = (llm.get("leadership_page_url") or "").strip()
        except Exception as e:
            raw_parts["llm_error"] = str(e)

    if llm_page and llm_page not in used_page:
        used_page = llm_page or used_page

    if not summary and bio:
        summary = (
            f"{display_name} — {title}. Summary from the issuer’s public website (not SEC): "
            f"{bio[:500]}{'…' if len(bio) > 500 else ''}"
        )

    if use_llm and advisor_llm_available():
        try:
            person_story_val = llm_person_advisor_story(
                display_name=display_name,
                title=title,
                company_name=company_name,
                s1_bio=_s1_strip,
                website_bio=bio or "",
                website_summary=summary or "",
            )
        except Exception as e:
            raw_parts["person_story_error"] = str(e)

    outreach_pack: dict[str, Any] = {}
    if base_root:
        try:
            outreach_pack = run_email_ping_suite(
                sess,
                display_name=display_name,
                website_root=base_root,
                verify_smtp=vs,
                mail_from=email_smtp_mail_from(),
            )
        except Exception as e:
            outreach_pack = {"error": str(e)}
    outreach_val = json.dumps(outreach_pack, ensure_ascii=False)[:48000] if outreach_pack else None

    status = "ok" if (photo or bio or summary) else "partial"
    if not text_bundle.strip():
        status = "partial"
        raw_parts["note"] = (
            "Could not fetch or match leadership-page text to this name; "
            "ensure issuer website is in the filing, or run "
            "`python -m wealth_leads enrich-client-research --force` after fixing the URL."
        )

    photo_blob_bin: Optional[bytes] = None
    photo_mime_val: Optional[str] = None
    if photo:
        b, m = _fetch_headshot_bytes(sess, str(photo))
        if b and m:
            photo_blob_bin, photo_mime_val = b, m

    return _save(
        status,
        bio_website=bio,
        photo_url=photo,
        leadership_page_url=used_page or None,
        linkedin_profile_url=li_prof,
        research_summary=summary or None,
        source_excerpt=text_bundle[:8000] if text_bundle else None,
        raw_json=json.dumps(raw_parts, ensure_ascii=False)[:48000],
        error_message=None,
        person_story=person_story_val,
        filing_narrative_bullets=filing_narrative_bullets_val,
        outreach_json=outreach_val,
        photo_blob=photo_blob_bin,
        photo_mime=photo_mime_val,
    )


def run_enrich_client_research(
    conn,
    *,
    limit: int = 15,
    cik: Optional[str] = None,
    force: bool = False,
    use_llm: bool = True,
    verify_smtp: Optional[bool] = None,
) -> dict[str, Any]:
    """Enrich up to `limit` lead_profile rows (newest first)."""
    lim = max(1, min(int(limit), 500))
    params: list[Any] = []
    wh = ""
    if cik and str(cik).strip():
        wh = " AND cik = ?"
        params.append(str(cik).strip())
    cur = conn.execute(
        f"""
        SELECT * FROM lead_profile
        WHERE 1=1 {wh}
        ORDER BY filing_date_latest DESC, cik, person_norm
        LIMIT ?
        """,
        (*params, lim * 3),
    )
    rows = list(cur.fetchall())
    done = 0
    skipped = 0
    errors: list[str] = []
    for row in rows:
        if done >= lim:
            break
        ck = str(row["cik"] or "").strip()
        pn = str(row["person_norm"] or "").strip()
        if not force:
            ex = get_lead_client_research(conn, ck, pn)
            if ex and (ex["status"] or "") == "ok":
                skipped += 1
                continue
        try:
            enrich_lead_profile_row(
                conn, row, use_llm=use_llm, verify_smtp=verify_smtp
            )
            done += 1
        except Exception as e:
            errors.append(f"{ck}/{pn}: {e}")
    return {"enriched": done, "skipped_ok": skipped, "errors": errors}


def row_to_client_research_dict(row: Optional[sqlite3.Row]) -> Optional[dict[str, Any]]:
    if row is None:
        return None
    return {k: row[k] for k in row.keys()}
