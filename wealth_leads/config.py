import os
from pathlib import Path

# SEC requires a descriptive User-Agent with contact info.
# https://www.sec.gov/os/webmaster-faq#code-support
# Use an email you actually read; some addresses (e.g. *@users.noreply.github.com)
# have triggered HTTP 403 from sec.gov in testing.
DEFAULT_USER_AGENT = (
    "EquitySignal/0.1 (SEC research; contact: you@example.com)"
)

SEC_ORIGIN = "https://www.sec.gov"
RSS_URL = (
    "https://www.sec.gov/cgi-bin/browse-edgar"
    "?action=getcurrent&type=S-1&count={count}&output=atom"
)


def rss_url_for_form(form_type: str) -> str:
    """EDGAR 'current' Atom feed for a single form type (e.g. S-1, 10-K)."""
    ft = form_type.strip().replace(" ", "")
    return (
        "https://www.sec.gov/cgi-bin/browse-edgar"
        f"?action=getcurrent&type={ft}&count={{count}}&output=atom"
    )


def sync_form_types() -> list[str]:
    """
    Form types to pull from the SEC 'current' RSS on each sync.
    Default: S-1 only. Recent 10-Ks for the *same issuers* come from submissions API
    follow (see follow_10k_for_s1_ciks). Add "10-K" here for a global 10-K firehose too.
    """
    raw = os.environ.get("SEC_SYNC_FORMS", "S-1")
    return [x.strip() for x in raw.split(",") if x.strip()]


def follow_10k_for_s1_ciks() -> bool:
    """After RSS sync, fetch recent 10-Ks per CIK seen from S-1 filings (submissions API)."""
    v = os.environ.get("SEC_FOLLOW_10K", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def submissions_10k_per_cik() -> int:
    """Max recent 10-K / 10-K/A filings to pull per CIK from data.sec.gov submissions."""
    return max(0, int(os.environ.get("SEC_10K_PER_CIK", "3")))


def follow_8k_for_s1_ciks() -> bool:
    """
    After RSS sync, fetch recent 8-K / 8-K/A (etc.) per CIK that appears on an S-1 in DB.
    Uses the same submissions JSON as the 10-K follow (one request per CIK when either is on).
    """
    v = os.environ.get("SEC_FOLLOW_8K", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def submissions_8k_per_cik() -> int:
    """Max recent 8-K family filings to pull per CIK (newest first). Keep modest — 8-Ks are frequent."""
    try:
        return max(0, int(os.environ.get("SEC_8K_PER_CIK", "5")))
    except ValueError:
        return 5


def issuer_sec_hints_min_refresh_days() -> int:
    """Minimum days between re-fetching submissions JSON for ticker hints only (standalone path)."""
    try:
        return max(1, int(os.environ.get("SEC_ISSUER_HINTS_MIN_DAYS", "7")))
    except ValueError:
        return 7


def fetch_standalone_issuer_sec_hints() -> bool:
    """
    If 10-K and 8-K follow are both off, still refresh ``issuer_sec_hints`` from submissions
    JSON on sync (throttled by ``issuer_sec_hints_min_refresh_days``).
    """
    v = os.environ.get("SEC_ISSUER_HINTS_STANDALONE", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def lead_page_stale_issuer_months() -> int:
    """
    If > 0, the lead profile shows a warning when the newest filing in the DB for this CIK
    is older than this many calendar months (approximate, 30.44 days/month).
    """
    try:
        return max(0, int(os.environ.get("WEALTH_LEADS_LEAD_STALE_ISSUER_MONTHS", "18")))
    except ValueError:
        return 18

# Stay under SEC fair-access guidance (~10 req/s); be conservative.
REQUEST_DELAY_SEC = 0.15


def user_agent() -> str:
    return os.environ.get("SEC_USER_AGENT", DEFAULT_USER_AGENT)


def rss_count() -> int:
    # EDGAR "current" Atom: higher count ≈ longer recent window (often months of S-1s).
    return int(os.environ.get("SEC_RSS_COUNT", "350"))


def database_path() -> str:
    """Default DB is always next to the project root (this repo), not the shell cwd."""
    explicit = os.environ.get("WEALTH_LEADS_DB")
    if explicit:
        return os.path.expanduser(explicit)
    root = Path(__file__).resolve().parent.parent
    return str(root / "wealth_leads.sqlite3")


def lead_desk_s1_only() -> bool:
    """If true, the lead desk lists only people with at least one NEO row from an S-1 or S-1/A."""
    v = os.environ.get("WEALTH_LEADS_LEAD_DESK_S1_ONLY", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def lead_desk_include_beneficial_only_leads() -> bool:
    """
    If true, the lead desk includes **major-shareholder** profiles built only from
    S-1 beneficial-ownership parsing (no NEO / named officer row). Those rows are easy to
    misread after splits, distress, or fraud — **default is off** so the product stays
    exec / NEO–quality focused.

    Set ``WEALTH_LEADS_LEAD_DESK_INCLUDE_BENEFICIAL_ONLY=1`` to also surface beneficial-only
    holders. Anyone who is both an officer and a beneficial holder is always included.
    """
    v = os.environ.get("WEALTH_LEADS_LEAD_DESK_INCLUDE_BENEFICIAL_ONLY", "0").strip().lower()
    return v not in ("0", "false", "no", "off")


def lead_desk_include_visibility_tier() -> bool:
    """
    If true (default), the desk lists **visibility** tier leads (S-1 officer table / roster
    without a parsed NEO summary-comp row). If false, those rows are hidden from desk,
    pipeline materialization, and other paths that use ``_lead_desk_filter_profiles``.
    """
    v = os.environ.get("WEALTH_LEADS_LEAD_DESK_INCLUDE_VISIBILITY", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def lead_desk_exclude_issuer_risk_high() -> bool:
    """
    If true (default), omit leads whose issuer text (stored ``issuer_summary`` scan) matches
    **high** distress patterns (e.g. bankruptcy, delisting phrasing).
    """
    v = os.environ.get("WEALTH_LEADS_DESK_EXCLUDE_ISSUER_RISK_HIGH", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def assign_exclude_visibility_tier() -> bool:
    """If true (default), monthly allocation skips **visibility** tier profiles."""
    v = os.environ.get("WEALTH_LEADS_ASSIGN_EXCLUDE_VISIBILITY", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def assign_exclude_issuer_risk_high() -> bool:
    """If true (default), allocation skips issuers with **high** heuristic risk."""
    v = os.environ.get("WEALTH_LEADS_ASSIGN_EXCLUDE_ISSUER_RISK_HIGH", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def assign_exclude_issuer_risk_elevated() -> bool:
    """
    If true, allocation also skips **elevated** heuristic risk (material weakness, reverse
    split language, etc.). Default false — those leads may still be assigned but show a banner.
    """
    v = os.environ.get("WEALTH_LEADS_ASSIGN_EXCLUDE_ISSUER_RISK_ELEVATED", "0").strip().lower()
    return v not in ("0", "false", "no", "off")


def assign_max_filing_stale_days() -> int:
    """
    If > 0, skip allocation for profiles whose ``filing_date`` is older than this many days.
    0 (default) disables. Uses the profile headline filing date string (YYYY-MM-DD).
    """
    try:
        return max(0, int(os.environ.get("WEALTH_LEADS_ASSIGN_MAX_FILING_STALE_DAYS", "0")))
    except ValueError:
        return 0


def lead_desk_us_registrant_hq_only() -> bool:
    """
    If true, the lead desk and admin pipeline list/CSV hide rows whose registrant HQ line
    does not parse to a U.S. state or ZIP (see ``registrant_hq_line_parses_as_united_states``).
    Default on for U.S.-advisor workflows; set WEALTH_LEADS_DESK_US_HQ_ONLY=0 to show all.
    """
    v = os.environ.get("WEALTH_LEADS_DESK_US_HQ_ONLY", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def profile_stale_warning_days() -> int:
    """
    If ``lead_profile`` max(built_at) is older than this many days (UTC), the pipeline banner
    shows a stale warning. Set WEALTH_LEADS_PROFILE_STALE_DAYS=0 to disable.
    """
    try:
        return max(0, int(os.environ.get("WEALTH_LEADS_PROFILE_STALE_DAYS", "10")))
    except ValueError:
        return 10


def lead_desk_min_signal_usd() -> float:
    """
    Minimum headline fiscal-year pay signal for Premium vs Standard: max(SCT total, stock+options)
    for the **headline** FY row (the same year as Latest total), not the best-ever year.

    Prefer WEALTH_LEADS_LEAD_DESK_MIN_SIGNAL_USD. If WEALTH_LEADS_LEAD_DESK_MIN_EQUITY_USD is
    set in the environment, it overrides (legacy: equity-only bar uses max equity across FYs).
    """
    if "WEALTH_LEADS_LEAD_DESK_MIN_EQUITY_USD" in os.environ:
        raw = os.environ["WEALTH_LEADS_LEAD_DESK_MIN_EQUITY_USD"].strip()
    else:
        raw = os.environ.get("WEALTH_LEADS_LEAD_DESK_MIN_SIGNAL_USD", "300000").strip()
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 300_000.0


def lead_desk_equity_only_min_usd() -> bool:
    """True if legacy WEALTH_LEADS_LEAD_DESK_MIN_EQUITY_USD is set (equity-only comparison)."""
    return "WEALTH_LEADS_LEAD_DESK_MIN_EQUITY_USD" in os.environ


def app_secret_key() -> str:
    """Required for signed session cookies on serve-app (min 32 chars recommended)."""
    return os.environ.get("WEALTH_LEADS_APP_SECRET", "").strip()


def app_allow_public_signup() -> bool:
    """After the first user exists, allow /register only if this is true (or zero users)."""
    v = os.environ.get("WEALTH_LEADS_ALLOW_SIGNUP", "0").strip().lower()
    return v in ("1", "true", "yes", "on")


def app_listen_port() -> int:
    return int(os.environ.get("WEALTH_LEADS_APP_PORT", "8080"))


def pipeline_blur_comp_columns() -> bool:
    """
    When True, pipeline review blurs pay / comp columns (demo of post-pay unlock UX).
    """
    return os.environ.get("WEALTH_LEADS_PIPELINE_BLUR_COMP", "0").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def desk_company_preview_blur() -> bool:
    """
    When True, desk issuer drill-in shows city/state clearly but blurs names, pay, and links
    (preview before purchase). Off by default for internal admin use.
    """
    return os.environ.get("WEALTH_LEADS_DESK_COMPANY_PREVIEW_BLUR", "0").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def uvicorn_reload_enabled() -> bool:
    """
    When True, `py -m wealth_leads serve-app` runs uvicorn with --reload so code edits apply without a manual restart.
    """
    return os.environ.get("WEALTH_LEADS_UVICORN_RELOAD", "0").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def require_app_auth() -> bool:
    """
    When False (default), the app opens straight to the data UI with no login.
    Set WEALTH_LEADS_REQUIRE_AUTH=1 to restore sign-in, My leads, and watchlist writes.
    """
    return os.environ.get("WEALTH_LEADS_REQUIRE_AUTH", "0").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def user_may_view_pipeline_exec_bundle(user: dict) -> bool:
    """
    Who may open ``/pipeline/company?...&sales_bundle=premium`` (NEO / verified SCT roster).

    Admins always qualify. Otherwise set ``WEALTH_LEADS_EXEC_BUNDLE_EMAIL_ALLOWLIST`` to a
    comma-separated list of advisor emails, or ``WEALTH_LEADS_PIPELINE_EXEC_BUNDLE_ALL_ADVISORS=1``
    to allow any signed-in user (dev only).
    """
    if user.get("is_admin"):
        return True
    if os.environ.get(
        "WEALTH_LEADS_PIPELINE_EXEC_BUNDLE_ALL_ADVISORS", "0"
    ).strip().lower() in ("1", "true", "yes", "on"):
        return True
    allow = os.environ.get("WEALTH_LEADS_EXEC_BUNDLE_EMAIL_ALLOWLIST", "").strip()
    if not allow:
        return False
    email = str(user.get("email") or "").strip().lower()
    allowed = {x.strip().lower() for x in allow.split(",") if x.strip()}
    return bool(email and email in allowed)


def auto_sync_interval_hours() -> float:
    """
    While the advisor server process is running, run SEC sync on this interval (hours).
    Set WEALTH_LEADS_AUTO_SYNC_HOURS=0 to disable. Default 24.
    """
    if "WEALTH_LEADS_AUTO_SYNC_HOURS" not in os.environ:
        return 24.0
    raw = os.environ["WEALTH_LEADS_AUTO_SYNC_HOURS"].strip().lower()
    if raw in ("0", "false", "no", "off"):
        return 0.0
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 24.0


def auto_sync_first_delay_sec() -> float:
    """Wait this long after server start before the first automatic sync."""
    raw = os.environ.get("WEALTH_LEADS_AUTO_SYNC_FIRST_DELAY_SEC", "300").strip()
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 300.0


def openai_api_key() -> str:
    """OpenAI API key for S-1 LLM extraction (enrich-s1-ai)."""
    return (
        os.environ.get("WEALTH_LEADS_OPENAI_API_KEY", "").strip()
        or os.environ.get("OPENAI_API_KEY", "").strip()
    )


def openai_s1_model() -> str:
    """Chat model for `enrich-s1-ai` when provider is OpenAI (JSON mode)."""
    m = os.environ.get("WEALTH_LEADS_S1_AI_MODEL", "gpt-4o-mini").strip()
    return m or "gpt-4o-mini"


def s1_ai_provider() -> str:
    """
    LLM backend for enrich-s1-ai: openai (default), anthropic, or ollama (local).
    """
    v = os.environ.get("WEALTH_LEADS_S1_AI_PROVIDER", "openai").strip().lower()
    if v in ("anthropic", "claude"):
        return "anthropic"
    if v in ("ollama", "local"):
        return "ollama"
    return "openai"


def ollama_base_url() -> str:
    """Ollama HTTP API base (no trailing slash)."""
    u = os.environ.get("WEALTH_LEADS_OLLAMA_URL", "http://127.0.0.1:11434").strip()
    return u.rstrip("/") or "http://127.0.0.1:11434"


def ollama_s1_model() -> str:
    """Model name as shown by `ollama list` (e.g. llama3.1, qwen2.5:14b)."""
    m = os.environ.get("WEALTH_LEADS_OLLAMA_MODEL", "llama3.1").strip()
    return m or "llama3.1"


def s1_ai_max_chars() -> int:
    """Max characters of plain text sent to the LLM for enrich-s1-ai (after HTML strip)."""
    try:
        n = int(os.environ.get("WEALTH_LEADS_S1_AI_MAX_CHARS", "100000").strip())
    except (TypeError, ValueError):
        n = 100_000
    return max(24_000, min(n, 500_000))


def s1_ai_document_mode() -> str:
    """
    How to build the text excerpt for enrich-s1-ai before calling the model:

    - windows (default): merge cover + phrase windows (comp, ownership, HQ, …). Good when
      the filing is long and the model context is capped.

    - linear / sequential / top / natural: single contiguous slice from the start of the
      plain filing (cover and prospectus summary first). Lets the model "read forward" like
      a human; raise WEALTH_LEADS_S1_AI_MAX_CHARS if your model fits more.

    - bookend / headtail: ~72% from the start + ~28% from the end (comp tables often
      appear later). Middle omitted.
    """
    v = os.environ.get("WEALTH_LEADS_S1_AI_DOCUMENT_MODE", "windows").strip().lower()
    if v in ("linear", "sequential", "top", "natural", "read"):
        return "linear"
    if v in ("bookend", "ends", "headtail", "head_tail"):
        return "bookend"
    return "windows"


def anthropic_api_key() -> str:
    """Anthropic API key for enrich-s1-ai when provider is anthropic."""
    return (
        os.environ.get("WEALTH_LEADS_ANTHROPIC_API_KEY", "").strip()
        or os.environ.get("ANTHROPIC_API_KEY", "").strip()
    )


def enrich_client_research_after_sync_enabled() -> bool:
    """
    When true, `sync` runs a capped `enrich-client-research` pass after rebuild-profiles
    (website photos / LinkedIn hints — still no LinkedIn scraping).
    """
    return os.environ.get(
        "WEALTH_LEADS_ENRICH_WEB_AFTER_SYNC", "0"
    ).strip().lower() in ("1", "true", "yes", "on")


def enrich_client_research_after_sync_limit() -> int:
    """Max lead profiles to enrich per sync when WEALTH_LEADS_ENRICH_WEB_AFTER_SYNC=1."""
    try:
        n = int(os.environ.get("WEALTH_LEADS_ENRICH_WEB_AFTER_SYNC_LIMIT", "12"))
    except (TypeError, ValueError):
        n = 12
    return max(1, min(n, 200))


def email_smtp_verify_enabled() -> bool:
    """
    When true (default), ``enrich-client-research`` and the lead page run SMTP RCPT probes
    (up to ``email_smtp_probe_max_candidates``) on guessed addresses when checks are still
    ``skipped``.

    Set ``WEALTH_LEADS_EMAIL_SMTP_VERIFY=0`` if outbound port 25 is blocked. Many MX
    return 250 for any RCPT (``uncertain``)—a hint only, not proof an inbox exists.
    """
    v = os.environ.get("WEALTH_LEADS_EMAIL_SMTP_VERIFY", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def email_smtp_probe_max_candidates() -> int:
    """Max guessed addresses to RCPT-probe per lead view or enrich pass (cap latency)."""
    try:
        n = int(os.environ.get("WEALTH_LEADS_EMAIL_SMTP_MAX_PROBES", "8"))
    except (TypeError, ValueError):
        n = 8
    return max(1, min(n, 20))


def email_hypothesis_top_n() -> int:
    """
    How many guessed corporate addresses to store and show per person (default 2).
    Advisors get a short shortlist, not a long permutation list.
    """
    try:
        n = int(os.environ.get("WEALTH_LEADS_EMAIL_TOP_PICKS", "2"))
    except (TypeError, ValueError):
        n = 2
    return max(1, min(n, 5))


def email_smtp_mail_from() -> str:
    """Optional MAIL FROM for RCPT probes (some MX require a plausible domain)."""
    return os.environ.get("WEALTH_LEADS_SMTP_MAIL_FROM", "").strip()


def anthropic_s1_model() -> str:
    """Claude model id for enrich-s1-ai (see Anthropic docs for current ids)."""
    m = os.environ.get(
        "WEALTH_LEADS_ANTHROPIC_S1_MODEL",
        "claude-3-5-haiku-20241022",
    ).strip()
    return m or "claude-3-5-haiku-20241022"


def advisor_ui_product_name() -> str:
    """
    Short product name in advisor-facing HTML (nav, page titles).
    Override with WEALTH_LEADS_UI_PRODUCT_NAME for a different display name.
    """
    v = (os.environ.get("WEALTH_LEADS_UI_PRODUCT_NAME") or "").strip()
    return v if v else "Equity Signal"
