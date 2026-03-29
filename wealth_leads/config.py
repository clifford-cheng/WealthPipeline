import os
from pathlib import Path

# SEC requires a descriptive User-Agent with contact info.
# https://www.sec.gov/os/webmaster-faq#code-support
# Use an email you actually read; some addresses (e.g. *@users.noreply.github.com)
# have triggered HTTP 403 from sec.gov in testing.
DEFAULT_USER_AGENT = (
    "WealthLeadsMVP/0.1 (personal research; contact: you@example.com)"
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

# Stay under SEC fair-access guidance (~10 req/s); be conservative.
REQUEST_DELAY_SEC = 0.15


def user_agent() -> str:
    return os.environ.get("SEC_USER_AGENT", DEFAULT_USER_AGENT)


def rss_count() -> int:
    return int(os.environ.get("SEC_RSS_COUNT", "100"))


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


def lead_desk_min_signal_usd() -> float:
    """
    Minimum "best single fiscal year" pay signal for the desk: max(SCT total, stock+options)
    for that person across FY rows (same DB rows as equity_hwm / per-year totals).
    Set to 0 to disable the monetary gate (S-1-only may still apply).

    Prefer WEALTH_LEADS_LEAD_DESK_MIN_SIGNAL_USD. If WEALTH_LEADS_LEAD_DESK_MIN_EQUITY_USD is
    set in the environment, it overrides (legacy: equity-only bar).
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
