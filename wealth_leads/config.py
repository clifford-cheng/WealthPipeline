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
    Override with env SEC_SYNC_FORMS=comma-separated list (default: S-1,10-K).
    """
    raw = os.environ.get("SEC_SYNC_FORMS", "S-1,10-K")
    return [x.strip() for x in raw.split(",") if x.strip()]

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
