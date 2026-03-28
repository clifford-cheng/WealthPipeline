from __future__ import annotations

import time
from typing import Optional

import requests

from wealth_leads.config import REQUEST_DELAY_SEC, SEC_ORIGIN, user_agent

_last_request = 0.0


def _throttle() -> None:
    global _last_request
    elapsed = time.monotonic() - _last_request
    if elapsed < REQUEST_DELAY_SEC:
        time.sleep(REQUEST_DELAY_SEC - elapsed)
    _last_request = time.monotonic()


def get_text(url: str, session: Optional[requests.Session] = None) -> str:
    _throttle()
    sess = session or requests.Session()
    # SEC returns 403 if User-Agent is generic; do not set Host manually.
    r = sess.get(
        url,
        headers={
            "User-Agent": user_agent(),
            "Accept": "application/atom+xml,application/xml,text/xml,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "en-US,en;q=0.9",
        },
        timeout=120,
    )
    r.raise_for_status()
    return r.text


def absolute_url(href: str) -> str:
    if href.startswith("http"):
        return href
    return SEC_ORIGIN + href
