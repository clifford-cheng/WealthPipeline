"""
Background SEC sync while the FastAPI app is running — no Task Scheduler required.

Controlled by WEALTH_LEADS_AUTO_SYNC_HOURS (default 24; 0 = off).
Logs to logs/sec-sync.log next to the project root.
"""
from __future__ import annotations

import threading
from pathlib import Path

from wealth_leads.config import auto_sync_first_delay_sec, auto_sync_interval_hours
from wealth_leads.sec_sync_exec import ROOT, run_sync


def _log_file() -> Path:
    p = ROOT / "logs"
    p.mkdir(parents=True, exist_ok=True)
    return p / "sec-sync.log"


def start_auto_sync_background() -> threading.Event | None:
    """
    Start a daemon thread that runs sync on an interval. Returns a threading.Event
    to signal shutdown (lifespan), or None if auto-sync is disabled.
    """
    hours = auto_sync_interval_hours()
    if hours <= 0:
        return None
    stop = threading.Event()

    def loop() -> None:
        first = auto_sync_first_delay_sec()
        if stop.wait(first):
            return
        interval_sec = hours * 3600.0
        while not stop.is_set():
            try:
                run_sync(log_append_path=_log_file())
            except Exception as e:
                try:
                    with open(_log_file(), "a", encoding="utf-8") as lf:
                        lf.write(f"auto_sync thread error: {e!r}\n")
                except OSError:
                    pass
            if stop.wait(interval_sec):
                break

    t = threading.Thread(
        target=loop,
        name="equity-signal-auto-sec-sync",
        daemon=True,
    )
    t.start()
    return stop
