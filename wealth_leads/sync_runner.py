"""
Run `python -m wealth_leads sync` in a subprocess so the FastAPI server keeps serving.
Admin UI triggers this; no separate terminal required on your machine.
"""
from __future__ import annotations

import subprocess
import threading
from datetime import datetime, timezone

from wealth_leads.sec_sync_exec import run_sync

_lock = threading.Lock()
_state: dict = {
    "phase": "idle",
    "message": "",
    "started_at": "",
    "finished_at": "",
    "returncode": None,
}


def sync_state() -> dict:
    with _lock:
        return dict(_state)


def start_sync_subprocess(*, force: bool = False) -> tuple[bool, str]:
    """
    Start sync if not already running.
    Returns (ok_to_start, user_message).
    """
    with _lock:
        if _state["phase"] == "running":
            return False, "SEC sync is already running. Refresh this page in a bit."
        _state["phase"] = "running"
        _state["message"] = "Starting…"
        _state["started_at"] = datetime.now(timezone.utc).isoformat()
        _state["finished_at"] = ""
        _state["returncode"] = None

    t = threading.Thread(
        target=_run_sync,
        kwargs={"force": force},
        name="wealthpipeline-sec-sync",
        daemon=True,
    )
    t.start()
    return True, "SEC sync started in the background. This page will refresh while it runs; it may take several minutes."


def _run_sync(*, force: bool) -> None:
    out_tail = ""
    err_tail = ""
    rc: int | None = None
    try:
        rc, out_tail, err_tail = run_sync(force=force)
    except subprocess.TimeoutExpired:
        rc = -1
        err_tail = "Sync subprocess timed out (2h limit)."
    except OSError as e:
        rc = -1
        err_tail = str(e)

    with _lock:
        _state["returncode"] = rc
        _state["finished_at"] = datetime.now(timezone.utc).isoformat()
        if rc == 0:
            _state["phase"] = "ok"
            tail = (out_tail + "\n" + err_tail).strip()
            _state["message"] = tail[-500:] if tail else "Finished."
        else:
            _state["phase"] = "error"
            _state["message"] = (err_tail or out_tail or "Sync failed.")[-800:]
