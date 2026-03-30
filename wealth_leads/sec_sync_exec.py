"""
Run `python -m wealth_leads sync` as a subprocess with a process-wide lock so
manual (Admin) and automatic background sync never hit SQLite at the same time.
"""
from __future__ import annotations

import os
import subprocess
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_lock = threading.Lock()


def run_sync(
    *,
    force: bool = False,
    log_append_path: Path | None = None,
    timeout_sec: int = 7200,
) -> tuple[int, str, str]:
    """
    Returns (returncode, stdout_tail, stderr_tail).
    If log_append_path is set, process output goes there and strings are empty.
    Otherwise output is captured (for Admin UI status).
    """
    cmd = [sys.executable, "-m", "wealth_leads", "sync"]
    if force:
        cmd.append("--force")
    env = os.environ.copy()
    with _lock:
        if log_append_path is not None:
            log_append_path.parent.mkdir(parents=True, exist_ok=True)
            with open(log_append_path, "a", encoding="utf-8") as lf:
                lf.write(
                    f"\n===== {datetime.now(timezone.utc).isoformat()} sync =====\n"
                )
                lf.flush()
                proc = subprocess.run(
                    cmd,
                    cwd=str(ROOT),
                    stdout=lf,
                    stderr=subprocess.STDOUT,
                    timeout=timeout_sec,
                    env=env,
                )
                lf.write(f"===== exit {proc.returncode} =====\n")
            return proc.returncode, "", ""
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            env=env,
        )
        out = (proc.stdout or "")[-4000:]
        err = (proc.stderr or "")[-4000:]
        return proc.returncode, out, err
