"""
Start the Equity Signal advisor app (sign-in, My leads, Admin).

Double-click Start Equity Signal Dashboard.bat, or run:
  py -3 serve_advisor.py
  python serve_advisor.py

This script forces the project root onto sys.path so "wealth_leads" imports even if
your current directory is wrong (common cause of "not found" / module errors).

Dev UX: Auto-reload defaults ON (uvicorn watches wealth_leads/ and project root).
Save a .py file → server restarts in ~1s → refresh the browser. Set WEALTH_LEADS_NO_RELOAD=1
to disable (single process, slightly more predictable on odd setups).
"""
from __future__ import annotations

import os
import sys
import threading
import time
import webbrowser
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
root_s = str(ROOT)
if root_s not in sys.path:
    sys.path.insert(0, root_s)


def _maybe_open_login(host: str, port: int) -> None:
    """
    Open the main UI once the server is up (/pipeline when auth is off, else /login).
    Default: only when stderr is a TTY (visible .bat / terminal).
    Set WEALTH_LEADS_OPEN_BROWSER=1 to always open, =0 to never.
    Hidden / background starts (e.g. Startup VBS) skip the browser.
    """
    v = os.environ.get("WEALTH_LEADS_OPEN_BROWSER", "").strip().lower()
    if v in ("0", "false", "no", "off"):
        return
    if v not in ("1", "true", "yes", "on", "force") and not sys.stderr.isatty():
        return
    from wealth_leads.config import require_app_auth

    path = "/login" if require_app_auth() else "/pipeline"
    url = f"http://{host}:{port}{path}"

    def _open() -> None:
        time.sleep(1.6)
        try:
            webbrowser.open(url)
        except OSError:
            pass

    threading.Thread(target=_open, daemon=True).start()


def main() -> None:
    os.environ.setdefault(
        "WEALTH_LEADS_APP_SECRET",
        "equity-signal-local-dev-only-change-me",
    )
    os.environ.setdefault("WEALTH_LEADS_ALLOW_SIGNUP", "1")
    host = os.environ.get("WEALTH_LEADS_BIND_HOST", "127.0.0.1")
    port = int(os.environ.get("WEALTH_LEADS_APP_PORT", "8765"))
    import uvicorn
    from wealth_leads.config import require_app_auth

    no_reload = os.environ.get("WEALTH_LEADS_NO_RELOAD", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    main_path = "/login" if require_app_auth() else "/pipeline"
    print(
        f"Equity Signal advisor: http://{host}:{port}{main_path}"
        + ("" if no_reload else "  (auto-reload on: save code → refresh browser)"),
        flush=True,
    )
    print(
        f"Check http://{host}:{port}/healthz — should be JSON with \"ok\": true. "
        "If the main page is 404, another app was on this port; close it and restart.",
        flush=True,
    )
    kw: dict = {"host": host, "port": port, "log_level": "info"}
    _maybe_open_login(host, port)
    if no_reload:
        # Load app in this process so cwd + sys.path from above always apply (reliable on Windows).
        from wealth_leads.web_app import app as fastapi_app

        uvicorn.run(fastapi_app, **kw)
    else:
        # Reload spawns a child interpreter that does not run this file's sys.path fix.
        root_s = str(ROOT)
        pp = os.environ.get("PYTHONPATH", "").strip()
        os.environ["PYTHONPATH"] = (
            root_s if not pp else f"{root_s}{os.pathsep}{pp}"
        )
        kw["reload"] = True
        kw["reload_dirs"] = [str(ROOT / "wealth_leads"), root_s]
        kw["app_dir"] = root_s
        uvicorn.run("wealth_leads.web_app:app", **kw)


if __name__ == "__main__":
    main()
