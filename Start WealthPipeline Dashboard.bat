@echo off
REM If double-clicking opens this file in Cursor/VS Code instead of running:
REM   use scripts\windows\Start Dashboard (use if .bat opens in editor).vbs
REM   or right-click this file -> Open with -> Command Prompt (always).
REM Startup / sync / refresh: see scripts\windows\  (Add to Startup, Sync SEC, Refresh server).
REM When the server is already running: double-click "Open WealthPipeline Dashboard.url"
REM (opens the login page in your browser — it does not start the server).
setlocal EnableExtensions
title WealthPipeline — Advisor app (local)

cd /d "%~dp0"

if not exist "wealth_leads\__main__.py" (
    echo This .bat must stay in the same folder as the "wealth_leads" project.
    pause
    exit /b 1
)

set "_PY="
where py >nul 2>&1 && set "_PY=py -3"
if not defined _PY where python >nul 2>&1 && set "_PY=python"
if not defined _PY (
    echo No Python launcher found.
    echo Install Python from https://www.python.org/downloads/
    echo Turn ON "Add python.exe to PATH", then log out and back in.
    pause
    exit /b 1
)

REM Advisor UI: sign-in, My assigned leads, Admin / allocation (territories).
REM Override for production: set WEALTH_LEADS_APP_SECRET yourself (32+ random chars).
if not defined WEALTH_LEADS_APP_SECRET (
    set "WEALTH_LEADS_APP_SECRET=wealthpipeline-local-dev-only-change-me"
    echo.
    echo Using a built-in LOCAL dev secret. For anything beyond your PC, set
    echo   WEALTH_LEADS_APP_SECRET
    echo to a long random value first.
    echo.
)

REM First account can register; more accounts allowed on localhost.
if not defined WEALTH_LEADS_ALLOW_SIGNUP set "WEALTH_LEADS_ALLOW_SIGNUP=1"

set "WEALTH_LEADS_APP_PORT=8765"

REM If the advisor is already running, only open the browser — no second console / server.
where powershell >nul 2>&1 && powershell -NoProfile -WindowStyle Hidden -File "%~dp0scripts\windows\if_advisor_running_open_browser.ps1"
if errorlevel 1 goto need_server
echo.
echo Advisor is already running — opened the pipeline page. No new window started.
echo Close the existing WealthPipeline console when you want to stop the server.
exit /b 0

:need_server
REM Something else on 8765 (e.g. "python -m wealth_leads serve") causes 404 on /login in the browser.
echo Freeing port 8765 if another process is listening there...
where powershell >nul 2>&1 && powershell -NoProfile -WindowStyle Hidden -Command "Get-NetTCPConnection -LocalPort 8765 -State Listen -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }" 2>nul
timeout /t 1 /nobreak >nul

echo Using: %_PY%
echo.
echo Open in browser:  http://127.0.0.1:8765/pipeline  ^(data review; no login by default^)
echo Optional sign-in: set WEALTH_LEADS_REQUIRE_AUTH=1 then use http://127.0.0.1:8765/login
echo After sign-in:    My assigned leads, Admin ^(territories / run allocation^)
echo Legacy full desk: Admin ^> Lead desk
echo.
echo Leave this window OPEN while you use the site. Close it to stop the server.
echo Code changes: save any .py under wealth_leads ^(or project root^) — server auto-restarts; then refresh the browser ^(Ctrl+F5 if CSS looks stuck^).
echo To turn OFF auto-reload ^(single process^): set WEALTH_LEADS_NO_RELOAD=1 before starting this .bat
echo If the browser says it can't connect, wait 2 seconds and refresh.
echo If /pipeline is 404: open http://127.0.0.1:8765/healthz — JSON with "ok" ^= advisor app.
echo   Plain "Not Found" on /healthz ^= wrong server; this .bat now clears port 8765 first.
echo If tables are empty, run scripts\windows\Sync SEC data then open dashboard.bat once.
echo Browser opens automatically from the server when this window is visible.
echo.

REM serve_advisor.py puts project root on sys.path — fixes "No module named wealth_leads"
%_PY% "%~dp0serve_advisor.py"
set "_ERR=%ERRORLEVEL%"

echo.
if not "%_ERR%"=="0" (
    echo The server exited with an error ^(code %_ERR%^).
    echo If you see missing modules ^(fastapi, uvicorn^), run:
    echo   cd /d "%~dp0"
    echo   %_PY% -m pip install -r requirements.txt
    echo.
    echo If port 8765 is already in use, close the other WealthPipeline window or run:
    echo   netstat -ano ^| findstr :8765
)
echo Server stopped.
pause
endlocal
