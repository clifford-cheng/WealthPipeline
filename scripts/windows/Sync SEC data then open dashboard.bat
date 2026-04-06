@echo off
setlocal EnableExtensions
title Equity Signal — Sync + Advisor app

set "WL_ROOT=%~dp0..\.."
cd /d "%WL_ROOT%"

if not exist "wealth_leads\__main__.py" (
    echo Project root not found. Expected wealth_leads next to scripts\windows.
    pause
    exit /b 1
)

set "_PY="
where py >nul 2>&1 && set "_PY=py -3"
if not defined _PY where python >nul 2>&1 && set "_PY=python"
if not defined _PY (
    echo No Python found. Install from https://www.python.org/downloads/ with "Add to PATH".
    pause
    exit /b 1
)

if not defined SEC_USER_AGENT (
    echo.
    echo Tip: For reliable SEC access, set SEC_USER_AGENT in this window, e.g.:
    echo   set SEC_USER_AGENT=EquitySignal/1.0 (contact: you@gmail.com)
    echo.
)

echo Using: %_PY%
echo Syncing from SEC...
%_PY% -m wealth_leads sync
if errorlevel 1 (
    echo Sync failed.
    pause
    exit /b 1
)

if not defined WEALTH_LEADS_APP_SECRET set "WEALTH_LEADS_APP_SECRET=equity-signal-local-dev-only-change-me"
if not defined WEALTH_LEADS_ALLOW_SIGNUP set "WEALTH_LEADS_ALLOW_SIGNUP=1"

set "WEALTH_LEADS_APP_PORT=8765"
echo Freeing port 8765 if another app is using it...
where powershell >nul 2>&1 && powershell -NoProfile -WindowStyle Hidden -Command "Get-NetTCPConnection -LocalPort 8765 -State Listen -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }" 2>nul
timeout /t 1 /nobreak >nul

echo.
echo Starting advisor app — browser opens from Python when this window is visible.
echo.
%_PY% "%WL_ROOT%\serve_advisor.py"

echo.
echo Server stopped.
pause
endlocal
