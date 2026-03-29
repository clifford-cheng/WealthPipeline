@echo off
setlocal EnableExtensions
title WealthPipeline — Sync + Dashboard

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
    echo No Python found. Install from https://www.python.org/downloads/ with "Add to PATH".
    pause
    exit /b 1
)

if not defined SEC_USER_AGENT (
    echo.
    echo Tip: For reliable SEC access, set SEC_USER_AGENT first in this window, e.g.:
    echo   set SEC_USER_AGENT=WealthPipeline/1.0 (contact: you@gmail.com)
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

echo.
echo Opening dashboard (leave this window open)...
%_PY% -m wealth_leads serve

echo.
echo Server stopped.
pause
endlocal
