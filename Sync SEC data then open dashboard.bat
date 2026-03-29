@echo off
title WealthPipeline — Sync + Dashboard
cd /d "%~dp0"

where python >nul 2>&1
if errorlevel 1 (
    echo Python was not found. Install it from https://www.python.org/downloads/
    pause
    exit /b 1
)

if not defined SEC_USER_AGENT (
    echo.
    echo Tip: For reliable SEC access, set SEC_USER_AGENT first, e.g. in this window:
    echo   set SEC_USER_AGENT=WealthPipeline/1.0 (contact: you@gmail.com)
    echo.
)

echo Syncing from SEC...
python -m wealth_leads sync
if errorlevel 1 (
    echo Sync failed.
    pause
    exit /b 1
)

echo.
echo Opening dashboard...
python -m wealth_leads serve

echo.
echo Server stopped.
pause
