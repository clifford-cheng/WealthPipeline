@echo off
title WealthPipeline Dashboard
cd /d "%~dp0"

where python >nul 2>&1
if errorlevel 1 (
    echo Python was not found. Install it from https://www.python.org/downloads/
    echo and check "Add python.exe to PATH", then try again.
    pause
    exit /b 1
)

echo.
echo Starting dashboard at http://127.0.0.1:8765
echo Close this window or press Ctrl+C to stop the server.
echo.

python -m wealth_leads serve

echo.
echo Server stopped.
pause
