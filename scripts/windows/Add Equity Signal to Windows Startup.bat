@echo off
title Add Equity Signal to Startup
cd /d "%~dp0"

where powershell >nul 2>&1
if errorlevel 1 (
    echo PowerShell not found.
    pause
    exit /b 1
)

echo This adds a shortcut to your Windows Startup folder so the advisor app
echo runs in the background when you log in. No window will appear.
echo.
echo Browser URL: http://127.0.0.1:8765/login
echo.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0install_startup.ps1"
if errorlevel 1 (
    echo Failed.
    pause
    exit /b 1
)
echo.
echo Tip: In your browser, bookmark http://127.0.0.1:8765/login
echo To undo: run "Remove Equity Signal from Startup.bat" in this folder.
echo.
pause
