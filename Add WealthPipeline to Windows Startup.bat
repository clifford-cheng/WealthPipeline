@echo off
title Add WealthPipeline to Startup
cd /d "%~dp0"

where powershell >nul 2>&1
if errorlevel 1 (
    echo PowerShell not found.
    pause
    exit /b 1
)

echo This adds a shortcut to your Windows Startup folder so the dashboard
echo server runs in the background when you log in. No window will appear.
echo.
echo Browser URL: http://127.0.0.1:8765
echo.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0install_startup.ps1"
if errorlevel 1 (
    echo Failed.
    pause
    exit /b 1
)
echo.
echo Tip: Bookmark http://127.0.0.1:8765 — you only need the browser after this.
echo To undo: run "Remove WealthPipeline from Startup.bat"
echo.
pause
