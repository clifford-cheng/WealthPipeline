@echo off
title Restart WealthPipeline server
cd /d "%~dp0"

echo Stopping anything on port 8765, then starting the dashboard again...
echo.

for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8765" ^| findstr "LISTENING"') do (
  taskkill /F /PID %%a >nul 2>&1
)

timeout /t 2 /nobreak >nul

wscript //B //Nologo "%~dp0Serve in background (hidden).vbs"

echo.
echo Done. Open http://127.0.0.1:8765  (hard refresh: Ctrl+F5 if the page looks old^)
echo.
pause
