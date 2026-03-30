@echo off
title Restart WealthPipeline server

echo Stopping anything on port 8765, then starting the advisor app in the background...
echo.

for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8765" ^| findstr "LISTENING"') do (
  taskkill /F /PID %%a >nul 2>&1
)

timeout /t 2 /nobreak >nul

wscript //B //Nologo "%~dp0Serve in background (hidden).vbs"

echo.
echo Done. Open http://127.0.0.1:8765/login  (hard refresh: Ctrl+F5 if the page looks old^)
echo.
pause
