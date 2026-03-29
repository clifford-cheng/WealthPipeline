@echo off
REM If double-clicking opens this file in Cursor/VS Code instead of running:
REM   use "Start Dashboard (use if .bat opens in editor).vbs" in this folder,
REM   or right-click this file -> Open with -> Command Prompt (always).
REM To avoid running this every time: run "Add WealthPipeline to Windows Startup.bat"
REM then bookmark http://127.0.0.1:8765 or double-click "Open WealthPipeline Dashboard.url".
setlocal EnableExtensions
title WealthPipeline Dashboard

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

echo Using: %_PY%
echo.
echo Dashboard: http://127.0.0.1:8765
echo Leave this window OPEN while you use the site. Close it to stop the server.
echo If the browser says it can't connect, wait 2 seconds and refresh.
echo If tables are empty, run "Sync SEC data then open dashboard.bat" once.
echo.

%_PY% -m wealth_leads serve
set "_ERR=%ERRORLEVEL%"

echo.
if not "%_ERR%"=="0" (
    echo The server exited with an error ^(code %_ERR%^).
    echo If you see "No module named wealth_leads", run:  %_PY% -m pip install -r requirements.txt
)
echo Server stopped.
pause
endlocal
