@echo off
REM Pulls new filings from SEC and rebuilds lead_profile. Does NOT start the web app.
REM For daily automation: use Windows Task Scheduler (see comments at bottom of this file).
setlocal EnableExtensions
set "WL_ROOT=%~dp0..\.."
cd /d "%WL_ROOT%"

if not exist "wealth_leads\__main__.py" (
    echo Wrong folder — expected Wealth App project root.
    exit /b 1
)

set "_PY="
where py >nul 2>&1 && set "_PY=py -3"
if not defined _PY where python >nul 2>&1 && set "_PY=python"
if not defined _PY (
    echo Python not found.
    exit /b 1
)

if not exist "logs" mkdir "logs" 2>nul
set "LOG=%WL_ROOT%\logs\sec-sync.log"

echo. >> "%LOG%"
echo ===== %date% %time% sync start ===== >> "%LOG%"
echo SEC sync starting... (log: logs\sec-sync.log)

REM Set SEC_USER_AGENT once for your PC (recommended). Examples:
REM   setx SEC_USER_AGENT "EquitySignal/1.0 (contact: you@email.com)"
REM Or uncomment and edit the next line:
REM set "SEC_USER_AGENT=EquitySignal/1.0 (contact: you@email.com)"

%_PY% -m wealth_leads sync >> "%LOG%" 2>&1
set "RC=%ERRORLEVEL%"
echo ===== %date% %time% sync end (exit %RC%) ===== >> "%LOG%"

if not "%RC%"=="0" echo Sync failed — see %LOG%
if "%RC%"=="0" echo Sync finished OK — see %LOG%

REM Double-click: pause so you can read. Task Scheduler: pass argument "silent" to skip pause.
if /i not "%~1"=="silent" pause
exit /b %RC%

REM ----- Task Scheduler (daily) -----
REM 1. Open Task Scheduler ^> Create Basic Task ^> Daily.
REM 2. Action: Start a program
REM 3. Program:  cmd.exe
REM 4. Arguments: /c ""C:\path\to\Wealth App\scripts\windows\Run SEC sync only.bat" silent"
REM    (use your real path; keep the quotes)
REM 5. Start in:  C:\path\to\Wealth App
REM 6. Optional: check "Run whether user is logged on or not" and store your Windows password.
REM 7. Ensure SEC_USER_AGENT is set for your account: setx SEC_USER_AGENT "EquitySignal/1.0 (contact: email)"
REM    Then log off/on once so Task Scheduler picks it up.
