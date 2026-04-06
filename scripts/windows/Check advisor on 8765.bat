@echo off
title Equity Signal — check port 8765
cd /d "%~dp0"
echo.
echo Run this while the server is supposed to be up (or to see what stole the port).
echo.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0check_port_8765.ps1"
echo.
pause
