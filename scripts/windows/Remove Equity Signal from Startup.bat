@echo off
title Remove Equity Signal autostart
powershell -NoProfile -ExecutionPolicy Bypass -Command "$s=[Environment]::GetFolderPath('Startup'); $names=@('Equity Signal Dashboard.lnk','WealthPipeline Dashboard.lnk'); $any=$false; foreach ($n in $names) { $p=Join-Path $s $n; if (Test-Path $p) { Remove-Item $p; Write-Host ('Removed: '+$p); $any=$true } }; if (-not $any) { Write-Host 'No Equity Signal (or legacy WealthPipeline) shortcut in Startup.' }"
pause
