@echo off
title Remove WealthPipeline autostart
powershell -NoProfile -ExecutionPolicy Bypass -Command "$p = Join-Path ([Environment]::GetFolderPath('Startup')) 'WealthPipeline Dashboard.lnk'; if (Test-Path $p) { Remove-Item $p; Write-Host 'Removed:' $p } else { Write-Host 'No WealthPipeline shortcut in Startup.' }"
pause
