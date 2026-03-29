# Creates a Startup shortcut so the dashboard server runs in the background when you log in.
$here = $PSScriptRoot
$vbs = Join-Path $here "Serve in background (hidden).vbs"
if (-not (Test-Path $vbs)) {
    Write-Error "Missing: $vbs"
    exit 1
}
$startup = [Environment]::GetFolderPath("Startup")
$lnkPath = Join-Path $startup "WealthPipeline Dashboard.lnk"
$w = New-Object -ComObject WScript.Shell
$s = $w.CreateShortcut($lnkPath)
$s.TargetPath = "wscript.exe"
$s.Arguments = "//B //Nologo `"$vbs`""
$s.WorkingDirectory = $here
$s.Description = "WealthPipeline: local dashboard on http://127.0.0.1:8765"
$s.Save()
Write-Host "Created: $lnkPath"
Write-Host "After you sign out and back in (or reboot), open http://127.0.0.1:8765 in your browser."
Write-Host "First time only: run Sync if you need fresh SEC data."
