# Creates a Startup shortcut so the advisor server runs in the background at login.
$scriptDir = $PSScriptRoot
$repoRoot = (Get-Item $scriptDir).Parent.Parent.FullName
$vbs = Join-Path $scriptDir "Serve in background (hidden).vbs"
if (-not (Test-Path $vbs)) {
    Write-Error "Missing: $vbs"
    exit 1
}
$startup = [Environment]::GetFolderPath("Startup")
$lnkPath = Join-Path $startup "Equity Signal Dashboard.lnk"
$w = New-Object -ComObject WScript.Shell
$s = $w.CreateShortcut($lnkPath)
$s.TargetPath = "wscript.exe"
$s.Arguments = "//B //Nologo `"$vbs`""
$s.WorkingDirectory = $repoRoot
$s.Description = "Equity Signal: advisor app on http://127.0.0.1:8765 (sign-in)"
$s.Save()
Write-Host "Created: $lnkPath"
Write-Host "After you sign out and back in (or reboot), open http://127.0.0.1:8765/login in your browser."
Write-Host "First time only: run Sync if you need fresh SEC data."
