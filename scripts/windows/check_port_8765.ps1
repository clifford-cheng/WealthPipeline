$p = 8765
$base = "http://127.0.0.1:$p"
Write-Host "--- Processes LISTENING on port $p ---"
Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction SilentlyContinue |
    Format-Table LocalAddress, LocalPort, OwningProcess -AutoSize
Write-Host "--- GET $base/healthz ---"
try {
    $r = Invoke-WebRequest -Uri "$base/healthz" -UseBasicParsing -TimeoutSec 3
    Write-Host "Status:" $r.StatusCode
    if ($r.RawContent -match "X-WealthPipeline-Server:\s*advisor") {
        Write-Host "Looks like: FastAPI advisor (correct)"
    } else {
        Write-Host "Missing X-WealthPipeline-Server: advisor — probably NOT the advisor app"
    }
    Write-Host "Body:" $r.Content
} catch {
    Write-Host $_.Exception.Message
}
Write-Host "--- GET $base/login ---"
try {
    $r = Invoke-WebRequest -Uri "$base/login" -UseBasicParsing -TimeoutSec 3
    Write-Host "Status:" $r.StatusCode
    $first = ($r.Content -split "`n")[0].Trim()
    Write-Host "First line of HTML:" $first
} catch {
    Write-Host $_.Exception.Message
}
