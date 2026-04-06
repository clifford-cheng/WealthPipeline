# Exit 0 = advisor already running on 8765 (skip starting another copy).
# Exit 1 = start serve_advisor (nothing there, wrong app, or port freed after kill).
try {
    $r = Invoke-WebRequest -Uri "http://127.0.0.1:8765/healthz" -UseBasicParsing -TimeoutSec 3
    # PS 5.1 / 7+: header keys differ; RawContent is reliable.
    if ($r.StatusCode -eq 200 -and $r.RawContent -match "X-EquitySignal-Server:\s*advisor" -and $r.Content -match '"ok"\s*:\s*true') {
        exit 0
    }
} catch {
    # unreachable, wrong app, or connection refused
}

Get-NetTCPConnection -LocalPort 8765 -State Listen -ErrorAction SilentlyContinue |
    ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }
exit 1
