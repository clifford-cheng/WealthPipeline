# Exit 0 = advisor OK on 8765; browser opened; skip starting another copy.
# Exit 1 = need to start the server.
try {
    $r = Invoke-WebRequest -Uri "http://127.0.0.1:8765/healthz" -UseBasicParsing -TimeoutSec 2
    if ($r.StatusCode -eq 200 -and $r.RawContent -match "X-EquitySignal-Server:\s*advisor" -and $r.Content -match '"ok"\s*:\s*true') {
        Start-Process "http://127.0.0.1:8765/pipeline"
        exit 0
    }
} catch {
}
exit 1
