# Sentinel-ML port check
# Windows reserved port ranges change on reboot / WSL restart.
# Run this BEFORE starting containers.

$ports = [ordered]@{
    "Kafka"      = 9192
    "Zookeeper"  = 2281
    "PostgreSQL" = 5432
    "Redis"      = 6379
    "MLflow"     = 5000
    "SpringAPI"  = 8280
    "FastAPI"    = 8295
    "Airflow"    = 8281
}

$raw = netsh interface ipv4 show excludedportrange protocol=tcp
$excluded = @()
foreach ($line in $raw) {
    if ($line -match '^\s+(\d+)\s+(\d+)') {
        $excluded += [PSCustomObject]@{
            Start = [int]$Matches[1]
            End   = [int]$Matches[2]
        }
    }
}

Write-Host ""
Write-Host "=== Reserved ranges: $($excluded.Count) ===" -ForegroundColor Cyan
foreach ($r in $excluded) { Write-Host "  $($r.Start) - $($r.End)" }

Write-Host ""
Write-Host "=== Port check ===" -ForegroundColor Cyan
$conflict = 0
foreach ($name in $ports.Keys) {
    $p = $ports[$name]
    $hit = $excluded | Where-Object { $p -ge $_.Start -and $p -le $_.End }
    if ($hit) {
        Write-Host ("  [CONFLICT] {0,-12} {1}  reserved {2}-{3}" -f $name, $p, $hit[0].Start, $hit[0].End) -ForegroundColor Red
        $conflict++
    } else {
        Write-Host ("  [OK]       {0,-12} {1}" -f $name, $p) -ForegroundColor Green
    }
}

Write-Host ""
if ($conflict -gt 0) {
    Write-Host "$conflict conflict(s). Change ports before starting containers." -ForegroundColor Red
    Write-Host "Update: docker-compose.yml + application.yml (api, simulator)"
} else {
    Write-Host "All clear. Safe to start containers." -ForegroundColor Green
}
