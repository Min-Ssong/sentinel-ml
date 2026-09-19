# Sentinel-ML port check
# Windows reserved port ranges change on reboot / WSL restart.
# Run this BEFORE starting containers.

$envPath = Join-Path $PSScriptRoot ".env"
$env_vars = @{}
Get-Content $envPath | Where-Object { $_ -match "^\s*[^#].*=" } | ForEach-Object {
    $k, $v = $_ -split "=", 2
    $env_vars[$k.Trim()] = $v.Trim()
}

$ports = @{
    "Kafka"      = [int]$env_vars["KAFKA_PORT"]
    "Zookeeper"  = [int]$env_vars["ZOOKEEPER_PORT"]
    "PostgreSQL" = [int]$env_vars["POSTGRES_PORT"]
    "Redis"      = [int]$env_vars["REDIS_PORT"]
    "MLflow"     = [int]$env_vars["MLFLOW_PORT"]
    "SpringAPI"  = [int]$env_vars["SPRING_API_PORT"]
    "FastAPI"    = [int]$env_vars["FASTAPI_PORT"]
    "Airflow"    = [int]$env_vars["AIRFLOW_PORT"]
    "Prometheus" = [int]$env_vars["PROMETHEUS_PORT"]
    "Grafana"    = [int]$env_vars["GRAFANA_PORT"]
    "Pushgateway"= [int]$env_vars["PUSHGATEWAY_PORT"]
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
