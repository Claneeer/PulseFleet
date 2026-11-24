<#
.SYNOPSIS
    PulseFleet (Marco 2) - Script Interativo de Demonstracao ao Vivo (Windows PowerShell)

.DESCRIPTION
    Executa as 7 etapas da demonstracao ao vivo:
      1. Healthcheck (FastAPI + Redis + ScyllaDB)
      2. Ingestao manual de evento critico (POST /api/v1/telemetry)
      3. Estado instantaneo O(1) via Redis (GET /live)
      4. Ranking de alertas termicos via Redis Sorted Set (GET /alerts/critical)
      5. Serie temporal regulatoria via ScyllaDB (GET /history)
      6. Inspecao direta no Redis CLI (HGETALL, TTL, ZREVRANGE, ZREVRANGEBYSCORE)
      7. Inspecao direta no ScyllaDB via CQLSH (DESCRIBE, TRACING ON, Anti-Padrao)

.EXAMPLE
    .\scripts\demo_queries.ps1
    Executa em modo interativo (pausa entre etapas).

.EXAMPLE
    .\scripts\demo_queries.ps1 -Auto
    Executa todas as etapas continuamente sem pausas.
#>
param(
    [switch]$Auto,
    [string]$ApiUrl = $(if ($env:API_URL) { $env:API_URL } else { "http://localhost:8000" }),
    [string]$VehicleId = $(if ($env:VEHICLE_ID) { $env:VEHICLE_ID } else { "VEIC-1024" }),
    [string]$YearMonth = $(if ($env:YEAR_MONTH) { $env:YEAR_MONTH } else { (Get-Date).ToUniversalTime().ToString("yyyy-MM") })
)

$NowIso = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")

function Write-Header {
    param([string]$Title)
    Write-Host ""
    Write-Host "==============================================================================" -ForegroundColor Cyan
    Write-Host " $Title" -ForegroundColor Cyan
    Write-Host "==============================================================================" -ForegroundColor Cyan
}

function Pause-Step {
    if (-not $Auto) {
        Write-Host ""
        Read-Host "Pressione [ENTER] para avancar para a proxima etapa" | Out-Null
    }
}

function Show-Json {
    param($Object)
    $Object | ConvertTo-Json -Depth 10
}

# ETAPA 1/7
Write-Header "ETAPA 1/7 - Healthcheck da Arquitetura Poliglota (FastAPI + Redis + ScyllaDB)"
try {
    $health = Invoke-RestMethod -Method Get -Uri "$ApiUrl/health" -ErrorAction Stop
    Show-Json $health
} catch {
    Write-Host "ERRO ao acessar $ApiUrl/health. Verifique se 'docker compose ps' mostra pulsefleet-api como healthy." -ForegroundColor Red
    Write-Host $_ -ForegroundColor Red
    exit 1
}
Pause-Step

# ETAPA 2/7
Write-Header "ETAPA 2/7 - Ingestao Manual de Evento Critico (POST /api/v1/telemetry)"
Write-Host "Enviando telemetria para $VehicleId com temperatura 9.4 C (violacao > 8.0 C)..." -ForegroundColor Green
$payload = @{
    vehicle_id  = $VehicleId
    timestamp   = $NowIso
    latitude    = -15.7942
    longitude   = -47.8822
    speed       = 72.5
    temperature = 9.4
} | ConvertTo-Json

$ingest = Invoke-RestMethod -Method Post -Uri "$ApiUrl/api/v1/telemetry" -ContentType "application/json" -Body $payload -ErrorAction Stop
Show-Json $ingest
Pause-Step

# ETAPA 3/7
Write-Header "ETAPA 3/7 - API REST: Estado Instantaneo O(1) via Redis (GET /live)"
Write-Host "GET $ApiUrl/api/v1/vehicles/$VehicleId/live" -ForegroundColor Green
$live = Invoke-RestMethod -Method Get -Uri "$ApiUrl/api/v1/vehicles/$VehicleId/live" -ErrorAction Stop
Show-Json $live
Pause-Step

# ETAPA 4/7
Write-Header "ETAPA 4/7 - API REST: Ranking de Alertas Termicos via Redis Sorted Set"
Write-Host "Top 5 veiculos mais quentes (ZREVRANGE):" -ForegroundColor Green
$top5 = Invoke-RestMethod -Method Get -Uri "$ApiUrl/api/v1/alerts/critical?limit=5" -ErrorAction Stop
Show-Json $top5

Write-Host ""
Write-Host "Apenas veiculos acima do limite seguro de 8.0 C (ZREVRANGEBYSCORE):" -ForegroundColor Green
$above8 = Invoke-RestMethod -Method Get -Uri "$ApiUrl/api/v1/alerts/critical?limit=10&min_temp=8.0" -ErrorAction Stop
Show-Json $above8
Pause-Step

# ETAPA 5/7
Write-Header "ETAPA 5/7 - API REST: Serie Temporal Regulatoria via ScyllaDB (GET /history)"
Write-Host "GET $ApiUrl/api/v1/vehicles/$VehicleId/history?year_month=$YearMonth&limit=5" -ForegroundColor Green
$history = Invoke-RestMethod -Method Get -Uri "$ApiUrl/api/v1/vehicles/$VehicleId/history?year_month=$YearMonth&limit=5" -ErrorAction Stop
Show-Json $history
Pause-Step

# ETAPA 6/7
Write-Header "ETAPA 6/7 - Inspecao Direta no Redis CLI (Hash, TTL e Sorted Set)"
Write-Host "1) HGETALL frota:veiculo:${VehicleId}:status" -ForegroundColor Green
docker compose exec -T redis redis-cli HGETALL "frota:veiculo:${VehicleId}:status"

Write-Host ""
Write-Host "2) TTL frota:veiculo:${VehicleId}:status (segundos restantes de 86400s / 24h)" -ForegroundColor Green
docker compose exec -T redis redis-cli TTL "frota:veiculo:${VehicleId}:status"

Write-Host ""
Write-Host "3) ZREVRANGE frota:alertas:temperatura 0 9 WITHSCORES (Top 10 mais quentes)" -ForegroundColor Green
docker compose exec -T redis redis-cli ZREVRANGE frota:alertas:temperatura 0 9 WITHSCORES

Write-Host ""
Write-Host "4) ZREVRANGEBYSCORE frota:alertas:temperatura +inf 8.0 WITHSCORES (Violacoes > 8 C)" -ForegroundColor Green
docker compose exec -T redis redis-cli ZREVRANGEBYSCORE frota:alertas:temperatura +inf 8.0 WITHSCORES
Pause-Step

# ETAPA 7/7
Write-Header "ETAPA 7/7 - Inspecao Direta no ScyllaDB via CQLSH (Partition Key + Ordering Fisico)"
Write-Host "1) DDL da tabela (CLUSTERING ORDER BY timestamp DESC + TWCS):" -ForegroundColor Green
docker compose exec -T scylla cqlsh -e "DESCRIBE TABLE pulsefleet.vehicle_telemetry;"

Write-Host ""
Write-Host "2) SELECT com Partition Key completa (vehicle_id, year_month) e TRACING ON:" -ForegroundColor Green
Write-Host "Observe que os timestamps ja retornam em ordem DECRESCENTE fisica, sem ALLOW FILTERING." -ForegroundColor Yellow
$validCql = "TRACING ON; SELECT vehicle_id, year_month, timestamp, temperature, speed, violation FROM pulsefleet.vehicle_telemetry WHERE vehicle_id = '$VehicleId' AND year_month = '$YearMonth' LIMIT 5;"
docker compose exec -T scylla cqlsh -e $validCql

Write-Host ""
Write-Host "3) Prova de conceito do Anti-Padrao (consulta SEM year_month -> rejeitada propositalmente pelo banco):" -ForegroundColor Yellow
$invalidCql = "SELECT vehicle_id, timestamp, temperature FROM pulsefleet.vehicle_telemetry WHERE vehicle_id = '$VehicleId' LIMIT 5;"
$cqlOutput = $invalidCql | docker compose exec -T scylla sh -c 'cqlsh 2>&1; echo EXIT:$?'
$exitLine = $cqlOutput | Where-Object { $_ -like "EXIT:*" } | Select-Object -Last 1
$cqlOutput | Where-Object { $_ -notlike "EXIT:*" -and $_.Trim() -ne "" } | ForEach-Object { Write-Host $_ -ForegroundColor DarkGray }
if ($exitLine -ne "EXIT:0") {
    Write-Host "[OK / ESPERADO] O ScyllaDB bloqueou a consulta como previsto, pois a Partition Key composta ((vehicle_id, year_month)) esta incompleta!" -ForegroundColor Green
}

Write-Host ""
Write-Host "Demonstracao concluida com sucesso!" -ForegroundColor Cyan
