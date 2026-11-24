#!/usr/bin/env bash
# ==============================================================================
# PulseFleet (Marco 2) - Script Interativo de Demonstracao ao Vivo
# ==============================================================================
# Uso:
#   ./scripts/demo_queries.sh          # Modo interativo (pausa entre etapas)
#   ./scripts/demo_queries.sh --auto   # Modo nao-interativo (execucao direta)
#
# Variaveis opcionais:
#   API_URL=http://localhost:8000
#   VEHICLE_ID=VEIC-1024
#   YEAR_MONTH=2026-11
# ==============================================================================

set -euo pipefail

API_URL="${API_URL:-http://localhost:8000}"
VEHICLE_ID="${VEHICLE_ID:-VEIC-1024}"
YEAR_MONTH="${YEAR_MONTH:-$(date -u +%Y-%m)}"
NOW_ISO="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
AUTO_MODE=false

if [[ "${1:-}" == "--auto" || "${1:-}" == "-y" ]]; then
  AUTO_MODE=true
fi

# Cores para facilitar a leitura durante a projecao do seminario
CYAN='\033[1;36m'
GREEN='\033[1;32m'
YELLOW='\033[1;33m'
RED='\033[1;31m'
RESET='\033[0m'

pretty_json() {
  if command -v jq >/dev/null 2>&1; then
    jq .
  elif command -v python3 >/dev/null 2>&1 && python3 -c "import json" >/dev/null 2>&1; then
    python3 -m json.tool
  elif command -v python >/dev/null 2>&1 && python -c "import json" >/dev/null 2>&1; then
    python -m json.tool
  else
    cat
  fi
}

pause_step() {
  if [[ "$AUTO_MODE" == false ]]; then
    echo ""
    read -r -p "$(echo -e "${YELLOW}Pressione [ENTER] para avancar para a proxima etapa...${RESET}")"
  fi
}

header() {
  echo ""
  echo -e "${CYAN}==============================================================================${RESET}"
  echo -e "${CYAN} $1${RESET}"
  echo -e "${CYAN}==============================================================================${RESET}"
}

header "ETAPA 1/7 - Healthcheck da Arquitetura Poliglota (FastAPI + Redis + ScyllaDB)"
curl -sS "${API_URL}/health" | pretty_json
pause_step

header "ETAPA 2/7 - Ingestao Manual de Evento Critico (POST /api/v1/telemetry)"
echo -e "${GREEN}Enviando telemetria para ${VEHICLE_ID} com temperatura 9.4 C (violacao > 8.0 C)...${RESET}"
curl -sS -X POST "${API_URL}/api/v1/telemetry" \
  -H "Content-Type: application/json" \
  -d "{
    \"vehicle_id\": \"${VEHICLE_ID}\",
    \"timestamp\": \"${NOW_ISO}\",
    \"latitude\": -15.7942,
    \"longitude\": -47.8822,
    \"speed\": 72.5,
    \"temperature\": 9.4
  }" | pretty_json
pause_step

header "ETAPA 3/7 - API REST: Estado Instantaneo O(1) via Redis (GET /live)"
echo -e "${GREEN}GET ${API_URL}/api/v1/vehicles/${VEHICLE_ID}/live${RESET}"
curl -sS "${API_URL}/api/v1/vehicles/${VEHICLE_ID}/live" | pretty_json
pause_step

header "ETAPA 4/7 - API REST: Ranking de Alertas Termicos via Redis Sorted Set"
echo -e "${GREEN}Top 5 veiculos mais quentes (ZREVRANGE):${RESET}"
curl -sS "${API_URL}/api/v1/alerts/critical?limit=5" | pretty_json
echo ""
echo -e "${GREEN}Apenas veiculos acima do limite seguro de 8.0 C (ZREVRANGEBYSCORE):${RESET}"
curl -sS "${API_URL}/api/v1/alerts/critical?limit=10&min_temp=8.0" | pretty_json
pause_step

header "ETAPA 5/7 - API REST: Serie Temporal Regulatoria via ScyllaDB (GET /history)"
echo -e "${GREEN}GET ${API_URL}/api/v1/vehicles/${VEHICLE_ID}/history?year_month=${YEAR_MONTH}&limit=5${RESET}"
curl -sS "${API_URL}/api/v1/vehicles/${VEHICLE_ID}/history?year_month=${YEAR_MONTH}&limit=5" | pretty_json
pause_step

header "ETAPA 6/7 - Inspecao Direta no Redis CLI (Hash, TTL e Sorted Set)"
echo -e "${GREEN}1) HGETALL frota:veiculo:${VEHICLE_ID}:status${RESET}"
docker compose exec -T redis redis-cli HGETALL "frota:veiculo:${VEHICLE_ID}:status"

echo ""
echo -e "${GREEN}2) TTL frota:veiculo:${VEHICLE_ID}:status (segundos restantes de 86400s / 24h)${RESET}"
docker compose exec -T redis redis-cli TTL "frota:veiculo:${VEHICLE_ID}:status"

echo ""
echo -e "${GREEN}3) ZREVRANGE frota:alertas:temperatura 0 9 WITHSCORES (Top 10 mais quentes)${RESET}"
docker compose exec -T redis redis-cli ZREVRANGE frota:alertas:temperatura 0 9 WITHSCORES

echo ""
echo -e "${GREEN}4) ZREVRANGEBYSCORE frota:alertas:temperatura +inf 8.0 WITHSCORES (Violacoes > 8 C)${RESET}"
docker compose exec -T redis redis-cli ZREVRANGEBYSCORE frota:alertas:temperatura +inf 8.0 WITHSCORES
pause_step

header "ETAPA 7/7 - Inspecao Direta no ScyllaDB via CQLSH (Partition Key + Ordering Fisico)"
echo -e "${GREEN}1) DDL da tabela (CLUSTERING ORDER BY timestamp DESC + TWCS):${RESET}"
docker compose exec -T scylla cqlsh -e "DESCRIBE TABLE pulsefleet.vehicle_telemetry;"

echo ""
echo -e "${GREEN}2) SELECT com Partition Key completa (vehicle_id, year_month) e TRACING ON:${RESET}"
echo -e "${YELLOW}Observe que os timestamps ja retornam em ordem DECRESCENTE fisica, sem ALLOW FILTERING.${RESET}"
docker compose exec -T scylla cqlsh -e "TRACING ON; SELECT vehicle_id, year_month, timestamp, temperature, speed, violation FROM pulsefleet.vehicle_telemetry WHERE vehicle_id = '${VEHICLE_ID}' AND year_month = '${YEAR_MONTH}' LIMIT 5;"

echo ""
echo -e "${YELLOW}3) Prova de conceito do Anti-Padrao (consulta SEM year_month -> rejeitada propositalmente pelo banco):${RESET}"
docker compose exec -T scylla bash -c "cqlsh -e \"SELECT vehicle_id, timestamp, temperature FROM pulsefleet.vehicle_telemetry WHERE vehicle_id = '${VEHICLE_ID}' LIMIT 5;\" 2>&1 && exit 0 || echo '__BLOCKED__'" | while IFS= read -r line; do
  if [[ "$line" == "__BLOCKED__" ]]; then
    echo -e "${GREEN}[OK / ESPERADO] O ScyllaDB bloqueou a consulta como previsto, pois a Partition Key composta ((vehicle_id, year_month)) esta incompleta!${RESET}"
  else
    echo "$line"
  fi
done

echo ""
echo -e "${CYAN}Demonstracao concluida com sucesso!${RESET}"
