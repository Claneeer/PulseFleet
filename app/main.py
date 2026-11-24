"""
PulseFleet (Marco 2) - API REST de Ingestao IoT e Auditoria Regulatoria.

Implementa o padrao de Persistencia Poliglota especificado no Marco 1:
  - Escrita dupla concorrente (Redis + ScyllaDB/Cassandra) na ingestao:
      * Redis Hash (frota:veiculo:{vehicle_id}:status) com EXPIRE 86400s
      * Redis Sorted Set (frota:alertas:temperatura) ranqueando desvios termicos
      * ScyllaDB/Cassandra (pulsefleet.vehicle_telemetry) append-only em LSM-Tree
  - Leituras direcionadas para a camada especializada de cada padrao de acesso:
      * Estado ao vivo O(1) -> Redis HGETALL
      * Ranking de alertas termicos O(log N + M) -> Redis ZREVRANGE / ZREVRANGEBYSCORE
      * Historico regulatorio -> ScyllaDB/Cassandra via Partition Key completa
        (vehicle_id, year_month) sem JOINs e sem ALLOW FILTERING.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field

from database import db

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
logger = logging.getLogger("pulsefleet.api")

# Faixa termica segura para cadeia fria farmaceutica (vacinas/biologicos: 2 C a 8 C)
TEMP_MIN: float = float(os.getenv("TEMP_MIN", "2.0"))
TEMP_MAX: float = float(os.getenv("TEMP_MAX", "8.0"))
STATUS_TTL_SECONDS: int = int(os.getenv("STATUS_TTL_SECONDS", "86400"))

# Chaves padronizadas no Redis conforme especificacao do Marco 1
ALERTS_ZSET_KEY: str = "frota:alertas:temperatura"


def vehicle_status_key(vehicle_id: str) -> str:
    """Retorna o padrao de chave Hash para estado instantaneo do veiculo."""
    return f"frota:veiculo:{vehicle_id}:status"


def classify_thermal_status(temperature: float) -> str:
    """Classifica o desvio termico em relacao a faixa regulatoria [2.0 C, 8.0 C]."""
    if temperature > TEMP_MAX:
        return "CRITICAL_HIGH"
    if temperature < TEMP_MIN:
        return "CRITICAL_LOW"
    return "NORMAL"


def to_utc(dt: datetime) -> datetime:
    """Normaliza timestamps recebidos para UTC timezone-aware."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_iso_utc(dt: datetime) -> str:
    """Formata datetime em ISO-8601 UTC com sufixo 'Z'."""
    utc_dt = to_utc(dt)
    return utc_dt.isoformat().replace("+00:00", "Z")


# ==============================================================================
# Modelos Pydantic (Contratos da API)
# ==============================================================================


class TelemetryIn(BaseModel):
    """Payload JSON enviado pelos sensores IoT embarcados nos veiculos."""

    vehicle_id: str = Field(
        ...,
        min_length=2,
        max_length=64,
        examples=["VEIC-1024"],
        description="Identificador unico do veiculo na frota",
    )
    timestamp: datetime = Field(
        ...,
        examples=["2026-11-24T08:30:00Z"],
        description="Instante da leitura em formato ISO-8601 UTC",
    )
    latitude: float = Field(..., ge=-90.0, le=90.0, examples=[-15.7942])
    longitude: float = Field(..., ge=-180.0, le=180.0, examples=[-47.8822])
    speed: float = Field(..., ge=0.0, examples=[72.5], description="Velocidade em km/h")
    temperature: float = Field(
        ...,
        ge=-80.0,
        le=100.0,
        examples=[9.4],
        description="Temperatura do bau refrigerado em graus Celsius",
    )


class TelemetryIngestResponse(BaseModel):
    status: str
    vehicle_id: str
    year_month: str
    timestamp: str
    temperature: float
    violation: bool
    thermal_status: str
    redis_key: str
    ttl_seconds: int
    latencies_ms: dict[str, float]


class VehicleLiveResponse(BaseModel):
    vehicle_id: str
    redis_key: str
    ttl_seconds: int
    timestamp: str
    latitude: float
    longitude: float
    speed: float
    temperature: float
    violation: bool
    thermal_status: str
    query_ms: float


class AlertEntry(BaseModel):
    rank: int
    vehicle_id: str
    temperature: float
    violation: bool
    thermal_status: str


class CriticalAlertsResponse(BaseModel):
    redis_key: str
    redis_command: str
    safe_range_celsius: dict[str, float]
    count: int
    query_ms: float
    alerts: list[AlertEntry]


class TelemetryRecord(BaseModel):
    vehicle_id: str
    year_month: str
    timestamp: str
    latitude: float
    longitude: float
    speed: float
    temperature: float
    violation: bool


class VehicleHistoryResponse(BaseModel):
    vehicle_id: str
    year_month: str
    partition_key: list[str]
    clustering_order: str
    count: int
    violation_count: int
    query_ms: float
    records: list[TelemetryRecord]


# ==============================================================================
# Ciclo de Vida da Aplicacao (Startup com Retry + Shutdown Gracioso)
# ==============================================================================


@asynccontextmanager
async def lifespan(_: FastAPI):
    logger.info("Iniciando PulseFleet API - conectando as camadas NoSQL...")
    await asyncio.to_thread(db.connect)
    logger.info("PulseFleet API pronta para receber telemetria.")
    try:
        yield
    finally:
        await asyncio.to_thread(db.close)


app = FastAPI(
    title="PulseFleet Telemetry & Audit API",
    description=(
        "Plataforma IoT de telemetria e auditoria regulatoria (ANVISA/FDA) com "
        "Persistencia Poliglota: Redis (estado O(1) + ranking de alertas) e "
        "ScyllaDB/Cassandra (series temporais imutaveis em LSM-Tree)."
    ),
    version="2.0.0",
    lifespan=lifespan,
)


# ==============================================================================
# Funcoes Auxiliares de Persistencia (executadas em threadpool)
# ==============================================================================


def _write_redis(
    vehicle_id: str,
    timestamp_iso: str,
    latitude: float,
    longitude: float,
    speed: float,
    temperature: float,
    violation: bool,
) -> float:
    """
    Atualiza o estado instantaneo e o ranking de desvios no Redis em 1 RTT
    usando pipeline (HSET + EXPIRE + ZADD).
    """
    if db.redis_client is None:
        raise RuntimeError("Cliente Redis nao inicializado.")

    key = vehicle_status_key(vehicle_id)
    start = time.perf_counter()

    # Pipeline reduz round-trips de rede de 3 para 1 (essencial em 2.000+ ops/s)
    pipe = db.redis_client.pipeline(transaction=True)
    pipe.hset(
        key,
        mapping={
            "timestamp": timestamp_iso,
            "latitude": f"{latitude:.6f}",
            "longitude": f"{longitude:.6f}",
            "speed": f"{speed:.2f}",
            "temperature": f"{temperature:.2f}",
            "violation": "true" if violation else "false",
        },
    )
    # TTL de 24h evita acumulo de veiculos inativos na memoria RAM
    pipe.expire(key, STATUS_TTL_SECONDS)
    # Sorted Set ordenado por score = temperatura para deteccao imediata O(log N)
    pipe.zadd(ALERTS_ZSET_KEY, {vehicle_id: temperature})
    pipe.execute()

    return round((time.perf_counter() - start) * 1000.0, 3)


def _write_cassandra(
    vehicle_id: str,
    year_month: str,
    timestamp_utc: datetime,
    latitude: float,
    longitude: float,
    speed: float,
    temperature: float,
    violation: bool,
) -> float:
    """
    Persiste evento imutavel (append-only) na LSM-Tree do ScyllaDB/Cassandra.

    Sem locks de escrita e sem JOINs: o dado vai direto para CommitLog + Memtable
    da particao ((vehicle_id, year_month)) ja ordenado por timestamp DESC.
    """
    if db.cassandra_session is None or db.insert_telemetry_stmt is None:
        raise RuntimeError("Sessao ScyllaDB/Cassandra nao inicializada.")

    start = time.perf_counter()
    # cassandra-driver utiliza datetime naive representando UTC
    ts_naive_utc = timestamp_utc.replace(tzinfo=None)
    db.cassandra_session.execute(
        db.insert_telemetry_stmt,
        (
            vehicle_id,
            year_month,
            ts_naive_utc,
            latitude,
            longitude,
            speed,
            temperature,
            violation,
        ),
    )
    return round((time.perf_counter() - start) * 1000.0, 3)


def _read_vehicle_live(vehicle_id: str) -> tuple[dict[str, str], int, float]:
    """Consulta O(1) do estado atual do veiculo + TTL restante no Redis."""
    if db.redis_client is None:
        raise RuntimeError("Cliente Redis nao inicializado.")

    key = vehicle_status_key(vehicle_id)
    start = time.perf_counter()
    pipe = db.redis_client.pipeline(transaction=False)
    pipe.hgetall(key)
    pipe.ttl(key)
    data, ttl = pipe.execute()
    elapsed_ms = round((time.perf_counter() - start) * 1000.0, 3)
    return data, int(ttl), elapsed_ms


def _read_critical_alerts(
    limit: int, min_temp: Optional[float]
) -> tuple[list[tuple[str, float]], str, float]:
    """
    Recupera o ranking dos veiculos mais quentes do Sorted Set Redis.
    - Sem min_temp: ZREVRANGE (Top-N temperaturas mais altas).
    - Com min_temp: ZREVRANGEBYSCORE (faixa critica >= min_temp, ex: > 8.0 C).
    """
    if db.redis_client is None:
        raise RuntimeError("Cliente Redis nao inicializado.")

    start = time.perf_counter()
    if min_temp is not None:
        rows = db.redis_client.zrevrangebyscore(
            ALERTS_ZSET_KEY,
            max="+inf",
            min=min_temp,
            start=0,
            num=limit,
            withscores=True,
        )
        command_used = f"ZREVRANGEBYSCORE {ALERTS_ZSET_KEY} +inf {min_temp} WITHSCORES LIMIT 0 {limit}"
    else:
        rows = db.redis_client.zrevrange(
            ALERTS_ZSET_KEY,
            start=0,
            end=limit - 1,
            withscores=True,
        )
        command_used = f"ZREVRANGE {ALERTS_ZSET_KEY} 0 {limit - 1} WITHSCORES"

    elapsed_ms = round((time.perf_counter() - start) * 1000.0, 3)
    return rows, command_used, elapsed_ms


def _read_vehicle_history(
    vehicle_id: str, year_month: str, limit: int
) -> tuple[list[TelemetryRecord], float]:
    """
    Consulta historica no ScyllaDB/Cassandra usando Partition Key completa.
    Garante leitura sequencial na particao sem full-scan e sem ALLOW FILTERING.
    """
    if db.cassandra_session is None or db.select_history_stmt is None:
        raise RuntimeError("Sessao ScyllaDB/Cassandra nao inicializada.")

    start = time.perf_counter()
    result_set = db.cassandra_session.execute(
        db.select_history_stmt,
        (vehicle_id, year_month, limit),
    )

    records: list[TelemetryRecord] = []
    for row in result_set:
        records.append(
            TelemetryRecord(
                vehicle_id=row.vehicle_id,
                year_month=row.year_month,
                timestamp=format_iso_utc(row.timestamp),
                latitude=row.latitude,
                longitude=row.longitude,
                speed=row.speed,
                temperature=row.temperature,
                violation=bool(row.violation),
            )
        )

    elapsed_ms = round((time.perf_counter() - start) * 1000.0, 3)
    return records, elapsed_ms


# ==============================================================================
# Endpoints REST
# ==============================================================================


@app.get("/", tags=["Sistema"])
async def root() -> dict[str, Any]:
    """Resumo da API e endpoints disponiveis para demonstracao."""
    return {
        "service": "PulseFleet Telemetry & Regulatory Audit API",
        "milestone": "Marco 2 - Sistema Completo e Funcional",
        "architecture": "Polyglot Persistence (Redis + ScyllaDB/Cassandra)",
        "safe_temperature_range_c": {"min": TEMP_MIN, "max": TEMP_MAX},
        "endpoints": {
            "health": "GET /health",
            "docs": "GET /docs",
            "ingest_telemetry": "POST /api/v1/telemetry",
            "vehicle_live": "GET /api/v1/vehicles/{vehicle_id}/live",
            "critical_alerts": "GET /api/v1/alerts/critical?limit=10&min_temp=8.0",
            "vehicle_history": "GET /api/v1/vehicles/{vehicle_id}/history?year_month=2026-11&limit=50",
        },
    }


@app.get("/health", tags=["Sistema"])
async def health() -> dict[str, Any]:
    """Healthcheck das camadas Redis e ScyllaDB/Cassandra."""
    report = await asyncio.to_thread(db.check_health)
    if report["status"] != "healthy":
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=report,
        )
    return report


@app.post(
    "/api/v1/telemetry",
    response_model=TelemetryIngestResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["Telemetria IoT"],
)
async def ingest_telemetry(payload: TelemetryIn) -> TelemetryIngestResponse:
    """
    Recebe um pacote de telemetria IoT e aplica persistencia poliglota concorrente:
      1. Calcula a flag `violation` (T < 2.0 C ou T > 8.0 C).
      2. Deriva o bucket mensal `year_month` (YYYY-MM) para particionamento temporal.
      3. Grava em paralelo no Redis (estado instantaneo + ranking) e no ScyllaDB (serie temporal).
    """
    ts_utc = to_utc(payload.timestamp)
    ts_iso = format_iso_utc(ts_utc)
    year_month = ts_utc.strftime("%Y-%m")
    violation = payload.temperature < TEMP_MIN or payload.temperature > TEMP_MAX
    thermal_status = classify_thermal_status(payload.temperature)

    total_start = time.perf_counter()
    try:
        redis_ms, cassandra_ms = await asyncio.gather(
            asyncio.to_thread(
                _write_redis,
                payload.vehicle_id,
                ts_iso,
                payload.latitude,
                payload.longitude,
                payload.speed,
                payload.temperature,
                violation,
            ),
            asyncio.to_thread(
                _write_cassandra,
                payload.vehicle_id,
                year_month,
                ts_utc,
                payload.latitude,
                payload.longitude,
                payload.speed,
                payload.temperature,
                violation,
            ),
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Falha na persistencia poliglota para %s", payload.vehicle_id)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Erro ao persistir telemetria nas camadas NoSQL: {exc}",
        ) from exc

    total_ms = round((time.perf_counter() - total_start) * 1000.0, 3)

    return TelemetryIngestResponse(
        status="ingested",
        vehicle_id=payload.vehicle_id,
        year_month=year_month,
        timestamp=ts_iso,
        temperature=payload.temperature,
        violation=violation,
        thermal_status=thermal_status,
        redis_key=vehicle_status_key(payload.vehicle_id),
        ttl_seconds=STATUS_TTL_SECONDS,
        latencies_ms={
            "redis": redis_ms,
            "scylla_cassandra": cassandra_ms,
            "total": total_ms,
        },
    )


@app.get(
    "/api/v1/vehicles/{vehicle_id}/live",
    response_model=VehicleLiveResponse,
    tags=["Monitoramento em Tempo Real (Redis)"],
)
async def get_vehicle_live(vehicle_id: str) -> VehicleLiveResponse:
    """Retorna o estado instantaneo do veiculo lido diretamente do Redis (HGETALL O(1))."""
    try:
        data, ttl, query_ms = await asyncio.to_thread(_read_vehicle_live, vehicle_id)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Erro ao consultar Redis: {exc}",
        ) from exc

    if not data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"Veiculo '{vehicle_id}' nao encontrado no cache Redis "
                f"(chave '{vehicle_status_key(vehicle_id)}' ausente ou expirada apos 24h)."
            ),
        )

    temperature = float(data["temperature"])
    violation = data.get("violation", "false").lower() == "true"

    return VehicleLiveResponse(
        vehicle_id=vehicle_id,
        redis_key=vehicle_status_key(vehicle_id),
        ttl_seconds=ttl,
        timestamp=data["timestamp"],
        latitude=float(data["latitude"]),
        longitude=float(data["longitude"]),
        speed=float(data["speed"]),
        temperature=temperature,
        violation=violation,
        thermal_status=classify_thermal_status(temperature),
        query_ms=query_ms,
    )


@app.get(
    "/api/v1/alerts/critical",
    response_model=CriticalAlertsResponse,
    tags=["Monitoramento em Tempo Real (Redis)"],
)
async def get_critical_alerts(
    limit: int = Query(
        default=10,
        ge=1,
        le=500,
        description="Quantidade maxima de veiculos no ranking",
    ),
    min_temp: Optional[float] = Query(
        default=None,
        description=(
            "Temperatura minima (C) para filtragem por faixa via ZREVRANGEBYSCORE "
            "(ex: 8.0 para listar apenas veiculos acima do limite seguro). "
            "Quando omitido, retorna o Top-N mais quentes via ZREVRANGE."
        ),
    ),
) -> CriticalAlertsResponse:
    """Retorna o ranking de desvios termicos a partir do Sorted Set no Redis."""
    try:
        rows, command_used, query_ms = await asyncio.to_thread(
            _read_critical_alerts, limit, min_temp
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Erro ao consultar ranking de alertas no Redis: {exc}",
        ) from exc

    alerts: list[AlertEntry] = []
    for idx, (vehicle_id, score) in enumerate(rows, start=1):
        temp = round(float(score), 2)
        alerts.append(
            AlertEntry(
                rank=idx,
                vehicle_id=vehicle_id,
                temperature=temp,
                violation=(temp < TEMP_MIN or temp > TEMP_MAX),
                thermal_status=classify_thermal_status(temp),
            )
        )

    return CriticalAlertsResponse(
        redis_key=ALERTS_ZSET_KEY,
        redis_command=command_used,
        safe_range_celsius={"min": TEMP_MIN, "max": TEMP_MAX},
        count=len(alerts),
        query_ms=query_ms,
        alerts=alerts,
    )


@app.get(
    "/api/v1/vehicles/{vehicle_id}/history",
    response_model=VehicleHistoryResponse,
    tags=["Auditoria Regulatoria (ScyllaDB / Cassandra)"],
)
async def get_vehicle_history(
    vehicle_id: str,
    year_month: str = Query(
        ...,
        pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
        examples=["2026-11"],
        description="Bucket mensal obrigatorio (YYYY-MM) que compoe a Partition Key",
    ),
    limit: int = Query(
        default=50,
        ge=1,
        le=1000,
        description="Numero maximo de registros retornados (ordem cronologica decrescente)",
    ),
) -> VehicleHistoryResponse:
    """
    Retorna a serie temporal imutavel do veiculo diretamente do ScyllaDB/Cassandra.

    Regra arquitetural rigorosa: exige a Partition Key completa (vehicle_id, year_month)
    e explora o CLUSTERING ORDER BY (timestamp DESC) fisico sem ALLOW FILTERING.
    """
    try:
        records, query_ms = await asyncio.to_thread(
            _read_vehicle_history, vehicle_id, year_month, limit
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Erro ao consultar historico no ScyllaDB/Cassandra: {exc}",
        ) from exc

    violation_count = sum(1 for rec in records if rec.violation)

    return VehicleHistoryResponse(
        vehicle_id=vehicle_id,
        year_month=year_month,
        partition_key=[vehicle_id, year_month],
        clustering_order="timestamp DESC (ordenacao fisica em SSTable/Memtable)",
        count=len(records),
        violation_count=violation_count,
        query_ms=query_ms,
        records=records,
    )
