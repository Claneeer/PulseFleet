"""
PulseFleet (Marco 2) - Simulador Concorrente de Telemetria IoT.

Simula de 10 a 100 caminhoes refrigerados emitindo leituras concorrentes de
GPS, velocidade e temperatura a cada 1-2 segundos para a API REST do PulseFleet.

Anomalias termicas injetadas propositalmente para demonstracao ao vivo:
  - VEIC-1024 : Falha grave no compressor (superaquecimento: 9.2 C a 14.8 C)
  - VEIC-1007 : Vedacao da porta comprometida (aquecimento: 8.3 C a 11.2 C)
  - VEIC-1013 : Termostato travado (risco de congelamento: -1.4 C a 1.5 C)
  - Demais    : Operacao nominal dentro da faixa ANVISA/FDA (2.0 C a 8.0 C)
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx

# Corredores logisticos brasileiros (lat, lon base) para coordenadas realistas
BASE_ROUTES: list[tuple[str, float, float]] = [
    ("Brasilia-DF", -15.7942, -47.8822),
    ("Sao Paulo-SP", -23.5505, -46.6333),
    ("Campinas-SP", -22.9099, -47.0626),
    ("Rio de Janeiro-RJ", -22.9068, -43.1729),
    ("Belo Horizonte-MG", -19.9167, -43.9345),
    ("Curitiba-PR", -25.4284, -49.2733),
    ("Goiania-GO", -16.6869, -49.2648),
    ("Porto Alegre-RS", -30.0346, -51.2177),
]


@dataclass
class VirtualTruck:
    """Estado dinamico de um caminhao refrigerado simulado."""

    vehicle_id: str
    profile: str  # "normal" | "hot_critical" | "hot_warning" | "cold_critical"
    latitude: float
    longitude: float
    speed: float
    temperature: float
    heading_lat: float
    heading_lon: float

    def next_reading(self) -> dict[str, object]:
        """Gera a proxima leitura de telemetria com evolucao fisica coerente."""
        # Movimentacao suave de GPS ao longo da rodovia
        self.latitude = max(-33.0, min(5.0, self.latitude + self.heading_lat * random.uniform(0.0005, 0.002)))
        self.longitude = max(-73.0, min(-34.0, self.longitude + self.heading_lon * random.uniform(0.0005, 0.002)))
        self.speed = round(max(0.0, min(110.0, self.speed + random.uniform(-4.5, 4.5))), 2)

        # Dinamica termica por perfil de operacao
        if self.profile == "hot_critical":
            # VEIC-1024: falha no compressor -> mantem-se acima de 9.2 C (critico)
            target = random.uniform(10.5, 14.5)
            self.temperature = max(9.2, min(14.8, self.temperature + (target - self.temperature) * 0.35 + random.uniform(-0.3, 0.4)))
        elif self.profile == "hot_warning":
            # VEIC-1007: infiltracao de calor -> mantem-se entre 8.3 C e 11.2 C
            target = random.uniform(8.8, 10.8)
            self.temperature = max(8.3, min(11.2, self.temperature + (target - self.temperature) * 0.35 + random.uniform(-0.25, 0.35)))
        elif self.profile == "cold_critical":
            # VEIC-1013: risco de congelamento de vacinas (< 2.0 C)
            target = random.uniform(-1.0, 1.2)
            self.temperature = max(-1.4, min(1.5, self.temperature + (target - self.temperature) * 0.35 + random.uniform(-0.2, 0.2)))
        else:
            # Operacao nominal estavel dentro de [2.5 C, 7.3 C]
            target = 5.0
            self.temperature = max(2.5, min(7.3, self.temperature + (target - self.temperature) * 0.2 + random.uniform(-0.45, 0.45)))

        self.temperature = round(self.temperature, 2)

        return {
            "vehicle_id": self.vehicle_id,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "latitude": round(self.latitude, 6),
            "longitude": round(self.longitude, 6),
            "speed": self.speed,
            "temperature": self.temperature,
        }


@dataclass
class SimulationMetrics:
    """Metricas agregadas para telemetria do proprio gerador de carga."""

    sent: int = 0
    errors: int = 0
    violations: int = 0
    total_latency_ms: float = 0.0
    hottest_vehicle: str = "-"
    hottest_temp: float = -999.0


def build_fleet(num_vehicles: int) -> list[VirtualTruck]:
    """
    Constroi a frota virtual garantindo presenca dos 3 veiculos com anomalia:
      - VEIC-1024 (hot_critical)
      - VEIC-1007 (hot_warning)
      - VEIC-1013 (cold_critical)
    """
    vehicle_ids: list[str] = ["VEIC-1024", "VEIC-1007", "VEIC-1013"]
    seq = 1001
    while len(vehicle_ids) < num_vehicles:
        candidate = f"VEIC-{seq}"
        if candidate not in vehicle_ids:
            vehicle_ids.append(candidate)
        seq += 1

    anomaly_profiles = {
        "VEIC-1024": ("hot_critical", 11.4),
        "VEIC-1007": ("hot_warning", 9.3),
        "VEIC-1013": ("cold_critical", 0.4),
    }

    fleet: list[VirtualTruck] = []
    for idx, vid in enumerate(vehicle_ids):
        _, base_lat, base_lon = BASE_ROUTES[idx % len(BASE_ROUTES)]
        profile, initial_temp = anomaly_profiles.get(
            vid, ("normal", round(random.uniform(3.2, 6.8), 2))
        )
        fleet.append(
            VirtualTruck(
                vehicle_id=vid,
                profile=profile,
                latitude=base_lat + random.uniform(-0.15, 0.15),
                longitude=base_lon + random.uniform(-0.15, 0.15),
                speed=round(random.uniform(50.0, 90.0), 2),
                temperature=initial_temp,
                heading_lat=random.choice([-1.0, 1.0]),
                heading_lon=random.choice([-1.0, 1.0]),
            )
        )
    return fleet


async def wait_for_api(client: httpx.AsyncClient, api_url: str, timeout_s: int = 60) -> None:
    """Aguarda a API responder /health=200 antes de disparar trafego."""
    deadline = time.monotonic() + timeout_s
    health_url = f"{api_url.rstrip('/')}/health"
    while time.monotonic() < deadline:
        try:
            resp = await client.get(health_url, timeout=5.0)
            if resp.status_code == 200:
                print(f"[SIMULADOR] API saudavel em {health_url}. Iniciando geracao de carga IoT...")
                return
        except httpx.HTTPError:
            pass
        print(f"[SIMULADOR] Aguardando API ficar disponivel em {health_url}...")
        await asyncio.sleep(2.0)

    raise RuntimeError(f"API nao ficou saudavel em {health_url} dentro de {timeout_s}s.")


async def truck_worker(
    truck: VirtualTruck,
    client: httpx.AsyncClient,
    endpoint: str,
    interval: float,
    stop_event: asyncio.Event,
    metrics: SimulationMetrics,
) -> None:
    """Loop assincrono independente representando o dispositivo IoT de 1 caminhao."""
    # Pequeno jitter inicial para distribuir requisicoes uniformemente no tempo
    await asyncio.sleep(random.uniform(0.0, min(interval, 1.0)))

    while not stop_event.is_set():
        payload = truck.next_reading()
        t0 = time.perf_counter()
        try:
            response = await client.post(endpoint, json=payload, timeout=10.0)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            if response.status_code in (200, 201):
                body = response.json()
                metrics.sent += 1
                metrics.total_latency_ms += elapsed_ms
                if body.get("violation"):
                    metrics.violations += 1
                temp = float(payload["temperature"])
                if temp > metrics.hottest_temp:
                    metrics.hottest_temp = temp
                    metrics.hottest_vehicle = truck.vehicle_id
            else:
                metrics.errors += 1
                print(
                    f"[ERRO] {truck.vehicle_id} HTTP {response.status_code}: {response.text[:160]}"
                )
        except httpx.HTTPError as exc:
            metrics.errors += 1
            print(f"[ERRO] {truck.vehicle_id} falha de rede: {exc}")

        # Espera intervalo configuravel com jitter realista (+-20%)
        sleep_time = max(0.2, interval * random.uniform(0.8, 1.2))
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=sleep_time)
        except asyncio.TimeoutError:
            pass


async def metrics_reporter(
    metrics: SimulationMetrics,
    start_time: float,
    stop_event: asyncio.Event,
) -> None:
    """Exibe resumo periodico de vazao, violacoes e latencia media no terminal."""
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            elapsed = max(0.001, time.monotonic() - start_time)
            ops_s = metrics.sent / elapsed
            avg_ms = (metrics.total_latency_ms / metrics.sent) if metrics.sent else 0.0
            print(
                f"[METRICAS] t={elapsed:5.1f}s | enviados={metrics.sent:5d} | "
                f"vazao={ops_s:6.1f} ops/s | violacoes={metrics.violations:4d} | "
                f"erros={metrics.errors:2d} | latencia_media={avg_ms:5.1f} ms | "
                f"pico_calor={metrics.hottest_vehicle} ({metrics.hottest_temp:.1f} C)"
            )


async def run_simulation(
    api_url: str,
    num_vehicles: int,
    interval: float,
    duration: float,
) -> None:
    """Orquestra todos os caminhoes virtuais concorrentes."""
    fleet = build_fleet(num_vehicles)
    metrics = SimulationMetrics()
    stop_event = asyncio.Event()
    endpoint = f"{api_url.rstrip('/')}/api/v1/telemetry"

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # Windows: tratado via KeyboardInterrupt no bloco principal
            pass

    limits = httpx.Limits(
        max_connections=max(100, num_vehicles * 2),
        max_keepalive_connections=max(50, num_vehicles),
    )

    print("=" * 78)
    print(" PulseFleet IoT Load Simulator (Marco 2)")
    print(f" Destino    : {endpoint}")
    print(f" Veiculos   : {num_vehicles} caminhoes concorrentes (intervalo ~{interval:.1f}s)")
    print(" Anomalias  : VEIC-1024 (>9.2 C), VEIC-1007 (>8.3 C), VEIC-1013 (<1.5 C)")
    print(f" Duracao    : {'continua (Ctrl+C para parar)' if duration <= 0 else f'{duration:.0f}s'}")
    print("=" * 78)

    async with httpx.AsyncClient(limits=limits) as client:
        await wait_for_api(client, api_url)
        start_time = time.monotonic()

        tasks = [
            asyncio.create_task(
                truck_worker(truck, client, endpoint, interval, stop_event, metrics)
            )
            for truck in fleet
        ]
        reporter = asyncio.create_task(metrics_reporter(metrics, start_time, stop_event))

        if duration > 0:
            await asyncio.sleep(duration)
            stop_event.set()
        else:
            await stop_event.wait()

        await asyncio.gather(*tasks, reporter, return_exceptions=True)

    total_elapsed = max(0.001, time.monotonic() - start_time)
    avg_ms = (metrics.total_latency_ms / metrics.sent) if metrics.sent else 0.0
    print("-" * 78)
    print(
        f"[RESUMO FINAL] duracao={total_elapsed:.1f}s | enviados={metrics.sent} | "
        f"vazao={metrics.sent / total_elapsed:.1f} ops/s | violacoes={metrics.violations} | "
        f"erros={metrics.errors} | latencia_media={avg_ms:.1f} ms"
    )
    print("-" * 78)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Simulador concorrente de telemetria IoT para o projeto PulseFleet."
    )
    parser.add_argument(
        "--url",
        default=os.getenv("API_URL", "http://localhost:8000"),
        help="URL base da API PulseFleet (padrao: http://localhost:8000)",
    )
    parser.add_argument(
        "--vehicles",
        type=int,
        default=int(os.getenv("SIM_VEHICLES", "25")),
        help="Quantidade de caminhoes simulados entre 10 e 100 (padrao: 25)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.getenv("SIM_INTERVAL", "1.5")),
        help="Intervalo medio em segundos entre leituras por veiculo (padrao: 1.5)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=float(os.getenv("SIM_DURATION", "0")),
        help="Duracao em segundos (0 = executa continuamente ate Ctrl+C)",
    )
    args = parser.parse_args()
    if not (10 <= args.vehicles <= 100):
        parser.error("--vehicles deve estar entre 10 e 100.")
    if args.interval <= 0:
        parser.error("--interval deve ser maior que zero.")
    return args


if __name__ == "__main__":
    cli_args = parse_args()
    try:
        asyncio.run(
            run_simulation(
                api_url=cli_args.url,
                num_vehicles=cli_args.vehicles,
                interval=cli_args.interval,
                duration=cli_args.duration,
            )
        )
    except KeyboardInterrupt:
        print("\n[SIMULADOR] Encerrado pelo usuario.")
