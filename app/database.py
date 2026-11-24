"""
PulseFleet (Marco 2) - Gerenciador Singleton de Conexoes Poliglotas.

Encapsula o ciclo de vida das duas camadas NoSQL definidas no Marco 1:
  1. Redis (Chave-Valor em memoria): pool de conexoes para leituras/escritas O(1).
  2. ScyllaDB / Apache Cassandra (Wide-Column / LSM-Tree): sessao CQL com
     Prepared Statements e TokenAwarePolicy (Hash Consistente / Murmur3) para
     rotear cada escrita diretamente ao no responsavel pela Partition Key
     composta (vehicle_id, year_month).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Optional

import redis
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, PreparedStatement, Session
from cassandra.policies import (
    DCAwareRoundRobinPolicy,
    ExponentialReconnectionPolicy,
    TokenAwarePolicy,
)

logger = logging.getLogger("pulsefleet.database")


class DatabaseManager:
    """Singleton responsavel pelas conexoes com Redis e ScyllaDB/Cassandra."""

    _instance: Optional["DatabaseManager"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "DatabaseManager":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return

        # Configuracoes via variaveis de ambiente (12-Factor App)
        self.redis_host: str = os.getenv("REDIS_HOST", "localhost")
        self.redis_port: int = int(os.getenv("REDIS_PORT", "6379"))
        self.cassandra_hosts: list[str] = [
            h.strip()
            for h in os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
            if h.strip()
        ]
        self.cassandra_port: int = int(os.getenv("CASSANDRA_PORT", "9042"))
        self.cassandra_keyspace: str = os.getenv("CASSANDRA_KEYSPACE", "pulsefleet")
        self.max_retries: int = int(os.getenv("DB_MAX_RETRIES", "20"))
        self.retry_delay_seconds: float = float(
            os.getenv("DB_RETRY_DELAY_SECONDS", "3")
        )

        # Recursos ativos
        self.redis_pool: Optional[redis.ConnectionPool] = None
        self.redis_client: Optional[redis.Redis] = None
        self.cassandra_cluster: Optional[Cluster] = None
        self.cassandra_session: Optional[Session] = None

        # Prepared Statements CQL (evitam re-parse por pacote e ativam Token-Aware Routing)
        self.insert_telemetry_stmt: Optional[PreparedStatement] = None
        self.select_history_stmt: Optional[PreparedStatement] = None

        self._initialized = True

    def connect(self) -> None:
        """Inicializa conexoes com Redis e ScyllaDB usando loop de retentativas."""
        self._connect_redis_with_retry()
        self._connect_cassandra_with_retry()

    def _connect_redis_with_retry(self) -> None:
        """Conecta ao Redis com pool reutilizavel e backoff em caso de boot lento."""
        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                pool = redis.ConnectionPool(
                    host=self.redis_host,
                    port=self.redis_port,
                    db=0,
                    max_connections=100,
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_timeout=5,
                    health_check_interval=30,
                )
                client = redis.Redis(connection_pool=pool)
                if client.ping():
                    self.redis_pool = pool
                    self.redis_client = client
                    logger.info(
                        "Redis conectado com sucesso em %s:%s (tentativa %d/%d).",
                        self.redis_host,
                        self.redis_port,
                        attempt,
                        self.max_retries,
                    )
                    return
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                logger.warning(
                    "Aguardando Redis (%s:%s) - tentativa %d/%d falhou: %s",
                    self.redis_host,
                    self.redis_port,
                    attempt,
                    self.max_retries,
                    exc,
                )
                time.sleep(self.retry_delay_seconds)

        raise RuntimeError(
            f"Nao foi possivel conectar ao Redis apos {self.max_retries} tentativas."
        ) from last_error

    def _connect_cassandra_with_retry(self) -> None:
        """
        Conecta ao ScyllaDB/Cassandra com tolerancia ao tempo de inicializacao.

        A politica TokenAwarePolicy + DCAwareRoundRobinPolicy implementa na
        camada cliente o roteamento por Hash Consistente (Murmur3Partitioner),
        enviando cada escrita diretamente ao no que detem o token de
        (vehicle_id, year_month).
        """
        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            cluster: Optional[Cluster] = None
            try:
                load_balancing = TokenAwarePolicy(DCAwareRoundRobinPolicy())
                reconnection = ExponentialReconnectionPolicy(
                    base_delay=1.0, max_delay=30.0
                )
                cluster = Cluster(
                    contact_points=self.cassandra_hosts,
                    port=self.cassandra_port,
                    load_balancing_policy=load_balancing,
                    reconnection_policy=reconnection,
                    protocol_version=4,
                    connect_timeout=10,
                )
                session = cluster.connect()
                self._ensure_schema(session)
                session.set_keyspace(self.cassandra_keyspace)

                self.cassandra_cluster = cluster
                self.cassandra_session = session
                self._prepare_statements()

                logger.info(
                    "ScyllaDB/Cassandra conectado em %s:%s (keyspace='%s', tentativa %d/%d).",
                    ",".join(self.cassandra_hosts),
                    self.cassandra_port,
                    self.cassandra_keyspace,
                    attempt,
                    self.max_retries,
                )
                return
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if cluster is not None:
                    try:
                        cluster.shutdown()
                    except Exception:  # noqa: BLE001
                        pass
                logger.warning(
                    "Aguardando ScyllaDB/Cassandra (%s:%s) - tentativa %d/%d falhou: %s",
                    ",".join(self.cassandra_hosts),
                    self.cassandra_port,
                    attempt,
                    self.max_retries,
                    exc,
                )
                time.sleep(self.retry_delay_seconds)

        raise RuntimeError(
            f"Nao foi possivel conectar ao ScyllaDB/Cassandra apos {self.max_retries} tentativas."
        ) from last_error

    def _ensure_schema(self, session: Session) -> None:
        """
        Garante de forma idempotente que o keyspace e a tabela existam.
        Atua como rede de seguranca alem do init-scripts/init-cassandra.cql.
        """
        session.execute(
            f"""
            CREATE KEYSPACE IF NOT EXISTS {self.cassandra_keyspace}
            WITH replication = {{
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }}
            AND durable_writes = true
            """
        )

        session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.cassandra_keyspace}.vehicle_telemetry (
                vehicle_id  text,
                year_month  text,
                timestamp   timestamp,
                latitude    double,
                longitude   double,
                speed       double,
                temperature double,
                violation   boolean,
                PRIMARY KEY ((vehicle_id, year_month), timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC)
              AND compaction = {{
                  'class': 'TimeWindowCompactionStrategy',
                  'compaction_window_unit': 'DAYS',
                  'compaction_window_size': 60
              }}
              AND default_time_to_live = 157680000
            """
        )

    def _prepare_statements(self) -> None:
        """
        Compila os Prepared Statements uma unica vez na inicializacao.

        - Em ambiente local (RF=1), usamos ConsistencyLevel.ONE.
        - Em producao (RF=3 com NetworkTopologyStrategy), recomenda-se
          ConsistencyLevel.LOCAL_QUORUM (W + R > N) para equilibrar latencia
          e consistencia forte em auditorias regulatorias (CAP/PACELC).
        """
        assert self.cassandra_session is not None

        # Insercao append-only em LSM-Tree (CommitLog sequencial + Memtable -> SSTable)
        self.insert_telemetry_stmt = self.cassandra_session.prepare(
            """
            INSERT INTO vehicle_telemetry (
                vehicle_id,
                year_month,
                timestamp,
                latitude,
                longitude,
                speed,
                temperature,
                violation
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
        )
        self.insert_telemetry_stmt.consistency_level = ConsistencyLevel.ONE

        # Consulta analitica estritamente ancorada na Partition Key completa
        # (vehicle_id, year_month), lendo em ordem fisica DESC sem ALLOW FILTERING.
        self.select_history_stmt = self.cassandra_session.prepare(
            """
            SELECT
                vehicle_id,
                year_month,
                timestamp,
                latitude,
                longitude,
                speed,
                temperature,
                violation
            FROM vehicle_telemetry
            WHERE vehicle_id = ? AND year_month = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """
        )
        self.select_history_stmt.consistency_level = ConsistencyLevel.ONE

    def check_health(self) -> dict[str, object]:
        """Verifica conectividade real de ambas as camadas de persistencia."""
        redis_ok = False
        redis_error: Optional[str] = None
        try:
            if self.redis_client is not None:
                redis_ok = bool(self.redis_client.ping())
        except Exception as exc:  # noqa: BLE001
            redis_error = str(exc)

        cassandra_ok = False
        cassandra_version: Optional[str] = None
        cassandra_error: Optional[str] = None
        try:
            if self.cassandra_session is not None:
                row = self.cassandra_session.execute(
                    "SELECT release_version FROM system.local"
                ).one()
                cassandra_ok = row is not None
                if row is not None:
                    cassandra_version = row.release_version
        except Exception as exc:  # noqa: BLE001
            cassandra_error = str(exc)

        return {
            "status": "healthy" if (redis_ok and cassandra_ok) else "degraded",
            "redis": {
                "connected": redis_ok,
                "host": f"{self.redis_host}:{self.redis_port}",
                "error": redis_error,
            },
            "scylla_cassandra": {
                "connected": cassandra_ok,
                "hosts": self.cassandra_hosts,
                "keyspace": self.cassandra_keyspace,
                "release_version": cassandra_version,
                "error": cassandra_error,
            },
        }

    def close(self) -> None:
        """Libera graciosamente conexoes e pools no encerramento da aplicacao."""
        if self.redis_client is not None:
            try:
                self.redis_client.close()
            except Exception:  # noqa: BLE001
                pass
            self.redis_client = None

        if self.redis_pool is not None:
            try:
                self.redis_pool.disconnect()
            except Exception:  # noqa: BLE001
                pass
            self.redis_pool = None

        if self.cassandra_cluster is not None:
            try:
                self.cassandra_cluster.shutdown()
            except Exception:  # noqa: BLE001
                pass
            self.cassandra_cluster = None
            self.cassandra_session = None

        logger.info("Conexoes com Redis e ScyllaDB/Cassandra encerradas.")


# Instancia Singleton compartilhada por toda a aplicacao FastAPI
db = DatabaseManager()
