-- =============================================================================
-- PulseFleet (Marco 2) - DDL da Camada Wide-Column (ScyllaDB / Apache Cassandra)
-- =============================================================================
-- Fundamentos do Marco 1 refletidos neste schema:
--   1. Sem JOINs (Query-Driven Modeling): a tabela ja e materializada na exata
--      ordem exigida pelos relatorios regulatorios ANVISA/FDA.
--   2. Partition Key Composta ((vehicle_id, year_month)): aplica bucketing
--      temporal mensal para limitar o tamanho de cada particao (< 100 MB),
--      evitando "unbounded partitions" (~518.400 eventos/veiculo/mes a 1 msg/5s).
--   3. Hash Consistente (Murmur3Partitioner): distribui as combinacoes
--      (vehicle_id, year_month) uniformemente pelo anel de nos sem hotspots.
--   4. Clustering Key (timestamp DESC): grava fisicamente em ordem cronologica
--      decrescente na LSM-Tree (Memtable -> SSTable), permitindo leitura das N
--      leituras mais recentes via varredura sequencial sem ordenacao em memoria.
--   5. TimeWindowCompactionStrategy (TWCS): estrategia de compactacao ideal
--      para series temporais append-only; agrupa SSTables por janela de tempo
--      evitando write amplification excessivo.
-- =============================================================================

-- Ambiente local / demonstracao: SimpleStrategy com Replication Factor = 1.
-- Em producao (multi-AZ / 3 racks), substituir por:
--   CREATE KEYSPACE IF NOT EXISTS pulsefleet
--   WITH replication = {
--       'class': 'NetworkTopologyStrategy',
--       'dc_sa_east_1': 3
--   } AND durable_writes = true;
CREATE KEYSPACE IF NOT EXISTS pulsefleet
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
}
AND durable_writes = true;

USE pulsefleet;

CREATE TABLE IF NOT EXISTS pulsefleet.vehicle_telemetry (
    vehicle_id  text,
    year_month  text,          -- Bucket temporal 'YYYY-MM' (ex: '2026-11') para evitar particoes gigantes (>100MB)
    timestamp   timestamp,     -- Clustering key: ordenacao fisica decrescente dentro da particao
    latitude    double,
    longitude   double,
    speed       double,
    temperature double,
    violation   boolean,       -- true se temperature < 2.0 C ou temperature > 8.0 C (cadeia fria farmaceutica)
    PRIMARY KEY ((vehicle_id, year_month), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  -- ~31 janelas para TTL de 5 anos (respeita limite twcs_max_window_count=50 do ScyllaDB)
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': 60
  }
  AND default_time_to_live = 157680000  -- Retencao regulatoria padrao de 5 anos (5 * 365 * 86400s)
  AND comment = 'Serie temporal imutavel (append-only) de telemetria IoT - Auditoria ANVISA/FDA';
