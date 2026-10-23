-- Issue #1257: per-contract resource and fee statistics.
--
-- Nullable per-event resource usage, filled in from the transaction meta of
-- the transaction that emitted the event (see src/resources.rs). Every event
-- of a transaction carries the same values, so aggregates de-duplicate by
-- (contract_id, tx_hash) before summing.
--
--   resource_fee  charged Soroban resource fee in stroops (declared fee when
--                 the meta does not report the charged amount)
--   cpu_insns     CPU instructions consumed (declared limit as a fallback)
--   mem_bytes     memory bytes consumed; NULL when the RPC does not emit
--                 core_metrics diagnostic events

ALTER TABLE events ADD COLUMN IF NOT EXISTS resource_fee BIGINT;
ALTER TABLE events ADD COLUMN IF NOT EXISTS cpu_insns    BIGINT;
ALTER TABLE events ADD COLUMN IF NOT EXISTS mem_bytes    BIGINT;

CREATE INDEX IF NOT EXISTS idx_events_resource_stats
    ON events (contract_id, tx_hash)
    WHERE resource_fee IS NOT NULL OR cpu_insns IS NOT NULL OR mem_bytes IS NOT NULL;

-- Refreshed hourly by `contract_resource_stats::spawn_refresh`.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_contract_resource_stats AS
WITH tx AS (
    SELECT DISTINCT ON (contract_id, tx_hash)
           contract_id, tx_hash, ledger, resource_fee, cpu_insns, mem_bytes
    FROM events
    WHERE resource_fee IS NOT NULL OR cpu_insns IS NOT NULL OR mem_bytes IS NOT NULL
    ORDER BY contract_id, tx_hash, ledger DESC
)
SELECT contract_id,
       COUNT(*)::BIGINT                                                  AS tx_count,
       MIN(ledger)                                                       AS first_ledger,
       MAX(ledger)                                                       AS last_ledger,
       SUM(resource_fee)::BIGINT                                         AS total_resource_fee,
       AVG(resource_fee)::DOUBLE PRECISION                               AS avg_resource_fee,
       percentile_cont(0.5)  WITHIN GROUP (ORDER BY resource_fee)        AS p50_resource_fee,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY resource_fee)        AS p95_resource_fee,
       MAX(resource_fee)                                                 AS max_resource_fee,
       SUM(cpu_insns)::BIGINT                                            AS total_cpu_insns,
       AVG(cpu_insns)::DOUBLE PRECISION                                  AS avg_cpu_insns,
       percentile_cont(0.5)  WITHIN GROUP (ORDER BY cpu_insns)           AS p50_cpu_insns,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY cpu_insns)           AS p95_cpu_insns,
       MAX(cpu_insns)                                                    AS max_cpu_insns,
       SUM(mem_bytes)::BIGINT                                            AS total_mem_bytes,
       AVG(mem_bytes)::DOUBLE PRECISION                                  AS avg_mem_bytes,
       percentile_cont(0.5)  WITHIN GROUP (ORDER BY mem_bytes)           AS p50_mem_bytes,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY mem_bytes)           AS p95_mem_bytes,
       MAX(mem_bytes)                                                    AS max_mem_bytes,
       NOW()                                                             AS refreshed_at
FROM tx
GROUP BY contract_id;

-- Required for REFRESH MATERIALIZED VIEW CONCURRENTLY.
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_contract_resource_stats
    ON mv_contract_resource_stats (contract_id);
