-- Reverts 20261017000001_event_resource_stats.sql.
DROP MATERIALIZED VIEW IF EXISTS mv_contract_resource_stats;
DROP INDEX IF EXISTS idx_events_resource_stats;
ALTER TABLE events DROP COLUMN IF EXISTS mem_bytes;
ALTER TABLE events DROP COLUMN IF EXISTS cpu_insns;
ALTER TABLE events DROP COLUMN IF EXISTS resource_fee;
