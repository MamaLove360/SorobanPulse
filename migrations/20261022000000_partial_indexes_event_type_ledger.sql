-- Issue #1343: Partial indexes for common filter combinations.
--
-- The hottest read paths (see docs/statistics-auto-analysis.md, "Top query
-- patterns") filter on event_type = 'contract' / 'system' and/or a ledger
-- range, and all of them sort by (ledger DESC, id DESC). These partial
-- indexes match that sort order exactly, so the planner can stream rows
-- straight from the index and stop at LIMIT instead of sorting.
--
-- `events` is range-partitioned (20260727000102), and CREATE INDEX
-- CONCURRENTLY is not supported on partitioned tables, so this migration runs
-- in a transaction like the other index migrations on this table.

-- 1. Contract events (the dominant event type).
--    Supersedes idx_events_contract_type_partial (ledger DESC), which could
--    not satisfy the id DESC tie-breaker without an extra sort step.
CREATE INDEX IF NOT EXISTS idx_events_contract_ledger_id_partial
    ON events (ledger DESC, id DESC)
    WHERE event_type = 'contract';

DROP INDEX IF EXISTS idx_events_contract_type_partial;

-- 2. System events (rare, so this index stays very small).
CREATE INDEX IF NOT EXISTS idx_events_system_ledger_id_partial
    ON events (ledger DESC, id DESC)
    WHERE event_type = 'system';

-- 3. Recent-ledger window.
--    Partial index predicates must be constants, so the threshold is
--    computed once from the current data (MAX(ledger) minus ~30 days of
--    ledgers at ~5 s/ledger) and baked into the index definition. Call
--    refresh_recent_ledger_index() periodically to slide the window forward.
CREATE OR REPLACE FUNCTION refresh_recent_ledger_index(window_ledgers BIGINT DEFAULT 518400)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    threshold BIGINT;
BEGIN
    SELECT GREATEST(COALESCE(MAX(ledger), 0) - window_ledgers, 0)
      INTO threshold
      FROM events;

    -- Build the replacement first so readers always have an index to use,
    -- then swap names. Everything runs in the caller's transaction.
    DROP INDEX IF EXISTS idx_events_recent_ledger_id_new;
    EXECUTE format(
        'CREATE INDEX idx_events_recent_ledger_id_new ON events (ledger DESC, id DESC) WHERE ledger > %s',
        threshold
    );
    DROP INDEX IF EXISTS idx_events_recent_ledger_id;
    ALTER INDEX idx_events_recent_ledger_id_new RENAME TO idx_events_recent_ledger_id;
    EXECUTE format(
        'COMMENT ON INDEX idx_events_recent_ledger_id IS %L',
        'Recent ledger window (ledger > ' || threshold || '): GET /v1/events?from_ledger=..&to_ledger=.. ORDER BY ledger DESC, id DESC. '
        || 'Rebuilt by refresh_recent_ledger_index().'
    );

    RETURN threshold;
END;
$$;

COMMENT ON FUNCTION refresh_recent_ledger_index(BIGINT) IS
    'Issue #1343: rebuild idx_events_recent_ledger_id with threshold MAX(ledger) - window_ledgers. Returns the new threshold.';

SELECT refresh_recent_ledger_index();

COMMENT ON INDEX idx_events_contract_ledger_id_partial IS
    'Hot-path partial index: event_type = contract, ORDER BY ledger DESC, id DESC (GET /v1/events?event_type=contract)';
COMMENT ON INDEX idx_events_system_ledger_id_partial IS
    'Partial index: event_type = system, ORDER BY ledger DESC, id DESC (GET /v1/events?event_type=system)';
