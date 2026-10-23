-- Revert Issue #1343: partial indexes for common filter combinations.
DROP INDEX IF EXISTS idx_events_recent_ledger_id;
DROP INDEX IF EXISTS idx_events_recent_ledger_id_new;
DROP FUNCTION IF EXISTS refresh_recent_ledger_index(BIGINT);
DROP INDEX IF EXISTS idx_events_system_ledger_id_partial;
DROP INDEX IF EXISTS idx_events_contract_ledger_id_partial;

CREATE INDEX IF NOT EXISTS idx_events_contract_type_partial
    ON events (ledger DESC)
    WHERE event_type = 'contract';
COMMENT ON INDEX idx_events_contract_type_partial IS
    'Hot-path partial index for contract events (event_type = contract) ordered by ledger';
