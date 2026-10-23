-- Issue #1251: typed SEP-41 token event columns, populated by the indexer
-- (and `POST /v1/admin/sep41/backfill` for already-indexed rows).
ALTER TABLE events ADD COLUMN IF NOT EXISTS sep41_event_type TEXT;
ALTER TABLE events ADD COLUMN IF NOT EXISTS sep41_from TEXT;
ALTER TABLE events ADD COLUMN IF NOT EXISTS sep41_to TEXT;
ALTER TABLE events ADD COLUMN IF NOT EXISTS sep41_amount NUMERIC(40, 0);

-- Partial indexes: only SEP-41 rows are indexed. (ledger DESC, id DESC)
-- matches the keyset pagination of GET /v1/events/sep41.
CREATE INDEX IF NOT EXISTS idx_events_sep41_from
    ON events (sep41_from, ledger DESC, id DESC) WHERE sep41_event_type IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_sep41_to
    ON events (sep41_to, ledger DESC, id DESC) WHERE sep41_event_type IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_sep41_ledger
    ON events (ledger DESC, id DESC) WHERE sep41_event_type IS NOT NULL;
