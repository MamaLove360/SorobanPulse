-- Incident correlation engine: incidents table
-- See .kiro/specs/incident-correlation/requirements.md (#1263)

CREATE TABLE IF NOT EXISTS incidents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kind TEXT NOT NULL,
    severity TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    title TEXT NOT NULL,
    signals JSONB NOT NULL DEFAULT '[]'::jsonb,
    opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index used for deduplication lookups: find recent incidents of a given kind.
CREATE INDEX IF NOT EXISTS idx_incidents_kind_opened_at
    ON incidents (kind, opened_at DESC);

-- Index for filtering by status (e.g. open incidents in the admin API).
CREATE INDEX IF NOT EXISTS idx_incidents_status_opened_at
    ON incidents (status, opened_at DESC);