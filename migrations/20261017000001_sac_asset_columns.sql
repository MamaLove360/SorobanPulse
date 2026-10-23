-- Issue #1254: underlying classic asset for Stellar Asset Contract events.
-- NULL for every event not emitted by a SAC. Native XLM is asset_code
-- 'native' with a NULL issuer.
ALTER TABLE events
    ADD COLUMN IF NOT EXISTS asset_code   TEXT,
    ADD COLUMN IF NOT EXISTS asset_issuer TEXT;

CREATE INDEX IF NOT EXISTS idx_events_asset
    ON events (asset_code, asset_issuer)
    WHERE asset_code IS NOT NULL;
