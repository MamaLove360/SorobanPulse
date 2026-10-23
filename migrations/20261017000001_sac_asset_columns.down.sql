DROP INDEX IF EXISTS idx_events_asset;
ALTER TABLE events
    DROP COLUMN IF EXISTS asset_issuer,
    DROP COLUMN IF EXISTS asset_code;
