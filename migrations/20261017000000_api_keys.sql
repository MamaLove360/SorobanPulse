-- Issue #1258: scoped API keys stored hashed in the database.
--
-- Keys are generated as `sp_<base58(32 random bytes)>` and only their
-- SHA-256 hex digest is stored. Revocation is a soft delete (`revoked_at`)
-- so audit history is kept and a revoked key can never be re-activated.

CREATE TABLE IF NOT EXISTS api_keys (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name         TEXT NOT NULL,
    key_hash     TEXT NOT NULL UNIQUE,
    scopes       TEXT[] NOT NULL DEFAULT '{}',
    -- Optional tenant binding for multi-tenant deployments.
    tenant_id    TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMPTZ,
    revoked_at   TIMESTAMPTZ,

    CONSTRAINT api_keys_name_not_blank CHECK (length(btrim(name)) > 0),
    CONSTRAINT api_keys_key_hash_sha256 CHECK (key_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT api_keys_scopes_known CHECK (
        scopes <@ ARRAY['read:events', 'write:subscriptions', 'admin']::TEXT[]
    )
);

CREATE INDEX IF NOT EXISTS idx_api_keys_created_at ON api_keys (created_at DESC);
