-- Issue #1253: distinguish specs fetched from on-chain WASM from manually
-- registered ABIs (which auto-fetch must never overwrite), and remember the
-- WASM hash an on-chain spec came from so the TTL refresh can spot upgrades.
ALTER TABLE contract_abis
    ADD COLUMN IF NOT EXISTS source    TEXT NOT NULL DEFAULT 'registered',
    ADD COLUMN IF NOT EXISTS wasm_hash TEXT;

CREATE INDEX IF NOT EXISTS idx_contract_abis_onchain_fetched
    ON contract_abis (fetched_at)
    WHERE source = 'onchain';

-- Earlier auto-fetch builds stored the spec as an opaque base64 blob; drop
-- those rows so they are re-fetched in the parsed format.
DELETE FROM contract_spec_cache WHERE spec @> '[{"name": "contractspecv0"}]';
DELETE FROM contract_abis WHERE abi @> '[{"name": "contractspecv0"}]';
