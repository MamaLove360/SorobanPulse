DROP INDEX IF EXISTS idx_contract_abis_onchain_fetched;
ALTER TABLE contract_abis
    DROP COLUMN IF EXISTS wasm_hash,
    DROP COLUMN IF EXISTS source;
