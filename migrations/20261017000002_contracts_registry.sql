-- Issue #1255: contract deployment audit trail, populated by the indexer
-- from ContractCreated / ContractUpdated system events.
CREATE TABLE IF NOT EXISTS contracts (
    contract_id     TEXT PRIMARY KEY,
    deployer        TEXT,
    wasm_hash       TEXT,
    deployed_at     TIMESTAMPTZ,
    deployed_ledger BIGINT,
    deploy_tx_hash  TEXT,
    -- Time/ledger of the latest deployment event (creation or upgrade).
    updated_at      TIMESTAMPTZ NOT NULL,
    updated_ledger  BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_contracts_deployer ON contracts (deployer) WHERE deployer IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_contracts_wasm_hash ON contracts (wasm_hash);
