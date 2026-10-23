# Configuration Management — Issue #997

> **Warning:** Changing security-sensitive settings (`API_KEY`, `ADMIN_API_KEY`,
> `WEBHOOK_SECRET`, `EVENT_DATA_ENCRYPTION_KEY`) in a running system requires a
> coordinated rolling restart to avoid authentication gaps.

## Overview

SorobanPulse configuration is unified in `src/config.rs` (the `Config` struct)
with validation collected in `src/config_validation.rs`. There are three input
sources, merged in the following priority order (highest wins):

```
Environment variables  >  CONFIG_FILE (TOML)  >  compiled-in defaults
```

---

## Input Sources

### Environment Variables

The primary and recommended source for all settings, especially secrets. See
the full reference table below.

### TOML Config File

Set `CONFIG_FILE=/path/to/config.toml` to load settings from a TOML file. Any
key present in the TOML file is used when the corresponding environment variable
is absent. Example:

```toml
# config.toml
DATABASE_URL = "postgres://user:pass@localhost/soroban_pulse"
DB_MAX_CONNECTIONS = "20"
STELLAR_RPC_URL = "https://soroban-testnet.stellar.org"
RUST_LOG = "info"
```

### Compiled-in Defaults

Every field in `Config` has a `Default` implementation. The defaults are
designed to be safe for development (auth disabled, lenient timeouts) but carry
warnings in production-like environments (see Validation below).

---

## Validation

Issue #997 introduces a unified validation pass in `src/config_validation.rs`;
issue #1247 makes `ConfigBuilder::build()` (in `src/config/builder.rs`) the
only place it runs. Parse errors collected while reading the environment and
every semantic check are reported together in one typed `ConfigError`, so
operators see a complete list rather than discovering problems one at a time.

```
[startup]
    │
    ├─ ConfigBuilder::from_env()        (env + file + defaults; parse errors collected)
    │
    ├─ .build()?                        (config_validation::validate, once)
    │   ├─ any error  → Err(ConfigError) → main exits non-zero
    │   └─ warnings   → log WARN, continue
    │
    └─ Connect to DB, run migrations, start indexer, ...
```

Configs can also be assembled in code. The database and RPC sections are
required at compile time — `build()` does not exist until both are supplied:

```rust
let config = ConfigBuilder::new()
    .with_database(DatabaseSettings::new("postgres://localhost/soroban_pulse"))
    .with_rpc(RpcSettings::new("https://soroban-testnet.stellar.org"))
    .with_server(ServerSettings { port: 8080, ..ServerSettings::default() })
    .build()?;
```

### Error vs Warning

| Severity | Examples | Effect |
|----------|---------|--------|
| **Error** (fatal) | Empty DATABASE_URL, min_connections > max_connections, missing TLS key file | Service refuses to start |
| **Warning** (advisory) | HTTP webhook URL, no API key in production, very large pool | Logged at WARN; service starts |

### Validation Checks

| Check | Category | Severity |
|-------|----------|----------|
| `DATABASE_URL` non-empty and starts with `postgres://` | database | Error |
| `DB_MIN_CONNECTIONS ≤ DB_MAX_CONNECTIONS` | database | Error |
| `DB_MAX_LIFETIME_SECS ≥ DB_IDLE_TIMEOUT_SECS` | database | Error |
| `STELLAR_RPC_URL` is a valid http/https URL | rpc | Error |
| `BLOOM_FILTER_FP_RATE` in (0, 1) | dedup | Error |
| `BLOOM_FILTER_CAPACITY > 0` | dedup | Error |
| TLS cert and key must both be present or both absent | tls | Error |
| WEBHOOK_URL using HTTP with REQUIRE_HTTPS=true | webhook | Error |
| `DB_STATEMENT_TIMEOUT_MS = 0` (disables timeouts) | database | Warning |
| RPC URL using plain HTTP | rpc | Warning |
| No API_KEY in production-like env | auth | Warning |
| Webhook URL without HMAC secret | webhook | Warning |
| Bloom filter memory estimated > 512 MB | dedup | Warning |
| `RATE_LIMIT_PER_MINUTE = 0` in production | rate-limit | Warning |

---

## Hot Reload

Most configuration takes effect only on restart. The exceptions are:

| Setting | Mechanism |
|---------|-----------|
| `AdaptivePoolConfig` fields | `watch::Sender<AdaptivePoolConfig>` in `src/db_pool/adaptive.rs` — send a new config to the channel |
| Feature flags | `GET/PUT /v1/admin/feature-flags` API — takes effect immediately |
| Indexer pause/resume | `POST /v1/admin/indexer/pause` and `/resume` |

---

## Configuration Reference

The following table is generated from `config_validation::config_schema()`.

### Database

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | **Yes** | — | PostgreSQL connection string |
| `DATABASE_REPLICA_URL` | No | — | Read-replica URL; HTTP handlers use this pool |
| `DB_MAX_CONNECTIONS` | No | `10` | Maximum pool connections |
| `DB_MIN_CONNECTIONS` | No | `1` | Minimum idle connections |
| `DB_IDLE_TIMEOUT_SECS` | No | `600` | Idle connection recycle interval |
| `DB_MAX_LIFETIME_SECS` | No | `1800` | Maximum connection age |
| `DB_STATEMENT_TIMEOUT_MS` | No | `5000` | Per-query timeout. 0 disables |

### RPC

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `STELLAR_RPC_URL` | No | `https://soroban-testnet.stellar.org` | Soroban RPC endpoint |
| `STELLAR_RPC_FALLBACK_URLS` | No | — | Comma-separated fallback URLs |
| `RPC_CONNECT_TIMEOUT_SECS` | No | `5` | TCP connect timeout |
| `RPC_REQUEST_TIMEOUT_SECS` | No | `30` | Full request timeout |

### Authentication

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `API_KEY` | No | — | Bearer token for all endpoints. Disabled when unset |
| `ADMIN_API_KEY` | No | — | Bearer token for `/v1/admin/*` endpoints |
| `ADMIN_API_KEY_SECONDARY` | No | — | Secondary admin key for zero-downtime rotation |

### Server

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `PORT` | No | `3000` | HTTP listen port |
| `RATE_LIMIT_PER_MINUTE` | No | `60` | Max requests per IP per minute. 0 = unlimited |
| `RUST_LOG` | No | `info` | Log verbosity (trace/debug/info/warn/error) |
| `RUST_LOG_FORMAT` | No | `text` | Log format: `text` or `json` |

### Indexer

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `START_LEDGER` | No | `0` | Starting ledger (0 = latest) |
| `INDEXER_LAG_WARN_THRESHOLD` | No | `100` | Ledgers of lag before warning |
| `INDEXER_LOCK_RETRY_SECS` | No | `30` | How often standbys retry advisory lock |

### Event Deduplication

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `BLOOM_FILTER_CAPACITY` | No | `1000000` | Max events before bloom filter rotates (Issue #996) |
| `BLOOM_FILTER_FP_RATE` | No | `0.001` | Target false-positive rate |

### SSE

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SSE_KEEPALIVE_SECS` | No | `15` | Keep-alive ping interval |
| `SSE_REPLAY_MAX_EVENTS` | No | `1000` | Reconnect ring buffer capacity |

### Webhook

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `WEBHOOK_URL` | No | — | Destination for webhook notifications |
| `WEBHOOK_SECRET` | No | — | HMAC-SHA256 signing secret |
| `WEBHOOK_REQUIRE_HTTPS` | No | `false` | Reject plain-HTTP webhook URLs |

### Observability

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SLOW_QUERY_THRESHOLD_MS` | No | `1000` | Queries above this are logged at WARN |
| `HEALTH_CHECK_TIMEOUT_MS` | No | `2000` | Timeout for /health DB ping |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | No | `http://localhost:4317` | OpenTelemetry collector (otel feature) |

### Retention

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `RETENTION_DAYS` | No | `90` | Days to keep events |
| `PRUNING_INTERVAL_HOURS` | No | `24` | Pruner task interval |

For the complete list of all variables (including email, SMS, multi-tenancy,
ML, SaaS), see `.env.example` which is the canonical reference.

---

## Secrets Management

Secrets (API keys, DB passwords, HMAC secrets, encryption keys) should be
provided via environment variables, never committed to files in the repository.

In production, use a secrets manager:

| Platform | Recommendation |
|----------|---------------|
| Kubernetes | `Secret` objects mounted as env vars or files |
| AWS ECS | AWS Secrets Manager via `secrets` in the task definition |
| Docker Compose | `.env` file outside the repository, or Docker secrets |
| Bare metal | systemd `EnvironmentFile` pointing to a file with `0600` permissions |

The service uses `secrecy::SecretString` for all key fields to prevent
accidental logging.

---

## Configuration Schema Generator

The `config_validation::config_schema()` function returns a structured
`Vec<ConfigFieldDoc>` that can be serialized to JSON for tooling:

```bash
# Dump the schema as JSON (requires the schema-cli binary)
cargo run --bin schema_cli -- dump-config-schema | jq .
```

This powers the documentation auto-generation pipeline. Keep
`config_schema()` in sync with any new fields added to `Config`.

---

## Writing Config Tests

Add tests to `src/config_validation.rs` (under `#[cfg(test)]`). Use
`Config::default()` as a base and mutate only the fields under test:

```rust
#[test]
fn my_new_check() {
    let mut cfg = Config::default();
    cfg.some_field = invalid_value;
    let report = validate(&cfg);
    assert!(!report.is_ok());
    assert!(report.errors.iter().any(|e| e.contains("SOME_FIELD")));
}
```

---

## References

- `src/config.rs` — `Config` struct and `from_env()` loader
- `src/config_validation.rs` — validation logic (Issue #997)
- `.env.example` — canonical variable reference
- `config.toml.example` — TOML config file example
- `docs/onboarding.md` — first-time setup guide
- `docs/development-setup.md` — IDE and dev environment setup
