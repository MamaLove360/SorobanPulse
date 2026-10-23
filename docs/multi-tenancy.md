# Multi-Tenant Access Isolation - Issue #887

## Overview

SorobanPulse supports comprehensive multi-tenant support with row-level security (RLS) policies and strong tenant isolation guarantees. Each tenant has isolated access to their event data through both application-level and database-level controls.

## Architecture

### Multi-Tenant Model

- **Default Tenant**: All existing data assigned to "default" tenant for backward compatibility
- **Tenant Context**: Extracted from request headers (`x-tenant-id`)
- **RLS Policies**: PostgreSQL Row-Level Security enforces data isolation at the database level
- **Tenant Provisioning**: Dynamic tenant creation and configuration via API

## Components

### Models

```rust
pub struct Event {
    // ... existing fields
    pub tenant_id: String,  // Tenant ownership
}

pub struct PaginationParams {
    pub tenant_id: Option<String>,  // Filter by tenant
    // ... existing fields
}
```

### Tenant Context

```rust
pub struct TenantContext {
    pub tenant_id: TenantId,
    pub user_id: Option<String>,
    pub request_id: String,
}
```

### Tenant Provider

```rust
pub trait TenantProvider: Send + Sync {
    fn get_tenant(&self, tenant_id: &str) -> Option<TenantProvisioning>;
    fn provision_tenant(&self, provisioning: TenantProvisioning) -> Result<(), String>;
    fn list_tenants(&self) -> Vec<TenantProvisioning>;
}
```

## HTTP Headers

All requests should include tenant context:

```http
GET /events?limit=10
X-Tenant-ID: acme-corp
X-User-ID: user@acme-corp.com
```

## Middleware Integration

The tenant middleware automatically extracts tenant context:

```rust
use axum::{
    middleware,
    Router,
};
use soroban_pulse::middleware::tenant_context_middleware;

let app = Router::new()
    .layer(middleware::from_fn(tenant_context_middleware));
```

## Database Schema

### Tenant Tables

```sql
-- Tenant metadata
CREATE TABLE tenants (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    admin_email TEXT,
    config JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    enabled BOOLEAN DEFAULT TRUE
);

-- RLS policy definitions
CREATE TABLE tenant_rls_policies (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL UNIQUE,
    policy_json JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

### Row-Level Security (RLS) setup

RLS is a **second layer of defence**. The primary control is the
application-level `tenant_id` filter that handlers add to every query. RLS
makes sure that a bug in that filter cannot leak another tenant's rows.

#### Migrations

| Migration | What it does |
|-----------|--------------|
| `20260527000001_rls_events.sql` | Original policy on the (then unpartitioned) `events` table. |
| `20260727000102_partition_events_by_month.sql` | Renamed that table to `events_legacy` and promoted the partitioned table to `events`. **The policy stayed on `events_legacy`, so the live table was unprotected.** |
| `20261022000001_rls_events_partitioned.sql` | Issue #1344: re-applies RLS to the partitioned `events` table, every existing partition and `events_legacy`, fixes the NULL/`''` scope handling, adds `WITH CHECK`, and makes `auto_create_partitions()` protect new partitions. |

#### Policy

`apply_events_partition_rls(regclass)` installs this policy on `events` and on each partition:

```sql
ALTER TABLE events ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON events
    USING (                                   -- which rows are visible (SELECT/UPDATE/DELETE)
        tenant_id IS NULL                                                   -- legacy single-tenant rows
        OR COALESCE(current_setting('app.current_tenant_id', TRUE), '') = '' -- unscoped session
        OR tenant_id = current_setting('app.current_tenant_id', TRUE)        -- own tenant
    )
    WITH CHECK (                              -- which rows may be written (INSERT/UPDATE)
        COALESCE(current_setting('app.current_tenant_id', TRUE), '') = ''
        OR tenant_id = current_setting('app.current_tenant_id', TRUE)
    );
```

| Session state | Can read | Can write |
|---------------|----------|-----------|
| `app.current_tenant_id = 'tenant-a'` | `tenant-a` rows (and legacy `NULL` rows) | only rows with `tenant_id = 'tenant-a'` |
| GUC never set (NULL) or set to `''` | all rows (unscoped: indexer, admin jobs) | any row |
| Superuser or `BYPASSRLS` role | all rows, whatever the scope | any row |
| Table owner | all rows, unless `FORCE ROW LEVEL SECURITY` is set | any row |

#### Scoping a request

Set the tenant **per transaction** with `SET LOCAL` semantics, so the scope
cannot leak to the next request that reuses a pooled connection:

```rust
let mut tx = pool.begin().await?;
sqlx::query("SELECT set_config('app.current_tenant_id', $1, true)") // true = transaction-local
    .bind(&tenant_id)
    .execute(&mut *tx)
    .await?;
let rows = sqlx::query_as::<_, Event>("SELECT * FROM events ORDER BY ledger DESC LIMIT 20")
    .fetch_all(&mut *tx)
    .await?;
tx.commit().await?;
```

#### Recommended roles

PostgreSQL skips RLS for superusers, for `BYPASSRLS` roles and for the
table owner. For the policy to apply, the API must connect as a role that is
none of these:

```sql
-- Owns the schema and runs migrations (bypasses RLS as owner).
CREATE ROLE soroban_pulse_owner LOGIN PASSWORD '...';

-- Used by the HTTP API. RLS applies.
CREATE ROLE soroban_pulse_app LOGIN PASSWORD '...' NOSUPERUSER NOBYPASSRLS;
GRANT USAGE ON SCHEMA public TO soroban_pulse_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO soroban_pulse_app;
ALTER DEFAULT PRIVILEGES FOR ROLE soroban_pulse_owner IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO soroban_pulse_app;
```

If one role must both own the tables and serve requests, add
`ALTER TABLE events FORCE ROW LEVEL SECURITY;`. Be aware that `pg_dump` run
as that role will then fail, so run backups as a superuser or a
`BYPASSRLS` role.

Admin operations (backups, support queries, re-indexing) run as a superuser
or `BYPASSRLS` role and always see every row.

#### Partitions

A policy on a partitioned parent table is not checked when someone queries a
partition directly (for example `SELECT * FROM events_2026_01`). That is why
every partition gets its own copy of the policy:

- Existing partitions are covered by migration `20261022000001`.
- Partitions created by `auto_create_partitions()` are covered automatically.
- Any partition you create by hand must be protected explicitly:

  ```sql
  CREATE TABLE events_2027_01 PARTITION OF events
      FOR VALUES FROM ('2027-01-01') TO ('2027-02-01');
  SELECT apply_events_partition_rls('events_2027_01');
  ```

Use this query to find unprotected partitions. It should return 0 rows:

```sql
SELECT c.relname
  FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
 WHERE i.inhparent = 'events'::regclass AND NOT c.relrowsecurity;
```

## Tenant Provisioning

### Creating a New Tenant

```rust
use soroban_pulse::multi_tenancy::TenantProvisioning;

let provisioning = TenantProvisioning::new(
    "acme-corp".to_string(),
    "ACME Corporation".to_string(),
)
.with_admin_email("admin@acme-corp.com".to_string());

provider.provision_tenant(provisioning)?;
```

### Tenant Configuration

```rust
use serde_json::json;

let provisioning = TenantProvisioning::new(
    "tenant-id".to_string(),
    "Tenant Name".to_string(),
)
.with_config(json!({
    "encryption_enabled": true,
    "audit_logging": true,
    "retention_days": 90
}));
```

## Access Control

### Default Tenant Access

Requests without `X-Tenant-ID` header default to "default" tenant:

```rust
let ctx = TenantContext::from_header(None);
assert_eq!(ctx.tenant_id.as_str(), "default");
```

### Query Filtering

All queries automatically filter by tenant_id:

```rust
// Returns only events from "acme-corp" tenant
GET /events?limit=10 HTTP/1.1
X-Tenant-ID: acme-corp
```

## Security Auditing

### Audit Logging

All cross-tenant access attempts are logged:

```sql
SELECT * FROM tenant_access_audit
WHERE tenant_id = 'acme-corp'
ORDER BY created_at DESC;
```

### RLS Violation Detection

The database prevents RLS violations at the storage layer:

```sql
-- This query will return no results if tenant_id doesn't match session
SELECT * FROM events
WHERE tenant_id = 'competitor-corp';
```

## API Endpoints

### List Tenants

```http
GET /api/admin/tenants
Authorization: Bearer <admin-token>
```

### Provision Tenant

```http
POST /api/admin/tenants
Authorization: Bearer <admin-token>
Content-Type: application/json

{
  "tenant_id": "new-tenant",
  "name": "New Tenant Inc",
  "admin_email": "admin@new-tenant.com"
}
```

### Inspect Tenant Config

```http
GET /api/admin/tenants/{tenant_id}
Authorization: Bearer <admin-token>
X-Tenant-ID: {tenant_id}
```

## Testing

### Unit Tests

```bash
cargo test multi_tenancy::tests
```

Tests cover:
- Tenant ID creation and validation
- Tenant context extraction from headers
- Tenant provisioning lifecycle
- Default tenant behavior

### Integration Tests

```bash
# Needs a PostgreSQL superuser, e.g. `docker compose -f docker-compose.test.yml up -d`
export DATABASE_URL=postgres://postgres:postgres@localhost/soroban_pulse_test
cargo test --test multi_tenant_isolation_tests
```

`tests/multi_tenant_isolation_tests.rs` covers auth-level tenant resolution
(tests 1–30) and, in the `rls` module, database-level isolation against a
real PostgreSQL instance (tests 31–37). Each RLS test runs in its own
database created by `#[sqlx::test]` with every migration applied, and
switches to a throwaway `NOSUPERUSER NOBYPASSRLS` role:

| Test | Verifies |
|------|----------|
| 31 | RLS and `tenant_isolation` are enabled on the partitioned `events` table and on every partition |
| 32 | With two tenants seeded, each scoped session sees only its own rows; cross-tenant queries return 0 rows |
| 33 | Querying a partition directly is still isolated |
| 34 | Cross-tenant `INSERT` and re-stamping are rejected; cross-tenant `UPDATE`/`DELETE` affect 0 rows |
| 35 | A superuser still reads every row, even with a tenant scope set |
| 36 | An unset (NULL) scope and an empty (`''`) scope behave the same |
| 37 | A `SET LOCAL` scope does not survive `COMMIT` on a pooled connection |

CI runs the suite against PostgreSQL 15 and 16 in the
`multi-tenant-rls` job of `.github/workflows/ci.yml`.

## Migration Guide

### Migrating Existing Data

1. All existing events assigned to "default" tenant
2. Run migration to add `tenant_id` column with default value
3. Existing API clients continue to work without changes
4. Enable RLS policies for new deployments

```sql
-- Migration adds tenant_id with default
ALTER TABLE events
ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'default';

-- Create indexes for performance
CREATE INDEX idx_events_tenant_id ON events(tenant_id);
CREATE INDEX idx_events_tenant_contract ON events(tenant_id, contract_id);
```

## Compliance & Security

- **SOC 2 Type II**: Multi-tenant isolation controls
- **GDPR**: Tenant-level data subject rights
- **HIPAA**: Tenant isolation for healthcare data
- **PCI DSS**: Cardholder data isolation per tenant

## Monitoring

### Tenant Metrics

```rust
// Monitor per-tenant activity
metrics::record_tenant_request(tenant_id);
metrics::record_tenant_query_count(tenant_id, count);
```

### Alerts

Configure alerts for:
- RLS policy violations
- Unauthorized cross-tenant access attempts
- Tenant provisioning failures
- Unusual tenant activity patterns

## Troubleshooting

**Access denied errors**: Check X-Tenant-ID header value
**RLS violations**: Verify database session tenant_id setting
**Tenant creation fails**: Check admin permissions and database constraints
**Queries return no results**: Verify tenant_id matches requested tenant

## References

- [PostgreSQL Row-Level Security](https://www.postgresql.org/docs/current/ddl-rowsecurity.html)
- [SaaS Multi-Tenancy Patterns](https://docs.microsoft.com/en-us/azure/sql-database/sql-database-multi-tenant-application)
