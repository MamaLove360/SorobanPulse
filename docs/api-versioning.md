# API Versioning Policy

## Overview

Soroban Pulse uses URL-based versioning to provide a stable API contract while allowing for breaking changes in future versions. All production API endpoints are prefixed with `/v1/`.

## Versioning Scheme

### Version Format

API versions follow the format `/v{major}/` where `{major}` is an integer starting at 1.

Examples:
- `/v1/events` - Version 1 of the events endpoint
- `/v2/events` - Version 2 of the events endpoint (future)

### What Constitutes a Version Change

A new major version is required when introducing:

- **Breaking changes to request parameters**: Removing or renaming query parameters, changing parameter types, or making optional parameters required
- **Breaking changes to response format**: Removing or renaming response fields, changing field types, or restructuring the response body
- **Breaking changes to behavior**: Changing the default sort order, pagination logic, or filtering semantics
- **Removal of endpoints**: Deprecating and removing an entire endpoint

### Backward-Compatible Changes

The following changes do NOT require a new version:

- Adding new optional query parameters
- Adding new fields to responses (clients should ignore unknown fields)
- Adding new endpoints
- Fixing bugs that restore documented behavior
- Performance improvements
- Internal refactoring

## Version Lifecycle

### 1. Active

The current production version. Receives all new features, bug fixes, and security updates.

- Current active version: `v1`
- Status: Fully supported
- Sunset date: None

### 2. Deprecated

A version that is still functional but will be removed in the future. Deprecated versions:

- Receive security fixes only (no new features)
- Return `Deprecation: true` header on all responses
- Return `Sunset` header with the removal date (RFC 7231 format)
- Return `Link` header pointing to the equivalent endpoint in the successor version

### 3. Sunset

A version that has been removed and no longer responds to requests. Requests to sunset endpoints return `410 Gone`.

## Deprecation Timeline

### Deprecation Notice Period

When a version is deprecated, clients receive a minimum of **6 months notice** before the sunset date.

### Deprecation Process

1. **Announcement (T-0)**: Deprecation is announced via:
   - Release notes
   - API changelog
   - Email to registered API users (if applicable)
   - `Deprecation` and `Sunset` headers on all responses

2. **Deprecation Period (T+0 to T+6 months)**: 
   - Deprecated version continues to function normally
   - All responses include deprecation headers
   - Documentation is updated with migration guidance
   - New clients are directed to the current version

3. **Sunset (T+6 months)**:
   - Deprecated endpoints are removed
   - Requests return `410 Gone` with a message directing clients to the current version
   - Monitoring alerts are configured to detect clients still using the old version

### Current Deprecation Status

#### Unversioned Routes (Deprecated)

The following unversioned routes are deprecated and will be removed on **2026-10-24**:

| Deprecated Route | Successor Route | Status |
|-----------------|-----------------|--------|
| `/events` | `/v1/events` | Deprecated |
| `/events/stream` | `/v1/events/stream` | Deprecated |
| `/events/contract/:contract_id` | `/v1/events/contract/:contract_id` | Deprecated |
| `/events/tx/:tx_hash` | `/v1/events/tx/:tx_hash` | Deprecated |

All deprecated routes return:
```
Deprecation: true
Sunset: Sat, 24 Oct 2026 00:00:00 GMT
Link: </v1/events>; rel="successor-version"
```

## Migration Guidance

For a step-by-step walkthrough with a full route table, `curl` examples and a bulk-replace one-liner, see [Migration Guide: Unversioned Routes → `/v1/`](migration-unversioned-to-v1.md).

### For API Clients

When you receive a `Deprecation: true` header:

1. **Check the `Sunset` header** to see when the endpoint will be removed
2. **Check the `Link` header** to find the successor endpoint
3. **Update your code** to use the versioned endpoint
4. **Test thoroughly** in a staging environment
5. **Deploy before the sunset date**

### Example Migration

**Before (deprecated):**
```bash
curl https://api.example.com/events?limit=10
```

**After (versioned):**
```bash
curl https://api.example.com/v1/events?limit=10
```

The response format is identical - only the URL changes.

### Breaking Changes Between Versions

When migrating between major versions (e.g., v1 to v2), consult the version-specific migration guide:

- [v1 to v2 Migration Guide](./migrations/v1-to-v2.md) (future)

## Version Support Matrix

| Version | Status | Released | Deprecated | Sunset | Support Level |
|---------|--------|----------|------------|--------|---------------|
| v1 | Active | 2026-03-14 | — | — | Full support |
| unversioned | Deprecated | 2026-03-14 | 2026-04-24 | 2026-10-24 | Security fixes only |

## OpenAPI Specification

The OpenAPI specification includes deprecation markers on deprecated paths:

```yaml
paths:
  /events:
    get:
      deprecated: true
      description: "Deprecated. Use /v1/events instead. This endpoint will be removed on 2026-10-24."
```

The current OpenAPI spec is available at `/openapi.json`.

## SDK Versioning

The official SDKs (`sdk/typescript`, `sdk/python`, `sdk/go`) are versioned
together and track the API version they target.

### Version Numbers

SDKs use [Semantic Versioning](https://semver.org/) `MAJOR.MINOR.PATCH`:

| Part | Changes when |
|------|--------------|
| `MAJOR` | The SDK targets a new API major version (`/v1/` -> `/v2/`). SDK `1.x` speaks `/v1/`, SDK `2.x` speaks `/v2/`. |
| `MINOR` | New endpoints, fields or SDK features are added, backward compatible (see [Backward-Compatible Changes](#backward-compatible-changes)). |
| `PATCH` | Bug fixes and regenerated clients with no public API change. |

A breaking change to an SDK's own surface (renamed method, removed option)
without a new API version is avoided; when unavoidable it waits for the next
`MAJOR`. Until the first published release SDKs stay at `0.x`, where a
`MINOR` bump may break.

All three SDKs are released at the same version from the same commit, so a
given version number means the same API coverage in every language.

### Release Tags

| Tag | Purpose |
|-----|---------|
| `sdk-vX.Y.Z` | Triggers the SDK release (npm, PyPI) for version `X.Y.Z` |
| `sdk/go/vX.Y.Z` | Go module version tag for the module in `sdk/go` (Go requires the subdirectory prefix) |

Before a release, the version in `sdk/typescript/package.json` and
`sdk/python/pyproject.toml` must equal the tag version.

### Package Names

| SDK | Package |
|-----|---------|
| TypeScript | `@soroban-pulse/sdk` (npm) |
| Python | `openapi_client` (PyPI name still to be chosen before the first release) |
| Go | `sdk/go` module; its `go.mod` path (`github.com/soroban-pulse/client-go`) must become `github.com/Soroban-Pulse/SorobanPulse/sdk/go` for `sdk/go/vX.Y.Z` tags to resolve |

### Deprecated API Versions

When an API version is deprecated, the SDK major that targets it keeps
receiving `PATCH` releases until the API version's sunset date, then stops.

## Monitoring Deprecated Endpoints

Operators can monitor usage of deprecated endpoints using the `soroban_pulse_http_request_duration_seconds` metric:

```promql
# Requests to deprecated unversioned endpoints
sum(rate(soroban_pulse_http_request_duration_seconds_count{route!~"/v[0-9]+/.*"}[5m])) by (route)
```

Set up alerts to notify when deprecated endpoints are still receiving traffic close to the sunset date.

## Questions and Support

For questions about API versioning or migration assistance:

- Open an issue on GitHub
- Check the [API documentation](../README.md)
- Review the [changelog](../CHANGELOG.md) (if available)
