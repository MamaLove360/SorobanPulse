# Migration Guide: Unversioned Routes → `/v1/`

The unversioned REST routes (`/events`, `/events/{id}`-style paths and
`/contracts`) are **deprecated** and will be removed in **v1.0**. They are
thin aliases that call exactly the same handlers as their `/v1/`
counterparts, so migrating is a pure path change: request parameters,
response bodies and status codes are identical.

This guide walks through the migration step by step.

- [1. Deprecated routes and their replacements](#1-deprecated-routes-and-their-replacements)
- [2. Before/after `curl` examples](#2-beforeafter-curl-examples)
- [3. Bulk-replacing paths in client code](#3-bulk-replacing-paths-in-client-code)
- [4. The `Deprecation` header and how to detect it](#4-the-deprecation-header-and-how-to-detect-it)
- [5. Timeline](#5-timeline)
- [6. Migration checklist](#6-migration-checklist)

---

## 1. Deprecated routes and their replacements

The deprecated aliases are registered in the `deprecated` router in
[`src/routes.rs`](../src/routes.rs) and wrapped by
`middleware::deprecation_middleware`
([`src/middleware/response_middleware.rs`](../src/middleware/response_middleware.rs)).
This is the complete list. No other unversioned REST route exists.

| # | Method | Deprecated route | `/v1/` replacement | Handler |
|---|--------|------------------|--------------------|---------|
| 1 | `GET` | `/events` | `/v1/events` | `handlers::get_events` |
| 2 | `GET` | `/events/stream` | `/v1/events/stream` | `handlers::stream_events` |
| 3 | `GET` | `/events/contract/{contract_id}` | `/v1/events/contract/{contract_id}` | `handlers::get_events_by_contract` |
| 4 | `GET` | `/events/contract/{contract_id}/stream` | `/v1/events/contract/{contract_id}/stream` | `handlers::stream_events_by_contract` |
| 5 | `GET` | `/events/tx/{tx_hash}` | `/v1/events/tx/{tx_hash}` | `handlers::get_events_by_tx` |
| 6 | `GET` | `/contracts` | `/v1/contracts` | `handlers::get_contracts` |

> **Not affected:** `/health`, `/healthz/*`, `/metrics`, `/status`,
> `/openapi.json`, `/docs`, `/graphql`, `/unsubscribe` and `/ui` are
> operational endpoints. They stay unversioned and are **not** deprecated.
> Every other REST endpoint, such as `/v1/events/stats` or `/v1/events/recent`,
> has only ever been available under `/v1/`.

---

## 2. Before/after `curl` examples

The examples below assume `BASE=http://localhost:3000` and, if auth is
enabled, `API_KEY` set to a valid key.

### 2.1 `GET /events` → `GET /v1/events`

```bash
# Before (deprecated)
curl -s -H "Authorization: Bearer $API_KEY" \
  "$BASE/events?limit=20&exact_count=true"

# After
curl -s -H "Authorization: Bearer $API_KEY" \
  "$BASE/v1/events?limit=20&exact_count=true"
```

### 2.2 `GET /events/stream` → `GET /v1/events/stream`

```bash
# Before (deprecated)
curl -N -H "Accept: text/event-stream" -H "Authorization: Bearer $API_KEY" \
  "$BASE/events/stream"

# After
curl -N -H "Accept: text/event-stream" -H "Authorization: Bearer $API_KEY" \
  "$BASE/v1/events/stream"
```

### 2.3 `GET /events/contract/{contract_id}` → `GET /v1/events/contract/{contract_id}`

```bash
CONTRACT_ID=CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA

# Before (deprecated)
curl -s -H "Authorization: Bearer $API_KEY" \
  "$BASE/events/contract/$CONTRACT_ID?limit=50"

# After
curl -s -H "Authorization: Bearer $API_KEY" \
  "$BASE/v1/events/contract/$CONTRACT_ID?limit=50"
```

### 2.4 `GET /events/contract/{contract_id}/stream` → `GET /v1/events/contract/{contract_id}/stream`

```bash
# Before (deprecated)
curl -N -H "Accept: text/event-stream" -H "Authorization: Bearer $API_KEY" \
  "$BASE/events/contract/$CONTRACT_ID/stream"

# After
curl -N -H "Accept: text/event-stream" -H "Authorization: Bearer $API_KEY" \
  "$BASE/v1/events/contract/$CONTRACT_ID/stream"
```

### 2.5 `GET /events/tx/{tx_hash}` → `GET /v1/events/tx/{tx_hash}`

```bash
TX_HASH=0000000000000000000000000000000000000000000000000000000000000000

# Before (deprecated)
curl -s -H "Authorization: Bearer $API_KEY" "$BASE/events/tx/$TX_HASH"

# After
curl -s -H "Authorization: Bearer $API_KEY" "$BASE/v1/events/tx/$TX_HASH"
```

### 2.6 `GET /contracts` → `GET /v1/contracts`

```bash
# Before (deprecated)
curl -s -H "Authorization: Bearer $API_KEY" "$BASE/contracts"

# After
curl -s -H "Authorization: Bearer $API_KEY" "$BASE/v1/contracts"
```

To check that a route is deprecated, inspect the response headers:

```bash
curl -s -D - -o /dev/null "$BASE/events" | grep -iE '^(deprecation|sunset|link):'
# Deprecation: true
# Sunset: Sat, 24 Oct 2026 00:00:00 GMT
# Link: </v1/events>; rel="successor-version"

curl -s -D - -o /dev/null "$BASE/v1/events" | grep -ic '^deprecation:'
# 0
```

---

## 3. Bulk-replacing paths in client code

The following GNU `sed` one-liner rewrites every unversioned `/events…` and
`/contracts…` path in place, and keeps a `.bak` backup of each file:

```bash
grep -rlE '/(events|contracts)' src/ \
  | xargs sed -E -i.bak 's#(://[^/"'"'"'` ]+|["'"'"'`( =}])/(events|contracts)([/?#"'"'"'` )]|$)#\1/v1/\2\3#g'
```

How it works:

- `(://[^/"'` ]+|["'`( =}])`: the path must start straight after a host
  (`http://host:port`) or after a string delimiter (`"`, `'`, `` ` ``, `(`,
  space, `=`, or `}` as in `${BASE_URL}/events`).
- `/(events|contracts)`: the deprecated path prefix.
- `([/?#"'` )]|$)`: the match must end at a path-segment boundary, so
  `/eventsfoo` is left alone.

It is **safe to re-run**. Paths that are already versioned (`/v1/events`)
or nested under another segment (`/admin/events/export`) do not match,
because `/events` there is preceded by `v1` or `admin`, not by a host or a
delimiter.

Example input and output:

```text
fetch("http://localhost:3000/events?limit=10");    → fetch("http://localhost:3000/v1/events?limit=10");
fetch(`${BASE_URL}/events/contract/${id}`);        → fetch(`${BASE_URL}/v1/events/contract/${id}`);
fetch('/events/stream');                           → fetch('/v1/events/stream');
fetch("https://api.example.com:8443/contracts");   → fetch("https://api.example.com:8443/v1/contracts");
fetch("http://localhost:3000/v1/events");          → (unchanged)
fetch("/admin/events/export");                     → (unchanged)
```

On **macOS/BSD sed**, use `sed -E -i '.bak' …` (note the space). Review the
result with `git diff` and delete the backups with
`find . -name '*.bak' -delete` once you are satisfied.

> If your client builds paths from fragments (for example
> `base + "/" + "events"`), the one-liner will not catch them. Search for
> `"events"` and `"contracts"` string literals as a follow-up.

---

## 4. The `Deprecation` header and how to detect it

Every response from a deprecated route, including error responses, carries
three extra headers that `deprecation_middleware` adds:

| Header | Value | Meaning |
|--------|-------|---------|
| `Deprecation` | `true` | This route is deprecated ([RFC 9745](https://www.rfc-editor.org/rfc/rfc9745)). |
| `Sunset` | `Sat, 24 Oct 2026 00:00:00 GMT` | Planned removal date, as an HTTP-date ([RFC 8594](https://www.rfc-editor.org/rfc/rfc8594)). |
| `Link` | `</v1{path}>; rel="successor-version"` | The `/v1/` path to use instead. The link carries the request path only; query strings are not repeated. |

Responses from `/v1/` routes never carry these headers.

Clients should log or alert when they see `Deprecation`, so that stale
call sites show up long before the sunset date.

### JavaScript / TypeScript

```ts
async function apiFetch(input: string, init?: RequestInit): Promise<Response> {
  const res = await fetch(input, init);
  if (res.headers.get("Deprecation") === "true") {
    console.warn(
      `[soroban-pulse] ${input} is deprecated; ` +
        `sunset=${res.headers.get("Sunset")} successor=${res.headers.get("Link")}`,
    );
  }
  return res;
}
```

### Python (`requests`)

```python
import logging
import requests

def api_get(url: str, **kwargs) -> requests.Response:
    resp = requests.get(url, **kwargs)
    if resp.headers.get("Deprecation") == "true":
        logging.warning(
            "%s is deprecated; sunset=%s successor=%s",
            url, resp.headers.get("Sunset"), resp.headers.get("Link"),
        )
    return resp
```

### Rust (`reqwest`)

```rust
let resp = client.get(url).send().await?;
if resp.headers().get("deprecation").map(|v| v == "true").unwrap_or(false) {
    tracing::warn!(
        url,
        sunset = ?resp.headers().get("sunset"),
        successor = ?resp.headers().get("link"),
        "calling a deprecated Soroban Pulse route"
    );
}
```

### Shell / CI smoke check

This fails the build if any listed endpoint still answers with
`Deprecation: true`:

```bash
for path in /v1/events /v1/contracts; do
  if curl -s -D - -o /dev/null "$BASE$path" | grep -qi '^deprecation: true'; then
    echo "deprecated route in use: $path" >&2; exit 1
  fi
done
```

---

## 5. Timeline

| Date | Milestone | State of unversioned routes |
|------|-----------|-----------------------------|
| 2026-03-14 | `/v1/` API released | Unversioned routes are the original API and fully supported |
| 2026-04-24 | Deprecation announced | Aliases respond normally and add `Deprecation`, `Sunset` and `Link` headers (**current state**) |
| 2026-10-24 | Sunset date (`Sunset` header) | Aliases may be removed at any release after this date and only receive security fixes |
| v1.0 release | Planned removal | Aliases are deleted from `src/routes.rs`, and requests return `404`/`410 Gone` |

See [API Versioning](api-versioning.md) and
[API Deprecation Policy](api-deprecation.md) for the general policy.

---

## 6. Migration checklist

- [ ] Run the `sed` one-liner (section 3) across every client repository.
- [ ] Search for path fragments the one-liner cannot see (`"events"`, `"contracts"`).
- [ ] Update hard-coded URLs in config files, dashboards, cron jobs and reverse-proxy rules.
- [ ] Add deprecation detection (section 4) to your HTTP client wrapper.
- [ ] Deploy to staging and confirm that no response carries `Deprecation: true`.
- [ ] Deploy to production before **2026-10-24**.
