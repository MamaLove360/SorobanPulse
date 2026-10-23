# API Usage Guide

Practical recipes for working with the SorobanPulse REST API.  Every example
uses `curl` and assumes the service is running at `http://localhost:3000`.
Substitute your deployed base URL as needed.

---

## Table of Contents

1. [Base URL and versioning](#base-url-and-versioning)
2. [Authentication](#authentication)
3. [Pagination](#pagination)
4. [Filtering](#filtering)
5. [Sorting](#sorting)
6. [Rate limiting](#rate-limiting)
7. [Error handling](#error-handling)
8. [Server-Sent Events (SSE)](#server-sent-events-sse)
9. [NDJSON export](#ndjson-export)
10. [Contract resource statistics](#contract-resource-statistics)
11. [Common curl recipes](#common-curl-recipes)

---

## Base URL and versioning

All stable endpoints live under `/v1/`:

```
http://localhost:3000/v1/events
http://localhost:3000/v1/events/{contract_id}
http://localhost:3000/v1/events/tx/{tx_hash}
http://localhost:3000/v1/events/stream
```

The unversioned paths (`/events`, `/events/{contract_id}`, etc.) continue to
work but return a `Deprecation: true` header.  Migrate to `/v1/` paths — they
will be removed in v1.0.

---

## Authentication

Authentication is **optional**.  When the `API_KEY` environment variable is
set the service enforces it on all endpoints except `/health` and `/healthz/*`.

Pass the key via either header:

```bash
# Bearer token
curl -H "Authorization: Bearer $API_KEY" http://localhost:3000/v1/events

# Custom header
curl -H "X-Api-Key: $API_KEY" http://localhost:3000/v1/events
```

### Admin endpoints

Administrative operations (pause/resume indexer, replay, anonymize) are gated
by a **separate** `ADMIN_API_KEY`:

```bash
curl -X POST \
     -H "Authorization: Bearer $ADMIN_API_KEY" \
     http://localhost:3000/v1/admin/indexer/pause
```

| Scenario | Response |
|---|---|
| No key when `API_KEY` is set | `401 Unauthorized` |
| Wrong key | `403 Forbidden` |
| Admin endpoint with regular `API_KEY` | `403 Forbidden` |
| Correct key | `200 OK` |

### Scoped API keys

For production and multi-tenant deployments, mint per-client keys with
explicit scopes instead of sharing `API_KEY`. Scoped keys look like
`sp_<base58>` and are sent the same way as any other key. Only a SHA-256
hash is stored, so the plaintext is shown **once**, at creation.

| Scope | Grants |
|---|---|
| `read:events` | All `GET`/`HEAD` requests (events, contracts, stats, streams) and read-only `POST` queries (`/v1/events/tx/batch`, `/graphql`) |
| `write:subscriptions` | Creating, updating and deleting subscriptions |
| `admin` | Everything, including `/v1/admin/*`. Implies every other scope |

Any other mutating request requires `admin`. A key without the scope a route
needs gets `403 Forbidden` with `{"error": "insufficient scope", "required_scope": "…"}`.

Key management requires `ADMIN_API_KEY` or a key with the `admin` scope —
even when `ADMIN_API_KEY` is unset, these endpoints never fall open:

```bash
# Create a key (the response's "key" field is the only time you see it)
curl -X POST -H "Authorization: Bearer $ADMIN_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"name": "billing-service", "scopes": ["read:events", "write:subscriptions"]}' \
     http://localhost:3000/v1/admin/api-keys

# List keys (never includes the plaintext key)
curl -H "Authorization: Bearer $ADMIN_API_KEY" http://localhost:3000/v1/admin/api-keys

# Revoke a key — rejected from the very next request
curl -X DELETE -H "Authorization: Bearer $ADMIN_API_KEY" \
     http://localhost:3000/v1/admin/api-keys/$KEY_ID
```

Scoped keys are checked against the database on every request (no cache), so
revocation is immediate. `last_used_at` is refreshed at most once a minute.
In multi-tenant mode, pass `"tenant_id"` when creating a non-admin key to bind
it to a tenant.

`API_KEY` and `ADMIN_API_KEY` keep working unchanged. If you only use scoped
keys, set `REQUIRE_API_KEY=true` so requests without a valid key are rejected
even when `API_KEY` is unset.

### Capability discovery

`GET /v1/meta` is public (no key, no rate limit) and reports the server's
compiled features, version, build SHA and API version:

```bash
curl http://localhost:3000/v1/meta
# {"features":["classical-crypto","dashboard","default"],"version":"1.0.0","build_sha":"076772f1a2b3","api_version":"v1"}
```

---

## Pagination

`GET /v1/events` returns paginated results.  Use `page` and `limit` to walk
through large datasets.

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `page` | integer | `1` | 1-based page number |
| `limit` | integer | `20` | Events per page (max 100) |
| `exact_count` | boolean | `false` | Use `true` for a precise `COUNT(*)`. Default uses PostgreSQL statistics for low-latency approximate counts |

### Response shape

```json
{
  "data": [ /* array of event objects */ ],
  "total": 1234,
  "page": 1,
  "limit": 20,
  "approximate": true
}
```

`approximate: true` means `total` comes from `pg_stat` — fast but may be
slightly stale.  Use `exact_count=true` when you need an exact row count.

### Examples

```bash
# First page, 20 events (default)
curl "http://localhost:3000/v1/events"

# Page 3, 50 events per page
curl "http://localhost:3000/v1/events?page=3&limit=50"

# Exact count (slower on large tables)
curl "http://localhost:3000/v1/events?exact_count=true"

# Iterate all pages in a shell loop
PAGE=1
while true; do
  RESP=$(curl -s "http://localhost:3000/v1/events?page=$PAGE&limit=100")
  COUNT=$(echo "$RESP" | jq '.data | length')
  echo "Page $PAGE: $COUNT events"
  [ "$COUNT" -lt 100 ] && break
  PAGE=$((PAGE + 1))
done
```

### Picking the right limit

| Use case | Recommended limit |
|---|---|
| Real-time dashboard (low latency) | 20–50 |
| Batch export | 100 |
| Streaming (use NDJSON instead) | — |

---

## ScVal rendering formats

Event `event_data` (the `value` field and each `topic` entry) is stored as
stellar-xdr's serde JSON. Every event endpoint (`/v1/events`, `/v1/events/{id}`,
`/v1/events/stream` and the export endpoints) accepts a `format` query
parameter to control how those ScVals are rendered:

- `json` (default): stellar-xdr's own serde JSON representation. Unchanged
  from before this parameter existed, for backward compatibility.
- `native`: a friendly JSON shape — `i128`/`u128`/`i64`/`u64` are rendered as
  strings (to avoid precision loss) and addresses as strkeys (`G...`/`C...`).
- `xdr`: the base64-encoded raw XDR bytes for the value.

```bash
curl "http://localhost:3000/v1/events?format=native"
curl "http://localhost:3000/v1/events?format=xdr"
```

`xdr` round-trips losslessly through `json`: decoding the stored JSON back
into an `ScVal` and re-encoding to XDR always produces the same bytes.

## Filtering

### By contract ID

```bash
# All events for one contract
curl "http://localhost:3000/v1/events/CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFCT4"
```

### By transaction hash

```bash
# All events from a specific transaction
curl "http://localhost:3000/v1/events/tx/abc123def456..."
```

Returns `200 OK` with an empty `data` array when the hash is valid but has no
indexed events — never `404`.

### By event type

Accepted values: `contract`, `diagnostic`, `system` (case-insensitive).

```bash
# Only contract events
curl "http://localhost:3000/v1/events?event_type=contract"

# Only diagnostic events
curl "http://localhost:3000/v1/events?event_type=diagnostic"

# Unknown type → 400 Bad Request
curl "http://localhost:3000/v1/events?event_type=unknown"
```

### By ledger range

```bash
# Events in ledger range 1 000 000 – 1 001 000
curl "http://localhost:3000/v1/events?from_ledger=1000000&to_ledger=1001000"

# Only events from ledger 500000 onward
curl "http://localhost:3000/v1/events?from_ledger=500000"

# Only events up to ledger 200000
curl "http://localhost:3000/v1/events?to_ledger=200000"
```

> `from_ledger > to_ledger` returns `400 Bad Request`.

### Combining filters

All filters compose:

```bash
# Diagnostic events for a specific contract in a ledger range
curl "http://localhost:3000/v1/events/CABC...?event_type=diagnostic&from_ledger=1000000&to_ledger=1001000"

# Paginated contract events in a range
curl "http://localhost:3000/v1/events?event_type=contract&from_ledger=900000&page=2&limit=50"
```

---

## Sorting

Events are returned in **ascending ledger order** (oldest first) by default —
this is the natural append order for blockchain data.

The API does not expose a user-facing sort parameter because chronological
ledger order is the only semantically meaningful sort for indexed blockchain
events.  Use the `from_ledger` / `to_ledger` filters to slice the window you
care about.

For reverse-chronological display, reverse the array client-side:

```bash
# Fetch last 20 events then reverse in jq
curl -s "http://localhost:3000/v1/events?limit=20" | jq '.data | reverse'
```

---

## Rate limiting

The default limit is **60 requests per IP per minute** (configurable via
`RATE_LIMIT_PER_MINUTE`).  Set to `0` to disable.

### When you are rate-limited

```
HTTP/1.1 429 Too Many Requests
Retry-After: 30
X-RateLimit-Limit: 60
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1712000060
```

### Handling 429 in shell scripts

```bash
call_api() {
  while true; do
    RESP=$(curl -sS -w "\n%{http_code}" "http://localhost:3000/v1/events")
    STATUS=$(echo "$RESP" | tail -1)
    BODY=$(echo "$RESP" | head -n -1)

    if [ "$STATUS" = "429" ]; then
      RETRY=$(echo "$BODY" | jq -r '.retry_after // 5')
      echo "Rate limited. Retrying in ${RETRY}s..." >&2
      sleep "$RETRY"
    else
      echo "$BODY"
      break
    fi
  done
}
```

### Handling 429 in Python

```python
import time, requests

def get_events(base_url: str, **params):
    while True:
        resp = requests.get(f"{base_url}/v1/events", params=params)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 5))
            print(f"Rate limited. Sleeping {retry_after}s")
            time.sleep(retry_after)
            continue
        resp.raise_for_status()
        return resp.json()
```

### Tips

- For high-throughput scenarios, use the NDJSON export endpoint which is
  optimised for bulk reads and counts against your limit more favourably.
- Set `RATE_LIMIT_PER_MINUTE=0` in development / test environments.
- The `/metrics` and `/healthz/*` endpoints are **exempt** from rate limiting.

---

## Error handling

All error responses use the [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457)
`application/problem+json` media type:

```
HTTP/1.1 400 Bad Request
Content-Type: application/problem+json
```

```json
{
  "type": "https://sorobanpulse.dev/errors/invalid-parameter",
  "title": "Invalid parameter",
  "status": 400,
  "detail": "`from_ledger` must not be greater than `to_ledger`",
  "instance": "/v1/events"
}
```

### Fields

| Field | Type | Description |
|---|---|---|
| `type` | string (URI) | Stable identifier for the error kind.  Dereferencing it documents the error. |
| `title` | string | Short, human-readable summary of the error kind (stable per `type`). |
| `status` | integer | HTTP status code, duplicated in the body for convenience. |
| `detail` | string | Human-readable explanation specific to this occurrence. |
| `instance` | string | URI of the request that produced the error (usually the path). |

### `type` URI convention

`type` values are absolute URIs under `https://sorobanpulse.dev/errors/`.
Clients should branch on `type`, not on `title` or `detail` (which may change
wording).  Examples:

| `type` | Typical status |
|---|---|
| `https://sorobanpulse.dev/errors/invalid-parameter` | `400` |
| `https://sorobanpulse.dev/errors/unauthorized` | `401` |
| `https://sorobanpulse.dev/errors/forbidden` | `403` |
| `https://sorobanpulse.dev/errors/not-found` | `404` |
| `https://sorobanpulse.dev/errors/validation-failed` | `422` |
| `https://sorobanpulse.dev/errors/rate-limited` | `429` |
| `https://sorobanpulse.dev/errors/internal` | `500` |
| `https://sorobanpulse.dev/errors/unavailable` | `503` |

> Older clients that read the legacy `error` field should migrate to `detail`.
=======

### HTTP status codes

| Status | Meaning | Common causes |
|---|---|---|
| `200 OK` | Success | — |
| `400 Bad Request` | Invalid parameters | Unknown `event_type`, `from_ledger > to_ledger`, invalid UUID |
| `401 Unauthorized` | Missing API key | `API_KEY` is set and no key was sent |
| `403 Forbidden` | Wrong key / insufficient privileges | Wrong `API_KEY`, or regular key on admin endpoint |
| `404 Not Found` | Route not found | Typo in path |
| `422 Unprocessable Entity` | Validation failure | Malformed request body |
| `429 Too Many Requests` | Rate limit exceeded | — |
| `500 Internal Server Error` | Unexpected server error | DB query failure, unhandled panic |
| `503 Service Unavailable` | Health check failed | DB unreachable, indexer stalled |

### Checking for errors robustly

```bash
# Check HTTP status code separately from body
HTTP_CODE=$(curl -s -o /tmp/resp.json -w "%{http_code}" \
  "http://localhost:3000/v1/events")

if [ "$HTTP_CODE" -ne 200 ]; then
  echo "Error $HTTP_CODE: $(jq -r .detail /tmp/resp.json)"
  exit 1
fi
cat /tmp/resp.json
```

### Parsing problems in Python

```python
resp = requests.get("http://localhost:3000/v1/events")
if resp.status_code >= 400:
    problem = resp.json()  # Content-Type: application/problem+json
    print(problem["type"], problem["title"], problem.get("detail"))
```

### Retryable vs non-retryable errors

| Status | Retry? | Notes |
|---|---|---|
| `400` | No | Fix the request |
| `401` | No | Check `API_KEY` |
| `403` | No | Check key permissions |
| `429` | Yes | Wait for `Retry-After` |
| `500` | Maybe | Transient — retry with backoff |
| `503` | Yes | Service restarting — retry with backoff |

### Exponential backoff example (Python)

```python
import time, requests

def get_with_retry(url, max_retries=5):
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code in (429, 500, 502, 503, 504):
                wait = min(2 ** attempt, 60)
                print(f"Attempt {attempt+1} got {resp.status_code}, retrying in {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.ConnectionError as e:
            wait = min(2 ** attempt, 60)
            print(f"Connection error: {e}. Retrying in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"Failed after {max_retries} retries")
```

---

## Idempotency keys

POST endpoints that create resources accept an optional `Idempotency-Key`
header.  Send a unique value (e.g. a UUID v4) with the request; retrying with
the **same** key returns the original response instead of creating a duplicate.

```bash
KEY=$(uuidgen)

# First attempt — creates the subscription
curl -X POST http://localhost:3000/v1/subscriptions \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $KEY" \
  -d '{"contract_id":"CABC...","event_type":"contract"}'

# Retry after a timeout — same key, same response, no duplicate created
curl -X POST http://localhost:3000/v1/subscriptions \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $KEY" \
  -d '{"contract_id":"CABC...","event_type":"contract"}'
```

### Semantics

- **Missing header** — the request is processed normally; no idempotency
  guarantee is provided.
- **Same key, same request body** — the cached response (status code and body)
  is replayed.  Only one resource is created.
- **Same key, different request body** — rejected with `422 Unprocessable
  Entity` (the key is already bound to a different request).
- **TTL** — keys are retained for **24 hours**, after which they are purged and
  the key may be reused.

### Supported endpoints

| Endpoint | Method |
|---|---|
| `/v1/subscriptions` | `POST` |
| `/v1/webhooks` (webhook registration) | `POST` |

### Retry recipe (Python)

```python
import uuid, requests

def create_subscription(base_url, api_key, payload, max_retries=5):
    key = str(uuid.uuid4())  # reuse across retries
    for attempt in range(max_retries):
        resp = requests.post(
            f"{base_url}/v1/subscriptions",
            json=payload,
            headers={"Authorization": f"Bearer {api_key}",
                     "Idempotency-Key": key},
            timeout=10,
        )
        if resp.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(2 ** attempt, 60))
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError("Failed after retries")
```

---

## Server-Sent Events (SSE)

Subscribe to a real-time stream of new events as they are indexed.

### All events

```bash
curl -N http://localhost:3000/v1/events/stream
```

### Filtered to one contract

```bash
curl -N "http://localhost:3000/v1/events/stream?contract_id=CABC..."
```

### Multiple contracts (multiplexed stream)

```bash
curl -N "http://localhost:3000/v1/events/stream/multi?contract_ids=CABC...,CDEF...,CGHI..."
```

### SSE event types

| Event name | Payload | Description |
|---|---|---|
| *(unnamed)* | JSON event object | New indexed event |
| `ping` | RFC 3339 timestamp | Keep-alive (every `SSE_KEEPALIVE_SECS`, default 15 s) |
| `close` | — | Server shutting down — reconnect |

### JavaScript (browser)

```javascript
const es = new EventSource('http://localhost:3000/v1/events/stream');

es.onmessage = (e) => {
  const event = JSON.parse(e.data);
  console.log('New event:', event.contract_id, 'ledger', event.ledger);
};

es.addEventListener('ping', (e) => {
  // Stream is alive; e.data is a timestamp
});

es.addEventListener('close', () => {
  // Server shutting down — EventSource reconnects automatically
});

es.onerror = (e) => {
  console.error('SSE error, will auto-reconnect', e);
};
```

### Reconnection with Last-Event-ID

The browser `EventSource` API sends `Last-Event-ID` on reconnect automatically.
The server replays events you missed during the disconnection window.

```bash
# Manual reconnection with last-event-id
curl -N \
  -H "Last-Event-ID: 0000000004294967296-0000000000" \
  http://localhost:3000/v1/events/stream
```

### Node.js (eventsource package)

```javascript
import EventSource from 'eventsource';

const es = new EventSource('http://localhost:3000/v1/events/stream');
es.onmessage = ({ data }) => console.log(JSON.parse(data));
```

---

## NDJSON export

Receive one JSON object per line instead of a wrapped array.  Enables streaming
processing — consumers can start handling events before the full response
arrives.

```bash
# Stream all events as NDJSON
curl -H "Accept: application/x-ndjson" http://localhost:3000/v1/events

# Pipe directly into jq for live processing
curl -sN -H "Accept: application/x-ndjson" \
  "http://localhost:3000/v1/events?from_ledger=1000000" \
  | jq '.contract_id + " ledger=" + (.ledger | tostring)'

# Export to a file
curl -sN -H "Accept: application/x-ndjson" \
  -H "Authorization: Bearer $API_KEY" \
  "http://localhost:3000/v1/events/export" > events.ndjson
```

Example output:

```
{"id":"uuid1","contract_id":"CABC...","event_type":"contract","ledger":1234567,...}
{"id":"uuid2","contract_id":"CABC...","event_type":"contract","ledger":1234568,...}
```

---

## Contract resource statistics

`GET /v1/contracts/{contract_id}/stats` returns per-contract CPU, memory and
fee aggregates, computed once per transaction (not per event) and refreshed
hourly:

```bash
curl -H "X-Api-Key: $API_KEY" http://localhost:3000/v1/contracts/$CONTRACT_ID/stats
```

```json
{
  "contract_id": "CA3D...",
  "tx_count": 1520,
  "first_ledger": 51000120,
  "last_ledger": 51230044,
  "resource_fee": { "total": 182400000, "avg": 120000.0, "p50": 98000.0, "p95": 310000.0, "max": 902311 },
  "cpu_insns":    { "total": 7412000000, "avg": 4876315.8, "p50": 4100000.0, "p95": 9800000.0, "max": 24000000 },
  "mem_bytes":    { "total": 3200000000, "avg": 2105263.2, "p50": 1900000.0, "p95": 4100000.0, "max": 9000000 },
  "refreshed_at": "2026-10-17T18:00:00Z"
}
```

- `resource_fee` is the charged Soroban resource fee in stroops (the declared
  fee when the transaction meta does not report it).
- `cpu_insns` is consumed CPU instructions (the declared limit as a fallback).
- `mem_bytes` requires an RPC with Soroban diagnostic events enabled; it is
  `null` otherwise.
- Returns `404` until resource data exists for the contract. Collection is
  opt-in on the indexer: set `RESOURCE_STATS_ENABLED=true` (one extra
  `getTransaction` call per new transaction).

---

## Common curl recipes

### Fetch all events for a contract (auto-paginate)

```bash
#!/usr/bin/env bash
CONTRACT="CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFCT4"
BASE="http://localhost:3000"
PAGE=1

while true; do
  RESP=$(curl -sf "${BASE}/v1/events/${CONTRACT}?page=${PAGE}&limit=100")
  COUNT=$(echo "$RESP" | jq '.data | length')
  echo "$RESP" | jq -c '.data[]'
  [ "$COUNT" -lt 100 ] && break
  PAGE=$((PAGE + 1))
done
```

### Check if an event appeared for a contract within the last N ledgers

```bash
CONTRACT="CABC..."
CURRENT_LEDGER=$(curl -sf http://localhost:3000/healthz/ready | jq .current_ledger)
FROM=$((CURRENT_LEDGER - 1000))

curl -sf "http://localhost:3000/v1/events/${CONTRACT}?from_ledger=${FROM}" \
  | jq '.data | length'
```

### Watch for new events (SSE, formatted)

```bash
curl -sN "http://localhost:3000/v1/events/stream" \
  | while IFS= read -r line; do
      # SSE lines start with "data: "
      if [[ "$line" == data:* ]]; then
        echo "${line#data: }" | jq '{id, contract_id, ledger, event_type}'
      fi
    done
```

### Check health before making requests

```bash
HEALTH=$(curl -sf http://localhost:3000/healthz/ready)
STATUS=$(echo "$HEALTH" | jq -r .status)

if [ "$STATUS" != "ok" ]; then
  echo "Service not ready: $HEALTH" >&2
  exit 1
fi

curl "http://localhost:3000/v1/events"
```

### Count total events by type

```bash
BASE="http://localhost:3000"

for TYPE in contract diagnostic system; do
  COUNT=$(curl -sf "${BASE}/v1/events?event_type=${TYPE}&exact_count=true" \
           | jq .total)
  printf "%-12s %s\n" "$TYPE" "$COUNT"
done
```

### Prometheus metrics quick-check

```bash
# Pull all soroban_pulse_* metrics
curl -sf http://localhost:3000/metrics \
  | grep '^soroban_pulse'
```

### Interactive OpenAPI explorer

```bash
# Open Swagger UI in your browser
open http://localhost:3000/docs

# Download the OpenAPI spec
curl -o openapi.json http://localhost:3000/openapi.json
```
