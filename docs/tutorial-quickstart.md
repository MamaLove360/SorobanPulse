# Tutorial: Index Your First Contract in 10 Minutes

This tutorial walks you through getting SorobanPulse running locally, pointing it at a Stellar testnet contract, and watching live events flow in. No prior Rust knowledge is required.

**What you'll build:** A running SorobanPulse instance that indexes events from a Soroban smart contract on the Stellar testnet and exposes them via a REST API.

**Prerequisites:**

- Docker and Docker Compose (Docker Desktop ≥ 4.x, or `docker-compose` CLI)
- `curl` and `git`
- A terminal on Linux, macOS, or WSL2

---

## Step 1 — Clone the repository

```bash
git clone https://github.com/Soroban-Pulse/SorobanPulse.git
cd SorobanPulse
```

Expected output:

```
Cloning into 'SorobanPulse'...
remote: Enumerating objects: ...
Receiving objects: 100% ...
```

---

## Step 2 — Create your environment file

Copy the provided example to `.env` and fill in the required values:

```bash
cp .env.example .env
```

Open `.env` in any editor. The two fields you **must** set are:

| Field | Example value | Notes |
|---|---|---|
| `POSTGRES_USER` | `pulse` | Postgres superuser for the local container |
| `POSTGRES_PASSWORD` | `changeme` | Keep this secret — never commit it |

The defaults for everything else work fine for local development:

```ini
# .env (minimum viable config — already set by .env.example)
STELLAR_RPC_URL=https://soroban-testnet.stellar.org
DATABASE_URL=postgres://pulse:changeme@db:5432/soroban_pulse
PORT=3000
RUST_LOG=info
```

> **Tip:** The `DATABASE_URL` host must be `db` (the Docker Compose service name), not `localhost`.

---

## Step 3 — Start the full stack

```bash
make docker-up
```

This command builds the Rust service image, starts a PostgreSQL 15 container, runs database migrations automatically, and waits until the app reports healthy.

Expected output (abbreviated):

```
[+] Building 45.2s (14/14) FINISHED
[+] Running 3/3
 ✔ Network sorobanpulse_default  Created
 ✔ Container sorobanpulse-db-1   Healthy
 ✔ Container sorobanpulse-app-1  Healthy
```

> First run is slower because Docker downloads the base images and compiles Rust.
> Subsequent starts take ~5 seconds.

---

## Step 4 — Verify the service is healthy

```bash
curl -s http://localhost:3000/health | python3 -m json.tool
```

Expected output:

```json
{
    "status": "ok",
    "db": "ok",
    "indexer": "ok"
}
```

If you see `"indexer": "stalled"`, wait 10 seconds and retry — the indexer needs one successful RPC poll before the health check turns green.

---

## Step 5 — Watch events arrive

The indexer polls the Stellar testnet RPC every 5 seconds and stores new contract events in PostgreSQL. Query the REST API to see what has been indexed:

```bash
curl -s "http://localhost:3000/v1/events?limit=5" | python3 -m json.tool
```

Expected output (your events will differ):

```json
{
    "data": [
        {
            "id": "3f8a1c2d-...",
            "contract_id": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
            "event_type": "contract",
            "tx_hash": "a1b2c3d4...",
            "ledger": 5943210,
            "timestamp": "2026-10-20T12:34:56Z",
            "event_data": {
                "topic": [["Symbol", "transfer"]],
                "value": {"I128": {"hi": 0, "lo": 1000000}}
            },
            "created_at": "2026-10-20T12:34:57Z"
        }
    ],
    "total": 42,
    "page": 1,
    "limit": 5,
    "approximate": true
}
```

> If `"total": 0`, the indexer has just started. Wait 15–30 seconds and retry.

---

## Step 6 — Filter by contract ID

The Stellar testnet has a number of active SAC (Stellar Asset Contract) contracts. Pick one from the events you received and filter:

```bash
# Replace with a real contract_id from Step 5
CONTRACT_ID="CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

curl -s "http://localhost:3000/v1/events/${CONTRACT_ID}?limit=10" \
  | python3 -m json.tool
```

Expected output: an array of events all sharing the same `contract_id`.

---

## Step 7 — Stream events in real time via SSE

Open a second terminal and subscribe to the Server-Sent Events stream:

```bash
curl -N "http://localhost:3000/v1/events/stream"
```

Back in your first terminal, wait for the next ledger (≤5 seconds). You'll see new events appear in the SSE terminal as they are indexed:

```
data: {"id":"abc123","contract_id":"CDLZF...","event_type":"contract","ledger":5943215,...}

data: {"id":"def456","contract_id":"CDLZF...","event_type":"contract","ledger":5943216,...}
```

Press `Ctrl+C` to disconnect — the server detects the disconnection cleanly.

---

## Step 8 — Explore the interactive API docs

Open your browser at **http://localhost:3000/docs** for the Swagger UI — an interactive explorer for every API endpoint. You can:

- Try requests directly in the browser
- See request/response schemas
- Download the OpenAPI JSON from **http://localhost:3000/openapi.json**

---

## Stopping the stack

```bash
make docker-down
```

Data is persisted in a Docker volume — the next `make docker-up` picks up from where you left off.

---

## What's next?

Now that events are flowing, explore the more powerful features:

| Feature | Where to go |
|---|---|
| **Webhook delivery** — push events to your own endpoint | [Webhook Signing](webhook_signing.md) |
| **Real-time SSE streams** — subscribe per-contract | [SSE Reconnection](sse-reconnection.md) |
| **Token alert bot** — Slack alerts on SAC transfers | [Token Alert Bot Tutorial](tutorial-token-alert-bot.md) |
| **CLI tool** — query events from the terminal | `cli/README.md` |
| **Full env var reference** — all configuration options | [Environment Variable Reference](configuration.md) |
| **Production deployment** — Docker, Kubernetes, Terraform | [Deployment Guide](deployment.md) |
