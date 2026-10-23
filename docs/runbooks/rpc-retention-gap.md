# Runbook: RPC Retention Gap

**Audience:** On-call engineers — no prior Soroban knowledge required.
**Estimated resolution time:** 15–30 minutes for Option A; 5 minutes for Option B.

---

## What is a Retention Gap?

The Soroban RPC node keeps an in-memory sliding window of recent ledgers
(approximately the last 17,280 ledgers ≈ 24 hours at the standard 5-second
ledger close time). Events from ledgers that fall outside this window are no
longer queryable via `getEvents`.

A **retention gap** happens when the SorobanPulse indexer was paused, restarted,
or redeployed and the RPC node has since rolled past the window that covers the
last ledger the indexer processed. When the indexer resumes, it requests events
from a ledger sequence that the RPC node no longer holds, causing a **sudden
jump** in the indexed ledger sequence.

**Symptoms to look for:**

1. `soroban_pulse_indexer_lag_ledgers` spikes sharply and then drops back down
   to near-zero in a short time (the indexer "skipped ahead").
2. The ledger sequence in the database jumps by thousands of ledgers with no
   corresponding events (a gap in the `events` table or in `indexer_gaps`).
3. Log lines like:
   ```
   WARN RPC cursor expired or outside retention window; advancing to latest ledger
   WARN Ledger gap detected: last_processed=1234500, current=1252000 (gap=17500)
   ```
4. The `LedgerSequenceDiscontinuity` Prometheus alert fires (see
   [docs/alerts.yml](../alerts.yml)).

---

## Step 1 — Detect the Gap

### 1a. Check the Prometheus / Grafana alert

The `LedgerSequenceDiscontinuity` alert in `docs/alerts.yml` fires when the
ledger sequence increases by more than `17000` in a single poll interval.
This threshold is intentionally set below the full 24-hour window so you get
an alert before the next long outage.

Verify the alert is firing:

```bash
# Prometheus query to confirm a jump
promtool query instant \
  'increase(soroban_pulse_indexer_current_ledger[5m]) > 1000'
```

### 1b. Query the indexer gaps table

```bash
psql "$DATABASE_URL" <<'SQL'
-- Show any recorded gaps (requires migration 20260926000001_indexer_gaps.sql)
SELECT
    gap_start_ledger,
    gap_end_ledger,
    gap_end_ledger - gap_start_ledger AS missing_ledgers,
    detected_at
FROM indexer_gaps
ORDER BY detected_at DESC
LIMIT 10;
SQL
```

### 1c. Compare indexed ledger vs. RPC latest ledger

```bash
# Current indexed ledger
psql "$DATABASE_URL" -c \
  "SELECT last_processed_ledger FROM indexer_checkpoints ORDER BY id DESC LIMIT 1;"

# Latest ledger available on the RPC node
curl -s "$STELLAR_RPC_URL" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getLatestLedger","params":{}}' \
  | jq '.result.sequence'
```

A difference of more than 17,280 ledgers means the RPC window has passed your
last checkpoint — this is a confirmed retention gap.

---

## Step 2 — Choose a Recovery Option

You have two options. **Option A** recovers the missing events; **Option B**
accepts the gap and moves forward.

| | Option A — Replay from Galexie | Option B — Accept gap and advance |
|---|---|---|
| **Missing events recovered?** | ✅ Yes | ❌ No |
| **Operational complexity** | Medium | Low |
| **Estimated time** | 15–30 min (plus backfill time) | 5 min |
| **When to use** | Gaps ≤ 7 days; business logic requires complete history | Gaps > 7 days; missing events are non-critical; time pressure |

---

## Option A — Replay from Galexie (recover missing events)

[Galexie](https://github.com/stellar/galexie) is Stellar's ledger data lake.
It stores every ledger's transaction metadata in Google Cloud Storage, making
it possible to backfill events that have already rolled out of the RPC node's
retention window.

### A1. Pause the live indexer

```bash
# Using the admin API (requires ADMIN_API_KEY)
curl -X POST "$SERVICE_URL/v1/admin/indexer/pause" \
  -H "Authorization: Bearer $ADMIN_API_KEY"
```

Confirm the indexer stopped:

```bash
curl -s "$SERVICE_URL/healthz/ready" | jq .indexer
# Expected: "paused"
```

### A2. Identify the exact gap boundaries

```bash
psql "$DATABASE_URL" <<'SQL'
SELECT
    gap_start_ledger,
    gap_end_ledger
FROM indexer_gaps
WHERE recovered_at IS NULL
ORDER BY gap_start_ledger
LIMIT 1;
SQL
```

If the `indexer_gaps` table has no rows, query the checkpoints table:

```bash
psql "$DATABASE_URL" <<'SQL'
SELECT last_processed_ledger FROM indexer_checkpoints ORDER BY id DESC LIMIT 1;
SQL
```

The gap runs from `last_processed_ledger + 1` to the ledger just before the
current RPC `getLatestLedger` sequence (or to wherever you want to resume).

### A3. Trigger the backfill via the admin API

```bash
export START_LEDGER=<gap_start_ledger>
export END_LEDGER=<gap_end_ledger>

curl -X POST "$SERVICE_URL/v1/admin/replay" \
  -H "Authorization: Bearer $ADMIN_API_KEY" \
  -H "Content-Type: application/json" \
  -d "{
    \"start_ledger\": $START_LEDGER,
    \"end_ledger\": $END_LEDGER,
    \"source\": \"galexie\"
  }"
```

The response includes a `replay_id`. Monitor progress:

```bash
curl -s "$SERVICE_URL/v1/admin/replay/$REPLAY_ID" \
  -H "Authorization: Bearer $ADMIN_API_KEY" | jq '{status, progress_pct, eta}'
```

### A4. Resume the live indexer

Once the replay job reports `status: "completed"`:

```bash
curl -X POST "$SERVICE_URL/v1/admin/indexer/resume" \
  -H "Authorization: Bearer $ADMIN_API_KEY"
```

### A5. Mark the gap as recovered

```bash
psql "$DATABASE_URL" <<'SQL'
UPDATE indexer_gaps
SET recovered_at = now(), recovery_method = 'galexie_replay'
WHERE gap_start_ledger = <gap_start_ledger>
  AND gap_end_ledger   = <gap_end_ledger>;
SQL
```

### A6. Verify

```bash
# Confirm no events are missing in the gap range
psql "$DATABASE_URL" <<'SQL'
SELECT COUNT(*) AS events_in_gap
FROM events
WHERE ledger BETWEEN <gap_start_ledger> AND <gap_end_ledger>;
SQL

# Should be > 0 for an active contract.  A result of 0 may mean no contracts
# emitted events in that ledger range (valid) or the replay failed (check logs).
```

---

## Option B — Accept the Gap and Advance

Use this when the missing ledgers are outside the Galexie availability window
(> 7 days old), the missing events are non-critical, or you need to restore
service immediately.

### B1. Set the indexer start ledger to the current RPC head

```bash
# Get the current latest ledger from the RPC node
LATEST=$(curl -s "$STELLAR_RPC_URL" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getLatestLedger","params":{}}' \
  | jq -r '.result.sequence')

echo "Will advance indexer to ledger $LATEST"
```

### B2. Update the checkpoint and record the gap

```bash
psql "$DATABASE_URL" <<'SQL'
-- Record the gap for audit purposes before advancing
INSERT INTO indexer_gaps (gap_start_ledger, gap_end_ledger, recovery_method, detected_at)
VALUES (<last_processed_ledger + 1>, <LATEST - 1>, 'accepted_gap', now())
ON CONFLICT DO NOTHING;

-- Advance the checkpoint so the indexer starts from the current head
UPDATE indexer_checkpoints
SET last_processed_ledger = <LATEST - 1>
WHERE id = (SELECT id FROM indexer_checkpoints ORDER BY id DESC LIMIT 1);
SQL
```

### B3. Set START_LEDGER and restart the indexer

If the indexer is not already running from the checkpoint automatically,
update the environment variable and restart:

```bash
# Kubernetes: patch the deployment
kubectl set env deployment/soroban-pulse START_LEDGER="$LATEST"
kubectl rollout restart deployment/soroban-pulse
kubectl rollout status deployment/soroban-pulse
```

Or via the admin API if the service is running:

```bash
curl -X POST "$SERVICE_URL/v1/admin/indexer/resume" \
  -H "Authorization: Bearer $ADMIN_API_KEY"
```

### B4. Notify stakeholders

Send an incident notification documenting:
- The gap range (start ledger → end ledger)
- The UTC time window covered by the gap
- Which contracts and event types may be missing
- Whether a Galexie replay is planned later

---

## Step 3 — Prevent Recurrence

1. **Lower the alert threshold.** The `LedgerSequenceDiscontinuity` alert fires
   when the sequence jumps by > 17,000. Consider reducing the threshold to 5,000
   if your deployment has SLA requirements that require earlier detection:
   ```yaml
   # In docs/alerts.yml — adjust the threshold
   expr: increase(soroban_pulse_indexer_current_ledger[5m]) > 5000
   ```

2. **Configure a rolling restart budget.** Ensure your Kubernetes `PodDisruptionBudget`
   (`k8s/pdb.yaml`) keeps at least one replica running at all times so the
   indexer only pauses during rolling deploys, not full restarts.

3. **Reduce indexer downtime during deploys.** The indexer advisory lock
   (`INDEXER_LOCK_RETRY_SECS`) means a standby replica promotes to leader
   within 30 seconds of the leader going down. Keep at least 2 replicas
   running so the standby is always warm.

4. **Enable the `indexer_gaps` migration.** Confirm
   `migrations/20260926000001_indexer_gaps.sql` has been applied:
   ```bash
   psql "$DATABASE_URL" -c "\d indexer_gaps"
   ```
   If the table does not exist, run `sqlx migrate run`.

---

## Reference: Key Metrics

| Metric | What it measures | Gap-related threshold |
|---|---|---|
| `soroban_pulse_indexer_current_ledger` | Last ledger processed | Sudden large increase = gap |
| `soroban_pulse_indexer_lag_ledgers` | Ledgers behind RPC latest | Spike then drop = gap absorbed |
| `soroban_pulse_indexer_latest_ledger` | Latest ledger from RPC | Should increase steadily |
| `soroban_pulse_rpc_errors_total` | RPC call failures | High rate = connectivity issue |

## Reference: Related Runbooks

- [Indexer Lag Runbook](indexer-lag.md) — for lag caused by slow RPC, not a retention gap
- [RPC Errors Runbook](rpc-errors.md) — for connectivity and error-rate issues
- [Operator Runbook](operator-runbook.md) — complete operator reference
