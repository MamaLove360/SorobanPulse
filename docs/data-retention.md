# Data Retention Policy

This document explains how SorobanPulse stores, retains, archives, and deletes data, and what operators and end-users can expect under GDPR and similar privacy regulations.

## Default Retention Periods

| Data Category | Default Retention | Configurable? | Notes |
|---|---|---|---|
| Indexed contract events | Indefinite | Yes (`EVENT_RETENTION_DAYS`) | Core indexer data; set a value to enable auto-expiry |
| Subscription metadata | Indefinite | No | Deleted when subscription is cancelled |
| Webhook configurations | Indefinite | No | Deleted when webhook is removed |
| Webhook delivery logs | 90 days | Yes (`DELIVERY_LOG_RETENTION_DAYS`) | Includes request/response payloads |
| Notification delivery logs | 90 days | Yes (`NOTIFICATION_LOG_RETENTION_DAYS`) | Per-channel send receipts |
| ABI cache entries | 30 days (TTL) | Yes (`ABI_CACHE_TTL_DAYS`) | Auto-evicted; re-fetched on demand |
| Ledger hash records | Indefinite | Yes | Used for deduplication; safe to truncate after 7 days |
| API access logs | 30 days | Via log aggregator | Controlled by your log infrastructure (see [log-aggregation.md](log-aggregation.md)) |
| Admin audit logs | 365 days | No | Retain for compliance; see [audit_logging.md](audit_logging.md) |

Set retention environment variables in `.env`:

```env
EVENT_RETENTION_DAYS=365
DELIVERY_LOG_RETENTION_DAYS=90
NOTIFICATION_LOG_RETENTION_DAYS=90
ABI_CACHE_TTL_DAYS=30
```

When `EVENT_RETENTION_DAYS` is unset (default), events are kept indefinitely. Set it only if your deployment has storage constraints or a legal obligation to purge historical events.

## Archival Procedures

Archive data before deleting it to preserve auditability.

### Archiving events to cold storage

Use `pg_dump` with a row filter to export events older than your retention window before the scheduled purge job removes them:

```bash
pg_dump \
  --table=events \
  --where="created_at < NOW() - INTERVAL '365 days'" \
  --format=custom \
  --compress=9 \
  --file="events_archive_$(date +%Y%m).pgdump" \
  "$DATABASE_URL"
```

Upload the dump to your object store of choice:

```bash
# AWS S3
aws s3 cp events_archive_$(date +%Y%m).pgdump s3://your-bucket/soroban-pulse/archives/

# GCP Cloud Storage
gsutil cp events_archive_$(date +%Y%m).pgdump gs://your-bucket/soroban-pulse/archives/
```

### Archiving delivery logs

```bash
psql "$DATABASE_URL" -c "\COPY (
  SELECT * FROM delivery_logs WHERE created_at < NOW() - INTERVAL '90 days'
) TO STDOUT CSV HEADER" | gzip > delivery_logs_$(date +%Y%m).csv.gz
```

### Scheduling archive + purge

Add a cron job or Kubernetes `CronJob` that runs the archive script before the purge:

```yaml
# k8s/archive-cronjob.yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: soroban-pulse-archive
spec:
  schedule: "0 2 1 * *"   # 02:00 on the 1st of every month
  jobTemplate:
    spec:
      template:
        spec:
          containers:
            - name: archiver
              image: your-registry/soroban-pulse-archiver:latest
              envFrom:
                - secretRef:
                    name: soroban-pulse-secrets
          restartPolicy: OnFailure
```

## GDPR Compliance Guide

SorobanPulse indexes **on-chain public data** from the Stellar network. Because ledger transactions are inherently public and immutable, the indexed events themselves do not normally constitute personal data under GDPR. However, **subscription metadata and delivery logs may contain personal data** (e.g., email addresses or webhook URLs that could identify a natural person).

### What personal data SorobanPulse holds

| Data | Location | Personally identifiable? |
|---|---|---|
| Contract event payloads | `events` table | Generally no (public blockchain data) |
| Subscriber email addresses | `subscriptions` table | **Yes** |
| Webhook endpoint URLs | `webhooks` table | Potentially (if URL contains user ID) |
| Email delivery logs | `delivery_logs` table | **Yes** (recipient address) |
| API authentication keys | `api_keys` table | Potentially (if linked to a person) |

### Right to Access (Article 15)

Query all personal data for a given email address:

```sql
-- Events are not personal data; focus on subscription metadata
SELECT id, email, created_at, filters FROM subscriptions WHERE email = $1;
SELECT id, endpoint_url, created_at FROM webhooks WHERE owner_email = $1;
SELECT id, channel, recipient, sent_at FROM delivery_logs WHERE recipient = $1;
```

Return the result set to the data subject within 30 days.

### Right to Rectification (Article 16)

Update subscriber contact details:

```sql
UPDATE subscriptions SET email = $new_email WHERE email = $old_email;
UPDATE delivery_logs SET recipient = $new_email WHERE recipient = $old_email;
```

### Right to Erasure (Article 17 — "Right to be Forgotten")

Delete all personal data for a subscriber:

```sql
BEGIN;

-- Remove delivery logs first (foreign key dependency)
DELETE FROM delivery_logs
WHERE subscription_id IN (SELECT id FROM subscriptions WHERE email = $1);

-- Remove webhook delivery logs
DELETE FROM delivery_logs
WHERE webhook_id IN (SELECT id FROM webhooks WHERE owner_email = $1);

-- Remove webhooks
DELETE FROM webhooks WHERE owner_email = $1;

-- Remove subscriptions
DELETE FROM subscriptions WHERE email = $1;

COMMIT;
```

Contract events on the Stellar ledger are immutable public data and are outside the scope of erasure. Inform the data subject of this limitation.

### Right to Portability (Article 20)

Export a subscriber's data as JSON:

```sql
SELECT row_to_json(s) FROM subscriptions s WHERE email = $1;
SELECT row_to_json(w) FROM webhooks w WHERE owner_email = $1;
```

### Data Processing Agreement (DPA)

If you are a SaaS operator running SorobanPulse on behalf of customers, you act as a **data processor** for any personal data your customers supply. Ensure you have a signed DPA with each customer and that your infrastructure providers (cloud, SMTP) also have DPAs in place.

### Data residency

SorobanPulse itself does not enforce data residency. To constrain where personal data is stored:
- Deploy PostgreSQL in the required region.
- Use region-specific SMTP relays for email delivery.
- Configure Kinesis / Pub/Sub / Kafka topics in the compliant region.

## Data Deletion Procedures

### Automated purge via cron

Enable automatic event purging by setting `EVENT_RETENTION_DAYS`. SorobanPulse runs a background worker that deletes expired rows in configurable batch sizes to avoid long-running locks:

```env
EVENT_RETENTION_DAYS=365
PURGE_BATCH_SIZE=5000          # rows deleted per transaction (default: 5000)
PURGE_INTERVAL_HOURS=6         # how often the purge worker runs (default: 6)
```

The purge worker logs each batch:

```
INFO soroban_pulse::purge: deleted 5000 expired events, next_run=2026-07-01T02:00:00Z
```

### Manual one-time deletion

Delete all events older than a specific date:

```sql
-- Dry run first
SELECT COUNT(*) FROM events WHERE created_at < '2025-01-01';

-- Then delete in batches to avoid table lock contention
DO $$
DECLARE
  deleted_count INT;
BEGIN
  LOOP
    DELETE FROM events
    WHERE id IN (
      SELECT id FROM events
      WHERE created_at < '2025-01-01'
      LIMIT 5000
    );
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    EXIT WHEN deleted_count = 0;
    PERFORM pg_sleep(0.1);  -- brief pause between batches
  END LOOP;
END $$;
```

### Delivery log purge

```sql
DELETE FROM delivery_logs WHERE created_at < NOW() - INTERVAL '90 days';
```

Run this during low-traffic windows; add `VACUUM ANALYZE delivery_logs;` afterwards to reclaim disk space.

### Full data wipe (decommission)

To completely remove all SorobanPulse data (e.g., when decommissioning an instance):

```bash
# Drop the entire schema — irreversible
psql "$DATABASE_URL" -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
```

Revoke database credentials and destroy any object-store archives according to your organisation's data disposal policy.

## Audit Trail Retention

Keep admin audit logs for at least 365 days, or longer when required by your compliance program. Audit records should include the actor, action, target resource, request identifier, timestamp, and outcome, but should not store full secrets or unnecessary payload data.

Audit logs should be immutable to normal application users. Export them to append-only object storage or a SIEM before database purge jobs run, and restrict deletion permissions to break-glass administrators. When responding to GDPR erasure requests, retain audit entries needed to prove compliance while redacting direct personal identifiers where legally required.

## Related Documentation

- [Encryption at rest setup guide](encryption.md)
- [Audit logging](audit_logging.md)
- [Log aggregation integration](log-aggregation.md)
- [Backup verification CI workflow](backup-verification.md)


## Partition Lifecycle and Pruning

SorobanPulse uses monthly partitions on the `events` table for efficient data retention and purging (see [table-partitioning.md](table-partitioning.md) for details on partition setup).

### Automatic partition pruning

The pruning task (configured by `PURGE_INTERVAL_HOURS`) not only deletes expired rows but also **drops entire partitions** that are older than the retention window. This is more efficient than individual row deletion for large datasets because:

1. **No full table scan**: PostgreSQL checks partition metadata instead of scanning every row.
2. **Instant space reclamation**: Dropping a partition releases storage immediately; no VACUUM needed.
3. **Lower CPU and I/O**: Batch deletes within a partition are I/O intensive; partition drops are metadata operations.

### Partition retention window

The partition pruning function uses this formula:

```
retention_months = max(1, RETENTION_DAYS / 30)
cutoff_date = today - (retention_months * 30 days)
```

For example, if `RETENTION_DAYS=365`:
- `retention_months = 365 / 30 = 12`
- Partitions created before `today - 360 days` are dropped.

### Dry-run mode

Before applying partition drops to production, test the logic:

```sql
SELECT * FROM drop_old_partitions(retain_months := 12, dry_run := true);
```

Output:
```
partition_name       | dropped | reason
---------------------|---------+-------
events_2024_01       | false   | [DRY RUN] Would drop (older than 12 months)
events_2024_02       | false   | [DRY RUN] Would drop (older than 12 months)
events_2024_03       | false   | [DRY RUN] Would drop (older than 12 months)
...
events_2025_08       | false   | Retained (within 12 month retention window)
```

### Manual partition drop

If you need to drop partitions outside the scheduled pruning window:

```sql
-- Check which partitions would be dropped with 6-month retention
SELECT * FROM drop_old_partitions(retain_months := 6, dry_run := true);

-- Execute the drop (careful! this is irreversible)
SELECT * FROM drop_old_partitions(retain_months := 6, dry_run := false);
```

### Monitoring partition drops

The `soroban_pulse_partitions_dropped_total` counter metric tracks the number of partitions dropped. Query it to verify pruning is running:

```
soroban_pulse_partitions_dropped_total{partition="events_2024_01"} 1
soroban_pulse_partitions_dropped_total{partition="events_2024_02"} 1
```

### Partition metadata

The `partition_drops_log` table records each partition drop for audit purposes:

```sql
SELECT partition_name, dropped_at, cutoff_date FROM partition_drops_log
ORDER BY dropped_at DESC LIMIT 10;
```

| partition_name | dropped_at | cutoff_date |
|---|---|---|
| events_2024_03 | 2026-07-01 12:00:00 | 2025-07-01 |
| events_2024_02 | 2026-07-01 12:00:00 | 2025-07-01 |

## Troubleshooting

### Pruning task is not running

Check logs for the pruning worker:

```bash
kubectl logs -n soroban-pulse deployment/soroban-pulse --tail=100 | grep -i prun
```

### Partitions are not being dropped

1. Verify the `drop_old_partitions` function exists:
   ```sql
   SELECT proname FROM pg_proc WHERE proname = 'drop_old_partitions';
   ```

2. Run a dry-run to see what would be dropped:
   ```sql
   SELECT * FROM drop_old_partitions(retain_months := 12, dry_run := true);
   ```

3. Check if events table actually has monthly partitions:
   ```sql
   SELECT tablename FROM pg_tables
   WHERE tablename LIKE 'events_%' AND schemaname = 'public'
   ORDER BY tablename;
   ```

If no partitions exist, run migration `20260727000102_partition_events_by_month.sql` first.

### Storage is growing despite pruning

Possible causes:
- `EVENT_RETENTION_DAYS` is set too high or to 0 (disabled).
- Pruning job is crashing (check logs).
- VACUUM is not keeping up with the deletions (see [pool-optimization.md](pool-optimization.md)).
- Non-events tables (webhooks, subscriptions, delivery_logs) are accumulating data.
