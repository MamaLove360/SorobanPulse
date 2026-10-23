# Event Encryption at Rest

SorobanPulse supports optional AES-256-GCM encryption of the `event_data` field before it is written to the database (issue #584).

## What is encrypted

Only the `event_data` JSON column is encrypted. Indexed fields used for querying (`contract_id`, `tx_hash`, `ledger`, `event_type`, `topic`, `tenant_id`, etc.) remain in plaintext so normal queries continue to work without decryption at the query layer.

Each encrypted value is stored as a JSON envelope:

```json
{"encrypted": true, "data": "<base64-ciphertext>", "nonce": "<base64-nonce>"}
```

## Prerequisites

Build with the `encryption` feature flag:

```bash
cargo build --release --features encryption
```

The feature enables the `aes-gcm` dependency. When the feature is disabled all encrypt/decrypt calls are no-ops (plaintext pass-through).

## Configuration

### Generate a key

```bash
openssl rand -hex 32
```

This produces a 64-character hex string representing a 32-byte key.

### Environment variables

| Variable | Required | Description |
|---|---|---|
| `EVENT_DATA_ENCRYPTION_KEY` | Yes (to enable encryption) | 64-char hex AES-256 key |
| `EVENT_DATA_ENCRYPTION_KEY_OLD` | Only during key rotation | Previous key, kept until re-encryption completes |

Add these to your `.env` or Kubernetes secret:

```bash
EVENT_DATA_ENCRYPTION_KEY=aabbcc...  # 64 hex chars
```

## How it works

1. **Write path** — The indexer calls `encryption::encrypt(key, &event_data)` before inserting each row. The ciphertext envelope replaces the plaintext `event_data` value in the database.
2. **Read path** — Handlers call `encryption::decrypt(key, old_key, &event_data)` after fetching rows. Non-encrypted rows (envelope missing `"encrypted": true`) are passed through unchanged, allowing gradual migration.
3. **SSE / WebSocket streams** — Decryption is applied to each event before it is broadcast to subscribers.

## Key rotation

Rotating the encryption key is a two-step process with zero downtime.

### Step 1 — Add the new key alongside the old one

```bash
EVENT_DATA_ENCRYPTION_KEY=<new-64-hex>
EVENT_DATA_ENCRYPTION_KEY_OLD=<old-64-hex>
```

Restart the service. New events are encrypted with the new key. Existing events encrypted with the old key are still readable because `decrypt()` falls back to `EVENT_DATA_ENCRYPTION_KEY_OLD` when the primary key fails.

### Step 2 — Re-encrypt existing events

Trigger the background re-encryption job via the admin API. The endpoint is
blocked behind `ADMIN_API_KEY` (sent as `Authorization: Bearer $ADMIN_API_KEY`
or `X-Admin-Api-Key`); requests without a valid key return `401`.

```bash
curl -X POST https://your-host/v1/admin/encryption/rotate \
  -H "Authorization: Bearer $ADMIN_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"new_key": "<new-64-hex>"}'
```

The body must contain `new_key`, a 64-character hex AES-256 key. The job:

1. Selects rows where `event_data->>'encrypted' = 'true'` that have not yet
   been re-encrypted, in batches of **100**.
2. For each batch, opens a transaction, decrypts every credential with the
   old key (falling back to the current key for rows already rotated),
   re-encrypts with `new_key`, and commits. Committing per batch keeps row
   locks short so normal reads and writes are not blocked.
3. Records progress after each committed batch, so the job is **resumable**:
   if the process is interrupted, calling `POST /v1/admin/encryption/rotate`
   again with the same `new_key` continues from the first unprocessed row
   instead of starting over.

Progress is exposed as the `soroban_pulse_reencryption_progress` Prometheus
gauge, which reports rows completed divided by the total number of rows to
re-encrypt (a value from `0` to `1`). The job is complete when the gauge
reaches `1`.

### Step 3 — Remove the old key

Once the job completes (gauge reaches 1), remove `EVENT_DATA_ENCRYPTION_KEY_OLD` and restart.

### Verifying the rotation

After rotation, decrypting a credential with the old key must fail. You can
confirm this by attempting a read with only the old key configured:

```bash
# With EVENT_DATA_ENCRYPTION_KEY set to the OLD key and no fallback key,
# reads of rotated rows return a decryption error (AES-GCM authentication
# failure) instead of plaintext.
EVENT_DATA_ENCRYPTION_KEY=<old-64-hex> EVENT_DATA_ENCRYPTION_KEY_OLD= \
  cargo run --features encryption -- --check-encryption
```

If any row still decrypts with the old key, the rotation did not finish —
re-run the rotation endpoint and wait for `soroban_pulse_reencryption_progress`
to reach `1` before removing the old key.

## Encrypted query support

All query handlers transparently decrypt `event_data` on read. No changes to query parameters are required. Filters on indexed columns (`contract_id`, `ledger`, etc.) work without modification because those columns are never encrypted.

## Verifying encryption

You can confirm rows are encrypted by querying the database directly:

```sql
SELECT id, event_data->>'encrypted' AS is_encrypted
FROM events
LIMIT 5;
```

Encrypted rows return `'true'`; plaintext rows return `NULL`.

## Security notes

- Each row uses a unique random 12-byte nonce. Nonce reuse is probabilistically impossible.
- The authentication tag provided by AES-256-GCM detects any tampering with stored ciphertext.
- Keys are never logged or exposed in metrics.
- Use a secrets manager (Kubernetes Secrets, AWS Secrets Manager, HashiCorp Vault) to inject `EVENT_DATA_ENCRYPTION_KEY` at runtime rather than committing it to environment files.

## Integration credentials (Issue #1162)

Integration credentials are encrypted at rest with the same AES-256-GCM
envelope, using a **separate** key. Unlike event encryption, this is always on
and is not gated on the `encryption` feature.

| Variable | Required | Description |
|---|---|---|
| `INTEGRATION_ENCRYPTION_KEY` | Yes in production | 64-char hex AES-256 key, distinct from `EVENT_DATA_ENCRYPTION_KEY` |
| `INTEGRATION_ENCRYPTION_KEY_OLD` | Only during key rotation | Previous key, kept until re-encryption completes |

### Encrypted columns

| Table | Column | Contents |
|---|---|---|
| `github_integrations` | `access_token` | GitHub token |
| `discord_integrations` | `webhook_url` | Discord webhook URL (embeds the webhook token) |
| `slack_integrations` | `webhook_url`, `bot_token`, `signing_secret` | Slack webhook URL, bot token, signing secret |
| `telegram_integrations` | `bot_token` | Telegram bot token |
| `pagerduty_integrations` | `routing_key`, `api_key` | PagerDuty routing key and REST API key |
| `notification_channels` | `config` (JSONB) | String values under sensitive keys such as `auth_token`, `api_key`, `bot_token`, `webhook_url`, `password`, `secret`, `token` |

Values are stored as `enc:v1:<base64 envelope>`. The migration
`20260929000001_encrypt_integration_credentials` adds `NOT VALID` CHECK
constraints so the database rejects any new plaintext value in these columns.

### Behaviour

- **Writes fail closed:** if `INTEGRATION_ENCRYPTION_KEY` is missing, creating
  an integration returns `503` instead of storing the credential in plaintext.
- **API responses are masked:** `GET` endpoints return `****` plus the last
  four characters (for example `****WXYZ`), never the plaintext or ciphertext.
- **Backfill:** at startup, rows written before this change are encrypted in
  place. After the first successful start, validate the constraints (the
  statements are listed in the migration file), for example:

  ```sql
  ALTER TABLE github_integrations VALIDATE CONSTRAINT github_integrations_access_token_encrypted;
  ```

### Rotating the integration key

1. Set `INTEGRATION_ENCRYPTION_KEY` to the new key and
   `INTEGRATION_ENCRYPTION_KEY_OLD` to the previous key, then restart.
2. Call `POST /v1/admin/reencrypt`. The response's `jobs` array includes
   `integration_credentials`. Every credential is decrypted (current or old
   key) and re-encrypted with the new key.
3. When the job logs `Integration credential re-encryption completed`, remove
   `INTEGRATION_ENCRYPTION_KEY_OLD` and restart.

### Verifying

```sql
-- Should return 0 rows
SELECT id FROM telegram_integrations WHERE bot_token NOT LIKE 'enc:v1:%';
```
