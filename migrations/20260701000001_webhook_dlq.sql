-- Webhook dead-letter queue (DLQ) persistence for #1260.
-- When a webhook exhausts all delivery retries it is moved here instead of
-- being silently discarded, allowing operators to inspect and replay it.

CREATE TABLE IF NOT EXISTS webhook_dlq (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL,
    event_id        UUID NOT NULL,
    payload         JSONB NOT NULL,
    last_error      TEXT,
    failed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    replayed_at     TIMESTAMPTZ
);

-- Look up DLQ entries for a given subscription (used by bulk replay filter).
CREATE INDEX IF NOT EXISTS idx_webhook_dlq_subscription_id
    ON webhook_dlq (subscription_id);

-- Order/inspect by failure time (paginated listing).
CREATE INDEX IF NOT EXISTS idx_webhook_dlq_failed_at
    ON webhook_dlq (failed_at);

-- Quickly find entries that have not yet been replayed.
CREATE INDEX IF NOT EXISTS idx_webhook_dlq_replayed_at
    ON webhook_dlq (replayed_at);
</br>