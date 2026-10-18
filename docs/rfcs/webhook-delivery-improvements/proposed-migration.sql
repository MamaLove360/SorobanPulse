-- Migration: 20260728000001_webhook_delivery_improvements.sql
-- Purpose: Add comprehensive webhook delivery observability, DLQ management, and reliability features

-- Track 1: Metrics & Observability
-- ===================================

CREATE TABLE IF NOT EXISTS webhook_endpoint_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint_url TEXT NOT NULL,
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL,
    total_attempts INT NOT NULL DEFAULT 0,
    successful INT NOT NULL DEFAULT 0,
    failed INT NOT NULL DEFAULT 0,
    avg_latency_ms NUMERIC,
    p50_latency_ms NUMERIC,
    p95_latency_ms NUMERIC,
    p99_latency_ms NUMERIC,
    success_rate_percent NUMERIC,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(endpoint_url, period_start)
);

CREATE INDEX IF NOT EXISTS idx_endpoint_metrics_endpoint_time 
    ON webhook_endpoint_metrics(endpoint_url, period_start DESC);
CREATE INDEX IF NOT EXISTS idx_endpoint_metrics_period 
    ON webhook_endpoint_metrics(period_start DESC);


-- Track 2: Dead-Letter Queue Management
-- ======================================

CREATE TABLE IF NOT EXISTS dlq_analysis (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint_url TEXT NOT NULL,
    failure_reason TEXT NOT NULL,
    failure_count INT NOT NULL DEFAULT 1,
    last_failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(endpoint_url, failure_reason)
);

CREATE INDEX IF NOT EXISTS idx_dlq_analysis_failure_count ON dlq_analysis(failure_count DESC);
CREATE INDEX IF NOT EXISTS idx_dlq_analysis_endpoint ON dlq_analysis(endpoint_url);

-- Add tracking for replay history (audit trail)
CREATE TABLE IF NOT EXISTS webhook_replay_audit (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    replayed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    endpoint_url TEXT,
    failure_reason TEXT,
    count_replayed INT NOT NULL,
    initiated_by TEXT,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_replay_audit_timestamp ON webhook_replay_audit(replayed_at DESC);


-- Track 3: Reliability Improvements (Circuit Breaker)
-- ===================================================

-- Extend rate_limit_endpoints with circuit breaker state
ALTER TABLE IF EXISTS rate_limit_endpoints
ADD COLUMN IF NOT EXISTS circuit_state TEXT DEFAULT 'closed' 
    CHECK (circuit_state IN ('closed', 'open', 'half_open')),
ADD COLUMN IF NOT EXISTS circuit_opened_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS circuit_failure_count INT DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_circuit_breaker_state 
    ON rate_limit_endpoints(circuit_state) 
    WHERE circuit_state != 'closed';

-- Track circuit breaker state changes for diagnostics
CREATE TABLE IF NOT EXISTS circuit_breaker_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint_url TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('opened', 'closed', 'half_open_test_success', 'half_open_test_failed')),
    triggered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    failure_count INT,
    consecutive_failures INT,
    p99_latency_ms NUMERIC
);

CREATE INDEX IF NOT EXISTS idx_circuit_events_endpoint_time 
    ON circuit_breaker_events(endpoint_url, triggered_at DESC);

-- SLO tracking per endpoint
ALTER TABLE IF EXISTS webhook_endpoint_metrics
ADD COLUMN IF NOT EXISTS slo_window TEXT DEFAULT '24h',
ADD COLUMN IF NOT EXISTS slo_target_percent NUMERIC DEFAULT 99.5,
ADD COLUMN IF NOT EXISTS slo_met BOOLEAN;


-- Track 4: Customer Visibility (Webhook Tracing)
-- ==============================================

CREATE TABLE IF NOT EXISTS webhook_trace_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID,
    subscription_id UUID NOT NULL,
    endpoint_url TEXT NOT NULL,
    delivery_attempt_num INT NOT NULL,
    http_status INT,
    latency_ms INT,
    error TEXT,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_webhook_trace_subscription 
    ON webhook_trace_log(subscription_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_webhook_trace_event 
    ON webhook_trace_log(event_id);
CREATE INDEX IF NOT EXISTS idx_webhook_trace_endpoint 
    ON webhook_trace_log(endpoint_url, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_webhook_trace_cleanup 
    ON webhook_trace_log(timestamp DESC) 
    WHERE timestamp < NOW() - INTERVAL '90 days';

-- Per-subscription analytics materialized view (updated hourly by aggregator)
CREATE TABLE IF NOT EXISTS subscription_analytics_hourly (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subscription_id UUID NOT NULL,
    endpoint_url TEXT NOT NULL,
    period_start TIMESTAMPTZ NOT NULL,
    total_delivered INT DEFAULT 0,
    total_failed INT DEFAULT 0,
    total_pending INT DEFAULT 0,
    avg_latency_ms NUMERIC,
    p95_latency_ms NUMERIC,
    p99_latency_ms NUMERIC,
    success_rate NUMERIC,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(subscription_id, endpoint_url, period_start)
);

CREATE INDEX IF NOT EXISTS idx_subscription_analytics_time 
    ON subscription_analytics_hourly(subscription_id, period_start DESC);


-- Helper Functions
-- ================

-- Calculate SLO attainment from metrics
CREATE OR REPLACE FUNCTION calculate_endpoint_slo(
    p_endpoint_url TEXT,
    p_hours_lookback INT DEFAULT 24
)
RETURNS TABLE(
    success_rate_percent NUMERIC,
    slo_met BOOLEAN,
    target_percent NUMERIC
) AS $$
    SELECT 
        ROUND((successful::numeric / NULLIF(total_attempts, 0) * 100), 2),
        (successful::numeric / NULLIF(total_attempts, 0)) >= 0.995,
        99.5::NUMERIC
    FROM webhook_endpoint_metrics
    WHERE endpoint_url = p_endpoint_url
        AND period_start > NOW() - (p_hours_lookback || ' hours')::INTERVAL
    ORDER BY period_end DESC
    LIMIT 1;
$$ LANGUAGE SQL;

-- Get DLQ alert summary
CREATE OR REPLACE FUNCTION get_dlq_alerts()
RETURNS TABLE(
    endpoint_url TEXT,
    pending_count INT,
    alert_type TEXT,
    details TEXT
) AS $$
    -- Alert 1: More than 1000 pending for any endpoint
    SELECT 
        url,
        COUNT(*)::INT,
        'BACKLOG_HIGH'::TEXT,
        'Endpoint has ' || COUNT(*) || ' pending webhooks'
    FROM webhook_failures
    WHERE status = 'pending'
    GROUP BY url
    HAVING COUNT(*) > 1000
    
    UNION ALL
    
    -- Alert 2: Oldest pending > 24 hours
    SELECT 
        url,
        COUNT(*)::INT,
        'BACKLOG_STALE'::TEXT,
        'Oldest pending webhook is ' || 
            EXTRACT(EPOCH FROM (NOW() - MIN(created_at)))::INT / 3600 || ' hours old'
    FROM webhook_failures
    WHERE status = 'pending'
    GROUP BY url
    HAVING MIN(created_at) < NOW() - INTERVAL '24 hours'
    
    UNION ALL
    
    -- Alert 3: Multiple consecutive failures (circuit breaker concern)
    SELECT 
        endpoint_url,
        circuit_failure_count,
        'CIRCUIT_BREAKER_RISK'::TEXT,
        'Endpoint has ' || circuit_failure_count || ' consecutive failures'
    FROM rate_limit_endpoints
    WHERE circuit_state IN ('open', 'half_open')
        AND circuit_failure_count >= 5;
$$ LANGUAGE SQL;

-- Get top failing endpoints summary
CREATE OR REPLACE FUNCTION get_dlq_stats()
RETURNS TABLE(
    total_pending INT,
    oldest_pending_hours INT,
    endpoints_affected INT,
    top_failure_reason TEXT,
    top_failure_count INT
) AS $$
    SELECT 
        COUNT(*)::INT as total_pending,
        EXTRACT(EPOCH FROM (NOW() - MIN(created_at)))::INT / 3600 as oldest_pending_hours,
        COUNT(DISTINCT url)::INT as endpoints_affected,
        (SELECT failure_reason FROM dlq_analysis ORDER BY failure_count DESC LIMIT 1)::TEXT,
        (SELECT failure_count FROM dlq_analysis ORDER BY failure_count DESC LIMIT 1)::INT
    FROM webhook_failures
    WHERE status = 'pending';
$$ LANGUAGE SQL;


-- Maintenance Tasks
-- =================

-- Cleanup old trace logs (can be called by cron job)
-- Usage: SELECT cleanup_old_webhook_traces(90);
CREATE OR REPLACE FUNCTION cleanup_old_webhook_traces(retention_days INT DEFAULT 90)
RETURNS TABLE(deleted_rows INT) AS $$
    DELETE FROM webhook_trace_log
    WHERE timestamp < NOW() - (retention_days || ' days')::INTERVAL
    RETURNING 1;
$$ LANGUAGE SQL;

-- Refresh subscription analytics (called by aggregator)
CREATE OR REPLACE PROCEDURE refresh_subscription_analytics()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO subscription_analytics_hourly 
    (subscription_id, endpoint_url, period_start, total_delivered, total_failed, total_pending, 
     avg_latency_ms, p95_latency_ms, p99_latency_ms, success_rate)
    SELECT 
        s.id,
        s.callback_url,
        date_trunc('hour', NOW()),
        COUNT(CASE WHEN dq.status = 'delivered' THEN 1 END),
        COUNT(CASE WHEN dq.status = 'failed' THEN 1 END),
        COUNT(CASE WHEN dq.status = 'pending' THEN 1 END),
        AVG(EXTRACT(EPOCH FROM (dq.updated_at - dq.created_at)) * 1000)::NUMERIC,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY 
            EXTRACT(EPOCH FROM (dq.updated_at - dq.created_at)) * 1000),
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY 
            EXTRACT(EPOCH FROM (dq.updated_at - dq.created_at)) * 1000),
        COUNT(CASE WHEN dq.status = 'delivered' THEN 1 END)::numeric / 
            NULLIF(COUNT(*), 0) * 100
    FROM subscriptions s
    LEFT JOIN delivery_queue dq ON dq.subscription_id = s.id
    WHERE dq.created_at > NOW() - INTERVAL '1 hour'
    GROUP BY s.id, s.callback_url
    ON CONFLICT (subscription_id, endpoint_url, period_start) DO UPDATE SET
        total_delivered = EXCLUDED.total_delivered,
        total_failed = EXCLUDED.total_failed,
        total_pending = EXCLUDED.total_pending,
        avg_latency_ms = EXCLUDED.avg_latency_ms,
        p95_latency_ms = EXCLUDED.p95_latency_ms,
        p99_latency_ms = EXCLUDED.p99_latency_ms,
        success_rate = EXCLUDED.success_rate;
END;
$$;

