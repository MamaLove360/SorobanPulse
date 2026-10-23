use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use sqlx::PgPool;
use std::sync::atomic::{AtomicU64, Ordering};

// The local module is also named `metrics`, which shadows the external crate
// of the same name. Use an explicit extern-crate alias to disambiguate.
extern crate metrics as m;

// ── Issue #993: Overflow-safe counter primitives ────────────────────────────
//
// Long-running instances accumulate counts in a handful of places using raw
// `u64`/`AtomicU64` arithmetic (as opposed to the `metrics` crate's own
// `counter!()` macro, whose internal representation is out of our control).
// Plain `fetch_add`/`+` wraps silently on overflow, which would make a
// long-lived counter appear to reset to a small number. `SafeCounter`
// saturates at `u64::MAX` instead of wrapping, and emits a
// `soroban_pulse_counter_overflow_total` metric the moment it saturates so
// the condition is observable rather than silent. See
// docs/metrics-design.md for the full audit and rationale.
pub struct SafeCounter {
    value: AtomicU64,
    name: &'static str,
}

impl SafeCounter {
    pub const fn new(name: &'static str) -> Self {
        Self {
            value: AtomicU64::new(0),
            name,
        }
    }

    /// Increment by `delta` using saturating arithmetic. Returns the new value.
    /// If the counter has already saturated at `u64::MAX`, records an
    /// overflow-detection metric instead of wrapping around to a small number.
    pub fn increment(&self, delta: u64) -> u64 {
        let mut current = self.value.load(Ordering::Relaxed);
        loop {
            let new_value = current.saturating_add(delta);
            match self.value.compare_exchange_weak(
                current,
                new_value,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    if new_value == u64::MAX && current != u64::MAX {
                        record_counter_overflow_detected(self.name);
                    }
                    return new_value;
                }
                Err(actual) => current = actual,
            }
        }
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

impl std::fmt::Debug for SafeCounter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SafeCounter")
            .field("name", &self.name)
            .field("value", &self.get())
            .finish()
    }
}

/// Record that a `SafeCounter` saturated instead of wrapping (issue #993).
pub fn record_counter_overflow_detected(counter_name: &str) {
    m::counter!(
        "soroban_pulse_counter_overflow_total",
        "counter" => counter_name.to_string()
    )
    .increment(1);
}

/// Publish the current value of a long-running counter as a gauge, so
/// operators can see counter state (and how close it is to saturating)
/// without needing to reconstruct it from the exported counter series
/// (issue #993).
pub fn record_counter_state(counter_name: &str, value: u64) {
    m::gauge!(
        "soroban_pulse_counter_state",
        "counter" => counter_name.to_string()
    )
    .set(value as f64);
}

/// SLO-aligned histogram buckets for HTTP request duration (seconds).
const HTTP_DURATION_BUCKETS: &[f64] = &[0.05, 0.1, 0.2, 0.5, 1.0, 5.0];

fn prometheus_builder() -> PrometheusBuilder {
    PrometheusBuilder::new()
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Full(
                "soroban_pulse_http_request_duration_seconds".to_string(),
            ),
            HTTP_DURATION_BUCKETS,
        )
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, "Invalid HTTP duration buckets; using exporter defaults");
            PrometheusBuilder::new()
        })
}

/// Initialize the Prometheus metrics exporter.
///
/// Installing the global recorder fails if one is already installed (e.g. a
/// second call within the same test binary). Rather than panicking, return a
/// handle to a detached recorder so callers still get a working `/metrics`
/// renderer.
pub fn init_metrics() -> PrometheusHandle {
    match prometheus_builder().install_recorder() {
        Ok(handle) => handle,
        Err(e) => {
            tracing::warn!(error = %e, "Prometheus recorder already installed; returning a detached handle");
            prometheus_builder().build_recorder().handle()
        }
    }
}

/// Record events indexed
pub fn record_events_indexed(count: u64) {
    m::counter!("soroban_pulse_events_indexed_total").increment(count);
}

/// Issue #1252: highest ledger a Galexie backfill run has fully processed.
pub fn update_backfill_progress_ledger(ledger: u64) {
    m::gauge!("soroban_pulse_backfill_progress_ledger").set(ledger as f64);
}

/// Issue #1252: ledgers processed by backfill.
pub fn record_backfill_ledgers(count: u64) {
    m::counter!("soroban_pulse_backfill_ledgers_total").increment(count);
}

/// Issue #1252: events newly inserted by backfill (duplicates excluded).
pub fn record_backfill_events(count: u64) {
    m::counter!("soroban_pulse_backfill_events_total").increment(count);
}

/// Issue #1255: a contract entered the deployment registry.
pub fn record_contract_tracked() {
    m::counter!("soroban_pulse_contracts_tracked_total").increment(1);
}

/// Update the current ledger being processed
pub fn update_current_ledger(ledger: u64) {
    m::gauge!("soroban_pulse_indexer_current_ledger").set(ledger as f64);
}

/// Update the latest ledger from RPC
pub fn update_latest_ledger(ledger: u64) {
    m::gauge!("soroban_pulse_indexer_latest_ledger").set(ledger as f64);
}

/// Update the indexer lag
pub fn update_indexer_lag(lag: u64) {
    m::gauge!("soroban_pulse_indexer_lag_ledgers").set(lag as f64);
}

/// Update the last checkpointed ledger
pub fn update_checkpoint_ledger(ledger: u64) {
    m::gauge!("soroban_pulse_indexer_checkpoint_ledger").set(ledger as f64);
}

/// Update the age of table statistics (seconds since last ANALYZE)
pub fn update_stats_age_seconds(age_secs: u64) {
    m::gauge!("soroban_pulse_stats_last_analyzed_age_seconds").set(age_secs as f64);
}

/// Set the is_leader gauge: 1.0 when this replica holds the advisory lock, 0.0 otherwise.
pub fn record_indexer_is_leader(is_leader: bool) {
    m::gauge!("soroban_pulse_indexer_is_leader").set(if is_leader { 1.0 } else { 0.0 });
}

/// Record an RPC error
pub fn record_rpc_error() {
    m::counter!("soroban_pulse_rpc_errors_total").increment(1);
    record_error_by_kind("rpc");
}

/// Record a validation failure
pub fn record_validation_failure() {
    m::counter!("soroban_pulse_events_validation_failed_total").increment(1);
}

/// Record an oversized event that was skipped due to exceeding MAX_EVENT_DATA_BYTES.
pub fn record_oversized_event() {
    m::counter!("soroban_pulse_events_oversized_total").increment(1);
}

/// Record a duplicate event
pub fn record_duplicate_event() {
    m::counter!("soroban_pulse_events_duplicate_total").increment(1);
}

/// Record an XDR validation failure (issue #267)
pub fn record_xdr_invalid() {
    m::counter!("soroban_pulse_events_xdr_invalid_total").increment(1);
}

/// Record an invalid contract ID (issue #370)
pub fn record_invalid_contract_id() {
    m::counter!("soroban_pulse_events_invalid_contract_id_total").increment(1);
}

/// Record a passed XDR validation (issue #616)
pub fn record_xdr_validation_pass() {
    m::counter!("soroban_pulse_xdr_validation_pass_total").increment(1);
}

/// Record a failed XDR validation with a field label (issue #616)
pub fn record_xdr_validation_fail(field: &str) {
    m::counter!(
        "soroban_pulse_xdr_validation_fail_total",
        "field" => field.to_string()
    )
    .increment(1);
}

/// Record an archive integrity failure (issue #371)
pub fn record_archive_integrity_failure() {
    m::counter!("soroban_pulse_archive_integrity_failures_total").increment(1);
}

/// Record an archive query (issue #623)
pub fn record_archive_query() {
    m::counter!("soroban_pulse_archive_queries_total").increment(1);
}

/// Record events restored from archive (issue #623)
pub fn record_archive_restore(count: u64) {
    m::counter!("soroban_pulse_archive_restored_events_total").increment(count);
}

/// Record a batch query and how many events it returned (issue #624)
pub fn record_batch_query(event_count: u64) {
    m::counter!("soroban_pulse_batch_queries_total").increment(1);
    m::counter!("soroban_pulse_batch_query_events_total").increment(event_count);
}

/// Record a full-text search query (issue #625)
pub fn record_fulltext_search() {
    m::counter!("soroban_pulse_fulltext_searches_total").increment(1);
}

/// Record an aggregation query (issue #626)
pub fn record_aggregation_query() {
    m::counter!("soroban_pulse_aggregation_queries_total").increment(1);
}

/// Update re-encryption progress gauge (issue #372)
pub fn update_reencrypt_progress(remaining: u64) {
    m::gauge!("soroban_pulse_reencrypt_progress").set(remaining as f64);
}

/// Update the key-rotation progress gauge as a fraction of rows completed
/// (issue #1270). `rows_total == 0` reports 0.0 rather than NaN.
pub fn update_reencryption_progress(rows_completed: u64, rows_total: u64) {
    let ratio = if rows_total == 0 {
        0.0
    } else {
        (rows_completed as f64 / rows_total as f64).clamp(0.0, 1.0)
    };
    m::gauge!("soroban_pulse_reencryption_progress").set(ratio);
}

/// Record a re-encryption error (issue #372)
pub fn record_reencrypt_error() {
    m::counter!("soroban_pulse_reencrypt_errors_total").increment(1);
}

/// Record a bloom filter hit (pre-filtered duplicate) (issue #266)
pub fn record_bloom_filter_hit() {
    m::counter!("soroban_pulse_bloom_filter_hits_total").increment(1);
}

/// Update the bloom filter size gauge (number of set bits) (issue #369)
pub fn update_bloom_filter_size(size: u64) {
    m::gauge!("soroban_pulse_bloom_filter_size").set(size as f64);
}

/// Record a normalizer error (issue #368)
pub fn record_normalizer_error() {
    m::counter!("soroban_pulse_normalizer_errors_total").increment(1);
}

/// Record a Kinesis publish failure (issue #265)
pub fn record_kinesis_publish_failure() {
    m::counter!("soroban_pulse_kinesis_publish_failures_total").increment(1);
}

/// Record a Pub/Sub publish failure (issue #264)
pub fn record_pubsub_publish_failure() {
    m::counter!("soroban_pulse_pubsub_publish_failures_total").increment(1);
}

/// Record a Pub/Sub message with ordering key set (issue #398)
pub fn record_pubsub_ordering_key_set() {
    m::counter!("soroban_pulse_pubsub_ordering_key_set_total").increment(1);
}

/// Record a rate-limited request rejection (429 Too Many Requests)
pub fn record_rate_limit_rejected() {
    m::counter!("soroban_pulse_rate_limit_rejected_total").increment(1);
}

/// Record a subscription registration rejected by the per-IP rate limit
/// (issue #1268).
pub fn record_subscription_rate_limited() {
    m::counter!("soroban_pulse_subscription_rate_limited_total").increment(1);
}

/// Record an SSE connection rejected by the per-IP connection limit
/// (issue #1268).
pub fn record_sse_rate_limited() {
    m::counter!("soroban_pulse_sse_rate_limited_total").increment(1);
}

/// Record that a notification was suppressed by a suppression list.
pub fn record_notification_suppressed() {
    m::counter!("soroban_pulse_notification_suppressed_total").increment(1);
}

/// Record a webhook failover event.
pub fn record_notification_failover(channel: &str) {
    m::counter!("soroban_pulse_notification_failover_total", "channel" => channel.to_string())
        .increment(1);
}

/// Record a persistent webhook delivery failure (all retries exhausted)
pub fn record_webhook_failure() {
    m::counter!("soroban_pulse_webhook_failures_total").increment(1);
    record_error_by_kind("webhook");
}

/// Record a PagerDuty delivery failure (all retries exhausted)
pub fn record_pagerduty_failure() {
    m::counter!("soroban_pulse_pagerduty_failures_total").increment(1);
}

/// Record a GitHub delivery failure (all retries exhausted)
pub fn record_github_failure() {
    m::counter!("soroban_pulse_github_failures_total").increment(1);
}

/// Record a Discord delivery failure (all retries exhausted)
pub fn record_discord_failure() {
    m::counter!("soroban_pulse_discord_failures_total").increment(1);
}

/// Record a Slack delivery failure (all retries exhausted)
pub fn record_slack_failure() {
    m::counter!("soroban_pulse_slack_failures_total").increment(1);
}

/// Record a Microsoft Teams delivery failure (all retries exhausted)
pub fn record_teams_failure() {
    m::counter!("soroban_pulse_teams_failures_total").increment(1);
}

/// Record a Telegram delivery failure (all retries exhausted)
pub fn record_telegram_failure() {
    m::counter!("soroban_pulse_telegram_failures_total").increment(1);
}

/// Record a Redis queue publish failure (all retries exhausted)
pub fn record_queue_publish_failure() {
    m::counter!("soroban_pulse_redis_publish_failures_total").increment(1);
}

/// Record an event dropped because the Redis in-memory buffer is full
pub fn record_redis_dropped() {
    m::counter!("soroban_pulse_redis_dropped_total").increment(1);
}

/// Record a successful Redis reconnection after a connection loss
pub fn record_redis_reconnect() {
    m::counter!("soroban_pulse_redis_reconnect_total").increment(1);
}

/// Update the Redis in-memory buffer size gauge
pub fn update_redis_buffer_size(size: usize) {
    m::gauge!("soroban_pulse_redis_buffer_size").set(size as f64);
}

/// Record an RPC failover event (primary URL failed, switched to fallback)
pub fn record_rpc_failover() {
    m::counter!("soroban_pulse_rpc_failover_total").increment(1);
}

/// Update the active RPC endpoint label gauge (1.0 = active)
pub fn set_rpc_active_endpoint(endpoint: &str) {
    m::gauge!("soroban_pulse_rpc_active_endpoint", "url" => endpoint.to_string()).set(1.0);
}

/// Issue #1256: p50 probe latency of an RPC endpoint, in milliseconds.
pub fn set_rpc_endpoint_latency_ms(endpoint: &str, latency_ms: f64) {
    m::gauge!("soroban_pulse_rpc_endpoint_latency_ms", "endpoint" => endpoint.to_string())
        .set(latency_ms);
}

/// Issue #1256: whether an RPC endpoint is in rotation (1) or marked
/// unavailable after repeated errors (0).
pub fn set_rpc_endpoint_available(endpoint: &str, available: bool) {
    m::gauge!("soroban_pulse_rpc_endpoint_available", "endpoint" => endpoint.to_string())
        .set(if available { 1.0 } else { 0.0 });
}

/// Record a Kinesis ProvisionedThroughputExceededException (throttled record)
pub fn record_kinesis_throttled() {
    m::counter!("soroban_pulse_kinesis_throttled_total").increment(1);
}

/// Record an email notification failure
pub fn record_email_failure() {
    m::counter!("soroban_pulse_email_failures_total").increment(1);
    record_error_by_kind("email");
}

/// Unified error counter: incremented from each error-generating module
/// with a kind label (rpc, webhook, email, db, sse)
pub fn record_error_by_kind(kind: &str) {
    m::counter!("soroban_pulse_errors_total", "kind" => kind.to_string()).increment(1);
}

/// Record a partition drop event
pub fn record_partition_dropped(partition_name: &str) {
    m::counter!("soroban_pulse_partitions_dropped_total", "partition" => partition_name.to_string())
        .increment(1);
}

/// Record an email bounce reported via the bounce webhook (Issue #484)
pub fn record_email_bounce() {
    m::counter!("soroban_pulse_email_bounces_total").increment(1);
}

/// Issue #619: Record a successful subscription email notification delivery.
pub fn record_email_notification_sent() {
    m::counter!("soroban_pulse_subscription_email_sent_total").increment(1);
}

/// Issue #619: Record a rate-limited subscription email (daily cap hit).
pub fn record_email_rate_limited() {
    m::counter!("soroban_pulse_subscription_email_rate_limited_total").increment(1);
}

/// Issue #619: Record a subscription email config update.
pub fn record_email_subscription_updated() {
    m::counter!("soroban_pulse_subscription_email_config_updates_total").increment(1);
}

/// Issue #620: Record a successful push notification delivery.
pub fn record_push_notification_sent(device_type: &str) {
    m::counter!("soroban_pulse_push_sent_total", "device_type" => device_type.to_string())
        .increment(1);
}

/// Issue #620: Record a failed push notification delivery.
pub fn record_push_notification_failed(device_type: &str) {
    m::counter!("soroban_pulse_push_failed_total", "device_type" => device_type.to_string())
        .increment(1);
}

/// Issue #620: Record an invalid/expired push token cleanup.
pub fn record_push_token_invalid() {
    m::counter!("soroban_pulse_push_token_invalid_total").increment(1);
}

/// Issue #839: Record a push notification delivery retry attempt.
pub fn record_push_retry(device_type: &str, attempt: u32) {
    m::counter!(
        "soroban_pulse_push_retries_total",
        "device_type" => device_type.to_string(),
        "attempt" => attempt.to_string()
    )
    .increment(1);
}

/// Issue #839: Record a Web Push notification sent.
pub fn record_web_push_sent() {
    m::counter!("soroban_pulse_web_push_sent_total").increment(1);
}

/// Issue #839: Record a Web Push notification failure.
pub fn record_web_push_failed() {
    m::counter!("soroban_pulse_web_push_failed_total").increment(1);
}

/// Issue #839: Record push notification delivery latency.
pub fn record_push_delivery_latency(device_type: &str, duration: std::time::Duration) {
    m::histogram!(
        "soroban_pulse_push_delivery_latency_seconds",
        "device_type" => device_type.to_string()
    )
    .record(duration.as_secs_f64());
}

/// Issue #622: Update DB connection pool utilization percentage gauge.
/// `max_connections` is passed in from config since PgPool does not expose it directly.
pub fn update_pool_utilization(pool: &PgPool, max_connections: u32) {
    let size = pool.size() as f64;
    let idle = pool.num_idle() as f64;
    let active = size - idle;
    let max = max_connections as f64;
    let utilization = if max > 0.0 { active / max } else { 0.0 };
    m::gauge!("soroban_pulse_db_pool_utilization").set(utilization);
    m::gauge!("soroban_pulse_db_pool_active_connections").set(active);
    m::gauge!("soroban_pulse_db_pool_max_connections").set(max);
    // Names documented in README.md ("Database pool metrics").
    m::gauge!("soroban_pulse_db_pool_size").set(size);
    m::gauge!("soroban_pulse_db_pool_idle").set(idle);
    m::gauge!("soroban_pulse_db_pool_max").set(max);
}

/// Issue #1244: adaptive tuner recommendations (see `db_pool::adaptive`).
pub fn update_pool_adaptive_targets(recommended_min: u32, recommended_max: u32) {
    m::gauge!("soroban_pulse_db_pool_adaptive_target_min").set(f64::from(recommended_min));
    m::gauge!("soroban_pulse_db_pool_adaptive_target_max").set(f64::from(recommended_max));
}

/// Issue #1244: a failed adaptive-tuner keepalive ping.
pub fn record_pool_health_check_failure() {
    m::counter!("soroban_pulse_db_pool_health_check_failures_total").increment(1);
}

/// Issue #1244: idle connections flagged as potentially stale by the tuner.
pub fn record_pool_stale_connections(count: u64) {
    m::counter!("soroban_pulse_db_pool_stale_connections_total").increment(count);
}

/// Issue #1244: a tracked acquisition that hit its timeout.
pub fn record_pool_acquire_timeout() {
    m::counter!("soroban_pulse_db_pool_acquire_timeout_total").increment(1);
}

/// Issue #622: Record connection acquisition latency.
pub fn record_pool_acquire_latency(duration: std::time::Duration) {
    m::histogram!("soroban_pulse_db_pool_acquire_latency_seconds")
        .record(duration.as_secs_f64());
}

/// Issue #622: Record a pool exhaustion event (utilization >= 90%).
pub fn record_pool_exhaustion_alert() {
    m::counter!("soroban_pulse_db_pool_exhaustion_alerts_total").increment(1);
}

/// Record a full-text search query duration
pub fn record_search_query_duration(duration: std::time::Duration) {
    m::histogram!("soroban_pulse_search_query_duration_seconds").record(duration.as_secs_f64());
}

/// Increment the contract count cache invalidation counter
pub fn record_contract_count_cache_invalidation() {
    m::counter!("soroban_pulse_contract_count_cache_invalidations_total").increment(1);
}

/// Update the contract count cache hit ratio gauge (hits / (hits + misses))
pub fn update_contract_count_cache_hit_ratio(hits: u64, misses: u64) {
    // Issue #993: saturating_add avoids a debug-build panic / release-build
    // wraparound if a long-running instance's hit+miss counts approach u64::MAX.
    let total = hits.saturating_add(misses);
    let ratio = if total == 0 { 0.0 } else { hits as f64 / total as f64 };
    m::gauge!("soroban_pulse_contract_count_cache_hit_ratio").set(ratio);
}

/// Record a Lua script timeout
pub fn record_lua_timeout() {
    m::counter!("soroban_pulse_lua_timeout_total").increment(1);
}

pub fn record_replay_job() {
    m::counter!("soroban_pulse_replay_jobs_total").increment(1);
}

/// Record a feature flag auto-rollback event (#587)
pub fn record_feature_flag_rollback(flag_name: &str) {
    m::counter!(
        "soroban_pulse_feature_flag_rollback_total",
        "flag_name" => flag_name.to_string()
    )
    .increment(1);
}

/// Record the number of database migrations applied during a run (issue #411)
pub fn record_migrations_applied(count: u64) {
    m::counter!("soroban_pulse_migrations_applied_total").increment(count);
}

/// Set the gauge tracking the highest applied migration version (issue #411)
pub fn set_last_migration_version(version: i64) {
    m::gauge!("soroban_pulse_last_migration_version").set(version as f64);
}

/// Record events pruned
pub fn increment_events_pruned(count: u64) {
    m::counter!("soroban_pulse_events_pruned_total").increment(count);
}

/// Record events deleted (GDPR right-to-erasure)
pub fn record_events_deleted(count: u64) {
    m::counter!("soroban_pulse_events_deleted_total").increment(count);
}

/// Record HTTP request duration
pub fn record_http_request_duration(
    duration: std::time::Duration,
    method: &str,
    route: &str,
    status: &str,
) {
    m::histogram!(
        "soroban_pulse_http_request_duration_seconds",
        "method" => method.to_string(),
        "route" => route.to_string(),
        "status" => status.to_string()
    )
    .record(duration.as_secs_f64());
}

/// Update the active SSE connections count
pub fn update_sse_connections(count: usize) {
    m::gauge!("soroban_pulse_sse_active_connections").set(count as f64);
}

/// Update the active WebSocket connections count
pub fn update_ws_connections(count: usize) {
    m::gauge!("soroban_pulse_ws_active_connections").set(count as f64);
}

/// Record SSE connection event (connected, disconnected, or error)
pub fn record_sse_connection_event(result: &str) {
    m::counter!("soroban_pulse_sse_connections_total", "result" => result.to_string()).increment(1);
    if result == "error" {
        record_error_by_kind("sse");
    }
}

/// Record SSE connection duration (seconds) - duration from connection to disconnection
pub fn record_sse_connection_duration(duration_secs: f64) {
    m::histogram!("soroban_pulse_sse_connection_duration_seconds").record(duration_secs);
}

/// Record timeseries query duration
pub fn record_timeseries_query_duration(duration: std::time::Duration) {
    m::histogram!("soroban_pulse_timeseries_query_duration_seconds")
        .record(duration.as_secs_f64());
}

/// Record temporal query duration (Issue #581)
pub fn record_temporal_query_duration(duration: std::time::Duration) {
    m::histogram!("soroban_pulse_temporal_query_duration_seconds")
        .record(duration.as_secs_f64());
}

/// Record a content-fingerprint deduplication hit (Issue #582).
/// Incremented when an event is skipped because its fingerprint matches a
/// recently stored event, indicating a content-identical retry.
pub fn record_content_dedup_hit() {
    m::counter!("soroban_pulse_content_dedup_hits_total").increment(1);
}

/// Record that an event fingerprint was computed and stored (Issue #582).
pub fn record_fingerprint_stored() {
    m::counter!("soroban_pulse_fingerprints_stored_total").increment(1);
}

/// Record a schema validation pass (issue #617)
pub fn record_schema_validation_pass(contract_id: &str) {
    m::counter!(
        "soroban_pulse_schema_validation_pass_total",
        "contract_id" => contract_id.to_string()
    )
    .increment(1);
}

/// Record a schema validation failure (issue #617)
pub fn record_schema_validation_fail(contract_id: &str) {
    m::counter!(
        "soroban_pulse_schema_validation_fail_total",
        "contract_id" => contract_id.to_string()
    )
    .increment(1);
}

/// Record an anonymization operation (issue #618)
pub fn record_anonymization_applied(rule_name: &str) {
    m::counter!(
        "soroban_pulse_anonymization_applied_total",
        "rule" => rule_name.to_string()
    )
    .increment(1);
}

/// Record a PII detection hit (issue #618)
pub fn record_pii_detected(field: &str) {
    m::counter!(
        "soroban_pulse_pii_detected_total",
        "field" => field.to_string()
    )
    .increment(1);
}

/// Record a session-level bloom filter dedup hit (issue #615)
pub fn record_session_bloom_hit() {
    m::counter!("soroban_pulse_session_bloom_hits_total").increment(1);
}

/// Record a session bloom filter reset on new ledger (issue #615)
pub fn record_session_bloom_reset() {
    m::counter!("soroban_pulse_session_bloom_resets_total").increment(1);
}

/// Record contract history query duration
pub fn record_contract_history_query_duration(duration: std::time::Duration) {
    m::histogram!("soroban_pulse_contract_history_query_duration_seconds")
        .record(duration.as_secs_f64());
}

/// Record SSE multi-stream contract IDs per connection (histogram)
pub fn record_sse_multi_contract_ids(count: u64) {
    m::histogram!("soroban_pulse_sse_multi_contract_ids").record(count as f64);
}

/// Issue #995: Record connection wait time (time a request spent waiting for a pool slot).
pub fn record_pool_wait_time(duration: std::time::Duration) {
    m::histogram!("soroban_pulse_db_pool_wait_seconds").record(duration.as_secs_f64());
}

/// Issue #995: Increment the counter of requests that waited >1 s for a pool connection.
pub fn record_pool_wait_timeout() {
    m::counter!("soroban_pulse_db_pool_wait_timeout_total").increment(1);
}

/// Issue #995: Record the current depth of the connection acquisition queue.
pub fn update_pool_queue_depth(depth: usize) {
    m::gauge!("soroban_pulse_db_pool_queue_depth").set(depth as f64);
}

/// Issue #996: Record a bloom filter reset triggered by memory pressure.
pub fn record_bloom_filter_memory_reset() {
    m::counter!("soroban_pulse_bloom_filter_memory_resets_total").increment(1);
}

/// Issue #996: Update the bloom filter fill ratio (inserted items / capacity).
pub fn update_bloom_filter_fill_ratio(ratio: f64) {
    m::gauge!("soroban_pulse_bloom_filter_fill_ratio").set(ratio);
}

/// Issue #996: Record the estimated memory usage of the bloom filter in bytes.
pub fn update_bloom_filter_memory_bytes(bytes: u64) {
    m::gauge!("soroban_pulse_bloom_filter_memory_bytes").set(bytes as f64);
}

/// Issue #996: Record when the bloom filter was rotated (periodic cleanup cycle).
pub fn record_bloom_filter_rotation() {
    m::counter!("soroban_pulse_bloom_filter_rotations_total").increment(1);
}

/// Issue #XXX: Update the estimated false-positive rate of the bloom filter.
/// Estimated from current fill ratio and filter parameters.
pub fn set_bloom_false_positive_rate(rate: f64) {
    m::gauge!("soroban_pulse_bloom_false_positive_rate").set(rate);
}

/// Issue #XXX: Record a webhook delivery trace error for end-to-end observability.
#[cfg(feature = "full-observability")]
pub fn record_webhook_trace_error() {
    m::counter!("soroban_pulse_webhook_trace_errors_total").increment(1);
}

/// Record SSE per-IP connection count (histogram, issue #453)
pub fn record_sse_connections_per_ip(count: usize) {
    m::histogram!("soroban_pulse_sse_connections_per_ip").record(count as f64);
}

/// Increment the lagged events counter (issue #451)
pub fn increment_sse_lagged_events(connection_id: &str, count: u64) {
    m::counter!(
        "soroban_pulse_sse_lagged_events_total",
        "connection_id" => connection_id.to_string()
    )
    .increment(count);
}

/// Increment the maintenance window suppression counter (#495).
pub fn record_notification_maintenance_suppressed() {
    m::counter!("soroban_pulse_notifications_maintenance_suppressed_total").increment(1);
}

/// Set the channel health gauge (#498): 1.0 = healthy, 0.0 = unhealthy.
pub fn set_channel_health(channel_name: &str, channel_type: &str, healthy: bool) {
    m::gauge!(
        "soroban_pulse_notification_channel_healthy",
        "channel" => channel_name.to_string(),
        "type" => channel_type.to_string()
    )
    .set(if healthy { 1.0 } else { 0.0 });
}

/// Record a slow query (issue #421).
pub fn record_slow_query(query_type: &str) {
    m::counter!("soroban_pulse_slow_queries_total", "query_type" => query_type.to_string())
        .increment(1);
}

/// Record query duration per query type (issue #421).
pub fn record_query_duration(query_type: &str, duration: std::time::Duration) {
    m::histogram!(
        "soroban_pulse_query_duration_seconds",
        "query_type" => query_type.to_string()
    )
    .record(duration.as_secs_f64());
}

/// Update DB connection pool metrics
pub fn update_db_pool_metrics(pool: &PgPool) {
    m::gauge!("soroban_pulse_db_pool_size").set(pool.size() as f64);
    m::gauge!("soroban_pulse_db_pool_idle").set(pool.num_idle() as f64);
}

/// Update the process RSS memory gauge (Linux only).
/// Reads VmRSS from /proc/self/status.
#[cfg(target_os = "linux")]
pub fn update_process_memory_bytes() {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                if let Some(kb_str) = rest.split_whitespace().next() {
                    if let Ok(kb) = kb_str.parse::<u64>() {
                        m::gauge!("soroban_pulse_process_memory_bytes").set((kb * 1024) as f64);
                    }
                }
                break;
            }
        }
    }
}

/// Spawn a background task that updates process memory every 30 seconds (Linux only).
#[cfg(target_os = "linux")]
pub fn spawn_memory_collector() -> tokio::task::JoinHandle<()> {
    tokio::spawn(async {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            interval.tick().await;
            update_process_memory_bytes();
        }
    })
}

/// #513: Record notification delivery latency per channel
pub fn record_notification_delivery_latency(channel: &str, latency_seconds: f64) {
    m::histogram!(
        "soroban_pulse_notification_delivery_latency_seconds",
        "channel" => channel.to_string()
    )
    .record(latency_seconds);
}

/// #514: Update notification rate per minute gauge per channel
pub fn update_notification_rate_per_minute(channel: &str, rate: f64) {
    m::gauge!(
        "soroban_pulse_notification_rate_per_minute",
        "channel" => channel.to_string()
    )
    .set(rate);
}

/// #695: Record a successful notification delivery attempt.
pub fn record_notification_delivery_success() {
    m::counter!("soroban_pulse_notification_delivery_success_total").increment(1);
}

/// #695: Record a failed notification delivery attempt.
pub fn record_notification_delivery_failure() {
    m::counter!("soroban_pulse_notification_delivery_failure_total").increment(1);
}

/// #695: Record a successful webhook delivery.
pub fn record_webhook_delivery_success() {
    m::counter!("soroban_pulse_webhook_delivery_success_total").increment(1);
}

/// #695: Update the per-contract event count gauge (used for top-10 contract popularity panel).
pub fn update_contract_event_count(contract_id: &str, count: i64) {
    m::gauge!(
        "soroban_pulse_contract_event_count",
        "contract_id" => contract_id.to_string()
    )
    .set(count as f64);
}

/// Issue #1257: per-contract resource usage for the Grafana "Contract
/// Resource Usage" panels. Only the top contracts by total fee are exported.
pub fn update_contract_resource_stats(
    contract_id: &str,
    total_resource_fee: Option<i64>,
    avg_cpu_insns: Option<f64>,
    avg_mem_bytes: Option<f64>,
) {
    let label = contract_id.to_string();
    if let Some(v) = total_resource_fee {
        m::gauge!("soroban_pulse_contract_resource_fee_total", "contract_id" => label.clone())
            .set(v as f64);
    }
    if let Some(v) = avg_cpu_insns {
        m::gauge!("soroban_pulse_contract_cpu_insns_avg", "contract_id" => label.clone()).set(v);
    }
    if let Some(v) = avg_mem_bytes {
        m::gauge!("soroban_pulse_contract_mem_bytes_avg", "contract_id" => label).set(v);
    }
}

/// #695: Record an observation of indexer lag for heatmap distribution.
pub fn record_indexer_lag_observation(lag: u64) {
    m::histogram!("soroban_pulse_indexer_lag_observation_ledgers").record(lag as f64);
}

// ── Issue #607: Contract ABI cache metrics ───────────────────────────────────

pub fn record_abi_cache_hit(contract_id: &str) {
    m::counter!(
        "soroban_pulse_abi_cache_hits_total",
        "contract_id" => contract_id.to_string()
    )
    .increment(1);
}

pub fn record_abi_cache_miss(contract_id: &str) {
    m::counter!(
        "soroban_pulse_abi_cache_misses_total",
        "contract_id" => contract_id.to_string()
    )
    .increment(1);
}

pub fn record_abi_validation_failure(contract_id: &str) {
    m::counter!(
        "soroban_pulse_abi_validation_failures_total",
        "contract_id" => contract_id.to_string()
    )
    .increment(1);
}

pub fn record_abi_cache_eviction() {
    m::counter!("soroban_pulse_abi_cache_evictions_total").increment(1);
}

// ── Issue #608: Ledger hash metrics ─────────────────────────────────────────

pub fn record_ledger_hash_mismatch(ledger: u64) {
    m::counter!(
        "soroban_pulse_ledger_hash_mismatches_total",
        "ledger" => ledger.to_string()
    )
    .increment(1);
}

pub fn record_ledger_hash_verified() {
    m::counter!("soroban_pulse_ledger_hashes_verified_total").increment(1);
}

pub fn update_ledger_hash_chain_height(ledger: u64) {
    m::gauge!("soroban_pulse_ledger_hash_chain_height").set(ledger as f64);
}

// ── Issue #609: Multi-chain / network metrics ────────────────────────────────

pub fn update_network_health(chain_id: &str, healthy: bool) {
    m::gauge!(
        "soroban_pulse_network_healthy",
        "chain_id" => chain_id.to_string()
    )
    .set(if healthy { 1.0 } else { 0.0 });
}

/// Indexer lag labeled by network (Issue #1063), so each concurrently
/// indexed network (testnet, mainnet, ...) gets its own lag gauge instead of
/// sharing the single global `soroban_pulse_indexer_lag_ledgers` metric.
pub fn update_network_indexer_lag(chain_id: &str, lag: u64) {
    m::gauge!(
        "soroban_pulse_network_indexer_lag_ledgers",
        "chain_id" => chain_id.to_string()
    )
    .set(lag as f64);
}

pub fn update_network_latest_ledger(chain_id: &str, ledger: u64) {
    m::gauge!(
        "soroban_pulse_network_latest_ledger",
        "chain_id" => chain_id.to_string()
    )
    .set(ledger as f64);
}

pub fn record_network_indexer_error(chain_id: &str) {
    m::counter!(
        "soroban_pulse_network_indexer_errors_total",
        "chain_id" => chain_id.to_string()
    )
    .increment(1);
}

// ── Issue #610: Compression metrics ─────────────────────────────────────────

pub fn record_compression_ratio(original_bytes: usize, compressed_bytes: usize) {
    if original_bytes > 0 {
        let ratio = compressed_bytes as f64 / original_bytes as f64;
        m::histogram!("soroban_pulse_compression_ratio").record(ratio);
    }
    m::counter!("soroban_pulse_events_compressed_total").increment(1);
    m::counter!(
        "soroban_pulse_compression_bytes_saved_total"
    )
    .increment(original_bytes.saturating_sub(compressed_bytes) as u64);
}

pub fn record_decompression_failure() {
    m::counter!("soroban_pulse_decompression_failures_total").increment(1);
}

// ── Issue #961: HTTP response compression metrics ───────────────────────────
//
// Distinct from `record_compression_ratio` above, which tracks storage-level
// event archival compression. These track the `tower_http::CompressionLayer`
// middleware that compresses outgoing HTTP responses, so operators can see
// what fraction of traffic is actually being compressed vs. bypassed (small
// responses, non-negotiating clients, excluded content types like SSE).

/// Record whether a single HTTP response left the compression layer
/// compressed (`Content-Encoding` present) or was passed through untouched.
pub fn record_http_compression_outcome(compressed: bool) {
    if compressed {
        m::counter!("soroban_pulse_http_compression_applied_total").increment(1);
    } else {
        m::counter!("soroban_pulse_http_compression_bypassed_total").increment(1);
    }
}

// ── Issue #962: pagination strategy metrics ─────────────────────────────────

/// Record which pagination strategy a `/v1/events` request used, so
/// operators can see the offset-vs-cursor adoption split and correlate it
/// with p99 latency on deep pages.
pub fn record_pagination_strategy(cursor: bool) {
    let strategy = if cursor { "cursor" } else { "offset" };
    m::counter!("soroban_pulse_pagination_requests_total", "strategy" => strategy).increment(1);
}

// ── SSE ring buffer metrics ──────────────────────────────────────────────────

/// Record the number of events replayed to a reconnecting SSE client.
pub fn record_sse_replayed_events(count: u64) {
    m::counter!("soroban_pulse_sse_replayed_events_total").increment(count);
}

/// Record a ring-buffer overflow (oldest event evicted to make room).
pub fn record_sse_ring_buffer_overflow() {
    m::counter!("soroban_pulse_sse_ring_buffer_overflows_total").increment(1);
}

/// Update the current number of events stored in the ring buffer.
pub fn update_sse_ring_buffer_size(size: usize) {
    m::gauge!("soroban_pulse_sse_ring_buffer_size").set(size as f64);
}

/// Record a ring-buffer miss — client's Last-Event-ID was evicted; replay fell back to DB.
pub fn record_sse_ring_buffer_miss() {
    m::counter!("soroban_pulse_sse_ring_buffer_misses_total").increment(1);
}

// ── Query result cache metrics ────────────────────────────────────────────────

/// Record a query-result cache hit.
pub fn record_query_cache_hit(query_type: &str) {
    m::counter!(
        "soroban_pulse_query_cache_hits_total",
        "query_type" => query_type.to_string()
    )
    .increment(1);
}

/// Record a query-result cache miss.
pub fn record_query_cache_miss(query_type: &str) {
    m::counter!(
        "soroban_pulse_query_cache_misses_total",
        "query_type" => query_type.to_string()
    )
    .increment(1);
}

/// Record the estimated row count from a query execution plan (EXPLAIN).
pub fn record_query_plan_estimated_rows(query_type: &str, estimated_rows: f64) {
    m::histogram!(
        "soroban_pulse_query_plan_estimated_rows",
        "query_type" => query_type.to_string()
    )
    .record(estimated_rows);
}

// ── Query Response Streaming metrics (Issue #688) ──────────────────────────────

/// Record item sent in streaming response
pub fn record_streaming_response_item_sent() {
    m::counter!("soroban_pulse_streaming_response_items_sent_total").increment(1);
}

/// Record streaming response completed with item count
pub fn record_streaming_response_completed(item_count: u64) {
    m::counter!("soroban_pulse_streaming_responses_completed_total").increment(1);
    m::histogram!("soroban_pulse_streaming_response_items_per_stream").record(item_count as f64);
}

/// Record streaming response error
pub fn record_streaming_response_error(error_type: &str) {
    m::counter!(
        "soroban_pulse_streaming_response_errors_total",
        "error_type" => error_type.to_string()
    )
    .increment(1);
}

/// Record a chunk flushed to a streaming client, in bytes on the wire.
pub fn record_streaming_response_chunk(bytes: u64) {
    m::counter!("soroban_pulse_streaming_response_chunks_total").increment(1);
    m::histogram!("soroban_pulse_streaming_response_chunk_bytes").record(bytes as f64);
}

/// Record that a producer parked on a full channel waiting for a slow consumer.
///
/// A rising rate here means clients are reading slower than the database
/// produces — the healthy signal that backpressure is doing its job, and the
/// early warning that request timeouts are coming.
pub fn record_streaming_response_backpressure() {
    m::counter!("soroban_pulse_streaming_response_backpressure_total").increment(1);
}

/// Record a stream that ended early. `reason` is `caller` or `client`.
pub fn record_streaming_response_cancelled(reason: &str) {
    m::counter!(
        "soroban_pulse_streaming_responses_cancelled_total",
        "reason" => reason.to_string()
    )
    .increment(1);
}

/// Record total wall time of a streaming response, in seconds.
pub fn record_streaming_response_duration(seconds: f64) {
    m::histogram!("soroban_pulse_streaming_response_duration_seconds").record(seconds);
}

// ── Query Result Streaming metrics (Issue #960) ───────────────────────────────

/// Record a row handed out by a streamed query.
pub fn record_query_stream_row() {
    m::counter!("soroban_pulse_query_stream_rows_total").increment(1);
}

/// Record a completed batch fetch and how many rows it returned.
pub fn record_query_stream_batch(rows: u64) {
    m::counter!("soroban_pulse_query_stream_batches_total").increment(1);
    m::histogram!("soroban_pulse_query_stream_batch_rows").record(rows as f64);
}

/// Record a failed batch fetch.
pub fn record_query_stream_error() {
    m::counter!("soroban_pulse_query_stream_batch_errors_total").increment(1);
}

/// Record a keep-alive tick emitted while a batch was still running.
pub fn record_query_stream_keepalive() {
    m::counter!("soroban_pulse_query_stream_keepalives_total").increment(1);
}

/// Record a stream stopped by its caller.
pub fn record_query_stream_cancelled() {
    m::counter!("soroban_pulse_query_streams_cancelled_total").increment(1);
}

/// Record a stream that stopped at `max_batches` with rows still unread.
pub fn record_query_stream_truncated() {
    m::counter!("soroban_pulse_query_streams_truncated_total").increment(1);
}

/// Record a stream that delivered its whole result set.
pub fn record_query_stream_completed(rows: u64) {
    m::counter!("soroban_pulse_query_streams_completed_total").increment(1);
    m::histogram!("soroban_pulse_query_stream_rows_per_stream").record(rows as f64);
}

// ── JSON Serialization metrics (Issue #687) ──────────────────────────────────

/// Record JSON serialization cache hit
pub fn record_serialization_cache_hit(entity_type: &str) {
    m::counter!(
        "soroban_pulse_serialization_cache_hits_total",
        "entity_type" => entity_type.to_string()
    )
    .increment(1);
}

/// Record JSON serialization cache miss
pub fn record_serialization_cache_miss(entity_type: &str) {
    m::counter!(
        "soroban_pulse_serialization_cache_misses_total",
        "entity_type" => entity_type.to_string()
    )
    .increment(1);
}

/// Record JSON serialization time in microseconds
pub fn record_serialization_time(entity_type: &str, duration_us: u64) {
    m::histogram!(
        "soroban_pulse_serialization_time_us",
        "entity_type" => entity_type.to_string()
    )
    .record(duration_us as f64);
}

/// Record an entry evicted from the serialization cache (TTL or capacity). (#959)
pub fn record_serialization_cache_eviction(entity_type: &str) {
    m::counter!(
        "soroban_pulse_serialization_cache_evictions_total",
        "entity_type" => entity_type.to_string()
    )
    .increment(1);
}

/// Record a deliberate invalidation. `strategy` is `key`, `entity_type`, or `all`. (#959)
pub fn record_serialization_cache_invalidation(entity_type: &str, strategy: &str) {
    m::counter!(
        "soroban_pulse_serialization_cache_invalidations_total",
        "entity_type" => entity_type.to_string(),
        "strategy" => strategy.to_string()
    )
    .increment(1);
}

/// Record entries loaded by a pre-warm pass. (#959)
pub fn record_serialization_cache_prewarm(entity_type: &str, entries: u64) {
    m::counter!(
        "soroban_pulse_serialization_cache_prewarmed_total",
        "entity_type" => entity_type.to_string()
    )
    .increment(entries);
}

/// Record bytes served from cache rather than re-serialized — the CPU the
/// cache actually saved, as opposed to how often it was consulted. (#959)
pub fn record_serialization_cache_bytes_saved(entity_type: &str, bytes: u64) {
    m::counter!(
        "soroban_pulse_serialization_cache_bytes_saved_total",
        "entity_type" => entity_type.to_string()
    )
    .increment(bytes);
}

/// Update the live serialization cache entry-count gauge. (#959)
pub fn update_serialization_cache_entry_count(count: u64) {
    m::gauge!("soroban_pulse_serialization_cache_entry_count").set(count as f64);
}

/// Update the observed hit rate, in the range 0.0 to 1.0. (#959)
pub fn update_serialization_cache_hit_rate(entity_type: &str, rate: f64) {
    m::gauge!(
        "soroban_pulse_serialization_cache_hit_rate",
        "entity_type" => entity_type.to_string()
    )
    .set(rate);
}

/// Update the current cache version, bumped on a bulk invalidation. (#959)
pub fn update_serialization_cache_version(entity_type: &str, version: u64) {
    m::gauge!(
        "soroban_pulse_serialization_cache_version",
        "entity_type" => entity_type.to_string()
    )
    .set(version as f64);
}

// ── PostgreSQL Query Plan Caching metrics (Issue #689 / #802) ──────────────────

/// Record query plan cache hit
pub fn record_query_plan_cache_hit() {
    m::counter!("soroban_pulse_query_plan_cache_hits_total").increment(1);
}

/// Record query plan cache miss
pub fn record_query_plan_cache_miss() {
    m::counter!("soroban_pulse_query_plan_cache_misses_total").increment(1);
}

/// Record query plan cached
pub fn record_query_plan_cached() {
    m::counter!("soroban_pulse_query_plans_cached_total").increment(1);
}

/// Record query planning time in milliseconds
pub fn record_query_planning_time(planning_time_ms: f64) {
    m::histogram!("soroban_pulse_query_planning_time_ms").record(planning_time_ms);
}

/// Record a query plan eviction from the LRU/TTL cache. (#802)
pub fn record_query_plan_cache_eviction() {
    m::counter!("soroban_pulse_query_plan_cache_evictions_total").increment(1);
}

/// Update the hit-ratio gauge (0.0 – 1.0).  Refreshed on every get(). (#802)
pub fn update_query_plan_hit_ratio(ratio: f64) {
    // Clamp to [0,1] and guard against any NaN that slips through.
    let safe = if ratio.is_finite() { ratio.clamp(0.0, 1.0) } else { 0.0 };
    m::gauge!("soroban_pulse_query_plan_cache_hit_ratio").set(safe);
}

/// Update the live entry-count gauge after each insert. (#802)
pub fn update_query_plan_cache_entry_count(count: u64) {
    m::gauge!("soroban_pulse_query_plan_cache_entry_count").set(count as f64);
}

// ── Schema health metrics (Issue #804) ─────────────────────────────────────

/// Update the count of public-schema indexes whose idx_scan is 0 since the
/// last statistics reset, excluding newly-created partition indexes. (#804)
pub fn update_schema_unused_indexes(count: u64) {
    m::gauge!("soroban_pulse_schema_unused_indexes_total").set(count as f64);
}

/// Update the count of missing future month partitions for the events table.
/// A value > 0 means partition pre-creation is lagging. (#804)
pub fn update_schema_missing_future_partitions(count: u64) {
    m::gauge!("soroban_pulse_schema_missing_future_partitions").set(count as f64);
}

// ── Advisory Lock metrics (Issue #686) ────────────────────────────────────────

/// Record successful advisory lock acquisition
pub fn record_advisory_lock_acquired(lock_id: i64) {
    m::counter!(
        "soroban_pulse_advisory_lock_acquired_total",
        "lock_id" => lock_id.to_string()
    )
    .increment(1);
}

/// Record advisory lock release
pub fn record_advisory_lock_released(lock_id: i64) {
    m::counter!(
        "soroban_pulse_advisory_lock_released_total",
        "lock_id" => lock_id.to_string()
    )
    .increment(1);
}

/// Record advisory lock acquisition retry
pub fn record_advisory_lock_retry(lock_id: i64) {
    m::counter!(
        "soroban_pulse_advisory_lock_retries_total",
        "lock_id" => lock_id.to_string()
    )
    .increment(1);
}

/// Record advisory lock acquisition timeout
pub fn record_advisory_lock_timeout(lock_id: i64) {
    m::counter!(
        "soroban_pulse_advisory_lock_timeouts_total",
        "lock_id" => lock_id.to_string()
    )
    .increment(1);
}

/// Record advisory lock acquisition error
pub fn record_advisory_lock_error(lock_id: i64) {
    m::counter!(
        "soroban_pulse_advisory_lock_errors_total",
        "lock_id" => lock_id.to_string()
    )
    .increment(1);
}

/// Record advisory lock release error
pub fn record_advisory_lock_release_error(lock_id: i64) {
    m::counter!(
        "soroban_pulse_advisory_lock_release_errors_total",
        "lock_id" => lock_id.to_string()
    )
    .increment(1);
}

// ── Webhook priority queue metrics ─────────────────────────────────────────────

/// Record a priority-queue dequeue with the observed wait time.
pub fn record_priority_dequeue(priority: &str, wait_ms: u64) {
    m::counter!(
        "soroban_pulse_webhook_priority_dequeued_total",
        "priority" => priority.to_string()
    )
    .increment(1);
    m::histogram!(
        "soroban_pulse_webhook_priority_wait_ms",
        "priority" => priority.to_string()
    )
    .record(wait_ms as f64);
}

/// Record a priority SLA violation (task waited longer than its priority allows).
pub fn record_priority_violation(priority: &str) {
    m::counter!(
        "soroban_pulse_webhook_priority_violations_total",
        "priority" => priority.to_string()
    )
    .increment(1);
}

// ── Webhook signing metrics ────────────────────────────────────────────────────

/// Record a webhook payload signing operation for a given key id.
pub fn record_webhook_signature_created(key_id: &str) {
    m::counter!(
        "soroban_pulse_webhook_signatures_created_total",
        "key_id" => key_id.to_string()
    )
    .increment(1);
}

/// Record a webhook signature verification result.
pub fn record_webhook_signature_verified(key_id: &str, success: bool) {
    m::counter!(
        "soroban_pulse_webhook_signature_verifications_total",
        "key_id" => key_id.to_string(),
        "result" => if success { "success" } else { "failure" }
    )
    .increment(1);
}

// ── HTTP caching metrics ───────────────────────────────────────────────────────

/// Record a conditional-request cache outcome for HTTP caching effectiveness.
pub fn record_http_cache_result(resource: &str, hit: bool) {
    m::counter!(
        "soroban_pulse_http_cache_results_total",
        "resource" => resource.to_string(),
        "result" => if hit { "hit" } else { "miss" }
    )
    .increment(1);
}

// ── Health check metrics ──────────────────────────────────────────────────────

/// Record successful PostgreSQL health check
pub fn record_postgres_health_ok(response_time_ms: u64) {
    m::counter!("soroban_pulse_postgres_health_checks_total", "status" => "ok").increment(1);
    m::histogram!("soroban_pulse_postgres_health_check_duration_ms").record(response_time_ms as f64);
}

/// Record degraded PostgreSQL health check
pub fn record_postgres_health_degraded(response_time_ms: u64) {
    m::counter!("soroban_pulse_postgres_health_checks_total", "status" => "degraded").increment(1);
    m::histogram!("soroban_pulse_postgres_health_check_duration_ms").record(response_time_ms as f64);
}

/// Record failed PostgreSQL health check
pub fn record_postgres_health_error(response_time_ms: u64) {
    m::counter!("soroban_pulse_postgres_health_checks_total", "status" => "error").increment(1);
    m::histogram!("soroban_pulse_postgres_health_check_duration_ms").record(response_time_ms as f64);
    record_error_by_kind("db");
}

/// Record successful RPC health check
pub fn record_rpc_health_ok(response_time_ms: u64) {
    m::counter!("soroban_pulse_rpc_health_checks_total", "status" => "ok").increment(1);
    m::histogram!("soroban_pulse_rpc_health_check_duration_ms").record(response_time_ms as f64);
}

/// Record failed RPC health check
pub fn record_rpc_health_error(response_time_ms: u64) {
    m::counter!("soroban_pulse_rpc_health_checks_total", "status" => "error").increment(1);
    m::histogram!("soroban_pulse_rpc_health_check_duration_ms").record(response_time_ms as f64);
}

/// Record a successful batch delivery of N events.
pub fn record_batch_delivered(count: u64) {
    m::counter!("soroban_pulse_batch_delivered_total").increment(count);
    m::counter!("soroban_pulse_batch_deliveries_total").increment(1);
}

/// Record a failed batch delivery attempt.
pub fn record_batch_delivery_failed() {
    m::counter!("soroban_pulse_batch_delivery_failures_total").increment(1);
}

/// Record a batch config update.
pub fn record_batch_config_updated() {
    m::counter!("soroban_pulse_batch_config_updates_total").increment(1);
}

// ── Issue #885: Conditional Request Handling ────────────────────────────

/// Record a 304 Not Modified response (bandwidth saved).
pub fn record_conditional_get_304() {
    m::counter!("soroban_pulse_conditional_get_304_total").increment(1);
}

/// Record bandwidth savings from conditional GETs.
pub fn record_conditional_get_bandwidth_saved(bytes: u64) {
    m::counter!("soroban_pulse_conditional_get_bandwidth_saved_bytes_total").increment(bytes);
}

/// Record ETag cache hit.
pub fn record_etag_cache_hit() {
    m::counter!("soroban_pulse_etag_cache_hits_total").increment(1);
}

/// Record ETag cache miss.
pub fn record_etag_cache_miss() {
    m::counter!("soroban_pulse_etag_cache_misses_total").increment(1);
}

// ── Issue #884: Subscription Pause/Resume ────────────────────────────────

/// Record subscription pause event.
pub fn record_subscription_paused() {
    m::counter!("soroban_pulse_subscriptions_paused_total").increment(1);
}

/// Record subscription resume event.
pub fn record_subscription_resumed() {
    m::counter!("soroban_pulse_subscriptions_resumed_total").increment(1);
}

/// Record auto-resume of paused subscriptions.
pub fn record_subscription_auto_resumed(count: u64) {
    m::counter!("soroban_pulse_subscriptions_auto_resumed_total").increment(count);
}

// ── Issue #882: Anomaly Detection Alerting ────────────────────────────────

/// Record anomaly detection configuration creation.
pub fn record_anomaly_detection_configured(count: u64) {
    m::counter!("soroban_pulse_anomaly_detection_configured_total").increment(count);
}

/// Record anomaly alerts retrieved.
pub fn record_anomaly_alerts_queried(count: u64) {
    m::counter!("soroban_pulse_anomaly_alerts_queried_total").increment(count);
}

/// Record anomaly alert acknowledged.
pub fn record_anomaly_alert_acknowledged() {
    m::counter!("soroban_pulse_anomaly_alerts_acknowledged_total").increment(1);
}

/// Record anomaly score observation.
pub fn record_anomaly_score(metric_name: &str, score: f64) {
    m::histogram!(
        "soroban_pulse_anomaly_score",
        "metric" => metric_name.to_string()
    )
    .record(score);
}

/// Record anomaly detection threshold crossing.
pub fn record_anomaly_threshold_crossed(metric_name: &str, severity: &str) {
    m::counter!(
        "soroban_pulse_anomaly_threshold_crossings_total",
        "metric" => metric_name.to_string(),
        "severity" => severity.to_string()
    )
    .increment(1);
}

// ── Issue #696: SLI / SLO dashboard metrics ────────────────────────────────

/// Set the rolling SLO completion ratio in `[0.0, 1.0]`.
///
/// A value of `1.0` means every sample in the window met the SLO target, while
/// `0.5` means half met it. Steady values != 1.0 across the window surface as
/// SLO completion gaps in the Grafana panel.
pub fn update_slo_completion_ratio(slo: &str, component: &str, ratio: f64) {
    m::gauge!(
        "soroban_pulse_slo_completion_ratio",
        "slo" => slo.to_string(),
        "component" => component.to_string()
    )
    .set(ratio.clamp(0.0, 1.0));
}

/// Set the fraction of the error budget remaining in `[0.0, 1.0]`.
///
/// `1.0` = full budget, `0.0` = exhausted. Values < 0.1 trigger the
/// `SLOErrorBudgetLow` Prometheus alert defined in `docs/alerts.yml`.
pub fn update_slo_error_budget_remaining(slo: &str, component: &str, ratio: f64) {
    m::gauge!(
        "soroban_pulse_slo_error_budget_remaining",
        "slo" => slo.to_string(),
        "component" => component.to_string()
    )
    .set(ratio.clamp(0.0, 1.0));
}

/// Set the SLO burn rate (0 == no consumption; 1 == on track to exhaust budget
/// exactly at end of window; > 2 == critical).
pub fn update_slo_burn_rate(slo: &str, component: &str, rate: f64) {
    let safe = if rate.is_finite() { rate.max(0.0) } else { 100.0 };
    m::gauge!(
        "soroban_pulse_slo_burn_rate",
        "slo" => slo.to_string(),
        "component" => component.to_string()
    )
    .set(safe);
}

/// Set the most recent SLI observation for an SLO (rendered in the SLI trend
/// line chart).
pub fn update_sli_current_value(slo: &str, component: &str, value: f64) {
    let safe = if value.is_finite() { value } else { 0.0 };
    m::gauge!(
        "soroban_pulse_sli_current_value",
        "slo" => slo.to_string(),
        "component" => component.to_string()
    )
    .set(safe);
}

/// Record an SLO status transition (Met → AtRisk → Breached). Used by the
/// Grafana alert panel and the `SLOStatusAtRisk` / `SLOStatusBreached` alert
/// rules.
pub fn record_slo_status_transition(slo: &str, status: &str) {
    m::counter!(
        "soroban_pulse_slo_evaluation_total",
        "slo" => slo.to_string(),
        "status" => status.to_string()
    )
    .increment(1);
}

// ── Issue #1248: Background job scheduler ───────────────────────────────────

/// Number of `JobScheduler` jobs that are still running.
pub fn set_background_jobs_alive(count: usize) {
    m::gauge!("soroban_pulse_background_jobs_alive").set(count as f64);
}

// ── Issue #630: Resource utilization metrics ────────────────────────────────

/// Update file descriptor count gauge
pub fn update_fd_count(count: u64) {
    m::gauge!("soroban_pulse_fd_count").set(count as f64);
}

/// Update disk I/O read bytes gauge
pub fn update_disk_read_bytes(bytes: u64) {
    m::gauge!("soroban_pulse_disk_read_bytes_total").set(bytes as f64);
}

/// Update disk I/O write bytes gauge
pub fn update_disk_write_bytes(bytes: u64) {
    m::gauge!("soroban_pulse_disk_write_bytes_total").set(bytes as f64);
}

/// Update disk read syscall count gauge
pub fn update_disk_syscalls_read(count: u64) {
    m::gauge!("soroban_pulse_disk_syscalls_read_total").set(count as f64);
}

/// Update disk write syscall count gauge
pub fn update_disk_syscalls_write(count: u64) {
    m::gauge!("soroban_pulse_disk_syscalls_write_total").set(count as f64);
}

/// Update process memory RSS gauge
pub fn update_memory_rss_bytes(bytes: u64) {
    m::gauge!("soroban_pulse_process_memory_rss_bytes").set(bytes as f64);
}

/// Update process memory VMS gauge
pub fn update_memory_vms_bytes(bytes: u64) {
    m::gauge!("soroban_pulse_process_memory_vms_bytes").set(bytes as f64);
}

/// Record a successful backup verification (Issue #894)
pub fn record_backup_verification_success() {
    m::counter!("soroban_pulse_backup_verification_success_total").increment(1);
}

/// Record a failed backup verification (Issue #894)
pub fn record_backup_verification_failure() {
    m::counter!("soroban_pulse_backup_verification_failure_total").increment(1);
}

/// Update backup size metrics (Issue #894)
pub fn update_backup_size_bytes(bytes: u64) {
    m::gauge!("soroban_pulse_backup_size_bytes").set(bytes as f64);
}

/// Update backup duration metrics in seconds (Issue #894)
pub fn update_backup_duration_seconds(duration: f64) {
    m::gauge!("soroban_pulse_backup_duration_seconds").set(duration);
}

/// Update restore duration metrics in seconds (Issue #894)
pub fn update_restore_duration_seconds(duration: f64) {
    m::gauge!("soroban_pulse_restore_duration_seconds").set(duration);
}

/// Record backup integrity verification (Issue #894)
pub fn record_backup_integrity_verified() {
    m::counter!("soroban_pulse_backup_integrity_verified_total").increment(1);
}

/// Record backup encryption verification (Issue #894)
pub fn record_backup_encryption_verified() {
    m::counter!("soroban_pulse_backup_encryption_verified_total").increment(1);
}

/// Record backup row count verification (Issue #894)
pub fn record_backup_row_count_verified() {
    m::counter!("soroban_pulse_backup_row_count_verified_total").increment(1);
}

/// Record distributed trace span created (Issue #895)
pub fn record_trace_span_created(span_name: &str) {
    m::counter!(
        "soroban_pulse_trace_spans_created_total",
        "span_name" => span_name.to_string()
    )
    .increment(1);
}

/// Record trace sampling decision (Issue #895)
pub fn record_trace_sampled(sampled: bool) {
    m::counter!(
        "soroban_pulse_trace_samples_total",
        "sampled" => if sampled { "true" } else { "false" }
    )
    .increment(1);
}

/// Update trace injection latency (Issue #895)
pub fn update_trace_injection_latency_ms(latency_ms: f64) {
    m::gauge!("soroban_pulse_trace_injection_latency_ms").set(latency_ms);
}

/// Record SLI latency percentile calculation (Issue #896)
pub fn record_sli_latency_percentile(percentile: &str, value_seconds: f64) {
    m::gauge!(
        "soroban_pulse_sli_latency_percentile",
        "percentile" => percentile.to_string()
    )
    .set(value_seconds);
}

/// Record error rate metric (Issue #896)
pub fn record_error_rate(endpoint: &str, rate: f64) {
    m::gauge!(
        "soroban_pulse_sli_error_rate",
        "endpoint" => endpoint.to_string()
    )
    .set(rate);
}

/// Record availability metric (Issue #896)
pub fn record_availability(endpoint: &str, availability: f64) {
    m::gauge!(
        "soroban_pulse_sli_availability",
        "endpoint" => endpoint.to_string()
    )
    .set(availability);
}

/// Record SLO budget burndown (Issue #896)
pub fn update_slo_budget_burndown(slo: &str, consumed: f64) {
    m::gauge!(
        "soroban_pulse_slo_budget_burndown",
        "slo" => slo.to_string()
    )
    .set(consumed);
}

/// Record alert fired (Issue #897)
pub fn record_alert_fired(alert_name: &str, severity: &str) {
    m::counter!(
        "soroban_pulse_alerts_fired_total",
        "alert_name" => alert_name.to_string(),
        "severity" => severity.to_string()
    )
    .increment(1);
}

/// Record alert resolved (Issue #897)
pub fn record_alert_resolved(alert_name: &str) {
    m::counter!(
        "soroban_pulse_alerts_resolved_total",
        "alert_name" => alert_name.to_string()
    )
    .increment(1);
}

/// Record alert silenced (Issue #897)
pub fn record_alert_silenced(alert_name: &str, duration_minutes: u64) {
    m::counter!(
        "soroban_pulse_alerts_silenced_total",
        "alert_name" => alert_name.to_string()
    )
    .increment(1);
    m::gauge!(
        "soroban_pulse_alert_silence_duration_minutes",
        "alert_name" => alert_name.to_string()
    )
    .set(duration_minutes as f64);
}

/// Update active alert count (Issue #897)
pub fn update_active_alerts_count(component: &str, count: u64) {
    m::gauge!(
        "soroban_pulse_active_alerts",
        "component" => component.to_string()
    )
    .set(count as f64);
}

// ── Issue #1260: Webhook dead-letter queue metrics ──────────────────────────

/// Update the current depth of the webhook dead-letter queue.
///
/// Published on every DLQ insert, delete, and replay so operators can alert on
/// a growing backlog of webhooks that exhausted all delivery retries.
pub fn update_webhook_dlq_depth(count: u64) {
    m::gauge!("soroban_pulse_webhook_dlq_depth").set(count as f64);
}

// ── Issue #1262: Derived metrics framework ──────────────────────────────────

/// Publish the current value of a user-defined derived metric.
///
/// The metric name is `soroban_pulse_derived_<name>` and carries a
/// `derived=true` label so derived series can be selected independently of
/// built-in metrics in Prometheus queries.
pub fn update_derived_metric(name: &str, value: f64) {
    let safe = if value.is_finite() { value } else { 0.0 };
    m::gauge!(
        "soroban_pulse_derived_metric",
        "name" => name.to_string(),
        "derived" => "true"
    )
    .set(safe);
}

/// Record how long a single derived metric evaluation took.
pub fn record_derived_metric_evaluation_duration(duration: std::time::Duration) {
    m::histogram!("soroban_pulse_derived_metrics_evaluation_duration_seconds")
        .record(duration.as_secs_f64());
}

/// Record a failed derived metric evaluation (query error, evaluator panic, ...).
pub fn record_derived_metric_evaluation_error() {
    m::counter!("soroban_pulse_derived_metrics_evaluation_errors_total").increment(1);
}

/// Record a derived metric definition that failed to parse.
pub fn record_derived_metric_parse_error() {
    m::counter!("soroban_pulse_derived_metrics_parse_errors_total").increment(1);
}

// ── Issue #1263: Incident correlation metrics ───────────────────────────────

/// Record an incident created by the correlation engine.
pub fn record_incident_created(kind: &str, severity: &str) {
    m::counter!(
        "soroban_pulse_incidents_created_total",
        "kind" => kind.to_string(),
        "severity" => severity.to_string()
    )
    .increment(1);
}

/// Record an incident resolved (manually or automatically).
pub fn record_incident_resolved(kind: &str) {
    m::counter!(
        "soroban_pulse_incidents_resolved_total",
        "kind" => kind.to_string()
    )
    .increment(1);
}

/// Update the number of currently open incidents.
pub fn update_open_incidents(count: u64) {
    m::gauge!("soroban_pulse_incidents_open").set(count as f64);
}

/// Record how long a correlation pass took.
pub fn record_correlation_duration(duration: std::time::Duration) {
    m::histogram!("soroban_pulse_correlation_duration_seconds")
        .record(duration.as_secs_f64());
}

/// Record a detector error encountered while gathering correlation inputs.
pub fn record_detector_error() {
    m::counter!("soroban_pulse_detector_errors_total").increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_init_metrics() {
        let handle = init_metrics();
        // The handle should be valid - we can't easily test the internal state
        // but we can at least verify it doesn't panic
        assert!(true);
    }

    #[test]
    fn test_record_events_indexed() {
        // This should not panic
        record_events_indexed(42);
        record_events_indexed(0);
        assert!(true);
    }

    #[test]
    fn test_update_current_ledger() {
        // This should not panic
        update_current_ledger(12345);
        update_current_ledger(0);
        assert!(true);
    }

    #[test]
    fn test_update_latest_ledger() {
        // This should not panic
        update_latest_ledger(67890);
        update_latest_ledger(0);
        assert!(true);
    }

    #[test]
    fn test_update_indexer_lag() {
        // This should not panic
        update_indexer_lag(100);
        update_indexer_lag(0);
        assert!(true);
    }

    #[test]
    fn test_record_rpc_error() {
        // This should not panic
        record_rpc_error();
        assert!(true);
    }

    #[test]
    fn test_record_validation_failure() {
        // This should not panic
        record_validation_failure();
        assert!(true);
    }

    #[test]
    fn test_record_http_request_duration() {
        // This should not panic
        let duration = Duration::from_millis(150);
        record_http_request_duration(duration, "GET", "/events", "200");
        record_http_request_duration(Duration::ZERO, "POST", "/health", "500");
        assert!(true);
    }

    #[tokio::test]
    async fn test_update_db_pool_metrics() {
        // Create a test pool
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .min_connections(1)
            .connect_lazy("postgres://localhost/test")
            .unwrap();

        // This should not panic
        update_db_pool_metrics(&pool);
        assert!(true);
    }

    // ── Issue #695: new custom dashboard panel metrics ───────────────────

    #[test]
    fn test_record_webhook_delivery_success() {
        record_webhook_delivery_success();
        assert!(true);
    }

    #[test]
    fn test_record_notification_delivery_success() {
        record_notification_delivery_success();
        assert!(true);
    }

    #[test]
    fn test_record_notification_delivery_failure() {
        record_notification_delivery_failure();
        assert!(true);
    }
}

/// Record a successful Prometheus remote write push
pub fn record_prometheus_remote_write_success() {
    m::counter!("soroban_pulse_prometheus_remote_write_success_total").increment(1);
}

/// Record a failed Prometheus remote write push
pub fn record_prometheus_remote_write_failure() {
    m::counter!("soroban_pulse_prometheus_remote_write_failures_total").increment(1);
}

/// Record Prometheus remote write endpoint health check OK
pub fn record_prometheus_remote_write_health_ok() {
    m::gauge!("soroban_pulse_prometheus_remote_write_health").set(1.0);
}

/// Record Prometheus remote write endpoint health check failure
pub fn record_prometheus_remote_write_health_fail() {
    m::gauge!("soroban_pulse_prometheus_remote_write_health").set(0.0);
}

/// Record EventBridge event submission success
pub fn record_eventbridge_put_events_success(count: u64) {
    m::counter!("soroban_pulse_eventbridge_put_events_success_total").increment(count);
}

/// Record EventBridge event submission failure
pub fn record_eventbridge_put_events_failure() {
    m::counter!("soroban_pulse_eventbridge_put_events_failures_total").increment(1);
}

/// Record EventBridge rule creation/update
pub fn record_eventbridge_rule_created() {
    m::counter!("soroban_pulse_eventbridge_rules_created_total").increment(1);
}

/// Record EventBridge rule deletion
pub fn record_eventbridge_rule_deleted() {
    m::counter!("soroban_pulse_eventbridge_rules_deleted_total").increment(1);
}

/// Update EventBridge active rules gauge
pub fn update_eventbridge_active_rules(count: u64) {
    m::gauge!("soroban_pulse_eventbridge_active_rules").set(count as f64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_contract_event_count() {
        update_contract_event_count("CABCDEF12345678901234567890123456789012345678901234567", 42);
        update_contract_event_count("CABCDEF12345678901234567890123456789012345678901234567", 0);
        assert!(true);
    }

    #[test]
    fn test_record_indexer_lag_observation() {
        record_indexer_lag_observation(0);
        record_indexer_lag_observation(1000);
        assert!(true);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn process_memory_bytes_is_nonzero_on_linux() {
        update_process_memory_bytes();
        // After updating, the gauge should have been set to a positive value.
        // We can't easily read back a gauge value from the metrics crate without
        // a recorder, so we verify the /proc/self/status parse succeeds instead.
        let status = std::fs::read_to_string("/proc/self/status").unwrap();
        let rss_kb: u64 = status
            .lines()
            .find(|l| l.starts_with("VmRSS:"))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse().ok())
            .expect("VmRSS not found in /proc/self/status");
        assert!(rss_kb > 0, "RSS should be non-zero");
    }
}
