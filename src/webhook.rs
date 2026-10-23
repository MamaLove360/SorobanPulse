use hmac::Mac;
use reqwest::Client;
use sqlx::Row;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn, Span};
use uuid::Uuid;

/// Validate that `url` is reachable and returns a 2xx response.
/// Used when creating or updating webhook notification channels (#503).
pub async fn validate_webhook_url(url: &str) -> Result<(), String> {
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| format!("failed to build HTTP client: {}", e))?;

    match client.head(url).send().await {
        Ok(resp) if resp.status().is_success() => Ok(()),
        Ok(resp) => Err(format!(
            "webhook URL returned non-2xx status: {}",
            resp.status()
        )),
        Err(e) => Err(format!("webhook URL is unreachable: {}", e)),
    }
}

use crate::{metrics, models::SorobanEvent, webhook_template::WebhookTemplate};
use serde_json::Value;


/// Transform webhook payload using a template (Issue #678).
/// Returns the transformed payload or the original event if template is invalid.
pub fn transform_webhook_payload(
    template: &str,
    event: &SorobanEvent,
) -> Result<Value, String> {
    let webhook_template = WebhookTemplate::new(template.to_string());

    // Validate template syntax
    webhook_template.validate()?;

    // Transform the event using the template
    let event_value = serde_json::to_value(event)
        .map_err(|e| format!("Failed to convert event to JSON: {}", e))?;

    webhook_template.transform(&event_value)
}

/// Sign a payload with HMAC-SHA256 and return the hex digest.
pub fn sign_payload(secret: &str, body: &[u8]) -> String {
    let mut mac = crate::hmac_util::hmac_sha256(secret.as_bytes());
    mac.update(body);
    let result = mac.finalize();
    hex::encode(result.into_bytes())
}

/// Evaluate the notification priority of an event using a JSONPath-style rule (Issue #492).
/// Returns the matched priority string, or `default_priority` if no rule is set or matches.
pub fn evaluate_priority<'a>(
    event: &SorobanEvent,
    rule_path: Option<&str>,
    rule_value: Option<&str>,
    rule_priority: Option<&'a str>,
    default_priority: &'a str,
) -> &'a str {
    if let (Some(path), Some(expected), Some(priority)) = (rule_path, rule_value, rule_priority) {
        let segments: Vec<&str> = path
            .trim_start_matches("$.")
            .split('.')
            .filter(|s| !s.is_empty())
            .collect();

        let mut current = &event.value;
        for segment in &segments {
            match current.get(segment) {
                Some(next) => current = next,
                None => return default_priority,
            }
        }

        if current.as_str() == Some(expected) {
            return priority;
        }
    }
    default_priority
}

/// Deliver a single event to the webhook URL with the default retry policy.
/// On final failure, insert into DLQ.
pub async fn deliver(
    client: Client,
    url: String,
    secret: Option<String>,
    event: SorobanEvent,
    pool: Option<&sqlx::PgPool>,
) {
    deliver_with_retry_policy(
        client,
        url,
        secret,
        event,
        pool,
        &crate::retry_policy::RetryPolicy::webhook_default(),
        "medium".to_string(),
    )
    .await
}

/// Deliver with custom retry policy and priority (Issues #474, #492).
/// The priority is included in the request payload and headers.
/// On success, a notification_acknowledgments record is inserted for escalation
/// tracking (Issue #493).
///
/// All parameters are owned so the resulting future is `'static` and safe to
/// pass to `tokio::spawn`.
///
/// See docs/adr/0003-webhook-retry-strategy.md for the design rationale.
pub async fn deliver_with_retry_policy(
    client: Client,
    url: String,
    secret: Option<String>,
    event: SorobanEvent,
    pool: Option<&sqlx::PgPool>,
    retry_policy: &crate::retry_policy::RetryPolicy,
    priority: String,
) {
    // Create a root span for the entire delivery lifecycle
    #[cfg(feature = "full-observability")]
    let root_span = tracing::info_span!(
        "webhook_delivery",
        webhook_url = %url,
        contract_id = %event.contract_id,
        tx_hash = %event.tx_hash,
        priority = %priority,
    );

    #[cfg(not(feature = "full-observability"))]
    let root_span = tracing::info_span!(
        "webhook_delivery",
        webhook_url = %url,
        contract_id = %event.contract_id,
    );

    let _enter = root_span.enter();

    // Check suppression list before attempting delivery (Issue #490)
    if let Some(pool) = pool {
        match sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM suppression_lists \
             WHERE target = $1 AND target_type = 'webhook' \
             AND (expires_at IS NULL OR expires_at > NOW())",
        )
        .bind(&url)
        .fetch_one(pool)
        .await
        {
            Ok(count) if count > 0 => {
                info!(url = %url, "Webhook URL suppressed, skipping delivery");
                crate::metrics::record_notification_suppressed();
                return;
            }
            _ => {}
        }
    }

    if let Some(pool) = pool {
        match endpoint_rate_limited(pool, &url).await {
            Ok(Some(next_retry_at)) => {
                enqueue_endpoint_retry(
                    pool,
                    &url,
                    &event,
                    &secret,
                    "endpoint rate limited",
                    next_retry_at,
                )
                .await;
                return;
            }
            Ok(None) => {}
            Err(e) => warn!(error = %e, url = %url, "Failed to check webhook endpoint rate limit"),
        }
    }

    let mut payload = match serde_json::to_value(&event) {
        Ok(value) => value,
        Err(e) => {
            error!(error = %e, "Failed to serialize event for webhook delivery");
            return;
        }
    };
    if let Some(obj) = payload.as_object_mut() {
        obj.insert("priority".to_string(), serde_json::json!(priority));
    }

    let body = match serde_json::to_vec(&payload) {
        Ok(b) => b,
        Err(e) => {
            error!(error = %e, "Failed to re-serialize webhook payload");
            return;
        }
    };

    let signature = secret.as_deref().map(|s| sign_payload(s, &body));
    let priority_owned = priority.clone();

    let log_pool = pool.cloned();
    let log_url = url.clone();
    let log_request_headers = {
        let mut headers = vec![
            ("Content-Type".to_string(), "application/json".to_string()),
            ("X-Notification-Priority".to_string(), priority_owned.clone()),
        ];
        if let Some(ref sig) = signature {
            headers.push(("X-Signature-256".to_string(), format!("sha256={sig}")));
        }
        headers
    };
    let log_request_body = payload.clone();
    let log_contract_id = event.contract_id.clone();
    let log_event_type = event.event_type.to_string();

    let result = retry_policy
        .execute_with_retry(|attempt| {
            let client = client.clone();
            let url = url.clone();
            let body = body.clone();
            let signature = signature.clone();
            let priority_header = priority_owned.clone();
            let log_pool = log_pool.clone();
            let log_url = log_url.clone();
            let log_request_headers = log_request_headers.clone();
            let log_request_body = log_request_body.clone();
            let log_contract_id = log_contract_id.clone();
            let log_event_type = log_event_type.clone();

            async move {
                // Create a child span for this attempt
                #[cfg(feature = "full-observability")]
                let attempt_span = tracing::info_span!(
                    "webhook_attempt",
                    attempt = attempt,
                    parent: &tracing::Span::current(),
                );

                #[cfg(not(feature = "full-observability"))]
                let attempt_span = tracing::info_span!(
                    "webhook_attempt",
                    attempt = attempt,
                );

                let _enter = attempt_span.enter();

                let mut req = client
                    .post(&url)
                    .header("Content-Type", "application/json")
                    .header("X-Notification-Priority", priority_header)
                    .body(body);

                if let Some(ref sig) = signature {
                    req = req.header("X-Signature-256", format!("sha256={sig}"));
                }

                // Create a span for the HTTP send
                #[cfg(feature = "full-observability")]
                let send_span = tracing::info_span!("webhook_http_send");

                #[cfg(not(feature = "full-observability"))]
                let send_span = tracing::info_span!("webhook_http_send");

                let _send_enter = send_span.enter();

                let started_at = std::time::Instant::now();
                let send_result = req.send().await;
                let duration_ms = started_at.elapsed().as_millis() as i64;
                drop(_send_enter);

                // Record the notification delivery cost for this attempt,
                // attributed to the webhook component (Issue #1264).
                crate::costs::calculator::record_notification_cost(
                    "webhook",
                    duration_ms as u64,
                );

                let outcome = match send_result {
                    Ok(resp) if resp.status().is_success() => {
                        info!(
                            url = %url,
                            contract_id = %event.contract_id,
                            attempt = attempt,
                            priority = %event.event_type,
                            duration_ms = duration_ms,
                            "Webhook delivered successfully"
                        );
                        crate::metrics::record_webhook_delivery_success();
                        let status = resp.status().as_u16() as i32;
                        let body_text = resp.text().await.unwrap_or_default();
                        (Ok(()), Some(status), body_text)
                    }
                    Ok(resp) => {
                        let status = resp.status().as_u16() as i32;
                        let body_text = resp.text().await.unwrap_or_default();
                        let error_msg = format!("HTTP {status}: {body_text}");
                        (Err(error_msg), Some(status), body_text)
                    }
                    Err(e) => (Err(format!("Request error: {}", e)), None, String::new()),
                };

                // Best-effort request/response log, spawned so it never
                // slows down delivery/retry timing (Issue #937).
                if let Some(pool) = log_pool {
                    let response_body: Option<Value> = if outcome.2.is_empty() {
                        None
                    } else {
                        Some(
                            serde_json::from_str(&outcome.2)
                                .unwrap_or_else(|_| serde_json::json!({"raw": outcome.2})),
                        )
                    };
                    let response_status = outcome.1;
                    let request_headers = log_request_headers.clone();
                    let request_body = log_request_body.clone();
                    let contract_id = log_contract_id.clone();
                    let event_type = log_event_type.clone();
                    let url_for_log = log_url.clone();
                    tokio::spawn(async move {
                        if let Err(e) = crate::webhook_logging::log_exchange(
                            &pool,
                            crate::webhook_logging::WebhookLogEntry {
                                url: &url_for_log,
                                request_headers: &request_headers,
                                request_body: &request_body,
                                response_status,
                                response_body: response_body.as_ref(),
                                duration_ms,
                                contract_id: Some(&contract_id),
                                event_type: Some(&event_type),
                            },
                        )
                        .await
                        {
                            warn!(error = %e, "Failed to store webhook request/response log");
                        }
                    });
                }

                outcome.0
            }
        })
        .await;

    match result {
        Ok(()) => {
            if let Some(pool) = pool {
                record_endpoint_success(pool, &url).await;
            }
            // Record the notification for escalation tracking (Issue #493).
            if let Some(pool) = pool {
                let notification_id = Uuid::new_v4();
                if let Err(e) = sqlx::query(
                    "INSERT INTO notification_acknowledgments \
                     (id, channel, event_contract_id, event_type, priority, status) \
                     VALUES ($1, 'webhook', $2, $3, $4, 'pending')",
                )
                .bind(notification_id)
                .bind(&event.contract_id)
                .bind(&event.event_type)
                .bind(priority)
                .execute(pool)
                .await
                {
                    warn!(error = %e, "Failed to record notification for escalation tracking");
                }
            }
        }
        Err(error_msg) => {
            error!(
                url = %url,
                contract_id = %event.contract_id,
                error = %error_msg,
                max_attempts = retry_policy.max_attempts,
                "Webhook delivery failed after all retries"
            );

            #[cfg(feature = "full-observability")]
            metrics::record_webhook_trace_error();

            if let Some(pool) = pool {
                let next_retry = record_endpoint_failure(pool, &url).await;
                let payload_val = serde_json::to_value(&event).unwrap_or(serde_json::json!({}));

                if let Err(e) = sqlx::query(
                    "INSERT INTO webhook_failures \
                     (url, payload, attempts, last_error, next_retry_at) \
                     VALUES ($1, $2, $3, $4, $5)",
                )
                .bind(&url)
                .bind(payload_val)
                .bind(retry_policy.max_attempts as i32)
                .bind(&error_msg)
                .bind(next_retry)
                .execute(pool)
                .await
                {
                    error!(error = %e, "Failed to insert webhook failure into DLQ");
                }

                // Move the exhausted delivery into the webhook DLQ (Issue #1260).
                if let Err(e) = enqueue_dlq(pool, None, &event, &error_msg).await {
                    error!(error = %e, url = %url, "Failed to insert webhook into DLQ");
                }
            }

            metrics::record_webhook_failure();
        }
    }
}

async fn endpoint_rate_limited(
    pool: &sqlx::PgPool,
    url: &str,
) -> Result<Option<chrono::DateTime<chrono::Utc>>, sqlx::Error> {
    let row = sqlx::query(
        "INSERT INTO rate_limit_endpoints (endpoint_url, window_start, window_count) \
         VALUES ($1, NOW(), 1) \
         ON CONFLICT (endpoint_url) DO UPDATE SET \
             window_start = CASE \
                 WHEN rate_limit_endpoints.window_start < NOW() - INTERVAL '1 minute' THEN NOW() \
                 ELSE rate_limit_endpoints.window_start END, \
             window_count = CASE \
                 WHEN rate_limit_endpoints.window_start < NOW() - INTERVAL '1 minute' THEN 1 \
                 ELSE rate_limit_endpoints.window_count + 1 END, \
             updated_at = NOW() \
         RETURNING per_minute_limit, window_count, backoff_until",
    )
    .bind(url)
    .fetch_one(pool)
    .await?;

    let limit: i32 = row.try_get("per_minute_limit")?;
    let count: i32 = row.try_get("window_count")?;
    let backoff_until: Option<chrono::DateTime<chrono::Utc>> = row.try_get("backoff_until")?;
    let now = chrono::Utc::now();
    if let Some(backoff_until) = backoff_until {
        if backoff_until > now {
            return Ok(Some(backoff_until));
        }
    }
    if count > limit {
        return Ok(Some(now + chrono::Duration::seconds(60)));
    }
    Ok(None)
}

async fn record_endpoint_success(pool: &sqlx::PgPool, url: &str) {
    if let Err(e) = sqlx::query(
        "UPDATE rate_limit_endpoints \
         SET consecutive_failures = 0, health_status = 'healthy', \
             backoff_until = NULL, updated_at = NOW() \
         WHERE endpoint_url = $1",
    )
    .bind(url)
    .execute(pool)
    .await
    {
        warn!(error = %e, url = %url, "Failed to record webhook endpoint success");
    }
}

async fn record_endpoint_failure(
    pool: &sqlx::PgPool,
    url: &str,
) -> chrono::DateTime<chrono::Utc> {
    let next_retry = chrono::Utc::now() + chrono::Duration::seconds(60);
    if let Err(e) = sqlx::query(
        "UPDATE rate_limit_endpoints \
         SET consecutive_failures = consecutive_failures + 1, \
             health_status = CASE \
                 WHEN consecutive_failures + 1 >= 3 THEN 'unhealthy' \
                 ELSE 'degraded' END, \
             backoff_until = NOW() + make_interval( \
                 secs => LEAST(900, POWER(2, LEAST(consecutive_failures + 1, 10))::int)), \
             updated_at = NOW() \
         WHERE endpoint_url = $1",
    )
    .bind(url)
    .execute(pool)
    .await
    {
        warn!(error = %e, url = %url, "Failed to record webhook endpoint failure");
    }
    next_retry
}

async fn enqueue_endpoint_retry(
    pool: &sqlx::PgPool,
    url: &str,
    event: &SorobanEvent,
    secret: &Option<String>,
    reason: &str,
    next_retry_at: chrono::DateTime<chrono::Utc>,
) {
    let payload = serde_json::to_value(event).unwrap_or(serde_json::json!({}));
    let secret_hash = secret.as_deref().map(|s| {
        use sha2::{Digest, Sha256};
        format!("{:x}", Sha256::digest(s.as_bytes()))
    });
    if let Err(e) = sqlx::query(
        "INSERT INTO webhook_retry_queue \
         (url, payload, secret_hash, attempt, max_attempts, next_retry_at, last_error, status) \
         VALUES ($1, $2, $3, 0, 5, $4, $5, 'pending')",
    )
    .bind(url)
    .bind(payload)
    .bind(secret_hash)
    .bind(next_retry_at)
    .bind(reason)
    .execute(pool)
    .await
    {
        error!(error = %e, url = %url, "Failed to enqueue endpoint-specific webhook retry");
    }
}

/// Deliver with failover: if the primary URL fails, attempt the failover URL (#499).
/// Returns true if delivered (primary or failover), false if both failed.
pub async fn deliver_with_failover(
    client: Client,
    primary_url: String,
    primary_secret: Option<String>,
    failover_url: Option<String>,
    failover_secret: Option<String>,
    event: SorobanEvent,
    pool: Option<&sqlx::PgPool>,
    retry_policy: &crate::retry_policy::RetryPolicy,
) -> bool {
    let body = match serde_json::to_vec(&event) {
        Ok(b) => b,
        Err(e) => {
            error!(error = %e, "Failed to serialize event for webhook delivery");
            return false;
        }
    };

    let primary_sig = primary_secret.as_deref().map(|s| sign_payload(s, &body));

    let primary_result = retry_policy.execute_with_retry(|attempt| {
        let client = client.clone();
        let url = primary_url.clone();
        let body = body.clone();
        let sig = primary_sig.clone();
        async move {
            let mut req = client
                .post(&url)
                .header("Content-Type", "application/json")
                .body(body);
            if let Some(ref s) = sig {
                req = req.header("X-Signature-256", format!("sha256={s}"));
            }
            match req.send().await {
                Ok(resp) if resp.status().is_success() => {
                    info!(url = %url, attempt = attempt, "Webhook delivered (primary)");
                    crate::metrics::record_webhook_delivery_success();
                    Ok(())
                }
                Ok(resp) => Err(format!("HTTP {}", resp.status())),
                Err(e) => Err(format!("Request error: {e}")),
            }
        }
    }).await;

    if primary_result.is_ok() {
        return true;
    }

    warn!(
        primary_url = %primary_url,
        "Primary webhook delivery failed, attempting failover"
    );

    // Attempt failover if configured
    if let Some(f_url) = failover_url {
        metrics::record_notification_failover("webhook");
        let f_sig = failover_secret.as_deref().map(|s| sign_payload(s, &body));

        let failover_result = retry_policy.execute_with_retry(|attempt| {
            let client = client.clone();
            let url = f_url.clone();
            let body = body.clone();
            let sig = f_sig.clone();
            async move {
                let mut req = client
                    .post(&url)
                    .header("Content-Type", "application/json")
                    .body(body);
                if let Some(ref s) = sig {
                    req = req.header("X-Signature-256", format!("sha256={s}"));
                }
                match req.send().await {
                    Ok(resp) if resp.status().is_success() => {
                        info!(url = %url, attempt = attempt, "Webhook delivered (failover)");
                        crate::metrics::record_webhook_delivery_success();
                        Ok(())
                    }
                    Ok(resp) => Err(format!("HTTP {}", resp.status())),
                    Err(e) => Err(format!("Request error: {e}")),
                }
            }
        }).await;

        if failover_result.is_ok() {
            return true;
        }
        error!(failover_url = %f_url, "Failover webhook delivery also failed");
    }

    // Record DLQ and metric
    if let Some(pool) = pool {
        let payload = serde_json::to_value(&event).unwrap_or(serde_json::json!({}));
        let next_retry = chrono::Utc::now() + chrono::Duration::seconds(60);
        if let Err(e) = sqlx::query(
            "INSERT INTO webhook_failures (url, payload, attempts, last_error, next_retry_at)
             VALUES ($1, $2, $3, $4, $5)"
        )
        .bind(&primary_url)
        .bind(payload)
        .bind(retry_policy.max_attempts as i32)
        .bind("Primary and failover both failed")
        .bind(next_retry)
        .execute(pool)
        .await
        {
            error!(error = %e, "Failed to insert webhook failure into DLQ");
        }

        // Move the exhausted delivery into the webhook DLQ (Issue #1260).
        if let Err(e) = enqueue_dlq(pool, None, &event, "Primary and failover both failed").await {
            error!(error = %e, "Failed to insert webhook into DLQ");
        }
    }
    metrics::record_webhook_failure();
    false
}

/// Insert an exhausted webhook delivery into the `webhook_dlq` table and
/// refresh the DLQ depth gauge (Issue #1260).
pub async fn enqueue_dlq(
    pool: &sqlx::PgPool,
    subscription_id: Option<Uuid>,
    event: &SorobanEvent,
    last_error: &str,
) -> Result<Uuid, sqlx::Error> {
    let payload = serde_json::to_value(event).unwrap_or(serde_json::json!({}));
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO webhook_dlq \
         (subscription_id, event_id, payload, last_error, failed_at) \
         VALUES ($1, $2, $3, $4, NOW()) \
         RETURNING id",
    )
    .bind(subscription_id)
    .bind(&event.tx_hash)
    .bind(payload)
    .bind(last_error)
    .fetch_one(pool)
    .await?;

    refresh_dlq_depth(pool).await;
    Ok(id)
}

/// Recompute the `soroban_pulse_webhook_dlq_depth` gauge from the table.
pub async fn refresh_dlq_depth(pool: &sqlx::PgPool) {
    match sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM webhook_dlq WHERE replayed_at IS NULL",
    )
    .fetch_one(pool)
    .await
    {
        Ok(depth) => metrics::set_webhook_dlq_depth(depth),
        Err(e) => warn!(error = %e, "Failed to compute webhook DLQ depth"),
    }
}

/// Re-enqueue a single DLQ row into the normal delivery queue and mark it
/// replayed (Issue #1260). Returns `Ok(false)` if the row does not exist or
/// has already been replayed.
pub async fn replay_dlq_entry(pool: &sqlx::PgPool, id: Uuid) -> Result<bool, sqlx::Error> {
    let row = sqlx::query(
        "SELECT payload, last_error FROM webhook_dlq \
         WHERE id = $1 AND replayed_at IS NULL",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(false);
    };

    let payload: Value = row.try_get("payload")?;
    let last_error: Option<String> = row.try_get("last_error").ok();
    let event: SorobanEvent = match serde_json::from_value(payload) {
        Ok(event) => event,
        Err(e) => {
            warn!(error = %e, dlq_id = %id, "Failed to deserialize DLQ payload");
            return Ok(false);
        }
    };

    // Re-enter the normal delivery queue.
    enqueue_endpoint_retry(
        pool,
        &event.contract_id,
        &event,
        &None,
        last_error.as_deref().unwrap_or("replayed from DLQ"),
        chrono::Utc::now(),
    )
    .await;

    sqlx::query("UPDATE webhook_dlq SET replayed_at = NOW() WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;

    refresh_dlq_depth(pool).await;
    Ok(true)
}

/// Bulk replay of DLQ entries, optionally filtered by `subscription_id`
/// (Issue #1260). Returns the number of entries replayed.
pub async fn replay_dlq_bulk(
    pool: &sqlx::PgPool,
    subscription_id: Option<Uuid>,
) -> Result<u64, sqlx::Error> {
    let ids: Vec<Uuid> = match subscription_id {
        Some(sub_id) => {
            sqlx::query_scalar(
                "SELECT id FROM webhook_dlq \
                 WHERE replayed_at IS NULL AND subscription_id = $1",
            )
            .bind(sub_id)
            .fetch_all(pool)
            .await?
        }
        None => {
            sqlx::query_scalar("SELECT id FROM webhook_dlq WHERE replayed_at IS NULL")
                .fetch_all(pool)
                .await?
        }
    };

    let mut replayed = 0u64;
    for id in ids {
        match replay_dlq_entry(pool, id).await {
            Ok(true) => replayed += 1,
            Ok(false) => {}
            Err(e) => warn!(error = %e, dlq_id = %id, "Failed to replay DLQ entry"),
        }
    }

    refresh_dlq_depth(pool).await;
    Ok(replayed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn mock_event(contract_id: &str) -> SorobanEvent {
        SorobanEvent {
            contract_id: contract_id.to_string(),
            event_type: "contract".to_string(),
            tx_hash: "abc123".to_string(),
            ledger: 100,
            ledger_closed_at: "2026-06-25T00:00:00Z".to_string(),
            ledger_hash: None,
            in_successful_call: true,
            value: json!({"amount": "100", "action": "transfer"}),
            topic: None,
            tenant_id: None,
        }
    }

    #[test]
    fn test_sign_payload_produces_consistent_hex() {
        let sig1 = sign_payload("mysecret", b"hello world");
        let sig2 = sign_payload("mysecret", b"hello world");
        assert_eq!(sig1, sig2);
        assert_eq!(sig1.len(), 64);
    }

    #[test]
    fn test_sign_payload_different_secrets_differ() {
        let sig1 = sign_payload("secret1", b"payload");
        let sig2 = sign_payload("secret2", b"payload");
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn test_sign_payload_known_value() {
        let sig = sign_payload("key", b"test");
        assert_eq!(
            sig,
            "02afb56304902c656fcb737cdd03de6205bb6d401da2812efd9b2d36a08af159"
        );
    }

    #[test]
    fn test_evaluate_priority_no_rule_returns_default() {
        let event = mock_event("C1");
        let p = evaluate_priority(&event, None, None, None, "medium");
        assert_eq!(p, "medium");
    }

    #[test]
    fn test_evaluate_priority_rule_matches() {
        let event = mock_event("C1");
        // event.value = {"amount": "100", "action": "transfer"}
        let p = evaluate_priority(
            &event,
            Some("$.action"),
            Some("transfer"),
            Some("critical"),
            "medium",
        );
        assert_eq!(p, "critical");
    }

    #[test]
    fn test_evaluate_priority_rule_no_match_returns_default() {
        let event = mock_event("C1");
        let p = evaluate_priority(
            &event,
            Some("$.action"),
            Some("mint"),
            Some("critical"),
            "low",
        );
        assert_eq!(p, "low");
    }

    #[test]
    fn test_evaluate_priority_missing_path_returns_default() {
        let event = mock_event("C1");
        let p = evaluate_priority(
            &event,
            Some("$.nonexistent.nested"),
            Some("value"),
            Some("high"),
            "medium",
        );
        assert_eq!(p, "medium");
    }
}
