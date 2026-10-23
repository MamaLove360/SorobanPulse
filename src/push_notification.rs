//! Issue #620 & #839: Push notification support (FCM, APNs, and Web Push).
//!
//! Sends push notifications to mobile and web clients when events matching
//! a subscription's filters are detected.
//!
//! # Configuration (environment variables)
//! - `FCM_SERVER_KEY` — Firebase Cloud Messaging server key (legacy HTTP API).
//! - `APNS_AUTH_KEY_PATH` — Path to APNs .p8 auth key file.
//! - `APNS_KEY_ID` — APNs key ID (10-character string).
//! - `APNS_TEAM_ID` — Apple developer team ID.
//! - `APNS_BUNDLE_ID` — App bundle ID used as APNs topic.
//! - `APNS_PRODUCTION` — Set to "true" for the production APNs endpoint.
//! - `VAPID_PUBLIC_KEY` — VAPID public key for Web Push (base64url-encoded).
//! - `VAPID_PRIVATE_KEY` — VAPID private key for Web Push (base64url-encoded).
//! - `VAPID_SUBJECT` — VAPID subject (mailto: or https: URI).
//! - `PUSH_MAX_RETRIES` — Maximum delivery retries (default: 5).
//! - `PUSH_BASE_DELAY_SECS` — Base delay for exponential backoff (default: 2).

use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::time::Duration;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::metrics;

/// Device/platform type for a push token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeviceType {
    Android,
    Ios,
    Web,
}

impl DeviceType {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "android" => Some(Self::Android),
            "ios" => Some(Self::Ios),
            "web" => Some(Self::Web),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Android => "android",
            Self::Ios => "ios",
            Self::Web => "web",
        }
    }
}

// ---------------------------------------------------------------------------
// FCM (Firebase Cloud Messaging) — legacy HTTP API
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct FcmConfig {
    pub server_key: String,
}

impl FcmConfig {
    pub fn from_env() -> Option<Self> {
        std::env::var("FCM_SERVER_KEY").ok().map(|k| Self { server_key: k })
    }
}

/// Send a push notification via FCM to a device token.
/// Returns `true` on success, `false` for an invalid/expired token (caller
/// should clean it up), and an `Err` for transient failures.
pub async fn fcm_send(
    client: &Client,
    config: &FcmConfig,
    token: &str,
    title: &str,
    body: &str,
    data: Option<&Value>,
) -> Result<bool, String> {
    let mut payload = json!({
        "to": token,
        "notification": {
            "title": title,
            "body": body,
        }
    });

    if let Some(d) = data {
        payload["data"] = d.clone();
    }

    let resp = client
        .post("https://fcm.googleapis.com/fcm/send")
        .header("Authorization", format!("key={}", config.server_key))
        .header("Content-Type", "application/json")
        .json(&payload)
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .map_err(|e| format!("FCM request failed: {e}"))?;

    let status = resp.status();
    if status == 401 {
        return Err("FCM: unauthorized — check FCM_SERVER_KEY".to_string());
    }

    let body_text = resp.text().await.unwrap_or_default();
    let parsed: Value = serde_json::from_str(&body_text)
        .unwrap_or_else(|_| json!({"error": body_text}));

    // FCM returns failure=1 with error "NotRegistered" for stale tokens.
    if parsed["failure"].as_i64().unwrap_or(0) > 0 {
        let err = parsed["results"][0]["error"]
            .as_str()
            .unwrap_or("unknown");
        if matches!(err, "NotRegistered" | "InvalidRegistration") {
            return Ok(false); // invalid token — caller should remove it
        }
        return Err(format!("FCM delivery failed: {err}"));
    }

    Ok(true)
}

// ---------------------------------------------------------------------------
// APNs (Apple Push Notification service) — token-based HTTP/2
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct ApnsConfig {
    pub auth_key: String,
    pub key_id: String,
    pub team_id: String,
    pub bundle_id: String,
    pub is_production: bool,
}

impl ApnsConfig {
    pub fn from_env() -> Option<Self> {
        let key_path = std::env::var("APNS_AUTH_KEY_PATH").ok()?;
        let auth_key = std::fs::read_to_string(&key_path).ok()?;
        let key_id = std::env::var("APNS_KEY_ID").ok()?;
        let team_id = std::env::var("APNS_TEAM_ID").ok()?;
        let bundle_id = std::env::var("APNS_BUNDLE_ID").ok()?;
        let is_production = std::env::var("APNS_PRODUCTION")
            .map(|v| v.to_ascii_lowercase() == "true")
            .unwrap_or(false);
        Some(Self {
            auth_key,
            key_id,
            team_id,
            bundle_id,
            is_production,
        })
    }

    pub fn endpoint(&self) -> &'static str {
        if self.is_production {
            "https://api.push.apple.com"
        } else {
            "https://api.sandbox.push.apple.com"
        }
    }
}

/// Build a minimal JWT for APNs token-based auth.
/// The JWT is valid for 1 hour; callers should cache and reuse it.
pub fn build_apns_jwt(config: &ApnsConfig) -> Result<String, String> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| format!("time error: {e}"))?
        .as_secs();

    let header = URL_SAFE_NO_PAD.encode(
        serde_json::to_string(&json!({"alg":"ES256","kid":config.key_id}))
            .map_err(|e| e.to_string())?,
    );
    let claims = URL_SAFE_NO_PAD.encode(
        serde_json::to_string(&json!({"iss":config.team_id,"iat":now}))
            .map_err(|e| e.to_string())?,
    );

    // NOTE: Real ES256 signing requires a proper ECDSA library such as `p256`.
    // Here we emit a placeholder signature; integrate `p256` + `jwt-compact`
    // (or equivalent) to produce a verifiable token in production.
    let placeholder_sig = URL_SAFE_NO_PAD.encode(b"placeholder-signature");

    Ok(format!("{header}.{claims}.{placeholder_sig}"))
}

/// Send a push notification via APNs.
/// Returns `true` on success, `false` for an invalid/expired device token.
pub async fn apns_send(
    client: &Client,
    config: &ApnsConfig,
    device_token: &str,
    title: &str,
    body: &str,
    jwt: &str,
) -> Result<bool, String> {
    let payload = json!({
        "aps": {
            "alert": {
                "title": title,
                "body": body,
            },
            "sound": "default",
        }
    });

    let url = format!("{}/3/device/{device_token}", config.endpoint());

    let resp = client
        .post(&url)
        .header("authorization", format!("bearer {jwt}"))
        .header("apns-topic", &config.bundle_id)
        .header("apns-push-type", "alert")
        .json(&payload)
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .map_err(|e| format!("APNs request failed: {e}"))?;

    let status = resp.status();
    if status.is_success() {
        return Ok(true);
    }

    let body_text = resp.text().await.unwrap_or_default();
    let parsed: Value = serde_json::from_str(&body_text)
        .unwrap_or_else(|_| json!({"reason": body_text}));
    let reason = parsed["reason"].as_str().unwrap_or("unknown");

    if matches!(
        reason,
        "BadDeviceToken" | "Unregistered" | "DeviceTokenNotForTopic"
    ) {
        return Ok(false); // invalid/expired token
    }

    Err(format!("APNs error {status}: {reason}"))
}

// ---------------------------------------------------------------------------
// Web Push (VAPID / RFC 8030)
// ---------------------------------------------------------------------------

/// Web Push subscription info following the W3C Push API specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebPushSubscription {
    pub endpoint: String,
    pub keys: WebPushKeys,
}

/// Encryption keys for Web Push (p256dh and auth).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebPushKeys {
    pub p256dh: String,
    pub auth: String,
}

/// VAPID configuration for Web Push notifications.
#[derive(Debug, Clone)]
pub struct VapidConfig {
    pub public_key: String,
    pub private_key: String,
    pub subject: String,
}

impl VapidConfig {
    pub fn from_env() -> Option<Self> {
        let public_key = std::env::var("VAPID_PUBLIC_KEY").ok()?;
        let private_key = std::env::var("VAPID_PRIVATE_KEY").ok()?;
        let subject = std::env::var("VAPID_SUBJECT").ok()?;
        Some(Self {
            public_key,
            private_key,
            subject,
        })
    }
}

/// Build a VAPID authorization header value.
///
/// This creates the `vapid t=<jwt>,k=<public_key>` authorization header
/// required for Web Push. The JWT claims include the audience (push service
/// origin) and subject (contact URI).
pub fn build_vapid_auth_header(config: &VapidConfig, endpoint: &str) -> Result<String, String> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

    let audience = url::Url::parse(endpoint)
        .map_err(|e| format!("invalid push endpoint URL: {e}"))?;
    let origin = format!(
        "{}://{}",
        audience.scheme(),
        audience.host_str().unwrap_or("localhost")
    );

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| format!("time error: {e}"))?
        .as_secs();

    let header = URL_SAFE_NO_PAD.encode(
        serde_json::to_string(&json!({"typ":"JWT","alg":"ES256"}))
            .map_err(|e| e.to_string())?,
    );
    let claims = URL_SAFE_NO_PAD.encode(
        serde_json::to_string(&json!({
            "aud": origin,
            "exp": now + 86400,
            "sub": config.subject,
        }))
        .map_err(|e| e.to_string())?,
    );

    // NOTE: Real ES256 signing requires an ECDSA library (e.g. `p256`).
    // Placeholder signature for the framework; replace with actual signing
    // in production.
    let placeholder_sig = URL_SAFE_NO_PAD.encode(b"vapid-placeholder-signature");
    let jwt = format!("{header}.{claims}.{placeholder_sig}");

    Ok(format!("vapid t={jwt},k={}", config.public_key))
}

/// Send a Web Push notification via the standard Web Push protocol (RFC 8030).
///
/// Returns `true` on success, `false` for an expired/invalid subscription
/// (HTTP 404 or 410), and `Err` for transient failures.
pub async fn web_push_send(
    client: &Client,
    config: &VapidConfig,
    subscription: &WebPushSubscription,
    title: &str,
    body: &str,
    data: Option<&Value>,
) -> Result<bool, String> {
    let auth_header = build_vapid_auth_header(config, &subscription.endpoint)?;

    let payload = json!({
        "title": title,
        "body": body,
        "data": data,
    });
    let payload_bytes = serde_json::to_vec(&payload).map_err(|e| e.to_string())?;

    let resp = client
        .post(&subscription.endpoint)
        .header("Authorization", &auth_header)
        .header("Content-Type", "application/json")
        .header("TTL", "86400")
        .body(payload_bytes)
        .timeout(Duration::from_secs(15))
        .send()
        .await
        .map_err(|e| format!("Web Push request failed: {e}"))?;

    let status = resp.status();
    if status.is_success() || status.as_u16() == 201 {
        return Ok(true);
    }

    // HTTP 404 or 410 (Gone) means the subscription is no longer valid.
    if status.as_u16() == 404 || status.as_u16() == 410 {
        return Ok(false);
    }

    let body_text = resp.text().await.unwrap_or_default();
    Err(format!("Web Push error {status}: {body_text}"))
}

// ---------------------------------------------------------------------------
// Exponential backoff retry logic (Issue #839)
// ---------------------------------------------------------------------------

/// Configuration for push notification retry behavior.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of delivery attempts before marking as permanently failed.
    pub max_retries: u32,
    /// Base delay in seconds for exponential backoff (delay = base * 2^attempt).
    pub base_delay_secs: u64,
    /// Maximum delay cap in seconds.
    pub max_delay_secs: u64,
}

impl RetryConfig {
    pub fn from_env() -> Self {
        let max_retries = std::env::var("PUSH_MAX_RETRIES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);
        let base_delay_secs = std::env::var("PUSH_BASE_DELAY_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2);
        Self {
            max_retries,
            base_delay_secs,
            max_delay_secs: 3600, // 1 hour cap
        }
    }

    /// Compute the next retry delay using exponential backoff with jitter.
    ///
    /// Returns `None` if `attempt` has exceeded `max_retries`.
    pub fn next_delay(&self, attempt: u32) -> Option<Duration> {
        if attempt >= self.max_retries {
            return None;
        }
        let exp = 2u64.saturating_pow(attempt);
        let delay_secs = self.base_delay_secs.saturating_mul(exp).min(self.max_delay_secs);
        // Add deterministic jitter: 10% of delay based on attempt number.
        let jitter = delay_secs / 10;
        Some(Duration::from_secs(delay_secs + jitter))
    }

    /// Whether a given attempt number has exhausted all retries.
    pub fn is_exhausted(&self, attempt: u32) -> bool {
        attempt >= self.max_retries
    }
}

// ---------------------------------------------------------------------------
// Delivery analytics (Issue #839)
// ---------------------------------------------------------------------------

/// In-memory push notification delivery analytics tracker.
///
/// Aggregates delivery statistics per device type for monitoring and the
/// analytics summary endpoint.
///
/// Issue #993: uses `metrics::SafeCounter` instead of raw `AtomicU64` so a
/// long-running instance saturates (and emits an overflow-detection metric)
/// rather than silently wrapping back to a small number.
#[derive(Debug)]
pub struct DeliveryAnalytics {
    pub total_sent: crate::metrics::SafeCounter,
    pub total_failed: crate::metrics::SafeCounter,
    pub total_invalid_tokens: crate::metrics::SafeCounter,
    pub total_retries: crate::metrics::SafeCounter,
    pub android_sent: crate::metrics::SafeCounter,
    pub ios_sent: crate::metrics::SafeCounter,
    pub web_sent: crate::metrics::SafeCounter,
}

impl Default for DeliveryAnalytics {
    fn default() -> Self {
        Self::new()
    }
}

impl DeliveryAnalytics {
    pub fn new() -> Self {
        Self {
            total_sent: crate::metrics::SafeCounter::new("push_total_sent"),
            total_failed: crate::metrics::SafeCounter::new("push_total_failed"),
            total_invalid_tokens: crate::metrics::SafeCounter::new("push_total_invalid_tokens"),
            total_retries: crate::metrics::SafeCounter::new("push_total_retries"),
            android_sent: crate::metrics::SafeCounter::new("push_android_sent"),
            ios_sent: crate::metrics::SafeCounter::new("push_ios_sent"),
            web_sent: crate::metrics::SafeCounter::new("push_web_sent"),
        }
    }

    pub fn record_sent(&self, device_type: DeviceType) {
        self.total_sent.increment(1);
        match device_type {
            DeviceType::Android => self.android_sent.increment(1),
            DeviceType::Ios => self.ios_sent.increment(1),
            DeviceType::Web => self.web_sent.increment(1),
        };
    }

    pub fn record_failed(&self) {
        self.total_failed.increment(1);
    }

    pub fn record_invalid_token(&self) {
        self.total_invalid_tokens.increment(1);
    }

    pub fn record_retry(&self) {
        self.total_retries.increment(1);
    }

    /// Snapshot the current analytics state as a JSON value.
    pub fn snapshot(&self) -> Value {
        let sent = self.total_sent.get();
        let failed = self.total_failed.get();
        // Issue #993: saturating_add avoids wraparound when computing the
        // combined total from two independently-saturating counters.
        let total = sent.saturating_add(failed);
        let delivery_rate = if total > 0 {
            (sent as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        // Publish counter state gauges so operators can see how close these
        // counters are to saturating (issue #993).
        crate::metrics::record_counter_state("push_total_sent", sent);
        crate::metrics::record_counter_state("push_total_failed", failed);

        json!({
            "total_sent": sent,
            "total_failed": failed,
            "total_invalid_tokens": self.total_invalid_tokens.get(),
            "total_retries": self.total_retries.get(),
            "delivery_rate_percent": (delivery_rate * 100.0).round() / 100.0,
            "by_platform": {
                "android": self.android_sent.get(),
                "ios": self.ios_sent.get(),
                "web": self.web_sent.get(),
            }
        })
    }
}

// ---------------------------------------------------------------------------
// Notification preferences (Issue #839)
// ---------------------------------------------------------------------------

/// User notification preferences for push notifications.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationPreferences {
    /// Whether push notifications are enabled globally.
    pub enabled: bool,
    /// Quiet hours start (0-23, hour of day in UTC).
    pub quiet_hours_start: Option<u8>,
    /// Quiet hours end (0-23, hour of day in UTC).
    pub quiet_hours_end: Option<u8>,
    /// Minimum interval between notifications in seconds (rate limiting).
    pub min_interval_secs: Option<u64>,
    /// Event types to include (empty = all).
    pub event_types: Vec<String>,
}

impl Default for NotificationPreferences {
    fn default() -> Self {
        Self {
            enabled: true,
            quiet_hours_start: None,
            quiet_hours_end: None,
            min_interval_secs: None,
            event_types: Vec::new(),
        }
    }
}

impl NotificationPreferences {
    /// Check whether notifications should be delivered at the given UTC hour.
    ///
    /// Returns `false` during quiet hours, `true` otherwise. When no quiet
    /// hours are configured this always returns `true`.
    pub fn is_within_active_hours(&self, current_hour_utc: u8) -> bool {
        let (start, end) = match (self.quiet_hours_start, self.quiet_hours_end) {
            (Some(s), Some(e)) => (s, e),
            _ => return true,
        };

        if start <= end {
            // e.g. quiet 22..06 doesn't wrap — quiet is [start, end)
            !(current_hour_utc >= start && current_hour_utc < end)
        } else {
            // Wraps midnight: quiet 22..06 means quiet if hour >= 22 OR hour < 6
            !(current_hour_utc >= start || current_hour_utc < end)
        }
    }

    /// Validate quiet hours values are in the valid range (0-23).
    pub fn validate(&self) -> Result<(), String> {
        if let Some(h) = self.quiet_hours_start {
            if h > 23 {
                return Err("quiet_hours_start must be 0-23".to_string());
            }
        }
        if let Some(h) = self.quiet_hours_end {
            if h > 23 {
                return Err("quiet_hours_end must be 0-23".to_string());
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Push delivery worker
// ---------------------------------------------------------------------------

/// Configuration for the push delivery worker.
pub struct PushWorkerConfig {
    pub fcm: Option<FcmConfig>,
    pub apns: Option<ApnsConfig>,
    pub vapid: Option<VapidConfig>,
    pub retry: RetryConfig,
}

impl PushWorkerConfig {
    pub fn from_env() -> Self {
        Self {
            fcm: FcmConfig::from_env(),
            apns: ApnsConfig::from_env(),
            vapid: VapidConfig::from_env(),
            retry: RetryConfig::from_env(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.fcm.is_some() || self.apns.is_some() || self.vapid.is_some()
    }
}

/// Issue #994: adapts push delivery (FCM/APNs) to the unified notification
/// delivery framework so push shares retry/error-handling/metrics with email
/// and SMS instead of each channel reimplementing them independently.
///
/// `target` passed to `deliver()` is the device's push token. Web push
/// tokens are routed through FCM, matching the existing behavior of
/// [`run_push_delivery_worker`] below (true W3C Web Push via VAPID needs a
/// full [`WebPushSubscription`], not a bare token, so it isn't represented
/// by this adapter — see docs/notification-architecture.md).
pub struct PushChannelAdapter {
    pub client: Client,
    pub config: std::sync::Arc<PushWorkerConfig>,
    pub device_type: DeviceType,
}

#[async_trait::async_trait]
impl crate::notification_delivery::DeliveryChannel for PushChannelAdapter {
    fn channel_name(&self) -> &'static str {
        "push"
    }

    fn retry_policy(&self) -> crate::retry_policy::RetryPolicy {
        crate::retry_policy::RetryPolicy {
            max_attempts: self.config.retry.max_retries.max(1),
            initial_backoff_ms: self.config.retry.base_delay_secs.saturating_mul(1000),
            backoff_multiplier: 2.0,
            max_backoff_ms: self.config.retry.max_delay_secs.saturating_mul(1000),
            strategy: Some(crate::retry_policy::RetryStrategy::Exponential),
            use_jitter: true,
        }
    }

    async fn deliver(
        &self,
        target: &str,
        subject: &str,
        body: &str,
    ) -> Result<
        crate::notification_delivery::DeliveryOutcome,
        crate::notification_delivery::NotificationError,
    > {
        use crate::notification_delivery::{DeliveryOutcome, NotificationError};

        let send_result: Result<bool, String> = match self.device_type {
            DeviceType::Ios => {
                let apns_cfg = match self.config.apns.as_ref() {
                    Some(c) => c,
                    None => {
                        return Err(NotificationError::Configuration(
                            "APNs not configured".to_string(),
                        ))
                    }
                };
                let jwt = match build_apns_jwt(apns_cfg) {
                    Ok(j) => j,
                    Err(e) => return Err(NotificationError::Configuration(e)),
                };
                apns_send(&self.client, apns_cfg, target, subject, body, &jwt).await
            }
            DeviceType::Android | DeviceType::Web => {
                let fcm_cfg = match self.config.fcm.as_ref() {
                    Some(c) => c,
                    None => {
                        return Err(NotificationError::Configuration(
                            "FCM not configured".to_string(),
                        ))
                    }
                };
                fcm_send(&self.client, fcm_cfg, target, subject, body, None).await
            }
        };

        match send_result {
            Ok(true) => Ok(DeliveryOutcome::Delivered),
            Ok(false) => Ok(DeliveryOutcome::InvalidTarget),
            Err(msg) => Err(NotificationError::Transient(msg)),
        }
    }
}

/// Background worker: polls subscriptions with push enabled and delivers
/// push notifications for pending events.
pub async fn run_push_delivery_worker(pool: sqlx::PgPool) {
    let config = PushWorkerConfig::from_env();
    if !config.is_enabled() {
        info!("No FCM_SERVER_KEY or APNS_AUTH_KEY_PATH set — push worker disabled");
        return;
    }

    let client = match crate::net::outbound::build_http_client(
        Client::builder().timeout(Duration::from_secs(15)),
        "push",
    ) {
        Ok(client) => client,
        Err(e) => {
            error!(error = %e, "Push worker disabled");
            return;
        }
    };

    let mut interval = tokio::time::interval(Duration::from_secs(30));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Cache APNs JWT to avoid regenerating on every request.
    let mut apns_jwt: Option<(String, std::time::Instant)> = None;

    loop {
        interval.tick().await;

        let rows: Vec<(Uuid, String, Option<String>, Uuid, Value, i64)> = match sqlx::query_as(
            "SELECT DISTINCT ON (s.id) s.id, s.push_token, s.device_type, \
                    dq.event_id, e.event_data, dq.ledger
             FROM subscriptions s
             JOIN delivery_queue dq ON dq.subscription_id = s.id
             JOIN events e ON e.id = dq.event_id
             WHERE s.push_enabled = true
               AND s.push_token IS NOT NULL
               AND s.status = 'active'
               AND dq.status = 'pending'
               AND dq.next_attempt_at <= NOW()
             ORDER BY s.id, dq.ledger ASC
             LIMIT 100",
        )
        .fetch_all(&pool)
        .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "Push delivery worker DB error");
                continue;
            }
        };

        for (sub_id, token, device_type_str, event_id, event_data, ledger) in rows {
            let device_type = device_type_str
                .as_deref()
                .and_then(DeviceType::from_str)
                .unwrap_or(DeviceType::Android);

            let title = "Soroban Event";
            let body_text = format!("New event at ledger {ledger}");
            let data = json!({ "event_id": event_id.to_string(), "ledger": ledger });

            let send_result = match device_type {
                DeviceType::Ios => {
                    if let Some(ref apns_cfg) = config.apns {
                        // Refresh JWT if older than 55 min (APNs tokens expire after 60 min).
                        let cached = apns_jwt
                            .as_ref()
                            .filter(|(_, issued)| issued.elapsed().as_secs() <= 3300)
                            .map(|(jwt, _)| jwt.clone());
                        let jwt = match cached {
                            Some(jwt) => jwt,
                            None => match build_apns_jwt(apns_cfg) {
                                Ok(j) => {
                                    apns_jwt = Some((j.clone(), std::time::Instant::now()));
                                    j
                                }
                                Err(e) => {
                                    warn!(error = %e, "Failed to build APNs JWT");
                                    continue;
                                }
                            },
                        };
                        apns_send(&client, apns_cfg, &token, title, &body_text, &jwt).await
                    } else {
                        Err("APNs not configured".to_string())
                    }
                }
                DeviceType::Android | DeviceType::Web => {
                    if let Some(ref fcm_cfg) = config.fcm {
                        fcm_send(&client, fcm_cfg, &token, title, &body_text, Some(&data)).await
                    } else {
                        Err("FCM not configured".to_string())
                    }
                }
            };

            let (status, error_msg, token_valid) = match send_result {
                Ok(true) => {
                    info!(token = %&token[..8.min(token.len())], device_type = %device_type.as_str(), ledger, "Push notification sent");
                    metrics::record_push_notification_sent(device_type.as_str());
                    ("sent", None, true)
                }
                Ok(false) => {
                    warn!(token = %&token[..8.min(token.len())], "Push token invalid/expired — cleaning up");
                    metrics::record_push_token_invalid();
                    // Disable the push token on this subscription.
                    let _ = sqlx::query(
                        "UPDATE subscriptions SET push_token = NULL, push_enabled = false \
                         WHERE id = $1",
                    )
                    .bind(sub_id)
                    .execute(&pool)
                    .await;
                    ("invalid_token", Some("token expired or not registered".to_string()), false)
                }
                Err(e) => {
                    warn!(error = %e, device_type = %device_type.as_str(), "Push delivery failed");
                    metrics::record_push_notification_failed(device_type.as_str());
                    ("failed", Some(e), true)
                }
            };

            let _ = sqlx::query(
                "INSERT INTO push_delivery_log \
                 (subscription_id, push_token, device_type, status, error, sent_at) \
                 VALUES ($1, $2, $3, $4, $5, CASE WHEN $4 = 'sent' THEN NOW() ELSE NULL END)",
            )
            .bind(sub_id)
            .bind(&token)
            .bind(device_type.as_str())
            .bind(status)
            .bind(error_msg.as_deref())
            .execute(&pool)
            .await;

            if token_valid && status == "sent" {
                let _ = sqlx::query(
                    "UPDATE delivery_queue SET status = 'delivered' \
                     WHERE subscription_id = $1 AND event_id = $2",
                )
                .bind(sub_id)
                .bind(event_id)
                .execute(&pool)
                .await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Push token management handlers
// ---------------------------------------------------------------------------

use axum::{
    extract::{Path, State},
    Json,
};
use crate::{error::AppError, routes::AppState};

#[derive(Debug, Deserialize)]
pub struct UpdatePushTokenRequest {
    pub push_token: Option<String>,
    pub device_type: Option<String>,
    pub enabled: bool,
}

/// `PUT /v1/subscriptions/{id}/push` — register or update a push token.
pub async fn update_subscription_push(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdatePushTokenRequest>,
) -> Result<Json<Value>, AppError> {
    use serde_json::json;

    if body.enabled {
        if body.push_token.as_ref().map(|t| t.is_empty()).unwrap_or(true) {
            return Err(AppError::Validation(
                "push_token is required when enabling push notifications".into(),
            ));
        }
        if let Some(ref dt) = body.device_type {
            if DeviceType::from_str(dt).is_none() {
                return Err(AppError::Validation(
                    "device_type must be 'android', 'ios', or 'web'".into(),
                ));
            }
        }
    }

    let rows = sqlx::query(
        "UPDATE subscriptions
         SET push_token = $2, device_type = $3, push_enabled = $4
         WHERE id = $1 AND status = 'active'",
    )
    .bind(id)
    .bind(&body.push_token)
    .bind(&body.device_type)
    .bind(body.enabled)
    .execute(&state.pool)
    .await?
    .rows_affected();

    if rows == 0 {
        return Err(AppError::NotFound);
    }

    Ok(Json(json!({
        "subscription_id": id,
        "push_enabled": body.enabled,
        "device_type": body.device_type,
    })))
}

/// `GET /v1/subscriptions/{id}/push` — get push config for a subscription.
pub async fn get_subscription_push(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, AppError> {
    use serde_json::json;

    let row: Option<(Option<String>, bool, Option<String>)> = sqlx::query_as(
        "SELECT push_token, push_enabled, device_type FROM subscriptions WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?;

    let (push_token, push_enabled, device_type) = row.ok_or(AppError::NotFound)?;

    // Mask token for privacy — show only first 8 characters.
    let token_masked = push_token.as_deref().map(|t| {
        let prefix = &t[..8.min(t.len())];
        format!("{prefix}...")
    });

    Ok(Json(json!({
        "subscription_id": id,
        "push_enabled": push_enabled,
        "device_type": device_type,
        "push_token_prefix": token_masked,
    })))
}

/// `GET /v1/admin/push/analytics` — push delivery analytics summary (Issue #839).
///
/// Returns aggregate delivery statistics across all platforms.
pub async fn get_push_analytics(
    State(state): State<AppState>,
) -> Result<Json<Value>, AppError> {
    let analytics = &state.push_analytics;
    Ok(Json(analytics.snapshot()))
}

/// `PUT /v1/subscriptions/{id}/push/preferences` — update notification
/// preferences (Issue #839).
pub async fn update_notification_preferences(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(prefs): Json<NotificationPreferences>,
) -> Result<Json<Value>, AppError> {
    prefs.validate().map_err(AppError::Validation)?;

    let prefs_json =
        serde_json::to_value(&prefs).map_err(|e| AppError::Internal(e.to_string()))?;

    let rows = sqlx::query(
        "UPDATE subscriptions SET push_preferences = $2 WHERE id = $1 AND status = 'active'",
    )
    .bind(id)
    .bind(&prefs_json)
    .execute(&state.pool)
    .await?
    .rows_affected();

    if rows == 0 {
        return Err(AppError::NotFound);
    }

    Ok(Json(json!({
        "subscription_id": id,
        "preferences": prefs_json,
    })))
}

/// `GET /v1/subscriptions/{id}/push/preferences` — retrieve notification
/// preferences (Issue #839).
pub async fn get_notification_preferences(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, AppError> {
    let row: Option<(Option<Value>,)> = sqlx::query_as(
        "SELECT push_preferences FROM subscriptions WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await?;

    let (prefs_json,) = row.ok_or(AppError::NotFound)?;
    let prefs = prefs_json.unwrap_or_else(|| {
        serde_json::to_value(NotificationPreferences::default()).unwrap_or_default()
    });

    Ok(Json(json!({
        "subscription_id": id,
        "preferences": prefs,
    })))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── DeviceType ──────────────────────────────────────────────────────

    #[test]
    fn device_type_roundtrip() {
        for (s, expected) in &[
            ("android", DeviceType::Android),
            ("ios", DeviceType::Ios),
            ("web", DeviceType::Web),
        ] {
            let dt = DeviceType::from_str(s).expect("should parse");
            assert_eq!(dt, *expected);
            assert_eq!(dt.as_str(), *s);
        }
    }

    #[test]
    fn device_type_case_insensitive() {
        assert_eq!(DeviceType::from_str("IOS"), Some(DeviceType::Ios));
        assert_eq!(DeviceType::from_str("Android"), Some(DeviceType::Android));
    }

    #[test]
    fn device_type_unknown_returns_none() {
        assert_eq!(DeviceType::from_str("blackberry"), None);
    }

    // ── APNs config ─────────────────────────────────────────────────────

    #[test]
    fn apns_config_production_endpoint() {
        let cfg = ApnsConfig {
            auth_key: String::new(),
            key_id: "K1234567890".to_string(),
            team_id: "TEAM123456".to_string(),
            bundle_id: "com.example.app".to_string(),
            is_production: true,
        };
        assert_eq!(cfg.endpoint(), "https://api.push.apple.com");
    }

    #[test]
    fn apns_config_sandbox_endpoint() {
        let cfg = ApnsConfig {
            auth_key: String::new(),
            key_id: "K1234567890".to_string(),
            team_id: "TEAM123456".to_string(),
            bundle_id: "com.example.app".to_string(),
            is_production: false,
        };
        assert_eq!(cfg.endpoint(), "https://api.sandbox.push.apple.com");
    }

    #[test]
    fn fcm_config_from_env_absent() {
        // Should be None when FCM_SERVER_KEY is unset.
        std::env::remove_var("FCM_SERVER_KEY");
        assert!(FcmConfig::from_env().is_none());
    }

    // ── PushWorkerConfig ────────────────────────────────────────────────

    #[test]
    fn push_worker_disabled_when_no_config() {
        let cfg = PushWorkerConfig {
            fcm: None,
            apns: None,
            vapid: None,
            retry: RetryConfig {
                max_retries: 5,
                base_delay_secs: 2,
                max_delay_secs: 3600,
            },
        };
        assert!(!cfg.is_enabled());
    }

    #[test]
    fn push_worker_enabled_with_vapid_only() {
        let cfg = PushWorkerConfig {
            fcm: None,
            apns: None,
            vapid: Some(VapidConfig {
                public_key: "test-pub".to_string(),
                private_key: "test-priv".to_string(),
                subject: "mailto:test@example.com".to_string(),
            }),
            retry: RetryConfig::from_env(),
        };
        assert!(cfg.is_enabled());
    }

    // ── VAPID auth header ───────────────────────────────────────────────

    #[test]
    fn vapid_auth_header_contains_jwt_and_key() {
        let config = VapidConfig {
            public_key: "BNcRd…test-key".to_string(),
            private_key: "priv-key".to_string(),
            subject: "mailto:admin@sorobanpulse.io".to_string(),
        };
        let header = build_vapid_auth_header(
            &config,
            "https://fcm.googleapis.com/fcm/send/some-token",
        )
        .expect("should build header");
        assert!(header.starts_with("vapid t="));
        assert!(header.contains(",k=BNcRd"));
    }

    #[test]
    fn vapid_auth_header_rejects_invalid_url() {
        let config = VapidConfig {
            public_key: "key".to_string(),
            private_key: "priv".to_string(),
            subject: "mailto:a@b.com".to_string(),
        };
        let result = build_vapid_auth_header(&config, "not-a-valid-url");
        assert!(result.is_err());
    }

    // ── Web Push subscription serialization ─────────────────────────────

    #[test]
    fn web_push_subscription_roundtrip() {
        let sub = WebPushSubscription {
            endpoint: "https://push.example.com/v1/token123".to_string(),
            keys: WebPushKeys {
                p256dh: "BNcRd…".to_string(),
                auth: "tBHI…".to_string(),
            },
        };
        let json_str = serde_json::to_string(&sub).unwrap();
        let parsed: WebPushSubscription = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed.endpoint, sub.endpoint);
        assert_eq!(parsed.keys.p256dh, sub.keys.p256dh);
        assert_eq!(parsed.keys.auth, sub.keys.auth);
    }

    // ── RetryConfig ─────────────────────────────────────────────────────

    #[test]
    fn retry_config_defaults() {
        std::env::remove_var("PUSH_MAX_RETRIES");
        std::env::remove_var("PUSH_BASE_DELAY_SECS");
        let cfg = RetryConfig::from_env();
        assert_eq!(cfg.max_retries, 5);
        assert_eq!(cfg.base_delay_secs, 2);
    }

    #[test]
    fn retry_next_delay_exponential_backoff() {
        let cfg = RetryConfig {
            max_retries: 5,
            base_delay_secs: 2,
            max_delay_secs: 3600,
        };
        // attempt 0: 2 * 2^0 = 2s + 10% jitter = 2s
        let d0 = cfg.next_delay(0).unwrap();
        assert_eq!(d0.as_secs(), 2); // 2 + 0 jitter (2/10=0)

        // attempt 1: 2 * 2^1 = 4s + 0s jitter
        let d1 = cfg.next_delay(1).unwrap();
        assert_eq!(d1.as_secs(), 4); // 4 + 0 jitter (4/10=0)

        // attempt 2: 2 * 2^2 = 8s + 0s jitter
        let d2 = cfg.next_delay(2).unwrap();
        assert_eq!(d2.as_secs(), 8); // 8 + 0 jitter (8/10=0)

        // attempt 3: 2 * 2^3 = 16s + 1s jitter
        let d3 = cfg.next_delay(3).unwrap();
        assert_eq!(d3.as_secs(), 17); // 16 + 1 jitter

        // attempt 4: 2 * 2^4 = 32s + 3s jitter
        let d4 = cfg.next_delay(4).unwrap();
        assert_eq!(d4.as_secs(), 35); // 32 + 3 jitter
    }

    #[test]
    fn retry_returns_none_when_exhausted() {
        let cfg = RetryConfig {
            max_retries: 3,
            base_delay_secs: 1,
            max_delay_secs: 3600,
        };
        assert!(cfg.next_delay(3).is_none());
        assert!(cfg.next_delay(10).is_none());
    }

    #[test]
    fn retry_delay_capped_at_max() {
        let cfg = RetryConfig {
            max_retries: 20,
            base_delay_secs: 100,
            max_delay_secs: 500,
        };
        let d = cfg.next_delay(10).unwrap();
        // 100 * 2^10 = 102400, capped to 500, + 50 jitter = 550
        assert!(d.as_secs() <= 550);
    }

    #[test]
    fn retry_is_exhausted() {
        let cfg = RetryConfig {
            max_retries: 3,
            base_delay_secs: 1,
            max_delay_secs: 3600,
        };
        assert!(!cfg.is_exhausted(0));
        assert!(!cfg.is_exhausted(2));
        assert!(cfg.is_exhausted(3));
        assert!(cfg.is_exhausted(100));
    }

    // ── DeliveryAnalytics ───────────────────────────────────────────────

    #[test]
    fn delivery_analytics_starts_at_zero() {
        let analytics = DeliveryAnalytics::new();
        let snap = analytics.snapshot();
        assert_eq!(snap["total_sent"], 0);
        assert_eq!(snap["total_failed"], 0);
        assert_eq!(snap["total_invalid_tokens"], 0);
        assert_eq!(snap["total_retries"], 0);
        assert_eq!(snap["by_platform"]["android"], 0);
        assert_eq!(snap["by_platform"]["ios"], 0);
        assert_eq!(snap["by_platform"]["web"], 0);
    }

    #[test]
    fn delivery_analytics_tracks_sent_by_platform() {
        let analytics = DeliveryAnalytics::new();
        analytics.record_sent(DeviceType::Android);
        analytics.record_sent(DeviceType::Android);
        analytics.record_sent(DeviceType::Ios);
        analytics.record_sent(DeviceType::Web);

        let snap = analytics.snapshot();
        assert_eq!(snap["total_sent"], 4);
        assert_eq!(snap["by_platform"]["android"], 2);
        assert_eq!(snap["by_platform"]["ios"], 1);
        assert_eq!(snap["by_platform"]["web"], 1);
    }

    #[test]
    fn delivery_analytics_delivery_rate() {
        let analytics = DeliveryAnalytics::new();
        for _ in 0..8 {
            analytics.record_sent(DeviceType::Android);
        }
        for _ in 0..2 {
            analytics.record_failed();
        }

        let snap = analytics.snapshot();
        // 8 sent / (8+2) total = 80%
        let rate = snap["delivery_rate_percent"].as_f64().unwrap();
        assert!((rate - 80.0).abs() < 0.01);
    }

    #[test]
    fn delivery_analytics_zero_division_safe() {
        let analytics = DeliveryAnalytics::new();
        let snap = analytics.snapshot();
        assert_eq!(snap["delivery_rate_percent"], 0.0);
    }

    #[test]
    fn delivery_analytics_tracks_retries_and_invalid_tokens() {
        let analytics = DeliveryAnalytics::new();
        analytics.record_retry();
        analytics.record_retry();
        analytics.record_invalid_token();

        let snap = analytics.snapshot();
        assert_eq!(snap["total_retries"], 2);
        assert_eq!(snap["total_invalid_tokens"], 1);
    }

    // ── NotificationPreferences ─────────────────────────────────────────

    #[test]
    fn notification_preferences_defaults() {
        let prefs = NotificationPreferences::default();
        assert!(prefs.enabled);
        assert!(prefs.quiet_hours_start.is_none());
        assert!(prefs.quiet_hours_end.is_none());
        assert!(prefs.min_interval_secs.is_none());
        assert!(prefs.event_types.is_empty());
    }

    #[test]
    fn notification_preferences_active_hours_no_quiet_hours() {
        let prefs = NotificationPreferences::default();
        // Without quiet hours, all hours are active.
        for h in 0..24 {
            assert!(prefs.is_within_active_hours(h));
        }
    }

    #[test]
    fn notification_preferences_quiet_hours_no_wrap() {
        let prefs = NotificationPreferences {
            quiet_hours_start: Some(2),
            quiet_hours_end: Some(6),
            ..Default::default()
        };
        // Hour 0 and 1: active (before quiet start)
        assert!(prefs.is_within_active_hours(0));
        assert!(prefs.is_within_active_hours(1));
        // Hours 2-5: quiet
        assert!(!prefs.is_within_active_hours(2));
        assert!(!prefs.is_within_active_hours(5));
        // Hour 6: active again
        assert!(prefs.is_within_active_hours(6));
        assert!(prefs.is_within_active_hours(12));
    }

    #[test]
    fn notification_preferences_quiet_hours_wraps_midnight() {
        let prefs = NotificationPreferences {
            quiet_hours_start: Some(22),
            quiet_hours_end: Some(6),
            ..Default::default()
        };
        // Hours 22, 23, 0-5: quiet
        assert!(!prefs.is_within_active_hours(22));
        assert!(!prefs.is_within_active_hours(23));
        assert!(!prefs.is_within_active_hours(0));
        assert!(!prefs.is_within_active_hours(3));
        assert!(!prefs.is_within_active_hours(5));
        // Hours 6-21: active
        assert!(prefs.is_within_active_hours(6));
        assert!(prefs.is_within_active_hours(12));
        assert!(prefs.is_within_active_hours(21));
    }

    #[test]
    fn notification_preferences_validate_ok() {
        let prefs = NotificationPreferences {
            quiet_hours_start: Some(22),
            quiet_hours_end: Some(6),
            ..Default::default()
        };
        assert!(prefs.validate().is_ok());
    }

    #[test]
    fn notification_preferences_validate_bad_start() {
        let prefs = NotificationPreferences {
            quiet_hours_start: Some(25),
            quiet_hours_end: Some(6),
            ..Default::default()
        };
        assert!(prefs.validate().is_err());
    }

    #[test]
    fn notification_preferences_validate_bad_end() {
        let prefs = NotificationPreferences {
            quiet_hours_start: Some(22),
            quiet_hours_end: Some(30),
            ..Default::default()
        };
        assert!(prefs.validate().is_err());
    }

    #[test]
    fn notification_preferences_serialization_roundtrip() {
        let prefs = NotificationPreferences {
            enabled: true,
            quiet_hours_start: Some(22),
            quiet_hours_end: Some(6),
            min_interval_secs: Some(300),
            event_types: vec!["transfer".to_string(), "mint".to_string()],
        };
        let json_str = serde_json::to_string(&prefs).unwrap();
        let parsed: NotificationPreferences = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed.enabled, prefs.enabled);
        assert_eq!(parsed.quiet_hours_start, prefs.quiet_hours_start);
        assert_eq!(parsed.quiet_hours_end, prefs.quiet_hours_end);
        assert_eq!(parsed.min_interval_secs, prefs.min_interval_secs);
        assert_eq!(parsed.event_types, prefs.event_types);
    }
}
