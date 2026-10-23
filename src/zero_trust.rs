//! Zero-Trust Security Implementation (Issue #838)
//!
//! Provides core building blocks for a zero-trust security model:
//!
//! - **Request signing & verification** via HMAC-SHA256 with timestamp freshness
//!   checks and constant-time signature comparison.
//! - **API key rotation** with primary/secondary key support and configurable
//!   grace periods.
//! - **Access decision evaluation** using a policy-based approach with Allow,
//!   Deny, and Challenge outcomes.
//! - **In-memory access logging** for audit trail and forensic analysis.

extern crate metrics as m;

use chrono::{DateTime, Utc};
use hmac::Mac;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Mutex;
use subtle::ConstantTimeEq;


/// Maximum allowed age (in seconds) for a signed request before it is
/// considered stale and rejected. Set to 5 minutes.
const MAX_TIMESTAMP_AGE_SECS: i64 = 300;

// ---------------------------------------------------------------------------
// Request Signature
// ---------------------------------------------------------------------------

/// HMAC-SHA256 request signing and verification (Issue #838).
///
/// The signature is computed over a canonical representation of the request:
/// `METHOD\nPATH\nTIMESTAMP\nBODY`.  Verification includes a timestamp
/// freshness check (requests older than 5 minutes are rejected) and
/// constant-time comparison to prevent timing-based side-channel attacks.
pub struct RequestSignature;

impl RequestSignature {
    /// Produce a hex-encoded HMAC-SHA256 signature for the given request
    /// components.
    ///
    /// # Arguments
    ///
    /// * `secret`    - Shared secret used as the HMAC key.
    /// * `method`    - HTTP method (e.g. `GET`, `POST`).
    /// * `path`      - Request path (e.g. `/api/v1/events`).
    /// * `timestamp` - ISO-8601 timestamp string.
    /// * `body`      - Raw request body (may be empty for bodyless methods).
    #[must_use]
    pub fn sign(secret: &str, method: &str, path: &str, timestamp: &str, body: &str) -> String {
        let payload = Self::canonical_payload(method, path, timestamp, body);
        let mut mac = crate::hmac_util::hmac_sha256(secret.as_bytes());
        mac.update(payload.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// Verify a hex-encoded HMAC-SHA256 signature.
    ///
    /// Returns `true` only when **both** of the following conditions hold:
    /// 1. The provided signature matches the expected signature (constant-time
    ///    comparison).
    /// 2. The `timestamp` is no older than [`MAX_TIMESTAMP_AGE_SECS`] (5 min).
    #[must_use]
    pub fn verify(
        secret: &str,
        method: &str,
        path: &str,
        timestamp: &str,
        body: &str,
        provided_sig: &str,
    ) -> bool {
        // Timestamp freshness check.
        if !Self::is_timestamp_fresh(timestamp) {
            return false;
        }

        let expected = Self::sign(secret, method, path, timestamp, body);

        // Constant-time comparison to mitigate timing attacks.
        expected.as_bytes().ct_eq(provided_sig.as_bytes()).into()
    }

    /// Build the canonical string that is signed.
    fn canonical_payload(method: &str, path: &str, timestamp: &str, body: &str) -> String {
        format!("{method}\n{path}\n{timestamp}\n{body}")
    }

    /// Return `true` if `timestamp` parses as RFC 3339 / ISO-8601 and is no
    /// older than [`MAX_TIMESTAMP_AGE_SECS`] seconds from now.
    fn is_timestamp_fresh(timestamp: &str) -> bool {
        let Ok(ts) = timestamp.parse::<DateTime<Utc>>() else {
            return false;
        };
        let age = Utc::now().signed_duration_since(ts);
        age.num_seconds().abs() <= MAX_TIMESTAMP_AGE_SECS
    }
}

// ---------------------------------------------------------------------------
// API Key Rotation
// ---------------------------------------------------------------------------

/// A set of API keys supporting seamless rotation with a grace period
/// (Issue #838).
///
/// When a rotation occurs the previous primary key is demoted to `secondary`
/// so that in-flight requests signed with the old key continue to validate
/// during the grace window.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApiKeySet {
    /// The current active key.
    pub primary: String,
    /// The previous key, retained temporarily after rotation.
    pub secondary: Option<String>,
    /// When the most recent rotation happened.
    pub rotated_at: Option<DateTime<Utc>>,
}

impl ApiKeySet {
    /// Create a new key set with only a primary key.
    #[must_use]
    pub fn new(primary: impl Into<String>) -> Self {
        Self {
            primary: primary.into(),
            secondary: None,
            rotated_at: None,
        }
    }

    /// Check whether `key` matches either the primary or secondary key.
    ///
    /// Comparison is performed using constant-time equality to avoid leaking
    /// information about which key (or how many characters) matched.
    #[must_use]
    pub fn is_valid(&self, key: &str) -> bool {
        let primary_match: bool = self
            .primary
            .as_bytes()
            .ct_eq(key.as_bytes())
            .into();

        let secondary_match: bool = self
            .secondary
            .as_ref()
            .map_or(false, |s| s.as_bytes().ct_eq(key.as_bytes()).into());

        primary_match || secondary_match
    }

    /// Rotate the key set: the current `primary` becomes `secondary` and
    /// `new_key` becomes the new `primary`.  `rotated_at` is set to the
    /// current UTC time.
    pub fn rotate(&mut self, new_key: impl Into<String>) {
        self.secondary = Some(std::mem::take(&mut self.primary));
        self.primary = new_key.into();
        self.rotated_at = Some(Utc::now());
    }

    /// Same as [`rotate`](Self::rotate), plus records a
    /// `soroban_pulse_api_key_rotations_total` metric (Issue #939) — use
    /// this instead of `rotate()` directly at any real rotation call site
    /// so rotation events are observable. Kept as a separate method rather
    /// than folding the `metrics::counter!` call into `rotate()` itself so
    /// `rotate()` stays a plain, dependency-free, synchronously-testable
    /// state transition (see this module's own tests).
    pub fn rotate_with_metrics(&mut self, new_key: impl Into<String>) {
        self.rotate(new_key);
        m::counter!("soroban_pulse_api_key_rotations_total").increment(1);
    }

    /// Return `true` if a rotation has occurred within the last
    /// `grace_period_secs` seconds.
    #[must_use]
    pub fn is_in_grace_period(&self, grace_period_secs: i64) -> bool {
        self.rotated_at.map_or(false, |rotated| {
            let elapsed = Utc::now().signed_duration_since(rotated);
            elapsed.num_seconds() < grace_period_secs
        })
    }
}

// ---------------------------------------------------------------------------
// Access Decision & Policy Evaluation
// ---------------------------------------------------------------------------

/// The method a client should use to prove its identity when challenged.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChallengeMethod {
    /// Multi-factor authentication required.
    Mfa,
    /// Client must re-authenticate via a fresh token.
    Reauthenticate,
    /// Client must present a valid CAPTCHA solution.
    Captcha,
}

impl fmt::Display for ChallengeMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Mfa => write!(f, "MFA"),
            Self::Reauthenticate => write!(f, "Reauthenticate"),
            Self::Captcha => write!(f, "CAPTCHA"),
        }
    }
}

/// The outcome of a zero-trust access evaluation (Issue #838).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccessDecision {
    /// Request is permitted.
    Allow,
    /// Request is denied with a human-readable reason.
    Deny(String),
    /// Request requires additional verification via the specified method.
    Challenge(ChallengeMethod),
}

impl fmt::Display for AccessDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Allow => write!(f, "Allow"),
            Self::Deny(reason) => write!(f, "Deny({reason})"),
            Self::Challenge(method) => write!(f, "Challenge({method})"),
        }
    }
}

/// Contextual information about an incoming request, used by the policy
/// evaluator to make an [`AccessDecision`] (Issue #838).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestContext {
    /// Source IP address as a string (supports both IPv4 and IPv6).
    pub ip_address: String,
    /// SHA-256 hash of the API key presented by the caller.
    pub api_key_hash: String,
    /// Request path (e.g. `/api/v1/events`).
    pub path: String,
    /// HTTP method (e.g. `GET`, `POST`, `DELETE`).
    pub method: String,
    /// When the request was received.
    pub timestamp: DateTime<Utc>,
    /// Value of the `User-Agent` header, if present.
    pub user_agent: Option<String>,
}

impl RequestContext {
    /// Create a new `RequestContext`.
    #[must_use]
    pub fn new(
        ip_address: impl Into<String>,
        api_key_hash: impl Into<String>,
        path: impl Into<String>,
        method: impl Into<String>,
    ) -> Self {
        Self {
            ip_address: ip_address.into(),
            api_key_hash: api_key_hash.into(),
            path: path.into(),
            method: method.into(),
            timestamp: Utc::now(),
            user_agent: None,
        }
    }

    /// Attach a user agent string to the context.
    #[must_use]
    pub fn with_user_agent(mut self, ua: impl Into<String>) -> Self {
        self.user_agent = Some(ua.into());
        self
    }

    /// Override the timestamp (useful for testing).
    #[must_use]
    pub fn with_timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.timestamp = ts;
        self
    }
}

/// A rule that contributes to an [`AccessDecision`].
///
/// Implementations inspect a [`RequestContext`] and return `Some(decision)` if
/// the rule is applicable, or `None` to defer to subsequent rules.
pub trait PolicyRule: Send + Sync {
    /// Evaluate the rule against the provided context.
    fn evaluate(&self, ctx: &RequestContext) -> Option<AccessDecision>;
}

/// Policy-based access evaluator (Issue #838).
///
/// Rules are evaluated in insertion order.  The first rule that returns a
/// non-`None` decision wins.  If no rule matches, the default decision is
/// `Deny("no matching policy rule")`.
pub struct PolicyEvaluator {
    rules: Vec<Box<dyn PolicyRule>>,
}

impl PolicyEvaluator {
    /// Create an empty evaluator with no rules.
    #[must_use]
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Append a rule to the evaluation chain.
    pub fn add_rule(&mut self, rule: Box<dyn PolicyRule>) {
        self.rules.push(rule);
    }

    /// Evaluate all rules against `ctx` and return the resulting decision.
    #[must_use]
    pub fn evaluate(&self, ctx: &RequestContext) -> AccessDecision {
        for rule in &self.rules {
            if let Some(decision) = rule.evaluate(ctx) {
                return decision;
            }
        }
        AccessDecision::Deny("no matching policy rule".to_owned())
    }
}

impl Default for PolicyEvaluator {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Built-in policy rules
// ---------------------------------------------------------------------------

/// Deny requests originating from specific IP addresses.
/// A single IP filter list entry: either an exact address or a CIDR block
/// (e.g. `"203.0.113.5"` or `"203.0.113.0/24"`, IPv4 or IPv6). Parsed once
/// at construction so evaluate() never re-parses on the hot path.
///
/// Issue #942: `IpDenyListRule` previously only supported exact-string
/// matches — a `/24` block had to be listed one address at a time, which
/// isn't practical for real network-level allow/deny lists.
#[derive(Clone, Debug)]
struct IpFilterEntry {
    network: std::net::IpAddr,
    prefix_len: u8,
}

impl IpFilterEntry {
    fn parse(entry: &str) -> Option<Self> {
        if let Some((addr, len)) = entry.split_once('/') {
            let network: std::net::IpAddr = addr.trim().parse().ok()?;
            let prefix_len: u8 = len.trim().parse().ok()?;
            let max_len = match network {
                std::net::IpAddr::V4(_) => 32,
                std::net::IpAddr::V6(_) => 128,
            };
            if prefix_len > max_len {
                return None;
            }
            Some(Self { network, prefix_len })
        } else {
            let network: std::net::IpAddr = entry.trim().parse().ok()?;
            let prefix_len = match network {
                std::net::IpAddr::V4(_) => 32,
                std::net::IpAddr::V6(_) => 128,
            };
            Some(Self { network, prefix_len })
        }
    }

    fn contains(&self, candidate: std::net::IpAddr) -> bool {
        match (self.network, candidate) {
            (std::net::IpAddr::V4(net), std::net::IpAddr::V4(addr)) => {
                let mask = if self.prefix_len == 0 {
                    0u32
                } else {
                    u32::MAX << (32 - self.prefix_len)
                };
                (u32::from(net) & mask) == (u32::from(addr) & mask)
            }
            (std::net::IpAddr::V6(net), std::net::IpAddr::V6(addr)) => {
                let mask = if self.prefix_len == 0 {
                    0u128
                } else {
                    u128::MAX << (128 - self.prefix_len)
                };
                (u128::from(net) & mask) == (u128::from(addr) & mask)
            }
            // IPv4/IPv6 family mismatch never matches.
            _ => false,
        }
    }
}

/// Parses a list of IP/CIDR strings, silently skipping unparseable entries
/// (a malformed config entry must never panic the whole rule set).
fn parse_ip_filter_list(entries: &[String]) -> Vec<IpFilterEntry> {
    entries.iter().filter_map(|e| IpFilterEntry::parse(e)).collect()
}

pub struct IpDenyListRule {
    denied: Vec<IpFilterEntry>,
}

impl IpDenyListRule {
    /// `denied_ips` accepts exact addresses and/or CIDR blocks
    /// (`"203.0.113.5"`, `"203.0.113.0/24"`, IPv4 or IPv6).
    #[must_use]
    pub fn new(denied_ips: Vec<String>) -> Self {
        Self { denied: parse_ip_filter_list(&denied_ips) }
    }
}

impl PolicyRule for IpDenyListRule {
    fn evaluate(&self, ctx: &RequestContext) -> Option<AccessDecision> {
        let Ok(candidate) = ctx.ip_address.parse::<std::net::IpAddr>() else {
            return None;
        };
        if self.denied.iter().any(|entry| entry.contains(candidate)) {
            Some(AccessDecision::Deny(format!(
                "IP address {} is blocked",
                ctx.ip_address
            )))
        } else {
            None
        }
    }
}

/// Only allow requests from a pre-approved set of IPs/CIDR blocks; every
/// other address is denied. The counterpart to [`IpDenyListRule`] — use
/// one or the other, not both, in a given [`PolicyEvaluator`] (an allow
/// list is the more restrictive posture: everything not explicitly listed
/// is rejected).
pub struct IpAllowListRule {
    allowed: Vec<IpFilterEntry>,
}

impl IpAllowListRule {
    /// `allowed_ips` accepts exact addresses and/or CIDR blocks.
    #[must_use]
    pub fn new(allowed_ips: Vec<String>) -> Self {
        Self { allowed: parse_ip_filter_list(&allowed_ips) }
    }
}

impl PolicyRule for IpAllowListRule {
    fn evaluate(&self, ctx: &RequestContext) -> Option<AccessDecision> {
        let Ok(candidate) = ctx.ip_address.parse::<std::net::IpAddr>() else {
            return Some(AccessDecision::Deny(
                "Unparseable client IP address".to_string(),
            ));
        };
        if self.allowed.iter().any(|entry| entry.contains(candidate)) {
            None // Not this rule's business to Allow — just don't block.
        } else {
            Some(AccessDecision::Deny(format!(
                "IP address {} is not on the allow list",
                ctx.ip_address
            )))
        }
    }
}

/// Require MFA for mutating operations (`POST`, `PUT`, `PATCH`, `DELETE`).
pub struct MutationChallengeRule;

impl PolicyRule for MutationChallengeRule {
    fn evaluate(&self, ctx: &RequestContext) -> Option<AccessDecision> {
        let upper = ctx.method.to_uppercase();
        if matches!(upper.as_str(), "POST" | "PUT" | "PATCH" | "DELETE") {
            Some(AccessDecision::Challenge(ChallengeMethod::Mfa))
        } else {
            None
        }
    }
}

/// Allow requests whose API key hash appears in a pre-approved set.
pub struct AllowedKeysRule {
    allowed_key_hashes: Vec<String>,
}

impl AllowedKeysRule {
    #[must_use]
    pub fn new(allowed_key_hashes: Vec<String>) -> Self {
        Self { allowed_key_hashes }
    }
}

impl PolicyRule for AllowedKeysRule {
    fn evaluate(&self, ctx: &RequestContext) -> Option<AccessDecision> {
        if self.allowed_key_hashes.contains(&ctx.api_key_hash) {
            Some(AccessDecision::Allow)
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Access Logger
// ---------------------------------------------------------------------------

/// A single entry in the in-memory access audit log (Issue #838).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccessLogEntry {
    /// The request context that was evaluated.
    pub context: RequestContext,
    /// The resulting access decision.
    pub decision: AccessDecision,
    /// When the entry was recorded.
    pub recorded_at: DateTime<Utc>,
}

/// Thread-safe, in-memory access log for audit and forensic analysis
/// (Issue #838).
///
/// Entries are stored in insertion order with a configurable capacity. When
/// the capacity is exceeded the oldest entries are dropped.
pub struct AccessLogger {
    entries: Mutex<Vec<AccessLogEntry>>,
    capacity: usize,
}

impl AccessLogger {
    /// Create a new logger that retains at most `capacity` entries.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: Mutex::new(Vec::with_capacity(capacity.min(1024))),
            capacity,
        }
    }

    /// Record an access decision for the given request context.
    pub fn record(&self, context: RequestContext, decision: AccessDecision) {
        let entry = AccessLogEntry {
            context,
            decision,
            recorded_at: Utc::now(),
        };
        let mut entries = self.entries.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries.len() >= self.capacity {
            entries.remove(0);
        }
        entries.push(entry);
    }

    /// Return the most recent `limit` entries, ordered newest-first.
    #[must_use]
    pub fn recent_entries(&self, limit: usize) -> Vec<AccessLogEntry> {
        let entries = self.entries.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        entries
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }

    /// Return the most recent `limit` entries associated with a specific API
    /// key hash, ordered newest-first.
    #[must_use]
    pub fn entries_for_key(&self, key_hash: &str, limit: usize) -> Vec<AccessLogEntry> {
        let entries = self.entries.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        entries
            .iter()
            .rev()
            .filter(|e| e.context.api_key_hash == key_hash)
            .take(limit)
            .cloned()
            .collect()
    }

    /// Return the total number of entries currently stored.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.lock().unwrap_or_else(std::sync::PoisonError::into_inner).len()
    }

    /// Return `true` if the log contains no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    // -- RequestSignature tests -----------------------------------------------

    #[test]
    fn sign_produces_hex_encoded_hmac() {
        let ts = Utc::now().to_rfc3339();
        let sig = RequestSignature::sign("secret", "GET", "/api/v1/events", &ts, "");
        // SHA-256 HMAC produces 32 bytes = 64 hex chars.
        assert_eq!(sig.len(), 64);
        assert!(sig.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn sign_is_deterministic() {
        let ts = Utc::now().to_rfc3339();
        let a = RequestSignature::sign("key", "POST", "/path", &ts, "body");
        let b = RequestSignature::sign("key", "POST", "/path", &ts, "body");
        assert_eq!(a, b);
    }

    #[test]
    fn verify_valid_signature() {
        let ts = Utc::now().to_rfc3339();
        let sig = RequestSignature::sign("s3cret", "POST", "/events", &ts, r#"{"a":1}"#);
        assert!(RequestSignature::verify(
            "s3cret", "POST", "/events", &ts, r#"{"a":1}"#, &sig,
        ));
    }

    #[test]
    fn verify_rejects_tampered_signature() {
        let ts = Utc::now().to_rfc3339();
        let mut sig = RequestSignature::sign("secret", "GET", "/", &ts, "");
        // Flip the last character.
        let last = sig.pop().unwrap();
        sig.push(if last == 'a' { 'b' } else { 'a' });
        assert!(!RequestSignature::verify("secret", "GET", "/", &ts, "", &sig));
    }

    #[test]
    fn verify_rejects_tampered_body() {
        let ts = Utc::now().to_rfc3339();
        let sig = RequestSignature::sign("secret", "POST", "/", &ts, "original");
        assert!(!RequestSignature::verify(
            "secret", "POST", "/", &ts, "tampered", &sig,
        ));
    }

    #[test]
    fn verify_rejects_tampered_method() {
        let ts = Utc::now().to_rfc3339();
        let sig = RequestSignature::sign("secret", "GET", "/path", &ts, "");
        assert!(!RequestSignature::verify(
            "secret", "DELETE", "/path", &ts, "", &sig,
        ));
    }

    #[test]
    fn verify_rejects_tampered_path() {
        let ts = Utc::now().to_rfc3339();
        let sig = RequestSignature::sign("secret", "GET", "/safe", &ts, "");
        assert!(!RequestSignature::verify(
            "secret", "GET", "/admin", &ts, "", &sig,
        ));
    }

    #[test]
    fn verify_rejects_wrong_secret() {
        let ts = Utc::now().to_rfc3339();
        let sig = RequestSignature::sign("correct-secret", "GET", "/", &ts, "");
        assert!(!RequestSignature::verify(
            "wrong-secret", "GET", "/", &ts, "", &sig,
        ));
    }

    #[test]
    fn verify_rejects_expired_timestamp() {
        let old = (Utc::now() - Duration::seconds(MAX_TIMESTAMP_AGE_SECS + 60)).to_rfc3339();
        let sig = RequestSignature::sign("secret", "GET", "/", &old, "");
        assert!(!RequestSignature::verify("secret", "GET", "/", &old, "", &sig));
    }

    #[test]
    fn verify_accepts_just_within_window() {
        let ts = (Utc::now() - Duration::seconds(MAX_TIMESTAMP_AGE_SECS - 10)).to_rfc3339();
        let sig = RequestSignature::sign("secret", "GET", "/", &ts, "");
        assert!(RequestSignature::verify("secret", "GET", "/", &ts, "", &sig));
    }

    #[test]
    fn verify_rejects_unparseable_timestamp() {
        let sig = RequestSignature::sign("secret", "GET", "/", "not-a-date", "");
        assert!(!RequestSignature::verify(
            "secret", "GET", "/", "not-a-date", "", &sig,
        ));
    }

    #[test]
    fn verify_rejects_completely_wrong_signature() {
        let ts = Utc::now().to_rfc3339();
        assert!(!RequestSignature::verify(
            "secret", "GET", "/", &ts, "", "00000000",
        ));
    }

    #[test]
    fn verify_rejects_empty_signature() {
        let ts = Utc::now().to_rfc3339();
        assert!(!RequestSignature::verify("secret", "GET", "/", &ts, "", ""));
    }

    // -- ApiKeySet tests ------------------------------------------------------

    #[test]
    fn new_keyset_has_no_secondary() {
        let ks = ApiKeySet::new("primary-key-1");
        assert_eq!(ks.primary, "primary-key-1");
        assert!(ks.secondary.is_none());
        assert!(ks.rotated_at.is_none());
    }

    #[test]
    fn is_valid_matches_primary() {
        let ks = ApiKeySet::new("my-key");
        assert!(ks.is_valid("my-key"));
        assert!(!ks.is_valid("other-key"));
    }

    #[test]
    fn rotate_moves_primary_to_secondary() {
        let mut ks = ApiKeySet::new("old-key");
        ks.rotate("new-key");

        assert_eq!(ks.primary, "new-key");
        assert_eq!(ks.secondary.as_deref(), Some("old-key"));
        assert!(ks.rotated_at.is_some());
    }

    #[test]
    fn rotate_with_metrics_behaves_identically_to_rotate() {
        let mut ks = ApiKeySet::new("old-key");
        ks.rotate_with_metrics("new-key");

        assert_eq!(ks.primary, "new-key");
        assert_eq!(ks.secondary.as_deref(), Some("old-key"));
        assert!(ks.rotated_at.is_some());
        assert!(ks.is_valid("new-key"));
        assert!(ks.is_valid("old-key"));
    }

    #[test]
    fn is_valid_accepts_secondary_after_rotation() {
        let mut ks = ApiKeySet::new("key-v1");
        ks.rotate("key-v2");

        assert!(ks.is_valid("key-v2"), "primary should match");
        assert!(ks.is_valid("key-v1"), "secondary should match");
        assert!(!ks.is_valid("key-v0"), "unknown key should not match");
    }

    #[test]
    fn double_rotation_drops_oldest_key() {
        let mut ks = ApiKeySet::new("v1");
        ks.rotate("v2");
        ks.rotate("v3");

        assert_eq!(ks.primary, "v3");
        assert_eq!(ks.secondary.as_deref(), Some("v2"));
        assert!(!ks.is_valid("v1"), "v1 should no longer be valid");
    }

    #[test]
    fn grace_period_active_immediately_after_rotation() {
        let mut ks = ApiKeySet::new("old");
        ks.rotate("new");
        assert!(ks.is_in_grace_period(3600));
    }

    #[test]
    fn no_grace_period_without_rotation() {
        let ks = ApiKeySet::new("key");
        assert!(!ks.is_in_grace_period(3600));
    }

    #[test]
    fn grace_period_expires() {
        let mut ks = ApiKeySet::new("old");
        ks.rotate("new");
        // Backdate the rotation timestamp.
        ks.rotated_at = Some(Utc::now() - Duration::seconds(7200));
        assert!(!ks.is_in_grace_period(3600));
    }

    // -- AccessDecision & PolicyEvaluator tests --------------------------------

    #[test]
    fn default_decision_is_deny() {
        let evaluator = PolicyEvaluator::new();
        let ctx = RequestContext::new("127.0.0.1", "hash", "/", "GET");
        assert_eq!(
            evaluator.evaluate(&ctx),
            AccessDecision::Deny("no matching policy rule".to_owned()),
        );
    }

    #[test]
    fn ip_deny_list_blocks_matching_ip() {
        let mut evaluator = PolicyEvaluator::new();
        evaluator.add_rule(Box::new(IpDenyListRule::new(vec![
            "10.0.0.1".to_owned(),
        ])));

        let blocked = RequestContext::new("10.0.0.1", "hash", "/", "GET");
        assert!(matches!(evaluator.evaluate(&blocked), AccessDecision::Deny(_)));

        let allowed = RequestContext::new("192.168.1.1", "hash", "/", "GET");
        // Falls through to default deny (no allow rule).
        assert!(matches!(evaluator.evaluate(&allowed), AccessDecision::Deny(_)));
    }

    #[test]
    fn allowed_keys_rule_permits_known_key() {
        let mut evaluator = PolicyEvaluator::new();
        evaluator.add_rule(Box::new(AllowedKeysRule::new(vec![
            "good-hash".to_owned(),
        ])));

        let ctx = RequestContext::new("1.2.3.4", "good-hash", "/", "GET");
        assert_eq!(evaluator.evaluate(&ctx), AccessDecision::Allow);
    }

    #[test]
    fn mutation_challenge_rule_requires_mfa_for_post() {
        let mut evaluator = PolicyEvaluator::new();
        evaluator.add_rule(Box::new(MutationChallengeRule));

        let ctx = RequestContext::new("1.2.3.4", "h", "/resource", "POST");
        assert_eq!(
            evaluator.evaluate(&ctx),
            AccessDecision::Challenge(ChallengeMethod::Mfa),
        );
    }

    #[test]
    fn mutation_challenge_rule_ignores_get() {
        let mut evaluator = PolicyEvaluator::new();
        evaluator.add_rule(Box::new(MutationChallengeRule));

        let ctx = RequestContext::new("1.2.3.4", "h", "/resource", "GET");
        // GET is not a mutation; rule does not match, so falls through to
        // default deny.
        assert!(matches!(evaluator.evaluate(&ctx), AccessDecision::Deny(_)));
    }

    #[test]
    fn first_matching_rule_wins() {
        let mut evaluator = PolicyEvaluator::new();
        evaluator.add_rule(Box::new(IpDenyListRule::new(vec![
            "evil.ip".to_owned(),
        ])));
        evaluator.add_rule(Box::new(AllowedKeysRule::new(vec![
            "known-hash".to_owned(),
        ])));

        // IP blocked even though key is allowed.
        let ctx = RequestContext::new("evil.ip", "known-hash", "/", "GET");
        assert!(matches!(evaluator.evaluate(&ctx), AccessDecision::Deny(_)));
    }

    #[test]
    fn allowed_key_rule_falls_through_for_unknown_key() {
        let mut evaluator = PolicyEvaluator::new();
        evaluator.add_rule(Box::new(AllowedKeysRule::new(vec![
            "known".to_owned(),
        ])));

        let ctx = RequestContext::new("1.2.3.4", "unknown", "/", "GET");
        assert_eq!(
            evaluator.evaluate(&ctx),
            AccessDecision::Deny("no matching policy rule".to_owned()),
        );
    }

    #[test]
    fn access_decision_display() {
        assert_eq!(AccessDecision::Allow.to_string(), "Allow");
        assert_eq!(
            AccessDecision::Deny("bad".to_owned()).to_string(),
            "Deny(bad)",
        );
        assert_eq!(
            AccessDecision::Challenge(ChallengeMethod::Captcha).to_string(),
            "Challenge(CAPTCHA)",
        );
    }

    // -- RequestContext builder tests -----------------------------------------

    #[test]
    fn request_context_builder() {
        let ctx = RequestContext::new("127.0.0.1", "abc", "/test", "GET")
            .with_user_agent("TestAgent/1.0");

        assert_eq!(ctx.ip_address, "127.0.0.1");
        assert_eq!(ctx.api_key_hash, "abc");
        assert_eq!(ctx.path, "/test");
        assert_eq!(ctx.method, "GET");
        assert_eq!(ctx.user_agent.as_deref(), Some("TestAgent/1.0"));
    }

    #[test]
    fn request_context_custom_timestamp() {
        let fixed_ts = Utc::now() - Duration::hours(1);
        let ctx = RequestContext::new("ip", "hash", "/", "GET")
            .with_timestamp(fixed_ts);
        assert_eq!(ctx.timestamp, fixed_ts);
    }

    // -- AccessLogger tests ---------------------------------------------------

    #[test]
    fn logger_records_and_retrieves_entries() {
        let logger = AccessLogger::new(100);
        assert!(logger.is_empty());

        let ctx = RequestContext::new("10.0.0.1", "h1", "/a", "GET");
        logger.record(ctx, AccessDecision::Allow);

        assert_eq!(logger.len(), 1);
        let recent = logger.recent_entries(10);
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].decision, AccessDecision::Allow);
    }

    #[test]
    fn logger_returns_newest_first() {
        let logger = AccessLogger::new(100);

        for i in 0..5 {
            let ctx = RequestContext::new("ip", &format!("key-{i}"), "/", "GET");
            logger.record(ctx, AccessDecision::Allow);
        }

        let recent = logger.recent_entries(3);
        assert_eq!(recent.len(), 3);
        assert_eq!(recent[0].context.api_key_hash, "key-4");
        assert_eq!(recent[1].context.api_key_hash, "key-3");
        assert_eq!(recent[2].context.api_key_hash, "key-2");
    }

    #[test]
    fn logger_filters_by_key_hash() {
        let logger = AccessLogger::new(100);

        let ctx_a = RequestContext::new("ip", "aaa", "/", "GET");
        logger.record(ctx_a, AccessDecision::Allow);

        let ctx_b = RequestContext::new("ip", "bbb", "/", "POST");
        logger.record(ctx_b, AccessDecision::Deny("denied".to_owned()));

        let ctx_a2 = RequestContext::new("ip", "aaa", "/other", "PUT");
        logger.record(ctx_a2, AccessDecision::Challenge(ChallengeMethod::Mfa));

        let for_a = logger.entries_for_key("aaa", 10);
        assert_eq!(for_a.len(), 2);
        // Newest first.
        assert_eq!(for_a[0].context.path, "/other");
        assert_eq!(for_a[1].context.path, "/");

        let for_b = logger.entries_for_key("bbb", 10);
        assert_eq!(for_b.len(), 1);

        let for_c = logger.entries_for_key("ccc", 10);
        assert!(for_c.is_empty());
    }

    #[test]
    fn logger_respects_capacity() {
        let logger = AccessLogger::new(3);

        for i in 0..5 {
            let ctx = RequestContext::new("ip", &format!("k{i}"), "/", "GET");
            logger.record(ctx, AccessDecision::Allow);
        }

        assert_eq!(logger.len(), 3);
        let recent = logger.recent_entries(10);
        assert_eq!(recent[0].context.api_key_hash, "k4");
        assert_eq!(recent[1].context.api_key_hash, "k3");
        assert_eq!(recent[2].context.api_key_hash, "k2");
    }

    #[test]
    fn logger_limit_zero_returns_empty() {
        let logger = AccessLogger::new(100);
        let ctx = RequestContext::new("ip", "h", "/", "GET");
        logger.record(ctx, AccessDecision::Allow);

        assert!(logger.recent_entries(0).is_empty());
        assert!(logger.entries_for_key("h", 0).is_empty());
    }

    // -- Integration-style test -----------------------------------------------

    #[test]
    fn end_to_end_sign_evaluate_log() {
        // 1. Sign a request.
        let ts = Utc::now().to_rfc3339();
        let sig = RequestSignature::sign("api-secret", "POST", "/events", &ts, "{}");
        assert!(RequestSignature::verify(
            "api-secret", "POST", "/events", &ts, "{}", &sig,
        ));

        // 2. Evaluate access policy.
        let mut evaluator = PolicyEvaluator::new();
        evaluator.add_rule(Box::new(AllowedKeysRule::new(vec![
            "trusted-hash".to_owned(),
        ])));

        let ctx = RequestContext::new("192.168.1.10", "trusted-hash", "/events", "POST");
        let decision = evaluator.evaluate(&ctx);
        assert_eq!(decision, AccessDecision::Allow);

        // 3. Log the outcome.
        let logger = AccessLogger::new(1000);
        logger.record(ctx, decision);
        assert_eq!(logger.len(), 1);

        let entries = logger.recent_entries(1);
        assert_eq!(entries[0].decision, AccessDecision::Allow);
        assert_eq!(entries[0].context.path, "/events");
    }

    #[test]
    fn end_to_end_rotation_and_validation() {
        let mut keys = ApiKeySet::new("original-key");
        assert!(keys.is_valid("original-key"));

        // Rotate to a new key.
        keys.rotate("rotated-key");
        assert!(keys.is_in_grace_period(3600));

        // Both keys should work during the grace period.
        assert!(keys.is_valid("rotated-key"));
        assert!(keys.is_valid("original-key"));

        // Simulate grace period expiry.
        keys.rotated_at = Some(Utc::now() - Duration::seconds(7200));
        assert!(!keys.is_in_grace_period(3600));

        // After another rotation the original key is evicted.
        keys.rotate("final-key");
        assert!(keys.is_valid("final-key"));
        assert!(keys.is_valid("rotated-key"));
        assert!(!keys.is_valid("original-key"));
    }

    // -- IP filter list (CIDR) tests -------------------------------------------

    fn ctx_from_ip(ip: &str) -> RequestContext {
        RequestContext::new(ip, "keyhash", "/", "GET")
    }

    #[test]
    fn ip_deny_list_matches_exact_address() {
        let rule = IpDenyListRule::new(vec!["203.0.113.5".to_string()]);
        assert!(matches!(
            rule.evaluate(&ctx_from_ip("203.0.113.5")),
            Some(AccessDecision::Deny(_))
        ));
        assert!(rule.evaluate(&ctx_from_ip("203.0.113.6")).is_none());
    }

    #[test]
    fn ip_deny_list_matches_cidr_block() {
        let rule = IpDenyListRule::new(vec!["203.0.113.0/24".to_string()]);
        assert!(matches!(
            rule.evaluate(&ctx_from_ip("203.0.113.200")),
            Some(AccessDecision::Deny(_))
        ));
        assert!(rule.evaluate(&ctx_from_ip("203.0.114.1")).is_none());
    }

    #[test]
    fn ip_deny_list_ipv6_cidr() {
        let rule = IpDenyListRule::new(vec!["2001:db8::/32".to_string()]);
        assert!(matches!(
            rule.evaluate(&ctx_from_ip("2001:db8::1")),
            Some(AccessDecision::Deny(_))
        ));
        assert!(rule.evaluate(&ctx_from_ip("2001:db9::1")).is_none());
    }

    #[test]
    fn ip_deny_list_ignores_malformed_entries_rather_than_panicking() {
        let rule = IpDenyListRule::new(vec!["not-an-ip".to_string(), "203.0.113.5".to_string()]);
        assert!(matches!(
            rule.evaluate(&ctx_from_ip("203.0.113.5")),
            Some(AccessDecision::Deny(_))
        ));
    }

    #[test]
    fn ip_allow_list_denies_anything_not_listed() {
        let rule = IpAllowListRule::new(vec!["203.0.113.0/24".to_string()]);
        assert!(rule.evaluate(&ctx_from_ip("203.0.113.10")).is_none());
        assert!(matches!(
            rule.evaluate(&ctx_from_ip("198.51.100.1")),
            Some(AccessDecision::Deny(_))
        ));
    }

    #[test]
    fn ip_family_mismatch_never_matches() {
        let rule = IpDenyListRule::new(vec!["203.0.113.0/24".to_string()]);
        assert!(rule.evaluate(&ctx_from_ip("2001:db8::1")).is_none());
    }
}
