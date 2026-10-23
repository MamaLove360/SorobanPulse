/// Webhook Request Signing (Issue: enhance webhook security with request signing)
///
/// Complements [`crate::webhook_verification`] (subscriber-side verification docs)
/// with the sender-side signing implementation: HMAC-SHA256 signing, multiple
/// active signing keys, key rotation, and signature metadata headers.
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use hmac::Mac;
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;


pub const SIGNATURE_HEADER: &str = "X-Signature-256";
pub const SIGNATURE_KEY_ID_HEADER: &str = "X-Signature-Key-Id";
pub const SIGNATURE_TIMESTAMP_HEADER: &str = "X-Signature-Timestamp";
pub const SIGNATURE_ALGORITHM_HEADER: &str = "X-Signature-Algorithm";

#[derive(Debug, thiserror::Error)]
pub enum SigningError {
    #[error("unknown signing key id: {0}")]
    UnknownKeyId(String),
    #[error("no active signing key configured")]
    NoActiveKey,
    #[error("invalid signature header format")]
    InvalidFormat,
    #[error("signature mismatch")]
    Mismatch,
    #[error("signature timestamp outside allowed skew")]
    TimestampSkew,
}

/// A single HMAC signing key with an identifier used for rotation and
/// multi-key support (subscribers can accept signatures from any known key).
#[derive(Clone)]
pub struct SigningKey {
    pub id: String,
    pub secret: Vec<u8>,
    pub created_at: u64,
    /// Keys marked inactive are still accepted for verification (grace period
    /// during rotation) but are never used to sign new deliveries.
    pub active: bool,
}

impl SigningKey {
    pub fn new(id: impl Into<String>, secret: impl Into<Vec<u8>>) -> Self {
        Self {
            id: id.into(),
            secret: secret.into(),
            created_at: now_secs(),
            active: true,
        }
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// The computed signature plus the metadata headers a delivery should carry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedRequest {
    pub signature_header: String,
    pub key_id: String,
    pub timestamp: u64,
    pub algorithm: &'static str,
}

impl SignedRequest {
    /// All headers that should be attached to the outgoing webhook request.
    pub fn headers(&self) -> HashMap<String, String> {
        let mut headers = HashMap::new();
        headers.insert(SIGNATURE_HEADER.to_string(), self.signature_header.clone());
        headers.insert(SIGNATURE_KEY_ID_HEADER.to_string(), self.key_id.clone());
        headers.insert(SIGNATURE_TIMESTAMP_HEADER.to_string(), self.timestamp.to_string());
        headers.insert(SIGNATURE_ALGORITHM_HEADER.to_string(), self.algorithm.to_string());
        headers
    }
}

/// Manages the set of signing keys used to sign outgoing webhook payloads and
/// supports zero-downtime key rotation: a new key is added as active, the
/// previous key is kept (marked inactive) for a grace period so subscribers
/// have time to pick up the new key before it's removed entirely.
#[derive(Default)]
pub struct WebhookKeyManager {
    keys: RwLock<Vec<SigningKey>>,
}

impl WebhookKeyManager {
    pub fn new() -> Self {
        Self { keys: RwLock::new(Vec::new()) }
    }

    /// Add or replace a key by id.
    pub fn add_key(&self, key: SigningKey) {
        let mut keys = self.keys.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        keys.retain(|k| k.id != key.id);
        keys.push(key);
    }

    /// Rotate signing keys: the given new key becomes the sole active signing
    /// key; all existing keys are retained but marked inactive (verification
    /// still succeeds for them until `remove_key` is called after the grace
    /// period elapses).
    pub fn rotate(&self, new_key: SigningKey) {
        let mut keys = self.keys.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        for k in keys.iter_mut() {
            k.active = false;
        }
        keys.retain(|k| k.id != new_key.id);
        keys.push(new_key);
    }

    pub fn remove_key(&self, key_id: &str) {
        self.keys.write().unwrap_or_else(std::sync::PoisonError::into_inner).retain(|k| k.id != key_id);
    }

    fn active_key(&self) -> Option<SigningKey> {
        self.keys
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .rev()
            .find(|k| k.active)
            .cloned()
    }

    fn find_key(&self, key_id: &str) -> Option<SigningKey> {
        self.keys.read().unwrap_or_else(std::sync::PoisonError::into_inner).iter().find(|k| k.id == key_id).cloned()
    }

    /// Sign a payload with the current active key, producing signature +
    /// metadata headers to attach to the outgoing webhook request.
    pub fn sign(&self, payload: &[u8]) -> Result<SignedRequest, SigningError> {
        let key = self.active_key().ok_or(SigningError::NoActiveKey)?;
        let timestamp = now_secs();
        let digest = compute_signature(&key.secret, timestamp, payload);

        crate::metrics::record_webhook_signature_created(&key.id);

        Ok(SignedRequest {
            signature_header: format!("sha256={}", digest),
            key_id: key.id,
            timestamp,
            algorithm: "hmac-sha256",
        })
    }

    /// Verify an inbound/replayed signature against the known key set,
    /// enforcing a timestamp skew window to mitigate replay attacks.
    pub fn verify(
        &self,
        key_id: &str,
        timestamp: u64,
        payload: &[u8],
        provided_signature: &str,
        max_skew_secs: u64,
    ) -> Result<(), SigningError> {
        let key = self.find_key(key_id).ok_or_else(|| SigningError::UnknownKeyId(key_id.to_string()))?;

        let now = now_secs();
        if now.abs_diff(timestamp) > max_skew_secs {
            crate::metrics::record_webhook_signature_verified(key_id, false);
            return Err(SigningError::TimestampSkew);
        }

        let (algo, provided) = provided_signature
            .split_once('=')
            .ok_or(SigningError::InvalidFormat)?;
        if algo != "sha256" {
            return Err(SigningError::InvalidFormat);
        }

        let expected = compute_signature(&key.secret, timestamp, payload);
        let ok = expected.as_bytes().ct_eq(provided.as_bytes()).into();
        crate::metrics::record_webhook_signature_verified(key_id, ok);
        if ok {
            Ok(())
        } else {
            Err(SigningError::Mismatch)
        }
    }
}

/// Computes `HMAC-SHA256(secret, "{timestamp}.{payload}")` as a lowercase hex digest.
/// Binding the timestamp into the signed material (rather than sending it
/// unsigned alongside) prevents tampering with the timestamp to bypass skew checks.
fn compute_signature(secret: &[u8], timestamp: u64, payload: &[u8]) -> String {
    let mut mac = crate::hmac_util::hmac_sha256(secret);
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(payload);
    hex::encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_and_verify_round_trip() {
        let manager = WebhookKeyManager::new();
        manager.add_key(SigningKey::new("key-1", b"super-secret".to_vec()));

        let payload = br#"{"event":"test"}"#;
        let signed = manager.sign(payload).unwrap();

        assert!(signed.signature_header.starts_with("sha256="));
        assert_eq!(signed.key_id, "key-1");

        manager
            .verify(&signed.key_id, signed.timestamp, payload, &signed.signature_header, 300)
            .expect("verification should succeed");
    }

    #[test]
    fn verify_rejects_tampered_payload() {
        let manager = WebhookKeyManager::new();
        manager.add_key(SigningKey::new("key-1", b"super-secret".to_vec()));

        let signed = manager.sign(b"original").unwrap();
        let result = manager.verify(&signed.key_id, signed.timestamp, b"tampered", &signed.signature_header, 300);
        assert!(matches!(result, Err(SigningError::Mismatch)));
    }

    #[test]
    fn verify_rejects_unknown_key() {
        let manager = WebhookKeyManager::new();
        let result = manager.verify("nonexistent", now_secs(), b"payload", "sha256=deadbeef", 300);
        assert!(matches!(result, Err(SigningError::UnknownKeyId(_))));
    }

    #[test]
    fn verify_rejects_stale_timestamp() {
        let manager = WebhookKeyManager::new();
        manager.add_key(SigningKey::new("key-1", b"secret".to_vec()));

        let old_timestamp = now_secs().saturating_sub(10_000);
        let digest = compute_signature(b"secret", old_timestamp, b"payload");
        let sig = format!("sha256={digest}");

        let result = manager.verify("key-1", old_timestamp, b"payload", &sig, 300);
        assert!(matches!(result, Err(SigningError::TimestampSkew)));
    }

    #[test]
    fn rotation_keeps_old_key_valid_during_grace_period() {
        let manager = WebhookKeyManager::new();
        manager.add_key(SigningKey::new("key-1", b"old-secret".to_vec()));
        let old_signed = manager.sign(b"payload").unwrap();
        assert_eq!(old_signed.key_id, "key-1");

        manager.rotate(SigningKey::new("key-2", b"new-secret".to_vec()));

        // New signatures use the newly active key.
        let new_signed = manager.sign(b"payload").unwrap();
        assert_eq!(new_signed.key_id, "key-2");

        // Old key still verifies (grace period) even though it's inactive for signing.
        manager
            .verify("key-1", old_signed.timestamp, b"payload", &old_signed.signature_header, 300)
            .expect("old key should still verify during grace period");
    }

    #[test]
    fn remove_key_invalidates_verification() {
        let manager = WebhookKeyManager::new();
        manager.add_key(SigningKey::new("key-1", b"secret".to_vec()));
        let signed = manager.sign(b"payload").unwrap();

        manager.remove_key("key-1");

        let result = manager.verify("key-1", signed.timestamp, b"payload", &signed.signature_header, 300);
        assert!(matches!(result, Err(SigningError::UnknownKeyId(_))));
    }

    #[test]
    fn signed_request_produces_expected_headers() {
        let manager = WebhookKeyManager::new();
        manager.add_key(SigningKey::new("key-1", b"secret".to_vec()));
        let signed = manager.sign(b"payload").unwrap();

        let headers = signed.headers();
        assert_eq!(headers.get(SIGNATURE_KEY_ID_HEADER).unwrap(), "key-1");
        assert_eq!(headers.get(SIGNATURE_ALGORITHM_HEADER).unwrap(), "hmac-sha256");
        assert!(headers.contains_key(SIGNATURE_TIMESTAMP_HEADER));
        assert!(headers.get(SIGNATURE_HEADER).unwrap().starts_with("sha256="));
    }

    #[test]
    fn sign_fails_with_no_active_key() {
        let manager = WebhookKeyManager::new();
        let result = manager.sign(b"payload");
        assert!(matches!(result, Err(SigningError::NoActiveKey)));
    }
}
