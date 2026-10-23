//! Issue #267: XDR validation for Soroban event data using the `stellar-xdr` crate.
//! Issue #370: Contract ID Strkey format validation.
//! Issue #616: Full validation module covering contract IDs, tx hashes, and ScVal types,
//!             with custom error types, pass/fail counters, and API doc annotations.
//!
//! ## Validation rules
//!
//! | Field           | Rule                                                       |
//! |-----------------|-------------------------------------------------------------|
//! | `contract_id`   | C-type Strkey: starts with `C`, 56 chars, base32 (A-Z 2-7)|
//! | `tx_hash`       | 64 lowercase hex characters (SHA-256 of the XDR envelope)  |
//! | `event_data.value` | Valid `ScVal` JSON when non-null                        |
//! | `event_data.topic` | Every element is a valid `ScVal` JSON                   |

use serde_json::Value;
use thiserror::Error;
use tracing::warn;

use crate::metrics;
use crate::scval_format::{decode_scval, MAX_SCVAL_JSON_DEPTH};

/// Structured error returned when an individual Stellar type fails validation.
#[derive(Debug, Error, PartialEq)]
pub enum XdrValidationError {
    #[error("contract_id '{0}' is not a valid Stellar C-type Strkey (expected 56 base32 chars starting with 'C')")]
    InvalidContractId(String),

    #[error("tx_hash '{0}' is not valid (expected 64 lowercase hex chars)")]
    InvalidTxHash(String),

    #[error("event_data.value is not a valid ScVal")]
    InvalidScValue,

    #[error("event_data.topic[{index}] is not a valid ScVal")]
    InvalidTopicElement { index: usize },
}

/// Validate that a JSON value can be deserialized as a `ScVal`.
///
/// Input nested deeper than [`MAX_SCVAL_JSON_DEPTH`] is rejected (issue #1226).
fn is_valid_sc_val(v: &Value) -> bool {
    decode_scval(v).is_some()
}

/// Validate that a `contract_id` is a valid Stellar Strkey (C-type).
///
/// Rules:
/// - Exactly 56 characters
/// - Starts with `'C'`
/// - All characters are valid base32 (`A–Z`, `2–7`)
pub fn validate_contract_id(contract_id: &str) -> bool {
    if contract_id.len() != 56 {
        return false;
    }
    if !contract_id.starts_with('C') {
        return false;
    }
    contract_id
        .chars()
        .all(|c| matches!(c, 'A'..='Z' | '2'..='7'))
}

/// Validate that a `tx_hash` conforms to the Stellar transaction hash format.
///
/// Rules:
/// - Exactly 64 characters
/// - All characters are ASCII hex digits (`0–9`, `a–f`, `A–F`)
pub fn validate_tx_hash(tx_hash: &str) -> bool {
    if tx_hash.len() != 64 {
        return false;
    }
    tx_hash.chars().all(|c| c.is_ascii_hexdigit())
}

/// Validate a `contract_id` and return a typed error on failure.
pub fn validate_contract_id_strict(contract_id: &str) -> Result<(), XdrValidationError> {
    if validate_contract_id(contract_id) {
        Ok(())
    } else {
        Err(XdrValidationError::InvalidContractId(contract_id.to_string()))
    }
}

/// Validate a `tx_hash` and return a typed error on failure.
pub fn validate_tx_hash_strict(tx_hash: &str) -> Result<(), XdrValidationError> {
    if validate_tx_hash(tx_hash) {
        Ok(())
    } else {
        Err(XdrValidationError::InvalidTxHash(tx_hash.to_string()))
    }
}

/// Validate the `contract_id`, `tx_hash`, `event_data.value`, and `event_data.topic`
/// fields of a Soroban event.
///
/// Returns `true` if all validations pass, `false` if the event should be skipped.
/// On failure, logs a WARN and increments appropriate metrics.
/// On success, increments the XDR-valid counter.
pub fn validate_xdr(
    tx_hash: &str,
    contract_id: &str,
    ledger: u64,
    value: &Value,
    topic: Option<&Vec<Value>>,
) -> bool {
    // Validate contract_id format
    if !validate_contract_id(contract_id) {
        warn!(
            tx_hash = %tx_hash,
            contract_id = %contract_id,
            ledger = ledger,
            "contract_id failed Strkey validation, skipping event",
        );
        metrics::record_invalid_contract_id();
        metrics::record_xdr_validation_fail("contract_id");
        return false;
    }

    // Validate tx_hash format
    if !validate_tx_hash(tx_hash) {
        warn!(
            tx_hash = %tx_hash,
            contract_id = %contract_id,
            ledger = ledger,
            "tx_hash failed hex validation, skipping event",
        );
        metrics::record_xdr_invalid();
        metrics::record_xdr_validation_fail("tx_hash");
        return false;
    }

    // Null value is acceptable (no XDR to validate)
    if !value.is_null() && !is_valid_sc_val(value) {
        warn!(
            tx_hash = %tx_hash,
            contract_id = %contract_id,
            ledger = ledger,
            raw_value = %value,
            "event_data.value failed XDR/ScVal validation, skipping event",
        );
        metrics::record_xdr_invalid();
        metrics::record_xdr_validation_fail("sc_value");
        return false;
    }

    if let Some(topics) = topic {
        for (i, t) in topics.iter().enumerate() {
            if !is_valid_sc_val(t) {
                warn!(
                    tx_hash = %tx_hash,
                    contract_id = %contract_id,
                    ledger = ledger,
                    topic_index = i,
                    raw_topic = %t,
                    "event_data.topic[{}] failed XDR/ScVal validation, skipping event",
                    i,
                );
                metrics::record_xdr_invalid();
                metrics::record_xdr_validation_fail("sc_topic");
                return false;
            }
        }
    }

    metrics::record_xdr_validation_pass();
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const VALID_CONTRACT_ID: &str =
        "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF";
    const VALID_TX_HASH: &str =
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn call(value: Value, topic: Option<Vec<Value>>) -> bool {
        validate_xdr(VALID_TX_HASH, VALID_CONTRACT_ID, 100, &value, topic.as_ref())
    }

    #[test]
    fn null_value_is_valid() {
        assert!(call(Value::Null, None));
    }

    #[test]
    fn valid_sc_val_void_passes() {
        let v = json!({"void": null});
        assert!(call(v, None));
    }

    #[test]
    fn valid_sc_val_bool_passes() {
        let v = json!({"bool": true});
        assert!(call(v, None));
    }

    #[test]
    fn deeply_nested_value_fails_without_panicking() {
        let mut v = json!({"u32": 1});
        for _ in 0..MAX_SCVAL_JSON_DEPTH {
            v = json!({"vec": [v]});
        }
        assert!(!call(v, None));
    }

    #[test]
    fn invalid_value_fails() {
        let v = json!("not_a_scval");
        assert!(!call(v, None));
    }

    #[test]
    fn invalid_number_value_fails() {
        let v = json!(42);
        assert!(!call(v, None));
    }

    #[test]
    fn valid_topic_passes() {
        let v = Value::Null;
        let topic = vec![json!({"void": null}), json!({"bool": false})];
        assert!(call(v, Some(topic)));
    }

    #[test]
    fn invalid_topic_element_fails() {
        let v = Value::Null;
        let topic = vec![json!({"void": null}), json!("bad_topic")];
        assert!(!call(v, Some(topic)));
    }

    #[test]
    fn empty_topic_passes() {
        assert!(call(Value::Null, Some(vec![])));
    }

    #[test]
    fn valid_c_type_strkey() {
        assert!(validate_contract_id(VALID_CONTRACT_ID));
    }

    #[test]
    fn invalid_strkey_wrong_type() {
        assert!(!validate_contract_id(
            "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"
        ));
    }

    #[test]
    fn invalid_strkey_wrong_length() {
        assert!(!validate_contract_id(
            "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        ));
    }

    #[test]
    fn invalid_strkey_invalid_chars() {
        assert!(!validate_contract_id(
            "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA@WHF"
        ));
    }

    #[test]
    fn invalid_strkey_lowercase() {
        assert!(!validate_contract_id(
            "caaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaawhf"
        ));
    }

    #[test]
    fn contract_id_validation_rejects_invalid_format() {
        let v = Value::Null;
        assert!(!validate_xdr(VALID_TX_HASH, "INVALID", 100, &v, None));
    }

    // tx_hash validation tests

    #[test]
    fn valid_tx_hash_passes() {
        assert!(validate_tx_hash(VALID_TX_HASH));
    }

    #[test]
    fn tx_hash_uppercase_hex_passes() {
        let hash = "A".repeat(64);
        assert!(validate_tx_hash(&hash));
    }

    #[test]
    fn tx_hash_wrong_length_fails() {
        assert!(!validate_tx_hash(&"a".repeat(63)));
        assert!(!validate_tx_hash(&"a".repeat(65)));
        assert!(!validate_tx_hash(""));
    }

    #[test]
    fn tx_hash_non_hex_fails() {
        let bad = format!("{}z{}", "a".repeat(32), "a".repeat(31));
        assert!(!validate_tx_hash(&bad));
    }

    #[test]
    fn validate_xdr_rejects_bad_tx_hash() {
        let v = Value::Null;
        assert!(!validate_xdr("INVALID_HASH", VALID_CONTRACT_ID, 100, &v, None));
    }

    // Strict helpers

    #[test]
    fn strict_contract_id_ok() {
        assert!(validate_contract_id_strict(VALID_CONTRACT_ID).is_ok());
    }

    #[test]
    fn strict_contract_id_err() {
        let err = validate_contract_id_strict("BAD").unwrap_err();
        assert!(matches!(err, XdrValidationError::InvalidContractId(_)));
    }

    #[test]
    fn strict_tx_hash_ok() {
        assert!(validate_tx_hash_strict(VALID_TX_HASH).is_ok());
    }

    #[test]
    fn strict_tx_hash_err() {
        let err = validate_tx_hash_strict("bad").unwrap_err();
        assert!(matches!(err, XdrValidationError::InvalidTxHash(_)));
    }
}
