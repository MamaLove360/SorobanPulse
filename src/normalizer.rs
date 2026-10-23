use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;

/// A single normalization rule loaded from the DB.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct NormalizationRule {
    pub pointer: String,
    pub transform: String,
    pub params: Value,
}

/// Built-in transform names.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Transform {
    DivideByDecimals,
    HexToDecimal,
    Base64Decode,
}

impl std::str::FromStr for Transform {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "divide_by_decimals" => Ok(Transform::DivideByDecimals),
            "hex_to_decimal" => Ok(Transform::HexToDecimal),
            "base64_decode" => Ok(Transform::Base64Decode),
            other => Err(format!("unknown transform: {other}")),
        }
    }
}

/// Apply a single transform to a JSON value, returning the transformed value.
pub fn apply_transform(
    transform: &Transform,
    params: &Value,
    value: &Value,
) -> Result<Value, String> {
    match transform {
        Transform::DivideByDecimals => {
            let decimals = params
                .get("decimals")
                .and_then(|v| v.as_u64())
                .ok_or("divide_by_decimals requires params.decimals (u64)")?;
            let raw = value
                .as_i64()
                .or_else(|| value.as_str().and_then(|s| s.parse::<i64>().ok()))
                .ok_or_else(|| format!("divide_by_decimals: expected integer, got {value}"))?;
            // 10^19 already overflows i64, so reject exponents that cannot be
            // represented instead of panicking (or silently truncating `decimals`).
            let divisor = u32::try_from(decimals)
                .ok()
                .and_then(|d| 10_i64.checked_pow(d))
                .ok_or_else(|| {
                    format!("divide_by_decimals: decimals {decimals} is too large (max 18)")
                })?;
            // Return as a JSON number (f64) — sufficient for display purposes
            Ok(Value::from(raw as f64 / divisor as f64))
        }
        Transform::HexToDecimal => {
            let hex = value
                .as_str()
                .ok_or_else(|| format!("hex_to_decimal: expected string, got {value}"))?
                .trim_start_matches("0x");
            let n = u128::from_str_radix(hex, 16).map_err(|e| format!("hex_to_decimal: {e}"))?;
            // u128 may exceed f64 precision; store as string to preserve accuracy
            Ok(Value::String(n.to_string()))
        }
        Transform::Base64Decode => {
            let encoded = value
                .as_str()
                .ok_or_else(|| format!("base64_decode: expected string, got {value}"))?;
            let bytes = STANDARD
                .decode(encoded)
                .map_err(|e| format!("base64_decode: {e}"))?;
            match std::str::from_utf8(&bytes) {
                Ok(s) => Ok(Value::String(s.to_owned())),
                Err(_) => {
                    // Not valid UTF-8 — return hex representation
                    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
                    Ok(Value::String(hex))
                }
            }
        }
    }
}

/// Resolve a JSON Pointer (RFC 6901) to a mutable reference within a Value.
fn pointer_get(value: &Value, pointer: &str) -> Option<Value> {
    if pointer.is_empty() || pointer == "/" {
        return Some(value.clone());
    }
    let mut current = value;
    for token in pointer.trim_start_matches('/').split('/') {
        let token = token.replace("~1", "/").replace("~0", "~");
        current = match current {
            Value::Object(map) => map.get(&token)?,
            Value::Array(arr) => {
                let idx: usize = token.parse().ok()?;
                arr.get(idx)?
            }
            _ => return None,
        };
    }
    Some(current.clone())
}

/// Set a value at a JSON Pointer path (mutates `root`).
fn pointer_set(root: &mut Value, pointer: &str, new_val: Value) {
    if pointer.is_empty() || pointer == "/" {
        *root = new_val;
        return;
    }
    let tokens: Vec<String> = pointer
        .trim_start_matches('/')
        .split('/')
        .map(|t| t.replace("~1", "/").replace("~0", "~"))
        .collect();
    let mut current = root;
    for (i, token) in tokens.iter().enumerate() {
        let is_last = i == tokens.len() - 1;
        current = match current {
            Value::Object(map) => {
                if is_last {
                    map.insert(token.clone(), new_val);
                    return;
                }
                map.entry(token.clone())
                    .or_insert(Value::Object(Default::default()))
            }
            Value::Array(arr) => {
                if let Ok(idx) = token.parse::<usize>() {
                    if is_last {
                        if idx < arr.len() {
                            arr[idx] = new_val;
                        }
                        return;
                    }
                    if idx < arr.len() {
                        &mut arr[idx]
                    } else {
                        return;
                    }
                } else {
                    return;
                }
            }
            _ => return,
        };
    }
}

/// Run the normalization pipeline for a given contract and event_data.
/// Returns `None` if there are no rules for this contract or if event_data is null/empty.
pub fn normalize(rules: &[NormalizationRule], event_data: &Value) -> Option<Value> {
    if rules.is_empty() {
        return None;
    }
    
    // Handle null or empty event_data
    if event_data.is_null() || (event_data.is_object() && event_data.as_object().map_or(false, |o| o.is_empty())) {
        tracing::warn!("event_data is null or empty, skipping normalization");
        return None;
    }
    
    let mut normalized = event_data.clone();
    for rule in rules {
        let transform: Transform = match rule.transform.parse() {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(transform = %rule.transform, error = %e, "Unknown transform, skipping");
                continue;
            }
        };
        let current = match pointer_get(&normalized, &rule.pointer) {
            Some(v) => v,
            None => {
                tracing::debug!(pointer = %rule.pointer, "JSON Pointer not found in event_data, skipping");
                continue;
            }
        };
        match apply_transform(&transform, &rule.params, &current) {
            Ok(new_val) => pointer_set(&mut normalized, &rule.pointer, new_val),
            Err(e) => {
                tracing::warn!(pointer = %rule.pointer, error = %e, "Transform failed, skipping")
            }
        }
    }
    Some(normalized)
}

/// Load normalization rules for a contract from the DB.
pub async fn load_rules(pool: &PgPool, contract_id: &str) -> Vec<NormalizationRule> {
    sqlx::query_as::<_, NormalizationRule>(
        "SELECT pointer, transform, params FROM normalization_rules WHERE contract_id = $1 ORDER BY created_at",
    )
    .bind(contract_id)
    .fetch_all(pool)
    .await
    .unwrap_or_default()
}

// ---------------------------------------------------------------------
// Schema mapping configuration
// ---------------------------------------------------------------------

/// Expected JSON type for a field in the normalized schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    String,
    Number,
    Bool,
    Object,
    Array,
    Any,
}

impl FieldType {
    fn matches(&self, value: &Value) -> bool {
        match self {
            FieldType::String => value.is_string(),
            FieldType::Number => value.is_number(),
            FieldType::Bool => value.is_boolean(),
            FieldType::Object => value.is_object(),
            FieldType::Array => value.is_array(),
            FieldType::Any => true,
        }
    }
}

/// A single field definition in a normalized schema: where the field lives
/// (JSON Pointer) and what type it must resolve to after normalization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaField {
    pub pointer: String,
    pub field_type: FieldType,
    pub required: bool,
}

/// Declarative mapping describing the shape event data must conform to
/// once normalization rules have been applied. This is the "consistent
/// schema" that normalized events are validated against.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NormalizedSchema {
    pub name: String,
    pub fields: Vec<SchemaField>,
}

impl NormalizedSchema {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into(), fields: Vec::new() }
    }

    pub fn with_field(mut self, pointer: impl Into<String>, field_type: FieldType, required: bool) -> Self {
        self.fields.push(SchemaField { pointer: pointer.into(), field_type, required });
        self
    }
}

/// A single schema validation failure.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaViolation {
    pub pointer: String,
    pub reason: String,
}

/// Validates a normalized JSON value against a `NormalizedSchema`,
/// returning any violations found (empty means the value is valid).
pub fn validate_schema(schema: &NormalizedSchema, value: &Value) -> Vec<SchemaViolation> {
    let mut violations = Vec::new();
    for field in &schema.fields {
        match pointer_get(value, &field.pointer) {
            Some(v) if !v.is_null() => {
                if !field.field_type.matches(&v) {
                    violations.push(SchemaViolation {
                        pointer: field.pointer.clone(),
                        reason: format!("expected {:?}, got {v}", field.field_type),
                    });
                }
            }
            _ => {
                if field.required {
                    violations.push(SchemaViolation {
                        pointer: field.pointer.clone(),
                        reason: "required field missing".to_string(),
                    });
                }
            }
        }
    }
    violations
}

// ---------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------

/// Running counters for normalization outcomes, intended to be exposed
/// via `src/metrics.rs`.
#[derive(Debug, Clone, Default)]
pub struct NormalizationMetrics {
    pub attempted: u64,
    pub succeeded: u64,
    pub rolled_back: u64,
    pub skipped: u64,
}

impl NormalizationMetrics {
    pub fn success_rate(&self) -> f64 {
        if self.attempted == 0 {
            return 1.0;
        }
        self.succeeded as f64 / self.attempted as f64
    }
}

// ---------------------------------------------------------------------
// Validated normalization with rollback
// ---------------------------------------------------------------------

/// Result of a validated normalization attempt.
#[derive(Debug, Clone)]
pub enum NormalizationOutcome {
    /// Normalization applied and the result passed schema validation.
    Applied(Value),
    /// Normalization rules produced a value that failed schema validation;
    /// the original, pre-normalization data was kept instead.
    RolledBack { original: Value, violations: Vec<SchemaViolation> },
    /// No rules applied (empty rule set or empty/null input).
    Skipped,
}

/// Applies normalization rules and validates the result against the given
/// schema. If validation fails, the transformation is rolled back and the
/// original event data is returned instead, so a bad rule can never
/// corrupt stored event data. Updates `metrics` as a side effect.
pub fn normalize_with_validation(
    rules: &[NormalizationRule],
    event_data: &Value,
    schema: &NormalizedSchema,
    metrics: &mut NormalizationMetrics,
) -> NormalizationOutcome {
    metrics.attempted += 1;

    let normalized = match normalize(rules, event_data) {
        Some(v) => v,
        None => {
            metrics.skipped += 1;
            return NormalizationOutcome::Skipped;
        }
    };

    let violations = validate_schema(schema, &normalized);
    if violations.is_empty() {
        metrics.succeeded += 1;
        NormalizationOutcome::Applied(normalized)
    } else {
        metrics.rolled_back += 1;
        tracing::warn!(
            schema = %schema.name,
            violation_count = violations.len(),
            "normalization result failed schema validation, rolling back to original data"
        );
        NormalizationOutcome::RolledBack { original: event_data.clone(), violations }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rule(pointer: &str, transform: &str, params: Value) -> NormalizationRule {
        NormalizationRule {
            pointer: pointer.to_string(),
            transform: transform.to_string(),
            params,
        }
    }

    // --- divide_by_decimals ---

    #[test]
    fn divide_by_decimals_integer() {
        let t = Transform::DivideByDecimals;
        let result = apply_transform(&t, &json!({"decimals": 7}), &json!(10_000_000)).unwrap();
        assert_eq!(result, json!(1.0));
    }

    #[test]
    fn divide_by_decimals_string_input() {
        let t = Transform::DivideByDecimals;
        let result = apply_transform(&t, &json!({"decimals": 2}), &json!("500")).unwrap();
        assert_eq!(result, json!(5.0));
    }

    #[test]
    fn divide_by_decimals_missing_params() {
        let t = Transform::DivideByDecimals;
        assert!(apply_transform(&t, &json!({}), &json!(100)).is_err());
    }

    #[test]
    fn divide_by_decimals_max_supported_decimals() {
        let t = Transform::DivideByDecimals;
        let result = apply_transform(&t, &json!({"decimals": 18}), &json!(0)).unwrap();
        assert_eq!(result, json!(0.0));
    }

    /// Regression for the fuzz_normalizer crash (#1228): `10_i64.pow(38)`
    /// overflowed and panicked for any integer input.
    #[test]
    fn divide_by_decimals_overflowing_decimals_returns_err() {
        let t = Transform::DivideByDecimals;
        for decimals in [19_u64, 38, u64::from(u32::MAX) + 1, u64::MAX] {
            assert!(apply_transform(&t, &json!({"decimals": decimals}), &json!(0)).is_err());
        }
    }

    // --- hex_to_decimal ---

    #[test]
    fn hex_to_decimal_plain() {
        let t = Transform::HexToDecimal;
        let result = apply_transform(&t, &json!({}), &json!("ff")).unwrap();
        assert_eq!(result, json!("255"));
    }

    #[test]
    fn hex_to_decimal_with_prefix() {
        let t = Transform::HexToDecimal;
        let result = apply_transform(&t, &json!({}), &json!("0x1a")).unwrap();
        assert_eq!(result, json!("26"));
    }

    #[test]
    fn hex_to_decimal_invalid() {
        let t = Transform::HexToDecimal;
        assert!(apply_transform(&t, &json!({}), &json!("xyz")).is_err());
    }

    // --- base64_decode ---

    #[test]
    fn base64_decode_utf8() {
        let t = Transform::Base64Decode;
        // "hello" in standard base64
        let result = apply_transform(&t, &json!({}), &json!("aGVsbG8=")).unwrap();
        assert_eq!(result, json!("hello"));
    }

    #[test]
    fn base64_decode_binary_returns_hex() {
        let t = Transform::Base64Decode;
        // bytes [0xff, 0xfe] — not valid UTF-8
        let result = apply_transform(&t, &json!({}), &json!("//4=")).unwrap();
        assert_eq!(result, json!("fffe"));
    }

    #[test]
    fn base64_decode_invalid() {
        let t = Transform::Base64Decode;
        assert!(apply_transform(&t, &json!({}), &json!("!!!")).is_err());
    }

    // --- pipeline ---

    #[test]
    fn normalize_applies_rules_in_order() {
        let rules = vec![rule(
            "/value/amount",
            "divide_by_decimals",
            json!({"decimals": 2}),
        )];
        let data = json!({"value": {"amount": 1000}, "topic": []});
        let result = normalize(&rules, &data).unwrap();
        assert_eq!(result["value"]["amount"], json!(10.0));
        // original untouched fields preserved
        assert_eq!(result["topic"], json!([]));
    }

    #[test]
    fn normalize_no_rules_returns_none() {
        let data = json!({"value": {}, "topic": []});
        assert!(normalize(&[], &data).is_none());
    }

    #[test]
    fn normalize_missing_pointer_skips() {
        let rules = vec![rule("/value/nonexistent", "hex_to_decimal", json!({}))];
        let data = json!({"value": {}, "topic": []});
        // Should not panic, just skip
        let result = normalize(&rules, &data).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn normalize_null_event_data_returns_none() {
        let rules = vec![rule("/value/amount", "divide_by_decimals", json!({"decimals": 2}))];
        let data = Value::Null;
        assert!(normalize(&rules, &data).is_none());
    }

    #[test]
    fn normalize_empty_object_returns_none() {
        let rules = vec![rule("/value/amount", "divide_by_decimals", json!({"decimals": 2}))];
        let data = json!({});
        assert!(normalize(&rules, &data).is_none());
    }

    #[test]
    fn normalize_diagnostic_event_with_missing_keys() {
        let rules = vec![rule("/value", "hex_to_decimal", json!({}))];
        let data = json!({"topic": []});
        // Should not panic, just return the data as-is
        let result = normalize(&rules, &data);
        assert!(result.is_some());
    }

    #[test]
    fn schema_validation_passes_for_conforming_value() {
        let schema = NormalizedSchema::new("transfer")
            .with_field("/amount", FieldType::Number, true)
            .with_field("/from", FieldType::String, true);
        let value = json!({"amount": 1.5, "from": "GABC"});
        assert!(validate_schema(&schema, &value).is_empty());
    }

    #[test]
    fn schema_validation_flags_missing_required_field() {
        let schema = NormalizedSchema::new("transfer").with_field("/amount", FieldType::Number, true);
        let value = json!({});
        let violations = validate_schema(&schema, &value);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].pointer, "/amount");
    }

    #[test]
    fn schema_validation_flags_wrong_type() {
        let schema = NormalizedSchema::new("transfer").with_field("/amount", FieldType::Number, true);
        let value = json!({"amount": "not-a-number"});
        let violations = validate_schema(&schema, &value);
        assert_eq!(violations.len(), 1);
    }

    #[test]
    fn normalize_with_validation_applies_when_schema_passes() {
        let rules = vec![rule("/amount", "divide_by_decimals", json!({"decimals": 2}))];
        let schema = NormalizedSchema::new("s").with_field("/amount", FieldType::Number, true);
        let mut metrics = NormalizationMetrics::default();
        let data = json!({"amount": 100});

        let outcome = normalize_with_validation(&rules, &data, &schema, &mut metrics);
        match outcome {
            NormalizationOutcome::Applied(v) => assert_eq!(v["amount"], 1.0),
            other => panic!("expected Applied, got {other:?}"),
        }
        assert_eq!(metrics.succeeded, 1);
        assert_eq!(metrics.attempted, 1);
    }

    #[test]
    fn normalize_with_validation_rolls_back_on_schema_failure() {
        // base64_decode turns the numeric-looking string into text, which
        // violates a schema expecting a Number at that pointer.
        let rules = vec![rule("/amount", "base64_decode", json!({}))];
        let schema = NormalizedSchema::new("s").with_field("/amount", FieldType::Number, true);
        let mut metrics = NormalizationMetrics::default();
        let data = json!({"amount": "aGVsbG8="});

        let outcome = normalize_with_validation(&rules, &data, &schema, &mut metrics);
        match outcome {
            NormalizationOutcome::RolledBack { original, violations } => {
                assert_eq!(original, data);
                assert!(!violations.is_empty());
            }
            other => panic!("expected RolledBack, got {other:?}"),
        }
        assert_eq!(metrics.rolled_back, 1);
        assert_eq!(metrics.succeeded, 0);
    }

    #[test]
    fn normalize_with_validation_skips_empty_rules() {
        let schema = NormalizedSchema::new("s");
        let mut metrics = NormalizationMetrics::default();
        let outcome = normalize_with_validation(&[], &json!({"a": 1}), &schema, &mut metrics);
        assert!(matches!(outcome, NormalizationOutcome::Skipped));
        assert_eq!(metrics.skipped, 1);
    }

    #[test]
    fn metrics_success_rate_computed_correctly() {
        let mut metrics = NormalizationMetrics::default();
        metrics.attempted = 4;
        metrics.succeeded = 3;
        assert_eq!(metrics.success_rate(), 0.75);
    }
}
