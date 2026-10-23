//! `format=native|json|xdr` rendering for ScVal event data (#1064).
//!
//! Event `event_data` (and each `topic` entry) is stored as stellar-xdr's
//! serde JSON representation. Consumers can ask for three renderings:
//!
//! - `json` (default, backward compatible): the raw stellar-xdr serde JSON
//!   as stored.
//! - `native`: a friendly JSON shape with `i128`/`u128`/`i64`/`u64` as
//!   strings and addresses as strkeys, easy to consume without knowing the
//!   XDR type tags.
//! - `xdr`: the base64-encoded XDR bytes for the value.

use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use stellar_xdr::curr::{Limits, ReadXdr, ScVal, WriteXdr};

/// Maximum JSON nesting depth accepted before an ScVal is decoded.
///
/// Decoding, native rendering and XDR encoding all recurse once per level,
/// so deeply nested `vec`/`map` input could overflow the stack (issue #1226).
pub const MAX_SCVAL_JSON_DEPTH: usize = 64;

/// Returns `true` when `value` nests deeper than [`MAX_SCVAL_JSON_DEPTH`].
/// Iterative, so it is safe to call on arbitrarily deep input.
pub fn exceeds_max_depth(value: &Value) -> bool {
    let mut stack = vec![(value, 1usize)];
    while let Some((v, depth)) = stack.pop() {
        if depth > MAX_SCVAL_JSON_DEPTH {
            return true;
        }
        match v {
            Value::Array(items) => stack.extend(items.iter().map(|i| (i, depth + 1))),
            Value::Object(map) => stack.extend(map.values().map(|i| (i, depth + 1))),
            _ => {}
        }
    }
    false
}

/// Decode a stellar-xdr serde JSON value as an `ScVal`, refusing input that
/// nests deeper than [`MAX_SCVAL_JSON_DEPTH`].
pub fn decode_scval(value: &Value) -> Option<ScVal> {
    if exceeds_max_depth(value) {
        return None;
    }
    serde_json::from_value::<ScVal>(value.clone()).ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ScValFormat {
    /// stellar-xdr's own serde JSON representation (current default).
    #[default]
    Json,
    /// Friendly JSON: i128/u128/i64/u64 as strings, addresses as strkeys.
    Native,
    /// Base64-encoded raw XDR bytes.
    Xdr,
}

impl std::str::FromStr for ScValFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "json" => Ok(ScValFormat::Json),
            "native" => Ok(ScValFormat::Native),
            "xdr" => Ok(ScValFormat::Xdr),
            other => Err(format!(
                "invalid format '{other}': expected one of native, json, xdr"
            )),
        }
    }
}

/// Render a stored ScVal JSON value (stellar-xdr serde JSON) in the
/// requested format. Falls back to returning the value unchanged if it does
/// not parse as an `ScVal` (e.g. already-decoded/anonymized event data).
pub fn render(value: &Value, format: ScValFormat) -> Value {
    match format {
        ScValFormat::Json => value.clone(),
        ScValFormat::Native => match decode_scval(value) {
            Some(scval) => to_native(&scval),
            None => value.clone(),
        },
        ScValFormat::Xdr => match decode_scval(value) {
            Some(scval) => match scval.to_xdr_base64(Limits::none()) {
                Ok(b64) => json!(b64),
                Err(_) => value.clone(),
            },
            None => value.clone(),
        },
    }
}

/// Render `event_data.value` and every `event_data.topic` entry using the
/// requested format, leaving other fields untouched.
pub fn render_event_data(event_data: &Value, format: ScValFormat) -> Value {
    if format == ScValFormat::Json {
        return event_data.clone();
    }
    let mut out = event_data.clone();
    if let Some(obj) = out.as_object_mut() {
        if let Some(v) = obj.get("value") {
            obj.insert("value".to_string(), render(v, format));
        }
        if let Some(Value::Array(topics)) = obj.get("topic").cloned() {
            let rendered: Vec<Value> = topics.iter().map(|t| render(t, format)).collect();
            obj.insert("topic".to_string(), Value::Array(rendered));
        }
    }
    out
}

fn to_native(value: &ScVal) -> Value {
    match value {
        ScVal::Bool(v) => json!(*v),
        ScVal::Void => Value::Null,
        ScVal::U32(v) => json!(*v),
        ScVal::I32(v) => json!(*v),
        ScVal::U64(v) => json!(v.to_string()),
        ScVal::I64(v) => json!(v.to_string()),
        ScVal::Timepoint(t) => json!(t.0.to_string()),
        ScVal::Duration(d) => json!(d.0.to_string()),
        ScVal::U128(v) => json!(((v.hi as u128) << 64 | v.lo as u128).to_string()),
        ScVal::I128(v) => {
            let combined = ((v.hi as i128) << 64) | (v.lo as i128);
            json!(combined.to_string())
        }
        ScVal::U256(v) => json!(format!("{v:?}")),
        ScVal::I256(v) => json!(format!("{v:?}")),
        ScVal::Bytes(b) => json!(base64::engine::general_purpose::STANDARD.encode(b.as_slice())),
        ScVal::String(s) => json!(s.to_string()),
        ScVal::Symbol(s) => json!(s.to_string()),
        ScVal::Vec(Some(vals)) => Value::Array(vals.0.iter().map(to_native).collect()),
        ScVal::Vec(None) => Value::Array(Vec::new()),
        ScVal::Map(Some(entries)) => {
            let mut obj = serde_json::Map::new();
            for entry in entries.0.iter() {
                let key = match to_native(&entry.key) {
                    Value::String(s) => s,
                    other => other.to_string(),
                };
                obj.insert(key, to_native(&entry.val));
            }
            Value::Object(obj)
        }
        ScVal::Map(None) => Value::Object(serde_json::Map::new()),
        ScVal::Address(addr) => json!(addr.to_string()),
        other => serde_json::to_value(other).unwrap_or(Value::Null),
    }
}

/// Parse a `format` query parameter, defaulting to `json` when absent so
/// existing callers keep their current behaviour.
pub fn parse_format(raw: Option<&str>) -> Result<ScValFormat, String> {
    match raw {
        None => Ok(ScValFormat::Json),
        Some(s) => s.parse(),
    }
}

/// Decode a stored topic/value entry into an `ScVal`. Events indexed from
/// RPC `getEvents` keep the base64 XDR strings the RPC returns; other paths
/// store stellar-xdr's serde JSON. Returns `None` for anything else.
pub fn parse_stored(value: &Value) -> Option<ScVal> {
    match value {
        Value::String(s) => ScVal::from_xdr_base64(s, Limits::none()).ok(),
        Value::Null => None,
        other => serde_json::from_value(other.clone()).ok(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn json_format_is_identity() {
        let v = json!({"i128": {"hi": 0, "lo": 42}});
        assert_eq!(render(&v, ScValFormat::Json), v);
    }

    #[test]
    fn native_renders_bool_and_void() {
        assert_eq!(render(&serde_json::to_value(ScVal::Bool(true)).unwrap(), ScValFormat::Native), json!(true));
        assert_eq!(render(&serde_json::to_value(ScVal::Void).unwrap(), ScValFormat::Native), Value::Null);
    }

    #[test]
    fn parse_format_rejects_unknown() {
        assert!(parse_format(Some("yaml")).is_err());
    }

    #[test]
    fn parse_format_defaults_to_json() {
        assert_eq!(parse_format(None).unwrap(), ScValFormat::Json);
    }

    /// Fuzz regression (#1236): non-ScVal JSON, extreme 128-bit parts and
    /// nested containers must render in every format without panicking.
    #[test]
    fn render_edge_cases_never_panic() {
        let cases = [
            json!(null),
            json!(42),
            json!("not an scval"),
            json!({"unknown_tag": 1}),
            serde_json::to_value(ScVal::I128(stellar_xdr::curr::Int128Parts { hi: i64::MIN, lo: u64::MAX })).unwrap(),
            serde_json::to_value(ScVal::U128(stellar_xdr::curr::UInt128Parts { hi: u64::MAX, lo: u64::MAX })).unwrap(),
            serde_json::to_value(ScVal::Vec(None)).unwrap(),
            serde_json::to_value(ScVal::Map(None)).unwrap(),
        ];
        for v in &cases {
            for format in [ScValFormat::Json, ScValFormat::Native, ScValFormat::Xdr] {
                let _ = render(v, format);
            }
        }
    }

    #[test]
    fn native_renders_extreme_i128_and_u128() {
        let min = ScVal::I128(stellar_xdr::curr::Int128Parts { hi: i64::MIN, lo: 0 });
        let max = ScVal::U128(stellar_xdr::curr::UInt128Parts { hi: u64::MAX, lo: u64::MAX });
        assert_eq!(render(&serde_json::to_value(min).unwrap(), ScValFormat::Native), json!(i128::MIN.to_string()));
        assert_eq!(render(&serde_json::to_value(max).unwrap(), ScValFormat::Native), json!(u128::MAX.to_string()));
    }

    proptest! {
        /// Issue #1064 acceptance criterion: xdr -> json -> xdr round-trips
        /// losslessly for simple scalar ScVals.
        #[test]
        fn xdr_json_xdr_round_trips(n in any::<i32>()) {
            let scval = ScVal::I32(n);
            let xdr_b64 = scval.to_xdr_base64(Limits::none()).unwrap();
            let json_val = serde_json::to_value(&scval).unwrap();
            let scval_back: ScVal = serde_json::from_value(json_val).unwrap();
            let xdr_b64_back = scval_back.to_xdr_base64(Limits::none()).unwrap();
            prop_assert_eq!(xdr_b64, xdr_b64_back);
        }
    }
}
