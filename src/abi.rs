use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::Arc;
use stellar_xdr::curr::ScVal;

/// Default ABI cache TTL in seconds (24 hours).
pub const DEFAULT_ABI_CACHE_TTL_SECS: u64 = 86_400;
/// Maximum number of entries kept in the in-process LRU cache.
pub const ABI_CACHE_MAX_ENTRIES: u64 = 10_000;

pub type AbiCache = moka::future::Cache<String, Value>;

/// Build an in-process ABI cache capped at `ABI_CACHE_MAX_ENTRIES`.
pub fn build_abi_cache() -> AbiCache {
    moka::future::Cache::builder()
        .max_capacity(ABI_CACHE_MAX_ENTRIES)
        .time_to_live(std::time::Duration::from_secs(DEFAULT_ABI_CACHE_TTL_SECS))
        .eviction_listener(|_key, _val, _cause| {
            crate::metrics::record_abi_cache_eviction();
        })
        .build()
}

/// Validate that an ABI value is a JSON array whose entries each have a "name" field.
pub fn validate_abi(abi: &Value) -> Result<(), String> {
    let entries = abi
        .as_array()
        .ok_or_else(|| "ABI must be a JSON array".to_string())?;
    for (i, entry) in entries.iter().enumerate() {
        if entry.get("name").and_then(Value::as_str).is_none() {
            return Err(format!("ABI entry {i} is missing a \"name\" field"));
        }
    }
    Ok(())
}

/// Fetch a contract ABI, using the in-process cache as the first layer and
/// falling back to the database.  Cache hits / misses are recorded as metrics.
pub async fn fetch_contract_abi(
    pool: &PgPool,
    cache: &AbiCache,
    contract_id: &str,
) -> Option<Value> {
    if let Some(abi) = cache.get(contract_id).await {
        crate::metrics::record_abi_cache_hit(contract_id);
        return Some(abi);
    }
    crate::metrics::record_abi_cache_miss(contract_id);

    let row = sqlx::query!(
        "SELECT abi FROM contract_abis
         WHERE contract_id = $1
           AND (expires_at IS NULL OR expires_at > NOW())",
        contract_id,
    )
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()?;

    let abi = row.abi;
    cache.insert(contract_id.to_string(), abi.clone()).await;
    Some(abi)
}

/// Invalidate the in-process cache entry for `contract_id`.
/// Called after an ABI is updated so the next read fetches fresh data.
pub async fn invalidate_abi_cache(cache: &AbiCache, contract_id: &str) {
    cache.invalidate(contract_id).await;
}

pub fn decode_event_data(abi: &Value, event_data: &Value) -> Option<Value> {
    let event_name = event_name_from_topic(event_data.get("topic")?)?;
    let def = abi
        .as_array()?
        .iter()
        .find(|e| e.get("name").and_then(Value::as_str) == Some(event_name.as_str()))?;
    let inputs = def.get("inputs")?.as_array()?;
    let values = decode_values(event_data.get("value")?);

    let mut decoded = serde_json::Map::new();
    decoded.insert("event".to_string(), Value::String(event_name.to_string()));
    for (i, input) in inputs.iter().enumerate() {
        let name = input
            .get("name")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| format!("field_{i}"));
        let value = values
            .get(&name)
            .or_else(|| values.get(&i.to_string()))
            .cloned()
            .unwrap_or(Value::Null);
        decoded.insert(name, value);
    }

    Some(Value::Object(decoded))
}

/// Decode using the cached ABI for `contract_id`.
pub async fn decode_event_with_cached_abi(
    pool: &PgPool,
    cache: &AbiCache,
    contract_id: &str,
    event_data: &Value,
) -> Option<Value> {
    let abi = fetch_contract_abi(pool, cache, contract_id).await?;
    decode_event_data(&abi, event_data)
}

/// Legacy version that hits the DB directly (kept for backwards compatibility).
pub async fn decode_event_with_registered_abi(
    pool: &PgPool,
    contract_id: &str,
    event_data: &Value,
) -> Option<Value> {
    let abi: Value = sqlx::query_scalar("SELECT abi FROM contract_abis WHERE contract_id = $1")
        .bind(contract_id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()?;
    decode_event_data(&abi, event_data)
}

pub async fn decode_existing_events(pool: PgPool, contract_id: String, abi: Value) {
    let rows = match sqlx::query("SELECT id, event_data FROM events WHERE contract_id = $1")
        .bind(&contract_id)
        .fetch_all(&pool)
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!(contract_id = %contract_id, error = %e, "failed to load events for ABI backfill");
            return;
        }
    };

    for row in rows {
        use sqlx::Row;
        let id: uuid::Uuid = match row.try_get("id") {
            Ok(id) => id,
            Err(_) => continue,
        };
        let event_data: Value = match row.try_get("event_data") {
            Ok(event_data) => event_data,
            Err(_) => continue,
        };
        let Some(decoded) = decode_event_data(&abi, &event_data) else {
            continue;
        };
        if let Err(e) = sqlx::query("UPDATE events SET event_data_decoded = $1 WHERE id = $2")
            .bind(decoded)
            .bind(id)
            .execute(&pool)
            .await
        {
            tracing::error!(event_id = %id, error = %e, "failed to backfill decoded event data");
        }
    }
}

fn event_name_from_topic(topic: &Value) -> Option<String> {
    let first = topic.as_array()?.first()?;
    // Live-indexed events keep the RPC's base64 XDR; decode to the name.
    match crate::scval_format::parse_stored(first) {
        Some(ScVal::Symbol(s)) => return Some(s.0.to_string()),
        Some(ScVal::String(s)) => return Some(s.0.to_string()),
        _ => {}
    }
    first
        .as_str()
        .or_else(|| first.get("symbol").and_then(Value::as_str))
        .or_else(|| first.get("string").and_then(Value::as_str))
        .map(str::to_owned)
}

fn decode_values(value: &Value) -> serde_json::Map<String, Value> {
    // Stored ScVals (base64 XDR or stellar-xdr serde JSON): vecs are keyed
    // by position, maps by key, and a single value is position 0.
    match crate::scval_format::parse_stored(value) {
        Some(ScVal::Vec(Some(vec))) => {
            return vec
                .0
                .to_vec()
                .into_iter()
                .enumerate()
                .map(|(i, val)| (i.to_string(), scval_to_json(val)))
                .collect();
        }
        Some(ScVal::Map(Some(map))) => {
            return map
                .0
                .to_vec()
                .into_iter()
                .map(|e| {
                    let key = match scval_to_json(e.key) {
                        Value::String(s) => s,
                        other => other.to_string(),
                    };
                    (key, scval_to_json(e.val))
                })
                .collect();
        }
        Some(scalar) => {
            let mut map = serde_json::Map::new();
            map.insert("0".to_string(), scval_to_json(scalar));
            return map;
        }
        None => {}
    }
    if let Some(obj) = value.as_object() {
        return obj.clone();
    }

    let mut map = serde_json::Map::new();
    map.insert("0".to_string(), decode_scalar(value));
    map
}

fn decode_scalar(value: &Value) -> Value {
    serde_json::from_value::<ScVal>(value.clone())
        .map(scval_to_json)
        .unwrap_or_else(|_| value.clone())
}

fn scval_to_json(value: ScVal) -> Value {
    match value {
        ScVal::Bool(v) => json!(v),
        ScVal::Void => Value::Null,
        ScVal::U32(v) => json!(v),
        ScVal::I32(v) => json!(v),
        ScVal::U64(v) => json!(v.to_string()),
        ScVal::I64(v) => json!(v.to_string()),
        ScVal::Symbol(v) => json!(v.to_string()),
        ScVal::String(v) => json!(v.to_string()),
        ScVal::Vec(Some(values)) => {
            Value::Array(values.0.to_vec().into_iter().map(scval_to_json).collect())
        }
        ScVal::Vec(None) => Value::Array(Vec::new()),
        other => serde_json::to_value(other).unwrap_or(Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_named_fields_from_registered_abi() {
        let abi = json!([
            {"name": "transfer", "inputs": [
                {"name": "from", "type": "address"},
                {"name": "to", "type": "address"},
                {"name": "amount", "type": "i128"}
            ]}
        ]);
        let event_data = json!({
            "topic": ["transfer"],
            "value": {"from": "GABC", "to": "GDEF", "amount": "1000"}
        });

        let decoded = decode_event_data(&abi, &event_data).unwrap();
        assert_eq!(decoded["event"], "transfer");
        assert_eq!(decoded["amount"], "1000");
    }

    #[test]
    fn decodes_base64_xdr_event_data_as_stored_by_the_indexer() {
        use stellar_xdr::curr::{Limits, ScMap, ScMapEntry, ScSymbol, WriteXdr};
        let sym = |s: &str| ScVal::Symbol(ScSymbol::try_from(s).unwrap());
        let b64 = |v: ScVal| json!(v.to_xdr_base64(Limits::none()).unwrap());
        let abi = json!([{"name": "transfer", "inputs": [{"name": "amount", "type": "u32"}]}]);

        let single = json!({ "topic": [b64(sym("transfer"))], "value": b64(ScVal::U32(7)) });
        assert_eq!(decode_event_data(&abi, &single).unwrap()["amount"], 7);

        let map = ScVal::Map(Some(ScMap(
            vec![ScMapEntry { key: sym("amount"), val: ScVal::U32(9) }].try_into().unwrap(),
        )));
        let named = json!({ "topic": [b64(sym("transfer"))], "value": b64(map) });
        assert_eq!(decode_event_data(&abi, &named).unwrap()["amount"], 9);
    }

    #[test]
    fn validate_abi_rejects_non_array() {
        assert!(validate_abi(&json!({"name": "foo"})).is_err());
    }

    #[test]
    fn validate_abi_rejects_entry_without_name() {
        assert!(validate_abi(&json!([{"inputs": []}])).is_err());
    }

    #[test]
    fn validate_abi_accepts_well_formed() {
        let abi = json!([{"name": "transfer", "inputs": []}]);
        assert!(validate_abi(&abi).is_ok());
    }
}
