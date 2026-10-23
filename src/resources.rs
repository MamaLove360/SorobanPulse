//! Per-contract resource and fee statistics (#1062).
//!
//! Disabled by default. Set `RESOURCE_STATS_ENABLED=true` to fetch `getTransaction` for
//! transactions that emitted events (one extra RPC call per new transaction).

use serde_json::{json, Value};
use sqlx::PgPool;
use std::time::Duration;

#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub struct TxResources {
    pub resource_fee: i64,
    pub instructions: i64,
    pub read_bytes: i64,
    pub write_bytes: i64,
}

/// Per-event resource usage written to the `events` table (issue #1257).
///
/// Prefers what the transaction meta reports as actually charged / consumed,
/// falling back to the declared values in the envelope. `None` when neither
/// is present.
#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub struct EventResources {
    pub resource_fee: Option<i64>,
    pub cpu_insns: Option<i64>,
    pub mem_bytes: Option<i64>,
}

/// Depth-first search for the first numeric-ish value under `key`.
fn find_num(v: &Value, key: &str) -> Option<i64> {
    match v {
        Value::Object(m) => {
            if let Some(x) = m.get(key) {
                if let Some(n) = x.as_i64() {
                    return Some(n);
                }
                if let Some(n) = x.as_str().and_then(|s| s.parse().ok()) {
                    return Some(n);
                }
            }
            m.values().find_map(|c| find_num(c, key))
        }
        Value::Array(a) => a.iter().find_map(|c| find_num(c, key)),
        _ => None,
    }
}

/// Extract resource usage from a `getTransaction` result requested with `xdrFormat: "json"`.
pub fn parse_resources(result: &Value) -> TxResources {
    let env = result.get("envelopeJson").unwrap_or(&Value::Null);
    TxResources {
        resource_fee: find_num(env, "resource_fee").unwrap_or(0),
        instructions: find_num(env, "instructions").unwrap_or(0),
        read_bytes: find_num(env, "disk_read_bytes").or_else(|| find_num(env, "read_bytes")).unwrap_or(0),
        write_bytes: find_num(env, "write_bytes").unwrap_or(0),
    }
}

/// Value of a `core_metrics` diagnostic event named `metric` (e.g. `cpu_insn`,
/// `mem_byte`), searched anywhere under `v`. These events are only present
/// when the RPC's core has Soroban diagnostic events enabled.
fn find_core_metric(v: &Value, metric: &str) -> Option<i64> {
    match v {
        Value::Object(m) => {
            if let Some(Value::Array(topics)) = m.get("topics") {
                let is_metric = topics.len() == 2
                    && topics[0].get("symbol").and_then(Value::as_str) == Some("core_metrics")
                    && topics[1].get("symbol").and_then(Value::as_str) == Some(metric);
                if is_metric {
                    let data = m.get("data")?;
                    let n = data.get("u64").or_else(|| data.get("u32")).unwrap_or(data);
                    return n.as_i64().or_else(|| n.as_str().and_then(|s| s.parse().ok()));
                }
            }
            m.values().find_map(|c| find_core_metric(c, metric))
        }
        Value::Array(a) => a.iter().find_map(|c| find_core_metric(c, metric)),
        _ => None,
    }
}

/// Extract per-event resource usage from a `getTransaction` result requested
/// with `xdrFormat: "json"` (issue #1257).
pub fn parse_event_resources(result: &Value) -> EventResources {
    let env = result.get("envelopeJson").unwrap_or(&Value::Null);

    let charged_fee = match (
        find_num(result, "total_non_refundable_resource_fee_charged"),
        find_num(result, "total_refundable_resource_fee_charged"),
    ) {
        (None, None) => None,
        (a, b) => Some(a.unwrap_or(0) + b.unwrap_or(0)),
    };

    EventResources {
        resource_fee: charged_fee.or_else(|| find_num(env, "resource_fee")),
        cpu_insns: find_core_metric(result, "cpu_insn").or_else(|| find_num(env, "instructions")),
        mem_bytes: find_core_metric(result, "mem_byte"),
    }
}

pub async fn fetch_transaction(
    client: &reqwest::Client,
    url: &str,
    tx_hash: &str,
) -> Result<(TxResources, EventResources), String> {
    let body = json!({"jsonrpc":"2.0","id":1,"method":"getTransaction",
        "params":{"hash": tx_hash, "xdrFormat": "json"}});
    let v: Value = client.post(url).json(&body).send().await.map_err(|e| e.to_string())?
        .json().await.map_err(|e| e.to_string())?;
    v.get("result")
        .map(|r| (parse_resources(r), parse_event_resources(r)))
        .ok_or_else(|| "getTransaction: no result".to_string())
}

/// Fetch resources for up to `batch` event transactions that have none stored.
pub async fn sync_batch(pool: &PgPool, client: &reqwest::Client, url: &str, batch: i64) -> Result<usize, String> {
    let rows: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT DISTINCT ON (e.tx_hash) e.tx_hash, e.contract_id, e.ledger
         FROM events e LEFT JOIN tx_resources r ON r.tx_hash = e.tx_hash
         WHERE r.tx_hash IS NULL ORDER BY e.tx_hash, e.ledger DESC LIMIT $1",
    )
    .bind(batch)
    .fetch_all(pool)
    .await
    .map_err(|e| e.to_string())?;
    let mut n = 0;
    for (hash, contract, ledger) in rows {
        let (r, ev) = fetch_transaction(client, url, &hash).await?;
        sqlx::query(
            "INSERT INTO tx_resources (tx_hash, contract_id, ledger, resource_fee, instructions, read_bytes, write_bytes)
             VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (tx_hash) DO NOTHING",
        )
        .bind(&hash).bind(&contract).bind(ledger)
        .bind(r.resource_fee).bind(r.instructions).bind(r.read_bytes).bind(r.write_bytes)
        .execute(pool).await.map_err(|e| e.to_string())?;
        // Issue #1257: per-event columns aggregated by mv_contract_resource_stats.
        sqlx::query(
            "UPDATE events SET resource_fee = $2, cpu_insns = $3, mem_bytes = $4
             WHERE tx_hash = $1 AND resource_fee IS NULL AND cpu_insns IS NULL AND mem_bytes IS NULL",
        )
        .bind(&hash).bind(ev.resource_fee).bind(ev.cpu_insns).bind(ev.mem_bytes)
        .execute(pool).await.map_err(|e| e.to_string())?;
        n += 1;
    }
    if n > 0 {
        let _ = sqlx::query("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_contract_resources_daily").execute(pool).await;
    }
    Ok(n)
}

pub fn enabled() -> bool {
    std::env::var("RESOURCE_STATS_ENABLED").map(|v| v == "true" || v == "1").unwrap_or(false)
}

pub fn spawn_sync(pool: PgPool, url: String) {
    if !enabled() {
        return;
    }
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let mut tick = tokio::time::interval(Duration::from_secs(30));
        loop {
            tick.tick().await;
            if let Err(e) = sync_batch(&pool, &client, &url, 50).await {
                tracing::warn!(error = %e, "resource stats sync failed");
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_nested_resources() {
        let v = json!({"envelopeJson":{"tx":{"tx":{"ext":{"v1":{"resource_fee":"1200",
            "resources":{"instructions":5000,"read_bytes":10,"write_bytes":20}}}}}}});
        assert_eq!(parse_resources(&v), TxResources{resource_fee:1200,instructions:5000,read_bytes:10,write_bytes:20});
    }

    fn core_metric(name: &str, value: Value) -> Value {
        json!({"in_successful_contract_call": true, "event": {"ext": "v0", "contract_id": null,
            "type_": "diagnostic", "body": {"v0": {
                "topics": [{"symbol": "core_metrics"}, {"symbol": name}],
                "data": {"u64": value}}}}})
    }

    #[test]
    fn event_resources_prefer_charged_fee_and_core_metrics() {
        let v = json!({
            "envelopeJson": {"tx": {"tx": {"ext": {"v1": {"resource_fee": "9999",
                "resources": {"instructions": 5000000}}}}}},
            "resultMetaJson": {"v4": {"soroban_meta": {"ext": {"v1": {
                "total_non_refundable_resource_fee_charged": "700",
                "total_refundable_resource_fee_charged": "300",
                "rent_fee_charged": "100"}}}}},
            "events": {"diagnosticEventsJson": [
                core_metric("read_entry", json!(4)),
                core_metric("cpu_insn", json!("1234567")),
                core_metric("mem_byte", json!(890123)),
            ]}
        });
        assert_eq!(
            parse_event_resources(&v),
            EventResources { resource_fee: Some(1000), cpu_insns: Some(1234567), mem_bytes: Some(890123) }
        );
    }

    #[test]
    fn event_resources_fall_back_to_declared_values() {
        let v = json!({"envelopeJson": {"tx": {"tx": {"ext": {"v1": {"resource_fee": "1200",
            "resources": {"instructions": 5000}}}}}}});
        assert_eq!(
            parse_event_resources(&v),
            EventResources { resource_fee: Some(1200), cpu_insns: Some(5000), mem_bytes: None }
        );
    }

    #[test]
    fn event_resources_none_without_soroban_data() {
        let v = json!({"envelopeJson": {"tx": {"tx": {"fee": 100}}}});
        assert_eq!(parse_event_resources(&v), EventResources::default());
    }

    #[test]
    fn core_metric_ignores_other_events() {
        let v = json!([{"topics": [{"symbol": "transfer"}, {"symbol": "cpu_insn"}], "data": {"u64": 1}}]);
        assert_eq!(find_core_metric(&v, "cpu_insn"), None);
    }
}
