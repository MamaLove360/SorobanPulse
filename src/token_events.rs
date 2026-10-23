//! SEP-41 token event decoding (issue #1055).
//!
//! Recognises `transfer`, `mint`, `burn`, `clawback`, `approve`, `set_admin`
//! and `set_authorized` events by topic 0 symbol plus arity. Amounts are kept
//! as decimal strings so i128 values never lose precision. Both the classic
//! layout and the CAP-67 layout (SAC `transfer` carrying a trailing asset
//! topic, muxed-id map values) are handled. Unknown events decode to `None`
//! so callers pass them through unchanged.

use axum::extract::{Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::Row;

use crate::error::AppError;
use crate::routes::AppState;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TokenEvent {
    pub kind: String,
    pub from: Option<String>,
    pub to: Option<String>,
    pub spender: Option<String>,
    /// Decimal string, exact i128.
    pub amount: Option<String>,
}

pub fn topic_str(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Object(m) => m
            .get("symbol")
            .or_else(|| m.get("address"))
            .or_else(|| m.get("string"))
            .and_then(|x| x.as_str().map(str::to_owned)),
        _ => None,
    }
}

/// Extract an exact i128 as a decimal string from the JSON shapes RPC produces.
pub fn amount_str(v: &Value) -> Option<String> {
    match v {
        Value::String(s) if s.parse::<i128>().is_ok() => Some(s.clone()),
        Value::Number(n) if n.is_i64() || n.is_u64() => Some(n.to_string()),
        Value::Object(m) => {
            if let Some(x) = m.get("i128").or_else(|| m.get("amount")) {
                return amount_str(x);
            }
            // {hi, lo} parts
            let hi = m.get("hi")?.as_str().map(str::to_owned).or_else(|| m.get("hi")?.as_i64().map(|x| x.to_string()))?;
            let lo = m.get("lo")?.as_str().map(str::to_owned).or_else(|| m.get("lo")?.as_u64().map(|x| x.to_string()))?;
            let v = (hi.parse::<i128>().ok()? << 64) | lo.parse::<u64>().ok()? as i128;
            Some(v.to_string())
        }
        _ => None,
    }
}

/// Decode a SEP-41 event from its topic array and value. Returns `None` for
/// anything that is not a recognised token event.
pub fn decode(topics: &[Value], value: &Value) -> Option<TokenEvent> {
    let name = topic_str(topics.first()?)?;
    let addr = |i: usize| topics.get(i).and_then(topic_str);
    // Arity counts address topics; a trailing asset-name topic (SAC / CAP-67) is allowed.
    let n = topics.len();
    let mut ev = TokenEvent { kind: name.clone(), from: None, to: None, spender: None, amount: None };
    // CAP-67 muxed transfers carry {amount, to_muxed_id}; amount_str handles the map.
    ev.amount = amount_str(value);
    match name.as_str() {
        "transfer" if n >= 3 => { ev.from = addr(1); ev.to = addr(2); }
        "mint" if n >= 3 => { ev.to = addr(2).or_else(|| addr(1)); ev.from = if n >= 3 { addr(1) } else { None }; }
        "burn" if n >= 2 => { ev.from = addr(1); }
        "clawback" if n >= 3 => { ev.from = addr(2).or_else(|| addr(1)); }
        "approve" if n >= 3 => { ev.from = addr(1); ev.spender = addr(2); }
        "set_admin" if n >= 2 => { ev.from = addr(1); ev.to = value.as_str().map(str::to_owned); ev.amount = None; }
        "set_authorized" if n >= 2 => { ev.to = addr(1); ev.amount = None; }
        _ => return None,
    }
    if ev.amount.is_none() && !matches!(name.as_str(), "set_admin" | "set_authorized") {
        return None;
    }
    Some(ev)
}

/// Decode from a stored `event_data` JSON ({"topic": [...], "value": ...}).
pub fn decode_event_data(event_data: &Value) -> Option<TokenEvent> {
    let topics = event_data.get("topic")?.as_array()?;
    decode(topics, event_data.get("value").unwrap_or(&Value::Null))
}

/// Populate `token_transfers` for events not yet decoded. Returns rows inserted.
pub async fn backfill(pool: &sqlx::PgPool, batch: i64) -> Result<u64, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT e.id, e.contract_id, e.ledger, e.tx_hash, e.event_data FROM events e
         WHERE e.event_data->'topic'->>0 IN ('transfer','mint','burn','clawback','approve','set_admin','set_authorized')
           AND NOT EXISTS (SELECT 1 FROM token_transfers t WHERE t.event_id = e.id)
         ORDER BY e.ledger LIMIT $1",
    )
    .bind(batch)
    .fetch_all(pool)
    .await?;
    let mut n = 0;
    for r in rows {
        let data: Value = r.try_get("event_data")?;
        let Some(ev) = decode_event_data(&data) else { continue };
        n += sqlx::query(
            "INSERT INTO token_transfers (event_id, contract_id, kind, from_address, to_address, spender, amount, ledger, tx_hash)
             VALUES ($1,$2,$3,$4,$5,$6,$7::numeric,$8,$9) ON CONFLICT DO NOTHING",
        )
        .bind(r.try_get::<uuid::Uuid, _>("id")?)
        .bind(r.try_get::<String, _>("contract_id")?)
        .bind(&ev.kind).bind(&ev.from).bind(&ev.to).bind(&ev.spender).bind(&ev.amount)
        .bind(r.try_get::<i64, _>("ledger")?)
        .bind(r.try_get::<String, _>("tx_hash")?)
        .execute(pool).await?.rows_affected();
    }
    Ok(n)
}

#[derive(Debug, Deserialize)]
pub struct TransfersQuery {
    pub from: Option<String>,
    pub to: Option<String>,
    pub min_amount: Option<String>,
    /// Cursor of the form `<ledger>:<event_id>` returned as `next_cursor`.
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

/// `GET /v1/tokens/{contract_id}/transfers`
pub async fn get_token_transfers(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Query(q): Query<TransfersQuery>,
) -> Result<Json<Value>, AppError> {
    crate::handlers::validate_contract_id(&contract_id)?;
    if let Some(m) = &q.min_amount {
        if m.parse::<i128>().is_err() {
            return Err(AppError::Validation("invalid min_amount".into()));
        }
    }
    let (cl, cid) = match &q.cursor {
        Some(c) => {
            let (l, i) = c.split_once(':').ok_or_else(|| AppError::Validation("invalid cursor".into()))?;
            (
                Some(l.parse::<i64>().map_err(|_| AppError::Validation("invalid cursor".into()))?),
                Some(i.parse::<uuid::Uuid>().map_err(|_| AppError::Validation("invalid cursor".into()))?),
            )
        }
        None => (None, None),
    };
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    let rows = sqlx::query(
        "SELECT event_id, kind, from_address, to_address, spender, amount::text AS amount, ledger, tx_hash
         FROM token_transfers
         WHERE contract_id = $1
           AND ($2::text IS NULL OR from_address = $2)
           AND ($3::text IS NULL OR to_address = $3)
           AND ($4::text IS NULL OR amount >= $4::numeric)
           AND ($5::bigint IS NULL OR (ledger, event_id) < ($5, $6))
         ORDER BY ledger DESC, event_id DESC LIMIT $7",
    )
    .bind(&contract_id).bind(&q.from).bind(&q.to).bind(&q.min_amount).bind(cl).bind(cid).bind(limit + 1)
    .fetch_all(&state.read_pool)
    .await?;
    let has_more = rows.len() as i64 > limit;
    let mut items = Vec::new();
    let mut next = None;
    for r in rows.iter().take(limit as usize) {
        let l: i64 = r.try_get("ledger")?;
        let id: uuid::Uuid = r.try_get("event_id")?;
        next = Some(format!("{l}:{id}"));
        items.push(json!({
            "event_id": id, "kind": r.try_get::<String,_>("kind")?,
            "from": r.try_get::<Option<String>,_>("from_address")?,
            "to": r.try_get::<Option<String>,_>("to_address")?,
            "spender": r.try_get::<Option<String>,_>("spender")?,
            "amount": r.try_get::<Option<String>,_>("amount")?,
            "ledger": l, "tx_hash": r.try_get::<String,_>("tx_hash")?,
        }));
    }
    Ok(Json(json!({ "contract_id": contract_id, "transfers": items, "next_cursor": if has_more { next } else { None } })))
}

/// `POST /v1/admin/token-transfers/backfill`
pub async fn backfill_token_transfers(State(state): State<AppState>) -> Result<Json<Value>, AppError> {
    let n = backfill(&state.pool, 5000).await?;
    Ok(Json(json!({ "inserted": n })))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn t(v: &[&str]) -> Vec<Value> { v.iter().map(|s| json!(s)).collect() }

    #[test]
    fn transfer_exact_i128() {
        let e = decode(&t(&["transfer", "GA", "GB"]), &json!("170141183460469231731687303715884105727")).unwrap();
        assert_eq!(e.amount.unwrap(), i128::MAX.to_string());
        assert_eq!(e.from.unwrap(), "GA");
    }
    #[test]
    fn sac_trailing_asset_topic_and_muxed_map() {
        let e = decode(&t(&["transfer", "GA", "GB", "native"]), &json!({"amount": "5", "to_muxed_id": 1})).unwrap();
        assert_eq!(e.amount.unwrap(), "5");
    }
    #[test]
    fn hi_lo() {
        assert_eq!(amount_str(&json!({"hi": "0", "lo": "18446744073709551615"})).unwrap(), u64::MAX.to_string());
    }
    #[test]
    fn each_kind() {
        assert!(decode(&t(&["mint", "GA", "GB"]), &json!("1")).is_some());
        assert!(decode(&t(&["burn", "GA"]), &json!("1")).is_some());
        assert!(decode(&t(&["clawback", "GA", "GB"]), &json!("1")).is_some());
        assert_eq!(decode(&t(&["approve", "GA", "GB"]), &json!("1")).unwrap().spender.unwrap(), "GB");
        assert!(decode(&t(&["set_admin", "GA"]), &json!("GB")).is_some());
        assert!(decode(&t(&["set_authorized", "GA"]), &json!(true)).is_some());
    }
    #[test]
    fn unknown_passes_through() {
        assert!(decode(&t(&["swap", "GA"]), &json!("1")).is_none());
        assert!(decode(&t(&["transfer", "GA"]), &json!("1")).is_none());
    }
}
