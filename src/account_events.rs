//! Address-centric event lookup (issue #1056).

use axum::extract::{Path, Query, State};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::Row;

use crate::error::AppError;
use crate::routes::AppState;

/// True for a syntactically plausible G/M/C strkey (56 chars, or 69 for M).
pub fn is_address(s: &str) -> bool {
    let ok_chars = s.bytes().all(|b| b.is_ascii_uppercase() || (b'2'..=b'7').contains(&b));
    match s.chars().next() {
        Some('G') | Some('C') => s.len() == 56 && ok_chars,
        Some('M') => s.len() == 69 && ok_chars,
        _ => false,
    }
}

/// Addresses found in the topics of an `event_data` value, with topic position.
pub fn extract_addresses(event_data: &Value) -> Vec<(String, i16)> {
    let mut out = Vec::new();
    if let Some(topics) = event_data.get("topic").and_then(Value::as_array) {
        for (i, t) in topics.iter().enumerate() {
            let s = t.as_str().or_else(|| t.get("address").and_then(Value::as_str));
            if let Some(s) = s {
                if is_address(s) {
                    out.push((s.to_owned(), i as i16));
                }
            }
        }
    }
    out
}

/// Fill `event_addresses` for events that have none yet. Returns rows inserted.
pub async fn backfill(pool: &sqlx::PgPool, batch: i64) -> Result<u64, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT e.id, e.ledger, e.event_data FROM events e
         WHERE NOT EXISTS (SELECT 1 FROM event_addresses a WHERE a.event_id = e.id)
           AND jsonb_typeof(e.event_data->'topic') = 'array'
         ORDER BY e.ledger DESC LIMIT $1",
    )
    .bind(batch)
    .fetch_all(pool)
    .await?;
    let mut n = 0;
    for r in rows {
        let data: Value = r.try_get("event_data")?;
        let id: uuid::Uuid = r.try_get("id")?;
        let ledger: i64 = r.try_get("ledger")?;
        for (addr, pos) in extract_addresses(&data) {
            n += sqlx::query("INSERT INTO event_addresses (event_id, address, position, ledger) VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING")
                .bind(id).bind(addr).bind(pos).bind(ledger)
                .execute(pool).await?.rows_affected();
        }
    }
    Ok(n)
}

#[derive(Debug, Deserialize)]
pub struct AccountEventsQuery {
    /// `any` (default), `topic1` or `topic2`.
    pub role: Option<String>,
    pub from_ledger: Option<i64>,
    pub to_ledger: Option<i64>,
    /// `<ledger>:<event_id>` from `next_cursor`.
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

/// `GET /v1/accounts/{address}/events` — events whose topics reference
/// `address`, across all contracts, newest first (issue #1250).
#[utoipa::path(
    get,
    path = "/v1/accounts/{address}/events",
    tag = "events",
    params(
        ("address" = String, Path, description = "Stellar G..., C... or M... strkey"),
        ("role" = Option<String>, Query, description = "any (default), topic1 or topic2"),
        ("from_ledger" = Option<i64>, Query, description = "Lowest ledger (inclusive)"),
        ("to_ledger" = Option<i64>, Query, description = "Highest ledger (inclusive)"),
        ("cursor" = Option<String>, Query, description = "`<ledger>:<event_id>` from a previous next_cursor"),
        ("limit" = Option<i64>, Query, description = "Page size, 1-200 (default 50)"),
    ),
    responses(
        (status = 200, description = "Events touching the address, newest first", body = serde_json::Value),
        (status = 400, description = "Invalid address, role or cursor"),
    ),
    security(("api_key" = []))
)]
pub async fn get_account_events(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(q): Query<AccountEventsQuery>,
) -> Result<Json<Value>, AppError> {
    if !is_address(&address) {
        return Err(AppError::Validation("invalid address".into()));
    }
    let pos: Option<i16> = match q.role.as_deref().unwrap_or("any") {
        "any" => None,
        "topic1" => Some(1),
        "topic2" => Some(2),
        _ => return Err(AppError::Validation("role must be any, topic1 or topic2".into())),
    };
    let (cl, cid) = match &q.cursor {
        Some(c) => {
            let bad = || AppError::Validation("invalid cursor".into());
            let (l, i) = c.split_once(':').ok_or_else(bad)?;
            (Some(l.parse::<i64>().map_err(|_| bad())?), Some(i.parse::<uuid::Uuid>().map_err(|_| bad())?))
        }
        None => (None, None),
    };
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    // DISTINCT ON collapses an address appearing in several topic positions.
    let rows = sqlx::query(
        "SELECT * FROM (
           SELECT DISTINCT ON (ea.ledger, ea.event_id)
                  e.id, e.contract_id, e.event_type, e.tx_hash, e.ledger, e.timestamp, e.event_data
           FROM event_addresses ea JOIN events e ON e.id = ea.event_id
           WHERE ea.address = $1
             AND ($2::smallint IS NULL OR ea.position = $2)
             AND ($3::bigint IS NULL OR ea.ledger >= $3)
             AND ($4::bigint IS NULL OR ea.ledger <= $4)
             AND ($5::bigint IS NULL OR (ea.ledger, ea.event_id) < ($5, $6))
           ORDER BY ea.ledger DESC, ea.event_id DESC
         ) s ORDER BY ledger DESC, id DESC LIMIT $7",
    )
    .bind(&address).bind(pos).bind(q.from_ledger).bind(q.to_ledger).bind(cl).bind(cid).bind(limit + 1)
    .fetch_all(&state.read_pool)
    .await?;
    let has_more = rows.len() as i64 > limit;
    let mut items = Vec::new();
    let mut next = None;
    for r in rows.iter().take(limit as usize) {
        let l: i64 = r.try_get("ledger")?;
        let id: uuid::Uuid = r.try_get("id")?;
        next = Some(format!("{l}:{id}"));
        items.push(json!({
            "id": id, "contract_id": r.try_get::<String,_>("contract_id")?,
            "event_type": r.try_get::<String,_>("event_type")?,
            "tx_hash": r.try_get::<String,_>("tx_hash")?, "ledger": l,
            "timestamp": r.try_get::<chrono::DateTime<chrono::Utc>,_>("timestamp")?,
            "event_data": r.try_get::<Value,_>("event_data")?,
        }));
    }
    Ok(Json(json!({ "address": address, "events": items, "next_cursor": if has_more { next } else { None } })))
}

/// `POST /v1/admin/event-addresses/backfill`
pub async fn backfill_event_addresses(State(state): State<AppState>) -> Result<Json<Value>, AppError> {
    Ok(Json(json!({ "inserted": backfill(&state.pool, 5000).await? })))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_strkeys() {
        let g = format!("G{}", "A".repeat(55));
        assert!(is_address(&g));
        assert!(!is_address("nope"));
        assert!(!is_address(&format!("X{}", "A".repeat(55))));
    }
    #[test]
    fn extracts_with_positions() {
        let g = format!("G{}", "A".repeat(55));
        let d = json!({"topic": ["transfer", g, {"address": g}], "value": "1"});
        assert_eq!(extract_addresses(&d), vec![(g.clone(), 1), (g, 2)]);
    }
}
