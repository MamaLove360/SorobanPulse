//! Typed SEP-41 token event decoding (issue #1251).
//!
//! Turns the raw `event_data` JSON of `transfer`, `mint`, `burn` and
//! `clawback` events into a [`Sep41Event`], which the indexer stores in the
//! nullable `events.sep41_*` columns so balance changes can be queried via
//! `GET /v1/events/sep41`.
//!
//! Topic layouts accepted (address topics are recognised by strkey shape, so
//! a trailing SAC/CAP-67 asset topic and the optional admin topic are both
//! tolerated):
//!
//! | event      | topics                                           |
//! |------------|--------------------------------------------------|
//! | `transfer` | `["transfer", from, to, (asset)]`                |
//! | `mint`     | `["mint", (admin), to, (asset)]`                 |
//! | `burn`     | `["burn", from, (asset)]`                        |
//! | `clawback` | `["clawback", (admin), from, (asset)]`           |
//!
//! The data value is an i128 amount, or a map carrying `amount` (CAP-67
//! muxed transfers, `{amount, asset}` documented in
//! `docs/contract-event-schemas.md`). Amounts are kept as exact decimal
//! strings. Anything else decodes to `None`.

use axum::extract::{Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::Row;

use crate::account_events::is_address;
use crate::error::AppError;
use crate::routes::AppState;
use crate::token_events::{amount_str, topic_str};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sep41EventType {
    Transfer,
    Mint,
    Burn,
    Clawback,
}

impl Sep41EventType {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Transfer => "transfer",
            Self::Mint => "mint",
            Self::Burn => "burn",
            Self::Clawback => "clawback",
        }
    }

    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "transfer" => Some(Self::Transfer),
            "mint" => Some(Self::Mint),
            "burn" => Some(Self::Burn),
            "clawback" => Some(Self::Clawback),
            _ => None,
        }
    }
}

/// A decoded SEP-41 balance-changing event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Sep41Event {
    pub event_type: Sep41EventType,
    /// Debited account (`transfer`, `burn`, `clawback`).
    pub from: Option<String>,
    /// Credited account (`transfer`, `mint`).
    pub to: Option<String>,
    /// Exact non-negative i128 as a decimal string.
    pub amount: String,
}

/// Decode a SEP-41 event from its topic array and data value.
#[must_use]
pub fn decode(topics: &[Value], value: &Value) -> Option<Sep41Event> {
    let event_type = Sep41EventType::parse(&topic_str(topics.first()?)?)?;
    let addrs: Vec<String> = topics[1..]
        .iter()
        .filter_map(topic_str)
        .filter(|s| is_address(s))
        .collect();
    let amount = amount_str(value)?;
    if amount.starts_with('-') {
        return None;
    }
    let (from, to) = match event_type {
        Sep41EventType::Transfer if addrs.len() >= 2 => (Some(addrs[0].clone()), Some(addrs[1].clone())),
        // The optional admin topic precedes the recipient / debited account.
        Sep41EventType::Mint => (None, Some(addrs.last()?.clone())),
        Sep41EventType::Burn => (Some(addrs.first()?.clone()), None),
        Sep41EventType::Clawback => (Some(addrs.last()?.clone()), None),
        Sep41EventType::Transfer => return None,
    };
    Some(Sep41Event { event_type, from, to, amount })
}

/// Decode from a stored `event_data` value (`{"topic": [...], "value": ...}`).
#[must_use]
pub fn decode_event_data(event_data: &Value) -> Option<Sep41Event> {
    let topics = event_data.get("topic")?.as_array()?;
    decode(topics, event_data.get("value").unwrap_or(&Value::Null))
}

/// Populate the `sep41_*` columns for already-indexed events, scanning in
/// `(ledger, id)` order after `after`. Returns `(updated, next_cursor)`;
/// `next_cursor` is `None` once the scan is complete.
pub async fn backfill(
    pool: &sqlx::PgPool,
    after: Option<(i64, uuid::Uuid)>,
    batch: i64,
) -> Result<(u64, Option<(i64, uuid::Uuid)>), sqlx::Error> {
    let (al, aid) = after.unzip();
    let rows = sqlx::query(
        "SELECT id, ledger, event_data FROM events
         WHERE sep41_event_type IS NULL
           AND event_data->'topic'->>0 IN ('transfer','mint','burn','clawback')
           AND ($1::bigint IS NULL OR (ledger, id) > ($1, $2))
         ORDER BY ledger, id LIMIT $3",
    )
    .bind(al)
    .bind(aid)
    .bind(batch)
    .fetch_all(pool)
    .await?;
    let mut updated = 0;
    let mut last = None;
    for r in &rows {
        let id: uuid::Uuid = r.try_get("id")?;
        last = Some((r.try_get::<i64, _>("ledger")?, id));
        let Some(ev) = decode_event_data(&r.try_get::<Value, _>("event_data")?) else { continue };
        updated += sqlx::query(
            "UPDATE events SET sep41_event_type = $2, sep41_from = $3, sep41_to = $4, sep41_amount = $5::numeric
             WHERE id = $1",
        )
        .bind(id)
        .bind(ev.event_type.as_str())
        .bind(&ev.from)
        .bind(&ev.to)
        .bind(&ev.amount)
        .execute(pool)
        .await?
        .rows_affected();
    }
    let next = if rows.len() as i64 == batch { last } else { None };
    Ok((updated, next))
}

#[derive(Debug, Deserialize)]
pub struct Sep41Query {
    /// Debited account address.
    pub from: Option<String>,
    /// Credited account address.
    pub to: Option<String>,
    /// Minimum amount (exact i128 decimal string, inclusive).
    pub min_amount: Option<String>,
    /// `transfer`, `mint`, `burn` or `clawback`.
    pub event_type: Option<String>,
    /// Restrict to one token contract.
    pub contract_id: Option<String>,
    /// `<ledger>:<event_id>` from a previous `next_cursor`.
    pub cursor: Option<String>,
    /// Page size, 1–200 (default 50).
    pub limit: Option<i64>,
}

fn parse_cursor(c: &str) -> Result<(i64, uuid::Uuid), AppError> {
    let bad = || AppError::Validation("invalid cursor".into());
    let (l, i) = c.split_once(':').ok_or_else(bad)?;
    Ok((l.parse().map_err(|_| bad())?, i.parse().map_err(|_| bad())?))
}

/// `GET /v1/events/sep41` — query decoded SEP-41 token events.
#[utoipa::path(
    get,
    path = "/v1/events/sep41",
    tag = "events",
    params(
        ("from" = Option<String>, Query, description = "Debited account address"),
        ("to" = Option<String>, Query, description = "Credited account address"),
        ("min_amount" = Option<String>, Query, description = "Minimum amount, exact i128 decimal string (inclusive)"),
        ("event_type" = Option<String>, Query, description = "transfer, mint, burn or clawback"),
        ("contract_id" = Option<String>, Query, description = "Restrict to one token contract"),
        ("cursor" = Option<String>, Query, description = "`<ledger>:<event_id>` from a previous next_cursor"),
        ("limit" = Option<i64>, Query, description = "Page size, 1-200 (default 50)"),
    ),
    responses(
        (status = 200, description = "Decoded SEP-41 events, newest first", body = serde_json::Value),
        (status = 400, description = "Invalid address, amount, event_type or cursor"),
    ),
    security(("api_key" = []))
)]
pub async fn get_sep41_events(
    State(state): State<AppState>,
    Query(q): Query<Sep41Query>,
) -> Result<Json<Value>, AppError> {
    for (name, addr) in [("from", &q.from), ("to", &q.to)] {
        if addr.as_deref().is_some_and(|a| !is_address(a)) {
            return Err(AppError::Validation(format!("invalid {name} address")));
        }
    }
    if let Some(m) = &q.min_amount {
        if m.parse::<i128>().is_err() {
            return Err(AppError::Validation("invalid min_amount".into()));
        }
    }
    let event_type = match q.event_type.as_deref() {
        Some(t) => Some(
            Sep41EventType::parse(t)
                .ok_or_else(|| AppError::Validation("event_type must be transfer, mint, burn or clawback".into()))?
                .as_str(),
        ),
        None => None,
    };
    if let Some(c) = &q.contract_id {
        crate::handlers::validate_contract_id(c)?;
    }
    let (cl, cid) = q.cursor.as_deref().map(parse_cursor).transpose()?.unzip();
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    let rows = sqlx::query(
        "SELECT id, contract_id, tx_hash, ledger, timestamp,
                sep41_event_type, sep41_from, sep41_to, sep41_amount::text AS sep41_amount
         FROM events
         WHERE sep41_event_type IS NOT NULL
           AND ($1::text IS NULL OR sep41_from = $1)
           AND ($2::text IS NULL OR sep41_to = $2)
           AND ($3::text IS NULL OR sep41_amount >= $3::numeric)
           AND ($4::text IS NULL OR sep41_event_type = $4)
           AND ($5::text IS NULL OR contract_id = $5)
           AND ($6::bigint IS NULL OR (ledger, id) < ($6, $7))
         ORDER BY ledger DESC, id DESC LIMIT $8",
    )
    .bind(&q.from)
    .bind(&q.to)
    .bind(&q.min_amount)
    .bind(event_type)
    .bind(&q.contract_id)
    .bind(cl)
    .bind(cid)
    .bind(limit + 1)
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
            "id": id,
            "contract_id": r.try_get::<String, _>("contract_id")?,
            "tx_hash": r.try_get::<String, _>("tx_hash")?,
            "ledger": l,
            "timestamp": r.try_get::<chrono::DateTime<chrono::Utc>, _>("timestamp")?,
            "event_type": r.try_get::<String, _>("sep41_event_type")?,
            "from": r.try_get::<Option<String>, _>("sep41_from")?,
            "to": r.try_get::<Option<String>, _>("sep41_to")?,
            "amount": r.try_get::<Option<String>, _>("sep41_amount")?,
        }));
    }
    Ok(Json(json!({ "events": items, "next_cursor": if has_more { next } else { None } })))
}

#[derive(Debug, Deserialize)]
pub struct BackfillQuery {
    pub cursor: Option<String>,
}

/// `POST /v1/admin/sep41/backfill` — decode one batch of already-indexed
/// events. Call repeatedly with the returned `next_cursor` until it is null.
pub async fn backfill_sep41(
    State(state): State<AppState>,
    Query(q): Query<BackfillQuery>,
) -> Result<Json<Value>, AppError> {
    let after = q.cursor.as_deref().map(parse_cursor).transpose()?;
    let (updated, next) = backfill(&state.pool, after, 5000).await?;
    Ok(Json(json!({
        "updated": updated,
        "next_cursor": next.map(|(l, id)| format!("{l}:{id}")),
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn g(c: char) -> String {
        format!("G{}", c.to_string().repeat(55))
    }

    fn topics(v: &[&str]) -> Vec<Value> {
        v.iter().map(|s| json!(s)).collect()
    }

    #[test]
    fn transfer() {
        let (a, b) = (g('A'), g('B'));
        let e = decode(&topics(&["transfer", &a, &b]), &json!("100")).unwrap();
        assert_eq!(
            e,
            Sep41Event { event_type: Sep41EventType::Transfer, from: Some(a), to: Some(b), amount: "100".into() }
        );
    }

    #[test]
    fn documented_amount_asset_map_and_trailing_asset_topic() {
        let (a, b) = (g('A'), g('B'));
        let data = json!({"topic": ["transfer", a, b, "native"], "value": {"amount": "1000000000", "asset": "native"}});
        let e = decode_event_data(&data).unwrap();
        assert_eq!(e.amount, "1000000000");
        assert_eq!(e.to.as_deref(), Some(b.as_str()));
    }

    #[test]
    fn mint_with_and_without_admin() {
        let (admin, to) = (g('A'), g('B'));
        for t in [vec!["mint", &admin, &to], vec!["mint", &to], vec!["mint", &to, "USDC"]] {
            let e = decode(&topics(&t), &json!("5")).unwrap();
            assert_eq!(e.to.as_deref(), Some(to.as_str()), "{t:?}");
            assert_eq!(e.from, None);
        }
    }

    #[test]
    fn burn_and_clawback() {
        let (admin, from) = (g('A'), g('B'));
        let e = decode(&topics(&["burn", &from]), &json!("1")).unwrap();
        assert_eq!((e.from.as_deref(), e.to), (Some(from.as_str()), None));
        let e = decode(&topics(&["clawback", &admin, &from]), &json!("1")).unwrap();
        assert_eq!(e.from.as_deref(), Some(from.as_str()));
    }

    #[test]
    fn exact_i128_amounts() {
        let e = decode(&topics(&["burn", &g('A')]), &json!({"hi": "1", "lo": "0"})).unwrap();
        assert_eq!(e.amount, (1i128 << 64).to_string());
        let e = decode(&topics(&["burn", &g('A')]), &json!(i128::MAX.to_string())).unwrap();
        assert_eq!(e.amount, i128::MAX.to_string());
    }

    #[test]
    fn rejects_non_sep41() {
        let (a, b) = (g('A'), g('B'));
        assert!(decode(&topics(&["approve", &a, &b]), &json!("1")).is_none());
        assert!(decode(&topics(&["transfer", &a]), &json!("1")).is_none());
        assert!(decode(&topics(&["transfer", &a, &b]), &json!("not a number")).is_none());
        assert!(decode(&topics(&["transfer", &a, &b]), &json!("-1")).is_none());
        assert!(decode(&topics(&["burn", "not-an-address"]), &json!("1")).is_none());
        assert!(decode(&[], &json!("1")).is_none());
        assert!(decode_event_data(&json!({"value": "1"})).is_none());
    }

    fn address() -> impl Strategy<Value = String> {
        "[GC][A-Z2-7]{55}"
    }

    proptest! {
        #[test]
        fn never_panics(names in prop::collection::vec(".{0,70}", 0..5), value in ".{0,50}") {
            let t: Vec<Value> = names.into_iter().map(Value::String).collect();
            let _ = decode(&t, &Value::String(value));
        }

        #[test]
        fn transfer_round_trips(from in address(), to in address(), amount in 0i128.., asset in proptest::option::of("[a-z]{1,12}")) {
            let mut t = vec![json!("transfer"), json!(from), json!(to)];
            if let Some(a) = asset { t.push(json!(a)); }
            let e = decode(&t, &json!(amount.to_string())).unwrap();
            prop_assert_eq!(e.event_type, Sep41EventType::Transfer);
            prop_assert_eq!(e.from, Some(from));
            prop_assert_eq!(e.to, Some(to));
            prop_assert_eq!(e.amount, amount.to_string());
        }

        #[test]
        fn mint_recipient_is_last_address(admin in proptest::option::of(address()), to in address(), amount in 0i128..) {
            let mut t = vec![json!("mint")];
            if let Some(a) = admin { t.push(json!(a)); }
            t.push(json!(to));
            let e = decode(&t, &json!({"amount": amount.to_string()})).unwrap();
            prop_assert_eq!(e.to, Some(to));
            prop_assert_eq!(e.amount, amount.to_string());
        }

        #[test]
        fn negative_amounts_rejected(from in address(), amount in i128::MIN..0) {
            prop_assert!(decode(&[json!("burn"), json!(from)], &json!(amount.to_string())).is_none());
        }

        #[test]
        fn hi_lo_matches_i128(v in 0i128..) {
            let hi = (v >> 64) as i64;
            let lo = v as u64;
            let e = decode(&[json!("burn"), json!(format!("G{}", "A".repeat(55)))], &json!({"hi": hi.to_string(), "lo": lo.to_string()})).unwrap();
            prop_assert_eq!(e.amount, v.to_string());
        }
    }
}
