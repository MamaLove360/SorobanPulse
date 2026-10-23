//! Contract deployment and WASM upgrade tracking (issue #1255).
//!
//! The indexer hands every stored `system` event to [`parse`]. Deployment
//! events (`ContractCreated`, `ContractUpdated`, and the host's
//! `executable_update`) are upserted into the `contracts` table inside the
//! same transaction as the event insert, so backfill and live indexing both
//! feed the audit trail. Upserts are ordered by ledger, so replaying events
//! out of order (e.g. a historical backfill after live indexing) converges
//! to the same row.

use axum::extract::{Path, State};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use stellar_xdr::curr::{ContractExecutable, ScVal};

use crate::error::AppError;
use crate::routes::AppState;
use crate::scval_format::parse_stored;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentKind {
    Created,
    Updated,
}

/// A deployment or upgrade decoded from a system event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deployment {
    pub kind: DeploymentKind,
    pub contract_id: String,
    /// Only known for `Created` events.
    pub deployer: Option<String>,
    /// Hex-encoded WASM hash, when the event carries one.
    pub wasm_hash: Option<String>,
}

/// Row of the `contracts` table plus its registry label.
#[derive(Debug, Clone, Serialize, sqlx::FromRow, utoipa::ToSchema)]
pub struct TrackedContract {
    pub contract_id: String,
    pub deployer: Option<String>,
    pub wasm_hash: Option<String>,
    pub deployed_at: Option<DateTime<Utc>>,
    pub deployed_ledger: Option<i64>,
    pub deploy_tx_hash: Option<String>,
    pub updated_at: Option<DateTime<Utc>>,
    pub updated_ledger: Option<i64>,
    /// Name from the contract labels registry (`contract_metadata`), if any.
    pub label: Option<String>,
}

fn kind_from_name(name: &str) -> Option<DeploymentKind> {
    let normalized: String = name.chars().filter(|c| *c != '_').collect::<String>().to_ascii_lowercase();
    match normalized.as_str() {
        "contractcreated" | "contractdeployed" => Some(DeploymentKind::Created),
        "contractupdated" | "contractupgraded" | "executableupdate" => Some(DeploymentKind::Updated),
        _ => None,
    }
}

fn text(v: &ScVal) -> Option<String> {
    match v {
        ScVal::Symbol(s) => std::str::from_utf8(s.as_slice()).ok().map(str::to_owned),
        ScVal::String(s) => std::str::from_utf8(s.as_slice()).ok().map(str::to_owned),
        _ => None,
    }
}

fn address(v: &ScVal) -> Option<String> {
    match v {
        ScVal::Address(a) => Some(a.to_string()),
        _ => None,
    }
}

fn map_get<'a>(v: Option<&'a ScVal>, keys: &[&str]) -> Option<&'a ScVal> {
    let ScVal::Map(Some(map)) = v? else { return None };
    map.0.iter().find(|e| text(&e.key).is_some_and(|k| keys.contains(&k.as_str()))).map(|e| &e.val)
}

/// Record the hex of every WASM hash inside `v`, in order, into `out`.
/// Recognises 32-byte `Bytes` and `ContractInstance` with a WASM executable.
fn collect_hashes(v: &ScVal, out: &mut Vec<String>) {
    match v {
        ScVal::Bytes(b) if b.len() == 32 => out.push(hex::encode(b.as_slice())),
        ScVal::ContractInstance(inst) => {
            if let ContractExecutable::Wasm(h) = &inst.executable {
                out.push(hex::encode(h.0));
            }
        }
        ScVal::Vec(Some(items)) => items.0.iter().for_each(|i| collect_hashes(i, out)),
        ScVal::Map(Some(map)) => map.0.iter().for_each(|e| collect_hashes(&e.val, out)),
        _ => {}
    }
}

/// Decode a deployment from a stored event. Returns `None` for anything
/// that is not a recognised deployment system event.
pub fn parse(contract_id: &str, event_type: &str, topics: &[Value], value: &Value) -> Option<Deployment> {
    if event_type != "system" {
        return None;
    }
    let topics: Vec<ScVal> = topics.iter().filter_map(parse_stored).collect();
    let kind = kind_from_name(&text(topics.first()?)?)?;
    let data = parse_stored(value);
    let rest = &topics[1..];

    let target = map_get(data.as_ref(), &["contract", "contract_id"])
        .and_then(address)
        .unwrap_or_else(|| contract_id.to_string());

    let deployer = (kind == DeploymentKind::Created)
        .then(|| {
            map_get(data.as_ref(), &["deployer", "creator"]).and_then(address).or_else(|| {
                rest.iter().filter_map(address).find(|a| *a != target)
            })
        })
        .flatten();

    // An explicit field wins; otherwise the last hash seen is the new one
    // (`executable_update` carries `[old, new]`).
    let wasm_hash = map_get(data.as_ref(), &["wasm_hash", "new_wasm_hash", "new_executable", "executable"])
        .and_then(|v| {
            let mut found = Vec::new();
            collect_hashes(v, &mut found);
            found.pop()
        })
        .or_else(|| {
            let mut found = Vec::new();
            rest.iter().for_each(|t| collect_hashes(t, &mut found));
            if let Some(d) = &data {
                collect_hashes(d, &mut found);
            }
            found.pop()
        });

    Some(Deployment { kind, contract_id: target, deployer, wasm_hash })
}

/// Upsert a deployment into `contracts`. Returns `true` when the contract
/// was not tracked before.
pub async fn record(
    conn: &mut sqlx::PgConnection,
    d: &Deployment,
    ledger: i64,
    at: DateTime<Utc>,
    tx_hash: &str,
) -> Result<bool, sqlx::Error> {
    let created = d.kind == DeploymentKind::Created;
    let inserted: bool = sqlx::query_scalar(
        "INSERT INTO contracts AS c
             (contract_id, deployer, wasm_hash, deployed_at, deployed_ledger, deploy_tx_hash, updated_at, updated_ledger)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (contract_id) DO UPDATE SET
             deployer        = COALESCE(EXCLUDED.deployer, c.deployer),
             deployed_at     = COALESCE(EXCLUDED.deployed_at, c.deployed_at),
             deployed_ledger = COALESCE(EXCLUDED.deployed_ledger, c.deployed_ledger),
             deploy_tx_hash  = COALESCE(EXCLUDED.deploy_tx_hash, c.deploy_tx_hash),
             wasm_hash = CASE WHEN EXCLUDED.updated_ledger >= c.updated_ledger
                              THEN COALESCE(EXCLUDED.wasm_hash, c.wasm_hash)
                              ELSE COALESCE(c.wasm_hash, EXCLUDED.wasm_hash) END,
             updated_at = CASE WHEN EXCLUDED.updated_ledger >= c.updated_ledger
                               THEN EXCLUDED.updated_at ELSE c.updated_at END,
             updated_ledger = GREATEST(c.updated_ledger, EXCLUDED.updated_ledger)
         RETURNING (xmax = 0)",
    )
    .bind(&d.contract_id)
    .bind(&d.deployer)
    .bind(&d.wasm_hash)
    .bind(created.then_some(at))
    .bind(created.then_some(ledger))
    .bind(created.then_some(tx_hash))
    .bind(at)
    .bind(ledger)
    .fetch_one(conn)
    .await?;
    if inserted {
        crate::metrics::record_contract_tracked();
    }
    Ok(inserted)
}

const SELECT_TRACKED: &str = "SELECT c.contract_id, c.deployer, c.wasm_hash, c.deployed_at, c.deployed_ledger,
            c.deploy_tx_hash, c.updated_at, c.updated_ledger, m.name AS label
     FROM contracts c LEFT JOIN contract_metadata m ON m.contract_id = c.contract_id";

/// `GET /v1/contracts/{contract_id}`: deployment record and label.
#[utoipa::path(
    get,
    path = "/v1/contracts/{contract_id}",
    tag = "events",
    params(("contract_id" = String, Path, description = "Stellar contract ID (56-char, starts with C)")),
    responses(
        (status = 200, description = "Contract deployment record", body = TrackedContract),
        (status = 400, description = "Invalid contract_id format", body = crate::models::ErrorResponse),
        (status = 404, description = "Contract has no deployment record and no events", body = crate::models::ErrorResponse),
    )
)]
pub async fn get_contract(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
) -> Result<Json<TrackedContract>, AppError> {
    crate::handlers::validate_contract_id(&contract_id)?;
    let tracked = sqlx::query_as::<_, TrackedContract>(&format!("{SELECT_TRACKED} WHERE c.contract_id = $1"))
        .bind(&contract_id)
        .fetch_optional(&state.read_pool)
        .await?;
    if let Some(row) = tracked {
        return Ok(Json(row));
    }
    // Contracts deployed before tracking began still have events; return
    // them with unknown deployment fields rather than a 404.
    let label: Option<Option<String>> = sqlx::query_scalar(
        "SELECT (SELECT name FROM contract_metadata WHERE contract_id = $1)
         WHERE EXISTS (SELECT 1 FROM events WHERE contract_id = $1)",
    )
    .bind(&contract_id)
    .fetch_optional(&state.read_pool)
    .await?;
    let label = label.ok_or(AppError::NotFound)?;
    Ok(Json(TrackedContract {
        contract_id,
        deployer: None,
        wasm_hash: None,
        deployed_at: None,
        deployed_ledger: None,
        deploy_tx_hash: None,
        updated_at: None,
        updated_ledger: None,
        label,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::str::FromStr;
    use stellar_xdr::curr::{
        Hash, Limits, ScAddress, ScContractInstance, ScMap, ScMapEntry, ScSymbol, ScVec, WriteXdr,
    };

    const CONTRACT: &str = "CA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJUWDA";
    const DEPLOYER: &str = "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN";

    fn b64(v: ScVal) -> Value {
        json!(v.to_xdr_base64(Limits::none()).unwrap())
    }
    fn sym(s: &str) -> ScVal {
        ScVal::Symbol(ScSymbol::try_from(s).unwrap())
    }
    fn addr(s: &str) -> ScVal {
        ScVal::Address(ScAddress::from_str(s).unwrap())
    }
    fn hash(b: u8) -> ScVal {
        ScVal::Bytes(vec![b; 32].try_into().unwrap())
    }

    #[test]
    fn parses_contract_created_with_map_data() {
        let data = ScVal::Map(Some(ScMap(
            vec![
                ScMapEntry { key: sym("deployer"), val: addr(DEPLOYER) },
                ScMapEntry { key: sym("wasm_hash"), val: hash(0xab) },
            ]
            .try_into()
            .unwrap(),
        )));
        let d = parse(CONTRACT, "system", &[b64(sym("ContractCreated"))], &b64(data)).unwrap();
        assert_eq!(d.kind, DeploymentKind::Created);
        assert_eq!(d.contract_id, CONTRACT);
        assert_eq!(d.deployer.as_deref(), Some(DEPLOYER));
        assert_eq!(d.wasm_hash, Some("ab".repeat(32)));
    }

    #[test]
    fn parses_created_with_topic_layout() {
        let topics = [b64(sym("contract_created")), b64(addr(DEPLOYER)), b64(hash(1))];
        let d = parse(CONTRACT, "system", &topics, &Value::Null).unwrap();
        assert_eq!(d.deployer.as_deref(), Some(DEPLOYER));
        assert_eq!(d.wasm_hash, Some("01".repeat(32)));
    }

    #[test]
    fn executable_update_takes_new_hash_and_no_deployer() {
        let exec = |b: u8| {
            ScVal::ContractInstance(ScContractInstance {
                executable: ContractExecutable::Wasm(Hash([b; 32])),
                storage: None,
            })
        };
        let topics = [b64(sym("executable_update")), b64(exec(1)), b64(exec(2))];
        let d = parse(CONTRACT, "system", &topics, &b64(ScVal::Void)).unwrap();
        assert_eq!(d.kind, DeploymentKind::Updated);
        assert_eq!(d.deployer, None);
        assert_eq!(d.wasm_hash, Some("02".repeat(32)));

        let vec_form = ScVal::Vec(Some(ScVec(vec![sym("Wasm"), hash(7)].try_into().unwrap())));
        let topics = [b64(sym("ContractUpdated")), b64(vec_form)];
        assert_eq!(parse(CONTRACT, "system", &topics, &Value::Null).unwrap().wasm_hash, Some("07".repeat(32)));
    }

    #[test]
    fn serde_json_topics_are_supported() {
        let topics = [json!({"symbol": "ContractCreated"}), json!({"address": DEPLOYER})];
        let d = parse(CONTRACT, "system", &topics, &Value::Null).unwrap();
        assert_eq!(d.deployer.as_deref(), Some(DEPLOYER));
    }

    #[test]
    fn ignores_non_deployment_events() {
        let topics = [b64(sym("ContractCreated"))];
        assert!(parse(CONTRACT, "contract", &topics, &Value::Null).is_none());
        assert!(parse(CONTRACT, "system", &[b64(sym("transfer"))], &Value::Null).is_none());
        assert!(parse(CONTRACT, "system", &[], &Value::Null).is_none());
    }
}
