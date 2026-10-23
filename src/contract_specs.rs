//! Auto-fetch contract specs (`contractspecv0`) from on-chain WASM
//! (issues #1057, #1253).
//!
//! Enabled with `AUTO_FETCH_CONTRACT_SPECS=true`. The indexer calls
//! [`request_fetch`] for every stored event; the first time a contract with
//! no stored spec is seen, the worker resolves the instance's WASM hash with
//! `getLedgerEntries`, downloads the `ContractCode` entry, parses the
//! `contractspecv0` custom section with `stellar-xdr` and stores it in
//! `contract_abis` (cached by WASM hash in `contract_spec_cache`). A periodic
//! sweep catches contracts missed across restarts, and on-chain specs older
//! than `CONTRACT_SPEC_TTL_SECS` are re-checked so upgrades pick up the new
//! spec. Manually registered ABIs are never overwritten.

use axum::extract::{Path, State};
use axum::Json;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use stellar_xdr::curr::{
    ContractDataDurability, ContractExecutable, ContractId, Hash, LedgerEntryData, LedgerKey,
    LedgerKeyContractCode, LedgerKeyContractData, Limited, Limits, ReadXdr, ScAddress, ScSpecEntry,
    ScSpecEventParamLocationV0, ScSpecTypeDef, ScSpecUdtUnionCaseV0, ScVal, WriteXdr,
};

use crate::error::AppError;
use crate::routes::AppState;

/// Delay between RPC lookups so the worker never floods the RPC node.
const RPC_SPACING: Duration = Duration::from_millis(500);
/// Bound on the in-memory set of contracts already queued this process.
const MAX_SEEN: usize = 100_000;

static QUEUE: OnceLock<tokio::sync::mpsc::UnboundedSender<String>> = OnceLock::new();
static SEEN: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

pub fn enabled() -> bool {
    std::env::var("AUTO_FETCH_CONTRACT_SPECS").map(|v| v == "true" || v == "1").unwrap_or(false)
}

/// How long an on-chain spec is trusted before re-checking the WASM hash.
fn spec_ttl() -> Duration {
    let secs = std::env::var("CONTRACT_SPEC_TTL_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(86_400);
    Duration::from_secs(secs)
}

fn http() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder().timeout(Duration::from_secs(10)).build().unwrap_or_default()
    })
}

/// Queue `contract_id` for a spec fetch the first time this process sees
/// it. Cheap and non-blocking; a no-op when the worker is not running.
pub fn request_fetch(contract_id: &str) {
    let Some(tx) = QUEUE.get() else { return };
    let mut seen = SEEN.get_or_init(Mutex::default).lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    if seen.len() >= MAX_SEEN {
        seen.clear();
    }
    if seen.insert(contract_id.to_owned()) {
        let _ = tx.send(contract_id.to_owned());
    }
}

fn instance_key(contract_id: &str) -> Option<LedgerKey> {
    Some(LedgerKey::ContractData(LedgerKeyContractData {
        contract: ScAddress::Contract(ContractId::from_str(contract_id).ok()?),
        key: ScVal::LedgerKeyContractInstance,
        durability: ContractDataDurability::Persistent,
    }))
}

/// WASM hash of a contract instance entry; `None` for SACs (no WASM).
pub fn wasm_hash_from_instance(entry: &LedgerEntryData) -> Option<[u8; 32]> {
    let LedgerEntryData::ContractData(data) = entry else { return None };
    let ScVal::ContractInstance(instance) = &data.val else { return None };
    match &instance.executable {
        ContractExecutable::Wasm(Hash(h)) => Some(*h),
        ContractExecutable::StellarAsset => None,
    }
}

fn leb(b: &[u8], p: &mut usize) -> Option<usize> {
    let (mut r, mut shift) = (0usize, 0);
    loop {
        let byte = *b.get(*p)?;
        *p += 1;
        r |= ((byte & 0x7f) as usize) << shift;
        if byte & 0x80 == 0 {
            return Some(r);
        }
        shift += 7;
        if shift > 35 {
            return None;
        }
    }
}

/// Return the payloads of all custom sections named `name`, concatenated.
pub fn custom_section(wasm: &[u8], name: &str) -> Option<Vec<u8>> {
    if wasm.get(0..4)? != b"\0asm" {
        return None;
    }
    let (mut p, mut out, mut found) = (8usize, Vec::new(), false);
    while p < wasm.len() {
        let id = wasm[p];
        p += 1;
        let size = leb(wasm, &mut p)?;
        let end = p.checked_add(size)?;
        if end > wasm.len() {
            break;
        }
        if id == 0 {
            let mut q = p;
            let nl = leb(wasm, &mut q)?;
            // The name must lie within this section; a bogus length used to
            // slice past `end` and panic (fuzz crash, issue #1224).
            let name_end = q.checked_add(nl).filter(|&e| e <= end)?;
            if &wasm[q..name_end] == name.as_bytes() {
                out.extend_from_slice(&wasm[name_end..end]);
                found = true;
            }
        }
        p = end;
    }
    found.then_some(out)
}

/// Parse a `contractspecv0` section: a stream of XDR `ScSpecEntry` values.
pub fn parse_spec(section: &[u8]) -> Result<Vec<ScSpecEntry>, stellar_xdr::curr::Error> {
    let mut reader = Limited::new(std::io::Cursor::new(section), Limits::len(section.len()));
    ScSpecEntry::read_xdr_iter(&mut reader).collect()
}

/// Human-readable Rust-like name of a spec type, e.g. `Vec<Option<i128>>`.
pub fn type_name(t: &ScSpecTypeDef) -> String {
    use ScSpecTypeDef as T;
    match t {
        T::Bool => "bool".into(),
        T::Void => "()".into(),
        T::U32 | T::I32 | T::U64 | T::I64 | T::U128 | T::I128 | T::U256 | T::I256 => t.name().to_lowercase(),
        T::Option(o) => format!("Option<{}>", type_name(&o.value_type)),
        T::Result(r) => format!("Result<{}, {}>", type_name(&r.ok_type), type_name(&r.error_type)),
        T::Vec(v) => format!("Vec<{}>", type_name(&v.element_type)),
        T::Map(m) => format!("Map<{}, {}>", type_name(&m.key_type), type_name(&m.value_type)),
        T::Tuple(tp) => format!("({})", tp.value_types.iter().map(type_name).collect::<Vec<_>>().join(", ")),
        T::BytesN(b) => format!("BytesN<{}>", b.n),
        T::Udt(u) => u.name.to_utf8_string_lossy(),
        other => other.name().to_string(),
    }
}

fn field(name: &impl ToString, t: &ScSpecTypeDef) -> Value {
    json!({ "name": name.to_string(), "type": type_name(t) })
}

/// Convert spec entries into the `contract_abis` JSON shape: an array of
/// `{kind, name, ...}` objects. Event entries come first and are keyed by
/// their first topic (what `abi::decode_event_data` matches on), with
/// `inputs` listing the params carried in the event data, in order.
pub fn spec_to_abi(entries: &[ScSpecEntry]) -> Value {
    let mut events = Vec::new();
    let mut rest = Vec::new();
    for entry in entries {
        match entry {
            ScSpecEntry::EventV0(e) => {
                let topic = e.prefix_topics.first().map_or_else(|| e.name.0.to_string(), |t| t.0.to_string());
                let inputs: Vec<Value> = e
                    .params
                    .iter()
                    .filter(|p| p.location == ScSpecEventParamLocationV0::Data)
                    .map(|p| field(&p.name, &p.type_))
                    .collect();
                let params: Vec<Value> = e
                    .params
                    .iter()
                    .map(|p| {
                        let mut f = field(&p.name, &p.type_);
                        f["location"] = json!(if p.location == ScSpecEventParamLocationV0::Data { "data" } else { "topic" });
                        f
                    })
                    .collect();
                events.push(json!({
                    "kind": "event", "name": topic, "event_name": e.name.0.to_string(), "doc": e.doc.to_string(),
                    "prefix_topics": e.prefix_topics.iter().map(|t| t.0.to_string()).collect::<Vec<_>>(),
                    "data_format": e.data_format.name().to_lowercase(), "inputs": inputs, "params": params,
                }));
            }
            ScSpecEntry::FunctionV0(f) => rest.push(json!({
                "kind": "function", "name": f.name.0.to_string(), "doc": f.doc.to_string(),
                "inputs": f.inputs.iter().map(|i| field(&i.name, &i.type_)).collect::<Vec<_>>(),
                "outputs": f.outputs.iter().map(type_name).collect::<Vec<_>>(),
            })),
            ScSpecEntry::UdtStructV0(s) => rest.push(json!({
                "kind": "struct", "name": s.name.to_string(), "doc": s.doc.to_string(),
                "fields": s.fields.iter().map(|f| field(&f.name, &f.type_)).collect::<Vec<_>>(),
            })),
            ScSpecEntry::UdtUnionV0(u) => rest.push(json!({
                "kind": "union", "name": u.name.to_string(), "doc": u.doc.to_string(),
                "cases": u.cases.iter().map(|c| match c {
                    ScSpecUdtUnionCaseV0::VoidV0(v) => json!({ "name": v.name.to_string(), "types": [] }),
                    ScSpecUdtUnionCaseV0::TupleV0(t) => json!({
                        "name": t.name.to_string(), "types": t.type_.iter().map(type_name).collect::<Vec<_>>(),
                    }),
                }).collect::<Vec<_>>(),
            })),
            ScSpecEntry::UdtEnumV0(e) => rest.push(json!({
                "kind": "enum", "name": e.name.to_string(), "doc": e.doc.to_string(),
                "cases": e.cases.iter().map(|c| json!({ "name": c.name.to_string(), "value": c.value })).collect::<Vec<_>>(),
            })),
            ScSpecEntry::UdtErrorEnumV0(e) => rest.push(json!({
                "kind": "error_enum", "name": e.name.to_string(), "doc": e.doc.to_string(),
                "cases": e.cases.iter().map(|c| json!({ "name": c.name.to_string(), "value": c.value })).collect::<Vec<_>>(),
            })),
        }
    }
    events.extend(rest);
    Value::Array(events)
}

async fn get_entry(client: &reqwest::Client, url: &str, key: &LedgerKey) -> Option<LedgerEntryData> {
    let key = key.to_xdr_base64(Limits::none()).ok()?;
    let body = json!({"jsonrpc":"2.0","id":1,"method":"getLedgerEntries","params":{"keys":[key]}});
    let v: Value = client.post(url).json(&body).send().await.ok()?.json().await.ok()?;
    let xdr = v.pointer("/result/entries/0/xdr")?.as_str()?;
    LedgerEntryData::from_xdr_base64(xdr, Limits::none()).ok()
}

/// Current WASM hash (hex) of a contract, or `None` for SAC / unknown.
pub async fn fetch_wasm_hash(client: &reqwest::Client, url: &str, contract_id: &str) -> Option<String> {
    let entry = get_entry(client, url, &instance_key(contract_id)?).await?;
    wasm_hash_from_instance(&entry).map(hex::encode)
}

async fn fetch_spec(client: &reqwest::Client, url: &str, wasm_hash: &str) -> Option<Value> {
    let hash: [u8; 32] = hex::decode(wasm_hash).ok()?.try_into().ok()?;
    let key = LedgerKey::ContractCode(LedgerKeyContractCode { hash: Hash(hash) });
    let LedgerEntryData::ContractCode(code) = get_entry(client, url, &key).await? else { return None };
    let section = custom_section(code.code.as_slice(), "contractspecv0")?;
    match parse_spec(&section) {
        Ok(entries) => Some(spec_to_abi(&entries)),
        Err(e) => {
            tracing::warn!(wasm_hash, error = %e, "invalid contractspecv0 section");
            None
        }
    }
}

/// Fetch and cache the spec for `wasm_hash`, then link it to `contract_id`.
/// Returns true if an on-chain spec is now stored for the contract. A
/// manually registered ABI for the contract is left untouched.
pub async fn store_spec_for(pool: &PgPool, client: &reqwest::Client, url: &str, contract_id: &str, wasm_hash: &str) -> bool {
    let cached: Option<Value> = sqlx::query_scalar("SELECT spec FROM contract_spec_cache WHERE wasm_hash = $1")
        .bind(wasm_hash)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten();
    let spec = match cached {
        Some(s) => s,
        None => {
            let Some(s) = fetch_spec(client, url, wasm_hash).await else { return false };
            let _ = sqlx::query("INSERT INTO contract_spec_cache (wasm_hash, spec) VALUES ($1, $2) ON CONFLICT DO NOTHING")
                .bind(wasm_hash)
                .bind(&s)
                .execute(pool)
                .await;
            s
        }
    };
    let abi_hash = hex::encode(Sha256::digest(spec.to_string()));
    sqlx::query(
        "INSERT INTO contract_abis (contract_id, abi, abi_hash, fetched_at, is_valid, source, wasm_hash)
         VALUES ($1, $2, $3, NOW(), true, 'onchain', $4)
         ON CONFLICT (contract_id) DO UPDATE SET abi = EXCLUDED.abi, abi_hash = EXCLUDED.abi_hash,
             wasm_hash = EXCLUDED.wasm_hash, updated_at = NOW(), fetched_at = NOW()
         WHERE contract_abis.source = 'onchain'",
    )
    .bind(contract_id)
    .bind(&spec)
    .bind(abi_hash)
    .bind(wasm_hash)
    .execute(pool)
    .await
    .is_ok_and(|r| r.rows_affected() > 0)
}

/// Resolve and store the spec for one contract, recording the attempt so
/// SACs and spec-less contracts are not re-queried for a day.
async fn fetch_and_record(pool: &PgPool, client: &reqwest::Client, url: &str, contract_id: &str) {
    let status = match fetch_wasm_hash(client, url, contract_id).await {
        None => "no_wasm", // SAC or unresolved: nothing to fetch
        Some(h) => {
            if store_spec_for(pool, client, url, contract_id, &h).await {
                "ok"
            } else {
                "no_spec"
            }
        }
    };
    let _ = sqlx::query(
        "INSERT INTO contract_spec_attempts (contract_id, status) VALUES ($1, $2)
         ON CONFLICT (contract_id) DO UPDATE SET status = $2, attempted_at = NOW()",
    )
    .bind(contract_id)
    .bind(status)
    .execute(pool)
    .await;
}

async fn needs_spec(pool: &PgPool, contract_id: &str) -> bool {
    sqlx::query_scalar::<_, bool>(
        "SELECT NOT EXISTS (SELECT 1 FROM contract_abis WHERE contract_id = $1)
            AND NOT EXISTS (SELECT 1 FROM contract_spec_attempts
                            WHERE contract_id = $1 AND attempted_at > NOW() - INTERVAL '1 day')",
    )
    .bind(contract_id)
    .fetch_one(pool)
    .await
    .unwrap_or(false)
}

/// One sweep over contracts lacking an ABI (catches anything queued before
/// a restart), rate limited to one RPC round trip per `interval`.
pub async fn run_once(pool: &PgPool, client: &reqwest::Client, url: &str, interval: Duration, batch: i64) {
    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT e.contract_id FROM events e
         WHERE NOT EXISTS (SELECT 1 FROM contract_abis a WHERE a.contract_id = e.contract_id)
           AND NOT EXISTS (SELECT 1 FROM contract_spec_attempts t WHERE t.contract_id = e.contract_id AND t.attempted_at > NOW() - INTERVAL '1 day')
         LIMIT $1",
    )
    .bind(batch)
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    for id in ids {
        tokio::time::sleep(interval).await;
        fetch_and_record(pool, client, url, &id).await;
    }
}

/// TTL refresh: re-check on-chain specs older than `ttl`. A changed WASM
/// hash (upgrade) stores the new spec; otherwise the row is marked fresh.
pub async fn refresh_stale(pool: &PgPool, client: &reqwest::Client, url: &str, ttl: Duration, batch: i64) {
    let rows: Vec<(String, Option<String>)> = sqlx::query_as(
        "SELECT contract_id, wasm_hash FROM contract_abis
         WHERE source = 'onchain' AND fetched_at < NOW() - make_interval(secs => $1)
         ORDER BY fetched_at LIMIT $2",
    )
    .bind(ttl.as_secs_f64())
    .bind(batch)
    .fetch_all(pool)
    .await
    .unwrap_or_default();
    for (id, old_hash) in rows {
        tokio::time::sleep(RPC_SPACING).await;
        if let Some(hash) = fetch_wasm_hash(client, url, &id).await {
            if old_hash.as_deref() != Some(hash.as_str()) && store_spec_for(pool, client, url, &id, &hash).await {
                tracing::info!(contract_id = %id, wasm_hash = %hash, "contract spec refreshed after upgrade");
                continue;
            }
        }
        let _ = sqlx::query("UPDATE contract_abis SET fetched_at = NOW() WHERE contract_id = $1 AND source = 'onchain'")
            .bind(&id)
            .execute(pool)
            .await;
    }
}

/// Background worker; a no-op unless `AUTO_FETCH_CONTRACT_SPECS` is set.
pub async fn run_worker(pool: PgPool, rpc_url: String) {
    if !enabled() {
        return;
    }
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    if QUEUE.set(tx).is_err() {
        return; // already running
    }
    let client = http();
    let ttl = spec_ttl();
    let mut sweep = tokio::time::interval(Duration::from_secs(30));
    loop {
        tokio::select! {
            Some(id) = rx.recv() => {
                if needs_spec(&pool, &id).await {
                    fetch_and_record(&pool, client, &rpc_url, &id).await;
                    tokio::time::sleep(RPC_SPACING).await;
                }
            }
            _ = sweep.tick() => {
                run_once(&pool, client, &rpc_url, RPC_SPACING, 20).await;
                refresh_stale(&pool, client, &rpc_url, ttl, 20).await;
            }
        }
    }
}

type SpecRow = (Value, String, Option<String>, chrono::DateTime<chrono::Utc>);

async fn load_spec(pool: &PgPool, contract_id: &str) -> Result<Option<SpecRow>, sqlx::Error> {
    sqlx::query_as("SELECT abi, source, wasm_hash, fetched_at FROM contract_abis WHERE contract_id = $1")
        .bind(contract_id)
        .fetch_optional(pool)
        .await
}

/// `GET /v1/contracts/{contract_id}/spec`: the contract's spec as JSON.
/// When auto-fetch is enabled and nothing is stored yet, the spec is
/// fetched from the chain on demand.
#[utoipa::path(
    get,
    path = "/v1/contracts/{contract_id}/spec",
    tag = "events",
    params(("contract_id" = String, Path, description = "Stellar contract ID (56-char, starts with C)")),
    responses(
        (status = 200, description = "Contract spec: functions, events and types"),
        (status = 400, description = "Invalid contract_id format", body = crate::models::ErrorResponse),
        (status = 404, description = "No spec available (e.g. SAC, or auto-fetch disabled)", body = crate::models::ErrorResponse),
    )
)]
pub async fn get_contract_spec(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
) -> Result<Json<Value>, AppError> {
    crate::handlers::validate_contract_id(&contract_id)?;
    let mut row = load_spec(&state.read_pool, &contract_id).await?;
    if row.is_none() && enabled() {
        let url = &state.config.stellar_rpc_url;
        if let Some(hash) = fetch_wasm_hash(http(), url, &contract_id).await {
            if store_spec_for(&state.pool, http(), url, &contract_id, &hash).await {
                row = load_spec(&state.pool, &contract_id).await?;
            }
        }
    }
    let (spec, source, wasm_hash, fetched_at) = row.ok_or(AppError::NotFound)?;
    Ok(Json(json!({
        "contract_id": contract_id,
        "source": source,
        "wasm_hash": wasm_hash,
        "fetched_at": fetched_at,
        "spec": spec,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use stellar_xdr::curr::{
        ContractDataEntry, ExtensionPoint, ScContractInstance, ScSpecEventDataFormat, ScSpecEventParamV0,
        ScSpecEventV0, ScSpecFunctionInputV0, ScSpecFunctionV0, ScSpecTypeOption, ScSpecTypeVec, ScSymbol,
    };

    /// Minimal WASM: header + custom section `name` with `payload`.
    fn wasm_with(name: &str, payload: &[u8]) -> Vec<u8> {
        let mut w = b"\0asm\x01\0\0\0".to_vec();
        let mut body = vec![u8::try_from(name.len()).unwrap()];
        body.extend(name.as_bytes());
        body.extend(payload);
        w.push(0);
        // multi-byte LEB128 size so the length decoder is exercised
        let n = body.len();
        w.extend([u8::try_from(n & 0x7f).unwrap() | 0x80, u8::try_from(n >> 7).unwrap()]);
        w.extend(body);
        w
    }

    fn sym(s: &str) -> ScSymbol {
        ScSymbol::try_from(s).unwrap()
    }

    fn sample_spec() -> Vec<ScSpecEntry> {
        vec![
            ScSpecEntry::FunctionV0(ScSpecFunctionV0 {
                doc: "Move tokens".try_into().unwrap(),
                name: sym("transfer"),
                inputs: vec![
                    ScSpecFunctionInputV0 { doc: Default::default(), name: "from".try_into().unwrap(), type_: ScSpecTypeDef::Address },
                    ScSpecFunctionInputV0 {
                        doc: Default::default(),
                        name: "memo".try_into().unwrap(),
                        type_: ScSpecTypeDef::Option(Box::new(ScSpecTypeOption {
                            value_type: Box::new(ScSpecTypeDef::Vec(Box::new(ScSpecTypeVec { element_type: Box::new(ScSpecTypeDef::I128) }))),
                        })),
                    },
                ]
                .try_into()
                .unwrap(),
                outputs: vec![ScSpecTypeDef::Void].try_into().unwrap(),
            }),
            ScSpecEntry::EventV0(ScSpecEventV0 {
                doc: Default::default(),
                lib: Default::default(),
                name: sym("Transfer"),
                prefix_topics: vec![sym("transfer")].try_into().unwrap(),
                params: vec![
                    ScSpecEventParamV0 { doc: Default::default(), name: "from".try_into().unwrap(), type_: ScSpecTypeDef::Address, location: ScSpecEventParamLocationV0::TopicList },
                    ScSpecEventParamV0 { doc: Default::default(), name: "amount".try_into().unwrap(), type_: ScSpecTypeDef::I128, location: ScSpecEventParamLocationV0::Data },
                ]
                .try_into()
                .unwrap(),
                data_format: ScSpecEventDataFormat::SingleValue,
            }),
        ]
    }

    #[test]
    fn parses_custom_section() {
        let w = wasm_with("contractspecv0", &[1, 2, 3]);
        assert_eq!(custom_section(&w, "contractspecv0").unwrap(), vec![1, 2, 3]);
        assert!(custom_section(&w, "other").is_none());
        assert!(custom_section(b"not wasm", "contractspecv0").is_none());
    }

    #[test]
    fn custom_section_name_past_section_end_does_not_panic() {
        // Section size 1, but name length 5 points past the section end.
        let mut w = b"\0asm\x01\0\0\0".to_vec();
        w.extend([0, 1, 5, b'a', b'b', b'c', b'd', b'e']);
        assert!(custom_section(&w, "abcde").is_none());
        // Name length overflowing usize must not panic either.
        let mut w = b"\0asm\x01\0\0\0".to_vec();
        w.extend([0, 6, 0xff, 0xff, 0xff, 0xff, 0x0f, 0]);
        assert!(custom_section(&w, "contractspecv0").is_none());
    }

    #[test]
    fn parses_spec_stream_from_wasm() {
        let mut section = Vec::new();
        for e in sample_spec() {
            section.extend(e.to_xdr(Limits::none()).unwrap());
        }
        let w = wasm_with("contractspecv0", &section);
        let entries = parse_spec(&custom_section(&w, "contractspecv0").unwrap()).unwrap();
        assert_eq!(entries, sample_spec());
        assert!(parse_spec(&section[..section.len() - 1]).is_err());
    }

    #[test]
    fn abi_lists_events_first_with_data_inputs() {
        let abi = spec_to_abi(&sample_spec());
        crate::abi::validate_abi(&abi).unwrap();
        let first = &abi[0];
        assert_eq!(first["kind"], "event");
        assert_eq!(first["name"], "transfer");
        assert_eq!(first["inputs"], json!([{"name": "amount", "type": "i128"}]));
        assert_eq!(first["params"][0]["location"], "topic");
        let func = &abi[1];
        assert_eq!(func["kind"], "function");
        assert_eq!(func["inputs"][1]["type"], "Option<Vec<i128>>");
        assert_eq!(func["outputs"], json!(["()"]));
    }

    #[test]
    fn registered_decoder_understands_onchain_abi() {
        let abi = spec_to_abi(&sample_spec());
        let event_data = json!({
            "topic": [{"symbol": "transfer"}, {"address": "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"}],
            "value": {"i128": {"hi": 0, "lo": 42}},
        });
        let decoded = crate::abi::decode_event_data(&abi, &event_data).unwrap();
        assert_eq!(decoded["event"], "transfer");
        assert!(!decoded["amount"].is_null());
    }

    #[test]
    fn instance_hash_extraction() {
        let entry = |exec| {
            LedgerEntryData::ContractData(ContractDataEntry {
                ext: ExtensionPoint::V0,
                contract: ScAddress::Contract(ContractId(Hash([9; 32]))),
                key: ScVal::LedgerKeyContractInstance,
                durability: ContractDataDurability::Persistent,
                val: ScVal::ContractInstance(ScContractInstance { executable: exec, storage: None }),
            })
        };
        assert_eq!(wasm_hash_from_instance(&entry(ContractExecutable::Wasm(Hash([7; 32])))), Some([7; 32]));
        assert_eq!(wasm_hash_from_instance(&entry(ContractExecutable::StellarAsset)), None);
    }

    #[test]
    fn instance_key_requires_contract_strkey() {
        assert!(instance_key("CA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJUWDA").is_some());
        assert!(instance_key("GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN").is_none());
    }
}
