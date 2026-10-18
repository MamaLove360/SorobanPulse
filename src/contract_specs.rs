//! Auto-fetch contract specs (`contractspecv0`) from on-chain WASM (issue #1057).
//!
//! Enabled with `AUTO_FETCH_CONTRACT_SPECS=true`. For each contract that has no
//! entry in `contract_abis`, the worker resolves the instance's WASM hash with
//! `getLedgerEntries`, downloads the `ContractCode` entry, extracts the
//! `contractspecv0` custom section and caches it by WASM hash.

use base64::{engine::general_purpose::STANDARD as B64, Engine};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::time::Duration;

pub fn enabled() -> bool {
    std::env::var("AUTO_FETCH_CONTRACT_SPECS").map(|v| v == "true" || v == "1").unwrap_or(false)
}

fn b32_decode(s: &str) -> Option<Vec<u8>> {
    let (mut buf, mut bits, mut out) = (0u32, 0u32, Vec::new());
    for c in s.bytes() {
        let v = match c { b'A'..=b'Z' => c - b'A', b'2'..=b'7' => c - b'2' + 26, _ => return None } as u32;
        buf = (buf << 5) | v;
        bits += 5;
        if bits >= 8 { bits -= 8; out.push((buf >> bits) as u8); buf &= (1 << bits) - 1; }
    }
    Some(out)
}

/// Decode a `C...` strkey to its 32-byte contract hash.
pub fn contract_hash(contract_id: &str) -> Option<[u8; 32]> {
    let raw = b32_decode(contract_id)?;
    if raw.len() != 35 || raw[0] != 2 << 3 { return None; }
    raw[1..33].try_into().ok()
}

fn key_b64(contract: [u8; 32]) -> String {
    // LedgerKey::ContractData{ contract: ScAddress::Contract, key: LedgerKeyContractInstance, durability: Persistent }
    let mut b = Vec::new();
    for w in [6u32, 1] { b.extend(w.to_be_bytes()); }
    b.extend(contract);
    for w in [20u32, 1] { b.extend(w.to_be_bytes()); }
    B64.encode(b)
}

fn code_key_b64(hash: &[u8]) -> String {
    let mut b = 7u32.to_be_bytes().to_vec();
    b.extend(hash);
    B64.encode(b)
}

/// Extract the WASM hash from a ContractData instance entry (`None` for SAC / non-WASM executables).
pub fn wasm_hash_from_instance(entry: &[u8]) -> Option<[u8; 32]> {
    let u = |o: usize| entry.get(o..o + 4).map(|b| u32::from_be_bytes(b.try_into().unwrap()));
    // entry type(6), ext, address(type,hash), key type, durability, val type(19), executable type(0=wasm)
    if u(0)? != 6 || u(60 - 4)? != 19 || u(60)? != 0 { return None; }
    entry.get(64..96)?.try_into().ok()
}

fn leb(b: &[u8], p: &mut usize) -> Option<usize> {
    let (mut r, mut shift) = (0usize, 0);
    loop {
        let byte = *b.get(*p)?; *p += 1;
        r |= ((byte & 0x7f) as usize) << shift;
        if byte & 0x80 == 0 { return Some(r); }
        shift += 7;
        if shift > 35 { return None; }
    }
}

/// Return the payloads of all custom sections named `name`, concatenated.
pub fn custom_section(wasm: &[u8], name: &str) -> Option<Vec<u8>> {
    if wasm.get(0..4)? != b"\0asm" { return None; }
    let (mut p, mut out, mut found) = (8usize, Vec::new(), false);
    while p < wasm.len() {
        let id = wasm[p]; p += 1;
        let size = leb(wasm, &mut p)?;
        let end = p.checked_add(size)?;
        if end > wasm.len() { break; }
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

/// Build the cached ABI value from the raw `contractspecv0` bytes. The value
/// is a JSON array (the `contract_abis` shape) holding the XDR stream of
/// `ScSpecEntry` values, base64-encoded.
pub fn spec_to_abi(spec: &[u8], wasm_hash: &str) -> Value {
    json!([{ "name": "contractspecv0", "wasm_hash": wasm_hash, "xdr_base64": B64.encode(spec) }])
}

/// Locate the WASM inside a ContractCode ledger entry (after the header).
pub fn wasm_from_code_entry(entry: &[u8]) -> Option<&[u8]> {
    let i = entry.windows(8).position(|w| w == b"\0asm\x01\0\0\0")?;
    Some(&entry[i..])
}

async fn get_entry(client: &reqwest::Client, url: &str, key: String) -> Option<Vec<u8>> {
    let body = json!({"jsonrpc":"2.0","id":1,"method":"getLedgerEntries","params":{"keys":[key]}});
    let v: Value = client.post(url).json(&body).send().await.ok()?.json().await.ok()?;
    let xdr = v.pointer("/result/entries/0/xdr")?.as_str()?;
    B64.decode(xdr).ok()
}

/// Current WASM hash (hex) of a contract, or `None` for SAC / unknown.
pub async fn fetch_wasm_hash(client: &reqwest::Client, url: &str, contract_id: &str) -> Option<String> {
    let entry = get_entry(client, url, key_b64(contract_hash(contract_id)?)).await?;
    wasm_hash_from_instance(&entry).map(hex::encode)
}

/// Fetch and cache the spec for `wasm_hash`, then link it to `contract_id`.
/// Returns true if a spec is now available for the contract.
pub async fn store_spec_for(pool: &PgPool, client: &reqwest::Client, url: &str, contract_id: &str, wasm_hash: &str) -> bool {
    let cached: Option<Value> = sqlx::query_scalar("SELECT spec FROM contract_spec_cache WHERE wasm_hash = $1")
        .bind(wasm_hash).fetch_optional(pool).await.ok().flatten();
    let spec = match cached {
        Some(s) => s,
        None => {
            let Ok(h) = hex::decode(wasm_hash) else { return false };
            let Some(entry) = get_entry(client, url, code_key_b64(&h)).await else { return false };
            let Some(wasm) = wasm_from_code_entry(&entry) else { return false };
            let Some(sec) = custom_section(wasm, "contractspecv0") else { return false };
            let s = spec_to_abi(&sec, wasm_hash);
            let _ = sqlx::query("INSERT INTO contract_spec_cache (wasm_hash, spec) VALUES ($1,$2) ON CONFLICT DO NOTHING")
                .bind(wasm_hash).bind(&s).execute(pool).await;
            s
        }
    };
    let abi_hash = hex::encode(Sha256::digest(spec.to_string()));
    sqlx::query(
        "INSERT INTO contract_abis (contract_id, abi, abi_hash, fetched_at, is_valid) VALUES ($1,$2,$3,NOW(),true)
         ON CONFLICT (contract_id) DO UPDATE SET abi = EXCLUDED.abi, abi_hash = EXCLUDED.abi_hash, updated_at = NOW(), fetched_at = NOW()",
    )
    .bind(contract_id).bind(&spec).bind(abi_hash).execute(pool).await.is_ok()
}

/// One pass over contracts lacking an ABI, rate limited to one RPC round trip per `interval`.
pub async fn run_once(pool: &PgPool, client: &reqwest::Client, url: &str, interval: Duration, batch: i64) {
    let ids: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT e.contract_id FROM events e
         WHERE NOT EXISTS (SELECT 1 FROM contract_abis a WHERE a.contract_id = e.contract_id)
           AND NOT EXISTS (SELECT 1 FROM contract_spec_attempts t WHERE t.contract_id = e.contract_id AND t.attempted_at > NOW() - INTERVAL '1 day')
         LIMIT $1",
    ).bind(batch).fetch_all(pool).await.unwrap_or_default();
    for id in ids {
        tokio::time::sleep(interval).await;
        let status = match fetch_wasm_hash(client, url, &id).await {
            None => "no_wasm", // SAC or unresolved: nothing to fetch, fall back gracefully
            Some(h) => if store_spec_for(pool, client, url, &id, &h).await { "ok" } else { "no_spec" },
        };
        let _ = sqlx::query("INSERT INTO contract_spec_attempts (contract_id, status) VALUES ($1,$2) ON CONFLICT (contract_id) DO UPDATE SET status = $2, attempted_at = NOW()")
            .bind(&id).bind(status).execute(pool).await;
    }
}

/// Background worker; a no-op unless `AUTO_FETCH_CONTRACT_SPECS` is set.
pub async fn run_worker(pool: PgPool, rpc_url: String) {
    if !enabled() { return; }
    let client = reqwest::Client::new();
    loop {
        run_once(&pool, &client, &rpc_url, Duration::from_millis(500), 20).await;
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal WASM: header + custom section "contractspecv0" with payload [1,2,3].
    fn sample() -> Vec<u8> {
        let mut w = b"\0asm\x01\0\0\0".to_vec();
        let name = b"contractspecv0";
        let mut body = vec![name.len() as u8];
        body.extend(name); body.extend([1, 2, 3]);
        w.push(0); w.push(body.len() as u8); w.extend(body);
        w
    }

    #[test]
    fn parses_custom_section() {
        assert_eq!(custom_section(&sample(), "contractspecv0").unwrap(), vec![1, 2, 3]);
        assert!(custom_section(&sample(), "other").is_none());
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
    fn finds_wasm_in_code_entry() {
        let mut e = vec![0, 0, 0, 7, 0, 0, 0, 0];
        e.extend(sample());
        assert_eq!(wasm_from_code_entry(&e).unwrap(), &sample()[..]);
    }
    #[test]
    fn sac_instance_has_no_wasm_hash() {
        let mut e = vec![0u8; 96];
        e[3] = 6; e[59] = 19; e[63] = 1; // executable = StellarAsset
        assert!(wasm_hash_from_instance(&e).is_none());
        e[63] = 0;
        assert!(wasm_hash_from_instance(&e).is_some());
    }
    #[test]
    fn strkey_roundtrip_len() {
        let id = "CA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJUWDA";
        assert!(contract_hash(id).is_some());
    }
}
