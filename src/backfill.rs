//! Historical backfill from the Stellar ledger data lake (Galexie), issue #1252.
//!
//! Galexie exports one zstd-compressed XDR `LedgerCloseMetaBatch` per file to
//! object storage, described by a `.config.json` manifest at the datastore
//! root. This module plans ledger-range work, downloads and decodes the
//! files in parallel, converts contract events into the same shape the RPC
//! `getEvents` returns, and hands them to an [`EventSink`] (the indexer's own
//! `ON CONFLICT DO NOTHING` insert path), so backfill can run alongside live
//! indexing without producing duplicates. Progress is checkpointed per range
//! in `backfill_checkpoints`, so an interrupted run resumes where it stopped.

use chrono::{DateTime, SecondsFormat};
use serde::Deserialize;
use sqlx::PgPool;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use stellar_xdr::curr::{
    ContractEvent, ContractEventBody, ContractEventType, LedgerCloseMeta, LedgerCloseMetaBatch, Limits, ReadXdr,
    TransactionEventStage, TransactionMeta, TransactionResultResult, WriteXdr,
};

use crate::models::SorobanEvent;

/// SDF's public pubnet data lake (AWS Open Data), used when no bucket is set
/// and the indexer runs against mainnet.
pub const DEFAULT_PUBNET_BUCKET: &str =
    "https://aws-public-blockchain.s3.us-east-2.amazonaws.com/v1.1/stellar/ledgers/pubnet";

/// Layout of a Galexie datastore, read from its `.config.json` manifest.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DatastoreSchema {
    pub ledgers_per_batch: u32,
    pub batches_per_partition: u32,
    #[serde(default = "default_compression")]
    pub compression: String,
}

fn default_compression() -> String {
    "zstd".into()
}

/// Where backfilled events go. Implemented by the indexer so backfill uses
/// exactly the live insert path (dedup, enrichment, deployment tracking).
#[async_trait::async_trait]
pub trait EventSink: Send + Sync {
    /// Store events, returning how many rows were newly inserted.
    async fn store_events(&self, events: &[SorobanEvent], schema_version: i32) -> Result<u64, String>;
}

/// Configuration for a backfill run.
#[derive(Debug, Clone)]
pub struct BackfillConfig {
    /// Base URL of the datastore (bucket URL plus key prefix).
    pub bucket_url: String,
    pub start_ledger: u32,
    pub end_ledger: u32,
    /// Number of ledgers per parallel worker range.
    pub chunk_size: u32,
    pub workers: usize,
    /// Download attempts per file before the range fails.
    pub retries: u32,
    /// Explicit layout; `None` reads the datastore's `.config.json`.
    pub schema: Option<DatastoreSchema>,
}

impl BackfillConfig {
    /// Build the config for `start..=end` from `BACKFILL_*` environment
    /// variables. Without `BACKFILL_BUCKET_URL`, mainnet uses SDF's public
    /// data lake; other networks must set it.
    pub fn from_env(start_ledger: u32, end_ledger: u32, chain_id: &str) -> Result<Self, String> {
        let num = |k: &str, d: u32| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
        let mut bucket_url = match std::env::var("BACKFILL_BUCKET_URL") {
            Ok(url) if !url.is_empty() => url.trim_end_matches('/').to_string(),
            _ if matches!(chain_id, "mainnet" | "pubnet" | "soroban-mainnet") => DEFAULT_PUBNET_BUCKET.to_string(),
            _ => return Err(format!("BACKFILL_BUCKET_URL is required for chain_id {chain_id:?}")),
        };
        let prefix = std::env::var("BACKFILL_PREFIX").unwrap_or_default();
        let prefix = prefix.trim_matches('/');
        if !prefix.is_empty() {
            bucket_url = format!("{bucket_url}/{prefix}");
        }
        let schema = std::env::var("BACKFILL_LEDGERS_PER_FILE").ok().and_then(|v| v.parse().ok()).map(|per_file: u32| {
            DatastoreSchema {
                ledgers_per_batch: per_file.max(1),
                batches_per_partition: num("BACKFILL_FILES_PER_PARTITION", 64_000).max(1),
                compression: std::env::var("BACKFILL_COMPRESSION").unwrap_or_else(|_| default_compression()),
            }
        });
        if end_ledger < start_ledger {
            return Err("--backfill-to must be >= --backfill-from".into());
        }
        Ok(Self {
            bucket_url,
            start_ledger,
            end_ledger,
            chunk_size: num("BACKFILL_CHUNK_SIZE", 1000).max(1),
            workers: num("BACKFILL_WORKERS", 4).max(1) as usize,
            retries: num("BACKFILL_RETRIES", 3).max(1),
            schema,
        })
    }
}

/// Parse `--backfill-from <ledger> --backfill-to <ledger>` (also accepts
/// `--flag=value`). `Ok(None)` when neither flag is present.
pub fn parse_cli_range<I: IntoIterator<Item = String>>(args: I) -> Result<Option<(u32, u32)>, String> {
    let (mut from, mut to) = (None, None);
    let mut it = args.into_iter();
    while let Some(arg) = it.next() {
        let (flag, inline) = match arg.split_once('=') {
            Some((f, v)) => (f.to_string(), Some(v.to_string())),
            None => (arg, None),
        };
        let slot = match flag.as_str() {
            "--backfill-from" => &mut from,
            "--backfill-to" => &mut to,
            _ => continue,
        };
        let raw = inline.or_else(|| it.next()).ok_or_else(|| format!("{flag} requires a ledger number"))?;
        *slot = Some(raw.parse::<u32>().map_err(|e| format!("{flag}: invalid ledger {raw:?}: {e}"))?);
    }
    match (from, to) {
        (None, None) => Ok(None),
        (Some(f), Some(t)) if f <= t => Ok(Some((f, t))),
        (Some(_), Some(_)) => Err("--backfill-from must be <= --backfill-to".into()),
        _ => Err("--backfill-from and --backfill-to must be given together".into()),
    }
}

/// Split an inclusive range into chunks of at most `chunk` ledgers.
pub fn split_range(start: u32, end: u32, chunk: u32) -> Vec<(u32, u32)> {
    let chunk = chunk.max(1);
    let mut out = Vec::new();
    let mut s = start;
    while s <= end {
        let e = s.saturating_add(chunk - 1).min(end);
        out.push((s, e));
        if e == u32::MAX {
            break;
        }
        s = e + 1;
    }
    out
}

/// Inclusive ledger bounds of the file containing `seq`.
pub fn file_bounds(schema: &DatastoreSchema, seq: u32) -> (u32, u32) {
    let per_file = schema.ledgers_per_batch.max(1);
    let start = seq - seq % per_file;
    (start, start.saturating_add(per_file - 1))
}

/// Galexie object key (relative to the datastore root) for the file
/// containing `seq`. Partition directories and file names lead with the
/// bit-inverted start ledger so the newest objects sort first.
pub fn object_key(schema: &DatastoreSchema, seq: u32) -> String {
    let mut key = String::new();
    if schema.batches_per_partition > 1 {
        let size = u64::from(schema.ledgers_per_batch.max(1)) * u64::from(schema.batches_per_partition);
        let start = u64::from(seq) / size * size;
        let end = (start + size - 1).min(u64::from(u32::MAX));
        key.push_str(&format!("{:08X}--{start}-{end}/", u64::from(u32::MAX) - start));
    }
    let (start, end) = file_bounds(schema, seq);
    key.push_str(&format!("{:08X}--{start}", u32::MAX - start));
    if end != start {
        key.push_str(&format!("-{end}"));
    }
    key.push_str(if schema.compression == "zstd" { ".xdr.zst" } else { ".xdr" });
    key
}

/// Load where a range should resume; returns `None` when already completed.
pub async fn load_resume_point(pool: &PgPool, start: u32, end: u32) -> Result<Option<u64>, sqlx::Error> {
    let row: Option<(i64, bool)> = sqlx::query_as(
        "SELECT next_ledger, completed FROM backfill_checkpoints WHERE range_start = $1 AND range_end = $2",
    )
    .bind(i64::from(start))
    .bind(i64::from(end))
    .fetch_optional(pool)
    .await?;
    Ok(match row {
        Some((_, true)) => None,
        Some((n, false)) => Some(u64::try_from(n).unwrap_or(u64::from(start))),
        None => Some(u64::from(start)),
    })
}

pub async fn save_checkpoint(pool: &PgPool, start: u32, end: u32, next: u64) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO backfill_checkpoints (range_start, range_end, next_ledger, completed, updated_at)
         VALUES ($1, $2, $3, $4, NOW())
         ON CONFLICT (range_start, range_end)
         DO UPDATE SET next_ledger = EXCLUDED.next_ledger, completed = EXCLUDED.completed, updated_at = NOW()",
    )
    .bind(i64::from(start))
    .bind(i64::from(end))
    .bind(i64::try_from(next).unwrap_or(i64::MAX))
    .bind(next > u64::from(end))
    .execute(pool)
    .await?;
    Ok(())
}

/// Read the datastore's `.config.json` manifest.
pub async fn load_manifest(client: &reqwest::Client, bucket_url: &str) -> Result<DatastoreSchema, String> {
    let url = format!("{bucket_url}/.config.json");
    let resp = client.get(&url).send().await.map_err(|e| format!("GET {url}: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!(
            "GET {url} returned {}; set BACKFILL_LEDGERS_PER_FILE (and BACKFILL_FILES_PER_PARTITION) for datastores without a manifest",
            resp.status()
        ));
    }
    resp.json().await.map_err(|e| format!("invalid datastore manifest {url}: {e}"))
}

async fn fetch_file(client: &reqwest::Client, url: &str, retries: u32) -> Result<Vec<u8>, String> {
    let mut last = String::new();
    for attempt in 0..retries {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_secs(1 << attempt.min(5))).await;
        }
        match client.get(url).send().await {
            Ok(r) if r.status().is_success() => match r.bytes().await {
                Ok(b) => return Ok(b.to_vec()),
                Err(e) => last = e.to_string(),
            },
            Ok(r) if r.status() == reqwest::StatusCode::NOT_FOUND => {
                return Err(format!("GET {url} returned 404 (ledger not in the data lake yet?)"));
            }
            Ok(r) => last = format!("status {}", r.status()),
            Err(e) => last = e.to_string(),
        }
    }
    Err(format!("GET {url} failed after {retries} attempts: {last}"))
}

/// Decompress and decode one Galexie file.
pub fn decode_batch(raw: &[u8], compression: &str) -> Result<LedgerCloseMetaBatch, String> {
    let xdr = if compression == "zstd" {
        zstd::decode_all(raw).map_err(|e| format!("zstd: {e}"))?
    } else {
        raw.to_vec()
    };
    LedgerCloseMetaBatch::from_xdr(&xdr, Limits { depth: 1000, len: xdr.len() }).map_err(|e| format!("xdr: {e}"))
}

pub fn ledger_seq(meta: &LedgerCloseMeta) -> u32 {
    match meta {
        LedgerCloseMeta::V0(m) => m.ledger_header.header.ledger_seq,
        LedgerCloseMeta::V1(m) => m.ledger_header.header.ledger_seq,
        LedgerCloseMeta::V2(m) => m.ledger_header.header.ledger_seq,
    }
}

/// Protocol version of the ledger (stored as the events' `schema_version`).
pub fn protocol_version(meta: &LedgerCloseMeta) -> u32 {
    match meta {
        LedgerCloseMeta::V0(m) => m.ledger_header.header.ledger_version,
        LedgerCloseMeta::V1(m) => m.ledger_header.header.ledger_version,
        LedgerCloseMeta::V2(m) => m.ledger_header.header.ledger_version,
    }
}

fn b64(v: &impl WriteXdr) -> serde_json::Value {
    serde_json::Value::String(v.to_xdr_base64(Limits::none()).unwrap_or_default())
}

/// Contract and system events of one ledger, shaped like RPC `getEvents`
/// results (base64 XDR topics/values, TOID ids) so stored rows match what
/// live indexing writes. Operation events use `toid(ledger, tx, op)`.
/// Transaction-level events (protocol 23+ fee events) follow the RPC's
/// numbering: `BeforeAllTxs` under tx slot 0 and `AfterAllTxs` under slot
/// 0xFFFFF, each with a ledger-wide running index. Diagnostic events are
/// skipped, as the live indexer never receives them.
pub fn events_from_ledger(meta: &LedgerCloseMeta) -> Vec<SorobanEvent> {
    let (header, txs): (_, Vec<_>) = match meta {
        LedgerCloseMeta::V0(_) => return Vec::new(), // pre-Soroban ledgers
        LedgerCloseMeta::V1(m) => {
            (&m.ledger_header, m.tx_processing.iter().map(|t| (&t.result, &t.tx_apply_processing)).collect())
        }
        LedgerCloseMeta::V2(m) => {
            (&m.ledger_header, m.tx_processing.iter().map(|t| (&t.result, &t.tx_apply_processing)).collect())
        }
    };
    let seq = header.header.ledger_seq;
    let closed_at = i64::try_from(header.header.scp_value.close_time.0)
        .ok()
        .and_then(|t| DateTime::from_timestamp(t, 0))
        .map(|t| t.to_rfc3339_opts(SecondsFormat::Secs, true))
        .unwrap_or_default();
    let ledger_hash = hex::encode(header.hash.0);
    let toid = |tx_slot: u64, op: usize| (u64::from(seq) << 32) | ((tx_slot & 0xF_FFFF) << 12) | (op as u64 & 0xFFF);

    let mut out = Vec::new();
    let (mut before_all, mut after_all) = (0usize, 0usize);
    for (i, (result, tx_meta)) in txs.into_iter().enumerate() {
        let tx_hash = hex::encode(result.transaction_hash.0);
        let success = matches!(
            result.result.result,
            TransactionResultResult::TxSuccess(_) | TransactionResultResult::TxFeeBumpInnerSuccess(_)
        );
        let mut push = |ev: &ContractEvent, id: u64, k: usize| {
            let Some(contract_id) = &ev.contract_id else { return };
            let kind = match ev.type_ {
                ContractEventType::Contract => "contract",
                ContractEventType::System => "system",
                ContractEventType::Diagnostic => return,
            };
            let ContractEventBody::V0(body) = &ev.body;
            out.push(SorobanEvent {
                rpc_id: Some(format!("{id:019}-{k:010}")),
                contract_id: contract_id.to_string(),
                event_type: kind.to_string(),
                tx_hash: tx_hash.clone(),
                ledger: u64::from(seq),
                ledger_closed_at: closed_at.clone(),
                ledger_hash: Some(ledger_hash.clone()),
                in_successful_call: success,
                value: b64(&body.data),
                topic: Some(body.topics.iter().map(b64).collect()),
                ..Default::default()
            });
        };
        // Application order is 1-based in the TOID.
        let tx_order = i as u64 + 1;
        match tx_meta {
            TransactionMeta::V3(m) => {
                for (k, ev) in m.soroban_meta.iter().flat_map(|s| s.events.iter()).enumerate() {
                    push(ev, toid(tx_order, 0), k);
                }
            }
            TransactionMeta::V4(m) => {
                for (op, op_meta) in m.operations.iter().enumerate() {
                    for (k, ev) in op_meta.events.iter().enumerate() {
                        push(ev, toid(tx_order, op), k);
                    }
                }
                // `AfterTx` is not emitted by current protocols; number it
                // after the first operation's events so ids stay unique.
                let mut after_tx = m.operations.first().map_or(0, |o| o.events.len());
                for te in m.events.iter() {
                    match te.stage {
                        TransactionEventStage::BeforeAllTxs => {
                            push(&te.event, toid(0, 0), before_all);
                            before_all += 1;
                        }
                        TransactionEventStage::AfterAllTxs => {
                            push(&te.event, toid(0xF_FFFF, 0), after_all);
                            after_all += 1;
                        }
                        TransactionEventStage::AfterTx => {
                            push(&te.event, toid(tx_order, 0), after_tx);
                            after_tx += 1;
                        }
                    }
                }
            }
            _ => {}
        }
    }
    out
}

struct Progress {
    ledgers: AtomicU64,
    events: AtomicU64,
    highest: AtomicU64,
}

/// Process one range, checkpointing after every file.
async fn run_range(
    pool: &PgPool,
    sink: &dyn EventSink,
    client: &reqwest::Client,
    cfg: &BackfillConfig,
    schema: &DatastoreSchema,
    (start, end): (u32, u32),
    progress: &Progress,
) -> Result<(), String> {
    let Some(mut next) = load_resume_point(pool, start, end).await.map_err(|e| e.to_string())? else {
        return Ok(()); // completed by an earlier run
    };
    while next <= u64::from(end) {
        let seq = u32::try_from(next).map_err(|e| e.to_string())?;
        let url = format!("{}/{}", cfg.bucket_url, object_key(schema, seq));
        let batch = decode_batch(&fetch_file(client, &url, cfg.retries).await?, &schema.compression)?;
        for meta in batch.ledger_close_metas.iter() {
            let ledger = ledger_seq(meta);
            if u64::from(ledger) < next || ledger > end {
                continue; // a multi-ledger file can straddle the range
            }
            let events = events_from_ledger(meta);
            let version = i32::try_from(protocol_version(meta)).unwrap_or(i32::MAX);
            let stored = sink.store_events(&events, version).await?;
            progress.events.fetch_add(stored, Ordering::Relaxed);
            crate::metrics::record_backfill_events(stored);
        }
        let done_through = file_bounds(schema, seq).1.min(end);
        let processed = u64::from(done_through) + 1 - next;
        next = u64::from(done_through) + 1;
        save_checkpoint(pool, start, end, next).await.map_err(|e| e.to_string())?;
        progress.ledgers.fetch_add(processed, Ordering::Relaxed);
        let highest = progress.highest.fetch_max(u64::from(done_through), Ordering::Relaxed).max(u64::from(done_through));
        crate::metrics::record_backfill_ledgers(processed);
        crate::metrics::update_backfill_progress_ledger(highest);
    }
    Ok(())
}

/// Totals of a completed backfill run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackfillSummary {
    pub ledgers: u64,
    pub events_inserted: u64,
}

/// Run all ranges with bounded parallelism. Every range runs to completion
/// or failure (so successful ranges keep their checkpoints); the first
/// error is returned.
pub async fn run(pool: PgPool, sink: Arc<dyn EventSink>, cfg: BackfillConfig) -> Result<BackfillSummary, String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .map_err(|e| e.to_string())?;
    let schema = match cfg.schema.clone() {
        Some(s) => s,
        None => load_manifest(&client, &cfg.bucket_url).await?,
    };
    tracing::info!(
        bucket = %cfg.bucket_url, from = cfg.start_ledger, to = cfg.end_ledger,
        ledgers_per_file = schema.ledgers_per_batch, workers = cfg.workers, "starting Galexie backfill"
    );
    let progress = Arc::new(Progress { ledgers: AtomicU64::new(0), events: AtomicU64::new(0), highest: AtomicU64::new(0) });
    let sem = Arc::new(tokio::sync::Semaphore::new(cfg.workers));
    let mut handles = Vec::new();
    for range in split_range(cfg.start_ledger, cfg.end_ledger, cfg.chunk_size) {
        let (pool, sink, client, cfg, schema, sem, progress) =
            (pool.clone(), sink.clone(), client.clone(), cfg.clone(), schema.clone(), sem.clone(), progress.clone());
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.map_err(|e| e.to_string())?;
            run_range(&pool, sink.as_ref(), &client, &cfg, &schema, range, &progress)
                .await
                .map_err(|e| format!("range {}-{}: {e}", range.0, range.1))
        }));
    }
    let mut first_err = None;
    for h in handles {
        let res = h.await.map_err(|e| e.to_string()).and_then(|r| r);
        if let Err(e) = res {
            tracing::error!(error = %e, "backfill range failed");
            first_err.get_or_insert(e);
        }
    }
    let summary = BackfillSummary {
        ledgers: progress.ledgers.load(Ordering::Relaxed),
        events_inserted: progress.events.load(Ordering::Relaxed),
    };
    match first_err {
        Some(e) => Err(e),
        None => Ok(summary),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn galexie() -> DatastoreSchema {
        DatastoreSchema { ledgers_per_batch: 1, batches_per_partition: 64_000, compression: "zstd".into() }
    }

    fn args(s: &str) -> Vec<String> {
        s.split_whitespace().map(str::to_owned).collect()
    }

    #[test]
    fn splits_ranges() {
        assert_eq!(split_range(1, 10, 4), vec![(1, 4), (5, 8), (9, 10)]);
        assert_eq!(split_range(5, 5, 100), vec![(5, 5)]);
    }

    #[test]
    fn builds_keys_matching_the_public_data_lake() {
        // Real object in s3://aws-public-blockchain/v1.1/stellar/ledgers/pubnet
        assert_eq!(object_key(&galexie(), 64_995_374), "FC20C9FF--64960000-65023999/FC203FD1--64995374.xdr.zst");
        let flat = DatastoreSchema { ledgers_per_batch: 64, batches_per_partition: 1, compression: "zstd".into() };
        assert_eq!(object_key(&flat, 70), "FFFFFFBF--64-127.xdr.zst");
        assert_eq!(file_bounds(&flat, 70), (64, 127));
    }

    #[test]
    fn parses_manifest() {
        let m: DatastoreSchema = serde_json::from_str(
            r#"{"networkPassphrase":"Public Global Stellar Network ; September 2015","version":"1.0","compression":"zstd","ledgersPerBatch":1,"batchesPerPartition":64000}"#,
        )
        .unwrap();
        assert_eq!(m, galexie());
    }

    #[test]
    fn parses_cli_flags() {
        assert_eq!(parse_cli_range(args("serve")).unwrap(), None);
        assert_eq!(parse_cli_range(args("--backfill-from 100 --backfill-to 200")).unwrap(), Some((100, 200)));
        assert_eq!(parse_cli_range(args("--backfill-to=9 --backfill-from=1")).unwrap(), Some((1, 9)));
        assert!(parse_cli_range(args("--backfill-from 5")).is_err());
        assert!(parse_cli_range(args("--backfill-from 9 --backfill-to 1")).is_err());
        assert!(parse_cli_range(args("--backfill-from x --backfill-to 1")).is_err());
        assert!(parse_cli_range(args("--backfill-from")).is_err());
    }

    /// Decodes a real pubnet file when `GALEXIE_SAMPLE` points at one, e.g.
    /// `FC20C9FF--64960000-65023999/FC203FD1--64995374.xdr.zst`.
    #[test]
    fn decodes_real_galexie_file_when_available() {
        let Ok(path) = std::env::var("GALEXIE_SAMPLE") else { return };
        let batch = decode_batch(&std::fs::read(path).unwrap(), "zstd").unwrap();
        let meta = &batch.ledger_close_metas[0];
        let events = events_from_ledger(meta);
        assert!(!events.is_empty());
        for e in &events {
            assert_eq!(e.ledger, u64::from(ledger_seq(meta)));
            assert!(e.contract_id.starts_with('C') && e.contract_id.len() == 56);
            assert_eq!(e.tx_hash.len(), 64);
            assert!(e.ledger_closed_at.ends_with('Z'));
            let ord = crate::toid::parse_event_id(e.rpc_id.as_deref().unwrap()).unwrap();
            assert_eq!(ord.ledger, ledger_seq(meta));
            assert!(ord.tx_index >= 1);
            assert!(crate::scval_format::parse_stored(&e.value).is_some());
        }
    }
}
