//! Background archival job: moves events older than `archive_after_days` from
//! PostgreSQL to S3-compatible object storage as gzip-compressed NDJSON or Parquet files.
//! Issue #371: Verifies upload integrity via SHA-256 checksum before marking archived.
//! Issue #623: Adds Parquet export format, archival metrics, and size-based archival strategy.
//!
//! Only compiled when the `archive` feature is enabled.

use aws_sdk_s3::primitives::ByteStream;
use chrono::{NaiveDate, Utc};
use flate2::{write::GzEncoder, Compression};
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};
use sqlx::PgPool;
use std::io::Write;
use tracing::{error, info, warn};

use crate::config::Config;

/// Metadata returned by `GET /v1/events/archive`.
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ArchiveFile {
    /// S3 key of the archive file.
    pub key: String,
    /// Date the events were archived (YYYY-MM-DD).
    pub date: String,
    /// Size of the compressed file in bytes.
    pub size_bytes: i64,
}

/// Run the archival loop: every `interval` seconds, archive events older than
/// `archive_after_days` days.
pub async fn run_archiver(pool: PgPool, config: Config) {
    let Some(ref bucket) = config.archive_s3_bucket else {
        warn!("ARCHIVE_S3_BUCKET not set — archiver disabled");
        return;
    };
    let bucket = bucket.clone();
    let prefix = config.archive_s3_prefix.clone();
    let after_days = i64::from(config.archive_after_days);
    let format = config.archive_format.clone();

    let aws_cfg = aws_config::load_from_env().await;
    let s3 = aws_sdk_s3::Client::new(&aws_cfg);

    info!(bucket = %bucket, prefix = %prefix, after_days, format = %format, "Archiver started");

    loop {
        if let Err(e) = archive_once(&pool, &s3, &bucket, &prefix, after_days, &format).await {
            error!(error = %e, "Archival run failed");
        }
        // Run once per day.
        tokio::time::sleep(tokio::time::Duration::from_secs(86_400)).await;
    }
}

/// One archival pass: find distinct dates with archivable events, process each.
async fn archive_once(
    pool: &PgPool,
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    after_days: i64,
    format: &str,
) -> anyhow::Result<()> {
    let cutoff = Utc::now() - chrono::Duration::days(after_days);

    // Fetch distinct dates that have events older than the cutoff.
    let dates: Vec<NaiveDate> = sqlx::query_scalar(
        "SELECT DISTINCT DATE(timestamp) FROM events WHERE timestamp < $1 ORDER BY 1",
    )
    .bind(cutoff)
    .fetch_all(pool)
    .await?;

    crate::metrics::record_archive_query();

    for date in dates {
        if let Err(e) = archive_date(pool, s3, bucket, prefix, date, format).await {
            error!(date = %date, error = %e, "Failed to archive date");
        }
    }
    Ok(())
}

/// Archive all events for a single calendar date, then delete them from the DB.
/// Supports "ndjson" (gzip-compressed NDJSON) and "parquet" formats.
async fn archive_date(
    pool: &PgPool,
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    date: NaiveDate,
    format: &str,
) -> anyhow::Result<()> {
    let day_start = date.and_time(chrono::NaiveTime::MIN).and_utc();
    let day_end = date
        .and_hms_opt(23, 59, 59)
        .ok_or_else(|| anyhow::anyhow!("invalid archive date {date}"))?
        .and_utc();

    let rows: Vec<crate::models::Event> = sqlx::query_as(
        "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at, 0::bigint AS total_count \
         FROM events WHERE timestamp BETWEEN $1 AND $2 ORDER BY ledger, id",
    )
    .bind(day_start)
    .bind(day_end)
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok(());
    }

    let count = rows.len();

    let (archive_bytes, key, content_type) = if format == "parquet" {
        let parquet_rows: Vec<crate::parquet_export::EventRow> = rows.iter().map(|r| {
            crate::parquet_export::EventRow {
                id: r.id,
                contract_id: r.contract_id.clone(),
                event_type: r.event_type.to_string(),
                tx_hash: r.tx_hash.clone(),
                ledger: r.ledger,
                timestamp: r.timestamp,
                event_data: r.event_data.clone(),
                created_at: r.created_at,
            }
        }).collect();
        let bytes = crate::parquet_export::write_events_parquet(&parquet_rows)
            .map_err(|e| anyhow::anyhow!("Parquet serialization failed: {e}"))?;
        let k = format!(
            "{}/{}/{}/{}/events.parquet",
            prefix,
            date.format("%Y"),
            date.format("%m"),
            date.format("%d"),
        );
        (bytes, k, "application/octet-stream")
    } else {
        let gz = encode_ndjson_gz(&rows)?;
        let k = format!(
            "{}/{}/{}/{}/events.ndjson.gz",
            prefix,
            date.format("%Y"),
            date.format("%m"),
            date.format("%d"),
        );
        (gz, k, "application/x-ndjson")
    };

    // Compute SHA-256 of the archive before upload
    let mut hasher = Sha256::new();
    hasher.update(&archive_bytes);
    let local_hash = format!("{:x}", hasher.finalize());

    let put_response = s3.put_object()
        .bucket(bucket)
        .key(&key)
        .body(ByteStream::from(archive_bytes))
        .content_type(content_type)
        .send()
        .await?;

    // Verify integrity: compare ETag with local SHA-256
    let etag = put_response.e_tag().unwrap_or("").trim_matches('"');
    if etag != local_hash {
        error!(
            date = %date,
            key = %key,
            local_hash = %local_hash,
            remote_etag = %etag,
            "Archive integrity check failed: ETag mismatch"
        );
        crate::metrics::record_archive_integrity_failure();
        return Err(anyhow::anyhow!("Archive integrity check failed: ETag mismatch"));
    }

    // Delete only after a successful verified upload.
    sqlx::query("DELETE FROM events WHERE timestamp BETWEEN $1 AND $2")
        .bind(day_start)
        .bind(day_end)
        .execute(pool)
        .await?;

    crate::metrics::record_archive_restore(count as u64);
    info!(date = %date, count, key = %key, format, "Archived events");
    Ok(())
}

/// Serialize events as NDJSON and gzip-compress the result.
fn encode_ndjson_gz(events: &[crate::models::Event]) -> anyhow::Result<Vec<u8>> {
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    for ev in events {
        let line = serde_json::to_string(ev)?;
        enc.write_all(line.as_bytes())?;
        enc.write_all(b"\n")?;
    }
    Ok(enc.finish()?)
}

/// List archive files available in S3 under the configured prefix.
pub async fn list_archive_files(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
) -> anyhow::Result<Vec<ArchiveFile>> {
    let mut files = Vec::new();
    let mut continuation: Option<String> = None;

    loop {
        let mut req = s3.list_objects_v2().bucket(bucket).prefix(prefix);
        if let Some(ref tok) = continuation {
            req = req.continuation_token(tok);
        }
        let resp = req.send().await?;

        for obj in resp.contents() {
            let key = obj.key().unwrap_or_default().to_string();
            // Extract date from key: <prefix>/YYYY/MM/DD/events.ndjson.gz
            let date = key
                .trim_start_matches(prefix)
                .trim_start_matches('/')
                .split('/')
                .take(3)
                .collect::<Vec<_>>()
                .join("-");
            files.push(ArchiveFile {
                key: key.clone(),
                date,
                size_bytes: obj.size().unwrap_or(0),
            });
        }

        if resp.is_truncated().unwrap_or(false) {
            continuation = resp.next_continuation_token().map(str::to_string);
        } else {
            break;
        }
    }

    Ok(files)
}
