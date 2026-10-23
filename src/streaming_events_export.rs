//! Streaming export handler for events (Issue #688 optimization).
//!
//! This module refactors the export handler to stream results instead of buffering them.
//! For NDJSON format, results are streamed directly as they come from the database.
//! For CSV format, we stream each row individually, maintaining the same RFC 4180 format.
//!
//! Peak memory usage is bounded by the streaming response buffer size (~8-16 KB)
//! regardless of result set size.

use crate::models::{EventType, ExportParams};
use crate::streaming_response::{
    create_negotiated_streaming_response, StreamingResultIterator, StreamCompression,
};
use axum::http::header;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{FromRow, PgPool};
use std::pin::Pin;
use futures::stream::{self, Stream, StreamExt};
use tracing::error;

/// Serializable event row for streaming export.
#[derive(Debug, Serialize, Deserialize, FromRow)]
pub struct StreamingEventRow {
    pub id: uuid::Uuid,
    pub contract_id: String,
    pub event_type: String,
    pub tx_hash: String,
    pub ledger: i64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub event_data: serde_json::Value,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Helper to escape CSV fields per RFC 4180.
fn csv_escape_field(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') || field.contains('\r')
    {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

/// Converts a streaming event to a CSV row.
fn event_to_csv_row(event: &StreamingEventRow, field_map: Option<&std::collections::HashMap<String, String>>) -> String {
    let default_cols = [
        "id",
        "contract_id",
        "event_type",
        "tx_hash",
        "ledger",
        "timestamp",
        "event_data",
        "created_at",
    ];

    let mut values: Vec<String> = Vec::with_capacity(default_cols.len());
    for col in &default_cols {
        match *col {
            "id" => values.push(csv_escape_field(&event.id.to_string())),
            "contract_id" => values.push(csv_escape_field(&event.contract_id)),
            "event_type" => values.push(csv_escape_field(&event.event_type)),
            "tx_hash" => values.push(csv_escape_field(&event.tx_hash)),
            "ledger" => values.push(event.ledger.to_string()),
            "timestamp" => values.push(csv_escape_field(&event.timestamp.to_rfc3339())),
            "event_data" => values.push(csv_escape_field(&event.event_data.to_string())),
            "created_at" => values.push(csv_escape_field(&event.created_at.to_rfc3339())),
            _ => {}
        }
    }
    format!("{}\n", values.join(","))
}

/// Builds a CSV header line.
pub fn build_csv_header(field_map: Option<&std::collections::HashMap<String, String>>) -> String {
    let default_cols = [
        "id",
        "contract_id",
        "event_type",
        "tx_hash",
        "ledger",
        "timestamp",
        "event_data",
        "created_at",
    ];

    let header_names: Vec<String> = default_cols
        .iter()
        .map(|c| {
            field_map
                .and_then(|m| m.get(*c))
                .cloned()
                .unwrap_or_else(|| (*c).to_string())
        })
        .collect();
    format!("{}\n", header_names.join(","))
}

/// Stream wrapper for CSV rows (serializes each row as newline-delimited CSV).
#[derive(Serialize)]
pub struct CsvStreamRow {
    #[serde(serialize_with = "serialize_csv_row")]
    pub row: String,
}

fn serialize_csv_row<S>(row: &str, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    // For CSV, we need to output the raw row data, not as a JSON string.
    // Since we're streaming NDJSON-formatted output where each line is JSON,
    // we'll encode the CSV row as a JSON string so it can be deserialize properly.
    serializer.serialize_str(row)
}

/// Wraps a streaming event row with a CSV header for the first row.
#[derive(Serialize)]
#[serde(untagged)]
pub enum ExportStreamItem {
    CsvHeader(String),
    Event(StreamingEventRow),
}

/// Builds a streaming query for events with the given parameters.
///
/// Returns a stream of event rows and the query SQL for reference.
pub async fn build_event_stream(
    pool: &PgPool,
    params: &ExportParams,
    max_rows: i64,
) -> Result<(StreamingResultIterator<StreamingEventRow>, i64), sqlx::Error> {
    let mut conditions: Vec<String> = Vec::new();
    let mut bind_idx: i32 = 1;

    if params.contract_id.is_some() {
        conditions.push(format!("contract_id = ${}", bind_idx));
        bind_idx += 1;
    }
    if params.event_type.is_some() {
        conditions.push(format!("event_type = ${}", bind_idx));
        bind_idx += 1;
    }
    if params.from_ledger.is_some() {
        conditions.push(format!("ledger >= ${}", bind_idx));
        bind_idx += 1;
    }
    if params.to_ledger.is_some() {
        conditions.push(format!("ledger <= ${}", bind_idx));
        bind_idx += 1;
    }
    if params.from_timestamp.is_some() {
        conditions.push(format!("timestamp >= ${}", bind_idx));
        bind_idx += 1;
    }
    if params.to_timestamp.is_some() {
        conditions.push(format!("timestamp <= ${}", bind_idx));
        bind_idx += 1;
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let query_str = format!(
        "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at \
         FROM events {where_clause} ORDER BY ledger ASC, id ASC LIMIT ${}"
        , bind_idx
    );

    let mut q = sqlx::query_as::<_, StreamingEventRow>(&query_str);

    if let Some(ref cid) = params.contract_id {
        q = q.bind(cid);
    }
    if let Some(ref et) = params.event_type {
        let et_str = format!("{:?}", et).to_lowercase(); // EventType enum to string
        q = q.bind(et_str);
    }
    if let Some(fl) = params.from_ledger {
        q = q.bind(fl);
    }
    if let Some(tl) = params.to_ledger {
        q = q.bind(tl);
    }
    if let Some(from_ts) = params.from_timestamp {
        q = q.bind(from_ts);
    }
    if let Some(to_ts) = params.to_timestamp {
        q = q.bind(to_ts);
    }
    q = q.bind(max_rows);

    // Get total count for reference (can be used for progress tracking)
    let count_str = format!("SELECT COUNT(*) FROM events {}", where_clause);
    let mut cq = sqlx::query_scalar::<_, i64>(&count_str);
    if let Some(ref cid) = params.contract_id {
        cq = cq.bind(cid);
    }
    if let Some(ref et) = params.event_type {
        let et_str = format!("{:?}", et).to_lowercase();
        cq = cq.bind(et_str);
    }
    if let Some(fl) = params.from_ledger {
        cq = cq.bind(fl);
    }
    if let Some(tl) = params.to_ledger {
        cq = cq.bind(tl);
    }
    if let Some(from_ts) = params.from_timestamp {
        cq = cq.bind(from_ts);
    }
    if let Some(to_ts) = params.from_timestamp {
        cq = cq.bind(to_ts);
    }
    let total_count = cq.fetch_one(pool).await?;

    // Create streaming iterator from the query
    let stream = q.fetch(pool);
    let boxed_stream: StreamingResultIterator<StreamingEventRow> =
        Box::pin(stream.map_ok(|r| r).map_err(|e| {
            error!("Database error in event stream: {}", e);
            e
        }));

    Ok((boxed_stream, total_count))
}

/// Creates a streaming NDJSON response for events.
pub async fn create_ndjson_stream(
    pool: &PgPool,
    params: &ExportParams,
    max_rows: i64,
    accept_encoding: &str,
) -> Result<(StreamingResultIterator<serde_json::Value>, i64), Box<dyn std::error::Error>> {
    let (event_stream, total_count) = build_event_stream(pool, params, max_rows).await?;

    // Map events to NDJSON format (each line is a JSON object)
    let ndjson_stream: StreamingResultIterator<serde_json::Value> = Box::pin(
        event_stream
            .map(|result| {
                result.map(|event| {
                    json!({
                        "id": event.id,
                        "contract_id": event.contract_id,
                        "event_type": event.event_type,
                        "tx_hash": event.tx_hash,
                        "ledger": event.ledger,
                        "timestamp": event.timestamp,
                        "event_data": event.event_data,
                        "created_at": event.created_at,
                    })
                })
            }),
    );

    Ok((ndjson_stream, total_count))
}

/// Creates a streaming CSV response for events.
///
/// This produces newline-delimited CSV output where each line is a complete CSV row.
pub async fn create_csv_stream(
    pool: &PgPool,
    params: &ExportParams,
    max_rows: i64,
    accept_encoding: &str,
    field_map: Option<&std::collections::HashMap<String, String>>,
) -> Result<(StreamingResultIterator<String>, i64), Box<dyn std::error::Error>> {
    let (event_stream, total_count) = build_event_stream(pool, params, max_rows).await?;

    // First emit the header line, then map events to CSV format
    let header = build_csv_header(field_map);
    let header_stream = stream::once(async move { Ok(header) });

    let csv_rows = event_stream.map(move |result| {
        result.map(|event| event_to_csv_row(&event, field_map))
    });

    let combined_stream: StreamingResultIterator<String> =
        Box::pin(header_stream.chain(csv_rows));

    Ok((combined_stream, total_count))
}
