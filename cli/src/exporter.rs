use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

use crate::{
    client::ApiClient,
    query::{EventQuery, EventsResponse, SorobanEvent},
};

// ---------------------------------------------------------------------------
// Export format
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, clap::ValueEnum)]
pub enum ExportFormat {
    Json,
    Csv,
    Jsonl,
    Parquet,
}

impl std::fmt::Display for ExportFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Json    => write!(f, "json"),
            Self::Csv     => write!(f, "csv"),
            Self::Jsonl   => write!(f, "jsonl"),
            Self::Parquet => write!(f, "parquet"),
        }
    }
}

// ---------------------------------------------------------------------------
// Export to file — paginates automatically until all pages are fetched
// ---------------------------------------------------------------------------

pub fn export_events(
    client: &ApiClient,
    mut query: EventQuery,
    format: ExportFormat,
    output_path: &Path,
    max_records: Option<u64>,
) -> Result<u64> {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template("{spinner:.cyan} {msg}")
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
    );

    query.page = 1;
    query.limit = 100; // larger pages for export
    let mut total_written: u64 = 0;
    let mut all_events = Vec::new();

    // For parquet, collect all events first
    if format == ExportFormat::Parquet {
        loop {
            pb.set_message(format!("Fetching page {} ({total_written} records so far)…", query.page));
            pb.tick();

            let owned: Vec<(String, String)> = query.to_params();
            let borrowed: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();

            let resp: EventsResponse = client.get(&query.path(), &borrowed)?;
            let events = resp.into_events();

            if events.is_empty() {
                break;
            }

            let batch_size = events.len() as u64;
            all_events.extend(events);
            total_written += batch_size;

            if let Some(max) = max_records {
                if total_written >= max {
                    all_events.truncate(max as usize);
                    total_written = max;
                    break;
                }
            }

            if (batch_size as u32) < query.limit {
                break;
            }

            query.page += 1;
        }

        pb.finish_and_clear();
        write_parquet(&all_events, output_path)?;
        return Ok(total_written);
    }

    // For other formats, stream directly
    let file = File::create(output_path)
        .with_context(|| format!("creating output file '{}'", output_path.display()))?;
    let mut writer = BufWriter::new(file);

    // Write format header
    match format {
        ExportFormat::Json => writer.write_all(b"[\n")?,
        ExportFormat::Csv  => write_csv_header(&mut writer)?,
        ExportFormat::Jsonl => {}
        ExportFormat::Parquet => unreachable!(),
    }

    let mut first_record = true;

    loop {
        pb.set_message(format!("Fetching page {} ({total_written} records so far)…", query.page));
        pb.tick();

        let owned: Vec<(String, String)> = query.to_params();
        let borrowed: Vec<(&str, &str)> = owned.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();

        let resp: EventsResponse = client.get(&query.path(), &borrowed)?;
        let events = resp.into_events();

        if events.is_empty() {
            break;
        }

        let batch_size = events.len() as u64;

        // Write batch
        match format {
            ExportFormat::Json => {
                for e in &events {
                    if !first_record { writer.write_all(b",\n")?; }
                    let line = serde_json::to_string_pretty(e)?;
                    writer.write_all(line.as_bytes())?;
                    first_record = false;
                }
            }
            ExportFormat::Jsonl => {
                for e in &events {
                    let line = serde_json::to_string(e)?;
                    writer.write_all(line.as_bytes())?;
                    writer.write_all(b"\n")?;
                }
            }
            ExportFormat::Csv => write_csv_rows(&mut writer, &events)?,
            ExportFormat::Parquet => unreachable!(),
        }

        total_written += batch_size;

        if let Some(max) = max_records {
            if total_written >= max {
                break;
            }
        }

        // Stop if fewer results than the page size — last page reached
        if (batch_size as u32) < query.limit {
            break;
        }

        query.page += 1;
    }

    // Write format footer
    if format == ExportFormat::Json {
        writer.write_all(b"\n]\n")?;
    }

    writer.flush()?;
    pb.finish_and_clear();

    Ok(total_written)
}

// ---------------------------------------------------------------------------
// CSV helpers
// ---------------------------------------------------------------------------

fn write_csv_header(w: &mut impl Write) -> Result<()> {
    writeln!(w, "ledger,contract_id,event_type,tx_hash,ledger_closed_at,in_successful_call,value")?;
    Ok(())
}

fn write_csv_rows(w: &mut impl Write, events: &[SorobanEvent]) -> Result<()> {
    for e in events {
        let value = e.value.to_string().replace('"', "\"\"");
        writeln!(
            w,
            "{},{},{},{},{},{},\"{}\"",
            e.ledger,
            e.contract_id,
            e.event_type,
            e.tx_hash,
            e.ledger_closed_at,
            e.in_successful_call,
            value,
        )?;
    }
    Ok(())
}

fn write_parquet(events: &[SorobanEvent], output_path: &Path) -> Result<()> {
    let json_string = serde_json::to_string(events)?;
    let json_value: serde_json::Value = serde_json::from_str(&json_string)?;

    let file = File::create(output_path)
        .with_context(|| format!("creating parquet file '{}'", output_path.display()))?;

    parquet::serde::to_writer(file, &json_value)?;
    Ok(())
}
