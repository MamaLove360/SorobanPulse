use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use std::time::Duration;

use crate::client::ApiClient;

pub struct ReplayStatus {
    pub status: String,
    pub progress: f64,
    pub ledger: i64,
    pub total_ledgers: i64,
}

pub fn run(client: &ApiClient, from_ledger: i64, to_ledger: Option<i64>) -> Result<()> {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template("{spinner:.cyan} {msg} [{bar:40.cyan/blue}]")
            .context("invalid progress style")?
            .progress_chars("=>-"),
    );

    let request = serde_json::json!({
        "from_ledger": from_ledger,
        "to_ledger": to_ledger,
    });

    let response: serde_json::Value = client.post("/v1/replay", &request)?;
    let replay_id = response["id"]
        .as_str()
        .context("missing replay id in response")?;

    pb.set_message(format!("Replay {} started", replay_id));

    loop {
        std::thread::sleep(Duration::from_millis(500));

        let status_response: serde_json::Value =
            client.get(&format!("/v1/replay/{}", replay_id), &[])?;

        let status = status_response["status"].as_str().unwrap_or("unknown");
        let progress = status_response["progress"].as_f64().unwrap_or(0.0);
        let ledger = status_response["ledger"].as_i64().unwrap_or(0);
        let total_ledgers = status_response["total_ledgers"].as_i64().unwrap_or(1);

        let pct = (progress * 100.0) as u64;
        pb.set_position((pct).min(99));
        pb.set_message(format!(
            "Replaying ledgers {} → {} ({pct}%)",
            ledger, total_ledgers
        ));

        if status == "completed" || status == "failed" {
            pb.finish_with_message(format!("Replay {}: {}", replay_id, status));
            break;
        }
    }

    Ok(())
}
