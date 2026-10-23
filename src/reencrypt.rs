//! Issue #372: Background re-encryption job for key rotation.
//! Migrates events encrypted with old key to new key in batches.

use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::encryption;
use crate::metrics;

/// Default batch size for the rotation job (rows per transaction).
pub const ROTATION_BATCH_SIZE: usize = 100;

/// Shared state for re-encryption job
#[derive(Clone)]
pub struct ReencryptState {
    pub is_running: Arc<AtomicBool>,
    pub rows_remaining: Arc<AtomicU64>,
    /// Total number of rows to process for the current run.
    pub rows_total: Arc<AtomicU64>,
    /// Number of rows completed so far for the current run.
    pub rows_completed: Arc<AtomicU64>,
}

impl ReencryptState {
    pub fn new() -> Self {
        Self {
            is_running: Arc::new(AtomicBool::new(false)),
            rows_remaining: Arc::new(AtomicU64::new(0)),
            rows_total: Arc::new(AtomicU64::new(0)),
            rows_completed: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }

    pub fn set_running(&self, running: bool) {
        self.is_running.store(running, Ordering::Relaxed);
    }

    pub fn set_remaining(&self, count: u64) {
        self.rows_remaining.store(count, Ordering::Relaxed);
    }

    pub fn get_remaining(&self) -> u64 {
        self.rows_remaining.load(Ordering::Relaxed)
    }

    pub fn set_total(&self, count: u64) {
        self.rows_total.store(count, Ordering::Relaxed);
    }

    pub fn get_total(&self) -> u64 {
        self.rows_total.load(Ordering::Relaxed)
    }

    pub fn set_completed(&self, count: u64) {
        self.rows_completed.store(count, Ordering::Relaxed);
    }

    pub fn get_completed(&self) -> u64 {
        self.rows_completed.load(Ordering::Relaxed)
    }
}

/// Persisted checkpoint for a resumable rotation run.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct RotationCheckpoint {
    /// Last credential id successfully processed (resume point).
    pub last_processed_id: Option<uuid::Uuid>,
    /// Total rows to process for this run.
    pub total_rows: i64,
    /// Rows completed so far.
    pub completed_rows: i64,
    /// Whether the run has finished.
    pub finished: bool,
}

/// Start a background re-encryption job.
/// Returns immediately; the job runs in the background.
pub fn start_reencrypt_job(
    pool: PgPool,
    new_key: [u8; 32],
    old_key: [u8; 32],
    batch_size: usize,
    state: ReencryptState,
) {
    if state.is_running() {
        warn!("Re-encryption job already running");
        return;
    }

    state.set_running(true);

    tokio::spawn(async move {
        if let Err(e) = run_reencrypt_job(&pool, new_key, old_key, batch_size, &state).await {
            error!(error = %e, "Re-encryption job failed");
        }
        state.set_running(false);
    });
}

/// Run the re-encryption job: fetch rows encrypted with old key, re-encrypt with new key.
async fn run_reencrypt_job(
    pool: &PgPool,
    new_key: [u8; 32],
    old_key: [u8; 32],
    batch_size: usize,
    state: &ReencryptState,
) -> anyhow::Result<()> {
    // Count total rows to re-encrypt
    let total: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM events WHERE event_data->>'encrypted' = 'true'"
    )
    .fetch_one(pool)
    .await?;

    state.set_remaining(total as u64);
    info!(total, "Starting re-encryption job");

    let mut offset = 0;
    let mut reencrypted = 0;

    loop {
        let rows: Vec<(uuid::Uuid, serde_json::Value)> = sqlx::query_as(
            "SELECT id, event_data FROM events \
             WHERE event_data->>'encrypted' = 'true' \
             ORDER BY id \
             LIMIT $1 OFFSET $2"
        )
        .bind(batch_size as i64)
        .bind(offset as i64)
        .fetch_all(pool)
        .await?;

        if rows.is_empty() {
            break;
        }

        for (id, event_data) in rows {
            // Try to decrypt with old key, then re-encrypt with new key
            match encryption::decrypt(&new_key, Some(&old_key), &event_data) {
                Ok(decrypted) => {
                    match encryption::encrypt(&new_key, &decrypted) {
                        Ok(reencrypted_data) => {
                            if let Err(e) = sqlx::query(
                                "UPDATE events SET event_data = $1 WHERE id = $2"
                            )
                            .bind(&reencrypted_data)
                            .bind(id)
                            .execute(pool)
                            .await {
                                error!(id = %id, error = %e, "Failed to update re-encrypted event");
                                metrics::record_reencrypt_error();
                            } else {
                                reencrypted += 1;
                            }
                        }
                        Err(e) => {
                            error!(id = %id, error = %e, "Failed to re-encrypt event");
                            metrics::record_reencrypt_error();
                        }
                    }
                }
                Err(e) => {
                    error!(id = %id, error = %e, "Failed to decrypt event with old key");
                    metrics::record_reencrypt_error();
                }
            }
        }

        offset += batch_size;
        let remaining = (total as usize - offset).max(0);
        state.set_remaining(remaining as u64);
        metrics::update_reencrypt_progress(remaining as u64);

        info!(reencrypted, remaining, "Re-encryption progress");
    }

    info!(total_reencrypted = reencrypted, "Re-encryption job completed");
    Ok(())
}

/// Ensure the checkpoint table used to resume rotation runs exists.
async fn ensure_checkpoint_table(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS reencryption_checkpoints (
            id SMALLINT PRIMARY KEY DEFAULT 1,
            last_processed_id UUID,
            total_rows BIGINT NOT NULL DEFAULT 0,
            completed_rows BIGINT NOT NULL DEFAULT 0,
            finished BOOLEAN NOT NULL DEFAULT FALSE,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Load the persisted checkpoint, if any.
pub async fn load_checkpoint(pool: &PgPool) -> anyhow::Result<Option<RotationCheckpoint>> {
    ensure_checkpoint_table(pool).await?;
    let row: Option<RotationCheckpoint> = sqlx::query_as(
        "SELECT last_processed_id, total_rows, completed_rows, finished \
         FROM reencryption_checkpoints WHERE id = 1",
    )
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Persist the checkpoint within an existing transaction.
async fn save_checkpoint(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    last_processed_id: Option<uuid::Uuid>,
    total_rows: i64,
    completed_rows: i64,
    finished: bool,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO reencryption_checkpoints \
            (id, last_processed_id, total_rows, completed_rows, finished, updated_at) \
         VALUES (1, $1, $2, $3, $4, now()) \
         ON CONFLICT (id) DO UPDATE SET \
            last_processed_id = EXCLUDED.last_processed_id, \
            total_rows = EXCLUDED.total_rows, \
            completed_rows = EXCLUDED.completed_rows, \
            finished = EXCLUDED.finished, \
            updated_at = now()",
    )
    .bind(last_processed_id)
    .bind(total_rows)
    .bind(completed_rows)
    .bind(finished)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Rotate the AES-GCM key used for credential encryption.
///
/// Decrypts each credential with `old_key`, re-encrypts with `new_key`, and
/// commits in batches of [`ROTATION_BATCH_SIZE`] rows to avoid long locks.
/// A checkpoint (last processed id) is persisted in the same transaction as
/// each batch, so an interrupted run resumes where it stopped.
///
/// Returns the number of rows re-encrypted by this invocation.
pub async fn rotate_credentials(
    pool: &PgPool,
    old_key: [u8; 32],
    new_key: [u8; 32],
    state: &ReencryptState,
) -> anyhow::Result<u64> {
    if state.is_running() {
        anyhow::bail!("re-encryption job already running");
    }
    state.set_running(true);

    let result = rotate_credentials_inner(pool, old_key, new_key, state).await;

    state.set_running(false);
    result
}

async fn rotate_credentials_inner(
    pool: &PgPool,
    old_key: [u8; 32],
    new_key: [u8; 32],
    state: &ReencryptState,
) -> anyhow::Result<u64> {
    ensure_checkpoint_table(pool).await?;

    // Resume from checkpoint if a previous run was interrupted.
    let checkpoint = load_checkpoint(pool).await?;
    let (mut last_id, total, mut completed) = match checkpoint {
        Some(cp) if !cp.finished => {
            info!(
                completed = cp.completed_rows,
                total = cp.total_rows,
                "Resuming re-encryption job from checkpoint"
            );
            (cp.last_processed_id, cp.total_rows, cp.completed_rows)
        }
        _ => {
            let total: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM credentials WHERE encrypted_data IS NOT NULL",
            )
            .fetch_one(pool)
            .await?;
            (None, total, 0)
        }
    };

    state.set_total(total as u64);
    state.set_completed(completed as u64);
    state.set_remaining((total - completed).max(0) as u64);
    metrics::update_reencryption_progress(completed as u64, total as u64);

    info!(total, completed, "Starting credential key rotation");

    let mut rotated_this_run: u64 = 0;

    loop {
        // Fetch the next batch of credential ids after the checkpoint.
        let ids: Vec<uuid::Uuid> = sqlx::query_scalar(
            "SELECT id FROM credentials \
             WHERE encrypted_data IS NOT NULL AND ($1::uuid IS NULL OR id > $1) \
             ORDER BY id \
             LIMIT $2",
        )
        .bind(last_id)
        .bind(ROTATION_BATCH_SIZE as i64)
        .fetch_all(pool)
        .await?;

        if ids.is_empty() {
            break;
        }

        // Process the batch inside a single transaction so the row updates and
        // the checkpoint commit atomically.
        let mut tx = pool.begin().await?;
        let mut batch_rotated: i64 = 0;

        for id in &ids {
            let encrypted_data: Option<serde_json::Value> = sqlx::query_scalar(
                "SELECT encrypted_data FROM credentials WHERE id = $1 FOR UPDATE",
            )
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;

            let Some(encrypted_data) = encrypted_data else {
                continue;
            };

            // Decrypt with the old key (falling back to the new key if the row
            // was already rotated by a previous interrupted run).
            let decrypted = match encryption::decrypt(&old_key, None, &encrypted_data) {
                Ok(v) => v,
                Err(old_err) => match encryption::decrypt(&new_key, None, &encrypted_data) {
                    Ok(v) => v,
                    Err(_) => {
                        error!(id = %id, error = %old_err, "Failed to decrypt credential with old key");
                        metrics::record_reencrypt_error();
                        return Err(anyhow::anyhow!(
                            "failed to decrypt credential {id} with old key: {old_err}"
                        ));
                    }
                },
            };

            let reencrypted = encryption::encrypt(&new_key, &decrypted)?;

            sqlx::query("UPDATE credentials SET encrypted_data = $1 WHERE id = $2")
                .bind(&reencrypted)
                .bind(id)
                .execute(&mut *tx)
                .await?;

            batch_rotated += 1;
        }

        last_id = ids.last().copied();
        completed += batch_rotated;
        rotated_this_run += batch_rotated as u64;

        let finished = completed >= total;
        save_checkpoint(&mut tx, last_id, total, completed, finished).await?;
        tx.commit().await?;

        state.set_completed(completed as u64);
        state.set_remaining((total - completed).max(0) as u64);
        metrics::update_reencryption_progress(completed as u64, total as u64);

        info!(completed, total, "Credential rotation progress");
    }

    // Mark the run finished so a subsequent rotation starts fresh.
    let mut tx = pool.begin().await?;
    save_checkpoint(&mut tx, last_id, total, completed, true).await?;
    tx.commit().await?;

    info!(
        rotated = rotated_this_run,
        total = completed,
        "Credential key rotation completed"
    );
    Ok(rotated_this_run)
}

/// Check if any rows are still encrypted with the old key.
pub async fn has_old_key_rows(pool: &PgPool) -> anyhow::Result<bool> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM events WHERE event_data->>'encrypted' = 'true' LIMIT 1"
    )
    .fetch_one(pool)
    .await?;

    Ok(count > 0)
}

/// Verify that no credential can still be decrypted with `old_key`.
///
/// Used by the admin rotation endpoint to confirm the rotation completed and
/// that the old key no longer unlocks any stored credential.
pub async fn verify_old_key_rejected(
    pool: &PgPool,
    old_key: [u8; 32],
) -> anyhow::Result<bool> {
    let rows: Vec<(uuid::Uuid, serde_json::Value)> = sqlx::query_as(
        "SELECT id, encrypted_data FROM credentials WHERE encrypted_data IS NOT NULL",
    )
    .fetch_all(pool)
    .await?;

    for (id, encrypted_data) in rows {
        if encryption::decrypt(&old_key, None, &encrypted_data).is_ok() {
            warn!(id = %id, "Credential still decryptable with old key after rotation");
            return Ok(false);
        }
    }

    Ok(true)
}
