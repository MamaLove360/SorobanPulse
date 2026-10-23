//! Unified background job orchestration (issue #1248).
//!
//! Every long-running background task (indexer, monitors, delivery workers,
//! publishers, …) is registered here by name so it can be listed via
//! `GET /v1/admin/jobs`, health-checked with [`JobScheduler::jobs_alive`],
//! and aborted together on shutdown with [`JobScheduler::shutdown_all`].
//!
//! Tasks are either spawned through the scheduler ([`JobScheduler::spawn`])
//! or, for helpers that already spawn internally and hand back a
//! `JoinHandle`, adopted after the fact ([`JobScheduler::adopt`]). Tasks that
//! are expected to finish (startup backfills, checks) use
//! [`JobScheduler::spawn_oneshot`] so that finishing does not count as a
//! failure in [`JobScheduler::jobs_alive`].

use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use axum::Json;
use serde::Serialize;
use serde_json::{json, Value};
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    /// The task has not completed.
    Running,
    /// A one-shot task that has finished.
    Completed,
    /// A long-running task that returned, panicked or was aborted.
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    /// Expected to run until shutdown.
    LongRunning,
    /// Expected to finish on its own.
    OneShot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobInfo {
    pub name: String,
    pub kind: JobKind,
    pub status: JobStatus,
}

struct Job {
    name: String,
    kind: JobKind,
    handle: JoinHandle<()>,
}

impl Job {
    fn status(&self) -> JobStatus {
        match (self.handle.is_finished(), self.kind) {
            (false, _) => JobStatus::Running,
            (true, JobKind::OneShot) => JobStatus::Completed,
            (true, JobKind::LongRunning) => JobStatus::Stopped,
        }
    }
}

/// Registry of named background tasks.
#[derive(Default)]
pub struct JobScheduler {
    jobs: Mutex<Vec<Job>>,
}

impl JobScheduler {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    // A panic while holding the lock cannot leave the Vec half-updated, so
    // recovering from poisoning is safe.
    fn lock(&self) -> MutexGuard<'_, Vec<Job>> {
        self.jobs.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Spawn a long-running `future` and register it under `name`.
    pub fn spawn<F>(&self, name: impl Into<String>, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.register(name.into(), JobKind::LongRunning, tokio::spawn(future));
    }

    /// Spawn a `future` that is expected to finish on its own.
    pub fn spawn_oneshot<F>(&self, name: impl Into<String>, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.register(name.into(), JobKind::OneShot, tokio::spawn(future));
    }

    /// Register a long-running task that was spawned elsewhere.
    pub fn adopt(&self, name: impl Into<String>, handle: JoinHandle<()>) {
        self.register(name.into(), JobKind::LongRunning, handle);
    }

    fn register(&self, name: String, kind: JobKind, handle: JoinHandle<()>) {
        tracing::debug!(job = %name, ?kind, "Registered background job");
        self.lock().push(Job { name, kind, handle });
        self.publish_metrics();
    }

    /// Name and status of every registered job, in registration order.
    #[must_use]
    pub fn jobs(&self) -> Vec<JobInfo> {
        self.lock()
            .iter()
            .map(|j| JobInfo { name: j.name.clone(), kind: j.kind, status: j.status() })
            .collect()
    }

    /// Number of registered jobs that are still running.
    #[must_use]
    pub fn alive_count(&self) -> usize {
        self.lock().iter().filter(|j| !j.handle.is_finished()).count()
    }

    /// Health predicate: true when no long-running job has stopped.
    #[must_use]
    pub fn jobs_alive(&self) -> bool {
        self.lock().iter().all(|j| j.status() != JobStatus::Stopped)
    }

    /// Remove the first job registered under `name` and return its handle,
    /// e.g. so the caller can await it during shutdown.
    pub fn take(&self, name: &str) -> Option<JoinHandle<()>> {
        let mut jobs = self.lock();
        let idx = jobs.iter().position(|j| j.name == name)?;
        let handle = jobs.remove(idx).handle;
        drop(jobs);
        self.publish_metrics();
        Some(handle)
    }

    /// Abort every registered job and clear the registry.
    pub fn shutdown_all(&self) {
        let jobs: Vec<Job> = std::mem::take(&mut *self.lock());
        for job in &jobs {
            tracing::debug!(job = %job.name, "Aborting background job");
            job.handle.abort();
        }
        self.publish_metrics();
    }

    /// Update `soroban_pulse_background_jobs_alive`.
    pub fn publish_metrics(&self) {
        crate::metrics::set_background_jobs_alive(self.alive_count());
    }
}

static SCHEDULER: OnceLock<Arc<JobScheduler>> = OnceLock::new();

/// The process-wide scheduler used by `main` and `GET /v1/admin/jobs`.
pub fn global() -> &'static Arc<JobScheduler> {
    SCHEDULER.get_or_init(|| Arc::new(JobScheduler::new()))
}

/// Periodically refresh the jobs-alive gauge so stopped jobs are reflected
/// even when nobody calls the admin endpoint.
pub async fn run_metrics_refresher(
    scheduler: Arc<JobScheduler>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(15));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = ticker.tick() => scheduler.publish_metrics(),
            _ = shutdown_rx.changed() => break,
        }
    }
}

/// `GET /v1/admin/jobs` — list background jobs and their status.
#[utoipa::path(
    get,
    path = "/v1/admin/jobs",
    tag = "admin",
    responses(
        (status = 200, description = "Registered background jobs and their status", body = serde_json::Value),
        (status = 401, description = "Missing or invalid admin API key"),
    ),
    security(("admin_key" = []))
)]
pub async fn list_jobs() -> Json<Value> {
    let scheduler = global();
    let jobs = scheduler.jobs();
    let alive = jobs.iter().filter(|j| j.status == JobStatus::Running).count();
    scheduler.publish_metrics();
    Json(json!({
        "jobs": jobs,
        "total": jobs.len(),
        "alive": alive,
        "healthy": scheduler.jobs_alive(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    async fn settle() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    #[tokio::test]
    async fn lists_running_and_stopped_jobs() {
        let s = JobScheduler::new();
        s.spawn("forever", std::future::pending());
        s.spawn("oneshot", async {});
        settle().await;
        assert_eq!(
            s.jobs(),
            vec![
                JobInfo { name: "forever".into(), kind: JobKind::LongRunning, status: JobStatus::Running },
                JobInfo { name: "oneshot".into(), kind: JobKind::LongRunning, status: JobStatus::Stopped },
            ]
        );
        assert_eq!(s.alive_count(), 1);
        assert!(!s.jobs_alive());
    }

    #[tokio::test]
    async fn finished_oneshot_is_completed_and_healthy() {
        let s = JobScheduler::new();
        s.spawn("forever", std::future::pending());
        s.spawn_oneshot("backfill", async {});
        settle().await;
        assert_eq!(s.jobs()[1].status, JobStatus::Completed);
        assert!(s.jobs_alive());
    }

    #[tokio::test]
    async fn adopt_registers_external_handle() {
        let s = JobScheduler::new();
        s.adopt("external", tokio::spawn(std::future::pending()));
        assert!(s.jobs_alive());
        assert_eq!(s.jobs()[0].name, "external");
    }

    #[tokio::test]
    async fn panicking_job_reports_stopped() {
        let s = JobScheduler::new();
        s.spawn("boom", async { panic!("job failed") });
        settle().await;
        assert_eq!(s.jobs()[0].status, JobStatus::Stopped);
    }

    #[tokio::test]
    async fn take_removes_and_returns_handle() {
        let s = JobScheduler::new();
        s.spawn("a", async {});
        s.spawn("b", std::future::pending());
        let h = s.take("a").expect("job a registered");
        h.await.expect("job a completes");
        assert!(s.take("a").is_none());
        assert_eq!(s.jobs().len(), 1);
    }

    #[tokio::test]
    async fn shutdown_all_aborts_everything() {
        let s = JobScheduler::new();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        s.spawn("waiter", async move {
            let _ = rx.await;
        });
        s.shutdown_all();
        assert!(s.jobs().is_empty());
        settle().await;
        // The aborted task dropped its receiver.
        assert!(tx.send(()).is_err());
    }

    #[test]
    fn empty_scheduler_is_alive() {
        assert!(JobScheduler::new().jobs_alive());
    }
}
