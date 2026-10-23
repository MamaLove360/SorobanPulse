//! Issue #266: Bloom filter deduplication pre-filter.
//! Issue #615: Session-level Bloom filter for per-RPC-poll deduplication with ledger-reset.
//! Issue #996: Memory-bounded bloom filter with rotation and fill-ratio tracking.
//!
//! Stores hashes of `(tx_hash, contract_id, event_type)` tuples to skip
//! database inserts for events that are very likely already indexed.
//! False positives cause a missed insert (the DB unique constraint is the
//! authoritative guard); false negatives are impossible by design.
//!
//! ## Issue #996 — Bounded memory / rotation strategy
//!
//! The original `EventBloomFilter` grew unboundedly: once seeded from the DB at
//! startup it was never reset, so over long runtimes its effective false-positive
//! rate silently degraded toward 100%.
//!
//! The fix uses a **double-buffer rotation** approach:
//! - Two bloom filters (`current` and `previous`) of fixed capacity are maintained.
//! - When `current` is filled to `fill_ratio_threshold` (default 80%), it is
//!   promoted to `previous` and a fresh `current` filter is created.
//! - Lookups check both filters so events in the rotated filter are still
//!   detected as duplicates.
//! - This bounds peak memory to `2 × bloom_memory(capacity, fp_rate)` and keeps
//!   the false-positive rate near the configured target.
//! - Metrics track fill ratio, memory usage, and rotation count.
//!
//! ## Deduplication layers
//!
//! 1. **Session Bloom filter** (`SessionBloomFilter`): reset every time a new ledger is
//!    detected. Catches duplicates within a single RPC poll session — e.g. overlapping
//!    cursors returning the same event twice in the same batch.
//! 2. **Persistent Bloom filter** (`EventBloomFilter`): seeded from recent DB rows at
//!    startup; rotated when full. Catches events already persisted to the DB.
//! 3. **DB `ON CONFLICT DO NOTHING`**: the authoritative guard for all cases.

use bloomfilter::Bloom;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashSet;

use crate::error::InitError;
use crate::metrics;

/// Build a bloom filter, mapping the crate's parameter-validation error
/// (zero capacity, fp_rate outside (0, 1)) to a typed [`InitError`].
fn new_bloom(capacity: usize, fp_rate: f64) -> Result<Bloom<String>, InitError> {
    Bloom::new_for_fp_rate(capacity, fp_rate).map_err(|reason| {
        InitError::invalid_config(
            "bloom filter",
            format!("capacity={capacity}, fp_rate={fp_rate}: {reason}"),
        )
    })
}

// ── Issue #615: Session-level Bloom filter ───────────────────────────────────

/// A per-poll-session Bloom filter that resets when a new ledger sequence is detected.
///
/// Unlike `EventBloomFilter` (which is long-lived and seeded from the DB), this filter
/// is scoped to a single indexer poll cycle. It is reset on every new ledger, so its
/// memory footprint is bounded by the number of events in one ledger.
pub struct SessionBloomFilter {
    inner: Mutex<Bloom<String>>,
    /// The ledger sequence at which the filter was last reset.
    current_ledger: Mutex<u64>,
    capacity: usize,
    fp_rate: f64,
}

impl SessionBloomFilter {
    /// Create a session filter sized for `capacity` events per ledger.
    pub fn new(capacity: usize, fp_rate: f64) -> Result<Self, InitError> {
        let bloom = new_bloom(capacity, fp_rate)?;
        Ok(Self {
            inner: Mutex::new(bloom),
            current_ledger: Mutex::new(0),
            capacity,
            fp_rate,
        })
    }

    /// Check whether this event was already seen in the current session.
    ///
    /// Automatically resets the filter when `ledger` advances beyond the last-seen ledger,
    /// then records the event. Returns `true` (duplicate) only when the same ledger is active.
    pub fn check_and_set(&self, tx_hash: &str, contract_id: &str, event_type: &str, ledger: u64) -> bool {
        let key = format!("{tx_hash}:{contract_id}:{event_type}");

        let mut current = self.current_ledger.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if ledger > *current {
            // New ledger detected — reset the filter. The parameters were
            // validated in `new`, so a rebuild failure is not expected; if it
            // happens, clear the existing filter in place instead.
            let mut guard = self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            match new_bloom(self.capacity, self.fp_rate) {
                Ok(fresh) => *guard = fresh,
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to rebuild session bloom filter; clearing in place");
                    guard.clear();
                }
            }
            drop(guard);
            *current = ledger;
            metrics::record_session_bloom_reset();
        }

        let mut guard = self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if guard.check(&key) {
            metrics::record_session_bloom_hit();
            return true;
        }
        guard.set(&key);
        false
    }
}

/// Thread-safe bloom filter for event deduplication.
///
/// Issue #996: Uses a **double-buffer rotation** strategy so memory usage is
/// bounded.  When the current filter fills past `fill_ratio_threshold` it is
/// swapped into the "previous" slot and a fresh current filter is created.
/// Lookups probe both buffers so recently-rotated entries are still detected.
pub struct EventBloomFilter {
    /// Active (current) bloom filter — new entries are inserted here.
    inner: Mutex<Bloom<String>>,
    /// Previous generation bloom filter — kept for duplicate detection after a rotation.
    previous: Mutex<Option<Bloom<String>>>,
    capacity: usize,
    fp_rate: f64,
    /// Issue #996: Fill-ratio threshold that triggers a rotation (0.0–1.0, default 0.80).
    fill_ratio_threshold: f64,
    /// Issue #996: Counter of items inserted into `inner` since last rotation.
    insert_count: AtomicU64,
    /// Issue #996: Total rotation count since process start.
    rotation_count: AtomicU64,
    /// Issue #627: Separate bloom filter for tracking contract existence.
    contract_filter: Mutex<Bloom<String>>,
    /// Issue #627: Exact set of known contracts for fallback.
    known_contracts: Mutex<HashSet<String>>,
}

impl EventBloomFilter {
    /// Create a new bloom filter with the given false-positive rate and capacity.
    ///
    /// Returns [`InitError::InvalidConfig`] if `fp_rate` is not in (0, 1) or
    /// `capacity` is 0.
    pub fn new(capacity: usize, fp_rate: f64) -> Result<Self, InitError> {
        Self::with_fill_threshold(capacity, fp_rate, 0.80)
    }

    /// Create a bloom filter with a custom fill-ratio rotation threshold.
    ///
    /// When the current filter's estimated fill ratio exceeds `fill_ratio_threshold`
    /// (0.0–1.0), the current filter is rotated into the "previous" slot and a
    /// fresh filter is created.  Set to `1.0` to disable rotation.
    ///
    /// Issue #996.
    pub fn with_fill_threshold(
        capacity: usize,
        fp_rate: f64,
        fill_ratio_threshold: f64,
    ) -> Result<Self, InitError> {
        let bloom = new_bloom(capacity, fp_rate)?;
        let contract_bloom = new_bloom((capacity / 10).max(1), fp_rate)?;
        let memory_bytes = Self::estimate_memory_bytes(capacity, fp_rate);
        metrics::update_bloom_filter_memory_bytes(memory_bytes);
        metrics::update_bloom_filter_fill_ratio(0.0);
        Ok(Self {
            inner: Mutex::new(bloom),
            previous: Mutex::new(None),
            capacity,
            fp_rate,
            fill_ratio_threshold: fill_ratio_threshold.clamp(0.01, 1.0),
            insert_count: AtomicU64::new(0),
            rotation_count: AtomicU64::new(0),
            contract_filter: Mutex::new(contract_bloom),
            known_contracts: Mutex::new(HashSet::new()),
        })
    }

    /// Build the deduplication key for an event.
    fn key(tx_hash: &str, contract_id: &str, event_type: &str) -> String {
        format!("{tx_hash}:{contract_id}:{event_type}")
    }

    /// Estimate the heap memory used by a bloom filter with the given parameters.
    ///
    /// The bloomfilter crate uses a bit array; the byte count is roughly
    /// `capacity × ln(fp_rate)^2 / ln(2)^2 / 8`.  This function returns a
    /// conservative upper-bound using the commonly-cited formula.
    pub fn estimate_memory_bytes(capacity: usize, fp_rate: f64) -> u64 {
        if capacity == 0 || fp_rate <= 0.0 || fp_rate >= 1.0 {
            return 0;
        }
        let bits_per_entry = -(fp_rate.ln()) / (2.0_f64.ln().powi(2));
        let total_bits = (capacity as f64 * bits_per_entry).ceil() as u64;
        (total_bits + 7) / 8
    }

    /// Issue #996: Rotate the filter if the fill ratio exceeds the threshold.
    ///
    /// Returns `true` if a rotation was performed.
    fn maybe_rotate(&self) -> bool {
        let count = self.insert_count.load(Ordering::Relaxed);
        let fill_ratio = count as f64 / self.capacity as f64;
        if fill_ratio < self.fill_ratio_threshold {
            metrics::update_bloom_filter_fill_ratio(fill_ratio);
            return false;
        }

        // Rotation: swap inner into previous and create a fresh inner.
        // Parameters were validated at construction; if a rebuild still fails,
        // skip this rotation and keep using the current filter.
        let new_bloom = match new_bloom(self.capacity, self.fp_rate) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "Bloom filter rotation skipped");
                return false;
            }
        };
        let old_inner = {
            let mut guard = self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::replace(&mut *guard, new_bloom)
        };
        *self.previous.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(old_inner);
        self.insert_count.store(0, Ordering::Relaxed);
        self.rotation_count.fetch_add(1, Ordering::Relaxed);

        metrics::record_bloom_filter_rotation();
        metrics::record_bloom_filter_memory_reset();
        metrics::update_bloom_filter_fill_ratio(0.0);
        // Two filters now in memory.
        let single_mem = Self::estimate_memory_bytes(self.capacity, self.fp_rate);
        metrics::update_bloom_filter_memory_bytes(single_mem * 2);

        true
    }

    /// Returns `true` if the event was probably already seen (bloom filter hit).
    /// Increments `soroban_pulse_bloom_filter_hits_total` on a hit.
    pub fn check(&self, tx_hash: &str, contract_id: &str, event_type: &str) -> bool {
        let k = Self::key(tx_hash, contract_id, event_type);

        // Check current filter.
        let hit_current = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .check(&k);

        // Check previous filter (may exist after a rotation).
        let hit_previous = if !hit_current {
            self.previous
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .map(|b| b.check(&k))
                .unwrap_or(false)
        } else {
            false
        };

        let hit = hit_current || hit_previous;
        if hit {
            metrics::record_bloom_filter_hit();
        }
        hit
    }

    /// Record that an event has been seen.
    ///
    /// Issue #996: increments the fill counter and triggers rotation when full.
    pub fn set(&self, tx_hash: &str, contract_id: &str, event_type: &str) {
        let k = Self::key(tx_hash, contract_id, event_type);
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .set(&k);
        let new_count = self.insert_count.fetch_add(1, Ordering::Relaxed) + 1;
        let fill_ratio = new_count as f64 / self.capacity as f64;
        metrics::update_bloom_filter_fill_ratio(fill_ratio.min(1.0));
        self.maybe_rotate();
    }

    /// Seed the filter from a list of `(tx_hash, contract_id, event_type)` tuples.
    /// Used at startup to pre-populate from recent DB rows.
    ///
    /// Issue #996: seeding counts toward the rotation threshold.
    pub fn seed(&self, entries: impl IntoIterator<Item = (String, String, String)>) {
        let mut guard = self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut contract_guard = self.contract_filter.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut known_contracts = self.known_contracts.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut count = 0u64;

        for (tx_hash, contract_id, event_type) in entries {
            let k = Self::key(&tx_hash, &contract_id, &event_type);
            guard.set(&k);
            count += 1;

            // Track contract existence.
            contract_guard.set(&contract_id);
            known_contracts.insert(contract_id);
        }
        drop(guard);
        drop(contract_guard);
        drop(known_contracts);

        // Update fill counters.
        let new_count = self.insert_count.fetch_add(count, Ordering::Relaxed) + count;
        let fill_ratio = new_count as f64 / self.capacity as f64;
        metrics::update_bloom_filter_fill_ratio(fill_ratio.min(1.0));
        let mem = Self::estimate_memory_bytes(self.capacity, self.fp_rate);
        metrics::update_bloom_filter_memory_bytes(mem);
        // Trigger rotation if seeding overloaded the filter.
        self.maybe_rotate();
    }

    /// Issue #996: Returns the current fill ratio (inserted items / capacity).
    pub fn fill_ratio(&self) -> f64 {
        let count = self.insert_count.load(Ordering::Relaxed);
        count as f64 / self.capacity as f64
    }

    /// Issue #996: Returns the total number of rotations since process start.
    pub fn rotation_count(&self) -> u64 {
        self.rotation_count.load(Ordering::Relaxed)
    }

    /// Issue #996: Returns the estimated total memory usage in bytes.
    /// If a previous filter exists (post-rotation) this is 2× the single-filter size.
    pub fn memory_bytes(&self) -> u64 {
        let single = Self::estimate_memory_bytes(self.capacity, self.fp_rate);
        let has_previous = self
            .previous
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some();
        if has_previous { single * 2 } else { single }
    }

    /// Issue #627: Check if a contract has any indexed events.
    /// Returns true if the contract is likely to exist (may have false positives).
    pub fn contains_contract(&self, contract_id: &str) -> bool {
        // First check the exact set for fast paths
        if let Ok(known) = self.known_contracts.lock() {
            if known.contains(contract_id) {
                return true;
            }
        }

        // Then check the bloom filter
        if let Ok(guard) = self.contract_filter.lock() {
            return guard.check(contract_id);
        }

        false
    }

    /// Issue #627: Add a contract to the bloom filter.
    pub fn add_contract(&self, contract_id: &str) {
        if let Ok(mut guard) = self.contract_filter.lock() {
            guard.set(contract_id);
        }
        if let Ok(mut known) = self.known_contracts.lock() {
            known.insert(contract_id.to_string());
        }
    }
}

/// Load recent events from the database and seed the bloom filter.
/// Loads up to `limit` most recent events by ledger descending.
pub async fn seed_from_db(
    filter: &EventBloomFilter,
    pool: &sqlx::PgPool,
    limit: i64,
) -> Result<usize, sqlx::Error> {
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT tx_hash, contract_id, event_type FROM events ORDER BY ledger DESC LIMIT $1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let count = rows.len();
    filter.seed(rows.into_iter().map(|(tx, cid, et)| (tx, cid, et)));
    
    // Estimate and record the current false-positive rate
    let fill_ratio = filter.fill_ratio();
    if fill_ratio > 0.0 {
        let estimated_fp_rate = estimate_false_positive_rate(filter.fp_rate, fill_ratio);
        metrics::set_bloom_false_positive_rate(estimated_fp_rate);
    }
    
    Ok(count)
}

/// Persist the bloom filter state to the database.
pub async fn persist_state(
    filter: &EventBloomFilter,
    pool: &sqlx::PgPool,
) -> Result<(), sqlx::Error> {
    let guard = filter.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let bitmap = guard.bitmap();
    let bitmap_bytes = bitmap.iter().map(|&b| b as i16).collect::<Vec<_>>();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    sqlx::query(
        "INSERT INTO indexer_bloom_state (capacity, fp_rate, bitmap, persisted_at) 
         VALUES ($1, $2, $3, to_timestamp($4))
         ON CONFLICT (id) DO UPDATE SET bitmap = $3, persisted_at = to_timestamp($4)"
    )
    .bind(filter.capacity as i32)
    .bind(filter.fp_rate)
    .bind(bitmap_bytes)
    .bind(now)
    .execute(pool)
    .await?;

    Ok(())
}

/// Calculate auto-sized bloom filter parameters from expected daily event count and target FP rate.
/// Returns (capacity, fp_rate) suitable for EventBloomFilter::with_fill_threshold.
///
/// For high-volume deployments (>10,000 events/day), auto-sizing ensures the filter doesn't
/// degrade under load. At 10x capacity, the FP rate should remain under the configured target.
pub fn auto_size_bloom_filter(
    expected_items_per_day: usize,
    target_fp_rate: f64,
) -> (usize, f64) {
    if expected_items_per_day == 0 {
        return (1_000_000, 0.001);
    }

    // Use 10× the daily rate as capacity to account for variable workloads and avoid
    // frequent rotations. At 10x, the actual FP rate should still be near the target.
    let capacity = expected_items_per_day * 10;

    // Clamp the target FP rate to reasonable bounds [0.0001, 0.01]
    let clamped_fp_rate = target_fp_rate.clamp(0.0001, 0.01);

    (capacity, clamped_fp_rate)
}

/// Estimate the actual false-positive rate of a bloom filter at a given fill ratio.
///
/// Uses the approximation: FP_rate_actual ≈ target_FP_rate * (fill_ratio ^ k_hash_functions)
/// where k is derived from the target FP rate. This is an approximation used for monitoring.
pub fn estimate_false_positive_rate(target_fp_rate: f64, fill_ratio: f64) -> f64 {
    if fill_ratio <= 0.0 || fill_ratio >= 1.0 || target_fp_rate <= 0.0 || target_fp_rate >= 1.0 {
        return target_fp_rate;
    }

    // Derive approximate k (number of hash functions) from target FP rate.
    // k = ln(2) * m / n = ln(2) / -ln(target_FP_rate)
    let k = std::f64::consts::LN_2 / (-target_fp_rate.ln());

    // Approximate actual FP rate at current fill ratio.
    // As fill ratio increases, FP rate approaches target asymptotically.
    // Using: FP_actual ≈ target * (fill_ratio ^ k)
    let estimated = target_fp_rate * fill_ratio.powf(k);

    // Clamp to [0, 1] and never lower than target (can only degrade, not improve)
    estimated.clamp(target_fp_rate, 1.0)
}

/// Restore the bloom filter state from the database if available and not stale.
pub async fn restore_state(
    pool: &sqlx::PgPool,
    max_age_secs: i64,
) -> Result<Option<EventBloomFilter>, sqlx::Error> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let row: Option<(i32, f64, Vec<i16>)> = sqlx::query_as(
        "SELECT capacity, fp_rate, bitmap FROM indexer_bloom_state 
         WHERE persisted_at > to_timestamp($1) LIMIT 1"
    )
    .bind(now - max_age_secs)
    .fetch_optional(pool)
    .await?;

    match row {
        Some((capacity, fp_rate, bitmap_bytes)) => {
            // Bitmap restoration is deferred to DB re-seeding; the persisted state
            // is used only to restore capacity/fp_rate parameters.
            let _ = &bitmap_bytes;

            // Corrupt persisted parameters are treated as "no usable state" so
            // the caller falls back to a freshly seeded filter.
            let (bloom, contract_bloom) = match (
                new_bloom(capacity as usize, fp_rate),
                new_bloom((capacity as usize).max(100) / 10, fp_rate),
            ) {
                (Ok(b), Ok(c)) => (b, c),
                (Err(e), _) | (_, Err(e)) => {
                    tracing::warn!(error = %e, "Ignoring persisted bloom filter state");
                    return Ok(None);
                }
            };
            Ok(Some(EventBloomFilter {
                inner: Mutex::new(bloom),
                previous: Mutex::new(None),
                capacity: capacity as usize,
                fp_rate,
                fill_ratio_threshold: 0.80,
                insert_count: AtomicU64::new(0),
                rotation_count: AtomicU64::new(0),
                contract_filter: Mutex::new(contract_bloom),
                known_contracts: Mutex::new(HashSet::new()),
            }))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_filter() -> EventBloomFilter {
        EventBloomFilter::new(10_000, 0.001).unwrap()
    }

    #[test]
    fn new_filter_has_no_hits() {
        let f = make_filter();
        assert!(!f.check("tx1", "contract1", "contract"));
    }

    #[test]
    fn set_then_check_returns_true() {
        let f = make_filter();
        f.set("tx1", "contract1", "contract");
        assert!(f.check("tx1", "contract1", "contract"));
    }

    #[test]
    fn different_event_type_not_hit() {
        let f = make_filter();
        f.set("tx1", "contract1", "contract");
        assert!(!f.check("tx1", "contract1", "system"));
    }

    #[test]
    fn different_tx_hash_not_hit() {
        let f = make_filter();
        f.set("tx1", "contract1", "contract");
        assert!(!f.check("tx2", "contract1", "contract"));
    }

    #[test]
    fn seed_populates_filter() {
        let f = make_filter();
        f.seed(vec![
            ("tx1".into(), "c1".into(), "contract".into()),
            ("tx2".into(), "c2".into(), "system".into()),
        ]);
        assert!(f.check("tx1", "c1", "contract"));
        assert!(f.check("tx2", "c2", "system"));
        assert!(!f.check("tx3", "c3", "contract"));
    }

    #[test]
    fn multiple_sets_all_hit() {
        let f = make_filter();
        for i in 0..100u32 {
            f.set(&format!("tx{i}"), "contract1", "contract");
        }
        for i in 0..100u32 {
            assert!(f.check(&format!("tx{i}"), "contract1", "contract"));
        }
    }

    #[test]
    fn filter_stores_capacity_and_fp_rate() {
        let f = EventBloomFilter::new(5000, 0.01).unwrap();
        assert_eq!(f.capacity, 5000);
        assert_eq!(f.fp_rate, 0.01);
    }

    // ── Issue #996: fill-ratio and rotation tests ─────────────────────────

    #[test]
    fn fill_ratio_zero_on_new_filter() {
        let f = EventBloomFilter::new(10_000, 0.001).unwrap();
        assert_eq!(f.fill_ratio(), 0.0);
        assert_eq!(f.rotation_count(), 0);
    }

    #[test]
    fn fill_ratio_increases_with_inserts() {
        let f = EventBloomFilter::new(1000, 0.001).unwrap();
        f.set("tx1", "c1", "contract");
        assert!(f.fill_ratio() > 0.0);
    }

    #[test]
    fn rotation_triggered_when_fill_threshold_exceeded() {
        // Small capacity so we hit the threshold quickly.
        let f = EventBloomFilter::with_fill_threshold(10, 0.01, 0.5).unwrap();
        for i in 0..6u32 {
            f.set(&format!("tx{i}"), "c1", "contract");
        }
        assert!(f.rotation_count() >= 1, "rotation should have fired");
    }

    #[test]
    fn entries_still_found_after_rotation() {
        let f = EventBloomFilter::with_fill_threshold(10, 0.01, 0.5).unwrap();
        f.set("tx_before_rotate", "c1", "contract");
        for i in 0..6u32 {
            f.set(&format!("tx{i}"), "c1", "contract");
        }
        assert!(f.check("tx_before_rotate", "c1", "contract"));
    }

    #[test]
    fn fill_ratio_resets_after_rotation() {
        let f = EventBloomFilter::with_fill_threshold(10, 0.01, 0.5).unwrap();
        for i in 0..8u32 {
            f.set(&format!("tx{i}"), "c1", "contract");
        }
        assert!(f.rotation_count() >= 1);
        assert!(f.fill_ratio() < 0.5, "fill ratio should reset after rotation");
    }

    #[test]
    fn memory_bytes_positive() {
        let f = EventBloomFilter::new(10_000, 0.001).unwrap();
        assert!(f.memory_bytes() > 0);
    }

    #[test]
    fn estimate_memory_bytes_scales_with_capacity() {
        let small = EventBloomFilter::estimate_memory_bytes(1_000, 0.01);
        let large = EventBloomFilter::estimate_memory_bytes(100_000, 0.01);
        assert!(large > small);
    }

    #[test]
    fn estimate_memory_bytes_invalid_inputs_return_zero() {
        assert_eq!(EventBloomFilter::estimate_memory_bytes(0, 0.01), 0);
        assert_eq!(EventBloomFilter::estimate_memory_bytes(1000, 0.0), 0);
        assert_eq!(EventBloomFilter::estimate_memory_bytes(1000, 1.0), 0);
    }

    // ── Issue #996: fill-ratio and rotation tests ─────────────────────────

    #[test]
    fn fill_ratio_zero_on_new_filter() {
        let f = EventBloomFilter::new(10_000, 0.001).unwrap();
        assert_eq!(f.fill_ratio(), 0.0);
        assert_eq!(f.rotation_count(), 0);
    }

    #[test]
    fn fill_ratio_increases_with_inserts() {
        let f = EventBloomFilter::new(1000, 0.001).unwrap();
        f.set("tx1", "c1", "contract");
        assert!(f.fill_ratio() > 0.0);
    }

    #[test]
    fn rotation_triggered_when_fill_threshold_exceeded() {
        // Use a very small capacity so we hit the threshold quickly.
        let f = EventBloomFilter::with_fill_threshold(10, 0.01, 0.5).unwrap();
        // Insert 6 items (> 50% of 10) to trigger rotation.
        for i in 0..6u32 {
            f.set(&format!("tx{i}"), "c1", "contract");
        }
        assert!(f.rotation_count() >= 1, "rotation should have fired");
    }

    #[test]
    fn entries_still_found_after_rotation() {
        // After rotation the previous filter should still answer `check` positively.
        let f = EventBloomFilter::with_fill_threshold(10, 0.01, 0.5).unwrap();
        f.set("tx_before_rotate", "c1", "contract");
        // Force a rotation by filling past threshold.
        for i in 0..6u32 {
            f.set(&format!("tx{i}"), "c1", "contract");
        }
        // The pre-rotation entry is in `previous` and must still be found.
        assert!(f.check("tx_before_rotate", "c1", "contract"));
    }

    #[test]
    fn fill_ratio_resets_after_rotation() {
        let f = EventBloomFilter::with_fill_threshold(10, 0.01, 0.5).unwrap();
        for i in 0..8u32 {
            f.set(&format!("tx{i}"), "c1", "contract");
        }
        assert!(f.rotation_count() >= 1);
        // After rotation, insert_count resets so fill_ratio is low.
        assert!(f.fill_ratio() < 0.5, "fill ratio should reset after rotation");
    }

    #[test]
    fn memory_bytes_positive() {
        let f = EventBloomFilter::new(10_000, 0.001).unwrap();
        assert!(f.memory_bytes() > 0);
    }

    #[test]
    fn estimate_memory_bytes_scales_with_capacity() {
        let small = EventBloomFilter::estimate_memory_bytes(1_000, 0.01);
        let large = EventBloomFilter::estimate_memory_bytes(100_000, 0.01);
        assert!(large > small);
    }

    #[test]
    fn estimate_memory_bytes_invalid_inputs_return_zero() {
        assert_eq!(EventBloomFilter::estimate_memory_bytes(0, 0.01), 0);
        assert_eq!(EventBloomFilter::estimate_memory_bytes(1000, 0.0), 0);
        assert_eq!(EventBloomFilter::estimate_memory_bytes(1000, 1.0), 0);
    }

    // ── Issue #615: SessionBloomFilter tests ─────────────────────────────────

    fn make_session_filter() -> SessionBloomFilter {
        SessionBloomFilter::new(10_000, 0.001).unwrap()
    }

    #[test]
    fn session_filter_first_event_not_duplicate() {
        let f = make_session_filter();
        assert!(!f.check_and_set("tx1", "c1", "contract", 100));
    }

    #[test]
    fn session_filter_second_same_event_is_duplicate() {
        let f = make_session_filter();
        f.check_and_set("tx1", "c1", "contract", 100);
        assert!(f.check_and_set("tx1", "c1", "contract", 100));
    }

    #[test]
    fn session_filter_different_tx_hash_not_duplicate() {
        let f = make_session_filter();
        f.check_and_set("tx1", "c1", "contract", 100);
        assert!(!f.check_and_set("tx2", "c1", "contract", 100));
    }

    #[test]
    fn session_filter_resets_on_new_ledger() {
        let f = make_session_filter();
        // Set in ledger 100
        f.check_and_set("tx1", "c1", "contract", 100);
        assert!(f.check_and_set("tx1", "c1", "contract", 100)); // duplicate

        // Advance to ledger 101 — filter resets, event is no longer cached
        assert!(!f.check_and_set("tx1", "c1", "contract", 101));
    }

    #[test]
    fn session_filter_same_ledger_detects_dups_across_calls() {
        let f = make_session_filter();
        for _ in 0..3 {
            f.check_and_set("txA", "cA", "contract", 50);
        }
        // After the first call above, subsequent ones should all be duplicates.
        // The first call returns false; the next two return true.
        // We can verify by resetting and doing a controlled sequence:
        let f2 = make_session_filter();
        let first = f2.check_and_set("txA", "cA", "contract", 50);
        let second = f2.check_and_set("txA", "cA", "contract", 50);
        assert!(!first);
        assert!(second);
    }

    // ── Auto-sizing tests ────────────────────────────────────────────────

    #[test]
    fn auto_size_zero_items_returns_defaults() {
        let (cap, fp) = auto_size_bloom_filter(0, 0.001);
        assert_eq!(cap, 1_000_000);
        assert_eq!(fp, 0.001);
    }

    #[test]
    fn auto_size_scales_capacity_by_10x() {
        let (cap, _fp) = auto_size_bloom_filter(10_000, 0.001);
        assert_eq!(cap, 100_000);
    }

    #[test]
    fn auto_size_high_daily_volume() {
        let (cap, fp) = auto_size_bloom_filter(100_000, 0.001);
        assert_eq!(cap, 1_000_000);
        assert_eq!(fp, 0.001);
    }

    #[test]
    fn auto_size_clamps_fp_rate() {
        let (_, fp_low) = auto_size_bloom_filter(10_000, 0.00001);
        assert_eq!(fp_low, 0.0001); // clamped to min

        let (_, fp_high) = auto_size_bloom_filter(10_000, 0.1);
        assert_eq!(fp_high, 0.01); // clamped to max
    }

    #[test]
    fn estimate_fp_rate_at_zero_fill_ratio_returns_target() {
        let est = estimate_false_positive_rate(0.001, 0.0);
        assert_eq!(est, 0.001);
    }

    #[test]
    fn estimate_fp_rate_at_full_capacity_higher_than_target() {
        let est = estimate_false_positive_rate(0.001, 1.0);
        assert!(est >= 0.001);
        assert!(est <= 1.0);
    }

    #[test]
    fn estimate_fp_rate_at_half_fill_shows_degradation() {
        let target = 0.001;
        let est_half = estimate_false_positive_rate(target, 0.5);
        let est_full = estimate_false_positive_rate(target, 1.0);
        // At half fill, FP rate should be better than at full
        assert!(est_half < est_full);
    }
}
