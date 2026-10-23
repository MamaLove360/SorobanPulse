/// Central cost aggregator.
///
/// `CostCalculator` collects per-resource cost entries, computes a
/// combined breakdown, and surfaces efficiency metrics.
use super::{
    compute::{self, ComputeRates, ComputeUsage},
    database::{self, DatabaseRates, DatabaseUsage},
    models::{
        CostBreakdown, CostEntry, CostMetrics, CostReport, ResourceType,
    },
};
use chrono::Utc;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Logical component that incurred a cost. Used to group the cost report
/// by subsystem (indexer, API, webhook delivery, email delivery).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CostComponent {
    Indexer,
    Api,
    Webhooks,
    Email,
}

impl CostComponent {
    /// Stable string identifier used in reports and dashboards.
    pub fn as_str(&self) -> &'static str {
        match self {
            CostComponent::Indexer => "indexer",
            CostComponent::Api => "api",
            CostComponent::Webhooks => "webhooks",
            CostComponent::Email => "email",
        }
    }
}

impl std::fmt::Display for CostComponent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Per-component cost totals included in the cost report.
#[derive(Debug, Clone, Default)]
pub struct ComponentCosts {
    pub indexer: f64,
    pub api: f64,
    pub webhooks: f64,
    pub email: f64,
}

impl ComponentCosts {
    pub fn total(&self) -> f64 {
        self.indexer + self.api + self.webhooks + self.email
    }

    pub fn get(&self, component: CostComponent) -> f64 {
        match component {
            CostComponent::Indexer => self.indexer,
            CostComponent::Api => self.api,
            CostComponent::Webhooks => self.webhooks,
            CostComponent::Email => self.email,
        }
    }

    pub fn add(&mut self, component: CostComponent, cost: f64) {
        match component {
            CostComponent::Indexer => self.indexer += cost,
            CostComponent::Api => self.api += cost,
            CostComponent::Webhooks => self.webhooks += cost,
            CostComponent::Email => self.email += cost,
        }
    }

    /// Convert to a map keyed by component name for serialization.
    pub fn as_map(&self) -> HashMap<String, f64> {
        let mut map = HashMap::new();
        map.insert(CostComponent::Indexer.as_str().to_string(), self.indexer);
        map.insert(CostComponent::Api.as_str().to_string(), self.api);
        map.insert(CostComponent::Webhooks.as_str().to_string(), self.webhooks);
        map.insert(CostComponent::Email.as_str().to_string(), self.email);
        map
    }
}

/// Thread-safe, shared cost calculator.
#[derive(Clone)]
pub struct CostCalculator {
    inner: Arc<RwLock<Inner>>,
}

struct Inner {
    entries: Vec<CostEntry>,
    /// Parallel to `entries`: the component that incurred each entry's cost.
    entry_components: Vec<CostComponent>,
    db_rates: DatabaseRates,
    compute_rates: ComputeRates,
    event_count: u64,
    request_count: u64,
}

impl CostCalculator {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                entries: Vec::new(),
                entry_components: Vec::new(),
                db_rates: DatabaseRates::default(),
                compute_rates: ComputeRates::default(),
                event_count: 0,
                request_count: 0,
            })),
        }
    }

    /// Construct a calculator sharing the same underlying state as `self`.
    ///
    /// Useful for handing a clone to the indexer, webhook, and email paths so
    /// that all of them attribute costs to the same aggregator.
    pub fn shared(&self) -> Self {
        self.clone()
    }

    /// Replace the default database pricing rates.
    pub fn with_db_rates(self, rates: DatabaseRates) -> Self {
        self.inner.write().unwrap_or_else(std::sync::PoisonError::into_inner).db_rates = rates;
        self
    }

    /// Replace the default compute pricing rates.
    pub fn with_compute_rates(self, rates: ComputeRates) -> Self {
        self.inner.write().unwrap_or_else(std::sync::PoisonError::into_inner).compute_rates = rates;
        self
    }

    /// Record database cost for the given usage snapshot over `hours`.
    pub fn record_database(&self, usage: &DatabaseUsage, hours: f64) {
        self.record_database_for(CostComponent::Api, usage, hours);
    }

    /// Record database cost attributed to a specific component.
    pub fn record_database_for(
        &self,
        component: CostComponent,
        usage: &DatabaseUsage,
        hours: f64,
    ) {
        let entry = {
            let inner = self.inner.read().unwrap_or_else(std::sync::PoisonError::into_inner);
            let db_cost = database::calculate(usage, &inner.db_rates, hours);
            database::to_cost_entry(&db_cost, "primary-database")
        };
        self.push_entry(component, entry);
    }

    /// Record compute cost for the given usage snapshot over `hours`.
    pub fn record_compute(&self, usage: &ComputeUsage, hours: f64) {
        self.record_compute_for(CostComponent::Api, usage, hours);
    }

    /// Record compute cost attributed to a specific component.
    pub fn record_compute_for(
        &self,
        component: CostComponent,
        usage: &ComputeUsage,
        hours: f64,
    ) {
        let entry = {
            let inner = self.inner.read().unwrap_or_else(std::sync::PoisonError::into_inner);
            let compute_cost = compute::calculate(usage, &inner.compute_rates, hours);
            compute::to_cost_entry(&compute_cost, "application-server")
        };
        self.push_entry(component, entry);
    }

    /// Record the cost of `count` RPC calls attributed to `component`.
    ///
    /// `cost_per_call` is the provider's per-request price; the total is
    /// recorded as a network cost entry so it rolls into the breakdown.
    pub fn record_rpc_call(
        &self,
        component: CostComponent,
        resource_id: &str,
        count: u64,
        cost_per_call: f64,
    ) {
        if count == 0 {
            return;
        }
        let entry = CostEntry {
            timestamp: Utc::now(),
            resource_type: ResourceType::Network,
            resource_id: resource_id.to_string(),
            cost: cost_per_call * count as f64,
            quantity: count as f64,
            unit: "rpc_call".to_string(),
        };
        self.push_entry(component, entry);
    }

    /// Record the cost of delivering `count` notifications attributed to
    /// `component` (e.g. webhook or email delivery).
    pub fn record_notification(
        &self,
        component: CostComponent,
        resource_id: &str,
        count: u64,
        cost_per_delivery: f64,
    ) {
        if count == 0 {
            return;
        }
        let entry = CostEntry {
            timestamp: Utc::now(),
            resource_type: ResourceType::Network,
            resource_id: resource_id.to_string(),
            cost: cost_per_delivery * count as f64,
            quantity: count as f64,
            unit: "notification".to_string(),
        };
        self.push_entry(component, entry);
    }

    /// Add an arbitrary pre-computed cost entry (e.g. network, storage).
    ///
    /// The entry is attributed to the API component by default; use
    /// [`record_entry_for`](Self::record_entry_for) to attribute it elsewhere.
    pub fn record_entry(&self, entry: CostEntry) {
        self.push_entry(CostComponent::Api, entry);
    }

    /// Add an arbitrary pre-computed cost entry attributed to `component`.
    pub fn record_entry_for(&self, component: CostComponent, entry: CostEntry) {
        self.push_entry(component, entry);
    }

    fn push_entry(&self, component: CostComponent, entry: CostEntry) {
        let mut inner = self.inner.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        inner.entries.push(entry);
        inner.entry_components.push(component);
    }

    /// Update the running event and request counters (used for efficiency metrics).
    pub fn update_counters(&self, events: u64, requests: u64) {
        let mut inner = self.inner.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        inner.event_count += events;
        inner.request_count += requests;
    }

    /// Compute a cost breakdown aggregated across all recorded entries.
    pub fn breakdown(&self) -> CostBreakdown {
        let inner = self.inner.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        build_breakdown(&inner.entries)
    }

    /// Compute per-component cost totals across all recorded entries.
    pub fn component_costs(&self) -> ComponentCosts {
        let inner = self.inner.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        build_component_costs(&inner.entries, &inner.entry_components)
    }

    /// Snapshot of all recorded entries (cloned), used by the report and
    /// forecast code paths.
    pub fn entries(&self) -> Vec<CostEntry> {
        self.inner.read().unwrap_or_else(std::sync::PoisonError::into_inner).entries.clone()
    }

    /// Snapshot of all recorded entries paired with the component that
    /// incurred them.
    pub fn entries_with_components(&self) -> Vec<(CostComponent, CostEntry)> {
        let inner = self.inner.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        inner
            .entries
            .iter()
            .cloned()
            .zip(inner.entry_components.iter().copied())
            .map(|(entry, component)| (component, entry))
            .collect()
    }

    /// Generate a cost report over the entries recorded since the given start time.
    pub fn report(&self, period_start: chrono::DateTime<Utc>) -> CostReport {
        let inner = self.inner.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut period_entries: Vec<CostEntry> = Vec::new();
        let mut period_components: Vec<CostComponent> = Vec::new();
        for (entry, component) in inner.entries.iter().zip(inner.entry_components.iter()) {
            if entry.timestamp >= period_start {
                period_entries.push(entry.clone());
                period_components.push(*component);
            }
        }

        let breakdown = build_breakdown(&period_entries);
        let by_component = build_component_costs(&period_entries, &period_components);

        let mut by_resource_type: HashMap<ResourceType, f64> = HashMap::new();
        for e in &period_entries {
            *by_resource_type.entry(e.resource_type).or_insert(0.0) += e.cost;
        }

        let mut resource_totals: HashMap<&str, f64> = HashMap::new();
        for e in &period_entries {
            *resource_totals.entry(e.resource_id.as_str()).or_insert(0.0) += e.cost;
        }
        let mut top_resources: Vec<(String, f64)> = resource_totals
            .into_iter()
            .map(|(id, cost)| (id.to_string(), cost))
            .collect();
        top_resources.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        top_resources.truncate(10);

        CostReport {
            period_start,
            period_end: Utc::now(),
            breakdown,
            by_resource_type,
            by_component: by_component.as_map(),
            top_resources,
        }
    }

    /// Compute efficiency metrics (cost-per-event, cost-per-request, etc.).
    pub fn metrics(&self) -> CostMetrics {
        let inner = self.inner.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        let breakdown = build_breakdown(&inner.entries);
        let total = breakdown.total;

        let cost_per_event = if inner.event_count > 0 {
            total / inner.event_count as f64
        } else {
            0.0
        };

        let cost_per_request = if inner.request_count > 0 {
            total / inner.request_count as f64
        } else {
            0.0
        };

        // Derive hourly rate from the time span of recorded entries.
        let hourly_rate = hourly_rate_from_entries(&inner.entries);

        CostMetrics {
            total_cost: total,
            cost_per_event,
            cost_per_request,
            hourly_rate,
            daily_average: hourly_rate * 24.0,
        }
    }

    /// Remove all entries older than `retain_hours` hours.
    pub fn prune_older_than(&self, retain_hours: i64) {
        let cutoff = Utc::now() - chrono::Duration::hours(retain_hours);
        let mut inner = self.inner.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        let Inner {
            entries,
            entry_components,
            ..
        } = &mut *inner;
        let (kept_entries, kept_components): (Vec<_>, Vec<_>) = entries
            .drain(..)
            .zip(entry_components.drain(..))
            .filter(|(e, _)| e.timestamp >= cutoff)
            .unzip();
        *entries = kept_entries;
        *entry_components = kept_components;
    }

    /// Total number of recorded cost entries.
    pub fn entry_count(&self) -> usize {
        self.inner.read().unwrap_or_else(std::sync::PoisonError::into_inner).entries.len()
    }
}

impl Default for CostCalculator {
    fn default() -> Self {
        Self::new()
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────

fn build_breakdown(entries: &[CostEntry]) -> CostBreakdown {
    let mut database = 0.0f64;
    let mut compute = 0.0f64;
    let mut storage = 0.0f64;
    let mut network = 0.0f64;
    let mut details = HashMap::new();

    for entry in entries {
        match entry.resource_type {
            ResourceType::Database => database += entry.cost,
            ResourceType::Compute | ResourceType::Memory => compute += entry.cost,
            ResourceType::Storage => storage += entry.cost,
            ResourceType::Network => network += entry.cost,
        }
        // Keep the latest entry per resource_id for the details map.
        details.insert(entry.resource_id.clone(), entry.clone());
    }

    let total = database + compute + storage + network;

    CostBreakdown {
        timestamp: Utc::now(),
        database,
        compute,
        storage,
        network,
        total,
        details,
    }
}

fn build_component_costs(
    entries: &[CostEntry],
    components: &[CostComponent],
) -> ComponentCosts {
    let mut costs = ComponentCosts::default();
    for (entry, component) in entries.iter().zip(components.iter()) {
        costs.add(*component, entry.cost);
    }
    costs
}

fn hourly_rate_from_entries(entries: &[CostEntry]) -> f64 {
    let Some(oldest) = entries.iter().map(|e| e.timestamp).min() else {
        return 0.0;
    };
    let hours = (Utc::now() - oldest).num_minutes() as f64 / 60.0;
    if hours < 0.001 {
        return 0.0;
    }
    let total: f64 = entries.iter().map(|e| e.cost).sum();
    total / hours
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::costs::database::DatabaseUsage;
    use crate::costs::compute::ComputeUsage;

    #[test]
    fn empty_calculator_yields_zero_breakdown() {
        let calc = CostCalculator::new();
        let b = calc.breakdown();
        assert_eq!(b.total, 0.0);
    }

    #[test]
    fn records_database_and_compute() {
        let calc = CostCalculator::new();
        calc.record_database(
            &DatabaseUsage { query_count: 1000, active_connections: 1, ..Default::default() },
            1.0,
        );
        calc.record_compute(
            &ComputeUsage { vcpu_count: 1, cpu_utilization: 0.5, ..Default::default() },
            1.0,
        );
        let b = calc.breakdown();
        assert!(b.database > 0.0);
        assert!(b.compute > 0.0);
        assert!(b.total > 0.0);
    }

    #[test]
    fn shared_calculator_shares_state() {
        let calc = CostCalculator::new();
        let shared = calc.shared();
        shared.record_rpc_call(CostComponent::Indexer, "rpc-provider", 10, 0.001);
        assert_eq!(calc.entry_count(), 1);
        assert!(calc.component_costs().indexer > 0.0);
    }

    #[test]
    fn report_groups_by_component() {
        let calc = CostCalculator::new();
        calc.record_rpc_call(CostComponent::Indexer, "rpc-provider", 100, 0.001);
        calc.record_notification(CostComponent::Webhooks, "webhook-delivery", 5, 0.01);
        calc.record_notification(CostComponent::Email, "email-delivery", 3, 0.02);

        let report = calc.report(Utc::now() - chrono::Duration::hours(1));
        assert!(report.by_component.get("indexer").copied().unwrap_or(0.0) > 0.0);
        assert!(report.by_component.get("webhooks").copied().unwrap_or(0.0) > 0.0);
        assert!(report.by_component.get("email").copied().unwrap_or(0.0) > 0.0);
        assert_eq!(report.by_component.get("api").copied().unwrap_or(0.0), 0.0);
    }

    #[test]
    fn prune_keeps_components_aligned() {
        let calc = CostCalculator::new();
        calc.record_rpc_call(CostComponent::Indexer, "rpc-provider", 1, 0.001);
        calc.prune_older_than(1);
        let pairs = calc.entries_with_components();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, CostComponent::Indexer);
    }
}
