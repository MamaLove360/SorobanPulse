/// Priority-Based Webhook Delivery (Issue: priority-based webhook delivery for critical events)
///
/// This module implements a priority queue for webhook deliveries so that critical
/// events (e.g. security incidents, payment failures) are delivered ahead of
/// routine/low-priority notifications when the delivery pipeline is under load.
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Priority levels for webhook delivery, ordered from lowest to highest urgency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[serde(rename_all = "snake_case")]
pub enum WebhookPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

impl Default for WebhookPriority {
    fn default() -> Self {
        WebhookPriority::Normal
    }
}

impl WebhookPriority {
    /// Maximum time (ms) a delivery at this priority is allowed to wait in queue
    /// before it is considered a priority violation.
    pub fn max_wait_ms(&self) -> u64 {
        match self {
            WebhookPriority::Critical => 1_000,
            WebhookPriority::High => 5_000,
            WebhookPriority::Normal => 30_000,
            WebhookPriority::Low => 120_000,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            WebhookPriority::Critical => "critical",
            WebhookPriority::High => "high",
            WebhookPriority::Normal => "normal",
            WebhookPriority::Low => "low",
        }
    }
}

/// A configurable rule that maps an event type / attribute to a priority level.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityRule {
    /// Event type pattern, e.g. "security.*" or "payment.failed".
    pub event_type_pattern: String,
    pub priority: WebhookPriority,
}

/// Ordered set of rules evaluated top-to-bottom; first match wins.
#[derive(Debug, Clone, Default)]
pub struct PriorityRuleSet {
    rules: Vec<PriorityRule>,
    default_priority: WebhookPriority,
}

impl PriorityRuleSet {
    pub fn new(default_priority: WebhookPriority) -> Self {
        Self {
            rules: Vec::new(),
            default_priority,
        }
    }

    pub fn add_rule(&mut self, pattern: impl Into<String>, priority: WebhookPriority) -> &mut Self {
        self.rules.push(PriorityRule {
            event_type_pattern: pattern.into(),
            priority,
        });
        self
    }

    /// Resolve the priority for an event type. Supports a trailing "*" wildcard.
    pub fn resolve(&self, event_type: &str) -> WebhookPriority {
        for rule in &self.rules {
            if Self::matches(&rule.event_type_pattern, event_type) {
                return rule.priority;
            }
        }
        self.default_priority
    }

    fn matches(pattern: &str, event_type: &str) -> bool {
        if let Some(prefix) = pattern.strip_suffix('*') {
            event_type.starts_with(prefix)
        } else {
            pattern == event_type
        }
    }
}

/// A single webhook delivery task tracked by the priority queue.
#[derive(Debug, Clone)]
pub struct WebhookDeliveryTask {
    pub id: Uuid,
    pub webhook_url: String,
    pub payload: serde_json::Value,
    pub priority: WebhookPriority,
    pub enqueued_at_ms: u64,
    /// Monotonically increasing sequence number used as a tiebreaker so that
    /// tasks of equal priority remain FIFO.
    sequence: u64,
}

impl WebhookDeliveryTask {
    pub fn new(webhook_url: impl Into<String>, payload: serde_json::Value, priority: WebhookPriority) -> Self {
        Self {
            id: Uuid::new_v4(),
            webhook_url: webhook_url.into(),
            payload,
            priority,
            enqueued_at_ms: now_ms(),
            sequence: 0,
        }
    }

    pub fn wait_ms(&self) -> u64 {
        now_ms().saturating_sub(self.enqueued_at_ms)
    }

    /// True if this task has been waiting longer than its priority allows.
    pub fn is_violation(&self) -> bool {
        self.wait_ms() > self.priority.max_wait_ms()
    }
}

impl PartialEq for WebhookDeliveryTask {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
    }
}
impl Eq for WebhookDeliveryTask {}

impl Ord for WebhookDeliveryTask {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher priority first; for equal priority, lower sequence (older) first.
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

impl PartialOrd for WebhookDeliveryTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Metrics snapshot for the priority queue.
#[derive(Debug, Clone, Default, Serialize)]
pub struct PriorityQueueMetrics {
    pub total_enqueued: u64,
    pub total_dequeued: u64,
    pub violations: u64,
    pub enqueued_by_priority: [u64; 4],
    pub current_depth: usize,
}

/// Thread-safe priority queue for webhook deliveries.
pub struct WebhookPriorityQueue {
    heap: Mutex<BinaryHeap<WebhookDeliveryTask>>,
    sequence: Mutex<u64>,
    metrics: Mutex<PriorityQueueMetrics>,
}

impl Default for WebhookPriorityQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl WebhookPriorityQueue {
    pub fn new() -> Self {
        Self {
            heap: Mutex::new(BinaryHeap::new()),
            sequence: Mutex::new(0),
            metrics: Mutex::new(PriorityQueueMetrics::default()),
        }
    }

    pub fn push(&self, mut task: WebhookDeliveryTask) {
        let mut seq = self.sequence.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        *seq += 1;
        task.sequence = *seq;
        drop(seq);

        let mut metrics = self.metrics.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        metrics.total_enqueued += 1;
        metrics.enqueued_by_priority[task.priority as usize] += 1;
        drop(metrics);

        let mut heap = self.heap.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        heap.push(task);
        self.metrics.lock().unwrap_or_else(std::sync::PoisonError::into_inner).current_depth = heap.len();
    }

    /// Pop the highest priority task, recording a violation metric if it
    /// breached its max wait time (used to drive alerting).
    pub fn pop(&self) -> Option<WebhookDeliveryTask> {
        let mut heap = self.heap.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let task = heap.pop();
        let depth = heap.len();
        drop(heap);

        if let Some(ref t) = task {
            let mut metrics = self.metrics.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            metrics.total_dequeued += 1;
            metrics.current_depth = depth;
            if t.is_violation() {
                metrics.violations += 1;
                crate::metrics::record_priority_violation(t.priority.as_str());
            }
            crate::metrics::record_priority_dequeue(t.priority.as_str(), t.wait_ms());
        }

        task
    }

    pub fn len(&self) -> usize {
        self.heap.lock().unwrap_or_else(std::sync::PoisonError::into_inner).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn metrics(&self) -> PriorityQueueMetrics {
        self.metrics.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    /// Scan the queue for tasks currently in violation without removing them.
    /// Intended to be polled periodically to drive alerting.
    pub fn violating_tasks(&self) -> Vec<Uuid> {
        self.heap
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter(|t| t.is_violation())
            .map(|t| t.id)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn higher_priority_dequeued_first() {
        let queue = WebhookPriorityQueue::new();
        queue.push(WebhookDeliveryTask::new("https://a", json!({}), WebhookPriority::Low));
        queue.push(WebhookDeliveryTask::new("https://b", json!({}), WebhookPriority::Critical));
        queue.push(WebhookDeliveryTask::new("https://c", json!({}), WebhookPriority::Normal));

        assert_eq!(queue.pop().unwrap().priority, WebhookPriority::Critical);
        assert_eq!(queue.pop().unwrap().priority, WebhookPriority::Normal);
        assert_eq!(queue.pop().unwrap().priority, WebhookPriority::Low);
        assert!(queue.pop().is_none());
    }

    #[test]
    fn equal_priority_is_fifo() {
        let queue = WebhookPriorityQueue::new();
        queue.push(WebhookDeliveryTask::new("https://first", json!({}), WebhookPriority::Normal));
        queue.push(WebhookDeliveryTask::new("https://second", json!({}), WebhookPriority::Normal));

        assert_eq!(queue.pop().unwrap().webhook_url, "https://first");
        assert_eq!(queue.pop().unwrap().webhook_url, "https://second");
    }

    #[test]
    fn rule_set_resolves_wildcard() {
        let mut rules = PriorityRuleSet::new(WebhookPriority::Normal);
        rules.add_rule("security.*", WebhookPriority::Critical);
        rules.add_rule("payment.failed", WebhookPriority::High);

        assert_eq!(rules.resolve("security.breach"), WebhookPriority::Critical);
        assert_eq!(rules.resolve("payment.failed"), WebhookPriority::High);
        assert_eq!(rules.resolve("user.updated"), WebhookPriority::Normal);
    }

    #[test]
    fn metrics_track_enqueue_and_dequeue() {
        let queue = WebhookPriorityQueue::new();
        queue.push(WebhookDeliveryTask::new("https://a", json!({}), WebhookPriority::Critical));
        queue.pop();

        let metrics = queue.metrics();
        assert_eq!(metrics.total_enqueued, 1);
        assert_eq!(metrics.total_dequeued, 1);
        assert_eq!(metrics.current_depth, 0);
    }

    #[test]
    fn violation_detection() {
        let mut task = WebhookDeliveryTask::new("https://a", json!({}), WebhookPriority::Critical);
        task.enqueued_at_ms = 0; // force a huge wait
        assert!(task.is_violation());
    }

    #[test]
    fn max_wait_ms_ordering_matches_urgency() {
        assert!(WebhookPriority::Critical.max_wait_ms() < WebhookPriority::High.max_wait_ms());
        assert!(WebhookPriority::High.max_wait_ms() < WebhookPriority::Normal.max_wait_ms());
        assert!(WebhookPriority::Normal.max_wait_ms() < WebhookPriority::Low.max_wait_ms());
    }
}
