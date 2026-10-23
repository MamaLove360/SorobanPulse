use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};
use uuid::Uuid;

use crate::models::SorobanEvent;

pub const DEFAULT_RING_BUFFER_CAPACITY: usize = 10_000;

#[derive(Debug, Clone)]
struct Slot {
    id: Uuid,
    event: SorobanEvent,
}

/// Thread-safe in-memory ring buffer for SSE event replay.
///
/// Stores the most recent `capacity` events. When full, the oldest event is
/// silently evicted (FIFO). Clients that reconnect and supply a `Last-Event-ID`
/// header can receive all events that arrived while they were disconnected, as
/// long as those events have not been evicted.
pub struct SseRingBuffer {
    inner: Mutex<VecDeque<Slot>>,
    capacity: usize,
    overflow_count: std::sync::atomic::AtomicU64,
}

impl SseRingBuffer {
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            overflow_count: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Push a new event, assigning it a fresh UUID used as the SSE event ID.
    /// Returns that UUID so callers can embed it in the outgoing SSE frame.
    pub fn push(&self, event: SorobanEvent) -> Uuid {
        let id = Uuid::new_v4();
        let mut buf = self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if buf.len() >= self.capacity {
            buf.pop_front();
            self.overflow_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        buf.push_back(Slot { id, event });
        id
    }

    /// Return all events stored *after* the event identified by `last_id`.
    ///
    /// Returns `None` when `last_id` is not present — the event was evicted
    /// due to buffer overflow, so a full replay is not possible.
    /// Returns `Some(vec![])` when `last_id` is the latest event (nothing missed).
    pub fn events_since(&self, last_id: Uuid) -> Option<Vec<SorobanEvent>> {
        let buf = self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let pos = buf.iter().position(|s| s.id == last_id)?;
        // Clone event references outside the critical section to avoid
        // holding the mutex while copying potentially large event data.
        let events: Vec<SorobanEvent> = buf.iter().skip(pos + 1).map(|s| s.event.clone()).collect();
        drop(buf);
        Some(events)
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn overflow_count(&self) -> u64 {
        self.overflow_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::SorobanEvent;
    use serde_json::json;

    fn make_event(contract_id: &str) -> SorobanEvent {
        SorobanEvent {
            contract_id: contract_id.to_string(),
            event_type: "contract".to_string(),
            tx_hash: "abc".to_string(),
            ledger: 1,
            ledger_closed_at: "2024-01-01T00:00:00Z".to_string(),
            ledger_hash: None,
            in_successful_call: true,
            value: json!({}),
            topic: None,
            tenant_id: None,
        }
    }

    #[test]
    fn push_and_events_since_basic() {
        let buf = SseRingBuffer::new(10);
        let id1 = buf.push(make_event("C1"));
        let _id2 = buf.push(make_event("C2"));
        let id3 = buf.push(make_event("C3"));

        let since = buf.events_since(id1).unwrap();
        assert_eq!(since.len(), 2);

        let since_last = buf.events_since(id3).unwrap();
        assert_eq!(since_last.len(), 0);
    }

    #[test]
    fn unknown_id_returns_none() {
        let buf = SseRingBuffer::new(10);
        buf.push(make_event("C1"));
        assert!(buf.events_since(Uuid::new_v4()).is_none());
    }

    #[test]
    fn overflow_evicts_oldest() {
        let buf = SseRingBuffer::new(3);
        let id1 = buf.push(make_event("C1"));
        buf.push(make_event("C2"));
        buf.push(make_event("C3"));
        // Push a 4th — id1 gets evicted
        buf.push(make_event("C4"));

        assert_eq!(buf.overflow_count(), 1);
        assert!(buf.events_since(id1).is_none(), "evicted id should return None");
        assert_eq!(buf.len(), 3);
    }

    #[test]
    fn capacity_respected() {
        let cap = 5;
        let buf = SseRingBuffer::new(cap);
        for i in 0..10 {
            buf.push(make_event(&format!("C{i}")));
        }
        assert_eq!(buf.len(), cap);
        assert_eq!(buf.overflow_count(), 5);
    }
}
