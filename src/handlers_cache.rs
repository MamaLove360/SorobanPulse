//! Helper utilities for serialization cache integration in event handlers.
//!
//! This module provides cache key generation and lookup patterns for hot
//! event fetches (Issue #959).

use serde_json::Value;

/// Generates a cache key for an event query result.
///
/// Key format: `events:{contract_id}:{event_type}:{page}:{limit}`
/// This enables per-contract invalidation when events are indexed.
pub fn event_cache_key(
    contract_id: Option<&str>,
    event_type: Option<&str>,
    page: i64,
    limit: i64,
) -> String {
    let cid = contract_id.unwrap_or("*");
    let et = event_type.unwrap_or("*");
    format!("events:{}:{}:{}:{}", cid, et, page, limit)
}

/// Generates an entity type key for invalidation purposes.
///
/// Used to invalidate all results for a contract when new events are indexed.
pub fn event_entity_type(contract_id: Option<&str>) -> String {
    let cid = contract_id.unwrap_or("*");
    format!("event:{}", cid)
}

/// Attempts to retrieve a cached serialized response, falling back to serialization on miss.
pub async fn get_or_serialize_events(
    cache: &std::sync::Arc<crate::serialization_cache::SerializedEventCache>,
    contract_id: Option<&str>,
    event_type: Option<&str>,
    page: i64,
    limit: i64,
    value: &Value,
) -> Result<Vec<u8>, serde_json::Error> {
    let key = event_cache_key(contract_id, event_type, page, limit);
    let entity_type = event_entity_type(contract_id);

    cache
        .get_or_serialize_entity(&entity_type, &key, value, |v| {
            serde_json::to_vec(v)
        })
        .await
}

/// Serializes event data without caching (for uncacheable queries).
pub async fn serialize_events_uncached(value: &Value) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_key_format() {
        let key = event_cache_key(Some("CABC123"), Some("contract"), 1, 20);
        assert_eq!(key, "events:CABC123:contract:1:20");

        let key = event_cache_key(None, None, 0, 100);
        assert_eq!(key, "events:*:*:0:100");
    }

    #[test]
    fn entity_type_format() {
        let et = event_entity_type(Some("CABC123"));
        assert_eq!(et, "event:CABC123");

        let et = event_entity_type(None);
        assert_eq!(et, "event:*");
    }
}
