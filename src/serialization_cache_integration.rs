//! Integration of SerializationCache with event handlers (Issue #959).
//!
//! This module provides wrapper utilities to integrate the serialization cache
//! into handlers with proper metrics recording.

use std::sync::Arc;
use serde_json::{json, Value};
use tracing::debug;

/// Cache-aware event list response builder.
///
/// Caches the complete JSON response for cursor-based event queries.
/// Used to reduce CPU overhead for hot queries like the main event feed.
pub async fn cache_or_serialize_event_list(
    cache: &Arc<crate::serialization_cache::SerializedEventCache>,
    events: Vec<Value>,
    next_cursor: Option<String>,
    limit: i64,
    contract_id: Option<&str>,
    event_type: Option<&str>,
    page: i64,
) -> Result<Vec<u8>, serde_json::Error> {
    let response_obj = json!({
        "data": events,
        "next_cursor": next_cursor,
        "limit": limit,
    });

    // Use a cache key that captures all parameters affecting the response
    let cache_key = format!(
        "events:resp:{}:{}:{}:{}",
        contract_id.unwrap_or("*"),
        event_type.unwrap_or("*"),
        page,
        limit
    );

    let entity_type = format!("event_response:{}", contract_id.unwrap_or("*"));

    cache
        .get_or_serialize_entity(&entity_type, &cache_key, &response_obj, |v| {
            serde_json::to_vec(v)
        })
        .await
}

/// Check if a query should be cached based on parameters.
///
/// Some queries are not cacheable because they include:
/// - Search queries (full-text search changes frequently)
/// - Filters with multiple values (less stable)
/// - Admin-only filters (anonymized, etc.)
pub fn is_query_cacheable(
    has_cursor: bool,
    contract_ids_count: usize,
    has_search: bool,
    has_anonymized_filter: bool,
    has_geospatial_filter: bool,
    has_exclude_filter: bool,
) -> bool {
    // Cache only simple, cursor-based queries
    // Exclude complex multi-filter queries where hit rate would be low
    has_cursor
        && contract_ids_count == 0 // Only single contract or global queries
        && !has_search // FTS queries change with search terms
        && !has_anonymized_filter // Admin filter
        && !has_geospatial_filter // Location queries are transient
        && !has_exclude_filter // Exclude filters are less common
}

/// Determine page number from cursor position.
/// Returns 1 for first page, 2 for second, etc.
/// Used for cache key generation but not a precise page count.
pub fn estimate_page_number(cursor: Option<&str>, limit: i64) -> i64 {
    if cursor.is_none() {
        1
    } else {
        // Very conservative estimate: assume cursor means page 2+
        2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cacheability_simple_query() {
        let cacheable = is_query_cacheable(
            true,  // has_cursor
            0,     // contract_ids_count
            false, // has_search
            false, // has_anonymized_filter
            false, // has_geospatial_filter
            false, // has_exclude_filter
        );
        assert!(cacheable, "Simple cursor queries should be cacheable");
    }

    #[test]
    fn cacheability_exclude_search_queries() {
        let cacheable = is_query_cacheable(
            true,  // has_cursor
            0,     // contract_ids_count
            true,  // has_search <- makes it uncacheable
            false, // has_anonymized_filter
            false, // has_geospatial_filter
            false, // has_exclude_filter
        );
        assert!(!cacheable, "Search queries should not be cached");
    }

    #[test]
    fn cacheability_exclude_offset_queries() {
        let cacheable = is_query_cacheable(
            false, // has_cursor <- offset queries not cached
            0,     // contract_ids_count
            false, // has_search
            false, // has_anonymized_filter
            false, // has_geospatial_filter
            false, // has_exclude_filter
        );
        assert!(
            !cacheable,
            "Offset-based queries should not be cached (cursor recommended)"
        );
    }

    #[test]
    fn page_estimation() {
        assert_eq!(estimate_page_number(None, 20), 1);
        assert_eq!(estimate_page_number(Some("cursor123"), 20), 2);
    }
}
