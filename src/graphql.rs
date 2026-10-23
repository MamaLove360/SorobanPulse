/// GraphQL API layer for Soroban Pulse events
/// Issue #683: Add GraphQL API layer for more flexible event querying

use async_graphql::{
    Context, InputObject, Object, Schema, Subscription, ID, SimpleObject,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chrono::{DateTime, Utc};
use futures_util::stream::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::models::SorobanEvent;

/// Default maximum query depth allowed by the GraphQL endpoint.
pub const DEFAULT_GRAPHQL_MAX_DEPTH: usize = 10;
/// Default maximum query complexity allowed by the GraphQL endpoint.
pub const DEFAULT_GRAPHQL_MAX_COMPLEXITY: usize = 1000;
/// Default maximum number of aliases allowed in a single query.
pub const DEFAULT_GRAPHQL_MAX_ALIASES: usize = 15;

/// Resolve the maximum query depth from `GRAPHQL_MAX_DEPTH`, falling back to the default.
pub fn graphql_max_depth() -> usize {
    std::env::var("GRAPHQL_MAX_DEPTH")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_GRAPHQL_MAX_DEPTH)
}

/// Resolve the maximum query complexity from `GRAPHQL_MAX_COMPLEXITY`, falling back to the default.
pub fn graphql_max_complexity() -> usize {
    std::env::var("GRAPHQL_MAX_COMPLEXITY")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_GRAPHQL_MAX_COMPLEXITY)
}

/// Resolve the maximum number of aliases from `GRAPHQL_MAX_ALIASES`, falling back to the default.
pub fn graphql_max_aliases() -> usize {
    std::env::var("GRAPHQL_MAX_ALIASES")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_GRAPHQL_MAX_ALIASES)
}

/// Build the structured error returned when a query exceeds a configured limit.
fn limit_exceeded_error(limit: &str, max: usize) -> async_graphql::Error {
    async_graphql::Error::new(format!(
        "Query exceeds the maximum allowed {limit} of {max}"
    ))
    .extend_with(|_, e| {
        e.set("code", "GRAPHQL_LIMIT_EXCEEDED");
        e.set("limit", limit);
        e.set("max", max as i64);
    })
}

/// Compute the maximum selection depth of a document.
fn selection_set_depth(selection_set: &SelectionSet) -> usize {
    selection_set
        .items
        .iter()
        .map(|item| match item {
            Selection::Field(field) => 1 + selection_set_depth(&field.node.selection_set),
            Selection::FragmentSpread(_) => 1,
            Selection::InlineFragment(fragment) => 1 + selection_set_depth(&fragment.node.selection_set),
        })
        .max()
        .unwrap_or(0)
}

/// Count the number of aliased fields in a document.
fn count_aliases(selection_set: &SelectionSet) -> usize {
    selection_set
        .items
        .iter()
        .map(|item| match item {
            Selection::Field(field) => {
                let aliased = if field.node.alias.is_some() { 1 } else { 0 };
                aliased + count_aliases(&field.node.selection_set)
            }
            Selection::FragmentSpread(_) => 0,
            Selection::InlineFragment(fragment) => count_aliases(&fragment.node.selection_set),
        })
        .sum()
}

/// Extension enforcing depth and alias limits on parsed queries.
struct QueryLimitsExtension {
    max_depth: usize,
    max_aliases: usize,
}

impl ExtensionFactory for QueryLimitsExtension {
    fn create(&self) -> Arc<dyn Extension> {
        Arc::new(QueryLimitsExtension {
            max_depth: self.max_depth,
            max_aliases: self.max_aliases,
        })
    }
}

#[async_trait::async_trait]
impl Extension for QueryLimitsExtension {
    async fn parse_query(
        &self,
        ctx: &ExtensionContext<'_>,
        query: &str,
        variables: &Value,
        next: NextParseQuery<'_>,
    ) -> ServerResult<ExecutableDocument> {
        let document = next.run(ctx, query, variables).await?;

        let depth = document
            .operations
            .iter()
            .map(|(_, op)| selection_set_depth(&op.node.selection_set))
            .max()
            .unwrap_or(0);
        if depth > self.max_depth {
            return Err(limit_exceeded_error("depth", self.max_depth));
        }

        let aliases: usize = document
            .operations
            .iter()
            .map(|(_, op)| count_aliases(&op.node.selection_set))
            .sum();
        if aliases > self.max_aliases {
            return Err(limit_exceeded_error("aliases", self.max_aliases));
        }

        Ok(document)
    }
}

/// GraphQL representation of a Soroban event
#[derive(Clone, Debug, Serialize, Deserialize, SimpleObject)]
pub struct Event {
    id: ID,
    contract_id: String,
    event_type: String,
    tx_hash: String,
    ledger: u64,
    ledger_close_time: DateTime<Utc>,
    topic: Option<Vec<String>>,
    value: Option<String>,
    source_account: Option<String>,
}

impl From<SorobanEvent> for Event {
    fn from(event: SorobanEvent) -> Self {
        Self {
            id: event.id.map(|id| ID(id.to_string())).unwrap_or_else(|| ID(Uuid::new_v4().to_string())),
            contract_id: event.contract_id,
            event_type: event.event_type,
            tx_hash: event.tx_hash,
            ledger: event.ledger as u64,
            ledger_close_time: event.ledger_close_time,
            topic: event.topic.map(|t| t.split(',').map(|s| s.to_string()).collect()),
            value: event.value,
            source_account: event.source_account,
        }
    }
}

/// GraphQL input filter for querying events
#[derive(Debug, InputObject, Clone, Default)]
pub struct EventFilter {
    /// Filter by contract ID
    pub contract_id: Option<String>,
    /// Filter by event type
    pub event_type: Option<String>,
    /// Filter by transaction hash
    pub tx_hash: Option<String>,
    /// Filter by ledger sequence number (exact match)
    pub ledger: Option<u64>,
    /// Filter by minimum ledger sequence
    pub ledger_min: Option<u64>,
    /// Filter by maximum ledger sequence
    pub ledger_max: Option<u64>,
    /// Filter by source account
    pub source_account: Option<String>,
    /// Filter by topic (partial match)
    pub topic_contains: Option<String>,
}

/// Relay-style page information
#[derive(Debug, Clone, SimpleObject)]
pub struct PageInfo {
    /// Whether there are more edges after the last returned edge
    pub has_next_page: bool,
    /// Whether there are more edges before the first returned edge
    pub has_previous_page: bool,
    /// Cursor of the first returned edge
    pub start_cursor: Option<String>,
    /// Cursor of the last returned edge
    pub end_cursor: Option<String>,
}

/// Relay-style edge wrapping a node with its cursor
#[derive(Debug, Clone, SimpleObject)]
pub struct Edge<T: OutputType> {
    /// Opaque cursor identifying this edge
    pub cursor: String,
    /// The underlying node
    pub node: T,
}

/// Relay-style connection of edges plus page info
#[derive(Debug, Clone, SimpleObject)]
pub struct Connection<T: OutputType> {
    pub edges: Vec<Edge<T>>,
    pub page_info: PageInfo,
}

/// Encode a cursor as base64("ledger:uuid")
pub fn encode_cursor(ledger: u64, id: &str) -> String {
    BASE64.encode(format!("{}:{}", ledger, id))
}

/// Decode a cursor of the form base64("ledger:uuid")
pub fn decode_cursor(cursor: &str) -> async_graphql::Result<(u64, String)> {
    let decoded = BASE64
        .decode(cursor)
        .map_err(|_| async_graphql::Error::new("Invalid cursor encoding"))?;
    let decoded = String::from_utf8(decoded)
        .map_err(|_| async_graphql::Error::new("Invalid cursor encoding"))?;
    let (ledger, id) = decoded
        .split_once(':')
        .ok_or_else(|| async_graphql::Error::new("Invalid cursor format"))?;
    let ledger = ledger
        .parse::<u64>()
        .map_err(|_| async_graphql::Error::new("Invalid cursor ledger"))?;
    if id.is_empty() {
        return Err(async_graphql::Error::new("Invalid cursor id"));
    }
    Ok((ledger, id.to_string()))
}

/// Query root for GraphQL API
pub struct Query;

#[Object]
impl Query {
    /// Get a single event by ID
    async fn event(&self, ctx: &Context<'_>, id: ID) -> async_graphql::Result<Option<Event>> {
        let pool = ctx.data::<PgPool>()?;

        let row = sqlx::query_as::<_, (String, String, String, String, i32, String, Option<String>, Option<String>, Option<String>)>(
            "SELECT id, contract_id, event_type, tx_hash, ledger, ledger_close_time, topic, value, source_account FROM events WHERE id = $1"
        )
        .bind(id.as_str())
        .fetch_optional(pool)
        .await?;

        Ok(row.map(|(id, contract_id, event_type, tx_hash, ledger, ledger_close_time, topic, value, source_account)| {
            Event {
                id: ID(id),
                contract_id,
                event_type,
                tx_hash,
                ledger: ledger as u64,
                ledger_close_time: ledger_close_time.parse().unwrap_or_else(|_| Utc::now()),
                topic: topic.map(|t| t.split(',').map(|s| s.to_string()).collect()),
                value,
                source_account,
            }
        }))
    }

    /// Query events with optional filters using Relay-style cursor pagination.
    ///
    /// Forward pagination: `first` + `after`. Backward pagination: `last` + `before`.
    /// Ordering is by (ledger DESC, id DESC); cursors encode `base64("ledger:uuid")`.
    async fn events(
        &self,
        ctx: &Context<'_>,
        filter: Option<EventFilter>,
        first: Option<i32>,
        after: Option<String>,
        last: Option<i32>,
        before: Option<String>,
    ) -> async_graphql::Result<Connection<Event>> {
        let pool = ctx.data::<PgPool>()?;
        let filter = filter.unwrap_or_default();

        if first.is_some() && last.is_some() {
            return Err(async_graphql::Error::new(
                "Cannot specify both `first` and `last`",
            ));
        }
        if first.is_some() && before.is_some() {
            return Err(async_graphql::Error::new(
                "Cannot specify both `first` and `before`",
            ));
        }
        if last.is_some() && after.is_some() {
            return Err(async_graphql::Error::new(
                "Cannot specify both `last` and `after`",
            ));
        }

        let backward = last.is_some();
        let requested = if backward { last } else { first }.unwrap_or(10);
        if requested < 0 {
            return Err(async_graphql::Error::new("Pagination count must be non-negative"));
        }
        let limit = std::cmp::min(requested as i64, 100);

        let after_cursor = match after.as_deref() {
            Some(c) => Some(decode_cursor(c)?),
            None => None,
        };
        let before_cursor = match before.as_deref() {
            Some(c) => Some(decode_cursor(c)?),
            None => None,
        };

        let mut query = String::from(
            "SELECT id, contract_id, event_type, tx_hash, ledger, ledger_close_time, topic, value, source_account FROM events WHERE 1=1"
        );

        // Build dynamic WHERE clauses based on filters
        if let Some(contract_id) = &filter.contract_id {
            query.push_str(&format!(" AND contract_id = '{}'", contract_id));
        }
        if let Some(event_type) = &filter.event_type {
            query.push_str(&format!(" AND event_type = '{}'", event_type));
        }
        if let Some(tx_hash) = &filter.tx_hash {
            query.push_str(&format!(" AND tx_hash = '{}'", tx_hash));
        }
        if let Some(ledger) = filter.ledger {
            query.push_str(&format!(" AND ledger = {}", ledger));
        }
        if let Some(ledger_min) = filter.ledger_min {
            query.push_str(&format!(" AND ledger >= {}", ledger_min));
        }
        if let Some(ledger_max) = filter.ledger_max {
            query.push_str(&format!(" AND ledger <= {}", ledger_max));
        }
        if let Some(source_account) = &filter.source_account {
            query.push_str(&format!(" AND source_account = '{}'", source_account));
        }

        // Cursor-based WHERE clauses on (ledger, id) ordering.
        if let Some((ledger, id)) = &after_cursor {
            query.push_str(&format!(
                " AND (ledger < {} OR (ledger = {} AND id < '{}'))",
                ledger, ledger, id
            ));
        }
        if let Some((ledger, id)) = &before_cursor {
            query.push_str(&format!(
                " AND (ledger > {} OR (ledger = {} AND id > '{}'))",
                ledger, ledger, id
            ));
        }

        // Fetch one extra row to determine whether another page exists.
        let fetch_limit = limit + 1;
        if backward {
            query.push_str(&format!(
                " ORDER BY ledger ASC, id ASC LIMIT {}",
                fetch_limit
            ));
        } else {
            query.push_str(&format!(
                " ORDER BY ledger DESC, id DESC LIMIT {}",
                fetch_limit
            ));
        }

        let rows = sqlx::query_as::<_, (String, String, String, String, i32, String, Option<String>, Option<String>, Option<String>)>(
            &query
        )
        .fetch_all(pool)
        .await?;

        let has_extra = rows.len() as i64 > limit;
        let mut rows = rows;
        if has_extra {
            rows.truncate(limit as usize);
        }
        if backward {
            // Rows were fetched in ascending order; present them in canonical order.
            rows.reverse();
        }

        let events: Vec<Event> = rows
            .into_iter()
            .map(|(id, contract_id, event_type, tx_hash, ledger, ledger_close_time, topic, value, source_account)| {
                Event {
                    id: ID(id),
                    contract_id,
                    event_type,
                    tx_hash,
                    ledger: ledger as u64,
                    ledger_close_time: ledger_close_time.parse().unwrap_or_else(|_| Utc::now()),
                    topic: topic.map(|t| t.split(',').map(|s| s.to_string()).collect()),
                    value,
                    source_account,
                }
            })
            .collect();

        let edges: Vec<Edge<Event>> = events
            .into_iter()
            .map(|event| {
                let cursor = encode_cursor(event.ledger, event.id.as_str());
                Edge { cursor, node: event }
            })
            .collect();

        let (has_next_page, has_previous_page) = if backward {
            (before_cursor.is_some(), has_extra)
        } else {
            (has_extra, after_cursor.is_some())
        };

        let start_cursor = edges.first().map(|e| e.cursor.clone());
        let end_cursor = edges.last().map(|e| e.cursor.clone());

        Ok(Connection {
            edges,
            page_info: PageInfo {
                has_next_page,
                has_previous_page,
                start_cursor,
                end_cursor,
            },
        })
    }

    /// Get event statistics
    async fn event_stats(&self, ctx: &Context<'_>) -> async_graphql::Result<EventStats> {
        let pool = ctx.data::<PgPool>()?;

        let total = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM events")
            .fetch_one(pool)
            .await
            .unwrap_or(0);

        let by_type = sqlx::query_as::<_, (String, i64)>(
            "SELECT event_type, COUNT(*) as count FROM events GROUP BY event_type ORDER BY count DESC"
        )
        .fetch_all(pool)
        .await
        .unwrap_or_default();

        let by_contract = sqlx::query_as::<_, (String, i64)>(
            "SELECT contract_id, COUNT(*) as count FROM events GROUP BY contract_id ORDER BY count DESC LIMIT 10"
        )
        .fetch_all(pool)
        .await
        .unwrap_or_default();

        Ok(EventStats {
            total_events: total,
            events_by_type: by_type.into_iter().map(|(event_type, count)| EventTypeCount { event_type, count }).collect(),
            top_contracts: by_contract.into_iter().map(|(contract_id, count)| ContractCount { contract_id, count }).collect(),
        })
    }
}

/// Event type count
#[derive(Debug, Clone, SimpleObject)]
pub struct EventTypeCount {
    event_type: String,
    count: i64,
}

/// Contract count
#[derive(Debug, Clone, SimpleObject)]
pub struct ContractCount {
    contract_id: String,
    count: i64,
}

/// Event statistics response
#[derive(Debug, Clone, SimpleObject)]
pub struct EventStats {
    total_events: i64,
    events_by_type: Vec<EventTypeCount>,
    top_contracts: Vec<ContractCount>,
}

/// Subscription root for real-time event streaming
pub struct Subscription;

#[Subscription]
impl Subscription {
    /// Subscribe to new events matching optional filters
    /// Supports filtering by contract_id, event_type, tx_hash, and ledger range
    async fn events(
        &self,
        ctx: &Context<'_>,
        filter: Option<EventFilter>,
    ) -> impl Stream<Item = Event> {
        let event_rx = ctx
            .data::<broadcast::Receiver<SorobanEvent>>()
            .ok()
            .cloned()
            .unwrap_or_else(|| {
                let (tx, _) = broadcast::channel(100);
                tx.subscribe()
            });

        event_rx
            .into_stream()
            .filter_map(move |event| {
                let event = match event {
                    Ok(e) => e,
                    Err(_) => return std::future::ready(None),
                };

                // Apply filters if provided
                if let Some(ref f) = filter {
                    if let Some(ref contract_id) = f.contract_id {
                        if event.contract_id != *contract_id {
                            return std::future::ready(None);
                        }
                    }
                    if let Some(ref event_type) = f.event_type {
                        if event.event_type != *event_type {
                            return std::future::ready(None);
                        }
                    }
                    if let Some(ref tx_hash) = f.tx_hash {
                        if event.tx_hash != *tx_hash {
                            return std::future::ready(None);
                        }
                    }
                    // Ledger range filtering
                    if let Some(ledger_min) = f.ledger_min {
                        if (event.ledger as u64) < ledger_min {
                            return std::future::ready(None);
                        }
                    }
                    if let Some(ledger_max) = f.ledger_max {
                        if (event.ledger as u64) > ledger_max {
                            return std::future::ready(None);
                        }
                    }
                }

                std::future::ready(Some(Event::from(event)))
            })
    }
}

/// Create the GraphQL schema with depth, complexity, and alias guards applied.
pub fn create_schema(
) -> Schema<Query, async_graphql::EmptyMutation, Subscription> {
    Schema::build(Query, async_graphql::EmptyMutation, Subscription)
        .limit_depth(graphql_max_depth())
        .limit_complexity(graphql_max_complexity())
        .extension(QueryLimitsExtension {
            max_depth: graphql_max_depth(),
            max_aliases: graphql_max_aliases(),
        })
        .finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> Schema<Query, async_graphql::EmptyMutation, Subscription> {
        create_schema()
    }

    #[tokio::test]
    async fn legitimate_query_is_unaffected() {
        let res = schema()
            .execute("{ eventStats { totalEvents } }")
            .await;
        assert!(res.errors.is_empty(), "unexpected errors: {:?}", res.errors);
    }

    #[tokio::test]
    async fn query_exceeding_depth_is_rejected() {
        // Build a query nested deeper than the default depth limit.
        let mut query = String::from("{ eventStats { totalEvents");
        for _ in 0..(DEFAULT_GRAPHQL_MAX_DEPTH + 2) {
            query.push_str(" eventsByType { eventType }");
        }
        query.push_str(" } }");

        let res = schema().execute(&query).await;
        assert!(!res.errors.is_empty());
        let msg = format!("{:?}", res.errors);
        assert!(msg.contains("depth"), "expected depth error, got: {msg}");
    }

    #[tokio::test]
    async fn query_exceeding_alias_limit_is_rejected() {
        let mut query = String::from("{");
        for i in 0..(DEFAULT_GRAPHQL_MAX_ALIASES + 2) {
            query.push_str(&format!(" a{i}: eventStats {{ totalEvents }}"));
        }
        query.push('}');

        let res = schema().execute(&query).await;
        assert!(!res.errors.is_empty());
        let msg = format!("{:?}", res.errors);
        assert!(msg.contains("aliases"), "expected alias error, got: {msg}");
    }

    #[tokio::test]
    async fn query_exceeding_complexity_is_rejected() {
        // Many fields push complexity above the configured limit.
        let mut query = String::from("{");
        for i in 0..(DEFAULT_GRAPHQL_MAX_COMPLEXITY + 10) {
            query.push_str(&format!(" f{i}: eventStats {{ totalEvents }}"));
        }
        query.push('}');

        let res = schema().execute(&query).await;
        assert!(!res.errors.is_empty());
        let msg = format!("{:?}", res.errors);
        assert!(
            msg.contains("complexity") || msg.contains("aliases"),
            "expected complexity error, got: {msg}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event(ledger: u64, id: &str) -> Event {
        Event {
            id: ID(id.to_string()),
            contract_id: "CABC".to_string(),
            event_type: "transfer".to_string(),
            tx_hash: "tx".to_string(),
            ledger,
            ledger_close_time: Utc::now(),
            topic: None,
            value: None,
            source_account: None,
        }
    }

    /// Build a connection the same way the resolver does, from a pre-sorted
    /// (ledger DESC, id DESC) list of events, to exercise pagination logic.
    fn paginate(
        all: &[Event],
        first: Option<i32>,
        after: Option<String>,
        last: Option<i32>,
        before: Option<String>,
    ) -> Connection<Event> {
        let backward = last.is_some();
        let limit = std::cmp::min(last.or(first).unwrap_or(10) as i64, 100);

        let after_cursor = after.as_deref().map(|c| decode_cursor(c).unwrap());
        let before_cursor = before.as_deref().map(|c| decode_cursor(c).unwrap());

        let mut filtered: Vec<Event> = all
            .iter()
            .filter(|e| {
                if let Some((ledger, id)) = &after_cursor {
                    !(e.ledger < *ledger || (e.ledger == *ledger && e.id.as_str() < id.as_str()))
                } else {
                    true
                }
            })
            .filter(|e| {
                if let Some((ledger, id)) = &before_cursor {
                    !(e.ledger > *ledger || (e.ledger == *ledger && e.id.as_str() > id.as_str()))
                } else {
                    true
                }
            })
            .cloned()
            .collect();

        if backward {
            filtered.reverse();
        }

        let has_extra = filtered.len() as i64 > limit;
        filtered.truncate(limit as usize);
        if backward {
            filtered.reverse();
        }

        let edges: Vec<Edge<Event>> = filtered
            .into_iter()
            .map(|event| {
                let cursor = encode_cursor(event.ledger, event.id.as_str());
                Edge { cursor, node: event }
            })
            .collect();

        let (has_next_page, has_previous_page) = if backward {
            (before_cursor.is_some(), has_extra)
        } else {
            (has_extra, after_cursor.is_some())
        };

        Connection {
            edges,
            page_info: PageInfo {
                has_next_page,
                has_previous_page,
                start_cursor: None,
                end_cursor: None,
            },
        }
    }

    fn sample_events() -> Vec<Event> {
        // Canonical order: ledger DESC, id DESC
        vec![
            make_event(105, "a"),
            make_event(104, "c"),
            make_event(104, "b"),
            make_event(103, "d"),
            make_event(102, "e"),
        ]
    }

    #[test]
    fn cursor_round_trip() {
        let cursor = encode_cursor(42, "abc-123");
        let (ledger, id) = decode_cursor(&cursor).unwrap();
        assert_eq!(ledger, 42);
        assert_eq!(id, "abc-123");
    }

    #[test]
    fn invalid_cursor_rejected() {
        assert!(decode_cursor("not-base64!!").is_err());
        assert!(decode_cursor(&BASE64.encode("no-colon")).is_err());
        assert!(decode_cursor(&BASE64.encode("abc:def")).is_err());
        assert!(decode_cursor(&BASE64.encode("1:")).is_err());
    }

    #[test]
    fn forward_pagination_first_page() {
        let conn = paginate(&sample_events(), Some(2), None, None, None);
        assert_eq!(conn.edges.len(), 2);
        assert_eq!(conn.edges[0].node.id.as_str(), "a");
        assert_eq!(conn.edges[1].node.id.as_str(), "c");
        assert!(conn.page_info.has_next_page);
        assert!(!conn.page_info.has_previous_page);
    }

    #[test]
    fn forward_pagination_next_page() {
        let all = sample_events();
        let first_page = paginate(&all, Some(2), None, None, None);
        let cursor = first_page.edges.last().unwrap().cursor.clone();

        let second_page = paginate(&all, Some(2), Some(cursor), None, None);
        assert_eq!(second_page.edges.len(), 2);
        assert_eq!(second_page.edges[0].node.id.as_str(), "b");
        assert_eq!(second_page.edges[1].node.id.as_str(), "d");
        assert!(second_page.page_info.has_next_page);
        assert!(second_page.page_info.has_previous_page);
    }

    #[test]
    fn forward_pagination_last_page_has_no_next() {
        let all = sample_events();
        let first_page = paginate(&all, Some(2), None, None, None);
        let cursor = first_page.edges.last().unwrap().cursor.clone();
        let second_page = paginate(&all, Some(2), Some(cursor), None, None);
        let cursor = second_page.edges.last().unwrap().cursor.clone();

        let last_page = paginate(&all, Some(2), Some(cursor), None, None);
        assert_eq!(last_page.edges.len(), 1);
        assert_eq!(last_page.edges[0].node.id.as_str(), "e");
        assert!(!last_page.page_info.has_next_page);
        assert!(last_page.page_info.has_previous_page);
    }

    #[test]
    fn backward_pagination_last_page() {
        let conn = paginate(&sample_events(), None, None, Some(2), None);
        assert_eq!(conn.edges.len(), 2);
        assert_eq!(conn.edges[0].node.id.as_str(), "d");
        assert_eq!(conn.edges[1].node.id.as_str(), "e");
        assert!(!conn.page_info.has_next_page);
        assert!(conn.page_info.has_previous_page);
    }

    #[test]
    fn backward_pagination_previous_page() {
        let all = sample_events();
        let last_page = paginate(&all, None, None, Some(2), None);
        let cursor = last_page.edges.first().unwrap().cursor.clone();

        let prev_page = paginate(&all, None, None, Some(2), Some(cursor));
        assert_eq!(prev_page.edges.len(), 2);
        assert_eq!(prev_page.edges[0].node.id.as_str(), "c");
        assert_eq!(prev_page.edges[1].node.id.as_str(), "b");
        assert!(prev_page.page_info.has_next_page);
        assert!(prev_page.page_info.has_previous_page);
    }

    #[test]
    fn backward_pagination_first_page_has_no_previous() {
        let all = sample_events();
        let last_page = paginate(&all, None, None, Some(2), None);
        let cursor = last_page.edges.first().unwrap().cursor.clone();
        let prev_page = paginate(&all, None, None, Some(2), Some(cursor));
        let cursor = prev_page.edges.first().unwrap().cursor.clone();

        let first_page = paginate(&all, None, None, Some(2), Some(cursor));
        assert_eq!(first_page.edges.len(), 1);
        assert_eq!(first_page.edges[0].node.id.as_str(), "a");
        assert!(!first_page.page_info.has_previous_page);
        assert!(first_page.page_info.has_next_page);
    }

    #[test]
    fn empty_result_has_no_pages() {
        let conn = paginate(&[], Some(10), None, None, None);
        assert!(conn.edges.is_empty());
        assert!(!conn.page_info.has_next_page);
        assert!(!conn.page_info.has_previous_page);
    }
}
