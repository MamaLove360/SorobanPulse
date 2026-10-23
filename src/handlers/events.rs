//! Event handlers: querying, streaming, exporting and managing indexed events.

#[allow(unused_imports)]
use super::*;

pub(super) fn events_feed_cache() -> &'static Mutex<HashMap<String, CacheEntry>> {
    EVENTS_FEED_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Map sqlx rows to a JSON array, projecting only the requested columns.
/// Gzip-compress `value` and return a base64-encoded string (standard alphabet, no padding).
/// Used by the `compact=true` query parameter to shrink large `event_data` payloads.
pub(super) fn compact_event_data(value: &Value) -> Result<Value, AppError> {
    use base64::engine::general_purpose::STANDARD;
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;

    let json_bytes = serde_json::to_vec(value)
        .map_err(|e| AppError::Internal(format!("serialization error: {e}")))?;

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(&json_bytes)
        .map_err(|e| AppError::Internal(format!("gzip write error: {e}")))?;
    let compressed = encoder
        .finish()
        .map_err(|e| AppError::Internal(format!("gzip finish error: {e}")))?;

    Ok(Value::String(STANDARD.encode(&compressed)))
}

/// Returns aggregate statistics about indexed events.
#[utoipa::path(
    get,
    path = "/v1/events/stats",
    tag = "events",
    responses(
        (status = 200, description = "Aggregate event statistics", body = crate::models::EventStats),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
pub async fn get_event_stats(State(state): State<AppState>) -> Result<impl IntoResponse, AppError> {
    use std::collections::HashMap;

    let cache_key = "stats";

    // Try to get from cache
    if let Some(cached) = state.stats_cache.get(cache_key).await {
        crate::metrics::record_stats_cache_hit();
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::CACHE_CONTROL,
            axum::http::HeaderValue::from_str(&format!(
                "public, max-age={}",
                state.config.stats_cache_ttl_secs
            ))
            .unwrap_or_else(|_| axum::http::HeaderValue::from_static("public, max-age=30")),
        );
        return Ok((headers, Json(cached)));
    }

    crate::metrics::record_stats_cache_miss();

    // Total events and ledger range from raw table (fast with index)
    let totals_row = sqlx::query(
        "SELECT COUNT(*) AS total_events, MIN(ledger) AS min_ledger, MAX(ledger) AS max_ledger FROM events",
    )
    .fetch_one(&state.read_pool)
    .await?;

    let total_events: i64 = totals_row.try_get("total_events")?;
    let min_ledger: Option<i64> = totals_row.try_get("min_ledger")?;
    let max_ledger: Option<i64> = totals_row.try_get("max_ledger")?;

    // Per-type counts and time-windowed counts from events_daily_summary matview
    let type_rows = sqlx::query(
        "SELECT event_type, SUM(event_count) AS cnt FROM events_daily_summary GROUP BY event_type",
    )
    .fetch_all(&state.read_pool)
    .await?;

    let mut events_by_type: HashMap<String, i64> = HashMap::new();
    for row in &type_rows {
        let et: String = row.try_get("event_type")?;
        let cnt: i64 = row.try_get("cnt")?;
        events_by_type.insert(et, cnt);
    }
    events_by_type.entry("contract".to_string()).or_insert(0);
    events_by_type.entry("diagnostic".to_string()).or_insert(0);
    events_by_type.entry("system".to_string()).or_insert(0);

    // 24h and 7d counts from daily_summary (last 1 and 7 days)
    let window_row = sqlx::query(
        r#"
        SELECT
            COALESCE(SUM(event_count) FILTER (WHERE event_date >= CURRENT_DATE - INTERVAL '1 day'), 0)  AS events_last_24h,
            COALESCE(SUM(event_count) FILTER (WHERE event_date >= CURRENT_DATE - INTERVAL '7 days'), 0) AS events_last_7d
        FROM events_daily_summary
        "#,
    )
    .fetch_one(&state.read_pool)
    .await?;

    let events_last_24h: i64 = window_row.try_get("events_last_24h")?;
    let events_last_7d: i64 = window_row.try_get("events_last_7d")?;

    // Top 10 contracts from events_contract_summary matview
    let top_rows = sqlx::query_as::<_, (String, i64)>(
        "SELECT contract_id, event_count FROM events_contract_summary ORDER BY event_count DESC LIMIT 10",
    )
    .fetch_all(&state.read_pool)
    .await?;

    let top_contracts = top_rows
        .into_iter()
        .map(
            |(contract_id, event_count)| crate::models::ContractStatEntry {
                contract_id,
                event_count,
            },
        )
        .collect();

    let stats = crate::models::EventStats {
        total_events,
        events_last_24h,
        events_last_7d,
        top_contracts,
        events_by_type,
        min_ledger,
        max_ledger,
        computed_at: Utc::now(),
    };

    let stats_json = serde_json::to_value(&stats)?;

    // Store in cache
    state
        .stats_cache
        .insert(cache_key.to_string(), stats_json.clone())
        .await;

    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_str(&format!(
            "public, max-age={}",
            state.config.stats_cache_ttl_secs
        ))
        .unwrap_or_else(|_| axum::http::HeaderValue::from_static("public, max-age=30")),
    );

    Ok((headers, Json(stats_json)))
}

/// Return per-contract daily event counts sourced from the `mv_contract_event_counts`
/// materialized view.  Results are cached in the in-process query cache for the
/// duration of `QUERY_CACHE_TTL_SECS` (default 5 min, clamped to [5 min, 60 min]).
#[utoipa::path(
    get,
    path = "/v1/contracts/{contract_id}/event-counts",
    tag = "events",
    params(
        ("contract_id" = String, Path, description = "Stellar contract ID"),
    ),
    responses(
        (status = 200, description = "Daily event counts for the contract"),
        (status = 400, description = "Invalid contract_id", body = ErrorResponse),
    )
)]
pub async fn get_contract_event_counts(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
) -> Result<Json<Value>, AppError> {
    validate_contract_id(&contract_id)?;

    let cache_key = format!("contract_event_counts:{contract_id}");

    if let Some(cached) = crate::query_cache::get(&state.query_result_cache, &cache_key).await {
        return Ok(Json(cached));
    }

    let rows = sqlx::query(
        "SELECT event_type, event_day, event_count, unique_tx_count, last_event_at \
         FROM mv_contract_event_counts \
         WHERE contract_id = $1 \
         ORDER BY event_day DESC \
         LIMIT 90",
    )
    .bind(&contract_id)
    .fetch_all(&state.read_pool)
    .await?;

    let counts: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "event_type":      r.try_get::<String, _>("event_type").unwrap_or_default(),
                "event_day":       r.try_get::<chrono::DateTime<chrono::Utc>, _>("event_day").ok()
                                     .map(|d| d.format("%Y-%m-%d").to_string())
                                     .unwrap_or_default(),
                "event_count":     r.try_get::<i64, _>("event_count").unwrap_or(0),
                "unique_tx_count": r.try_get::<i64, _>("unique_tx_count").unwrap_or(0),
                "last_event_at":   r.try_get::<chrono::DateTime<chrono::Utc>, _>("last_event_at").ok()
                                     .map(|t| t.to_rfc3339())
                                     .unwrap_or_default(),
            })
        })
        .collect();

    let result = json!({
        "contract_id": contract_id,
        "counts":      counts,
        "cached_at":   Utc::now().to_rfc3339(),
    });

    crate::query_cache::set(&state.query_result_cache, cache_key, result.clone()).await;

    Ok(Json(result))
}

/// Stream new events in real time via Server-Sent Events.
///
/// This endpoint is less preferred for contract-specific streaming; use
/// `/v1/events/contract/{contract_id}/stream` instead.
#[utoipa::path(
    get,
    path = "/v1/events/stream",
    tag = "events",
    params(
        ("contract_id" = Option<String>, Query, description = "Filter by contract ID (less preferred)"),
        ("fields" = Option<String>, Query, description = "Comma-separated list of fields to include in each event"),
    ),
    responses(
        (status = 200, description = "SSE stream of new events (text/event-stream)"),
        (status = 400, description = "Invalid contract_id format", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 429, description = "Too many requests", body = ErrorResponse),
        (status = 503, description = "Too many SSE connections", body = ErrorResponse),
    )
)]
#[instrument(skip(state, headers, extensions), fields(contract_id = ?params.contract_id))]
pub async fn stream_events(
    State(state): State<AppState>,
    Query(params): Query<StreamParams>,
    headers: axum::http::HeaderMap,
    extensions: axum::http::Extensions,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, Json<Value>)> {
    let tenant_id = extract_tenant_id(&extensions).map(|s| s.to_owned());
    let client_ip = extract_client_ip(&headers);
    let scval_format = crate::scval_format::parse_format(params.format.as_deref())
        .map_err(|e| (StatusCode::BAD_REQUEST, Json(json!({ "error": e, "code": "VALIDATION_ERROR" }))))?;
    stream_events_internal(State(state), params.contract_id, params.fields, params.event_type, headers, tenant_id, client_ip, scval_format)
        .await
}

/// Stream new events for a specific contract in real time via Server-Sent Events.
#[utoipa::path(
    get,
    path = "/v1/events/contract/{contract_id}/stream",
    tag = "events",
    params(
        ("contract_id" = String, Path, description = "Stellar contract ID"),
        ("fields" = Option<String>, Query, description = "Comma-separated list of fields to include in each event"),
    ),
    responses(
        (status = 200, description = "SSE stream of contract events (text/event-stream)"),
        (status = 400, description = "Invalid contract_id format", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 429, description = "Too many requests", body = ErrorResponse),
        (status = 503, description = "Too many SSE connections", body = ErrorResponse),
    )
)]
pub async fn stream_events_by_contract(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Query(params): Query<StreamParams>,
    headers: axum::http::HeaderMap,
    extensions: axum::http::Extensions,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, Json<Value>)> {
    validate_contract_id(&contract_id).map_err(|e| {
        let (status, body) = e.into_response_parts();
        (status, body)
    })?;
    let tenant_id = extract_tenant_id(&extensions).map(|s| s.to_owned());
    let client_ip = extract_client_ip(&headers);
    let scval_format = crate::scval_format::parse_format(params.format.as_deref())
        .map_err(|e| (StatusCode::BAD_REQUEST, Json(json!({ "error": e, "code": "VALIDATION_ERROR" }))))?;
    stream_events_internal(State(state), Some(contract_id), params.fields, params.event_type, headers, tenant_id, client_ip, scval_format)
        .await
}

/// Stream events for multiple contracts simultaneously via Server-Sent Events.
#[utoipa::path(
    get,
    path = "/v1/events/stream/multi",
    tag = "events",
    params(
        ("contract_ids" = String, Query, description = "Comma-separated list of contract IDs to subscribe to"),
    ),
    responses(
        (status = 200, description = "SSE stream of events from the specified contracts (text/event-stream)"),
        (status = 400, description = "Invalid or empty contract_ids", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 429, description = "Too many requests", body = ErrorResponse),
        (status = 503, description = "Too many SSE connections", body = ErrorResponse),
    )
)]
#[instrument(skip(state, headers, extensions), fields(contract_ids = ?params.contract_ids))]
pub async fn stream_events_multi(
    State(state): State<AppState>,
    Query(params): Query<crate::models::MultiStreamParams>,
    headers: axum::http::HeaderMap,
    extensions: axum::http::Extensions,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, Json<Value>)> {
    let tenant_id = extract_tenant_id(&extensions).map(|s| s.to_owned());
    let client_ip = extract_client_ip(&headers);
    let event_type_filter = params.event_type;
    let raw = params.contract_ids.unwrap_or_default();
    if raw.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "contract_ids must not be empty", "code": "VALIDATION_ERROR" })),
        ));
    }

    let ids: Vec<String> = raw.split(',').map(|s| s.trim().to_string()).collect();

    // Validate number of contract IDs does not exceed limit
    if ids.len() > state.config.sse_multi_max_contract_ids {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": format!("too many contract IDs (max {})", state.config.sse_multi_max_contract_ids),
                "code": "VALIDATION_ERROR",
                "limit": state.config.sse_multi_max_contract_ids,
                "provided": ids.len(),
            })),
        ));
    }

    // Record histogram metric for contract IDs per connection
    crate::metrics::record_sse_multi_contract_ids(ids.len() as u64);

    // Validate every ID; collect all invalid ones for a helpful error message.
    let invalid: Vec<String> = ids
        .iter()
        .filter(|id| validate_contract_id(id).is_err())
        .cloned()
        .collect();

    if !invalid.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "invalid contract_id(s)",
                "code": "VALIDATION_ERROR",
                "invalid_ids": invalid,
            })),
        ));
    }

    // Check global connection limit
    let current_connections = state
        .sse_connections
        .load(std::sync::atomic::Ordering::Relaxed);
    if current_connections >= state.sse_max_connections {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "too many SSE connections", "code": "SSE_LIMIT_EXCEEDED" })),
        ));
    }

    // #453: Per-IP connection limit check
    let max_per_ip = state.config.sse_max_connections_per_ip;
    if max_per_ip > 0 {
        let current_ip_count = state
            .sse_connections_per_ip
            .get(&client_ip)
            .map(|v| *v)
            .unwrap_or(0);
        if current_ip_count >= max_per_ip {
            return Err((
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({
                    "error": "too many SSE connections from this IP",
                    "code": "SSE_IP_LIMIT_EXCEEDED",
                    "limit": max_per_ip,
                })),
            ));
        }
        *state.sse_connections_per_ip.entry(client_ip.clone()).or_insert(0) += 1;
        let new_ip_count = state.sse_connections_per_ip.get(&client_ip).map(|v| *v).unwrap_or(0);
        crate::metrics::record_sse_connections_per_ip(new_ip_count);
    }

    state
        .sse_connections
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let new_count = state
        .sse_connections
        .load(std::sync::atomic::Ordering::Relaxed);
    crate::metrics::update_sse_connections(new_count);

    let keepalive_ms = state.sse_keepalive_interval_ms;
    let sse_connections = state.sse_connections.clone();
    let sse_connections_per_ip = state.sse_connections_per_ip.clone();
    let client_ip_cleanup = client_ip.clone();
    let max_per_ip_cleanup = max_per_ip;
    let max_lag = state.config.sse_max_lag_before_disconnect;
    let connection_id = Uuid::new_v4().to_string();

    // Replay missed events if the client sends Last-Event-ID.
    // Try ring buffer first; fall back to DB on miss.
    let last_event_id = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| uuid::Uuid::parse_str(s).ok());

    let ring_replay_multi: Vec<crate::models::SorobanEvent> = if let Some(last_id) = last_event_id {
        match state.sse_ring_buffer.events_since(last_id) {
            Some(events) => events
                .into_iter()
                .filter(|ev| {
                    if !ids.contains(&ev.contract_id) { return false; }
                    if let Some(et) = event_type_filter {
                        if ev.event_type != et.to_string() { return false; }
                    }
                    true
                })
                .take(state.config.sse_replay_limit as usize)
                .collect(),
            None => {
                crate::metrics::record_sse_ring_buffer_miss();
                vec![]
            }
        }
    } else {
        vec![]
    };

    // DB fallback for multi-stream when ring buffer misses.
    let db_replay_multi: Vec<crate::models::Event> =
        if let (Some(last_id), true) = (last_event_id.clone(), ring_replay_multi.is_empty()) {
            let base_offset = 2usize;
            let placeholders: String = ids
                .iter()
                .enumerate()
                .map(|(i, _)| format!("${}", base_offset + i))
                .collect::<Vec<_>>()
                .join(", ");
            let tid = tenant_id.as_deref();
            let tenant_clause = if tid.is_some() {
                format!(" AND tenant_id = ${}", base_offset + ids.len())
            } else {
                String::new()
            };
            let sql = format!(
                "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at, 0::bigint AS total_count \
                 FROM events WHERE created_at > (SELECT created_at FROM events WHERE id = $1) \
                 AND contract_id IN ({}){} ORDER BY created_at ASC LIMIT {}",
                placeholders, tenant_clause, state.config.sse_replay_limit
            );
            let mut q = sqlx::query_as::<_, crate::models::Event>(&sql).bind(last_id);
            for id in &ids {
                q = q.bind(id);
            }
            if let Some(tid) = tid {
                q = q.bind(tid);
            }
            q.fetch_all(&state.pool).await.unwrap_or_default()
        } else {
            vec![]
        };

    let total_replayed_multi = ring_replay_multi.len() + db_replay_multi.len();
    if total_replayed_multi > 0 {
        crate::metrics::record_sse_replayed_events(total_replayed_multi as u64);
    }

    let has_replay = total_replayed_multi > 0;
    let rx = state.event_tx.subscribe();
    let enc_key = state.encryption_key;
    let enc_key_old = state.encryption_key_old;

    let replay_stream = futures::stream::iter(ring_replay_multi.into_iter().map(move |ev| {
        let eid = ev.id.map(|u| u.to_string())
            .unwrap_or_else(|| format!("{}-{}", ev.tx_hash, ev.ledger));
        let data = serde_json::to_string(&ev).unwrap_or_default();
        Ok(Event::default()
            .id(eid)
            .retry(Duration::from_millis(keepalive_ms))
            .data(data))
    }));

    let db_replay_stream_multi = futures::stream::iter(db_replay_multi.into_iter().filter_map(move |mut ev| {
        ev.event_data = decrypt_event_data(&ev.event_data, enc_key.as_ref(), enc_key_old.as_ref());
        let data = serde_json::to_string(&ev).unwrap_or_default();
        Some(Ok(Event::default()
            .id(ev.id.to_string())
            .retry(Duration::from_millis(keepalive_ms))
            .data(data)))
    }));

    // #454: replay_complete event
    let replay_complete_stream = if has_replay {
        futures::stream::iter(vec![Ok(Event::default()
            .event("replay_complete")
            .data("replay complete"))])
    } else {
        futures::stream::iter(vec![])
    };

    let live_stream = futures::stream::unfold(
        (rx, ids, keepalive_ms, tenant_id, event_type_filter, false, max_lag, connection_id.clone()),
        move |(mut rx, filter_ids, ka, tid, et_filter, closed, max_lag, conn_id)| async move {
            if closed {
                return None;
            }
            let mut interval = tokio::time::interval(Duration::from_millis(ka));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await;
            loop {
                tokio::select! {
                    recv = rx.recv() => match recv {
                        Ok(event) => {
                            if !filter_ids.contains(&event.contract_id) {
                                continue;
                            }
                            // #452: Filter by event_type
                            if let Some(et) = et_filter {
                                if event.event_type.to_string() != et.to_string() {
                                    continue;
                                }
                            }
                            if let Some(ref tenant) = tid {
                                if event.tenant_id.as_deref() != Some(tenant.as_str()) {
                                    continue;
                                }
                            }
                            let data = serde_json::to_string(&event).unwrap_or_default();
                            let eid = event.id.map(|u| u.to_string())
                                .unwrap_or_else(|| format!("{}-{}", event.tx_hash, event.ledger));
                            let sse = Event::default()
                                .id(eid)
                                .retry(Duration::from_millis(ka))
                                .data(data);
                            return Some((Ok(sse), (rx, filter_ids, ka, tid, et_filter, false, max_lag, conn_id)));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            // #451: Send lag notification
                            crate::metrics::increment_sse_lagged_events(&conn_id, n);
                            let lag_data = serde_json::json!({ "missed": n, "last_event_id": null });
                            let lag_event = Event::default().event("lag").data(lag_data.to_string());
                            if max_lag > 0 && n >= max_lag {
                                return Some((Ok(lag_event), (rx, filter_ids, ka, tid, et_filter, true, max_lag, conn_id)));
                            }
                            return Some((Ok(lag_event), (rx, filter_ids, ka, tid, et_filter, false, max_lag, conn_id)));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            let close_event = Event::default().event("close").data("stream closed");
                            return Some((Ok(close_event), (rx, filter_ids, ka, tid, et_filter, true, max_lag, conn_id)));
                        }
                    },
                    _ = interval.tick() => {
                        let ts = chrono::Utc::now().to_rfc3339();
                        let ping = Event::default().event("ping").data(ts);
                        return Some((Ok(ping), (rx, filter_ids, ka, tid, et_filter, false, max_lag, conn_id)));
                    }
                }
            }
        },
    );

    let combined = replay_stream
        .chain(db_replay_stream_multi)
        .chain(replay_complete_stream)
        .chain(live_stream);

    let stream_with_cleanup = futures::stream::unfold(
        (Box::pin(combined), sse_connections.clone(), sse_connections_per_ip, client_ip_cleanup, max_per_ip_cleanup),
        move |(mut stream, counter, ip_map, ip, max_ip)| async move {
            match stream.next().await {
                Some(item) => Some((item, (stream, counter, ip_map, ip, max_ip))),
                None => {
                    let new_count = counter.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) - 1;
                    crate::metrics::update_sse_connections(new_count);
                    if max_ip > 0 {
                        let mut entry = ip_map.entry(ip.clone()).or_insert(0);
                        if *entry > 0 { *entry -= 1; }
                    }
                    None
                }
            }
        },
    );

    Ok(Sse::new(stream_with_cleanup))
}

/// WebSocket event stream. Clients receive events as JSON text frames.
/// After connecting, a client may send `{"contract_id":"CABC..."}` to filter
/// by contract, or `{}` / omit the field to receive all events.
#[utoipa::path(
    get,
    path = "/v1/events/ws",
    tag = "events",
    params(
        ("contract_id" = Option<String>, Query, description = "Initial contract ID filter (can also be set via WebSocket message)"),
    ),
    responses(
        (status = 101, description = "WebSocket upgrade"),
        (status = 400, description = "Invalid contract_id", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn ws_events(
    State(state): State<AppState>,
    Query(params): Query<StreamParams>,
    ws: axum::extract::ws::WebSocketUpgrade,
) -> Result<impl IntoResponse, AppError> {
    if let Some(ref cid) = params.contract_id {
        validate_contract_id(cid)?;
    }

    let initial_filter = params.contract_id.clone();
    let event_tx = state.event_tx.clone();
    let enc_key = state.encryption_key;
    let enc_key_old = state.encryption_key_old;

    Ok(ws.on_upgrade(move |mut socket| async move {
        use axum::extract::ws::Message;

        let mut rx = event_tx.subscribe();
        let mut contract_filter = initial_filter;

        loop {
            tokio::select! {
                // Incoming message from client (filter update or close)
                msg = socket.recv() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                                contract_filter = v.get("contract_id")
                                    .and_then(|c| c.as_str())
                                    .map(|s| s.to_string());
                            }
                        }
                        Some(Ok(Message::Close(_))) | None => break,
                        _ => {}
                    }
                }
                // Outgoing event from broadcast channel
                result = rx.recv() => {
                    match result {
                        Ok(mut event) => {
                            if let Some(ref cid) = contract_filter {
                                if &event.contract_id != cid {
                                    continue;
                                }
                            }
                            let text = serde_json::to_string(&event).unwrap_or_default();
                            if socket.send(Message::Text(text.into())).await.is_err() {
                                break;
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        }
    }))
}

pub(super) async fn stream_events_internal(
    State(state): State<AppState>,
    contract_filter: Option<String>,
    fields: Option<String>,
    event_type_filter: Option<crate::models::EventType>,
    headers: axum::http::HeaderMap,
    tenant_id: Option<String>,
    client_ip: String,
    scval_format: crate::scval_format::ScValFormat,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, Json<Value>)> {
    // Check if we've reached the max SSE connections limit
    let current_connections = state
        .sse_connections
        .load(std::sync::atomic::Ordering::Relaxed);
    if current_connections >= state.sse_max_connections {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "too many SSE connections",
                "code": "SSE_LIMIT_EXCEEDED"
            })),
        ));
    }

    // #453: Per-IP connection limit check
    let max_per_ip = state.config.sse_max_connections_per_ip;
    if max_per_ip > 0 {
        let current_ip_count = state
            .sse_connections_per_ip
            .get(&client_ip)
            .map(|v| *v)
            .unwrap_or(0);
        if current_ip_count >= max_per_ip {
            return Err((
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({
                    "error": "too many SSE connections from this IP",
                    "code": "SSE_IP_LIMIT_EXCEEDED",
                    "limit": max_per_ip,
                })),
            ));
        }
        // Increment per-IP counter
        *state.sse_connections_per_ip.entry(client_ip.clone()).or_insert(0) += 1;
        let new_ip_count = state.sse_connections_per_ip.get(&client_ip).map(|v| *v).unwrap_or(0);
        crate::metrics::record_sse_connections_per_ip(new_ip_count);
    }

    // Increment connection counter
    state
        .sse_connections
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let new_count = state
        .sse_connections
        .load(std::sync::atomic::Ordering::Relaxed);
    crate::metrics::update_sse_connections(new_count);

    let keepalive_ms = state.sse_keepalive_interval_ms;
    let sse_connections = state.sse_connections.clone();
    let sse_connections_per_ip = state.sse_connections_per_ip.clone();
    let client_ip_cleanup = client_ip.clone();
    let max_per_ip_cleanup = max_per_ip;

    // Validate contract_id if provided
    if let Some(ref cid) = contract_filter {
        validate_contract_id(cid).map_err(|e| {
            // Decrement per-IP counter on validation error
            if max_per_ip > 0 {
                let mut entry = sse_connections_per_ip.entry(client_ip.clone()).or_insert(0);
                if *entry > 0 { *entry -= 1; }
            }
            state.sse_connections.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            let body = json!({ "error": e.to_string(), "code": "VALIDATION_ERROR" });
            (StatusCode::BAD_REQUEST, Json(body))
        })?;
    }

    // Resolve field projection
    let field_columns: Option<Vec<&'static str>> = fields.as_deref().and_then(|f| {
        let trimmed = f.trim();
        if trimmed.is_empty() {
            return None;
        }
        let cols: Vec<&'static str> = trimmed
            .split(',')
            .map(|s| s.trim())
            .filter_map(|s| {
                PaginationParams::ALLOWED_FIELDS
                    .iter()
                    .find(|&&a| a == s)
                    .copied()
            })
            .collect();
        if cols.is_empty() { None } else { Some(cols) }
    });

    // Replay missed events if the client sends Last-Event-ID.
    // Strategy: check the in-memory ring buffer first (O(n), no DB round-trip).
    // If the ID was evicted (buffer overflow), fall back to a DB query.
    let last_event_id = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok());

    // Ring-buffer replay: returns SorobanEvent (broadcast type), converted to SSE frames directly.
    let ring_replay: Vec<crate::models::SorobanEvent> = if let Some(last_id) = last_event_id {
        match state.sse_ring_buffer.events_since(last_id) {
            Some(events) => {
                // Apply contract filter and event_type filter
                events
                    .into_iter()
                    .filter(|ev| {
                        if let Some(ref cid) = contract_filter {
                            if &ev.contract_id != cid {
                                return false;
                            }
                        }
                        if let Some(et) = event_type_filter {
                            if ev.event_type != et.to_string() {
                                return false;
                            }
                        }
                        true
                    })
                    .take(state.config.sse_replay_limit as usize)
                    .collect()
            }
            None => {
                // Ring buffer miss — evicted; fall back to DB query.
                crate::metrics::record_sse_ring_buffer_miss();
                vec![]
            }
        }
    } else {
        vec![]
    };

    // DB fallback replay when ring buffer miss occurs (last_event_id set but ring returned empty
    // because of eviction). Also used when ring buffer is disabled.
    let db_replay: Vec<crate::models::Event> = if let (Some(last_id), true) =
        (last_event_id.clone(), ring_replay.is_empty())
    {
        let q = if let Some(ref cid) = contract_filter {
            sqlx::query_as::<_, crate::models::Event>(
                "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, event_data_normalized, created_at, schema_version, 0::bigint AS total_count \
                 FROM events WHERE created_at > (SELECT created_at FROM events WHERE id = $1) \
                 AND contract_id = $2 ORDER BY created_at ASC LIMIT $3",
            )
            .bind(last_id)
            .bind(cid)
            .bind(state.config.sse_replay_limit as i64)
            .fetch_all(&state.pool)
            .await
        } else {
            sqlx::query_as::<_, crate::models::Event>(
                "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, event_data_normalized, created_at, schema_version, 0::bigint AS total_count \
                 FROM events WHERE created_at > (SELECT created_at FROM events WHERE id = $1) \
                 ORDER BY created_at ASC LIMIT $2",
            )
            .bind(last_id)
            .bind(state.config.sse_replay_limit as i64)
            .fetch_all(&state.pool)
            .await
        };
        q.unwrap_or_default()
    } else {
        vec![]
    };

    let total_replayed = ring_replay.len() + db_replay.len();
    if total_replayed > 0 {
        crate::metrics::record_sse_replayed_events(total_replayed as u64);
    }

    let has_replay = total_replayed > 0;
    let rx = state.event_tx.subscribe();
    let enc_key = state.encryption_key;
    let enc_key_old = state.encryption_key_old;
    let max_lag = state.config.sse_max_lag_before_disconnect;
    // Generate a stable connection ID for lag metrics
    let connection_id = Uuid::new_v4().to_string();

    // Ring-buffer replay stream: SorobanEvent items already filtered above.
    let ring_replay_stream = stream::iter(ring_replay.into_iter().map(move |ev| {
        let id_str = ev.id.map(|u| u.to_string())
            .unwrap_or_else(|| format!("{}-{}", ev.tx_hash, ev.ledger));
        let data = serde_json::to_string(&ev).unwrap_or_default();
        Ok(Event::default()
            .id(id_str)
            .retry(Duration::from_millis(keepalive_ms))
            .data(data))
    }));

    let field_columns_replay = field_columns.clone();
    // DB fallback replay stream: full Event records from DB.
    let db_replay_stream = stream::iter(db_replay.into_iter().filter_map(move |mut ev| {
        ev.event_data = decrypt_event_data(&ev.event_data, enc_key.as_ref(), enc_key_old.as_ref());
        ev.event_data = crate::scval_format::render_event_data(&ev.event_data, scval_format);
        let data = match &field_columns_replay {
            Some(cols) => serde_json::to_string(&filter_fields(
                &ev,
                cols,
                enc_key.as_ref(),
                enc_key_old.as_ref(),
            ))
            .unwrap_or_default(),
            None => serde_json::to_string(&ev).unwrap_or_default(),
        };
        Some(Ok(Event::default()
            .id(ev.id.to_string())
            .retry(Duration::from_millis(keepalive_ms))
            .data(data)))
    }));

    // replay_complete sentinel after all replayed events.
    let replay_complete_stream = if has_replay {
        stream::iter(vec![Ok(Event::default()
            .event("replay_complete")
            .data("replay complete"))])
    } else {
        stream::iter(vec![])
    };

    let live_stream = stream::unfold(
        (
            rx,
            contract_filter,
            keepalive_ms,
            field_columns,
            enc_key,
            enc_key_old,
            tenant_id,
            event_type_filter,
            false, // closed
            state.shutdown_rx.clone(),
            max_lag,
            connection_id.clone(),
        ),
        move |(mut rx, filter, ka, cols, ek, ek_old, tid, et_filter, closed, mut shutdown_rx, max_lag, conn_id)| async move {
            if closed {
                return None;
            }
            let mut interval = tokio::time::interval(Duration::from_millis(ka));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await;
            loop {
                tokio::select! {
                    recv = rx.recv() => match recv {
                        Ok(event) => {
                            if let Some(ref cid) = filter {
                                if &event.contract_id != cid {
                                    continue;
                                }
                            }
                            // #452: Filter by event_type
                            if let Some(et) = et_filter {
                                if event.event_type.to_string() != et.to_string() {
                                    continue;
                                }
                            }
                            if let Some(ref tenant) = tid {
                                if event.tenant_id.as_deref() != Some(tenant.as_str()) {
                                    continue;
                                }
                            }
                            let data = serde_json::to_string(&event).unwrap_or_default();
                            let eid = event.id.map(|u| u.to_string())
                                .unwrap_or_else(|| format!("{}-{}", event.tx_hash, event.ledger));
                            let sse = Event::default()
                                .id(eid)
                                .retry(Duration::from_millis(ka))
                                .data(data);
                            return Some((Ok(sse), (rx, filter, ka, cols, ek, ek_old, tid, et_filter, false, shutdown_rx, max_lag, conn_id)));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            // #451: Send lag notification event
                            crate::metrics::increment_sse_lagged_events(&conn_id, n);
                            let last_id = rx.len(); // approximate position
                            let lag_data = serde_json::json!({
                                "missed": n,
                                "last_event_id": null,
                            });
                            let lag_event = Event::default()
                                .event("lag")
                                .data(lag_data.to_string());
                            // #451: Disconnect if lag exceeds threshold
                            if max_lag > 0 && n >= max_lag {
                                return Some((Ok(lag_event), (rx, filter, ka, cols, ek, ek_old, tid, et_filter, true, shutdown_rx, max_lag, conn_id)));
                            }
                            return Some((Ok(lag_event), (rx, filter, ka, cols, ek, ek_old, tid, et_filter, false, shutdown_rx, max_lag, conn_id)));
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            let close_event = Event::default().event("close").data("stream closed");
                            return Some((Ok(close_event), (rx, filter, ka, cols, ek, ek_old, tid, et_filter, true, shutdown_rx, max_lag, conn_id)));
                        }
                    },
                    _ = interval.tick() => {
                        let ts = chrono::Utc::now().to_rfc3339();
                        let ping = Event::default().event("ping").data(ts);
                        return Some((Ok(ping), (rx, filter, ka, cols, ek, ek_old, tid, et_filter, false, shutdown_rx, max_lag, conn_id)));
                    }
                    _ = shutdown_rx.changed() => {
                        let close_event = Event::default().event("close").data("server shutting down");
                        return Some((Ok(close_event), (rx, filter, ka, cols, ek, ek_old, tid, et_filter, true, shutdown_rx, max_lag, conn_id)));
                    }
                }
            }
        },
    );

    let combined = ring_replay_stream
        .chain(db_replay_stream)
        .chain(replay_complete_stream)
        .chain(live_stream);
    let combined = Box::pin(combined);

    // Wrap the stream to decrement the connection counter when the stream ends
    let stream_with_cleanup = stream::unfold(
        (Box::pin(combined), sse_connections.clone(), sse_connections_per_ip, client_ip_cleanup, max_per_ip_cleanup),
        move |(mut stream, counter, ip_map, ip, max_ip)| async move {
            match stream.next().await {
                Some(item) => Some((item, (stream, counter, ip_map, ip, max_ip))),
                None => {
                    let new_count = counter.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) - 1;
                    crate::metrics::update_sse_connections(new_count);
                    // Decrement per-IP counter
                    if max_ip > 0 {
                        let mut entry = ip_map.entry(ip.clone()).or_insert(0);
                        if *entry > 0 { *entry -= 1; }
                    }
                    None
                }
            }
        },
    );

    Ok(Sse::new(stream_with_cleanup))
}

/// Decrypt event_data if an encryption key is configured.
pub(super) fn decrypt_event_data(raw: &Value, key: Option<&[u8; 32]>, old_key: Option<&[u8; 32]>) -> Value {
    if let Some(k) = key {
        crate::encryption::decrypt(k, old_key, raw).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "Failed to decrypt event_data, returning raw value");
            raw.clone()
        })
    } else {
        raw.clone()
    }
}

#[utoipa::path(
    get,
    path = "/v1/events",
    tag = "events",
    params(
        ("page" = Option<i64>, Query, description = "Page number (default: 1)"),
        ("limit" = Option<i64>, Query, description = "Results per page, 1–100 (default: 20)"),
        ("exact_count" = Option<bool>, Query, description = "Use exact COUNT(*) instead of approximate"),
        ("event_type" = Option<crate::models::EventType>, Query, description = "Filter by event type: contract, diagnostic, system"),
        ("from_ledger" = Option<i64>, Query, description = "Return events at or after this ledger"),
        ("to_ledger" = Option<i64>, Query, description = "Return events at or before this ledger"),
        ("ledger_hash" = Option<String>, Query, description = "Filter by ledger hash"),
        ("anonymized" = Option<bool>, Query, description = "Filter events by anonymized status (admin only)"),
        ("from_timestamp" = Option<String>, Query, description = "Return events at or after this timestamp (ISO 8601 format, e.g., 2026-03-14T00:00:00Z)"),
        ("to_timestamp" = Option<String>, Query, description = "Return events at or before this timestamp (ISO 8601 format, e.g., 2026-03-14T00:00:00Z)"),
        ("sort" = Option<String>, Query, description = "Sort order: asc (oldest first) or desc (newest first, default)"),
        ("sort_by" = Option<crate::models::SortBy>, Query, description = "Sort column: ledger (default), timestamp, or created_at"),
        ("topic_sym" = Option<String>, Query, description = "Filter by first topic symbol (uses topic_0_sym generated column index)"),
        ("topic_0" = Option<String>, Query, description = "Filter by exact value of topic[0] (e.g. 'transfer'). Uses the topic_0_sym generated-column index for O(log n) lookups."),
        ("topic_1" = Option<String>, Query, description = "Filter by exact value of topic[1]. Uses GIN index on event_data->'topic'."),
        ("topic_2" = Option<String>, Query, description = "Filter by exact value of topic[2]. Uses GIN index on event_data->'topic'."),
        ("topic_3" = Option<String>, Query, description = "Filter by exact value of topic[3]. Uses GIN index on event_data->'topic'."),
        ("search" = Option<String>, Query, description = "Full-text search query for event_data (searches all string values in the JSON)"),
        ("compact" = Option<bool>, Query, description = "Return event_data as a base64-encoded gzip-compressed JSON string instead of the full JSON object. Clients that need the full data can decode it; clients that only need metadata can ignore it. Default: false."),
        ("contract_id_prefix" = Option<String>, Query, description = "Filter events by contract ID prefix (minimum 4 alphanumeric characters, uses LIKE 'prefix%' with the contract_id index)."),
    ),
    responses(
        (status = 200, description = "Paginated list of events (JSON or NDJSON depending on Accept header). When compact=true, each event's event_data field is a base64-encoded gzip-compressed JSON string (Content-Encoding: gzip).",
            content(
                ("application/json" = Value),
                ("application/x-ndjson" = String),
            )
        ),
        (status = 400, description = "Invalid query parameters"),
    )
)]
#[instrument(skip(state, headers, extensions), fields(page = ?params.page, limit = ?params.limit, contract_id = ?params.contract_id))]
pub async fn get_events(
    State(state): State<AppState>,
    Query(params): Query<PaginationParams>,
    headers: HeaderMap,
    extensions: axum::http::Extensions,
) -> Result<impl IntoResponse, AppError> {
    let tenant_id = extract_tenant_id(&extensions).map(|s| s.to_owned());
    let tenant_id = tenant_id.as_deref();
    // Validate ledger range
    if let Some(from) = params.from_ledger {
        validate_ledger_param("from_ledger", from)?;
    }
    if let Some(to) = params.to_ledger {
        validate_ledger_param("to_ledger", to)?;
    }
    if let (Some(from), Some(to)) = (params.from_ledger, params.to_ledger) {
        if from > to {
            return Err(AppError::Validation(
                "from_ledger must be <= to_ledger".to_string(),
            ));
        }
    }

    // Validate and parse timestamp range
    let from_ts = if let Some(ref ts) = params.from_timestamp {
        Some(validate_timestamp(ts)?)
    } else {
        None
    };
    let to_ts = if let Some(ref ts) = params.to_timestamp {
        Some(validate_timestamp(ts)?)
    } else {
        None
    };

    // Validate timestamp range
    if let (Some(from), Some(to)) = (from_ts, to_ts) {
        if from > to {
            return Err(AppError::Validation(
                "from_timestamp must be <= to_timestamp".to_string(),
            ));
        }
    }

    if params.anonymized.is_some() && !is_admin_request(&headers, &state.config.admin_api_keys) {
        return Err(AppError::Forbidden(
            "anonymized filter requires admin privileges".to_string(),
        ));
    }

    // Validate contract_id if provided
    if let Some(ref cid) = params.contract_id {
        validate_contract_id(cid)?;
    }

    // Parse and validate contract_ids if provided
    let contract_ids_list: Vec<String> = if let Some(ref cids) = params.contract_ids {
        let ids: Vec<&str> = cids.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
        
        if ids.is_empty() {
            return Err(AppError::Validation(
                "contract_ids parameter is empty".to_string(),
            ));
        }
        
        if ids.len() > PaginationParams::MAX_CONTRACT_IDS_FILTER {
            return Err(AppError::Validation(
                format!(
                    "contract_ids exceeds maximum of {} IDs",
                    PaginationParams::MAX_CONTRACT_IDS_FILTER
                ),
            ));
        }
        
        for id in &ids {
            validate_contract_id(id)?;
        }
        
        ids.iter().map(|s| s.to_string()).collect()
    } else {
        Vec::new()
    };

    // Validate contract_id_prefix if provided (#459)
    if let Some(ref prefix) = params.contract_id_prefix {
        let trimmed = prefix.trim();
        if trimmed.len() < 4 {
            return Err(AppError::Validation(
                "contract_id_prefix must be at least 4 characters".to_string(),
            ));
        }
        if !trimmed.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(AppError::Validation(
                "contract_id_prefix must contain only alphanumeric characters".to_string(),
            ));
        }
    }

    let limit = params.limit();
    let columns = resolve_columns(&params)?;
    let sort_order = params.sort.unwrap_or(crate::models::SortOrder::Desc);
    let dir = sort_order.as_sql();
    let sort_by = params.sort_by.unwrap_or(crate::models::SortBy::Ledger);
    let sort_col = sort_by.as_sql_col();

    // Cursor-based path
    crate::metrics::record_pagination_strategy(params.cursor.is_some());
    if let Some(ref cursor_str) = params.cursor {
        let (cursor_tag, cursor_val_text, cursor_id) = decode_cursor_tagged(cursor_str)?;
        if cursor_tag != sort_by.as_tag() {
            return Err(crate::error::AppError::Validation(
                "cursor sort column does not match sort_by".to_string(),
            ));
        }

        let cursor_op = if sort_order == crate::models::SortOrder::Asc {
            ">"
        } else {
            "<"
        };

        let mut conditions: Vec<String> = vec![format!(
            "({col}, id) {op} ($1, $2)",
            col = sort_col,
            op = cursor_op
        )];
        let mut bind_idx: i32 = 3;

        if params.contract_id.is_some() {
            conditions.push(format!("contract_id = ${bind_idx}"));
            bind_idx += 1;
        }
        if !contract_ids_list.is_empty() {
            conditions.push(format!("contract_id = ANY(${bind_idx}::text[])"));
            bind_idx += 1;
        }
        if params.contract_id_prefix.is_some() {
            conditions.push(format!("contract_id LIKE ${bind_idx}"));
            bind_idx += 1;
        }
        if params.event_type.is_some() {
            conditions.push(format!("event_type = ${bind_idx}"));
            bind_idx += 1;
        }
        if params.from_ledger.is_some() {
            conditions.push(format!("ledger >= ${bind_idx}"));
            bind_idx += 1;
        }
        if params.to_ledger.is_some() {
            conditions.push(format!("ledger <= ${bind_idx}"));
            bind_idx += 1;
        }
        if params.ledger_hash.is_some() {
            conditions.push(format!("ledger_hash = ${bind_idx}"));
            bind_idx += 1;
        }
        if params.anonymized.is_some() {
            conditions.push(format!("anonymized = ${bind_idx}"));
            bind_idx += 1;
        }
        if params.in_successful_call.is_some() {
            conditions.push(format!("in_successful_call = ${bind_idx}"));
            bind_idx += 1;
        }
        if params.schema_version.is_some() {
            conditions.push(format!("schema_version = ${bind_idx}"));
            bind_idx += 1;
        }
        if params.topic_sym.is_some() {
            conditions.push(format!("topic_0_sym = ${bind_idx}"));
            bind_idx += 1;
        }
        if topic_filter.is_some() {
            conditions.push(format!("event_data->'topic' @> ${bind_idx}::jsonb"));
            bind_idx += 1;
        }
        // topic_0 uses the generated topic_0_sym column index for efficiency
        if params.topic_0.is_some() {
            conditions.push(format!("topic_0_sym = ${bind_idx}"));
            bind_idx += 1;
        }
        // topic_1/2/3 use GIN index on event_data->'topic'
        if params.topic_1.is_some() {
            conditions.push(format!("event_data->'topic'->1 @> ${bind_idx}::jsonb"));
            bind_idx += 1;
        }
        if params.topic_2.is_some() {
            conditions.push(format!("event_data->'topic'->2 @> ${bind_idx}::jsonb"));
            bind_idx += 1;
        }
        if params.topic_3.is_some() {
            conditions.push(format!("event_data->'topic'->3 @> ${bind_idx}::jsonb"));
            bind_idx += 1;
        }

        let mut search_param_idx: Option<i32> = None;
        if params.search.is_some() {
            search_param_idx = Some(bind_idx);
            conditions.push(format!(
                "event_data_tsv @@ plainto_tsquery('english', ${})",
                bind_idx
            ));
            bind_idx += 1;
        }

        if params.data_pattern.is_some() {
            conditions.push(format!(
                "jsonb_path_exists(event_data, ${bind_idx}::jsonpath)"
            ));
            bind_idx += 1;
        }
        if from_ts.is_some() {
            conditions.push(format!("timestamp >= ${bind_idx}"));
            bind_idx += 1;
        }
        if to_ts.is_some() {
            conditions.push(format!("timestamp <= ${bind_idx}"));
            bind_idx += 1;
        }
        // Exclusion filters (Issue #463)
        if !exclude_contract_ids_list.is_empty() {
            conditions.push(format!("contract_id != ALL(${bind_idx}::text[])"));
            bind_idx += 1;
        }
        if !exclude_event_types_list.is_empty() {
            conditions.push(format!("event_type != ALL(${bind_idx}::text[])"));
            bind_idx += 1;
        }
        // Geospatial filtering (Issue #465)
        if let (Some(lat), Some(lon), Some(radius)) = (params.near_lat, params.near_lon, params.radius_km) {
            conditions.push(format!(
                "earth_distance(ll_to_earth(latitude, longitude), ll_to_earth(${bind_idx}, ${bind_idx + 1})) <= ${bind_idx + 2} * 1000"
            ));
            bind_idx += 3;
        }
        maybe_add_tenant_condition(&mut conditions, &mut bind_idx, tenant_id);
        // Issue #1063: filter to one network when indexing multiple
        // networks concurrently from this deployment. Omitted returns
        // events from every configured network.
        if params.network.is_some() {
            conditions.push(format!("chain_id = ${bind_idx}"));
            bind_idx += 1;
        }

        let where_clause = format!("WHERE {}", conditions.join(" AND "));

        let mut select_cols = columns.to_vec();
        if !select_cols.contains(&sort_col) {
            select_cols.push(sort_col);
        }
        if !select_cols.contains(&"id") {
            select_cols.push("id");
        }
        if !select_cols.contains(&"created_at") {
            select_cols.push("created_at");
        }

        let mut select_query_cols: Vec<String> = select_cols.iter().map(|v| v.to_string()).collect();
        if params.rank_by_relevance.unwrap_or(false) {
            if params.search.is_none() {
                return Err(AppError::Validation(
                    "rank_by_relevance requires search query".to_string(),
                ));
            }
            if select_query_cols
                .iter()
                .all(|c| c != "relevance_score")
            {
                select_cols.push("relevance_score");
            }
            if let Some(search_idx) = search_param_idx {
                select_query_cols.push(format!(
                    "ts_rank(event_data_tsv, plainto_tsquery('english', ${})) AS relevance_score",
                    search_idx
                ));
            }
        }

        // Defence-in-depth: re-validate each column before SQL interpolation
        for col in &select_cols {
            if !models::PaginationParams::validate_column_name(col) {
                return Err(AppError::Validation(format!(
                    "invalid column name: {}",
                    col
                )));
            }
        }

        let order_clause = if params.rank_by_relevance.unwrap_or(false) {
            "relevance_score DESC, id DESC".to_string()
        } else if sort_col == "ledger" {
            // Issue #1065: within a ledger, order deterministically by the
            // decoded TOID fields before falling back to insertion id.
            format!(
                "ledger {dir}, tx_index {dir} NULLS LAST, op_index {dir} NULLS LAST, event_index {dir} NULLS LAST, id {dir}",
                dir = dir
            )
        } else {
            format!("{col} {dir}, id {dir}", col = sort_col, dir = dir)
        };

        let query_str = format!(
            "SELECT {} FROM events {} ORDER BY {} LIMIT ${}",
            select_query_cols.join(", "),
            where_clause,
            order_clause,
            bind_idx,
        );

        let mut q = sqlx::query(&query_str);
        match sort_by {
            crate::models::SortBy::Ledger => {
                let val = cursor_val_text.parse::<i64>().map_err(|_| {
                    crate::error::AppError::Validation("invalid ledger cursor".to_string())
                })?;
                q = q.bind(val).bind(cursor_id);
            }
            crate::models::SortBy::Timestamp | crate::models::SortBy::CreatedAt => {
                let ts = cursor_val_text
                    .parse::<chrono::DateTime<chrono::Utc>>()
                    .map_err(|_| {
                        crate::error::AppError::Validation("invalid timestamp cursor".to_string())
                    })?;
                q = q.bind(ts).bind(cursor_id);
            }
        }
        if let Some(ref cid) = params.contract_id {
            q = q.bind(cid);
        }
        if !contract_ids_list.is_empty() {
            q = q.bind(&contract_ids_list);
        }
        if let Some(ref prefix) = params.contract_id_prefix {
            q = q.bind(format!("{}%", prefix.trim()));
        }
        if let Some(ref et) = params.event_type {
            q = q.bind(et);
        }
        if let Some(fl) = params.from_ledger {
            q = q.bind(fl);
        }
        if let Some(tl) = params.to_ledger {
            q = q.bind(tl);
        }
        if let Some(ref hash) = params.ledger_hash {
            q = q.bind(hash);
        }
        if let Some(anonymized) = params.anonymized {
            q = q.bind(anonymized);
        }
        if let Some(isc) = params.in_successful_call {
            q = q.bind(isc);
        }
        if let Some(sv) = params.schema_version {
            q = q.bind(sv);
        }
        if let Some(ref ts) = params.topic_sym {
            q = q.bind(ts);
        }
        if let Some(ref tf) = topic_filter {
            q = q.bind(tf.to_string());
        }
        if let Some(ref t0) = params.topic_0 {
            q = q.bind(t0);
        }
        if let Some(ref t1) = params.topic_1 {
            q = q.bind(serde_json::json!(t1).to_string());
        }
        if let Some(ref t2) = params.topic_2 {
            q = q.bind(serde_json::json!(t2).to_string());
        }
        if let Some(ref t3) = params.topic_3 {
            q = q.bind(serde_json::json!(t3).to_string());
        }
        if let Some(ref data_pattern) = params.data_pattern {
            let pattern = params.pattern.as_deref().ok_or_else(|| {
                AppError::Validation("pattern is required when data_pattern is set".to_string())
            })?;
            let expr = format_jsonpath_filter(data_pattern, pattern);
            q = q.bind(expr);
        }
        if let Some(ref search) = params.search {
            q = q.bind(search);
        }
        if let Some(ts) = from_ts {
            q = q.bind(ts);
        }
        if let Some(ts) = to_ts {
            q = q.bind(ts);
        }
        // Bind exclusion filters (Issue #463)
        if !exclude_contract_ids_list.is_empty() {
            q = q.bind(&exclude_contract_ids_list);
        }
        if !exclude_event_types_list.is_empty() {
            q = q.bind(&exclude_event_types_list);
        }
        // Bind geospatial filters (Issue #465)
        if let (Some(lat), Some(lon), Some(radius)) = (params.near_lat, params.near_lon, params.radius_km) {
            q = q.bind(lat).bind(lon).bind(radius);
        }
        if let Some(tid) = tenant_id {
            q = q.bind(tid);
        }
        if let Some(ref network) = params.network {
            q = q.bind(network);
        }
        q = q.bind(limit);

        let _db_span = info_span!("db_query", query_type = "get_events_cursor").entered();
        let rows = timed_query(
            q.fetch_all(&state.read_pool),
            "get_events_cursor",
            state.config.slow_query_threshold_ms,
            None,
        )
        .await?;
        drop(_db_span);

        let has_more = rows.len() as i64 == limit;
        let next_cursor = if let (true, Some(last)) = (has_more, rows.last()) {
            let last_id: uuid::Uuid = last.try_get("id")?;
            let last_val_text = match sort_by {
                crate::models::SortBy::Ledger => {
                    let v: i64 = last.try_get("ledger")?;
                    v.to_string()
                }
                crate::models::SortBy::Timestamp => {
                    let v: chrono::DateTime<chrono::Utc> = last.try_get("timestamp")?;
                    v.to_rfc3339()
                }
                crate::models::SortBy::CreatedAt => {
                    let v: chrono::DateTime<chrono::Utc> = last.try_get("created_at")?;
                    v.to_rfc3339()
                }
            };
            Some(encode_cursor_tagged(
                sort_by.as_tag(),
                &last_val_text,
                last_id,
            ))
        } else {
            None
        };

        let scval_format = crate::scval_format::parse_format(params.format.as_deref())
            .map_err(AppError::Validation)?;
        let events = rows_to_json_with_format(
            &rows,
            &columns,
            state.encryption_key.as_ref(),
            state.encryption_key_old.as_ref(),
            params.compact.unwrap_or(false),
            scval_format,
        )?;

        // Build ETag from last row's id + created_at
        let etag = rows.first().and_then(|row| {
            let id: Option<Uuid> = row.try_get("id").ok();
            let created_at: Option<DateTime<Utc>> = row.try_get("created_at").ok();
            id.zip(created_at)
                .map(|(id, ca)| compute_etag(&id, &ca, None))
        });

        // Check If-None-Match — return 304 if ETag matches
        if let Some(ref tag) = etag {
            if let Some(inm) = headers.get("if-none-match").and_then(|v| v.to_str().ok()) {
                if inm == tag {
                    let resp = axum::http::Response::builder()
                        .status(StatusCode::NOT_MODIFIED)
                        .header("ETag", tag.as_str())
                        .header("Cache-Control", "no-cache")
                        .body(axum::body::Body::empty())
                        .unwrap_or_else(|e| response_build_error(e));
                    return Ok(resp.into_response());
                }
            }
        }

        let want_ndjson = accepts_ndjson(&headers);
        if want_ndjson {
            let mut resp = ndjson_response(events.into_iter()).into_response();
            if let Some(ref tag) = etag {
                insert_etag_headers(resp.headers_mut(), tag);
            }
            return Ok(resp);
        }

        let compact_mode = params.compact.unwrap_or(false);
        let mut resp = json_response(json!({
            "data": events,
            "next_cursor": next_cursor,
            "limit": limit,
        }));
        if let Some(ref tag) = etag {
            insert_etag_headers(resp.headers_mut(), tag);
        }
        if compact_mode {
            resp.headers_mut().insert(
                "X-Event-Data-Encoding",
                axum::http::HeaderValue::from_static("gzip+base64"),
            );
        }
        return Ok(resp.into_response());
    }

    // Offset-based path (deprecated fallback)
    let offset = params.offset();
    let exact = params.exact_count.unwrap_or(false);

    let mut conditions: Vec<String> = Vec::new();
    let mut bind_idx: i32 = 1;

    if params.contract_id.is_some() {
        conditions.push(format!("contract_id = ${bind_idx}"));
        bind_idx += 1;
    }
    if !contract_ids_list.is_empty() {
        conditions.push(format!("contract_id = ANY(${bind_idx}::text[])"));
        bind_idx += 1;
    }
    if params.contract_id_prefix.is_some() {
        conditions.push(format!("contract_id LIKE ${bind_idx}"));
        bind_idx += 1;
    }
    if params.event_type.is_some() {
        conditions.push(format!("event_type = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.from_ledger.is_some() {
        conditions.push(format!("ledger >= ${bind_idx}"));
        bind_idx += 1;
    }
    if params.to_ledger.is_some() {
        conditions.push(format!("ledger <= ${bind_idx}"));
        bind_idx += 1;
    }
    if params.ledger_hash.is_some() {
        conditions.push(format!("ledger_hash = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.anonymized.is_some() {
        conditions.push(format!("anonymized = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.in_successful_call.is_some() {
        conditions.push(format!("in_successful_call = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.schema_version.is_some() {
        conditions.push(format!("schema_version = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.topic_sym.is_some() {
        conditions.push(format!("topic_0_sym = ${bind_idx}"));
        bind_idx += 1;
    }
    if topic_filter.is_some() {
        conditions.push(format!("event_data->'topic' @> ${bind_idx}::jsonb"));
        bind_idx += 1;
    }
    if params.search.is_some() {
        conditions.push(format!(
            "event_data_tsv @@ plainto_tsquery('english', ${bind_idx})"
        ));
        bind_idx += 1;
    }
    if from_ts.is_some() {
        conditions.push(format!("timestamp >= ${bind_idx}"));
        bind_idx += 1;
    }
    if to_ts.is_some() {
        conditions.push(format!("timestamp <= ${bind_idx}"));
        bind_idx += 1;
    }
    // Exclusion filters (Issue #463)
    if !exclude_contract_ids_list.is_empty() {
        conditions.push(format!("contract_id != ALL(${bind_idx}::text[])"));
        bind_idx += 1;
    }
    if !exclude_event_types_list.is_empty() {
        conditions.push(format!("event_type != ALL(${bind_idx}::text[])"));
        bind_idx += 1;
    }
    // Geospatial filtering (Issue #465)
    if let (Some(lat), Some(lon), Some(radius)) = (params.near_lat, params.near_lon, params.radius_km) {
        conditions.push(format!(
            "earth_distance(ll_to_earth(latitude, longitude), ll_to_earth(${bind_idx}, ${bind_idx + 1})) <= ${bind_idx + 2} * 1000"
        ));
        bind_idx += 3;
    }
    maybe_add_tenant_condition(&mut conditions, &mut bind_idx, tenant_id);

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let mut select_cols = columns.to_vec();
    if !select_cols.contains(&sort_col) {
        select_cols.push(sort_col);
    }
    if !select_cols.contains(&"id") {
        select_cols.push("id");
    }
    // Always fetch created_at for ETag computation
    if !select_cols.contains(&"created_at") {
        select_cols.push("created_at");
    }

    // Defence-in-depth: re-validate each column before SQL interpolation
    for col in &select_cols {
        if !models::PaginationParams::validate_column_name(col) {
            return Err(AppError::Validation(format!(
                "invalid column name: {}",
                col
            )));
        }
    }

    let query_str = format!(
        "SELECT {} FROM events {} ORDER BY {col} {dir}, id {dir} LIMIT ${} OFFSET ${}",
        select_cols.join(", "),
        where_clause,
        col = sort_col,
        dir = dir,
        bind_idx,
        bind_idx + 1,
    );

    let mut q = sqlx::query(&query_str);
    if let Some(ref cid) = params.contract_id {
        q = q.bind(cid);
    }
    if !contract_ids_list.is_empty() {
        q = q.bind(&contract_ids_list);
    }
    if let Some(ref prefix) = params.contract_id_prefix {
        q = q.bind(format!("{}%", prefix.trim()));
    }
    if let Some(ref et) = params.event_type {
        q = q.bind(et);
    }
    if let Some(fl) = params.from_ledger {
        q = q.bind(fl);
    }
    if let Some(tl) = params.to_ledger {
        q = q.bind(tl);
    }
    if let Some(ref hash) = params.ledger_hash {
        q = q.bind(hash);
    }
    if let Some(anonymized) = params.anonymized {
        q = q.bind(anonymized);
    }
    if let Some(isc) = params.in_successful_call {
        q = q.bind(isc);
    }
    if let Some(sv) = params.schema_version {
        q = q.bind(sv);
    }
    if let Some(ref ts) = params.topic_sym {
        q = q.bind(ts);
    }
    if let Some(ref tf) = topic_filter {
        q = q.bind(tf.to_string());
    }
    if let Some(ref search) = params.search {
        q = q.bind(search);
    }
    if let Some(ts) = from_ts {
        q = q.bind(ts);
    }
    if let Some(ts) = to_ts {
        q = q.bind(ts);
    }
    // Bind exclusion filters (Issue #463)
    if !exclude_contract_ids_list.is_empty() {
        q = q.bind(&exclude_contract_ids_list);
    }
    if !exclude_event_types_list.is_empty() {
        q = q.bind(&exclude_event_types_list);
    }
    // Bind geospatial filters (Issue #465)
    if let (Some(lat), Some(lon), Some(radius)) = (params.near_lat, params.near_lon, params.radius_km) {
        q = q.bind(lat).bind(lon).bind(radius);
    }
    if let Some(tid) = tenant_id {
        q = q.bind(tid);
    }
    q = q.bind(limit).bind(offset);

    let _db_span = info_span!("db_query", query_type = "get_events_offset").entered();
    let rows = timed_query(
        q.fetch_all(&state.read_pool),
        "get_events_offset",
        state.config.slow_query_threshold_ms,
        None,
    )
    .await?;
    drop(_db_span);

    let has_more = rows.len() as i64 == limit;
    let next_cursor = if let (true, Some(last)) = (has_more, rows.last()) {
        let last_id: uuid::Uuid = last.try_get("id")?;
        let last_val_text = match sort_by {
            crate::models::SortBy::Ledger => {
                let v: i64 = last.try_get("ledger")?;
                v.to_string()
            }
            crate::models::SortBy::Timestamp => {
                let v: chrono::DateTime<chrono::Utc> = last.try_get("timestamp")?;
                v.to_rfc3339()
            }
            crate::models::SortBy::CreatedAt => {
                let v: chrono::DateTime<chrono::Utc> = last.try_get("created_at")?;
                v.to_rfc3339()
            }
        };
        Some(encode_cursor_tagged(
            sort_by.as_tag(),
            &last_val_text,
            last_id,
        ))
    } else {
        None
    };

    let events = rows_to_json(
        &rows,
        &columns,
        state.encryption_key.as_ref(),
        state.encryption_key_old.as_ref(),
        params.compact.unwrap_or(false),
    )?;

    let (total, approximate): (i64, bool) = if exact || !conditions.is_empty() {
        let count_str = format!("SELECT COUNT(*) FROM events {}", where_clause);
        let mut cq = sqlx::query_scalar::<_, i64>(&count_str);
        if let Some(ref cid) = params.contract_id {
            cq = cq.bind(cid);
        }
        if let Some(ref et) = params.event_type {
            cq = cq.bind(et);
        }
        if let Some(fl) = params.from_ledger {
            cq = cq.bind(fl);
        }
        if let Some(tl) = params.to_ledger {
            cq = cq.bind(tl);
        }
        if let Some(ref hash) = params.ledger_hash {
            cq = cq.bind(hash);
        }
        if let Some(anonymized) = params.anonymized {
            cq = cq.bind(anonymized);
        }
        if let Some(isc) = params.in_successful_call {
            cq = cq.bind(isc);
        }
        if let Some(sv) = params.schema_version {
            cq = cq.bind(sv);
        }
        if let Some(ref ts) = params.topic_sym {
            cq = cq.bind(ts);
        }
        if let Some(ref tf) = topic_filter {
            cq = cq.bind(tf.to_string());
        }
        if let Some(ref search) = params.search {
            cq = cq.bind(search);
        }
        if let Some(ts) = from_ts {
            cq = cq.bind(ts);
        }
        if let Some(ts) = to_ts {
            cq = cq.bind(ts);
        }
        // Bind exclusion filters for count query (Issue #463)
        if !exclude_contract_ids_list.is_empty() {
            cq = cq.bind(&exclude_contract_ids_list);
        }
        if !exclude_event_types_list.is_empty() {
            cq = cq.bind(&exclude_event_types_list);
        }
        // Bind geospatial filters for count query (Issue #465)
        if let (Some(lat), Some(lon), Some(radius)) = (params.near_lat, params.near_lon, params.radius_km) {
            cq = cq.bind(lat).bind(lon).bind(radius);
        }
        if let Some(tid) = tenant_id {
            cq = cq.bind(tid);
        }
        let _count_span = info_span!("db_query", query_type = "count_events").entered();
        let count = cq.fetch_one(&state.read_pool).await?;
        drop(_count_span);
        (count, false)
    } else {
        // In multi-tenant mode we can't use the pg_class estimate (it's for the whole table).
        // Fall back to an exact count scoped to the tenant.
        if tenant_id.is_some() {
            let count =
                sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM events WHERE tenant_id = $1")
                    .bind(tenant_id)
                    .fetch_one(&state.read_pool)
                    .await?;
            (count, false)
        } else {
            // query_scalar! gives compile-time SQL verification via the .sqlx offline cache.
            let count = sqlx::query_scalar!(
                "SELECT reltuples::bigint AS estimate FROM pg_class WHERE relname = 'events'",
            )
            .fetch_one(&state.read_pool)
            .await?;
            (count, true)
        }
    };

    // Build ETag from last row's id + created_at + total
    let etag = rows.first().and_then(|row| {
        let id: Option<Uuid> = row.try_get("id").ok();
        let created_at: Option<DateTime<Utc>> = row.try_get("created_at").ok();
        id.zip(created_at)
            .map(|(id, ca)| compute_etag(&id, &ca, Some(total)))
    });

    // Check If-None-Match — return 304 if ETag matches
    if let Some(ref tag) = etag {
        if let Some(inm) = headers.get("if-none-match").and_then(|v| v.to_str().ok()) {
            if inm == tag {
                let resp = axum::http::Response::builder()
                    .status(StatusCode::NOT_MODIFIED)
                    .header("ETag", tag.as_str())
                    .header("Cache-Control", "no-cache")
                    .body(axum::body::Body::empty())
                    .unwrap_or_else(|e| response_build_error(e));
                return Ok(resp.into_response());
            }
        }
    }

    let body = json!({
        "data": events,
        "next_cursor": next_cursor,
        "total": total,
        "page": params.page.unwrap_or(1),
        "limit": limit,
        "approximate": approximate,
        "pagination": "offset — migrate to cursor parameter for better performance",
    });

    let mut body_obj = body.as_object().cloned().unwrap_or_default();
    
    // Add approximation metadata when using approximate count
    if approximate {
        // Get stats age and dead tuple ratio
        let stats_info: (Option<chrono::DateTime<chrono::Utc>>, Option<f64>) = sqlx::query_as(
            "SELECT last_analyze, CASE WHEN n_live_tup > 0 THEN (n_dead_tup::float / n_live_tup) * 100 ELSE 0 END \
             FROM pg_stat_user_tables WHERE relname = 'events'"
        )
        .fetch_optional(&state.read_pool)
        .await
        .unwrap_or(None)
        .map(|(last_analyze, error_pct)| (last_analyze, error_pct))
        .unwrap_or((None, None));
        
        if let Some(error_pct) = stats_info.1 {
            body_obj.insert("approximate_error_pct".to_string(), json!(error_pct.min(100.0)));
        }
        if let Some(last_analyze) = stats_info.0 {
            body_obj.insert("last_analyzed".to_string(), json!(last_analyze.to_rfc3339()));
        }
    }

    let body = serde_json::Value::Object(body_obj);

    // Content negotiation: return NDJSON when the client requests it (Issue #417)
    if accepts_ndjson(&headers) {
        let ndjson = ndjson_response(events.into_iter());
        return Ok(ndjson.into_response());
    }

    let mut response = Json(body).into_response();
    if let Some(ref tag) = etag {
        insert_etag_headers(response.headers_mut(), tag);
    }
    if params.compact.unwrap_or(false) {
        response.headers_mut().insert(
            "X-Event-Data-Encoding",
            axum::http::HeaderValue::from_static("gzip+base64"),
        );
    }
    Ok(response)
}

#[utoipa::path(
    get,
    path = "/v1/events/feed.rss",
    tag = "events",
    params(
        ("contract_id" = Option<String>, Query, description = "Filter feed by contract ID"),
        ("event_type" = Option<String>, Query, description = "Filter feed by event type"),
        ("from_ledger" = Option<i64>, Query, description = "Minimum ledger"),
        ("to_ledger" = Option<i64>, Query, description = "Maximum ledger"),
        ("limit" = Option<i64>, Query, description = "Maximum feed items, capped at 100")
    ),
    responses(
        (status = 200, description = "Atom feed for indexed events"),
        (status = 400, description = "Invalid filter", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse)
    )
)]
pub async fn get_events_feed(
    State(state): State<AppState>,
    Query(params): Query<PaginationParams>,
    extensions: axum::http::Extensions,
) -> Result<Response, AppError> {
    let tenant_id = extract_tenant_id(&extensions).map(|s| s.to_owned());
    let tenant_id = tenant_id.as_deref();

    if let Some(ref cid) = params.contract_id {
        validate_contract_id(cid)?;
    }
    if let Some(from) = params.from_ledger {
        validate_ledger_param("from_ledger", from)?;
    }
    if let Some(to) = params.to_ledger {
        validate_ledger_param("to_ledger", to)?;
    }
    if let (Some(from), Some(to)) = (params.from_ledger, params.to_ledger) {
        if from > to {
            return Err(AppError::Validation(
                "from_ledger must be <= to_ledger".to_string(),
            ));
        }
    }

    let cache_key = format!(
        "{:?}:{:?}:{:?}:{:?}:{:?}:{:?}",
        tenant_id,
        params.contract_id,
        params.event_type,
        params.from_ledger,
        params.to_ledger,
        params.limit
    );
    {
        let cache = events_feed_cache().lock().await;
        if let Some(entry) = cache.get(&cache_key) {
            if std::time::Instant::now() < entry.expires_at {
                return Ok(xml_response(entry.data.as_str().unwrap_or_default().to_string()));
            }
        }
    }

    let limit = params.limit().min(100);
    let mut query = QueryBuilder::<sqlx::Postgres>::new(
        "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data \
         FROM events",
    );
    if params.contract_id.is_some()
        || params.event_type.is_some()
        || params.from_ledger.is_some()
        || params.to_ledger.is_some()
        || tenant_id.is_some()
    {
        query.push(" WHERE ");
        let mut needs_and = false;
        if let Some(ref contract_id) = params.contract_id {
            query.push("contract_id = ");
            query.push_bind(contract_id);
            needs_and = true;
        }
        if let Some(event_type) = params.event_type {
            if needs_and {
                query.push(" AND ");
            }
            query.push("event_type = ");
            query.push_bind(event_type.to_string());
            needs_and = true;
        }
        if let Some(from_ledger) = params.from_ledger {
            if needs_and {
                query.push(" AND ");
            }
            query.push("ledger >= ");
            query.push_bind(from_ledger);
            needs_and = true;
        }
        if let Some(to_ledger) = params.to_ledger {
            if needs_and {
                query.push(" AND ");
            }
            query.push("ledger <= ");
            query.push_bind(to_ledger);
            needs_and = true;
        }
        if let Some(tid) = tenant_id {
            if needs_and {
                query.push(" AND ");
            }
            query.push("tenant_id = ");
            query.push_bind(tid);
        }
    }
    // Issue #1065: order deterministically within a ledger using the
    // decoded TOID fields before falling back to insertion id, so
    // re-indexing and replicas agree on order regardless of insert order.
    query.push(
        " ORDER BY ledger DESC, tx_index DESC NULLS LAST, op_index DESC NULLS LAST, event_index DESC NULLS LAST, id DESC LIMIT ",
    );
    query.push_bind(limit);

    let rows = query.build().fetch_all(&state.read_pool).await?;
    let feed = render_atom_feed(&rows)?;
    events_feed_cache().lock().await.insert(
        cache_key,
        CacheEntry {
            data: Value::String(feed.clone()),
            expires_at: std::time::Instant::now() + Duration::from_secs(60),
        },
    );
    Ok(xml_response(feed))
}

#[utoipa::path(
    get,
    path = "/v1/events/export",
    tag = "events",
    params(
        ("format" = Option<String>, Query, description = "Output format: `csv` (default) or `parquet`. \
            CSV output streams RFC 4180-compliant text with a header row. \
            Parquet output requires the `parquet` feature flag."),
        ("event_type" = Option<String>, Query, description = "Filter by event type: contract, diagnostic, system"),
        ("from_ledger" = Option<i64>, Query, description = "Return events at or after this ledger"),
        ("to_ledger" = Option<i64>, Query, description = "Return events at or before this ledger"),
        ("contract_id" = Option<String>, Query, description = "Filter by contract ID"),
        ("field_map" = Option<String>, Query, description = "Optional JSON object mapping source field names to target output names, e.g. {\"event_data\":\"raw_data\"}"),
    ),
    responses(
        (status = 200, description = "Exported events. \
            CSV: `Content-Type: text/csv`, header row `id,contract_id,event_type,tx_hash,ledger,timestamp,event_data,created_at`, \
            streamed with `Content-Disposition: attachment; filename=\"events.csv\"`. \
            Parquet: `Content-Type: application/octet-stream`, \
            `Content-Disposition: attachment; filename=\"events.parquet\"`."),
        (status = 400, description = "Invalid query parameters or unsupported format"),
        (status = 401, description = "API key required"),
    )
)]
pub async fn export_events(
    State(state): State<AppState>,
    Query(params): Query<ExportParams>,
    headers: HeaderMap,
) -> Result<Response<Body>, AppError> {
    if state.config.api_keys.is_empty() {
        return Err(AppError::Validation(
            "export endpoint requires API key authentication".to_string(),
        ));
    }

    if let (Some(from), Some(to)) = (params.from_ledger, params.to_ledger) {
        if from > to {
            return Err(AppError::Validation(
                "from_ledger must be <= to_ledger".to_string(),
            ));
        }
    }
    if let Some(from) = params.from_ledger {
        validate_ledger_param("from_ledger", from)?;
    }
    if let Some(to) = params.to_ledger {
        validate_ledger_param("to_ledger", to)?;
    }
    // Validate timestamp range
    if let (Some(from_ts), Some(to_ts)) = (params.from_timestamp, params.to_timestamp) {
        if from_ts >= to_ts {
            return Err(AppError::Validation(
                "from_timestamp must be < to_timestamp".to_string(),
            ));
        }
    }

    let fmt = params.format.as_deref().unwrap_or("csv");
    let want_csv = fmt == "csv" || fmt.is_empty();
    let want_parquet = fmt == "parquet";
    let want_jsonl = fmt == "jsonl" || fmt == "ndjson";

    if !want_csv && !want_parquet && !want_jsonl {
        return Err(AppError::Validation(format!(
            "unsupported format '{fmt}': use 'csv', 'parquet', or 'jsonl'"
        )));
    }

    // Compression support
    let accept_encoding = headers.get(axum::http::header::ACCEPT_ENCODING)
        .and_then(|v| v.to_str().ok()).unwrap_or("");
    let use_gzip = accept_encoding.contains("gzip");
    let use_br = accept_encoding.contains("br");

    let max_rows = state.config.export_max_rows as i64;
    let mut conditions: Vec<String> = Vec::new();
    let mut bind_idx: i32 = 1;

    if params.contract_id.is_some() {
        conditions.push(format!("contract_id = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.event_type.is_some() {
        conditions.push(format!("event_type = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.from_ledger.is_some() {
        conditions.push(format!("ledger >= ${bind_idx}"));
        bind_idx += 1;
    }
    if params.to_ledger.is_some() {
        conditions.push(format!("ledger <= ${bind_idx}"));
        bind_idx += 1;
    }
    if params.from_timestamp.is_some() {
        conditions.push(format!("timestamp >= ${bind_idx}"));
        bind_idx += 1;
    }
    if params.to_timestamp.is_some() {
        conditions.push(format!("timestamp <= ${bind_idx}"));
        bind_idx += 1;
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let query_str = format!(
        "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at \
         FROM events {where_clause} ORDER BY ledger ASC, id ASC LIMIT ${bind_idx}"
    );

    let mut q = sqlx::query(&query_str);
    if let Some(ref cid) = params.contract_id {
        q = q.bind(cid);
    }
    if let Some(ref et) = params.event_type {
        q = q.bind(et);
    }
    if let Some(fl) = params.from_ledger {
        q = q.bind(fl);
    }
    if let Some(tl) = params.to_ledger {
        q = q.bind(tl);
    }
    if let Some(from_ts) = params.from_timestamp {
        q = q.bind(from_ts);
    }
    if let Some(to_ts) = params.to_timestamp {
        q = q.bind(to_ts);
    }
    q = q.bind(max_rows);

    let rows = q.fetch_all(&state.pool).await?;

    // Parse optional field_map parameter (JSON object string) which maps
    // source field names -> target output field names.
    let field_map: Option<std::collections::HashMap<String, String>> = if let Some(ref fm) = params.field_map {
        match serde_json::from_str(fm) {
            Ok(m) => Some(m),
            Err(_) => {
                return Err(AppError::Validation("field_map must be a JSON object mapping source field names to target field names".to_string()));
            }
        }
    } else {
        None
    };

    // Validate that all source fields in the map are valid allowed fields.
    if let Some(ref fm) = field_map {
        for src in fm.keys() {
            if !models::PaginationParams::ALLOWED_FIELDS.contains(&src.as_str()) {
                return Err(AppError::Validation(format!("unknown source field in field_map: {}", src)));
            }
        }
    }

    // Get total count of available rows (for Content-Range header)
    let total_count: i64 = {
        let count_str = format!("SELECT COUNT(*) FROM events {}", where_clause);
        let mut cq = sqlx::query_scalar::<_, i64>(&count_str);
        if let Some(ref cid) = params.contract_id {
            cq = cq.bind(cid);
        }
        if let Some(ref et) = params.event_type {
            cq = cq.bind(et);
        }
        if let Some(fl) = params.from_ledger {
            cq = cq.bind(fl);
        }
        if let Some(tl) = params.to_ledger {
            cq = cq.bind(tl);
        }
        cq.fetch_one(&state.pool).await?
    };

    let returned_count = rows.len() as i64;
    let content_range = format!("items 0-{}/{}", returned_count.saturating_sub(1), total_count);

    // JSON Lines format
    if want_jsonl {
        let mut jsonl = String::new();
        // ...existing code...
        for row in &rows {
            // ...existing code...
        }
        let mut body: Vec<u8> = jsonl.into_bytes();
        let mut content_encoding = None;
        if use_gzip {
            body = compress_gzip(&body).await?;
            content_encoding = Some("gzip");
        } else if use_br {
            body = compress_brotli(&body).await?;
            content_encoding = Some("br");
        }
        let mut builder = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/x-ndjson")
            .header(header::CONTENT_DISPOSITION, "attachment; filename=\"events.jsonl\"")
            .header("Content-Range", content_range);
        if let Some(enc) = content_encoding {
            builder = builder.header(header::CONTENT_ENCODING, enc);
        }
        return Ok(builder.body(Body::from(body)).unwrap_or_else(|e| response_build_error(e)));
    }

    #[cfg(feature = "parquet")]
    if want_parquet {
        use crate::parquet_export::{write_events_parquet_with_field_map, EventRow};
        let event_rows: Vec<EventRow> = rows
            .iter()
            .map(|row| {
                Ok::<_, sqlx::Error>(EventRow {
                    id: row.try_get("id")?,
                    contract_id: row.try_get("contract_id")?,
                    event_type: row.try_get("event_type")?,
                    tx_hash: row.try_get("tx_hash")?,
                    ledger: row.try_get("ledger")?,
                    timestamp: row.try_get("timestamp")?,
                    event_data: row.try_get("event_data")?,
                    created_at: row.try_get("created_at")?,
                })
            })
            .collect::<Result<_, _>>()?;

        let mut bytes = write_events_parquet_with_field_map(&event_rows, field_map.as_ref())
            .map_err(|e| AppError::Internal(e.to_string()))?;
        let mut content_encoding = None;
        let mut filename = "events.parquet";
        if use_gzip {
            bytes = compress_gzip(&bytes).await?;
            content_encoding = Some("gzip");
            filename = "events.parquet.gz";
        } else if use_br {
            bytes = compress_brotli(&bytes).await?;
            content_encoding = Some("br");
            filename = "events.parquet.br";
        }
        let mut builder = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::CONTENT_DISPOSITION, format!("attachment; filename=\"{}\"", filename))
            .header("Content-Range", content_range);
        if let Some(enc) = content_encoding {
            builder = builder.header(header::CONTENT_ENCODING, enc);
        }
        return Ok(builder.body(Body::from(bytes)).unwrap_or_else(|e| response_build_error(e)));
    }

    // Default: CSV (RFC 4180)
    // Build each row as a Bytes chunk and stream them so the full result set
    // is never held in a single allocation.
    use bytes::Bytes;
    use futures::stream;

    let default_cols = [
        "id",
        "contract_id",
        "event_type",
        "tx_hash",
        "ledger",
        "timestamp",
        "event_data",
        "created_at",
    ];

    let header_names: Vec<String> = default_cols
        .iter()
        .map(|c| field_map.as_ref().and_then(|m| m.get(*c)).cloned().unwrap_or_else(|| (*c).to_string()))
        .collect();
    let csv_header = format!("{}\n", header_names.join(","));
    let mut csv_data = String::with_capacity(rows.len() * 128 + 128);
    csv_data.push_str(&csv_header);
    for row in &rows {
        let id: uuid::Uuid = row.try_get("id")?;
        let contract_id: String = row.try_get("contract_id")?;
        let event_type: String = row.try_get("event_type")?;
        let tx_hash: String = row.try_get("tx_hash")?;
        let ledger: i64 = row.try_get("ledger")?;
        let timestamp: chrono::DateTime<chrono::Utc> = row.try_get("timestamp")?;
        let event_data: serde_json::Value = row.try_get("event_data")?;
        let created_at: chrono::DateTime<chrono::Utc> = row.try_get("created_at")?;
        let data_str = event_data.to_string();
        let mut values: Vec<String> = Vec::with_capacity(default_cols.len());
        for col in &default_cols {
            match *col {
                "id" => values.push(csv_escape_field(&id.to_string())),
                "contract_id" => values.push(csv_escape_field(&contract_id)),
                "event_type" => values.push(csv_escape_field(&event_type)),
                "tx_hash" => values.push(csv_escape_field(&tx_hash)),
                "ledger" => values.push(ledger.to_string()),
                "timestamp" => values.push(csv_escape_field(&timestamp.to_rfc3339())),
                "event_data" => values.push(csv_escape_field(&data_str)),
                "created_at" => values.push(csv_escape_field(&created_at.to_rfc3339())),
                _ => {}
            }
        }
        let line = format!("{}\n", values.join(","));
        csv_data.push_str(&line);
    }
    let mut body: Vec<u8> = csv_data.into_bytes();
    let mut content_encoding = None;
    let mut filename = "events.csv";
    if use_gzip {
        body = compress_gzip(&body).await?;
        content_encoding = Some("gzip");
        filename = "events.csv.gz";
    } else if use_br {
        body = compress_brotli(&body).await?;
        content_encoding = Some("br");
        filename = "events.csv.br";
    }
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/csv")
        .header(header::CONTENT_DISPOSITION, format!("attachment; filename=\"{}\"", filename))
        .header("Content-Range", content_range);
    if let Some(enc) = content_encoding {
        builder = builder.header(header::CONTENT_ENCODING, enc);
    }
    Ok(builder.body(Body::from(body)).unwrap_or_else(|e| response_build_error(e)))
}

#[utoipa::path(
    get,
    path = "/v1/events/recent",
    tag = "events",
    params(
        ("limit" = Option<i64>, Query, description = "Number of most-recent events to return, 1–100 (default: 20)"),
        ("event_type" = Option<crate::models::EventType>, Query, description = "Filter by event type: contract, diagnostic, system"),
        ("contract_id" = Option<String>, Query, description = "Filter by contract ID"),
        ("cursor" = Option<String>, Query, description = "Cursor for pagination (opaque, URL-safe)"),
    ),
    responses(
        (status = 200, description = "Most recently indexed events in descending ledger order"),
        (status = 400, description = "Invalid query parameters or unsupported ledger range filter"),
    )
)]
pub async fn get_recent_events(
    State(state): State<AppState>,
    Query(params): Query<RecentParams>,
) -> Result<Json<Value>, AppError> {
    if params.from_ledger.is_some() || params.to_ledger.is_some() {
        return Err(AppError::Validation(
            "from_ledger and to_ledger are not supported on /v1/events/recent".to_string(),
        ));
    }
    if let Some(ref cid) = params.contract_id {
        validate_contract_id(cid)?;
    }

    let limit = params.limit.unwrap_or(20).clamp(1, 100);

    let mut conditions: Vec<String> = Vec::new();
    let mut bind_idx: i32 = 1;

    if params.contract_id.is_some() {
        conditions.push(format!("contract_id = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.event_type.is_some() {
        conditions.push(format!("event_type = ${bind_idx}"));
        bind_idx += 1;
    }

    // Handle cursor-based pagination
    if let Some(ref cursor_str) = params.cursor {
        let (cursor_ledger, cursor_id) = decode_cursor(cursor_str)?;
        conditions.push(format!(
            "(ledger, id) < (${}, ${})",
            bind_idx, bind_idx + 1
        ));
        bind_idx += 2;
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let query_str = format!(
        "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at, schema_version, asset_code, asset_issuer, 0::bigint AS total_count \
         FROM events {} ORDER BY ledger DESC, id DESC LIMIT ${}",
        where_clause, bind_idx,
    );

    let mut q = sqlx::query_as::<_, crate::models::Event>(&query_str);
    if let Some(ref cid) = params.contract_id {
        q = q.bind(cid);
    }
    if let Some(ref et) = params.event_type {
        q = q.bind(et);
    }
    if let Some(ref cursor_str) = params.cursor {
        let (cursor_ledger, cursor_id) = decode_cursor(cursor_str)?;
        q = q.bind(cursor_ledger);
        q = q.bind(cursor_id);
    }
    q = q.bind(limit + 1); // Fetch one extra to determine if there's a next page

    let rows = q.fetch_all(&state.pool).await?;
    let has_next = rows.len() > limit as usize;
    let rows = if has_next {
        &rows[..limit as usize]
    } else {
        &rows
    };

    let events: Vec<Value> = rows
        .iter()
        .map(|e| {
            filter_fields(
                e,
                crate::models::PaginationParams::ALLOWED_FIELDS,
                state.encryption_key.as_ref(),
                state.encryption_key_old.as_ref(),
            )
        })
        .collect();

    let mut response = json!({
        "data": events,
        "limit": limit,
    });

    if has_next && !rows.is_empty() {
        let last_event = &rows[rows.len() - 1];
        let next_cursor = encode_cursor(last_event.ledger, last_event.id);
        response["next_cursor"] = json!(next_cursor);
    }

    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/v1/events/contract/{contract_id}",
    tag = "events",
    params(
        ("contract_id" = String, Path, description = "Stellar contract ID (56-char, starts with C)"),
        ("page" = Option<i64>, Query, description = "Page number (default: 1)"),
        ("limit" = Option<i64>, Query, description = "Results per page, 1–100 (default: 20)"),
        ("exact_count" = Option<bool>, Query, description = "Use exact COUNT(*) instead of approximate (default: false)"),
        ("from_ledger" = Option<i64>, Query, description = "Return events at or after this ledger"),
        ("to_ledger" = Option<i64>, Query, description = "Return events at or before this ledger"),
        ("sort" = Option<String>, Query, description = "Sort order: asc (oldest first) or desc (newest first, default)"),
    ),
    responses(
        (status = 200, description = "Events for the given contract"),
        (status = 400, description = "Invalid contract_id format or ledger range", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 404, description = "No events found for contract", body = ErrorResponse),
        (status = 429, description = "Too many requests", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[instrument(skip(state, extensions), fields(contract_id = %contract_id))]
pub async fn get_events_by_contract(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Query(params): Query<PaginationParams>,
    extensions: axum::http::Extensions,
) -> Result<Json<Value>, AppError> {
    validate_contract_id(&contract_id)?;

    if let Some(from) = params.from_ledger {
        validate_ledger_param("from_ledger", from)?;
    }
    if let Some(to) = params.to_ledger {
        validate_ledger_param("to_ledger", to)?;
    }
    if let (Some(from), Some(to)) = (params.from_ledger, params.to_ledger) {
        if from > to {
            return Err(AppError::Validation(
                "from_ledger must be <= to_ledger".to_string(),
            ));
        }
    }

    let tenant_id = extract_tenant_id(&extensions).map(|s| s.to_owned());
    let tenant_id = tenant_id.as_deref();

    let limit = params.limit();
    let offset = params.offset();
    let exact = params.exact_count.unwrap_or(false);
    let columns = resolve_columns(&params)?;
    let dir = params
        .sort
        .unwrap_or(crate::models::SortOrder::Desc)
        .as_sql();

    let mut conditions: Vec<String> = vec!["contract_id = $1".to_string()];
    let mut bind_idx: i32 = 2;

    if params.from_ledger.is_some() {
        conditions.push(format!("ledger >= ${bind_idx}"));
        bind_idx += 1;
    }
    if params.to_ledger.is_some() {
        conditions.push(format!("ledger <= ${bind_idx}"));
        bind_idx += 1;
    }
    maybe_add_tenant_condition(&mut conditions, &mut bind_idx, tenant_id);

    let where_clause = format!("WHERE {}", conditions.join(" AND "));
    let query_str = format!(
        "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at, 0::bigint AS total_count \
         FROM events {} ORDER BY ledger {dir} LIMIT ${} OFFSET ${}",
        where_clause, bind_idx, bind_idx + 1,
    );

    let mut q = sqlx::query_as::<_, models::Event>(&query_str).bind(&contract_id);
    if let Some(fl) = params.from_ledger {
        q = q.bind(fl);
    }
    if let Some(tl) = params.to_ledger {
        q = q.bind(tl);
    }
    if let Some(tid) = tenant_id {
        q = q.bind(tid);
    }
    q = q.bind(limit).bind(offset);

    let rows = timed_query(
        q.fetch_all(&state.read_pool),
        "get_events_by_contract",
        state.config.slow_query_threshold_ms,
        Some(&contract_id),
    )
    .await?;

    if rows.is_empty() {
        return Err(AppError::NotFound);
    }

    let events: Vec<Value> = rows
        .iter()
        .map(|e| {
            filter_fields(
                e,
                &columns,
                state.encryption_key.as_ref(),
                state.encryption_key_old.as_ref(),
            )
        })
        .collect();

    // Fetch total count. Skip cache in multi-tenant mode to avoid cross-tenant leakage.
    let total: i64 = if params.from_ledger.is_none() && params.to_ledger.is_none() && tenant_id.is_none() {
        if let Some(cached) = state.contract_count_cache.get(&contract_id).await {
            crate::metrics::update_contract_count_cache_hit_ratio(1, 0);
            cached
        } else {
            crate::metrics::update_contract_count_cache_hit_ratio(0, 1);
            // query_scalar! gives compile-time SQL verification via the .sqlx offline cache.
            let count: i64 =
                sqlx::query_scalar!("SELECT COUNT(*) FROM events WHERE contract_id = $1", &contract_id as &str)
                    .fetch_one(&state.pool)
                    .await?;
            state
                .contract_count_cache
                .insert(contract_id.clone(), count)
                .await;
            count
        }
    } else {
        let mut count_conditions: Vec<String> = vec!["contract_id = $1".to_string()];
        let mut cidx: i32 = 2;
        if params.from_ledger.is_some() {
            count_conditions.push(format!("ledger >= ${cidx}"));
            cidx += 1;
        }
        if params.to_ledger.is_some() {
            count_conditions.push(format!("ledger <= ${cidx}"));
            cidx += 1;
        }
        maybe_add_tenant_condition(&mut count_conditions, &mut cidx, tenant_id);
        let count_str = format!(
            "SELECT COUNT(*) FROM events WHERE {}",
            count_conditions.join(" AND ")
        );
        let mut cq = sqlx::query_scalar::<_, i64>(&count_str).bind(&contract_id);
        if let Some(fl) = params.from_ledger {
            cq = cq.bind(fl);
        }
        if let Some(tl) = params.to_ledger {
            cq = cq.bind(tl);
        }
        if let Some(tid) = tenant_id {
            cq = cq.bind(tid);
        }
        cq.fetch_one(&state.pool).await?
    };

    let mut response = json!({
        "data": events,
        "contract_id": contract_id,
        "total": total,
        "page": params.page.unwrap_or(1),
        "limit": limit,
        "approximate": false,
    });

    if let Some(fl) = params.from_ledger {
        response["from_ledger"] = json!(fl);
    }
    if let Some(tl) = params.to_ledger {
        response["to_ledger"] = json!(tl);
    }

    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/v1/events/tx/{tx_hash}",
    tag = "events",
    params(
        ("tx_hash" = String, Path, description = "Transaction hash (64 hex chars, case-insensitive — normalized to lowercase)"),
    ),
    responses(
        (status = 200, description = "Events for the given transaction (empty array if none)"),
        (status = 400, description = "Invalid tx_hash format", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 429, description = "Too many requests", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[instrument(skip(state, extensions), fields(tx_hash = %tx_hash))]
pub async fn get_events_by_tx(
    State(state): State<AppState>,
    Path(tx_hash): Path<String>,
    Query(params): Query<PaginationParams>,
    extensions: axum::http::Extensions,
) -> Result<Json<Value>, AppError> {
    let tx_hash = tx_hash.to_lowercase();
    validate_tx_hash(&tx_hash)?;

    let tenant_id = extract_tenant_id(&extensions).map(|s| s.to_owned());
    let tenant_id = tenant_id.as_deref();

    let columns = resolve_columns(&params)?;

    let mut select_cols = columns.to_vec();
    if !select_cols.contains(&"ledger") {
        select_cols.push("ledger");
    }
    if !select_cols.contains(&"id") {
        select_cols.push("id");
    }

    let mut conditions: Vec<String> = vec!["tx_hash = $1".to_string()];
    let mut bind_idx: i32 = 2;
    maybe_add_tenant_condition(&mut conditions, &mut bind_idx, tenant_id);

    let query_str = format!(
        "SELECT {} FROM events WHERE {} ORDER BY ledger DESC, id DESC",
        select_cols.join(", "),
        conditions.join(" AND "),
    );

    let mut q = sqlx::query(&query_str).bind(&tx_hash);
    if let Some(tid) = tenant_id {
        q = q.bind(tid);
    }
    let rows = timed_query(
        q.fetch_all(&state.read_pool),
        "get_events_by_tx",
        state.config.slow_query_threshold_ms,
        Some(&tx_hash),
    )
    .await?;

    let total = rows.len() as i64;
    let events = rows_to_json(
        &rows,
        &columns,
        state.encryption_key.as_ref(),
        state.encryption_key_old.as_ref(),
        false,
    )?;

    Ok(Json(json!({
        "data": events,
        "tx_hash": tx_hash,
        "total": total,
        "approximate": false,
    })))
}

#[utoipa::path(
    get,
    path = "/v1/events/tx/{tx_hash}/related",
    tag = "events",
    params(
        ("tx_hash" = String, Path, description = "Root transaction hash (64 hex chars)"),
        ("depth" = Option<u8>, Query, description = "Reference traversal depth, default 1, max 3"),
    ),
    responses(
        (status = 200, description = "Events from transactions referenced by event_data", body = serde_json::Value),
        (status = 400, description = "Invalid tx_hash or depth", body = ErrorResponse),
    )
)]
pub async fn get_related_events_by_tx(
    State(state): State<AppState>,
    Path(tx_hash): Path<String>,
    Query(params): Query<RelatedTxParams>,
) -> Result<Json<Value>, AppError> {
    let root_tx_hash = tx_hash.to_lowercase();
    validate_tx_hash(&root_tx_hash)?;
    let max_depth = params.depth.unwrap_or(1);
    if max_depth > 3 {
        return Err(AppError::Validation("depth must be between 0 and 3".to_string()));
    }

    let mut seen = std::collections::HashSet::from([root_tx_hash.clone()]);
    let mut frontier = vec![root_tx_hash.clone()];
    let mut all_events = Vec::new();

    for depth in 0..=max_depth {
        if frontier.is_empty() {
            break;
        }
        let rows = sqlx::query(
            "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, event_data_decoded, created_at, schema_version, 0::bigint AS total_count
             FROM events WHERE tx_hash = ANY($1) ORDER BY ledger DESC, id DESC",
        )
        .bind(&frontier)
        .fetch_all(&state.read_pool)
        .await?;

        let mut next = Vec::new();
        for row in rows {
            let event_data: Value = row.try_get("event_data")?;
            collect_tx_refs(&event_data, &mut next);
            all_events.push(row_to_event_json(&row)?);
        }
        if depth == max_depth {
            break;
        }
        frontier = next
            .into_iter()
            .map(|h| h.to_lowercase())
            .filter(|h| validate_tx_hash(h).is_ok())
            .filter(|h| seen.insert(h.clone()))
            .collect();
    }

    let total = all_events.len();
    Ok(Json(json!({ "tx_hash": root_tx_hash, "depth": max_depth, "data": all_events, "total": total })))
}

pub(super) fn row_to_event_json(row: &sqlx::postgres::PgRow) -> Result<Value, AppError> {
    Ok(json!({
        "id": row.try_get::<Uuid, _>("id")?,
        "contract_id": row.try_get::<String, _>("contract_id")?,
        "event_type": row.try_get::<String, _>("event_type")?,
        "tx_hash": row.try_get::<String, _>("tx_hash")?,
        "ledger": row.try_get::<i64, _>("ledger")?,
        "timestamp": row.try_get::<DateTime<Utc>, _>("timestamp")?,
        "event_data": row.try_get::<Value, _>("event_data")?,
        "event_data_decoded": row.try_get::<Option<Value>, _>("event_data_decoded")?,
        "created_at": row.try_get::<DateTime<Utc>, _>("created_at")?,
        "schema_version": row.try_get::<i32, _>("schema_version")?,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/events/tx/batch",
    tag = "events",
    request_body = crate::models::BatchTxRequest,
    responses(
        (status = 200, description = "Map of tx_hash -> events for all requested hashes"),
        (status = 400, description = "Invalid hashes or too many hashes", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 429, description = "Too many requests", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
#[instrument(skip(state, body))]
pub async fn bulk_insert_events(
    State(state): State<AppState>,
    Json(body): Json<crate::models::BulkInsertRequest>,
) -> Result<Json<crate::models::BulkInsertResponse>, AppError> {
    const MAX_BATCH_SIZE: usize = 1000;
    
    if body.events.is_empty() {
        return Err(AppError::Validation("events list cannot be empty".to_string()));
    }
    
    if body.events.len() > MAX_BATCH_SIZE {
        return Err(AppError::Validation(format!(
            "events list exceeds maximum of {} events",
            MAX_BATCH_SIZE
        )));
    }
    
    let mut inserted = 0i64;
    let mut skipped = 0i64;
    let mut failed = 0i64;
    let mut errors = Vec::new();
    
    for event in body.events {
        // Validate contract_id
        if let Err(e) = validate_contract_id(&event.contract_id) {
            failed += 1;
            errors.push(format!("Invalid contract_id: {}", e));
            continue;
        }
        
        // Validate tx_hash
        if let Err(e) = validate_tx_hash(&event.tx_hash) {
            failed += 1;
            errors.push(format!("Invalid tx_hash: {}", e));
            continue;
        }
        
        // Validate event_type
        let event_type = match event.event_type.as_str() {
            "contract" | "diagnostic" | "system" => event.event_type.clone(),
            _ => {
                failed += 1;
                errors.push(format!("Invalid event_type: {}", event.event_type));
                continue;
            }
        };
        
        let id = Uuid::new_v4();
        let result = sqlx::query(
            "INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, event_data_normalized, ledger_hash, in_successful_call, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
             ON CONFLICT (contract_id, tx_hash, ledger, event_type) DO NOTHING"
        )
        .bind(id)
        .bind(&event.contract_id)
        .bind(&event_type)
        .bind(&event.tx_hash)
        .bind(event.ledger)
        .bind(event.timestamp)
        .bind(&event.event_data)
        .bind(&event.event_data_normalized)
        .bind(&event.ledger_hash)
        .bind(event.in_successful_call.unwrap_or(false))
        .execute(&state.pool)
        .await;
        
        match result {
            Ok(result) => {
                if result.rows_affected() > 0 {
                    inserted += 1;
                } else {
                    skipped += 1;
                }
            }
            Err(e) => {
                failed += 1;
                errors.push(format!("Database error: {}", e));
            }
        }
    }
    
    Ok(Json(crate::models::BulkInsertResponse {
        inserted,
        skipped,
        failed,
        errors,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/admin/events/bulk",
    tag = "Admin",
    request_body = crate::models::BulkInsertRequest,
    responses(
        (status = 200, description = "Bulk insert result", body = crate::models::BulkInsertResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 429, description = "Too many requests", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse),
    )
)]
pub async fn get_events_by_tx_batch(
    State(state): State<AppState>,
    Json(body): Json<crate::models::BatchTxRequest>,
) -> Result<Json<Value>, AppError> {
    if body.hashes.len() > state.config.batch_tx_max_size {
        return Err(AppError::Validation(format!(
            "too many hashes: maximum is {}",
            state.config.batch_tx_max_size
        )));
    }

    // Validate all hashes; collect invalid ones for a helpful error.
    let invalid: Vec<String> = body
        .hashes
        .iter()
        .filter(|h| validate_tx_hash(h).is_err())
        .cloned()
        .collect();
    if !invalid.is_empty() {
        return Err(AppError::Validation(format!(
            "invalid tx_hash(es): {}",
            invalid.join(", ")
        )));
    }

    // Deduplicate hashes using HashSet
    let mut unique_hashes: std::collections::HashSet<String> = std::collections::HashSet::new();
    for h in &body.hashes {
        unique_hashes.insert(h.to_lowercase());
    }
    let deduplicated_count = unique_hashes.len();
    let hashes: Vec<String> = unique_hashes.into_iter().collect();

    // Single query using ANY().
    let rows = sqlx::query(
        "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at \
         FROM events WHERE tx_hash = ANY($1) ORDER BY tx_hash, ledger DESC, id DESC",
    )
    .bind(&hashes)
    .fetch_all(&state.read_pool)
    .await?;

    // Build result map: all requested hashes present, even if empty.
    let mut result: serde_json::Map<String, Value> = hashes
        .iter()
        .map(|h| (h.clone(), Value::Array(vec![])))
        .collect();

    let all_cols: &[&str] = &[
        "id",
        "contract_id",
        "event_type",
        "tx_hash",
        "ledger",
        "timestamp",
        "event_data",
        "created_at",
    ];
    for row in &rows {
        let tx_hash: String = row.try_get("tx_hash")?;
        let event_json = rows_to_json(
            std::slice::from_ref(row),
            all_cols,
            state.encryption_key.as_ref(),
            state.encryption_key_old.as_ref(),
            false,
        )?;
        if let Some(arr) = result.get_mut(&tx_hash).and_then(|v| v.as_array_mut()) {
            if let Some(ev) = event_json.into_iter().next() {
                arr.push(ev);
            }
        }
    }

    // Add deduplicated_count to response
    let mut response_obj = result;
    response_obj.insert("deduplicated_count".to_string(), Value::Number(deduplicated_count.into()));

    Ok(Json(Value::Object(response_obj)))
}

#[utoipa::path(
    get,
    path = "/v1/events/ledger-hash/{hash}",
    tag = "events",
    params(
        ("hash" = String, Path, description = "Ledger hash"),
    ),
    responses(
        (status = 200, description = "Events for the given ledger hash"),
        (status = 400, description = "Invalid parameters", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn get_events_by_ledger_hash(
    State(state): State<AppState>,
    Path(hash): Path<String>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<Value>, AppError> {
    let columns = resolve_columns(&params)?;
    let mut select_cols = columns.to_vec();
    if !select_cols.contains(&"ledger") {
        select_cols.push("ledger");
    }
    if !select_cols.contains(&"id") {
        select_cols.push("id");
    }

    let query_str = format!(
        "SELECT {} FROM events WHERE ledger_hash = $1 ORDER BY ledger DESC, id DESC",
        select_cols.join(", "),
    );
    let rows = sqlx::query(&query_str)
        .bind(&hash)
        .fetch_all(&state.read_pool)
        .await?;

    let events = rows_to_json(
        &rows,
        &columns,
        state.encryption_key.as_ref(),
        state.encryption_key_old.as_ref(),
        false,
    )?;
    Ok(Json(json!({
        "data": events,
        "ledger_hash": hash,
        "total": rows.len() as i64,
        "approximate": false,
    })))
}

/// Anonymize a specific event for GDPR compliance.
/// Replaces event_data with {"anonymized": true} and hashes tx_hash with SHA-256.
/// Idempotent: already-anonymized events return 200 without re-processing.
#[utoipa::path(
    post,
    path = "/v1/admin/events/{id}/anonymize",
    tag = "admin",
    params(
        ("id" = String, Path, description = "Event UUID"),
    ),
    responses(
        (status = 200, description = "Event anonymized"),
        (status = 404, description = "Event not found", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn anonymize_event(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, AppError> {
    use sha2::{Digest, Sha256};

    // Fetch current tx_hash and anonymized flag
    let row = sqlx::query("SELECT tx_hash, anonymized FROM events WHERE id = $1")
        .bind(id)
        .fetch_optional(&state.pool)
        .await?
        .ok_or(AppError::NotFound)?;

    let already_anonymized: bool = row.try_get("anonymized")?;
    if already_anonymized {
        tracing::info!(event_id = %id, "Event already anonymized, skipping");
        return Ok(Json(json!({ "id": id, "anonymized": true })));
    }

    let tx_hash: String = row.try_get("tx_hash")?;
    let hashed_tx = {
        let mut h = Sha256::new();
        h.update(tx_hash.as_bytes());
        format!("{:x}", h.finalize())
    };

    sqlx::query("UPDATE events SET anonymized = TRUE, event_data = $1, tx_hash = $2 WHERE id = $3")
        .bind(json!({"anonymized": true}))
        .bind(&hashed_tx)
        .bind(id)
        .execute(&state.pool)
        .await?;

    tracing::info!(event_id = %id, anonymized_at = %chrono::Utc::now(), "Event anonymized");

    Ok(Json(json!({ "id": id, "anonymized": true })))
}

/// Delete all events for a contract (GDPR right-to-erasure).
/// Optionally anonymize instead of deleting with `anonymize_only=true`.
/// Logs deletion request for audit purposes.
#[utoipa::path(
    delete,
    path = "/v1/admin/events/contract/{contract_id}",
    tag = "admin",
    params(
        ("contract_id" = String, Path, description = "Contract ID"),
        ("anonymize_only" = Option<bool>, Query, description = "If true, anonymize instead of deleting (default: false)"),
    ),
    responses(
        (status = 200, description = "Events deleted or anonymized"),
        (status = 400, description = "Invalid contract_id", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn delete_contract_events(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Value>, AppError> {
    validate_contract_id(&contract_id)?;

    let anonymize_only = params
        .get("anonymize_only")
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(false);

    let client_ip = headers
        .get("x-forwarded-for")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("unknown");

    if anonymize_only {
        // Anonymize: set anonymized=true and hash tx_hash
        use sha2::{Digest, Sha256};

        let rows = sqlx::query("SELECT id, tx_hash FROM events WHERE contract_id = $1 AND anonymized = FALSE")
            .bind(&contract_id)
            .fetch_all(&state.pool)
            .await?;

        let count = rows.len() as u64;

        for row in rows {
            let id: Uuid = row.try_get("id")?;
            let tx_hash: String = row.try_get("tx_hash")?;
            let hashed_tx = {
                let mut h = Sha256::new();
                h.update(tx_hash.as_bytes());
                format!("{:x}", h.finalize())
            };

            sqlx::query("UPDATE events SET anonymized = TRUE, event_data = $1, tx_hash = $2 WHERE id = $3")
                .bind(json!({"anonymized": true}))
                .bind(&hashed_tx)
                .bind(id)
                .execute(&state.pool)
                .await?;
        }

        tracing::info!(
            contract_id = %contract_id,
            count = count,
            client_ip = client_ip,
            timestamp = %chrono::Utc::now(),
            "Events anonymized for contract (GDPR)"
        );

        crate::metrics::record_events_deleted(count);

        Ok(Json(json!({
            "contract_id": contract_id,
            "action": "anonymized",
            "count": count,
        })))
    } else {
        // Hard delete
        let result = sqlx::query("DELETE FROM events WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&state.pool)
            .await?;

        let count = result.rows_affected();

        tracing::info!(
            contract_id = %contract_id,
            count = count,
            client_ip = client_ip,
            timestamp = %chrono::Utc::now(),
            "Events deleted for contract (GDPR right-to-erasure)"
        );

        crate::metrics::record_events_deleted(count);

        Ok(Json(json!({
            "contract_id": contract_id,
            "action": "deleted",
            "count": count,
        })))
    }
}

#[utoipa::path(
    get,
    path = "/v1/events/diff",
    tag = "events",
    params(
        ("from_ledger" = i64, Query, description = "Start ledger (inclusive, required)"),
        ("to_ledger" = i64, Query, description = "End ledger (inclusive, required)"),
    ),
    responses(
        (status = 200, description = "Event diff grouped by contract", body = crate::models::DiffResponse),
        (status = 400, description = "Invalid or missing ledger range", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
    )
)]
pub async fn get_events_diff(
    State(state): State<AppState>,
    Query(params): Query<crate::models::DiffParams>,
) -> Result<Json<Value>, AppError> {
    // Validate from_ledger and to_ledger are positive
    if params.from_ledger < 0 {
        return Err(AppError::Validation(
            "from_ledger must be a positive integer".to_string(),
        ));
    }
    if params.to_ledger < 0 {
        return Err(AppError::Validation(
            "to_ledger must be a positive integer".to_string(),
        ));
    }

    // Validate from_ledger < to_ledger
    if params.from_ledger >= params.to_ledger {
        return Err(AppError::Validation(
            "from_ledger must be less than to_ledger".to_string(),
        ));
    }

    // Validate ledger range does not exceed maximum
    let ledger_range = params.to_ledger - params.from_ledger;
    if ledger_range > state.config.max_ledger_range as i64 {
        return Err(AppError::Validation(format!(
            "ledger range exceeds maximum of {}",
            state.config.max_ledger_range
        )));
    }

    // Single query: count per (contract_id, event_type) in range
    let rows = sqlx::query(
        "SELECT contract_id, event_type, COUNT(*) AS cnt \
         FROM events \
         WHERE ledger >= $1 AND ledger <= $2 \
         GROUP BY contract_id, event_type",
    )
    .bind(params.from_ledger)
    .bind(params.to_ledger)
    .fetch_all(&state.read_pool)
    .await?;

    // Aggregate into per-contract map
    let mut map: std::collections::HashMap<String, crate::models::ContractDiff> =
        std::collections::HashMap::new();
    for row in &rows {
        let contract_id: String = row.try_get("contract_id")?;
        let event_type: String = row.try_get("event_type")?;
        let cnt: i64 = row.try_get("cnt")?;
        let entry = map
            .entry(contract_id.clone())
            .or_insert_with(|| crate::models::ContractDiff {
                contract_id,
                event_counts: std::collections::HashMap::new(),
                total: 0,
            });
        entry.event_counts.insert(event_type, cnt);
        entry.total += cnt;
    }

    let mut contracts: Vec<crate::models::ContractDiff> = map.into_values().collect();
    contracts.sort_by(|a, b| b.total.cmp(&a.total));

    Ok(Json(
        serde_json::to_value(crate::models::DiffResponse {
            from_ledger: params.from_ledger,
            to_ledger: params.to_ledger,
            contracts,
        })
        .map_err(|e| AppError::Internal(format!("serialization error: {e}")))?,
    ))
}

pub async fn replay_events(
    State(state): State<AppState>,
    Json(request): Json<models::ReplayRequest>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    // Validate ledger range
    if request.from_ledger > request.to_ledger {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "from_ledger must be <= to_ledger"
            })),
        ));
    }

    // Validate range size (max 10,000 ledgers)
    let range_size = request.to_ledger.saturating_sub(request.from_ledger) + 1;
    if range_size > 10_000 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "ledger range cannot exceed 10,000 ledgers"
            })),
        ));
    }

    // Check if this replica is the active indexer (holds the advisory lock)
    let is_active = state
        .indexer_state
        .is_active_indexer
        .load(std::sync::atomic::Ordering::Relaxed);
    if !is_active {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "replay endpoint only available on active indexer"
            })),
        ));
    }

    // Validate the requested range against the indexed window.
    let from = request.from_ledger as i64;
    let to = request.to_ledger as i64;

    let warning: Option<String> =
        match get_indexed_ledger_range(&state.pool).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
        })? {
            (None, None) => {
                // No events indexed at all — any range is entirely outside.
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "error": "requested range is entirely outside the indexed window: no events have been indexed yet"
                    })),
                ));
            }
            (Some(min_indexed), Some(max_indexed)) => {
                // Entirely before the indexed window.
                if to < min_indexed {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "error": format!(
                                "requested range [{from}, {to}] is entirely before the indexed window [{min_indexed}, {max_indexed}]"
                            )
                        })),
                    ));
                }
                // Entirely after the indexed window (future ledgers).
                if from > max_indexed {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "error": format!(
                                "requested range [{from}, {to}] is entirely after the indexed window [{min_indexed}, {max_indexed}]"
                            )
                        })),
                    ));
                }
                // Partial overlap — warn the caller.
                if from < min_indexed || to > max_indexed {
                    Some(format!(
                        "requested range [{from}, {to}] is partially outside the indexed window [{min_indexed}, {max_indexed}]; only ledgers [{}, {}] will be replayed",
                        from.max(min_indexed),
                        to.min(max_indexed),
                    ))
                } else {
                    None
                }
            }
            _ => None,
        };

    // Record the replay job metric
    crate::metrics::record_replay_job();

    // Spawn background task to handle the replay
    let pool = state.pool.clone();
    let rpc_url = state.config.stellar_rpc_url.clone();
    let from_ledger = request.from_ledger;
    let to_ledger = request.to_ledger;

    tokio::spawn(async move {
        if let Err(e) = execute_replay_job(pool, &rpc_url, from_ledger, to_ledger).await {
            tracing::error!(error = %e, "Replay job failed");
        }
    });

    let mut body = json!({
        "message": "replay job accepted",
        "from_ledger": request.from_ledger,
        "to_ledger": request.to_ledger
    });
    if let Some(w) = warning {
        body["warning"] = json!(w);
    }

    Ok((StatusCode::ACCEPTED, Json(body)))
}

/// Store event with idempotency using ON CONFLICT DO NOTHING
pub(super) async fn store_event_with_idempotency(
    pool: &sqlx::PgPool,
    event: &crate::models::SorobanEvent,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let ledger = i64::try_from(event.ledger)?;
    let timestamp = DateTime::parse_from_rfc3339(&event.ledger_closed_at)
        .map(|dt| dt.with_timezone(&chrono::Utc))?;

    let event_data = serde_json::json!({
        "value": event.value,
        "topic": event.topic
    });

    let contract_id = event.contract_id.clone();
    let event_type = event.event_type.clone();
    let tx_hash = event.tx_hash.clone();

    let rows_affected = crate::error::with_deadlock_retry(3, || {
        let pool = pool.clone();
        let contract_id = contract_id.clone();
        let event_type = event_type.clone();
        let tx_hash = tx_hash.clone();
        let event_data = event_data.clone();
        async move {
            sqlx::query(
                r#"
                INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (tx_hash, contract_id, event_type) DO NOTHING
                "#,
            )
            .bind(contract_id)
            .bind(event_type)
            .bind(tx_hash)
            .bind(ledger)
            .bind(timestamp)
            .bind(event_data)
            .execute(&pool)
            .await
            .map(|r| r.rows_affected())
        }
    })
    .await
    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

    Ok(rows_affected)
}

/// Validate event data against a contract's JSON Schema
#[utoipa::path(
    post,
    path = "/v1/admin/contracts/{contract_id}/validate",
    tag = "admin",
    params(
        ("contract_id" = String, Path, description = "Contract ID")
    ),
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "Validation result"),
        (status = 400, description = "Validation failed with error details"),
        (status = 404, description = "No schema registered for this contract"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn validate_event_data_against_schema(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Json(event_data): Json<serde_json::Value>,
) -> Result<impl IntoResponse, AppError> {
    validate_contract_id(&contract_id)?;

    let validator = state.schema_validator
        .as_ref()
        .ok_or_else(|| AppError::Internal("Schema validator not initialized".to_string()))?;

    match validator.validate_event_data(&contract_id, &event_data).await {
        None => Err(AppError::NotFound),
        Some((true, _)) => Ok((
            StatusCode::OK,
            Json(json!({
                "valid": true,
                "message": "Event data is valid"
            }))
        )),
        Some((false, errors)) => {
            let error_msg = format!("Event data validation failed with {} error(s)", errors.len());
            Err(AppError::ValidationWithDetails(error_msg, errors))
        }
    }
}

/// Start a background masking job for event data
#[utoipa::path(
    post,
    path = "/v1/admin/mask-events",
    tag = "admin",
    request_body = models::MaskEventsRequest,
    responses(
        (status = 200, description = "Masking job started", body = models::MaskEventsResponse),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn start_mask_events(
    State(state): State<AppState>,
    Json(req): Json<models::MaskEventsRequest>,
) -> Result<Json<models::MaskEventsResponse>, AppError> {
    let job_id = Uuid::new_v4().to_string();
    
    let pool = state.pool.clone();
    let contract_ids = req.contract_ids.clone();
    let job_id_clone = job_id.clone();
    
    tokio::spawn(async move {
        let _ = mask_events_background(&pool, contract_ids, &job_id_clone).await;
    });
    
    Ok(Json(models::MaskEventsResponse {
        job_id,
        status: "pending".to_string(),
    }))
}

pub(super) async fn mask_events_background(
    pool: &sqlx::PgPool,
    contract_ids: Option<Vec<String>>,
    _job_id: &str,
) -> Result<(), AppError> {
    let mut conditions: Vec<String> = vec!["event_data IS NOT NULL".to_string()];
    let mut bind_idx = 1;
    
    if let Some(ref ids) = contract_ids {
        if !ids.is_empty() {
            let placeholders = ids.iter().enumerate()
                .map(|(i, _)| format!("${}", bind_idx + i))
                .collect::<Vec<_>>()
                .join(",");
            conditions.push(format!("contract_id IN ({})", placeholders));
            bind_idx += ids.len() as i32;
        }
    }
    
    let where_clause = conditions.join(" AND ");
    let query_str = format!(
        "SELECT id, event_data FROM events WHERE {} ORDER BY id LIMIT 1000",
        where_clause
    );
    
    let mut q = sqlx::query(&query_str);
    if let Some(ref ids) = contract_ids {
        for id in ids {
            q = q.bind(id);
        }
    }
    
    let rows = q.fetch_all(pool).await?;
    
    for row in rows {
        let id: Uuid = row.try_get("id")?;
        let event_data: serde_json::Value = row.try_get("event_data")?;
        let masked_data = mask_event_data(&event_data);
        
        sqlx::query("UPDATE events SET event_data = $1 WHERE id = $2")
            .bind(&masked_data)
            .bind(id)
            .execute(pool)
            .await?;
    }
    
    Ok(())
}

pub(super) fn mask_event_data(data: &serde_json::Value) -> serde_json::Value {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    fn deterministic_hash(value: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }
    
    match data {
        serde_json::Value::Object(obj) => {
            let mut new_obj = serde_json::Map::new();
            for (key, value) in obj {
                let masked_value = mask_event_data(value);
                let key_lower = key.to_lowercase();
                
                if key_lower.contains("address") || key_lower.contains("account") {
                    if let serde_json::Value::String(s) = &masked_value {
                        let hash = deterministic_hash(s);
                        new_obj.insert(key.clone(), serde_json::Value::String(format!("G{:x}", hash)[..56].to_string()));
                        continue;
                    }
                }
                new_obj.insert(key.clone(), masked_value);
            }
            serde_json::Value::Object(new_obj)
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(mask_event_data).collect())
        }
        _ => data.clone(),
    }
}

/// GET /v1/events/temporal — time-based event queries (Issue #581).
///
/// Supports relative time expressions (`since=24h`, `before=1h`) and absolute
/// ISO 8601 timestamps (`from_timestamp`, `to_timestamp`). When `aggregate=true`
/// the response contains bucketed counts instead of raw events.
#[utoipa::path(
    get,
    path = "/v1/events/temporal",
    tag = "events",
    params(
        ("since" = Option<String>, Query, description = "Relative start of window, e.g. '24h', '1d', '7d', '30m', '1w'"),
        ("before" = Option<String>, Query, description = "Relative end of window (default: now)"),
        ("from_timestamp" = Option<String>, Query, description = "Absolute ISO 8601 start timestamp (mutually exclusive with since)"),
        ("to_timestamp" = Option<String>, Query, description = "Absolute ISO 8601 end timestamp"),
        ("contract_id" = Option<String>, Query, description = "Filter by contract ID"),
        ("event_type" = Option<String>, Query, description = "Filter by event type"),
        ("aggregate" = Option<bool>, Query, description = "Return aggregated bucket counts instead of raw events"),
        ("window" = Option<String>, Query, description = "Aggregation bucket size: 1m, 5m, 1h, 1d (default: 1h)"),
        ("limit" = Option<i64>, Query, description = "Max events or buckets (default: 100, max: 1000)"),
        ("page" = Option<i64>, Query, description = "Page number for offset pagination (default: 1)"),
    ),
    responses(
        (status = 200, description = "Temporal query results", body = models::TemporalQueryResponse),
        (status = 400, description = "Invalid parameters", body = models::ErrorResponse),
    )
)]
pub async fn get_temporal_events(
    State(state): State<AppState>,
    Query(params): Query<models::TemporalParams>,
) -> Result<Json<models::TemporalQueryResponse>, AppError> {
    let query_start = std::time::Instant::now();
    let now = Utc::now();

    // Resolve the time window boundaries.
    let from: DateTime<Utc> = if let Some(ref since) = params.since {
        if params.from_timestamp.is_some() {
            return Err(AppError::Validation(
                "'since' and 'from_timestamp' are mutually exclusive".to_string(),
            ));
        }
        let dur = parse_relative_duration(since)?;
        now - dur
    } else if let Some(ref ts) = params.from_timestamp {
        validate_timestamp(ts)?
    } else {
        return Err(AppError::Validation(
            "either 'since' or 'from_timestamp' is required".to_string(),
        ));
    };

    let to: DateTime<Utc> = if let Some(ref before) = params.before {
        if params.to_timestamp.is_some() {
            return Err(AppError::Validation(
                "'before' and 'to_timestamp' are mutually exclusive".to_string(),
            ));
        }
        let dur = parse_relative_duration(before)?;
        now - dur
    } else if let Some(ref ts) = params.to_timestamp {
        validate_timestamp(ts)?
    } else {
        now
    };

    if from >= to {
        return Err(AppError::Validation(
            "start of window must be before end of window".to_string(),
        ));
    }

    if let Some(ref cid) = params.contract_id {
        validate_contract_id(cid)?;
    }

    let limit = params.limit.unwrap_or(100).clamp(1, 1000);
    let page = params.page.unwrap_or(1).max(1);
    let offset = (page - 1) * limit;

    let aggregate = params.aggregate.unwrap_or(false);

    if aggregate {
        // Aggregation path: return bucketed event counts.
        let interval = match params.window.as_deref().unwrap_or("1h") {
            "1m" => "1 minute",
            "5m" => "5 minutes",
            "1h" => "1 hour",
            "1d" => "1 day",
            other => {
                return Err(AppError::Validation(format!(
                    "invalid window '{}': use 1m, 5m, 1h, or 1d",
                    other
                )));
            }
        };

        let mut conditions = vec![
            "timestamp >= $1".to_string(),
            "timestamp <= $2".to_string(),
        ];
        let mut bind_idx = 3usize;

        if params.contract_id.is_some() {
            conditions.push(format!("contract_id = ${bind_idx}"));
            bind_idx += 1;
        }
        if params.event_type.is_some() {
            conditions.push(format!("event_type = ${bind_idx}"));
            bind_idx += 1;
        }
        let _ = bind_idx;

        let where_clause = conditions.join(" AND ");
        let query_str = format!(
            "SELECT \
                date_trunc('{interval}', timestamp) AS bucket_start, \
                COUNT(*) AS event_count, \
                COUNT(DISTINCT contract_id) AS contract_count \
             FROM events \
             WHERE {where_clause} \
             GROUP BY date_trunc('{interval}', timestamp) \
             ORDER BY bucket_start ASC \
             LIMIT {limit} OFFSET {offset}",
            interval = interval,
            where_clause = where_clause,
            limit = limit,
            offset = offset,
        );

        let mut q = sqlx::query(&query_str).bind(from).bind(to);
        if let Some(ref cid) = params.contract_id {
            q = q.bind(cid);
        }
        if let Some(ref et) = params.event_type {
            q = q.bind(et);
        }

        let rows = q.fetch_all(&state.read_pool).await?;

        let mut buckets = Vec::with_capacity(rows.len());
        for row in &rows {
            let bucket_start: DateTime<Utc> = row.try_get("bucket_start")?;
            let event_count: i64 = row.try_get("event_count")?;
            let contract_count: i64 = row.try_get("contract_count")?;
            buckets.push(models::TemporalBucket {
                bucket_start,
                event_count,
                contract_count,
            });
        }

        let total = buckets.len();
        crate::metrics::record_temporal_query_duration(query_start.elapsed());

        Ok(Json(models::TemporalQueryResponse {
            from,
            to,
            events: vec![],
            buckets,
            total,
        }))
    } else {
        // Raw events path.
        let mut conditions = vec![
            "timestamp >= $1".to_string(),
            "timestamp <= $2".to_string(),
        ];
        let mut bind_idx = 3usize;

        if params.contract_id.is_some() {
            conditions.push(format!("contract_id = ${bind_idx}"));
            bind_idx += 1;
        }
        if params.event_type.is_some() {
            conditions.push(format!("event_type = ${bind_idx}"));
            bind_idx += 1;
        }
        let _ = bind_idx;

        let where_clause = conditions.join(" AND ");
        let query_str = format!(
            "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, \
                    event_data, event_data_normalized, event_data_decoded, \
                    ledger_hash, in_successful_call, created_at, \
                    schema_version, anonymized, fingerprint, \
                    0::bigint AS total_count \
             FROM events \
             WHERE {where_clause} \
             ORDER BY timestamp DESC \
             LIMIT {limit} OFFSET {offset}",
            where_clause = where_clause,
            limit = limit,
            offset = offset,
        );

        let mut q = sqlx::query_as::<_, models::Event>(&query_str)
            .bind(from)
            .bind(to);
        if let Some(ref cid) = params.contract_id {
            q = q.bind(cid);
        }
        if let Some(ref et) = params.event_type {
            q = q.bind(et);
        }

        let events = q.fetch_all(&state.read_pool).await?;
        let total = events.len();

        crate::metrics::record_temporal_query_duration(query_start.elapsed());

        Ok(Json(models::TemporalQueryResponse {
            from,
            to,
            events,
            buckets: vec![],
            total,
        }))
    }
}

/// WebSocket endpoint for event streaming (alternative to SSE)
#[utoipa::path(
    get,
    path = "/v1/events/ws",
    tag = "events",
    params(
        ("contract_id" = Option<String>, Query, description = "Filter by contract ID"),
    ),
    responses(
        (status = 101, description = "WebSocket upgrade"),
        (status = 400, description = "Invalid parameters"),
    )
)]
pub async fn ws_stream_events(
    State(state): State<AppState>,
    Query(params): Query<models::StreamParams>,
    ws: axum::extract::ws::WebSocketUpgrade,
) -> Result<impl IntoResponse, AppError> {
    if let Some(ref cid) = params.contract_id {
        validate_contract_id(cid)?;
    }
    
    let contract_id = params.contract_id.clone();
    let event_tx = state.event_tx.clone();
    let keepalive_ms = state.sse_keepalive_interval_ms;
    let ws_connections = state.sse_connections.clone();
    
    Ok(ws.on_upgrade(move |socket| {
        handle_ws_connection(socket, contract_id, event_tx, keepalive_ms, ws_connections)
    }))
}

/// `POST /v1/events/batch` — bulk event retrieval with multiple filters.
///
/// Accepts an array of filter objects and returns all matching events.
/// Supports JSON and CSV output. Default max batch size is 10,000
/// events total across all filters.
#[utoipa::path(
    post,
    path = "/v1/events/batch",
    tag = "events",
    request_body = BatchQueryRequest,
    responses(
        (status = 200, description = "Batch query results"),
        (status = 400, description = "Invalid request or exceeds max batch size"),
    )
)]
pub async fn batch_query_events(
    State(state): State<AppState>,
    Json(body): Json<BatchQueryRequest>,
) -> Result<impl IntoResponse, AppError> {
    let max_total = state.config.batch_events_max_size as i64;

    if body.filters.is_empty() {
        return Err(AppError::Validation("filters array must not be empty".to_string()));
    }
    if body.filters.len() > 100 {
        return Err(AppError::Validation(
            "Maximum 100 filter objects per batch request".to_string(),
        ));
    }

    let format = body.format.to_lowercase();
    if format != "json" && format != "csv" {
        return Err(AppError::Validation(
            "format must be 'json' or 'csv'".to_string(),
        ));
    }

    let mut all_events: Vec<Value> = Vec::new();
    let job_id = Uuid::new_v4().to_string();
    let all_cols: &[&str] = &["id", "contract_id", "event_type", "tx_hash", "ledger", "timestamp", "event_data", "created_at"];

    for filter in &body.filters {
        let remaining = max_total - all_events.len() as i64;
        if remaining <= 0 {
            break;
        }
        let limit = filter.limit.unwrap_or(1000).min(remaining).min(10_000);

        // Build query with optional string/i64 bind parameters
        let contract_id = filter.contract_id.clone();
        let event_type = filter.event_type.clone();
        let from_ledger = filter.from_ledger;
        let to_ledger = filter.to_ledger;
        let tx_hash = filter.tx_hash.clone();

        let mut sql = String::from(
            "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at \
             FROM events WHERE 1=1",
        );
        let mut bind_idx = 1i32;

        if contract_id.is_some() {
            sql.push_str(&format!(" AND contract_id = ${bind_idx}"));
            bind_idx += 1;
        }
        if event_type.is_some() {
            sql.push_str(&format!(" AND event_type = ${bind_idx}"));
            bind_idx += 1;
        }
        if from_ledger.is_some() {
            sql.push_str(&format!(" AND ledger >= ${bind_idx}"));
            bind_idx += 1;
        }
        if to_ledger.is_some() {
            sql.push_str(&format!(" AND ledger <= ${bind_idx}"));
            bind_idx += 1;
        }
        if tx_hash.is_some() {
            sql.push_str(&format!(" AND tx_hash = ${bind_idx}"));
            bind_idx += 1;
        }
        let _ = bind_idx;

        sql.push_str(" ORDER BY ledger DESC, id DESC");
        sql.push_str(&format!(" LIMIT {limit}"));

        let mut query = sqlx::query(&sql);
        if let Some(ref v) = contract_id { query = query.bind(v); }
        if let Some(ref v) = event_type { query = query.bind(v); }
        if let Some(v) = from_ledger { query = query.bind(v); }
        if let Some(v) = to_ledger { query = query.bind(v); }
        if let Some(ref v) = tx_hash { query = query.bind(v); }

        let rows = query
            .fetch_all(&state.read_pool)
            .await
            .map_err(|e| AppError::Internal(format!("Batch query failed: {e}")))?;

        let events = rows_to_json(
            &rows,
            all_cols,
            state.encryption_key.as_ref(),
            state.encryption_key_old.as_ref(),
            false,
        )?;
        all_events.extend(events);
    }

    crate::metrics::record_batch_query(all_events.len() as u64);

    if format == "csv" {
        let mut csv = String::from("id,contract_id,event_type,tx_hash,ledger,timestamp\n");
        for ev in &all_events {
            let id = ev["id"].as_str().unwrap_or("");
            let cid = ev["contract_id"].as_str().unwrap_or("");
            let et = ev["event_type"].as_str().unwrap_or("");
            let th = ev["tx_hash"].as_str().unwrap_or("");
            let ledger = ev["ledger"].as_i64().unwrap_or(0);
            let ts = ev["timestamp"].as_str().unwrap_or("");
            csv.push_str(&format!("{id},{cid},{et},{th},{ledger},{ts}\n"));
        }
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/csv")
            .header(header::CONTENT_DISPOSITION, format!("attachment; filename=\"batch_{job_id}.csv\""))
            .body(Body::from(csv))
            .unwrap()
            .into_response());
    }

    Ok(Json(json!({
        "job_id": job_id,
        "total": all_events.len(),
        "data": all_events,
    })).into_response())
}

/// `GET /v1/events/aggregations` — faceted event statistics.
///
/// Returns event_type distribution, top contract_ids by event count,
/// time-series bucketed counts, and topic distribution. Results are cached
/// per unique parameter set for `AGGREGATION_CACHE_TTL_SECS` seconds.
#[utoipa::path(
    get,
    path = "/v1/events/aggregations",
    tag = "aggregations",
    params(
        ("contract_id" = Option<String>, Query, description = "Filter by contract ID"),
        ("event_type" = Option<String>, Query, description = "Filter by event type"),
        ("from_ledger" = Option<i64>, Query, description = "Minimum ledger"),
        ("to_ledger" = Option<i64>, Query, description = "Maximum ledger"),
        ("time_bucket" = Option<String>, Query, description = "Time bucket: hourly, daily (default), weekly"),
        ("limit" = Option<i64>, Query, description = "Max buckets per dimension (default 100, max 500)"),
    ),
    responses(
        (status = 200, description = "Aggregated event statistics"),
        (status = 400, description = "Invalid parameters"),
    )
)]
pub async fn events_aggregations(
    State(state): State<AppState>,
    Query(params): Query<EventsAggregationParams>,
) -> Result<Json<Value>, AppError> {
    let time_bucket = params.time_bucket.to_lowercase();
    if !["hourly", "daily", "weekly"].contains(&time_bucket.as_str()) {
        return Err(AppError::Validation(
            "time_bucket must be 'hourly', 'daily', or 'weekly'".to_string(),
        ));
    }

    let limit = params.limit.unwrap_or(100).max(1).min(500);

    // Build a stable cache key from the request params
    let cache_key = format!(
        "agg:{}:{}:{}:{}:{}:{}",
        params.contract_id.as_deref().unwrap_or(""),
        params.event_type.as_deref().unwrap_or(""),
        params.from_ledger.unwrap_or(0),
        params.to_ledger.unwrap_or(0),
        time_bucket,
        limit,
    );

    if let Some(cached) = state.aggregation_cache.get(&cache_key).await {
        return Ok(Json(cached));
    }

    // Build WHERE clause parts
    let mut where_parts: Vec<String> = vec!["1=1".to_string()];
    let mut bind_idx = 1i32;
    let mut contract_id_bind: Option<String> = None;
    let mut event_type_bind: Option<String> = None;
    let mut from_ledger_bind: Option<i64> = None;
    let mut to_ledger_bind: Option<i64> = None;

    if let Some(ref cid) = params.contract_id {
        where_parts.push(format!("contract_id = ${bind_idx}"));
        contract_id_bind = Some(cid.clone());
        bind_idx += 1;
    }
    if let Some(ref et) = params.event_type {
        where_parts.push(format!("event_type = ${bind_idx}"));
        event_type_bind = Some(et.clone());
        bind_idx += 1;
    }
    if let Some(from) = params.from_ledger {
        where_parts.push(format!("ledger >= ${bind_idx}"));
        from_ledger_bind = Some(from);
        bind_idx += 1;
    }
    if let Some(to) = params.to_ledger {
        where_parts.push(format!("ledger <= ${bind_idx}"));
        to_ledger_bind = Some(to);
        bind_idx += 1;
    }
    let _ = bind_idx;
    let where_clause = where_parts.join(" AND ");

    macro_rules! bind_filters {
        ($q:expr) => {{
            let mut q = $q;
            if let Some(ref v) = contract_id_bind { q = q.bind(v); }
            if let Some(ref v) = event_type_bind { q = q.bind(v); }
            if let Some(v) = from_ledger_bind { q = q.bind(v); }
            if let Some(v) = to_ledger_bind { q = q.bind(v); }
            q
        }};
    }

    // 1. event_type aggregation
    let event_type_sql = format!(
        "SELECT event_type, COUNT(*) AS count FROM events WHERE {where_clause} \
         GROUP BY event_type ORDER BY count DESC LIMIT {limit}"
    );
    let event_type_rows = bind_filters!(sqlx::query(&event_type_sql))
        .fetch_all(&state.read_pool)
        .await
        .map_err(|e| AppError::Internal(format!("event_type aggregation failed: {e}")))?;

    let event_type_agg: Vec<Value> = event_type_rows
        .iter()
        .map(|r| {
            let et: String = r.try_get("event_type").unwrap_or_default();
            let count: i64 = r.try_get("count").unwrap_or(0);
            json!({"event_type": et, "count": count})
        })
        .collect();

    // 2. contract_id aggregation
    let contract_id_sql = format!(
        "SELECT contract_id, COUNT(*) AS count FROM events WHERE {where_clause} \
         GROUP BY contract_id ORDER BY count DESC LIMIT {limit}"
    );
    let contract_id_rows = bind_filters!(sqlx::query(&contract_id_sql))
        .fetch_all(&state.read_pool)
        .await
        .map_err(|e| AppError::Internal(format!("contract_id aggregation failed: {e}")))?;

    let contract_id_agg: Vec<Value> = contract_id_rows
        .iter()
        .map(|r| {
            let cid: String = r.try_get("contract_id").unwrap_or_default();
            let count: i64 = r.try_get("count").unwrap_or(0);
            json!({"contract_id": cid, "count": count})
        })
        .collect();

    // 3. Time-based aggregation
    let trunc_fn = match time_bucket.as_str() {
        "hourly" => "hour",
        "weekly" => "week",
        _ => "day",
    };
    let time_sql = format!(
        "SELECT date_trunc('{trunc_fn}', timestamp) AS bucket, COUNT(*) AS count \
         FROM events WHERE {where_clause} \
         GROUP BY bucket ORDER BY bucket DESC LIMIT {limit}"
    );
    let time_rows = bind_filters!(sqlx::query(&time_sql))
        .fetch_all(&state.read_pool)
        .await
        .map_err(|e| AppError::Internal(format!("time aggregation failed: {e}")))?;

    let time_agg: Vec<Value> = time_rows
        .iter()
        .map(|r| {
            let bucket: Option<chrono::DateTime<Utc>> = r.try_get("bucket").ok();
            let count: i64 = r.try_get("count").unwrap_or(0);
            let bucket_str = bucket.map(|b| b.to_rfc3339()).unwrap_or_default();
            json!({"bucket": bucket_str, "count": count})
        })
        .collect();

    // 4. Topic aggregation (top topics from event_data->>'topic')
    let topic_sql = format!(
        "SELECT event_data->>'topic' AS topic, COUNT(*) AS count \
         FROM events WHERE {where_clause} AND event_data->>'topic' IS NOT NULL \
         GROUP BY topic ORDER BY count DESC LIMIT {limit}"
    );
    let topic_rows = bind_filters!(sqlx::query(&topic_sql))
        .fetch_all(&state.read_pool)
        .await
        .map_err(|e| AppError::Internal(format!("topic aggregation failed: {e}")))?;

    let topic_agg: Vec<Value> = topic_rows
        .iter()
        .map(|r| {
            let topic: String = r.try_get("topic").unwrap_or_default();
            let count: i64 = r.try_get("count").unwrap_or(0);
            json!({"topic": topic, "count": count})
        })
        .collect();

    let result = json!({
        "time_bucket": time_bucket,
        "filters": {
            "contract_id": params.contract_id,
            "event_type": params.event_type,
            "from_ledger": params.from_ledger,
            "to_ledger": params.to_ledger,
        },
        "aggregations": {
            "by_event_type": event_type_agg,
            "by_contract_id": contract_id_agg,
            "by_time": time_agg,
            "by_topic": topic_agg,
        },
    });

    state.aggregation_cache.insert(cache_key, result.clone()).await;
    crate::metrics::record_aggregation_query();

    Ok(Json(result))
}

/// Scan a single event's data for PII (dry-run, does not modify the event)
#[utoipa::path(
    post,
    path = "/v1/config/anonymization/scan",
    tag = "config",
    request_body = serde_json::Value,
    responses(
        (status = 200, description = "PII scan result"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    security(("api_key" = []))
)]
pub async fn scan_event_for_pii(
    State(state): State<AppState>,
    Json(event_data): Json<serde_json::Value>,
) -> Result<impl IntoResponse, AppError> {
    let detection_rules = match state.anonymization_config.as_ref() {
        Some(cfg) => cfg.compile_detection_rules().await,
        None => {
            crate::anonymization::default_rules()
                .iter()
                .filter(|r| r.enabled)
                .filter_map(|r| Regex::new(&r.pattern).ok().map(|re| (r.name.clone(), re)))
                .collect()
        }
    };

    let result = crate::anonymization::scan_for_pii(&event_data, &detection_rules);
    let detections: Vec<serde_json::Value> = result
        .detections
        .iter()
        .map(|d| json!({ "field": d.field_path, "rule": d.rule_name }))
        .collect();

    Ok(Json(json!({
        "pii_detected": !detections.is_empty(),
        "detections": detections,
    })))
}

/// Return trending events and contracts using a z-score algorithm.
///
/// `GET /v1/analytics/trending`
///
/// Compares event counts in the current window against a rolling baseline to
/// produce a z-score for each (contract_id, event_type) pair. Results are
/// sorted by z-score descending so the most anomalously active items appear
/// first.
///
/// ### Query parameters
/// | Name           | Default | Description |
/// |----------------|---------|-------------|
/// | `window_mins`  | 60      | Size of the current window in minutes |
/// | `baseline_hrs` | 24      | Hours of history used to build the baseline mean/stdev |
/// | `contract_id`  | –       | Restrict to a single contract |
/// | `event_type`   | –       | Restrict to a single event type |
/// | `min_zscore`   | 1.0     | Only return items with z-score ≥ this value |
/// | `limit`        | 20      | Maximum rows to return |
#[utoipa::path(
    get,
    path = "/v1/analytics/trending",
    tag = "analytics",
    params(
        ("window_mins"  = Option<i64>,   Query, description = "Current window size in minutes (default 60)"),
        ("baseline_hrs" = Option<i64>,   Query, description = "Baseline history in hours (default 24)"),
        ("contract_id"  = Option<String>,Query, description = "Filter to a specific contract"),
        ("event_type"   = Option<String>,Query, description = "Filter to a specific event type"),
        ("min_zscore"   = Option<f64>,   Query, description = "Minimum z-score threshold (default 1.0)"),
        ("limit"        = Option<i64>,   Query, description = "Max items to return (default 20, max 100)"),
    ),
    responses(
        (status = 200, description = "Trending events"),
        (status = 400, description = "Invalid parameters"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn get_trending_events(
    State(state): State<AppState>,
    Query(params): Query<TrendingParams>,
) -> Result<Json<Value>, AppError> {
    let window_mins  = params.window_mins.unwrap_or(60).clamp(1, 1440);
    let baseline_hrs = params.baseline_hrs.unwrap_or(24).clamp(1, 720);
    let min_zscore   = params.min_zscore.unwrap_or(1.0_f64);
    let limit        = params.limit.unwrap_or(20).clamp(1, 100);

    // ── Step 1: counts in the current window ─────────────────────────────────
    let mut cur_conds = vec!["timestamp >= NOW() - ($1 || ' minutes')::interval".to_string()];
    let mut idx: i32 = 2;
    if params.contract_id.is_some() { cur_conds.push(format!("contract_id = ${idx}")); idx += 1; }
    if params.event_type.is_some()  { cur_conds.push(format!("event_type = ${idx}"));  idx += 1; }

    let cur_sql = format!(
        "SELECT contract_id, event_type, COUNT(*) AS cnt \
         FROM events WHERE {} \
         GROUP BY contract_id, event_type",
        cur_conds.join(" AND ")
    );

    let mut cur_q = sqlx::query(&cur_sql).bind(window_mins.to_string());
    if let Some(ref v) = params.contract_id { cur_q = cur_q.bind(v); }
    if let Some(ref v) = params.event_type  { cur_q = cur_q.bind(v); }
    let cur_rows = cur_q.fetch_all(&state.pool).await?;

    // ── Step 2: per-window counts over the baseline period ───────────────────
    // We slice the baseline into same-sized windows and count events per slice.
    // Then we compute mean and stdev across those window counts.
    let mut base_conds = vec![
        format!("timestamp >= NOW() - ($1 || ' hours')::interval"),
        format!("timestamp < NOW() - ($2 || ' minutes')::interval"),
    ];
    let mut bidx: i32 = 3;
    if params.contract_id.is_some() { base_conds.push(format!("contract_id = ${bidx}")); bidx += 1; }
    if params.event_type.is_some()  { base_conds.push(format!("event_type = ${bidx}"));  bidx += 1; }

    let base_sql = format!(
        "SELECT contract_id, event_type, \
                COUNT(*) AS total_cnt, \
                GREATEST(1, FLOOR(EXTRACT(EPOCH FROM (NOW() - ($2 || ' minutes')::interval - (NOW() - ($1 || ' hours')::interval))) / ({window_mins} * 60.0))) AS num_windows
         FROM events WHERE {} \
         GROUP BY contract_id, event_type",
        base_conds.join(" AND ")
    );

    let mut base_q = sqlx::query(&base_sql)
        .bind(baseline_hrs.to_string())
        .bind(window_mins.to_string());
    if let Some(ref v) = params.contract_id { base_q = base_q.bind(v); }
    if let Some(ref v) = params.event_type  { base_q = base_q.bind(v); }
    let base_rows = base_q.fetch_all(&state.pool).await?;

    // Build baseline lookup: (contract_id, event_type) -> (mean_per_window, stdev estimate)
    use std::collections::HashMap;
    let mut baseline: HashMap<(String, String), (f64, f64)> = HashMap::new();
    for row in &base_rows {
        let cid: String  = row.try_get("contract_id").unwrap_or_default();
        let et:  String  = row.try_get("event_type").unwrap_or_default();
        let total: i64   = row.try_get("total_cnt").unwrap_or(0);
        let n: f64       = row.try_get::<f64, _>("num_windows").unwrap_or(1.0).max(1.0);
        let mean = total as f64 / n;
        // Poisson-like stdev: sqrt(mean), floor to 1 so we never divide by 0
        let stdev = mean.sqrt().max(1.0);
        baseline.insert((cid, et), (mean, stdev));
    }

    // ── Step 3: compute z-scores ──────────────────────────────────────────────
    let mut results: Vec<Value> = Vec::new();
    for row in &cur_rows {
        let cid: String = row.try_get("contract_id").unwrap_or_default();
        let et:  String = row.try_get("event_type").unwrap_or_default();
        let cnt: i64    = row.try_get("cnt").unwrap_or(0);

        let (mean, stdev) = baseline
            .get(&(cid.clone(), et.clone()))
            .copied()
            .unwrap_or((0.0, 1.0));

        let zscore = (cnt as f64 - mean) / stdev;

        if zscore >= min_zscore {
            results.push(json!({
                "contract_id":    cid,
                "event_type":     et,
                "count":          cnt,
                "baseline_mean":  (mean  * 100.0).round() / 100.0,
                "baseline_stdev": (stdev * 100.0).round() / 100.0,
                "zscore":         (zscore * 100.0).round() / 100.0,
            }));
        }
    }

    // Sort by zscore desc, apply limit
    results.sort_by(|a, b| {
        b["zscore"]
            .as_f64()
            .unwrap_or(0.0)
            .partial_cmp(&a["zscore"].as_f64().unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    results.truncate(limit as usize);

    Ok(Json(json!({
        "trending": results,
        "count":          results.len(),
        "window_mins":    window_mins,
        "baseline_hrs":   baseline_hrs,
        "min_zscore":     min_zscore,
        "algorithm":      "zscore",
    })))
}

/// Detect correlated event activity across contracts.
///
/// `GET /v1/analytics/correlations`
///
/// Buckets events into fixed-width time slots and computes a Pearson
/// correlation coefficient for every pair of contracts that both have activity
/// within the requested window.  Only pairs whose |r| ≥ `min_correlation` are
/// returned, sorted by descending |r|.
///
/// ### Query parameters
/// | Name              | Default | Description |
/// |-------------------|---------|-------------|
/// | `window_hrs`      | 24      | How far back to look (hours) |
/// | `bucket_mins`     | 60      | Slot width in minutes |
/// | `min_correlation` | 0.5     | Minimum |r| to include in results |
/// | `contract_ids`    | –       | Comma-separated list to restrict analysis |
/// | `event_type`      | –       | Restrict to a specific event type |
/// | `limit`           | 50      | Max pairs to return |
#[utoipa::path(
    get,
    path = "/v1/analytics/correlations",
    tag = "analytics",
    params(
        ("window_hrs"      = Option<i64>,   Query, description = "Lookback window in hours (default 24)"),
        ("bucket_mins"     = Option<i64>,   Query, description = "Time-bucket width in minutes (default 60)"),
        ("min_correlation" = Option<f64>,   Query, description = "Minimum absolute Pearson r (default 0.5)"),
        ("contract_ids"    = Option<String>,Query, description = "Comma-separated contract IDs to analyse"),
        ("event_type"      = Option<String>,Query, description = "Restrict to a specific event type"),
        ("limit"           = Option<i64>,   Query, description = "Max pairs to return (default 50, max 200)"),
    ),
    responses(
        (status = 200, description = "Correlation pairs"),
        (status = 400, description = "Invalid parameters"),
        (status = 500, description = "Internal server error"),
    )
)]
pub async fn get_event_correlations(
    State(state): State<AppState>,
    Query(params): Query<CorrelationParams>,
) -> Result<Json<Value>, AppError> {
    let window_hrs      = params.window_hrs.unwrap_or(24).clamp(1, 720);
    let bucket_mins     = params.bucket_mins.unwrap_or(60).clamp(1, 1440);
    let min_correlation = params.min_correlation.unwrap_or(0.5_f64).clamp(0.0, 1.0);
    let limit           = params.limit.unwrap_or(50).clamp(1, 200);

    // Optional contract list filter
    let contract_filter: Option<Vec<String>> = params
        .contract_ids
        .as_deref()
        .map(|s| s.split(',').map(|id| id.trim().to_string()).collect());

    // Build per-bucket counts: (contract_id, bucket_start) -> count
    let mut conds = vec![
        "timestamp >= NOW() - ($1 || ' hours')::interval".to_string(),
    ];
    let mut idx: i32 = 2;
    if params.event_type.is_some() {
        conds.push(format!("event_type = ${idx}"));
        idx += 1;
    }
    // contract_ids filter handled post-query to avoid dynamic IN clause
    let _ = idx; // suppress unused warning

    let bucket_sql = format!(
        "SELECT contract_id, \
                date_trunc('hour', timestamp) + \
                  (EXTRACT(MINUTE FROM timestamp)::int / {bucket_mins} * {bucket_mins} || ' minutes')::interval AS bucket, \
                COUNT(*) AS cnt \
         FROM events WHERE {} \
         GROUP BY contract_id, bucket \
         ORDER BY bucket",
        conds.join(" AND ")
    );

    let mut q = sqlx::query(&bucket_sql).bind(window_hrs.to_string());
    if let Some(ref v) = params.event_type { q = q.bind(v); }
    let rows = q.fetch_all(&state.pool).await?;

    // Build time-series map: contract_id -> Vec<(bucket, count)>
    use std::collections::HashMap;
    let mut series: HashMap<String, Vec<(String, f64)>> = HashMap::new();
    for row in &rows {
        let cid: String = row.try_get("contract_id").unwrap_or_default();
        let bucket: DateTime<Utc> = row.try_get("bucket").unwrap_or_else(|_| Utc::now());
        let cnt: i64 = row.try_get("cnt").unwrap_or(0);

        if let Some(ref filter) = contract_filter {
            if !filter.contains(&cid) {
                continue;
            }
        }
        series
            .entry(cid)
            .or_default()
            .push((bucket.to_rfc3339(), cnt as f64));
    }

    // Collect all unique buckets to align vectors
    let mut all_buckets: Vec<String> = series
        .values()
        .flat_map(|v| v.iter().map(|(b, _)| b.clone()))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    all_buckets.sort();

    // Build aligned count vectors per contract
    let contract_ids: Vec<String> = series.keys().cloned().collect();
    let mut vectors: HashMap<String, Vec<f64>> = HashMap::new();
    for cid in &contract_ids {
        let bucket_map: HashMap<String, f64> = series[cid]
            .iter()
            .cloned()
            .collect();
        let vec: Vec<f64> = all_buckets
            .iter()
            .map(|b| *bucket_map.get(b).unwrap_or(&0.0))
            .collect();
        vectors.insert(cid.clone(), vec);
    }

    // Pearson correlation helper
    fn pearson(a: &[f64], b: &[f64]) -> f64 {
        let n = a.len() as f64;
        if n == 0.0 { return 0.0; }
        let mean_a = a.iter().sum::<f64>() / n;
        let mean_b = b.iter().sum::<f64>() / n;
        let cov: f64 = a.iter().zip(b).map(|(x, y)| (x - mean_a) * (y - mean_b)).sum();
        let std_a = (a.iter().map(|x| (x - mean_a).powi(2)).sum::<f64>() / n).sqrt();
        let std_b = (b.iter().map(|x| (x - mean_b).powi(2)).sum::<f64>() / n).sqrt();
        if std_a == 0.0 || std_b == 0.0 { return 0.0; }
        cov / (std_a * std_b)
    }

    // Compute all pairs
    let mut pairs: Vec<Value> = Vec::new();
    for i in 0..contract_ids.len() {
        for j in (i + 1)..contract_ids.len() {
            let cid_a = &contract_ids[i];
            let cid_b = &contract_ids[j];
            let va = vectors.get(cid_a).map(Vec::as_slice).unwrap_or(&[]);
            let vb = vectors.get(cid_b).map(Vec::as_slice).unwrap_or(&[]);
            let r = pearson(va, vb);

            if r.abs() >= min_correlation {
                pairs.push(json!({
                    "contract_a":  cid_a,
                    "contract_b":  cid_b,
                    "correlation": (r * 10000.0).round() / 10000.0,
                    "direction":   if r >= 0.0 { "positive" } else { "negative" },
                    "strength":    if r.abs() >= 0.9 { "very_strong" }
                                   else if r.abs() >= 0.7 { "strong" }
                                   else if r.abs() >= 0.5 { "moderate" }
                                   else { "weak" },
                }));
            }
        }
    }

    pairs.sort_by(|a, b| {
        b["correlation"]
            .as_f64()
            .unwrap_or(0.0)
            .abs()
            .partial_cmp(&a["correlation"].as_f64().unwrap_or(0.0).abs())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    pairs.truncate(limit as usize);

    Ok(Json(json!({
        "correlations":    pairs,
        "count":           pairs.len(),
        "contracts_analysed": contract_ids.len(),
        "buckets":         all_buckets.len(),
        "window_hrs":      window_hrs,
        "bucket_mins":     bucket_mins,
        "min_correlation": min_correlation,
        "algorithm":       "pearson",
    })))
}

/// Export events as CSV.
///
/// `GET /v1/export/csv`
///
/// Thin wrapper around the existing export_events logic with `format=csv`
/// forced. Accepts the same filter query parameters.
#[utoipa::path(
    get,
    path = "/v1/export/csv",
    tag = "export",
    params(
        ("contract_id"   = Option<String>,   Query, description = "Filter by contract ID"),
        ("event_type"    = Option<String>,   Query, description = "Filter by event type"),
        ("from_ledger"   = Option<i64>,      Query, description = "Start ledger (inclusive)"),
        ("to_ledger"     = Option<i64>,      Query, description = "End ledger (inclusive)"),
        ("from_timestamp"= Option<DateTime<Utc>>, Query, description = "Start timestamp (inclusive)"),
        ("to_timestamp"  = Option<DateTime<Utc>>, Query, description = "End timestamp (exclusive)"),
    ),
    responses(
        (status = 200, description = "CSV file", content_type = "text/csv"),
        (status = 400, description = "Invalid parameters"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error"),
    ),
    security(("api_key" = []))
)]
pub async fn export_events_csv(
    state: State<AppState>,
    Query(mut params): Query<ExportParams>,
    headers: HeaderMap,
) -> Result<Response<Body>, AppError> {
    params.format = Some("csv".to_string());
    export_events(state, Query(params), headers).await
}

/// Export events as JSON (newline-delimited).
///
/// `GET /v1/export/json`
///
/// Returns one JSON object per line (NDJSON/JSONL).
#[utoipa::path(
    get,
    path = "/v1/export/json",
    tag = "export",
    params(
        ("contract_id"   = Option<String>,   Query, description = "Filter by contract ID"),
        ("event_type"    = Option<String>,   Query, description = "Filter by event type"),
        ("from_ledger"   = Option<i64>,      Query, description = "Start ledger (inclusive)"),
        ("to_ledger"     = Option<i64>,      Query, description = "End ledger (inclusive)"),
        ("from_timestamp"= Option<DateTime<Utc>>, Query, description = "Start timestamp (inclusive)"),
        ("to_timestamp"  = Option<DateTime<Utc>>, Query, description = "End timestamp (exclusive)"),
    ),
    responses(
        (status = 200, description = "NDJSON file", content_type = "application/x-ndjson"),
        (status = 400, description = "Invalid parameters"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error"),
    ),
    security(("api_key" = []))
)]
pub async fn export_events_json(
    state: State<AppState>,
    Query(mut params): Query<ExportParams>,
    headers: HeaderMap,
) -> Result<Response<Body>, AppError> {
    params.format = Some("jsonl".to_string());
    export_events(state, Query(params), headers).await
}

/// Export events as Apache Parquet.
///
/// `GET /v1/export/parquet`
///
/// Returns a binary Parquet file.
#[utoipa::path(
    get,
    path = "/v1/export/parquet",
    tag = "export",
    params(
        ("contract_id"   = Option<String>,   Query, description = "Filter by contract ID"),
        ("event_type"    = Option<String>,   Query, description = "Filter by event type"),
        ("from_ledger"   = Option<i64>,      Query, description = "Start ledger (inclusive)"),
        ("to_ledger"     = Option<i64>,      Query, description = "End ledger (inclusive)"),
        ("from_timestamp"= Option<DateTime<Utc>>, Query, description = "Start timestamp (inclusive)"),
        ("to_timestamp"  = Option<DateTime<Utc>>, Query, description = "End timestamp (exclusive)"),
    ),
    responses(
        (status = 200, description = "Parquet file", content_type = "application/octet-stream"),
        (status = 400, description = "Invalid parameters"),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal server error"),
    ),
    security(("api_key" = []))
)]
pub async fn export_events_parquet(
    state: State<AppState>,
    Query(mut params): Query<ExportParams>,
    headers: HeaderMap,
) -> Result<Response<Body>, AppError> {
    params.format = Some("parquet".to_string());
    export_events(state, Query(params), headers).await
}

/// POST /v1/admin/events/export
/// Start a new bulk event export with specified format and compression
pub async fn start_event_export(
    State(state): State<AppState>,
    Json(request): Json<crate::bulk_export::ExportRequest>,
) -> Result<Json<serde_json::Value>, AppError> {
    let job_id = state.bulk_export_manager.start_export(&state.pool, request).await
        .map_err(|e| AppError::Internal(e))?;

    Ok(Json(json!({
        "job_id": job_id,
        "status": "pending",
    })))
}
