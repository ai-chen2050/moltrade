use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::Json,
    routing::{delete, get, post},
};
use prometheus::{Encoder, TextEncoder};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;

use crate::api::metrics::Metrics;
use crate::core::dedupe_engine::DeduplicationEngine;
use crate::core::relay_pool::RelayPool;
use crate::core::subscription::SubscriptionService;

#[derive(Clone)]
pub struct AppState {
    pub pool: Arc<RelayPool>,
    pub dedupe: Arc<DeduplicationEngine>,
    pub metrics: Arc<Metrics>,
    pub subscriptions: Option<Arc<SubscriptionService>>,
    pub platform_pubkey: Option<String>,
}

/// Create the REST API router
pub fn create_router(
    pool: Arc<RelayPool>,
    dedupe: Arc<DeduplicationEngine>,
    metrics: Arc<Metrics>,
    subscriptions: Option<Arc<SubscriptionService>>,
    platform_pubkey: Option<String>,
) -> Router {
    let state = AppState {
        pool,
        dedupe,
        metrics,
        subscriptions,
        platform_pubkey,
    };
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(prometheus_metrics))
        .route("/status", get(status))
        .route("/api/metrics/summary", get(metrics_summary))
        .route("/api/metrics/memory", get(memory))
        .route("/api/relays", get(list_relays))
        .route("/api/relays/add", post(add_relay))
        .route("/api/relays/remove", delete(remove_relay))
        .route("/api/bots/register", post(register_bot))
        .route("/api/subscriptions", post(add_subscription))
        .route("/api/subscriptions/:bot_pubkey", get(list_subscriptions))
        .with_state(state)
}

/// Health check endpoint
async fn health() -> Json<serde_json::Value> {
    Json(json!({
        "status": "healthy",
        "service": "moltrade-relayer"
    }))
}

/// Metrics endpoint for Prometheus
async fn prometheus_metrics() -> Result<String, StatusCode> {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();

    encoder
        .encode(&metric_families, &mut buffer)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(String::from_utf8_lossy(&buffer).to_string())
}

/// Get connection status
async fn status(State(state): State<AppState>) -> Json<serde_json::Value> {
    let statuses = state.pool.get_connection_statuses().await;
    let active = state.pool.active_connections();
    let deque_status = state.dedupe.get_stats().await;

    Json(json!({
        "active_connections": active,
        "connections": statuses.iter().map(|(url, status)| {
            json!({
                "url": url,
                "status": format!("{:?}", status)
            })
        }).collect::<Vec<_>>(),
        "deduplication_engine": {
            "bloom_filter_size": deque_status.bloom_filter_size,
            "lru_cache_size": deque_status.lru_cache_size,
            "rocksdb_entry_count": deque_status.rocksdb_approximate_count,
            "hot_set_size": deque_status.hot_set_size,
        }
    }))
}

/// Request body for adding a relay
#[derive(Debug, Deserialize)]
struct AddRelayRequest {
    url: String,
}

/// Request body for removing a relay
#[derive(Debug, Deserialize)]
struct RemoveRelayRequest {
    url: String,
}

/// Response for relay operations
#[derive(Debug, Serialize)]
struct RelayResponse {
    success: bool,
    message: String,
}

/// Add a new relay
async fn add_relay(
    State(state): State<AppState>,
    Json(payload): Json<AddRelayRequest>,
) -> Result<Json<RelayResponse>, StatusCode> {
    match state.pool.connect_and_subscribe(payload.url.clone()).await {
        Ok(_) => Ok(Json(RelayResponse {
            success: true,
            message: format!("Successfully connected to relay: {}", payload.url),
        })),
        Err(e) => {
            tracing::error!("Failed to add relay {}: {}", payload.url, e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Remove a relay
async fn remove_relay(
    State(state): State<AppState>,
    Json(payload): Json<RemoveRelayRequest>,
) -> Result<Json<RelayResponse>, StatusCode> {
    match state.pool.disconnect_relay(&payload.url).await {
        Ok(_) => Ok(Json(RelayResponse {
            success: true,
            message: format!("Successfully disconnected relay: {}", payload.url),
        })),
        Err(e) => {
            tracing::error!("Failed to remove relay {}: {}", payload.url, e);
            Err(StatusCode::NOT_FOUND)
        }
    }
}

/// List all relays
async fn list_relays(State(state): State<AppState>) -> Json<serde_json::Value> {
    let _relay_urls = state.pool.list_relays();
    let statuses = state.pool.get_connection_statuses().await;

    let mut relay_info = Vec::new();
    for (url, status) in statuses {
        relay_info.push(json!({
            "url": url,
            "status": format!("{:?}", status)
        }));
    }

    Json(json!({
        "relays": relay_info,
        "count": relay_info.len()
    }))
}

#[derive(Debug, Deserialize)]
struct RegisterBotRequest {
    bot_pubkey: String,
    nostr_pubkey: String,
    eth_address: String,
    name: String,
}

#[derive(Debug, Serialize)]
struct RegisterBotResponse {
    success: bool,
    message: String,
    platform_pubkey: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AddSubscriptionRequest {
    bot_pubkey: String,
    follower_pubkey: String,
    shared_secret: String,
}

#[derive(Debug, Serialize)]
struct SubscriptionsResponse {
    subscriptions: Vec<SubscriptionItem>,
}

#[derive(Debug, Serialize)]
struct SubscriptionItem {
    follower_pubkey: String,
}

/// Register or upsert a bot
async fn register_bot(
    State(state): State<AppState>,
    Json(payload): Json<RegisterBotRequest>,
) -> Result<Json<RegisterBotResponse>, StatusCode> {
    let svc = match &state.subscriptions {
        Some(s) => s,
        None => return Err(StatusCode::SERVICE_UNAVAILABLE),
    };

    svc.register_bot(
        &payload.bot_pubkey,
        &payload.nostr_pubkey,
        &payload.eth_address,
        &payload.name,
    )
        .await
        .map_err(|e| {
            tracing::error!("Failed to register bot: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(RegisterBotResponse {
        success: true,
        message: "bot registered".to_string(),
        platform_pubkey: state.platform_pubkey.clone(),
    }))
}

/// Add or update a subscription
async fn add_subscription(
    State(state): State<AppState>,
    Json(payload): Json<AddSubscriptionRequest>,
) -> Result<Json<RelayResponse>, StatusCode> {
    let svc = match &state.subscriptions {
        Some(s) => s,
        None => return Err(StatusCode::SERVICE_UNAVAILABLE),
    };

    svc.add_subscription(
        &payload.bot_pubkey,
        &payload.follower_pubkey,
        &payload.shared_secret,
    )
    .await
    .map_err(|e| {
        tracing::error!("Failed to add subscription: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(RelayResponse {
        success: true,
        message: "subscription saved".to_string(),
    }))
}

/// List subscriptions for a bot
async fn list_subscriptions(
    State(state): State<AppState>,
    axum::extract::Path(bot_pubkey): axum::extract::Path<String>,
) -> Result<Json<SubscriptionsResponse>, StatusCode> {
    let svc = match &state.subscriptions {
        Some(s) => s,
        None => return Err(StatusCode::SERVICE_UNAVAILABLE),
    };

    let subs = svc.list_subscriptions(&bot_pubkey).await.map_err(|e| {
        tracing::error!("Failed to list subscriptions: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(SubscriptionsResponse {
        subscriptions: subs
            .into_iter()
            .map(|s| SubscriptionItem {
                follower_pubkey: s.follower_pubkey,
            })
            .collect(),
    }))
}

/// Summary metrics endpoint (JSON)
async fn metrics_summary(State(state): State<AppState>) -> Json<serde_json::Value> {
    let m = &state.metrics;
    // Convert the kb to MB（1 MB = 1024 * 1024 bytes）
    let memory_usage_mb = m.memory_usage.get() as f64 / 1024.0;
    Json(serde_json::json!({
        "events_processed_total": m.events_processed.get(),
        "duplicates_filtered_total": m.duplicates_filtered.get(),
        "events_in_queue": m.events_in_queue.get(),
        "active_connections": m.active_connections.get(),
        "memory_usage_mb": memory_usage_mb,
    }))
}

/// Memory-only endpoint
async fn memory(State(state): State<AppState>) -> Json<serde_json::Value> {
    // Convert the byte to MB
    let memory_usage_mb = state.metrics.memory_usage.get() as f64 / 1024.0;
    Json(serde_json::json!({
        "memory_usage_mb": memory_usage_mb,
    }))
}
