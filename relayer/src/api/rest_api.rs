use axum::{
    Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::Json,
    routing::{delete, get, post},
};
use chrono::{Datelike, Utc};
use prometheus::{Encoder, TextEncoder};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::error::SqlState;

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
    pub settlement_token: Option<String>,
    pub subscription_daily_limit: u64,
    pub subscription_limiters: Arc<Mutex<HashMap<String, DailyLimit>>>,
}

#[derive(Debug)]
pub struct DailyLimit {
    limit: u64,
    day: i32,
    count: u64,
}

impl DailyLimit {
    fn new(limit: u64) -> Self {
        Self {
            limit,
            day: current_day_id(),
            count: 0,
        }
    }

    fn reset_if_needed(&mut self) {
        let today = current_day_id();
        if self.day != today {
            self.day = today;
            self.count = 0;
        }
    }
}

fn current_day_id() -> i32 {
    let d = Utc::now().date_naive();
    d.year() * 10_000 + d.month() as i32 * 100 + d.day() as i32
}

/// Create the REST API router
pub fn create_router(
    pool: Arc<RelayPool>,
    dedupe: Arc<DeduplicationEngine>,
    metrics: Arc<Metrics>,
    subscriptions: Option<Arc<SubscriptionService>>,
    platform_pubkey: Option<String>,
    settlement_token: Option<String>,
    subscription_daily_limit: u64,
) -> Router {
    let state = AppState {
        pool,
        dedupe,
        metrics,
        subscriptions,
        platform_pubkey,
        settlement_token,
        subscription_daily_limit,
        subscription_limiters: Arc::new(Mutex::new(HashMap::new())),
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
        .route("/api/subscriptions/{bot_pubkey}", get(list_subscriptions))
        .route(
            "/api/subscriptions/by-eth/{eth_address}",
            get(list_subscriptions_by_eth),
        )
        .route("/api/trades/record", post(record_trade))
        .route("/api/trades/settlement", post(update_trade_settlement))
        .route("/api/credits", get(list_credits))
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
        "relayer_nostr_pubkey": state.platform_pubkey,
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
    headers: HeaderMap,
    Json(payload): Json<AddRelayRequest>,
) -> Result<Json<RelayResponse>, StatusCode> {
    if !is_token_valid(&headers, state.settlement_token.as_deref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }

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
    headers: HeaderMap,
    Json(payload): Json<RemoveRelayRequest>,
) -> Result<Json<RelayResponse>, StatusCode> {
    if !is_token_valid(&headers, state.settlement_token.as_deref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }

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
struct RecordTradeRequest {
    bot_pubkey: String,
    follower_pubkey: Option<String>,
    role: String,
    symbol: String,
    side: String,
    size: f64,
    price: f64,
    tx_hash: Option<String>,
    oid: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UpdateSettlementRequest {
    tx_hash: Option<String>,
    oid: Option<String>,
    status: String,
    pnl: Option<f64>,
    pnl_usd: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct CreditsQuery {
    bot_pubkey: Option<String>,
    follower_pubkey: Option<String>,
}

#[derive(Debug, Serialize)]
struct CreditItem {
    bot_pubkey: String,
    follower_pubkey: String,
    credits: f64,
}

#[derive(Debug, Serialize)]
struct CreditsResponse {
    credits: Vec<CreditItem>,
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
    if !is_valid_eth_address(&payload.eth_address) {
        return Err(StatusCode::BAD_REQUEST);
    }

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

    let eth_addr = svc
        .get_bot_eth_address(&payload.bot_pubkey)
        .await
        .map_err(|e| {
            tracing::error!("Failed to query bot eth address: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::BAD_REQUEST)?;

    enforce_subscription_limit(&state, &eth_addr).await?;

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

/// Record a trade tx hash for later settlement/PnL tracking
async fn record_trade(
    State(state): State<AppState>,
    Json(payload): Json<RecordTradeRequest>,
) -> Result<Json<RelayResponse>, StatusCode> {
    let svc = match &state.subscriptions {
        Some(s) => s,
        None => return Err(StatusCode::SERVICE_UNAVAILABLE),
    };

    // Ensure bot exists to avoid FK errors
    let exists = svc.bot_exists(&payload.bot_pubkey).await.map_err(|e| {
        tracing::error!("Failed to verify bot before recording trade: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    if !exists {
        return Err(StatusCode::BAD_REQUEST);
    }

    let role = if payload.role.eq_ignore_ascii_case("follower") {
        "follower"
    } else {
        "leader"
    };

    svc.record_trade_tx(
        &payload.bot_pubkey,
        payload.follower_pubkey.as_deref(),
        role,
        &payload.symbol,
        &payload.side,
        payload.size,
        payload.price,
        payload.tx_hash.as_deref(),
        payload.oid.as_deref(),
        false,
    )
    .await
    .map_err(|e| {
        if let Some(db_err) = e.downcast_ref::<tokio_postgres::Error>() {
            if let Some(code) = db_err.code() {
                if code == &SqlState::FOREIGN_KEY_VIOLATION {
                    tracing::warn!(
                        "record_trade foreign key violation (bot missing?): {}",
                        db_err
                    );
                    return StatusCode::BAD_REQUEST;
                }
                if code == &SqlState::UNIQUE_VIOLATION {
                    tracing::warn!("record_trade duplicate tx_hash/oid: {}", db_err);
                    return StatusCode::OK; // idempotent insert
                }
            }
        }
        tracing::error!("Failed to record trade tx: {:?}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(RelayResponse {
        success: true,
        message: "trade recorded".to_string(),
    }))
}

/// Update trade settlement/PnL after chain confirmation
async fn update_trade_settlement(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<UpdateSettlementRequest>,
) -> Result<Json<RelayResponse>, StatusCode> {
    let svc = match &state.subscriptions {
        Some(s) => s,
        None => return Err(StatusCode::SERVICE_UNAVAILABLE),
    };

    if !is_token_valid(&headers, state.settlement_token.as_deref()) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    svc.update_trade_settlement(
        payload.tx_hash.as_deref(),
        payload.oid.as_deref(),
        &payload.status,
        payload.pnl,
        payload.pnl_usd,
    )
    .await
    .map_err(|e| {
        tracing::error!("Failed to update trade settlement: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(RelayResponse {
        success: true,
        message: "trade settlement updated".to_string(),
    }))
}

/// List credits (optionally filter by bot or follower)
async fn list_credits(
    State(state): State<AppState>,
    Query(q): Query<CreditsQuery>,
) -> Result<Json<CreditsResponse>, StatusCode> {
    let svc = match &state.subscriptions {
        Some(s) => s,
        None => return Err(StatusCode::SERVICE_UNAVAILABLE),
    };

    let rows = svc
        .list_credits(q.bot_pubkey.as_deref(), q.follower_pubkey.as_deref())
        .await
        .map_err(|e| {
            tracing::error!("Failed to list credits: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(CreditsResponse {
        credits: rows
            .into_iter()
            .map(|r| CreditItem {
                bot_pubkey: r.bot_pubkey,
                follower_pubkey: r.follower_pubkey,
                credits: r.credits,
            })
            .collect(),
    }))
}

fn is_token_valid(headers: &HeaderMap, expected: Option<&str>) -> bool {
    match expected {
        None => true, // no token configured -> allow
        Some(token) => headers
            .get("X-Settlement-Token")
            .and_then(|h| h.to_str().ok())
            .map(|v| v == token)
            .unwrap_or(false),
    }
}

fn is_valid_eth_address(addr: &str) -> bool {
    if addr.len() != 42 || !addr.starts_with("0x") {
        return false;
    }
    addr.as_bytes()[2..]
        .iter()
        .all(|b| (*b as char).is_ascii_hexdigit())
}

async fn enforce_subscription_limit(state: &AppState, eth_addr: &str) -> Result<(), StatusCode> {
    if state.subscription_daily_limit == 0 {
        return Ok(());
    }

    let mut guard = state.subscription_limiters.lock().await;
    let entry = guard
        .entry(eth_addr.to_string())
        .or_insert_with(|| DailyLimit::new(state.subscription_daily_limit));

    entry.reset_if_needed();
    if entry.count >= entry.limit {
        return Err(StatusCode::TOO_MANY_REQUESTS);
    }
    entry.count += 1;
    Ok(())
}

/// List subscriptions for a bot
async fn list_subscriptions(
    State(state): State<AppState>,
    Path(bot_pubkey): Path<String>,
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

/// List subscriptions for a bot resolved by eth address
async fn list_subscriptions_by_eth(
    State(state): State<AppState>,
    Path(eth_address): Path<String>,
) -> Result<Json<SubscriptionsResponse>, StatusCode> {
    let svc = match &state.subscriptions {
        Some(s) => s,
        None => return Err(StatusCode::SERVICE_UNAVAILABLE),
    };

    let bot = svc.find_bot_by_eth(&eth_address).await.map_err(|e| {
        tracing::error!("Failed to lookup bot by eth address: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let bot_pubkey = match bot {
        Some(b) => b.bot_pubkey,
        None => return Err(StatusCode::NOT_FOUND),
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
