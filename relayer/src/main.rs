mod api;
mod config;
mod core;
mod storage;

use anyhow::{Context, Result};
use api::{metrics::Metrics, rest_api, websocket};
use clap::Parser;
use config::AppConfig;
use core::{
    dedupe_engine::DeduplicationEngine, event_router::EventRouter, relay_pool::RelayPool,
    settlement_worker::SettlementWorker, subscription::FanoutMessage,
    subscription::SubscriptionService,
};
use flume::Receiver;
use nostr_sdk::Event;
use nostr_sdk::ToBech32;
use nostr_sdk::prelude::{Client, Keys};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use storage::rocksdb_store::RocksDBStore;
use tokio::signal;
use tracing::{error, info, warn};
use tracing_subscriber;

#[derive(Parser, Debug)]
#[command(name = "moltrade-relayer")]
#[command(about = "Moltrade Relayer service", version)]
struct Cli {
    /// Path to configuration TOML file
    #[arg(long)]
    config: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // CLI
    let cli = Cli::parse();

    // Load config if provided
    let (cfg, cfg_path) = load_config(&cli)?;

    // Initialize tracing - prefer config log level if provided, else env, else default
    init_tracing(&cfg);

    info!("Starting Moltrade Relayer...");

    // Initialize metrics
    let metrics = Arc::new(Metrics::new().context("Failed to initialize metrics")?);

    // Initialize RocksDB storage
    let rocksdb = init_rocksdb(&cfg)?;
    info!("RocksDB storage initialized");

    // Initialize deduplication engine
    let dedupe_engine = init_dedupe_engine(&cfg, rocksdb.clone(), metrics.clone());
    info!("Deduplication engine initialized");

    // Warm dedup engine from RocksDB successful-forward index to avoid duplicate downstream sends after restart
    let warm_limit = cfg
        .as_ref()
        .map(|c| c.deduplication.hotset_size)
        .unwrap_or(10_000);
    dedupe_engine.warm_from_db(warm_limit).await;

    // Initialize relay pool
    let (health_check_interval, max_connections) = relay_settings(&cfg);
    let allowed_kinds = resolve_allowed_kinds(&cfg);
    let nostr_keys = load_nostr_keys(&cfg, cfg_path.as_deref())?;
    let platform_pubkey = nostr_keys.as_ref().map(|k| {
        k.public_key()
            .to_bech32()
            .unwrap_or_else(|_| k.public_key().to_hex())
    });
    let nostr_client = init_nostr_publisher(&cfg, nostr_keys.as_ref()).await?;
    let (relay_pool, relay_event_rx) = RelayPool::new(
        health_check_interval,
        max_connections,
        allowed_kinds.clone(),
    );
    let relay_pool = Arc::new(relay_pool.with_metrics(metrics.clone()));
    info!("Relay pool initialized");

    // Start health checks
    relay_pool.start_health_checks().await;
    info!("Health checks started");

    // Connect to relays (example - load from config file or environment)
    let relay_urls = bootstrap_relays(&cfg).await?;
    info!("Loading {} relay URLs", relay_urls.len());

    relay_pool
        .subscribe_all(relay_urls)
        .await
        .context("Failed to subscribe to relays")?;
    info!("Subscribed to all relays");

    // Create downstream event channel
    let (downstream_tx, downstream_rx) = flume::unbounded();

    // Optional Postgres-backed subscription service for fanout
    let subscription_service = init_subscription_service(&cfg).await?;

    // Start settlement worker (Hyperliquid tx hash polling)
    if let Some(subs) = subscription_service.clone() {
        let settlement_cfg = cfg.as_ref().and_then(|c| c.settlement.clone());
        let base_url = settlement_cfg
            .as_ref()
            .map(|s| s.explorer_base.clone())
            .unwrap_or_else(|| "https://app.hyperliquid.xyz/explorer/transaction".to_string());
        let interval_secs: u64 = settlement_cfg.as_ref().map(|s| s.poll_secs).unwrap_or(30);
        let batch_limit: i64 = settlement_cfg.as_ref().map(|s| s.batch_limit).unwrap_or(50);
        let credit_cfg = settlement_cfg.as_ref().and_then(|s| s.credit.clone());
        let worker = SettlementWorker::new(
            subs.clone(),
            base_url,
            Duration::from_secs(interval_secs),
            batch_limit,
            credit_cfg,
        );
        tokio::spawn(async move { worker.run().await });
        info!(
            "Settlement worker started (interval={}s, batch={}, credit_cfg={})",
            interval_secs,
            batch_limit,
            settlement_cfg
                .as_ref()
                .and_then(|c| c.credit.as_ref())
                .map(|c| format!("leader_rate={}, follower_rate={}, min_credit={}, profit_multiplier={}, enable={}", c.leader_rate, c.follower_rate, c.min_credit, c.profit_multiplier, c.enable))
                .unwrap_or_else(|| "disabled".to_string())
        );
    }

    if let (Some(subs), Some(pk)) = (subscription_service.as_ref(), platform_pubkey.as_ref()) {
        if let Err(e) = subs
            .ensure_platform_pubkey(pk, nostr_client.clone(), nostr_keys.as_ref())
            .await
        {
            warn!("Failed to record/publish platform pubkey: {}", e);
        }
    }

    // Fanout channel (only if subscription service is enabled)
    let (fanout_tx, fanout_rx) = if subscription_service.is_some() {
        let (tx, rx) = flume::unbounded();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    // Initialize event router
    let event_router = EventRouter::new(
        dedupe_engine.clone(),
        cfg.as_ref().map(|c| c.output.batch_size).unwrap_or(100), // batch size
        Duration::from_millis(cfg.as_ref().map(|c| c.output.max_latency_ms).unwrap_or(100) as u64), // max latency
        downstream_tx.clone(),
        allowed_kinds,
        fanout_tx,
        subscription_service.clone(),
        nostr_keys,
        nostr_client.clone(),
    )
    .with_metrics(metrics.clone());

    // Spawn event router task
    let router_handle = tokio::spawn(async move {
        if let Err(e) = event_router.process_stream(relay_event_rx).await {
            error!("Event router error: {}", e);
        }
    });

    // Create REST API router
    let subscription_daily_limit = cfg
        .as_ref()
        .and_then(|c| c.subscriptions.as_ref())
        .map(|s| s.daily_limit)
        .unwrap_or(1000);
    let rest_router = rest_api::create_router(
        relay_pool.clone(),
        dedupe_engine.clone(),
        metrics.clone(),
        subscription_service.clone(),
        platform_pubkey.clone(),
        cfg.as_ref()
            .and_then(|c| c.settlement.as_ref())
            .and_then(|s| s.token.clone()),
        subscription_daily_limit,
    );

    // Build HTTP server (WebSocket streaming optional)
    let websocket_enabled = cfg
        .as_ref()
        .map(|c| c.output.websocket_enabled)
        .unwrap_or(true);

    let app = build_app(rest_router, downstream_rx, fanout_rx, websocket_enabled);

    // Start HTTP server
    let addr = match &cfg {
        Some(c) => format!("{}:{}", c.output.bind_address, c.output.websocket_port),
        None => "127.0.0.1:8080".to_string(),
    };
    info!("Starting HTTP server on {}", addr);
    let server_addr_for_logs = addr.clone();
    let server_handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .context("Failed to bind to address")
            .unwrap();
        axum::serve(listener, app)
            .await
            .context("Failed to start server")
            .unwrap();
    });

    info!("Moltrade Relayer started successfully");
    info!("REST API: http://{}", server_addr_for_logs);
    info!("WebSocket: ws://{}/ws", server_addr_for_logs);
    info!("Metrics: http://{}/metrics", server_addr_for_logs);

    // Periodically update memory usage gauge
    spawn_memory_metrics(metrics.clone());
    // Wait for shutdown signal
    signal::ctrl_c()
        .await
        .context("Failed to listen for shutdown signal")?;
    info!("Shutdown signal received, gracefully shutting down...");

    // Cancel tasks
    router_handle.abort();
    server_handle.abort();

    info!("Shutdown complete");
    Ok(())
}

/// Load relay URLs from environment or config file
/// In production, this should load from a config file or database
async fn load_relay_urls() -> Result<Vec<String>> {
    // Example: load from environment variable
    if let Ok(urls) = std::env::var("RELAY_URLS") {
        return Ok(urls
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect());
    }

    // Default example relays (replace with actual relay URLs)
    Ok(vec![
        "wss://relay.damus.io".to_string(),
        "wss://nos.lol".to_string(),
        "wss://relay.snort.social".to_string(),
    ])
}

fn load_config(cli: &Cli) -> Result<(Option<AppConfig>, Option<PathBuf>)> {
    match &cli.config {
        Some(path) => Ok((Some(AppConfig::load_from_path(path)?), Some(path.clone()))),
        None => Ok((None, None)),
    }
}

fn init_tracing(cfg: &Option<AppConfig>) {
    let default_level = cfg
        .as_ref()
        .map(|c| c.monitoring.log_level.clone())
        .unwrap_or_else(|| "info".to_string());

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| format!("moltrade_relayer={}", default_level).into()),
        )
        .init();
}

fn init_rocksdb(cfg: &Option<AppConfig>) -> Result<Arc<RocksDBStore>> {
    let rocks_path = cfg
        .as_ref()
        .map(|c| c.deduplication.rocksdb_path.as_str())
        .unwrap_or("./data/rocksdb");
    Ok(Arc::new(
        RocksDBStore::new(rocks_path).context("Failed to initialize RocksDB storage")?,
    ))
}

fn init_dedupe_engine(
    cfg: &Option<AppConfig>,
    rocksdb: Arc<RocksDBStore>,
    metrics: Arc<Metrics>,
) -> Arc<DeduplicationEngine> {
    match cfg {
        Some(c) => Arc::new(
            DeduplicationEngine::new_with_params(
                rocksdb.clone(),
                c.deduplication.hotset_size,
                c.deduplication.bloom_capacity,
                c.deduplication.lru_size,
            )
            .with_metrics(metrics),
        ),
        None => Arc::new(DeduplicationEngine::new(rocksdb).with_metrics(metrics)),
    }
}

fn relay_settings(cfg: &Option<AppConfig>) -> (Duration, usize) {
    match cfg {
        Some(c) => (
            Duration::from_secs(c.relay.health_check_interval),
            c.relay.max_connections,
        ),
        None => (Duration::from_secs(30), 10_000),
    }
}

fn resolve_allowed_kinds(cfg: &Option<AppConfig>) -> Option<Vec<u16>> {
    cfg.as_ref()
        .map(|c| c.filters.allowed_kinds.clone())
        .filter(|kinds| !kinds.is_empty())
}

fn load_nostr_keys(cfg: &Option<AppConfig>, cfg_path: Option<&Path>) -> Result<Option<Keys>> {
    if let Some(nostr) = cfg.as_ref().and_then(|c| c.nostr.as_ref()) {
        let secret = nostr.secret_key.trim();

        if secret.is_empty() {
            let keys = Keys::generate();
            persist_generated_secret(cfg_path, &keys)?;
            return Ok(Some(keys));
        }

        match Keys::parse(secret) {
            Ok(keys) => Ok(Some(keys)),
            Err(_) => {
                warn!("Invalid nostr secret key in config; generating a new one.");
                let keys = Keys::generate();
                persist_generated_secret(cfg_path, &keys)?;
                Ok(Some(keys))
            }
        }
    } else {
        Ok(None)
    }
}

fn persist_generated_secret(cfg_path: Option<&Path>, keys: &Keys) -> Result<()> {
    let nsec = keys
        .secret_key()
        .to_bech32()
        .context("Failed to encode generated secret key")?;

    if let Some(path) = cfg_path {
        if let Ok(contents) = std::fs::read_to_string(path) {
            match toml::from_str::<toml::Value>(&contents) {
                Ok(mut value) => {
                    value["nostr"]["secret_key"] = toml::Value::String(nsec.clone());
                    match toml::to_string_pretty(&value) {
                        Ok(serialized) => {
                            if let Err(e) = std::fs::write(path, serialized) {
                                warn!("Failed to write generated nostr key to config: {}", e);
                            }
                        }
                        Err(e) => {
                            warn!("Failed to serialize config with generated nostr key: {}", e)
                        }
                    }
                }
                Err(e) => warn!(
                    "Failed to parse config TOML for nostr key persistence: {}",
                    e
                ),
            }
        } else {
            warn!("Failed to read config file for nostr key persistence");
        }
    } else {
        warn!("No config path provided; generated nostr key will not be persisted to file.");
    }

    Ok(())
}

async fn init_nostr_publisher(
    cfg: &Option<AppConfig>,
    keys: Option<&Keys>,
) -> Result<Option<Arc<Client>>> {
    let keys = match keys {
        Some(k) => k,
        None => return Ok(None),
    };

    let client = Client::new(keys.clone());
    let relays = match cfg {
        Some(c) => c.relay.bootstrap_relays.clone(),
        None => load_relay_urls().await?,
    };

    for url in relays {
        client.add_relay(url).await.ok();
    }
    client.connect().await;
    Ok(Some(Arc::new(client)))
}

async fn bootstrap_relays(cfg: &Option<AppConfig>) -> Result<Vec<String>> {
    match cfg {
        Some(c) => Ok(c.relay.bootstrap_relays.clone()),
        None => load_relay_urls().await,
    }
}

async fn init_subscription_service(
    cfg: &Option<AppConfig>,
) -> Result<Option<Arc<SubscriptionService>>> {
    if let Some(pg) = cfg.as_ref().and_then(|c| c.postgres.as_ref()) {
        let svc = SubscriptionService::new(&pg.dsn, pg.max_connections)
            .await
            .context("Failed to initialize subscription service")?;
        Ok(Some(Arc::new(svc)))
    } else {
        Ok(None)
    }
}

fn build_app(
    rest_router: axum::Router,
    downstream_rx: Receiver<Event>,
    fanout_rx: Option<Receiver<FanoutMessage>>,
    websocket_enabled: bool,
) -> axum::Router {
    if websocket_enabled {
        let downstream_rx_arc = Arc::new(downstream_rx);
        let fanout_rx_arc = fanout_rx.map(Arc::new);
        let ws_router =
            websocket::create_websocket_router(downstream_rx_arc.clone(), fanout_rx_arc);
        axum::Router::new().merge(rest_router).merge(ws_router)
    } else {
        let mut warned = false;
        tokio::spawn(async move {
            // Drain events to avoid unbounded queue growth when WebSocket streaming is disabled
            while let Ok(_event) = downstream_rx.recv_async().await {
                if !warned {
                    warn!("WebSocket streaming disabled; dropping downstream events.");
                    warned = true;
                }
            }
        });

        if let Some(rx) = fanout_rx {
            tokio::spawn(async move { while rx.recv_async().await.is_ok() {} });
        }

        axum::Router::new().merge(rest_router)
    }
}

fn spawn_memory_metrics(metrics: Arc<Metrics>) {
    tokio::spawn(async move {
        use sysinfo::{ProcessesToUpdate, System};
        let mut sys = System::new();
        loop {
            sys.refresh_processes(ProcessesToUpdate::All, true);
            if let Ok(pid) = sysinfo::get_current_pid() {
                if let Some(process) = sys.process(pid) {
                    metrics.memory_usage.set(process.memory() as f64 / 1024.0);
                }
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });
}
