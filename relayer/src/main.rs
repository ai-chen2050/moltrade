mod api;
mod config;
mod core;
mod storage;

use anyhow::{Context, Result};
use api::{metrics::Metrics, rest_api, websocket};
use clap::Parser;
use config::AppConfig;
use core::{
    dedupe_engine::DeduplicationEngine, downstream::DownstreamForwarder, event_router::EventRouter,
    relay_pool::RelayPool, subscription::FanoutMessage, subscription::SubscriptionService,
};
use flume::Receiver;
use nostr_sdk::Event;
use nostr_sdk::ToBech32;
use nostr_sdk::prelude::{Client, Keys};
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
    let cfg = load_config(&cli)?;

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
    let nostr_keys = load_nostr_keys(&cfg)?;
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
    let rest_router = rest_api::create_router(
        relay_pool.clone(),
        dedupe_engine.clone(),
        metrics.clone(),
        subscription_service.clone(),
        platform_pubkey.clone(),
    );

    // Handle downstream forwarding based on config
    let websocket_enabled = cfg
        .as_ref()
        .map(|c| c.output.websocket_enabled)
        .unwrap_or(true);

    let app = build_app(
        &cfg,
        rest_router,
        downstream_rx,
        fanout_rx,
        websocket_enabled,
        rocksdb.clone(),
    );

    // Start HTTP server
    let addr = match &cfg {
        Some(c) => format!("0.0.0.0:{}", c.output.websocket_port),
        None => "0.0.0.0:8080".to_string(),
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

fn load_config(cli: &Cli) -> Result<Option<AppConfig>> {
    match &cli.config {
        Some(path) => Ok(Some(AppConfig::load_from_path(path)?)),
        None => Ok(None),
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

fn load_nostr_keys(cfg: &Option<AppConfig>) -> Result<Option<Keys>> {
    if let Some(nostr) = cfg.as_ref().and_then(|c| c.nostr.as_ref()) {
        let keys = Keys::parse(&nostr.secret_key).context("Failed to parse nostr secret key")?;
        Ok(Some(keys))
    } else {
        Ok(None)
    }
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
    cfg: &Option<AppConfig>,
    rest_router: axum::Router,
    downstream_rx: Receiver<Event>,
    fanout_rx: Option<Receiver<FanoutMessage>>,
    websocket_enabled: bool,
    rocksdb: Arc<RocksDBStore>,
) -> axum::Router {
    if websocket_enabled {
        let downstream_rx_arc = Arc::new(downstream_rx);
        let fanout_rx_arc = fanout_rx.map(Arc::new);
        let ws_router =
            websocket::create_websocket_router(downstream_rx_arc.clone(), fanout_rx_arc);
        axum::Router::new().merge(rest_router).merge(ws_router)
    } else {
        let downstream_tcp = cfg
            .as_ref()
            .map(|c| c.output.downstream_tcp.clone())
            .unwrap_or_default();
        let downstream_rest = cfg
            .as_ref()
            .map(|c| c.output.downstream_rest.clone())
            .unwrap_or_default();

        if !downstream_tcp.is_empty() || !downstream_rest.is_empty() {
            let forwarder = DownstreamForwarder::new(
                downstream_tcp.clone(),
                downstream_rest.clone(),
                rocksdb.clone(),
            );
            let downstream_rx_for_forwarder = downstream_rx;
            tokio::spawn(async move {
                if let Err(e) = forwarder.forward_events(downstream_rx_for_forwarder).await {
                    error!("Downstream forwarder error: {}", e);
                }
            });
            info!(
                "Downstream forwarding enabled (TCP: {:?}, REST: {:?})",
                downstream_tcp, downstream_rest
            );
        } else {
            warn!(
                "websocket_enabled is false but no downstream endpoints configured. Events will be dropped."
            );
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
