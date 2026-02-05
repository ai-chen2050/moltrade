use anyhow::{Context, Result};
use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct RelayConfig {
    pub bootstrap_relays: Vec<String>,
    pub max_connections: usize,
    pub health_check_interval: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeduplicationConfig {
    pub hotset_size: usize,
    pub bloom_capacity: usize,
    pub lru_size: usize,
    pub rocksdb_path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OutputConfig {
    pub websocket_enabled: bool,
    pub websocket_port: u16,
    #[serde(default)]
    pub downstream_tcp: Vec<String>,
    #[serde(default)]
    pub downstream_rest: Vec<String>,
    pub batch_size: usize,
    pub max_latency_ms: u64,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct FilterConfig {
    #[serde(default = "default_allowed_kinds")]
    pub allowed_kinds: Vec<u16>,
}

fn default_allowed_kinds() -> Vec<u16> {
    vec![30931, 30932, 30933, 30934]
}

#[derive(Debug, Clone, Deserialize)]
pub struct MonitoringConfig {
    pub prometheus_port: u16,
    pub log_level: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PostgresConfig {
    pub dsn: String,
    #[serde(default = "default_pg_pool_size")]
    pub max_connections: usize,
}

fn default_pg_pool_size() -> usize {
    5
}

#[derive(Debug, Clone, Deserialize)]
pub struct NostrConfig {
    /// Platform nostr nsec (hex or bech32) used to decrypt inbound and encrypt outbound
    pub secret_key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SettlementConfig {
    #[serde(default = "default_explorer_base")]
    pub explorer_base: String,
    #[serde(default = "default_poll_secs")]
    pub poll_secs: u64,
    #[serde(default = "default_batch_limit")]
    pub batch_limit: i64,
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub credit: Option<SettlementCreditConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SettlementCreditConfig {
    #[serde(default = "default_leader_rate")]
    pub leader_rate: f64,
    #[serde(default = "default_follower_rate")]
    pub follower_rate: f64,
    #[serde(default = "default_min_credit")]
    pub min_credit: f64,
    #[serde(default = "default_profit_multiplier")]
    pub profit_multiplier: f64,
    #[serde(default = "default_credit_enable")]
    pub enable: bool,
}

fn default_explorer_base() -> String {
    "https://app.hyperliquid.xyz/explorer/transaction".to_string()
}

fn default_poll_secs() -> u64 {
    30
}

fn default_batch_limit() -> i64 {
    50
}

fn default_leader_rate() -> f64 {
    0.002
}

fn default_follower_rate() -> f64 {
    0.001
}

fn default_min_credit() -> f64 {
    0.5
}

fn default_profit_multiplier() -> f64 {
    1.2
}

fn default_credit_enable() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct SubscriptionsConfig {
    #[serde(default = "default_subscription_daily_limit")]
    pub daily_limit: u64,
}

fn default_subscription_daily_limit() -> u64 {
    1000
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub relay: RelayConfig,
    pub deduplication: DeduplicationConfig,
    pub output: OutputConfig,
    #[serde(default)]
    pub filters: FilterConfig,
    #[serde(default)]
    pub postgres: Option<PostgresConfig>,
    #[serde(default)]
    pub nostr: Option<NostrConfig>,
    #[serde(default)]
    pub settlement: Option<SettlementConfig>,
    #[serde(default)]
    pub subscriptions: Option<SubscriptionsConfig>,
    pub monitoring: MonitoringConfig,
}

impl AppConfig {
    pub fn load_from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let data = fs::read_to_string(&path).with_context(|| {
            format!(
                "Failed to read config file at {}",
                path.as_ref().to_string_lossy()
            )
        })?;
        let cfg: AppConfig = toml::from_str(&data).context("Failed to parse TOML config")?;
        Ok(cfg)
    }
}
