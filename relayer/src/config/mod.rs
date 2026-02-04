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
pub struct AppConfig {
    pub relay: RelayConfig,
    pub deduplication: DeduplicationConfig,
    pub output: OutputConfig,
    #[serde(default)]
    pub filters: FilterConfig,
    #[serde(default)]
    pub postgres: Option<PostgresConfig>,
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
