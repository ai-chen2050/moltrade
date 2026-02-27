use anyhow::{Context, Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use chrono::{DateTime, TimeZone, Utc};
use deadpool_postgres::{Config as PgConfig, Pool, Runtime};
use nostr_sdk::prelude::{Client, EventBuilder, Keys};
use nostr_sdk::{Event, Kind};
use rand::RngCore;
use rand::rng;
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio_postgres::types::ToSql;
use tokio_postgres::{NoTls, Row};
use tracing::{info, warn};

/// Row shape for subscriptions
#[derive(Debug, Clone)]
pub struct SubscriptionRow {
    pub follower_pubkey: String,
    pub shared_secret: String,
}

#[derive(Debug, Clone)]
pub struct BotRecord {
    pub bot_pubkey: String,
    pub nostr_pubkey: String,
    pub eth_address: String,
}

#[derive(Debug, Clone)]
pub struct PendingTrade {
    pub tx_hash: Option<String>,
    pub oid: Option<String>,
    pub bot_pubkey: String,
    pub follower_pubkey: Option<String>,
    pub role: String,
    pub size: f64,
    pub price: f64,
    pub pnl_usd: Option<f64>,
    pub is_test: bool,
}

#[derive(Debug, Clone)]
pub struct CreditBalance {
    pub bot_pubkey: String,
    pub follower_pubkey: String,
    pub credits: f64,
}

/// Dashboard summary metrics for frontend cards
#[derive(Debug, Clone, Serialize)]
pub struct DashboardSummary {
    pub total_ai_agents: i64,
    pub total_ai_agents_change_pct: f64,
    pub total_strategies: i64,
    pub total_strategies_change_pct: f64,
    pub cumulative_pnl: f64,
    pub cumulative_pnl_change_pct: f64,
    pub avg_win_rate: f64,
    pub avg_win_rate_change_pct: f64,
}

#[derive(Debug, Clone)]
struct DashboardSummaryCache {
    value: DashboardSummary,
    fetched_at: Instant,
}

/// Strategy row for trending strategy page
#[derive(Debug, Clone, Serialize)]
pub struct StrategyTrendingItem {
    pub strategy: String,
    pub category: String,
    pub agents: i64,
    pub followers: i64,
    pub profit_share: f64,
    pub buy_count: i64,
    pub sell_count: i64,
    pub volume: f64,
    pub pnl_period: f64,
    pub roi: f64,
    pub max_drawdown: f64,
    pub trading_pairs: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub win_rate: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyOverview {
    pub strategy: String,
    pub category: String,
    pub trading_pairs: i64,
    pub profit_share: f64,
    pub total_roi: f64,
    pub total_profit: f64,
    pub total_assets: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub win_rate: Option<f64>,
    pub trading_frequency: String,
    pub latest_price: f64,
    pub runtime_days: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyPositionItem {
    pub asset: String,
    pub side: String,
    pub amount: f64,
    pub entry_price: f64,
    pub current_price: f64,
    pub unrealized_pnl: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyActivityItem {
    pub id: i64,
    pub action: String,
    pub asset: String,
    pub amount: f64,
    pub price: f64,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyDetail {
    pub overview: StrategyOverview,
    pub current_positions: Vec<StrategyPositionItem>,
    pub activity_logs: Vec<StrategyActivityItem>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyPerformancePoint {
    pub ts: DateTime<Utc>,
    pub value: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyPerformanceSeries {
    pub strategy: String,
    pub metric: String,
    pub interval: String,
    pub points: Vec<StrategyPerformancePoint>,
}

#[derive(Debug, Clone, Copy)]
pub enum StrategyRankBy {
    Pnl,
    Followers,
    Roi,
}

#[derive(Debug, Clone, Copy)]
pub enum SortOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy)]
pub enum StrategyPerformanceMetric {
    Pnl,
    Roi,
}

impl StrategyPerformanceMetric {
    pub fn as_str(&self) -> &'static str {
        match self {
            StrategyPerformanceMetric::Pnl => "pnl",
            StrategyPerformanceMetric::Roi => "roi",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum StrategyPerformanceInterval {
    Hour,
    Day,
}

impl StrategyPerformanceInterval {
    fn date_trunc_unit(&self) -> &'static str {
        match self {
            StrategyPerformanceInterval::Hour => "hour",
            StrategyPerformanceInterval::Day => "day",
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            StrategyPerformanceInterval::Hour => "1h",
            StrategyPerformanceInterval::Day => "1d",
        }
    }
}

#[derive(Debug, Clone)]
pub enum StrategyCategory {
    Grid,
    Dca,
    Martingale,
    Momentum,
    Arbitrage,
    SignalBased,
}

impl StrategyCategory {
    fn as_str(&self) -> &'static str {
        match self {
            StrategyCategory::Grid => "grid",
            StrategyCategory::Dca => "dca",
            StrategyCategory::Martingale => "martingale",
            StrategyCategory::Momentum => "momentum",
            StrategyCategory::Arbitrage => "arbitrage",
            StrategyCategory::SignalBased => "signal-based",
        }
    }
}

fn strategy_category_label(strategy: &str) -> &'static str {
    if strategy.eq_ignore_ascii_case("grid") {
        "grid"
    } else if strategy.eq_ignore_ascii_case("momentum") {
        "momentum"
    } else if strategy.eq_ignore_ascii_case("mean_reversion") {
        "dca"
    } else if strategy.to_ascii_lowercase().contains("arbitrage") {
        "arbitrage"
    } else if strategy.to_ascii_lowercase().contains("martingale") {
        "martingale"
    } else {
        "signal-based"
    }
}

/// Leaderboard row: one leader with 30D aggregated stats
#[derive(Debug, Clone, Serialize)]
pub struct LeaderboardItem {
    pub bot_pubkey: String,
    pub name: String,
    pub eth_address: String,
    pub followers: i64,
    pub buy_count: i64,
    pub sell_count: i64,
    pub volume: f64,
    pub pnl_30d: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub win_rate: Option<f64>,
}

/// Leader detail: profile + 7D stats + distribution + holdings/trades
#[derive(Debug, Clone, Serialize)]
pub struct LeaderDetail {
    pub bot_pubkey: String,
    pub name: String,
    pub eth_address: String,
    pub realized_pnl_7d: f64,
    pub total_pnl: f64,
    pub unrealized_pnl: Option<f64>,
    pub win_rate_7d: Option<f64>,
    pub buy_count_7d: i64,
    pub sell_count_7d: i64,
    pub volume_7d: f64,
    pub avg_duration_7d_secs: Option<f64>,
    pub success_count_7d: i64,
    pub failure_count_7d: i64,
    pub token_count_7d: i64,
    pub holdings: Vec<LeaderHolding>,
    pub trades: Vec<LeaderTradeRow>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LeaderHolding {
    pub symbol: String,
    pub realized_pnl: f64,
    pub trade_count: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct LeaderTradeRow {
    pub id: i64,
    pub symbol: String,
    pub side: String,
    pub size: f64,
    pub price: f64,
    pub pnl_usd: Option<f64>,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct SignalInsert {
    pub event_id: String,
    pub kind: u16,
    pub bot_pubkey: Option<String>,
    pub leader_pubkey: String,
    pub follower_pubkey: Option<String>,
    pub agent_eth_address: Option<String>,
    pub role: Option<String>,
    pub symbol: Option<String>,
    pub strategy: Option<String>,
    pub side: Option<String>,
    pub size: Option<f64>,
    pub price: Option<f64>,
    pub status: Option<String>,
    pub tx_hash: Option<String>,
    pub pnl: Option<f64>,
    pub pnl_usd: Option<f64>,
    pub raw_content: String,
    pub event_created_at: DateTime<Utc>,
}

/// Message ready for fanout to followers over WebSocket
#[derive(Debug, Clone, Serialize)]
pub struct FanoutMessage {
    pub target_pubkey: String,
    pub bot_pubkey: String,
    pub kind: u16,
    pub original_event_id: String,
    pub payload: String,
}

/// Service managing Postgres-backed subscriptions and fanout encryption
#[derive(Clone, Debug)]
pub struct SubscriptionService {
    pool: Pool,
    dashboard_summary_cache: Arc<Mutex<Option<DashboardSummaryCache>>>,
}

impl SubscriptionService {
    /// Build a Postgres pool and ensure schema
    pub async fn new(dsn: &str, max_connections: usize) -> Result<Self> {
        let mut cfg = PgConfig::new();
        cfg.url = Some(dsn.to_string());
        cfg.pool = Some(deadpool_postgres::PoolConfig {
            max_size: max_connections,
            ..Default::default()
        });

        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .context("Failed to create Postgres pool")?;

        let svc = Self {
            pool,
            dashboard_summary_cache: Arc::new(Mutex::new(None)),
        };
        svc.init_schema().await?;
        Ok(svc)
    }

    /// Initialize tables if they do not exist
    async fn init_schema(&self) -> Result<()> {
        let client = self.pool.get().await.context("Failed to get PG client")?;
        client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS bots (
                    bot_pubkey TEXT PRIMARY KEY,
                    nostr_pubkey TEXT NOT NULL,
                    eth_address TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                ALTER TABLE bots ADD COLUMN IF NOT EXISTS nostr_pubkey TEXT NOT NULL DEFAULT '';
                ALTER TABLE bots ADD COLUMN IF NOT EXISTS eth_address TEXT NOT NULL DEFAULT '';
                ALTER TABLE bots ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now();
                CREATE TABLE IF NOT EXISTS subscriptions (
                    id BIGSERIAL PRIMARY KEY,
                    bot_pubkey TEXT NOT NULL REFERENCES bots(bot_pubkey) ON DELETE CASCADE,
                    follower_pubkey TEXT NOT NULL,
                    shared_secret TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(bot_pubkey, follower_pubkey)
                );
                CREATE TABLE IF NOT EXISTS platform_state (
                    id TEXT PRIMARY KEY,
                    pubkey TEXT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                CREATE TABLE IF NOT EXISTS trade_executions (
                    id BIGSERIAL PRIMARY KEY,
                    bot_pubkey TEXT NOT NULL REFERENCES bots(bot_pubkey) ON DELETE CASCADE,
                    follower_pubkey TEXT NULL,
                    role TEXT NOT NULL CHECK (role IN ('leader','follower')),
                    symbol TEXT NOT NULL,
                    strategy TEXT NULL,
                    side TEXT NOT NULL,
                    size DOUBLE PRECISION NOT NULL,
                    price DOUBLE PRECISION NOT NULL,
                    tx_hash TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL DEFAULT 'pending',
                    pnl DOUBLE PRECISION NULL,
                    pnl_usd DOUBLE PRECISION NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                ALTER TABLE trade_executions ALTER COLUMN size TYPE DOUBLE PRECISION USING size::double precision;
                ALTER TABLE trade_executions ALTER COLUMN price TYPE DOUBLE PRECISION USING price::double precision;
                ALTER TABLE trade_executions ALTER COLUMN pnl TYPE DOUBLE PRECISION USING pnl::double precision;
                ALTER TABLE trade_executions ALTER COLUMN pnl_usd TYPE DOUBLE PRECISION USING pnl_usd::double precision;
                ALTER TABLE trade_executions ALTER COLUMN tx_hash DROP NOT NULL;
                ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS oid TEXT UNIQUE;
                ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS is_test BOOLEAN NOT NULL DEFAULT false;
                ALTER TABLE trade_executions ADD COLUMN IF NOT EXISTS strategy TEXT NULL;
                CREATE INDEX IF NOT EXISTS idx_trade_executions_status ON trade_executions (status);
                CREATE INDEX IF NOT EXISTS idx_trade_executions_status_created_at ON trade_executions (status, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_trade_executions_strategy_created_at ON trade_executions (strategy, created_at DESC);
                CREATE TABLE IF NOT EXISTS credits (
                    bot_pubkey TEXT NOT NULL REFERENCES bots(bot_pubkey) ON DELETE CASCADE,
                    follower_pubkey TEXT NOT NULL,
                    credits NUMERIC NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (bot_pubkey, follower_pubkey)
                );
                ALTER TABLE credits ALTER COLUMN credits TYPE DOUBLE PRECISION USING credits::double precision;
                ALTER TABLE credits ALTER COLUMN credits SET DEFAULT 0.0;
                CREATE TABLE IF NOT EXISTS signals (
                    id BIGSERIAL PRIMARY KEY,
                    event_id TEXT NOT NULL UNIQUE,
                    kind INTEGER NOT NULL,
                    bot_pubkey TEXT NULL REFERENCES bots(bot_pubkey) ON DELETE SET NULL,
                    leader_pubkey TEXT NOT NULL,
                    follower_pubkey TEXT NULL,
                    agent_eth_address TEXT NULL,
                    role TEXT NULL,
                    symbol TEXT NULL,
                    strategy TEXT NULL,
                    side TEXT NULL,
                    size DOUBLE PRECISION NULL,
                    price DOUBLE PRECISION NULL,
                    status TEXT NULL,
                    tx_hash TEXT NULL,
                    pnl DOUBLE PRECISION NULL,
                    pnl_usd DOUBLE PRECISION NULL,
                    raw_content TEXT NOT NULL,
                    event_created_at TIMESTAMPTZ NOT NULL,
                    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                ALTER TABLE signals ADD COLUMN IF NOT EXISTS strategy TEXT NULL;",
            )
            .await
            .context("Failed to initialize subscription schema")?;
        Ok(())
    }

    /// Register or upsert a bot
    pub async fn register_bot(
        &self,
        bot_pubkey: &str,
        nostr_pubkey: &str,
        eth_address: &str,
        name: &str,
    ) -> Result<()> {
        let client = self.pool.get().await.context("Failed to get PG client")?;
        client
            .execute(
                "INSERT INTO bots (bot_pubkey, nostr_pubkey, eth_address, name) VALUES ($1, $2, $3, $4)
                 ON CONFLICT (bot_pubkey) DO UPDATE SET name = EXCLUDED.name, nostr_pubkey = EXCLUDED.nostr_pubkey, eth_address = EXCLUDED.eth_address",
                &[&bot_pubkey, &nostr_pubkey, &eth_address, &name],
            )
            .await
            .context("Failed to upsert bot")?;
        self.invalidate_dashboard_summary_cache().await;
        Ok(())
    }

    /// Add or update a subscription for a follower
    pub async fn add_subscription(
        &self,
        bot_pubkey: &str,
        follower_pubkey: &str,
        shared_secret: &str,
    ) -> Result<()> {
        let client = self.pool.get().await.context("Failed to get PG client")?;
        client
            .execute(
                "INSERT INTO subscriptions (bot_pubkey, follower_pubkey, shared_secret)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (bot_pubkey, follower_pubkey) DO UPDATE
                 SET shared_secret = EXCLUDED.shared_secret",
                &[&bot_pubkey, &follower_pubkey, &shared_secret],
            )
            .await
            .context("Failed to upsert subscription")?;
        Ok(())
    }

    /// List subscriptions for a bot
    pub async fn list_subscriptions(&self, bot_pubkey: &str) -> Result<Vec<SubscriptionRow>> {
        let client = self.pool.get().await.context("Failed to get PG client")?;
        let rows = client
            .query(
                "SELECT follower_pubkey, shared_secret FROM subscriptions WHERE bot_pubkey = $1",
                &[&bot_pubkey],
            )
            .await
            .context("Failed to query subscriptions")?;

        Ok(rows
            .into_iter()
            .map(|row| SubscriptionRow {
                follower_pubkey: row.get(0),
                shared_secret: row.get(1),
            })
            .collect())
    }

    /// Produce encrypted fanout messages for all followers of the bot that emitted the event
    pub async fn fanout_for_event(&self, event: &Event) -> Result<Vec<FanoutMessage>> {
        let bot_pubkey = event.pubkey.to_hex();
        let subscribers = self.list_subscriptions(&bot_pubkey).await?;
        if subscribers.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(subscribers.len());
        for sub in subscribers {
            let ciphertext = encrypt_with_secret(&event.content, &sub.shared_secret)?;
            out.push(FanoutMessage {
                target_pubkey: sub.follower_pubkey,
                bot_pubkey: bot_pubkey.clone(),
                kind: event.kind.as_u16(),
                original_event_id: event.id.to_hex(),
                payload: ciphertext,
            });
        }

        Ok(out)
    }

    /// Find a bot by its agent eth address
    pub async fn find_bot_by_eth(&self, eth_address: &str) -> Result<Option<BotRecord>> {
        let client = self.pool.get().await.context("Failed to get PG client")?;
        let row = client
            .query_opt(
                "SELECT bot_pubkey, nostr_pubkey, eth_address FROM bots WHERE eth_address = $1",
                &[&eth_address],
            )
            .await
            .context("Failed to query bot by eth address")?;

        Ok(row.map(row_to_bot_record))
    }

    pub async fn get_bot_eth_address(&self, bot_pubkey: &str) -> Result<Option<String>> {
        let client = self.pool.get().await.context("Failed to get PG client")?;
        let row = client
            .query_opt(
                "SELECT eth_address FROM bots WHERE bot_pubkey = $1",
                &[&bot_pubkey],
            )
            .await
            .context("Failed to query bot eth address")?;

        Ok(row.map(|r| r.get(0)))
    }

    pub async fn bot_exists(&self, bot_pubkey: &str) -> Result<bool> {
        let client = self.pool.get().await.context("Failed to get PG client")?;
        let row = client
            .query_opt("SELECT 1 FROM bots WHERE bot_pubkey = $1", &[&bot_pubkey])
            .await
            .context("Failed to query bot existence")?;
        Ok(row.is_some())
    }

    pub async fn update_bot_last_seen(&self, bot_pubkey: &str) -> Result<()> {
        let client = self.pool.get().await.context("Failed to get PG client")?;
        client
            .execute(
                "UPDATE bots SET last_seen_at = now() WHERE bot_pubkey = $1",
                &[&bot_pubkey],
            )
            .await
            .context("Failed to update bot last_seen_at")?;
        Ok(())
    }

    pub async fn ensure_platform_pubkey(
        &self,
        current_pubkey: &str,
        nostr_client: Option<Arc<Client>>,
        nostr_keys: Option<&Keys>,
    ) -> Result<()> {
        let client = self.pool.get().await.context("Failed to get PG client")?;

        let existing: Option<String> = client
            .query_opt(
                "SELECT pubkey FROM platform_state WHERE id = 'platform'",
                &[],
            )
            .await
            .context("Failed to query platform_state")?
            .map(|row| row.get(0));

        let needs_update = match &existing {
            Some(p) => p != current_pubkey,
            None => true,
        };

        if !needs_update {
            return Ok(());
        }

        client
            .execute(
                "INSERT INTO platform_state (id, pubkey, updated_at) VALUES ('platform', $1, now())
                 ON CONFLICT (id) DO UPDATE SET pubkey = EXCLUDED.pubkey, updated_at = now()",
                &[&current_pubkey],
            )
            .await
            .context("Failed to upsert platform_state")?;

        if let (Some(client), Some(_keys)) = (nostr_client, nostr_keys) {
            let content = json!({
                "op": "platform_key_rotation",
                "new_pubkey": current_pubkey,
                "previous_pubkey": existing,
                "ts": Utc::now().timestamp(),
            })
            .to_string();
            let builder = EventBuilder::new(Kind::Custom(39990), content);

            if let Err(e) = client.send_event_builder(builder).await {
                warn!("Failed to publish platform key rotation event: {}", e);
            } else {
                info!(
                    "Published platform key rotation event for pubkey {}",
                    current_pubkey
                );
            }
        } else {
            warn!("Platform key changed but no nostr publisher configured; skipping broadcast");
        }

        Ok(())
    }

    /// Record a trade submission with tx hash for later settlement/PnL lookup
    pub async fn record_trade_tx(
        &self,
        bot_pubkey: &str,
        follower_pubkey: Option<&str>,
        role: &str,
        symbol: &str,
        strategy: Option<&str>,
        side: &str,
        size: f64,
        price: f64,
        tx_hash: Option<&str>,
        oid: Option<&str>,
        is_test: bool,
    ) -> Result<()> {
        let client = self.pool.get().await.context("Failed to get PG client")?;
        client
            .execute(
                "INSERT INTO trade_executions (bot_pubkey, follower_pubkey, role, symbol, strategy, side, size, price, tx_hash, oid, is_test)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                 ON CONFLICT DO NOTHING",
                &[&bot_pubkey, &follower_pubkey, &role, &symbol, &strategy, &side, &size, &price, &tx_hash, &oid, &is_test],
            )
            .await
            .context("Failed to record trade tx")?;
        self.invalidate_dashboard_summary_cache().await;
        Ok(())
    }

    /// Update trade settlement/PnL once the chain confirms
    pub async fn update_trade_settlement(
        &self,
        tx_hash: Option<&str>,
        oid: Option<&str>,
        status: &str,
        pnl: Option<f64>,
        pnl_usd: Option<f64>,
    ) -> Result<()> {
        if tx_hash.is_none() && oid.is_none() {
            return Ok(());
        }
        let client = self.pool.get().await.context("Failed to get PG client")?;
        client
            .execute(
                "UPDATE trade_executions
                 SET status = $2,
                     pnl = COALESCE($3, pnl),
                     pnl_usd = COALESCE($4, pnl_usd),
                     updated_at = now()
                 WHERE ($1 IS NOT NULL AND tx_hash = $1)
                    OR ($5 IS NOT NULL AND oid = $5)",
                &[&tx_hash, &status, &pnl, &pnl_usd, &oid],
            )
            .await
            .context("Failed to update trade settlement")?;
        self.invalidate_dashboard_summary_cache().await;
        Ok(())
    }

    pub async fn list_pending_trades(&self, limit: i64) -> Result<Vec<PendingTrade>> {
        let client = self.pool.get().await.context("Failed to get PG client")?;
        let rows = client
            .query(
                "SELECT tx_hash, oid, bot_pubkey, follower_pubkey, role, size, price, pnl_usd, is_test
                 FROM trade_executions
                 WHERE status = 'pending'
                 ORDER BY created_at ASC
                 LIMIT $1",
                &[&limit],
            )
            .await
            .context("Failed to query pending trades")?;

        Ok(rows
            .into_iter()
            .map(|row| PendingTrade {
                tx_hash: row.get(0),
                oid: row.get(1),
                bot_pubkey: row.get(2),
                follower_pubkey: row.get(3),
                role: row.get(4),
                size: row.get(5),
                price: row.get(6),
                pnl_usd: row.get(7),
                is_test: row.get(8),
            })
            .collect())
    }

    pub async fn list_credits(
        &self,
        bot_pubkey: Option<&str>,
        follower_pubkey: Option<&str>,
    ) -> Result<Vec<CreditBalance>> {
        let client = self.pool.get().await.context("Failed to get PG client")?;

        let mut conditions = Vec::new();
        let mut owned_params: Vec<String> = Vec::new();

        if let Some(b) = bot_pubkey {
            owned_params.push(b.to_string());
            conditions.push(format!("bot_pubkey = ${}", owned_params.len()));
        }

        if let Some(f) = follower_pubkey {
            owned_params.push(f.to_string());
            conditions.push(format!("follower_pubkey = ${}", owned_params.len()));
        }

        let mut query = "SELECT bot_pubkey, follower_pubkey, credits FROM credits".to_string();
        if !conditions.is_empty() {
            query.push_str(" WHERE ");
            query.push_str(&conditions.join(" AND "));
        }
        query.push_str(" ORDER BY credits DESC");

        let params: Vec<&(dyn ToSql + Sync)> = owned_params
            .iter()
            .map(|s| s as &(dyn ToSql + Sync))
            .collect();

        let rows = client
            .query(&query, &params)
            .await
            .context("Failed to query credits")?;

        Ok(rows
            .into_iter()
            .map(|row| CreditBalance {
                bot_pubkey: row.get(0),
                follower_pubkey: row.get(1),
                credits: row.get(2),
            })
            .collect())
    }

    /// Dashboard summary: total agents, total strategies, cumulative pnl, avg win rate.
    pub async fn get_dashboard_summary(&self) -> Result<DashboardSummary> {
        const DASHBOARD_SUMMARY_CACHE_TTL: Duration = Duration::from_secs(10);

        {
            let cache = self.dashboard_summary_cache.lock().await;
            if let Some(entry) = cache.as_ref() {
                if entry.fetched_at.elapsed() < DASHBOARD_SUMMARY_CACHE_TTL {
                    return Ok(entry.value.clone());
                }
            }
        }

        let client = self.pool.get().await.context("Failed to get PG client")?;
        let row = client
            .query_one(
                r#"
WITH time_windows AS (
  SELECT
    now() - INTERVAL '7 days' AS cur_start,
    now() - INTERVAL '14 days' AS prev_start
),
bot_stats AS (
  SELECT
    COUNT(*)::bigint AS total_ai_agents,
    COUNT(*) FILTER (WHERE b.created_at >= tw.cur_start)::bigint AS current_agents_7d,
    COUNT(*) FILTER (WHERE b.created_at >= tw.prev_start AND b.created_at < tw.cur_start)::bigint AS prev_agents_7d
  FROM bots b
  CROSS JOIN time_windows tw
),
confirmed_stats AS (
  SELECT
    COALESCE(SUM(pnl_usd), 0)::double precision AS cumulative_pnl,
    COALESCE(SUM(pnl_usd) FILTER (WHERE te.created_at >= tw.cur_start), 0)::double precision AS current_pnl_7d,
    COALESCE(SUM(pnl_usd) FILTER (WHERE te.created_at >= tw.prev_start AND te.created_at < tw.cur_start), 0)::double precision AS prev_pnl_7d
  FROM trade_executions te
  CROSS JOIN time_windows tw
  WHERE te.status = 'confirmed'
),
winrate_stats AS (
  SELECT
    COUNT(*) FILTER (WHERE te.status = 'confirmed' AND te.pnl_usd IS NOT NULL AND te.created_at >= tw.cur_start)::bigint AS current_settled_7d,
    COUNT(*) FILTER (WHERE te.status = 'confirmed' AND te.pnl_usd > 0 AND te.created_at >= tw.cur_start)::bigint AS current_wins_7d,
    COUNT(*) FILTER (WHERE te.status = 'confirmed' AND te.pnl_usd IS NOT NULL AND te.created_at >= tw.prev_start AND te.created_at < tw.cur_start)::bigint AS prev_settled_7d,
    COUNT(*) FILTER (WHERE te.status = 'confirmed' AND te.pnl_usd > 0 AND te.created_at >= tw.prev_start AND te.created_at < tw.cur_start)::bigint AS prev_wins_7d
  FROM trade_executions te
  CROSS JOIN time_windows tw
),
winrate_values AS (
  SELECT
    CASE
      WHEN ws.current_settled_7d > 0
      THEN (ws.current_wins_7d::double precision / ws.current_settled_7d::double precision) * 100.0
      ELSE 0.0
    END AS current_win_rate,
    CASE
      WHEN ws.prev_settled_7d > 0
      THEN (ws.prev_wins_7d::double precision / ws.prev_settled_7d::double precision) * 100.0
      ELSE 0.0
    END AS prev_win_rate
  FROM winrate_stats ws
)
SELECT
    bs.total_ai_agents,
    CASE
      WHEN bs.prev_agents_7d = 0 AND bs.current_agents_7d = 0 THEN 0.0
      WHEN bs.prev_agents_7d = 0 THEN 100.0
      ELSE ((bs.current_agents_7d - bs.prev_agents_7d)::double precision / bs.prev_agents_7d::double precision) * 100.0
    END AS total_ai_agents_change_pct,
    0::bigint AS total_strategies,
    0.0::double precision AS total_strategies_change_pct,
    cs.cumulative_pnl,
    CASE
      WHEN cs.prev_pnl_7d = 0.0 AND cs.current_pnl_7d = 0.0 THEN 0.0
      WHEN cs.prev_pnl_7d = 0.0 THEN 100.0
      ELSE ((cs.current_pnl_7d - cs.prev_pnl_7d) / ABS(cs.prev_pnl_7d)) * 100.0
    END AS cumulative_pnl_change_pct,
    wv.current_win_rate AS avg_win_rate,
    CASE
      WHEN wv.prev_win_rate = 0.0 AND wv.current_win_rate = 0.0 THEN 0.0
      WHEN wv.prev_win_rate = 0.0 THEN 100.0
      ELSE ((wv.current_win_rate - wv.prev_win_rate) / ABS(wv.prev_win_rate)) * 100.0
    END AS avg_win_rate_change_pct
FROM bot_stats bs
CROSS JOIN confirmed_stats cs
CROSS JOIN winrate_values wv
"#,
                &[],
            )
            .await
            .context("Failed to query dashboard summary")?;

        let summary = DashboardSummary {
            total_ai_agents: row.get(0),
            total_ai_agents_change_pct: row.get(1),
            total_strategies: row.get(2),
            total_strategies_change_pct: row.get(3),
            cumulative_pnl: row.get(4),
            cumulative_pnl_change_pct: row.get(5),
            avg_win_rate: row.get(6),
            avg_win_rate_change_pct: row.get(7),
        };

        let mut cache = self.dashboard_summary_cache.lock().await;
        *cache = Some(DashboardSummaryCache {
            value: summary.clone(),
            fetched_at: Instant::now(),
        });

        Ok(summary)
    }

    /// Increase follower credits for a bot after confirmed settlement
    pub async fn award_credits(
        &self,
        bot_pubkey: &str,
        follower_pubkey: &str,
        delta: f64,
    ) -> Result<()> {
        let client = self.pool.get().await.context("Failed to get PG client")?;
        client
            .execute(
                "INSERT INTO credits AS c (bot_pubkey, follower_pubkey, credits)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (bot_pubkey, follower_pubkey)
                 DO UPDATE SET credits = c.credits + EXCLUDED.credits, updated_at = now()",
                &[&bot_pubkey, &follower_pubkey, &delta],
            )
            .await
            .context("Failed to award credits")?;
        Ok(())
    }

    pub async fn record_signal(&self, signal: SignalInsert) -> Result<()> {
        let client = self.pool.get().await.context("Failed to get PG client")?;

        client
            .execute(
                "INSERT INTO signals (
                    event_id,
                    kind,
                    bot_pubkey,
                    leader_pubkey,
                    follower_pubkey,
                    agent_eth_address,
                    role,
                    symbol,
                    strategy,
                    side,
                    size,
                    price,
                    status,
                    tx_hash,
                    pnl,
                    pnl_usd,
                    raw_content,
                    event_created_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
                )
                ON CONFLICT (event_id) DO NOTHING",
                &[
                    &signal.event_id,
                    &(signal.kind as i32),
                    &signal.bot_pubkey,
                    &signal.leader_pubkey,
                    &signal.follower_pubkey,
                    &signal.agent_eth_address,
                    &signal.role,
                    &signal.symbol,
                    &signal.strategy,
                    &signal.side,
                    &signal.size,
                    &signal.price,
                    &signal.status,
                    &signal.tx_hash,
                    &signal.pnl,
                    &signal.pnl_usd,
                    &signal.raw_content,
                    &signal.event_created_at,
                ],
            )
            .await
            .context("Failed to record signal event")?;

        Ok(())
    }

    /// List trending strategies aggregated from trade executions in given period.
    pub async fn list_trending_strategies(
        &self,
        period_days: Option<i64>,
        limit: i64,
        offset: i64,
        rank_by: StrategyRankBy,
        sort_order: SortOrder,
        category: Option<StrategyCategory>,
    ) -> Result<Vec<StrategyTrendingItem>> {
        let since = match period_days {
            Some(days) => {
                let since_ts = Utc::now().timestamp() - days * 24 * 3600;
                Utc.timestamp_opt(since_ts, 0)
                    .single()
                    .unwrap_or_else(Utc::now)
            }
            None => Utc.timestamp_opt(0, 0).single().unwrap_or_else(Utc::now),
        };

        let order_metric_sql = match rank_by {
            StrategyRankBy::Pnl => "COALESCE(ss.pnl_period, 0)",
            StrategyRankBy::Followers => "COALESCE(sf.followers, 0)",
            StrategyRankBy::Roi => "CASE WHEN COALESCE(ss.volume, 0) > 0 THEN (COALESCE(ss.pnl_period, 0) / ss.volume) * 100.0 ELSE 0.0 END",
        };
        let order_dir_sql = match sort_order {
            SortOrder::Asc => "ASC",
            SortOrder::Desc => "DESC",
        };
        let category_filter = category.as_ref().map(StrategyCategory::as_str);

        let sql = format!(
            r#"
WITH default_strategies(strategy, default_order) AS (
  VALUES
    ('momentum', 1),
    ('mean_reversion', 2),
    ('grid', 3),
    ('trend_following', 4)
),
scoped AS (
  SELECT bot_pubkey, strategy, symbol, side, size, price, status, pnl_usd, created_at
  FROM trade_executions
  WHERE created_at >= $1
    AND strategy IS NOT NULL
    AND strategy <> ''
    AND strategy <> 'test'
),
strategy_pool AS (
  SELECT ds.strategy, ds.default_order
  FROM default_strategies ds
  UNION
  SELECT DISTINCT s.strategy, NULL::int AS default_order
  FROM scoped s
  WHERE s.strategy NOT IN (SELECT strategy FROM default_strategies)
),
categorized_pool AS (
  SELECT
    sp.strategy,
    sp.default_order,
    CASE
      WHEN sp.strategy = 'grid' THEN 'grid'
      WHEN sp.strategy = 'momentum' THEN 'momentum'
      WHEN sp.strategy = 'mean_reversion' THEN 'dca'
      WHEN sp.strategy LIKE '%arbitrage%' THEN 'arbitrage'
      WHEN sp.strategy LIKE '%martingale%' THEN 'martingale'
      ELSE 'signal-based'
    END AS category
  FROM strategy_pool sp
),
strat_stats AS (
  SELECT strategy,
         COUNT(DISTINCT bot_pubkey)::bigint AS agents,
         COUNT(*) FILTER (WHERE side = 'buy')::bigint AS buy_count,
         COUNT(*) FILTER (WHERE side = 'sell')::bigint AS sell_count,
         COALESCE(SUM(size * price), 0)::double precision AS volume,
         COALESCE(SUM(pnl_usd) FILTER (WHERE status = 'confirmed'), 0)::double precision AS pnl_period,
         COUNT(*) FILTER (WHERE status = 'confirmed' AND pnl_usd IS NOT NULL)::bigint AS settled_count,
         COUNT(*) FILTER (WHERE status = 'confirmed' AND pnl_usd > 0)::bigint AS win_count
  FROM scoped
  GROUP BY strategy
),
strat_bots AS (
  SELECT DISTINCT strategy, bot_pubkey
  FROM scoped
),
strat_followers AS (
  SELECT sb.strategy, COUNT(DISTINCT s.follower_pubkey)::bigint AS followers
  FROM strat_bots sb
  LEFT JOIN subscriptions s ON s.bot_pubkey = sb.bot_pubkey
  GROUP BY sb.strategy
),
strat_pairs AS (
  SELECT strategy, COUNT(DISTINCT symbol)::bigint AS trading_pairs
  FROM scoped
  GROUP BY strategy
),
strategy_curve AS (
  SELECT
    strategy,
    created_at,
    bot_pubkey,
    SUM(pnl_usd) OVER (
      PARTITION BY strategy
      ORDER BY created_at, bot_pubkey
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_pnl
  FROM scoped
  WHERE status = 'confirmed' AND pnl_usd IS NOT NULL
),
strategy_curve_with_peak AS (
  SELECT
    strategy,
    cumulative_pnl,
    MAX(cumulative_pnl) OVER (
      PARTITION BY strategy
      ORDER BY created_at, bot_pubkey
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_peak
  FROM strategy_curve
),
strat_drawdown AS (
  SELECT
    strategy,
    COALESCE(
      MAX(
        CASE
          WHEN running_peak > 0
          THEN ((running_peak - cumulative_pnl) / running_peak) * 100.0
          ELSE 0.0
        END
      ),
      0.0
    )::double precision AS max_drawdown
  FROM strategy_curve_with_peak
  GROUP BY strategy
)
SELECT cp.strategy,
       cp.category,
       COALESCE(ss.agents, 0)::bigint AS agents,
       COALESCE(sf.followers, 0)::bigint AS followers,
       0.0::double precision AS profit_share,
       COALESCE(ss.buy_count, 0)::bigint AS buy_count,
       COALESCE(ss.sell_count, 0)::bigint AS sell_count,
       COALESCE(ss.volume, 0)::double precision AS volume,
       COALESCE(ss.pnl_period, 0)::double precision AS pnl_period,
       CASE
         WHEN COALESCE(ss.volume, 0) > 0
         THEN (COALESCE(ss.pnl_period, 0) / ss.volume) * 100.0
         ELSE 0.0
       END AS roi,
       COALESCE(sd.max_drawdown, 0)::double precision AS max_drawdown,
       COALESCE(spa.trading_pairs, 0)::bigint AS trading_pairs,
       CASE
         WHEN COALESCE(ss.settled_count, 0) > 0
         THEN (ss.win_count::double precision / ss.settled_count::double precision) * 100.0
         ELSE NULL
       END AS win_rate
FROM categorized_pool cp
LEFT JOIN strat_stats ss ON ss.strategy = cp.strategy
LEFT JOIN strat_followers sf ON sf.strategy = cp.strategy
LEFT JOIN strat_pairs spa ON spa.strategy = cp.strategy
LEFT JOIN strat_drawdown sd ON sd.strategy = cp.strategy
WHERE ($4::text IS NULL OR cp.category = $4::text)
ORDER BY
  {order_metric_sql} {order_dir_sql} NULLS LAST,
  CASE WHEN cp.default_order IS NULL THEN 1 ELSE 0 END,
  cp.default_order,
  cp.strategy ASC
LIMIT $2 OFFSET $3
"#
        );

        let client = self.pool.get().await.context("Failed to get PG client")?;
        let rows = client
            .query(&sql, &[&since, &limit, &offset, &category_filter])
            .await
            .context("Failed to query trending strategies")?;

        Ok(rows
            .into_iter()
            .map(|row| StrategyTrendingItem {
                strategy: row.get(0),
                category: row.get(1),
                agents: row.get(2),
                followers: row.get(3),
                profit_share: row.get(4),
                buy_count: row.get(5),
                sell_count: row.get(6),
                volume: row.get(7),
                pnl_period: row.get(8),
                roi: row.get(9),
                max_drawdown: row.get(10),
                trading_pairs: row.get(11),
                win_rate: row.get(12),
            })
            .collect())
    }

    /// Strategy detail payload for strategy detail page.
    pub async fn get_strategy_detail(
        &self,
        strategy: &str,
        period_days: Option<i64>,
        activity_limit: i64,
    ) -> Result<StrategyDetail> {
        let since = match period_days {
            Some(days) => {
                let since_ts = Utc::now().timestamp() - days * 24 * 3600;
                Utc.timestamp_opt(since_ts, 0)
                    .single()
                    .unwrap_or_else(Utc::now)
            }
            None => Utc.timestamp_opt(0, 0).single().unwrap_or_else(Utc::now),
        };

        let client = self.pool.get().await.context("Failed to get PG client")?;

        let overview_row = client
            .query_one(
                r#"
WITH scoped AS (
  SELECT symbol, side, size, price, status, pnl_usd, created_at
  FROM trade_executions
  WHERE strategy = $1
    AND created_at >= $2
),
latest_trade AS (
  SELECT price
  FROM trade_executions
  WHERE strategy = $1
  ORDER BY created_at DESC
  LIMIT 1
)
SELECT
  COUNT(DISTINCT symbol)::bigint AS trading_pairs,
  COALESCE(SUM(pnl_usd) FILTER (WHERE status = 'confirmed'), 0)::double precision AS total_profit,
  COALESCE(SUM(size * price), 0)::double precision AS total_volume,
  COUNT(*) FILTER (WHERE status = 'confirmed' AND pnl_usd IS NOT NULL)::bigint AS settled_count,
  COUNT(*) FILTER (WHERE status = 'confirmed' AND pnl_usd > 0)::bigint AS win_count,
  COALESCE(
    EXTRACT(EPOCH FROM (now() - MIN(created_at))) / 86400.0,
    0
  )::double precision AS runtime_days,
  COALESCE((SELECT price FROM latest_trade), 0)::double precision AS latest_price,
  COUNT(*)::bigint AS trade_count
FROM scoped
"#,
                &[&strategy, &since],
            )
            .await
            .context("Failed to query strategy overview")?;

        let total_profit: f64 = overview_row.get(1);
        let total_volume: f64 = overview_row.get(2);
        let settled_count: i64 = overview_row.get(3);
        let win_count: i64 = overview_row.get(4);
        let runtime_days_raw: f64 = overview_row.get(5);
        let latest_price: f64 = overview_row.get(6);
        let trade_count: i64 = overview_row.get(7);

        let total_roi = if total_volume > 0.0 {
            (total_profit / total_volume) * 100.0
        } else {
            0.0
        };
        let win_rate = if settled_count > 0 {
            Some((win_count as f64 / settled_count as f64) * 100.0)
        } else {
            None
        };

        let trading_frequency = if runtime_days_raw <= 0.0 {
            "Low".to_string()
        } else {
            let trades_per_day = trade_count as f64 / runtime_days_raw.max(1.0);
            if trades_per_day < 3.0 {
                "Low".to_string()
            } else if trades_per_day < 10.0 {
                "Medium".to_string()
            } else {
                "High".to_string()
            }
        };

        let position_rows = client
            .query(
                r#"
WITH open_trades AS (
  SELECT symbol, side, size, price
  FROM trade_executions
  WHERE strategy = $1
    AND status IN ('pending', 'submitted')
),
latest_price AS (
  SELECT DISTINCT ON (symbol) symbol, price
  FROM trade_executions
  WHERE strategy = $1
  ORDER BY symbol, created_at DESC
)
SELECT
  ot.symbol,
  CASE
    WHEN SUM(CASE WHEN ot.side = 'buy' THEN ot.size ELSE -ot.size END) >= 0 THEN 'long'
    ELSE 'short'
  END AS side,
  ABS(SUM(CASE WHEN ot.side = 'buy' THEN ot.size ELSE -ot.size END))::double precision AS amount,
  COALESCE(SUM(ot.size * ot.price) / NULLIF(SUM(ot.size), 0), 0)::double precision AS entry_price,
  COALESCE(lp.price, 0)::double precision AS current_price
FROM open_trades ot
LEFT JOIN latest_price lp ON lp.symbol = ot.symbol
GROUP BY ot.symbol, lp.price
HAVING ABS(SUM(CASE WHEN ot.side = 'buy' THEN ot.size ELSE -ot.size END)) > 0
ORDER BY ot.symbol
"#,
                &[&strategy],
            )
            .await
            .context("Failed to query strategy current positions")?;

        let current_positions: Vec<StrategyPositionItem> = position_rows
            .into_iter()
            .map(|r| {
                let side: String = r.get(1);
                let amount: f64 = r.get(2);
                let entry_price: f64 = r.get(3);
                let current_price: f64 = r.get(4);
                let unrealized_pnl = if side == "long" {
                    (current_price - entry_price) * amount
                } else {
                    (entry_price - current_price) * amount
                };

                StrategyPositionItem {
                    asset: r.get(0),
                    side,
                    amount,
                    entry_price,
                    current_price,
                    unrealized_pnl,
                }
            })
            .collect();

        let activity_rows = client
            .query(
                "SELECT id, side, symbol, size, price, status, created_at
                 FROM trade_executions
                 WHERE strategy = $1
                 ORDER BY created_at DESC
                 LIMIT $2",
                &[&strategy, &activity_limit],
            )
            .await
            .context("Failed to query strategy activity logs")?;

        let activity_logs: Vec<StrategyActivityItem> = activity_rows
            .into_iter()
            .map(|r| StrategyActivityItem {
                id: r.get(0),
                action: r.get(1),
                asset: r.get(2),
                amount: r.get(3),
                price: r.get(4),
                status: r.get(5),
                created_at: r.get(6),
            })
            .collect();

        Ok(StrategyDetail {
            overview: StrategyOverview {
                strategy: strategy.to_string(),
                category: strategy_category_label(strategy).to_string(),
                trading_pairs: overview_row.get(0),
                profit_share: 0.0,
                total_roi,
                total_profit,
                total_assets: 0.0,
                win_rate,
                trading_frequency,
                latest_price,
                runtime_days: runtime_days_raw.floor() as i64,
            },
            current_positions,
            activity_logs,
        })
    }

    /// Time series for strategy performance chart.
    pub async fn get_strategy_performance(
        &self,
        strategy: &str,
        period_days: Option<i64>,
        metric: StrategyPerformanceMetric,
        interval: StrategyPerformanceInterval,
    ) -> Result<StrategyPerformanceSeries> {
        let since = match period_days {
            Some(days) => {
                let since_ts = Utc::now().timestamp() - days * 24 * 3600;
                Utc.timestamp_opt(since_ts, 0)
                    .single()
                    .unwrap_or_else(Utc::now)
            }
            None => Utc.timestamp_opt(0, 0).single().unwrap_or_else(Utc::now),
        };

        let value_expr = match metric {
            StrategyPerformanceMetric::Pnl => "COALESCE(cumulative_pnl, 0)::double precision",
            StrategyPerformanceMetric::Roi => "CASE WHEN COALESCE(cumulative_volume, 0) > 0 THEN (cumulative_pnl / cumulative_volume) * 100.0 ELSE 0.0 END",
        };

        let sql = format!(
            r#"
WITH scoped AS (
  SELECT created_at, pnl_usd, size, price, status
  FROM trade_executions
  WHERE strategy = $1
    AND created_at >= $2
),
bucketed AS (
  SELECT
    date_trunc('{bucket}', created_at) AS bucket_ts,
    COALESCE(SUM(pnl_usd) FILTER (WHERE status = 'confirmed'), 0)::double precision AS bucket_pnl,
    COALESCE(SUM(size * price), 0)::double precision AS bucket_volume
  FROM scoped
  GROUP BY 1
),
series AS (
  SELECT
    bucket_ts,
    SUM(bucket_pnl) OVER (ORDER BY bucket_ts) AS cumulative_pnl,
    SUM(bucket_volume) OVER (ORDER BY bucket_ts) AS cumulative_volume
  FROM bucketed
)
SELECT
  bucket_ts,
  {value_expr} AS value
FROM series
ORDER BY bucket_ts ASC
"#,
            bucket = interval.date_trunc_unit(),
            value_expr = value_expr
        );

        let client = self.pool.get().await.context("Failed to get PG client")?;
        let rows = client
            .query(&sql, &[&strategy, &since])
            .await
            .context("Failed to query strategy performance series")?;

        let points = rows
            .into_iter()
            .map(|row| StrategyPerformancePoint {
                ts: row.get(0),
                value: row.get(1),
            })
            .collect();

        Ok(StrategyPerformanceSeries {
            strategy: strategy.to_string(),
            metric: metric.as_str().to_string(),
            interval: interval.as_str().to_string(),
            points,
        })
    }

    /// List leaders with 30D stats for leaderboard. Sorted by pnl_30d descending.
    pub async fn list_leaderboard(
        &self,
        period_days: i64,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<LeaderboardItem>> {
        let since_ts = Utc::now().timestamp() - period_days * 24 * 3600;
        let since = Utc
            .timestamp_opt(since_ts, 0)
            .single()
            .unwrap_or_else(Utc::now);
        let client = self.pool.get().await.context("Failed to get PG client")?;
        let rows = client
            .query(
                r#"
WITH stats AS (
  SELECT bot_pubkey,
         COUNT(*) FILTER (WHERE side = 'buy') AS buy_count,
         COUNT(*) FILTER (WHERE side = 'sell') AS sell_count,
         COALESCE(SUM(size * price), 0) AS volume,
         COALESCE(SUM(pnl_usd) FILTER (WHERE status = 'confirmed'), 0) AS pnl_30d,
         COUNT(*) FILTER (WHERE status = 'confirmed' AND pnl_usd IS NOT NULL) AS settled_count,
         COUNT(*) FILTER (WHERE status = 'confirmed' AND pnl_usd > 0) AS win_count
  FROM trade_executions
  WHERE created_at >= $1
  GROUP BY bot_pubkey
)
SELECT b.bot_pubkey, b.name, b.eth_address,
       (SELECT COUNT(*)::bigint FROM subscriptions s WHERE s.bot_pubkey = b.bot_pubkey) AS followers,
       COALESCE(st.buy_count, 0)::bigint, COALESCE(st.sell_count, 0)::bigint,
       COALESCE(st.volume, 0), COALESCE(st.pnl_30d, 0),
       CASE WHEN COALESCE(st.settled_count, 0) > 0
            THEN (st.win_count::float / st.settled_count * 100.0)
            ELSE NULL END AS win_rate
FROM bots b
LEFT JOIN stats st ON st.bot_pubkey = b.bot_pubkey
ORDER BY COALESCE(st.pnl_30d, 0) DESC NULLS LAST
LIMIT $2 OFFSET $3
"#,
                &[&since, &limit, &offset],
            )
            .await
            .context("Failed to query leaderboard")?;

        Ok(rows
            .into_iter()
            .map(|row| LeaderboardItem {
                bot_pubkey: row.get(0),
                name: row.get(1),
                eth_address: row.get(2),
                followers: row.get::<_, i64>(3),
                buy_count: row.get::<_, i64>(4),
                sell_count: row.get::<_, i64>(5),
                volume: row.get::<_, f64>(6),
                pnl_30d: row.get::<_, f64>(7),
                win_rate: row.get(8),
            })
            .collect())
    }

    /// Get one leader's detail: 7D stats, distribution, per-symbol holdings (from trades), recent trades.
    pub async fn get_leader_detail(
        &self,
        bot_pubkey_or_eth: &str,
        trades_limit: i64,
    ) -> Result<Option<LeaderDetail>> {
        let client = self.pool.get().await.context("Failed to get PG client")?;
        let bot = client
            .query_opt(
                "SELECT bot_pubkey, name, eth_address FROM bots WHERE bot_pubkey = $1 OR eth_address = $1",
                &[&bot_pubkey_or_eth],
            )
            .await
            .context("Failed to query bot")?;
        let row = match bot {
            Some(r) => r,
            None => return Ok(None),
        };
        let bot_pubkey: String = row.get(0);
        let name: String = row.get(1);
        let eth_address: String = row.get(2);

        let since_7d_ts = Utc::now().timestamp() - 7 * 24 * 3600;
        let since_7d = Utc
            .timestamp_opt(since_7d_ts, 0)
            .single()
            .unwrap_or_else(Utc::now);

        let stats_row = client
            .query_opt(
                r#"
SELECT
  COALESCE(SUM(pnl_usd) FILTER (WHERE status = 'confirmed'), 0),
  COUNT(*) FILTER (WHERE side = 'buy'),
  COUNT(*) FILTER (WHERE side = 'sell'),
  COALESCE(SUM(size * price), 0),
  COUNT(*) FILTER (WHERE status = 'confirmed' AND pnl_usd IS NOT NULL),
  COUNT(*) FILTER (WHERE status = 'confirmed' AND pnl_usd > 0),
  COUNT(*) FILTER (WHERE status = 'confirmed' AND pnl_usd <= 0),
  COUNT(DISTINCT symbol)
FROM trade_executions
WHERE bot_pubkey = $1 AND created_at >= $2
"#,
                &[&bot_pubkey, &since_7d],
            )
            .await
            .context("Failed to query leader 7d stats")?;

        let (realized_pnl_7d, buy_count_7d, sell_count_7d, volume_7d, settled_count, success_count_7d, failure_count_7d, token_count_7d) =
            match stats_row {
                Some(r) => (
                    r.get::<_, f64>(0),
                    r.get::<_, i64>(1),
                    r.get::<_, i64>(2),
                    r.get::<_, f64>(3),
                    r.get::<_, i64>(4),
                    r.get::<_, i64>(5),
                    r.get::<_, i64>(6),
                    r.get::<_, i64>(7),
                ),
                None => (0.0, 0_i64, 0_i64, 0.0, 0_i64, 0_i64, 0_i64, 0_i64),
            };

        let win_rate_7d = if settled_count > 0 {
            Some((success_count_7d as f64 / settled_count as f64) * 100.0)
        } else {
            None
        };

        let total_pnl_row = client
            .query_opt(
                "SELECT COALESCE(SUM(pnl_usd), 0) FROM trade_executions WHERE bot_pubkey = $1 AND status = 'confirmed'",
                &[&bot_pubkey],
            )
            .await
            .context("Failed to query total pnl")?;
        let total_pnl: f64 = total_pnl_row
            .map(|r| r.get::<_, f64>(0))
            .unwrap_or(0.0);

        let holdings_rows = client
            .query(
                r#"
SELECT symbol,
       COALESCE(SUM(pnl_usd) FILTER (WHERE status = 'confirmed'), 0),
       COUNT(*)::bigint
FROM trade_executions
WHERE bot_pubkey = $1 AND created_at >= $2
GROUP BY symbol
ORDER BY 2 DESC NULLS LAST
LIMIT 50
"#,
                &[&bot_pubkey, &since_7d],
            )
            .await
            .context("Failed to query holdings by symbol")?;
        let holdings: Vec<LeaderHolding> = holdings_rows
            .into_iter()
            .map(|r| LeaderHolding {
                symbol: r.get(0),
                realized_pnl: r.get(1),
                trade_count: r.get(2),
            })
            .collect();

        let trades_rows = client
            .query(
                "SELECT id, symbol, side, size, price, pnl_usd, status, created_at
                 FROM trade_executions
                 WHERE bot_pubkey = $1
                 ORDER BY created_at DESC
                 LIMIT $2",
                &[&bot_pubkey, &trades_limit],
            )
            .await
            .context("Failed to query leader trades")?;
        let trades: Vec<LeaderTradeRow> = trades_rows
            .into_iter()
            .map(|r| LeaderTradeRow {
                id: r.get(0),
                symbol: r.get(1),
                side: r.get(2),
                size: r.get(3),
                price: r.get(4),
                pnl_usd: r.get(5),
                status: r.get(6),
                created_at: r.get(7),
            })
            .collect();

        Ok(Some(LeaderDetail {
            bot_pubkey: bot_pubkey.clone(),
            name,
            eth_address,
            realized_pnl_7d,
            total_pnl,
            unrealized_pnl: None,
            win_rate_7d,
            buy_count_7d,
            sell_count_7d,
            volume_7d,
            avg_duration_7d_secs: None,
            success_count_7d,
            failure_count_7d,
            token_count_7d,
            holdings,
            trades,
        }))
    }

    async fn invalidate_dashboard_summary_cache(&self) {
        let mut cache = self.dashboard_summary_cache.lock().await;
        *cache = None;
    }
}

fn row_to_bot_record(row: Row) -> BotRecord {
    BotRecord {
        bot_pubkey: row.get(0),
        nostr_pubkey: row.get(1),
        eth_address: row.get(2),
    }
}

/// Encrypt a payload using a shared secret derived key (ChaCha20-Poly1305)
fn encrypt_with_secret(content: &str, shared_secret: &str) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(shared_secret.as_bytes());
    let key_bytes = hasher.finalize();
    let key = Key::from_slice(&key_bytes[..32]);
    let cipher = ChaCha20Poly1305::new(key);

    let mut nonce_bytes = [0u8; 12];
    let mut rng = rng();
    rng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, content.as_bytes())
        .map_err(|_| anyhow!("Failed to encrypt content"))?;

    let mut combined = Vec::with_capacity(nonce_bytes.len() + ciphertext.len());
    combined.extend_from_slice(&nonce_bytes);
    combined.extend_from_slice(&ciphertext);

    Ok(BASE64.encode(combined))
}
