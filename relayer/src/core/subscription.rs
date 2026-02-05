use anyhow::{Context, Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use chrono::Utc;
use deadpool_postgres::{Config as PgConfig, Pool, Runtime};
use nostr_sdk::prelude::{Client, EventBuilder, Keys};
use nostr_sdk::{Event, Kind};
use rand::RngCore;
use rand::rng;
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;
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
    pub tx_hash: String,
    pub bot_pubkey: String,
    pub follower_pubkey: Option<String>,
    pub role: String,
    pub size: f64,
    pub price: f64,
    pub pnl_usd: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct CreditBalance {
    pub bot_pubkey: String,
    pub follower_pubkey: String,
    pub credits: f64,
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

        let svc = Self { pool };
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
                CREATE TABLE IF NOT EXISTS credits (
                    bot_pubkey TEXT NOT NULL REFERENCES bots(bot_pubkey) ON DELETE CASCADE,
                    follower_pubkey TEXT NOT NULL,
                    credits NUMERIC NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (bot_pubkey, follower_pubkey)
                );",
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
        side: &str,
        size: f64,
        price: f64,
        tx_hash: &str,
    ) -> Result<()> {
        let client = self.pool.get().await.context("Failed to get PG client")?;
        client
            .execute(
                "INSERT INTO trade_executions (bot_pubkey, follower_pubkey, role, symbol, side, size, price, tx_hash)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                 ON CONFLICT (tx_hash) DO NOTHING",
                &[&bot_pubkey, &follower_pubkey, &role, &symbol, &side, &size, &price, &tx_hash],
            )
            .await
            .context("Failed to record trade tx")?;
        Ok(())
    }

    /// Update trade settlement/PnL once the chain confirms
    pub async fn update_trade_settlement(
        &self,
        tx_hash: &str,
        status: &str,
        pnl: Option<f64>,
        pnl_usd: Option<f64>,
    ) -> Result<()> {
        let client = self.pool.get().await.context("Failed to get PG client")?;
        client
            .execute(
                "UPDATE trade_executions
                 SET status = $2,
                     pnl = COALESCE($3, pnl),
                     pnl_usd = COALESCE($4, pnl_usd),
                     updated_at = now()
                 WHERE tx_hash = $1",
                &[&tx_hash, &status, &pnl, &pnl_usd],
            )
            .await
            .context("Failed to update trade settlement")?;
        Ok(())
    }

    pub async fn list_pending_trades(&self, limit: i64) -> Result<Vec<PendingTrade>> {
        let client = self.pool.get().await.context("Failed to get PG client")?;
        let rows = client
            .query(
                "SELECT tx_hash, bot_pubkey, follower_pubkey, role, size, price, pnl_usd
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
                bot_pubkey: row.get(1),
                follower_pubkey: row.get(2),
                role: row.get(3),
                size: row.get(4),
                price: row.get(5),
                pnl_usd: row.get(6),
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
                "INSERT INTO credits (bot_pubkey, follower_pubkey, credits)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (bot_pubkey, follower_pubkey)
                 DO UPDATE SET credits = credits + EXCLUDED.credits, updated_at = now()",
                &[&bot_pubkey, &follower_pubkey, &delta],
            )
            .await
            .context("Failed to award credits")?;
        Ok(())
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
