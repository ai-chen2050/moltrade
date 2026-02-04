use anyhow::{Context, Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use deadpool_postgres::{Config as PgConfig, Pool, Runtime};
use nostr_sdk::Event;
use rand::RngCore;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio_postgres::NoTls;

/// Row shape for subscriptions
#[derive(Debug, Clone)]
pub struct SubscriptionRow {
    pub follower_pubkey: String,
    pub shared_secret: String,
}

/// Message ready for fanout to followers over WebSocket
#[derive(Debug, Clone, Serialize)]
pub struct FanoutMessage {
    pub target_pubkey: String,
    pub bot_pubkey: String,
    pub kind: u16,
    pub original_event_id: String,
    pub ciphertext: String,
}

/// Service managing Postgres-backed subscriptions and fanout encryption
#[derive(Clone)]
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
                    name TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                CREATE TABLE IF NOT EXISTS subscriptions (
                    id BIGSERIAL PRIMARY KEY,
                    bot_pubkey TEXT NOT NULL REFERENCES bots(bot_pubkey) ON DELETE CASCADE,
                    follower_pubkey TEXT NOT NULL,
                    shared_secret TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE(bot_pubkey, follower_pubkey)
                );",
            )
            .await
            .context("Failed to initialize subscription schema")?;
        Ok(())
    }

    /// Register or upsert a bot
    pub async fn register_bot(&self, bot_pubkey: &str, name: &str) -> Result<()> {
        let client = self.pool.get().await.context("Failed to get PG client")?;
        client
            .execute(
                "INSERT INTO bots (bot_pubkey, name) VALUES ($1, $2)
                 ON CONFLICT (bot_pubkey) DO UPDATE SET name = EXCLUDED.name",
                &[&bot_pubkey, &name],
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
                ciphertext,
            });
        }

        Ok(out)
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
    rand::thread_rng().fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, content.as_bytes())
        .map_err(|_| anyhow!("Failed to encrypt content"))?;

    let mut combined = Vec::with_capacity(nonce_bytes.len() + ciphertext.len());
    combined.extend_from_slice(&nonce_bytes);
    combined.extend_from_slice(&ciphertext);

    Ok(BASE64.encode(combined))
}
