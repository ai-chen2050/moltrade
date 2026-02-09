use anyhow::Result;
use flume::{Receiver, Sender};
use nostr_sdk::Event;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, error, info};

use crate::api::metrics::Metrics;
use crate::core::dedupe_engine::DeduplicationEngine;
use crate::core::subscription::{FanoutMessage, SubscriptionService};
use nostr_sdk::Kind;
use nostr_sdk::nips::nip04;
use nostr_sdk::prelude::{Client, EventBuilder, Keys, PublicKey, Tag, Timestamp};
use serde_json::Value;
use std::str::FromStr;

const KIND_TRADE_SIGNAL: u16 = 30931;
const KIND_COPYTRADE_INTENT: u16 = 30932;
const KIND_HEARTBEAT: u16 = 30933;
const KIND_EXECUTION_REPORT: u16 = 30934;
const KIND_AGENT_REGISTER: u16 = 30935;
const STALE_AFTER: Duration = Duration::from_secs(10 * 60);

/// Wrapper for Event to enable sorting by timestamp
#[derive(Clone)]
struct EventWrapper {
    event: Event,
    timestamp: u64,
}

impl PartialEq for EventWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp
    }
}

impl Eq for EventWrapper {}

impl PartialOrd for EventWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EventWrapper {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}

/// Event router that sorts events by timestamp and routes to downstream systems
pub struct EventRouter {
    dedupe_engine: Arc<DeduplicationEngine>,
    batch_size: usize,
    max_latency: Duration,
    downstream_tx: Sender<Event>,
    allowed_kinds: Option<Vec<u16>>,
    fanout_tx: Option<Sender<FanoutMessage>>,
    subscription_service: Option<Arc<SubscriptionService>>,
    nostr_keys: Option<Keys>,
    nostr_client: Option<Arc<Client>>,
    pending_events: Arc<RwLock<Vec<EventWrapper>>>,
    heartbeat_seen: Option<Arc<RwLock<HashMap<String, Instant>>>>,
    metrics: Option<Arc<Metrics>>,
}

impl EventRouter {
    /// Create a new event router
    pub fn new(
        dedupe_engine: Arc<DeduplicationEngine>,
        batch_size: usize,
        max_latency: Duration,
        downstream_tx: Sender<Event>,
        allowed_kinds: Option<Vec<u16>>,
        fanout_tx: Option<Sender<FanoutMessage>>,
        subscription_service: Option<Arc<SubscriptionService>>,
        nostr_keys: Option<Keys>,
        nostr_client: Option<Arc<Client>>,
    ) -> Self {
        let heartbeat_seen = subscription_service
            .as_ref()
            .map(|_| Arc::new(RwLock::new(HashMap::new())));

        Self {
            dedupe_engine,
            batch_size,
            max_latency,
            downstream_tx,
            allowed_kinds,
            fanout_tx,
            subscription_service,
            nostr_keys,
            nostr_client,
            pending_events: Arc::new(RwLock::new(Vec::new())),
            heartbeat_seen,
            metrics: None,
        }
    }

    /// Attach metrics collection
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Process incoming event stream, deduplicate, and route to downstream
    pub async fn process_stream(self, input: Receiver<Event>) -> Result<()> {
        let mut last_flush = Instant::now();

        loop {
            // Use timeout to periodically flush even if no new events arrive
            let timeout = tokio::time::sleep(self.max_latency);
            tokio::pin!(timeout);

            tokio::select! {
                // Receive new event
                result = input.recv_async() => {
                    match result {
                        Ok(event) => {
                            // Kind filtering (drop events not in allowlist if configured)
                            if let Some(allowed) = &self.allowed_kinds {
                                if !allowed.contains(&event.kind.as_u16()) {
                                    continue;
                                }
                            }
                            // Deduplication check
                            if !self.dedupe_engine.is_duplicate(&event).await {
                                // Add to pending events (will be sorted before flushing)
                                let timestamp = event.created_at.as_secs();
                                let wrapper = EventWrapper {
                                    event,
                                    timestamp,
                                };

                                let mut pending = self.pending_events.write().await;
                                pending.push(wrapper);
                                if let Some(m) = &self.metrics {
                                    m.events_in_queue.set(pending.len() as f64);
                                }

                                // If we have enough events, flush a batch
                                if pending.len() >= self.batch_size {
                                    drop(pending);
                                    self.flush_batch().await?;
                                    last_flush = Instant::now();
                                }
                            }
                        }
                        Err(_) => {
                            info!("Event stream closed, flushing remaining events");
                            self.flush_all().await?;
                            break;
                        }
                    }
                }
                // Timeout - flush if we have events and enough time has passed
                _ = timeout => {
                    let pending = self.pending_events.read().await;
                    if !pending.is_empty() && last_flush.elapsed() >= self.max_latency {
                        drop(pending);
                        let start = Instant::now();
                        self.flush_batch().await?;
                        if let Some(m) = &self.metrics {
                            let elapsed = start.elapsed().as_secs_f64();
                            m.processing_latency.observe(elapsed);
                        }
                        last_flush = Instant::now();
                    }
                }
            }
        }

        Ok(())
    }

    /// Flush a batch of events sorted by timestamp
    async fn flush_batch(&self) -> Result<()> {
        let mut pending = self.pending_events.write().await;
        let batch_size = self.batch_size.min(pending.len());

        if batch_size == 0 {
            return Ok(());
        }

        // Sort by timestamp (ascending - oldest first)
        pending.sort();

        // Take the oldest events (first batch_size events)
        let batch: Vec<Event> = pending
            .drain(0..batch_size)
            .map(|wrapper| wrapper.event)
            .collect();

        drop(pending);

        // Send events to downstream in timestamp order
        for event in batch {
            if self.is_stale(&event) {
                debug!(
                    "Skip stale event id={} kind={} age_secs={}",
                    event.id.to_hex(),
                    event.kind.as_u16(),
                    Timestamp::now()
                        .as_secs()
                        .saturating_sub(event.created_at.as_secs())
                );
                continue;
            }
            self.maybe_update_last_seen(&event).await;
            if let Err(e) = self.handle_copytrade_fanout(&event).await {
                error!("Fanout processing failed: {}", e);
            }
            if let Err(e) = self.downstream_tx.send_async(event).await {
                error!("Failed to send event to downstream: {}", e);
            }
            if let Some(m) = &self.metrics {
                m.events_processed.inc();
            }
        }

        debug!("Flushed batch of {} events", batch_size);
        if let Some(m) = &self.metrics {
            let remaining = self.pending_events.read().await.len();
            m.events_in_queue.set(remaining as f64);
        }
        Ok(())
    }

    /// Flush all remaining events
    async fn flush_all(&self) -> Result<()> {
        let mut pending = self.pending_events.write().await;
        let count = pending.len();

        // Sort by timestamp before flushing
        pending.sort();

        let events: Vec<Event> = pending.drain(..).map(|wrapper| wrapper.event).collect();

        for event in events {
            if let Err(e) = self.downstream_tx.send_async(event).await {
                error!("Failed to send event to downstream: {}", e);
            }
            if let Some(m) = &self.metrics {
                m.events_processed.inc();
            }
        }

        info!("Flushed all remaining {} events", count);
        if let Some(m) = &self.metrics {
            m.events_in_queue.set(0.0);
        }
        Ok(())
    }

    async fn handle_copytrade_fanout(&self, event: &Event) -> Result<()> {
        // Short-circuit only heartbeats: execution reports must be processed for DB writes
        if event.kind.as_u16() == KIND_HEARTBEAT {
            return Ok(());
        }

        // Agent registration is plaintext and upserts the bot record
        if event.kind.as_u16() == KIND_AGENT_REGISTER {
            let subs = match &self.subscription_service {
                Some(s) => s,
                None => return Ok(()),
            };

            let parsed: Value = match serde_json::from_str(&event.content) {
                Ok(v) => v,
                Err(e) => {
                    error!(
                        "Agent register decode failed for {}: {}",
                        event.id.to_hex(),
                        e
                    );
                    return Ok(());
                }
            };

            let nostr_pubkey = parsed
                .get("nostr_pubkey")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| event.pubkey.to_hex());
            let bot_pubkey = parsed
                .get("bot_pubkey")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| event.pubkey.to_hex());
            let eth_address = parsed
                .get("eth_address")
                .or_else(|| parsed.get("account"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let name = parsed
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("agent")
                .to_string();

            if eth_address.is_empty() {
                error!("Agent register missing eth_address for {}", bot_pubkey);
                return Ok(());
            }

            if let Err(e) = subs
                .register_bot(&bot_pubkey, &nostr_pubkey, &eth_address, &name)
                .await
            {
                error!("Agent register upsert failed for {}: {}", bot_pubkey, e);
            } else {
                info!(
                    "Registered bot via nostr: bot_pubkey={} eth={}",
                    bot_pubkey, eth_address
                );
            }

            return Ok(());
        }

        // Preconditions: need subscription service and platform nostr keys
        let subs = match &self.subscription_service {
            Some(s) => s,
            None => return Ok(()),
        };
        let nostr_keys = match &self.nostr_keys {
            Some(k) => k,
            None => return Ok(()),
        };

        // Skip decrypting events we just published (self-sent fanout echoes)
        if event.pubkey == nostr_keys.public_key() {
            debug!("Skip self-published fanout event {}", event.id.to_hex());
            return Ok(());
        }

        // Decrypt content using platform key and sender pubkey
        let plaintext = match nip04::decrypt(nostr_keys.secret_key(), &event.pubkey, &event.content)
        {
            Ok(p) => p,
            Err(e) => {
                error!("Failed to decrypt event {}: {}", event.id.to_hex(), e);
                return Ok(());
            }
        };

        let preview = if plaintext.len() > 256 {
            format!("{}...", &plaintext[..256])
        } else {
            plaintext.clone()
        };
        debug!(
            "Decrypted nostr event id={} kind={} from={} preview={}",
            event.id.to_hex(),
            event.kind.as_u16(),
            event.pubkey.to_hex(),
            preview,
        );

        // Extract agent eth address from JSON payload
        let agent_eth = extract_agent_eth(&plaintext)
            .ok_or_else(|| anyhow::anyhow!("agent eth address missing"))?;

        // Find leader bot by eth address
        let bot = match subs.find_bot_by_eth(&agent_eth).await? {
            Some(b) => b,
            None => {
                error!("No bot registered for eth address {}", agent_eth);
                return Ok(());
            }
        };

        // Followers for this bot

        // Persist trade tx info if present in payload
        self.maybe_record_trade(subs, &bot.bot_pubkey, &plaintext)
            .await;
        let followers = subs.list_subscriptions(&bot.bot_pubkey).await?;
        if followers.is_empty() {
            return Ok(());
        }

        // Fanout over WebSocket (plaintext)
        if let Some(fanout_tx) = &self.fanout_tx {
            for follower in &followers {
                let msg = FanoutMessage {
                    target_pubkey: follower.follower_pubkey.clone(),
                    bot_pubkey: bot.bot_pubkey.clone(),
                    kind: event.kind.as_u16(),
                    original_event_id: event.id.to_hex(),
                    payload: plaintext.clone(),
                };
                if let Err(e) = fanout_tx.send_async(msg).await {
                    error!("Failed to send fanout ws payload: {}", e);
                }
            }
        }

        // Publish encrypted nostr events to followers if client exists
        if let Some(client) = &self.nostr_client {
            for follower in followers {
                let follower_pk_str = follower.shared_secret.as_str();
                let follower_pk = match PublicKey::from_str(follower_pk_str) {
                    Ok(pk) => pk,
                    Err(e) => {
                        error!(
                            "Invalid follower shared_secret pubkey {}: {}",
                            follower_pk_str, e
                        );
                        continue;
                    }
                };

                let encrypted =
                    match nip04::encrypt(nostr_keys.secret_key(), &follower_pk, &plaintext) {
                        Ok(ct) => ct,
                        Err(e) => {
                            error!("Encrypt for follower {} failed: {}", follower_pk_str, e);
                            continue;
                        }
                    };

                let mut builder = EventBuilder::new(Kind::Custom(event.kind.as_u16()), encrypted);
                builder = builder.tag(Tag::public_key(follower_pk));

                if let Err(e) = client.send_event_builder(builder).await {
                    error!("Publish to follower {} failed: {}", follower_pk_str, e);
                }
            }
        }

        Ok(())
    }
}

impl EventRouter {
    fn is_stale(&self, event: &Event) -> bool {
        let now = Timestamp::now().as_secs();
        let created = event.created_at.as_secs();
        now.saturating_sub(created) > STALE_AFTER.as_secs()
    }

    async fn maybe_update_last_seen(&self, event: &Event) {
        const HEARTBEAT_KIND: u16 = KIND_HEARTBEAT;
        const MIN_INTERVAL: Duration = Duration::from_secs(15 * 60);

        if event.kind.as_u16() != HEARTBEAT_KIND {
            return;
        }

        let subs = match &self.subscription_service {
            Some(s) => s,
            None => return,
        };

        let cache = match &self.heartbeat_seen {
            Some(c) => c,
            None => return,
        };

        let bot_pubkey = event.pubkey.to_hex();
        let now = Instant::now();

        let should_update = {
            let mut guard = cache.write().await;
            match guard.get(&bot_pubkey) {
                Some(last) if now.duration_since(*last) < MIN_INTERVAL => false,
                _ => {
                    guard.insert(bot_pubkey.clone(), now);
                    true
                }
            }
        };

        if should_update {
            if let Err(e) = subs.update_bot_last_seen(&bot_pubkey).await {
                error!("Failed to update last_seen for bot {}: {}", bot_pubkey, e);
            }
        }
    }
}

fn extract_agent_eth(plaintext: &str) -> Option<String> {
    let parsed: serde_json::Value = serde_json::from_str(plaintext).ok()?;
    parsed
        .get("agent_eth_address")
        .or_else(|| parsed.get("agent"))
        .or_else(|| parsed.get("account"))
        .or_else(|| parsed.get("eth_address"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

#[derive(Debug)]
struct TradeMeta {
    tx_hash: String,
    symbol: String,
    side: String,
    size: f64,
    price: f64,
    status: Option<String>,
    pnl: Option<f64>,
    pnl_usd: Option<f64>,
    follower_pubkey: Option<String>,
    role: String,
}

impl EventRouter {
    async fn maybe_record_trade(
        &self,
        subs: &SubscriptionService,
        bot_pubkey: &str,
        plaintext: &str,
    ) {
        let meta = match extract_trade_meta(plaintext) {
            Some(m) => m,
            None => return,
        };

        if let Err(e) = subs
            .record_trade_tx(
                bot_pubkey,
                meta.follower_pubkey.as_deref(),
                &meta.role,
                &meta.symbol,
                &meta.side,
                meta.size,
                meta.price,
                &meta.tx_hash,
            )
            .await
        {
            error!("Failed to record trade tx {}: {}", meta.tx_hash, e);
        }

        if meta.status.is_some() || meta.pnl.is_some() || meta.pnl_usd.is_some() {
            if let Err(e) = subs
                .update_trade_settlement(
                    &meta.tx_hash,
                    meta.status.as_deref().unwrap_or("pending"),
                    meta.pnl,
                    meta.pnl_usd,
                )
                .await
            {
                error!("Failed to update settlement for {}: {}", meta.tx_hash, e);
            }
        }
    }
}

fn extract_trade_meta(plaintext: &str) -> Option<TradeMeta> {
    let parsed: Value = serde_json::from_str(plaintext).ok()?;
    let tx_hash = parsed.get("tx_hash")?.as_str()?.to_string();
    let symbol = parsed.get("symbol")?.as_str()?.to_string();
    let side = parsed.get("side")?.as_str()?.to_string();
    let size = parsed.get("size")?.as_f64()?;
    let price = parsed.get("price")?.as_f64()?;

    let status = parsed
        .get("status")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let pnl = parsed.get("pnl").and_then(|v| v.as_f64());
    let pnl_usd = parsed.get("pnl_usd").and_then(|v| v.as_f64());
    let follower_pubkey = parsed
        .get("follower_pubkey")
        .or_else(|| parsed.get("follower"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let role = parsed
        .get("role")
        .and_then(|v| v.as_str())
        .unwrap_or("leader")
        .to_string();

    Some(TradeMeta {
        tx_hash,
        symbol,
        side,
        size,
        price,
        status,
        pnl,
        pnl_usd,
        follower_pubkey,
        role,
    })
}
