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
use nostr_sdk::prelude::{Client, EventBuilder, Keys, PublicKey, Tag};
use serde_json;
use std::str::FromStr;

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
        // Preconditions: need subscription service and platform nostr keys
        let subs = match &self.subscription_service {
            Some(s) => s,
            None => return Ok(()),
        };
        let nostr_keys = match &self.nostr_keys {
            Some(k) => k,
            None => return Ok(()),
        };

        // Decrypt content using platform key and sender pubkey
        let plaintext = match nip04::decrypt(nostr_keys.secret_key(), &event.pubkey, &event.content)
        {
            Ok(p) => p,
            Err(e) => {
                error!("Failed to decrypt event {}: {}", event.id.to_hex(), e);
                return Ok(());
            }
        };

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
                let follower_pk = match PublicKey::from_str(&follower.follower_pubkey) {
                    Ok(pk) => pk,
                    Err(e) => {
                        error!(
                            "Invalid follower pubkey {}: {}",
                            follower.follower_pubkey, e
                        );
                        continue;
                    }
                };

                let encrypted =
                    match nip04::encrypt(nostr_keys.secret_key(), &follower_pk, &plaintext) {
                        Ok(ct) => ct,
                        Err(e) => {
                            error!(
                                "Encrypt for follower {} failed: {}",
                                follower.follower_pubkey, e
                            );
                            continue;
                        }
                    };

                let mut builder = EventBuilder::new(Kind::Custom(event.kind.as_u16()), encrypted);
                builder = builder.tag(Tag::public_key(follower_pk));

                if let Err(e) = client.send_event_builder(builder).await {
                    error!(
                        "Publish to follower {} failed: {}",
                        follower.follower_pubkey, e
                    );
                }
            }
        }

        Ok(())
    }
}

impl EventRouter {
    async fn maybe_update_last_seen(&self, event: &Event) {
        const HEARTBEAT_KIND: u16 = 30934;
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
        .or_else(|| parsed.get("eth_address"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}
