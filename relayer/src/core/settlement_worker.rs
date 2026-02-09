use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use reqwest::StatusCode;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::config::SettlementCreditConfig;
use crate::core::subscription::SubscriptionService;

#[derive(Clone, Debug)]
pub struct SettlementWorker {
    svc: Arc<SubscriptionService>,
    client: reqwest::Client,
    base_url: String,
    interval: Duration,
    batch_limit: i64,
    credit_cfg: Option<SettlementCreditConfig>,
}

impl SettlementWorker {
    pub fn new(
        svc: Arc<SubscriptionService>,
        base_url: String,
        interval: Duration,
        batch_limit: i64,
        credit_cfg: Option<SettlementCreditConfig>,
    ) -> Self {
        Self {
            svc,
            client: reqwest::Client::new(),
            base_url,
            interval,
            batch_limit,
            credit_cfg,
        }
    }

    pub async fn run(self) {
        loop {
            if let Err(e) = self.tick().await {
                warn!("settlement tick failed: {}", e);
            }
            sleep(self.interval).await;
        }
    }

    async fn tick(&self) -> Result<()> {
        let trades = self.svc.list_pending_trades(self.batch_limit).await?;

        if trades.is_empty() {
            debug!("settlement: no pending trades");
            return Ok(());
        }

        for t in trades {
            match self.verify_tx(&t.tx_hash).await {
                Ok(Some(true)) => {
                    self.svc
                        .update_trade_settlement(&t.tx_hash, "confirmed", None, None)
                        .await?;
                    if let Some(credit) = self.compute_credit(&t) {
                        let recipient = t.follower_pubkey.as_deref().unwrap_or(&t.bot_pubkey);
                        self.svc
                            .award_credits(&t.bot_pubkey, recipient, credit)
                            .await?;
                    }
                    info!("settlement: confirmed {}", t.tx_hash);
                }
                Ok(Some(false)) => {
                    self.svc
                        .update_trade_settlement(&t.tx_hash, "failed", None, None)
                        .await?;
                    warn!("settlement: marked failed {}", t.tx_hash);
                }
                Ok(None) => {
                    debug!("settlement: tx {} not yet found", t.tx_hash);
                }
                Err(e) => {
                    error!("settlement: verify {} error: {}", t.tx_hash, e);
                }
            }
        }

        Ok(())
    }

    /// Naive verifier: HTTP GET the explorer endpoint; 200 -> confirmed, 404 -> unknown
    async fn verify_tx(&self, tx_hash: &str) -> Result<Option<bool>> {
        let url = format!("{}/{}", self.base_url.trim_end_matches('/'), tx_hash);
        let resp = self.client.get(&url).send().await?;
        match resp.status() {
            StatusCode::OK => Ok(Some(true)),
            StatusCode::NOT_FOUND => Ok(None),
            s if s.is_client_error() || s.is_server_error() => Ok(Some(false)),
            _ => Ok(None),
        }
    }

    fn compute_credit(&self, trade: &crate::core::subscription::PendingTrade) -> Option<f64> {
        let cfg = match self.credit_cfg.as_ref() {
            Some(c) if c.enable => c,
            _ => return None,
        };

        let rate = if trade.role == "leader" {
            cfg.leader_rate
        } else {
            cfg.follower_rate
        };

        let mut credit = (trade.size * trade.price * rate).max(cfg.min_credit);
        if let Some(pnl) = trade.pnl_usd {
            if pnl > 0.0 {
                credit *= cfg.profit_multiplier;
            }
        }

        if credit.is_finite() && credit > 0.0 {
            Some(credit)
        } else {
            None
        }
    }
}
