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
            // Early-phase behavior: award credits as soon as we have an oid (tx hash may be absent for Hyperliquid).
            // If tx_hash is present we still attempt verification; otherwise we short-circuit to credit award.
            match self.verify_tx_opt(t.tx_hash.as_deref()).await {
                Ok(Some(true)) => {
                    self.svc
                        .update_trade_settlement(
                            t.tx_hash.as_deref(),
                            t.oid.as_deref(),
                            "confirmed",
                            None,
                            None,
                        )
                        .await?;
                    if let Some(credit) = self.compute_credit(&t) {
                        let recipient = t.follower_pubkey.as_deref().unwrap_or(&t.bot_pubkey);
                        if let Err(e) = self
                            .svc
                            .award_credits(&t.bot_pubkey, recipient, credit)
                            .await
                        {
                            log_award_error(&e, &t.bot_pubkey, recipient);
                            continue;
                        }
                    }
                    info!(
                        "settlement: confirmed tx_hash={:?} oid={:?}",
                        t.tx_hash, t.oid
                    );
                }
                Ok(Some(false)) => {
                    self.svc
                        .update_trade_settlement(
                            t.tx_hash.as_deref(),
                            t.oid.as_deref(),
                            "failed",
                            None,
                            None,
                        )
                        .await?;
                    warn!(
                        "settlement: marked failed tx_hash={:?} oid={:?}",
                        t.tx_hash, t.oid
                    );
                }
                Ok(None) => {
                    // If no tx hash, treat pending entry as immediately credit-eligible.
                    if t.tx_hash.is_none() {
                        if let Some(credit) = self.compute_credit(&t) {
                            let recipient = t.follower_pubkey.as_deref().unwrap_or(&t.bot_pubkey);
                            if let Err(e) = self
                                .svc
                                .award_credits(&t.bot_pubkey, recipient, credit)
                                .await
                            {
                                log_award_error(&e, &t.bot_pubkey, recipient);
                                continue;
                            }
                        }
                        self.svc
                            .update_trade_settlement(
                                t.tx_hash.as_deref(),
                                t.oid.as_deref(),
                                "confirmed",
                                None,
                                None,
                            )
                            .await?;
                        info!("settlement: credited pending trade with oid={:?}", t.oid);
                    } else {
                        debug!("settlement: tx {:?} not yet found", t.tx_hash);
                    }
                }
                Err(e) => {
                    error!(
                        "settlement: verify tx_hash={:?} oid={:?} error: {}",
                        t.tx_hash, t.oid, e
                    );
                }
            }
        }

        Ok(())
    }

    /// Naive verifier: HTTP GET the explorer endpoint; 200 -> confirmed, 404 -> unknown
    async fn verify_tx_opt(&self, tx_hash: Option<&str>) -> Result<Option<bool>> {
        let tx = match tx_hash {
            Some(v) if !v.is_empty() => v,
            _ => return Ok(None),
        };
        let url = format!("{}/{}", self.base_url.trim_end_matches('/'), tx);
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

        let base_rate = if trade.role == "leader" {
            cfg.leader_rate
        } else {
            cfg.follower_rate
        };

        let mut credit = (trade.size * trade.price * base_rate).max(cfg.min_credit);
        if let Some(pnl) = trade.pnl_usd {
            if pnl > 0.0 {
                credit *= cfg.profit_multiplier;
            }
        }

        if trade.is_test {
            credit *= cfg.test_multiplier;
        }

        if credit.is_finite() && credit > 0.0 {
            Some(credit)
        } else {
            None
        }
    }
}

fn log_award_error(err: &anyhow::Error, bot_pubkey: &str, follower: &str) {
    if let Some(db_err) = err.downcast_ref::<tokio_postgres::Error>() {
        if let Some(code) = db_err.code() {
            warn!(
                "award_credits failed (pg code={:?}): bot={} follower={} err={:?}",
                code, bot_pubkey, follower, db_err
            );
            return;
        }
    }

    warn!(
        "award_credits failed: bot={} follower={} err={:?}",
        bot_pubkey, follower, err
    );
}
