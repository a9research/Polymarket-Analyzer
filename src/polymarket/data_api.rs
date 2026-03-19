use anyhow::Context;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct DataApiClient {
    http: Client,
    base_url: String,
    rate_limit: Duration,
}

impl DataApiClient {
    pub fn new(http: Client, rate_limit: Duration) -> Self {
        Self {
            http,
            base_url: "https://data-api.polymarket.com".to_string(),
            rate_limit,
        }
    }

    pub async fn trades_all(&self, wallet: &str) -> anyhow::Result<Vec<Trade>> {
        let mut out = Vec::new();
        let mut offset: u32 = 0;
        let limit: u32 = 500;

        loop {
            let page = self
                .trades_page(wallet, limit, offset)
                .await
                .with_context(|| format!("fetch trades page offset={offset} limit={limit}"))?;
            if page.is_empty() {
                break;
            }
            offset = offset.saturating_add(limit);
            out.extend(page);
            tokio::time::sleep(self.rate_limit).await;
        }

        Ok(out)
    }

    pub async fn trades_page(
        &self,
        wallet: &str,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<Trade>> {
        const MAX_ATTEMPTS: u32 = 3;
        let url = format!("{}/trades", self.base_url);
        let mut last_err: Option<String> = None;

        for attempt in 1..=MAX_ATTEMPTS {
            let result = self
                .http
                .get(&url)
                .query(&[
                    ("user", wallet),
                    ("limit", &limit.to_string()),
                    ("offset", &offset.to_string()),
                ])
                .send()
                .await;

            let resp = match result {
                Ok(r) => r,
                Err(e) => {
                    let is_retryable = e.is_timeout() || e.is_connect();
                    last_err = Some(e.to_string());
                    if is_retryable && attempt < MAX_ATTEMPTS {
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        continue;
                    }
                    anyhow::bail!(
                        "request data-api /trades failed wallet={wallet} offset={offset} attempt={attempt}: {e}"
                    );
                }
            };

            let status = resp.status();
            if !status.is_success() {
                let body = resp
                    .text()
                    .await
                    .unwrap_or_else(|_| "<failed to read error body>".to_string());
                anyhow::bail!(
                    "data-api /trades non-success status={} wallet={} offset={} limit={} body={}",
                    status,
                    wallet,
                    offset,
                    limit,
                    body
                );
            }

            let trades = resp.json::<Vec<Trade>>().await?;
            return Ok(trades);
        }

        anyhow::bail!(
            "request data-api /trades failed after {} attempts wallet={wallet} offset={offset}: {}",
            MAX_ATTEMPTS,
            last_err.unwrap_or_else(|| "unknown".to_string())
        );
    }
}

/// Data API `/trades` schema (subset we need).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Trade {
    pub proxy_wallet: Option<String>,
    pub side: TradeSide,
    pub asset: Option<String>,
    pub condition_id: String,
    pub size: f64,
    pub price: f64,
    pub timestamp: i64,
    pub title: Option<String>,
    pub slug: String,
    pub event_slug: Option<String>,
    pub outcome: Option<String>,
    pub outcome_index: Option<i64>,
    pub transaction_hash: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradeSide {
    Buy,
    Sell,
}

