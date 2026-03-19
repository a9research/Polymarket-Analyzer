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

    /// `page_limit`: `limit` query param and offset increment (must be consistent).
    pub async fn trades_all(&self, wallet: &str, page_limit: u32) -> anyhow::Result<TradesAllResult> {
        let mut out = Vec::new();
        let mut offset: u32 = 0;
        // Data API: limit max 10000 per OpenAPI.
        let limit: u32 = page_limit.clamp(1, 10_000);
        let mut max_offset_allowed: Option<u32> = None;

        loop {
            if let Some(max) = max_offset_allowed {
                if offset > max {
                    break;
                }
            }

            let page = self
                .trades_page(wallet, limit, offset)
                .await
                .with_context(|| format!("fetch trades page offset={offset} limit={limit}"));

            let page = match page {
                Ok(p) => p,
                Err(e) => {
                    // `e.to_string()` may only show the outer `with_context` message; the API body
                    // lives on inner causes. Join the full chain so we can detect offset limits.
                    let msg = anyhow_error_chain_text(&e);
                    if let Some(max_off) = parse_max_offset_exceeded(&msg) {
                        tracing::warn!(
                            "data-api returned max offset exceeded; stopping further pages. wallet={} offset={} max_off={}",
                            wallet,
                            offset,
                            max_off
                        );
                        max_offset_allowed = Some(max_off);
                        // The failing offset is already beyond allowed max, so stop.
                        break;
                    }
                    return Err(e);
                }
            };

            if page.is_empty() {
                break;
            }
            offset = offset.saturating_add(limit);
            out.extend(page);
            tokio::time::sleep(self.rate_limit).await;
        }

        Ok(TradesAllResult {
            trades: out,
            truncated: max_offset_allowed.is_some(),
            max_offset_allowed,
        })
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

#[derive(Debug, Clone)]
pub struct TradesAllResult {
    pub trades: Vec<Trade>,
    pub truncated: bool,
    pub max_offset_allowed: Option<u32>,
}

fn anyhow_error_chain_text(e: &anyhow::Error) -> String {
    e.chain()
        .map(|c| c.to_string())
        .collect::<Vec<_>>()
        .join(" ")
}

fn parse_max_offset_exceeded(msg: &str) -> Option<u32> {
    // Example body: {"error":"max historical activity offset of 3000 exceeded"}
    let marker = "max historical activity offset of ";
    let Some(start) = msg.find(marker) else {
        return None;
    };
    let rest = &msg[start + marker.len()..];
    let digits: String = rest
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse::<u32>().ok()
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

