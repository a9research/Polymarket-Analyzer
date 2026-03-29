use anyhow::Context;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
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
            fetched_incrementally: false,
            incremental_api_delta_count: 0,
        })
    }

    /// Fetch trades **strictly newer** than `watermark_ms_exclusive` using offset pagination.
    ///
    /// **Assumption:** `/trades` returns rows in **non-increasing timestamp order** (newest first),
    /// which matches Polymarket user activity in practice. If ordering differs, use full
    /// [`Self::trades_all`] (`data_api_incremental_trades = false`).
    pub async fn trades_since_watermark(
        &self,
        wallet: &str,
        page_limit: u32,
        watermark_ms_exclusive: i64,
        max_pages: u32,
    ) -> anyhow::Result<(Vec<Trade>, bool, Option<u32>)> {
        let limit: u32 = page_limit.clamp(1, 10_000);
        let mut offset: u32 = 0;
        let mut delta: Vec<Trade> = Vec::new();
        let mut pages: u32 = 0;
        let mut hit_global_offset_cap: Option<u32> = None;

        loop {
            if pages >= max_pages.max(1) {
                tracing::warn!(
                    wallet = wallet,
                    pages = pages,
                    "data-api incremental: stopped at max_pages cap; list may be incomplete"
                );
                break;
            }

            let page = self
                .trades_page(wallet, limit, offset)
                .await
                .with_context(|| format!("incremental trades_page offset={offset}"))?;

            pages += 1;
            if page.is_empty() {
                break;
            }

            let mut reached_old = false;
            for t in page {
                let ms = trade_timestamp_ms(t.timestamp);
                if ms <= watermark_ms_exclusive {
                    reached_old = true;
                    break;
                }
                delta.push(t);
            }

            if reached_old {
                break;
            }

            offset = offset.saturating_add(limit);
            if offset > 10_000 {
                hit_global_offset_cap = Some(10_000);
                tracing::warn!(
                    wallet = wallet,
                    "data-api incremental: hit offset>10000 while still receiving only newer-than-watermark rows; data may be incomplete"
                );
                break;
            }
        }

        let truncated = hit_global_offset_cap.is_some() || pages >= max_pages.max(1);
        Ok((delta, truncated, hit_global_offset_cap))
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

    /// Open positions (`GET /positions`). Paginates with `limit`/`offset` when supported.
    pub async fn positions_all(&self, wallet: &str, page_limit: u32) -> anyhow::Result<Vec<UserPosition>> {
        self.paginate_user_endpoint("/positions", wallet, page_limit).await
    }

    /// Settled / closed positions (`GET /closed-positions`).
    pub async fn closed_positions_all(
        &self,
        wallet: &str,
        page_limit: u32,
    ) -> anyhow::Result<Vec<ClosedPosition>> {
        self.paginate_user_endpoint("/closed-positions", wallet, page_limit)
            .await
    }

    /// Polymarket Data API caps `limit` per route; exceeding it still returns at most that many rows.
    /// `/closed-positions`: OpenAPI `maximum: 50` — must paginate with `limit <= 50` and advance `offset`
    /// by returned length (or `limit` on full pages), otherwise a request with `limit=500` returns 50 rows
    /// and callers incorrectly stop after the first page.
    fn data_api_max_limit_per_request(path: &str) -> u32 {
        match path {
            "/closed-positions" => 50,
            _ => 10_000,
        }
    }

    async fn paginate_user_endpoint<T: DeserializeOwned>(
        &self,
        path: &str,
        wallet: &str,
        page_limit: u32,
    ) -> anyhow::Result<Vec<T>> {
        let cap = Self::data_api_max_limit_per_request(path);
        let limit: u32 = page_limit.clamp(1, cap);
        let mut out = Vec::new();
        let mut offset: u32 = 0;
        loop {
            let page = self
                .user_json_page::<T>(path, wallet, limit, offset)
                .await
                .with_context(|| format!("fetch data-api {path} offset={offset} limit={limit}"))?;
            if page.is_empty() {
                break;
            }
            let n = page.len();
            out.extend(page);
            offset = offset.saturating_add(n as u32);
            if (n as u32) < limit {
                break;
            }
            tokio::time::sleep(self.rate_limit).await;
        }
        Ok(out)
    }

    /// `GET /activity` — user event stream (TRADE / MERGE / REDEEM / …). Optional `market` = condition id.
    /// `start` / `end`: Unix seconds filters per Polymarket Data API OpenAPI.
    pub async fn activity_page(
        &self,
        wallet: &str,
        market_condition_id: Option<&str>,
        limit: u32,
        offset: u32,
        start_ts_sec: Option<i64>,
        end_ts_sec: Option<i64>,
    ) -> anyhow::Result<Vec<Activity>> {
        let url = format!("{}/activity", self.base_url);
        let lim = limit.clamp(1, 500);
        let off = offset.min(10_000);
        let mut req = self.http.get(&url).query(&[
            ("user", wallet),
            ("limit", &lim.to_string()),
            ("offset", &off.to_string()),
        ]);
        if let Some(m) = market_condition_id.filter(|s| !s.trim().is_empty()) {
            req = req.query(&[("market", m)]);
        }
        if let Some(s) = start_ts_sec {
            req = req.query(&[("start", &s.to_string())]);
        }
        if let Some(e) = end_ts_sec {
            req = req.query(&[("end", &e.to_string())]);
        }
        let resp = req.send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp
                .text()
                .await
                .unwrap_or_else(|_| "<no body>".to_string());
            anyhow::bail!("data-api /activity status={status} body={body}");
        }
        Ok(resp.json::<Vec<Activity>>().await.unwrap_or_default())
    }

    /// Paginate `/activity` for one `market` (condition id) until empty or offset cap.
    pub async fn activity_all_for_market(
        &self,
        wallet: &str,
        market_condition_id: &str,
        page_limit: u32,
        start_ts_sec: Option<i64>,
    ) -> anyhow::Result<Vec<Activity>> {
        let mut out = Vec::new();
        let mut offset: u32 = 0;
        let limit = page_limit.clamp(1, 500);
        loop {
            let page = self
                .activity_page(
                    wallet,
                    Some(market_condition_id),
                    limit,
                    offset,
                    start_ts_sec,
                    None,
                )
                .await
                .with_context(|| format!("activity market={market_condition_id} offset={offset}"))?;
            if page.is_empty() {
                break;
            }
            let n = page.len();
            out.extend(page);
            if n < limit as usize {
                break;
            }
            offset = offset.saturating_add(limit);
            if offset > 10_000 {
                tracing::warn!(
                    wallet,
                    market = market_condition_id,
                    "data-api /activity offset>10000; stopping pagination"
                );
                break;
            }
            tokio::time::sleep(self.rate_limit).await;
        }
        Ok(out)
    }

    async fn user_json_page<T: DeserializeOwned>(
        &self,
        path: &str,
        wallet: &str,
        limit: u32,
        offset: u32,
    ) -> anyhow::Result<Vec<T>> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .http
            .get(&url)
            .query(&[
                ("user", wallet),
                ("limit", &limit.to_string()),
                ("offset", &offset.to_string()),
            ])
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp
                .text()
                .await
                .unwrap_or_else(|_| "<no body>".to_string());
            anyhow::bail!("data-api {path} status={status} body={body}");
        }

        let page = resp.json::<Vec<T>>().await.unwrap_or_default();
        Ok(page)
    }
}

#[derive(Debug, Clone)]
pub struct TradesAllResult {
    pub trades: Vec<Trade>,
    pub truncated: bool,
    pub max_offset_allowed: Option<u32>,
    /// True when merged from PG `wallet_primary_trade_row` + API delta (see `trades_since_watermark`).
    pub fetched_incrementally: bool,
    /// Count of trades newly fetched from API in incremental mode (before dedup merge).
    pub incremental_api_delta_count: usize,
}

/// Millisecond timestamp for ordering / watermarking (accepts sec or ms from API).
pub fn trade_timestamp_ms(ts: i64) -> i64 {
    if ts > 1_000_000_000_000 {
        ts
    } else {
        ts.saturating_mul(1000)
    }
}

/// Stable key for dedup across incremental merges and DB round-trips.
pub fn trade_dedup_key(t: &Trade) -> String {
    let side_s = match t.side {
        TradeSide::Buy => "BUY",
        TradeSide::Sell => "SELL",
    };
    if let Some(ref h) = t.transaction_hash {
        format!("tx:{h}")
    } else {
        format!(
            "synth:{}:{}:{}:{}:{}:{}",
            t.condition_id, t.slug, t.timestamp, side_s, t.size, t.price
        )
    }
}

/// Max `trade_timestamp_ms` in `prev`, or `None` if empty.
pub fn wallet_trades_watermark_ms(prev: &[Trade]) -> Option<i64> {
    prev.iter().map(|t| trade_timestamp_ms(t.timestamp)).max()
}

/// Append API `delta` onto `prev`, skipping duplicates (by [`trade_dedup_key`]).
pub fn merge_trades_incremental(prev: Vec<Trade>, delta: Vec<Trade>) -> Vec<Trade> {
    let mut seen: HashSet<String> = prev.iter().map(|t| trade_dedup_key(t)).collect();
    let mut out = prev;
    for t in delta {
        let k = trade_dedup_key(&t);
        if seen.insert(k) {
            out.push(t);
        }
    }
    out
}

fn anyhow_error_chain_text(e: &anyhow::Error) -> String {
    e.chain()
        .map(|c| c.to_string())
        .collect::<Vec<_>>()
        .join(" ")
}

/// Data API `GET /activity` row (subset; extra fields ignored).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Activity {
    #[serde(default)]
    pub proxy_wallet: Option<String>,
    #[serde(default)]
    pub timestamp: i64,
    #[serde(default)]
    pub condition_id: Option<String>,
    /// TRADE, SPLIT, MERGE, REDEEM, REWARD, CONVERSION, MAKER_REBATE
    #[serde(default, rename = "type")]
    pub event_type: Option<String>,
    #[serde(default)]
    pub size: Option<f64>,
    #[serde(default)]
    pub usdc_size: Option<f64>,
    #[serde(default)]
    pub transaction_hash: Option<String>,
    #[serde(default)]
    pub price: Option<f64>,
    #[serde(default)]
    pub asset: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub outcome_index: Option<i32>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub slug: Option<String>,
    #[serde(default)]
    pub event_slug: Option<String>,
    #[serde(default)]
    pub outcome: Option<String>,
}

/// Normalize activity `timestamp` to **Unix seconds** (API may return sec or ms).
pub fn activity_ts_sec(a: &Activity) -> i64 {
    let t = a.timestamp;
    if t > 1_000_000_000_000 {
        t / 1000
    } else {
        t
    }
}

pub fn activity_dedup_key(a: &Activity) -> String {
    if let Some(ref h) = a
        .transaction_hash
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        return format!("tx:{}", h.to_lowercase());
    }
    format!(
        "raw:{}:{}:{}:{}:{}",
        activity_ts_sec(a),
        a.event_type.as_deref().unwrap_or(""),
        a.size.map(|x| format!("{x:.10}")).unwrap_or_default(),
        a.price.map(|x| format!("{x:.10}")).unwrap_or_default(),
        a.side.as_deref().unwrap_or("")
    )
}

pub fn merge_activities_incremental(existing: Vec<Activity>, delta: Vec<Activity>) -> Vec<Activity> {
    let mut seen: std::collections::HashSet<String> =
        existing.iter().map(activity_dedup_key).collect();
    let mut out = existing;
    for a in delta {
        let k = activity_dedup_key(&a);
        if seen.insert(k) {
            out.push(a);
        }
    }
    out.sort_by_key(activity_ts_sec);
    out
}

pub fn max_activity_ts_sec(activities: &[Activity]) -> i64 {
    activities.iter().map(activity_ts_sec).max().unwrap_or(0)
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradeSide {
    Buy,
    Sell,
}

/// Data API `/positions` row (subset; extra fields ignored at JSON level via serde).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct UserPosition {
    #[serde(default)]
    pub proxy_wallet: Option<String>,
    #[serde(default)]
    pub asset: Option<String>,
    #[serde(default)]
    pub condition_id: Option<String>,
    #[serde(default)]
    pub slug: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default)]
    pub size: Option<f64>,
    #[serde(default)]
    pub avg_price: Option<f64>,
    #[serde(default)]
    pub cur_price: Option<f64>,
    #[serde(default)]
    pub initial_value: Option<f64>,
    #[serde(default)]
    pub current_value: Option<f64>,
    #[serde(default)]
    pub cash_pnl: Option<f64>,
    #[serde(default)]
    pub realized_pnl: Option<f64>,
    #[serde(default)]
    pub percent_pnl: Option<f64>,
}

/// Data API `/closed-positions` row (subset).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ClosedPosition {
    #[serde(default)]
    pub condition_id: Option<String>,
    #[serde(default)]
    pub slug: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default)]
    pub size: Option<f64>,
    #[serde(default)]
    pub avg_price: Option<f64>,
    #[serde(default)]
    pub realized_pnl: Option<f64>,
    #[serde(default)]
    pub total_bought: Option<f64>,
    #[serde(default)]
    pub end_date: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(ts: i64, tx: Option<&str>) -> Trade {
        Trade {
            proxy_wallet: None,
            side: TradeSide::Buy,
            asset: None,
            condition_id: "0xc1".to_string(),
            size: 1.0,
            price: 0.5,
            timestamp: ts,
            title: None,
            slug: "s".to_string(),
            event_slug: None,
            outcome: None,
            outcome_index: None,
            transaction_hash: tx.map(String::from),
        }
    }

    #[test]
    fn merge_incremental_dedupes_by_tx() {
        let prev = vec![mk(1000, Some("0xabc"))];
        let delta = vec![
            mk(1000, Some("0xabc")),
            mk(2000, Some("0xdef")),
        ];
        let m = merge_trades_incremental(prev, delta);
        assert_eq!(m.len(), 2);
    }
}

