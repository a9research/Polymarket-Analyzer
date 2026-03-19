use anyhow::Context;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct GammaApiClient {
    http: Client,
    base_url: String,
    rate_limit: Duration,
}

impl GammaApiClient {
    pub fn new(http: Client, rate_limit: Duration) -> Self {
        Self {
            http,
            base_url: "https://gamma-api.polymarket.com".to_string(),
            rate_limit,
        }
    }

    pub async fn market_by_slug(&self, slug: &str) -> anyhow::Result<Market> {
        let url = format!("{}/markets/slug/{}", self.base_url, slug);
        let resp = self
            .http
            .get(url)
            .send()
            .await?
            .error_for_status()
            .with_context(|| format!("fetch gamma market by slug={slug}"))?;

        tokio::time::sleep(self.rate_limit).await;
        Ok(resp.json::<Market>().await?)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Market {
    pub id: Option<String>,
    pub slug: Option<String>,
    pub question: Option<String>,

    /// ISO 8601 date-time strings per Gamma schema.
    pub end_date: Option<String>,
    pub closed_time: Option<String>,
    pub closed: Option<bool>,
    pub resolved_by: Option<String>,
}

