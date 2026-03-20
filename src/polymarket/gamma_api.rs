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

    /// `GET /public-profile?address=` — display name / avatar when profile exists.
    pub async fn public_profile_by_address(&self, address: &str) -> anyhow::Result<PublicProfile> {
        let url = format!("{}/public-profile", self.base_url);
        let resp = self
            .http
            .get(url)
            .query(&[("address", address)])
            .send()
            .await?;

        tokio::time::sleep(self.rate_limit).await;

        if !resp.status().is_success() {
            anyhow::bail!("gamma public-profile status={}", resp.status());
        }
        Ok(resp.json::<PublicProfile>().await?)
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
    #[serde(default)]
    pub creator_username: Option<String>,
    #[serde(default)]
    pub image: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct PublicProfile {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub pseudonym: Option<String>,
    #[serde(default)]
    pub profile_image: Option<String>,
    #[serde(default)]
    pub x_username: Option<String>,
}

