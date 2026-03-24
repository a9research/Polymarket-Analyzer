use anyhow::Context;
use reqwest::Client;
use serde::de::DeserializeOwned;
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

    pub fn base_url(&self) -> &str {
        self.base_url.as_str()
    }

    pub async fn get_json<T: DeserializeOwned>(&self, url: &str) -> anyhow::Result<T> {
        let resp = self
            .http
            .get(url)
            .send()
            .await?
            .error_for_status()
            .with_context(|| format!("gamma GET {url}"))?;
        tokio::time::sleep(self.rate_limit).await;
        Ok(resp.json::<T>().await?)
    }

    pub async fn market_by_slug(&self, slug: &str) -> anyhow::Result<Market> {
        self.market_by_slug_impl(slug, false).await
    }

    /// 与 [`Self::market_by_slug`] 相同，但带 **`include_tag=true`**，响应含 `category` / `tags`（Gamma OpenAPI）。
    pub async fn market_by_slug_include_tags(&self, slug: &str) -> anyhow::Result<Market> {
        self.market_by_slug_impl(slug, true).await
    }

    async fn market_by_slug_impl(&self, slug: &str, include_tag: bool) -> anyhow::Result<Market> {
        let url = format!("{}/markets/slug/{}", self.base_url, slug);
        let req = self.http.get(url);
        let req = if include_tag {
            req.query(&[("include_tag", "true")])
        } else {
            req
        };
        let resp = req
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

    /// Raw `GET /public-profile` for BFF passthrough (preserve 404 / body).
    pub async fn fetch_public_profile_response(
        &self,
        address: &str,
    ) -> reqwest::Result<reqwest::Response> {
        let url = format!("{}/public-profile", self.base_url);
        let resp = self
            .http
            .get(url)
            .query(&[("address", address)])
            .send()
            .await?;
        tokio::time::sleep(self.rate_limit).await;
        Ok(resp)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GammaTag {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub slug: Option<String>,
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
    /// Gamma 类目字符串（与官网一致方向；需 `include_tag=true` 时更完整）。
    #[serde(default)]
    pub category: Option<String>,
    /// 需 `GET .../markets/slug/{slug}?include_tag=true`。
    #[serde(default)]
    pub tags: Option<Vec<GammaTag>>,
    /// JSON array serialized as string, e.g. `["Yes","No"]`.
    #[serde(default)]
    pub outcomes: Option<String>,
    /// JSON array serialized as string; when resolved often `["1","0"]` (USDC per share).
    #[serde(default)]
    pub outcome_prices: Option<String>,
    /// JSON array of CLOB token ids aligned with `outcomes`.
    #[serde(default)]
    pub clob_token_ids: Option<String>,
    #[serde(default)]
    pub uma_resolution_status: Option<String>,
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
    /// ISO 8601 when the profile was created (Gamma schema).
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub bio: Option<String>,
    #[serde(default)]
    pub verified_badge: Option<bool>,
    #[serde(default)]
    pub proxy_wallet: Option<String>,
}

