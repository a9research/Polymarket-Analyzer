//! Gamma **`/tags`** + **`/sports`** → tag id 展示名与「体育类 tag → sport 键」映射，供 `market_distribution` 与官网类目对齐。
//!
//! 市场维度：优先用 `GET /markets/slug/{slug}?include_tag=true` 返回的 **`category`**，否则用 **`tags`**（先匹配 `/sports` 中的 tag id → `sports:{sport}`，再用全量 tag 目录的 label/slug）。

use super::gamma_api::{GammaApiClient, GammaTag, Market};
use anyhow::Context;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// 进程内缓存（多钱包 analyze 复用，减少 `/tags` 全量拉取）。
static TAXONOMY_CACHE: std::sync::OnceLock<Mutex<Option<(Instant, u64, Arc<GammaTaxonomy>)>>> =
    std::sync::OnceLock::new();

#[derive(Debug, Clone, Default)]
pub struct GammaTaxonomy {
    /// Tag id → 展示用短名（优先 `label`，否则 `slug`）。
    tags_by_id: HashMap<String, String>,
    /// `/sports` 里 `tags` 字段拆出的 id → `sport` 缩写。
    tag_id_to_sport: HashMap<String, String>,
    /// 所有落在任一项运动下的 tag id（用于判断「是否体育」而不强依赖 sport 字符串）。
    sport_tag_ids: HashSet<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct SportsMetadataRow {
    #[serde(default)]
    sport: Option<String>,
    /// 文档：comma-separated tag IDs
    #[serde(default)]
    tags: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct TagRow {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    slug: Option<String>,
}

impl GammaTaxonomy {
    pub fn empty() -> Self {
        Self::default()
    }

    /// 拉取 `/tags`（分页）与 `/sports`，失败时返回空映射（由调用方回退 slug 规则）。
    pub async fn fetch(client: &GammaApiClient) -> anyhow::Result<Self> {
        let mut tags_by_id = HashMap::new();
        let mut tag_id_to_sport: HashMap<String, String> = HashMap::new();
        let mut sport_tag_ids = HashSet::new();

        let mut offset: u32 = 0;
        const PAGE: u32 = 500;
        loop {
            let url = format!(
                "{}/tags?limit={}&offset={}",
                client.base_url(),
                PAGE,
                offset
            );
            let arr: Vec<TagRow> = client
                .get_json(&url)
                .await
                .with_context(|| format!("gamma GET /tags offset={offset}"))?;
            let n = arr.len();
            for row in arr {
                let Some(id) = row.id.filter(|s| !s.trim().is_empty()) else {
                    continue;
                };
                let disp = row
                    .label
                    .filter(|s| !s.trim().is_empty())
                    .or(row.slug.clone())
                    .unwrap_or_else(|| id.clone());
                tags_by_id.insert(id, disp);
            }
            if n == 0 {
                break;
            }
            offset += n as u32;
            if n < PAGE as usize {
                break;
            }
        }

        let sports_url = format!("{}/sports", client.base_url());
        let sports: Vec<SportsMetadataRow> = client
            .get_json(&sports_url)
            .await
            .context("gamma GET /sports")?;

        for row in sports {
            let sport_key = normalize_bucket_key(
                row.sport
                    .as_deref()
                    .unwrap_or("sports")
                    .trim(),
            );
            let Some(tags_csv) = row.tags.as_deref() else {
                continue;
            };
            for raw in tags_csv.split(',') {
                let tid = raw.trim();
                if tid.is_empty() {
                    continue;
                }
                sport_tag_ids.insert(tid.to_string());
                tag_id_to_sport
                    .entry(tid.to_string())
                    .or_insert_with(|| sport_key.clone());
            }
        }

        Ok(GammaTaxonomy {
            tags_by_id,
            tag_id_to_sport,
            sport_tag_ids,
        })
    }

    /// 带 TTL 的进程级缓存；`ttl_sec == 0` 时每次全量拉取。
    pub async fn cached(client: &GammaApiClient, ttl_sec: u64) -> Arc<Self> {
        if ttl_sec == 0 {
            return Arc::new(
                Self::fetch(client)
                    .await
                    .unwrap_or_else(|e| {
                        tracing::warn!("gamma taxonomy fetch (no cache): {e:#}");
                        Self::empty()
                    }),
            );
        }
        let cell = TAXONOMY_CACHE.get_or_init(|| Mutex::new(None));
        let mut g = cell.lock().await;
        let now = Instant::now();
        if let Some((t, cached_ttl, arc)) = g.as_ref() {
            if *cached_ttl == ttl_sec && now.duration_since(*t) < Duration::from_secs(ttl_sec.max(60)) {
                return arc.clone();
            }
        }
        let arc = Arc::new(
            Self::fetch(client)
                .await
                .unwrap_or_else(|e| {
                    tracing::warn!("gamma taxonomy fetch: {e:#}");
                    Self::empty()
                }),
        );
        *g = Some((now, ttl_sec, arc.clone()));
        arc
    }

    /// 由 Gamma `Market`（建议 `include_tag=true`）得到 `market_distribution` 桶名。
    pub fn bucket_for_market(&self, m: &Market) -> Option<String> {
        if let Some(c) = m.category.as_deref() {
            let c = c.trim();
            if !c.is_empty() {
                return Some(normalize_bucket_key(c));
            }
        }
        let tags: &[GammaTag] = match m.tags.as_deref() {
            Some(t) if !t.is_empty() => t,
            _ => return None,
        };

        for t in tags {
            let id = t.id.as_deref().unwrap_or("").trim();
            if id.is_empty() {
                continue;
            }
            if self.sport_tag_ids.contains(id) {
                let sport = self
                    .tag_id_to_sport
                    .get(id)
                    .map(String::as_str)
                    .unwrap_or("sports");
                return Some(format!("sports:{sport}"));
            }
        }

        for t in tags {
            let id = t.id.as_deref().unwrap_or("").trim();
            if id.is_empty() {
                continue;
            }
            if let Some(label) = self.tags_by_id.get(id) {
                return Some(normalize_bucket_key(label));
            }
        }

        for t in tags {
            if let Some(s) = t.slug.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
                return Some(normalize_bucket_key(s));
            }
        }
        None
    }
}

fn normalize_bucket_key(s: &str) -> String {
    let s = s.trim().to_lowercase();
    s.chars()
        .map(|c| {
            if c.is_whitespace() || c == '_' {
                '-'
            } else {
                c
            }
        })
        .collect()
}
