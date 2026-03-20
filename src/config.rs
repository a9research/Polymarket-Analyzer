use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// How primary analytics blocks are computed (`build_report` input stream).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyticsConfig {
    /// `data_api` (default) or `canonical` — `canonical` uses merged + DATA_API_TRADE-like events after in-memory merge.
    #[serde(default = "default_analytics_source")]
    pub source: String,
    /// Emit `metrics_canonical_shadow` when canonical merge artifacts are available.
    #[serde(default = "default_true")]
    pub canonical_shadow: bool,
}

fn default_analytics_source() -> String {
    "data_api".to_string()
}

impl Default for AnalyticsConfig {
    fn default() -> Self {
        Self {
            source: default_analytics_source(),
            canonical_shadow: true,
        }
    }
}

/// Canonical merge + `markets_dim` enrichment (requires Postgres + `persist_raw`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_true")]
    pub enrich_markets_dim: bool,
}

impl Default for CanonicalConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            enrich_markets_dim: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub rate_limit_ms: u64,
    #[serde(default)]
    pub cache_ttl_sec: u64,
    #[serde(default)]
    pub database_url: Option<String>,
    /// HTTP request timeout in seconds (default 90; increase if API is slow or far away).
    #[serde(default)]
    pub timeout_sec: u64,

    /// Page size for `GET /trades` (also used as offset step). Data API allows up to 10000.
    #[serde(default = "default_trades_page_limit")]
    pub trades_page_limit: u32,

    #[serde(default)]
    pub market_type: MarketTypeConfig,

    #[serde(default)]
    pub subgraph: SubgraphConfig,

    #[serde(default)]
    pub reconciliation: ReconciliationConfig,

    #[serde(default)]
    pub ingestion: IngestionConfig,

    #[serde(default)]
    pub canonical: CanonicalConfig,

    #[serde(default)]
    pub analytics: AnalyticsConfig,
}

fn default_trades_page_limit() -> u32 {
    500
}

fn default_activity_subgraph_url() -> String {
    "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/activity-subgraph/0.0.4/gn"
        .to_string()
}

fn default_orderbook_subgraph_url() -> String {
    "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"
        .to_string()
}

fn default_pnl_subgraph_url() -> String {
    "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/pnl-subgraph/0.0.14/gn"
        .to_string()
}

fn default_subgraph_user_agent() -> String {
    "polymarket-account-analyzer/0.2 (subgraph)".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubgraphConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_activity_subgraph_url")]
    pub activity_url: String,
    #[serde(default = "default_orderbook_subgraph_url")]
    pub orderbook_url: String,
    #[serde(default = "default_pnl_subgraph_url")]
    pub pnl_url: String,
    #[serde(default = "default_subgraph_page_size")]
    pub page_size: u32,
    #[serde(default = "default_subgraph_max_pages")]
    pub max_pages: u32,
    #[serde(default = "default_positions_page_size")]
    pub positions_page_size: u32,
    #[serde(default)]
    pub skip_pnl_positions: bool,
    #[serde(default = "default_subgraph_timeout_sec")]
    pub timeout_sec: u64,
    #[serde(default = "default_subgraph_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_subgraph_pnl_max_retries")]
    pub pnl_max_retries: u32,
    #[serde(default)]
    pub cap_rows_per_stream: u32,
    #[serde(default = "default_true")]
    pub show_progress: bool,
    #[serde(default = "default_subgraph_user_agent")]
    pub user_agent: String,
    /// Request extra `orderFilledEvent` fields (tx hash, condition, size, price). Falls back to minimal if subgraph rejects.
    #[serde(default = "default_true")]
    pub extended_fill_fields: bool,
}

fn default_subgraph_page_size() -> u32 {
    1000
}
fn default_subgraph_max_pages() -> u32 {
    500
}
fn default_positions_page_size() -> u32 {
    100
}
fn default_subgraph_timeout_sec() -> u64 {
    600
}
fn default_subgraph_max_retries() -> u32 {
    5
}
fn default_subgraph_pnl_max_retries() -> u32 {
    8
}
fn default_true() -> bool {
    true
}

impl Default for SubgraphConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            activity_url: default_activity_subgraph_url(),
            orderbook_url: default_orderbook_subgraph_url(),
            pnl_url: default_pnl_subgraph_url(),
            page_size: default_subgraph_page_size(),
            max_pages: default_subgraph_max_pages(),
            positions_page_size: default_positions_page_size(),
            skip_pnl_positions: false,
            timeout_sec: default_subgraph_timeout_sec(),
            max_retries: default_subgraph_max_retries(),
            pnl_max_retries: default_subgraph_pnl_max_retries(),
            cap_rows_per_stream: 0,
            show_progress: true,
            user_agent: default_subgraph_user_agent(),
            extended_fill_fields: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconciliationConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Match trades to subgraph fills when timestamps within this many seconds.
    #[serde(default = "default_recon_window")]
    pub time_window_sec: i64,
    /// Relative size tolerance for v1 canonical matching (e.g. 0.05 = 5%).
    #[serde(default = "default_size_tolerance_pct")]
    pub size_tolerance_pct: f64,
    /// Absolute price tolerance for v1 matching on both sides.
    #[serde(default = "default_price_tolerance_abs")]
    pub price_tolerance_abs: f64,
    #[serde(default)]
    pub require_condition_match: bool,
    #[serde(default = "default_rules_version")]
    pub rules_version: String,
    /// If relative \|shadow_volume − primary_volume\| / max(primary, ε) ≥ this, append a quality note (when shadow runs).
    #[serde(default = "default_shadow_volume_alert_ratio")]
    pub shadow_volume_alert_ratio: f64,
    /// When `api_only / (matched + api_only) ≥` this and `api_only ≥ api_only_alert_min`, append coverage alert (v1).
    #[serde(default = "default_api_only_ratio_alert")]
    pub api_only_ratio_alert: f64,
    #[serde(default = "default_api_only_alert_min")]
    pub api_only_alert_min: usize,
}

fn default_recon_window() -> i64 {
    120
}

fn default_size_tolerance_pct() -> f64 {
    0.05
}

fn default_price_tolerance_abs() -> f64 {
    0.02
}

fn default_rules_version() -> String {
    "1".to_string()
}

fn default_shadow_volume_alert_ratio() -> f64 {
    0.30
}

fn default_api_only_ratio_alert() -> f64 {
    0.40
}

fn default_api_only_alert_min() -> usize {
    12
}

impl Default for ReconciliationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            time_window_sec: default_recon_window(),
            size_tolerance_pct: default_size_tolerance_pct(),
            price_tolerance_abs: default_price_tolerance_abs(),
            require_condition_match: false,
            rules_version: default_rules_version(),
            shadow_volume_alert_ratio: default_shadow_volume_alert_ratio(),
            api_only_ratio_alert: default_api_only_ratio_alert(),
            api_only_alert_min: default_api_only_alert_min(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionConfig {
    /// Persist raw chunks to Postgres when database is configured.
    #[serde(default = "default_true")]
    pub persist_raw: bool,
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            persist_raw: true,
        }
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            rate_limit_ms: 150,
            cache_ttl_sec: 600,
            database_url: None,
            timeout_sec: 90,
            trades_page_limit: default_trades_page_limit(),
            market_type: MarketTypeConfig::default(),
            subgraph: SubgraphConfig::default(),
            reconciliation: ReconciliationConfig::default(),
            ingestion: IngestionConfig::default(),
            canonical: CanonicalConfig::default(),
            analytics: AnalyticsConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketTypeConfig {
    #[serde(default)]
    pub default_type: String,

    /// Ordered rules; first match wins.
    #[serde(default)]
    pub rules: Vec<MarketTypeRule>,
}

impl Default for MarketTypeConfig {
    fn default() -> Self {
        Self {
            default_type: "unknown".to_string(),
            rules: vec![
                MarketTypeRule::Contains {
                    contains: "5-min".to_string(),
                    r#type: "5-min".to_string(),
                },
                MarketTypeRule::Contains {
                    contains: "1h".to_string(),
                    r#type: "1h".to_string(),
                },
                MarketTypeRule::Contains {
                    contains: "daily".to_string(),
                    r#type: "daily".to_string(),
                },
                MarketTypeRule::Contains {
                    contains: "politics".to_string(),
                    r#type: "politics".to_string(),
                },
                MarketTypeRule::Contains {
                    contains: "sports".to_string(),
                    r#type: "sports".to_string(),
                },
                MarketTypeRule::Contains {
                    contains: "crypto".to_string(),
                    r#type: "crypto".to_string(),
                },
            ],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MarketTypeRule {
    Contains { contains: String, r#type: String },
    Prefix { prefix: String, r#type: String },
}

impl MarketTypeConfig {
    pub fn classify_slug<'a>(&'a self, slug: &str) -> &'a str {
        for rule in &self.rules {
            match rule {
                MarketTypeRule::Contains { contains, r#type } if slug.contains(contains) => {
                    return r#type.as_str();
                }
                MarketTypeRule::Prefix { prefix, r#type } if slug.starts_with(prefix) => {
                    return r#type.as_str();
                }
                _ => {}
            }
        }
        self.default_type.as_str()
    }
}

pub fn load_config(path: Option<&std::path::Path>) -> anyhow::Result<AppConfig> {
    let Some(path) = path else {
        return Ok(AppConfig::default());
    };
    let bytes = std::fs::read(path)?;
    let s = std::str::from_utf8(&bytes)?;
    let cfg: AppConfig = toml::from_str(s)?;
    Ok(AppConfig::default().merge(cfg))
}

trait Merge {
    fn merge(self, other: Self) -> Self;
}

impl Merge for AppConfig {
    fn merge(self, other: Self) -> Self {
        Self {
            rate_limit_ms: if other.rate_limit_ms == 0 {
                self.rate_limit_ms
            } else {
                other.rate_limit_ms
            },
            cache_ttl_sec: if other.cache_ttl_sec == 0 {
                self.cache_ttl_sec
            } else {
                other.cache_ttl_sec
            },
            database_url: other.database_url.or(self.database_url),
            timeout_sec: if other.timeout_sec == 0 {
                self.timeout_sec
            } else {
                other.timeout_sec
            },
            trades_page_limit: if other.trades_page_limit == 0 {
                self.trades_page_limit
            } else {
                other.trades_page_limit
            },
            market_type: self.market_type.merge(other.market_type),
            subgraph: other.subgraph,
            reconciliation: other.reconciliation,
            ingestion: other.ingestion,
            canonical: other.canonical,
            analytics: other.analytics,
        }
    }
}

/// Config subset serialized for `report_cache` key (§1.5.5).
pub fn report_cache_fingerprint_value(cfg: &AppConfig) -> serde_json::Value {
    serde_json::json!({
        "subgraph": {
            "enabled": cfg.subgraph.enabled,
            "cap_rows_per_stream": cfg.subgraph.cap_rows_per_stream,
            "page_size": cfg.subgraph.page_size,
            "max_pages": cfg.subgraph.max_pages,
            "positions_page_size": cfg.subgraph.positions_page_size,
            "skip_pnl_positions": cfg.subgraph.skip_pnl_positions,
            "extended_fill_fields": cfg.subgraph.extended_fill_fields,
        },
        "canonical": {
            "enabled": cfg.canonical.enabled,
            "enrich_markets_dim": cfg.canonical.enrich_markets_dim,
        },
        "reconciliation": {
            "enabled": cfg.reconciliation.enabled,
            "time_window_sec": cfg.reconciliation.time_window_sec,
            "size_tolerance_pct": cfg.reconciliation.size_tolerance_pct,
            "price_tolerance_abs": cfg.reconciliation.price_tolerance_abs,
            "require_condition_match": cfg.reconciliation.require_condition_match,
            "rules_version": cfg.reconciliation.rules_version,
            "shadow_volume_alert_ratio": cfg.reconciliation.shadow_volume_alert_ratio,
            "api_only_ratio_alert": cfg.reconciliation.api_only_ratio_alert,
            "api_only_alert_min": cfg.reconciliation.api_only_alert_min,
        },
        "trades_page_limit": cfg.trades_page_limit,
        "ingestion": {
            "persist_raw": cfg.ingestion.persist_raw,
        },
        "analytics": {
            "source": cfg.analytics.source,
            "canonical_shadow": cfg.analytics.canonical_shadow,
        },
    })
}

/// Stable cache key: `wallet_lower` + SHA256 prefix of fingerprint JSON.
pub fn report_cache_key(wallet: &str, cfg: &AppConfig) -> String {
    let w = wallet.trim().to_lowercase();
    let body = serde_json::to_string(&report_cache_fingerprint_value(cfg)).unwrap_or_default();
    let mut h = Sha256::new();
    h.update(body.as_bytes());
    let hex = format!("{:x}", h.finalize());
    let short: String = hex.chars().take(16).collect();
    format!("{w}:{short}")
}

impl Merge for MarketTypeConfig {
    fn merge(self, other: Self) -> Self {
        Self {
            default_type: if other.default_type.is_empty() {
                self.default_type
            } else {
                other.default_type
            },
            rules: if other.rules.is_empty() { self.rules } else { other.rules },
        }
    }
}
