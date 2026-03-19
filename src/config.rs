use serde::{Deserialize, Serialize};

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
    /// Use 3000 to minimize round-trips when max historical offset is 3000 (typically 2 requests).
    #[serde(default = "default_trades_page_limit")]
    pub trades_page_limit: u32,

    #[serde(default)]
    pub market_type: MarketTypeConfig,
}

fn default_trades_page_limit() -> u32 {
    500
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
    Ok(AppConfig {
        ..AppConfig::default()
    }
    .merge(cfg))
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
        }
    }
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

