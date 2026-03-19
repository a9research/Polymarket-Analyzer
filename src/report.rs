use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyzeReport {
    pub wallet: String,
    pub trades_count: usize,
    pub total_volume: f64,
    pub lifetime: LifetimeMetrics,
    pub market_distribution: Vec<MarketDistributionItem>,
    pub price_buckets: BTreeMap<String, usize>,
    #[serde(default)]
    pub data_fetch: DataFetchMeta,
    pub time_analysis: TimeAnalysis,
    pub trading_patterns: TradingPatterns,
    pub strategy_inference: StrategyInference,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DataFetchMeta {
    /// Whether upstream data was truncated due to API historical offset limits.
    #[serde(default)]
    pub truncated: bool,
    /// The max offset reported by upstream (when parseable).
    #[serde(default)]
    pub max_offset_allowed: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifetimeMetrics {
    pub total_trades: usize,
    pub total_volume: f64,
    pub net_pnl: f64,
    pub open_position_value: f64,
    pub max_single_win: f64,
    pub max_single_loss: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketDistributionItem {
    pub market_type: String,
    pub trades: usize,
    pub volume: f64,
    pub pnl: f64,
    pub trades_pct: f64,
    pub volume_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeAnalysis {
    pub active_hours_utc: BTreeMap<u32, usize>,
    pub entry_to_resolution_seconds: Vec<i64>,
    pub holding_duration_seconds: Vec<i64>,
    pub metadata_missing_ratio: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingPatterns {
    pub grid_like_markets: usize,
    pub side_bias: SideBias,
    pub win_rate_overall: f64,
    pub win_rate_by_market_type: Vec<WinRateByMarketType>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SideBias {
    pub buy_pct: f64,
    pub sell_pct: f64,
    pub yes_pct: f64,
    pub no_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WinRateByMarketType {
    pub market_type: String,
    pub win_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyInference {
    pub primary_style: String,
    pub rule_json: serde_json::Value,
    pub pseudocode: String,
}

