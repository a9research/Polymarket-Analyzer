use crate::canonical::{CanonicalShadowMetrics, ReconciliationReportV1};
use crate::reconciliation::ReconciliationSummary;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

fn default_schema_version() -> String {
    // Legacy cached reports without this field deserialize as 1.0.0; new runs set 2.0.0 in `build_report`.
    "1.0.0".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DataLineage {
    /// Where lifetime / distribution / patterns metrics were computed from.
    #[serde(default = "default_lineage_analytics")]
    pub analytics_primary_source: String,
    #[serde(default)]
    pub canonical_merge_applied: bool,
    #[serde(default)]
    pub markets_dim_enriched: bool,
}

fn default_lineage_analytics() -> String {
    "data_api_trades".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CanonicalSummary {
    pub enabled: bool,
    pub run_id: String,
    pub rules_version: String,
    pub merged_trade_fills: usize,
    pub data_api_trade_only: usize,
    pub subgraph_fill_only: usize,
    pub redemptions: usize,
    pub position_snapshots: usize,
    pub ambiguous_queue_rows: usize,
    pub canonical_events_total: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DataApiTruncationMeta {
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub max_offset_allowed: Option<u32>,
    #[serde(default)]
    pub trades_received: usize,
}

/// §1.5 `ingestion.truncation`: data_api + subgraph pagination metadata.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IngestionTruncation {
    #[serde(default)]
    pub data_api: DataApiTruncationMeta,
    /// Per-stream `_spike_pagination` or error flags (JSON object).
    #[serde(default)]
    pub subgraph: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SectionProvenance {
    /// e.g. `data_api_trades` | `canonical_synthetic_trades`
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReportProvenance {
    #[serde(default)]
    pub lifetime: SectionProvenance,
    #[serde(default)]
    pub market_distribution: SectionProvenance,
    #[serde(default)]
    pub price_buckets: SectionProvenance,
    #[serde(default)]
    pub time_analysis: SectionProvenance,
    #[serde(default)]
    pub trading_patterns: SectionProvenance,
    #[serde(default)]
    pub strategy_inference: SectionProvenance,
}

impl ReportProvenance {
    /// 各分析块使用同一 `source` 与 `notes`（整报告级血缘）。
    pub fn uniform(source: impl Into<String>, notes: Vec<String>) -> Self {
        let source = source.into();
        Self {
            lifetime: SectionProvenance {
                source: source.clone(),
                notes: notes.clone(),
            },
            market_distribution: SectionProvenance {
                source: source.clone(),
                notes: notes.clone(),
            },
            price_buckets: SectionProvenance {
                source: source.clone(),
                notes: notes.clone(),
            },
            time_analysis: SectionProvenance {
                source: source.clone(),
                notes: notes.clone(),
            },
            trading_patterns: SectionProvenance {
                source: source.clone(),
                notes: notes.clone(),
            },
            strategy_inference: SectionProvenance { source, notes },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IngestionMeta {
    pub run_id: Option<String>,
    pub persist_raw: bool,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub truncation: Option<IngestionTruncation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyzeReport {
    #[serde(default = "default_schema_version")]
    pub schema_version: String,
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
    /// Postgres raw ingestion + run id when enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingestion: Option<IngestionMeta>,
    /// Subgraph streams (same shape as former spike JSON sections).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subgraph: Option<serde_json::Value>,
    /// v0 timestamp-window reconciliation (optional).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconciliation: Option<ReconciliationSummary>,
    /// v1 canonical merge statistics + ambiguous cases (optional).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconciliation_v1: Option<ReconciliationReportV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_summary: Option<CanonicalSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_lineage: Option<DataLineage>,
    /// Per-block analytics source (§1.5.4 / §1.5.5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<ReportProvenance>,
    /// When canonical merge ran and `analytics.canonical_shadow`, compares to primary metrics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_canonical_shadow: Option<CanonicalShadowMetrics>,
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

