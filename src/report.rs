use crate::canonical::{CanonicalShadowMetrics, ReconciliationReportV1};
use crate::reconciliation::ReconciliationSummary;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// 新算报告根字段 `schema_version`；与 README / 前端说明对齐，变更时同步 bump。
pub const REPORT_SCHEMA_VERSION: &str = "2.5.4";

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

/// Fixed price bands for dashboards (`<0.1`, `0.1–0.3`, …).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedPriceBucket {
    pub label: String,
    pub range_low: f64,
    pub range_high: f64,
    pub count: usize,
}

/// Compact row for UI / LLM prompts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeHighlight {
    pub slug: String,
    pub side: String,
    pub price: f64,
    pub size: f64,
    /// Realized PnL on this fill (average-cost model); **0** on buys.
    #[serde(default)]
    pub pnl: f64,
    pub cash_flow: f64,
    pub timestamp: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionRowDisplay {
    pub slug: Option<String>,
    pub title: Option<String>,
    pub outcome: Option<String>,
    pub size: Option<f64>,
    pub avg_price: Option<f64>,
    pub cur_price: Option<f64>,
    pub cash_pnl: Option<f64>,
    pub current_value: Option<f64>,
}

/// 逐笔台账：Data API 每条成交一行 + 已结算持仓的 **SETTLEMENT** 合成行（买入/卖出或清算价与名义、盈亏）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeLedgerRow {
    /// Unix 毫秒（结算行用 Gamma 解析时间，缺省则为最后一笔成交时间）。
    pub ts_ms: i64,
    pub slug: String,
    pub condition_id: String,
    pub outcome: Option<String>,
    /// `BUY` | `SELL` | `SETTLEMENT`
    pub row_kind: String,
    pub size: f64,
    /// 买入均价：BUY 行为成交价；SELL 行为平均成本；SETTLEMENT 为持仓成本。
    pub buy_price: f64,
    pub buy_total: f64,
    /// 卖出/清算价：SELL 为成交价；SETTLEMENT 为 Gamma payout；BUY 为 0。
    pub sell_price: f64,
    pub sell_total: f64,
    /// 本行已实现盈亏：SELL 为平均成本法；SETTLEMENT 为 `size*(payout−avg)`；BUY 为 0。
    pub pnl: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

/// `trade_ledger` 与 `trade_ledger_paired` 对账：无记录被删，仅视图过滤 BUY；Σpnl 应一致（BUY 行 pnl 恒为 0）。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TradeLedgerIntegrity {
    pub full_row_count: usize,
    pub buy_row_count: usize,
    pub sell_row_count: usize,
    pub settlement_row_count: usize,
    pub paired_row_count: usize,
    pub sum_pnl_full: f64,
    pub sum_pnl_paired: f64,
    /// `paired_row_count == sell + settlement` 且 Σpnl 差在容差内。
    pub integrity_ok: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FrontendPresentation {
    pub biggest_wins: Vec<TradeHighlight>,
    pub biggest_losses: Vec<TradeHighlight>,
    pub recent_trades: Vec<TradeHighlight>,
    pub current_positions: Vec<PositionRowDisplay>,
    /// 时间序：成交 + 结算行；可与 `lifetime.net_pnl` 逐项对照。
    #[serde(default)]
    pub trade_ledger: Vec<TradeLedgerRow>,
    /// 仅 **SELL** + **SETTLEMENT**（退出侧）：每行均有 `pnl`；买入开仓不单独占行，与 `trade_pnl` 平均成本法一致。
    #[serde(default)]
    pub trade_ledger_paired: Vec<TradeLedgerRow>,
    /// 完整台账 vs 已平仓视图行数与 Σpnl 对账（2.5.4+）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trade_ledger_integrity: Option<TradeLedgerIntegrity>,
    /// One-shot text for pasting into an LLM as wallet context.
    pub ai_copy_prompt: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GammaProfileSummary {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar_url: Option<String>,
    /// ISO 8601 from Gamma `createdAt`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bio: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified_badge: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_wallet: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub x_username: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyzeReport {
    #[serde(default = "default_schema_version")]
    pub schema_version: String,
    pub wallet: String,
    /// Distinct markets: unique non-empty `slug` (else `condition_id`), aligned with Polymarket
    /// `user-stats.trades` for typical accounts.
    pub trades_count: usize,
    /// Raw Data API `/trades` row count (fills / executions).
    #[serde(default)]
    pub trades_fill_count: usize,
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
    /// Same buckets as `price_buckets` with human labels for charts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_buckets_chart: Option<Vec<NormalizedPriceBucket>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frontend: Option<FrontendPresentation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gamma_profile: Option<GammaProfileSummary>,
    /// RFC3339 UTC: set on fresh `build_report`; on cache read, overwritten from Postgres `updated_at`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report_updated_at: Option<String>,
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
    /// Per-trade SELL-realized + Gamma settlement on remaining shares at resolved markets.
    pub net_pnl: f64,
    /// Σ `shares × (Gamma outcomePrice − avg_cost)` for inventory still open at resolution (see notes).
    #[serde(default)]
    pub net_pnl_settlement: f64,
    /// Sum of `currentValue` from Data API open positions when available.
    pub open_position_value: f64,
    pub max_single_win: f64,
    pub max_single_loss: f64,
    /// Sum of `realizedPnl` from `/closed-positions` when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_realized_pnl_sum: Option<f64>,
    /// Count of open rows from `/positions`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub open_positions_count: Option<usize>,
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
    /// P50 seconds from trade time to market resolution (Gamma end/close), when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_to_resolution_p50_sec: Option<f64>,
    /// P90 seconds from trade time to resolution; **< 60** often indicates last-minute / degen entry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_to_resolution_p90_sec: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingPatterns {
    pub grid_like_markets: usize,
    pub side_bias: SideBias,
    /// Win rate from per-trade cash flow (same as before).
    pub win_rate_overall: f64,
    pub win_rate_by_market_type: Vec<WinRateByMarketType>,
    /// Fraction of distinct markets with >3 trades (grid-like), **0–1**.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grid_like_market_ratio: Option<f64>,
    /// Win rate from closed positions (`realizedPnl > 0`) when enough samples.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub win_rate_closed_positions: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_positions_sample_size: Option<usize>,
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
