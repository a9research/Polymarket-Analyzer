use chrono::{DateTime, Timelike, Utc};
use clap::{Args, Parser, Subcommand};
use polymarket_account_analyzer::{
    canonical::{
        compute_shadow_metrics, normalize_condition_id, slug_map_from_canonical_events,
        synthetic_trades_from_events,
    },
    config::{
        apply_env_overrides, load_config, report_cache_key, AppConfig, ReconciliationConfig,
    },
    polymarket::{
        data_api::{ClosedPosition, DataApiClient, Trade, TradeSide, UserPosition},
        gamma_api::GammaApiClient,
        gamma_taxonomy::GammaTaxonomy,
    },
    reconciliation::{
        shadow_volume_discrepancy_note, summary_from_v1, v1_coverage_alert_notes,
    },
    report::{
        AnalyzeReport, CanonicalSummary, DataApiTruncationMeta, DataFetchMeta, DataLineage,
        FrontendPresentation, GammaProfileSummary, IngestionMeta, IngestionTruncation,
        LifetimeMetrics, MarketDistributionItem, NormalizedPriceBucket, PositionRowDisplay,
        ReportProvenance, REPORT_SCHEMA_VERSION, SideBias, StrategyInference, TimeAnalysis,
        TradeHighlight, TradeLedgerIntegrity, TradeLedgerRow, TradingPatterns,
        WinRateByMarketType,
    },
    settlement_pnl::{settlement_legs_for_open_book, GammaResolutionPayouts, SettlementLeg},
    storage::Storage,
    strategy::{self, StrategyInputs},
    trade_pnl,
};
use reqwest::Client;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::{path::PathBuf, time::Duration};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

/// Extra Data API + Gamma data merged into `build_report` (schema 2.1+).
#[derive(Default)]
struct ReportAugment {
    resolution_ts_by_slug: HashMap<String, i64>,
    /// Gamma-resolved payout vectors keyed by market `slug` (same cap as resolution times).
    gamma_resolution_by_slug: HashMap<String, GammaResolutionPayouts>,
    /// `market_distribution` 桶名：Gamma `category` / `tags` + `/tags`·`/sports` 目录（与 `max_gamma_slugs_for_timing` 同批 slug）。
    gamma_bucket_by_slug: HashMap<String, String>,
    open_positions: Vec<UserPosition>,
    closed_positions: Vec<ClosedPosition>,
    gamma_profile: Option<GammaProfileSummary>,
}

fn trade_ts_sec(t: &Trade) -> i64 {
    let ts = t.timestamp;
    if ts > 1_000_000_000_000 {
        ts / 1000
    } else {
        ts
    }
}

fn trade_ts_ms(t: &Trade) -> i64 {
    let ts = t.timestamp;
    if ts > 1_000_000_000_000 {
        ts
    } else {
        ts * 1000
    }
}

/// 时间序台账：每条 `/trades` 一行 + 已 resolve 的 **SETTLEMENT** 行（与 `net_pnl` 口径一致）。
/// 从完整台账去掉 `BUY` 行：只保留产生已实现盈亏的 **卖出** 与 **结算**（「成对」的退出侧视角，成本为持仓均价）。
fn trade_ledger_paired_from_full(full: &[TradeLedgerRow]) -> Vec<TradeLedgerRow> {
    full.iter()
        .filter(|r| r.row_kind != "BUY")
        .cloned()
        .collect()
}

/// 证明：`trade_ledger_paired` 仅为 `trade_ledger` 去掉 BUY 的**视图**，不删数据；Σpnl 因 BUY 行 pnl=0 应与全表一致。
fn compute_trade_ledger_integrity(
    full: &[TradeLedgerRow],
    paired: &[TradeLedgerRow],
) -> TradeLedgerIntegrity {
    let full_row_count = full.len();
    let buy_row_count = full.iter().filter(|r| r.row_kind == "BUY").count();
    let sell_row_count = full.iter().filter(|r| r.row_kind == "SELL").count();
    let settlement_row_count = full
        .iter()
        .filter(|r| r.row_kind == "SETTLEMENT")
        .count();
    let paired_row_count = paired.len();
    let rows_ok = paired_row_count == sell_row_count + settlement_row_count
        && full_row_count == buy_row_count + sell_row_count + settlement_row_count;
    let sum_pnl_full: f64 = full.iter().map(|r| r.pnl).sum();
    let sum_pnl_paired: f64 = paired.iter().map(|r| r.pnl).sum();
    let diff = (sum_pnl_full - sum_pnl_paired).abs();
    let tol = 1e-6_f64.max(1e-9 * sum_pnl_full.abs().max(sum_pnl_paired.abs()));
    let pnl_ok = diff <= tol;
    TradeLedgerIntegrity {
        full_row_count,
        buy_row_count,
        sell_row_count,
        settlement_row_count,
        paired_row_count,
        sum_pnl_full,
        sum_pnl_paired,
        integrity_ok: rows_ok && pnl_ok,
    }
}

fn build_trade_ledger(
    trades: &[Trade],
    per_trade_pnl: &[f64],
    settlement_legs: &[SettlementLeg],
    resolution_ts_by_slug: &HashMap<String, i64>,
) -> Vec<TradeLedgerRow> {
    let last_fill_ms = trades.iter().map(trade_ts_ms).max().unwrap_or(0);
    let mut rows = Vec::with_capacity(trades.len() + settlement_legs.len());
    for (i, t) in trades.iter().enumerate() {
        let ts_ms = trade_ts_ms(t);
        let size = t.size;
        let price = t.price;
        let pnl = per_trade_pnl.get(i).copied().unwrap_or(0.0);
        let (row_kind, buy_price, buy_total, sell_price, sell_total) = match t.side {
            TradeSide::Buy => {
                let bt = size * price;
                ("BUY".to_string(), price, bt, 0.0, 0.0)
            }
            TradeSide::Sell => {
                let st = size * price;
                let avg = if size > 1e-12 && price.is_finite() {
                    price - pnl / size
                } else {
                    price
                };
                let bt = size * avg;
                ("SELL".to_string(), avg, bt, price, st)
            }
        };
        rows.push(TradeLedgerRow {
            ts_ms,
            slug: t.slug.clone(),
            condition_id: t.condition_id.clone(),
            outcome: t.outcome.clone(),
            row_kind,
            size,
            buy_price,
            buy_total,
            sell_price,
            sell_total,
            pnl,
            title: t.title.clone(),
        });
    }
    for leg in settlement_legs {
        let ts_ms = resolution_ts_by_slug
            .get(&leg.slug)
            .map(|sec| (*sec).saturating_mul(1000))
            .filter(|&ms| ms > 0)
            .unwrap_or(last_fill_ms);
        let buy_total = leg.shares * leg.avg_entry_price;
        let sell_total = leg.shares * leg.payout_per_share;
        rows.push(TradeLedgerRow {
            ts_ms,
            slug: leg.slug.clone(),
            condition_id: String::new(),
            outcome: None,
            row_kind: "SETTLEMENT".to_string(),
            size: leg.shares,
            buy_price: leg.avg_entry_price,
            buy_total,
            sell_price: leg.payout_per_share,
            sell_total,
            pnl: leg.pnl,
            title: None,
        });
    }
    rows.sort_by(|a, b| {
        a.ts_ms
            .cmp(&b.ts_ms)
            .then_with(|| a.row_kind.cmp(&b.row_kind))
    });
    rows
}

fn percentile_i64_sorted(sorted: &[i64], p: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p.clamp(0.0, 1.0)).round() as usize;
    Some(sorted[idx.min(sorted.len() - 1)] as f64)
}

async fn fetch_gamma_context_for_slugs(
    gamma: &GammaApiClient,
    slugs_source: &[String],
    max_slugs: usize,
    taxonomy: Option<&GammaTaxonomy>,
) -> (
    HashMap<String, i64>,
    HashMap<String, GammaResolutionPayouts>,
    HashMap<String, String>,
) {
    if max_slugs == 0 {
        return (HashMap::new(), HashMap::new(), HashMap::new());
    }
    let mut slugs: Vec<String> = slugs_source
        .iter()
        .cloned()
        .filter(|s| !s.trim().is_empty())
        .collect();
    slugs.sort();
    slugs.dedup();
    let mut resolution_ts_by_slug = HashMap::new();
    let mut gamma_resolution_by_slug = HashMap::new();
    let mut gamma_bucket_by_slug = HashMap::new();
    for slug in slugs.into_iter().take(max_slugs) {
        let market_res = if taxonomy.is_some() {
            gamma.market_by_slug_include_tags(&slug).await
        } else {
            gamma.market_by_slug(&slug).await
        };
        match market_res {
            Ok(m) => {
                if let Some(tax) = taxonomy {
                    if let Some(bucket) = tax.bucket_for_market(&m) {
                        gamma_bucket_by_slug.insert(slug.clone(), bucket);
                    }
                }
                let ts = m
                    .closed_time
                    .as_deref()
                    .and_then(parse_gamma_datetime_utc)
                    .or_else(|| m.end_date.as_deref().and_then(parse_gamma_datetime_utc));
                if let Some(dt) = ts {
                    resolution_ts_by_slug.insert(slug.clone(), dt.timestamp());
                }
                if let Some(gp) = GammaResolutionPayouts::from_market(&m) {
                    gamma_resolution_by_slug.insert(slug, gp);
                }
            }
            Err(e) => tracing::debug!("gamma context skip slug={slug} err={e:#}"),
        }
    }
    (
        resolution_ts_by_slug,
        gamma_resolution_by_slug,
        gamma_bucket_by_slug,
    )
}

fn slugs_from_positions(open: &[UserPosition], closed: &[ClosedPosition]) -> Vec<String> {
    let mut v: Vec<String> = open
        .iter()
        .filter_map(|p| p.slug.as_ref().map(|s| s.trim().to_string()))
        .chain(
            closed
                .iter()
                .filter_map(|p| p.slug.as_ref().map(|s| s.trim().to_string())),
        )
        .filter(|s| !s.is_empty())
        .collect();
    v.sort();
    v.dedup();
    v
}

fn build_frontend_presentation(
    wallet: &str,
    trades: &[Trade],
    per_trade_pnl: &[f64],
    open: &[UserPosition],
    primary_style: &str,
    win_rate: f64,
    total_vol: f64,
    trade_ledger: Vec<TradeLedgerRow>,
    trade_ledger_paired: Vec<TradeLedgerRow>,
    trade_ledger_integrity: Option<TradeLedgerIntegrity>,
) -> FrontendPresentation {
    let mut scored: Vec<TradeHighlight> = trades
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let vol = t.size * t.price;
            let cash_flow = match t.side {
                TradeSide::Buy => -vol,
                TradeSide::Sell => vol,
            };
            let pnl = per_trade_pnl.get(i).copied().unwrap_or(0.0);
            TradeHighlight {
                slug: t.slug.clone(),
                side: match t.side {
                    TradeSide::Buy => "BUY".to_string(),
                    TradeSide::Sell => "SELL".to_string(),
                },
                price: t.price,
                size: t.size,
                pnl,
                cash_flow,
                timestamp: t.timestamp,
                title: t.title.clone(),
            }
        })
        .collect();

    let mut wins: Vec<TradeHighlight> = scored
        .iter()
        .filter(|h| h.pnl > 0.0)
        .cloned()
        .collect();
    wins.sort_by(|a, b| b.pnl.partial_cmp(&a.pnl).unwrap_or(std::cmp::Ordering::Equal));
    wins.truncate(5);

    let mut losses: Vec<TradeHighlight> = scored
        .iter()
        .filter(|h| h.pnl < 0.0)
        .cloned()
        .collect();
    losses.sort_by(|a, b| a.pnl.partial_cmp(&b.pnl).unwrap_or(std::cmp::Ordering::Equal));
    losses.truncate(5);

    scored.sort_by_key(|h| h.timestamp);
    let recent: Vec<TradeHighlight> = scored.iter().rev().take(20).cloned().collect();

    let current_positions: Vec<PositionRowDisplay> = open
        .iter()
        .take(50)
        .map(|p| PositionRowDisplay {
            slug: p.slug.clone(),
            title: p.title.clone(),
            outcome: p.outcome.clone(),
            size: p.size,
            avg_price: p.avg_price,
            cur_price: p.cur_price,
            cash_pnl: p.cash_pnl,
            current_value: p.current_value,
        })
        .collect();

    let ai_copy_prompt = format!(
        "Analyze Polymarket wallet {}.\nApprox {} trades; notional volume {:.2} (trade cash-flow basis).\nInferred style: {}.\nPer-trade win rate ~{:.1}% (cash-flow sign).\nfrontend.*.pnl = realized PnL on sells (average-cost inventory per outcome).\nUse strategy_inference.rule_json (entry P90, jackpot_bias, preferred_price_ranges) and frontend.biggest_wins/losses (sort by pnl).",
        wallet,
        trades.len(),
        total_vol,
        primary_style,
        win_rate
    );

    FrontendPresentation {
        biggest_wins: wins,
        biggest_losses: losses,
        recent_trades: recent,
        current_positions,
        trade_ledger,
        trade_ledger_paired,
        trade_ledger_integrity,
        ai_copy_prompt,
    }
}

/// Shadow 体积与主报告差异告警：写入对账摘要、v1 notes；若无 v1 则写入 `report.notes`。
fn apply_shadow_volume_quality_alert(
    report: &mut AnalyzeReport,
    recon: &ReconciliationConfig,
    primary_vol: f64,
    shadow_vol: f64,
    analytics_src: &str,
) {
    let Some(alert) = shadow_volume_discrepancy_note(
        primary_vol,
        shadow_vol,
        recon.shadow_volume_alert_ratio,
        analytics_src,
    ) else {
        return;
    };
    if let Some(ref mut s) = report.reconciliation {
        s.note.push('\n');
        s.note.push_str(&alert);
    }
    if let Some(ref mut v1) = report.reconciliation_v1 {
        v1.notes.push(alert);
    } else {
        report.notes.push(alert);
    }
}

fn write_json_output<T: serde::Serialize>(value: &T, out: Option<&PathBuf>) -> anyhow::Result<()> {
    let json = serde_json::to_string_pretty(value)?;
    match out {
        Some(path) => std::fs::write(path, json)?,
        None => println!("{json}"),
    }
    Ok(())
}

#[derive(Debug, Parser)]
#[command(name = "polymarket-account-analyzer")]
#[command(version)]
#[command(about = "Analyze a Polymarket wallet (read-only)")]
struct Cli {
    /// Config path (TOML). If omitted, defaults are used.
    #[arg(long)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    cmd: Command,
}

#[derive(Debug, Args, Clone, Default)]
struct AnalyzeCliFlags {
    /// Skip writing raw chunks to Postgres for this run.
    #[arg(long, default_value_t = false)]
    no_persist_raw: bool,
}

#[derive(Debug, Clone, Default)]
struct AnalyzeOverrides {
    no_persist_raw: bool,
}

impl From<&AnalyzeCliFlags> for AnalyzeOverrides {
    fn from(f: &AnalyzeCliFlags) -> Self {
        Self {
            no_persist_raw: f.no_persist_raw,
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct AnalyzeQuery {
    #[serde(default)]
    no_cache: bool,
    /// Return only a Postgres-cached report (no full pipeline). 404 if miss.
    #[serde(default)]
    cached_only: bool,
    #[serde(default)]
    no_persist_raw: bool,
}

#[derive(Debug, serde::Deserialize)]
struct LeaderboardQuery {
    /// 1–100, default 30.
    #[serde(default)]
    limit: Option<i64>,
    /// `all` (default) uses lifetime snapshot table; `today` UTC day; `week` / `month` rolling windows on cached per-trade rows.
    #[serde(default)]
    period: Option<String>,
}

impl From<&AnalyzeQuery> for AnalyzeOverrides {
    fn from(q: &AnalyzeQuery) -> Self {
        Self {
            no_persist_raw: q.no_persist_raw,
        }
    }
}

fn effective_config(base: &AppConfig, o: &AnalyzeOverrides) -> AppConfig {
    let mut c = base.clone();
    if o.no_persist_raw {
        c.ingestion.persist_raw = false;
    }
    c
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Analyze a wallet and output JSON report.
    Analyze {
        wallet: String,
        #[command(flatten)]
        flags: AnalyzeCliFlags,
        /// Write report JSON to this file (default: stdout).
        #[arg(long)]
        out: Option<PathBuf>,
        /// Bypass postgres cache for this request.
        #[arg(long, default_value_t = false)]
        no_cache: bool,
    },
    /// Start REST API server.
    Serve {
        /// Bind address, e.g. 127.0.0.1:3000
        #[arg(long, default_value = "127.0.0.1:3000")]
        bind: String,
    },
    /// Export `reconciliation_ambiguous_queue` rows for an ingestion run (requires Postgres).
    ExportAmbiguous {
        /// `ingestion_run.id` (UUID string).
        run_id: String,
    },
    /// Set `review_status` on one ambiguous queue row (state: pending|reviewed|dismissed|escalated).
    SetAmbiguousReview {
        /// `ingestion_run.id` (UUID string).
        run_id: String,
        /// Same `trade_key` as in export-ambiguous JSON.
        trade_key: String,
        /// `pending` | `reviewed` | `dismissed` | `escalated`
        status: String,
    },
    /// Rebuild `AnalyzeReport` from Postgres `canonical_events` for a past run (no Data API / subgraph).
    ReportFromCanonicalRun {
        /// `ingestion_run.id` (UUID string).
        run_id: String,
        #[arg(long)]
        out: Option<PathBuf>,
        /// Also upsert `report_cache_kv` (same key as live `analyze`).
        #[arg(long, default_value_t = false)]
        write_cache: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let mut cfg = load_config(cli.config.as_deref())?;
    apply_env_overrides(&mut cfg);

    match cli.cmd {
        Command::Analyze {
            wallet,
            flags,
            out,
            no_cache,
        } => {
            let cfg = effective_config(&cfg, &AnalyzeOverrides::from(&flags));
            let storage = init_storage(&cfg).await?;
            let report = analyze_wallet(&cfg, storage.as_ref(), &wallet, no_cache).await?;
            write_json_output(&report, out.as_ref())?;
        }
        Command::Serve { bind } => {
            serve(cfg, bind).await?;
        }
        Command::ExportAmbiguous { run_id } => {
            let storage = init_storage(&cfg).await?;
            let Some(store) = storage else {
                anyhow::bail!("DATABASE_URL / postgres required for export-ambiguous");
            };
            let rows = store.fetch_ambiguous_queue_by_run(&run_id).await?;
            println!("{}", serde_json::to_string_pretty(&rows)?);
        }
        Command::SetAmbiguousReview {
            run_id,
            trade_key,
            status,
        } => {
            let storage = init_storage(&cfg).await?;
            let Some(store) = storage else {
                anyhow::bail!("DATABASE_URL / postgres required for set-ambiguous-review");
            };
            let n = store
                .update_ambiguous_review_status(&run_id, &trade_key, &status)
                .await?;
            println!("updated_rows={n}");
        }
        Command::ReportFromCanonicalRun {
            run_id,
            out,
            write_cache,
        } => {
            let storage = init_storage(&cfg).await?;
            let Some(store) = storage else {
                anyhow::bail!("DATABASE_URL required for report-from-canonical-run");
            };
            let report =
                report_from_canonical_pg_run(&cfg, &store, &run_id, write_cache).await?;
            write_json_output(&report, out.as_ref())?;
        }
    }

    Ok(())
}

async fn init_storage(cfg: &AppConfig) -> anyhow::Result<Option<Storage>> {
    let db_url = cfg
        .database_url
        .clone()
        .or_else(|| std::env::var("DATABASE_URL").ok());
    let Some(database_url) = db_url else {
        tracing::warn!("DATABASE_URL not configured; running without postgres cache");
        return Ok(None);
    };

    match Storage::connect(&database_url).await {
        Ok(storage) => {
            if let Err(e) = storage.init_schema().await {
                tracing::warn!("postgres schema init failed; disabling cache: {e:#}");
                return Ok(None);
            }
            Ok(Some(storage))
        }
        Err(e) => {
            tracing::warn!("postgres connect failed; disabling cache: {e:#}");
            Ok(None)
        }
    }
}

/// §1.5 / 差距矩阵 **E**：从已落库的 `canonical_events` 重放指标（不访问外网）。
async fn report_from_canonical_pg_run(
    cfg: &AppConfig,
    store: &Storage,
    run_id: &str,
    write_cache: bool,
) -> anyhow::Result<AnalyzeReport> {
    let wallet = store
        .fetch_wallet_for_run(run_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("no ingestion_run row for run_id={run_id}"))?;

    let events = store.fetch_canonical_events_by_run(run_id).await?;
    if events.is_empty() {
        anyhow::bail!(
            "no canonical_events for run_id={run_id}; run `analyze` with --with-canonical (and persist) first"
        );
    }

    let slug_map = slug_map_from_canonical_events(&events);
    let synth = synthetic_trades_from_events(&events, &slug_map);
    if synth.is_empty() {
        anyhow::bail!(
            "canonical_events has {} rows but zero MERGED_TRADE_FILL/DATA_API_TRADE with size/price; cannot build trade stream",
            events.len()
        );
    }

    let mut report = build_report_from_trades(
        cfg,
        &wallet,
        &synth,
        false,
        None,
        &ReportAugment::default(),
    );
    report.notes.insert(
        0,
        format!(
            "Report rebuilt from Postgres canonical_events replay (run_id={}, {} trade-like rows, {} canonical rows).",
            run_id,
            synth.len(),
            events.len()
        ),
    );

    if let Some(v1) = store.fetch_reconciliation_v1_by_run(run_id).await? {
        let mut summ = summary_from_v1(&v1, cfg.reconciliation.time_window_sec);
        for n in v1_coverage_alert_notes(
            &v1.counts,
            cfg.reconciliation.api_only_ratio_alert,
            cfg.reconciliation.api_only_alert_min,
        ) {
            summ.note.push('\n');
            summ.note.push_str(&n);
        }
        report.reconciliation_v1 = Some(v1.clone());
        report.reconciliation = Some(summ);
        report.canonical_summary = Some(CanonicalSummary {
            enabled: true,
            run_id: run_id.to_string(),
            rules_version: v1.rules_version.clone(),
            merged_trade_fills: v1.counts.matched,
            data_api_trade_only: v1.counts.api_only,
            subgraph_fill_only: v1.counts.subgraph_fill_only,
            redemptions: v1.counts.redemptions,
            position_snapshots: v1.counts.position_snapshots,
            ambiguous_queue_rows: v1.counts.ambiguous,
            canonical_events_total: v1.counts.canonical_total,
        });
    }

    let shadow = compute_shadow_metrics(&events);
    let shadow_vol = shadow.total_volume;
    report.metrics_canonical_shadow = Some(shadow);
    let primary_vol = report.total_volume;
    apply_shadow_volume_quality_alert(
        &mut report,
        &cfg.reconciliation,
        primary_vol,
        shadow_vol,
        "canonical_pg_replay",
    );

    report.data_lineage = Some(DataLineage {
        analytics_primary_source: "canonical_pg_replay".to_string(),
        canonical_merge_applied: true,
        markets_dim_enriched: false,
    });

    let prov_note = format!("canonical_pg_replay;run_id={run_id};events={}", events.len());
    report.provenance = Some(ReportProvenance::uniform(
        "canonical_pg_replay",
        vec![prov_note],
    ));

    report.ingestion = Some(IngestionMeta {
        run_id: Some(run_id.to_string()),
        persist_raw: false,
        status: "canonical_pg_replay".to_string(),
        truncation: None,
    });

    if write_cache {
        let cache_key = report_cache_key(&wallet, cfg);
        let pnls = trade_pnl::per_trade_realized_pnl(&synth);
        store.upsert_report(&cache_key, &wallet, &report).await?;
        store
            .replace_wallet_trade_pnls(&cache_key, &wallet, &synth, &pnls)
            .await?;
        store
            .upsert_leaderboard_row(&wallet, &cache_key, &report)
            .await?;
    }

    Ok(report)
}

/// Refresh wallet snapshot JSON after analyze cache hit (background).
async fn wallet_refresh_after_cache_hit(
    store: Storage,
    cfg: AppConfig,
    wallet_lc: String,
    cache_key: String,
    cached: AnalyzeReport,
) {
    if !cfg.ingestion.persist_wallet_snapshots {
        return;
    }
    if let Err(e) = store
        .refresh_wallet_snapshots_after_cache_hit(&wallet_lc, &cache_key, &cached)
        .await
    {
        tracing::warn!("refresh_wallet_snapshots_after_cache_hit: {e:#}");
    }
}

async fn analyze_wallet(
    cfg: &AppConfig,
    storage: Option<&Storage>,
    wallet: &str,
    no_cache: bool,
) -> anyhow::Result<AnalyzeReport> {
    let cache_key = report_cache_key(wallet, cfg);
    let w = wallet.to_lowercase();

    if !no_cache {
        if let Some(store) = storage {
            if let Some(cached) = store
                .get_cached_report(&cache_key, wallet, cfg.cache_ttl_sec as i64)
                .await?
            {
                if cfg.ingestion.persist_wallet_snapshots {
                    let store_cl = store.clone();
                    let cfg_cl = cfg.clone();
                    let w_cl = w.clone();
                    let ck_cl = cache_key.clone();
                    let cached_cl = cached.clone();
                    tokio::spawn(async move {
                        wallet_refresh_after_cache_hit(store_cl, cfg_cl, w_cl, ck_cl, cached_cl).await;
                    });
                }
                return Ok(cached);
            }
        }
    }

    let mut persist_raw = cfg.ingestion.persist_raw && storage.is_some();
    if cfg.ingestion.persist_raw && storage.is_none() {
        tracing::warn!("ingestion.persist_raw=true but postgres unavailable; skipping raw persistence");
        persist_raw = false;
    }

    let params_snapshot = serde_json::json!({
        "ingestion": cfg.ingestion,
        "trades_page_limit": cfg.trades_page_limit,
        "market_type": cfg.market_type,
        "analyzer": "account_positions_v1",
    });

    let mut run_id: Option<Uuid> = None;
    let mut ingest_meta: Option<IngestionMeta> = None;
    if persist_raw {
        let store = storage.expect("persist_raw implies storage");
        let id = store
            .create_ingestion_run(wallet, &params_snapshot)
            .await?;
        run_id = Some(id);
        ingest_meta = Some(IngestionMeta {
            run_id: Some(id.to_string()),
            persist_raw: true,
            status: "running".to_string(),
            truncation: None,
        });
    }

    let timeout = Duration::from_secs(cfg.timeout_sec.max(10));
    let http = Client::builder()
        .user_agent("polymarket-account-analyzer/0.1")
        .timeout(timeout)
        .connect_timeout(Duration::from_secs(15))
        .no_proxy()
        .build()?;

    let rate = Duration::from_millis(cfg.rate_limit_ms);
    let data = DataApiClient::new(http.clone(), rate);

    let pos_base = if cfg.ingestion.data_api_positions_limit > 0 {
        cfg.ingestion.data_api_positions_limit
    } else {
        cfg.trades_page_limit
    };
    let pos_page = pos_base.clamp(10, 10_000);
    let open_positions = data
        .positions_all(wallet, pos_page)
        .await
        .unwrap_or_else(|e| {
            tracing::warn!("data-api /positions failed: {e:#}");
            Vec::new()
        });
    let closed_positions = data
        .closed_positions_all(wallet, pos_page)
        .await
        .unwrap_or_else(|e| {
            tracing::warn!("data-api /closed-positions failed: {e:#}");
            Vec::new()
        });

    if let (Some(store), Some(rid)) = (storage, run_id) {
        if cfg.ingestion.persist_positions_raw {
            for (i, p) in open_positions.iter().enumerate() {
                let row_payload = serde_json::to_value(p)?;
                store
                    .insert_raw_data_api_open_position(&rid, wallet, i as i32, &row_payload)
                    .await?;
            }
            for (i, p) in closed_positions.iter().enumerate() {
                let row_payload = serde_json::to_value(p)?;
                store
                    .insert_raw_data_api_closed_position(&rid, wallet, i as i32, &row_payload)
                    .await?;
            }
        }
    }

    let gamma_timing = GammaApiClient::new(http.clone(), rate);
    let gamma_timing_cap = cfg.ingestion.max_gamma_slugs_for_timing as usize;
    let taxonomy_arc = if cfg.ingestion.gamma_taxonomy {
        Some(GammaTaxonomy::cached(&gamma_timing, cfg.ingestion.gamma_taxonomy_cache_ttl_sec).await)
    } else {
        None
    };
    let position_slugs = slugs_from_positions(&open_positions, &closed_positions);
    let (resolution_ts_by_slug, gamma_resolution_by_slug, gamma_bucket_by_slug) =
        fetch_gamma_context_for_slugs(
            &gamma_timing,
            &position_slugs,
            gamma_timing_cap,
            taxonomy_arc.as_deref(),
        )
        .await;
    let gamma_profile = match gamma_timing.public_profile_by_address(wallet).await {
        Ok(p) => Some(GammaProfileSummary {
            display_name: p.name.clone().or_else(|| p.pseudonym.clone()),
            username: p.pseudonym.clone().or_else(|| p.x_username.clone()),
            avatar_url: p.profile_image.clone(),
            created_at: p.created_at.clone(),
            bio: p.bio.clone(),
            verified_badge: p.verified_badge,
            proxy_wallet: p.proxy_wallet.clone(),
            x_username: p.x_username.clone(),
        }),
        Err(e) => {
            tracing::debug!("gamma public-profile: {e:#}");
            None
        }
    };

    let augment = ReportAugment {
        resolution_ts_by_slug,
        gamma_resolution_by_slug,
        gamma_bucket_by_slug,
        open_positions,
        closed_positions,
        gamma_profile,
    };

    if cfg.canonical.enrich_markets_dim {
        if let Some(store) = storage {
            let gamma = GammaApiClient::new(http.clone(), rate);
            let mut seen: HashSet<String> = HashSet::new();
            for slug in position_slugs {
                if !seen.insert(slug.clone()) {
                    continue;
                }
                match gamma.market_by_slug(&slug).await {
                    Ok(m) => {
                        let end_d = m
                            .end_date
                            .as_deref()
                            .and_then(parse_gamma_datetime_utc);
                        let closed_d = m
                            .closed_time
                            .as_deref()
                            .and_then(parse_gamma_datetime_utc);
                        let raw = serde_json::to_value(&m).unwrap_or_default();
                        if let Err(e) = store
                            .upsert_market_dim(
                                &slug,
                                m.question.as_deref(),
                                end_d,
                                closed_d,
                                &raw,
                            )
                            .await
                        {
                            tracing::warn!("markets_dim upsert slug={slug} err={e:#}");
                        }
                    }
                    Err(e) => tracing::debug!("gamma skip slug={slug} err={e:#}"),
                }
            }
        }
    }

    let ingest_status = "ok";
    let subgraph_trunc = serde_json::json!({
        "fetched": false,
        "reason": "not_used",
    });

    let mut report = build_analyze_report(
        cfg,
        wallet,
        false,
        None,
        &augment,
    );

    let mut im = match ingest_meta.clone() {
        Some(m) => m,
        None => IngestionMeta {
            run_id: None,
            persist_raw: false,
            status: String::new(),
            truncation: None,
        },
    };
    im.truncation = Some(IngestionTruncation {
        data_api: DataApiTruncationMeta {
            truncated: false,
            max_offset_allowed: None,
            trades_received: 0,
        },
        subgraph: subgraph_trunc,
    });
    report.ingestion = Some(im);

    let analytics_src = "data_api_positions";
    let markets_dim_enriched = cfg.canonical.enrich_markets_dim && storage.is_some();

    report.data_lineage = Some(DataLineage {
        analytics_primary_source: analytics_src.to_string(),
        canonical_merge_applied: false,
        markets_dim_enriched,
    });

    report.provenance = Some(ReportProvenance::uniform(
        analytics_src.to_string(),
        vec![],
    ));

    if let Some(ref mut im) = report.ingestion {
        im.status = ingest_status.to_string();
    }

    if let (Some(store), Some(rid)) = (storage, run_id) {
        store.finish_ingestion_run(&rid, ingest_status).await?;
    }

    if let Some(store) = storage {
        store.upsert_report(&cache_key, wallet, &report).await?;
        store
            .upsert_leaderboard_row(wallet, &cache_key, &report)
            .await?;
    }

    Ok(report)
}

fn parse_gamma_datetime_utc(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s.trim())
        .ok()
        .map(|d| d.with_timezone(&Utc))
}

#[derive(Debug, serde::Deserialize)]
struct PositionActivityQuery {
    /// Data API `market` filter: condition id (`0x…`).
    market: String,
    /// Skip Postgres read; still writes merged result when DB is configured.
    #[serde(default)]
    no_cache: bool,
}

fn is_valid_polymarket_wallet(s: &str) -> bool {
    let s = s.trim();
    let Some(rest) = s.strip_prefix("0x") else {
        return false;
    };
    rest.len() == 40 && rest.chars().all(|c| c.is_ascii_hexdigit())
}

/// Per-market Data API `/activity` with optional Postgres merge + incremental `start` fetch.
async fn fetch_position_activity_json(
    cfg: &AppConfig,
    storage: Option<&Storage>,
    wallet: &str,
    q: &PositionActivityQuery,
) -> anyhow::Result<serde_json::Value> {
    use polymarket_account_analyzer::polymarket::data_api::{
        max_activity_ts_sec, merge_activities_incremental,
    };
    let w = wallet.trim().to_lowercase();
    if !is_valid_polymarket_wallet(&w) {
        anyhow::bail!("invalid wallet address");
    }
    let market_norm = normalize_condition_id(q.market.trim());
    if market_norm.is_empty() {
        anyhow::bail!("market (condition id) is required");
    }

    let timeout = Duration::from_secs(cfg.timeout_sec.max(10));
    let http = Client::builder()
        .user_agent("polymarket-account-analyzer/0.1")
        .timeout(timeout)
        .connect_timeout(Duration::from_secs(15))
        .no_proxy()
        .build()?;
    let data = DataApiClient::new(http, Duration::from_millis(cfg.rate_limit_ms));
    let page_limit = cfg.trades_page_limit.clamp(1, 500);

    let (merged, incremental_from_cache) = if let Some(store) = storage {
        if q.no_cache {
            let full = data
                .activity_all_for_market(&w, &market_norm, page_limit, None)
                .await?;
            let max_t = max_activity_ts_sec(&full);
            store
                .upsert_wallet_market_activity_cache(&w, &market_norm, &full, max_t)
                .await?;
            (full, false)
        } else {
            match store
                .fetch_wallet_market_activity_cache(&w, &market_norm)
                .await?
            {
                Some((cached, max_ts)) => {
                    let start_ts = if max_ts > 0 { Some(max_ts + 1) } else { None };
                    let delta = data
                        .activity_all_for_market(&w, &market_norm, page_limit, start_ts)
                        .await?;
                    let merged = merge_activities_incremental(cached, delta);
                    let max_t = max_activity_ts_sec(&merged);
                    store
                        .upsert_wallet_market_activity_cache(&w, &market_norm, &merged, max_t)
                        .await?;
                    (merged, true)
                }
                None => {
                    let full = data
                        .activity_all_for_market(&w, &market_norm, page_limit, None)
                        .await?;
                    let max_t = max_activity_ts_sec(&full);
                    store
                        .upsert_wallet_market_activity_cache(&w, &market_norm, &full, max_t)
                        .await?;
                    (full, false)
                }
            }
        }
    } else {
        let full = data
            .activity_all_for_market(&w, &market_norm, page_limit, None)
            .await?;
        (full, false)
    };

    let max_ts = max_activity_ts_sec(&merged);

    Ok(serde_json::json!({
        "wallet": w,
        "market": market_norm,
        "activity_count": merged.len(),
        "max_timestamp_sec": max_ts,
        "incremental_from_cache": incremental_from_cache,
        "no_cache": q.no_cache,
        "postgres": storage.is_some(),
        "activities": merged,
    }))
}

/// Account-level report from `/positions` + `/closed-positions` + Gamma (no `/trades`).
fn build_analyze_report(
    cfg: &AppConfig,
    wallet: &str,
    truncated: bool,
    max_offset_allowed: Option<u32>,
    augment: &ReportAugment,
) -> AnalyzeReport {
    let mut market_keys: HashSet<String> = HashSet::new();
    for p in &augment.open_positions {
        let slug = p.slug.as_deref().unwrap_or("").trim();
        let cid = p.condition_id.as_deref().filter(|s| !s.trim().is_empty());
        let key = if !slug.is_empty() {
            slug.to_lowercase()
        } else if let Some(c) = cid {
            format!("cid:{}", normalize_condition_id(c))
        } else {
            continue;
        };
        market_keys.insert(key);
    }
    for cp in &augment.closed_positions {
        let slug = cp.slug.as_deref().unwrap_or("").trim();
        let cid = cp.condition_id.as_deref().filter(|s| !s.trim().is_empty());
        let key = if !slug.is_empty() {
            slug.to_lowercase()
        } else if let Some(cond) = cid {
            format!("cid:{}", normalize_condition_id(cond))
        } else {
            continue;
        };
        market_keys.insert(key);
    }
    let distinct_slugs_count = market_keys.len();
    let row_count = augment.open_positions.len() + augment.closed_positions.len();
    let fill_denom = row_count.max(1);

    let mut total_volume = 0.0_f64;
    let mut net_from_positions = 0.0_f64;
    let mut pnls: Vec<f64> = Vec::new();

    let mut dist: HashMap<String, (usize, f64, f64)> = HashMap::new();

    for p in &augment.open_positions {
        let sz = p.size.unwrap_or(0.0);
        let ap = p.avg_price.unwrap_or(0.0);
        let vol = sz * ap;
        total_volume += vol;
        let pnl = p.cash_pnl.unwrap_or(0.0);
        net_from_positions += pnl;
        pnls.push(pnl);

        let slug_raw = p.slug.clone().unwrap_or_default();
        let mt = augment
            .gamma_bucket_by_slug
            .get(&slug_raw)
            .map(|s| s.as_str())
            .unwrap_or_else(|| cfg.market_type.classify_slug(&slug_raw))
            .to_string();
        let e = dist.entry(mt).or_insert((0, 0.0, 0.0));
        e.0 += 1;
        e.1 += vol;
        e.2 += pnl;
    }
    for c in &augment.closed_positions {
        let sz = c.size.unwrap_or(0.0);
        let ap = c.avg_price.unwrap_or(0.0);
        let vol = sz * ap;
        total_volume += vol;
        let pnl = c.realized_pnl.unwrap_or(0.0);
        net_from_positions += pnl;
        pnls.push(pnl);

        let slug_raw = c.slug.clone().unwrap_or_default();
        let mt = augment
            .gamma_bucket_by_slug
            .get(&slug_raw)
            .map(|s| s.as_str())
            .unwrap_or_else(|| cfg.market_type.classify_slug(&slug_raw))
            .to_string();
        let e = dist.entry(mt).or_insert((0, 0.0, 0.0));
        e.0 += 1;
        e.1 += vol;
        e.2 += pnl;
    }

    let max_single_win = pnls.iter().cloned().filter(|x| *x > 0.0).fold(0.0_f64, f64::max);
    let max_single_loss = pnls
        .iter()
        .cloned()
        .filter(|x| *x < 0.0)
        .fold(0.0_f64, f64::min);

    let mut price_buckets: BTreeMap<String, usize> = BTreeMap::from([
        ("lt_0_1".to_string(), 0),
        ("0_1_to_0_3".to_string(), 0),
        ("0_3_to_0_5".to_string(), 0),
        ("0_5_to_0_7".to_string(), 0),
        ("0_7_to_0_9".to_string(), 0),
        ("gt_0_9".to_string(), 0),
    ]);
    for p in &augment.open_positions {
        let ap = p.avg_price.unwrap_or(0.0);
        let k = if ap < 0.1 {
            "lt_0_1"
        } else if ap < 0.3 {
            "0_1_to_0_3"
        } else if ap < 0.5 {
            "0_3_to_0_5"
        } else if ap < 0.7 {
            "0_5_to_0_7"
        } else if ap < 0.9 {
            "0_7_to_0_9"
        } else {
            "gt_0_9"
        };
        *price_buckets.entry(k.to_string()).or_default() += 1;
    }
    for c in &augment.closed_positions {
        let ap = c.avg_price.unwrap_or(0.0);
        let k = if ap < 0.1 {
            "lt_0_1"
        } else if ap < 0.3 {
            "0_1_to_0_3"
        } else if ap < 0.5 {
            "0_3_to_0_5"
        } else if ap < 0.7 {
            "0_5_to_0_7"
        } else if ap < 0.9 {
            "0_7_to_0_9"
        } else {
            "gt_0_9"
        };
        *price_buckets.entry(k.to_string()).or_default() += 1;
    }

    let lifetime_net_pnl = net_from_positions;
    let total_volume_nonzero = if total_volume == 0.0 { 1.0 } else { total_volume };
    let mut market_distribution: Vec<MarketDistributionItem> = dist
        .into_iter()
        .map(|(market_type, (cnt, vol, pnl))| MarketDistributionItem {
            market_type,
            trades: cnt,
            volume: vol,
            pnl,
            trades_pct: (cnt as f64 / fill_denom as f64) * 100.0,
            volume_pct: (vol / total_volume_nonzero) * 100.0,
        })
        .collect();
    market_distribution.sort_by(|a, b| b.volume.total_cmp(&a.volume));
    market_distribution.truncate(10);

    let active_hours_utc: BTreeMap<u32, usize> = (0..24u32).map(|h| (h, 0)).collect();
    let time_analysis = TimeAnalysis {
        active_hours_utc,
        entry_to_resolution_seconds: vec![],
        holding_duration_seconds: vec![],
        metadata_missing_ratio: 1.0,
        entry_to_resolution_p50_sec: None,
        entry_to_resolution_p90_sec: None,
    };

    let mut closed_wins = 0_usize;
    let mut closed_n = 0_usize;
    for c in &augment.closed_positions {
        if let Some(r) = c.realized_pnl {
            closed_n += 1;
            if r > 0.0 {
                closed_wins += 1;
            }
        }
    }
    let (win_rate_closed_positions, closed_positions_sample_size) = if closed_n >= 5 {
        (
            Some((closed_wins as f64 / closed_n as f64) * 100.0),
            Some(closed_n),
        )
    } else if closed_n > 0 {
        (None, Some(closed_n))
    } else {
        (None, None)
    };

    let side_bias = SideBias {
        buy_pct: 50.0,
        sell_pct: 50.0,
        yes_pct: 50.0,
        no_pct: 50.0,
    };
    let trading_patterns = TradingPatterns {
        grid_like_markets: 0,
        side_bias: side_bias.clone(),
        win_rate_overall: 0.0,
        win_rate_by_market_type: vec![],
        grid_like_market_ratio: Some(0.0),
        win_rate_closed_positions,
        closed_positions_sample_size,
    };

    let no_trades: &[Trade] = &[];
    let (primary_style, rule_json, pseudocode) = strategy::infer_strategy(StrategyInputs {
        market_distribution: &market_distribution,
        patterns: &trading_patterns,
        side_bias: &trading_patterns.side_bias,
        total_volume,
        trades_count: row_count,
        trades: no_trades,
        resolution_ts_by_slug: &augment.resolution_ts_by_slug,
    });

    let strategy_inference = StrategyInference {
        primary_style: primary_style.clone(),
        rule_json,
        pseudocode,
    };

    let price_buckets_chart = Some(vec![
        NormalizedPriceBucket {
            label: "<0.1".into(),
            range_low: 0.0,
            range_high: 0.1,
            count: *price_buckets.get("lt_0_1").unwrap_or(&0),
        },
        NormalizedPriceBucket {
            label: "0.1–0.3".into(),
            range_low: 0.1,
            range_high: 0.3,
            count: *price_buckets.get("0_1_to_0_3").unwrap_or(&0),
        },
        NormalizedPriceBucket {
            label: "0.3–0.5".into(),
            range_low: 0.3,
            range_high: 0.5,
            count: *price_buckets.get("0_3_to_0_5").unwrap_or(&0),
        },
        NormalizedPriceBucket {
            label: "0.5–0.7".into(),
            range_low: 0.5,
            range_high: 0.7,
            count: *price_buckets.get("0_5_to_0_7").unwrap_or(&0),
        },
        NormalizedPriceBucket {
            label: "0.7–0.9".into(),
            range_low: 0.7,
            range_high: 0.9,
            count: *price_buckets.get("0_7_to_0_9").unwrap_or(&0),
        },
        NormalizedPriceBucket {
            label: "≥0.9".into(),
            range_low: 0.9,
            range_high: 1.0,
            count: *price_buckets.get("gt_0_9").unwrap_or(&0),
        },
    ]);

    let open_position_value: f64 = augment
        .open_positions
        .iter()
        .filter_map(|p| p.current_value)
        .sum();
    let closed_realized_pnl_sum: f64 = augment
        .closed_positions
        .iter()
        .filter_map(|p| p.realized_pnl)
        .sum();
    let open_positions_count = augment.open_positions.len();

    AnalyzeReport {
        schema_version: REPORT_SCHEMA_VERSION.to_string(),
        analysis_pipeline: Some("account".to_string()),
        wallet: wallet.to_string(),
        trades_count: distinct_slugs_count,
        trades_fill_count: 0,
        total_volume,
        data_fetch: DataFetchMeta {
            truncated,
            max_offset_allowed,
        },
        lifetime: LifetimeMetrics {
            total_trades: distinct_slugs_count,
            total_volume,
            net_pnl: lifetime_net_pnl,
            net_pnl_settlement: 0.0,
            open_position_value,
            max_single_win,
            max_single_loss,
            closed_realized_pnl_sum: if augment.closed_positions.is_empty() {
                None
            } else {
                Some(closed_realized_pnl_sum)
            },
            open_positions_count: if open_positions_count == 0 {
                None
            } else {
                Some(open_positions_count)
            },
        },
        market_distribution,
        price_buckets,
        time_analysis,
        trading_patterns,
        strategy_inference,
        ingestion: None,
        subgraph: None,
        reconciliation: None,
        reconciliation_v1: None,
        canonical_summary: None,
        data_lineage: Some(DataLineage {
            analytics_primary_source: "data_api_positions".to_string(),
            canonical_merge_applied: false,
            markets_dim_enriched: false,
        }),
        provenance: Some(ReportProvenance::uniform(
            "data_api_positions",
            vec!["Account pipeline: metrics from /positions + /closed-positions (+ Gamma); analyze does not paginate /trades. Per-market fills: GET /position-activity/:wallet?market=.".into()],
        )),
        metrics_canonical_shadow: None,
        price_buckets_chart,
        frontend: None,
        gamma_profile: augment.gamma_profile.clone(),
        notes: vec![
            "Account rollup: from /positions + /closed-positions only (no /trades). lifetime.net_pnl = sum(open cashPnl + closed realizedPnl). net_pnl_settlement=0. Use GET /position-activity/:wallet?market= for per-market /activity.".into(),
            format!(
                "trades_count={} distinct markets (slug or condition_id); position rows={}.",
                distinct_slugs_count, row_count
            ),
        ],
        report_updated_at: Some(Utc::now().to_rfc3339()),
    }
}

fn build_report_from_trades(
    cfg: &AppConfig,
    wallet: &str,
    trades: &[Trade],
    truncated: bool,
    max_offset_allowed: Option<u32>,
    augment: &ReportAugment,
) -> AnalyzeReport {
    let trade_fills_count = trades.len();
    let distinct_slugs_count = trades
        .iter()
        .map(|t| {
            let s = t.slug.trim();
            if s.is_empty() {
                format!("cid:{}", t.condition_id.to_lowercase())
            } else {
                s.to_lowercase()
            }
        })
        .collect::<HashSet<_>>()
        .len();
    let fill_denom = trade_fills_count.max(1);
    let per_trade_pnl: Vec<f64> = trade_pnl::per_trade_realized_pnl(trades);
    let net_pnl_realized_trades: f64 = per_trade_pnl.iter().sum();

    let book = trade_pnl::outcome_book_after_trades(trades);
    let mut condition_to_slug: HashMap<String, String> = HashMap::new();
    let mut asset_to_slug: HashMap<String, String> = HashMap::new();
    for t in trades {
        if t.slug.trim().is_empty() {
            continue;
        }
        condition_to_slug
            .entry(normalize_condition_id(&t.condition_id))
            .or_insert_with(|| t.slug.clone());
        if let Some(ref a) = t.asset {
            let al = a.trim().to_lowercase();
            if !al.is_empty() {
                asset_to_slug
                    .entry(al)
                    .or_insert_with(|| t.slug.clone());
            }
        }
    }
    let settlement_legs_vec = settlement_legs_for_open_book(
        &book,
        &augment.gamma_resolution_by_slug,
        &condition_to_slug,
        &asset_to_slug,
    );
    let net_pnl_settlement: f64 = settlement_legs_vec.iter().map(|l| l.pnl).sum();
    let max_settlement_win = settlement_legs_vec
        .iter()
        .map(|l| l.pnl)
        .fold(0.0_f64, f64::max);
    let mut min_settlement_loss = 0.0_f64;
    for l in &settlement_legs_vec {
        if l.pnl < min_settlement_loss {
            min_settlement_loss = l.pnl;
        }
    }
    let lifetime_net_pnl = net_pnl_realized_trades + net_pnl_settlement;

    let mut total_volume = 0.0_f64;
    let max_single_win;
    let max_single_loss;
    let mut price_buckets: BTreeMap<String, usize> = BTreeMap::from([
        ("lt_0_1".to_string(), 0),
        ("0_1_to_0_3".to_string(), 0),
        ("0_3_to_0_5".to_string(), 0),
        ("0_5_to_0_7".to_string(), 0),
        ("0_7_to_0_9".to_string(), 0),
        ("gt_0_9".to_string(), 0),
    ]);

    let mut dist: HashMap<String, (usize, f64, f64)> = HashMap::new();
    let mut trades_per_market: HashMap<String, usize> = HashMap::new();
    let mut buy_count = 0_usize;
    let mut sell_count = 0_usize;
    let mut yes_count = 0_usize;
    let mut no_count = 0_usize;
    let mut active_hours_utc: BTreeMap<u32, usize> = (0..24u32).map(|h| (h, 0)).collect();
    let mut wins_by_market: HashMap<String, (usize, usize)> = HashMap::new();

    for t in trades {
        let volume = t.size * t.price;
        total_volume += volume;

        let cash_flow = match t.side {
            TradeSide::Buy => -volume,
            TradeSide::Sell => volume,
        };

        match t.side {
            TradeSide::Buy => buy_count += 1,
            TradeSide::Sell => sell_count += 1,
        }
        if let Some(ref o) = t.outcome {
            let o_lower = o.to_lowercase();
            if o_lower.contains("yes") {
                yes_count += 1;
            } else if o_lower.contains("no") {
                no_count += 1;
            }
        }

        let ts_sec = trade_ts_sec(t);
        if let Some(dt) = DateTime::from_timestamp(ts_sec, 0) {
            let hour = dt.with_timezone(&Utc).hour();
            *active_hours_utc.entry(hour).or_insert(0) += 1;
        }

        let bucket_key = if t.price < 0.1 {
            "lt_0_1"
        } else if t.price < 0.3 {
            "0_1_to_0_3"
        } else if t.price < 0.5 {
            "0_3_to_0_5"
        } else if t.price < 0.7 {
            "0_5_to_0_7"
        } else if t.price < 0.9 {
            "0_7_to_0_9"
        } else {
            "gt_0_9"
        };
        *price_buckets.entry(bucket_key.to_string()).or_default() += 1;

        let market_type = augment
            .gamma_bucket_by_slug
            .get(&t.slug)
            .map(|s| s.as_str())
            .unwrap_or_else(|| cfg.market_type.classify_slug(&t.slug))
            .to_string();
        let entry = dist.entry(market_type.clone()).or_insert((0, 0.0, 0.0));
        entry.0 += 1;
        entry.1 += volume;
        entry.2 += cash_flow;

        *trades_per_market.entry(t.slug.clone()).or_insert(0) += 1;

        let win = cash_flow > 0.0;
        let w = wins_by_market.entry(market_type).or_insert((0, 0));
        if win {
            w.0 += 1;
        }
        w.1 += 1;
    }

    max_single_win = per_trade_pnl
        .iter()
        .cloned()
        .filter(|x| *x > 0.0)
        .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap_or(0.0)
        .max(max_settlement_win);
    max_single_loss = per_trade_pnl
        .iter()
        .cloned()
        .filter(|x| *x < 0.0)
        .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap_or(0.0)
        .min(min_settlement_loss);

    let total_volume_nonzero = if total_volume == 0.0 { 1.0 } else { total_volume };
    let mut market_distribution: Vec<MarketDistributionItem> = dist
        .into_iter()
        .map(|(market_type, (cnt, vol, pnl))| MarketDistributionItem {
            market_type,
            trades: cnt,
            volume: vol,
            pnl,
            trades_pct: (cnt as f64 / fill_denom as f64) * 100.0,
            volume_pct: (vol / total_volume_nonzero) * 100.0,
        })
        .collect();
    market_distribution.sort_by(|a, b| b.volume.total_cmp(&a.volume));
    market_distribution.truncate(10);

    let grid_like_markets = trades_per_market.values().filter(|&&c| c > 3).count();
    let total_side = (buy_count + sell_count).max(1);
    let total_outcome = (yes_count + no_count).max(1);
    let side_bias = SideBias {
        buy_pct: (buy_count as f64 / total_side as f64) * 100.0,
        sell_pct: (sell_count as f64 / total_side as f64) * 100.0,
        yes_pct: (yes_count as f64 / total_outcome as f64) * 100.0,
        no_pct: (no_count as f64 / total_outcome as f64) * 100.0,
    };

    let win_count: usize = wins_by_market.values().map(|(w, _)| *w).sum();
    let win_rate_overall = if trades.is_empty() {
        0.0
    } else {
        (win_count as f64 / trades.len() as f64) * 100.0
    };
    let win_rate_by_market_type: Vec<WinRateByMarketType> = wins_by_market
        .into_iter()
        .map(|(market_type, (wins, total))| WinRateByMarketType {
            market_type,
            win_rate: if total == 0 { 0.0 } else { (wins as f64 / total as f64) * 100.0 },
        })
        .collect();

    let distinct_markets = trades_per_market.len().max(1);
    let grid_ratio = grid_like_markets as f64 / distinct_markets as f64;

    let mut entry_to_resolution_seconds: Vec<i64> = Vec::new();
    for t in trades {
        let te = trade_ts_sec(t);
        if let Some(&res) = augment.resolution_ts_by_slug.get(&t.slug) {
            let d = res - te;
            if d > 0 && d < 86400 * 90 {
                entry_to_resolution_seconds.push(d);
            }
        }
    }
    entry_to_resolution_seconds.sort_unstable();
    let entry_p50 = percentile_i64_sorted(&entry_to_resolution_seconds, 0.5);
    let entry_p90 = percentile_i64_sorted(&entry_to_resolution_seconds, 0.9);
    let metadata_missing_ratio = if trades.is_empty() {
        1.0
    } else {
        1.0 - (entry_to_resolution_seconds.len() as f64 / trades.len() as f64).clamp(0.0, 1.0)
    };

    let time_analysis = TimeAnalysis {
        active_hours_utc,
        entry_to_resolution_seconds: entry_to_resolution_seconds.clone(),
        holding_duration_seconds: vec![],
        metadata_missing_ratio,
        entry_to_resolution_p50_sec: entry_p50,
        entry_to_resolution_p90_sec: entry_p90,
    };

    let mut closed_wins = 0_usize;
    let mut closed_n = 0_usize;
    for c in &augment.closed_positions {
        if let Some(r) = c.realized_pnl {
            closed_n += 1;
            if r > 0.0 {
                closed_wins += 1;
            }
        }
    }
    let (win_rate_closed_positions, closed_positions_sample_size) = if closed_n >= 5 {
        (
            Some((closed_wins as f64 / closed_n as f64) * 100.0),
            Some(closed_n),
        )
    } else if closed_n > 0 {
        (None, Some(closed_n))
    } else {
        (None, None)
    };

    let trading_patterns = TradingPatterns {
        grid_like_markets,
        side_bias,
        win_rate_overall,
        win_rate_by_market_type,
        grid_like_market_ratio: Some(grid_ratio),
        win_rate_closed_positions,
        closed_positions_sample_size,
    };

    let (primary_style, rule_json, pseudocode) = strategy::infer_strategy(StrategyInputs {
        market_distribution: &market_distribution,
        patterns: &trading_patterns,
        side_bias: &trading_patterns.side_bias,
        total_volume,
        trades_count: trades.len(),
        trades,
        resolution_ts_by_slug: &augment.resolution_ts_by_slug,
    });

    let strategy_inference = StrategyInference {
        primary_style: primary_style.clone(),
        rule_json,
        pseudocode,
    };

    let price_buckets_chart = Some(vec![
        NormalizedPriceBucket {
            label: "<0.1".into(),
            range_low: 0.0,
            range_high: 0.1,
            count: *price_buckets.get("lt_0_1").unwrap_or(&0),
        },
        NormalizedPriceBucket {
            label: "0.1–0.3".into(),
            range_low: 0.1,
            range_high: 0.3,
            count: *price_buckets.get("0_1_to_0_3").unwrap_or(&0),
        },
        NormalizedPriceBucket {
            label: "0.3–0.5".into(),
            range_low: 0.3,
            range_high: 0.5,
            count: *price_buckets.get("0_3_to_0_5").unwrap_or(&0),
        },
        NormalizedPriceBucket {
            label: "0.5–0.7".into(),
            range_low: 0.5,
            range_high: 0.7,
            count: *price_buckets.get("0_5_to_0_7").unwrap_or(&0),
        },
        NormalizedPriceBucket {
            label: "0.7–0.9".into(),
            range_low: 0.7,
            range_high: 0.9,
            count: *price_buckets.get("0_7_to_0_9").unwrap_or(&0),
        },
        NormalizedPriceBucket {
            label: "≥0.9".into(),
            range_low: 0.9,
            range_high: 1.0,
            count: *price_buckets.get("gt_0_9").unwrap_or(&0),
        },
    ]);

    let trade_ledger = build_trade_ledger(
        trades,
        &per_trade_pnl,
        &settlement_legs_vec,
        &augment.resolution_ts_by_slug,
    );
    let trade_ledger_paired = trade_ledger_paired_from_full(&trade_ledger);
    let trade_ledger_integrity = {
        let i = compute_trade_ledger_integrity(&trade_ledger, &trade_ledger_paired);
        if !i.integrity_ok {
            tracing::warn!(?i, "trade_ledger_integrity check failed");
        }
        Some(i)
    };
    let frontend = Some(build_frontend_presentation(
        wallet,
        trades,
        &per_trade_pnl,
        &augment.open_positions,
        &primary_style,
        win_rate_overall,
        total_volume,
        trade_ledger,
        trade_ledger_paired,
        trade_ledger_integrity,
    ));

    let open_position_value: f64 = augment
        .open_positions
        .iter()
        .filter_map(|p| p.current_value)
        .sum();
    let closed_realized_pnl_sum: f64 = augment
        .closed_positions
        .iter()
        .filter_map(|p| p.realized_pnl)
        .sum();
    let open_positions_count = augment.open_positions.len();

    AnalyzeReport {
        schema_version: REPORT_SCHEMA_VERSION.to_string(),
        analysis_pipeline: None,
        wallet: wallet.to_string(),
        trades_count: distinct_slugs_count,
        trades_fill_count: trade_fills_count,
        total_volume,
        data_fetch: DataFetchMeta {
            truncated,
            max_offset_allowed,
        },
        lifetime: LifetimeMetrics {
            total_trades: distinct_slugs_count,
            total_volume,
            net_pnl: lifetime_net_pnl,
            net_pnl_settlement,
            open_position_value,
            max_single_win,
            max_single_loss,
            closed_realized_pnl_sum: if augment.closed_positions.is_empty() {
                None
            } else {
                Some(closed_realized_pnl_sum)
            },
            open_positions_count: if open_positions_count == 0 {
                None
            } else {
                Some(open_positions_count)
            },
        },
        market_distribution,
        price_buckets,
        time_analysis,
        trading_patterns,
        strategy_inference,
        ingestion: None,
        subgraph: None,
        reconciliation: None,
        reconciliation_v1: None,
        canonical_summary: None,
        data_lineage: None,
        provenance: None,
        metrics_canonical_shadow: None,
        price_buckets_chart,
        frontend,
        gamma_profile: augment.gamma_profile.clone(),
        notes: {
            let mut notes = vec![
                "lifetime.net_pnl = per-trade SELL-realized (avg-cost) + lifetime.net_pnl_settlement (Gamma outcomePrices on remaining inventory at resolved markets; BUY-only legs included). Capped by max_gamma_slugs_for_timing. closed_realized_pnl_sum / open_position_value cross-checks unchanged; entry P50/P90 use Gamma times.".into(),
                format!(
                    "trades_count (2.4+)=distinct markets (unique slug, else condition_id), aligned with Polymarket user-stats.trades; trades_fill_count={} is Data API /trades row count.",
                    trade_fills_count
                ),
            ];
            if truncated {
                notes.push(format!(
                    "Data API trades were truncated due to upstream historical offset limit (max_offset_allowed={})",
                    max_offset_allowed
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "unknown".to_string())
                ));
            }
            if !settlement_legs_vec.is_empty() {
                notes.push(format!(
                    "net_pnl_settlement: {} ({} resolved open leg(s) matched via Gamma outcomePrices).",
                    net_pnl_settlement,
                    settlement_legs_vec.len()
                ));
            }
            notes.push(
                "frontend.trade_ledger: chronological rows (BUY/SELL per Data API fill; SETTLEMENT = resolve payout on remaining shares). Sum of `pnl` ≈ lifetime.net_pnl when fills + settlements complete the book.".into(),
            );
            notes.push(
                "frontend.trade_ledger_paired (2.5.3+): only SELL + SETTLEMENT rows from the same ledger; each row has realized `pnl`. BUY opens are folded into avg cost (same model as per_trade_realized_pnl). Not strict FIFO lot pairing.".into(),
            );
            notes.push(
                "frontend.trade_ledger_integrity (2.5.4+): row counts + sum(pnl) prove paired view is a BUY-filter of full ledger; no fills removed from trade_ledger.".into(),
            );
            notes.push(
                "market_distribution (2.5.5+): primary bucket from Gamma GET /markets/slug?include_tag=true (category + tags) with /tags + /sports id→label/sport maps; slug [market_type] rules are fallback only. Slugs beyond max_gamma_slugs_for_timing fall back to slug rules.".into(),
            );
            notes
        },
        report_updated_at: Some(Utc::now().to_rfc3339()),
    }
}

async fn serve(cfg: AppConfig, bind: String) -> anyhow::Result<()> {
    use axum::{
        extract::{Path, Query, State},
        http::{HeaderValue, Method, StatusCode},
        response::IntoResponse,
        routing::get,
        Json, Router,
    };
    use tower_http::cors::{Any, CorsLayer};

    let storage = init_storage(&cfg).await?;

    let gamma_http = reqwest::Client::builder()
        .user_agent(concat!("polymarket-account-analyzer/", env!("CARGO_PKG_VERSION")))
        .timeout(std::time::Duration::from_secs(cfg.timeout_sec.max(15)))
        .connect_timeout(std::time::Duration::from_secs(30))
        .no_proxy()
        .build()?;
    let gamma_client = GammaApiClient::new(
        gamma_http,
        std::time::Duration::from_millis(cfg.rate_limit_ms.max(1)),
    );

    #[derive(Clone)]
    struct AppState {
        cfg: AppConfig,
        storage: Option<Storage>,
        gamma: GammaApiClient,
    }

    let state = AppState {
        cfg: cfg.clone(),
        storage,
        gamma: gamma_client,
    };

    async fn analyze_handler(
        Path(wallet): Path<String>,
        Query(q): Query<AnalyzeQuery>,
        State(st): State<AppState>,
    ) -> impl IntoResponse {
        let cfg = effective_config(&st.cfg, &AnalyzeOverrides::from(&q));
        if q.cached_only {
            if q.no_cache {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": "cached_only is incompatible with no_cache"
                    })),
                )
                    .into_response();
            }
            let Some(ref store) = st.storage else {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({ "error": "cache_miss" })),
                )
                    .into_response();
            };
            let cache_key = report_cache_key(&wallet, &cfg);
            match store
                .get_cached_report(&cache_key, &wallet, cfg.cache_ttl_sec as i64)
                .await
            {
                Ok(Some(report)) => (StatusCode::OK, Json(report)).into_response(),
                // 204：缓存未命中，避免浏览器把「正常 miss」记成 404 失败请求
                Ok(None) => StatusCode::NO_CONTENT.into_response(),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": format!("{:#}", e) })),
                )
                    .into_response(),
            }
        } else {
            match analyze_wallet(&cfg, st.storage.as_ref(), &wallet, q.no_cache).await {
                Ok(report) => (StatusCode::OK, Json(report)).into_response(),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({
                        "error": format!("{:#}", e)
                    })),
                )
                    .into_response(),
            }
        }
    }

    async fn leaderboard_handler(
        Query(q): Query<LeaderboardQuery>,
        State(st): State<AppState>,
    ) -> impl IntoResponse {
        let limit = q.limit.unwrap_or(30).clamp(1, 100);
        let period = q
            .period
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("all")
            .to_ascii_lowercase();
        let Some(ref store) = st.storage else {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({
                    "error": "leaderboard requires postgres (DATABASE_URL)"
                })),
            )
                .into_response();
        };

        use chrono::{Duration, TimeZone, Utc};

        let res = if period == "all" || period == "lifetime" {
            store.fetch_leaderboard(limit).await
        } else {
            let cutoff_ms: Option<i64> = match period.as_str() {
                "today" | "day" | "daily" => {
                    let nd = Utc::now().date_naive();
                    nd.and_hms_opt(0, 0, 0)
                        .map(|t| Utc.from_utc_datetime(&t).timestamp_millis())
                }
                "week" | "weekly" => {
                    let t = Utc::now() - Duration::days(7);
                    Some(t.timestamp_millis())
                }
                "month" | "monthly" => {
                    let t = Utc::now() - Duration::days(30);
                    Some(t.timestamp_millis())
                }
                _ => None,
            };
            match cutoff_ms {
                Some(cm) => store.fetch_leaderboard_since_trade_ts(limit, cm).await,
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({
                            "error": format!(
                                "invalid period '{period}'; use all, today, week, or month"
                            )
                        })),
                    )
                        .into_response();
                }
            }
        };

        match res {
            Ok(rows) => (
                StatusCode::OK,
                Json(serde_json::json!({ "items": rows, "period": period })),
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": format!("{:#}", e) })),
            )
                .into_response(),
        }
    }

    async fn health_handler() -> &'static str {
        // 快速探活：不访问数据库与外部 API；`/analyze` 可能耗时数分钟。
        "ok"
    }

    /// 部署验收：返回**当前二进制**将写入报告的 `schema_version`（与完整跑一遍 analyze 一致）。
    async fn version_handler() -> Json<serde_json::Value> {
        Json(serde_json::json!({
            "ok": true,
            "report_schema_version": REPORT_SCHEMA_VERSION,
            "package_version": env!("CARGO_PKG_VERSION"),
        }))
    }

    /// 浏览器同源拉 Gamma 会 CORS；由 **Rust** 代拉（与 `/analyze` 相同出网路径，无需本机 VPN/HTTPS_PROXY）。
    async fn gamma_public_profile_handler(
        Path(wallet): Path<String>,
        State(st): State<AppState>,
    ) -> impl IntoResponse {
        let w = wallet.trim().to_lowercase();
        if !is_valid_polymarket_wallet(&w) {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "invalid_address" })),
            )
                .into_response();
        }
        match st.gamma.fetch_public_profile_response(&w).await {
            Ok(resp) => {
                let status =
                    StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
                let ct = resp
                    .headers()
                    .get(axum::http::header::CONTENT_TYPE)
                    .and_then(|v| HeaderValue::from_bytes(v.as_bytes()).ok());
                let body = match resp.bytes().await {
                    Ok(b) => b,
                    Err(e) => {
                        return (
                            StatusCode::BAD_GATEWAY,
                            Json(serde_json::json!({
                                "error": format!("gamma public-profile read body: {e:#}")
                            })),
                        )
                            .into_response();
                    }
                };
                let mut res = axum::response::Response::new(axum::body::Body::from(body.to_vec()));
                *res.status_mut() = status;
                if let Some(ct) = ct {
                    res.headers_mut().insert(axum::http::header::CONTENT_TYPE, ct);
                }
                res.into_response()
            }
            Err(e) => (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({
                    "error": format!("gamma public-profile fetch: {e:#}")
                })),
            )
                .into_response(),
        }
    }

    async fn position_activity_handler(
        Path(wallet): Path<String>,
        Query(q): Query<PositionActivityQuery>,
        State(st): State<AppState>,
    ) -> impl IntoResponse {
        if q.market.trim().is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": "missing market query param" })),
            )
                .into_response();
        }
        match fetch_position_activity_json(&st.cfg, st.storage.as_ref(), &wallet, &q).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(e) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": format!("{e:#}") })),
            )
                .into_response(),
        }
    }

    let mut app = Router::new()
        .route("/health", get(health_handler))
        .route("/version", get(version_handler))
        .route("/gamma-public-profile/:wallet", get(gamma_public_profile_handler))
        .route("/analyze/:wallet", get(analyze_handler))
        .route("/position-activity/:wallet", get(position_activity_handler))
        .route("/leaderboard", get(leaderboard_handler))
        .with_state(state);

    let cors_origins: Vec<HeaderValue> = cfg
        .cors_allowed_origins
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.parse::<HeaderValue>().ok())
        .collect();

    if !cors_origins.is_empty() {
        let layer = CorsLayer::new()
            .allow_origin(tower_http::cors::AllowOrigin::list(cors_origins))
            .allow_methods([Method::GET, Method::OPTIONS])
            .allow_headers(Any);
        app = app.layer(layer);
    }

    tracing::info!("listening on {}", bind.as_str());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
