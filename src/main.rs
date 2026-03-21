use chrono::{DateTime, Timelike, Utc};
use clap::{Args, Parser, Subcommand};
use polymarket_account_analyzer::{
    canonical::{
        build_canonical_merge, compute_shadow_metrics, normalize_condition_id,
        slug_map_from_canonical_events, synthetic_trades_from_events, CanonicalPipelineParams,
    },
    config::{apply_env_overrides, load_config, report_cache_key, AppConfig, ReconciliationConfig},
    polymarket::{
        data_api::{
            merge_trades_incremental, trade_timestamp_ms, wallet_trades_watermark_ms,
            ClosedPosition, DataApiClient,
            Trade, TradeSide, TradesAllResult, UserPosition,
        },
        gamma_api::GammaApiClient,
        subgraph::{fetch_subgraph_for_wallet, SubgraphWalletParams},
    },
    reconciliation::{
        extract_fills_rows, extract_redemption_rows, json_value_ts_secs, reconcile_v0,
        shadow_volume_discrepancy_note, subgraph_fetch_failed_summary, summary_from_v1,
        v1_coverage_alert_notes, ReconciliationSummary,
    },
    report::{
        AnalyzeReport, CanonicalSummary, DataApiTruncationMeta, DataFetchMeta, DataLineage,
        FrontendPresentation, GammaProfileSummary, IngestionMeta, IngestionTruncation,
        LifetimeMetrics, MarketDistributionItem, NormalizedPriceBucket, PositionRowDisplay,
        ReportProvenance, SideBias, StrategyInference, TimeAnalysis, TradeHighlight,
        TradingPatterns, WinRateByMarketType,
    },
    storage::{Storage, WalletPipelineSnapshotMeta},
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

fn percentile_i64_sorted(sorted: &[i64], p: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p.clamp(0.0, 1.0)).round() as usize;
    Some(sorted[idx.min(sorted.len() - 1)] as f64)
}

async fn fetch_resolution_times_for_trades(
    gamma: &GammaApiClient,
    trades: &[Trade],
    max_slugs: usize,
) -> HashMap<String, i64> {
    if max_slugs == 0 {
        return HashMap::new();
    }
    let slugs_set: HashSet<String> = trades.iter().map(|t| t.slug.clone()).collect();
    let mut slugs: Vec<String> = slugs_set.into_iter().collect();
    slugs.sort();
    let mut out = HashMap::new();
    for slug in slugs.into_iter().take(max_slugs) {
        match gamma.market_by_slug(&slug).await {
            Ok(m) => {
                let ts = m
                    .closed_time
                    .as_deref()
                    .and_then(parse_gamma_datetime_utc)
                    .or_else(|| m.end_date.as_deref().and_then(parse_gamma_datetime_utc));
                if let Some(dt) = ts {
                    out.insert(slug, dt.timestamp());
                }
            }
            Err(e) => tracing::debug!("gamma resolution time skip slug={slug} err={e:#}"),
        }
    }
    out
}

fn build_frontend_presentation(
    wallet: &str,
    trades: &[Trade],
    per_trade_pnl: &[f64],
    open: &[UserPosition],
    primary_style: &str,
    win_rate: f64,
    total_vol: f64,
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
    /// Enable subgraph fetch for this run (overrides `[subgraph].enabled = false`).
    #[arg(long, default_value_t = false)]
    with_subgraph: bool,
    /// Disable subgraph for this run.
    #[arg(long, default_value_t = false)]
    no_subgraph: bool,
    /// Cap each subgraph stream to N rows (0 = use config only).
    #[arg(long)]
    subgraph_cap_rows: Option<u32>,
    /// Enable v0 reconciliation (overrides `[reconciliation].enabled = false`).
    #[arg(long, default_value_t = false)]
    with_reconciliation: bool,
    /// Disable reconciliation for this run.
    #[arg(long, default_value_t = false)]
    no_reconciliation: bool,
    /// Skip writing raw chunks to Postgres for this run.
    #[arg(long, default_value_t = false)]
    no_persist_raw: bool,
    /// Enable canonical merge + Postgres persistence (requires `persist_raw` + DB).
    #[arg(long, default_value_t = false)]
    with_canonical: bool,
    #[arg(long, default_value_t = false)]
    no_canonical: bool,
}

#[derive(Debug, Clone, Default)]
struct AnalyzeOverrides {
    with_subgraph: bool,
    no_subgraph: bool,
    subgraph_cap_rows: Option<u32>,
    with_reconciliation: bool,
    no_reconciliation: bool,
    no_persist_raw: bool,
    with_canonical: bool,
    no_canonical: bool,
}

impl From<&AnalyzeCliFlags> for AnalyzeOverrides {
    fn from(f: &AnalyzeCliFlags) -> Self {
        Self {
            with_subgraph: f.with_subgraph,
            no_subgraph: f.no_subgraph,
            subgraph_cap_rows: f.subgraph_cap_rows,
            with_reconciliation: f.with_reconciliation,
            no_reconciliation: f.no_reconciliation,
            no_persist_raw: f.no_persist_raw,
            with_canonical: f.with_canonical,
            no_canonical: f.no_canonical,
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct AnalyzeQuery {
    #[serde(default)]
    no_cache: bool,
    #[serde(default)]
    with_subgraph: bool,
    #[serde(default)]
    no_subgraph: bool,
    #[serde(default)]
    subgraph_cap_rows: Option<u32>,
    #[serde(default)]
    with_reconciliation: bool,
    #[serde(default)]
    no_reconciliation: bool,
    #[serde(default)]
    no_persist_raw: bool,
    #[serde(default)]
    with_canonical: bool,
    #[serde(default)]
    no_canonical: bool,
}

#[derive(Debug, serde::Deserialize)]
struct LeaderboardQuery {
    /// 1–100, default 30.
    #[serde(default)]
    limit: Option<i64>,
}

impl From<&AnalyzeQuery> for AnalyzeOverrides {
    fn from(q: &AnalyzeQuery) -> Self {
        Self {
            with_subgraph: q.with_subgraph,
            no_subgraph: q.no_subgraph,
            subgraph_cap_rows: q.subgraph_cap_rows,
            with_reconciliation: q.with_reconciliation,
            no_reconciliation: q.no_reconciliation,
            no_persist_raw: q.no_persist_raw,
            with_canonical: q.with_canonical,
            no_canonical: q.no_canonical,
        }
    }
}

fn effective_config(base: &AppConfig, o: &AnalyzeOverrides) -> AppConfig {
    let mut c = base.clone();
    if o.no_subgraph {
        c.subgraph.enabled = false;
    } else if o.with_subgraph {
        c.subgraph.enabled = true;
    }
    if let Some(cap) = o.subgraph_cap_rows {
        c.subgraph.cap_rows_per_stream = cap;
    }
    if o.no_reconciliation {
        c.reconciliation.enabled = false;
    } else if o.with_reconciliation {
        c.reconciliation.enabled = true;
    }
    if o.no_persist_raw {
        c.ingestion.persist_raw = false;
    }
    if o.no_canonical {
        c.canonical.enabled = false;
    } else if o.with_canonical {
        c.canonical.enabled = true;
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

    let mut report = build_report(
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
                    let mut refreshed_trades_on_hit = false;
                    if cfg.ingestion.data_api_incremental_trades {
                        let prev = store
                            .fetch_wallet_primary_trades(&w)
                            .await
                            .unwrap_or_default();
                        if let Some(wm) = wallet_trades_watermark_ms(&prev) {
                            let timeout = Duration::from_secs(cfg.timeout_sec.max(10));
                            let http_hit = Client::builder()
                                .user_agent("polymarket-account-analyzer/0.1")
                                .timeout(timeout)
                                .connect_timeout(Duration::from_secs(15))
                                .no_proxy()
                                .build()?;
                            let rate_hit = Duration::from_millis(cfg.rate_limit_ms);
                            let data_hit = DataApiClient::new(http_hit, rate_hit);
                            if let Ok((delta, _, _)) = data_hit
                                .trades_since_watermark(
                                    wallet,
                                    cfg.trades_page_limit,
                                    wm,
                                    cfg.ingestion.data_api_incremental_max_pages,
                                )
                                .await
                            {
                                if !delta.is_empty() {
                                    let merged = merge_trades_incremental(prev, delta);
                                    let pnls = trade_pnl::per_trade_realized_pnl(&merged);
                                    let canon = store
                                        .fetch_wallet_canonical_event_rows(&w)
                                        .await
                                        .unwrap_or_default();
                                    let canon_opt = if canon.is_empty() {
                                        None
                                    } else {
                                        Some(canon.as_slice())
                                    };
                                    let snap_meta = WalletPipelineSnapshotMeta {
                                        last_ingestion_run_id: cached
                                            .ingestion
                                            .as_ref()
                                            .and_then(|i| i.run_id.clone()),
                                        analytics_lane: cached
                                            .data_lineage
                                            .as_ref()
                                            .map(|d| d.analytics_primary_source.clone())
                                            .unwrap_or_else(|| "data_api_trades".to_string()),
                                        cache_key: cache_key.clone(),
                                        schema_version: cached.schema_version.clone(),
                                        data_api_truncated: cached.data_fetch.truncated,
                                        data_api_max_offset_allowed: cached
                                            .data_fetch
                                            .max_offset_allowed
                                            .and_then(|x| i32::try_from(x).ok()),
                                        data_api_trade_watermark_ms: merged
                                            .iter()
                                            .map(|t| trade_timestamp_ms(t.timestamp))
                                            .max(),
                                    };
                                    if let Err(e) = store
                                        .replace_wallet_pipeline_snapshots(
                                            &w,
                                            &snap_meta,
                                            &merged,
                                            &pnls,
                                            canon_opt,
                                            &cached,
                                        )
                                        .await
                                    {
                                        tracing::warn!(
                                            "replace_wallet_pipeline_snapshots (cache-hit incremental): {e:#}"
                                        );
                                    } else {
                                        refreshed_trades_on_hit = true;
                                    }
                                }
                            }
                        }
                    }
                    if !refreshed_trades_on_hit {
                        if let Err(e) = store
                            .refresh_wallet_snapshots_after_cache_hit(&w, &cache_key, &cached)
                            .await
                        {
                            tracing::warn!("refresh_wallet_snapshots_after_cache_hit: {e:#}");
                        }
                    }
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
        "subgraph": cfg.subgraph,
        "reconciliation": cfg.reconciliation,
        "ingestion": cfg.ingestion,
        "canonical": cfg.canonical,
        "analytics": cfg.analytics,
        "trades_page_limit": cfg.trades_page_limit,
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

    let trades_result: TradesAllResult =
        if cfg.ingestion.data_api_incremental_trades && storage.is_some() {
            let store = storage.expect("incremental trades implies storage");
            let prev = store
                .fetch_wallet_primary_trades(&w)
                .await
                .unwrap_or_default();
            if prev.is_empty() {
                data.trades_all(wallet, cfg.trades_page_limit).await?
            } else if let Some(wm) = wallet_trades_watermark_ms(&prev) {
                let (delta, inc_trunc, max_off) = data
                    .trades_since_watermark(
                        wallet,
                        cfg.trades_page_limit,
                        wm,
                        cfg.ingestion.data_api_incremental_max_pages,
                    )
                    .await?;
                let delta_count = delta.len();
                let merged = merge_trades_incremental(prev, delta);
                TradesAllResult {
                    trades: merged,
                    truncated: inc_trunc,
                    max_offset_allowed: max_off,
                    fetched_incrementally: true,
                    incremental_api_delta_count: delta_count,
                }
            } else {
                data.trades_all(wallet, cfg.trades_page_limit).await?
            }
        } else {
            data.trades_all(wallet, cfg.trades_page_limit).await?
        };

    if let (Some(store), Some(rid)) = (storage, run_id) {
        let step = cfg.trades_page_limit.max(1) as usize;
        for (i, chunk) in trades_result.trades.chunks(step).enumerate() {
            let payload = serde_json::to_value(chunk)?;
            store
                .insert_raw_chunk(&rid, wallet, "data_api_trades", i as i32, &payload)
                .await?;
            let page_offset = (i * step) as i32;
            for (j, t) in chunk.iter().enumerate() {
                let row_payload = serde_json::to_value(t)?;
                store
                    .insert_raw_data_api_trade(&rid, wallet, page_offset, j as i32, &row_payload)
                    .await?;
            }
        }
    }

    let gamma_timing = GammaApiClient::new(http.clone(), rate);
    let gamma_timing_cap = cfg.ingestion.max_gamma_slugs_for_timing as usize;
    let resolution_ts_by_slug = fetch_resolution_times_for_trades(
        &gamma_timing,
        &trades_result.trades,
        gamma_timing_cap,
    )
    .await;
    let gamma_profile = match gamma_timing.public_profile_by_address(wallet).await {
        Ok(p) => Some(GammaProfileSummary {
            display_name: p.name.clone().or_else(|| p.pseudonym.clone()),
            username: p.pseudonym.clone().or_else(|| p.x_username.clone()),
            avatar_url: p.profile_image,
        }),
        Err(e) => {
            tracing::debug!("gamma public-profile: {e:#}");
            None
        }
    };

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

    let augment = ReportAugment {
        resolution_ts_by_slug,
        open_positions,
        closed_positions,
        gamma_profile,
    };

    let mut slug_by_condition: HashMap<String, String> = HashMap::new();
    for t in &trades_result.trades {
        slug_by_condition
            .entry(normalize_condition_id(&t.condition_id))
            .or_insert_with(|| t.slug.clone());
    }

    if cfg.canonical.enrich_markets_dim {
        if let Some(store) = storage {
            let gamma = GammaApiClient::new(http.clone(), rate);
            let mut seen: HashSet<String> = HashSet::new();
            for t in &trades_result.trades {
                if !seen.insert(t.slug.clone()) {
                    continue;
                }
                match gamma.market_by_slug(&t.slug).await {
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
                                &t.slug,
                                m.question.as_deref(),
                                end_d,
                                closed_d,
                                &raw,
                            )
                            .await
                        {
                            tracing::warn!("markets_dim upsert slug={} err={:#}", t.slug, e);
                        }
                    }
                    Err(e) => tracing::debug!("gamma skip slug={} err={:#}", t.slug, e),
                }
            }
        }
    }

    let mut ingest_status = "ok";
    let mut subgraph_json: Option<serde_json::Value> = None;
    let mut subgraph_trunc = serde_json::json!({ "fetched": false });
    let mut maker_rows_mem: Vec<serde_json::Value> = Vec::new();
    let mut taker_rows_mem: Vec<serde_json::Value> = Vec::new();
    let mut redemption_rows_mem: Vec<serde_json::Value> = Vec::new();
    let mut position_rows_mem: Vec<serde_json::Value> = Vec::new();

    if cfg.subgraph.enabled {
        let p = subgraph_wallet_params(cfg);
        match fetch_subgraph_for_wallet(wallet, &p).await {
            Ok(sub) => {
                maker_rows_mem = extract_fills_rows(&sub.orderbook_order_filled_events_maker);
                taker_rows_mem = extract_fills_rows(&sub.orderbook_order_filled_events_taker);
                redemption_rows_mem = extract_redemption_rows(&sub.activity_redemptions);
                position_rows_mem = sub
                    .pnl_user_positions
                    .get("data")
                    .and_then(|d| d.get("userPositions"))
                    .and_then(|x| x.as_array())
                    .cloned()
                    .unwrap_or_default();

                subgraph_trunc = serde_json::json!({
                    "fetched": true,
                    "activity_redemptions": sub.activity_redemptions.get("_spike_pagination"),
                    "orderbook_maker": sub.orderbook_order_filled_events_maker.get("_spike_pagination"),
                    "orderbook_taker": sub.orderbook_order_filled_events_taker.get("_spike_pagination"),
                    "pnl_user_positions": sub.pnl_user_positions.get("_spike_pagination")
                        .or_else(|| sub.pnl_user_positions.get("_spike_error")),
                });

                if let (Some(store), Some(rid)) = (storage, run_id) {
                    let ps = cfg.subgraph.page_size.max(1) as usize;
                    let rows = redemption_rows_mem.clone();
                    persist_json_rows_chunks(store, &rid, wallet, "activity_redemptions", &rows, ps)
                        .await?;
                    for r in &rows {
                        let Some(eid) = r.get("id").and_then(|x| x.as_str()) else {
                            continue;
                        };
                        let ts = r
                            .get("timestamp")
                            .and_then(json_value_ts_secs)
                            .and_then(|s| DateTime::from_timestamp(s, 0));
                        store
                            .insert_raw_subgraph_redemption(&rid, wallet, eid, ts, r)
                            .await?;
                    }
                    let m = maker_rows_mem.clone();
                    persist_json_rows_chunks(store, &rid, wallet, "orderbook_fills_maker", &m, ps)
                        .await?;
                    for row in &m {
                        persist_raw_order_fill_row(store, &rid, wallet, "maker", row).await?;
                    }
                    let t = taker_rows_mem.clone();
                    persist_json_rows_chunks(store, &rid, wallet, "orderbook_fills_taker", &t, ps)
                        .await?;
                    for row in &t {
                        persist_raw_order_fill_row(store, &rid, wallet, "taker", row).await?;
                    }
                    for row in &position_rows_mem {
                        let Some(pid) = row.get("id").and_then(|x| x.as_str()) else {
                            continue;
                        };
                        store
                            .insert_raw_subgraph_user_position(&rid, wallet, pid, row)
                            .await?;
                    }
                    let pos_ps = cfg.subgraph.positions_page_size.max(1) as usize;
                    persist_json_rows_chunks(
                        store,
                        &rid,
                        wallet,
                        "pnl_user_positions",
                        &position_rows_mem,
                        pos_ps,
                    )
                    .await?;
                }

                subgraph_json = Some(serde_json::to_value(&sub)?);
                if sub.partial_failure {
                    ingest_status = "partial";
                }
            }
            Err(e) => {
                tracing::warn!("subgraph fetch failed: {e:#}");
                ingest_status = "partial";
                subgraph_trunc = serde_json::json!({
                    "fetched": false,
                    "error": format!("{e:#}"),
                });
                subgraph_json = Some(serde_json::json!({ "_fetch_error": format!("{e:#}") }));
            }
        }
    } else {
        subgraph_trunc = serde_json::json!({
            "fetched": false,
            "reason": "subgraph_disabled",
        });
    }

    let should_merge = cfg.analytics.source == "canonical"
        || cfg.analytics.canonical_shadow
        || (cfg.canonical.enabled && persist_raw && storage.is_some());

    let merge_opt = if should_merge {
        let pipe = CanonicalPipelineParams {
            time_window_sec: cfg.reconciliation.time_window_sec,
            size_tolerance_pct: cfg.reconciliation.size_tolerance_pct,
            price_tolerance_abs: cfg.reconciliation.price_tolerance_abs,
            require_condition_match: cfg.reconciliation.require_condition_match,
        };
        Some(build_canonical_merge(
            wallet,
            &trades_result.trades,
            &maker_rows_mem,
            &taker_rows_mem,
            &redemption_rows_mem,
            &position_rows_mem,
            &slug_by_condition,
            &pipe,
            &cfg.reconciliation.rules_version,
        ))
    } else {
        None
    };

    let owned_synth: Option<Vec<Trade>> = if cfg.analytics.source == "canonical" {
        merge_opt
            .as_ref()
            .map(|artifacts| synthetic_trades_from_events(&artifacts.events, &slug_by_condition))
    } else {
        None
    };

    let trades_for_report: &[Trade] = match &owned_synth {
        Some(s) if !s.is_empty() => s.as_slice(),
        _ => {
            if cfg.analytics.source == "canonical" && merge_opt.is_some() {
                tracing::warn!(
                    "analytics.source=canonical produced zero synthetic trades; falling back to data_api trades"
                );
            }
            trades_result.trades.as_slice()
        }
    };

    let mut report = build_report(
        cfg,
        wallet,
        trades_for_report,
        trades_result.truncated,
        trades_result.max_offset_allowed,
        &augment,
    );

    if trades_result.fetched_incrementally {
        report.notes.push(format!(
            "data_api_trades: incremental merge (api_new_rows≈{}, truncated={})",
            trades_result.incremental_api_delta_count,
            trades_result.truncated
        ));
    }

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
            truncated: trades_result.truncated,
            max_offset_allowed: trades_result.max_offset_allowed,
            trades_received: trades_result.trades.len(),
        },
        subgraph: subgraph_trunc,
    });
    report.ingestion = Some(im);

    report.subgraph = subgraph_json.clone();

    let markets_dim_enriched = cfg.canonical.enrich_markets_dim && storage.is_some();

    let analytics_src = if cfg.analytics.source == "canonical" {
        "canonical_synthetic_trades"
    } else {
        "data_api_trades"
    };

    let mut merge_persisted = false;
    if cfg.canonical.enabled {
        if !persist_raw || storage.is_none() || run_id.is_none() {
            tracing::warn!(
                "canonical.enabled requires Postgres + persist_raw + active ingestion run; skipping canonical persist"
            );
        } else if let Some(ref artifacts) = merge_opt {
            let store = storage.expect("storage");
            let rid = run_id.expect("run_id");
            if let Err(e) = store.persist_canonical_merge(&rid, wallet, artifacts).await {
                tracing::warn!("persist_canonical_merge failed: {e:#}");
                ingest_status = "partial";
            } else {
                merge_persisted = true;
            }
        }
    }

    if let Some(ref artifacts) = merge_opt {
        report.canonical_summary = Some(CanonicalSummary {
            enabled: true,
            run_id: run_id.map(|u| u.to_string()).unwrap_or_default(),
            rules_version: artifacts.report.rules_version.clone(),
            merged_trade_fills: artifacts.report.counts.matched,
            data_api_trade_only: artifacts.report.counts.api_only,
            subgraph_fill_only: artifacts.report.counts.subgraph_fill_only,
            redemptions: artifacts.report.counts.redemptions,
            position_snapshots: artifacts.report.counts.position_snapshots,
            ambiguous_queue_rows: artifacts.report.counts.ambiguous,
            canonical_events_total: artifacts.report.counts.canonical_total,
        });
    }

    if cfg.reconciliation.enabled {
        if let Some(ref artifacts) = merge_opt {
            let mut summ = summary_from_v1(
                &artifacts.report,
                cfg.reconciliation.time_window_sec,
            );
            for n in v1_coverage_alert_notes(
                &artifacts.report.counts,
                cfg.reconciliation.api_only_ratio_alert,
                cfg.reconciliation.api_only_alert_min,
            ) {
                summ.note.push('\n');
                summ.note.push_str(&n);
            }
            report.reconciliation_v1 = Some(artifacts.report.clone());
            report.reconciliation = Some(summ);
        } else if cfg.subgraph.enabled {
            if let Some(ref doc) = subgraph_json {
                if doc.get("_fetch_error").is_some() {
                    let msg = doc
                        .get("_fetch_error")
                        .and_then(|v| v.as_str())
                        .unwrap_or("unknown error");
                    report.reconciliation = Some(subgraph_fetch_failed_summary(
                        trades_result.trades.len(),
                        msg,
                    ));
                } else {
                    let m = doc
                        .get("orderbook_order_filled_events_maker")
                        .cloned()
                        .unwrap_or(serde_json::json!({"data":{"orderFilledEvents":[]}}));
                    let t = doc
                        .get("orderbook_order_filled_events_taker")
                        .cloned()
                        .unwrap_or(serde_json::json!({"data":{"orderFilledEvents":[]}}));
                    let r = doc
                        .get("activity_redemptions")
                        .cloned()
                        .unwrap_or(serde_json::json!({"data":{"redemptions":[]}}));
                    report.reconciliation = Some(reconcile_v0(
                        &trades_result.trades,
                        &m,
                        &t,
                        &r,
                        cfg.reconciliation.time_window_sec,
                    ));
                }
            }
        } else {
            report.reconciliation = Some(ReconciliationSummary {
                enabled: true,
                low_confidence: true,
                note: "Subgraph disabled; enable [subgraph].enabled or pass with_subgraph / ?with_subgraph=true."
                    .to_string(),
                ..Default::default()
            });
        }
    }

    if cfg.analytics.canonical_shadow {
        if let Some(ref artifacts) = merge_opt {
            let shadow = compute_shadow_metrics(&artifacts.events);
            let shadow_vol = shadow.total_volume;
            let note = format!(
                "shadow trade_like_volume={:.6} vs report.total_volume={:.6} (primary source: {})",
                shadow_vol,
                report.total_volume,
                analytics_src
            );
            report.metrics_canonical_shadow = Some(shadow);
            match &mut report.reconciliation_v1 {
                Some(v1) => v1.notes.push(note),
                None => {
                    let mut v1 = artifacts.report.clone();
                    v1.notes.push(note);
                    report.reconciliation_v1 = Some(v1);
                }
            }
            let primary_vol = report.total_volume;
            apply_shadow_volume_quality_alert(
                &mut report,
                &cfg.reconciliation,
                primary_vol,
                shadow_vol,
                analytics_src,
            );
        }
    }

    if merge_opt.is_some() {
        report.data_lineage = Some(DataLineage {
            analytics_primary_source: analytics_src.to_string(),
            canonical_merge_applied: merge_persisted,
            markets_dim_enriched,
        });
    }

    let mut prov_notes: Vec<String> = Vec::new();
    if trades_result.truncated {
        prov_notes.push("data_api_historical_truncation".to_string());
    }
    if ingest_status != "ok" {
        prov_notes.push(format!("ingestion_status:{ingest_status}"));
    }
    report.provenance = Some(ReportProvenance::uniform(
        analytics_src.to_string(),
        prov_notes.clone(),
    ));

    if let Some(ref mut im) = report.ingestion {
        im.status = ingest_status.to_string();
    }

    if let (Some(store), Some(rid)) = (storage, run_id) {
        store.finish_ingestion_run(&rid, ingest_status).await?;
    }

    if let Some(store) = storage {
        store.upsert_report(&cache_key, wallet, &report).await?;
        let pnls = trade_pnl::per_trade_realized_pnl(trades_for_report);
        store
            .replace_wallet_trade_pnls(&cache_key, wallet, trades_for_report, &pnls)
            .await?;
        store
            .upsert_leaderboard_row(wallet, &cache_key, &report)
            .await?;

        if cfg.ingestion.persist_wallet_snapshots {
            let w = wallet.to_lowercase();
            let snap_meta = WalletPipelineSnapshotMeta {
                last_ingestion_run_id: run_id.map(|u| u.to_string()),
                analytics_lane: analytics_src.to_string(),
                cache_key: cache_key.clone(),
                schema_version: report.schema_version.clone(),
                data_api_truncated: trades_result.truncated,
                data_api_max_offset_allowed: trades_result
                    .max_offset_allowed
                    .and_then(|x| i32::try_from(x).ok()),
                data_api_trade_watermark_ms: trades_for_report
                    .iter()
                    .map(|t| trade_timestamp_ms(t.timestamp))
                    .max(),
            };
            let canon_slice = merge_opt.as_ref().map(|m| m.events.as_slice());
            if let Err(e) = store
                .replace_wallet_pipeline_snapshots(
                    &w,
                    &snap_meta,
                    trades_for_report,
                    &pnls,
                    canon_slice,
                    &report,
                )
                .await
            {
                tracing::warn!("replace_wallet_pipeline_snapshots failed: {e:#}");
            }
        }
    }

    Ok(report)
}

fn subgraph_wallet_params(cfg: &AppConfig) -> SubgraphWalletParams {
    SubgraphWalletParams {
        activity_url: cfg.subgraph.activity_url.clone(),
        orderbook_url: cfg.subgraph.orderbook_url.clone(),
        pnl_url: cfg.subgraph.pnl_url.clone(),
        page_size: cfg.subgraph.page_size,
        max_pages: cfg.subgraph.max_pages,
        positions_page_size: cfg.subgraph.positions_page_size,
        skip_pnl_positions: cfg.subgraph.skip_pnl_positions,
        cap_rows: (cfg.subgraph.cap_rows_per_stream > 0).then_some(cfg.subgraph.cap_rows_per_stream),
        timeout: Duration::from_secs(cfg.subgraph.timeout_sec.max(30)),
        max_retries: cfg.subgraph.max_retries,
        pnl_max_retries: cfg.subgraph.pnl_max_retries,
        user_agent: cfg.subgraph.user_agent.clone(),
        show_progress: cfg.subgraph.show_progress,
        extended_fill_fields: cfg.subgraph.extended_fill_fields,
    }
}

fn parse_gamma_datetime_utc(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s.trim())
        .ok()
        .map(|d| d.with_timezone(&Utc))
}

async fn persist_raw_order_fill_row(
    store: &Storage,
    run_id: &Uuid,
    wallet: &str,
    role: &str,
    row: &serde_json::Value,
) -> anyhow::Result<()> {
    let Some(id) = row.get("id").and_then(|x| x.as_str()) else {
        return Ok(());
    };
    let ts = row
        .get("timestamp")
        .and_then(json_value_ts_secs)
        .and_then(|sec| DateTime::from_timestamp(sec, 0));
    let cond = row
        .get("condition")
        .or_else(|| row.get("conditionId"))
        .and_then(|x| x.as_str());
    store
        .insert_raw_subgraph_order_filled(run_id, wallet, role, id, ts, cond, row)
        .await
}

async fn persist_json_rows_chunks(
    storage: &Storage,
    run_id: &Uuid,
    wallet: &str,
    source: &str,
    rows: &[serde_json::Value],
    chunk_size: usize,
) -> anyhow::Result<()> {
    let cs = chunk_size.max(1);
    if rows.is_empty() {
        return Ok(());
    }
    for (i, ch) in rows.chunks(cs).enumerate() {
        let payload = serde_json::to_value(ch)?;
        storage
            .insert_raw_chunk(run_id, wallet, source, i as i32, &payload)
            .await?;
    }
    Ok(())
}

fn build_report(
    cfg: &AppConfig,
    wallet: &str,
    trades: &[Trade],
    truncated: bool,
    max_offset_allowed: Option<u32>,
    augment: &ReportAugment,
) -> AnalyzeReport {
    let per_trade_pnl = trade_pnl::per_trade_realized_pnl(trades);
    let mut total_volume = 0.0_f64;
    let mut net_pnl = 0.0_f64;
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
        net_pnl += cash_flow;

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

        let market_type = cfg.market_type.classify_slug(&t.slug).to_string();
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
        .unwrap_or(0.0);
    max_single_loss = per_trade_pnl
        .iter()
        .cloned()
        .filter(|x| *x < 0.0)
        .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        .unwrap_or(0.0);

    let total_trades = trades.len().max(1);
    let total_volume_nonzero = if total_volume == 0.0 { 1.0 } else { total_volume };
    let mut market_distribution: Vec<MarketDistributionItem> = dist
        .into_iter()
        .map(|(market_type, (cnt, vol, pnl))| MarketDistributionItem {
            market_type,
            trades: cnt,
            volume: vol,
            pnl,
            trades_pct: (cnt as f64 / total_trades as f64) * 100.0,
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

    let frontend = Some(build_frontend_presentation(
        wallet,
        trades,
        &per_trade_pnl,
        &augment.open_positions,
        &primary_style,
        win_rate_overall,
        total_volume,
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
        schema_version: "2.2.0".to_string(),
        wallet: wallet.to_string(),
        trades_count: trades.len(),
        total_volume,
        data_fetch: DataFetchMeta {
            truncated,
            max_offset_allowed,
        },
        lifetime: LifetimeMetrics {
            total_trades: trades.len(),
            total_volume,
            net_pnl,
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
                "net_pnl: per-trade cash flow; closed_realized_pnl_sum from /closed-positions; open_position_value from /positions; entry P50/P90 use Gamma resolution times (capped slugs).".into(),
            ];
            if truncated {
                notes.push(format!(
                    "Data API trades were truncated due to upstream historical offset limit (max_offset_allowed={})",
                    max_offset_allowed
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "unknown".to_string())
                ));
            }
            notes
        },
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

    #[derive(Clone)]
    struct AppState {
        cfg: AppConfig,
        storage: Option<Storage>,
    }

    let state = AppState {
        cfg: cfg.clone(),
        storage,
    };

    async fn analyze_handler(
        Path(wallet): Path<String>,
        Query(q): Query<AnalyzeQuery>,
        State(st): State<AppState>,
    ) -> impl IntoResponse {
        let cfg = effective_config(&st.cfg, &AnalyzeOverrides::from(&q));
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

    async fn leaderboard_handler(
        Query(q): Query<LeaderboardQuery>,
        State(st): State<AppState>,
    ) -> impl IntoResponse {
        let limit = q.limit.unwrap_or(30).clamp(1, 100);
        let Some(ref store) = st.storage else {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({
                    "error": "leaderboard requires postgres (DATABASE_URL)"
                })),
            )
                .into_response();
        };
        match store.fetch_leaderboard(limit).await {
            Ok(rows) => (
                StatusCode::OK,
                Json(serde_json::json!({ "items": rows })),
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

    let mut app = Router::new()
        .route("/health", get(health_handler))
        .route("/analyze/:wallet", get(analyze_handler))
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
