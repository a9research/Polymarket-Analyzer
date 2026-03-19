use clap::{Parser, Subcommand};
use chrono::{DateTime, Timelike, Utc};
use polymarket_account_analyzer::{
    config::{load_config, AppConfig},
    polymarket::data_api::{DataApiClient, Trade, TradeSide},
    report::{
        AnalyzeReport, DataFetchMeta, LifetimeMetrics, MarketDistributionItem, SideBias,
        StrategyInference, TimeAnalysis, TradingPatterns, WinRateByMarketType,
    },
    storage::Storage,
};
use reqwest::Client;
use std::collections::{BTreeMap, HashMap};
use std::{path::PathBuf, time::Duration};
use tracing_subscriber::EnvFilter;

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

#[derive(Debug, Subcommand)]
enum Command {
    /// Analyze a wallet and output JSON report.
    Analyze {
        wallet: String,
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let cfg = load_config(cli.config.as_deref())?;

    match cli.cmd {
        Command::Analyze {
            wallet,
            out,
            no_cache,
        } => {
            let storage = init_storage(&cfg).await?;
            let report = analyze_wallet(&cfg, storage.as_ref(), &wallet, no_cache).await?;
            let json = serde_json::to_string_pretty(&report)?;
            match out {
                Some(path) => {
                    std::fs::write(path, json)?;
                }
                None => {
                    println!("{json}");
                }
            }
        }
        Command::Serve { bind } => {
            serve(cfg, bind).await?;
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

async fn analyze_wallet(
    cfg: &AppConfig,
    storage: Option<&Storage>,
    wallet: &str,
    no_cache: bool,
) -> anyhow::Result<AnalyzeReport> {
    if !no_cache {
        if let Some(store) = storage {
            if let Some(cached) = store
                .get_cached_report(wallet, cfg.cache_ttl_sec as i64)
                .await?
            {
                return Ok(cached);
            }
        }
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
    let _ = (http, rate);

    let trades_result = data
        .trades_all(wallet, cfg.trades_page_limit)
        .await?;
    let report = build_report(
        cfg,
        wallet,
        &trades_result.trades,
        trades_result.truncated,
        trades_result.max_offset_allowed,
    );

    if let Some(store) = storage {
        store.upsert_report(wallet, &report).await?;
    }

    Ok(report)
}

fn build_report(
    cfg: &AppConfig,
    wallet: &str,
    trades: &[Trade],
    truncated: bool,
    max_offset_allowed: Option<u32>,
) -> AnalyzeReport {
    let mut total_volume = 0.0_f64;
    let mut net_pnl = 0.0_f64;
    let mut max_single_win = 0.0_f64;
    let mut max_single_loss = 0.0_f64;
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
        if cash_flow > max_single_win {
            max_single_win = cash_flow;
        }
        if cash_flow < max_single_loss {
            max_single_loss = cash_flow;
        }

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

        let ts_ms = t.timestamp;
        let ts_sec = if ts_ms > 1_000_000_000_000 {
            ts_ms / 1000
        } else {
            ts_ms
        };
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

    let time_analysis = TimeAnalysis {
        active_hours_utc,
        entry_to_resolution_seconds: vec![],
        holding_duration_seconds: vec![],
        metadata_missing_ratio: 1.0,
    };

    let trading_patterns = TradingPatterns {
        grid_like_markets,
        side_bias,
        win_rate_overall,
        win_rate_by_market_type,
    };

    let (primary_style, rule_json, pseudocode) = infer_strategy(
        &market_distribution,
        &trading_patterns,
        &trading_patterns.side_bias,
        total_volume,
        trades.len(),
    );

    let strategy_inference = StrategyInference {
        primary_style,
        rule_json,
        pseudocode,
    };

    AnalyzeReport {
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
            open_position_value: 0.0,
            max_single_win,
            max_single_loss,
        },
        market_distribution,
        price_buckets,
        time_analysis,
        trading_patterns,
        strategy_inference,
        notes: {
            let mut notes = vec![
            "PnL and win-rate are trade-level (cash flow); position reconstruction and Gamma metadata for entry/holding time next."
                .to_string(),
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

fn infer_strategy(
    market_dist: &[MarketDistributionItem],
    patterns: &TradingPatterns,
    side_bias: &SideBias,
    total_volume: f64,
    trades_count: usize,
) -> (String, serde_json::Value, String) {
    let top_type = market_dist
        .first()
        .map(|m| m.market_type.as_str())
        .unwrap_or("unknown");
    let is_grid = patterns.grid_like_markets > 0;
    let high_freq = trades_count >= 50;
    let primary_style = match (top_type, is_grid, high_freq) {
        (t, true, true) if t.contains("5-min") || t.contains("1h") => {
            "high_frequency_scalper".to_string()
        }
        (_, true, _) => "grid_bot".to_string(),
        (t, _, _) if t == "politics" || t == "event" || t == "unknown" => {
            "long_hold_event_bettor".to_string()
        }
        (t, _, _) if t.contains("daily") || t.contains("1h") => "momentum_trader".to_string(),
        _ => "mixed".to_string(),
    };

    let preferred_low = if side_bias.buy_pct > 60.0 { 0.3 } else { 0.0 };
    let preferred_high = if side_bias.sell_pct > 60.0 { 0.7 } else { 1.0 };
    let rule_json = serde_json::json!({
        "entry_window_sec": null,
        "preferred_price_range": [preferred_low, preferred_high],
        "multi_entry_markets": patterns.grid_like_markets,
        "side_bias": { "buy_pct": side_bias.buy_pct, "sell_pct": side_bias.sell_pct },
        "primary_market_type": top_type,
        "total_volume": total_volume,
        "trades_count": trades_count,
    });

    let pseudocode = format!(
        "FOR each market in top_types:\n  IF market matches {}:\n    ENTRY: prefer price in [{}, {}]\n    SIDE_BIAS: buy {}% / sell {}%\n  IF multi_entry_markets > 0: GRID-like behavior (same market >3 trades)\n  STYLE: {}",
        top_type,
        preferred_low,
        preferred_high,
        side_bias.buy_pct as i32,
        side_bias.sell_pct as i32,
        primary_style
    );

    (primary_style, rule_json, pseudocode)
}

async fn serve(cfg: AppConfig, bind: String) -> anyhow::Result<()> {
    use axum::{
        extract::{Path, Query},
        http::StatusCode,
        response::IntoResponse,
        routing::get,
        Json, Router,
    };

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

    #[derive(Debug, serde::Deserialize)]
    struct AnalyzeQuery {
        #[serde(default)]
        no_cache: bool,
    }

    async fn analyze_handler(
        Path(wallet): Path<String>,
        Query(q): Query<AnalyzeQuery>,
        axum::extract::State(st): axum::extract::State<AppState>,
    ) -> impl IntoResponse {
        match analyze_wallet(&st.cfg, st.storage.as_ref(), &wallet, q.no_cache).await {
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

    let app = Router::new()
        .route("/analyze/:wallet", get(analyze_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!("listening on {}", bind);
    axum::serve(listener, app).await?;
    Ok(())
}
