//! Heuristic strategy labeling + rich `rule_json` / pseudocode for `AnalyzeReport`.

use chrono::Datelike;
use crate::polymarket::data_api::Trade;
use crate::report::{MarketDistributionItem, SideBias, TradingPatterns};
use serde_json::{json, Value};
use std::collections::HashMap;

pub struct StrategyInputs<'a> {
    pub market_distribution: &'a [MarketDistributionItem],
    pub patterns: &'a TradingPatterns,
    pub side_bias: &'a SideBias,
    pub total_volume: f64,
    pub trades_count: usize,
    pub trades: &'a [Trade],
    /// Unix seconds: market resolution (Gamma closed/end) by slug.
    pub resolution_ts_by_slug: &'a HashMap<String, i64>,
}

fn percentile(sorted: &[f64], p: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p.clamp(0.0, 1.0)).round() as usize;
    Some(sorted[idx.min(sorted.len() - 1)])
}

fn trade_ts_sec(t: &Trade) -> i64 {
    let ts = t.timestamp;
    if ts > 1_000_000_000_000 {
        ts / 1000
    } else {
        ts
    }
}

fn grid_stats(trades: &[Trade]) -> (usize, usize, f64) {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for t in trades {
        *counts.entry(t.slug.clone()).or_insert(0) += 1;
    }
    let n_markets = counts.len().max(1);
    let grid_like = counts.values().filter(|&&c| c > 3).count();
    let ratio = grid_like as f64 / n_markets as f64;
    (grid_like, n_markets, ratio)
}

fn entry_seconds_to_resolution(
    trades: &[Trade],
    resolution_ts_by_slug: &HashMap<String, i64>,
) -> Vec<f64> {
    let mut out = Vec::new();
    for t in trades {
        let Some(&res) = resolution_ts_by_slug.get(&t.slug) else {
            continue;
        };
        let te = trade_ts_sec(t);
        let delta = res - te;
        // Trade should be before resolution; ignore pathological skew.
        if (1..86400 * 30).contains(&delta) {
            out.push(delta as f64);
        }
    }
    out
}

fn jackpot_volume_ratio(trades: &[Trade], total_volume: f64) -> f64 {
    if total_volume <= 0.0 {
        return 0.0;
    }
    let mut v = 0.0_f64;
    for t in trades {
        if t.price < 0.15 {
            v += t.size * t.price;
        }
    }
    (v / total_volume).clamp(0.0, 1.0)
}

fn preferred_price_ranges(trades: &[Trade], total_volume: f64) -> Vec<Value> {
    let bands: [(f64, f64, &str); 6] = [
        (0.0, 0.1, "<0.1"),
        (0.1, 0.3, "0.1–0.3"),
        (0.3, 0.5, "0.3–0.5"),
        (0.5, 0.7, "0.5–0.7"),
        (0.7, 0.9, "0.7–0.9"),
        (0.9, 1.0001, "0.9–1.0"),
    ];
    let mut vol = [0.0_f64; 6];
    for t in trades {
        let px = t.price.clamp(0.0, 1.0);
        let vv = t.size * t.price;
        for (i, (lo, hi, _)) in bands.iter().enumerate() {
            if px >= *lo && px < *hi {
                vol[i] += vv;
                break;
            }
        }
    }
    let tv = total_volume.max(1.0);
    bands
        .iter()
        .enumerate()
        .map(|(i, (lo, hi, label))| {
            json!({
                "label": label,
                "range_low": lo,
                "range_high": hi,
                "volume_share": vol[i] / tv,
            })
        })
        .collect()
}

fn multi_window_count(trades: &[Trade]) -> usize {
    use chrono::{TimeZone, Utc};
    let mut days: std::collections::HashSet<(i32, u32, u32)> = std::collections::HashSet::new();
    for t in trades {
        let ts = trade_ts_sec(t);
        if let Some(dt) = Utc.timestamp_opt(ts, 0).single() {
            days.insert((dt.year(), dt.month(), dt.day()));
        }
    }
    days.len()
}

/// Main entry: builds `primary_style`, `rule_json`, `pseudocode`.
pub fn infer_strategy(inputs: StrategyInputs<'_>) -> (String, Value, String) {
    let StrategyInputs {
        market_distribution,
        patterns: _patterns,
        side_bias,
        total_volume,
        trades_count,
        trades,
        resolution_ts_by_slug,
    } = inputs;

    let top_type = market_distribution
        .first()
        .map(|m| m.market_type.as_str())
        .unwrap_or("unknown");

    let (grid_like_markets, _n_markets, grid_ratio) = grid_stats(trades);
    let mut entry_secs = entry_seconds_to_resolution(trades, resolution_ts_by_slug);
    entry_secs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p50 = percentile(&entry_secs, 0.5);
    let p90 = percentile(&entry_secs, 0.9);
    let entry_window_sec_avg = if entry_secs.is_empty() {
        None
    } else {
        Some(entry_secs.iter().sum::<f64>() / entry_secs.len() as f64)
    };

    let jackpot_bias = jackpot_volume_ratio(trades, total_volume);
    let ranges = preferred_price_ranges(trades, total_volume);
    let multi_window = multi_window_count(trades);

    let is_grid = grid_like_markets > 0;
    let high_freq = trades_count >= 50;

    let mut primary_style = match (top_type, is_grid, high_freq) {
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

    let degen_grid = grid_ratio > 0.20
        && p90.map(|x| x < 60.0).unwrap_or(false)
        && entry_secs.len() >= 8;
    if degen_grid {
        primary_style = "high-frequency-grid-scalper".to_string();
    }

    let preferred_low = if side_bias.buy_pct > 60.0 { 0.3 } else { 0.0 };
    let preferred_high = if side_bias.sell_pct > 60.0 { 0.7 } else { 1.0 };

    let rule_json = json!({
        "entry_window_sec_avg": entry_window_sec_avg,
        "entry_to_resolution_p50_sec": p50,
        "entry_to_resolution_p90_sec": p90,
        "preferred_price_ranges": ranges,
        "jackpot_bias": jackpot_bias,
        "multi_window_count": multi_window,
        "grid_like_market_ratio": grid_ratio,
        "grid_like_markets": grid_like_markets,
        "preferred_price_range_legacy": [preferred_low, preferred_high],
        "side_bias": { "buy_pct": side_bias.buy_pct, "sell_pct": side_bias.sell_pct },
        "primary_market_type": top_type,
        "total_volume": total_volume,
        "trades_count": trades_count,
        "entry_timing_samples": entry_secs.len(),
    });

    let pseudocode = if primary_style == "high-frequency-grid-scalper" {
        format!(
            r#"STRATEGY: high-frequency-grid-scalper
// Detected: grid_like_market_ratio={:.1}% (>20%) AND P90(seconds-to-resolution)={:.0}s (<60s) with {} timing samples
// jackpot_bias={:.2} (volume share at price<0.15), multi_window_count={} active UTC days

WHILE true:
  FOR market IN watchlist:
    t_res := resolution_time(market)
    LOOP:
      WAIT_UNTIL(now >= t_res - 30s)   // last-30s sprint / resolution snipe window
      px := mid_or_last_price(market)
      IF px < 0.15 OR px IN preferred_jackpot_band:
        size := BASE_SIZE * (1.0 + {:.2})   // scale with jackpot_bias
        PLACE aggressive orders (grid / layered quotes)
      IF multi_window_count > 1:
        FAN_OUT across parallel_markets[] // do not block on a single window
      SLEEP short_poll
"#,
            grid_ratio * 100.0,
            p90.unwrap_or(0.0),
            entry_secs.len(),
            jackpot_bias,
            multi_window,
            jackpot_bias,
        )
    } else {
        format!(
            r#"FOR each market in top_types:
  IF market matches {}:
    ENTRY: prefer price in [{}, {}]
    SIDE_BIAS: buy {}% / sell {}%
  IF multi_entry_markets > 0: GRID-like behavior (same market >3 trades)
  IF entry P90 to resolution known: tune snipe window using rule_json.entry_to_resolution_p90_sec
  STYLE: {}"#,
            top_type,
            preferred_low,
            preferred_high,
            side_bias.buy_pct as i32,
            side_bias.sell_pct as i32,
            primary_style
        )
    };

    (primary_style, rule_json, pseudocode)
}
