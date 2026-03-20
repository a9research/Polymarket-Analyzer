//! v0 reconciliation: match Data API trade timestamps to subgraph orderFilledEvents (maker ∪ taker by id).
//! `summary_from_v1` maps persisted canonical merge stats into the legacy summary struct.

use crate::canonical::{ReconciliationCountsV1, ReconciliationReportV1};
use crate::polymarket::data_api::Trade;
use serde_json::Value;
use std::collections::HashMap;

/// Subgraph / redemption `timestamp` field → unix seconds.
pub fn json_value_ts_secs(v: &Value) -> Option<i64> {
    json_timestamp_secs(v)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct ReconciliationSummary {
    pub enabled: bool,
    pub method: String,
    pub time_window_sec: i64,
    pub api_trade_count: usize,
    /// Unique fill event ids (maker ∪ taker).
    pub subgraph_fill_events_unique: usize,
    pub redemption_count: usize,
    /// Greedy one-to-one matches within time window (low confidence).
    pub matched_pairs_estimate: usize,
    pub api_only_estimate: usize,
    pub subgraph_fill_only_estimate: usize,
    pub low_confidence: bool,
    pub note: String,
}

fn json_timestamp_secs(v: &Value) -> Option<i64> {
    if let Some(n) = v.as_i64() {
        return Some(if n > 1_000_000_000_000 { n / 1000 } else { n });
    }
    if let Some(n) = v.as_u64() {
        let n = n as i64;
        return Some(if n > 1_000_000_000_000 { n / 1000 } else { n });
    }
    if let Some(s) = v.as_str() {
        if let Ok(n) = s.parse::<i64>() {
            return Some(if n > 1_000_000_000_000 { n / 1000 } else { n });
        }
    }
    None
}

fn trade_ts_sec(t: &Trade) -> i64 {
    let ts = t.timestamp;
    if ts > 1_000_000_000_000 {
        ts / 1000
    } else {
        ts
    }
}

/// Collect unique fill ids → timestamp from maker and taker rows.
fn fill_id_map(maker: &[Value], taker: &[Value]) -> HashMap<String, i64> {
    let mut m: HashMap<String, i64> = HashMap::new();
    for row in maker.iter().chain(taker.iter()) {
        let Some(id) = row.get("id").and_then(|x| x.as_str()).map(str::to_owned) else {
            continue;
        };
        let Some(ts) = row.get("timestamp").and_then(json_timestamp_secs) else {
            continue;
        };
        m.entry(id).or_insert(ts);
    }
    m
}

fn redemption_count(redemptions: &[Value]) -> usize {
    redemptions.len()
}

/// Extract orderFilledEvents arrays from subgraph section JSON `{ data: { orderFilledEvents: [...] } }`.
pub fn extract_fills_rows(section: &Value) -> Vec<Value> {
    section
        .get("data")
        .and_then(|d| d.get("orderFilledEvents"))
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default()
}

pub fn extract_redemption_rows(section: &Value) -> Vec<Value> {
    section
        .get("data")
        .and_then(|d| d.get("redemptions"))
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default()
}

pub fn reconcile_v0(
    trades: &[Trade],
    maker_section: &Value,
    taker_section: &Value,
    redemptions_section: &Value,
    time_window_sec: i64,
) -> ReconciliationSummary {
    let maker_rows = extract_fills_rows(maker_section);
    let taker_rows = extract_fills_rows(taker_section);
    let redemption_n = redemption_count(&extract_redemption_rows(redemptions_section));

    let fill_map = fill_id_map(&maker_rows, &taker_rows);
    let mut fills_ts: Vec<i64> = fill_map.values().copied().collect();
    fills_ts.sort_unstable();

    let mut trades_ts: Vec<i64> = trades.iter().map(trade_ts_sec).collect();
    trades_ts.sort_unstable();

    let w = time_window_sec.max(0);
    let mut used_fill = vec![false; fills_ts.len()];
    let mut matched = 0usize;
    for t in &trades_ts {
        for (i, f) in fills_ts.iter().enumerate() {
            if used_fill[i] {
                continue;
            }
            if (*f - *t).abs() <= w {
                used_fill[i] = true;
                matched += 1;
                break;
            }
        }
    }

    let fill_n = fills_ts.len();
    let matched_fills = used_fill.iter().filter(|&&u| u).count();
    let api_only = trades_ts.len().saturating_sub(matched);
    let subgraph_only = fill_n.saturating_sub(matched_fills);

    ReconciliationSummary {
        enabled: true,
        method: "v0_timestamp_window_one_to_one".to_string(),
        time_window_sec: w,
        api_trade_count: trades.len(),
        subgraph_fill_events_unique: fill_n,
        redemption_count: redemption_n,
        matched_pairs_estimate: matched,
        api_only_estimate: api_only,
        subgraph_fill_only_estimate: subgraph_only,
        low_confidence: true,
        note: "No size/price on subgraph fills in current schema; matches are timestamp proximity only."
            .to_string(),
    }
}

/// Bridge v1 canonical report into the compact `ReconciliationSummary` used in `AnalyzeReport`.
pub fn summary_from_v1(r: &ReconciliationReportV1, time_window_sec: i64) -> ReconciliationSummary {
    let low = r.counts.ambiguous > 0;
    ReconciliationSummary {
        enabled: true,
        method: format!("v1_canonical_{}", r.rules_version),
        time_window_sec,
        api_trade_count: r.counts.matched + r.counts.api_only,
        subgraph_fill_events_unique: r.counts.matched + r.counts.subgraph_fill_only,
        redemption_count: r.counts.redemptions,
        matched_pairs_estimate: r.counts.matched,
        api_only_estimate: r.counts.api_only,
        subgraph_fill_only_estimate: r.counts.subgraph_fill_only,
        low_confidence: low,
        note: "Canonical merge persisted to Postgres (canonical_events, source_event_map, reconciliation_report). See reconciliation_v1 for full counts and ambiguous cases.".to_string(),
    }
}

/// v1 merge quality hints (§1.5 P2 — large coverage gaps / ambiguous / subgraph-only skew).
pub fn v1_coverage_alert_notes(
    c: &ReconciliationCountsV1,
    api_only_ratio_alert: f64,
    api_only_min: usize,
) -> Vec<String> {
    let mut v = Vec::new();
    let denom = c.matched + c.api_only;
    if denom > 0 && c.api_only >= api_only_min {
        let ratio = c.api_only as f64 / denom as f64;
        if ratio >= api_only_ratio_alert {
            v.push(format!(
                "quality_alert: high api_only ratio {:.1}% (api_only={}, matched={}, threshold={:.0}%)",
                ratio * 100.0,
                c.api_only,
                c.matched,
                api_only_ratio_alert * 100.0
            ));
        }
    }
    if c.ambiguous > 0 {
        v.push(format!(
            "quality_alert: {} ambiguous merge case(s); export reconciliation_ambiguous_queue for review.",
            c.ambiguous
        ));
    }
    let api_side = c.matched + c.api_only;
    if c.subgraph_fill_only > api_side.saturating_mul(2) && c.subgraph_fill_only >= 25 {
        v.push(format!(
            "quality_alert: subgraph_fill_only ({}) is large vs matched+api_only ({}); check subgraph cap, truncation, or missing Data API history.",
            c.subgraph_fill_only, api_side
        ));
    }
    v
}

/// When subgraph HTTP/GraphQL fails: explicit low-confidence row instead of silent omission.
pub fn subgraph_fetch_failed_summary(trade_count: usize, err_msg: &str) -> ReconciliationSummary {
    ReconciliationSummary {
        enabled: true,
        method: "skipped_subgraph_fetch_error".to_string(),
        time_window_sec: 0,
        api_trade_count: trade_count,
        subgraph_fill_events_unique: 0,
        redemption_count: 0,
        matched_pairs_estimate: 0,
        api_only_estimate: trade_count,
        subgraph_fill_only_estimate: 0,
        low_confidence: true,
        note: format!(
            "Subgraph fetch failed; v0/v1 reconciliation skipped. Error: {}",
            err_msg.trim().chars().take(500).collect::<String>()
        ),
    }
}

/// Shadow vs primary volume — relative diff alert for `canonical_shadow` path.
pub fn shadow_volume_discrepancy_note(
    primary_volume: f64,
    shadow_volume: f64,
    ratio_threshold: f64,
    analytics_src: &str,
) -> Option<String> {
    let base = primary_volume.abs().max(1e-12);
    let rel = (shadow_volume - primary_volume).abs() / base;
    if rel >= ratio_threshold {
        Some(format!(
            "quality_alert: shadow vs primary volume rel_diff={:.2} (threshold={:.2}; primary={:.6} shadow={:.6}; src={})",
            rel, ratio_threshold, primary_volume, shadow_volume, analytics_src
        ))
    } else {
        None
    }
}
