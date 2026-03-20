//! Canonical event layer: merge Data API trades with subgraph fills (v1 rules),
//! redemptions as separate events, optional PnL position snapshots.
//! Persists via `Storage` (canonical_events, source_event_map, reconciliation_report, ambiguous queue).

use crate::polymarket::data_api::{Trade, TradeSide};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub const RULES_VERSION_V1: &str = "1";

/// Side-by-side comparison metrics vs Data API–based `build_report` (§1.5.5).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalShadowMetrics {
    pub trade_like_event_count: usize,
    pub total_volume: f64,
    pub net_pnl_proxy: f64,
    pub explanation: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconciliationReportV1 {
    pub rules_version: String,
    pub counts: ReconciliationCountsV1,
    #[serde(default)]
    pub coverage_pct: serde_json::Value,
    #[serde(default)]
    pub ambiguous: Vec<AmbiguousCaseV1>,
    #[serde(default)]
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReconciliationCountsV1 {
    pub matched: usize,
    pub api_only: usize,
    pub subgraph_fill_only: usize,
    pub redemptions: usize,
    pub position_snapshots: usize,
    pub ambiguous: usize,
    pub canonical_total: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AmbiguousCaseV1 {
    pub trade_key: String,
    pub candidate_fill_ids: Vec<String>,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct CanonicalPipelineParams {
    pub time_window_sec: i64,
    /// Relative size tolerance, e.g. 0.05 = 5%.
    pub size_tolerance_pct: f64,
    pub price_tolerance_abs: f64,
    pub require_condition_match: bool,
}

impl Default for CanonicalPipelineParams {
    fn default() -> Self {
        Self {
            time_window_sec: 120,
            size_tolerance_pct: 0.05,
            price_tolerance_abs: 0.02,
            require_condition_match: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CanonicalEventInsert {
    pub id: Uuid,
    pub event_type: String,
    pub occurred_at: DateTime<Utc>,
    pub condition_id: Option<String>,
    pub market_slug: Option<String>,
    pub amounts: Value,
    pub source_refs: Value,
}

#[derive(Debug, Clone)]
pub struct SourceMapInsert {
    pub canonical_id: Uuid,
    pub source: String,
    pub source_row_id: String,
    pub confidence: String,
}

#[derive(Debug, Clone)]
pub struct AmbiguousInsert {
    pub trade_key: String,
    pub candidates: Value,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct CanonicalMergeArtifacts {
    pub events: Vec<CanonicalEventInsert>,
    pub source_maps: Vec<SourceMapInsert>,
    pub ambiguous: Vec<AmbiguousInsert>,
    pub report: ReconciliationReportV1,
}

/// Normalize condition id / bytes hex for comparisons.
pub fn normalize_condition_id(s: &str) -> String {
    norm_condition(s)
}

/// Build `condition_id → slug` from persisted canonical rows (for `synthetic_trades_from_events`).
pub fn slug_map_from_canonical_events(events: &[CanonicalEventInsert]) -> HashMap<String, String> {
    let mut m = HashMap::new();
    for e in events {
        if let (Some(cid), Some(slug)) = (&e.condition_id, &e.market_slug) {
            if slug.is_empty() {
                continue;
            }
            m.entry(normalize_condition_id(cid))
                .or_insert_with(|| slug.clone());
        }
    }
    m
}

fn norm_condition(s: &str) -> String {
    let t = s.trim().to_lowercase();
    t.strip_prefix("0x").map(|x| x.to_string()).unwrap_or(t)
}

fn trade_ts_sec(t: &Trade) -> i64 {
    let ts = t.timestamp;
    if ts > 1_000_000_000_000 {
        ts / 1000
    } else {
        ts
    }
}

fn json_ts_secs(v: &Value) -> Option<i64> {
    if let Some(n) = v.as_i64() {
        return Some(if n > 1_000_000_000_000 {
            n / 1000
        } else {
            n
        });
    }
    if let Some(n) = v.as_u64() {
        let n = n as i64;
        return Some(if n > 1_000_000_000_000 {
            n / 1000
        } else {
            n
        });
    }
    if let Some(s) = v.as_str() {
        if let Ok(n) = s.parse::<i64>() {
            return Some(if n > 1_000_000_000_000 {
                n / 1000
            } else {
                n
            });
        }
    }
    None
}

#[derive(Debug, Clone)]
struct FillNorm {
    id: String,
    ts: i64,
    condition: Option<String>,
    size: Option<f64>,
    price: Option<f64>,
    tx: Option<String>,
}

fn parse_fill(row: &Value) -> Option<FillNorm> {
    let id = row.get("id")?.as_str()?.to_string();
    let ts = json_ts_secs(row.get("timestamp")?)?;
    let condition = row
        .get("condition")
        .or_else(|| row.get("conditionId"))
        .and_then(|x| x.as_str())
        .map(|s| s.to_string());
    let size = row
        .get("size")
        .and_then(|x| x.as_f64())
        .or_else(|| row.get("size").and_then(|x| x.as_str()?.parse().ok()));
    let price = row
        .get("price")
        .and_then(|x| x.as_f64())
        .or_else(|| row.get("price").and_then(|x| x.as_str()?.parse().ok()));
    let tx = row
        .get("transactionHash")
        .or_else(|| row.get("txHash"))
        .and_then(|x| x.as_str())
        .map(str::to_owned);
    Some(FillNorm {
        id,
        ts,
        condition,
        size,
        price,
        tx,
    })
}

fn merge_fill_rows(maker: &[Value], taker: &[Value]) -> HashMap<String, FillNorm> {
    let mut m: HashMap<String, FillNorm> = HashMap::new();
    for row in maker.iter().chain(taker.iter()) {
        let Some(f) = parse_fill(row) else {
            continue;
        };
        let id = f.id.clone();
        m.entry(id)
            .and_modify(|e| {
                if e.condition.is_none() {
                    e.condition = f.condition.clone();
                }
                if e.size.is_none() {
                    e.size = f.size;
                }
                if e.price.is_none() {
                    e.price = f.price;
                }
                if e.tx.is_none() {
                    e.tx = f.tx.clone();
                }
            })
            .or_insert(f);
    }
    m
}

fn record_data_api_trade_row(
    events: &mut Vec<CanonicalEventInsert>,
    source_maps: &mut Vec<SourceMapInsert>,
    t: &Trade,
    idx: usize,
    slug_by_condition: &HashMap<String, String>,
    ambiguity_flag: Option<&'static str>,
) {
    let cid = Uuid::new_v4();
    let tkey = trade_key(t, idx);
    let slug = slug_for_condition(&t.condition_id, slug_by_condition);
    let mut amounts = json!({
        "size": t.size,
        "price": t.price,
        "side": format!("{:?}", t.side),
    });
    if let Some(f) = ambiguity_flag {
        if let Some(obj) = amounts.as_object_mut() {
            obj.insert(f.to_string(), json!(true));
        }
    }
    events.push(CanonicalEventInsert {
        id: cid,
        event_type: "DATA_API_TRADE".to_string(),
        occurred_at: DateTime::from_timestamp(trade_ts_sec(t), 0).unwrap_or_else(Utc::now),
        condition_id: Some(t.condition_id.clone()),
        market_slug: slug,
        amounts,
        source_refs: json!([{ "source": "data_api", "ref": tkey.clone() }]),
    });
    source_maps.push(SourceMapInsert {
        canonical_id: cid,
        source: "data_api".to_string(),
        source_row_id: tkey,
        confidence: if ambiguity_flag.is_some() {
            "ambiguous".to_string()
        } else {
            "unmatched_primary".to_string()
        },
    });
}

fn trade_key(t: &Trade, idx: usize) -> String {
    if let Some(ref h) = t.transaction_hash {
        if !h.is_empty() {
            return format!("tx:{h}");
        }
    }
    format!(
        "api:{}:{}:{}:{}:{}",
        idx,
        norm_condition(&t.condition_id),
        trade_ts_sec(t),
        t.size,
        t.price
    )
}

fn score_match(
    t: &Trade,
    f: &FillNorm,
    w: i64,
    p: &CanonicalPipelineParams,
) -> Option<f64> {
    let dt = (f.ts - trade_ts_sec(t)).unsigned_abs() as f64;
    if dt > w as f64 {
        return None;
    }
    let tc = norm_condition(&t.condition_id);
    match f.condition.as_ref().map(|c| norm_condition(c)) {
        Some(fc) if fc != tc => {
            if p.require_condition_match {
                return None;
            }
            // If both sides have a condition id and they differ, skip unless tx hash matches.
            let tx_hit = t
                .transaction_hash
                .as_ref()
                .zip(f.tx.as_ref())
                .map(|(a, b)| a.eq_ignore_ascii_case(b))
                .unwrap_or(false);
            if !tx_hit {
                return None;
            }
        }
        None if p.require_condition_match => return None,
        _ => {}
    }
    let mut score = dt;
    if let Some(fs) = f.size {
        let ts = t.size;
        if ts > 1e-9 {
            let rel = ((fs - ts).abs() / ts) as f64;
            if rel > p.size_tolerance_pct {
                return None;
            }
            score += rel * 10_000.0;
        }
    }
    if let Some(fp) = f.price {
        let tp = t.price;
        if (fp - tp).abs() > p.price_tolerance_abs {
            return None;
        }
        score += (fp - tp).abs() * 1_000.0;
    }
    Some(score)
}

/// Build canonical merge artifacts (in-memory). Caller persists to Postgres.
pub fn build_canonical_merge(
    wallet: &str,
    trades: &[Trade],
    maker_fills: &[Value],
    taker_fills: &[Value],
    redemptions: &[Value],
    position_rows: &[Value],
    slug_by_condition: &HashMap<String, String>,
    p: &CanonicalPipelineParams,
    rules_version: &str,
) -> CanonicalMergeArtifacts {
    let notes = vec![
        "Canonical layer merges subgraph orderFilledEvents (maker∪taker) with Data API trades."
            .to_string(),
    ];

    let fill_map = merge_fill_rows(maker_fills, taker_fills);
    let mut fills: Vec<FillNorm> = fill_map.into_values().collect();
    fills.sort_by_key(|f| f.ts);

    let mut used_fill: HashSet<String> = HashSet::new();
    let mut events: Vec<CanonicalEventInsert> = Vec::new();
    let mut source_maps: Vec<SourceMapInsert> = Vec::new();
    let mut ambiguous: Vec<AmbiguousInsert> = Vec::new();

    let w = p.time_window_sec.max(0);

    let mut matched = 0usize;
    let mut ambiguous_n = 0usize;

    for (idx, t) in trades.iter().enumerate() {
        let tkey = trade_key(t, idx);

        // 1) Exact tx match
        let mut candidates: Vec<(String, f64)> = Vec::new();
        if let Some(ref tx) = t.transaction_hash {
            if !tx.is_empty() {
                for f in &fills {
                    if used_fill.contains(&f.id) {
                        continue;
                    }
                    if f
                        .tx
                        .as_ref()
                        .map(|ft| ft.eq_ignore_ascii_case(tx))
                        .unwrap_or(false)
                    {
                        candidates.push((f.id.clone(), 0.0));
                    }
                }
            }
        }

        if candidates.len() > 1 {
            ambiguous.push(AmbiguousInsert {
                trade_key: tkey.clone(),
                candidates: json!(candidates.iter().map(|x| &x.0).collect::<Vec<_>>()),
                reason: "multiple_transaction_hash_matches".to_string(),
            });
            ambiguous_n += 1;
            record_data_api_trade_row(
                &mut events,
                &mut source_maps,
                t,
                idx,
                slug_by_condition,
                Some("ambiguous_multiple_tx_matches"),
            );
            continue;
        }

        if candidates.len() == 1 {
            let fid = candidates[0].0.clone();
            if let Some(f) = fills.iter().find(|x| x.id == fid) {
                used_fill.insert(fid.clone());
                let cid = Uuid::new_v4();
                let slug = slug_for_condition(&t.condition_id, slug_by_condition);
                let amounts = json!({
                    "size": t.size,
                    "price": t.price,
                    "side": format!("{:?}", t.side),
                    "fill_size": f.size,
                    "fill_price": f.price,
                });
                let refs = json!([
                    { "source": "data_api", "ref": tkey.clone() },
                    { "source": "subgraph_orderbook", "ref": f.id.clone() }
                ]);
                events.push(CanonicalEventInsert {
                    id: cid,
                    event_type: "MERGED_TRADE_FILL".to_string(),
                    occurred_at: DateTime::from_timestamp(trade_ts_sec(t), 0)
                        .unwrap_or_else(Utc::now),
                    condition_id: Some(t.condition_id.clone()),
                    market_slug: slug.clone(),
                    amounts,
                    source_refs: refs,
                });
                source_maps.push(SourceMapInsert {
                    canonical_id: cid,
                    source: "data_api".to_string(),
                    source_row_id: tkey.clone(),
                    confidence: "exact".to_string(),
                });
                source_maps.push(SourceMapInsert {
                    canonical_id: cid,
                    source: "subgraph_orderbook".to_string(),
                    source_row_id: f.id.clone(),
                    confidence: "exact".to_string(),
                });
                matched += 1;
                continue;
            }
        }

        // 2) Scored fuzzy match
        let mut scored: Vec<(String, f64)> = Vec::new();
        for f in &fills {
            if used_fill.contains(&f.id) {
                continue;
            }
            if let Some(s) = score_match(t, f, w, p) {
                scored.push((f.id.clone(), s));
            }
        }
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        if scored.len() >= 2 {
            let (_a, sa) = &scored[0];
            let (_, sb) = &scored[1];
            if *sb <= sa * 1.10 && (sb - sa).abs() < 1.0 {
                ambiguous.push(AmbiguousInsert {
                    trade_key: tkey.clone(),
                    candidates: json!(scored.iter().take(5).map(|x| &x.0).collect::<Vec<_>>()),
                    reason: "multiple_close_scoring_matches".to_string(),
                });
                ambiguous_n += 1;
                record_data_api_trade_row(
                    &mut events,
                    &mut source_maps,
                    t,
                    idx,
                    slug_by_condition,
                    Some("ambiguous_close_scoring_matches"),
                );
                continue;
            }
        }

        if let Some((fid, _)) = scored.first() {
            let fid = fid.clone();
            if let Some(f) = fills.iter().find(|x| x.id == fid) {
                used_fill.insert(fid.clone());
                let cid = Uuid::new_v4();
                let slug = slug_for_condition(&t.condition_id, slug_by_condition);
                let conf = if t.transaction_hash.is_some() && f.tx.is_some() {
                    "fuzzy"
                } else {
                    "fuzzy"
                };
                let amounts = json!({
                    "size": t.size,
                    "price": t.price,
                    "side": format!("{:?}", t.side),
                    "fill_size": f.size,
                    "fill_price": f.price,
                });
                events.push(CanonicalEventInsert {
                    id: cid,
                    event_type: "MERGED_TRADE_FILL".to_string(),
                    occurred_at: DateTime::from_timestamp(trade_ts_sec(t), 0)
                        .unwrap_or_else(Utc::now),
                    condition_id: Some(t.condition_id.clone()),
                    market_slug: slug.clone(),
                    amounts,
                    source_refs: json!([
                        { "source": "data_api", "ref": tkey },
                        { "source": "subgraph_orderbook", "ref": f.id.clone() }
                    ]),
                });
                source_maps.push(SourceMapInsert {
                    canonical_id: cid,
                    source: "data_api".to_string(),
                    source_row_id: trade_key(t, idx),
                    confidence: conf.to_string(),
                });
                source_maps.push(SourceMapInsert {
                    canonical_id: cid,
                    source: "subgraph_orderbook".to_string(),
                    source_row_id: f.id.clone(),
                    confidence: conf.to_string(),
                });
                matched += 1;
                continue;
            }
        }

        // API-only canonical row
        record_data_api_trade_row(
            &mut events,
            &mut source_maps,
            t,
            idx,
            slug_by_condition,
            None,
        );
    }

    let mut subgraph_only = 0usize;
    for f in &fills {
        if used_fill.contains(&f.id) {
            continue;
        }
        let cid = Uuid::new_v4();
        let cond = f.condition.clone();
        let slug = cond
            .as_ref()
            .and_then(|c| slug_for_condition(c, slug_by_condition));
        events.push(CanonicalEventInsert {
            id: cid,
            event_type: "SUBGRAPH_FILL".to_string(),
            occurred_at: DateTime::from_timestamp(f.ts, 0).unwrap_or_else(Utc::now),
            condition_id: cond.clone(),
            market_slug: slug,
            amounts: json!({
                "fill_size": f.size,
                "fill_price": f.price,
            }),
            source_refs: json!([{ "source": "subgraph_orderbook", "ref": f.id.clone() }]),
        });
        source_maps.push(SourceMapInsert {
            canonical_id: cid,
            source: "subgraph_orderbook".to_string(),
            source_row_id: f.id.clone(),
            confidence: "unmatched_primary".to_string(),
        });
        subgraph_only += 1;
    }

    let mut redemption_n = 0usize;
    for r in redemptions {
        let Some(id) = r.get("id").and_then(|x| x.as_str()) else {
            continue;
        };
        let ts = r
            .get("timestamp")
            .and_then(json_ts_secs)
            .unwrap_or_else(|| Utc::now().timestamp());
        let cid = Uuid::new_v4();
        let condition = r
            .get("condition")
            .and_then(|x| x.as_str())
            .map(str::to_owned);
        let slug = condition
            .as_ref()
            .and_then(|c| slug_for_condition(c, slug_by_condition));
        events.push(CanonicalEventInsert {
            id: cid,
            event_type: "REDEMPTION".to_string(),
            occurred_at: DateTime::from_timestamp(ts, 0).unwrap_or_else(Utc::now),
            condition_id: condition.clone(),
            market_slug: slug,
            amounts: json!({
                "payout": r.get("payout"),
                "indexSets": r.get("indexSets"),
            }),
            source_refs: json!([{ "source": "subgraph_activity", "ref": id }]),
        });
        source_maps.push(SourceMapInsert {
            canonical_id: cid,
            source: "subgraph_activity".to_string(),
            source_row_id: id.to_string(),
            confidence: "exact".to_string(),
        });
        redemption_n += 1;
    }

    let mut pos_n = 0usize;
    for row in position_rows {
        let Some(pid) = row.get("id").and_then(|x| x.as_str()) else {
            continue;
        };
        let cid = Uuid::new_v4();
        events.push(CanonicalEventInsert {
            id: cid,
            event_type: "PNL_POSITION_SNAPSHOT".to_string(),
            occurred_at: Utc::now(),
            condition_id: None,
            market_slug: None,
            amounts: row.clone(),
            source_refs: json!([{ "source": "subgraph_pnl", "ref": pid }]),
        });
        source_maps.push(SourceMapInsert {
            canonical_id: cid,
            source: "subgraph_pnl".to_string(),
            source_row_id: pid.to_string(),
            confidence: "exact".to_string(),
        });
        pos_n += 1;
    }

    let api_only = trades.len().saturating_sub(matched);
    let canonical_total = events.len();
    let denom = trades.len().max(1);
    let coverage = (matched as f64 / denom as f64) * 100.0;

    let report = ReconciliationReportV1 {
        rules_version: rules_version.to_string(),
        counts: ReconciliationCountsV1 {
            matched,
            api_only,
            subgraph_fill_only: subgraph_only,
            redemptions: redemption_n,
            position_snapshots: pos_n,
            ambiguous: ambiguous_n,
            canonical_total,
        },
        coverage_pct: json!({
            "trade_match_pct": coverage,
            "wallet": wallet,
        }),
        ambiguous: ambiguous
            .iter()
            .map(|a| AmbiguousCaseV1 {
                trade_key: a.trade_key.clone(),
                candidate_fill_ids: a
                    .candidates
                    .as_array()
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|x| x.as_str().map(str::to_owned))
                            .collect()
                    })
                    .unwrap_or_default(),
                reason: a.reason.clone(),
            })
            .collect(),
        notes,
    };

    CanonicalMergeArtifacts {
        events,
        source_maps,
        ambiguous,
        report,
    }
}

fn slug_for_condition(condition_id: &str, map: &HashMap<String, String>) -> Option<String> {
    let k = norm_condition(condition_id);
    map.get(&k).cloned().or_else(|| map.get(condition_id).cloned())
}

/// Aggregate volume / cash-flow proxy from `MERGED_TRADE_FILL` + `DATA_API_TRADE` rows only.
pub fn compute_shadow_metrics(events: &[CanonicalEventInsert]) -> CanonicalShadowMetrics {
    let mut n = 0_usize;
    let mut vol = 0.0_f64;
    let mut pnl = 0.0_f64;
    for e in events {
        if e.event_type != "MERGED_TRADE_FILL" && e.event_type != "DATA_API_TRADE" {
            continue;
        }
        let Some(size) = e.amounts.get("size").and_then(|x| x.as_f64()) else {
            continue;
        };
        let price = e
            .amounts
            .get("price")
            .and_then(|x| x.as_f64())
            .unwrap_or(0.0);
        let v = size * price;
        n += 1;
        vol += v;
        let side_s = e
            .amounts
            .get("side")
            .and_then(|x| x.as_str())
            .unwrap_or("Buy");
        let is_sell = side_s.contains("Sell");
        pnl += if is_sell { v } else { -v };
    }
    CanonicalShadowMetrics {
        trade_like_event_count: n,
        total_volume: vol,
        net_pnl_proxy: pnl,
        explanation: "Summed size*price on MERGED_TRADE_FILL and DATA_API_TRADE only; compare to Data API trade stream when analytics.source=data_api.".to_string(),
    }
}

/// Rebuild a minimal `Trade` list from canonical events for `analytics.source = canonical`.
pub fn synthetic_trades_from_events(
    events: &[CanonicalEventInsert],
    slug_by_condition: &HashMap<String, String>,
) -> Vec<Trade> {
    let mut out = Vec::new();
    for e in events {
        if e.event_type != "MERGED_TRADE_FILL" && e.event_type != "DATA_API_TRADE" {
            continue;
        }
        let Some(size) = e.amounts.get("size").and_then(|x| x.as_f64()) else {
            continue;
        };
        let price = e
            .amounts
            .get("price")
            .and_then(|x| x.as_f64())
            .unwrap_or(0.0);
        let side_s = e
            .amounts
            .get("side")
            .and_then(|x| x.as_str())
            .unwrap_or("Buy");
        let side = if side_s.contains("Sell") {
            TradeSide::Sell
        } else {
            TradeSide::Buy
        };
        let condition_id = e
            .condition_id
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        let slug = e
            .market_slug
            .clone()
            .or_else(|| slug_for_condition(&condition_id, slug_by_condition))
            .unwrap_or_else(|| "unknown".to_string());
        let ts_ms = e.occurred_at.timestamp() * 1000;
        out.push(Trade {
            proxy_wallet: None,
            side,
            asset: None,
            condition_id,
            size,
            price,
            timestamp: ts_ms,
            title: None,
            slug,
            event_slug: None,
            outcome: None,
            outcome_index: None,
            transaction_hash: None,
        });
    }
    out
}
