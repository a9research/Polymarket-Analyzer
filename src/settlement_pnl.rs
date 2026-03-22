//! PnL from **holding** outcome shares through Gamma market resolution (no SELL required).
//!
//! Uses Gamma `outcomePrices` as per-share redemption value (typically 0 or 1 USDC) and the same
//! avg-cost inventory as `trade_pnl`, so each open leg contributes `shares * (payout - avg_price)`.

use crate::canonical::normalize_condition_id;
use crate::polymarket::gamma_api::Market;
use std::collections::HashMap;

#[derive(Clone, Debug, Default)]
pub struct GammaResolutionPayouts {
    pub outcomes: Vec<String>,
    pub payout_by_outcome_index: Vec<f64>,
    pub clob_token_ids: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub struct SettlementBreakdown {
    pub total: f64,
    pub legs_used: usize,
    /// Largest positive settlement leg (0 if none).
    pub max_leg_win: f64,
    /// Most negative settlement leg (0 if none; otherwise ≤ 0).
    pub min_leg_loss: f64,
}

fn parse_json_string_list(raw: &str) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(raw).unwrap_or_default()
}

fn parse_outcome_prices(raw: &str) -> Vec<f64> {
    let Ok(vals) = serde_json::from_str::<Vec<serde_json::Value>>(raw) else {
        return Vec::new();
    };
    vals.into_iter()
        .filter_map(|v| {
            v.as_f64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .collect()
}

impl GammaResolutionPayouts {
    /// Build payout table when the market is closed and looks resolved (UMA resolved and/or sharp 0/1 prices).
    pub fn from_market(m: &Market) -> Option<Self> {
        if !m.closed.unwrap_or(false) {
            return None;
        }
        let outcomes_s = m.outcomes.as_ref()?.trim();
        let prices_s = m.outcome_prices.as_ref()?.trim();
        if outcomes_s.is_empty() || prices_s.is_empty() {
            return None;
        }
        let outcomes = parse_json_string_list(outcomes_s);
        let payout_by_outcome_index = parse_outcome_prices(prices_s);
        if outcomes.is_empty() || outcomes.len() != payout_by_outcome_index.len() {
            return None;
        }

        let resolved_flag = m
            .uma_resolution_status
            .as_deref()
            .map(|s| s.eq_ignore_ascii_case("resolved"))
            .unwrap_or(false);

        if !resolved_flag {
            let max = payout_by_outcome_index
                .iter()
                .copied()
                .fold(f64::NEG_INFINITY, f64::max);
            let min = payout_by_outcome_index
                .iter()
                .copied()
                .fold(f64::INFINITY, f64::min);
            if !max.is_finite() || max < 0.99 || min > 0.05 {
                return None;
            }
        }

        let clob_token_ids = m
            .clob_token_ids
            .as_ref()
            .map(|s| parse_json_string_list(s.trim()))
            .unwrap_or_default();

        Some(Self {
            outcomes,
            payout_by_outcome_index,
            clob_token_ids,
        })
    }
}

fn outcome_index_from_leg(
    leg: &str,
    outcomes: &[String],
    clob_ids: &[String],
) -> Option<usize> {
    if let Some(name) = leg.strip_prefix("o:") {
        let name = name.trim().to_lowercase();
        outcomes
            .iter()
            .position(|o| o.trim().to_lowercase() == name)
    } else if let Some(idx_s) = leg.strip_prefix("i:") {
        let i: usize = idx_s.trim().parse().ok()?;
        (i < outcomes.len()).then_some(i)
    } else if let Some(asset) = leg.strip_prefix("asset:") {
        let a = asset.trim().to_lowercase();
        if clob_ids.is_empty() {
            return None;
        }
        clob_ids
            .iter()
            .position(|c| c.trim().to_lowercase() == a)
    } else {
        None
    }
}

/// For each non-zero inventory key, if Gamma payouts exist for that market's slug, add
/// `shares * (payout - avg_price)`.
pub fn settlement_breakdown_for_open_book(
    book: &HashMap<String, (f64, f64)>,
    payouts_by_slug: &HashMap<String, GammaResolutionPayouts>,
    condition_to_slug: &HashMap<String, String>,
) -> SettlementBreakdown {
    let mut total = 0.0_f64;
    let mut legs_used = 0_usize;
    let mut max_leg_win = 0.0_f64;
    let mut min_leg_loss = 0.0_f64;

    for (key, (shares, avg_price)) in book {
        if *shares <= 1e-12 || !shares.is_finite() || !avg_price.is_finite() {
            continue;
        }
        let Some((cid, leg)) = key.split_once("::") else {
            continue;
        };
        let cid = normalize_condition_id(cid);
        let Some(slug) = condition_to_slug.get(&cid) else {
            continue;
        };
        let slug = slug.trim();
        if slug.is_empty() {
            continue;
        }
        let Some(gp) = payouts_by_slug.get(slug) else {
            continue;
        };
        let Some(oi) = outcome_index_from_leg(leg, &gp.outcomes, &gp.clob_token_ids) else {
            continue;
        };
        let Some(&payout) = gp.payout_by_outcome_index.get(oi) else {
            continue;
        };
        if !payout.is_finite() {
            continue;
        }
        let leg_pnl = *shares * (payout - *avg_price);
        if !leg_pnl.is_finite() {
            continue;
        }
        total += leg_pnl;
        legs_used += 1;
        if leg_pnl > max_leg_win {
            max_leg_win = leg_pnl;
        }
        if leg_pnl < min_leg_loss {
            min_leg_loss = leg_pnl;
        }
    }

    SettlementBreakdown {
        total,
        legs_used,
        max_leg_win,
        min_leg_loss,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_market_trump_2024_sample() {
        let m = Market {
            id: None,
            slug: None,
            question: None,
            end_date: None,
            closed_time: None,
            closed: Some(true),
            resolved_by: None,
            outcomes: Some(r#"["Yes", "No"]"#.into()),
            outcome_prices: Some(r#"["1", "0"]"#.into()),
            clob_token_ids: Some(
                r#"["21742633143463906290569050155826241533067272736897614950488156847949938836455", "48331043336612883890938759509493159234755048973500640148014422747788308965732"]"#
                    .into(),
            ),
            uma_resolution_status: Some("resolved".into()),
            creator_username: None,
            image: None,
        };
        let gp = GammaResolutionPayouts::from_market(&m).expect("parsed");
        assert_eq!(gp.outcomes.len(), 2);
        assert!((gp.payout_by_outcome_index[0] - 1.0).abs() < 1e-9);
        assert!((gp.payout_by_outcome_index[1] - 0.0).abs() < 1e-9);
    }

    #[test]
    fn settlement_buy_yes_no_sell() {
        use crate::polymarket::data_api::{Trade, TradeSide};

        let trades = vec![Trade {
            proxy_wallet: None,
            side: TradeSide::Buy,
            asset: None,
            condition_id: "0xc1".into(),
            size: 10.0,
            price: 0.4,
            timestamp: 1000,
            title: None,
            slug: "some-market".into(),
            event_slug: None,
            outcome: Some("Yes".into()),
            outcome_index: None,
            transaction_hash: None,
        }];

        let book = crate::trade_pnl::outcome_book_after_trades(&trades);
        let mut payouts: HashMap<String, GammaResolutionPayouts> = HashMap::new();
        payouts.insert(
            "some-market".into(),
            GammaResolutionPayouts {
                outcomes: vec!["Yes".into(), "No".into()],
                payout_by_outcome_index: vec![1.0, 0.0],
                clob_token_ids: vec![],
            },
        );
        let mut cond = HashMap::new();
        cond.insert(
            crate::canonical::normalize_condition_id("0xc1"),
            "some-market".into(),
        );

        let s = settlement_breakdown_for_open_book(&book, &payouts, &cond);
        assert_eq!(s.legs_used, 1);
        assert!((s.total - 10.0 * (1.0 - 0.4)).abs() < 1e-6);
    }
}
