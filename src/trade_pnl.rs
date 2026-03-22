//! Per-trade **realized** PnL from the Data API trade stream using average-cost inventory
//! per outcome position. Buys contribute `pnl = 0`; sells realize `(price - avg_cost) * size_sold`.
//!
//! **Inventory key:** prefer `asset` (outcome token id) when present so BUY rows that carry
//! `outcome: "Yes"` and SELL rows that only have `outcome_index` still share one book.

use crate::polymarket::data_api::{Trade, TradeSide};
use std::collections::HashMap;

fn trade_ts_sec(t: &Trade) -> i64 {
    let ts = t.timestamp;
    if ts > 1_000_000_000_000 {
        ts / 1000
    } else {
        ts
    }
}

fn position_key(t: &Trade) -> String {
    if let Some(ref a) = t.asset {
        let a = a.trim();
        if !a.is_empty() {
            return format!("asset:{}", a.to_lowercase());
        }
    }
    let leg = if let Some(ref o) = t.outcome {
        let s = o.trim();
        if s.is_empty() {
            t.outcome_index
                .map(|i| format!("i:{i}"))
                .unwrap_or_else(|| "unknown".into())
        } else {
            format!("o:{}", s.to_lowercase())
        }
    } else if let Some(i) = t.outcome_index {
        format!("i:{i}")
    } else {
        "unknown".into()
    };
    format!("{}::{}", t.condition_id, leg)
}

struct BookPos {
    shares: f64,
    avg_price: f64,
}

/// Returns one realized PnL value per input trade, in **the same order** as `trades`.
///
/// Model: long-only inventory per market outcome; sells close against average cost.
/// Sells larger than held shares only realize on the covered portion (rest treated as anomaly, 0 PnL).
pub fn per_trade_realized_pnl(trades: &[Trade]) -> Vec<f64> {
    let mut order: Vec<usize> = (0..trades.len()).collect();
    order.sort_by(|&a, &b| {
        let ta = trade_ts_sec(&trades[a]);
        let tb = trade_ts_sec(&trades[b]);
        ta.cmp(&tb).then_with(|| {
            trades[a]
                .transaction_hash
                .cmp(&trades[b].transaction_hash)
        }).then_with(|| trades[a].slug.cmp(&trades[b].slug))
            .then_with(|| a.cmp(&b))
    });

    let mut out = vec![0.0_f64; trades.len()];
    let mut book: HashMap<String, BookPos> = HashMap::new();

    for &i in &order {
        let t = &trades[i];
        let key = position_key(t);
        let size = t.size;
        let price = t.price;
        if size <= 0.0 || !price.is_finite() || !size.is_finite() {
            continue;
        }

        match t.side {
            TradeSide::Buy => {
                let p = book.entry(key).or_insert(BookPos {
                    shares: 0.0,
                    avg_price: 0.0,
                });
                let new_shares = p.shares + size;
                if new_shares > 1e-12 {
                    p.avg_price = (p.shares * p.avg_price + size * price) / new_shares;
                } else {
                    p.avg_price = price;
                }
                p.shares = new_shares;
                out[i] = 0.0;
            }
            TradeSide::Sell => {
                let p = book.entry(key).or_insert(BookPos {
                    shares: 0.0,
                    avg_price: 0.0,
                });
                if p.shares <= 1e-12 {
                    out[i] = 0.0;
                    continue;
                }
                let sell_qty = size.min(p.shares);
                let realized = sell_qty * (price - p.avg_price);
                out[i] = if realized.is_finite() { realized } else { 0.0 };
                p.shares -= sell_qty;
            }
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t_buy(size: f64, price: f64, ts: i64) -> Trade {
        Trade {
            proxy_wallet: None,
            side: TradeSide::Buy,
            asset: None,
            condition_id: "c1".into(),
            size,
            price,
            timestamp: ts,
            title: None,
            slug: "m1".into(),
            event_slug: None,
            outcome: Some("Yes".into()),
            outcome_index: None,
            transaction_hash: None,
        }
    }

    fn t_sell(size: f64, price: f64, ts: i64) -> Trade {
        Trade {
            side: TradeSide::Sell,
            timestamp: ts,
            ..t_buy(size, price, ts)
        }
    }

    #[test]
    fn buy_then_sell_realized() {
        let trades = vec![t_buy(10.0, 0.5, 1000), t_sell(10.0, 0.7, 2000)];
        let p = per_trade_realized_pnl(&trades);
        assert!((p[0] - 0.0).abs() < 1e-9);
        assert!((p[1] - 2.0).abs() < 1e-9);
    }

    /// BUY 带 `outcome`、SELL 仅带 `outcome_index` 时，旧逻辑会拆成两个 book；用同一 `asset` 应对齐。
    #[test]
    fn outcome_vs_outcome_index_same_asset_reconciles() {
        let mut b = t_buy(10.0, 0.4, 100);
        b.asset = Some("0xabc_token_yes".into());
        b.outcome = Some("Yes".into());
        b.outcome_index = None;

        let mut s = t_sell(10.0, 0.6, 200);
        s.asset = Some("0xabc_token_yes".into());
        s.outcome = None;
        s.outcome_index = Some(0);

        let trades = vec![b, s];
        let p = per_trade_realized_pnl(&trades);
        assert!(p[0].abs() < 1e-9);
        let expected = 10.0 * (0.6 - 0.4);
        assert!(
            (p[1] - expected).abs() < 1e-6,
            "got {} want {}",
            p[1],
            expected
        );
    }

    #[test]
    fn partial_sell_then_rest() {
        let trades = vec![
            t_buy(10.0, 0.4, 100),
            t_sell(4.0, 0.5, 200),
            t_sell(6.0, 0.6, 300),
        ];
        let p = per_trade_realized_pnl(&trades);
        assert!((p[0]).abs() < 1e-9);
        assert!((p[1] - 4.0 * (0.5 - 0.4)).abs() < 1e-9);
        assert!((p[2] - 6.0 * (0.6 - 0.4)).abs() < 1e-9);
    }
}
