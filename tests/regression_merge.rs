//! §1.5 P2 回归：无网络、纯内存与 JSON 固件。

use polymarket_account_analyzer::canonical::{
    build_canonical_merge, slug_map_from_canonical_events, CanonicalPipelineParams,
    ReconciliationCountsV1, RULES_VERSION_V1,
};
use polymarket_account_analyzer::config::{report_cache_fingerprint_value, AppConfig};
use polymarket_account_analyzer::polymarket::data_api::{Trade, TradeSide};
use polymarket_account_analyzer::reconciliation::{
    reconcile_v0, shadow_volume_discrepancy_note, subgraph_fetch_failed_summary,
    v1_coverage_alert_notes,
};
use serde_json::json;
use std::collections::HashMap;

fn sample_trade(tx: &str) -> Trade {
    Trade {
        proxy_wallet: None,
        side: TradeSide::Buy,
        asset: None,
        condition_id: "0xabccondition".to_string(),
        size: 10.0,
        price: 0.55,
        timestamp: 1_700_000_000,
        title: None,
        slug: "test-market-slug".to_string(),
        event_slug: None,
        outcome: None,
        outcome_index: None,
        transaction_hash: Some(tx.to_string()),
    }
}

#[test]
fn canonical_merge_exact_tx_match_counts() {
    let trades = vec![sample_trade("0xdeadbeef")];
    let fill = json!({
        "id": "fill-1",
        "timestamp": 1_700_000_000,
        "condition": "0xabccondition",
        "size": 10.0,
        "price": 0.55,
        "transactionHash": "0xdeadbeef"
    });
    let pipe = CanonicalPipelineParams::default();
    let slug = HashMap::from([(
        polymarket_account_analyzer::canonical::normalize_condition_id("0xabccondition"),
        "test-market-slug".to_string(),
    )]);
    let art = build_canonical_merge(
        "0xw",
        &trades,
        &[fill],
        &[],
        &[],
        &[],
        &slug,
        &pipe,
        RULES_VERSION_V1,
    );
    assert_eq!(art.report.counts.matched, 1, "{:?}", art.report.counts);
    assert_eq!(art.report.counts.api_only, 0);
}

#[test]
fn reconcile_v0_empty_streams() {
    let empty = json!({"data":{"orderFilledEvents":[]}});
    let r = reconcile_v0(&[], &empty, &empty, &empty, 120);
    assert_eq!(r.method, "v0_timestamp_window_one_to_one");
    assert_eq!(r.api_trade_count, 0);
}

#[test]
fn report_cache_fingerprint_includes_reconciliation_quality() {
    let v = report_cache_fingerprint_value(&AppConfig::default());
    let recon = v.get("reconciliation").expect("reconciliation");
    assert!(recon.get("shadow_volume_alert_ratio").is_some());
    assert!(recon.get("api_only_ratio_alert").is_some());
    let ing = v.get("ingestion").expect("ingestion");
    assert!(ing.get("persist_raw").is_some());
    assert!(ing.get("max_gamma_slugs_for_timing").is_some());
    assert!(ing.get("data_api_positions_limit").is_some());
    assert!(ing.get("persist_positions_raw").is_some());
    assert!(ing.get("persist_wallet_snapshots").is_some());
    assert!(ing.get("data_api_incremental_trades").is_some());
    assert!(ing.get("data_api_incremental_max_pages").is_some());
}

#[test]
fn v1_coverage_alert_notes_trigger_on_skew() {
    let c = ReconciliationCountsV1 {
        matched: 10,
        api_only: 90,
        subgraph_fill_only: 0,
        redemptions: 0,
        position_snapshots: 0,
        ambiguous: 2,
        canonical_total: 102,
    };
    let notes = v1_coverage_alert_notes(&c, 0.40, 12);
    assert!(
        notes.iter().any(|n| n.contains("api_only ratio")),
        "{notes:?}"
    );
    assert!(notes.iter().any(|n| n.contains("ambiguous")));
}

#[test]
fn subgraph_fetch_failed_summary_low_confidence() {
    let s = subgraph_fetch_failed_summary(42, "timeout");
    assert!(s.low_confidence);
    assert!(s.note.contains("timeout"));
    assert_eq!(s.api_trade_count, 42);
}

#[test]
fn shadow_volume_discrepancy_note_threshold() {
    assert!(shadow_volume_discrepancy_note(100.0, 200.0, 0.30, "data_api_trades").is_some());
    assert!(shadow_volume_discrepancy_note(100.0, 115.0, 0.30, "data_api_trades").is_none());
}

#[test]
fn slug_map_from_events_populates() {
    use chrono::Utc;
    use polymarket_account_analyzer::canonical::CanonicalEventInsert;
    use uuid::Uuid;
    let e = CanonicalEventInsert {
        id: Uuid::nil(),
        event_type: "MERGED_TRADE_FILL".to_string(),
        occurred_at: Utc::now(),
        condition_id: Some("0xabc".to_string()),
        market_slug: Some("m1".to_string()),
        amounts: serde_json::json!({}),
        source_refs: serde_json::json!([]),
    };
    let m = slug_map_from_canonical_events(&[e]);
    assert_eq!(
        m.get(&polymarket_account_analyzer::canonical::normalize_condition_id("0xabc"))
            .map(String::as_str),
        Some("m1")
    );
}
