//! Spike: verify Polymarket Goldsky subgraphs are reachable and queries match schema.
//!
//! Run:
//!   cargo run --example subgraph_spike -- 0x5924ca480d8b08cd5f3e5811fa378c4082475af6
//!   cargo run --example subgraph_spike -- --out subgraph_spike.json 0x5924ca480d8b08cd5f3e5811fa378c4082475af6
//!   cargo run --example subgraph_spike -- --introspect-redemption
//!
//! If a query fails with GraphQL errors, open the Playground for that subgraph and adjust
//! field names / `where` filters (schema versions change).

use anyhow::Context;
use clap::Parser;
use reqwest::Client;
use serde_json::{json, Map, Value};

type JsonMap = Map<String, Value>;
use std::path::PathBuf;
use std::time::Duration;

const ACTIVITY_URL: &str =
    "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/activity-subgraph/0.0.4/gn";
const ORDERBOOK_URL: &str =
    "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn";
const PNL_URL: &str =
    "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/pnl-subgraph/0.0.14/gn";

#[derive(Parser, Debug)]
#[command(about = "Polymarket subgraph spike (feasibility probe)")]
struct Args {
    /// Wallet to filter (lowercase 0x... recommended). Ignored if only --introspect-*.
    #[arg(default_value = "0x56687bf447db6ffa42ffe2204a05edaa20f55839")]
    wallet: String,

    /// Print Redemption type fields from Activity subgraph (schema discovery).
    #[arg(long, default_value_t = false)]
    introspect_redemption: bool,

    /// Skip HTTP calls to subgraphs (only useful with introspect).
    #[arg(long, default_value_t = false)]
    skip_wallet_queries: bool,

    /// Write merged JSON report to this path (avoids terminal scrollback limits).
    #[arg(long)]
    out: Option<PathBuf>,

    /// When `--out` is set, also print each section to stdout (may be truncated).
    #[arg(long, default_value_t = false)]
    tee: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let mut report = JsonMap::new();
    report.insert(
        "spike".to_string(),
        json!({ "tool": "subgraph_spike", "version": "0.1" }),
    );

    let client = Client::builder()
        .user_agent("polymarket-subgraph-spike/0.1")
        .timeout(Duration::from_secs(60))
        .connect_timeout(Duration::from_secs(15))
        .no_proxy()
        .build()
        .context("build http client")?;

    if args.introspect_redemption {
        let body = json!({
            "query": r#"query {
                __type(name: "Redemption") {
                    name
                    fields { name type { name kind ofType { name kind } } }
                }
            }"#
        });
        let v = post_graphql(&client, ACTIVITY_URL, &body).await?;
        report.insert("activity_introspect_redemption".to_string(), v.clone());
        print_section(
            "=== Activity introspect Redemption ===",
            &v,
            args.out.is_some(),
            args.tee,
        )?;
    }

    if args.skip_wallet_queries {
        return finish_report(&args, &report);
    }

    let w = args.wallet.to_lowercase();
    report.insert("wallet".to_string(), json!(w));

    // Activity: redemptions for redeemer (adjust in Playground if filter name differs)
    let activity_q = json!({
        "query": r#"query Redemptions($addr: String!, $n: Int!) {
            redemptions(first: $n, orderBy: timestamp, orderDirection: desc, where: { redeemer: $addr }) {
                id
                timestamp
                payout
                redeemer
                condition
                indexSets
            }
        }"#,
        "variables": { "addr": w.clone(), "n": 5 }
    });
    let v = post_graphql(&client, ACTIVITY_URL, &activity_q).await?;
    report.insert("activity_redemptions".to_string(), v.clone());
    print_section(
        "=== Activity redemptions (redeemer) ===",
        &v,
        args.out.is_some(),
        args.tee,
    )?;

    // Orderbook: try maker / taker filters (one may be empty depending on schema)
    let ob_maker = json!({
        "query": r#"query Fills($addr: String!, $n: Int!) {
            orderFilledEvents(first: $n, orderBy: timestamp, orderDirection: desc, where: { maker: $addr }) {
                id
                timestamp
                maker
                taker
            }
        }"#,
        "variables": { "addr": w.clone(), "n": 5 }
    });
    let v = post_graphql(&client, ORDERBOOK_URL, &ob_maker).await?;
    report.insert("orderbook_order_filled_events_maker".to_string(), v.clone());
    print_section(
        "=== Orderbook orderFilledEvents (maker) ===",
        &v,
        args.out.is_some(),
        args.tee,
    )?;

    let ob_taker = json!({
        "query": r#"query Fills($addr: String!, $n: Int!) {
            orderFilledEvents(first: $n, orderBy: timestamp, orderDirection: desc, where: { taker: $addr }) {
                id
                timestamp
                maker
                taker
            }
        }"#,
        "variables": { "addr": w.clone(), "n": 5 }
    });
    let v = post_graphql(&client, ORDERBOOK_URL, &ob_taker).await?;
    report.insert("orderbook_order_filled_events_taker".to_string(), v.clone());
    print_section(
        "=== Orderbook orderFilledEvents (taker) ===",
        &v,
        args.out.is_some(),
        args.tee,
    )?;

    // PnL subgraph: userPositions — field names may differ; adjust after introspect if needed
    let pnl_q = json!({
        "query": r#"query UserPnl($addr: String!, $n: Int!) {
            userPositions(first: $n, where: { user: $addr }) {
                id
                user
            }
        }"#,
        "variables": { "addr": w, "n": 5 }
    });
    let v = post_graphql(&client, PNL_URL, &pnl_q).await?;
    report.insert("pnl_user_positions".to_string(), v.clone());
    print_section(
        "=== PnL userPositions (user) ===",
        &v,
        args.out.is_some(),
        args.tee,
    )?;

    finish_report(&args, &report)
}

fn print_section(
    title: &str,
    v: &Value,
    out_set: bool,
    tee: bool,
) -> anyhow::Result<()> {
    if !out_set || tee {
        println!("{title}\n{}", serde_json::to_string_pretty(v)?);
    }
    Ok(())
}

fn finish_report(args: &Args, report: &JsonMap) -> anyhow::Result<()> {
    if let Some(path) = &args.out {
        let text = serde_json::to_string_pretty(&Value::Object(report.clone()))?;
        std::fs::write(path, &text).with_context(|| format!("write {}", path.display()))?;
        println!(
            "\nWrote merged report ({} bytes) to {}",
            text.len(),
            path.display()
        );
    }
    println!("Done. If you see `errors` in JSON, open the subgraph Playground and fix the query.");
    Ok(())
}

async fn post_graphql(client: &Client, url: &str, body: &serde_json::Value) -> anyhow::Result<serde_json::Value> {
    let resp = client
        .post(url)
        .header("content-type", "application/json")
        .json(body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?
        .error_for_status()
        .with_context(|| format!("non-success status from {url}"))?;
    Ok(resp.json().await?)
}
