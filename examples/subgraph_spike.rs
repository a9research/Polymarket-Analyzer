//! Spike: verify Polymarket Goldsky subgraphs are reachable and queries match schema.
//!
//! Run:
//!   cargo run --example subgraph_spike -- 0x5924ca480d8b08cd5f3e5811fa378c4082475af6
//!   cargo run --example subgraph_spike -- --out subgraph_spike.json 0x5924...
//!   cargo run --example subgraph_spike -- --quick 0x5924...   # only 5 rows each
//!   cargo run --example subgraph_spike -- --skip-pnl-positions --out full.json 0x...  # no PnL positions
//!   cargo run --example subgraph_spike -- --introspect-redemption
//!
//! If a query fails with GraphQL errors, open the Playground for that subgraph and adjust
//! field names / `where` filters (schema versions change).

use anyhow::Context;
use clap::Parser;
use reqwest::Client;
use reqwest::StatusCode;
use serde_json::{json, Map, Value};
use std::path::PathBuf;
use std::time::Duration;

type JsonMap = Map<String, Value>;

const ACTIVITY_URL: &str =
    "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/activity-subgraph/0.0.4/gn";
const ORDERBOOK_URL: &str =
    "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn";
const PNL_URL: &str =
    "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/pnl-subgraph/0.0.14/gn";

/// Subgraph `first` is often capped at 1000.
const MAX_FIRST: u32 = 1000;

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

    /// Only fetch 5 rows per endpoint (fast smoke test).
    #[arg(long, default_value_t = false)]
    quick: bool,

    /// Page size for pagination (`first`); clamped to 1..=1000.
    #[arg(long, default_value_t = 1000)]
    page_size: u32,

    /// Safety stop after this many pages per entity stream.
    #[arg(long, default_value_t = 500)]
    max_pages: u32,

    /// Skip PnL subgraph `userPositions` (Activity + Orderbook still fully paginated).
    /// Use when you only need redemptions/fills, or to avoid rare subgraph timeouts.
    #[arg(long, default_value_t = false)]
    skip_pnl_positions: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let mut report = JsonMap::new();
    report.insert(
        "spike".to_string(),
        json!({
            "tool": "subgraph_spike",
            "version": "0.3",
            "quick": args.quick,
            "page_size": args.page_size.min(MAX_FIRST).max(1),
            "max_pages": args.max_pages,
            "skip_pnl_positions": args.skip_pnl_positions,
        }),
    );

    let timeout_sec = if args.quick { 60 } else { 300 };
    let client = Client::builder()
        .user_agent("polymarket-subgraph-spike/0.3")
        .timeout(Duration::from_secs(timeout_sec))
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
    report.insert("wallet".to_string(), json!(w.clone()));

    let page_size = args.page_size.clamp(1, MAX_FIRST);
    let max_pages = args.max_pages.max(1);

    if args.quick {
        // Single page of 5 (legacy smoke test)
        let n = 5u32;
        let activity_q = json!({
            "query": r#"query Redemptions($addr: String!, $n: Int!) {
                redemptions(first: $n, orderBy: timestamp, orderDirection: desc, where: { redeemer: $addr }) {
                    id timestamp payout redeemer condition indexSets
                }
            }"#,
            "variables": { "addr": w.clone(), "n": n }
        });
        let v = post_graphql(&client, ACTIVITY_URL, &activity_q).await?;
        report.insert("activity_redemptions".to_string(), v.clone());
        print_section(
            "=== Activity redemptions (redeemer) ===",
            &v,
            args.out.is_some(),
            args.tee,
        )?;

        let ob_maker = json!({
            "query": r#"query Fills($addr: String!, $n: Int!) {
                orderFilledEvents(first: $n, orderBy: timestamp, orderDirection: desc, where: { maker: $addr }) {
                    id timestamp maker taker
                }
            }"#,
            "variables": { "addr": w.clone(), "n": n }
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
                    id timestamp maker taker
                }
            }"#,
            "variables": { "addr": w.clone(), "n": n }
        });
        let v = post_graphql(&client, ORDERBOOK_URL, &ob_taker).await?;
        report.insert("orderbook_order_filled_events_taker".to_string(), v.clone());
        print_section(
            "=== Orderbook orderFilledEvents (taker) ===",
            &v,
            args.out.is_some(),
            args.tee,
        )?;

        let pnl_q = json!({
            "query": r#"query UserPnl($addr: String!, $n: Int!) {
                userPositions(first: $n, where: { user: $addr }) { id user }
            }"#,
            "variables": { "addr": w, "n": n }
        });
        let v = post_graphql(&client, PNL_URL, &pnl_q).await?;
        report.insert("pnl_user_positions".to_string(), v.clone());
        print_section(
            "=== PnL userPositions (user) ===",
            &v,
            args.out.is_some(),
            args.tee,
        )?;
    } else {
        let (rows, meta) = paginate_redemptions(&client, &w, page_size, max_pages).await?;
        let v = json!({
            "data": { "redemptions": rows },
            "_spike_pagination": meta
        });
        report.insert("activity_redemptions".to_string(), v.clone());
        print_section(
            &format!(
                "=== Activity redemptions (full paginated, {} rows) ===",
                rows.len()
            ),
            &v,
            args.out.is_some(),
            args.tee,
        )?;

        let (rows, meta) =
            paginate_order_fills(&client, ORDERBOOK_URL, "maker", &w, page_size, max_pages).await?;
        let v = json!({
            "data": { "orderFilledEvents": rows },
            "_spike_pagination": meta
        });
        report.insert("orderbook_order_filled_events_maker".to_string(), v.clone());
        print_section(
            &format!(
                "=== Orderbook fills as maker (full paginated, {} rows) ===",
                rows.len()
            ),
            &v,
            args.out.is_some(),
            args.tee,
        )?;

        let (rows, meta) =
            paginate_order_fills(&client, ORDERBOOK_URL, "taker", &w, page_size, max_pages).await?;
        let v = json!({
            "data": { "orderFilledEvents": rows },
            "_spike_pagination": meta
        });
        report.insert("orderbook_order_filled_events_taker".to_string(), v.clone());
        print_section(
            &format!(
                "=== Orderbook fills as taker (full paginated, {} rows) ===",
                rows.len()
            ),
            &v,
            args.out.is_some(),
            args.tee,
        )?;

        if args.skip_pnl_positions {
            let v = json!({
                "_spike_skipped": true,
                "reason": "--skip-pnl-positions"
            });
            report.insert("pnl_user_positions".to_string(), v.clone());
            print_section(
                "=== PnL userPositions (skipped) ===",
                &v,
                args.out.is_some(),
                args.tee,
            )?;
        } else {
            let (rows, meta) = paginate_user_positions(&client, &w, page_size, max_pages).await?;
            let v = json!({
                "data": { "userPositions": rows },
                "_spike_pagination": meta
            });
            report.insert("pnl_user_positions".to_string(), v.clone());
            print_section(
                &format!(
                    "=== PnL userPositions (full paginated, {} rows) ===",
                    rows.len()
                ),
                &v,
                args.out.is_some(),
                args.tee,
            )?;
        }
    }

    finish_report(&args, &report)
}

async fn paginate_redemptions(
    client: &Client,
    addr: &str,
    page_size: u32,
    max_pages: u32,
) -> anyhow::Result<(Vec<Value>, Value)> {
    let query = r#"query Redemptions($addr: String!, $first: Int!, $skip: Int!) {
        redemptions(
            first: $first
            skip: $skip
            orderBy: timestamp
            orderDirection: desc
            where: { redeemer: $addr }
        ) {
            id
            timestamp
            payout
            redeemer
            condition
            indexSets
        }
    }"#;

    paginate_graphql(
        client,
        ACTIVITY_URL,
        query,
        addr,
        page_size,
        max_pages,
        &["data", "redemptions"],
    )
    .await
}

async fn paginate_order_fills(
    client: &Client,
    url: &str,
    role: &str,
    addr: &str,
    page_size: u32,
    max_pages: u32,
) -> anyhow::Result<(Vec<Value>, Value)> {
    let (query, where_field) = match role {
        "maker" => (
            r#"query Fills($addr: String!, $first: Int!, $skip: Int!) {
                orderFilledEvents(
                    first: $first
                    skip: $skip
                    orderBy: timestamp
                    orderDirection: desc
                    where: { maker: $addr }
                ) {
                    id
                    timestamp
                    maker
                    taker
                }
            }"#,
            "maker",
        ),
        "taker" => (
            r#"query Fills($addr: String!, $first: Int!, $skip: Int!) {
                orderFilledEvents(
                    first: $first
                    skip: $skip
                    orderBy: timestamp
                    orderDirection: desc
                    where: { taker: $addr }
                ) {
                    id
                    timestamp
                    maker
                    taker
                }
            }"#,
            "taker",
        ),
        _ => anyhow::bail!("role must be maker or taker"),
    };

    let (rows, mut meta) = paginate_graphql(client, url, query, addr, page_size, max_pages, &[
        "data",
        "orderFilledEvents",
    ])
    .await?;
    if let Value::Object(ref mut m) = meta {
        m.insert("role".to_string(), json!(where_field));
    }
    Ok((rows, meta))
}

/// Paginate `userPositions` by **cursor** (`id_gt` + `orderBy: id`), avoiding deep `skip` which
/// triggers statement timeouts on hosted subgraphs for active wallets.
async fn paginate_user_positions(
    client: &Client,
    addr: &str,
    page_size: u32,
    max_pages: u32,
) -> anyhow::Result<(Vec<Value>, Value)> {
    const QUERY_FIRST: &str = r#"query UserPnlFirst($addr: String!, $first: Int!) {
        userPositions(
            first: $first
            orderBy: id
            orderDirection: asc
            where: { user: $addr }
        ) {
            id
            user
        }
    }"#;

    const QUERY_AFTER: &str = r#"query UserPnlAfter($addr: String!, $first: Int!, $idGt: String!) {
        userPositions(
            first: $first
            orderBy: id
            orderDirection: asc
            where: { user: $addr, id_gt: $idGt }
        ) {
            id
            user
        }
    }"#;

    let mut all: Vec<Value> = Vec::new();
    let mut pages: u32 = 0;
    let mut cursor: Option<String> = None;

    loop {
        if pages >= max_pages {
            break;
        }

        let body = if let Some(ref id) = cursor {
            json!({
                "query": QUERY_AFTER,
                "variables": {
                    "addr": addr,
                    "first": page_size,
                    "idGt": id
                }
            })
        } else {
            json!({
                "query": QUERY_FIRST,
                "variables": { "addr": addr, "first": page_size }
            })
        };

        let v = post_graphql(client, PNL_URL, &body).await?;
        let chunk = extract_array_at_path(&v, &["data", "userPositions"])
            .with_context(|| "missing data.userPositions in PnL response")?;
        let n = chunk.len();
        if n == 0 {
            break;
        }

        let last_id = chunk
            .last()
            .and_then(|row| row.get("id"))
            .and_then(|x| x.as_str())
            .map(str::to_owned)
            .context("userPositions row missing string id")?;

        all.extend(chunk);
        pages += 1;

        if n < page_size as usize {
            break;
        }
        cursor = Some(last_id);
    }

    let stopped = if pages >= max_pages {
        "max_pages"
    } else if all.is_empty() {
        "empty"
    } else {
        "short_page_or_empty"
    };

    let meta = json!({
        "pages_fetched": pages,
        "total_rows": all.len(),
        "page_size": page_size,
        "max_pages_cap": max_pages,
        "pagination": "cursor_id_gt",
        "stopped_because": stopped
    });

    Ok((all, meta))
}

/// Paginate until a page returns fewer than `page_size` items, or `max_pages` reached.
async fn paginate_graphql(
    client: &Client,
    url: &str,
    query: &str,
    addr: &str,
    page_size: u32,
    max_pages: u32,
    path: &[&str],
) -> anyhow::Result<(Vec<Value>, Value)> {
    let mut all: Vec<Value> = Vec::new();
    let mut skip: u32 = 0;
    let mut pages: u32 = 0;

    loop {
        if pages >= max_pages {
            break;
        }
        let body = json!({
            "query": query,
            "variables": {
                "addr": addr,
                "first": page_size,
                "skip": skip
            }
        });
        let v = post_graphql(client, url, &body).await?;
        let chunk = extract_array_at_path(&v, path)
            .with_context(|| format!("missing array at {:?} in response", path))?;
        let n = chunk.len();
        all.extend(chunk);
        pages += 1;
        if n < page_size as usize {
            break;
        }
        skip = skip.saturating_add(page_size);
    }

    let meta = json!({
        "pages_fetched": pages,
        "total_rows": all.len(),
        "page_size": page_size,
        "max_pages_cap": max_pages,
        "pagination": "skip",
        "stopped_because": if pages >= max_pages { "max_pages" } else { "short_page_or_empty" }
    });

    Ok((all, meta))
}

fn extract_array_at_path(v: &Value, path: &[&str]) -> anyhow::Result<Vec<Value>> {
    let mut cur = v;
    for key in path {
        cur = cur
            .get(*key)
            .with_context(|| format!("path segment {:?}", key))?;
    }
    cur.as_array()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("not an array at end of path"))
}

fn print_section(
    title: &str,
    v: &Value,
    out_set: bool,
    tee: bool,
) -> anyhow::Result<()> {
    if !out_set || tee {
        println!("{title}\n{}", serde_json::to_string_pretty(v)?);
    } else {
        println!("{title}");
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

fn graphql_error_is_retriable(errs: &[Value]) -> bool {
    let s = serde_json::to_string(&Value::Array(errs.to_vec()))
        .unwrap_or_default()
        .to_lowercase();
    s.contains("timeout")
        || s.contains("timed out")
        || s.contains("429")
        || s.contains("too many")
        || s.contains("rate limit")
        || s.contains("503")
        || s.contains("502")
        || s.contains("504")
}

fn anyhow_chain_is_retriable(e: &anyhow::Error) -> bool {
    let s = format!("{e:#}").to_lowercase();
    s.contains("timeout")
        || s.contains("timed out")
        || s.contains("statement timeout")
        || s.contains("429")
        || s.contains("too many")
        || s.contains("rate limit")
        || s.contains("503")
        || s.contains("502")
        || s.contains("504")
}

/// POST with limited retries for transient timeouts / rate limits (hosted subgraph).
async fn post_graphql(client: &Client, url: &str, body: &serde_json::Value) -> anyhow::Result<Value> {
    const MAX_ATTEMPTS: u32 = 3;
    let mut backoff_ms: u64 = 500;

    for attempt in 0..MAX_ATTEMPTS {
        match post_graphql_once(client, url, body).await {
            Ok(v) => return Ok(v),
            Err(e) => {
                let retriable = anyhow_chain_is_retriable(&e);
                if attempt + 1 < MAX_ATTEMPTS && retriable {
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(8000);
                    continue;
                }
                return Err(e);
            }
        }
    }
    unreachable!("post_graphql loop always returns")
}

async fn post_graphql_once(
    client: &Client,
    url: &str,
    body: &serde_json::Value,
) -> anyhow::Result<Value> {
    let resp = client
        .post(url)
        .header("content-type", "application/json")
        .json(body)
        .send()
        .await
        .with_context(|| format!("POST {url}"))?;

    let status = resp.status();
    let bytes = resp.bytes().await.with_context(|| format!("read body {url}"))?;

    if status == StatusCode::TOO_MANY_REQUESTS {
        anyhow::bail!("HTTP 429 Too Many Requests from {url}");
    }
    if !status.is_success() {
        let text = String::from_utf8_lossy(&bytes);
        let snippet: String = text.chars().take(800).collect();
        anyhow::bail!("HTTP {status} from {url}: {snippet}");
    }

    let v: Value = serde_json::from_slice(&bytes).with_context(|| format!("JSON from {url}"))?;

    if let Some(errs) = v.get("errors").and_then(|e| e.as_array()) {
        if !errs.is_empty() {
            let pretty = serde_json::to_string_pretty(errs)?;
            if graphql_error_is_retriable(errs.as_slice()) {
                anyhow::bail!("GraphQL errors (retriable): {pretty}");
            }
            anyhow::bail!("GraphQL errors: {pretty}");
        }
    }
    Ok(v)
}
