//! Goldsky Polymarket subgraphs: redemptions, order fills (maker/taker), PnL userPositions.
//! Used by `analyze` when `[subgraph].enabled` (or CLI/API override).

use anyhow::Context;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
use reqwest::StatusCode;
use serde_json::{json, Map, Value};
use std::time::Duration;

pub const MAX_FIRST: u32 = 1000;

/// Runtime parameters for a wallet subgraph fetch (from config + overrides).
#[derive(Debug, Clone)]
pub struct SubgraphWalletParams {
    pub activity_url: String,
    pub orderbook_url: String,
    pub pnl_url: String,
    pub page_size: u32,
    pub max_pages: u32,
    pub positions_page_size: u32,
    pub skip_pnl_positions: bool,
    pub cap_rows: Option<u32>,
    pub timeout: Duration,
    pub max_retries: u32,
    pub pnl_max_retries: u32,
    pub user_agent: String,
    pub show_progress: bool,
    /// When true, request extra fill fields; auto-falls back to minimal if subgraph rejects.
    pub extended_fill_fields: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SubgraphFetchReport {
    pub activity_redemptions: Value,
    pub orderbook_order_filled_events_maker: Value,
    pub orderbook_order_filled_events_taker: Value,
    pub pnl_user_positions: Value,
    #[serde(default)]
    pub partial_failure: bool,
}

struct PaginateOpts<'a> {
    label: &'static str,
    progress: Option<&'a ProgressBar>,
    page_size: u32,
    max_pages: u32,
    cap_rows: Option<u32>,
}

impl<'a> PaginateOpts<'a> {
    fn tick(&self, pages: u32, total_rows: usize) {
        if let Some(pb) = self.progress {
            pb.set_message(format!(
                "{} · page {} · {} rows",
                self.label, pages, total_rows
            ));
        }
    }
}

fn new_spinner(show: bool) -> Option<ProgressBar> {
    if !show {
        return None;
    }
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template("{spinner:.cyan.bold} {msg}").expect("progress template"),
    );
    pb.enable_steady_tick(Duration::from_millis(100));
    Some(pb)
}

pub async fn fetch_subgraph_for_wallet(
    wallet: &str,
    p: &SubgraphWalletParams,
) -> anyhow::Result<SubgraphFetchReport> {
    let w = wallet.to_lowercase();
    let client = Client::builder()
        .user_agent(&p.user_agent)
        .timeout(p.timeout)
        .connect_timeout(Duration::from_secs(15))
        .no_proxy()
        .build()
        .context("build subgraph http client")?;

    let page_size = p.page_size.clamp(1, MAX_FIRST);
    let max_pages = p.max_pages.max(1);
    let positions_ps = p.positions_page_size.clamp(1, MAX_FIRST);
    let cap_rows = p.cap_rows;
    let mut partial_failure = false;

    // --- redemptions ---
    let pb = new_spinner(p.show_progress);
    let (rows, meta) = paginate_redemptions(
        &client,
        &p.activity_url,
        &w,
        PaginateOpts {
            label: "Activity · redemptions",
            progress: pb.as_ref(),
            page_size,
            max_pages,
            cap_rows,
        },
        p.max_retries,
    )
    .await?;
    if let Some(pb) = pb {
        pb.finish_with_message(format!("Activity · redemptions · {} rows ✓", rows.len()));
    }
    let activity_redemptions = json!({
        "data": { "redemptions": rows },
        "_spike_pagination": meta
    });

    // --- maker ---
    let pb = new_spinner(p.show_progress);
    let (rows, mut meta) = paginate_order_fills(
        &client,
        &p.orderbook_url,
        "maker",
        &w,
        PaginateOpts {
            label: "Orderbook · maker fills",
            progress: pb.as_ref(),
            page_size,
            max_pages,
            cap_rows,
        },
        p.max_retries,
        p.extended_fill_fields,
    )
    .await?;
    if let Some(pb) = pb {
        pb.finish_with_message(format!("Orderbook · maker · {} rows ✓", rows.len()));
    }
    if let Value::Object(ref mut m) = meta {
        m.insert("role".to_string(), json!("maker"));
    }
    let orderbook_maker = json!({
        "data": { "orderFilledEvents": rows },
        "_spike_pagination": meta
    });

    // --- taker ---
    let pb = new_spinner(p.show_progress);
    let (rows, mut meta) = paginate_order_fills(
        &client,
        &p.orderbook_url,
        "taker",
        &w,
        PaginateOpts {
            label: "Orderbook · taker fills",
            progress: pb.as_ref(),
            page_size,
            max_pages,
            cap_rows,
        },
        p.max_retries,
        p.extended_fill_fields,
    )
    .await?;
    if let Some(pb) = pb {
        pb.finish_with_message(format!("Orderbook · taker · {} rows ✓", rows.len()));
    }
    if let Value::Object(ref mut m) = meta {
        m.insert("role".to_string(), json!("taker"));
    }
    let orderbook_taker = json!({
        "data": { "orderFilledEvents": rows },
        "_spike_pagination": meta
    });

    // --- PnL positions ---
    let pnl_user_positions = if p.skip_pnl_positions {
        json!({
            "_spike_skipped": true,
            "reason": "skip_pnl_positions in config"
        })
    } else {
        let pb = new_spinner(p.show_progress);
        match paginate_user_positions(
            &client,
            &p.pnl_url,
            &w,
            PaginateOpts {
                label: "PnL · userPositions",
                progress: pb.as_ref(),
                page_size: positions_ps,
                max_pages,
                cap_rows,
            },
            p.pnl_max_retries,
        )
        .await
        {
            Ok((rows, meta)) => {
                if let Some(pb) = pb {
                    pb.finish_with_message(format!("PnL · userPositions · {} rows ✓", rows.len()));
                }
                json!({
                    "data": { "userPositions": rows },
                    "_spike_pagination": meta
                })
            }
            Err(e) => {
                if let Some(pb) = pb {
                    pb.finish_and_clear();
                }
                partial_failure = true;
                json!({
                    "_spike_error": format!("{e:#}"),
                    "_spike_hints": [
                        "Try smaller positions_page_size or skip_pnl_positions",
                    ]
                })
            }
        }
    };

    Ok(SubgraphFetchReport {
        activity_redemptions,
        orderbook_order_filled_events_maker: orderbook_maker,
        orderbook_order_filled_events_taker: orderbook_taker,
        pnl_user_positions,
        partial_failure,
    })
}

async fn paginate_redemptions(
    client: &Client,
    url: &str,
    addr: &str,
    opts: PaginateOpts<'_>,
    max_retries: u32,
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
        url,
        query,
        addr,
        &["data", "redemptions"],
        &opts,
        max_retries,
        "activity_redemptions",
    )
    .await
}

const FILL_MAKER_MIN: &str = r#"query Fills($addr: String!, $first: Int!, $skip: Int!) {
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
            }"#;

const FILL_MAKER_EXT: &str = r#"query Fills($addr: String!, $first: Int!, $skip: Int!) {
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
                    transactionHash
                    condition
                    size
                    price
                }
            }"#;

const FILL_TAKER_MIN: &str = r#"query Fills($addr: String!, $first: Int!, $skip: Int!) {
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
            }"#;

const FILL_TAKER_EXT: &str = r#"query Fills($addr: String!, $first: Int!, $skip: Int!) {
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
                    transactionHash
                    condition
                    size
                    price
                }
            }"#;

async fn paginate_order_fills(
    client: &Client,
    url: &str,
    role: &str,
    addr: &str,
    opts: PaginateOpts<'_>,
    max_retries: u32,
    try_extended: bool,
) -> anyhow::Result<(Vec<Value>, Value)> {
    let (q_ext, q_min) = match role {
        "maker" => (FILL_MAKER_EXT, FILL_MAKER_MIN),
        "taker" => (FILL_TAKER_EXT, FILL_TAKER_MIN),
        _ => anyhow::bail!("role must be maker or taker"),
    };
    let source = match role {
        "maker" => "orderbook_fills_maker",
        "taker" => "orderbook_fills_taker",
        _ => "orderbook_fills",
    };

    if try_extended {
        match paginate_graphql(
            client,
            url,
            q_ext,
            addr,
            &["data", "orderFilledEvents"],
            &opts,
            max_retries,
            source,
        )
        .await
        {
            Ok(r) => return Ok(r),
            Err(e) => {
                let msg = format!("{e:#}");
                let msg_lc = msg.to_lowercase();
                // Goldsky / schema variants: "Cannot query field …" or "Type `OrderFilledEvent` has no field `size`"
                let extended_fields_rejected = msg_lc.contains("cannot query field")
                    || msg_lc.contains("has no field")
                    || msg_lc.contains("unknown field");
                if extended_fields_rejected {
                    tracing::warn!(
                        "subgraph orderFilledEvents extended fields not supported; using minimal query ({role})"
                    );
                } else {
                    return Err(e);
                }
            }
        }
    }

    paginate_graphql(
        client,
        url,
        q_min,
        addr,
        &["data", "orderFilledEvents"],
        &opts,
        max_retries,
        source,
    )
    .await
}

async fn paginate_user_positions(
    client: &Client,
    pnl_url: &str,
    addr: &str,
    opts: PaginateOpts<'_>,
    pnl_max_retries: u32,
) -> anyhow::Result<(Vec<Value>, Value)> {
    let page_size = opts.page_size;
    let max_pages = opts.max_pages;
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
    let mut hit_cap = false;

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

        let v = post_graphql_pnl(client, pnl_url, &body, pnl_max_retries).await?;
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
        opts.tick(pages, all.len());

        if let Some(cap) = opts.cap_rows {
            if cap > 0 && all.len() >= cap as usize {
                all.truncate(cap as usize);
                hit_cap = true;
                break;
            }
        }
        if n < page_size as usize {
            break;
        }
        cursor = Some(last_id);
    }

    let stopped = if hit_cap {
        "cap_rows"
    } else if pages >= max_pages {
        "max_pages"
    } else if all.is_empty() {
        "empty"
    } else {
        "short_page_or_empty"
    };

    let mut meta_map = Map::new();
    meta_map.insert("pages_fetched".into(), json!(pages));
    meta_map.insert("total_rows".into(), json!(all.len()));
    meta_map.insert("page_size".into(), json!(page_size));
    meta_map.insert("max_pages_cap".into(), json!(max_pages));
    meta_map.insert("pagination".into(), json!("cursor_id_gt"));
    meta_map.insert("stopped_because".into(), json!(stopped));
    if let Some(c) = opts.cap_rows {
        meta_map.insert("cap_rows_requested".into(), json!(c));
    }

    Ok((all, Value::Object(meta_map)))
}

async fn paginate_graphql(
    client: &Client,
    url: &str,
    query: &str,
    addr: &str,
    path: &[&str],
    opts: &PaginateOpts<'_>,
    max_retries: u32,
    _source: &str,
) -> anyhow::Result<(Vec<Value>, Value)> {
    let page_size = opts.page_size;
    let max_pages = opts.max_pages;
    let mut all: Vec<Value> = Vec::new();
    let mut skip: u32 = 0;
    let mut pages: u32 = 0;
    let mut hit_cap = false;

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
        let v = post_graphql(client, url, &body, max_retries).await?;
        let chunk = extract_array_at_path(&v, path)
            .with_context(|| format!("missing array at {:?} in response", path))?;
        let n = chunk.len();
        all.extend(chunk);
        pages += 1;
        opts.tick(pages, all.len());
        if let Some(cap) = opts.cap_rows {
            if cap > 0 && all.len() >= cap as usize {
                all.truncate(cap as usize);
                hit_cap = true;
                break;
            }
        }
        if n < page_size as usize {
            break;
        }
        skip = skip.saturating_add(page_size);
    }

    let stopped = if hit_cap {
        "cap_rows"
    } else if pages >= max_pages {
        "max_pages"
    } else {
        "short_page_or_empty"
    };

    let mut meta_map = Map::new();
    meta_map.insert("pages_fetched".into(), json!(pages));
    meta_map.insert("total_rows".into(), json!(all.len()));
    meta_map.insert("page_size".into(), json!(page_size));
    meta_map.insert("max_pages_cap".into(), json!(max_pages));
    meta_map.insert("pagination".into(), json!("skip"));
    meta_map.insert("stopped_because".into(), json!(stopped));
    if let Some(c) = opts.cap_rows {
        meta_map.insert("cap_rows_requested".into(), json!(c));
    }

    Ok((all, Value::Object(meta_map)))
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

async fn post_graphql(
    client: &Client,
    url: &str,
    body: &Value,
    max_attempts: u32,
) -> anyhow::Result<Value> {
    post_graphql_retry(client, url, body, max_attempts.max(1), 500).await
}

async fn post_graphql_pnl(
    client: &Client,
    url: &str,
    body: &Value,
    max_attempts: u32,
) -> anyhow::Result<Value> {
    post_graphql_retry(
        client,
        url,
        body,
        max_attempts.max(1),
        1500,
    )
    .await
}

async fn post_graphql_retry(
    client: &Client,
    url: &str,
    body: &Value,
    max_attempts: u32,
    initial_backoff_ms: u64,
) -> anyhow::Result<Value> {
    let mut backoff_ms = initial_backoff_ms;
    for attempt in 0..max_attempts {
        match post_graphql_once(client, url, body).await {
            Ok(v) => return Ok(v),
            Err(e) => {
                let retriable = anyhow_chain_is_retriable(&e);
                if attempt + 1 < max_attempts && retriable {
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(20_000);
                    continue;
                }
                return Err(e);
            }
        }
    }
    unreachable!("post_graphql_retry loop always returns")
}

async fn post_graphql_once(client: &Client, url: &str, body: &Value) -> anyhow::Result<Value> {
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
