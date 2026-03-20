//! Spike: verify Polymarket Goldsky subgraphs are reachable and queries match schema.
//!
//! Run:
//!   cargo run --example subgraph_spike -- 0x5924ca480d8b08cd5f3e5811fa378c4082475af6
//!   cargo run --example subgraph_spike -- --out subgraph_spike.json 0x5924...
//!   cargo run --example subgraph_spike -- --quick 0x5924...   # only 5 rows each
//!   cargo run --example subgraph_spike -- --skip-pnl-positions --out full.json 0x...  # no PnL positions
//!   cargo run --example subgraph_spike -- --out full.json --flush-every-pages 25 0x...  # intra-stream snapshots
//!   cargo run --example subgraph_spike -- --no-progress --out full.json 0x...  # no spinner
//!   cargo run --example subgraph_spike -- --introspect-redemption
//!
//! If a query fails with GraphQL errors, open the Playground for that subgraph and adjust
//! field names / `where` filters (schema versions change).

use anyhow::Context;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
use reqwest::StatusCode;
use serde_json::{json, Map, Value};
use std::path::{Path, PathBuf};
use std::time::Duration;

type JsonMap = Map<String, Value>;

#[derive(Clone, Copy)]
enum SpikePaginationKind {
    Skip,
    CursorIdGt,
}

/// When `flush_every_pages > 0`, rewrite `--out` during pagination (crash recovery; can be slow).
struct FlushConfig<'a> {
    path: &'a Path,
    report: &'a mut JsonMap,
    section_key: &'static str,
    data_field: &'static str,
    kind: SpikePaginationKind,
    role: Option<&'static str>,
}

struct PaginateOpts<'a> {
    label: &'static str,
    progress: Option<&'a ProgressBar>,
    flush_every: u32,
    flush: Option<FlushConfig<'a>>,
    page_size: u32,
    max_pages: u32,
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

    fn maybe_flush(&mut self, rows: &[Value], pages: u32) -> anyhow::Result<()> {
        if self.flush_every == 0 || pages % self.flush_every != 0 {
            return Ok(());
        }
        if let Some(ref mut f) = self.flush {
            apply_section_flush(f, rows, pages, self.page_size, self.max_pages, true)?;
        }
        Ok(())
    }
}

fn new_section_spinner(no_progress: bool) -> Option<ProgressBar> {
    if no_progress {
        return None;
    }
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template("{spinner:.cyan.bold} {msg}")
            .expect("progress template"),
    );
    pb.enable_steady_tick(Duration::from_millis(100));
    Some(pb)
}

fn write_report_atomic(path: &Path, report: &JsonMap) -> anyhow::Result<()> {
    let tmp = path.with_extension("json.tmp");
    let text =
        serde_json::to_string_pretty(&Value::Object(report.clone())).context("serialize report")?;
    std::fs::write(&tmp, &text).with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("rename to {}", path.display()))?;
    Ok(())
}

fn apply_section_flush(
    f: &mut FlushConfig,
    rows: &[Value],
    pages: u32,
    page_size: u32,
    max_pages: u32,
    in_progress: bool,
) -> anyhow::Result<()> {
    let stopped = if in_progress {
        "in_progress"
    } else if pages >= max_pages {
        "max_pages"
    } else if rows.is_empty() {
        "empty"
    } else {
        "short_page_or_empty"
    };
    let pagination_str = match f.kind {
        SpikePaginationKind::Skip => "skip",
        SpikePaginationKind::CursorIdGt => "cursor_id_gt",
    };
    let mut meta = Map::new();
    meta.insert("pages_fetched".into(), json!(pages));
    meta.insert("total_rows".into(), json!(rows.len()));
    meta.insert("page_size".into(), json!(page_size));
    meta.insert("max_pages_cap".into(), json!(max_pages));
    meta.insert("pagination".into(), json!(pagination_str));
    meta.insert("stopped_because".into(), json!(stopped));
    if let Some(role) = f.role {
        meta.insert("role".into(), json!(role));
    }
    let mut data = Map::new();
    data.insert(f.data_field.to_string(), json!(rows));
    let section = json!({
        "data": Value::Object(data),
        "_spike_pagination": Value::Object(meta)
    });
    f.report.insert(f.section_key.to_string(), section);
    write_report_atomic(f.path, f.report)
}

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

    /// Page size for PnL `userPositions` only (smaller = lighter queries; default 100).
    /// Activity / Orderbook still use `--page-size`.
    #[arg(long, default_value_t = 100)]
    positions_page_size: u32,

    /// Skip PnL subgraph `userPositions` (Activity + Orderbook still fully paginated).
    /// Use when you only need redemptions/fills, or to avoid rare subgraph timeouts.
    #[arg(long, default_value_t = false)]
    skip_pnl_positions: bool,

    /// Disable terminal spinner / progress messages (for CI or logs).
    #[arg(long, default_value_t = false)]
    no_progress: bool,

    /// Rewrite `--out` every N pages **during** each stream (0 = off). Heavy for huge JSON;
    /// use for crash recovery. Each completed stream still flushes once without this.
    #[arg(long, default_value_t = 0)]
    flush_every_pages: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let mut report = JsonMap::new();
    let positions_ps = args.positions_page_size.clamp(1, MAX_FIRST);
    report.insert(
        "spike".to_string(),
        json!({
            "tool": "subgraph_spike",
            "version": "0.5",
            "quick": args.quick,
            "page_size": args.page_size.min(MAX_FIRST).max(1),
            "positions_page_size": positions_ps,
            "max_pages": args.max_pages,
            "skip_pnl_positions": args.skip_pnl_positions,
            "no_progress": args.no_progress,
            "flush_every_pages": args.flush_every_pages,
        }),
    );

    let timeout_sec = if args.quick { 60 } else { 600 };
    let client = Client::builder()
        .user_agent("polymarket-subgraph-spike/0.5")
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
    let mut pnl_section_failed: Option<anyhow::Error> = None;

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
        let flush_every = args.flush_every_pages;
        let out_path = args.out.as_deref();

        let flush_redemptions = match (out_path, flush_every > 0) {
            (Some(path), true) => Some(FlushConfig {
                path,
                report: &mut report,
                section_key: "activity_redemptions",
                data_field: "redemptions",
                kind: SpikePaginationKind::Skip,
                role: None,
            }),
            _ => None,
        };
        let pb = new_section_spinner(args.no_progress);
        let (rows, meta) = paginate_redemptions(
            &client,
            &w,
            PaginateOpts {
                label: "Activity · redemptions",
                progress: pb.as_ref(),
                flush_every,
                flush: flush_redemptions,
                page_size,
                max_pages,
            },
        )
        .await?;
        if let Some(pb) = pb {
            pb.finish_with_message(format!(
                "Activity · redemptions · {} rows ✓",
                rows.len()
            ));
        }
        let v = json!({
            "data": { "redemptions": rows },
            "_spike_pagination": meta
        });
        report.insert("activity_redemptions".to_string(), v.clone());
        if let Some(p) = out_path {
            write_report_atomic(p, &report)?;
        }
        print_section(
            &format!(
                "=== Activity redemptions (full paginated, {} rows) ===",
                rows.len()
            ),
            &v,
            args.out.is_some(),
            args.tee,
        )?;

        let flush_maker = match (out_path, flush_every > 0) {
            (Some(path), true) => Some(FlushConfig {
                path,
                report: &mut report,
                section_key: "orderbook_order_filled_events_maker",
                data_field: "orderFilledEvents",
                kind: SpikePaginationKind::Skip,
                role: Some("maker"),
            }),
            _ => None,
        };
        let pb = new_section_spinner(args.no_progress);
        let (rows, meta) = paginate_order_fills(
            &client,
            ORDERBOOK_URL,
            "maker",
            &w,
            PaginateOpts {
                label: "Orderbook · maker fills",
                progress: pb.as_ref(),
                flush_every,
                flush: flush_maker,
                page_size,
                max_pages,
            },
        )
        .await?;
        if let Some(pb) = pb {
            pb.finish_with_message(format!(
                "Orderbook · maker · {} rows ✓",
                rows.len()
            ));
        }
        let v = json!({
            "data": { "orderFilledEvents": rows },
            "_spike_pagination": meta
        });
        report.insert("orderbook_order_filled_events_maker".to_string(), v.clone());
        if let Some(p) = out_path {
            write_report_atomic(p, &report)?;
        }
        print_section(
            &format!(
                "=== Orderbook fills as maker (full paginated, {} rows) ===",
                rows.len()
            ),
            &v,
            args.out.is_some(),
            args.tee,
        )?;

        let flush_taker = match (out_path, flush_every > 0) {
            (Some(path), true) => Some(FlushConfig {
                path,
                report: &mut report,
                section_key: "orderbook_order_filled_events_taker",
                data_field: "orderFilledEvents",
                kind: SpikePaginationKind::Skip,
                role: Some("taker"),
            }),
            _ => None,
        };
        let pb = new_section_spinner(args.no_progress);
        let (rows, meta) = paginate_order_fills(
            &client,
            ORDERBOOK_URL,
            "taker",
            &w,
            PaginateOpts {
                label: "Orderbook · taker fills",
                progress: pb.as_ref(),
                flush_every,
                flush: flush_taker,
                page_size,
                max_pages,
            },
        )
        .await?;
        if let Some(pb) = pb {
            pb.finish_with_message(format!(
                "Orderbook · taker · {} rows ✓",
                rows.len()
            ));
        }
        let v = json!({
            "data": { "orderFilledEvents": rows },
            "_spike_pagination": meta
        });
        report.insert("orderbook_order_filled_events_taker".to_string(), v.clone());
        if let Some(p) = out_path {
            write_report_atomic(p, &report)?;
        }
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
            if let Some(p) = out_path {
                write_report_atomic(p, &report)?;
            }
            print_section(
                "=== PnL userPositions (skipped) ===",
                &v,
                args.out.is_some(),
                args.tee,
            )?;
        } else {
            let flush_pnl = match (out_path, flush_every > 0) {
                (Some(path), true) => Some(FlushConfig {
                    path,
                    report: &mut report,
                    section_key: "pnl_user_positions",
                    data_field: "userPositions",
                    kind: SpikePaginationKind::CursorIdGt,
                    role: None,
                }),
                _ => None,
            };
            let pb = new_section_spinner(args.no_progress);
            match paginate_user_positions(
                &client,
                &w,
                PaginateOpts {
                    label: "PnL · userPositions",
                    progress: pb.as_ref(),
                    flush_every,
                    flush: flush_pnl,
                    page_size: positions_ps,
                    max_pages,
                },
            )
            .await
            {
                Ok((rows, meta)) => {
                    if let Some(pb) = pb {
                        pb.finish_with_message(format!(
                            "PnL · userPositions · {} rows ✓",
                            rows.len()
                        ));
                    }
                    let v = json!({
                        "data": { "userPositions": rows },
                        "_spike_pagination": meta
                    });
                    report.insert("pnl_user_positions".to_string(), v.clone());
                    if let Some(p) = out_path {
                        write_report_atomic(p, &report)?;
                    }
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
                Err(e) => {
                    if let Some(pb) = pb {
                        pb.finish_and_clear();
                    }
                    let err_obj = json!({
                        "_spike_error": format!("{e:#}"),
                        "_spike_hints": [
                            "PnL 子图对超大账户仍可能首屏超时：再减小 --positions-page-size（如 50）",
                            "若不需要仓位列表：加 --skip-pnl-positions",
                            "Activity/Orderbook 数据已保留在本 JSON 中（若使用了 --out）"
                        ]
                    });
                    report.insert("pnl_user_positions".to_string(), err_obj.clone());
                    report.insert(
                        "_spike_partial_failure".to_string(),
                        json!({
                            "section": "pnl_user_positions",
                            "message": format!("{e:#}")
                        }),
                    );
                    eprintln!(
                        "=== WARNING: PnL userPositions failed; writing partial report if --out set ===\n{e:#}"
                    );
                    if let Some(p) = out_path {
                        write_report_atomic(p, &report)?;
                    }
                    print_section(
                        "=== PnL userPositions (FAILED — see _spike_error) ===",
                        &err_obj,
                        args.out.is_some(),
                        args.tee,
                    )?;
                    pnl_section_failed = Some(e);
                }
            }
        }
    }

    finish_report(&args, &report)?;
    if let Some(e) = pnl_section_failed {
        anyhow::bail!(
            "pnl userPositions failed (partial JSON was still written if --out was set): {e:#}"
        );
    }
    Ok(())
}

async fn paginate_redemptions<'a>(
    client: &Client,
    addr: &str,
    opts: PaginateOpts<'a>,
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
        &["data", "redemptions"],
        opts,
    )
    .await
}

async fn paginate_order_fills<'a>(
    client: &Client,
    url: &str,
    role: &str,
    addr: &str,
    opts: PaginateOpts<'a>,
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

    let (rows, mut meta) = paginate_graphql(client, url, query, addr, &[
        "data",
        "orderFilledEvents",
    ], opts)
    .await?;
    if let Value::Object(ref mut m) = meta {
        m.insert("role".to_string(), json!(where_field));
    }
    Ok((rows, meta))
}

/// Paginate `userPositions` by **cursor** (`id_gt` + `orderBy: id`), avoiding deep `skip` which
/// triggers statement timeouts on hosted subgraphs for active wallets.
async fn paginate_user_positions<'a>(
    client: &Client,
    addr: &str,
    mut opts: PaginateOpts<'a>,
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

        let v = post_graphql_pnl(client, PNL_URL, &body).await?;
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
        opts.maybe_flush(&all, pages)?;

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
async fn paginate_graphql<'a>(
    client: &Client,
    url: &str,
    query: &str,
    addr: &str,
    path: &[&str],
    mut opts: PaginateOpts<'a>,
) -> anyhow::Result<(Vec<Value>, Value)> {
    let page_size = opts.page_size;
    let max_pages = opts.max_pages;
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
        opts.tick(pages, all.len());
        opts.maybe_flush(&all, pages)?;
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
        write_report_atomic(path, report)?;
        let nbytes = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        println!(
            "\nWrote merged report ({} bytes) to {}",
            nbytes,
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
    post_graphql_retry(client, url, body, 5, 500).await
}

/// PnL `userPositions` queries are heavier; more attempts and longer backoff.
async fn post_graphql_pnl(client: &Client, url: &str, body: &serde_json::Value) -> anyhow::Result<Value> {
    post_graphql_retry(client, url, body, 8, 1500).await
}

async fn post_graphql_retry(
    client: &Client,
    url: &str,
    body: &serde_json::Value,
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
