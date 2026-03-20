# Polymarket Account Analyzer

**English:** Read-only Rust tool to analyze any Polymarket wallet: fetch public Data API (and optionally Goldsky subgraphs + Gamma metadata), produce a structured JSON report with strategy inference, optional Postgres caching/reconciliation/canonical persistence, **CLI + REST API**. No trading or signing.

**中文：** 只读分析工具：对任意钱包拉取公开数据，多维度交易分析 + 策略反推 JSON/伪代码；可选 PostgreSQL 缓存、子图对账、规范层落库与 **PG 重放报告**；提供 **命令行** 与 **Axum API**。不涉及下单、签名或私钥。

|  |  |
|--|--|
| **语言** | Rust (stable) |
| **许可证** | Apache-2.0 |
| **本 crate 路径** |  monorepo 内 `polymarket-account-analyzer/`（仓库根目录为上一级 `account-analyzer/`） |

---

## 目录

- [能做什么](#能做什么)
- [架构与工作流](#架构与工作流)
- [仓库结构](#仓库结构)
- [快速开始](#快速开始)（含 **[配置文件怎么用](#配置文件怎么用必读)**）
- [数据量与分页（拉多少条）](#数据量与分页拉多少条)
- [功能开关与覆盖（CLI / HTTP）](#功能开关与覆盖cli--http)
- [CLI 命令与示例](#cli-命令与示例)
- [HTTP API](#http-api)
- [配置详解（TOML）](#配置详解toml)
- [报告缓存与指纹](#报告缓存与指纹)
- [日志与排错](#日志与排错)
- [数据源与限制](#数据源与限制)
- [报告 JSON（schema 2.x）](#报告-jsonschema-2x)
- [Postgres 表（节选）](#postgres-表节选)
- [开发与测试](#开发与测试)
- [路线图 / 已知待办](#路线图--已知待办)
- [相关文档](#相关文档)

---

## 能做什么

| 类别 | 能力 |
|------|------|
| **分析维度** | Lifetime（笔数、volume、净 PnL 现金流口径、极值）；市场分布 Top10（可配置 slug→类型）；价格分桶；UTC 活跃小时；交易模式（网格倾向、方向偏好、胜率）；策略反推（`primary_style`、`rule_json`、`pseudocode`） |
| **数据源** | Polymarket **Data API** `/trades`（主路径）；可选 **Gamma** `markets/slug`；可选 **Goldsky 子图**（redemptions、order fills maker/taker、PnL userPositions） |
| **对账** | **v0** 时间窗粗匹配；**v1** canonical 合并（tx 优先 + 模糊匹配 + ambiguous 队列）；`quality_alert` 提示；子图失败时 **`skipped_subgraph_fetch_error`** 显式摘要 |
| **存储** | 可选 PG：**报告缓存**（`report_cache_kv` + 配置指纹）、行级 **raw**、**markets_dim**、**canonical_events** 等（见下表） |
| **输出** | CLI stdout / `--out`；`serve` 后 `GET /analyze/:wallet` |

**非目标：** 不保证「链上绝对全历史」（受 Data API offset 上限与公共子图限制）；不提供前端页面；不在本 crate 内实现 Turbo/自建索引（见 [raw 表契约](../artifacts/raw-ingestion-contract.md)）。

---

## 架构与工作流

```
┌─────────────────────────────────────────────────────────────┐
│  产品层：CLI / GET /analyze/:wallet  →  AnalyzeReport JSON   │
└─────────────────────────────────────────────────────────────┘
                              ↑
        build_report ← Trade 流（Data API | 内存合成 | PG 重放）
                              ↑
┌─────────────────────────────────────────────────────────────┐
│  可选：canonical 合并 → canonical_events、reconciliation_*    │
└─────────────────────────────────────────────────────────────┘
                              ↑
┌─────────────────────────────────────────────────────────────┐
│  采集：Data API ∥ Subgraph ∥ Gamma                          │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  可选 PG：ingestion_run、raw_*、markets_dim、report_cache_kv  │
└─────────────────────────────────────────────────────────────┘
```

**三条「指标计算」路径：**

1. **默认**：`analyze` / API 使用 **Data API 内存 trades** 驱动 `build_report`。  
2. **`[analytics].source = "canonical"`**：使用**当次 run 内存合并**生成的合成 `Trade`（若无合成事件则回退 Data API）。  
3. **`report-from-canonical-run`**：只读 PG **`canonical_events`**（+ 可选 `reconciliation_report`），**无需外网**，`data_lineage.analytics_primary_source = canonical_pg_replay`。

**端到端（简版）：** 配置 →（可选）`ingestion_run` → 拉 trades / 子图 / Gamma →（可选）raw 与 canonical 落库 → 生成报告（`provenance`、`truncation`、对账字段）→（可选）`export-ambiguous` / `set-ambiguous-review` →（可选）PG 重放。

---

## 仓库结构

Monorepo 根目录（`account-analyzer/`）中与本工具相关的主要路径：

```
account-analyzer/
├── polymarket-account-analyzer/    # ← 本 README 所在 crate
│   ├── src/
│   │   ├── main.rs                 # CLI、analyze 流水线、serve、PG 重放
│   │   ├── config.rs               # TOML、report_cache_key（配置指纹）
│   │   ├── report.rs               # AnalyzeReport schema 2.x
│   │   ├── storage.rs              # Postgres init_schema + 读写
│   │   ├── reconciliation.rs       # v0 / v1 摘要、quality_alert
│   │   ├── canonical.rs            # v1 合并、shadow、synthetic trades
│   │   └── polymarket/             # data_api、gamma_api、subgraph
│   ├── tests/regression_merge.rs   # 无网络集成测试
│   └── config.example.toml
├── artifacts/                      # PROJECT-OVERVIEW、规划、手工清单、progress-log
├── scripts/regression-check.sh
└── docs/、memory/、specs/、task_plan.md …
```

---

## 快速开始

```bash
# 在 monorepo 中进入 crate 目录
cd polymarket-account-analyzer

# 编译
cargo build --release

# 分析钱包 → stdout
cargo run --release -- analyze 0x5924ca480d8b08cd5f3e5811fa378c4082475af6

# 写入 JSON
cargo run --release -- analyze 0x5924ca480d8b08cd5f3e5811fa378c4082475af6 --out report.json

# 跳过 Postgres 报告缓存
cargo run --release -- analyze 0x5924ca480d8b08cd5f3e5811fa378c4082475af6 --no-cache

# 子图 + 对账 + 规范层落库（需 DATABASE_URL，且建议 config.toml 中 persist_raw）
cargo run --release -- analyze 0x5924ca480d8b08cd5f3e5811fa378c4082475af6 \
  --with-subgraph --with-reconciliation --with-canonical --out report.json

# 启动 API（默认 127.0.0.1:3000）
cargo run --release -- serve --bind 127.0.0.1:3000
```

### 配置文件怎么用（必读）

程序 **不会** 自动读取当前目录下的 `config.toml`。只有你在命令行里写了 **`--config /某路径/某文件.toml`** 时，才会去读那个文件；否则一律用 **编译在程序里的默认配置**（例如 `trades_page_limit = 500`、无数据库连接串等）。

| 你怎么运行 | 实际生效的配置 |
|------------|----------------|
| **不写 `--config`** | 不读任何 TOML；全部用代码默认值。此时若要连 Postgres，只能依赖环境变量 **`DATABASE_URL`**（见下）。 |
| **`--config config.toml`** | 读取该文件，在默认值上 **合并覆盖**（文件里写了的项才改）。 |

**数据库连接串怎么给：**

1. **写在 TOML 里**：`database_url = "postgres://..."`（在你用 `--config` 加载的那份文件里）。  
2. **环境变量**：`export DATABASE_URL="postgres://..."`  
3. **两者都有时**：以 **TOML 里的 `database_url` 为准**（非空则不用环境变量）。

**常见用法示例：**

```bash
cd polymarket-account-analyzer

# --- 1）零配置文件：只分析钱包，不要 Postgres（报告打印到终端）---
cargo run --release -- analyze 0x你的钱包地址

# --- 2）零配置文件，但要缓存/落库：只用环境变量指数据库 ---
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/polymarket_analyzer"
cargo run --release -- analyze 0x你的钱包地址 --out report.json

# --- 3）用配置文件：先复制模板，再每次显式带上 --config ---
cp config.example.toml config.toml
# 编辑 config.toml（改 database_url、trades_page_limit、[subgraph] 等）
cargo run --release -- --config config.toml analyze 0x你的钱包地址 --out report.json

# --- 4）API 模式同样要指定配置文件（若你需要 TOML 里的设置）---
cargo run --release -- --config config.toml serve --bind 127.0.0.1:3000
```

**注意：** `cargo run` 后面第一个 **`--`** 用来把参数交给本程序；因此带配置文件时推荐写成：  
`cargo run --release -- --config config.toml analyze ...`  
（若你直接运行二进制 `./target/release/polymarket-account-analyzer`，则写成：  
`./target/release/polymarket-account-analyzer --config config.toml analyze ...`。）

---

## 数据量与分页（拉多少条）

### Data API（`GET /trades`，主分析流）

| 配置项 | 含义 | 默认 | 备注 |
|--------|------|------|------|
| **`trades_page_limit`** | 每页 `limit`，同时也是 **offset 步长**（翻页增量） | **500** | 单次请求会被限制在 **1～10000** |
| **`rate_limit_ms`** | 每页请求之间的间隔（毫秒） | **150** | 减轻限流；非「条数」但影响总耗时 |

- **没有** CLI/HTTP 参数覆盖 `trades_page_limit`，只能通过 **`config.toml`**（或改代码默认值）。
- 能拉到的 **总成交条数** 还受 Polymarket **历史 offset 上限**约束；到顶会停翻页，报告里见 **`data_fetch.truncated`** / **`ingestion.truncation.data_api`**（与「每页多大」无关）。

示例（更少请求、仍受服务端 offset 上限约束）：

```toml
trades_page_limit = 3000
rate_limit_ms = 200
```

### Subgraph（`[subgraph]`，需 `enabled = true` 或 `--with-subgraph`）

| 配置项 | 含义 | 默认 | 备注 |
|--------|------|------|------|
| **`page_size`** | GraphQL 每页条数 | **1000** | 各活动流分页 |
| **`max_pages`** | 每流最多翻页数 | **500** | 粗上界约 `page_size × max_pages`（以实际查询为准） |
| **`positions_page_size`** | PnL `userPositions` 等持仓相关分页 | **100** | PnL 侧用 **`id_gt` 游标**，避免深 `skip` |
| **`cap_rows_per_stream`** | 每条子图流 **硬封顶**行数；**0 = 不限制** | **0** | 冒烟/限成本时设为例如 `1000` |
| **`skip_pnl_positions`** | 跳过 PnL 仓位拉取 | `false` | 极大钱包可关以缩短时间 |
| **`timeout_sec` / `max_retries` / `pnl_max_retries`** | 子图超时与重试 | 600 / 5 / 8 | 弱网可调大 |

**CLI / HTTP 覆盖：** `--subgraph-cap-rows <N>` 或 `?subgraph_cap_rows=N` 会设置当次运行的 **`cap_rows_per_stream`**（`N` 为 `0` 时行为以代码为准；一般用于设正数封顶）。

---

## 功能开关与覆盖（CLI / HTTP）

以下标志在 **`analyze` 子命令**与 **`GET /analyze/:wallet`** 中语义一致：在 TOML 基础上 **仅当次** 打开/关闭（`with_*` 与 `no_*` 成对出现，后者可关掉 TOML 里已开启的项）。

| 标志 | 作用 |
|------|------|
| `--with-subgraph` / `no_subgraph` | 是否拉 Goldsky 子图 |
| `--with-reconciliation` / `no_reconciliation` | 是否做对账（v0/v1 取决于是否 canonical） |
| `--with-canonical` / `no_canonical` | 是否做规范合并并写 PG（需 **`DATABASE_URL` + 建议 `persist_raw`**） |
| `--no-persist-raw` | 当次不写 raw 表（仍可有报告缓存，视库是否可用） |
| `--subgraph-cap-rows` | 见上文「子图硬封顶」 |
| `--no-cache` | 跳过 Postgres **报告缓存**读/写，强制重算 |

**仅 CLI：** `export-ambiguous`、`set-ambiguous-review`、`report-from-canonical-run` 不使用上述 analyze 标志（后两者只依赖数据库）。

---

## CLI 命令与示例

| 命令 | 说明 |
|------|------|
| **`analyze <wallet>`** | 主流程：拉数 → 报告 JSON |
| **`serve`** | `GET /analyze/:wallet` |
| **`export-ambiguous <run_id>`** | 导出 `reconciliation_ambiguous_queue`（需 PG） |
| **`set-ambiguous-review <run_id> <trade_key> <status>`** | 更新复核状态 |
| **`report-from-canonical-run <run_id>`** | 从 **`canonical_events`** 重放报告（**无外网**） |

全局选项：**`--config path.toml`**。未指定则使用内置默认配置。

```bash
# 分析 + 写文件 + 跳过缓存
cargo run --release -- analyze 0x... --out report.json --no-cache

# 子图 + 对账 + 规范层（与 TOML 配合）
cargo run --release -- analyze 0x... \
  --with-subgraph --with-reconciliation --with-canonical --out report.json

# 仅当次限制子图每流最多 2000 行
cargo run --release -- analyze 0x... --with-subgraph --subgraph-cap-rows 2000

# 启动 API
cargo run --release -- serve --bind 127.0.0.1:3000

# 导出 ambiguous（run_id = ingestion_run.id UUID）
cargo run --release -- export-ambiguous "550e8400-e29b-41d4-a716-446655440000"

# 标记已复核
cargo run --release -- set-ambiguous-review "550e8400-..." "trade_key_here" reviewed

# 从 PG 重放（需先前带 --with-canonical 跑过 analyze）
cargo run --release -- report-from-canonical-run "550e8400-..." --out replay.json
cargo run --release -- report-from-canonical-run "550e8400-..." --out replay.json --write-cache
```

---

## HTTP API

**路由：** `GET /analyze/:wallet`（`wallet` 为路径参数，**不要**对 `0x` 地址再做一层 URL 编码破坏路径，一般直接写即可）。

**Query 参数**（与 `Analyze` CLI 覆盖一致，均为可选；布尔值常用 `true` / `false`）：

| 参数 | 说明 |
|------|------|
| `no_cache` | 跳过报告缓存 |
| `with_subgraph` / `no_subgraph` | 子图开关 |
| `subgraph_cap_rows` | 子图每流行数上限（整数，同 CLI） |
| `with_reconciliation` / `no_reconciliation` | 对账开关 |
| `no_persist_raw` | 当次不写 raw |
| `with_canonical` / `no_canonical` | 规范层落库开关 |

```bash
curl -s "http://127.0.0.1:3000/analyze/0x5924ca480d8b08cd5f3e5811fa378c4082475af6" | jq .
curl -s "http://127.0.0.1:3000/analyze/0x...?no_cache=true" | jq .
curl -s "http://127.0.0.1:3000/analyze/0x...?with_subgraph=true&with_reconciliation=true" | jq .
curl -s "http://127.0.0.1:3000/analyze/0x...?with_subgraph=true&subgraph_cap_rows=500" | jq .
```

> **说明：** 当前 **没有** 与 `report-from-canonical-run`、`export-ambiguous`、`set-ambiguous-review` 等价的 HTTP 接口；这些仅 CLI。

---

## 配置详解（TOML）

复制 **`config.example.toml` → `config.toml`** 后按需编辑；运行时务必 **`--config config.toml`**，否则该文件**不会被读取**（见上文 **[配置文件怎么用（必读）](#配置文件怎么用必读)**）。

连接数据库：**TOML 里 `database_url` 非空则优先**；否则用环境变量 **`DATABASE_URL`**（`init_storage`）。

### 顶层常用项

| 项 | 作用 | 默认 |
|----|------|------|
| `database_url` | PostgreSQL 连接串 | 无（仅 `DATABASE_URL` 或 TOML） |
| `cache_ttl_sec` | 报告缓存 TTL（秒） | 600 |
| `timeout_sec` | Data API / Gamma HTTP 超时（秒） | 90 |
| `trades_page_limit` | Data API 分页 | 500 |
| `rate_limit_ms` | Data API 请求间隔 | 150 |

### 各配置块

| 块 | 作用 |
|----|------|
| **`[subgraph]`** | 子图 URL、`enabled`、**分页与封顶**、超时/重试、`extended_fill_fields`、`show_progress`、`user_agent` 等 |
| **`[reconciliation]`** | `enabled`、v0 时间窗、v1 **`size_tolerance_pct` / `price_tolerance_abs` / `require_condition_match` / `rules_version`**、质量阈值 **`shadow_volume_alert_ratio`、`api_only_ratio_alert`、`api_only_alert_min`** |
| **`[analytics]`** | **`source`**: `data_api`（默认）或 **`canonical`**（合成成交驱动主指标，空则回退 Data API）；**`canonical_shadow`**：算 shadow 与 `metrics_canonical_shadow` |
| **`[ingestion]`** | **`persist_raw`**：是否写 `ingestion_run` / `raw_*` |
| **`[canonical]`** | **`enabled`**：规范合并 + `canonical_events` 等落库；**`enrich_markets_dim`**：Gamma 补市场维表 |
| **`[market_type]`** | 按 slug 归类市场类型：**规则有序，首条命中生效**；支持 **`contains`** / **`prefix`**（见 `config.rs` / 示例文件） |

---

## 报告缓存与指纹

启用 Postgres 时，同一钱包的报告可能来自 **`report_cache_kv`**。缓存键由 **钱包（小写）+ 配置指纹哈希** 组成，指纹包含例如：

`trades_page_limit`、`subgraph`（enabled、cap、page_size、max_pages、positions_page_size、skip_pnl、extended_fill_fields）、`canonical`、`reconciliation` 各字段、`ingestion.persist_raw`、`analytics` 等。

**修改上述任一字段** 后若仍命中旧缓存，请使用 **`--no-cache`** 或 **`?no_cache=true`**。  
**`--subgraph-cap-rows` / `subgraph_cap_rows`** 会改变 `cap_rows_per_stream`，同样影响指纹。

---

## 日志与排错

使用 **`RUST_LOG`**（`tracing_subscriber` 默认环境过滤器），例如：

```bash
RUST_LOG=info cargo run --release -- analyze 0x...
RUST_LOG=polymarket_account_analyzer=debug cargo run --release -- serve --bind 127.0.0.1:3000
```

- 未配置 **`DATABASE_URL`**（且无可用 `database_url`）：日志中会提示 **without postgres cache**，分析仍可进行，但无缓存与 raw/canonical。  
- 子图失败时报告中会有 **`skipped_subgraph_fetch_error`** 等摘要，JSON 内也可能含 **`_fetch_error`**。

---

## 数据源与限制

- **Data API**（公开）：[`data-api.polymarket.com`](https://data-api.polymarket.com) — `GET /trades?user=&limit=&offset=`  
- **Gamma**：市场元信息（如 `endDate` / `closedTime`）  
- **子图**：[Polymarket Subgraph 文档](https://docs.polymarket.com/market-data/subgraph)；PnL `userPositions` 使用 **`id_gt` 游标分页**，避免深 `skip` 超时。

**历史深度：** 服务端可能对 offset 设硬上限（如 `max historical activity offset of 3000 exceeded`）。本工具会停止翻页并在报告中标注 **`data_fetch.truncated`** / **`ingestion.truncation.data_api`**。更全链上历史需其他管道（子图全量、自建索引等），见上级目录 [**`artifacts/report-and-reconciliation-plan.md`**](../artifacts/report-and-reconciliation-plan.md)。

---

## 报告 JSON（schema 2.x）

`schema_version: "2.0.0"`，主要字段包括：

`wallet`、`trades_count`、`total_volume`、`lifetime`、`market_distribution`、`price_buckets`、`time_analysis`、`trading_patterns`、`strategy_inference`、`notes`、`data_fetch`、`ingestion`（含 **`truncation`**）、`subgraph`、`reconciliation`、`reconciliation_v1`、`canonical_summary`、`data_lineage`、**`provenance`**、**`metrics_canonical_shadow`**。

---

## Postgres 表（节选）

启动时会 **`init_schema`**（内联 SQL，无独立 migrations 目录）：

| 类别 | 表 |
|------|-----|
| 报告缓存 | **`report_cache_kv`**（主）；`analyze_report_cache`（仅 legacy 读） |
| 任务与 Raw | `ingestion_run`、`raw_ingestion_chunk`、`raw_data_api_trades`、`raw_subgraph_*` |
| 维表 | `markets_dim` |
| 规范与对账 | `canonical_events`、`source_event_map`、`reconciliation_report`、`reconciliation_ambiguous_queue` |

---

## 开发与测试

```bash
cd polymarket-account-analyzer
cargo test              # 含 tests/regression_merge.rs（7 项，无网络）
cargo build --release
```

Monorepo 根目录一键脚本（若存在）：

```bash
./scripts/regression-check.sh
```

手工验收步骤：[ **`artifacts/manual-test-checklist.md`**](../artifacts/manual-test-checklist.md)。

---

## 路线图 / 已知待办

**已实现（产品闭环）：** 截断元数据、配置指纹缓存、analytics canonical / shadow、v0/v1 对账、ambiguous 导出与复核状态、`quality_alert`、子图失败摘要、**PG 重放 CLI**、raw 表对外契约文档。

**部分完成 / 愿景差距：**

- 时间维度中「相对 resolution / 持仓时长」强依赖 Gamma 补全；元信息不足时为占位或缺失比例。  
- 赎回、仓位快照进入 canonical 与统计，与主 KPI trade 流 **并列**，未合并为单一仪表盘树。  
- 未实现规划 §4.2 那种深层嵌套的 `report.vNext` 树状结构；当前为扁平 `AnalyzeReport` + 扩展字段。

**明确未做 / 欢迎 PR 的方向：**

| 方向 | 说明 |
|------|------|
| Raw | 行级显式 `fetched_at` 等列 |
| Canonical | DB CHECK、Rust enum、`event_type`、独立匹配模块、规则插件化 |
| 对账 | 工单/Web UI、更复杂大额差异规则 |
| Gamma | `condition_id ↔ slug` 维表增强、fill 缺字段的结构化低置信 |
| API | HTTP 暴露 `report-from-canonical-run`；PG 重放联查 `markets_dim` |
| 基础设施 | 链上绝对全量（Turbo/自建索引）**不在本 crate 范围** |

更细的差距矩阵见 [**`artifacts/report-and-reconciliation-plan.md`**](../artifacts/report-and-reconciliation-plan.md) §1.5.4。

---

## 相关文档

| 文档 | 内容 |
|------|------|
| [**`artifacts/PROJECT-OVERVIEW.md`**](../artifacts/PROJECT-OVERVIEW.md) | 与本文互补的**总览**（完成度表、索引） |
| [**`artifacts/report-and-reconciliation-plan.md`**](../artifacts/report-and-reconciliation-plan.md) | 全量数据、对账、Postgres、报告规划；**§1.5** 为工作流权威摘要 |
| [**`artifacts/raw-ingestion-contract.md`**](../artifacts/raw-ingestion-contract.md) | 外部管道与 raw 表契约（P3） |
| [**`artifacts/manual-test-checklist.md`**](../artifacts/manual-test-checklist.md) | 手工测试清单 |
| [**`artifacts/progress-log.md`**](../artifacts/progress-log.md) | 按日迭代与验证记录 |
| [`task_plan.md`](../task_plan.md)、[`findings.md`](../findings.md)、[`progress.md`](../progress.md) | 任务、决策、进度摘要 |

---

## 许可证

Apache-2.0
