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
- [快速开始](#快速开始)
- [CLI 命令一览](#cli-命令一览)
- [HTTP API](#http-api)
- [配置与环境变量](#配置与环境变量)
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

**配置：** `cp config.example.toml config.toml`，设置 `database_url` 或使用环境变量 **`DATABASE_URL`**。

---

## CLI 命令一览

| 命令 | 说明 |
|------|------|
| `analyze <wallet>` | 主流程；`--out`、`--no-cache`、`--with-subgraph`、`--with-reconciliation`、`--with-canonical`、`--no-persist-raw`、`--subgraph-cap-rows` 等 |
| `serve` | REST：`GET /analyze/:wallet`，query 与 CLI 覆盖语义一致 |
| `export-ambiguous <ingestion_run_uuid>` | 导出 ambiguous 队列 JSON（含 `review_status`） |
| `set-ambiguous-review <run_id> <trade_key> <status>` | `pending` \| `reviewed` \| `dismissed` \| `escalated` |
| `report-from-canonical-run <run_id>` | 从 PG `canonical_events` 重放报告；`--out`；可选 `--write-cache` |

全局选项：`--config <path.toml>`。

---

## HTTP API

```bash
curl -s "http://127.0.0.1:3000/analyze/0x5924ca480d8b08cd5f3e5811fa378c4082475af6" | jq .
curl -s "http://127.0.0.1:3000/analyze/0x...?no_cache=true" | jq .
curl -s "http://127.0.0.1:3000/analyze/0x...?with_subgraph=true&with_reconciliation=true" | jq .
```

> **说明：** 当前 **没有** 与 `report-from-canonical-run` 等价的 HTTP 接口；PG 重放仅 CLI。

---

## 配置与环境变量

| 配置块 / 项 | 作用 |
|-------------|------|
| `database_url` / **`DATABASE_URL`** | PostgreSQL；未配置则无缓存、无 raw/canonical 落库 |
| `cache_ttl_sec` | 报告缓存 TTL |
| `trades_page_limit` | Data API `limit` 与 offset 步长（最大 10000） |
| **`[subgraph]`** | Goldsky URL、分页、`cap_rows_per_stream`、`skip_pnl_positions`、超时与重试等 |
| **`[reconciliation]`** | 是否启用、时间窗、v1 容差、`rules_version`、**质量阈值**（`shadow_volume_alert_ratio`、`api_only_ratio_alert`、`api_only_alert_min`） |
| **`[analytics]`** | `source`: `data_api` \| `canonical`；`canonical_shadow` |
| **`[ingestion]`** | `persist_raw` |
| **`[canonical]`** | 规范层落库开关、`enrich_markets_dim` |
| **`[market_type]`** | slug → 市场类型规则 |

CLI/API 布尔与 cap 类参数可 **覆盖** TOML（详见 `config.example.toml`）。修改子图 cap 或开关后若与缓存不一致，请使用 **`--no-cache`**。

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
