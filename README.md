# Polymarket Account Analyzer

**English:** Read-only Rust tool to analyze any Polymarket wallet: fetch public Data API (and optionally Goldsky subgraphs + Gamma metadata), produce a structured JSON report with strategy inference, optional Postgres caching/reconciliation/canonical persistence, **CLI + REST API**. No trading or signing.

**中文：** 只读分析工具：对任意钱包拉取公开数据，多维度交易分析 + 策略反推 JSON/伪代码；可选 PostgreSQL 缓存、子图对账、规范层落库与 **PG 重放报告**；提供 **命令行** 与 **Axum API**。不涉及下单、签名或私钥。

|  |  |
|--|--|
| **语言** | Rust (stable) |
| **许可证** | Apache-2.0 |
| **GitHub 克隆目录** | `git clone … Polymarket-Analyzer.git` 后进入 **`Polymarket-Analyzer/`**（即本仓库根目录，与 `Cargo.toml` 同级） |
| **二进制文件名** | `cargo build --release` 生成 `target/release/polymarket-account-analyzer`（与 Rust 包名一致，与文件夹名无关） |

---

## 目录

- [能做什么](#能做什么)
- [架构与工作流](#架构与工作流)
- [仓库结构](#仓库结构)
- [快速开始](#快速开始)（含 **[配置文件怎么用](#配置文件怎么用必读)**）
- [配置：TOML、PAA 环境变量与 Docker](#paaconfig)
- [Docker Compose 部署](#docker-compose-部署)
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

**非目标：** 不保证「链上绝对全历史」（受 Data API offset 上限与公共子图限制）；不提供前端页面；不在本 crate 内实现 Turbo/自建索引（扩展规划见下方 [相关文档](#相关文档) 中 `artifacts/` 链接，**仅 monorepo 含该目录时可用**）。

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

**标准 GitHub 克隆**（仓库 **[Polymarket-Analyzer](https://github.com/a9research/Polymarket-Analyzer)**）后，项目根目录名为 **`Polymarket-Analyzer/`**，主要文件：

```
Polymarket-Analyzer/
├── Cargo.toml
├── Cargo.lock
├── README.md
├── Dockerfile
├── docker-compose.yml
├── config.example.toml
├── src/
│   ├── main.rs
│   ├── config.rs
│   ├── report.rs
│   ├── storage.rs
│   ├── reconciliation.rs
│   ├── canonical.rs
│   └── polymarket/                 # data_api、gamma_api、subgraph
└── tests/
    └── regression_merge.rs
```

若你在**更大 monorepo** 里把本 crate 放在子目录（例如 `…/polymarket-account-analyzer/`），则所有命令里的 `cd` 改为进入**该子目录**即可；文档中的 `../artifacts/` 等链接仅在上级仓库实际包含这些目录时有效。

---

## 快速开始

```bash
git clone https://github.com/a9research/Polymarket-Analyzer.git
cd Polymarket-Analyzer

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
cd Polymarket-Analyzer

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

<a id="paaconfig"></a>

## 配置：TOML、PAA 环境变量与 Docker

环境变量名中带 **`PAA_`** 前缀（见下表），避免与系统变量冲突。

### 能在 `docker-compose.yml` 里写参数吗？

**可以。** 在 `analyzer` 服务的 **`environment:`** 里配置即可，无需挂载 `config.toml`：

| 方式 | 说明 |
|------|------|
| **标准变量** | **`DATABASE_URL`**、**`RUST_LOG`** 见下表；不经过 `PAA_` 前缀。 |
| **`PAA_*` 前缀** | 覆盖应用业务参数。**生效顺序**：代码默认 → `--config` TOML（若有）→ **`PAA_*`（若设置且可解析）**。之后 **`analyze` 的 CLI / HTTP query** 仍可在**当次**覆盖部分开关（例如 `with_subgraph`）。 |

#### 标准环境变量（非 PAA\_）

| 变量 | 是什么意思、用来干嘛 |
|------|----------------------|
| **`DATABASE_URL`** | **PostgreSQL 连接串**。有它且连得上时：会启用**报告缓存**（`report_cache_kv`）、可选 **raw / canonical** 等落库；没有则程序照样能分析，只是**纯内存、无缓存**。Docker Compose 里指向服务名 **`postgres`** 和 compose 里建的库。 |
| **`RUST_LOG`** | **日志级别**（`tracing` / `tracing-subscriber`）。例如 `info`、`debug`。用来**排错**、看请求与上游错误；不改变业务结果。常用：`RUST_LOG=info` 或 `RUST_LOG=polymarket_account_analyzer=debug`。 |

#### `PAA_*` 参数说明（完整实现在 `config::apply_env_overrides`）

| 变量 | 是什么意思、用来干嘛 |
|------|----------------------|
| **`PAA_TRADES_PAGE_LIMIT`** | 向 Polymarket **Data API** 拉成交时，每一页请求多少条，同时也是 **翻页的 offset 步长**。**调大**可减少 HTTP 次数；**过大**单次响应更重。合法范围程序内限制为 **1～10000**（与官方上限一致）。 |
| **`PAA_RATE_LIMIT_MS`** | 每拉**一页** Data API 后**休眠多少毫秒**再拉下一页，用来**降低被限流**的概率。只影响速度与节奏，不改变「总共能拉多少」的上限（那由服务端 offset 等规则决定）。 |
| **`PAA_CACHE_TTL_SEC`** | **同钱包、同配置指纹**的报告在 Postgres 里缓存**多少秒内视为仍有效**。过期后会重新算报告。用来**省重复计算**和上游流量。 |
| **`PAA_TIMEOUT_SEC`** | 访问 **Data API** 和 **Gamma**（市场元信息）时，单次 HTTP 请求**最多等多少秒**。网络慢或接口卡顿时**调大**可减少误超时；**调小**可更快失败重试（取决于你的运维策略）。 |
| **`PAA_SUBGRAPH_ENABLED`** | 是否在**默认**情况下就拉 **Goldsky 子图**（赎回、成交 fill、PnL 等）。`true`/`1`/`yes`/`on` 为开，`false`/`0` 等为关。仍可在单次请求用 URL 的 `with_subgraph` 等**再改**。 |
| **`PAA_SUBGRAPH_PAGE_SIZE`** | 子图 GraphQL 查询里**每页返回多少条**。影响单次查询体量和翻页次数。 |
| **`PAA_SUBGRAPH_MAX_PAGES`** | 每个子图数据流**最多翻多少页**，用来**防止极端大户无限拉取**、控制时间与费用。 |
| **`PAA_SUBGRAPH_POSITIONS_PAGE_SIZE`** | **PnL 子图里 `userPositions`（持仓）** 分页大小；与活动/成交类流的分页是两套逻辑。 |
| **`PAA_SUBGRAPH_CAP_ROWS_PER_STREAM`** | 每条子图流**最多保留多少行**；达到后停止继续拉该流。**`0` 表示不限制**。适合做**冒烟测试**或给大户**封顶成本**；也可被单次请求的 `subgraph_cap_rows` 覆盖。 |
| **`PAA_SKIP_PNL_POSITIONS`** | 为 `true` 时**不拉 PnL 里的仓位列表**，可**明显缩短**大户运行时间，但报告里与仓位相关的子图信息会少。 |
| **`PAA_EXTENDED_FILL_FIELDS`** | 是否在子图 `orderFilledEvent` 上**请求额外字段**（如 tx、condition、size、price）。**利于对账与 canonical**；若子图端拒绝，程序会**自动回退**到精简字段。 |
| **`PAA_RECONCILIATION_ENABLED`** | 是否在默认流程里做 **Data API 与子图（及 canonical）之间的对账摘要**（v0/v1 逻辑由是否开 canonical 等决定）。 |
| **`PAA_CANONICAL_ENABLED`** | 是否做 **规范层合并**并把结果**写入 Postgres**（`canonical_events` 等）。依赖 **已配置数据库** 且一般与 **raw 落库**一起用；用于可追溯、PG 重放报告等。 |
| **`PAA_PERSIST_RAW`** | 是否在分析跑批时把 **原始 API/子图块** 写入 **`raw_*` 表**。关则只做报告（若开了缓存仍有 `report_cache`），**不做行级 raw 审计**。 |
| **`PAA_MAX_GAMMA_SLUGS_TIMING`** | **`[ingestion].max_gamma_slugs_for_timing`**：Gamma `markets/slug` 去重后最多请求多少 slug（用于 entry→resolution 计时）。**`0`** = 不调 Gamma、跳过该计时。 |
| **`PAA_DATA_API_POSITIONS_LIMIT`** | **`[ingestion].data_api_positions_limit`**：`/positions`、`/closed-positions` 每页 limit；**`0`** = 使用 **`trades_page_limit`**。 |
| **`PAA_PERSIST_POSITIONS_RAW`** | **`[ingestion].persist_positions_raw`**：`persist_raw` 时是否写入 **`raw_data_api_open_positions`** / **`raw_data_api_closed_positions`**。 |
| **`PAA_PERSIST_WALLET_SNAPSHOTS`** | **`[ingestion].persist_wallet_snapshots`**：是否在每次完整 **`analyze`** 后写入 **`wallet_*`** 表（主交易行、canonical 行、对账/前端/策略/整份报告快照）。 |
| **`PAA_DATA_API_INCREMENTAL_TRADES`** | **`[ingestion].data_api_incremental_trades`**：是否在 PG 已有 `wallet_primary_trade_row` 时对 `/trades` **按水位增量拉取并合并**（见 `artifacts/wallet-pipeline-snapshot.md`）。 |
| **`PAA_DATA_API_INCREMENTAL_MAX_PAGES`** | **`[ingestion].data_api_incremental_max_pages`**：增量模式最多翻页数（每页 = `trades_page_limit`）。 |
| **`PAA_ANALYTICS_SOURCE`** | **主报告指标**（lifetime、分布、策略反推等）基于哪条「成交流」：**`data_api`** = 直接用 Data API 成交；**`canonical`** = 优先用内存合并后的合成成交（没有则回退 Data API）。 |
| **`PAA_ANALYTICS_CANONICAL_SHADOW`** | 是否在算主指标的同时**也算一套 canonical 影子指标**（`metrics_canonical_shadow`），用于**对比 Data API 与合并视图**、在报告 notes 里提示差异。 |
| **`PAA_ENRICH_MARKETS_DIM`** | 是否用 **Gamma** 把市场里出现的 slug **补进 `markets_dim` 维表**（便于后续分析与展示）。需要外网访问 Gamma。 |
| **`PAA_CORS_ORIGINS`** | 同 TOML **`cors_allowed_origins`**：Vercel 等前端**跨域**访问 **`serve`** 时必填（逗号分隔多个 Origin）。 |

**尚未通过 `PAA_*` 暴露的**（仍需 TOML）：`[market_type]` 规则、`[reconciliation]` 里容差/阈值、`[subgraph]` 超时/重试秒数等 —— 需要时用 **`--config` + 挂载文件**。

### 哪些主要靠 TOML（或上面的 `PAA_*`）

| 类别 | 示例 |
|------|------|
| **Data API 节奏** | `trades_page_limit` → **`PAA_TRADES_PAGE_LIMIT`** 或 TOML |
| **子图分页等** | `page_size` / `max_pages` → **`PAA_SUBGRAPH_*`** 或 TOML（**`subgraph_cap_rows` 仍可**在 URL/CLI 当次覆盖） |
| **分析 / 对账** | `[analytics]` 部分字段有 **`PAA_`**；容差、阈值等仍靠 TOML |
| **落库 / 规范层** | **`PAA_PERSIST_RAW`**、**`PAA_CANONICAL_ENABLED`** 等或 TOML；**`[market_type]` 仅 TOML** |

无 `--config` 时：只靠 **默认 + `PAA_*` + `DATABASE_URL`** 即可跑 Docker。带 `--config` 时：TOML 先合并，再由 **`PAA_*`** 覆盖同名字段。

调用时加参数能改的，见 [功能开关与覆盖（CLI / HTTP）](#功能开关与覆盖cli--http)。

### 不用 Docker（本机）

1. 进入仓库根目录 **`Polymarket-Analyzer/`**。  
2. 复制模板并编辑：  
   `cp config.example.toml config.toml`  
3. 在 `config.toml` 里改需要的项（例如 `trades_page_limit`、`[subgraph]`）。  
4. **数据库**：在 `config.toml` 写 `database_url`，或**不写**该项、改用环境变量 **`DATABASE_URL`**（见上文优先级说明）。  
5. 每次运行**显式**带上配置文件，例如：

```bash
# CLI 分析
cargo run --release -- --config config.toml analyze 0x你的钱包地址 --out report.json

# 或已编译的二进制
./target/release/polymarket-account-analyzer --config config.toml serve --bind 127.0.0.1:3000
```

`config.toml` 已在 `.gitignore` 中，勿把含口令的文件提交到 Git。

### 在 Docker Compose 中挂载配置

1. 在 **`Polymarket-Analyzer/`** 下复制 Docker 用模板：  
   `cp config.docker.example.toml config.docker.toml`  
2. 编辑 **`config.docker.toml`**（按需改 `trades_page_limit`、`[subgraph]` 等）。  
   - **建议不写 `database_url`**：这样会继续使用 `docker-compose.yml` 里已为容器设好的 **`DATABASE_URL`**（`postgres` 服务名 + 库名）。  
   - 若你写了 `database_url`，主机名必须是 **`postgres`**，且账号库名与 compose 一致，例如：  
     `postgres://postgres:postgres@postgres:5432/polymarket_analyzer`  
3. 编辑 **`docker-compose.yml`** 里 `analyzer` 服务：把原来的 **`command: ["serve", "--bind", "0.0.0.0:3000"]` 整行删掉或改成注释**，改为下面两段（**同一个服务里只能有一个 `command:`**）：

```yaml
    volumes:
      - ./config.docker.toml:/config/config.toml:ro
    command: ["--config", "/config/config.toml", "serve", "--bind", "0.0.0.0:3000"]
```

（也可直接编辑仓库里已写好的注释块：删掉默认 `command` 行，再取消下面 `volumes` / `command` 的注释。）

4. 重新启动：

```bash
docker compose up --build -d
```

之后 **`trades_page_limit` 等 TOML 项**会对 **`GET /analyze/...`** 生效；仍可在 URL 上加 `with_subgraph` 等做当次覆盖。

**一次性 `docker compose run`（带配置）** 示例：

```bash
docker compose run --rm \
  -v "$(pwd)/config.docker.toml:/config/config.toml:ro" \
  -v "$(pwd)/out:/out" \
  analyzer --config /config/config.toml analyze 0x你的钱包地址 --out /out/report.json
```

`config.docker.toml` 已列入 `.gitignore`，请勿提交含敏感信息的副本。

---

## Docker Compose 部署

可以用 **`docker-compose.yml`** 同时拉起 **PostgreSQL** 与本程序的 **HTTP 服务**（默认 `serve`），容器内通过服务名 **`postgres`** 访问数据库；`DATABASE_URL` 已在 compose 里写好，**无需**再拷 `config.toml` 也能用缓存与 raw 落库（仍可用 query 打开子图/对账等）。

**前置：** 已安装 [Docker](https://docs.docker.com/get-docker/) 与 [Docker Compose V2](https://docs.docker.com/compose/)（`docker compose` 命令）。

```bash
cd Polymarket-Analyzer

# 构建并后台启动（Postgres + analyzer API）
docker compose up --build -d

# 健康检查通过后，本机访问 API（钱包地址按实际替换）
curl -s "http://127.0.0.1:3000/analyze/0x你的钱包地址" | jq .

# Gamma 公开资料（供前端 BFF；与 analyze 同源出网）
curl -s "http://127.0.0.1:3000/gamma-public-profile/0x你的钱包地址" | jq .

# 子图 + 对账（仅当次 query，不写进镜像）
curl -s "http://127.0.0.1:3000/analyze/0x你的钱包地址?with_subgraph=true&with_reconciliation=true" | jq .

# 查看日志
docker compose logs -f analyzer

# 停止并删除容器（数据卷保留）
docker compose down
```

**一次性 CLI 分析（不进常驻 API）：** 把报告写到宿主机目录 `./out`：

```bash
mkdir -p out
docker compose run --rm -v "$(pwd)/out:/out" analyzer \
  analyze 0x你的钱包地址 --out /out/report.json
```

**要靠 TOML / `PAA_*` / 挂载文件调的参数：** 见 **[配置：TOML、PAA 环境变量与 Docker](#paaconfig)**；仓库内 **`config.docker.example.toml`** 可复制为 **`config.docker.toml`**（挂载 `--config` 时用）。

**文件说明：**

| 文件 | 作用 |
|------|------|
| `Dockerfile` | 多阶段构建 release 二进制；运行时仅 `ca-certificates`（HTTPS） |
| `docker-compose.yml` | `postgres`（健康检查）+ `analyzer`（默认 `0.0.0.0:3000`） |
| **`docker-compose.postgres-only.yml`** | **仅 PostgreSQL**；`5432` 绑定 **`127.0.0.1`**，供**宿主机** Rust 连接（不经 Docker 跑后端） |
| `config.docker.example.toml` | Compose 挂载用 TOML 模板 → `config.docker.toml`（见「配置：TOML、PAA…」） |
| `.dockerignore` | 缩小构建上下文 |

**生产注意：** 修改默认数据库口令、不要用示例口令；默认 **未** 把 Postgres 端口映射到宿主机；需要本机 `psql` 调试时再在 compose 里打开 `ports`。日志级别：`RUST_LOG=debug docker compose up`。

---

## 极简测试部署（最快，推荐先试）

**不用在服务器装 Rust**：用仓库自带 **`docker-compose.yml`** 同时起 **Postgres + API**。

1. 准备一台 Linux VPS，安装 [Docker](https://docs.docker.com/engine/install/) 与 **Docker Compose V2**。
2. 把代码放到服务器（`git clone` 或上传），进入目录：  
   `cd polymarket-account-analyzer`
3. 一键后台启动：  
   `docker compose up --build -d`
4. 在云平台安全组 / `ufw` **放行 `3000`**，浏览器或 curl：  
   `http://你的VPS公网IP:3000/analyze/0x你的钱包地址`  
   本机在服务器上测：`curl -sS "http://127.0.0.1:3000/analyze/0x..." | head -c 200`

**绑定域名 `api.forevex.trade`（可选，仍可先不配 DNS）：**

- DNS 里给 **`forevex.trade`** 增加子域 **`api`** 的 **A 记录** → VPS 公网 IP（各面板一般填主机记录 **`api`**）。  
- 验证：`dig +short api.forevex.trade A`  
- 访问：`http://api.forevex.trade:3000/analyze/...`（同样要放行 **3000**）。

**要上 HTTPS（Let's Encrypt）**：在服务器装 **Caddy**，`Caddyfile` 写两行（API 仍监听本机 `3000`，与 compose 默认一致）：

```caddyfile
api.forevex.trade {
    reverse_proxy 127.0.0.1:3000
}
```

然后 `sudo systemctl reload caddy`（或按 Caddy 文档 reload）。对外即 **`https://api.forevex.trade/analyze/{wallet}`**。前端跨域需配 **`PAA_CORS_ORIGINS`** / `cors_allowed_origins`。

更细说明见 **`documents/deployment-server.mdx`**（文档站「服务器部署」）。

### 怎么确认在跑？为什么 `curl /analyze` 像「没反应」？

1. **SSH 登录 VPS**，看容器是否在跑：  
   `cd polymarket-account-analyzer && docker compose ps`  
   应看到 **`postgres`**、**`analyzer`** 为 `running` / `healthy`。
2. **看 API 日志**（最有用）：  
   `docker compose logs -f --tail=80 analyzer`  
   启动成功时会有 **`listening on 0.0.0.0:3000`**；请求 `/analyze` 时会持续打日志直到算完。
3. **先测健康检查**（**秒回**，不连数据库、不拉 Data API）：  
   `curl -v --max-time 5 http://127.0.0.1:3000/health`  
   应返回 **`ok`**。若本机都失败，说明服务没起来或端口不是 3000。
4. **`/analyze` 可能很慢**：会从 Polymarket Data API 等拉数据，**几分钟都正常**；`curl` **要等整份 JSON 响应结束**才会把 body 给你，期间终端可能**长时间没有输出**。请加大超时，例如：  
   `curl -v --max-time 600 "http://127.0.0.1:3000/analyze/0x你的地址" | head -c 500`
5. **从你电脑访问 `http://54.x.x.x:3000` 一直卡住**：检查云厂商 **安全组入站**、Linux **`sudo ufw status`**，是否放行 **TCP 3000**；可用 `curl -v --max-time 10 http://54.x.x.x:3000/health` 先测（部署了新版本才有 `/health`，旧镜像需 `docker compose up --build -d` 重建）。

---

## 进阶：Docker 只跑 Postgres + 宿主机 Rust + 域名（api.forevex.trade）

适用：**只想容器跑库**、**API 用本机 `cargo build`** 调试/升级，对外仍可用 **Caddy + `api.forevex.trade`**。

### 1) DNS

在 DNS 中为 **`api.forevex.trade`** 配置 **A / AAAA** → VPS IP。检查：`dig +short api.forevex.trade A`。

### 2) 服务器防火墙

放行 **22**（SSH）、**80**、**443**。**不要**把 Postgres **5432** 暴露到公网；下文 Compose 仅绑定 **127.0.0.1:5432**。

### 3) 只启动 PostgreSQL

在 **`polymarket-account-analyzer/`** 目录：

```bash
docker compose -f docker-compose.postgres-only.yml up -d
```

**生产务必**编辑该文件中的 `POSTGRES_PASSWORD`（及同步到 `DATABASE_URL`）。

宿主机连接串示例：

```bash
export DATABASE_URL="postgres://postgres:你的强口令@127.0.0.1:5432/polymarket_analyzer"
```

### 4) 编译并运行 Rust API（监听本机回环）

```bash
cd polymarket-account-analyzer
cargo build --release
export DATABASE_URL="postgres://postgres:你的强口令@127.0.0.1:5432/polymarket_analyzer"
export RUST_LOG=info
./target/release/polymarket-account-analyzer serve --bind 127.0.0.1:3000
```

验证：`curl -sS "http://127.0.0.1:3000/analyze/0x你的地址" | head -c 200`

可选 **`systemd`** 常驻：创建 `polymarket-analyzer.service`，`ExecStart` 指向上述二进制，`Environment=DATABASE_URL=...`，`After=docker.service`。勿将含密码的 unit 提交到 Git。

### 5) 反代 + TLS（Caddy 示例）

安装 [Caddy](https://caddyserver.com/docs/install)，在 `Caddyfile` 中增加：

```caddyfile
api.forevex.trade {
    reverse_proxy 127.0.0.1:3000
}
```

`caddy reload` 后，Caddy 会自动申请 **Let’s Encrypt** 证书。对外地址：`https://api.forevex.trade/analyze/{wallet}`。

若浏览器跨域访问，需配置 **`cors_allowed_origins`** / **`PAA_CORS_ORIGINS`**（见上文配置章节）。

**Nginx** 亦可：`proxy_pass http://127.0.0.1:3000;`，证书用 **certbot** `--nginx`。

### 6) 文档站（Mintlify）

更完整的步骤（含跨平台编译、安全清单、与全套 Docker 对比）见 monorepo **`documents/deployment-server.mdx`**（在线文档导航 **「服务器部署」**）。

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

**路由：**

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | **`/analyze/:wallet`** | 完整 `AnalyzeReport` JSON |
| `GET` | **`/leaderboard`** | 首页榜单（需 **Postgres**）；Query：`limit` 默认 30（1～100）；`period`：`all`（默认，生涯快照表）、`today`（UTC 自然日起）、`week` / `month`（最近 7 / 30 天，按 `wallet_trade_pnl` 时间戳聚合，且仅统计与 `wallet_leaderboard_stats.cache_key` 一致的行） |

**CORS（浏览器直连 VPS）：** 配置顶层 **`cors_allowed_origins`**（逗号分隔），或环境变量 **`PAA_CORS_ORIGINS`**（例如 `https://stats.example.com,http://localhost:3000`）。为空则**不**挂载 CORS 中间件。

`GET /analyze/:wallet`：`wallet` 为路径参数，**不要**对 `0x` 地址再做一层 URL 编码破坏路径，一般直接写即可。**命中** Postgres 报告缓存时，响应会**立即**返回缓存 JSON；增量刷新 `wallet_*` 快照在**后台任务**中执行，不再阻塞该次 HTTP。

**Query 参数**（与 `Analyze` CLI 覆盖一致，均为可选；布尔值常用 `true` / `false`）：

| 参数 | 说明 |
|------|------|
| `no_cache` | 跳过报告缓存 |
| `cached_only` | **仅**读 Postgres 报告缓存；命中返回 200 JSON，未命中 **204 No Content**（无 body，避免浏览器把正常 miss 记成 404）；未配置存储时仍为 **404** `{"error":"cache_miss"}`；与 `no_cache` 互斥。布尔查询请写 **`cached_only=true`**（不要用 `=1`，Axum 会 400） |
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
curl -s "http://127.0.0.1:3000/leaderboard?limit=20" | jq .
curl -s "http://127.0.0.1:3000/leaderboard?limit=20&period=today" | jq .
```

> **说明：** 当前 **没有** 与 `report-from-canonical-run`、`export-ambiguous`、`set-ambiguous-review` 等价的 HTTP 接口；这些仅 CLI。

---

## 配置详解（TOML）

复制 **`config.example.toml` → `config.toml`** 后按需编辑；运行时务必 **`--config config.toml`**，否则该文件**不会被读取**（见 **[配置文件怎么用（必读）](#配置文件怎么用必读)**）。**本地 vs Docker Compose 操作步骤**见 **[配置：TOML、PAA 环境变量与 Docker](#paaconfig)**。

连接数据库：**TOML 里 `database_url` 非空则优先**；否则用环境变量 **`DATABASE_URL`**（`init_storage`）。

### 顶层常用项

| 项 | 作用 | 默认 |
|----|------|------|
| `database_url` | PostgreSQL 连接串 | 无（仅 `DATABASE_URL` 或 TOML） |
| `cache_ttl_sec` | 报告缓存 TTL（秒） | 600 |
| `timeout_sec` | Data API / Gamma HTTP 超时（秒） | 90 |
| `trades_page_limit` | Data API 分页 | 500 |
| `rate_limit_ms` | Data API 请求间隔 | 150 |
| `cors_allowed_origins` | `serve` 时允许的浏览器 `Origin`（逗号分隔）；空 = 不启用 CORS | 空 |

### 各配置块

| 块 | 作用 |
|----|------|
| **`[subgraph]`** | 子图 URL、`enabled`、**分页与封顶**、超时/重试、`extended_fill_fields`、`show_progress`、`user_agent` 等 |
| **`[reconciliation]`** | `enabled`、v0 时间窗、v1 **`size_tolerance_pct` / `price_tolerance_abs` / `require_condition_match` / `rules_version`**、质量阈值 **`shadow_volume_alert_ratio`、`api_only_ratio_alert`、`api_only_alert_min`** |
| **`[analytics]`** | **`source`**: `data_api`（默认）或 **`canonical`**（合成成交驱动主指标，空则回退 Data API）；**`canonical_shadow`**：算 shadow 与 `metrics_canonical_shadow` |
| **`[ingestion]`** | **`persist_raw`**；**`max_gamma_slugs_for_timing`**；**`data_api_positions_limit`**；**`persist_positions_raw`**；**`persist_wallet_snapshots`**；**`data_api_incremental_trades`** / **`data_api_incremental_max_pages`**（**`wallet_*`** 与增量说明见 [`artifacts/wallet-pipeline-snapshot.md`](../artifacts/wallet-pipeline-snapshot.md)） |
| **`[canonical]`** | **`enabled`**：规范合并 + `canonical_events` 等落库；**`enrich_markets_dim`**：Gamma 补市场维表 |
| **`[market_type]`** | 按 slug 归类市场类型：**规则有序，首条命中生效**；支持 **`contains`** / **`prefix`**（见 `config.rs` / 示例文件） |

---

## 报告缓存与指纹

启用 Postgres 时，同一钱包的报告可能来自 **`report_cache_kv`**。缓存键由 **钱包（小写）+ 配置指纹哈希** 组成，指纹包含例如：

`trades_page_limit`、`subgraph`（enabled、cap、page_size、max_pages、positions_page_size、skip_pnl、extended_fill_fields）、`canonical`、`reconciliation` 各字段、`ingestion`（含 **`persist_raw`、`max_gamma_slugs_for_timing`、`data_api_positions_limit`、`persist_positions_raw`、`persist_wallet_snapshots`、`data_api_incremental_trades`、`data_api_incremental_max_pages`**）、`analytics` 等。

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

**历史深度：** 服务端可能对 offset 设硬上限（如 `max historical activity offset of 3000 exceeded`）。本工具会停止翻页并在报告中标注 **`data_fetch.truncated`** / **`ingestion.truncation.data_api`**。更全链上历史需其他管道（子图全量、自建索引等）；详细规划见 [相关文档](#相关文档) 中的 **`report-and-reconciliation-plan.md`**（路径因仓库布局而异）。

---

## 报告 JSON（schema 2.x）

当前发布 **`schema_version: "2.5.2"`**（旧缓存可能仍为 `2.5.1` 及更早）。**2.5.2**：**`frontend.trade_ledger`** — 时间序逐笔台账（Data API 每笔成交 BUY/SELL + 已 resolve 持仓的 **SETTLEMENT** 行；字段含买入/卖出侧价额与本行 **`pnl`**）。**2.5.1**：结算 PnL 补齐 **`asset:{token}`** 库存键。**2.5**：**`net_pnl_settlement`**、**`net_pnl`**。**2.4** / **2.3**：略。

| 块 | 说明 |
|----|------|
| **`lifetime`** | **`net_pnl`**（2.5+）= SELL 已实现 + **`net_pnl_settlement`**（Gamma 结算份额）；`total_volume` = Σ `size*price`；**`total_trades`**（2.4+）= 不同 slug/条件数；**`max_single_win` / `max_single_loss`**（含结算腿极值）；`open_position_value`、**`closed_realized_pnl_sum`**、**`open_positions_count`** |
| **`time_analysis`** | **`entry_to_resolution_seconds`**（有 Gamma 解析时填充）、**`entry_to_resolution_p50_sec` / `p90_sec`**、`metadata_missing_ratio` 按样本比例更新 |
| **`trading_patterns`** | **`grid_like_market_ratio`**、**`win_rate_closed_positions`**（样本 ≥5 时）、**`closed_positions_sample_size`** |
| **`strategy_inference`** | `src/strategy.rs`：**`high-frequency-grid-scalper`**（网格占比 >20% 且 entry P90 < 60s 等）；**`rule_json`** 含 `entry_window_sec_avg`、`preferred_price_ranges`、`jackpot_bias`、`multi_window_count` 等；更长的 **pseudocode** |
| **`price_buckets_chart`** | 固定区间 + `label`（`<0.1`、`0.1–0.3`…），便于前端图表轴一致 |
| **`frontend`** | **`biggest_wins` / `biggest_losses`**（按 **`pnl`** 排序：单笔**已实现**盈亏，平均成本法；买为 0）、**`recent_trades`**、**`trade_ledger`**（2.5.2+，逐笔台账）、**`current_positions`**、**`ai_copy_prompt`** |
| **`gamma_profile`** | Gamma **`/public-profile`**：`display_name`、`username`、`avatar_url`、`created_at`、`bio`、`verified_badge`、`proxy_wallet`、`x_username`（有则填） |

仍包含：`wallet`、**`trades_count`**（2.4+ 为市场数）、**`trades_fill_count`**（2.4+）、**`report_updated_at`**（RFC3339 UTC：新算为生成时刻；读缓存时取 PG `updated_at`）、`total_volume`、`market_distribution`、`price_buckets`、`trading_patterns`、`notes`、`data_fetch`、`ingestion`（含 **`truncation`**）、`subgraph`、`reconciliation`、`reconciliation_v1`、`canonical_summary`、`data_lineage`、**`provenance`**、**`metrics_canonical_shadow`**。

---

## Postgres 表（节选）

启动时会 **`init_schema`**（内联 SQL，无独立 migrations 目录）：

| 类别 | 表 |
|------|-----|
| 报告缓存 | **`report_cache_kv`**（主）；`analyze_report_cache`（仅 legacy 读） |
| 前端 / 榜单 | **`wallet_trade_pnl`**（每笔 `pnl` + 成交字段，按 `cache_key` 覆盖写入）；**`wallet_leaderboard_stats`**（按 `lifetime.net_pnl` 排名；**`trades_count` 列** 2.4+ 与报告一致为**市场数**，非 `/trades` 行数） |
| 任务与 Raw | `ingestion_run`、`raw_ingestion_chunk`、`raw_data_api_trades`、**`raw_data_api_open_positions`**、**`raw_data_api_closed_positions`**、`raw_subgraph_*` |
| 维表 | `markets_dim` |
| 规范与对账 | `canonical_events`、`source_event_map`、`reconciliation_report`、`reconciliation_ambiguous_queue` |
| Wallet 最新快照（BI / 分步） | **`wallet_pipeline_meta`**、**`wallet_primary_trade_row`**、**`wallet_canonical_event_row`**、**`wallet_reconciliation_current`**、**`wallet_frontend_current`**、**`wallet_strategy_current`**、**`wallet_report_snapshot`**（见 **`artifacts/wallet-pipeline-snapshot.md`**） |

---

## 开发与测试

```bash
cd Polymarket-Analyzer
cargo test              # 含 tests/regression_merge.rs（7 项，无网络）
cargo build --release
```

若上游 monorepo 根目录提供一键脚本（本仓库单独克隆时**可能没有**）：

```bash
./scripts/regression-check.sh
```

手工验收清单：见 [相关文档](#相关文档) 中的 **`manual-test-checklist.md`**（同上，仅部分仓库布局存在）。

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

更细的差距矩阵见 **`report-and-reconciliation-plan.md`** §1.5.4（路径见下表）。

---

## 相关文档

**说明：** 单独克隆 **`Polymarket-Analyzer`** 时，仓库内**通常没有**下表中的 `artifacts/`、`task_plan.md` 等文件（它们可能只存在于上游 **Forevex / account-analyzer** 类 monorepo）。若链接 404，以本 README 与源码为准即可。

| 文档 | 内容 |
|------|------|
| [**`artifacts/PROJECT-OVERVIEW.md`**](../artifacts/PROJECT-OVERVIEW.md) | 与本文互补的**总览**（完成度表、索引） |
| [**`artifacts/report-and-reconciliation-plan.md`**](../artifacts/report-and-reconciliation-plan.md) | 全量数据、对账、Postgres、报告规划；**§1.5** 为工作流权威摘要 |
| [**`artifacts/raw-ingestion-contract.md`**](../artifacts/raw-ingestion-contract.md) | 外部管道与 raw 表契约（P3） |
| [**`artifacts/manual-test-checklist.md`**](../artifacts/manual-test-checklist.md) | 手工测试清单 |
| [**`artifacts/progress-log.md`**](../artifacts/progress-log.md) | 按日迭代与验证记录 |
| [**`artifacts/backlog-waiting.md`**](../artifacts/backlog-waiting.md) | **待解决 / 按需开发**（增量假设、缓存与行表一致性、后续能力） |
| [**`documents/deployment-server.mdx`**](../documents/deployment-server.mdx) | **生产部署**：Postgres-only Docker、宿主机 Rust、域名与 TLS（文档站同步） |
| [`task_plan.md`](../task_plan.md)、[`findings.md`](../findings.md)、[`progress.md`](../progress.md) | 任务、决策、进度摘要 |

---

## 许可证

Apache-2.0
