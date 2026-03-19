# Polymarket Account Analyzer

只读分析工具：输入任意 Polymarket 钱包地址，拉取公开 Data API 数据，从多维度分析交易历史并输出报告与策略推断。支持 CLI 与 REST API，可选 Postgres 缓存。

## 功能

- **全市场支持**：支持 BTC/ETH 5-min、1h、daily、event、政治、体育、股票等任意预测市场
- **分析维度**：Lifetime 指标、市场分布、价格分桶、时间分布、交易模式、策略反推（风格 + 规则 JSON + 伪代码）
- **输出**：CLI 打印 / 写文件；REST API `GET /analyze/:wallet` 返回 JSON
- **缓存**：可选 Postgres 缓存分析结果，避免重复拉取
- **只读**：不涉及交易、下单或签名

## 环境要求

- Rust 最新稳定版
- （可选）PostgreSQL，用于缓存

## 快速开始

```bash
# 克隆后进入项目目录
cd polymarket-account-analyzer

# 编译
cargo build --release

# CLI：分析钱包并输出到 stdout
cargo run --release -- analyze 0x5924ca480d8b08cd5f3e5811fa378c4082475af6

# CLI：将报告写入文件
cargo run --release -- analyze 0x5924ca480d8b08cd5f3e5811fa378c4082475af6 --out report.json

# 跳过缓存强制重新拉取
cargo run --release -- analyze 0x5924ca480d8b08cd5f3e5811fa378c4082475af6 --no-cache

# 启动 API 服务（默认 127.0.0.1:3000）
cargo run --release -- serve

# 指定监听地址
cargo run --release -- serve --bind 0.0.0.0:8080
```

## API 使用

```bash
# 分析指定钱包
curl -s "http://127.0.0.1:3000/analyze/0x5924ca480d8b08cd5f3e5811fa378c4082475af6" | jq .

# 跳过缓存
curl -s "http://127.0.0.1:3000/analyze/0x5924ca480d8b08cd5f3e5811fa378c4082475af6?no_cache=true" | jq .
```

## 配置

复制示例配置并按需修改：

```bash
cp config.example.toml config.toml
```

| 选项 | 说明 | 默认 |
|------|------|------|
| `rate_limit_ms` | 请求间隔（毫秒） | 150 |
| `cache_ttl_sec` | 缓存有效期（秒） | 600 |
| `timeout_sec` | HTTP 超时（秒） | 90 |
| `trades_page_limit` | `/trades` 每页条数（与 offset 步长一致，最大 10000） | 500；可设 3000 减少请求次数 |
| `database_url` | Postgres 连接串（可选） | 无，也可用环境变量 `DATABASE_URL` |
| `[market_type]` | 市场类型分类规则（slug → type） | 见 `config.example.toml` |

未配置 `database_url` 时，不启用缓存，每次请求都会请求 Polymarket Data API。

## 数据源

- [Polymarket Data API](https://data-api.polymarket.com)（公开，无需认证）
  - `GET /trades?user={wallet}&limit=500&offset={n}`：分页拉取全部 trades
- 可选：[Gamma API](https://gamma-api.polymarket.com) 用于市场元信息（如 endDate/closedTime）补全

### 关于「分页」与历史深度上限（重要）

**本工具已经在做分页**：按 `limit`（例如 500）递增 `offset` 逐页请求，直到返回空页或遇到服务端限制。

报错 **`max historical activity offset of 3000 exceeded`** 来自 **Polymarket 服务端对「历史活动」的 offset 硬上限**，不是「客户端没分页」造成的。在该限制下，即使用更小的 `limit` 分页，**也无法用更大的 `offset` 继续往更早的历史翻页**（上限由服务端决定）。

当前实现会在触发该限制时 **停止继续请求**，并返回已获取到的 trades；报告里会有：

- `data_fetch.truncated: true`
- `data_fetch.max_offset_allowed`（若可从错误信息解析，一般为 `3000`）

若你需要 **超出该 API 窗口的完整链上历史**，需要走其他数据源（例如官方文档提到的 **Subgraph / 链上索引** 等），而不是仅靠当前 `GET /trades` 的 offset 翻页。

## 技术栈

- Rust（Axum 0.7、tokio、reqwest + rustls、serde、chrono、anyhow、clap 4）
- 可选：sqlx + Postgres 缓存

## Subgraph spike（可行性探针）

在接链上全量/结算数据前进主架构前，可用示例对 **Goldsky 官方子图** 发 GraphQL，验证地址过滤与字段是否与 Playground 一致：

```bash
# 看 Activity 里 Redemption 类型有哪些字段
cargo run --example subgraph_spike -- --introspect-redemption --skip-wallet-queries

# 对指定钱包探测：redemptions + orderFilledEvents(maker/taker) + userPositions
cargo run --example subgraph_spike -- 0xYourWallet...

# 合并结果写入 JSON 文件（终端不全时用这个；默认不再把大段 JSON 打到 stdout）
cargo run --example subgraph_spike -- --out subgraph_spike.json 0xYourWallet...

# 仍要同时在终端打印各段 JSON（可能很长）
cargo run --example subgraph_spike -- --out subgraph_spike.json --tee 0xYourWallet...
```

子图文档：[Polymarket Subgraph](https://docs.polymarket.com/market-data/subgraph)

## 许可证

Apache-2.0
