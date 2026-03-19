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
| `database_url` | Postgres 连接串（可选） | 无，也可用环境变量 `DATABASE_URL` |
| `[market_type]` | 市场类型分类规则（slug → type） | 见 `config.example.toml` |

未配置 `database_url` 时，不启用缓存，每次请求都会请求 Polymarket Data API。

## 数据源

- [Polymarket Data API](https://data-api.polymarket.com)（公开，无需认证）
  - `GET /trades?user={wallet}&limit=500&offset={n}`：分页拉取全部 trades
- 可选：[Gamma API](https://gamma-api.polymarket.com) 用于市场元信息（如 endDate/closedTime）补全

## 技术栈

- Rust（Axum 0.7、tokio、reqwest + rustls、serde、chrono、anyhow、clap 4）
- 可选：sqlx + Postgres 缓存

## 许可证

Apache-2.0
