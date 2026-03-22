use crate::canonical::{CanonicalEventInsert, CanonicalMergeArtifacts};
use crate::polymarket::data_api::{trade_dedup_key, Trade, TradeSide};
use crate::report::AnalyzeReport;
use anyhow::Context;
use chrono::{DateTime, Utc};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::time::Duration;
use uuid::Uuid;

/// Metadata for wallet-scoped **latest** pipeline snapshots (Scheme A: multi-table, overwrite per analyze).
#[derive(Debug, Clone)]
pub struct WalletPipelineSnapshotMeta {
    pub last_ingestion_run_id: Option<String>,
    pub analytics_lane: String,
    pub cache_key: String,
    pub schema_version: String,
    pub data_api_truncated: bool,
    pub data_api_max_offset_allowed: Option<i32>,
    /// Max trade `timestamp` (normalized to ms) after this run; used for next incremental fetch.
    pub data_api_trade_watermark_ms: Option<i64>,
}

fn wallet_snapshot_meta_from_report(
    report: &AnalyzeReport,
    cache_key: &str,
    trades: Option<&[Trade]>,
) -> WalletPipelineSnapshotMeta {
    let wm = trades.and_then(|t| {
        t.iter()
            .map(|x| crate::polymarket::data_api::trade_timestamp_ms(x.timestamp))
            .max()
    });
    WalletPipelineSnapshotMeta {
        last_ingestion_run_id: report.ingestion.as_ref().and_then(|i| i.run_id.clone()),
        analytics_lane: report
            .data_lineage
            .as_ref()
            .map(|d| d.analytics_primary_source.clone())
            .unwrap_or_else(|| "data_api_trades".to_string()),
        cache_key: cache_key.to_string(),
        schema_version: report.schema_version.clone(),
        data_api_truncated: report.data_fetch.truncated,
        data_api_max_offset_allowed: report
            .data_fetch
            .max_offset_allowed
            .and_then(|u| i32::try_from(u).ok()),
        data_api_trade_watermark_ms: wm,
    }
}

#[derive(Clone)]
pub struct Storage {
    pool: PgPool,
}

impl Storage {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(10))
            .connect(database_url)
            .await
            .context("connect postgres")?;
        Ok(Self { pool })
    }

    pub async fn init_schema(&self) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS analyze_report_cache (
                wallet TEXT PRIMARY KEY,
                report_json JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create analyze_report_cache table")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS report_cache_kv (
                cache_key TEXT PRIMARY KEY,
                wallet TEXT NOT NULL,
                report_json JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create report_cache_kv table")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS report_cache_kv_wallet_idx
            ON report_cache_kv (wallet)
            "#,
        )
        .execute(&self.pool)
        .await
        .context("index report_cache_kv wallet")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS wallet_trade_pnl (
                cache_key TEXT NOT NULL,
                trade_index INT NOT NULL,
                wallet TEXT NOT NULL,
                condition_id TEXT NOT NULL,
                slug TEXT NOT NULL,
                side TEXT NOT NULL,
                size DOUBLE PRECISION NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                cash_flow DOUBLE PRECISION NOT NULL,
                pnl DOUBLE PRECISION NOT NULL,
                timestamp BIGINT NOT NULL,
                PRIMARY KEY (cache_key, trade_index)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create wallet_trade_pnl")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS wallet_trade_pnl_wallet_idx
            ON wallet_trade_pnl (wallet)
            "#,
        )
        .execute(&self.pool)
        .await
        .context("index wallet_trade_pnl wallet")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS wallet_leaderboard_stats (
                wallet TEXT PRIMARY KEY,
                cache_key TEXT NOT NULL,
                net_pnl DOUBLE PRECISION NOT NULL,
                total_volume DOUBLE PRECISION NOT NULL,
                trades_count BIGINT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create wallet_leaderboard_stats")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS ingestion_run (
                id TEXT PRIMARY KEY,
                wallet TEXT NOT NULL,
                params JSONB NOT NULL,
                status TEXT NOT NULL DEFAULT 'running',
                started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                finished_at TIMESTAMPTZ
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create ingestion_run table")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS raw_ingestion_chunk (
                id BIGSERIAL PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES ingestion_run(id) ON DELETE CASCADE,
                wallet TEXT NOT NULL,
                source TEXT NOT NULL,
                chunk_index INT NOT NULL,
                payload JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(run_id, source, chunk_index)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create raw_ingestion_chunk table")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS raw_data_api_trades (
                id BIGSERIAL PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES ingestion_run(id) ON DELETE CASCADE,
                wallet TEXT NOT NULL,
                page_offset INT NOT NULL,
                row_index INT NOT NULL,
                payload JSONB NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(run_id, page_offset, row_index)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create raw_data_api_trades")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS raw_data_api_open_positions (
                id BIGSERIAL PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES ingestion_run(id) ON DELETE CASCADE,
                wallet TEXT NOT NULL,
                row_index INT NOT NULL,
                payload JSONB NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(run_id, row_index)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create raw_data_api_open_positions")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS raw_data_api_closed_positions (
                id BIGSERIAL PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES ingestion_run(id) ON DELETE CASCADE,
                wallet TEXT NOT NULL,
                row_index INT NOT NULL,
                payload JSONB NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(run_id, row_index)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create raw_data_api_closed_positions")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS raw_subgraph_redemptions (
                id BIGSERIAL PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES ingestion_run(id) ON DELETE CASCADE,
                wallet TEXT NOT NULL,
                event_id TEXT NOT NULL,
                occurred_at TIMESTAMPTZ,
                payload JSONB NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(run_id, event_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create raw_subgraph_redemptions")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS raw_subgraph_order_filled (
                id BIGSERIAL PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES ingestion_run(id) ON DELETE CASCADE,
                wallet TEXT NOT NULL,
                side_role TEXT NOT NULL,
                event_id TEXT NOT NULL,
                occurred_at TIMESTAMPTZ,
                condition_id TEXT,
                payload JSONB NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(run_id, event_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create raw_subgraph_order_filled")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS raw_subgraph_user_positions (
                id BIGSERIAL PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES ingestion_run(id) ON DELETE CASCADE,
                wallet TEXT NOT NULL,
                position_id TEXT NOT NULL,
                payload JSONB NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE(run_id, position_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create raw_subgraph_user_positions")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS markets_dim (
                slug TEXT PRIMARY KEY,
                title TEXT,
                end_date TIMESTAMPTZ,
                closed_time TIMESTAMPTZ,
                raw_gamma JSONB NOT NULL DEFAULT '{}',
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create markets_dim")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS canonical_events (
                id UUID PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES ingestion_run(id) ON DELETE CASCADE,
                wallet TEXT NOT NULL,
                event_type TEXT NOT NULL,
                occurred_at TIMESTAMPTZ NOT NULL,
                condition_id TEXT,
                market_slug TEXT,
                amounts JSONB NOT NULL DEFAULT '{}',
                source_refs JSONB NOT NULL DEFAULT '[]',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create canonical_events")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_canonical_wallet_run
            ON canonical_events(wallet, run_id)
            "#,
        )
        .execute(&self.pool)
        .await
        .context("index canonical_events")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS source_event_map (
                id BIGSERIAL PRIMARY KEY,
                canonical_id UUID NOT NULL REFERENCES canonical_events(id) ON DELETE CASCADE,
                source TEXT NOT NULL,
                source_row_id TEXT NOT NULL,
                confidence TEXT NOT NULL,
                UNIQUE(canonical_id, source, source_row_id)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create source_event_map")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS reconciliation_report (
                id BIGSERIAL PRIMARY KEY,
                wallet TEXT NOT NULL,
                run_id TEXT REFERENCES ingestion_run(id) ON DELETE SET NULL,
                rules_version TEXT NOT NULL,
                report JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create reconciliation_report")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_reconciliation_wallet
            ON reconciliation_report(wallet)
            "#,
        )
        .execute(&self.pool)
        .await
        .context("index reconciliation_report")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS reconciliation_ambiguous_queue (
                id BIGSERIAL PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES ingestion_run(id) ON DELETE CASCADE,
                wallet TEXT NOT NULL,
                trade_key TEXT NOT NULL,
                candidates JSONB NOT NULL,
                reason TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create reconciliation_ambiguous_queue")?;

        sqlx::query(
            r#"
            ALTER TABLE reconciliation_ambiguous_queue
            ADD COLUMN IF NOT EXISTS review_status TEXT NOT NULL DEFAULT 'pending'
            "#,
        )
        .execute(&self.pool)
        .await
        .context("alter reconciliation_ambiguous_queue review_status")?;

        sqlx::query(
            r#"
            ALTER TABLE reconciliation_ambiguous_queue
            ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            "#,
        )
        .execute(&self.pool)
        .await
        .context("alter reconciliation_ambiguous_queue updated_at")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS wallet_pipeline_meta (
                wallet TEXT PRIMARY KEY,
                last_ingestion_run_id TEXT,
                analytics_lane TEXT NOT NULL,
                cache_key TEXT NOT NULL,
                schema_version TEXT NOT NULL,
                data_api_truncated BOOLEAN NOT NULL DEFAULT false,
                data_api_max_offset_allowed INT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create wallet_pipeline_meta")?;

        sqlx::query(
            r#"
            ALTER TABLE wallet_pipeline_meta
            ADD COLUMN IF NOT EXISTS data_api_trade_watermark_ms BIGINT
            "#,
        )
        .execute(&self.pool)
        .await
        .context("alter wallet_pipeline_meta watermark")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS wallet_primary_trade_row (
                wallet TEXT NOT NULL,
                seq INT NOT NULL,
                lane TEXT NOT NULL,
                side TEXT NOT NULL,
                size DOUBLE PRECISION NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                cash_flow DOUBLE PRECISION NOT NULL,
                realized_pnl DOUBLE PRECISION NOT NULL,
                condition_id TEXT NOT NULL,
                slug TEXT NOT NULL,
                title TEXT,
                outcome TEXT,
                timestamp_ms BIGINT NOT NULL,
                occurred_at TIMESTAMPTZ NOT NULL,
                transaction_hash TEXT,
                asset TEXT,
                event_slug TEXT,
                outcome_index BIGINT,
                proxy_wallet TEXT,
                trade_key TEXT NOT NULL,
                PRIMARY KEY (wallet, seq)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create wallet_primary_trade_row")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_wallet_primary_trade_time
            ON wallet_primary_trade_row (wallet, occurred_at)
            "#,
        )
        .execute(&self.pool)
        .await
        .context("index wallet_primary_trade_row time")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS wallet_canonical_event_row (
                wallet TEXT NOT NULL,
                seq INT NOT NULL,
                canonical_id UUID NOT NULL,
                event_type TEXT NOT NULL,
                occurred_at TIMESTAMPTZ NOT NULL,
                condition_id TEXT,
                market_slug TEXT,
                amounts JSONB NOT NULL,
                source_refs JSONB NOT NULL,
                PRIMARY KEY (wallet, seq)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create wallet_canonical_event_row")?;

        sqlx::query(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS wallet_canonical_event_row_canonical_id
            ON wallet_canonical_event_row (canonical_id)
            "#,
        )
        .execute(&self.pool)
        .await
        .context("unique canonical_id wallet_canonical_event_row")?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_wallet_canonical_event_time
            ON wallet_canonical_event_row (wallet, occurred_at)
            "#,
        )
        .execute(&self.pool)
        .await
        .context("index wallet_canonical_event_row time")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS wallet_reconciliation_current (
                wallet TEXT PRIMARY KEY,
                reconciliation_v0 JSONB,
                reconciliation_v1 JSONB,
                last_run_id TEXT,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create wallet_reconciliation_current")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS wallet_frontend_current (
                wallet TEXT PRIMARY KEY,
                payload JSONB,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create wallet_frontend_current")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS wallet_strategy_current (
                wallet TEXT PRIMARY KEY,
                payload JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create wallet_strategy_current")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS wallet_report_snapshot (
                wallet TEXT PRIMARY KEY,
                cache_key TEXT NOT NULL,
                report_json JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .context("create wallet_report_snapshot")?;

        Ok(())
    }

    /// Prefer `report_cache_kv` by `cache_key` (config fingerprint); fall back to legacy `analyze_report_cache` by wallet.
    pub async fn get_cached_report(
        &self,
        cache_key: &str,
        wallet_legacy: &str,
        ttl_seconds: i64,
    ) -> anyhow::Result<Option<AnalyzeReport>> {
        let row = sqlx::query(
            r#"
            SELECT report_json
            FROM report_cache_kv
            WHERE cache_key = $1
              AND updated_at > NOW() - ($2::bigint * INTERVAL '1 second')
            "#,
        )
        .bind(cache_key)
        .bind(ttl_seconds)
        .fetch_optional(&self.pool)
        .await
        .context("query report_cache_kv")?;

        if let Some(row) = row {
            let value: serde_json::Value = row.try_get("report_json")?;
            let report: AnalyzeReport = serde_json::from_value(value)?;
            return Ok(Some(report));
        }

        let row = sqlx::query(
            r#"
            SELECT report_json
            FROM analyze_report_cache
            WHERE wallet = $1
              AND updated_at > NOW() - ($2::bigint * INTERVAL '1 second')
            "#,
        )
        .bind(wallet_legacy)
        .bind(ttl_seconds)
        .fetch_optional(&self.pool)
        .await
        .context("query legacy analyze_report_cache")?;

        let Some(row) = row else {
            return Ok(None);
        };

        let value: serde_json::Value = row.try_get("report_json")?;
        let report: AnalyzeReport = serde_json::from_value(value)?;
        Ok(Some(report))
    }

    pub async fn upsert_report(
        &self,
        cache_key: &str,
        wallet: &str,
        report: &AnalyzeReport,
    ) -> anyhow::Result<()> {
        let report_json = serde_json::to_value(report)?;
        sqlx::query(
            r#"
            INSERT INTO report_cache_kv(cache_key, wallet, report_json, updated_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (cache_key)
            DO UPDATE SET report_json = EXCLUDED.report_json, updated_at = NOW(), wallet = EXCLUDED.wallet
            "#,
        )
        .bind(cache_key)
        .bind(wallet)
        .bind(report_json)
        .execute(&self.pool)
        .await
        .context("upsert report_cache_kv")?;
        Ok(())
    }

    /// Reconstruct [`Trade`] stream from `wallet_primary_trade_row` (for incremental merge / cache-hit refresh).
    pub async fn fetch_wallet_primary_trades(&self, wallet: &str) -> anyhow::Result<Vec<Trade>> {
        let rows = sqlx::query(
            r#"
            SELECT side, size, price, condition_id, slug, title, outcome, timestamp_ms,
                   transaction_hash, asset, event_slug, outcome_index, proxy_wallet
            FROM wallet_primary_trade_row
            WHERE lower(wallet) = lower($1::text)
            ORDER BY seq ASC
            "#,
        )
        .bind(wallet)
        .fetch_all(&self.pool)
        .await
        .context("fetch_wallet_primary_trades")?;

        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let side_s: String = r.try_get("side")?;
            let side = match side_s.as_str() {
                "SELL" => TradeSide::Sell,
                _ => TradeSide::Buy,
            };
            let ts_ms: i64 = r.try_get("timestamp_ms")?;
            out.push(Trade {
                proxy_wallet: r.try_get("proxy_wallet")?,
                side,
                asset: r.try_get("asset")?,
                condition_id: r.try_get("condition_id")?,
                size: r.try_get("size")?,
                price: r.try_get("price")?,
                timestamp: ts_ms,
                title: r.try_get("title")?,
                slug: r.try_get("slug")?,
                event_slug: r.try_get("event_slug")?,
                outcome: r.try_get("outcome")?,
                outcome_index: r.try_get("outcome_index")?,
                transaction_hash: r.try_get("transaction_hash")?,
            });
        }
        Ok(out)
    }

    pub async fn fetch_wallet_canonical_event_rows(
        &self,
        wallet: &str,
    ) -> anyhow::Result<Vec<CanonicalEventInsert>> {
        let rows = sqlx::query(
            r#"
            SELECT canonical_id, event_type, occurred_at, condition_id, market_slug, amounts, source_refs
            FROM wallet_canonical_event_row
            WHERE lower(wallet) = lower($1::text)
            ORDER BY seq ASC
            "#,
        )
        .bind(wallet)
        .fetch_all(&self.pool)
        .await
        .context("fetch_wallet_canonical_event_rows")?;

        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(CanonicalEventInsert {
                id: r.try_get("canonical_id")?,
                event_type: r.try_get("event_type")?,
                occurred_at: r.try_get("occurred_at")?,
                condition_id: r.try_get("condition_id")?,
                market_slug: r.try_get("market_slug")?,
                amounts: r.try_get("amounts")?,
                source_refs: r.try_get("source_refs")?,
            });
        }
        Ok(out)
    }

    /// On **report cache hit**, align `wallet_*` JSON snapshots with the cached report. If trade rows exist in PG, full replace; otherwise JSON-only upsert.
    pub async fn refresh_wallet_snapshots_after_cache_hit(
        &self,
        wallet: &str,
        cache_key: &str,
        report: &AnalyzeReport,
    ) -> anyhow::Result<()> {
        let trades = self.fetch_wallet_primary_trades(wallet).await?;
        if trades.is_empty() {
            let meta = wallet_snapshot_meta_from_report(report, cache_key, None);
            return self
                .upsert_wallet_pipeline_snapshots_json_only(wallet, &meta, report)
                .await;
        }
        let canon = self.fetch_wallet_canonical_event_rows(wallet).await?;
        let pnls = crate::trade_pnl::per_trade_realized_pnl(&trades);
        let meta = wallet_snapshot_meta_from_report(report, cache_key, Some(&trades));
        let canon_opt = if canon.is_empty() {
            None
        } else {
            Some(canon.as_slice())
        };
        self.replace_wallet_pipeline_snapshots(wallet, &meta, &trades, &pnls, canon_opt, report)
            .await
    }

    /// Updates meta + reconciliation/frontend/strategy/report JSON tables **without** touching trade/canonical rows.
    pub async fn upsert_wallet_pipeline_snapshots_json_only(
        &self,
        wallet: &str,
        meta: &WalletPipelineSnapshotMeta,
        report: &AnalyzeReport,
    ) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;

        let v0_json = report
            .reconciliation
            .as_ref()
            .map(|r| serde_json::to_value(r))
            .transpose()
            .context("serialize reconciliation v0")?;
        let v1_json = report
            .reconciliation_v1
            .as_ref()
            .map(|r| serde_json::to_value(r))
            .transpose()
            .context("serialize reconciliation v1")?;

        sqlx::query(
            r#"
            INSERT INTO wallet_pipeline_meta (
                wallet, last_ingestion_run_id, analytics_lane, cache_key, schema_version,
                data_api_truncated, data_api_max_offset_allowed, data_api_trade_watermark_ms, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
            ON CONFLICT (wallet) DO UPDATE SET
                last_ingestion_run_id = EXCLUDED.last_ingestion_run_id,
                analytics_lane = EXCLUDED.analytics_lane,
                cache_key = EXCLUDED.cache_key,
                schema_version = EXCLUDED.schema_version,
                data_api_truncated = EXCLUDED.data_api_truncated,
                data_api_max_offset_allowed = EXCLUDED.data_api_max_offset_allowed,
                data_api_trade_watermark_ms = EXCLUDED.data_api_trade_watermark_ms,
                updated_at = NOW()
            "#,
        )
        .bind(wallet)
        .bind(&meta.last_ingestion_run_id)
        .bind(&meta.analytics_lane)
        .bind(&meta.cache_key)
        .bind(&meta.schema_version)
        .bind(meta.data_api_truncated)
        .bind(meta.data_api_max_offset_allowed)
        .bind(meta.data_api_trade_watermark_ms)
        .execute(&mut *tx)
        .await
        .context("upsert wallet_pipeline_meta (json-only)")?;

        sqlx::query(
            r#"
            INSERT INTO wallet_reconciliation_current (wallet, reconciliation_v0, reconciliation_v1, last_run_id, updated_at)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (wallet) DO UPDATE SET
                reconciliation_v0 = EXCLUDED.reconciliation_v0,
                reconciliation_v1 = EXCLUDED.reconciliation_v1,
                last_run_id = EXCLUDED.last_run_id,
                updated_at = NOW()
            "#,
        )
        .bind(wallet)
        .bind(v0_json)
        .bind(v1_json)
        .bind(&meta.last_ingestion_run_id)
        .execute(&mut *tx)
        .await
        .context("upsert wallet_reconciliation_current (json-only)")?;

        let frontend_json = report
            .frontend
            .as_ref()
            .map(|f| serde_json::to_value(f))
            .transpose()
            .context("serialize frontend")?;

        sqlx::query(
            r#"
            INSERT INTO wallet_frontend_current (wallet, payload, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (wallet) DO UPDATE SET
                payload = EXCLUDED.payload,
                updated_at = NOW()
            "#,
        )
        .bind(wallet)
        .bind(frontend_json)
        .execute(&mut *tx)
        .await
        .context("upsert wallet_frontend_current (json-only)")?;

        let strategy_json =
            serde_json::to_value(&report.strategy_inference).context("serialize strategy")?;

        sqlx::query(
            r#"
            INSERT INTO wallet_strategy_current (wallet, payload, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (wallet) DO UPDATE SET
                payload = EXCLUDED.payload,
                updated_at = NOW()
            "#,
        )
        .bind(wallet)
        .bind(strategy_json)
        .execute(&mut *tx)
        .await
        .context("upsert wallet_strategy_current (json-only)")?;

        let report_json = serde_json::to_value(report).context("serialize full report")?;

        sqlx::query(
            r#"
            INSERT INTO wallet_report_snapshot (wallet, cache_key, report_json, updated_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (wallet) DO UPDATE SET
                cache_key = EXCLUDED.cache_key,
                report_json = EXCLUDED.report_json,
                updated_at = NOW()
            "#,
        )
        .bind(wallet)
        .bind(&meta.cache_key)
        .bind(report_json)
        .execute(&mut *tx)
        .await
        .context("upsert wallet_report_snapshot (json-only)")?;

        tx.commit().await?;
        Ok(())
    }

    /// Replace per-trade realized PnL rows for this report cache key (see `trade_pnl::per_trade_realized_pnl`).
    pub async fn replace_wallet_trade_pnls(
        &self,
        cache_key: &str,
        wallet: &str,
        trades: &[Trade],
        pnls: &[f64],
    ) -> anyhow::Result<()> {
        if trades.len() != pnls.len() {
            anyhow::bail!("trades / pnls length mismatch");
        }
        let w = wallet.to_lowercase();
        let mut tx = self.pool.begin().await.context("begin tx trade_pnl")?;
        sqlx::query("DELETE FROM wallet_trade_pnl WHERE cache_key = $1")
            .bind(cache_key)
            .execute(&mut *tx)
            .await
            .context("delete wallet_trade_pnl")?;
        for (i, t) in trades.iter().enumerate() {
            let vol = t.size * t.price;
            let cash_flow = match t.side {
                TradeSide::Buy => -vol,
                TradeSide::Sell => vol,
            };
            let pnl = pnls.get(i).copied().unwrap_or(0.0);
            let side_s = match t.side {
                TradeSide::Buy => "BUY",
                TradeSide::Sell => "SELL",
            };
            sqlx::query(
                r#"
                INSERT INTO wallet_trade_pnl (
                    cache_key, trade_index, wallet, condition_id, slug, side,
                    size, price, cash_flow, pnl, timestamp
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                "#,
            )
            .bind(cache_key)
            .bind(i as i32)
            .bind(&w)
            .bind(&t.condition_id)
            .bind(&t.slug)
            .bind(side_s)
            .bind(t.size)
            .bind(t.price)
            .bind(cash_flow)
            .bind(pnl)
            .bind(t.timestamp)
            .execute(&mut *tx)
            .await
            .context("insert wallet_trade_pnl row")?;
        }
        tx.commit().await.context("commit trade_pnl")?;
        Ok(())
    }

    /// Upsert leaderboard row for homepage ranking (`ORDER BY net_pnl DESC`).
    pub async fn upsert_leaderboard_row(
        &self,
        wallet: &str,
        cache_key: &str,
        report: &AnalyzeReport,
    ) -> anyhow::Result<()> {
        let w = wallet.to_lowercase();
        sqlx::query(
            r#"
            INSERT INTO wallet_leaderboard_stats (wallet, cache_key, net_pnl, total_volume, trades_count, updated_at)
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (wallet) DO UPDATE SET
                cache_key = EXCLUDED.cache_key,
                net_pnl = EXCLUDED.net_pnl,
                total_volume = EXCLUDED.total_volume,
                trades_count = EXCLUDED.trades_count,
                updated_at = NOW()
            "#,
        )
        .bind(&w)
        .bind(cache_key)
        .bind(report.lifetime.net_pnl)
        .bind(report.total_volume)
        .bind(report.trades_count as i64)
        .execute(&self.pool)
        .await
        .context("upsert wallet_leaderboard_stats")?;
        Ok(())
    }

    pub async fn fetch_leaderboard(&self, limit: i64) -> anyhow::Result<Vec<serde_json::Value>> {
        let rows = sqlx::query(
            r#"
            SELECT wallet, net_pnl, total_volume, trades_count, updated_at
            FROM wallet_leaderboard_stats
            ORDER BY net_pnl DESC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .context("fetch leaderboard")?;

        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let updated: DateTime<Utc> = r.try_get::<DateTime<Utc>, _>("updated_at")?;
            out.push(serde_json::json!({
                "wallet": r.try_get::<String, _>("wallet")?,
                "net_pnl": r.try_get::<f64, _>("net_pnl")?,
                "total_volume": r.try_get::<f64, _>("total_volume")?,
                "trades_count": r.try_get::<i64, _>("trades_count")?,
                "updated_at": updated.to_rfc3339(),
            }));
        }
        Ok(out)
    }

    /// Rank wallets by **sum of per-trade realized PnL** in `wallet_trade_pnl` since `cutoff_ms`
    /// (trade `timestamp` normalized to milliseconds), scoped to rows whose `cache_key` matches
    /// the current row in `wallet_leaderboard_stats` (avoids stale rows from superseded cache keys).
    ///
    /// Windows: use UTC calendar start-of-day for “today”, or rolling N days for week/month in the API layer.
    pub async fn fetch_leaderboard_since_trade_ts(
        &self,
        limit: i64,
        cutoff_ms: i64,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let rows = sqlx::query(
            r#"
            SELECT w.wallet AS wallet,
                   COALESCE(SUM(w.pnl), 0)::double precision AS net_pnl,
                   COALESCE(SUM(ABS(w.cash_flow)), 0)::double precision AS total_volume,
                   COUNT(*)::bigint AS trades_count,
                   MAX(s.updated_at) AS updated_at
            FROM wallet_trade_pnl w
            INNER JOIN wallet_leaderboard_stats s
              ON LOWER(s.wallet) = LOWER(w.wallet) AND s.cache_key = w.cache_key
            WHERE (CASE WHEN w.timestamp > 1000000000000 THEN w.timestamp ELSE w.timestamp * 1000 END) >= $1
            GROUP BY w.wallet
            ORDER BY net_pnl DESC
            LIMIT $2
            "#,
        )
        .bind(cutoff_ms)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .context("fetch leaderboard (time window)")?;

        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let updated: DateTime<Utc> = r.try_get::<DateTime<Utc>, _>("updated_at")?;
            out.push(serde_json::json!({
                "wallet": r.try_get::<String, _>("wallet")?,
                "net_pnl": r.try_get::<f64, _>("net_pnl")?,
                "total_volume": r.try_get::<f64, _>("total_volume")?,
                "trades_count": r.try_get::<i64, _>("trades_count")?,
                "updated_at": updated.to_rfc3339(),
            }));
        }
        Ok(out)
    }

    /// Export ambiguous reconciliation rows for a given `ingestion_run.id` (§1.5 / P2).
    pub async fn fetch_ambiguous_queue_by_run(
        &self,
        run_id: &str,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let rows = sqlx::query(
            r#"
            SELECT trade_key, candidates, reason, created_at, review_status, updated_at
            FROM reconciliation_ambiguous_queue
            WHERE run_id = $1
            ORDER BY id
            "#,
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
        .context("fetch ambiguous queue")?;

        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let created: DateTime<Utc> = r.try_get("created_at")?;
            let updated: DateTime<Utc> = r.try_get("updated_at")?;
            out.push(serde_json::json!({
                "trade_key": r.try_get::<String, _>("trade_key")?,
                "candidates": r.try_get::<serde_json::Value, _>("candidates")?,
                "reason": r.try_get::<String, _>("reason")?,
                "created_at": created.to_rfc3339(),
                "review_status": r.try_get::<String, _>("review_status")?,
                "updated_at": updated.to_rfc3339(),
            }));
        }
        Ok(out)
    }

    /// Human review state machine: `pending` | `reviewed` | `dismissed` | `escalated`.
    pub async fn update_ambiguous_review_status(
        &self,
        run_id: &str,
        trade_key: &str,
        review_status: &str,
    ) -> anyhow::Result<u64> {
        let allowed = ["pending", "reviewed", "dismissed", "escalated"];
        if !allowed.contains(&review_status) {
            anyhow::bail!(
                "review_status must be one of: {}; got {:?}",
                allowed.join(", "),
                review_status
            );
        }
        let res = sqlx::query(
            r#"
            UPDATE reconciliation_ambiguous_queue
            SET review_status = $3, updated_at = NOW()
            WHERE run_id = $1 AND trade_key = $2
            "#,
        )
        .bind(run_id)
        .bind(trade_key)
        .bind(review_status)
        .execute(&self.pool)
        .await
        .context("update ambiguous review_status")?;
        Ok(res.rows_affected())
    }

    /// Wallet recorded for this ingestion run (`ingestion_run.id` text = UUID string).
    pub async fn fetch_wallet_for_run(&self, run_id: &str) -> anyhow::Result<Option<String>> {
        let row = sqlx::query(
            r#"
            SELECT wallet FROM ingestion_run WHERE id = $1
            "#,
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
        .context("fetch_wallet_for_run")?;
        let Some(row) = row else {
            return Ok(None);
        };
        Ok(Some(row.try_get("wallet")?))
    }

    /// Load persisted canonical events for replay (`ORDER BY occurred_at, id`).
    pub async fn fetch_canonical_events_by_run(
        &self,
        run_id: &str,
    ) -> anyhow::Result<Vec<CanonicalEventInsert>> {
        let rows = sqlx::query(
            r#"
            SELECT id, event_type, occurred_at, condition_id, market_slug, amounts, source_refs
            FROM canonical_events
            WHERE run_id = $1
            ORDER BY occurred_at ASC, id ASC
            "#,
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
        .context("fetch_canonical_events_by_run")?;

        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(CanonicalEventInsert {
                id: r.try_get("id")?,
                event_type: r.try_get("event_type")?,
                occurred_at: r.try_get("occurred_at")?,
                condition_id: r.try_get("condition_id")?,
                market_slug: r.try_get("market_slug")?,
                amounts: r.try_get("amounts")?,
                source_refs: r.try_get("source_refs")?,
            });
        }
        Ok(out)
    }

    /// Latest `reconciliation_report.report` JSON for this run (if canonical persist ran).
    pub async fn fetch_reconciliation_v1_by_run(
        &self,
        run_id: &str,
    ) -> anyhow::Result<Option<crate::canonical::ReconciliationReportV1>> {
        let row = sqlx::query(
            r#"
            SELECT report
            FROM reconciliation_report
            WHERE run_id = $1
            ORDER BY id DESC
            LIMIT 1
            "#,
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
        .context("fetch_reconciliation_v1_by_run")?;
        let Some(row) = row else {
            return Ok(None);
        };
        let v: serde_json::Value = row.try_get("report")?;
        let rep: crate::canonical::ReconciliationReportV1 = serde_json::from_value(v)
            .context("deserialize reconciliation_report JSON")?;
        Ok(Some(rep))
    }

    pub async fn create_ingestion_run(
        &self,
        wallet: &str,
        params: &serde_json::Value,
    ) -> anyhow::Result<Uuid> {
        let id = Uuid::new_v4();
        let id_s = id.to_string();
        sqlx::query(
            r#"
            INSERT INTO ingestion_run (id, wallet, params, status)
            VALUES ($1, $2, $3, 'running')
            "#,
        )
        .bind(&id_s)
        .bind(wallet)
        .bind(params)
        .execute(&self.pool)
        .await
        .context("insert ingestion_run")?;
        Ok(id)
    }

    pub async fn finish_ingestion_run(&self, run_id: &Uuid, status: &str) -> anyhow::Result<()> {
        let id_s = run_id.to_string();
        sqlx::query(
            r#"
            UPDATE ingestion_run
            SET status = $2, finished_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(&id_s)
        .bind(status)
        .execute(&self.pool)
        .await
        .context("finish ingestion_run")?;
        Ok(())
    }

    pub async fn insert_raw_chunk(
        &self,
        run_id: &Uuid,
        wallet: &str,
        source: &str,
        chunk_index: i32,
        payload: &serde_json::Value,
    ) -> anyhow::Result<()> {
        let id_s = run_id.to_string();
        sqlx::query(
            r#"
            INSERT INTO raw_ingestion_chunk (run_id, wallet, source, chunk_index, payload)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (run_id, source, chunk_index)
            DO UPDATE SET payload = EXCLUDED.payload
            "#,
        )
        .bind(&id_s)
        .bind(wallet)
        .bind(source)
        .bind(chunk_index)
        .bind(payload)
        .execute(&self.pool)
        .await
        .context("insert raw_ingestion_chunk")?;
        Ok(())
    }

    pub async fn insert_raw_data_api_trade(
        &self,
        run_id: &Uuid,
        wallet: &str,
        page_offset: i32,
        row_index: i32,
        payload: &serde_json::Value,
    ) -> anyhow::Result<()> {
        let id_s = run_id.to_string();
        sqlx::query(
            r#"
            INSERT INTO raw_data_api_trades (run_id, wallet, page_offset, row_index, payload)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (run_id, page_offset, row_index)
            DO UPDATE SET payload = EXCLUDED.payload
            "#,
        )
        .bind(&id_s)
        .bind(wallet)
        .bind(page_offset)
        .bind(row_index)
        .bind(payload)
        .execute(&self.pool)
        .await
        .context("insert raw_data_api_trades")?;
        Ok(())
    }

    pub async fn insert_raw_data_api_open_position(
        &self,
        run_id: &Uuid,
        wallet: &str,
        row_index: i32,
        payload: &serde_json::Value,
    ) -> anyhow::Result<()> {
        let id_s = run_id.to_string();
        sqlx::query(
            r#"
            INSERT INTO raw_data_api_open_positions (run_id, wallet, row_index, payload)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (run_id, row_index)
            DO UPDATE SET payload = EXCLUDED.payload
            "#,
        )
        .bind(&id_s)
        .bind(wallet)
        .bind(row_index)
        .bind(payload)
        .execute(&self.pool)
        .await
        .context("insert raw_data_api_open_positions")?;
        Ok(())
    }

    pub async fn insert_raw_data_api_closed_position(
        &self,
        run_id: &Uuid,
        wallet: &str,
        row_index: i32,
        payload: &serde_json::Value,
    ) -> anyhow::Result<()> {
        let id_s = run_id.to_string();
        sqlx::query(
            r#"
            INSERT INTO raw_data_api_closed_positions (run_id, wallet, row_index, payload)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (run_id, row_index)
            DO UPDATE SET payload = EXCLUDED.payload
            "#,
        )
        .bind(&id_s)
        .bind(wallet)
        .bind(row_index)
        .bind(payload)
        .execute(&self.pool)
        .await
        .context("insert raw_data_api_closed_positions")?;
        Ok(())
    }

    pub async fn insert_raw_subgraph_redemption(
        &self,
        run_id: &Uuid,
        wallet: &str,
        event_id: &str,
        occurred_at: Option<DateTime<Utc>>,
        payload: &serde_json::Value,
    ) -> anyhow::Result<()> {
        let id_s = run_id.to_string();
        sqlx::query(
            r#"
            INSERT INTO raw_subgraph_redemptions (run_id, wallet, event_id, occurred_at, payload)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (run_id, event_id)
            DO UPDATE SET payload = EXCLUDED.payload, occurred_at = EXCLUDED.occurred_at
            "#,
        )
        .bind(&id_s)
        .bind(wallet)
        .bind(event_id)
        .bind(occurred_at)
        .bind(payload)
        .execute(&self.pool)
        .await
        .context("insert raw_subgraph_redemptions")?;
        Ok(())
    }

    pub async fn insert_raw_subgraph_order_filled(
        &self,
        run_id: &Uuid,
        wallet: &str,
        side_role: &str,
        event_id: &str,
        occurred_at: Option<DateTime<Utc>>,
        condition_id: Option<&str>,
        payload: &serde_json::Value,
    ) -> anyhow::Result<()> {
        let id_s = run_id.to_string();
        sqlx::query(
            r#"
            INSERT INTO raw_subgraph_order_filled
                (run_id, wallet, side_role, event_id, occurred_at, condition_id, payload)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (run_id, event_id)
            DO UPDATE SET
                payload = EXCLUDED.payload,
                occurred_at = EXCLUDED.occurred_at,
                condition_id = EXCLUDED.condition_id,
                side_role = EXCLUDED.side_role
            "#,
        )
        .bind(&id_s)
        .bind(wallet)
        .bind(side_role)
        .bind(event_id)
        .bind(occurred_at)
        .bind(condition_id)
        .bind(payload)
        .execute(&self.pool)
        .await
        .context("insert raw_subgraph_order_filled")?;
        Ok(())
    }

    pub async fn insert_raw_subgraph_user_position(
        &self,
        run_id: &Uuid,
        wallet: &str,
        position_id: &str,
        payload: &serde_json::Value,
    ) -> anyhow::Result<()> {
        let id_s = run_id.to_string();
        sqlx::query(
            r#"
            INSERT INTO raw_subgraph_user_positions (run_id, wallet, position_id, payload)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (run_id, position_id)
            DO UPDATE SET payload = EXCLUDED.payload
            "#,
        )
        .bind(&id_s)
        .bind(wallet)
        .bind(position_id)
        .bind(payload)
        .execute(&self.pool)
        .await
        .context("insert raw_subgraph_user_positions")?;
        Ok(())
    }

    pub async fn upsert_market_dim(
        &self,
        slug: &str,
        title: Option<&str>,
        end_date: Option<DateTime<Utc>>,
        closed_time: Option<DateTime<Utc>>,
        raw_gamma: &serde_json::Value,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO markets_dim (slug, title, end_date, closed_time, raw_gamma, updated_at)
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (slug)
            DO UPDATE SET
                title = COALESCE(EXCLUDED.title, markets_dim.title),
                end_date = COALESCE(EXCLUDED.end_date, markets_dim.end_date),
                closed_time = COALESCE(EXCLUDED.closed_time, markets_dim.closed_time),
                raw_gamma = EXCLUDED.raw_gamma,
                updated_at = NOW()
            "#,
        )
        .bind(slug)
        .bind(title)
        .bind(end_date)
        .bind(closed_time)
        .bind(raw_gamma)
        .execute(&self.pool)
        .await
        .context("upsert markets_dim")?;
        Ok(())
    }

    /// Persist canonical events, source map, ambiguous rows, and reconciliation JSON for one run.
    pub async fn persist_canonical_merge(
        &self,
        run_id: &Uuid,
        wallet: &str,
        artifacts: &CanonicalMergeArtifacts,
    ) -> anyhow::Result<()> {
        let id_s = run_id.to_string();
        let mut tx = self.pool.begin().await?;

        for e in &artifacts.events {
            sqlx::query(
                r#"
                INSERT INTO canonical_events
                    (id, run_id, wallet, event_type, occurred_at, condition_id, market_slug, amounts, source_refs)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                "#,
            )
            .bind(e.id)
            .bind(&id_s)
            .bind(wallet)
            .bind(&e.event_type)
            .bind(e.occurred_at)
            .bind(&e.condition_id)
            .bind(&e.market_slug)
            .bind(&e.amounts)
            .bind(&e.source_refs)
            .execute(&mut *tx)
            .await
            .context("insert canonical_events")?;
        }

        for m in &artifacts.source_maps {
            sqlx::query(
                r#"
                INSERT INTO source_event_map (canonical_id, source, source_row_id, confidence)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (canonical_id, source, source_row_id) DO NOTHING
                "#,
            )
            .bind(m.canonical_id)
            .bind(&m.source)
            .bind(&m.source_row_id)
            .bind(&m.confidence)
            .execute(&mut *tx)
            .await
            .context("insert source_event_map")?;
        }

        for a in &artifacts.ambiguous {
            sqlx::query(
                r#"
                INSERT INTO reconciliation_ambiguous_queue
                    (run_id, wallet, trade_key, candidates, reason)
                VALUES ($1, $2, $3, $4, $5)
                "#,
            )
            .bind(&id_s)
            .bind(wallet)
            .bind(&a.trade_key)
            .bind(&a.candidates)
            .bind(&a.reason)
            .execute(&mut *tx)
            .await
            .context("insert reconciliation_ambiguous_queue")?;
        }

        let report_json = serde_json::to_value(&artifacts.report)?;
        sqlx::query(
            r#"
            INSERT INTO reconciliation_report (wallet, run_id, rules_version, report)
            VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(wallet)
        .bind(&id_s)
        .bind(&artifacts.report.rules_version)
        .bind(report_json)
        .execute(&mut *tx)
        .await
        .context("insert reconciliation_report")?;

        tx.commit().await?;
        Ok(())
    }

    fn trade_ts_ms(ts: i64) -> i64 {
        if ts > 1_000_000_000_000 {
            ts
        } else {
            ts.saturating_mul(1000)
        }
    }

    fn trade_occurred_at_utc(ts: i64) -> DateTime<Utc> {
        let sec = if ts > 1_000_000_000_000 {
            ts / 1000
        } else {
            ts
        };
        DateTime::from_timestamp(sec, 0).unwrap_or_else(Utc::now)
    }

    /// Replace wallet-scoped snapshots used for SQL/BI and staged downstream work (`raw → canonical → reconciliation → report`).
    /// **Deletes** prior rows for this wallet under `wallet_primary_trade_row` / `wallet_canonical_event_row`, then **upserts** meta + JSON tables.
    /// Uses `pg_advisory_xact_lock` per wallet to avoid concurrent `analyze` interleaving DELETE/INSERT (duplicate `(wallet,seq)`).
    /// Wallet keys are normalized to **lowercase**; deletes match `lower(wallet)` so mixed-case historical rows are cleared.
    pub async fn replace_wallet_pipeline_snapshots(
        &self,
        wallet: &str,
        meta: &WalletPipelineSnapshotMeta,
        trades: &[Trade],
        trade_pnls: &[f64],
        canonical_events: Option<&[CanonicalEventInsert]>,
        report: &AnalyzeReport,
    ) -> anyhow::Result<()> {
        if trades.len() != trade_pnls.len() {
            anyhow::bail!("trades / trade_pnls length mismatch for wallet pipeline snapshots");
        }

        let wallet_lc = wallet.to_lowercase();

        let mut tx = self.pool.begin().await?;

        // Serialize same-wallet snapshot replaces (concurrent analyze / cache-hit refresh).
        sqlx::query(r#"SELECT pg_advisory_xact_lock(914001, hashtext(lower($1::text)))"#)
            .bind(&wallet_lc)
            .execute(&mut *tx)
            .await
            .context("advisory lock replace_wallet_pipeline_snapshots")?;

        // Match any historical wallet casing; re-insert only lowercase keys.
        sqlx::query("DELETE FROM wallet_primary_trade_row WHERE lower(wallet) = lower($1::text)")
            .bind(&wallet_lc)
            .execute(&mut *tx)
            .await
            .context("delete wallet_primary_trade_row")?;

        for (seq_usize, t) in trades.iter().enumerate() {
            let seq = i32::try_from(seq_usize).context("trade seq overflow")?;
            let vol = t.size * t.price;
            let cash_flow = match t.side {
                TradeSide::Buy => -vol,
                TradeSide::Sell => vol,
            };
            let side_s = match t.side {
                TradeSide::Buy => "BUY",
                TradeSide::Sell => "SELL",
            };
            let pnl = trade_pnls.get(seq_usize).copied().unwrap_or(0.0);
            let ts_ms = Self::trade_ts_ms(t.timestamp);
            let occurred_at = Self::trade_occurred_at_utc(t.timestamp);
            let trade_key = trade_dedup_key(t);

            sqlx::query(
                r#"
                INSERT INTO wallet_primary_trade_row (
                    wallet, seq, lane, side, size, price, cash_flow, realized_pnl,
                    condition_id, slug, title, outcome, timestamp_ms, occurred_at,
                    transaction_hash, asset, event_slug, outcome_index, proxy_wallet, trade_key
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
                "#,
            )
            .bind(&wallet_lc)
            .bind(seq)
            .bind(&meta.analytics_lane)
            .bind(side_s)
            .bind(t.size)
            .bind(t.price)
            .bind(cash_flow)
            .bind(pnl)
            .bind(&t.condition_id)
            .bind(&t.slug)
            .bind(&t.title)
            .bind(&t.outcome)
            .bind(ts_ms)
            .bind(occurred_at)
            .bind(&t.transaction_hash)
            .bind(&t.asset)
            .bind(&t.event_slug)
            .bind(t.outcome_index)
            .bind(&t.proxy_wallet)
            .bind(&trade_key)
            .execute(&mut *tx)
            .await
            .context("insert wallet_primary_trade_row")?;
        }

        sqlx::query("DELETE FROM wallet_canonical_event_row WHERE lower(wallet) = lower($1::text)")
            .bind(&wallet_lc)
            .execute(&mut *tx)
            .await
            .context("delete wallet_canonical_event_row")?;

        if let Some(events) = canonical_events {
            for (seq, e) in events.iter().enumerate() {
                let seq = i32::try_from(seq).context("canonical seq overflow")?;
                sqlx::query(
                    r#"
                    INSERT INTO wallet_canonical_event_row (
                        wallet, seq, canonical_id, event_type, occurred_at,
                        condition_id, market_slug, amounts, source_refs
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    "#,
                )
                .bind(&wallet_lc)
                .bind(seq)
                .bind(e.id)
                .bind(&e.event_type)
                .bind(e.occurred_at)
                .bind(&e.condition_id)
                .bind(&e.market_slug)
                .bind(&e.amounts)
                .bind(&e.source_refs)
                .execute(&mut *tx)
                .await
                .context("insert wallet_canonical_event_row")?;
            }
        }

        let v0_json = report
            .reconciliation
            .as_ref()
            .map(|r| serde_json::to_value(r))
            .transpose()
            .context("serialize reconciliation v0")?;
        let v1_json = report
            .reconciliation_v1
            .as_ref()
            .map(|r| serde_json::to_value(r))
            .transpose()
            .context("serialize reconciliation v1")?;

        // One statement per execute: Postgres prepared statements cannot contain multiple commands.
        for (sql, ctx) in [
            (
                "DELETE FROM wallet_pipeline_meta WHERE lower(wallet) = lower($1::text)",
                "delete wallet_pipeline_meta (casing dedup)",
            ),
            (
                "DELETE FROM wallet_reconciliation_current WHERE lower(wallet) = lower($1::text)",
                "delete wallet_reconciliation_current (casing dedup)",
            ),
            (
                "DELETE FROM wallet_frontend_current WHERE lower(wallet) = lower($1::text)",
                "delete wallet_frontend_current (casing dedup)",
            ),
            (
                "DELETE FROM wallet_strategy_current WHERE lower(wallet) = lower($1::text)",
                "delete wallet_strategy_current (casing dedup)",
            ),
            (
                "DELETE FROM wallet_report_snapshot WHERE lower(wallet) = lower($1::text)",
                "delete wallet_report_snapshot (casing dedup)",
            ),
        ] {
            sqlx::query(sql)
                .bind(&wallet_lc)
                .execute(&mut *tx)
                .await
                .context(ctx)?;
        }

        sqlx::query(
            r#"
            INSERT INTO wallet_pipeline_meta (
                wallet, last_ingestion_run_id, analytics_lane, cache_key, schema_version,
                data_api_truncated, data_api_max_offset_allowed, data_api_trade_watermark_ms, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
            ON CONFLICT (wallet) DO UPDATE SET
                last_ingestion_run_id = EXCLUDED.last_ingestion_run_id,
                analytics_lane = EXCLUDED.analytics_lane,
                cache_key = EXCLUDED.cache_key,
                schema_version = EXCLUDED.schema_version,
                data_api_truncated = EXCLUDED.data_api_truncated,
                data_api_max_offset_allowed = EXCLUDED.data_api_max_offset_allowed,
                data_api_trade_watermark_ms = EXCLUDED.data_api_trade_watermark_ms,
                updated_at = NOW()
            "#,
        )
        .bind(&wallet_lc)
        .bind(&meta.last_ingestion_run_id)
        .bind(&meta.analytics_lane)
        .bind(&meta.cache_key)
        .bind(&meta.schema_version)
        .bind(meta.data_api_truncated)
        .bind(meta.data_api_max_offset_allowed)
        .bind(meta.data_api_trade_watermark_ms)
        .execute(&mut *tx)
        .await
        .context("upsert wallet_pipeline_meta")?;

        sqlx::query(
            r#"
            INSERT INTO wallet_reconciliation_current (wallet, reconciliation_v0, reconciliation_v1, last_run_id, updated_at)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (wallet) DO UPDATE SET
                reconciliation_v0 = EXCLUDED.reconciliation_v0,
                reconciliation_v1 = EXCLUDED.reconciliation_v1,
                last_run_id = EXCLUDED.last_run_id,
                updated_at = NOW()
            "#,
        )
        .bind(&wallet_lc)
        .bind(v0_json)
        .bind(v1_json)
        .bind(&meta.last_ingestion_run_id)
        .execute(&mut *tx)
        .await
        .context("upsert wallet_reconciliation_current")?;

        let frontend_json = report
            .frontend
            .as_ref()
            .map(|f| serde_json::to_value(f))
            .transpose()
            .context("serialize frontend")?;

        sqlx::query(
            r#"
            INSERT INTO wallet_frontend_current (wallet, payload, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (wallet) DO UPDATE SET
                payload = EXCLUDED.payload,
                updated_at = NOW()
            "#,
        )
        .bind(&wallet_lc)
        .bind(frontend_json)
        .execute(&mut *tx)
        .await
        .context("upsert wallet_frontend_current")?;

        let strategy_json =
            serde_json::to_value(&report.strategy_inference).context("serialize strategy")?;

        sqlx::query(
            r#"
            INSERT INTO wallet_strategy_current (wallet, payload, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (wallet) DO UPDATE SET
                payload = EXCLUDED.payload,
                updated_at = NOW()
            "#,
        )
        .bind(&wallet_lc)
        .bind(strategy_json)
        .execute(&mut *tx)
        .await
        .context("upsert wallet_strategy_current")?;

        let report_json = serde_json::to_value(report).context("serialize full report")?;

        sqlx::query(
            r#"
            INSERT INTO wallet_report_snapshot (wallet, cache_key, report_json, updated_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (wallet) DO UPDATE SET
                cache_key = EXCLUDED.cache_key,
                report_json = EXCLUDED.report_json,
                updated_at = NOW()
            "#,
        )
        .bind(&wallet_lc)
        .bind(&meta.cache_key)
        .bind(report_json)
        .execute(&mut *tx)
        .await
        .context("upsert wallet_report_snapshot")?;

        tx.commit().await?;
        Ok(())
    }
}
