use crate::canonical::{CanonicalEventInsert, CanonicalMergeArtifacts};
use crate::report::AnalyzeReport;
use anyhow::Context;
use chrono::{DateTime, Utc};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::time::Duration;
use uuid::Uuid;

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
}
