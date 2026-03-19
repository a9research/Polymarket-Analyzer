use crate::report::AnalyzeReport;
use anyhow::Context;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::time::Duration;

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
        Ok(())
    }

    pub async fn get_cached_report(
        &self,
        wallet: &str,
        ttl_seconds: i64,
    ) -> anyhow::Result<Option<AnalyzeReport>> {
        let row = sqlx::query(
            r#"
            SELECT report_json
            FROM analyze_report_cache
            WHERE wallet = $1
              AND updated_at > NOW() - ($2::bigint * INTERVAL '1 second')
            "#,
        )
        .bind(wallet)
        .bind(ttl_seconds)
        .fetch_optional(&self.pool)
        .await
        .context("query cached report")?;

        let Some(row) = row else {
            return Ok(None);
        };

        let value: serde_json::Value = row.try_get("report_json")?;
        let report: AnalyzeReport = serde_json::from_value(value)?;
        Ok(Some(report))
    }

    pub async fn upsert_report(&self, wallet: &str, report: &AnalyzeReport) -> anyhow::Result<()> {
        let report_json = serde_json::to_value(report)?;
        sqlx::query(
            r#"
            INSERT INTO analyze_report_cache(wallet, report_json, updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (wallet)
            DO UPDATE SET report_json = EXCLUDED.report_json, updated_at = NOW()
            "#,
        )
        .bind(wallet)
        .bind(report_json)
        .execute(&self.pool)
        .await
        .context("upsert report cache")?;
        Ok(())
    }
}

