use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use insight_durable::{McpInteractionPrincipal, McpServerTask, McpServerTaskDurableRepository};
use sqlx::Row;

use super::{
    PostgresDurableRepository, RepositoryError, RepositoryErrorExt as _, SqliteDurableRepository,
};

const MAX_EXPIRY_BATCH: u32 = 1_024;

fn sqlite_time(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn parse_sqlite_time(value: String) -> Result<DateTime<Utc>, RepositoryError> {
    DateTime::parse_from_rfc3339(&value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| RepositoryError::invalid_data())
}

fn parse_sqlite(row: sqlx::sqlite::SqliteRow) -> Result<McpServerTask, RepositoryError> {
    McpServerTask::new(
        row.try_get::<String, _>("task_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        McpInteractionPrincipal::new(
            row.try_get::<String, _>("tenant_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get::<String, _>("user_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        row.try_get::<String, _>("run_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get::<String, _>("agent_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        parse_sqlite_time(
            row.try_get::<String, _>("created_at")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        parse_sqlite_time(
            row.try_get::<String, _>("expires_at")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
    )
}

fn parse_postgres(row: sqlx::postgres::PgRow) -> Result<McpServerTask, RepositoryError> {
    McpServerTask::new(
        row.try_get::<String, _>("task_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        McpInteractionPrincipal::new(
            row.try_get::<String, _>("tenant_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get::<String, _>("user_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        row.try_get::<String, _>("run_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get::<String, _>("agent_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("created_at")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("expires_at")
            .map_err(|_| RepositoryError::invalid_data())?,
    )
}

#[async_trait]
impl McpServerTaskDurableRepository for SqliteDurableRepository {
    async fn create_mcp_server_task(&self, task: McpServerTask) -> Result<bool, RepositoryError> {
        let result = sqlx::query(
            "INSERT INTO mcp_server_tasks(task_id,tenant_id,user_id,run_id,agent_id,created_at,expires_at)
             VALUES(?,?,?,?,?,?,?)
             ON CONFLICT(task_id) DO NOTHING",
        )
        .bind(task.task_id())
        .bind(task.principal().tenant_id())
        .bind(task.principal().user_id())
        .bind(task.run_id())
        .bind(task.agent_id())
        .bind(sqlite_time(task.created_at()))
        .bind(sqlite_time(task.expires_at()))
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        Ok(result.rows_affected() == 1)
    }

    async fn load_mcp_server_task(
        &self,
        principal: &McpInteractionPrincipal,
        task_id: &str,
    ) -> Result<Option<McpServerTask>, RepositoryError> {
        sqlx::query(
            "SELECT task_id,tenant_id,user_id,run_id,agent_id,created_at,expires_at
             FROM mcp_server_tasks
             WHERE task_id=? AND tenant_id=? AND user_id=?",
        )
        .bind(task_id)
        .bind(principal.tenant_id())
        .bind(principal.user_id())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .map(parse_sqlite)
        .transpose()
    }

    async fn list_expired_mcp_server_tasks(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<McpServerTask>, RepositoryError> {
        if limit == 0 || limit > MAX_EXPIRY_BATCH {
            return Err(RepositoryError::invalid_data());
        }
        sqlx::query(
            "SELECT task_id,tenant_id,user_id,run_id,agent_id,created_at,expires_at
             FROM mcp_server_tasks
             WHERE expires_at<=?
             ORDER BY expires_at,task_id
             LIMIT ?",
        )
        .bind(sqlite_time(now))
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .into_iter()
        .map(parse_sqlite)
        .collect()
    }

    async fn delete_expired_mcp_server_task(
        &self,
        task_id: &str,
        expected_expires_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        let result = sqlx::query(
            "DELETE FROM mcp_server_tasks
             WHERE task_id=? AND expires_at=? AND expires_at<=?",
        )
        .bind(task_id)
        .bind(sqlite_time(expected_expires_at))
        .bind(sqlite_time(now))
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        Ok(result.rows_affected() == 1)
    }
}

#[async_trait]
impl McpServerTaskDurableRepository for PostgresDurableRepository {
    async fn create_mcp_server_task(&self, task: McpServerTask) -> Result<bool, RepositoryError> {
        let result = sqlx::query(
            "INSERT INTO mcp_server_tasks(task_id,tenant_id,user_id,run_id,agent_id,created_at,expires_at)
             VALUES($1,$2,$3,$4,$5,$6,$7)
             ON CONFLICT(task_id) DO NOTHING",
        )
        .bind(task.task_id())
        .bind(task.principal().tenant_id())
        .bind(task.principal().user_id())
        .bind(task.run_id())
        .bind(task.agent_id())
        .bind(task.created_at())
        .bind(task.expires_at())
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        Ok(result.rows_affected() == 1)
    }

    async fn load_mcp_server_task(
        &self,
        principal: &McpInteractionPrincipal,
        task_id: &str,
    ) -> Result<Option<McpServerTask>, RepositoryError> {
        sqlx::query(
            "SELECT task_id,tenant_id,user_id,run_id,agent_id,created_at,expires_at
             FROM mcp_server_tasks
             WHERE task_id=$1 AND tenant_id=$2 AND user_id=$3",
        )
        .bind(task_id)
        .bind(principal.tenant_id())
        .bind(principal.user_id())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .map(parse_postgres)
        .transpose()
    }

    async fn list_expired_mcp_server_tasks(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<McpServerTask>, RepositoryError> {
        if limit == 0 || limit > MAX_EXPIRY_BATCH {
            return Err(RepositoryError::invalid_data());
        }
        sqlx::query(
            "SELECT task_id,tenant_id,user_id,run_id,agent_id,created_at,expires_at
             FROM mcp_server_tasks
             WHERE expires_at<=$1
             ORDER BY expires_at,task_id
             LIMIT $2",
        )
        .bind(now)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .into_iter()
        .map(parse_postgres)
        .collect()
    }

    async fn delete_expired_mcp_server_task(
        &self,
        task_id: &str,
        expected_expires_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        let result = sqlx::query(
            "DELETE FROM mcp_server_tasks
             WHERE task_id=$1 AND expires_at=$2 AND expires_at<=$3",
        )
        .bind(task_id)
        .bind(expected_expires_at)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        Ok(result.rows_affected() == 1)
    }
}
