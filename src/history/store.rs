use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::{
    migrate::{MigrateError, Migrator},
    postgres::PgPoolOptions,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    AssertSqlSafe, PgPool, Row, SqlitePool,
};

use crate::{
    config::{HistoryConfig, HistoryProvider},
    engine::event::RunEvent,
    error::AppError,
    request_context::RequestContext,
};

static SQLITE_MIGRATOR: Migrator = sqlx::migrate!("migrations/sqlite");
static POSTGRES_MIGRATOR: Migrator = sqlx::migrate!("migrations/postgres");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl RunStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    fn from_str(value: &str) -> Self {
        match value {
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            _ => Self::Running,
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RunSummary {
    pub run_id: String,
    pub request_id: String,
    pub agent_id: String,
    pub caller_service: Option<String>,
    pub tenant_id: Option<String>,
    pub user_id: Option<String>,
    pub status: RunStatus,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub input_summary: Value,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunEventRecord {
    #[serde(rename = "type")]
    pub event_type: String,
    pub seq: u64,
    pub request_id: String,
    pub run_id: String,
    pub agent_id: String,
    #[serde(rename = "time")]
    pub timestamp: DateTime<Utc>,
    pub code: i32,
    pub message: String,
    pub data: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunRecord {
    pub run_id: String,
    pub request_id: String,
    pub agent_id: String,
    pub caller_service: Option<String>,
    pub tenant_id: Option<String>,
    pub user_id: Option<String>,
    pub status: RunStatus,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub input_summary: Value,
    pub error_message: Option<String>,
    pub events: Vec<RunEventRecord>,
    pub step_outputs: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunHistoryPage {
    pub items: Vec<RunSummary>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct RunHistoryQuery {
    pub agent_id: Option<String>,
    pub request_id: Option<String>,
    pub caller_service: Option<String>,
    pub tenant_id: Option<String>,
    pub user_id: Option<String>,
    pub status: Option<RunStatus>,
    pub started_after: Option<DateTime<Utc>>,
    pub started_before: Option<DateTime<Utc>>,
    pub after: Option<String>,
    pub limit: usize,
}

#[derive(Clone)]
pub struct RunHistoryStore {
    inner: Arc<dyn RunHistoryRepository>,
}

impl RunHistoryStore {
    pub fn noop() -> Self {
        Self {
            inner: Arc::new(NoopRunHistoryRepository),
        }
    }

    pub async fn from_config(config: &HistoryConfig) -> Result<Self, AppError> {
        match config.provider {
            HistoryProvider::Sqlite => Self::sqlite_url(&config.database_url).await,
            HistoryProvider::Postgres => Self::postgres(&config.database_url).await,
        }
    }

    pub async fn sqlite(path: impl AsRef<Path>) -> Result<Self, AppError> {
        let path = path.as_ref().to_path_buf();
        Ok(Self {
            inner: Arc::new(SqlxRunHistoryRepository::sqlite_path(&path).await?),
        })
    }

    pub async fn sqlite_url(database_url: &str) -> Result<Self, AppError> {
        Ok(Self {
            inner: Arc::new(SqlxRunHistoryRepository::sqlite_url(database_url).await?),
        })
    }

    pub async fn sqlite_in_memory() -> Result<Self, AppError> {
        Ok(Self {
            inner: Arc::new(SqlxRunHistoryRepository::sqlite_in_memory().await?),
        })
    }

    pub async fn postgres(database_url: &str) -> Result<Self, AppError> {
        Ok(Self {
            inner: Arc::new(SqlxRunHistoryRepository::postgres(database_url).await?),
        })
    }

    pub async fn create_run(
        &self,
        run_id: &str,
        agent_id: &str,
        request: &RequestContext,
        started_at: DateTime<Utc>,
        input_summary: Value,
    ) {
        if let Err(err) = self
            .inner
            .create_run(run_id, agent_id, request, started_at, input_summary)
            .await
        {
            tracing::warn!(run_id, agent_id, error = %err, "failed to record run start");
        }
    }

    pub async fn record_event(&self, event: &RunEvent) {
        if let Err(err) = self.inner.record_event(event).await {
            tracing::warn!(run_id = %event.run_id, error = %err, "failed to record run event");
        }
    }

    pub async fn record_step_output(&self, run_id: &str, step_id: &str, output: Value) {
        if let Err(err) = self.inner.record_step_output(run_id, step_id, output).await {
            tracing::warn!(run_id, step_id, error = %err, "failed to record step output");
        }
    }

    pub async fn finish_run(&self, run_id: &str, status: RunStatus, error_message: Option<String>) {
        if let Err(err) = self.inner.finish_run(run_id, status, error_message).await {
            tracing::warn!(run_id, error = %err, "failed to record run finish");
        }
    }

    pub async fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>, AppError> {
        self.inner.get_run(run_id).await
    }

    pub async fn list_agent_runs(
        &self,
        agent_id: &str,
        limit: usize,
    ) -> Result<Vec<RunSummary>, AppError> {
        self.list_runs(RunHistoryQuery {
            agent_id: Some(agent_id.to_string()),
            limit,
            ..Default::default()
        })
        .await
    }

    pub async fn list_runs(&self, query: RunHistoryQuery) -> Result<Vec<RunSummary>, AppError> {
        Ok(self.list_runs_page(query).await?.items)
    }

    pub async fn list_runs_page(&self, query: RunHistoryQuery) -> Result<RunHistoryPage, AppError> {
        self.inner.list_runs_page(query).await
    }
}

impl Default for RunHistoryStore {
    fn default() -> Self {
        Self::noop()
    }
}

#[async_trait]
trait RunHistoryRepository: Send + Sync {
    async fn create_run(
        &self,
        run_id: &str,
        agent_id: &str,
        request: &RequestContext,
        started_at: DateTime<Utc>,
        input_summary: Value,
    ) -> Result<(), AppError>;
    async fn record_event(&self, event: &RunEvent) -> Result<(), AppError>;
    async fn record_step_output(
        &self,
        run_id: &str,
        step_id: &str,
        output: Value,
    ) -> Result<(), AppError>;
    async fn finish_run(
        &self,
        run_id: &str,
        status: RunStatus,
        error_message: Option<String>,
    ) -> Result<(), AppError>;
    async fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>, AppError>;
    async fn list_runs_page(&self, query: RunHistoryQuery) -> Result<RunHistoryPage, AppError>;
}

struct NoopRunHistoryRepository;

#[async_trait]
impl RunHistoryRepository for NoopRunHistoryRepository {
    async fn create_run(
        &self,
        _run_id: &str,
        _agent_id: &str,
        _request: &RequestContext,
        _started_at: DateTime<Utc>,
        _input_summary: Value,
    ) -> Result<(), AppError> {
        Ok(())
    }

    async fn record_event(&self, _event: &RunEvent) -> Result<(), AppError> {
        Ok(())
    }

    async fn record_step_output(
        &self,
        _run_id: &str,
        _step_id: &str,
        _output: Value,
    ) -> Result<(), AppError> {
        Ok(())
    }

    async fn finish_run(
        &self,
        _run_id: &str,
        _status: RunStatus,
        _error_message: Option<String>,
    ) -> Result<(), AppError> {
        Ok(())
    }

    async fn get_run(&self, _run_id: &str) -> Result<Option<RunRecord>, AppError> {
        Ok(None)
    }

    async fn list_runs_page(&self, _query: RunHistoryQuery) -> Result<RunHistoryPage, AppError> {
        Ok(RunHistoryPage {
            items: Vec::new(),
            next_cursor: None,
        })
    }
}

struct SqlxRunHistoryRepository {
    backend: SqlxRunHistoryBackend,
}

enum SqlxRunHistoryBackend {
    Sqlite(SqlitePool),
    Postgres(PgPool),
}

#[derive(Debug, Clone, Copy)]
enum SqlDialect {
    Sqlite,
    Postgres,
}

impl SqlDialect {
    fn placeholder(self, index: usize) -> String {
        match self {
            Self::Sqlite => "?".to_string(),
            Self::Postgres => format!("${index}"),
        }
    }
}

impl SqlxRunHistoryBackend {
    fn dialect(&self) -> SqlDialect {
        match self {
            Self::Sqlite(_) => SqlDialect::Sqlite,
            Self::Postgres(_) => SqlDialect::Postgres,
        }
    }
}

impl SqlxRunHistoryRepository {
    async fn sqlite_path(path: &Path) -> Result<Self, AppError> {
        create_parent_dir(path)?;
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);
        Self::connect_sqlite(options, 5).await
    }

    async fn sqlite_url(database_url: &str) -> Result<Self, AppError> {
        create_sqlite_parent_dir_from_url(database_url)?;
        let options = SqliteConnectOptions::from_str(database_url)
            .map_err(|err| AppError::Config(format!("invalid sqlite history database_url: {err}")))?
            .create_if_missing(true);
        let max_connections = sqlite_max_connections(database_url);
        Self::connect_sqlite(options, max_connections).await
    }

    async fn sqlite_in_memory() -> Result<Self, AppError> {
        let options = SqliteConnectOptions::new().in_memory(true);
        Self::connect_sqlite(options, 1).await
    }

    async fn connect_sqlite(
        options: SqliteConnectOptions,
        max_connections: u32,
    ) -> Result<Self, AppError> {
        let pool = SqlitePoolOptions::new()
            .max_connections(max_connections)
            .connect_with(options)
            .await
            .map_err(map_sqlx_error)?;
        prepare_sqlite_schema_for_migration(&pool).await?;
        SQLITE_MIGRATOR
            .run(&pool)
            .await
            .map_err(map_migrate_error)?;
        ensure_sqlite_legacy_columns(&pool).await?;
        tracing::info!("run history sqlite backend initialized");
        Ok(Self {
            backend: SqlxRunHistoryBackend::Sqlite(pool),
        })
    }

    async fn postgres(database_url: &str) -> Result<Self, AppError> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .map_err(map_sqlx_error)?;
        prepare_postgres_schema_for_migration(&pool).await?;
        POSTGRES_MIGRATOR
            .run(&pool)
            .await
            .map_err(map_migrate_error)?;
        ensure_postgres_legacy_columns(&pool).await?;
        tracing::info!("run history postgres backend initialized");
        Ok(Self {
            backend: SqlxRunHistoryBackend::Postgres(pool),
        })
    }
}

#[async_trait]
impl RunHistoryRepository for SqlxRunHistoryRepository {
    async fn create_run(
        &self,
        run_id: &str,
        agent_id: &str,
        request: &RequestContext,
        started_at: DateTime<Utc>,
        input_summary: Value,
    ) -> Result<(), AppError> {
        match &self.backend {
            SqlxRunHistoryBackend::Sqlite(pool) => {
                sqlx::query(
                    "INSERT INTO runs (run_id, request_id, agent_id, caller_service, tenant_id, user_id, status, started_at, ended_at, input_summary, error_message)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL)
                     ON CONFLICT(run_id) DO UPDATE SET
                        request_id = excluded.request_id,
                        agent_id = excluded.agent_id,
                        caller_service = excluded.caller_service,
                        tenant_id = excluded.tenant_id,
                        user_id = excluded.user_id,
                        status = excluded.status,
                        started_at = excluded.started_at,
                        ended_at = NULL,
                        input_summary = excluded.input_summary,
                        error_message = NULL",
                )
                .bind(run_id)
                .bind(&request.request_id)
                .bind(agent_id)
                .bind(&request.caller_service)
                .bind(&request.tenant_id)
                .bind(&request.user_id)
                .bind(RunStatus::Running.as_str())
                .bind(started_at.to_rfc3339())
                .bind(input_summary.to_string())
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;
            }
            SqlxRunHistoryBackend::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO runs (run_id, request_id, agent_id, caller_service, tenant_id, user_id, status, started_at, ended_at, input_summary, error_message)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL, $9, NULL)
                     ON CONFLICT(run_id) DO UPDATE SET
                        request_id = excluded.request_id,
                        agent_id = excluded.agent_id,
                        caller_service = excluded.caller_service,
                        tenant_id = excluded.tenant_id,
                        user_id = excluded.user_id,
                        status = excluded.status,
                        started_at = excluded.started_at,
                        ended_at = NULL,
                        input_summary = excluded.input_summary,
                        error_message = NULL",
                )
                .bind(run_id)
                .bind(&request.request_id)
                .bind(agent_id)
                .bind(&request.caller_service)
                .bind(&request.tenant_id)
                .bind(&request.user_id)
                .bind(RunStatus::Running.as_str())
                .bind(started_at.to_rfc3339())
                .bind(input_summary.to_string())
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;
            }
        }
        Ok(())
    }

    async fn record_event(&self, event: &RunEvent) -> Result<(), AppError> {
        let event_type = event.event_type.as_str();
        let timestamp = event.timestamp.to_rfc3339();
        let data = event.data.to_string();
        match &self.backend {
            SqlxRunHistoryBackend::Sqlite(pool) => {
                sqlx::query(
                    "INSERT INTO run_events (run_id, type, seq, timestamp, code, message, data)
                     VALUES (?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(&event.run_id)
                .bind(event_type)
                .bind(event.seq as i64)
                .bind(timestamp)
                .bind(event.code)
                .bind(&event.message)
                .bind(&data)
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;
            }
            SqlxRunHistoryBackend::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO run_events (run_id, type, seq, timestamp, code, message, data)
                     VALUES ($1, $2, $3, $4, $5, $6, $7)",
                )
                .bind(&event.run_id)
                .bind(event_type)
                .bind(event.seq as i64)
                .bind(timestamp)
                .bind(event.code)
                .bind(&event.message)
                .bind(&data)
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;
            }
        }
        Ok(())
    }

    async fn record_step_output(
        &self,
        run_id: &str,
        step_id: &str,
        output: Value,
    ) -> Result<(), AppError> {
        let output = output.to_string();
        let created_at = Utc::now().to_rfc3339();
        match &self.backend {
            SqlxRunHistoryBackend::Sqlite(pool) => {
                sqlx::query(
                    "INSERT INTO step_outputs (run_id, step_id, output, created_at)
                     VALUES (?, ?, ?, ?)
                     ON CONFLICT(run_id, step_id) DO UPDATE SET
                        output = excluded.output,
                        created_at = excluded.created_at",
                )
                .bind(run_id)
                .bind(step_id)
                .bind(output)
                .bind(created_at)
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;
            }
            SqlxRunHistoryBackend::Postgres(pool) => {
                sqlx::query(
                    "INSERT INTO step_outputs (run_id, step_id, output, created_at)
                     VALUES ($1, $2, $3, $4)
                     ON CONFLICT(run_id, step_id) DO UPDATE SET
                        output = excluded.output,
                        created_at = excluded.created_at",
                )
                .bind(run_id)
                .bind(step_id)
                .bind(output)
                .bind(created_at)
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;
            }
        }
        Ok(())
    }

    async fn finish_run(
        &self,
        run_id: &str,
        status: RunStatus,
        error_message: Option<String>,
    ) -> Result<(), AppError> {
        let ended_at = Utc::now().to_rfc3339();
        match &self.backend {
            SqlxRunHistoryBackend::Sqlite(pool) => {
                sqlx::query(
                    "UPDATE runs SET status = ?, ended_at = ?, error_message = ? WHERE run_id = ?",
                )
                .bind(status.as_str())
                .bind(&ended_at)
                .bind(&error_message)
                .bind(run_id)
                .execute(pool)
                .await
                .map_err(map_sqlx_error)?;
            }
            SqlxRunHistoryBackend::Postgres(pool) => {
                sqlx::query("UPDATE runs SET status = $2, ended_at = $3, error_message = $4 WHERE run_id = $1")
                    .bind(run_id)
                    .bind(status.as_str())
                    .bind(&ended_at)
                    .bind(&error_message)
                    .execute(pool)
                    .await
                    .map_err(map_sqlx_error)?;
            }
        }
        Ok(())
    }

    async fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>, AppError> {
        let summary = match &self.backend {
            SqlxRunHistoryBackend::Sqlite(pool) => {
                sqlx::query_as::<_, RunSummaryRow>(
                    "SELECT run_id, request_id, agent_id, caller_service, tenant_id, user_id, status, started_at, ended_at, input_summary, error_message
                     FROM runs WHERE run_id = ?",
                )
                .bind(run_id)
                .fetch_optional(pool)
                .await
                .map_err(map_sqlx_error)?
            }
            SqlxRunHistoryBackend::Postgres(pool) => {
                sqlx::query_as::<_, RunSummaryRow>(
                    "SELECT run_id, request_id, agent_id, caller_service, tenant_id, user_id, status, started_at, ended_at, input_summary, error_message
                     FROM runs WHERE run_id = $1",
                )
                .bind(run_id)
                .fetch_optional(pool)
                .await
                .map_err(map_sqlx_error)?
            }
        };

        let Some(summary) = summary else {
            return Ok(None);
        };

        let events = match &self.backend {
            SqlxRunHistoryBackend::Sqlite(pool) => sqlx::query_as::<_, RunEventRow>(
                "SELECT type, seq, timestamp, code, message, data
                     FROM run_events WHERE run_id = ? ORDER BY seq ASC, id ASC",
            )
            .bind(run_id)
            .fetch_all(pool)
            .await
            .map_err(map_sqlx_error)?,
            SqlxRunHistoryBackend::Postgres(pool) => sqlx::query_as::<_, RunEventRow>(
                "SELECT type, seq, timestamp, code, message, data
                     FROM run_events WHERE run_id = $1 ORDER BY seq ASC, id ASC",
            )
            .bind(run_id)
            .fetch_all(pool)
            .await
            .map_err(map_sqlx_error)?,
        }
        .into_iter()
        .map(|row| row.into_record(&summary))
        .collect::<Vec<_>>();

        let step_outputs = match &self.backend {
            SqlxRunHistoryBackend::Sqlite(pool) => sqlx::query_as::<_, StepOutputRow>(
                "SELECT step_id, output FROM step_outputs WHERE run_id = ? ORDER BY step_id ASC",
            )
            .bind(run_id)
            .fetch_all(pool)
            .await
            .map_err(map_sqlx_error)?,
            SqlxRunHistoryBackend::Postgres(pool) => sqlx::query_as::<_, StepOutputRow>(
                "SELECT step_id, output FROM step_outputs WHERE run_id = $1 ORDER BY step_id ASC",
            )
            .bind(run_id)
            .fetch_all(pool)
            .await
            .map_err(map_sqlx_error)?,
        }
        .into_iter()
        .map(|row| (row.step_id, parse_json_or_null(&row.output)))
        .collect::<BTreeMap<_, _>>();

        Ok(Some(RunRecord {
            run_id: summary.run_id,
            request_id: summary.request_id,
            agent_id: summary.agent_id,
            caller_service: summary.caller_service,
            tenant_id: summary.tenant_id,
            user_id: summary.user_id,
            status: RunStatus::from_str(&summary.status),
            started_at: parse_datetime_or_now(&summary.started_at),
            ended_at: summary.ended_at.as_deref().map(parse_datetime_or_now),
            input_summary: parse_json_or_null(&summary.input_summary),
            error_message: summary.error_message,
            events,
            step_outputs,
        }))
    }

    async fn list_runs_page(&self, query: RunHistoryQuery) -> Result<RunHistoryPage, AppError> {
        let query = build_list_runs_query(query, self.backend.dialect())?;
        match &self.backend {
            SqlxRunHistoryBackend::Sqlite(pool) => {
                let mut db_query = sqlx::query_as::<_, RunSummaryRow>(AssertSqlSafe(query.sql));
                for param in query.text_params {
                    db_query = db_query.bind(param);
                }
                db_query = db_query.bind(query.limit);
                db_query
                    .fetch_all(pool)
                    .await
                    .map_err(map_sqlx_error)
                    .map(|rows| run_summary_page(rows, query.returned_limit))
            }
            SqlxRunHistoryBackend::Postgres(pool) => {
                let mut db_query = sqlx::query_as::<_, RunSummaryRow>(AssertSqlSafe(query.sql));
                for param in query.text_params {
                    db_query = db_query.bind(param);
                }
                db_query = db_query.bind(query.limit);
                db_query
                    .fetch_all(pool)
                    .await
                    .map_err(map_sqlx_error)
                    .map(|rows| run_summary_page(rows, query.returned_limit))
            }
        }
    }
}

#[derive(Debug, sqlx::FromRow)]
struct RunSummaryRow {
    run_id: String,
    request_id: String,
    agent_id: String,
    caller_service: Option<String>,
    tenant_id: Option<String>,
    user_id: Option<String>,
    status: String,
    started_at: String,
    ended_at: Option<String>,
    input_summary: String,
    error_message: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
struct RunEventRow {
    #[sqlx(rename = "type")]
    event_type: String,
    seq: i64,
    timestamp: String,
    code: i32,
    message: String,
    data: String,
}

#[derive(Debug, sqlx::FromRow)]
struct StepOutputRow {
    step_id: String,
    output: String,
}

impl From<RunSummaryRow> for RunSummary {
    fn from(row: RunSummaryRow) -> Self {
        Self {
            run_id: row.run_id,
            request_id: row.request_id,
            agent_id: row.agent_id,
            caller_service: row.caller_service,
            tenant_id: row.tenant_id,
            user_id: row.user_id,
            status: RunStatus::from_str(&row.status),
            started_at: parse_datetime_or_now(&row.started_at),
            ended_at: row.ended_at.as_deref().map(parse_datetime_or_now),
            input_summary: parse_json_or_null(&row.input_summary),
            error_message: row.error_message,
        }
    }
}

impl RunEventRow {
    fn into_record(self, run: &RunSummaryRow) -> RunEventRecord {
        RunEventRecord {
            event_type: self.event_type,
            seq: self.seq.max(0) as u64,
            request_id: run.request_id.clone(),
            run_id: run.run_id.clone(),
            agent_id: run.agent_id.clone(),
            timestamp: parse_datetime_or_now(&self.timestamp),
            code: self.code,
            message: self.message,
            data: parse_json_or_null(&self.data),
        }
    }
}

struct ListRunsSql {
    sql: String,
    text_params: Vec<String>,
    limit: i64,
    returned_limit: usize,
}

fn build_list_runs_query(
    query: RunHistoryQuery,
    dialect: SqlDialect,
) -> Result<ListRunsSql, AppError> {
    let mut sql = "SELECT run_id, request_id, agent_id, caller_service, tenant_id, user_id, status, started_at, ended_at, input_summary, error_message FROM runs".to_string();
    let mut conditions = Vec::new();
    let mut params = Vec::new();

    push_text_filter(
        &mut conditions,
        &mut params,
        dialect,
        "agent_id",
        query.agent_id,
    );
    push_text_filter(
        &mut conditions,
        &mut params,
        dialect,
        "request_id",
        query.request_id,
    );
    push_text_filter(
        &mut conditions,
        &mut params,
        dialect,
        "caller_service",
        query.caller_service,
    );
    push_text_filter(
        &mut conditions,
        &mut params,
        dialect,
        "tenant_id",
        query.tenant_id,
    );
    push_text_filter(
        &mut conditions,
        &mut params,
        dialect,
        "user_id",
        query.user_id,
    );
    if let Some(status) = query.status {
        push_filter(
            &mut conditions,
            &mut params,
            dialect,
            "status",
            "=",
            status.as_str().to_string(),
        );
    }
    if let Some(started_after) = query.started_after {
        push_filter(
            &mut conditions,
            &mut params,
            dialect,
            "started_at",
            ">=",
            started_after.to_rfc3339(),
        );
    }
    if let Some(started_before) = query.started_before {
        push_filter(
            &mut conditions,
            &mut params,
            dialect,
            "started_at",
            "<=",
            started_before.to_rfc3339(),
        );
    }
    if let Some(after) = query.after {
        push_cursor_filter(&mut conditions, &mut params, dialect, &after)?;
    }

    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }

    sql.push_str(" ORDER BY started_at DESC, run_id DESC");
    let returned_limit = effective_limit(query.limit);
    let limit_placeholder = dialect.placeholder(params.len() + 1);
    sql.push_str(" LIMIT ");
    sql.push_str(&limit_placeholder);

    Ok(ListRunsSql {
        sql,
        text_params: params,
        limit: (returned_limit + 1) as i64,
        returned_limit,
    })
}

fn push_text_filter(
    conditions: &mut Vec<String>,
    params: &mut Vec<String>,
    dialect: SqlDialect,
    column: &'static str,
    value: Option<String>,
) {
    let Some(value) = value else {
        return;
    };
    let value = value.trim();
    if value.is_empty() {
        return;
    }
    match column {
        "agent_id" | "request_id" | "caller_service" | "tenant_id" | "user_id" => {
            push_filter(conditions, params, dialect, column, "=", value.to_string());
        }
        _ => unreachable!("unsupported run history filter column"),
    }
}

fn push_filter(
    conditions: &mut Vec<String>,
    params: &mut Vec<String>,
    dialect: SqlDialect,
    column: &'static str,
    operator: &'static str,
    value: String,
) {
    match (column, operator) {
        (
            "agent_id" | "request_id" | "caller_service" | "tenant_id" | "user_id" | "status",
            "=",
        )
        | ("started_at", ">=" | "<=") => {
            let placeholder = dialect.placeholder(params.len() + 1);
            conditions.push(format!("{column} {operator} {placeholder}"));
            params.push(value);
        }
        _ => unreachable!("unsupported run history filter"),
    }
}

fn push_cursor_filter(
    conditions: &mut Vec<String>,
    params: &mut Vec<String>,
    dialect: SqlDialect,
    cursor: &str,
) -> Result<(), AppError> {
    let cursor = decode_run_history_cursor(cursor)?;
    let started_at = cursor.started_at.to_rfc3339();
    let started_at_less = dialect.placeholder(params.len() + 1);
    params.push(started_at.clone());
    let started_at_equal = dialect.placeholder(params.len() + 1);
    params.push(started_at);
    let run_id_less = dialect.placeholder(params.len() + 1);
    params.push(cursor.run_id);
    conditions.push(format!(
        "(started_at < {started_at_less} OR (started_at = {started_at_equal} AND run_id < {run_id_less}))"
    ));
    Ok(())
}

fn run_summary_page(rows: Vec<RunSummaryRow>, returned_limit: usize) -> RunHistoryPage {
    let has_more = rows.len() > returned_limit;
    let items = rows
        .into_iter()
        .take(returned_limit)
        .map(RunSummary::from)
        .collect::<Vec<_>>();
    let next_cursor = has_more
        .then(|| items.last().map(encode_run_history_cursor))
        .flatten();
    RunHistoryPage { items, next_cursor }
}

#[derive(Debug)]
struct DecodedRunHistoryCursor {
    started_at: DateTime<Utc>,
    run_id: String,
}

fn encode_run_history_cursor(run: &RunSummary) -> String {
    format!("{}:{}", run.started_at.timestamp_micros(), run.run_id)
}

fn decode_run_history_cursor(cursor: &str) -> Result<DecodedRunHistoryCursor, AppError> {
    let (micros, run_id) = cursor
        .split_once(':')
        .ok_or_else(|| AppError::Input("invalid run history cursor".to_string()))?;
    let micros = micros
        .parse::<i64>()
        .map_err(|_| AppError::Input("invalid run history cursor".to_string()))?;
    let started_at = DateTime::<Utc>::from_timestamp_micros(micros)
        .ok_or_else(|| AppError::Input("invalid run history cursor".to_string()))?;
    let run_id = run_id.trim();
    if run_id.is_empty() {
        return Err(AppError::Input("invalid run history cursor".to_string()));
    }
    Ok(DecodedRunHistoryCursor {
        started_at,
        run_id: run_id.to_string(),
    })
}

fn effective_limit(limit: usize) -> usize {
    if limit == 0 {
        50
    } else {
        limit.min(200)
    }
}

async fn prepare_sqlite_schema_for_migration(pool: &SqlitePool) -> Result<(), AppError> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            request_id TEXT NOT NULL DEFAULT '',
            agent_id TEXT NOT NULL,
            caller_service TEXT,
            tenant_id TEXT,
            user_id TEXT,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            input_summary TEXT NOT NULL,
            error_message TEXT
        )",
    )
    .execute(pool)
    .await
    .map_err(map_sqlx_error)?;
    ensure_sqlite_legacy_columns(pool).await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS run_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            event TEXT NOT NULL,
            step_id TEXT,
            timestamp TEXT NOT NULL,
            content TEXT NOT NULL,
            result TEXT NOT NULL,
            code INTEGER NOT NULL,
            message TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await
    .map_err(map_sqlx_error)?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS step_outputs (
            run_id TEXT NOT NULL,
            step_id TEXT NOT NULL,
            output TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (run_id, step_id)
        )",
    )
    .execute(pool)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

async fn prepare_postgres_schema_for_migration(pool: &PgPool) -> Result<(), AppError> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            request_id TEXT NOT NULL DEFAULT '',
            agent_id TEXT NOT NULL,
            caller_service TEXT,
            tenant_id TEXT,
            user_id TEXT,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            input_summary TEXT NOT NULL,
            error_message TEXT
        )",
    )
    .execute(pool)
    .await
    .map_err(map_sqlx_error)?;
    ensure_postgres_legacy_columns(pool).await
}

async fn ensure_sqlite_legacy_columns(pool: &SqlitePool) -> Result<(), AppError> {
    let columns = sqlx::query("PRAGMA table_info(runs)")
        .fetch_all(pool)
        .await
        .map_err(map_sqlx_error)?
        .into_iter()
        .filter_map(|row| row.try_get::<String, _>("name").ok())
        .collect::<BTreeSet<_>>();
    add_sqlite_column_if_missing(pool, &columns, "request_id", "TEXT NOT NULL DEFAULT ''").await?;
    add_sqlite_column_if_missing(pool, &columns, "caller_service", "TEXT").await?;
    add_sqlite_column_if_missing(pool, &columns, "tenant_id", "TEXT").await?;
    add_sqlite_column_if_missing(pool, &columns, "user_id", "TEXT").await?;
    Ok(())
}

async fn add_sqlite_column_if_missing(
    pool: &SqlitePool,
    columns: &BTreeSet<String>,
    column: &str,
    definition: &str,
) -> Result<(), AppError> {
    if columns.contains(column) {
        return Ok(());
    }
    sqlx::query(AssertSqlSafe(format!(
        "ALTER TABLE runs ADD COLUMN {column} {definition}"
    )))
    .execute(pool)
    .await
    .map_err(map_sqlx_error)?;
    Ok(())
}

async fn ensure_postgres_legacy_columns(pool: &PgPool) -> Result<(), AppError> {
    let statements = [
        "ALTER TABLE runs ADD COLUMN IF NOT EXISTS request_id TEXT NOT NULL DEFAULT ''",
        "ALTER TABLE runs ADD COLUMN IF NOT EXISTS caller_service TEXT",
        "ALTER TABLE runs ADD COLUMN IF NOT EXISTS tenant_id TEXT",
        "ALTER TABLE runs ADD COLUMN IF NOT EXISTS user_id TEXT",
    ];
    for statement in statements {
        sqlx::query(statement)
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;
    }
    Ok(())
}

fn create_parent_dir(path: &Path) -> Result<(), AppError> {
    if let Some(parent) = path.parent() {
        if parent.as_os_str().is_empty() {
            return Ok(());
        }
        std::fs::create_dir_all(parent).map_err(|err| {
            AppError::Config(format!(
                "failed to create run history directory '{}': {err}",
                parent.display()
            ))
        })?;
    }
    Ok(())
}

fn create_sqlite_parent_dir_from_url(database_url: &str) -> Result<(), AppError> {
    let Some(path) = sqlite_file_path_from_url(database_url) else {
        return Ok(());
    };
    create_parent_dir(&path)
}

fn sqlite_file_path_from_url(database_url: &str) -> Option<PathBuf> {
    let database_url = database_url.split('?').next().unwrap_or(database_url);
    let path = database_url
        .strip_prefix("sqlite://")
        .or_else(|| database_url.strip_prefix("sqlite:"))
        .unwrap_or(database_url);
    if path.is_empty() || path == ":memory:" || path.starts_with("file:") {
        return None;
    }
    Some(PathBuf::from(path))
}

fn sqlite_max_connections(database_url: &str) -> u32 {
    if sqlite_file_path_from_url(database_url).is_some() {
        5
    } else {
        1
    }
}

fn summarize_input(input: &Value) -> Value {
    let object = input.as_object();
    let mut keys = object
        .map(|object| object.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    keys.sort();
    json!({
        "keys": keys,
        "report_text_chars": input.get("report_text").and_then(Value::as_str).map(|value| value.chars().count()),
        "images_count": input.get("images").and_then(Value::as_array).map(Vec::len),
        "messages_count": input.get("messages").and_then(Value::as_array).map(Vec::len),
        "question_chars": input.get("question").and_then(Value::as_str).map(|value| value.chars().count()),
    })
}

pub fn input_summary(input: &Value) -> Value {
    summarize_input(input)
}

fn parse_json_or_null(value: &str) -> Value {
    serde_json::from_str(value).unwrap_or(Value::Null)
}

fn parse_datetime_or_now(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

fn map_sqlx_error(err: sqlx::Error) -> AppError {
    AppError::Run(format!("run history sqlx error: {err}"))
}

fn map_migrate_error(err: MigrateError) -> AppError {
    AppError::Run(format!("run history migration error: {err}"))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::{
        engine::event::{RunEvent, RunEventScope, RunEventType},
        request_context::RequestContext,
    };

    #[tokio::test]
    async fn sqlite_in_memory_store_records_runs_events_and_step_outputs() {
        let store = RunHistoryStore::sqlite_in_memory().await.unwrap();
        let request = RequestContext {
            request_id: "req_history_store".to_string(),
            caller_service: Some("api-test".to_string()),
            tenant_id: Some("tenant-a".to_string()),
            user_id: Some("user-a".to_string()),
        };

        store
            .create_run(
                "run_history_store",
                "agent-a",
                &request,
                Utc::now(),
                json!({"keys": ["text"]}),
            )
            .await;
        store
            .record_event(&RunEvent::ok(
                RunEventType::StepCompleted,
                1,
                RunEventScope {
                    request_id: request.request_id.clone(),
                    run_id: "run_history_store".to_string(),
                    agent_id: "agent-a".to_string(),
                    step_id: Some("step-a".to_string()),
                },
                json!({"step_id": "step-a", "status": "completed"}),
            ))
            .await;
        store
            .record_step_output("run_history_store", "step-a", json!({"text": "done"}))
            .await;
        store
            .finish_run("run_history_store", RunStatus::Completed, None)
            .await;

        let run = store
            .get_run("run_history_store")
            .await
            .unwrap()
            .expect("run should be recorded");
        assert_eq!(run.request_id, "req_history_store");
        assert_eq!(run.agent_id, "agent-a");
        assert_eq!(run.caller_service.as_deref(), Some("api-test"));
        assert_eq!(run.tenant_id.as_deref(), Some("tenant-a"));
        assert_eq!(run.user_id.as_deref(), Some("user-a"));
        assert_eq!(run.status, RunStatus::Completed);
        assert_eq!(run.events.len(), 1);
        assert_eq!(run.events[0].event_type, "step.completed");
        assert_eq!(run.events[0].seq, 1);
        assert_eq!(run.events[0].request_id, "req_history_store");
        assert_eq!(run.events[0].run_id, "run_history_store");
        assert_eq!(run.events[0].agent_id, "agent-a");
        assert_eq!(run.events[0].data["status"], "completed");
        assert_eq!(run.step_outputs["step-a"]["text"], "done");

        let runs = store
            .list_runs(RunHistoryQuery {
                request_id: Some("req_history_store".to_string()),
                caller_service: Some("api-test".to_string()),
                tenant_id: Some("tenant-a".to_string()),
                user_id: Some("user-a".to_string()),
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].run_id, "run_history_store");
    }

    #[tokio::test]
    async fn sqlite_migration_preserves_legacy_run_events() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.sqlite3");
        let options = SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .unwrap();
        sqlx::raw_sql(
            "CREATE TABLE runs (
                run_id TEXT PRIMARY KEY,
                agent_id TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                input_summary TEXT NOT NULL,
                error_message TEXT
            );
            CREATE TABLE run_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                event TEXT NOT NULL,
                step_id TEXT,
                timestamp TEXT NOT NULL,
                content TEXT NOT NULL,
                result TEXT NOT NULL,
                code INTEGER NOT NULL,
                message TEXT NOT NULL
            );
            CREATE TABLE step_outputs (
                run_id TEXT NOT NULL,
                step_id TEXT NOT NULL,
                output TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (run_id, step_id)
            );",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO runs (run_id, agent_id, status, started_at, input_summary)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind("run_legacy")
        .bind("agent-a")
        .bind("completed")
        .bind(Utc::now().to_rfc3339())
        .bind("{}")
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO run_events (
                run_id, event, step_id, timestamp, content, result, code, message
             ) VALUES (?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("run_legacy")
        .bind("token_delta")
        .bind("generate")
        .bind(Utc::now().to_rfc3339())
        .bind("hello")
        .bind("null")
        .bind(0)
        .bind("ok")
        .bind("run_legacy")
        .bind("run_completed")
        .bind(Option::<String>::None)
        .bind(Utc::now().to_rfc3339())
        .bind("")
        .bind("null")
        .bind(0)
        .bind("ok")
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        let store = RunHistoryStore::sqlite(&path).await.unwrap();
        let run = store.get_run("run_legacy").await.unwrap().unwrap();

        assert_eq!(run.request_id, "");
        assert_eq!(run.events.len(), 2);
        assert_eq!(run.events[0].event_type, "content.delta");
        assert_eq!(run.events[0].seq, 1);
        assert_eq!(run.events[0].run_id, "run_legacy");
        assert_eq!(run.events[0].agent_id, "agent-a");
        assert_eq!(run.events[0].data["step_id"], "generate");
        assert_eq!(run.events[0].data["content"], "hello");
        assert_eq!(run.events[1].event_type, "run.completed");
        assert_eq!(run.events[1].seq, 2);
        assert_eq!(run.events[1].data["status"], "completed");
        assert_eq!(run.events[1].data["content"], "hello");
        assert!(run.events[1].data["output"].is_null());
    }

    #[tokio::test]
    async fn sqlite_store_filters_by_status_time_range_and_cursor() {
        let store = RunHistoryStore::sqlite_in_memory().await.unwrap();
        let request = RequestContext {
            request_id: "req_history_filter".to_string(),
            caller_service: Some("api-test".to_string()),
            tenant_id: Some("tenant-a".to_string()),
            user_id: Some("user-a".to_string()),
        };
        let base = Utc::now();

        for (index, status) in [
            (1, RunStatus::Completed),
            (2, RunStatus::Failed),
            (3, RunStatus::Completed),
        ] {
            let run_id = format!("run_history_filter_{index}");
            store
                .create_run(
                    &run_id,
                    "agent-a",
                    &request,
                    base + chrono::Duration::seconds(index),
                    json!({"index": index}),
                )
                .await;
            store.finish_run(&run_id, status, None).await;
        }

        let filtered = store
            .list_runs_page(RunHistoryQuery {
                agent_id: Some("agent-a".to_string()),
                status: Some(RunStatus::Completed),
                started_after: Some(base + chrono::Duration::seconds(1)),
                started_before: Some(base + chrono::Duration::seconds(3)),
                limit: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(filtered.items.len(), 2);
        assert_eq!(filtered.items[0].run_id, "run_history_filter_3");
        assert_eq!(filtered.items[1].run_id, "run_history_filter_1");
        assert!(filtered.next_cursor.is_none());

        let first_page = store
            .list_runs_page(RunHistoryQuery {
                agent_id: Some("agent-a".to_string()),
                limit: 2,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            first_page
                .items
                .iter()
                .map(|run| run.run_id.as_str())
                .collect::<Vec<_>>(),
            vec!["run_history_filter_3", "run_history_filter_2"]
        );
        let next_cursor = first_page
            .next_cursor
            .expect("first page should include next cursor");

        let second_page = store
            .list_runs_page(RunHistoryQuery {
                agent_id: Some("agent-a".to_string()),
                after: Some(next_cursor),
                limit: 2,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            second_page
                .items
                .iter()
                .map(|run| run.run_id.as_str())
                .collect::<Vec<_>>(),
            vec!["run_history_filter_1"]
        );
        assert!(second_page.next_cursor.is_none());
    }
}
