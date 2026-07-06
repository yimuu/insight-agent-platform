use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::task;

use crate::{engine::event::RunEvent, error::AppError};

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
}

#[derive(Debug, Clone, Serialize)]
pub struct RunSummary {
    pub run_id: String,
    pub agent_id: String,
    pub status: RunStatus,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub input_summary: Value,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunEventRecord {
    pub event: String,
    pub step_id: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub content: String,
    pub result: Value,
    pub code: i32,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunRecord {
    pub run_id: String,
    pub agent_id: String,
    pub status: RunStatus,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub input_summary: Value,
    pub error_message: Option<String>,
    pub events: Vec<RunEventRecord>,
    pub step_outputs: BTreeMap<String, Value>,
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

    pub fn sqlite(path: impl AsRef<Path>) -> Result<Self, AppError> {
        Ok(Self {
            inner: Arc::new(SqliteRunHistoryRepository::open(path.as_ref())?),
        })
    }

    pub fn sqlite_in_memory() -> Result<Self, AppError> {
        Ok(Self {
            inner: Arc::new(SqliteRunHistoryRepository::open_in_memory()?),
        })
    }

    pub async fn create_run(
        &self,
        run_id: &str,
        agent_id: &str,
        started_at: DateTime<Utc>,
        input_summary: Value,
    ) {
        if let Err(err) = self
            .inner
            .create_run(run_id, agent_id, started_at, input_summary)
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
        self.inner.list_agent_runs(agent_id, limit).await
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
    async fn list_agent_runs(
        &self,
        agent_id: &str,
        limit: usize,
    ) -> Result<Vec<RunSummary>, AppError>;
}

struct NoopRunHistoryRepository;

#[async_trait]
impl RunHistoryRepository for NoopRunHistoryRepository {
    async fn create_run(
        &self,
        _run_id: &str,
        _agent_id: &str,
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

    async fn list_agent_runs(
        &self,
        _agent_id: &str,
        _limit: usize,
    ) -> Result<Vec<RunSummary>, AppError> {
        Ok(Vec::new())
    }
}

#[derive(Clone)]
struct SqliteRunHistoryRepository {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteRunHistoryRepository {
    fn open(path: &Path) -> Result<Self, AppError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| {
                AppError::Config(format!(
                    "failed to create run history directory '{}': {err}",
                    parent.display()
                ))
            })?;
        }
        let conn = Connection::open(path).map_err(map_sqlite_error)?;
        let repository = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        repository.init()?;
        Ok(repository)
    }

    fn open_in_memory() -> Result<Self, AppError> {
        let conn = Connection::open_in_memory().map_err(map_sqlite_error)?;
        let repository = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        repository.init()?;
        Ok(repository)
    }

    fn init(&self) -> Result<(), AppError> {
        let conn = self.conn.lock().map_err(map_mutex_error)?;
        conn.execute_batch(
            r#"
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    input_summary TEXT NOT NULL,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_runs_agent_started_at ON runs(agent_id, started_at DESC);

CREATE TABLE IF NOT EXISTS run_events (
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

CREATE INDEX IF NOT EXISTS idx_run_events_run_id ON run_events(run_id, id);

CREATE TABLE IF NOT EXISTS step_outputs (
    run_id TEXT NOT NULL,
    step_id TEXT NOT NULL,
    output TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, step_id)
);
"#,
        )
        .map_err(map_sqlite_error)
    }

    async fn with_conn<T, F>(&self, f: F) -> Result<T, AppError>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T, AppError> + Send + 'static,
    {
        let conn = self.conn.clone();
        task::spawn_blocking(move || {
            let conn = conn.lock().map_err(map_mutex_error)?;
            f(&conn)
        })
        .await
        .map_err(|err| AppError::Run(format!("run history task join error: {err}")))?
    }
}

#[async_trait]
impl RunHistoryRepository for SqliteRunHistoryRepository {
    async fn create_run(
        &self,
        run_id: &str,
        agent_id: &str,
        started_at: DateTime<Utc>,
        input_summary: Value,
    ) -> Result<(), AppError> {
        let run_id = run_id.to_string();
        let agent_id = agent_id.to_string();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT OR REPLACE INTO runs (run_id, agent_id, status, started_at, ended_at, input_summary, error_message)
                 VALUES (?1, ?2, ?3, ?4, NULL, ?5, NULL)",
                params![
                    run_id,
                    agent_id,
                    RunStatus::Running.as_str(),
                    started_at.to_rfc3339(),
                    input_summary.to_string()
                ],
            )
            .map_err(map_sqlite_error)?;
            Ok(())
        })
        .await
    }

    async fn record_event(&self, event: &RunEvent) -> Result<(), AppError> {
        let run_id = event.run_id.clone();
        let event_name = event.event.as_sse_name().to_string();
        let step_id = event.step_id.clone();
        let timestamp = event.timestamp;
        let content = event.content.clone();
        let result = event.result.clone();
        let code = event.code;
        let message = event.message.clone();

        self.with_conn(move |conn| {
            conn.execute(
                "INSERT INTO run_events (run_id, event, step_id, timestamp, content, result, code, message)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    run_id,
                    event_name,
                    step_id,
                    timestamp.to_rfc3339(),
                    content,
                    result.to_string(),
                    code,
                    message,
                ],
            )
            .map_err(map_sqlite_error)?;
            Ok(())
        })
        .await
    }

    async fn record_step_output(
        &self,
        run_id: &str,
        step_id: &str,
        output: Value,
    ) -> Result<(), AppError> {
        let run_id = run_id.to_string();
        let step_id = step_id.to_string();
        self.with_conn(move |conn| {
            conn.execute(
                "INSERT OR REPLACE INTO step_outputs (run_id, step_id, output, created_at)
                 VALUES (?1, ?2, ?3, ?4)",
                params![run_id, step_id, output.to_string(), Utc::now().to_rfc3339()],
            )
            .map_err(map_sqlite_error)?;
            Ok(())
        })
        .await
    }

    async fn finish_run(
        &self,
        run_id: &str,
        status: RunStatus,
        error_message: Option<String>,
    ) -> Result<(), AppError> {
        let run_id = run_id.to_string();
        self.with_conn(move |conn| {
            conn.execute(
                "UPDATE runs SET status = ?2, ended_at = ?3, error_message = ?4 WHERE run_id = ?1",
                params![
                    run_id,
                    status.as_str(),
                    Utc::now().to_rfc3339(),
                    error_message
                ],
            )
            .map_err(map_sqlite_error)?;
            Ok(())
        })
        .await
    }

    async fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>, AppError> {
        let run_id = run_id.to_string();
        self.with_conn(move |conn| {
            let summary = conn
                .query_row(
                    "SELECT run_id, agent_id, status, started_at, ended_at, input_summary, error_message
                     FROM runs WHERE run_id = ?1",
                    params![run_id],
                    read_run_summary,
                )
                .optional()
                .map_err(map_sqlite_error)?;

            let Some(summary) = summary else {
                return Ok(None);
            };

            let mut event_stmt = conn
                .prepare(
                    "SELECT event, step_id, timestamp, content, result, code, message
                     FROM run_events WHERE run_id = ?1 ORDER BY id ASC",
                )
                .map_err(map_sqlite_error)?;
            let events = event_stmt
                .query_map(params![run_id], read_run_event)
                .map_err(map_sqlite_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(map_sqlite_error)?;

            let mut output_stmt = conn
                .prepare(
                    "SELECT step_id, output FROM step_outputs WHERE run_id = ?1 ORDER BY step_id ASC",
                )
                .map_err(map_sqlite_error)?;
            let step_outputs = output_stmt
                .query_map(params![run_id], |row| {
                    let step_id: String = row.get(0)?;
                    let output: String = row.get(1)?;
                    Ok((step_id, parse_json_or_null(&output)))
                })
                .map_err(map_sqlite_error)?
                .collect::<Result<BTreeMap<_, _>, _>>()
                .map_err(map_sqlite_error)?;

            Ok(Some(RunRecord {
                run_id: summary.run_id,
                agent_id: summary.agent_id,
                status: summary.status,
                started_at: summary.started_at,
                ended_at: summary.ended_at,
                input_summary: summary.input_summary,
                error_message: summary.error_message,
                events,
                step_outputs,
            }))
        })
        .await
    }

    async fn list_agent_runs(
        &self,
        agent_id: &str,
        limit: usize,
    ) -> Result<Vec<RunSummary>, AppError> {
        let agent_id = agent_id.to_string();
        self.with_conn(move |conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT run_id, agent_id, status, started_at, ended_at, input_summary, error_message
                     FROM runs WHERE agent_id = ?1 ORDER BY started_at DESC LIMIT ?2",
                )
                .map_err(map_sqlite_error)?;
            let runs = stmt
                .query_map(params![agent_id, limit as i64], read_run_summary)
                .map_err(map_sqlite_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(map_sqlite_error)?;
            Ok(runs)
        })
        .await
    }
}

fn read_run_summary(row: &rusqlite::Row<'_>) -> rusqlite::Result<RunSummary> {
    let status: String = row.get(2)?;
    let started_at: String = row.get(3)?;
    let ended_at: Option<String> = row.get(4)?;
    let input_summary: String = row.get(5)?;
    Ok(RunSummary {
        run_id: row.get(0)?,
        agent_id: row.get(1)?,
        status: RunStatus::from_str(&status),
        started_at: parse_datetime_or_now(&started_at),
        ended_at: ended_at.as_deref().map(parse_datetime_or_now),
        input_summary: parse_json_or_null(&input_summary),
        error_message: row.get(6)?,
    })
}

fn read_run_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<RunEventRecord> {
    let timestamp: String = row.get(2)?;
    let result: String = row.get(4)?;
    Ok(RunEventRecord {
        event: row.get(0)?,
        step_id: row.get(1)?,
        timestamp: parse_datetime_or_now(&timestamp),
        content: row.get(3)?,
        result: parse_json_or_null(&result),
        code: row.get(5)?,
        message: row.get(6)?,
    })
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

fn map_sqlite_error(err: rusqlite::Error) -> AppError {
    AppError::Run(format!("run history sqlite error: {err}"))
}

fn map_mutex_error<T>(err: std::sync::PoisonError<T>) -> AppError {
    AppError::Run(format!("run history lock error: {err}"))
}
