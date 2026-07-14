use std::{path::Path, str::FromStr};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Serialize};
use sqlx::{
    migrate::Migrator,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    FromRow, Sqlite, SqlitePool, Transaction,
};

use crate::{
    events::protocol::{RunEvent, RunEventScope, RunEventType, EVENT_SCHEMA_VERSION},
    history::{
        repository::{HistoryError, RunRepository, TerminalProposal, TerminalSequence},
        types::{
            NewRun, NodeOutputRecord, RunAttachment, RunLifecycle, RunRecord, RunStatus,
            RunTerminal, StopError, TerminalUpdate,
        },
    },
    outcome::{FailureKind, RunFailure, RunOutput},
};

static MIGRATOR: Migrator = sqlx::migrate!("migrations/formal_v1/sqlite");

#[derive(Clone)]
pub struct SqliteRunRepository {
    pool: SqlitePool,
}

impl SqliteRunRepository {
    pub async fn connect(database_url: &str) -> Result<Self, HistoryError> {
        let options = SqliteConnectOptions::from_str(database_url)
            .map_err(|error| init_error("invalid SQLite history configuration", error))?
            .foreign_keys(true);
        Self::connect_options(options, 5).await
    }

    pub async fn in_memory() -> Result<Self, HistoryError> {
        let options = SqliteConnectOptions::new()
            .filename(":memory:")
            .foreign_keys(true);
        Self::connect_options(options, 1).await
    }

    pub async fn connect_path(path: &Path) -> Result<Self, HistoryError> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true);
        Self::connect_options(options, 5).await
    }

    async fn connect_options(
        options: SqliteConnectOptions,
        max_connections: u32,
    ) -> Result<Self, HistoryError> {
        let pool = SqlitePoolOptions::new()
            .max_connections(max_connections)
            .connect_with(options)
            .await
            .map_err(|error| init_error("failed to initialize SQLite history", error))?;
        MIGRATOR
            .run(&pool)
            .await
            .map_err(|error| init_error("failed to migrate SQLite history", error))?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl RunRepository for SqliteRunRepository {
    async fn create_run(&self, run: NewRun) -> Result<(), HistoryError> {
        let input_summary = serialize_json(&run.input_summary, "input summary")?;
        sqlx::query(
            "INSERT INTO runs (
                run_id, request_id, agent_id, agent_version, attachment, status,
                started_at, ended_at, updated_at, input_summary, output,
                error_kind, error_code, error_message
             ) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, NULL, NULL, NULL, NULL)",
        )
        .bind(&run.run_id)
        .bind(&run.request_id)
        .bind(&run.agent_id)
        .bind(&run.agent_version)
        .bind(run.attachment.as_str())
        .bind(RunStatus::Created.as_str())
        .bind(format_timestamp(run.created_at))
        .bind(input_summary)
        .execute(&self.pool)
        .await
        .map_err(write_error)?;
        Ok(())
    }

    async fn mark_running(
        &self,
        run_id: &str,
        started_at: DateTime<Utc>,
    ) -> Result<(), HistoryError> {
        let result = sqlx::query(
            "UPDATE runs
             SET status = ?, started_at = ?, updated_at = ?
             WHERE run_id = ? AND status = ?",
        )
        .bind(RunStatus::Running.as_str())
        .bind(format_timestamp(started_at))
        .bind(format_timestamp(started_at))
        .bind(run_id)
        .bind(RunStatus::Created.as_str())
        .execute(&self.pool)
        .await
        .map_err(write_error)?;
        if result.rows_affected() != 1 {
            return Err(HistoryError::new(
                "HISTORY_CONFLICT",
                "run cannot transition to running",
            ));
        }
        Ok(())
    }

    async fn append_events(&self, events: &[RunEvent]) -> Result<(), HistoryError> {
        if events.is_empty() {
            return Ok(());
        }
        let mut transaction = self.pool.begin().await.map_err(write_error)?;
        for event in events {
            insert_event(&mut transaction, event).await?;
        }
        transaction.commit().await.map_err(write_error)
    }

    async fn put_node_output(&self, output: NodeOutputRecord) -> Result<(), HistoryError> {
        let value = serialize_json(&output.output, "node output")?;
        sqlx::query(
            "INSERT INTO node_outputs (run_id, node_id, output, completed_at)
             VALUES (?, ?, ?, ?)",
        )
        .bind(&output.run_id)
        .bind(&output.node_id)
        .bind(value)
        .bind(format_timestamp(output.completed_at))
        .execute(&self.pool)
        .await
        .map_err(write_error)?;
        Ok(())
    }

    async fn commit_terminal(
        &self,
        proposal: TerminalProposal,
        sequence: TerminalSequence,
    ) -> Result<RunEvent, HistoryError> {
        match commit_sqlite_once(&self.pool, proposal.clone(), sequence).await {
            Ok(event) => Ok(event),
            Err(error)
                if matches!(sequence, TerminalSequence::NextDurable)
                    && matches!(error.code(), "HISTORY_WRITE_FAILED" | "HISTORY_READ_FAILED") =>
            {
                commit_sqlite_once(&self.pool, proposal, sequence).await
            }
            Err(error) => Err(error),
        }
    }

    async fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>, HistoryError> {
        let row = sqlx::query_as::<_, RunRow>(
            "SELECT run_id, request_id, agent_id, agent_version, attachment, status,
                    started_at, ended_at, updated_at, input_summary, output,
                    error_kind, error_code, error_message
             FROM runs WHERE run_id = ?",
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(read_error)?;
        row.map(run_record_from_row).transpose()
    }

    async fn list_events_after(
        &self,
        run_id: &str,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<RunEvent>, HistoryError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let after_seq = to_i64(after_seq, "event sequence is too large")?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = sqlx::query_as::<_, EventRow>(
            "SELECT e.seq, e.event_type, e.node_id, e.timestamp, e.code, e.message, e.data,
                    r.request_id, r.run_id, r.agent_id, r.agent_version
             FROM run_events e
             JOIN runs r ON r.run_id = e.run_id
             WHERE e.run_id = ? AND e.seq > ?
             ORDER BY e.seq ASC
             LIMIT ?",
        )
        .bind(run_id)
        .bind(after_seq)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(read_error)?;
        rows.into_iter().map(run_event_from_row).collect()
    }

    async fn mark_incomplete_interrupted(&self, at: DateTime<Utc>) -> Result<u64, HistoryError> {
        let mut transaction = self.pool.begin().await.map_err(write_error)?;
        let rows = sqlx::query_as::<_, IncompleteRunRow>(
            "SELECT r.run_id, r.request_id, r.agent_id, r.agent_version,
                    COALESCE(MAX(e.seq), 0) AS max_seq
             FROM runs r
             LEFT JOIN run_events e ON e.run_id = r.run_id
             WHERE r.status IN (?, ?)
             GROUP BY r.run_id, r.request_id, r.agent_id, r.agent_version
             ORDER BY r.run_id",
        )
        .bind(RunStatus::Created.as_str())
        .bind(RunStatus::Running.as_str())
        .fetch_all(&mut *transaction)
        .await
        .map_err(read_error)?;
        let timestamp = format_timestamp(at);
        let mut interrupted = 0_u64;
        for row in rows {
            let result = sqlx::query(
                "UPDATE runs
                 SET status = ?, ended_at = ?, updated_at = ?, output = NULL,
                     error_kind = NULL, error_code = ?, error_message = ?
                 WHERE run_id = ? AND status IN (?, ?)",
            )
            .bind(RunStatus::Interrupted.as_str())
            .bind(&timestamp)
            .bind(&timestamp)
            .bind("RUN_INTERRUPTED")
            .bind("run interrupted during startup reconciliation")
            .bind(&row.run_id)
            .bind(RunStatus::Created.as_str())
            .bind(RunStatus::Running.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(write_error)?;
            if result.rows_affected() == 0 {
                continue;
            }
            let seq = row
                .max_seq
                .checked_add(1)
                .and_then(|value| u64::try_from(value).ok())
                .ok_or_else(|| {
                    HistoryError::new("HISTORY_DATA_INVALID", "stored event sequence overflowed")
                })?;
            let proposal = TerminalProposal::new(
                RunEventScope::for_run(
                    row.request_id,
                    row.run_id.clone(),
                    row.agent_id,
                    row.agent_version,
                ),
                TerminalUpdate::new(
                    row.run_id,
                    at,
                    RunTerminal::Interrupted {
                        error: StopError {
                            code: "RUN_INTERRUPTED".to_string(),
                            message: "run interrupted during startup reconciliation".to_string(),
                        },
                    },
                ),
            )?;
            insert_event(&mut transaction, &proposal.event_at(seq)).await?;
            interrupted += 1;
        }
        transaction.commit().await.map_err(write_error)?;
        Ok(interrupted)
    }
}

async fn commit_sqlite_once(
    pool: &SqlitePool,
    proposal: TerminalProposal,
    sequence: TerminalSequence,
) -> Result<RunEvent, HistoryError> {
    let run_id = proposal.run_id().to_string();
    let mut transaction = pool.begin().await.map_err(write_error)?;
    let locked = sqlx::query("UPDATE runs SET updated_at = updated_at WHERE run_id = ?")
        .bind(&run_id)
        .execute(&mut *transaction)
        .await
        .map_err(write_error)?;
    if locked.rows_affected() != 1 {
        return Err(HistoryError::new("RUN_NOT_FOUND", "run not found"));
    }
    let status_value: String = sqlx::query_scalar("SELECT status FROM runs WHERE run_id = ?")
        .bind(&run_id)
        .fetch_one(&mut *transaction)
        .await
        .map_err(read_error)?;
    let status = RunStatus::parse(&status_value)
        .ok_or_else(|| invalid_data(format!("invalid stored run status '{status_value}'")))?;
    if status.is_terminal() {
        ensure_contiguous_events_sqlite(&mut transaction, &run_id).await?;
        let existing = load_last_event_sqlite(&mut transaction, &run_id).await?;
        ensure_terminal_matches_status(status, &existing)?;
        transaction.rollback().await.map_err(write_error)?;
        return Ok(existing);
    }
    if !matches!(status, RunStatus::Created | RunStatus::Running) {
        return Err(HistoryError::new(
            "HISTORY_CONFLICT",
            "run cannot be recovered from its current state",
        ));
    }
    let maximum: i64 =
        sqlx::query_scalar("SELECT COALESCE(MAX(seq), 0) FROM run_events WHERE run_id = ?")
            .bind(&run_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(read_error)?;
    let next = maximum
        .checked_add(1)
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| HistoryError::new("HISTORY_DATA_INVALID", "event sequence overflowed"))?;
    if let TerminalSequence::Expected(expected) = sequence {
        if expected != next {
            return Err(HistoryError::new(
                "HISTORY_EVENT_INVALID",
                "expected terminal sequence does not match durable next sequence",
            ));
        }
    }
    let terminal = proposal.event_at(next);
    let (_, update) = proposal.into_parts();
    let output = update
        .terminal
        .output()
        .map(|output| serialize_json(output, "run output"))
        .transpose()?;
    let error_kind = update.terminal.failure().map(|error| error.kind.as_str());
    sqlx::query(
        "UPDATE runs
         SET status = ?, ended_at = ?, updated_at = ?, output = ?,
             error_kind = ?, error_code = ?, error_message = ?
         WHERE run_id = ?",
    )
    .bind(update.status().as_str())
    .bind(format_timestamp(update.ended_at))
    .bind(format_timestamp(update.ended_at))
    .bind(output)
    .bind(error_kind)
    .bind(update.terminal.error_code())
    .bind(update.terminal.error_message())
    .bind(&run_id)
    .execute(&mut *transaction)
    .await
    .map_err(write_error)?;
    insert_event(&mut transaction, &terminal).await?;
    ensure_contiguous_events_sqlite(&mut transaction, &run_id).await?;
    transaction.commit().await.map_err(write_error)?;
    Ok(terminal)
}

async fn load_last_event_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &str,
) -> Result<RunEvent, HistoryError> {
    let row = sqlx::query_as::<_, EventRow>(
        "SELECT e.seq, e.event_type, e.node_id, e.timestamp, e.code, e.message, e.data,
                r.request_id, r.run_id, r.agent_id, r.agent_version
         FROM run_events e
         JOIN runs r ON r.run_id = e.run_id
         WHERE e.run_id = ?
         ORDER BY e.seq DESC
         LIMIT 1",
    )
    .bind(run_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(read_error)?
    .ok_or_else(|| {
        HistoryError::new(
            "HISTORY_TERMINAL_EVENT_MISSING",
            "terminal run has no terminal event",
        )
    })?;
    run_event_from_row(row)
}

fn ensure_terminal_matches_status(status: RunStatus, event: &RunEvent) -> Result<(), HistoryError> {
    if terminal_event_type(status) != event.event_type || event.node_id.is_some() {
        return Err(HistoryError::new(
            "HISTORY_TERMINAL_EVENT_MISMATCH",
            "stored terminal event does not match run status",
        ));
    }
    Ok(())
}

async fn ensure_contiguous_events_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &str,
) -> Result<(), HistoryError> {
    let (count, minimum, maximum) = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT COUNT(*), COALESCE(MIN(seq), 0), COALESCE(MAX(seq), 0)
         FROM run_events WHERE run_id = ?",
    )
    .bind(run_id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(read_error)?;
    if count == 0 || minimum != 1 || count != maximum {
        return Err(HistoryError::new(
            "HISTORY_RECOVERY_GAP",
            "cannot recover a run with an event sequence gap",
        ));
    }
    Ok(())
}

async fn insert_event(
    transaction: &mut Transaction<'_, Sqlite>,
    event: &RunEvent,
) -> Result<(), HistoryError> {
    if event.schema_version != EVENT_SCHEMA_VERSION {
        return Err(HistoryError::new(
            "HISTORY_EVENT_INVALID",
            "unsupported event schema version",
        ));
    }
    let seq = to_i64(event.seq, "event sequence is too large")?;
    let data = serialize_json(&event.data, "event data")?;
    sqlx::query(
        "INSERT INTO run_events (
            run_id, seq, event_type, node_id, timestamp, code, message, data
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&event.run_id)
    .bind(seq)
    .bind(event.event_type.as_str())
    .bind(&event.node_id)
    .bind(format_timestamp(event.timestamp))
    .bind(&event.code)
    .bind(&event.message)
    .bind(data)
    .execute(&mut **transaction)
    .await
    .map_err(write_error)?;
    Ok(())
}

#[derive(Debug, FromRow)]
struct RunRow {
    run_id: String,
    request_id: String,
    agent_id: String,
    agent_version: String,
    attachment: String,
    status: String,
    started_at: Option<String>,
    ended_at: Option<String>,
    updated_at: String,
    input_summary: String,
    output: Option<String>,
    error_kind: Option<String>,
    error_code: Option<String>,
    error_message: Option<String>,
}

fn run_record_from_row(row: RunRow) -> Result<RunRecord, HistoryError> {
    let status = RunStatus::parse(&row.status)
        .ok_or_else(|| invalid_data(format!("invalid stored run status '{}'", row.status)))?;
    let output = row
        .output
        .map(|value| deserialize_json(&value, "output"))
        .transpose()?;
    let error_kind = row
        .error_kind
        .as_deref()
        .map(parse_failure_kind)
        .transpose()?;
    let ended_at = row
        .ended_at
        .map(|value| parse_timestamp(&value, "ended_at"))
        .transpose()?;
    let lifecycle = lifecycle_from_columns(
        status,
        ended_at.as_ref(),
        output,
        error_kind,
        row.error_code,
        row.error_message,
    )?;

    Ok(RunRecord {
        run_id: row.run_id,
        request_id: row.request_id,
        agent_id: row.agent_id,
        agent_version: row.agent_version,
        attachment: RunAttachment::parse(&row.attachment).ok_or_else(|| {
            invalid_data(format!(
                "invalid stored run attachment '{}'",
                row.attachment
            ))
        })?,
        lifecycle,
        started_at: row
            .started_at
            .map(|value| parse_timestamp(&value, "started_at"))
            .transpose()?,
        ended_at,
        updated_at: parse_timestamp(&row.updated_at, "updated_at")?,
        input_summary: deserialize_json(&row.input_summary, "input_summary")?,
    })
}

fn lifecycle_from_columns(
    status: RunStatus,
    ended_at: Option<&DateTime<Utc>>,
    output: Option<RunOutput>,
    error_kind: Option<FailureKind>,
    error_code: Option<String>,
    error_message: Option<String>,
) -> Result<RunLifecycle, HistoryError> {
    match (
        status,
        ended_at,
        output,
        error_kind,
        error_code,
        error_message,
    ) {
        (RunStatus::Created, None, None, None, None, None) => Ok(RunLifecycle::Created),
        (RunStatus::Running, None, None, None, None, None) => Ok(RunLifecycle::Running),
        (RunStatus::Completed, Some(_), Some(output), None, None, None) => {
            Ok(RunLifecycle::Completed { output })
        }
        (RunStatus::Failed, Some(_), None, Some(kind), Some(code), Some(message)) => {
            Ok(RunLifecycle::Failed {
                error: RunFailure {
                    kind,
                    code,
                    message,
                },
            })
        }
        (RunStatus::Cancelled, Some(_), None, None, Some(code), Some(message)) => {
            Ok(RunLifecycle::Cancelled {
                error: StopError { code, message },
            })
        }
        (RunStatus::Interrupted, Some(_), None, None, Some(code), Some(message)) => {
            Ok(RunLifecycle::Interrupted {
                error: StopError { code, message },
            })
        }
        (status, _, _, _, _, _) => Err(HistoryError::new(
            "HISTORY_TERMINAL_CORRUPT",
            format!(
                "run columns are inconsistent with lifecycle status '{}'",
                status.as_str()
            ),
        )),
    }
}

fn parse_failure_kind(value: &str) -> Result<FailureKind, HistoryError> {
    FailureKind::parse(value).ok_or_else(|| {
        HistoryError::new(
            "HISTORY_TERMINAL_CORRUPT",
            format!("stored failure kind '{value}' is invalid"),
        )
    })
}

#[derive(Debug, FromRow)]
struct EventRow {
    seq: i64,
    event_type: String,
    node_id: Option<String>,
    timestamp: String,
    code: String,
    message: String,
    data: String,
    request_id: String,
    run_id: String,
    agent_id: String,
    agent_version: String,
}

fn run_event_from_row(row: EventRow) -> Result<RunEvent, HistoryError> {
    Ok(RunEvent {
        schema_version: EVENT_SCHEMA_VERSION,
        event_type: RunEventType::parse(&row.event_type).ok_or_else(|| {
            invalid_data(format!("invalid stored event type '{}'", row.event_type))
        })?,
        seq: u64::try_from(row.seq)
            .map_err(|_| invalid_data("stored event sequence must not be negative"))?,
        request_id: row.request_id,
        run_id: row.run_id,
        agent_id: row.agent_id,
        agent_version: row.agent_version,
        node_id: row.node_id,
        timestamp: parse_timestamp(&row.timestamp, "event timestamp")?,
        code: row.code,
        message: row.message,
        data: deserialize_json(&row.data, "event data")?,
    })
}

#[derive(Debug, FromRow)]
struct IncompleteRunRow {
    run_id: String,
    request_id: String,
    agent_id: String,
    agent_version: String,
    max_seq: i64,
}

fn serialize_json<T>(value: &T, field: &str) -> Result<String, HistoryError>
where
    T: Serialize,
{
    serde_json::to_string(value).map_err(|error| {
        HistoryError::with_source(
            "HISTORY_DATA_INVALID",
            format!("failed to serialize {field}"),
            error,
        )
    })
}

fn deserialize_json<T>(value: &str, field: &str) -> Result<T, HistoryError>
where
    T: DeserializeOwned,
{
    serde_json::from_str(value).map_err(|error| {
        HistoryError::with_source(
            "HISTORY_DATA_INVALID",
            format!("invalid stored {field}"),
            error,
        )
    })
}

fn format_timestamp(timestamp: DateTime<Utc>) -> String {
    timestamp.to_rfc3339()
}

fn parse_timestamp(value: &str, field: &str) -> Result<DateTime<Utc>, HistoryError> {
    DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&Utc))
        .map_err(|error| {
            HistoryError::with_source(
                "HISTORY_DATA_INVALID",
                format!("invalid stored {field}"),
                error,
            )
        })
}

fn to_i64(value: u64, message: &'static str) -> Result<i64, HistoryError> {
    i64::try_from(value).map_err(|_| HistoryError::new("HISTORY_DATA_INVALID", message))
}

fn invalid_data(message: impl Into<String>) -> HistoryError {
    HistoryError::new("HISTORY_DATA_INVALID", message)
}

fn terminal_event_type(status: RunStatus) -> RunEventType {
    match status {
        RunStatus::Completed => RunEventType::RunCompleted,
        RunStatus::Failed => RunEventType::RunFailed,
        RunStatus::Cancelled => RunEventType::RunCancelled,
        RunStatus::Interrupted => RunEventType::RunInterrupted,
        RunStatus::Created | RunStatus::Running => unreachable!("typed terminal is terminal"),
    }
}

fn init_error<E>(message: &'static str, source: E) -> HistoryError
where
    E: std::error::Error + Send + Sync + 'static,
{
    HistoryError::with_source("HISTORY_INIT_FAILED", message, source)
}

fn write_error(error: sqlx::Error) -> HistoryError {
    HistoryError::with_source("HISTORY_WRITE_FAILED", "history write failed", error)
}

fn read_error(error: sqlx::Error) -> HistoryError {
    HistoryError::with_source("HISTORY_READ_FAILED", "history read failed", error)
}
