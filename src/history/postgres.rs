use std::collections::BTreeSet;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{
    migrate::Migrator, postgres::PgPoolOptions, types::Json, FromRow, PgPool, Postgres, Transaction,
};

use crate::{
    dsl::compiled::RunOutput,
    events::protocol::{RunEvent, RunEventType, EVENT_SCHEMA_VERSION},
    history::{
        repository::{validate_recovery_event, HistoryError, RunRepository},
        types::{NewRun, NodeOutputRecord, RunAttachment, RunRecord, RunStatus, TerminalUpdate},
    },
};

static MIGRATOR: Migrator = sqlx::migrate!("migrations/formal_v1/postgres");

#[derive(Clone)]
pub struct PostgresRunRepository {
    pool: PgPool,
}

impl PostgresRunRepository {
    pub async fn connect(database_url: &str) -> Result<Self, HistoryError> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .map_err(|error| init_error("failed to initialize PostgreSQL history", error))?;
        MIGRATOR
            .run(&pool)
            .await
            .map_err(|error| init_error("failed to migrate PostgreSQL history", error))?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl RunRepository for PostgresRunRepository {
    async fn create_run(&self, run: NewRun) -> Result<(), HistoryError> {
        sqlx::query(
            "INSERT INTO runs (
                run_id, request_id, agent_id, agent_version, attachment, status,
                started_at, ended_at, updated_at, input_summary, output, error_code, error_message
             ) VALUES ($1, $2, $3, $4, $5, $6, NULL, NULL, $7, $8, NULL, NULL, NULL)",
        )
        .bind(&run.run_id)
        .bind(&run.request_id)
        .bind(&run.agent_id)
        .bind(&run.agent_version)
        .bind(run.attachment.as_str())
        .bind(RunStatus::Created.as_str())
        .bind(run.created_at)
        .bind(Json(run.input_summary))
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
             SET status = $1, started_at = $2, updated_at = $2
             WHERE run_id = $3 AND status = $4",
        )
        .bind(RunStatus::Running.as_str())
        .bind(started_at)
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
        let run_ids = events
            .iter()
            .map(|event| event.run_id.as_str())
            .collect::<BTreeSet<_>>();
        for run_id in run_ids {
            let locked: Option<String> =
                sqlx::query_scalar("SELECT run_id FROM runs WHERE run_id = $1 FOR UPDATE")
                    .bind(run_id)
                    .fetch_optional(&mut *transaction)
                    .await
                    .map_err(read_error)?;
            if locked.is_none() {
                return Err(HistoryError::new("RUN_NOT_FOUND", "run not found"));
            }
        }
        for event in events {
            insert_event(&mut transaction, event).await?;
        }
        transaction.commit().await.map_err(write_error)
    }

    async fn put_node_output(&self, output: NodeOutputRecord) -> Result<(), HistoryError> {
        sqlx::query(
            "INSERT INTO node_outputs (run_id, node_id, output, completed_at)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(&output.run_id)
        .bind(&output.node_id)
        .bind(Json(output.output))
        .bind(output.completed_at)
        .execute(&self.pool)
        .await
        .map_err(write_error)?;
        Ok(())
    }

    async fn finish_run(
        &self,
        update: TerminalUpdate,
        event: RunEvent,
    ) -> Result<bool, HistoryError> {
        let expected_event_type = terminal_event_type(update.status).ok_or_else(|| {
            HistoryError::new(
                "HISTORY_EVENT_INVALID",
                "terminal update must contain a terminal status",
            )
        })?;
        if update.run_id != event.run_id {
            return Err(HistoryError::new(
                "HISTORY_EVENT_INVALID",
                "terminal update and event belong to different runs",
            ));
        }
        if event.event_type != expected_event_type || event.node_id.is_some() {
            return Err(HistoryError::new(
                "HISTORY_EVENT_INVALID",
                "terminal event does not match the terminal status",
            ));
        }
        let output = update.output.map(Json);
        let mut transaction = self.pool.begin().await.map_err(write_error)?;
        let result = sqlx::query(
            "UPDATE runs
             SET status = $1, ended_at = $2, updated_at = $2, output = $3,
                 error_code = $4, error_message = $5
             WHERE run_id = $6 AND status IN ($7, $8)",
        )
        .bind(update.status.as_str())
        .bind(update.ended_at)
        .bind(output)
        .bind(&update.error_code)
        .bind(&update.error_message)
        .bind(&update.run_id)
        .bind(RunStatus::Created.as_str())
        .bind(RunStatus::Running.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(write_error)?;
        if result.rows_affected() == 0 {
            transaction.commit().await.map_err(write_error)?;
            return Ok(false);
        }
        insert_event(&mut transaction, &event).await?;
        transaction.commit().await.map_err(write_error)?;
        Ok(true)
    }

    async fn recover_run(
        &self,
        update: TerminalUpdate,
        terminal: RunEvent,
    ) -> Result<RunEvent, HistoryError> {
        validate_recovery_event(&update, &terminal)?;
        match recover_postgres_once(&self.pool, update.clone(), terminal.clone()).await {
            Ok(event) => Ok(event),
            Err(error)
                if matches!(error.code(), "HISTORY_WRITE_FAILED" | "HISTORY_READ_FAILED") =>
            {
                recover_postgres_once(&self.pool, update, terminal).await
            }
            Err(error) => Err(error),
        }
    }

    async fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>, HistoryError> {
        let row = sqlx::query_as::<_, RunRow>(
            "SELECT run_id, request_id, agent_id, agent_version, attachment, status,
                    started_at, ended_at, updated_at, input_summary, output,
                    error_code, error_message
             FROM runs WHERE run_id = $1",
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
             WHERE e.run_id = $1 AND e.seq > $2
             ORDER BY e.seq ASC
             LIMIT $3",
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
            "SELECT r.run_id, COALESCE(MAX(e.seq), 0) AS max_seq
             FROM runs r
             LEFT JOIN run_events e ON e.run_id = r.run_id
             WHERE r.status IN ($1, $2)
             GROUP BY r.run_id
             ORDER BY r.run_id",
        )
        .bind(RunStatus::Created.as_str())
        .bind(RunStatus::Running.as_str())
        .fetch_all(&mut *transaction)
        .await
        .map_err(read_error)?;
        let mut interrupted = 0_u64;
        for row in rows {
            let result = sqlx::query(
                "UPDATE runs
                 SET status = $1, ended_at = $2, updated_at = $2, output = NULL,
                     error_code = $3, error_message = $4
                 WHERE run_id = $5 AND status IN ($6, $7)",
            )
            .bind(RunStatus::Interrupted.as_str())
            .bind(at)
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
            let seq = row.max_seq.checked_add(1).ok_or_else(|| {
                HistoryError::new("HISTORY_DATA_INVALID", "stored event sequence overflowed")
            })?;
            sqlx::query(
                "INSERT INTO run_events (
                    run_id, seq, event_type, node_id, timestamp, code, message, data
                 ) VALUES ($1, $2, $3, NULL, $4, $5, $6, $7)",
            )
            .bind(&row.run_id)
            .bind(seq)
            .bind(RunEventType::RunInterrupted.as_str())
            .bind(at)
            .bind("RUN_INTERRUPTED")
            .bind("run interrupted during startup reconciliation")
            .bind(Json(serde_json::json!({})))
            .execute(&mut *transaction)
            .await
            .map_err(write_error)?;
            interrupted += 1;
        }
        transaction.commit().await.map_err(write_error)?;
        Ok(interrupted)
    }
}

async fn recover_postgres_once(
    pool: &PgPool,
    update: TerminalUpdate,
    mut terminal: RunEvent,
) -> Result<RunEvent, HistoryError> {
    let output = update.output.as_ref().map(Json);
    let mut transaction = pool.begin().await.map_err(write_error)?;
    let status_value: String =
        sqlx::query_scalar("SELECT status FROM runs WHERE run_id = $1 FOR UPDATE")
            .bind(&update.run_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(read_error)?
            .ok_or_else(|| HistoryError::new("RUN_NOT_FOUND", "run not found"))?;
    let status = RunStatus::parse(&status_value)
        .ok_or_else(|| invalid_data(format!("invalid stored run status '{status_value}'")))?;
    if status.is_terminal() {
        ensure_contiguous_events_postgres(&mut transaction, &update.run_id).await?;
        let existing = load_last_event_postgres(&mut transaction, &update.run_id).await?;
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
        sqlx::query_scalar("SELECT COALESCE(MAX(seq), 0) FROM run_events WHERE run_id = $1")
            .bind(&update.run_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(read_error)?;
    let next = maximum
        .checked_add(1)
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| HistoryError::new("HISTORY_DATA_INVALID", "event sequence overflowed"))?;
    terminal.seq = next;
    sqlx::query(
        "UPDATE runs
         SET status = $1, ended_at = $2, updated_at = $2, output = $3,
             error_code = $4, error_message = $5
         WHERE run_id = $6",
    )
    .bind(update.status.as_str())
    .bind(update.ended_at)
    .bind(output)
    .bind(&update.error_code)
    .bind(&update.error_message)
    .bind(&update.run_id)
    .execute(&mut *transaction)
    .await
    .map_err(write_error)?;
    insert_event(&mut transaction, &terminal).await?;
    ensure_contiguous_events_postgres(&mut transaction, &update.run_id).await?;
    transaction.commit().await.map_err(write_error)?;
    Ok(terminal)
}

async fn load_last_event_postgres(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &str,
) -> Result<RunEvent, HistoryError> {
    let row = sqlx::query_as::<_, EventRow>(
        "SELECT e.seq, e.event_type, e.node_id, e.timestamp, e.code, e.message, e.data,
                r.request_id, r.run_id, r.agent_id, r.agent_version
         FROM run_events e
         JOIN runs r ON r.run_id = e.run_id
         WHERE e.run_id = $1
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
    if terminal_event_type(status) != Some(event.event_type) || event.node_id.is_some() {
        return Err(HistoryError::new(
            "HISTORY_TERMINAL_EVENT_MISMATCH",
            "stored terminal event does not match run status",
        ));
    }
    Ok(())
}

async fn ensure_contiguous_events_postgres(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &str,
) -> Result<(), HistoryError> {
    let (count, minimum, maximum) = sqlx::query_as::<_, (i64, i64, i64)>(
        "SELECT COUNT(*), COALESCE(MIN(seq), 0), COALESCE(MAX(seq), 0)
         FROM run_events WHERE run_id = $1",
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
    transaction: &mut Transaction<'_, Postgres>,
    event: &RunEvent,
) -> Result<(), HistoryError> {
    if event.schema_version != EVENT_SCHEMA_VERSION {
        return Err(HistoryError::new(
            "HISTORY_EVENT_INVALID",
            "unsupported event schema version",
        ));
    }
    let seq = to_i64(event.seq, "event sequence is too large")?;
    sqlx::query(
        "INSERT INTO run_events (
            run_id, seq, event_type, node_id, timestamp, code, message, data
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(&event.run_id)
    .bind(seq)
    .bind(event.event_type.as_str())
    .bind(&event.node_id)
    .bind(event.timestamp)
    .bind(&event.code)
    .bind(&event.message)
    .bind(Json(&event.data))
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
    started_at: Option<DateTime<Utc>>,
    ended_at: Option<DateTime<Utc>>,
    updated_at: DateTime<Utc>,
    input_summary: Json<serde_json::Value>,
    output: Option<Json<RunOutput>>,
    error_code: Option<String>,
    error_message: Option<String>,
}

fn run_record_from_row(row: RunRow) -> Result<RunRecord, HistoryError> {
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
        status: RunStatus::parse(&row.status)
            .ok_or_else(|| invalid_data(format!("invalid stored run status '{}'", row.status)))?,
        started_at: row.started_at,
        ended_at: row.ended_at,
        updated_at: row.updated_at,
        input_summary: row.input_summary.0,
        output: row.output.map(|output| output.0),
        error_code: row.error_code,
        error_message: row.error_message,
    })
}

#[derive(Debug, FromRow)]
struct EventRow {
    seq: i64,
    event_type: String,
    node_id: Option<String>,
    timestamp: DateTime<Utc>,
    code: String,
    message: String,
    data: Json<serde_json::Value>,
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
        timestamp: row.timestamp,
        code: row.code,
        message: row.message,
        data: row.data.0,
    })
}

#[derive(Debug, FromRow)]
struct IncompleteRunRow {
    run_id: String,
    max_seq: i64,
}

fn terminal_event_type(status: RunStatus) -> Option<RunEventType> {
    match status {
        RunStatus::Completed => Some(RunEventType::RunCompleted),
        RunStatus::Failed => Some(RunEventType::RunFailed),
        RunStatus::Cancelled => Some(RunEventType::RunCancelled),
        RunStatus::Interrupted => Some(RunEventType::RunInterrupted),
        RunStatus::Created | RunStatus::Running => None,
    }
}

fn to_i64(value: u64, message: &'static str) -> Result<i64, HistoryError> {
    i64::try_from(value).map_err(|_| HistoryError::new("HISTORY_DATA_INVALID", message))
}

fn invalid_data(message: impl Into<String>) -> HistoryError {
    HistoryError::new("HISTORY_DATA_INVALID", message)
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
