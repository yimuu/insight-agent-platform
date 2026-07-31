use super::RepositoryErrorExt as _;

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use insight_durable::mcp_interaction::adapter as interaction_adapter;
use insight_durable::{
    CreateMcpInteractionCommand, McpInteraction, McpInteractionDisposition,
    McpInteractionDurableRepository, McpInteractionId, McpInteractionListFilter,
    McpInteractionMode, McpInteractionOutcome, McpInteractionPrincipal, McpInteractionRequest,
    McpInteractionSecretAuthority, McpInteractionState, McpSecretCiphertext,
    ResolveMcpInteractionCommand, TransitionMcpInteractionCommand,
};
use insight_engine::{
    run_stream::{
        RunInteractionMode, RunInteractionOutcome, RunInteractionState, RunInteractionSummary,
    },
    ContentHash, RunId, RunLifecycle, TransitionOutcome,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use sqlx::{postgres::PgRow, AssertSqlSafe, Postgres, Row, Sqlite, Transaction};

use super::{PostgresDurableRepository, RepositoryError, SqliteDurableRepository};

const MAX_INTERACTION_LIST: u32 = 1_024;
const MAX_INTERACTION_LIST_USIZE: usize = 1_024;
const SQLITE_INTERACTION_COLUMNS: &str = "interaction_id,tenant_id,user_id,run_id,operation_id,server_id,binding_hash,logical_request_key,generation,request_json,interaction_state,outcome,interaction_version,deadline,created_at,updated_at,closed_at";
const POSTGRES_INTERACTION_COLUMNS: &str = "interaction_id,tenant_id,user_id,run_id,operation_id,server_id,binding_hash,logical_request_key,generation,request_json,interaction_state,outcome,interaction_version,deadline,created_at,updated_at,closed_at";

fn intent_hash(value: &impl Serialize) -> Result<String, RepositoryError> {
    let bytes = serde_jcs::to_vec(value).map_err(|_| RepositoryError::canonicalization())?;
    Ok(ContentHash::from_bytes(&bytes).as_str().to_owned())
}

fn enum_wire(value: &impl Serialize) -> Result<String, RepositoryError> {
    serde_json::to_value(value)
        .map_err(|_| RepositoryError::invalid_data())?
        .as_str()
        .map(str::to_owned)
        .ok_or_else(RepositoryError::invalid_data)
}

fn parse_enum<T: DeserializeOwned>(value: String) -> Result<T, RepositoryError> {
    serde_json::from_value(Value::String(value)).map_err(|_| RepositoryError::invalid_data())
}

fn sqlite_time(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn parse_sqlite_time(value: String) -> Result<DateTime<Utc>, RepositoryError> {
    DateTime::parse_from_rfc3339(&value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| RepositoryError::invalid_data())
}

fn parse_sqlite_interaction(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<McpInteraction, RepositoryError> {
    Ok(interaction_adapter::interaction_from_storage(
        McpInteractionId::new(
            row.try_get::<String, _>("interaction_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        McpInteractionPrincipal::new(
            row.try_get::<String, _>("tenant_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get::<String, _>("user_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        row.try_get("run_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("operation_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("server_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("binding_hash")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("logical_request_key")
            .map_err(|_| RepositoryError::invalid_data())?,
        u32::try_from(
            row.try_get::<i64, _>("generation")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        serde_json::from_str::<McpInteractionRequest>(
            &row.try_get::<String, _>("request_json")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        parse_enum(
            row.try_get("interaction_state")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        row.try_get::<Option<String>, _>("outcome")
            .map_err(|_| RepositoryError::invalid_data())?
            .map(parse_enum)
            .transpose()?,
        u64::try_from(
            row.try_get::<i64, _>("interaction_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        parse_sqlite_time(
            row.try_get("deadline")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        parse_sqlite_time(
            row.try_get("created_at")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        parse_sqlite_time(
            row.try_get("updated_at")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        row.try_get::<Option<String>, _>("closed_at")
            .map_err(|_| RepositoryError::invalid_data())?
            .map(parse_sqlite_time)
            .transpose()?,
    ))
}

fn parse_postgres_interaction(row: &PgRow) -> Result<McpInteraction, RepositoryError> {
    Ok(interaction_adapter::interaction_from_storage(
        McpInteractionId::new(
            row.try_get::<String, _>("interaction_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        McpInteractionPrincipal::new(
            row.try_get::<String, _>("tenant_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get::<String, _>("user_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        row.try_get("run_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("operation_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("server_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("binding_hash")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("logical_request_key")
            .map_err(|_| RepositoryError::invalid_data())?,
        u32::try_from(
            row.try_get::<i64, _>("generation")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        serde_json::from_value::<McpInteractionRequest>(
            row.try_get("request_json")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        parse_enum(
            row.try_get("interaction_state")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        row.try_get::<Option<String>, _>("outcome")
            .map_err(|_| RepositoryError::invalid_data())?
            .map(parse_enum)
            .transpose()?,
        u64::try_from(
            row.try_get::<i64, _>("interaction_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("deadline")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("created_at")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("updated_at")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("closed_at")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))
}

async fn load_sqlite_interaction(
    tx: &mut Transaction<'_, Sqlite>,
    interaction_id: &McpInteractionId,
) -> Result<Option<McpInteraction>, RepositoryError> {
    let query =
        format!("SELECT {SQLITE_INTERACTION_COLUMNS} FROM mcp_interactions WHERE interaction_id=?");
    sqlx::query(AssertSqlSafe(query))
        .bind(interaction_id.as_str())
        .fetch_optional(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(parse_sqlite_interaction)
        .transpose()
}

async fn load_postgres_interaction(
    tx: &mut Transaction<'_, Postgres>,
    interaction_id: &McpInteractionId,
    for_update: bool,
) -> Result<Option<McpInteraction>, RepositoryError> {
    let suffix = if for_update { " FOR UPDATE" } else { "" };
    let query = format!(
        "SELECT {POSTGRES_INTERACTION_COLUMNS} FROM mcp_interactions WHERE interaction_id=$1{suffix}"
    );
    sqlx::query(AssertSqlSafe(query))
        .bind(interaction_id.as_str())
        .fetch_optional(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(parse_postgres_interaction)
        .transpose()
}

fn terminal_resolution(
    disposition: McpInteractionDisposition,
) -> (McpInteractionState, McpInteractionOutcome) {
    match disposition {
        McpInteractionDisposition::Accept => (
            McpInteractionState::Responded,
            McpInteractionOutcome::Accepted,
        ),
        McpInteractionDisposition::Decline => {
            (McpInteractionState::Closed, McpInteractionOutcome::Declined)
        }
        McpInteractionDisposition::Cancel => (
            McpInteractionState::Closed,
            McpInteractionOutcome::Cancelled,
        ),
    }
}

fn run_accepts_new_interactions(lifecycle: String) -> Result<bool, RepositoryError> {
    Ok(!parse_enum::<RunLifecycle>(lifecycle)?.is_terminal())
}

const fn project_interaction_mode(mode: McpInteractionMode) -> RunInteractionMode {
    match mode {
        McpInteractionMode::Form => RunInteractionMode::Form,
        McpInteractionMode::Url => RunInteractionMode::Url,
        McpInteractionMode::Approval => RunInteractionMode::Approval,
        McpInteractionMode::Authorization => RunInteractionMode::Authorization,
    }
}

const fn project_interaction_state(state: McpInteractionState) -> RunInteractionState {
    match state {
        McpInteractionState::Requested => RunInteractionState::Requested,
        McpInteractionState::Responded => RunInteractionState::Responded,
        McpInteractionState::Retrying => RunInteractionState::Retrying,
        McpInteractionState::Closed => RunInteractionState::Closed,
    }
}

const fn project_interaction_outcome(outcome: McpInteractionOutcome) -> RunInteractionOutcome {
    match outcome {
        McpInteractionOutcome::Accepted => RunInteractionOutcome::Accepted,
        McpInteractionOutcome::Declined => RunInteractionOutcome::Declined,
        McpInteractionOutcome::Cancelled => RunInteractionOutcome::Cancelled,
        McpInteractionOutcome::Expired => RunInteractionOutcome::Expired,
        McpInteractionOutcome::RunTerminal => RunInteractionOutcome::RunTerminal,
        McpInteractionOutcome::RetryCompleted => RunInteractionOutcome::RetryCompleted,
        McpInteractionOutcome::RetryFailed => RunInteractionOutcome::RetryFailed,
    }
}

fn terminal_interaction_summary(
    interaction: &McpInteraction,
) -> Result<RunInteractionSummary, RepositoryError> {
    RunInteractionSummary::new(
        interaction.interaction_id().as_str(),
        interaction.server_id(),
        project_interaction_mode(interaction.request().mode()),
        project_interaction_state(interaction.state()),
        interaction.outcome().map(project_interaction_outcome),
        interaction.deadline(),
    )
    .map_err(|_| RepositoryError::invalid_data())
}

pub(crate) async fn close_and_load_terminal_interactions_sqlite(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
) -> Result<Vec<RunInteractionSummary>, RepositoryError> {
    let (lifecycle, terminal_at) = sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT lifecycle,strftime('%Y-%m-%dT%H:%M:%fZ',terminal_at)
         FROM workflow_runs WHERE run_id=?",
    )
    .bind(run_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    if run_accepts_new_interactions(lifecycle)? {
        return Err(RepositoryError::invalid_data());
    }
    let terminal_at = sqlite_time(parse_sqlite_time(
        terminal_at.ok_or_else(RepositoryError::invalid_data)?,
    )?);
    let query = format!(
        "SELECT {SQLITE_INTERACTION_COLUMNS} FROM mcp_interactions
         WHERE run_id=? ORDER BY interaction_id COLLATE BINARY LIMIT ?"
    );
    let rows = sqlx::query(AssertSqlSafe(query.clone()))
        .bind(run_id.as_str())
        .bind(i64::from(MAX_INTERACTION_LIST) + 1)
        .fetch_all(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    if rows.len() > MAX_INTERACTION_LIST_USIZE {
        return Err(RepositoryError::invalid_data());
    }
    let interactions = rows
        .iter()
        .map(parse_sqlite_interaction)
        .collect::<Result<Vec<_>, _>>()?;
    let open_count = interactions
        .iter()
        .filter(|interaction| interaction.state() != McpInteractionState::Closed)
        .count();
    if interactions.iter().any(|interaction| {
        interaction.state() != McpInteractionState::Closed
            && i64::try_from(interaction.version()) == Ok(i64::MAX)
    }) {
        return Err(RepositoryError::invalid_data());
    }
    let updated = sqlx::query(
        "UPDATE mcp_interactions
         SET interaction_state='closed',outcome='run_terminal',
             interaction_version=interaction_version+1,updated_at=?,closed_at=?
         WHERE run_id=? AND interaction_state<>'closed'",
    )
    .bind(&terminal_at)
    .bind(&terminal_at)
    .bind(run_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if updated != u64::try_from(open_count).map_err(|_| RepositoryError::invalid_data())? {
        return Err(RepositoryError::invalid_data());
    }
    let rows = sqlx::query(AssertSqlSafe(query))
        .bind(run_id.as_str())
        .bind(i64::from(MAX_INTERACTION_LIST) + 1)
        .fetch_all(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    if rows.len() != interactions.len() {
        return Err(RepositoryError::invalid_data());
    }
    rows.iter()
        .map(parse_sqlite_interaction)
        .map(|interaction| {
            let interaction = interaction?;
            if interaction.state() != McpInteractionState::Closed {
                return Err(RepositoryError::invalid_data());
            }
            terminal_interaction_summary(&interaction)
        })
        .collect()
}

pub(crate) async fn close_and_load_terminal_interactions_postgres(
    transaction: &mut Transaction<'_, Postgres>,
    run_id: &RunId,
) -> Result<Vec<RunInteractionSummary>, RepositoryError> {
    let (lifecycle, terminal_at) = sqlx::query_as::<_, (String, Option<DateTime<Utc>>)>(
        "SELECT lifecycle,terminal_at FROM workflow_runs WHERE run_id=$1 FOR UPDATE",
    )
    .bind(run_id.as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .ok_or_else(RepositoryError::invalid_data)?;
    if run_accepts_new_interactions(lifecycle)? {
        return Err(RepositoryError::invalid_data());
    }
    let terminal_at = terminal_at.ok_or_else(RepositoryError::invalid_data)?;
    let locked_query = format!(
        "SELECT {POSTGRES_INTERACTION_COLUMNS} FROM mcp_interactions
         WHERE run_id=$1 ORDER BY interaction_id COLLATE \"C\" LIMIT $2 FOR UPDATE"
    );
    let rows = sqlx::query(AssertSqlSafe(locked_query))
        .bind(run_id.as_str())
        .bind(i64::from(MAX_INTERACTION_LIST) + 1)
        .fetch_all(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    if rows.len() > MAX_INTERACTION_LIST_USIZE {
        return Err(RepositoryError::invalid_data());
    }
    let interactions = rows
        .iter()
        .map(parse_postgres_interaction)
        .collect::<Result<Vec<_>, _>>()?;
    let open_count = interactions
        .iter()
        .filter(|interaction| interaction.state() != McpInteractionState::Closed)
        .count();
    if interactions.iter().any(|interaction| {
        interaction.state() != McpInteractionState::Closed
            && i64::try_from(interaction.version()) == Ok(i64::MAX)
    }) {
        return Err(RepositoryError::invalid_data());
    }
    let updated = sqlx::query(
        "UPDATE mcp_interactions
         SET interaction_state='closed',outcome='run_terminal',
             interaction_version=interaction_version+1,
             updated_at=$1,closed_at=$1
         WHERE run_id=$2 AND interaction_state<>'closed'",
    )
    .bind(terminal_at)
    .bind(run_id.as_str())
    .execute(&mut **transaction)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if updated != u64::try_from(open_count).map_err(|_| RepositoryError::invalid_data())? {
        return Err(RepositoryError::invalid_data());
    }
    let query = format!(
        "SELECT {POSTGRES_INTERACTION_COLUMNS} FROM mcp_interactions
         WHERE run_id=$1 ORDER BY interaction_id COLLATE \"C\" LIMIT $2"
    );
    let rows = sqlx::query(AssertSqlSafe(query))
        .bind(run_id.as_str())
        .bind(i64::from(MAX_INTERACTION_LIST) + 1)
        .fetch_all(&mut **transaction)
        .await
        .map_err(RepositoryError::storage)?;
    if rows.len() != interactions.len() {
        return Err(RepositoryError::invalid_data());
    }
    rows.iter()
        .map(parse_postgres_interaction)
        .map(|interaction| {
            let interaction = interaction?;
            if interaction.state() != McpInteractionState::Closed {
                return Err(RepositoryError::invalid_data());
            }
            terminal_interaction_summary(&interaction)
        })
        .collect()
}

#[async_trait]
impl McpInteractionDurableRepository for SqliteDurableRepository {
    async fn load_mcp_run_principal(
        &self,
        run_id: &str,
    ) -> Result<Option<McpInteractionPrincipal>, RepositoryError> {
        if let Some((tenant_id, user_id)) = sqlx::query_as::<_, (String, String)>(
            "SELECT tenant_id,user_id FROM full_conversation_turns WHERE run_id=?",
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        {
            return McpInteractionPrincipal::new(tenant_id, user_id).map(Some);
        }
        let exists =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM workflow_runs WHERE run_id=?")
                .bind(run_id)
                .fetch_one(&self.pool)
                .await
                .map_err(RepositoryError::storage)?;
        (exists == 1)
            .then(|| McpInteractionPrincipal::new("default", "service"))
            .transpose()
    }

    async fn create_mcp_interaction(
        &self,
        command: CreateMcpInteractionCommand,
    ) -> Result<TransitionOutcome<McpInteraction>, RepositoryError> {
        let hash = intent_hash(&command)?;
        let interaction = command.interaction();
        let request_json = serde_jcs::to_string(interaction.request())
            .map_err(|_| RepositoryError::canonicalization())?;
        let _writer = self.writer.lock().await;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        if let Some(existing_hash) = sqlx::query_scalar::<_, String>(
            "SELECT creation_intent_hash FROM mcp_interactions WHERE interaction_id=?",
        )
        .bind(interaction.interaction_id().as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        {
            if existing_hash != hash {
                tx.rollback().await.map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::StateConflict);
            }
            let authoritative = load_sqlite_interaction(&mut tx, interaction.interaction_id())
                .await?
                .ok_or_else(RepositoryError::invalid_data)?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay { authoritative });
        }
        let lifecycle =
            sqlx::query_scalar::<_, String>("SELECT lifecycle FROM workflow_runs WHERE run_id=?")
                .bind(interaction.run_id())
                .fetch_optional(&mut *tx)
                .await
                .map_err(RepositoryError::storage)?;
        if lifecycle.map(run_accepts_new_interactions).transpose()? != Some(true) {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let interaction_count =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM mcp_interactions WHERE run_id=?")
                .bind(interaction.run_id())
                .fetch_one(&mut *tx)
                .await
                .map_err(RepositoryError::storage)?;
        if interaction_count < 0 || interaction_count >= i64::from(MAX_INTERACTION_LIST) {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let rows = sqlx::query(
            "INSERT OR IGNORE INTO mcp_interactions(
               interaction_id,tenant_id,user_id,run_id,operation_id,server_id,binding_hash,
               logical_request_key,generation,request_json,interaction_state,outcome,
               interaction_version,deadline,created_at,updated_at,closed_at,creation_intent_hash
             ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(interaction.interaction_id().as_str())
        .bind(interaction.principal().tenant_id())
        .bind(interaction.principal().user_id())
        .bind(interaction.run_id())
        .bind(interaction.operation_id())
        .bind(interaction.server_id())
        .bind(interaction.binding_hash())
        .bind(interaction.logical_request_key())
        .bind(i64::from(interaction.generation()))
        .bind(request_json)
        .bind(enum_wire(&interaction.state())?)
        .bind(
            interaction
                .outcome()
                .map(|value| enum_wire(&value))
                .transpose()?,
        )
        .bind(i64::try_from(interaction.version()).map_err(|_| RepositoryError::invalid_data())?)
        .bind(sqlite_time(interaction.deadline()))
        .bind(sqlite_time(interaction.created_at()))
        .bind(sqlite_time(interaction.updated_at()))
        .bind(interaction.closed_at().map(sqlite_time))
        .bind(&hash)
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if rows == 1 {
            sqlx::query(
                "INSERT INTO mcp_interaction_secrets(
                   interaction_id,request_ciphertext,request_secret_hash
                 ) VALUES(?,?,?)",
            )
            .bind(interaction.interaction_id().as_str())
            .bind(command.request_secret().expose_ciphertext())
            .bind(command.request_secret_hash())
            .execute(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?;
            let stored = load_sqlite_interaction(&mut tx, interaction.interaction_id())
                .await?
                .ok_or_else(RepositoryError::invalid_data)?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::Committed { result: stored });
        }
        let existing_hash = sqlx::query_scalar::<_, String>(
            "SELECT creation_intent_hash FROM mcp_interactions WHERE interaction_id=?",
        )
        .bind(interaction.interaction_id().as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(existing_hash) = existing_hash else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        if existing_hash != hash {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let authoritative = load_sqlite_interaction(&mut tx, interaction.interaction_id())
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::ExactReplay { authoritative })
    }

    async fn load_mcp_interaction(
        &self,
        interaction_id: &McpInteractionId,
    ) -> Result<Option<McpInteraction>, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let result = load_sqlite_interaction(&mut tx, interaction_id).await?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(result)
    }

    async fn list_mcp_interactions(
        &self,
        principal: &McpInteractionPrincipal,
        filter: &McpInteractionListFilter,
        limit: u32,
    ) -> Result<Vec<McpInteraction>, RepositoryError> {
        if limit == 0 || limit > MAX_INTERACTION_LIST {
            return Err(RepositoryError::invalid_data());
        }
        let state = filter.state.map(|value| enum_wire(&value)).transpose()?;
        let query = format!(
            "SELECT {SQLITE_INTERACTION_COLUMNS} FROM mcp_interactions
             WHERE tenant_id=? AND user_id=?
               AND (? IS NULL OR run_id=?)
               AND (? IS NULL OR interaction_state=?)
               AND (? IS NULL OR interaction_id>?)
             ORDER BY interaction_id LIMIT ?"
        );
        let rows = sqlx::query(AssertSqlSafe(query))
            .bind(principal.tenant_id())
            .bind(principal.user_id())
            .bind(filter.run_id.as_deref())
            .bind(filter.run_id.as_deref())
            .bind(state.as_deref())
            .bind(state.as_deref())
            .bind(filter.after_interaction_id.as_deref())
            .bind(filter.after_interaction_id.as_deref())
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await
            .map_err(RepositoryError::storage)?;
        rows.iter().map(parse_sqlite_interaction).collect()
    }

    async fn resolve_mcp_interaction(
        &self,
        command: ResolveMcpInteractionCommand,
    ) -> Result<TransitionOutcome<McpInteraction>, RepositoryError> {
        let hash = intent_hash(&command)?;
        let _writer = self.writer.lock().await;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        if let Some((stored_hash, _)) = sqlx::query_as::<_, (String, i64)>(
            "SELECT intent_hash,result_version FROM mcp_interaction_transition_receipts
             WHERE interaction_id=? AND request_id=?",
        )
        .bind(command.interaction_id().as_str())
        .bind(command.request_id())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        {
            if stored_hash != hash {
                tx.rollback().await.map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::StateConflict);
            }
            let authoritative = load_sqlite_interaction(&mut tx, command.interaction_id())
                .await?
                .ok_or_else(RepositoryError::invalid_data)?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay { authoritative });
        }
        let current = load_sqlite_interaction(&mut tx, command.interaction_id())
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        if current.principal() != command.principal()
            || current.state() != McpInteractionState::Requested
            || current.version() != command.expected_version()
            || command.responded_at() > current.deadline()
        {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let (state, outcome) = terminal_resolution(command.disposition());
        let version = current.version().saturating_add(1);
        let closed_at =
            (state == McpInteractionState::Closed).then(|| sqlite_time(command.responded_at()));
        let rows = sqlx::query(
            "UPDATE mcp_interactions SET interaction_state=?,outcome=?,
                interaction_version=?,updated_at=?,closed_at=?
             WHERE interaction_id=? AND interaction_state='requested'
               AND interaction_version=?",
        )
        .bind(enum_wire(&state)?)
        .bind(enum_wire(&outcome)?)
        .bind(i64::try_from(version).map_err(|_| RepositoryError::invalid_data())?)
        .bind(sqlite_time(command.responded_at()))
        .bind(closed_at)
        .bind(command.interaction_id().as_str())
        .bind(
            i64::try_from(command.expected_version())
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if rows != 1 {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        if let Some(secret) = command.response_secret() {
            sqlx::query(
                "UPDATE mcp_interaction_secrets
                 SET response_ciphertext=?,response_hash=? WHERE interaction_id=?",
            )
            .bind(secret.expose_ciphertext())
            .bind(command.response_hash())
            .bind(command.interaction_id().as_str())
            .execute(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?;
        }
        sqlx::query(
            "INSERT INTO mcp_interaction_transition_receipts(
               interaction_id,request_id,intent_hash,result_version,created_at
             ) VALUES(?,?,?,?,?)",
        )
        .bind(command.interaction_id().as_str())
        .bind(command.request_id())
        .bind(&hash)
        .bind(i64::try_from(version).map_err(|_| RepositoryError::invalid_data())?)
        .bind(sqlite_time(command.responded_at()))
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let result = load_sqlite_interaction(&mut tx, command.interaction_id())
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result })
    }

    async fn transition_mcp_interaction(
        &self,
        command: TransitionMcpInteractionCommand,
    ) -> Result<TransitionOutcome<McpInteraction>, RepositoryError> {
        let hash = intent_hash(&command)?;
        let _writer = self.writer.lock().await;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        if let Some(stored_hash) = sqlx::query_scalar::<_, String>(
            "SELECT intent_hash FROM mcp_interaction_transition_receipts
             WHERE interaction_id=? AND request_id=?",
        )
        .bind(command.interaction_id().as_str())
        .bind(command.request_id())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        {
            if stored_hash != hash {
                tx.rollback().await.map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::StateConflict);
            }
            let authoritative = load_sqlite_interaction(&mut tx, command.interaction_id())
                .await?
                .ok_or_else(RepositoryError::invalid_data)?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay { authoritative });
        }
        let current = load_sqlite_interaction(&mut tx, command.interaction_id())
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        if current.version() != command.expected_version()
            || current.state() == McpInteractionState::Closed
            || (command.outcome().is_none() && current.state() != McpInteractionState::Responded)
        {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let (state, outcome, closed_at) = match command.outcome() {
            None => (McpInteractionState::Retrying, current.outcome(), None),
            Some(outcome) => (
                McpInteractionState::Closed,
                Some(outcome),
                Some(sqlite_time(command.transitioned_at())),
            ),
        };
        let version = current.version().saturating_add(1);
        let rows = sqlx::query(
            "UPDATE mcp_interactions SET interaction_state=?,outcome=?,
                interaction_version=?,updated_at=?,closed_at=?
             WHERE interaction_id=? AND interaction_version=?
               AND interaction_state<>'closed'",
        )
        .bind(enum_wire(&state)?)
        .bind(outcome.map(|value| enum_wire(&value)).transpose()?)
        .bind(i64::try_from(version).map_err(|_| RepositoryError::invalid_data())?)
        .bind(sqlite_time(command.transitioned_at()))
        .bind(closed_at)
        .bind(command.interaction_id().as_str())
        .bind(
            i64::try_from(command.expected_version())
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if rows != 1 {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        sqlx::query(
            "INSERT INTO mcp_interaction_transition_receipts(
               interaction_id,request_id,intent_hash,result_version,created_at
             ) VALUES(?,?,?,?,?)",
        )
        .bind(command.interaction_id().as_str())
        .bind(command.request_id())
        .bind(&hash)
        .bind(i64::try_from(version).map_err(|_| RepositoryError::invalid_data())?)
        .bind(sqlite_time(command.transitioned_at()))
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let result = load_sqlite_interaction(&mut tx, command.interaction_id())
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result })
    }

    async fn load_mcp_interaction_secret(
        &self,
        interaction_id: &McpInteractionId,
    ) -> Result<Option<McpInteractionSecretAuthority>, RepositoryError> {
        sqlx::query_as::<_, (String, String, Option<String>, Option<String>)>(
            "SELECT request_ciphertext,request_secret_hash,response_ciphertext,response_hash
             FROM mcp_interaction_secrets WHERE interaction_id=?",
        )
        .bind(interaction_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .map(|(request, request_hash, response, response_hash)| {
            Ok(McpInteractionSecretAuthority {
                request_secret: McpSecretCiphertext::new(request)?,
                request_hash,
                response_secret: response.map(McpSecretCiphertext::new).transpose()?,
                response_hash,
            })
        })
        .transpose()
    }

    async fn list_mcp_interactions_ready_for_retry(
        &self,
        limit: u32,
    ) -> Result<Vec<McpInteraction>, RepositoryError> {
        if limit == 0 || limit > MAX_INTERACTION_LIST {
            return Err(RepositoryError::invalid_data());
        }
        let query = format!(
            "SELECT {SQLITE_INTERACTION_COLUMNS} FROM mcp_interactions
             WHERE interaction_state='responded'
             ORDER BY updated_at,interaction_id LIMIT ?"
        );
        let rows = sqlx::query(AssertSqlSafe(query))
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await
            .map_err(RepositoryError::storage)?;
        rows.iter().map(parse_sqlite_interaction).collect()
    }
}

#[async_trait]
impl McpInteractionDurableRepository for PostgresDurableRepository {
    async fn load_mcp_run_principal(
        &self,
        run_id: &str,
    ) -> Result<Option<McpInteractionPrincipal>, RepositoryError> {
        if let Some((tenant_id, user_id)) = sqlx::query_as::<_, (String, String)>(
            "SELECT tenant_id,user_id FROM full_conversation_turns WHERE run_id=$1",
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        {
            return McpInteractionPrincipal::new(tenant_id, user_id).map(Some);
        }
        let exists =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM workflow_runs WHERE run_id=$1")
                .bind(run_id)
                .fetch_one(&self.pool)
                .await
                .map_err(RepositoryError::storage)?;
        (exists == 1)
            .then(|| McpInteractionPrincipal::new("default", "service"))
            .transpose()
    }

    async fn create_mcp_interaction(
        &self,
        command: CreateMcpInteractionCommand,
    ) -> Result<TransitionOutcome<McpInteraction>, RepositoryError> {
        let hash = intent_hash(&command)?;
        let interaction = command.interaction();
        let request_json = serde_json::to_value(interaction.request())
            .map_err(|_| RepositoryError::canonicalization())?;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        if let Some(existing_hash) = sqlx::query_scalar::<_, String>(
            "SELECT creation_intent_hash FROM mcp_interactions WHERE interaction_id=$1",
        )
        .bind(interaction.interaction_id().as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        {
            if existing_hash != hash {
                tx.rollback().await.map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::StateConflict);
            }
            let authoritative =
                load_postgres_interaction(&mut tx, interaction.interaction_id(), false)
                    .await?
                    .ok_or_else(RepositoryError::invalid_data)?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay { authoritative });
        }
        let lifecycle = sqlx::query_scalar::<_, String>(
            "SELECT lifecycle FROM workflow_runs WHERE run_id=$1 FOR UPDATE",
        )
        .bind(interaction.run_id())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        if let Some(existing_hash) = sqlx::query_scalar::<_, String>(
            "SELECT creation_intent_hash FROM mcp_interactions WHERE interaction_id=$1",
        )
        .bind(interaction.interaction_id().as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        {
            if existing_hash != hash {
                tx.rollback().await.map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::StateConflict);
            }
            let authoritative =
                load_postgres_interaction(&mut tx, interaction.interaction_id(), false)
                    .await?
                    .ok_or_else(RepositoryError::invalid_data)?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay { authoritative });
        }
        if lifecycle.map(run_accepts_new_interactions).transpose()? != Some(true) {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let interaction_count =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM mcp_interactions WHERE run_id=$1")
                .bind(interaction.run_id())
                .fetch_one(&mut *tx)
                .await
                .map_err(RepositoryError::storage)?;
        if interaction_count < 0 || interaction_count >= i64::from(MAX_INTERACTION_LIST) {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let rows = sqlx::query(
            "INSERT INTO mcp_interactions(
               interaction_id,tenant_id,user_id,run_id,operation_id,server_id,binding_hash,
               logical_request_key,generation,request_json,interaction_state,outcome,
               interaction_version,deadline,created_at,updated_at,closed_at,creation_intent_hash
             ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
             ON CONFLICT DO NOTHING",
        )
        .bind(interaction.interaction_id().as_str())
        .bind(interaction.principal().tenant_id())
        .bind(interaction.principal().user_id())
        .bind(interaction.run_id())
        .bind(interaction.operation_id())
        .bind(interaction.server_id())
        .bind(interaction.binding_hash())
        .bind(interaction.logical_request_key())
        .bind(i64::from(interaction.generation()))
        .bind(request_json)
        .bind(enum_wire(&interaction.state())?)
        .bind(
            interaction
                .outcome()
                .map(|value| enum_wire(&value))
                .transpose()?,
        )
        .bind(i64::try_from(interaction.version()).map_err(|_| RepositoryError::invalid_data())?)
        .bind(interaction.deadline())
        .bind(interaction.created_at())
        .bind(interaction.updated_at())
        .bind(interaction.closed_at())
        .bind(&hash)
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if rows == 1 {
            sqlx::query(
                "INSERT INTO mcp_interaction_secrets(
                   interaction_id,request_ciphertext,request_secret_hash
                 ) VALUES($1,$2,$3)",
            )
            .bind(interaction.interaction_id().as_str())
            .bind(command.request_secret().expose_ciphertext())
            .bind(command.request_secret_hash())
            .execute(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?;
            let stored = load_postgres_interaction(&mut tx, interaction.interaction_id(), false)
                .await?
                .ok_or_else(RepositoryError::invalid_data)?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::Committed { result: stored });
        }
        let existing_hash = sqlx::query_scalar::<_, String>(
            "SELECT creation_intent_hash FROM mcp_interactions WHERE interaction_id=$1",
        )
        .bind(interaction.interaction_id().as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let Some(existing_hash) = existing_hash else {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        if existing_hash != hash {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let authoritative = load_postgres_interaction(&mut tx, interaction.interaction_id(), false)
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::ExactReplay { authoritative })
    }

    async fn load_mcp_interaction(
        &self,
        interaction_id: &McpInteractionId,
    ) -> Result<Option<McpInteraction>, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let result = load_postgres_interaction(&mut tx, interaction_id, false).await?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(result)
    }

    async fn list_mcp_interactions(
        &self,
        principal: &McpInteractionPrincipal,
        filter: &McpInteractionListFilter,
        limit: u32,
    ) -> Result<Vec<McpInteraction>, RepositoryError> {
        if limit == 0 || limit > MAX_INTERACTION_LIST {
            return Err(RepositoryError::invalid_data());
        }
        let state = filter.state.map(|value| enum_wire(&value)).transpose()?;
        let query = format!(
            "SELECT {POSTGRES_INTERACTION_COLUMNS} FROM mcp_interactions
             WHERE tenant_id=$1 AND user_id=$2
               AND ($3::text IS NULL OR run_id=$3)
               AND ($4::text IS NULL OR interaction_state=$4)
               AND ($5::text IS NULL OR interaction_id>$5)
             ORDER BY interaction_id LIMIT $6"
        );
        let rows = sqlx::query(AssertSqlSafe(query))
            .bind(principal.tenant_id())
            .bind(principal.user_id())
            .bind(filter.run_id.as_deref())
            .bind(state.as_deref())
            .bind(filter.after_interaction_id.as_deref())
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await
            .map_err(RepositoryError::storage)?;
        rows.iter().map(parse_postgres_interaction).collect()
    }

    async fn resolve_mcp_interaction(
        &self,
        command: ResolveMcpInteractionCommand,
    ) -> Result<TransitionOutcome<McpInteraction>, RepositoryError> {
        let hash = intent_hash(&command)?;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        if let Some(stored_hash) = sqlx::query_scalar::<_, String>(
            "SELECT intent_hash FROM mcp_interaction_transition_receipts
             WHERE interaction_id=$1 AND request_id=$2",
        )
        .bind(command.interaction_id().as_str())
        .bind(command.request_id())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        {
            if stored_hash != hash {
                tx.rollback().await.map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::StateConflict);
            }
            let authoritative = load_postgres_interaction(&mut tx, command.interaction_id(), false)
                .await?
                .ok_or_else(RepositoryError::invalid_data)?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay { authoritative });
        }
        let current = load_postgres_interaction(&mut tx, command.interaction_id(), true)
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        if current.principal() != command.principal()
            || current.state() != McpInteractionState::Requested
            || current.version() != command.expected_version()
            || command.responded_at() > current.deadline()
        {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let (state, outcome) = terminal_resolution(command.disposition());
        let version = current.version().saturating_add(1);
        let closed_at = (state == McpInteractionState::Closed).then_some(command.responded_at());
        let rows = sqlx::query(
            "UPDATE mcp_interactions SET interaction_state=$1,outcome=$2,
                interaction_version=$3,updated_at=$4,closed_at=$5
             WHERE interaction_id=$6 AND interaction_state='requested'
               AND interaction_version=$7",
        )
        .bind(enum_wire(&state)?)
        .bind(enum_wire(&outcome)?)
        .bind(i64::try_from(version).map_err(|_| RepositoryError::invalid_data())?)
        .bind(command.responded_at())
        .bind(closed_at)
        .bind(command.interaction_id().as_str())
        .bind(
            i64::try_from(command.expected_version())
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if rows != 1 {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        if let Some(secret) = command.response_secret() {
            sqlx::query(
                "UPDATE mcp_interaction_secrets
                 SET response_ciphertext=$1,response_hash=$2 WHERE interaction_id=$3",
            )
            .bind(secret.expose_ciphertext())
            .bind(command.response_hash())
            .bind(command.interaction_id().as_str())
            .execute(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?;
        }
        sqlx::query(
            "INSERT INTO mcp_interaction_transition_receipts(
               interaction_id,request_id,intent_hash,result_version,created_at
             ) VALUES($1,$2,$3,$4,$5)",
        )
        .bind(command.interaction_id().as_str())
        .bind(command.request_id())
        .bind(&hash)
        .bind(i64::try_from(version).map_err(|_| RepositoryError::invalid_data())?)
        .bind(command.responded_at())
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let result = load_postgres_interaction(&mut tx, command.interaction_id(), false)
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result })
    }

    async fn transition_mcp_interaction(
        &self,
        command: TransitionMcpInteractionCommand,
    ) -> Result<TransitionOutcome<McpInteraction>, RepositoryError> {
        let hash = intent_hash(&command)?;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        if let Some(stored_hash) = sqlx::query_scalar::<_, String>(
            "SELECT intent_hash FROM mcp_interaction_transition_receipts
             WHERE interaction_id=$1 AND request_id=$2",
        )
        .bind(command.interaction_id().as_str())
        .bind(command.request_id())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        {
            if stored_hash != hash {
                tx.rollback().await.map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::StateConflict);
            }
            let authoritative = load_postgres_interaction(&mut tx, command.interaction_id(), false)
                .await?
                .ok_or_else(RepositoryError::invalid_data)?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::ExactReplay { authoritative });
        }
        let current = load_postgres_interaction(&mut tx, command.interaction_id(), true)
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        if current.version() != command.expected_version()
            || current.state() == McpInteractionState::Closed
            || (command.outcome().is_none() && current.state() != McpInteractionState::Responded)
        {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let (state, outcome, closed_at) = match command.outcome() {
            None => (McpInteractionState::Retrying, current.outcome(), None),
            Some(outcome) => (
                McpInteractionState::Closed,
                Some(outcome),
                Some(command.transitioned_at()),
            ),
        };
        let version = current.version().saturating_add(1);
        let rows = sqlx::query(
            "UPDATE mcp_interactions SET interaction_state=$1,outcome=$2,
                interaction_version=$3,updated_at=$4,closed_at=$5
             WHERE interaction_id=$6 AND interaction_version=$7
               AND interaction_state<>'closed'",
        )
        .bind(enum_wire(&state)?)
        .bind(outcome.map(|value| enum_wire(&value)).transpose()?)
        .bind(i64::try_from(version).map_err(|_| RepositoryError::invalid_data())?)
        .bind(command.transitioned_at())
        .bind(closed_at)
        .bind(command.interaction_id().as_str())
        .bind(
            i64::try_from(command.expected_version())
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if rows != 1 {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        sqlx::query(
            "INSERT INTO mcp_interaction_transition_receipts(
               interaction_id,request_id,intent_hash,result_version,created_at
             ) VALUES($1,$2,$3,$4,$5)",
        )
        .bind(command.interaction_id().as_str())
        .bind(command.request_id())
        .bind(&hash)
        .bind(i64::try_from(version).map_err(|_| RepositoryError::invalid_data())?)
        .bind(command.transitioned_at())
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let result = load_postgres_interaction(&mut tx, command.interaction_id(), false)
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result })
    }

    async fn load_mcp_interaction_secret(
        &self,
        interaction_id: &McpInteractionId,
    ) -> Result<Option<McpInteractionSecretAuthority>, RepositoryError> {
        sqlx::query_as::<_, (String, String, Option<String>, Option<String>)>(
            "SELECT request_ciphertext,request_secret_hash,response_ciphertext,response_hash
             FROM mcp_interaction_secrets WHERE interaction_id=$1",
        )
        .bind(interaction_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .map(|(request, request_hash, response, response_hash)| {
            Ok(McpInteractionSecretAuthority {
                request_secret: McpSecretCiphertext::new(request)?,
                request_hash,
                response_secret: response.map(McpSecretCiphertext::new).transpose()?,
                response_hash,
            })
        })
        .transpose()
    }

    async fn list_mcp_interactions_ready_for_retry(
        &self,
        limit: u32,
    ) -> Result<Vec<McpInteraction>, RepositoryError> {
        if limit == 0 || limit > MAX_INTERACTION_LIST {
            return Err(RepositoryError::invalid_data());
        }
        let query = format!(
            "SELECT {POSTGRES_INTERACTION_COLUMNS} FROM mcp_interactions
             WHERE interaction_state='responded'
             ORDER BY updated_at,interaction_id LIMIT $1"
        );
        let rows = sqlx::query(AssertSqlSafe(query))
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await
            .map_err(RepositoryError::storage)?;
        rows.iter().map(parse_postgres_interaction).collect()
    }
}

#[cfg(test)]
mod tests {
    use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};

    use super::*;

    async fn terminal_interaction_pool() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        for statement in [
            "CREATE TABLE workflow_runs (
                run_id TEXT PRIMARY KEY,lifecycle TEXT NOT NULL,terminal_at TEXT
             )",
            "CREATE TABLE mcp_interactions (
                interaction_id TEXT PRIMARY KEY,tenant_id TEXT,user_id TEXT,run_id TEXT,
                operation_id TEXT,server_id TEXT,binding_hash TEXT,logical_request_key TEXT,
                generation INTEGER,request_json TEXT,interaction_state TEXT,outcome TEXT,
                interaction_version INTEGER,deadline TEXT,created_at TEXT,updated_at TEXT,
                closed_at TEXT
             )",
        ] {
            sqlx::query(statement).execute(&pool).await.unwrap();
        }
        pool
    }

    async fn insert_terminal_run(pool: &SqlitePool, run_id: &RunId) {
        sqlx::query(
            "INSERT INTO workflow_runs(run_id,lifecycle,terminal_at)
             VALUES (?,'cancelled','2026-07-30 12:00:06')",
        )
        .bind(run_id.as_str())
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_interactions(pool: &SqlitePool, run_id: &RunId, count: i64) {
        sqlx::query(
            "WITH digits(d) AS (
                 VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)
             ), numbers(n) AS (
                 SELECT ones.d + 10*tens.d + 100*hundreds.d + 1000*thousands.d
                 FROM digits ones CROSS JOIN digits tens
                 CROSS JOIN digits hundreds CROSS JOIN digits thousands
             )
             INSERT INTO mcp_interactions(
                 interaction_id,tenant_id,user_id,run_id,operation_id,server_id,binding_hash,
                 logical_request_key,generation,request_json,interaction_state,outcome,
                 interaction_version,deadline,created_at,updated_at,closed_at
             )
             SELECT printf('%s.interaction.limit-%04d',?,n),'tenant-a','user-a',?,
                    printf('operation-%04d',n),'server-a',?,printf('request-%04d',n),n+1,?,
                    'requested',NULL,1,'2026-07-30T12:10:00.000000Z',
                    '2026-07-30T12:00:00.000000Z','2026-07-30T12:00:00.000000Z',NULL
             FROM numbers WHERE n < ? ORDER BY n",
        )
        .bind(run_id.as_str())
        .bind(run_id.as_str())
        .bind("a".repeat(64))
        .bind(
            serde_json::to_string(&McpInteractionRequest::Approval {
                message: "body-free terminal summary".to_owned(),
                effect: "read_only".to_owned(),
            })
            .unwrap(),
        )
        .bind(count)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn sqlite_terminal_interaction_projection_accepts_1024_and_rejects_1025() {
        let pool = terminal_interaction_pool().await;
        let accepted_run = RunId::new("run_interaction_limit_accepted").unwrap();
        insert_terminal_run(&pool, &accepted_run).await;
        insert_interactions(&pool, &accepted_run, 1_024).await;

        let mut transaction = pool.begin().await.unwrap();
        let summaries =
            close_and_load_terminal_interactions_sqlite(&mut transaction, &accepted_run)
                .await
                .unwrap();
        assert_eq!(summaries.len(), MAX_INTERACTION_LIST_USIZE);
        assert_eq!(
            summaries[0].interaction_id(),
            "run_interaction_limit_accepted.interaction.limit-0000"
        );
        assert_eq!(
            summaries[1_023].interaction_id(),
            "run_interaction_limit_accepted.interaction.limit-1023"
        );
        assert!(summaries.windows(2).all(|pair| {
            pair[0].interaction_id().as_bytes() < pair[1].interaction_id().as_bytes()
        }));
        transaction.commit().await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM mcp_interactions
                 WHERE run_id=? AND interaction_state='closed'
                   AND outcome='run_terminal' AND interaction_version=2
                   AND updated_at='2026-07-30T12:00:06.000000Z'
                   AND closed_at=updated_at",
            )
            .bind(accepted_run.as_str())
            .fetch_one(&pool)
            .await
            .unwrap(),
            1_024
        );

        let rejected_run = RunId::new("run_interaction_limit_rejected").unwrap();
        insert_terminal_run(&pool, &rejected_run).await;
        insert_interactions(&pool, &rejected_run, 1_025).await;
        let mut transaction = pool.begin().await.unwrap();
        assert!(
            close_and_load_terminal_interactions_sqlite(&mut transaction, &rejected_run)
                .await
                .is_err()
        );
        transaction.rollback().await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM mcp_interactions
                 WHERE run_id=? AND interaction_state='requested'
                   AND outcome IS NULL AND interaction_version=1 AND closed_at IS NULL",
            )
            .bind(rejected_run.as_str())
            .fetch_one(&pool)
            .await
            .unwrap(),
            1_025
        );
    }
}
