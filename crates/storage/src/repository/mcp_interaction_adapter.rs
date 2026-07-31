use super::RepositoryErrorExt as _;

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use insight_durable::mcp_interaction::adapter as interaction_adapter;
use insight_durable::{
    CreateMcpInteractionCommand, McpInteraction, McpInteractionDisposition,
    McpInteractionDurableRepository, McpInteractionId, McpInteractionListFilter,
    McpInteractionOutcome, McpInteractionPrincipal, McpInteractionRequest,
    McpInteractionSecretAuthority, McpInteractionState, McpSecretCiphertext,
    ResolveMcpInteractionCommand, TransitionMcpInteractionCommand,
};
use insight_engine::{ContentHash, TransitionOutcome};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use sqlx::{postgres::PgRow, AssertSqlSafe, Postgres, Row, Sqlite, Transaction};

use super::{PostgresDurableRepository, RepositoryError, SqliteDurableRepository};

const MAX_INTERACTION_LIST: u32 = 1_024;
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
