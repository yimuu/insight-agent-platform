use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use insight_durable::mcp_remote_task::adapter as task_adapter;
use insight_durable::{
    ClaimMcpRemoteTasksCommand, CreateMcpRemoteTaskCommand, FinalizeMcpRemoteTaskCommand,
    McpInteractionPrincipal, McpRemoteTask, McpRemoteTaskDurableRepository, McpRemoteTaskId,
    McpRemoteTaskPollClaim, McpRemoteTaskSecret, McpRemoteTaskStatus, McpSecretCiphertext,
    ObserveMcpRemoteTaskCommand,
};
use insight_engine::{ContentHash, TransitionOutcome};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use sqlx::{postgres::PgRow, AssertSqlSafe, Row};

use super::{
    PostgresDurableRepository, RepositoryError, RepositoryErrorExt as _, SqliteDurableRepository,
};

const TASK_COLUMNS: &str = "task_id,tenant_id,user_id,run_id,operation_id,logical_request_key,server_id,binding_hash,protocol_version,capability_id,task_status,task_version,remote_created_at,remote_updated_at,ttl_deadline,poll_interval_ms,next_poll_at,lease_owner,lease_epoch,lease_expires_at,terminal_receipt_hash,terminal_at,created_at,updated_at";

fn hash(value: &impl Serialize) -> Result<String, RepositoryError> {
    let bytes = serde_jcs::to_vec(value).map_err(|_| RepositoryError::canonicalization())?;
    Ok(ContentHash::from_bytes(&bytes).as_str().to_owned())
}

fn create_intent(command: &CreateMcpRemoteTaskCommand) -> Result<String, RepositoryError> {
    hash(&(
        command.task(),
        command.remote_task_id_hash(),
        command.initial_payload_hash(),
    ))
}

fn observation_intent(command: &ObserveMcpRemoteTaskCommand) -> Result<String, RepositoryError> {
    hash(&(
        command.task_id(),
        command.request_id(),
        command.owner(),
        command.lease_epoch(),
        command.expected_version(),
        command.status(),
        command.remote_updated_at(),
        command.poll_interval_ms(),
        command.next_poll_at(),
        command.payload_hash(),
        command.terminal_receipt_hash(),
        command.observed_at(),
    ))
}

fn finalization_intent(command: &FinalizeMcpRemoteTaskCommand) -> Result<String, RepositoryError> {
    hash(&(
        command.task_id(),
        command.request_id(),
        command.status(),
        command.payload_hash(),
        command.terminal_receipt_hash(),
        command.finalized_at(),
    ))
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

fn parse_sqlite_task(row: &sqlx::sqlite::SqliteRow) -> Result<McpRemoteTask, RepositoryError> {
    Ok(task_adapter::task_from_storage(
        McpRemoteTaskId::new(row.try_get::<String, _>("task_id").map_err(storage_data)?)?,
        McpInteractionPrincipal::new(
            row.try_get::<String, _>("tenant_id")
                .map_err(storage_data)?,
            row.try_get::<String, _>("user_id").map_err(storage_data)?,
        )?,
        row.try_get("run_id").map_err(storage_data)?,
        row.try_get("operation_id").map_err(storage_data)?,
        row.try_get("logical_request_key").map_err(storage_data)?,
        row.try_get("server_id").map_err(storage_data)?,
        row.try_get("binding_hash").map_err(storage_data)?,
        row.try_get("protocol_version").map_err(storage_data)?,
        row.try_get("capability_id").map_err(storage_data)?,
        parse_enum(row.try_get("task_status").map_err(storage_data)?)?,
        u64::try_from(
            row.try_get::<i64, _>("task_version")
                .map_err(storage_data)?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        parse_sqlite_time(row.try_get("remote_created_at").map_err(storage_data)?)?,
        parse_sqlite_time(row.try_get("remote_updated_at").map_err(storage_data)?)?,
        parse_sqlite_time(row.try_get("ttl_deadline").map_err(storage_data)?)?,
        u64::try_from(
            row.try_get::<i64, _>("poll_interval_ms")
                .map_err(storage_data)?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get::<Option<String>, _>("next_poll_at")
            .map_err(storage_data)?
            .map(parse_sqlite_time)
            .transpose()?,
        row.try_get("lease_owner").map_err(storage_data)?,
        u64::try_from(row.try_get::<i64, _>("lease_epoch").map_err(storage_data)?)
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get::<Option<String>, _>("lease_expires_at")
            .map_err(storage_data)?
            .map(parse_sqlite_time)
            .transpose()?,
        row.try_get("terminal_receipt_hash").map_err(storage_data)?,
        row.try_get::<Option<String>, _>("terminal_at")
            .map_err(storage_data)?
            .map(parse_sqlite_time)
            .transpose()?,
        parse_sqlite_time(row.try_get("created_at").map_err(storage_data)?)?,
        parse_sqlite_time(row.try_get("updated_at").map_err(storage_data)?)?,
    ))
}

fn parse_postgres_task(row: &PgRow) -> Result<McpRemoteTask, RepositoryError> {
    Ok(task_adapter::task_from_storage(
        McpRemoteTaskId::new(row.try_get::<String, _>("task_id").map_err(storage_data)?)?,
        McpInteractionPrincipal::new(
            row.try_get::<String, _>("tenant_id")
                .map_err(storage_data)?,
            row.try_get::<String, _>("user_id").map_err(storage_data)?,
        )?,
        row.try_get("run_id").map_err(storage_data)?,
        row.try_get("operation_id").map_err(storage_data)?,
        row.try_get("logical_request_key").map_err(storage_data)?,
        row.try_get("server_id").map_err(storage_data)?,
        row.try_get("binding_hash").map_err(storage_data)?,
        row.try_get("protocol_version").map_err(storage_data)?,
        row.try_get("capability_id").map_err(storage_data)?,
        parse_enum(row.try_get("task_status").map_err(storage_data)?)?,
        u64::try_from(
            row.try_get::<i64, _>("task_version")
                .map_err(storage_data)?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("remote_created_at").map_err(storage_data)?,
        row.try_get("remote_updated_at").map_err(storage_data)?,
        row.try_get("ttl_deadline").map_err(storage_data)?,
        u64::try_from(
            row.try_get::<i64, _>("poll_interval_ms")
                .map_err(storage_data)?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("next_poll_at").map_err(storage_data)?,
        row.try_get("lease_owner").map_err(storage_data)?,
        u64::try_from(row.try_get::<i64, _>("lease_epoch").map_err(storage_data)?)
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("lease_expires_at").map_err(storage_data)?,
        row.try_get("terminal_receipt_hash").map_err(storage_data)?,
        row.try_get("terminal_at").map_err(storage_data)?,
        row.try_get("created_at").map_err(storage_data)?,
        row.try_get("updated_at").map_err(storage_data)?,
    ))
}

fn storage_data(_: sqlx::Error) -> RepositoryError {
    RepositoryError::invalid_data()
}

fn parse_sqlite_secret(
    row: sqlx::sqlite::SqliteRow,
) -> Result<McpRemoteTaskSecret, RepositoryError> {
    Ok(McpRemoteTaskSecret {
        remote_task_id: McpSecretCiphertext::new(
            row.try_get::<String, _>("remote_task_ciphertext")
                .map_err(storage_data)?,
        )?,
        remote_task_id_hash: row.try_get("remote_task_hash").map_err(storage_data)?,
        latest_payload: McpSecretCiphertext::new(
            row.try_get::<String, _>("latest_payload_ciphertext")
                .map_err(storage_data)?,
        )?,
        latest_payload_hash: row.try_get("latest_payload_hash").map_err(storage_data)?,
    })
}

fn parse_postgres_secret(row: PgRow) -> Result<McpRemoteTaskSecret, RepositoryError> {
    Ok(McpRemoteTaskSecret {
        remote_task_id: McpSecretCiphertext::new(
            row.try_get::<String, _>("remote_task_ciphertext")
                .map_err(storage_data)?,
        )?,
        remote_task_id_hash: row.try_get("remote_task_hash").map_err(storage_data)?,
        latest_payload: McpSecretCiphertext::new(
            row.try_get::<String, _>("latest_payload_ciphertext")
                .map_err(storage_data)?,
        )?,
        latest_payload_hash: row.try_get("latest_payload_hash").map_err(storage_data)?,
    })
}

#[async_trait]
impl McpRemoteTaskDurableRepository for SqliteDurableRepository {
    async fn create_mcp_remote_task(
        &self,
        command: CreateMcpRemoteTaskCommand,
    ) -> Result<TransitionOutcome<McpRemoteTask>, RepositoryError> {
        let _guard = self.writer.lock().await;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let task = command.task();
        let intent = create_intent(&command)?;
        let existing = sqlx::query(AssertSqlSafe(format!(
            "SELECT {TASK_COLUMNS},creation_intent_hash FROM mcp_remote_tasks
             WHERE task_id=? OR (run_id=? AND operation_id=? AND logical_request_key=?)"
        )))
        .bind(task.task_id().as_str())
        .bind(task.run_id())
        .bind(task.operation_id())
        .bind(task.logical_request_key())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        if let Some(row) = existing {
            let same = row
                .try_get::<String, _>("creation_intent_hash")
                .map_err(storage_data)?
                == intent;
            let authoritative = parse_sqlite_task(&row)?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(if same {
                TransitionOutcome::ExactReplay { authoritative }
            } else {
                TransitionOutcome::StateConflict
            });
        }
        sqlx::query(
            "INSERT INTO mcp_remote_tasks(
               task_id,tenant_id,user_id,run_id,operation_id,logical_request_key,server_id,
               binding_hash,protocol_version,capability_id,remote_task_ciphertext,remote_task_hash,
               task_status,task_version,remote_created_at,remote_updated_at,ttl_deadline,
               poll_interval_ms,next_poll_at,lease_owner,lease_epoch,lease_expires_at,
               latest_payload_ciphertext,latest_payload_hash,terminal_receipt_hash,terminal_at,
               created_at,updated_at,creation_intent_hash
             ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,0,NULL,?,?,NULL,NULL,?,?,?)",
        )
        .bind(task.task_id().as_str())
        .bind(task.principal().tenant_id())
        .bind(task.principal().user_id())
        .bind(task.run_id())
        .bind(task.operation_id())
        .bind(task.logical_request_key())
        .bind(task.server_id())
        .bind(task.binding_hash())
        .bind(task.protocol_version())
        .bind(task.capability_id())
        .bind(command.remote_task_id().expose_ciphertext())
        .bind(command.remote_task_id_hash())
        .bind(enum_wire(&task.status())?)
        .bind(i64::try_from(task.version()).map_err(|_| RepositoryError::invalid_data())?)
        .bind(sqlite_time(task.remote_created_at()))
        .bind(sqlite_time(task.remote_updated_at()))
        .bind(sqlite_time(task.ttl_deadline()))
        .bind(i64::try_from(task.poll_interval_ms()).map_err(|_| RepositoryError::invalid_data())?)
        .bind(task.next_poll_at().map(sqlite_time))
        .bind(command.initial_payload().expose_ciphertext())
        .bind(command.initial_payload_hash())
        .bind(sqlite_time(task.created_at()))
        .bind(sqlite_time(task.updated_at()))
        .bind(intent)
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed {
            result: task.clone(),
        })
    }

    async fn load_mcp_remote_task(
        &self,
        task_id: &McpRemoteTaskId,
    ) -> Result<Option<McpRemoteTask>, RepositoryError> {
        load_sqlite_task(&self.pool, task_id).await
    }

    async fn load_mcp_remote_task_secret(
        &self,
        task_id: &McpRemoteTaskId,
    ) -> Result<Option<McpRemoteTaskSecret>, RepositoryError> {
        sqlx::query(
            "SELECT remote_task_ciphertext,remote_task_hash,
                    latest_payload_ciphertext,latest_payload_hash
             FROM mcp_remote_tasks WHERE task_id=?",
        )
        .bind(task_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .map(parse_sqlite_secret)
        .transpose()
    }

    async fn claim_mcp_remote_tasks(
        &self,
        command: ClaimMcpRemoteTasksCommand,
    ) -> Result<Vec<McpRemoteTaskPollClaim>, RepositoryError> {
        let _guard = self.writer.lock().await;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let ids = sqlx::query_scalar::<_, String>(
            "SELECT task_id FROM mcp_remote_tasks
             WHERE task_status IN('working','input_required')
               AND next_poll_at<=?
               AND (lease_expires_at IS NULL OR lease_expires_at<=?)
             ORDER BY next_poll_at,task_id LIMIT ?",
        )
        .bind(sqlite_time(command.now()))
        .bind(sqlite_time(command.now()))
        .bind(i64::from(command.limit()))
        .fetch_all(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let mut claims = Vec::with_capacity(ids.len());
        for id in ids {
            let rows = sqlx::query(
                "UPDATE mcp_remote_tasks
                 SET lease_owner=?,lease_epoch=lease_epoch+1,lease_expires_at=?,updated_at=?
                 WHERE task_id=? AND task_status IN('working','input_required')
                   AND (lease_expires_at IS NULL OR lease_expires_at<=?)",
            )
            .bind(command.owner())
            .bind(sqlite_time(command.lease_expires_at()))
            .bind(sqlite_time(command.now()))
            .bind(&id)
            .bind(sqlite_time(command.now()))
            .execute(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if rows != 1 {
                continue;
            }
            let task = load_sqlite_task_tx(
                &mut tx,
                &McpRemoteTaskId::new(id).map_err(|_| RepositoryError::invalid_data())?,
            )
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
            let secret = load_sqlite_secret_tx(&mut tx, task.task_id())
                .await?
                .ok_or_else(RepositoryError::invalid_data)?;
            claims.push(McpRemoteTaskPollClaim {
                owner: command.owner().to_owned(),
                lease_epoch: task.lease_epoch(),
                lease_expires_at: command.lease_expires_at(),
                task,
                secret,
            });
        }
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(claims)
    }

    async fn claim_mcp_remote_task(
        &self,
        task_id: &McpRemoteTaskId,
        command: ClaimMcpRemoteTasksCommand,
    ) -> Result<Option<McpRemoteTaskPollClaim>, RepositoryError> {
        let _guard = self.writer.lock().await;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let rows = sqlx::query(
            "UPDATE mcp_remote_tasks
             SET lease_owner=?,lease_epoch=lease_epoch+1,lease_expires_at=?,updated_at=?
             WHERE task_id=? AND task_status IN('working','input_required')
               AND next_poll_at<=?
               AND (lease_expires_at IS NULL OR lease_expires_at<=?)",
        )
        .bind(command.owner())
        .bind(sqlite_time(command.lease_expires_at()))
        .bind(sqlite_time(command.now()))
        .bind(task_id.as_str())
        .bind(sqlite_time(command.now()))
        .bind(sqlite_time(command.now()))
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if rows != 1 {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(None);
        }
        let task = load_sqlite_task_tx(&mut tx, task_id)
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        let secret = load_sqlite_secret_tx(&mut tx, task_id)
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        let claim = McpRemoteTaskPollClaim {
            owner: command.owner().to_owned(),
            lease_epoch: task.lease_epoch(),
            lease_expires_at: command.lease_expires_at(),
            task,
            secret,
        };
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(Some(claim))
    }

    async fn observe_mcp_remote_task(
        &self,
        command: ObserveMcpRemoteTaskCommand,
    ) -> Result<TransitionOutcome<McpRemoteTask>, RepositoryError> {
        let _guard = self.writer.lock().await;
        let tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        observe_sqlite(tx, &command).await
    }

    async fn finalize_mcp_remote_task(
        &self,
        command: FinalizeMcpRemoteTaskCommand,
    ) -> Result<TransitionOutcome<McpRemoteTask>, RepositoryError> {
        let _guard = self.writer.lock().await;
        let tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        finalize_sqlite(tx, &command).await
    }
}

#[async_trait]
impl McpRemoteTaskDurableRepository for PostgresDurableRepository {
    async fn create_mcp_remote_task(
        &self,
        command: CreateMcpRemoteTaskCommand,
    ) -> Result<TransitionOutcome<McpRemoteTask>, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let task = command.task();
        let intent = create_intent(&command)?;
        let existing = sqlx::query(AssertSqlSafe(format!(
            "SELECT {TASK_COLUMNS},creation_intent_hash FROM mcp_remote_tasks
             WHERE task_id=$1 OR (run_id=$2 AND operation_id=$3 AND logical_request_key=$4)
             FOR UPDATE"
        )))
        .bind(task.task_id().as_str())
        .bind(task.run_id())
        .bind(task.operation_id())
        .bind(task.logical_request_key())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        if let Some(row) = existing {
            let same = row
                .try_get::<String, _>("creation_intent_hash")
                .map_err(storage_data)?
                == intent;
            let authoritative = parse_postgres_task(&row)?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(if same {
                TransitionOutcome::ExactReplay { authoritative }
            } else {
                TransitionOutcome::StateConflict
            });
        }
        sqlx::query(
            "INSERT INTO mcp_remote_tasks(
               task_id,tenant_id,user_id,run_id,operation_id,logical_request_key,server_id,
               binding_hash,protocol_version,capability_id,remote_task_ciphertext,remote_task_hash,
               task_status,task_version,remote_created_at,remote_updated_at,ttl_deadline,
               poll_interval_ms,next_poll_at,lease_owner,lease_epoch,lease_expires_at,
               latest_payload_ciphertext,latest_payload_hash,terminal_receipt_hash,terminal_at,
               created_at,updated_at,creation_intent_hash
             ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,NULL,0,NULL,$20,$21,NULL,NULL,$22,$23,$24)",
        )
        .bind(task.task_id().as_str()).bind(task.principal().tenant_id())
        .bind(task.principal().user_id()).bind(task.run_id()).bind(task.operation_id())
        .bind(task.logical_request_key()).bind(task.server_id()).bind(task.binding_hash())
        .bind(task.protocol_version()).bind(task.capability_id())
        .bind(command.remote_task_id().expose_ciphertext()).bind(command.remote_task_id_hash())
        .bind(enum_wire(&task.status())?).bind(i64::try_from(task.version()).map_err(|_| RepositoryError::invalid_data())?)
        .bind(task.remote_created_at()).bind(task.remote_updated_at()).bind(task.ttl_deadline())
        .bind(i64::try_from(task.poll_interval_ms()).map_err(|_| RepositoryError::invalid_data())?)
        .bind(task.next_poll_at()).bind(command.initial_payload().expose_ciphertext())
        .bind(command.initial_payload_hash()).bind(task.created_at()).bind(task.updated_at()).bind(intent)
        .execute(&mut *tx).await.map_err(RepositoryError::storage)?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed {
            result: task.clone(),
        })
    }

    async fn load_mcp_remote_task(
        &self,
        task_id: &McpRemoteTaskId,
    ) -> Result<Option<McpRemoteTask>, RepositoryError> {
        let row = sqlx::query(AssertSqlSafe(format!(
            "SELECT {TASK_COLUMNS} FROM mcp_remote_tasks WHERE task_id=$1"
        )))
        .bind(task_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        row.as_ref().map(parse_postgres_task).transpose()
    }

    async fn load_mcp_remote_task_secret(
        &self,
        task_id: &McpRemoteTaskId,
    ) -> Result<Option<McpRemoteTaskSecret>, RepositoryError> {
        sqlx::query(
            "SELECT remote_task_ciphertext,remote_task_hash,
                    latest_payload_ciphertext,latest_payload_hash
             FROM mcp_remote_tasks WHERE task_id=$1",
        )
        .bind(task_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .map(parse_postgres_secret)
        .transpose()
    }

    async fn claim_mcp_remote_tasks(
        &self,
        command: ClaimMcpRemoteTasksCommand,
    ) -> Result<Vec<McpRemoteTaskPollClaim>, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let rows = sqlx::query(AssertSqlSafe(format!(
            "SELECT {TASK_COLUMNS},remote_task_ciphertext,remote_task_hash,
                    latest_payload_ciphertext,latest_payload_hash
             FROM mcp_remote_tasks
             WHERE task_status IN('working','input_required')
               AND next_poll_at<=$1
               AND (lease_expires_at IS NULL OR lease_expires_at<=$1)
             ORDER BY next_poll_at,task_id
             LIMIT $2 FOR UPDATE SKIP LOCKED"
        )))
        .bind(command.now())
        .bind(i64::from(command.limit()))
        .fetch_all(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let task_id: String = row.try_get("task_id").map_err(storage_data)?;
            sqlx::query(
                "UPDATE mcp_remote_tasks
                 SET lease_owner=$1,lease_epoch=lease_epoch+1,lease_expires_at=$2,updated_at=$3
                 WHERE task_id=$4",
            )
            .bind(command.owner())
            .bind(command.lease_expires_at())
            .bind(command.now())
            .bind(&task_id)
            .execute(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?;
            let task = load_postgres_task_tx(
                &mut tx,
                &McpRemoteTaskId::new(task_id).map_err(|_| RepositoryError::invalid_data())?,
            )
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
            let secret = McpRemoteTaskSecret {
                remote_task_id: McpSecretCiphertext::new(
                    row.try_get::<String, _>("remote_task_ciphertext")
                        .map_err(storage_data)?,
                )?,
                remote_task_id_hash: row.try_get("remote_task_hash").map_err(storage_data)?,
                latest_payload: McpSecretCiphertext::new(
                    row.try_get::<String, _>("latest_payload_ciphertext")
                        .map_err(storage_data)?,
                )?,
                latest_payload_hash: row.try_get("latest_payload_hash").map_err(storage_data)?,
            };
            claims.push(McpRemoteTaskPollClaim {
                owner: command.owner().to_owned(),
                lease_epoch: task.lease_epoch(),
                lease_expires_at: command.lease_expires_at(),
                task,
                secret,
            });
        }
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(claims)
    }

    async fn claim_mcp_remote_task(
        &self,
        task_id: &McpRemoteTaskId,
        command: ClaimMcpRemoteTasksCommand,
    ) -> Result<Option<McpRemoteTaskPollClaim>, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let rows = sqlx::query(
            "UPDATE mcp_remote_tasks
             SET lease_owner=$1,lease_epoch=lease_epoch+1,lease_expires_at=$2,updated_at=$3
             WHERE task_id=$4 AND task_status IN('working','input_required')
               AND next_poll_at<=$3
               AND (lease_expires_at IS NULL OR lease_expires_at<=$3)",
        )
        .bind(command.owner())
        .bind(command.lease_expires_at())
        .bind(command.now())
        .bind(task_id.as_str())
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if rows != 1 {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(None);
        }
        let task = load_postgres_task_tx(&mut tx, task_id)
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        let row = sqlx::query(
            "SELECT remote_task_ciphertext,remote_task_hash,
                    latest_payload_ciphertext,latest_payload_hash
             FROM mcp_remote_tasks WHERE task_id=$1",
        )
        .bind(task_id.as_str())
        .fetch_one(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let secret = parse_postgres_secret(row)?;
        let claim = McpRemoteTaskPollClaim {
            owner: command.owner().to_owned(),
            lease_epoch: task.lease_epoch(),
            lease_expires_at: command.lease_expires_at(),
            task,
            secret,
        };
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(Some(claim))
    }

    async fn observe_mcp_remote_task(
        &self,
        command: ObserveMcpRemoteTaskCommand,
    ) -> Result<TransitionOutcome<McpRemoteTask>, RepositoryError> {
        let tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        observe_postgres(tx, &command).await
    }

    async fn finalize_mcp_remote_task(
        &self,
        command: FinalizeMcpRemoteTaskCommand,
    ) -> Result<TransitionOutcome<McpRemoteTask>, RepositoryError> {
        let tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        finalize_postgres(tx, &command).await
    }
}

async fn observe_sqlite(
    mut tx: sqlx::Transaction<'_, sqlx::Sqlite>,
    command: &ObserveMcpRemoteTaskCommand,
) -> Result<TransitionOutcome<McpRemoteTask>, RepositoryError> {
    let intent = observation_intent(command)?;
    if let Some(existing) = sqlx::query_scalar::<_, String>(
        "SELECT intent_hash FROM mcp_remote_task_receipts WHERE task_id=? AND request_id=?",
    )
    .bind(command.task_id().as_str())
    .bind(command.request_id())
    .fetch_optional(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?
    {
        let task = load_sqlite_task_tx(&mut tx, command.task_id())
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(if existing == intent {
            TransitionOutcome::ExactReplay {
                authoritative: task,
            }
        } else {
            TransitionOutcome::StateConflict
        });
    }
    let Some(task) = load_sqlite_task_tx(&mut tx, command.task_id()).await? else {
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    if !valid_observation(&task, command) {
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let version = task.version() + 1;
    let rows = sqlx::query(
        "UPDATE mcp_remote_tasks SET
           task_status=?,task_version=?,remote_updated_at=?,poll_interval_ms=?,next_poll_at=?,
           lease_owner=NULL,lease_expires_at=NULL,latest_payload_ciphertext=?,
           latest_payload_hash=?,terminal_receipt_hash=?,terminal_at=?,updated_at=?
         WHERE task_id=? AND task_version=? AND lease_owner=? AND lease_epoch=?",
    )
    .bind(enum_wire(&command.status())?)
    .bind(i64::try_from(version).map_err(|_| RepositoryError::invalid_data())?)
    .bind(sqlite_time(command.remote_updated_at()))
    .bind(i64::try_from(command.poll_interval_ms()).map_err(|_| RepositoryError::invalid_data())?)
    .bind(command.next_poll_at().map(sqlite_time))
    .bind(command.payload().expose_ciphertext())
    .bind(command.payload_hash())
    .bind(command.terminal_receipt_hash())
    .bind(
        command
            .status()
            .is_terminal()
            .then(|| sqlite_time(command.observed_at())),
    )
    .bind(sqlite_time(command.observed_at()))
    .bind(command.task_id().as_str())
    .bind(i64::try_from(task.version()).map_err(|_| RepositoryError::invalid_data())?)
    .bind(command.owner())
    .bind(i64::try_from(command.lease_epoch()).map_err(|_| RepositoryError::invalid_data())?)
    .execute(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if rows != 1 {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    sqlx::query("INSERT INTO mcp_remote_task_receipts(task_id,request_id,intent_hash,result_version,created_at) VALUES(?,?,?,?,?)")
        .bind(command.task_id().as_str()).bind(command.request_id()).bind(intent)
        .bind(i64::try_from(version).map_err(|_| RepositoryError::invalid_data())?)
        .bind(sqlite_time(command.observed_at())).execute(&mut *tx).await.map_err(RepositoryError::storage)?;
    let result = load_sqlite_task_tx(&mut tx, command.task_id())
        .await?
        .ok_or_else(RepositoryError::invalid_data)?;
    tx.commit().await.map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed { result })
}

async fn observe_postgres(
    mut tx: sqlx::Transaction<'_, sqlx::Postgres>,
    command: &ObserveMcpRemoteTaskCommand,
) -> Result<TransitionOutcome<McpRemoteTask>, RepositoryError> {
    let intent = observation_intent(command)?;
    if let Some(existing) = sqlx::query_scalar::<_, String>(
        "SELECT intent_hash FROM mcp_remote_task_receipts WHERE task_id=$1 AND request_id=$2",
    )
    .bind(command.task_id().as_str())
    .bind(command.request_id())
    .fetch_optional(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?
    {
        let task = load_postgres_task_tx(&mut tx, command.task_id())
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(if existing == intent {
            TransitionOutcome::ExactReplay {
                authoritative: task,
            }
        } else {
            TransitionOutcome::StateConflict
        });
    }
    let Some(task) = load_postgres_task_tx(&mut tx, command.task_id()).await? else {
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    if !valid_observation(&task, command) {
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let version = task.version() + 1;
    let rows = sqlx::query(
        "UPDATE mcp_remote_tasks SET
           task_status=$1,task_version=$2,remote_updated_at=$3,poll_interval_ms=$4,next_poll_at=$5,
           lease_owner=NULL,lease_expires_at=NULL,latest_payload_ciphertext=$6,
           latest_payload_hash=$7,terminal_receipt_hash=$8,terminal_at=$9,updated_at=$10
         WHERE task_id=$11 AND task_version=$12 AND lease_owner=$13 AND lease_epoch=$14",
    )
    .bind(enum_wire(&command.status())?)
    .bind(i64::try_from(version).map_err(|_| RepositoryError::invalid_data())?)
    .bind(command.remote_updated_at())
    .bind(i64::try_from(command.poll_interval_ms()).map_err(|_| RepositoryError::invalid_data())?)
    .bind(command.next_poll_at())
    .bind(command.payload().expose_ciphertext())
    .bind(command.payload_hash())
    .bind(command.terminal_receipt_hash())
    .bind(
        command
            .status()
            .is_terminal()
            .then_some(command.observed_at()),
    )
    .bind(command.observed_at())
    .bind(command.task_id().as_str())
    .bind(i64::try_from(task.version()).map_err(|_| RepositoryError::invalid_data())?)
    .bind(command.owner())
    .bind(i64::try_from(command.lease_epoch()).map_err(|_| RepositoryError::invalid_data())?)
    .execute(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if rows != 1 {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    sqlx::query("INSERT INTO mcp_remote_task_receipts(task_id,request_id,intent_hash,result_version,created_at) VALUES($1,$2,$3,$4,$5)")
        .bind(command.task_id().as_str()).bind(command.request_id()).bind(intent)
        .bind(i64::try_from(version).map_err(|_| RepositoryError::invalid_data())?)
        .bind(command.observed_at()).execute(&mut *tx).await.map_err(RepositoryError::storage)?;
    let result = load_postgres_task_tx(&mut tx, command.task_id())
        .await?
        .ok_or_else(RepositoryError::invalid_data)?;
    tx.commit().await.map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed { result })
}

async fn finalize_sqlite(
    mut tx: sqlx::Transaction<'_, sqlx::Sqlite>,
    command: &FinalizeMcpRemoteTaskCommand,
) -> Result<TransitionOutcome<McpRemoteTask>, RepositoryError> {
    let intent = finalization_intent(command)?;
    if let Some(existing) = sqlx::query_scalar::<_, String>(
        "SELECT intent_hash FROM mcp_remote_task_receipts WHERE task_id=? AND request_id=?",
    )
    .bind(command.task_id().as_str())
    .bind(command.request_id())
    .fetch_optional(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?
    {
        let task = load_sqlite_task_tx(&mut tx, command.task_id())
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(if existing == intent {
            TransitionOutcome::ExactReplay {
                authoritative: task,
            }
        } else {
            TransitionOutcome::StateConflict
        });
    }
    let Some(task) = load_sqlite_task_tx(&mut tx, command.task_id()).await? else {
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    if !valid_finalization(&task, command) {
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let version = task.version() + 1;
    let rows = sqlx::query(
        "UPDATE mcp_remote_tasks SET
           task_status=?,task_version=?,next_poll_at=NULL,lease_owner=NULL,lease_expires_at=NULL,
           latest_payload_ciphertext=?,latest_payload_hash=?,terminal_receipt_hash=?,
           terminal_at=?,updated_at=?
         WHERE task_id=? AND task_version=? AND task_status IN('working','input_required')",
    )
    .bind(enum_wire(&command.status())?)
    .bind(i64::try_from(version).map_err(|_| RepositoryError::invalid_data())?)
    .bind(command.payload().expose_ciphertext())
    .bind(command.payload_hash())
    .bind(command.terminal_receipt_hash())
    .bind(sqlite_time(command.finalized_at()))
    .bind(sqlite_time(command.finalized_at()))
    .bind(command.task_id().as_str())
    .bind(i64::try_from(task.version()).map_err(|_| RepositoryError::invalid_data())?)
    .execute(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if rows != 1 {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    sqlx::query("INSERT INTO mcp_remote_task_receipts(task_id,request_id,intent_hash,result_version,created_at) VALUES(?,?,?,?,?)")
        .bind(command.task_id().as_str())
        .bind(command.request_id())
        .bind(intent)
        .bind(i64::try_from(version).map_err(|_| RepositoryError::invalid_data())?)
        .bind(sqlite_time(command.finalized_at()))
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
    let result = load_sqlite_task_tx(&mut tx, command.task_id())
        .await?
        .ok_or_else(RepositoryError::invalid_data)?;
    tx.commit().await.map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed { result })
}

async fn finalize_postgres(
    mut tx: sqlx::Transaction<'_, sqlx::Postgres>,
    command: &FinalizeMcpRemoteTaskCommand,
) -> Result<TransitionOutcome<McpRemoteTask>, RepositoryError> {
    let intent = finalization_intent(command)?;
    if let Some(existing) = sqlx::query_scalar::<_, String>(
        "SELECT intent_hash FROM mcp_remote_task_receipts WHERE task_id=$1 AND request_id=$2",
    )
    .bind(command.task_id().as_str())
    .bind(command.request_id())
    .fetch_optional(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?
    {
        let task = load_postgres_task_tx(&mut tx, command.task_id())
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(if existing == intent {
            TransitionOutcome::ExactReplay {
                authoritative: task,
            }
        } else {
            TransitionOutcome::StateConflict
        });
    }
    let Some(task) = load_postgres_task_tx(&mut tx, command.task_id()).await? else {
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    if !valid_finalization(&task, command) {
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    let version = task.version() + 1;
    let rows = sqlx::query(
        "UPDATE mcp_remote_tasks SET
           task_status=$1,task_version=$2,next_poll_at=NULL,lease_owner=NULL,lease_expires_at=NULL,
           latest_payload_ciphertext=$3,latest_payload_hash=$4,terminal_receipt_hash=$5,
           terminal_at=$6,updated_at=$6
         WHERE task_id=$7 AND task_version=$8 AND task_status IN('working','input_required')",
    )
    .bind(enum_wire(&command.status())?)
    .bind(i64::try_from(version).map_err(|_| RepositoryError::invalid_data())?)
    .bind(command.payload().expose_ciphertext())
    .bind(command.payload_hash())
    .bind(command.terminal_receipt_hash())
    .bind(command.finalized_at())
    .bind(command.task_id().as_str())
    .bind(i64::try_from(task.version()).map_err(|_| RepositoryError::invalid_data())?)
    .execute(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if rows != 1 {
        tx.rollback().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    }
    sqlx::query("INSERT INTO mcp_remote_task_receipts(task_id,request_id,intent_hash,result_version,created_at) VALUES($1,$2,$3,$4,$5)")
        .bind(command.task_id().as_str())
        .bind(command.request_id())
        .bind(intent)
        .bind(i64::try_from(version).map_err(|_| RepositoryError::invalid_data())?)
        .bind(command.finalized_at())
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
    let result = load_postgres_task_tx(&mut tx, command.task_id())
        .await?
        .ok_or_else(RepositoryError::invalid_data)?;
    tx.commit().await.map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed { result })
}

fn valid_finalization(task: &McpRemoteTask, command: &FinalizeMcpRemoteTaskCommand) -> bool {
    !task.status().is_terminal()
        && (command.status() != McpRemoteTaskStatus::Expired
            || command.finalized_at() >= task.ttl_deadline())
}

fn valid_observation(task: &McpRemoteTask, command: &ObserveMcpRemoteTaskCommand) -> bool {
    !task.status().is_terminal()
        && task.version() == command.expected_version()
        && task.lease_owner() == Some(command.owner())
        && task.lease_epoch() == command.lease_epoch()
        && task
            .lease_expires_at()
            .is_some_and(|deadline| deadline > command.observed_at())
        && command.remote_updated_at() >= task.remote_updated_at()
        && ((command.observed_at() >= task.ttl_deadline()
            && command.status() == McpRemoteTaskStatus::Expired)
            || (command.observed_at() < task.ttl_deadline()
                && command.status() != McpRemoteTaskStatus::Expired))
}

async fn load_sqlite_task(
    pool: &sqlx::SqlitePool,
    task_id: &McpRemoteTaskId,
) -> Result<Option<McpRemoteTask>, RepositoryError> {
    let row = sqlx::query(AssertSqlSafe(format!(
        "SELECT {TASK_COLUMNS} FROM mcp_remote_tasks WHERE task_id=?"
    )))
    .bind(task_id.as_str())
    .fetch_optional(pool)
    .await
    .map_err(RepositoryError::storage)?;
    row.as_ref().map(parse_sqlite_task).transpose()
}

async fn load_sqlite_task_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: &McpRemoteTaskId,
) -> Result<Option<McpRemoteTask>, RepositoryError> {
    let row = sqlx::query(AssertSqlSafe(format!(
        "SELECT {TASK_COLUMNS} FROM mcp_remote_tasks WHERE task_id=?"
    )))
    .bind(task_id.as_str())
    .fetch_optional(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    row.as_ref().map(parse_sqlite_task).transpose()
}

async fn load_sqlite_secret_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task_id: &McpRemoteTaskId,
) -> Result<Option<McpRemoteTaskSecret>, RepositoryError> {
    sqlx::query(
        "SELECT remote_task_ciphertext,remote_task_hash,
                latest_payload_ciphertext,latest_payload_hash
         FROM mcp_remote_tasks WHERE task_id=?",
    )
    .bind(task_id.as_str())
    .fetch_optional(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?
    .map(parse_sqlite_secret)
    .transpose()
}

async fn load_postgres_task_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    task_id: &McpRemoteTaskId,
) -> Result<Option<McpRemoteTask>, RepositoryError> {
    let row = sqlx::query(AssertSqlSafe(format!(
        "SELECT {TASK_COLUMNS} FROM mcp_remote_tasks WHERE task_id=$1 FOR UPDATE"
    )))
    .bind(task_id.as_str())
    .fetch_optional(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    row.as_ref().map(parse_postgres_task).transpose()
}
