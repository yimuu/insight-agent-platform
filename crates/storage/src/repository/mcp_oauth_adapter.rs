use super::RepositoryErrorExt as _;

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use insight_durable::mcp_oauth::adapter as oauth_adapter;
use insight_durable::{
    ClaimMcpOAuthRefreshCommand, CompleteMcpOAuthCallbackCommand,
    ConsumeMcpOAuthTransactionCommand, CreateMcpOAuthTransactionCommand, McpInteractionPrincipal,
    McpOAuthCallbackCompletion, McpOAuthCredential, McpOAuthCredentialSecret,
    McpOAuthDurableRepository, McpOAuthTransaction, McpOAuthTransactionId,
    McpOAuthTransactionSecret, McpOAuthTransactionState, McpSecretCiphertext,
    StoreMcpOAuthCredentialCommand,
};
use insight_engine::{ContentHash, TransitionOutcome};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use sqlx::{postgres::PgRow, AssertSqlSafe, Row};

use super::{PostgresDurableRepository, RepositoryError, SqliteDurableRepository};

const SCRUBBED_CIPHERTEXT: &str = "enc:v1:deleted";
const SCRUBBED_SECRET_HASH: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";
const MAX_EXPIRY_BATCH: u32 = 1_000;

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

fn parse_sqlite_transaction(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<McpOAuthTransaction, RepositoryError> {
    Ok(oauth_adapter::transaction_from_storage(
        McpOAuthTransactionId::new(
            row.try_get::<String, _>("transaction_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        McpInteractionPrincipal::new(
            row.try_get::<String, _>("tenant_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get::<String, _>("user_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        row.try_get("server_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("issuer")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("resource")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("client_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("redirect_uri")
            .map_err(|_| RepositoryError::invalid_data())?,
        serde_json::from_str(
            &row.try_get::<String, _>("scopes_json")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("state_hash")
            .map_err(|_| RepositoryError::invalid_data())?,
        parse_enum(
            row.try_get("transaction_state")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        u64::try_from(
            row.try_get::<i64, _>("transaction_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        parse_sqlite_time(
            row.try_get("expires_at")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        parse_sqlite_time(
            row.try_get("created_at")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        row.try_get::<Option<String>, _>("consumed_at")
            .map_err(|_| RepositoryError::invalid_data())?
            .map(parse_sqlite_time)
            .transpose()?,
    ))
}

fn parse_postgres_transaction(row: &PgRow) -> Result<McpOAuthTransaction, RepositoryError> {
    Ok(oauth_adapter::transaction_from_storage(
        McpOAuthTransactionId::new(
            row.try_get::<String, _>("transaction_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        McpInteractionPrincipal::new(
            row.try_get::<String, _>("tenant_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get::<String, _>("user_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        row.try_get("server_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("issuer")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("resource")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("client_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("redirect_uri")
            .map_err(|_| RepositoryError::invalid_data())?,
        serde_json::from_value(
            row.try_get::<Value, _>("scopes_json")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("state_hash")
            .map_err(|_| RepositoryError::invalid_data())?,
        parse_enum(
            row.try_get("transaction_state")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        u64::try_from(
            row.try_get::<i64, _>("transaction_version")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("expires_at")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("created_at")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("consumed_at")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))
}

fn parse_sqlite_credential(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<McpOAuthCredential, RepositoryError> {
    Ok(oauth_adapter::credential_from_storage(
        McpInteractionPrincipal::new(
            row.try_get::<String, _>("tenant_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get::<String, _>("user_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        row.try_get("server_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("issuer")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("client_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("resource")
            .map_err(|_| RepositoryError::invalid_data())?,
        serde_json::from_str(
            &row.try_get::<String, _>("scopes_json")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("token_type")
            .map_err(|_| RepositoryError::invalid_data())?,
        u64::try_from(
            row.try_get::<i64, _>("credential_generation")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get::<Option<String>, _>("access_expires_at")
            .map_err(|_| RepositoryError::invalid_data())?
            .map(parse_sqlite_time)
            .transpose()?,
        parse_sqlite_time(
            row.try_get("updated_at")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        row.try_get::<Option<String>, _>("revoked_at")
            .map_err(|_| RepositoryError::invalid_data())?
            .map(parse_sqlite_time)
            .transpose()?,
    ))
}

fn parse_postgres_credential(row: &PgRow) -> Result<McpOAuthCredential, RepositoryError> {
    Ok(oauth_adapter::credential_from_storage(
        McpInteractionPrincipal::new(
            row.try_get::<String, _>("tenant_id")
                .map_err(|_| RepositoryError::invalid_data())?,
            row.try_get::<String, _>("user_id")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        row.try_get("server_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("issuer")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("client_id")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("resource")
            .map_err(|_| RepositoryError::invalid_data())?,
        serde_json::from_value(
            row.try_get::<Value, _>("scopes_json")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("token_type")
            .map_err(|_| RepositoryError::invalid_data())?,
        u64::try_from(
            row.try_get::<i64, _>("credential_generation")
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("access_expires_at")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("updated_at")
            .map_err(|_| RepositoryError::invalid_data())?,
        row.try_get("revoked_at")
            .map_err(|_| RepositoryError::invalid_data())?,
    ))
}

const TRANSACTION_COLUMNS: &str = "transaction_id,tenant_id,user_id,server_id,issuer,resource,client_id,redirect_uri,scopes_json,state_hash,transaction_state,transaction_version,expires_at,created_at,consumed_at";
const CREDENTIAL_COLUMNS: &str = "tenant_id,user_id,server_id,issuer,client_id,resource,scopes_json,token_type,credential_generation,access_expires_at,updated_at,revoked_at";

fn callback_authority_matches(
    transaction: &McpOAuthTransaction,
    consume: &ConsumeMcpOAuthTransactionCommand,
    store: &StoreMcpOAuthCredentialCommand,
) -> bool {
    let credential = store.credential();
    transaction.principal() == consume.principal()
        && transaction.principal() == credential.principal()
        && transaction.server_id() == credential.server_id()
        && transaction.issuer() == credential.issuer()
        && transaction.client_id() == credential.client_id()
        && transaction.resource() == credential.resource()
        && transaction
            .scopes()
            .iter()
            .all(|scope| credential.scopes().contains(scope))
        && credential.revoked_at().is_none()
}

#[async_trait]
impl McpOAuthDurableRepository for SqliteDurableRepository {
    async fn create_mcp_oauth_transaction(
        &self,
        command: CreateMcpOAuthTransactionCommand,
    ) -> Result<TransitionOutcome<McpOAuthTransaction>, RepositoryError> {
        let _guard = self.writer.lock().await;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let transaction = command.transaction();
        let intent = intent_hash(&command)?;
        if let Some(row) = sqlx::query(AssertSqlSafe(format!(
            "SELECT {TRANSACTION_COLUMNS},creation_intent_hash FROM mcp_oauth_transactions WHERE transaction_id=?"
        )))
        .bind(transaction.transaction_id().as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        {
            let existing = parse_sqlite_transaction(&row)?;
            let existing_intent: String = row
                .try_get("creation_intent_hash")
                .map_err(|_| RepositoryError::invalid_data())?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(if existing_intent == intent {
                TransitionOutcome::ExactReplay {
                    authoritative: existing,
                }
            } else {
                TransitionOutcome::StateConflict
            });
        }
        sqlx::query("INSERT INTO mcp_oauth_transactions(transaction_id,tenant_id,user_id,server_id,issuer,resource,client_id,redirect_uri,scopes_json,state_hash,transaction_state,transaction_version,transaction_ciphertext,transaction_secret_hash,expires_at,created_at,consumed_at,creation_intent_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            .bind(transaction.transaction_id().as_str())
            .bind(transaction.principal().tenant_id())
            .bind(transaction.principal().user_id())
            .bind(transaction.server_id())
            .bind(transaction.issuer())
            .bind(transaction.resource())
            .bind(transaction.client_id())
            .bind(transaction.redirect_uri())
            .bind(serde_json::to_string(transaction.scopes()).map_err(|_| RepositoryError::invalid_data())?)
            .bind(transaction.state_hash())
            .bind(enum_wire(&transaction.state())?)
            .bind(i64::try_from(transaction.version()).map_err(|_| RepositoryError::invalid_data())?)
            .bind(command.transaction_secret().expose_ciphertext())
            .bind(command.transaction_secret_hash())
            .bind(sqlite_time(transaction.expires_at()))
            .bind(sqlite_time(transaction.created_at()))
            .bind(Option::<String>::None)
            .bind(intent)
            .execute(&mut *tx).await.map_err(RepositoryError::storage)?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed {
            result: transaction.clone(),
        })
    }

    async fn load_mcp_oauth_transaction(
        &self,
        transaction_id: &McpOAuthTransactionId,
    ) -> Result<Option<McpOAuthTransaction>, RepositoryError> {
        sqlx::query(AssertSqlSafe(format!(
            "SELECT {TRANSACTION_COLUMNS} FROM mcp_oauth_transactions WHERE transaction_id=?"
        )))
        .bind(transaction_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .as_ref()
        .map(parse_sqlite_transaction)
        .transpose()
    }

    async fn load_mcp_oauth_transaction_secret(
        &self,
        transaction_id: &McpOAuthTransactionId,
    ) -> Result<Option<McpOAuthTransactionSecret>, RepositoryError> {
        let row = sqlx::query("SELECT transaction_ciphertext,transaction_secret_hash FROM mcp_oauth_transactions WHERE transaction_id=? AND transaction_state='pending'")
            .bind(transaction_id.as_str()).fetch_optional(&self.pool).await.map_err(RepositoryError::storage)?;
        row.map(|row| {
            Ok(McpOAuthTransactionSecret {
                transaction_secret: McpSecretCiphertext::new(
                    row.try_get::<String, _>("transaction_ciphertext")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?,
                transaction_secret_hash: row
                    .try_get("transaction_secret_hash")
                    .map_err(|_| RepositoryError::invalid_data())?,
            })
        })
        .transpose()
    }

    async fn consume_mcp_oauth_transaction(
        &self,
        command: ConsumeMcpOAuthTransactionCommand,
    ) -> Result<TransitionOutcome<McpOAuthTransaction>, RepositoryError> {
        let _guard = self.writer.lock().await;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let intent = intent_hash(&command)?;
        if let Some(row) = sqlx::query("SELECT intent_hash FROM mcp_oauth_transaction_receipts WHERE transaction_id=? AND request_id=?")
            .bind(command.transaction_id().as_str()).bind(command.request_id())
            .fetch_optional(&mut *tx).await.map_err(RepositoryError::storage)?
        {
            let same = row.try_get::<String, _>("intent_hash").map_err(|_| RepositoryError::invalid_data())? == intent;
            let current = load_sqlite_transaction_tx(&mut tx, command.transaction_id()).await?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(if same {
                TransitionOutcome::ExactReplay { authoritative: current.ok_or_else(RepositoryError::invalid_data)? }
            } else { TransitionOutcome::StateConflict });
        }
        let Some(current) = load_sqlite_transaction_tx(&mut tx, command.transaction_id()).await?
        else {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        if current.principal() != command.principal()
            || current.state() != McpOAuthTransactionState::Pending
            || current.version() != command.expected_version()
            || current.expires_at() <= command.consumed_at()
            || !constant_time_eq(
                current.state_hash().as_bytes(),
                command.callback_state_hash().as_bytes(),
            )
        {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let version = current.version() + 1;
        sqlx::query("UPDATE mcp_oauth_transactions SET transaction_state='consumed',transaction_version=?,transaction_ciphertext=?,transaction_secret_hash=?,consumed_at=? WHERE transaction_id=? AND transaction_version=?")
            .bind(i64::try_from(version).map_err(|_| RepositoryError::invalid_data())?)
            .bind(SCRUBBED_CIPHERTEXT).bind(SCRUBBED_SECRET_HASH)
            .bind(sqlite_time(command.consumed_at())).bind(command.transaction_id().as_str())
            .bind(i64::try_from(current.version()).map_err(|_| RepositoryError::invalid_data())?)
            .execute(&mut *tx).await.map_err(RepositoryError::storage)?;
        sqlx::query("INSERT INTO mcp_oauth_transaction_receipts(transaction_id,request_id,intent_hash,result_version,created_at) VALUES(?,?,?,?,?)")
            .bind(command.transaction_id().as_str()).bind(command.request_id()).bind(intent)
            .bind(i64::try_from(version).map_err(|_| RepositoryError::invalid_data())?)
            .bind(sqlite_time(command.consumed_at())).execute(&mut *tx).await.map_err(RepositoryError::storage)?;
        let result = load_sqlite_transaction_tx(&mut tx, command.transaction_id())
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result })
    }

    async fn store_mcp_oauth_credential(
        &self,
        command: StoreMcpOAuthCredentialCommand,
    ) -> Result<TransitionOutcome<McpOAuthCredential>, RepositoryError> {
        let _guard = self.writer.lock().await;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let credential = command.credential();
        let intent = intent_hash(&command)?;
        if let Some(row) = sqlx::query("SELECT intent_hash FROM mcp_oauth_credential_receipts WHERE tenant_id=? AND user_id=? AND server_id=? AND request_id=?")
            .bind(credential.principal().tenant_id()).bind(credential.principal().user_id())
            .bind(credential.server_id()).bind(command.request_id())
            .fetch_optional(&mut *tx).await.map_err(RepositoryError::storage)?
        {
            let same = row.try_get::<String, _>("intent_hash").map_err(|_| RepositoryError::invalid_data())? == intent;
            let current = load_sqlite_credential_tx(&mut tx, credential.principal(), credential.server_id()).await?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(if same {
                TransitionOutcome::ExactReplay { authoritative: current.ok_or_else(RepositoryError::invalid_data)? }
            } else { TransitionOutcome::StateConflict });
        }
        let existing =
            load_sqlite_credential_tx(&mut tx, credential.principal(), credential.server_id())
                .await?;
        let refresh_fence_ok = if let Some(owner) = command.refresh_lease_owner() {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM mcp_oauth_refresh_leases
                 WHERE tenant_id=? AND user_id=? AND server_id=?
                   AND credential_generation=? AND lease_owner=?
                   AND dispatched_at IS NOT NULL AND lease_expires_at>?",
            )
            .bind(credential.principal().tenant_id())
            .bind(credential.principal().user_id())
            .bind(credential.server_id())
            .bind(
                i64::try_from(
                    command
                        .expected_generation()
                        .ok_or_else(RepositoryError::invalid_data)?,
                )
                .map_err(|_| RepositoryError::invalid_data())?,
            )
            .bind(owner)
            .bind(sqlite_time(credential.updated_at()))
            .fetch_one(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?
                == 1
        } else {
            true
        };
        let generation_ok = refresh_fence_ok
            && match (&existing, command.expected_generation()) {
                (None, None) => credential.generation() == 1,
                (Some(current), Some(expected)) => {
                    current.generation() == expected && credential.generation() == expected + 1
                }
                _ => false,
            };
        if !generation_ok {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        sqlx::query("INSERT INTO mcp_oauth_credentials(tenant_id,user_id,server_id,issuer,client_id,resource,scopes_json,token_type,credential_generation,access_ciphertext,access_token_hash,refresh_ciphertext,refresh_token_hash,access_expires_at,updated_at,revoked_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL) ON CONFLICT(tenant_id,user_id,server_id) DO UPDATE SET issuer=excluded.issuer,client_id=excluded.client_id,resource=excluded.resource,scopes_json=excluded.scopes_json,token_type=excluded.token_type,credential_generation=excluded.credential_generation,access_ciphertext=excluded.access_ciphertext,access_token_hash=excluded.access_token_hash,refresh_ciphertext=excluded.refresh_ciphertext,refresh_token_hash=excluded.refresh_token_hash,access_expires_at=excluded.access_expires_at,updated_at=excluded.updated_at,revoked_at=NULL")
            .bind(credential.principal().tenant_id()).bind(credential.principal().user_id())
            .bind(credential.server_id()).bind(credential.issuer()).bind(credential.client_id())
            .bind(credential.resource()).bind(serde_json::to_string(credential.scopes()).map_err(|_| RepositoryError::invalid_data())?)
            .bind(credential.token_type()).bind(i64::try_from(credential.generation()).map_err(|_| RepositoryError::invalid_data())?)
            .bind(command.access_token().expose_ciphertext()).bind(command.access_token_hash())
            .bind(command.refresh_token().map(McpSecretCiphertext::expose_ciphertext))
            .bind(command.refresh_token_hash()).bind(credential.access_expires_at().map(sqlite_time))
            .bind(sqlite_time(credential.updated_at())).execute(&mut *tx).await.map_err(RepositoryError::storage)?;
        insert_sqlite_credential_receipt(&mut tx, credential, command.request_id(), &intent)
            .await?;
        if let (Some(owner), Some(generation)) =
            (command.refresh_lease_owner(), command.expected_generation())
        {
            let rows = sqlx::query(
                "DELETE FROM mcp_oauth_refresh_leases
                 WHERE tenant_id=? AND user_id=? AND server_id=?
                   AND credential_generation=? AND lease_owner=?
                   AND dispatched_at IS NOT NULL",
            )
            .bind(credential.principal().tenant_id())
            .bind(credential.principal().user_id())
            .bind(credential.server_id())
            .bind(i64::try_from(generation).map_err(|_| RepositoryError::invalid_data())?)
            .bind(owner)
            .execute(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if rows != 1 {
                tx.rollback().await.map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::StaleLease);
            }
        }
        let result =
            load_sqlite_credential_tx(&mut tx, credential.principal(), credential.server_id())
                .await?
                .ok_or_else(RepositoryError::invalid_data)?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result })
    }

    async fn complete_mcp_oauth_callback(
        &self,
        command: CompleteMcpOAuthCallbackCommand,
    ) -> Result<TransitionOutcome<McpOAuthCallbackCompletion>, RepositoryError> {
        let _guard = self.writer.lock().await;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let consume = command.consume();
        let store = command.credential();
        let credential = store.credential();
        let consume_intent = intent_hash(consume)?;
        let credential_intent = intent_hash(store)?;
        let transaction_receipt = sqlx::query_scalar::<_, String>(
            "SELECT intent_hash FROM mcp_oauth_transaction_receipts
             WHERE transaction_id=? AND request_id=?",
        )
        .bind(consume.transaction_id().as_str())
        .bind(consume.request_id())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let credential_receipt = sqlx::query_scalar::<_, String>(
            "SELECT intent_hash FROM mcp_oauth_credential_receipts
             WHERE tenant_id=? AND user_id=? AND server_id=? AND request_id=?",
        )
        .bind(credential.principal().tenant_id())
        .bind(credential.principal().user_id())
        .bind(credential.server_id())
        .bind(store.request_id())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        if transaction_receipt.is_some() || credential_receipt.is_some() {
            let replay = transaction_receipt.as_deref() == Some(consume_intent.as_str())
                && credential_receipt.as_deref() == Some(credential_intent.as_str());
            let transaction = load_sqlite_transaction_tx(&mut tx, consume.transaction_id()).await?;
            let credential =
                load_sqlite_credential_tx(&mut tx, credential.principal(), credential.server_id())
                    .await?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(if replay {
                TransitionOutcome::ExactReplay {
                    authoritative: McpOAuthCallbackCompletion {
                        transaction: transaction.ok_or_else(RepositoryError::invalid_data)?,
                        credential: credential.ok_or_else(RepositoryError::invalid_data)?,
                    },
                }
            } else {
                TransitionOutcome::StateConflict
            });
        }
        let Some(transaction) =
            load_sqlite_transaction_tx(&mut tx, consume.transaction_id()).await?
        else {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        if transaction.state() != McpOAuthTransactionState::Pending
            || transaction.version() != consume.expected_version()
            || transaction.expires_at() <= consume.consumed_at()
            || !constant_time_eq(
                transaction.state_hash().as_bytes(),
                consume.callback_state_hash().as_bytes(),
            )
            || !callback_authority_matches(&transaction, consume, store)
        {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let existing =
            load_sqlite_credential_tx(&mut tx, credential.principal(), credential.server_id())
                .await?;
        let generation_ok = match (&existing, store.expected_generation()) {
            (None, None) => credential.generation() == 1,
            (Some(current), Some(expected)) => {
                current.generation() == expected && credential.generation() == expected + 1
            }
            _ => false,
        };
        if !generation_ok {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        sqlx::query("INSERT INTO mcp_oauth_credentials(tenant_id,user_id,server_id,issuer,client_id,resource,scopes_json,token_type,credential_generation,access_ciphertext,access_token_hash,refresh_ciphertext,refresh_token_hash,access_expires_at,updated_at,revoked_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL) ON CONFLICT(tenant_id,user_id,server_id) DO UPDATE SET issuer=excluded.issuer,client_id=excluded.client_id,resource=excluded.resource,scopes_json=excluded.scopes_json,token_type=excluded.token_type,credential_generation=excluded.credential_generation,access_ciphertext=excluded.access_ciphertext,access_token_hash=excluded.access_token_hash,refresh_ciphertext=excluded.refresh_ciphertext,refresh_token_hash=excluded.refresh_token_hash,access_expires_at=excluded.access_expires_at,updated_at=excluded.updated_at,revoked_at=NULL")
            .bind(credential.principal().tenant_id()).bind(credential.principal().user_id())
            .bind(credential.server_id()).bind(credential.issuer()).bind(credential.client_id())
            .bind(credential.resource()).bind(serde_json::to_string(credential.scopes()).map_err(|_| RepositoryError::invalid_data())?)
            .bind(credential.token_type()).bind(i64::try_from(credential.generation()).map_err(|_| RepositoryError::invalid_data())?)
            .bind(store.access_token().expose_ciphertext()).bind(store.access_token_hash())
            .bind(store.refresh_token().map(McpSecretCiphertext::expose_ciphertext))
            .bind(store.refresh_token_hash()).bind(credential.access_expires_at().map(sqlite_time))
            .bind(sqlite_time(credential.updated_at())).execute(&mut *tx).await.map_err(RepositoryError::storage)?;
        insert_sqlite_credential_receipt(
            &mut tx,
            credential,
            store.request_id(),
            &credential_intent,
        )
        .await?;
        let transaction_version = transaction.version() + 1;
        let rows = sqlx::query(
            "UPDATE mcp_oauth_transactions
             SET transaction_state='consumed',transaction_version=?,
                 transaction_ciphertext=?,transaction_secret_hash=?,consumed_at=?
             WHERE transaction_id=? AND transaction_version=? AND transaction_state='pending'",
        )
        .bind(i64::try_from(transaction_version).map_err(|_| RepositoryError::invalid_data())?)
        .bind(SCRUBBED_CIPHERTEXT)
        .bind(SCRUBBED_SECRET_HASH)
        .bind(sqlite_time(consume.consumed_at()))
        .bind(consume.transaction_id().as_str())
        .bind(i64::try_from(transaction.version()).map_err(|_| RepositoryError::invalid_data())?)
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if rows != 1 {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        sqlx::query("INSERT INTO mcp_oauth_transaction_receipts(transaction_id,request_id,intent_hash,result_version,created_at) VALUES(?,?,?,?,?)")
            .bind(consume.transaction_id().as_str()).bind(consume.request_id()).bind(consume_intent)
            .bind(i64::try_from(transaction_version).map_err(|_| RepositoryError::invalid_data())?)
            .bind(sqlite_time(consume.consumed_at())).execute(&mut *tx).await.map_err(RepositoryError::storage)?;
        let result = McpOAuthCallbackCompletion {
            transaction: load_sqlite_transaction_tx(&mut tx, consume.transaction_id())
                .await?
                .ok_or_else(RepositoryError::invalid_data)?,
            credential: load_sqlite_credential_tx(
                &mut tx,
                credential.principal(),
                credential.server_id(),
            )
            .await?
            .ok_or_else(RepositoryError::invalid_data)?,
        };
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result })
    }

    #[cfg(any())]
    async fn complete_mcp_oauth_callback(
        &self,
        command: CompleteMcpOAuthCallbackCommand,
    ) -> Result<TransitionOutcome<McpOAuthCallbackCompletion>, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let consume = command.consume();
        let store = command.credential();
        let credential = store.credential();
        let consume_intent = intent_hash(consume)?;
        let credential_intent = intent_hash(store)?;
        let transaction_receipt = sqlx::query_scalar::<_, String>(
            "SELECT intent_hash FROM mcp_oauth_transaction_receipts
             WHERE transaction_id=$1 AND request_id=$2",
        )
        .bind(consume.transaction_id().as_str())
        .bind(consume.request_id())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let credential_receipt = sqlx::query_scalar::<_, String>(
            "SELECT intent_hash FROM mcp_oauth_credential_receipts
             WHERE tenant_id=$1 AND user_id=$2 AND server_id=$3 AND request_id=$4",
        )
        .bind(credential.principal().tenant_id())
        .bind(credential.principal().user_id())
        .bind(credential.server_id())
        .bind(store.request_id())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        if transaction_receipt.is_some() || credential_receipt.is_some() {
            let replay = transaction_receipt.as_deref() == Some(consume_intent.as_str())
                && credential_receipt.as_deref() == Some(credential_intent.as_str());
            let transaction =
                load_postgres_transaction_tx(&mut tx, consume.transaction_id()).await?;
            let credential = load_postgres_credential_tx(
                &mut tx,
                credential.principal(),
                credential.server_id(),
            )
            .await?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(if replay {
                TransitionOutcome::ExactReplay {
                    authoritative: McpOAuthCallbackCompletion {
                        transaction: transaction.ok_or_else(RepositoryError::invalid_data)?,
                        credential: credential.ok_or_else(RepositoryError::invalid_data)?,
                    },
                }
            } else {
                TransitionOutcome::StateConflict
            });
        }
        let Some(transaction) =
            load_postgres_transaction_tx(&mut tx, consume.transaction_id()).await?
        else {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        if transaction.state() != McpOAuthTransactionState::Pending
            || transaction.version() != consume.expected_version()
            || transaction.expires_at() <= consume.consumed_at()
            || !constant_time_eq(
                transaction.state_hash().as_bytes(),
                consume.callback_state_hash().as_bytes(),
            )
            || !callback_authority_matches(&transaction, consume, store)
        {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let existing =
            load_postgres_credential_tx(&mut tx, credential.principal(), credential.server_id())
                .await?;
        let generation_ok = match (&existing, store.expected_generation()) {
            (None, None) => credential.generation() == 1,
            (Some(current), Some(expected)) => {
                current.generation() == expected && credential.generation() == expected + 1
            }
            _ => false,
        };
        if !generation_ok {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        sqlx::query("INSERT INTO mcp_oauth_credentials(tenant_id,user_id,server_id,issuer,client_id,resource,scopes_json,token_type,credential_generation,access_ciphertext,access_token_hash,refresh_ciphertext,refresh_token_hash,access_expires_at,updated_at,revoked_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,NULL) ON CONFLICT(tenant_id,user_id,server_id) DO UPDATE SET issuer=excluded.issuer,client_id=excluded.client_id,resource=excluded.resource,scopes_json=excluded.scopes_json,token_type=excluded.token_type,credential_generation=excluded.credential_generation,access_ciphertext=excluded.access_ciphertext,access_token_hash=excluded.access_token_hash,refresh_ciphertext=excluded.refresh_ciphertext,refresh_token_hash=excluded.refresh_token_hash,access_expires_at=excluded.access_expires_at,updated_at=excluded.updated_at,revoked_at=NULL")
            .bind(credential.principal().tenant_id()).bind(credential.principal().user_id()).bind(credential.server_id())
            .bind(credential.issuer()).bind(credential.client_id()).bind(credential.resource())
            .bind(serde_json::to_value(credential.scopes()).map_err(|_| RepositoryError::invalid_data())?)
            .bind(credential.token_type()).bind(i64::try_from(credential.generation()).map_err(|_| RepositoryError::invalid_data())?)
            .bind(store.access_token().expose_ciphertext()).bind(store.access_token_hash())
            .bind(store.refresh_token().map(McpSecretCiphertext::expose_ciphertext)).bind(store.refresh_token_hash())
            .bind(credential.access_expires_at()).bind(credential.updated_at())
            .execute(&mut *tx).await.map_err(RepositoryError::storage)?;
        insert_postgres_credential_receipt(
            &mut tx,
            credential,
            store.request_id(),
            &credential_intent,
        )
        .await?;
        let transaction_version = transaction.version() + 1;
        let rows = sqlx::query(
            "UPDATE mcp_oauth_transactions
             SET transaction_state='consumed',transaction_version=$1,consumed_at=$2
             WHERE transaction_id=$3 AND transaction_version=$4 AND transaction_state='pending'",
        )
        .bind(i64::try_from(transaction_version).map_err(|_| RepositoryError::invalid_data())?)
        .bind(consume.consumed_at())
        .bind(consume.transaction_id().as_str())
        .bind(i64::try_from(transaction.version()).map_err(|_| RepositoryError::invalid_data())?)
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if rows != 1 {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        sqlx::query("INSERT INTO mcp_oauth_transaction_receipts(transaction_id,request_id,intent_hash,result_version,created_at) VALUES($1,$2,$3,$4,$5)")
            .bind(consume.transaction_id().as_str()).bind(consume.request_id()).bind(consume_intent)
            .bind(i64::try_from(transaction_version).map_err(|_| RepositoryError::invalid_data())?)
            .bind(consume.consumed_at()).execute(&mut *tx).await.map_err(RepositoryError::storage)?;
        let result = McpOAuthCallbackCompletion {
            transaction: load_postgres_transaction_tx(&mut tx, consume.transaction_id())
                .await?
                .ok_or_else(RepositoryError::invalid_data)?,
            credential: load_postgres_credential_tx(
                &mut tx,
                credential.principal(),
                credential.server_id(),
            )
            .await?
            .ok_or_else(RepositoryError::invalid_data)?,
        };
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result })
    }

    #[cfg(any())]
    async fn complete_mcp_oauth_callback(
        &self,
        command: CompleteMcpOAuthCallbackCommand,
    ) -> Result<TransitionOutcome<McpOAuthCallbackCompletion>, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let consume = command.consume();
        let store = command.credential();
        let credential = store.credential();
        let consume_intent = intent_hash(consume)?;
        let credential_intent = intent_hash(store)?;
        let transaction_receipt = sqlx::query_scalar::<_, String>(
            "SELECT intent_hash FROM mcp_oauth_transaction_receipts
             WHERE transaction_id=$1 AND request_id=$2",
        )
        .bind(consume.transaction_id().as_str())
        .bind(consume.request_id())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let credential_receipt = sqlx::query_scalar::<_, String>(
            "SELECT intent_hash FROM mcp_oauth_credential_receipts
             WHERE tenant_id=$1 AND user_id=$2 AND server_id=$3 AND request_id=$4",
        )
        .bind(credential.principal().tenant_id())
        .bind(credential.principal().user_id())
        .bind(credential.server_id())
        .bind(store.request_id())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        if transaction_receipt.is_some() || credential_receipt.is_some() {
            let replay = transaction_receipt.as_deref() == Some(consume_intent.as_str())
                && credential_receipt.as_deref() == Some(credential_intent.as_str());
            let transaction =
                load_postgres_transaction_tx(&mut tx, consume.transaction_id()).await?;
            let credential = load_postgres_credential_tx(
                &mut tx,
                credential.principal(),
                credential.server_id(),
            )
            .await?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(if replay {
                TransitionOutcome::ExactReplay {
                    authoritative: McpOAuthCallbackCompletion {
                        transaction: transaction.ok_or_else(RepositoryError::invalid_data)?,
                        credential: credential.ok_or_else(RepositoryError::invalid_data)?,
                    },
                }
            } else {
                TransitionOutcome::StateConflict
            });
        }
        let Some(transaction) =
            load_postgres_transaction_tx(&mut tx, consume.transaction_id()).await?
        else {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        if transaction.state() != McpOAuthTransactionState::Pending
            || transaction.version() != consume.expected_version()
            || transaction.expires_at() <= consume.consumed_at()
            || !constant_time_eq(
                transaction.state_hash().as_bytes(),
                consume.callback_state_hash().as_bytes(),
            )
            || !callback_authority_matches(&transaction, consume, store)
        {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let existing =
            load_postgres_credential_tx(&mut tx, credential.principal(), credential.server_id())
                .await?;
        let generation_ok = match (&existing, store.expected_generation()) {
            (None, None) => credential.generation() == 1,
            (Some(current), Some(expected)) => {
                current.generation() == expected && credential.generation() == expected + 1
            }
            _ => false,
        };
        if !generation_ok {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        sqlx::query("INSERT INTO mcp_oauth_credentials(tenant_id,user_id,server_id,issuer,client_id,resource,scopes_json,token_type,credential_generation,access_ciphertext,access_token_hash,refresh_ciphertext,refresh_token_hash,access_expires_at,updated_at,revoked_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,NULL) ON CONFLICT(tenant_id,user_id,server_id) DO UPDATE SET issuer=excluded.issuer,client_id=excluded.client_id,resource=excluded.resource,scopes_json=excluded.scopes_json,token_type=excluded.token_type,credential_generation=excluded.credential_generation,access_ciphertext=excluded.access_ciphertext,access_token_hash=excluded.access_token_hash,refresh_ciphertext=excluded.refresh_ciphertext,refresh_token_hash=excluded.refresh_token_hash,access_expires_at=excluded.access_expires_at,updated_at=excluded.updated_at,revoked_at=NULL")
            .bind(credential.principal().tenant_id()).bind(credential.principal().user_id()).bind(credential.server_id())
            .bind(credential.issuer()).bind(credential.client_id()).bind(credential.resource())
            .bind(serde_json::to_value(credential.scopes()).map_err(|_| RepositoryError::invalid_data())?)
            .bind(credential.token_type()).bind(i64::try_from(credential.generation()).map_err(|_| RepositoryError::invalid_data())?)
            .bind(store.access_token().expose_ciphertext()).bind(store.access_token_hash())
            .bind(store.refresh_token().map(McpSecretCiphertext::expose_ciphertext)).bind(store.refresh_token_hash())
            .bind(credential.access_expires_at()).bind(credential.updated_at())
            .execute(&mut *tx).await.map_err(RepositoryError::storage)?;
        insert_postgres_credential_receipt(
            &mut tx,
            credential,
            store.request_id(),
            &credential_intent,
        )
        .await?;
        let transaction_version = transaction.version() + 1;
        let rows = sqlx::query(
            "UPDATE mcp_oauth_transactions
             SET transaction_state='consumed',transaction_version=$1,consumed_at=$2
             WHERE transaction_id=$3 AND transaction_version=$4 AND transaction_state='pending'",
        )
        .bind(i64::try_from(transaction_version).map_err(|_| RepositoryError::invalid_data())?)
        .bind(consume.consumed_at())
        .bind(consume.transaction_id().as_str())
        .bind(i64::try_from(transaction.version()).map_err(|_| RepositoryError::invalid_data())?)
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if rows != 1 {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        sqlx::query("INSERT INTO mcp_oauth_transaction_receipts(transaction_id,request_id,intent_hash,result_version,created_at) VALUES($1,$2,$3,$4,$5)")
            .bind(consume.transaction_id().as_str()).bind(consume.request_id()).bind(consume_intent)
            .bind(i64::try_from(transaction_version).map_err(|_| RepositoryError::invalid_data())?)
            .bind(consume.consumed_at()).execute(&mut *tx).await.map_err(RepositoryError::storage)?;
        let result = McpOAuthCallbackCompletion {
            transaction: load_postgres_transaction_tx(&mut tx, consume.transaction_id())
                .await?
                .ok_or_else(RepositoryError::invalid_data)?,
            credential: load_postgres_credential_tx(
                &mut tx,
                credential.principal(),
                credential.server_id(),
            )
            .await?
            .ok_or_else(RepositoryError::invalid_data)?,
        };
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result })
    }

    async fn load_mcp_oauth_credential(
        &self,
        principal: &McpInteractionPrincipal,
        server_id: &str,
    ) -> Result<Option<McpOAuthCredential>, RepositoryError> {
        let row = sqlx::query(AssertSqlSafe(format!("SELECT {CREDENTIAL_COLUMNS} FROM mcp_oauth_credentials WHERE tenant_id=? AND user_id=? AND server_id=?")))
            .bind(principal.tenant_id()).bind(principal.user_id()).bind(server_id)
            .fetch_optional(&self.pool).await.map_err(RepositoryError::storage)?;
        row.as_ref().map(parse_sqlite_credential).transpose()
    }

    async fn load_mcp_oauth_credential_secret(
        &self,
        principal: &McpInteractionPrincipal,
        server_id: &str,
    ) -> Result<Option<McpOAuthCredentialSecret>, RepositoryError> {
        let row = sqlx::query("SELECT access_ciphertext,access_token_hash,refresh_ciphertext,refresh_token_hash,revoked_at FROM mcp_oauth_credentials WHERE tenant_id=? AND user_id=? AND server_id=?")
            .bind(principal.tenant_id()).bind(principal.user_id()).bind(server_id)
            .fetch_optional(&self.pool).await.map_err(RepositoryError::storage)?;
        row.filter(|row| {
            row.try_get::<Option<String>, _>("revoked_at")
                .ok()
                .flatten()
                .is_none()
        })
        .map(parse_sqlite_credential_secret)
        .transpose()
    }

    async fn claim_mcp_oauth_refresh(
        &self,
        command: ClaimMcpOAuthRefreshCommand,
    ) -> Result<bool, RepositoryError> {
        let _guard = self.writer.lock().await;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let current =
            load_sqlite_credential_tx(&mut tx, command.principal(), command.server_id()).await?;
        if !current.is_some_and(|credential| {
            credential.revoked_at().is_none()
                && credential.generation() == command.expected_generation()
        }) {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(false);
        }
        let stale_dispatched = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM mcp_oauth_refresh_leases
             WHERE tenant_id=? AND user_id=? AND server_id=?
               AND credential_generation=? AND dispatched_at IS NOT NULL
               AND lease_expires_at<=?",
        )
        .bind(command.principal().tenant_id())
        .bind(command.principal().user_id())
        .bind(command.server_id())
        .bind(
            i64::try_from(command.expected_generation())
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .bind(sqlite_time(command.now()))
        .fetch_one(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
            == 1;
        if stale_dispatched {
            quarantine_sqlite_refresh_tx(
                &mut tx,
                command.principal(),
                command.server_id(),
                command.expected_generation(),
                None,
                command.now(),
            )
            .await?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(false);
        }
        let rows = sqlx::query(
            "INSERT INTO mcp_oauth_refresh_leases(
               tenant_id,user_id,server_id,credential_generation,lease_owner,lease_expires_at,updated_at,dispatched_at
             ) VALUES(?,?,?,?,?,?,?,NULL)
             ON CONFLICT(tenant_id,user_id,server_id) DO UPDATE SET
               credential_generation=excluded.credential_generation,
               lease_owner=excluded.lease_owner,
               lease_expires_at=excluded.lease_expires_at,
               updated_at=excluded.updated_at,
               dispatched_at=NULL
             WHERE (mcp_oauth_refresh_leases.lease_expires_at<=excluded.updated_at
                    AND mcp_oauth_refresh_leases.dispatched_at IS NULL)
                OR (mcp_oauth_refresh_leases.lease_owner=excluded.lease_owner
                    AND mcp_oauth_refresh_leases.credential_generation=excluded.credential_generation
                    AND mcp_oauth_refresh_leases.dispatched_at IS NULL)",
        )
        .bind(command.principal().tenant_id())
        .bind(command.principal().user_id())
        .bind(command.server_id())
        .bind(
            i64::try_from(command.expected_generation())
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .bind(command.owner())
        .bind(sqlite_time(command.lease_expires_at()))
        .bind(sqlite_time(command.now()))
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(rows == 1)
    }

    async fn mark_mcp_oauth_refresh_dispatched(
        &self,
        principal: &McpInteractionPrincipal,
        server_id: &str,
        generation: u64,
        owner: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        let _guard = self.writer.lock().await;
        let rows = sqlx::query(
            "UPDATE mcp_oauth_refresh_leases
             SET dispatched_at=?
             WHERE tenant_id=? AND user_id=? AND server_id=?
               AND credential_generation=? AND lease_owner=?
               AND dispatched_at IS NULL AND lease_expires_at>?",
        )
        .bind(sqlite_time(now))
        .bind(principal.tenant_id())
        .bind(principal.user_id())
        .bind(server_id)
        .bind(i64::try_from(generation).map_err(|_| RepositoryError::invalid_data())?)
        .bind(owner)
        .bind(sqlite_time(now))
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        Ok(rows == 1)
    }

    async fn quarantine_mcp_oauth_refresh(
        &self,
        principal: &McpInteractionPrincipal,
        server_id: &str,
        generation: u64,
        owner: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        let _guard = self.writer.lock().await;
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let quarantined = quarantine_sqlite_refresh_tx(
            &mut tx,
            principal,
            server_id,
            generation,
            Some(owner),
            now,
        )
        .await?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(quarantined)
    }

    async fn release_mcp_oauth_refresh(
        &self,
        principal: &McpInteractionPrincipal,
        server_id: &str,
        generation: u64,
        owner: &str,
    ) -> Result<bool, RepositoryError> {
        let _guard = self.writer.lock().await;
        let rows = sqlx::query(
            "DELETE FROM mcp_oauth_refresh_leases
             WHERE tenant_id=? AND user_id=? AND server_id=?
               AND credential_generation=? AND lease_owner=?
               AND dispatched_at IS NULL",
        )
        .bind(principal.tenant_id())
        .bind(principal.user_id())
        .bind(server_id)
        .bind(i64::try_from(generation).map_err(|_| RepositoryError::invalid_data())?)
        .bind(owner)
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        Ok(rows == 1)
    }

    async fn expire_mcp_oauth_transactions(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, RepositoryError> {
        if limit == 0 || limit > MAX_EXPIRY_BATCH {
            return Err(RepositoryError::invalid_data());
        }
        let _guard = self.writer.lock().await;
        let rows = sqlx::query(
            "UPDATE mcp_oauth_transactions
             SET transaction_state='expired',
                 transaction_version=transaction_version+1,
                 transaction_ciphertext=?,
                 transaction_secret_hash=?,
                 consumed_at=?
             WHERE transaction_id IN (
               SELECT transaction_id
               FROM mcp_oauth_transactions
               WHERE transaction_state='pending' AND expires_at<=?
               ORDER BY expires_at,transaction_id
               LIMIT ?
             )",
        )
        .bind(SCRUBBED_CIPHERTEXT)
        .bind(SCRUBBED_SECRET_HASH)
        .bind(sqlite_time(now))
        .bind(sqlite_time(now))
        .bind(i64::from(limit))
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        Ok(rows)
    }

    async fn delete_mcp_oauth_credential(
        &self,
        principal: &McpInteractionPrincipal,
        server_id: &str,
        request_id: &str,
        now: DateTime<Utc>,
    ) -> Result<TransitionOutcome<McpOAuthCredential>, RepositoryError> {
        delete_sqlite_credential(self, principal, server_id, request_id, now).await
    }
}

async fn load_sqlite_transaction_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: &McpOAuthTransactionId,
) -> Result<Option<McpOAuthTransaction>, RepositoryError> {
    let row = sqlx::query(AssertSqlSafe(format!(
        "SELECT {TRANSACTION_COLUMNS} FROM mcp_oauth_transactions WHERE transaction_id=?"
    )))
    .bind(id.as_str())
    .fetch_optional(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    row.as_ref().map(parse_sqlite_transaction).transpose()
}

async fn load_sqlite_credential_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    principal: &McpInteractionPrincipal,
    server_id: &str,
) -> Result<Option<McpOAuthCredential>, RepositoryError> {
    let row = sqlx::query(AssertSqlSafe(format!("SELECT {CREDENTIAL_COLUMNS} FROM mcp_oauth_credentials WHERE tenant_id=? AND user_id=? AND server_id=?")))
        .bind(principal.tenant_id()).bind(principal.user_id()).bind(server_id)
        .fetch_optional(&mut **tx).await.map_err(RepositoryError::storage)?;
    row.as_ref().map(parse_sqlite_credential).transpose()
}

async fn quarantine_sqlite_refresh_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    principal: &McpInteractionPrincipal,
    server_id: &str,
    generation: u64,
    owner: Option<&str>,
    now: DateTime<Utc>,
) -> Result<bool, RepositoryError> {
    let generation_i64 = i64::try_from(generation).map_err(|_| RepositoryError::invalid_data())?;
    let lease_exists = if let Some(owner) = owner {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM mcp_oauth_refresh_leases
             WHERE tenant_id=? AND user_id=? AND server_id=?
               AND credential_generation=? AND lease_owner=?
               AND dispatched_at IS NOT NULL",
        )
        .bind(principal.tenant_id())
        .bind(principal.user_id())
        .bind(server_id)
        .bind(generation_i64)
        .bind(owner)
        .fetch_one(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?
            == 1
    } else {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM mcp_oauth_refresh_leases
             WHERE tenant_id=? AND user_id=? AND server_id=?
               AND credential_generation=? AND dispatched_at IS NOT NULL",
        )
        .bind(principal.tenant_id())
        .bind(principal.user_id())
        .bind(server_id)
        .bind(generation_i64)
        .fetch_one(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?
            == 1
    };
    let current = load_sqlite_credential_tx(tx, principal, server_id).await?;
    if !lease_exists
        || !current.is_some_and(|credential| {
            credential.generation() == generation && credential.revoked_at().is_none()
        })
    {
        return Ok(false);
    }
    let next_generation = generation
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let rows = sqlx::query(
        "UPDATE mcp_oauth_credentials
         SET credential_generation=?,access_ciphertext=?,access_token_hash=?,
             refresh_ciphertext=NULL,refresh_token_hash=NULL,
             access_expires_at=NULL,updated_at=?,revoked_at=?
         WHERE tenant_id=? AND user_id=? AND server_id=?
           AND credential_generation=? AND revoked_at IS NULL",
    )
    .bind(i64::try_from(next_generation).map_err(|_| RepositoryError::invalid_data())?)
    .bind(SCRUBBED_CIPHERTEXT)
    .bind(SCRUBBED_SECRET_HASH)
    .bind(sqlite_time(now))
    .bind(sqlite_time(now))
    .bind(principal.tenant_id())
    .bind(principal.user_id())
    .bind(server_id)
    .bind(generation_i64)
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if rows != 1 {
        return Ok(false);
    }
    sqlx::query(
        "DELETE FROM mcp_oauth_refresh_leases
         WHERE tenant_id=? AND user_id=? AND server_id=?
           AND credential_generation=?",
    )
    .bind(principal.tenant_id())
    .bind(principal.user_id())
    .bind(server_id)
    .bind(generation_i64)
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(true)
}

async fn insert_sqlite_credential_receipt(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    credential: &McpOAuthCredential,
    request_id: &str,
    intent: &str,
) -> Result<(), RepositoryError> {
    sqlx::query("INSERT INTO mcp_oauth_credential_receipts(tenant_id,user_id,server_id,request_id,intent_hash,result_generation,created_at) VALUES(?,?,?,?,?,?,?)")
        .bind(credential.principal().tenant_id()).bind(credential.principal().user_id())
        .bind(credential.server_id()).bind(request_id).bind(intent)
        .bind(i64::try_from(credential.generation()).map_err(|_| RepositoryError::invalid_data())?)
        .bind(sqlite_time(credential.updated_at())).execute(&mut **tx).await.map_err(RepositoryError::storage)?;
    Ok(())
}

async fn delete_sqlite_credential(
    repository: &SqliteDurableRepository,
    principal: &McpInteractionPrincipal,
    server_id: &str,
    request_id: &str,
    now: DateTime<Utc>,
) -> Result<TransitionOutcome<McpOAuthCredential>, RepositoryError> {
    if request_id.is_empty() || request_id.len() > 256 {
        return Err(RepositoryError::invalid_data());
    }
    let _guard = repository.writer.lock().await;
    let mut tx = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    let intent = intent_hash(&(principal, server_id, "delete"))?;
    if let Some(row) = sqlx::query("SELECT intent_hash FROM mcp_oauth_credential_receipts WHERE tenant_id=? AND user_id=? AND server_id=? AND request_id=?")
        .bind(principal.tenant_id()).bind(principal.user_id()).bind(server_id).bind(request_id)
        .fetch_optional(&mut *tx).await.map_err(RepositoryError::storage)?
    {
        let same = row.try_get::<String, _>("intent_hash").map_err(|_| RepositoryError::invalid_data())? == intent;
        let current = load_sqlite_credential_tx(&mut tx, principal, server_id).await?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(if same { TransitionOutcome::ExactReplay { authoritative: current.ok_or_else(RepositoryError::invalid_data)? } } else { TransitionOutcome::StateConflict });
    }
    let Some(current) = load_sqlite_credential_tx(&mut tx, principal, server_id).await? else {
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    let generation = current.generation() + 1;
    sqlx::query(
        "DELETE FROM mcp_oauth_refresh_leases
         WHERE tenant_id=? AND user_id=? AND server_id=?",
    )
    .bind(principal.tenant_id())
    .bind(principal.user_id())
    .bind(server_id)
    .execute(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    sqlx::query("UPDATE mcp_oauth_credentials SET credential_generation=?,access_ciphertext='enc:v1:deleted',access_token_hash=?,refresh_ciphertext=NULL,refresh_token_hash=NULL,updated_at=?,revoked_at=? WHERE tenant_id=? AND user_id=? AND server_id=?")
        .bind(i64::try_from(generation).map_err(|_| RepositoryError::invalid_data())?)
        .bind("0".repeat(64)).bind(sqlite_time(now)).bind(sqlite_time(now))
        .bind(principal.tenant_id()).bind(principal.user_id()).bind(server_id)
        .execute(&mut *tx).await.map_err(RepositoryError::storage)?;
    let result = load_sqlite_credential_tx(&mut tx, principal, server_id)
        .await?
        .ok_or_else(RepositoryError::invalid_data)?;
    insert_sqlite_credential_receipt(&mut tx, &result, request_id, &intent).await?;
    tx.commit().await.map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed { result })
}

fn parse_sqlite_credential_secret(
    row: sqlx::sqlite::SqliteRow,
) -> Result<McpOAuthCredentialSecret, RepositoryError> {
    Ok(McpOAuthCredentialSecret {
        access_token: McpSecretCiphertext::new(
            row.try_get::<String, _>("access_ciphertext")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        access_token_hash: row
            .try_get("access_token_hash")
            .map_err(|_| RepositoryError::invalid_data())?,
        refresh_token: row
            .try_get::<Option<String>, _>("refresh_ciphertext")
            .map_err(|_| RepositoryError::invalid_data())?
            .map(McpSecretCiphertext::new)
            .transpose()?,
        refresh_token_hash: row
            .try_get("refresh_token_hash")
            .map_err(|_| RepositoryError::invalid_data())?,
    })
}

#[async_trait]
impl McpOAuthDurableRepository for PostgresDurableRepository {
    async fn create_mcp_oauth_transaction(
        &self,
        command: CreateMcpOAuthTransactionCommand,
    ) -> Result<TransitionOutcome<McpOAuthTransaction>, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let transaction = command.transaction();
        let intent = intent_hash(&command)?;
        if let Some(row) = sqlx::query(AssertSqlSafe(format!("SELECT {TRANSACTION_COLUMNS},creation_intent_hash FROM mcp_oauth_transactions WHERE transaction_id=$1 FOR UPDATE")))
            .bind(transaction.transaction_id().as_str()).fetch_optional(&mut *tx).await.map_err(RepositoryError::storage)?
        {
            let existing = parse_postgres_transaction(&row)?;
            let same = row.try_get::<String, _>("creation_intent_hash").map_err(|_| RepositoryError::invalid_data())? == intent;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(if same { TransitionOutcome::ExactReplay { authoritative: existing } } else { TransitionOutcome::StateConflict });
        }
        sqlx::query("INSERT INTO mcp_oauth_transactions(transaction_id,tenant_id,user_id,server_id,issuer,resource,client_id,redirect_uri,scopes_json,state_hash,transaction_state,transaction_version,transaction_ciphertext,transaction_secret_hash,expires_at,created_at,consumed_at,creation_intent_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,NULL,$17)")
            .bind(transaction.transaction_id().as_str()).bind(transaction.principal().tenant_id())
            .bind(transaction.principal().user_id()).bind(transaction.server_id()).bind(transaction.issuer())
            .bind(transaction.resource()).bind(transaction.client_id()).bind(transaction.redirect_uri())
            .bind(serde_json::to_value(transaction.scopes()).map_err(|_| RepositoryError::invalid_data())?)
            .bind(transaction.state_hash()).bind(enum_wire(&transaction.state())?)
            .bind(i64::try_from(transaction.version()).map_err(|_| RepositoryError::invalid_data())?)
            .bind(command.transaction_secret().expose_ciphertext()).bind(command.transaction_secret_hash())
            .bind(transaction.expires_at()).bind(transaction.created_at()).bind(intent)
            .execute(&mut *tx).await.map_err(RepositoryError::storage)?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed {
            result: transaction.clone(),
        })
    }

    async fn load_mcp_oauth_transaction(
        &self,
        transaction_id: &McpOAuthTransactionId,
    ) -> Result<Option<McpOAuthTransaction>, RepositoryError> {
        let row = sqlx::query(AssertSqlSafe(format!(
            "SELECT {TRANSACTION_COLUMNS} FROM mcp_oauth_transactions WHERE transaction_id=$1"
        )))
        .bind(transaction_id.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        row.as_ref().map(parse_postgres_transaction).transpose()
    }

    async fn load_mcp_oauth_transaction_secret(
        &self,
        transaction_id: &McpOAuthTransactionId,
    ) -> Result<Option<McpOAuthTransactionSecret>, RepositoryError> {
        let row = sqlx::query("SELECT transaction_ciphertext,transaction_secret_hash FROM mcp_oauth_transactions WHERE transaction_id=$1 AND transaction_state='pending'")
            .bind(transaction_id.as_str()).fetch_optional(&self.pool).await.map_err(RepositoryError::storage)?;
        row.map(|row| {
            Ok(McpOAuthTransactionSecret {
                transaction_secret: McpSecretCiphertext::new(
                    row.try_get::<String, _>("transaction_ciphertext")
                        .map_err(|_| RepositoryError::invalid_data())?,
                )?,
                transaction_secret_hash: row
                    .try_get("transaction_secret_hash")
                    .map_err(|_| RepositoryError::invalid_data())?,
            })
        })
        .transpose()
    }

    async fn consume_mcp_oauth_transaction(
        &self,
        command: ConsumeMcpOAuthTransactionCommand,
    ) -> Result<TransitionOutcome<McpOAuthTransaction>, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let intent = intent_hash(&command)?;
        if let Some(row) = sqlx::query("SELECT intent_hash FROM mcp_oauth_transaction_receipts WHERE transaction_id=$1 AND request_id=$2")
            .bind(command.transaction_id().as_str()).bind(command.request_id()).fetch_optional(&mut *tx).await.map_err(RepositoryError::storage)?
        {
            let same = row.try_get::<String, _>("intent_hash").map_err(|_| RepositoryError::invalid_data())? == intent;
            let current = load_postgres_transaction_tx(&mut tx, command.transaction_id()).await?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(if same { TransitionOutcome::ExactReplay { authoritative: current.ok_or_else(RepositoryError::invalid_data)? } } else { TransitionOutcome::StateConflict });
        }
        let Some(current) = load_postgres_transaction_tx(&mut tx, command.transaction_id()).await?
        else {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        if current.principal() != command.principal()
            || current.state() != McpOAuthTransactionState::Pending
            || current.version() != command.expected_version()
            || current.expires_at() <= command.consumed_at()
            || !constant_time_eq(
                current.state_hash().as_bytes(),
                command.callback_state_hash().as_bytes(),
            )
        {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let version = current.version() + 1;
        sqlx::query("UPDATE mcp_oauth_transactions SET transaction_state='consumed',transaction_version=$1,transaction_ciphertext=$2,transaction_secret_hash=$3,consumed_at=$4 WHERE transaction_id=$5 AND transaction_version=$6")
            .bind(i64::try_from(version).map_err(|_| RepositoryError::invalid_data())?)
            .bind(SCRUBBED_CIPHERTEXT).bind(SCRUBBED_SECRET_HASH).bind(command.consumed_at())
            .bind(command.transaction_id().as_str()).bind(i64::try_from(current.version()).map_err(|_| RepositoryError::invalid_data())?)
            .execute(&mut *tx).await.map_err(RepositoryError::storage)?;
        sqlx::query("INSERT INTO mcp_oauth_transaction_receipts(transaction_id,request_id,intent_hash,result_version,created_at) VALUES($1,$2,$3,$4,$5)")
            .bind(command.transaction_id().as_str()).bind(command.request_id()).bind(intent)
            .bind(i64::try_from(version).map_err(|_| RepositoryError::invalid_data())?).bind(command.consumed_at())
            .execute(&mut *tx).await.map_err(RepositoryError::storage)?;
        let result = load_postgres_transaction_tx(&mut tx, command.transaction_id())
            .await?
            .ok_or_else(RepositoryError::invalid_data)?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result })
    }

    async fn store_mcp_oauth_credential(
        &self,
        command: StoreMcpOAuthCredentialCommand,
    ) -> Result<TransitionOutcome<McpOAuthCredential>, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let credential = command.credential();
        let intent = intent_hash(&command)?;
        if let Some(row) = sqlx::query("SELECT intent_hash FROM mcp_oauth_credential_receipts WHERE tenant_id=$1 AND user_id=$2 AND server_id=$3 AND request_id=$4")
            .bind(credential.principal().tenant_id()).bind(credential.principal().user_id()).bind(credential.server_id()).bind(command.request_id())
            .fetch_optional(&mut *tx).await.map_err(RepositoryError::storage)?
        {
            let same = row.try_get::<String, _>("intent_hash").map_err(|_| RepositoryError::invalid_data())? == intent;
            let current = load_postgres_credential_tx(&mut tx, credential.principal(), credential.server_id()).await?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(if same { TransitionOutcome::ExactReplay { authoritative: current.ok_or_else(RepositoryError::invalid_data)? } } else { TransitionOutcome::StateConflict });
        }
        let existing =
            load_postgres_credential_tx(&mut tx, credential.principal(), credential.server_id())
                .await?;
        let refresh_fence_ok = if let Some(owner) = command.refresh_lease_owner() {
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM mcp_oauth_refresh_leases
                 WHERE tenant_id=$1 AND user_id=$2 AND server_id=$3
                   AND credential_generation=$4 AND lease_owner=$5
                   AND dispatched_at IS NOT NULL AND lease_expires_at>$6",
            )
            .bind(credential.principal().tenant_id())
            .bind(credential.principal().user_id())
            .bind(credential.server_id())
            .bind(
                i64::try_from(
                    command
                        .expected_generation()
                        .ok_or_else(RepositoryError::invalid_data)?,
                )
                .map_err(|_| RepositoryError::invalid_data())?,
            )
            .bind(owner)
            .bind(credential.updated_at())
            .fetch_one(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?
                == 1
        } else {
            true
        };
        let generation_ok = refresh_fence_ok
            && match (&existing, command.expected_generation()) {
                (None, None) => credential.generation() == 1,
                (Some(current), Some(expected)) => {
                    current.generation() == expected && credential.generation() == expected + 1
                }
                _ => false,
            };
        if !generation_ok {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        sqlx::query("INSERT INTO mcp_oauth_credentials(tenant_id,user_id,server_id,issuer,client_id,resource,scopes_json,token_type,credential_generation,access_ciphertext,access_token_hash,refresh_ciphertext,refresh_token_hash,access_expires_at,updated_at,revoked_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,NULL) ON CONFLICT(tenant_id,user_id,server_id) DO UPDATE SET issuer=excluded.issuer,client_id=excluded.client_id,resource=excluded.resource,scopes_json=excluded.scopes_json,token_type=excluded.token_type,credential_generation=excluded.credential_generation,access_ciphertext=excluded.access_ciphertext,access_token_hash=excluded.access_token_hash,refresh_ciphertext=excluded.refresh_ciphertext,refresh_token_hash=excluded.refresh_token_hash,access_expires_at=excluded.access_expires_at,updated_at=excluded.updated_at,revoked_at=NULL")
            .bind(credential.principal().tenant_id()).bind(credential.principal().user_id()).bind(credential.server_id())
            .bind(credential.issuer()).bind(credential.client_id()).bind(credential.resource())
            .bind(serde_json::to_value(credential.scopes()).map_err(|_| RepositoryError::invalid_data())?)
            .bind(credential.token_type()).bind(i64::try_from(credential.generation()).map_err(|_| RepositoryError::invalid_data())?)
            .bind(command.access_token().expose_ciphertext()).bind(command.access_token_hash())
            .bind(command.refresh_token().map(McpSecretCiphertext::expose_ciphertext)).bind(command.refresh_token_hash())
            .bind(credential.access_expires_at()).bind(credential.updated_at())
            .execute(&mut *tx).await.map_err(RepositoryError::storage)?;
        insert_postgres_credential_receipt(&mut tx, credential, command.request_id(), &intent)
            .await?;
        if let (Some(owner), Some(generation)) =
            (command.refresh_lease_owner(), command.expected_generation())
        {
            let rows = sqlx::query(
                "DELETE FROM mcp_oauth_refresh_leases
                 WHERE tenant_id=$1 AND user_id=$2 AND server_id=$3
                   AND credential_generation=$4 AND lease_owner=$5
                   AND dispatched_at IS NOT NULL",
            )
            .bind(credential.principal().tenant_id())
            .bind(credential.principal().user_id())
            .bind(credential.server_id())
            .bind(i64::try_from(generation).map_err(|_| RepositoryError::invalid_data())?)
            .bind(owner)
            .execute(&mut *tx)
            .await
            .map_err(RepositoryError::storage)?
            .rows_affected();
            if rows != 1 {
                tx.rollback().await.map_err(RepositoryError::storage)?;
                return Ok(TransitionOutcome::StaleLease);
            }
        }
        let result =
            load_postgres_credential_tx(&mut tx, credential.principal(), credential.server_id())
                .await?
                .ok_or_else(RepositoryError::invalid_data)?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result })
    }

    async fn complete_mcp_oauth_callback(
        &self,
        command: CompleteMcpOAuthCallbackCommand,
    ) -> Result<TransitionOutcome<McpOAuthCallbackCompletion>, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let consume = command.consume();
        let store = command.credential();
        let credential = store.credential();
        let consume_intent = intent_hash(consume)?;
        let credential_intent = intent_hash(store)?;
        let transaction_receipt = sqlx::query_scalar::<_, String>(
            "SELECT intent_hash FROM mcp_oauth_transaction_receipts
             WHERE transaction_id=$1 AND request_id=$2",
        )
        .bind(consume.transaction_id().as_str())
        .bind(consume.request_id())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        let credential_receipt = sqlx::query_scalar::<_, String>(
            "SELECT intent_hash FROM mcp_oauth_credential_receipts
             WHERE tenant_id=$1 AND user_id=$2 AND server_id=$3 AND request_id=$4",
        )
        .bind(credential.principal().tenant_id())
        .bind(credential.principal().user_id())
        .bind(credential.server_id())
        .bind(store.request_id())
        .fetch_optional(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?;
        if transaction_receipt.is_some() || credential_receipt.is_some() {
            let replay = transaction_receipt.as_deref() == Some(consume_intent.as_str())
                && credential_receipt.as_deref() == Some(credential_intent.as_str());
            let transaction =
                load_postgres_transaction_tx(&mut tx, consume.transaction_id()).await?;
            let credential = load_postgres_credential_tx(
                &mut tx,
                credential.principal(),
                credential.server_id(),
            )
            .await?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(if replay {
                TransitionOutcome::ExactReplay {
                    authoritative: McpOAuthCallbackCompletion {
                        transaction: transaction.ok_or_else(RepositoryError::invalid_data)?,
                        credential: credential.ok_or_else(RepositoryError::invalid_data)?,
                    },
                }
            } else {
                TransitionOutcome::StateConflict
            });
        }
        let Some(transaction) =
            load_postgres_transaction_tx(&mut tx, consume.transaction_id()).await?
        else {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        };
        if transaction.state() != McpOAuthTransactionState::Pending
            || transaction.version() != consume.expected_version()
            || transaction.expires_at() <= consume.consumed_at()
            || !constant_time_eq(
                transaction.state_hash().as_bytes(),
                consume.callback_state_hash().as_bytes(),
            )
            || !callback_authority_matches(&transaction, consume, store)
        {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        let existing =
            load_postgres_credential_tx(&mut tx, credential.principal(), credential.server_id())
                .await?;
        let generation_ok = match (&existing, store.expected_generation()) {
            (None, None) => credential.generation() == 1,
            (Some(current), Some(expected)) => {
                current.generation() == expected && credential.generation() == expected + 1
            }
            _ => false,
        };
        if !generation_ok {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        sqlx::query("INSERT INTO mcp_oauth_credentials(tenant_id,user_id,server_id,issuer,client_id,resource,scopes_json,token_type,credential_generation,access_ciphertext,access_token_hash,refresh_ciphertext,refresh_token_hash,access_expires_at,updated_at,revoked_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,NULL) ON CONFLICT(tenant_id,user_id,server_id) DO UPDATE SET issuer=excluded.issuer,client_id=excluded.client_id,resource=excluded.resource,scopes_json=excluded.scopes_json,token_type=excluded.token_type,credential_generation=excluded.credential_generation,access_ciphertext=excluded.access_ciphertext,access_token_hash=excluded.access_token_hash,refresh_ciphertext=excluded.refresh_ciphertext,refresh_token_hash=excluded.refresh_token_hash,access_expires_at=excluded.access_expires_at,updated_at=excluded.updated_at,revoked_at=NULL")
            .bind(credential.principal().tenant_id()).bind(credential.principal().user_id()).bind(credential.server_id())
            .bind(credential.issuer()).bind(credential.client_id()).bind(credential.resource())
            .bind(serde_json::to_value(credential.scopes()).map_err(|_| RepositoryError::invalid_data())?)
            .bind(credential.token_type()).bind(i64::try_from(credential.generation()).map_err(|_| RepositoryError::invalid_data())?)
            .bind(store.access_token().expose_ciphertext()).bind(store.access_token_hash())
            .bind(store.refresh_token().map(McpSecretCiphertext::expose_ciphertext)).bind(store.refresh_token_hash())
            .bind(credential.access_expires_at()).bind(credential.updated_at())
            .execute(&mut *tx).await.map_err(RepositoryError::storage)?;
        insert_postgres_credential_receipt(
            &mut tx,
            credential,
            store.request_id(),
            &credential_intent,
        )
        .await?;
        let transaction_version = transaction.version() + 1;
        let rows = sqlx::query(
            "UPDATE mcp_oauth_transactions
             SET transaction_state='consumed',transaction_version=$1,
                 transaction_ciphertext=$2,transaction_secret_hash=$3,consumed_at=$4
             WHERE transaction_id=$5 AND transaction_version=$6 AND transaction_state='pending'",
        )
        .bind(i64::try_from(transaction_version).map_err(|_| RepositoryError::invalid_data())?)
        .bind(SCRUBBED_CIPHERTEXT)
        .bind(SCRUBBED_SECRET_HASH)
        .bind(consume.consumed_at())
        .bind(consume.transaction_id().as_str())
        .bind(i64::try_from(transaction.version()).map_err(|_| RepositoryError::invalid_data())?)
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        if rows != 1 {
            tx.rollback().await.map_err(RepositoryError::storage)?;
            return Ok(TransitionOutcome::StateConflict);
        }
        sqlx::query("INSERT INTO mcp_oauth_transaction_receipts(transaction_id,request_id,intent_hash,result_version,created_at) VALUES($1,$2,$3,$4,$5)")
            .bind(consume.transaction_id().as_str()).bind(consume.request_id()).bind(consume_intent)
            .bind(i64::try_from(transaction_version).map_err(|_| RepositoryError::invalid_data())?)
            .bind(consume.consumed_at()).execute(&mut *tx).await.map_err(RepositoryError::storage)?;
        let result = McpOAuthCallbackCompletion {
            transaction: load_postgres_transaction_tx(&mut tx, consume.transaction_id())
                .await?
                .ok_or_else(RepositoryError::invalid_data)?,
            credential: load_postgres_credential_tx(
                &mut tx,
                credential.principal(),
                credential.server_id(),
            )
            .await?
            .ok_or_else(RepositoryError::invalid_data)?,
        };
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(TransitionOutcome::Committed { result })
    }

    async fn load_mcp_oauth_credential(
        &self,
        principal: &McpInteractionPrincipal,
        server_id: &str,
    ) -> Result<Option<McpOAuthCredential>, RepositoryError> {
        let row = sqlx::query(AssertSqlSafe(format!("SELECT {CREDENTIAL_COLUMNS} FROM mcp_oauth_credentials WHERE tenant_id=$1 AND user_id=$2 AND server_id=$3")))
            .bind(principal.tenant_id()).bind(principal.user_id()).bind(server_id)
            .fetch_optional(&self.pool).await.map_err(RepositoryError::storage)?;
        row.as_ref().map(parse_postgres_credential).transpose()
    }

    async fn load_mcp_oauth_credential_secret(
        &self,
        principal: &McpInteractionPrincipal,
        server_id: &str,
    ) -> Result<Option<McpOAuthCredentialSecret>, RepositoryError> {
        let row = sqlx::query("SELECT access_ciphertext,access_token_hash,refresh_ciphertext,refresh_token_hash,revoked_at FROM mcp_oauth_credentials WHERE tenant_id=$1 AND user_id=$2 AND server_id=$3")
            .bind(principal.tenant_id()).bind(principal.user_id()).bind(server_id)
            .fetch_optional(&self.pool).await.map_err(RepositoryError::storage)?;
        row.filter(|row| {
            row.try_get::<Option<DateTime<Utc>>, _>("revoked_at")
                .ok()
                .flatten()
                .is_none()
        })
        .map(parse_postgres_credential_secret)
        .transpose()
    }

    async fn claim_mcp_oauth_refresh(
        &self,
        command: ClaimMcpOAuthRefreshCommand,
    ) -> Result<bool, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let current =
            load_postgres_credential_tx(&mut tx, command.principal(), command.server_id()).await?;
        if !current.is_some_and(|credential| {
            credential.revoked_at().is_none()
                && credential.generation() == command.expected_generation()
        }) {
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(false);
        }
        let stale_dispatched = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM mcp_oauth_refresh_leases
             WHERE tenant_id=$1 AND user_id=$2 AND server_id=$3
               AND credential_generation=$4 AND dispatched_at IS NOT NULL
               AND lease_expires_at<=$5",
        )
        .bind(command.principal().tenant_id())
        .bind(command.principal().user_id())
        .bind(command.server_id())
        .bind(
            i64::try_from(command.expected_generation())
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .bind(command.now())
        .fetch_one(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
            == 1;
        if stale_dispatched {
            quarantine_postgres_refresh_tx(
                &mut tx,
                command.principal(),
                command.server_id(),
                command.expected_generation(),
                None,
                command.now(),
            )
            .await?;
            tx.commit().await.map_err(RepositoryError::storage)?;
            return Ok(false);
        }
        let rows = sqlx::query(
            "INSERT INTO mcp_oauth_refresh_leases(
               tenant_id,user_id,server_id,credential_generation,lease_owner,lease_expires_at,updated_at,dispatched_at
             ) VALUES($1,$2,$3,$4,$5,$6,$7,NULL)
             ON CONFLICT(tenant_id,user_id,server_id) DO UPDATE SET
               credential_generation=excluded.credential_generation,
               lease_owner=excluded.lease_owner,
               lease_expires_at=excluded.lease_expires_at,
               updated_at=excluded.updated_at,
               dispatched_at=NULL
             WHERE (mcp_oauth_refresh_leases.lease_expires_at<=excluded.updated_at
                    AND mcp_oauth_refresh_leases.dispatched_at IS NULL)
                OR (mcp_oauth_refresh_leases.lease_owner=excluded.lease_owner
                    AND mcp_oauth_refresh_leases.credential_generation=excluded.credential_generation
                    AND mcp_oauth_refresh_leases.dispatched_at IS NULL)",
        )
        .bind(command.principal().tenant_id())
        .bind(command.principal().user_id())
        .bind(command.server_id())
        .bind(
            i64::try_from(command.expected_generation())
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .bind(command.owner())
        .bind(command.lease_expires_at())
        .bind(command.now())
        .execute(&mut *tx)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(rows == 1)
    }

    async fn mark_mcp_oauth_refresh_dispatched(
        &self,
        principal: &McpInteractionPrincipal,
        server_id: &str,
        generation: u64,
        owner: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        let rows = sqlx::query(
            "UPDATE mcp_oauth_refresh_leases
             SET dispatched_at=$1
             WHERE tenant_id=$2 AND user_id=$3 AND server_id=$4
               AND credential_generation=$5 AND lease_owner=$6
               AND dispatched_at IS NULL AND lease_expires_at>$1",
        )
        .bind(now)
        .bind(principal.tenant_id())
        .bind(principal.user_id())
        .bind(server_id)
        .bind(i64::try_from(generation).map_err(|_| RepositoryError::invalid_data())?)
        .bind(owner)
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        Ok(rows == 1)
    }

    async fn quarantine_mcp_oauth_refresh(
        &self,
        principal: &McpInteractionPrincipal,
        server_id: &str,
        generation: u64,
        owner: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, RepositoryError> {
        let mut tx = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let quarantined = quarantine_postgres_refresh_tx(
            &mut tx,
            principal,
            server_id,
            generation,
            Some(owner),
            now,
        )
        .await?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        Ok(quarantined)
    }

    async fn release_mcp_oauth_refresh(
        &self,
        principal: &McpInteractionPrincipal,
        server_id: &str,
        generation: u64,
        owner: &str,
    ) -> Result<bool, RepositoryError> {
        let rows = sqlx::query(
            "DELETE FROM mcp_oauth_refresh_leases
             WHERE tenant_id=$1 AND user_id=$2 AND server_id=$3
               AND credential_generation=$4 AND lease_owner=$5
               AND dispatched_at IS NULL",
        )
        .bind(principal.tenant_id())
        .bind(principal.user_id())
        .bind(server_id)
        .bind(i64::try_from(generation).map_err(|_| RepositoryError::invalid_data())?)
        .bind(owner)
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        Ok(rows == 1)
    }

    async fn expire_mcp_oauth_transactions(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, RepositoryError> {
        if limit == 0 || limit > MAX_EXPIRY_BATCH {
            return Err(RepositoryError::invalid_data());
        }
        let rows = sqlx::query(
            "WITH candidates AS (
               SELECT transaction_id
               FROM mcp_oauth_transactions
               WHERE transaction_state='pending' AND expires_at<=$1
               ORDER BY expires_at,transaction_id
               FOR UPDATE SKIP LOCKED
               LIMIT $2
             )
             UPDATE mcp_oauth_transactions AS transactions
             SET transaction_state='expired',
                 transaction_version=transactions.transaction_version+1,
                 transaction_ciphertext=$3,
                 transaction_secret_hash=$4,
                 consumed_at=$1
             FROM candidates
             WHERE transactions.transaction_id=candidates.transaction_id
               AND transactions.transaction_state='pending'",
        )
        .bind(now)
        .bind(i64::from(limit))
        .bind(SCRUBBED_CIPHERTEXT)
        .bind(SCRUBBED_SECRET_HASH)
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?
        .rows_affected();
        Ok(rows)
    }

    async fn delete_mcp_oauth_credential(
        &self,
        principal: &McpInteractionPrincipal,
        server_id: &str,
        request_id: &str,
        now: DateTime<Utc>,
    ) -> Result<TransitionOutcome<McpOAuthCredential>, RepositoryError> {
        delete_postgres_credential(self, principal, server_id, request_id, now).await
    }
}

async fn load_postgres_transaction_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: &McpOAuthTransactionId,
) -> Result<Option<McpOAuthTransaction>, RepositoryError> {
    let row = sqlx::query(AssertSqlSafe(format!("SELECT {TRANSACTION_COLUMNS} FROM mcp_oauth_transactions WHERE transaction_id=$1 FOR UPDATE")))
        .bind(id.as_str()).fetch_optional(&mut **tx).await.map_err(RepositoryError::storage)?;
    row.as_ref().map(parse_postgres_transaction).transpose()
}

async fn load_postgres_credential_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    principal: &McpInteractionPrincipal,
    server_id: &str,
) -> Result<Option<McpOAuthCredential>, RepositoryError> {
    let row = sqlx::query(AssertSqlSafe(format!("SELECT {CREDENTIAL_COLUMNS} FROM mcp_oauth_credentials WHERE tenant_id=$1 AND user_id=$2 AND server_id=$3 FOR UPDATE")))
        .bind(principal.tenant_id()).bind(principal.user_id()).bind(server_id)
        .fetch_optional(&mut **tx).await.map_err(RepositoryError::storage)?;
    row.as_ref().map(parse_postgres_credential).transpose()
}

async fn quarantine_postgres_refresh_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    principal: &McpInteractionPrincipal,
    server_id: &str,
    generation: u64,
    owner: Option<&str>,
    now: DateTime<Utc>,
) -> Result<bool, RepositoryError> {
    let generation_i64 = i64::try_from(generation).map_err(|_| RepositoryError::invalid_data())?;
    let lease_exists = if let Some(owner) = owner {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM mcp_oauth_refresh_leases
             WHERE tenant_id=$1 AND user_id=$2 AND server_id=$3
               AND credential_generation=$4 AND lease_owner=$5
               AND dispatched_at IS NOT NULL",
        )
        .bind(principal.tenant_id())
        .bind(principal.user_id())
        .bind(server_id)
        .bind(generation_i64)
        .bind(owner)
        .fetch_one(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?
            == 1
    } else {
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM mcp_oauth_refresh_leases
             WHERE tenant_id=$1 AND user_id=$2 AND server_id=$3
               AND credential_generation=$4 AND dispatched_at IS NOT NULL",
        )
        .bind(principal.tenant_id())
        .bind(principal.user_id())
        .bind(server_id)
        .bind(generation_i64)
        .fetch_one(&mut **tx)
        .await
        .map_err(RepositoryError::storage)?
            == 1
    };
    let current = load_postgres_credential_tx(tx, principal, server_id).await?;
    if !lease_exists
        || !current.is_some_and(|credential| {
            credential.generation() == generation && credential.revoked_at().is_none()
        })
    {
        return Ok(false);
    }
    let next_generation = generation
        .checked_add(1)
        .ok_or_else(RepositoryError::invalid_data)?;
    let rows = sqlx::query(
        "UPDATE mcp_oauth_credentials
         SET credential_generation=$1,access_ciphertext=$2,access_token_hash=$3,
             refresh_ciphertext=NULL,refresh_token_hash=NULL,
             access_expires_at=NULL,updated_at=$4,revoked_at=$4
         WHERE tenant_id=$5 AND user_id=$6 AND server_id=$7
           AND credential_generation=$8 AND revoked_at IS NULL",
    )
    .bind(i64::try_from(next_generation).map_err(|_| RepositoryError::invalid_data())?)
    .bind(SCRUBBED_CIPHERTEXT)
    .bind(SCRUBBED_SECRET_HASH)
    .bind(now)
    .bind(principal.tenant_id())
    .bind(principal.user_id())
    .bind(server_id)
    .bind(generation_i64)
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?
    .rows_affected();
    if rows != 1 {
        return Ok(false);
    }
    sqlx::query(
        "DELETE FROM mcp_oauth_refresh_leases
         WHERE tenant_id=$1 AND user_id=$2 AND server_id=$3
           AND credential_generation=$4",
    )
    .bind(principal.tenant_id())
    .bind(principal.user_id())
    .bind(server_id)
    .bind(generation_i64)
    .execute(&mut **tx)
    .await
    .map_err(RepositoryError::storage)?;
    Ok(true)
}

async fn insert_postgres_credential_receipt(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    credential: &McpOAuthCredential,
    request_id: &str,
    intent: &str,
) -> Result<(), RepositoryError> {
    sqlx::query("INSERT INTO mcp_oauth_credential_receipts(tenant_id,user_id,server_id,request_id,intent_hash,result_generation,created_at) VALUES($1,$2,$3,$4,$5,$6,$7)")
        .bind(credential.principal().tenant_id()).bind(credential.principal().user_id())
        .bind(credential.server_id()).bind(request_id).bind(intent)
        .bind(i64::try_from(credential.generation()).map_err(|_| RepositoryError::invalid_data())?)
        .bind(credential.updated_at()).execute(&mut **tx).await.map_err(RepositoryError::storage)?;
    Ok(())
}

async fn delete_postgres_credential(
    repository: &PostgresDurableRepository,
    principal: &McpInteractionPrincipal,
    server_id: &str,
    request_id: &str,
    now: DateTime<Utc>,
) -> Result<TransitionOutcome<McpOAuthCredential>, RepositoryError> {
    if request_id.is_empty() || request_id.len() > 256 {
        return Err(RepositoryError::invalid_data());
    }
    let mut tx = repository
        .pool
        .begin()
        .await
        .map_err(RepositoryError::storage)?;
    let intent = intent_hash(&(principal, server_id, "delete"))?;
    if let Some(row) = sqlx::query("SELECT intent_hash FROM mcp_oauth_credential_receipts WHERE tenant_id=$1 AND user_id=$2 AND server_id=$3 AND request_id=$4")
        .bind(principal.tenant_id()).bind(principal.user_id()).bind(server_id).bind(request_id)
        .fetch_optional(&mut *tx).await.map_err(RepositoryError::storage)?
    {
        let same = row.try_get::<String, _>("intent_hash").map_err(|_| RepositoryError::invalid_data())? == intent;
        let current = load_postgres_credential_tx(&mut tx, principal, server_id).await?;
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(if same { TransitionOutcome::ExactReplay { authoritative: current.ok_or_else(RepositoryError::invalid_data)? } } else { TransitionOutcome::StateConflict });
    }
    let Some(current) = load_postgres_credential_tx(&mut tx, principal, server_id).await? else {
        tx.commit().await.map_err(RepositoryError::storage)?;
        return Ok(TransitionOutcome::StateConflict);
    };
    let generation = current.generation() + 1;
    sqlx::query(
        "DELETE FROM mcp_oauth_refresh_leases
         WHERE tenant_id=$1 AND user_id=$2 AND server_id=$3",
    )
    .bind(principal.tenant_id())
    .bind(principal.user_id())
    .bind(server_id)
    .execute(&mut *tx)
    .await
    .map_err(RepositoryError::storage)?;
    sqlx::query("UPDATE mcp_oauth_credentials SET credential_generation=$1,access_ciphertext='enc:v1:deleted',access_token_hash=$2,refresh_ciphertext=NULL,refresh_token_hash=NULL,updated_at=$3,revoked_at=$3 WHERE tenant_id=$4 AND user_id=$5 AND server_id=$6")
        .bind(i64::try_from(generation).map_err(|_| RepositoryError::invalid_data())?).bind("0".repeat(64)).bind(now)
        .bind(principal.tenant_id()).bind(principal.user_id()).bind(server_id)
        .execute(&mut *tx).await.map_err(RepositoryError::storage)?;
    let result = load_postgres_credential_tx(&mut tx, principal, server_id)
        .await?
        .ok_or_else(RepositoryError::invalid_data)?;
    insert_postgres_credential_receipt(&mut tx, &result, request_id, &intent).await?;
    tx.commit().await.map_err(RepositoryError::storage)?;
    Ok(TransitionOutcome::Committed { result })
}

fn parse_postgres_credential_secret(
    row: PgRow,
) -> Result<McpOAuthCredentialSecret, RepositoryError> {
    Ok(McpOAuthCredentialSecret {
        access_token: McpSecretCiphertext::new(
            row.try_get::<String, _>("access_ciphertext")
                .map_err(|_| RepositoryError::invalid_data())?,
        )?,
        access_token_hash: row
            .try_get("access_token_hash")
            .map_err(|_| RepositoryError::invalid_data())?,
        refresh_token: row
            .try_get::<Option<String>, _>("refresh_ciphertext")
            .map_err(|_| RepositoryError::invalid_data())?
            .map(McpSecretCiphertext::new)
            .transpose()?,
        refresh_token_hash: row
            .try_get("refresh_token_hash")
            .map_err(|_| RepositoryError::invalid_data())?,
    })
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let length = left.len().max(right.len());
    for index in 0..length {
        difference |= usize::from(
            left.get(index).copied().unwrap_or_default()
                ^ right.get(index).copied().unwrap_or_default(),
        );
    }
    difference == 0
}
