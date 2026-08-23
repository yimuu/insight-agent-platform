use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{AssertSqlSafe, Row};
use uuid::Uuid;

use insight_durable::{
    AcknowledgeFileDeletionCommand, BoundConversationFilesQuery, BoundRunFilesQuery,
    ClaimFileDeletionsCommand, CompleteFileCommand, CreateFileCommand, CreateFileOutcome,
    FileDeletionClaim, FileDurableRepository, FileQuery, FileStatus, RepositoryError, StoredFile,
};

use super::{
    database_time, PostgresDurableRepository, RepositoryErrorExt as _, SqliteDurableRepository,
};

const FILE_COLUMNS: &str = "file_id,tenant_id,user_id,filename,media_type,expected_size_bytes,actual_size_bytes,checksum_sha256,object_key,object_etag,object_version_id,status,created_at,upload_expires_at,ready_at,deleted_at";
const BOUND_FILE_COLUMNS: &str = "f.file_id AS file_id,f.tenant_id AS tenant_id,f.user_id AS user_id,f.filename AS filename,f.media_type AS media_type,f.expected_size_bytes AS expected_size_bytes,f.actual_size_bytes AS actual_size_bytes,f.checksum_sha256 AS checksum_sha256,f.object_key AS object_key,f.object_etag AS object_etag,f.object_version_id AS object_version_id,f.status AS status,f.created_at AS created_at,f.upload_expires_at AS upload_expires_at,f.ready_at AS ready_at,f.deleted_at AS deleted_at,b.filename AS bound_filename,b.media_type AS bound_media_type,b.size_bytes AS bound_size_bytes,b.object_etag AS bound_object_etag,b.object_version_id AS bound_object_version_id";

fn validate_bound_file(
    file: StoredFile,
    bound_filename: String,
    bound_media_type: String,
    bound_size_bytes: i64,
    bound_object_etag: String,
    bound_object_version_id: Option<String>,
) -> Result<StoredFile, RepositoryError> {
    if file.filename != bound_filename
        || file.media_type != bound_media_type
        || file.actual_size_bytes != u64::try_from(bound_size_bytes).ok()
        || file.object_etag.as_deref() != Some(bound_object_etag.as_str())
        || file.object_version_id != bound_object_version_id
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(file)
}

fn validate_create(command: &CreateFileCommand) -> Result<(), RepositoryError> {
    if command.file_id.is_empty()
        || command.file_id.len() > 256
        || command.tenant_id.is_empty()
        || command.tenant_id.len() > 256
        || command.user_id.is_empty()
        || command.user_id.len() > 256
        || command.filename.is_empty()
        || command.filename.len() > 1024
        || command.media_type.is_empty()
        || command.media_type.len() > 255
        || command.object_key.is_empty()
        || command.object_key.len() > 1024
        || command.request_hash.len() != 71
        || !command.request_hash.starts_with("sha256:")
        || command.upload_expires_at <= command.created_at
        || command.idempotency_key.as_ref().is_some_and(|key| {
            key.is_empty() || key.len() > 256 || key.chars().any(|character| character.is_control())
        })
        || command.checksum_sha256.as_ref().is_some_and(|checksum| {
            checksum.len() != 64
                || !checksum
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

fn same_create(file: &StoredFile, command: &CreateFileCommand) -> bool {
    file.file_id == command.file_id
        && file.tenant_id == command.tenant_id
        && file.user_id == command.user_id
        && file.filename == command.filename
        && file.media_type == command.media_type
        && file.expected_size_bytes == command.expected_size_bytes
        && file.checksum_sha256 == command.checksum_sha256
        && file.object_key == command.object_key
        && file.status == FileStatus::PendingUpload
}

fn conflict() -> RepositoryError {
    insight_engine::repository::adapter::repository_error(
        insight_engine::repository::REPOSITORY_INTENT_CONFLICT,
        "file idempotency key is bound to a different request",
    )
}

fn postgres_file_idempotency_lock_key(tenant_id: &str, user_id: &str, key: &str) -> String {
    format!(
        "file-create.v1:{}:{}:{}:{}:{}:{}",
        tenant_id.len(),
        tenant_id,
        user_id.len(),
        user_id,
        key.len(),
        key
    )
}

fn sqlite_file(row: &sqlx::sqlite::SqliteRow) -> Result<StoredFile, RepositoryError> {
    stored_file(
        row.try_get("file_id"),
        row.try_get("tenant_id"),
        row.try_get("user_id"),
        row.try_get("filename"),
        row.try_get("media_type"),
        row.try_get::<i64, _>("expected_size_bytes"),
        row.try_get::<Option<i64>, _>("actual_size_bytes"),
        row.try_get("checksum_sha256"),
        row.try_get("object_key"),
        row.try_get("object_etag"),
        row.try_get("object_version_id"),
        row.try_get("status"),
        row.try_get("created_at"),
        row.try_get("upload_expires_at"),
        row.try_get("ready_at"),
        row.try_get("deleted_at"),
    )
}

fn postgres_file(row: &sqlx::postgres::PgRow) -> Result<StoredFile, RepositoryError> {
    stored_file(
        row.try_get("file_id"),
        row.try_get("tenant_id"),
        row.try_get("user_id"),
        row.try_get("filename"),
        row.try_get("media_type"),
        row.try_get::<i64, _>("expected_size_bytes"),
        row.try_get::<Option<i64>, _>("actual_size_bytes"),
        row.try_get("checksum_sha256"),
        row.try_get("object_key"),
        row.try_get("object_etag"),
        row.try_get("object_version_id"),
        row.try_get("status"),
        row.try_get("created_at"),
        row.try_get("upload_expires_at"),
        row.try_get("ready_at"),
        row.try_get("deleted_at"),
    )
}

#[allow(clippy::too_many_arguments)]
fn stored_file(
    file_id: Result<String, sqlx::Error>,
    tenant_id: Result<String, sqlx::Error>,
    user_id: Result<String, sqlx::Error>,
    filename: Result<String, sqlx::Error>,
    media_type: Result<String, sqlx::Error>,
    expected_size_bytes: Result<i64, sqlx::Error>,
    actual_size_bytes: Result<Option<i64>, sqlx::Error>,
    checksum_sha256: Result<Option<String>, sqlx::Error>,
    object_key: Result<String, sqlx::Error>,
    object_etag: Result<Option<String>, sqlx::Error>,
    object_version_id: Result<Option<String>, sqlx::Error>,
    status: Result<String, sqlx::Error>,
    created_at: Result<DateTime<Utc>, sqlx::Error>,
    upload_expires_at: Result<DateTime<Utc>, sqlx::Error>,
    ready_at: Result<Option<DateTime<Utc>>, sqlx::Error>,
    deleted_at: Result<Option<DateTime<Utc>>, sqlx::Error>,
) -> Result<StoredFile, RepositoryError> {
    let expected_size_bytes = u64::try_from(expected_size_bytes.map_err(RepositoryError::storage)?)
        .map_err(|_| RepositoryError::invalid_data())?;
    let actual_size_bytes = actual_size_bytes
        .map_err(RepositoryError::storage)?
        .map(u64::try_from)
        .transpose()
        .map_err(|_| RepositoryError::invalid_data())?;
    Ok(StoredFile {
        file_id: file_id.map_err(RepositoryError::storage)?,
        tenant_id: tenant_id.map_err(RepositoryError::storage)?,
        user_id: user_id.map_err(RepositoryError::storage)?,
        filename: filename.map_err(RepositoryError::storage)?,
        media_type: media_type.map_err(RepositoryError::storage)?,
        expected_size_bytes,
        actual_size_bytes,
        checksum_sha256: checksum_sha256.map_err(RepositoryError::storage)?,
        object_key: object_key.map_err(RepositoryError::storage)?,
        object_etag: object_etag.map_err(RepositoryError::storage)?,
        object_version_id: object_version_id.map_err(RepositoryError::storage)?,
        status: FileStatus::parse(&status.map_err(RepositoryError::storage)?)?,
        created_at: created_at.map_err(RepositoryError::storage)?,
        upload_expires_at: upload_expires_at.map_err(RepositoryError::storage)?,
        ready_at: ready_at.map_err(RepositoryError::storage)?,
        deleted_at: deleted_at.map_err(RepositoryError::storage)?,
    })
}

#[async_trait]
impl FileDurableRepository for SqliteDurableRepository {
    async fn create_file(
        &self,
        command: CreateFileCommand,
    ) -> Result<CreateFileOutcome, RepositoryError> {
        validate_create(&command)?;
        let _guard = self.writer.lock().await;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        if let Some(key) = &command.idempotency_key {
            let query = format!(
                "SELECT {FILE_COLUMNS} FROM files WHERE tenant_id=? AND user_id=? AND idempotency_key=?"
            );
            if let Some(row) = sqlx::query(AssertSqlSafe(query))
                .bind(&command.tenant_id)
                .bind(&command.user_id)
                .bind(key)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?
            {
                let file = sqlite_file(&row)?;
                let request_hash = sqlx::query_scalar::<_, String>(
                    "SELECT request_hash FROM files WHERE file_id=?",
                )
                .bind(&file.file_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?;
                if request_hash != command.request_hash {
                    return Err(conflict());
                }
                transaction
                    .commit()
                    .await
                    .map_err(RepositoryError::storage)?;
                return Ok(CreateFileOutcome {
                    file,
                    replayed: true,
                });
            }
        }
        let inserted = sqlx::query(
            "INSERT OR IGNORE INTO files(
               file_id,tenant_id,user_id,filename,media_type,expected_size_bytes,
               checksum_sha256,object_key,status,idempotency_key,request_hash,
               created_at,upload_expires_at
             ) VALUES(?,?,?,?,?,?,?,?, 'pending_upload',?,?,?,?)",
        )
        .bind(&command.file_id)
        .bind(&command.tenant_id)
        .bind(&command.user_id)
        .bind(&command.filename)
        .bind(&command.media_type)
        .bind(
            i64::try_from(command.expected_size_bytes)
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .bind(&command.checksum_sha256)
        .bind(&command.object_key)
        .bind(&command.idempotency_key)
        .bind(&command.request_hash)
        .bind(database_time(command.created_at))
        .bind(database_time(command.upload_expires_at))
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let query = format!("SELECT {FILE_COLUMNS} FROM files WHERE file_id=?");
        let row = sqlx::query(AssertSqlSafe(query))
            .bind(&command.file_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?
            .ok_or_else(conflict)?;
        let file = sqlite_file(&row)?;
        if !same_create(&file, &command) {
            return Err(conflict());
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(CreateFileOutcome {
            file,
            replayed: inserted.rows_affected() == 0,
        })
    }

    async fn get_file(&self, query: FileQuery) -> Result<Option<StoredFile>, RepositoryError> {
        let sql = format!(
            "SELECT {FILE_COLUMNS} FROM files WHERE file_id=? AND tenant_id=? AND user_id=?"
        );
        sqlx::query(AssertSqlSafe(sql))
            .bind(query.file_id)
            .bind(query.tenant_id)
            .bind(query.user_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(RepositoryError::storage)?
            .map(|row| sqlite_file(&row))
            .transpose()
    }

    async fn complete_file(
        &self,
        command: CompleteFileCommand,
    ) -> Result<Option<StoredFile>, RepositoryError> {
        let _guard = self.writer.lock().await;
        sqlx::query(
            "UPDATE files SET status='ready',actual_size_bytes=?,object_etag=?,object_version_id=?,ready_at=?
             WHERE file_id=? AND tenant_id=? AND user_id=? AND status='pending_upload'
               AND expected_size_bytes=? AND upload_expires_at>=?",
        )
        .bind(i64::try_from(command.actual_size_bytes).map_err(|_| RepositoryError::invalid_data())?)
        .bind(&command.object_etag)
        .bind(&command.object_version_id)
        .bind(database_time(command.ready_at))
        .bind(&command.query.file_id)
        .bind(&command.query.tenant_id)
        .bind(&command.query.user_id)
        .bind(i64::try_from(command.actual_size_bytes).map_err(|_| RepositoryError::invalid_data())?)
        .bind(database_time(command.ready_at))
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        self.get_file(command.query).await
    }

    async fn begin_file_delete(
        &self,
        query: FileQuery,
        deleted_at: DateTime<Utc>,
    ) -> Result<Option<StoredFile>, RepositoryError> {
        let _guard = self.writer.lock().await;
        sqlx::query(
            "UPDATE files SET status='deleting',deleted_at=?
             WHERE file_id=? AND tenant_id=? AND user_id=? AND status NOT IN('deleting','deleted')",
        )
        .bind(database_time(deleted_at))
        .bind(&query.file_id)
        .bind(&query.tenant_id)
        .bind(&query.user_id)
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        self.get_file(query).await
    }

    async fn finish_file_delete(
        &self,
        query: FileQuery,
        deleted_at: DateTime<Utc>,
    ) -> Result<Option<StoredFile>, RepositoryError> {
        let _guard = self.writer.lock().await;
        sqlx::query(
            "UPDATE files SET status='deleted',deleted_at=?
             WHERE file_id=? AND tenant_id=? AND user_id=? AND status IN('deleting','deleted')",
        )
        .bind(database_time(deleted_at))
        .bind(&query.file_id)
        .bind(&query.tenant_id)
        .bind(&query.user_id)
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        self.get_file(query).await
    }

    async fn fail_file(
        &self,
        query: FileQuery,
        failed_at: DateTime<Utc>,
    ) -> Result<Option<StoredFile>, RepositoryError> {
        let _guard = self.writer.lock().await;
        sqlx::query(
            "UPDATE files SET status='failed',deleted_at=?
             WHERE file_id=? AND tenant_id=? AND user_id=? AND status='pending_upload'",
        )
        .bind(database_time(failed_at))
        .bind(&query.file_id)
        .bind(&query.tenant_id)
        .bind(&query.user_id)
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        self.get_file(query).await
    }

    async fn expire_pending_files(
        &self,
        observed_at: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<StoredFile>, RepositoryError> {
        if limit == 0 || limit > 10_000 {
            return Err(RepositoryError::invalid_data());
        }
        let _guard = self.writer.lock().await;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let ids = sqlx::query_scalar::<_, String>(
            "SELECT file_id FROM files
             WHERE status='pending_upload' AND upload_expires_at<?
             ORDER BY upload_expires_at,file_id LIMIT ?",
        )
        .bind(database_time(observed_at))
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        for file_id in &ids {
            sqlx::query(
                "UPDATE files SET status='expired',deleted_at=?
                 WHERE file_id=? AND status='pending_upload' AND upload_expires_at<?",
            )
            .bind(database_time(observed_at))
            .bind(file_id)
            .bind(database_time(observed_at))
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
        }
        let mut files = Vec::new();
        for file_id in ids {
            let sql = format!("SELECT {FILE_COLUMNS} FROM files WHERE file_id=?");
            if let Some(row) = sqlx::query(AssertSqlSafe(sql))
                .bind(file_id)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?
            {
                files.push(sqlite_file(&row)?);
            }
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(files)
    }

    async fn list_deletable_files(
        &self,
        observed_at: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<StoredFile>, RepositoryError> {
        if limit == 0 || limit > 10_000 {
            return Err(RepositoryError::invalid_data());
        }
        let sql = format!(
            "SELECT {FILE_COLUMNS} FROM files f
             WHERE f.status IN('deleting','expired','failed')
               AND NOT EXISTS(
                 SELECT 1 FROM file_bindings b WHERE b.file_id=f.file_id
                   AND b.released_at IS NULL
                   AND (b.retain_until IS NULL OR b.retain_until>?))
             ORDER BY COALESCE(f.deleted_at,f.upload_expires_at),f.file_id LIMIT ?"
        );
        let rows = sqlx::query(AssertSqlSafe(sql))
            .bind(database_time(observed_at))
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await
            .map_err(RepositoryError::storage)?;
        rows.iter().map(sqlite_file).collect()
    }

    async fn resolve_bound_run_files(
        &self,
        query: BoundRunFilesQuery,
    ) -> Result<Vec<StoredFile>, RepositoryError> {
        if query.run_id.is_empty() || query.file_ids.len() > 100 {
            return Err(RepositoryError::invalid_data());
        }
        let mut seen = std::collections::HashSet::new();
        let mut files = Vec::with_capacity(query.file_ids.len());
        for file_id in query.file_ids {
            if !seen.insert(file_id.clone()) {
                return Err(RepositoryError::invalid_data());
            }
            let sql = format!(
                "SELECT {BOUND_FILE_COLUMNS} FROM files f JOIN file_bindings b ON b.file_id=f.file_id
                 WHERE b.target_kind='run' AND b.target_id=? AND b.file_id=?
                   AND b.released_at IS NULL AND (b.retain_until IS NULL OR b.retain_until>?)"
            );
            let row = sqlx::query(AssertSqlSafe(sql))
                .bind(&query.run_id)
                .bind(&file_id)
                .bind(database_time(query.observed_at))
                .fetch_optional(&self.pool)
                .await
                .map_err(RepositoryError::storage)?
                .ok_or_else(RepositoryError::invalid_data)?;
            files.push(validate_bound_file(
                sqlite_file(&row)?,
                row.try_get("bound_filename")
                    .map_err(RepositoryError::storage)?,
                row.try_get("bound_media_type")
                    .map_err(RepositoryError::storage)?,
                row.try_get("bound_size_bytes")
                    .map_err(RepositoryError::storage)?,
                row.try_get("bound_object_etag")
                    .map_err(RepositoryError::storage)?,
                row.try_get("bound_object_version_id")
                    .map_err(RepositoryError::storage)?,
            )?);
        }
        Ok(files)
    }

    async fn resolve_bound_conversation_files(
        &self,
        query: BoundConversationFilesQuery,
    ) -> Result<Vec<StoredFile>, RepositoryError> {
        if query.conversation_id.is_empty()
            || query.tenant_id.is_empty()
            || query.user_id.is_empty()
            || query.file_ids.len() > 100
        {
            return Err(RepositoryError::invalid_data());
        }
        let mut seen = std::collections::HashSet::new();
        let mut files = Vec::with_capacity(query.file_ids.len());
        for file_id in query.file_ids {
            if !seen.insert(file_id.clone()) {
                return Err(RepositoryError::invalid_data());
            }
            let sql = format!(
                "SELECT {BOUND_FILE_COLUMNS} FROM files f JOIN file_bindings b ON b.file_id=f.file_id
                 WHERE b.target_kind='conversation' AND b.target_id=? AND b.tenant_id=?
                   AND b.user_id=? AND b.file_id=? AND b.released_at IS NULL
                   AND (b.retain_until IS NULL OR b.retain_until>?)"
            );
            let row = sqlx::query(AssertSqlSafe(sql))
                .bind(&query.conversation_id)
                .bind(&query.tenant_id)
                .bind(&query.user_id)
                .bind(&file_id)
                .bind(database_time(query.observed_at))
                .fetch_optional(&self.pool)
                .await
                .map_err(RepositoryError::storage)?
                .ok_or_else(RepositoryError::invalid_data)?;
            files.push(validate_bound_file(
                sqlite_file(&row)?,
                row.try_get("bound_filename")
                    .map_err(RepositoryError::storage)?,
                row.try_get("bound_media_type")
                    .map_err(RepositoryError::storage)?,
                row.try_get("bound_size_bytes")
                    .map_err(RepositoryError::storage)?,
                row.try_get("bound_object_etag")
                    .map_err(RepositoryError::storage)?,
                row.try_get("bound_object_version_id")
                    .map_err(RepositoryError::storage)?,
            )?);
        }
        Ok(files)
    }

    async fn claim_file_deletions(
        &self,
        command: ClaimFileDeletionsCommand,
    ) -> Result<Vec<FileDeletionClaim>, RepositoryError> {
        if command.limit == 0
            || command.limit > 10_000
            || command.claim_expires_at <= command.observed_at
        {
            return Err(RepositoryError::invalid_data());
        }
        let _guard = self.writer.lock().await;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let ids = sqlx::query_scalar::<_, String>(
            "SELECT f.file_id FROM files f
             WHERE f.status IN('deleting','expired','failed')
               AND (f.deletion_claim_token IS NULL OR f.deletion_claim_expires_at<=?)
               AND NOT EXISTS(
                 SELECT 1 FROM file_bindings b WHERE b.file_id=f.file_id
                   AND b.released_at IS NULL
                   AND (b.retain_until IS NULL OR b.retain_until>?))
             ORDER BY COALESCE(f.deleted_at,f.upload_expires_at),f.file_id LIMIT ?",
        )
        .bind(database_time(command.observed_at))
        .bind(database_time(command.observed_at))
        .bind(i64::from(command.limit))
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let mut claims = Vec::with_capacity(ids.len());
        for file_id in ids {
            let claim_token = format!("claim_{}", Uuid::new_v4().simple());
            let updated = sqlx::query(
                "UPDATE files SET deletion_claim_token=?,deletion_claim_expires_at=?,
                    deletion_fence=deletion_fence+1
                 WHERE file_id=? AND status IN('deleting','expired','failed')
                   AND (deletion_claim_token IS NULL OR deletion_claim_expires_at<=?)",
            )
            .bind(&claim_token)
            .bind(database_time(command.claim_expires_at))
            .bind(&file_id)
            .bind(database_time(command.observed_at))
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            if updated.rows_affected() != 1 {
                continue;
            }
            let sql = format!("SELECT {FILE_COLUMNS},deletion_fence FROM files WHERE file_id=?");
            let row = sqlx::query(AssertSqlSafe(sql))
                .bind(&file_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?;
            claims.push(FileDeletionClaim {
                file: sqlite_file(&row)?,
                claim_token,
                deletion_fence: u64::try_from(
                    row.try_get::<i64, _>("deletion_fence")
                        .map_err(RepositoryError::storage)?,
                )
                .map_err(|_| RepositoryError::invalid_data())?,
            });
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(claims)
    }

    async fn acknowledge_file_deletion(
        &self,
        command: AcknowledgeFileDeletionCommand,
    ) -> Result<bool, RepositoryError> {
        let fence =
            i64::try_from(command.deletion_fence).map_err(|_| RepositoryError::invalid_data())?;
        let _guard = self.writer.lock().await;
        let updated = sqlx::query(
            "UPDATE files SET status='deleted',deleted_at=?,deletion_claim_token=NULL,
                deletion_claim_expires_at=NULL
             WHERE file_id=? AND deletion_claim_token=? AND deletion_fence=?
               AND status IN('deleting','expired','failed')",
        )
        .bind(database_time(command.deleted_at))
        .bind(command.file_id)
        .bind(command.claim_token)
        .bind(fence)
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        Ok(updated.rows_affected() == 1)
    }
}

#[async_trait]
impl FileDurableRepository for PostgresDurableRepository {
    async fn create_file(
        &self,
        command: CreateFileCommand,
    ) -> Result<CreateFileOutcome, RepositoryError> {
        validate_create(&command)?;
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        if let Some(key) = &command.idempotency_key {
            sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 901176211919::bigint))")
                .bind(postgres_file_idempotency_lock_key(
                    &command.tenant_id,
                    &command.user_id,
                    key,
                ))
                .execute(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?;
            let query = format!(
                "SELECT {FILE_COLUMNS},request_hash FROM files WHERE tenant_id=$1 AND user_id=$2 AND idempotency_key=$3 FOR SHARE"
            );
            if let Some(row) = sqlx::query(AssertSqlSafe(query))
                .bind(&command.tenant_id)
                .bind(&command.user_id)
                .bind(key)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?
            {
                if row
                    .try_get::<String, _>("request_hash")
                    .map_err(RepositoryError::storage)?
                    != command.request_hash
                {
                    return Err(conflict());
                }
                let file = postgres_file(&row)?;
                transaction
                    .commit()
                    .await
                    .map_err(RepositoryError::storage)?;
                return Ok(CreateFileOutcome {
                    file,
                    replayed: true,
                });
            }
        }
        let inserted = sqlx::query(
            "INSERT INTO files(
               file_id,tenant_id,user_id,filename,media_type,expected_size_bytes,
               checksum_sha256,object_key,status,idempotency_key,request_hash,
               created_at,upload_expires_at
             ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'pending_upload',$9,$10,$11,$12)
             ON CONFLICT DO NOTHING",
        )
        .bind(&command.file_id)
        .bind(&command.tenant_id)
        .bind(&command.user_id)
        .bind(&command.filename)
        .bind(&command.media_type)
        .bind(
            i64::try_from(command.expected_size_bytes)
                .map_err(|_| RepositoryError::invalid_data())?,
        )
        .bind(&command.checksum_sha256)
        .bind(&command.object_key)
        .bind(&command.idempotency_key)
        .bind(&command.request_hash)
        .bind(database_time(command.created_at))
        .bind(database_time(command.upload_expires_at))
        .execute(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let query = format!("SELECT {FILE_COLUMNS} FROM files WHERE file_id=$1 FOR SHARE");
        let row = sqlx::query(AssertSqlSafe(query))
            .bind(&command.file_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?
            .ok_or_else(conflict)?;
        let file = postgres_file(&row)?;
        if !same_create(&file, &command) {
            return Err(conflict());
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(CreateFileOutcome {
            file,
            replayed: inserted.rows_affected() == 0,
        })
    }

    async fn get_file(&self, query: FileQuery) -> Result<Option<StoredFile>, RepositoryError> {
        let sql = format!(
            "SELECT {FILE_COLUMNS} FROM files WHERE file_id=$1 AND tenant_id=$2 AND user_id=$3"
        );
        sqlx::query(AssertSqlSafe(sql))
            .bind(query.file_id)
            .bind(query.tenant_id)
            .bind(query.user_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(RepositoryError::storage)?
            .map(|row| postgres_file(&row))
            .transpose()
    }

    async fn complete_file(
        &self,
        command: CompleteFileCommand,
    ) -> Result<Option<StoredFile>, RepositoryError> {
        sqlx::query(
            "UPDATE files SET status='ready',actual_size_bytes=$1,object_etag=$2,object_version_id=$3,ready_at=$4
             WHERE file_id=$5 AND tenant_id=$6 AND user_id=$7 AND status='pending_upload'
               AND expected_size_bytes=$1 AND upload_expires_at>=$4",
        )
        .bind(i64::try_from(command.actual_size_bytes).map_err(|_| RepositoryError::invalid_data())?)
        .bind(&command.object_etag)
        .bind(&command.object_version_id)
        .bind(database_time(command.ready_at))
        .bind(&command.query.file_id)
        .bind(&command.query.tenant_id)
        .bind(&command.query.user_id)
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        self.get_file(command.query).await
    }

    async fn begin_file_delete(
        &self,
        query: FileQuery,
        deleted_at: DateTime<Utc>,
    ) -> Result<Option<StoredFile>, RepositoryError> {
        sqlx::query(
            "UPDATE files SET status='deleting',deleted_at=$1
             WHERE file_id=$2 AND tenant_id=$3 AND user_id=$4 AND status NOT IN('deleting','deleted')",
        )
        .bind(database_time(deleted_at))
        .bind(&query.file_id)
        .bind(&query.tenant_id)
        .bind(&query.user_id)
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        self.get_file(query).await
    }

    async fn finish_file_delete(
        &self,
        query: FileQuery,
        deleted_at: DateTime<Utc>,
    ) -> Result<Option<StoredFile>, RepositoryError> {
        sqlx::query(
            "UPDATE files SET status='deleted',deleted_at=$1
             WHERE file_id=$2 AND tenant_id=$3 AND user_id=$4 AND status IN('deleting','deleted')",
        )
        .bind(database_time(deleted_at))
        .bind(&query.file_id)
        .bind(&query.tenant_id)
        .bind(&query.user_id)
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        self.get_file(query).await
    }

    async fn fail_file(
        &self,
        query: FileQuery,
        failed_at: DateTime<Utc>,
    ) -> Result<Option<StoredFile>, RepositoryError> {
        sqlx::query(
            "UPDATE files SET status='failed',deleted_at=$1
             WHERE file_id=$2 AND tenant_id=$3 AND user_id=$4 AND status='pending_upload'",
        )
        .bind(database_time(failed_at))
        .bind(&query.file_id)
        .bind(&query.tenant_id)
        .bind(&query.user_id)
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        self.get_file(query).await
    }

    async fn expire_pending_files(
        &self,
        observed_at: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<StoredFile>, RepositoryError> {
        if limit == 0 || limit > 10_000 {
            return Err(RepositoryError::invalid_data());
        }
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let ids = sqlx::query_scalar::<_, String>(
            "SELECT file_id FROM files
             WHERE status='pending_upload' AND upload_expires_at<$1
             ORDER BY upload_expires_at,file_id LIMIT $2 FOR UPDATE SKIP LOCKED",
        )
        .bind(database_time(observed_at))
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        for file_id in &ids {
            sqlx::query(
                "UPDATE files SET status='expired',deleted_at=$1
                 WHERE file_id=$2 AND status='pending_upload' AND upload_expires_at<$1",
            )
            .bind(database_time(observed_at))
            .bind(file_id)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
        }
        let mut files = Vec::new();
        for file_id in ids {
            let sql = format!("SELECT {FILE_COLUMNS} FROM files WHERE file_id=$1");
            if let Some(row) = sqlx::query(AssertSqlSafe(sql))
                .bind(file_id)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?
            {
                files.push(postgres_file(&row)?);
            }
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(files)
    }

    async fn list_deletable_files(
        &self,
        observed_at: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<StoredFile>, RepositoryError> {
        if limit == 0 || limit > 10_000 {
            return Err(RepositoryError::invalid_data());
        }
        let sql = format!(
            "SELECT {FILE_COLUMNS} FROM files f
             WHERE f.status IN('deleting','expired','failed')
               AND NOT EXISTS(
                 SELECT 1 FROM file_bindings b WHERE b.file_id=f.file_id
                   AND b.released_at IS NULL
                   AND (b.retain_until IS NULL OR b.retain_until>$1))
             ORDER BY COALESCE(f.deleted_at,f.upload_expires_at),f.file_id LIMIT $2"
        );
        let rows = sqlx::query(AssertSqlSafe(sql))
            .bind(database_time(observed_at))
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await
            .map_err(RepositoryError::storage)?;
        rows.iter().map(postgres_file).collect()
    }

    async fn resolve_bound_run_files(
        &self,
        query: BoundRunFilesQuery,
    ) -> Result<Vec<StoredFile>, RepositoryError> {
        if query.run_id.is_empty() || query.file_ids.len() > 100 {
            return Err(RepositoryError::invalid_data());
        }
        let mut seen = std::collections::HashSet::new();
        let mut files = Vec::with_capacity(query.file_ids.len());
        for file_id in query.file_ids {
            if !seen.insert(file_id.clone()) {
                return Err(RepositoryError::invalid_data());
            }
            let sql = format!(
                "SELECT {BOUND_FILE_COLUMNS} FROM files f JOIN file_bindings b ON b.file_id=f.file_id
                 WHERE b.target_kind='run' AND b.target_id=$1 AND b.file_id=$2
                   AND b.released_at IS NULL AND (b.retain_until IS NULL OR b.retain_until>$3)"
            );
            let row = sqlx::query(AssertSqlSafe(sql))
                .bind(&query.run_id)
                .bind(&file_id)
                .bind(database_time(query.observed_at))
                .fetch_optional(&self.pool)
                .await
                .map_err(RepositoryError::storage)?
                .ok_or_else(RepositoryError::invalid_data)?;
            files.push(validate_bound_file(
                postgres_file(&row)?,
                row.try_get("bound_filename")
                    .map_err(RepositoryError::storage)?,
                row.try_get("bound_media_type")
                    .map_err(RepositoryError::storage)?,
                row.try_get("bound_size_bytes")
                    .map_err(RepositoryError::storage)?,
                row.try_get("bound_object_etag")
                    .map_err(RepositoryError::storage)?,
                row.try_get("bound_object_version_id")
                    .map_err(RepositoryError::storage)?,
            )?);
        }
        Ok(files)
    }

    async fn resolve_bound_conversation_files(
        &self,
        query: BoundConversationFilesQuery,
    ) -> Result<Vec<StoredFile>, RepositoryError> {
        if query.conversation_id.is_empty()
            || query.tenant_id.is_empty()
            || query.user_id.is_empty()
            || query.file_ids.len() > 100
        {
            return Err(RepositoryError::invalid_data());
        }
        let mut seen = std::collections::HashSet::new();
        let mut files = Vec::with_capacity(query.file_ids.len());
        for file_id in query.file_ids {
            if !seen.insert(file_id.clone()) {
                return Err(RepositoryError::invalid_data());
            }
            let sql = format!(
                "SELECT {BOUND_FILE_COLUMNS} FROM files f JOIN file_bindings b ON b.file_id=f.file_id
                 WHERE b.target_kind='conversation' AND b.target_id=$1 AND b.tenant_id=$2
                   AND b.user_id=$3 AND b.file_id=$4 AND b.released_at IS NULL
                   AND (b.retain_until IS NULL OR b.retain_until>$5)"
            );
            let row = sqlx::query(AssertSqlSafe(sql))
                .bind(&query.conversation_id)
                .bind(&query.tenant_id)
                .bind(&query.user_id)
                .bind(&file_id)
                .bind(database_time(query.observed_at))
                .fetch_optional(&self.pool)
                .await
                .map_err(RepositoryError::storage)?
                .ok_or_else(RepositoryError::invalid_data)?;
            files.push(validate_bound_file(
                postgres_file(&row)?,
                row.try_get("bound_filename")
                    .map_err(RepositoryError::storage)?,
                row.try_get("bound_media_type")
                    .map_err(RepositoryError::storage)?,
                row.try_get("bound_size_bytes")
                    .map_err(RepositoryError::storage)?,
                row.try_get("bound_object_etag")
                    .map_err(RepositoryError::storage)?,
                row.try_get("bound_object_version_id")
                    .map_err(RepositoryError::storage)?,
            )?);
        }
        Ok(files)
    }

    async fn claim_file_deletions(
        &self,
        command: ClaimFileDeletionsCommand,
    ) -> Result<Vec<FileDeletionClaim>, RepositoryError> {
        if command.limit == 0
            || command.limit > 10_000
            || command.claim_expires_at <= command.observed_at
        {
            return Err(RepositoryError::invalid_data());
        }
        let mut transaction = self.pool.begin().await.map_err(RepositoryError::storage)?;
        let rows = sqlx::query(
            "SELECT f.file_id FROM files f
             WHERE f.status IN('deleting','expired','failed')
               AND (f.deletion_claim_token IS NULL OR f.deletion_claim_expires_at<=$1)
               AND NOT EXISTS(
                 SELECT 1 FROM file_bindings b WHERE b.file_id=f.file_id
                   AND b.released_at IS NULL
                   AND (b.retain_until IS NULL OR b.retain_until>$1))
             ORDER BY COALESCE(f.deleted_at,f.upload_expires_at),f.file_id
             LIMIT $2 FOR UPDATE SKIP LOCKED",
        )
        .bind(database_time(command.observed_at))
        .bind(i64::from(command.limit))
        .fetch_all(&mut *transaction)
        .await
        .map_err(RepositoryError::storage)?;
        let mut claims = Vec::with_capacity(rows.len());
        for row in rows {
            let file_id: String = row.try_get("file_id").map_err(RepositoryError::storage)?;
            let claim_token = format!("claim_{}", Uuid::new_v4().simple());
            sqlx::query(
                "UPDATE files SET deletion_claim_token=$1,deletion_claim_expires_at=$2,
                    deletion_fence=deletion_fence+1 WHERE file_id=$3",
            )
            .bind(&claim_token)
            .bind(database_time(command.claim_expires_at))
            .bind(&file_id)
            .execute(&mut *transaction)
            .await
            .map_err(RepositoryError::storage)?;
            let sql = format!("SELECT {FILE_COLUMNS},deletion_fence FROM files WHERE file_id=$1");
            let row = sqlx::query(AssertSqlSafe(sql))
                .bind(&file_id)
                .fetch_one(&mut *transaction)
                .await
                .map_err(RepositoryError::storage)?;
            claims.push(FileDeletionClaim {
                file: postgres_file(&row)?,
                claim_token,
                deletion_fence: u64::try_from(
                    row.try_get::<i64, _>("deletion_fence")
                        .map_err(RepositoryError::storage)?,
                )
                .map_err(|_| RepositoryError::invalid_data())?,
            });
        }
        transaction
            .commit()
            .await
            .map_err(RepositoryError::storage)?;
        Ok(claims)
    }

    async fn acknowledge_file_deletion(
        &self,
        command: AcknowledgeFileDeletionCommand,
    ) -> Result<bool, RepositoryError> {
        let updated = sqlx::query(
            "UPDATE files SET status='deleted',deleted_at=$1,deletion_claim_token=NULL,
                deletion_claim_expires_at=NULL
             WHERE file_id=$2 AND deletion_claim_token=$3 AND deletion_fence=$4
               AND status IN('deleting','expired','failed')",
        )
        .bind(database_time(command.deleted_at))
        .bind(command.file_id)
        .bind(command.claim_token)
        .bind(i64::try_from(command.deletion_fence).map_err(|_| RepositoryError::invalid_data())?)
        .execute(&self.pool)
        .await
        .map_err(RepositoryError::storage)?;
        Ok(updated.rows_affected() == 1)
    }
}

#[cfg(test)]
mod tests {
    use super::postgres_file_idempotency_lock_key;

    #[test]
    fn postgres_file_lock_key_is_nul_free_and_field_boundary_safe() {
        let first = postgres_file_idempotency_lock_key("tenant-a", "user", "key");
        let shifted = postgres_file_idempotency_lock_key("tenant", "a-user", "key");
        assert_ne!(first, shifted);
        assert!(!first.contains('\0'));
        assert_eq!(
            first,
            postgres_file_idempotency_lock_key("tenant-a", "user", "key")
        );
    }
}
