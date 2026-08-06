//! File metadata, S3 capability, and admission service.

use std::{collections::HashSet, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use insight_durable::{
    AcknowledgeFileDeletionCommand, BoundConversationFilesQuery, BoundRunFilesQuery,
    ClaimFileDeletionsCommand, CompleteFileCommand, CreateFileCommand, FileDurableRepository,
    FileQuery, FileStatus, RepositoryError, StoredFile,
};
use insight_engine::file_store::{
    AuthorizedFile, AuthorizedFileUrl, FileAdmissionAuthority, FileAuthorityError, FileServiceApi,
};
pub use insight_engine::file_store::{
    CreateFileRequest, CreateFileResponse, FileDownloadCapability, FilePrincipal, FileServiceError,
    FileUploadCapability, PublicFile,
};
use insight_engine::RunId;

use crate::s3_storage::{PresignedS3Request, S3Storage};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileServiceConfig {
    pub max_file_bytes: u64,
    pub max_files_per_invocation: usize,
    pub max_total_file_bytes_per_invocation: u64,
    pub pending_upload_ttl: Duration,
    pub deletion_claim_ttl: Duration,
}

impl FileServiceConfig {
    fn validate(&self) -> Result<(), FileServiceError> {
        if self.max_file_bytes == 0
            || self.max_files_per_invocation == 0
            || self.max_files_per_invocation > 100
            || self.max_total_file_bytes_per_invocation < self.max_file_bytes
            || self.pending_upload_ttl.is_zero()
            || self.pending_upload_ttl > Duration::from_secs(24 * 60 * 60)
            || self.deletion_claim_ttl.is_zero()
            || self.deletion_claim_ttl > Duration::from_secs(60 * 60)
        {
            return Err(FileServiceError::new(
                "FILE_SERVICE_CONFIG_INVALID",
                "File service configuration is invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedFile {
    pub file_id: String,
    pub filename: String,
    pub media_type: String,
    pub size_bytes: u64,
    pub object_key: String,
    pub object_etag: String,
    pub object_version_id: Option<String>,
}

#[derive(Clone)]
pub struct FileService {
    repository: Arc<dyn FileDurableRepository>,
    storage: Arc<S3Storage>,
    config: FileServiceConfig,
}

impl FileService {
    pub fn new(
        repository: Arc<dyn FileDurableRepository>,
        storage: Arc<S3Storage>,
        config: FileServiceConfig,
    ) -> Result<Self, FileServiceError> {
        config.validate()?;
        Ok(Self {
            repository,
            storage,
            config,
        })
    }

    pub async fn create_file(
        &self,
        principal: &FilePrincipal,
        request: CreateFileRequest,
        idempotency_key: Option<&str>,
    ) -> Result<CreateFileResponse, FileServiceError> {
        self.validate_create_request(&request, idempotency_key)?;
        let canonical = serde_jcs::to_vec(&json!({
            "filename": request.filename,
            "media_type": request.media_type,
            "size_bytes": request.size_bytes,
            "sha256": request.sha256,
        }))
        .map_err(|_| FileServiceError::invalid_request())?;
        let request_hash = format!("sha256:{}", hex_digest(&canonical));
        let now = Utc::now();
        let upload_expires_at = now
            + chrono::Duration::from_std(self.config.pending_upload_ttl)
                .map_err(|_| FileServiceError::invalid_request())?;
        let candidate_file_id = format!("file_{}", Uuid::new_v4().simple());
        let object_key = format!(
            "files/{}/{candidate_file_id}/content",
            tenant_namespace(&principal.tenant_id)
        );
        let outcome = self
            .repository
            .create_file(CreateFileCommand {
                file_id: candidate_file_id,
                tenant_id: principal.tenant_id.clone(),
                user_id: principal.user_id.clone(),
                filename: request.filename,
                media_type: request.media_type,
                expected_size_bytes: request.size_bytes,
                checksum_sha256: request.sha256,
                object_key,
                idempotency_key: idempotency_key.map(str::to_owned),
                request_hash,
                created_at: now,
                upload_expires_at,
            })
            .await
            .map_err(repository_error)?;
        let upload = if outcome.file.status == FileStatus::PendingUpload {
            let signed = self
                .storage
                .presign_put(
                    &outcome.file.object_key,
                    outcome.file.expected_size_bytes,
                    &outcome.file.media_type,
                    outcome.file.checksum_sha256.as_deref(),
                )
                .await
                .map_err(|_| FileServiceError::object_storage())?;
            let capability_expires_at = Utc::now()
                + chrono::Duration::from_std(self.storage.presign_upload_ttl())
                    .map_err(|_| FileServiceError::object_storage())?;
            Some(upload_capability(signed, capability_expires_at))
        } else {
            None
        };
        Ok(CreateFileResponse {
            file: public_file(&outcome.file),
            upload,
            replayed: outcome.replayed,
        })
    }

    pub async fn complete_file(
        &self,
        principal: &FilePrincipal,
        file_id: &str,
    ) -> Result<PublicFile, FileServiceError> {
        let query = file_query(principal, file_id)?;
        let file = self
            .repository
            .get_file(query.clone())
            .await
            .map_err(repository_error)?
            .ok_or_else(FileServiceError::not_found)?;
        if !matches!(file.status, FileStatus::PendingUpload | FileStatus::Ready) {
            return Err(Self::state_error(file.status));
        }
        if file.status == FileStatus::PendingUpload && file.upload_expires_at < Utc::now() {
            return Err(FileServiceError::new(
                "FILE_UPLOAD_EXPIRED",
                "File upload has expired",
            ));
        }
        let object = self
            .storage
            .find_object(&file.object_key)
            .await
            .map_err(|_| FileServiceError::object_storage())?
            .ok_or_else(|| {
                FileServiceError::new("FILE_UPLOAD_INCOMPLETE", "Uploaded object was not found")
            })?;
        if let Err(error) = verify_object(&file, &object) {
            if file.status == FileStatus::PendingUpload {
                let _ = self.repository.fail_file(query.clone(), Utc::now()).await;
            }
            return Err(error);
        }
        if file.status == FileStatus::Ready {
            return Ok(public_file(&file));
        }
        let completed = self
            .repository
            .complete_file(CompleteFileCommand {
                query,
                actual_size_bytes: object.size_bytes,
                object_etag: object.etag.ok_or_else(|| {
                    FileServiceError::new("FILE_CONTENT_MISMATCH", "File identity is unavailable")
                })?,
                object_version_id: object.version_id,
                ready_at: Utc::now(),
            })
            .await
            .map_err(repository_error)?
            .ok_or_else(FileServiceError::not_found)?;
        if completed.status != FileStatus::Ready {
            return Err(Self::state_error(completed.status));
        }
        Ok(public_file(&completed))
    }

    pub async fn get_file(
        &self,
        principal: &FilePrincipal,
        file_id: &str,
    ) -> Result<PublicFile, FileServiceError> {
        let file = self
            .repository
            .get_file(file_query(principal, file_id)?)
            .await
            .map_err(repository_error)?
            .ok_or_else(FileServiceError::not_found)?;
        if matches!(file.status, FileStatus::Deleting | FileStatus::Deleted) {
            return Err(FileServiceError::not_found());
        }
        Ok(public_file(&file))
    }

    pub async fn create_download(
        &self,
        principal: &FilePrincipal,
        file_id: &str,
    ) -> Result<FileDownloadCapability, FileServiceError> {
        let file = self.ready_file(principal, file_id).await?;
        self.verify_frozen_object(&file).await?;
        let signed = self
            .storage
            .presign_get(&file.object_key)
            .await
            .map_err(|_| FileServiceError::object_storage())?;
        Ok(FileDownloadCapability {
            method: signed.method,
            url: signed.url,
            headers: signed.headers,
            expires_at: Utc::now()
                + chrono::Duration::from_std(self.storage.presign_download_ttl())
                    .map_err(|_| FileServiceError::object_storage())?,
        })
    }

    pub async fn resolve_files(
        &self,
        principal: &FilePrincipal,
        file_ids: &[String],
    ) -> Result<Vec<ResolvedFile>, FileServiceError> {
        if file_ids.len() > self.config.max_files_per_invocation {
            return Err(FileServiceError::new(
                "FILE_LIMIT_EXCEEDED",
                "Too many files",
            ));
        }
        let mut seen = HashSet::new();
        let mut total = 0_u64;
        let mut files = Vec::with_capacity(file_ids.len());
        for file_id in file_ids {
            if !seen.insert(file_id) {
                return Err(FileServiceError::invalid_request());
            }
            let file = self.ready_file(principal, file_id).await?;
            let size = file
                .actual_size_bytes
                .ok_or_else(FileServiceError::repository)?;
            if size > self.config.max_file_bytes {
                return Err(FileServiceError::new("FILE_TOO_LARGE", "File is too large"));
            }
            total = total.checked_add(size).ok_or_else(|| {
                FileServiceError::new("FILE_LIMIT_EXCEEDED", "Total file size is too large")
            })?;
            if total > self.config.max_total_file_bytes_per_invocation {
                return Err(FileServiceError::new(
                    "FILE_LIMIT_EXCEEDED",
                    "Total file size is too large",
                ));
            }
            self.verify_frozen_object(&file).await?;
            files.push(ResolvedFile {
                file_id: file.file_id,
                filename: file.filename,
                media_type: file.media_type,
                size_bytes: size,
                object_key: file.object_key,
                object_etag: file.object_etag.ok_or_else(FileServiceError::repository)?,
                object_version_id: file.object_version_id,
            });
        }
        Ok(files)
    }

    pub async fn delete_file(
        &self,
        principal: &FilePrincipal,
        file_id: &str,
    ) -> Result<(), FileServiceError> {
        let query = file_query(principal, file_id)?;
        self.repository
            .begin_file_delete(query.clone(), Utc::now())
            .await
            .map_err(repository_error)?
            .ok_or_else(FileServiceError::not_found)?;
        Ok(())
    }

    pub async fn gc_once(&self, limit: u32) -> Result<u64, FileServiceError> {
        if limit == 0 || limit > 10_000 {
            return Err(FileServiceError::invalid_request());
        }
        let now = Utc::now();
        self.repository
            .expire_pending_files(now, limit)
            .await
            .map_err(repository_error)?;
        let claim_expires_at = now
            + chrono::Duration::from_std(self.config.deletion_claim_ttl)
                .map_err(|_| FileServiceError::repository())?;
        let claims = self
            .repository
            .claim_file_deletions(ClaimFileDeletionsCommand {
                observed_at: now,
                claim_expires_at,
                limit,
            })
            .await
            .map_err(repository_error)?;
        let mut deleted = 0_u64;
        for claim in claims {
            self.storage
                .delete_object_if_identity(
                    &claim.file.object_key,
                    claim.file.object_etag.as_deref(),
                    claim.file.object_version_id.as_deref(),
                )
                .await
                .map_err(|_| FileServiceError::object_storage())?;
            if self
                .repository
                .acknowledge_file_deletion(AcknowledgeFileDeletionCommand {
                    file_id: claim.file.file_id,
                    claim_token: claim.claim_token,
                    deletion_fence: claim.deletion_fence,
                    deleted_at: Utc::now(),
                })
                .await
                .map_err(repository_error)?
            {
                deleted = deleted.saturating_add(1);
            }
        }
        Ok(deleted)
    }

    pub async fn read_resolved_file(
        &self,
        file: &ResolvedFile,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FileServiceError> {
        if u64::try_from(max_bytes)
            .ok()
            .is_none_or(|max| file.size_bytes > max)
        {
            return Err(FileServiceError::new("FILE_TOO_LARGE", "File is too large"));
        }
        let bytes = self
            .storage
            .get_bytes(&file.object_key, max_bytes)
            .await
            .map_err(|_| FileServiceError::object_storage())?;
        Ok(bytes)
    }

    async fn ready_file(
        &self,
        principal: &FilePrincipal,
        file_id: &str,
    ) -> Result<StoredFile, FileServiceError> {
        let file = self
            .repository
            .get_file(file_query(principal, file_id)?)
            .await
            .map_err(repository_error)?
            .ok_or_else(FileServiceError::not_found)?;
        if file.status != FileStatus::Ready {
            return Err(Self::state_error(file.status));
        }
        Ok(file)
    }

    async fn verify_frozen_object(&self, file: &StoredFile) -> Result<(), FileServiceError> {
        let object = self
            .storage
            .find_object(&file.object_key)
            .await
            .map_err(|_| FileServiceError::object_storage())?
            .ok_or_else(|| {
                FileServiceError::new(
                    "FILE_IDENTITY_CHANGED",
                    "Stored file identity changed after completion",
                )
            })?;
        verify_object(file, &object)
    }

    fn validate_create_request(
        &self,
        request: &CreateFileRequest,
        idempotency_key: Option<&str>,
    ) -> Result<(), FileServiceError> {
        if request.size_bytes == 0 || request.size_bytes > self.config.max_file_bytes {
            return Err(FileServiceError::new(
                "FILE_LIMIT_EXCEEDED",
                "File exceeds the configured size limit",
            ));
        }
        if request.filename.is_empty()
            || request.filename.len() > 1024
            || request.filename.chars().any(char::is_control)
            || request.media_type.is_empty()
            || request.media_type.len() > 255
            || request.media_type.chars().any(char::is_whitespace)
            || request.sha256.as_ref().is_some_and(|sha256| {
                sha256.len() != 64
                    || !sha256
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
            || idempotency_key.is_some_and(|key| {
                key.is_empty()
                    || key.len() > 256
                    || key.chars().any(|character| character.is_control())
            })
        {
            return Err(FileServiceError::invalid_request());
        }
        Ok(())
    }

    fn state_error(status: FileStatus) -> FileServiceError {
        match status {
            FileStatus::PendingUpload => {
                FileServiceError::new("FILE_NOT_READY", "File upload is not complete")
            }
            FileStatus::Expired => {
                FileServiceError::new("FILE_UPLOAD_EXPIRED", "File upload has expired")
            }
            FileStatus::Failed => {
                FileServiceError::new("FILE_UPLOAD_FAILED", "File upload failed verification")
            }
            FileStatus::Deleting | FileStatus::Deleted => FileServiceError::not_found(),
            FileStatus::Ready => FileServiceError::repository(),
        }
    }
}

#[async_trait::async_trait]
impl FileServiceApi for FileService {
    async fn create_file(
        &self,
        principal: &FilePrincipal,
        request: CreateFileRequest,
        idempotency_key: Option<&str>,
    ) -> Result<CreateFileResponse, FileServiceError> {
        FileService::create_file(self, principal, request, idempotency_key).await
    }

    async fn complete_file(
        &self,
        principal: &FilePrincipal,
        file_id: &str,
    ) -> Result<PublicFile, FileServiceError> {
        FileService::complete_file(self, principal, file_id).await
    }

    async fn get_file(
        &self,
        principal: &FilePrincipal,
        file_id: &str,
    ) -> Result<PublicFile, FileServiceError> {
        FileService::get_file(self, principal, file_id).await
    }

    async fn create_download(
        &self,
        principal: &FilePrincipal,
        file_id: &str,
    ) -> Result<FileDownloadCapability, FileServiceError> {
        FileService::create_download(self, principal, file_id).await
    }

    async fn delete_file(
        &self,
        principal: &FilePrincipal,
        file_id: &str,
    ) -> Result<(), FileServiceError> {
        FileService::delete_file(self, principal, file_id).await
    }
}

#[async_trait::async_trait]
impl FileAdmissionAuthority for FileService {
    async fn resolve_files(
        &self,
        tenant_id: &str,
        user_id: &str,
        file_ids: &[String],
    ) -> Result<Vec<AuthorizedFile>, FileAuthorityError> {
        let principal = FilePrincipal::new(tenant_id, user_id).map_err(authority_error)?;
        FileService::resolve_files(self, &principal, file_ids)
            .await
            .map_err(authority_error)
            .map(|files| {
                files
                    .into_iter()
                    .map(|file| {
                        AuthorizedFile::from_storage_adapter(
                            file.file_id,
                            file.filename,
                            file.media_type,
                            file.size_bytes,
                            file.object_key,
                            file.object_etag,
                            file.object_version_id,
                        )
                    })
                    .collect()
            })
    }

    async fn resolve_conversation_files(
        &self,
        conversation_id: &str,
        tenant_id: &str,
        user_id: &str,
        current_file_ids: &[String],
        history_file_ids: &[String],
    ) -> Result<Vec<AuthorizedFile>, FileAuthorityError> {
        let mut seen = HashSet::new();
        if current_file_ids.len() + history_file_ids.len() > self.config.max_files_per_invocation
            || current_file_ids
                .iter()
                .chain(history_file_ids)
                .any(|file_id| !seen.insert(file_id.as_str()))
        {
            return Err(authority_error(FileServiceError::new(
                "FILE_LIMIT_EXCEEDED",
                "Too many or duplicate files",
            )));
        }
        let principal = FilePrincipal::new(tenant_id, user_id).map_err(authority_error)?;
        let current = FileService::resolve_files(self, &principal, current_file_ids)
            .await
            .map_err(authority_error)?;
        let history = self
            .repository
            .resolve_bound_conversation_files(BoundConversationFilesQuery {
                conversation_id: conversation_id.to_owned(),
                tenant_id: tenant_id.to_owned(),
                user_id: user_id.to_owned(),
                file_ids: history_file_ids.to_vec(),
                observed_at: Utc::now(),
            })
            .await
            .map_err(repository_error)
            .map_err(authority_error)?;
        let mut total = current
            .iter()
            .try_fold(0_u64, |total, file| total.checked_add(file.size_bytes))
            .ok_or_else(|| {
                authority_error(FileServiceError::new(
                    "FILE_LIMIT_EXCEEDED",
                    "Total file size is too large",
                ))
            })?;
        let mut authorized = current
            .into_iter()
            .map(|file| {
                AuthorizedFile::from_storage_adapter(
                    file.file_id,
                    file.filename,
                    file.media_type,
                    file.size_bytes,
                    file.object_key,
                    file.object_etag,
                    file.object_version_id,
                )
            })
            .collect::<Vec<_>>();
        for file in history {
            let size = file
                .actual_size_bytes
                .ok_or_else(FileServiceError::repository)
                .map_err(authority_error)?;
            if size > self.config.max_file_bytes {
                return Err(authority_error(FileServiceError::new(
                    "FILE_TOO_LARGE",
                    "File is too large",
                )));
            }
            total = total.checked_add(size).ok_or_else(|| {
                authority_error(FileServiceError::new(
                    "FILE_LIMIT_EXCEEDED",
                    "Total file size is too large",
                ))
            })?;
            if total > self.config.max_total_file_bytes_per_invocation {
                return Err(authority_error(FileServiceError::new(
                    "FILE_LIMIT_EXCEEDED",
                    "Total file size is too large",
                )));
            }
            self.verify_frozen_object(&file)
                .await
                .map_err(authority_error)?;
            authorized.push(AuthorizedFile::from_storage_adapter(
                file.file_id,
                file.filename,
                file.media_type,
                size,
                file.object_key,
                file.object_etag
                    .ok_or_else(|| authority_error(FileServiceError::repository()))?,
                file.object_version_id,
            ));
        }
        Ok(authorized)
    }

    async fn resolve_run_files(
        &self,
        run_id: &RunId,
        file_ids: &[String],
    ) -> Result<Vec<AuthorizedFile>, FileAuthorityError> {
        self.repository
            .resolve_bound_run_files(BoundRunFilesQuery {
                run_id: run_id.as_str().to_owned(),
                file_ids: file_ids.to_vec(),
                observed_at: Utc::now(),
            })
            .await
            .map_err(repository_error)
            .map_err(authority_error)?
            .into_iter()
            .map(|file| {
                Ok(AuthorizedFile::from_storage_adapter(
                    file.file_id,
                    file.filename,
                    file.media_type,
                    file.actual_size_bytes
                        .ok_or_else(FileServiceError::repository)?,
                    file.object_key,
                    file.object_etag.ok_or_else(FileServiceError::repository)?,
                    file.object_version_id,
                ))
            })
            .collect::<Result<Vec<_>, FileServiceError>>()
            .map_err(authority_error)
    }

    async fn read_file(
        &self,
        file: &AuthorizedFile,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FileAuthorityError> {
        FileService::read_resolved_file(
            self,
            &ResolvedFile {
                file_id: file.file_id().to_owned(),
                filename: file.filename().to_owned(),
                media_type: file.media_type().to_owned(),
                size_bytes: file.size_bytes(),
                object_key: file.object_key_for_storage_adapter().to_owned(),
                object_etag: file.object_etag_for_storage_adapter().to_owned(),
                object_version_id: file
                    .object_version_id_for_storage_adapter()
                    .map(str::to_owned),
            },
            max_bytes,
        )
        .await
        .map_err(authority_error)
    }

    async fn presign_file_read(
        &self,
        file: &AuthorizedFile,
    ) -> Result<AuthorizedFileUrl, FileAuthorityError> {
        let metadata = self
            .storage
            .find_object(file.object_key_for_storage_adapter())
            .await
            .map_err(|_| FileServiceError::object_storage())
            .map_err(authority_error)?
            .ok_or_else(|| {
                authority_error(FileServiceError::new(
                    "FILE_IDENTITY_CHANGED",
                    "Stored file identity changed after completion",
                ))
            })?;
        if metadata.size_bytes != file.size_bytes()
            || metadata.media_type.as_deref() != Some(file.media_type())
            || metadata.etag.as_deref() != Some(file.object_etag_for_storage_adapter())
            || metadata.version_id.as_deref() != file.object_version_id_for_storage_adapter()
        {
            return Err(authority_error(FileServiceError::new(
                "FILE_IDENTITY_CHANGED",
                "Stored file identity changed after completion",
            )));
        }
        let signed = self
            .storage
            .presign_get(file.object_key_for_storage_adapter())
            .await
            .map_err(|_| FileServiceError::object_storage())
            .map_err(authority_error)?;
        if signed.method != "GET"
            || signed
                .headers
                .keys()
                .any(|name| !name.eq_ignore_ascii_case("host"))
        {
            return Err(authority_error(FileServiceError::object_storage()));
        }
        Ok(AuthorizedFileUrl::from_storage_adapter(signed.url))
    }
}

fn authority_error(error: FileServiceError) -> FileAuthorityError {
    FileAuthorityError::from_storage_adapter(error.code(), error.message())
}

fn repository_error(error: RepositoryError) -> FileServiceError {
    if error.code() == insight_engine::repository::REPOSITORY_INTENT_CONFLICT {
        return FileServiceError::new(
            "IDEMPOTENCY_KEY_REUSED",
            "Idempotency key was already used for a different request",
        );
    }
    FileServiceError::repository()
}

fn file_query(principal: &FilePrincipal, file_id: &str) -> Result<FileQuery, FileServiceError> {
    if file_id.is_empty() || file_id.len() > 256 || file_id.chars().any(char::is_whitespace) {
        return Err(FileServiceError::not_found());
    }
    Ok(FileQuery {
        file_id: file_id.to_owned(),
        tenant_id: principal.tenant_id.clone(),
        user_id: principal.user_id.clone(),
    })
}

fn tenant_namespace(tenant_id: &str) -> String {
    hex_digest(tenant_id.as_bytes())[..32].to_owned()
}

fn hex_digest(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .chain(digest[16..].iter().map(|byte| format!("{byte:02x}")))
        .collect()
}

fn public_file(file: &StoredFile) -> PublicFile {
    PublicFile {
        file_id: file.file_id.clone(),
        filename: file.filename.clone(),
        media_type: file.media_type.clone(),
        size_bytes: file.actual_size_bytes.unwrap_or(file.expected_size_bytes),
        status: file.status.as_str().to_owned(),
        created_at: file.created_at,
    }
}

fn upload_capability(
    signed: PresignedS3Request,
    expires_at: DateTime<Utc>,
) -> FileUploadCapability {
    FileUploadCapability {
        method: signed.method,
        url: signed.url,
        headers: signed.headers,
        expires_at,
    }
}

fn verify_object(
    file: &StoredFile,
    object: &crate::s3_storage::S3ObjectMetadata,
) -> Result<(), FileServiceError> {
    if object.size_bytes != file.expected_size_bytes
        || object.media_type.as_deref() != Some(file.media_type.as_str())
        || file
            .checksum_sha256
            .as_ref()
            .is_some_and(|expected| object.checksum_sha256.as_deref() != Some(expected.as_str()))
    {
        return Err(FileServiceError::new(
            "FILE_CONTENT_MISMATCH",
            "Uploaded object does not match the declared file",
        ));
    }
    if file.status == FileStatus::Ready
        && (object.etag != file.object_etag || object.version_id != file.object_version_id)
    {
        return Err(FileServiceError::new(
            "FILE_IDENTITY_CHANGED",
            "Stored file identity changed after completion",
        ));
    }
    Ok(())
}
