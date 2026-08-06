//! Durable metadata authority for immutable user-uploaded files.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::{RepositoryError, RepositoryErrorExt as _};

/// Immutable File identity that must be revalidated and bound in the same
/// transaction as a Run admission. Storage locators intentionally remain in
/// the File repository and never enter this public-safe value.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RunFileBinding {
    pub file_id: String,
    pub tenant_id: String,
    pub user_id: String,
    pub filename: String,
    pub media_type: String,
    pub size_bytes: u64,
    pub object_etag: String,
    pub object_version_id: Option<String>,
}

impl RunFileBinding {
    pub fn validate(&self) -> Result<(), RepositoryError> {
        let valid = !self.file_id.is_empty()
            && self.file_id.len() <= 256
            && !self.tenant_id.is_empty()
            && self.tenant_id.len() <= 256
            && !self.user_id.is_empty()
            && self.user_id.len() <= 256
            && !self.filename.is_empty()
            && self.filename.len() <= 1024
            && !self.media_type.is_empty()
            && self.media_type.len() <= 255
            && !self.object_etag.is_empty()
            && self.object_etag.len() <= 1024
            && self
                .object_version_id
                .as_ref()
                .is_none_or(|value| !value.is_empty() && value.len() <= 1024)
            && [
                self.file_id.as_str(),
                self.tenant_id.as_str(),
                self.user_id.as_str(),
                self.filename.as_str(),
                self.media_type.as_str(),
                self.object_etag.as_str(),
            ]
            .into_iter()
            .all(|value| !value.chars().any(char::is_control));
        if !valid {
            return Err(RepositoryError::invalid_data());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileStatus {
    PendingUpload,
    Ready,
    Expired,
    Failed,
    Deleting,
    Deleted,
}

impl FileStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PendingUpload => "pending_upload",
            Self::Ready => "ready",
            Self::Expired => "expired",
            Self::Failed => "failed",
            Self::Deleting => "deleting",
            Self::Deleted => "deleted",
        }
    }

    pub fn parse(value: &str) -> Result<Self, RepositoryError> {
        match value {
            "pending_upload" => Ok(Self::PendingUpload),
            "ready" => Ok(Self::Ready),
            "expired" => Ok(Self::Expired),
            "failed" => Ok(Self::Failed),
            "deleting" => Ok(Self::Deleting),
            "deleted" => Ok(Self::Deleted),
            _ => Err(RepositoryError::invalid_data()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredFile {
    pub file_id: String,
    pub tenant_id: String,
    pub user_id: String,
    pub filename: String,
    pub media_type: String,
    pub expected_size_bytes: u64,
    pub actual_size_bytes: Option<u64>,
    pub checksum_sha256: Option<String>,
    pub object_key: String,
    pub object_etag: Option<String>,
    pub object_version_id: Option<String>,
    pub status: FileStatus,
    pub created_at: DateTime<Utc>,
    pub upload_expires_at: DateTime<Utc>,
    pub ready_at: Option<DateTime<Utc>>,
    pub deleted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateFileCommand {
    pub file_id: String,
    pub tenant_id: String,
    pub user_id: String,
    pub filename: String,
    pub media_type: String,
    pub expected_size_bytes: u64,
    pub checksum_sha256: Option<String>,
    pub object_key: String,
    pub idempotency_key: Option<String>,
    pub request_hash: String,
    pub created_at: DateTime<Utc>,
    pub upload_expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateFileOutcome {
    pub file: StoredFile,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileQuery {
    pub file_id: String,
    pub tenant_id: String,
    pub user_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteFileCommand {
    pub query: FileQuery,
    pub actual_size_bytes: u64,
    pub object_etag: String,
    pub object_version_id: Option<String>,
    pub ready_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundRunFilesQuery {
    pub run_id: String,
    pub file_ids: Vec<String>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundConversationFilesQuery {
    pub conversation_id: String,
    pub tenant_id: String,
    pub user_id: String,
    pub file_ids: Vec<String>,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileDeletionClaim {
    pub file: StoredFile,
    pub claim_token: String,
    pub deletion_fence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimFileDeletionsCommand {
    pub observed_at: DateTime<Utc>,
    pub claim_expires_at: DateTime<Utc>,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcknowledgeFileDeletionCommand {
    pub file_id: String,
    pub claim_token: String,
    pub deletion_fence: u64,
    pub deleted_at: DateTime<Utc>,
}

#[async_trait]
pub trait FileDurableRepository: Send + Sync {
    async fn create_file(
        &self,
        command: CreateFileCommand,
    ) -> Result<CreateFileOutcome, RepositoryError>;

    async fn get_file(&self, query: FileQuery) -> Result<Option<StoredFile>, RepositoryError>;

    async fn complete_file(
        &self,
        command: CompleteFileCommand,
    ) -> Result<Option<StoredFile>, RepositoryError>;

    async fn begin_file_delete(
        &self,
        query: FileQuery,
        deleted_at: DateTime<Utc>,
    ) -> Result<Option<StoredFile>, RepositoryError>;

    async fn finish_file_delete(
        &self,
        query: FileQuery,
        deleted_at: DateTime<Utc>,
    ) -> Result<Option<StoredFile>, RepositoryError>;

    async fn fail_file(
        &self,
        query: FileQuery,
        failed_at: DateTime<Utc>,
    ) -> Result<Option<StoredFile>, RepositoryError>;

    async fn expire_pending_files(
        &self,
        observed_at: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<StoredFile>, RepositoryError>;

    async fn list_deletable_files(
        &self,
        observed_at: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<StoredFile>, RepositoryError>;

    /// Resolves the immutable storage locators authorized by this Run's
    /// admission transaction. This remains valid after a user tombstones the
    /// public File, until the binding retention authority is released.
    async fn resolve_bound_run_files(
        &self,
        query: BoundRunFilesQuery,
    ) -> Result<Vec<StoredFile>, RepositoryError>;

    /// Resolves files retained by a platform-managed Conversation history.
    /// This authority survives a public File tombstone until Conversation
    /// privacy deletion or retention releases the immutable binding.
    async fn resolve_bound_conversation_files(
        &self,
        query: BoundConversationFilesQuery,
    ) -> Result<Vec<StoredFile>, RepositoryError>;

    async fn claim_file_deletions(
        &self,
        command: ClaimFileDeletionsCommand,
    ) -> Result<Vec<FileDeletionClaim>, RepositoryError>;

    async fn acknowledge_file_deletion(
        &self,
        command: AcknowledgeFileDeletionCommand,
    ) -> Result<bool, RepositoryError>;
}
