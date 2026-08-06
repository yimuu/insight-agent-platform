//! Principal-scoped immutable File admission boundary.

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::RunId;

/// Authenticated File Service caller. The HTTP layer constructs this value
/// after validating the principal headers; storage implementations must still
/// enforce the same tenant/user scope in their durable queries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilePrincipal {
    pub tenant_id: String,
    pub user_id: String,
}

impl FilePrincipal {
    pub fn new(
        tenant_id: impl Into<String>,
        user_id: impl Into<String>,
    ) -> Result<Self, FileServiceError> {
        let principal = Self {
            tenant_id: tenant_id.into(),
            user_id: user_id.into(),
        };
        if principal.tenant_id.is_empty()
            || principal.tenant_id.len() > 256
            || principal.user_id.is_empty()
            || principal.user_id.len() > 256
        {
            return Err(FileServiceError::invalid_request());
        }
        Ok(principal)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateFileRequest {
    pub filename: String,
    pub media_type: String,
    pub size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PublicFile {
    pub file_id: String,
    pub filename: String,
    pub media_type: String,
    pub size_bytes: u64,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FileUploadCapability {
    pub method: String,
    pub url: String,
    pub headers: BTreeMap<String, String>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CreateFileResponse {
    pub file: PublicFile,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload: Option<FileUploadCapability>,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FileDownloadCapability {
    pub method: String,
    pub url: String,
    pub headers: BTreeMap<String, String>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileServiceError {
    code: &'static str,
    message: &'static str,
}

impl FileServiceError {
    #[doc(hidden)]
    pub const fn new(code: &'static str, message: &'static str) -> Self {
        Self { code, message }
    }

    pub const fn code(&self) -> &'static str {
        self.code
    }

    pub const fn message(&self) -> &'static str {
        self.message
    }

    #[doc(hidden)]
    pub const fn invalid_request() -> Self {
        Self::new("FILE_REQUEST_INVALID", "File request is invalid")
    }

    #[doc(hidden)]
    pub const fn not_found() -> Self {
        Self::new("FILE_NOT_FOUND", "File not found")
    }

    #[doc(hidden)]
    pub const fn repository() -> Self {
        Self::new(
            "FILE_SERVICE_UNAVAILABLE",
            "File metadata service is unavailable",
        )
    }

    #[doc(hidden)]
    pub const fn object_storage() -> Self {
        Self::new(
            "OBJECT_STORAGE_UNAVAILABLE",
            "Object storage is unavailable",
        )
    }
}

impl std::fmt::Display for FileServiceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for FileServiceError {}

/// Transport-neutral File Service boundary consumed by the HTTP crate. This
/// keeps the API layer independent from concrete S3 and SQL adapters.
#[async_trait]
pub trait FileServiceApi: Send + Sync {
    async fn create_file(
        &self,
        principal: &FilePrincipal,
        request: CreateFileRequest,
        idempotency_key: Option<&str>,
    ) -> Result<CreateFileResponse, FileServiceError>;

    async fn complete_file(
        &self,
        principal: &FilePrincipal,
        file_id: &str,
    ) -> Result<PublicFile, FileServiceError>;

    async fn get_file(
        &self,
        principal: &FilePrincipal,
        file_id: &str,
    ) -> Result<PublicFile, FileServiceError>;

    async fn create_download(
        &self,
        principal: &FilePrincipal,
        file_id: &str,
    ) -> Result<FileDownloadCapability, FileServiceError>;

    async fn delete_file(
        &self,
        principal: &FilePrincipal,
        file_id: &str,
    ) -> Result<(), FileServiceError>;
}

/// One repository-authorized immutable File version. Private storage identity
/// is deliberately non-serializable and omitted from Debug output.
#[derive(Clone, PartialEq, Eq)]
pub struct AuthorizedFile {
    file_id: String,
    filename: String,
    media_type: String,
    size_bytes: u64,
    object_key: String,
    object_etag: String,
    object_version_id: Option<String>,
}

impl AuthorizedFile {
    #[doc(hidden)]
    pub fn from_storage_adapter(
        file_id: String,
        filename: String,
        media_type: String,
        size_bytes: u64,
        object_key: String,
        object_etag: String,
        object_version_id: Option<String>,
    ) -> Self {
        Self {
            file_id,
            filename,
            media_type,
            size_bytes,
            object_key,
            object_etag,
            object_version_id,
        }
    }

    pub fn file_id(&self) -> &str {
        &self.file_id
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn media_type(&self) -> &str {
        &self.media_type
    }

    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    #[doc(hidden)]
    pub fn object_key_for_storage_adapter(&self) -> &str {
        &self.object_key
    }

    #[doc(hidden)]
    pub fn object_etag_for_storage_adapter(&self) -> &str {
        &self.object_etag
    }

    #[doc(hidden)]
    pub fn object_version_id_for_storage_adapter(&self) -> Option<&str> {
        self.object_version_id.as_deref()
    }
}

impl std::fmt::Debug for AuthorizedFile {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorizedFile")
            .field("file_id", &self.file_id)
            .field("filename", &self.filename)
            .field("media_type", &self.media_type)
            .field("size_bytes", &self.size_bytes)
            .finish_non_exhaustive()
    }
}

/// Ephemeral read capability delivered directly to a provider adapter. The
/// URL is intentionally non-serializable and redacted from Debug output so it
/// cannot enter durable payloads or ordinary diagnostics.
#[derive(Clone, PartialEq, Eq)]
pub struct AuthorizedFileUrl(String);

impl AuthorizedFileUrl {
    #[doc(hidden)]
    pub fn from_storage_adapter(url: String) -> Self {
        Self(url)
    }

    #[doc(hidden)]
    pub fn expose_to_provider_adapter(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for AuthorizedFileUrl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AuthorizedFileUrl(<redacted>)")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileAuthorityError {
    code: &'static str,
    message: &'static str,
}

impl FileAuthorityError {
    #[doc(hidden)]
    pub const fn from_storage_adapter(code: &'static str, message: &'static str) -> Self {
        Self { code, message }
    }

    pub const fn code(&self) -> &'static str {
        self.code
    }

    pub const fn message(&self) -> &'static str {
        self.message
    }
}

impl std::fmt::Display for FileAuthorityError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for FileAuthorityError {}

#[async_trait]
pub trait FileAdmissionAuthority: Send + Sync {
    async fn resolve_files(
        &self,
        tenant_id: &str,
        user_id: &str,
        file_ids: &[String],
    ) -> Result<Vec<AuthorizedFile>, FileAuthorityError>;

    async fn resolve_conversation_files(
        &self,
        conversation_id: &str,
        tenant_id: &str,
        user_id: &str,
        current_file_ids: &[String],
        history_file_ids: &[String],
    ) -> Result<Vec<AuthorizedFile>, FileAuthorityError>;

    async fn resolve_run_files(
        &self,
        run_id: &RunId,
        file_ids: &[String],
    ) -> Result<Vec<AuthorizedFile>, FileAuthorityError>;

    async fn read_file(
        &self,
        file: &AuthorizedFile,
        max_bytes: usize,
    ) -> Result<Vec<u8>, FileAuthorityError>;

    async fn presign_file_read(
        &self,
        file: &AuthorizedFile,
    ) -> Result<AuthorizedFileUrl, FileAuthorityError>;
}
