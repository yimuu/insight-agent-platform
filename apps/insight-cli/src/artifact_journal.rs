//! Crash-safe local journal for one public Artifact upload.

use crate::artifact::{
    ArtifactMutationAcceptedV1, ArtifactUploadReportV1, PrepareArtifactUploadRequestV1,
    PrepareArtifactUploadResponseV1,
};
use insight_platform_contracts::{canonical_digest, ResourceId, ResourceKind, Sha256Digest};
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
use uuid::Uuid;

const JOURNAL_KIND: &str = "insight.platform.artifact-upload-journal/v2";
const MAX_JOURNAL_BYTES: u64 = 32_768;

#[derive(Debug)]
pub enum ArtifactJournalError {
    Io { path: String, detail: String },
    Invalid(String),
}

impl fmt::Display for ArtifactJournalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, detail } => {
                write!(
                    formatter,
                    "cannot persist Artifact upload journal at {path}: {detail}"
                )
            }
            Self::Invalid(detail) => {
                write!(formatter, "Artifact upload journal is invalid: {detail}")
            }
        }
    }
}

impl std::error::Error for ArtifactJournalError {}

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactUploadJournalV2 {
    pub upload_attempt_id: ResourceId,
    pub tenant_id: ResourceId,
    pub schema_version: u16,
    pub kind: String,
    pub request_digest: Sha256Digest,
    pub request: PrepareArtifactUploadRequestV1,
    pub prepare_receipt: String,
    pub prepared: Option<PrepareArtifactUploadResponseV1>,
    pub object_uploaded: bool,
    pub complete_receipt: Option<String>,
    pub completed: Option<ArtifactMutationAcceptedV1>,
    pub result: Option<ArtifactUploadReportV1>,
}

impl fmt::Debug for ArtifactUploadJournalV2 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactUploadJournalV2")
            .field("schema_version", &self.schema_version)
            .field("kind", &self.kind)
            .field("request_digest", &self.request_digest)
            .field("request", &self.request)
            .field("prepare_receipt", &self.prepare_receipt)
            .field("prepared", &self.prepared.as_ref().map(|_| "[REDACTED]"))
            .field("object_uploaded", &self.object_uploaded)
            .field("complete_receipt", &self.complete_receipt)
            .field("completed", &self.completed)
            .field("result", &self.result)
            .finish()
    }
}

impl ArtifactUploadJournalV2 {
    pub fn new(
        tenant_id: ResourceId,
        request_digest: Sha256Digest,
        request: PrepareArtifactUploadRequestV1,
    ) -> Result<Self, ArtifactJournalError> {
        let upload_attempt_id = ResourceId::from_uuid_v7(
            ResourceKind::ServerRequest,
            Uuid::now_v7(),
        )
        .map_err(|_| {
            ArtifactJournalError::Invalid("upload attempt identity generation failed".into())
        })?;
        let journal = Self {
            prepare_receipt: Self::receipt(&upload_attempt_id),
            upload_attempt_id,
            tenant_id,
            schema_version: 2,
            kind: JOURNAL_KIND.to_owned(),
            request_digest,
            request,
            prepared: None,
            object_uploaded: false,
            complete_receipt: None,
            completed: None,
            result: None,
        };
        journal.validate()?;
        Ok(journal)
    }
    fn receipt(attempt: &ResourceId) -> String {
        format!("insight-artifact-v2-{attempt}-prepare")
    }

    pub fn validate(&self) -> Result<(), ArtifactJournalError> {
        let observed_digest = canonical_digest(
            &serde_json::to_value(&self.request)
                .map_err(|_| ArtifactJournalError::Invalid("request is not JSON".to_owned()))?,
        )
        .map_err(|_| ArtifactJournalError::Invalid("request is not canonical".to_owned()))?;
        let prepared = self.prepared.as_ref();
        if self.schema_version != 2
            || self.upload_attempt_id.kind() != ResourceKind::ServerRequest
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.prepare_receipt != Self::receipt(&self.upload_attempt_id)
            || self.kind != JOURNAL_KIND
            || observed_digest != self.request_digest.to_string()
            || !valid_receipt(&self.prepare_receipt)
            || self.object_uploaded && prepared.is_none()
            || self.complete_receipt.is_some() != prepared.is_some()
            || self
                .complete_receipt
                .as_deref()
                .is_some_and(|receipt| !valid_receipt(receipt))
            || self.completed.is_some() && !self.object_uploaded
            || self.result.is_some() && self.completed.is_none()
            || self.completed.as_ref().is_some_and(|completed| {
                prepared.is_none_or(|prepared| {
                    completed.artifact_id != prepared.artifact_id
                        || completed.operation_id != prepared.operation_id
                })
            })
            || self.result.as_ref().is_some_and(|result| {
                prepared.is_none_or(|prepared| {
                    result.artifact_id != prepared.artifact_id.to_string()
                        || result.operation_id != prepared.operation_id.to_string()
                        || result.upload_grant_id != prepared.upload_grant_id.to_string()
                })
            })
        {
            return Err(ArtifactJournalError::Invalid(
                "request, Receipt, phase ordering, or authority identity is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}

pub fn journal_path(directory: &Path, request_digest: &Sha256Digest) -> PathBuf {
    let digest = request_digest
        .to_string()
        .strip_prefix("sha256:")
        .unwrap_or("invalid-digest")
        .to_owned();
    directory.join(format!("{digest}.json"))
}

pub fn load(path: &Path) -> Result<Option<ArtifactUploadJournalV2>, ArtifactJournalError> {
    let Some(bytes) = crate::read_runtime_file_bytes(path, MAX_JOURNAL_BYTES).map_err(|_| {
        ArtifactJournalError::Invalid("journal is not a bounded private single-link file".into())
    })?
    else {
        return Ok(None);
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if fs::symlink_metadata(path)
            .map_err(|error| io_error(path, error))?
            .permissions()
            .mode()
            & 0o077
            != 0
        {
            return Err(ArtifactJournalError::Invalid(
                "journal permissions are not private".into(),
            ));
        }
    }
    let value = insight_platform_contracts::parse_strict_json(
        &bytes,
        insight_platform_contracts::JsonLimits {
            max_bytes: MAX_JOURNAL_BYTES as usize,
            max_depth: 16,
            max_properties_per_object: 32,
            max_items_per_array: 16,
            max_string_bytes: 16_384,
        },
    )
    .map_err(|_| ArtifactJournalError::Invalid("journal is not bounded strict JSON".into()))?;
    let journal: ArtifactUploadJournalV2 = serde_json::from_value(value).map_err(|_| {
        ArtifactJournalError::Invalid("journal violates current closed contract".into())
    })?;
    journal.validate()?;
    Ok(Some(journal))
}

pub fn save(path: &Path, journal: &ArtifactUploadJournalV2) -> Result<(), ArtifactJournalError> {
    journal.validate()?;
    let directory = path
        .parent()
        .ok_or_else(|| ArtifactJournalError::Invalid("journal path has no parent".to_owned()))?;
    crate::private_state::ensure_durable_directory(directory)
        .map_err(|error| io_error(directory, error))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))
            .map_err(|error| io_error(directory, error))?;
    }
    let bytes = serde_json::to_vec_pretty(journal)
        .map_err(|_| ArtifactJournalError::Invalid("journal cannot be serialized".to_owned()))?;
    if bytes.len() as u64 > MAX_JOURNAL_BYTES {
        return Err(ArtifactJournalError::Invalid(
            "journal exceeds its size limit".to_owned(),
        ));
    }
    let temporary = path.with_extension(format!("{}.tmp", Uuid::now_v7()));
    let result = (|| {
        #[cfg(unix)]
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        let mut file = options
            .open(&temporary)
            .map_err(|error| io_error(&temporary, error))?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| io_error(&temporary, error))?;
        fs::rename(&temporary, path).map_err(|error| io_error(path, error))?;
        crate::private_state::sync_directory(directory).map_err(|error| io_error(directory, error))
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn valid_receipt(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && value.is_ascii()
        && !value.bytes().any(|byte| byte.is_ascii_control())
}

fn io_error(path: &Path, error: std::io::Error) -> ArtifactJournalError {
    ArtifactJournalError::Io {
        path: path.display().to_string(),
        detail: error.to_string(),
    }
}
