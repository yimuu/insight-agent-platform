//! Crash-safe local intent/result journal for public `insight apply` orchestration.
//!
//! The journal contains only authority IDs, ETags, digests, Receipt keys and trace IDs. It never
//! stores the access token, Secret value, Resource document, Deployment closure or Artifact body.

use insight_platform_contracts::{ResourceId, Sha256Digest, TraceId};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
use uuid::Uuid;

const JOURNAL_KIND: &str = "insight.platform.apply-journal/v1";
const MAX_JOURNAL_BYTES: u64 = 131_072;

#[derive(Debug)]
pub enum ApplyJournalError {
    Io { path: String, detail: String },
    Invalid(String),
}

impl fmt::Display for ApplyJournalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, detail } => {
                write!(
                    formatter,
                    "cannot persist apply journal at {path}: {detail}"
                )
            }
            Self::Invalid(detail) => write!(formatter, "apply journal is invalid: {detail}"),
        }
    }
}

impl std::error::Error for ApplyJournalError {}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JournalIntent {
    pub receipt: String,
    pub if_match: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JournalResource {
    pub resource_id: ResourceId,
    pub etag: String,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JournalPublishedVersion {
    pub resource_version_id: ResourceId,
    pub revision_no: u64,
    pub content_digest: Sha256Digest,
    pub artifact_id: Option<ResourceId>,
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JournalPublishResult {
    pub resource_etag: String,
    pub versions: Vec<JournalPublishedVersion>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JournalDeployment {
    pub deployment_id: ResourceId,
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ApplyJournalV1 {
    pub schema_version: u16,
    pub kind: String,
    pub manifest_digest: Sha256Digest,
    pub create_intent: JournalIntent,
    pub resource: Option<JournalResource>,
    pub validation_intent: Option<JournalIntent>,
    pub validation_operation_id: Option<ResourceId>,
    pub validated_resource_etag: Option<String>,
    pub publish_intent: Option<JournalIntent>,
    pub publish: Option<JournalPublishResult>,
    pub deployment_intent: Option<JournalIntent>,
    pub deployment: Option<JournalDeployment>,
    pub activation_intent: Option<JournalIntent>,
    pub final_resource_etag: Option<String>,
    pub step_trace_ids: BTreeMap<String, TraceId>,
}

impl ApplyJournalV1 {
    pub fn new(manifest_digest: Sha256Digest, create_receipt: String) -> Self {
        Self {
            schema_version: 1,
            kind: JOURNAL_KIND.to_owned(),
            manifest_digest,
            create_intent: JournalIntent {
                receipt: create_receipt,
                if_match: None,
            },
            resource: None,
            validation_intent: None,
            validation_operation_id: None,
            validated_resource_etag: None,
            publish_intent: None,
            publish: None,
            deployment_intent: None,
            deployment: None,
            activation_intent: None,
            final_resource_etag: None,
            step_trace_ids: BTreeMap::new(),
        }
    }

    pub fn validate(
        &self,
        expected_manifest_digest: &Sha256Digest,
    ) -> Result<(), ApplyJournalError> {
        if self.schema_version != 1
            || self.kind != JOURNAL_KIND
            || &self.manifest_digest != expected_manifest_digest
            || !valid_intent(&self.create_intent, false)
            || self
                .resource
                .as_ref()
                .is_some_and(|resource| resource.version == 0 || !valid_etag(&resource.etag))
            || self
                .validation_intent
                .as_ref()
                .is_some_and(|intent| !valid_intent(intent, true))
            || self.validation_intent.is_some() && self.resource.is_none()
            || self.validation_operation_id.is_some() && self.validation_intent.is_none()
            || self.validated_resource_etag.is_some() && self.validation_operation_id.is_none()
            || self
                .validated_resource_etag
                .as_ref()
                .is_some_and(|etag| !valid_etag(etag))
            || self
                .publish_intent
                .as_ref()
                .is_some_and(|intent| !valid_intent(intent, true))
            || self.publish_intent.is_some() && self.validated_resource_etag.is_none()
            || self.publish.is_some() && self.publish_intent.is_none()
            || self.publish.as_ref().is_some_and(|publish| {
                !valid_etag(&publish.resource_etag)
                    || publish.versions.is_empty()
                    || publish.versions.len() > 2
                    || publish.versions.iter().any(|version| {
                        version.revision_no == 0
                            || !valid_etag(&version.etag)
                            || version.artifact_id.as_ref().is_some_and(|id| {
                                id.kind() != insight_platform_contracts::ResourceKind::Artifact
                            })
                    })
            })
            || self
                .deployment_intent
                .as_ref()
                .is_some_and(|intent| !valid_intent(intent, true))
            || self.deployment_intent.is_some() && self.publish.is_none()
            || self.deployment.is_some() && self.deployment_intent.is_none()
            || self
                .deployment
                .as_ref()
                .is_some_and(|deployment| !valid_etag(&deployment.etag))
            || self
                .activation_intent
                .as_ref()
                .is_some_and(|intent| !valid_intent(intent, true))
            || self.activation_intent.is_some() && self.deployment.is_none()
            || self.final_resource_etag.is_some() && self.activation_intent.is_none()
            || self
                .final_resource_etag
                .as_ref()
                .is_some_and(|etag| !valid_etag(etag))
            || self.step_trace_ids.keys().any(|step| {
                !matches!(
                    step.as_str(),
                    "create" | "validate" | "read_validated" | "publish" | "deploy" | "activate"
                )
            })
        {
            return Err(ApplyJournalError::Invalid(
                "schema, identity, monotonic step closure, ETag, Receipt, or trace is invalid"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

pub fn journal_path(directory: &Path, digest: &Sha256Digest) -> PathBuf {
    let digest = digest.to_string();
    directory.join(format!(
        "{}.json",
        digest.strip_prefix("sha256:").unwrap_or("invalid-digest")
    ))
}

pub fn load(path: &Path) -> Result<Option<ApplyJournalV1>, ApplyJournalError> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(path, error)),
    };
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_JOURNAL_BYTES {
        return Err(ApplyJournalError::Invalid(
            "journal is not a bounded regular file".to_owned(),
        ));
    }
    let bytes = fs::read(path).map_err(|error| io_error(path, error))?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| ApplyJournalError::Invalid("journal is not closed JSON".to_owned()))
}

pub fn save(path: &Path, journal: &ApplyJournalV1) -> Result<(), ApplyJournalError> {
    journal.validate(&journal.manifest_digest)?;
    let directory = path.parent().ok_or_else(|| {
        ApplyJournalError::Invalid("journal path has no parent directory".to_owned())
    })?;
    fs::create_dir_all(directory).map_err(|error| io_error(directory, error))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))
            .map_err(|error| io_error(directory, error))?;
    }
    let bytes = serde_json::to_vec_pretty(journal)
        .map_err(|_| ApplyJournalError::Invalid("journal cannot be serialized".to_owned()))?;
    if bytes.len() as u64 > MAX_JOURNAL_BYTES {
        return Err(ApplyJournalError::Invalid(
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
        options.mode(0o600);
        let mut file = options
            .open(&temporary)
            .map_err(|error| io_error(&temporary, error))?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| io_error(&temporary, error))?;
        fs::rename(&temporary, path).map_err(|error| io_error(path, error))
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn valid_intent(intent: &JournalIntent, require_if_match: bool) -> bool {
    !intent.receipt.is_empty()
        && intent.receipt.len() <= 255
        && intent.receipt.is_ascii()
        && !intent.receipt.bytes().any(|byte| byte.is_ascii_control())
        && (require_if_match == intent.if_match.is_some())
        && intent.if_match.as_ref().is_none_or(|etag| valid_etag(etag))
}

fn valid_etag(value: &str) -> bool {
    value.len() >= 3
        && value.len() <= 128
        && value.starts_with('"')
        && value.ends_with('"')
        && value.is_ascii()
        && !value.bytes().any(|byte| byte.is_ascii_control())
}

fn io_error(path: &Path, error: std::io::Error) -> ApplyJournalError {
    ApplyJournalError::Io {
        path: path.display().to_string(),
        detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{ResourceKind, Sha256Digest};
    use tempfile::TempDir;

    fn digest() -> Sha256Digest {
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .parse()
            .unwrap()
    }

    #[test]
    fn journal_round_trip_is_bounded_and_monotonic() {
        let directory = TempDir::new().unwrap();
        let digest = digest();
        let path = journal_path(directory.path(), &digest);
        let mut journal = ApplyJournalV1::new(digest.clone(), "receipt-create".to_owned());
        save(&path, &journal).unwrap();
        assert_eq!(load(&path).unwrap(), Some(journal.clone()));

        journal.validation_intent = Some(JournalIntent {
            receipt: "receipt-validate".to_owned(),
            if_match: Some("\"pol_example-1\"".to_owned()),
        });
        assert!(journal.validate(&digest).is_err());
        journal.resource = Some(JournalResource {
            resource_id: ResourceId::from_uuid_v7(ResourceKind::Policy, Uuid::now_v7()).unwrap(),
            etag: "\"pol_example-1\"".to_owned(),
            version: 1,
        });
        assert!(journal.validate(&digest).is_ok());
    }
}
