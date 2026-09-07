use crate::{
    JobState, PrincipalKind, PublicJobKind, ResourceId, ResourceKind, Sha256Digest, UtcTimestamp,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt};

const MAX_SAFE_OPERATION_FAILURE_CODE_BYTES: usize = 64;
const MAX_SAFE_OPERATION_FAILURE_MESSAGE_BYTES: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicJobState {
    Queued,
    Running,
    Waiting,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    ReconciliationRequired,
}

impl From<JobState> for PublicJobState {
    fn from(value: JobState) -> Self {
        match value {
            JobState::Ready | JobState::RetryScheduled => Self::Queued,
            JobState::Leased | JobState::Running | JobState::Cancelling => Self::Running,
            JobState::Waiting => Self::Waiting,
            JobState::Succeeded => Self::Succeeded,
            JobState::Failed => Self::Failed,
            JobState::Cancelled => Self::Cancelled,
            JobState::TimedOut => Self::TimedOut,
            JobState::ReconciliationRequired => Self::ReconciliationRequired,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PublicJobTarget {
    ResourceVersion {
        resource_id: ResourceId,
        resource_version: u64,
    },
    Deployment {
        deployment_id: ResourceId,
    },
    ContextDataset {
        context_dataset_id: ResourceId,
    },
    Artifact {
        artifact_id: ResourceId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundedOperationProgress {
    pub completed_units: u64,
    pub total_units: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SafeJobResult {
    Digest {
        result_digest: Sha256Digest,
    },
    ContextDatasetGeneration {
        result_digest: Sha256Digest,
        generation_id: ResourceId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SafeJobFailure {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadOperation {
    pub tenant_id: ResourceId,
    pub principal_id: ResourceId,
    pub principal_kind: PrincipalKind,
    pub operation_id: ResourceId,
    pub request_digest: Sha256Digest,
    pub deadline: DateTime<Utc>,
}

impl ReadOperation {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), OperationViewError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.principal_id.kind() != ResourceKind::Principal
            || self.principal_kind == PrincipalKind::InstallationOperator
            || self.operation_id.kind() != ResourceKind::Job
            || self.deadline <= now
        {
            return Err(OperationViewError::InvalidView);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationViewV1 {
    pub operation_id: ResourceId,
    pub tenant_id: ResourceId,
    pub kind: PublicJobKind,
    pub target: PublicJobTarget,
    pub state: PublicJobState,
    pub progress: Option<BoundedOperationProgress>,
    pub result: Option<SafeJobResult>,
    pub error: Option<SafeJobFailure>,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
    pub etag: String,
}

impl OperationViewV1 {
    pub fn validate(&self) -> Result<(), OperationViewError> {
        if self.operation_id.kind() != ResourceKind::Job
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.updated_at < self.created_at
            || self.etag
                != operation_etag(&self.operation_id.to_string(), self.version_from_etag()?)
        {
            return Err(OperationViewError::InvalidView);
        }
        validate_kind_target(self.kind, &self.target)?;
        if let Some(progress) = &self.progress {
            if progress.total_units == 0 || progress.completed_units > progress.total_units {
                return Err(OperationViewError::InvalidView);
            }
        }
        if self.result.is_some() && self.state != PublicJobState::Succeeded
            || self.error.is_some() && self.state != PublicJobState::Failed
        {
            return Err(OperationViewError::InvalidView);
        }
        match (&self.result, self.kind, self.state) {
            (
                Some(SafeJobResult::ContextDatasetGeneration { generation_id, .. }),
                PublicJobKind::ContextDatasetBuild,
                PublicJobState::Succeeded,
            ) if generation_id.kind() == ResourceKind::DatasetGeneration => {}
            (Some(SafeJobResult::Digest { .. }), PublicJobKind::ContextDatasetBuild, _)
            | (
                Some(SafeJobResult::ContextDatasetGeneration { .. }),
                PublicJobKind::ResourceValidation
                | PublicJobKind::McpDiscovery
                | PublicJobKind::ArtifactVerify
                | PublicJobKind::ArtifactDelete,
                _,
            )
            | (
                Some(SafeJobResult::ContextDatasetGeneration { .. }),
                PublicJobKind::ContextDatasetBuild,
                _,
            )
            | (None, PublicJobKind::ContextDatasetBuild, PublicJobState::Succeeded) => {
                return Err(OperationViewError::InvalidView)
            }
            (Some(SafeJobResult::Digest { .. }), _, _) | (None, _, _) => {}
        }
        if let Some(failure) = &self.error {
            if failure.code.is_empty()
                || failure.code.len() > MAX_SAFE_OPERATION_FAILURE_CODE_BYTES
                || failure.message.is_empty()
                || failure.message.len() > MAX_SAFE_OPERATION_FAILURE_MESSAGE_BYTES
                || !failure
                    .code
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
            {
                return Err(OperationViewError::InvalidView);
            }
        }
        Ok(())
    }

    fn version_from_etag(&self) -> Result<u64, OperationViewError> {
        let prefix = format!("\"{}-", self.operation_id);
        self.etag
            .strip_prefix(&prefix)
            .and_then(|value| value.strip_suffix('"'))
            .and_then(|value| value.parse().ok())
            .filter(|version| *version > 0)
            .ok_or(OperationViewError::InvalidView)
    }
}

pub fn operation_etag(operation_id: &str, version: u64) -> String {
    format!("\"{operation_id}-{version}\"")
}

pub fn validate_kind_target(
    kind: PublicJobKind,
    target: &PublicJobTarget,
) -> Result<(), OperationViewError> {
    let valid = match (kind, target) {
        (
            PublicJobKind::ResourceValidation,
            PublicJobTarget::ResourceVersion {
                resource_id,
                resource_version,
            },
        ) => resource_id.kind() != ResourceKind::Job && *resource_version > 0,
        (PublicJobKind::McpDiscovery, PublicJobTarget::Deployment { deployment_id }) => {
            deployment_id.kind() == ResourceKind::McpDeployment
        }
        (
            PublicJobKind::ContextDatasetBuild,
            PublicJobTarget::ContextDataset { context_dataset_id },
        ) => context_dataset_id.kind() == ResourceKind::ContextDataset,
        (
            PublicJobKind::ArtifactVerify | PublicJobKind::ArtifactDelete,
            PublicJobTarget::Artifact { artifact_id },
        ) => artifact_id.kind() == ResourceKind::Artifact,
        _ => false,
    };
    valid
        .then_some(())
        .ok_or(OperationViewError::InvalidKindTarget)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationViewError {
    InvalidKindTarget,
    InvalidView,
}

impl fmt::Display for OperationViewError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidKindTarget => "public Job kind and target do not match",
            Self::InvalidView => "public Operation view is invalid",
        })
    }
}

impl Error for OperationViewError {}

#[cfg(test)]
mod tests {
    use super::*;
    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1ca-32e4-75e1-a9e8-d95ca0f6{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn digest() -> Sha256Digest {
        format!("sha256:{}", "a".repeat(64)).parse().unwrap()
    }

    #[test]
    fn public_kind_target_matrix_is_closed() {
        let artifact = PublicJobTarget::Artifact {
            artifact_id: id(ResourceKind::Artifact, 1),
        };
        assert!(validate_kind_target(PublicJobKind::ArtifactVerify, &artifact).is_ok());
        assert!(validate_kind_target(PublicJobKind::ArtifactDelete, &artifact).is_ok());
        assert!(validate_kind_target(PublicJobKind::McpDiscovery, &artifact).is_err());

        let deployment = PublicJobTarget::Deployment {
            deployment_id: id(ResourceKind::McpDeployment, 2),
        };
        assert!(validate_kind_target(PublicJobKind::McpDiscovery, &deployment).is_ok());
        assert!(validate_kind_target(PublicJobKind::ArtifactVerify, &deployment).is_err());
    }

    #[test]
    fn internal_job_states_have_only_safe_public_projections() {
        assert_eq!(
            PublicJobState::from(JobState::Leased),
            PublicJobState::Running
        );
        assert_eq!(
            PublicJobState::from(JobState::RetryScheduled),
            PublicJobState::Queued
        );
        assert_eq!(
            PublicJobState::from(JobState::ReconciliationRequired),
            PublicJobState::ReconciliationRequired
        );
    }

    #[test]
    fn context_generation_result_is_closed_kind_state_and_id_bound() {
        let operation_id = id(ResourceKind::Job, 10);
        let timestamp = UtcTimestamp::from_datetime(Utc::now());
        let mut view = OperationViewV1 {
            operation_id: operation_id.clone(),
            tenant_id: id(ResourceKind::Tenant, 11),
            kind: PublicJobKind::ContextDatasetBuild,
            target: PublicJobTarget::ContextDataset {
                context_dataset_id: id(ResourceKind::ContextDataset, 12),
            },
            state: PublicJobState::Succeeded,
            progress: None,
            result: Some(SafeJobResult::ContextDatasetGeneration {
                result_digest: digest(),
                generation_id: id(ResourceKind::DatasetGeneration, 13),
            }),
            error: None,
            created_at: timestamp.clone(),
            updated_at: timestamp,
            etag: operation_etag(&operation_id.to_string(), 1),
        };
        assert!(view.validate().is_ok());

        view.result = Some(SafeJobResult::Digest {
            result_digest: digest(),
        });
        assert!(view.validate().is_err());
        view.result = None;
        assert!(view.validate().is_err());
        view.result = Some(SafeJobResult::ContextDatasetGeneration {
            result_digest: digest(),
            generation_id: id(ResourceKind::Job, 14),
        });
        assert!(view.validate().is_err());
        view.kind = PublicJobKind::ArtifactVerify;
        view.target = PublicJobTarget::Artifact {
            artifact_id: id(ResourceKind::Artifact, 15),
        };
        view.result = Some(SafeJobResult::Digest {
            result_digest: digest(),
        });
        assert!(view.validate().is_ok());
        view.result = Some(SafeJobResult::ContextDatasetGeneration {
            result_digest: digest(),
            generation_id: id(ResourceKind::DatasetGeneration, 16),
        });
        assert!(view.validate().is_err());
    }
}
