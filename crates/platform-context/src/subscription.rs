use crate::digest_without_field;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{ExactDeploymentRef, ResourceId, ResourceKind, Sha256Digest};
use insight_platform_jobs::JobFence;
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt};

pub const CONTEXT_SUBSCRIPTION_ADMISSION_SCHEMA_VERSION: u32 = 1;
pub const MAX_CONTEXT_SUBSCRIPTION_RESOURCE_URI_BYTES: usize = 8_192;
pub const MAX_CONTEXT_SUBSCRIPTION_ADMISSION_BYTES: usize = 64 * 1_024;
pub const MAX_CONTEXT_SUBSCRIPTION_ADMISSION_CLOCK_SKEW_SECONDS: i64 = 60;
pub const CONTEXT_SUBSCRIPTION_REFRESH_EXECUTION_SCHEMA_VERSION: u32 = 1;
pub const MAX_CONTEXT_SUBSCRIPTION_REFRESH_RESOURCES: u32 = 4_096;
pub const MAX_CONTEXT_SUBSCRIPTION_REFRESH_ITEMS: u32 = 65_536;
pub const MAX_CONTEXT_SUBSCRIPTION_REFRESH_BYTES: u64 = 64 * 1_024 * 1_024;
pub const MAX_CONTEXT_SUBSCRIPTION_REFRESH_CURSOR_BYTES: usize = 2_048;

/// Why a durable subscription refresh is required. The resource identity is
/// frozen to the published subscription root, never copied from an untrusted
/// notification body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextSubscriptionRefreshCause {
    ResourceUpdated,
    ResourceListChanged,
    ToolListChanged,
    PromptListChanged,
    FullReconcile { observed_subscription_version: u64 },
}

impl ContextSubscriptionRefreshCause {
    fn validate(&self) -> Result<(), ContextSubscriptionAdmissionError> {
        if matches!(
            self,
            Self::FullReconcile {
                observed_subscription_version: 0,
                ..
            }
        ) {
            return Err(ContextSubscriptionAdmissionError::InvalidRequest);
        }
        Ok(())
    }
}

/// Closed command accepted by the Context application owner. It contains all
/// facts needed to reject drift before the owner creates durable work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextSubscriptionRefreshRequest {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub subscription_id: ResourceId,
    pub context_deployment: ExactDeploymentRef,
    pub mcp_deployment: ExactDeploymentRef,
    pub discovery_snapshot_id: ResourceId,
    pub discovery_snapshot_digest: Sha256Digest,
    pub resource_uri: String,
    pub resource_uri_digest: Sha256Digest,
    pub authorization_generation: u64,
    pub session_generation: u64,
    pub event_generation: u64,
    pub event_key_digest: Sha256Digest,
    pub body_digest: Sha256Digest,
    pub cause: ContextSubscriptionRefreshCause,
    pub deadline: DateTime<Utc>,
    pub request_digest: Sha256Digest,
}

impl ContextSubscriptionRefreshRequest {
    pub fn canonical_request_digest(
        &self,
    ) -> Result<Sha256Digest, ContextSubscriptionAdmissionError> {
        digest_without_field(self, "request_digest")
            .map_err(|_| ContextSubscriptionAdmissionError::Canonicalization)
    }

    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ContextSubscriptionAdmissionError> {
        self.context_deployment
            .validate()
            .map_err(|_| ContextSubscriptionAdmissionError::InvalidRequest)?;
        self.mcp_deployment
            .validate()
            .map_err(|_| ContextSubscriptionAdmissionError::InvalidRequest)?;
        self.cause.validate()?;
        let encoded = serde_json::to_vec(self)
            .map_err(|_| ContextSubscriptionAdmissionError::Canonicalization)?;
        if self.schema_version != CONTEXT_SUBSCRIPTION_ADMISSION_SCHEMA_VERSION
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.subscription_id.kind() != ResourceKind::McpOperation
            || self.context_deployment.resource_kind != ResourceKind::ContextDeployment
            || self.mcp_deployment.resource_kind != ResourceKind::McpDeployment
            || self.discovery_snapshot_id.kind() != ResourceKind::McpDiscoverySnapshot
            || self.resource_uri.is_empty()
            || self.resource_uri.len() > MAX_CONTEXT_SUBSCRIPTION_RESOURCE_URI_BYTES
            || self.resource_uri.chars().any(char::is_control)
            || self.authorization_generation == 0
            || self.session_generation == 0
            || self.event_generation == 0
            || self.deadline <= now
            || self.deadline > now + Duration::hours(24)
            || encoded.len() > MAX_CONTEXT_SUBSCRIPTION_ADMISSION_BYTES
            || self.canonical_request_digest()? != self.request_digest
        {
            return Err(ContextSubscriptionAdmissionError::InvalidRequest);
        }
        Ok(())
    }
}

/// Audit context supplied by the caller but revalidated and persisted by the
/// Context owner transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextSubscriptionAdmissionAudit {
    pub schema_version: u32,
    pub request_id: ResourceId,
    pub correlation_digest: Sha256Digest,
}

impl ContextSubscriptionAdmissionAudit {
    pub fn validate(&self) -> Result<(), ContextSubscriptionAdmissionError> {
        if self.schema_version != CONTEXT_SUBSCRIPTION_ADMISSION_SCHEMA_VERSION
            || self.request_id.kind() != ResourceKind::ServerRequest
        {
            return Err(ContextSubscriptionAdmissionError::InvalidAudit);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmitContextSubscriptionRefresh {
    pub request: ContextSubscriptionRefreshRequest,
    pub audit: ContextSubscriptionAdmissionAudit,
}

impl AdmitContextSubscriptionRefresh {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ContextSubscriptionAdmissionError> {
        self.request.validate_at(now)?;
        self.audit.validate()
    }
}

/// Immutable payload stored on the shared Context Job. The worker must reload
/// the referenced published closures before performing remote work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextSubscriptionRefreshJobPayload {
    pub schema_version: u32,
    pub request: ContextSubscriptionRefreshRequest,
}

impl ContextSubscriptionRefreshJobPayload {
    pub fn from_request(request: ContextSubscriptionRefreshRequest) -> Self {
        Self {
            schema_version: CONTEXT_SUBSCRIPTION_ADMISSION_SCHEMA_VERSION,
            request,
        }
    }

    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ContextSubscriptionAdmissionError> {
        if self.schema_version != CONTEXT_SUBSCRIPTION_ADMISSION_SCHEMA_VERSION {
            return Err(ContextSubscriptionAdmissionError::InvalidJobPayload);
        }
        self.request
            .validate_at(now)
            .map_err(|_| ContextSubscriptionAdmissionError::InvalidJobPayload)
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, ContextSubscriptionAdmissionError> {
        crate::digest(self).map_err(|_| ContextSubscriptionAdmissionError::Canonicalization)
    }
}

/// Stable acceptance returned after the Job, Receipt, Event, and Outbox commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedContextSubscriptionRefresh {
    pub schema_version: u32,
    pub request_digest: Sha256Digest,
    pub durable_work_digest: Sha256Digest,
    pub job_id: ResourceId,
    pub accepted_at: DateTime<Utc>,
}

impl AcceptedContextSubscriptionRefresh {
    pub fn validate_for(
        &self,
        request: &ContextSubscriptionRefreshRequest,
        now: DateTime<Utc>,
    ) -> Result<(), ContextSubscriptionAdmissionError> {
        if self.schema_version != CONTEXT_SUBSCRIPTION_ADMISSION_SCHEMA_VERSION
            || self.request_digest != request.request_digest
            || self.job_id.kind() != ResourceKind::Job
            || self.accepted_at
                > now + Duration::seconds(MAX_CONTEXT_SUBSCRIPTION_ADMISSION_CLOCK_SKEW_SECONDS)
            || self.accepted_at > request.deadline
        {
            return Err(ContextSubscriptionAdmissionError::InvalidAcceptance);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextSubscriptionAdmissionError {
    InvalidRequest,
    InvalidAudit,
    InvalidJobPayload,
    InvalidAcceptance,
    Rejected,
    Unavailable,
    CommitUncertain,
    Canonicalization,
}

impl fmt::Display for ContextSubscriptionAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidRequest => "Context subscription refresh request is invalid",
            Self::InvalidAudit => "Context subscription refresh audit is invalid",
            Self::InvalidJobPayload => "Context subscription refresh Job payload is invalid",
            Self::InvalidAcceptance => "Context subscription refresh acceptance is invalid",
            Self::Rejected => "Context subscription refresh admission was rejected",
            Self::Unavailable => "Context subscription refresh admission is unavailable",
            Self::CommitUncertain => "Context subscription refresh admission commit is uncertain",
            Self::Canonicalization => "Context subscription refresh canonicalization failed",
        })
    }
}

impl Error for ContextSubscriptionAdmissionError {}

#[async_trait]
pub trait ContextSubscriptionAdmissionAuthority: Send + Sync {
    async fn admit_context_subscription_refresh(
        &self,
        command: AdmitContextSubscriptionRefresh,
    ) -> Result<AcceptedContextSubscriptionRefresh, ContextSubscriptionAdmissionError>;
}

/// Credential-free request sent by a fenced Context Worker attempt to the MCP Host.
/// The Host reloads the current subscription and execution closure before I/O.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextSubscriptionRefreshAttempt {
    pub schema_version: u32,
    pub job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub job_fence: JobFence,
    pub attempt_number: u32,
    pub request: ContextSubscriptionRefreshRequest,
}

impl ContextSubscriptionRefreshAttempt {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ContextSubscriptionExecutionError> {
        self.request
            .validate_at(now)
            .map_err(|_| ContextSubscriptionExecutionError::InvalidAttempt)?;
        if self.schema_version != CONTEXT_SUBSCRIPTION_REFRESH_EXECUTION_SCHEMA_VERSION
            || self.job_id.kind() != ResourceKind::Job
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.job_fence.expected_version == 0
            || self.job_fence.lease_generation == 0
            || self.job_fence.worker_process_generation_id != self.worker_process_generation_id
            || self.attempt_number == 0
        {
            return Err(ContextSubscriptionExecutionError::InvalidAttempt);
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, ContextSubscriptionExecutionError> {
        crate::digest(self).map_err(|_| ContextSubscriptionExecutionError::Canonicalization)
    }
}

/// Bounded proof of a successful read-only resource refresh. Remote content and locators are
/// intentionally absent; a successful refresh does not create a Context Observation or cache.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextSubscriptionRefreshEvidence {
    pub schema_version: u32,
    pub attempt_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub response_digest: Sha256Digest,
    pub resource_set_digest: Sha256Digest,
    pub resource_count: u32,
    pub item_count: u32,
    pub byte_count: u64,
    pub remote_revision: Option<String>,
    pub cursor: Option<String>,
    pub observed_at: DateTime<Utc>,
}

impl ContextSubscriptionRefreshEvidence {
    pub fn validate_for(
        &self,
        attempt: &ContextSubscriptionRefreshAttempt,
        now: DateTime<Utc>,
    ) -> Result<(), ContextSubscriptionExecutionError> {
        let attempt_digest = attempt.canonical_digest()?;
        if self.schema_version != CONTEXT_SUBSCRIPTION_REFRESH_EXECUTION_SCHEMA_VERSION
            || self.attempt_digest != attempt_digest
            || self.request_digest != attempt.request.request_digest
            || self.resource_count > MAX_CONTEXT_SUBSCRIPTION_REFRESH_RESOURCES
            || self.item_count > MAX_CONTEXT_SUBSCRIPTION_REFRESH_ITEMS
            || self.byte_count > MAX_CONTEXT_SUBSCRIPTION_REFRESH_BYTES
            || !valid_optional_evidence(&self.remote_revision)
            || !valid_optional_evidence(&self.cursor)
            || self.observed_at > attempt.request.deadline
            || self.observed_at
                > now + Duration::seconds(MAX_CONTEXT_SUBSCRIPTION_ADMISSION_CLOCK_SKEW_SECONDS)
        {
            return Err(ContextSubscriptionExecutionError::InvalidEvidence);
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<Sha256Digest, ContextSubscriptionExecutionError> {
        crate::digest(self).map_err(|_| ContextSubscriptionExecutionError::Canonicalization)
    }
}

fn valid_optional_evidence(value: &Option<String>) -> bool {
    value.as_ref().is_none_or(|value| {
        !value.is_empty()
            && value.len() <= MAX_CONTEXT_SUBSCRIPTION_REFRESH_CURSOR_BYTES
            && !value.chars().any(char::is_control)
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextSubscriptionRefreshFailureClass {
    Rejected,
    Dependency,
    Capacity,
    Deadline,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextSubscriptionRefreshResponse {
    Completed {
        evidence: ContextSubscriptionRefreshEvidence,
    },
    RetryableFailure {
        class: ContextSubscriptionRefreshFailureClass,
        evidence_digest: Sha256Digest,
        retry_after_milliseconds: u64,
    },
    PermanentFailure {
        class: ContextSubscriptionRefreshFailureClass,
        evidence_digest: Sha256Digest,
    },
}

impl ContextSubscriptionRefreshResponse {
    pub fn canonical_digest(&self) -> Result<Sha256Digest, ContextSubscriptionExecutionError> {
        crate::digest(self).map_err(|_| ContextSubscriptionExecutionError::Canonicalization)
    }

    pub fn validate_for(
        &self,
        attempt: &ContextSubscriptionRefreshAttempt,
        now: DateTime<Utc>,
    ) -> Result<(), ContextSubscriptionExecutionError> {
        match self {
            Self::Completed { evidence } => evidence.validate_for(attempt, now),
            Self::RetryableFailure {
                class,
                retry_after_milliseconds,
                ..
            } => {
                if matches!(
                    class,
                    ContextSubscriptionRefreshFailureClass::Rejected
                        | ContextSubscriptionRefreshFailureClass::Cancelled
                ) || *retry_after_milliseconds == 0
                    || *retry_after_milliseconds > 3_600_000
                {
                    return Err(ContextSubscriptionExecutionError::InvalidResponse);
                }
                Ok(())
            }
            Self::PermanentFailure { class, .. } => {
                if matches!(
                    class,
                    ContextSubscriptionRefreshFailureClass::Dependency
                        | ContextSubscriptionRefreshFailureClass::Capacity
                ) {
                    return Err(ContextSubscriptionExecutionError::InvalidResponse);
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextSubscriptionExecutionError {
    InvalidAttempt,
    InvalidEvidence,
    InvalidResponse,
    Rejected,
    Unavailable,
    CompletionUncertain,
    Canonicalization,
}

impl fmt::Display for ContextSubscriptionExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidAttempt => "Context subscription refresh attempt is invalid",
            Self::InvalidEvidence => "Context subscription refresh evidence is invalid",
            Self::InvalidResponse => "Context subscription refresh response is invalid",
            Self::Rejected => "Context subscription refresh was rejected before dispatch",
            Self::Unavailable => "Context subscription refresh backend is unavailable",
            Self::CompletionUncertain => "Context subscription refresh completion is uncertain",
            Self::Canonicalization => "Context subscription refresh canonicalization failed",
        })
    }
}

impl Error for ContextSubscriptionExecutionError {}

#[async_trait]
pub trait ContextSubscriptionRefreshBackend: Send + Sync {
    async fn refresh_subscription_resources(
        &self,
        attempt: ContextSubscriptionRefreshAttempt,
    ) -> Result<ContextSubscriptionRefreshResponse, ContextSubscriptionExecutionError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::canonical_digest;
    use serde_json::json;

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1c9-32e4-75e1-a9e8-d95ca0f5{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn digest(label: &str) -> Sha256Digest {
        canonical_digest(&json!({"subscription_admission": label}))
            .unwrap()
            .parse()
            .unwrap()
    }

    fn request(now: DateTime<Utc>) -> ContextSubscriptionRefreshRequest {
        let mut request = ContextSubscriptionRefreshRequest {
            schema_version: CONTEXT_SUBSCRIPTION_ADMISSION_SCHEMA_VERSION,
            tenant_id: id(ResourceKind::Tenant, 1),
            subscription_id: id(ResourceKind::McpOperation, 2),
            context_deployment: ExactDeploymentRef::new(
                id(ResourceKind::ContextDeployment, 3),
                digest("context-deployment"),
            )
            .unwrap(),
            mcp_deployment: ExactDeploymentRef::new(
                id(ResourceKind::McpDeployment, 4),
                digest("mcp-deployment"),
            )
            .unwrap(),
            discovery_snapshot_id: id(ResourceKind::McpDiscoverySnapshot, 5),
            discovery_snapshot_digest: digest("discovery"),
            resource_uri: "mcp://knowledge/root".to_owned(),
            resource_uri_digest: digest("resource-uri"),
            authorization_generation: 7,
            session_generation: 8,
            event_generation: 9,
            event_key_digest: digest("event-key"),
            body_digest: digest("body"),
            cause: ContextSubscriptionRefreshCause::ResourceUpdated,
            deadline: now + Duration::minutes(5),
            request_digest: digest("placeholder"),
        };
        request.request_digest = request.canonical_request_digest().unwrap();
        request
    }

    fn attempt(now: DateTime<Utc>) -> ContextSubscriptionRefreshAttempt {
        let worker_process_generation_id = id(ResourceKind::WorkerProcessGeneration, 7);
        ContextSubscriptionRefreshAttempt {
            schema_version: CONTEXT_SUBSCRIPTION_REFRESH_EXECUTION_SCHEMA_VERSION,
            job_id: id(ResourceKind::Job, 6),
            worker_process_generation_id: worker_process_generation_id.clone(),
            job_fence: JobFence {
                expected_version: 3,
                worker_process_generation_id,
                lease_generation: 2,
                token_digest: digest("lease-token"),
            },
            attempt_number: 1,
            request: request(now),
        }
    }

    fn evidence(
        attempt: &ContextSubscriptionRefreshAttempt,
        now: DateTime<Utc>,
    ) -> ContextSubscriptionRefreshEvidence {
        ContextSubscriptionRefreshEvidence {
            schema_version: CONTEXT_SUBSCRIPTION_REFRESH_EXECUTION_SCHEMA_VERSION,
            attempt_digest: attempt.canonical_digest().unwrap(),
            request_digest: attempt.request.request_digest.clone(),
            response_digest: digest("response"),
            resource_set_digest: digest("resource-set"),
            resource_count: 2,
            item_count: 3,
            byte_count: 4_096,
            remote_revision: Some("revision-7".to_owned()),
            cursor: Some("cursor-8".to_owned()),
            observed_at: now,
        }
    }

    #[test]
    fn validates_closed_refresh_and_stable_job_payload() {
        let now = Utc::now();
        let request = request(now);
        request.validate_at(now).unwrap();
        let payload = ContextSubscriptionRefreshJobPayload::from_request(request.clone());
        payload.validate_at(now).unwrap();
        assert_eq!(
            payload.canonical_digest().unwrap(),
            payload.canonical_digest().unwrap()
        );

        let acceptance = AcceptedContextSubscriptionRefresh {
            schema_version: CONTEXT_SUBSCRIPTION_ADMISSION_SCHEMA_VERSION,
            request_digest: request.request_digest.clone(),
            durable_work_digest: payload.canonical_digest().unwrap(),
            job_id: id(ResourceKind::Job, 6),
            accepted_at: now,
        };
        acceptance.validate_for(&request, now).unwrap();
    }

    #[test]
    fn rejects_digest_drift_and_unbounded_resource_identity() {
        let now = Utc::now();
        let mut drifted = request(now);
        drifted.event_generation += 1;
        assert_eq!(
            drifted.validate_at(now),
            Err(ContextSubscriptionAdmissionError::InvalidRequest)
        );

        let mut oversized = request(now);
        oversized.resource_uri = "x".repeat(MAX_CONTEXT_SUBSCRIPTION_RESOURCE_URI_BYTES + 1);
        oversized.request_digest = oversized.canonical_request_digest().unwrap();
        assert_eq!(
            oversized.validate_at(now),
            Err(ContextSubscriptionAdmissionError::InvalidRequest)
        );
    }

    #[test]
    fn validates_fenced_refresh_attempt_and_bounded_evidence() {
        let now = Utc::now();
        let attempt = attempt(now);
        attempt.validate_at(now).unwrap();
        let response = ContextSubscriptionRefreshResponse::Completed {
            evidence: evidence(&attempt, now),
        };
        response.validate_for(&attempt, now).unwrap();
    }

    #[test]
    fn rejects_attempt_drift_unbounded_evidence_and_invalid_failure_mapping() {
        let now = Utc::now();
        let attempt = attempt(now);

        let mut wrong_worker = attempt.clone();
        wrong_worker.worker_process_generation_id = id(ResourceKind::WorkerProcessGeneration, 8);
        assert_eq!(
            wrong_worker.validate_at(now),
            Err(ContextSubscriptionExecutionError::InvalidAttempt)
        );

        let mut oversized = evidence(&attempt, now);
        oversized.byte_count = MAX_CONTEXT_SUBSCRIPTION_REFRESH_BYTES + 1;
        assert_eq!(
            oversized.validate_for(&attempt, now),
            Err(ContextSubscriptionExecutionError::InvalidEvidence)
        );

        let invalid_retry = ContextSubscriptionRefreshResponse::RetryableFailure {
            class: ContextSubscriptionRefreshFailureClass::Rejected,
            evidence_digest: digest("rejected"),
            retry_after_milliseconds: 1,
        };
        assert_eq!(
            invalid_retry.validate_for(&attempt, now),
            Err(ContextSubscriptionExecutionError::InvalidResponse)
        );
    }
}
