use crate::digest_without_field;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{ExactDeploymentRef, ResourceId, ResourceKind, Sha256Digest};
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt};

pub const CONTEXT_SUBSCRIPTION_ADMISSION_SCHEMA_VERSION: u32 = 1;
pub const MAX_CONTEXT_SUBSCRIPTION_RESOURCE_URI_BYTES: usize = 8_192;
pub const MAX_CONTEXT_SUBSCRIPTION_ADMISSION_BYTES: usize = 64 * 1_024;
pub const MAX_CONTEXT_SUBSCRIPTION_ADMISSION_CLOCK_SKEW_SECONDS: i64 = 60;

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
}
