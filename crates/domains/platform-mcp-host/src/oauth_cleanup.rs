use async_trait::async_trait;
use insight_platform_contracts::{
    ExactSecretBindingRef, ResourceId, ResourceKind, SecretResolutionPolicy, Sha256Digest,
    TraceIdentityV1, WorkerManifest,
};

use serde::{Deserialize, Serialize};
use std::{error::Error, fmt};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpOAuthPkceCleanupCause {
    Authorized,
    Declined,
    Expired,
}

/// Secret-free durable hint carried by the terminal OAuth Event/Outbox projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpOAuthPkceCleanupHint {
    pub schema_version: u32,
    pub secret_binding_id: ResourceId,
    pub binding_generation: u64,
}

impl McpOAuthPkceCleanupHint {
    pub fn validate(&self) -> Result<(), McpOAuthPkceCleanupError> {
        if self.schema_version != 1
            || self.secret_binding_id.kind() != ResourceKind::SecretBinding
            || self.binding_generation == 0
            || self.binding_generation > i64::MAX as u64
        {
            return Err(McpOAuthPkceCleanupError::Rejected(
                "mcp_oauth_pkce_cleanup_hint_invalid",
            ));
        }
        Ok(())
    }
}

/// Trusted delivery metadata is taken from the committed Event envelope, not duplicated inside
/// the durable cleanup hint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpOAuthPkceCleanupRequest {
    pub cleanup_job_id: ResourceId,
    pub task_generation: u64,
    pub deletion_effect_identity: Sha256Digest,
    pub fence: insight_platform_jobs::JobFence,
    pub tenant_id: ResourceId,
    pub task_id: ResourceId,
    pub cause: McpOAuthPkceCleanupCause,
    pub hint: McpOAuthPkceCleanupHint,
}

impl McpOAuthPkceCleanupRequest {
    pub fn validate(&self) -> Result<(), McpOAuthPkceCleanupError> {
        self.hint.validate()?;
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.task_id.kind() != ResourceKind::Interaction
            || self.cleanup_job_id.kind() != ResourceKind::Job
            || self.task_generation == 0
            || self.task_generation > i64::MAX as u64
            || self.fence.worker_process_generation_id.kind()
                != ResourceKind::WorkerProcessGeneration
            || self.fence.lease_generation == 0
            || self.fence.expected_version == 0
            || self.fence.lease_generation > i64::MAX as u64
            || self.fence.expected_version > i64::MAX as u64
            || crate::McpOAuthPkceCleanupJobPayload::effect_identity(
                &self.tenant_id,
                &self.task_id,
                self.task_generation,
                &self.hint,
            )
            .ok()
            .as_ref()
                != Some(&self.deletion_effect_identity)
        {
            return Err(McpOAuthPkceCleanupError::Rejected(
                "mcp_oauth_pkce_cleanup_envelope_invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizedMcpOAuthPkceCleanup {
    pub tenant_id: ResourceId,
    pub task_id: ResourceId,
    pub secret_binding: ExactSecretBindingRef,
}

impl AuthorizedMcpOAuthPkceCleanup {
    pub fn validate_for(
        &self,
        request: &McpOAuthPkceCleanupRequest,
    ) -> Result<(), McpOAuthPkceCleanupError> {
        request.validate()?;
        if self.tenant_id != request.tenant_id
            || self.task_id != request.task_id
            || self.secret_binding.secret_binding_id != request.hint.secret_binding_id
            || self.secret_binding.binding_generation != request.hint.binding_generation
            || self.secret_binding.purpose.as_str() != super::MCP_OAUTH_PKCE_SECRET_PURPOSE
            || self.secret_binding.validate().is_err()
            || !matches!(
                self.secret_binding.resolution_policy,
                SecretResolutionPolicy::Pinned { .. }
            )
        {
            return Err(McpOAuthPkceCleanupError::Rejected(
                "mcp_oauth_pkce_cleanup_authority_invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthPkceCleanupAuthorityError {
    StaleOrNotFound,
    Unavailable,
}

#[async_trait]
pub trait McpOAuthPkceCleanupAuthority: Send + Sync {
    async fn authorize_cleanup(
        &self,
        request: &McpOAuthPkceCleanupRequest,
    ) -> Result<AuthorizedMcpOAuthPkceCleanup, McpOAuthPkceCleanupAuthorityError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpOAuthPkceSecretCleanupDisposition {
    Deleted,
    AlreadyAbsent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthPkceSecretCleanupError {
    Rejected,
    TemporarilyUnavailable,
    OutcomeUncertain,
}

/// Trusted Secret Manager port. Implementations must delete only the exact pinned generation.
#[async_trait]
pub trait McpOAuthPkceSecretCleaner: Send + Sync {
    async fn delete_exact(
        &self,
        authorization: &AuthorizedMcpOAuthPkceCleanup,
    ) -> Result<McpOAuthPkceSecretCleanupDisposition, McpOAuthPkceSecretCleanupError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthPkceCleanupOutcome {
    Deleted,
    AlreadyAbsent,
    IgnoredStale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthPkceCleanupError {
    Rejected(&'static str),
    TemporarilyUnavailable(&'static str),
    OutcomeUncertain(&'static str),
}

impl McpOAuthPkceCleanupError {
    pub const fn safe_code(self) -> &'static str {
        match self {
            Self::Rejected(code)
            | Self::TemporarilyUnavailable(code)
            | Self::OutcomeUncertain(code) => code,
        }
    }
}

impl fmt::Display for McpOAuthPkceCleanupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Rejected(_) => "MCP OAuth PKCE cleanup was rejected",
            Self::TemporarilyUnavailable(_) => "MCP OAuth PKCE cleanup dependency is unavailable",
            Self::OutcomeUncertain(_) => "MCP OAuth PKCE cleanup outcome is uncertain",
        })
    }
}

impl Error for McpOAuthPkceCleanupError {}

/// Exact shared-Job lease for an already terminal OAuth Task.
///
/// `publish_attempts` is observation only. The claim owner and epoch are the mutation fence, so a
/// worker that resumes after its lease was reclaimed cannot acknowledge another worker's cleanup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedMcpOAuthPkceCleanup {
    pub event_id: ResourceId,
    pub attempt_no: u32,
    pub trace: TraceIdentityV1,
    pub request: McpOAuthPkceCleanupRequest,
}

impl ClaimedMcpOAuthPkceCleanup {
    pub fn validate(&self) -> Result<(), McpOAuthPkceCleanupDeliveryError> {
        self.request
            .validate()
            .map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptJob)?;
        if self.event_id.kind() != ResourceKind::Event
            || self.attempt_no == 0
            || self.trace.validate().is_err()
        {
            return Err(McpOAuthPkceCleanupDeliveryError::CorruptJob);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimDueMcpOAuthPkceCleanups {
    pub worker_manifest: WorkerManifest,
    pub claim_owner: ResourceId,
    pub lease_token_digests: Vec<Sha256Digest>,
    pub maximum_claims: u16,
    pub lease_milliseconds: u64,
}

impl ClaimDueMcpOAuthPkceCleanups {
    pub fn validate(
        &self,
        maximum_batch: u16,
        maximum_lease_milliseconds: u64,
    ) -> Result<(), McpOAuthPkceCleanupDeliveryError> {
        if self.claim_owner.kind() != ResourceKind::WorkerProcessGeneration
            || self.worker_manifest.validate().is_err()
            || self.worker_manifest.work_class != insight_platform_contracts::WorkClass::Recovery
            || self.lease_token_digests.len() != usize::from(self.maximum_claims)
            || self
                .lease_token_digests
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != self.lease_token_digests.len()
            || self.maximum_claims == 0
            || self.maximum_claims > maximum_batch
            || self.lease_milliseconds < 2
            || self.lease_milliseconds > maximum_lease_milliseconds
        {
            return Err(McpOAuthPkceCleanupDeliveryError::InvalidCommand);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthPkceCleanupSettlement {
    Completed {
        proof: McpOAuthPkceSecretCleanupDisposition,
    },
    Stale,
    Retry {
        failure_code: &'static str,
        delay_milliseconds: u64,
    },
    DeadLetter {
        failure_code: &'static str,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthPkceCleanupDeliveryError {
    InvalidCommand,
    CorruptJob,
    Unavailable,
}

impl fmt::Display for McpOAuthPkceCleanupDeliveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidCommand => "MCP OAuth cleanup delivery command is invalid",
            Self::CorruptJob => "MCP OAuth cleanup Job is invalid",
            Self::Unavailable => "MCP OAuth cleanup Job authority is unavailable",
        })
    }
}

impl Error for McpOAuthPkceCleanupDeliveryError {}

#[async_trait]
pub trait McpOAuthPkceCleanupJobs: Send + Sync {
    /// Restricted system expiry records a Task first-winner and its exact cleanup obligation.
    async fn expire_due_mcp_oauth_tasks(
        &self,
        command: crate::DriveExpiredMcpOAuthTasks,
    ) -> Result<
        insight_platform_jobs::store::SafetyScanPage<ResourceId>,
        McpOAuthPkceCleanupDeliveryError,
    >;

    async fn claim_due_mcp_oauth_pkce_cleanups(
        &self,
        command: ClaimDueMcpOAuthPkceCleanups,
    ) -> Result<Vec<ClaimedMcpOAuthPkceCleanup>, McpOAuthPkceCleanupDeliveryError>;

    /// Returns `false` when the exact claim fence was already lost. This is a normal first-winner
    /// outcome and must never be retried as an unfenced update.
    async fn settle_mcp_oauth_pkce_cleanup(
        &self,
        claim: &ClaimedMcpOAuthPkceCleanup,
        settlement: McpOAuthPkceCleanupSettlement,
    ) -> Result<bool, McpOAuthPkceCleanupDeliveryError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpOAuthPkceCleanupWorkerConfig {
    pub maximum_batch: u16,
    pub maximum_lease_milliseconds: u64,
    pub claim_batch: u16,
    pub lease_milliseconds: u64,
    pub retry_base_milliseconds: u64,
    pub retry_maximum_milliseconds: u64,
}

impl McpOAuthPkceCleanupWorkerConfig {
    pub fn validate(self) -> Result<(), McpOAuthPkceCleanupDeliveryError> {
        if self.maximum_batch == 0
            || self.maximum_lease_milliseconds == 0
            || self.claim_batch == 0
            || self.claim_batch > self.maximum_batch
            || self.lease_milliseconds < 2
            || self.lease_milliseconds > self.maximum_lease_milliseconds
            || self.retry_base_milliseconds == 0
            || self.retry_maximum_milliseconds < self.retry_base_milliseconds
            || self.retry_maximum_milliseconds > 3_600_000
        {
            return Err(McpOAuthPkceCleanupDeliveryError::InvalidCommand);
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct McpOAuthPkceCleanupWorkerSummary {
    pub claimed: u16,
    pub completed: u16,
    pub deferred: u16,
    pub dead_lettered: u16,
    pub lost_claims: u16,
}
