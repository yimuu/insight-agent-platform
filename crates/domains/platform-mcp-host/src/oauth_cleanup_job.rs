//! Shared-Job ownership of exact PKCE deletion. The Task remains terminal;
//! physical recovery creates a new Job without changing deletion identity.
use crate::{
    McpOAuthPkceCleanupCause, McpOAuthPkceCleanupHint, McpOAuthPkceSecretCleanupDisposition,
};
use insight_platform_contracts::{
    canonical_digest, CommandAudit, DomainOperationRequirements, ExecutionRequirement, ResourceId,
    ResourceKind, Sha256Digest, WorkerExecutionCapability,
};
use serde::{Deserialize, Serialize};

pub const MCP_OAUTH_CLEANUP_JOB_VERSION: u32 = 1;
pub const MCP_OAUTH_CLEANUP_ATTEMPT_LIMIT: u32 = 8;
/// A terminal Task may authorize this many successor Jobs for the same exact
/// deletion effect. This budget is independent of each Job's attempt budget.
pub const MCP_OAUTH_CLEANUP_RECOVERY_LIMIT: usize = 8;
pub const MCP_OAUTH_CLEANUP_MAX_CHAIN_JOBS: usize = MCP_OAUTH_CLEANUP_RECOVERY_LIMIT + 1;
pub const MCP_OAUTH_CLEANUP_DEADLINE_SECONDS: i64 = 2_592_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpOAuthPkceCleanupJobPayload {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub task_id: ResourceId,
    pub task_generation: u64,
    pub cause: McpOAuthPkceCleanupCause,
    pub hint: McpOAuthPkceCleanupHint,
    pub source_event_id: ResourceId,
    pub deletion_effect_identity: Sha256Digest,
    pub predecessor_job_id: Option<ResourceId>,
    pub recovery_evidence_digest: Option<Sha256Digest>,
    pub deletion_proof: Option<McpOAuthPkceSecretCleanupDisposition>,
}
impl McpOAuthPkceCleanupJobPayload {
    pub fn effect_identity(
        tenant: &ResourceId,
        task: &ResourceId,
        generation: u64,
        hint: &McpOAuthPkceCleanupHint,
    ) -> Result<Sha256Digest, &'static str> {
        canonical_digest(&serde_json::json!({"contract":"mcp.pkce.exact_deletion","version":1,"tenant_id":tenant,"task_id":task,"task_generation":generation,"secret_binding_id":hint.secret_binding_id,"binding_generation":hint.binding_generation}))
            .map_err(|_|"cleanup effect cannot be canonicalized")?.parse().map_err(|_|"cleanup effect digest invalid")
    }
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != MCP_OAUTH_CLEANUP_JOB_VERSION
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.task_id.kind() != ResourceKind::Interaction
            || self.task_generation == 0
            || self.task_generation > i64::MAX as u64
            || self.source_event_id.kind() != ResourceKind::Event
            || self.hint.validate().is_err()
            || self
                .predecessor_job_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::Job)
            || self.predecessor_job_id.is_some() != self.recovery_evidence_digest.is_some()
            || self.deletion_effect_identity
                != Self::effect_identity(
                    &self.tenant_id,
                    &self.task_id,
                    self.task_generation,
                    &self.hint,
                )?
        {
            return Err("cleanup Job payload ownership or effect identity invalid");
        }
        Ok(())
    }
    pub fn execution_requirement(&self) -> Result<ExecutionRequirement, &'static str> {
        self.validate()?;
        Ok(ExecutionRequirement::DomainOperation {
            operation_abi_identity: cleanup_operation_identity(),
            requirements: DomainOperationRequirements::Control {
                control_policy_digest: cleanup_policy_identity(),
            },
        })
    }
}
fn cleanup_operation_identity() -> Sha256Digest {
    canonical_digest(&serde_json::json!({"contract":"mcp.pkce.exact_secret_generation_delete","abi":1,"task_current_job_fence":1,"absence_is_success":true})).expect("closed cleanup ABI").parse().expect("canonical digest")
}
fn cleanup_policy_identity() -> Sha256Digest {
    canonical_digest(&serde_json::json!({"contract":"mcp.pkce.cleanup_policy","version":1,"attempt_limit":MCP_OAUTH_CLEANUP_ATTEMPT_LIMIT,"recovery_limit":MCP_OAUTH_CLEANUP_RECOVERY_LIMIT,"deadline_seconds":MCP_OAUTH_CLEANUP_DEADLINE_SECONDS})).expect("closed cleanup policy").parse().expect("canonical digest")
}
pub fn mcp_oauth_cleanup_execution_capability() -> WorkerExecutionCapability {
    WorkerExecutionCapability::DomainOperation {
        operation_abi_identity: cleanup_operation_identity(),
        adapter: None,
    }
}

#[derive(Debug, Clone)]
pub struct RecoverMcpOAuthPkceCleanup {
    pub audit: CommandAudit,
    pub task_id: ResourceId,
    pub expected_task_generation: u64,
    pub expected_task_version: u64,
    pub previous_job_id: ResourceId,
    pub new_job_id: ResourceId,
    pub attempt_limit: u32,
    pub recovery_evidence_digest: Sha256Digest,
}
impl RecoverMcpOAuthPkceCleanup {
    pub fn request_digest(&self) -> Result<Sha256Digest, &'static str> {
        canonical_digest(&serde_json::json!({"operation":"mcp.pkce.cleanup.recover","version":1,"tenant_id":self.audit.tenant_id,"task_id":self.task_id,"task_generation":self.expected_task_generation,"task_version":self.expected_task_version,"previous_job_id":self.previous_job_id,"attempt_limit":self.attempt_limit,"recovery_evidence_digest":self.recovery_evidence_digest})).map_err(|_|"recovery request canonicalization failed")?.parse().map_err(|_|"invalid recovery digest")
    }
    pub fn validate_at(&self, now: chrono::DateTime<chrono::Utc>) -> Result<(), &'static str> {
        self.audit
            .validate_at(now)
            .map_err(|_| "invalid recovery audit")?;
        if self.task_id.kind() != ResourceKind::Interaction
            || self.previous_job_id.kind() != ResourceKind::Job
            || self.new_job_id.kind() != ResourceKind::Job
            || self.previous_job_id == self.new_job_id
            || self.expected_task_generation == 0
            || self.expected_task_generation > i64::MAX as u64
            || self.expected_task_version == 0
            || self.expected_task_version > i64::MAX as u64
            || !(1..=MCP_OAUTH_CLEANUP_ATTEMPT_LIMIT).contains(&self.attempt_limit)
            || self.audit.request_digest != self.request_digest()?
        {
            return Err("invalid recovery command");
        }
        Ok(())
    }
}

/// Safe current authority used by both recovery and retirement. The pinned hint
/// identifies a Secret generation; it contains no Secret locator or value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpOAuthCleanupChainIdentity {
    pub tenant_id: ResourceId,
    pub task_id: ResourceId,
    pub task_generation: u64,
    pub current_job_id: ResourceId,
    pub cause: McpOAuthPkceCleanupCause,
    pub hint: McpOAuthPkceCleanupHint,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpOAuthCleanupChainJob {
    pub job_id: ResourceId,
    pub state: insight_platform_contracts::JobState,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub terminal_at: Option<chrono::DateTime<chrono::Utc>>,
    pub deadline: chrono::DateTime<chrono::Utc>,
    pub payload_digest: Sha256Digest,
    pub payload: McpOAuthPkceCleanupJobPayload,
}
/// Links are current-to-initial, exactly as persisted. No caller-supplied count
/// authorizes recovery and no partial chain proves retirement.
pub fn validate_mcp_oauth_cleanup_chain(
    identity: &McpOAuthCleanupChainIdentity,
    chain: &[McpOAuthCleanupChainJob],
) -> Result<usize, &'static str> {
    if chain.is_empty() || chain.len() > MCP_OAUTH_CLEANUP_MAX_CHAIN_JOBS {
        return Err("cleanup chain is empty or exceeds recovery budget");
    }
    let mut seen = std::collections::BTreeSet::new();
    for (position, job) in chain.iter().enumerate() {
        job.payload.validate()?;
        let payload_digest = canonical_digest(
            &serde_json::to_value(&job.payload).map_err(|_| "cleanup payload cannot be encoded")?,
        )
        .map_err(|_| "cleanup payload cannot be canonicalized")?;
        if job.job_id.kind() != ResourceKind::Job
            || !seen.insert(job.job_id.clone())
            || payload_digest != job.payload_digest.to_string()
            || (position == 0 && job.job_id != identity.current_job_id)
            || job.payload.tenant_id != identity.tenant_id
            || job.payload.task_id != identity.task_id
            || job.payload.task_generation != identity.task_generation
            || job.payload.hint != identity.hint
            || job.payload.cause != identity.cause
            || job.deadline < job.created_at
            || job
                .terminal_at
                .is_some_and(|terminal| terminal < job.created_at)
            || job.payload.predecessor_job_id.as_ref()
                != chain.get(position + 1).map(|next| &next.job_id)
            || (position != 0
                && (!matches!(
                    job.state,
                    insight_platform_contracts::JobState::Failed
                        | insight_platform_contracts::JobState::Cancelled
                        | insight_platform_contracts::JobState::TimedOut
                ) || job.terminal_at.is_none()
                    || job.payload.deletion_proof.is_some()))
        {
            return Err("cleanup chain identity, predecessor or terminal evidence invalid");
        }
    }
    Ok(chain.len() - 1)
}
