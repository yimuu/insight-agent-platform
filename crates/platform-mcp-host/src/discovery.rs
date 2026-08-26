use super::{
    digest_without_field, placeholder_digest, static_digest, valid_code, McpAuthorizationContext,
    McpExecutionContractResolutionError, McpHostError, McpTransportFailure, SafeMcpFailure,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::FutureExt;
use insight_platform_contracts::{
    ArtifactRef, CommandAudit, CommandOutcome, ExactDeploymentRef, ExactVersionRef, JobState,
    McpDeploymentClosure, McpDiscoverySnapshot, McpExperimentalFeature, McpNegotiatedCapabilities,
    McpProtocolPolicyDocument, McpServerExecutionContract, McpTransportKind, ResourceId,
    ResourceKind, Sha256Digest,
};
use insight_platform_jobs::JobFence;
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt, panic::AssertUnwindSafe, sync::Arc, time::Duration};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewMcpDiscoveryExecutionContract {
    pub deployment: ExactDeploymentRef,
    pub deployment_closure: McpDeploymentClosure,
    pub server: McpServerExecutionContract,
    pub protocol_profile: McpProtocolPolicyDocument,
    pub authorization: McpAuthorizationContext,
}

/// Exact executable contract used before a Discovery Snapshot exists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDiscoveryExecutionContract {
    pub schema_version: u32,
    pub deployment: ExactDeploymentRef,
    pub deployment_closure: McpDeploymentClosure,
    pub server: McpServerExecutionContract,
    pub protocol_profile: McpProtocolPolicyDocument,
    pub authorization: McpAuthorizationContext,
    pub canonical_digest: Sha256Digest,
}

/// Fenced authority lookup issued by an MCP worker after the shared Job has started.
///
/// The lookup follows only the durable discovery operation and the exact active lease. It never
/// follows an active registry head and does not require a Discovery Snapshot to exist yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpDiscoveryContractQuery {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub operation_id: ResourceId,
    pub job_id: ResourceId,
    pub fence: JobFence,
}

impl McpDiscoveryContractQuery {
    pub fn validate(&self) -> Result<(), McpExecutionContractResolutionError> {
        if self.schema_version != 1
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.operation_id.kind() != ResourceKind::McpOperation
            || self.job_id.kind() != ResourceKind::Job
            || self.fence.expected_version == 0
            || self.fence.worker_process_generation_id.kind()
                != ResourceKind::WorkerProcessGeneration
            || self.fence.lease_generation == 0
        {
            return Err(McpExecutionContractResolutionError::InvalidQuery);
        }
        Ok(())
    }
}

/// Fully reconstructed execution input returned by the durable authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedMcpDiscoveryExecution {
    pub operation_version: u64,
    pub admission_digest: Sha256Digest,
    pub attempt_limit: u32,
    pub contract: McpDiscoveryExecutionContract,
    pub request: McpDiscoveryRequest,
}

impl ResolvedMcpDiscoveryExecution {
    pub fn validate_for(
        &self,
        query: &McpDiscoveryContractQuery,
        now: DateTime<Utc>,
    ) -> Result<(), McpExecutionContractResolutionError> {
        query.validate()?;
        if self.operation_version == 0
            || self.attempt_limit == 0
            || self.request.physical_attempt > self.attempt_limit
            || self.contract.validate_canonical_at(now).is_err()
            || self.request.validate_for(&self.contract, now).is_err()
            || self.request.operation_id != query.operation_id
            || self.request.tenant_id != query.tenant_id
            || self.request.job_id != query.job_id
            || self.request.worker_process_generation_id != query.fence.worker_process_generation_id
            || self.request.lease_generation != query.fence.lease_generation
        {
            return Err(McpExecutionContractResolutionError::NotFoundOrChanged);
        }
        Ok(())
    }
}

#[async_trait]
pub trait McpDiscoveryExecutionContractResolver: Send + Sync {
    async fn resolve_mcp_discovery_execution(
        &self,
        query: &McpDiscoveryContractQuery,
    ) -> Result<ResolvedMcpDiscoveryExecution, McpExecutionContractResolutionError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpDiscoveryPersistenceError {
    InvalidCommand,
    Conflict,
    AuthorityUnavailable,
}

impl fmt::Display for McpDiscoveryPersistenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidCommand => "MCP discovery persistence command is invalid",
            Self::Conflict => "MCP discovery persistence first-winner was lost",
            Self::AuthorityUnavailable => "MCP discovery persistence authority is unavailable",
        })
    }
}

impl Error for McpDiscoveryPersistenceError {}

#[async_trait]
pub trait McpDiscoveryResultStore: Send + Sync {
    async fn commit_mcp_discovery_result(
        &self,
        command: CommitMcpDiscovery,
    ) -> Result<CommandOutcome<McpDiscoveryOperationRecord>, McpDiscoveryPersistenceError>;

    async fn resolve_mcp_discovery_attempt_result(
        &self,
        command: ResolveMcpDiscoveryAttempt,
    ) -> Result<CommandOutcome<McpDiscoveryOperationRecord>, McpDiscoveryPersistenceError>;
}

#[derive(Debug, Clone)]
pub struct ExecuteMcpDiscoveryJob {
    pub query: McpDiscoveryContractQuery,
    pub audit: McpWorkerAudit,
    pub snapshot_id: ResourceId,
    pub artifact_link_id: ResourceId,
    pub retry_at: DateTime<Utc>,
}

impl ExecuteMcpDiscoveryJob {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.query
            .validate()
            .map_err(|_| McpHostError::InvalidDiscovery)?;
        self.audit.validate_at(now)?;
        if self.audit.tenant_id != self.query.tenant_id
            || self.audit.worker_process_generation_id
                != self.query.fence.worker_process_generation_id
            || self.snapshot_id.kind() != ResourceKind::McpDiscoverySnapshot
            || self.artifact_link_id.kind() != ResourceKind::ArtifactLink
            || self.retry_at <= now
        {
            return Err(McpHostError::InvalidDiscovery);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum McpDiscoveryWorkerResult {
    SnapshotCommitted(CommandOutcome<McpDiscoveryOperationRecord>),
    AttemptResolved(CommandOutcome<McpDiscoveryOperationRecord>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpDiscoveryWorkerError {
    InvalidCommand,
    Contract(McpExecutionContractResolutionError),
    Persistence(McpDiscoveryPersistenceError),
}

impl fmt::Display for McpDiscoveryWorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCommand => formatter.write_str("MCP discovery worker command is invalid"),
            Self::Contract(failure) => write!(formatter, "{failure}"),
            Self::Persistence(failure) => write!(formatter, "{failure}"),
        }
    }
}

impl Error for McpDiscoveryWorkerError {}

pub struct McpDiscoveryWorker {
    resolver: Arc<dyn McpDiscoveryExecutionContractResolver>,
    client: Arc<dyn McpDiscoveryClient>,
    store: Arc<dyn McpDiscoveryResultStore>,
}

#[derive(Debug, Clone)]
enum PreparedMcpDiscoveryCommand {
    Commit(Box<CommitMcpDiscovery>),
    Resolve(Box<ResolveMcpDiscoveryAttempt>),
}

#[derive(Debug, Clone)]
pub struct PreparedMcpDiscovery {
    command: PreparedMcpDiscoveryCommand,
}

impl PreparedMcpDiscovery {
    pub fn refresh_fence(&mut self, fence: JobFence) -> Result<(), McpDiscoveryWorkerError> {
        let current = match &self.command {
            PreparedMcpDiscoveryCommand::Commit(command) => &command.fence,
            PreparedMcpDiscoveryCommand::Resolve(command) => &command.fence,
        };
        if fence.expected_version <= current.expected_version
            || fence.worker_process_generation_id != current.worker_process_generation_id
            || fence.lease_generation != current.lease_generation
            || fence.token_digest != current.token_digest
        {
            return Err(McpDiscoveryWorkerError::InvalidCommand);
        }
        match &mut self.command {
            PreparedMcpDiscoveryCommand::Commit(command) => command.fence = fence,
            PreparedMcpDiscoveryCommand::Resolve(command) => command.fence = fence,
        }
        Ok(())
    }
}

impl McpDiscoveryWorker {
    pub fn new(
        resolver: Arc<dyn McpDiscoveryExecutionContractResolver>,
        client: Arc<dyn McpDiscoveryClient>,
        store: Arc<dyn McpDiscoveryResultStore>,
    ) -> Self {
        Self {
            resolver,
            client,
            store,
        }
    }

    pub async fn execute(
        &self,
        command: ExecuteMcpDiscoveryJob,
    ) -> Result<McpDiscoveryWorkerResult, McpDiscoveryWorkerError> {
        let prepared = self.prepare(command).await?;
        self.commit(prepared).await
    }

    pub async fn prepare(
        &self,
        command: ExecuteMcpDiscoveryJob,
    ) -> Result<PreparedMcpDiscovery, McpDiscoveryWorkerError> {
        let started_at = Utc::now();
        command
            .validate_at(started_at)
            .map_err(|_| McpDiscoveryWorkerError::InvalidCommand)?;
        let resolved = self
            .resolver
            .resolve_mcp_discovery_execution(&command.query)
            .await
            .map_err(McpDiscoveryWorkerError::Contract)?;
        resolved
            .validate_for(&command.query, Utc::now())
            .map_err(McpDiscoveryWorkerError::Contract)?;
        let outcome = self
            .client
            .discover(&resolved.contract, &resolved.request)
            .await;
        if let Ok(McpDiscoveryOutcome::Candidate(candidate)) = outcome {
            let snapshot = candidate
                .into_snapshot(command.snapshot_id, &resolved.contract)
                .map_err(|_| McpDiscoveryWorkerError::InvalidCommand)?;
            return Ok(PreparedMcpDiscovery {
                command: PreparedMcpDiscoveryCommand::Commit(Box::new(CommitMcpDiscovery {
                    audit: command.audit,
                    operation_id: command.query.operation_id,
                    job_id: command.query.job_id,
                    fence: command.query.fence,
                    expected_operation_version: resolved.operation_version,
                    artifact_link_id: command.artifact_link_id,
                    snapshot,
                })),
            });
        }

        let resolution = match outcome {
            Ok(McpDiscoveryOutcome::RetryableFailure(failure))
                if resolved.request.physical_attempt < resolved.attempt_limit
                    && command.retry_at < resolved.request.deadline
                    && command.retry_at > Utc::now() =>
            {
                McpDiscoveryAttemptResolution::Retry {
                    retry_at: command.retry_at,
                    failure,
                }
            }
            Ok(
                McpDiscoveryOutcome::RetryableFailure(failure)
                | McpDiscoveryOutcome::PermanentFailure(failure),
            ) => McpDiscoveryAttemptResolution::Failed { failure },
            Ok(McpDiscoveryOutcome::ReauthorizationRequired { challenge_digest }) => {
                McpDiscoveryAttemptResolution::ReauthorizationRequired { challenge_digest }
            }
            Ok(McpDiscoveryOutcome::Candidate(_)) => unreachable!("candidate handled above"),
            Err(_) => McpDiscoveryAttemptResolution::Failed {
                failure: SafeMcpFailure {
                    safe_code: "mcp_discovery_host_rejected".to_owned(),
                    safe_message: "MCP discovery response failed host validation".to_owned(),
                    evidence_digest: static_digest("mcp_discovery_host_rejected"),
                },
            },
        };
        Ok(PreparedMcpDiscovery {
            command: PreparedMcpDiscoveryCommand::Resolve(Box::new(ResolveMcpDiscoveryAttempt {
                audit: command.audit,
                operation_id: command.query.operation_id,
                job_id: command.query.job_id,
                fence: command.query.fence,
                expected_operation_version: resolved.operation_version,
                resolution,
            })),
        })
    }

    pub async fn commit(
        &self,
        prepared: PreparedMcpDiscovery,
    ) -> Result<McpDiscoveryWorkerResult, McpDiscoveryWorkerError> {
        match prepared.command {
            PreparedMcpDiscoveryCommand::Commit(command) => self
                .store
                .commit_mcp_discovery_result(*command)
                .await
                .map(McpDiscoveryWorkerResult::SnapshotCommitted)
                .map_err(McpDiscoveryWorkerError::Persistence),
            PreparedMcpDiscoveryCommand::Resolve(command) => self
                .store
                .resolve_mcp_discovery_attempt_result(*command)
                .await
                .map(McpDiscoveryWorkerResult::AttemptResolved)
                .map_err(McpDiscoveryWorkerError::Persistence),
        }
    }
}

impl McpDiscoveryExecutionContract {
    pub fn build(input: NewMcpDiscoveryExecutionContract) -> Result<Self, McpHostError> {
        let mut contract = Self {
            schema_version: 1,
            deployment: input.deployment,
            deployment_closure: input.deployment_closure,
            server: input.server,
            protocol_profile: input.protocol_profile,
            authorization: input.authorization,
            canonical_digest: placeholder_digest()?,
        };
        contract.validate_at(Utc::now())?;
        contract.canonical_digest = digest_without_field(&contract, "canonical_digest")?;
        Ok(contract)
    }

    pub const fn transport_kind(&self) -> McpTransportKind {
        self.server.transport
    }

    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        if self.schema_version != 1
            || self.deployment.resource_kind != ResourceKind::McpDeployment
            || self.deployment.validate().is_err()
            || self.deployment_closure.validate().is_err()
            || self.server.validate().is_err()
            || self.protocol_profile.validate().is_err()
            || self.deployment_closure.server_revision != self.server.revision
            || self.deployment_closure.protocol_policy != self.server.protocol_policy
            || self.deployment_closure.transport.kind() != self.server.transport
            || self.authorization.mcp_deployment != self.deployment
            || self.authorization.audience_identity_digest
                != self.deployment_closure.server_identity_digest
            || !insight_platform_contracts::exact_secret_binding_purposes_match(
                &self.deployment_closure.secret_bindings,
                &self.server.deployment_credential_requirements,
            )
            || self.deployment_closure.auth_policy.is_none()
            || self.server.authorization_credential_purpose.as_ref()
                != Some(&self.authorization.token_secret_binding.purpose)
            || self.protocol_profile.canonical_digest().ok().as_ref()
                != Some(&self.server.protocol_policy.semantic_digest)
            || !discovery_transport_features_allowed(self)
        {
            return Err(McpHostError::InvalidDiscovery);
        }
        self.authorization.validate_canonical_at(now)
    }

    pub fn validate_canonical_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.validate_at(now)?;
        if digest_without_field(self, "canonical_digest")? != self.canonical_digest {
            return Err(McpHostError::InvalidDiscovery);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDiscoveryRequest {
    pub schema_version: u32,
    pub operation_id: ResourceId,
    pub tenant_id: ResourceId,
    pub job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub lease_generation: u64,
    pub physical_attempt: u32,
    pub authorization_binding_id: ResourceId,
    pub deadline: DateTime<Utc>,
}

impl McpDiscoveryRequest {
    pub fn validate_for(
        &self,
        contract: &McpDiscoveryExecutionContract,
        now: DateTime<Utc>,
    ) -> Result<(), McpHostError> {
        if self.schema_version != 1
            || self.operation_id.kind() != ResourceKind::McpOperation
            || self.tenant_id != contract.authorization.tenant_id
            || self.job_id.kind() != ResourceKind::Job
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.lease_generation == 0
            || self.physical_attempt == 0
            || self.authorization_binding_id != contract.authorization.authorization_binding_id
            || self.deadline <= now
            || self.deadline > contract.authorization.expires_at
        {
            return Err(McpHostError::InvalidDiscovery);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDiscoveryCandidate {
    pub schema_version: u32,
    pub negotiated_version: String,
    pub negotiated_capabilities: McpNegotiatedCapabilities,
    pub objects_artifact: ArtifactRef,
    pub observed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub canonical_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpDiscoveryOutcome {
    Candidate(McpDiscoveryCandidate),
    RetryableFailure(SafeMcpFailure),
    PermanentFailure(SafeMcpFailure),
    ReauthorizationRequired { challenge_digest: Sha256Digest },
}

impl McpDiscoveryOutcome {
    pub fn validate_for(
        &self,
        contract: &McpDiscoveryExecutionContract,
    ) -> Result<(), McpHostError> {
        match self {
            Self::Candidate(candidate) => candidate.validate_for(contract),
            Self::RetryableFailure(failure) | Self::PermanentFailure(failure) => failure.validate(),
            Self::ReauthorizationRequired { .. } => Ok(()),
        }
    }
}

impl McpDiscoveryCandidate {
    pub fn build(
        negotiated_version: String,
        negotiated_capabilities: McpNegotiatedCapabilities,
        objects_artifact: ArtifactRef,
        observed_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
        contract: &McpDiscoveryExecutionContract,
    ) -> Result<Self, McpHostError> {
        let mut candidate = Self {
            schema_version: 1,
            negotiated_version,
            negotiated_capabilities,
            objects_artifact,
            observed_at,
            expires_at,
            canonical_digest: placeholder_digest()?,
        };
        candidate.validate_shape_for(contract)?;
        candidate.canonical_digest = digest_without_field(&candidate, "canonical_digest")?;
        Ok(candidate)
    }

    pub fn validate_for(
        &self,
        contract: &McpDiscoveryExecutionContract,
    ) -> Result<(), McpHostError> {
        self.validate_shape_for(contract)?;
        if digest_without_field(self, "canonical_digest")? != self.canonical_digest {
            return Err(McpHostError::InvalidDiscovery);
        }
        Ok(())
    }

    fn validate_shape_for(
        &self,
        contract: &McpDiscoveryExecutionContract,
    ) -> Result<(), McpHostError> {
        if self.schema_version != 1
            || !contract
                .protocol_profile
                .offered_versions
                .contains(&self.negotiated_version)
            || self.objects_artifact.validate().is_err()
            || self.expires_at <= self.observed_at
            || self.expires_at > contract.authorization.expires_at
            || !candidate_capabilities_allowed(self, contract)
        {
            return Err(McpHostError::InvalidDiscovery);
        }
        Ok(())
    }

    pub fn into_snapshot(
        self,
        snapshot_id: ResourceId,
        contract: &McpDiscoveryExecutionContract,
    ) -> Result<McpDiscoverySnapshot, McpHostError> {
        self.validate_for(contract)?;
        McpDiscoverySnapshot::build(
            snapshot_id,
            contract.deployment.clone(),
            contract.server.revision.clone(),
            contract.server.protocol_policy.clone(),
            contract.authorization.canonical_digest.clone(),
            self.negotiated_version,
            self.negotiated_capabilities,
            self.objects_artifact,
            self.observed_at,
            self.expires_at,
        )
        .map_err(|_| McpHostError::InvalidDiscovery)
    }
}

#[async_trait]
pub trait McpDiscoveryTransport: Send + Sync {
    fn kind(&self) -> McpTransportKind;

    async fn discover(
        &self,
        contract: &McpDiscoveryExecutionContract,
        request: &McpDiscoveryRequest,
    ) -> Result<McpDiscoveryCandidate, McpTransportFailure>;
}

#[async_trait]
pub trait McpDiscoveryClient: Send + Sync {
    async fn discover(
        &self,
        contract: &McpDiscoveryExecutionContract,
        request: &McpDiscoveryRequest,
    ) -> Result<McpDiscoveryOutcome, McpHostError>;
}

pub struct McpDiscoveryService {
    transport: Arc<dyn McpDiscoveryTransport>,
}

impl McpDiscoveryService {
    pub fn new(transport: Arc<dyn McpDiscoveryTransport>) -> Self {
        Self { transport }
    }
}

#[async_trait]
impl McpDiscoveryClient for McpDiscoveryService {
    async fn discover(
        &self,
        contract: &McpDiscoveryExecutionContract,
        request: &McpDiscoveryRequest,
    ) -> Result<McpDiscoveryOutcome, McpHostError> {
        let now = Utc::now();
        contract.validate_canonical_at(now)?;
        request.validate_for(contract, now)?;
        if self.transport.kind() != contract.transport_kind() {
            return Err(McpHostError::WrongTransport);
        }
        let remaining = u64::try_from((request.deadline - now).num_milliseconds())
            .map_err(|_| McpHostError::InvalidDiscovery)?;
        let timeout = remaining.min(contract.server.limits.total_timeout_milliseconds);
        let future = AssertUnwindSafe(self.transport.discover(contract, request)).catch_unwind();
        let outcome = match tokio::time::timeout(Duration::from_millis(timeout), future).await {
            Ok(Ok(Ok(candidate))) => McpDiscoveryOutcome::Candidate(candidate),
            Ok(Ok(Err(failure))) => map_discovery_transport_failure(failure)?,
            Ok(Err(_)) => retryable_discovery_failure("mcp_discovery_transport_panic"),
            Err(_) => retryable_discovery_failure("mcp_discovery_transport_timeout"),
        };
        outcome.validate_for(contract)?;
        Ok(outcome)
    }
}

fn map_discovery_transport_failure(
    failure: McpTransportFailure,
) -> Result<McpDiscoveryOutcome, McpHostError> {
    failure.validate_wire_shape()?;
    Ok(match failure {
        McpTransportFailure::RejectedBeforeDispatch(failure)
        | McpTransportFailure::Permanent(failure) => McpDiscoveryOutcome::PermanentFailure(failure),
        McpTransportFailure::RetryableBeforeDispatch(failure)
        | McpTransportFailure::PostDispatchUncertain { failure, .. } => {
            McpDiscoveryOutcome::RetryableFailure(failure)
        }
        McpTransportFailure::ReauthorizationRequired { challenge_digest } => {
            McpDiscoveryOutcome::ReauthorizationRequired { challenge_digest }
        }
    })
}

fn retryable_discovery_failure(domain: &str) -> McpDiscoveryOutcome {
    McpDiscoveryOutcome::RetryableFailure(SafeMcpFailure {
        safe_code: domain.to_owned(),
        safe_message: "MCP discovery completion could not be observed".to_owned(),
        evidence_digest: static_digest(domain),
    })
}

fn discovery_transport_features_allowed(contract: &McpDiscoveryExecutionContract) -> bool {
    let features = &contract.protocol_profile.transport_features;
    match contract.server.transport {
        McpTransportKind::StreamableHttp => {
            features.streamable_http_get || features.streamable_http_sse
        }
    }
}

fn candidate_capabilities_allowed(
    candidate: &McpDiscoveryCandidate,
    contract: &McpDiscoveryExecutionContract,
) -> bool {
    let negotiated = &candidate.negotiated_capabilities;
    let server = &contract.protocol_profile.allowed_server_capabilities;
    let client = &contract.protocol_profile.client_capabilities;
    let tasks_enabled = contract
        .protocol_profile
        .experimental_features
        .contains(&McpExperimentalFeature::Tasks)
        && server.tasks;
    (!negotiated.tools || server.tools)
        && (!negotiated.resources || server.resources)
        && (!negotiated.prompts || server.prompts)
        && (!negotiated.logging || server.logging)
        && (!negotiated.tasks || tasks_enabled)
        && (!negotiated.tasks_list || tasks_enabled)
        && (!negotiated.tasks_cancel || tasks_enabled)
        && (!negotiated.tasks_tools_call || tasks_enabled)
        && (negotiated.tasks
            || !(negotiated.tasks_list || negotiated.tasks_cancel || negotiated.tasks_tools_call))
        && (!negotiated.subscriptions || server.subscriptions)
        && (!negotiated.elicitation || client.elicitation_form || client.elicitation_url)
        && (!negotiated.sampling || client.sampling)
        && (!negotiated.roots || client.roots)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewMcpDiscoverySnapshotRecord {
    pub tenant_id: ResourceId,
    pub source_operation_id: ResourceId,
    pub artifact_link_id: ResourceId,
    pub snapshot: McpDiscoverySnapshot,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewMcpDiscoveryAdmission {
    pub operation_id: ResourceId,
    pub job_id: ResourceId,
    pub tenant_id: ResourceId,
    pub mcp_deployment: ExactDeploymentRef,
    pub server_revision: ExactVersionRef,
    pub protocol_profile: ExactVersionRef,
    pub authorization_binding_id: ResourceId,
    pub authorization_generation: u64,
    pub authorization_context_digest: Sha256Digest,
    pub principal_id: ResourceId,
    pub requested_at: DateTime<Utc>,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct CreateMcpDiscoveryOperation {
    pub audit: CommandAudit,
    pub operation_id: ResourceId,
    pub job_id: ResourceId,
    pub logical_key: String,
    pub mcp_deployment: ExactDeploymentRef,
    pub authorization_binding_id: ResourceId,
    pub attempt_limit: u16,
    pub deadline: DateTime<Utc>,
}

impl CreateMcpDiscoveryOperation {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.audit
            .validate_at(now)
            .map_err(|_| McpHostError::InvalidDiscovery)?;
        if self.operation_id.kind() != ResourceKind::McpOperation
            || self.job_id.kind() != ResourceKind::Job
            || self.logical_key.is_empty()
            || self.logical_key.len() > 255
            || self.logical_key.chars().any(char::is_control)
            || self.mcp_deployment.resource_kind != ResourceKind::McpDeployment
            || self.mcp_deployment.validate().is_err()
            || self.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
            || self.attempt_limit == 0
            || self.attempt_limit > 8
            || self.deadline <= now
        {
            return Err(McpHostError::InvalidDiscovery);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpWorkerAudit {
    pub tenant_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub receipt_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
}

impl McpWorkerAudit {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.receipt_id.kind() != ResourceKind::Receipt
            || self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
            || self.receipt_expires_at <= now
        {
            return Err(McpHostError::InvalidDiscovery);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CommitMcpDiscovery {
    pub audit: McpWorkerAudit,
    pub operation_id: ResourceId,
    pub job_id: ResourceId,
    pub fence: JobFence,
    pub expected_operation_version: u64,
    pub artifact_link_id: ResourceId,
    pub snapshot: McpDiscoverySnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum McpDiscoveryAttemptResolution {
    Retry {
        retry_at: DateTime<Utc>,
        failure: SafeMcpFailure,
    },
    Failed {
        failure: SafeMcpFailure,
    },
    ReauthorizationRequired {
        challenge_digest: Sha256Digest,
    },
    Cancelled {
        failure: SafeMcpFailure,
    },
}

impl McpDiscoveryAttemptResolution {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        match self {
            Self::Retry { retry_at, failure } => {
                failure.validate()?;
                if *retry_at <= now {
                    return Err(McpHostError::InvalidDiscovery);
                }
            }
            Self::Failed { failure } | Self::Cancelled { failure } => failure.validate()?,
            Self::ReauthorizationRequired { .. } => {}
        }
        Ok(())
    }

    pub const fn operation_state(&self) -> McpDiscoveryOperationState {
        match self {
            Self::Retry { .. } => McpDiscoveryOperationState::Pending,
            Self::Failed { .. } | Self::ReauthorizationRequired { .. } => {
                McpDiscoveryOperationState::Failed
            }
            Self::Cancelled { .. } => McpDiscoveryOperationState::Cancelled,
        }
    }

    pub const fn job_state(&self) -> JobState {
        match self {
            Self::Retry { .. } => JobState::RetryScheduled,
            Self::Failed { .. } | Self::ReauthorizationRequired { .. } => JobState::Failed,
            Self::Cancelled { .. } => JobState::Cancelled,
        }
    }

    pub const fn event_type(&self) -> &'static str {
        match self {
            Self::Retry { .. } => "mcp.discovery_retry_scheduled",
            Self::Failed { .. } => "mcp.discovery_failed",
            Self::ReauthorizationRequired { .. } => "mcp.discovery_reauthorization_required",
            Self::Cancelled { .. } => "mcp.discovery_cancelled",
        }
    }

    pub const fn retry_at(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::Retry { retry_at, .. } => Some(*retry_at),
            Self::Failed { .. } | Self::ReauthorizationRequired { .. } | Self::Cancelled { .. } => {
                None
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolveMcpDiscoveryAttempt {
    pub audit: McpWorkerAudit,
    pub operation_id: ResourceId,
    pub job_id: ResourceId,
    pub fence: JobFence,
    pub expected_operation_version: u64,
    pub resolution: McpDiscoveryAttemptResolution,
}

#[derive(Debug, Clone)]
pub struct CancelMcpDiscoveryOperation {
    pub audit: CommandAudit,
    pub operation_id: ResourceId,
    pub expected_operation_version: u64,
    pub reason_code: String,
}

impl CancelMcpDiscoveryOperation {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.audit
            .validate_at(now)
            .map_err(|_| McpHostError::InvalidDiscovery)?;
        if self.operation_id.kind() != ResourceKind::McpOperation
            || self.expected_operation_version == 0
            || !valid_code(&self.reason_code)
        {
            return Err(McpHostError::InvalidDiscovery);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RecoverExpiredMcpDiscoveryJob {
    pub tenant_id: ResourceId,
    pub operation_id: ResourceId,
    pub job_id: ResourceId,
    pub observed_operation_version: u64,
    pub observed_job_version: u64,
    pub observed_lease_generation: u64,
    pub retry_at: Option<DateTime<Utc>>,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpiredMcpDiscoveryJobObservation {
    pub tenant_id: ResourceId,
    pub operation_id: ResourceId,
    pub job_id: ResourceId,
    pub operation_version: u64,
    pub job_version: u64,
    pub lease_generation: u64,
    pub physical_attempt: u32,
    pub attempt_limit: u32,
    pub job_state: JobState,
    pub lease_expires_at: DateTime<Utc>,
    pub deadline: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
}

impl ExpiredMcpDiscoveryJobObservation {
    pub fn validate(&self) -> Result<(), McpHostError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.operation_id.kind() != ResourceKind::McpOperation
            || self.job_id.kind() != ResourceKind::Job
            || self.operation_version == 0
            || self.job_version == 0
            || self.lease_generation == 0
            || self.attempt_limit == 0
            || self.physical_attempt > self.attempt_limit
            || (self.job_state == JobState::Leased && self.physical_attempt != 0)
            || (self.job_state == JobState::Running && self.physical_attempt == 0)
            || !matches!(self.job_state, JobState::Leased | JobState::Running)
            || self.lease_expires_at > self.deadline
            || self.lease_expires_at > self.observed_at
        {
            return Err(McpHostError::InvalidDiscovery);
        }
        Ok(())
    }
}

impl RecoverExpiredMcpDiscoveryJob {
    pub fn validate(&self) -> Result<(), McpHostError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.operation_id.kind() != ResourceKind::McpOperation
            || self.job_id.kind() != ResourceKind::Job
            || self.observed_operation_version == 0
            || self.observed_job_version == 0
            || self.observed_lease_generation == 0
            || self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
        {
            return Err(McpHostError::InvalidDiscovery);
        }
        Ok(())
    }
}

impl ResolveMcpDiscoveryAttempt {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.audit.validate_at(now)?;
        self.resolution.validate_at(now)?;
        if self.operation_id.kind() != ResourceKind::McpOperation
            || self.job_id.kind() != ResourceKind::Job
            || self.expected_operation_version == 0
            || self.fence.expected_version == 0
            || self.fence.lease_generation == 0
            || self.fence.worker_process_generation_id != self.audit.worker_process_generation_id
        {
            return Err(McpHostError::InvalidDiscovery);
        }
        Ok(())
    }
}

impl CommitMcpDiscovery {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.audit.validate_at(now)?;
        if self.operation_id.kind() != ResourceKind::McpOperation
            || self.job_id.kind() != ResourceKind::Job
            || self.expected_operation_version == 0
            || self.artifact_link_id.kind() != ResourceKind::ArtifactLink
            || self.snapshot.validate().is_err()
            || self.snapshot.expires_at <= now
            || self.fence.expected_version == 0
            || self.fence.lease_generation == 0
            || self.fence.worker_process_generation_id != self.audit.worker_process_generation_id
        {
            return Err(McpHostError::InvalidDiscovery);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDiscoveryAdmission {
    pub schema_version: u32,
    pub operation_id: ResourceId,
    pub job_id: ResourceId,
    pub tenant_id: ResourceId,
    pub mcp_deployment: ExactDeploymentRef,
    pub server_revision: ExactVersionRef,
    pub protocol_profile: ExactVersionRef,
    pub authorization_binding_id: ResourceId,
    pub authorization_generation: u64,
    pub authorization_context_digest: Sha256Digest,
    pub principal_id: ResourceId,
    pub requested_at: DateTime<Utc>,
    pub deadline: DateTime<Utc>,
    pub canonical_digest: Sha256Digest,
}

impl McpDiscoveryAdmission {
    pub fn build(input: NewMcpDiscoveryAdmission) -> Result<Self, McpHostError> {
        let mut admission = Self {
            schema_version: 1,
            operation_id: input.operation_id,
            job_id: input.job_id,
            tenant_id: input.tenant_id,
            mcp_deployment: input.mcp_deployment,
            server_revision: input.server_revision,
            protocol_profile: input.protocol_profile,
            authorization_binding_id: input.authorization_binding_id,
            authorization_generation: input.authorization_generation,
            authorization_context_digest: input.authorization_context_digest,
            principal_id: input.principal_id,
            requested_at: input.requested_at,
            deadline: input.deadline,
            canonical_digest: placeholder_digest()?,
        };
        admission.validate_shape()?;
        admission.canonical_digest = digest_without_field(&admission, "canonical_digest")?;
        Ok(admission)
    }

    pub fn validate(&self) -> Result<(), McpHostError> {
        self.validate_shape()?;
        if digest_without_field(self, "canonical_digest")? != self.canonical_digest {
            return Err(McpHostError::InvalidDiscovery);
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), McpHostError> {
        if self.schema_version != 1
            || self.operation_id.kind() != ResourceKind::McpOperation
            || self.job_id.kind() != ResourceKind::Job
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.mcp_deployment.resource_kind != ResourceKind::McpDeployment
            || self.mcp_deployment.validate().is_err()
            || self.server_revision.resource_kind != ResourceKind::McpServerRevision
            || self.server_revision.validate().is_err()
            || self.protocol_profile.resource_kind != ResourceKind::PolicyRevision
            || self.protocol_profile.validate().is_err()
            || self.authorization_binding_id.kind() != ResourceKind::McpAuthorizationBinding
            || self.authorization_generation == 0
            || self.principal_id.kind() != ResourceKind::Principal
            || self.deadline <= self.requested_at
        {
            return Err(McpHostError::InvalidDiscovery);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDiscoveryResultBinding {
    pub snapshot_id: ResourceId,
    pub snapshot_digest: Sha256Digest,
    pub objects_artifact: ArtifactRef,
    pub artifact_link_id: ResourceId,
}

impl McpDiscoveryResultBinding {
    pub fn validate(&self) -> Result<(), McpHostError> {
        if self.snapshot_id.kind() != ResourceKind::McpDiscoverySnapshot
            || self.objects_artifact.validate().is_err()
            || self.artifact_link_id.kind() != ResourceKind::ArtifactLink
        {
            return Err(McpHostError::InvalidDiscovery);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDiscoveryOperationPayload {
    pub schema_version: u32,
    pub admission: McpDiscoveryAdmission,
    pub result: Option<McpDiscoveryResultBinding>,
    pub canonical_digest: Sha256Digest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpDiscoveryOperationState {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

impl McpDiscoveryOperationState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut
        )
    }
}

impl std::str::FromStr for McpDiscoveryOperationState {
    type Err = McpHostError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "timed_out" => Ok(Self::TimedOut),
            _ => Err(McpHostError::InvalidDiscovery),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpDiscoveryOperationRecord {
    pub tenant_id: ResourceId,
    pub operation_id: ResourceId,
    pub job_id: ResourceId,
    pub logical_key: String,
    pub state: McpDiscoveryOperationState,
    pub version: u64,
    pub payload: McpDiscoveryOperationPayload,
    pub deadline: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub terminal_at: Option<DateTime<Utc>>,
}

impl McpDiscoveryOperationRecord {
    pub fn validate(&self) -> Result<(), McpHostError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.operation_id.kind() != ResourceKind::McpOperation
            || self.job_id.kind() != ResourceKind::Job
            || self.logical_key.is_empty()
            || self.logical_key.len() > 255
            || self.logical_key.chars().any(char::is_control)
            || self.version == 0
            || self.payload.admission.operation_id != self.operation_id
            || self.payload.admission.job_id != self.job_id
            || self.payload.admission.tenant_id != self.tenant_id
            || self.deadline != self.payload.admission.deadline
            || self.updated_at < self.created_at
            || self.state.is_terminal() != self.terminal_at.is_some()
            || self
                .terminal_at
                .is_some_and(|terminal_at| terminal_at < self.created_at)
            || (self.state == McpDiscoveryOperationState::Succeeded)
                != self.payload.result.is_some()
        {
            return Err(McpHostError::InvalidDiscovery);
        }
        self.payload.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDiscoveryJobPayload {
    pub schema_version: u32,
    pub operation_id: ResourceId,
    pub admission_digest: Sha256Digest,
    pub canonical_digest: Sha256Digest,
}

/// Closed payload registry for the shared MCP work-class queue.
///
/// New MCP durable work extends this tagged union rather than weakening claim validation or
/// creating a per-operation Job table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum McpJobPayload {
    Discovery(McpDiscoveryJobPayload),
    Subscription(super::McpSubscriptionJobPayload),
}

impl McpJobPayload {
    pub fn validate_for_owner(&self, owner_id: &ResourceId) -> Result<(), McpHostError> {
        match self {
            Self::Discovery(payload) => {
                payload.validate()?;
                if payload.operation_id != *owner_id {
                    return Err(McpHostError::InvalidDiscovery);
                }
                Ok(())
            }
            Self::Subscription(payload) => payload.validate_for(owner_id),
        }
    }
}

impl McpDiscoveryJobPayload {
    pub fn build(admission: &McpDiscoveryAdmission) -> Result<Self, McpHostError> {
        admission.validate()?;
        let mut payload = Self {
            schema_version: 1,
            operation_id: admission.operation_id.clone(),
            admission_digest: admission.canonical_digest.clone(),
            canonical_digest: placeholder_digest()?,
        };
        payload.canonical_digest = digest_without_field(&payload, "canonical_digest")?;
        payload.validate()?;
        Ok(payload)
    }

    pub fn validate(&self) -> Result<(), McpHostError> {
        if self.schema_version != 1
            || self.operation_id.kind() != ResourceKind::McpOperation
            || digest_without_field(self, "canonical_digest")? != self.canonical_digest
        {
            return Err(McpHostError::InvalidDiscovery);
        }
        Ok(())
    }
}

impl McpDiscoveryOperationPayload {
    pub fn pending(admission: McpDiscoveryAdmission) -> Result<Self, McpHostError> {
        let mut payload = Self {
            schema_version: 1,
            admission,
            result: None,
            canonical_digest: placeholder_digest()?,
        };
        payload.validate_shape()?;
        payload.canonical_digest = digest_without_field(&payload, "canonical_digest")?;
        Ok(payload)
    }

    pub fn complete(&self, result: McpDiscoveryResultBinding) -> Result<Self, McpHostError> {
        if self.result.is_some() {
            return Err(McpHostError::InvalidDiscovery);
        }
        let mut next = Self {
            schema_version: 1,
            admission: self.admission.clone(),
            result: Some(result),
            canonical_digest: placeholder_digest()?,
        };
        next.validate_shape()?;
        next.canonical_digest = digest_without_field(&next, "canonical_digest")?;
        Ok(next)
    }

    pub fn validate(&self) -> Result<(), McpHostError> {
        self.validate_shape()?;
        if digest_without_field(self, "canonical_digest")? != self.canonical_digest {
            return Err(McpHostError::InvalidDiscovery);
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), McpHostError> {
        if self.schema_version != 1 {
            return Err(McpHostError::InvalidDiscovery);
        }
        self.admission.validate()?;
        if let Some(result) = &self.result {
            result.validate()?;
        }
        Ok(())
    }
}

/// Immutable discovery authority stored in the shared Resource aggregate.
///
/// The nested snapshot is the value frozen into published projections. The surrounding record
/// proves that it came from a durable MCP operation and that the raw bounded objects Artifact is
/// held by an exact ArtifactLink owned by that operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDiscoverySnapshotRecord {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub source_operation_id: ResourceId,
    pub artifact_link_id: ResourceId,
    pub snapshot: McpDiscoverySnapshot,
    pub completed_at: DateTime<Utc>,
    pub canonical_digest: Sha256Digest,
}

impl McpDiscoverySnapshotRecord {
    pub fn build(input: NewMcpDiscoverySnapshotRecord) -> Result<Self, McpHostError> {
        let mut record = Self {
            schema_version: 1,
            tenant_id: input.tenant_id,
            source_operation_id: input.source_operation_id,
            artifact_link_id: input.artifact_link_id,
            snapshot: input.snapshot,
            completed_at: input.completed_at,
            canonical_digest: placeholder_digest()?,
        };
        record.validate_shape()?;
        record.canonical_digest = digest_without_field(&record, "canonical_digest")?;
        Ok(record)
    }

    pub fn validate(&self) -> Result<(), McpHostError> {
        self.validate_shape()?;
        if digest_without_field(self, "canonical_digest")? != self.canonical_digest {
            return Err(McpHostError::InvalidDiscovery);
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), McpHostError> {
        if self.schema_version != 1
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.source_operation_id.kind() != ResourceKind::McpOperation
            || self.artifact_link_id.kind() != ResourceKind::ArtifactLink
            || self.snapshot.validate().is_err()
            || self.completed_at < self.snapshot.observed_at
            || self.completed_at >= self.snapshot.expires_at
        {
            return Err(McpHostError::InvalidDiscovery);
        }
        Ok(())
    }
}
