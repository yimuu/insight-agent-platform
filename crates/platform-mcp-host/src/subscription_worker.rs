use super::{
    digest, static_digest, CompleteMcpSubscriptionReconcile, CompleteMcpSubscriptionRefresh,
    DueMcpSubscriptionReconcile, DueMcpSubscriptionRecovery, EncryptedMcpState,
    McpExecutionContractResolutionError, McpHostError, McpHostExecutionContract,
    McpNotificationClass, McpResourceSubscriptionBinding, McpSubscriptionReconcileScan,
    McpSubscriptionRecord, McpSubscriptionRecoveryScan, McpSubscriptionState,
    McpSubscriptionWorkerAudit, McpTransportFailure, RecoverDueMcpSubscription,
    ReportMcpSubscriptionSessionLoss, ReportMcpSubscriptionTransportTermination, SafeMcpFailure,
    SaveMcpSubscriptionSession, WakeMcpSubscriptionReconcile,
};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::FutureExt;
use insight_platform_contracts::{
    CommandOutcome, McpSessionState, McpTransportKind, ResourceId, ResourceKind, Sha256Digest,
};
use insight_platform_jobs::JobFence;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet, error::Error, fmt, panic::AssertUnwindSafe, sync::Arc, time::Duration,
};
use tokio::sync::Semaphore;

const MAX_SUBSCRIPTION_CLOCK_SKEW_SECONDS: i64 = 60;

/// Exact fenced lookup for one claimed shared MCP subscription Job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpSubscriptionContractQuery {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub subscription_id: ResourceId,
    pub job_id: ResourceId,
    pub fence: JobFence,
}

impl McpSubscriptionContractQuery {
    pub fn validate(&self) -> Result<(), McpExecutionContractResolutionError> {
        if self.schema_version != 1
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.subscription_id.kind() != ResourceKind::McpOperation
            || self.job_id.kind() != ResourceKind::Job
            || self.fence.expected_version == 0
            || self.fence.lease_generation == 0
            || self.fence.worker_process_generation_id.kind()
                != ResourceKind::WorkerProcessGeneration
        {
            return Err(McpExecutionContractResolutionError::InvalidQuery);
        }
        Ok(())
    }
}

/// Reconstructed durable subscription and immutable MCP execution closure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedMcpSubscriptionExecution {
    pub record: McpSubscriptionRecord,
    pub contract: McpHostExecutionContract,
}

impl ResolvedMcpSubscriptionExecution {
    pub fn validate_for(
        &self,
        query: &McpSubscriptionContractQuery,
        now: DateTime<Utc>,
    ) -> Result<(), McpExecutionContractResolutionError> {
        query.validate()?;
        if self.record.tenant_id != query.tenant_id
            || self.record.subscription_id != query.subscription_id
            || self.record.job_id != query.job_id
            || self.record.deadline <= now
            || self.record.state.is_terminal()
            || self.record.validate_at(now).is_err()
            || self.contract.validate_canonical_at(now).is_err()
            || self
                .record
                .payload
                .binding
                .validate_for_execution_contract_at(&self.contract, now)
                .is_err()
        {
            return Err(McpExecutionContractResolutionError::NotFoundOrChanged);
        }
        Ok(())
    }
}

#[async_trait]
pub trait McpSubscriptionExecutionResolver: Send + Sync {
    async fn resolve_mcp_subscription_execution(
        &self,
        query: &McpSubscriptionContractQuery,
    ) -> Result<ResolvedMcpSubscriptionExecution, McpExecutionContractResolutionError>;
}

/// Credential-free transport result. The remote session handle is encrypted before it crosses
/// the connector boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EstablishedMcpSubscription {
    pub transport_kind: McpTransportKind,
    pub binding_digest: Sha256Digest,
    pub encrypted_opaque_session: EncryptedMcpState,
    pub established_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub evidence_digest: Sha256Digest,
}

/// Deferred activation for a prepared live subscription. The connector completes protocol
/// initialization and `resources/subscribe` before returning this handle, but MUST NOT begin
/// consuming the server-to-client stream until the Host has durably committed the matching
/// session generation as Ready.
#[async_trait]
pub trait McpSubscriptionActivation: Send {
    async fn activate(self: Box<Self>);
}

/// A protocol subscription whose remote session is prepared but whose notification stream has not
/// yet been activated. Keeping activation separate closes the race where a notification could
/// arrive before the durable session generation exists.
pub struct PreparedMcpSubscription {
    pub established: EstablishedMcpSubscription,
    activation: Box<dyn McpSubscriptionActivation>,
}

impl PreparedMcpSubscription {
    pub fn new(
        established: EstablishedMcpSubscription,
        activation: Box<dyn McpSubscriptionActivation>,
    ) -> Self {
        Self {
            established,
            activation,
        }
    }

    pub async fn activate(self) {
        self.activation.activate().await;
    }

    /// Splits the durable evidence from its deferred live-stream activation. Internal brokers use
    /// this to carry the commit-before-activation protocol across a process boundary.
    pub fn into_parts(
        self,
    ) -> (
        EstablishedMcpSubscription,
        Box<dyn McpSubscriptionActivation>,
    ) {
        (self.established, self.activation)
    }
}

impl EstablishedMcpSubscription {
    pub fn validate_for(
        &self,
        binding: &McpResourceSubscriptionBinding,
        contract: &McpHostExecutionContract,
        now: DateTime<Utc>,
    ) -> Result<(), McpHostError> {
        self.encrypted_opaque_session.validate()?;
        let maximum = i64::try_from(contract.server.limits.maximum_session_milliseconds)
            .map_err(|_| McpHostError::InvalidSubscription)?;
        if self.transport_kind != binding.transport_kind
            || self.transport_kind != contract.transport_kind()
            || self.binding_digest != binding.canonical_digest
            || self.established_at
                > now + ChronoDuration::seconds(MAX_SUBSCRIPTION_CLOCK_SKEW_SECONDS)
            || self.expires_at <= now
            || self.expires_at > now + ChronoDuration::milliseconds(maximum)
        {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }
}

/// Transport port for connect + initialize + `resources/subscribe`.
///
/// Implementations own the live HTTP/SSE or managed-runner connection. They receive only the
/// exact immutable contract and return an encrypted reconstructable handle; they cannot mutate
/// durable subscription or Job state.
#[async_trait]
pub trait McpSubscriptionTransport: Send + Sync {
    fn kind(&self) -> McpTransportKind;

    async fn establish(
        &self,
        contract: &McpHostExecutionContract,
        binding: &McpResourceSubscriptionBinding,
        session_generation: u64,
        worker_process_generation_id: &ResourceId,
        deadline: DateTime<Utc>,
    ) -> Result<PreparedMcpSubscription, McpTransportFailure>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum McpSubscriptionRefreshReason {
    ResourceUpdated {
        resource_uri: String,
        resource_uri_digest: Sha256Digest,
    },
    ResourceListChanged,
    ToolListChanged,
    PromptListChanged,
}

/// Bounded request to the durable Context/Discovery scheduling boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpSubscriptionInvalidationRequest {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub subscription_id: ResourceId,
    pub context_deployment: insight_platform_contracts::ExactDeploymentRef,
    pub mcp_deployment: insight_platform_contracts::ExactDeploymentRef,
    pub discovery_snapshot_id: ResourceId,
    pub discovery_snapshot_digest: Sha256Digest,
    pub authorization_generation: u64,
    pub session_generation: u64,
    pub event_generation: u64,
    pub event_key_digest: Sha256Digest,
    pub body_digest: Sha256Digest,
    pub reason: McpSubscriptionRefreshReason,
    pub request_digest: Sha256Digest,
}

impl McpSubscriptionInvalidationRequest {
    pub fn build(record: &McpSubscriptionRecord) -> Result<Self, McpHostError> {
        let pending = record
            .payload
            .pending_invalidation
            .as_ref()
            .ok_or(McpHostError::InvalidSubscription)?;
        let binding = &record.payload.binding;
        let reason = match pending.class {
            McpNotificationClass::ResourceUpdated => {
                // MCP permits an update URI to identify a sub-resource of the subscribed root.
                // The untrusted notification URI is retained only as bounded digest evidence;
                // downstream work always re-reads the exact published binding root.
                McpSubscriptionRefreshReason::ResourceUpdated {
                    resource_uri: binding.resource_uri.clone(),
                    resource_uri_digest: binding.resource_uri_digest.clone(),
                }
            }
            McpNotificationClass::ResourceListChanged => {
                McpSubscriptionRefreshReason::ResourceListChanged
            }
            McpNotificationClass::ToolListChanged => McpSubscriptionRefreshReason::ToolListChanged,
            McpNotificationClass::PromptListChanged => {
                McpSubscriptionRefreshReason::PromptListChanged
            }
        };
        let mut request = Self {
            schema_version: 1,
            tenant_id: record.tenant_id.clone(),
            subscription_id: record.subscription_id.clone(),
            context_deployment: binding.context_deployment.clone(),
            mcp_deployment: binding.mcp_deployment.clone(),
            discovery_snapshot_id: binding.discovery_snapshot_id.clone(),
            discovery_snapshot_digest: binding.discovery_snapshot_digest.clone(),
            authorization_generation: binding.authorization_generation,
            session_generation: pending.session_generation,
            event_generation: pending.event_generation,
            event_key_digest: pending.event_key_digest.clone(),
            body_digest: pending.body_digest.clone(),
            reason,
            request_digest: static_digest("mcp_subscription_invalidation_placeholder"),
        };
        request.request_digest = request.canonical_request_digest()?;
        request.validate_for(record)?;
        Ok(request)
    }

    pub fn validate_for(&self, record: &McpSubscriptionRecord) -> Result<(), McpHostError> {
        let pending = record
            .payload
            .pending_invalidation
            .as_ref()
            .ok_or(McpHostError::InvalidSubscription)?;
        let binding = &record.payload.binding;
        let reason_matches = match (&self.reason, pending.class) {
            (
                McpSubscriptionRefreshReason::ResourceUpdated {
                    resource_uri,
                    resource_uri_digest,
                },
                McpNotificationClass::ResourceUpdated,
            ) => {
                resource_uri == &binding.resource_uri
                    && resource_uri_digest == &binding.resource_uri_digest
                    && pending.resource_uri_digest.as_ref() == Some(resource_uri_digest)
            }
            (
                McpSubscriptionRefreshReason::ResourceListChanged,
                McpNotificationClass::ResourceListChanged,
            )
            | (
                McpSubscriptionRefreshReason::ToolListChanged,
                McpNotificationClass::ToolListChanged,
            )
            | (
                McpSubscriptionRefreshReason::PromptListChanged,
                McpNotificationClass::PromptListChanged,
            ) => pending.resource_uri_digest.is_none(),
            _ => false,
        };
        if self.schema_version != 1
            || self.tenant_id != record.tenant_id
            || self.subscription_id != record.subscription_id
            || self.context_deployment != binding.context_deployment
            || self.mcp_deployment != binding.mcp_deployment
            || self.discovery_snapshot_id != binding.discovery_snapshot_id
            || self.discovery_snapshot_digest != binding.discovery_snapshot_digest
            || self.authorization_generation != binding.authorization_generation
            || self.session_generation != pending.session_generation
            || self.event_generation != pending.event_generation
            || self.event_key_digest != pending.event_key_digest
            || self.body_digest != pending.body_digest
            || !reason_matches
            || self.canonical_request_digest()? != self.request_digest
        {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }

    fn canonical_request_digest(&self) -> Result<Sha256Digest, McpHostError> {
        digest(&serde_json::json!({
            "authorization_generation": self.authorization_generation,
            "body_digest": self.body_digest,
            "context_deployment": self.context_deployment,
            "discovery_snapshot_digest": self.discovery_snapshot_digest,
            "discovery_snapshot_id": self.discovery_snapshot_id,
            "event_generation": self.event_generation,
            "event_key_digest": self.event_key_digest,
            "mcp_deployment": self.mcp_deployment,
            "reason": self.reason,
            "schema_version": self.schema_version,
            "session_generation": self.session_generation,
            "subscription_id": self.subscription_id,
            "tenant_id": self.tenant_id,
        }))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedMcpSubscriptionInvalidation {
    pub request_digest: Sha256Digest,
    pub durable_work_digest: Sha256Digest,
    pub accepted_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpSubscriptionReconcileRequest {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub subscription_id: ResourceId,
    pub context_deployment: insight_platform_contracts::ExactDeploymentRef,
    pub mcp_deployment: insight_platform_contracts::ExactDeploymentRef,
    pub discovery_snapshot_id: ResourceId,
    pub discovery_snapshot_digest: Sha256Digest,
    pub authorization_generation: u64,
    pub session_generation: u64,
    pub observed_subscription_version: u64,
    pub resource_uri: String,
    pub resource_uri_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
}

impl McpSubscriptionReconcileRequest {
    pub fn build(record: &McpSubscriptionRecord) -> Result<Self, McpHostError> {
        if record.payload.pending_invalidation.is_some()
            || record.state != McpSubscriptionState::Active
            || !matches!(
                record.payload.session.state,
                McpSessionState::Ready | McpSessionState::Degraded
            )
        {
            return Err(McpHostError::InvalidSubscription);
        }
        let binding = &record.payload.binding;
        let mut request = Self {
            schema_version: 1,
            tenant_id: record.tenant_id.clone(),
            subscription_id: record.subscription_id.clone(),
            context_deployment: binding.context_deployment.clone(),
            mcp_deployment: binding.mcp_deployment.clone(),
            discovery_snapshot_id: binding.discovery_snapshot_id.clone(),
            discovery_snapshot_digest: binding.discovery_snapshot_digest.clone(),
            authorization_generation: binding.authorization_generation,
            session_generation: record.payload.session.generation,
            observed_subscription_version: record.version,
            resource_uri: binding.resource_uri.clone(),
            resource_uri_digest: binding.resource_uri_digest.clone(),
            request_digest: static_digest("mcp_subscription_reconcile_placeholder"),
        };
        request.request_digest = request.canonical_request_digest()?;
        Ok(request)
    }

    pub fn validate_for(&self, record: &McpSubscriptionRecord) -> Result<(), McpHostError> {
        let binding = &record.payload.binding;
        if self.schema_version != 1
            || self.tenant_id != record.tenant_id
            || self.subscription_id != record.subscription_id
            || self.context_deployment != binding.context_deployment
            || self.mcp_deployment != binding.mcp_deployment
            || self.discovery_snapshot_id != binding.discovery_snapshot_id
            || self.discovery_snapshot_digest != binding.discovery_snapshot_digest
            || self.authorization_generation != binding.authorization_generation
            || self.session_generation != record.payload.session.generation
            || self.observed_subscription_version != record.version
            || self.resource_uri != binding.resource_uri
            || self.resource_uri_digest != binding.resource_uri_digest
            || record.payload.pending_invalidation.is_some()
            || self.canonical_request_digest()? != self.request_digest
        {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }

    fn canonical_request_digest(&self) -> Result<Sha256Digest, McpHostError> {
        digest(&serde_json::json!({
            "authorization_generation": self.authorization_generation,
            "context_deployment": self.context_deployment,
            "discovery_snapshot_digest": self.discovery_snapshot_digest,
            "discovery_snapshot_id": self.discovery_snapshot_id,
            "mcp_deployment": self.mcp_deployment,
            "observed_subscription_version": self.observed_subscription_version,
            "resource_uri": self.resource_uri,
            "resource_uri_digest": self.resource_uri_digest,
            "schema_version": self.schema_version,
            "session_generation": self.session_generation,
            "subscription_id": self.subscription_id,
            "tenant_id": self.tenant_id,
        }))
    }
}

impl AcceptedMcpSubscriptionInvalidation {
    pub fn validate_for(
        &self,
        request: &McpSubscriptionInvalidationRequest,
        now: DateTime<Utc>,
    ) -> Result<(), McpHostError> {
        if self.request_digest != request.request_digest
            || self.accepted_at > now + ChronoDuration::seconds(MAX_SUBSCRIPTION_CLOCK_SKEW_SECONDS)
        {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpSubscriptionInvalidationError {
    Rejected,
    Unavailable,
    CommitUncertain,
}

impl fmt::Display for McpSubscriptionInvalidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Rejected => "MCP subscription invalidation was rejected",
            Self::Unavailable => "MCP subscription invalidation target is unavailable",
            Self::CommitUncertain => "MCP subscription invalidation acceptance is uncertain",
        })
    }
}

impl Error for McpSubscriptionInvalidationError {}

#[async_trait]
pub trait McpSubscriptionInvalidationTarget: Send + Sync {
    async fn accept_invalidation(
        &self,
        request: McpSubscriptionInvalidationRequest,
    ) -> Result<AcceptedMcpSubscriptionInvalidation, McpSubscriptionInvalidationError>;

    async fn accept_reconcile(
        &self,
        request: McpSubscriptionReconcileRequest,
    ) -> Result<AcceptedMcpSubscriptionInvalidation, McpSubscriptionInvalidationError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpSubscriptionPersistenceError {
    InvalidCommand,
    Conflict,
    AuthorityUnavailable,
    CommitUncertain,
}

impl fmt::Display for McpSubscriptionPersistenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidCommand => "MCP subscription persistence command is invalid",
            Self::Conflict => "MCP subscription persistence first-winner was lost",
            Self::AuthorityUnavailable => "MCP subscription persistence authority is unavailable",
            Self::CommitUncertain => "MCP subscription persistence commit is uncertain",
        })
    }
}

impl Error for McpSubscriptionPersistenceError {}

#[async_trait]
pub trait McpSubscriptionAuthority: Send + Sync {
    async fn save_subscription_session(
        &self,
        command: SaveMcpSubscriptionSession,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, McpSubscriptionPersistenceError>;

    async fn complete_subscription_refresh(
        &self,
        command: CompleteMcpSubscriptionRefresh,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, McpSubscriptionPersistenceError>;

    async fn complete_subscription_reconcile(
        &self,
        command: CompleteMcpSubscriptionReconcile,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, McpSubscriptionPersistenceError>;
}

#[async_trait]
pub trait McpSubscriptionReconcileAuthority: Send + Sync {
    async fn list_due_reconciliations(
        &self,
        scan: McpSubscriptionReconcileScan,
    ) -> Result<Vec<DueMcpSubscriptionReconcile>, McpSubscriptionPersistenceError>;

    async fn wake_reconciliation(
        &self,
        command: WakeMcpSubscriptionReconcile,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, McpSubscriptionPersistenceError>;
}

#[async_trait]
pub trait McpSubscriptionRecoveryAuthority: Send + Sync {
    async fn list_due_recoveries(
        &self,
        scan: McpSubscriptionRecoveryScan,
    ) -> Result<Vec<DueMcpSubscriptionRecovery>, McpSubscriptionPersistenceError>;

    async fn recover_due_subscription(
        &self,
        command: RecoverDueMcpSubscription,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, McpSubscriptionPersistenceError>;

    async fn report_session_loss(
        &self,
        command: ReportMcpSubscriptionSessionLoss,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, McpSubscriptionPersistenceError>;
}

#[async_trait]
pub trait McpSubscriptionTransportTerminationAuthority: Send + Sync {
    async fn report_transport_termination(
        &self,
        command: ReportMcpSubscriptionTransportTermination,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, McpSubscriptionPersistenceError>;
}

#[derive(Debug, Clone)]
pub struct DriveMcpSubscriptionRecoveries {
    pub scan: McpSubscriptionRecoveryScan,
    pub audits: Vec<McpSubscriptionWorkerAudit>,
}

impl DriveMcpSubscriptionRecoveries {
    fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.scan.validate()?;
        if self.audits.len() != usize::from(self.scan.limit) {
            return Err(McpHostError::InvalidSubscription);
        }
        let mut identities = BTreeSet::new();
        for audit in &self.audits {
            audit.validate_at(now)?;
            if audit.tenant_id != self.scan.tenant_id
                || ![&audit.receipt_id, &audit.event_id, &audit.outbox_id]
                    .into_iter()
                    .all(|identity| identities.insert(identity.to_string()))
            {
                return Err(McpHostError::InvalidSubscription);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpSubscriptionRecoveryDriveOutcome {
    pub observed: u16,
    pub recovered: u16,
    pub stale: u16,
}

pub struct McpSubscriptionRecoveryDriver {
    authority: Arc<dyn McpSubscriptionRecoveryAuthority>,
    permits: Arc<Semaphore>,
}

impl McpSubscriptionRecoveryDriver {
    pub fn new(
        authority: Arc<dyn McpSubscriptionRecoveryAuthority>,
        maximum_concurrent_scans: usize,
    ) -> Result<Self, McpHostError> {
        if maximum_concurrent_scans == 0 || maximum_concurrent_scans > 64 {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(Self {
            authority,
            permits: Arc::new(Semaphore::new(maximum_concurrent_scans)),
        })
    }

    pub async fn drive(
        &self,
        command: DriveMcpSubscriptionRecoveries,
    ) -> Result<McpSubscriptionRecoveryDriveOutcome, McpSubscriptionReconcileDriverError> {
        command
            .validate_at(Utc::now())
            .map_err(|_| McpSubscriptionReconcileDriverError::InvalidCommand)?;
        let _permit = Arc::clone(&self.permits)
            .try_acquire_owned()
            .map_err(|_| McpSubscriptionReconcileDriverError::Saturated)?;
        let tenant_id = command.scan.tenant_id.clone();
        let candidates = self
            .authority
            .list_due_recoveries(command.scan)
            .await
            .map_err(McpSubscriptionReconcileDriverError::Persistence)?;
        if candidates.len() > command.audits.len() {
            return Err(McpSubscriptionReconcileDriverError::InvalidCommand);
        }
        let mut observed_subscriptions = BTreeSet::new();
        for candidate in &candidates {
            candidate
                .validate()
                .map_err(|_| McpSubscriptionReconcileDriverError::InvalidCommand)?;
            if candidate.tenant_id != tenant_id
                || !observed_subscriptions
                    .insert((candidate.subscription_id.clone(), candidate.job_id.clone()))
            {
                return Err(McpSubscriptionReconcileDriverError::InvalidCommand);
            }
        }
        let observed = u16::try_from(candidates.len())
            .map_err(|_| McpSubscriptionReconcileDriverError::InvalidCommand)?;
        let mut recovered = 0_u16;
        let mut stale = 0_u16;
        for (candidate, audit) in candidates.into_iter().zip(command.audits) {
            let mut recovery = RecoverDueMcpSubscription { audit, candidate };
            recovery.audit.request_digest = recovery
                .request_digest()
                .map_err(|_| McpSubscriptionReconcileDriverError::InvalidCommand)?;
            match self.authority.recover_due_subscription(recovery).await {
                Ok(_) => recovered = recovered.saturating_add(1),
                Err(McpSubscriptionPersistenceError::Conflict) => {
                    stale = stale.saturating_add(1);
                }
                Err(failure) => {
                    return Err(McpSubscriptionReconcileDriverError::Persistence(failure));
                }
            }
        }
        Ok(McpSubscriptionRecoveryDriveOutcome {
            observed,
            recovered,
            stale,
        })
    }
}

#[derive(Debug, Clone)]
pub struct DriveMcpSubscriptionReconciliations {
    pub scan: McpSubscriptionReconcileScan,
    pub audits: Vec<McpSubscriptionWorkerAudit>,
}

impl DriveMcpSubscriptionReconciliations {
    fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.scan.validate()?;
        if self.audits.len() != usize::from(self.scan.limit) {
            return Err(McpHostError::InvalidSubscription);
        }
        let mut identities = BTreeSet::new();
        for audit in &self.audits {
            audit.validate_at(now)?;
            if audit.tenant_id != self.scan.tenant_id
                || ![&audit.receipt_id, &audit.event_id, &audit.outbox_id]
                    .into_iter()
                    .all(|identity| identities.insert(identity.to_string()))
            {
                return Err(McpHostError::InvalidSubscription);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpSubscriptionReconcileDriveOutcome {
    pub observed: u16,
    pub scheduled: u16,
    pub stale: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpSubscriptionReconcileDriverError {
    InvalidCommand,
    Saturated,
    Persistence(McpSubscriptionPersistenceError),
}

impl fmt::Display for McpSubscriptionReconcileDriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCommand => {
                formatter.write_str("MCP subscription reconcile drive is invalid")
            }
            Self::Saturated => {
                formatter.write_str("MCP subscription reconcile control permit is saturated")
            }
            Self::Persistence(failure) => write!(formatter, "{failure}"),
        }
    }
}

impl Error for McpSubscriptionReconcileDriverError {}

/// Bounded critical-control driver. It owns a dedicated local permit and issues one durable wake
/// command per observed candidate; notification and request saturation cannot expand the scan.
pub struct McpSubscriptionReconcileDriver {
    authority: Arc<dyn McpSubscriptionReconcileAuthority>,
    permits: Arc<Semaphore>,
}

impl McpSubscriptionReconcileDriver {
    pub fn new(
        authority: Arc<dyn McpSubscriptionReconcileAuthority>,
        maximum_concurrent_scans: usize,
    ) -> Result<Self, McpHostError> {
        if maximum_concurrent_scans == 0 || maximum_concurrent_scans > 64 {
            return Err(McpHostError::InvalidSubscription);
        }
        Ok(Self {
            authority,
            permits: Arc::new(Semaphore::new(maximum_concurrent_scans)),
        })
    }

    pub async fn drive(
        &self,
        command: DriveMcpSubscriptionReconciliations,
    ) -> Result<McpSubscriptionReconcileDriveOutcome, McpSubscriptionReconcileDriverError> {
        command
            .validate_at(Utc::now())
            .map_err(|_| McpSubscriptionReconcileDriverError::InvalidCommand)?;
        let _permit = Arc::clone(&self.permits)
            .try_acquire_owned()
            .map_err(|_| McpSubscriptionReconcileDriverError::Saturated)?;
        let tenant_id = command.scan.tenant_id.clone();
        let candidates = self
            .authority
            .list_due_reconciliations(command.scan)
            .await
            .map_err(McpSubscriptionReconcileDriverError::Persistence)?;
        if candidates.len() > command.audits.len() {
            return Err(McpSubscriptionReconcileDriverError::InvalidCommand);
        }
        let mut observed_subscriptions = BTreeSet::new();
        for candidate in &candidates {
            candidate
                .validate()
                .map_err(|_| McpSubscriptionReconcileDriverError::InvalidCommand)?;
            if candidate.tenant_id != tenant_id
                || !observed_subscriptions
                    .insert((candidate.subscription_id.clone(), candidate.job_id.clone()))
            {
                return Err(McpSubscriptionReconcileDriverError::InvalidCommand);
            }
        }
        let observed = u16::try_from(candidates.len())
            .map_err(|_| McpSubscriptionReconcileDriverError::InvalidCommand)?;
        let mut scheduled = 0_u16;
        let mut stale = 0_u16;
        for (candidate, audit) in candidates.into_iter().zip(command.audits) {
            let mut wake = WakeMcpSubscriptionReconcile { audit, candidate };
            wake.audit.request_digest = wake
                .request_digest()
                .map_err(|_| McpSubscriptionReconcileDriverError::InvalidCommand)?;
            match self.authority.wake_reconciliation(wake).await {
                Ok(_) => scheduled = scheduled.saturating_add(1),
                Err(McpSubscriptionPersistenceError::Conflict) => {
                    stale = stale.saturating_add(1);
                }
                Err(failure) => {
                    return Err(McpSubscriptionReconcileDriverError::Persistence(failure));
                }
            }
        }
        Ok(McpSubscriptionReconcileDriveOutcome {
            observed,
            scheduled,
            stale,
        })
    }
}

#[derive(Debug, Clone)]
pub struct McpSubscriptionWorkerAudits {
    pub connecting: McpSubscriptionWorkerAudit,
    pub initializing: McpSubscriptionWorkerAudit,
    pub ready: McpSubscriptionWorkerAudit,
    pub terminal: McpSubscriptionWorkerAudit,
    pub refresh: McpSubscriptionWorkerAudit,
}

impl McpSubscriptionWorkerAudits {
    fn validate_at(
        &self,
        tenant_id: &ResourceId,
        worker_id: &ResourceId,
        now: DateTime<Utc>,
    ) -> Result<(), McpHostError> {
        let audits = [
            &self.connecting,
            &self.initializing,
            &self.ready,
            &self.terminal,
            &self.refresh,
        ];
        let mut identities = BTreeSet::new();
        for audit in audits {
            audit.validate_at(now)?;
            if &audit.tenant_id != tenant_id
                || &audit.worker_process_generation_id != worker_id
                || ![&audit.receipt_id, &audit.event_id, &audit.outbox_id]
                    .into_iter()
                    .all(|identity| identities.insert(identity.to_string()))
            {
                return Err(McpHostError::InvalidSubscription);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ExecuteMcpSubscriptionJob {
    pub query: McpSubscriptionContractQuery,
    pub audits: McpSubscriptionWorkerAudits,
}

impl ExecuteMcpSubscriptionJob {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
        self.query
            .validate()
            .map_err(|_| McpHostError::InvalidSubscription)?;
        self.audits.validate_at(
            &self.query.tenant_id,
            &self.query.fence.worker_process_generation_id,
            now,
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum McpSubscriptionWorkerResult {
    Established(CommandOutcome<McpSubscriptionRecord>),
    RefreshAccepted(CommandOutcome<McpSubscriptionRecord>),
    Reconciled(CommandOutcome<McpSubscriptionRecord>),
    Terminalized(CommandOutcome<McpSubscriptionRecord>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpSubscriptionWorkerError {
    InvalidCommand,
    Contract(McpExecutionContractResolutionError),
    Transport(McpTransportFailure),
    Invalidation(McpSubscriptionInvalidationError),
    Persistence(McpSubscriptionPersistenceError),
}

impl fmt::Display for McpSubscriptionWorkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCommand => {
                formatter.write_str("MCP subscription worker command is invalid")
            }
            Self::Contract(failure) => write!(formatter, "{failure}"),
            Self::Transport(_) => formatter.write_str("MCP subscription transport failed"),
            Self::Invalidation(failure) => write!(formatter, "{failure}"),
            Self::Persistence(failure) => write!(formatter, "{failure}"),
        }
    }
}

impl Error for McpSubscriptionWorkerError {}

pub struct McpSubscriptionWorker {
    resolver: Arc<dyn McpSubscriptionExecutionResolver>,
    transport: Arc<dyn McpSubscriptionTransport>,
    invalidation_target: Arc<dyn McpSubscriptionInvalidationTarget>,
    authority: Arc<dyn McpSubscriptionAuthority>,
}

impl McpSubscriptionWorker {
    pub fn new(
        resolver: Arc<dyn McpSubscriptionExecutionResolver>,
        transport: Arc<dyn McpSubscriptionTransport>,
        invalidation_target: Arc<dyn McpSubscriptionInvalidationTarget>,
        authority: Arc<dyn McpSubscriptionAuthority>,
    ) -> Self {
        Self {
            resolver,
            transport,
            invalidation_target,
            authority,
        }
    }

    pub async fn execute(
        &self,
        command: ExecuteMcpSubscriptionJob,
    ) -> Result<McpSubscriptionWorkerResult, McpSubscriptionWorkerError> {
        let started_at = Utc::now();
        command
            .validate_at(started_at)
            .map_err(|_| McpSubscriptionWorkerError::InvalidCommand)?;
        let resolved = self
            .resolver
            .resolve_mcp_subscription_execution(&command.query)
            .await
            .map_err(McpSubscriptionWorkerError::Contract)?;
        resolved
            .validate_for(&command.query, Utc::now())
            .map_err(McpSubscriptionWorkerError::Contract)?;
        if self.transport.kind() != resolved.contract.transport_kind() {
            return Err(McpSubscriptionWorkerError::InvalidCommand);
        }

        if resolved.record.payload.pending_invalidation.is_some() {
            return self.refresh(command, resolved).await;
        }
        if resolved.record.state == McpSubscriptionState::Active
            && matches!(
                resolved.record.payload.session.state,
                McpSessionState::Ready | McpSessionState::Degraded
            )
        {
            return self.reconcile(command, resolved).await;
        }
        self.establish(command, resolved).await
    }

    async fn establish(
        &self,
        command: ExecuteMcpSubscriptionJob,
        resolved: ResolvedMcpSubscriptionExecution,
    ) -> Result<McpSubscriptionWorkerResult, McpSubscriptionWorkerError> {
        let mut record = resolved.record;
        let mut fence = command.query.fence;
        match record.payload.session.state {
            McpSessionState::Disconnected => {
                let outcome = self
                    .save_session_phase(
                        &record,
                        &fence,
                        command.audits.connecting.clone(),
                        McpSessionState::Connecting,
                        None,
                        None,
                    )
                    .await?;
                record = outcome_record(&outcome).clone();
                fence = next_fence(&fence)?;
                let outcome = self
                    .save_session_phase(
                        &record,
                        &fence,
                        command.audits.initializing.clone(),
                        McpSessionState::Initializing,
                        None,
                        None,
                    )
                    .await?;
                record = outcome_record(&outcome).clone();
                fence = next_fence(&fence)?;
            }
            McpSessionState::Connecting => {
                let outcome = self
                    .save_session_phase(
                        &record,
                        &fence,
                        command.audits.initializing.clone(),
                        McpSessionState::Initializing,
                        None,
                        None,
                    )
                    .await?;
                record = outcome_record(&outcome).clone();
                fence = next_fence(&fence)?;
            }
            McpSessionState::Initializing => {}
            _ => return Err(McpSubscriptionWorkerError::InvalidCommand),
        }

        let now = Utc::now();
        let remaining = u64::try_from((record.deadline - now).num_milliseconds())
            .map_err(|_| McpSubscriptionWorkerError::InvalidCommand)?;
        let timeout_milliseconds =
            remaining.min(resolved.contract.server.limits.total_timeout_milliseconds);
        let future = AssertUnwindSafe(self.transport.establish(
            &resolved.contract,
            &record.payload.binding,
            record.payload.session.generation,
            &fence.worker_process_generation_id,
            record.deadline,
        ))
        .catch_unwind();
        let prepared =
            match tokio::time::timeout(Duration::from_millis(timeout_milliseconds), future).await {
                Ok(Ok(Ok(prepared))) => prepared,
                Ok(Ok(Err(failure))) => {
                    return self
                        .handle_establish_failure(record, fence, command.audits.terminal, failure)
                        .await;
                }
                Ok(Err(_)) => {
                    let failure = retryable_failure("mcp_subscription_transport_panic");
                    return Err(McpSubscriptionWorkerError::Transport(failure));
                }
                Err(_) => {
                    let failure = retryable_failure("mcp_subscription_transport_timeout");
                    return Err(McpSubscriptionWorkerError::Transport(failure));
                }
            };
        prepared
            .established
            .validate_for(&record.payload.binding, &resolved.contract, Utc::now())
            .map_err(|_| McpSubscriptionWorkerError::InvalidCommand)?;
        let reconcile_audit = command.audits.refresh.clone();
        let ready = self
            .save_session_phase(
                &record,
                &fence,
                command.audits.ready,
                McpSessionState::Ready,
                Some((
                    prepared.established.encrypted_opaque_session.clone(),
                    prepared.established.expires_at,
                )),
                Some(prepared.established.evidence_digest.clone()),
            )
            .await?;
        prepared.activate().await;
        let ready_record = outcome_record(&ready);
        if ready_record.payload.full_reconcile_required {
            return self
                .reconcile_record(ready_record.clone(), next_fence(&fence)?, reconcile_audit)
                .await;
        }
        Ok(McpSubscriptionWorkerResult::Established(ready))
    }

    async fn handle_establish_failure(
        &self,
        record: McpSubscriptionRecord,
        fence: JobFence,
        audit: McpSubscriptionWorkerAudit,
        failure: McpTransportFailure,
    ) -> Result<McpSubscriptionWorkerResult, McpSubscriptionWorkerError> {
        failure
            .validate_wire_shape()
            .map_err(|_| McpSubscriptionWorkerError::InvalidCommand)?;
        let terminal = match &failure {
            McpTransportFailure::ReauthorizationRequired { challenge_digest } => {
                Some((McpSessionState::ReauthRequired, challenge_digest.clone()))
            }
            McpTransportFailure::RejectedBeforeDispatch(failure)
            | McpTransportFailure::Permanent(failure) => {
                Some((McpSessionState::Failed, failure.evidence_digest.clone()))
            }
            McpTransportFailure::RetryableBeforeDispatch(_)
            | McpTransportFailure::PostDispatchUncertain { .. } => None,
        };
        let Some((target, phase_evidence_digest)) = terminal else {
            return Err(McpSubscriptionWorkerError::Transport(failure));
        };
        let terminal = self
            .save_session_phase(
                &record,
                &fence,
                audit,
                target,
                None,
                Some(phase_evidence_digest),
            )
            .await?;
        Ok(McpSubscriptionWorkerResult::Terminalized(terminal))
    }

    async fn refresh(
        &self,
        command: ExecuteMcpSubscriptionJob,
        resolved: ResolvedMcpSubscriptionExecution,
    ) -> Result<McpSubscriptionWorkerResult, McpSubscriptionWorkerError> {
        if !matches!(resolved.record.state, McpSubscriptionState::Active)
            || !matches!(
                resolved.record.payload.session.state,
                McpSessionState::Ready | McpSessionState::Degraded
            )
        {
            return Err(McpSubscriptionWorkerError::InvalidCommand);
        }
        let request = McpSubscriptionInvalidationRequest::build(&resolved.record)
            .map_err(|_| McpSubscriptionWorkerError::InvalidCommand)?;
        let accepted = self
            .invalidation_target
            .accept_invalidation(request.clone())
            .await
            .map_err(McpSubscriptionWorkerError::Invalidation)?;
        accepted
            .validate_for(&request, Utc::now())
            .map_err(|_| McpSubscriptionWorkerError::InvalidCommand)?;
        let pending = resolved
            .record
            .payload
            .pending_invalidation
            .as_ref()
            .ok_or(McpSubscriptionWorkerError::InvalidCommand)?;
        let mut completion = CompleteMcpSubscriptionRefresh {
            audit: command.audits.refresh,
            subscription_id: resolved.record.subscription_id.clone(),
            job_id: resolved.record.job_id.clone(),
            fence: command.query.fence,
            expected_subscription_version: resolved.record.version,
            expected_session_generation: pending.session_generation,
            expected_event_generation: pending.event_generation,
            refresh_evidence_digest: accepted.durable_work_digest,
        };
        completion.audit.request_digest = completion
            .request_digest()
            .map_err(|_| McpSubscriptionWorkerError::InvalidCommand)?;
        let outcome = self
            .authority
            .complete_subscription_refresh(completion)
            .await
            .map_err(McpSubscriptionWorkerError::Persistence)?;
        Ok(McpSubscriptionWorkerResult::RefreshAccepted(outcome))
    }

    async fn reconcile(
        &self,
        command: ExecuteMcpSubscriptionJob,
        resolved: ResolvedMcpSubscriptionExecution,
    ) -> Result<McpSubscriptionWorkerResult, McpSubscriptionWorkerError> {
        self.reconcile_record(resolved.record, command.query.fence, command.audits.refresh)
            .await
    }

    async fn reconcile_record(
        &self,
        record: McpSubscriptionRecord,
        fence: JobFence,
        audit: McpSubscriptionWorkerAudit,
    ) -> Result<McpSubscriptionWorkerResult, McpSubscriptionWorkerError> {
        let request = McpSubscriptionReconcileRequest::build(&record)
            .map_err(|_| McpSubscriptionWorkerError::InvalidCommand)?;
        request
            .validate_for(&record)
            .map_err(|_| McpSubscriptionWorkerError::InvalidCommand)?;
        let accepted = self
            .invalidation_target
            .accept_reconcile(request.clone())
            .await
            .map_err(McpSubscriptionWorkerError::Invalidation)?;
        if accepted.request_digest != request.request_digest
            || accepted.accepted_at
                > Utc::now() + ChronoDuration::seconds(MAX_SUBSCRIPTION_CLOCK_SKEW_SECONDS)
        {
            return Err(McpSubscriptionWorkerError::InvalidCommand);
        }
        let mut completion = CompleteMcpSubscriptionReconcile {
            audit,
            subscription_id: record.subscription_id.clone(),
            job_id: record.job_id.clone(),
            fence,
            expected_subscription_version: record.version,
            expected_session_generation: record.payload.session.generation,
            reconcile_evidence_digest: accepted.durable_work_digest,
        };
        completion.audit.request_digest = completion
            .request_digest()
            .map_err(|_| McpSubscriptionWorkerError::InvalidCommand)?;
        let outcome = self
            .authority
            .complete_subscription_reconcile(completion)
            .await
            .map_err(McpSubscriptionWorkerError::Persistence)?;
        Ok(McpSubscriptionWorkerResult::Reconciled(outcome))
    }

    async fn save_session_phase(
        &self,
        record: &McpSubscriptionRecord,
        fence: &JobFence,
        audit: McpSubscriptionWorkerAudit,
        target: McpSessionState,
        ready: Option<(EncryptedMcpState, DateTime<Utc>)>,
        phase_evidence_digest: Option<Sha256Digest>,
    ) -> Result<CommandOutcome<McpSubscriptionRecord>, McpSubscriptionWorkerError> {
        let expected_record_version = record
            .version
            .checked_add(1)
            .ok_or(McpSubscriptionWorkerError::InvalidCommand)?;
        let expected_session_version = record
            .payload
            .session
            .version
            .checked_add(1)
            .ok_or(McpSubscriptionWorkerError::InvalidCommand)?;
        let (encrypted_opaque_session, expires_at) = ready.unzip();
        let mut command = SaveMcpSubscriptionSession {
            audit,
            subscription_id: record.subscription_id.clone(),
            job_id: record.job_id.clone(),
            fence: fence.clone(),
            expected_subscription_version: record.version,
            expected_session_version: record.payload.session.version,
            target,
            encrypted_opaque_session,
            expires_at,
            phase_evidence_digest,
        };
        command.audit.request_digest = command
            .request_digest()
            .map_err(|_| McpSubscriptionWorkerError::InvalidCommand)?;
        let outcome = self
            .authority
            .save_subscription_session(command)
            .await
            .map_err(McpSubscriptionWorkerError::Persistence)?;
        let next = outcome_record(&outcome);
        if next.version != expected_record_version
            || next.payload.session.version != expected_session_version
            || next.payload.session.state != target
        {
            return Err(McpSubscriptionWorkerError::Persistence(
                McpSubscriptionPersistenceError::Conflict,
            ));
        }
        Ok(outcome)
    }
}

fn next_fence(current: &JobFence) -> Result<JobFence, McpSubscriptionWorkerError> {
    Ok(JobFence {
        expected_version: current
            .expected_version
            .checked_add(1)
            .ok_or(McpSubscriptionWorkerError::InvalidCommand)?,
        worker_process_generation_id: current.worker_process_generation_id.clone(),
        lease_generation: current.lease_generation,
        token_digest: current.token_digest.clone(),
    })
}

fn outcome_record(outcome: &CommandOutcome<McpSubscriptionRecord>) -> &McpSubscriptionRecord {
    match outcome {
        CommandOutcome::Applied(record) | CommandOutcome::Replayed(record) => record,
    }
}

fn retryable_failure(code: &str) -> McpTransportFailure {
    McpTransportFailure::RetryableBeforeDispatch(SafeMcpFailure {
        safe_code: code.to_owned(),
        safe_message: "MCP subscription completion could not be observed".to_owned(),
        evidence_digest: static_digest(code),
    })
}
