use super::{
    digest, static_digest, CompleteMcpSubscriptionReconcile, CompleteMcpSubscriptionRefresh,
    DueMcpSubscriptionReconcile, DueMcpSubscriptionRecovery, EncryptedMcpState,
    McpExecutionContractResolutionError, McpHostError, McpHostExecutionContract,
    McpNotificationClass, McpResourceSubscriptionBinding, McpSubscriptionReconcileScan,
    McpSubscriptionRecord, McpSubscriptionRecoveryScan, McpSubscriptionState,
    McpSubscriptionWorkerAudit, McpTransportFailure, RecoverDueMcpSubscription,
    ReportMcpSubscriptionSessionLoss, ReportMcpSubscriptionTransportTermination,
    SaveMcpSubscriptionSession, WakeMcpSubscriptionReconcile,
};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use insight_platform_contracts::{
    CommandOutcome, McpSessionState, McpTransportKind, ResourceId, ResourceKind, Sha256Digest,
};
use insight_platform_jobs::JobFence;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, error::Error, fmt};

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
/// Implementations own the live HTTP/SSE connection. Managed stdio subscriptions are admitted as
/// physical Sandbox Jobs and never enter this Host-local transport boundary. Implementations
/// receive only the exact immutable contract and return an encrypted reconstructable handle; they
/// cannot mutate durable subscription or Job state.
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
    pub resource_uri: String,
    pub resource_uri_digest: Sha256Digest,
    pub authorization_generation: u64,
    pub session_generation: u64,
    pub event_generation: u64,
    pub event_key_digest: Sha256Digest,
    pub body_digest: Sha256Digest,
    pub reason: McpSubscriptionRefreshReason,
    pub deadline: DateTime<Utc>,
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
            resource_uri: binding.resource_uri.clone(),
            resource_uri_digest: binding.resource_uri_digest.clone(),
            authorization_generation: binding.authorization_generation,
            session_generation: pending.session_generation,
            event_generation: pending.event_generation,
            event_key_digest: pending.event_key_digest.clone(),
            body_digest: pending.body_digest.clone(),
            reason,
            deadline: record.deadline,
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
            || self.resource_uri != binding.resource_uri
            || self.resource_uri_digest != binding.resource_uri_digest
            || self.authorization_generation != binding.authorization_generation
            || self.session_generation != pending.session_generation
            || self.event_generation != pending.event_generation
            || self.event_key_digest != pending.event_key_digest
            || self.body_digest != pending.body_digest
            || self.deadline != record.deadline
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
            "resource_uri": self.resource_uri,
            "resource_uri_digest": self.resource_uri_digest,
            "reason": self.reason,
            "schema_version": self.schema_version,
            "session_generation": self.session_generation,
            "subscription_id": self.subscription_id,
            "tenant_id": self.tenant_id,
            "deadline": self.deadline,
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
    pub deadline: DateTime<Utc>,
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
            deadline: record.deadline,
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
            || self.deadline != record.deadline
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
            "deadline": self.deadline,
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
    ) -> Result<
        insight_platform_jobs::store::SafetyScanPage<DueMcpSubscriptionReconcile>,
        McpSubscriptionPersistenceError,
    >;

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
    ) -> Result<
        insight_platform_jobs::store::SafetyScanPage<DueMcpSubscriptionRecovery>,
        McpSubscriptionPersistenceError,
    >;

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
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpSubscriptionRecoveryDriveOutcome {
    pub diagnostics: Vec<insight_platform_jobs::store::SafeScanDiagnostic>,
    pub next_cursor: Option<insight_platform_jobs::store::SafetyScanCursor>,
    pub exhausted: bool,
    pub observed: u16,
    pub recovered: u16,
    pub stale: u16,
}

#[derive(Debug, Clone)]
pub struct DriveMcpSubscriptionReconciliations {
    pub scan: McpSubscriptionReconcileScan,
    pub audits: Vec<McpSubscriptionWorkerAudit>,
}

impl DriveMcpSubscriptionReconciliations {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpHostError> {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpSubscriptionReconcileDriveOutcome {
    pub diagnostics: Vec<insight_platform_jobs::store::SafeScanDiagnostic>,
    pub next_cursor: Option<insight_platform_jobs::store::SafetyScanCursor>,
    pub exhausted: bool,
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

#[derive(Debug, Clone)]
pub struct McpSubscriptionWorkerAudits {
    pub connecting: McpSubscriptionWorkerAudit,
    pub initializing: McpSubscriptionWorkerAudit,
    pub ready: McpSubscriptionWorkerAudit,
    pub terminal: McpSubscriptionWorkerAudit,
    pub refresh: McpSubscriptionWorkerAudit,
}

impl McpSubscriptionWorkerAudits {
    pub fn validate_at(
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
    LeaseCoordination,
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
            Self::LeaseCoordination => {
                formatter.write_str("MCP subscription lease coordination failed")
            }
            Self::Contract(failure) => write!(formatter, "{failure}"),
            Self::Transport(failure) => write!(
                formatter,
                "MCP subscription transport failed: {}",
                transport_failure_code(failure)
            ),
            Self::Invalidation(failure) => write!(formatter, "{failure}"),
            Self::Persistence(failure) => write!(formatter, "{failure}"),
        }
    }
}

impl Error for McpSubscriptionWorkerError {}

/// Coordinates the only long remote-I/O window with the durable Job lease owner. Implementations
/// must serialize the enter/exit handshake with heartbeat writes and return the latest exact
/// fence. This keeps subscription owner transitions and heartbeat CAS from racing each other.
#[async_trait]
pub trait McpSubscriptionRemoteIoLease: Send + Sync {
    async fn enter_remote_io(&self, fence: &JobFence) -> Result<JobFence, ()>;

    async fn exit_remote_io(&self, fence: &JobFence) -> Result<JobFence, ()>;
}

pub fn transport_failure_code(failure: &McpTransportFailure) -> &str {
    match failure {
        McpTransportFailure::RejectedBeforeDispatch(failure)
        | McpTransportFailure::RetryableBeforeDispatch(failure)
        | McpTransportFailure::Permanent(failure)
        | McpTransportFailure::PostDispatchUncertain { failure, .. } => &failure.safe_code,
        McpTransportFailure::ReauthorizationRequired { .. } => "mcp_reauthorization_required",
    }
}

pub const MAX_SUBSCRIPTION_CLOCK_SKEW_SECONDS: i64 = 60;
