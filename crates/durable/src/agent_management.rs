//! Durable Agent authoring and deployment control-plane contracts.
//!
//! Mutable authoring state lives only in a versioned Draft. Validation,
//! Definition, resolution, Deployment and Debug evidence is immutable. Public
//! routing changes only through an explicit Agent entity CAS transaction.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{PublicationHead, RepositoryError, VersionedPlan};

pub const AGENT_MANAGEMENT_MAX_REQUEST_ID_BYTES: usize = 256;
pub const AGENT_MANAGEMENT_MAX_OPERATOR_ID_BYTES: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentAuthoringMode {
    YamlPackage,
    Graph,
}

impl AgentAuthoringMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::YamlPackage => "yaml_package",
            Self::Graph => "graph",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentLifecycle {
    Editable,
    Archived,
}

impl AgentLifecycle {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Editable => "editable",
            Self::Archived => "archived",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentOperationStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl AgentOperationStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentDebugStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Expired,
}

impl AgentDebugStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Expired => "expired",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedAgent {
    pub agent_id: String,
    pub authoring_mode: AgentAuthoringMode,
    pub labels: Value,
    pub lifecycle: AgentLifecycle,
    pub entity_version: u64,
    pub draft_version: u64,
    pub active_definition_revision_id: Option<String>,
    pub active_deployment_revision_id: Option<String>,
    pub archived_publication_head: Option<PublicationHead>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentStoredDraft {
    pub agent_id: String,
    pub draft_version: u64,
    pub author_hash: String,
    pub document: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentStoredDraftView {
    pub agent_id: String,
    pub view_version: u64,
    pub document: Value,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentValidationReport {
    pub validation_id: String,
    pub agent_id: String,
    pub draft_version: u64,
    pub author_hash: String,
    pub policy_digest: String,
    pub status: AgentOperationStatus,
    pub semantic_hash: Option<String>,
    pub report_hash: String,
    pub document: Value,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentDefinitionRevision {
    pub agent_id: String,
    pub definition_id: String,
    pub definition_revision_id: String,
    pub revision_number: u64,
    pub source_draft_version: u64,
    pub validation_id: String,
    pub author_hash: String,
    pub semantic_hash: String,
    pub compiler_version: String,
    pub expression_engine_version: String,
    pub author_document: Value,
    pub canonical_plan: Value,
    pub descriptor_contracts: Value,
    pub display_name: String,
    pub public_description: String,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentDeploymentResolution {
    pub resolution_id: String,
    pub agent_id: String,
    pub definition_revision_id: String,
    pub status: AgentOperationStatus,
    pub catalog_snapshot_hash: String,
    pub resolution_hash: String,
    pub resolved_bindings: Value,
    pub worker_contracts: Value,
    pub dependency_heads: Value,
    pub risks: Value,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentDeploymentRevision {
    pub agent_id: String,
    pub definition_id: String,
    pub definition_revision_id: String,
    pub deployment_revision_id: String,
    pub resolution_id: String,
    pub plan_hash: String,
    pub binding_hash: String,
    pub resolved_bindings: Value,
    pub worker_contracts: Value,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentDebugSession {
    pub debug_session_id: String,
    pub agent_id: String,
    pub source: Value,
    pub source_hash: String,
    pub execution_profile_id: String,
    pub profile_mode: String,
    pub status: AgentDebugStatus,
    pub definition_revision_id: Option<String>,
    pub deployment_revision_id: Option<String>,
    pub run_id: Option<String>,
    pub failure_code: Option<String>,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub created_by: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManagementPage<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentDebugRuntimeCount {
    pub state: AgentDebugStatus,
    pub profile_mode: String,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManagementOperationCount {
    pub operation: String,
    pub outcome: String,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManagementRuntimeStats {
    pub drafts_current: u64,
    pub validations_pending: u64,
    pub deployment_resolutions_pending: u64,
    pub active_agents: u64,
    pub archived_agents: u64,
    pub debug_sessions: Vec<AgentDebugRuntimeCount>,
    pub operations: Vec<AgentManagementOperationCount>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentMutationReceipt {
    pub replayed: bool,
    pub status: u16,
    pub response: Value,
    pub etag: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentManagementConflict {
    NotFound,
    ForbiddenState,
    PreconditionFailed,
    IdempotencyKeyReused,
    Referenced,
    ValidationStale,
    ValidationFailed,
    ResolutionExpired,
    DependencyHeadChanged,
    DebugSessionActive,
    CapacityExceeded,
}

#[derive(Debug)]
pub enum AgentManagementWriteError {
    Conflict(AgentManagementConflict),
    Repository(RepositoryError),
}

impl From<RepositoryError> for AgentManagementWriteError {
    fn from(value: RepositoryError) -> Self {
        Self::Repository(value)
    }
}

impl std::fmt::Display for AgentManagementWriteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conflict(_) => formatter.write_str("Agent management state conflict"),
            Self::Repository(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for AgentManagementWriteError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMutationMetadata {
    pub operator_id: String,
    pub capability: String,
    pub method: String,
    pub canonical_path: String,
    pub request_id: String,
    pub request_hash: String,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateAgentCommand {
    pub metadata: AgentMutationMetadata,
    pub agent_id: String,
    pub authoring_mode: AgentAuthoringMode,
    pub labels: Value,
    pub draft_document: Value,
    pub author_hash: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReplaceAgentDraftCommand {
    pub metadata: AgentMutationMetadata,
    pub agent_id: String,
    pub expected_draft_version: u64,
    pub draft_document: Value,
    pub author_hash: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReplaceAgentDraftViewCommand {
    pub metadata: AgentMutationMetadata,
    pub agent_id: String,
    pub expected_view_version: u64,
    pub document: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateAgentLabelsCommand {
    pub metadata: AgentMutationMetadata,
    pub agent_id: String,
    pub expected_entity_version: u64,
    pub labels: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteAgentCommand {
    pub metadata: AgentMutationMetadata,
    pub agent_id: String,
    pub expected_entity_version: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateAgentValidationCommand {
    pub metadata: AgentMutationMetadata,
    pub report: AgentValidationReport,
    pub expected_draft_version: u64,
    pub expected_author_hash: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PublishAgentDefinitionCommand {
    pub metadata: AgentMutationMetadata,
    pub expected_draft_version: u64,
    pub validation_id: String,
    pub validation_policy_digest: String,
    pub plan: VersionedPlan,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateAgentResolutionCommand {
    pub metadata: AgentMutationMetadata,
    pub resolution: AgentDeploymentResolution,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InstallAgentDeploymentCommand {
    pub metadata: AgentMutationMetadata,
    pub resolution_id: String,
    pub expected_resolution_hash: String,
    pub expected_dependency_heads: Value,
    pub plan: VersionedPlan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivateManagedAgentDeploymentCommand {
    pub metadata: AgentMutationMetadata,
    pub agent_id: String,
    pub expected_entity_version: u64,
    pub deployment_revision_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeactivateManagedAgentCommand {
    pub metadata: AgentMutationMetadata,
    pub agent_id: String,
    pub expected_entity_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveAgentCommand {
    pub metadata: AgentMutationMetadata,
    pub agent_id: String,
    pub expected_entity_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreAgentCommand {
    pub metadata: AgentMutationMetadata,
    pub agent_id: String,
    pub expected_entity_version: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateAgentDebugSessionCommand {
    pub metadata: AgentMutationMetadata,
    pub session: AgentDebugSession,
    pub max_active_sessions: u32,
    pub retain_until: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompleteAgentDebugSessionCommand {
    pub debug_session_id: String,
    pub status: AgentDebugStatus,
    /// Optional exact temporary Definition/Deployment installed by the debug
    /// worker. The repository installs it in the same transaction that moves
    /// the session out of `queued`, without changing a public publication
    /// head.
    pub plan: Option<VersionedPlan>,
    pub definition_revision_id: Option<String>,
    pub deployment_revision_id: Option<String>,
    pub run_id: Option<String>,
    pub failure_code: Option<String>,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelAgentDebugSessionCommand {
    pub metadata: AgentMutationMetadata,
    pub agent_id: String,
    pub debug_session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordAgentManagementRejectionCommand {
    pub actor_id: String,
    pub capability: String,
    pub request_id: String,
    pub agent_id: Option<String>,
    pub subject_id: String,
    pub result_code: String,
    pub now: DateTime<Utc>,
}

#[async_trait]
pub trait AgentManagementDurableRepository: Send + Sync {
    /// Records an authenticated request rejected before a durable mutation
    /// commits. Request and response bodies are deliberately absent.
    async fn record_agent_management_rejection(
        &self,
        command: RecordAgentManagementRejectionCommand,
    ) -> Result<(), RepositoryError>;

    async fn replay_agent_mutation(
        &self,
        metadata: &AgentMutationMetadata,
    ) -> Result<Option<AgentMutationReceipt>, AgentManagementWriteError>;
    async fn create_agent(
        &self,
        command: CreateAgentCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError>;
    async fn get_agent(&self, agent_id: &str) -> Result<Option<ManagedAgent>, RepositoryError>;
    async fn list_agents(
        &self,
        lifecycle: Option<AgentLifecycle>,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<AgentManagementPage<ManagedAgent>, RepositoryError>;
    async fn update_agent_labels(
        &self,
        command: UpdateAgentLabelsCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError>;
    async fn delete_agent(
        &self,
        command: DeleteAgentCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError>;

    async fn get_agent_draft(
        &self,
        agent_id: &str,
    ) -> Result<Option<AgentStoredDraft>, RepositoryError>;
    async fn replace_agent_draft(
        &self,
        command: ReplaceAgentDraftCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError>;
    async fn get_agent_draft_view(
        &self,
        agent_id: &str,
    ) -> Result<Option<AgentStoredDraftView>, RepositoryError>;
    async fn replace_agent_draft_view(
        &self,
        command: ReplaceAgentDraftViewCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError>;

    async fn create_agent_validation(
        &self,
        command: CreateAgentValidationCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError>;
    async fn get_agent_validation(
        &self,
        agent_id: &str,
        validation_id: &str,
    ) -> Result<Option<AgentValidationReport>, RepositoryError>;

    async fn publish_agent_definition(
        &self,
        command: PublishAgentDefinitionCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError>;
    async fn get_agent_definition(
        &self,
        agent_id: &str,
        definition_revision_id: &str,
    ) -> Result<Option<AgentDefinitionRevision>, RepositoryError>;
    async fn list_agent_definitions(
        &self,
        agent_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<AgentManagementPage<AgentDefinitionRevision>, RepositoryError>;

    async fn create_agent_deployment_resolution(
        &self,
        command: CreateAgentResolutionCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError>;
    async fn get_agent_deployment_resolution(
        &self,
        agent_id: &str,
        resolution_id: &str,
    ) -> Result<Option<AgentDeploymentResolution>, RepositoryError>;
    async fn install_agent_deployment(
        &self,
        command: InstallAgentDeploymentCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError>;
    async fn get_agent_deployment(
        &self,
        agent_id: &str,
        deployment_revision_id: &str,
    ) -> Result<Option<AgentDeploymentRevision>, RepositoryError>;
    async fn list_agent_deployments(
        &self,
        agent_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<AgentManagementPage<AgentDeploymentRevision>, RepositoryError>;

    async fn activate_managed_agent_deployment(
        &self,
        command: ActivateManagedAgentDeploymentCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError>;
    async fn deactivate_managed_agent(
        &self,
        command: DeactivateManagedAgentCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError>;
    async fn archive_agent(
        &self,
        command: ArchiveAgentCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError>;
    async fn restore_agent(
        &self,
        command: RestoreAgentCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError>;

    async fn create_agent_debug_session(
        &self,
        command: CreateAgentDebugSessionCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError>;
    async fn get_agent_debug_session(
        &self,
        agent_id: &str,
        debug_session_id: &str,
    ) -> Result<Option<AgentDebugSession>, RepositoryError>;
    async fn list_agent_debug_sessions(
        &self,
        agent_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<AgentManagementPage<AgentDebugSession>, RepositoryError>;
    async fn complete_agent_debug_session(
        &self,
        command: CompleteAgentDebugSessionCommand,
    ) -> Result<(), AgentManagementWriteError>;
    async fn cancel_agent_debug_session(
        &self,
        command: CancelAgentDebugSessionCommand,
    ) -> Result<AgentMutationReceipt, AgentManagementWriteError>;
    async fn cleanup_expired_agent_debug_sessions(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, RepositoryError>;

    async fn load_agent_management_runtime_stats(
        &self,
    ) -> Result<AgentManagementRuntimeStats, RepositoryError>;
}
