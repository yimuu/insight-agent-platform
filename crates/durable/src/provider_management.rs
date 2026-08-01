//! Durable model Provider management control-plane contracts.
//!
//! Provider configuration is mutable only in a Draft. Discovery snapshots,
//! connection-test results, validation reports, and published revisions are
//! immutable. Network I/O is performed by leased workers outside repository
//! transactions; the repository owns CAS, idempotency, lifecycle, and the
//! operational suspension fence.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::RepositoryError;

pub const PROVIDER_MANAGEMENT_MAX_REQUEST_ID_BYTES: usize = 256;
pub const PROVIDER_MANAGEMENT_MAX_OPERATOR_ID_BYTES: usize = 256;
pub const PROVIDER_MANAGEMENT_NOTIFY_CHANNEL_PREFIX: &str = "insight_provider_management_";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderOperationalState {
    Enabled,
    Suspended,
    Retired,
}

impl ProviderOperationalState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Suspended => "suspended",
            Self::Retired => "retired",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderOperationStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl ProviderOperationStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub const fn terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderConnectionTestMode {
    Metadata,
    Canary,
    CapabilityProbe,
}

impl ProviderConnectionTestMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Metadata => "metadata",
            Self::Canary => "canary",
            Self::CapabilityProbe => "capability_probe",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedProvider {
    pub provider_id: String,
    pub display_name: String,
    pub adapter_type: String,
    pub operational_state: ProviderOperationalState,
    pub provider_version: u64,
    pub draft_version: u64,
    pub active_revision_id: Option<String>,
    pub suspension_fence: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderStoredDraft {
    pub provider_id: String,
    pub draft_version: u64,
    pub provider_input_hash: String,
    pub document: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderOperationFailure {
    pub code: String,
    pub stage: String,
    pub retryable: bool,
    pub correlation_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDiscoveryOperation {
    pub discovery_id: String,
    pub provider_id: String,
    pub source_draft_version: u64,
    pub provider_input_hash: String,
    pub status: ProviderOperationStatus,
    pub cancel_requested: bool,
    pub attempts: u32,
    pub claimed_by: Option<String>,
    pub claim_token: Option<String>,
    pub claim_expires_at: Option<DateTime<Utc>>,
    pub failure: Option<ProviderOperationFailure>,
    pub stale: bool,
    pub stale_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDiscoverySnapshot {
    pub discovery_id: String,
    pub provider_id: String,
    pub source_draft_version: u64,
    pub provider_input_hash: String,
    pub catalog_fingerprint: String,
    pub document: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderModelCandidate {
    pub discovery_id: String,
    pub ordinal: u32,
    pub model_id: String,
    pub candidate_fingerprint: String,
    pub document: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConnectionTest {
    pub test_id: String,
    pub provider_id: String,
    pub source_draft_version: u64,
    pub provider_input_hash: String,
    pub mode: ProviderConnectionTestMode,
    pub status: ProviderOperationStatus,
    pub cancel_requested: bool,
    pub attempts: u32,
    pub claimed_by: Option<String>,
    pub claim_token: Option<String>,
    pub claim_expires_at: Option<DateTime<Utc>>,
    pub failure: Option<ProviderOperationFailure>,
    pub result_hash: Option<String>,
    pub result: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderValidationReport {
    pub validation_id: String,
    pub provider_id: String,
    pub draft_version: u64,
    pub provider_input_hash: String,
    pub report_hash: String,
    pub valid: bool,
    pub document: Value,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRevision {
    pub revision_id: String,
    pub provider_id: String,
    pub revision_number: u64,
    pub source_draft_version: u64,
    pub validation_id: String,
    pub discovery_id: Option<String>,
    pub connection_test_id: Option<String>,
    pub revision_hash: String,
    pub document: Value,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
}

/// Immutable compatibility evidence created only by the offline clean-cut
/// migration. It lets a managed Provider Revision materialize the exact model
/// binding hash already pinned by a historical Deployment without rewriting
/// that Deployment or its Canonical Plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderLegacyModelBinding {
    pub revision_id: String,
    pub provider_id: String,
    pub model_id: String,
    pub legacy_binding_hash: String,
    pub legacy_binding_evidence: Value,
    pub source_definition_id: String,
    pub source_deployment_revision_id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderManagementPage<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderMutationReceipt {
    pub replayed: bool,
    pub status: u16,
    pub response: Value,
    pub etag: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderManagementConflict {
    NotFound,
    ForbiddenState,
    PreconditionFailed,
    IdempotencyKeyReused,
    Referenced,
    OperationStale,
    CandidateMismatch,
    ValidationFailed,
    CapacityExceeded,
    FenceLost,
}

#[derive(Debug)]
pub enum ProviderManagementWriteError {
    Conflict(ProviderManagementConflict),
    Repository(RepositoryError),
}

impl From<RepositoryError> for ProviderManagementWriteError {
    fn from(value: RepositoryError) -> Self {
        Self::Repository(value)
    }
}

impl std::fmt::Display for ProviderManagementWriteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conflict(_) => formatter.write_str("Provider management state conflict"),
            Self::Repository(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ProviderManagementWriteError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderMutationMetadata {
    pub operator_id: String,
    pub capability: String,
    pub method: String,
    pub canonical_path: String,
    pub request_id: String,
    pub request_hash: String,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateProviderCommand {
    pub metadata: ProviderMutationMetadata,
    pub provider_id: String,
    pub display_name: String,
    pub adapter_type: String,
    pub draft_document: Value,
    pub provider_input_hash: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReplaceProviderDraftCommand {
    pub metadata: ProviderMutationMetadata,
    pub provider_id: String,
    pub expected_draft_version: u64,
    pub draft_document: Value,
    pub provider_input_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteProviderCommand {
    pub metadata: ProviderMutationMetadata,
    pub provider_id: String,
    pub expected_provider_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProviderDiscoveryCommand {
    pub metadata: ProviderMutationMetadata,
    pub discovery_id: String,
    pub provider_id: String,
    pub expected_draft_version: u64,
    pub provider_input_hash: String,
    pub max_pending_operations: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelProviderOperationCommand {
    pub metadata: ProviderMutationMetadata,
    pub provider_id: String,
    pub operation_id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateProviderValidationCommand {
    pub metadata: ProviderMutationMetadata,
    pub report: ProviderValidationReport,
    pub expected_draft_version: u64,
    pub expected_provider_input_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProviderConnectionTestCommand {
    pub metadata: ProviderMutationMetadata,
    pub test_id: String,
    pub provider_id: String,
    pub expected_draft_version: u64,
    pub provider_input_hash: String,
    pub mode: ProviderConnectionTestMode,
    pub max_pending_operations: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PublishProviderRevisionCommand {
    pub metadata: ProviderMutationMetadata,
    pub revision_id: String,
    pub provider_id: String,
    pub expected_draft_version: u64,
    pub validation_id: String,
    pub discovery_id: Option<String>,
    pub connection_test_id: Option<String>,
    pub revision_hash: String,
    pub document: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivateProviderRevisionCommand {
    pub metadata: ProviderMutationMetadata,
    pub provider_id: String,
    pub revision_id: String,
    pub expected_provider_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeactivateProviderRevisionCommand {
    pub metadata: ProviderMutationMetadata,
    pub provider_id: String,
    pub expected_provider_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuspendProviderCommand {
    pub metadata: ProviderMutationMetadata,
    pub provider_id: String,
    pub expected_provider_version: u64,
    pub reason_code: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeProviderCommand {
    pub metadata: ProviderMutationMetadata,
    pub provider_id: String,
    pub expected_provider_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetireProviderCommand {
    pub metadata: ProviderMutationMetadata,
    pub provider_id: String,
    pub expected_provider_version: u64,
    pub reason_code: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordProviderManagementRejectionCommand {
    pub actor_id: String,
    pub capability: String,
    pub request_id: String,
    pub provider_id: Option<String>,
    pub subject_id: String,
    pub result_code: String,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimProviderOperationsCommand {
    pub worker_id: String,
    pub now: DateTime<Utc>,
    pub lease_expires_at: DateTime<Utc>,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDiscoveryClaim {
    pub operation: ProviderDiscoveryOperation,
    pub claim_token: String,
    pub draft_document: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConnectionTestClaim {
    pub operation: ProviderConnectionTest,
    pub claim_token: String,
    pub draft_document: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompleteProviderDiscoveryResult {
    Succeeded {
        catalog_fingerprint: String,
        snapshot_document: Value,
    },
    Failed(ProviderOperationFailure),
    Cancelled,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompleteProviderDiscoveryCommand {
    pub discovery_id: String,
    pub claim_token: String,
    pub now: DateTime<Utc>,
    pub result: CompleteProviderDiscoveryResult,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompleteProviderConnectionTestResult {
    Succeeded { result_hash: String, result: Value },
    Failed(ProviderOperationFailure),
    Cancelled,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompleteProviderConnectionTestCommand {
    pub test_id: String,
    pub claim_token: String,
    pub now: DateTime<Utc>,
    pub result: CompleteProviderConnectionTestResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderFence {
    pub provider_id: String,
    pub operational_state: ProviderOperationalState,
    pub active_revision_id: Option<String>,
    pub suspension_fence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderManagementRuntimeStats {
    pub pending_discoveries: u64,
    pub running_discoveries: u64,
    pub pending_connection_tests: u64,
    pub running_connection_tests: u64,
    pub active_providers: u64,
    pub suspended_providers: u64,
    pub enabled_providers: u64,
    pub retired_providers: u64,
    pub connection_tests: Vec<ProviderConnectionTestRuntimeCount>,
    pub operations: Vec<ProviderManagementOperationCount>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConnectionTestRuntimeCount {
    pub mode: ProviderConnectionTestMode,
    pub outcome: ProviderOperationStatus,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderManagementOperationCount {
    pub operation: String,
    pub outcome: String,
    pub count: u64,
}

#[async_trait]
pub trait ProviderManagementNotificationStream: Send {
    /// Receives an opaque, lossy hint. Consumers reload durable state and do
    /// not derive object identity or ordering from the notification itself.
    async fn recv(&mut self) -> Result<(), RepositoryError>;
}

#[async_trait]
pub trait ProviderManagementDurableRepository: Send + Sync {
    /// PostgreSQL returns a cross-runtime LISTEN stream. SQLite returns None
    /// and relies on its single-process safety poll.
    async fn open_provider_management_notification_stream(
        &self,
    ) -> Result<Option<Box<dyn ProviderManagementNotificationStream>>, RepositoryError> {
        Ok(None)
    }

    /// Records an authenticated request rejected before a durable mutation
    /// commits. Request and response bodies are deliberately absent.
    async fn record_provider_management_rejection(
        &self,
        command: RecordProviderManagementRejectionCommand,
    ) -> Result<(), RepositoryError>;

    async fn replay_provider_mutation(
        &self,
        metadata: &ProviderMutationMetadata,
    ) -> Result<Option<ProviderMutationReceipt>, ProviderManagementWriteError>;
    async fn create_provider(
        &self,
        command: CreateProviderCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError>;
    async fn replace_provider_draft(
        &self,
        command: ReplaceProviderDraftCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError>;
    async fn delete_provider(
        &self,
        command: DeleteProviderCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError>;
    async fn get_provider(
        &self,
        provider_id: &str,
    ) -> Result<Option<ManagedProvider>, RepositoryError>;
    async fn get_provider_draft(
        &self,
        provider_id: &str,
    ) -> Result<Option<ProviderStoredDraft>, RepositoryError>;
    async fn list_providers(
        &self,
        state: Option<ProviderOperationalState>,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<ProviderManagementPage<ManagedProvider>, RepositoryError>;

    async fn create_provider_discovery(
        &self,
        command: CreateProviderDiscoveryCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError>;
    async fn cancel_provider_discovery(
        &self,
        command: CancelProviderOperationCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError>;
    async fn get_provider_discovery(
        &self,
        provider_id: &str,
        discovery_id: &str,
    ) -> Result<Option<ProviderDiscoveryOperation>, RepositoryError>;
    async fn get_provider_discovery_snapshot(
        &self,
        provider_id: &str,
        discovery_id: &str,
    ) -> Result<Option<ProviderDiscoverySnapshot>, RepositoryError>;
    async fn list_provider_model_candidates(
        &self,
        provider_id: &str,
        discovery_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<ProviderManagementPage<ProviderModelCandidate>, RepositoryError>;
    async fn claim_provider_discoveries(
        &self,
        command: ClaimProviderOperationsCommand,
    ) -> Result<Vec<ProviderDiscoveryClaim>, RepositoryError>;
    async fn complete_provider_discovery(
        &self,
        command: CompleteProviderDiscoveryCommand,
    ) -> Result<(), ProviderManagementWriteError>;

    async fn create_provider_connection_test(
        &self,
        command: CreateProviderConnectionTestCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError>;
    async fn cancel_provider_connection_test(
        &self,
        command: CancelProviderOperationCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError>;
    async fn get_provider_connection_test(
        &self,
        provider_id: &str,
        test_id: &str,
    ) -> Result<Option<ProviderConnectionTest>, RepositoryError>;
    async fn claim_provider_connection_tests(
        &self,
        command: ClaimProviderOperationsCommand,
    ) -> Result<Vec<ProviderConnectionTestClaim>, RepositoryError>;
    async fn complete_provider_connection_test(
        &self,
        command: CompleteProviderConnectionTestCommand,
    ) -> Result<(), ProviderManagementWriteError>;

    async fn create_provider_validation(
        &self,
        command: CreateProviderValidationCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError>;
    async fn get_provider_validation(
        &self,
        provider_id: &str,
        validation_id: &str,
    ) -> Result<Option<ProviderValidationReport>, RepositoryError>;
    async fn publish_provider_revision(
        &self,
        command: PublishProviderRevisionCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError>;
    async fn get_provider_revision(
        &self,
        provider_id: &str,
        revision_id: &str,
    ) -> Result<Option<ProviderRevision>, RepositoryError>;
    async fn list_provider_revisions(
        &self,
        provider_id: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<ProviderManagementPage<ProviderRevision>, RepositoryError>;

    async fn activate_provider_revision(
        &self,
        command: ActivateProviderRevisionCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError>;
    async fn deactivate_provider_revision(
        &self,
        command: DeactivateProviderRevisionCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError>;
    async fn suspend_provider(
        &self,
        command: SuspendProviderCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError>;
    async fn resume_provider(
        &self,
        command: ResumeProviderCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError>;
    async fn retire_provider(
        &self,
        command: RetireProviderCommand,
    ) -> Result<ProviderMutationReceipt, ProviderManagementWriteError>;
    async fn load_active_provider_revisions(
        &self,
    ) -> Result<Vec<(ManagedProvider, ProviderRevision)>, RepositoryError>;
    async fn load_provider_revision_archive(
        &self,
    ) -> Result<Vec<(ManagedProvider, ProviderRevision)>, RepositoryError>;
    async fn load_provider_legacy_model_bindings(
        &self,
        revision_id: &str,
    ) -> Result<Vec<ProviderLegacyModelBinding>, RepositoryError>;
    async fn load_provider_fence(
        &self,
        provider_id: &str,
    ) -> Result<Option<ProviderFence>, RepositoryError>;
    async fn load_provider_management_runtime_stats(
        &self,
    ) -> Result<ProviderManagementRuntimeStats, RepositoryError>;
    async fn cleanup_terminal_provider_operations(
        &self,
        finished_before: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, RepositoryError>;
}
