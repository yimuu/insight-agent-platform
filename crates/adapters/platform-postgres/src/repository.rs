mod convergence_commands;
mod external_leaf_completion;
use external_leaf_completion::{require_committed_external_leaf_completion, CompletionValidation};
mod capability_commands;
mod child_run_commands;
mod context_commands;
mod controller_commands;
mod controller_queries;
mod installation_commands;
mod job_commands;
mod model_commands;
mod model_configuration;
mod model_connection;
mod model_credential_import;
mod model_default_commands;
mod model_policy_bootstrap;
mod model_quota;
pub(crate) use model_default_commands::validate_default_model_closure;
mod model_convergence;
use model_convergence::{model_run_convergence_goal, settle_suppressed_model_continuation};
mod conversations;
mod quota_commands;
mod registry_commands;
mod registry_queries;
mod run_commands;
pub(crate) use conversations::authorize_conversation_run_read;
pub use conversations::{ConversationHistoryValue, ConversationReadScope};
mod run_execution;
pub mod run_live;
mod run_queries;
mod scheduler_commands;
mod task_commands;
mod task_queries;
use insight_platform_orchestrator::store::RunResultRecord;
mod security_commands;
use chrono::{DateTime, Duration, Utc};
use insight_platform_artifacts::{ArtifactCommandLimits, ArtifactJobPayload};
use insight_platform_context::{
    ContextDatasetBuildJobPayload, ContextJobPayload, ContextQueryError as DomainContextQueryError,
    ContextQueryLimits, ContextQueryRecord, ContextQueryRequest, CreateContextQuery,
    PrepareContextDispatch,
};
use insight_platform_contracts::{
    canonical_digest, canonical_json, checked_in_hard_limit_profile, is_job_kind_work_owner_triple,
    ActiveTarget, AdministrativeGate, AgentDeploymentClosure, AgentResourceSpec, ArtifactRef,
    ArtifactRetentionPolicy, AuthoringPackage, CandidateSelectionPolicyDocument, ClosedJsonSchema,
    CommandAudit, CommandOutcome, DataClassification, DeploymentClosure, EntityLifecycle,
    ExactDatasetGenerationRef, ExactDeploymentRef, ExactSecretBindingRef, ExactVersionRef, Failure,
    FailureClass, FailureCode, FailureSource, FrozenSlotTarget, HardLimitProfile,
    InstallationPrincipalBinding, InvocationState, JobKind, JobState, JsonLimits,
    NodeExecutionState, Permission, PermissionSet, PlanNodeKind, PlatformFailureCode, PolicyKind,
    PolicyReferenceRole, PrincipalBindingState, PrincipalBindingsPayload, PrincipalKind,
    PrincipalSnapshot, PublicRunEventType, PublishedVersionPayload, RegistryResourceKind,
    ResourceDocument, ResourceDraftPayload, ResourceId, ResourceKind, Retryability,
    RunBindingsSnapshot, RunState, SandboxArtifactIoPolicyDocument, SchedulerPriority,
    SchedulingPolicyDocument, ScopeState, SecretBindingPayload, SecretPurpose, Sha256Digest,
    TenantConfig, TenantPrincipalPayload, TraceId, TraceIdentityV1, ValidationSummary, ValueRef,
    WorkClass, WorkerManifest, MAX_RESOURCE_DEPENDENCIES,
};
use insight_platform_contracts::{TypedPayload, DEFAULT_PAYLOAD_LIMIT};
use insight_platform_invocations::{
    decide_control as decide_capability_control, AdmitCapabilityInvocation, CapabilityControlKind,
    CapabilityInvocationRecord, CapabilityJobPayload, ExactInvocationValueRef,
    InvocationCommandLimits, InvocationError as DomainInvocationError, InvocationOrigin,
    InvocationValueStorage, PrepareCapabilityDispatch,
};
use insight_platform_jobs::store::MAX_JOB_LEASE_MILLISECONDS;
use insight_platform_jobs::store::{
    HeartbeatJob, JobCommandFence as JobFence, JobRecord, SafetyScanCursor, SafetyScanPage,
};
use insight_platform_jobs::{
    decide_claim as decide_job_claim, decide_claim_continuation as decide_job_claim_continuation,
    decide_expired_lease as decide_expired_job_lease, decide_heartbeat as decide_job_heartbeat,
    decide_owner_terminal as decide_job_owner_terminal, decide_resume as decide_job_resume,
    decide_retry as decide_job_retry, decide_retry_due as decide_job_retry_due,
    decide_start as decide_job_start, decide_terminal as decide_job_terminal,
    decide_wait as decide_job_wait, decide_wake as decide_job_wake, JobError as DomainJobError,
    JobFence as DomainJobFence, JobLease, JobOwnerRef, JobProjection, LeasePolicy, WakeContract,
    WakeSource,
};
use insight_platform_mcp_host::McpJobPayload;
use insight_platform_models::{
    CreateModelTurn, ModelTurnError as DomainModelTurnError, ModelTurnLimits, ModelTurnTransaction,
    PrepareModelDispatch,
};
use insight_platform_orchestrator::history::{
    PublicReplayError, PublicRunEventPage, PublicRunEventRecord, PublicRunReadPosition,
    PUBLIC_RUN_EVENT_PAGE_VERSION,
};
use insight_platform_orchestrator::store::RunRecord;
use insight_platform_orchestrator::store::*;
use insight_platform_orchestrator::{
    decide_cancel, decide_child_link_cancel, decide_child_link_terminal, decide_controller,
    decide_pause, decide_timeout, derive_candidate_selection, derive_expression_controller,
    prepare_child_run, required_expression_inputs, AdmitRun, ChildLinkState, ChildOutcome,
    ChildRunLinkPayload, ChildRunLinkProjection, CommittedExpressionInput, ControlDecision,
    ControllerDecision, ControllerEvaluation, ControllerObservation, DurableWaitKind,
    DurableWaitOutcome, ExactRunValueRef, ObserveRunTimeout, OrchestrationJobPayload,
    OrchestratorError, PrepareChildRun, RequestRunCancel, RunCurrentSnapshot, RunInputValue,
    RunStore, RunTransaction, ScopeDataEnvironmentSnapshot, ScopeEnvironmentLimits, SetRunPause,
};
use insight_platform_plan::{
    ExactDataPortRef, JoinPolicy, JoinRemainderPolicy, MapFailurePolicy, PlanLimits, PlanNodeKey,
    RuntimeNode, RuntimePlan,
};
use insight_platform_registry::{
    build_registry_validation_summary, validate_resource_draft_replacement, ActivateResource,
    CreateDeployment, CreateResourceDraft, NewPublishedVersion, PublishResourceVersions,
    RecordResourceValidation, RegistryCommandError, RegistryStore, RegistryTransaction,
    RegistryValidationJobPayload, RequestResourceValidation, SetResourceGate,
    SuspendResourceDeployment, TransitionResourceLifecycle, UpdateResourceDraft,
};
use insight_platform_sandbox::contracts::SandboxDispatcherJobPayloadV1;
use insight_platform_scheduler::{SchedulerHardLimits, TenantSchedulingPolicyBinding};
use insight_platform_security::{
    BindTenantArtifactPolicies, BindTenantPrincipal, BindTenantSchedulingPolicy,
    CreateSecretBinding as CreateSecretBindingCommand, EncryptedOpaqueReference,
    PreparedSecretBindingAuthority, PreparedSecretBindingRegistrationDisposition,
    PreparedSecretBindingRegistrationError, PreparedSecretBindingRegistrationOutcome,
    RegisterPreparedSecretBinding, RevokeSecretBinding, RevokeTenantPrincipal, RotateSecretBinding,
    SecretBindingResolutionAuthority, SecretBindingResolutionError, SecretBindingResolutionRecord,
    SecurityCommandError, SecurityStore, SecurityTransaction, UpdateTenantPrincipalPermissions,
};
use insight_platform_tasks::store::TaskRecord;
use insight_platform_tasks::{
    decide_resolution as decide_task_resolution, ResolveTask as DomainResolveTask,
    TaskError as DomainTaskError, TaskKind, TaskPayload, TaskProjection, TaskState,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use sqlx::{postgres::PgRow, Acquire, PgPool, Postgres, Row, Transaction};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    ops::Deref,
};
use uuid::{Uuid, Variant, Version};

pub(crate) fn database_timestamp(value: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(value.timestamp_micros())
        .expect("a valid DateTime always has a representable microsecond timestamp")
}

/// Starts one coherent, non-mutating authority snapshot for externally brokered reads.
///
/// The second authorization after object I/O detects later state changes, so row locks would only
/// widen contention and force the Artifact Broker's database role to own UPDATE privilege.
pub(crate) async fn begin_read_only_repeatable(
    pool: &PgPool,
) -> Result<Transaction<'_, Postgres>, RepositoryError> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *transaction)
        .await?;
    Ok(transaction)
}

pub const DEFAULT_SCHEDULER_LIMITS: SchedulerHardLimits = SchedulerHardLimits {
    maximum_deficit: 1_000_000,
    maximum_tenants: 64,
    maximum_window_per_tenant: 16,
    maximum_batch: 64,
};

#[derive(Debug)]
pub enum RepositoryError {
    Database(sqlx::Error),
    InvalidInput(String),
    NotFound(&'static str),
    Conflict(&'static str),
    StaleFence,
    LeaseExpired,
    QuotaExceeded,
    CapacityUnavailable,
    PermissionDenied,
    IdempotencyConflict,
    PublicHistoryGap { replay_floor: u64 },
    InvalidPersistedObject(insight_platform_jobs::store::SafeScanDiagnostic),
    CorruptRow(String),
}

impl fmt::Display for RepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(failure) => write!(formatter, "PostgreSQL repository failed: {failure}"),
            Self::InvalidInput(message) => write!(formatter, "invalid repository input: {message}"),
            Self::NotFound(kind) => write!(formatter, "{kind} was not found"),
            Self::Conflict(kind) => write!(formatter, "{kind} changed concurrently"),
            Self::StaleFence => formatter.write_str("job lease fence is stale"),
            Self::LeaseExpired => formatter.write_str("job lease has expired"),
            Self::QuotaExceeded => formatter.write_str("quota would be exceeded"),
            Self::CapacityUnavailable => {
                formatter.write_str("new work admission is temporarily unavailable")
            }
            Self::PermissionDenied => formatter.write_str("permission denied"),
            Self::PublicHistoryGap { replay_floor } => write!(
                formatter,
                "public Run history before sequence {replay_floor} has been retained out"
            ),
            Self::IdempotencyConflict => {
                formatter.write_str("idempotency key was used with a different request")
            }
            Self::CorruptRow(message) => write!(formatter, "repository row is invalid: {message}"),
            Self::InvalidPersistedObject(_) => formatter.write_str("persisted object is invalid"),
        }
    }
}

impl Error for RepositoryError {}

impl From<sqlx::Error> for RepositoryError {
    fn from(failure: sqlx::Error) -> Self {
        Self::Database(failure)
    }
}

impl From<RegistryCommandError> for RepositoryError {
    fn from(failure: RegistryCommandError) -> Self {
        Self::InvalidInput(failure.to_string())
    }
}

impl From<SecurityCommandError> for RepositoryError {
    fn from(failure: SecurityCommandError) -> Self {
        Self::InvalidInput(failure.to_string())
    }
}

impl From<OrchestratorError> for RepositoryError {
    fn from(failure: OrchestratorError) -> Self {
        match failure {
            OrchestratorError::StaleGeneration => Self::Conflict("run control generation"),
            OrchestratorError::ControlConflict => Self::Conflict("run control"),
            _ => Self::InvalidInput(failure.to_string()),
        }
    }
}

impl From<insight_platform_plan::PlanError> for RepositoryError {
    fn from(failure: insight_platform_plan::PlanError) -> Self {
        Self::InvalidInput(failure.to_string())
    }
}

impl From<DomainJobError> for RepositoryError {
    fn from(failure: DomainJobError) -> Self {
        match failure {
            DomainJobError::StaleFence => Self::StaleFence,
            DomainJobError::LeaseExpired => Self::LeaseExpired,
            DomainJobError::NotClaimable => Self::Conflict("job claim"),
            _ => Self::InvalidInput(failure.to_string()),
        }
    }
}

impl From<DomainTaskError> for RepositoryError {
    fn from(failure: DomainTaskError) -> Self {
        match failure {
            DomainTaskError::FirstWinnerLost => Self::Conflict("Task first-winner"),
            DomainTaskError::DeadlineExceeded | DomainTaskError::DeadlineNotReached => {
                Self::Conflict("Task deadline")
            }
            _ => Self::InvalidInput(failure.to_string()),
        }
    }
}

impl From<DomainInvocationError> for RepositoryError {
    fn from(failure: DomainInvocationError) -> Self {
        match failure {
            DomainInvocationError::FirstWinnerLost => {
                Self::Conflict("CapabilityInvocation first-winner")
            }
            DomainInvocationError::AdmissionRejected => {
                Self::Conflict("CapabilityInvocation admission")
            }
            _ => Self::InvalidInput(failure.to_string()),
        }
    }
}

impl From<DomainModelTurnError> for RepositoryError {
    fn from(failure: DomainModelTurnError) -> Self {
        match failure {
            DomainModelTurnError::FirstWinnerLost => Self::Conflict("ModelTurn first-winner"),
            DomainModelTurnError::AdmissionRejected => Self::Conflict("ModelTurn admission"),
            DomainModelTurnError::UsageCeilingExceeded => Self::QuotaExceeded,
            _ => Self::InvalidInput(failure.to_string()),
        }
    }
}

impl From<DomainContextQueryError> for RepositoryError {
    fn from(failure: DomainContextQueryError) -> Self {
        match failure {
            DomainContextQueryError::FirstWinnerLost => Self::Conflict("ContextQuery first-winner"),
            DomainContextQueryError::AdmissionRejected => Self::Conflict("ContextQuery admission"),
            DomainContextQueryError::StaleFence => Self::StaleFence,
            _ => Self::InvalidInput(failure.to_string()),
        }
    }
}

#[derive(Clone)]
pub struct PgRepository {
    pub(crate) outbox_admission_backlog: u32,
    pool: PgPool,
    artifact_limits: ArtifactCommandLimits,
    invocation_limits: InvocationCommandLimits,
    context_query_limits: ContextQueryLimits,
    model_turn_limits: ModelTurnLimits,
    scheduler_limits: SchedulerHardLimits,
    plan_limits: PlanLimits,
    scope_environment_limits: ScopeEnvironmentLimits,
    expression_inline_limits: JsonLimits,
    recovery_batch_limit: u16,
    recovery_shard_limit: u16,
}

#[derive(Debug, Clone)]
pub struct ResolvedRootRunTarget {
    pub agent: ExactDeploymentRef,
    pub closure: insight_platform_contracts::AgentDeploymentClosure,
    pub context_dataset_views: Vec<insight_platform_contracts::RunContextDatasetView>,
}

impl PgRepository {
    pub fn new(pool: PgPool) -> Self {
        Self::with_hard_limit_profile(pool, &checked_in_hard_limit_profile())
            .expect("checked-in HardLimitProfile must construct repository limits")
    }

    pub fn with_hard_limit_profile(
        pool: PgPool,
        profile: &HardLimitProfile,
    ) -> Result<Self, RepositoryError> {
        profile
            .validate()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let maximum_batch =
            u16::try_from(profile.run_scheduler.claim_batch.q1_default).map_err(|_| {
                RepositoryError::InvalidInput(
                    "run_scheduler.claim_batch exceeds scheduler representation".to_owned(),
                )
            })?;
        let scheduler_limits = SchedulerHardLimits {
            maximum_batch,
            ..DEFAULT_SCHEDULER_LIMITS
        };
        scheduler_limits
            .validate()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let recovery_batch_limit = u16::try_from(profile.control_data.recovery_batch.q1_default)
            .map_err(|_| {
                RepositoryError::InvalidInput(
                    "control_data.recovery_batch exceeds repository representation".to_owned(),
                )
            })?;
        let recovery_shard_limit = u16::try_from(profile.control_data.recovery_shards.hard_max)
            .map_err(|_| {
                RepositoryError::InvalidInput(
                    "control_data.recovery_shards exceeds repository representation".to_owned(),
                )
            })?;
        Ok(Self {
            outbox_admission_backlog: u32::try_from(
                profile.control_data.outbox_admission_backlog.q1_default,
            )
            .map_err(|_| RepositoryError::InvalidInput("Outbox admission threshold".to_owned()))?,
            pool,
            artifact_limits: ArtifactCommandLimits::from_profile(profile)
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
            invocation_limits: InvocationCommandLimits::from_profile(profile)
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
            context_query_limits: ContextQueryLimits::from_profile(profile)
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
            model_turn_limits: ModelTurnLimits::from_profile(profile)
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
            scheduler_limits,
            plan_limits: PlanLimits::from_profile(profile)
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
            scope_environment_limits: ScopeEnvironmentLimits::from_profile(profile)
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
            expression_inline_limits: JsonLimits {
                max_bytes: usize::try_from(profile.run_scheduler.inline_value_bytes.q1_default)
                    .map_err(|_| RepositoryError::InvalidInput("inline value limit".to_owned()))?,
                max_depth: usize::try_from(profile.api.json_depth.q1_default)
                    .map_err(|_| RepositoryError::InvalidInput("JSON depth limit".to_owned()))?,
                max_properties_per_object: usize::try_from(profile.api.json_properties.q1_default)
                    .map_err(|_| RepositoryError::InvalidInput("JSON property limit".to_owned()))?,
                max_items_per_array: usize::try_from(profile.api.json_items.q1_default)
                    .map_err(|_| RepositoryError::InvalidInput("JSON item limit".to_owned()))?,
                max_string_bytes: usize::try_from(
                    profile.run_scheduler.inline_value_bytes.q1_default,
                )
                .map_err(|_| RepositoryError::InvalidInput("inline string limit".to_owned()))?,
            },
            recovery_batch_limit,
            recovery_shard_limit,
        })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub(crate) const fn artifact_limits(&self) -> ArtifactCommandLimits {
        self.artifact_limits
    }

    pub(crate) const fn recovery_batch_limit(&self) -> u16 {
        self.recovery_batch_limit
    }

    pub(crate) const fn recovery_shard_limit(&self) -> u16 {
        self.recovery_shard_limit
    }

    pub(crate) const fn invocation_limits(&self) -> InvocationCommandLimits {
        self.invocation_limits
    }

    pub(crate) const fn model_turn_limits(&self) -> ModelTurnLimits {
        self.model_turn_limits
    }

    pub(crate) const fn context_query_limits(&self) -> ContextQueryLimits {
        self.context_query_limits
    }

    pub(crate) const fn scope_environment_limits(&self) -> ScopeEnvironmentLimits {
        self.scope_environment_limits
    }

    pub async fn begin_registry_transaction(
        &self,
    ) -> Result<PgRegistryTransaction, RepositoryError> {
        Ok(PgRegistryTransaction {
            outbox_admission_backlog: self.outbox_admission_backlog,
            transaction: self.pool.begin().await?,
        })
    }

    pub async fn begin_security_transaction(
        &self,
    ) -> Result<PgSecurityTransaction, RepositoryError> {
        Ok(PgSecurityTransaction {
            transaction: self.pool.begin().await?,
        })
    }

    pub async fn begin_run_transaction(&self) -> Result<PgRunTransaction, RepositoryError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .execute(&mut *transaction)
            .await?;
        Ok(PgRunTransaction {
            outbox_admission_backlog: self.outbox_admission_backlog,
            transaction,
            scope_environment_limits: self.scope_environment_limits,
        })
    }

    pub async fn begin_scheduler_transaction(
        &self,
    ) -> Result<PgSchedulerTransaction, RepositoryError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .execute(&mut *transaction)
            .await?;
        Ok(PgSchedulerTransaction {
            transaction,
            orchestration_partition_hints: None,
            limits: self.scheduler_limits,
            invocation_limits: self.invocation_limits,
            context_query_limits: self.context_query_limits,
            model_turn_limits: self.model_turn_limits,
            plan_limits: self.plan_limits,
            scope_environment_limits: self.scope_environment_limits,
            expression_inline_limits: self.expression_inline_limits,
            recovery_batch_limit: self.recovery_batch_limit,
            recovery_shard_limit: self.recovery_shard_limit,
        })
    }

    /// Observe only physical dispatch hints before taking a SERIALIZABLE snapshot.
    /// No claim, credit, quota or owner mutation occurs before the returned transaction.
    pub async fn begin_orchestration_claim_transaction(
        &self,
    ) -> Result<PgSchedulerTransaction, RepositoryError> {
        let hints = crate::partition_scheduler::available_partition_hints(
            &self.pool,
            WorkClass::Orchestration,
        )
        .await?;
        let mut transaction = self.begin_scheduler_transaction().await?;
        transaction.orchestration_partition_hints = Some(hints);
        Ok(transaction)
    }
}

/// Replays the development bootstrap only when every immutable/configuration root still matches
/// the exact closed input. Runtime-created rows and mutable quota usage are deliberately ignored;
/// they are ordinary authority state and must survive a local process restart.
struct DevelopmentReplayEvidence<'a> {
    installation_payload: &'a TypedPayload,
    installation_event: &'a TypedPayload,
    tenant_config: &'a TypedPayload,
    developer_payload: &'a TypedPayload,
    service_principal_payloads: &'a [TypedPayload],
    tenant_bindings: &'a [TypedPayload],
    artifact_authority: Option<&'a DevelopmentArtifactAuthorityMaterial>,
}

async fn verify_development_profile_replay(
    transaction: &mut Transaction<'_, Postgres>,
    command: &BootstrapDevelopmentProfile,
    evidence: DevelopmentReplayEvidence<'_>,
) -> Result<(), RepositoryError> {
    for (principal, payload) in [(&command.installation, evidence.installation_payload)] {
        let exact: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM insight_platform.principals
                WHERE principal_id = $1 AND state = 'active'
                  AND authentication_authority_digest = $2 AND subject_digest = $3
                  AND payload_schema_version = $4 AND payload_digest = $5
            )
            "#,
        )
        .bind(principal.principal_id.to_string())
        .bind(principal.authentication_authority_digest.to_string())
        .bind(principal.subject_digest.to_string())
        .bind(payload.schema_version)
        .bind(&payload.digest)
        .fetch_one(&mut **transaction)
        .await?;
        if !exact {
            return Err(RepositoryError::Conflict(
                "development bootstrap installation principal",
            ));
        }
    }
    for (principal, payload) in std::iter::once((&command.developer, evidence.developer_payload))
        .chain(
            command
                .service_principals
                .iter()
                .zip(evidence.service_principal_payloads.iter()),
        )
    {
        let exact: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM insight_platform.principals
                WHERE principal_id = $1 AND state = 'active'
                  AND authentication_authority_digest = $2 AND subject_digest = $3
                  AND payload_schema_version = $4 AND payload_digest = $5
            )
            "#,
        )
        .bind(principal.principal_id.to_string())
        .bind(principal.authentication_authority_digest.to_string())
        .bind(principal.subject_digest.to_string())
        .bind(payload.schema_version)
        .bind(&payload.digest)
        .fetch_one(&mut **transaction)
        .await?;
        if !exact {
            return Err(RepositoryError::Conflict(
                "development bootstrap tenant principal",
            ));
        }
    }

    let event_id = format!(
        "evt_{}",
        command.installation.request_id.uuid().hyphenated()
    );
    let exact_event: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM insight_platform.events
            WHERE tenant_id IS NULL AND event_id = $1 AND aggregate_kind = 'principal'
              AND aggregate_id = $2 AND aggregate_version = 1
              AND event_type = 'installation.bootstrap' AND visibility = 'internal'
              AND payload_schema_version = $3 AND payload_digest = $4
        )
        "#,
    )
    .bind(event_id)
    .bind(command.installation.principal_id.to_string())
    .bind(evidence.installation_event.schema_version)
    .bind(&evidence.installation_event.digest)
    .fetch_one(&mut **transaction)
    .await?;
    if !exact_event {
        return Err(RepositoryError::Conflict("development bootstrap event"));
    }

    let tenant_id: ResourceId = command.tenant.tenant_id.parse().map_err(
        |error: insight_platform_contracts::ResourceIdError| {
            RepositoryError::InvalidInput(error.to_string())
        },
    )?;
    let current_tenant = load_tenant(transaction, &tenant_id).await?;
    let expected_config: TenantConfig =
        decode_typed_payload(evidence.tenant_config, "development Tenant configuration")?;
    let mut immutable_config = current_tenant.config.clone();
    // An explicit later Scheduling binding is mutable domain authority, not seed drift.
    immutable_config.scheduling_policy = expected_config.scheduling_policy.clone();
    // An authoring default is later mutable Tenant state, never an initialization repair target.
    immutable_config.default_model = expected_config.default_model.clone();
    let immutable_payload = TypedPayload::with_limit(1, &immutable_config, 65_536)?;
    if current_tenant.state != command.tenant.state
        || immutable_payload.digest != evidence.tenant_config.digest
        || evidence.tenant_bindings.len() != command.tenant_principal_bindings.len()
    {
        return Err(RepositoryError::Conflict("development bootstrap tenant"));
    }
    let current_identity = match current_tenant.config.scheduling_policy.as_ref() {
        Some(exact) => Some(
            security_commands::load_tenant_scheduler_policy_identity(
                transaction,
                &tenant_id,
                exact,
            )
            .await?,
        ),
        None if expected_config.scheduling_policy.is_none() => None,
        None => {
            return Err(RepositoryError::Conflict(
                "development bootstrap Scheduling binding",
            ))
        }
    };
    let fairness = sqlx::query("SELECT policy_version_id,policy_version_digest,rules_digest FROM insight_platform.scheduler_tenant_state WHERE tenant_id=$1 ORDER BY work_class")
        .bind(tenant_id.to_string()).fetch_all(&mut **transaction).await?;
    if fairness.len() != WorkClass::ALL.len() {
        return Err(RepositoryError::Conflict(
            "development bootstrap fairness enrollment",
        ));
    }
    for row in fairness {
        let actual = (
            row.try_get::<Option<String>, _>("policy_version_id")?,
            row.try_get::<Option<String>, _>("policy_version_digest")?,
            row.try_get::<Option<String>, _>("rules_digest")?,
        );
        let expected = current_identity
            .as_ref()
            .map(|(revision, rules)| {
                (
                    Some(revision.revision_id.to_string()),
                    Some(revision.semantic_digest.to_string()),
                    Some(rules.to_string()),
                )
            })
            .unwrap_or((None, None, None));
        if actual != expected {
            return Err(RepositoryError::Conflict(
                "development bootstrap fairness binding",
            ));
        }
    }

    for (binding, payload) in command
        .tenant_principal_bindings
        .iter()
        .zip(evidence.tenant_bindings.iter())
    {
        let exact: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM insight_platform.tenant_principals
                WHERE tenant_id = $1 AND principal_id = $2 AND principal_kind = $3
                  AND state = 'active' AND permissions_schema_version = $4
                  AND permissions_digest = $5
            )
            "#,
        )
        .bind(binding.tenant_id.to_string())
        .bind(binding.principal_id.to_string())
        .bind(binding.principal_kind.as_str())
        .bind(payload.schema_version)
        .bind(&payload.digest)
        .fetch_one(&mut **transaction)
        .await?;
        if !exact {
            return Err(RepositoryError::Conflict(
                "development bootstrap tenant binding",
            ));
        }
    }

    let (Some(seed), Some(material)) = (&command.artifact_authority, evidence.artifact_authority)
    else {
        return if command.artifact_authority.is_none() && evidence.artifact_authority.is_none() {
            Ok(())
        } else {
            Err(RepositoryError::Conflict(
                "development bootstrap artifact configuration",
            ))
        };
    };
    for (resource_id, deployment_id, payload) in [
        (
            &seed.retention_policy_id,
            &seed.retention_policy_deployment_id,
            &material.retention_resource,
        ),
        (
            &seed.artifact_io_policy_id,
            &seed.artifact_io_policy_deployment_id,
            &material.artifact_io_resource,
        ),
        (
            &seed.scheduling_policy_id,
            &seed.scheduling_policy_deployment_id,
            &material.scheduling_resource,
        ),
    ] {
        let exact: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM insight_platform.resources
                WHERE tenant_id = $1 AND resource_id = $2 AND resource_kind = 'policy'
                  AND lifecycle_state = 'active' AND gate_state = 'enabled'
                  AND active_version_id IS NULL AND active_deployment_id = $3
                  AND payload_schema_version = $4 AND payload_digest = $5
            )
            "#,
        )
        .bind(&command.tenant.tenant_id)
        .bind(resource_id.to_string())
        .bind(deployment_id.to_string())
        .bind(payload.schema_version)
        .bind(&payload.digest)
        .fetch_one(&mut **transaction)
        .await?;
        if !exact {
            return Err(RepositoryError::Conflict(
                "development bootstrap artifact policy resource",
            ));
        }
    }
    for (resource_id, revision_id, payload) in [
        (
            &seed.retention_policy_id,
            &seed.retention_policy_revision_id,
            &material.retention_version,
        ),
        (
            &seed.artifact_io_policy_id,
            &seed.artifact_io_policy_revision_id,
            &material.artifact_io_version,
        ),
        (
            &seed.scheduling_policy_id,
            &seed.scheduling_policy_revision_id,
            &material.scheduling_version,
        ),
    ] {
        let exact: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM insight_platform.resource_versions
                WHERE tenant_id = $1 AND resource_version_id = $2 AND resource_id = $3
                  AND resource_version_kind = 'policy_revision' AND revision_no = 1
                  AND content_digest = $4 AND artifact_id = $5
                  AND payload_schema_version = $6 AND payload_digest = $4
            )
            "#,
        )
        .bind(&command.tenant.tenant_id)
        .bind(revision_id.to_string())
        .bind(resource_id.to_string())
        .bind(&payload.digest)
        .bind(seed.authoring_artifact_id.to_string())
        .bind(payload.schema_version)
        .fetch_one(&mut **transaction)
        .await?;
        if !exact {
            return Err(RepositoryError::Conflict(
                "development bootstrap artifact policy version",
            ));
        }
    }
    for (resource_id, revision_id, deployment_id, payload) in [
        (
            &seed.retention_policy_id,
            &seed.retention_policy_revision_id,
            &seed.retention_policy_deployment_id,
            &material.retention_deployment,
        ),
        (
            &seed.artifact_io_policy_id,
            &seed.artifact_io_policy_revision_id,
            &seed.artifact_io_policy_deployment_id,
            &material.artifact_io_deployment,
        ),
        (
            &seed.scheduling_policy_id,
            &seed.scheduling_policy_revision_id,
            &seed.scheduling_policy_deployment_id,
            &material.scheduling_deployment,
        ),
    ] {
        let exact: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM insight_platform.deployments
                WHERE tenant_id = $1 AND deployment_id = $2 AND resource_id = $3
                  AND resource_version_id = $4 AND environment = 'local'
                  AND bindings_digest = $5 AND payload_schema_version = $6
            )
            "#,
        )
        .bind(&command.tenant.tenant_id)
        .bind(deployment_id.to_string())
        .bind(resource_id.to_string())
        .bind(revision_id.to_string())
        .bind(&payload.digest)
        .bind(payload.schema_version)
        .fetch_one(&mut **transaction)
        .await?;
        if !exact {
            return Err(RepositoryError::Conflict(
                "development bootstrap artifact policy deployment",
            ));
        }
    }

    let exact_blob: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM insight_platform.artifact_blobs
            WHERE tenant_id = $1 AND blob_id = $2 AND backend = 'builtin'
              AND storage_binding_digest = $3 AND security_domain_digest = $4
              AND object_generation = 'builtin-v1' AND key_id = 'builtin-development'
              AND encryption_domain_id = $5 AND content_digest = $6
              AND size_bytes = $7 AND object_reference_ciphertext = $8
              AND state = 'verified' AND verified_at IS NOT NULL
        )
        "#,
    )
    .bind(&command.tenant.tenant_id)
    .bind(seed.authoring_blob_id.to_string())
    .bind(
        seed.artifact_io_policy
            .write_storage_binding_digest
            .to_string(),
    )
    .bind(material.security_domain_digest.to_string())
    .bind(seed.artifact_io_policy.encryption_domain_id.to_string())
    .bind(material.authoring_content_digest.to_string())
    .bind(material.authoring_size_bytes)
    .bind(
        canonical_json(&serde_json::json!({
            "kind": "builtin-development-authority",
            "tenant_id": command.tenant.tenant_id,
        }))
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
    )
    .fetch_one(&mut **transaction)
    .await?;
    let exact_artifact: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM insight_platform.artifacts
            WHERE tenant_id = $1 AND artifact_id = $2 AND blob_id = $3
              AND purpose = 'authoring_document' AND classification = 'internal'
              AND expected_size_bytes = $4 AND expected_digest = $5
              AND declared_media_type = 'application/json'
              AND verified_media_type = 'application/json' AND state = 'ready'
              AND metadata_schema_version = $6 AND metadata_digest = $7
              AND retention_policy_revision_id = $8 AND created_by = $9
        )
        "#,
    )
    .bind(&command.tenant.tenant_id)
    .bind(seed.authoring_artifact_id.to_string())
    .bind(seed.authoring_blob_id.to_string())
    .bind(material.authoring_size_bytes)
    .bind(material.authoring_content_digest.to_string())
    .bind(material.authoring_metadata.schema_version)
    .bind(&material.authoring_metadata.digest)
    .bind(seed.retention_policy_revision_id.to_string())
    .bind(command.developer.principal_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if !exact_blob || !exact_artifact {
        return Err(RepositoryError::Conflict(
            "development bootstrap builtin artifact",
        ));
    }
    for (quota_id, work_class, metric, limit) in [
        (
            &seed.staging_quota_account_id,
            "artifact",
            "artifact.staging_bytes",
            seed.staging_quota_bytes,
        ),
        (
            &seed.orchestration_quota_account_id,
            "orchestration",
            "concurrent_jobs",
            seed.orchestration_concurrent_jobs,
        ),
    ] {
        let exact: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM insight_platform.quota_accounts
                WHERE tenant_id = $1 AND quota_account_id = $2
                  AND scope_kind = 'tenant' AND scope_id = $1
                  AND work_class = $3 AND metric = $4 AND limit_value = $5
                  AND payload_schema_version = $6 AND payload_digest = $7
            )
            "#,
        )
        .bind(&command.tenant.tenant_id)
        .bind(quota_id.to_string())
        .bind(work_class)
        .bind(metric)
        .bind(limit)
        .bind(material.quota_payload.schema_version)
        .bind(&material.quota_payload.digest)
        .fetch_one(&mut **transaction)
        .await?;
        if !exact {
            return Err(RepositoryError::Conflict(
                "development bootstrap quota root",
            ));
        }
    }
    Ok(())
}

pub struct PgRegistryTransaction {
    pub(crate) outbox_admission_backlog: u32,
    pub(crate) transaction: Transaction<'static, Postgres>,
}

impl PgRepository {}

pub struct PgSecurityTransaction {
    transaction: Transaction<'static, Postgres>,
}

impl SecurityStore for PgRepository {
    type Error = RepositoryError;
    type Transaction<'a> = PgSecurityTransaction;

    async fn begin(&self) -> Result<Self::Transaction<'_>, Self::Error> {
        self.begin_security_transaction().await
    }
}

#[async_trait::async_trait]
impl SecretBindingResolutionAuthority for PgRepository {
    async fn load_for_resolution(
        &self,
        tenant_id: &ResourceId,
        secret_binding_id: &ResourceId,
    ) -> Result<SecretBindingResolutionRecord, SecretBindingResolutionError> {
        if tenant_id.kind() != ResourceKind::Tenant
            || secret_binding_id.kind() != ResourceKind::SecretBinding
        {
            return Err(SecretBindingResolutionError::NotFound);
        }
        let row = sqlx::query(
            r#"
            SELECT tenant_id, secret_binding_id, purpose, provider, state, generation,
                   opaque_reference_ciphertext, key_id, reference_digest,
                   payload_schema_version, payload, payload_digest
            FROM insight_platform.secret_bindings
            WHERE tenant_id = $1 AND secret_binding_id = $2
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(secret_binding_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| SecretBindingResolutionError::Unavailable)?
        .ok_or(SecretBindingResolutionError::NotFound)?;
        secret_binding_resolution_from_row(row)
            .map_err(|_| SecretBindingResolutionError::InvalidEvidence)
    }
}

impl PgRepository {
    async fn register_prepared_secret_binding(
        &self,
        command: RegisterPreparedSecretBinding,
    ) -> Result<PreparedSecretBindingRegistrationOutcome, RepositoryError> {
        command.validate_at(Utc::now())?;
        let exact_binding = command.exact_binding()?;
        let mut transaction = self.pool.begin().await?;
        require_tenant_permission(&mut transaction, &command.audit, Permission::SecretBind).await?;
        if let Some(identity) = &command.delegated_import {
            model_credential_import::require_import_principal(&mut transaction, identity).await?;
        }
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "secret_binding",
            &command.secret_binding_id.to_string(),
            "security.prepared_secret_binding.register",
        )
        .await?
        {
            // The Receipt row above serializes an exact preparation replay. A different
            // preparation cannot pass the SecretBinding primary key insert below. Avoiding a
            // redundant row lock lets the dedicated Security Authority retain SELECT+INSERT-only
            // privilege on SecretBinding current state.
            let current = load_secret_binding_resolution_in_transaction(
                &mut transaction,
                &command.audit.tenant_id,
                &command.secret_binding_id,
            )
            .await?;
            validate_registered_prepared_binding(&current, &command)?;
            transaction.commit().await?;
            return Ok(PreparedSecretBindingRegistrationOutcome {
                disposition: PreparedSecretBindingRegistrationDisposition::Replayed,
                exact_binding,
            });
        }
        let payload = SecretBindingPayload {
            provider_id: command.provider_id.clone(),
            resolution_policy: insight_platform_contracts::SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: command.opaque_version_identity_digest.clone(),
            },
        };
        let payload = TypedPayload::with_limit(1, &payload, 65_536)?;
        let row = sqlx::query(
            r#"
            INSERT INTO insight_platform.secret_bindings (
                tenant_id, secret_binding_id, purpose, provider, state,
                opaque_reference_ciphertext, key_id, reference_digest,
                payload_schema_version, payload, payload_digest
            ) VALUES ($1, $2, $3, $4, 'active', $5, $6, $7, $8, $9, $10)
            ON CONFLICT (tenant_id, secret_binding_id) DO NOTHING
            RETURNING tenant_id, secret_binding_id, purpose, provider, state, generation, version,
                      opaque_reference_ciphertext, key_id, reference_digest,
                      payload_schema_version, payload, payload_digest
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.secret_binding_id.to_string())
        .bind(command.purpose.as_str())
        .bind(command.provider_id.to_string())
        .bind(command.encrypted_reference.as_bytes())
        .bind(&command.key_id)
        .bind(command.reference_digest.to_string())
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(row) = row else {
            // A current exact Binding remains authoritative after the original Receipt expires.
            // Never replace its ciphertext or reopen a revoked Binding on an import retry.
            if command.delegated_import.is_none() {
                return Err(RepositoryError::Conflict("secret binding"));
            }
            let current = load_secret_binding_resolution_in_transaction(
                &mut transaction,
                &command.audit.tenant_id,
                &command.secret_binding_id,
            )
            .await?;
            validate_registered_prepared_binding(&current, &command)?;
            terminalize_command_receipt(
                &mut transaction,
                &command.audit,
                &command.secret_binding_id.to_string(),
                "registered",
            )
            .await?;
            transaction.commit().await?;
            return Ok(PreparedSecretBindingRegistrationOutcome {
                disposition: PreparedSecretBindingRegistrationDisposition::Replayed,
                exact_binding,
            });
        };
        let aggregate_version: i64 = row.try_get("version")?;
        let current = secret_binding_resolution_from_row(row)?;
        validate_registered_prepared_binding(&current, &command)?;
        let secret_binding_id = command.secret_binding_id.to_string();
        append_command_event(
            &mut transaction,
            &command.audit,
            "secret_binding",
            &secret_binding_id,
            aggregate_version,
            "security.prepared_secret_binding_registered",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "generation": current.generation,
                    "preparation_digest": command.preparation_digest,
                    "delegated_import": command.delegated_import,
                    "provider_id": command.provider_id,
                    "provider_storage_evidence_digest": command.provider_storage_evidence_digest,
                    "purpose": command.purpose,
                    "state": current.state,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &secret_binding_id,
            "registered",
        )
        .await?;
        transaction.commit().await?;
        Ok(PreparedSecretBindingRegistrationOutcome {
            disposition: PreparedSecretBindingRegistrationDisposition::Applied,
            exact_binding,
        })
    }
}

#[async_trait::async_trait]
impl PreparedSecretBindingAuthority for PgRepository {
    async fn register_prepared(
        &self,
        command: RegisterPreparedSecretBinding,
    ) -> Result<PreparedSecretBindingRegistrationOutcome, PreparedSecretBindingRegistrationError>
    {
        self.register_prepared_secret_binding(command)
            .await
            .map_err(map_prepared_secret_binding_registration_error)
    }
}

fn map_prepared_secret_binding_registration_error(
    failure: RepositoryError,
) -> PreparedSecretBindingRegistrationError {
    match failure {
        RepositoryError::Database(_) => {
            PreparedSecretBindingRegistrationError::TemporarilyUnavailable
        }
        _ => PreparedSecretBindingRegistrationError::Rejected,
    }
}

pub struct PgSchedulerTransaction {
    transaction: Transaction<'static, Postgres>,
    orchestration_partition_hints:
        Option<std::collections::VecDeque<insight_platform_contracts::SchedulerPartitionId>>,
    limits: SchedulerHardLimits,
    invocation_limits: InvocationCommandLimits,
    context_query_limits: ContextQueryLimits,
    model_turn_limits: ModelTurnLimits,
    plan_limits: PlanLimits,
    scope_environment_limits: ScopeEnvironmentLimits,
    expression_inline_limits: JsonLimits,
    recovery_batch_limit: u16,
    recovery_shard_limit: u16,
}

impl PgSchedulerTransaction {
    pub async fn commit(self) -> Result<(), RepositoryError> {
        self.transaction.commit().await?;
        Ok(())
    }

    pub async fn rollback(self) -> Result<(), RepositoryError> {
        self.transaction.rollback().await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct LockedOrchestrationJobParents {
    run: RunRecord,
    node_id: String,
    node_version: i64,
    scope_id: String,
    scope_version: i64,
}

pub(crate) async fn load_job_by_text(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    job_id: &str,
) -> Result<JobRecord, RepositoryError> {
    let row =
        sqlx::query("SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2")
            .bind(tenant_id)
            .bind(job_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(RepositoryError::NotFound("job"))?;
    persisted_job_from_row(row)
}

fn require_orchestration_job(job: &JobRecord) -> Result<(), RepositoryError> {
    crate::recovery_isolation::job(
        require_orchestration_job_inner(job),
        job,
        insight_platform_jobs::store::SafetyScanPhase::OwnerValidation,
    )
}
fn require_orchestration_job_inner(job: &JobRecord) -> Result<(), RepositoryError> {
    if job.work_class != WorkClass::Orchestration.as_str()
        || job.owner_kind != ResourceKind::NodeExecution.descriptor().name
        || job.run_id.is_none()
        || job.node_id.is_none()
        || job.owner_id != job.node_id.as_deref().unwrap_or_default()
    {
        return Err(RepositoryError::InvalidInput(
            "Job is not an orchestration Node work item".to_owned(),
        ));
    }
    let payload = decode_orchestration_job_payload(&job.payload)?;
    if payload.node_execution_id.to_string() != job.owner_id {
        return Err(RepositoryError::CorruptRow(
            "orchestration Job payload owner".to_owned(),
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn claim_job_mutation_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    fence: &JobFence,
    receipt_id: &ResourceId,
    operation: &str,
    idempotency_key_digest: &Sha256Digest,
    request_digest: &Sha256Digest,
    payload: &TypedPayload,
    expires_at: DateTime<Utc>,
) -> Result<bool, RepositoryError> {
    let inserted = sqlx::query(
        r#"
        INSERT INTO insight_platform.receipts (
            tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
            dedupe_owner_id, operation, idempotency_key_digest, request_digest, state,
            payload_schema_version, payload, payload_digest, expires_at
        ) VALUES ($1, $2, 'job_commit', 'job', $3, $4, $5, $6, $7,
                  'processing', $8, $9, $10, $11)
        ON CONFLICT (
            tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
            operation, idempotency_key_digest
        ) DO NOTHING
        RETURNING receipt_id
        "#,
    )
    .bind(&fence.tenant_id)
    .bind(receipt_id.to_string())
    .bind(&fence.job_id)
    .bind(fence.worker_id.to_string())
    .bind(operation)
    .bind(idempotency_key_digest.to_string())
    .bind(request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(expires_at)
    .fetch_optional(&mut **transaction)
    .await?;
    if inserted.is_some() {
        return Ok(false);
    }
    let row = sqlx::query(
        r#"
        SELECT receipt_id, request_digest, state, payload_digest
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'job_commit'
          AND scope_kind = 'job' AND scope_id = $2 AND dedupe_owner_id = $3
          AND operation = $4 AND idempotency_key_digest = $5
        FOR UPDATE
        "#,
    )
    .bind(&fence.tenant_id)
    .bind(&fence.job_id)
    .bind(fence.worker_id.to_string())
    .bind(operation)
    .bind(idempotency_key_digest.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if row.try_get::<String, _>("request_digest")? != request_digest.to_string() {
        return Err(RepositoryError::IdempotencyConflict);
    }
    if row.try_get::<String, _>("payload_digest")? != payload.digest {
        return Err(RepositoryError::IdempotencyConflict);
    }
    if row.try_get::<String, _>("state")? != "succeeded" {
        return Err(RepositoryError::Conflict("Job mutation receipt"));
    }
    Ok(true)
}

async fn terminalize_job_mutation_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    fence: &JobFence,
    receipt_id: &ResourceId,
    request_digest: &Sha256Digest,
    disposition: &str,
) -> Result<(), RepositoryError> {
    terminalize_job_mutation_receipt_with_reference(
        transaction,
        fence,
        receipt_id,
        request_digest,
        disposition,
        &fence.job_id,
    )
    .await
}

async fn terminalize_job_mutation_receipt_with_reference(
    transaction: &mut Transaction<'_, Postgres>,
    fence: &JobFence,
    receipt_id: &ResourceId,
    request_digest: &Sha256Digest,
    disposition: &str,
    response_reference_id: &str,
) -> Result<(), RepositoryError> {
    let updated = sqlx::query(
        r#"
        UPDATE insight_platform.receipts
        SET state = 'succeeded', disposition = $4, response_reference_id = $5,
            completed_at = clock_timestamp()
        WHERE tenant_id = $1 AND receipt_id = $2 AND request_digest = $3
          AND state = 'processing'
        "#,
    )
    .bind(&fence.tenant_id)
    .bind(receipt_id.to_string())
    .bind(request_digest.to_string())
    .bind(disposition)
    .bind(response_reference_id)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if updated != 1 {
        return Err(RepositoryError::Conflict("Job mutation receipt"));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_registry_validation_event(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    event_id: &ResourceId,
    aggregate_kind: &str,
    aggregate_id: &str,
    aggregate_version: i64,
    trace: &TraceIdentityV1,
    event_type: &str,
    payload: &TypedPayload,
) -> Result<(), RepositoryError> {
    sqlx::query(
        r#"
        INSERT INTO insight_platform.events (
            tenant_id, event_id, aggregate_kind, aggregate_id, aggregate_version,
            trace_id, event_type, visibility, payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'internal', $8, $9, $10)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(event_id.to_string())
    .bind(aggregate_kind)
    .bind(aggregate_id)
    .bind(aggregate_version)
    .bind(trace.trace_id.to_string())
    .bind(event_type)
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_registry_validation_outbox(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    outbox_id: &ResourceId,
    event_id: &ResourceId,
    trace: &TraceIdentityV1,
) -> Result<(), RepositoryError> {
    sqlx::query(
        r#"
        INSERT INTO insight_platform.outbox_events (tenant_id, outbox_id, event_id, trace_id)
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(outbox_id.to_string())
    .bind(event_id.to_string())
    .bind(trace.trace_id.to_string())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn claim_job_wake_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &WakeOrchestrationJob,
    payload: &TypedPayload,
) -> Result<bool, RepositoryError> {
    let tenant_id = command.tenant_id.to_string();
    let job_id = command.job_id.to_string();
    let operation = format!("orchestration.job.wake.{}", command.source.as_str());
    let inserted = sqlx::query(
        r#"
        INSERT INTO insight_platform.receipts (
            tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
            dedupe_owner_id, operation, idempotency_key_digest, request_digest, state,
            payload_schema_version, payload, payload_digest, expires_at
        ) VALUES ($1, $2, 'job_commit', 'job', $3, $3, $4, $5, $6,
                  'processing', $7, $8, $9, $10)
        ON CONFLICT (
            tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
            operation, idempotency_key_digest
        ) DO NOTHING
        RETURNING receipt_id
        "#,
    )
    .bind(&tenant_id)
    .bind(command.mutations.receipt_id.to_string())
    .bind(&job_id)
    .bind(&operation)
    .bind(command.idempotency_key_digest.to_string())
    .bind(command.request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(command.receipt_expires_at)
    .fetch_optional(&mut **transaction)
    .await?;
    if inserted.is_some() {
        return Ok(false);
    }
    let row = sqlx::query(
        r#"
        SELECT request_digest, state FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'job_commit'
          AND scope_kind = 'job' AND scope_id = $2 AND dedupe_owner_id = $2
          AND operation = $3 AND idempotency_key_digest = $4
        FOR UPDATE
        "#,
    )
    .bind(&tenant_id)
    .bind(&job_id)
    .bind(&operation)
    .bind(command.idempotency_key_digest.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if row.try_get::<String, _>("request_digest")? != command.request_digest.to_string() {
        return Err(RepositoryError::IdempotencyConflict);
    }
    if row.try_get::<String, _>("state")? != "succeeded" {
        return Err(RepositoryError::Conflict("Job wake receipt"));
    }
    Ok(true)
}

async fn terminalize_job_wake_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    command: &WakeOrchestrationJob,
    disposition: &str,
) -> Result<(), RepositoryError> {
    let updated = sqlx::query(
        r#"
        UPDATE insight_platform.receipts
        SET state = 'succeeded', disposition = $4, response_reference_id = $5,
            completed_at = clock_timestamp()
        WHERE tenant_id = $1 AND receipt_id = $2 AND request_digest = $3
          AND state = 'processing'
        "#,
    )
    .bind(command.tenant_id.to_string())
    .bind(command.mutations.receipt_id.to_string())
    .bind(command.request_digest.to_string())
    .bind(disposition)
    .bind(command.job_id.to_string())
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if updated != 1 {
        return Err(RepositoryError::Conflict("Job wake receipt"));
    }
    Ok(())
}

async fn lock_terminal_orchestration_parents(
    transaction: &mut Transaction<'_, Postgres>,
    job: &JobRecord,
) -> Result<LockedOrchestrationJobParents, RepositoryError> {
    let parents = lock_orchestration_job_parents(transaction, job, "running").await?;
    let other_live_nodes: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*) FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND record_kind = 'node_execution'
          AND node_id <> $3 AND terminal_at IS NULL
        "#,
    )
    .bind(&job.tenant_id)
    .bind(&parents.run.run_id)
    .bind(&parents.node_id)
    .fetch_one(&mut **transaction)
    .await?;
    if parents.run.active_work_count != 1
        || other_live_nodes != 0
        || parents.run.current.control.cancel_requested_at.is_some()
        || parents.run.current.control.timeout_requested_at.is_some()
    {
        return Err(RepositoryError::Conflict(
            "terminal orchestration parent closure",
        ));
    }
    Ok(parents)
}

fn orchestration_node_state_for_job_state(
    job_state: &str,
) -> Result<&'static str, RepositoryError> {
    match job_state {
        "ready" | "leased" => Ok("ready"),
        "running" => Ok("running"),
        "waiting" => Ok("waiting"),
        "retry_scheduled" => Ok("retry_scheduled"),
        "cancelling" => Ok("cancelling"),
        _ => Err(RepositoryError::CorruptRow(
            "orchestration Job and Node states are incompatible".to_owned(),
        )),
    }
}

async fn lock_running_orchestration_job_parents(
    transaction: &mut Transaction<'_, Postgres>,
    job: &JobRecord,
) -> Result<LockedOrchestrationJobParents, RepositoryError> {
    let parents = lock_orchestration_job_parents(transaction, job, "running").await?;
    if parents.run.active_work_count < 1 {
        return Err(RepositoryError::Conflict("orchestration active work"));
    }
    Ok(parents)
}

async fn lock_waiting_orchestration_job_parents(
    transaction: &mut Transaction<'_, Postgres>,
    job: &JobRecord,
) -> Result<LockedOrchestrationJobParents, RepositoryError> {
    lock_orchestration_job_parents_matching(
        transaction,
        job,
        "waiting",
        &[RunState::Waiting, RunState::Running],
    )
    .await
}

async fn lock_orchestration_job_parents(
    transaction: &mut Transaction<'_, Postgres>,
    job: &JobRecord,
    expected_node_state: &str,
) -> Result<LockedOrchestrationJobParents, RepositoryError> {
    lock_orchestration_job_parents_matching(
        transaction,
        job,
        expected_node_state,
        &[RunState::Running],
    )
    .await
}

async fn lock_orchestration_job_parents_matching(
    transaction: &mut Transaction<'_, Postgres>,
    job: &JobRecord,
    expected_node_state: &str,
    allowed_run_states: &[RunState],
) -> Result<LockedOrchestrationJobParents, RepositoryError> {
    let run_id: ResourceId = job
        .run_id
        .as_deref()
        .ok_or_else(|| RepositoryError::CorruptRow("orchestration Job has no Run".to_owned()))?
        .parse()
        .map_err(|failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        })?;
    let node_id = job
        .node_id
        .as_deref()
        .ok_or_else(|| RepositoryError::CorruptRow("orchestration Job has no Node".to_owned()))?;
    let tenant_id: ResourceId =
        job.tenant_id
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?;
    let run = load_run_for_update(transaction, &tenant_id, &run_id).await?;
    let scope_id: String = sqlx::query_scalar(
        r#"
        SELECT scope_id FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id = $2
        "#,
    )
    .bind(&job.tenant_id)
    .bind(node_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("orchestration Node"))?;
    if scope_id == node_id {
        return Err(RepositoryError::CorruptRow(
            "orchestration Node cannot be its own Scope".to_owned(),
        ));
    }
    let rows = sqlx::query(
        r#"
        SELECT node_id, scope_id, state, version, record_kind
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id IN ($2, $3)
        ORDER BY node_id
        FOR UPDATE
        "#,
    )
    .bind(&job.tenant_id)
    .bind(node_id)
    .bind(&scope_id)
    .fetch_all(&mut **transaction)
    .await?;
    if rows.len() != 2 {
        return Err(RepositoryError::Conflict(
            "terminal orchestration parent closure",
        ));
    }
    let mut node = None;
    let mut scope = None;
    for row in rows {
        let locked_id: String = row.try_get("node_id")?;
        if locked_id == node_id {
            node = Some(row);
        } else if locked_id == scope_id {
            scope = Some(row);
        } else {
            return Err(RepositoryError::CorruptRow(
                "terminal parent lock returned an unexpected Node".to_owned(),
            ));
        }
    }
    let node = node.ok_or(RepositoryError::NotFound("orchestration Node"))?;
    let scope = scope.ok_or(RepositoryError::NotFound("orchestration Scope"))?;
    let run_state = run
        .state
        .parse::<RunState>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    if run.terminal_at.is_some()
        || !allowed_run_states.contains(&run_state)
        || node.try_get::<String, _>("record_kind")? != "node_execution"
        || node.try_get::<String, _>("scope_id")? != scope_id
        || node.try_get::<String, _>("state")? != expected_node_state
        || scope.try_get::<String, _>("record_kind")? != "scope_instance"
        || scope.try_get::<String, _>("state")? != "open"
    {
        return Err(RepositoryError::Conflict("orchestration parent closure"));
    }
    Ok(LockedOrchestrationJobParents {
        run,
        node_id: node.try_get("node_id")?,
        node_version: node.try_get("version")?,
        scope_id: scope.try_get("node_id")?,
        scope_version: scope.try_get("version")?,
    })
}

async fn lock_job_quota_bundle(
    transaction: &mut Transaction<'_, Postgres>,
    job: &JobRecord,
    _settlement_entry_ids: &[ResourceId],
) -> Result<Vec<QuotaAccountRecord>, RepositoryError> {
    let (accounts, already_settled) = lock_job_quota_bundle_state(transaction, job).await?;
    if already_settled {
        return Err(RepositoryError::Conflict("orchestration quota settlement"));
    }
    Ok(accounts)
}

async fn lock_job_quota_bundle_state(
    transaction: &mut Transaction<'_, Postgres>,
    job: &JobRecord,
) -> Result<(Vec<QuotaAccountRecord>, bool), RepositoryError> {
    let reservation_id = job.quota_reservation_id.as_deref().ok_or_else(|| {
        RepositoryError::CorruptRow("leased orchestration Job has no quota reservation".to_owned())
    })?;
    let rows = sqlx::query(
        r#"
        SELECT account.*,
               reserve.reserved_amount AS reservation_amount,
               reserve.used_amount AS reservation_used_amount
        FROM insight_platform.quota_ledger AS reserve
        JOIN insight_platform.quota_accounts AS account
          ON account.tenant_id = reserve.tenant_id
         AND account.quota_account_id = reserve.quota_account_id
        WHERE reserve.tenant_id = $1 AND reserve.correlation_id = $2
          AND reserve.entry_kind = 'reserve'
        ORDER BY account.tenant_id, account.quota_account_id
        FOR UPDATE OF account
        "#,
    )
    .bind(&job.tenant_id)
    .bind(reservation_id)
    .fetch_all(&mut **transaction)
    .await?;
    if rows.is_empty() || rows.len() > MAX_ORCHESTRATION_QUOTA_LINES {
        return Err(RepositoryError::CorruptRow(
            "orchestration quota bundle is empty or unbounded".to_owned(),
        ));
    }
    let already_settled: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM insight_platform.quota_ledger
            WHERE tenant_id = $1 AND correlation_id = $2 AND entry_kind = 'settle'
        )
        "#,
    )
    .bind(&job.tenant_id)
    .bind(reservation_id)
    .fetch_one(&mut **transaction)
    .await?;
    let mut accounts = Vec::with_capacity(rows.len());
    for row in rows {
        if row.try_get::<i64, _>("reservation_amount")? != 1
            || row.try_get::<i64, _>("reservation_used_amount")? != 0
        {
            return Err(RepositoryError::CorruptRow(
                "orchestration reservation amount is invalid".to_owned(),
            ));
        }
        let account = quota_account_from_row(row)?;
        if account.tenant_id != job.tenant_id
            || account.work_class != WorkClass::Orchestration.as_str()
            || account.metric != "concurrent_jobs"
            || account.reserved_value < 1
        {
            return Err(RepositoryError::CorruptRow(
                "orchestration quota account does not match its Job".to_owned(),
            ));
        }
        accounts.push(account);
    }
    Ok((accounts, already_settled))
}

async fn settle_locked_job_quota_bundle(
    transaction: &mut Transaction<'_, Postgres>,
    job: &JobRecord,
    accounts: &[QuotaAccountRecord],
    settlement_entry_ids: &[ResourceId],
    request_digest: &Sha256Digest,
) -> Result<(), RepositoryError> {
    let reservation_id = job.quota_reservation_id.as_deref().ok_or_else(|| {
        RepositoryError::CorruptRow("leased orchestration Job has no quota reservation".to_owned())
    })?;
    for (account, entry_id) in accounts.iter().zip(settlement_entry_ids) {
        let row = sqlx::query(
            r#"
            UPDATE insight_platform.quota_accounts
            SET reserved_value = reserved_value - 1, version = version + 1,
                updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND quota_account_id = $2 AND version = $3
              AND reserved_value >= 1
            RETURNING version
            "#,
        )
        .bind(&account.tenant_id)
        .bind(&account.quota_account_id)
        .bind(account.version)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("orchestration quota account"))?;
        let account_version: i64 = row.try_get("version")?;
        let quota_entry_id = entry_id.to_string();
        insert_quota_entry(
            transaction,
            QuotaEntryInsert {
                tenant_id: &account.tenant_id,
                quota_entry_id: &quota_entry_id,
                quota_account_id: &account.quota_account_id,
                correlation_id: reservation_id,
                entry_kind: "settle",
                reserved_amount: 1,
                used_amount: 0,
                account_version,
                request_digest: request_digest.as_str(),
            },
        )
        .await?;
    }
    Ok(())
}

fn root_scope_environment(
    input: &RunInputValue,
    limits: ScopeEnvironmentLimits,
) -> Result<ScopeDataEnvironmentSnapshot, RepositoryError> {
    ScopeDataEnvironmentSnapshot::build(
        BTreeMap::from([(
            ExactDataPortRef::RunInput {
                schema_digest: input.schema_digest.clone(),
            },
            ExactRunValueRef {
                value_id: input.value_id.clone(),
                schema_digest: input.schema_digest.clone(),
                content_digest: input.content_digest.clone(),
            },
        )]),
        limits,
    )
    .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))
}

pub(crate) fn require_exact_running_job_fence(
    job: &JobRecord,
    fence: &JobFence,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let worker_id = fence.worker_id.to_string();
    if job.tenant_id != fence.tenant_id
        || job.job_id != fence.job_id
        || job.state != JobState::Running.as_str()
        || job.version != fence.expected_job_version
        || job.lease_epoch != fence.lease_epoch
        || job.worker_id.as_deref() != Some(worker_id.as_str())
        || job.lease_token_digest.as_deref() != Some(fence.lease_token_digest.as_str())
        || job
            .lease_expires_at
            .is_none_or(|expires_at| expires_at <= database_now)
        || job.terminal_at.is_some()
    {
        return Err(RepositoryError::Conflict("running Job fence"));
    }
    Ok(())
}

async fn load_scope_environment_chain(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    run_id: &ResourceId,
    scope_id: &ResourceId,
    limits: ScopeEnvironmentLimits,
) -> Result<Vec<ScopeDataEnvironmentSnapshot>, RepositoryError> {
    let maximum_depth = i32::try_from(limits.maximum_lexical_depth)
        .map_err(|_| RepositoryError::InvalidInput("Scope depth exceeds integer".to_owned()))?;
    let rows = sqlx::query(
        r#"
        WITH RECURSIVE lexical_scope AS (
            SELECT scope.node_id, scope.parent_node_id, scope.node_kind, scope.state,
                   scope.payload_schema_version, scope.payload, scope.payload_digest,
                   1::integer AS depth, ARRAY[scope.node_id]::text[] AS path
            FROM insight_platform.run_nodes AS scope
            WHERE scope.tenant_id = $1 AND scope.run_id = $2 AND scope.node_id = $3
              AND scope.record_kind = 'scope_instance'
          UNION ALL
            SELECT parent_scope.node_id, parent_scope.parent_node_id, parent_scope.node_kind,
                   parent_scope.state, parent_scope.payload_schema_version,
                   parent_scope.payload, parent_scope.payload_digest,
                   current.depth + 1, current.path || parent_scope.node_id
            FROM lexical_scope AS current
            JOIN insight_platform.run_nodes AS creator
              ON creator.tenant_id = $1 AND creator.run_id = $2
             AND creator.node_id = current.parent_node_id
             AND creator.record_kind = 'node_execution'
            JOIN insight_platform.run_nodes AS parent_scope
              ON parent_scope.tenant_id = creator.tenant_id
             AND parent_scope.run_id = creator.run_id
             AND parent_scope.node_id = creator.scope_id
             AND parent_scope.record_kind = 'scope_instance'
            WHERE current.parent_node_id IS NOT NULL AND current.depth < $4
              AND NOT parent_scope.node_id = ANY(current.path)
        )
        SELECT node_id, parent_node_id, node_kind, state, payload_schema_version,
               payload, payload_digest, depth
        FROM lexical_scope
        ORDER BY depth
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(run_id.to_string())
    .bind(scope_id.to_string())
    .bind(maximum_depth)
    .fetch_all(&mut **transaction)
    .await?;
    if rows.is_empty()
        || rows.len() > limits.maximum_lexical_depth
        || rows.last().is_some_and(|row| {
            row.try_get::<Option<String>, _>("parent_node_id")
                .ok()
                .flatten()
                .is_some()
        })
    {
        return Err(RepositoryError::CorruptRow(
            "Scope lexical chain is missing, cyclic, or exceeds its bound".to_owned(),
        ));
    }
    let mut environments = Vec::with_capacity(rows.len());
    for row in rows {
        if row.try_get::<String, _>("state")? != ScopeState::Open.as_str() {
            return Err(RepositoryError::Conflict("Scope lexical chain is not open"));
        }
        let payload =
            payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
        let kind: String = row.try_get("node_kind")?;
        let (environment, expected_controller) = match kind.as_str() {
            "root" => {
                let root: StoredRootScopePayload = decode_typed_payload(&payload, "root Scope")?;
                (root.environment, None)
            }
            "parallel_leg" | "loop_iteration" | "map_item" => {
                let nested: StoredControllerScopePayload =
                    decode_typed_payload(&payload, "controller Scope")?;
                (
                    nested.environment,
                    Some(nested.controller_node_execution_id),
                )
            }
            _ => {
                return Err(RepositoryError::CorruptRow(
                    "Scope has an unregistered data-environment kind".to_owned(),
                ))
            }
        };
        if expected_controller.as_ref().map(ResourceId::to_string)
            != row.try_get::<Option<String>, _>("parent_node_id")?
        {
            return Err(RepositoryError::CorruptRow(
                "Scope controller owner differs from its payload".to_owned(),
            ));
        }
        environment
            .validate(limits)
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        environments.push(environment);
    }
    Ok(environments)
}

async fn load_resolved_expression_values(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    run_id: &ResourceId,
    ports: Vec<ExactDataPortRef>,
    references: Vec<ExactRunValueRef>,
) -> Result<Vec<ResolvedExpressionInput>, RepositoryError> {
    if ports.len() != references.len() {
        return Err(RepositoryError::CorruptRow(
            "Scope input resolution changed arity".to_owned(),
        ));
    }
    let mut inputs = Vec::with_capacity(ports.len());
    for (port, reference) in ports.into_iter().zip(references) {
        let row = sqlx::query(
            r#"
            SELECT value.value_id, value.node_id, value.value_kind,
                   value.classification, value.schema_digest,
                   value.content_digest, value.inline_value, value.artifact_id,
                   artifact.state AS artifact_state, artifact.terminal_at AS artifact_terminal_at,
                   artifact.classification AS artifact_classification,
                   artifact.verified_media_type, blob.state AS blob_state,
                   blob.deleted_at AS blob_deleted_at, blob.content_digest AS blob_content_digest,
                   blob.size_bytes
            FROM insight_platform.run_values AS value
            LEFT JOIN insight_platform.artifacts AS artifact
              ON artifact.tenant_id = value.tenant_id AND artifact.artifact_id = value.artifact_id
            LEFT JOIN insight_platform.artifact_blobs AS blob
              ON blob.tenant_id = artifact.tenant_id AND blob.blob_id = artifact.blob_id
            WHERE value.tenant_id = $1 AND value.run_id = $2 AND value.value_id = $3
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(run_id.to_string())
        .bind(reference.value_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("resolved RunValue"))?;
        let schema_digest: Sha256Digest = row
            .try_get::<String, _>("schema_digest")?
            .parse()
            .map_err(|_| RepositoryError::CorruptRow("RunValue schema digest".to_owned()))?;
        let content_digest: Sha256Digest = row
            .try_get::<String, _>("content_digest")?
            .parse()
            .map_err(|_| RepositoryError::CorruptRow("RunValue content digest".to_owned()))?;
        if schema_digest != reference.schema_digest
            || content_digest != reference.content_digest
            || &schema_digest != port.schema_digest()
        {
            return Err(RepositoryError::Conflict("resolved RunValue evidence"));
        }
        let classification: DataClassification = row
            .try_get::<String, _>("classification")?
            .parse::<DataClassification>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        let inline_value: Option<Value> = row.try_get("inline_value")?;
        let artifact_id: Option<String> = row.try_get("artifact_id")?;
        let value = match (inline_value, artifact_id) {
            (Some(value), None) => {
                let observed: Sha256Digest = canonical_digest(&value)
                    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?
                    .parse()
                    .map_err(|_| RepositoryError::CorruptRow("RunValue digest".to_owned()))?;
                if observed != content_digest {
                    return Err(RepositoryError::CorruptRow(
                        "RunValue content digest differs".to_owned(),
                    ));
                }
                ValueRef::Inline { value }
            }
            (None, Some(artifact_id))
                if row
                    .try_get::<Option<String>, _>("artifact_state")?
                    .as_deref()
                    == Some("ready")
                    && row
                        .try_get::<Option<DateTime<Utc>>, _>("artifact_terminal_at")?
                        .is_none()
                    && row.try_get::<Option<String>, _>("blob_state")?.as_deref()
                        == Some("verified")
                    && row
                        .try_get::<Option<DateTime<Utc>>, _>("blob_deleted_at")?
                        .is_none()
                    && row
                        .try_get::<Option<String>, _>("blob_content_digest")?
                        .as_deref()
                        == Some(content_digest.as_str())
                    && row
                        .try_get::<Option<String>, _>("artifact_classification")?
                        .as_deref()
                        == Some(classification.as_str()) =>
            {
                ValueRef::Artifact {
                    artifact: ArtifactRef::new(
                        artifact_id.parse().map_err(|_| {
                            RepositoryError::CorruptRow("Artifact ID is invalid".to_owned())
                        })?,
                        content_digest.clone(),
                        u64::try_from(row.try_get::<i64, _>("size_bytes")?).map_err(|_| {
                            RepositoryError::CorruptRow("Artifact size is invalid".to_owned())
                        })?,
                        row.try_get::<String, _>("verified_media_type")?,
                        classification,
                        None,
                    )
                    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
                }
            }
            _ => {
                return Err(RepositoryError::CorruptRow(
                    "RunValue storage shape is invalid".to_owned(),
                ))
            }
        };
        inputs.push(ResolvedExpressionInput {
            run_value_id: reference.value_id,
            producing_node_id: row
                .try_get::<Option<String>, _>("node_id")?
                .map(|value| value.parse())
                .transpose()
                .map_err(|_| RepositoryError::CorruptRow("RunValue Node ID".to_owned()))?,
            value_kind: row.try_get("value_kind")?,
            port,
            classification,
            schema_digest,
            content_digest,
            value,
        });
    }
    Ok(inputs)
}

fn expression_loop_iteration(
    node: &insight_platform_plan::RuntimeNode,
    row: &PgRow,
) -> Result<u32, RepositoryError> {
    if !matches!(node, insight_platform_plan::RuntimeNode::Loop { .. }) {
        return Ok(0);
    }
    let payload = payload_from_row(row, "payload_schema_version", "payload", "payload_digest")?;
    if payload.value.get("wait").is_none() {
        return Ok(0);
    }
    let pending: StoredPendingControllerNodePayload =
        decode_typed_payload(&payload, "Loop continuation Node")?;
    match pending.wait {
        StoredControllerWait::Loop { iteration } => Ok(iteration),
        _ => Err(RepositoryError::Conflict("Loop continuation wait kind")),
    }
}

fn decode_orchestration_job_payload(
    payload: &TypedPayload,
) -> Result<OrchestrationJobPayload, RepositoryError> {
    OrchestrationJobPayload::from_payload(payload)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))
}

fn orchestration_job_payload_with_wake(
    current: &JobRecord,
    wake_contract: Option<WakeContract>,
) -> Result<TypedPayload, RepositoryError> {
    let mut payload: OrchestrationJobPayload = decode_orchestration_job_payload(&current.payload)?;
    payload
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    payload.wake_contract = wake_contract;
    payload.to_payload().map_err(RepositoryError::from)
}

#[derive(Debug, Clone)]
struct ControllerSourceNode {
    plan_node_key: PlanNodeKey,
    node_kind: PlanNodeKind,
    scope_id: String,
    version: i64,
}

#[derive(Debug, Clone)]
struct OpenLoopIterationContext {
    root_loop_node_execution_id: ResourceId,
    scope_id: ResourceId,
    lexical_parent_scope_id: ResourceId,
    iteration: u32,
    loop_plan_node_key: PlanNodeKey,
}

#[allow(clippy::too_many_arguments)]
async fn derive_controller_step_shape(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    parents: &LockedOrchestrationJobParents,
    source_node: &ControllerSourceNode,
    plan: &RuntimePlan,
    runtime_node: &insight_platform_plan::RuntimeNode,
    decision: &ControllerDecision,
    maximum_fan_out: usize,
) -> Result<ControllerStepShape, RepositoryError> {
    match (decision, runtime_node) {
        (
            ControllerDecision::CompleteNode { activate },
            insight_platform_plan::RuntimeNode::Start { .. }
            | insight_platform_plan::RuntimeNode::Compute { .. }
            | insight_platform_plan::RuntimeNode::Branch { .. }
            | insight_platform_plan::RuntimeNode::Join { .. }
            | insight_platform_plan::RuntimeNode::Map { .. }
            | insight_platform_plan::RuntimeNode::Loop { .. }
            | insight_platform_plan::RuntimeNode::ErrorBoundary { .. }
            | insight_platform_plan::RuntimeNode::HumanTask { .. }
            | insight_platform_plan::RuntimeNode::TimerWait { .. }
            | insight_platform_plan::RuntimeNode::SignalWait { .. }
            | insight_platform_plan::RuntimeNode::ChildAgentCall { .. }
            | insight_platform_plan::RuntimeNode::ContextQuery { .. }
            | insight_platform_plan::RuntimeNode::ModelLoop { .. }
            | insight_platform_plan::RuntimeNode::CapabilityCall { .. },
        ) => {
            if let [target] = activate.as_slice() {
                let loop_context = if matches!(
                    runtime_node,
                    insight_platform_plan::RuntimeNode::Loop { .. }
                ) {
                    load_open_loop_iteration_context(
                        transaction,
                        current_job,
                        parents,
                        source_node,
                        None,
                    )
                    .await?
                } else {
                    None
                };
                if let Some(context) = loop_context {
                    Ok(ControllerStepShape::LoopConditionExit {
                        target: target.clone(),
                        context,
                    })
                } else if let Some(exit) = load_parallel_leg_exit(
                    transaction,
                    current_job,
                    parents,
                    source_node,
                    Some(target),
                )
                .await?
                {
                    Ok(exit)
                } else if let Some(exit) =
                    load_loop_iteration_exit(transaction, current_job, parents, source_node, target)
                        .await?
                {
                    Ok(exit)
                } else if let Some(exit) =
                    load_map_item_exit(transaction, current_job, parents, source_node, Some(target))
                        .await?
                {
                    Ok(exit)
                } else {
                    Ok(ControllerStepShape::Sequential {
                        targets: activate.clone(),
                    })
                }
            } else {
                Ok(ControllerStepShape::Sequential {
                    targets: activate.clone(),
                })
            }
        }
        (
            ControllerDecision::OpenMapItems {
                next, item_count, ..
            },
            insight_platform_plan::RuntimeNode::Map { .. },
        ) if *item_count == 0 => Ok(ControllerStepShape::Sequential {
            targets: vec![next.clone()],
        }),
        (
            ControllerDecision::OpenMapItems {
                body,
                next,
                item_count,
                failure_policy,
            },
            insight_platform_plan::RuntimeNode::Map { .. },
        ) => {
            load_map_batch_shape(
                transaction,
                current_job,
                parents,
                source_node,
                body,
                next,
                *failure_policy,
                *item_count,
                maximum_fan_out,
            )
            .await
        }
        (
            ControllerDecision::OpenLoopIteration { body, iteration },
            insight_platform_plan::RuntimeNode::Loop {
                body: plan_body, ..
            },
        ) if body == plan_body => Ok(ControllerStepShape::LoopIteration {
            body: body.clone(),
            loop_plan_node_key: source_node.plan_node_key.clone(),
            iteration: *iteration,
            existing_scope: load_open_loop_iteration_context(
                transaction,
                current_job,
                parents,
                source_node,
                Some(*iteration),
            )
            .await?,
        }),
        (
            ControllerDecision::FanOut {
                activate,
                create_pending,
            },
            insight_platform_plan::RuntimeNode::Fork { join, .. },
        ) if create_pending == std::slice::from_ref(join) => {
            let insight_platform_plan::RuntimeNode::Join {
                policy,
                quorum,
                remainder,
                ..
            } = plan.node(join)?
            else {
                return Err(RepositoryError::InvalidInput(
                    "Fork pending target is not a Join node".to_owned(),
                ));
            };
            if quorum.is_some_and(|required| usize::from(required) > activate.len()) {
                return Err(RepositoryError::InvalidInput(
                    "Join quorum exceeds Fork leg count".to_owned(),
                ));
            }
            Ok(ControllerStepShape::Fork {
                legs: activate.clone(),
                join: join.clone(),
                policy: *policy,
                quorum: *quorum,
                remainder: *remainder,
            })
        }
        _ => Err(RepositoryError::InvalidInput(
            "controller decision requires its typed fan-out, wait, leaf, or terminal adapter"
                .to_owned(),
        )),
    }
}

#[derive(Debug, Clone)]
enum ControllerStepShape {
    Sequential {
        targets: Vec<PlanNodeKey>,
    },
    Fork {
        legs: Vec<PlanNodeKey>,
        join: PlanNodeKey,
        policy: JoinPolicy,
        quorum: Option<u16>,
        remainder: Option<JoinRemainderPolicy>,
    },
    LoopIteration {
        body: PlanNodeKey,
        loop_plan_node_key: PlanNodeKey,
        iteration: u32,
        existing_scope: Option<OpenLoopIterationContext>,
    },
    LoopConditionExit {
        target: PlanNodeKey,
        context: OpenLoopIterationContext,
    },
    LoopIterationExit {
        loop_node_execution_id: ResourceId,
        root_loop_node_execution_id: ResourceId,
        loop_plan_node_key: PlanNodeKey,
        expected_scope_id: ResourceId,
        iteration: u32,
    },
    MapBatch {
        body: PlanNodeKey,
        next: PlanNodeKey,
        failure_policy: MapFailurePolicy,
        item_count: u32,
        batch_start: u32,
        batch_size: u32,
        root_map_node_execution_id: ResourceId,
        has_more: bool,
    },
    MapItemExit {
        map_wait_node_execution_id: Option<ResourceId>,
        map_plan_node_key: PlanNodeKey,
        next_plan_node_key: PlanNodeKey,
        failure_policy: MapFailurePolicy,
        item_count: u32,
        item_index: u32,
        root_map_node_execution_id: ResourceId,
    },
    ParallelLegExit {
        join_node_execution_id: ResourceId,
        join_plan_node_key: PlanNodeKey,
        expected_scope_ids: Vec<ResourceId>,
        policy: JoinPolicy,
        quorum: Option<u16>,
        remainder: Option<JoinRemainderPolicy>,
    },
}

const fn map_policy_requires_admission_barrier(policy: MapFailurePolicy) -> bool {
    !matches!(policy, MapFailurePolicy::AllSettled)
}

impl ControllerStepShape {
    fn activation_targets(&self) -> Result<Vec<PlanNodeKey>, RepositoryError> {
        match self {
            Self::Sequential { targets } => Ok(targets.clone()),
            Self::Fork { legs, .. } => Ok(legs.clone()),
            Self::LoopIteration { body, .. } => Ok(vec![body.clone()]),
            Self::LoopConditionExit { target, .. } => Ok(vec![target.clone()]),
            Self::MapBatch {
                body, batch_size, ..
            } => Ok(vec![
                body.clone();
                usize::try_from(*batch_size).map_err(|_| {
                    RepositoryError::InvalidInput(
                        "Map batch size exceeds platform representation".to_owned(),
                    )
                })?
            ]),
            Self::LoopIterationExit { .. }
            | Self::MapItemExit { .. }
            | Self::ParallelLegExit { .. } => Ok(Vec::new()),
        }
    }

    fn validate_slots(&self, mutations: &ControllerStepMutationIds) -> Result<(), RepositoryError> {
        match self {
            Self::Sequential { targets }
                if targets.len() == mutations.activations.len()
                    && mutations.pending_nodes.is_empty()
                    && mutations
                        .activations
                        .iter()
                        .all(|slot| slot.scope.is_none())
                    && mutations.structural_exit.is_none()
                    && mutations.pending_wake.is_none()
                    && mutations.remainder_cancellations.is_empty() =>
            {
                Ok(())
            }
            Self::Fork { legs, .. }
                if legs.len() == mutations.activations.len()
                    && mutations.pending_nodes.len() == 1
                    && mutations
                        .activations
                        .iter()
                        .all(|slot| slot.scope.is_some())
                    && mutations.structural_exit.is_none()
                    && mutations.pending_wake.is_none()
                    && mutations.remainder_cancellations.is_empty() =>
            {
                Ok(())
            }
            Self::LoopIteration { existing_scope, .. }
                if mutations.activations.len() == 1
                    && mutations.pending_nodes.len() == 1
                    && mutations.activations[0].scope.is_some() == existing_scope.is_none()
                    && mutations.structural_exit.is_none()
                    && mutations.pending_wake.is_none()
                    && mutations.remainder_cancellations.is_empty() =>
            {
                Ok(())
            }
            Self::LoopConditionExit { .. }
                if mutations.activations.len() == 1
                    && mutations.pending_nodes.is_empty()
                    && mutations.activations[0].scope.is_none()
                    && mutations.structural_exit.is_some()
                    && mutations.pending_wake.is_none()
                    && mutations.remainder_cancellations.is_empty() =>
            {
                Ok(())
            }
            Self::LoopIterationExit { .. }
                if mutations.activations.is_empty()
                    && mutations.pending_nodes.is_empty()
                    && mutations.structural_exit.is_some()
                    && mutations.pending_wake.is_some()
                    && mutations.remainder_cancellations.is_empty() =>
            {
                Ok(())
            }
            Self::MapBatch {
                failure_policy,
                item_count,
                batch_start,
                batch_size,
                has_more,
                ..
            } if !mutations.activations.is_empty()
                && mutations.pending_nodes.len() == 1
                && mutations
                    .activations
                    .iter()
                    .all(|slot| slot.scope.is_some())
                && mutations.structural_exit.is_none()
                && mutations.remainder_cancellations.is_empty()
                && mutations.pending_wake.is_some()
                    == (*has_more && !map_policy_requires_admission_barrier(*failure_policy))
                && u32::try_from(mutations.activations.len()).ok() == Some(*batch_size)
                && batch_start
                    .checked_add(*batch_size)
                    .is_some_and(|end| end <= *item_count) =>
            {
                Ok(())
            }
            Self::MapItemExit { .. }
                if mutations.activations.is_empty()
                    && mutations.pending_nodes.is_empty()
                    && mutations.structural_exit.is_some()
                    && mutations.pending_wake.is_some() =>
            {
                Ok(())
            }
            Self::ParallelLegExit { .. }
                if mutations.activations.is_empty()
                    && mutations.pending_nodes.is_empty()
                    && mutations.structural_exit.is_some()
                    && mutations.pending_wake.is_some() =>
            {
                Ok(())
            }
            _ => Err(RepositoryError::InvalidInput(
                "controller mutation slot shape does not match the deterministic decision"
                    .to_owned(),
            )),
        }
    }

    fn mutation_requirements(
        &self,
        runtime_node: &insight_platform_plan::RuntimeNode,
    ) -> Result<ControllerMutationRequirements, RepositoryError> {
        let activation_scopes = match self {
            Self::Sequential { targets } => vec![false; targets.len()],
            Self::Fork { legs, .. } => vec![true; legs.len()],
            Self::LoopIteration { existing_scope, .. } => vec![existing_scope.is_none()],
            Self::LoopConditionExit { .. } => vec![false],
            Self::MapBatch { batch_size, .. } => {
                vec![
                    true;
                    usize::try_from(*batch_size).map_err(|_| {
                        RepositoryError::InvalidInput(
                            "Map batch size exceeds platform representation".to_owned(),
                        )
                    })?
                ]
            }
            Self::LoopIterationExit { .. }
            | Self::MapItemExit { .. }
            | Self::ParallelLegExit { .. } => Vec::new(),
        };
        let pending_node_count = usize::from(matches!(
            self,
            Self::Fork { .. } | Self::LoopIteration { .. } | Self::MapBatch { .. }
        ));
        let structural_exit = match self {
            Self::LoopIterationExit { .. } => {
                let insight_platform_plan::RuntimeNode::Compute { .. } = runtime_node else {
                    return Err(RepositoryError::Conflict("Loop body Plan node"));
                };
                // The carried arity belongs to the Loop node, not the body Compute. It is loaded
                // from the frozen continuation descriptor by the dedicated requirement loader.
                ControllerStructuralRequirement::LoopRollover {
                    carried_value_count: 0,
                }
            }
            Self::LoopConditionExit { .. }
            | Self::MapItemExit { .. }
            | Self::ParallelLegExit { .. } => ControllerStructuralRequirement::Close,
            _ => ControllerStructuralRequirement::None,
        };
        let pending_wake = match self {
            Self::LoopIterationExit { .. }
            | Self::MapItemExit { .. }
            | Self::ParallelLegExit { .. } => true,
            Self::MapBatch {
                failure_policy,
                has_more,
                ..
            } => *has_more && !map_policy_requires_admission_barrier(*failure_policy),
            _ => false,
        };
        Ok(ControllerMutationRequirements {
            activation_scopes,
            pending_node_count,
            structural_exit,
            pending_wake,
            remainder_cancellation_scope_ids: Vec::new(),
        })
    }
}

async fn load_controller_remainder_requirements(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    parents: &LockedOrchestrationJobParents,
    plan: &RuntimePlan,
    shape: &ControllerStepShape,
    source_outcome: ChildOutcome,
) -> Result<Vec<ResourceId>, RepositoryError> {
    let (active_scope_ids, cancel_active) = match shape {
        ControllerStepShape::MapItemExit {
            map_plan_node_key,
            root_map_node_execution_id,
            ..
        } => {
            let rows = sqlx::query(
                r#"
                SELECT node_id, state, payload_schema_version, payload, payload_digest
                FROM insight_platform.run_nodes
                WHERE tenant_id = $1 AND run_id = $2 AND record_kind = 'scope_instance'
                  AND parent_node_id = $3 AND node_kind = 'map_item'
                "#,
            )
            .bind(&current_job.tenant_id)
            .bind(&parents.run.run_id)
            .bind(root_map_node_execution_id.to_string())
            .fetch_all(&mut **transaction)
            .await?;
            let mut outcomes = BTreeMap::new();
            let mut active = Vec::new();
            for row in rows {
                let scope_id: String = row.try_get("node_id")?;
                let payload =
                    payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
                let stored: StoredControllerScopePayload =
                    decode_typed_payload(&payload, "Map cancellation Scope")?;
                let StoredControllerScopeDescriptor::MapItem { item_index, .. } = stored.descriptor
                else {
                    return Err(RepositoryError::Conflict("Map cancellation Scope kind"));
                };
                if stored.controller_node_execution_id != *root_map_node_execution_id {
                    return Err(RepositoryError::Conflict("Map cancellation Scope owner"));
                }
                let outcome = if scope_id == parents.scope_id {
                    source_outcome
                } else {
                    let outcome = child_outcome_from_scope_state(row.try_get("state")?)?;
                    if outcome == ChildOutcome::Active {
                        active.push(scope_id);
                    }
                    outcome
                };
                if outcomes.insert(item_index, outcome).is_some() {
                    return Err(RepositoryError::Conflict(
                        "duplicate Map cancellation index",
                    ));
                }
            }
            let children = outcomes.into_values().collect::<Vec<_>>();
            let map_node = plan.node(map_plan_node_key)?;
            let decision =
                decide_controller(map_node, &ControllerObservation::MapSettlement { children })?;
            (
                active,
                matches!(decision, ControllerDecision::FailNode { .. }),
            )
        }
        ControllerStepShape::ParallelLegExit {
            join_plan_node_key,
            expected_scope_ids,
            remainder,
            ..
        } => {
            let expected_ids = expected_scope_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            let rows = sqlx::query(
                r#"
                SELECT node_id, state
                FROM insight_platform.run_nodes
                WHERE tenant_id = $1 AND run_id = $2 AND record_kind = 'scope_instance'
                  AND node_id = ANY($3::text[]) AND node_kind = 'parallel_leg'
                "#,
            )
            .bind(&current_job.tenant_id)
            .bind(&parents.run.run_id)
            .bind(&expected_ids)
            .fetch_all(&mut **transaction)
            .await?;
            if rows.len() != expected_ids.len() {
                return Err(RepositoryError::Conflict("Join cancellation Scope set"));
            }
            let mut outcomes = BTreeMap::new();
            let mut active = Vec::new();
            for row in rows {
                let scope_id: String = row.try_get("node_id")?;
                let outcome = if scope_id == parents.scope_id {
                    source_outcome
                } else {
                    let outcome = child_outcome_from_scope_state(row.try_get("state")?)?;
                    if outcome == ChildOutcome::Active {
                        active.push(scope_id.clone());
                    }
                    outcome
                };
                outcomes.insert(scope_id, outcome);
            }
            let children = expected_ids
                .iter()
                .map(|scope_id| {
                    outcomes.get(scope_id).copied().ok_or_else(|| {
                        RepositoryError::CorruptRow("Join cancellation lost a Scope".to_owned())
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let decision = decide_controller(
                plan.node(join_plan_node_key)?,
                &ControllerObservation::Join { children },
            )?;
            let cancel = matches!(decision, ControllerDecision::FailNode { .. })
                || matches!(decision, ControllerDecision::CompleteNode { .. })
                    && *remainder == Some(JoinRemainderPolicy::Cancel);
            (active, cancel)
        }
        _ => return Ok(Vec::new()),
    };
    if !cancel_active {
        return Ok(Vec::new());
    }
    active_scope_ids
        .into_iter()
        .map(|scope_id| {
            scope_id
                .parse()
                .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                })
        })
        .collect()
}

async fn validate_agent_deployment_publish_pair(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    agent_id: &ResourceId,
    closure: &AgentDeploymentClosure,
) -> Result<(), RepositoryError> {
    let rows = sqlx::query(
        r#"
        SELECT resource_version_id, resource_id, resource_version_kind, revision_no
        FROM insight_platform.resource_versions
        WHERE tenant_id = $1 AND (
            (resource_version_id = $2 AND resource_version_kind = 'agent_interface_revision') OR
            (resource_version_id = $3 AND resource_version_kind = 'agent_plan_revision')
        )
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(closure.interface.revision_id.to_string())
    .bind(closure.plan.revision_id.to_string())
    .fetch_all(&mut **transaction)
    .await?;
    if rows.len() != 2 {
        return Err(RepositoryError::NotFound(
            "exact Agent Interface/Plan revision pair",
        ));
    }
    let expected_agent_id = agent_id.to_string();
    let mut revision_no = None;
    for row in rows {
        let resource_id: String = row.try_get("resource_id")?;
        let row_revision_no: i64 = row.try_get("revision_no")?;
        if resource_id != expected_agent_id
            || revision_no.is_some_and(|expected| expected != row_revision_no)
        {
            return Err(RepositoryError::Conflict(
                "Agent Deployment Interface/Plan publish batch",
            ));
        }
        revision_no = Some(row_revision_no);
    }
    Ok(())
}

fn validate_failure_mutation_shape(
    mutations: &ControllerStepMutationIds,
    structured_exit: bool,
    error_route: Option<&ErrorBoundaryRoute>,
) -> Result<(), RepositoryError> {
    if error_route.is_some() {
        let matches = mutations.activations.len() == 1
            && mutations.activations[0].scope.is_none()
            && mutations.pending_nodes.is_empty()
            && mutations.structural_exit.is_none()
            && mutations.pending_wake.is_none()
            && mutations.remainder_cancellations.is_empty();
        return if matches {
            Ok(())
        } else {
            Err(RepositoryError::InvalidInput(
                "ErrorBoundary failure requires exactly one handler activation".to_owned(),
            ))
        };
    }
    let common = mutations.activations.is_empty()
        && mutations.pending_nodes.is_empty()
        && mutations.structural_exit.is_some();
    let matches = if structured_exit {
        common && mutations.pending_wake.is_some()
    } else {
        common && mutations.pending_wake.is_none() && mutations.remainder_cancellations.is_empty()
    };
    if !matches {
        return Err(RepositoryError::InvalidInput(
            "orchestration failure mutation shape does not match its Scope".to_owned(),
        ));
    }
    Ok(())
}

async fn require_exact_runtime_plan(
    transaction: &mut Transaction<'_, Postgres>,
    run: &RunRecord,
    plan: &RuntimePlan,
    plan_digest: &Sha256Digest,
) -> Result<(), RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT resource_id, revision_no, payload_schema_version, payload, payload_digest
        FROM insight_platform.resource_versions
        WHERE tenant_id = $1 AND resource_version_id = $2
          AND resource_version_kind = 'agent_plan_revision' AND content_digest = $3
        "#,
    )
    .bind(&run.tenant_id)
    .bind(run.bindings.plan.revision_id.to_string())
    .bind(run.bindings.plan.semantic_digest.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("exact Agent Plan revision"))?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let plan_resource_id: String = row.try_get("resource_id")?;
    let plan_revision_no: i64 = row.try_get("revision_no")?;
    let interface_row = sqlx::query(
        r#"
        SELECT resource_id, revision_no
        FROM insight_platform.resource_versions
        WHERE tenant_id = $1 AND resource_version_id = $2
          AND resource_version_kind = 'agent_interface_revision'
        "#,
    )
    .bind(&run.tenant_id)
    .bind(run.bindings.agent_interface.revision_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("exact Agent Interface revision"))?;
    let interface_resource_id: String = interface_row.try_get("resource_id")?;
    let interface_revision_no: i64 = interface_row.try_get("revision_no")?;
    if plan_resource_id != interface_resource_id || plan_revision_no != interface_revision_no {
        return Err(RepositoryError::Conflict(
            "runtime Plan and Agent Interface publish batch",
        ));
    }
    let published = decode_published_version_payload(&payload)?;
    published
        .validate_for(RegistryResourceKind::Agent, &run.bindings.plan.revision_id)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let ResourceDocument::Agent(agent) = published.document else {
        return Err(RepositoryError::CorruptRow(
            "Agent Plan revision contains a non-Agent document".to_owned(),
        ));
    };
    if plan.interface_contract_digest != agent.contract_digest {
        return Err(RepositoryError::Conflict(
            "runtime Plan Agent interface contract digest",
        ));
    }
    plan.validate_terminal_schema_digests(
        &agent.output_schema.canonical_digest,
        &agent.error_schema.canonical_digest,
    )
    .map_err(|_| RepositoryError::Conflict("runtime Plan terminal interface binding"))?;
    if &agent.typed_plan_digest != plan_digest {
        return Err(RepositoryError::Conflict("exact typed Plan digest"));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum ExternalLeafSuccessKind {
    Child,
    Context,
    Model,
    Capability,
}

#[derive(Debug)]
struct ExternalLeafSuccessOwner<'a> {
    tenant_id: &'a ResourceId,
    run_id: &'a ResourceId,
    node_id: &'a ResourceId,
    owner_id: &'a ResourceId,
    owner_job_id: &'a ResourceId,
    output_value_id: &'a ResourceId,
    kind: ExternalLeafSuccessKind,
}

#[derive(Debug)]
struct ExternalLeafSuccessWait {
    plan_digest: Sha256Digest,
    source_orchestration_job_id: ResourceId,
    output_port: ExactDataPortRef,
    root_scope_id: ResourceId,
    continuation_attempt_limit: i32,
    retry_backoff_milliseconds: u64,
    priority: SchedulerPriority,
    deadline: DateTime<Utc>,
}

pub(crate) async fn settle_context_leaf_success_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    query: &ContextQueryRecord,
    context_job_id: &ResourceId,
    output: &ExactInvocationValueRef,
    mutations: &insight_platform_contracts::ExternalLeafResumeMutationIds,
    scope_limits: ScopeEnvironmentLimits,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let output_value_id = query
        .output_value_id
        .as_ref()
        .ok_or(RepositoryError::Conflict("Context terminal output"))?;
    settle_external_leaf_success_in_transaction(
        transaction,
        ExternalLeafSuccessOwner {
            tenant_id: &query.tenant_id,
            run_id: &query.run_id,
            node_id: &query.node_execution_id,
            owner_id: &query.context_query_id,
            owner_job_id: context_job_id,
            output_value_id,
            kind: ExternalLeafSuccessKind::Context,
        },
        output,
        mutations,
        scope_limits,
        database_now,
    )
    .await
}

pub(crate) async fn settle_model_leaf_success_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    turn: &insight_platform_models::ModelTurnRecord,
    model_job_id: &ResourceId,
    output: &ExactInvocationValueRef,
    mutations: &insight_platform_contracts::ExternalLeafResumeMutationIds,
    scope_limits: ScopeEnvironmentLimits,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let output_value_id = turn
        .output_value_id
        .as_ref()
        .ok_or(RepositoryError::Conflict("Model terminal output"))?;
    let result = turn
        .payload
        .result
        .as_ref()
        .ok_or(RepositoryError::Conflict("Model terminal result"))?;
    if result.output.value_id != *output_value_id
        || result.finish_reason != insight_platform_models::CanonicalFinishReason::Completed
        || result.tool_intent_count != 0
        || output.value_kind != "model_structured_output"
        || output.schema_digest != result.output.schema_digest
    {
        return Err(RepositoryError::Conflict(
            "Model structured terminal output",
        ));
    }
    settle_external_leaf_success_in_transaction(
        transaction,
        ExternalLeafSuccessOwner {
            tenant_id: &turn.tenant_id,
            run_id: &turn.run_id,
            node_id: &turn.node_execution_id,
            owner_id: &turn.model_turn_id,
            owner_job_id: model_job_id,
            output_value_id: &output.value_id,
            kind: ExternalLeafSuccessKind::Model,
        },
        output,
        mutations,
        scope_limits,
        database_now,
    )
    .await
}

pub(crate) async fn handoff_model_tool_continuation_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    turn: &insight_platform_models::ModelTurnRecord,
    model_job_id: &ResourceId,
    output: &insight_platform_models::ModelOutputValue,
    mutations: &insight_platform_models::ModelToolContinuationMutationIds,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    mutations
        .validate()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let tool_intent_count = u16::try_from(output.response.tool_intents.len()).map_err(|_| {
        RepositoryError::InvalidInput("Model tool-intent count exceeds u16".to_owned())
    })?;
    if tool_intent_count == 0
        || turn.output_value_id.as_ref() != Some(&output.value_id)
        || turn.payload.result.as_ref().is_none_or(|result| {
            result.output.value_id != output.value_id
                || result.response_digest != output.content_digest
                || result.tool_intent_count != u32::from(tool_intent_count)
        })
    {
        return Err(RepositoryError::Conflict(
            "Model tool continuation terminal response",
        ));
    }
    let run = load_run_for_update(transaction, &turn.tenant_id, &turn.run_id).await?;
    let node_row = sqlx::query(
        r#"
        SELECT state, version, payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id = $2 AND run_id = $3
          AND record_kind = 'node_execution'
        FOR UPDATE
        "#,
    )
    .bind(turn.tenant_id.to_string())
    .bind(turn.node_execution_id.to_string())
    .bind(turn.run_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Model tool continuation Node"))?;
    if node_row.try_get::<String, _>("state")? != NodeExecutionState::Waiting.as_str() {
        return Err(RepositoryError::Conflict(
            "Model tool continuation Node state",
        ));
    }
    let wait_payload = payload_from_row(
        &node_row,
        "payload_schema_version",
        "payload",
        "payload_digest",
    )?;
    let wait: StoredModelTurnWaitPayload =
        decode_typed_payload(&wait_payload, "Model tool continuation wait")?;
    if wait.model_turn_id != turn.model_turn_id
        || &wait.model_job_id != model_job_id
        || wait.round_ordinal != turn.round_ordinal
    {
        return Err(RepositoryError::Conflict(
            "Model tool continuation frozen owner",
        ));
    }
    let source_job = load_job_by_text(
        transaction,
        &run.tenant_id,
        &wait.source_orchestration_job_id.to_string(),
    )
    .await?;
    require_orchestration_job(&source_job)?;
    let source_payload: OrchestrationJobPayload =
        decode_orchestration_job_payload(&source_job.payload)?;
    if source_job.state != JobState::Succeeded.as_str()
        || source_job.job_kind != JobKind::OrchestrationNode.as_str()
        || source_job.terminal_at.is_none()
        || source_job.owner_id != turn.node_execution_id.to_string()
        || source_job.run_id.as_deref() != Some(run.run_id.as_str())
        || source_payload.root_scope_id != wait.root_scope_id
        || source_payload.bindings_digest != run.bindings.canonical_digest
    {
        return Err(RepositoryError::Conflict(
            "Model tool continuation source Job",
        ));
    }
    if let Some(goal) = model_run_convergence_goal(&run, database_now)? {
        return settle_suppressed_model_continuation(
            transaction,
            &run,
            &goal,
            true,
            &mutations.run_event_id,
            &mutations.run_outbox_id,
            &turn.model_turn_id,
            database_now,
        )
        .await;
    }
    let total_capability_calls = wait
        .total_capability_calls
        .checked_add(u32::from(tool_intent_count))
        .ok_or_else(|| RepositoryError::Conflict("Model tool call budget overflow"))?;
    if wait.model_turn_id != turn.model_turn_id
        || &wait.model_job_id != model_job_id
        || wait.round_ordinal != turn.round_ordinal
        || turn.round_ordinal >= wait.maximum_rounds
        || tool_intent_count > wait.maximum_parallel_calls_per_round
        || total_capability_calls > wait.maximum_capability_calls
        || wait.token_budget == 0
    {
        return Err(RepositoryError::Conflict(
            "Model tool continuation budget or owner",
        ));
    }
    let continuation = insight_platform_orchestrator::ModelToolContinuation {
        model_turn_id: turn.model_turn_id.clone(),
        response_value_id: output.value_id.clone(),
        response_digest: output.content_digest.clone(),
        round_ordinal: turn.round_ordinal,
        tool_intent_count,
        results: Vec::new(),
    };
    continuation
        .validate()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let next_wait = TypedPayload::with_limit(
        1,
        &StoredModelToolContinuationWaitPayload {
            plan_node_key: wait.plan_node_key,
            plan_digest: wait.plan_digest,
            source_orchestration_job_id: wait.source_orchestration_job_id,
            model_turn_id: turn.model_turn_id.clone(),
            model_job_id: model_job_id.clone(),
            response_value_id: output.value_id.clone(),
            response_digest: output.content_digest.clone(),
            round_ordinal: turn.round_ordinal,
            tool_intent_count,
            total_capability_calls,
            output_port: wait.output_port,
            resume_plan_node_key: wait.resume_plan_node_key,
            resume_node_kind: wait.resume_node_kind,
            root_scope_id: wait.root_scope_id.clone(),
            continuation_attempt_limit: wait.continuation_attempt_limit,
            retry_backoff_milliseconds: wait.retry_backoff_milliseconds,
            priority: wait.priority,
            deadline: wait.deadline,
            maximum_rounds: wait.maximum_rounds,
            maximum_capability_calls: wait.maximum_capability_calls,
            maximum_parallel_calls_per_round: wait.maximum_parallel_calls_per_round,
            token_budget: wait
                .token_budget
                .checked_sub(
                    output
                        .response
                        .usage
                        .input_tokens
                        .unwrap_or_default()
                        .checked_add(output.response.usage.output_tokens.unwrap_or_default())
                        .ok_or_else(|| RepositoryError::Conflict("Model token usage overflow"))?,
                )
                .ok_or_else(|| RepositoryError::Conflict("Model token budget exhausted"))?,
        },
        262_144,
    )?;
    let node_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'ready', version = version + 1, enqueue_round = 0,
            payload_schema_version = $4, payload = $5, payload_digest = $6,
            updated_at = $7
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'node_execution' AND state = 'waiting'
          AND terminal_at IS NULL
        RETURNING version
        "#,
    )
    .bind(turn.tenant_id.to_string())
    .bind(turn.node_execution_id.to_string())
    .bind(node_row.try_get::<i64, _>("version")?)
    .bind(next_wait.schema_version)
    .bind(&next_wait.value)
    .bind(&next_wait.digest)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "Model tool continuation Node handoff",
    ))?;
    let job_payload = OrchestrationJobPayload {
        external_leaf_completion: None,
        bindings_digest: run.bindings.canonical_digest.clone(),
        node_execution_id: turn.node_execution_id.clone(),
        root_scope_id: wait.root_scope_id,
        retry_backoff_milliseconds: wait.retry_backoff_milliseconds,
        wake_contract: None,
        convergence_failure: None,
        model_tool_continuation: Some(continuation),
    };
    job_payload
        .validate()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let job_payload = job_payload.to_payload()?;
    let continuation_job = job_from_row(
        sqlx::query(
            r#"
            INSERT INTO insight_platform.jobs (
                tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, trace_id, run_id, node_id, state, attempt_limit, scheduled_at, deadline, priority, request_digest, payload_schema_version, payload, payload_digest, scheduler_partition_id, execution_requirement_version, execution_requirement, execution_requirement_digest
            ) VALUES (
                $1, $2, 'orchestration_node', 'orchestration', 'node_execution', $3, (SELECT trace_id FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $4), $4, $3, 'ready', $5, $6, $7, $8, $9, $10, $11, $12, (SELECT scheduler_partition_id FROM insight_platform.tenants WHERE tenant_id=$1), (SELECT execution_requirement_version FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement_digest FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4)
            ) RETURNING *
            "#,
        )
        .bind(turn.tenant_id.to_string())
        .bind(mutations.continuation_job_id.to_string())
        .bind(turn.node_execution_id.to_string())
        .bind(turn.run_id.to_string())
        .bind(wait.continuation_attempt_limit)
        .bind(database_now)
        .bind(wait.deadline)
        .bind(scheduler_priority_to_database(wait.priority))
        .bind(&job_payload.digest)
        .bind(job_payload.schema_version)
        .bind(&job_payload.value)
        .bind(&job_payload.digest)
        .fetch_one(&mut **transaction)
        .await?,
    )?;
    let mut current = run.current.clone();
    if run.state == RunState::Waiting.as_str() {
        current.waiting_reason = None;
    }
    current.validate(&turn.run_id)?;
    let current_payload = TypedPayload::from_versioned(1, &current, 1_048_576)?;
    let resumed_run = run_from_row(
        sqlx::query(
            r#"
            UPDATE insight_platform.runs
            SET state = CASE WHEN state = 'waiting' THEN 'running' ELSE state END,
                version = version + 1, active_work_count = active_work_count - 1,
                current_schema_version = $4, current_payload = $5,
                current_payload_digest = $6, updated_at = $7
            WHERE tenant_id = $1 AND run_id = $2 AND version = $3
              AND state IN ('running', 'waiting') AND active_work_count > 0
              AND terminal_at IS NULL
            RETURNING *
            "#,
        )
        .bind(&run.tenant_id)
        .bind(&run.run_id)
        .bind(run.version)
        .bind(current_payload.schema_version)
        .bind(&current_payload.value)
        .bind(&current_payload.digest)
        .bind(database_now)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict(
            "Model tool continuation Run handoff",
        ))?,
    )?;
    let evidence = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "continuation_job_id": continuation_job.job_id,
            "model_turn_id": turn.model_turn_id,
            "response_value_id": output.value_id,
            "round_ordinal": turn.round_ordinal,
            "tool_intent_count": tool_intent_count,
        }),
        65_536,
    )?;
    append_scheduler_event(
        transaction,
        &run.tenant_id,
        &mutations.run_event_id,
        &mutations.run_outbox_id,
        "run",
        &run.run_id,
        resumed_run.version,
        Some(&run.run_id),
        "run.model_tool_continuation_ready",
        &evidence,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &run.tenant_id,
        &mutations.node_event_id,
        &mutations.node_outbox_id,
        "node_execution",
        &turn.node_execution_id.to_string(),
        node_version,
        Some(&run.run_id),
        "node.model_tool_continuation_ready",
        &evidence,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &run.tenant_id,
        &mutations.continuation_job_event_id,
        &mutations.continuation_job_outbox_id,
        "job",
        &continuation_job.job_id,
        continuation_job.version,
        Some(&run.run_id),
        "job.ready",
        &evidence,
    )
    .await?;
    Ok(())
}

async fn settle_external_leaf_success_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    owner: ExternalLeafSuccessOwner<'_>,
    output: &ExactInvocationValueRef,
    mutations: &insight_platform_contracts::ExternalLeafResumeMutationIds,
    scope_limits: ScopeEnvironmentLimits,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    mutations
        .validate()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let run = load_run_for_update(transaction, owner.tenant_id, owner.run_id).await?;
    let node_row = sqlx::query(
        r#"
        SELECT scope_id, state, version, payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id = $2 AND run_id = $3
          AND record_kind = 'node_execution'
        FOR UPDATE
        "#,
    )
    .bind(owner.tenant_id.to_string())
    .bind(owner.node_id.to_string())
    .bind(owner.run_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Context leaf Node"))?;
    if node_row.try_get::<String, _>("state")? != NodeExecutionState::Waiting.as_str() {
        return Err(RepositoryError::Conflict("Context leaf terminal state"));
    }
    let wait_payload = payload_from_row(
        &node_row,
        "payload_schema_version",
        "payload",
        "payload_digest",
    )?;
    let wait = match owner.kind {
        ExternalLeafSuccessKind::Capability => {
            let stored: StoredCapabilityInvocationWaitPayload =
                decode_typed_payload(&wait_payload, "Capability leaf wait")?;
            if &stored.invocation_id != owner.owner_id
                || stored.capability_job_id.as_ref() != Some(owner.owner_job_id)
            {
                return Err(RepositoryError::Conflict("Capability leaf owner"));
            }
            ExternalLeafSuccessWait {
                plan_digest: stored.plan_digest,
                source_orchestration_job_id: stored.source_orchestration_job_id,
                output_port: stored.output_port,
                root_scope_id: stored.root_scope_id,
                continuation_attempt_limit: stored.continuation_attempt_limit,
                retry_backoff_milliseconds: stored.retry_backoff_milliseconds,
                priority: stored.priority,
                deadline: stored.deadline,
            }
        }
        ExternalLeafSuccessKind::Child => {
            let stored: StoredChildRunWaitPayload =
                decode_typed_payload(&wait_payload, "Child leaf wait")?;
            if &stored.child_run_id != owner.owner_id || &stored.child_link_id != owner.owner_job_id
            {
                return Err(RepositoryError::Conflict("Child leaf owner"));
            }
            ExternalLeafSuccessWait {
                plan_digest: stored.plan_digest,
                source_orchestration_job_id: stored.source_orchestration_job_id,
                output_port: stored.output_port,
                root_scope_id: stored.root_scope_id,
                continuation_attempt_limit: stored.continuation_attempt_limit,
                retry_backoff_milliseconds: stored.retry_backoff_milliseconds,
                priority: stored.priority,
                deadline: stored.deadline,
            }
        }
        ExternalLeafSuccessKind::Context => {
            let stored: StoredContextQueryWaitPayload =
                decode_typed_payload(&wait_payload, "Context leaf wait")?;
            if &stored.context_query_id != owner.owner_id
                || &stored.context_job_id != owner.owner_job_id
            {
                return Err(RepositoryError::Conflict("Context leaf owner"));
            }
            ExternalLeafSuccessWait {
                plan_digest: stored.plan_digest,
                source_orchestration_job_id: stored.source_orchestration_job_id,
                output_port: stored.result_port,
                root_scope_id: stored.root_scope_id,
                continuation_attempt_limit: stored.continuation_attempt_limit,
                retry_backoff_milliseconds: stored.retry_backoff_milliseconds,
                priority: stored.priority,
                deadline: stored.deadline,
            }
        }
        ExternalLeafSuccessKind::Model => {
            let stored: StoredModelTurnWaitPayload =
                decode_typed_payload(&wait_payload, "Model leaf wait")?;
            if &stored.model_turn_id != owner.owner_id || &stored.model_job_id != owner.owner_job_id
            {
                return Err(RepositoryError::Conflict("Model leaf owner"));
            }
            ExternalLeafSuccessWait {
                plan_digest: stored.plan_digest,
                source_orchestration_job_id: stored.source_orchestration_job_id,
                output_port: stored.output_port,
                root_scope_id: stored.root_scope_id,
                continuation_attempt_limit: stored.continuation_attempt_limit,
                retry_backoff_milliseconds: stored.retry_backoff_milliseconds,
                priority: stored.priority,
                deadline: stored.deadline,
            }
        }
    };
    if wait.source_orchestration_job_id.kind() != ResourceKind::Job
        || wait.output_port.schema_digest() != &output.schema_digest
        || &output.run_id != owner.run_id
        || output.producing_node_id.as_ref() != Some(owner.node_id)
        || owner.output_value_id != &output.value_id
    {
        return Err(RepositoryError::Conflict("Context leaf terminal contract"));
    }
    let plan_row = sqlx::query(
        r#"
        SELECT payload_schema_version, payload, payload_digest
        FROM insight_platform.resource_versions
        WHERE tenant_id = $1 AND resource_version_id = $2
          AND resource_version_kind = 'agent_plan_revision' AND content_digest = $3
        "#,
    )
    .bind(&run.tenant_id)
    .bind(run.bindings.plan.revision_id.to_string())
    .bind(run.bindings.plan.semantic_digest.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("exact Agent Plan revision"))?;
    let plan_payload = payload_from_row(
        &plan_row,
        "payload_schema_version",
        "payload",
        "payload_digest",
    )?;
    let published = decode_published_version_payload(&plan_payload)?;
    let ResourceDocument::Agent(agent) = published.document else {
        return Err(RepositoryError::CorruptRow(
            "Agent Plan revision contains a non-Agent document".to_owned(),
        ));
    };
    if agent.typed_plan_digest != wait.plan_digest {
        return Err(RepositoryError::Conflict("Context leaf exact Plan"));
    }
    let source_job = load_job_by_text(
        transaction,
        &run.tenant_id,
        &wait.source_orchestration_job_id.to_string(),
    )
    .await?;
    require_orchestration_job(&source_job)?;
    let source_payload: OrchestrationJobPayload =
        decode_orchestration_job_payload(&source_job.payload)?;
    if source_job.state != JobState::Succeeded.as_str()
        || source_job.terminal_at.is_none()
        || source_job.owner_id != owner.node_id.to_string()
        || source_job.run_id.as_deref() != Some(run.run_id.as_str())
        || source_job.attempt_limit != wait.continuation_attempt_limit
        || source_job.priority != wait.priority
        || source_job.deadline.min(run.deadline) != wait.deadline
        || source_payload.root_scope_id != wait.root_scope_id
        || source_payload.retry_backoff_milliseconds != wait.retry_backoff_milliseconds
        || source_payload.bindings_digest != run.bindings.canonical_digest
    {
        return Err(RepositoryError::Conflict(
            "Context source orchestration Job contract",
        ));
    }

    let scope_id: ResourceId = node_row.try_get::<String, _>("scope_id")?.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let scope_row = sqlx::query(
        r#"
        SELECT node_kind, state, version, parent_node_id,
               payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id = $2 AND run_id = $3
          AND record_kind = 'scope_instance'
        FOR UPDATE
        "#,
    )
    .bind(owner.tenant_id.to_string())
    .bind(scope_id.to_string())
    .bind(owner.run_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Context leaf Scope"))?;
    if scope_row.try_get::<String, _>("state")? != ScopeState::Open.as_str() {
        return Err(RepositoryError::Conflict("Context leaf open Scope"));
    }
    let scope_payload = payload_from_row(
        &scope_row,
        "payload_schema_version",
        "payload",
        "payload_digest",
    )?;
    let binding = ExactRunValueRef {
        value_id: output.value_id.clone(),
        schema_digest: output.schema_digest.clone(),
        content_digest: output.content_digest.clone(),
    };
    let next_scope_payload = match scope_row.try_get::<String, _>("node_kind")?.as_str() {
        "root" => {
            let mut root: StoredRootScopePayload =
                decode_typed_payload(&scope_payload, "root Scope")?;
            root.environment
                .bind_new(wait.output_port.clone(), binding, scope_limits)?;
            TypedPayload::with_limit(1, &root, 262_144)?
        }
        "parallel_leg" | "loop_iteration" | "map_item" => {
            let mut nested: StoredControllerScopePayload =
                decode_typed_payload(&scope_payload, "controller Scope")?;
            nested
                .environment
                .bind_new(wait.output_port.clone(), binding, scope_limits)?;
            TypedPayload::with_limit(1, &nested, 262_144)?
        }
        _ => return Err(RepositoryError::Conflict("Context leaf Scope kind")),
    };
    let scope_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET version = version + 1, payload_schema_version = $4,
            payload = $5, payload_digest = $6, updated_at = $7
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'scope_instance' AND state = 'open'
        RETURNING version
        "#,
    )
    .bind(owner.tenant_id.to_string())
    .bind(scope_id.to_string())
    .bind(scope_row.try_get::<i64, _>("version")?)
    .bind(next_scope_payload.schema_version)
    .bind(&next_scope_payload.value)
    .bind(&next_scope_payload.digest)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Context leaf Scope binding"))?;
    let leaf_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'ready', version = version + 1,
            enqueue_round = 0, updated_at = $4
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'node_execution' AND state = 'waiting' AND terminal_at IS NULL
        RETURNING version
        "#,
    )
    .bind(owner.tenant_id.to_string())
    .bind(owner.node_id.to_string())
    .bind(node_row.try_get::<i64, _>("version")?)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Context leaf Node terminal"))?;
    let completion_owner = match owner.kind {
        ExternalLeafSuccessKind::Child => {
            insight_platform_orchestrator::ExternalLeafCompletionOwner::Child {
                child_run_id: owner.owner_id.clone(),
                child_link_id: owner.owner_job_id.clone(),
            }
        }
        ExternalLeafSuccessKind::Context => {
            insight_platform_orchestrator::ExternalLeafCompletionOwner::Context {
                context_query_id: owner.owner_id.clone(),
                context_job_id: owner.owner_job_id.clone(),
            }
        }
        ExternalLeafSuccessKind::Model => {
            insight_platform_orchestrator::ExternalLeafCompletionOwner::Model {
                model_turn_id: owner.owner_id.clone(),
                model_job_id: owner.owner_job_id.clone(),
            }
        }
        ExternalLeafSuccessKind::Capability => {
            insight_platform_orchestrator::ExternalLeafCompletionOwner::Capability {
                invocation_id: owner.owner_id.clone(),
                invocation_job_id: owner.owner_job_id.clone(),
            }
        }
    };
    let job_payload = OrchestrationJobPayload {
        bindings_digest: run.bindings.canonical_digest.clone(),
        node_execution_id: owner.node_id.clone(),
        root_scope_id: wait.root_scope_id,
        retry_backoff_milliseconds: wait.retry_backoff_milliseconds,
        wake_contract: None,
        convergence_failure: None,
        model_tool_continuation: None,
        external_leaf_completion: Some(insight_platform_orchestrator::ExternalLeafCompletion {
            source_orchestration_job_id: wait.source_orchestration_job_id,
            owner: completion_owner,
            output: ExactRunValueRef {
                value_id: output.value_id.clone(),
                schema_digest: output.schema_digest.clone(),
                content_digest: output.content_digest.clone(),
            },
        }),
    };
    let job_payload = job_payload.to_payload()?;
    let continuation_job = sqlx::query(
        r#"
        INSERT INTO insight_platform.jobs (
                tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, trace_id, run_id, node_id, state, attempt_limit, scheduled_at, deadline, priority, request_digest, payload_schema_version, payload, payload_digest, scheduler_partition_id, execution_requirement_version, execution_requirement, execution_requirement_digest
            ) VALUES (
                $1, $2, 'orchestration_node', 'orchestration', 'node_execution', $3, (SELECT trace_id FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $4), $4, $3, 'ready', $5, $6, $7, $8, $9, $10, $11, $12, (SELECT scheduler_partition_id FROM insight_platform.tenants WHERE tenant_id=$1), (SELECT execution_requirement_version FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement_digest FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4)
            ) RETURNING *
        "#,
    )
    .bind(owner.tenant_id.to_string())
    .bind(mutations.continuation_job_id.to_string())
    .bind(owner.node_id.to_string())
    .bind(owner.run_id.to_string())
    .bind(wait.continuation_attempt_limit)
    .bind(database_now)
    .bind(wait.deadline)
    .bind(scheduler_priority_to_database(wait.priority))
    .bind(&job_payload.digest)
    .bind(job_payload.schema_version)
    .bind(&job_payload.value)
    .bind(&job_payload.digest)
    .fetch_one(&mut **transaction)
    .await?;
    let continuation_job = job_from_row(continuation_job)?;
    let mut current = run.current.clone();
    if run.state == RunState::Waiting.as_str() {
        current.waiting_reason = None;
    }
    current.validate(owner.run_id)?;
    let current_payload = TypedPayload::from_versioned(1, &current, 1_048_576)?;
    let release_active_permit = matches!(
        owner.kind,
        ExternalLeafSuccessKind::Context
            | ExternalLeafSuccessKind::Model
            | ExternalLeafSuccessKind::Capability
    );
    let run_row = sqlx::query(
        r#"
        UPDATE insight_platform.runs
        SET state = CASE WHEN state = 'waiting' THEN 'running' ELSE state END,
            version = version + 1, active_work_count = active_work_count - $8,
            current_schema_version = $4,
            current_payload = $5, current_payload_digest = $6, updated_at = $7
        WHERE tenant_id = $1 AND run_id = $2 AND version = $3
          AND state IN ('running', 'waiting', 'cancelling') AND active_work_count >= $8
          AND terminal_at IS NULL
        RETURNING *
        "#,
    )
    .bind(&run.tenant_id)
    .bind(&run.run_id)
    .bind(run.version)
    .bind(current_payload.schema_version)
    .bind(&current_payload.value)
    .bind(&current_payload.digest)
    .bind(database_now)
    .bind(i32::from(release_active_permit))
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Context leaf Run resume"))?;
    let resumed_run = run_from_row(run_row)?;
    let common = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "owner_job_id": owner.owner_job_id,
            "owner_id": owner.owner_id,
            "continuation_job_id": continuation_job.job_id,
            "node_execution_id": owner.node_id,
            "output_value_id": output.value_id,
            "scope_version": scope_version,
        }),
        65_536,
    )?;
    append_scheduler_event(
        transaction,
        &run.tenant_id,
        &mutations.run_event_id,
        &mutations.run_outbox_id,
        "run",
        &run.run_id,
        resumed_run.version,
        Some(&run.run_id),
        "run.external_leaf_resumed",
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &run.tenant_id,
        &mutations.leaf_node_event_id,
        &mutations.leaf_node_outbox_id,
        "node_execution",
        &owner.node_id.to_string(),
        leaf_version,
        Some(&run.run_id),
        "node.external_leaf_completion_ready",
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &run.tenant_id,
        &mutations.continuation_job_event_id,
        &mutations.continuation_job_outbox_id,
        "job",
        &continuation_job.job_id,
        continuation_job.version,
        Some(&run.run_id),
        "job.ready",
        &common,
    )
    .await?;
    Ok(())
}

#[derive(Debug)]
struct ExternalLeafFailureWait {
    plan_digest: Sha256Digest,
    source_orchestration_job_id: ResourceId,
    root_scope_id: ResourceId,
    continuation_attempt_limit: i32,
    retry_backoff_milliseconds: u64,
    priority: SchedulerPriority,
    deadline: DateTime<Utc>,
}

#[allow(clippy::too_many_arguments)]
async fn settle_external_leaf_failure_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    run_id: &ResourceId,
    node_id: &ResourceId,
    owner_job_id: &ResourceId,
    owner_kind: &'static str,
    failure: &Failure,
    release_active_permit: bool,
    wait: ExternalLeafFailureWait,
    mutations: &insight_platform_contracts::ExternalLeafFailureMutationIds,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    failure
        .validate(1_024)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    mutations
        .validate()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let run = load_run_for_update(transaction, tenant_id, run_id).await?;
    let node_row = sqlx::query(
        r#"
        SELECT state, version
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id = $2 AND run_id = $3
          AND record_kind = 'node_execution'
        FOR UPDATE
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(node_id.to_string())
    .bind(run_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("external failure leaf Node"))?;
    if node_row.try_get::<String, _>("state")? != NodeExecutionState::Waiting.as_str() {
        return Err(RepositoryError::Conflict(
            "external failure leaf terminal state",
        ));
    }
    let plan_row = sqlx::query(
        r#"
        SELECT payload_schema_version, payload, payload_digest
        FROM insight_platform.resource_versions
        WHERE tenant_id = $1 AND resource_version_id = $2
          AND resource_version_kind = 'agent_plan_revision' AND content_digest = $3
        "#,
    )
    .bind(&run.tenant_id)
    .bind(run.bindings.plan.revision_id.to_string())
    .bind(run.bindings.plan.semantic_digest.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("exact Agent Plan revision"))?;
    let plan_payload = payload_from_row(
        &plan_row,
        "payload_schema_version",
        "payload",
        "payload_digest",
    )?;
    let published = decode_published_version_payload(&plan_payload)?;
    let ResourceDocument::Agent(agent) = published.document else {
        return Err(RepositoryError::CorruptRow(
            "Agent Plan revision contains a non-Agent document".to_owned(),
        ));
    };
    if agent.typed_plan_digest != wait.plan_digest {
        return Err(RepositoryError::Conflict(
            "external failure leaf exact Plan",
        ));
    }
    let source_job = load_job_by_text(
        transaction,
        &run.tenant_id,
        &wait.source_orchestration_job_id.to_string(),
    )
    .await?;
    require_orchestration_job(&source_job)?;
    let source_payload: OrchestrationJobPayload =
        decode_orchestration_job_payload(&source_job.payload)?;
    if source_job.state != JobState::Succeeded.as_str()
        || source_job.terminal_at.is_none()
        || source_job.owner_id != node_id.to_string()
        || source_job.run_id.as_deref() != Some(run.run_id.as_str())
        || source_job.attempt_limit != wait.continuation_attempt_limit
        || source_job.priority != wait.priority
        || source_job.deadline.min(run.deadline) != wait.deadline
        || source_payload.root_scope_id != wait.root_scope_id
        || source_payload.retry_backoff_milliseconds != wait.retry_backoff_milliseconds
        || source_payload.bindings_digest != run.bindings.canonical_digest
        || source_payload.convergence_failure.is_some()
    {
        return Err(RepositoryError::Conflict(
            "external failure source orchestration Job contract",
        ));
    }
    let node_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'ready', version = version + 1, enqueue_round = 0,
            updated_at = $4
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'node_execution' AND state = 'waiting'
          AND terminal_at IS NULL
        RETURNING version
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(node_id.to_string())
    .bind(node_row.try_get::<i64, _>("version")?)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "external failure leaf convergence",
    ))?;
    let job_payload = OrchestrationJobPayload {
        external_leaf_completion: None,
        bindings_digest: run.bindings.canonical_digest.clone(),
        node_execution_id: node_id.clone(),
        root_scope_id: wait.root_scope_id,
        retry_backoff_milliseconds: wait.retry_backoff_milliseconds,
        wake_contract: None,
        convergence_failure: Some(failure.clone()),
        model_tool_continuation: None,
    };
    job_payload
        .validate()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let job_payload = job_payload.to_payload()?;
    let convergence_job = sqlx::query(
        r#"
        INSERT INTO insight_platform.jobs (
                tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, trace_id, run_id, node_id, state, attempt_limit, scheduled_at, deadline, priority, request_digest, payload_schema_version, payload, payload_digest, scheduler_partition_id, execution_requirement_version, execution_requirement, execution_requirement_digest
            ) VALUES (
                $1, $2, 'orchestration_node', 'orchestration', 'node_execution', $3, (SELECT trace_id FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $4), $4, $3, 'ready', $5, $6, $7, $8, $9, $10, $11, $12, (SELECT scheduler_partition_id FROM insight_platform.tenants WHERE tenant_id=$1), (SELECT execution_requirement_version FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement_digest FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4)
            ) RETURNING *
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(mutations.convergence_job_id.to_string())
    .bind(node_id.to_string())
    .bind(run_id.to_string())
    .bind(wait.continuation_attempt_limit)
    .bind(database_now)
    .bind(wait.deadline)
    .bind(scheduler_priority_to_database(wait.priority))
    .bind(&job_payload.digest)
    .bind(job_payload.schema_version)
    .bind(&job_payload.value)
    .bind(&job_payload.digest)
    .fetch_one(&mut **transaction)
    .await?;
    let convergence_job = job_from_row(convergence_job)?;
    let mut current = run.current.clone();
    if run.state == RunState::Waiting.as_str() {
        current.waiting_reason = None;
    }
    current.validate(run_id)?;
    let current_payload = TypedPayload::from_versioned(1, &current, 1_048_576)?;
    let run_row = sqlx::query(
        r#"
        UPDATE insight_platform.runs
        SET state = CASE WHEN state = 'waiting' THEN 'running' ELSE state END,
            version = version + 1,
            active_work_count = active_work_count - $8,
            current_schema_version = $4,
            current_payload = $5, current_payload_digest = $6, updated_at = $7
        WHERE tenant_id = $1 AND run_id = $2 AND version = $3
          AND state IN ('running', 'waiting', 'cancelling') AND active_work_count >= $8
          AND terminal_at IS NULL
        RETURNING *
        "#,
    )
    .bind(&run.tenant_id)
    .bind(&run.run_id)
    .bind(run.version)
    .bind(current_payload.schema_version)
    .bind(&current_payload.value)
    .bind(&current_payload.digest)
    .bind(database_now)
    .bind(i32::from(release_active_permit))
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "external failure leaf Run handoff",
    ))?;
    let resumed_run = run_from_row(run_row)?;
    let common = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "convergence_job_id": convergence_job.job_id,
            "failure": failure,
            "owner_job_id": owner_job_id,
            "owner_kind": owner_kind,
        }),
        65_536,
    )?;
    append_scheduler_event(
        transaction,
        &run.tenant_id,
        &mutations.run_event_id,
        &mutations.run_outbox_id,
        "run",
        &run.run_id,
        resumed_run.version,
        Some(&run.run_id),
        "run.external_leaf_failure_pending",
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &run.tenant_id,
        &mutations.leaf_node_event_id,
        &mutations.leaf_node_outbox_id,
        "node_execution",
        &node_id.to_string(),
        node_version,
        Some(&run.run_id),
        "node.failure_convergence_ready",
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &run.tenant_id,
        &mutations.convergence_job_event_id,
        &mutations.convergence_job_outbox_id,
        "job",
        &convergence_job.job_id,
        convergence_job.version,
        Some(&run.run_id),
        "job.ready",
        &common,
    )
    .await?;
    Ok(())
}

pub(crate) async fn settle_context_leaf_failure_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    query: &ContextQueryRecord,
    context_job_id: &ResourceId,
    failure: &Failure,
    mutations: &insight_platform_contracts::ExternalLeafFailureMutationIds,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id = $2 AND run_id = $3
          AND record_kind = 'node_execution' AND state = 'waiting'
        "#,
    )
    .bind(query.tenant_id.to_string())
    .bind(query.node_execution_id.to_string())
    .bind(query.run_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Context failure leaf wait"))?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let wait: StoredContextQueryWaitPayload =
        decode_typed_payload(&payload, "Context failure leaf wait")?;
    if wait.context_query_id != query.context_query_id || &wait.context_job_id != context_job_id {
        return Err(RepositoryError::Conflict("Context failure leaf owner"));
    }
    settle_external_leaf_failure_in_transaction(
        transaction,
        &query.tenant_id,
        &query.run_id,
        &query.node_execution_id,
        context_job_id,
        "context_query",
        failure,
        true,
        ExternalLeafFailureWait {
            plan_digest: wait.plan_digest,
            source_orchestration_job_id: wait.source_orchestration_job_id,
            root_scope_id: wait.root_scope_id,
            continuation_attempt_limit: wait.continuation_attempt_limit,
            retry_backoff_milliseconds: wait.retry_backoff_milliseconds,
            priority: wait.priority,
            deadline: wait.deadline,
        },
        mutations,
        database_now,
    )
    .await
}

pub(crate) async fn settle_capability_leaf_failure_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &CapabilityInvocationRecord,
    capability_job_id: &ResourceId,
    failure: &Failure,
    mutations: &insight_platform_contracts::ExternalLeafFailureMutationIds,
    release_active_permit: bool,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id = $2 AND run_id = $3
          AND record_kind = 'node_execution' AND state = 'waiting'
        "#,
    )
    .bind(invocation.tenant_id.to_string())
    .bind(invocation.node_execution_id.to_string())
    .bind(invocation.run_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Capability failure leaf wait"))?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let wait: StoredCapabilityInvocationWaitPayload =
        decode_typed_payload(&payload, "Capability failure leaf wait")?;
    if wait.invocation_id != invocation.invocation_id
        || wait.capability_job_id.as_ref() != Some(capability_job_id)
    {
        return Err(RepositoryError::Conflict("Capability failure leaf owner"));
    }
    settle_external_leaf_failure_in_transaction(
        transaction,
        &invocation.tenant_id,
        &invocation.run_id,
        &invocation.node_execution_id,
        capability_job_id,
        "capability_invocation",
        failure,
        release_active_permit,
        ExternalLeafFailureWait {
            plan_digest: wait.plan_digest,
            source_orchestration_job_id: wait.source_orchestration_job_id,
            root_scope_id: wait.root_scope_id,
            continuation_attempt_limit: wait.continuation_attempt_limit,
            retry_backoff_milliseconds: wait.retry_backoff_milliseconds,
            priority: wait.priority,
            deadline: wait.deadline,
        },
        mutations,
        database_now,
    )
    .await
}

pub(crate) async fn settle_model_tool_failure_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &CapabilityInvocationRecord,
    capability_job_id: &ResourceId,
    failure: &Failure,
    mutations: &insight_platform_contracts::ExternalLeafFailureMutationIds,
    release_active_permit: bool,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let InvocationOrigin::ModelToolCall {
        model_turn_id,
        model_call_id_digest,
    } = &invocation.payload.admission.origin_key
    else {
        return Err(RepositoryError::Conflict("Model tool failure origin"));
    };
    let run = load_run_for_update(transaction, &invocation.tenant_id, &invocation.run_id).await?;
    let row = sqlx::query(
        r#"
        SELECT payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id = $2 AND run_id = $3
          AND record_kind = 'node_execution' AND state = 'waiting'
        "#,
    )
    .bind(invocation.tenant_id.to_string())
    .bind(invocation.node_execution_id.to_string())
    .bind(invocation.run_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Model tool failure wait"))?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let wait: StoredModelToolBatchWaitPayload =
        decode_typed_payload(&payload, "Model tool failure wait")?;
    if &wait.continuation.model_turn_id != model_turn_id
        || !wait.calls.iter().any(|call| {
            call.invocation_id == invocation.invocation_id
                && &call.call_id_digest == model_call_id_digest
                && &call.capability_job_id == capability_job_id
                && call.result.is_none()
        })
    {
        return Err(RepositoryError::Conflict("Model tool failure owner"));
    }
    if let Some(goal) = model_run_convergence_goal(&run, database_now)? {
        return settle_suppressed_model_continuation(
            transaction,
            &run,
            &goal,
            release_active_permit,
            &mutations.run_event_id,
            &mutations.run_outbox_id,
            &invocation.invocation_id,
            database_now,
        )
        .await;
    }
    for sibling_call in wait
        .calls
        .iter()
        .filter(|call| call.invocation_id != invocation.invocation_id && call.result.is_none())
    {
        let sibling = crate::invocation_repository::load_capability_invocation(
            transaction,
            &invocation.tenant_id,
            &sibling_call.invocation_id,
            true,
        )
        .await?;
        if matches!(
            sibling.state,
            InvocationState::Succeeded
                | InvocationState::Failed
                | InvocationState::Cancelled
                | InvocationState::TimedOut
        ) {
            continue;
        }
        let (current_job, current_projection, current_job_payload) =
            if let Some(job_id) = sibling.payload.current_job_id.as_ref() {
                let job = crate::capability_execution_repository::load_capability_job(
                    transaction,
                    &invocation.tenant_id,
                    job_id,
                    true,
                )
                .await?;
                let projection = job_projection(&job)?;
                let payload: CapabilityJobPayload =
                    decode_versioned_payload(&job.payload, "Model tool sibling Capability Job")?;
                (Some(job), Some(projection), Some(payload))
            } else {
                (None, None, None)
            };
        let decision = decide_capability_control(
            &sibling,
            current_projection.as_ref(),
            current_job_payload.as_ref(),
            CapabilityControlKind::Cancel,
            database_now,
        )?;
        if decision.invocation.state != InvocationState::Cancelled {
            return Err(RepositoryError::Conflict(
                "active Model tool sibling violated sequential claim",
            ));
        }
        if let (Some(current_job), Some(next_job), Some(next_payload)) = (
            current_job.as_ref(),
            decision.job.as_ref(),
            decision.job_payload.as_ref(),
        ) {
            crate::capability_execution_repository::update_capability_job(
                transaction,
                current_job,
                next_job,
                next_payload,
                database_now,
            )
            .await?;
        }
        crate::capability_execution_repository::update_capability_invocation(
            transaction,
            &sibling,
            &decision.invocation,
        )
        .await?;
        if let Some(task_id) = sibling.payload.approval_task_id.as_ref() {
            let task = load_task_for_update(transaction, &invocation.tenant_id, task_id).await?;
            let projection = task_projection(&task)?;
            if projection.state == TaskState::Pending {
                let next = decide_task_resolution(
                    &projection,
                    DomainResolveTask {
                        expected_generation: projection.generation,
                        expected_version: projection.version,
                        target: TaskState::Cancelled,
                        principal: None,
                        response_value_id: None,
                        response_schema_digest: None,
                    },
                    database_now,
                )?;
                let payload = TypedPayload::new(2, &next.payload)?;
                let updated = sqlx::query(
                    r#"
                    UPDATE insight_platform.tasks
                    SET state = 'cancelled', version = $4,
                        payload_schema_version = $5, payload = $6, payload_digest = $7,
                        responded_at = $8, updated_at = $8
                    WHERE tenant_id = $1 AND task_id = $2 AND version = $3
                      AND state = 'pending' AND responded_at IS NULL
                    "#,
                )
                .bind(invocation.tenant_id.to_string())
                .bind(task_id.to_string())
                .bind(i64::try_from(projection.version).map_err(|_| {
                    RepositoryError::CorruptRow("Model tool sibling Task version".to_owned())
                })?)
                .bind(i64::try_from(next.version).map_err(|_| {
                    RepositoryError::CorruptRow("Model tool sibling Task version".to_owned())
                })?)
                .bind(payload.schema_version)
                .bind(&payload.value)
                .bind(&payload.digest)
                .bind(database_now)
                .execute(&mut **transaction)
                .await?;
                if updated.rows_affected() != 1 {
                    return Err(RepositoryError::Conflict(
                        "Model tool sibling Approval Task",
                    ));
                }
            }
        }
        append_scheduler_event(
            transaction,
            &invocation.tenant_id.to_string(),
            &sibling_call.sibling_cancel_event_id,
            &sibling_call.sibling_cancel_outbox_id,
            "capability_invocation",
            &sibling.invocation_id.to_string(),
            i64::try_from(decision.invocation.version).map_err(|_| {
                RepositoryError::CorruptRow("Model tool sibling Invocation version".to_owned())
            })?,
            Some(&invocation.run_id.to_string()),
            "capability.cancelled_by_model_tool_failure",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "failed_invocation_id": invocation.invocation_id,
                    "model_turn_id": model_turn_id,
                }),
            )?,
        )
        .await?;
    }
    settle_external_leaf_failure_in_transaction(
        transaction,
        &invocation.tenant_id,
        &invocation.run_id,
        &invocation.node_execution_id,
        capability_job_id,
        "model_tool_call",
        failure,
        release_active_permit,
        ExternalLeafFailureWait {
            plan_digest: wait.continuation.plan_digest,
            source_orchestration_job_id: wait.continuation.source_orchestration_job_id,
            root_scope_id: wait.continuation.root_scope_id,
            continuation_attempt_limit: wait.continuation.continuation_attempt_limit,
            retry_backoff_milliseconds: wait.continuation.retry_backoff_milliseconds,
            priority: wait.continuation.priority,
            deadline: wait.continuation.deadline,
        },
        mutations,
        database_now,
    )
    .await
}

pub(crate) async fn settle_model_leaf_failure_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    turn: &insight_platform_models::ModelTurnRecord,
    model_job_id: &ResourceId,
    failure: &Failure,
    mutations: &insight_platform_contracts::ExternalLeafFailureMutationIds,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id = $2 AND run_id = $3
          AND record_kind = 'node_execution' AND state = 'waiting'
        "#,
    )
    .bind(turn.tenant_id.to_string())
    .bind(turn.node_execution_id.to_string())
    .bind(turn.run_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Model failure leaf wait"))?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let wait: StoredModelTurnWaitPayload =
        decode_typed_payload(&payload, "Model failure leaf wait")?;
    if wait.model_turn_id != turn.model_turn_id || &wait.model_job_id != model_job_id {
        return Err(RepositoryError::Conflict("Model failure leaf owner"));
    }
    settle_external_leaf_failure_in_transaction(
        transaction,
        &turn.tenant_id,
        &turn.run_id,
        &turn.node_execution_id,
        model_job_id,
        "model_turn",
        failure,
        true,
        ExternalLeafFailureWait {
            plan_digest: wait.plan_digest,
            source_orchestration_job_id: wait.source_orchestration_job_id,
            root_scope_id: wait.root_scope_id,
            continuation_attempt_limit: wait.continuation_attempt_limit,
            retry_backoff_milliseconds: wait.retry_backoff_milliseconds,
            priority: wait.priority,
            deadline: wait.deadline,
        },
        mutations,
        database_now,
    )
    .await
}

pub(crate) async fn settle_capability_leaf_success_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &CapabilityInvocationRecord,
    capability_job_id: &ResourceId,
    output: &ExactInvocationValueRef,
    mutations: &insight_platform_contracts::ExternalLeafResumeMutationIds,
    scope_limits: ScopeEnvironmentLimits,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    if invocation
        .payload
        .result
        .as_ref()
        .map(|result| &result.output.value_id)
        != Some(&output.value_id)
    {
        return Err(RepositoryError::Conflict("Capability leaf terminal output"));
    }
    settle_external_leaf_success_in_transaction(
        transaction,
        ExternalLeafSuccessOwner {
            tenant_id: &invocation.tenant_id,
            run_id: &invocation.run_id,
            node_id: &invocation.node_execution_id,
            owner_id: &invocation.invocation_id,
            owner_job_id: capability_job_id,
            output_value_id: &output.value_id,
            kind: ExternalLeafSuccessKind::Capability,
        },
        output,
        mutations,
        scope_limits,
        database_now,
    )
    .await
}

pub(crate) async fn settle_model_tool_success_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &CapabilityInvocationRecord,
    capability_job_id: &ResourceId,
    output: &ExactInvocationValueRef,
    mutations: &insight_platform_contracts::ExternalLeafResumeMutationIds,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    mutations
        .validate()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let InvocationOrigin::ModelToolCall {
        model_turn_id,
        model_call_id_digest,
    } = &invocation.payload.admission.origin_key
    else {
        return Err(RepositoryError::Conflict("Model tool Invocation origin"));
    };
    if output.run_id != invocation.run_id
        || output.producing_node_id.as_ref() != Some(&invocation.node_execution_id)
        || invocation
            .payload
            .result
            .as_ref()
            .map(|result| &result.output.value_id)
            != Some(&output.value_id)
    {
        return Err(RepositoryError::Conflict("Model tool output owner"));
    }
    let run = load_run_for_update(transaction, &invocation.tenant_id, &invocation.run_id).await?;
    let convergence_goal = model_run_convergence_goal(&run, database_now)?;
    let node_row = sqlx::query(
        r#"
        SELECT state, version, payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id = $2 AND run_id = $3
          AND record_kind = 'node_execution'
        FOR UPDATE
        "#,
    )
    .bind(invocation.tenant_id.to_string())
    .bind(invocation.node_execution_id.to_string())
    .bind(invocation.run_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Model tool batch Node"))?;
    if node_row.try_get::<String, _>("state")? != NodeExecutionState::Waiting.as_str() {
        return Err(RepositoryError::Conflict("Model tool batch Node state"));
    }
    let payload = payload_from_row(
        &node_row,
        "payload_schema_version",
        "payload",
        "payload_digest",
    )?;
    let mut wait: StoredModelToolBatchWaitPayload =
        decode_typed_payload(&payload, "Model tool batch wait")?;
    if &wait.continuation.model_turn_id != model_turn_id {
        return Err(RepositoryError::Conflict("Model tool batch owner"));
    }
    let call = wait
        .calls
        .iter_mut()
        .find(|call| {
            call.invocation_id == invocation.invocation_id
                && &call.call_id_digest == model_call_id_digest
        })
        .ok_or(RepositoryError::Conflict("Model tool batch call"))?;
    if &call.capability_job_id != capability_job_id || call.result.is_some() {
        return Err(RepositoryError::Conflict("Model tool batch call state"));
    }
    call.result = Some(insight_platform_orchestrator::ModelToolResultReference {
        call_id: call.call_id.clone(),
        invocation_id: invocation.invocation_id.clone(),
        output_value_id: output.value_id.clone(),
        schema_digest: output.schema_digest.clone(),
        content_digest: output.content_digest.clone(),
        classification: output.classification,
    });
    let all_committed = wait.calls.iter().all(|call| call.result.is_some());
    let wait_payload = TypedPayload::with_limit(1, &wait, 262_144)?;
    let next_state = if all_committed && convergence_goal.is_none() {
        "ready"
    } else {
        "waiting"
    };
    let node_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = $4, version = version + 1,
            enqueue_round = CASE WHEN $4 = 'ready' THEN 0 ELSE enqueue_round END,
            payload_schema_version = $5, payload = $6, payload_digest = $7,
            updated_at = $8
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'node_execution' AND state = 'waiting'
          AND terminal_at IS NULL
        RETURNING version
        "#,
    )
    .bind(invocation.tenant_id.to_string())
    .bind(invocation.node_execution_id.to_string())
    .bind(node_row.try_get::<i64, _>("version")?)
    .bind(next_state)
    .bind(wait_payload.schema_version)
    .bind(&wait_payload.value)
    .bind(&wait_payload.digest)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Model tool batch result"))?;

    if let Some(goal) = convergence_goal {
        settle_suppressed_model_continuation(
            transaction,
            &run,
            &goal,
            true,
            &mutations.run_event_id,
            &mutations.run_outbox_id,
            &invocation.invocation_id,
            database_now,
        )
        .await?;
        append_scheduler_event(
            transaction, &run.tenant_id, &mutations.leaf_node_event_id,
            &mutations.leaf_node_outbox_id, "node_execution", &invocation.node_execution_id.to_string(),
            node_version, Some(&run.run_id), "node.model_tool_result_committed",
            &TypedPayload::with_limit(1, &serde_json::json!({
                "capability_job_id": capability_job_id, "invocation_id": invocation.invocation_id,
                "output_value_id": output.value_id, "continuation_suppressed": true,
            }), 65_536)?,
        ).await?;
        return Ok(());
    }
    if run.active_work_count < 1 || run.terminal_at.is_some() {
        return Err(RepositoryError::Conflict("Model tool active permit"));
    }
    let mut current = run.current.clone();
    if !all_committed && run.active_work_count == 1 {
        current.waiting_reason = Some("model_tools".to_owned());
    } else if all_committed && run.state == RunState::Waiting.as_str() {
        current.waiting_reason = None;
    }
    current.validate(&invocation.run_id)?;
    let current_payload = TypedPayload::from_versioned(1, &current, 1_048_576)?;
    let run = run_from_row(
        sqlx::query(
            r#"
            UPDATE insight_platform.runs
            SET state = CASE
                    WHEN $7 THEN CASE WHEN state = 'waiting' THEN 'running' ELSE state END
                    WHEN active_work_count = 1 THEN 'waiting'
                    ELSE state
                END,
                version = version + 1, active_work_count = active_work_count - 1,
                current_schema_version = $4, current_payload = $5,
                current_payload_digest = $6, updated_at = $8
            WHERE tenant_id = $1 AND run_id = $2 AND version = $3
              AND state IN ('running', 'waiting') AND active_work_count > 0
              AND terminal_at IS NULL
            RETURNING *
            "#,
        )
        .bind(&run.tenant_id)
        .bind(&run.run_id)
        .bind(run.version)
        .bind(current_payload.schema_version)
        .bind(&current_payload.value)
        .bind(&current_payload.digest)
        .bind(all_committed)
        .bind(database_now)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("Model tool Run settlement"))?,
    )?;

    let mut continuation_job = None;
    if all_committed {
        let continuation = insight_platform_orchestrator::ModelToolContinuation {
            model_turn_id: wait.continuation.model_turn_id.clone(),
            response_value_id: wait.continuation.response_value_id.clone(),
            response_digest: wait.continuation.response_digest.clone(),
            round_ordinal: wait.continuation.round_ordinal,
            tool_intent_count: wait.continuation.tool_intent_count,
            results: wait
                .calls
                .iter()
                .map(|call| call.result.clone().expect("all results checked"))
                .collect(),
        };
        continuation
            .validate()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        let job_payload = OrchestrationJobPayload {
            external_leaf_completion: None,
            bindings_digest: run.bindings.canonical_digest.clone(),
            node_execution_id: invocation.node_execution_id.clone(),
            root_scope_id: wait.continuation.root_scope_id.clone(),
            retry_backoff_milliseconds: wait.continuation.retry_backoff_milliseconds,
            wake_contract: None,
            convergence_failure: None,
            model_tool_continuation: Some(continuation),
        };
        let job_payload = job_payload.to_payload()?;
        continuation_job = Some(job_from_row(
            sqlx::query(
                r#"
                INSERT INTO insight_platform.jobs (
                tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, trace_id, run_id, node_id, state, attempt_limit, scheduled_at, deadline, priority, request_digest, payload_schema_version, payload, payload_digest, scheduler_partition_id, execution_requirement_version, execution_requirement, execution_requirement_digest
            ) VALUES (
                $1, $2, 'orchestration_node', 'orchestration', 'node_execution', $3, (SELECT trace_id FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $4), $4, $3, 'ready', $5, $6, $7, $8, $9, $10, $11, $12, (SELECT scheduler_partition_id FROM insight_platform.tenants WHERE tenant_id=$1), (SELECT execution_requirement_version FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement_digest FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4)
            ) RETURNING *
                "#,
            )
            .bind(invocation.tenant_id.to_string())
            .bind(mutations.continuation_job_id.to_string())
            .bind(invocation.node_execution_id.to_string())
            .bind(invocation.run_id.to_string())
            .bind(wait.continuation.continuation_attempt_limit)
            .bind(database_now)
            .bind(wait.continuation.deadline)
            .bind(scheduler_priority_to_database(wait.continuation.priority))
            .bind(&job_payload.digest)
            .bind(job_payload.schema_version)
            .bind(&job_payload.value)
            .bind(&job_payload.digest)
            .fetch_one(&mut **transaction)
            .await?,
        )?);
    }
    let evidence = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "all_committed": all_committed,
            "capability_job_id": capability_job_id,
            "invocation_id": invocation.invocation_id,
            "model_turn_id": model_turn_id,
            "output_value_id": output.value_id,
        }),
        65_536,
    )?;
    append_scheduler_event(
        transaction,
        &run.tenant_id,
        &mutations.run_event_id,
        &mutations.run_outbox_id,
        "run",
        &run.run_id,
        run.version,
        Some(&run.run_id),
        "run.model_tool_result_committed",
        &evidence,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &run.tenant_id,
        &mutations.leaf_node_event_id,
        &mutations.leaf_node_outbox_id,
        "node_execution",
        &invocation.node_execution_id.to_string(),
        node_version,
        Some(&run.run_id),
        "node.model_tool_result_committed",
        &evidence,
    )
    .await?;
    if let Some(job) = continuation_job {
        append_scheduler_event(
            transaction,
            &run.tenant_id,
            &mutations.continuation_job_event_id,
            &mutations.continuation_job_outbox_id,
            "job",
            &job.job_id,
            job.version,
            Some(&run.run_id),
            "job.ready",
            &evidence,
        )
        .await?;
    }
    Ok(())
}

async fn load_exact_agent_interface_spec(
    transaction: &mut Transaction<'_, Postgres>,
    run: &RunRecord,
) -> Result<AgentResourceSpec, RepositoryError> {
    let exact = &run.bindings.agent_interface;
    let row = sqlx::query(
        r#"
        SELECT payload_schema_version, payload, payload_digest
        FROM insight_platform.resource_versions
        WHERE tenant_id = $1 AND resource_version_id = $2
          AND resource_version_kind = 'agent_interface_revision' AND content_digest = $3
        "#,
    )
    .bind(&run.tenant_id)
    .bind(exact.revision_id.to_string())
    .bind(exact.semantic_digest.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("exact Agent interface revision"))?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let published = decode_published_version_payload(&payload)?;
    published
        .validate_for(RegistryResourceKind::Agent, &exact.revision_id)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let ResourceDocument::Agent(agent) = published.document else {
        return Err(RepositoryError::CorruptRow(
            "Agent interface revision contains a non-Agent document".to_owned(),
        ));
    };
    Ok(agent)
}

async fn require_exact_terminal_value(
    transaction: &mut Transaction<'_, Postgres>,
    run: &RunRecord,
    scope_id: &str,
    port: &ExactDataPortRef,
    schema: &ClosedJsonSchema,
    evidence: &MaterializedTerminalValue,
    limits: ScopeEnvironmentLimits,
) -> Result<String, RepositoryError> {
    if schema.canonical_digest != *port.schema_digest()
        || schema.canonical_digest != evidence.schema_digest
    {
        return Err(RepositoryError::Conflict(
            "terminal port Interface schema binding",
        ));
    }
    let tenant_id: ResourceId =
        run.tenant_id
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?;
    let run_id: ResourceId =
        run.run_id
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?;
    let scope_id: ResourceId =
        scope_id
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?;
    let environments =
        load_scope_environment_chain(transaction, &tenant_id, &run_id, &scope_id, limits).await?;
    let references = insight_platform_orchestrator::resolve_scope_inputs(
        std::slice::from_ref(port),
        &environments,
        limits,
    )?;
    let mut resolved = load_resolved_expression_values(
        transaction,
        &tenant_id,
        &run_id,
        vec![port.clone()],
        references,
    )
    .await?;
    let value = resolved
        .pop()
        .ok_or_else(|| RepositoryError::CorruptRow("terminal RunValue missing".to_owned()))?;
    if value.run_value_id != evidence.value_id
        || value.classification != evidence.classification
        || value.schema_digest != evidence.schema_digest
        || value.content_digest != evidence.content_digest
    {
        return Err(RepositoryError::Conflict("terminal RunValue evidence"));
    }
    match &value.value {
        ValueRef::Inline { value } if value == &evidence.body => {}
        ValueRef::Artifact { artifact }
            if artifact.media_type() == "application/json"
                || artifact.media_type().ends_with("+json") => {}
        _ => {
            return Err(RepositoryError::Conflict(
                "terminal RunValue materialization",
            ))
        }
    }
    let materialized_digest: Sha256Digest = canonical_digest(&evidence.body)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
        .parse()
        .map_err(|_| RepositoryError::InvalidInput("terminal body digest".to_owned()))?;
    if materialized_digest != evidence.content_digest {
        return Err(RepositoryError::Conflict(
            "terminal RunValue content digest",
        ));
    }
    schema
        .validate_instance(&evidence.body)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    Ok(value.run_value_id.to_string())
}

async fn load_controller_source_node(
    transaction: &mut Transaction<'_, Postgres>,
    job: &JobRecord,
    parents: &LockedOrchestrationJobParents,
    plan: &RuntimePlan,
) -> Result<ControllerSourceNode, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT plan_node_key, node_kind, scope_id, version
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id = $2
          AND run_id = $3 AND record_kind = 'node_execution' AND state = 'running'
        "#,
    )
    .bind(&job.tenant_id)
    .bind(&parents.node_id)
    .bind(&parents.run.run_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("running controller Node"))?;
    let plan_node_key = PlanNodeKey::new(row.try_get("plan_node_key")?)?;
    let node_kind = row
        .try_get::<String, _>("node_kind")?
        .parse::<PlanNodeKind>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let runtime_node = plan.node(&plan_node_key)?;
    let job_payload: OrchestrationJobPayload = decode_orchestration_job_payload(&job.payload)?;
    job_payload
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    if runtime_node.kind() != node_kind
        || job_payload.bindings_digest != parents.run.bindings.canonical_digest
        || job_payload.node_execution_id.to_string() != parents.node_id
        || row.try_get::<String, _>("scope_id")? != parents.scope_id
    {
        return Err(RepositoryError::Conflict(
            "controller Node exact Plan binding",
        ));
    }
    Ok(ControllerSourceNode {
        plan_node_key,
        node_kind,
        scope_id: parents.scope_id.clone(),
        version: row.try_get("version")?,
    })
}

fn controller_failure(code: &'static str) -> Failure {
    let (platform_code, class) = match code {
        "budget_exhausted" => (PlatformFailureCode::BudgetExhausted, FailureClass::Quota),
        "child_failed" | "quorum_unreachable" | "map_item_failed" | "map_error_limit_exceeded" => (
            PlatformFailureCode::DependencyUnavailable,
            FailureClass::Dependency,
        ),
        _ => (
            PlatformFailureCode::PlanInvariantFailed,
            FailureClass::Platform,
        ),
    };
    Failure {
        code: FailureCode::Platform {
            code: platform_code,
        },
        class,
        retryability: Retryability::Never,
        safe_message: None,
        details_ref: None,
        source: FailureSource::Plan,
    }
}

fn derive_orchestration_failure(
    cause: &OrchestrationFailureCause,
    runtime_node: &insight_platform_plan::RuntimeNode,
) -> Result<(Failure, Option<String>), RepositoryError> {
    match cause {
        OrchestrationFailureCause::Committed { failure } => {
            if matches!(
                runtime_node,
                insight_platform_plan::RuntimeNode::Join { .. }
                    | insight_platform_plan::RuntimeNode::Map { .. }
                    | insight_platform_plan::RuntimeNode::Loop { .. }
                    | insight_platform_plan::RuntimeNode::ErrorBoundary { .. }
            ) {
                return Err(RepositoryError::InvalidInput(
                    "controller Node failure must be derived from its exact observation".to_owned(),
                ));
            }
            Ok((failure.clone(), None))
        }
        OrchestrationFailureCause::Admission { failure } => {
            if !matches!(runtime_node, RuntimeNode::ChildAgentCall { .. })
                || !matches!(
                    failure.code,
                    FailureCode::Platform {
                        code: PlatformFailureCode::BudgetExhausted
                    }
                )
                || failure.class != FailureClass::Quota
                || failure.retryability != Retryability::Never
                || failure.source != FailureSource::Plan
            {
                return Err(RepositoryError::InvalidInput(
                    "admission failure is inconsistent with the exact Plan node".to_owned(),
                ));
            }
            Ok((failure.clone(), None))
        }
        OrchestrationFailureCause::Controller { observation } => {
            let ControllerDecision::FailNode { code } =
                decide_controller(runtime_node, observation)?
            else {
                return Err(RepositoryError::InvalidInput(
                    "controller observation does not produce a failed Node".to_owned(),
                ));
            };
            Ok((controller_failure(code), Some(code.to_owned())))
        }
    }
}

async fn require_failure_references(
    transaction: &mut Transaction<'_, Postgres>,
    run: &RunRecord,
    failure: &Failure,
) -> Result<(), RepositoryError> {
    failure
        .validate(1_024)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    if let FailureCode::Declared {
        interface_revision_id,
        ..
    } = &failure.code
    {
        let bound = run
            .bindings
            .exact_version_refs()
            .into_iter()
            .any(|reference| &reference.revision_id == interface_revision_id);
        if !bound {
            return Err(RepositoryError::Conflict(
                "declared Failure interface binding",
            ));
        }
    }
    if let Some(details) = &failure.details_ref {
        let tenant_id = run.tenant_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?;
        require_ready_run_artifact(transaction, &tenant_id, details).await?;
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct ErrorBoundaryRoute {
    boundary_node_id: String,
    boundary_parent_node_id: Option<String>,
    target: PlanNodeKey,
}

fn failure_route_keys(failure: &Failure) -> Vec<&str> {
    let code = match &failure.code {
        FailureCode::Platform { code } => code.as_str(),
        FailureCode::Declared { code, .. } => code.as_str(),
    };
    vec![code, failure.class.as_str()]
}

async fn find_matching_error_boundary(
    transaction: &mut Transaction<'_, Postgres>,
    run: &RunRecord,
    source_node_id: &str,
    plan: &RuntimePlan,
    failure: &Failure,
    maximum_depth: usize,
) -> Result<Option<ErrorBoundaryRoute>, RepositoryError> {
    let mut current_id = source_node_id.to_owned();
    let route_keys = failure_route_keys(failure);
    for _ in 0..maximum_depth {
        let row = sqlx::query(
            r#"
            SELECT parent_node_id, plan_node_key, node_kind
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
              AND record_kind = 'node_execution'
            "#,
        )
        .bind(&run.tenant_id)
        .bind(&run.run_id)
        .bind(&current_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("failure ancestor chain"))?;
        let Some(parent_id) = row.try_get::<Option<String>, _>("parent_node_id")? else {
            return Ok(None);
        };
        let parent = sqlx::query(
            r#"
            SELECT parent_node_id, plan_node_key, node_kind
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
              AND record_kind = 'node_execution'
            "#,
        )
        .bind(&run.tenant_id)
        .bind(&run.run_id)
        .bind(&parent_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("failure ancestor Node"))?;
        let plan_node_key = PlanNodeKey::new(parent.try_get("plan_node_key")?)?;
        let runtime_node = plan.node(&plan_node_key)?;
        if runtime_node.kind().as_str() != parent.try_get::<String, _>("node_kind")? {
            return Err(RepositoryError::Conflict("failure ancestor Plan binding"));
        }
        if let insight_platform_plan::RuntimeNode::ErrorBoundary { handlers, .. } = runtime_node {
            if let Some(target) = route_keys
                .iter()
                .find_map(|key| handlers.get(*key))
                .cloned()
            {
                return Ok(Some(ErrorBoundaryRoute {
                    boundary_node_id: parent_id,
                    boundary_parent_node_id: parent.try_get("parent_node_id")?,
                    target,
                }));
            }
        }
        current_id = parent_id;
    }
    Err(RepositoryError::CorruptRow(
        "failure ancestor chain exceeds the Plan bound".to_owned(),
    ))
}

async fn require_exact_controller_observation(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    parents: &LockedOrchestrationJobParents,
    source_node: &ControllerSourceNode,
    runtime_node: &insight_platform_plan::RuntimeNode,
    observation: &ControllerObservation,
    completion_limits: CompletionValidation<'_>,
) -> Result<(), RepositoryError> {
    let payload = decode_orchestration_job_payload(&current_job.payload)?;
    if payload.external_leaf_completion.is_some()
        != matches!(observation, ControllerObservation::ExternalLeafCompleted)
    {
        return Err(RepositoryError::Conflict(
            "external leaf completion observation",
        ));
    }
    if payload.external_leaf_completion.is_some() {
        return require_committed_external_leaf_completion(
            transaction,
            current_job,
            &parents.run,
            source_node,
            runtime_node,
            completion_limits,
        )
        .await;
    }
    if let RuntimeNode::SignalWait {
        signal_key,
        payload: payload_port,
        ..
    } = runtime_node
    {
        let ControllerObservation::DurableWait {
            wait_kind: DurableWaitKind::Signal,
            outcome,
        } = observation
        else {
            return Ok(());
        };
        let row = sqlx::query(
            r#"
            SELECT payload_schema_version, payload, payload_digest
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
              AND record_kind = 'node_execution' AND state = 'running'
            "#,
        )
        .bind(&current_job.tenant_id)
        .bind(&parents.run.run_id)
        .bind(&parents.node_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("running Signal Node"))?;
        let payload =
            payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
        let wait: StoredSignalWaitPayload =
            decode_typed_payload(&payload, "running Signal wait Node")?;
        let resolution = wait
            .resolution
            .ok_or(RepositoryError::Conflict("unresolved Signal observation"))?;
        if wait.plan_node_key != source_node.plan_node_key
            || wait.signal_key != *signal_key
            || wait.payload_port != *payload_port
            || resolution.outcome != *outcome
            || wait.payload_port.is_some() != resolution.payload.is_some()
        {
            return Err(RepositoryError::Conflict(
                "Signal durable observation evidence",
            ));
        }
        return Ok(());
    }
    if matches!(runtime_node, RuntimeNode::TimerWait { .. }) {
        let ControllerObservation::DurableWait {
            wait_kind: DurableWaitKind::Timer,
            outcome,
        } = observation
        else {
            return Ok(());
        };
        let row = sqlx::query(
            r#"
            SELECT payload_schema_version, payload, payload_digest
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
              AND record_kind = 'node_execution' AND state = 'running'
            "#,
        )
        .bind(&current_job.tenant_id)
        .bind(&parents.run.run_id)
        .bind(&parents.node_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("running Timer Node"))?;
        let payload =
            payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
        let wait: StoredTimerWaitPayload =
            decode_typed_payload(&payload, "running Timer wait Node")?;
        if wait.plan_node_key != source_node.plan_node_key || wait.resolution != Some(*outcome) {
            return Err(RepositoryError::Conflict(
                "Timer durable observation evidence",
            ));
        }
        return Ok(());
    }
    if let RuntimeNode::HumanTask {
        definition,
        response,
        ..
    } = runtime_node
    {
        let ControllerObservation::DurableWait {
            wait_kind: DurableWaitKind::HumanTask,
            outcome,
        } = observation
        else {
            return Ok(());
        };
        let row = sqlx::query(
            r#"
            SELECT payload_schema_version, payload, payload_digest
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
              AND record_kind = 'node_execution' AND state = 'running'
            "#,
        )
        .bind(&current_job.tenant_id)
        .bind(&parents.run.run_id)
        .bind(&parents.node_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("running HumanTask Node"))?;
        let payload =
            payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
        let wait: StoredHumanTaskWaitPayload =
            decode_typed_payload(&payload, "running HumanTask wait Node")?;
        let resolution = wait.resolution.ok_or(RepositoryError::Conflict(
            "unresolved HumanTask observation",
        ))?;
        if wait.plan_node_key != source_node.plan_node_key
            || wait.response_port != *response
            || wait.task_id.kind()
                != runtime_human_task_definition(definition)
                    .task_kind()
                    .task_id_kind()
            || resolution.outcome != *outcome
            || matches!(outcome, DurableWaitOutcome::Succeeded) != resolution.response.is_some()
        {
            return Err(RepositoryError::Conflict(
                "HumanTask durable observation evidence",
            ));
        }
        return Ok(());
    }
    if let insight_platform_plan::RuntimeNode::Map {
        body,
        next,
        failure_policy,
        ..
    } = runtime_node
    {
        let row = sqlx::query(
            r#"
            SELECT payload_schema_version, payload, payload_digest
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
              AND record_kind = 'node_execution' AND state = 'running'
            "#,
        )
        .bind(&current_job.tenant_id)
        .bind(&parents.run.run_id)
        .bind(&parents.node_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("running Map Node"))?;
        let payload =
            payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
        match observation {
            ControllerObservation::Map { item_count } if payload.value.get("wait").is_some() => {
                let pending: StoredPendingControllerNodePayload =
                    decode_typed_payload(&payload, "running Map admission Node")?;
                let StoredControllerWait::MapAdmission {
                    body_plan_node_key,
                    failure_policy: stored_policy,
                    item_count: stored_count,
                    next_item_index,
                    next_plan_node_key,
                } = pending.wait
                else {
                    return Err(RepositoryError::Conflict("Map admission phase"));
                };
                if pending.plan_node_key != source_node.plan_node_key
                    || body_plan_node_key != *body
                    || next_plan_node_key != *next
                    || stored_policy != *failure_policy
                    || stored_count != *item_count
                    || next_item_index >= stored_count
                    || !pending.expected_scope_ids.is_empty()
                {
                    return Err(RepositoryError::Conflict(
                        "Map admission observation contract",
                    ));
                }
            }
            ControllerObservation::Map { .. } => {}
            ControllerObservation::MapSettlement { children } => {
                let pending: StoredPendingControllerNodePayload =
                    decode_typed_payload(&payload, "running Map settlement Node")?;
                let StoredControllerWait::MapSettlement {
                    failure_policy: stored_policy,
                    item_count,
                    admitted_item_count,
                    next_plan_node_key,
                } = pending.wait
                else {
                    return Err(RepositoryError::Conflict("Map settlement phase"));
                };
                let expected_item_count = usize::try_from(admitted_item_count).map_err(|_| {
                    RepositoryError::InvalidInput(
                        "Map item count exceeds platform representation".to_owned(),
                    )
                })?;
                if pending.plan_node_key != source_node.plan_node_key
                    || next_plan_node_key != *next
                    || stored_policy != *failure_policy
                    || admitted_item_count == 0
                    || admitted_item_count > item_count
                    || !pending.expected_scope_ids.is_empty()
                    || children.len() != expected_item_count
                {
                    return Err(RepositoryError::Conflict(
                        "Map settlement observation contract",
                    ));
                }
                let root_map_node_id = pending.controller_node_execution_id.to_string();
                let rows = sqlx::query(
                    r#"
                    SELECT state, payload_schema_version, payload, payload_digest
                    FROM insight_platform.run_nodes
                    WHERE tenant_id = $1 AND run_id = $2 AND record_kind = 'scope_instance'
                      AND parent_node_id = $3 AND node_kind = 'map_item'
                    "#,
                )
                .bind(&current_job.tenant_id)
                .bind(&parents.run.run_id)
                .bind(&root_map_node_id)
                .fetch_all(&mut **transaction)
                .await?;
                if rows.len() != children.len() {
                    return Err(RepositoryError::Conflict("Map observed Scope set"));
                }
                let mut outcomes = BTreeMap::new();
                for row in rows {
                    let scope_payload = payload_from_row(
                        &row,
                        "payload_schema_version",
                        "payload",
                        "payload_digest",
                    )?;
                    let stored: StoredControllerScopePayload =
                        decode_typed_payload(&scope_payload, "MapItem Scope observation")?;
                    let StoredControllerScopeDescriptor::MapItem {
                        failure_policy: item_policy,
                        item_count: stored_count,
                        item_index,
                        map_plan_node_key,
                        next_plan_node_key: item_next,
                    } = stored.descriptor
                    else {
                        return Err(RepositoryError::Conflict("MapItem Scope descriptor"));
                    };
                    if stored.controller_node_execution_id != pending.controller_node_execution_id
                        || item_policy != *failure_policy
                        || stored_count != item_count
                        || map_plan_node_key != source_node.plan_node_key
                        || item_next != *next
                        || item_index >= admitted_item_count
                    {
                        return Err(RepositoryError::Conflict(
                            "MapItem Scope observation contract",
                        ));
                    }
                    let outcome = match row
                        .try_get::<String, _>("state")?
                        .parse::<ScopeState>()
                        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?
                    {
                        ScopeState::Open => ChildOutcome::Active,
                        ScopeState::Succeeded => ChildOutcome::Succeeded,
                        ScopeState::Failed => ChildOutcome::Failed,
                        ScopeState::Cancelled => ChildOutcome::Cancelled,
                        ScopeState::Closing => {
                            return Err(RepositoryError::Conflict("Map observed closing Scope"));
                        }
                    };
                    if outcomes.insert(item_index, outcome).is_some() {
                        return Err(RepositoryError::Conflict("duplicate Map item index"));
                    }
                }
                let exact = (0..admitted_item_count)
                    .map(|index| {
                        outcomes.get(&index).copied().ok_or_else(|| {
                            RepositoryError::CorruptRow("Map lost an item outcome".to_owned())
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if children != &exact {
                    return Err(RepositoryError::Conflict("exact Map observation"));
                }
            }
            _ => {}
        }
        return Ok(());
    }
    if matches!(
        runtime_node,
        insight_platform_plan::RuntimeNode::Loop { .. }
    ) {
        let ControllerObservation::Loop { iteration, .. } = observation else {
            return Ok(());
        };
        let row = sqlx::query(
            r#"
            SELECT parent_node_id, payload_schema_version, payload, payload_digest
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
              AND record_kind = 'node_execution' AND state = 'running'
            "#,
        )
        .bind(&current_job.tenant_id)
        .bind(&parents.run.run_id)
        .bind(&parents.node_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("running Loop Node"))?;
        let payload =
            payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
        if payload.value.get("wait").is_some() {
            let pending: StoredPendingControllerNodePayload =
                decode_typed_payload(&payload, "running Loop continuation Node")?;
            let StoredControllerWait::Loop {
                iteration: stored_iteration,
            } = pending.wait
            else {
                return Err(RepositoryError::Conflict("Loop continuation wait kind"));
            };
            let controller_node_id = row
                .try_get::<Option<String>, _>("parent_node_id")?
                .ok_or_else(|| {
                    RepositoryError::CorruptRow(
                        "Loop continuation has no controller owner".to_owned(),
                    )
                })?;
            if pending.controller_node_execution_id.to_string() != controller_node_id
                || pending.plan_node_key != source_node.plan_node_key
                || stored_iteration != *iteration
                || pending.expected_scope_ids.len() != 1
            {
                return Err(RepositoryError::Conflict(
                    "Loop frozen observation contract",
                ));
            }
        } else if *iteration != 0 {
            return Err(RepositoryError::Conflict(
                "initial Loop iteration observation",
            ));
        }
        return Ok(());
    }
    let insight_platform_plan::RuntimeNode::Join {
        policy,
        quorum,
        remainder,
        ..
    } = runtime_node
    else {
        return Ok(());
    };
    let row = sqlx::query(
        r#"
        SELECT parent_node_id, payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
          AND record_kind = 'node_execution' AND state = 'running'
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.run.run_id)
    .bind(&parents.node_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("running Join Node"))?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let pending: StoredPendingControllerNodePayload =
        decode_typed_payload(&payload, "running Join Node")?;
    let StoredControllerWait::Join {
        policy: stored_policy,
        quorum: stored_quorum,
        remainder: stored_remainder,
    } = pending.wait
    else {
        return Err(RepositoryError::Conflict("Join continuation wait kind"));
    };
    let unique_scope_ids = pending
        .expected_scope_ids
        .iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    let controller_node_id = row
        .try_get::<Option<String>, _>("parent_node_id")?
        .ok_or_else(|| {
            RepositoryError::CorruptRow("Join Node has no controller owner".to_owned())
        })?;
    if pending.controller_node_execution_id.to_string() != controller_node_id
        || pending.plan_node_key != source_node.plan_node_key
        || &stored_policy != policy
        || &stored_quorum != quorum
        || &stored_remainder != remainder
        || pending.expected_scope_ids.is_empty()
        || unique_scope_ids.len() != pending.expected_scope_ids.len()
    {
        return Err(RepositoryError::Conflict(
            "Join frozen observation contract",
        ));
    }
    let expected_ids = pending
        .expected_scope_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT node_id, parent_node_id, node_kind, state
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2
          AND node_id = ANY($3::text[]) AND record_kind = 'scope_instance'
        ORDER BY node_id
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.run.run_id)
    .bind(&expected_ids)
    .fetch_all(&mut **transaction)
    .await?;
    if rows.len() != expected_ids.len() {
        return Err(RepositoryError::Conflict("Join observed Scope set"));
    }
    let mut outcomes = BTreeMap::new();
    for row in rows {
        let scope_id: String = row.try_get("node_id")?;
        if row
            .try_get::<Option<String>, _>("parent_node_id")?
            .as_deref()
            != Some(controller_node_id.as_str())
            || row.try_get::<String, _>("node_kind")? != "parallel_leg"
        {
            return Err(RepositoryError::Conflict("Join observed Scope ownership"));
        }
        let outcome = match row
            .try_get::<String, _>("state")?
            .parse::<ScopeState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?
        {
            ScopeState::Open => ChildOutcome::Active,
            ScopeState::Succeeded => ChildOutcome::Succeeded,
            ScopeState::Failed => ChildOutcome::Failed,
            ScopeState::Cancelled => ChildOutcome::Cancelled,
            ScopeState::Closing => {
                return Err(RepositoryError::Conflict("Join observed closing Scope"));
            }
        };
        outcomes.insert(scope_id, outcome);
    }
    let exact = ControllerObservation::Join {
        children: expected_ids
            .iter()
            .map(|scope_id| {
                outcomes.get(scope_id).copied().ok_or_else(|| {
                    RepositoryError::CorruptRow("Join lost an observed Scope".to_owned())
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
    };
    if observation != &exact {
        return Err(RepositoryError::Conflict("exact Join observation"));
    }
    Ok(())
}

async fn load_map_settlement_observation(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    run_id: &str,
    pending: &StoredPendingControllerNodePayload,
    admitted_item_count: u32,
) -> Result<ControllerObservation, RepositoryError> {
    let root_map_node_id = pending.controller_node_execution_id.to_string();
    let rows = sqlx::query(
        r#"
        SELECT state, payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND record_kind = 'scope_instance'
          AND parent_node_id = $3 AND node_kind = 'map_item'
        "#,
    )
    .bind(tenant_id)
    .bind(run_id)
    .bind(&root_map_node_id)
    .fetch_all(&mut **transaction)
    .await?;
    if rows.len() != usize::try_from(admitted_item_count).unwrap_or(usize::MAX) {
        return Err(RepositoryError::Conflict("Map observed Scope set"));
    }
    let mut outcomes = BTreeMap::new();
    for row in rows {
        let scope_payload =
            payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
        let stored: StoredControllerScopePayload =
            decode_typed_payload(&scope_payload, "MapItem Scope observation")?;
        let StoredControllerScopeDescriptor::MapItem { item_index, .. } = stored.descriptor else {
            return Err(RepositoryError::Conflict("MapItem Scope descriptor"));
        };
        if stored.controller_node_execution_id != pending.controller_node_execution_id
            || item_index >= admitted_item_count
        {
            return Err(RepositoryError::Conflict(
                "MapItem Scope observation contract",
            ));
        }
        let outcome = child_outcome_from_scope_state(row.try_get("state")?)?;
        if outcomes.insert(item_index, outcome).is_some() {
            return Err(RepositoryError::Conflict("duplicate Map item index"));
        }
    }
    Ok(ControllerObservation::MapSettlement {
        children: (0..admitted_item_count)
            .map(|index| {
                outcomes.get(&index).copied().ok_or_else(|| {
                    RepositoryError::CorruptRow("Map lost an item outcome".to_owned())
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
    })
}

async fn load_join_observation(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    run_id: &str,
    payload: &TypedPayload,
) -> Result<ControllerObservation, RepositoryError> {
    let pending: StoredPendingControllerNodePayload =
        decode_typed_payload(payload, "running Join Node")?;
    if !matches!(pending.wait, StoredControllerWait::Join { .. })
        || pending.expected_scope_ids.is_empty()
    {
        return Err(RepositoryError::Conflict(
            "Join frozen observation contract",
        ));
    }
    let expected_ids = pending
        .expected_scope_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    if expected_ids.iter().collect::<BTreeSet<_>>().len() != expected_ids.len() {
        return Err(RepositoryError::Conflict("Join frozen Scope set"));
    }
    let rows = sqlx::query(
        r#"
        SELECT node_id, state
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND record_kind = 'scope_instance'
          AND node_id = ANY($3::text[]) AND node_kind = 'parallel_leg'
        "#,
    )
    .bind(tenant_id)
    .bind(run_id)
    .bind(&expected_ids)
    .fetch_all(&mut **transaction)
    .await?;
    if rows.len() != expected_ids.len() {
        return Err(RepositoryError::Conflict("Join observed Scope set"));
    }
    let outcomes = rows
        .into_iter()
        .map(|row| {
            Ok((
                row.try_get::<String, _>("node_id")?,
                child_outcome_from_scope_state(row.try_get("state")?)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, RepositoryError>>()?;
    Ok(ControllerObservation::Join {
        children: expected_ids
            .iter()
            .map(|scope_id| {
                outcomes.get(scope_id).copied().ok_or_else(|| {
                    RepositoryError::CorruptRow("Join lost an observed Scope".to_owned())
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn child_outcome_from_scope_state(state: String) -> Result<ChildOutcome, RepositoryError> {
    match state
        .parse::<ScopeState>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?
    {
        ScopeState::Open => Ok(ChildOutcome::Active),
        ScopeState::Succeeded => Ok(ChildOutcome::Succeeded),
        ScopeState::Failed => Ok(ChildOutcome::Failed),
        ScopeState::Cancelled => Ok(ChildOutcome::Cancelled),
        ScopeState::Closing => Err(RepositoryError::Conflict("observed closing Scope")),
    }
}

#[allow(clippy::too_many_arguments)]
async fn require_exact_derived_expression_evidence(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    parents: &LockedOrchestrationJobParents,
    source_node: &ControllerSourceNode,
    runtime_node: &insight_platform_plan::RuntimeNode,
    observation: &ControllerObservation,
    derived: &DerivedExpressionCommitEvidence,
    expression_limits: insight_platform_plan::ExpressionLimits,
    scope_limits: ScopeEnvironmentLimits,
) -> Result<(), RepositoryError> {
    if !matches!(
        runtime_node,
        insight_platform_plan::RuntimeNode::Compute { .. }
            | insight_platform_plan::RuntimeNode::Branch { .. }
            | insight_platform_plan::RuntimeNode::Map { .. }
            | insight_platform_plan::RuntimeNode::Loop { .. }
    ) || &derived.evaluation.observation != observation
        || derived.evaluation.evidence.node_execution_version != source_node.version
        || derived.evaluation.evidence.node_execution_id.to_string() != parents.node_id
    {
        return Err(RepositoryError::Conflict(
            "derived expression controller evidence",
        ));
    }
    let required_ports = required_expression_inputs(runtime_node)?;
    let tenant_id: ResourceId = current_job.tenant_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let run_id: ResourceId = parents.run.run_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let scope_id: ResourceId = parents.scope_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let environments =
        load_scope_environment_chain(transaction, &tenant_id, &run_id, &scope_id, scope_limits)
            .await?;
    let value_refs = insight_platform_orchestrator::resolve_scope_inputs(
        &required_ports,
        &environments,
        scope_limits,
    )?;
    let resolved = load_resolved_expression_values(
        transaction,
        &tenant_id,
        &run_id,
        required_ports,
        value_refs,
    )
    .await?;
    if resolved.len() != derived.materialized_inputs.len() {
        return Err(RepositoryError::Conflict(
            "derived expression input closure",
        ));
    }
    for (expected, materialized) in resolved.iter().zip(&derived.materialized_inputs) {
        if expected.run_value_id != materialized.run_value_id
            || expected.port != materialized.port
            || expected.classification != materialized.classification
            || expected.schema_digest != materialized.value.schema_digest
            || expected.content_digest != materialized.value.canonical_digest
            || materialized.value.validate().is_err()
            || matches!(
                &expected.value,
                ValueRef::Inline { value } if value != &materialized.value.value
            )
        {
            return Err(RepositoryError::Conflict(
                "derived expression input evidence",
            ));
        }
    }
    let loop_iteration = match observation {
        ControllerObservation::Loop { iteration, .. } => *iteration,
        _ => 0,
    };
    let exact = derive_expression_controller(
        runtime_node,
        derived.materialized_inputs.clone(),
        derived.evaluation.evidence.node_execution_id.clone(),
        source_node.version,
        loop_iteration,
        expression_limits,
    )?;
    if exact != derived.evaluation {
        return Err(RepositoryError::Conflict(
            "derived expression evaluation evidence",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn commit_derived_expression_values(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    parents: &LockedOrchestrationJobParents,
    source_node: &ControllerSourceNode,
    runtime_node: &insight_platform_plan::RuntimeNode,
    shape: &ControllerStepShape,
    mutations: &ControllerStepMutationIds,
    derived: &DerivedExpressionCommitEvidence,
    inline_limits: JsonLimits,
    scope_limits: ScopeEnvironmentLimits,
) -> Result<CommittedDerivedExpressionValues, RepositoryError> {
    if let insight_platform_plan::RuntimeNode::Map { item_port, .. } = runtime_node {
        return commit_derived_map_item_values(
            transaction,
            current_job,
            parents,
            source_node,
            item_port,
            shape,
            mutations,
            derived,
            inline_limits,
            scope_limits,
        )
        .await;
    }
    if !matches!(
        runtime_node,
        insight_platform_plan::RuntimeNode::Compute { .. }
    ) {
        if !derived.evaluation.outputs.is_empty() || !derived.output_value_ids.is_empty() {
            return Err(RepositoryError::Conflict("non-Compute expression outputs"));
        }
        return Ok(CommittedDerivedExpressionValues {
            source_scope_version: parents.scope_version,
            new_scope_bindings: Vec::new(),
        });
    }
    if derived.evaluation.outputs.len() != derived.output_value_ids.len() {
        return Err(RepositoryError::InvalidInput(
            "Compute output mutation arity".to_owned(),
        ));
    }
    let current_value_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.run_values WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.run.run_id)
    .fetch_one(&mut **transaction)
    .await?;
    let next_value_count = usize::try_from(current_value_count)
        .ok()
        .and_then(|count| count.checked_add(derived.output_value_ids.len()))
        .ok_or_else(|| RepositoryError::Conflict("RunValue reference bound"))?;
    if next_value_count > scope_limits.maximum_bindings_per_scope {
        return Err(RepositoryError::Conflict("RunValue reference bound"));
    }
    let scope_row = sqlx::query(
        r#"
        SELECT parent_node_id, node_kind, state, version,
               payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
          AND record_kind = 'scope_instance'
        FOR UPDATE
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.run.run_id)
    .bind(&parents.scope_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Compute output Scope"))?;
    if scope_row.try_get::<String, _>("state")? != ScopeState::Open.as_str()
        || scope_row.try_get::<i64, _>("version")? != parents.scope_version
    {
        return Err(RepositoryError::Conflict("Compute output Scope fence"));
    }
    let scope_payload = payload_from_row(
        &scope_row,
        "payload_schema_version",
        "payload",
        "payload_digest",
    )?;
    let scope_kind: String = scope_row.try_get("node_kind")?;
    let mut root = None;
    let mut nested = None;
    let environment = match scope_kind.as_str() {
        "root" => {
            let payload: StoredRootScopePayload =
                decode_typed_payload(&scope_payload, "Compute root Scope")?;
            root = Some(payload);
            &mut root
                .as_mut()
                .expect("root Scope payload assigned")
                .environment
        }
        "parallel_leg" | "loop_iteration" | "map_item" => {
            let payload: StoredControllerScopePayload =
                decode_typed_payload(&scope_payload, "Compute controller Scope")?;
            if scope_row.try_get::<Option<String>, _>("parent_node_id")?
                != Some(payload.controller_node_execution_id.to_string())
            {
                return Err(RepositoryError::CorruptRow(
                    "Compute Scope controller owner differs".to_owned(),
                ));
            }
            nested = Some(payload);
            &mut nested
                .as_mut()
                .expect("controller Scope payload assigned")
                .environment
        }
        _ => {
            return Err(RepositoryError::CorruptRow(
                "Compute Scope kind is unregistered".to_owned(),
            ))
        }
    };
    environment
        .validate(scope_limits)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    for (output, value_id) in derived
        .evaluation
        .outputs
        .iter()
        .zip(&derived.output_value_ids)
    {
        if output.port.producer_node_id() != Some(&source_node.plan_node_key)
            || (ValueRef::Inline {
                value: output.value.value.clone(),
            })
            .validate(inline_limits)
            .is_err()
        {
            return Err(RepositoryError::InvalidInput(
                "Compute output violates its exact Plan or Inline limit".to_owned(),
            ));
        }
        sqlx::query(
            r#"
            INSERT INTO insight_platform.run_values (
                tenant_id, value_id, run_id, node_id, value_kind, classification,
                schema_digest, content_digest, inline_value, artifact_id
            ) VALUES ($1, $2, $3, $4, 'expression_output', $5, $6, $7, $8, NULL)
            "#,
        )
        .bind(&current_job.tenant_id)
        .bind(value_id.to_string())
        .bind(&parents.run.run_id)
        .bind(&parents.node_id)
        .bind(derived.evaluation.effective_classification.as_str())
        .bind(output.value.schema_digest.to_string())
        .bind(output.value.canonical_digest.to_string())
        .bind(&output.value.value)
        .execute(&mut **transaction)
        .await?;
        environment
            .bind_new(
                output.port.clone(),
                ExactRunValueRef {
                    value_id: value_id.clone(),
                    schema_digest: output.value.schema_digest.clone(),
                    content_digest: output.value.canonical_digest.clone(),
                },
                scope_limits,
            )
            .map_err(|_| RepositoryError::Conflict("Compute output Scope binding"))?;
    }
    let next_payload = match (root, nested) {
        (Some(root), None) => TypedPayload::with_limit(1, &root, 262_144)?,
        (None, Some(nested)) => TypedPayload::with_limit(1, &nested, 262_144)?,
        _ => {
            return Err(RepositoryError::CorruptRow(
                "Compute Scope payload variant is ambiguous".to_owned(),
            ))
        }
    };
    let next_scope_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET version = version + 1, payload_schema_version = $4,
            payload = $5, payload_digest = $6, updated_at = clock_timestamp()
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'scope_instance' AND state = 'open' AND terminal_at IS NULL
        RETURNING version
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.scope_id)
    .bind(parents.scope_version)
    .bind(next_payload.schema_version)
    .bind(&next_payload.value)
    .bind(&next_payload.digest)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Compute output Scope CAS"))?;
    Ok(CommittedDerivedExpressionValues {
        source_scope_version: next_scope_version,
        new_scope_bindings: Vec::new(),
    })
}

#[allow(clippy::too_many_arguments)]
async fn commit_derived_map_item_values(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    parents: &LockedOrchestrationJobParents,
    source_node: &ControllerSourceNode,
    item_port: &ExactDataPortRef,
    shape: &ControllerStepShape,
    mutations: &ControllerStepMutationIds,
    derived: &DerivedExpressionCommitEvidence,
    inline_limits: JsonLimits,
    scope_limits: ScopeEnvironmentLimits,
) -> Result<CommittedDerivedExpressionValues, RepositoryError> {
    let ControllerStepShape::MapBatch {
        batch_start,
        batch_size,
        item_count,
        ..
    } = shape
    else {
        if !derived.output_value_ids.is_empty() {
            return Err(RepositoryError::InvalidInput(
                "zero-item Map has item value mutation slots".to_owned(),
            ));
        }
        return Ok(CommittedDerivedExpressionValues {
            source_scope_version: parents.scope_version,
            new_scope_bindings: Vec::new(),
        });
    };
    if item_port.producer_node_id() != Some(&source_node.plan_node_key)
        || !derived.evaluation.outputs.is_empty()
        || derived.evaluation.evaluated_results.len() != 1
        || derived.output_value_ids.len() != usize::try_from(*batch_size).unwrap_or(usize::MAX)
        || mutations.activations.len() != derived.output_value_ids.len()
    {
        return Err(RepositoryError::InvalidInput(
            "Map item value mutation shape".to_owned(),
        ));
    }
    let items = derived.evaluation.evaluated_results[0]
        .value
        .as_array()
        .ok_or_else(|| RepositoryError::Conflict("derived Map array"))?;
    if items.len() != usize::try_from(*item_count).unwrap_or(usize::MAX) {
        return Err(RepositoryError::Conflict("derived Map item count"));
    }
    let current_value_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.run_values WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.run.run_id)
    .fetch_one(&mut **transaction)
    .await?;
    let next_value_count = usize::try_from(current_value_count)
        .ok()
        .and_then(|count| count.checked_add(derived.output_value_ids.len()))
        .ok_or(RepositoryError::Conflict("RunValue reference bound"))?;
    if next_value_count > scope_limits.maximum_bindings_per_scope {
        return Err(RepositoryError::Conflict("RunValue reference bound"));
    }
    let mut new_scope_bindings = Vec::with_capacity(derived.output_value_ids.len());
    for (offset, (value_id, activation)) in derived
        .output_value_ids
        .iter()
        .zip(&mutations.activations)
        .enumerate()
    {
        let item_index = usize::try_from(*batch_start)
            .ok()
            .and_then(|start| start.checked_add(offset))
            .ok_or_else(|| RepositoryError::InvalidInput("Map item index overflow".to_owned()))?;
        let item = items
            .get(item_index)
            .ok_or(RepositoryError::Conflict("derived Map item"))?
            .clone();
        let value = insight_platform_contracts::ClosedJsonValue::build(
            item_port.schema_digest().clone(),
            item,
        )
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        if (ValueRef::Inline {
            value: value.value.clone(),
        })
        .validate(inline_limits)
        .is_err()
        {
            return Err(RepositoryError::InvalidInput(
                "Map item violates its Inline limit".to_owned(),
            ));
        }
        let scope_instance_id = activation
            .scope
            .as_ref()
            .ok_or_else(|| RepositoryError::InvalidInput("Map item Scope slot".to_owned()))?
            .scope_instance_id
            .clone();
        sqlx::query(
            r#"
            INSERT INTO insight_platform.run_values (
                tenant_id, value_id, run_id, node_id, value_kind, classification,
                schema_digest, content_digest, inline_value, artifact_id
            ) VALUES ($1, $2, $3, $4, 'map_item', $5, $6, $7, $8, NULL)
            "#,
        )
        .bind(&current_job.tenant_id)
        .bind(value_id.to_string())
        .bind(&parents.run.run_id)
        .bind(&parents.node_id)
        .bind(derived.evaluation.effective_classification.as_str())
        .bind(value.schema_digest.to_string())
        .bind(value.canonical_digest.to_string())
        .bind(&value.value)
        .execute(&mut **transaction)
        .await?;
        new_scope_bindings.push(DerivedNewScopeBinding {
            scope_instance_id,
            port: item_port.clone(),
            value: ExactRunValueRef {
                value_id: value_id.clone(),
                schema_digest: value.schema_digest,
                content_digest: value.canonical_digest,
            },
        });
    }
    Ok(CommittedDerivedExpressionValues {
        source_scope_version: parents.scope_version,
        new_scope_bindings,
    })
}

async fn load_parallel_leg_exit(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    parents: &LockedOrchestrationJobParents,
    source_node: &ControllerSourceNode,
    target: Option<&PlanNodeKey>,
) -> Result<Option<ControllerStepShape>, RepositoryError> {
    let scope_row = sqlx::query(
        r#"
        SELECT node_kind, parent_node_id, payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id = $2 AND run_id = $3
          AND record_kind = 'scope_instance' AND state = 'open'
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&source_node.scope_id)
    .bind(&parents.run.run_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("controller source Scope"))?;
    if scope_row.try_get::<String, _>("node_kind")? != "parallel_leg" {
        return Ok(None);
    }
    let payload = payload_from_row(
        &scope_row,
        "payload_schema_version",
        "payload",
        "payload_digest",
    )?;
    let scope: StoredControllerScopePayload =
        decode_typed_payload(&payload, "controller ParallelLeg Scope")?;
    let StoredControllerScopeDescriptor::ParallelLeg {
        join_plan_node_key, ..
    } = &scope.descriptor
    else {
        return Err(RepositoryError::Conflict("ParallelLeg Scope descriptor"));
    };
    if target.is_some_and(|target| target != join_plan_node_key) {
        return Ok(None);
    }
    let controller_node_id = scope.controller_node_execution_id.to_string();
    if scope_row
        .try_get::<Option<String>, _>("parent_node_id")?
        .as_deref()
        != Some(controller_node_id.as_str())
    {
        return Err(RepositoryError::Conflict(
            "ParallelLeg Scope controller owner",
        ));
    }
    let row = sqlx::query(
        r#"
        SELECT node_id, payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND record_kind = 'node_execution'
          AND parent_node_id = $3 AND plan_node_key = $4
          AND state IN ('pending', 'ready', 'running', 'succeeded')
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.run.run_id)
    .bind(&controller_node_id)
    .bind(join_plan_node_key.as_str())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("pending Fork Join Node"))?;
    let pending_payload =
        payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let pending: StoredPendingControllerNodePayload =
        decode_typed_payload(&pending_payload, "pending Fork Join Node")?;
    let StoredControllerWait::Join {
        policy,
        quorum,
        remainder,
    } = pending.wait
    else {
        return Err(RepositoryError::Conflict("Fork Join wait kind"));
    };
    let source_scope_id: ResourceId = source_node.scope_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    if pending.controller_node_execution_id != scope.controller_node_execution_id
        || pending.plan_node_key != *join_plan_node_key
        || !pending.expected_scope_ids.contains(&source_scope_id)
    {
        return Err(RepositoryError::Conflict("Fork Join structural ownership"));
    }
    Ok(Some(ControllerStepShape::ParallelLegExit {
        join_node_execution_id: row.try_get::<String, _>("node_id")?.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?,
        join_plan_node_key: join_plan_node_key.clone(),
        expected_scope_ids: pending.expected_scope_ids,
        policy,
        quorum,
        remainder,
    }))
}

async fn load_loop_iteration_exit(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    parents: &LockedOrchestrationJobParents,
    source_node: &ControllerSourceNode,
    target: &PlanNodeKey,
) -> Result<Option<ControllerStepShape>, RepositoryError> {
    let scope_row = sqlx::query(
        r#"
        SELECT node_kind, parent_node_id, payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id = $2 AND run_id = $3
          AND record_kind = 'scope_instance' AND state = 'open'
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&source_node.scope_id)
    .bind(&parents.run.run_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("controller source Scope"))?;
    if scope_row.try_get::<String, _>("node_kind")? != "loop_iteration" {
        return Ok(None);
    }
    let payload = payload_from_row(
        &scope_row,
        "payload_schema_version",
        "payload",
        "payload_digest",
    )?;
    let scope: StoredControllerScopePayload =
        decode_typed_payload(&payload, "controller LoopIteration Scope")?;
    let (iteration, loop_plan_node_key) = match &scope.descriptor {
        StoredControllerScopeDescriptor::LoopIteration {
            iteration,
            loop_plan_node_key,
        } => (*iteration, loop_plan_node_key),
        StoredControllerScopeDescriptor::ParallelLeg { .. }
        | StoredControllerScopeDescriptor::MapItem { .. } => {
            return Err(RepositoryError::Conflict("LoopIteration Scope descriptor"));
        }
    };
    if target != loop_plan_node_key {
        return Ok(None);
    }
    let controller_node_id = scope.controller_node_execution_id.to_string();
    if scope_row
        .try_get::<Option<String>, _>("parent_node_id")?
        .as_deref()
        != Some(controller_node_id.as_str())
    {
        return Err(RepositoryError::Conflict(
            "LoopIteration Scope controller owner",
        ));
    }
    let row = sqlx::query(
        r#"
        SELECT node_id, payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND record_kind = 'node_execution'
          AND parent_node_id = $3 AND plan_node_key = $4 AND state = 'pending'
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.run.run_id)
    .bind(&controller_node_id)
    .bind(loop_plan_node_key.as_str())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("pending Loop continuation Node"))?;
    let pending_payload =
        payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let pending: StoredPendingControllerNodePayload =
        decode_typed_payload(&pending_payload, "pending Loop continuation Node")?;
    let expected_next_iteration = iteration.checked_add(1).ok_or_else(|| {
        RepositoryError::CorruptRow("Loop iteration counter overflowed".to_owned())
    })?;
    if pending.controller_node_execution_id != scope.controller_node_execution_id
        || pending.plan_node_key != *loop_plan_node_key
        || pending.expected_scope_ids
            != vec![source_node.scope_id.parse().map_err(
                |failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                },
            )?]
        || pending.wait
            != (StoredControllerWait::Loop {
                iteration: expected_next_iteration,
            })
    {
        return Err(RepositoryError::Conflict(
            "Loop continuation structural ownership",
        ));
    }
    Ok(Some(ControllerStepShape::LoopIterationExit {
        loop_node_execution_id: row.try_get::<String, _>("node_id")?.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?,
        root_loop_node_execution_id: scope.controller_node_execution_id,
        loop_plan_node_key: loop_plan_node_key.clone(),
        expected_scope_id: source_node.scope_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?,
        iteration,
    }))
}

async fn load_open_loop_iteration_context(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    parents: &LockedOrchestrationJobParents,
    source_node: &ControllerSourceNode,
    expected_iteration: Option<u32>,
) -> Result<Option<OpenLoopIterationContext>, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT parent_node_id, node_kind, state, version,
               payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
          AND record_kind = 'scope_instance'
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.run.run_id)
    .bind(&source_node.scope_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Loop source Scope"))?;
    if row.try_get::<String, _>("node_kind")? != "loop_iteration" {
        return Ok(None);
    }
    if row.try_get::<String, _>("state")? != ScopeState::Open.as_str()
        || row.try_get::<i64, _>("version")? != parents.scope_version
    {
        return Err(RepositoryError::Conflict("Loop iteration Scope fence"));
    }
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let stored: StoredControllerScopePayload =
        decode_typed_payload(&payload, "open LoopIteration Scope")?;
    let StoredControllerScopeDescriptor::LoopIteration {
        iteration,
        loop_plan_node_key,
    } = stored.descriptor
    else {
        return Err(RepositoryError::Conflict("Loop iteration Scope descriptor"));
    };
    if loop_plan_node_key != source_node.plan_node_key
        || expected_iteration.is_some_and(|expected| expected != iteration)
        || row.try_get::<Option<String>, _>("parent_node_id")?
            != Some(stored.controller_node_execution_id.to_string())
    {
        return Err(RepositoryError::Conflict("Loop iteration Scope ownership"));
    }
    let lexical_parent_scope_id: ResourceId = sqlx::query_scalar::<_, String>(
        r#"
        SELECT scope_id
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
          AND record_kind = 'node_execution'
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.run.run_id)
    .bind(stored.controller_node_execution_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("root Loop controller"))?
    .parse()
    .map_err(|failure: insight_platform_contracts::ResourceIdError| {
        RepositoryError::CorruptRow(failure.to_string())
    })?;
    let scope_id: ResourceId = source_node.scope_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    if lexical_parent_scope_id == scope_id {
        return Err(RepositoryError::Conflict("Loop Scope lexical self-cycle"));
    }
    Ok(Some(OpenLoopIterationContext {
        root_loop_node_execution_id: stored.controller_node_execution_id,
        scope_id,
        lexical_parent_scope_id,
        iteration,
        loop_plan_node_key,
    }))
}

#[allow(clippy::too_many_arguments)]
async fn load_map_batch_shape(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    parents: &LockedOrchestrationJobParents,
    source_node: &ControllerSourceNode,
    body: &PlanNodeKey,
    next: &PlanNodeKey,
    failure_policy: MapFailurePolicy,
    item_count: u32,
    maximum_batch: usize,
) -> Result<ControllerStepShape, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
          AND record_kind = 'node_execution' AND state = 'running'
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.run.run_id)
    .bind(&parents.node_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("running Map admission Node"))?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let (root_map_node_execution_id, batch_start) = if payload.value.get("wait").is_some() {
        let pending: StoredPendingControllerNodePayload =
            decode_typed_payload(&payload, "running Map admission continuation")?;
        let StoredControllerWait::MapAdmission {
            body_plan_node_key,
            failure_policy: stored_policy,
            item_count: stored_count,
            next_item_index,
            next_plan_node_key,
        } = pending.wait
        else {
            return Err(RepositoryError::Conflict("Map admission wait kind"));
        };
        if pending.plan_node_key != source_node.plan_node_key
            || body_plan_node_key != *body
            || next_plan_node_key != *next
            || stored_policy != failure_policy
            || stored_count != item_count
            || !pending.expected_scope_ids.is_empty()
        {
            return Err(RepositoryError::Conflict("Map admission frozen contract"));
        }
        (pending.controller_node_execution_id, next_item_index)
    } else {
        (
            parents.node_id.parse().map_err(
                |failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                },
            )?,
            0,
        )
    };
    if maximum_batch == 0 || batch_start >= item_count {
        return Err(RepositoryError::Conflict("Map admission batch cursor"));
    }
    let maximum_batch = u32::try_from(maximum_batch)
        .map_err(|_| RepositoryError::InvalidInput("Map batch limit exceeds u32".to_owned()))?;
    let remaining = item_count
        .checked_sub(batch_start)
        .ok_or(RepositoryError::Conflict("Map admission remaining count"))?;
    let batch_size = remaining.min(maximum_batch);
    let batch_end = batch_start
        .checked_add(batch_size)
        .ok_or_else(|| RepositoryError::InvalidInput("Map batch cursor overflow".to_owned()))?;
    Ok(ControllerStepShape::MapBatch {
        body: body.clone(),
        next: next.clone(),
        failure_policy,
        item_count,
        batch_start,
        batch_size,
        root_map_node_execution_id,
        has_more: batch_end < item_count,
    })
}

async fn load_map_item_exit(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    parents: &LockedOrchestrationJobParents,
    source_node: &ControllerSourceNode,
    target: Option<&PlanNodeKey>,
) -> Result<Option<ControllerStepShape>, RepositoryError> {
    let scope_row = sqlx::query(
        r#"
        SELECT node_kind, parent_node_id, payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id = $2 AND run_id = $3
          AND record_kind = 'scope_instance' AND state = 'open'
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&source_node.scope_id)
    .bind(&parents.run.run_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("controller source Scope"))?;
    if scope_row.try_get::<String, _>("node_kind")? != "map_item" {
        return Ok(None);
    }
    let payload = payload_from_row(
        &scope_row,
        "payload_schema_version",
        "payload",
        "payload_digest",
    )?;
    let scope: StoredControllerScopePayload =
        decode_typed_payload(&payload, "controller MapItem Scope")?;
    let (failure_policy, item_count, item_index, map_plan_node_key, next_plan_node_key) =
        match &scope.descriptor {
            StoredControllerScopeDescriptor::MapItem {
                failure_policy,
                item_count,
                item_index,
                map_plan_node_key,
                next_plan_node_key,
            } => (
                *failure_policy,
                *item_count,
                *item_index,
                map_plan_node_key,
                next_plan_node_key,
            ),
            _ => return Err(RepositoryError::Conflict("MapItem Scope descriptor")),
        };
    if target.is_some_and(|target| target != map_plan_node_key) {
        return Ok(None);
    }
    let root_map_node_execution_id = scope.controller_node_execution_id;
    let root_map_node_id = root_map_node_execution_id.to_string();
    if scope_row
        .try_get::<Option<String>, _>("parent_node_id")?
        .as_deref()
        != Some(root_map_node_id.as_str())
    {
        return Err(RepositoryError::Conflict("MapItem Scope controller owner"));
    }
    let candidates = sqlx::query(
        r#"
        SELECT node_id, payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND record_kind = 'node_execution'
          AND parent_node_id = $3 AND plan_node_key = $4 AND state = 'pending'
        ORDER BY node_id
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.run.run_id)
    .bind(&root_map_node_id)
    .bind(map_plan_node_key.as_str())
    .fetch_all(&mut **transaction)
    .await?;
    let mut wait_node = None;
    for row in candidates {
        let candidate_payload =
            payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
        let pending: StoredPendingControllerNodePayload =
            decode_typed_payload(&candidate_payload, "pending Map controller Node")?;
        let frozen_contract_matches = match &pending.wait {
            StoredControllerWait::MapAdmission {
                failure_policy: stored_policy,
                item_count: stored_count,
                next_item_index,
                next_plan_node_key: stored_next,
                ..
            } => {
                map_policy_requires_admission_barrier(failure_policy)
                    && stored_policy == &failure_policy
                    && stored_count == &item_count
                    && stored_next == next_plan_node_key
                    && *next_item_index < item_count
            }
            StoredControllerWait::MapSettlement {
                failure_policy: stored_policy,
                item_count: stored_count,
                admitted_item_count,
                next_plan_node_key: stored_next,
            } => {
                stored_policy == &failure_policy
                    && stored_count == &item_count
                    && *admitted_item_count == item_count
                    && stored_next == next_plan_node_key
            }
            _ => false,
        };
        if pending.controller_node_execution_id != root_map_node_execution_id
            || pending.plan_node_key != *map_plan_node_key
            || !pending.expected_scope_ids.is_empty()
            || !frozen_contract_matches
            || wait_node.is_some()
        {
            return Err(RepositoryError::Conflict("Map wait frozen contract"));
        }
        wait_node = Some(row.try_get::<String, _>("node_id")?.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?);
    }
    Ok(Some(ControllerStepShape::MapItemExit {
        map_wait_node_execution_id: wait_node,
        map_plan_node_key: map_plan_node_key.clone(),
        next_plan_node_key: next_plan_node_key.clone(),
        failure_policy,
        item_count,
        item_index,
        root_map_node_execution_id,
    }))
}

#[allow(clippy::too_many_arguments)]
async fn mutate_orchestration_controller_step(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    next_job: &JobProjection,
    parents: &LockedOrchestrationJobParents,
    source_node: &ControllerSourceNode,
    shape: &ControllerStepShape,
    mutations: &ControllerStepMutationIds,
    plan: &RuntimePlan,
    request_digest: &Sha256Digest,
    scope_environment_limits: ScopeEnvironmentLimits,
    derived_scope_bindings: &[DerivedNewScopeBinding],
    database_now: DateTime<Utc>,
) -> Result<AppliedOrchestrationControllerStep, RepositoryError> {
    let targets = shape.activation_targets()?;
    let slots = &mutations.activations;
    let pending_slots = &mutations.pending_nodes;
    if derived_scope_bindings
        .iter()
        .map(|binding| &binding.scope_instance_id)
        .collect::<BTreeSet<_>>()
        .len()
        != derived_scope_bindings.len()
        || derived_scope_bindings.iter().any(|binding| {
            !slots.iter().any(|slot| {
                slot.scope.as_ref().map(|scope| &scope.scope_instance_id)
                    == Some(&binding.scope_instance_id)
            })
        })
    {
        return Err(RepositoryError::InvalidInput(
            "derived Scope bindings do not match activation slots".to_owned(),
        ));
    }
    let result_payload = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "activated_plan_node_keys": targets,
            "pending_plan_node_keys": match shape {
                ControllerStepShape::Sequential { .. } => Vec::<PlanNodeKey>::new(),
                ControllerStepShape::Fork { join, .. } => vec![join.clone()],
                ControllerStepShape::LoopIteration {
                    loop_plan_node_key,
                    ..
                }
                | ControllerStepShape::LoopIterationExit {
                    loop_plan_node_key,
                    ..
                } => vec![loop_plan_node_key.clone()],
                ControllerStepShape::LoopConditionExit { .. } => Vec::new(),
                ControllerStepShape::MapBatch { next, .. }
                | ControllerStepShape::MapItemExit {
                    next_plan_node_key: next,
                    ..
                } => vec![next.clone()],
                ControllerStepShape::ParallelLegExit {
                    join_plan_node_key,
                    ..
                } => vec![join_plan_node_key.clone()],
            },
            "plan_node_key": source_node.plan_node_key,
            "source_node_kind": source_node.node_kind,
        }),
        65_536,
    )?;
    let job = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'succeeded', version = $4, result_digest = $5,
            worker_id = NULL, lease_token_digest = NULL,
            lease_expires_at = NULL, heartbeat_at = NULL,
            terminal_at = $6, updated_at = $6
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3 AND state = 'running'
        RETURNING *
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&current_job.job_id)
    .bind(current_job.version)
    .bind(
        i64::try_from(next_job.version)
            .map_err(|_| RepositoryError::InvalidInput("Job version exceeds bigint".to_owned()))?,
    )
    .bind(&result_payload.digest)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("controller source Job"))?;
    let source_job = job_from_row(job)?;
    let source_node_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'succeeded', version = version + 1,
            terminal_at = $5, updated_at = $5
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'node_execution' AND state = $4
        RETURNING version
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.node_id)
    .bind(source_node.version)
    .bind(NodeExecutionState::Running.as_str())
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("controller source Node"))?;

    let current_payload: OrchestrationJobPayload =
        decode_orchestration_job_payload(&current_job.payload)?;
    let mut activations = Vec::with_capacity(targets.len());
    let mut created_scopes = Vec::with_capacity(targets.len());
    for (index, (target, slot)) in targets.iter().zip(slots).enumerate() {
        let target_node = plan.node(target)?;
        let target_scope_id = if let ControllerStepShape::LoopConditionExit { context, .. } = shape
        {
            context.lexical_parent_scope_id.to_string()
        } else if let Some(scope) = &slot.scope {
            let (
                scope_kind,
                scope_parent_node_id,
                scope_controller_node_execution_id,
                logical_key,
                descriptor,
            ) = match shape {
                ControllerStepShape::Fork { join, .. } => (
                    "parallel_leg",
                    parents.node_id.clone(),
                    parents.node_id.parse().map_err(
                        |failure: insight_platform_contracts::ResourceIdError| {
                            RepositoryError::CorruptRow(failure.to_string())
                        },
                    )?,
                    format!("scope:{}:parallel_leg:{index}", parents.node_id),
                    StoredControllerScopeDescriptor::ParallelLeg {
                        leg_index: u32::try_from(index).map_err(|_| {
                            RepositoryError::InvalidInput("Fork leg index exceeds u32".to_owned())
                        })?,
                        leg_plan_node_key: target.clone(),
                        join_plan_node_key: join.clone(),
                    },
                ),
                ControllerStepShape::LoopIteration {
                    loop_plan_node_key,
                    iteration,
                    ..
                } => (
                    "loop_iteration",
                    parents.node_id.clone(),
                    parents.node_id.parse().map_err(
                        |failure: insight_platform_contracts::ResourceIdError| {
                            RepositoryError::CorruptRow(failure.to_string())
                        },
                    )?,
                    format!("scope:{}:loop_iteration:{iteration}", parents.node_id),
                    StoredControllerScopeDescriptor::LoopIteration {
                        iteration: *iteration,
                        loop_plan_node_key: loop_plan_node_key.clone(),
                    },
                ),
                ControllerStepShape::MapBatch {
                    next,
                    failure_policy,
                    item_count,
                    batch_start,
                    root_map_node_execution_id,
                    ..
                } => {
                    let item_index = batch_start
                        .checked_add(u32::try_from(index).map_err(|_| {
                            RepositoryError::InvalidInput("Map batch index exceeds u32".to_owned())
                        })?)
                        .ok_or_else(|| {
                            RepositoryError::InvalidInput("Map item index overflow".to_owned())
                        })?;
                    (
                        "map_item",
                        root_map_node_execution_id.to_string(),
                        root_map_node_execution_id.clone(),
                        format!("scope:{root_map_node_execution_id}:map_item:{item_index}"),
                        StoredControllerScopeDescriptor::MapItem {
                            failure_policy: *failure_policy,
                            item_count: *item_count,
                            item_index,
                            map_plan_node_key: source_node.plan_node_key.clone(),
                            next_plan_node_key: next.clone(),
                        },
                    )
                }
                _ => {
                    return Err(RepositoryError::InvalidInput(
                        "controller activation cannot create this Scope kind".to_owned(),
                    ));
                }
            };
            let environment = if let Some(binding) = derived_scope_bindings
                .iter()
                .find(|binding| binding.scope_instance_id == scope.scope_instance_id)
            {
                ScopeDataEnvironmentSnapshot::build(
                    BTreeMap::from([(binding.port.clone(), binding.value.clone())]),
                    scope_environment_limits,
                )
            } else {
                ScopeDataEnvironmentSnapshot::empty(scope_environment_limits)
            }
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
            let scope_payload = TypedPayload::with_limit(
                1,
                &StoredControllerScopePayload {
                    controller_node_execution_id: scope_controller_node_execution_id,
                    descriptor,
                    environment,
                },
                262_144,
            )?;
            sqlx::query(
                r#"
                INSERT INTO insight_platform.run_nodes (
                    tenant_id, node_id, run_id, parent_node_id, record_kind, scope_id,
                    plan_node_key, activation_ordinal, related_run_id, logical_key,
                    node_kind, state, payload_schema_version, payload, payload_digest, deadline
                ) VALUES (
                    $1, $2, $3, $4, 'scope_instance', $2,
                    NULL, NULL, NULL, $5, $6, 'open', $7, $8, $9, $10
                )
                "#,
            )
            .bind(&current_job.tenant_id)
            .bind(scope.scope_instance_id.to_string())
            .bind(&parents.run.run_id)
            .bind(scope_parent_node_id)
            .bind(logical_key)
            .bind(scope_kind)
            .bind(scope_payload.schema_version)
            .bind(&scope_payload.value)
            .bind(&scope_payload.digest)
            .bind(current_job.deadline.min(parents.run.deadline))
            .execute(&mut **transaction)
            .await?;
            created_scopes.push(ControllerScopeRecord {
                scope_id: scope.scope_instance_id.to_string(),
                scope_kind: scope_kind.to_owned(),
                state: ScopeState::Open.as_str().to_owned(),
                version: 1,
            });
            scope.scope_instance_id.to_string()
        } else {
            source_node.scope_id.clone()
        };
        let activation_ordinal: i32 = sqlx::query_scalar(
            r#"
            SELECT COALESCE(max(activation_ordinal), 0) + 1
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2
              AND record_kind = 'node_execution' AND plan_node_key = $3
            "#,
        )
        .bind(&current_job.tenant_id)
        .bind(&parents.run.run_id)
        .bind(target.as_str())
        .fetch_one(&mut **transaction)
        .await?;
        let node_payload = TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "plan_node_key": target,
                "plan_source_digest": parents.run.bindings.plan.semantic_digest,
                "required_control_tokens": [{
                    "source_node_execution_id": parents.node_id,
                    "source_port": "success",
                }],
            }),
            262_144,
        )?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.run_nodes (
                tenant_id, node_id, run_id, parent_node_id, record_kind, scope_id,
                plan_node_key, activation_ordinal, related_run_id, logical_key,
                node_kind, state, enqueue_round,
                payload_schema_version, payload, payload_digest, deadline
            ) VALUES (
                $1, $2, $3, $4, 'node_execution', $5,
                $6, $7, NULL, $8, $9, 'ready', 0, $10, $11, $12, $13
            )
            "#,
        )
        .bind(&current_job.tenant_id)
        .bind(slot.node_execution_id.to_string())
        .bind(&parents.run.run_id)
        .bind(&parents.node_id)
        .bind(&target_scope_id)
        .bind(target.as_str())
        .bind(activation_ordinal)
        .bind(format!(
            "controller:{}:{}:{}",
            parents.node_id,
            target.as_str(),
            index
        ))
        .bind(target_node.kind().as_str())
        .bind(node_payload.schema_version)
        .bind(&node_payload.value)
        .bind(&node_payload.digest)
        .bind(current_job.deadline.min(parents.run.deadline))
        .execute(&mut **transaction)
        .await?;

        let job_payload = OrchestrationJobPayload {
            external_leaf_completion: None,
            bindings_digest: parents.run.bindings.canonical_digest.clone(),
            node_execution_id: slot.node_execution_id.clone(),
            root_scope_id: current_payload.root_scope_id.clone(),
            retry_backoff_milliseconds: current_payload.retry_backoff_milliseconds,
            wake_contract: None,
            convergence_failure: None,
            model_tool_continuation: None,
        };
        job_payload
            .validate()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        let job_payload = job_payload.to_payload()?;
        let row = sqlx::query(
            r#"
            INSERT INTO insight_platform.jobs (
                tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, trace_id, run_id, node_id, state, attempt_limit, scheduled_at, deadline, priority, request_digest, payload_schema_version, payload, payload_digest, scheduler_partition_id, execution_requirement_version, execution_requirement, execution_requirement_digest
            ) VALUES (
                $1, $2, 'orchestration_node', 'orchestration', 'node_execution', $3, (SELECT trace_id FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $4), $4, $3, 'ready', $5, $6, $7, $8, $9, $10, $11, $12, (SELECT scheduler_partition_id FROM insight_platform.tenants WHERE tenant_id=$1), (SELECT execution_requirement_version FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement_digest FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4)
            ) RETURNING *
            "#,
        )
        .bind(&current_job.tenant_id)
        .bind(slot.orchestration_job_id.to_string())
        .bind(slot.node_execution_id.to_string())
        .bind(&parents.run.run_id)
        .bind(current_job.attempt_limit)
        .bind(database_now)
        .bind(current_job.deadline.min(parents.run.deadline))
        .bind(scheduler_priority_to_database(current_job.priority))
        .bind(&job_payload.digest)
        .bind(job_payload.schema_version)
        .bind(&job_payload.value)
        .bind(&job_payload.digest)
        .fetch_one(&mut **transaction)
        .await?;
        activations.push(ControllerActivationRecord {
            node_id: slot.node_execution_id.to_string(),
            plan_node_key: target.clone(),
            node_kind: target_node.kind(),
            node_version: 1,
            job: job_from_row(row)?,
        });
    }

    let mut pending_nodes = Vec::with_capacity(pending_slots.len());
    let pending_spec = match shape {
        ControllerStepShape::Fork {
            join,
            policy,
            quorum,
            remainder,
            ..
        } => Some((
            join.clone(),
            parents.node_id.parse().map_err(
                |failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                },
            )?,
            parents.node_id.clone(),
            slots
                .iter()
                .map(|slot| {
                    slot.scope
                        .as_ref()
                        .map(|scope| scope.scope_instance_id.clone())
                        .ok_or_else(|| {
                            RepositoryError::InvalidInput(
                                "Fork leg is missing its ParallelLeg Scope".to_owned(),
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?,
            StoredControllerWait::Join {
                policy: *policy,
                quorum: *quorum,
                remainder: *remainder,
            },
        )),
        ControllerStepShape::LoopIteration {
            loop_plan_node_key,
            iteration,
            existing_scope,
            ..
        } => Some((
            loop_plan_node_key.clone(),
            existing_scope
                .as_ref()
                .map(|context| context.root_loop_node_execution_id.clone())
                .unwrap_or(parents.node_id.parse().map_err(
                    |failure: insight_platform_contracts::ResourceIdError| {
                        RepositoryError::CorruptRow(failure.to_string())
                    },
                )?),
            existing_scope
                .as_ref()
                .map(|context| context.root_loop_node_execution_id.to_string())
                .unwrap_or_else(|| parents.node_id.clone()),
            vec![if let Some(context) = existing_scope {
                context.scope_id.clone()
            } else {
                slots
                    .first()
                    .and_then(|slot| slot.scope.as_ref())
                    .map(|scope| scope.scope_instance_id.clone())
                    .ok_or_else(|| {
                        RepositoryError::InvalidInput(
                            "Loop body is missing its LoopIteration Scope".to_owned(),
                        )
                    })?
            }],
            StoredControllerWait::Loop {
                iteration: iteration.checked_add(1).ok_or_else(|| {
                    RepositoryError::InvalidInput("Loop iteration overflow".to_owned())
                })?,
            },
        )),
        ControllerStepShape::MapBatch {
            body,
            next,
            failure_policy,
            item_count,
            batch_start,
            batch_size,
            root_map_node_execution_id,
            has_more,
        } => {
            let next_item_index = batch_start.checked_add(*batch_size).ok_or_else(|| {
                RepositoryError::InvalidInput("Map batch cursor overflow".to_owned())
            })?;
            let wait = if *has_more {
                StoredControllerWait::MapAdmission {
                    body_plan_node_key: body.clone(),
                    failure_policy: *failure_policy,
                    item_count: *item_count,
                    next_item_index,
                    next_plan_node_key: next.clone(),
                }
            } else {
                StoredControllerWait::MapSettlement {
                    failure_policy: *failure_policy,
                    item_count: *item_count,
                    admitted_item_count: *item_count,
                    next_plan_node_key: next.clone(),
                }
            };
            Some((
                source_node.plan_node_key.clone(),
                root_map_node_execution_id.clone(),
                root_map_node_execution_id.to_string(),
                Vec::new(),
                wait,
            ))
        }
        _ => None,
    };
    let mut immediately_woken_nodes = Vec::new();
    if let Some((
        pending_plan_node_key,
        controller_node_execution_id,
        pending_parent_node_id,
        expected_scope_ids,
        wait,
    )) = pending_spec
    {
        let pending_slot = pending_slots.first().ok_or_else(|| {
            RepositoryError::InvalidInput(
                "controller is missing its pending continuation slot".to_owned(),
            )
        })?;
        let pending_node = plan.node(&pending_plan_node_key)?;
        let pending_payload = TypedPayload::with_limit(
            1,
            &StoredPendingControllerNodePayload {
                controller_node_execution_id,
                expected_scope_ids,
                plan_node_key: pending_plan_node_key.clone(),
                wait,
            },
            262_144,
        )?;
        let activation_ordinal: i32 = sqlx::query_scalar(
            r#"
            SELECT COALESCE(max(activation_ordinal), 0) + 1
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2
              AND record_kind = 'node_execution' AND plan_node_key = $3
            "#,
        )
        .bind(&current_job.tenant_id)
        .bind(&parents.run.run_id)
        .bind(pending_plan_node_key.as_str())
        .fetch_one(&mut **transaction)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.run_nodes (
                tenant_id, node_id, run_id, parent_node_id, record_kind, scope_id,
                plan_node_key, activation_ordinal, related_run_id, logical_key,
                node_kind, state, payload_schema_version, payload, payload_digest, deadline
            ) VALUES (
                $1, $2, $3, $4, 'node_execution', $5,
                $6, $7, NULL, $8, $9, 'pending', $10, $11, $12, $13
            )
            "#,
        )
        .bind(&current_job.tenant_id)
        .bind(pending_slot.node_execution_id.to_string())
        .bind(&parents.run.run_id)
        .bind(&pending_parent_node_id)
        .bind(&source_node.scope_id)
        .bind(pending_plan_node_key.as_str())
        .bind(activation_ordinal)
        .bind(format!(
            "controller:{}:pending:{}",
            parents.node_id,
            pending_plan_node_key.as_str()
        ))
        .bind(pending_node.kind().as_str())
        .bind(pending_payload.schema_version)
        .bind(&pending_payload.value)
        .bind(&pending_payload.digest)
        .bind(current_job.deadline.min(parents.run.deadline))
        .execute(&mut **transaction)
        .await?;
        pending_nodes.push(ControllerPendingNodeRecord {
            node_id: pending_slot.node_execution_id.to_string(),
            plan_node_key: pending_plan_node_key.clone(),
            node_kind: pending_node.kind(),
            node_version: 1,
        });
        if matches!(
            shape,
            ControllerStepShape::MapBatch {
                failure_policy: MapFailurePolicy::AllSettled,
                has_more: true,
                ..
            }
        ) {
            let wake = mutations.pending_wake.as_ref().ok_or_else(|| {
                RepositoryError::InvalidInput(
                    "Map admission continuation is missing its wake slot".to_owned(),
                )
            })?;
            let node_version: i64 = sqlx::query_scalar(
                r#"
                UPDATE insight_platform.run_nodes
                SET state = 'ready', version = version + 1, enqueue_round = 0,
                    updated_at = clock_timestamp()
                WHERE tenant_id = $1 AND node_id = $2 AND version = $3
                  AND record_kind = 'node_execution' AND state = 'pending'
                  AND terminal_at IS NULL
                RETURNING version
                "#,
            )
            .bind(&current_job.tenant_id)
            .bind(pending_slot.node_execution_id.to_string())
            .bind(1_i64)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(RepositoryError::Conflict("Map admission continuation wake"))?;
            let job_payload = OrchestrationJobPayload {
                external_leaf_completion: None,
                bindings_digest: parents.run.bindings.canonical_digest.clone(),
                node_execution_id: pending_slot.node_execution_id.clone(),
                root_scope_id: current_payload.root_scope_id.clone(),
                retry_backoff_milliseconds: current_payload.retry_backoff_milliseconds,
                wake_contract: None,
                convergence_failure: None,
                model_tool_continuation: None,
            };
            job_payload
                .validate()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            let job_payload = job_payload.to_payload()?;
            let row = sqlx::query(
                r#"
                INSERT INTO insight_platform.jobs (
                tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, trace_id, run_id, node_id, state, attempt_limit, scheduled_at, deadline, priority, request_digest, payload_schema_version, payload, payload_digest, scheduler_partition_id, execution_requirement_version, execution_requirement, execution_requirement_digest
            ) VALUES (
                $1, $2, 'orchestration_node', 'orchestration', 'node_execution', $3, (SELECT trace_id FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $4), $4, $3, 'ready', $5, $6, $7, $8, $9, $10, $11, $12, (SELECT scheduler_partition_id FROM insight_platform.tenants WHERE tenant_id=$1), (SELECT execution_requirement_version FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement_digest FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4)
            ) RETURNING *
                "#,
            )
            .bind(&current_job.tenant_id)
            .bind(wake.orchestration_job_id.to_string())
            .bind(pending_slot.node_execution_id.to_string())
            .bind(&parents.run.run_id)
            .bind(current_job.attempt_limit)
            .bind(database_now)
            .bind(current_job.deadline.min(parents.run.deadline))
            .bind(scheduler_priority_to_database(current_job.priority))
            .bind(wake.request_digest.to_string())
            .bind(job_payload.schema_version)
            .bind(&job_payload.value)
            .bind(&job_payload.digest)
            .fetch_one(&mut **transaction)
            .await?;
            immediately_woken_nodes.push(ControllerActivationRecord {
                node_id: pending_slot.node_execution_id.to_string(),
                plan_node_key: pending_plan_node_key,
                node_kind: pending_node.kind(),
                node_version,
                job: job_from_row(row)?,
            });
        }
    }

    let mut structural = match shape {
        ControllerStepShape::LoopIterationExit { .. } => {
            settle_loop_iteration_and_wake_continuation(
                transaction,
                current_job,
                parents,
                shape,
                mutations,
                plan,
                scope_environment_limits,
                database_now,
            )
            .await?
        }
        ControllerStepShape::MapItemExit { .. } => {
            settle_map_item_and_maybe_wake_settlement(
                transaction,
                current_job,
                parents,
                shape,
                mutations,
                plan,
                ChildOutcome::Succeeded,
                ScopeState::Succeeded,
                request_digest,
                database_now,
            )
            .await?
        }
        ControllerStepShape::LoopConditionExit { context, .. } => {
            close_loop_condition_scope(
                transaction,
                current_job,
                parents,
                context,
                mutations,
                database_now,
            )
            .await?
        }
        _ => {
            settle_parallel_leg_and_maybe_wake_join(
                transaction,
                current_job,
                parents,
                shape,
                mutations,
                plan,
                ChildOutcome::Succeeded,
                ScopeState::Succeeded,
                request_digest,
                database_now,
            )
            .await?
        }
    };
    if matches!(shape, ControllerStepShape::LoopIterationExit { .. }) {
        let rollover = mutations
            .structural_exit
            .as_ref()
            .and_then(|exit| exit.loop_rollover.as_ref())
            .ok_or_else(|| {
                RepositoryError::CorruptRow("Loop rollover Scope slot disappeared".to_owned())
            })?;
        created_scopes.push(ControllerScopeRecord {
            scope_id: rollover.scope.scope_instance_id.to_string(),
            scope_kind: "loop_iteration".to_owned(),
            state: ScopeState::Open.as_str().to_owned(),
            version: 1,
        });
    }
    structural.woken_nodes.extend(immediately_woken_nodes);
    let active_work_decrement = i32::try_from(
        1_usize
            .checked_add(structural.cancelled_active_work_count)
            .ok_or_else(|| {
                RepositoryError::InvalidInput(
                    "controller active-work decrement exceeds the platform bound".to_owned(),
                )
            })?,
    )
    .map_err(|_| {
        RepositoryError::InvalidInput("controller active-work decrement exceeds integer".to_owned())
    })?;

    let run = sqlx::query(
        r#"
        UPDATE insight_platform.runs
        SET version = version + 1, active_work_count = active_work_count - $4,
            updated_at = $5
        WHERE tenant_id = $1 AND run_id = $2 AND version = $3
          AND state = 'running' AND active_work_count >= $4 AND terminal_at IS NULL
        RETURNING *
        "#,
    )
    .bind(&parents.run.tenant_id)
    .bind(&parents.run.run_id)
    .bind(parents.run.version)
    .bind(active_work_decrement)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("controller source Run"))?;
    Ok(AppliedOrchestrationControllerStep {
        run: run_from_row(run)?,
        source_node_id: parents.node_id.clone(),
        source_node_version,
        source_job,
        activations,
        created_scopes,
        pending_nodes,
        settled_scopes: structural.settled_scopes,
        woken_nodes: structural.woken_nodes,
        cancelled_remainders: structural.cancelled_remainders,
        settled_quota_account_ids: Vec::new(),
    })
}

#[derive(Debug)]
struct ParallelLegSettlement {
    settled_scopes: Vec<ControllerScopeRecord>,
    woken_nodes: Vec<ControllerActivationRecord>,
    cancelled_remainders: Vec<ControllerCancelledRemainderRecord>,
    cancelled_active_work_count: usize,
}

impl ParallelLegSettlement {
    fn empty() -> Self {
        Self {
            settled_scopes: Vec::new(),
            woken_nodes: Vec::new(),
            cancelled_remainders: Vec::new(),
            cancelled_active_work_count: 0,
        }
    }
}

async fn close_loop_condition_scope(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    parents: &LockedOrchestrationJobParents,
    context: &OpenLoopIterationContext,
    mutations: &ControllerStepMutationIds,
    database_now: DateTime<Utc>,
) -> Result<ParallelLegSettlement, RepositoryError> {
    let structural = mutations.structural_exit.as_ref().ok_or_else(|| {
        RepositoryError::InvalidInput("Loop condition exit has no Scope event slot".to_owned())
    })?;
    if structural.loop_rollover.is_some()
        || context.scope_id.to_string() != parents.scope_id
        || context.iteration == 0
    {
        return Err(RepositoryError::InvalidInput(
            "Loop condition exit mutation shape".to_owned(),
        ));
    }
    let row = sqlx::query(
        r#"
        SELECT parent_node_id, node_kind, state, version,
               payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
          AND record_kind = 'scope_instance'
        FOR UPDATE
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.run.run_id)
    .bind(context.scope_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Loop condition Scope"))?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let stored: StoredControllerScopePayload =
        decode_typed_payload(&payload, "Loop condition Scope")?;
    if row.try_get::<String, _>("node_kind")? != "loop_iteration"
        || row.try_get::<String, _>("state")? != ScopeState::Open.as_str()
        || row.try_get::<i64, _>("version")? != parents.scope_version
        || row.try_get::<Option<String>, _>("parent_node_id")?
            != Some(context.root_loop_node_execution_id.to_string())
        || stored.controller_node_execution_id != context.root_loop_node_execution_id
        || stored.descriptor
            != (StoredControllerScopeDescriptor::LoopIteration {
                iteration: context.iteration,
                loop_plan_node_key: context.loop_plan_node_key.clone(),
            })
    {
        return Err(RepositoryError::Conflict("Loop condition Scope fence"));
    }
    let closing_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'closing', version = version + 1, updated_at = $4
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'scope_instance' AND state = 'open' AND terminal_at IS NULL
        RETURNING version
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(context.scope_id.to_string())
    .bind(parents.scope_version)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Loop condition Scope closing"))?;
    let version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'succeeded', version = version + 1,
            terminal_at = $4, updated_at = $4
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'scope_instance' AND state = 'closing'
        RETURNING version
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(context.scope_id.to_string())
    .bind(closing_version)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Loop condition Scope terminal"))?;
    Ok(ParallelLegSettlement {
        settled_scopes: vec![ControllerScopeRecord {
            scope_id: context.scope_id.to_string(),
            scope_kind: "loop_iteration".to_owned(),
            state: ScopeState::Succeeded.as_str().to_owned(),
            version,
        }],
        woken_nodes: Vec::new(),
        cancelled_remainders: Vec::new(),
        cancelled_active_work_count: 0,
    })
}

#[allow(clippy::too_many_arguments)]
async fn settle_loop_iteration_and_wake_continuation(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    parents: &LockedOrchestrationJobParents,
    shape: &ControllerStepShape,
    mutations: &ControllerStepMutationIds,
    plan: &RuntimePlan,
    scope_environment_limits: ScopeEnvironmentLimits,
    database_now: DateTime<Utc>,
) -> Result<ParallelLegSettlement, RepositoryError> {
    let ControllerStepShape::LoopIterationExit {
        loop_node_execution_id,
        root_loop_node_execution_id,
        loop_plan_node_key,
        expected_scope_id,
        iteration,
    } = shape
    else {
        return Ok(ParallelLegSettlement::empty());
    };
    if expected_scope_id.to_string() != parents.scope_id {
        return Err(RepositoryError::Conflict(
            "LoopIteration source Scope identity",
        ));
    }
    let structural_exit = mutations.structural_exit.as_ref().ok_or_else(|| {
        RepositoryError::InvalidInput("Loop exit is missing its Scope event slot".to_owned())
    })?;
    let _ = structural_exit;
    let pending_wake = mutations.pending_wake.as_ref().ok_or_else(|| {
        RepositoryError::InvalidInput("Loop exit is missing its wake slot".to_owned())
    })?;
    let scope_row = sqlx::query(
        r#"
        SELECT parent_node_id, node_kind, state, version,
               payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
          AND record_kind = 'scope_instance'
        FOR UPDATE
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.run.run_id)
    .bind(&parents.scope_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("LoopIteration Scope settlement"))?;
    if scope_row.try_get::<String, _>("node_kind")? != "loop_iteration"
        || scope_row.try_get::<String, _>("state")? != ScopeState::Open.as_str()
        || scope_row.try_get::<i64, _>("version")? != parents.scope_version
    {
        return Err(RepositoryError::Conflict(
            "LoopIteration Scope current state",
        ));
    }
    let scope_payload = payload_from_row(
        &scope_row,
        "payload_schema_version",
        "payload",
        "payload_digest",
    )?;
    let stored_scope: StoredControllerScopePayload =
        decode_typed_payload(&scope_payload, "LoopIteration Scope")?;
    let controller_node_id = stored_scope.controller_node_execution_id.to_string();
    if scope_row
        .try_get::<Option<String>, _>("parent_node_id")?
        .as_deref()
        != Some(controller_node_id.as_str())
        || stored_scope.descriptor
            != (StoredControllerScopeDescriptor::LoopIteration {
                iteration: *iteration,
                loop_plan_node_key: loop_plan_node_key.clone(),
            })
    {
        return Err(RepositoryError::Conflict(
            "LoopIteration Scope frozen descriptor",
        ));
    }
    if stored_scope.controller_node_execution_id != *root_loop_node_execution_id {
        return Err(RepositoryError::Conflict("Loop root controller owner"));
    }
    let loop_node = plan.node(loop_plan_node_key)?;
    let insight_platform_plan::RuntimeNode::Loop { carried_ports, .. } = loop_node else {
        return Err(RepositoryError::Conflict("Loop continuation Plan node"));
    };
    let rollover = mutations
        .structural_exit
        .as_ref()
        .and_then(|exit| exit.loop_rollover.as_ref())
        .ok_or_else(|| {
            RepositoryError::InvalidInput("Loop exit is missing its rollover slot".to_owned())
        })?;
    if rollover.carried_value_ids.len() != carried_ports.len() {
        return Err(RepositoryError::InvalidInput(
            "Loop carried value mutation shape".to_owned(),
        ));
    }
    let tenant_id: ResourceId = current_job.tenant_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let run_id: ResourceId = parents.run.run_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let current_scope_id: ResourceId = parents.scope_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let body_ports = carried_ports
        .iter()
        .map(|port| port.body_output_port.clone())
        .collect::<Vec<_>>();
    let environments = load_scope_environment_chain(
        transaction,
        &tenant_id,
        &run_id,
        &current_scope_id,
        scope_environment_limits,
    )
    .await?;
    let references = insight_platform_orchestrator::resolve_scope_inputs(
        &body_ports,
        &environments,
        scope_environment_limits,
    )?;
    let resolved =
        load_resolved_expression_values(transaction, &tenant_id, &run_id, body_ports, references)
            .await?;
    let current_value_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.run_values WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.run.run_id)
    .fetch_one(&mut **transaction)
    .await?;
    let next_value_count = usize::try_from(current_value_count)
        .ok()
        .and_then(|count| count.checked_add(carried_ports.len()))
        .ok_or(RepositoryError::Conflict("RunValue reference bound"))?;
    if next_value_count > scope_environment_limits.maximum_bindings_per_scope {
        return Err(RepositoryError::Conflict("RunValue reference bound"));
    }
    let mut next_bindings = BTreeMap::new();
    for ((carried, source), value_id) in carried_ports
        .iter()
        .zip(resolved)
        .zip(&rollover.carried_value_ids)
    {
        if source.schema_digest != *carried.next_iteration_port.schema_digest()
            || source.schema_digest != *carried.body_output_port.schema_digest()
        {
            return Err(RepositoryError::Conflict("Loop carried schema evidence"));
        }
        let (inline_value, artifact_id) = match source.value {
            ValueRef::Inline { value } => (Some(value), None),
            ValueRef::Artifact { artifact } => (None, Some(artifact.artifact_id().to_string())),
        };
        sqlx::query(
            r#"
            INSERT INTO insight_platform.run_values (
                tenant_id, value_id, run_id, node_id, value_kind, classification,
                schema_digest, content_digest, inline_value, artifact_id
            ) VALUES ($1, $2, $3, $4, 'loop_carried', $5, $6, $7, $8, $9)
            "#,
        )
        .bind(&current_job.tenant_id)
        .bind(value_id.to_string())
        .bind(&parents.run.run_id)
        .bind(&parents.node_id)
        .bind(source.classification.as_str())
        .bind(source.schema_digest.to_string())
        .bind(source.content_digest.to_string())
        .bind(inline_value)
        .bind(artifact_id)
        .execute(&mut **transaction)
        .await?;
        next_bindings.insert(
            carried.next_iteration_port.clone(),
            ExactRunValueRef {
                value_id: value_id.clone(),
                schema_digest: source.schema_digest,
                content_digest: source.content_digest,
            },
        );
    }
    let next_iteration = iteration.checked_add(1).ok_or_else(|| {
        RepositoryError::CorruptRow("Loop iteration counter overflowed".to_owned())
    })?;
    let next_environment =
        ScopeDataEnvironmentSnapshot::build(next_bindings, scope_environment_limits)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let next_scope_payload = TypedPayload::with_limit(
        1,
        &StoredControllerScopePayload {
            controller_node_execution_id: root_loop_node_execution_id.clone(),
            descriptor: StoredControllerScopeDescriptor::LoopIteration {
                iteration: next_iteration,
                loop_plan_node_key: loop_plan_node_key.clone(),
            },
            environment: next_environment,
        },
        262_144,
    )?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_nodes (
            tenant_id, node_id, run_id, parent_node_id, record_kind, scope_id,
            plan_node_key, activation_ordinal, related_run_id, logical_key,
            node_kind, state, payload_schema_version, payload, payload_digest, deadline
        ) VALUES (
            $1, $2, $3, $4, 'scope_instance', $2,
            NULL, NULL, NULL, $5, 'loop_iteration', 'open', $6, $7, $8, $9
        )
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(rollover.scope.scope_instance_id.to_string())
    .bind(&parents.run.run_id)
    .bind(root_loop_node_execution_id.to_string())
    .bind(format!(
        "scope:{root_loop_node_execution_id}:loop_iteration:{next_iteration}"
    ))
    .bind(next_scope_payload.schema_version)
    .bind(&next_scope_payload.value)
    .bind(&next_scope_payload.digest)
    .bind(current_job.deadline.min(parents.run.deadline))
    .execute(&mut **transaction)
    .await?;
    let closing_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'closing', version = version + 1, updated_at = $4
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'scope_instance' AND state = 'open' AND terminal_at IS NULL
        RETURNING version
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.scope_id)
    .bind(parents.scope_version)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("LoopIteration Scope closing"))?;
    let scope_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'succeeded', version = version + 1,
            terminal_at = $4, updated_at = $4
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'scope_instance' AND state = 'closing'
        RETURNING version
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.scope_id)
    .bind(closing_version)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("LoopIteration Scope terminal"))?;

    let pending_row = sqlx::query(
        r#"
        SELECT parent_node_id, plan_node_key, node_kind, state, version,
               payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
          AND record_kind = 'node_execution'
        FOR UPDATE
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.run.run_id)
    .bind(loop_node_execution_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("pending Loop continuation"))?;
    if pending_row
        .try_get::<Option<String>, _>("parent_node_id")?
        .as_deref()
        != Some(controller_node_id.as_str())
        || pending_row.try_get::<String, _>("plan_node_key")? != loop_plan_node_key.as_str()
        || pending_row.try_get::<String, _>("node_kind")? != PlanNodeKind::Loop.as_str()
        || pending_row.try_get::<String, _>("state")? != NodeExecutionState::Pending.as_str()
    {
        return Err(RepositoryError::Conflict(
            "pending Loop continuation ownership",
        ));
    }
    let pending_payload = payload_from_row(
        &pending_row,
        "payload_schema_version",
        "payload",
        "payload_digest",
    )?;
    let stored_pending: StoredPendingControllerNodePayload =
        decode_typed_payload(&pending_payload, "pending Loop continuation")?;
    if stored_pending.controller_node_execution_id != stored_scope.controller_node_execution_id
        || stored_pending.plan_node_key != *loop_plan_node_key
        || stored_pending.expected_scope_ids != vec![expected_scope_id.clone()]
        || stored_pending.wait
            != (StoredControllerWait::Loop {
                iteration: next_iteration,
            })
    {
        return Err(RepositoryError::Conflict(
            "pending Loop continuation contract",
        ));
    }
    let node_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'ready', scope_id = $5, version = version + 1, enqueue_round = 0,
            updated_at = $4
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'node_execution' AND state = 'pending'
          AND terminal_at IS NULL
        RETURNING version
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(loop_node_execution_id.to_string())
    .bind(pending_row.try_get::<i64, _>("version")?)
    .bind(database_now)
    .bind(rollover.scope.scope_instance_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Loop continuation wake"))?;
    let current_payload: OrchestrationJobPayload =
        decode_orchestration_job_payload(&current_job.payload)?;
    let job_payload = OrchestrationJobPayload {
        external_leaf_completion: None,
        bindings_digest: parents.run.bindings.canonical_digest.clone(),
        node_execution_id: loop_node_execution_id.clone(),
        root_scope_id: current_payload.root_scope_id,
        retry_backoff_milliseconds: current_payload.retry_backoff_milliseconds,
        wake_contract: None,
        convergence_failure: None,
        model_tool_continuation: None,
    };
    job_payload
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let job_payload = job_payload.to_payload()?;
    let job = sqlx::query(
        r#"
        INSERT INTO insight_platform.jobs (
                tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, trace_id, run_id, node_id, state, attempt_limit, scheduled_at, deadline, priority, request_digest, payload_schema_version, payload, payload_digest, scheduler_partition_id, execution_requirement_version, execution_requirement, execution_requirement_digest
            ) VALUES (
                $1, $2, 'orchestration_node', 'orchestration', 'node_execution', $3, (SELECT trace_id FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $4), $4, $3, 'ready', $5, $6, $7, $8, $9, $10, $11, $12, (SELECT scheduler_partition_id FROM insight_platform.tenants WHERE tenant_id=$1), (SELECT execution_requirement_version FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement_digest FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4)
            ) RETURNING *
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(pending_wake.orchestration_job_id.to_string())
    .bind(loop_node_execution_id.to_string())
    .bind(&parents.run.run_id)
    .bind(current_job.attempt_limit)
    .bind(database_now)
    .bind(current_job.deadline.min(parents.run.deadline))
    .bind(scheduler_priority_to_database(current_job.priority))
    .bind(pending_wake.request_digest.to_string())
    .bind(job_payload.schema_version)
    .bind(&job_payload.value)
    .bind(&job_payload.digest)
    .fetch_one(&mut **transaction)
    .await?;
    Ok(ParallelLegSettlement {
        settled_scopes: vec![ControllerScopeRecord {
            scope_id: parents.scope_id.clone(),
            scope_kind: "loop_iteration".to_owned(),
            state: ScopeState::Succeeded.as_str().to_owned(),
            version: scope_version,
        }],
        woken_nodes: vec![ControllerActivationRecord {
            node_id: loop_node_execution_id.to_string(),
            plan_node_key: loop_plan_node_key.clone(),
            node_kind: PlanNodeKind::Loop,
            node_version,
            job: job_from_row(job)?,
        }],
        cancelled_remainders: Vec::new(),
        cancelled_active_work_count: 0,
    })
}

#[allow(clippy::too_many_arguments)]
async fn settle_map_item_and_maybe_wake_settlement(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    parents: &LockedOrchestrationJobParents,
    shape: &ControllerStepShape,
    mutations: &ControllerStepMutationIds,
    plan: &RuntimePlan,
    source_outcome: ChildOutcome,
    source_scope_state: ScopeState,
    request_digest: &Sha256Digest,
    database_now: DateTime<Utc>,
) -> Result<ParallelLegSettlement, RepositoryError> {
    if !matches!(
        (source_outcome, source_scope_state),
        (ChildOutcome::Succeeded, ScopeState::Succeeded)
            | (ChildOutcome::Failed, ScopeState::Failed)
    ) {
        return Err(RepositoryError::InvalidInput(
            "MapItem source outcome and Scope state do not match".to_owned(),
        ));
    }
    let ControllerStepShape::MapItemExit {
        map_wait_node_execution_id,
        map_plan_node_key,
        next_plan_node_key,
        failure_policy,
        item_count,
        item_index,
        root_map_node_execution_id,
    } = shape
    else {
        return Ok(ParallelLegSettlement::empty());
    };
    let root_map_node_id = root_map_node_execution_id.to_string();
    // Every item terminal transaction for a Map generation locks the shared wait Node first.
    // That gives sibling exits one deterministic lock order (wait -> Scopes) and avoids the
    // source-Scope/wait-Node inversion that would otherwise deadlock concurrent terminal winners.
    let wait_state = if let Some(wait_node_execution_id) = map_wait_node_execution_id {
        let wait_row = sqlx::query(
            r#"
            SELECT parent_node_id, plan_node_key, node_kind, state, version,
                   payload_schema_version, payload, payload_digest
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
              AND record_kind = 'node_execution'
            FOR UPDATE
            "#,
        )
        .bind(&current_job.tenant_id)
        .bind(&parents.run.run_id)
        .bind(wait_node_execution_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("pending Map wait Node"))?;
        if wait_row
            .try_get::<Option<String>, _>("parent_node_id")?
            .as_deref()
            != Some(root_map_node_id.as_str())
            || wait_row.try_get::<String, _>("plan_node_key")? != map_plan_node_key.as_str()
            || wait_row.try_get::<String, _>("node_kind")? != PlanNodeKind::Map.as_str()
            || wait_row.try_get::<String, _>("state")? != NodeExecutionState::Pending.as_str()
        {
            return Err(RepositoryError::Conflict("pending Map wait ownership"));
        }
        let wait_payload = payload_from_row(
            &wait_row,
            "payload_schema_version",
            "payload",
            "payload_digest",
        )?;
        let stored_pending: StoredPendingControllerNodePayload =
            decode_typed_payload(&wait_payload, "pending Map wait Node")?;
        if stored_pending.controller_node_execution_id != *root_map_node_execution_id
            || stored_pending.plan_node_key != *map_plan_node_key
            || !stored_pending.expected_scope_ids.is_empty()
        {
            return Err(RepositoryError::Conflict("pending Map wait contract"));
        }
        Some((wait_node_execution_id.clone(), wait_row, stored_pending))
    } else {
        None
    };
    let structural_exit = mutations.structural_exit.as_ref().ok_or_else(|| {
        RepositoryError::InvalidInput("Map item exit is missing its Scope event slot".to_owned())
    })?;
    let _ = structural_exit;
    let scope_row = sqlx::query(
        r#"
        SELECT parent_node_id, node_kind, state, version,
               payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
          AND record_kind = 'scope_instance'
        FOR UPDATE
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.run.run_id)
    .bind(&parents.scope_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("MapItem Scope settlement"))?;
    if scope_row.try_get::<String, _>("node_kind")? != "map_item"
        || scope_row.try_get::<String, _>("state")? != ScopeState::Open.as_str()
        || scope_row.try_get::<i64, _>("version")? != parents.scope_version
        || scope_row
            .try_get::<Option<String>, _>("parent_node_id")?
            .as_deref()
            != Some(root_map_node_id.as_str())
    {
        return Err(RepositoryError::Conflict("MapItem Scope current state"));
    }
    let scope_payload = payload_from_row(
        &scope_row,
        "payload_schema_version",
        "payload",
        "payload_digest",
    )?;
    let stored_scope: StoredControllerScopePayload =
        decode_typed_payload(&scope_payload, "MapItem Scope")?;
    if stored_scope.controller_node_execution_id != *root_map_node_execution_id
        || stored_scope.descriptor
            != (StoredControllerScopeDescriptor::MapItem {
                failure_policy: *failure_policy,
                item_count: *item_count,
                item_index: *item_index,
                map_plan_node_key: map_plan_node_key.clone(),
                next_plan_node_key: next_plan_node_key.clone(),
            })
    {
        return Err(RepositoryError::Conflict("MapItem Scope frozen descriptor"));
    }
    let closing_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'closing', version = version + 1, updated_at = $4
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'scope_instance' AND state = 'open' AND terminal_at IS NULL
        RETURNING version
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.scope_id)
    .bind(parents.scope_version)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("MapItem Scope closing"))?;
    let scope_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = $4, version = version + 1,
            terminal_at = $5, updated_at = $5
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'scope_instance' AND state = 'closing'
        RETURNING version
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.scope_id)
    .bind(closing_version)
    .bind(source_scope_state.as_str())
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("MapItem Scope terminal"))?;
    let settled_scopes = vec![ControllerScopeRecord {
        scope_id: parents.scope_id.clone(),
        scope_kind: "map_item".to_owned(),
        state: source_scope_state.as_str().to_owned(),
        version: scope_version,
    }];
    let Some((wait_node_execution_id, wait_row, mut stored_pending)) = wait_state else {
        return Ok(ParallelLegSettlement {
            settled_scopes,
            woken_nodes: Vec::new(),
            cancelled_remainders: Vec::new(),
            cancelled_active_work_count: 0,
        });
    };
    let map_node = plan.node(map_plan_node_key)?;
    let insight_platform_plan::RuntimeNode::Map { body, .. } = map_node else {
        return Err(RepositoryError::Conflict("Map wait Plan node"));
    };
    let (admitted_item_count, admission_barrier) = match &stored_pending.wait {
        StoredControllerWait::MapAdmission {
            body_plan_node_key,
            failure_policy: stored_policy,
            item_count: stored_count,
            next_item_index,
            next_plan_node_key: stored_next,
        } if map_policy_requires_admission_barrier(*failure_policy)
            && body_plan_node_key == body
            && stored_policy == failure_policy
            && stored_count == item_count
            && stored_next == next_plan_node_key
            && *next_item_index > 0
            && *next_item_index < *item_count =>
        {
            (*next_item_index, true)
        }
        StoredControllerWait::MapSettlement {
            failure_policy: stored_policy,
            item_count: stored_count,
            admitted_item_count,
            next_plan_node_key: stored_next,
        } if stored_policy == failure_policy
            && stored_count == item_count
            && stored_next == next_plan_node_key
            && *admitted_item_count > 0
            && *admitted_item_count <= *item_count =>
        {
            (*admitted_item_count, false)
        }
        _ => {
            return Err(RepositoryError::Conflict(
                "pending Map wait frozen contract",
            ))
        }
    };
    let rows = sqlx::query(
        r#"
        SELECT node_id, state, payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND record_kind = 'scope_instance'
          AND parent_node_id = $3 AND node_kind = 'map_item'
        FOR UPDATE
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.run.run_id)
    .bind(&root_map_node_id)
    .fetch_all(&mut **transaction)
    .await?;
    let expected_item_count = usize::try_from(admitted_item_count).map_err(|_| {
        RepositoryError::InvalidInput("Map item count exceeds platform representation".to_owned())
    })?;
    if rows.len() != expected_item_count {
        return Err(RepositoryError::Conflict("Map settlement Scope set"));
    }
    let mut indexed_outcomes = BTreeMap::new();
    let mut active_scope_ids = Vec::new();
    for row in rows {
        let payload =
            payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
        let stored: StoredControllerScopePayload =
            decode_typed_payload(&payload, "Map settlement Scope")?;
        let StoredControllerScopeDescriptor::MapItem {
            failure_policy: stored_policy,
            item_count: stored_count,
            item_index: stored_index,
            map_plan_node_key: stored_map,
            next_plan_node_key: stored_next,
        } = stored.descriptor
        else {
            return Err(RepositoryError::Conflict("Map settlement Scope kind"));
        };
        if stored.controller_node_execution_id != *root_map_node_execution_id
            || stored_policy != *failure_policy
            || stored_count != *item_count
            || stored_map != *map_plan_node_key
            || stored_next != *next_plan_node_key
            || stored_index >= admitted_item_count
        {
            return Err(RepositoryError::Conflict("Map settlement Scope descriptor"));
        }
        let outcome = match row
            .try_get::<String, _>("state")?
            .parse::<ScopeState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?
        {
            ScopeState::Open => ChildOutcome::Active,
            ScopeState::Succeeded => ChildOutcome::Succeeded,
            ScopeState::Failed => ChildOutcome::Failed,
            ScopeState::Cancelled => ChildOutcome::Cancelled,
            ScopeState::Closing => {
                return Err(RepositoryError::Conflict(
                    "Map settlement observed closing Scope",
                ));
            }
        };
        if outcome == ChildOutcome::Active {
            active_scope_ids.push(row.try_get("node_id")?);
        }
        if indexed_outcomes.insert(stored_index, outcome).is_some() {
            return Err(RepositoryError::Conflict(
                "duplicate Map settlement item index",
            ));
        }
    }
    let children = (0..admitted_item_count)
        .map(|index| {
            indexed_outcomes.get(&index).copied().ok_or_else(|| {
                RepositoryError::CorruptRow("Map settlement lost item index".to_owned())
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let decision = decide_controller(map_node, &ControllerObservation::MapSettlement { children })?;
    match decision {
        ControllerDecision::WaitForChildren => {
            if !mutations.remainder_cancellations.is_empty() {
                return Err(RepositoryError::InvalidInput(
                    "waiting Map cannot accept remainder-cancellation slots".to_owned(),
                ));
            }
            Ok(ParallelLegSettlement {
                settled_scopes,
                woken_nodes: Vec::new(),
                cancelled_remainders: Vec::new(),
                cancelled_active_work_count: 0,
            })
        }
        decision @ (ControllerDecision::CompleteNode { .. }
        | ControllerDecision::FailNode { .. }) => {
            let cancel_active = matches!(decision, ControllerDecision::FailNode { .. })
                && !active_scope_ids.is_empty();
            let (cancelled_remainders, cancelled_active_work_count) = if cancel_active {
                cancel_structured_remainders(
                    transaction,
                    current_job,
                    &parents.run.run_id,
                    &root_map_node_id,
                    "map_item",
                    "map_failure_sibling_cancelled",
                    &active_scope_ids,
                    &mutations.remainder_cancellations,
                    request_digest,
                    database_now,
                )
                .await?
            } else {
                if !mutations.remainder_cancellations.is_empty() {
                    return Err(RepositoryError::InvalidInput(
                        "Map supplied unexpected remainder-cancellation slots".to_owned(),
                    ));
                }
                (Vec::new(), 0)
            };
            let pending_wake = mutations.pending_wake.as_ref().ok_or_else(|| {
                RepositoryError::InvalidInput("Map wait is missing its wake slot".to_owned())
            })?;
            if admission_barrier && matches!(decision, ControllerDecision::FailNode { .. }) {
                stored_pending.wait = StoredControllerWait::MapSettlement {
                    failure_policy: *failure_policy,
                    item_count: *item_count,
                    admitted_item_count,
                    next_plan_node_key: next_plan_node_key.clone(),
                };
            }
            let woken_payload = TypedPayload::with_limit(1, &stored_pending, 262_144)?;
            let node_version: i64 = sqlx::query_scalar(
                r#"
                UPDATE insight_platform.run_nodes
                SET state = 'ready', version = version + 1, enqueue_round = 0,
                    payload_schema_version = $5, payload = $6, payload_digest = $7,
                    updated_at = $4
                WHERE tenant_id = $1 AND node_id = $2 AND version = $3
                  AND record_kind = 'node_execution' AND state = 'pending'
                  AND terminal_at IS NULL
                RETURNING version
                "#,
            )
            .bind(&current_job.tenant_id)
            .bind(wait_node_execution_id.to_string())
            .bind(wait_row.try_get::<i64, _>("version")?)
            .bind(database_now)
            .bind(woken_payload.schema_version)
            .bind(&woken_payload.value)
            .bind(&woken_payload.digest)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(RepositoryError::Conflict("Map wait wake"))?;
            let current_payload: OrchestrationJobPayload =
                decode_orchestration_job_payload(&current_job.payload)?;
            let job_payload = OrchestrationJobPayload {
                external_leaf_completion: None,
                bindings_digest: parents.run.bindings.canonical_digest.clone(),
                node_execution_id: wait_node_execution_id.clone(),
                root_scope_id: current_payload.root_scope_id,
                retry_backoff_milliseconds: current_payload.retry_backoff_milliseconds,
                wake_contract: None,
                convergence_failure: None,
                model_tool_continuation: None,
            };
            job_payload
                .validate()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            let job_payload = job_payload.to_payload()?;
            let job = sqlx::query(
                r#"
                INSERT INTO insight_platform.jobs (
                tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, trace_id, run_id, node_id, state, attempt_limit, scheduled_at, deadline, priority, request_digest, payload_schema_version, payload, payload_digest, scheduler_partition_id, execution_requirement_version, execution_requirement, execution_requirement_digest
            ) VALUES (
                $1, $2, 'orchestration_node', 'orchestration', 'node_execution', $3, (SELECT trace_id FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $4), $4, $3, 'ready', $5, $6, $7, $8, $9, $10, $11, $12, (SELECT scheduler_partition_id FROM insight_platform.tenants WHERE tenant_id=$1), (SELECT execution_requirement_version FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement_digest FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4)
            ) RETURNING *
                "#,
            )
            .bind(&current_job.tenant_id)
            .bind(pending_wake.orchestration_job_id.to_string())
            .bind(wait_node_execution_id.to_string())
            .bind(&parents.run.run_id)
            .bind(current_job.attempt_limit)
            .bind(database_now)
            .bind(current_job.deadline.min(parents.run.deadline))
            .bind(scheduler_priority_to_database(current_job.priority))
            .bind(pending_wake.request_digest.to_string())
            .bind(job_payload.schema_version)
            .bind(&job_payload.value)
            .bind(&job_payload.digest)
            .fetch_one(&mut **transaction)
            .await?;
            Ok(ParallelLegSettlement {
                settled_scopes,
                woken_nodes: vec![ControllerActivationRecord {
                    node_id: wait_node_execution_id.to_string(),
                    plan_node_key: map_plan_node_key.clone(),
                    node_kind: PlanNodeKind::Map,
                    node_version,
                    job: job_from_row(job)?,
                }],
                cancelled_remainders,
                cancelled_active_work_count,
            })
        }
        _ => Err(RepositoryError::Conflict("Map settlement decision")),
    }
}

#[allow(clippy::too_many_arguments)]
async fn settle_parallel_leg_and_maybe_wake_join(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    parents: &LockedOrchestrationJobParents,
    shape: &ControllerStepShape,
    mutations: &ControllerStepMutationIds,
    plan: &RuntimePlan,
    source_outcome: ChildOutcome,
    source_scope_state: ScopeState,
    request_digest: &Sha256Digest,
    database_now: DateTime<Utc>,
) -> Result<ParallelLegSettlement, RepositoryError> {
    if !matches!(
        (source_outcome, source_scope_state),
        (ChildOutcome::Succeeded, ScopeState::Succeeded)
            | (ChildOutcome::Failed, ScopeState::Failed)
    ) {
        return Err(RepositoryError::InvalidInput(
            "ParallelLeg source outcome and Scope state do not match".to_owned(),
        ));
    }
    let ControllerStepShape::ParallelLegExit {
        join_node_execution_id,
        join_plan_node_key,
        expected_scope_ids,
        policy,
        quorum,
        remainder,
    } = shape
    else {
        return Ok(ParallelLegSettlement::empty());
    };
    let structural_exit = mutations.structural_exit.as_ref().ok_or_else(|| {
        RepositoryError::InvalidInput("ParallelLeg exit is missing its mutation slot".to_owned())
    })?;
    let _ = structural_exit;
    let pending_wake = mutations.pending_wake.as_ref().ok_or_else(|| {
        RepositoryError::InvalidInput("Join wake is missing its mutation slot".to_owned())
    })?;
    let expected_ids = expected_scope_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let unique_ids = expected_ids.iter().cloned().collect::<BTreeSet<_>>();
    if expected_ids.is_empty()
        || unique_ids.len() != expected_ids.len()
        || !unique_ids.contains(&parents.scope_id)
    {
        return Err(RepositoryError::CorruptRow(
            "Fork Join expected Scope set is invalid".to_owned(),
        ));
    }
    let rows = sqlx::query(
        r#"
        SELECT node_id, parent_node_id, node_kind, state, version,
               payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2
          AND node_id = ANY($3::text[]) AND record_kind = 'scope_instance'
        ORDER BY node_id
        FOR UPDATE
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.run.run_id)
    .bind(&expected_ids)
    .fetch_all(&mut **transaction)
    .await?;
    if rows.len() != expected_ids.len() {
        return Err(RepositoryError::Conflict("Fork Join Scope closure"));
    }
    let controller_node_id = rows
        .first()
        .and_then(|row| {
            row.try_get::<Option<String>, _>("parent_node_id")
                .ok()
                .flatten()
        })
        .ok_or_else(|| {
            RepositoryError::CorruptRow("ParallelLeg Scope has no controller owner".to_owned())
        })?;
    let mut outcomes = BTreeMap::new();
    let mut active_count = 0_usize;
    for row in rows {
        let scope_id: String = row.try_get("node_id")?;
        let parent_node_id: Option<String> = row.try_get("parent_node_id")?;
        let state: String = row.try_get("state")?;
        let version: i64 = row.try_get("version")?;
        if parent_node_id.as_deref() != Some(controller_node_id.as_str())
            || row.try_get::<String, _>("node_kind")? != "parallel_leg"
        {
            return Err(RepositoryError::Conflict("Fork Join ParallelLeg ownership"));
        }
        let payload =
            payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
        let stored: StoredControllerScopePayload =
            decode_typed_payload(&payload, "Fork Join ParallelLeg Scope")?;
        let StoredControllerScopeDescriptor::ParallelLeg {
            join_plan_node_key: stored_join,
            ..
        } = stored.descriptor
        else {
            return Err(RepositoryError::Conflict(
                "Fork Join ParallelLeg descriptor kind",
            ));
        };
        if stored.controller_node_execution_id.to_string() != controller_node_id
            || stored_join != *join_plan_node_key
        {
            return Err(RepositoryError::Conflict(
                "Fork Join ParallelLeg descriptor",
            ));
        }
        let outcome = if scope_id == parents.scope_id {
            if state != ScopeState::Open.as_str() || version != parents.scope_version {
                return Err(RepositoryError::Conflict(
                    "current ParallelLeg Scope closure",
                ));
            }
            source_outcome
        } else {
            match state
                .parse::<ScopeState>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?
            {
                ScopeState::Open => {
                    active_count = active_count.saturating_add(1);
                    ChildOutcome::Active
                }
                ScopeState::Succeeded => ChildOutcome::Succeeded,
                ScopeState::Failed => ChildOutcome::Failed,
                ScopeState::Cancelled => ChildOutcome::Cancelled,
                ScopeState::Closing => {
                    return Err(RepositoryError::Conflict(
                        "Fork Join Scope is still closing",
                    ));
                }
            }
        };
        outcomes.insert(scope_id, outcome);
    }
    if outcomes.len() != expected_ids.len() {
        return Err(RepositoryError::Conflict("Fork Join Scope closure"));
    }
    let active_scope_ids = expected_ids
        .iter()
        .filter(|scope_id| outcomes.get(*scope_id) == Some(&ChildOutcome::Active))
        .cloned()
        .collect::<Vec<_>>();
    let closing_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'closing', version = version + 1, updated_at = $4
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'scope_instance' AND state = 'open' AND terminal_at IS NULL
        RETURNING version
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.scope_id)
    .bind(parents.scope_version)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("ParallelLeg Scope closing"))?;
    let scope_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = $4, version = version + 1,
            terminal_at = $5, updated_at = $5
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'scope_instance' AND state = 'closing'
        RETURNING version
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.scope_id)
    .bind(closing_version)
    .bind(source_scope_state.as_str())
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("ParallelLeg Scope terminal"))?;
    let settled_scopes = vec![ControllerScopeRecord {
        scope_id: parents.scope_id.clone(),
        scope_kind: "parallel_leg".to_owned(),
        state: source_scope_state.as_str().to_owned(),
        version: scope_version,
    }];

    let children = expected_ids
        .iter()
        .map(|scope_id| {
            outcomes.get(scope_id).copied().ok_or_else(|| {
                RepositoryError::CorruptRow("Fork Join lost a child outcome".to_owned())
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let join_node = plan.node(join_plan_node_key)?;
    let insight_platform_plan::RuntimeNode::Join {
        policy: plan_policy,
        quorum: plan_quorum,
        remainder: plan_remainder,
        ..
    } = join_node
    else {
        return Err(RepositoryError::Conflict("Fork Join Plan node"));
    };
    if plan_policy != policy || plan_quorum != quorum || plan_remainder != remainder {
        return Err(RepositoryError::Conflict("Fork Join frozen policy"));
    }
    let decision = decide_controller(join_node, &ControllerObservation::Join { children })?;
    let join_row = sqlx::query(
        r#"
        SELECT parent_node_id, plan_node_key, node_kind, state, version,
               payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
          AND record_kind = 'node_execution'
        FOR UPDATE
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.run.run_id)
    .bind(join_node_execution_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Fork Join pending Node"))?;
    if join_row
        .try_get::<Option<String>, _>("parent_node_id")?
        .as_deref()
        != Some(controller_node_id.as_str())
        || join_row.try_get::<String, _>("plan_node_key")? != join_plan_node_key.as_str()
        || join_row.try_get::<String, _>("node_kind")? != PlanNodeKind::Join.as_str()
    {
        return Err(RepositoryError::Conflict("Fork Join pending Node owner"));
    }
    let join_payload = payload_from_row(
        &join_row,
        "payload_schema_version",
        "payload",
        "payload_digest",
    )?;
    let pending: StoredPendingControllerNodePayload =
        decode_typed_payload(&join_payload, "Fork Join pending Node")?;
    if pending.controller_node_execution_id.to_string() != controller_node_id
        || pending.plan_node_key != *join_plan_node_key
        || pending.expected_scope_ids != *expected_scope_ids
    {
        return Err(RepositoryError::Conflict("Fork Join pending contract"));
    }
    let join_state: String = join_row.try_get("state")?;
    if join_state != NodeExecutionState::Pending.as_str() {
        if *policy == JoinPolicy::Quorum
            && *remainder == Some(JoinRemainderPolicy::Drain)
            && matches!(join_state.as_str(), "ready" | "running" | "succeeded")
        {
            if !mutations.remainder_cancellations.is_empty() {
                return Err(RepositoryError::InvalidInput(
                    "Drain Join cannot accept remainder-cancellation slots".to_owned(),
                ));
            }
            return Ok(ParallelLegSettlement {
                settled_scopes,
                woken_nodes: Vec::new(),
                cancelled_remainders: Vec::new(),
                cancelled_active_work_count: 0,
            });
        }
        return Err(RepositoryError::Conflict("Fork Join pending state"));
    }
    match decision {
        ControllerDecision::WaitForChildren => {
            if !mutations.remainder_cancellations.is_empty() {
                return Err(RepositoryError::InvalidInput(
                    "waiting Join cannot accept remainder-cancellation slots".to_owned(),
                ));
            }
            Ok(ParallelLegSettlement {
                settled_scopes,
                woken_nodes: Vec::new(),
                cancelled_remainders: Vec::new(),
                cancelled_active_work_count: 0,
            })
        }
        decision @ (ControllerDecision::CompleteNode { .. }
        | ControllerDecision::FailNode { .. }) => {
            if matches!(
                &decision,
                ControllerDecision::CompleteNode { activate } if activate.len() != 1
            ) {
                return Err(RepositoryError::Conflict("Fork Join completion activation"));
            }
            let cancel_active = match &decision {
                ControllerDecision::CompleteNode { .. } => {
                    active_count > 0 && *remainder == Some(JoinRemainderPolicy::Cancel)
                }
                ControllerDecision::FailNode { .. } => active_count > 0,
                _ => false,
            };
            let (cancelled_remainders, cancelled_active_work_count) = if cancel_active {
                cancel_structured_remainders(
                    transaction,
                    current_job,
                    &parents.run.run_id,
                    &controller_node_id,
                    "parallel_leg",
                    match decision {
                        ControllerDecision::CompleteNode { .. } => "join_remainder_cancelled",
                        ControllerDecision::FailNode { .. } => "join_failure_sibling_cancelled",
                        _ => unreachable!("terminal Join decision was matched above"),
                    },
                    &active_scope_ids,
                    &mutations.remainder_cancellations,
                    request_digest,
                    database_now,
                )
                .await?
            } else {
                if !mutations.remainder_cancellations.is_empty() {
                    return Err(RepositoryError::InvalidInput(
                        "controller supplied unexpected remainder-cancellation slots".to_owned(),
                    ));
                }
                (Vec::new(), 0)
            };
            let node_version: i64 = sqlx::query_scalar(
                r#"
                UPDATE insight_platform.run_nodes
                SET state = 'ready', version = version + 1, enqueue_round = 0,
                    updated_at = $4
                WHERE tenant_id = $1 AND node_id = $2 AND version = $3
                  AND record_kind = 'node_execution' AND state = 'pending'
                  AND terminal_at IS NULL
                RETURNING version
                "#,
            )
            .bind(&current_job.tenant_id)
            .bind(join_node_execution_id.to_string())
            .bind(join_row.try_get::<i64, _>("version")?)
            .bind(database_now)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(RepositoryError::Conflict("Fork Join wake"))?;
            let current_payload: OrchestrationJobPayload =
                decode_orchestration_job_payload(&current_job.payload)?;
            let job_payload = OrchestrationJobPayload {
                external_leaf_completion: None,
                bindings_digest: parents.run.bindings.canonical_digest.clone(),
                node_execution_id: join_node_execution_id.clone(),
                root_scope_id: current_payload.root_scope_id,
                retry_backoff_milliseconds: current_payload.retry_backoff_milliseconds,
                wake_contract: None,
                convergence_failure: None,
                model_tool_continuation: None,
            };
            job_payload
                .validate()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            let job_payload = job_payload.to_payload()?;
            let job = sqlx::query(
                r#"
                INSERT INTO insight_platform.jobs (
                tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, trace_id, run_id, node_id, state, attempt_limit, scheduled_at, deadline, priority, request_digest, payload_schema_version, payload, payload_digest, scheduler_partition_id, execution_requirement_version, execution_requirement, execution_requirement_digest
            ) VALUES (
                $1, $2, 'orchestration_node', 'orchestration', 'node_execution', $3, (SELECT trace_id FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $4), $4, $3, 'ready', $5, $6, $7, $8, $9, $10, $11, $12, (SELECT scheduler_partition_id FROM insight_platform.tenants WHERE tenant_id=$1), (SELECT execution_requirement_version FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement_digest FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4)
            ) RETURNING *
                "#,
            )
            .bind(&current_job.tenant_id)
            .bind(pending_wake.orchestration_job_id.to_string())
            .bind(join_node_execution_id.to_string())
            .bind(&parents.run.run_id)
            .bind(current_job.attempt_limit)
            .bind(database_now)
            .bind(current_job.deadline.min(parents.run.deadline))
            .bind(scheduler_priority_to_database(current_job.priority))
            .bind(pending_wake.request_digest.to_string())
            .bind(job_payload.schema_version)
            .bind(&job_payload.value)
            .bind(&job_payload.digest)
            .fetch_one(&mut **transaction)
            .await?;
            Ok(ParallelLegSettlement {
                settled_scopes,
                woken_nodes: vec![ControllerActivationRecord {
                    node_id: join_node_execution_id.to_string(),
                    plan_node_key: join_plan_node_key.clone(),
                    node_kind: PlanNodeKind::Join,
                    node_version,
                    job: job_from_row(job)?,
                }],
                cancelled_remainders,
                cancelled_active_work_count,
            })
        }
        _ => Err(RepositoryError::Conflict("Fork Join controller decision")),
    }
}

#[allow(clippy::too_many_arguments)]
async fn mutate_failed_orchestration_job(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    next_job: &JobProjection,
    parents: &LockedOrchestrationJobParents,
    source_node: &ControllerSourceNode,
    structured_exit: Option<&ControllerStepShape>,
    error_route: Option<&ErrorBoundaryRoute>,
    mutations: &ControllerStepMutationIds,
    plan: &RuntimePlan,
    failure: Failure,
    controller_code: Option<String>,
    failure_payload: &TypedPayload,
    request_digest: &Sha256Digest,
    database_now: DateTime<Utc>,
) -> Result<FailedOrchestrationJob, RepositoryError> {
    let job = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'failed', version = $4, result_digest = $5,
            worker_id = NULL, lease_token_digest = NULL,
            lease_expires_at = NULL, heartbeat_at = NULL,
            terminal_at = $6, updated_at = $6
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND state = 'running' AND terminal_at IS NULL
        RETURNING *
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&current_job.job_id)
    .bind(current_job.version)
    .bind(i64::try_from(next_job.version).map_err(|_| {
        RepositoryError::InvalidInput("failed Job version exceeds bigint".to_owned())
    })?)
    .bind(&failure_payload.digest)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("failed orchestration Job"))?;
    let source_job = job_from_row(job)?;
    let source_node_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'failed', version = version + 1,
            terminal_at = $4, updated_at = $4
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'node_execution' AND state = 'running'
          AND terminal_at IS NULL
        RETURNING version
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.node_id)
    .bind(source_node.version)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("failed orchestration Node"))?;

    if let Some(route) = error_route {
        let slot = mutations.activations.first().ok_or_else(|| {
            RepositoryError::InvalidInput(
                "ErrorBoundary failure is missing its handler activation".to_owned(),
            )
        })?;
        let handler = activate_error_boundary_handler(
            transaction,
            current_job,
            parents,
            route,
            slot,
            plan,
            failure_payload,
            database_now,
        )
        .await?;
        let run = sqlx::query(
            r#"
            UPDATE insight_platform.runs
            SET version = version + 1, active_work_count = active_work_count - 1,
                updated_at = $4
            WHERE tenant_id = $1 AND run_id = $2 AND version = $3
              AND state = 'running' AND active_work_count > 0
              AND terminal_at IS NULL
            RETURNING *
            "#,
        )
        .bind(&parents.run.tenant_id)
        .bind(&parents.run.run_id)
        .bind(parents.run.version)
        .bind(database_now)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("ErrorBoundary Run advance"))?;
        return Ok(FailedOrchestrationJob {
            run: run_from_row(run)?,
            source_node_id: parents.node_id.clone(),
            source_node_version,
            source_job,
            failure,
            controller_code,
            handler_activations: vec![handler],
            settled_scopes: Vec::new(),
            woken_nodes: Vec::new(),
            cancelled_remainders: Vec::new(),
            settled_quota_account_ids: Vec::new(),
        });
    }

    let (run, settled_scopes, woken_nodes, cancelled_remainders) =
        if let Some(shape) = structured_exit {
            let structural = match shape {
                ControllerStepShape::ParallelLegExit { .. } => {
                    settle_parallel_leg_and_maybe_wake_join(
                        transaction,
                        current_job,
                        parents,
                        shape,
                        mutations,
                        plan,
                        ChildOutcome::Failed,
                        ScopeState::Failed,
                        request_digest,
                        database_now,
                    )
                    .await?
                }
                ControllerStepShape::MapItemExit { .. } => {
                    settle_map_item_and_maybe_wake_settlement(
                        transaction,
                        current_job,
                        parents,
                        shape,
                        mutations,
                        plan,
                        ChildOutcome::Failed,
                        ScopeState::Failed,
                        request_digest,
                        database_now,
                    )
                    .await?
                }
                _ => {
                    return Err(RepositoryError::CorruptRow(
                        "failure selected a non-terminal structured adapter".to_owned(),
                    ));
                }
            };
            let active_work_decrement = i32::try_from(
                1_usize
                    .checked_add(structural.cancelled_active_work_count)
                    .ok_or_else(|| {
                        RepositoryError::InvalidInput(
                            "failed orchestration active-work decrement overflow".to_owned(),
                        )
                    })?,
            )
            .map_err(|_| {
                RepositoryError::InvalidInput(
                    "failed orchestration active-work decrement exceeds integer".to_owned(),
                )
            })?;
            let run = sqlx::query(
                r#"
                UPDATE insight_platform.runs
                SET version = version + 1, active_work_count = active_work_count - $4,
                    updated_at = $5
                WHERE tenant_id = $1 AND run_id = $2 AND version = $3
                  AND state = 'running' AND active_work_count >= $4
                  AND terminal_at IS NULL
                RETURNING *
                "#,
            )
            .bind(&parents.run.tenant_id)
            .bind(&parents.run.run_id)
            .bind(parents.run.version)
            .bind(active_work_decrement)
            .bind(database_now)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(RepositoryError::Conflict("failed structured Scope Run"))?;
            (
                run_from_row(run)?,
                structural.settled_scopes,
                structural.woken_nodes,
                structural.cancelled_remainders,
            )
        } else {
            let job_payload: OrchestrationJobPayload =
                decode_orchestration_job_payload(&current_job.payload)?;
            if job_payload.root_scope_id.to_string() != parents.scope_id {
                return Err(RepositoryError::InvalidInput(
                    "nested failure requires a typed Scope propagation adapter".to_owned(),
                ));
            }
            let other_live_nodes: i64 = sqlx::query_scalar(
                r#"
                SELECT count(*) FROM insight_platform.run_nodes
                WHERE tenant_id = $1 AND run_id = $2 AND record_kind = 'node_execution'
                  AND node_id <> $3 AND terminal_at IS NULL
                "#,
            )
            .bind(&current_job.tenant_id)
            .bind(&parents.run.run_id)
            .bind(&parents.node_id)
            .fetch_one(&mut **transaction)
            .await?;
            if other_live_nodes != 0 || parents.run.active_work_count != 1 {
                return Err(RepositoryError::Conflict(
                    "failed root orchestration closure",
                ));
            }
            let closing_version: i64 = sqlx::query_scalar(
                r#"
                UPDATE insight_platform.run_nodes
                SET state = 'closing', version = version + 1, updated_at = $4
                WHERE tenant_id = $1 AND node_id = $2 AND version = $3
                  AND record_kind = 'scope_instance' AND state = 'open'
                  AND terminal_at IS NULL
                RETURNING version
                "#,
            )
            .bind(&current_job.tenant_id)
            .bind(&parents.scope_id)
            .bind(parents.scope_version)
            .bind(database_now)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(RepositoryError::Conflict("failed root Scope closing"))?;
            let scope_version: i64 = sqlx::query_scalar(
                r#"
                UPDATE insight_platform.run_nodes
                SET state = 'failed', version = version + 1,
                    terminal_at = $4, updated_at = $4
                WHERE tenant_id = $1 AND node_id = $2 AND version = $3
                  AND record_kind = 'scope_instance' AND state = 'closing'
                RETURNING version
                "#,
            )
            .bind(&current_job.tenant_id)
            .bind(&parents.scope_id)
            .bind(closing_version)
            .bind(database_now)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(RepositoryError::Conflict("failed root Scope"))?;
            let run_id: ResourceId = parents.run.run_id.parse().map_err(
                |failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                },
            )?;
            let mut current_snapshot = parents.run.current.clone();
            current_snapshot.failure = Some(failure.clone());
            current_snapshot.output_value_id = None;
            current_snapshot.waiting_reason = None;
            current_snapshot
                .validate(&run_id)
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
            let current_payload = TypedPayload::from_versioned(1, &current_snapshot, 1_048_576)?;
            let run = sqlx::query(
                r#"
                UPDATE insight_platform.runs
                SET state = 'failed', version = version + 1, active_work_count = 0,
                    output_value_id = NULL, current_schema_version = $4,
                    current_payload = $5, current_payload_digest = $6,
                    terminal_at = $7, updated_at = $7
                WHERE tenant_id = $1 AND run_id = $2 AND version = $3
                  AND state = 'running' AND active_work_count = 1
                  AND terminal_at IS NULL
                RETURNING *
                "#,
            )
            .bind(&parents.run.tenant_id)
            .bind(&parents.run.run_id)
            .bind(parents.run.version)
            .bind(current_payload.schema_version)
            .bind(&current_payload.value)
            .bind(&current_payload.digest)
            .bind(database_now)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(RepositoryError::Conflict("failed root Run"))?;
            (
                run_from_row(run)?,
                vec![ControllerScopeRecord {
                    scope_id: parents.scope_id.clone(),
                    scope_kind: "root".to_owned(),
                    state: ScopeState::Failed.as_str().to_owned(),
                    version: scope_version,
                }],
                Vec::new(),
                Vec::new(),
            )
        };
    Ok(FailedOrchestrationJob {
        run,
        source_node_id: parents.node_id.clone(),
        source_node_version,
        source_job,
        failure,
        controller_code,
        handler_activations: Vec::new(),
        settled_scopes,
        woken_nodes,
        cancelled_remainders,
        settled_quota_account_ids: Vec::new(),
    })
}

#[allow(clippy::too_many_arguments)]
async fn activate_error_boundary_handler(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    parents: &LockedOrchestrationJobParents,
    route: &ErrorBoundaryRoute,
    slot: &ControllerActivationSlot,
    plan: &RuntimePlan,
    failure_payload: &TypedPayload,
    database_now: DateTime<Utc>,
) -> Result<ControllerActivationRecord, RepositoryError> {
    let target_node = plan.node(&route.target)?;
    let activation_ordinal: i32 = sqlx::query_scalar(
        r#"
        SELECT COALESCE(max(activation_ordinal), 0) + 1
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2
          AND record_kind = 'node_execution' AND plan_node_key = $3
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.run.run_id)
    .bind(route.target.as_str())
    .fetch_one(&mut **transaction)
    .await?;
    let node_payload = TypedPayload::with_limit(
        1,
        &StoredErrorBoundaryHandlerPayload {
            error_boundary_node_id: route.boundary_node_id.parse().map_err(
                |failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                },
            )?,
            failure_digest: failure_payload
                .digest
                .parse::<Sha256Digest>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
            plan_node_key: route.target.clone(),
            plan_source_digest: parents.run.bindings.plan.semantic_digest.clone(),
            required_control_tokens: vec![StoredControllerControlToken {
                source_node_execution_id: parents.node_id.parse().map_err(
                    |failure: insight_platform_contracts::ResourceIdError| {
                        RepositoryError::CorruptRow(failure.to_string())
                    },
                )?,
                source_port: StoredControllerPort::Failure,
            }],
        },
        262_144,
    )?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_nodes (
            tenant_id, node_id, run_id, parent_node_id, record_kind, scope_id,
            plan_node_key, activation_ordinal, related_run_id, logical_key,
            node_kind, state, enqueue_round,
            payload_schema_version, payload, payload_digest, deadline
        ) VALUES (
            $1, $2, $3, $4, 'node_execution', $5,
            $6, $7, NULL, $8, $9, 'ready', 0, $10, $11, $12, $13
        )
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(slot.node_execution_id.to_string())
    .bind(&parents.run.run_id)
    .bind(&route.boundary_parent_node_id)
    .bind(&parents.scope_id)
    .bind(route.target.as_str())
    .bind(activation_ordinal)
    .bind(format!(
        "error-boundary:{}:{}",
        route.boundary_node_id,
        route.target.as_str()
    ))
    .bind(target_node.kind().as_str())
    .bind(node_payload.schema_version)
    .bind(&node_payload.value)
    .bind(&node_payload.digest)
    .bind(current_job.deadline.min(parents.run.deadline))
    .execute(&mut **transaction)
    .await?;
    let current_payload: OrchestrationJobPayload =
        decode_orchestration_job_payload(&current_job.payload)?;
    let job_payload = OrchestrationJobPayload {
        external_leaf_completion: None,
        bindings_digest: parents.run.bindings.canonical_digest.clone(),
        node_execution_id: slot.node_execution_id.clone(),
        root_scope_id: current_payload.root_scope_id,
        retry_backoff_milliseconds: current_payload.retry_backoff_milliseconds,
        wake_contract: None,
        convergence_failure: None,
        model_tool_continuation: None,
    };
    job_payload
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let job_payload = job_payload.to_payload()?;
    let row = sqlx::query(
        r#"
        INSERT INTO insight_platform.jobs (
                tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, trace_id, run_id, node_id, state, attempt_limit, scheduled_at, deadline, priority, request_digest, payload_schema_version, payload, payload_digest, scheduler_partition_id, execution_requirement_version, execution_requirement, execution_requirement_digest
            ) VALUES (
                $1, $2, 'orchestration_node', 'orchestration', 'node_execution', $3, (SELECT trace_id FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $4), $4, $3, 'ready', $5, $6, $7, $8, $9, $10, $11, $12, (SELECT scheduler_partition_id FROM insight_platform.tenants WHERE tenant_id=$1), (SELECT execution_requirement_version FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement_digest FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4)
            ) RETURNING *
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(slot.orchestration_job_id.to_string())
    .bind(slot.node_execution_id.to_string())
    .bind(&parents.run.run_id)
    .bind(current_job.attempt_limit)
    .bind(database_now)
    .bind(current_job.deadline.min(parents.run.deadline))
    .bind(scheduler_priority_to_database(current_job.priority))
    .bind(&job_payload.digest)
    .bind(job_payload.schema_version)
    .bind(&job_payload.value)
    .bind(&job_payload.digest)
    .fetch_one(&mut **transaction)
    .await?;
    Ok(ControllerActivationRecord {
        node_id: slot.node_execution_id.to_string(),
        plan_node_key: route.target.clone(),
        node_kind: target_node.kind(),
        node_version: 1,
        job: job_from_row(row)?,
    })
}

#[allow(clippy::too_many_arguments)]
async fn cancel_structured_remainders(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    run_id: &str,
    controller_node_id: &str,
    expected_scope_kind: &str,
    reason_code: &str,
    active_scope_ids: &[String],
    slots: &[ControllerRemainderCancellationSlot],
    request_digest: &Sha256Digest,
    database_now: DateTime<Utc>,
) -> Result<(Vec<ControllerCancelledRemainderRecord>, usize), RepositoryError> {
    let active_set = active_scope_ids.iter().cloned().collect::<BTreeSet<_>>();
    let slot_set = slots
        .iter()
        .map(|slot| slot.expected_scope_id.to_string())
        .collect::<BTreeSet<_>>();
    if active_set.is_empty()
        || active_set.len() != active_scope_ids.len()
        || slot_set.len() != slots.len()
        || active_set != slot_set
    {
        return Err(RepositoryError::InvalidInput(
            "remainder-cancellation slots do not match the exact active Scope set".to_owned(),
        ));
    }
    let slots_by_scope = slots
        .iter()
        .map(|slot| (slot.expected_scope_id.to_string(), slot))
        .collect::<BTreeMap<_, _>>();
    let mut cancelled = Vec::with_capacity(active_scope_ids.len());
    let mut cancelled_active_work_count = 0_usize;
    for scope_id in active_scope_ids {
        let slot = slots_by_scope.get(scope_id).ok_or_else(|| {
            RepositoryError::CorruptRow("remainder cancellation lost its slot".to_owned())
        })?;
        let scope = sqlx::query(
            r#"
            SELECT node_kind, state, version, parent_node_id
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
              AND record_kind = 'scope_instance'
            FOR UPDATE
            "#,
        )
        .bind(&current_job.tenant_id)
        .bind(run_id)
        .bind(scope_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("structured remainder Scope"))?;
        if scope.try_get::<String, _>("node_kind")? != expected_scope_kind
            || scope.try_get::<String, _>("state")? != ScopeState::Open.as_str()
            || scope
                .try_get::<Option<String>, _>("parent_node_id")?
                .as_deref()
                != Some(controller_node_id)
        {
            return Err(RepositoryError::Conflict(
                "structured remainder Scope owner",
            ));
        }
        let live_nodes = sqlx::query(
            r#"
            SELECT node_id, state, version
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND scope_id = $3
              AND record_kind = 'node_execution' AND terminal_at IS NULL
            ORDER BY node_id
            FOR UPDATE
            "#,
        )
        .bind(&current_job.tenant_id)
        .bind(run_id)
        .bind(scope_id)
        .fetch_all(&mut **transaction)
        .await?;
        if live_nodes.len() != 1 {
            return Err(RepositoryError::Conflict(
                "remainder Scope must own one live Node",
            ));
        }
        let node = &live_nodes[0];
        let node_id: String = node.try_get("node_id")?;
        let node_state = node
            .try_get::<String, _>("state")?
            .parse::<NodeExecutionState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        let job_rows = sqlx::query(
            r#"
            SELECT * FROM insight_platform.jobs
            WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
              AND state IN (
                  'ready', 'leased', 'running', 'waiting', 'retry_scheduled',
                  'cancelling'
              )
            ORDER BY job_id
            FOR UPDATE
            "#,
        )
        .bind(&current_job.tenant_id)
        .bind(run_id)
        .bind(&node_id)
        .fetch_all(&mut **transaction)
        .await?;
        if job_rows.len() != 1 {
            return Err(RepositoryError::Conflict(
                "remainder Node must own one live Job",
            ));
        }
        let job =
            job_from_row(job_rows.into_iter().next().ok_or_else(|| {
                RepositoryError::CorruptRow("remainder Job disappeared".to_owned())
            })?)?;
        require_orchestration_job(&job)?;
        let expected_node_state = orchestration_node_state_for_job_state(&job.state)?;
        if node_state.as_str() != expected_node_state {
            return Err(RepositoryError::Conflict("remainder Job and Node state"));
        }
        let next_job = decide_job_owner_terminal(&job_projection(&job)?, JobState::Cancelled)?;
        let mut settled_quota_account_ids = Vec::new();
        if job.quota_reservation_id.is_some() {
            let (quota_accounts, already_settled) =
                lock_job_quota_bundle_state(transaction, &job).await?;
            if !already_settled {
                settle_locked_job_quota_bundle(
                    transaction,
                    &job,
                    &quota_accounts,
                    &slot.quota_entry_ids,
                    request_digest,
                )
                .await?;
                settled_quota_account_ids = quota_accounts
                    .iter()
                    .map(|account| account.quota_account_id.clone())
                    .collect();
                cancelled_active_work_count =
                    cancelled_active_work_count.checked_add(1).ok_or_else(|| {
                        RepositoryError::InvalidInput(
                            "remainder active-work count overflow".to_owned(),
                        )
                    })?;
            }
        }
        let job_row = sqlx::query(
            r#"
            UPDATE insight_platform.jobs
            SET state = 'cancelled', version = $4, worker_id = NULL,
                lease_token_digest = NULL, lease_expires_at = NULL,
                heartbeat_at = NULL, retry_at = NULL,
                wake_kind = NULL, wake_state = NULL,
                terminal_at = $5, updated_at = $5
            WHERE tenant_id = $1 AND job_id = $2 AND version = $3
              AND state = $6 AND terminal_at IS NULL
            RETURNING *
            "#,
        )
        .bind(&current_job.tenant_id)
        .bind(&job.job_id)
        .bind(job.version)
        .bind(i64::try_from(next_job.version).map_err(|_| {
            RepositoryError::InvalidInput("remainder Job version exceeds bigint".to_owned())
        })?)
        .bind(database_now)
        .bind(&job.state)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("remainder Job cancellation"))?;
        let cancelled_job = job_from_row(job_row)?;

        let current_node_version: i64 = node.try_get("version")?;
        let node_cancelling_version = if node_state == NodeExecutionState::Running {
            Some(
                sqlx::query_scalar(
                    r#"
                    UPDATE insight_platform.run_nodes
                    SET state = 'cancelling', version = version + 1, updated_at = $4
                    WHERE tenant_id = $1 AND node_id = $2 AND version = $3
                      AND record_kind = 'node_execution' AND state = 'running'
                      AND terminal_at IS NULL
                    RETURNING version
                    "#,
                )
                .bind(&current_job.tenant_id)
                .bind(&node_id)
                .bind(current_node_version)
                .bind(database_now)
                .fetch_optional(&mut **transaction)
                .await?
                .ok_or(RepositoryError::Conflict("remainder Node cancelling"))?,
            )
        } else {
            None
        };
        let terminal_from = if node_cancelling_version.is_some() {
            NodeExecutionState::Cancelling
        } else {
            node_state
        };
        if !terminal_from.can_transition_to(NodeExecutionState::Cancelled) {
            return Err(RepositoryError::Conflict(
                "remainder Node cancellation transition",
            ));
        }
        let node_version: i64 = sqlx::query_scalar(
            r#"
            UPDATE insight_platform.run_nodes
            SET state = 'cancelled', version = version + 1,
                terminal_at = $5, updated_at = $5
            WHERE tenant_id = $1 AND node_id = $2 AND version = $3
              AND record_kind = 'node_execution' AND state = $4
              AND terminal_at IS NULL
            RETURNING version
            "#,
        )
        .bind(&current_job.tenant_id)
        .bind(&node_id)
        .bind(node_cancelling_version.unwrap_or(current_node_version))
        .bind(terminal_from.as_str())
        .bind(database_now)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict(
            "remainder Node terminal cancellation",
        ))?;
        let scope_closing_version: i64 = sqlx::query_scalar(
            r#"
            UPDATE insight_platform.run_nodes
            SET state = 'closing', version = version + 1, updated_at = $4
            WHERE tenant_id = $1 AND node_id = $2 AND version = $3
              AND record_kind = 'scope_instance' AND state = 'open'
              AND terminal_at IS NULL
            RETURNING version
            "#,
        )
        .bind(&current_job.tenant_id)
        .bind(scope_id)
        .bind(scope.try_get::<i64, _>("version")?)
        .bind(database_now)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("remainder Scope closing"))?;
        let scope_version: i64 = sqlx::query_scalar(
            r#"
            UPDATE insight_platform.run_nodes
            SET state = 'cancelled', version = version + 1,
                terminal_at = $4, updated_at = $4
            WHERE tenant_id = $1 AND node_id = $2 AND version = $3
              AND record_kind = 'scope_instance' AND state = 'closing'
            RETURNING version
            "#,
        )
        .bind(&current_job.tenant_id)
        .bind(scope_id)
        .bind(scope_closing_version)
        .bind(database_now)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict(
            "remainder Scope terminal cancellation",
        ))?;
        cancelled.push(ControllerCancelledRemainderRecord {
            scope: ControllerScopeRecord {
                scope_id: scope_id.clone(),
                scope_kind: expected_scope_kind.to_owned(),
                state: ScopeState::Cancelled.as_str().to_owned(),
                version: scope_version,
            },
            reason_code: reason_code.to_owned(),
            node_id,
            node_version,
            node_cancelling_version,
            job: cancelled_job,
            settled_quota_account_ids,
        });
    }
    Ok((cancelled, cancelled_active_work_count))
}

#[allow(clippy::too_many_arguments)]
async fn mutate_yielded_orchestration_job(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    next_job: &JobProjection,
    parents: &LockedOrchestrationJobParents,
    wake_contract: Option<WakeContract>,
    wait_due_at: Option<DateTime<Utc>>,
    node_payload: Option<&TypedPayload>,
    database_now: DateTime<Utc>,
) -> Result<YieldedOrchestrationJob, RepositoryError> {
    let target_node_state = match next_job.state {
        JobState::Waiting => NodeExecutionState::Waiting,
        JobState::RetryScheduled => NodeExecutionState::RetryScheduled,
        _ => {
            return Err(RepositoryError::InvalidInput(
                "orchestration yield decision is not waiting or retry-scheduled".to_owned(),
            ))
        }
    };
    if !NodeExecutionState::Running.can_transition_to(target_node_state) {
        return Err(RepositoryError::Conflict(
            "orchestration Node yield transition",
        ));
    }
    let payload =
        orchestration_job_payload_with_wake(current_job, wake_contract.as_ref().cloned())?;
    let (wake_kind, wake_state, wake_generation) = match &wake_contract {
        Some(wake) => (
            Some(wake.kind.as_str()),
            Some("pending"),
            i64::try_from(wake.generation).map_err(|_| {
                RepositoryError::InvalidInput("wake generation exceeds bigint".to_owned())
            })?,
        ),
        None => (None, None, 0),
    };
    let job = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = $4, version = $5, worker_id = NULL, lease_token_digest = NULL,
            lease_expires_at = NULL, heartbeat_at = NULL, retry_at = $6,
            scheduled_at = COALESCE($7, scheduled_at),
            wake_kind = $8, wake_state = $9, wake_generation = $10,
            payload_schema_version = $11, payload = $12, payload_digest = $13,
            started_at = CASE WHEN $8::text IS NULL THEN NULL ELSE started_at END,
            updated_at = $14
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3 AND state = 'running'
        RETURNING *
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&current_job.job_id)
    .bind(current_job.version)
    .bind(next_job.state.as_str())
    .bind(
        i64::try_from(next_job.version)
            .map_err(|_| RepositoryError::InvalidInput("Job version exceeds bigint".to_owned()))?,
    )
    .bind(next_job.retry_at)
    .bind(wait_due_at)
    .bind(wake_kind)
    .bind(wake_state)
    .bind(wake_generation)
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("orchestration Job"))?;
    let job = job_from_row(job)?;

    let node_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = $4, version = version + 1, retry_at = $5,
            payload_schema_version = COALESCE($7, payload_schema_version),
            payload = COALESCE($8, payload), payload_digest = COALESCE($9, payload_digest),
            updated_at = $6
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'node_execution' AND state = 'running'
          AND terminal_at IS NULL
        RETURNING version
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.node_id)
    .bind(parents.node_version)
    .bind(target_node_state.as_str())
    .bind(next_job.retry_at)
    .bind(database_now)
    .bind(node_payload.map(|payload| payload.schema_version))
    .bind(node_payload.map(|payload| &payload.value))
    .bind(node_payload.map(|payload| payload.digest.as_str()))
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("orchestration Node"))?;

    let mut current_snapshot = parents.run.current.clone();
    if parents.run.active_work_count == 1 {
        if let Some(wake) = wake_contract.as_ref() {
            current_snapshot.waiting_reason = Some(wake.kind.as_str().to_owned());
        }
    }
    let run_id: ResourceId = parents.run.run_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    current_snapshot
        .validate(&run_id)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let current_payload = TypedPayload::from_versioned(1, &current_snapshot, 1_048_576)?;
    let run = sqlx::query(
        r#"
        UPDATE insight_platform.runs
        SET state = CASE
                WHEN active_work_count = 1 AND $4 THEN 'waiting'
                ELSE state
            END,
            version = version + 1, active_work_count = active_work_count - 1,
            current_schema_version = $5, current_payload = $6,
            current_payload_digest = $7, updated_at = $8
        WHERE tenant_id = $1 AND run_id = $2 AND version = $3
          AND state = 'running' AND active_work_count > 0 AND terminal_at IS NULL
        RETURNING *
        "#,
    )
    .bind(&parents.run.tenant_id)
    .bind(&parents.run.run_id)
    .bind(parents.run.version)
    .bind(wake_contract.is_some())
    .bind(current_payload.schema_version)
    .bind(&current_payload.value)
    .bind(&current_payload.digest)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("orchestration Run"))?;

    Ok(YieldedOrchestrationJob {
        run: run_from_row(run)?,
        job,
        node_id: parents.node_id.clone(),
        node_version,
        settled_quota_account_ids: Vec::new(),
    })
}

#[allow(clippy::too_many_arguments)]
async fn mutate_deferred_orchestration_task(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    next_job: &JobProjection,
    parents: &LockedOrchestrationJobParents,
    source_node: &ControllerSourceNode,
    command: &DeferOrchestrationToTask,
    task_payload: &TypedPayload,
    database_now: DateTime<Utc>,
) -> Result<DeferredOrchestrationTask, RepositoryError> {
    if !NodeExecutionState::Running.can_transition_to(NodeExecutionState::Waiting) {
        return Err(RepositoryError::Conflict(
            "orchestration Task wait transition",
        ));
    }
    let RuntimeNode::HumanTask { response, .. } = command.plan.node(&source_node.plan_node_key)?
    else {
        return Err(RepositoryError::Conflict("orchestration Task Plan node"));
    };
    let node_payload = TypedPayload::with_limit(
        1,
        &StoredHumanTaskWaitPayload {
            plan_node_key: source_node.plan_node_key.clone(),
            task_id: command.task_id.clone(),
            response_port: response.clone(),
            resolution: None,
        },
        262_144,
    )?;
    let job = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'succeeded', version = $4, result_digest = $5,
            worker_id = NULL, lease_token_digest = NULL,
            lease_expires_at = NULL, heartbeat_at = NULL,
            terminal_at = $6, updated_at = $6
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3 AND state = 'running'
        RETURNING *
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&current_job.job_id)
    .bind(current_job.version)
    .bind(
        i64::try_from(next_job.version)
            .map_err(|_| RepositoryError::InvalidInput("Job version exceeds bigint".to_owned()))?,
    )
    .bind(&task_payload.digest)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("orchestration Task deferral Job"))?;
    let job = job_from_row(job)?;
    let task = sqlx::query(
        r#"
        INSERT INTO insight_platform.tasks (
            tenant_id, task_id, task_kind, owner_kind, owner_id, run_id, node_id,
            invocation_id, state, generation, version, response_schema_digest,
            principal_snapshot_schema_version, payload_schema_version, payload, payload_digest,
            response_value_id, deadline, responded_at, trace_id
        ) VALUES (
            $1, $2, $3, 'node_execution', $4, $5, $4,
            NULL, 'pending', 1, 1, $6,
            1, $7, $8, $9, NULL, $10, NULL, $11
        )
        RETURNING *
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(command.task_id.to_string())
    .bind(command.definition.task_kind().as_str())
    .bind(&parents.node_id)
    .bind(&parents.run.run_id)
    .bind(
        command
            .response_schema_digest
            .as_ref()
            .map(ToString::to_string),
    )
    .bind(task_payload.schema_version)
    .bind(&task_payload.value)
    .bind(&task_payload.digest)
    .bind(command.task_deadline)
    .bind(current_job.trace.trace_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    let task = task_from_row(task)?;
    let node_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'waiting', version = version + 1,
            payload_schema_version = $5, payload = $6, payload_digest = $7,
            updated_at = $4
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'node_execution' AND state = 'running'
          AND terminal_at IS NULL
        RETURNING version
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.node_id)
    .bind(parents.node_version)
    .bind(database_now)
    .bind(node_payload.schema_version)
    .bind(&node_payload.value)
    .bind(&node_payload.digest)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "orchestration Task deferral Node",
    ))?;
    let mut current_snapshot = parents.run.current.clone();
    if parents.run.active_work_count == 1 {
        current_snapshot.waiting_reason = Some(command.definition.task_kind().as_str().to_owned());
    }
    let run_id: ResourceId = parents.run.run_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    current_snapshot
        .validate(&run_id)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let current_payload = TypedPayload::from_versioned(1, &current_snapshot, 1_048_576)?;
    let run = sqlx::query(
        r#"
        UPDATE insight_platform.runs
        SET state = CASE WHEN active_work_count = 1 THEN 'waiting' ELSE state END,
            version = version + 1, active_work_count = active_work_count - 1,
            current_schema_version = $4, current_payload = $5,
            current_payload_digest = $6, updated_at = $7
        WHERE tenant_id = $1 AND run_id = $2 AND version = $3
          AND state = 'running' AND active_work_count > 0 AND terminal_at IS NULL
        RETURNING *
        "#,
    )
    .bind(&parents.run.tenant_id)
    .bind(&parents.run.run_id)
    .bind(parents.run.version)
    .bind(current_payload.schema_version)
    .bind(&current_payload.value)
    .bind(&current_payload.digest)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("orchestration Task deferral Run"))?;
    Ok(DeferredOrchestrationTask {
        run: run_from_row(run)?,
        node_id: parents.node_id.clone(),
        node_version,
        job,
        task,
        settled_quota_account_ids: Vec::new(),
    })
}

#[allow(clippy::too_many_arguments)]
async fn mutate_deferred_orchestration_context_query(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    next_job: &JobProjection,
    parents: &LockedOrchestrationJobParents,
    query: &insight_platform_context::ContextQueryRecord,
    context_job: &JobRecord,
    node_payload: &TypedPayload,
    database_now: DateTime<Utc>,
) -> Result<DeferredOrchestrationContextQuery, RepositoryError> {
    if !NodeExecutionState::Running.can_transition_to(NodeExecutionState::Waiting) {
        return Err(RepositoryError::Conflict(
            "orchestration Context wait transition",
        ));
    }
    let source_job = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'succeeded', version = $4, result_digest = $5,
            worker_id = NULL, lease_token_digest = NULL,
            lease_expires_at = NULL, heartbeat_at = NULL,
            terminal_at = $6, updated_at = $6
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3 AND state = 'running'
        RETURNING *
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&current_job.job_id)
    .bind(current_job.version)
    .bind(
        i64::try_from(next_job.version)
            .map_err(|_| RepositoryError::InvalidInput("Job version exceeds bigint".to_owned()))?,
    )
    .bind(&node_payload.digest)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "orchestration Context deferral Job",
    ))?;
    let source_job = job_from_row(source_job)?;
    let node_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'waiting', version = version + 1,
            payload_schema_version = $5, payload = $6, payload_digest = $7,
            updated_at = $4
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'node_execution' AND state = 'running'
          AND terminal_at IS NULL
        RETURNING version
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.node_id)
    .bind(parents.node_version)
    .bind(database_now)
    .bind(node_payload.schema_version)
    .bind(&node_payload.value)
    .bind(&node_payload.digest)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "orchestration Context deferral Node",
    ))?;
    let mut current_snapshot = parents.run.current.clone();
    if parents.run.active_work_count == 1 {
        current_snapshot.waiting_reason = Some("context_query".to_owned());
    }
    let run_id: ResourceId = parents.run.run_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    current_snapshot
        .validate(&run_id)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let current_payload = TypedPayload::from_versioned(1, &current_snapshot, 1_048_576)?;
    let run = sqlx::query(
        r#"
        UPDATE insight_platform.runs
        SET state = CASE WHEN active_work_count = 1 THEN 'waiting' ELSE state END,
            version = version + 1, active_work_count = active_work_count - 1,
            current_schema_version = $4, current_payload = $5,
            current_payload_digest = $6, updated_at = $7
        WHERE tenant_id = $1 AND run_id = $2 AND version = $3
          AND state = 'running' AND active_work_count > 0 AND terminal_at IS NULL
        RETURNING *
        "#,
    )
    .bind(&parents.run.tenant_id)
    .bind(&parents.run.run_id)
    .bind(parents.run.version)
    .bind(current_payload.schema_version)
    .bind(&current_payload.value)
    .bind(&current_payload.digest)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "orchestration Context deferral Run",
    ))?;
    Ok(DeferredOrchestrationContextQuery {
        run: run_from_row(run)?,
        node_id: parents.node_id.clone(),
        node_version,
        source_job,
        query: query.clone(),
        context_job: context_job.clone(),
        settled_quota_account_ids: Vec::new(),
    })
}

#[allow(clippy::too_many_arguments)]
async fn mutate_deferred_orchestration_model_turn(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    next_job: &JobProjection,
    parents: &LockedOrchestrationJobParents,
    prepared: &crate::model_turn_repository::PreparedModelExecution,
    node_payload: &TypedPayload,
    database_now: DateTime<Utc>,
) -> Result<DeferredOrchestrationModelTurn, RepositoryError> {
    if !NodeExecutionState::Running.can_transition_to(NodeExecutionState::Waiting) {
        return Err(RepositoryError::Conflict(
            "orchestration Model wait transition",
        ));
    }
    let source_job = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'succeeded', version = $4, result_digest = $5,
            worker_id = NULL, lease_token_digest = NULL,
            lease_expires_at = NULL, heartbeat_at = NULL,
            terminal_at = $6, updated_at = $6
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3 AND state = 'running'
        RETURNING *
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&current_job.job_id)
    .bind(current_job.version)
    .bind(
        i64::try_from(next_job.version)
            .map_err(|_| RepositoryError::InvalidInput("Job version exceeds bigint".to_owned()))?,
    )
    .bind(&node_payload.digest)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "orchestration Model deferral Job",
    ))?;
    let source_job = job_from_row(source_job)?;
    let node_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'waiting', version = version + 1,
            payload_schema_version = $5, payload = $6, payload_digest = $7,
            updated_at = $4
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'node_execution' AND state = 'running'
          AND terminal_at IS NULL
        RETURNING version
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.node_id)
    .bind(parents.node_version)
    .bind(database_now)
    .bind(node_payload.schema_version)
    .bind(&node_payload.value)
    .bind(&node_payload.digest)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "orchestration Model deferral Node",
    ))?;
    let mut current_snapshot = parents.run.current.clone();
    if parents.run.active_work_count == 1 {
        current_snapshot.waiting_reason = Some("model_turn".to_owned());
    }
    let run_id: ResourceId = parents.run.run_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    current_snapshot
        .validate(&run_id)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let current_payload = TypedPayload::from_versioned(1, &current_snapshot, 1_048_576)?;
    let run = sqlx::query(
        r#"
        UPDATE insight_platform.runs
        SET state = CASE WHEN active_work_count = 1 THEN 'waiting' ELSE state END,
            version = version + 1, active_work_count = active_work_count - 1,
            current_schema_version = $4, current_payload = $5,
            current_payload_digest = $6, updated_at = $7
        WHERE tenant_id = $1 AND run_id = $2 AND version = $3
          AND state = 'running' AND active_work_count > 0 AND terminal_at IS NULL
        RETURNING *
        "#,
    )
    .bind(&parents.run.tenant_id)
    .bind(&parents.run.run_id)
    .bind(parents.run.version)
    .bind(current_payload.schema_version)
    .bind(&current_payload.value)
    .bind(&current_payload.digest)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "orchestration Model deferral Run",
    ))?;
    Ok(DeferredOrchestrationModelTurn {
        run: run_from_row(run)?,
        node_id: parents.node_id.clone(),
        node_version,
        source_job,
        turn: prepared.turn.clone(),
        model_job: prepared.job.clone(),
        settled_quota_account_ids: Vec::new(),
    })
}

#[allow(clippy::too_many_arguments)]
async fn mutate_deferred_orchestration_capability_invocation(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    next_job: &JobProjection,
    parents: &LockedOrchestrationJobParents,
    invocation: &CapabilityInvocationRecord,
    capability_job: Option<&JobRecord>,
    node_payload: &TypedPayload,
    database_now: DateTime<Utc>,
) -> Result<DeferredOrchestrationCapabilityInvocation, RepositoryError> {
    if !NodeExecutionState::Running.can_transition_to(NodeExecutionState::Waiting) {
        return Err(RepositoryError::Conflict(
            "orchestration Capability wait transition",
        ));
    }
    let source_job = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'succeeded', version = $4, result_digest = $5,
            worker_id = NULL, lease_token_digest = NULL,
            lease_expires_at = NULL, heartbeat_at = NULL,
            terminal_at = $6, updated_at = $6
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3 AND state = 'running'
        RETURNING *
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&current_job.job_id)
    .bind(current_job.version)
    .bind(
        i64::try_from(next_job.version)
            .map_err(|_| RepositoryError::InvalidInput("Job version exceeds bigint".to_owned()))?,
    )
    .bind(&node_payload.digest)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "orchestration Capability deferral Job",
    ))?;
    let source_job = job_from_row(source_job)?;
    let node_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'waiting', version = version + 1,
            payload_schema_version = $5, payload = $6, payload_digest = $7,
            updated_at = $4
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'node_execution' AND state = 'running'
          AND terminal_at IS NULL
        RETURNING version
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.node_id)
    .bind(parents.node_version)
    .bind(database_now)
    .bind(node_payload.schema_version)
    .bind(&node_payload.value)
    .bind(&node_payload.digest)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "orchestration Capability deferral Node",
    ))?;
    let mut current_snapshot = parents.run.current.clone();
    if parents.run.active_work_count == 1 {
        current_snapshot.waiting_reason = Some("capability_invocation".to_owned());
    }
    let run_id: ResourceId = parents.run.run_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    current_snapshot
        .validate(&run_id)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let current_payload = TypedPayload::from_versioned(1, &current_snapshot, 1_048_576)?;
    let run = sqlx::query(
        r#"
        UPDATE insight_platform.runs
        SET state = CASE WHEN active_work_count = 1 THEN 'waiting' ELSE state END,
            version = version + 1, active_work_count = active_work_count - 1,
            current_schema_version = $4, current_payload = $5,
            current_payload_digest = $6, updated_at = $7
        WHERE tenant_id = $1 AND run_id = $2 AND version = $3
          AND state = 'running' AND active_work_count > 0 AND terminal_at IS NULL
        RETURNING *
        "#,
    )
    .bind(&parents.run.tenant_id)
    .bind(&parents.run.run_id)
    .bind(parents.run.version)
    .bind(current_payload.schema_version)
    .bind(&current_payload.value)
    .bind(&current_payload.digest)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "orchestration Capability deferral Run",
    ))?;
    Ok(DeferredOrchestrationCapabilityInvocation {
        run: run_from_row(run)?,
        node_id: parents.node_id.clone(),
        node_version,
        source_job,
        invocation: invocation.clone(),
        capability_job: capability_job.cloned(),
        settled_quota_account_ids: Vec::new(),
    })
}

async fn require_exact_capability_candidate_selection(
    transaction: &mut Transaction<'_, Postgres>,
    parents: &LockedOrchestrationJobParents,
    runtime_node: &insight_platform_plan::RuntimeNode,
    command: &DeferOrchestrationToCapabilityInvocation,
    scope_limits: ScopeEnvironmentLimits,
) -> Result<u16, RepositoryError> {
    let insight_platform_plan::RuntimeNode::CapabilityCall {
        capability_slot_id,
        input,
        candidate_route,
        ..
    } = runtime_node
    else {
        return Err(RepositoryError::Conflict(
            "orchestration Capability exact Plan node",
        ));
    };
    let slot = parents
        .run
        .bindings
        .slots
        .iter()
        .find(|slot| slot.slot_id == *capability_slot_id)
        .ok_or(RepositoryError::NotFound("frozen Capability slot"))?;
    if command
        .plan
        .dependency_slots
        .get(capability_slot_id)
        .is_none_or(|plan_slot| plan_slot.requirement_digest != slot.requirement_digest)
    {
        return Err(RepositoryError::Conflict(
            "Capability Plan frozen requirement",
        ));
    }
    let FrozenSlotTarget::Capability {
        candidates,
        selection_policy,
        ..
    } = &slot.target
    else {
        return Err(RepositoryError::InvalidInput(
            "selected slot is not a Capability slot".to_owned(),
        ));
    };
    let policy =
        load_exact_frozen_selection_policy(transaction, &parents.run, selection_policy, true)
            .await?;
    let tenant_id: ResourceId = parents.run.tenant_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let run_id: ResourceId = parents.run.run_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let scope_id: ResourceId = parents.scope_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let environments =
        load_scope_environment_chain(transaction, &tenant_id, &run_id, &scope_id, scope_limits)
            .await?;
    let references = insight_platform_orchestrator::resolve_scope_inputs(
        std::slice::from_ref(input),
        &environments,
        scope_limits,
    )?;
    let mut resolved = load_resolved_expression_values(
        transaction,
        &tenant_id,
        &run_id,
        vec![input.clone()],
        references,
    )
    .await?;
    let exact_input = resolved.pop().ok_or_else(|| {
        RepositoryError::CorruptRow("Capability input RunValue missing".to_owned())
    })?;
    if exact_input != command.input {
        return Err(RepositoryError::Conflict(
            "Capability input RunValue evidence",
        ));
    }
    let route = match (candidate_route, command.route.as_ref()) {
        (None, None) => None,
        (Some(port), Some((provided, materialized))) => {
            let references = insight_platform_orchestrator::resolve_scope_inputs(
                std::slice::from_ref(port),
                &environments,
                scope_limits,
            )?;
            let mut resolved = load_resolved_expression_values(
                transaction,
                &tenant_id,
                &run_id,
                vec![port.clone()],
                references,
            )
            .await?;
            let exact_route = resolved.pop().ok_or_else(|| {
                RepositoryError::CorruptRow("Capability route RunValue missing".to_owned())
            })?;
            let reference = ExactRunValueRef {
                value_id: exact_route.run_value_id.clone(),
                schema_digest: exact_route.schema_digest.clone(),
                content_digest: exact_route.content_digest.clone(),
            };
            if &exact_route != provided
                || materialized.schema_digest != exact_route.schema_digest
                || materialized.canonical_digest != exact_route.content_digest
                || matches!(&exact_route.value, ValueRef::Inline { value } if value != &materialized.value)
            {
                return Err(RepositoryError::Conflict(
                    "Capability route RunValue evidence",
                ));
            }
            Some((reference, materialized))
        }
        _ => {
            return Err(RepositoryError::Conflict(
                "Capability route exact Plan contract",
            ))
        }
    };
    let route = route.as_ref().map(|(reference, value)| (reference, *value));
    let exact = derive_candidate_selection(
        capability_slot_id,
        selection_policy,
        &policy,
        candidates,
        route,
    )?;
    if exact != command.selection_evidence {
        return Err(RepositoryError::Conflict(
            "exact Capability candidate selection",
        ));
    }
    candidates
        .iter()
        .position(|candidate| candidate == &exact.selected_deployment)
        .and_then(|ordinal| u16::try_from(ordinal).ok())
        .ok_or(RepositoryError::Conflict(
            "Capability selected candidate ordinal",
        ))
}

async fn require_exact_model_candidate_selection(
    transaction: &mut Transaction<'_, Postgres>,
    parents: &LockedOrchestrationJobParents,
    runtime_node: &RuntimeNode,
    command: &DeferOrchestrationToModelTurn,
    scope_limits: ScopeEnvironmentLimits,
) -> Result<u16, RepositoryError> {
    let RuntimeNode::ModelLoop {
        model_slot_id,
        skill_slot_ids,
        capability_slot_ids,
        input,
        model_route,
        ..
    } = runtime_node
    else {
        return Err(RepositoryError::Conflict(
            "orchestration Model exact Plan node",
        ));
    };
    let slot = parents
        .run
        .bindings
        .slots
        .iter()
        .find(|slot| slot.slot_id == *model_slot_id)
        .ok_or(RepositoryError::NotFound("frozen Model slot"))?;
    if command
        .plan
        .dependency_slots
        .get(model_slot_id)
        .is_none_or(|plan_slot| plan_slot.requirement_digest != slot.requirement_digest)
    {
        return Err(RepositoryError::Conflict("Model Plan frozen requirement"));
    }
    let FrozenSlotTarget::Model {
        candidates,
        selection_policy,
    } = &slot.target
    else {
        return Err(RepositoryError::Conflict("frozen Model slot kind"));
    };
    let policy =
        load_exact_frozen_selection_policy(transaction, &parents.run, selection_policy, true)
            .await?;
    let tenant_id: ResourceId = parents.run.tenant_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let run_id: ResourceId = parents.run.run_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let scope_id: ResourceId = parents.scope_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let environments =
        load_scope_environment_chain(transaction, &tenant_id, &run_id, &scope_id, scope_limits)
            .await?;
    let references = insight_platform_orchestrator::resolve_scope_inputs(
        std::slice::from_ref(input),
        &environments,
        scope_limits,
    )?;
    let mut resolved = load_resolved_expression_values(
        transaction,
        &tenant_id,
        &run_id,
        vec![input.clone()],
        references,
    )
    .await?;
    if resolved.pop().as_ref() != Some(&command.input) {
        return Err(RepositoryError::Conflict("Model input RunValue evidence"));
    }
    let route = match (model_route, command.route.as_ref()) {
        (None, None) => None,
        (Some(port), Some((provided, materialized))) => {
            let references = insight_platform_orchestrator::resolve_scope_inputs(
                std::slice::from_ref(port),
                &environments,
                scope_limits,
            )?;
            let mut resolved = load_resolved_expression_values(
                transaction,
                &tenant_id,
                &run_id,
                vec![port.clone()],
                references,
            )
            .await?;
            let exact_route = resolved.pop().ok_or_else(|| {
                RepositoryError::CorruptRow("Model route RunValue missing".to_owned())
            })?;
            let reference = ExactRunValueRef {
                value_id: exact_route.run_value_id.clone(),
                schema_digest: exact_route.schema_digest.clone(),
                content_digest: exact_route.content_digest.clone(),
            };
            if &exact_route != provided
                || materialized.schema_digest != exact_route.schema_digest
                || materialized.canonical_digest != exact_route.content_digest
                || matches!(&exact_route.value, ValueRef::Inline { value } if value != &materialized.value)
            {
                return Err(RepositoryError::Conflict("Model route RunValue evidence"));
            }
            Some((reference, materialized))
        }
        _ => return Err(RepositoryError::Conflict("Model route exact Plan contract")),
    };
    let route = route.as_ref().map(|(reference, value)| (reference, *value));
    let exact =
        derive_candidate_selection(model_slot_id, selection_policy, &policy, candidates, route)?;
    if exact != command.selection_evidence {
        return Err(RepositoryError::Conflict("exact Model candidate selection"));
    }
    let expected_tool_slots = skill_slot_ids
        .iter()
        .chain(capability_slot_ids)
        .map(|slot_id| {
            parents
                .run
                .bindings
                .slots
                .iter()
                .find(|slot| slot.slot_id == *slot_id)
                .cloned()
                .ok_or(RepositoryError::NotFound("frozen Model tool slot"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if expected_tool_slots != command.tool_slots {
        return Err(RepositoryError::Conflict("exact Model tool slots"));
    }
    candidates
        .iter()
        .position(|candidate| candidate == &exact.selected_deployment)
        .and_then(|ordinal| u16::try_from(ordinal).ok())
        .ok_or(RepositoryError::Conflict(
            "Model selected candidate ordinal",
        ))
}

fn require_child_run_plan_contract(
    runtime_node: &insight_platform_plan::RuntimeNode,
    command: &DeferOrchestrationToChildRun,
) -> Result<(), RepositoryError> {
    let insight_platform_plan::RuntimeNode::ChildAgentCall {
        child_agent_slot_id,
        budget,
        cancellation_policy,
        attempt_limit,
        retry_backoff_milliseconds,
        ..
    } = runtime_node
    else {
        return Err(RepositoryError::Conflict(
            "orchestration child exact Plan node",
        ));
    };
    if child_agent_slot_id != &command.slot_id
        || &command.budget != budget
        || cancellation_policy != &command.cancellation_policy
        || attempt_limit != &command.child_attempt_limit
        || retry_backoff_milliseconds != &command.child_retry_backoff_milliseconds
    {
        return Err(RepositoryError::Conflict(
            "orchestration child exact Plan contract",
        ));
    }
    Ok(())
}

async fn require_exact_child_candidate_selection(
    transaction: &mut Transaction<'_, Postgres>,
    parents: &LockedOrchestrationJobParents,
    runtime_node: &insight_platform_plan::RuntimeNode,
    command: &DeferOrchestrationToChildRun,
    scope_limits: ScopeEnvironmentLimits,
) -> Result<(), RepositoryError> {
    require_child_run_plan_contract(runtime_node, command)?;
    let insight_platform_plan::RuntimeNode::ChildAgentCall {
        input,
        candidate_route,
        ..
    } = runtime_node
    else {
        unreachable!("exact child contract was checked above");
    };

    let slot = parents
        .run
        .bindings
        .slots
        .iter()
        .find(|slot| slot.slot_id == command.slot_id)
        .ok_or(RepositoryError::NotFound("frozen child Agent slot"))?;
    if command
        .plan
        .dependency_slots
        .get(&command.slot_id)
        .is_none_or(|plan_slot| plan_slot.requirement_digest != slot.requirement_digest)
    {
        return Err(RepositoryError::Conflict("child Plan frozen requirement"));
    }
    let FrozenSlotTarget::ChildAgent {
        candidates,
        selection_policy,
    } = &slot.target
    else {
        return Err(RepositoryError::InvalidInput(
            "selected slot is not a child Agent slot".to_owned(),
        ));
    };
    let policy =
        load_exact_frozen_selection_policy(transaction, &parents.run, selection_policy, true)
            .await?;

    let tenant_id: ResourceId = parents.run.tenant_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let run_id: ResourceId = parents.run.run_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let scope_id: ResourceId = parents.scope_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let environments =
        load_scope_environment_chain(transaction, &tenant_id, &run_id, &scope_id, scope_limits)
            .await?;
    let input_references = insight_platform_orchestrator::resolve_scope_inputs(
        std::slice::from_ref(input),
        &environments,
        scope_limits,
    )?;
    let mut resolved_input = load_resolved_expression_values(
        transaction,
        &tenant_id,
        &run_id,
        vec![input.clone()],
        input_references,
    )
    .await?;
    let resolved_input = resolved_input
        .pop()
        .ok_or_else(|| RepositoryError::CorruptRow("child input RunValue missing".to_owned()))?;
    if command.source_value_ids != [resolved_input.run_value_id.clone()]
        || command.input.classification != resolved_input.classification
        || command.input.schema_digest != resolved_input.schema_digest
        || command.input.content_digest != resolved_input.content_digest
        || command.input.value != resolved_input.value
    {
        return Err(RepositoryError::Conflict("child input RunValue evidence"));
    }

    let route = match (
        candidate_route,
        command.selection_evidence.route_value.as_ref(),
        command.materialized_route.as_ref(),
    ) {
        (None, None, None) => None,
        (Some(port), Some(reference), Some(materialized)) => {
            let references = insight_platform_orchestrator::resolve_scope_inputs(
                std::slice::from_ref(port),
                &environments,
                scope_limits,
            )?;
            let mut resolved = load_resolved_expression_values(
                transaction,
                &tenant_id,
                &run_id,
                vec![port.clone()],
                references,
            )
            .await?;
            let value = resolved.pop().ok_or_else(|| {
                RepositoryError::CorruptRow("selection route RunValue missing".to_owned())
            })?;
            if value.run_value_id != reference.value_id
                || value.schema_digest != reference.schema_digest
                || value.content_digest != reference.content_digest
                || materialized.schema_digest != reference.schema_digest
                || materialized.canonical_digest != reference.content_digest
                || matches!(&value.value, ValueRef::Inline { value } if value != &materialized.value)
            {
                return Err(RepositoryError::Conflict(
                    "selection route RunValue evidence",
                ));
            }
            Some((reference, materialized))
        }
        _ => {
            return Err(RepositoryError::Conflict(
                "selection route exact Plan contract",
            ))
        }
    };
    let exact = derive_candidate_selection(
        &command.slot_id,
        selection_policy,
        &policy,
        candidates,
        route,
    )?;
    if exact != command.selection_evidence
        || exact.selected_deployment != command.selected_child_deployment
    {
        return Err(RepositoryError::Conflict("exact child candidate selection"));
    }
    Ok(())
}

pub(crate) async fn load_exact_frozen_selection_policy(
    transaction: &mut Transaction<'_, Postgres>,
    run: &RunRecord,
    binding: &insight_platform_contracts::ExactPolicyBinding,
    lock_rows: bool,
) -> Result<CandidateSelectionPolicyDocument, RepositoryError> {
    load_exact_selection_policy_for_tenant(transaction, &run.tenant_id, binding, lock_rows).await
}

pub(crate) async fn load_exact_selection_policy_for_tenant(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    binding: &insight_platform_contracts::ExactPolicyBinding,
    lock_rows: bool,
) -> Result<CandidateSelectionPolicyDocument, RepositoryError> {
    binding
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    if lock_rows {
        let locked = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT true
            FROM insight_platform.deployments AS deployment
            JOIN insight_platform.resources AS resource
              ON resource.tenant_id = deployment.tenant_id
             AND resource.resource_id = deployment.resource_id
            JOIN insight_platform.resource_versions AS version
              ON version.tenant_id = deployment.tenant_id
             AND version.resource_version_id = deployment.resource_version_id
             AND version.resource_id = deployment.resource_id
            WHERE deployment.tenant_id = $1 AND deployment.deployment_id = $2
              AND deployment.bindings_digest = $3
              AND deployment.resource_version_id = $4
              AND version.content_digest = $5
              AND resource.resource_kind = 'policy'
              AND resource.lifecycle_state = 'active' AND resource.gate_state = 'enabled'
              AND version.resource_version_kind = 'policy_revision'
            FOR SHARE OF deployment, resource, version
            "#,
        )
        .bind(tenant_id)
        .bind(binding.deployment.deployment_id.to_string())
        .bind(binding.deployment.deployment_digest.to_string())
        .bind(binding.revision.revision_id.to_string())
        .bind(binding.revision.semantic_digest.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .is_some();
        if !locked {
            return Err(RepositoryError::NotFound(
                "frozen Selection Policy Deployment",
            ));
        }
    }
    let row = sqlx::query(
        r#"
        SELECT deployment.resource_version_id,
               deployment.payload_schema_version AS bindings_schema_version,
               deployment.bindings, deployment.bindings_digest,
               version.content_digest,
               version.payload_schema_version AS version_schema_version,
               version.payload AS version_payload, version.payload_digest AS version_payload_digest
        FROM insight_platform.deployments AS deployment
        JOIN insight_platform.resources AS resource
          ON resource.tenant_id = deployment.tenant_id
         AND resource.resource_id = deployment.resource_id
        JOIN insight_platform.resource_versions AS version
          ON version.tenant_id = deployment.tenant_id
         AND version.resource_version_id = deployment.resource_version_id
         AND version.resource_id = deployment.resource_id
        WHERE deployment.tenant_id = $1 AND deployment.deployment_id = $2
          AND deployment.bindings_digest = $3
          AND deployment.resource_version_id = $4
          AND version.content_digest = $5
          AND resource.resource_kind = 'policy'
          AND resource.lifecycle_state = 'active' AND resource.gate_state = 'enabled'
          AND version.resource_version_kind = 'policy_revision'
        "#,
    )
    .bind(tenant_id)
    .bind(binding.deployment.deployment_id.to_string())
    .bind(binding.deployment.deployment_digest.to_string())
    .bind(binding.revision.revision_id.to_string())
    .bind(binding.revision.semantic_digest.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound(
        "frozen Selection Policy Deployment",
    ))?;
    let bindings = payload_from_row(
        &row,
        "bindings_schema_version",
        "bindings",
        "bindings_digest",
    )?;
    let DeploymentClosure::Policy(closure) = decode_deployment_closure(&bindings)? else {
        return Err(RepositoryError::CorruptRow(
            "Selection Policy Deployment contains the wrong closure".to_owned(),
        ));
    };
    if closure.policy_revision != binding.revision {
        return Err(RepositoryError::Conflict(
            "frozen Selection Policy Revision",
        ));
    }
    let version_payload = payload_from_row(
        &row,
        "version_schema_version",
        "version_payload",
        "version_payload_digest",
    )?;
    let published = decode_published_version_payload(&version_payload)?;
    published
        .validate_for(RegistryResourceKind::Policy, &binding.revision.revision_id)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let ResourceDocument::Policy(policy) = published.document else {
        return Err(RepositoryError::CorruptRow(
            "Selection Policy Revision contains the wrong document".to_owned(),
        ));
    };
    if policy.policy_kind != PolicyKind::Selection {
        return Err(RepositoryError::Conflict(
            "frozen Policy is not a Selection Policy",
        ));
    }
    policy.selection.ok_or_else(|| {
        RepositoryError::CorruptRow("Selection Policy document is missing".to_owned())
    })
}

async fn require_enabled_exact_agent_deployment(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    exact: &ExactDeploymentRef,
) -> Result<DeploymentRecord, RepositoryError> {
    let deployment = load_deployment(transaction, tenant_id, &exact.deployment_id).await?;
    if deployment.bindings.digest != exact.deployment_digest.to_string() {
        return Err(RepositoryError::Conflict("exact child Agent Deployment"));
    }
    let resource_id: ResourceId = deployment.resource_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let resource = load_resource(transaction, tenant_id, &resource_id).await?;
    if resource.resource_kind != RegistryResourceKind::Agent.as_str()
        || resource.lifecycle_state != EntityLifecycle::Active.as_str()
        || resource.gate_state != AdministrativeGate::Enabled.as_str()
    {
        return Err(RepositoryError::Conflict("child Agent Deployment gate"));
    }
    Ok(deployment)
}

async fn require_child_input_schema(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    interface: &ExactVersionRef,
    input_schema_digest: &Sha256Digest,
) -> Result<(), RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT version.payload_schema_version, version.payload, version.payload_digest
        FROM insight_platform.resource_versions AS version
        JOIN insight_platform.resources AS resource
          ON resource.tenant_id = version.tenant_id
         AND resource.resource_id = version.resource_id
        WHERE version.tenant_id = $1 AND version.resource_version_id = $2
          AND version.resource_version_kind = 'agent_interface_revision'
          AND version.content_digest = $3
          AND resource.resource_kind = 'agent'
          AND resource.lifecycle_state = 'active' AND resource.gate_state = 'enabled'
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(interface.revision_id.to_string())
    .bind(interface.semantic_digest.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("exact child Agent interface"))?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let published = decode_published_version_payload(&payload)?;
    let ResourceDocument::Agent(spec) = published.document else {
        return Err(RepositoryError::CorruptRow(
            "child Agent interface contains a non-Agent document".to_owned(),
        ));
    };
    if &spec.input_schema.canonical_digest != input_schema_digest {
        return Err(RepositoryError::InvalidInput(
            "child input does not match the exact Agent interface schema".to_owned(),
        ));
    }
    Ok(())
}

async fn require_child_input_sources(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    parent_run_id: &ResourceId,
    source_value_ids: &[ResourceId],
    child_classification: DataClassification,
) -> Result<(), RepositoryError> {
    if source_value_ids.is_empty() {
        return Ok(());
    }
    let source_ids = source_value_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT value_id, classification
        FROM insight_platform.run_values
        WHERE tenant_id = $1 AND run_id = $2 AND value_id = ANY($3)
        ORDER BY value_id
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(parent_run_id.to_string())
    .bind(&source_ids)
    .fetch_all(&mut **transaction)
    .await?;
    if rows.len() != source_ids.len() {
        return Err(RepositoryError::NotFound("parent Run input source value"));
    }
    let mut joined = DataClassification::Public;
    for row in rows {
        let classification = row
            .try_get::<String, _>("classification")?
            .parse::<DataClassification>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        joined = joined.join(classification);
    }
    if child_classification.rank() < joined.rank() {
        return Err(RepositoryError::PermissionDenied);
    }
    Ok(())
}

async fn count_run_descendants(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    parent_run_id: &ResourceId,
) -> Result<u32, RepositoryError> {
    let count: i64 = sqlx::query_scalar(
        r#"
        WITH RECURSIVE descendants(run_id) AS (
            SELECT run_id
            FROM insight_platform.runs
            WHERE tenant_id = $1 AND parent_run_id = $2
            UNION
            SELECT child.run_id
            FROM insight_platform.runs AS child
            JOIN descendants AS parent ON parent.run_id = child.parent_run_id
            WHERE child.tenant_id = $1
        )
        SELECT count(*) FROM descendants
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(parent_run_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    u32::try_from(count)
        .map_err(|_| RepositoryError::CorruptRow("Run descendant count exceeds u32".to_owned()))
}

#[allow(clippy::too_many_arguments)]
async fn mutate_deferred_orchestration_child_run(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    next_parent_job: &JobProjection,
    parents: &LockedOrchestrationJobParents,
    locked_root: Option<&RunRecord>,
    command: &DeferOrchestrationToChildRun,
    child_bindings: &RunBindingsSnapshot,
    child_execution: &crate::execution_requirements::StoredExecutionRequirement,
    child_budget: &insight_platform_orchestrator::ChildBudget,
    child_ancestry: &insight_platform_orchestrator::RunAncestrySnapshot,
    child_link_payload: &ChildRunLinkPayload,
    child_wait: &StoredChildRunWaitPayload,
    child_entry_plan_node_key: &PlanNodeKey,
    child_entry_node_kind: PlanNodeKind,
    scope_environment_limits: ScopeEnvironmentLimits,
    database_now: DateTime<Utc>,
) -> Result<DeferredOrchestrationChildRun, RepositoryError> {
    if !NodeExecutionState::Running.can_transition_to(NodeExecutionState::Waiting) {
        return Err(RepositoryError::Conflict(
            "orchestration child wait transition",
        ));
    }

    let parent_job = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'succeeded', version = $4, result_digest = $5,
            worker_id = NULL, lease_token_digest = NULL,
            lease_expires_at = NULL, heartbeat_at = NULL,
            terminal_at = $6, updated_at = $6
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3 AND state = 'running'
        RETURNING *
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&current_job.job_id)
    .bind(current_job.version)
    .bind(
        i64::try_from(next_parent_job.version)
            .map_err(|_| RepositoryError::InvalidInput("Job version exceeds bigint".to_owned()))?,
    )
    .bind(child_link_payload.input_digest.to_string())
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "orchestration child deferral Job",
    ))?;
    let parent_job = job_from_row(parent_job)?;

    let child_bindings_payload = TypedPayload::from_versioned(1, child_bindings, 1_048_576)?;
    let mut child_current = RunCurrentSnapshot::initial(
        command.child_run_id.clone(),
        command.selected_child_deployment.deployment_id.clone(),
        command.input.value_id.clone(),
    );
    child_current.ancestry = child_ancestry.clone();
    child_current
        .validate(&command.child_run_id)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let child_current_payload = TypedPayload::from_versioned(1, &child_current, 1_048_576)?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.runs (
            tenant_id, run_id, root_run_id, parent_run_id, parent_node_id,
            agent_deployment_id, principal_id, trace_id, state,
            bindings_schema_version, bindings, bindings_digest,
            current_schema_version, current_payload, current_payload_digest,
            depth, deadline, execution_requirement_version, execution_requirement, execution_requirement_digest,
            created_at, updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, 'queued',
            $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $20
        )
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(command.child_run_id.to_string())
    .bind(child_ancestry.root_run_id.to_string())
    .bind(&parents.run.run_id)
    .bind(&parents.node_id)
    .bind(command.selected_child_deployment.deployment_id.to_string())
    .bind(parents.run.bindings.principal.principal_id.to_string())
    .bind(current_job.trace.trace_id.to_string())
    .bind(child_bindings_payload.schema_version)
    .bind(&child_bindings_payload.value)
    .bind(child_bindings.canonical_digest.to_string())
    .bind(child_current_payload.schema_version)
    .bind(&child_current_payload.value)
    .bind(&child_current_payload.digest)
    .bind(i32::from(child_ancestry.depth))
    .bind(child_budget.deadline)
    .bind(child_execution.version).bind(&child_execution.value).bind(&child_execution.digest)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?;

    let child_scope_payload = TypedPayload::new(
        1,
        &StoredRootScopePayload {
            root_run_id: child_ancestry.root_run_id.clone(),
            environment: root_scope_environment(&command.input, scope_environment_limits)?,
        },
    )?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_nodes (
            tenant_id, node_id, run_id, parent_node_id, record_kind, scope_id,
            plan_node_key, activation_ordinal, related_run_id, logical_key,
            node_kind, state, payload_schema_version, payload, payload_digest, deadline, created_at, updated_at
        ) VALUES (
            $1, $2, $3, NULL, 'scope_instance', $2,
            NULL, NULL, NULL, 'scope:root', 'root', 'open', $4, $5, $6, $7, $8, $8
        )
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(command.child_root_scope_id.to_string())
    .bind(command.child_run_id.to_string())
    .bind(child_scope_payload.schema_version)
    .bind(&child_scope_payload.value)
    .bind(&child_scope_payload.digest)
    .bind(child_budget.deadline)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?;

    let child_node_payload = TypedPayload::new(
        1,
        &serde_json::json!({
            "plan_node_key": child_entry_plan_node_key,
            "required_control_tokens": ["root"],
        }),
    )?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_nodes (
            tenant_id, node_id, run_id, parent_node_id, record_kind, scope_id,
            plan_node_key, activation_ordinal, related_run_id, logical_key,
            node_kind, state, enqueue_round,
            payload_schema_version, payload, payload_digest, deadline, created_at, updated_at
        ) VALUES (
            $1, $2, $3, NULL, 'node_execution', $4,
            $5, 1, NULL, $6, $7, 'ready', 0, $8, $9, $10, $11, $12, $12
        )
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(command.child_entry_node_execution_id.to_string())
    .bind(command.child_run_id.to_string())
    .bind(command.child_root_scope_id.to_string())
    .bind(child_entry_plan_node_key.as_str())
    .bind(format!("entry:{}:1", child_entry_plan_node_key.as_str()))
    .bind(child_entry_node_kind.as_str())
    .bind(child_node_payload.schema_version)
    .bind(&child_node_payload.value)
    .bind(&child_node_payload.digest)
    .bind(child_budget.deadline)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?;

    let (inline_value, artifact_id) = match &command.input.value {
        ValueRef::Inline { value } => (Some(value), None),
        ValueRef::Artifact { artifact } => (None, Some(artifact.artifact_id().to_string())),
    };
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_values (
            tenant_id, value_id, run_id, node_id, value_kind, classification,
            schema_digest, content_digest, inline_value, artifact_id, created_at
        ) VALUES ($1, $2, $3, NULL, 'run_input', $4, $5, $6, $7, $8, $9)
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(command.input.value_id.to_string())
    .bind(command.child_run_id.to_string())
    .bind(command.input.classification.as_str())
    .bind(command.input.schema_digest.to_string())
    .bind(command.input.content_digest.to_string())
    .bind(inline_value)
    .bind(artifact_id)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "UPDATE insight_platform.runs SET input_value_id = $3 WHERE tenant_id = $1 AND run_id = $2",
    )
    .bind(&current_job.tenant_id)
    .bind(command.child_run_id.to_string())
    .bind(command.input.value_id.to_string())
    .execute(&mut **transaction)
    .await?;

    let child_job_payload = OrchestrationJobPayload {
        external_leaf_completion: None,
        bindings_digest: child_bindings.canonical_digest.clone(),
        node_execution_id: command.child_entry_node_execution_id.clone(),
        root_scope_id: command.child_root_scope_id.clone(),
        retry_backoff_milliseconds: command.child_retry_backoff_milliseconds,
        wake_contract: None,
        convergence_failure: None,
        model_tool_continuation: None,
    }
    .to_payload()?;
    let child_job = sqlx::query(
        r#"
        INSERT INTO insight_platform.jobs (
                tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, trace_id, run_id, node_id, state, attempt_limit, scheduled_at, deadline, priority, request_digest, payload_schema_version, payload, payload_digest, scheduler_partition_id, execution_requirement_version, execution_requirement, execution_requirement_digest, created_at, updated_at
            ) VALUES (
                $1, $2, 'orchestration_node', 'orchestration', 'node_execution', $3, (SELECT trace_id FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $4), $4, $3, 'ready', $5, $6, $7, $8, $9, $10, $11, $12, (SELECT scheduler_partition_id FROM insight_platform.tenants WHERE tenant_id=$1), (SELECT execution_requirement_version FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement_digest FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), $6, $6
            )
        RETURNING *
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(command.child_orchestration_job_id.to_string())
    .bind(command.child_entry_node_execution_id.to_string())
    .bind(command.child_run_id.to_string())
    .bind(i32::from(command.child_attempt_limit))
    .bind(database_now)
    .bind(child_budget.deadline)
    .bind(scheduler_priority_to_database(current_job.priority))
    .bind(command.request_digest.to_string())
    .bind(child_job_payload.schema_version)
    .bind(&child_job_payload.value)
    .bind(&child_job_payload.digest)
    .fetch_one(&mut **transaction)
    .await?;
    let child_job = job_from_row(child_job)?;

    let stored_link_payload = TypedPayload::with_limit(
        1,
        &StoredChildRunLinkPayload {
            slot_id: command.slot_id.clone(),
            source_value_ids: command.source_value_ids.clone(),
            child_root_scope_id: command.child_root_scope_id.clone(),
            child_entry_node_execution_id: command.child_entry_node_execution_id.clone(),
            child_orchestration_job_id: command.child_orchestration_job_id.clone(),
            link: child_link_payload.clone(),
        },
        262_144,
    )?;
    let child_link = sqlx::query(
        r#"
        INSERT INTO insight_platform.run_nodes (
            tenant_id, node_id, run_id, parent_node_id, record_kind, scope_id,
            plan_node_key, activation_ordinal, related_run_id, logical_key,
            node_kind, state, generation, version,
            payload_schema_version, payload, payload_digest, deadline, created_at, updated_at
        ) VALUES (
            $1, $2, $3, $4, 'child_run_link', $5,
            NULL, NULL, $6, $7,
            'child_run', 'running', 1, 1, $8, $9, $10, $11, $12, $12
        )
        RETURNING *
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(command.child_link_id.to_string())
    .bind(&parents.run.run_id)
    .bind(&parents.node_id)
    .bind(&parents.scope_id)
    .bind(command.child_run_id.to_string())
    .bind(&command.logical_key)
    .bind(stored_link_payload.schema_version)
    .bind(&stored_link_payload.value)
    .bind(&stored_link_payload.digest)
    .bind(child_budget.deadline)
    .bind(database_now)
    .fetch_one(&mut **transaction)
    .await?;
    let child_link = child_run_link_from_row(child_link)?;

    let child_wait_payload = TypedPayload::with_limit(1, child_wait, 262_144)?;
    let parent_node_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'waiting', version = version + 1,
            payload_schema_version = $4, payload = $5, payload_digest = $6,
            updated_at = $7
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'node_execution' AND state = 'running'
          AND terminal_at IS NULL
        RETURNING version
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.node_id)
    .bind(parents.node_version)
    .bind(child_wait_payload.schema_version)
    .bind(&child_wait_payload.value)
    .bind(&child_wait_payload.digest)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "orchestration child deferral Node",
    ))?;

    let mut parent_current = parents.run.current.clone();
    if parents.run.active_work_count == 1 {
        parent_current.waiting_reason = Some("child_run".to_owned());
    }
    let parent_run_id: ResourceId = parents.run.run_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    parent_current
        .validate(&parent_run_id)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let parent_current_payload = TypedPayload::from_versioned(1, &parent_current, 1_048_576)?;
    let parent_run = sqlx::query(
        r#"
        UPDATE insight_platform.runs
        SET state = CASE WHEN active_work_count = 1 THEN 'waiting' ELSE state END,
            version = version + 1, active_work_count = active_work_count - 1,
            descendant_count = descendant_count + CASE WHEN root_run_id = run_id THEN 1 ELSE 0 END,
            current_schema_version = $4, current_payload = $5,
            current_payload_digest = $6, updated_at = $7
        WHERE tenant_id = $1 AND run_id = $2 AND version = $3
          AND state = 'running' AND active_work_count > 0 AND terminal_at IS NULL
          AND descendant_count < $8
        RETURNING *
        "#,
    )
    .bind(&parents.run.tenant_id)
    .bind(&parents.run.run_id)
    .bind(parents.run.version)
    .bind(parent_current_payload.schema_version)
    .bind(&parent_current_payload.value)
    .bind(&parent_current_payload.digest)
    .bind(database_now)
    .bind(i32::try_from(MAX_DESCENDANT_RUNS).expect("descendant hard limit fits integer"))
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "orchestration child deferral parent Run",
    ))?;
    let parent_run = run_from_row(parent_run)?;

    let root_run = if let Some(root) = locked_root {
        let row = sqlx::query(
            r#"
            UPDATE insight_platform.runs
            SET descendant_count = descendant_count + 1,
                version = version + 1, updated_at = $4
            WHERE tenant_id = $1 AND run_id = $2 AND version = $3
              AND terminal_at IS NULL AND descendant_count < $5
            RETURNING *
            "#,
        )
        .bind(&root.tenant_id)
        .bind(&root.run_id)
        .bind(root.version)
        .bind(database_now)
        .bind(i32::try_from(MAX_DESCENDANT_RUNS).expect("descendant hard limit fits integer"))
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict(
            "orchestration child deferral root Run",
        ))?;
        run_from_row(row)?
    } else {
        parent_run.clone()
    };

    let tenant_id: ResourceId = command.fence.tenant_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let child_run = load_run(transaction, &tenant_id, &command.child_run_id).await?;
    Ok(DeferredOrchestrationChildRun {
        root_run,
        parent_run,
        parent_node_id: parents.node_id.clone(),
        parent_node_version,
        parent_job,
        child_link,
        child_run,
        child_job,
        settled_quota_account_ids: Vec::new(),
    })
}

#[allow(clippy::too_many_arguments)]
async fn settle_terminal_child_run(
    transaction: &mut Transaction<'_, Postgres>,
    parent_run: &RunRecord,
    child_run: &RunRecord,
    child_link: &ChildRunLinkRecord,
    next_link: &ChildRunLinkProjection,
    slot: &TerminalChildRunSlot,
    scope_limits: ScopeEnvironmentLimits,
    database_now: DateTime<Utc>,
) -> Result<ResolvedOrchestrationChildRun, RepositoryError> {
    let tenant_id: ResourceId = parent_run.tenant_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let parent_run_id: ResourceId = parent_run.run_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let parent_node_id: ResourceId = child_link.parent_node_execution_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let child_run_id: ResourceId = child_run.run_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let child_link_id: ResourceId = child_link.child_link_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let node_row = sqlx::query(
        r#"
        SELECT state, version, plan_node_key,
               payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id = $2 AND run_id = $3
          AND record_kind = 'node_execution'
        FOR UPDATE
        "#,
    )
    .bind(&parent_run.tenant_id)
    .bind(&child_link.parent_node_execution_id)
    .bind(&parent_run.run_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("terminal child parent Node"))?;
    if node_row.try_get::<String, _>("state")? != NodeExecutionState::Waiting.as_str() {
        return Err(RepositoryError::Conflict(
            "terminal child parent Node state",
        ));
    }
    let wait_payload = payload_from_row(
        &node_row,
        "payload_schema_version",
        "payload",
        "payload_digest",
    )?;
    let wait: StoredChildRunWaitPayload =
        decode_typed_payload(&wait_payload, "Child terminal wait")?;
    if wait.child_link_id != child_link_id
        || wait.child_run_id != child_run_id
        || wait.plan_node_key.as_str() != node_row.try_get::<String, _>("plan_node_key")?
    {
        return Err(RepositoryError::Conflict("terminal child wait owner"));
    }
    let link = sqlx::query(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = $4, version = $5, terminal_at = $6, updated_at = $6
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'child_run_link'
          AND state IN ('running', 'waiting', 'cancelling') AND terminal_at IS NULL
        RETURNING *
        "#,
    )
    .bind(&child_link.tenant_id)
    .bind(&child_link.child_link_id)
    .bind(child_link.version)
    .bind(next_link.state.as_str())
    .bind(i64::try_from(next_link.version).map_err(|_| {
        RepositoryError::InvalidInput("ChildRunLink version exceeds bigint".to_owned())
    })?)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("terminal ChildRunLink"))?;
    let link = child_run_link_from_row(link)?;

    let (parent_output_value_id, resume_job) = match next_link.state {
        ChildLinkState::Succeeded => {
            let child_output_value_id = child_run
                .output_value_id
                .as_deref()
                .ok_or(RepositoryError::Conflict("successful child output"))?
                .parse::<ResourceId>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            let evidence_row = sqlx::query(
                r#"
                SELECT schema_digest, content_digest
                FROM insight_platform.run_values
                WHERE tenant_id = $1 AND run_id = $2 AND value_id = $3
                "#,
            )
            .bind(&child_run.tenant_id)
            .bind(&child_run.run_id)
            .bind(child_output_value_id.to_string())
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(RepositoryError::Conflict("successful child RunValue"))?;
            let schema_digest: Sha256Digest = evidence_row
                .try_get::<String, _>("schema_digest")?
                .parse()
                .map_err(|_| {
                    RepositoryError::CorruptRow("child output schema digest".to_owned())
                })?;
            let content_digest: Sha256Digest = evidence_row
                .try_get::<String, _>("content_digest")?
                .parse()
                .map_err(|_| {
                    RepositoryError::CorruptRow("child output content digest".to_owned())
                })?;
            if wait.output_port.schema_digest() != &schema_digest {
                return Err(RepositoryError::Conflict("child parent output schema"));
            }
            let child_interface = load_exact_agent_interface_spec(transaction, child_run).await?;
            if child_interface.output_schema.canonical_digest != schema_digest {
                return Err(RepositoryError::Conflict("child interface output schema"));
            }
            let mut outputs = load_resolved_expression_values(
                transaction,
                &tenant_id,
                &child_run_id,
                vec![wait.output_port.clone()],
                vec![ExactRunValueRef {
                    value_id: child_output_value_id,
                    schema_digest: schema_digest.clone(),
                    content_digest: content_digest.clone(),
                }],
            )
            .await?;
            let output = outputs
                .pop()
                .ok_or(RepositoryError::Conflict("successful child output"))?;
            let (inline_value, artifact_id, storage) = match &output.value {
                ValueRef::Inline { value } => (Some(value), None, InvocationValueStorage::Inline),
                ValueRef::Artifact { artifact } => (
                    None,
                    Some(artifact.artifact_id().to_string()),
                    InvocationValueStorage::Artifact {
                        artifact: artifact.clone(),
                    },
                ),
            };
            sqlx::query(
                r#"
                INSERT INTO insight_platform.run_values (
                    tenant_id, value_id, run_id, node_id, value_kind, classification,
                    schema_digest, content_digest, inline_value, artifact_id
                ) VALUES ($1, $2, $3, $4, 'run_output', $5, $6, $7, $8, $9)
                "#,
            )
            .bind(&parent_run.tenant_id)
            .bind(slot.parent_output_value_id.to_string())
            .bind(&parent_run.run_id)
            .bind(&child_link.parent_node_execution_id)
            .bind(output.classification.as_str())
            .bind(output.schema_digest.to_string())
            .bind(output.content_digest.to_string())
            .bind(inline_value)
            .bind(artifact_id)
            .execute(&mut **transaction)
            .await?;
            let exact_output = ExactInvocationValueRef {
                schema_version: 1,
                value_id: slot.parent_output_value_id.clone(),
                run_id: parent_run_id.clone(),
                producing_node_id: Some(parent_node_id.clone()),
                value_kind: "run_output".to_owned(),
                classification: output.classification,
                schema_digest: output.schema_digest,
                content_digest: output.content_digest,
                storage,
            };
            exact_output
                .validate()
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
            settle_external_leaf_success_in_transaction(
                transaction,
                ExternalLeafSuccessOwner {
                    tenant_id: &tenant_id,
                    run_id: &parent_run_id,
                    node_id: &parent_node_id,
                    owner_id: &child_run_id,
                    owner_job_id: &child_link_id,
                    output_value_id: &slot.parent_output_value_id,
                    kind: ExternalLeafSuccessKind::Child,
                },
                &exact_output,
                &slot.resume_mutations(),
                scope_limits,
                database_now,
            )
            .await?;
            let job = load_job_by_text(
                transaction,
                &parent_run.tenant_id,
                &slot.resume_job_id.to_string(),
            )
            .await?;
            (Some(slot.parent_output_value_id.clone()), job)
        }
        ChildLinkState::Failed | ChildLinkState::Cancelled | ChildLinkState::TimedOut => {
            let failure = child_link_failure(child_run, next_link.state)?;
            settle_external_leaf_failure_in_transaction(
                transaction,
                &tenant_id,
                &parent_run_id,
                &parent_node_id,
                &child_link_id,
                "child_run",
                &failure,
                false,
                ExternalLeafFailureWait {
                    plan_digest: wait.plan_digest,
                    source_orchestration_job_id: wait.source_orchestration_job_id,
                    root_scope_id: wait.root_scope_id,
                    continuation_attempt_limit: wait.continuation_attempt_limit,
                    retry_backoff_milliseconds: wait.retry_backoff_milliseconds,
                    priority: wait.priority,
                    deadline: wait.deadline,
                },
                &slot.failure_mutations(),
                database_now,
            )
            .await?;
            let job = load_job_by_text(
                transaction,
                &parent_run.tenant_id,
                &slot.resume_job_id.to_string(),
            )
            .await?;
            (None, job)
        }
        _ => return Err(RepositoryError::Conflict("non-terminal ChildRunLink")),
    };
    let parent_run = load_run(transaction, &tenant_id, &parent_run_id).await?;
    let parent_node_version: i64 = sqlx::query_scalar(
        "SELECT version FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
    )
    .bind(&parent_run.tenant_id)
    .bind(&child_link.parent_node_execution_id)
    .fetch_one(&mut **transaction)
    .await?;
    Ok(ResolvedOrchestrationChildRun {
        parent_run,
        parent_node_id: child_link.parent_node_execution_id.clone(),
        parent_node_version,
        child_link: link,
        child_run: child_run.clone(),
        parent_output_value_id,
        resume_job,
    })
}

fn child_link_failure(
    child_run: &RunRecord,
    state: ChildLinkState,
) -> Result<Failure, RepositoryError> {
    let (class, retryability) = match state {
        ChildLinkState::Failed => {
            let child = child_run
                .current
                .failure
                .as_ref()
                .ok_or(RepositoryError::Conflict("failed child Failure"))?;
            (child.class, child.retryability)
        }
        ChildLinkState::TimedOut => (FailureClass::Deadline, Retryability::SafeWithinPolicy),
        ChildLinkState::Cancelled => (FailureClass::Cancelled, Retryability::Never),
        _ => return Err(RepositoryError::Conflict("non-failed child terminal")),
    };
    Ok(Failure {
        code: FailureCode::Platform {
            code: PlatformFailureCode::ChildAgentFailed,
        },
        class,
        retryability,
        safe_message: Some("child agent did not complete successfully".to_owned()),
        details_ref: None,
        source: FailureSource::ChildAgent,
    })
}

#[allow(clippy::too_many_arguments)]
async fn mutate_terminal_child_run(
    transaction: &mut Transaction<'_, Postgres>,
    parent_run: &RunRecord,
    parent_node_version: i64,
    parent_scope_id: &str,
    source_job: &JobRecord,
    child_run: &RunRecord,
    child_link: &ChildRunLinkRecord,
    next_link: &ChildRunLinkProjection,
    slot: &TerminalChildRunSlot,
    converging: bool,
    database_now: DateTime<Utc>,
) -> Result<ResolvedOrchestrationChildRun, RepositoryError> {
    let link = sqlx::query(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = $4, version = $5, terminal_at = $6, updated_at = $6
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'child_run_link'
          AND state IN ('running', 'waiting', 'cancelling') AND terminal_at IS NULL
        RETURNING *
        "#,
    )
    .bind(&child_link.tenant_id)
    .bind(&child_link.child_link_id)
    .bind(child_link.version)
    .bind(next_link.state.as_str())
    .bind(i64::try_from(next_link.version).map_err(|_| {
        RepositoryError::InvalidInput("ChildRunLink version exceeds bigint".to_owned())
    })?)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("terminal ChildRunLink"))?;
    let link = child_run_link_from_row(link)?;

    let target_state = if converging { "cancelling" } else { "ready" };
    let next_parent_node_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = $4, version = version + 1, updated_at = $5
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'node_execution' AND state = 'waiting'
          AND terminal_at IS NULL
        RETURNING version
        "#,
    )
    .bind(&parent_run.tenant_id)
    .bind(&child_link.parent_node_execution_id)
    .bind(parent_node_version)
    .bind(target_state)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("terminal child parent Node"))?;

    let mut parent_current = parent_run.current.clone();
    parent_current.waiting_reason = None;
    let parent_run_id: ResourceId = parent_run.run_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    parent_current
        .validate(&parent_run_id)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let parent_current_payload = TypedPayload::from_versioned(1, &parent_current, 1_048_576)?;
    let next_parent_state = if converging { "cancelling" } else { "running" };
    let parent = sqlx::query(
        r#"
        UPDATE insight_platform.runs
        SET state = $4, version = version + 1,
            current_schema_version = $5, current_payload = $6,
            current_payload_digest = $7, updated_at = $8
        WHERE tenant_id = $1 AND run_id = $2 AND version = $3
          AND state IN ('running', 'waiting', 'cancelling')
          AND terminal_at IS NULL
        RETURNING *
        "#,
    )
    .bind(&parent_run.tenant_id)
    .bind(&parent_run.run_id)
    .bind(parent_run.version)
    .bind(next_parent_state)
    .bind(parent_current_payload.schema_version)
    .bind(&parent_current_payload.value)
    .bind(&parent_current_payload.digest)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("terminal child parent Run"))?;
    let parent = run_from_row(parent)?;

    let source_payload: OrchestrationJobPayload =
        decode_orchestration_job_payload(&source_job.payload)?;
    if source_payload.node_execution_id.to_string() != child_link.parent_node_execution_id
        || source_payload.bindings_digest != parent_run.bindings.canonical_digest
    {
        return Err(RepositoryError::CorruptRow(
            "terminal child source Job disagrees with its parent Node".to_owned(),
        ));
    }
    let wait_row = sqlx::query("SELECT scope_id,payload_schema_version,payload,payload_digest FROM insight_platform.run_nodes WHERE tenant_id=$1 AND node_id=$2 AND run_id=$3 AND record_kind='node_execution'")
        .bind(&parent_run.tenant_id).bind(&child_link.parent_node_execution_id).bind(&parent_run.run_id)
        .fetch_one(&mut **transaction).await?;
    let wait: StoredChildRunWaitPayload = decode_typed_payload(
        &payload_from_row(
            &wait_row,
            "payload_schema_version",
            "payload",
            "payload_digest",
        )?,
        "terminal child wait",
    )?;
    if wait.root_scope_id != source_payload.root_scope_id
        || wait.source_orchestration_job_id.to_string() != source_job.job_id
        || wait.child_link_id.to_string() != child_link.child_link_id
        || wait.child_run_id.to_string() != child_run.run_id
        || wait_row.try_get::<String, _>("scope_id")? != parent_scope_id
    {
        return Err(RepositoryError::CorruptRow(
            "terminal child frozen source and lexical scope".into(),
        ));
    }
    let resume_payload = OrchestrationJobPayload {
        external_leaf_completion: None,
        wake_contract: None,
        convergence_failure: None,
        model_tool_continuation: None,
        ..source_payload
    }
    .to_payload()?;
    let minimum_deadline = database_now
        .checked_add_signed(Duration::seconds(1))
        .ok_or_else(|| RepositoryError::InvalidInput("resume deadline overflowed".to_owned()))?;
    let resume_deadline = parent.deadline.max(minimum_deadline);
    let resume_job = sqlx::query(
        r#"
        INSERT INTO insight_platform.jobs (
                tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, trace_id, run_id, node_id, state, attempt_limit, scheduled_at, deadline, priority, request_digest, payload_schema_version, payload, payload_digest, scheduler_partition_id, execution_requirement_version, execution_requirement, execution_requirement_digest
            ) VALUES (
                $1, $2, 'orchestration_node', 'orchestration', 'node_execution', $3, (SELECT trace_id FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $4), $4, $3, $5, $6, $7, $8, $9, $10, $11, $12, $13, (SELECT scheduler_partition_id FROM insight_platform.tenants WHERE tenant_id=$1), (SELECT execution_requirement_version FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement_digest FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4)
            )
        RETURNING *
        "#,
    )
    .bind(&parent.tenant_id)
    .bind(slot.resume_job_id.to_string())
    .bind(&child_link.parent_node_execution_id)
    .bind(&parent.run_id)
    .bind(target_state)
    .bind(source_job.attempt_limit)
    .bind(database_now)
    .bind(resume_deadline)
    .bind(scheduler_priority_to_database(source_job.priority))
    .bind(slot.resume_request_digest.to_string())
    .bind(resume_payload.schema_version)
    .bind(&resume_payload.value)
    .bind(&resume_payload.digest)
    .fetch_one(&mut **transaction)
    .await?;
    let resume_job = job_from_row(resume_job)?;
    Ok(ResolvedOrchestrationChildRun {
        parent_run: parent,
        parent_node_id: child_link.parent_node_execution_id.clone(),
        parent_node_version: next_parent_node_version,
        child_link: link,
        child_run: child_run.clone(),
        parent_output_value_id: None,
        resume_job,
    })
}

async fn append_terminal_child_run_events(
    transaction: &mut Transaction<'_, Postgres>,
    resolved: &ResolvedOrchestrationChildRun,
    slot: &TerminalChildRunSlot,
    converging: bool,
) -> Result<(), RepositoryError> {
    let parent_run_id = resolved.parent_run.run_id.as_str();
    let payload = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "child_link_id": resolved.child_link.child_link_id,
            "child_run_id": resolved.child_run.run_id,
            "child_run_state": resolved.child_run.state,
            "parent_converging": converging,
            "resume_job_id": resolved.resume_job.job_id,
        }),
        65_536,
    )?;
    append_scheduler_event(
        transaction,
        &resolved.child_link.tenant_id,
        &slot.child_link_event_id,
        &slot.child_link_outbox_id,
        "child_run_link",
        &resolved.child_link.child_link_id,
        resolved.child_link.version,
        Some(parent_run_id),
        "child.completed",
        &payload,
    )
    .await?;
    if !converging {
        return Ok(());
    }
    append_scheduler_event(
        transaction,
        &resolved.parent_run.tenant_id,
        &slot.parent_run_event_id,
        &slot.parent_run_outbox_id,
        "run",
        parent_run_id,
        resolved.parent_run.version,
        Some(parent_run_id),
        if converging {
            "run.child_cancel_converging"
        } else {
            "run.child_resolved"
        },
        &payload,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &resolved.parent_run.tenant_id,
        &slot.parent_node_event_id,
        &slot.parent_node_outbox_id,
        "node_execution",
        &resolved.parent_node_id,
        resolved.parent_node_version,
        Some(parent_run_id),
        if converging {
            "node.child_cancel_converging"
        } else {
            "node.child_ready"
        },
        &payload,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &resolved.resume_job.tenant_id,
        &slot.resume_job_event_id,
        &slot.resume_job_outbox_id,
        "job",
        &resolved.resume_job.job_id,
        resolved.resume_job.version,
        Some(parent_run_id),
        if converging {
            "job.cancelling"
        } else {
            "job.ready"
        },
        &payload,
    )
    .await?;
    Ok(())
}

async fn mutate_cancelling_child_run(
    transaction: &mut Transaction<'_, Postgres>,
    parent_run: &RunRecord,
    child_run: &RunRecord,
    child_link: &ChildRunLinkRecord,
    next_link: &ChildRunLinkProjection,
    next_child_current: insight_platform_orchestrator::RunCurrentSnapshot,
    database_now: DateTime<Utc>,
) -> Result<CancellingOrchestrationChildRun, RepositoryError> {
    let current_child_state = child_run
        .state
        .parse::<RunState>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    if current_child_state != RunState::Cancelling
        && !current_child_state.can_transition_to(RunState::Cancelling)
    {
        return Err(RepositoryError::Conflict("child Run cancel transition"));
    }
    let link = sqlx::query(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'cancelling', version = $4, updated_at = $5
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'child_run_link' AND state IN ('running', 'waiting')
          AND terminal_at IS NULL
        RETURNING *
        "#,
    )
    .bind(&child_link.tenant_id)
    .bind(&child_link.child_link_id)
    .bind(child_link.version)
    .bind(i64::try_from(next_link.version).map_err(|_| {
        RepositoryError::InvalidInput("ChildRunLink version exceeds bigint".to_owned())
    })?)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("ChildRunLink cancellation"))?;
    let link = child_run_link_from_row(link)?;

    let child_current = next_child_current;
    let child_run_id: ResourceId = child_run.run_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    child_current
        .validate(&child_run_id)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let child_current_payload = TypedPayload::from_versioned(1, &child_current, 1_048_576)?;
    let cancel_generation =
        i64::try_from(child_current.control.cancel_generation).map_err(|_| {
            RepositoryError::InvalidInput("child cancel generation exceeds bigint".to_owned())
        })?;
    let child = sqlx::query(
        r#"
        UPDATE insight_platform.runs
        SET state = 'cancelling', version = version + 1,
            current_schema_version = $4, current_payload = $5,
            current_payload_digest = $6, cancel_generation = $7, timeout_generation = $9,
            updated_at = $8
        WHERE tenant_id = $1 AND run_id = $2 AND version = $3
          AND state IN ('queued', 'running', 'waiting', 'cancelling')
          AND terminal_at IS NULL
        RETURNING *
        "#,
    )
    .bind(&child_run.tenant_id)
    .bind(&child_run.run_id)
    .bind(child_run.version)
    .bind(child_current_payload.schema_version)
    .bind(&child_current_payload.value)
    .bind(&child_current_payload.digest)
    .bind(cancel_generation)
    .bind(database_now)
    .bind(
        i64::try_from(child_current.control.timeout_generation)
            .map_err(|_| RepositoryError::CorruptRow("child timeout generation".into()))?,
    )
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("child Run cancellation"))?;
    Ok(CancellingOrchestrationChildRun {
        parent_run: parent_run.clone(),
        child_link: link,
        child_run: run_from_row(child)?,
    })
}

async fn append_cancelling_child_run_events(
    transaction: &mut Transaction<'_, Postgres>,
    cancelling: &CancellingOrchestrationChildRun,
    slot: &ChildRunCancellationSlot,
) -> Result<(), RepositoryError> {
    let payload = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "cancel_generation": cancelling.child_run.cancel_generation,
            "child_link_id": cancelling.child_link.child_link_id,
            "child_run_id": cancelling.child_run.run_id,
            "parent_run_id": cancelling.parent_run.run_id,
        }),
        65_536,
    )?;
    append_scheduler_event(
        transaction,
        &cancelling.child_link.tenant_id,
        &slot.child_link_event_id,
        &slot.child_link_outbox_id,
        "child_run_link",
        &cancelling.child_link.child_link_id,
        cancelling.child_link.version,
        Some(&cancelling.parent_run.run_id),
        "child.cancelling",
        &payload,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &cancelling.child_run.tenant_id,
        &slot.child_run_event_id,
        &slot.child_run_outbox_id,
        "run",
        &cancelling.child_run.run_id,
        cancelling.child_run.version,
        Some(&cancelling.child_run.run_id),
        "run.parent_control_converging",
        &payload,
    )
    .await?;
    Ok(())
}

async fn insert_task_response_value(
    transaction: &mut Transaction<'_, Postgres>,
    task: &TaskRecord,
    run: &RunRecord,
    node_id: &str,
    response: &RunInputValue,
) -> Result<(), RepositoryError> {
    let tenant_id: ResourceId = task.tenant_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    if let ValueRef::Artifact { artifact } = &response.value {
        require_ready_run_artifact(transaction, &tenant_id, artifact).await?;
    }
    let (inline_value, artifact_id) = match &response.value {
        ValueRef::Inline { value } => (Some(value), None),
        ValueRef::Artifact { artifact } => (None, Some(artifact.artifact_id().to_string())),
    };
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_values (
            tenant_id, value_id, run_id, node_id, value_kind, classification,
            schema_digest, content_digest, inline_value, artifact_id
        ) VALUES ($1, $2, $3, $4, 'task_response', $5, $6, $7, $8, $9)
        "#,
    )
    .bind(&task.tenant_id)
    .bind(response.value_id.to_string())
    .bind(&run.run_id)
    .bind(node_id)
    .bind(response.classification.as_str())
    .bind(response.schema_digest.to_string())
    .bind(response.content_digest.to_string())
    .bind(inline_value)
    .bind(artifact_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_signal_payload_value(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    run_id: &str,
    node_id: &str,
    payload: &RunInputValue,
) -> Result<(), RepositoryError> {
    let tenant: ResourceId =
        tenant_id
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?;
    if let ValueRef::Artifact { artifact } = &payload.value {
        require_ready_run_artifact(transaction, &tenant, artifact).await?;
    }
    let (inline_value, artifact_id) = match &payload.value {
        ValueRef::Inline { value } => (Some(value), None),
        ValueRef::Artifact { artifact } => (None, Some(artifact.artifact_id().to_string())),
    };
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_values (
            tenant_id, value_id, run_id, node_id, value_kind, classification,
            schema_digest, content_digest, inline_value, artifact_id
        ) VALUES ($1, $2, $3, $4, 'signal_payload', $5, $6, $7, $8, $9)
        "#,
    )
    .bind(tenant_id)
    .bind(payload.value_id.to_string())
    .bind(run_id)
    .bind(node_id)
    .bind(payload.classification.as_str())
    .bind(payload.schema_digest.to_string())
    .bind(payload.content_digest.to_string())
    .bind(inline_value)
    .bind(artifact_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn bind_run_value_to_scope(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    run_id: &str,
    scope_id: &str,
    response_port: &ExactDataPortRef,
    response: &ExactRunValueRef,
    limits: ScopeEnvironmentLimits,
) -> Result<(), RepositoryError> {
    let scope = sqlx::query(
        r#"
        SELECT parent_node_id, node_kind, version,
               payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
          AND record_kind = 'scope_instance' AND state = 'open'
          AND terminal_at IS NULL
        FOR UPDATE
        "#,
    )
    .bind(tenant_id)
    .bind(run_id)
    .bind(scope_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("durable-wait value Scope"))?;
    let payload = payload_from_row(
        &scope,
        "payload_schema_version",
        "payload",
        "payload_digest",
    )?;
    let scope_kind: String = scope.try_get("node_kind")?;
    let mut root = None;
    let mut nested = None;
    let environment = match scope_kind.as_str() {
        "root" => {
            root = Some(decode_typed_payload::<StoredRootScopePayload>(
                &payload,
                "durable-wait root Scope",
            )?);
            &mut root.as_mut().expect("root Scope assigned").environment
        }
        "parallel_leg" | "loop_iteration" | "map_item" => {
            let value = decode_typed_payload::<StoredControllerScopePayload>(
                &payload,
                "durable-wait controller Scope",
            )?;
            if scope.try_get::<Option<String>, _>("parent_node_id")?
                != Some(value.controller_node_execution_id.to_string())
            {
                return Err(RepositoryError::CorruptRow(
                    "durable-wait Scope controller owner differs".to_owned(),
                ));
            }
            nested = Some(value);
            &mut nested
                .as_mut()
                .expect("controller Scope assigned")
                .environment
        }
        _ => {
            return Err(RepositoryError::CorruptRow(
                "durable-wait Scope kind is unregistered".to_owned(),
            ))
        }
    };
    environment
        .bind_new(response_port.clone(), response.clone(), limits)
        .map_err(|_| RepositoryError::Conflict("durable-wait value Scope binding"))?;
    let next_payload = match (root, nested) {
        (Some(root), None) => TypedPayload::with_limit(1, &root, 262_144)?,
        (None, Some(nested)) => TypedPayload::with_limit(1, &nested, 262_144)?,
        _ => {
            return Err(RepositoryError::CorruptRow(
                "durable-wait Scope payload variant is ambiguous".to_owned(),
            ))
        }
    };
    let updated = sqlx::query(
        r#"
        UPDATE insight_platform.run_nodes
        SET version = version + 1, payload_schema_version = $5,
            payload = $6, payload_digest = $7, updated_at = clock_timestamp()
        WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3 AND version = $4
          AND record_kind = 'scope_instance' AND state = 'open' AND terminal_at IS NULL
        "#,
    )
    .bind(tenant_id)
    .bind(run_id)
    .bind(scope_id)
    .bind(scope.try_get::<i64, _>("version")?)
    .bind(next_payload.schema_version)
    .bind(&next_payload.value)
    .bind(&next_payload.digest)
    .execute(&mut **transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(RepositoryError::Conflict("durable-wait value Scope CAS"));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn mutate_resolved_orchestration_task(
    transaction: &mut Transaction<'_, Postgres>,
    current_task: &TaskRecord,
    next_task: &TaskProjection,
    current_run: &RunRecord,
    current_node_version: i64,
    source_job: &JobRecord,
    resolved_node_payload: &TypedPayload,
    resume_job_id: &ResourceId,
    resume_request_digest: &Sha256Digest,
    database_now: DateTime<Utc>,
) -> Result<ResolvedOrchestrationTask, RepositoryError> {
    let task_payload = TypedPayload::with_limit(
        next_task.payload_schema_version as i32,
        &next_task.payload,
        262_144,
    )?;
    let task = sqlx::query(
        r#"
        UPDATE insight_platform.tasks
        SET state = $4, version = $5, payload_schema_version = $6,
            payload = $7, payload_digest = $8, response_value_id = $9,
            responded_at = $10, updated_at = $10
        WHERE tenant_id = $1 AND task_id = $2 AND version = $3
          AND generation = $11 AND state = 'pending' AND responded_at IS NULL
        RETURNING *
        "#,
    )
    .bind(&current_task.tenant_id)
    .bind(&current_task.task_id)
    .bind(current_task.version)
    .bind(next_task.state.as_str())
    .bind(
        i64::try_from(next_task.version)
            .map_err(|_| RepositoryError::InvalidInput("Task version exceeds bigint".to_owned()))?,
    )
    .bind(task_payload.schema_version)
    .bind(&task_payload.value)
    .bind(&task_payload.digest)
    .bind(
        next_task
            .response_value_id
            .as_ref()
            .map(ToString::to_string),
    )
    .bind(database_now)
    .bind(current_task.generation)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Task first-winner"))?;
    let task = task_from_row(task)?;
    let node_id = current_task
        .node_id
        .as_deref()
        .ok_or_else(|| RepositoryError::CorruptRow("Task has no Node".to_owned()))?;
    let node_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'ready', version = version + 1,
            payload_schema_version = $5, payload = $6, payload_digest = $7,
            updated_at = $4
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'node_execution' AND state = 'waiting'
          AND terminal_at IS NULL
        RETURNING version
        "#,
    )
    .bind(&current_task.tenant_id)
    .bind(node_id)
    .bind(current_node_version)
    .bind(database_now)
    .bind(resolved_node_payload.schema_version)
    .bind(&resolved_node_payload.value)
    .bind(&resolved_node_payload.digest)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Task owner Node wake"))?;
    let mut current_snapshot = current_run.current.clone();
    current_snapshot.waiting_reason = None;
    let run_id: ResourceId = current_run.run_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    current_snapshot
        .validate(&run_id)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let current_payload = TypedPayload::from_versioned(1, &current_snapshot, 1_048_576)?;
    let run = sqlx::query(
        r#"
        UPDATE insight_platform.runs
        SET state = CASE WHEN state = 'waiting' THEN 'running' ELSE state END,
            version = version + 1, current_schema_version = $4,
            current_payload = $5, current_payload_digest = $6, updated_at = $7
        WHERE tenant_id = $1 AND run_id = $2 AND version = $3
          AND state IN ('waiting', 'running') AND active_work_count = 0
          AND terminal_at IS NULL
        RETURNING *
        "#,
    )
    .bind(&current_run.tenant_id)
    .bind(&current_run.run_id)
    .bind(current_run.version)
    .bind(current_payload.schema_version)
    .bind(&current_payload.value)
    .bind(&current_payload.digest)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Task owner Run wake"))?;
    let run = run_from_row(run)?;
    let job = sqlx::query(
        r#"
        INSERT INTO insight_platform.jobs (
                tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, trace_id, invocation_id, run_id, node_id, state, attempt_limit, scheduled_at, deadline, priority, request_digest, effect_key_digest, payload_schema_version, payload, payload_digest, scheduler_partition_id, execution_requirement_version, execution_requirement, execution_requirement_digest
            ) VALUES (
                $1, $2, 'orchestration_node', 'orchestration', 'node_execution', $3, (SELECT trace_id FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $4), NULL, $4, $3, 'ready', $5, $6, $7, $8, $9, NULL, $10, $11, $12, (SELECT scheduler_partition_id FROM insight_platform.tenants WHERE tenant_id=$1), (SELECT execution_requirement_version FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4), (SELECT execution_requirement_digest FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$4)
            )
        RETURNING *
        "#,
    )
    .bind(&current_task.tenant_id)
    .bind(resume_job_id.to_string())
    .bind(node_id)
    .bind(&run.run_id)
    .bind(source_job.attempt_limit)
    .bind(database_now)
    .bind(source_job.deadline)
    .bind(scheduler_priority_to_database(source_job.priority))
    .bind(resume_request_digest.to_string())
    .bind(source_job.payload.schema_version)
    .bind(&source_job.payload.value)
    .bind(&source_job.payload.digest)
    .fetch_one(&mut **transaction)
    .await?;
    Ok(ResolvedOrchestrationTask {
        run,
        node_id: node_id.to_owned(),
        node_version,
        task,
        job: job_from_row(job)?,
    })
}

async fn mutate_woken_orchestration_job(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    next_job: &JobProjection,
    parents: &LockedOrchestrationJobParents,
    resolved_node_payload: &TypedPayload,
    database_now: DateTime<Utc>,
) -> Result<WokenOrchestrationJob, RepositoryError> {
    if next_job.state != JobState::Ready
        || !NodeExecutionState::Waiting.can_transition_to(NodeExecutionState::Ready)
    {
        return Err(RepositoryError::Conflict("orchestration wake transition"));
    }
    let payload = orchestration_job_payload_with_wake(current_job, None)?;
    let job = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'ready', version = $4, scheduled_at = $5, retry_at = NULL,
            wake_kind = NULL, wake_state = NULL, wake_generation = 0,
            payload_schema_version = $6, payload = $7, payload_digest = $8,
            updated_at = $5
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND state = 'waiting' AND worker_id IS NULL
        RETURNING *
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&current_job.job_id)
    .bind(current_job.version)
    .bind(
        i64::try_from(next_job.version)
            .map_err(|_| RepositoryError::InvalidInput("Job version exceeds bigint".to_owned()))?,
    )
    .bind(database_now)
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("waiting orchestration Job"))?;
    let job = job_from_row(job)?;
    let node_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'ready', version = version + 1, retry_at = NULL,
            payload_schema_version = $5, payload = $6, payload_digest = $7,
            updated_at = $4
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'node_execution' AND state = 'waiting'
          AND terminal_at IS NULL
        RETURNING version
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.node_id)
    .bind(parents.node_version)
    .bind(database_now)
    .bind(resolved_node_payload.schema_version)
    .bind(&resolved_node_payload.value)
    .bind(&resolved_node_payload.digest)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("waiting orchestration Node"))?;
    let mut current_snapshot = parents.run.current.clone();
    current_snapshot.waiting_reason = None;
    let run_id: ResourceId = parents.run.run_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    current_snapshot
        .validate(&run_id)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let current_payload = TypedPayload::from_versioned(1, &current_snapshot, 1_048_576)?;
    let run = sqlx::query(
        r#"
        UPDATE insight_platform.runs
        SET state = CASE WHEN state = 'waiting' THEN 'running' ELSE state END,
            version = version + 1, current_schema_version = $4,
            current_payload = $5, current_payload_digest = $6, updated_at = $7
        WHERE tenant_id = $1 AND run_id = $2 AND version = $3
          AND state IN ('waiting', 'running')
          AND terminal_at IS NULL
        RETURNING *
        "#,
    )
    .bind(&parents.run.tenant_id)
    .bind(&parents.run.run_id)
    .bind(parents.run.version)
    .bind(current_payload.schema_version)
    .bind(&current_payload.value)
    .bind(&current_payload.digest)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("waiting orchestration Run"))?;
    Ok(WokenOrchestrationJob {
        run: run_from_row(run)?,
        job,
        node_id: parents.node_id.clone(),
        node_version,
    })
}

async fn mutate_recovered_orchestration_job(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    next_job: &JobProjection,
    parents: &LockedOrchestrationJobParents,
    database_now: DateTime<Utc>,
) -> Result<RecoveredOrchestrationJob, RepositoryError> {
    let (expected_node_state, target_node_state) =
        match (current_job.state.as_str(), next_job.state) {
            ("leased", JobState::Ready) => ("ready", NodeExecutionState::Ready),
            ("running", JobState::RetryScheduled) => {
                ("running", NodeExecutionState::RetryScheduled)
            }
            _ => {
                return Err(RepositoryError::InvalidInput(
                    "expired orchestration recovery decision is unsupported".to_owned(),
                ))
            }
        };
    let job = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = $4, version = $5, worker_id = NULL, lease_token_digest = NULL,
            lease_expires_at = NULL, heartbeat_at = NULL, retry_at = $6,
            started_at = NULL, updated_at = $7
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND state IN ('leased', 'running')
        RETURNING *
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&current_job.job_id)
    .bind(current_job.version)
    .bind(next_job.state.as_str())
    .bind(
        i64::try_from(next_job.version)
            .map_err(|_| RepositoryError::InvalidInput("Job version exceeds bigint".to_owned()))?,
    )
    .bind(next_job.retry_at)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("expired orchestration Job"))?;
    let job = job_from_row(job)?;
    let node_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = $4, version = version + 1, retry_at = $5, updated_at = $6
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'node_execution' AND state = $7
          AND terminal_at IS NULL
        RETURNING version
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.node_id)
    .bind(parents.node_version)
    .bind(target_node_state.as_str())
    .bind(next_job.retry_at)
    .bind(database_now)
    .bind(expected_node_state)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("expired orchestration Node"))?;
    let run = sqlx::query(
        r#"
        UPDATE insight_platform.runs
        SET version = version + 1, active_work_count = active_work_count - 1,
            updated_at = $4
        WHERE tenant_id = $1 AND run_id = $2 AND version = $3
          AND state = 'running' AND active_work_count > 0 AND terminal_at IS NULL
        RETURNING *
        "#,
    )
    .bind(&parents.run.tenant_id)
    .bind(&parents.run.run_id)
    .bind(parents.run.version)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("expired orchestration Run"))?;
    Ok(RecoveredOrchestrationJob {
        run: run_from_row(run)?,
        job,
        node_id: parents.node_id.clone(),
        node_version,
        settled_quota_account_ids: Vec::new(),
    })
}

async fn append_orchestration_yield_events(
    transaction: &mut Transaction<'_, Postgres>,
    yielded: &YieldedOrchestrationJob,
    mutations: &OrchestrationYieldMutationIds,
    outcome_payload: &TypedPayload,
) -> Result<(), RepositoryError> {
    let run_id = yielded.run.run_id.as_str();
    let suffix = yielded.job.state.as_str();
    let common = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "job_id": yielded.job.job_id,
            "lease_generation": yielded.job.lease_epoch,
            "outcome_digest": outcome_payload.digest,
            "settled_quota_account_ids": yielded.settled_quota_account_ids,
        }),
        65_536,
    )?;
    append_scheduler_event(
        transaction,
        &yielded.run.tenant_id,
        &mutations.run_event_id,
        &mutations.run_outbox_id,
        "run",
        run_id,
        yielded.run.version,
        Some(run_id),
        &format!("run.work_{suffix}"),
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &yielded.run.tenant_id,
        &mutations.node_event_id,
        &mutations.node_outbox_id,
        "node_execution",
        &yielded.node_id,
        yielded.node_version,
        Some(run_id),
        &format!("node.{suffix}"),
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &yielded.run.tenant_id,
        &mutations.job_event_id,
        &mutations.job_outbox_id,
        "job",
        &yielded.job.job_id,
        yielded.job.version,
        Some(run_id),
        &format!("job.{suffix}"),
        outcome_payload,
    )
    .await?;
    Ok(())
}

async fn append_orchestration_controller_events(
    transaction: &mut Transaction<'_, Postgres>,
    applied: &AppliedOrchestrationControllerStep,
    mutations: &ControllerStepMutationIds,
    plan_digest: &Sha256Digest,
    decision_payload: &TypedPayload,
) -> Result<(), RepositoryError> {
    let run_id = applied.run.run_id.as_str();
    let common = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "activated_node_ids": applied
                .activations
                .iter()
                .map(|activation| activation.node_id.clone())
                .collect::<Vec<_>>(),
            "created_scope_ids": applied
                .created_scopes
                .iter()
                .map(|scope| scope.scope_id.clone())
                .collect::<Vec<_>>(),
            "cancelled_remainder_scope_ids": applied
                .cancelled_remainders
                .iter()
                .map(|remainder| remainder.scope.scope_id.clone())
                .collect::<Vec<_>>(),
            "pending_node_ids": applied
                .pending_nodes
                .iter()
                .map(|node| node.node_id.clone())
                .collect::<Vec<_>>(),
            "plan_digest": plan_digest,
            "settled_scope_ids": applied
                .settled_scopes
                .iter()
                .map(|scope| scope.scope_id.clone())
                .collect::<Vec<_>>(),
            "settled_quota_account_ids": applied.settled_quota_account_ids,
            "source_job_id": applied.source_job.job_id,
            "source_node_id": applied.source_node_id,
            "woken_node_ids": applied
                .woken_nodes
                .iter()
                .map(|node| node.node_id.clone())
                .collect::<Vec<_>>(),
        }),
        65_536,
    )?;
    append_scheduler_event(
        transaction,
        &applied.run.tenant_id,
        &mutations.run_event_id,
        &mutations.run_outbox_id,
        "run",
        run_id,
        applied.run.version,
        Some(run_id),
        "run.controller_advanced",
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &applied.run.tenant_id,
        &mutations.node_event_id,
        &mutations.node_outbox_id,
        "node_execution",
        &applied.source_node_id,
        applied.source_node_version,
        Some(run_id),
        "node.controller_completed",
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &applied.run.tenant_id,
        &mutations.job_event_id,
        &mutations.job_outbox_id,
        "job",
        &applied.source_job.job_id,
        applied.source_job.version,
        Some(run_id),
        "job.controller_completed",
        decision_payload,
    )
    .await?;
    for (activation, slot) in applied.activations.iter().zip(&mutations.activations) {
        let activation_payload = TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "plan_digest": plan_digest,
                "plan_node_key": activation.plan_node_key,
                "source_node_id": applied.source_node_id,
            }),
            65_536,
        )?;
        if let Some(scope_slot) = &slot.scope {
            let scope = applied
                .created_scopes
                .iter()
                .find(|scope| scope.scope_id == scope_slot.scope_instance_id.to_string())
                .ok_or_else(|| {
                    RepositoryError::CorruptRow(
                        "controller activation lost its created Scope".to_owned(),
                    )
                })?;
            append_scheduler_event(
                transaction,
                &applied.run.tenant_id,
                &scope_slot.scope_event_id,
                &scope_slot.scope_outbox_id,
                "scope_instance",
                &scope.scope_id,
                scope.version,
                Some(run_id),
                "scope.opened",
                &activation_payload,
            )
            .await?;
        }
        append_scheduler_event(
            transaction,
            &applied.run.tenant_id,
            &slot.node_event_id,
            &slot.node_outbox_id,
            "node_execution",
            &activation.node_id,
            activation.node_version,
            Some(run_id),
            "node.control_activated",
            &activation_payload,
        )
        .await?;
        append_scheduler_event(
            transaction,
            &applied.run.tenant_id,
            &slot.job_event_id,
            &slot.job_outbox_id,
            "job",
            &activation.job.job_id,
            activation.job.version,
            Some(run_id),
            "job.ready",
            &activation_payload,
        )
        .await?;
    }
    if let Some(rollover) = mutations
        .structural_exit
        .as_ref()
        .and_then(|exit| exit.loop_rollover.as_ref())
    {
        let scope = applied
            .created_scopes
            .iter()
            .find(|scope| scope.scope_id == rollover.scope.scope_instance_id.to_string())
            .ok_or_else(|| {
                RepositoryError::CorruptRow("Loop rollover lost its created Scope".to_owned())
            })?;
        append_scheduler_event(
            transaction,
            &applied.run.tenant_id,
            &rollover.scope.scope_event_id,
            &rollover.scope.scope_outbox_id,
            "scope_instance",
            &scope.scope_id,
            scope.version,
            Some(run_id),
            "scope.opened",
            &common,
        )
        .await?;
    }
    for (pending, slot) in applied.pending_nodes.iter().zip(&mutations.pending_nodes) {
        let pending_payload = TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "plan_digest": plan_digest,
                "plan_node_key": pending.plan_node_key,
                "source_node_id": applied.source_node_id,
            }),
            65_536,
        )?;
        append_scheduler_event(
            transaction,
            &applied.run.tenant_id,
            &slot.node_event_id,
            &slot.node_outbox_id,
            "node_execution",
            &pending.node_id,
            pending.node_version,
            Some(run_id),
            "node.control_pending",
            &pending_payload,
        )
        .await?;
    }
    if !applied.settled_scopes.is_empty() {
        let slot = mutations.structural_exit.as_ref().ok_or_else(|| {
            RepositoryError::CorruptRow(
                "controller Scope settlement lost its event identities".to_owned(),
            )
        })?;
        if applied.settled_scopes.len() != 1 {
            return Err(RepositoryError::CorruptRow(
                "one controller step may settle only its source Scope".to_owned(),
            ));
        }
        let scope = &applied.settled_scopes[0];
        append_scheduler_event(
            transaction,
            &applied.run.tenant_id,
            &slot.scope_closing_event_id,
            &slot.scope_closing_outbox_id,
            "scope_instance",
            &scope.scope_id,
            scope.version - 1,
            Some(run_id),
            "scope.closing",
            &common,
        )
        .await?;
        append_scheduler_event(
            transaction,
            &applied.run.tenant_id,
            &slot.scope_terminal_event_id,
            &slot.scope_terminal_outbox_id,
            "scope_instance",
            &scope.scope_id,
            scope.version,
            Some(run_id),
            "scope.succeeded",
            &common,
        )
        .await?;
    }
    if applied.cancelled_remainders.len() != mutations.remainder_cancellations.len() {
        return Err(RepositoryError::CorruptRow(
            "controller remainder cancellation lost its event slots".to_owned(),
        ));
    }
    for cancellation in &applied.cancelled_remainders {
        let slot = mutations
            .remainder_cancellations
            .iter()
            .find(|slot| slot.expected_scope_id.to_string() == cancellation.scope.scope_id)
            .ok_or_else(|| {
                RepositoryError::CorruptRow(
                    "controller remainder cancellation lost its exact Scope slot".to_owned(),
                )
            })?;
        let payload = TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "job_id": cancellation.job.job_id,
                "node_id": cancellation.node_id,
                "plan_digest": plan_digest,
                "reason_code": cancellation.reason_code,
                "scope_id": cancellation.scope.scope_id,
                "settled_quota_account_ids": cancellation.settled_quota_account_ids,
                "source_node_id": applied.source_node_id,
            }),
            65_536,
        )?;
        if let Some(cancelling_version) = cancellation.node_cancelling_version {
            append_scheduler_event(
                transaction,
                &applied.run.tenant_id,
                &slot.node_cancelling_event_id,
                &slot.node_cancelling_outbox_id,
                "node_execution",
                &cancellation.node_id,
                cancelling_version,
                Some(run_id),
                "node.cancelling",
                &payload,
            )
            .await?;
        }
        append_scheduler_event(
            transaction,
            &applied.run.tenant_id,
            &slot.node_terminal_event_id,
            &slot.node_terminal_outbox_id,
            "node_execution",
            &cancellation.node_id,
            cancellation.node_version,
            Some(run_id),
            "node.cancelled",
            &payload,
        )
        .await?;
        append_scheduler_event(
            transaction,
            &applied.run.tenant_id,
            &slot.scope_closing_event_id,
            &slot.scope_closing_outbox_id,
            "scope_instance",
            &cancellation.scope.scope_id,
            cancellation.scope.version - 1,
            Some(run_id),
            "scope.closing",
            &payload,
        )
        .await?;
        append_scheduler_event(
            transaction,
            &applied.run.tenant_id,
            &slot.scope_terminal_event_id,
            &slot.scope_terminal_outbox_id,
            "scope_instance",
            &cancellation.scope.scope_id,
            cancellation.scope.version,
            Some(run_id),
            "scope.cancelled",
            &payload,
        )
        .await?;
        append_scheduler_event(
            transaction,
            &applied.run.tenant_id,
            &slot.job_terminal_event_id,
            &slot.job_terminal_outbox_id,
            "job",
            &cancellation.job.job_id,
            cancellation.job.version,
            Some(run_id),
            "job.cancelled",
            &payload,
        )
        .await?;
    }
    if !applied.woken_nodes.is_empty() {
        let slot = mutations.pending_wake.as_ref().ok_or_else(|| {
            RepositoryError::CorruptRow("controller Join wake lost its event identities".to_owned())
        })?;
        if applied.woken_nodes.len() != 1 {
            return Err(RepositoryError::CorruptRow(
                "one controller step may wake only one Join".to_owned(),
            ));
        }
        let woken = &applied.woken_nodes[0];
        append_scheduler_event(
            transaction,
            &applied.run.tenant_id,
            &slot.node_event_id,
            &slot.node_outbox_id,
            "node_execution",
            &woken.node_id,
            woken.node_version,
            Some(run_id),
            "node.control_ready",
            &common,
        )
        .await?;
        append_scheduler_event(
            transaction,
            &applied.run.tenant_id,
            &slot.job_event_id,
            &slot.job_outbox_id,
            "job",
            &woken.job.job_id,
            woken.job.version,
            Some(run_id),
            "job.ready",
            &common,
        )
        .await?;
    }
    Ok(())
}

async fn append_failed_orchestration_events(
    transaction: &mut Transaction<'_, Postgres>,
    failed: &FailedOrchestrationJob,
    mutations: &ControllerStepMutationIds,
    plan_digest: &Sha256Digest,
    failure_payload: &TypedPayload,
) -> Result<(), RepositoryError> {
    let run_id = failed.run.run_id.as_str();
    let common = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "cancelled_remainder_scope_ids": failed
                .cancelled_remainders
                .iter()
                .map(|remainder| remainder.scope.scope_id.clone())
                .collect::<Vec<_>>(),
            "controller_code": failed.controller_code,
            "failure_digest": failure_payload.digest,
            "handler_node_ids": failed
                .handler_activations
                .iter()
                .map(|activation| activation.node_id.clone())
                .collect::<Vec<_>>(),
            "plan_digest": plan_digest,
            "settled_quota_account_ids": failed.settled_quota_account_ids,
            "source_job_id": failed.source_job.job_id,
            "source_node_id": failed.source_node_id,
            "woken_node_ids": failed
                .woken_nodes
                .iter()
                .map(|node| node.node_id.clone())
                .collect::<Vec<_>>(),
        }),
        65_536,
    )?;
    append_scheduler_event(
        transaction,
        &failed.run.tenant_id,
        &mutations.run_event_id,
        &mutations.run_outbox_id,
        "run",
        run_id,
        failed.run.version,
        Some(run_id),
        if failed.run.state == RunState::Failed.as_str() {
            "run.failed"
        } else {
            "run.work_failed"
        },
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &failed.run.tenant_id,
        &mutations.node_event_id,
        &mutations.node_outbox_id,
        "node_execution",
        &failed.source_node_id,
        failed.source_node_version,
        Some(run_id),
        "node.failed",
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &failed.run.tenant_id,
        &mutations.job_event_id,
        &mutations.job_outbox_id,
        "job",
        &failed.source_job.job_id,
        failed.source_job.version,
        Some(run_id),
        "job.failed",
        failure_payload,
    )
    .await?;
    if !failed.handler_activations.is_empty() {
        if failed.handler_activations.len() != 1
            || mutations.activations.len() != 1
            || !failed.settled_scopes.is_empty()
        {
            return Err(RepositoryError::CorruptRow(
                "failed ErrorBoundary handler event shape is invalid".to_owned(),
            ));
        }
        let handler = &failed.handler_activations[0];
        let slot = &mutations.activations[0];
        let payload = TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "failure_digest": failure_payload.digest,
                "plan_digest": plan_digest,
                "plan_node_key": handler.plan_node_key,
                "source_node_id": failed.source_node_id,
            }),
            65_536,
        )?;
        append_scheduler_event(
            transaction,
            &failed.run.tenant_id,
            &slot.node_event_id,
            &slot.node_outbox_id,
            "node_execution",
            &handler.node_id,
            handler.node_version,
            Some(run_id),
            "node.control_activated",
            &payload,
        )
        .await?;
        append_scheduler_event(
            transaction,
            &failed.run.tenant_id,
            &slot.job_event_id,
            &slot.job_outbox_id,
            "job",
            &handler.job.job_id,
            handler.job.version,
            Some(run_id),
            "job.ready",
            &payload,
        )
        .await?;
    } else if failed.settled_scopes.len() != 1 {
        return Err(RepositoryError::CorruptRow(
            "failed orchestration must settle exactly one source Scope".to_owned(),
        ));
    } else {
        let scope_slot = mutations.structural_exit.as_ref().ok_or_else(|| {
            RepositoryError::CorruptRow(
                "failed orchestration lost its Scope event identities".to_owned(),
            )
        })?;
        let scope = &failed.settled_scopes[0];
        append_scheduler_event(
            transaction,
            &failed.run.tenant_id,
            &scope_slot.scope_closing_event_id,
            &scope_slot.scope_closing_outbox_id,
            "scope_instance",
            &scope.scope_id,
            scope.version - 1,
            Some(run_id),
            "scope.closing",
            &common,
        )
        .await?;
        append_scheduler_event(
            transaction,
            &failed.run.tenant_id,
            &scope_slot.scope_terminal_event_id,
            &scope_slot.scope_terminal_outbox_id,
            "scope_instance",
            &scope.scope_id,
            scope.version,
            Some(run_id),
            "scope.failed",
            &common,
        )
        .await?;
    }

    if failed.cancelled_remainders.len() != mutations.remainder_cancellations.len() {
        return Err(RepositoryError::CorruptRow(
            "failed orchestration lost remainder-cancellation event slots".to_owned(),
        ));
    }
    for cancellation in &failed.cancelled_remainders {
        let slot = mutations
            .remainder_cancellations
            .iter()
            .find(|slot| slot.expected_scope_id.to_string() == cancellation.scope.scope_id)
            .ok_or_else(|| {
                RepositoryError::CorruptRow(
                    "failed orchestration lost an exact cancellation Scope slot".to_owned(),
                )
            })?;
        let payload = TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "failure_digest": failure_payload.digest,
                "job_id": cancellation.job.job_id,
                "node_id": cancellation.node_id,
                "plan_digest": plan_digest,
                "reason_code": cancellation.reason_code,
                "scope_id": cancellation.scope.scope_id,
                "settled_quota_account_ids": cancellation.settled_quota_account_ids,
                "source_node_id": failed.source_node_id,
            }),
            65_536,
        )?;
        if let Some(cancelling_version) = cancellation.node_cancelling_version {
            append_scheduler_event(
                transaction,
                &failed.run.tenant_id,
                &slot.node_cancelling_event_id,
                &slot.node_cancelling_outbox_id,
                "node_execution",
                &cancellation.node_id,
                cancelling_version,
                Some(run_id),
                "node.cancelling",
                &payload,
            )
            .await?;
        }
        append_scheduler_event(
            transaction,
            &failed.run.tenant_id,
            &slot.node_terminal_event_id,
            &slot.node_terminal_outbox_id,
            "node_execution",
            &cancellation.node_id,
            cancellation.node_version,
            Some(run_id),
            "node.cancelled",
            &payload,
        )
        .await?;
        append_scheduler_event(
            transaction,
            &failed.run.tenant_id,
            &slot.scope_closing_event_id,
            &slot.scope_closing_outbox_id,
            "scope_instance",
            &cancellation.scope.scope_id,
            cancellation.scope.version - 1,
            Some(run_id),
            "scope.closing",
            &payload,
        )
        .await?;
        append_scheduler_event(
            transaction,
            &failed.run.tenant_id,
            &slot.scope_terminal_event_id,
            &slot.scope_terminal_outbox_id,
            "scope_instance",
            &cancellation.scope.scope_id,
            cancellation.scope.version,
            Some(run_id),
            "scope.cancelled",
            &payload,
        )
        .await?;
        append_scheduler_event(
            transaction,
            &failed.run.tenant_id,
            &slot.job_terminal_event_id,
            &slot.job_terminal_outbox_id,
            "job",
            &cancellation.job.job_id,
            cancellation.job.version,
            Some(run_id),
            "job.cancelled",
            &payload,
        )
        .await?;
    }
    if !failed.woken_nodes.is_empty() {
        let slot = mutations.pending_wake.as_ref().ok_or_else(|| {
            RepositoryError::CorruptRow(
                "failed orchestration Join wake lost its event identities".to_owned(),
            )
        })?;
        if failed.woken_nodes.len() != 1 {
            return Err(RepositoryError::CorruptRow(
                "failed orchestration may wake only one Join".to_owned(),
            ));
        }
        let woken = &failed.woken_nodes[0];
        append_scheduler_event(
            transaction,
            &failed.run.tenant_id,
            &slot.node_event_id,
            &slot.node_outbox_id,
            "node_execution",
            &woken.node_id,
            woken.node_version,
            Some(run_id),
            "node.control_ready",
            &common,
        )
        .await?;
        append_scheduler_event(
            transaction,
            &failed.run.tenant_id,
            &slot.job_event_id,
            &slot.job_outbox_id,
            "job",
            &woken.job.job_id,
            woken.job.version,
            Some(run_id),
            "job.ready",
            &common,
        )
        .await?;
    }
    Ok(())
}

async fn append_deferred_orchestration_task_events(
    transaction: &mut Transaction<'_, Postgres>,
    deferred: &DeferredOrchestrationTask,
    mutations: &DeferOrchestrationTaskMutationIds,
    receipt_payload: &TypedPayload,
) -> Result<(), RepositoryError> {
    let run_id = deferred.run.run_id.as_str();
    let common = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "job_id": deferred.job.job_id,
            "settled_quota_account_ids": deferred.settled_quota_account_ids,
            "task_id": deferred.task.task_id,
            "task_kind": deferred.task.task_kind.as_str(),
        }),
        65_536,
    )?;
    append_scheduler_event(
        transaction,
        &deferred.run.tenant_id,
        &mutations.run_event_id,
        &mutations.run_outbox_id,
        "run",
        run_id,
        deferred.run.version,
        Some(run_id),
        "run.task_waiting",
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &deferred.run.tenant_id,
        &mutations.node_event_id,
        &mutations.node_outbox_id,
        "node_execution",
        &deferred.node_id,
        deferred.node_version,
        Some(run_id),
        "node.task_waiting",
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &deferred.run.tenant_id,
        &mutations.task_event_id,
        &mutations.task_outbox_id,
        "interaction",
        &deferred.task.task_id,
        deferred.task.version,
        Some(run_id),
        "interaction.required",
        receipt_payload,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &deferred.run.tenant_id,
        &mutations.job_event_id,
        &mutations.job_outbox_id,
        "job",
        &deferred.job.job_id,
        deferred.job.version,
        Some(run_id),
        "job.task_deferred",
        &common,
    )
    .await?;
    Ok(())
}

async fn append_deferred_orchestration_context_events(
    transaction: &mut Transaction<'_, Postgres>,
    deferred: &DeferredOrchestrationContextQuery,
    mutations: &OrchestrationYieldMutationIds,
) -> Result<(), RepositoryError> {
    let run_id = deferred.run.run_id.as_str();
    let common = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "context_job_id": deferred.context_job.job_id,
            "context_query_id": deferred.query.context_query_id,
            "source_job_id": deferred.source_job.job_id,
            "settled_quota_account_ids": deferred.settled_quota_account_ids,
        }),
        65_536,
    )?;
    append_scheduler_event(
        transaction,
        &deferred.run.tenant_id,
        &mutations.run_event_id,
        &mutations.run_outbox_id,
        "run",
        run_id,
        deferred.run.version,
        Some(run_id),
        "run.context_waiting",
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &deferred.run.tenant_id,
        &mutations.node_event_id,
        &mutations.node_outbox_id,
        "node_execution",
        &deferred.node_id,
        deferred.node_version,
        Some(run_id),
        "node.context_waiting",
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &deferred.run.tenant_id,
        &mutations.job_event_id,
        &mutations.job_outbox_id,
        "job",
        &deferred.source_job.job_id,
        deferred.source_job.version,
        Some(run_id),
        "job.context_deferred",
        &common,
    )
    .await?;
    Ok(())
}

async fn append_deferred_orchestration_model_events(
    transaction: &mut Transaction<'_, Postgres>,
    deferred: &DeferredOrchestrationModelTurn,
    mutations: &OrchestrationYieldMutationIds,
) -> Result<(), RepositoryError> {
    let run_id = deferred.run.run_id.as_str();
    let common = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "model_job_id": deferred.model_job.job_id,
            "model_turn_id": deferred.turn.model_turn_id,
            "source_job_id": deferred.source_job.job_id,
            "settled_quota_account_ids": deferred.settled_quota_account_ids,
        }),
        65_536,
    )?;
    append_scheduler_event(
        transaction,
        &deferred.run.tenant_id,
        &mutations.run_event_id,
        &mutations.run_outbox_id,
        "run",
        run_id,
        deferred.run.version,
        Some(run_id),
        "run.model_waiting",
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &deferred.run.tenant_id,
        &mutations.node_event_id,
        &mutations.node_outbox_id,
        "node_execution",
        &deferred.node_id,
        deferred.node_version,
        Some(run_id),
        "node.model_waiting",
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &deferred.run.tenant_id,
        &mutations.job_event_id,
        &mutations.job_outbox_id,
        "job",
        &deferred.source_job.job_id,
        deferred.source_job.version,
        Some(run_id),
        "job.model_deferred",
        &common,
    )
    .await?;
    Ok(())
}

async fn append_deferred_orchestration_capability_events(
    transaction: &mut Transaction<'_, Postgres>,
    deferred: &DeferredOrchestrationCapabilityInvocation,
    mutations: &OrchestrationYieldMutationIds,
) -> Result<(), RepositoryError> {
    let run_id = deferred.run.run_id.as_str();
    let common = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "capability_job_id": deferred.capability_job.as_ref().map(|job| &job.job_id),
            "invocation_id": deferred.invocation.invocation_id,
            "source_job_id": deferred.source_job.job_id,
            "settled_quota_account_ids": deferred.settled_quota_account_ids,
        }),
        65_536,
    )?;
    append_scheduler_event(
        transaction,
        &deferred.run.tenant_id,
        &mutations.run_event_id,
        &mutations.run_outbox_id,
        "run",
        run_id,
        deferred.run.version,
        Some(run_id),
        "run.capability_waiting",
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &deferred.run.tenant_id,
        &mutations.node_event_id,
        &mutations.node_outbox_id,
        "node_execution",
        &deferred.node_id,
        deferred.node_version,
        Some(run_id),
        "node.capability_waiting",
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &deferred.run.tenant_id,
        &mutations.job_event_id,
        &mutations.job_outbox_id,
        "job",
        &deferred.source_job.job_id,
        deferred.source_job.version,
        Some(run_id),
        "job.capability_deferred",
        &common,
    )
    .await?;
    Ok(())
}

async fn append_deferred_orchestration_child_events(
    transaction: &mut Transaction<'_, Postgres>,
    deferred: &DeferredOrchestrationChildRun,
    mutations: &DeferOrchestrationChildMutationIds,
    receipt_payload: &TypedPayload,
) -> Result<(), RepositoryError> {
    let parent_run_id = deferred.parent_run.run_id.as_str();
    let child_run_id = deferred.child_run.run_id.as_str();
    let common = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "child_link_id": deferred.child_link.child_link_id,
            "child_run_id": deferred.child_run.run_id,
            "parent_job_id": deferred.parent_job.job_id,
            "parent_node_id": deferred.parent_node_id,
            "settled_quota_account_ids": deferred.settled_quota_account_ids,
        }),
        65_536,
    )?;
    if deferred.root_run.run_id != deferred.parent_run.run_id {
        append_scheduler_event(
            transaction,
            &deferred.root_run.tenant_id,
            &mutations.root_run_event_id,
            &mutations.root_run_outbox_id,
            "run",
            &deferred.root_run.run_id,
            deferred.root_run.version,
            Some(&deferred.root_run.run_id),
            "run.descendant_started",
            &common,
        )
        .await?;
    }
    append_scheduler_event(
        transaction,
        &deferred.parent_run.tenant_id,
        &mutations.parent_run_event_id,
        &mutations.parent_run_outbox_id,
        "run",
        parent_run_id,
        deferred.parent_run.version,
        Some(parent_run_id),
        "run.child_waiting",
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &deferred.parent_run.tenant_id,
        &mutations.parent_node_event_id,
        &mutations.parent_node_outbox_id,
        "node_execution",
        &deferred.parent_node_id,
        deferred.parent_node_version,
        Some(parent_run_id),
        "node.child_waiting",
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &deferred.parent_run.tenant_id,
        &mutations.parent_job_event_id,
        &mutations.parent_job_outbox_id,
        "job",
        &deferred.parent_job.job_id,
        deferred.parent_job.version,
        Some(parent_run_id),
        "job.child_deferred",
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &deferred.child_link.tenant_id,
        &mutations.child_link_event_id,
        &mutations.child_link_outbox_id,
        "child_run_link",
        &deferred.child_link.child_link_id,
        deferred.child_link.version,
        Some(parent_run_id),
        "child.started",
        receipt_payload,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &deferred.child_run.tenant_id,
        &mutations.child_run_event_id,
        &mutations.child_run_outbox_id,
        "run",
        child_run_id,
        deferred.child_run.version,
        Some(child_run_id),
        "run.admitted",
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &deferred.child_job.tenant_id,
        &mutations.child_job_event_id,
        &mutations.child_job_outbox_id,
        "job",
        &deferred.child_job.job_id,
        deferred.child_job.version,
        Some(child_run_id),
        "job.ready",
        &common,
    )
    .await?;
    Ok(())
}

async fn append_resolved_orchestration_task_events(
    transaction: &mut Transaction<'_, Postgres>,
    resolved: &ResolvedOrchestrationTask,
    command: &ResolveOrchestrationTask,
) -> Result<(), RepositoryError> {
    let run_id = resolved.run.run_id.as_str();
    let payload = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "response_schema_digest": resolved.task.response_schema_digest,
            "response_value_id": resolved.task.response_value_id,
            "resume_job_id": resolved.job.job_id,
            "state": resolved.task.state.as_str(),
            "task_id": resolved.task.task_id,
        }),
        65_536,
    )?;
    append_command_event_version_for_run(
        transaction,
        &command.audit,
        "interaction",
        &resolved.task.task_id,
        Some(resolved.task.version),
        Some(run_id),
        "interaction.respond",
        &payload,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &resolved.run.tenant_id,
        &command.mutations.run_event_id,
        &command.mutations.run_outbox_id,
        "run",
        run_id,
        resolved.run.version,
        Some(run_id),
        "run.task_resolved",
        &payload,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &resolved.run.tenant_id,
        &command.mutations.node_event_id,
        &command.mutations.node_outbox_id,
        "node_execution",
        &resolved.node_id,
        resolved.node_version,
        Some(run_id),
        "node.task_resolved",
        &payload,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &resolved.run.tenant_id,
        &command.mutations.job_event_id,
        &command.mutations.job_outbox_id,
        "job",
        &resolved.job.job_id,
        resolved.job.version,
        Some(run_id),
        "job.ready_after_task",
        &payload,
    )
    .await?;
    Ok(())
}

async fn append_expired_orchestration_task_events(
    transaction: &mut Transaction<'_, Postgres>,
    resolved: &ResolvedOrchestrationTask,
    slot: &ExpiredOrchestrationTaskSlot,
) -> Result<(), RepositoryError> {
    let run_id = resolved.run.run_id.as_str();
    let payload = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "resume_job_id": resolved.job.job_id,
            "state": resolved.task.state.as_str(),
            "task_id": resolved.task.task_id,
        }),
        65_536,
    )?;
    append_scheduler_event(
        transaction,
        &resolved.run.tenant_id,
        &slot.task_event_id,
        &slot.task_outbox_id,
        "interaction",
        &resolved.task.task_id,
        resolved.task.version,
        Some(run_id),
        "interaction.expired",
        &payload,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &resolved.run.tenant_id,
        &slot.run_event_id,
        &slot.run_outbox_id,
        "run",
        run_id,
        resolved.run.version,
        Some(run_id),
        "run.task_expired",
        &payload,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &resolved.run.tenant_id,
        &slot.node_event_id,
        &slot.node_outbox_id,
        "node_execution",
        &resolved.node_id,
        resolved.node_version,
        Some(run_id),
        "node.task_expired",
        &payload,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &resolved.run.tenant_id,
        &slot.job_event_id,
        &slot.job_outbox_id,
        "job",
        &resolved.job.job_id,
        resolved.job.version,
        Some(run_id),
        "job.ready_after_task",
        &payload,
    )
    .await?;
    Ok(())
}

async fn append_orchestration_wake_events(
    transaction: &mut Transaction<'_, Postgres>,
    woken: &WokenOrchestrationJob,
    mutations: &OrchestrationWakeMutationIds,
    payload: &TypedPayload,
) -> Result<(), RepositoryError> {
    let run_id = woken.run.run_id.as_str();
    append_scheduler_event(
        transaction,
        &woken.run.tenant_id,
        &mutations.run_event_id,
        &mutations.run_outbox_id,
        "run",
        run_id,
        woken.run.version,
        Some(run_id),
        "run.woken",
        payload,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &woken.job.tenant_id,
        &mutations.node_event_id,
        &mutations.node_outbox_id,
        "node_execution",
        &woken.node_id,
        woken.node_version,
        Some(run_id),
        "node.woken",
        payload,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &woken.job.tenant_id,
        &mutations.job_event_id,
        &mutations.job_outbox_id,
        "job",
        &woken.job.job_id,
        woken.job.version,
        Some(run_id),
        "job.woken",
        payload,
    )
    .await?;
    Ok(())
}

async fn append_orchestration_recovery_events(
    transaction: &mut Transaction<'_, Postgres>,
    recovered: &RecoveredOrchestrationJob,
    slot: &ExpiredOrchestrationRecoverySlot,
) -> Result<(), RepositoryError> {
    let run_id = recovered.run.run_id.as_str();
    let payload = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "attempt_count": recovered.job.attempt_no,
            "job_id": recovered.job.job_id,
            "lease_generation": recovered.job.lease_epoch,
            "recovered_state": recovered.job.state,
            "settled_quota_account_ids": recovered.settled_quota_account_ids,
        }),
        65_536,
    )?;
    append_scheduler_event(
        transaction,
        &recovered.run.tenant_id,
        &slot.run_event_id,
        &slot.run_outbox_id,
        "run",
        run_id,
        recovered.run.version,
        Some(run_id),
        "run.work_recovered",
        &payload,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &recovered.run.tenant_id,
        &slot.node_event_id,
        &slot.node_outbox_id,
        "node_execution",
        &recovered.node_id,
        recovered.node_version,
        Some(run_id),
        "node.lease_lost",
        &payload,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &recovered.run.tenant_id,
        &slot.job_event_id,
        &slot.job_outbox_id,
        "job",
        &recovered.job.job_id,
        recovered.job.version,
        Some(run_id),
        "job.lease_lost",
        &payload,
    )
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn mutate_terminal_orchestration_run(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    next_job: &JobProjection,
    parents: &LockedOrchestrationJobParents,
    terminal_state: OrchestrationRunTerminalState,
    result_digest: &Sha256Digest,
    output_value_id: Option<&str>,
    terminal_failure: Option<&Failure>,
    database_now: DateTime<Utc>,
) -> Result<CompletedOrchestrationRun, RepositoryError> {
    let run_state = parents
        .run
        .state
        .parse::<RunState>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let target_run_state = terminal_state
        .as_str()
        .parse::<RunState>()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let target_node_state = terminal_state
        .as_str()
        .parse::<NodeExecutionState>()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let target_scope_state = terminal_state
        .scope_state()
        .parse::<ScopeState>()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    if !run_state.can_transition_to(target_run_state)
        || !NodeExecutionState::Running.can_transition_to(target_node_state)
        || !ScopeState::Open.can_transition_to(ScopeState::Closing)
        || !ScopeState::Closing.can_transition_to(target_scope_state)
    {
        return Err(RepositoryError::Conflict(
            "terminal orchestration state transition",
        ));
    }
    let job = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = $4, version = $5, result_digest = $6,
            worker_id = NULL, lease_token_digest = NULL,
            lease_expires_at = NULL, heartbeat_at = NULL,
            terminal_at = $7, updated_at = $7
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND state = 'running'
        RETURNING *
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&current_job.job_id)
    .bind(current_job.version)
    .bind(next_job.state.as_str())
    .bind(
        i64::try_from(next_job.version)
            .map_err(|_| RepositoryError::InvalidInput("Job version exceeds bigint".to_owned()))?,
    )
    .bind(result_digest.to_string())
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("orchestration Job"))?;
    let job = job_from_row(job)?;

    let node_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = $4, version = version + 1, terminal_at = $5, updated_at = $5
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'node_execution' AND state = 'running'
        RETURNING version
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.node_id)
    .bind(parents.node_version)
    .bind(terminal_state.as_str())
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("orchestration Node"))?;

    let scope_closing_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = 'closing', version = version + 1, updated_at = $4
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'scope_instance' AND state = 'open'
        RETURNING version
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.scope_id)
    .bind(parents.scope_version)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("orchestration Scope"))?;
    let scope_version: i64 = sqlx::query_scalar(
        r#"
        UPDATE insight_platform.run_nodes
        SET state = $4, version = version + 1, terminal_at = $5, updated_at = $5
        WHERE tenant_id = $1 AND node_id = $2 AND version = $3
          AND record_kind = 'scope_instance' AND state = 'closing'
        RETURNING version
        "#,
    )
    .bind(&current_job.tenant_id)
    .bind(&parents.scope_id)
    .bind(scope_closing_version)
    .bind(terminal_state.scope_state())
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("orchestration Scope"))?;

    let mut current_snapshot = parents.run.current.clone();
    current_snapshot.output_value_id = output_value_id.map(str::parse).transpose().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::InvalidInput(failure.to_string())
        },
    )?;
    current_snapshot.failure = terminal_failure.cloned();
    current_snapshot
        .validate(&parents.run.run_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let current_payload = TypedPayload::from_versioned(1, &current_snapshot, 1_048_576)?;
    let run = sqlx::query(
        r#"
        UPDATE insight_platform.runs
        SET state = $4, version = version + 1, active_work_count = active_work_count - 1,
            output_value_id = $5, current_schema_version = $6, current_payload = $7,
            current_payload_digest = $8, terminal_at = $9, updated_at = $9
        WHERE tenant_id = $1 AND run_id = $2 AND version = $3
          AND active_work_count = 1 AND terminal_at IS NULL
        RETURNING *
        "#,
    )
    .bind(&parents.run.tenant_id)
    .bind(&parents.run.run_id)
    .bind(parents.run.version)
    .bind(terminal_state.as_str())
    .bind(output_value_id)
    .bind(current_payload.schema_version)
    .bind(&current_payload.value)
    .bind(&current_payload.digest)
    .bind(database_now)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("orchestration Run"))?;
    Ok(CompletedOrchestrationRun {
        run: run_from_row(run)?,
        job,
        node_id: parents.node_id.clone(),
        scope_id: parents.scope_id.clone(),
        node_version,
        scope_version,
        settled_quota_account_ids: Vec::new(),
    })
}

async fn append_orchestration_terminal_events(
    transaction: &mut Transaction<'_, Postgres>,
    completed: &CompletedOrchestrationRun,
    terminal_state: OrchestrationRunTerminalState,
    mutations: &OrchestrationTerminalMutationIds,
    result_payload: &TypedPayload,
) -> Result<(), RepositoryError> {
    let run_id = completed.run.run_id.as_str();
    let common = TypedPayload::new(
        1,
        &serde_json::json!({
            "job_id": completed.job.job_id,
            "lease_generation": completed.job.lease_epoch,
            "result_payload_digest": result_payload.digest,
            "settled_quota_account_ids": completed.settled_quota_account_ids,
            "terminal_state": terminal_state.as_str(),
        }),
    )?;
    append_scheduler_event(
        transaction,
        &completed.run.tenant_id,
        &mutations.run_event_id,
        &mutations.run_outbox_id,
        "run",
        run_id,
        completed.run.version,
        Some(run_id),
        "run.terminal_committed",
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &completed.run.tenant_id,
        &mutations.node_event_id,
        &mutations.node_outbox_id,
        "node_execution",
        &completed.node_id,
        completed.node_version,
        Some(run_id),
        "node.terminal_committed",
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &completed.run.tenant_id,
        &mutations.scope_closing_event_id,
        &mutations.scope_closing_outbox_id,
        "scope_instance",
        &completed.scope_id,
        completed.scope_version - 1,
        Some(run_id),
        "scope.closing",
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &completed.run.tenant_id,
        &mutations.scope_terminal_event_id,
        &mutations.scope_terminal_outbox_id,
        "scope_instance",
        &completed.scope_id,
        completed.scope_version,
        Some(run_id),
        "scope.terminal_committed",
        &common,
    )
    .await?;
    append_scheduler_event(
        transaction,
        &completed.run.tenant_id,
        &mutations.job_event_id,
        &mutations.job_outbox_id,
        "job",
        &completed.job.job_id,
        completed.job.version,
        Some(run_id),
        "job.terminal_committed",
        result_payload,
    )
    .await?;
    Ok(())
}

async fn load_completed_orchestration_run(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    job_id: &str,
) -> Result<CompletedOrchestrationRun, RepositoryError> {
    let job = load_job_by_text(transaction, tenant_id, job_id).await?;
    require_orchestration_job(&job)?;
    if job.terminal_at.is_none() {
        return Err(RepositoryError::Conflict("orchestration Job terminal"));
    }
    let run_id: ResourceId = job
        .run_id
        .as_deref()
        .ok_or_else(|| RepositoryError::CorruptRow("orchestration Job has no Run".to_owned()))?
        .parse()
        .map_err(|failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        })?;
    let node_id = job
        .node_id
        .clone()
        .ok_or_else(|| RepositoryError::CorruptRow("orchestration Job has no Node".to_owned()))?;
    let tenant: ResourceId =
        tenant_id
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?;
    let run = load_run(transaction, &tenant, &run_id).await?;
    let node = sqlx::query(
        r#"
        SELECT node.scope_id, node.version AS node_version, scope.version AS scope_version
        FROM insight_platform.run_nodes AS node
        JOIN insight_platform.run_nodes AS scope
          ON scope.tenant_id = node.tenant_id AND scope.node_id = node.scope_id
        WHERE node.tenant_id = $1 AND node.node_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(&node_id)
    .fetch_one(&mut **transaction)
    .await?;
    let reservation_id = job.quota_reservation_id.as_deref().ok_or_else(|| {
        RepositoryError::CorruptRow("terminal orchestration Job lost quota evidence".to_owned())
    })?;
    let settled_quota_account_ids = sqlx::query_scalar::<_, String>(
        r#"
        SELECT quota_account_id FROM insight_platform.quota_ledger
        WHERE tenant_id = $1 AND correlation_id = $2 AND entry_kind = 'settle'
        ORDER BY quota_account_id
        "#,
    )
    .bind(tenant_id)
    .bind(reservation_id)
    .fetch_all(&mut **transaction)
    .await?;
    Ok(CompletedOrchestrationRun {
        run,
        job,
        node_id,
        scope_id: node.try_get("scope_id")?,
        node_version: node.try_get("node_version")?,
        scope_version: node.try_get("scope_version")?,
        settled_quota_account_ids,
    })
}

async fn load_yielded_orchestration_job(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    job_id: &str,
    expected_state: JobState,
) -> Result<YieldedOrchestrationJob, RepositoryError> {
    let job = load_job_by_text(transaction, tenant_id, job_id).await?;
    require_orchestration_job(&job)?;
    if job.state != expected_state.as_str() || job.worker_id.is_some() {
        return Err(RepositoryError::Conflict("orchestration Job yield replay"));
    }
    let run_id: ResourceId = job
        .run_id
        .as_deref()
        .ok_or_else(|| RepositoryError::CorruptRow("orchestration Job has no Run".to_owned()))?
        .parse()
        .map_err(|failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        })?;
    let tenant: ResourceId =
        tenant_id
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?;
    let run = load_run(transaction, &tenant, &run_id).await?;
    let node_id = job
        .node_id
        .clone()
        .ok_or_else(|| RepositoryError::CorruptRow("orchestration Job has no Node".to_owned()))?;
    let node_version: i64 = sqlx::query_scalar(
        "SELECT version FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2 AND state = $3",
    )
    .bind(tenant_id)
    .bind(&node_id)
    .bind(expected_state.as_str())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("orchestration Node yield replay"))?;
    let reservation_id = job.quota_reservation_id.as_deref().ok_or_else(|| {
        RepositoryError::CorruptRow("yielded orchestration Job lost quota evidence".to_owned())
    })?;
    let settled_quota_account_ids = sqlx::query_scalar::<_, String>(
        r#"
        SELECT quota_account_id FROM insight_platform.quota_ledger
        WHERE tenant_id = $1 AND correlation_id = $2 AND entry_kind = 'settle'
        ORDER BY quota_account_id
        "#,
    )
    .bind(tenant_id)
    .bind(reservation_id)
    .fetch_all(&mut **transaction)
    .await?;
    if settled_quota_account_ids.is_empty() {
        return Err(RepositoryError::CorruptRow(
            "yielded orchestration Job has no quota settlement".to_owned(),
        ));
    }
    Ok(YieldedOrchestrationJob {
        run,
        job,
        node_id,
        node_version,
        settled_quota_account_ids,
    })
}

#[allow(clippy::too_many_arguments)]
async fn load_replayed_error_boundary_handler(
    transaction: &mut Transaction<'_, Postgres>,
    run: &RunRecord,
    source_job: &JobRecord,
    source_node_id: &str,
    source_scope_id: &str,
    route: &ErrorBoundaryRoute,
    slot: &ControllerActivationSlot,
    plan: &RuntimePlan,
    failure_payload: &TypedPayload,
) -> Result<ControllerActivationRecord, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT parent_node_id, scope_id, plan_node_key, node_kind, state, version,
               payload_schema_version, payload, payload_digest
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
          AND record_kind = 'node_execution'
        "#,
    )
    .bind(&run.tenant_id)
    .bind(&run.run_id)
    .bind(slot.node_execution_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "failed ErrorBoundary handler Node replay",
    ))?;
    let target = plan.node(&route.target)?;
    if row.try_get::<Option<String>, _>("parent_node_id")? != route.boundary_parent_node_id
        || row.try_get::<String, _>("scope_id")? != source_scope_id
        || row.try_get::<String, _>("plan_node_key")? != route.target.as_str()
        || row.try_get::<String, _>("node_kind")? != target.kind().as_str()
    {
        return Err(RepositoryError::Conflict(
            "failed ErrorBoundary handler ownership replay",
        ));
    }
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let stored: StoredErrorBoundaryHandlerPayload =
        decode_typed_payload(&payload, "ErrorBoundary handler Node")?;
    let expected = StoredErrorBoundaryHandlerPayload {
        error_boundary_node_id: route.boundary_node_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?,
        failure_digest: failure_payload
            .digest
            .parse::<Sha256Digest>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        plan_node_key: route.target.clone(),
        plan_source_digest: run.bindings.plan.semantic_digest.clone(),
        required_control_tokens: vec![StoredControllerControlToken {
            source_node_execution_id: source_node_id.parse().map_err(
                |failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                },
            )?,
            source_port: StoredControllerPort::Failure,
        }],
    };
    if stored != expected {
        return Err(RepositoryError::Conflict(
            "failed ErrorBoundary handler payload replay",
        ));
    }

    let handler_node_id = slot.node_execution_id.to_string();
    let job = load_job_by_text(
        transaction,
        &run.tenant_id,
        &slot.orchestration_job_id.to_string(),
    )
    .await?;
    require_orchestration_job(&job)?;
    if job.owner_id != handler_node_id
        || job.node_id.as_deref() != Some(handler_node_id.as_str())
        || job.run_id.as_deref() != Some(run.run_id.as_str())
    {
        return Err(RepositoryError::Conflict(
            "failed ErrorBoundary handler Job replay",
        ));
    }
    let source_job_payload: OrchestrationJobPayload =
        decode_orchestration_job_payload(&source_job.payload)?;
    let handler_job_payload: OrchestrationJobPayload =
        decode_orchestration_job_payload(&job.payload)?;
    let expected_job_payload = OrchestrationJobPayload {
        external_leaf_completion: None,
        bindings_digest: run.bindings.canonical_digest.clone(),
        node_execution_id: slot.node_execution_id.clone(),
        root_scope_id: source_job_payload.root_scope_id,
        retry_backoff_milliseconds: source_job_payload.retry_backoff_milliseconds,
        wake_contract: None,
        convergence_failure: None,
        model_tool_continuation: None,
    };
    if handler_job_payload != expected_job_payload || job.request_digest != job.payload.digest {
        return Err(RepositoryError::Conflict(
            "failed ErrorBoundary handler Job payload replay",
        ));
    }
    Ok(ControllerActivationRecord {
        node_id: handler_node_id,
        plan_node_key: route.target.clone(),
        node_kind: target.kind(),
        node_version: row.try_get("version")?,
        job,
    })
}

async fn load_failed_orchestration_job(
    transaction: &mut Transaction<'_, Postgres>,
    fence: &JobFence,
    plan: &RuntimePlan,
    plan_digest: &Sha256Digest,
    cause: &OrchestrationFailureCause,
    mutations: &ControllerStepMutationIds,
) -> Result<FailedOrchestrationJob, RepositoryError> {
    let source_job = load_job_by_text(transaction, &fence.tenant_id, &fence.job_id).await?;
    require_orchestration_job(&source_job)?;
    if source_job.state != JobState::Failed.as_str() || source_job.terminal_at.is_none() {
        return Err(RepositoryError::Conflict("failed orchestration Job replay"));
    }
    let run_id: ResourceId = source_job
        .run_id
        .as_deref()
        .ok_or_else(|| RepositoryError::CorruptRow("failed Job has no Run".to_owned()))?
        .parse()
        .map_err(|failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        })?;
    let tenant_id: ResourceId = fence.tenant_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let run = load_run(transaction, &tenant_id, &run_id).await?;
    require_exact_runtime_plan(transaction, &run, plan, plan_digest).await?;
    let source_node_id = source_job
        .node_id
        .clone()
        .ok_or_else(|| RepositoryError::CorruptRow("failed Job has no Node".to_owned()))?;
    let source = sqlx::query(
        r#"
        SELECT source.plan_node_key, source.node_kind, source.version, source.scope_id,
               scope.node_kind AS scope_kind, scope.state AS scope_state,
               scope.version AS scope_version
        FROM insight_platform.run_nodes AS source
        JOIN insight_platform.run_nodes AS scope
          ON scope.tenant_id = source.tenant_id
         AND scope.run_id = source.run_id
         AND scope.node_id = source.scope_id
        WHERE source.tenant_id = $1 AND source.run_id = $2 AND source.node_id = $3
          AND source.record_kind = 'node_execution' AND source.state = 'failed'
          AND scope.record_kind = 'scope_instance'
        "#,
    )
    .bind(&fence.tenant_id)
    .bind(&run.run_id)
    .bind(&source_node_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "failed orchestration source replay",
    ))?;
    let plan_node_key = PlanNodeKey::new(source.try_get("plan_node_key")?)?;
    let runtime_node = plan.node(&plan_node_key)?;
    if runtime_node.kind().as_str() != source.try_get::<String, _>("node_kind")? {
        return Err(RepositoryError::Conflict(
            "failed orchestration Plan binding replay",
        ));
    }
    let (failure, controller_code) = derive_orchestration_failure(cause, runtime_node)?;
    let failure_payload = TypedPayload::with_limit(1, &failure, 65_536)?;
    if source_job.result_digest.as_deref() != Some(failure_payload.digest.as_str()) {
        return Err(RepositoryError::Conflict(
            "failed orchestration result replay",
        ));
    }
    let error_route = find_matching_error_boundary(
        transaction,
        &run,
        &source_node_id,
        plan,
        &failure,
        plan.nodes.len(),
    )
    .await?;
    if error_route.is_none()
        && run.state == RunState::Failed.as_str()
        && run.current.failure.as_ref() != Some(&failure)
    {
        return Err(RepositoryError::Conflict("failed Run snapshot replay"));
    }
    let source_scope_kind: String = source.try_get("scope_kind")?;
    let structured_exit =
        error_route.is_none() && matches!(source_scope_kind.as_str(), "parallel_leg" | "map_item");
    validate_failure_mutation_shape(mutations, structured_exit, error_route.as_ref())?;
    let source_scope_id: String = source.try_get("scope_id")?;
    let handler_activations = if let Some(route) = &error_route {
        let slot = mutations.activations.first().ok_or_else(|| {
            RepositoryError::CorruptRow(
                "failed ErrorBoundary replay lost its activation slot".to_owned(),
            )
        })?;
        vec![
            load_replayed_error_boundary_handler(
                transaction,
                &run,
                &source_job,
                &source_node_id,
                &source_scope_id,
                route,
                slot,
                plan,
                &failure_payload,
            )
            .await?,
        ]
    } else {
        Vec::new()
    };
    let settled_scopes = if error_route.is_some() {
        Vec::new()
    } else {
        let scope_state: String = source.try_get("scope_state")?;
        if scope_state != ScopeState::Failed.as_str() {
            return Err(RepositoryError::Conflict(
                "failed orchestration Scope settlement replay",
            ));
        }
        vec![ControllerScopeRecord {
            scope_id: source_scope_id,
            scope_kind: source.try_get("scope_kind")?,
            state: scope_state,
            version: source.try_get("scope_version")?,
        }]
    };

    let mut cancelled_remainders = Vec::with_capacity(mutations.remainder_cancellations.len());
    for slot in &mutations.remainder_cancellations {
        let scope_id = slot.expected_scope_id.to_string();
        let scope = sqlx::query(
            r#"
            SELECT node_kind, state, version
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
              AND record_kind = 'scope_instance'
            "#,
        )
        .bind(&fence.tenant_id)
        .bind(&run.run_id)
        .bind(&scope_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict(
            "failed cancellation Scope replay",
        ))?;
        let cancellation_scope_kind: String = scope.try_get("node_kind")?;
        if cancellation_scope_kind != source_scope_kind
            || !matches!(
                cancellation_scope_kind.as_str(),
                "parallel_leg" | "map_item"
            )
            || scope.try_get::<String, _>("state")? != ScopeState::Cancelled.as_str()
        {
            return Err(RepositoryError::Conflict(
                "failed cancellation Scope terminal replay",
            ));
        }
        let nodes = sqlx::query(
            r#"
            SELECT node_id, version
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND scope_id = $3
              AND record_kind = 'node_execution' AND state = 'cancelled'
            ORDER BY node_id
            "#,
        )
        .bind(&fence.tenant_id)
        .bind(&run.run_id)
        .bind(&scope_id)
        .fetch_all(&mut **transaction)
        .await?;
        if nodes.len() != 1 {
            return Err(RepositoryError::Conflict("failed cancellation Node replay"));
        }
        let node = &nodes[0];
        let node_id: String = node.try_get("node_id")?;
        let job_rows = sqlx::query(
            r#"
            SELECT * FROM insight_platform.jobs
            WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3 AND state = 'cancelled'
            ORDER BY job_id
            "#,
        )
        .bind(&fence.tenant_id)
        .bind(&run.run_id)
        .bind(&node_id)
        .fetch_all(&mut **transaction)
        .await?;
        if job_rows.len() != 1 {
            return Err(RepositoryError::Conflict("failed cancellation Job replay"));
        }
        let job = persisted_job_from_row(job_rows.into_iter().next().ok_or_else(|| {
            RepositoryError::CorruptRow("failed cancellation Job disappeared".to_owned())
        })?)?;
        let settled_quota_account_ids = if let Some(reservation_id) = &job.quota_reservation_id {
            sqlx::query_scalar::<_, String>(
                r#"
                SELECT quota_account_id FROM insight_platform.quota_ledger
                WHERE tenant_id = $1 AND correlation_id = $2 AND entry_kind = 'settle'
                ORDER BY quota_account_id
                "#,
            )
            .bind(&fence.tenant_id)
            .bind(reservation_id)
            .fetch_all(&mut **transaction)
            .await?
        } else {
            Vec::new()
        };
        let node_cancelling_version = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT aggregate_version FROM insight_platform.events
            WHERE tenant_id = $1 AND event_id = $2
              AND aggregate_kind = 'node_execution' AND aggregate_id = $3
              AND event_type = 'node.cancelling'
            "#,
        )
        .bind(&fence.tenant_id)
        .bind(slot.node_cancelling_event_id.to_string())
        .bind(&node_id)
        .fetch_optional(&mut **transaction)
        .await?;
        cancelled_remainders.push(ControllerCancelledRemainderRecord {
            scope: ControllerScopeRecord {
                scope_id,
                scope_kind: cancellation_scope_kind.clone(),
                state: ScopeState::Cancelled.as_str().to_owned(),
                version: scope.try_get("version")?,
            },
            reason_code: if cancellation_scope_kind == "map_item" {
                "map_failure_sibling_cancelled".to_owned()
            } else {
                "join_failure_sibling_cancelled".to_owned()
            },
            node_id,
            node_version: node.try_get("version")?,
            node_cancelling_version,
            job,
            settled_quota_account_ids,
        });
    }
    let mut woken_nodes = Vec::new();
    if let Some(wake) = &mutations.pending_wake {
        if let Some(row) =
            sqlx::query("SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2")
                .bind(&fence.tenant_id)
                .bind(wake.orchestration_job_id.to_string())
                .fetch_optional(&mut **transaction)
                .await?
        {
            let job = persisted_job_from_row(row)?;
            require_orchestration_job(&job)?;
            if job.run_id.as_deref() != Some(run.run_id.as_str()) {
                return Err(RepositoryError::Conflict(
                    "failed orchestration Join Job replay",
                ));
            }
            let node_id = job
                .node_id
                .clone()
                .ok_or_else(|| RepositoryError::CorruptRow("Join Job has no Node".to_owned()))?;
            let node = sqlx::query(
                r#"
                SELECT plan_node_key, node_kind, version
                FROM insight_platform.run_nodes
                WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
                  AND record_kind = 'node_execution'
                "#,
            )
            .bind(&fence.tenant_id)
            .bind(&run.run_id)
            .bind(&node_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(RepositoryError::Conflict(
                "failed orchestration Join Node replay",
            ))?;
            woken_nodes.push(ControllerActivationRecord {
                node_id,
                plan_node_key: PlanNodeKey::new(node.try_get("plan_node_key")?)?,
                node_kind: node
                    .try_get::<String, _>("node_kind")?
                    .parse::<PlanNodeKind>()
                    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
                node_version: node.try_get("version")?,
                job,
            });
        }
    }
    let reservation_id = source_job
        .quota_reservation_id
        .as_deref()
        .ok_or_else(|| RepositoryError::CorruptRow("failed Job lost quota evidence".to_owned()))?;
    let settled_quota_account_ids = sqlx::query_scalar::<_, String>(
        r#"
        SELECT quota_account_id FROM insight_platform.quota_ledger
        WHERE tenant_id = $1 AND correlation_id = $2 AND entry_kind = 'settle'
        ORDER BY quota_account_id
        "#,
    )
    .bind(&fence.tenant_id)
    .bind(reservation_id)
    .fetch_all(&mut **transaction)
    .await?;
    if settled_quota_account_ids.is_empty() {
        return Err(RepositoryError::CorruptRow(
            "failed Job has no quota settlement".to_owned(),
        ));
    }
    Ok(FailedOrchestrationJob {
        run,
        source_node_id,
        source_node_version: source.try_get("version")?,
        source_job,
        failure,
        controller_code,
        handler_activations,
        settled_scopes,
        woken_nodes,
        cancelled_remainders,
        settled_quota_account_ids,
    })
}

async fn load_applied_orchestration_controller_step(
    transaction: &mut Transaction<'_, Postgres>,
    fence: &JobFence,
    mutations: &ControllerStepMutationIds,
) -> Result<AppliedOrchestrationControllerStep, RepositoryError> {
    let slots = &mutations.activations;
    let pending_slots = &mutations.pending_nodes;
    let source_job = load_job_by_text(transaction, &fence.tenant_id, &fence.job_id).await?;
    require_orchestration_job(&source_job)?;
    if source_job.state != JobState::Succeeded.as_str() || source_job.terminal_at.is_none() {
        return Err(RepositoryError::Conflict(
            "orchestration controller step replay",
        ));
    }
    let run_id: ResourceId = source_job
        .run_id
        .as_deref()
        .ok_or_else(|| RepositoryError::CorruptRow("controller Job has no Run".to_owned()))?
        .parse()
        .map_err(|failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        })?;
    let tenant_id: ResourceId = fence.tenant_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let run = load_run(transaction, &tenant_id, &run_id).await?;
    let source_node_id = source_job
        .node_id
        .clone()
        .ok_or_else(|| RepositoryError::CorruptRow("controller Job has no Node".to_owned()))?;
    let source_node_version: i64 = sqlx::query_scalar(
        r#"
        SELECT version FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id = $2
          AND record_kind = 'node_execution' AND state = 'succeeded'
        "#,
    )
    .bind(&fence.tenant_id)
    .bind(&source_node_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "orchestration controller source replay",
    ))?;
    let mut activations = Vec::with_capacity(slots.len());
    let mut created_scopes = Vec::with_capacity(slots.len());
    for slot in slots {
        let row = sqlx::query(
            r#"
            SELECT plan_node_key, node_kind, version, parent_node_id
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND node_id = $2 AND run_id = $3
              AND record_kind = 'node_execution'
            "#,
        )
        .bind(&fence.tenant_id)
        .bind(slot.node_execution_id.to_string())
        .bind(&run.run_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict(
            "orchestration controller activation replay",
        ))?;
        if row
            .try_get::<Option<String>, _>("parent_node_id")?
            .as_deref()
            != Some(source_node_id.as_str())
        {
            return Err(RepositoryError::Conflict(
                "orchestration controller activation owner replay",
            ));
        }
        let job = load_job_by_text(
            transaction,
            &fence.tenant_id,
            &slot.orchestration_job_id.to_string(),
        )
        .await?;
        if job.owner_id != slot.node_execution_id.to_string()
            || job.run_id.as_deref() != Some(run.run_id.as_str())
        {
            return Err(RepositoryError::Conflict(
                "orchestration controller activation Job replay",
            ));
        }
        activations.push(ControllerActivationRecord {
            node_id: slot.node_execution_id.to_string(),
            plan_node_key: PlanNodeKey::new(row.try_get("plan_node_key")?)?,
            node_kind: row
                .try_get::<String, _>("node_kind")?
                .parse::<PlanNodeKind>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
            node_version: row.try_get("version")?,
            job,
        });
        if let Some(scope) = &slot.scope {
            let row = sqlx::query(
                r#"
                SELECT node_kind, state, version, parent_node_id,
                       payload_schema_version, payload, payload_digest
                FROM insight_platform.run_nodes
                WHERE tenant_id = $1 AND node_id = $2 AND run_id = $3
                  AND record_kind = 'scope_instance'
                "#,
            )
            .bind(&fence.tenant_id)
            .bind(scope.scope_instance_id.to_string())
            .bind(&run.run_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(RepositoryError::Conflict(
                "orchestration controller Scope replay",
            ))?;
            let scope_payload =
                payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
            let stored_scope: StoredControllerScopePayload =
                decode_typed_payload(&scope_payload, "controller Scope replay")?;
            let stored_controller_node_id = stored_scope.controller_node_execution_id.to_string();
            if row
                .try_get::<Option<String>, _>("parent_node_id")?
                .as_deref()
                != Some(stored_controller_node_id.as_str())
                || (!matches!(
                    stored_scope.descriptor,
                    StoredControllerScopeDescriptor::MapItem { .. }
                ) && stored_controller_node_id != source_node_id)
            {
                return Err(RepositoryError::Conflict(
                    "orchestration controller Scope owner replay",
                ));
            }
            created_scopes.push(ControllerScopeRecord {
                scope_id: scope.scope_instance_id.to_string(),
                scope_kind: row.try_get("node_kind")?,
                state: row.try_get("state")?,
                version: row.try_get("version")?,
            });
        }
    }
    if let Some(rollover) = mutations
        .structural_exit
        .as_ref()
        .and_then(|exit| exit.loop_rollover.as_ref())
    {
        let row = sqlx::query(
            r#"
            SELECT node_kind, state, version, parent_node_id,
                   payload_schema_version, payload, payload_digest
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND node_id = $2 AND run_id = $3
              AND record_kind = 'scope_instance'
            "#,
        )
        .bind(&fence.tenant_id)
        .bind(rollover.scope.scope_instance_id.to_string())
        .bind(&run.run_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("Loop rollover Scope replay"))?;
        let payload =
            payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
        let stored: StoredControllerScopePayload =
            decode_typed_payload(&payload, "Loop rollover Scope replay")?;
        let state: String = row.try_get("state")?;
        let version: i64 = row.try_get("version")?;
        if row.try_get::<String, _>("node_kind")? != "loop_iteration"
            || !matches!(state.as_str(), "open" | "succeeded")
            || version < 1
            || row.try_get::<Option<String>, _>("parent_node_id")?
                != Some(stored.controller_node_execution_id.to_string())
        {
            return Err(RepositoryError::Conflict(
                "Loop rollover Scope ownership replay",
            ));
        }
        created_scopes.push(ControllerScopeRecord {
            scope_id: rollover.scope.scope_instance_id.to_string(),
            scope_kind: "loop_iteration".to_owned(),
            state,
            version,
        });
    }
    let mut pending_nodes = Vec::with_capacity(pending_slots.len());
    for slot in pending_slots {
        let row = sqlx::query(
            r#"
            SELECT plan_node_key, node_kind, version, parent_node_id,
                   payload_schema_version, payload, payload_digest
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND node_id = $2 AND run_id = $3
              AND record_kind = 'node_execution'
            "#,
        )
        .bind(&fence.tenant_id)
        .bind(slot.node_execution_id.to_string())
        .bind(&run.run_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict(
            "orchestration controller pending Node replay",
        ))?;
        let pending_payload =
            payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
        let stored_pending: StoredPendingControllerNodePayload =
            decode_typed_payload(&pending_payload, "controller pending Node replay")?;
        let stored_controller_node_id = stored_pending.controller_node_execution_id.to_string();
        if row
            .try_get::<Option<String>, _>("parent_node_id")?
            .as_deref()
            != Some(stored_controller_node_id.as_str())
            || stored_pending.plan_node_key.as_str() != row.try_get::<String, _>("plan_node_key")?
        {
            return Err(RepositoryError::Conflict(
                "orchestration controller pending Node owner replay",
            ));
        }
        pending_nodes.push(ControllerPendingNodeRecord {
            node_id: slot.node_execution_id.to_string(),
            plan_node_key: PlanNodeKey::new(row.try_get("plan_node_key")?)?,
            node_kind: row
                .try_get::<String, _>("node_kind")?
                .parse::<PlanNodeKind>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
            node_version: row.try_get("version")?,
        });
    }
    let mut settled_scopes = Vec::new();
    if mutations.structural_exit.is_some() {
        let row = sqlx::query(
            r#"
            SELECT scope.node_id, scope.node_kind, scope.state, scope.version
            FROM insight_platform.run_nodes AS source
            JOIN insight_platform.run_nodes AS scope
              ON scope.tenant_id = source.tenant_id
             AND scope.run_id = source.run_id
             AND scope.node_id = source.scope_id
            WHERE source.tenant_id = $1 AND source.run_id = $2 AND source.node_id = $3
              AND source.record_kind = 'node_execution'
              AND scope.record_kind = 'scope_instance'
            "#,
        )
        .bind(&fence.tenant_id)
        .bind(&run.run_id)
        .bind(&source_node_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict(
            "orchestration controller structural replay",
        ))?;
        let state: String = row.try_get("state")?;
        let scope_kind: String = row.try_get("node_kind")?;
        if !matches!(
            scope_kind.as_str(),
            "parallel_leg" | "loop_iteration" | "map_item"
        ) || state != ScopeState::Succeeded.as_str()
        {
            return Err(RepositoryError::Conflict(
                "orchestration controller Scope settlement replay",
            ));
        }
        settled_scopes.push(ControllerScopeRecord {
            scope_id: row.try_get("node_id")?,
            scope_kind,
            state,
            version: row.try_get("version")?,
        });
    }
    let mut cancelled_remainders = Vec::with_capacity(mutations.remainder_cancellations.len());
    for slot in &mutations.remainder_cancellations {
        let scope_id = slot.expected_scope_id.to_string();
        let scope = sqlx::query(
            r#"
            SELECT node_kind, state, version
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
              AND record_kind = 'scope_instance'
            "#,
        )
        .bind(&fence.tenant_id)
        .bind(&run.run_id)
        .bind(&scope_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict(
            "controller remainder Scope replay",
        ))?;
        let cancellation_scope_kind: String = scope.try_get("node_kind")?;
        if !matches!(
            cancellation_scope_kind.as_str(),
            "parallel_leg" | "map_item"
        ) || scope.try_get::<String, _>("state")? != ScopeState::Cancelled.as_str()
        {
            return Err(RepositoryError::Conflict(
                "controller remainder Scope terminal replay",
            ));
        }
        let nodes = sqlx::query(
            r#"
            SELECT node_id, version
            FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND run_id = $2 AND scope_id = $3
              AND record_kind = 'node_execution' AND state = 'cancelled'
            ORDER BY node_id
            "#,
        )
        .bind(&fence.tenant_id)
        .bind(&run.run_id)
        .bind(&scope_id)
        .fetch_all(&mut **transaction)
        .await?;
        if nodes.len() != 1 {
            return Err(RepositoryError::Conflict(
                "controller remainder Node replay",
            ));
        }
        let node = &nodes[0];
        let node_id: String = node.try_get("node_id")?;
        let job_rows = sqlx::query(
            r#"
            SELECT * FROM insight_platform.jobs
            WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3 AND state = 'cancelled'
            ORDER BY job_id
            "#,
        )
        .bind(&fence.tenant_id)
        .bind(&run.run_id)
        .bind(&node_id)
        .fetch_all(&mut **transaction)
        .await?;
        if job_rows.len() != 1 {
            return Err(RepositoryError::Conflict("controller remainder Job replay"));
        }
        let job = persisted_job_from_row(job_rows.into_iter().next().ok_or_else(|| {
            RepositoryError::CorruptRow("controller remainder Job disappeared".to_owned())
        })?)?;
        let settled_quota_account_ids = if let Some(reservation_id) = &job.quota_reservation_id {
            sqlx::query_scalar::<_, String>(
                r#"
                SELECT quota_account_id FROM insight_platform.quota_ledger
                WHERE tenant_id = $1 AND correlation_id = $2 AND entry_kind = 'settle'
                ORDER BY quota_account_id
                "#,
            )
            .bind(&fence.tenant_id)
            .bind(reservation_id)
            .fetch_all(&mut **transaction)
            .await?
        } else {
            Vec::new()
        };
        let node_cancelling_version = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT aggregate_version FROM insight_platform.events
            WHERE tenant_id = $1 AND event_id = $2
              AND aggregate_kind = 'node_execution' AND aggregate_id = $3
              AND event_type = 'node.cancelling'
            "#,
        )
        .bind(&fence.tenant_id)
        .bind(slot.node_cancelling_event_id.to_string())
        .bind(&node_id)
        .fetch_optional(&mut **transaction)
        .await?;
        cancelled_remainders.push(ControllerCancelledRemainderRecord {
            scope: ControllerScopeRecord {
                scope_id,
                scope_kind: cancellation_scope_kind.clone(),
                state: ScopeState::Cancelled.as_str().to_owned(),
                version: scope.try_get("version")?,
            },
            reason_code: if cancellation_scope_kind == "map_item" {
                "map_failure_sibling_cancelled".to_owned()
            } else {
                "join_remainder_cancelled".to_owned()
            },
            node_id,
            node_version: node.try_get("version")?,
            node_cancelling_version,
            job,
            settled_quota_account_ids,
        });
    }
    let mut woken_nodes = Vec::new();
    if let Some(wake) = &mutations.pending_wake {
        let row =
            sqlx::query("SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2")
                .bind(&fence.tenant_id)
                .bind(wake.orchestration_job_id.to_string())
                .fetch_optional(&mut **transaction)
                .await?;
        if let Some(row) = row {
            let job = persisted_job_from_row(row)?;
            require_orchestration_job(&job)?;
            if job.run_id.as_deref() != Some(run.run_id.as_str()) {
                return Err(RepositoryError::Conflict(
                    "orchestration controller Join Job replay",
                ));
            }
            let node_id = job
                .node_id
                .clone()
                .ok_or_else(|| RepositoryError::CorruptRow("Join Job has no Node".to_owned()))?;
            let node = sqlx::query(
                r#"
                SELECT plan_node_key, node_kind, version
                FROM insight_platform.run_nodes
                WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
                  AND record_kind = 'node_execution'
                "#,
            )
            .bind(&fence.tenant_id)
            .bind(&run.run_id)
            .bind(&node_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(RepositoryError::Conflict(
                "orchestration controller Join Node replay",
            ))?;
            woken_nodes.push(ControllerActivationRecord {
                node_id,
                plan_node_key: PlanNodeKey::new(node.try_get("plan_node_key")?)?,
                node_kind: node
                    .try_get::<String, _>("node_kind")?
                    .parse::<PlanNodeKind>()
                    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
                node_version: node.try_get("version")?,
                job,
            });
        }
    }
    let reservation_id = source_job.quota_reservation_id.as_deref().ok_or_else(|| {
        RepositoryError::CorruptRow("controller Job lost quota evidence".to_owned())
    })?;
    let settled_quota_account_ids = sqlx::query_scalar::<_, String>(
        r#"
        SELECT quota_account_id FROM insight_platform.quota_ledger
        WHERE tenant_id = $1 AND correlation_id = $2 AND entry_kind = 'settle'
        ORDER BY quota_account_id
        "#,
    )
    .bind(&fence.tenant_id)
    .bind(reservation_id)
    .fetch_all(&mut **transaction)
    .await?;
    if settled_quota_account_ids.is_empty() {
        return Err(RepositoryError::CorruptRow(
            "controller Job has no quota settlement".to_owned(),
        ));
    }
    Ok(AppliedOrchestrationControllerStep {
        run,
        source_node_id,
        source_node_version,
        source_job,
        activations,
        created_scopes,
        pending_nodes,
        settled_scopes,
        woken_nodes,
        cancelled_remainders,
        settled_quota_account_ids,
    })
}

async fn load_deferred_orchestration_task(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    job_id: &str,
    task_id: &str,
) -> Result<DeferredOrchestrationTask, RepositoryError> {
    let job = load_job_by_text(transaction, tenant_id, job_id).await?;
    require_orchestration_job(&job)?;
    if job.state != JobState::Succeeded.as_str() || job.terminal_at.is_none() {
        return Err(RepositoryError::Conflict(
            "orchestration Task deferral replay",
        ));
    }
    let task = load_task_by_text(transaction, tenant_id, task_id).await?;
    if task.owner_id != job.owner_id || task.run_id != job.run_id || task.node_id != job.node_id {
        return Err(RepositoryError::Conflict(
            "orchestration Task deferral replay",
        ));
    }
    let run_id: ResourceId = job
        .run_id
        .as_deref()
        .ok_or_else(|| RepositoryError::CorruptRow("orchestration Job has no Run".to_owned()))?
        .parse()
        .map_err(|failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        })?;
    let tenant: ResourceId =
        tenant_id
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?;
    let run = load_run(transaction, &tenant, &run_id).await?;
    let node_id = job
        .node_id
        .clone()
        .ok_or_else(|| RepositoryError::CorruptRow("orchestration Job has no Node".to_owned()))?;
    let node_version: i64 = sqlx::query_scalar(
        "SELECT version FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
    )
    .bind(tenant_id)
    .bind(&node_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "orchestration Task deferral replay",
    ))?;
    let reservation_id = job.quota_reservation_id.as_deref().ok_or_else(|| {
        RepositoryError::CorruptRow("deferred orchestration Job lost quota evidence".to_owned())
    })?;
    let settled_quota_account_ids = sqlx::query_scalar::<_, String>(
        r#"
        SELECT quota_account_id FROM insight_platform.quota_ledger
        WHERE tenant_id = $1 AND correlation_id = $2 AND entry_kind = 'settle'
        ORDER BY quota_account_id
        "#,
    )
    .bind(tenant_id)
    .bind(reservation_id)
    .fetch_all(&mut **transaction)
    .await?;
    Ok(DeferredOrchestrationTask {
        run,
        node_id,
        node_version,
        job,
        task,
        settled_quota_account_ids,
    })
}

async fn load_deferred_orchestration_capability_invocation(
    transaction: &mut Transaction<'_, Postgres>,
    fence: &JobFence,
    invocation_id: &ResourceId,
) -> Result<DeferredOrchestrationCapabilityInvocation, RepositoryError> {
    let source_job = load_job_by_text(transaction, &fence.tenant_id, &fence.job_id).await?;
    require_orchestration_job(&source_job)?;
    if source_job.state != JobState::Succeeded.as_str() || source_job.terminal_at.is_none() {
        return Err(RepositoryError::Conflict(
            "orchestration Capability deferral replay",
        ));
    }
    let tenant_id: ResourceId = fence.tenant_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let invocation = crate::invocation_repository::load_capability_invocation(
        transaction,
        &tenant_id,
        invocation_id,
        false,
    )
    .await?;
    if invocation.node_execution_id.to_string() != source_job.owner_id
        || invocation.run_id.to_string() != source_job.run_id.as_deref().unwrap_or_default()
    {
        return Err(RepositoryError::Conflict(
            "orchestration Capability deferral replay",
        ));
    }
    let capability_job = if let Some(job_id) = invocation.payload.current_job_id.as_ref() {
        Some(
            crate::capability_execution_repository::load_capability_job(
                transaction,
                &tenant_id,
                job_id,
                false,
            )
            .await?,
        )
    } else {
        None
    };
    let run = load_run(transaction, &tenant_id, &invocation.run_id).await?;
    let node_id = invocation.node_execution_id.to_string();
    let node_version: i64 = sqlx::query_scalar(
        r#"
        SELECT version FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id = $2
          AND record_kind = 'node_execution' AND state = 'waiting'
        "#,
    )
    .bind(&fence.tenant_id)
    .bind(&node_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "orchestration Capability deferral replay",
    ))?;
    let reservation_id = source_job.quota_reservation_id.as_deref().ok_or_else(|| {
        RepositoryError::CorruptRow("deferred orchestration Job lost quota evidence".to_owned())
    })?;
    let settled_quota_account_ids = sqlx::query_scalar::<_, String>(
        r#"
        SELECT quota_account_id FROM insight_platform.quota_ledger
        WHERE tenant_id = $1 AND correlation_id = $2 AND entry_kind = 'settle'
        ORDER BY quota_account_id
        "#,
    )
    .bind(&fence.tenant_id)
    .bind(reservation_id)
    .fetch_all(&mut **transaction)
    .await?;
    Ok(DeferredOrchestrationCapabilityInvocation {
        run,
        node_id,
        node_version,
        source_job,
        invocation,
        capability_job,
        settled_quota_account_ids,
    })
}

async fn load_deferred_orchestration_context_query(
    transaction: &mut Transaction<'_, Postgres>,
    fence: &JobFence,
    context_query_id: &ResourceId,
    context_job_id: &ResourceId,
    limits: ContextQueryLimits,
) -> Result<DeferredOrchestrationContextQuery, RepositoryError> {
    use crate::context_query_repository::{load_context_job, load_context_query};

    let source_job = load_job_by_text(transaction, &fence.tenant_id, &fence.job_id).await?;
    require_orchestration_job(&source_job)?;
    if source_job.state != JobState::Succeeded.as_str() || source_job.terminal_at.is_none() {
        return Err(RepositoryError::Conflict(
            "orchestration Context deferral replay",
        ));
    }
    let tenant_id: ResourceId = fence.tenant_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let query =
        load_context_query(transaction, &tenant_id, context_query_id, false, limits).await?;
    let context_job = load_context_job(transaction, &tenant_id, context_job_id, false).await?;
    if query.node_execution_id.to_string() != source_job.owner_id
        || query.run_id.to_string() != source_job.run_id.as_deref().unwrap_or_default()
        || query.payload.current_job_id.as_ref() != Some(context_job_id)
        || context_job.owner_id != context_query_id.to_string()
    {
        return Err(RepositoryError::Conflict(
            "orchestration Context deferral replay",
        ));
    }
    let run = load_run(transaction, &tenant_id, &query.run_id).await?;
    let node_id = query.node_execution_id.to_string();
    let node_version: i64 = sqlx::query_scalar(
        r#"
        SELECT version FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id = $2
          AND record_kind = 'node_execution' AND state = 'waiting'
        "#,
    )
    .bind(&fence.tenant_id)
    .bind(&node_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "orchestration Context deferral replay",
    ))?;
    let reservation_id = source_job.quota_reservation_id.as_deref().ok_or_else(|| {
        RepositoryError::CorruptRow("deferred orchestration Job lost quota evidence".to_owned())
    })?;
    let settled_quota_account_ids = sqlx::query_scalar::<_, String>(
        r#"
        SELECT quota_account_id FROM insight_platform.quota_ledger
        WHERE tenant_id = $1 AND correlation_id = $2 AND entry_kind = 'settle'
        ORDER BY quota_account_id
        "#,
    )
    .bind(&fence.tenant_id)
    .bind(reservation_id)
    .fetch_all(&mut **transaction)
    .await?;
    Ok(DeferredOrchestrationContextQuery {
        run,
        node_id,
        node_version,
        source_job,
        query,
        context_job,
        settled_quota_account_ids,
    })
}

async fn load_deferred_orchestration_child_run(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    parent_job_id: &str,
    child_link_id: &str,
) -> Result<DeferredOrchestrationChildRun, RepositoryError> {
    let parent_job = load_job_by_text(transaction, tenant_id, parent_job_id).await?;
    require_orchestration_job(&parent_job)?;
    if parent_job.state != JobState::Succeeded.as_str() || parent_job.terminal_at.is_none() {
        return Err(RepositoryError::Conflict(
            "orchestration child deferral replay",
        ));
    }
    let child_link = load_child_run_link_by_id(transaction, tenant_id, child_link_id).await?;
    if Some(child_link.parent_run_id.as_str()) != parent_job.run_id.as_deref()
        || Some(child_link.parent_node_execution_id.as_str()) != parent_job.node_id.as_deref()
        || child_link.payload.parent_attempt_ordinal
            != u16::try_from(parent_job.attempt_no).map_err(|_| {
                RepositoryError::CorruptRow("orchestration attempt exceeds u16".to_owned())
            })?
    {
        return Err(RepositoryError::Conflict(
            "orchestration child deferral replay",
        ));
    }
    let tenant: ResourceId =
        tenant_id
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?;
    let parent_run_id: ResourceId = child_link.parent_run_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let child_run_id: ResourceId = child_link.child_run_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let parent_run = load_run(transaction, &tenant, &parent_run_id).await?;
    let child_run = load_run(transaction, &tenant, &child_run_id).await?;
    if child_run.parent_run_id.as_deref() != Some(child_link.parent_run_id.as_str())
        || child_run.parent_node_id.as_deref() != Some(child_link.parent_node_execution_id.as_str())
        || child_run.deadline != child_link.deadline
        || child_run.current.ancestry.parent_child_link_id.as_ref()
            != Some(&child_link.child_link_id.parse().map_err(
                |failure: insight_platform_contracts::ResourceIdError| {
                    RepositoryError::CorruptRow(failure.to_string())
                },
            )?)
    {
        return Err(RepositoryError::Conflict(
            "orchestration child relation replay",
        ));
    }
    let root_run_id: ResourceId = child_run.root_run_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let root_run = if root_run_id == parent_run_id {
        parent_run.clone()
    } else {
        load_run(transaction, &tenant, &root_run_id).await?
    };
    let child_job = load_job(transaction, &tenant, &child_link.child_orchestration_job_id).await?;
    require_orchestration_job(&child_job)?;
    if child_job.run_id.as_deref() != Some(child_link.child_run_id.as_str())
        || child_job.deadline != child_run.deadline
        || child_job.node_id.as_deref()
            != Some(
                child_link
                    .child_entry_node_execution_id
                    .to_string()
                    .as_str(),
            )
    {
        return Err(RepositoryError::Conflict("orchestration child Job replay"));
    }
    let parent_node_version: i64 = sqlx::query_scalar(
        "SELECT version FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
    )
    .bind(tenant_id)
    .bind(&child_link.parent_node_execution_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("parent NodeExecution"))?;
    let reservation_id = parent_job.quota_reservation_id.as_deref().ok_or_else(|| {
        RepositoryError::CorruptRow("deferred orchestration Job lost quota evidence".to_owned())
    })?;
    let settled_quota_account_ids = sqlx::query_scalar::<_, String>(
        r#"
        SELECT quota_account_id FROM insight_platform.quota_ledger
        WHERE tenant_id = $1 AND correlation_id = $2 AND entry_kind = 'settle'
        ORDER BY quota_account_id
        "#,
    )
    .bind(tenant_id)
    .bind(reservation_id)
    .fetch_all(&mut **transaction)
    .await?;
    if settled_quota_account_ids.is_empty() {
        return Err(RepositoryError::CorruptRow(
            "deferred orchestration child has no quota settlement".to_owned(),
        ));
    }
    Ok(DeferredOrchestrationChildRun {
        root_run,
        parent_run,
        parent_node_id: child_link.parent_node_execution_id.clone(),
        parent_node_version,
        parent_job,
        child_link,
        child_run,
        child_job,
        settled_quota_account_ids,
    })
}

async fn load_task_source_orchestration_job(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    node_id: &str,
) -> Result<JobRecord, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT * FROM insight_platform.jobs
        WHERE tenant_id = $1 AND work_class = 'orchestration'
          AND owner_kind = 'node_execution' AND owner_id = $2
          AND state = 'succeeded' AND terminal_at IS NOT NULL
        ORDER BY terminal_at DESC, job_id DESC
        LIMIT 1
        "#,
    )
    .bind(tenant_id)
    .bind(node_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Task source orchestration Job"))?;
    let job = persisted_job_from_row(row)?;
    require_orchestration_job(&job)?;
    Ok(job)
}

async fn load_resolved_orchestration_task(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    task_id: &str,
    resume_job_id: &str,
) -> Result<ResolvedOrchestrationTask, RepositoryError> {
    let task = load_task_by_text(transaction, tenant_id, task_id).await?;
    if !matches!(
        task.state,
        TaskState::Responded | TaskState::Declined | TaskState::Cancelled
    ) || task.responded_at.is_none()
    {
        return Err(RepositoryError::Conflict("Task response replay"));
    }
    let job = load_job_by_text(transaction, tenant_id, resume_job_id).await?;
    require_orchestration_job(&job)?;
    if job.owner_id != task.owner_id || job.run_id != task.run_id || job.node_id != task.node_id {
        return Err(RepositoryError::Conflict("Task response replay"));
    }
    let run_id: ResourceId = task
        .run_id
        .as_deref()
        .ok_or_else(|| RepositoryError::CorruptRow("Task has no Run".to_owned()))?
        .parse()
        .map_err(|failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        })?;
    let tenant: ResourceId =
        tenant_id
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?;
    let run = load_run(transaction, &tenant, &run_id).await?;
    let node_id = task
        .node_id
        .clone()
        .ok_or_else(|| RepositoryError::CorruptRow("Task has no Node".to_owned()))?;
    let node_version: i64 = sqlx::query_scalar(
        "SELECT version FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2",
    )
    .bind(tenant_id)
    .bind(&node_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Task response replay"))?;
    Ok(ResolvedOrchestrationTask {
        run,
        node_id,
        node_version,
        task,
        job,
    })
}

async fn load_woken_orchestration_job(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    job_id: &str,
) -> Result<WokenOrchestrationJob, RepositoryError> {
    let job = load_job_by_text(transaction, tenant_id, job_id).await?;
    require_orchestration_job(&job)?;
    if job.state != JobState::Ready.as_str()
        || job.wake_kind.is_some()
        || job.wake_state.is_some()
        || job.wake_generation != 0
    {
        return Err(RepositoryError::Conflict("orchestration Job wake replay"));
    }
    let node_id = job
        .node_id
        .clone()
        .ok_or_else(|| RepositoryError::CorruptRow("orchestration Job has no Node".to_owned()))?;
    let node_version: i64 = sqlx::query_scalar(
        "SELECT version FROM insight_platform.run_nodes WHERE tenant_id = $1 AND node_id = $2 AND state = 'ready'",
    )
    .bind(tenant_id)
    .bind(&node_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("orchestration Node wake replay"))?;
    let run_id: ResourceId = job
        .run_id
        .as_deref()
        .ok_or_else(|| RepositoryError::CorruptRow("orchestration Job has no Run".to_owned()))?
        .parse()
        .map_err(|failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        })?;
    let tenant_id: ResourceId =
        tenant_id
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?;
    let run = load_run(transaction, &tenant_id, &run_id).await?;
    Ok(WokenOrchestrationJob {
        run,
        job,
        node_id,
        node_version,
    })
}

#[derive(Debug, Clone)]
struct OrchestrationCandidate {
    job: JobRecord,
    run_id: String,
    run_version: i64,
    principal_id: String,
    agent_deployment_id: String,
    node_id: String,
    node_version: i64,
    scope_id: String,
    scope_version: i64,
}

#[derive(Debug, Clone)]
struct TerminalChildRunCandidate {
    tenant_id: String,
    child_link_id: String,
    parent_run_id: String,
    child_run_id: String,
    root_run_id: String,
}

impl OrchestrationCandidate {
    fn lock_key(&self) -> (&str, &str, &str, &str) {
        (
            self.job.tenant_id.as_str(),
            self.run_id.as_str(),
            self.node_id.as_str(),
            self.job.job_id.as_str(),
        )
    }
}

#[derive(Debug, Clone)]
struct LockedRunParent {
    tenant_id: String,
    run_id: String,
    version: i64,
}

#[derive(Debug, Clone)]
struct LockedNodeParent {
    tenant_id: String,
    node_id: String,
    version: i64,
    state: String,
    record_kind: String,
}

struct LockedOrchestrationParents {
    runs: BTreeMap<String, LockedRunParent>,
    nodes: BTreeMap<String, LockedNodeParent>,
}

fn scheduler_priority_from_database(priority: i16) -> Result<SchedulerPriority, RepositoryError> {
    match priority {
        -1 => Ok(SchedulerPriority::Low),
        0 => Ok(SchedulerPriority::Normal),
        1 => Ok(SchedulerPriority::High),
        2 => Ok(SchedulerPriority::CriticalControl),
        _ => Err(RepositoryError::CorruptRow(
            "Job has an invalid priority".to_owned(),
        )),
    }
}

const fn scheduler_priority_to_database(priority: SchedulerPriority) -> i16 {
    match priority {
        SchedulerPriority::Low => -1,
        SchedulerPriority::Normal => 0,
        SchedulerPriority::High => 1,
        SchedulerPriority::CriticalControl => 2,
    }
}

async fn enumerate_orchestration_candidates(
    transaction: &mut Transaction<'_, Postgres>,
    database_now: DateTime<Utc>,
    candidate_job_ids: &[String],
    capabilities: &insight_platform_contracts::WorkerExecutionCapabilities,
    limits: SchedulerHardLimits,
    diagnostics: &mut Vec<insight_platform_jobs::store::SafeScanDiagnostic>,
    scope_limits: ScopeEnvironmentLimits,
) -> Result<Vec<OrchestrationCandidate>, RepositoryError> {
    let maximum_rows = i64::from(limits.maximum_tenants)
        .checked_mul(i64::from(limits.maximum_window_per_tenant))
        .ok_or_else(|| {
            RepositoryError::InvalidInput("scheduler candidate bound overflowed".to_owned())
        })?;
    let rows = sqlx::query(
        r#"
        WITH eligible AS (
            SELECT job.tenant_id, job.job_id,
                   row_number() OVER (
                       PARTITION BY job.tenant_id
                       ORDER BY job.priority DESC,
                                COALESCE(job.retry_at, job.scheduled_at),
                                node.enqueue_round,
                                job.job_id
                   ) AS tenant_rank
            FROM insight_platform.jobs AS job
            JOIN insight_platform.tenants AS tenant
              ON tenant.tenant_id = job.tenant_id AND tenant.state = 'active'
            JOIN insight_platform.runs AS run
              ON run.tenant_id = job.tenant_id AND run.run_id = job.run_id
            JOIN insight_platform.run_nodes AS node
              ON node.tenant_id = job.tenant_id AND node.node_id = job.node_id
            JOIN insight_platform.run_nodes AS scope
              ON scope.tenant_id = node.tenant_id AND scope.node_id = node.scope_id
            WHERE job.work_class = 'orchestration' AND job.job_id = ANY($2)
              AND job.execution_requirement_family='program'
              AND (job.execution_semantic_identity,job.execution_ir_abi_version) IN (
                  SELECT capability->>'program_semantic_identity',(capability->>'ir_abi_version')::integer
                  FROM jsonb_array_elements($5::jsonb) capability WHERE capability->>'family'='program'
              )
              AND job.owner_kind = 'node_execution' AND job.owner_id = node.node_id
              AND job.state IN ('ready', 'retry_scheduled')
              AND job.terminal_at IS NULL AND job.worker_id IS NULL
              AND (
                  job.attempt_no < job.attempt_limit
                  OR (
                      job.state = 'ready'
                      AND job.attempt_no > 0
                      AND job.attempt_no <= job.attempt_limit
                      -- Durable wait preserves the current physical attempt's
                      -- start time; ordinary retry clears it before becoming
                      -- ready. The typed timestamp proves continuation without
                      -- retaining a consumed wake contract.
                      AND job.started_at IS NOT NULL
                  )
              )
              AND job.scheduled_at <= $1 AND (job.retry_at IS NULL OR job.retry_at <= $1)
              AND job.deadline > $1
              AND job.priority BETWEEN -1 AND 1
              AND run.state IN ('queued', 'running') AND run.terminal_at IS NULL
              AND run.deadline > $1
              AND run.current_payload #>> '{control,pause_requested}' = 'false'
              AND run.current_payload #> '{control,cancel_requested_at}' = 'null'::jsonb
              AND run.current_payload #> '{control,timeout_requested_at}' = 'null'::jsonb
              AND node.record_kind = 'node_execution' AND node.state = 'ready'
              AND node.terminal_at IS NULL AND node.deadline > $1
              AND node.enqueue_round IS NOT NULL
              AND scope.record_kind = 'scope_instance' AND scope.state = 'open'
              AND scope.terminal_at IS NULL AND scope.deadline > $1
              -- JSON null is a non-NULL SQL value. Only admit tenants whose closed
              -- TenantConfig contains the exact Scheduling binding object; otherwise
              -- one unbound tenant can poison the complete cross-tenant window.
              AND (
                  jsonb_typeof(tenant.config -> 'scheduling_policy') = 'object'
                  OR (job.payload_schema_version = 2 AND (
                      jsonb_typeof(job.payload -> 'external_leaf_completion') = 'object'
                      OR jsonb_typeof(job.payload -> 'convergence_failure') = 'object'
                  ))
              )
        )
        SELECT job.*,
               run.version AS scheduler_run_version,
               run.principal_id AS scheduler_principal_id,
               run.agent_deployment_id AS scheduler_agent_deployment_id,
               node.version AS scheduler_node_version,
               node.plan_node_key AS scheduler_node_key,node.node_kind AS scheduler_node_kind,
               node.payload_schema_version AS scheduler_node_payload_version,
               node.payload AS scheduler_node_payload,node.payload_digest AS scheduler_node_payload_digest,
               node.scope_id AS scheduler_scope_id,
               scope.version AS scheduler_scope_version
        FROM eligible
        JOIN insight_platform.jobs AS job
          ON job.tenant_id = eligible.tenant_id AND job.job_id = eligible.job_id
        JOIN insight_platform.runs AS run
          ON run.tenant_id = job.tenant_id AND run.run_id = job.run_id
        JOIN insight_platform.run_nodes AS node
          ON node.tenant_id = job.tenant_id AND node.node_id = job.node_id
        JOIN insight_platform.run_nodes AS scope
          ON scope.tenant_id = node.tenant_id AND scope.node_id = node.scope_id
        WHERE eligible.tenant_rank <= $3
        ORDER BY job.tenant_id, eligible.tenant_rank, job.job_id
        LIMIT $4
        "#,
    )
    .bind(database_now)
    .bind(candidate_job_ids)
    .bind(i64::from(limits.maximum_window_per_tenant))
    .bind(maximum_rows)
    .bind(serde_json::to_value(&capabilities.capabilities).map_err(|error|RepositoryError::InvalidInput(error.to_string()))?)
    .fetch_all(&mut **transaction)
    .await?;
    let mut candidates = Vec::new();
    for row in rows {
        let candidate_result: Result<OrchestrationCandidate, RepositoryError> = (|| {
            let run_version = row.try_get("scheduler_run_version")?;
            let principal_id = row.try_get("scheduler_principal_id")?;
            let agent_deployment_id = row.try_get("scheduler_agent_deployment_id")?;
            let node_version = row.try_get("scheduler_node_version")?;
            let scope_id = row.try_get("scheduler_scope_id")?;
            let scope_version = row.try_get("scheduler_scope_version")?;
            let node_diagnostic = crate::recovery_isolation::identity(
                &row,
                "node_id",
                ResourceKind::NodeExecution,
                insight_platform_jobs::store::SafetyScanPhase::OwnerValidation,
            )?;
            crate::recovery_isolation::persisted(
                (|| {
                    let payload = payload_from_row(
                        &row,
                        "scheduler_node_payload_version",
                        "scheduler_node_payload",
                        "scheduler_node_payload_digest",
                    )?;
                    if payload.schema_version != 1 {
                        return Err(RepositoryError::CorruptRow(
                            "invalid Node payload schema".into(),
                        ));
                    }
                    PlanNodeKey::new(row.try_get::<String, _>("scheduler_node_key")?)
                        .map_err(|_| RepositoryError::CorruptRow("invalid Node Plan key".into()))?;
                    row.try_get::<String, _>("scheduler_node_kind")?
                        .parse::<PlanNodeKind>()
                        .map_err(|_| RepositoryError::CorruptRow("invalid Node kind".into()))?;
                    Ok(())
                })(),
                &node_diagnostic,
            )?;
            let job = persisted_job_from_row(row)?;
            require_orchestration_job(&job)?;
            crate::recovery_isolation::job(
                job_projection(&job),
                &job,
                insight_platform_jobs::store::SafetyScanPhase::OwnerValidation,
            )?;
            let run_id = job.run_id.clone().ok_or_else(|| {
                RepositoryError::CorruptRow("orchestration Job has no Run".to_owned())
            })?;
            let node_id = job.node_id.clone().ok_or_else(|| {
                RepositoryError::CorruptRow("orchestration Job has no Node".to_owned())
            })?;
            Ok(OrchestrationCandidate {
                job,
                run_id,
                run_version,
                principal_id,
                agent_deployment_id,
                node_id,
                node_version,
                scope_id,
                scope_version,
            })
        })();
        if let Some(candidate) = crate::recovery_isolation::collect(candidate_result, diagnostics)?
        {
            let tenant = candidate
                .job
                .tenant_id
                .parse()
                .map_err(|_| RepositoryError::CorruptRow("invalid candidate tenant".into()))?;
            let run_id = candidate
                .run_id
                .parse()
                .map_err(|_| RepositoryError::CorruptRow("invalid candidate Run".into()))?;
            // Validate the immutable snapshot before shared quota or any mutation.
            // Later FOR UPDATE/version checks retain the exact same SERIALIZABLE fact.
            if crate::recovery_isolation::collect(
                load_run(transaction, &tenant, &run_id).await,
                diagnostics,
            )?
            .is_some()
            {
                let scope_id = candidate
                    .scope_id
                    .parse()
                    .map_err(|_| RepositoryError::CorruptRow("invalid candidate Scope".into()))?;
                let chain = load_scope_environment_chain(
                    transaction,
                    &tenant,
                    &run_id,
                    &scope_id,
                    scope_limits,
                )
                .await;
                let chain = crate::recovery_isolation::addressed(
                    chain,
                    &tenant,
                    &scope_id,
                    insight_platform_jobs::store::SafetyScanPhase::OwnerDecode,
                );
                if crate::recovery_isolation::collect(chain, diagnostics)?.is_some() {
                    candidates.push(candidate);
                }
            }
        }
    }
    Ok(candidates)
}

pub(crate) async fn load_tenant_scheduling_policy(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
) -> Result<TenantSchedulingPolicyBinding, RepositoryError> {
    // Scheduling binding mutation holds every fairness row before changing
    // TenantConfig. The caller already holds this work class's fairness row;
    // worker roles therefore need SELECT, never UPDATE privilege on tenants.
    let row = sqlx::query(
        r#"
        SELECT tenant_id, state, version, config_schema_version, config,
               config_digest, created_at, updated_at
        FROM insight_platform.tenants
        WHERE tenant_id = $1 AND state = 'active'
        "#,
    )
    .bind(tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("active tenant"))?;
    let tenant = tenant_from_row(row)?;
    let exact = tenant.config.scheduling_policy.ok_or_else(|| {
        RepositoryError::InvalidInput("tenant has no Scheduling policy binding".to_owned())
    })?;
    let (revision, policy) =
        load_exact_active_policy_deployment(transaction, tenant_id, &exact, PolicyKind::Scheduling)
            .await?;
    let document = policy.scheduling.ok_or_else(|| {
        RepositoryError::CorruptRow("Scheduling policy has no closed document".to_owned())
    })?;
    let binding = TenantSchedulingPolicyBinding {
        tenant_id: tenant_id.clone(),
        policy_version_id: revision.revision_id,
        policy_version_digest: revision.semantic_digest,
        rules_digest: policy.rules_digest,
        weight: document.weight,
        burst: document.burst,
        aging_rounds: document.aging_rounds,
    };
    binding
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(binding)
}

async fn lock_orchestration_quota_accounts(
    transaction: &mut Transaction<'_, Postgres>,
    selected: &[OrchestrationCandidate],
) -> Result<BTreeMap<String, QuotaAccountRecord>, RepositoryError> {
    let tenants = selected
        .iter()
        .map(|candidate| candidate.job.tenant_id.clone())
        .collect::<Vec<_>>();
    let runs = selected
        .iter()
        .map(|candidate| candidate.run_id.clone())
        .collect::<Vec<_>>();
    let principals = selected
        .iter()
        .map(|candidate| candidate.principal_id.clone())
        .collect::<Vec<_>>();
    let deployments = selected
        .iter()
        .map(|candidate| candidate.agent_deployment_id.clone())
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT *
        FROM insight_platform.quota_accounts
        WHERE work_class = 'orchestration' AND metric = 'concurrent_jobs'
          AND (
              (scope_kind = 'tenant' AND scope_id = ANY($1))
              OR (scope_kind = 'run' AND scope_id = ANY($2))
              OR (scope_kind = 'principal' AND scope_id = ANY($3))
              OR (scope_kind = 'agent_deployment' AND scope_id = ANY($4))
          )
        ORDER BY tenant_id, quota_account_id
        FOR UPDATE
        "#,
    )
    .bind(tenants)
    .bind(runs)
    .bind(principals)
    .bind(deployments)
    .fetch_all(&mut **transaction)
    .await?;
    let mut accounts = BTreeMap::new();
    for row in rows {
        let account = quota_account_from_row(row)?;
        if accounts
            .insert(account.quota_account_id.clone(), account)
            .is_some()
        {
            return Err(RepositoryError::CorruptRow(
                "quota account lock query returned a duplicate".to_owned(),
            ));
        }
    }
    Ok(accounts)
}

fn quota_account_ids_for_candidate(
    candidate: &OrchestrationCandidate,
    accounts: &BTreeMap<String, QuotaAccountRecord>,
) -> Result<Vec<String>, RepositoryError> {
    let mut relevant = accounts
        .values()
        .filter(|account| {
            account.tenant_id == candidate.job.tenant_id
                && match account.scope_kind.as_str() {
                    "tenant" => account.scope_id == candidate.job.tenant_id,
                    "run" => account.scope_id == candidate.run_id,
                    "principal" => account.scope_id == candidate.principal_id,
                    "agent_deployment" => account.scope_id == candidate.agent_deployment_id,
                    _ => false,
                }
        })
        .map(|account| account.quota_account_id.clone())
        .collect::<Vec<_>>();
    relevant.sort();
    let tenant_lines = relevant
        .iter()
        .filter(|account_id| {
            accounts
                .get(*account_id)
                .is_some_and(|account| account.scope_kind == "tenant")
        })
        .count();
    if tenant_lines != 1 || relevant.len() > MAX_ORCHESTRATION_QUOTA_LINES {
        return Err(RepositoryError::QuotaExceeded);
    }
    Ok(relevant)
}

async fn reserve_orchestration_quota_bundles(
    transaction: &mut Transaction<'_, Postgres>,
    selected: &[(OrchestrationCandidate, &OrchestrationClaimSlot)],
    accounts: &BTreeMap<String, QuotaAccountRecord>,
    decisions: &BTreeMap<String, JobProjection>,
) -> Result<BTreeMap<String, Vec<String>>, RepositoryError> {
    let mut lines_by_job = BTreeMap::new();
    let mut increments = BTreeMap::<String, i64>::new();
    for (candidate, _) in selected {
        let lines = quota_account_ids_for_candidate(candidate, accounts)?;
        for account_id in &lines {
            let increment = increments.entry(account_id.clone()).or_default();
            *increment = increment
                .checked_add(1)
                .ok_or_else(|| RepositoryError::QuotaExceeded)?;
        }
        lines_by_job.insert(candidate.job.job_id.clone(), lines);
    }
    let mut resulting_versions = BTreeMap::new();
    for (account_id, increment) in &increments {
        let account = accounts.get(account_id).ok_or_else(|| {
            RepositoryError::CorruptRow("selected quota account disappeared".to_owned())
        })?;
        if account
            .reserved_value
            .checked_add(account.used_value)
            .and_then(|value| value.checked_add(*increment))
            .is_none_or(|value| value > account.limit_value)
        {
            return Err(RepositoryError::QuotaExceeded);
        }
        let row = sqlx::query(
            r#"
            UPDATE insight_platform.quota_accounts
            SET reserved_value = reserved_value + $4, version = version + 1,
                updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND quota_account_id = $2 AND version = $3
              AND reserved_value + used_value + $4 <= limit_value
            RETURNING version
            "#,
        )
        .bind(&account.tenant_id)
        .bind(&account.quota_account_id)
        .bind(account.version)
        .bind(*increment)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::QuotaExceeded)?;
        resulting_versions.insert(account_id.clone(), row.try_get("version")?);
    }
    for (candidate, slot) in selected {
        let lines = lines_by_job.get(&candidate.job.job_id).ok_or_else(|| {
            RepositoryError::CorruptRow("Job quota bundle was not derived".to_owned())
        })?;
        let next = decisions.get(&candidate.job.job_id).ok_or_else(|| {
            RepositoryError::CorruptRow("Job claim decision was not derived".to_owned())
        })?;
        let request = TypedPayload::with_limit(
            1,
            &serde_json::json!({
                "job_id": candidate.job.job_id,
                "lease_generation": next.lease_generation,
                "quota_account_ids": lines,
                "quota_reservation_id": slot.quota_reservation_id,
            }),
            65_536,
        )?;
        let reservation_id = slot.quota_reservation_id.to_string();
        for (index, account_id) in lines.iter().enumerate() {
            let account = accounts.get(account_id).ok_or_else(|| {
                RepositoryError::CorruptRow("Job quota account was not locked".to_owned())
            })?;
            let quota_entry_id = slot.quota_entry_ids[index].to_string();
            insert_quota_entry(
                transaction,
                QuotaEntryInsert {
                    tenant_id: &account.tenant_id,
                    quota_entry_id: &quota_entry_id,
                    quota_account_id: account_id,
                    correlation_id: &reservation_id,
                    entry_kind: "reserve",
                    reserved_amount: 1,
                    used_amount: 0,
                    account_version: *resulting_versions.get(account_id).ok_or_else(|| {
                        RepositoryError::CorruptRow(
                            "quota mutation returned no account version".to_owned(),
                        )
                    })?,
                    request_digest: &request.digest,
                },
            )
            .await?;
        }
    }
    Ok(lines_by_job)
}

async fn lock_orchestration_parents(
    transaction: &mut Transaction<'_, Postgres>,
    selected: &[(OrchestrationCandidate, &OrchestrationClaimSlot)],
    database_now: DateTime<Utc>,
) -> Result<LockedOrchestrationParents, RepositoryError> {
    let run_ids = selected
        .iter()
        .map(|(candidate, _)| candidate.run_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT * FROM insight_platform.runs
        WHERE run_id = ANY($1)
        ORDER BY tenant_id, run_id
        FOR UPDATE
        "#,
    )
    .bind(&run_ids)
    .fetch_all(&mut **transaction)
    .await?;
    let mut runs = BTreeMap::new();
    for row in rows {
        let record = run_from_row(row)?;
        if !matches!(record.state.as_str(), "queued" | "running")
            || record.deadline <= database_now
            || record.current.control.pause_requested
            || record.current.control.cancel_requested_at.is_some()
            || record.current.control.timeout_requested_at.is_some()
        {
            return Err(RepositoryError::Conflict("orchestration Run parent"));
        }
        runs.insert(
            record.run_id.clone(),
            LockedRunParent {
                tenant_id: record.tenant_id,
                run_id: record.run_id,
                version: record.version,
            },
        );
    }
    if runs.len() != run_ids.len() {
        return Err(RepositoryError::Conflict("orchestration Run parent"));
    }

    let node_ids = selected
        .iter()
        .flat_map(|(candidate, _)| [candidate.node_id.clone(), candidate.scope_id.clone()])
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT tenant_id, node_id, record_kind, state, version, deadline
        FROM insight_platform.run_nodes
        WHERE node_id = ANY($1)
        ORDER BY tenant_id, node_id
        FOR UPDATE
        "#,
    )
    .bind(&node_ids)
    .fetch_all(&mut **transaction)
    .await?;
    let mut nodes = BTreeMap::new();
    let mut node_deadlines = BTreeMap::new();
    for row in rows {
        let node_id: String = row.try_get("node_id")?;
        node_deadlines.insert(
            node_id.clone(),
            row.try_get::<DateTime<Utc>, _>("deadline")?,
        );
        nodes.insert(
            node_id.clone(),
            LockedNodeParent {
                tenant_id: row.try_get("tenant_id")?,
                node_id,
                version: row.try_get("version")?,
                state: row.try_get("state")?,
                record_kind: row.try_get("record_kind")?,
            },
        );
    }
    if nodes.len() != node_ids.len() {
        return Err(RepositoryError::Conflict("orchestration Node parent"));
    }
    for (candidate, _) in selected {
        let run = runs
            .get(&candidate.run_id)
            .ok_or(RepositoryError::Conflict("orchestration Run parent"))?;
        let node = nodes
            .get(&candidate.node_id)
            .ok_or(RepositoryError::Conflict("orchestration Node parent"))?;
        let scope = nodes
            .get(&candidate.scope_id)
            .ok_or(RepositoryError::Conflict("orchestration Scope parent"))?;
        if run.tenant_id != candidate.job.tenant_id
            || run.version != candidate.run_version
            || node.tenant_id != candidate.job.tenant_id
            || node.version != candidate.node_version
            || node.record_kind != "node_execution"
            || node.state != "ready"
            || node_deadlines
                .get(&candidate.node_id)
                .is_none_or(|deadline| *deadline <= database_now)
            || scope.tenant_id != candidate.job.tenant_id
            || scope.version != candidate.scope_version
            || scope.record_kind != "scope_instance"
            || scope.state != "open"
            || node_deadlines
                .get(&candidate.scope_id)
                .is_none_or(|deadline| *deadline <= database_now)
        {
            return Err(RepositoryError::Conflict("orchestration parent"));
        }
    }
    Ok(LockedOrchestrationParents { runs, nodes })
}

async fn lock_selected_jobs(
    transaction: &mut Transaction<'_, Postgres>,
    selected: &[(OrchestrationCandidate, &OrchestrationClaimSlot)],
) -> Result<BTreeMap<String, JobRecord>, RepositoryError> {
    let job_ids = selected
        .iter()
        .map(|(candidate, _)| candidate.job.job_id.clone())
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT * FROM insight_platform.jobs
        WHERE job_id = ANY($1)
        ORDER BY tenant_id, job_id
        FOR UPDATE
        "#,
    )
    .bind(job_ids)
    .fetch_all(&mut **transaction)
    .await?;
    let mut jobs = BTreeMap::new();
    for row in rows {
        let job = job_from_row(row)?;
        jobs.insert(job.job_id.clone(), job);
    }
    if jobs.len() != selected.len() {
        return Err(RepositoryError::Conflict("selected orchestration Job"));
    }
    Ok(jobs)
}

async fn mutate_orchestration_parents(
    transaction: &mut Transaction<'_, Postgres>,
    selected: &[(OrchestrationCandidate, &OrchestrationClaimSlot)],
    locked: &LockedOrchestrationParents,
    database_now: DateTime<Utc>,
) -> Result<BTreeMap<String, (i64, i64)>, RepositoryError> {
    let mut claimed_per_run = BTreeMap::<String, i32>::new();
    for (candidate, _) in selected {
        let count = claimed_per_run.entry(candidate.run_id.clone()).or_default();
        *count = count.checked_add(1).ok_or_else(|| {
            RepositoryError::InvalidInput("Run active work count overflowed".to_owned())
        })?;
    }
    let mut run_versions = BTreeMap::new();
    for (run_id, increment) in claimed_per_run {
        let parent = locked
            .runs
            .get(&run_id)
            .ok_or(RepositoryError::Conflict("orchestration Run parent"))?;
        let row = sqlx::query(
            r#"
            UPDATE insight_platform.runs
            SET state = 'running', active_work_count = active_work_count + $4,
                version = version + 1, started_at = COALESCE(started_at, $5),
                updated_at = $5
            WHERE tenant_id = $1 AND run_id = $2 AND version = $3
              AND state IN ('queued', 'running') AND terminal_at IS NULL
            RETURNING version
            "#,
        )
        .bind(&parent.tenant_id)
        .bind(&parent.run_id)
        .bind(parent.version)
        .bind(increment)
        .bind(database_now)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("orchestration Run parent"))?;
        run_versions.insert(run_id, row.try_get("version")?);
    }

    let mut result = BTreeMap::new();
    for (candidate, _) in selected {
        let parent = locked
            .nodes
            .get(&candidate.node_id)
            .ok_or(RepositoryError::Conflict("orchestration Node parent"))?;
        let row = sqlx::query(
            r#"
            UPDATE insight_platform.run_nodes
            SET version = version + 1, updated_at = $5
            WHERE tenant_id = $1 AND node_id = $2 AND version = $3
              AND state = $4 AND record_kind = 'node_execution' AND terminal_at IS NULL
            RETURNING version
            "#,
        )
        .bind(&parent.tenant_id)
        .bind(&parent.node_id)
        .bind(parent.version)
        .bind(&parent.state)
        .bind(database_now)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("orchestration Node parent"))?;
        let node_version: i64 = row.try_get("version")?;
        let run_version = *run_versions
            .get(&candidate.run_id)
            .ok_or(RepositoryError::Conflict("orchestration Run parent"))?;
        result.insert(candidate.job.job_id.clone(), (run_version, node_version));
    }
    Ok(result)
}

async fn mutate_claimed_jobs(
    transaction: &mut Transaction<'_, Postgres>,
    selected: &[(OrchestrationCandidate, &OrchestrationClaimSlot)],
    decisions: &BTreeMap<String, JobProjection>,
    quota_lines: &BTreeMap<String, Vec<String>>,
    worker_build_digest: &Sha256Digest,
) -> Result<Vec<ClaimedOrchestrationJob>, RepositoryError> {
    let mut claimed = Vec::with_capacity(selected.len());
    for (candidate, slot) in selected {
        let next = decisions
            .get(&candidate.job.job_id)
            .ok_or_else(|| RepositoryError::CorruptRow("missing Job claim decision".to_owned()))?;
        let lease = next.lease.as_ref().ok_or_else(|| {
            RepositoryError::CorruptRow("Job claim decision has no lease".to_owned())
        })?;
        let row =
            sqlx::query(
                r#"
            UPDATE insight_platform.jobs
            SET state = $4, version = $5, attempt_no = $6, lease_epoch = $7,
                worker_id = $8, lease_token_digest = $9,
                lease_expires_at = $10, heartbeat_at = $11, retry_at = NULL,
                quota_reservation_id = $12, updated_at = $11, attempt_build_digest=$13
            WHERE tenant_id = $1 AND job_id = $2 AND version = $3
              AND state IN ('ready', 'retry_scheduled') AND worker_id IS NULL
              AND (
                  quota_reservation_id IS NULL OR EXISTS (
                      SELECT 1 FROM insight_platform.quota_ledger AS settled
                      WHERE settled.tenant_id = jobs.tenant_id
                        AND settled.correlation_id = jobs.quota_reservation_id
                        AND settled.entry_kind = 'settle'
                  )
              )
            RETURNING *
            "#,
            )
            .bind(&candidate.job.tenant_id)
            .bind(&candidate.job.job_id)
            .bind(candidate.job.version)
            .bind(next.state.as_str())
            .bind(i64::try_from(next.version).map_err(|_| {
                RepositoryError::InvalidInput("Job version exceeds bigint".to_owned())
            })?)
            .bind(i32::try_from(next.attempt_count).map_err(|_| {
                RepositoryError::InvalidInput("Job attempt count exceeds integer".to_owned())
            })?)
            .bind(i64::try_from(next.lease_generation).map_err(|_| {
                RepositoryError::InvalidInput("Job lease generation exceeds bigint".to_owned())
            })?)
            .bind(lease.worker_process_generation_id.to_string())
            .bind(lease.token_digest.to_string())
            .bind(lease.expires_at)
            .bind(lease.heartbeat_at)
            .bind(slot.quota_reservation_id.to_string())
            .bind(worker_build_digest.to_string())
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(RepositoryError::Conflict("selected orchestration Job"))?;
        claimed.push(ClaimedOrchestrationJob {
            job: job_from_row(row)?,
            run_version: 0,
            node_version: 0,
            quota_reservation_id: slot.quota_reservation_id.to_string(),
            quota_account_ids: quota_lines.get(&candidate.job.job_id).cloned().ok_or_else(
                || RepositoryError::CorruptRow("missing Job quota bundle".to_owned()),
            )?,
        });
    }
    Ok(claimed)
}

async fn append_orchestration_claim_events(
    transaction: &mut Transaction<'_, Postgres>,
    selected: &[(OrchestrationCandidate, &OrchestrationClaimSlot)],
    claimed: &[ClaimedOrchestrationJob],
    parent_versions: &BTreeMap<String, (i64, i64)>,
    quota_lines: &BTreeMap<String, Vec<String>>,
) -> Result<(), RepositoryError> {
    let claimed_by_job = claimed
        .iter()
        .map(|record| (record.job.job_id.as_str(), &record.job))
        .collect::<BTreeMap<_, _>>();
    let mut emitted_runs = BTreeSet::new();
    for (candidate, slot) in selected {
        let (run_version, node_version) = parent_versions
            .get(&candidate.job.job_id)
            .copied()
            .ok_or_else(|| RepositoryError::CorruptRow("missing parent version".to_owned()))?;
        let job = claimed_by_job
            .get(candidate.job.job_id.as_str())
            .copied()
            .ok_or_else(|| RepositoryError::CorruptRow("missing claimed Job".to_owned()))?;
        if emitted_runs.insert(candidate.run_id.clone()) {
            let run_job_ids = selected
                .iter()
                .filter(|(other, _)| other.run_id == candidate.run_id)
                .map(|(other, _)| other.job.job_id.as_str())
                .collect::<Vec<_>>();
            append_scheduler_event(
                transaction,
                &candidate.job.tenant_id,
                &slot.run_event_id,
                &slot.run_outbox_id,
                "run",
                &candidate.run_id,
                run_version,
                Some(&candidate.run_id),
                "run.work_claimed",
                &TypedPayload::new(
                    1,
                    &serde_json::json!({
                        "active_work_increment": run_job_ids.len(),
                        "job_ids": run_job_ids,
                    }),
                )?,
            )
            .await?;
        }
        append_scheduler_event(
            transaction,
            &candidate.job.tenant_id,
            &slot.node_event_id,
            &slot.node_outbox_id,
            "node_execution",
            &candidate.node_id,
            node_version,
            Some(&candidate.run_id),
            "node.work_claimed",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "job_id": job.job_id,
                    "lease_generation": job.lease_epoch,
                }),
            )?,
        )
        .await?;
        append_scheduler_event(
            transaction,
            &candidate.job.tenant_id,
            &slot.job_event_id,
            &slot.job_outbox_id,
            "job",
            &job.job_id,
            job.version,
            Some(&candidate.run_id),
            "job.claimed",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "lease_generation": job.lease_epoch,
                    "node_version": node_version,
                    "quota_account_ids": quota_lines.get(&job.job_id),
                    "quota_reservation_id": slot.quota_reservation_id,
                    "run_version": run_version,
                    "worker_process_generation_id": job.worker_id,
                }),
            )?,
        )
        .await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn append_scheduler_event(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    event_id: &ResourceId,
    outbox_id: &ResourceId,
    aggregate_kind: &str,
    aggregate_id: &str,
    aggregate_version: i64,
    run_id: Option<&str>,
    event_type: &str,
    payload: &TypedPayload,
) -> Result<(), RepositoryError> {
    let trace_id: String = if let Some(run_id) = run_id {
        sqlx::query_scalar(
            "SELECT trace_id FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $2",
        )
        .bind(tenant_id)
        .bind(run_id)
        .fetch_one(&mut **transaction)
        .await?
    } else {
        sqlx::query_scalar(
            r#"
            SELECT COALESCE(
                (
                    SELECT trace_id
                    FROM insight_platform.jobs
                    WHERE tenant_id = $1
                      AND (job_id = $2 OR (owner_kind = $3 AND owner_id = $2))
                    ORDER BY (job_id = $2) DESC, updated_at DESC, job_id DESC
                    LIMIT 1
                ),
                (
                    SELECT trace_id FROM insight_platform.tasks
                    WHERE tenant_id = $1 AND task_id = $2
                ),
                (
                    SELECT trace_id FROM insight_platform.invocations
                    WHERE tenant_id = $1 AND invocation_id = $2
                )
            )
            "#,
        )
        .bind(tenant_id)
        .bind(aggregate_id)
        .bind(aggregate_kind)
        .fetch_one(&mut **transaction)
        .await?
    };
    insert_scheduler_event(
        transaction,
        tenant_id,
        event_id,
        outbox_id,
        aggregate_kind,
        aggregate_id,
        aggregate_version,
        &trace_id,
        run_id,
        event_type,
        payload,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn append_scheduler_event_with_trace(
    transaction: &mut Transaction<'_, Postgres>,
    trace: TraceIdentityV1,
    tenant_id: &str,
    event_id: &ResourceId,
    outbox_id: &ResourceId,
    aggregate_kind: &str,
    aggregate_id: &str,
    aggregate_version: i64,
    run_id: Option<&str>,
    event_type: &str,
    payload: &TypedPayload,
) -> Result<(), RepositoryError> {
    let trace_id = trace.trace_id.to_string();
    insert_scheduler_event(
        transaction,
        tenant_id,
        event_id,
        outbox_id,
        aggregate_kind,
        aggregate_id,
        aggregate_version,
        &trace_id,
        run_id,
        event_type,
        payload,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn insert_scheduler_event(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    event_id: &ResourceId,
    outbox_id: &ResourceId,
    aggregate_kind: &str,
    aggregate_id: &str,
    aggregate_version: i64,
    trace_id: &str,
    run_id: Option<&str>,
    event_type: &str,
    payload: &TypedPayload,
) -> Result<(), RepositoryError> {
    let public_event_type = public_run_event_type(aggregate_kind, event_type, &payload.value);
    let public_sequence = match (run_id, public_event_type) {
        (Some(run_id), Some(_)) => {
            Some(next_public_run_sequence(transaction, tenant_id, run_id).await?)
        }
        _ => None,
    };
    let visibility = if public_sequence.is_some() {
        "public"
    } else {
        "internal"
    };
    sqlx::query(
        r#"
        INSERT INTO insight_platform.events (
            tenant_id, event_id, aggregate_kind, aggregate_id, aggregate_version,
            trace_id, run_id, public_sequence, event_type, visibility,
            payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        "#,
    )
    .bind(tenant_id)
    .bind(event_id.to_string())
    .bind(aggregate_kind)
    .bind(aggregate_id)
    .bind(aggregate_version)
    .bind(trace_id)
    .bind(run_id)
    .bind(public_sequence)
    .bind(event_type)
    .bind(visibility)
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.outbox_events (tenant_id, outbox_id, event_id, trace_id)
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(tenant_id)
    .bind(outbox_id.to_string())
    .bind(event_id.to_string())
    .bind(trace_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

pub struct PgRunTransaction {
    outbox_admission_backlog: u32,
    transaction: Transaction<'static, Postgres>,
    scope_environment_limits: ScopeEnvironmentLimits,
}

#[derive(Debug, Clone)]
pub struct NewTenant {
    pub tenant_id: String,
    pub state: String,
    pub config: TenantConfig,
}

#[derive(Debug, Clone)]
pub struct BootstrapInstallationOperator {
    pub principal_id: ResourceId,
    pub request_id: ResourceId,
    pub authentication_authority_digest: Sha256Digest,
    pub subject_digest: Sha256Digest,
    pub evidence_digest: Sha256Digest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapOutcome {
    Created,
    Replayed,
}

#[derive(Debug, Clone)]
pub struct BootstrapDevelopmentProfile {
    pub installation: BootstrapInstallationOperator,
    pub tenant: NewTenant,
    pub developer: NewPrincipal,
    pub service_principals: Vec<NewPrincipal>,
    pub tenant_principal_bindings: Vec<NewTenantPrincipal>,
    pub artifact_authority: Option<DevelopmentArtifactAuthoritySeed>,
}

/// Exact non-production roots required to break the Artifact-policy publication bootstrap cycle.
/// The one-shot development bootstrap materializes these as ordinary immutable Policy versions,
/// Deployments and tenant bindings in the same transaction as the fresh tenant. Production
/// installation remains owned by its deployment/GitOps workflow and never uses this seed.
#[derive(Debug, Clone)]
pub struct DevelopmentArtifactAuthoritySeed {
    pub authoring_artifact_id: ResourceId,
    pub authoring_blob_id: ResourceId,
    pub retention_policy_id: ResourceId,
    pub retention_policy_revision_id: ResourceId,
    pub retention_policy_deployment_id: ResourceId,
    pub artifact_io_policy_id: ResourceId,
    pub artifact_io_policy_revision_id: ResourceId,
    pub artifact_io_policy_deployment_id: ResourceId,
    pub scheduling_policy_id: ResourceId,
    pub scheduling_policy_revision_id: ResourceId,
    pub scheduling_policy_deployment_id: ResourceId,
    pub staging_quota_account_id: ResourceId,
    pub orchestration_quota_account_id: ResourceId,
    pub retention_policy: ArtifactRetentionPolicy,
    pub artifact_io_policy: SandboxArtifactIoPolicyDocument,
    pub scheduling_policy: SchedulingPolicyDocument,
    pub staging_quota_bytes: i64,
    pub orchestration_concurrent_jobs: i64,
}

fn validate_development_bootstrap(
    command: &BootstrapDevelopmentProfile,
) -> Result<(), RepositoryError> {
    if command.installation.principal_id.kind() != ResourceKind::Principal
        || command.installation.request_id.kind() != ResourceKind::ServerRequest
        || command.developer.principal_id.kind() != ResourceKind::Principal
        || command.installation.principal_id == command.developer.principal_id
        || !command
            .developer
            .installation_bindings
            .installation_bindings
            .is_empty()
        || command.service_principals.is_empty()
        || command.service_principals.len() > 16
    {
        return Err(RepositoryError::InvalidInput(
            "development bootstrap principal identity is invalid".to_owned(),
        ));
    }
    let mut allowed_principals = BTreeSet::from([command.developer.principal_id.clone()]);
    for principal in &command.service_principals {
        if principal.principal_id.kind() != ResourceKind::Principal
            || principal.principal_id == command.installation.principal_id
            || !principal
                .installation_bindings
                .installation_bindings
                .is_empty()
            || !allowed_principals.insert(principal.principal_id.clone())
        {
            return Err(RepositoryError::InvalidInput(
                "development bootstrap service identity is invalid".to_owned(),
            ));
        }
    }
    if let Some(seed) = &command.artifact_authority {
        seed.validate()?;
    }
    validate_id(&command.tenant.tenant_id)?;
    validate_code("tenant state", &command.tenant.state)?;
    if command.tenant.config.scheduling_policy.is_some() {
        return Err(RepositoryError::InvalidInput(
            "development tenant scheduling policy must be bound after publication".to_owned(),
        ));
    }
    if command.tenant_principal_bindings.is_empty() {
        return Err(RepositoryError::InvalidInput(
            "development bootstrap requires a tenant principal binding".to_owned(),
        ));
    }
    let tenant_id = ResourceId::parse_expected(&command.tenant.tenant_id, ResourceKind::Tenant)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let mut bindings = BTreeSet::new();
    for binding in &command.tenant_principal_bindings {
        if binding.tenant_id != tenant_id
            || !allowed_principals.contains(&binding.principal_id)
            || binding.principal_kind == PrincipalKind::InstallationOperator
            || !bindings.insert((binding.principal_id.clone(), binding.principal_kind))
        {
            return Err(RepositoryError::InvalidInput(
                "development bootstrap tenant principal binding is invalid".to_owned(),
            ));
        }
    }
    Ok(())
}

impl DevelopmentArtifactAuthoritySeed {
    fn validate(&self) -> Result<(), RepositoryError> {
        for (id, kind) in [
            (&self.authoring_artifact_id, ResourceKind::Artifact),
            (&self.authoring_blob_id, ResourceKind::InternalBlob),
            (&self.retention_policy_id, ResourceKind::Policy),
            (
                &self.retention_policy_revision_id,
                ResourceKind::PolicyRevision,
            ),
            (
                &self.retention_policy_deployment_id,
                ResourceKind::PolicyDeployment,
            ),
            (&self.artifact_io_policy_id, ResourceKind::Policy),
            (
                &self.artifact_io_policy_revision_id,
                ResourceKind::PolicyRevision,
            ),
            (
                &self.artifact_io_policy_deployment_id,
                ResourceKind::PolicyDeployment,
            ),
            (&self.scheduling_policy_id, ResourceKind::Policy),
            (
                &self.scheduling_policy_revision_id,
                ResourceKind::PolicyRevision,
            ),
            (
                &self.scheduling_policy_deployment_id,
                ResourceKind::PolicyDeployment,
            ),
            (&self.staging_quota_account_id, ResourceKind::QuotaAccount),
            (
                &self.orchestration_quota_account_id,
                ResourceKind::QuotaAccount,
            ),
            (
                &self.artifact_io_policy.encryption_domain_id,
                ResourceKind::EncryptionDomain,
            ),
        ] {
            if id.kind() != kind {
                return Err(RepositoryError::InvalidInput(
                    "development Artifact bootstrap identity is invalid".to_owned(),
                ));
            }
        }
        let unique = [
            &self.authoring_artifact_id,
            &self.authoring_blob_id,
            &self.retention_policy_id,
            &self.retention_policy_revision_id,
            &self.retention_policy_deployment_id,
            &self.artifact_io_policy_id,
            &self.artifact_io_policy_revision_id,
            &self.artifact_io_policy_deployment_id,
            &self.scheduling_policy_id,
            &self.scheduling_policy_revision_id,
            &self.scheduling_policy_deployment_id,
            &self.staging_quota_account_id,
            &self.orchestration_quota_account_id,
            &self.artifact_io_policy.encryption_domain_id,
        ]
        .into_iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
        if unique.len() != 14
            || self.staging_quota_bytes <= 0
            || self.orchestration_concurrent_jobs <= 0
            || self.retention_policy.validate().is_err()
            || self.artifact_io_policy.validate().is_err()
            || self.scheduling_policy.validate().is_err()
        {
            return Err(RepositoryError::InvalidInput(
                "development Artifact bootstrap closure is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}

struct DevelopmentArtifactAuthorityMaterial {
    tenant_config: TenantConfig,
    authoring_content_digest: Sha256Digest,
    authoring_size_bytes: i64,
    authoring_metadata: TypedPayload,
    retention_resource: TypedPayload,
    retention_version: TypedPayload,
    retention_deployment: TypedPayload,
    artifact_io_resource: TypedPayload,
    artifact_io_version: TypedPayload,
    artifact_io_deployment: TypedPayload,
    scheduling_resource: TypedPayload,
    scheduling_version: TypedPayload,
    scheduling_deployment: TypedPayload,
    quota_payload: TypedPayload,
    security_domain_digest: Sha256Digest,
}

fn development_seed_digest(
    tenant_id: &ResourceId,
    purpose: &str,
) -> Result<Sha256Digest, RepositoryError> {
    canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "environment_class": "development",
        "purpose": purpose,
        "tenant_id": tenant_id,
    }))
    .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
    .parse::<Sha256Digest>()
    .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))
}

fn development_artifact_authority_material(
    tenant_id: &ResourceId,
    seed: &DevelopmentArtifactAuthoritySeed,
) -> Result<DevelopmentArtifactAuthorityMaterial, RepositoryError> {
    seed.validate()?;
    let authoring_value = serde_json::json!({
        "schema_version": 1,
        "environment_class": "development",
        "kind": "insight.platform.builtin-artifact-authority/v1",
        "tenant_id": tenant_id,
    });
    let authoring_bytes = canonical_json(&authoring_value)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let authoring_content_digest: Sha256Digest = canonical_digest(&authoring_value)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
        .parse::<Sha256Digest>()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let authoring_size_bytes = i64::try_from(authoring_bytes.len()).map_err(|_| {
        RepositoryError::InvalidInput("development bootstrap Artifact is too large".to_owned())
    })?;
    let authoring_ref = ArtifactRef::new(
        seed.authoring_artifact_id.clone(),
        authoring_content_digest.clone(),
        u64::try_from(authoring_size_bytes).map_err(|_| {
            RepositoryError::InvalidInput("development bootstrap Artifact is too large".to_owned())
        })?,
        "application/json",
        DataClassification::Internal,
        Some("builtin-artifact-authority.json".to_owned()),
    )
    .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let authoring_package = AuthoringPackage {
        artifact: authoring_ref.clone(),
        manifest_digest: authoring_content_digest.clone(),
    };
    let retention_rules_digest = seed
        .retention_policy
        .canonical_digest()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let retention_document =
        ResourceDocument::Policy(Box::new(insight_platform_contracts::PolicyResourceSpec {
            authoring_package: authoring_package.clone(),
            contract_digest: development_seed_digest(tenant_id, "artifact-retention-contract")?,
            dependency_versions: Vec::new(),
            policy_versions: Vec::new(),
            policy_kind: PolicyKind::Retention,
            rules_digest: retention_rules_digest,
            selection: None,
            scheduling: None,
            retention: Some(seed.retention_policy.clone()),
            model_safety: None,
            model_budget: None,
            model_public_projection: None,
            mcp_protocol: None,
            mcp_auth: None,
            sandbox_isolation: None,
            sandbox_resource: None,
            sandbox_network: None,
            sandbox_artifact_io: None,
            sandbox_secret_resolution: None,
        }));
    let artifact_io_rules_digest = seed
        .artifact_io_policy
        .canonical_digest()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let artifact_io_document =
        ResourceDocument::Policy(Box::new(insight_platform_contracts::PolicyResourceSpec {
            authoring_package,
            contract_digest: development_seed_digest(tenant_id, "artifact-io-contract")?,
            dependency_versions: Vec::new(),
            policy_versions: Vec::new(),
            policy_kind: PolicyKind::ArtifactIo,
            rules_digest: artifact_io_rules_digest,
            selection: None,
            scheduling: None,
            retention: None,
            model_safety: None,
            model_budget: None,
            model_public_projection: None,
            mcp_protocol: None,
            mcp_auth: None,
            sandbox_isolation: None,
            sandbox_resource: None,
            sandbox_network: None,
            sandbox_artifact_io: Some(seed.artifact_io_policy.clone()),
            sandbox_secret_resolution: None,
        }));
    let scheduling_rules_digest = seed
        .scheduling_policy
        .canonical_digest()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let scheduling_document =
        ResourceDocument::Policy(Box::new(insight_platform_contracts::PolicyResourceSpec {
            authoring_package: AuthoringPackage {
                artifact: authoring_ref.clone(),
                manifest_digest: authoring_content_digest.clone(),
            },
            contract_digest: development_seed_digest(tenant_id, "scheduling-contract")?,
            dependency_versions: Vec::new(),
            policy_versions: Vec::new(),
            policy_kind: PolicyKind::Scheduling,
            rules_digest: scheduling_rules_digest,
            selection: None,
            scheduling: Some(seed.scheduling_policy.clone()),
            retention: None,
            model_safety: None,
            model_budget: None,
            model_public_projection: None,
            mcp_protocol: None,
            mcp_auth: None,
            sandbox_isolation: None,
            sandbox_resource: None,
            sandbox_network: None,
            sandbox_artifact_io: None,
            sandbox_secret_resolution: None,
        }));
    retention_document
        .validate()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    artifact_io_document
        .validate()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    scheduling_document
        .validate()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let validation = |purpose| -> Result<ValidationSummary, RepositoryError> {
        Ok(ValidationSummary {
            program_requirement: None,
            validator_digest: development_seed_digest(tenant_id, "builtin-validator")?,
            validated_draft_digest: development_seed_digest(tenant_id, purpose)?,
            dependency_closure_digest: development_seed_digest(
                tenant_id,
                "builtin-empty-dependency-closure",
            )?,
            security_evidence_digest: development_seed_digest(
                tenant_id,
                "builtin-development-security-evidence",
            )?,
            warnings: Vec::new(),
        })
    };
    let retention_resource = TypedPayload::new(
        1,
        &ResourceDraftPayload {
            alias: None,
            display_name: "Built-in local Artifact retention".to_owned(),
            document: retention_document.clone(),
            validation: None,
        },
    )?;
    let retention_version = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document: retention_document,
            validation: validation("builtin-retention-draft")?,
        },
    )?;
    let artifact_io_resource = TypedPayload::new(
        1,
        &ResourceDraftPayload {
            alias: None,
            display_name: "Built-in local Artifact I/O".to_owned(),
            document: artifact_io_document.clone(),
            validation: None,
        },
    )?;
    let artifact_io_version = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document: artifact_io_document,
            validation: validation("builtin-artifact-io-draft")?,
        },
    )?;
    let scheduling_resource = TypedPayload::new(
        1,
        &ResourceDraftPayload {
            alias: None,
            display_name: "Built-in local Scheduling".to_owned(),
            document: scheduling_document.clone(),
            validation: None,
        },
    )?;
    let scheduling_version = TypedPayload::new(
        1,
        &PublishedVersionPayload {
            document: scheduling_document,
            validation: validation("builtin-scheduling-draft")?,
        },
    )?;
    let qualification_evidence = authoring_ref.clone();
    let retention_deployment = TypedPayload::new(
        1,
        &DeploymentClosure::Policy(insight_platform_contracts::PolicyDeploymentClosure {
            policy_revision: ExactVersionRef::new(
                seed.retention_policy_revision_id.clone(),
                retention_version
                    .digest
                    .parse::<Sha256Digest>()
                    .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
            )
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
            applicability_digest: development_seed_digest(
                tenant_id,
                "builtin-local-applicability",
            )?,
            qualification_evidence: qualification_evidence.clone(),
        }),
    )?;
    let artifact_io_deployment = TypedPayload::new(
        1,
        &DeploymentClosure::Policy(insight_platform_contracts::PolicyDeploymentClosure {
            policy_revision: ExactVersionRef::new(
                seed.artifact_io_policy_revision_id.clone(),
                artifact_io_version
                    .digest
                    .parse::<Sha256Digest>()
                    .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
            )
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
            applicability_digest: development_seed_digest(
                tenant_id,
                "builtin-local-applicability",
            )?,
            qualification_evidence,
        }),
    )?;
    let scheduling_deployment = TypedPayload::new(
        1,
        &DeploymentClosure::Policy(insight_platform_contracts::PolicyDeploymentClosure {
            policy_revision: ExactVersionRef::new(
                seed.scheduling_policy_revision_id.clone(),
                scheduling_version
                    .digest
                    .parse::<Sha256Digest>()
                    .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
            )
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
            applicability_digest: development_seed_digest(
                tenant_id,
                "builtin-local-applicability",
            )?,
            qualification_evidence: authoring_ref,
        }),
    )?;
    let tenant_config = TenantConfig {
        default_model: None,
        scheduling_policy: Some(
            ExactDeploymentRef::new(
                seed.scheduling_policy_deployment_id.clone(),
                scheduling_deployment
                    .digest
                    .parse::<Sha256Digest>()
                    .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
            )
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
        ),
        artifact_retention_policy: Some(
            ExactDeploymentRef::new(
                seed.retention_policy_deployment_id.clone(),
                retention_deployment
                    .digest
                    .parse::<Sha256Digest>()
                    .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
            )
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
        ),
        artifact_io_policy: Some(
            ExactDeploymentRef::new(
                seed.artifact_io_policy_deployment_id.clone(),
                artifact_io_deployment
                    .digest
                    .parse::<Sha256Digest>()
                    .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
            )
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
        ),
    };
    tenant_config
        .validate()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    Ok(DevelopmentArtifactAuthorityMaterial {
        tenant_config,
        authoring_content_digest,
        authoring_size_bytes,
        authoring_metadata: TypedPayload::new(
            1,
            &serde_json::json!({"kind": "builtin_development_authority"}),
        )?,
        retention_resource,
        retention_version,
        retention_deployment,
        artifact_io_resource,
        artifact_io_version,
        artifact_io_deployment,
        scheduling_resource,
        scheduling_version,
        scheduling_deployment,
        quota_payload: TypedPayload::new(1, &serde_json::json!({"profile": "local_development"}))?,
        security_domain_digest: development_seed_digest(
            tenant_id,
            "builtin-artifact-security-domain",
        )?,
    })
}

async fn insert_development_artifact_authority(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    developer_principal_id: &ResourceId,
    seed: &DevelopmentArtifactAuthoritySeed,
    material: &DevelopmentArtifactAuthorityMaterial,
) -> Result<(), RepositoryError> {
    for (resource_id, payload) in [
        (&seed.retention_policy_id, &material.retention_resource),
        (&seed.artifact_io_policy_id, &material.artifact_io_resource),
        (&seed.scheduling_policy_id, &material.scheduling_resource),
    ] {
        sqlx::query(
            r#"
            INSERT INTO insight_platform.resources (
                tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
                payload_schema_version, payload, payload_digest
            ) VALUES ($1, $2, 'policy', 'active', 'enabled', $3, $4, $5)
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(resource_id.to_string())
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .execute(&mut **transaction)
        .await?;
    }
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_blobs (
            tenant_id, blob_id, backend, storage_binding_digest,
            security_domain_digest, object_reference_ciphertext, object_generation, key_id,
            encryption_domain_id, content_digest, size_bytes, state, verified_at,
            created_at, updated_at
        ) VALUES ($1, $2, 'builtin', $3, $4, $5, 'builtin-v1',
                  'builtin-development', $6, $7, $8, 'verified',
                  statement_timestamp(), statement_timestamp(), statement_timestamp())
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(seed.authoring_blob_id.to_string())
    .bind(
        seed.artifact_io_policy
            .write_storage_binding_digest
            .to_string(),
    )
    .bind(material.security_domain_digest.to_string())
    .bind(
        canonical_json(&serde_json::json!({
            "kind": "builtin-development-authority",
            "tenant_id": tenant_id,
        }))
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?,
    )
    .bind(seed.artifact_io_policy.encryption_domain_id.to_string())
    .bind(material.authoring_content_digest.to_string())
    .bind(material.authoring_size_bytes)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifacts (
            tenant_id, artifact_id, blob_id, purpose, classification,
            expected_size_bytes, expected_digest, declared_media_type,
            verified_media_type, state, metadata_schema_version, metadata,
            metadata_digest, retention_policy_revision_id, retain_until, created_by
        ) VALUES ($1, $2, $3, 'authoring_document', 'internal', $4, $5,
                  'application/json', 'application/json', 'ready', $6, $7, $8,
                  $9, clock_timestamp() + interval '365 days', $10)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(seed.authoring_artifact_id.to_string())
    .bind(seed.authoring_blob_id.to_string())
    .bind(material.authoring_size_bytes)
    .bind(material.authoring_content_digest.to_string())
    .bind(material.authoring_metadata.schema_version)
    .bind(&material.authoring_metadata.value)
    .bind(&material.authoring_metadata.digest)
    .bind(seed.retention_policy_revision_id.to_string())
    .bind(developer_principal_id.to_string())
    .execute(&mut **transaction)
    .await?;
    for (resource_id, revision_id, version) in [
        (
            &seed.retention_policy_id,
            &seed.retention_policy_revision_id,
            &material.retention_version,
        ),
        (
            &seed.artifact_io_policy_id,
            &seed.artifact_io_policy_revision_id,
            &material.artifact_io_version,
        ),
        (
            &seed.scheduling_policy_id,
            &seed.scheduling_policy_revision_id,
            &material.scheduling_version,
        ),
    ] {
        sqlx::query(
            r#"
            INSERT INTO insight_platform.resource_versions (
                tenant_id, resource_version_id, resource_id, resource_version_kind,
                revision_no, content_digest, artifact_id, payload_schema_version,
                payload, payload_digest, created_by
            ) VALUES ($1, $2, $3, 'policy_revision', 1, $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(revision_id.to_string())
        .bind(resource_id.to_string())
        .bind(&version.digest)
        .bind(seed.authoring_artifact_id.to_string())
        .bind(version.schema_version)
        .bind(&version.value)
        .bind(&version.digest)
        .bind(developer_principal_id.to_string())
        .execute(&mut **transaction)
        .await?;
    }
    for (resource_id, revision_id, deployment_id, deployment) in [
        (
            &seed.retention_policy_id,
            &seed.retention_policy_revision_id,
            &seed.retention_policy_deployment_id,
            &material.retention_deployment,
        ),
        (
            &seed.artifact_io_policy_id,
            &seed.artifact_io_policy_revision_id,
            &seed.artifact_io_policy_deployment_id,
            &material.artifact_io_deployment,
        ),
        (
            &seed.scheduling_policy_id,
            &seed.scheduling_policy_revision_id,
            &seed.scheduling_policy_deployment_id,
            &material.scheduling_deployment,
        ),
    ] {
        sqlx::query(
            r#"
            INSERT INTO insight_platform.deployments (
                tenant_id, deployment_id, resource_id, resource_version_id, environment,
                bindings_digest, payload_schema_version, bindings, created_by
            ) VALUES ($1, $2, $3, $4, 'local', $5, $6, $7, $8)
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(deployment_id.to_string())
        .bind(resource_id.to_string())
        .bind(revision_id.to_string())
        .bind(&deployment.digest)
        .bind(deployment.schema_version)
        .bind(&deployment.value)
        .bind(developer_principal_id.to_string())
        .execute(&mut **transaction)
        .await?;
        sqlx::query(
            r#"
            UPDATE insight_platform.resources
            SET active_version_id = NULL, active_deployment_id = $3,
                version = version + 2, updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND resource_id = $2
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(resource_id.to_string())
        .bind(deployment_id.to_string())
        .execute(&mut **transaction)
        .await?;
    }
    sqlx::query(
        r#"
        INSERT INTO insight_platform.quota_accounts (
            tenant_id, quota_account_id, scope_kind, scope_id, work_class, metric,
            limit_value, payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, 'tenant', $1, 'artifact', 'artifact.staging_bytes',
                  $3, $4, $5, $6)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(seed.staging_quota_account_id.to_string())
    .bind(seed.staging_quota_bytes)
    .bind(material.quota_payload.schema_version)
    .bind(&material.quota_payload.value)
    .bind(&material.quota_payload.digest)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.quota_accounts (
            tenant_id, quota_account_id, scope_kind, scope_id, work_class, metric,
            limit_value, payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, 'tenant', $1, 'orchestration', 'concurrent_jobs',
                  $3, $4, $5, $6)
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(seed.orchestration_quota_account_id.to_string())
    .bind(seed.orchestration_concurrent_jobs)
    .bind(material.quota_payload.schema_version)
    .bind(&material.quota_payload.value)
    .bind(&material.quota_payload.digest)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub struct TenantRecord {
    pub tenant_id: String,
    pub state: String,
    pub version: i64,
    pub config: TenantConfig,
    pub config_digest: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewPrincipal {
    pub principal_id: ResourceId,
    pub authentication_authority_digest: Sha256Digest,
    pub subject_digest: Sha256Digest,
    pub installation_bindings: PrincipalBindingsPayload,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PrincipalRecord {
    pub principal_id: String,
    pub state: String,
    pub authentication_authority_digest: String,
    pub subject_digest: String,
    pub version: i64,
    pub payload: TypedPayload,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewTenantPrincipal {
    pub tenant_id: ResourceId,
    pub principal_id: ResourceId,
    pub principal_kind: PrincipalKind,
    pub payload: TenantPrincipalPayload,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TenantPrincipalRecord {
    pub tenant_id: String,
    pub principal_id: String,
    pub principal_kind: String,
    pub state: String,
    pub generation: i64,
    pub version: i64,
    pub permissions: TypedPayload,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewSecretBinding {
    pub tenant_id: ResourceId,
    pub secret_binding_id: ResourceId,
    pub purpose: SecretPurpose,
    pub provider_id: ResourceId,
    pub opaque_reference_ciphertext: Vec<u8>,
    pub key_id: String,
    pub reference_digest: Sha256Digest,
    pub payload: SecretBindingPayload,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SecretBindingRecord {
    pub tenant_id: String,
    pub secret_binding_id: String,
    pub purpose: String,
    pub provider_id: String,
    pub state: String,
    pub generation: i64,
    pub version: i64,
    pub opaque_reference_ciphertext: Vec<u8>,
    pub key_id: String,
    pub reference_digest: String,
    pub payload: TypedPayload,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

/// SecretBinding projection safe for management responses and command replay.
///
/// It deliberately excludes ciphertext, KMS key identity, and opaque reference digest.
#[derive(Debug, Clone, PartialEq)]
pub struct SecretBindingMetadataRecord {
    pub tenant_id: String,
    pub secret_binding_id: String,
    pub purpose: String,
    pub provider_id: String,
    pub state: String,
    pub generation: i64,
    pub version: i64,
    pub payload: TypedPayload,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunAdmissionReceiptResult {
    schema_version: u32,
    run: RunRecord,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResourceRecord {
    pub tenant_id: String,
    pub resource_id: String,
    pub resource_kind: String,
    pub lifecycle_state: String,
    pub gate_state: String,
    pub draft_generation: i64,
    pub active_version_id: Option<String>,
    pub active_deployment_id: Option<String>,
    pub version: i64,
    pub payload: TypedPayload,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceUpdateReceiptResult {
    schema_version: u32,
    tenant_id: String,
    resource_id: String,
    resource_kind: String,
    lifecycle_state: String,
    gate_state: String,
    draft_generation: i64,
    active_version_id: Option<String>,
    active_deployment_id: Option<String>,
    version: i64,
    draft: ResourceDraftPayload,
    payload_digest: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl ResourceUpdateReceiptResult {
    fn from_record(record: &ResourceRecord) -> Result<Self, RepositoryError> {
        let draft: ResourceDraftPayload =
            decode_typed_payload(&record.payload, "Resource Receipt Draft")?;
        draft
            .validate()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        Ok(Self {
            schema_version: 1,
            tenant_id: record.tenant_id.clone(),
            resource_id: record.resource_id.clone(),
            resource_kind: record.resource_kind.clone(),
            lifecycle_state: record.lifecycle_state.clone(),
            gate_state: record.gate_state.clone(),
            draft_generation: record.draft_generation,
            active_version_id: record.active_version_id.clone(),
            active_deployment_id: record.active_deployment_id.clone(),
            version: record.version,
            draft,
            payload_digest: record.payload.digest.clone(),
            created_at: record.created_at,
            updated_at: record.updated_at,
        })
    }

    fn into_record(self) -> Result<ResourceRecord, RepositoryError> {
        if self.schema_version != 1 {
            return Err(RepositoryError::CorruptRow(
                "resource update Receipt schema is unsupported".to_owned(),
            ));
        }
        self.draft
            .validate()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        let payload = TypedPayload::new(1, &self.draft)?;
        if payload.digest != self.payload_digest {
            return Err(RepositoryError::CorruptRow(
                "Resource Receipt payload digest differs from stored Draft".to_owned(),
            ));
        }
        Ok(ResourceRecord {
            tenant_id: self.tenant_id,
            resource_id: self.resource_id,
            resource_kind: self.resource_kind,
            lifecycle_state: self.lifecycle_state,
            gate_state: self.gate_state,
            draft_generation: self.draft_generation,
            active_version_id: self.active_version_id,
            active_deployment_id: self.active_deployment_id,
            version: self.version,
            payload,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResourceVersionRecord {
    pub tenant_id: String,
    pub resource_version_id: String,
    pub resource_id: String,
    pub resource_version_kind: String,
    pub revision_no: i64,
    pub content_digest: String,
    pub artifact_id: Option<String>,
    pub payload: TypedPayload,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct PublishedResource {
    pub resource: ResourceRecord,
    pub versions: Vec<ResourceVersionRecord>,
}

#[derive(Debug, Clone)]
pub enum ResourcePublishPreparation {
    Current(ResourceRecord),
    Replayed(PublishedResource),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourcePublishReceiptResult {
    alias: Option<insight_platform_contracts::ResourceAlias>,
    schema_version: u32,
    tenant_id: String,
    resource_id: String,
    resource_kind: String,
    lifecycle_state: String,
    gate_state: String,
    draft_generation: i64,
    active_version_id: Option<String>,
    active_deployment_id: Option<String>,
    version: i64,
    resource_payload_digest: String,
    display_name: String,
    resource_version_ids: Vec<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeploymentRecord {
    pub tenant_id: String,
    pub deployment_id: String,
    pub resource_id: String,
    pub resource_version_id: String,
    pub environment: String,
    pub bindings: TypedPayload,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewJob {
    pub tenant_id: String,
    pub job_id: String,
    pub job_kind: String,
    pub work_class: String,
    pub owner_kind: String,
    pub owner_id: String,
    pub trace_id: TraceId,
    pub invocation_id: Option<String>,
    pub run_id: Option<String>,
    pub node_id: Option<String>,
    pub attempt_limit: i32,
    pub scheduled_at: DateTime<Utc>,
    pub deadline: DateTime<Utc>,
    pub priority: SchedulerPriority,
    pub request_digest: String,
    pub effect_key_digest: Option<String>,
    pub payload: TypedPayload,
    pub execution_requirement: insight_platform_contracts::ExecutionRequirement,
}

impl NewJob {
    fn validate(&self) -> Result<(), RepositoryError> {
        for id in [
            Some(self.tenant_id.as_str()),
            Some(self.job_id.as_str()),
            Some(self.owner_id.as_str()),
            self.invocation_id.as_deref(),
            self.run_id.as_deref(),
            self.node_id.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            validate_id(id)?;
        }
        let work_class = self
            .work_class
            .parse::<WorkClass>()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let job_kind = self
            .job_kind
            .parse::<JobKind>()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let owner_id = self
            .owner_id
            .parse::<ResourceId>()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        if self.owner_kind != owner_id.kind().descriptor().name
            || !is_job_kind_work_owner_triple(job_kind, work_class, owner_id.kind())
        {
            return Err(RepositoryError::InvalidInput(
                "work class and typed owner pair are not registered".to_owned(),
            ));
        }
        validate_digest(&self.request_digest)?;
        if let Some(digest) = &self.effect_key_digest {
            validate_digest(digest)?;
        }
        if self.attempt_limit <= 0 || self.deadline <= self.scheduled_at {
            return Err(RepositoryError::InvalidInput(
                "job attempt limit and time range are invalid".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RegistryValidationAccepted {
    pub job: JobRecord,
}

impl Deref for RegistryValidationAccepted {
    type Target = JobRecord;

    fn deref(&self) -> &Self::Target {
        &self.job
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RegistryValidationReceiptResult {
    schema_version: u32,
    job: JobRecord,
}

fn validate_claimed_job_payload(job: &JobRecord) -> Result<(), RepositoryError> {
    crate::recovery_isolation::job(
        validate_claimed_job_payload_inner(job),
        job,
        insight_platform_jobs::store::SafetyScanPhase::OwnerValidation,
    )
}
fn validate_claimed_job_payload_inner(job: &JobRecord) -> Result<(), RepositoryError> {
    let work_class = job
        .work_class
        .parse::<WorkClass>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let owner_id = job
        .owner_id
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    match work_class {
        WorkClass::RegistryValidation => {
            let payload: RegistryValidationJobPayload =
                decode_versioned_payload(&job.payload, "Registry validation Job")?;
            payload
                .validate_for_owner(&owner_id)
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))
        }
        WorkClass::Artifact => {
            let payload: ArtifactJobPayload = decode_typed_payload(&job.payload, "Artifact Job")?;
            payload
                .validate_for_owner(&owner_id)
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))
        }
        WorkClass::Mcp => {
            let payload: McpJobPayload = decode_typed_payload(&job.payload, "MCP Job")?;
            payload
                .validate_for_owner(&owner_id)
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))
        }
        WorkClass::Sandbox => {
            let payload: SandboxDispatcherJobPayloadV1 =
                decode_versioned_payload(&job.payload, "OpenSandbox Job")?;
            payload
                .validate_for(&job_projection(job)?)
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))
        }
        WorkClass::Context if owner_id.kind() == ResourceKind::ContextDataset => {
            let payload: ContextDatasetBuildJobPayload =
                decode_versioned_payload(&job.payload, "Context Dataset build Job")?;
            payload
                .validate_for_owner(&owner_id)
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))
        }
        WorkClass::Orchestration
        | WorkClass::Model
        | WorkClass::CapabilityNative
        | WorkClass::CapabilityRemote
        | WorkClass::Context
        | WorkClass::Interaction
        | WorkClass::Recovery => Ok(()),
    }
}

#[derive(Debug, Clone)]
pub struct ClaimJobs {
    pub work_class: String,
    /// The manifest of this physical executable, verified by the worker's
    /// deployment registration before it enters the claim port.
    pub worker_manifest: WorkerManifest,
    pub worker_id: ResourceId,
    pub limit: u16,
    pub lease_milliseconds: i64,
    pub lease_token_digests: Vec<Sha256Digest>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactWorkerRole {
    DataWorker,
    Maintenance,
}

impl ArtifactWorkerRole {
    const fn job_kinds(self) -> &'static [&'static str] {
        match self {
            Self::DataWorker => &["artifact_scan", "artifact_rescan"],
            Self::Maintenance => &["artifact_delete", "artifact_blob_cleanup"],
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClaimArtifactJobs {
    pub role: ArtifactWorkerRole,
    pub worker_manifest: WorkerManifest,
    pub worker_id: ResourceId,
    pub limit: u16,
    pub lease_milliseconds: i64,
    pub lease_token_digests: Vec<Sha256Digest>,
}

#[derive(Debug, Clone)]
struct DerivedExpressionCommitEvidence {
    materialized_inputs: Vec<CommittedExpressionInput>,
    evaluation: ControllerEvaluation,
    output_value_ids: Vec<ResourceId>,
}

#[derive(Debug, Clone)]
struct DerivedNewScopeBinding {
    scope_instance_id: ResourceId,
    port: ExactDataPortRef,
    value: ExactRunValueRef,
}

struct CommittedDerivedExpressionValues {
    source_scope_version: i64,
    new_scope_bindings: Vec<DerivedNewScopeBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum StoredControllerScopeDescriptor {
    ParallelLeg {
        leg_index: u32,
        leg_plan_node_key: PlanNodeKey,
        join_plan_node_key: PlanNodeKey,
    },
    LoopIteration {
        iteration: u32,
        loop_plan_node_key: PlanNodeKey,
    },
    MapItem {
        failure_policy: MapFailurePolicy,
        item_count: u32,
        item_index: u32,
        map_plan_node_key: PlanNodeKey,
        next_plan_node_key: PlanNodeKey,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredRootScopePayload {
    root_run_id: ResourceId,
    environment: ScopeDataEnvironmentSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredControllerScopePayload {
    controller_node_execution_id: ResourceId,
    descriptor: StoredControllerScopeDescriptor,
    environment: ScopeDataEnvironmentSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum StoredControllerWait {
    Join {
        policy: JoinPolicy,
        quorum: Option<u16>,
        remainder: Option<JoinRemainderPolicy>,
    },
    Loop {
        iteration: u32,
    },
    MapAdmission {
        body_plan_node_key: PlanNodeKey,
        failure_policy: MapFailurePolicy,
        item_count: u32,
        next_item_index: u32,
        next_plan_node_key: PlanNodeKey,
    },
    MapSettlement {
        failure_policy: MapFailurePolicy,
        item_count: u32,
        admitted_item_count: u32,
        next_plan_node_key: PlanNodeKey,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredPendingControllerNodePayload {
    controller_node_execution_id: ResourceId,
    expected_scope_ids: Vec<ResourceId>,
    plan_node_key: PlanNodeKey,
    wait: StoredControllerWait,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredHumanTaskWaitPayload {
    plan_node_key: PlanNodeKey,
    task_id: ResourceId,
    response_port: ExactDataPortRef,
    resolution: Option<StoredHumanTaskResolution>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredHumanTaskResolution {
    outcome: DurableWaitOutcome,
    response: Option<ExactRunValueRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredTimerWaitPayload {
    plan_node_key: PlanNodeKey,
    due_at: DateTime<Utc>,
    resolution: Option<DurableWaitOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredSignalWaitPayload {
    plan_node_key: PlanNodeKey,
    signal_key: String,
    payload_port: Option<ExactDataPortRef>,
    resolution: Option<StoredSignalResolution>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredSignalResolution {
    outcome: DurableWaitOutcome,
    payload: Option<ExactRunValueRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredContextQueryWaitPayload {
    plan_node_key: PlanNodeKey,
    plan_digest: Sha256Digest,
    source_orchestration_job_id: ResourceId,
    context_query_id: ResourceId,
    context_job_id: ResourceId,
    result_port: ExactDataPortRef,
    resume_plan_node_key: PlanNodeKey,
    resume_node_kind: PlanNodeKind,
    root_scope_id: ResourceId,
    continuation_attempt_limit: i32,
    retry_backoff_milliseconds: u64,
    priority: SchedulerPriority,
    deadline: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredModelTurnWaitPayload {
    pub(crate) plan_node_key: PlanNodeKey,
    pub(crate) plan_digest: Sha256Digest,
    pub(crate) source_orchestration_job_id: ResourceId,
    pub(crate) model_turn_id: ResourceId,
    pub(crate) model_job_id: ResourceId,
    pub(crate) output_port: ExactDataPortRef,
    pub(crate) resume_plan_node_key: PlanNodeKey,
    pub(crate) resume_node_kind: PlanNodeKind,
    pub(crate) root_scope_id: ResourceId,
    pub(crate) continuation_attempt_limit: i32,
    pub(crate) retry_backoff_milliseconds: u64,
    pub(crate) priority: SchedulerPriority,
    pub(crate) deadline: DateTime<Utc>,
    pub(crate) round_ordinal: u16,
    pub(crate) maximum_rounds: u16,
    pub(crate) total_capability_calls: u32,
    pub(crate) maximum_capability_calls: u32,
    pub(crate) maximum_parallel_calls_per_round: u16,
    pub(crate) token_budget: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredModelToolContinuationWaitPayload {
    plan_node_key: PlanNodeKey,
    plan_digest: Sha256Digest,
    source_orchestration_job_id: ResourceId,
    model_turn_id: ResourceId,
    model_job_id: ResourceId,
    response_value_id: ResourceId,
    response_digest: Sha256Digest,
    round_ordinal: u16,
    tool_intent_count: u16,
    total_capability_calls: u32,
    output_port: ExactDataPortRef,
    resume_plan_node_key: PlanNodeKey,
    resume_node_kind: PlanNodeKind,
    root_scope_id: ResourceId,
    continuation_attempt_limit: i32,
    retry_backoff_milliseconds: u64,
    priority: SchedulerPriority,
    deadline: DateTime<Utc>,
    maximum_rounds: u16,
    maximum_capability_calls: u32,
    maximum_parallel_calls_per_round: u16,
    token_budget: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredModelToolCallWait {
    pub(crate) call_id: String,
    pub(crate) call_id_digest: Sha256Digest,
    pub(crate) invocation_id: ResourceId,
    pub(crate) capability_job_id: ResourceId,
    pub(crate) input_value_id: ResourceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) result: Option<insight_platform_orchestrator::ModelToolResultReference>,
    pub(crate) sibling_cancel_event_id: ResourceId,
    pub(crate) sibling_cancel_outbox_id: ResourceId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredModelToolBatchWaitPayload {
    pub(crate) continuation: StoredModelToolContinuationWaitPayload,
    pub(crate) calls: Vec<StoredModelToolCallWait>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredCapabilityInvocationWaitPayload {
    pub(crate) plan_node_key: PlanNodeKey,
    pub(crate) plan_digest: Sha256Digest,
    pub(crate) source_orchestration_job_id: ResourceId,
    pub(crate) invocation_id: ResourceId,
    pub(crate) capability_job_id: Option<ResourceId>,
    pub(crate) output_port: ExactDataPortRef,
    pub(crate) resume_plan_node_key: PlanNodeKey,
    pub(crate) resume_node_kind: PlanNodeKind,
    pub(crate) root_scope_id: ResourceId,
    pub(crate) continuation_attempt_limit: i32,
    pub(crate) retry_backoff_milliseconds: u64,
    pub(crate) priority: SchedulerPriority,
    pub(crate) deadline: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum StoredControllerPort {
    Failure,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredControllerControlToken {
    source_node_execution_id: ResourceId,
    source_port: StoredControllerPort,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredErrorBoundaryHandlerPayload {
    error_boundary_node_id: ResourceId,
    failure_digest: Sha256Digest,
    plan_node_key: PlanNodeKey,
    plan_source_digest: Sha256Digest,
    required_control_tokens: Vec<StoredControllerControlToken>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredChildRunLinkPayload {
    slot_id: String,
    source_value_ids: Vec<ResourceId>,
    child_root_scope_id: ResourceId,
    child_entry_node_execution_id: ResourceId,
    child_orchestration_job_id: ResourceId,
    link: ChildRunLinkPayload,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredChildRunWaitPayload {
    plan_node_key: PlanNodeKey,
    plan_digest: Sha256Digest,
    source_orchestration_job_id: ResourceId,
    child_link_id: ResourceId,
    child_run_id: ResourceId,
    output_port: ExactDataPortRef,
    resume_plan_node_key: PlanNodeKey,
    resume_node_kind: PlanNodeKind,
    root_scope_id: ResourceId,
    continuation_attempt_limit: i32,
    retry_backoff_milliseconds: u64,
    priority: SchedulerPriority,
    deadline: DateTime<Utc>,
}

pub(crate) fn safety_scan_page<T>(
    records: Vec<T>,
    scanned_count: usize,
    limit: u16,
    last_cursor: Option<SafetyScanCursor>,
) -> SafetyScanPage<T> {
    let exhausted = scanned_count < usize::from(limit);
    SafetyScanPage {
        records,
        diagnostics: Vec::new(),
        next_cursor: (!exhausted).then_some(last_cursor).flatten(),
        exhausted,
    }
}

pub(crate) fn safety_scan_cursor_from_row(
    row: &PgRow,
    item_column: &str,
    item_kind: ResourceKind,
) -> Result<SafetyScanCursor, RepositoryError> {
    let cursor = SafetyScanCursor {
        sort_at: row.try_get("scan_sort_at")?,
        tenant_id: row.try_get::<String, _>("tenant_id")?.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?,
        item_id: row.try_get::<String, _>(item_column)?.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?,
    };
    cursor.validate(item_kind)?;
    Ok(cursor)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OrchestrationWakeReceiptPayload {
    expected_job_version: i64,
    expected_wake_generation: u64,
    job_id: ResourceId,
    source: String,
    signal_key: Option<String>,
    signal_payload_evidence: Option<Value>,
    signal_principal: Option<PrincipalSnapshot>,
}

impl ClaimJobs {
    fn validate(&self) -> Result<(), RepositoryError> {
        let work_class = self
            .work_class
            .parse::<WorkClass>()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        validate_claim_manifest(&self.worker_manifest, work_class)?;
        if matches!(
            work_class,
            WorkClass::Sandbox | WorkClass::Artifact | WorkClass::RegistryValidation
        ) {
            return Err(RepositoryError::InvalidInput(
                "Sandbox, Artifact, and RegistryValidation Jobs require dedicated role-gated claim paths"
                    .to_owned(),
            ));
        }
        validate_claim_bounds(
            &self.worker_id,
            self.limit,
            self.lease_milliseconds,
            &self.lease_token_digests,
        )
    }
}

impl ClaimArtifactJobs {
    fn validate(&self) -> Result<(), RepositoryError> {
        validate_claim_manifest(&self.worker_manifest, WorkClass::Artifact)?;
        validate_claim_bounds(
            &self.worker_id,
            self.limit,
            self.lease_milliseconds,
            &self.lease_token_digests,
        )
    }
}

pub(crate) fn validate_claim_manifest(
    manifest: &WorkerManifest,
    work_class: WorkClass,
) -> Result<(), RepositoryError> {
    manifest
        .validate()
        .map_err(|error| RepositoryError::InvalidInput(error.to_string()))?;
    if manifest.work_class != work_class {
        return Err(RepositoryError::InvalidInput(
            "worker manifest work class does not match the claim lane".into(),
        ));
    }
    Ok(())
}

fn validate_claim_bounds(
    worker_id: &ResourceId,
    limit: u16,
    lease_milliseconds: i64,
    lease_token_digests: &[Sha256Digest],
) -> Result<(), RepositoryError> {
    if worker_id.kind() != ResourceKind::WorkerProcessGeneration {
        return Err(RepositoryError::InvalidInput(
            "worker ID must identify a WorkerProcessGeneration".to_owned(),
        ));
    }
    let unique_tokens = lease_token_digests
        .iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    if limit == 0
        || limit > 256
        || lease_milliseconds <= 0
        || lease_milliseconds > MAX_JOB_LEASE_MILLISECONDS
        || lease_token_digests.len() != usize::from(limit)
        || unique_tokens.len() != lease_token_digests.len()
    {
        return Err(RepositoryError::InvalidInput(
            "claim limit or lease is outside the platform bound".to_owned(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobTerminalState {
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

impl JobTerminalState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
        }
    }

    pub const fn job_state(self) -> JobState {
        match self {
            Self::Succeeded => JobState::Succeeded,
            Self::Failed => JobState::Failed,
            Self::Cancelled => JobState::Cancelled,
            Self::TimedOut => JobState::TimedOut,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CommitJob {
    pub fence: JobFence,
    pub terminal_state: JobTerminalState,
    pub result_digest: String,
    pub result_payload: TypedPayload,
    pub receipt_id: String,
    pub idempotency_key_digest: String,
    pub request_digest: String,
    pub receipt_payload: TypedPayload,
    pub receipt_expires_at: DateTime<Utc>,
    pub event_id: String,
    pub event_type: String,
    pub event_payload: TypedPayload,
    pub outbox_id: String,
}

/// Fenced terminal mutation owned exclusively by the Registry Validation Worker.
///
/// The Job keeps its immutable request payload so that the public Operation projection can
/// continue to expose the resource target after completion.  The validated draft and the Job
/// terminal state are committed in one PostgreSQL transaction.
#[derive(Debug, Clone)]
pub struct CommitRegistryValidation {
    pub compilation: Option<insight_platform_agent_compiler::AgentCompilationEvidenceV1>,
    pub fence: JobFence,
    pub validator_principal_id: ResourceId,
    pub validator_digest: Sha256Digest,
    pub validation_profile_digest: Sha256Digest,
    pub receipt_id: ResourceId,
    pub resource_event_id: ResourceId,
    pub resource_outbox_id: ResourceId,
    pub job_event_id: ResourceId,
    pub job_outbox_id: ResourceId,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub receipt_expires_at: DateTime<Utc>,
}

impl CommitRegistryValidation {
    fn validate(&self) -> Result<(), RepositoryError> {
        self.fence.validate()?;
        if self.validator_principal_id.kind() != ResourceKind::Principal
            || self.receipt_id.kind() != ResourceKind::Receipt
            || self.resource_event_id.kind() != ResourceKind::Event
            || self.resource_outbox_id.kind() != ResourceKind::OutboxEvent
            || self.job_event_id.kind() != ResourceKind::Event
            || self.job_outbox_id.kind() != ResourceKind::OutboxEvent
            || self.receipt_expires_at <= Utc::now()
        {
            return Err(RepositoryError::InvalidInput(
                "registry validation commit identity is invalid".to_owned(),
            ));
        }
        let identities = [
            self.receipt_id.to_string(),
            self.resource_event_id.to_string(),
            self.resource_outbox_id.to_string(),
            self.job_event_id.to_string(),
            self.job_outbox_id.to_string(),
        ];
        if identities.iter().collect::<BTreeSet<_>>().len() != identities.len() {
            return Err(RepositoryError::InvalidInput(
                "registry validation commit mutation IDs must be distinct".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RegistryValidationCommitOutcome {
    Committed {
        job: Box<JobRecord>,
        resource: Box<ResourceRecord>,
    },
    Replayed,
}

impl CommitJob {
    fn validate(&self) -> Result<(), RepositoryError> {
        self.fence.validate()?;
        for id in [&self.receipt_id, &self.event_id, &self.outbox_id] {
            validate_id(id)?;
        }
        validate_digest(&self.result_digest)?;
        validate_digest(&self.idempotency_key_digest)?;
        validate_digest(&self.request_digest)?;
        validate_event_type(&self.event_type)?;
        if self.receipt_expires_at <= Utc::now() {
            return Err(RepositoryError::InvalidInput(
                "receipt expiry must be in the future".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum JobCommitOutcome {
    Committed(Box<JobRecord>),
    Replayed { disposition: Option<String> },
}

#[derive(Debug, Clone)]
pub struct NewQuotaAccount {
    pub tenant_id: String,
    pub quota_account_id: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub work_class: String,
    pub metric: String,
    pub limit_value: i64,
    pub payload: TypedPayload,
}

impl NewQuotaAccount {
    fn validate(&self) -> Result<(), RepositoryError> {
        for id in [&self.tenant_id, &self.quota_account_id, &self.scope_id] {
            validate_id(id)?;
        }
        validate_code("quota scope kind", &self.scope_kind)?;
        validate_code("quota work class", &self.work_class)?;
        validate_event_type(&self.metric)?;
        if self.limit_value < 0 {
            return Err(RepositoryError::InvalidInput(
                "quota limit cannot be negative".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QuotaAccountRecord {
    pub tenant_id: String,
    pub quota_account_id: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub work_class: String,
    pub metric: String,
    pub limit_value: i64,
    pub reserved_value: i64,
    pub used_value: i64,
    pub version: i64,
    pub payload: TypedPayload,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ReserveQuota {
    pub tenant_id: String,
    pub quota_account_id: String,
    pub quota_entry_id: String,
    pub correlation_id: String,
    pub amount: i64,
    pub request_digest: String,
}

impl ReserveQuota {
    fn validate(&self) -> Result<(), RepositoryError> {
        validate_quota_mutation(
            &self.tenant_id,
            &self.quota_account_id,
            &self.quota_entry_id,
            &self.correlation_id,
            self.amount,
            &self.request_digest,
        )
    }
}

#[derive(Debug, Clone)]
pub struct SettleQuota {
    pub tenant_id: String,
    pub quota_account_id: String,
    pub quota_entry_id: String,
    pub correlation_id: String,
    pub used_amount: i64,
    pub request_digest: String,
}

impl SettleQuota {
    fn validate(&self) -> Result<(), RepositoryError> {
        validate_quota_mutation(
            &self.tenant_id,
            &self.quota_account_id,
            &self.quota_entry_id,
            &self.correlation_id,
            self.used_amount,
            &self.request_digest,
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum QuotaMutationOutcome {
    Applied(QuotaAccountRecord),
    Replayed(QuotaAccountRecord),
}

pub(crate) async fn claim_command_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &CommandAudit,
    scope_kind: &str,
    scope_id: &str,
    operation: &str,
) -> Result<bool, RepositoryError> {
    let payload = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "operation": operation,
            "principal_id": audit.principal_id,
            "scope_id": scope_id,
            "scope_kind": scope_kind,
        }),
        65_536,
    )?;
    let inserted = sqlx::query(
        r#"
        INSERT INTO insight_platform.receipts (
            tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
            dedupe_owner_id, operation, idempotency_key_digest, request_digest,
            state, payload_schema_version, payload, payload_digest, expires_at
        ) VALUES ($1, $2, 'command', $3, $4, $5, $6, $7, $8,
                  'processing', $9, $10, $11, $12)
        ON CONFLICT (
            tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
            operation, idempotency_key_digest
        ) DO NOTHING
        RETURNING receipt_id
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(audit.receipt_id.to_string())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(audit.principal_id.to_string())
    .bind(operation)
    .bind(audit.idempotency_key_digest.to_string())
    .bind(audit.request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(audit.receipt_expires_at)
    .fetch_optional(&mut **transaction)
    .await?;
    if inserted.is_some() {
        return Ok(false);
    }

    let existing = sqlx::query(
        r#"
        SELECT request_digest, state
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'command'
          AND scope_kind = $2 AND scope_id = $3 AND dedupe_owner_id = $4
          AND operation = $5 AND idempotency_key_digest = $6
        FOR UPDATE
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(audit.principal_id.to_string())
    .bind(operation)
    .bind(audit.idempotency_key_digest.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    let request_digest: String = existing.try_get("request_digest")?;
    if request_digest != audit.request_digest.to_string() {
        return Err(RepositoryError::IdempotencyConflict);
    }
    let state: String = existing.try_get("state")?;
    if state != "succeeded" {
        return Err(RepositoryError::Conflict("command receipt"));
    }
    Ok(true)
}

/// Resource creation is collection-scoped because the server generates the Resource ID. A replay
/// therefore resolves the first winner through the Receipt response reference rather than trusting
/// a newly generated candidate ID.
async fn claim_resource_create_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &CommandAudit,
) -> Result<Option<ResourceId>, RepositoryError> {
    let scope_id = audit.tenant_id.to_string();
    let payload = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "operation": "resource.create_draft",
            "principal_id": audit.principal_id,
            "scope_id": scope_id,
            "scope_kind": "resource_collection",
        }),
        65_536,
    )?;
    let inserted = sqlx::query(
        r#"
        INSERT INTO insight_platform.receipts (
            tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
            dedupe_owner_id, operation, idempotency_key_digest, request_digest,
            state, payload_schema_version, payload, payload_digest, expires_at
        ) VALUES ($1, $2, 'command', 'resource_collection', $1, $3,
                  'resource.create_draft', $4, $5, 'processing', $6, $7, $8, $9)
        ON CONFLICT (
            tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
            operation, idempotency_key_digest
        ) DO NOTHING
        RETURNING receipt_id
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(audit.receipt_id.to_string())
    .bind(audit.principal_id.to_string())
    .bind(audit.idempotency_key_digest.to_string())
    .bind(audit.request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(audit.receipt_expires_at)
    .fetch_optional(&mut **transaction)
    .await?;
    if inserted.is_some() {
        return Ok(None);
    }
    let existing = sqlx::query(
        r#"
        SELECT request_digest, state, response_reference_id
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'command'
          AND scope_kind = 'resource_collection' AND scope_id = $1
          AND dedupe_owner_id = $2 AND operation = 'resource.create_draft'
          AND idempotency_key_digest = $3
        FOR UPDATE
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(audit.principal_id.to_string())
    .bind(audit.idempotency_key_digest.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if existing.try_get::<String, _>("request_digest")? != audit.request_digest.to_string() {
        return Err(RepositoryError::IdempotencyConflict);
    }
    if existing.try_get::<String, _>("state")? != "succeeded" {
        return Err(RepositoryError::Conflict("command receipt"));
    }
    let resource_id = existing
        .try_get::<Option<String>, _>("response_reference_id")?
        .ok_or_else(|| RepositoryError::CorruptRow("create Receipt has no result".to_owned()))?
        .parse()
        .map_err(|_| RepositoryError::CorruptRow("create Receipt result is invalid".to_owned()))?;
    Ok(Some(resource_id))
}

async fn read_run_admission_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &CommandAudit,
    admission_scope_id: &ResourceId,
) -> Result<Option<RunRecord>, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT request_digest, state, payload_schema_version, payload, payload_digest
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'command'
          AND scope_kind = 'run_admission' AND scope_id = $2
          AND dedupe_owner_id = $3 AND operation = 'run.admit'
          AND idempotency_key_digest = $4
        FOR UPDATE
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(admission_scope_id.to_string())
    .bind(audit.principal_id.to_string())
    .bind(audit.idempotency_key_digest.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    if row.try_get::<String, _>("request_digest")? != audit.request_digest.to_string() {
        return Err(RepositoryError::IdempotencyConflict);
    }
    if row.try_get::<String, _>("state")? != "succeeded" {
        return Err(RepositoryError::Conflict("run admission receipt"));
    }
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let result: RunAdmissionReceiptResult =
        decode_versioned_payload(&payload, "run admission Receipt result")?;
    validate_run_admission_receipt_result(&result, &audit.tenant_id)?;
    Ok(Some(result.run))
}

async fn claim_run_admission_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &CommandAudit,
    admission_scope_id: &ResourceId,
) -> Result<Option<RunRecord>, RepositoryError> {
    let payload = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "operation": "run.admit",
            "principal_id": audit.principal_id,
            "scope_id": admission_scope_id,
            "scope_kind": "run_admission",
        }),
        65_536,
    )?;
    let inserted = sqlx::query(
        r#"
        INSERT INTO insight_platform.receipts (
            tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
            dedupe_owner_id, operation, idempotency_key_digest, request_digest,
            state, payload_schema_version, payload, payload_digest, expires_at
        ) VALUES ($1, $2, 'command', 'run_admission', $3, $4, 'run.admit',
                  $5, $6, 'processing', $7, $8, $9, $10)
        ON CONFLICT (
            tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
            operation, idempotency_key_digest
        ) DO NOTHING
        RETURNING receipt_id
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(audit.receipt_id.to_string())
    .bind(admission_scope_id.to_string())
    .bind(audit.principal_id.to_string())
    .bind(audit.idempotency_key_digest.to_string())
    .bind(audit.request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(audit.receipt_expires_at)
    .fetch_optional(&mut **transaction)
    .await?;
    if inserted.is_some() {
        return Ok(None);
    }
    read_run_admission_receipt(transaction, audit, admission_scope_id)
        .await?
        .map(Some)
        .ok_or(RepositoryError::Conflict(
            "run admission Receipt disappeared",
        ))
}

fn validate_run_admission_receipt_result(
    result: &RunAdmissionReceiptResult,
    tenant_id: &ResourceId,
) -> Result<(), RepositoryError> {
    if result.schema_version != 1
        || result.run.tenant_id != tenant_id.to_string()
        || result.run.version != 1
        || result.run.state != RunState::Queued.as_str()
    {
        return Err(RepositoryError::CorruptRow(
            "run admission Receipt result is inconsistent".to_owned(),
        ));
    }
    result.run.current.validate(
        &result
            .run
            .run_id
            .parse()
            .map_err(|_| RepositoryError::CorruptRow("Run Receipt ID is invalid".to_owned()))?,
    )?;
    Ok(())
}

async fn terminalize_run_admission_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &CommandAudit,
    record: &RunRecord,
) -> Result<(), RepositoryError> {
    let result = RunAdmissionReceiptResult {
        schema_version: 1,
        run: record.clone(),
    };
    let payload = TypedPayload::from_versioned(1, &result, 1_048_576)?;
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.receipts
        SET state = 'succeeded', disposition = 'admitted', response_reference_id = $4,
            payload_schema_version = $5, payload = $6, payload_digest = $7,
            completed_at = clock_timestamp()
        WHERE tenant_id = $1 AND receipt_id = $2 AND request_digest = $3
          AND state = 'processing'
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(audit.receipt_id.to_string())
    .bind(audit.request_digest.to_string())
    .bind(&record.run_id)
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict("run admission receipt"));
    }
    Ok(())
}

pub(crate) async fn terminalize_command_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &CommandAudit,
    response_reference_id: &str,
    disposition: &str,
) -> Result<(), RepositoryError> {
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.receipts
        SET state = 'succeeded', disposition = $4, response_reference_id = $5,
            completed_at = clock_timestamp()
        WHERE tenant_id = $1 AND receipt_id = $2 AND request_digest = $3
          AND state = 'processing'
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(audit.receipt_id.to_string())
    .bind(audit.request_digest.to_string())
    .bind(disposition)
    .bind(response_reference_id)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict("command receipt"));
    }
    Ok(())
}

pub(crate) async fn load_command_receipt_response_reference(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &CommandAudit,
    scope_kind: &str,
    scope_id: &str,
    operation: &str,
) -> Result<String, RepositoryError> {
    sqlx::query_scalar(
        r#"
        SELECT response_reference_id
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'command'
          AND scope_kind = $2 AND scope_id = $3 AND dedupe_owner_id = $4
          AND operation = $5 AND idempotency_key_digest = $6
          AND request_digest = $7 AND state = 'succeeded'
        FOR UPDATE
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(scope_kind)
    .bind(scope_id)
    .bind(audit.principal_id.to_string())
    .bind(operation)
    .bind(audit.idempotency_key_digest.to_string())
    .bind(audit.request_digest.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .flatten()
    .ok_or_else(|| RepositoryError::CorruptRow("command Receipt result is missing".to_owned()))
}

async fn load_resource_projection_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &CommandAudit,
    resource_id: &ResourceId,
    operation: &str,
    result_name: &str,
) -> Result<ResourceUpdateReceiptResult, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT payload_schema_version, payload, payload_digest
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'command'
          AND scope_kind = 'resource' AND scope_id = $2 AND dedupe_owner_id = $3
          AND operation = $4 AND idempotency_key_digest = $5
          AND request_digest = $6 AND state = 'succeeded'
        FOR UPDATE
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(resource_id.to_string())
    .bind(audit.principal_id.to_string())
    .bind(operation)
    .bind(audit.idempotency_key_digest.to_string())
    .bind(audit.request_digest.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| RepositoryError::CorruptRow(format!("{result_name} is missing")))?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    decode_versioned_payload(&payload, result_name)
}

async fn terminalize_resource_projection_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &CommandAudit,
    record: &ResourceRecord,
    disposition: &str,
) -> Result<(), RepositoryError> {
    let result = ResourceUpdateReceiptResult::from_record(record)?;
    let payload = TypedPayload::from_versioned(1, &result, 262_144)?;
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.receipts
        SET state = 'succeeded', disposition = $4, response_reference_id = $5,
            payload_schema_version = $6, payload = $7, payload_digest = $8,
            completed_at = clock_timestamp()
        WHERE tenant_id = $1 AND receipt_id = $2 AND request_digest = $3
          AND state = 'processing'
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(audit.receipt_id.to_string())
    .bind(audit.request_digest.to_string())
    .bind(disposition)
    .bind(&record.resource_id)
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict("command receipt"));
    }
    Ok(())
}

async fn load_registry_validation_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &CommandAudit,
    resource_id: &ResourceId,
) -> Result<RegistryValidationAccepted, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT payload_schema_version, payload, payload_digest
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'command'
          AND scope_kind = 'resource' AND scope_id = $2 AND dedupe_owner_id = $3
          AND operation = 'resource.validate' AND idempotency_key_digest = $4
          AND request_digest = $5 AND state = 'succeeded'
        FOR UPDATE
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(resource_id.to_string())
    .bind(audit.principal_id.to_string())
    .bind(audit.idempotency_key_digest.to_string())
    .bind(audit.request_digest.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| {
        RepositoryError::CorruptRow("registry validation Receipt result is missing".to_owned())
    })?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let result: RegistryValidationReceiptResult =
        decode_versioned_payload(&payload, "registry validation Receipt result")?;
    if result.schema_version != 1
        || result.job.tenant_id != audit.tenant_id.to_string()
        || result.job.work_class != WorkClass::RegistryValidation.as_str()
        || result.job.state != JobState::Ready.as_str()
        || result.job.version != 1
    {
        return Err(RepositoryError::CorruptRow(
            "registry validation Receipt result is inconsistent".to_owned(),
        ));
    }
    validate_claimed_job_payload(&result.job)?;
    Ok(RegistryValidationAccepted { job: result.job })
}

async fn terminalize_registry_validation_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &CommandAudit,
    resource_id: &ResourceId,
    accepted: &RegistryValidationAccepted,
) -> Result<(), RepositoryError> {
    let result = RegistryValidationReceiptResult {
        schema_version: 1,
        job: accepted.job.clone(),
    };
    let payload = TypedPayload::from_versioned(1, &result, 262_144)?;
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.receipts
        SET state = 'succeeded', disposition = 'accepted', response_reference_id = $4,
            payload_schema_version = $5, payload = $6, payload_digest = $7,
            completed_at = clock_timestamp()
        WHERE tenant_id = $1 AND receipt_id = $2 AND request_digest = $3
          AND scope_kind = 'resource' AND scope_id = $8 AND state = 'processing'
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(audit.receipt_id.to_string())
    .bind(audit.request_digest.to_string())
    .bind(&accepted.job.job_id)
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(resource_id.to_string())
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict("command receipt"));
    }
    Ok(())
}

async fn load_resource_publish_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &CommandAudit,
    resource_id: &ResourceId,
) -> Result<PublishedResource, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT payload_schema_version, payload, payload_digest
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'command'
          AND scope_kind = 'resource' AND scope_id = $2 AND dedupe_owner_id = $3
          AND operation = 'resource.publish' AND idempotency_key_digest = $4
          AND request_digest = $5 AND state = 'succeeded'
        FOR UPDATE
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(resource_id.to_string())
    .bind(audit.principal_id.to_string())
    .bind(audit.idempotency_key_digest.to_string())
    .bind(audit.request_digest.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| {
        RepositoryError::CorruptRow("resource publish Receipt result is missing".to_owned())
    })?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let result: ResourcePublishReceiptResult =
        decode_versioned_payload(&payload, "resource publish Receipt result")?;
    if result.schema_version != 1
        || result.tenant_id != audit.tenant_id.to_string()
        || result.resource_id != resource_id.to_string()
        || result.resource_version_ids.is_empty()
        || result.resource_version_ids.len() > 2
    {
        return Err(RepositoryError::CorruptRow(
            "resource publish Receipt result is inconsistent".to_owned(),
        ));
    }
    let mut versions = Vec::with_capacity(result.resource_version_ids.len());
    for version_id in &result.resource_version_ids {
        let row = sqlx::query(
            r#"
            SELECT tenant_id, resource_version_id, resource_id, resource_version_kind,
                   revision_no, content_digest, artifact_id, payload_schema_version,
                   payload, payload_digest, created_by, created_at
            FROM insight_platform.resource_versions
            WHERE tenant_id = $1 AND resource_id = $2 AND resource_version_id = $3
            "#,
        )
        .bind(&result.tenant_id)
        .bind(&result.resource_id)
        .bind(version_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| {
            RepositoryError::CorruptRow(
                "resource publish Receipt immutable version is missing".to_owned(),
            )
        })?;
        versions.push(resource_version_from_row(row)?);
    }
    let first_payload: PublishedVersionPayload =
        decode_typed_payload(&versions[0].payload, "published ResourceVersion")?;
    for version in versions.iter().skip(1) {
        let payload: PublishedVersionPayload =
            decode_typed_payload(&version.payload, "published ResourceVersion")?;
        if payload != first_payload {
            return Err(RepositoryError::CorruptRow(
                "resource publish Receipt version batch payloads differ".to_owned(),
            ));
        }
    }
    let draft = ResourceDraftPayload {
        alias: result.alias,
        display_name: result.display_name,
        document: first_payload.document,
        validation: Some(first_payload.validation),
    };
    draft
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let resource_payload = TypedPayload::new(1, &draft)?;
    if resource_payload.digest != result.resource_payload_digest {
        return Err(RepositoryError::CorruptRow(
            "resource publish Receipt resource payload digest differs".to_owned(),
        ));
    }
    Ok(PublishedResource {
        resource: ResourceRecord {
            tenant_id: result.tenant_id,
            resource_id: result.resource_id,
            resource_kind: result.resource_kind,
            lifecycle_state: result.lifecycle_state,
            gate_state: result.gate_state,
            draft_generation: result.draft_generation,
            active_version_id: result.active_version_id,
            active_deployment_id: result.active_deployment_id,
            version: result.version,
            payload: resource_payload,
            created_at: result.created_at,
            updated_at: result.updated_at,
        },
        versions,
    })
}

async fn terminalize_resource_publish_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &CommandAudit,
    published: &PublishedResource,
    display_name: &str,
) -> Result<(), RepositoryError> {
    let result = ResourcePublishReceiptResult {
        alias: decode_resource_draft(&published.resource.payload)?.alias,
        schema_version: 1,
        tenant_id: published.resource.tenant_id.clone(),
        resource_id: published.resource.resource_id.clone(),
        resource_kind: published.resource.resource_kind.clone(),
        lifecycle_state: published.resource.lifecycle_state.clone(),
        gate_state: published.resource.gate_state.clone(),
        draft_generation: published.resource.draft_generation,
        active_version_id: published.resource.active_version_id.clone(),
        active_deployment_id: published.resource.active_deployment_id.clone(),
        version: published.resource.version,
        resource_payload_digest: published.resource.payload.digest.clone(),
        display_name: display_name.to_owned(),
        resource_version_ids: published
            .versions
            .iter()
            .map(|version| version.resource_version_id.clone())
            .collect(),
        created_at: published.resource.created_at,
        updated_at: published.resource.updated_at,
    };
    let payload = TypedPayload::from_versioned(1, &result, 262_144)?;
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.receipts
        SET state = 'succeeded', disposition = 'published', response_reference_id = $4,
            payload_schema_version = $5, payload = $6, payload_digest = $7,
            completed_at = clock_timestamp()
        WHERE tenant_id = $1 AND receipt_id = $2 AND request_digest = $3
          AND state = 'processing'
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(audit.receipt_id.to_string())
    .bind(audit.request_digest.to_string())
    .bind(&published.resource.resource_id)
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict("command receipt"));
    }
    Ok(())
}

pub(crate) async fn append_command_event(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &CommandAudit,
    aggregate_kind: &str,
    aggregate_id: &str,
    aggregate_version: i64,
    event_type: &str,
    payload: &TypedPayload,
) -> Result<(), RepositoryError> {
    append_command_event_version(
        transaction,
        audit,
        aggregate_kind,
        aggregate_id,
        Some(aggregate_version),
        event_type,
        payload,
    )
    .await
}

async fn append_command_event_version(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &CommandAudit,
    aggregate_kind: &str,
    aggregate_id: &str,
    aggregate_version: Option<i64>,
    event_type: &str,
    payload: &TypedPayload,
) -> Result<(), RepositoryError> {
    let run_id = (aggregate_kind == "run").then_some(aggregate_id);
    append_command_event_version_for_run(
        transaction,
        audit,
        aggregate_kind,
        aggregate_id,
        aggregate_version,
        run_id,
        event_type,
        payload,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn append_command_event_version_for_run(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &CommandAudit,
    aggregate_kind: &str,
    aggregate_id: &str,
    aggregate_version: Option<i64>,
    run_id: Option<&str>,
    event_type: &str,
    payload: &TypedPayload,
) -> Result<(), RepositoryError> {
    let public_event_type = public_run_event_type(aggregate_kind, event_type, &payload.value);
    let public_sequence = match (run_id, public_event_type) {
        (Some(run_id), Some(_)) => {
            Some(next_public_run_sequence(transaction, &audit.tenant_id.to_string(), run_id).await?)
        }
        _ => None,
    };
    let visibility = if public_sequence.is_some() {
        "public"
    } else {
        "internal"
    };
    sqlx::query(
        r#"
        INSERT INTO insight_platform.events (
            tenant_id, event_id, aggregate_kind, aggregate_id, aggregate_version,
            trace_id, run_id, public_sequence, event_type, visibility,
            payload_schema_version, payload, payload_digest
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(audit.event_id.to_string())
    .bind(aggregate_kind)
    .bind(aggregate_id)
    .bind(aggregate_version)
    .bind(audit.trace.trace_id.to_string())
    .bind(run_id)
    .bind(public_sequence)
    .bind(event_type)
    .bind(visibility)
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.outbox_events (tenant_id, outbox_id, event_id, trace_id)
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(audit.outbox_id.to_string())
    .bind(audit.event_id.to_string())
    .bind(audit.trace.trace_id.to_string())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn next_public_run_sequence(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    run_id: &str,
) -> Result<i64, RepositoryError> {
    sqlx::query_scalar(
        r#"
        UPDATE insight_platform.runs
        SET public_sequence = public_sequence + 1
        WHERE tenant_id = $1 AND run_id = $2
        RETURNING public_sequence
        "#,
    )
    .bind(tenant_id)
    .bind(run_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("public Run event authority"))
}

fn public_run_event_type(
    aggregate_kind: &str,
    event_type: &str,
    payload: &Value,
) -> Option<PublicRunEventType> {
    use PublicRunEventType as Public;

    match (aggregate_kind, event_type) {
        ("model_turn", name) => name.parse::<Public>().ok().filter(|event| {
            event.durable_source_kind()
                == Some(insight_platform_contracts::PublicRunEventSourceKind::ModelTurn)
        }),
        ("run", "run.admitted") => Some(Public::RunQueued),
        ("run", "run.pause_requested") => Some(Public::RunPaused),
        ("run", "run.pause_resumed") => Some(Public::RunResumed),
        ("run", "run.cancel_requested") => Some(Public::RunCancelling),
        ("run", "run.task_waiting" | "run.child_waiting") => Some(Public::RunWaiting),
        ("run", "run.failed") => Some(Public::RunFailed),
        ("run", "run.terminal_committed" | "run.terminal_converged") => {
            public_terminal_run_event(payload)
        }
        ("node_execution", "node.started") => Some(Public::NodeStarted),
        ("node_execution", "node.controller_completed") => Some(Public::NodeCompleted),
        ("node_execution", "node.failed") => Some(Public::NodeFailed),
        ("node_execution", "node.cancelled") => Some(Public::NodeCancelled),
        ("node_execution", "node.terminal_committed" | "node.terminal_converged") => {
            public_terminal_node_event(payload)
        }
        ("child_run_link", "child.started") => Some(Public::ChildStarted),
        ("child_run_link", "child.completed") => public_terminal_child_event(payload),
        ("interaction", "interaction.required") => Some(Public::InteractionRequired),
        ("interaction", "interaction.respond" | "interaction.expired") => {
            Some(Public::InteractionResolved)
        }
        _ => None,
    }
}

fn public_terminal_run_event(payload: &Value) -> Option<PublicRunEventType> {
    use PublicRunEventType as Public;
    match payload.get("terminal_state").and_then(Value::as_str) {
        Some("succeeded") => Some(Public::RunCompleted),
        Some("failed") => Some(Public::RunFailed),
        Some("cancelled") => Some(Public::RunCancelled),
        Some("timed_out") => Some(Public::RunTimedOut),
        _ => None,
    }
}

fn public_terminal_node_event(payload: &Value) -> Option<PublicRunEventType> {
    use PublicRunEventType as Public;
    match payload.get("terminal_state").and_then(Value::as_str) {
        Some("succeeded") => Some(Public::NodeCompleted),
        Some("failed") => Some(Public::NodeFailed),
        Some("cancelled") => Some(Public::NodeCancelled),
        Some("timed_out") => Some(Public::NodeTimedOut),
        _ => None,
    }
}

fn public_terminal_child_event(payload: &Value) -> Option<PublicRunEventType> {
    use PublicRunEventType as Public;
    match payload.get("state").and_then(Value::as_str) {
        Some("succeeded") => Some(Public::ChildCompleted),
        Some("failed") => Some(Public::ChildFailed),
        Some("cancelled") => Some(Public::ChildCancelled),
        Some("timed_out") => Some(Public::ChildTimedOut),
        _ => Some(Public::ChildCompleted),
    }
}

fn public_run_event_from_row(row: PgRow) -> Result<PublicRunEventRecord, RepositoryError> {
    let aggregate_kind: String = row.try_get("aggregate_kind")?;
    let stored_event_type: String = row.try_get("event_type")?;
    let payload: Value = row.try_get("payload")?;
    let event_type = public_run_event_type(&aggregate_kind, &stored_event_type, &payload)
        .ok_or_else(|| {
            RepositoryError::CorruptRow(
                "public Run Event has no closed public projection".to_owned(),
            )
        })?;
    let event_id = row
        .try_get::<String, _>("event_id")?
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let source_id = row
        .try_get::<String, _>("aggregate_id")?
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let trace_id = row
        .try_get::<String, _>("trace_id")?
        .parse::<TraceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let source_kind = event_type.durable_source_kind().ok_or_else(|| {
        RepositoryError::CorruptRow("durable public Event has no source kind".to_owned())
    })?;
    let source_projection_version = u64::try_from(
        row.try_get::<Option<i64>, _>("aggregate_version")?
            .ok_or_else(|| {
                RepositoryError::CorruptRow("public Event source version is absent".to_owned())
            })?,
    )
    .map_err(|_| RepositoryError::CorruptRow("invalid public Event source version".to_owned()))?;
    let sequence = u64::try_from(
        row.try_get::<Option<i64>, _>("public_sequence")?
            .ok_or_else(|| {
                RepositoryError::CorruptRow("public Event sequence is absent".to_owned())
            })?,
    )
    .map_err(|_| RepositoryError::CorruptRow("invalid public Event sequence".to_owned()))?;
    if event_id.kind() != ResourceKind::Event || source_id.kind() != source_kind.resource_kind() {
        return Err(RepositoryError::CorruptRow(
            "public Event nominal identity disagrees with its projection".to_owned(),
        ));
    }
    Ok(PublicRunEventRecord {
        event_id,
        trace_id,
        sequence,
        event_type,
        source_id,
        source_projection_version,
        safe_summary: row
            .try_get::<Option<String>, _>("model_failure_message")?
            .as_deref()
            .and_then(insight_platform_models::public_model_failure_summary)
            .map(str::to_owned),
        occurred_at: row.try_get("occurred_at")?,
    })
}

pub(crate) async fn load_tenant(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
) -> Result<TenantRecord, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT tenant_id, state, version, config_schema_version, config,
               config_digest, created_at, updated_at
        FROM insight_platform.tenants
        WHERE tenant_id = $1
        "#,
    )
    .bind(tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("tenant"))?;
    tenant_from_row(row)
}

async fn load_tenant_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
) -> Result<TenantRecord, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT tenant_id, state, version, config_schema_version, config,
               config_digest, created_at, updated_at
        FROM insight_platform.tenants
        WHERE tenant_id = $1
        FOR UPDATE
        "#,
    )
    .bind(tenant_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("tenant"))?;
    tenant_from_row(row)
}

pub(crate) fn decode_published_version_payload(
    payload: &TypedPayload,
) -> Result<PublishedVersionPayload, RepositoryError> {
    let mut value = payload.value.clone();
    let Value::Object(object) = &mut value else {
        return Err(RepositoryError::CorruptRow(
            "published ResourceVersion payload is not an object".to_owned(),
        ));
    };
    object.remove("schema_version");
    serde_json::from_value(value)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))
}

pub(crate) fn decode_typed_payload<T: DeserializeOwned>(
    payload: &TypedPayload,
    kind: &str,
) -> Result<T, RepositoryError> {
    let mut value = payload.value.clone();
    let Value::Object(object) = &mut value else {
        return Err(RepositoryError::CorruptRow(format!(
            "{kind} payload is not an object"
        )));
    };
    object.remove("schema_version");
    serde_json::from_value(value)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))
}

pub(crate) fn decode_versioned_payload<T: DeserializeOwned>(
    payload: &TypedPayload,
    kind: &str,
) -> Result<T, RepositoryError> {
    serde_json::from_value(payload.value.clone())
        .map_err(|failure| RepositoryError::CorruptRow(format!("{kind}: {failure}")))
}

pub(crate) async fn require_tenant_permission(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &CommandAudit,
    permission: Permission,
) -> Result<PrincipalSnapshot, RepositoryError> {
    let snapshot = load_current_principal_snapshot(
        transaction,
        &audit.tenant_id,
        &audit.principal_id,
        audit.principal_kind,
    )
    .await?;
    if !snapshot.permissions.contains(permission) {
        return Err(RepositoryError::PermissionDenied);
    }
    Ok(snapshot)
}

pub(crate) async fn load_current_principal_snapshot(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    principal_kind: PrincipalKind,
) -> Result<PrincipalSnapshot, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT principal.version AS principal_version,
               binding.generation AS binding_generation,
               binding.version AS binding_version,
               binding.permissions_schema_version,
               binding.permissions,
               binding.permissions_digest
        FROM insight_platform.tenant_principals AS binding
        JOIN insight_platform.principals AS principal
          ON principal.principal_id = binding.principal_id
        WHERE binding.tenant_id = $1 AND binding.principal_id = $2
          AND binding.principal_kind = $3
          AND binding.state = 'active' AND principal.state = 'active'
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(principal_id.to_string())
    .bind(principal_kind.as_str())
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(row) = row else {
        return Err(RepositoryError::PermissionDenied);
    };
    let permissions_payload = payload_from_row(
        &row,
        "permissions_schema_version",
        "permissions",
        "permissions_digest",
    )?;
    let permissions: TenantPrincipalPayload =
        decode_typed_payload(&permissions_payload, "tenant principal permissions")?;
    let snapshot = PrincipalSnapshot::build(
        tenant_id.clone(),
        principal_id.clone(),
        principal_kind,
        permissions.permissions,
        u64::try_from(row.try_get::<i64, _>("principal_version")?)
            .map_err(|_| RepositoryError::CorruptRow("negative principal version".to_owned()))?,
        u64::try_from(row.try_get::<i64, _>("binding_generation")?)
            .map_err(|_| RepositoryError::CorruptRow("negative binding generation".to_owned()))?,
        u64::try_from(row.try_get::<i64, _>("binding_version")?)
            .map_err(|_| RepositoryError::CorruptRow("negative binding version".to_owned()))?,
    )
    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    if snapshot.permissions_digest.as_str() != permissions_payload.digest {
        return Err(RepositoryError::CorruptRow(
            "tenant principal permissions digest is inconsistent".to_owned(),
        ));
    }
    Ok(snapshot)
}

async fn require_ready_authoring_artifact(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    package: &AuthoringPackage,
) -> Result<(), RepositoryError> {
    require_ready_artifact_ref(
        transaction,
        tenant_id,
        &package.artifact,
        "ready authoring artifact",
    )
    .await
}

async fn require_ready_typed_plan_artifact(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    document: &ResourceDocument,
) -> Result<(), RepositoryError> {
    let Some((artifact_id, content_digest)) = document.typed_plan_artifact() else {
        return Ok(());
    };
    let matched: Option<i32> = sqlx::query_scalar(
        r#"
        SELECT 1
        FROM insight_platform.artifacts AS artifact
        JOIN insight_platform.artifact_blobs AS blob
          ON blob.tenant_id = artifact.tenant_id AND blob.blob_id = artifact.blob_id
        WHERE artifact.tenant_id = $1 AND artifact.artifact_id = $2
          AND artifact.purpose = 'typed_plan'
          AND artifact.state = 'ready' AND artifact.terminal_at IS NULL
          AND artifact.expected_digest = $3 AND artifact.verified_media_type = 'application/json'
          AND blob.state = 'verified' AND blob.deleted_at IS NULL
          AND blob.content_digest = $3 AND blob.size_bytes = artifact.expected_size_bytes
        FOR SHARE OF artifact, blob
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(artifact_id.to_string())
    .bind(content_digest.to_string())
    .fetch_optional(&mut **transaction)
    .await?;
    if matched.is_none() {
        return Err(RepositoryError::NotFound("ready typed Plan artifact"));
    }
    Ok(())
}

async fn require_ready_sandbox_runtime_bundle(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    document: &ResourceDocument,
) -> Result<(), RepositoryError> {
    let ResourceDocument::SandboxPackage(package) = document else {
        return Ok(());
    };
    require_ready_artifact_ref(
        transaction,
        tenant_id,
        &package.source_artifact,
        "ready OpenSandbox Package source artifact",
    )
    .await?;
    require_ready_artifact_ref(
        transaction,
        tenant_id,
        &package.build_evidence,
        "ready OpenSandbox Package build evidence",
    )
    .await
}

async fn require_ready_artifact_ref(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    artifact: &ArtifactRef,
    not_found_label: &'static str,
) -> Result<(), RepositoryError> {
    let matched: Option<i32> = sqlx::query_scalar(
        r#"
        SELECT 1
        FROM insight_platform.artifacts AS artifact
        JOIN insight_platform.artifact_blobs AS blob
          ON blob.tenant_id = artifact.tenant_id AND blob.blob_id = artifact.blob_id
        WHERE artifact.tenant_id = $1 AND artifact.artifact_id = $2
          AND artifact.state = 'ready' AND artifact.terminal_at IS NULL
          AND blob.state = 'verified' AND blob.deleted_at IS NULL
          AND blob.content_digest = $3 AND blob.size_bytes = $4
          AND artifact.verified_media_type = $5 AND artifact.classification = $6
        FOR SHARE OF artifact, blob
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(artifact.artifact_id().to_string())
    .bind(artifact.content_digest().to_string())
    .bind(i64::try_from(artifact.byte_length()).map_err(|_| {
        RepositoryError::InvalidInput("artifact byte length exceeds PostgreSQL bigint".to_owned())
    })?)
    .bind(artifact.media_type())
    .bind(artifact.classification().as_str())
    .fetch_optional(&mut **transaction)
    .await?;
    if matched.is_none() {
        return Err(RepositoryError::NotFound(not_found_label));
    }
    Ok(())
}

fn write_permission(kind: RegistryResourceKind) -> Permission {
    match kind {
        RegistryResourceKind::Agent => Permission::AgentWrite,
        RegistryResourceKind::Skill => Permission::SkillWrite,
        RegistryResourceKind::CapabilityInterface
        | RegistryResourceKind::CapabilityImplementation => Permission::CapabilityWrite,
        RegistryResourceKind::ContextSourceInterface
        | RegistryResourceKind::ContextSourceImplementation
        | RegistryResourceKind::ContextDataset => Permission::ContextWrite,
        RegistryResourceKind::McpServer => Permission::McpWrite,
        RegistryResourceKind::ModelProvider | RegistryResourceKind::ModelProfile => {
            Permission::ModelWrite
        }
        RegistryResourceKind::Policy => Permission::PolicyWrite,
        RegistryResourceKind::SandboxRuntime
        | RegistryResourceKind::SandboxPackage
        | RegistryResourceKind::SandboxProfile => Permission::SandboxWrite,
    }
}

fn registry_validation_write_permission_map() -> Value {
    Value::Object(
        RegistryResourceKind::ALL
            .iter()
            .map(|kind| {
                (
                    kind.as_str().to_owned(),
                    Value::String(write_permission(*kind).to_string()),
                )
            })
            .collect(),
    )
}

fn read_permission(kind: RegistryResourceKind) -> Permission {
    match kind {
        RegistryResourceKind::Agent => Permission::AgentRead,
        RegistryResourceKind::Skill => Permission::SkillRead,
        RegistryResourceKind::CapabilityInterface
        | RegistryResourceKind::CapabilityImplementation => Permission::CapabilityRead,
        RegistryResourceKind::ContextSourceInterface
        | RegistryResourceKind::ContextSourceImplementation
        | RegistryResourceKind::ContextDataset => Permission::ContextRead,
        RegistryResourceKind::McpServer => Permission::McpRead,
        RegistryResourceKind::ModelProvider | RegistryResourceKind::ModelProfile => {
            Permission::ModelRead
        }
        RegistryResourceKind::Policy => Permission::PolicyRead,
        RegistryResourceKind::SandboxRuntime
        | RegistryResourceKind::SandboxPackage
        | RegistryResourceKind::SandboxProfile => Permission::SandboxRead,
    }
}

fn publish_permission(kind: RegistryResourceKind) -> Permission {
    match kind {
        RegistryResourceKind::Agent => Permission::AgentPublish,
        RegistryResourceKind::Skill => Permission::SkillPublish,
        RegistryResourceKind::CapabilityInterface
        | RegistryResourceKind::CapabilityImplementation => Permission::CapabilityPublish,
        RegistryResourceKind::ContextSourceInterface
        | RegistryResourceKind::ContextSourceImplementation
        | RegistryResourceKind::ContextDataset => Permission::ContextPublish,
        RegistryResourceKind::McpServer => Permission::McpPublish,
        RegistryResourceKind::ModelProvider | RegistryResourceKind::ModelProfile => {
            Permission::ModelPublish
        }
        RegistryResourceKind::Policy => Permission::PolicyPublish,
        RegistryResourceKind::SandboxRuntime
        | RegistryResourceKind::SandboxPackage
        | RegistryResourceKind::SandboxProfile => Permission::SandboxPublish,
    }
}

fn deploy_permission(kind: RegistryResourceKind) -> Permission {
    match kind {
        RegistryResourceKind::Agent => Permission::AgentDeploy,
        RegistryResourceKind::CapabilityImplementation => Permission::CapabilityDeploy,
        RegistryResourceKind::ContextSourceImplementation => Permission::ContextDeploy,
        RegistryResourceKind::McpServer => Permission::McpDeploy,
        RegistryResourceKind::ModelProvider | RegistryResourceKind::ModelProfile => {
            Permission::ModelDeploy
        }
        _ => write_permission(kind),
    }
}

fn activate_permission(kind: RegistryResourceKind) -> Permission {
    match kind {
        RegistryResourceKind::Agent => Permission::AgentActivate,
        RegistryResourceKind::Skill => Permission::SkillActivate,
        RegistryResourceKind::CapabilityInterface
        | RegistryResourceKind::CapabilityImplementation => Permission::CapabilityActivate,
        RegistryResourceKind::ContextSourceInterface
        | RegistryResourceKind::ContextSourceImplementation
        | RegistryResourceKind::ContextDataset => Permission::ContextActivate,
        RegistryResourceKind::McpServer => Permission::McpActivate,
        RegistryResourceKind::ModelProvider | RegistryResourceKind::ModelProfile => {
            Permission::ModelActivate
        }
        RegistryResourceKind::Policy => Permission::PolicyActivate,
        RegistryResourceKind::SandboxRuntime
        | RegistryResourceKind::SandboxPackage
        | RegistryResourceKind::SandboxProfile => Permission::SandboxActivate,
    }
}

fn decode_resource_draft(payload: &TypedPayload) -> Result<ResourceDraftPayload, RepositoryError> {
    let mut value = payload.value.clone();
    let object = value.as_object_mut().ok_or_else(|| {
        RepositoryError::CorruptRow("resource payload is not an object".to_owned())
    })?;
    object.remove("schema_version");
    let draft: ResourceDraftPayload = serde_json::from_value(value)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    draft
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(draft)
}

fn tenant_principal_scope_kind(kind: PrincipalKind) -> String {
    format!("tenant_principal_{}", kind.as_str())
}

async fn load_tenant_principal(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    principal_kind: PrincipalKind,
) -> Result<TenantPrincipalRecord, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT tenant_id, principal_id, principal_kind, state, generation, version,
               permissions_schema_version, permissions, permissions_digest,
               created_at, updated_at
        FROM insight_platform.tenant_principals
        WHERE tenant_id = $1 AND principal_id = $2 AND principal_kind = $3
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(principal_id.to_string())
    .bind(principal_kind.as_str())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("tenant principal"))?;
    tenant_principal_from_row(row)
}

async fn classify_tenant_principal_cas(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    principal_kind: PrincipalKind,
) -> Result<RepositoryError, RepositoryError> {
    let exists: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM insight_platform.tenant_principals
            WHERE tenant_id = $1 AND principal_id = $2 AND principal_kind = $3
        )
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(principal_id.to_string())
    .bind(principal_kind.as_str())
    .fetch_one(&mut **transaction)
    .await?;
    Ok(if exists {
        RepositoryError::Conflict("tenant principal")
    } else {
        RepositoryError::NotFound("tenant principal")
    })
}

pub(crate) async fn load_secret_binding_metadata(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    secret_binding_id: &ResourceId,
) -> Result<SecretBindingMetadataRecord, RepositoryError> {
    load_secret_binding_metadata_with_lock(transaction, tenant_id, secret_binding_id, false).await
}

async fn load_secret_binding_metadata_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    secret_binding_id: &ResourceId,
) -> Result<SecretBindingMetadataRecord, RepositoryError> {
    load_secret_binding_metadata_with_lock(transaction, tenant_id, secret_binding_id, true).await
}

pub(crate) async fn load_secret_binding_resolution_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    secret_binding_id: &ResourceId,
) -> Result<SecretBindingResolutionRecord, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT tenant_id, secret_binding_id, purpose, provider, state, generation,
               opaque_reference_ciphertext, key_id, reference_digest,
               payload_schema_version, payload, payload_digest
        FROM insight_platform.secret_bindings
        WHERE tenant_id = $1 AND secret_binding_id = $2
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(secret_binding_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("secret binding"))?;
    secret_binding_resolution_from_row(row)
}

fn validate_registered_prepared_binding(
    current: &SecretBindingResolutionRecord,
    command: &RegisterPreparedSecretBinding,
) -> Result<(), RepositoryError> {
    let expected_policy = insight_platform_contracts::SecretResolutionPolicy::Pinned {
        opaque_version_identity_digest: command.opaque_version_identity_digest.clone(),
    };
    if current.tenant_id != command.audit.tenant_id
        || current.secret_binding_id != command.secret_binding_id
        || current.purpose != command.purpose
        || current.provider_id != command.provider_id
        || current.state != insight_platform_contracts::SecretBindingState::Active
        || current.generation != 1
        || current.reference_digest != command.reference_digest
        || current.payload.provider_id != command.provider_id
        || current.payload.resolution_policy != expected_policy
    {
        return Err(RepositoryError::Conflict("prepared secret binding"));
    }
    Ok(())
}

async fn load_secret_binding_metadata_with_lock(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    secret_binding_id: &ResourceId,
    for_update: bool,
) -> Result<SecretBindingMetadataRecord, RepositoryError> {
    let query = if for_update {
        r#"
        SELECT tenant_id, secret_binding_id, purpose, provider, state, generation, version,
               payload_schema_version, payload, payload_digest, created_at, updated_at, revoked_at
        FROM insight_platform.secret_bindings
        WHERE tenant_id = $1 AND secret_binding_id = $2
        FOR UPDATE
        "#
    } else {
        r#"
        SELECT tenant_id, secret_binding_id, purpose, provider, state, generation, version,
               payload_schema_version, payload, payload_digest, created_at, updated_at, revoked_at
        FROM insight_platform.secret_bindings
        WHERE tenant_id = $1 AND secret_binding_id = $2
        "#
    };
    let row = sqlx::query(query)
        .bind(tenant_id.to_string())
        .bind(secret_binding_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::NotFound("secret binding"))?;
    secret_binding_metadata_from_row(row)
}

async fn classify_secret_binding_cas(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    secret_binding_id: &ResourceId,
) -> Result<RepositoryError, RepositoryError> {
    let exists: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM insight_platform.secret_bindings
            WHERE tenant_id = $1 AND secret_binding_id = $2
        )
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(secret_binding_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    Ok(if exists {
        RepositoryError::Conflict("secret binding")
    } else {
        RepositoryError::NotFound("secret binding")
    })
}

async fn append_secret_binding_event(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &CommandAudit,
    record: &SecretBindingMetadataRecord,
    event_type: &str,
    provider_evidence_digest: Option<&Sha256Digest>,
) -> Result<(), RepositoryError> {
    append_command_event(
        transaction,
        audit,
        "secret_binding",
        &record.secret_binding_id,
        record.version,
        event_type,
        &TypedPayload::new(
            1,
            &serde_json::json!({
                "generation": record.generation,
                "provider_evidence_digest": provider_evidence_digest,
                "provider_id": record.provider_id,
                "purpose": record.purpose,
                "resolution_policy_digest": record.payload.digest,
                "state": record.state,
            }),
        )?,
    )
    .await
}

async fn validate_run_bindings_exist(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    snapshot: &RunBindingsSnapshot,
) -> Result<(), RepositoryError> {
    snapshot
        .validate()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    for slot in &snapshot.slots {
        let FrozenSlotTarget::Context { binding } = &slot.target else {
            continue;
        };
        if binding.owner_agent_deployment_id != snapshot.agent.deployment_id {
            return Err(RepositoryError::InvalidInput(
                "Run Context binding owner differs from Agent Deployment".to_owned(),
            ));
        }
        if let insight_platform_contracts::ContextConsistencyPolicy::PinAtRunAdmission {
            dataset_id,
        } = &binding.consistency
        {
            let view = snapshot
                .context_dataset_views
                .iter()
                .find(|view| view.context_binding_id == binding.context_binding_id)
                .ok_or_else(|| {
                    RepositoryError::InvalidInput(
                        "Run Context binding lacks its admission-time Dataset view".to_owned(),
                    )
                })?;
            let active_matches: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM insight_platform.resources AS resource
                    JOIN insight_platform.resource_versions AS version
                      ON version.tenant_id = resource.tenant_id
                     AND version.resource_id = resource.resource_id
                     AND version.resource_version_id = resource.active_version_id
                    WHERE resource.tenant_id = $1
                      AND resource.resource_id = $2
                      AND resource.resource_kind = 'context_dataset'
                      AND resource.lifecycle_state = 'active'
                      AND resource.gate_state = 'enabled'
                      AND version.resource_version_id = $3
                      AND version.resource_version_kind = 'dataset_generation'
                      AND version.content_digest = $4
                )
                "#,
            )
            .bind(tenant_id.to_string())
            .bind(dataset_id.to_string())
            .bind(view.generation.generation_id.to_string())
            .bind(view.generation.generation_digest.to_string())
            .fetch_one(&mut **transaction)
            .await?;
            if !active_matches {
                return Err(RepositoryError::Conflict(
                    "Context Dataset active head at Run admission",
                ));
            }
        }
    }
    validate_exact_version_refs_exist(transaction, tenant_id, &snapshot.exact_version_refs())
        .await?;
    validate_exact_dataset_generation_refs_exist(
        transaction,
        tenant_id,
        &snapshot.exact_dataset_generation_refs(),
    )
    .await?;

    let mut pending = snapshot
        .exact_deployment_refs()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    let mut visited = BTreeSet::new();
    while let Some(reference) = pending.pop() {
        if !visited.insert(reference.deployment_id.to_string()) {
            continue;
        }
        if visited.len() > MAX_RESOURCE_DEPENDENCIES {
            return Err(RepositoryError::InvalidInput(
                "run deployment closure exceeds its hard bound".to_owned(),
            ));
        }
        let owner_kind = deployment_owner_kind(reference.resource_kind).ok_or_else(|| {
            RepositoryError::InvalidInput(
                "run binding has an unsupported deployment kind".to_owned(),
            )
        })?;
        let row = sqlx::query(
            r#"
            SELECT deployment.tenant_id, deployment.deployment_id, deployment.resource_id,
                   deployment.resource_version_id, deployment.environment,
                   deployment.bindings_digest, deployment.payload_schema_version,
                   deployment.bindings, deployment.created_by, deployment.created_at,
                   resource.active_deployment_id
            FROM insight_platform.deployments AS deployment
            JOIN insight_platform.resources AS resource
              ON resource.tenant_id = deployment.tenant_id
             AND resource.resource_id = deployment.resource_id
            WHERE deployment.tenant_id = $1 AND deployment.deployment_id = $2
              AND deployment.bindings_digest = $3 AND resource.resource_kind = $4
              AND resource.lifecycle_state = 'active' AND resource.gate_state = 'enabled'
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(reference.deployment_id.to_string())
        .bind(reference.deployment_digest.to_string())
        .bind(owner_kind.as_str())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::NotFound("bindable deployment"))?;
        let active_deployment_id: Option<String> = row.try_get("active_deployment_id")?;
        let deployment = deployment_from_row(row)?;
        if reference == snapshot.agent
            && active_deployment_id.as_deref() != Some(reference.deployment_id.to_string().as_str())
        {
            return Err(RepositoryError::NotFound("active agent deployment"));
        }

        let closure = decode_deployment_closure(&deployment.bindings)?;
        if closure.resource_kind() != owner_kind {
            return Err(RepositoryError::CorruptRow(
                "deployment closure kind does not match its owner".to_owned(),
            ));
        }
        validate_deployment_closure_exists(transaction, tenant_id, &closure).await?;
        pending.extend(closure.exact_deployment_refs().into_iter().cloned());
    }
    Ok(())
}

pub(crate) async fn require_ready_run_artifact(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    artifact: &ArtifactRef,
) -> Result<(), RepositoryError> {
    let byte_length = i64::try_from(artifact.byte_length()).map_err(|_| {
        RepositoryError::InvalidInput("artifact byte length exceeds bigint".to_owned())
    })?;
    let matched: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM insight_platform.artifacts AS artifact
            JOIN insight_platform.artifact_blobs AS blob
              ON blob.tenant_id = artifact.tenant_id AND blob.blob_id = artifact.blob_id
            WHERE artifact.tenant_id = $1 AND artifact.artifact_id = $2
              AND artifact.state = 'ready' AND artifact.terminal_at IS NULL
              AND blob.state = 'verified' AND blob.deleted_at IS NULL
              AND blob.content_digest = $3 AND blob.size_bytes = $4
              AND artifact.verified_media_type = $5 AND artifact.classification = $6
        )
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(artifact.artifact_id().to_string())
    .bind(artifact.content_digest().to_string())
    .bind(byte_length)
    .bind(artifact.media_type())
    .bind(artifact.classification().as_str())
    .fetch_one(&mut **transaction)
    .await?;
    if !matched {
        return Err(RepositoryError::NotFound("ready run input artifact"));
    }
    Ok(())
}

async fn load_canonical_model_request(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    run_id: &ResourceId,
    node_id: &ResourceId,
    value_id: &ResourceId,
) -> Result<insight_platform_models::CanonicalModelRequest, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT inline_value, content_digest
        FROM insight_platform.run_values
        WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3 AND value_id = $4
          AND value_kind = 'model_request' AND artifact_id IS NULL
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(run_id.to_string())
    .bind(node_id.to_string())
    .bind(value_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict(
        "canonical Model request RunValue",
    ))?;
    let value: Value = row.try_get("inline_value")?;
    if row.try_get::<String, _>("content_digest")?
        != canonical_digest(&value)
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?
    {
        return Err(RepositoryError::Conflict("canonical Model request digest"));
    }
    serde_json::from_value(value).map_err(|failure| {
        RepositoryError::CorruptRow(format!("canonical Model request: {failure}"))
    })
}

fn canonical_request_contains_exact_tool_results(
    request: &insight_platform_models::CanonicalModelRequest,
    expected: &[insight_platform_orchestrator::ModelToolResultReference],
    prior_message_count: usize,
) -> bool {
    let observed = request
        .messages
        .iter()
        .skip(prior_message_count)
        .flat_map(|message| {
            message.parts.iter().filter_map(move |part| match part {
                insight_platform_models::CanonicalMessagePart::ToolResult(result)
                    if message.role == insight_platform_models::CanonicalMessageRole::Tool =>
                {
                    Some(result)
                }
                _ => None,
            })
        })
        .collect::<Vec<_>>();
    observed.len() == expected.len()
        && observed.iter().zip(expected).all(|(result, exact)| {
            result.call_id == exact.call_id
                && result.invocation_id == exact.invocation_id
                && result.output_value_id == exact.output_value_id
                && result.output_schema_digest == exact.schema_digest
                && result.content_digest == exact.content_digest
                && result.classification == exact.classification
        })
}

fn deployment_owner_kind(kind: ResourceKind) -> Option<RegistryResourceKind> {
    RegistryResourceKind::ALL
        .iter()
        .copied()
        .find(|candidate| candidate.deployment_kind() == Some(kind))
}

pub(crate) fn decode_deployment_closure(
    payload: &TypedPayload,
) -> Result<DeploymentClosure, RepositoryError> {
    let mut value = payload.value.clone();
    let object = value.as_object_mut().ok_or_else(|| {
        RepositoryError::CorruptRow("deployment bindings are not an object".to_owned())
    })?;
    object.remove("schema_version");
    let closure: DeploymentClosure = serde_json::from_value(value)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    closure
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(closure)
}

pub(crate) async fn load_exact_active_policy_deployment(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    exact: &ExactDeploymentRef,
    expected_kind: PolicyKind,
) -> Result<
    (
        ExactVersionRef,
        insight_platform_contracts::PolicyResourceSpec,
    ),
    RepositoryError,
> {
    exact
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    if exact.resource_kind != ResourceKind::PolicyDeployment {
        return Err(RepositoryError::CorruptRow(
            "tenant Policy binding has the wrong Deployment kind".to_owned(),
        ));
    }
    let row = sqlx::query(
        r#"
        SELECT deployment.resource_version_id,
               deployment.payload_schema_version AS bindings_schema_version,
               deployment.bindings, deployment.bindings_digest,
               version.payload_schema_version AS version_schema_version,
               version.payload AS version_payload, version.payload_digest AS version_payload_digest
        FROM insight_platform.deployments AS deployment
        JOIN insight_platform.resources AS resource
          ON resource.tenant_id = deployment.tenant_id
         AND resource.resource_id = deployment.resource_id
        JOIN insight_platform.resource_versions AS version
          ON version.tenant_id = deployment.tenant_id
         AND version.resource_version_id = deployment.resource_version_id
         AND version.resource_id = deployment.resource_id
        WHERE deployment.tenant_id = $1 AND deployment.deployment_id = $2
          AND deployment.bindings_digest = $3
          AND resource.resource_kind = 'policy'
          AND resource.lifecycle_state = 'active' AND resource.gate_state = 'enabled'
          AND resource.active_deployment_id = deployment.deployment_id
          AND version.resource_version_kind = 'policy_revision'
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(exact.deployment_id.to_string())
    .bind(exact.deployment_digest.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("active Policy Deployment"))?;
    let bindings = payload_from_row(
        &row,
        "bindings_schema_version",
        "bindings",
        "bindings_digest",
    )?;
    let DeploymentClosure::Policy(closure) = decode_deployment_closure(&bindings)? else {
        return Err(RepositoryError::CorruptRow(
            "Policy Deployment contains the wrong closure".to_owned(),
        ));
    };
    let resource_version_id = row
        .try_get::<String, _>("resource_version_id")?
        .parse::<ResourceId>()
        .map_err(|_| RepositoryError::CorruptRow("Policy Revision ID is invalid".to_owned()))?;
    if closure.policy_revision.revision_id != resource_version_id {
        return Err(RepositoryError::CorruptRow(
            "Policy Deployment closure differs from its owner Revision".to_owned(),
        ));
    }
    let version_payload = payload_from_row(
        &row,
        "version_schema_version",
        "version_payload",
        "version_payload_digest",
    )?;
    let published = decode_published_version_payload(&version_payload)?;
    published
        .validate_for(
            RegistryResourceKind::Policy,
            &closure.policy_revision.revision_id,
        )
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let ResourceDocument::Policy(policy) = published.document else {
        return Err(RepositoryError::CorruptRow(
            "Policy Revision contains the wrong document".to_owned(),
        ));
    };
    if policy.policy_kind != expected_kind {
        return Err(RepositoryError::InvalidInput(
            "Policy Deployment resolves to the wrong PolicyKind".to_owned(),
        ));
    }
    Ok((closure.policy_revision, *policy))
}

async fn lock_active_policy_deployment_for_binding(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    exact: &ExactDeploymentRef,
) -> Result<(), RepositoryError> {
    let locked = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT true
        FROM insight_platform.deployments AS deployment
        JOIN insight_platform.resources AS resource
          ON resource.tenant_id = deployment.tenant_id
         AND resource.resource_id = deployment.resource_id
        WHERE deployment.tenant_id = $1 AND deployment.deployment_id = $2
          AND deployment.bindings_digest = $3
          AND resource.resource_kind = 'policy'
          AND resource.lifecycle_state = 'active' AND resource.gate_state = 'enabled'
          AND resource.active_deployment_id = deployment.deployment_id
        FOR SHARE OF deployment, resource
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(exact.deployment_id.to_string())
    .bind(exact.deployment_digest.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    if !locked {
        return Err(RepositoryError::NotFound("active Policy Deployment"));
    }
    Ok(())
}

pub(crate) async fn load_run(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    run_id: &ResourceId,
) -> Result<RunRecord, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT *
        FROM insight_platform.runs
        WHERE tenant_id = $1 AND run_id = $2
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(run_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("run"))?;
    persisted_run_from_row(row)
}

pub(crate) async fn load_run_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    run_id: &ResourceId,
) -> Result<RunRecord, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT *
        FROM insight_platform.runs
        WHERE tenant_id = $1 AND run_id = $2
        FOR UPDATE
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(run_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("run"))?;
    persisted_run_from_row(row)
}

pub(crate) async fn load_resource(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    resource_id: &ResourceId,
) -> Result<ResourceRecord, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
               draft_generation, active_version_id, active_deployment_id, version,
               payload_schema_version, payload, payload_digest, created_at, updated_at
        FROM insight_platform.resources
        WHERE tenant_id = $1 AND resource_id = $2
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(resource_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("resource"))?;
    resource_from_row(row)
}

pub(crate) async fn load_resource_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    resource_id: &ResourceId,
) -> Result<ResourceRecord, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT tenant_id, resource_id, resource_kind, lifecycle_state, gate_state,
               draft_generation, active_version_id, active_deployment_id, version,
               payload_schema_version, payload, payload_digest, created_at, updated_at
        FROM insight_platform.resources
        WHERE tenant_id = $1 AND resource_id = $2
        FOR UPDATE
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(resource_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("resource"))?;
    resource_from_row(row)
}

pub(crate) async fn load_deployment(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    deployment_id: &ResourceId,
) -> Result<DeploymentRecord, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT tenant_id, deployment_id, resource_id, resource_version_id, environment,
               bindings_digest, payload_schema_version, bindings, created_by, created_at
        FROM insight_platform.deployments
        WHERE tenant_id = $1 AND deployment_id = $2
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(deployment_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("deployment"))?;
    deployment_from_row(row)
}

async fn load_job(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
) -> Result<JobRecord, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT * FROM insight_platform.jobs
        WHERE tenant_id = $1 AND job_id = $2
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(job_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("job"))?;
    persisted_job_from_row(row)
}

pub(crate) async fn load_job_for_update_by_text(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    job_id: &str,
) -> Result<JobRecord, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT * FROM insight_platform.jobs
        WHERE tenant_id = $1 AND job_id = $2
        FOR UPDATE
        "#,
    )
    .bind(tenant_id)
    .bind(job_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("job"))?;
    persisted_job_from_row(row)
}

pub(crate) async fn load_task_by_text(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    task_id: &str,
) -> Result<TaskRecord, RepositoryError> {
    let row =
        sqlx::query("SELECT * FROM insight_platform.tasks WHERE tenant_id = $1 AND task_id = $2")
            .bind(tenant_id)
            .bind(task_id)
            .fetch_optional(&mut **transaction)
            .await?
            .ok_or(RepositoryError::NotFound("task"))?;
    persisted_task_from_row(row)
}

pub(crate) async fn load_task_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    task_id: &ResourceId,
) -> Result<TaskRecord, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT * FROM insight_platform.tasks
        WHERE tenant_id = $1 AND task_id = $2
        FOR UPDATE
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(task_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("task"))?;
    persisted_task_from_row(row)
}

async fn load_child_run_link_by_logical_key(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    parent_run_id: &str,
    logical_key: &str,
) -> Result<Option<ChildRunLinkRecord>, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT * FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND logical_key = $3
          AND record_kind = 'child_run_link'
        "#,
    )
    .bind(tenant_id)
    .bind(parent_run_id)
    .bind(logical_key)
    .fetch_optional(&mut **transaction)
    .await?;
    row.map(persisted_child_run_link_from_row).transpose()
}

async fn load_child_run_link_by_id(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    child_link_id: &str,
) -> Result<ChildRunLinkRecord, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT * FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id = $2 AND record_kind = 'child_run_link'
        "#,
    )
    .bind(tenant_id)
    .bind(child_link_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("ChildRunLink"))?;
    persisted_child_run_link_from_row(row)
}

async fn load_child_run_link_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    child_link_id: &str,
) -> Result<ChildRunLinkRecord, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT * FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id = $2 AND record_kind = 'child_run_link'
        FOR UPDATE
        "#,
    )
    .bind(tenant_id)
    .bind(child_link_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("ChildRunLink"))?;
    persisted_child_run_link_from_row(row)
}

async fn lock_waiting_child_parent_node(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    parent_run_id: &str,
    parent_node_id: &str,
) -> Result<(i64, String), RepositoryError> {
    let scope_id: String = sqlx::query_scalar(
        r#"
        SELECT scope_id FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND node_id = $3
        "#,
    )
    .bind(tenant_id)
    .bind(parent_run_id)
    .bind(parent_node_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("parent NodeExecution"))?;
    let rows = sqlx::query(
        r#"
        SELECT node_id, record_kind, state, version
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND run_id = $2 AND node_id IN ($3, $4)
        ORDER BY node_id
        FOR UPDATE
        "#,
    )
    .bind(tenant_id)
    .bind(parent_run_id)
    .bind(parent_node_id)
    .bind(&scope_id)
    .fetch_all(&mut **transaction)
    .await?;
    if rows.len() != 2 {
        return Err(RepositoryError::Conflict("terminal child parent closure"));
    }
    let mut node_version = None;
    let mut scope_valid = false;
    for row in rows {
        let node_id: String = row.try_get("node_id")?;
        let record_kind: String = row.try_get("record_kind")?;
        let state: String = row.try_get("state")?;
        if node_id == parent_node_id {
            if record_kind != "node_execution" || state != "waiting" {
                return Err(RepositoryError::Conflict("terminal child parent Node"));
            }
            node_version = Some(row.try_get("version")?);
        } else if node_id == scope_id {
            scope_valid = record_kind == "scope_instance" && state == "open";
        }
    }
    if !scope_valid {
        return Err(RepositoryError::Conflict("terminal child parent Scope"));
    }
    Ok((
        node_version.ok_or(RepositoryError::Conflict("terminal child parent Node"))?,
        scope_id,
    ))
}

async fn require_same_child_run_request(
    transaction: &mut Transaction<'_, Postgres>,
    existing: &ChildRunLinkRecord,
    parent_job: &JobRecord,
    parent_run: &RunRecord,
    command: &DeferOrchestrationToChildRun,
    plan_digest: &Sha256Digest,
) -> Result<(), RepositoryError> {
    require_exact_runtime_plan(transaction, parent_run, &command.plan, plan_digest).await?;
    let node_key: String = sqlx::query_scalar(
        "SELECT plan_node_key FROM insight_platform.run_nodes WHERE tenant_id=$1 AND run_id=$2 AND node_id=$3 AND record_kind='node_execution' AND node_kind='child_agent_call'",
    )
    .bind(&parent_run.tenant_id)
    .bind(&parent_run.run_id)
    .bind(&existing.parent_node_execution_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("orchestration child replay Plan node"))?;
    require_child_run_plan_contract(command.plan.node(&PlanNodeKey::new(node_key)?)?, command)?;
    let tenant_id: ResourceId = parent_run.tenant_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let child_run_id: ResourceId = existing.child_run_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let child_run = load_run(transaction, &tenant_id, &child_run_id).await?;
    // Logical replay proves the budget against the original admission instant. A new command
    // or a later clock cannot move the already committed deadline.
    let budget = insight_platform_orchestrator::derive_child_budget(
        &command.budget,
        child_run.created_at,
        parent_run.deadline.min(parent_job.deadline),
    )?;
    let parent_attempt_ordinal = u16::try_from(parent_job.attempt_no)
        .map_err(|_| RepositoryError::CorruptRow("orchestration attempt exceeds u16".to_owned()))?;
    let expected = ChildRunLinkPayload {
        parent_attempt_ordinal,
        child_agent_deployment: command.selected_child_deployment.clone(),
        input_digest: command.input.content_digest.clone(),
        cancellation_policy: command.cancellation_policy,
        budget,
    };
    if existing.parent_node_execution_id != parent_job.node_id.as_deref().unwrap_or_default()
        || existing.slot_id != command.slot_id
        || existing.source_value_ids != command.source_value_ids
        || existing.payload != expected
        || child_run.deadline != existing.deadline
    {
        return Err(RepositoryError::Conflict("orchestration child logical key"));
    }
    Ok(())
}

fn domain_job_fence(fence: &JobFence) -> Result<DomainJobFence, RepositoryError> {
    Ok(DomainJobFence {
        expected_version: u64::try_from(fence.expected_job_version)
            .map_err(|_| RepositoryError::InvalidInput("negative Job fence version".to_owned()))?,
        worker_process_generation_id: fence.worker_id.clone(),
        lease_generation: u64::try_from(fence.lease_epoch).map_err(|_| {
            RepositoryError::InvalidInput("negative Job fence generation".to_owned())
        })?,
        token_digest: fence.lease_token_digest.clone(),
    })
}

fn validate_publish_batch(
    kind: RegistryResourceKind,
    versions: &[NewPublishedVersion],
) -> Result<(), RepositoryError> {
    let mut kinds = BTreeSet::new();
    let mut ordinals = BTreeSet::new();
    for version in versions {
        if !kind.allows_version_kind(version.resource_version_id.kind())
            || !kinds.insert(version.resource_version_id.kind())
        {
            return Err(RepositoryError::InvalidInput(
                "publish batch contains a duplicate or incompatible version kind".to_owned(),
            ));
        }
        ordinals.insert(version.revision_no);
    }
    let valid = if kind == RegistryResourceKind::Agent {
        versions.len() == 2
            && kinds.contains(&ResourceKind::AgentInterfaceRevision)
            && kinds.contains(&ResourceKind::AgentPlanRevision)
            && ordinals.len() == 1
    } else {
        versions.len() == 1
    };
    if !valid {
        return Err(RepositoryError::InvalidInput(
            "publish batch does not match the resource version matrix".to_owned(),
        ));
    }
    Ok(())
}

fn require_version_content_contract(version: &NewPublishedVersion) -> Result<(), RepositoryError> {
    let ResourceDocument::Agent(agent) = &version.payload.document else {
        return Ok(());
    };
    if version.resource_version_id.kind() == ResourceKind::AgentPlanRevision
        && (version.content_digest != agent.typed_plan_digest
            || version.artifact_id.as_ref() != Some(&agent.typed_plan_artifact_id))
    {
        return Err(RepositoryError::InvalidInput(
            "Agent Plan revision must bind the exact typed Plan artifact and digest".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) async fn validate_deployment_closure_exists(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    closure: &DeploymentClosure,
) -> Result<(), RepositoryError> {
    let version_refs = closure.exact_version_refs();
    validate_exact_version_refs_exist(transaction, tenant_id, &version_refs).await?;
    validate_exact_dataset_generation_refs_exist(
        transaction,
        tenant_id,
        &closure.exact_dataset_generation_refs(),
    )
    .await?;
    match closure {
        DeploymentClosure::CapabilityInterface(capability) => {
            let implementation =
                crate::invocation_repository::load_enabled_exact_published_version(
                    transaction,
                    tenant_id,
                    &capability.implementation,
                    RegistryResourceKind::CapabilityImplementation,
                )
                .await?;
            let ResourceDocument::CapabilityImplementation(implementation) =
                implementation.document
            else {
                return Err(RepositoryError::CorruptRow(
                    "Capability Implementation revision contains the wrong document".to_owned(),
                ));
            };
            if implementation.interface_revision != capability.interface
                || implementation.backend_kind != capability.backend.kind()
                || capability
                    .backend
                    .validate_for(&implementation.backend_contract)
                    .is_err()
            {
                return Err(RepositoryError::Conflict(
                    "Capability Deployment implementation closure",
                ));
            }
            validate_secret_binding_purposes(
                &capability.secret_bindings,
                &implementation.credential_requirements,
            )?;
            require_ready_run_artifact(transaction, tenant_id, &capability.conformance_evidence)
                .await?;
        }
        DeploymentClosure::ContextSourceInterface(context) => {
            let implementation =
                crate::invocation_repository::load_enabled_exact_published_version(
                    transaction,
                    tenant_id,
                    &context.implementation,
                    RegistryResourceKind::ContextSourceImplementation,
                )
                .await?;
            let ResourceDocument::ContextSourceImplementation(implementation) =
                implementation.document
            else {
                return Err(RepositoryError::CorruptRow(
                    "Context Implementation revision contains the wrong document".to_owned(),
                ));
            };
            if implementation.interface_revision != context.interface
                || implementation.backend_kind != context.backend.kind()
                || implementation
                    .contract
                    .validate_binding(&context.backend)
                    .is_err()
            {
                return Err(RepositoryError::Conflict(
                    "Context Deployment implementation closure",
                ));
            }
            validate_secret_binding_purposes(
                &context.secret_bindings,
                &implementation.contract.credential_requirements,
            )?;
            for (reference, role) in [
                (&context.parser_policy, PolicyReferenceRole::Parser),
                (&context.chunker_policy, PolicyReferenceRole::Chunker),
                (&context.ranking_policy, PolicyReferenceRole::Ranking),
                (&context.data_policy, PolicyReferenceRole::Data),
            ]
            .into_iter()
            .chain(
                context
                    .network_policy
                    .iter()
                    .map(|reference| (reference, PolicyReferenceRole::Network)),
            )
            .chain(
                context
                    .tls_policy
                    .iter()
                    .map(|reference| (reference, PolicyReferenceRole::Tls)),
            )
            .chain(
                context
                    .trust_policy
                    .iter()
                    .map(|reference| (reference, PolicyReferenceRole::Trust)),
            ) {
                require_exact_policy_kind(transaction, tenant_id, reference, role).await?;
            }
            require_ready_run_artifact(transaction, tenant_id, &context.conformance_evidence)
                .await?;
        }
        DeploymentClosure::McpServer(mcp) => {
            let server = crate::invocation_repository::load_enabled_exact_published_version(
                transaction,
                tenant_id,
                &mcp.server_revision,
                RegistryResourceKind::McpServer,
            )
            .await?;
            let ResourceDocument::McpServer(server) = server.document else {
                return Err(RepositoryError::CorruptRow(
                    "MCP Server revision contains the wrong document".to_owned(),
                ));
            };
            if server.transport != mcp.transport.kind()
                || server.protocol_policy != mcp.protocol_policy
                || server.authorization_credential_purpose.is_some() != mcp.auth_policy.is_some()
            {
                return Err(RepositoryError::Conflict("MCP Deployment Server closure"));
            }
            let protocol = crate::invocation_repository::load_enabled_exact_published_version(
                transaction,
                tenant_id,
                &mcp.protocol_policy,
                RegistryResourceKind::Policy,
            )
            .await?;
            let ResourceDocument::Policy(protocol) = protocol.document else {
                return Err(RepositoryError::CorruptRow(
                    "MCP Protocol Policy revision contains the wrong document".to_owned(),
                ));
            };
            if protocol.policy_kind != PolicyKind::Protocol || protocol.mcp_protocol.is_none() {
                return Err(RepositoryError::Conflict(
                    "MCP Deployment Protocol Policy closure",
                ));
            }
            if let Some(auth_policy) = &mcp.auth_policy {
                let auth = crate::invocation_repository::load_enabled_exact_published_version(
                    transaction,
                    tenant_id,
                    auth_policy,
                    RegistryResourceKind::Policy,
                )
                .await?;
                let ResourceDocument::Policy(auth) = auth.document else {
                    return Err(RepositoryError::CorruptRow(
                        "MCP Auth Profile revision contains the wrong document".to_owned(),
                    ));
                };
                let profile = auth.mcp_auth.as_ref().ok_or(RepositoryError::Conflict(
                    "MCP Deployment Auth Profile document",
                ))?;
                if auth.policy_kind != PolicyKind::McpAuth
                    || profile.resource_indicator.endpoint_identity_digest
                        != mcp.server_identity_digest
                    || profile
                        .client_credential_purpose
                        .as_ref()
                        .is_some_and(|purpose| {
                            !server.deployment_credential_requirements.contains(purpose)
                        })
                    || server
                        .authorization_credential_purpose
                        .as_ref()
                        .is_some_and(|purpose| {
                            profile.client_credential_purpose.as_ref() == Some(purpose)
                        })
                {
                    return Err(RepositoryError::Conflict(
                        "MCP Deployment Auth Profile closure",
                    ));
                }
            }
            validate_secret_binding_purposes(
                &mcp.secret_bindings,
                &server.deployment_credential_requirements,
            )?;
            require_ready_run_artifact(transaction, tenant_id, &mcp.conformance_evidence).await?;
        }
        DeploymentClosure::ModelProvider(model_provider) => {
            let provider = crate::invocation_repository::load_enabled_exact_published_version(
                transaction,
                tenant_id,
                &model_provider.provider_revision,
                RegistryResourceKind::ModelProvider,
            )
            .await?;
            let ResourceDocument::ModelProvider(provider) = provider.document else {
                return Err(RepositoryError::CorruptRow(
                    "Model Provider revision contains the wrong document".to_owned(),
                ));
            };
            if provider.protocol_policy != model_provider.protocol_policy {
                return Err(RepositoryError::Conflict(
                    "Model Provider Deployment protocol closure",
                ));
            }
            validate_secret_binding_purposes(
                &model_provider.secret_bindings,
                &provider.credential_requirements,
            )?;
            require_ready_run_artifact(
                transaction,
                tenant_id,
                &model_provider.admission_evidence.artifact,
            )
            .await?;
            if model_provider.admission_evidence.basis
                == insight_platform_contracts::ModelEvidenceBasis::OperatorDeclaration
            {
                insight_platform_contracts::validate_model_provider_declaration(&provider)
                    .map_err(|_| RepositoryError::Conflict("Model Provider declaration"))?;
                if model_provider.admission_evidence.artifact != provider.authoring_package.artifact
                {
                    return Err(RepositoryError::Conflict(
                        "Model Provider declaration Artifact",
                    ));
                }
            }
        }
        DeploymentClosure::ModelProfile(model_profile) => {
            let profile = crate::invocation_repository::load_enabled_exact_published_version(
                transaction,
                tenant_id,
                &model_profile.profile_revision,
                RegistryResourceKind::ModelProfile,
            )
            .await?;
            let ResourceDocument::ModelProfile(profile) = profile.document else {
                return Err(RepositoryError::CorruptRow(
                    "Model Profile revision contains the wrong document".to_owned(),
                ));
            };
            let provider_deployment = load_deployment(
                transaction,
                tenant_id,
                &model_profile.provider_deployment.deployment_id,
            )
            .await?;
            if provider_deployment.bindings.digest
                != model_profile
                    .provider_deployment
                    .deployment_digest
                    .to_string()
            {
                return Err(RepositoryError::Conflict(
                    "Model Profile exact Provider Deployment",
                ));
            }
            let provider_closure = match decode_deployment_closure(&provider_deployment.bindings)? {
                DeploymentClosure::ModelProvider(closure) => closure,
                _ => {
                    return Err(RepositoryError::Conflict(
                        "Model Profile Provider Deployment closure",
                    ));
                }
            };
            if profile.provider_revision != provider_closure.provider_revision
                || model_profile.generation_defaults.schema_digest
                    != profile.parameter_schema_digest
            {
                return Err(RepositoryError::Conflict(
                    "Model Profile Deployment binding closure",
                ));
            }
        }
        DeploymentClosure::Skill(skill) => {
            require_ready_run_artifact(transaction, tenant_id, &skill.qualification_evidence)
                .await?;
        }
        DeploymentClosure::Policy(policy) => {
            require_ready_run_artifact(transaction, tenant_id, &policy.qualification_evidence)
                .await?;
        }
        DeploymentClosure::SandboxProfile(profile) => {
            require_ready_run_artifact(transaction, tenant_id, &profile.qualification_evidence)
                .await?;
        }
        DeploymentClosure::Agent(_) => {}
    }
    for reference in closure.exact_deployment_refs() {
        let matched: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM insight_platform.deployments AS deployment
                JOIN insight_platform.resources AS resource
                  ON resource.tenant_id = deployment.tenant_id
                 AND resource.resource_id = deployment.resource_id
                WHERE deployment.tenant_id = $1
                  AND deployment.deployment_id = $2
                  AND deployment.bindings_digest = $3
                  AND resource.lifecycle_state = 'active'
                  AND resource.gate_state = 'enabled'
            )
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(reference.deployment_id.to_string())
        .bind(reference.deployment_digest.to_string())
        .fetch_one(&mut **transaction)
        .await?;
        if !matched {
            return Err(RepositoryError::NotFound("deployment closure binding"));
        }
    }
    for binding in closure.exact_policy_bindings() {
        let deployment =
            load_deployment(transaction, tenant_id, &binding.deployment.deployment_id).await?;
        if deployment.bindings.digest != binding.deployment.deployment_digest.to_string() {
            return Err(RepositoryError::Conflict("exact Policy Deployment digest"));
        }
        let DeploymentClosure::Policy(policy) = decode_deployment_closure(&deployment.bindings)?
        else {
            return Err(RepositoryError::Conflict("exact Policy Deployment closure"));
        };
        if policy.policy_revision != binding.revision {
            return Err(RepositoryError::Conflict(
                "exact Policy Deployment Revision",
            ));
        }
    }
    validate_active_secret_bindings(transaction, tenant_id, closure.secret_bindings()).await?;
    Ok(())
}

async fn require_exact_policy_kind(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    reference: &ExactVersionRef,
    role: PolicyReferenceRole,
) -> Result<(), RepositoryError> {
    let published = crate::invocation_repository::load_enabled_exact_published_version(
        transaction,
        tenant_id,
        reference,
        RegistryResourceKind::Policy,
    )
    .await?;
    let ResourceDocument::Policy(policy) = published.document else {
        return Err(RepositoryError::CorruptRow(
            "Policy revision contains the wrong document".to_owned(),
        ));
    };
    if policy.policy_kind != role.expected_kind() {
        return Err(RepositoryError::Conflict("Deployment Policy role"));
    }
    Ok(())
}

fn validate_secret_binding_purposes(
    secret_bindings: &[ExactSecretBindingRef],
    required_purposes: &[SecretPurpose],
) -> Result<(), RepositoryError> {
    let mut actual_purposes = secret_bindings
        .iter()
        .map(|binding| binding.purpose.clone())
        .collect::<Vec<_>>();
    actual_purposes.sort();
    if actual_purposes != required_purposes
        || actual_purposes.windows(2).any(|pair| pair[0] == pair[1])
    {
        return Err(RepositoryError::Conflict(
            "Deployment credential requirements",
        ));
    }
    Ok(())
}

async fn validate_active_secret_bindings(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    secret_bindings: &[ExactSecretBindingRef],
) -> Result<(), RepositoryError> {
    for reference in secret_bindings {
        let record =
            load_secret_binding_metadata(transaction, tenant_id, &reference.secret_binding_id)
                .await?;
        let payload: SecretBindingPayload = decode_typed_payload(&record.payload, "SecretBinding")?;
        if record.payload.schema_version != 1
            || record.state != "active"
            || record.secret_binding_id != reference.secret_binding_id.to_string()
            || record.provider_id != reference.provider_id.to_string()
            || record.purpose != reference.purpose.as_str()
            || payload.provider_id != reference.provider_id
        {
            return Err(RepositoryError::Conflict(
                "Deployment active SecretBinding reference",
            ));
        }
    }
    Ok(())
}

pub(crate) async fn validate_exact_secret_bindings_at_creation(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    secret_bindings: &[ExactSecretBindingRef],
) -> Result<(), RepositoryError> {
    for reference in secret_bindings {
        let record =
            load_secret_binding_metadata(transaction, tenant_id, &reference.secret_binding_id)
                .await?;
        let payload: SecretBindingPayload = decode_typed_payload(&record.payload, "SecretBinding")?;
        if record.payload.schema_version != 1
            || record.state != "active"
            || record.secret_binding_id != reference.secret_binding_id.to_string()
            || u64::try_from(record.generation).ok() != Some(reference.binding_generation)
            || record.provider_id != reference.provider_id.to_string()
            || record.purpose != reference.purpose.as_str()
            || payload.provider_id != reference.provider_id
            || payload.resolution_policy != reference.resolution_policy
        {
            return Err(RepositoryError::Conflict("exact SecretBinding reference"));
        }
    }
    Ok(())
}

async fn validate_exact_version_refs_exist(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    references: &[&ExactVersionRef],
) -> Result<(), RepositoryError> {
    for reference in references {
        let matched: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM insight_platform.resource_versions AS version
                JOIN insight_platform.resources AS resource
                  ON resource.tenant_id = version.tenant_id
                 AND resource.resource_id = version.resource_id
                WHERE version.tenant_id = $1 AND version.resource_version_id = $2
                  AND version.content_digest = $3 AND version.resource_version_kind = $4
                  AND resource.lifecycle_state = 'active' AND resource.gate_state = 'enabled'
            )
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(reference.revision_id.to_string())
        .bind(reference.semantic_digest.to_string())
        .bind(reference.resource_kind.descriptor().name)
        .fetch_one(&mut **transaction)
        .await?;
        if !matched {
            return Err(RepositoryError::NotFound("exact version binding"));
        }
    }
    Ok(())
}

async fn validate_exact_dataset_generation_refs_exist(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    references: &[&ExactDatasetGenerationRef],
) -> Result<(), RepositoryError> {
    for reference in references {
        let matched: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM insight_platform.resource_versions AS version
                JOIN insight_platform.resources AS resource
                  ON resource.tenant_id = version.tenant_id
                 AND resource.resource_id = version.resource_id
                WHERE version.tenant_id = $1
                  AND version.resource_id = $2
                  AND version.resource_version_id = $3
                  AND version.content_digest = $4
                  AND version.resource_version_kind = 'dataset_generation'
                  AND resource.resource_kind = 'context_dataset'
                  AND resource.lifecycle_state = 'active'
                  AND resource.gate_state = 'enabled'
            )
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(reference.dataset_id.to_string())
        .bind(reference.generation_id.to_string())
        .bind(reference.generation_digest.to_string())
        .fetch_one(&mut **transaction)
        .await?;
        if !matched {
            return Err(RepositoryError::NotFound(
                "exact Context Dataset Generation binding",
            ));
        }
    }
    Ok(())
}

async fn resolve_activation_target(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    resource_id: &ResourceId,
    kind: RegistryResourceKind,
    target: &ActiveTarget,
) -> Result<(Option<String>, Option<String>), RepositoryError> {
    match target {
        ActiveTarget::Version { version } => {
            version
                .validate()
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
            if kind.activation_target() != insight_platform_contracts::ActivationTargetKind::Version
                || !kind.allows_version_kind(version.resource_kind)
            {
                return Err(RepositoryError::InvalidInput(
                    "resource does not activate a version target".to_owned(),
                ));
            }
            let matched: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS (
                    SELECT 1 FROM insight_platform.resource_versions
                    WHERE tenant_id = $1 AND resource_id = $2 AND resource_version_id = $3
                      AND content_digest = $4
                )
                "#,
            )
            .bind(tenant_id.to_string())
            .bind(resource_id.to_string())
            .bind(version.revision_id.to_string())
            .bind(version.semantic_digest.to_string())
            .fetch_one(&mut **transaction)
            .await?;
            if !matched {
                return Err(RepositoryError::NotFound("activation target"));
            }
            Ok((Some(version.revision_id.to_string()), None))
        }
        ActiveTarget::Deployment { deployment } => {
            deployment
                .validate()
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
            if kind.activation_target()
                != insight_platform_contracts::ActivationTargetKind::Deployment
                || kind.deployment_kind() != Some(deployment.resource_kind)
            {
                return Err(RepositoryError::InvalidInput(
                    "resource does not activate a deployment target".to_owned(),
                ));
            }
            let matched: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS (
                    SELECT 1 FROM insight_platform.deployments
                    WHERE tenant_id = $1 AND resource_id = $2 AND deployment_id = $3
                      AND bindings_digest = $4
                )
                "#,
            )
            .bind(tenant_id.to_string())
            .bind(resource_id.to_string())
            .bind(deployment.deployment_id.to_string())
            .bind(deployment.deployment_digest.to_string())
            .fetch_one(&mut **transaction)
            .await?;
            if !matched {
                return Err(RepositoryError::NotFound("activation target"));
            }
            Ok((None, Some(deployment.deployment_id.to_string())))
        }
    }
}

fn tenant_from_row(row: PgRow) -> Result<TenantRecord, RepositoryError> {
    let payload = payload_from_row(&row, "config_schema_version", "config", "config_digest")?;
    let mut value = payload.value.clone();
    let Value::Object(object) = &mut value else {
        return Err(RepositoryError::CorruptRow(
            "tenant config is not an object".to_owned(),
        ));
    };
    object.remove("schema_version");
    let config: TenantConfig = serde_json::from_value(value)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    config
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(TenantRecord {
        tenant_id: row.try_get("tenant_id")?,
        state: row.try_get("state")?,
        version: row.try_get("version")?,
        config,
        config_digest: payload.digest,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn principal_from_row(row: PgRow) -> Result<PrincipalRecord, RepositoryError> {
    Ok(PrincipalRecord {
        principal_id: row.try_get("principal_id")?,
        state: row.try_get("state")?,
        authentication_authority_digest: row.try_get("authentication_authority_digest")?,
        subject_digest: row.try_get("subject_digest")?,
        version: row.try_get("version")?,
        payload: payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn tenant_principal_from_row(row: PgRow) -> Result<TenantPrincipalRecord, RepositoryError> {
    Ok(TenantPrincipalRecord {
        tenant_id: row.try_get("tenant_id")?,
        principal_id: row.try_get("principal_id")?,
        principal_kind: row.try_get("principal_kind")?,
        state: row.try_get("state")?,
        generation: row.try_get("generation")?,
        version: row.try_get("version")?,
        permissions: payload_from_row(
            &row,
            "permissions_schema_version",
            "permissions",
            "permissions_digest",
        )?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn secret_binding_from_row(row: PgRow) -> Result<SecretBindingRecord, RepositoryError> {
    Ok(SecretBindingRecord {
        tenant_id: row.try_get("tenant_id")?,
        secret_binding_id: row.try_get("secret_binding_id")?,
        purpose: row.try_get("purpose")?,
        provider_id: row.try_get("provider")?,
        state: row.try_get("state")?,
        generation: row.try_get("generation")?,
        version: row.try_get("version")?,
        opaque_reference_ciphertext: row.try_get("opaque_reference_ciphertext")?,
        key_id: row.try_get("key_id")?,
        reference_digest: row.try_get("reference_digest")?,
        payload: payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        revoked_at: row.try_get("revoked_at")?,
    })
}

fn secret_binding_metadata_from_row(
    row: PgRow,
) -> Result<SecretBindingMetadataRecord, RepositoryError> {
    Ok(SecretBindingMetadataRecord {
        tenant_id: row.try_get("tenant_id")?,
        secret_binding_id: row.try_get("secret_binding_id")?,
        purpose: row.try_get("purpose")?,
        provider_id: row.try_get("provider")?,
        state: row.try_get("state")?,
        generation: row.try_get("generation")?,
        version: row.try_get("version")?,
        payload: payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
        revoked_at: row.try_get("revoked_at")?,
    })
}

fn secret_binding_resolution_from_row(
    row: PgRow,
) -> Result<SecretBindingResolutionRecord, RepositoryError> {
    let payload_record =
        payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    if payload_record.schema_version != 1 {
        return Err(RepositoryError::CorruptRow(
            "SecretBinding resolution payload schema is unsupported".to_owned(),
        ));
    }
    let record = SecretBindingResolutionRecord {
        tenant_id: row
            .try_get::<String, _>("tenant_id")?
            .parse()
            .map_err(|failure| {
                RepositoryError::CorruptRow(format!("SecretBinding tenant: {failure}"))
            })?,
        secret_binding_id: row
            .try_get::<String, _>("secret_binding_id")?
            .parse()
            .map_err(|failure| {
                RepositoryError::CorruptRow(format!("SecretBinding ID: {failure}"))
            })?,
        purpose: row
            .try_get::<String, _>("purpose")?
            .parse()
            .map_err(|failure| {
                RepositoryError::CorruptRow(format!("SecretBinding purpose: {failure}"))
            })?,
        provider_id: row
            .try_get::<String, _>("provider")?
            .parse()
            .map_err(|failure| {
                RepositoryError::CorruptRow(format!("SecretBinding provider: {failure}"))
            })?,
        state: row
            .try_get::<String, _>("state")?
            .parse()
            .map_err(|failure| {
                RepositoryError::CorruptRow(format!("SecretBinding state: {failure}"))
            })?,
        generation: u64::try_from(row.try_get::<i64, _>("generation")?).map_err(|_| {
            RepositoryError::CorruptRow("SecretBinding generation is invalid".to_owned())
        })?,
        encrypted_reference: EncryptedOpaqueReference::new(
            row.try_get("opaque_reference_ciphertext")?,
        )
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        key_id: row.try_get("key_id")?,
        reference_digest: row
            .try_get::<String, _>("reference_digest")?
            .parse()
            .map_err(|failure| {
                RepositoryError::CorruptRow(format!("SecretBinding reference digest: {failure}"))
            })?,
        payload: decode_typed_payload(&payload_record, "SecretBinding resolution")?,
    };
    record.validate().map_err(|failure| {
        RepositoryError::CorruptRow(format!("SecretBinding resolution: {failure:?}"))
    })?;
    Ok(record)
}

fn resource_from_row(row: PgRow) -> Result<ResourceRecord, RepositoryError> {
    Ok(ResourceRecord {
        tenant_id: row.try_get("tenant_id")?,
        resource_id: row.try_get("resource_id")?,
        resource_kind: row.try_get("resource_kind")?,
        lifecycle_state: row.try_get("lifecycle_state")?,
        gate_state: row.try_get("gate_state")?,
        draft_generation: row.try_get("draft_generation")?,
        active_version_id: row.try_get("active_version_id")?,
        active_deployment_id: row.try_get("active_deployment_id")?,
        version: row.try_get("version")?,
        payload: payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn resource_version_from_row(row: PgRow) -> Result<ResourceVersionRecord, RepositoryError> {
    Ok(ResourceVersionRecord {
        tenant_id: row.try_get("tenant_id")?,
        resource_version_id: row.try_get("resource_version_id")?,
        resource_id: row.try_get("resource_id")?,
        resource_version_kind: row.try_get("resource_version_kind")?,
        revision_no: row.try_get("revision_no")?,
        content_digest: row.try_get("content_digest")?,
        artifact_id: row.try_get("artifact_id")?,
        payload: payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?,
        created_by: row.try_get("created_by")?,
        created_at: row.try_get("created_at")?,
    })
}

fn deployment_from_row(row: PgRow) -> Result<DeploymentRecord, RepositoryError> {
    Ok(DeploymentRecord {
        tenant_id: row.try_get("tenant_id")?,
        deployment_id: row.try_get("deployment_id")?,
        resource_id: row.try_get("resource_id")?,
        resource_version_id: row.try_get("resource_version_id")?,
        environment: row.try_get("environment")?,
        bindings: payload_from_row(
            &row,
            "payload_schema_version",
            "bindings",
            "bindings_digest",
        )?,
        created_by: row.try_get("created_by")?,
        created_at: row.try_get("created_at")?,
    })
}

pub(crate) fn run_bindings_from_row(
    row: &PgRow,
    schema_column: &str,
    value_column: &str,
    digest_column: &str,
) -> Result<RunBindingsSnapshot, RepositoryError> {
    let bindings_schema_version: i32 = row.try_get(schema_column)?;
    let bindings_value: Value = row.try_get(value_column)?;
    let bindings_digest: String = row.try_get(digest_column)?;
    if bindings_schema_version != 1
        || bindings_value.get("schema_version").and_then(Value::as_i64) != Some(1)
    {
        return Err(RepositoryError::CorruptRow(
            "run bindings schema version does not match".to_owned(),
        ));
    }
    let bindings_bytes = serde_jcs::to_vec(&bindings_value)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    if bindings_bytes.len() > DEFAULT_PAYLOAD_LIMIT {
        return Err(RepositoryError::CorruptRow(
            "run bindings exceed their hard bound".to_owned(),
        ));
    }
    let bindings: RunBindingsSnapshot = serde_json::from_value(bindings_value)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    bindings
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    if bindings.canonical_digest.to_string() != bindings_digest {
        return Err(RepositoryError::CorruptRow(
            "run bindings canonical digest does not match".to_owned(),
        ));
    }
    Ok(bindings)
}

pub(crate) fn persisted_run_from_row(row: PgRow) -> Result<RunRecord, RepositoryError> {
    let diagnostic = crate::recovery_isolation::identity(
        &row,
        "run_id",
        ResourceKind::Run,
        insight_platform_jobs::store::SafetyScanPhase::OwnerDecode,
    )?;
    crate::recovery_isolation::persisted(run_from_row(row), &diagnostic)
}

pub(crate) fn run_from_row(row: PgRow) -> Result<RunRecord, RepositoryError> {
    let run_id_text: String = row.try_get("run_id")?;
    let run_id: ResourceId =
        run_id_text
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?;
    if run_id.kind() != ResourceKind::Run {
        return Err(RepositoryError::CorruptRow(
            "run_id has the wrong nominal kind".to_owned(),
        ));
    }
    let bindings = run_bindings_from_row(
        &row,
        "bindings_schema_version",
        "bindings",
        "bindings_digest",
    )?;

    let current_payload = payload_from_row(
        &row,
        "current_schema_version",
        "current_payload",
        "current_payload_digest",
    )?;
    let current: RunCurrentSnapshot = serde_json::from_value(current_payload.value)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    current
        .validate(&run_id)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;

    let agent_deployment_id: String = row.try_get("agent_deployment_id")?;
    let input_value_id: Option<String> = row.try_get("input_value_id")?;
    let output_value_id: Option<String> = row.try_get("output_value_id")?;
    let root_run_id: String = row.try_get("root_run_id")?;
    let parent_run_id: Option<String> = row.try_get("parent_run_id")?;
    let parent_node_id: Option<String> = row.try_get("parent_node_id")?;
    let depth: i32 = row.try_get("depth")?;
    let pause_generation: i64 = row.try_get("pause_generation")?;
    let cancel_generation: i64 = row.try_get("cancel_generation")?;
    let timeout_generation: i64 = row.try_get("timeout_generation")?;
    let state: String = row.try_get("state")?;
    state
        .parse::<RunState>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    if agent_deployment_id != bindings.agent.deployment_id.to_string()
        || input_value_id.as_deref() != Some(current.input_value_id.to_string().as_str())
        || output_value_id != current.output_value_id.as_ref().map(ToString::to_string)
        || root_run_id != current.ancestry.root_run_id.to_string()
        || parent_run_id
            != current
                .ancestry
                .parent_run_id
                .as_ref()
                .map(ToString::to_string)
        || parent_node_id
            != current
                .ancestry
                .parent_node_execution_id
                .as_ref()
                .map(ToString::to_string)
        || depth != i32::from(current.ancestry.depth)
        || u64::try_from(pause_generation).ok() != Some(current.control.pause_generation)
        || u64::try_from(cancel_generation).ok() != Some(current.control.cancel_generation)
        || u64::try_from(timeout_generation).ok() != Some(current.control.timeout_generation)
    {
        return Err(RepositoryError::CorruptRow(
            "run hot columns disagree with the typed current snapshot".to_owned(),
        ));
    }

    Ok(RunRecord {
        tenant_id: row.try_get("tenant_id")?,
        run_id: run_id_text,
        root_run_id,
        parent_run_id,
        parent_node_id,
        agent_deployment_id,
        principal_id: row.try_get("principal_id")?,
        trace: TraceIdentityV1::new(
            row.try_get::<String, _>("trace_id")?
                .parse::<TraceId>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        ),
        state,
        version: row.try_get("version")?,
        bindings,
        execution_requirement: crate::execution_requirements::requirement_from_row(&row)?,
        current,
        input_value_id,
        output_value_id,
        depth,
        descendant_count: row.try_get("descendant_count")?,
        active_work_count: row.try_get("active_work_count")?,
        pause_generation,
        cancel_generation,
        timeout_generation,
        public_sequence: row.try_get("public_sequence")?,
        public_replay_floor: row.try_get("public_replay_floor")?,
        retry_at: row.try_get("retry_at")?,
        deadline: row.try_get("deadline")?,
        started_at: row.try_get("started_at")?,
        terminal_at: row.try_get("terminal_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn orchestration_signal_target_from_row(
    row: PgRow,
) -> Result<OrchestrationSignalWakeTarget, RepositoryError> {
    let wake_generation: i64 = row.try_get("wake_generation")?;
    Ok(OrchestrationSignalWakeTarget {
        job_id: row.try_get::<String, _>("job_id")?.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?,
        job_version: row.try_get("version")?,
        wake_generation: u64::try_from(wake_generation).map_err(|_| {
            RepositoryError::CorruptRow("negative Signal wake generation".to_owned())
        })?,
    })
}

pub(crate) fn persisted_job_from_row(row: PgRow) -> Result<JobRecord, RepositoryError> {
    let diagnostic = crate::recovery_isolation::identity(
        &row,
        "job_id",
        ResourceKind::Job,
        insight_platform_jobs::store::SafetyScanPhase::JobDecode,
    )?;
    crate::recovery_isolation::persisted(job_from_row(row), &diagnostic)
}

pub(crate) fn job_from_row(row: PgRow) -> Result<JobRecord, RepositoryError> {
    let job_kind: String = row.try_get("job_kind")?;
    let job_kind_value = job_kind
        .parse::<JobKind>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let work_class: String = row.try_get("work_class")?;
    let work_class_value = work_class
        .parse::<WorkClass>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let owner_kind: String = row.try_get("owner_kind")?;
    let owner_id: String = row.try_get("owner_id")?;
    let typed_owner = owner_id
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let state: String = row.try_get("state")?;
    state
        .parse::<JobState>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    if owner_kind != typed_owner.kind().descriptor().name
        || !is_job_kind_work_owner_triple(job_kind_value, work_class_value, typed_owner.kind())
    {
        return Err(RepositoryError::CorruptRow(
            "Job work class and typed owner pair are not registered".to_owned(),
        ));
    }
    Ok(JobRecord {
        tenant_id: row.try_get("tenant_id")?,
        job_id: row.try_get("job_id")?,
        job_kind,
        work_class,
        owner_kind,
        owner_id,
        trace: TraceIdentityV1::new(
            row.try_get::<String, _>("trace_id")?
                .parse::<TraceId>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        ),
        invocation_id: row.try_get("invocation_id")?,
        run_id: row.try_get("run_id")?,
        node_id: row.try_get("node_id")?,
        state,
        version: row.try_get("version")?,
        attempt_no: row.try_get("attempt_no")?,
        attempt_limit: row.try_get("attempt_limit")?,
        lease_epoch: row.try_get("lease_epoch")?,
        worker_id: row.try_get("worker_id")?,
        lease_token_digest: row.try_get("lease_token_digest")?,
        lease_expires_at: row.try_get("lease_expires_at")?,
        heartbeat_at: row.try_get("heartbeat_at")?,
        scheduled_at: row.try_get("scheduled_at")?,
        retry_at: row.try_get("retry_at")?,
        deadline: row.try_get("deadline")?,
        priority: scheduler_priority_from_database(row.try_get("priority")?)?,
        wake_kind: row.try_get("wake_kind")?,
        wake_state: row.try_get("wake_state")?,
        wake_generation: row.try_get("wake_generation")?,
        request_digest: row.try_get("request_digest")?,
        result_digest: row.try_get("result_digest")?,
        effect_key_digest: row.try_get("effect_key_digest")?,
        quota_reservation_id: row.try_get("quota_reservation_id")?,
        payload: payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?,
        execution_requirement: crate::execution_requirements::requirement_from_row(&row)?,
        attempt_build_digest: row
            .try_get::<Option<String>, _>("attempt_build_digest")?
            .map(|digest| {
                digest
                    .parse::<Sha256Digest>()
                    .map_err(|error| RepositoryError::CorruptRow(error.to_string()))
            })
            .transpose()?,
        started_at: row.try_get("started_at")?,
        terminal_at: row.try_get("terminal_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn persisted_child_run_link_from_row(row: PgRow) -> Result<ChildRunLinkRecord, RepositoryError> {
    let diagnostic = crate::recovery_isolation::identity(
        &row,
        "node_id",
        ResourceKind::ChildRunLink,
        insight_platform_jobs::store::SafetyScanPhase::OwnerDecode,
    )?;
    crate::recovery_isolation::persisted(child_run_link_from_row(row), &diagnostic)
}
fn child_run_link_from_row(row: PgRow) -> Result<ChildRunLinkRecord, RepositoryError> {
    let tenant_id: String = row.try_get("tenant_id")?;
    let child_link_id: String = row.try_get("node_id")?;
    let parent_run_id: String = row.try_get("run_id")?;
    let parent_node_execution_id: String = row
        .try_get::<Option<String>, _>("parent_node_id")?
        .ok_or_else(|| RepositoryError::CorruptRow("ChildRunLink has no parent Node".to_owned()))?;
    let child_run_id: String = row
        .try_get::<Option<String>, _>("related_run_id")?
        .ok_or_else(|| RepositoryError::CorruptRow("ChildRunLink has no child Run".to_owned()))?;
    if row.try_get::<String, _>("record_kind")? != "child_run_link"
        || row.try_get::<String, _>("node_kind")? != "child_run"
    {
        return Err(RepositoryError::CorruptRow(
            "ChildRunLink row has an invalid kind".to_owned(),
        ));
    }
    let stored: StoredChildRunLinkPayload = decode_typed_payload(
        &payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?,
        "ChildRunLink",
    )?;
    if stored.slot_id.is_empty()
        || stored.slot_id.len() > 128
        || stored.source_value_ids.len() > MAX_CHILD_INPUT_SOURCES
        || stored
            .source_value_ids
            .iter()
            .any(|id| id.kind() != ResourceKind::RunValue)
        || stored.child_root_scope_id.kind() != ResourceKind::ScopeInstance
        || stored.child_entry_node_execution_id.kind() != ResourceKind::NodeExecution
        || stored.child_orchestration_job_id.kind() != ResourceKind::Job
    {
        return Err(RepositoryError::CorruptRow(
            "ChildRunLink stored request evidence is invalid".to_owned(),
        ));
    }
    let state = row
        .try_get::<String, _>("state")?
        .parse::<ChildLinkState>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let generation: i64 = row.try_get("generation")?;
    let version: i64 = row.try_get("version")?;
    let deadline: DateTime<Utc> = row.try_get("deadline")?;
    let terminal_at: Option<DateTime<Utc>> = row.try_get("terminal_at")?;
    let projection = ChildRunLinkProjection {
        tenant_id: tenant_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?,
        child_link_id: child_link_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?,
        parent_run_id: parent_run_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?,
        parent_node_execution_id: parent_node_execution_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?,
        child_run_id: child_run_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?,
        state,
        generation: u64::try_from(generation).map_err(|_| {
            RepositoryError::CorruptRow("negative ChildRunLink generation".to_owned())
        })?,
        version: u64::try_from(version)
            .map_err(|_| RepositoryError::CorruptRow("negative ChildRunLink version".to_owned()))?,
        payload: stored.link.clone(),
        deadline,
        terminal_at,
    };
    projection
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(ChildRunLinkRecord {
        tenant_id,
        child_link_id,
        parent_run_id,
        parent_node_execution_id,
        child_run_id,
        state,
        generation,
        version,
        slot_id: stored.slot_id,
        source_value_ids: stored.source_value_ids,
        child_root_scope_id: stored.child_root_scope_id,
        child_entry_node_execution_id: stored.child_entry_node_execution_id,
        child_orchestration_job_id: stored.child_orchestration_job_id,
        payload: stored.link,
        deadline,
        terminal_at,
    })
}

fn child_link_projection(
    record: &ChildRunLinkRecord,
) -> Result<ChildRunLinkProjection, RepositoryError> {
    let projection = ChildRunLinkProjection {
        tenant_id: record.tenant_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?,
        child_link_id: record.child_link_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?,
        parent_run_id: record.parent_run_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?,
        parent_node_execution_id: record.parent_node_execution_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?,
        child_run_id: record.child_run_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?,
        state: record.state,
        generation: u64::try_from(record.generation).map_err(|_| {
            RepositoryError::CorruptRow("negative ChildRunLink generation".to_owned())
        })?,
        version: u64::try_from(record.version)
            .map_err(|_| RepositoryError::CorruptRow("negative ChildRunLink version".to_owned()))?,
        payload: record.payload.clone(),
        deadline: record.deadline,
        terminal_at: record.terminal_at,
    };
    projection
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(projection)
}

pub(crate) fn persisted_task_from_row(row: PgRow) -> Result<TaskRecord, RepositoryError> {
    let task_id: ResourceId = row
        .try_get::<String, _>("task_id")?
        .parse()
        .map_err(|_| RepositoryError::CorruptRow("invalid Task identity".into()))?;
    if ![
        TaskKind::Approval.task_id_kind(),
        TaskKind::InteractionForm.task_id_kind(),
    ]
    .contains(&task_id.kind())
    {
        return Err(RepositoryError::CorruptRow(
            "invalid Task identity kind".into(),
        ));
    }
    let diagnostic = crate::recovery_isolation::identity(
        &row,
        "task_id",
        task_id.kind(),
        insight_platform_jobs::store::SafetyScanPhase::OwnerDecode,
    )?;
    crate::recovery_isolation::persisted(task_from_row(row), &diagnostic)
}

pub(crate) fn task_from_row(row: PgRow) -> Result<TaskRecord, RepositoryError> {
    let task_kind = row
        .try_get::<String, _>("task_kind")?
        .parse::<TaskKind>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let state = row
        .try_get::<String, _>("state")?
        .parse::<TaskState>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let task_id: String = row.try_get("task_id")?;
    let typed_task_id = task_id
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let principal_snapshot_schema_version: i32 =
        row.try_get("principal_snapshot_schema_version")?;
    if typed_task_id.kind() != task_kind.task_id_kind() || principal_snapshot_schema_version != 1 {
        return Err(RepositoryError::CorruptRow(
            "Task identity or PrincipalSnapshot schema version is invalid".to_owned(),
        ));
    }
    let record = TaskRecord {
        tenant_id: row.try_get("tenant_id")?,
        task_id,
        trace: TraceIdentityV1::new(
            row.try_get::<String, _>("trace_id")?
                .parse::<TraceId>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        ),
        task_kind,
        owner_kind: row.try_get("owner_kind")?,
        owner_id: row.try_get("owner_id")?,
        run_id: row.try_get("run_id")?,
        node_id: row.try_get("node_id")?,
        invocation_id: row.try_get("invocation_id")?,
        state,
        generation: row.try_get("generation")?,
        version: row.try_get("version")?,
        response_schema_digest: row.try_get("response_schema_digest")?,
        payload: payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?,
        response_value_id: row.try_get("response_value_id")?,
        deadline: row.try_get("deadline")?,
        responded_at: row.try_get("responded_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    };
    task_projection(&record)?;
    Ok(record)
}

pub(crate) fn task_projection(record: &TaskRecord) -> Result<TaskProjection, RepositoryError> {
    let payload: TaskPayload = decode_typed_payload(&record.payload, "Task")?;
    let projection = TaskProjection {
        payload_schema_version: u32::try_from(record.payload.schema_version).map_err(|_| {
            RepositoryError::CorruptRow("Task payload version is invalid".to_owned())
        })?,
        tenant_id: record.tenant_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?,
        task_id: record.task_id.parse().map_err(
            |failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            },
        )?,
        kind: record.task_kind,
        state: record.state,
        generation: u64::try_from(record.generation)
            .map_err(|_| RepositoryError::CorruptRow("negative Task generation".to_owned()))?,
        version: u64::try_from(record.version)
            .map_err(|_| RepositoryError::CorruptRow("negative Task version".to_owned()))?,
        response_schema_digest: record
            .response_schema_digest
            .as_deref()
            .map(str::parse::<Sha256Digest>)
            .transpose()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        payload,
        response_value_id: record
            .response_value_id
            .as_deref()
            .map(str::parse)
            .transpose()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?,
        deadline: record.deadline,
        resolved_at: record.responded_at,
    };
    projection
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(projection)
}

pub(crate) fn has_started_worker_attempt(record: &JobRecord) -> bool {
    record.started_at.is_some() && record.attempt_no > 0
}

/// Parse the shared lease columns without inferring a domain's wake payload.
pub(crate) fn job_lease_projection(
    record: &JobRecord,
) -> Result<Option<JobLease>, RepositoryError> {
    let lease = match (
        &record.worker_id,
        &record.lease_token_digest,
        record.heartbeat_at,
        record.lease_expires_at,
    ) {
        (Some(worker_id), Some(token_digest), Some(heartbeat_at), Some(expires_at)) => {
            Some(JobLease {
                worker_process_generation_id: worker_id.parse().map_err(
                    |failure: insight_platform_contracts::ResourceIdError| {
                        RepositoryError::CorruptRow(failure.to_string())
                    },
                )?,
                lease_generation: u64::try_from(record.lease_epoch).map_err(|_| {
                    RepositoryError::CorruptRow("negative Job lease generation".to_owned())
                })?,
                token_digest: token_digest
                    .parse::<Sha256Digest>()
                    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
                heartbeat_at,
                expires_at,
            })
        }
        (None, None, None, None) => None,
        _ => {
            return Err(RepositoryError::CorruptRow(
                "Job lease columns are incomplete".to_owned(),
            ))
        }
    };
    if let Some(lease) = &lease {
        lease
            .validate(record.deadline)
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    }
    Ok(lease)
}

pub(crate) fn job_projection(record: &JobRecord) -> Result<JobProjection, RepositoryError> {
    let tenant_id = record
        .tenant_id
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let job_id = record
        .job_id
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let work_class = record
        .work_class
        .parse::<WorkClass>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let owner_id = record
        .owner_id
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let state = record
        .state
        .parse::<JobState>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let wake = match work_class {
        WorkClass::Orchestration => {
            let payload: OrchestrationJobPayload =
                decode_orchestration_job_payload(&record.payload)?;
            payload
                .validate()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            if payload.node_execution_id != owner_id {
                return Err(RepositoryError::CorruptRow(
                    "orchestration Job payload owner does not match the Job owner".to_owned(),
                ));
            }
            payload.wake_contract
        }
        WorkClass::CapabilityNative | WorkClass::CapabilityRemote => {
            let payload: CapabilityJobPayload =
                decode_versioned_payload(&record.payload, "Capability Job")?;
            if payload.binding.invocation_id != owner_id {
                return Err(RepositoryError::CorruptRow(
                    "Capability Job payload owner does not match the Job owner".to_owned(),
                ));
            }
            payload.wake_contract
        }
        WorkClass::Context if owner_id.kind() == ResourceKind::ContextQuery => {
            let payload: ContextJobPayload =
                decode_versioned_payload(&record.payload, "Context Job")?;
            if payload.binding.context_query_id != owner_id {
                return Err(RepositoryError::CorruptRow(
                    "Context Job payload owner does not match the Job owner".to_owned(),
                ));
            }
            payload.wake_contract
        }
        WorkClass::Context if owner_id.kind() == ResourceKind::ContextDataset => {
            let payload: ContextDatasetBuildJobPayload =
                decode_versioned_payload(&record.payload, "Context Dataset build Job")?;
            payload
                .validate_for_owner(&owner_id)
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            payload.wake_contract
        }
        WorkClass::Context if owner_id.kind() == ResourceKind::McpOperation => {
            let payload: insight_platform_context::ContextSubscriptionRefreshJobPayload =
                decode_versioned_payload(&record.payload, "Context subscription refresh Job")?;
            payload
                .validate_frozen()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            if payload.request.subscription_id != owner_id
                || payload.request.deadline != record.deadline
            {
                return Err(RepositoryError::CorruptRow(
                    "Context subscription refresh Job owner does not match its payload".to_owned(),
                ));
            }
            None
        }
        WorkClass::Context => {
            return Err(RepositoryError::CorruptRow(
                "Context Job has an unsupported owner kind".to_owned(),
            ));
        }
        _ => None,
    };
    match (&wake, &record.wake_kind, &record.wake_state) {
        (None, None, None) if record.wake_generation == 0 => {}
        (Some(wake), Some(kind), Some(state))
            if kind == wake.kind.as_str()
                && state == "pending"
                && u64::try_from(record.wake_generation).ok() == Some(wake.generation) => {}
        _ => {
            return Err(RepositoryError::CorruptRow(
                "Job wake columns and closed payload disagree".to_owned(),
            ))
        }
    }
    let lease = job_lease_projection(record)?;
    let projection = JobProjection {
        trace: record.trace,
        tenant_id,
        job_id,
        work_class,
        owner: JobOwnerRef {
            owner_id: owner_id.clone(),
            owner_kind: owner_id.kind(),
        },
        state,
        version: u64::try_from(record.version)
            .map_err(|_| RepositoryError::CorruptRow("negative Job version".to_owned()))?,
        attempt_count: u32::try_from(record.attempt_no)
            .map_err(|_| RepositoryError::CorruptRow("negative Job attempt count".to_owned()))?,
        attempt_limit: u32::try_from(record.attempt_limit)
            .map_err(|_| RepositoryError::CorruptRow("negative Job attempt limit".to_owned()))?,
        lease_generation: u64::try_from(record.lease_epoch)
            .map_err(|_| RepositoryError::CorruptRow("negative Job lease generation".to_owned()))?,
        lease,
        scheduled_at: record.scheduled_at,
        retry_at: record.retry_at,
        wake,
        deadline: record.deadline,
    };
    projection
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(projection)
}

fn quota_account_from_row(row: PgRow) -> Result<QuotaAccountRecord, RepositoryError> {
    Ok(QuotaAccountRecord {
        tenant_id: row.try_get("tenant_id")?,
        quota_account_id: row.try_get("quota_account_id")?,
        scope_kind: row.try_get("scope_kind")?,
        scope_id: row.try_get("scope_id")?,
        work_class: row.try_get("work_class")?,
        metric: row.try_get("metric")?,
        limit_value: row.try_get("limit_value")?,
        reserved_value: row.try_get("reserved_value")?,
        used_value: row.try_get("used_value")?,
        version: row.try_get("version")?,
        payload: payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

pub(crate) fn payload_from_row(
    row: &PgRow,
    schema_column: &str,
    value_column: &str,
    digest_column: &str,
) -> Result<TypedPayload, RepositoryError> {
    let schema_version: i32 = row.try_get(schema_column)?;
    let value: Value = row.try_get(value_column)?;
    let digest: String = row.try_get(digest_column)?;
    let canonical = serde_jcs::to_vec(&value)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    if sha256(&canonical) != digest {
        return Err(RepositoryError::CorruptRow(format!(
            "{value_column} digest does not match"
        )));
    }
    if value.get("schema_version").and_then(Value::as_i64) != Some(i64::from(schema_version)) {
        return Err(RepositoryError::CorruptRow(format!(
            "{value_column} schema version does not match"
        )));
    }
    Ok(TypedPayload {
        schema_version,
        value,
        digest,
    })
}

async fn existing_quota_entry(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &str,
    quota_account_id: &str,
    correlation_id: &str,
    entry_kind: &str,
    request_digest: &str,
) -> Result<Option<QuotaAccountRecord>, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT request_digest
        FROM insight_platform.quota_ledger
        WHERE tenant_id = $1 AND quota_account_id = $2
          AND correlation_id = $3 AND entry_kind = $4
        FOR SHARE
        "#,
    )
    .bind(tenant_id)
    .bind(quota_account_id)
    .bind(correlation_id)
    .bind(entry_kind)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let actual_digest: String = row.try_get("request_digest")?;
    if actual_digest != request_digest {
        return Err(RepositoryError::IdempotencyConflict);
    }
    let account = sqlx::query(
        "SELECT * FROM insight_platform.quota_accounts WHERE tenant_id = $1 AND quota_account_id = $2",
    )
    .bind(tenant_id)
    .bind(quota_account_id)
    .fetch_one(&mut **transaction)
    .await?;
    Ok(Some(quota_account_from_row(account)?))
}

struct QuotaEntryInsert<'a> {
    tenant_id: &'a str,
    quota_entry_id: &'a str,
    quota_account_id: &'a str,
    correlation_id: &'a str,
    entry_kind: &'a str,
    reserved_amount: i64,
    used_amount: i64,
    account_version: i64,
    request_digest: &'a str,
}

async fn insert_quota_entry(
    transaction: &mut Transaction<'_, Postgres>,
    entry: QuotaEntryInsert<'_>,
) -> Result<(), RepositoryError> {
    sqlx::query(
        r#"
        INSERT INTO insight_platform.quota_ledger (
            tenant_id, quota_entry_id, quota_account_id, correlation_id,
            entry_kind, reserved_amount, used_amount, account_version, request_digest
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#,
    )
    .bind(entry.tenant_id)
    .bind(entry.quota_entry_id)
    .bind(entry.quota_account_id)
    .bind(entry.correlation_id)
    .bind(entry.entry_kind)
    .bind(entry.reserved_amount)
    .bind(entry.used_amount)
    .bind(entry.account_version)
    .bind(entry.request_digest)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn validate_quota_mutation(
    tenant_id: &str,
    quota_account_id: &str,
    quota_entry_id: &str,
    correlation_id: &str,
    amount: i64,
    request_digest: &str,
) -> Result<(), RepositoryError> {
    for id in [tenant_id, quota_account_id, quota_entry_id, correlation_id] {
        validate_id(id)?;
    }
    validate_digest(request_digest)?;
    if amount <= 0 {
        return Err(RepositoryError::InvalidInput(
            "quota mutation amount must be positive".to_owned(),
        ));
    }
    Ok(())
}

fn validate_id(value: &str) -> Result<(), RepositoryError> {
    let Some((prefix, raw_uuid)) = value.split_once('_') else {
        return Err(RepositoryError::InvalidInput(format!(
            "{value:?} is not a platform id"
        )));
    };
    if prefix.len() < 2
        || prefix.len() > 8
        || !prefix
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    {
        return Err(RepositoryError::InvalidInput(format!(
            "{value:?} has an invalid platform id prefix"
        )));
    }
    let uuid = Uuid::parse_str(raw_uuid)
        .map_err(|_| RepositoryError::InvalidInput(format!("{value:?} has an invalid UUID")))?;
    if uuid.get_version() != Some(Version::SortRand)
        || uuid.get_variant() != Variant::RFC4122
        || format!("{prefix}_{}", uuid.hyphenated()) != value
    {
        return Err(RepositoryError::InvalidInput(format!(
            "{value:?} is not a canonical UUIDv7 platform id"
        )));
    }
    Ok(())
}

fn validate_digest(value: &str) -> Result<(), RepositoryError> {
    let valid = value.len() == 71
        && value.starts_with("sha256:")
        && value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    if valid {
        Ok(())
    } else {
        Err(RepositoryError::InvalidInput(format!(
            "{value:?} is not a sha256 digest"
        )))
    }
}

fn validate_code(label: &str, value: &str) -> Result<(), RepositoryError> {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return Err(RepositoryError::InvalidInput(format!("{label} is empty")));
    };
    if !first.is_ascii_lowercase()
        || value.len() > 128
        || !bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(RepositoryError::InvalidInput(format!(
            "{label} is not a stable code"
        )));
    }
    Ok(())
}

fn validate_event_type(value: &str) -> Result<(), RepositoryError> {
    if value.is_empty()
        || value.len() > 128
        || !value.starts_with(|character: char| character.is_ascii_lowercase())
        || !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'.')
        })
    {
        return Err(RepositoryError::InvalidInput(format!(
            "{value:?} is not a stable event code"
        )));
    }
    Ok(())
}

fn sha256(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    format!("sha256:{encoded}")
}

impl From<insight_platform_contracts::PayloadError> for RepositoryError {
    fn from(error: insight_platform_contracts::PayloadError) -> Self {
        match error {
            insight_platform_contracts::PayloadError::InvalidInput(message) => {
                Self::InvalidInput(message)
            }
        }
    }
}

impl From<insight_platform_orchestrator::store::ControllerStoreError> for RepositoryError {
    fn from(error: insight_platform_orchestrator::store::ControllerStoreError) -> Self {
        match error {
            insight_platform_orchestrator::store::ControllerStoreError::InvalidInput(message) => {
                Self::InvalidInput(message)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct ExamplePayload<'a> {
        kind: &'a str,
        value: u32,
    }

    fn test_worker_manifest(work_class: WorkClass) -> WorkerManifest {
        let digest: Sha256Digest = format!("sha256:{}", "1".repeat(64)).parse().unwrap();
        WorkerManifest {
            manifest_version: 2,
            worker_role: "unit_fixture".into(),
            work_class,
            adapter_runtime_digest: digest.clone(),
            worker_build_digest: format!("sha256:{}", "2".repeat(64)).parse().unwrap(),
            execution_capabilities:
                insight_platform_plan::execution::program_execution_capabilities(),
            protocol_version: 1,
            max_concurrency: 1,
            critical_control_reserved_slots: 1,
        }
    }

    #[test]
    fn typed_payload_is_closed_by_version_and_digest() {
        let payload = TypedPayload::new(
            2,
            &ExamplePayload {
                kind: "example",
                value: 7,
            },
        )
        .unwrap();
        assert_eq!(payload.value["schema_version"], 2);
        assert!(payload.digest.starts_with("sha256:"));
        assert_eq!(payload.digest.len(), 71);
    }

    #[test]
    fn typed_payload_rejects_non_object_and_oversize() {
        assert!(TypedPayload::new(1, &vec![1, 2, 3]).is_err());
        assert!(TypedPayload::with_limit(1, &serde_json::json!({"large": "abcd"}), 4).is_err());
    }

    #[test]
    fn model_turn_wait_requires_explicit_total_capability_calls() {
        let id = |kind| ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap();
        let digest = |marker: char| {
            format!("sha256:{}", marker.to_string().repeat(64))
                .parse()
                .unwrap()
        };
        let wait = StoredModelTurnWaitPayload {
            plan_node_key: PlanNodeKey::new("model-loop".to_owned()).unwrap(),
            plan_digest: digest('a'),
            source_orchestration_job_id: id(ResourceKind::Job),
            model_turn_id: id(ResourceKind::ModelTurn),
            model_job_id: id(ResourceKind::Job),
            output_port: ExactDataPortRef::RunInput {
                schema_digest: digest('b'),
            },
            resume_plan_node_key: PlanNodeKey::new("resume".to_owned()).unwrap(),
            resume_node_kind: PlanNodeKind::Return,
            root_scope_id: id(ResourceKind::ScopeInstance),
            continuation_attempt_limit: 3,
            retry_backoff_milliseconds: 100,
            priority: SchedulerPriority::Normal,
            deadline: Utc::now() + Duration::minutes(1),
            round_ordinal: 2,
            maximum_rounds: 4,
            total_capability_calls: 3,
            maximum_capability_calls: 8,
            maximum_parallel_calls_per_round: 2,
            token_budget: 1_024,
        };
        let mut value = serde_json::to_value(&wait).unwrap();
        assert_eq!(
            serde_json::from_value::<StoredModelTurnWaitPayload>(value.clone()).unwrap(),
            wait
        );
        value
            .as_object_mut()
            .unwrap()
            .remove("total_capability_calls");
        assert!(serde_json::from_value::<StoredModelTurnWaitPayload>(value).is_err());
    }

    #[test]
    fn registry_validation_claim_permissions_follow_the_domain_permission_map() {
        let permissions = registry_validation_write_permission_map();
        for kind in RegistryResourceKind::ALL {
            assert_eq!(
                permissions[kind.as_str()],
                write_permission(*kind).to_string()
            );
        }
        assert_eq!(
            permissions[RegistryResourceKind::ContextSourceInterface.as_str()],
            Permission::ContextWrite.to_string()
        );
        assert_eq!(
            permissions[RegistryResourceKind::CapabilityImplementation.as_str()],
            Permission::CapabilityWrite.to_string()
        );
    }

    #[test]
    fn development_bootstrap_requires_distinct_tenant_scoped_identity() {
        let id = |kind| ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap();
        let digest = |marker: char| {
            format!("sha256:{}", marker.to_string().repeat(64))
                .parse()
                .unwrap()
        };
        let installation_principal_id = id(ResourceKind::Principal);
        let developer_principal_id = id(ResourceKind::Principal);
        let registry_validator_principal_id = id(ResourceKind::Principal);
        let tenant_id = id(ResourceKind::Tenant);
        let command = BootstrapDevelopmentProfile {
            installation: BootstrapInstallationOperator {
                principal_id: installation_principal_id.clone(),
                request_id: id(ResourceKind::ServerRequest),
                authentication_authority_digest: digest('a'),
                subject_digest: digest('b'),
                evidence_digest: digest('c'),
            },
            tenant: NewTenant {
                tenant_id: tenant_id.to_string(),
                state: "active".to_owned(),
                config: TenantConfig::default(),
            },
            developer: NewPrincipal {
                principal_id: developer_principal_id.clone(),
                authentication_authority_digest: digest('a'),
                subject_digest: digest('d'),
                installation_bindings: PrincipalBindingsPayload {
                    installation_bindings: Vec::new(),
                },
            },
            service_principals: vec![NewPrincipal {
                principal_id: registry_validator_principal_id.clone(),
                authentication_authority_digest: digest('a'),
                subject_digest: digest('e'),
                installation_bindings: PrincipalBindingsPayload {
                    installation_bindings: Vec::new(),
                },
            }],
            tenant_principal_bindings: vec![NewTenantPrincipal {
                tenant_id: tenant_id.clone(),
                principal_id: developer_principal_id.clone(),
                principal_kind: PrincipalKind::AgentAuthor,
                payload: TenantPrincipalPayload {
                    permissions: PermissionSet::new(vec![Permission::AgentRead]).unwrap(),
                },
            }],
            artifact_authority: None,
        };
        assert!(validate_development_bootstrap(&command).is_ok());

        let mut reused_operator = command.clone();
        reused_operator.developer.principal_id = installation_principal_id;
        assert!(matches!(
            validate_development_bootstrap(&reused_operator),
            Err(RepositoryError::InvalidInput(_))
        ));

        let mut mismatched_tenant = command;
        mismatched_tenant.tenant_principal_bindings[0].tenant_id = id(ResourceKind::Tenant);
        assert!(matches!(
            validate_development_bootstrap(&mismatched_tenant),
            Err(RepositoryError::InvalidInput(_))
        ));
    }

    #[test]
    fn controller_completion_projects_only_the_succeeded_node_authority() {
        use PublicRunEventType as Public;
        // The controller transaction terminalizes its source node as succeeded before this event.
        assert_eq!(
            public_run_event_type("node_execution", "node.controller_completed", &Value::Null),
            Some(Public::NodeCompleted)
        );
        for (kind, event) in [
            ("job", "job.controller_completed"),
            ("run", "run.controller_advanced"),
            ("job", "node.controller_completed"),
            ("node_execution", "node.model_waiting"),
            ("node_execution", "node.external_leaf_completion_ready"),
        ] {
            assert_eq!(public_run_event_type(kind, event, &Value::Null), None);
        }
    }

    #[test]
    fn model_public_projection_uses_only_owning_durable_source() {
        use PublicRunEventType as Public;
        for (name, expected) in [
            ("model.started", Public::ModelStarted),
            ("model.tool_intent", Public::ModelToolIntent),
            ("model.completed", Public::ModelCompleted),
            ("model.failed", Public::ModelFailed),
            ("model.cancelled", Public::ModelCancelled),
            ("model.timed_out", Public::ModelTimedOut),
        ] {
            assert_eq!(
                public_run_event_type("model_turn", name, &Value::Null),
                Some(expected)
            );
            for wrong_source in ["run", "node_execution", "invocation", "unknown"] {
                assert_eq!(
                    public_run_event_type(wrong_source, name, &Value::Null),
                    None
                );
            }
        }
        for name in [
            "model.delta",
            "model.retry_scheduled",
            "model.cancelling",
            "model.unknown",
            "run.completed",
            "node.started",
        ] {
            assert_eq!(
                public_run_event_type("model_turn", name, &Value::Null),
                None
            );
        }
    }

    #[test]
    fn terminal_public_projection_requires_actual_producer_field() {
        use PublicRunEventType as Public;
        for (state, run, node) in [
            ("succeeded", Public::RunCompleted, Public::NodeCompleted),
            ("failed", Public::RunFailed, Public::NodeFailed),
            ("cancelled", Public::RunCancelled, Public::NodeCancelled),
            ("timed_out", Public::RunTimedOut, Public::NodeTimedOut),
        ] {
            for (source, name, expected) in [
                ("run", "run.terminal_committed", run),
                ("run", "run.terminal_converged", run),
                ("node_execution", "node.terminal_committed", node),
                ("node_execution", "node.terminal_converged", node),
            ] {
                assert_eq!(
                    public_run_event_type(
                        source,
                        name,
                        &serde_json::json!({"terminal_state":state})
                    ),
                    Some(expected)
                );
                assert_eq!(
                    public_run_event_type(source, name, &serde_json::json!({"state":state})),
                    None
                );
                for invalid in [
                    Value::Null,
                    serde_json::json!({"terminal_state":"running"}),
                    serde_json::json!({"terminal_state":1}),
                ] {
                    assert_eq!(public_run_event_type(source, name, &invalid), None);
                }
            }
        }
    }

    #[test]
    fn orchestration_task_response_has_a_public_resolved_projection() {
        assert_eq!(
            public_run_event_type("interaction", "interaction.respond", &Value::Null),
            Some(PublicRunEventType::InteractionResolved)
        );
        assert_eq!(
            public_run_event_type("interaction", "interaction.responded", &Value::Null),
            None
        );
    }

    #[test]
    fn sandbox_claim_rejects_arbitrary_json_in_the_shared_job_payload() {
        let now = Utc::now();
        let job = JobRecord {
            execution_requirement:
                insight_platform_contracts::ExecutionRequirement::DomainOperation {
                    operation_abi_identity: format!("sha256:{}", "1".repeat(64)).parse().unwrap(),
                    requirements:
                        insight_platform_contracts::DomainOperationRequirements::Control {
                            control_policy_digest: format!("sha256:{}", "2".repeat(64))
                                .parse()
                                .unwrap(),
                        },
                },
            attempt_build_digest: None,
            tenant_id: "ten_0198f1c3-8f49-7c3e-b1f3-773c28367b90".to_owned(),
            job_id: "job_0198f1c3-8f49-7c3e-b1f3-773c28367b91".to_owned(),
            job_kind: "sandbox_capability_execution".to_owned(),
            work_class: "sandbox".to_owned(),
            owner_kind: "job".to_owned(),
            owner_id: "job_0198f1c3-8f49-7c3e-b1f3-773c28367b92".to_owned(),
            trace: TraceIdentityV1::generate(),
            invocation_id: Some("inv_0198f1c3-8f49-7c3e-b1f3-773c28367b93".to_owned()),
            run_id: None,
            node_id: None,
            state: "ready".to_owned(),
            version: 1,
            attempt_no: 0,
            attempt_limit: 1,
            lease_epoch: 0,
            worker_id: None,
            lease_token_digest: None,
            lease_expires_at: None,
            heartbeat_at: None,
            scheduled_at: now,
            retry_at: None,
            deadline: now + Duration::minutes(1),
            priority: SchedulerPriority::Normal,
            wake_kind: None,
            wake_state: None,
            wake_generation: 0,
            request_digest: format!("sha256:{}", "a".repeat(64)),
            result_digest: None,
            effect_key_digest: None,
            quota_reservation_id: None,
            payload: TypedPayload::new(1, &serde_json::json!({"arbitrary": true})).unwrap(),
            started_at: None,
            terminal_at: None,
            created_at: now,
            updated_at: now,
        };
        let RepositoryError::InvalidPersistedObject(diagnostic) =
            validate_claimed_job_payload(&job).unwrap_err()
        else {
            panic!("invalid persisted Sandbox payload must carry an owning-object diagnostic");
        };
        assert_eq!(diagnostic.schema_version, 1);
        assert_eq!(diagnostic.tenant_id, job.tenant_id.parse().unwrap());
        assert_eq!(diagnostic.item_id, job.job_id.parse().unwrap());
        assert_eq!(
            diagnostic.phase,
            insight_platform_jobs::store::SafetyScanPhase::OwnerValidation
        );
        assert_eq!(
            diagnostic.code,
            insight_platform_jobs::store::SafetyScanDiagnosticCode::InvalidPersistedObject
        );
        diagnostic.validate().unwrap();
        let generic_claim = ClaimJobs {
            worker_manifest: test_worker_manifest(WorkClass::Sandbox),
            work_class: WorkClass::Sandbox.as_str().to_owned(),
            worker_id: "wrk_0198f1c3-8f49-7c3e-b1f3-773c28367b94".parse().unwrap(),
            limit: 1,
            lease_milliseconds: 1_000,
            lease_token_digests: vec![format!("sha256:{}", "b".repeat(64)).parse().unwrap()],
        };
        assert!(matches!(
            generic_claim.validate(),
            Err(RepositoryError::InvalidInput(_))
        ));

        let generic_artifact_claim = ClaimJobs {
            work_class: WorkClass::Artifact.as_str().to_owned(),
            ..generic_claim.clone()
        };
        assert!(matches!(
            generic_artifact_claim.validate(),
            Err(RepositoryError::InvalidInput(_))
        ));
        assert_eq!(
            ArtifactWorkerRole::DataWorker.job_kinds(),
            &["artifact_scan", "artifact_rescan"]
        );
        assert_eq!(
            ArtifactWorkerRole::Maintenance.job_kinds(),
            &["artifact_delete", "artifact_blob_cleanup"]
        );
    }

    #[tokio::test]
    async fn dedicated_mcp_claims_reject_the_wrong_work_class_before_database_io() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .unwrap();
        let repository = PgRepository::new(pool);
        let command = ClaimJobs {
            worker_manifest: test_worker_manifest(WorkClass::Context),
            work_class: WorkClass::Context.as_str().to_owned(),
            worker_id: "wrk_0198f1c3-8f49-7c3e-b1f3-773c28367b94".parse().unwrap(),
            limit: 1,
            lease_milliseconds: 1_000,
            lease_token_digests: vec![format!("sha256:{}", "b".repeat(64)).parse().unwrap()],
        };
        assert!(matches!(
            repository.claim_mcp_discovery_jobs(command.clone()).await,
            Err(RepositoryError::InvalidInput(_))
        ));
        assert!(matches!(
            repository.claim_mcp_subscription_jobs(command).await,
            Err(RepositoryError::InvalidInput(_))
        ));
        assert!(matches!(
            repository
                .list_due_mcp_subscription_reconciliations_global(0, 60_000, None)
                .await,
            Err(RepositoryError::InvalidInput(_))
        ));
        assert!(matches!(
            repository
                .list_due_mcp_subscription_recoveries_global(0, None)
                .await,
            Err(RepositoryError::InvalidInput(_))
        ));
    }

    #[test]
    fn identifiers_and_digests_fail_closed() {
        assert!(validate_id("job_018f22bb-3c21-7c65-b5f8-67e2452f8a9b").is_ok());
        assert!(validate_id("job_not-a-uuid").is_err());
        assert!(validate_digest(&format!("sha256:{}", "a".repeat(64))).is_ok());
        assert!(validate_digest("sha256:xyz").is_err());
    }

    #[test]
    fn only_stopping_map_policies_require_a_batch_admission_barrier() {
        assert!(!map_policy_requires_admission_barrier(
            MapFailurePolicy::AllSettled
        ));
        assert!(map_policy_requires_admission_barrier(
            MapFailurePolicy::FailFast
        ));
        assert!(map_policy_requires_admission_barrier(
            MapFailurePolicy::BoundedErrorCount {
                maximum_failures: 1,
            }
        ));
    }

    #[test]
    fn terminal_child_scan_uses_the_recovery_batch_limit() {
        let new_id = |kind| ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap();
        let slots = (0..65)
            .map(|_| TerminalChildRunSlot {
                parent_output_value_id: new_id(ResourceKind::RunValue),

                resume_job_id: new_id(ResourceKind::Job),
                resume_request_digest: format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
                child_link_event_id: new_id(ResourceKind::Event),
                child_link_outbox_id: new_id(ResourceKind::OutboxEvent),
                parent_run_event_id: new_id(ResourceKind::Event),
                parent_run_outbox_id: new_id(ResourceKind::OutboxEvent),
                parent_node_event_id: new_id(ResourceKind::Event),
                parent_node_outbox_id: new_id(ResourceKind::OutboxEvent),

                resume_job_event_id: new_id(ResourceKind::Event),
                resume_job_outbox_id: new_id(ResourceKind::OutboxEvent),
            })
            .collect();
        let command = DriveTerminalChildRuns {
            after: None,
            limit: 65,
            slots,
        };
        assert!(command.validate(1_000).is_ok());
        assert!(matches!(
            command.validate(64),
            Err(insight_platform_orchestrator::store::ControllerStoreError::InvalidInput(_))
        ));
    }

    #[test]
    fn child_cancellation_scan_uses_the_recovery_batch_limit() {
        let new_id = |kind| ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap();
        let slots = (0..65)
            .map(|_| ChildRunCancellationSlot {
                child_link_event_id: new_id(ResourceKind::Event),
                child_link_outbox_id: new_id(ResourceKind::OutboxEvent),
                child_run_event_id: new_id(ResourceKind::Event),
                child_run_outbox_id: new_id(ResourceKind::OutboxEvent),
            })
            .collect();
        let command = DriveChildRunCancellations {
            after: None,
            limit: 65,
            slots,
        };
        assert!(command.validate(1_000).is_ok());
        assert!(matches!(
            command.validate(64),
            Err(insight_platform_orchestrator::store::ControllerStoreError::InvalidInput(_))
        ));
    }
}
