mod activation;
mod artifact;
mod common;
mod control_repository;
mod error;
mod human_task;
mod ingress;
#[doc(hidden)]
pub mod migration_manifest;
mod model;
mod model_tool_parent_resume;
mod model_tool_queue;
mod postgres;
mod postgres_activation;
mod postgres_control;
mod postgres_model_tool_queue;
mod postgres_projection;
mod postgres_recovery;
mod postgres_retrieval_publication;
mod postgres_scheduler;
mod projection;
mod public_outbox;
mod recovery_repository;
mod retrieval_publication;
#[cfg(test)]
mod retrieval_safety_tests;
mod scheduler_repository;
mod scheduler_runtime;
#[cfg(test)]
mod scheduler_safety_tests;
mod sqlite;
mod sqlite_activation;
mod sqlite_control;
mod sqlite_model_tool_queue;
mod sqlite_projection;
mod sqlite_recovery;
mod sqlite_retrieval_publication;
mod sqlite_scheduler;

use async_trait::async_trait;

use crate::engine::{TransitionKey, TransitionOutcome};

pub use activation::{
    ActivationAdmissionCommand, ActivationCasCommand, ActivationCommitReceipt,
    ActivationProjection, ActivationTimerKind, AttemptCompletion, AttemptCompletionAuthority,
    CommittedTerminalActivationAuthority, CommittedTerminalActivationProof,
    CommittedTerminalActivationResult, CompleteAttemptCommand, FencedAttemptCommand,
    FireTimerCommand, GrantAttemptLeaseCommand, HeartbeatAttemptCommand, LeaseGrantAuthority,
    ReceiveSignalCommand, RecordEffectEvidenceCommand, RegisterWaitCommand, ResolveSignalCommand,
    RetryScheduleAuthority, ScheduleActivationTimerCommand, SignalReceipt, TaskClaim,
    TaskDispatchSpec, TaskEnvelope, TimerFireAuthority, WaitResolutionAuthority,
};
pub use artifact::{
    AcknowledgeArtifactDeletionCommand, ArtifactDeletionClaim, ArtifactDurableRepository,
    ArtifactReceipt, ArtifactReferenceAuthority, ArtifactReferenceTarget, ArtifactRetentionRelease,
    ArtifactState, ArtifactStoreAuthority, BindArtifactStoreAuthorityCommand, OrphanSweepBatch,
    OrphanSweepCommand, PayloadId, PayloadReceipt, PutInlinePayloadCommand,
    ReferenceArtifactCommand, ReleaseRunArtifactRetentionCommand, RetainedArtifact,
    StageArtifactCommand, StorageLocator, StoredInlinePayload, VerifyArtifactCommand,
};
pub use control_repository::{
    ClaimSchedulerRunCommand, CloseScopeAdmissionCommand, ConsumeControlTokenCommand,
    ControlCommitReceipt, ControlDurableRepository, CreateChildScopeCommand, CreateForkCommand,
    CreateReuseCandidateCommand, EmitControlTokenCommand, FencedSchedulerRunCommand,
    ForkLegAdmission, HeartbeatSchedulerRunCommand, JoinArrivalReceipt, JoinBarrierAuthority,
    MaterializeReuseCandidateCommand, RecordJoinArrivalCommand, RejectReuseCandidateCommand,
    ReuseCompatibility, RevokeControlTokenCommand, SchedulerLeaseRepository, SchedulerRunLease,
    SettleScopeCommand, TokenConsumerKind,
};
#[allow(unused_imports)]
pub use error::{
    RepositoryError, REPOSITORY_ACTIVATION_NOT_FOUND, REPOSITORY_ARTIFACT_STORE_CONFLICT,
    REPOSITORY_CANONICALIZATION_FAILED, REPOSITORY_CONFIGURATION_INVALID,
    REPOSITORY_CONSTRAINT_CONFLICT, REPOSITORY_DATA_INVALID, REPOSITORY_INTENT_CONFLICT,
    REPOSITORY_MIGRATION_FAILED, REPOSITORY_PLAN_CONFLICT, REPOSITORY_REDRIVE_REQUIRES_FORK,
    REPOSITORY_RUN_MIGRATING, REPOSITORY_RUN_NOT_FOUND, REPOSITORY_SCHEDULER_ACTION_UNSUPPORTED,
    REPOSITORY_SCHEDULER_CRASH_INJECTED, REPOSITORY_STORAGE_FAILURE,
};
pub use human_task::{
    ClaimHumanWorkItemCommand, CompleteHumanWorkItemCommand, HumanTaskDurableRepository,
    HumanTaskPrincipal, HumanWorkItem, HumanWorkItemClaim, HumanWorkItemCompletionAuthority,
    HumanWorkItemId, HumanWorkItemState,
};
pub use ingress::{
    DueTimer, ExistingSignalSubmission, PendingSignalResolution, RuntimeIngressDurableRepository,
    SignalInboxState, SignalWaitTarget,
};
pub use model::{
    CommitReceipt, CreateRunCommand, DurableResponseSnapshot, PlanInstallOutcome,
    PlanPublicationOutcome, PublicEventIntent, PublicRunAttachment, PublicationHead,
    PublicationOrigin, PublishVersionedPlanCommand, ResponseTerminalKind, ResponseUsageStatus,
    RunProjection, RunTransitionCommand, VersionedPlan, VersionedPlanCatalog,
};
pub use model_tool_parent_resume::ModelToolParentResume;
#[cfg(test)]
pub(crate) use model_tool_queue::{
    deterministic_tool_identity, parse_action_from_stored_evidence, StoredModelToolActionEvidence,
};
pub use model_tool_queue::{
    FrozenModelToolAction, ModelToolBatchActivation, ModelToolBatchActivationOutcome,
    ModelToolContinuationStatus, ModelToolFailureClass, ModelToolTaskClaim,
    ModelToolTaskCommitReceipt, ModelToolTaskDisposition, ModelToolTaskHeartbeatOutcome,
    ModelToolTaskIdentity, ModelToolTaskOutcome, ModelToolTaskStatus,
    ModelToolTaskTransitionOutcome, FUNCTION_CALL_COMPLETE_SEAL_INDEX, MAX_MODEL_TOOL_RESULT_BYTES,
};
#[allow(unused_imports)]
pub use postgres::PostgresDurableRepository;
pub use projection::{
    ProjectionAudit, ProjectionDurableRepository, ProjectionRebuildSnapshot,
    ProjectionRepairReceipt, ProjectionSubject, ProjectionSubjectKind,
};
pub use public_outbox::{
    OrderedPublicEventRead, PublicEventClaim, PublicEventNotificationStream,
    PublicEventOutboxRepository, PublicEventPosition, PublishedPublicEvent,
    PUBLIC_EVENT_NOTIFY_CHANNEL,
};
pub use recovery_repository::{
    BeginMigrationCommand, ContinueAsNewCommand, FinalizeMigrationCommand, ForkRunCommand,
    MigrationIntentReceipt, MigrationMappingCompatibility, RecoveryDurableRepository,
    RecoveryEventReceipt, RecoveryRevisionSpec, RecoveryRunReceipt, RedriveRunCommand,
};
pub use scheduler_repository::{
    DurableTaskExecutionRequest, FailOnceSchedulerCrash, ModelToolCallCheckpoint, NoSchedulerCrash,
    SchedulerCommitReceipt, SchedulerCrashInjector, SchedulerCrashPoint,
    SchedulerDurableRepository, SchedulerFailureDisposition, SchedulerStoredValue,
    SchedulerTaskClaim, SchedulerTaskClaimMode, SchedulerTaskCommitOutcome,
    SchedulerTaskCompletionReceipt, SchedulerTaskFailure, SchedulerTaskHeartbeatOutcome,
    SchedulerTaskOutcome, SchedulerTaskSuccess, SCHEDULER_CHECKPOINT_SCHEMA_VERSION,
    SCHEDULER_TASK_ENVELOPE_SCHEMA_VERSION,
};
pub use scheduler_runtime::{
    consume_model_tool_task_once, consume_model_tool_task_once_with_observer,
    consume_scheduler_task_once, consume_scheduler_task_once_with_artifact_store,
    consume_scheduler_task_once_with_artifact_store_and_retrieval_observer,
    consume_scheduler_task_once_with_retrieval_observer, drive_scheduler_once,
    drive_scheduler_until_quiescent, FrozenSchedulerWorkerFailurePolicy, ModelToolLiveObserver,
    ModelToolWorkerPumpOutcome, ModelToolWorkerRepository, NoopModelToolLiveObserver,
    NoopSchedulerRetrievalLiveObserver, SchedulerDriveOutcome, SchedulerRecoveryOutcome,
    SchedulerRetrievalLiveObserver, SchedulerWorkerFailurePolicy, SchedulerWorkerPumpOutcome,
    TerminalSchedulerWorkerFailurePolicy,
};
#[allow(unused_imports)]
pub use sqlite::SqliteDurableRepository;

#[async_trait]
pub trait DurableRepository: Send + Sync {
    async fn install_versioned_plan(
        &self,
        plan: &VersionedPlan,
    ) -> Result<PlanInstallOutcome, RepositoryError>;

    /// Atomically installs immutable deployment rows and advances the durable
    /// public route after locking and comparing every direct subflow route
    /// used to build the deployment. This is the publication boundary;
    /// archive installation must never change a route head.
    async fn publish_versioned_plan(
        &self,
        command: PublishVersionedPlanCommand,
    ) -> Result<PlanPublicationOutcome, RepositoryError>;

    /// Installs an immutable built-in deployment and atomically creates or
    /// advances a built-in route head. An explicit Graph publication head is
    /// never overwritten; the returned head is the durable authority after
    /// the transaction.
    async fn publish_builtin_versioned_plan(
        &self,
        plan: &VersionedPlan,
    ) -> Result<PublicationHead, RepositoryError>;

    /// Loads all immutable deployments and their explicit public route heads
    /// from one repository snapshot. Callers must not infer current routing
    /// from row order or timestamps.
    async fn load_versioned_plan_catalog(&self) -> Result<VersionedPlanCatalog, RepositoryError>;

    async fn create_run(
        &self,
        transition_key: TransitionKey,
        command: CreateRunCommand,
    ) -> Result<TransitionOutcome<CommitReceipt>, RepositoryError>;

    async fn commit_run_transition(
        &self,
        transition_key: TransitionKey,
        command: RunTransitionCommand,
    ) -> Result<TransitionOutcome<CommitReceipt>, RepositoryError>;

    async fn load_run(
        &self,
        run_id: &crate::engine::RunId,
    ) -> Result<Option<RunProjection>, RepositoryError>;

    async fn load_response_snapshot(
        &self,
        _run_id: &crate::engine::RunId,
    ) -> Result<Option<DurableResponseSnapshot>, RepositoryError> {
        Ok(None)
    }
}

#[async_trait]
pub trait ActivationDurableRepository: DurableRepository + ProjectionDurableRepository {
    async fn admit_activation(
        &self,
        transition_key: TransitionKey,
        command: ActivationAdmissionCommand,
    ) -> Result<TransitionOutcome<ActivationCommitReceipt>, RepositoryError>;

    async fn make_activation_ready(
        &self,
        transition_key: TransitionKey,
        command: ActivationCasCommand,
    ) -> Result<TransitionOutcome<ActivationCommitReceipt>, RepositoryError>;

    async fn grant_attempt_lease(
        &self,
        transition_key: TransitionKey,
        command: GrantAttemptLeaseCommand,
    ) -> Result<TransitionOutcome<LeaseGrantAuthority>, RepositoryError>;

    async fn mark_attempt_running(
        &self,
        transition_key: TransitionKey,
        command: FencedAttemptCommand,
    ) -> Result<TransitionOutcome<ActivationCommitReceipt>, RepositoryError>;

    async fn heartbeat_attempt(
        &self,
        command: HeartbeatAttemptCommand,
    ) -> Result<TransitionOutcome<()>, RepositoryError>;

    async fn record_effect_evidence(
        &self,
        transition_key: TransitionKey,
        command: RecordEffectEvidenceCommand,
    ) -> Result<TransitionOutcome<ActivationCommitReceipt>, RepositoryError>;

    async fn complete_attempt(
        &self,
        transition_key: TransitionKey,
        command: CompleteAttemptCommand,
    ) -> Result<TransitionOutcome<AttemptCompletionAuthority>, RepositoryError>;

    async fn register_wait(
        &self,
        transition_key: TransitionKey,
        command: RegisterWaitCommand,
    ) -> Result<TransitionOutcome<ActivationCommitReceipt>, RepositoryError>;

    async fn schedule_activation_timer(
        &self,
        transition_key: TransitionKey,
        command: ScheduleActivationTimerCommand,
    ) -> Result<TransitionOutcome<crate::engine::TimerId>, RepositoryError>;

    async fn fire_timer(
        &self,
        transition_key: TransitionKey,
        command: FireTimerCommand,
    ) -> Result<TransitionOutcome<TimerFireAuthority>, RepositoryError>;

    async fn cancel_timer(
        &self,
        run_id: &crate::engine::RunId,
        timer_id: &crate::engine::TimerId,
    ) -> Result<bool, RepositoryError>;

    async fn receive_signal(
        &self,
        command: ReceiveSignalCommand,
    ) -> Result<TransitionOutcome<SignalReceipt>, RepositoryError>;

    async fn resolve_wait_signal(
        &self,
        transition_key: TransitionKey,
        command: ResolveSignalCommand,
    ) -> Result<TransitionOutcome<WaitResolutionAuthority>, RepositoryError>;

    async fn claim_task_outbox(
        &self,
        claimant: &str,
        claim_seconds: u32,
        limit: u32,
    ) -> Result<Vec<TaskClaim>, RepositoryError>;

    async fn mark_task_published(&self, claim: &TaskClaim) -> Result<bool, RepositoryError>;

    async fn ack_task(&self, claim: &TaskClaim) -> Result<bool, RepositoryError>;

    async fn load_activation(
        &self,
        run_id: &crate::engine::RunId,
        activation_id: &crate::engine::ActivationId,
    ) -> Result<Option<ActivationProjection>, RepositoryError>;
}
