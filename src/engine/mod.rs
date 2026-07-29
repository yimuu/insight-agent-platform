//! Compatibility facade for the durable graph execution surface.
//!
//! Authoritative contracts and adapters are owned by the layered workspace
//! crates; this module only rebuilds the frozen root paths from those types.

pub mod artifact_store {
    pub use insight_storage::artifact_store::*;
}
pub mod leaf_adapters;
pub mod repository;
pub mod retrieval_adapter;
pub mod worker;

pub use insight_engine::{
    aggregate, control, error, event, identity, persistence, plan, recovery, run, scheduler, scope,
    state, value,
};

pub mod retrieval {
    pub use insight_engine::retrieval::{
        deterministic_retrieval_id, FrozenRetrievalTarget, RetrievalCompletion,
        RETRIEVAL_BINDING_INVALID, RETRIEVAL_COMPLETION_INVALID,
    };
}

pub use aggregate::{
    ActivationAttemptAggregate, AttemptFailureDisposition, CommittedOutputProof, ExecutionKind,
    LeaseExpiryProof, LeaseGrantProof, RetryScheduleProof, RetryTimerProof,
    TerminalActivationProof, TerminalActivationResult, TerminationProgress, WaitResolutionProof,
    WaitResolutionSubject, WorkerCancellation, WorkerEffectClass, WorkerEffectPolicy,
    WorkerExecutionPolicy, AGGREGATE_COMMAND_INTENT_SCHEMA_VERSION, TERMINAL_PROOF_ATTEMPT_LIVE,
    TERMINAL_PROOF_NOT_TERMINAL,
};
pub use artifact_store::{
    ArtifactStoreDeploymentCapability, ArtifactStoreDeploymentContract,
    LocalContentAddressedArtifactStore, TenantArtifactEncryptionKeyring, WorkerArtifactStore,
};
pub use control::{
    ApplyOutcome, BranchCorrelation, BranchDecision, ChildRequirement, ChildSettlement,
    ControlToken, ControlTokenProvenance, ForkGroup, ForkLeg, ForkLegCorrelation, JoinArrival,
    JoinFailure, JoinMode, JoinState, JoinStatus, LegSettlement, LegSettlementClass, MergeArrival,
    MergeState, ScopeTracker, ScopeTrackerState, SettlementHandling, TrackedChild,
};
pub use error::ModelError;
pub use event::{
    EventSeq, ExecutionControlFrame, ExecutionEventContext, ExecutionEventEnvelope,
    ExecutionEventId, ExecutionEventKind, ExecutionEventPayload, ExecutionForkLeg,
    ExecutionJoinMode, ExecutionLegSettlementClass, ExecutionScopeKind, ExecutionTransitionIntent,
    ExecutionValueSummary, IntentHash, InternalFailureCode, InternalFailureKind,
    InternalFailureSummary, PendingExecutionEvent, ProjectionMutationKind, PublicErrorCode,
    PublicEventContext, PublicEventEnvelope, PublicEventId, PublicEventKind, PublicEventPayload,
    PublicFailureKind, PublicFailureSummary, SignalId, TimerId, TransitionIdentity, TransitionKey,
    TransitionLedger, TransitionOutcome, TransitionPrecondition, TransitionRequest,
    EXECUTION_EVENT_SCHEMA_VERSION, EXECUTION_TRANSITION_INTENT_SCHEMA_VERSION,
    PUBLIC_EVENT_SCHEMA_VERSION, TRANSITION_INTENT_CONFLICT,
};
pub use identity::{
    ActivationId, ArtifactId, AttemptNo, ContentHash, ControlTokenId, DefinitionRevisionId,
    DeploymentRevisionId, DynamicKey, EffectId, ForkGroupId, Generation, LeaseEpoch, LegId, NodeId,
    PortId, RunId, ScopeInstanceId,
};
pub use leaf_adapters::{
    production_worker_registry, production_worker_registry_with_leaf_adapters,
    production_worker_registry_with_live_run_stream, ActionTaskExecutor, LlmTaskExecutor,
    LlmTokenObservation,
};
pub use persistence::PersistenceMode;
pub use plan::{
    BranchCaseId, ControlEdge, ControlEdgeId, ControlPort, ControlPortId, ControlRoute,
    DataBinding, DataBindingId, DataPort, DataPortId, DescriptorConfigurationContract,
    DescriptorContract, DescriptorContractRegistry, DescriptorFieldContract, DescriptorRegistry,
    DescriptorValueSchema, LeafDescriptorRef, LeafTaskKind, LinkedPlan, MergeCorrelation, Node,
    NodeKind, PhiBinding, PhiBindingId, Plan, PlanBuilder, PlanError, PlanIndex, PlanJoinMode,
    PlanMetadata, PlanProperty, PlanType, Policy, PolicyId, PortName, PureExpression, ScopeId,
    ScopeMetadata, SecretRef, SemanticHash, SourceDocumentId, SourceMap, SourceMapPolicy,
    SourceSpan, StableNodeIdGenerator, SubflowContractRegistry, SubflowInterfaceContract,
    SubflowInterfaceRegistry, ValueSource, VersionTag, WorkerContract, WorkerInputPortContract,
    CEL_EXPRESSION_ENGINE_VERSION, DSL_MAJOR_VERSION, LITERAL_EXPRESSION_ENGINE_VERSION,
    MAX_PLAN_JSON_BYTES, PLAN_SEMANTIC_PROJECTION_VERSION, PLAN_WIRE_VERSION,
};
pub use recovery::{
    decide_redrive_effect, ExecutionRevisionPin, MigrationNodeMapping, MigrationReadiness,
    RedriveEffectDecision, ReusableNodeClass, ReuseCandidate, ReuseEvidence, ReuseMaterialization,
    ReuseRejection, ReuseTargetContract, RunLineage, RunLineageKind, RECOVERY_LINEAGE_INVALID,
    RECOVERY_MATERIALIZATION_MISMATCH, RECOVERY_MIGRATION_NOT_READY,
    RECOVERY_MIGRATION_SCHEMA_INCOMPATIBLE, RECOVERY_REUSE_INELIGIBLE, RECOVERY_REVISION_MISMATCH,
};
pub use retrieval::{
    deterministic_retrieval_id, FrozenRetrievalTarget, RetrievalCompletion,
    RETRIEVAL_BINDING_INVALID, RETRIEVAL_COMPLETION_INVALID,
};
pub use retrieval_adapter::{
    install_retrieval_workers, production_worker_registry_with_live_run_stream_and_retrievals,
    production_worker_registry_with_retrievals, RetrievalTaskExecutor,
};
pub use run::{
    RunExecutionAggregate, RunTerminationProgress, RunWorkProjection,
    RUN_AGGREGATE_INTENT_SCHEMA_VERSION,
};
pub use scheduler::{
    BoundTaskInput, DeterministicIds, LogicalOccurrence, NativeOutput, PlannedSchedulerAction,
    RunTerminalFact, RuntimeValue, SafeError, SchedulerAction, SchedulerCheckpointId,
    SchedulerDecision, SchedulerError, SchedulerFacts, SchedulerIntent, SchedulerPlanner,
    SchedulerPlanningFailure, SchedulerPlanningFailureKind, SchedulerPrecondition,
    SchedulerQuiescence, SchedulerTaskId, SchedulerTaskKind, SchedulerWaitId,
    StructuralOutcomeFact, TaskAdmissionClass, TaskOutputContract, SCHEDULER_EXPRESSION_INVALID,
    SCHEDULER_FACT_INCONSISTENT, SCHEDULER_FACT_MISSING, SCHEDULER_GRAPH_INVALID,
    SCHEDULER_ID_INVALID, SCHEDULER_INTENT_SCHEMA_VERSION, SCHEDULER_VALUE_TYPE_MISMATCH,
};
pub use scope::{MapItemIdentity, ScopeInstance, ScopeKind};
pub use state::{
    ActivationLifecycle, ActivationState, ActivationTerminationReason, AdmissionState,
    AttemptLifecycle, AttemptState, CompletionDisposition, EffectEvidence, EffectIdempotency,
    LeaseExpiryDisposition, LeaseFence, RunLifecycle, RunState, TerminationClaim,
    TerminationIntent, TerminationReason,
};
pub use value::{ArtifactRef, InlineValueRef, ValueRef};
pub use worker::{
    LeafTaskExecutor, ModelCallAuthority, ModelCallCompletion, ModelContinuationTurn,
    ModelFinishReason, ModelFunctionCallPublication, ModelIncompleteFunctionCallPublication,
    ModelTokenUsage, ModelToolCall, ModelToolCallBatch, ModelToolResult, ResponseItemAuthority,
    TaskExecutionRequest, TaskExecutionResult, WorkerExecutionContext, WorkerExecutorRegistry,
    WorkerFailure, WorkerFailureClass, WORKER_EXECUTION_CONTEXT_INVALID, WORKER_FAILURE_INVALID,
    WORKER_IMPLEMENTATION_NOT_FOUND, WORKER_MODEL_TOOL_CLAIM_INVALID, WORKER_OUTPUT_INVALID,
    WORKER_TASK_KIND_MISMATCH,
};
