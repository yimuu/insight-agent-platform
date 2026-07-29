//! Runtime orchestration and production adapters.

pub mod catalog;
pub mod control;
#[doc(hidden)]
pub mod internal;
pub mod leaf_adapters;
pub mod retrieval_adapter;
pub mod run_service;
pub mod run_stream;
pub mod scheduler_runtime;
pub mod terminal_execution;
pub mod terminal_only;

pub use control::{stop_pair, ExecutionControl, StopController, StopReason, StopSignal};
pub use insight_durable::terminal_store::{
    Conversation, ConversationContent, ConversationMessage, ConversationMessagePage,
    ConversationRole, MessageCursor,
};
pub use insight_engine::execution::{RunError, RunErrorKind};
pub use run_service::{
    AnyAttachedRun, AttachedRun, ConversationResponseVisibilityGuard, ConversationStreamDelivery,
    ConversationStreamPrivacy, DeployedAgentCatalog, ForkRecoveryOptions,
    FullConversationVisibilityGuard, GraphPublication, MigrationNodeMappingRequest,
    ProductionRunRepository, PublicArtifact, RecoveryOperation, RecoveryRequestMetadata,
    RecoveryReusePolicy, RecoveryRunResult, RequestMetadata, RunRepositoryCapability, RunService,
    RunServiceConfig, RunSubscription, ServiceError, SubscriptionError, WorkCoordinatorConfig,
};
pub use run_stream::{
    CompletedFunctionCallPublication, CompletedFunctionCallTailPublication,
    InMemoryLiveRunStreamBroker, LiveRunObservationIdentity, LiveRunStreamBroker,
    LiveRunStreamBrokerCapability, LiveRunStreamBrokerError, LiveRunStreamByteLimits,
    LiveRunStreamCloseOutcome, LiveRunStreamDelivery, LiveRunStreamGap, LiveRunStreamItemIdentity,
    LiveRunStreamPayload, LiveRunStreamPublication, LiveRunStreamPublishOutcome, LiveRunStreamSeal,
    LiveRunStreamSealStatus, LiveRunStreamSourceIdentity, LiveRunStreamSubscriber,
    RunCompletedSnapshot, RunFailedSnapshot, RunInitialSnapshot, RunObjectKind,
    RunOutputContentPart, RunOutputItem, RunOutputItemStatus, RunOutputRole, RunPublicError,
    RunPublicResultError, RunRetrieval, RunRetrievalMetadata, RunRetrievalPublicProjection,
    RunRetrievalResult, RunStatus, RunStoppedSnapshot, RunStreamEvent, RunStreamEventType,
    RunStreamGapAction, RunToolCompletedArgumentsProjection, RunToolContent,
    RunToolPublicProjection, RunToolResult, RunUsage, RunUsageInputDetails, RunUsageOutputDetails,
    RunUsageStatus, MAX_FUNCTION_CALL_ARGUMENT_BYTES, RUN_STREAM_PROTOCOL_VERSION,
};
pub use terminal_execution::{
    execute_terminal_plan, TerminalExecutionConfig, TerminalExecutionError,
    TerminalExecutionOutcome, TerminalFailureKind,
};
pub use terminal_only::{
    ConversationAttachedTurn, ConversationDetachedTurn, ConversationMessagePageView,
    ConversationMessageView, ConversationVisibilityGuard, RecoveryCapability,
    RunPersistenceCapability, TerminalAttachedRun, TerminalOnlyRunConfig, TerminalOnlyRunEngine,
    TerminalOnlyStore, TerminalRunSubscription,
};
