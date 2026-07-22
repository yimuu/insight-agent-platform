//! Compatibility facade for the engine-owned worker contract.

pub use insight_engine::worker::{
    LeafTaskExecutor, ModelCallAuthority, ModelCallCompletion, ModelContinuationTurn,
    ModelFinishReason, ModelFunctionCallPublication, ModelIncompleteFunctionCallPublication,
    ModelTokenUsage, ModelToolCall, ModelToolCallBatch, ModelToolResult, ResponseItemAuthority,
    TaskExecutionOrigin, TaskExecutionRequest, TaskExecutionResult, WorkerExecutionContext,
    WorkerExecutorRegistry, WorkerFailure, WorkerFailureClass, WorkerRuntimeServices,
    WORKER_EXECUTION_CONTEXT_INVALID, WORKER_FAILURE_INVALID, WORKER_IMPLEMENTATION_NOT_FOUND,
    WORKER_MODEL_TOOL_CLAIM_INVALID, WORKER_OUTPUT_INVALID, WORKER_TASK_KIND_MISMATCH,
};
