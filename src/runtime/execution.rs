use std::sync::Arc;

use chrono::Utc;
use serde_json::json;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::sleep,
};
use tokio_util::sync::CancellationToken;

use crate::{
    dsl::{compiled::CompiledNode, EmitPolicy},
    events::{
        hub::{EventError, EventHub},
        protocol::{RunEventScope, RunEventType},
    },
    history::types::NodeOutputRecord,
    nodes::registry::NodeExecutorRegistry,
};

use super::{ExecutionControl, RunContext, RunError, RunErrorKind, StopReason, StopSignal};

#[derive(Debug)]
pub enum NodeExecutionFailure {
    Node { node_id: String, error: RunError },
    Stop { node_id: String, error: RunError },
    Infrastructure(RunError),
}

#[derive(Debug)]
pub struct NodeExecutionResult {
    pub node_id: String,
    pub context: RunContext,
    pub outcome: crate::dsl::compiled::NodeOutcome,
}

#[derive(Clone)]
pub struct ExecutionLimiter {
    global: Arc<Semaphore>,
    per_run: Arc<Semaphore>,
}

struct ExecutionPermits {
    _per_run: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
}

impl ExecutionLimiter {
    pub fn new(global: Arc<Semaphore>, per_run: Arc<Semaphore>) -> Self {
        Self { global, per_run }
    }

    async fn acquire(&self, stop: &StopSignal) -> Result<ExecutionPermits, RunError> {
        let per_run = acquire_permit(Arc::clone(&self.per_run), stop, "per-Run").await?;
        let global = acquire_permit(Arc::clone(&self.global), stop, "process").await?;
        Ok(ExecutionPermits {
            _per_run: per_run,
            _global: global,
        })
    }
}

pub async fn execute_node(
    node: CompiledNode,
    context: RunContext,
    executors: NodeExecutorRegistry,
    events: EventHub,
    stop: StopSignal,
    limiter: ExecutionLimiter,
) -> Result<NodeExecutionResult, NodeExecutionFailure> {
    execute_node_with_cancellation(
        node,
        context,
        executors,
        events,
        stop,
        CancellationToken::new(),
        limiter,
    )
    .await
}

pub(crate) async fn execute_node_with_cancellation(
    node: CompiledNode,
    context: RunContext,
    executors: NodeExecutorRegistry,
    events: EventHub,
    stop: StopSignal,
    task_cancel: CancellationToken,
    limiter: ExecutionLimiter,
) -> Result<NodeExecutionResult, NodeExecutionFailure> {
    let execution = execute_node_inner(node, context, executors, events, stop, limiter);
    tokio::pin!(execution);
    tokio::select! {
        biased;
        result = &mut execution => result,
        _ = task_cancel.cancelled() => Err(NodeExecutionFailure::Infrastructure(
            global_cancellation_error()
        )),
    }
}

async fn execute_node_inner(
    node: CompiledNode,
    context: RunContext,
    executors: NodeExecutorRegistry,
    events: EventHub,
    stop: StopSignal,
    limiter: ExecutionLimiter,
) -> Result<NodeExecutionResult, NodeExecutionFailure> {
    let node_id = node.id.clone();
    let _permits = limiter
        .acquire(&stop)
        .await
        .map_err(|error| classify_failure(&node_id, error))?;

    let node_scope = scope_for(&context).for_node(&node_id);
    events
        .publish(
            node_scope.clone(),
            RunEventType::NodeStarted,
            json!({"type":node.kind}),
        )
        .await
        .map_err(|error| NodeExecutionFailure::Infrastructure(event_error(error)))?;

    let executor = executors
        .resolve(&node.kind)
        .map_err(NodeExecutionFailure::Infrastructure)?;
    let emitter_events = events.clone();
    let emitter_scope = node_scope.clone();
    let control = ExecutionControl::new(stop, node.timeout, move |content| {
        let events = emitter_events.clone();
        let scope = emitter_scope.clone();
        async move {
            events
                .publish(
                    scope,
                    RunEventType::ContentDelta,
                    json!({"content":content}),
                )
                .await
                .map(|_| ())
                .map_err(event_error)
        }
    })
    .with_content_enabled(node.emit == EmitPolicy::Content);

    let outcome = {
        let execution = executor.execute(&node, &context, &control);
        tokio::pin!(execution);
        tokio::select! {
            biased;
            result = &mut execution => result,
            _ = control.stopped() => Err(stopped_error(&control)),
            _ = sleep(control.remaining()) => Err(RunError::timeout()),
        }
    };
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(error) if error.kind() == RunErrorKind::Infrastructure => {
            return Err(NodeExecutionFailure::Infrastructure(error));
        }
        Err(error) => {
            let error = normalize_execution_error(&control, error)
                .map_err(NodeExecutionFailure::Infrastructure)?;
            events
                .publish_error(
                    node_scope,
                    RunEventType::NodeFailed,
                    error.code(),
                    error.message(),
                    json!({}),
                )
                .await
                .map_err(|event| NodeExecutionFailure::Infrastructure(event_error(event)))?;
            return Err(classify_failure(&node_id, error));
        }
    };
    events
        .put_node_output(NodeOutputRecord {
            run_id: context.metadata().run_id.clone(),
            node_id: node_id.clone(),
            output: outcome.output.clone(),
            completed_at: Utc::now(),
        })
        .await
        .map_err(|error| NodeExecutionFailure::Infrastructure(event_error(error)))?;
    events
        .publish(
            node_scope,
            RunEventType::NodeCompleted,
            json!({"output":outcome.output}),
        )
        .await
        .map_err(|error| NodeExecutionFailure::Infrastructure(event_error(error)))?;

    Ok(NodeExecutionResult {
        node_id,
        context,
        outcome,
    })
}

async fn acquire_permit(
    semaphore: Arc<Semaphore>,
    stop: &StopSignal,
    capacity: &'static str,
) -> Result<OwnedSemaphorePermit, RunError> {
    tokio::select! {
        biased;
        _ = stop.stopped() => Err(stopped_error_from_signal(stop)),
        permit = semaphore.acquire_owned() => permit.map_err(|_| {
            RunError::infrastructure(
                "NODE_CAPACITY_CLOSED",
                format!("{capacity} node execution capacity is closed"),
            )
        }),
    }
}

fn scope_for(context: &RunContext) -> RunEventScope {
    let metadata = context.metadata();
    RunEventScope::for_run(
        &metadata.request_id,
        &metadata.run_id,
        &metadata.agent_id,
        &metadata.agent_version,
    )
}

fn classify_failure(node_id: &str, error: RunError) -> NodeExecutionFailure {
    match error.kind() {
        RunErrorKind::Node => NodeExecutionFailure::Node {
            node_id: node_id.to_string(),
            error,
        },
        RunErrorKind::Stop => NodeExecutionFailure::Stop {
            node_id: node_id.to_string(),
            error,
        },
        RunErrorKind::Infrastructure => NodeExecutionFailure::Infrastructure(error),
    }
}

fn normalize_execution_error(
    control: &ExecutionControl,
    error: RunError,
) -> Result<RunError, RunError> {
    if error.kind() != RunErrorKind::Stop {
        return Ok(error);
    }

    match control.stop_reason() {
        Some(reason) => Ok(RunError::stopped(reason)),
        None => Err(unbacked_stop_error()),
    }
}

fn unbacked_stop_error() -> RunError {
    RunError::infrastructure(
        "UNBACKED_STOP",
        "node returned a stop error without a runtime stop signal",
    )
}

fn stopped_error(control: &ExecutionControl) -> RunError {
    control
        .stop_reason()
        .map(RunError::stopped)
        .unwrap_or_else(|| RunError::stopped(StopReason::Interrupted))
}

fn stopped_error_from_signal(stop: &StopSignal) -> RunError {
    stop.reason()
        .map(RunError::stopped)
        .unwrap_or_else(|| RunError::stopped(StopReason::Interrupted))
}

fn event_error(error: EventError) -> RunError {
    RunError::infrastructure(error.code(), error.to_string())
}

fn global_cancellation_error() -> RunError {
    RunError::infrastructure("INFRASTRUCTURE_FAILURE", "runtime infrastructure failed")
}
