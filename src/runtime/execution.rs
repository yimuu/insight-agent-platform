use std::{sync::Arc, time::Instant};

use chrono::Utc;
use serde_json::json;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::sleep,
};
use tokio_util::sync::CancellationToken;

use crate::{
    dsl::{
        compiled::{CompiledNode, NodeControl, NodeTransition},
        EmitPolicy,
    },
    events::{
        hub::{EventError, EventHub},
        protocol::{RunEventScope, RunEventType},
    },
    history::types::NodeOutputRecord,
    nodes::registry::NodeExecutorRegistry,
    observability::{elapsed_ms, json_size_bytes},
    outcome::TerminalOutcome,
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
    let started = Instant::now();
    let node_id = node.id.clone();
    let node_kind = node.kind.clone();
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
            tracing::info!(
                event_name = "node.failed",
                run_id = context.metadata().run_id.as_str(),
                request_id = context.metadata().request_id.as_str(),
                agent_id = context.metadata().agent_id.as_str(),
                agent_version = context.metadata().agent_version.as_str(),
                node_id = node_id.as_str(),
                kind = node_kind.as_str(),
                elapsed_ms = elapsed_ms(started),
                error_code = error.code(),
                error_kind = run_error_kind(error.kind()),
                "node failed"
            );
            return Err(classify_failure(&node_id, error));
        }
    };
    validate_control_transition_contract(&node_id, &node.control, &outcome.transition)
        .map_err(NodeExecutionFailure::Infrastructure)?;
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
    let terminal_outcome = match &outcome.transition {
        NodeTransition::End(TerminalOutcome::Success { .. }) => "success",
        NodeTransition::End(TerminalOutcome::Failure { .. }) => "failure",
        _ => "",
    };
    tracing::info!(
        event_name = "node.completed",
        run_id = context.metadata().run_id.as_str(),
        request_id = context.metadata().request_id.as_str(),
        agent_id = context.metadata().agent_id.as_str(),
        agent_version = context.metadata().agent_version.as_str(),
        node_id = node_id.as_str(),
        kind = node_kind.as_str(),
        terminal_outcome,
        elapsed_ms = elapsed_ms(started),
        output_bytes = json_size_bytes(&outcome.output),
        "node completed"
    );

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
        RunErrorKind::Node | RunErrorKind::Timeout => NodeExecutionFailure::Node {
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

pub(super) fn validate_control_transition_contract(
    node_id: &str,
    control: &NodeControl,
    transition: &NodeTransition,
) -> Result<(), RunError> {
    let control_outcome = match control {
        NodeControl::End { outcome } | NodeControl::BranchEnd { outcome } => Some(*outcome),
        _ => None,
    };
    let transition_outcome = match transition {
        NodeTransition::End(outcome) => Some(outcome.kind()),
        _ => None,
    };

    if control_outcome != transition_outcome {
        return Err(RunError::infrastructure(
            "SCHEDULER_INVARIANT_VIOLATION",
            format!("node '{node_id}' terminal control and executor transition disagree"),
        ));
    }
    Ok(())
}

fn run_error_kind(kind: RunErrorKind) -> &'static str {
    match kind {
        RunErrorKind::Node => "node",
        RunErrorKind::Timeout => "timeout",
        RunErrorKind::Stop => "stop",
        RunErrorKind::Infrastructure => "infrastructure",
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
