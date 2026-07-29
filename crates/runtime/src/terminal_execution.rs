//! In-process execution loop used by the terminal-only runtime.
//!
//! The loop consumes the engine's deterministic planner and
//! [`insight_engine::TerminalSchedulerState`]. It never receives a durable
//! repository, recovery fence, task lease, or checkpoint callback.

use std::{
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use futures::FutureExt;
use insight_durable::model_tool_queue::adapter as model_tool_adapter;
use insight_engine::{
    response::{
        CompletedFunctionCallTailPublication, LiveResponseBroker, LiveResponseItemIdentity,
        LiveResponsePayload, LiveResponsePublication, LiveResponsePublishOutcome,
        LiveWorkflowObservationIdentity, WorkflowPublicError, WorkflowToolPublicProjection,
        WorkflowToolResult,
    },
    worker::{
        adapter as worker_adapter, ModelCallAuthority, ModelContinuationTurn,
        ModelToolActionExecutionSpec, ModelToolCallBatch, ModelToolExecutionSpec, ModelToolResult,
        ResponseItemAuthority, TaskExecutionRequest, TaskExecutionResult, WorkerExecutionContext,
        WorkerExecutorRegistry, WorkerFailure, WorkerFailureClass, WorkerRuntimeServices,
    },
    AttemptNo, ContentHash, EffectEvidence, EffectId, LeaseEpoch, PersistenceMode, RunId,
    RunTerminalFact, RuntimeValue, SchedulerDecision, SchedulerPlanner, SchedulerQuiescence,
    SchedulerTaskId, TerminalSchedulerApply, TerminalSchedulerState, TerminationReason,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use crate::catalog::DeployedAgent;

const DEFAULT_ACTION_BUDGET: u32 = 100_000;
const DEFAULT_MAX_MODEL_TOOL_CALLS: u32 = 1_024;
const TERMINAL_EXECUTION_INVALID: &str = "TERMINAL_EXECUTION_INVALID";
const TERMINAL_EXECUTOR_PANICKED: &str = "TERMINAL_EXECUTOR_PANICKED";
const TERMINAL_WAIT_UNAVAILABLE: &str = "TERMINAL_WAIT_UNAVAILABLE";
const TERMINAL_SUBFLOW_UNAVAILABLE: &str = "TERMINAL_SUBFLOW_UNAVAILABLE";
const TERMINAL_ACTION_BUDGET_EXHAUSTED: &str = "TERMINAL_ACTION_BUDGET_EXHAUSTED";
const TERMINAL_MODEL_TOOL_INVALID: &str = "TERMINAL_MODEL_TOOL_INVALID";
const TERMINAL_MODEL_TOOL_LIMIT: &str = "TERMINAL_MODEL_TOOL_LIMIT";
const TERMINAL_MODEL_TOOL_PUBLIC_RESULT_INVALID: &str = "MODEL_TOOL_PUBLIC_RESULT_INVALID";
const TERMINAL_MODEL_TOOL_PROGRESS_QUEUE_CAPACITY: usize = 16;
const TERMINAL_MODEL_TOOL_PROGRESS_BURST: u32 = 8;
const TERMINAL_MODEL_TOOL_PROGRESS_WINDOW: Duration = Duration::from_secs(1);
const TERMINAL_MODEL_TOOL_PROGRESS_TOTAL_LIMIT: u32 = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TerminalExecutionConfig {
    pub run_timeout: Duration,
    pub action_budget: u32,
}

impl TerminalExecutionConfig {
    pub fn new(run_timeout: Duration) -> Result<Self, TerminalExecutionError> {
        let config = Self {
            run_timeout,
            action_budget: DEFAULT_ACTION_BUDGET,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn with_action_budget(
        mut self,
        action_budget: u32,
    ) -> Result<Self, TerminalExecutionError> {
        self.action_budget = action_budget;
        self.validate()?;
        Ok(self)
    }

    fn validate(self) -> Result<Self, TerminalExecutionError> {
        if self.run_timeout.is_zero() || self.action_budget == 0 {
            return Err(TerminalExecutionError::infrastructure(
                TERMINAL_EXECUTION_INVALID,
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "terminal_state", rename_all = "snake_case")]
pub enum TerminalExecutionOutcome {
    Succeeded {
        output: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        usage: Option<Value>,
        #[serde(default)]
        tool_results: Vec<WorkflowToolResult>,
    },
    Failed {
        failure_kind: TerminalFailureKind,
        error_code: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        safe_message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        usage: Option<Value>,
        #[serde(default)]
        tool_results: Vec<WorkflowToolResult>,
    },
    Cancelled {
        #[serde(default)]
        tool_results: Vec<WorkflowToolResult>,
    },
    TimedOut {
        #[serde(default)]
        tool_results: Vec<WorkflowToolResult>,
    },
}

impl TerminalExecutionOutcome {
    pub fn terminal_state(&self) -> &'static str {
        match self {
            Self::Succeeded { .. } => "succeeded",
            Self::Failed { .. } => "failed",
            Self::Cancelled { .. } => "cancelled",
            Self::TimedOut { .. } => "timed_out",
        }
    }

    pub fn output(&self) -> Option<&Value> {
        match self {
            Self::Succeeded { output, .. } => Some(output),
            _ => None,
        }
    }

    pub fn usage(&self) -> Option<&Value> {
        match self {
            Self::Succeeded { usage, .. } | Self::Failed { usage, .. } => usage.as_ref(),
            Self::Cancelled { .. } | Self::TimedOut { .. } => None,
        }
    }

    pub fn error_code(&self) -> Option<&str> {
        match self {
            Self::Failed { error_code, .. } => Some(error_code),
            Self::Succeeded { .. } | Self::Cancelled { .. } | Self::TimedOut { .. } => None,
        }
    }

    pub fn tool_results(&self) -> &[WorkflowToolResult] {
        match self {
            Self::Succeeded { tool_results, .. }
            | Self::Failed { tool_results, .. }
            | Self::Cancelled { tool_results }
            | Self::TimedOut { tool_results } => tool_results,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalFailureKind {
    Workflow,
    Operation,
    Timeout,
    Infrastructure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalExecutionError {
    kind: TerminalFailureKind,
    code: &'static str,
}

impl TerminalExecutionError {
    fn workflow(code: &'static str) -> Self {
        Self {
            kind: TerminalFailureKind::Workflow,
            code,
        }
    }

    fn infrastructure(code: &'static str) -> Self {
        Self {
            kind: TerminalFailureKind::Infrastructure,
            code,
        }
    }

    pub fn kind(&self) -> TerminalFailureKind {
        self.kind
    }

    pub fn code(&self) -> &'static str {
        self.code
    }
}

impl std::fmt::Display for TerminalExecutionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code)
    }
}

impl std::error::Error for TerminalExecutionError {}

struct TerminalExecutionObservations {
    live_response_broker: Arc<dyn LiveResponseBroker>,
    tool_results: Vec<WorkflowToolResult>,
    public_output_index: Arc<AtomicU32>,
}

/// Execute one verified deployment entirely in process.
pub async fn execute_terminal_plan(
    agent: Arc<DeployedAgent>,
    workers: Arc<WorkerExecutorRegistry>,
    run_id: RunId,
    response_id: String,
    input: RuntimeValue,
    cancellation: CancellationToken,
    config: TerminalExecutionConfig,
    live_response_broker: Arc<dyn LiveResponseBroker>,
) -> Result<TerminalExecutionOutcome, TerminalExecutionError> {
    if agent.persistence_mode() != PersistenceMode::TerminalOnly {
        return Err(TerminalExecutionError::infrastructure(
            TERMINAL_EXECUTION_INVALID,
        ));
    }
    let config = config.validate()?;
    let deadline = tokio::time::Instant::now() + config.run_timeout;
    let linked = agent
        .linked_plan()
        .map_err(|_| TerminalExecutionError::infrastructure(TERMINAL_EXECUTION_INVALID))?;
    let planner = SchedulerPlanner::new(&linked);
    let mut state =
        TerminalSchedulerState::new(insight_engine::SchedulerFacts::new(run_id, 0, input));
    let mut usage = TerminalUsage::default();
    let mut observations = TerminalExecutionObservations {
        live_response_broker,
        tool_results: Vec::new(),
        public_output_index: Arc::new(AtomicU32::new(0)),
    };

    for _ in 0..config.action_budget {
        if cancellation.is_cancelled() {
            state.request_termination(TerminationReason::Cancelled);
        }
        if tokio::time::Instant::now() >= deadline {
            state.request_termination(TerminationReason::TimedOut);
        }
        state.set_observed_time_ms(unix_time_ms());

        let decision = match planner.plan(state.facts()) {
            Ok(decision) => decision,
            Err(error) => SchedulerDecision::Action(Box::new(
                planner
                    .fail_closed_action(state.facts(), &error)
                    .map_err(|_| {
                        TerminalExecutionError::infrastructure(TERMINAL_EXECUTION_INVALID)
                    })?,
            )),
        };
        match decision {
            SchedulerDecision::Action(action) => {
                match state.apply_planned_action(&action).map_err(|_| {
                    TerminalExecutionError::infrastructure(TERMINAL_EXECUTION_INVALID)
                })? {
                    TerminalSchedulerApply::Applied | TerminalSchedulerApply::ExactReplay => {}
                    TerminalSchedulerApply::Dispatch { request, .. } => {
                        let result = execute_task_with_retries(
                            workers.as_ref(),
                            &request,
                            &response_id,
                            Vec::new(),
                            cancellation.child_token(),
                            deadline,
                            &mut usage,
                            &mut observations,
                            None,
                        )
                        .await;
                        match result {
                            Ok(result) => state.complete_task(&request, &result).map_err(|_| {
                                TerminalExecutionError::infrastructure(TERMINAL_EXECUTION_INVALID)
                            })?,
                            Err(failure) => {
                                state.fail_task(&request, &failure).map_err(|_| {
                                    TerminalExecutionError::infrastructure(
                                        TERMINAL_EXECUTION_INVALID,
                                    )
                                })?;
                            }
                        }
                    }
                }
            }
            SchedulerDecision::Quiescent(quiescence) => match quiescence {
                SchedulerQuiescence::RunSucceeded
                | SchedulerQuiescence::RunFailed
                | SchedulerQuiescence::RunCancelled => {
                    return terminal_outcome(
                        state.terminal(),
                        usage.finish(),
                        observations.tool_results,
                    );
                }
                SchedulerQuiescence::WaitingForWait { wait_id, .. } => {
                    let due_at_ms = state
                        .timer_due_at_ms(&wait_id)
                        .map_err(|_| TerminalExecutionError::workflow(TERMINAL_WAIT_UNAVAILABLE))?;
                    let delay = Duration::from_millis(due_at_ms.saturating_sub(unix_time_ms()));
                    tokio::select! {
                        _ = cancellation.cancelled() => {
                            state.request_termination(TerminationReason::Cancelled);
                        }
                        _ = tokio::time::sleep_until(deadline) => {
                            state.request_termination(TerminationReason::TimedOut);
                        }
                        _ = tokio::time::sleep(delay) => {
                            state.set_observed_time_ms(unix_time_ms().max(due_at_ms));
                            state.resolve_timer_wait(&wait_id).map_err(|_| {
                                TerminalExecutionError::infrastructure(
                                    TERMINAL_EXECUTION_INVALID,
                                )
                            })?;
                        }
                    }
                }
                SchedulerQuiescence::WaitingForChildRun { .. } => {
                    return Err(TerminalExecutionError::workflow(
                        TERMINAL_SUBFLOW_UNAVAILABLE,
                    ));
                }
                SchedulerQuiescence::WaitingForTask { .. }
                | SchedulerQuiescence::WaitingForChildren { .. }
                | SchedulerQuiescence::WaitingForDrain { .. } => {
                    return Err(TerminalExecutionError::infrastructure(
                        TERMINAL_EXECUTION_INVALID,
                    ));
                }
            },
        }
    }

    Err(TerminalExecutionError::infrastructure(
        TERMINAL_ACTION_BUDGET_EXHAUSTED,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn execute_task_with_retries(
    workers: &WorkerExecutorRegistry,
    request: &TaskExecutionRequest,
    response_id: &str,
    continuation: Vec<ModelContinuationTurn>,
    cancellation: CancellationToken,
    run_deadline: tokio::time::Instant,
    usage: &mut TerminalUsage,
    observations: &mut TerminalExecutionObservations,
    mut tool_publication: Option<&mut TerminalToolPublication>,
) -> Result<TaskExecutionResult, WorkerFailure> {
    let policy = request.effect_policy();
    let mut attempt = 1_u32;
    loop {
        let deadline = worker_deadline(run_deadline, policy.timeout_ms());
        let result = if request.task_kind() == insight_engine::SchedulerTaskKind::Llm
            && request.implementation() == "core.llm"
        {
            execute_model_with_tools(
                workers,
                request,
                response_id,
                continuation.clone(),
                cancellation.child_token(),
                deadline,
                run_deadline,
                usage,
                attempt,
                observations,
            )
            .await
        } else {
            if let Some(publication) = tool_publication.as_deref_mut() {
                publication.started(attempt);
            }
            execute_worker_once(
                workers,
                request,
                None,
                Vec::new(),
                cancellation.child_token(),
                deadline,
                attempt,
                tool_publication.as_deref_mut(),
                Some(Arc::clone(&observations.public_output_index)),
            )
            .await
        };
        match result {
            Ok(result) => return Ok(result),
            Err(failure) => {
                let evidence = if failure.class() == WorkerFailureClass::EffectOutcomeUnknown {
                    EffectEvidence::Unknown
                } else {
                    EffectEvidence::Started
                };
                if !failure.retryable()
                    || attempt >= policy.max_attempts()
                    || !evidence.permits_automatic_retry(policy.effect_idempotency())
                {
                    return Err(failure);
                }
                let backoff = retry_backoff(policy, attempt);
                attempt = attempt.saturating_add(1);
                wait_retry_backoff(backoff, &cancellation, run_deadline).await?;
            }
        }
    }
}

async fn wait_retry_backoff(
    backoff: Duration,
    cancellation: &CancellationToken,
    run_deadline: tokio::time::Instant,
) -> Result<(), WorkerFailure> {
    tokio::select! {
        _ = cancellation.cancelled() => Err(worker_failure(
            WorkerFailureClass::ControlTermination,
            "WORKER_CANCELLED",
            false,
        )),
        _ = tokio::time::sleep_until(run_deadline) => Err(worker_failure(
            WorkerFailureClass::ControlTermination,
            "WORKER_TIMEOUT",
            false,
        )),
        _ = tokio::time::sleep(backoff) => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_model_with_tools(
    workers: &WorkerExecutorRegistry,
    request: &TaskExecutionRequest,
    response_id: &str,
    mut turns: Vec<ModelContinuationTurn>,
    cancellation: CancellationToken,
    mut operation_deadline: DateTime<Utc>,
    run_deadline: tokio::time::Instant,
    usage: &mut TerminalUsage,
    attempt: u32,
    observations: &mut TerminalExecutionObservations,
) -> Result<TaskExecutionResult, WorkerFailure> {
    let (max_rounds, max_calls, tools) =
        model_tool_adapter::parse_frozen_model_tool_contract(request.deployment_binding())
            .map_err(|_| {
                worker_failure(
                    WorkerFailureClass::InvariantCorruption,
                    TERMINAL_MODEL_TOOL_INVALID,
                    false,
                )
            })?;
    let max_calls = max_calls.min(DEFAULT_MAX_MODEL_TOOL_CALLS);
    let mut total_calls = u32::try_from(turns.iter().map(|turn| turn.calls().len()).sum::<usize>())
        .unwrap_or(u32::MAX);
    loop {
        let model_call_no = u32::try_from(turns.len())
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| {
                worker_failure(
                    WorkerFailureClass::InvariantCorruption,
                    TERMINAL_MODEL_TOOL_LIMIT,
                    false,
                )
            })?;
        let publish = matches!(
            request.public_configuration().get("publish"),
            Some(insight_engine::plan::DescriptorValue::Boolean(true))
        );
        let authority =
            ModelCallAuthority::new_with_publication(response_id, model_call_no, publish, None)
                .map_err(|_| {
                    worker_failure(
                        WorkerFailureClass::InvariantCorruption,
                        TERMINAL_MODEL_TOOL_INVALID,
                        false,
                    )
                })?;
        let result = execute_worker_once(
            workers,
            request,
            Some(authority),
            turns.clone(),
            cancellation.child_token(),
            operation_deadline,
            attempt,
            None,
            Some(Arc::clone(&observations.public_output_index)),
        )
        .await?;
        if let Some(model_call) = result.model_call() {
            usage.record(model_call.usage().and_then(|value| value.public_value()));
        }
        let Some(batch) = result.model_tool_call_batch() else {
            return Ok(result);
        };
        if batch.model_call_no() != model_call_no
            || model_call_no > max_rounds
            || total_calls.saturating_add(u32::try_from(batch.calls().len()).unwrap_or(u32::MAX))
                > max_calls
        {
            return Err(worker_failure(
                WorkerFailureClass::InfrastructureFailure,
                TERMINAL_MODEL_TOOL_LIMIT,
                false,
            ));
        }
        total_calls =
            total_calls.saturating_add(u32::try_from(batch.calls().len()).unwrap_or(u32::MAX));
        publish_terminal_function_call_tails(
            observations.live_response_broker.as_ref(),
            request,
            attempt,
            batch,
        )?;

        let mut prepared = Vec::with_capacity(batch.calls().len());
        for call in batch.calls() {
            let action = tools.get(call.name()).cloned().ok_or_else(|| {
                worker_failure(
                    WorkerFailureClass::InvariantCorruption,
                    TERMINAL_MODEL_TOOL_INVALID,
                    false,
                )
            })?;
            model_tool_adapter::validate_tool_arguments(&action, call.arguments()).map_err(
                |_| {
                    worker_failure(
                        WorkerFailureClass::InvariantCorruption,
                        TERMINAL_MODEL_TOOL_INVALID,
                        false,
                    )
                },
            )?;
            let identity = terminal_tool_identity(
                request.run_id(),
                request.activation_id(),
                model_call_no,
                call.index(),
                call.call_id(),
            )?;
            let action_spec = ModelToolActionExecutionSpec::new(
                action.action_id(),
                action.action_version(),
                action.descriptor_hash(),
                action.input_schema().clone(),
                action.effect_policy().clone(),
                action.deployment_binding().clone(),
            );
            let spec = ModelToolExecutionSpec::new(
                request.run_id().clone(),
                request.activation_id().clone(),
                model_call_no,
                identity.0.clone(),
                identity.1,
                call.index(),
                action_spec,
                call.arguments().clone(),
            )
            .map_err(|_| {
                worker_failure(
                    WorkerFailureClass::InvariantCorruption,
                    TERMINAL_MODEL_TOOL_INVALID,
                    false,
                )
            })?;
            let tool_request =
                TaskExecutionRequest::from_model_tool_claim(&spec).map_err(|_| {
                    worker_failure(
                        WorkerFailureClass::InvariantCorruption,
                        TERMINAL_MODEL_TOOL_INVALID,
                        false,
                    )
                })?;
            prepared.push((
                call.clone(),
                action,
                identity.0,
                tool_request,
                cancellation.child_token(),
            ));
        }
        let run_id = request.run_id().clone();
        let activation_id = request.activation_id().clone();
        let broker = Arc::clone(&observations.live_response_broker);
        let public_output_index = Arc::clone(&observations.public_output_index);
        let completed = futures::future::join_all(prepared.into_iter().map(
            |(call, action, tool_task_id, tool_request, tool_cancellation)| {
                let broker = Arc::clone(&broker);
                let run_id = run_id.clone();
                let activation_id = activation_id.clone();
                let public_output_index = Arc::clone(&public_output_index);
                async move {
                    let mut publication = TerminalToolPublication::new(
                        Arc::clone(&broker),
                        &run_id,
                        &activation_id,
                        tool_task_id.as_str(),
                        call.call_id(),
                        action.name(),
                        action.effective_public_policy(),
                        call.arguments(),
                    )?;
                    let mut local_usage = TerminalUsage::default();
                    let mut local_observations = TerminalExecutionObservations {
                        live_response_broker: broker,
                        tool_results: Vec::new(),
                        public_output_index,
                    };
                    let tool_result = execute_task_with_retries(
                        workers,
                        &tool_request,
                        response_id,
                        Vec::new(),
                        tool_cancellation,
                        run_deadline,
                        &mut local_usage,
                        &mut local_observations,
                        Some(&mut publication),
                    )
                    .await;
                    let tool_result = match tool_result {
                        Ok(result) => result,
                        Err(failure) => {
                            publication.failed(&failure);
                            return Err(failure);
                        }
                    };
                    let processed = (|| {
                        let value = tool_result
                            .outputs()
                            .values()
                            .next()
                            .map(|value| value.value().clone())
                            .ok_or_else(|| {
                                worker_failure(
                                    WorkerFailureClass::InvariantCorruption,
                                    TERMINAL_MODEL_TOOL_INVALID,
                                    false,
                                )
                            })?;
                        model_tool_adapter::validate_tool_result(&action, &value).map_err(
                            |_| {
                                worker_failure(
                                    WorkerFailureClass::InvariantCorruption,
                                    TERMINAL_MODEL_TOOL_INVALID,
                                    false,
                                )
                            },
                        )?;
                        let model_result = ModelToolResult::new(call.call_id(), value.clone())
                            .map_err(|_| {
                                worker_failure(
                                    WorkerFailureClass::InvariantCorruption,
                                    TERMINAL_MODEL_TOOL_INVALID,
                                    false,
                                )
                            })?;
                        let public_result = publication.completed(&value)?;
                        Ok::<_, WorkerFailure>((model_result, public_result))
                    })();
                    if let Err(failure) = &processed {
                        publication.failed(failure);
                    }
                    processed
                }
            },
        ))
        .await;
        let mut results = Vec::with_capacity(completed.len());
        let mut first_failure = None;
        for completed in completed {
            match completed {
                Ok((result, public_result)) => {
                    results.push(result);
                    if let Some(public_result) = public_result {
                        observations.tool_results.push(public_result);
                    }
                }
                Err(failure) => {
                    first_failure.get_or_insert(failure);
                }
            }
        }
        if let Some(failure) = first_failure {
            return Err(failure);
        }
        turns.push(
            ModelContinuationTurn::new(
                model_call_no,
                batch.assistant_content().map(str::to_owned),
                batch.calls().to_vec(),
                results,
            )
            .map_err(|_| {
                worker_failure(
                    WorkerFailureClass::InvariantCorruption,
                    TERMINAL_MODEL_TOOL_INVALID,
                    false,
                )
            })?,
        );
        operation_deadline = worker_deadline(run_deadline, request.effect_policy().timeout_ms());
    }
}

fn publish_terminal_function_call_tails(
    broker: &dyn LiveResponseBroker,
    request: &TaskExecutionRequest,
    attempt: u32,
    batch: &ModelToolCallBatch,
) -> Result<(), WorkerFailure> {
    let attempt_no = AttemptNo::new(attempt).map_err(|_| {
        worker_failure(
            WorkerFailureClass::InvariantCorruption,
            TERMINAL_MODEL_TOOL_INVALID,
            false,
        )
    })?;
    for public_call in batch.public_function_calls() {
        let call = batch
            .calls()
            .get(usize::try_from(public_call.call_index()).unwrap_or(usize::MAX))
            .filter(|call| call.index() == public_call.call_index())
            .ok_or_else(|| {
                worker_failure(
                    WorkerFailureClass::InvariantCorruption,
                    TERMINAL_MODEL_TOOL_INVALID,
                    false,
                )
            })?;
        let arguments_jcs = serde_jcs::to_string(call.arguments()).map_err(|_| {
            worker_failure(
                WorkerFailureClass::InvariantCorruption,
                TERMINAL_MODEL_TOOL_INVALID,
                false,
            )
        })?;
        let identity = LiveResponseItemIdentity::new(
            request.run_id().clone(),
            request.activation_id().clone(),
            attempt_no,
            batch.model_call_no(),
            public_call.public_item().item_id(),
            public_call.public_item().output_index(),
        )
        .map_err(|_| {
            worker_failure(
                WorkerFailureClass::InvariantCorruption,
                TERMINAL_MODEL_TOOL_INVALID,
                false,
            )
        })?;
        let seal_index = public_call.completed_seal_index().ok_or_else(|| {
            worker_failure(
                WorkerFailureClass::InvariantCorruption,
                TERMINAL_MODEL_TOOL_INVALID,
                false,
            )
        })?;
        let publication = CompletedFunctionCallTailPublication::build(
            identity,
            call.call_id(),
            call.name(),
            arguments_jcs,
            seal_index,
        )
        .map_err(|_| {
            worker_failure(
                WorkerFailureClass::InvariantCorruption,
                TERMINAL_MODEL_TOOL_INVALID,
                false,
            )
        })?;
        let (frames, seal) = publication.into_parts();
        for frame in frames {
            let _ = broker.publish(frame);
        }
        let _ = broker.seal(seal);
    }
    Ok(())
}

struct TerminalToolPublication {
    broker: Arc<dyn LiveResponseBroker>,
    projection: WorkflowToolPublicProjection,
    run_id: RunId,
    activation_id: insight_engine::ActivationId,
    source_id: String,
    call_id: String,
    tool_name: String,
    started_arguments: Option<Value>,
    active_identity: Option<LiveWorkflowObservationIdentity>,
    next_local_sequence: u64,
    first_started_at: Option<Instant>,
    progress_window_started_at: Instant,
    progress_in_window: u32,
    progress_total: u32,
}

impl TerminalToolPublication {
    #[allow(clippy::too_many_arguments)]
    fn new(
        broker: Arc<dyn LiveResponseBroker>,
        run_id: &RunId,
        activation_id: &insight_engine::ActivationId,
        source_id: &str,
        call_id: &str,
        tool_name: &str,
        effective_public_policy: &Value,
        arguments: &Value,
    ) -> Result<Self, WorkerFailure> {
        let projection =
            WorkflowToolPublicProjection::from_frozen_effective_policy(effective_public_policy)
                .map_err(|_| {
                    worker_failure(
                        WorkerFailureClass::InvariantCorruption,
                        TERMINAL_MODEL_TOOL_INVALID,
                        false,
                    )
                })?;
        let started_arguments = projection
            .project_validated_completed_arguments(arguments)
            .map_err(|_| {
                worker_failure(
                    WorkerFailureClass::InvariantCorruption,
                    TERMINAL_MODEL_TOOL_INVALID,
                    false,
                )
            })?
            .workflow_started_arguments()
            .cloned();
        Ok(Self {
            broker,
            projection,
            run_id: run_id.clone(),
            activation_id: activation_id.clone(),
            source_id: source_id.to_owned(),
            call_id: call_id.to_owned(),
            tool_name: tool_name.to_owned(),
            started_arguments,
            active_identity: None,
            next_local_sequence: 0,
            first_started_at: None,
            progress_window_started_at: Instant::now(),
            progress_in_window: 0,
            progress_total: 0,
        })
    }

    fn progress_authorized(&self) -> bool {
        self.projection.progress_authorized()
    }

    fn started(&mut self, attempt: u32) {
        if !self.projection.call_authorized() {
            return;
        }
        let Ok(attempt_no) = AttemptNo::new(attempt) else {
            return;
        };
        let Ok(identity) = LiveWorkflowObservationIdentity::new(
            self.run_id.clone(),
            self.activation_id.clone(),
            attempt_no,
            &self.source_id,
        ) else {
            return;
        };
        self.first_started_at.get_or_insert_with(Instant::now);
        self.active_identity = Some(identity);
        self.next_local_sequence = 0;
        self.emit(LiveResponsePayload::ToolStarted {
            call_id: self.call_id.clone(),
            tool_name: self.tool_name.clone(),
            arguments: self.started_arguments.clone(),
        });
    }

    fn progress(
        &mut self,
        value: &Value,
    ) -> Result<worker_adapter::ModelToolProgressDisposition, worker_adapter::ModelToolProgressError>
    {
        if self.active_identity.is_none() {
            return Ok(worker_adapter::ModelToolProgressDisposition::Dropped);
        }
        let content = self
            .projection
            .project_validated_progress(value)
            .map_err(|_| worker_adapter::invalid_model_tool_progress())?
            .ok_or_else(worker_adapter::invalid_model_tool_progress)?;
        let now = Instant::now();
        if now.duration_since(self.progress_window_started_at)
            >= TERMINAL_MODEL_TOOL_PROGRESS_WINDOW
        {
            self.progress_window_started_at = now;
            self.progress_in_window = 0;
        }
        if self.progress_total >= TERMINAL_MODEL_TOOL_PROGRESS_TOTAL_LIMIT
            || self.progress_in_window >= TERMINAL_MODEL_TOOL_PROGRESS_BURST
        {
            self.next_local_sequence = self.next_local_sequence.saturating_add(1);
            return Ok(worker_adapter::ModelToolProgressDisposition::Dropped);
        }
        self.progress_total = self.progress_total.saturating_add(1);
        self.progress_in_window = self.progress_in_window.saturating_add(1);
        let outcome = self.emit(LiveResponsePayload::ToolProgress {
            call_id: self.call_id.clone(),
            tool_name: self.tool_name.clone(),
            content,
        });
        Ok(
            if matches!(
                outcome,
                Some(
                    LiveResponsePublishOutcome::Enqueued
                        | LiveResponsePublishOutcome::EnqueuedAfterGap
                        | LiveResponsePublishOutcome::EnqueuedAfterBestEffortLoss
                )
            ) {
                worker_adapter::ModelToolProgressDisposition::Published
            } else {
                worker_adapter::ModelToolProgressDisposition::Dropped
            },
        )
    }

    fn completed(&mut self, value: &Value) -> Result<Option<WorkflowToolResult>, WorkerFailure> {
        let projected = self
            .projection
            .project_validated_completed_result(self.call_id.clone(), self.tool_name.clone(), value)
            .map_err(|_| {
                worker_failure(
                    WorkerFailureClass::InvariantCorruption,
                    TERMINAL_MODEL_TOOL_PUBLIC_RESULT_INVALID,
                    false,
                )
            })?;
        if !self.projection.call_authorized() {
            self.active_identity = None;
            return Ok(None);
        }
        let result = match projected {
            Some(result) => result,
            None => {
                WorkflowToolResult::new(self.call_id.clone(), self.tool_name.clone(), Vec::new())
                    .map_err(|_| {
                        worker_failure(
                            WorkerFailureClass::InvariantCorruption,
                            TERMINAL_MODEL_TOOL_PUBLIC_RESULT_INVALID,
                            false,
                        )
                    })?
            }
        };
        let duration_ms = self.duration_ms();
        self.emit(LiveResponsePayload::ToolCompleted {
            call_id: self.call_id.clone(),
            tool_name: self.tool_name.clone(),
            duration_ms,
            content: result.content().to_vec(),
        });
        self.active_identity = None;
        Ok(Some(result))
    }

    fn failed(&mut self, failure: &WorkerFailure) {
        if !self.projection.call_authorized() || self.active_identity.is_none() {
            return;
        }
        if failure.class() == WorkerFailureClass::ControlTermination
            && failure.code() == "WORKER_CANCELLED"
        {
            self.active_identity = None;
            return;
        }
        let message = match failure.class() {
            WorkerFailureClass::SafeBusinessFailure => "The tool request was rejected.",
            WorkerFailureClass::EffectOutcomeUnknown => "The tool outcome could not be confirmed.",
            WorkerFailureClass::ControlTermination
                if failure.code() == "WORKER_DEADLINE_EXCEEDED"
                    || failure.code() == "WORKER_TIMEOUT" =>
            {
                "The tool execution timed out."
            }
            WorkerFailureClass::ControlTermination => "The tool execution was cancelled.",
            WorkerFailureClass::InfrastructureFailure | WorkerFailureClass::InvariantCorruption => {
                "The tool could not be completed."
            }
        };
        self.emit(LiveResponsePayload::ToolFailed {
            call_id: self.call_id.clone(),
            tool_name: self.tool_name.clone(),
            duration_ms: self.duration_ms(),
            error: WorkflowPublicError {
                code: failure.code().to_owned(),
                message: message.to_owned(),
            },
        });
        self.active_identity = None;
    }

    fn duration_ms(&self) -> u64 {
        self.first_started_at
            .map(|started_at| u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0)
    }

    fn emit(&mut self, payload: LiveResponsePayload) -> Option<LiveResponsePublishOutcome> {
        let identity = self.active_identity.clone()?;
        let publication = LiveResponsePublication::new_workflow_observation(
            identity,
            self.next_local_sequence,
            payload,
        )
        .ok()?;
        self.next_local_sequence = self.next_local_sequence.saturating_add(1);
        Some(self.broker.publish(publication))
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_worker_once(
    workers: &WorkerExecutorRegistry,
    request: &TaskExecutionRequest,
    model_call: Option<ModelCallAuthority>,
    continuation: Vec<ModelContinuationTurn>,
    cancellation: CancellationToken,
    deadline: DateTime<Utc>,
    attempt: u32,
    mut tool_publication: Option<&mut TerminalToolPublication>,
    public_output_index: Option<Arc<AtomicU32>>,
) -> Result<TaskExecutionResult, WorkerFailure> {
    let attempt_no = AttemptNo::new(attempt).map_err(|_| {
        worker_failure(
            WorkerFailureClass::InvariantCorruption,
            TERMINAL_EXECUTION_INVALID,
            false,
        )
    })?;
    let lease_epoch = LeaseEpoch::new(u64::from(attempt)).map_err(|_| {
        worker_failure(
            WorkerFailureClass::InvariantCorruption,
            TERMINAL_EXECUTION_INVALID,
            false,
        )
    })?;
    let mut context = WorkerExecutionContext::new(
        attempt_no,
        lease_epoch,
        format!("terminal-{}", request.task_id().as_str()),
        deadline,
    )
    .map_err(|_| {
        worker_failure(
            WorkerFailureClass::InvariantCorruption,
            TERMINAL_EXECUTION_INVALID,
            false,
        )
    })?;
    if let Some(authority) = model_call {
        context = context.with_model_call(authority);
    }
    if !continuation.is_empty() {
        context = context.with_model_continuation(continuation).map_err(|_| {
            worker_failure(
                WorkerFailureClass::InvariantCorruption,
                TERMINAL_MODEL_TOOL_INVALID,
                false,
            )
        })?;
    }

    let needs_allocator = context
        .model_call()
        .is_some_and(|authority| authority.publication_enabled());
    let (mut services, mut requests) = if needs_allocator {
        let (allocator, requests) = worker_adapter::model_call_public_item_reservation_channel();
        (
            worker_adapter::services_with_model_call_public_item_allocator(
                WorkerRuntimeServices::default(),
                allocator,
            ),
            Some(requests),
        )
    } else {
        (WorkerRuntimeServices::default(), None)
    };
    let mut progress_requests = None;
    if tool_publication
        .as_deref()
        .is_some_and(TerminalToolPublication::progress_authorized)
    {
        let (publisher, receiver) = worker_adapter::model_tool_progress_channel(
            TERMINAL_MODEL_TOOL_PROGRESS_QUEUE_CAPACITY,
        );
        services = worker_adapter::services_with_model_tool_progress_publisher(services, publisher);
        progress_requests = Some(receiver);
    }
    let worker = AssertUnwindSafe(worker_adapter::execute_with_runtime_services(
        workers,
        &context,
        request,
        &services,
        cancellation.child_token(),
    ))
    .catch_unwind();
    tokio::pin!(worker);
    let deadline = tokio::time::Instant::from_std(
        std::time::Instant::now()
            + deadline
                .signed_duration_since(Utc::now())
                .to_std()
                .unwrap_or(Duration::ZERO),
    );
    let public_output_index = public_output_index.unwrap_or_else(|| Arc::new(AtomicU32::new(0)));

    loop {
        tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(worker_failure(
                    WorkerFailureClass::ControlTermination,
                    "WORKER_CANCELLED",
                    false,
                ));
            }
            _ = tokio::time::sleep_until(deadline) => {
                return Err(worker_failure(
                    WorkerFailureClass::InfrastructureFailure,
                    "WORKER_DEADLINE_EXCEEDED",
                    true,
                ));
            }
            result = &mut worker => {
                return match result {
                    Ok(result) => result,
                    Err(_) => Err(worker_failure(
                        WorkerFailureClass::EffectOutcomeUnknown,
                        TERMINAL_EXECUTOR_PANICKED,
                        false,
                    )),
                };
            }
            reservation = async {
                match requests.as_mut() {
                    Some(requests) => requests.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some(reservation) = reservation {
                    let reservation_label = request_id_for_item(&reservation);
                    let output_index = public_output_index.fetch_add(1, Ordering::Relaxed);
                    let item = ResponseItemAuthority::new(
                        format!(
                            "item_{}",
                            ContentHash::from_bytes(
                                format!(
                                    "{}:{}:{}",
                                    request.run_id().as_str(),
                                    reservation_label,
                                    output_index,
                                )
                                .as_bytes()
                            )
                            .as_str()
                            .trim_start_matches("sha256:")
                        ),
                        output_index,
                    );
                    worker_adapter::respond_reservation(
                        reservation,
                        item.map_err(|_| {
                            worker_adapter::ModelCallPublicItemReservationError::StateConflict
                        }),
                    );
                }
            }
            progress = async {
                match progress_requests.as_mut() {
                    Some(requests) => requests.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some(progress) = progress {
                    let result = tool_publication
                        .as_deref_mut()
                        .map_or(
                            Ok(worker_adapter::ModelToolProgressDisposition::Dropped),
                            |publication| publication.progress(progress.value()),
                        );
                    progress.respond(result);
                }
            }
        }
    }
}

fn request_id_for_item(request: &worker_adapter::ModelCallPublicItemReservationRequest) -> String {
    match worker_adapter::reservation_kind(request) {
        worker_adapter::ModelCallPublicItemReservationKind::Message => "message".to_owned(),
        worker_adapter::ModelCallPublicItemReservationKind::FunctionCall {
            call_index,
            call_id,
            tool_name,
        } => format!("function:{call_index}:{call_id}:{tool_name}"),
    }
}

fn terminal_tool_identity(
    run_id: &RunId,
    activation_id: &insight_engine::ActivationId,
    model_call_no: u32,
    call_index: u32,
    call_id: &str,
) -> Result<(SchedulerTaskId, EffectId), WorkerFailure> {
    let canonical = serde_jcs::to_vec(&json!({
        "run_id": run_id,
        "activation_id": activation_id,
        "model_call_no": model_call_no,
        "call_index": call_index,
        "call_id": call_id,
    }))
    .map_err(|_| {
        worker_failure(
            WorkerFailureClass::InvariantCorruption,
            TERMINAL_MODEL_TOOL_INVALID,
            false,
        )
    })?;
    let task = ContentHash::from_bytes(
        &[
            b"terminal_model_tool_task.v1\0".as_slice(),
            canonical.as_slice(),
        ]
        .concat(),
    );
    let effect = ContentHash::from_bytes(
        &[
            b"terminal_model_tool_effect.v1\0".as_slice(),
            canonical.as_slice(),
        ]
        .concat(),
    );
    Ok((
        SchedulerTaskId::parse(format!(
            "task_{}",
            task.as_str().trim_start_matches("sha256:")
        ))
        .map_err(|_| {
            worker_failure(
                WorkerFailureClass::InvariantCorruption,
                TERMINAL_MODEL_TOOL_INVALID,
                false,
            )
        })?,
        EffectId::new(format!(
            "effect_{}",
            effect.as_str().trim_start_matches("sha256:")
        ))
        .map_err(|_| {
            worker_failure(
                WorkerFailureClass::InvariantCorruption,
                TERMINAL_MODEL_TOOL_INVALID,
                false,
            )
        })?,
    ))
}

fn retry_backoff(policy: &insight_engine::WorkerEffectPolicy, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(63);
    Duration::from_millis(
        policy
            .initial_backoff_ms()
            .saturating_mul(1_u64 << exponent)
            .min(policy.max_backoff_ms()),
    )
}

fn worker_deadline(run_deadline: tokio::time::Instant, timeout_ms: u64) -> DateTime<Utc> {
    let remaining = run_deadline.saturating_duration_since(tokio::time::Instant::now());
    Utc::now()
        + chrono::Duration::from_std(remaining.min(Duration::from_millis(timeout_ms)))
            .unwrap_or_else(|_| chrono::Duration::zero())
}

fn worker_failure(class: WorkerFailureClass, code: &'static str, retryable: bool) -> WorkerFailure {
    WorkerFailure::new(class, code, retryable).expect("terminal worker failure constants are valid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::response_stream::InMemoryLiveResponseBroker;
    use insight_engine::response::{LiveResponseDelivery, ResponseStreamEvent};

    #[tokio::test(start_paused = true)]
    async fn retry_backoff_is_cut_off_by_the_hard_run_deadline() {
        let cancellation = CancellationToken::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let failure = wait_retry_backoff(Duration::from_secs(30), &cancellation, deadline)
            .await
            .unwrap_err();
        assert_eq!(failure.code(), "WORKER_TIMEOUT");
        assert_eq!(tokio::time::Instant::now(), deadline);
    }

    #[tokio::test]
    async fn terminal_tool_publication_emits_progress_terminal_result_and_rejects_late_updates() {
        let broker = Arc::new(InMemoryLiveResponseBroker::new(16, 4).unwrap());
        let run_id = RunId::new("run_terminal_progress").unwrap();
        let mut subscriber = broker.subscribe(run_id.clone()).await.unwrap();
        let progress_schema = json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": {},
            "type": "object",
            "properties": {
                "completed": {"type": "integer", "minimum": 0},
                "total": {"type": "integer", "minimum": 1}
            },
            "required": ["completed", "total"],
            "additionalProperties": false
        });
        let result_schema = json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": {},
            "type": "object",
            "properties": {"value": {"type": "integer"}},
            "required": ["value"],
            "additionalProperties": false
        });
        let policy = json!({
            "call": true,
            "arguments": "private",
            "progress": progress_schema,
            "result": result_schema
        });
        let mut publication = TerminalToolPublication::new(
            broker,
            &run_id,
            &insight_engine::ActivationId::new("activation_terminal_progress").unwrap(),
            "task_terminal_progress",
            "call_terminal_progress",
            "progress_counter",
            &policy,
            &json!({"total": 2}),
        )
        .unwrap();

        publication.started(1);
        assert_eq!(
            publication
                .progress(&json!({"completed": 1, "total": 2}))
                .unwrap(),
            worker_adapter::ModelToolProgressDisposition::Published
        );
        assert_eq!(
            publication
                .progress(&json!({"completed": "invalid", "total": 2}))
                .unwrap_err()
                .code(),
            "MODEL_TOOL_PUBLIC_PROGRESS_INVALID"
        );
        let result = publication
            .completed(&json!({"value": 42}))
            .unwrap()
            .unwrap();
        assert_eq!(result.content()[0].json(), Some(&json!({"value": 42})));
        assert_eq!(
            publication
                .progress(&json!({"completed": 2, "total": 2}))
                .unwrap(),
            worker_adapter::ModelToolProgressDisposition::Dropped
        );

        let mut events = Vec::new();
        for sequence in 0..3 {
            let LiveResponseDelivery::Publication(publication) = subscriber.recv().await.unwrap()
            else {
                panic!("expected a terminal-only tool publication");
            };
            events.push(publication.into_public_event(sequence));
        }
        assert!(matches!(
            events[0],
            ResponseStreamEvent::WorkflowToolStarted { .. }
        ));
        assert!(matches!(
            events[1],
            ResponseStreamEvent::WorkflowToolProgress { .. }
        ));
        assert!(matches!(
            events[2],
            ResponseStreamEvent::WorkflowToolCompleted { duration_ms: _, .. }
        ));
    }
}

fn terminal_outcome(
    terminal: Option<&RunTerminalFact>,
    usage: Option<Value>,
    tool_results: Vec<WorkflowToolResult>,
) -> Result<TerminalExecutionOutcome, TerminalExecutionError> {
    match terminal
        .ok_or_else(|| TerminalExecutionError::infrastructure(TERMINAL_EXECUTION_INVALID))?
    {
        RunTerminalFact::Succeeded(output) => Ok(TerminalExecutionOutcome::Succeeded {
            output: output.value().clone(),
            usage,
            tool_results,
        }),
        RunTerminalFact::Failed(error) => Ok(TerminalExecutionOutcome::Failed {
            failure_kind: TerminalFailureKind::Workflow,
            error_code: error.code().to_owned(),
            safe_message: Some(error.message().to_owned()),
            usage,
            tool_results,
        }),
        RunTerminalFact::FailedInternal(failure) => Ok(TerminalExecutionOutcome::Failed {
            failure_kind: match failure.class() {
                WorkerFailureClass::SafeBusinessFailure => TerminalFailureKind::Workflow,
                WorkerFailureClass::InfrastructureFailure
                | WorkerFailureClass::EffectOutcomeUnknown
                | WorkerFailureClass::InvariantCorruption => TerminalFailureKind::Infrastructure,
                WorkerFailureClass::ControlTermination => TerminalFailureKind::Operation,
            },
            error_code: failure.code().to_owned(),
            safe_message: failure
                .safe_error()
                .and_then(|value| value.value().get("message"))
                .and_then(Value::as_str)
                .map(str::to_owned),
            usage,
            tool_results,
        }),
        RunTerminalFact::FailedPlanning(failure) => Ok(TerminalExecutionOutcome::Failed {
            failure_kind: TerminalFailureKind::Infrastructure,
            error_code: failure.internal_code().to_owned(),
            safe_message: None,
            usage,
            tool_results,
        }),
        RunTerminalFact::Cancelled | RunTerminalFact::Interrupted => {
            Ok(TerminalExecutionOutcome::Cancelled { tool_results })
        }
        RunTerminalFact::TimedOut => Ok(TerminalExecutionOutcome::TimedOut { tool_results }),
    }
}

fn unix_time_ms() -> u64 {
    u64::try_from(Utc::now().timestamp_millis()).unwrap_or(0)
}

#[derive(Debug, Default)]
struct TerminalUsage {
    values: Vec<Value>,
}

impl TerminalUsage {
    fn record(&mut self, value: Option<Value>) {
        if let Some(value) = value {
            self.values.push(value);
        }
    }

    fn finish(self) -> Option<Value> {
        match self.values.len() {
            0 => None,
            1 => self.values.into_iter().next(),
            _ => Some(json!({"model_calls": self.values})),
        }
    }
}
