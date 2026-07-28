//! In-process execution loop used by the terminal-only runtime.
//!
//! The loop consumes the engine's deterministic planner and
//! [`insight_engine::TerminalSchedulerState`]. It never receives a durable
//! repository, recovery fence, task lease, or checkpoint callback.

use std::{panic::AssertUnwindSafe, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use futures::FutureExt;
use insight_durable::model_tool_queue::adapter as model_tool_adapter;
use insight_engine::{
    worker::{
        adapter as worker_adapter, ModelCallAuthority, ModelContinuationTurn,
        ModelToolActionExecutionSpec, ModelToolExecutionSpec, ModelToolResult,
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
    },
    Failed {
        failure_kind: TerminalFailureKind,
        error_code: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        safe_message: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        usage: Option<Value>,
    },
    Cancelled,
    TimedOut,
}

impl TerminalExecutionOutcome {
    pub fn terminal_state(&self) -> &'static str {
        match self {
            Self::Succeeded { .. } => "succeeded",
            Self::Failed { .. } => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
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
            Self::Cancelled | Self::TimedOut => None,
        }
    }

    pub fn error_code(&self) -> Option<&str> {
        match self {
            Self::Failed { error_code, .. } => Some(error_code),
            Self::Succeeded { .. } | Self::Cancelled | Self::TimedOut => None,
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

/// Execute one verified deployment entirely in process.
pub async fn execute_terminal_plan(
    agent: Arc<DeployedAgent>,
    workers: Arc<WorkerExecutorRegistry>,
    run_id: RunId,
    response_id: String,
    input: RuntimeValue,
    cancellation: CancellationToken,
    config: TerminalExecutionConfig,
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
                    return terminal_outcome(state.terminal(), usage.finish());
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
            )
            .await
        } else {
            execute_worker_once(
                workers,
                request,
                None,
                Vec::new(),
                cancellation.child_token(),
                deadline,
                attempt,
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

        let mut results = Vec::with_capacity(batch.calls().len());
        for call in batch.calls() {
            let action = tools.get(call.name()).ok_or_else(|| {
                worker_failure(
                    WorkerFailureClass::InvariantCorruption,
                    TERMINAL_MODEL_TOOL_INVALID,
                    false,
                )
            })?;
            model_tool_adapter::validate_tool_arguments(action, call.arguments()).map_err(
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
                identity.0,
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
            let tool_result = Box::pin(execute_task_with_retries(
                workers,
                &tool_request,
                response_id,
                Vec::new(),
                cancellation.child_token(),
                run_deadline,
                usage,
            ))
            .await?;
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
            model_tool_adapter::validate_tool_result(action, &value).map_err(|_| {
                worker_failure(
                    WorkerFailureClass::InvariantCorruption,
                    TERMINAL_MODEL_TOOL_INVALID,
                    false,
                )
            })?;
            results.push(ModelToolResult::new(call.call_id(), value).map_err(|_| {
                worker_failure(
                    WorkerFailureClass::InvariantCorruption,
                    TERMINAL_MODEL_TOOL_INVALID,
                    false,
                )
            })?);
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

#[allow(clippy::too_many_arguments)]
async fn execute_worker_once(
    workers: &WorkerExecutorRegistry,
    request: &TaskExecutionRequest,
    model_call: Option<ModelCallAuthority>,
    continuation: Vec<ModelContinuationTurn>,
    cancellation: CancellationToken,
    deadline: DateTime<Utc>,
    attempt: u32,
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
    let (services, mut requests) = if needs_allocator {
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
    let mut output_index = 0_u32;

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
                    output_index = output_index.saturating_add(1);
                    worker_adapter::respond_reservation(
                        reservation,
                        item.map_err(|_| {
                            worker_adapter::ModelCallPublicItemReservationError::StateConflict
                        }),
                    );
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
}

fn terminal_outcome(
    terminal: Option<&RunTerminalFact>,
    usage: Option<Value>,
) -> Result<TerminalExecutionOutcome, TerminalExecutionError> {
    match terminal
        .ok_or_else(|| TerminalExecutionError::infrastructure(TERMINAL_EXECUTION_INVALID))?
    {
        RunTerminalFact::Succeeded(output) => Ok(TerminalExecutionOutcome::Succeeded {
            output: output.value().clone(),
            usage,
        }),
        RunTerminalFact::Failed(error) => Ok(TerminalExecutionOutcome::Failed {
            failure_kind: TerminalFailureKind::Workflow,
            error_code: error.code().to_owned(),
            safe_message: Some(error.message().to_owned()),
            usage,
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
        }),
        RunTerminalFact::FailedPlanning(failure) => Ok(TerminalExecutionOutcome::Failed {
            failure_kind: TerminalFailureKind::Infrastructure,
            error_code: failure.internal_code().to_owned(),
            safe_message: None,
            usage,
        }),
        RunTerminalFact::Cancelled | RunTerminalFact::Interrupted => {
            Ok(TerminalExecutionOutcome::Cancelled)
        }
        RunTerminalFact::TimedOut => Ok(TerminalExecutionOutcome::TimedOut),
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
