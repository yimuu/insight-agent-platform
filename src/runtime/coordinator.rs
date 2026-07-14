use std::{sync::Arc, time::Instant};

use chrono::Utc;
use serde_json::{json, Value};

use crate::{
    dsl::compiled::CompiledAgent,
    events::{
        hub::{EventError, EventHub},
        protocol::{RunEventScope, RunEventType},
    },
    history::{
        repository::{HistoryError, RunRepository},
        types::{
            NewRun, RunLifecycle, RunRecord, RunStatus, RunTerminal, StopError, TerminalUpdate,
        },
    },
    nodes::registry::NodeExecutorRegistry,
    observability::{elapsed_ms, json_size_bytes},
    outcome::{FailureKind, RunFailure, TerminalOutcome},
};

use super::{
    ExecutionLimiter, RunContext, RunError, RunErrorKind, RunMetadata, RunState, Scheduler,
    SchedulerResult, StopReason, StopSignal,
};

pub struct RunCoordinator {
    agent: Arc<CompiledAgent>,
    executors: NodeExecutorRegistry,
    events: EventHub,
    repository: Arc<dyn RunRepository>,
    limiter: ExecutionLimiter,
}

struct TerminalLogSummary {
    status: RunStatus,
    output_bytes: usize,
    error_code: String,
    failure_kind: Option<FailureKind>,
}

impl RunCoordinator {
    pub fn new(
        agent: Arc<CompiledAgent>,
        executors: NodeExecutorRegistry,
        events: EventHub,
        repository: Arc<dyn RunRepository>,
        limiter: ExecutionLimiter,
    ) -> Self {
        Self {
            agent,
            executors,
            events,
            repository,
            limiter,
        }
    }

    pub async fn execute(
        &self,
        new_run: NewRun,
        input: Value,
        stop: StopSignal,
    ) -> Result<RunStatus, RunError> {
        self.validate_run(&new_run, &input)?;
        self.repository
            .create_run(new_run.clone())
            .await
            .map_err(history_error)?;
        self.execute_managed(new_run, input, stop, Arc::new(RunState::new()))
            .await
    }

    pub(crate) async fn execute_existing(
        &self,
        new_run: NewRun,
        input: Value,
        stop: StopSignal,
        state: Arc<RunState>,
    ) -> Result<RunStatus, RunError> {
        self.validate_run(&new_run, &input)?;
        self.execute_managed(new_run, input, stop, state).await
    }

    async fn execute_managed(
        &self,
        new_run: NewRun,
        input: Value,
        stop: StopSignal,
        state: Arc<RunState>,
    ) -> Result<RunStatus, RunError> {
        let started = Instant::now();
        match self
            .execute_inner(&new_run, input, stop, Arc::clone(&state), started)
            .await
        {
            Ok(status) => Ok(status),
            Err(error) => {
                tracing::error!(
                    run_id = new_run.run_id,
                    code = error.code(),
                    "run infrastructure failed; recovering durable terminal state"
                );
                self.recover_infrastructure_failure(&state, &new_run, started)
                    .await
            }
        }
    }

    async fn execute_inner(
        &self,
        new_run: &NewRun,
        input: Value,
        stop: StopSignal,
        state: Arc<RunState>,
        started: Instant,
    ) -> Result<RunStatus, RunError> {
        self.events
            .publish(
                run_scope(new_run),
                RunEventType::RunCreated,
                json!({
                    "status": RunStatus::Created,
                    "attachment": new_run.attachment,
                }),
            )
            .await
            .map_err(event_error)?;

        let started_at = Utc::now();
        self.repository
            .mark_running(&new_run.run_id, started_at)
            .await
            .map_err(history_error)?;
        state.start().await?;
        self.events
            .publish(
                run_scope(new_run),
                RunEventType::RunStarted,
                json!({"status":RunStatus::Running}),
            )
            .await
            .map_err(event_error)?;
        tracing::info!(
            event_name = "run.started",
            run_id = new_run.run_id.as_str(),
            request_id = new_run.request_id.as_str(),
            agent_id = new_run.agent_id.as_str(),
            agent_version = new_run.agent_version.as_str(),
            attachment = new_run.attachment.as_str(),
            "run started"
        );

        let context = RunContext::new(
            RunMetadata {
                run_id: new_run.run_id.clone(),
                request_id: new_run.request_id.clone(),
                agent_id: new_run.agent_id.clone(),
                agent_version: new_run.agent_version.clone(),
                started_at,
            },
            input,
        )
        .with_templates(Arc::clone(&self.agent.templates));
        let result = Scheduler::new(
            Arc::clone(&self.agent),
            self.executors.clone(),
            self.events.clone(),
            self.limiter.clone(),
        )
        .run(context, stop)
        .await?;

        let terminal = match result {
            SchedulerResult::Ended(TerminalOutcome::Success { output }) => {
                RunTerminal::Completed { output }
            }
            SchedulerResult::Ended(TerminalOutcome::Failure { error }) => RunTerminal::Failed {
                error: RunFailure {
                    kind: FailureKind::Workflow,
                    code: error.code,
                    message: error.message,
                },
            },
            SchedulerResult::Failed(error) => {
                let kind = match error.kind() {
                    RunErrorKind::Node => FailureKind::Node,
                    RunErrorKind::Timeout => FailureKind::Timeout,
                    RunErrorKind::Infrastructure => FailureKind::Infrastructure,
                    RunErrorKind::Stop => {
                        return Err(RunError::infrastructure(
                            "RUN_TERMINAL_INVALID",
                            "scheduler returned a stop error as a failed result",
                        ));
                    }
                };
                RunTerminal::Failed {
                    error: RunFailure {
                        kind,
                        code: error.code().to_string(),
                        message: error.message().to_string(),
                    },
                }
            }
            SchedulerResult::Stopped(error) => match error.stop_reason() {
                Some(StopReason::Cancelled) => RunTerminal::Cancelled {
                    error: StopError {
                        code: error.code().to_string(),
                        message: error.message().to_string(),
                    },
                },
                Some(StopReason::Interrupted) => RunTerminal::Interrupted {
                    error: StopError {
                        code: error.code().to_string(),
                        message: error.message().to_string(),
                    },
                },
                Some(StopReason::TimedOut) => RunTerminal::Failed {
                    error: RunFailure {
                        kind: FailureKind::Timeout,
                        code: error.code().to_string(),
                        message: error.message().to_string(),
                    },
                },
                None => {
                    return Err(RunError::infrastructure(
                        "RUN_TERMINAL_INVALID",
                        "scheduler returned an untyped stop result",
                    ));
                }
            },
        };
        self.finish_terminal(&state, new_run, terminal, started)
            .await
    }

    fn validate_run(&self, run: &NewRun, input: &Value) -> Result<(), RunError> {
        if run.agent_id != self.agent.id || run.agent_version != self.agent.version_hash {
            return Err(RunError::new(
                "RUN_AGENT_MISMATCH",
                "run metadata does not match the compiled agent",
            ));
        }
        if !self.agent.input_schema.is_valid(input) {
            return Err(RunError::new(
                "INPUT_INVALID",
                "input does not match the agent schema",
            ));
        }
        Ok(())
    }

    async fn finish_terminal(
        &self,
        state: &RunState,
        run: &NewRun,
        terminal: RunTerminal,
        started: Instant,
    ) -> Result<RunStatus, RunError> {
        let status = terminal.status();
        let (event_type, code, message, data, output_bytes, error_code, failure_kind) =
            match &terminal {
                RunTerminal::Completed { output } => (
                    RunEventType::RunCompleted,
                    "OK".to_string(),
                    "ok".to_string(),
                    serde_json::to_value(output).map_err(|_| {
                        RunError::new("RUN_OUTPUT_INVALID", "failed to serialize run output")
                    })?,
                    json_size_bytes(output),
                    String::new(),
                    None,
                ),
                RunTerminal::Failed { error } => (
                    RunEventType::RunFailed,
                    error.code.clone(),
                    error.message.clone(),
                    json!({"kind": error.kind}),
                    0,
                    error.code.clone(),
                    Some(error.kind),
                ),
                RunTerminal::Cancelled { error } => (
                    RunEventType::RunCancelled,
                    error.code.clone(),
                    error.message.clone(),
                    json!({}),
                    0,
                    error.code.clone(),
                    None,
                ),
                RunTerminal::Interrupted { error } => (
                    RunEventType::RunInterrupted,
                    error.code.clone(),
                    error.message.clone(),
                    json!({}),
                    0,
                    error.code.clone(),
                    None,
                ),
            };
        let update = TerminalUpdate::new(&run.run_id, Utc::now(), terminal);
        let published = self
            .events
            .publish_terminal(run_scope(run), event_type, update, code, message, data)
            .await
            .map_err(event_error)?;
        let durable = self
            .commit_terminal_state(
                state,
                run,
                published,
                TerminalLogSummary {
                    status,
                    output_bytes,
                    error_code,
                    failure_kind,
                },
            )
            .await?;
        tracing::info!(
            event_name = "run.finished",
            run_id = run.run_id.as_str(),
            request_id = run.request_id.as_str(),
            agent_id = run.agent_id.as_str(),
            agent_version = run.agent_version.as_str(),
            attachment = run.attachment.as_str(),
            status = durable.status.as_str(),
            elapsed_ms = elapsed_ms(started),
            output_bytes = durable.output_bytes,
            error_code = durable.error_code.as_str(),
            failure_kind = durable.failure_kind.map(FailureKind::as_str).unwrap_or(""),
            "run finished"
        );
        Ok(durable.status)
    }

    async fn recover_infrastructure_failure(
        &self,
        state: &RunState,
        run: &NewRun,
        started: Instant,
    ) -> Result<RunStatus, RunError> {
        let message = "runtime infrastructure failed";
        let update = TerminalUpdate::new(
            &run.run_id,
            Utc::now(),
            RunTerminal::Failed {
                error: RunFailure {
                    kind: FailureKind::Infrastructure,
                    code: "INFRASTRUCTURE_FAILURE".to_string(),
                    message: message.to_string(),
                },
            },
        );
        let published = self
            .events
            .recover_terminal(
                run_scope(run),
                RunEventType::RunFailed,
                update,
                "INFRASTRUCTURE_FAILURE",
                message,
                json!({"kind":FailureKind::Infrastructure}),
            )
            .await
            .map_err(event_error)?;
        let durable = self
            .commit_terminal_state(
                state,
                run,
                published,
                TerminalLogSummary {
                    status: RunStatus::Failed,
                    output_bytes: 0,
                    error_code: "INFRASTRUCTURE_FAILURE".to_string(),
                    failure_kind: Some(FailureKind::Infrastructure),
                },
            )
            .await?;
        tracing::info!(
            event_name = "run.finished",
            run_id = run.run_id.as_str(),
            request_id = run.request_id.as_str(),
            agent_id = run.agent_id.as_str(),
            agent_version = run.agent_version.as_str(),
            attachment = run.attachment.as_str(),
            status = durable.status.as_str(),
            elapsed_ms = elapsed_ms(started),
            output_bytes = durable.output_bytes,
            error_code = durable.error_code.as_str(),
            failure_kind = durable.failure_kind.map(FailureKind::as_str).unwrap_or(""),
            "run finished"
        );
        Ok(durable.status)
    }

    async fn commit_terminal_state(
        &self,
        state: &RunState,
        run: &NewRun,
        published: Option<crate::events::protocol::RunEvent>,
        attempted: TerminalLogSummary,
    ) -> Result<TerminalLogSummary, RunError> {
        let durable = if published.is_some() {
            attempted
        } else {
            let record = self
                .repository
                .get_run(&run.run_id)
                .await
                .map_err(history_error)?
                .ok_or_else(|| {
                    RunError::new("RUN_NOT_FOUND", "run not found after terminal race")
                })?;
            terminal_log_summary_from_record(&record)
        };
        state.try_terminal(durable.status).await?;
        Ok(durable)
    }
}

fn terminal_log_summary_from_record(record: &RunRecord) -> TerminalLogSummary {
    let (output_bytes, error_code, failure_kind) = match &record.lifecycle {
        RunLifecycle::Completed { output } => (json_size_bytes(output), String::new(), None),
        RunLifecycle::Failed { error } => (0, error.code.clone(), Some(error.kind)),
        RunLifecycle::Cancelled { error } | RunLifecycle::Interrupted { error } => {
            (0, error.code.clone(), None)
        }
        RunLifecycle::Created | RunLifecycle::Running => (0, String::new(), None),
    };
    TerminalLogSummary {
        status: record.status(),
        output_bytes,
        error_code,
        failure_kind,
    }
}

fn run_scope(run: &NewRun) -> RunEventScope {
    RunEventScope::for_run(
        &run.request_id,
        &run.run_id,
        &run.agent_id,
        &run.agent_version,
    )
}

fn history_error(error: HistoryError) -> RunError {
    RunError::infrastructure(error.code(), error.to_string())
}

fn event_error(error: EventError) -> RunError {
    RunError::infrastructure(error.code(), error.to_string())
}
