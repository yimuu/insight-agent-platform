use std::{sync::Arc, time::Instant};

use chrono::Utc;
use serde_json::{json, Value};
use tokio::{sync::Semaphore, time::Instant as TokioInstant};

use crate::{
    dsl::vnext::{compiler::CompiledWorkflow, raw::is_valid_error_code},
    events::{
        hub::{EventError, EventHub, TerminalResolution},
        protocol::{RunEventScope, RunEventType},
    },
    history::{
        repository::{HistoryError, RunRepository},
        types::{
            NewRun, RunLifecycle, RunRecord, RunStatus, RunTerminal, StopError, TerminalUpdate,
        },
    },
    observability::{elapsed_ms, json_size_bytes},
    outcome::{FailureKind, RunFailure, TerminalOutcome},
};

const INFRASTRUCTURE_FAILURE_CODE: &str = "INFRASTRUCTURE_FAILURE";
const INFRASTRUCTURE_FAILURE_MESSAGE: &str = "runtime infrastructure failed";
const OPERATION_TIMEOUT_CODE: &str = "OPERATION_TIMEOUT";

use super::{
    scope_scheduler::{ScopeScheduler, ScopeSchedulerConfig},
    RunError, RunErrorKind, RunExecutionResult, RunMetadata, RunState, StopReason, StopSignal,
};

pub struct RunCoordinator {
    agent: Arc<CompiledWorkflow>,
    events: EventHub,
    repository: Arc<dyn RunRepository>,
    global_operation_permits: Arc<Semaphore>,
    scheduler_config: ScopeSchedulerConfig,
}

struct TerminalLogSummary {
    status: RunStatus,
    output_bytes: usize,
    error_code: String,
    failure_kind: Option<FailureKind>,
}

impl RunCoordinator {
    pub fn new(
        agent: Arc<CompiledWorkflow>,
        events: EventHub,
        repository: Arc<dyn RunRepository>,
        global_operation_permits: Arc<Semaphore>,
        scheduler_config: ScopeSchedulerConfig,
    ) -> Self {
        Self {
            agent,
            events,
            repository,
            global_operation_permits,
            scheduler_config,
        }
    }

    pub async fn execute(
        &self,
        new_run: NewRun,
        input: Value,
        stop: StopSignal,
        execution_deadline: TokioInstant,
    ) -> Result<RunStatus, RunError> {
        bind_execution_deadline(&stop, execution_deadline)?;
        self.validate_run(&new_run, &input)?;
        self.repository
            .create_run(new_run.clone())
            .await
            .map_err(history_error)?;
        self.execute_managed(
            new_run,
            input,
            stop,
            Arc::new(RunState::new()),
            execution_deadline,
        )
        .await
    }

    pub(crate) async fn execute_existing(
        &self,
        new_run: NewRun,
        input: Value,
        stop: StopSignal,
        state: Arc<RunState>,
        execution_deadline: TokioInstant,
    ) -> Result<RunStatus, RunError> {
        bind_execution_deadline(&stop, execution_deadline)?;
        self.validate_run(&new_run, &input)?;
        self.execute_managed(new_run, input, stop, state, execution_deadline)
            .await
    }

    async fn execute_managed(
        &self,
        new_run: NewRun,
        input: Value,
        stop: StopSignal,
        state: Arc<RunState>,
        execution_deadline: TokioInstant,
    ) -> Result<RunStatus, RunError> {
        let started = Instant::now();
        match self
            .execute_inner(
                &new_run,
                input,
                stop,
                Arc::clone(&state),
                started,
                execution_deadline,
            )
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
        execution_deadline: TokioInstant,
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

        let metadata = RunMetadata {
            run_id: new_run.run_id.clone(),
            request_id: new_run.request_id.clone(),
            agent_id: new_run.agent_id.clone(),
            agent_version: new_run.agent_version.clone(),
            started_at,
            execution_deadline,
        };
        let result = ScopeScheduler::new(
            Arc::clone(&self.agent),
            Arc::clone(&self.global_operation_permits),
            self.events.clone(),
            self.scheduler_config.clone(),
        )
        .run(metadata, input, stop)
        .await?;

        let terminal = match result {
            RunExecutionResult::Ended(TerminalOutcome::Success { output }) => {
                RunTerminal::Completed { output }
            }
            RunExecutionResult::Ended(TerminalOutcome::Failure { error }) => RunTerminal::Failed {
                error: RunFailure {
                    kind: FailureKind::Workflow,
                    code: error.code,
                    message: error.message,
                },
            },
            RunExecutionResult::Failed(error) => RunTerminal::Failed {
                error: public_run_failure(&new_run.run_id, &error)?,
            },
            RunExecutionResult::Stopped(error) => match error.stop_reason() {
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
        if run.agent_id != self.agent.ir.metadata.id.as_str()
            || run.agent_version != self.agent.version_hash
        {
            return Err(RunError::infrastructure(
                "RUN_AGENT_MISMATCH",
                "run metadata does not match the compiled agent",
            ));
        }
        if !self.agent.input_validator().is_valid(input) {
            return Err(RunError::infrastructure(
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
        let attempted = terminal_log_summary_from_terminal(&terminal);
        let update = TerminalUpdate::new(&run.run_id, Utc::now(), terminal);
        let resolution = self
            .events
            .publish_terminal(run_scope(run), update)
            .await
            .map_err(event_error)?;
        let durable = self
            .commit_terminal_state(state, run, resolution, attempted)
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
        let terminal = RunTerminal::Failed {
            error: public_infrastructure_failure(),
        };
        let attempted = terminal_log_summary_from_terminal(&terminal);
        let update = TerminalUpdate::new(&run.run_id, Utc::now(), terminal);
        let resolution = self
            .events
            .recover_terminal(run_scope(run), update)
            .await
            .map_err(event_error)?;
        let durable = self
            .commit_terminal_state(state, run, resolution, attempted)
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

    pub(crate) async fn recover_task_panic(
        &self,
        state: &RunState,
        run: &NewRun,
        started: Instant,
    ) -> Result<RunStatus, RunError> {
        self.recover_infrastructure_failure(state, run, started)
            .await
    }

    async fn commit_terminal_state(
        &self,
        state: &RunState,
        run: &NewRun,
        resolution: TerminalResolution,
        attempted: TerminalLogSummary,
    ) -> Result<TerminalLogSummary, RunError> {
        let durable = match resolution {
            TerminalResolution::Requested(_) => attempted,
            TerminalResolution::Authoritative(_) => {
                let record = self
                    .repository
                    .get_run(&run.run_id)
                    .await
                    .map_err(history_error)?
                    .ok_or_else(|| {
                        RunError::infrastructure(
                            "RUN_NOT_FOUND",
                            "run not found after terminal race",
                        )
                    })?;
                terminal_log_summary_from_record(&record)
            }
        };
        state.try_terminal(durable.status).await?;
        Ok(durable)
    }
}

fn bind_execution_deadline(
    stop: &StopSignal,
    execution_deadline: TokioInstant,
) -> Result<(), RunError> {
    stop.bind_deadline(execution_deadline).map_err(|_| {
        RunError::infrastructure(
            "RUN_STOP_DEADLINE_INVALID",
            "Run stop signal was bound to a different execution deadline",
        )
    })
}

fn terminal_log_summary_from_terminal(terminal: &RunTerminal) -> TerminalLogSummary {
    match terminal {
        RunTerminal::Completed { output } => TerminalLogSummary {
            status: RunStatus::Completed,
            output_bytes: json_size_bytes(output),
            error_code: String::new(),
            failure_kind: None,
        },
        RunTerminal::Failed { error } => TerminalLogSummary {
            status: RunStatus::Failed,
            output_bytes: 0,
            error_code: error.code.clone(),
            failure_kind: Some(error.kind),
        },
        RunTerminal::Cancelled { error } => TerminalLogSummary {
            status: RunStatus::Cancelled,
            output_bytes: 0,
            error_code: error.code.clone(),
            failure_kind: None,
        },
        RunTerminal::Interrupted { error } => TerminalLogSummary {
            status: RunStatus::Interrupted,
            output_bytes: 0,
            error_code: error.code.clone(),
            failure_kind: None,
        },
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

fn public_run_failure(run_id: &str, error: &RunError) -> Result<RunFailure, RunError> {
    let (kind, code, public_message) = match error.kind() {
        RunErrorKind::Operation => {
            if !is_valid_error_code(error.code()) {
                tracing::error!(
                    run_id,
                    failure_kind = "infrastructure",
                    "root operation returned an invalid public error code"
                );
                return Ok(public_infrastructure_failure());
            }
            tracing::warn!(
                run_id,
                code = error.code(),
                failure_kind = "operation",
                "root operation failed"
            );
            (FailureKind::Operation, error.code(), "operation failed")
        }
        RunErrorKind::Timeout => {
            if error.code() != OPERATION_TIMEOUT_CODE {
                tracing::error!(
                    run_id,
                    failure_kind = "infrastructure",
                    "root operation returned an invalid timeout error code"
                );
                return Ok(public_infrastructure_failure());
            }
            tracing::warn!(
                run_id,
                code = error.code(),
                failure_kind = "timeout",
                "root operation timed out"
            );
            (FailureKind::Timeout, error.code(), "operation timed out")
        }
        RunErrorKind::Infrastructure => {
            tracing::error!(
                run_id,
                failure_kind = "infrastructure",
                "root operation reported an infrastructure failure"
            );
            return Ok(public_infrastructure_failure());
        }
        RunErrorKind::Stop => {
            return Err(RunError::infrastructure(
                "RUN_TERMINAL_INVALID",
                "scheduler returned a stop error as a failed result",
            ));
        }
    };
    Ok(RunFailure {
        kind,
        code: code.to_string(),
        message: public_message.to_string(),
    })
}

fn public_infrastructure_failure() -> RunFailure {
    RunFailure {
        kind: FailureKind::Infrastructure,
        code: INFRASTRUCTURE_FAILURE_CODE.to_string(),
        message: INFRASTRUCTURE_FAILURE_MESSAGE.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Write},
        sync::{Arc, Mutex},
    };

    use tracing_subscriber::fmt::MakeWriter;

    use super::*;

    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    struct CapturedWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for CapturedWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CapturedWriter(Arc::clone(&self.0))
        }
    }

    impl CapturedLogs {
        fn text(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    #[test]
    fn root_operation_failure_keeps_diagnostic_out_of_public_terminal() {
        let failure = public_run_failure(
            "run_private_diagnostic",
            &RunError::operation(
                "ACTION_PRIVATE_FAILURE",
                "database password and private provider diagnostic",
            ),
        )
        .unwrap();

        assert_eq!(failure.kind, FailureKind::Operation);
        assert_eq!(failure.code, "ACTION_PRIVATE_FAILURE");
        assert_eq!(failure.message, "operation failed");
        assert!(!failure.message.contains("password"));
        assert!(!failure.message.contains("provider"));
    }

    #[test]
    fn invalid_or_internal_run_error_codes_become_one_public_infrastructure_failure() {
        for error in [
            RunError::operation(
                "invalid-private-code",
                "private operation payload must not escape",
            ),
            RunError::infrastructure(
                "PRIVATE_INTERNAL_FAILURE",
                "private infrastructure payload must not escape",
            ),
        ] {
            let failure = public_run_failure("run_invalid_code", &error).unwrap();
            assert_eq!(failure.kind, FailureKind::Infrastructure);
            assert_eq!(failure.code, INFRASTRUCTURE_FAILURE_CODE);
            assert_eq!(failure.message, INFRASTRUCTURE_FAILURE_MESSAGE);
            assert!(!failure.message.contains("private"));
            assert!(is_valid_error_code(&failure.code));
        }
    }

    #[test]
    fn operation_timeout_keeps_only_the_frozen_public_timeout_code() {
        let failure =
            public_run_failure("run_operation_timeout", &RunError::operation_timeout()).unwrap();

        assert_eq!(failure.kind, FailureKind::Timeout);
        assert_eq!(failure.code, OPERATION_TIMEOUT_CODE);
        assert_eq!(failure.message, "operation timed out");
    }

    #[test]
    fn root_operation_failure_log_excludes_private_diagnostic() {
        const PRIVATE_DIAGNOSTIC: &str = "PRIVATE_ROOT_OPERATION_DIAGNOSTIC_MUST_NOT_REACH_LOGS";
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .with_writer(logs.clone())
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            public_run_failure(
                "run_log_redaction",
                &RunError::operation("ACTION_PRIVATE_FAILURE", PRIVATE_DIAGNOSTIC),
            )
            .unwrap();
        });

        let rendered = logs.text();
        assert!(rendered.contains("run_log_redaction"));
        assert!(rendered.contains("ACTION_PRIVATE_FAILURE"));
        assert!(rendered.contains("failure_kind=\"operation\""));
        assert!(!rendered.contains(PRIVATE_DIAGNOSTIC));
    }
}
