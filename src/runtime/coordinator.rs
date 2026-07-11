use std::sync::Arc;

use chrono::Utc;
use serde_json::{json, Value};

use crate::{
    dsl::compiled::{CompiledAgent, NodeTransition, RunOutput},
    events::{
        hub::{EventError, EventHub},
        protocol::{RunEventScope, RunEventType},
    },
    history::{
        repository::{HistoryError, RunRepository},
        types::{NewRun, RunStatus, TerminalUpdate},
    },
    nodes::registry::NodeExecutorRegistry,
};

use super::{
    execute_node, ExecutionLimiter, NodeExecutionFailure, RunContext, RunError, RunMetadata,
    RunState, StopSignal,
};

pub struct RunCoordinator {
    agent: Arc<CompiledAgent>,
    executors: NodeExecutorRegistry,
    events: EventHub,
    repository: Arc<dyn RunRepository>,
    limiter: ExecutionLimiter,
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
        match self
            .execute_inner(&new_run, input, stop, Arc::clone(&state))
            .await
        {
            Ok(status) => Ok(status),
            Err(error) => {
                tracing::error!(
                    run_id = new_run.run_id,
                    code = error.code(),
                    "run infrastructure failed; recovering durable terminal state"
                );
                self.recover_infrastructure_failure(&state, &new_run).await
            }
        }
    }

    async fn execute_inner(
        &self,
        new_run: &NewRun,
        input: Value,
        stop: StopSignal,
        state: Arc<RunState>,
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

        let mut context = RunContext::new(
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
        let mut current = self.agent.entry.clone();

        loop {
            if let Some(reason) = stop.reason() {
                return self
                    .finish_error(&state, new_run, RunError::stopped(reason))
                    .await;
            }
            let node = self.agent.nodes.get(&current).cloned().ok_or_else(|| {
                RunError::new(
                    "NODE_NOT_FOUND",
                    format!("compiled node '{current}' was not found"),
                )
            })?;
            let outcome = match execute_node(
                node.clone(),
                context,
                self.executors.clone(),
                self.events.clone(),
                stop.clone(),
                self.limiter.clone(),
            )
            .await
            {
                Ok(result) => {
                    context = result.context;
                    context.set_node_output(&result.node_id, result.outcome.output.clone());
                    result.outcome
                }
                Err(NodeExecutionFailure::Node { error, .. })
                | Err(NodeExecutionFailure::Stop { error, .. }) => {
                    return self.finish_error(&state, new_run, error).await;
                }
                Err(NodeExecutionFailure::Infrastructure(error)) => return Err(error),
            };

            match outcome.transition {
                NodeTransition::Next => {
                    current = node.next.clone().ok_or_else(|| {
                        RunError::new(
                            "NODE_NEXT_MISSING",
                            format!("node '{}' completed without a next node", node.id),
                        )
                    })?;
                }
                NodeTransition::Goto(target) => current = target,
                NodeTransition::ActivateFork => {
                    return Err(RunError::new(
                        "NODE_FORK_UNSUPPORTED",
                        "fork activation is not supported by the sequential coordinator",
                    ));
                }
                NodeTransition::Complete(output) => {
                    return self.complete(&state, new_run, output).await;
                }
            }
        }
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

    async fn complete(
        &self,
        state: &RunState,
        run: &NewRun,
        output: RunOutput,
    ) -> Result<RunStatus, RunError> {
        let update = TerminalUpdate::new(
            &run.run_id,
            RunStatus::Completed,
            Utc::now(),
            Some(output.clone()),
            None,
            None,
        )
        .map_err(|error| RunError::infrastructure(error.code(), error.to_string()))?;
        let data = serde_json::to_value(&output)
            .map_err(|_| RunError::new("RUN_OUTPUT_INVALID", "failed to serialize run output"))?;
        let published = self
            .events
            .publish_terminal(
                run_scope(run),
                RunEventType::RunCompleted,
                update,
                "OK",
                "ok",
                data,
            )
            .await
            .map_err(event_error)?;
        self.commit_terminal_state(state, run, published, RunStatus::Completed)
            .await
    }

    async fn finish_error(
        &self,
        state: &RunState,
        run: &NewRun,
        error: RunError,
    ) -> Result<RunStatus, RunError> {
        let (status, event_type) = match error.code() {
            "RUN_CANCELLED" => (RunStatus::Cancelled, RunEventType::RunCancelled),
            "RUN_INTERRUPTED" => (RunStatus::Interrupted, RunEventType::RunInterrupted),
            _ => (RunStatus::Failed, RunEventType::RunFailed),
        };
        let update = TerminalUpdate::new(
            &run.run_id,
            status,
            Utc::now(),
            None,
            Some(error.code().to_string()),
            Some(error.message().to_string()),
        )
        .map_err(|type_error| {
            RunError::infrastructure(type_error.code(), type_error.to_string())
        })?;
        let published = self
            .events
            .publish_terminal(
                run_scope(run),
                event_type,
                update,
                error.code(),
                error.message(),
                json!({}),
            )
            .await
            .map_err(event_error)?;
        self.commit_terminal_state(state, run, published, status)
            .await
    }

    async fn recover_infrastructure_failure(
        &self,
        state: &RunState,
        run: &NewRun,
    ) -> Result<RunStatus, RunError> {
        let message = "runtime infrastructure failed";
        let update = TerminalUpdate::new(
            &run.run_id,
            RunStatus::Failed,
            Utc::now(),
            None,
            Some("INFRASTRUCTURE_FAILURE".to_string()),
            Some(message.to_string()),
        )
        .map_err(|error| RunError::infrastructure(error.code(), error.to_string()))?;
        let published = self
            .events
            .recover_terminal(
                run_scope(run),
                RunEventType::RunFailed,
                update,
                "INFRASTRUCTURE_FAILURE",
                message,
                json!({}),
            )
            .await
            .map_err(event_error)?;
        self.commit_terminal_state(state, run, published, RunStatus::Failed)
            .await
    }

    async fn commit_terminal_state(
        &self,
        state: &RunState,
        run: &NewRun,
        published: Option<crate::events::protocol::RunEvent>,
        expected: RunStatus,
    ) -> Result<RunStatus, RunError> {
        let durable_status = if published.is_some() {
            expected
        } else {
            self.repository
                .get_run(&run.run_id)
                .await
                .map_err(history_error)?
                .ok_or_else(|| RunError::new("RUN_NOT_FOUND", "run not found after terminal race"))?
                .status
        };
        state.try_terminal(durable_status).await?;
        Ok(durable_status)
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
