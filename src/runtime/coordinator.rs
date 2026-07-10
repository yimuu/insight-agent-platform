use std::sync::Arc;

use chrono::Utc;
use serde_json::{json, Value};
use tokio::time::sleep;

use crate::{
    dsl::compiled::{CompiledAgent, NodeTransition, RunOutput},
    events::{
        hub::{EventError, EventHub},
        protocol::{RunEventScope, RunEventType},
    },
    history::{
        repository::{HistoryError, RunRepository},
        types::{NewRun, NodeOutputRecord, RunStatus, TerminalUpdate},
    },
    nodes::registry::NodeExecutorRegistry,
};

use super::{ExecutionControl, RunContext, RunError, RunMetadata, RunState, StopSignal};

pub struct RunCoordinator {
    agent: Arc<CompiledAgent>,
    executors: NodeExecutorRegistry,
    events: EventHub,
    repository: Arc<dyn RunRepository>,
}

impl RunCoordinator {
    pub fn new(
        agent: Arc<CompiledAgent>,
        executors: NodeExecutorRegistry,
        events: EventHub,
        repository: Arc<dyn RunRepository>,
    ) -> Self {
        Self {
            agent,
            executors,
            events,
            repository,
        }
    }

    pub async fn execute(
        &self,
        new_run: NewRun,
        input: Value,
        stop: StopSignal,
    ) -> Result<RunStatus, RunError> {
        self.validate_run(&new_run, &input)?;
        let state = RunState::new();
        self.repository
            .create_run(new_run.clone())
            .await
            .map_err(history_error)?;
        self.events
            .publish(
                run_scope(&new_run),
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
                run_scope(&new_run),
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
                    .finish_error(&state, &new_run, None, RunError::stopped(reason))
                    .await;
            }
            let node = self.agent.nodes.get(&current).ok_or_else(|| {
                RunError::new(
                    "NODE_NOT_FOUND",
                    format!("compiled node '{current}' was not found"),
                )
            })?;
            let node_scope = run_scope(&new_run).for_node(&node.id);
            self.events
                .publish(
                    node_scope.clone(),
                    RunEventType::NodeStarted,
                    json!({"type":node.kind}),
                )
                .await
                .map_err(event_error)?;

            let executor = match self.executors.resolve(&node.kind) {
                Ok(executor) => executor,
                Err(error) => {
                    return self
                        .finish_error(&state, &new_run, Some(&node.id), error)
                        .await;
                }
            };
            let emitter_events = self.events.clone();
            let emitter_scope = node_scope.clone();
            let control = ExecutionControl::new(stop.clone(), node.timeout, move |content| {
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
            .with_content_enabled(node.emit == crate::dsl::EmitPolicy::Content);
            let outcome = {
                let execution = executor.execute(node, &context, &control);
                tokio::pin!(execution);
                tokio::select! {
                    result = &mut execution => result,
                    _ = control.stopped() => Err(stopped_error(&control)),
                    _ = sleep(control.remaining()) => Err(RunError::timeout()),
                }
            };
            let outcome = match outcome {
                Ok(outcome) => outcome,
                Err(error) => {
                    return self
                        .finish_error(&state, &new_run, Some(&node.id), error)
                        .await;
                }
            };
            if let Some(reason) = stop.reason() {
                return self
                    .finish_error(&state, &new_run, Some(&node.id), RunError::stopped(reason))
                    .await;
            }

            self.events
                .put_node_output(NodeOutputRecord {
                    run_id: new_run.run_id.clone(),
                    node_id: node.id.clone(),
                    output: outcome.output.clone(),
                    completed_at: Utc::now(),
                })
                .await
                .map_err(event_error)?;
            self.events
                .publish(
                    node_scope,
                    RunEventType::NodeCompleted,
                    json!({"output":outcome.output}),
                )
                .await
                .map_err(event_error)?;
            self.events.flush().await.map_err(event_error)?;
            context.set_node_output(&node.id, outcome.output);

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
                NodeTransition::Complete(output) => {
                    return self.complete(&state, &new_run, output).await;
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
        if !state.try_terminal(RunStatus::Completed).await? {
            return Ok(state.status().await);
        }
        let update = TerminalUpdate::new(
            &run.run_id,
            RunStatus::Completed,
            Utc::now(),
            Some(output.clone()),
            None,
            None,
        )
        .map_err(|error| RunError::new(error.code(), error.to_string()))?;
        let data = serde_json::to_value(&output)
            .map_err(|_| RunError::new("RUN_OUTPUT_INVALID", "failed to serialize run output"))?;
        self.events
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
        Ok(RunStatus::Completed)
    }

    async fn finish_error(
        &self,
        state: &RunState,
        run: &NewRun,
        node_id: Option<&str>,
        error: RunError,
    ) -> Result<RunStatus, RunError> {
        if let Some(node_id) = node_id {
            self.events
                .publish_error(
                    run_scope(run).for_node(node_id),
                    RunEventType::NodeFailed,
                    error.code(),
                    error.message(),
                    json!({}),
                )
                .await
                .map_err(event_error)?;
        }
        let (status, event_type) = match error.code() {
            "RUN_CANCELLED" => (RunStatus::Cancelled, RunEventType::RunCancelled),
            "RUN_INTERRUPTED" => (RunStatus::Interrupted, RunEventType::RunInterrupted),
            _ => (RunStatus::Failed, RunEventType::RunFailed),
        };
        if !state.try_terminal(status).await? {
            return Ok(state.status().await);
        }
        let update = TerminalUpdate::new(
            &run.run_id,
            status,
            Utc::now(),
            None,
            Some(error.code().to_string()),
            Some(error.message().to_string()),
        )
        .map_err(|type_error| RunError::new(type_error.code(), type_error.to_string()))?;
        self.events
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
        Ok(status)
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

fn stopped_error(control: &ExecutionControl) -> RunError {
    control
        .stop_reason()
        .map(RunError::stopped)
        .unwrap_or_else(|| RunError::new("RUN_STOPPED", "run stopped"))
}

fn history_error(error: HistoryError) -> RunError {
    RunError::new(error.code(), error.to_string())
}

fn event_error(error: EventError) -> RunError {
    RunError::new(error.code(), error.to_string())
}
