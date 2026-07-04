use std::collections::BTreeMap;

use chrono::Utc;
use futures::{stream, Stream, StreamExt};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::{
    agent::{
        config::{ModelConfig, StepConfig, StepKind},
        loader::LoadedAgent,
    },
    engine::{
        context::RunContext,
        event::{RunEvent, RunEventKind},
    },
    error::AppError,
    model::types::ModelClient,
    prompt::{renderer::PromptRenderer, store::PromptStore},
    tools::registry::ToolRegistry,
};

#[derive(Clone)]
pub struct RunEngine<M: ModelClient> {
    model: M,
    tools: ToolRegistry,
    renderer: PromptRenderer,
}

impl<M: ModelClient> RunEngine<M> {
    pub fn new(model: M, tools: ToolRegistry) -> Self {
        Self {
            model,
            tools,
            renderer: PromptRenderer::new(),
        }
    }

    pub fn run(&self, agent: LoadedAgent, input: Value) -> impl Stream<Item = RunEvent> {
        let engine = self.clone();
        stream::once(async move { engine.run_collect(agent, input).await }).flat_map(stream::iter)
    }

    async fn run_collect(&self, agent: LoadedAgent, input: Value) -> Vec<RunEvent> {
        let run_id = format!("run_{}", Uuid::new_v4());
        let mut ctx = RunContext {
            run_id: run_id.clone(),
            agent_id: agent.config.id.clone(),
            started_at: Utc::now(),
            input,
            step_outputs: BTreeMap::new(),
        };
        let mut events = vec![self.event(&ctx, None, RunEventKind::RunStarted, json!({}))];
        let store = PromptStore::new(agent.prompts.clone());

        for step in &agent.config.steps {
            events.push(self.event(
                &ctx,
                Some(&step.id),
                RunEventKind::StepStarted,
                json!({ "step_type": step.kind }),
            ));
            match self
                .execute_step(step, &agent.config.model, &store, &mut ctx, &mut events)
                .await
            {
                Ok(output) => {
                    ctx.set_step_output(&step.id, output.clone());
                    events.push(self.event(
                        &ctx,
                        Some(&step.id),
                        RunEventKind::StepCompleted,
                        json!({ "output": output }),
                    ));
                }
                Err(err) => {
                    events.push(self.event(
                        &ctx,
                        Some(&step.id),
                        RunEventKind::Error,
                        json!({ "message": err.to_string() }),
                    ));
                    return events;
                }
            }
        }

        let output = agent
            .config
            .steps
            .last()
            .and_then(|step| ctx.step_outputs.get(&step.id))
            .cloned()
            .unwrap_or(Value::Null);
        events.push(self.event(
            &ctx,
            None,
            RunEventKind::RunCompleted,
            json!({ "output": output }),
        ));
        events
    }

    async fn execute_step(
        &self,
        step: &StepConfig,
        model_config: &ModelConfig,
        store: &PromptStore,
        ctx: &mut RunContext,
        _events: &mut Vec<RunEvent>,
    ) -> Result<Value, AppError> {
        let _ = &self.model;
        let _ = &self.tools;

        match step.kind {
            StepKind::Prompt => {
                let template =
                    resolve_prompt(step.prompt.as_deref(), step.prompt_ref.as_deref(), store)?;
                let rendered = self.renderer.render(template, &ctx.template_data())?;
                Ok(Value::String(rendered))
            }
            StepKind::Llm => Err(AppError::Run(format!(
                "llm step '{}' is not available until the model task is completed for provider '{}'",
                step.id, model_config.provider
            ))),
            StepKind::Tool => Err(AppError::Run(format!(
                "tool step '{}' is not available until the tool registry task is completed",
                step.id
            ))),
        }
    }

    fn event(
        &self,
        ctx: &RunContext,
        step_id: Option<&str>,
        kind: RunEventKind,
        payload: Value,
    ) -> RunEvent {
        RunEvent {
            kind,
            run_id: ctx.run_id.clone(),
            agent_id: ctx.agent_id.clone(),
            step_id: step_id.map(str::to_string),
            timestamp: Utc::now(),
            payload,
        }
    }
}

fn resolve_prompt<'a>(
    inline: Option<&'a str>,
    prompt_ref: Option<&str>,
    store: &'a PromptStore,
) -> Result<&'a str, AppError> {
    if let Some(inline) = inline {
        return Ok(inline);
    }
    if let Some(prompt_ref) = prompt_ref {
        return store.resolve_ref(prompt_ref);
    }
    Err(AppError::Config(
        "step requires prompt or prompt_ref".to_string(),
    ))
}
