use std::collections::BTreeMap;

use chrono::Utc;
use futures::{Stream, StreamExt};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
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
    model::types::{ChatMessage, ChatRequest, ModelClient},
    prompt::{renderer::PromptRenderer, store::PromptStore},
    tools::registry::{ToolContext, ToolRegistry},
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
        let (tx, rx) = mpsc::unbounded_channel();
        let engine = self.clone();
        tokio::spawn(async move {
            engine
                .run_streaming(agent, input, EventSender::new(tx))
                .await;
        });
        UnboundedReceiverStream::new(rx)
    }

    async fn run_streaming(&self, agent: LoadedAgent, input: Value, events: EventSender) {
        let run_id = format!("run_{}", Uuid::new_v4());
        let mut ctx = RunContext {
            run_id,
            agent_id: agent.config.id.clone(),
            started_at: Utc::now(),
            input,
            step_outputs: BTreeMap::new(),
        };
        let store = PromptStore::new(agent.prompts.clone());
        events.emit(&ctx, None, RunEventKind::RunStarted, json!({}));

        for step in &agent.config.steps {
            events.emit(
                &ctx,
                Some(&step.id),
                RunEventKind::StepStarted,
                json!({ "step_type": step.kind }),
            );
            match self
                .execute_step(step, &agent.config.model, &store, &mut ctx, &events)
                .await
            {
                Ok(output) => {
                    ctx.set_step_output(&step.id, output.clone());
                    events.emit(
                        &ctx,
                        Some(&step.id),
                        RunEventKind::StepCompleted,
                        json!({ "output": output }),
                    );
                }
                Err(err) => {
                    events.emit(
                        &ctx,
                        Some(&step.id),
                        RunEventKind::Error,
                        json!({ "message": err.to_string() }),
                    );
                    return;
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
        events.emit(
            &ctx,
            None,
            RunEventKind::RunCompleted,
            json!({ "output": output }),
        );
    }

    async fn execute_step(
        &self,
        step: &StepConfig,
        model_config: &ModelConfig,
        store: &PromptStore,
        ctx: &mut RunContext,
        events: &EventSender,
    ) -> Result<Value, AppError> {
        match step.kind {
            StepKind::Prompt => {
                let template =
                    resolve_prompt(step.prompt.as_deref(), step.prompt_ref.as_deref(), store)?;
                let rendered = self.renderer.render(template, &ctx.template_data())?;
                Ok(Value::String(rendered))
            }
            StepKind::Llm => {
                self.execute_llm_step(step, model_config, store, ctx, events)
                    .await
            }
            StepKind::Tool => self.execute_tool_step(step, ctx, events).await,
        }
    }

    async fn execute_llm_step(
        &self,
        step: &StepConfig,
        model_config: &ModelConfig,
        store: &PromptStore,
        ctx: &RunContext,
        events: &EventSender,
    ) -> Result<Value, AppError> {
        let mut messages = Vec::new();

        if let Some(system_template) = resolve_optional_prompt(
            step.system_prompt.as_deref(),
            step.system_prompt_ref.as_deref(),
            store,
        )? {
            let system_prompt = self
                .renderer
                .render(&system_template, &ctx.template_data())?;
            messages.push(ChatMessage {
                role: "system".to_string(),
                content: system_prompt,
            });
        }

        let prompt_template =
            resolve_prompt(step.prompt.as_deref(), step.prompt_ref.as_deref(), store)?;
        let user_prompt = self
            .renderer
            .render(prompt_template, &ctx.template_data())?;
        messages.push(ChatMessage {
            role: "user".to_string(),
            content: user_prompt,
        });

        let request = ChatRequest {
            model: model_config
                .model
                .clone()
                .unwrap_or_else(|| model_config.provider.clone()),
            messages,
            temperature: model_config.temperature,
            max_tokens: model_config.max_tokens,
        };

        let mut stream = self.model.stream_chat(request).await?;
        let mut output = String::new();

        while let Some(chunk) = stream.next().await {
            let delta = chunk?;
            if delta.is_empty() {
                continue;
            }
            output.push_str(&delta);
            events.emit(
                ctx,
                Some(&step.id),
                RunEventKind::TokenDelta,
                json!({ "delta": delta }),
            );
        }

        Ok(Value::String(output))
    }

    async fn execute_tool_step(
        &self,
        step: &StepConfig,
        ctx: &RunContext,
        events: &EventSender,
    ) -> Result<Value, AppError> {
        let tool_name = step.tool.as_deref().ok_or_else(|| {
            AppError::Config(format!("step '{}' type 'tool' requires tool", step.id))
        })?;
        let tool = self
            .tools
            .get(tool_name)
            .ok_or_else(|| AppError::Run(format!("tool '{}' is not registered", tool_name)))?;

        events.emit(
            ctx,
            Some(&step.id),
            RunEventKind::ToolCallStarted,
            json!({ "tool": tool_name }),
        );
        let output = tool
            .call(
                step.args.clone(),
                ToolContext {
                    run_id: ctx.run_id.clone(),
                },
            )
            .await?;
        events.emit(
            ctx,
            Some(&step.id),
            RunEventKind::ToolCallCompleted,
            json!({ "tool": tool_name, "output": output.clone() }),
        );
        Ok(output)
    }
}

#[derive(Clone)]
struct EventSender {
    tx: mpsc::UnboundedSender<RunEvent>,
}

impl EventSender {
    fn new(tx: mpsc::UnboundedSender<RunEvent>) -> Self {
        Self { tx }
    }

    fn emit(&self, ctx: &RunContext, step_id: Option<&str>, kind: RunEventKind, payload: Value) {
        let _ = self.tx.send(RunEvent {
            kind,
            run_id: ctx.run_id.clone(),
            agent_id: ctx.agent_id.clone(),
            step_id: step_id.map(str::to_string),
            timestamp: Utc::now(),
            payload,
        });
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

fn resolve_optional_prompt(
    inline: Option<&str>,
    prompt_ref: Option<&str>,
    store: &PromptStore,
) -> Result<Option<String>, AppError> {
    match (inline, prompt_ref) {
        (Some(inline), None) => Ok(Some(inline.to_string())),
        (None, Some(prompt_ref)) => store
            .resolve_ref(prompt_ref)
            .map(|value| Some(value.to_string())),
        (None, None) => Ok(None),
        (Some(_), Some(_)) => Err(AppError::Config(
            "system prompt requires either system_prompt or system_prompt_ref".to_string(),
        )),
    }
}
