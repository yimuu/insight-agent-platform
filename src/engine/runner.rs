use std::{
    collections::BTreeMap,
    pin::Pin,
    task::{Context, Poll},
};

use chrono::Utc;
use futures::{Stream, StreamExt};
use serde_json::{json, Value};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
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
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let engine = self.clone();
        let task = tokio::spawn(async move {
            engine
                .run_streaming(agent, input, EventSender::new(tx, cancel_rx))
                .await;
        });
        RunEventStream::new(UnboundedReceiverStream::new(rx), cancel_tx, task)
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
        if !events.emit(&ctx, None, RunEventKind::RunStarted, json!({})) {
            return;
        }

        for step in &agent.config.steps {
            if !events.emit(
                &ctx,
                Some(&step.id),
                RunEventKind::StepStarted,
                json!({ "step_type": step.kind }),
            ) {
                return;
            }
            match self
                .execute_step(step, &agent.config.model, &store, &mut ctx, &events)
                .await
            {
                Ok(Some(output)) => {
                    ctx.set_step_output(&step.id, output.clone());
                    if !events.emit(
                        &ctx,
                        Some(&step.id),
                        RunEventKind::StepCompleted,
                        json!({ "output": output }),
                    ) {
                        return;
                    }
                }
                Ok(None) => return,
                Err(err) => {
                    let _ = events.emit(
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
        let _ = events.emit(
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
    ) -> Result<Option<Value>, AppError> {
        if events.is_closed() {
            return Ok(None);
        }

        match step.kind {
            StepKind::Prompt => {
                let template =
                    resolve_prompt(step.prompt.as_deref(), step.prompt_ref.as_deref(), store)?;
                let rendered = self.renderer.render(template, &ctx.template_data())?;
                Ok(Some(Value::String(rendered)))
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
    ) -> Result<Option<Value>, AppError> {
        if events.is_closed() {
            return Ok(None);
        }

        let mut messages = Vec::new();

        if let Some(system_template) = resolve_optional_prompt(
            step.system_prompt.as_deref(),
            step.system_prompt_ref.as_deref(),
            store,
        )? {
            let system_prompt = self
                .renderer
                .render(&system_template, &ctx.template_data())?;
            messages.push(ChatMessage::text("system", system_prompt));
        }

        let prompt_template =
            resolve_prompt(step.prompt.as_deref(), step.prompt_ref.as_deref(), store)?;
        let user_prompt = self
            .renderer
            .render(prompt_template, &ctx.template_data())?;
        messages.push(ChatMessage::text("user", user_prompt));

        let request = ChatRequest {
            model: model_config.model.clone().unwrap_or_default(),
            messages,
            temperature: model_config.temperature,
            max_tokens: model_config.max_tokens,
        };

        if events.is_closed() {
            return Ok(None);
        }

        let mut stream = tokio::select! {
            _ = events.closed() => return Ok(None),
            result = self.model.stream_chat(request) => result?,
        };
        let mut output = String::new();

        loop {
            let next_chunk = tokio::select! {
                _ = events.closed() => return Ok(None),
                chunk = stream.next() => chunk,
            };

            let Some(chunk) = next_chunk else {
                break;
            };

            let delta = chunk?;
            if delta.is_empty() {
                continue;
            }
            output.push_str(&delta);
            if !events.emit(
                ctx,
                Some(&step.id),
                RunEventKind::TokenDelta,
                json!({ "delta": delta }),
            ) {
                return Ok(None);
            }
        }

        Ok(Some(Value::String(output)))
    }

    async fn execute_tool_step(
        &self,
        step: &StepConfig,
        ctx: &RunContext,
        events: &EventSender,
    ) -> Result<Option<Value>, AppError> {
        if events.is_closed() {
            return Ok(None);
        }

        let tool_name = step.tool.as_deref().ok_or_else(|| {
            AppError::Config(format!("step '{}' type 'tool' requires tool", step.id))
        })?;
        let tool = self
            .tools
            .get(tool_name)
            .ok_or_else(|| AppError::Run(format!("tool '{}' is not registered", tool_name)))?;

        if !events.emit(
            ctx,
            Some(&step.id),
            RunEventKind::ToolCallStarted,
            json!({ "tool": tool_name }),
        ) {
            return Ok(None);
        }

        if events.is_closed() {
            return Ok(None);
        }

        let output = tokio::select! {
            _ = events.closed() => return Ok(None),
            result = tool.call(
                step.args.clone(),
                ToolContext {
                    run_id: ctx.run_id.clone(),
                },
            ) => result?,
        };

        if !events.emit(
            ctx,
            Some(&step.id),
            RunEventKind::ToolCallCompleted,
            json!({ "tool": tool_name, "output": output.clone() }),
        ) {
            return Ok(None);
        }
        Ok(Some(output))
    }
}

#[derive(Clone)]
struct EventSender {
    tx: mpsc::UnboundedSender<RunEvent>,
    cancel_rx: watch::Receiver<bool>,
}

impl EventSender {
    fn new(tx: mpsc::UnboundedSender<RunEvent>, cancel_rx: watch::Receiver<bool>) -> Self {
        Self { tx, cancel_rx }
    }

    fn emit(
        &self,
        ctx: &RunContext,
        step_id: Option<&str>,
        kind: RunEventKind,
        payload: Value,
    ) -> bool {
        self.tx
            .send(RunEvent {
                kind,
                run_id: ctx.run_id.clone(),
                agent_id: ctx.agent_id.clone(),
                step_id: step_id.map(str::to_string),
                timestamp: Utc::now(),
                payload,
            })
            .is_ok()
    }

    fn is_closed(&self) -> bool {
        self.tx.is_closed() || *self.cancel_rx.borrow()
    }

    async fn closed(&self) {
        if self.is_closed() {
            return;
        }

        let mut cancel_rx = self.cancel_rx.clone();
        tokio::select! {
            _ = self.tx.closed() => {}
            changed = cancel_rx.changed() => {
                let _ = changed;
            }
        }
    }
}

struct RunEventStream {
    inner: UnboundedReceiverStream<RunEvent>,
    cancel_tx: Option<watch::Sender<bool>>,
    task: Option<JoinHandle<()>>,
}

impl RunEventStream {
    fn new(
        inner: UnboundedReceiverStream<RunEvent>,
        cancel_tx: watch::Sender<bool>,
        task: JoinHandle<()>,
    ) -> Self {
        Self {
            inner,
            cancel_tx: Some(cancel_tx),
            task: Some(task),
        }
    }
}

impl Stream for RunEventStream {
    type Item = RunEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl Drop for RunEventStream {
    fn drop(&mut self) {
        if let Some(cancel_tx) = self.cancel_tx.take() {
            let _ = cancel_tx.send(true);
        }
        if let Some(task) = self.task.take() {
            task.abort();
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
