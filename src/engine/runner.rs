use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Instant,
};

use chrono::Utc;
use futures::{Stream, StreamExt};
use serde_json::Value;
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
    model::types::{ChatContentPart, ChatMessage, ChatRequest, ModelClient},
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
        let run_started = Instant::now();
        let run_id = format!("run_{}", Uuid::new_v4());
        let mut ctx = RunContext {
            run_id,
            agent_id: agent.config.id.clone(),
            started_at: Utc::now(),
            input,
            step_outputs: BTreeMap::new(),
        };
        let store = PromptStore::new(agent.prompts.clone());
        tracing::info!(
            run_id = %ctx.run_id,
            agent_id = %ctx.agent_id,
            steps_count = agent.config.steps.len(),
            provider = %agent.config.model.provider,
            model_type = ?agent.config.model.model_type,
            model = ?agent.config.model.model,
            "agent run started"
        );
        if !events.emit(&ctx, None, RunEventKind::RunStarted) {
            return;
        }

        for step in &agent.config.steps {
            let step_started = Instant::now();
            tracing::info!(
                run_id = %ctx.run_id,
                agent_id = %ctx.agent_id,
                step_id = %step.id,
                step_type = ?step.kind,
                "agent step started"
            );
            if !events.emit(&ctx, Some(&step.id), RunEventKind::StepStarted) {
                return;
            }
            match self
                .execute_step(step, &agent.config.model, &store, &mut ctx, &events)
                .await
            {
                Ok(Some(output)) => {
                    ctx.set_step_output(&step.id, output.clone());
                    tracing::info!(
                        run_id = %ctx.run_id,
                        agent_id = %ctx.agent_id,
                        step_id = %step.id,
                        elapsed_ms = step_started.elapsed().as_millis(),
                        output = %summarize_value(&output),
                        output_preview = %format_value_for_log(&output, 1200),
                        "agent step completed"
                    );
                    tracing::debug!(
                        run_id = %ctx.run_id,
                        agent_id = %ctx.agent_id,
                        step_id = %step.id,
                        output_text = %format_value_for_log(&output, 8000),
                        "agent step output"
                    );
                    if !events.emit(&ctx, Some(&step.id), RunEventKind::StepCompleted) {
                        return;
                    }
                }
                Ok(None) => return,
                Err(err) => {
                    let code = err.api_code();
                    let message = err.to_string();
                    tracing::error!(
                        run_id = %ctx.run_id,
                        agent_id = %ctx.agent_id,
                        step_id = %step.id,
                        elapsed_ms = step_started.elapsed().as_millis(),
                        error = %message,
                        "agent step failed"
                    );
                    let _ = events.emit_error(&ctx, Some(&step.id), code, message);
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
        let _ = events.emit(&ctx, None, RunEventKind::RunCompleted);
        tracing::info!(
            run_id = %ctx.run_id,
            agent_id = %ctx.agent_id,
            elapsed_ms = run_started.elapsed().as_millis(),
            output = %summarize_value(&output),
            "agent run completed"
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
        messages.push(build_user_message(
            user_prompt,
            step.image_input.as_deref(),
            ctx,
        )?);
        let image_parts_count = count_message_images(&messages);
        let text_chars_count = count_message_text_chars(&messages);

        let request = ChatRequest {
            provider: model_config.provider.clone(),
            model_type: model_config.model_type,
            model: model_config.model.clone().unwrap_or_default(),
            messages,
            temperature: model_config.temperature,
            max_tokens: model_config.max_tokens,
        };

        if events.is_closed() {
            return Ok(None);
        }

        tracing::debug!(
            run_id = %ctx.run_id,
            agent_id = %ctx.agent_id,
            step_id = %step.id,
            provider = %request.provider,
            model_type = ?request.model_type,
            model = %request.model,
            messages_count = request.messages.len(),
            image_parts_count,
            text_chars_count,
            temperature = ?request.temperature,
            max_tokens = ?request.max_tokens,
            "llm step request prepared"
        );

        let mut stream = tokio::select! {
            _ = events.closed() => return Ok(None),
            result = self.model.stream_chat(request) => result?,
        };
        let mut output = String::new();
        let mut chunks_count = 0_usize;
        let mut step_emitted_content = false;

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
            chunks_count += 1;
            let outbound_delta =
                format_outbound_delta(&delta, step_emitted_content, events.has_emitted_content());
            step_emitted_content = true;
            output.push_str(&delta);
            if !events.emit_content(
                ctx,
                Some(&step.id),
                RunEventKind::TokenDelta,
                outbound_delta,
            ) {
                return Ok(None);
            }
        }

        tracing::debug!(
            run_id = %ctx.run_id,
            agent_id = %ctx.agent_id,
            step_id = %step.id,
            chunks_count,
            output_chars = output.chars().count(),
            "llm step stream consumed"
        );

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

        tracing::info!(
            run_id = %ctx.run_id,
            agent_id = %ctx.agent_id,
            step_id = %step.id,
            tool = %tool_name,
            "tool call started"
        );
        if !events.emit(ctx, Some(&step.id), RunEventKind::ToolCallStarted) {
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

        tracing::info!(
            run_id = %ctx.run_id,
            agent_id = %ctx.agent_id,
            step_id = %step.id,
            tool = %tool_name,
            output = %summarize_value(&output),
            output_preview = %format_value_for_log(&output, 1200),
            "tool call completed"
        );
        if !events.emit(ctx, Some(&step.id), RunEventKind::ToolCallCompleted) {
            return Ok(None);
        }
        Ok(Some(output))
    }
}

fn summarize_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(_) => "bool".to_string(),
        Value::Number(_) => "number".to_string(),
        Value::String(value) => format!("string chars={}", value.chars().count()),
        Value::Array(value) => format!("array items={}", value.len()),
        Value::Object(value) => format!("object keys={}", value.len()),
    }
}

fn format_value_for_log(value: &Value, max_chars: usize) -> String {
    let text = match value {
        Value::String(value) => value.clone(),
        other => other.to_string(),
    };
    truncate_for_log(&text, max_chars)
}

fn truncate_for_log(value: &str, max_chars: usize) -> String {
    let mut truncated = value.chars().take(max_chars).collect::<String>();
    if value.chars().count() > max_chars {
        truncated.push_str("...");
    }
    truncated
}

fn format_outbound_delta(
    delta: &str,
    step_emitted_content: bool,
    run_emitted_content: bool,
) -> String {
    if step_emitted_content || !run_emitted_content || delta.starts_with('\n') {
        delta.to_string()
    } else {
        format!("\n\n{delta}")
    }
}

fn count_message_images(messages: &[ChatMessage]) -> usize {
    messages
        .iter()
        .map(|message| match &message.content {
            crate::model::types::ChatContent::Text(_) => 0,
            crate::model::types::ChatContent::Parts(parts) => parts
                .iter()
                .filter(|part| {
                    matches!(part, crate::model::types::ChatContentPart::ImageUrl { .. })
                })
                .count(),
        })
        .sum()
}

fn count_message_text_chars(messages: &[ChatMessage]) -> usize {
    messages
        .iter()
        .map(|message| match &message.content {
            crate::model::types::ChatContent::Text(text) => text.chars().count(),
            crate::model::types::ChatContent::Parts(parts) => parts
                .iter()
                .map(|part| match part {
                    crate::model::types::ChatContentPart::Text { text } => text.chars().count(),
                    crate::model::types::ChatContentPart::ImageUrl { .. } => 0,
                })
                .sum(),
        })
        .sum()
}

#[derive(Clone)]
struct EventSender {
    tx: mpsc::UnboundedSender<RunEvent>,
    cancel_rx: watch::Receiver<bool>,
    emitted_content: Arc<AtomicBool>,
}

impl EventSender {
    fn new(tx: mpsc::UnboundedSender<RunEvent>, cancel_rx: watch::Receiver<bool>) -> Self {
        Self {
            tx,
            cancel_rx,
            emitted_content: Arc::new(AtomicBool::new(false)),
        }
    }

    fn emit(&self, ctx: &RunContext, step_id: Option<&str>, event: RunEventKind) -> bool {
        self.emit_content(ctx, step_id, event, String::new())
    }

    fn emit_content(
        &self,
        ctx: &RunContext,
        step_id: Option<&str>,
        event: RunEventKind,
        content: impl Into<String>,
    ) -> bool {
        let content = content.into();
        let has_content = !content.is_empty();
        let emitted = self
            .tx
            .send(RunEvent::ok(
                event,
                ctx.run_id.clone(),
                ctx.agent_id.clone(),
                step_id.map(str::to_string),
                content,
                Value::Null,
            ))
            .is_ok();
        if emitted && event == RunEventKind::TokenDelta && has_content {
            self.emitted_content.store(true, Ordering::SeqCst);
        }
        emitted
    }

    fn has_emitted_content(&self) -> bool {
        self.emitted_content.load(Ordering::SeqCst)
    }

    fn emit_error(
        &self,
        ctx: &RunContext,
        step_id: Option<&str>,
        code: i32,
        message: impl Into<String>,
    ) -> bool {
        self.tx
            .send(RunEvent::error(
                ctx.run_id.clone(),
                ctx.agent_id.clone(),
                step_id.map(str::to_string),
                code,
                message,
            ))
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

fn build_user_message(
    prompt: String,
    image_input: Option<&str>,
    ctx: &RunContext,
) -> Result<ChatMessage, AppError> {
    let Some(path) = image_input else {
        return Ok(ChatMessage::text("user", prompt));
    };
    let images = resolve_image_input(path, ctx)?;
    if images.is_empty() {
        return Ok(ChatMessage::text("user", prompt));
    }

    let mut parts = Vec::with_capacity(images.len() + 1);
    parts.push(ChatContentPart::text(prompt));
    parts.extend(images.into_iter().map(ChatContentPart::image_url));
    Ok(ChatMessage::multimodal("user", parts))
}

fn resolve_image_input(path: &str, ctx: &RunContext) -> Result<Vec<String>, AppError> {
    if path != "input.images" {
        return Err(AppError::Config(format!(
            "unsupported image_input path '{path}'"
        )));
    }

    let Some(images) = ctx.input.get("images") else {
        return Ok(Vec::new());
    };
    let Some(images) = images.as_array() else {
        return Err(AppError::Run(
            "image_input 'input.images' must be an array".to_string(),
        ));
    };

    Ok(images
        .iter()
        .filter_map(|image| image.as_str().map(str::to_string))
        .collect())
}
