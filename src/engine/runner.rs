use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};

use cel_interpreter::{Context as CelContext, Program as CelProgram, Value as CelValue};
use chrono::Utc;
use futures::{Stream, StreamExt};
use serde_json::{Map, Value};
use tokio::{
    sync::{mpsc, watch, Mutex},
    task::JoinHandle,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use uuid::Uuid;

use crate::{
    agent::{
        config::{ModelConfig, StepConfig, StepKind},
        loader::LoadedAgent,
    },
    code::registry::{CodeContext, CodeRegistry},
    engine::{
        context::RunContext,
        event::{RunEvent, RunEventScope, RunEventType},
    },
    error::AppError,
    history::store::{input_summary, RunHistoryStore, RunStatus},
    model::types::{ChatContentPart, ChatMessage, ChatRequest, ModelClient},
    prompt::{renderer::PromptRenderer, store::PromptStore},
    request_context::RequestContext,
    tools::registry::{ToolContext, ToolRegistry},
};

#[derive(Clone)]
pub struct RunEngine<M: ModelClient> {
    model: M,
    tools: ToolRegistry,
    code_handlers: CodeRegistry,
    history_store: RunHistoryStore,
    renderer: PromptRenderer,
}

impl<M: ModelClient> RunEngine<M> {
    pub fn new(model: M, tools: ToolRegistry) -> Self {
        Self {
            model,
            tools,
            code_handlers: CodeRegistry::default(),
            history_store: RunHistoryStore::default(),
            renderer: PromptRenderer::new(),
        }
    }

    pub fn with_code_handlers(mut self, code_handlers: CodeRegistry) -> Self {
        self.code_handlers = code_handlers;
        self
    }

    pub fn with_history_store(mut self, history_store: RunHistoryStore) -> Self {
        self.history_store = history_store;
        self
    }

    pub fn history_store(&self) -> RunHistoryStore {
        self.history_store.clone()
    }

    pub fn run(&self, agent: LoadedAgent, input: Value) -> impl Stream<Item = RunEvent> {
        self.run_with_request_id(agent, input, format!("req_{}", Uuid::new_v4()))
    }

    pub fn run_with_request_id(
        &self,
        agent: LoadedAgent,
        input: Value,
        request_id: String,
    ) -> impl Stream<Item = RunEvent> {
        self.run_with_request_context(
            agent,
            input,
            RequestContext {
                request_id,
                ..Default::default()
            },
        )
    }

    pub fn run_with_request_context(
        &self,
        agent: LoadedAgent,
        input: Value,
        request: RequestContext,
    ) -> impl Stream<Item = RunEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let engine = self.clone();
        let task = tokio::spawn(async move {
            let history_store = engine.history_store.clone();
            engine
                .run_streaming(
                    agent,
                    input,
                    request,
                    EventSender::new(tx, cancel_rx, history_store),
                )
                .await;
        });
        RunEventStream::new(UnboundedReceiverStream::new(rx), cancel_tx, task)
    }

    async fn run_streaming(
        &self,
        agent: LoadedAgent,
        input: Value,
        request: RequestContext,
        events: EventSender,
    ) {
        let run_started = Instant::now();
        let run_id = format!("run_{}", Uuid::new_v4());
        let mut ctx = RunContext {
            request,
            run_id,
            agent_id: agent.config.id.clone(),
            started_at: Utc::now(),
            input,
            step_outputs: BTreeMap::new(),
        };
        self.history_store
            .create_run(
                &ctx.run_id,
                &ctx.agent_id,
                &ctx.request,
                ctx.started_at,
                input_summary(&ctx.input),
            )
            .await;
        let store = PromptStore::new(agent.prompts.clone());
        tracing::info!(
            request_id = %ctx.request.request_id,
            run_id = %ctx.run_id,
            agent_id = %ctx.agent_id,
            steps_count = agent.config.steps.len(),
            provider = %agent.config.model.provider,
            model_type = ?agent.config.model.model_type,
            model = ?agent.config.model.model,
            "agent run started"
        );
        if !events.emit(&ctx, None, RunEventType::RunStarted).await {
            return;
        }

        let step_index_by_id = agent
            .config
            .steps
            .iter()
            .enumerate()
            .map(|(index, step)| (step.id.as_str(), index))
            .collect::<BTreeMap<_, _>>();
        let max_step_executions = agent.config.steps.len().saturating_mul(3).max(1);
        let mut step_index = 0_usize;
        let mut step_executions = 0_usize;
        let mut final_output = Value::Null;
        let mut final_step_id = None;

        while step_index < agent.config.steps.len() {
            step_executions += 1;
            if step_executions > max_step_executions {
                let err =
                    AppError::Run("condition routing exceeded maximum step executions".to_string());
                let code = err.api_code();
                let message = err.to_string();
                tracing::error!(
                    run_id = %ctx.run_id,
                    agent_id = %ctx.agent_id,
                    error = %message,
                    "agent run failed"
                );
                let _ = events.emit_error(&ctx, None, code, message).await;
                return;
            }

            let step = &agent.config.steps[step_index];
            let step_started = Instant::now();
            tracing::info!(
                run_id = %ctx.run_id,
                agent_id = %ctx.agent_id,
                step_id = %step.id,
                step_type = ?step.kind,
                "agent step started"
            );
            if !events
                .emit(&ctx, Some(&step.id), RunEventType::StepStarted)
                .await
            {
                return;
            }
            match self
                .execute_step(step, &agent.config.model, &store, &mut ctx, &events)
                .await
            {
                Ok(StepExecution::Completed { output, next }) => {
                    if let Some(output) = output.clone() {
                        final_output = output.clone();
                        final_step_id = Some(step.id.clone());
                        ctx.set_step_output(&step.id, output.clone());
                        if let Some(stored_output) = ctx.step_outputs.get(&step.id) {
                            self.history_store
                                .record_step_output(&ctx.run_id, &step.id, stored_output.clone())
                                .await;
                        }
                    }
                    tracing::info!(
                        run_id = %ctx.run_id,
                        agent_id = %ctx.agent_id,
                        step_id = %step.id,
                        elapsed_ms = step_started.elapsed().as_millis(),
                        output = %output.as_ref().map(summarize_value).unwrap_or_else(|| "null".to_string()),
                        output_preview = %output.as_ref().map(|value| format_value_for_log(value, 1200)).unwrap_or_default(),
                        "agent step completed"
                    );
                    if let Some(output) = &output {
                        tracing::debug!(
                            run_id = %ctx.run_id,
                            agent_id = %ctx.agent_id,
                            step_id = %step.id,
                            output_text = %format_value_for_log(output, 8000),
                            "agent step output"
                        );
                    }
                    if !events
                        .emit(&ctx, Some(&step.id), RunEventType::StepCompleted)
                        .await
                    {
                        return;
                    }

                    match next {
                        NextStep::Continue => {
                            if step.end {
                                break;
                            }
                            step_index += 1;
                        }
                        NextStep::Goto(step_id) => {
                            let Some(next_index) = step_index_by_id.get(step_id.as_str()) else {
                                let err = AppError::Run(format!(
                                    "condition routed to unknown step '{step_id}'"
                                ));
                                let code = err.api_code();
                                let message = err.to_string();
                                let _ =
                                    events.emit_error(&ctx, Some(&step.id), code, message).await;
                                return;
                            };
                            tracing::info!(
                                run_id = %ctx.run_id,
                                agent_id = %ctx.agent_id,
                                step_id = %step.id,
                                next_step_id = %step_id,
                                "agent step routed"
                            );
                            step_index = *next_index;
                        }
                    }
                }
                Ok(StepExecution::Cancelled) => return,
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
                    let _ = events.emit_error(&ctx, Some(&step.id), code, message).await;
                    return;
                }
            }
        }

        let _ = events
            .emit_run_completed(&ctx, final_step_id.as_deref(), final_output.clone())
            .await;
        self.history_store
            .finish_run(&ctx.run_id, RunStatus::Completed, None)
            .await;
        tracing::info!(
            run_id = %ctx.run_id,
            agent_id = %ctx.agent_id,
            elapsed_ms = run_started.elapsed().as_millis(),
            output = %summarize_value(&final_output),
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
    ) -> Result<StepExecution, AppError> {
        if events.is_closed() {
            return Ok(StepExecution::Cancelled);
        }

        match step.kind {
            StepKind::Prompt => {
                let template =
                    resolve_prompt(step.prompt.as_deref(), step.prompt_ref.as_deref(), store)?;
                let rendered = self.renderer.render(template, &ctx.template_data())?;
                Ok(StepExecution::output(Value::String(rendered)))
            }
            StepKind::Text => self.execute_text_step(step, store, ctx, events).await,
            StepKind::Llm => {
                self.execute_llm_step(step, model_config, store, ctx, events)
                    .await
            }
            StepKind::Tool => self.execute_tool_step(step, ctx, events).await,
            StepKind::Code => self.execute_code_step(step, ctx, events).await,
            StepKind::Condition => self.execute_condition_step(step, ctx),
        }
    }

    async fn execute_llm_step(
        &self,
        step: &StepConfig,
        model_config: &ModelConfig,
        store: &PromptStore,
        ctx: &RunContext,
        events: &EventSender,
    ) -> Result<StepExecution, AppError> {
        if events.is_closed() {
            return Ok(StepExecution::Cancelled);
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
            return Ok(StepExecution::Cancelled);
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
            _ = events.closed() => return Ok(StepExecution::Cancelled),
            result = self.model.stream_chat(request) => result?,
        };
        let mut output = String::new();
        let mut chunks_count = 0_usize;

        loop {
            let next_chunk = tokio::select! {
                _ = events.closed() => return Ok(StepExecution::Cancelled),
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
            output.push_str(&delta);
            if !events.emit_content(ctx, Some(&step.id), delta).await {
                return Ok(StepExecution::Cancelled);
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

        Ok(StepExecution::output(Value::String(output)))
    }

    async fn execute_text_step(
        &self,
        step: &StepConfig,
        store: &PromptStore,
        ctx: &RunContext,
        events: &EventSender,
    ) -> Result<StepExecution, AppError> {
        if events.is_closed() {
            return Ok(StepExecution::Cancelled);
        }

        let template = resolve_prompt(step.prompt.as_deref(), step.prompt_ref.as_deref(), store)?;
        let rendered = self.renderer.render(template, &ctx.template_data())?;
        if !rendered.is_empty()
            && !events
                .emit_content(ctx, Some(&step.id), rendered.clone())
                .await
        {
            return Ok(StepExecution::Cancelled);
        }
        Ok(StepExecution::output(Value::String(rendered)))
    }

    fn execute_condition_step(
        &self,
        step: &StepConfig,
        ctx: &RunContext,
    ) -> Result<StepExecution, AppError> {
        let data = ctx.template_data();
        for (index, case) in step.cases.iter().enumerate() {
            if evaluate_condition(&case.when, &data)? {
                return Ok(StepExecution::goto(
                    json_object(vec![
                        ("matched", Value::Bool(true)),
                        ("matched_case", Value::from(index)),
                        ("goto", Value::String(case.goto.clone())),
                    ]),
                    case.goto.clone(),
                ));
            }
        }

        if let Some(default) = &step.default {
            return Ok(StepExecution::goto(
                json_object(vec![
                    ("matched", Value::Bool(false)),
                    ("goto", Value::String(default.clone())),
                ]),
                default.clone(),
            ));
        }

        Ok(StepExecution::output(json_object(vec![(
            "matched",
            Value::Bool(false),
        )])))
    }

    async fn execute_tool_step(
        &self,
        step: &StepConfig,
        ctx: &RunContext,
        events: &EventSender,
    ) -> Result<StepExecution, AppError> {
        if events.is_closed() {
            return Ok(StepExecution::Cancelled);
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
        if !events
            .emit(ctx, Some(&step.id), RunEventType::ToolCallStarted)
            .await
        {
            return Ok(StepExecution::Cancelled);
        }

        if events.is_closed() {
            return Ok(StepExecution::Cancelled);
        }

        let output = tokio::select! {
            _ = events.closed() => return Ok(StepExecution::Cancelled),
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
        if !events
            .emit(ctx, Some(&step.id), RunEventType::ToolCallCompleted)
            .await
        {
            return Ok(StepExecution::Cancelled);
        }
        Ok(StepExecution::output(output))
    }

    async fn execute_code_step(
        &self,
        step: &StepConfig,
        ctx: &RunContext,
        events: &EventSender,
    ) -> Result<StepExecution, AppError> {
        if events.is_closed() {
            return Ok(StepExecution::Cancelled);
        }

        let handler_name = step.handler.as_deref().ok_or_else(|| {
            AppError::Config(format!("step '{}' type 'code' requires handler", step.id))
        })?;
        let handler = self.code_handlers.get(handler_name).ok_or_else(|| {
            AppError::Run(format!("code handler '{}' is not registered", handler_name))
        })?;
        let inputs = render_template_value(&self.renderer, &step.inputs, &ctx.template_data())?;
        let emit_ctx = ctx.clone();
        let emit_events = events.clone();
        let emit_step_id = step.id.clone();
        let code_ctx = CodeContext::new(
            ctx.run_id.clone(),
            Arc::new(move |content| {
                let emit_ctx = emit_ctx.clone();
                let emit_events = emit_events.clone();
                let emit_step_id = emit_step_id.clone();
                Box::pin(async move {
                    if emit_events
                        .emit_content(&emit_ctx, Some(&emit_step_id), content)
                        .await
                    {
                        Ok(())
                    } else {
                        Err(AppError::Run(
                            "run stream closed while emitting code output".to_string(),
                        ))
                    }
                })
            }),
        );

        tracing::info!(
            run_id = %ctx.run_id,
            agent_id = %ctx.agent_id,
            step_id = %step.id,
            handler = %handler_name,
            input = %summarize_value(&inputs),
            input_preview = %format_value_for_log(&inputs, 1200),
            "code handler started"
        );

        let output = tokio::select! {
            _ = events.closed() => return Ok(StepExecution::Cancelled),
            result = handler.call(inputs, code_ctx) => result?,
        };

        tracing::info!(
            run_id = %ctx.run_id,
            agent_id = %ctx.agent_id,
            step_id = %step.id,
            handler = %handler_name,
            output = %summarize_value(&output),
            output_preview = %format_value_for_log(&output, 1200),
            "code handler completed"
        );
        Ok(StepExecution::output(output))
    }
}

#[derive(Debug)]
enum StepExecution {
    Completed {
        output: Option<Value>,
        next: NextStep,
    },
    Cancelled,
}

impl StepExecution {
    fn output(output: Value) -> Self {
        Self::Completed {
            output: Some(output),
            next: NextStep::Continue,
        }
    }

    fn goto(output: Value, step_id: String) -> Self {
        Self::Completed {
            output: Some(output),
            next: NextStep::Goto(step_id),
        }
    }
}

#[derive(Debug)]
enum NextStep {
    Continue,
    Goto(String),
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

fn evaluate_condition(expression: &str, data: &Value) -> Result<bool, AppError> {
    let expression = expression.trim();
    let program = CelProgram::compile(expression).map_err(|err| {
        AppError::Run(format!(
            "invalid condition expression '{expression}': {err}"
        ))
    })?;
    let mut context = CelContext::default();

    let Value::Object(object) = data else {
        return Err(AppError::Run(
            "condition context data must be a JSON object".to_string(),
        ));
    };

    for (name, value) in object {
        context.add_variable(name, value.clone()).map_err(|err| {
            AppError::Run(format!(
                "failed to prepare condition variable '{name}' for expression '{expression}': {err}"
            ))
        })?;
    }

    match program.execute(&context).map_err(|err| {
        AppError::Run(format!(
            "failed to evaluate condition expression '{expression}': {err}"
        ))
    })? {
        CelValue::Bool(value) => Ok(value),
        value => Err(AppError::Run(format!(
            "condition expression '{expression}' returned {}, expected bool",
            value.type_of()
        ))),
    }
}

fn json_object(fields: Vec<(&str, Value)>) -> Value {
    Value::Object(
        fields
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect::<Map<_, _>>(),
    )
}

fn lifecycle_event_data(event_type: RunEventType, step_id: Option<&str>) -> Value {
    let mut data = Map::new();
    if let Some(step_id) = step_id {
        data.insert("step_id".to_string(), Value::String(step_id.to_string()));
    }
    let status = match event_type {
        RunEventType::RunStarted | RunEventType::StepStarted | RunEventType::ToolCallStarted => {
            Some("running")
        }
        RunEventType::ToolCallCompleted | RunEventType::StepCompleted => Some("completed"),
        RunEventType::StepFailed | RunEventType::RunFailed => Some("failed"),
        RunEventType::RunCancelled => Some("cancelled"),
        RunEventType::RunCompleted => Some("completed"),
        RunEventType::ThinkingDelta | RunEventType::ContentDelta => None,
    };
    if let Some(status) = status {
        data.insert("status".to_string(), Value::String(status.to_string()));
    }
    Value::Object(data)
}

fn display_content_from_output(output: &Value) -> String {
    match output {
        Value::String(value) => value.clone(),
        Value::Object(object) => object
            .get("text")
            .or_else(|| object.get("content"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        _ => String::new(),
    }
}

fn structured_output_from_value(output: Value) -> Value {
    match output {
        Value::Object(_) | Value::Array(_) => output,
        _ => Value::Null,
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

fn event_scope(ctx: &RunContext, step_id: Option<&str>) -> RunEventScope {
    RunEventScope {
        request_id: ctx.request.request_id.clone(),
        run_id: ctx.run_id.clone(),
        agent_id: ctx.agent_id.clone(),
        step_id: step_id.map(str::to_string),
    }
}

#[derive(Default)]
struct EmissionState {
    seq: u64,
    accumulated_content: String,
    step_content: BTreeMap<String, String>,
}

impl EmissionState {
    fn next_seq(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }
}

#[derive(Clone)]
struct EventSender {
    tx: mpsc::UnboundedSender<RunEvent>,
    cancel_rx: watch::Receiver<bool>,
    state: Arc<Mutex<EmissionState>>,
    history_store: RunHistoryStore,
}

impl EventSender {
    fn new(
        tx: mpsc::UnboundedSender<RunEvent>,
        cancel_rx: watch::Receiver<bool>,
        history_store: RunHistoryStore,
    ) -> Self {
        Self {
            tx,
            cancel_rx,
            state: Arc::new(Mutex::new(EmissionState::default())),
            history_store,
        }
    }

    async fn emit(
        &self,
        ctx: &RunContext,
        step_id: Option<&str>,
        event_type: RunEventType,
    ) -> bool {
        let data = lifecycle_event_data(event_type, step_id);
        self.send_event(ctx, step_id, event_type, data).await
    }

    async fn emit_content(
        &self,
        ctx: &RunContext,
        step_id: Option<&str>,
        content: impl Into<String>,
    ) -> bool {
        let raw_content = content.into();
        let (run_event, emitted) = {
            let mut state = self.state.lock().await;
            let step_emitted_content = step_id
                .and_then(|id| state.step_content.get(id))
                .is_some_and(|content| !content.is_empty());
            let content = format_outbound_delta(
                &raw_content,
                step_emitted_content,
                !state.accumulated_content.is_empty(),
            );
            let mut data = Map::new();
            if let Some(step_id) = step_id {
                data.insert("step_id".to_string(), Value::String(step_id.to_string()));
            }
            data.insert("content".to_string(), Value::String(content.clone()));
            let seq = state.next_seq();
            let run_event = RunEvent::ok(
                RunEventType::ContentDelta,
                seq,
                event_scope(ctx, step_id),
                Value::Object(data),
            );
            let emitted = self.tx.send(run_event.clone()).is_ok();
            if emitted && !raw_content.is_empty() {
                state.accumulated_content.push_str(&content);
                if let Some(step_id) = step_id {
                    state
                        .step_content
                        .entry(step_id.to_string())
                        .or_default()
                        .push_str(&raw_content);
                }
            }
            (run_event, emitted)
        };
        if emitted {
            self.history_store.record_event(&run_event).await;
        }
        emitted
    }

    async fn emit_run_completed(
        &self,
        ctx: &RunContext,
        final_step_id: Option<&str>,
        output: Value,
    ) -> bool {
        let display_content = display_content_from_output(&output);
        let structured_output = structured_output_from_value(output);
        let (run_event, emitted) = {
            let mut state = self.state.lock().await;
            let final_content_was_streamed = final_step_id
                .and_then(|step_id| state.step_content.get(step_id))
                .is_some_and(|content| content == &display_content);
            let mut content = state.accumulated_content.clone();
            if !display_content.is_empty() && !final_content_was_streamed {
                content.push_str(&format_outbound_delta(
                    &display_content,
                    false,
                    !content.is_empty(),
                ));
            }
            let data = json_object(vec![
                ("status", Value::String("completed".to_string())),
                ("content", Value::String(content)),
                ("content_format", Value::String("markdown".to_string())),
                ("output", structured_output),
                ("conversation", Value::Null),
            ]);
            let seq = state.next_seq();
            let run_event = RunEvent::ok(
                RunEventType::RunCompleted,
                seq,
                event_scope(ctx, None),
                data,
            );
            let emitted = self.tx.send(run_event.clone()).is_ok();
            (run_event, emitted)
        };
        if emitted {
            self.history_store.record_event(&run_event).await;
        }
        emitted
    }

    async fn send_event(
        &self,
        ctx: &RunContext,
        step_id: Option<&str>,
        event_type: RunEventType,
        data: Value,
    ) -> bool {
        let (run_event, emitted) = {
            let mut state = self.state.lock().await;
            let seq = state.next_seq();
            let run_event = RunEvent::ok(event_type, seq, event_scope(ctx, step_id), data);
            let emitted = self.tx.send(run_event.clone()).is_ok();
            (run_event, emitted)
        };
        if emitted {
            self.history_store.record_event(&run_event).await;
        }
        emitted
    }

    async fn emit_error(
        &self,
        ctx: &RunContext,
        step_id: Option<&str>,
        code: i32,
        message: impl Into<String>,
    ) -> bool {
        let message = message.into();
        let (recorded_events, emitted) = {
            let mut state = self.state.lock().await;
            let mut recorded_events = Vec::new();
            if let Some(step_id) = step_id {
                let seq = state.next_seq();
                let step_failed = RunEvent::failed(
                    RunEventType::StepFailed,
                    seq,
                    event_scope(ctx, Some(step_id)),
                    code,
                    message.clone(),
                );
                if self.tx.send(step_failed.clone()).is_ok() {
                    recorded_events.push(step_failed);
                }
            }
            let seq = state.next_seq();
            let run_failed = RunEvent::failed(
                RunEventType::RunFailed,
                seq,
                event_scope(ctx, step_id),
                code,
                message.clone(),
            );
            let emitted = self.tx.send(run_failed.clone()).is_ok();
            if emitted {
                recorded_events.push(run_failed);
            }
            (recorded_events, emitted)
        };
        for event in recorded_events {
            self.history_store.record_event(&event).await;
        }
        self.history_store
            .finish_run(&ctx.run_id, RunStatus::Failed, Some(message))
            .await;
        emitted
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

fn render_template_value(
    renderer: &PromptRenderer,
    value: &Value,
    data: &Value,
) -> Result<Value, AppError> {
    match value {
        Value::String(template) => renderer.render(template, data).map(Value::String),
        Value::Array(items) => items
            .iter()
            .map(|item| render_template_value(renderer, item, data))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Value::Object(object) => object
            .iter()
            .map(|(key, value)| {
                render_template_value(renderer, value, data).map(|rendered| (key.clone(), rendered))
            })
            .collect::<Result<Map<_, _>, _>>()
            .map(Value::Object),
        other => Ok(other.clone()),
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
