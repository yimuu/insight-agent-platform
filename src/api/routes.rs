use std::{convert::Infallible, sync::Arc};

use axum::{
    extract::{rejection::JsonRejection, Path, State},
    response::{
        sse::{Event, Sse},
        IntoResponse,
    },
    routing::{get, post},
    Json, Router,
};
use futures::{future, StreamExt};
use jsonschema::JSONSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    agent::{loader::LoadedAgent, registry::AgentRegistry},
    api::sse::encode_event_or_sanitized_error,
    engine::runner::RunEngine,
    error::AppError,
    model::types::ModelClient,
};

pub type EventEncoder = fn(crate::engine::event::RunEvent) -> Result<Event, AppError>;

#[derive(Clone)]
pub struct AppState<M: ModelClient> {
    pub registry: AgentRegistry,
    pub engine: RunEngine<M>,
    pub event_encoder: EventEncoder,
}

pub fn build_router<M: ModelClient>(state: AppState<M>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/agents", get(list_agents::<M>))
        .route("/v1/agents/:agent_id", get(get_agent::<M>))
        .route(
            "/v1/agents/:agent_id/runs/stream",
            post(run_agent_stream::<M>),
        )
        .with_state(Arc::new(state))
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn list_agents<M: ModelClient>(
    State(state): State<Arc<AppState<M>>>,
) -> Json<Vec<AgentSummary>> {
    Json(state.registry.list().map(AgentSummary::from).collect())
}

async fn get_agent<M: ModelClient>(
    State(state): State<Arc<AppState<M>>>,
    Path(agent_id): Path<String>,
) -> Result<Json<AgentSummary>, AppError> {
    let agent = state
        .registry
        .get(&agent_id)
        .ok_or_else(|| AppError::NotFound(format!("agent '{agent_id}' not found")))?;
    Ok(Json(AgentSummary::from(agent)))
}

#[derive(Debug, Deserialize)]
struct RunRequest {
    input: Value,
}

async fn run_agent_stream<M: ModelClient>(
    State(state): State<Arc<AppState<M>>>,
    Path(agent_id): Path<String>,
    request: Result<Json<RunRequest>, JsonRejection>,
) -> Result<impl IntoResponse, AppError> {
    let Json(request) = request.map_err(map_run_request_rejection)?;
    let agent = state
        .registry
        .get(&agent_id)
        .ok_or_else(|| AppError::NotFound(format!("agent '{agent_id}' not found")))?
        .clone();
    let input_summary = summarize_run_input(&request.input);
    tracing::info!(
        agent_id = %agent_id,
        input_keys = ?input_summary.keys,
        "agent stream request received"
    );
    tracing::debug!(
        agent_id = %agent_id,
        report_text_chars = input_summary.report_text_chars,
        images_count = input_summary.images_count,
        messages_count = input_summary.messages_count,
        question_chars = input_summary.question_chars,
        "agent stream request input summary"
    );

    let compiled_schema = JSONSchema::compile(&agent.config.input.schema).map_err(|err| {
        tracing::error!(agent_id = %agent_id, error = %err, "agent input schema compile failed");
        AppError::Config(format!(
            "invalid input schema for agent '{agent_id}': {err}"
        ))
    })?;

    if let Err(errors) = compiled_schema.validate(&request.input) {
        let messages = errors.map(|err| err.to_string()).collect::<Vec<_>>();
        tracing::warn!(
            agent_id = %agent_id,
            errors = ?messages,
            "agent run input validation failed"
        );
        return Err(AppError::Input(format!(
            "input validation failed: {}",
            messages.join("; ")
        )));
    }
    tracing::debug!(agent_id = %agent_id, "agent run input validation passed");

    let event_encoder = state.event_encoder;
    let stream = state
        .engine
        .run(agent, request.input)
        .scan(false, move |failed, event| {
            let next = if *failed {
                None
            } else {
                let encoded = event_encoder(event.clone());
                if encoded.is_err() {
                    *failed = true;
                }
                Some(Ok::<Event, Infallible>(encode_event_or_sanitized_error(
                    event, encoded,
                )))
            };
            future::ready(next)
        });

    Ok(Sse::new(stream))
}

#[derive(Debug, PartialEq, Eq)]
struct RunInputSummary {
    keys: Vec<String>,
    report_text_chars: Option<usize>,
    images_count: Option<usize>,
    messages_count: Option<usize>,
    question_chars: Option<usize>,
}

fn summarize_run_input(input: &Value) -> RunInputSummary {
    let object = input.as_object();
    let mut keys = object
        .map(|object| object.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    keys.sort();
    RunInputSummary {
        keys,
        report_text_chars: input
            .get("report_text")
            .and_then(Value::as_str)
            .map(|value| value.chars().count()),
        images_count: input.get("images").and_then(Value::as_array).map(Vec::len),
        messages_count: input
            .get("messages")
            .and_then(Value::as_array)
            .map(Vec::len),
        question_chars: input
            .get("question")
            .and_then(Value::as_str)
            .map(|value| value.chars().count()),
    }
}

fn map_run_request_rejection(rejection: JsonRejection) -> AppError {
    AppError::Input(match rejection {
        JsonRejection::MissingJsonContentType(_) => {
            "request body must be application/json".to_string()
        }
        other => format!("invalid request body: {}", other.body_text()),
    })
}

#[derive(Debug, Serialize)]
struct AgentSummary {
    id: String,
    name: String,
    description: String,
    input_schema: Value,
}

impl From<&LoadedAgent> for AgentSummary {
    fn from(agent: &LoadedAgent) -> Self {
        Self {
            id: agent.config.id.clone(),
            name: agent.config.name.clone(),
            description: agent.config.description.clone(),
            input_schema: agent.config.input.schema.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{summarize_run_input, RunInputSummary};

    #[test]
    fn summarizes_medical_input_without_copying_content() {
        let summary = summarize_run_input(&json!({
            "report_text": "血红蛋白 105",
            "images": [
                "http://127.0.0.1/report.png",
                "data:image/png;base64,abc123"
            ],
            "messages": [
                {"role": "user", "content": "原文不应进入日志"}
            ],
            "question": "严重吗？"
        }));

        assert_eq!(
            summary,
            RunInputSummary {
                keys: vec![
                    "images".to_string(),
                    "messages".to_string(),
                    "question".to_string(),
                    "report_text".to_string(),
                ],
                report_text_chars: Some(8),
                images_count: Some(2),
                messages_count: Some(1),
                question_chars: Some(4),
            }
        );
    }
}
