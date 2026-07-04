use std::{convert::Infallible, sync::Arc};

use axum::{
    extract::{Path, State},
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
    Json(request): Json<RunRequest>,
) -> Result<impl IntoResponse, AppError> {
    let agent = state
        .registry
        .get(&agent_id)
        .ok_or_else(|| AppError::NotFound(format!("agent '{agent_id}' not found")))?
        .clone();

    let compiled_schema = JSONSchema::compile(&agent.config.input.schema).map_err(|err| {
        AppError::Config(format!(
            "invalid input schema for agent '{agent_id}': {err}"
        ))
    })?;

    if let Err(errors) = compiled_schema.validate(&request.input) {
        let messages = errors.map(|err| err.to_string()).collect::<Vec<_>>();
        return Err(AppError::Input(format!(
            "input validation failed: {}",
            messages.join("; ")
        )));
    }

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
