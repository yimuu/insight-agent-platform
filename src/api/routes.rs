use std::{convert::Infallible, sync::Arc};

use axum::{
    body::Body,
    extract::{rejection::JsonRejection, Path, Query, State},
    http::{
        header::{HeaderName, AUTHORIZATION},
        HeaderMap, HeaderValue, Request,
    },
    middleware::{self, Next},
    response::{
        sse::{Event, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use futures::{future, StreamExt};
use jsonschema::JSONSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::{
    agent::{loader::LoadedAgent, registry::AgentRegistry},
    api::sse::encode_event_or_sanitized_error,
    engine::runner::RunEngine,
    error::AppError,
    history::store::RunHistoryQuery,
    model::types::ModelClient,
    request_context::RequestContext,
    response::ApiResponse,
};

pub type EventEncoder = fn(crate::engine::event::RunEvent) -> Result<Event, AppError>;
const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");
const X_CALLER_SERVICE: HeaderName = HeaderName::from_static("x-caller-service");
const X_TENANT_ID: HeaderName = HeaderName::from_static("x-tenant-id");
const X_USER_ID: HeaderName = HeaderName::from_static("x-user-id");

#[derive(Debug, Clone, Default)]
pub struct RuntimeAuth {
    internal_bearer_token: Option<String>,
}

impl RuntimeAuth {
    pub fn disabled() -> Self {
        Self {
            internal_bearer_token: None,
        }
    }

    pub fn bearer_token(token: impl Into<String>) -> Self {
        Self {
            internal_bearer_token: Some(token.into()),
        }
    }
}

#[derive(Clone)]
pub struct AppState<M: ModelClient> {
    pub registry: AgentRegistry,
    pub engine: RunEngine<M>,
    pub event_encoder: EventEncoder,
    pub auth: RuntimeAuth,
}

pub fn build_router<M: ModelClient>(state: AppState<M>) -> Router {
    let state = Arc::new(state);
    let auth = state.auth.clone();
    let v1_routes = Router::new()
        .route("/v1/agents", get(list_agents::<M>))
        .route("/v1/agents/:agent_id", get(get_agent::<M>))
        .route("/v1/agents/:agent_id/runs", get(list_agent_runs::<M>))
        .route("/v1/runs", get(list_runs::<M>))
        .route("/v1/runs/:run_id", get(get_run::<M>))
        .route(
            "/v1/agents/:agent_id/runs/stream",
            post(run_agent_stream::<M>),
        )
        .route_layer(middleware::from_fn(
            move |headers: HeaderMap, request: Request<Body>, next: Next| {
                let auth = auth.clone();
                async move {
                    ensure_internal_auth(&auth, &headers)?;
                    Ok::<Response, AppError>(next.run(request).await)
                }
            },
        ))
        .with_state(state);

    Router::new().route("/health", get(health)).merge(v1_routes)
}

async fn health() -> Json<ApiResponse<Value>> {
    Json(ApiResponse::ok(json!({ "status": "ok" })))
}

async fn list_agents<M: ModelClient>(
    State(state): State<Arc<AppState<M>>>,
) -> Result<Json<ApiResponse<Vec<AgentSummary>>>, AppError> {
    Ok(Json(ApiResponse::ok(
        state.registry.list().map(AgentSummary::from).collect(),
    )))
}

async fn get_agent<M: ModelClient>(
    State(state): State<Arc<AppState<M>>>,
    Path(agent_id): Path<String>,
) -> Result<Json<ApiResponse<AgentSummary>>, AppError> {
    let agent = state
        .registry
        .get(&agent_id)
        .ok_or_else(|| AppError::NotFound(format!("agent '{agent_id}' not found")))?;
    Ok(Json(ApiResponse::ok(AgentSummary::from(agent))))
}

async fn list_agent_runs<M: ModelClient>(
    State(state): State<Arc<AppState<M>>>,
    Path(agent_id): Path<String>,
    Query(query): Query<RunHistoryQueryParams>,
) -> Result<Json<ApiResponse<Vec<crate::history::store::RunSummary>>>, AppError> {
    if state.registry.get(&agent_id).is_none() {
        return Err(AppError::NotFound(format!("agent '{agent_id}' not found")));
    }
    let runs = state
        .engine
        .history_store()
        .list_runs(query.into_history_query(Some(agent_id)))
        .await?;
    Ok(Json(ApiResponse::ok(runs)))
}

async fn list_runs<M: ModelClient>(
    State(state): State<Arc<AppState<M>>>,
    Query(query): Query<RunHistoryQueryParams>,
) -> Result<Json<ApiResponse<Vec<crate::history::store::RunSummary>>>, AppError> {
    let runs = state
        .engine
        .history_store()
        .list_runs(query.into_history_query(None))
        .await?;
    Ok(Json(ApiResponse::ok(
        runs.into_iter()
            .filter(|run| state.registry.get(&run.agent_id).is_some())
            .collect::<Vec<_>>(),
    )))
}

async fn get_run<M: ModelClient>(
    State(state): State<Arc<AppState<M>>>,
    Path(run_id): Path<String>,
) -> Result<Json<ApiResponse<crate::history::store::RunRecord>>, AppError> {
    let run = state
        .engine
        .history_store()
        .get_run(&run_id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("run '{run_id}' not found")))?;
    Ok(Json(ApiResponse::ok(run)))
}

async fn run_agent_stream<M: ModelClient>(
    State(state): State<Arc<AppState<M>>>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
    request: Result<Json<Value>, JsonRejection>,
) -> Result<impl IntoResponse, AppError> {
    let request_context = request_context_from_headers(&headers);
    let Json(input) = request.map_err(map_run_request_rejection)?;
    let agent = state
        .registry
        .get(&agent_id)
        .ok_or_else(|| AppError::NotFound(format!("agent '{agent_id}' not found")))?
        .clone();
    let input_summary = summarize_run_input(&input);
    tracing::info!(
        request_id = %request_context.request_id,
        caller_service = ?request_context.caller_service,
        tenant_id = ?request_context.tenant_id,
        user_id = ?request_context.user_id,
        agent_id = %agent_id,
        input_keys = ?input_summary.keys,
        "agent stream request received"
    );
    tracing::debug!(
        request_id = %request_context.request_id,
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

    if let Err(errors) = compiled_schema.validate(&input) {
        let messages = errors.map(|err| err.to_string()).collect::<Vec<_>>();
        tracing::warn!(
            agent_id = %agent_id,
            request_id = %request_context.request_id,
            errors = ?messages,
            "agent run input validation failed"
        );
        return Err(AppError::Input(format!(
            "input validation failed: {}",
            messages.join("; ")
        )));
    }
    tracing::debug!(agent_id = %agent_id, request_id = %request_context.request_id, "agent run input validation passed");

    let event_encoder = state.event_encoder;
    let request_id = request_context.request_id.clone();
    let stream = state
        .engine
        .run_with_request_context(agent, input, request_context)
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

    let mut response = Sse::new(stream).into_response();
    let request_id_header = HeaderValue::from_str(&request_id)
        .map_err(|err| AppError::Run(format!("failed to encode request id header: {err}")))?;
    response
        .headers_mut()
        .insert(X_REQUEST_ID, request_id_header);
    Ok(response)
}

fn ensure_internal_auth(auth: &RuntimeAuth, headers: &HeaderMap) -> Result<(), AppError> {
    let Some(token) = auth.internal_bearer_token.as_deref() else {
        return Ok(());
    };
    let expected = format!("Bearer {token}");
    if headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == expected)
    {
        return Ok(());
    }
    Err(AppError::Unauthorized(
        "missing or invalid internal authorization".to_string(),
    ))
}

fn request_context_from_headers(headers: &HeaderMap) -> RequestContext {
    RequestContext {
        request_id: request_id_from_headers(headers),
        caller_service: optional_header(headers, &X_CALLER_SERVICE),
        tenant_id: optional_header(headers, &X_TENANT_ID),
        user_id: optional_header(headers, &X_USER_ID),
    }
}

fn request_id_from_headers(headers: &HeaderMap) -> String {
    optional_header(headers, &X_REQUEST_ID).unwrap_or_else(|| format!("req_{}", Uuid::new_v4()))
}

fn optional_header(headers: &HeaderMap, name: &HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
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

#[derive(Debug, Default, Deserialize)]
struct RunHistoryQueryParams {
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    caller_service: Option<String>,
    #[serde(default)]
    tenant_id: Option<String>,
    #[serde(default)]
    user_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

impl RunHistoryQueryParams {
    fn into_history_query(self, agent_id: Option<String>) -> RunHistoryQuery {
        RunHistoryQuery {
            agent_id,
            request_id: normalize_query_value(self.request_id),
            caller_service: normalize_query_value(self.caller_service),
            tenant_id: normalize_query_value(self.tenant_id),
            user_id: normalize_query_value(self.user_id),
            limit: self.limit.unwrap_or(50),
        }
    }
}

fn normalize_query_value(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
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
