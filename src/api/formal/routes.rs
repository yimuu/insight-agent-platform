use std::{sync::Arc, time::Duration};

use axum::{
    body::Body,
    extract::{rejection::JsonRejection, Path, State},
    http::{header, HeaderMap, HeaderName, HeaderValue, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Serialize;
use serde_json::{json, Value};

use crate::{
    dsl::vnext::compiler::CompiledWorkflow,
    runtime::{RequestMetadata, RunService},
};

use super::{
    auth::ApiAuth,
    response::{ApiError, ApiResponse},
    sse::response_stream,
};

const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");
const X_RUN_ID: HeaderName = HeaderName::from_static("x-run-id");

#[derive(Clone)]
pub struct FormalApiState {
    pub service: RunService,
    pub auth: ApiAuth,
    pub sse_keep_alive_interval: Duration,
    pub readiness_probe_timeout: Duration,
}

pub fn build_router(state: FormalApiState) -> Router {
    let state = Arc::new(state);
    let auth = state.auth.clone();
    let v1 = Router::new()
        .route("/v1/agents", get(list_agents))
        .route("/v1/agents/{agent_id}", get(get_agent))
        .route(
            "/v1/agents/{agent_id}/runs/stream",
            post(create_attached_run),
        )
        .route("/v1/agents/{agent_id}/runs", post(create_detached_run))
        .route("/v1/runs/{run_id}", get(get_run).delete(cancel_run))
        .route_layer(middleware::from_fn(
            move |headers: HeaderMap, request: Request<Body>, next: Next| {
                let auth = auth.clone();
                async move {
                    if !auth.accepts(&headers) {
                        return Err(ApiError::unauthorized());
                    }
                    Ok::<Response, ApiError>(next.run(request).await)
                }
            },
        ));

    Router::new()
        .route("/health", get(health))
        .route("/health/live", get(live))
        .route("/health/ready", get(health))
        .merge(v1)
        .with_state(state)
}

async fn live() -> Response {
    no_store(Json(ApiResponse::ok(json!({"status":"live"}))))
}

async fn health(State(state): State<Arc<FormalApiState>>) -> Response {
    if state
        .service
        .check_readiness(state.readiness_probe_timeout)
        .await
        .is_ok()
    {
        no_store((
            StatusCode::OK,
            Json(ApiResponse::ok(json!({"status":"ok"}))),
        ))
    } else {
        no_store((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiResponse {
                code: "RUNTIME_UNHEALTHY",
                message: "runtime is unhealthy",
                data: json!({"status":"degraded"}),
            }),
        ))
    }
}

fn no_store(response: impl IntoResponse) -> Response {
    let mut response = response.into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[derive(Debug, Serialize)]
struct AgentMetadata {
    id: String,
    name: String,
    description: String,
    version: String,
    input_schema: Value,
}

impl From<&CompiledWorkflow> for AgentMetadata {
    fn from(agent: &CompiledWorkflow) -> Self {
        Self {
            id: agent.ir.metadata.id.as_str().to_string(),
            name: agent.ir.metadata.name.clone(),
            description: agent.ir.metadata.description.clone(),
            version: agent.version_hash.clone(),
            input_schema: agent.input_validator().document().clone(),
        }
    }
}

async fn list_agents(
    State(state): State<Arc<FormalApiState>>,
) -> Json<ApiResponse<Vec<AgentMetadata>>> {
    Json(ApiResponse::ok(
        state
            .service
            .agents()
            .list()
            .map(|agent| AgentMetadata::from(agent.as_ref()))
            .collect(),
    ))
}

async fn get_agent(
    State(state): State<Arc<FormalApiState>>,
    Path(agent_id): Path<String>,
) -> Result<Json<ApiResponse<AgentMetadata>>, ApiError> {
    let agent = state.service.agents().get(&agent_id).ok_or_else(|| {
        ApiError::from(crate::runtime::ServiceError::new(
            "AGENT_NOT_FOUND",
            "agent not found",
        ))
    })?;
    Ok(Json(ApiResponse::ok(AgentMetadata::from(agent.as_ref()))))
}

async fn create_attached_run(
    State(state): State<Arc<FormalApiState>>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<Value>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(input) = input.map_err(ApiError::from)?;
    let attached = state
        .service
        .create_attached(
            &agent_id,
            input,
            RequestMetadata {
                request_id: request_id(&headers),
            },
        )
        .await
        .map_err(ApiError::from)?;
    let run_id = attached.run_id;
    let request_id = attached.request_id;
    let mut response =
        response_stream(attached.subscription, state.sse_keep_alive_interval).into_response();
    insert_header(&mut response, X_RUN_ID, &run_id)?;
    insert_header(&mut response, X_REQUEST_ID, &request_id)?;
    Ok(response)
}

async fn create_detached_run(
    State(state): State<Arc<FormalApiState>>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<Value>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(input) = input.map_err(ApiError::from)?;
    let record = state
        .service
        .create_detached(
            &agent_id,
            input,
            RequestMetadata {
                request_id: request_id(&headers),
            },
        )
        .await
        .map_err(ApiError::from)?;
    let request_id = record.request_id.clone();
    let run_id = record.run_id.clone();
    let mut response = (StatusCode::ACCEPTED, Json(ApiResponse::ok(record))).into_response();
    insert_header(&mut response, X_RUN_ID, &run_id)?;
    insert_header(&mut response, X_REQUEST_ID, &request_id)?;
    Ok(response)
}

async fn get_run(
    State(state): State<Arc<FormalApiState>>,
    Path(run_id): Path<String>,
) -> Result<Json<ApiResponse<crate::history::types::RunRecord>>, ApiError> {
    let run = state
        .service
        .get_run(&run_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(run)))
}

async fn cancel_run(
    State(state): State<Arc<FormalApiState>>,
    Path(run_id): Path<String>,
) -> Result<Json<ApiResponse<crate::history::types::RunRecord>>, ApiError> {
    let run = state
        .service
        .cancel(&run_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(run)))
}

fn request_id(headers: &HeaderMap) -> Option<String> {
    headers
        .get(&X_REQUEST_ID)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn insert_header(response: &mut Response, name: HeaderName, value: &str) -> Result<(), ApiError> {
    let value = HeaderValue::from_str(value).map_err(|_| ApiError::internal())?;
    response.headers_mut().insert(name, value);
    Ok(())
}
