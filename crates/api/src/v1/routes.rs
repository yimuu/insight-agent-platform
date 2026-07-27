//! Axum routes for the v1 HTTP API.

use std::{sync::Arc, time::Duration};

use axum::{
    body::{to_bytes, Body},
    extract::{rejection::JsonRejection, Path, Query, State},
    http::{header, HeaderMap, HeaderName, HeaderValue, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use insight_dsl::{
    graph::TraceOverlay, GraphAuthorDocument, GraphSemanticEditBatch, StoredGraphView,
    ViewDocument, MAX_GRAPH_DOCUMENT_BYTES,
};
use insight_engine::{history::types::RunRecord, human::HumanWorkItem};
use insight_runtime::{
    catalog::{AgentStreamingContract, DeployedAgent},
    ForkRecoveryOptions, MigrationNodeMappingRequest, RecoveryRequestMetadata, RecoveryReusePolicy,
    RecoveryRunResult, RequestMetadata, RunService, ServiceError,
};

use super::{
    auth::ApiAuth,
    response::{ApiError, ApiResponse},
    sse::response_stream,
};

const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");
const X_RUN_ID: HeaderName = HeaderName::from_static("x-run-id");
const X_RESPONSE_ID: HeaderName = HeaderName::from_static("x-response-id");
const X_ACCEL_BUFFERING: HeaderName = HeaderName::from_static("x-accel-buffering");
const X_CONTENT_TYPE_OPTIONS: HeaderName = HeaderName::from_static("x-content-type-options");
const CONTENT_SECURITY_POLICY: HeaderName = HeaderName::from_static("content-security-policy");

#[derive(Clone)]
pub struct ApiState {
    pub service: RunService,
    pub auth: ApiAuth,
    pub sse_keep_alive_interval: Duration,
    pub readiness_probe_timeout: Duration,
}

pub fn build_router(state: ApiState) -> Router {
    let state = Arc::new(state);
    let auth = state.auth.clone();
    let human_auth = state.auth.clone();
    let v1 = Router::new()
        .route("/v1/agents", get(list_agents))
        .route("/v1/agents/{agent_id}", get(get_agent))
        .route(
            "/v1/graph-agents/{agent_id}/revisions",
            post(publish_graph_revision),
        )
        .route(
            "/v1/graph-agents/{agent_id}/revisions/{definition_revision_id}",
            get(get_graph_revision),
        )
        .route(
            "/v1/graph-agents/{agent_id}/revisions/{definition_revision_id}/semantic-edits",
            post(apply_graph_semantic_edits),
        )
        .route(
            "/v1/graph-agents/{agent_id}/revisions/{definition_revision_id}/view",
            get(get_graph_view).put(put_graph_view),
        )
        .route(
            "/v1/agents/{agent_id}/deployments/{deployment_revision_id}/runs",
            post(create_pinned_run),
        )
        .route(
            "/v1/agents/{agent_id}/runs/stream",
            post(create_attached_run),
        )
        .route("/v1/agents/{agent_id}/runs", post(create_detached_run))
        .route("/v1/runs/{run_id}", get(get_run).delete(cancel_run))
        .route(
            "/v1/runs/{run_id}/artifacts/{artifact_id}",
            get(get_run_artifact),
        )
        .route(
            "/v1/runs/{run_id}/execution-graph",
            get(get_execution_graph),
        )
        .route("/v1/runs/{run_id}/trace", get(get_trace_overlay))
        .route("/v1/runs/{run_id}/pause", post(pause_run))
        .route("/v1/runs/{run_id}/resume", post(resume_run))
        .route("/v1/runs/{run_id}/redrive", post(redrive_run))
        .route("/v1/runs/{run_id}/fork", post(fork_run))
        .route("/v1/runs/{run_id}/migrate", post(migrate_run))
        .route(
            "/v1/runs/{run_id}/continue-as-new",
            post(continue_run_as_new),
        )
        .route("/v1/runs/{run_id}/signals/{signal_name}", post(signal_run))
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
    let human_tasks = Router::new()
        .route("/v1/human-tasks", get(list_human_tasks))
        .route(
            "/v1/human-tasks/{work_item_id}/claim",
            post(claim_human_task),
        )
        .route(
            "/v1/human-tasks/{work_item_id}/complete",
            post(complete_human_task),
        )
        .route_layer(middleware::from_fn(
            move |headers: HeaderMap, request: Request<Body>, next: Next| {
                let auth = human_auth.clone();
                async move {
                    if !auth.accepts_human_task(&headers) {
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
        .route("/metrics", get(metrics))
        .merge(v1)
        .merge(human_tasks)
        .with_state(state)
}

async fn live() -> Response {
    no_store(Json(ApiResponse::ok(json!({"status":"live"}))))
}

async fn metrics(State(state): State<Arc<ApiState>>) -> Response {
    let mut response = state.service.prometheus_metrics().into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    no_store(response)
}

async fn health(State(state): State<Arc<ApiState>>) -> Response {
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
    output_schema: Value,
    streaming: AgentStreamingContract,
}

impl From<&DeployedAgent> for AgentMetadata {
    fn from(agent: &DeployedAgent) -> Self {
        let published = agent.published();
        let metadata = published.metadata();
        Self {
            id: metadata.id.clone(),
            name: metadata.name.clone(),
            description: metadata.description.clone(),
            version: agent.deployment_revision_id().as_str().to_owned(),
            input_schema: published.public_input_schema(),
            output_schema: published.public_output_schema(),
            streaming: published.public_streaming_contract(),
        }
    }
}

async fn list_agents(
    State(state): State<Arc<ApiState>>,
) -> Result<Json<ApiResponse<Vec<AgentMetadata>>>, ApiError> {
    let agents = state
        .service
        .list_current_agents()
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(
        agents
            .iter()
            .map(|agent| AgentMetadata::from(agent.as_ref()))
            .collect(),
    )))
}

async fn get_agent(
    State(state): State<Arc<ApiState>>,
    Path(agent_id): Path<String>,
) -> Result<Json<ApiResponse<AgentMetadata>>, ApiError> {
    let agent = state
        .service
        .get_current_agent(&agent_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(AgentMetadata::from(agent.as_ref()))))
}

async fn publish_graph_revision(
    State(state): State<Arc<ApiState>>,
    Path(agent_id): Path<String>,
    body: Body,
) -> Result<Response, ApiError> {
    let bytes = strict_document_body(body, "GRAPH_DOCUMENT_INVALID").await?;
    let graph = GraphAuthorDocument::decode_json(&bytes).map_err(|_| {
        ApiError::from(ServiceError::new(
            "GRAPH_DOCUMENT_INVALID",
            "graph author document is invalid",
        ))
    })?;
    let publication = state
        .service
        .publish_graph(&agent_id, graph)
        .await
        .map_err(ApiError::from)?;
    Ok((StatusCode::CREATED, Json(ApiResponse::ok(publication))).into_response())
}

async fn get_graph_revision(
    State(state): State<Arc<ApiState>>,
    Path((agent_id, definition_revision_id)): Path<(String, String)>,
) -> Result<Json<ApiResponse<GraphAuthorDocument>>, ApiError> {
    let graph = state
        .service
        .graph_author(&agent_id, &definition_revision_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(graph)))
}

async fn apply_graph_semantic_edits(
    State(state): State<Arc<ApiState>>,
    Path((agent_id, definition_revision_id)): Path<(String, String)>,
    body: Body,
) -> Result<Response, ApiError> {
    let bytes = strict_document_body(body, "GRAPH_EDIT_INVALID").await?;
    let batch = GraphSemanticEditBatch::decode_json(&bytes).map_err(|_| {
        ApiError::from(ServiceError::new(
            "GRAPH_EDIT_INVALID",
            "graph semantic edit document is invalid",
        ))
    })?;
    let publication = state
        .service
        .apply_graph_semantic_edits(&agent_id, &definition_revision_id, batch)
        .await
        .map_err(ApiError::from)?;
    Ok((StatusCode::CREATED, Json(ApiResponse::ok(publication))).into_response())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphViewQuery {
    expected_version: u64,
}

async fn get_graph_view(
    State(state): State<Arc<ApiState>>,
    Path((agent_id, definition_revision_id)): Path<(String, String)>,
) -> Result<Json<ApiResponse<StoredGraphView>>, ApiError> {
    let view = state
        .service
        .graph_view(&agent_id, &definition_revision_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(view)))
}

async fn put_graph_view(
    State(state): State<Arc<ApiState>>,
    Path((agent_id, definition_revision_id)): Path<(String, String)>,
    Query(query): Query<GraphViewQuery>,
    body: Body,
) -> Result<Json<ApiResponse<StoredGraphView>>, ApiError> {
    let bytes = strict_document_body(body, "GRAPH_VIEW_INVALID").await?;
    let view = ViewDocument::decode_json(&bytes).map_err(|_| {
        ApiError::from(ServiceError::new(
            "GRAPH_VIEW_INVALID",
            "graph view document is invalid",
        ))
    })?;
    let stored = state
        .service
        .save_graph_view(
            &agent_id,
            &definition_revision_id,
            query.expected_version,
            view,
        )
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(stored)))
}

async fn create_pinned_run(
    State(state): State<Arc<ApiState>>,
    Path((agent_id, deployment_revision_id)): Path<(String, String)>,
    headers: HeaderMap,
    input: Result<Json<Value>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(input) = input.map_err(ApiError::from)?;
    let record = state
        .service
        .create_detached_from_deployment(
            &agent_id,
            &deployment_revision_id,
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

async fn get_execution_graph(
    State(state): State<Arc<ApiState>>,
    Path(run_id): Path<String>,
) -> Result<Json<ApiResponse<GraphAuthorDocument>>, ApiError> {
    let graph = state
        .service
        .execution_graph(&run_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(graph)))
}

async fn get_trace_overlay(
    State(state): State<Arc<ApiState>>,
    Path(run_id): Path<String>,
) -> Result<Json<ApiResponse<TraceOverlay>>, ApiError> {
    let trace = state
        .service
        .trace_overlay(&run_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(trace)))
}

async fn strict_document_body(
    body: Body,
    code: &'static str,
) -> Result<axum::body::Bytes, ApiError> {
    to_bytes(body, MAX_GRAPH_DOCUMENT_BYTES + 1)
        .await
        .map_err(|_| ApiError::from(ServiceError::new(code, "graph document body is invalid")))
}

async fn create_attached_run(
    State(state): State<Arc<ApiState>>,
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
    let run_id = attached.run_id.clone();
    let response_id = attached.response_id.clone();
    let request_id = attached.request_id.clone();
    let mut response = response_stream(attached, state.sse_keep_alive_interval).into_response();
    insert_header(&mut response, X_RUN_ID, &run_id)?;
    insert_header(&mut response, X_RESPONSE_ID, &response_id)?;
    insert_header(&mut response, X_REQUEST_ID, &request_id)?;
    insert_header(
        &mut response,
        header::CACHE_CONTROL,
        "no-store, no-transform",
    )?;
    insert_header(&mut response, X_ACCEL_BUFFERING, "no")?;
    Ok(response)
}

async fn create_detached_run(
    State(state): State<Arc<ApiState>>,
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
    State(state): State<Arc<ApiState>>,
    Path(run_id): Path<String>,
) -> Result<Json<ApiResponse<RunRecord>>, ApiError> {
    let run = state
        .service
        .get_run(&run_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(run)))
}

async fn get_run_artifact(
    State(state): State<Arc<ApiState>>,
    Path((run_id, artifact_id)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let artifact = state
        .service
        .read_public_artifact(&run_id, &artifact_id)
        .await
        .map_err(ApiError::from)?;
    let media_type = artifact
        .artifact()
        .media_type()
        .unwrap_or("application/octet-stream")
        .to_owned();
    let content_length = artifact.artifact().size_bytes().to_string();
    let etag = format!("\"{}\"", artifact.artifact().content_hash().as_str());
    let mut response = Response::new(Body::from(artifact.into_bytes()));
    insert_header(&mut response, header::CONTENT_TYPE, &media_type)?;
    insert_header(&mut response, header::CONTENT_LENGTH, &content_length)?;
    insert_header(&mut response, header::ETAG, &etag)?;
    insert_header(
        &mut response,
        header::CACHE_CONTROL,
        "private, no-store, no-transform",
    )?;
    insert_header(&mut response, X_CONTENT_TYPE_OPTIONS, "nosniff")?;
    insert_header(
        &mut response,
        CONTENT_SECURITY_POLICY,
        "sandbox; default-src 'none'",
    )?;
    Ok(response)
}

async fn cancel_run(
    State(state): State<Arc<ApiState>>,
    Path(run_id): Path<String>,
) -> Result<Json<ApiResponse<RunRecord>>, ApiError> {
    let run = state
        .service
        .cancel(&run_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(run)))
}

async fn pause_run(
    State(state): State<Arc<ApiState>>,
    Path(run_id): Path<String>,
) -> Result<Json<ApiResponse<RunRecord>>, ApiError> {
    let run = state.service.pause(&run_id).await.map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(run)))
}

async fn resume_run(
    State(state): State<Arc<ApiState>>,
    Path(run_id): Path<String>,
) -> Result<Json<ApiResponse<RunRecord>>, ApiError> {
    let run = state
        .service
        .resume(&run_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(run)))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryBaseRequest {
    expected_projection_version: u64,
    reuse_policy: RecoveryReusePolicy,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ForkRequest {
    expected_projection_version: u64,
    reuse_policy: RecoveryReusePolicy,
    target_deployment_revision_id: Option<String>,
    checkpoint_id: Option<String>,
    input: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryInputRequest {
    expected_projection_version: u64,
    reuse_policy: RecoveryReusePolicy,
    input: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MigrateRequest {
    expected_projection_version: u64,
    reuse_policy: RecoveryReusePolicy,
    target_agent_id: String,
    input: Value,
    #[serde(default)]
    mappings: Vec<MigrationNodeMappingRequest>,
}

async fn redrive_run(
    State(state): State<Arc<ApiState>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<RecoveryBaseRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<RecoveryRunResult>>, ApiError> {
    let Json(input) = input.map_err(ApiError::from)?;
    let result = state
        .service
        .redrive(
            &run_id,
            recovery_metadata(
                &headers,
                input.expected_projection_version,
                input.reuse_policy,
            )?,
        )
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(result)))
}

async fn fork_run(
    State(state): State<Arc<ApiState>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<ForkRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<RecoveryRunResult>>, ApiError> {
    let Json(input) = input.map_err(ApiError::from)?;
    let result = state
        .service
        .fork_with_options(
            &run_id,
            ForkRecoveryOptions {
                target_deployment_revision_id: input.target_deployment_revision_id,
                checkpoint_id: input.checkpoint_id,
                input_override: input.input,
            },
            recovery_metadata(
                &headers,
                input.expected_projection_version,
                input.reuse_policy,
            )?,
        )
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(result)))
}

async fn migrate_run(
    State(state): State<Arc<ApiState>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<MigrateRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<RecoveryRunResult>>, ApiError> {
    let Json(input) = input.map_err(ApiError::from)?;
    let result = state
        .service
        .migrate(
            &run_id,
            &input.target_agent_id,
            input.input,
            input.mappings,
            recovery_metadata(
                &headers,
                input.expected_projection_version,
                input.reuse_policy,
            )?,
        )
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(result)))
}

async fn continue_run_as_new(
    State(state): State<Arc<ApiState>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<RecoveryInputRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<RecoveryRunResult>>, ApiError> {
    let Json(input) = input.map_err(ApiError::from)?;
    let result = state
        .service
        .continue_as_new(
            &run_id,
            input.input,
            recovery_metadata(
                &headers,
                input.expected_projection_version,
                input.reuse_policy,
            )?,
        )
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(result)))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SignalRequest {
    message_id: String,
    value: Value,
}

async fn signal_run(
    State(state): State<Arc<ApiState>>,
    Path((run_id, signal_name)): Path<(String, String)>,
    input: Result<Json<SignalRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<RunRecord>>, ApiError> {
    let Json(input) = input.map_err(ApiError::from)?;
    let run = state
        .service
        .signal(&run_id, &signal_name, &input.message_id, input.value)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(run)))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HumanTaskListQuery {
    limit: Option<u32>,
}

async fn list_human_tasks(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    Query(query): Query<HumanTaskListQuery>,
) -> Result<Json<ApiResponse<Vec<HumanWorkItem>>>, ApiError> {
    let (identity, groups) = human_principal(&state.auth, &headers)?;
    let items = state
        .service
        .list_human_tasks(&identity, groups, query.limit.unwrap_or(100))
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(items)))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HumanTaskClaimRequest {}

async fn claim_human_task(
    State(state): State<Arc<ApiState>>,
    Path(work_item_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<HumanTaskClaimRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<HumanWorkItem>>, ApiError> {
    let Json(_input) = input.map_err(ApiError::from)?;
    let (identity, groups) = human_principal(&state.auth, &headers)?;
    let request_id = request_id(&headers).ok_or_else(|| {
        ApiError::from(ServiceError::new(
            "HUMAN_TASK_REQUEST_INVALID",
            "human task mutation requires a stable x-request-id",
        ))
    })?;
    let item = state
        .service
        .claim_human_task(&work_item_id, &identity, groups, &request_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(item)))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HumanTaskCompleteRequest {
    claim_fence: u64,
    value: Value,
}

async fn complete_human_task(
    State(state): State<Arc<ApiState>>,
    Path(work_item_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<HumanTaskCompleteRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<HumanWorkItem>>, ApiError> {
    let Json(input) = input.map_err(ApiError::from)?;
    let (identity, groups) = human_principal(&state.auth, &headers)?;
    let request_id = request_id(&headers).ok_or_else(|| {
        ApiError::from(ServiceError::new(
            "HUMAN_TASK_REQUEST_INVALID",
            "human task mutation requires a stable x-request-id",
        ))
    })?;
    let item = state
        .service
        .complete_human_task(
            &work_item_id,
            &identity,
            groups,
            &request_id,
            input.claim_fence,
            input.value,
        )
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(item)))
}

fn human_principal(auth: &ApiAuth, headers: &HeaderMap) -> Result<(String, Vec<String>), ApiError> {
    auth.human_principal(headers).ok_or_else(|| {
        ApiError::from(ServiceError::new(
            "HUMAN_TASK_IDENTITY_UNAVAILABLE",
            "human task API requires an authenticated principal resolver",
        ))
    })
}

fn request_id(headers: &HeaderMap) -> Option<String> {
    headers
        .get(&X_REQUEST_ID)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn recovery_metadata(
    headers: &HeaderMap,
    expected_source_projection_version: u64,
    reuse_policy: RecoveryReusePolicy,
) -> Result<RecoveryRequestMetadata, ApiError> {
    let request_id = request_id(headers).ok_or_else(|| {
        ApiError::from(ServiceError::new(
            "RECOVERY_REQUEST_INVALID",
            "recovery requires a stable x-request-id",
        ))
    })?;
    Ok(RecoveryRequestMetadata {
        request_id,
        expected_source_projection_version,
        reuse_policy,
    })
}

fn insert_header(response: &mut Response, name: HeaderName, value: &str) -> Result<(), ApiError> {
    let value = HeaderValue::from_str(value).map_err(|_| ApiError::internal())?;
    response.headers_mut().insert(name, value);
    Ok(())
}
