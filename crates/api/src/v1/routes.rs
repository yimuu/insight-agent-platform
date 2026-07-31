//! Axum routes for the v1 HTTP API.

use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use axum::{
    body::{to_bytes, Body, Bytes},
    extract::{
        rejection::{JsonRejection, QueryRejection},
        Extension, Path, Query, State,
    },
    http::{header, HeaderMap, HeaderName, HeaderValue, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use insight_dsl::{
    GraphAuthorDocument, GraphSemanticEditBatch, StoredGraphView, ViewDocument,
    MAX_GRAPH_DOCUMENT_BYTES,
};
use insight_durable::McpInteractionDurableRepository;
use insight_engine::{history::types::RunRecord, human::HumanWorkItem};
use insight_runtime::{
    catalog::{AgentMcpCapabilitySummary, AgentStreamingContract, DeployedAgent},
    AnyAttachedRun, ConversationResponseVisibilityGuard, ConversationStreamDelivery,
    ForkRecoveryOptions, MessageCursor, MigrationNodeMappingRequest, RecoveryRequestMetadata,
    RecoveryReusePolicy, RequestMetadata, RunService, ServiceError,
};

use super::{
    auth::ApiAuth,
    conversation::{
        AppendConversationMessageRequest, ArchiveConversationRequest, ConversationDeleteDto,
        ConversationMessagePageDto, ConversationMessagesQuery, ConversationPrincipal,
        ConversationTurnDto, CreateConversationRequest,
    },
    mcp_interactions::{build_mcp_interaction_router, McpInteractionApiState},
    mcp_oauth::{build_mcp_oauth_router, McpOAuthApiState},
    models::{
        run_persistence_capability_for_mode, PersistenceMode, RecoveryCapability, RecoveryRunDto,
        RunDto, RunPersistenceCapability,
    },
    response::{ApiError, ApiResponse},
    sse::{
        full_conversation_run_stream, full_conversation_run_stream_with_interactions, run_stream,
        run_stream_with_interactions, terminal_conversation_run_stream,
        terminal_conversation_run_stream_with_interactions, terminal_run_stream,
        terminal_run_stream_with_interactions,
    },
};

const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");
const X_TENANT_ID: HeaderName = HeaderName::from_static("x-tenant-id");
const X_USER_ID: HeaderName = HeaderName::from_static("x-user-id");
const X_RUN_ID: HeaderName = HeaderName::from_static("x-run-id");
const X_MESSAGE_ID: HeaderName = HeaderName::from_static("x-message-id");
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

#[derive(Clone)]
struct McpInteractionStreamAuthority {
    repository: Arc<dyn McpInteractionDurableRepository>,
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
        .route("/v1/conversations", post(create_conversation))
        .route(
            "/v1/conversations/{conversation_id}",
            get(get_conversation).delete(delete_conversation),
        )
        .route(
            "/v1/conversations/{conversation_id}/messages",
            get(list_conversation_messages).post(create_conversation_detached_turn),
        )
        .route(
            "/v1/conversations/{conversation_id}/messages/stream",
            post(create_conversation_attached_turn),
        )
        .route(
            "/v1/conversations/{conversation_id}/archive",
            post(archive_conversation),
        )
        .route_layer(middleware::from_fn(
            move |headers: HeaderMap, request: Request<Body>, next: Next| {
                let auth = auth.clone();
                async move {
                    if !auth.accepts(&headers) {
                        return Err(ApiError::unauthorized());
                    }
                    if has_ambiguous_principal_headers(&headers) {
                        return Err(ApiError::input_invalid());
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

pub fn build_router_with_mcp_interactions(
    state: ApiState,
    repository: Arc<dyn insight_durable::McpInteractionDurableRepository>,
    protector: Arc<dyn insight_durable::McpSecretProtector>,
) -> Router {
    let interaction_stream = McpInteractionStreamAuthority {
        repository: Arc::clone(&repository),
    };
    let mcp = build_mcp_interaction_router(McpInteractionApiState {
        auth: state.auth.clone(),
        repository,
        protector,
    });
    build_router(state)
        .merge(mcp)
        .layer(Extension(interaction_stream))
}

pub fn build_router_with_mcp(
    state: ApiState,
    interaction_repository: Arc<dyn insight_durable::McpInteractionDurableRepository>,
    protector: Arc<dyn insight_durable::McpSecretProtector>,
    oauth: Option<McpOAuthApiState>,
) -> Router {
    let interaction_stream = McpInteractionStreamAuthority {
        repository: Arc::clone(&interaction_repository),
    };
    let interactions = build_mcp_interaction_router(McpInteractionApiState {
        auth: state.auth.clone(),
        repository: interaction_repository,
        protector,
    });
    let mut router = build_router(state).merge(interactions);
    if let Some(oauth) = oauth {
        router = router.merge(build_mcp_oauth_router(oauth));
    }
    router.layer(Extension(interaction_stream))
}

async fn live() -> Response {
    no_store(Json(ApiResponse::ok(json!({"status":"live"}))))
}

async fn metrics(State(state): State<Arc<ApiState>>) -> Response {
    let mut output = state.service.prometheus_metrics();
    output.push_str(&insight_mcp::prometheus_metrics());
    let mut response = output.into_response();
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

struct GuardedResponseBody {
    inner: Pin<Box<dyn Stream<Item = Result<Bytes, axum::Error>> + Send>>,
    visibility: Option<ConversationResponseVisibilityGuard>,
    delivered: Option<ConversationStreamDelivery>,
}

impl Stream for GuardedResponseBody {
    type Item = Result<Bytes, axum::Error>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // A previously returned frame has now been handed back to transport.
        // Do not keep idle or unpolled bodies in the DELETE quiescence count.
        self.delivered = None;
        if self
            .visibility
            .as_ref()
            .is_some_and(ConversationResponseVisibilityGuard::is_cancelled)
        {
            self.visibility = None;
            return Poll::Ready(None);
        }

        match self.inner.as_mut().poll_next(context) {
            Poll::Ready(Some(item)) => {
                // Re-check cancellation while atomically entering the delivery
                // count. If DELETE won the race, discard the private bytes.
                let Some(delivery) = self
                    .visibility
                    .as_ref()
                    .and_then(ConversationResponseVisibilityGuard::try_begin_delivery)
                else {
                    self.visibility = None;
                    return Poll::Ready(None);
                };
                self.delivered = Some(delivery);
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => {
                self.visibility = None;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

fn with_conversation_visibility(
    response: impl IntoResponse,
    visibility: Option<ConversationResponseVisibilityGuard>,
) -> Response {
    let response = response.into_response();
    let Some(visibility) = visibility else {
        return response;
    };
    let (parts, body) = response.into_parts();
    let stream = GuardedResponseBody {
        inner: Box::pin(body.into_data_stream()),
        visibility: Some(visibility),
        delivered: None,
    };
    Response::from_parts(parts, Body::from_stream(stream))
}

fn conversation_write_response<T: Serialize>(
    status: StatusCode,
    data: T,
    request_id: &str,
) -> Result<Response, ApiError> {
    let mut response = (status, Json(ApiResponse::ok(data))).into_response();
    insert_header(&mut response, X_REQUEST_ID, request_id)?;
    Ok(no_store(response))
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
    #[serde(flatten)]
    persistence: RunPersistenceCapability,
    /// Whether this immutable Deployment Revision permits process-local waits.
    /// Such waits are never recoverable in terminal-only mode.
    volatile_waits_enabled: bool,
    wait_recovery: RecoveryCapability,
    #[serde(skip_serializing_if = "Option::is_none")]
    mcp: Option<AgentMcpCapabilitySummary>,
}

impl From<&DeployedAgent> for AgentMetadata {
    fn from(agent: &DeployedAgent) -> Self {
        Self::from_agent(agent)
    }
}

impl AgentMetadata {
    fn from_agent(agent: &DeployedAgent) -> Self {
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
            persistence: run_persistence_capability_for_mode(agent.persistence_mode()),
            volatile_waits_enabled: agent.persistence_policy().allow_volatile_waits(),
            wait_recovery: match agent.persistence_mode() {
                PersistenceMode::Full => RecoveryCapability::Full,
                PersistenceMode::TerminalOnly => RecoveryCapability::None,
            },
            mcp: agent.mcp_capability_summary(),
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
            .map(|agent| AgentMetadata::from_agent(agent.as_ref()))
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
    Ok(Json(ApiResponse::ok(AgentMetadata::from_agent(
        agent.as_ref(),
    ))))
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
    let principal = run_principal(&headers)?;
    let record = state
        .service
        .create_detached_from_deployment_for_tenant(
            &principal.tenant_id,
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
    let dto = run_dto(
        &state,
        &principal.tenant_id,
        principal.user_id.as_deref(),
        record,
    )
    .await?;
    let mut response = (StatusCode::ACCEPTED, Json(ApiResponse::ok(dto))).into_response();
    insert_header(&mut response, X_RUN_ID, &run_id)?;
    insert_header(&mut response, X_REQUEST_ID, &request_id)?;
    Ok(response)
}

async fn get_execution_graph(
    State(state): State<Arc<ApiState>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<ApiResponse<GraphAuthorDocument>>, ApiError> {
    let principal = run_principal(&headers)?;
    let graph = state
        .service
        .execution_graph_for_principal(&principal.tenant_id, principal.user_id.as_deref(), &run_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(graph)))
}

async fn get_trace_overlay(
    State(state): State<Arc<ApiState>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let principal = run_principal(&headers)?;
    let trace = state
        .service
        .trace_overlay_for_principal(&principal.tenant_id, principal.user_id.as_deref(), &run_id)
        .await
        .map_err(ApiError::from)?;
    let visibility = state
        .service
        .acquire_run_content_response_visibility(
            &principal.tenant_id,
            principal.user_id.as_deref(),
            &run_id,
        )
        .await
        .map_err(ApiError::from)?;
    Ok(with_conversation_visibility(
        no_store(Json(ApiResponse::ok(trace))),
        visibility,
    ))
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
    authority: Option<Extension<McpInteractionStreamAuthority>>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<Value>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(input) = input.map_err(ApiError::from)?;
    let principal = run_principal(&headers)?;
    let interaction_repository = authority.map(|Extension(authority)| authority.repository);
    let attached = state
        .service
        .create_attached_any(
            &principal.tenant_id,
            &agent_id,
            input,
            RequestMetadata {
                request_id: request_id(&headers),
            },
        )
        .await
        .map_err(ApiError::from)?;
    let (run_id, request_id, mut response) = match (attached, interaction_repository) {
        (AnyAttachedRun::Full(attached), Some(repository)) => (
            attached.run_id.clone(),
            attached.request_id.clone(),
            run_stream_with_interactions(attached, state.sse_keep_alive_interval, repository)
                .into_response(),
        ),
        (AnyAttachedRun::Full(attached), None) => (
            attached.run_id.clone(),
            attached.request_id.clone(),
            run_stream(attached, state.sse_keep_alive_interval).into_response(),
        ),
        (AnyAttachedRun::TerminalOnly(attached), Some(repository)) => (
            attached.run_id.clone(),
            attached.request_id.clone(),
            terminal_run_stream_with_interactions(
                attached,
                state.sse_keep_alive_interval,
                repository,
            )
            .into_response(),
        ),
        (AnyAttachedRun::TerminalOnly(attached), None) => (
            attached.run_id.clone(),
            attached.request_id.clone(),
            terminal_run_stream(attached, state.sse_keep_alive_interval).into_response(),
        ),
    };
    insert_header(&mut response, X_RUN_ID, &run_id)?;
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
    let principal = run_principal(&headers)?;
    let record = state
        .service
        .create_detached_for_tenant(
            &principal.tenant_id,
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
    let dto = run_dto(
        &state,
        &principal.tenant_id,
        principal.user_id.as_deref(),
        record,
    )
    .await?;
    let mut response = (StatusCode::ACCEPTED, Json(ApiResponse::ok(dto))).into_response();
    insert_header(&mut response, X_RUN_ID, &run_id)?;
    insert_header(&mut response, X_REQUEST_ID, &request_id)?;
    Ok(response)
}

async fn get_run(
    State(state): State<Arc<ApiState>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let principal = run_principal(&headers)?;
    let mut run = state
        .service
        .get_run_for_principal(&principal.tenant_id, principal.user_id.as_deref(), &run_id)
        .await
        .map_err(ApiError::from)?;
    let visibility = state
        .service
        .finalize_run_response_visibility(
            &principal.tenant_id,
            principal.user_id.as_deref(),
            &mut run,
        )
        .await
        .map_err(ApiError::from)?;
    let dto = run_dto(
        &state,
        &principal.tenant_id,
        principal.user_id.as_deref(),
        run,
    )
    .await?;
    Ok(with_conversation_visibility(
        no_store(Json(ApiResponse::ok(dto))),
        visibility,
    ))
}

async fn get_run_artifact(
    State(state): State<Arc<ApiState>>,
    Path((run_id, artifact_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let principal = run_principal(&headers)?;
    let artifact = state
        .service
        .read_public_artifact_for_principal(
            &principal.tenant_id,
            principal.user_id.as_deref(),
            &run_id,
            &artifact_id,
        )
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
    let visibility = state
        .service
        .acquire_run_content_response_visibility(
            &principal.tenant_id,
            principal.user_id.as_deref(),
            &run_id,
        )
        .await
        .map_err(|_| {
            ApiError::from(ServiceError::new(
                "ARTIFACT_NOT_FOUND",
                "Artifact not found",
            ))
        })?;
    Ok(with_conversation_visibility(response, visibility))
}

async fn cancel_run(
    State(state): State<Arc<ApiState>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let principal = run_principal(&headers)?;
    let mut run = state
        .service
        .cancel_for_principal(&principal.tenant_id, principal.user_id.as_deref(), &run_id)
        .await
        .map_err(ApiError::from)?;
    let visibility = state
        .service
        .finalize_run_response_visibility(
            &principal.tenant_id,
            principal.user_id.as_deref(),
            &mut run,
        )
        .await
        .map_err(ApiError::from)?;
    let dto = run_dto(
        &state,
        &principal.tenant_id,
        principal.user_id.as_deref(),
        run,
    )
    .await?;
    Ok(with_conversation_visibility(
        no_store(Json(ApiResponse::ok(dto))),
        visibility,
    ))
}

async fn pause_run(
    State(state): State<Arc<ApiState>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<ApiResponse<RunDto>>, ApiError> {
    let principal = run_principal(&headers)?;
    require_full_recovery(&state, &principal, &run_id).await?;
    let run = state.service.pause(&run_id).await.map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(
        run_dto(
            &state,
            &principal.tenant_id,
            principal.user_id.as_deref(),
            run,
        )
        .await?,
    )))
}

async fn resume_run(
    State(state): State<Arc<ApiState>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<ApiResponse<RunDto>>, ApiError> {
    let principal = run_principal(&headers)?;
    require_full_recovery(&state, &principal, &run_id).await?;
    let run = state
        .service
        .resume(&run_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(
        run_dto(
            &state,
            &principal.tenant_id,
            principal.user_id.as_deref(),
            run,
        )
        .await?,
    )))
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
) -> Result<Json<ApiResponse<RecoveryRunDto>>, ApiError> {
    let principal = run_principal(&headers)?;
    require_full_recovery(&state, &principal, &run_id).await?;
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
    Ok(Json(ApiResponse::ok(RecoveryRunDto::from(result))))
}

async fn fork_run(
    State(state): State<Arc<ApiState>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<ForkRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<RecoveryRunDto>>, ApiError> {
    let principal = run_principal(&headers)?;
    require_full_recovery(&state, &principal, &run_id).await?;
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
    Ok(Json(ApiResponse::ok(RecoveryRunDto::from(result))))
}

async fn migrate_run(
    State(state): State<Arc<ApiState>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<MigrateRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<RecoveryRunDto>>, ApiError> {
    let principal = run_principal(&headers)?;
    require_full_recovery(&state, &principal, &run_id).await?;
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
    Ok(Json(ApiResponse::ok(RecoveryRunDto::from(result))))
}

async fn continue_run_as_new(
    State(state): State<Arc<ApiState>>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<RecoveryInputRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<RecoveryRunDto>>, ApiError> {
    let principal = run_principal(&headers)?;
    require_full_recovery(&state, &principal, &run_id).await?;
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
    Ok(Json(ApiResponse::ok(RecoveryRunDto::from(result))))
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
    headers: HeaderMap,
    input: Result<Json<SignalRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<RunDto>>, ApiError> {
    let principal = run_principal(&headers)?;
    require_full_recovery(&state, &principal, &run_id).await?;
    let Json(input) = input.map_err(ApiError::from)?;
    let run = state
        .service
        .signal(&run_id, &signal_name, &input.message_id, input.value)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ApiResponse::ok(
        run_dto(
            &state,
            &principal.tenant_id,
            principal.user_id.as_deref(),
            run,
        )
        .await?,
    )))
}

async fn create_conversation(
    State(state): State<Arc<ApiState>>,
    headers: HeaderMap,
    input: Result<Json<CreateConversationRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let principal = conversation_principal(&headers)?;
    let request_id = required_conversation_request_id(&headers)?;
    let Json(input) = input.map_err(|_| conversation_request_invalid())?;
    let conversation = state
        .service
        .create_conversation(
            &principal.tenant_id,
            &principal.user_id,
            &input.agent_id,
            &request_id,
        )
        .await
        .map_err(ApiError::from)?;
    conversation_write_response(StatusCode::CREATED, conversation, &request_id)
}

async fn get_conversation(
    State(state): State<Arc<ApiState>>,
    Path(conversation_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let principal = conversation_principal(&headers)?;
    let conversation = state
        .service
        .get_conversation(&conversation_id, &principal.tenant_id, &principal.user_id)
        .await
        .map_err(ApiError::from)?;
    let visibility = state
        .service
        .acquire_conversation_response_visibility(
            &conversation_id,
            &principal.tenant_id,
            &principal.user_id,
        )
        .await
        .map_err(ApiError::from)?;
    Ok(with_conversation_visibility(
        no_store(Json(ApiResponse::ok(conversation))),
        Some(visibility),
    ))
}

async fn list_conversation_messages(
    State(state): State<Arc<ApiState>>,
    Path(conversation_id): Path<String>,
    headers: HeaderMap,
    query: Result<Query<ConversationMessagesQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let principal = conversation_principal(&headers)?;
    let Query(query) = query.map_err(|_| conversation_request_invalid())?;
    let (default_limit, max_limit) = state
        .service
        .conversation_page_limits()
        .map_err(ApiError::from)?;
    let limit = query
        .validated_limit_with(default_limit, max_limit)
        .map_err(|_| conversation_request_invalid())?;
    let before = query
        .decoded_cursor()
        .map_err(|_| conversation_request_invalid())?
        .map(|cursor| MessageCursor {
            message_order: cursor.message_order,
            message_id: cursor.message_id,
        });
    let page = state
        .service
        .list_conversation_messages(
            &conversation_id,
            &principal.tenant_id,
            &principal.user_id,
            before,
            limit,
        )
        .await
        .map_err(ApiError::from)?;
    let next_cursor =
        page.next_cursor
            .map(|cursor| super::conversation::ConversationMessageCursor {
                message_order: cursor.message_order,
                message_id: cursor.message_id,
            });
    let page = ConversationMessagePageDto::new(page.messages, next_cursor);
    let visibility = state
        .service
        .acquire_conversation_response_visibility(
            &conversation_id,
            &principal.tenant_id,
            &principal.user_id,
        )
        .await
        .map_err(ApiError::from)?;
    Ok(with_conversation_visibility(
        no_store(Json(ApiResponse::ok(page))),
        Some(visibility),
    ))
}

async fn create_conversation_detached_turn(
    State(state): State<Arc<ApiState>>,
    Path(conversation_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<AppendConversationMessageRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let principal = conversation_principal(&headers)?;
    let request_id = required_conversation_request_id(&headers)?;
    let Json(input) = input.map_err(|_| conversation_request_invalid())?;
    let turn = state
        .service
        .create_conversation_detached_turn(
            &conversation_id,
            &principal.tenant_id,
            &principal.user_id,
            &request_id,
            input.content,
        )
        .await
        .map_err(ApiError::from)?;
    let run_id = turn.run.run_id.clone();
    let message_id = turn.user_message.message_id.clone();
    let run = run_dto(
        &state,
        &principal.tenant_id,
        Some(&principal.user_id),
        turn.run,
    )
    .await?;
    let dto = ConversationTurnDto {
        user_message: turn.user_message,
        run,
        replayed: turn.replayed,
    };
    let mut response = (StatusCode::ACCEPTED, Json(ApiResponse::ok(dto))).into_response();
    insert_header(&mut response, X_REQUEST_ID, &request_id)?;
    insert_header(&mut response, X_RUN_ID, &run_id)?;
    insert_header(&mut response, X_MESSAGE_ID, &message_id)?;
    let visibility = state
        .service
        .acquire_conversation_response_visibility(
            &conversation_id,
            &principal.tenant_id,
            &principal.user_id,
        )
        .await
        .map_err(ApiError::from)?;
    Ok(with_conversation_visibility(
        no_store(response),
        Some(visibility),
    ))
}

async fn create_conversation_attached_turn(
    State(state): State<Arc<ApiState>>,
    authority: Option<Extension<McpInteractionStreamAuthority>>,
    Path(conversation_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<AppendConversationMessageRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let principal = conversation_principal(&headers)?;
    let interaction_repository = authority.map(|Extension(authority)| authority.repository);
    let request_id = required_conversation_request_id(&headers)?;
    let Json(input) = input.map_err(|_| conversation_request_invalid())?;
    let turn = state
        .service
        .create_conversation_attached_turn(
            &conversation_id,
            &principal.tenant_id,
            &principal.user_id,
            &request_id,
            input.content,
        )
        .await
        .map_err(ApiError::from)?;
    let message_id = turn.user_message.message_id;
    let (run_id, admitted_request_id, mut response) = match (turn.attached, interaction_repository)
    {
        (AnyAttachedRun::Full(attached), Some(repository)) => (
            attached.run_id.clone(),
            attached.request_id.clone(),
            full_conversation_run_stream_with_interactions(
                attached,
                state.sse_keep_alive_interval,
                state.service.clone(),
                conversation_id.clone(),
                principal.tenant_id.clone(),
                principal.user_id.clone(),
                repository,
            )
            .into_response(),
        ),
        (AnyAttachedRun::Full(attached), None) => (
            attached.run_id.clone(),
            attached.request_id.clone(),
            full_conversation_run_stream(
                attached,
                state.sse_keep_alive_interval,
                state.service.clone(),
                conversation_id.clone(),
                principal.tenant_id.clone(),
                principal.user_id.clone(),
            )
            .into_response(),
        ),
        (AnyAttachedRun::TerminalOnly(attached), Some(repository)) => (
            attached.run_id.clone(),
            attached.request_id.clone(),
            terminal_conversation_run_stream_with_interactions(
                attached,
                state.sse_keep_alive_interval,
                state.service.clone(),
                principal.tenant_id.clone(),
                principal.user_id.clone(),
                repository,
            )
            .into_response(),
        ),
        (AnyAttachedRun::TerminalOnly(attached), None) => (
            attached.run_id.clone(),
            attached.request_id.clone(),
            terminal_conversation_run_stream(
                attached,
                state.sse_keep_alive_interval,
                state.service.clone(),
                principal.tenant_id.clone(),
                principal.user_id.clone(),
            )
            .into_response(),
        ),
    };
    insert_header(&mut response, X_RUN_ID, &run_id)?;
    insert_header(&mut response, X_REQUEST_ID, &admitted_request_id)?;
    insert_header(&mut response, X_MESSAGE_ID, &message_id)?;
    insert_header(
        &mut response,
        header::CACHE_CONTROL,
        "no-store, no-transform",
    )?;
    insert_header(&mut response, X_ACCEL_BUFFERING, "no")?;
    Ok(response)
}

async fn archive_conversation(
    State(state): State<Arc<ApiState>>,
    Path(conversation_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<ArchiveConversationRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let principal = conversation_principal(&headers)?;
    let request_id = required_conversation_request_id(&headers)?;
    let Json(_input) = input.map_err(|_| conversation_request_invalid())?;
    let conversation = state
        .service
        .archive_conversation(&conversation_id, &principal.tenant_id, &principal.user_id)
        .await
        .map_err(ApiError::from)?;
    conversation_write_response(StatusCode::OK, conversation, &request_id)
}

async fn delete_conversation(
    State(state): State<Arc<ApiState>>,
    Path(conversation_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let principal = conversation_principal(&headers)?;
    let request_id = required_conversation_request_id(&headers)?;
    let deleted = state
        .service
        .delete_conversation(&conversation_id, &principal.tenant_id, &principal.user_id)
        .await
        .map_err(ApiError::from)?;
    conversation_write_response(
        StatusCode::OK,
        ConversationDeleteDto { deleted },
        &request_id,
    )
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
    exactly_one_header(headers, &X_REQUEST_ID)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty() && !value.contains(','))
        .map(ToOwned::to_owned)
}

async fn run_dto(
    state: &ApiState,
    tenant_id: &str,
    user_id: Option<&str>,
    run: RunRecord,
) -> Result<RunDto, ApiError> {
    let capability = state
        .service
        .run_persistence_capability_for_principal(tenant_id, user_id, &run.run_id)
        .await
        .map_err(ApiError::from)?;
    Ok(RunDto::new(run, capability))
}

async fn require_full_recovery(
    state: &ApiState,
    principal: &RunPrincipal,
    run_id: &str,
) -> Result<(), ApiError> {
    state
        .service
        .require_full_recovery_capability_for_principal(
            &principal.tenant_id,
            principal.user_id.as_deref(),
            run_id,
        )
        .await
        .map_err(ApiError::from)
}

fn conversation_principal(headers: &HeaderMap) -> Result<ConversationPrincipal, ApiError> {
    Ok(ConversationPrincipal {
        tenant_id: conversation_identity_header(headers, &X_TENANT_ID)?,
        user_id: conversation_identity_header(headers, &X_USER_ID)?,
    })
}

struct RunPrincipal {
    tenant_id: String,
    user_id: Option<String>,
}

fn run_principal(headers: &HeaderMap) -> Result<RunPrincipal, ApiError> {
    let tenant_id = run_tenant_id(headers)?;
    let user_id = if headers.contains_key(&X_USER_ID) {
        Some(bounded_identity_header(headers, &X_USER_ID).ok_or_else(ApiError::input_invalid)?)
    } else {
        None
    };
    Ok(RunPrincipal { tenant_id, user_id })
}

fn run_tenant_id(headers: &HeaderMap) -> Result<String, ApiError> {
    if !headers.contains_key(&X_TENANT_ID) {
        return Ok("default".to_owned());
    }
    bounded_identity_header(headers, &X_TENANT_ID).ok_or_else(ApiError::input_invalid)
}

fn required_conversation_request_id(headers: &HeaderMap) -> Result<String, ApiError> {
    request_id(headers)
        .filter(|value| {
            value.len() <= 256
                && !value.contains(',')
                && !value
                    .chars()
                    .any(|character| character.is_control() || character.is_whitespace())
        })
        .ok_or_else(conversation_request_invalid)
}

fn conversation_identity_header(
    headers: &HeaderMap,
    name: &HeaderName,
) -> Result<String, ApiError> {
    bounded_identity_header(headers, name).ok_or_else(conversation_request_invalid)
}

fn bounded_identity_header(headers: &HeaderMap, name: &HeaderName) -> Option<String> {
    exactly_one_header(headers, name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 256
                && !value.contains(',')
                && !value
                    .chars()
                    .any(|character| character.is_control() || character.is_whitespace())
        })
        .map(ToOwned::to_owned)
}

fn exactly_one_header<'a>(headers: &'a HeaderMap, name: &HeaderName) -> Option<&'a HeaderValue> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?;
    values.next().is_none().then_some(value)
}

fn has_ambiguous_principal_headers(headers: &HeaderMap) -> bool {
    [&X_TENANT_ID, &X_USER_ID, &X_REQUEST_ID]
        .into_iter()
        .any(|name| {
            let values = headers.get_all(name);
            let mut values = values.iter();
            let Some(first) = values.next() else {
                return false;
            };
            values.next().is_some() || first.to_str().map_or(true, |value| value.contains(','))
        })
}

fn conversation_request_invalid() -> ApiError {
    ApiError::from(ServiceError::new(
        "CONVERSATION_REQUEST_INVALID",
        "conversation request is invalid",
    ))
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

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue};

    use super::{
        conversation_principal, has_ambiguous_principal_headers, required_conversation_request_id,
        run_tenant_id, X_REQUEST_ID, X_TENANT_ID, X_USER_ID,
    };

    #[test]
    fn conversation_principal_requires_both_bounded_identity_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(X_TENANT_ID, HeaderValue::from_static("tenant-a"));
        assert!(conversation_principal(&headers).is_err());

        headers.insert(X_USER_ID, HeaderValue::from_static("user-a"));
        let principal = conversation_principal(&headers).unwrap();
        assert_eq!(principal.tenant_id, "tenant-a");
        assert_eq!(principal.user_id, "user-a");

        headers.insert(X_USER_ID, HeaderValue::from_static("not a principal"));
        assert!(conversation_principal(&headers).is_err());

        headers.insert(X_USER_ID, HeaderValue::from_static("user-a,user-b"));
        assert!(conversation_principal(&headers).is_err());

        headers.insert(X_USER_ID, HeaderValue::from_static("user-a"));
        headers.append(X_USER_ID, HeaderValue::from_static("user-b"));
        assert!(conversation_principal(&headers).is_err());
    }

    #[test]
    fn conversation_mutation_requires_a_nonempty_request_id() {
        let mut headers = HeaderMap::new();
        assert!(required_conversation_request_id(&headers).is_err());
        headers.insert(X_REQUEST_ID, HeaderValue::from_static("   "));
        assert!(required_conversation_request_id(&headers).is_err());
        headers.insert(X_REQUEST_ID, HeaderValue::from_static("not stable"));
        assert!(required_conversation_request_id(&headers).is_err());
        headers.insert(X_REQUEST_ID, HeaderValue::from_static("request-1"));
        assert_eq!(
            required_conversation_request_id(&headers).unwrap(),
            "request-1"
        );
        headers.append(X_REQUEST_ID, HeaderValue::from_static("request-2"));
        assert!(required_conversation_request_id(&headers).is_err());

        headers.remove(X_REQUEST_ID);
        headers.insert(
            X_REQUEST_ID,
            HeaderValue::from_static("request-1,request-2"),
        );
        assert!(required_conversation_request_id(&headers).is_err());
    }

    #[test]
    fn run_tenant_defaults_but_rejects_an_invalid_explicit_carrier() {
        let mut headers = HeaderMap::new();
        assert_eq!(run_tenant_id(&headers).unwrap(), "default");
        headers.insert(X_TENANT_ID, HeaderValue::from_static("tenant-a"));
        assert_eq!(run_tenant_id(&headers).unwrap(), "tenant-a");
        headers.insert(X_TENANT_ID, HeaderValue::from_static("not a tenant"));
        assert!(run_tenant_id(&headers).is_err());
    }

    #[test]
    fn duplicate_or_comma_merged_security_headers_are_rejected_at_the_router_boundary() {
        let mut headers = HeaderMap::new();
        headers.insert(X_TENANT_ID, HeaderValue::from_static("tenant-a"));
        headers.append(X_TENANT_ID, HeaderValue::from_static("tenant-b"));
        assert!(has_ambiguous_principal_headers(&headers));

        headers.remove(X_TENANT_ID);
        headers.insert(X_USER_ID, HeaderValue::from_static("user-a,user-b"));
        assert!(has_ambiguous_principal_headers(&headers));

        headers.remove(X_USER_ID);
        headers.insert(X_REQUEST_ID, HeaderValue::from_static("request-a"));
        assert!(!has_ambiguous_principal_headers(&headers));
    }
}
