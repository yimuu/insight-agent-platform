//! Installation-scoped Agent authoring and deployment management API.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    sync::Arc,
};

use axum::{
    body::{to_bytes, Body},
    extract::{
        rejection::{JsonRejection, QueryRejection},
        DefaultBodyLimit, Extension, Path, Query, State,
    },
    http::{header, HeaderMap, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{Duration, Utc};
use insight_dsl::{GraphAuthorDocument, GraphSemanticEditBatch, ViewDocument};
use insight_durable::{
    ActivateManagedAgentDeploymentCommand, AgentAuthoringMode, AgentDebugSession, AgentDebugStatus,
    AgentLifecycle, AgentManagementConflict, AgentManagementDurableRepository,
    AgentManagementWriteError, AgentMutationMetadata, AgentMutationReceipt, AgentOperationStatus,
    AgentValidationReport, ArchiveAgentCommand, CancelAgentDebugSessionCommand,
    CompleteAgentDebugSessionCommand, CreateAgentCommand, CreateAgentDebugSessionCommand,
    CreateAgentResolutionCommand, CreateAgentValidationCommand, DeactivateManagedAgentCommand,
    DeleteAgentCommand, InstallAgentDeploymentCommand, PublishAgentDefinitionCommand,
    RecordAgentManagementRejectionCommand, ReplaceAgentDraftCommand, ReplaceAgentDraftViewCommand,
    RestoreAgentCommand, UpdateAgentLabelsCommand, VersionedPlan,
    AGENT_MANAGEMENT_MAX_REQUEST_ID_BYTES,
};
use insight_engine::{history::types::RunStatus, plan::NodeKind, DefinitionRevisionId};
use insight_runtime::{
    catalog::{compile_managed_agent_package, freeze_managed_agent_definition, PublishedAgent},
    run_service::ManagedDeploymentCandidate,
    AnyAttachedRun, RunService,
};
use ring::hmac;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    auth::{
        OperatorAuth, OperatorPrincipal, AGENT_ACTIVATE, AGENT_ARCHIVE, AGENT_DEBUG_LIVE,
        AGENT_DEBUG_SANDBOX, AGENT_DEPLOY, AGENT_PUBLISH, AGENT_READ, AGENT_VALIDATE, AGENT_WRITE,
    },
    response::ApiResponse,
    sse::{run_stream, terminal_run_stream},
};

const MAX_MANAGEMENT_BODY_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_PAGE_SIZE: u32 = 50;
const MAX_PAGE_SIZE: u32 = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentDebugProfileMode {
    Sandbox,
    Live,
}

impl AgentDebugProfileMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Sandbox => "sandbox",
            Self::Live => "live",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AgentDebugProfilePolicy {
    pub mode: AgentDebugProfileMode,
    pub max_concurrent_sessions: u32,
    pub session_timeout: Duration,
    pub content_retention: Duration,
    pub allow_external_actions: bool,
    pub allow_live_provider_credentials: bool,
}

#[derive(Clone)]
pub struct AgentManagementPolicy {
    pub validation_policy_digest: String,
    pub cursor_signing_key: [u8; 32],
    pub max_prompt_files: usize,
    pub max_prompt_file_bytes: usize,
    pub max_package_bytes: usize,
    pub resolution_ttl: Duration,
    pub debug_profiles: BTreeMap<String, AgentDebugProfilePolicy>,
}

impl std::fmt::Debug for AgentManagementPolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentManagementPolicy")
            .field("validation_policy_digest", &self.validation_policy_digest)
            .field("max_prompt_files", &self.max_prompt_files)
            .field("max_package_bytes", &self.max_package_bytes)
            .field("resolution_ttl", &self.resolution_ttl)
            .field("debug_profiles", &self.debug_profiles)
            .field("cursor_signing_key", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone)]
pub struct AgentManagementApiState {
    pub auth: OperatorAuth,
    pub repository: Arc<dyn AgentManagementDurableRepository>,
    pub run_service: RunService,
    pub policy: Arc<AgentManagementPolicy>,
    pub debug_stream_keep_alive_interval: std::time::Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentPromptFile {
    pub path: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentDraftSource {
    YamlPackage {
        agent_yaml: String,
        #[serde(default)]
        prompt_files: Vec<AgentPromptFile>,
    },
    Graph {
        document: Value,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentDraftDocument {
    pub source: AgentDraftSource,
}

#[derive(Debug)]
struct ManagementError {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    data: Value,
}

impl ManagementError {
    fn new(status: StatusCode, code: &'static str, message: &'static str) -> Self {
        Self {
            status,
            code,
            message,
            data: Value::Null,
        }
    }
    fn with_data(mut self, data: Value) -> Self {
        self.data = data;
        self
    }
    fn invalid() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "AGENT_MANAGEMENT_INVALID_REQUEST",
            "Agent management request is invalid",
        )
    }
    fn unauthorized() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "Operator authentication is required",
        )
    }
    fn forbidden() -> Self {
        Self::new(
            StatusCode::FORBIDDEN,
            "AGENT_MANAGEMENT_FORBIDDEN",
            "Operator capability is required",
        )
    }
    fn not_found() -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "AGENT_MANAGEMENT_NOT_FOUND",
            "Agent management object was not found",
        )
    }
    fn precondition_required() -> Self {
        Self::new(
            StatusCode::PRECONDITION_REQUIRED,
            "AGENT_MANAGEMENT_PRECONDITION_REQUIRED",
            "If-Match is required",
        )
    }
    fn internal() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL",
            "internal server error",
        )
    }
}

impl IntoResponse for ManagementError {
    fn into_response(self) -> Response {
        private_no_store((
            self.status,
            Json(json!({"code":self.code,"message":self.message,"data":self.data})),
        ))
    }
}

impl From<AgentManagementWriteError> for ManagementError {
    fn from(error: AgentManagementWriteError) -> Self {
        let AgentManagementWriteError::Conflict(conflict) = error else {
            return Self::internal();
        };
        match conflict {
            AgentManagementConflict::NotFound => Self::not_found(),
            AgentManagementConflict::ForbiddenState
            | AgentManagementConflict::Referenced
            | AgentManagementConflict::DebugSessionActive => Self::new(
                StatusCode::CONFLICT,
                "AGENT_STATE_CONFLICT",
                "Agent state does not allow this operation",
            ),
            AgentManagementConflict::PreconditionFailed => Self::new(
                StatusCode::PRECONDITION_FAILED,
                "AGENT_MANAGEMENT_PRECONDITION_FAILED",
                "Agent management precondition failed",
            ),
            AgentManagementConflict::IdempotencyKeyReused => Self::new(
                StatusCode::CONFLICT,
                "AGENT_IDEMPOTENCY_KEY_REUSED",
                "request ID was already used for a different request",
            ),
            AgentManagementConflict::ValidationStale => Self::new(
                StatusCode::CONFLICT,
                "AGENT_VALIDATION_STALE",
                "Agent validation evidence is stale",
            ),
            AgentManagementConflict::ValidationFailed => Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "AGENT_VALIDATION_FAILED",
                "Agent validation failed",
            ),
            AgentManagementConflict::ResolutionExpired => Self::new(
                StatusCode::CONFLICT,
                "AGENT_RESOLUTION_EXPIRED",
                "Agent Deployment Resolution expired",
            ),
            AgentManagementConflict::DependencyHeadChanged => Self::new(
                StatusCode::CONFLICT,
                "AGENT_DEPENDENCY_HEAD_CHANGED",
                "an exact dependency head changed",
            ),
            AgentManagementConflict::CapacityExceeded => Self::new(
                StatusCode::TOO_MANY_REQUESTS,
                "AGENT_OPERATION_CAPACITY_EXCEEDED",
                "Agent operation capacity was exceeded",
            ),
        }
    }
}

pub fn build_agent_management_router(state: AgentManagementApiState) -> Router {
    let auth = state.auth.clone();
    let audit_repository = Arc::clone(&state.repository);
    Router::new()
        .route("/v1/admin/agents", get(list_agents).post(create_agent))
        .route(
            "/v1/admin/agents/{agent_id}",
            get(get_agent).patch(update_agent).delete(delete_agent),
        )
        .route(
            "/v1/admin/agents/{agent_id}/draft",
            get(get_draft).put(replace_draft),
        )
        .route(
            "/v1/admin/agents/{agent_id}/draft/semantic-edits",
            post(apply_semantic_edits),
        )
        .route(
            "/v1/admin/agents/{agent_id}/draft/view",
            get(get_draft_view).put(replace_draft_view),
        )
        .route(
            "/v1/admin/agents/{agent_id}/validations",
            post(create_validation),
        )
        .route(
            "/v1/admin/agents/{agent_id}/validations/{validation_id}",
            get(get_validation),
        )
        .route(
            "/v1/admin/agents/{agent_id}/revisions",
            get(list_revisions).post(publish_revision),
        )
        .route(
            "/v1/admin/agents/{agent_id}/revisions/{definition_revision_id}",
            get(get_revision),
        )
        .route(
            "/v1/admin/agents/{agent_id}/deployment-resolutions",
            post(create_resolution),
        )
        .route(
            "/v1/admin/agents/{agent_id}/deployment-resolutions/{resolution_id}",
            get(get_resolution),
        )
        .route(
            "/v1/admin/agents/{agent_id}/deployments",
            get(list_deployments).post(create_deployment),
        )
        .route(
            "/v1/admin/agents/{agent_id}/deployments/{deployment_revision_id}",
            get(get_deployment),
        )
        .route(
            "/v1/admin/agents/{agent_id}/active-deployment",
            get(get_active_deployment)
                .put(activate_deployment)
                .delete(deactivate_agent),
        )
        .route("/v1/admin/agents/{agent_id}/archive", post(archive_agent))
        .route("/v1/admin/agents/{agent_id}/restore", post(restore_agent))
        .route(
            "/v1/admin/agents/{agent_id}/debug-sessions",
            get(list_debug_sessions).post(create_debug_session),
        )
        .route(
            "/v1/admin/agents/{agent_id}/debug-sessions/{debug_session_id}",
            get(get_debug_session).delete(cancel_debug_session),
        )
        .route(
            "/v1/admin/agents/{agent_id}/debug-sessions/{debug_session_id}/stream",
            get(stream_debug_session),
        )
        .route_layer(DefaultBodyLimit::max(MAX_MANAGEMENT_BODY_BYTES))
        .route_layer(middleware::from_fn(
            move |headers: HeaderMap,
                  mut request: axum::http::Request<axum::body::Body>,
                  next: Next| {
                let auth = auth.clone();
                let audit_repository = Arc::clone(&audit_repository);
                async move {
                    let method = request.method().clone();
                    let path = request.uri().path().to_owned();
                    let agent_id = management_agent_id(&path);
                    let request_id = headers
                        .get("x-request-id")
                        .and_then(|value| value.to_str().ok())
                        .filter(|value| value.len() <= AGENT_MANAGEMENT_MAX_REQUEST_ID_BYTES)
                        .unwrap_or("missing")
                        .to_owned();
                    let principal = auth.resolve(&headers);
                    let mut early_response = None;
                    if request.method() == axum::http::Method::DELETE {
                        let (parts, body) = request.into_parts();
                        let bytes = match to_bytes(body, MAX_MANAGEMENT_BODY_BYTES).await {
                            Ok(bytes) => bytes,
                            Err(_) => {
                                early_response = Some(ManagementError::invalid().into_response());
                                Default::default()
                            }
                        };
                        if early_response.is_none() && !bytes.is_empty() {
                            early_response = Some(ManagementError::invalid().into_response());
                        }
                        request = axum::http::Request::from_parts(parts, Body::empty());
                    }
                    let mut response = match principal.clone() {
                        Some(principal) => {
                            if let Some(response) = early_response {
                                response
                            } else {
                                match super::strict_json::validate_request(
                                    request,
                                    MAX_MANAGEMENT_BODY_BYTES,
                                )
                                .await
                                {
                                    Ok(mut request) => {
                                        request.extensions_mut().insert(principal);
                                        next.run(request).await
                                    }
                                    Err(()) => ManagementError::invalid().into_response(),
                                }
                            }
                        }
                        None => ManagementError::unauthorized().into_response(),
                    };
                    if !response.status().is_success() {
                        if let Some(principal) = principal {
                            let (capability, subject_id) =
                                agent_rejection_class(&principal, &method, &path);
                            let _ = audit_repository
                                .record_agent_management_rejection(
                                    RecordAgentManagementRejectionCommand {
                                        actor_id: principal.identity().to_owned(),
                                        capability: capability.to_owned(),
                                        request_id,
                                        agent_id,
                                        subject_id: subject_id.to_owned(),
                                        result_code: format!("http_{}", response.status().as_u16()),
                                        now: Utc::now(),
                                    },
                                )
                                .await;
                        }
                    }
                    response.headers_mut().insert(
                        header::CACHE_CONTROL,
                        HeaderValue::from_static("private, no-store"),
                    );
                    response.headers_mut().insert(
                        header::HeaderName::from_static("x-content-type-options"),
                        HeaderValue::from_static("nosniff"),
                    );
                    response
                }
            },
        ))
        .with_state(Arc::new(state))
}

fn management_agent_id(path: &str) -> Option<String> {
    path.strip_prefix("/v1/admin/agents/")
        .and_then(|suffix| suffix.split('/').next())
        .filter(|value| !value.is_empty() && value.len() <= 256)
        .map(str::to_owned)
}

fn agent_rejection_class<'a>(
    principal: &OperatorPrincipal,
    method: &axum::http::Method,
    path: &str,
) -> (&'a str, &'a str) {
    if path.contains("/debug-sessions") {
        return (
            if principal.has_capability(AGENT_DEBUG_LIVE) {
                AGENT_DEBUG_LIVE
            } else {
                AGENT_DEBUG_SANDBOX
            },
            "agent.debug_session",
        );
    }
    if method == axum::http::Method::GET {
        return (AGENT_READ, "agent.read");
    }
    if path.contains("/validations") {
        return (AGENT_VALIDATE, "agent.validation");
    }
    if path.ends_with("/revisions") {
        return (AGENT_PUBLISH, "agent.definition");
    }
    if path.contains("/active-deployment") {
        return (AGENT_ACTIVATE, "agent.active_deployment");
    }
    if path.ends_with("/archive") || path.ends_with("/restore") {
        return (AGENT_ARCHIVE, "agent.archive");
    }
    if path.contains("/deployment-resolutions") || path.contains("/deployments") {
        return (AGENT_DEPLOY, "agent.deployment");
    }
    (AGENT_WRITE, "agent.write")
}

fn private_no_store(response: impl IntoResponse) -> Response {
    let mut response = response.into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    response.headers_mut().insert(
        header::HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    response
}

fn require(principal: &OperatorPrincipal, capability: &str) -> Result<(), ManagementError> {
    principal
        .has_capability(capability)
        .then_some(())
        .ok_or_else(ManagementError::forbidden)
}

fn valid_agent_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && !value.starts_with('-')
        && !value.ends_with('-')
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_'
        })
}

fn sha256(bytes: &[u8]) -> String {
    let mut result = String::from("sha256:");
    for byte in Sha256::digest(bytes) {
        write!(&mut result, "{byte:02x}").expect("String write");
    }
    result
}

fn canonical_hash(value: &Value) -> Result<String, ManagementError> {
    serde_jcs::to_vec(value)
        .map(|bytes| sha256(&bytes))
        .map_err(|_| ManagementError::invalid())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 71
        && value.strip_prefix("sha256:").is_some_and(|digest| {
            digest
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        })
}

fn opaque_encode(value: &str, key: &[u8; 32]) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA256, key);
    let signature = hmac::sign(&key, value.as_bytes());
    format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(value),
        URL_SAFE_NO_PAD.encode(signature.as_ref())
    )
}

fn opaque_decode(value: Option<&str>, key: &[u8; 32]) -> Result<Option<String>, ManagementError> {
    value
        .map(|value| {
            let (payload, signature) =
                value.split_once('.').ok_or_else(ManagementError::invalid)?;
            let decoded = URL_SAFE_NO_PAD
                .decode(payload)
                .ok()
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .filter(|value| value.len() <= 256 && !value.chars().any(char::is_control))
                .ok_or_else(ManagementError::invalid)?;
            let signature = URL_SAFE_NO_PAD
                .decode(signature)
                .map_err(|_| ManagementError::invalid())?;
            let key = hmac::Key::new(hmac::HMAC_SHA256, key);
            hmac::verify(&key, decoded.as_bytes(), &signature)
                .map_err(|_| ManagementError::invalid())?;
            Ok(decoded)
        })
        .transpose()
}

fn parse_etag(headers: &HeaderMap, prefix: &str) -> Result<u64, ManagementError> {
    headers
        .get(header::IF_MATCH)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(ManagementError::precondition_required)?
        .strip_prefix(&format!("\"{prefix}-"))
        .and_then(|value| value.strip_suffix('"'))
        .and_then(|value| value.parse().ok())
        .ok_or_else(ManagementError::invalid)
}

fn mutation_metadata(
    principal: &OperatorPrincipal,
    capability: &str,
    headers: &HeaderMap,
    method: &str,
    path: String,
    body: &Value,
) -> Result<AgentMutationMetadata, ManagementError> {
    let request_id = headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| {
            !value.is_empty()
                && value.len() <= AGENT_MANAGEMENT_MAX_REQUEST_ID_BYTES
                && !value.chars().any(char::is_control)
        })
        .ok_or_else(ManagementError::invalid)?;
    Ok(AgentMutationMetadata {
        operator_id: principal.identity().to_owned(),
        capability: capability.to_owned(),
        method: method.to_owned(),
        canonical_path: path,
        request_id: request_id.to_owned(),
        request_hash: canonical_hash(body)?,
        now: Utc::now(),
    })
}

fn receipt_response(receipt: AgentMutationReceipt) -> Result<Response, ManagementError> {
    let status = StatusCode::from_u16(receipt.status).map_err(|_| ManagementError::internal())?;
    let mut response = private_no_store((status, Json(ApiResponse::ok(receipt.response))));
    if let Some(etag) = receipt.etag {
        response.headers_mut().insert(
            header::ETAG,
            HeaderValue::from_str(&etag).map_err(|_| ManagementError::internal())?,
        );
    }
    if receipt.replayed {
        response
            .headers_mut()
            .insert("idempotent-replayed", HeaderValue::from_static("true"));
    }
    Ok(response)
}

fn page_limit(value: Option<u32>) -> Result<u32, ManagementError> {
    let value = value.unwrap_or(DEFAULT_PAGE_SIZE);
    (value > 0 && value <= MAX_PAGE_SIZE)
        .then_some(value)
        .ok_or_else(ManagementError::invalid)
}

fn validate_prompt_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 512
        && !path.starts_with('/')
        && !path.contains('\\')
        && path
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}

fn normalize_draft(
    draft: AgentDraftDocument,
    mode: AgentAuthoringMode,
    policy: &AgentManagementPolicy,
) -> Result<AgentDraftDocument, ManagementError> {
    match (&draft.source, mode) {
        (AgentDraftSource::YamlPackage { .. }, AgentAuthoringMode::YamlPackage)
        | (AgentDraftSource::Graph { .. }, AgentAuthoringMode::Graph) => {}
        _ => return Err(ManagementError::invalid()),
    }
    if let AgentDraftSource::YamlPackage {
        agent_yaml,
        prompt_files,
    } = &draft.source
    {
        if agent_yaml.is_empty()
            || prompt_files.len() > policy.max_prompt_files
            || prompt_files.iter().any(|file| {
                !validate_prompt_path(&file.path)
                    || file.content.len() > policy.max_prompt_file_bytes
            })
        {
            return Err(ManagementError::invalid());
        }
        let mut paths = BTreeSet::new();
        if prompt_files.iter().any(|file| !paths.insert(&file.path)) {
            return Err(ManagementError::invalid());
        }
        let total = agent_yaml.len()
            + prompt_files
                .iter()
                .map(|file| file.path.len() + file.content.len())
                .sum::<usize>();
        if total > policy.max_package_bytes {
            return Err(ManagementError::invalid());
        }
    }
    Ok(draft)
}

fn managed_definition_revision(agent_id: &str, author_hash: &str) -> DefinitionRevisionId {
    let identity = sha256(format!("managed-definition/v1:{agent_id}:{author_hash}").as_bytes());
    DefinitionRevisionId::new(format!("defrev_{}", identity.trim_start_matches("sha256:")))
        .expect("hash-derived Definition Revision ID is valid")
}

fn compile_draft(
    agent_id: &str,
    author_hash: &str,
    draft: &AgentDraftDocument,
) -> Result<Arc<PublishedAgent>, ManagementError> {
    let revision = managed_definition_revision(agent_id, author_hash);
    let published = match &draft.source {
        AgentDraftSource::YamlPackage {
            agent_yaml,
            prompt_files,
        } => compile_managed_agent_package(
            agent_id,
            revision,
            agent_yaml.clone(),
            prompt_files
                .iter()
                .map(|file| (file.path.clone(), file.content.clone()))
                .collect(),
        )
        .map_err(|_| {
            ManagementError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "AGENT_VALIDATION_FAILED",
                "Agent definition is invalid",
            )
        })?,
        AgentDraftSource::Graph { document } => {
            let mut document = document.clone();
            let metadata = document
                .get_mut("metadata")
                .and_then(Value::as_object_mut)
                .ok_or_else(ManagementError::invalid)?;
            metadata.insert(
                "definition_revision_id".to_owned(),
                Value::String(revision.as_str().to_owned()),
            );
            let graph = GraphAuthorDocument::decode_json(
                &serde_json::to_vec(&document).map_err(|_| ManagementError::invalid())?,
            )
            .map_err(|_| {
                ManagementError::new(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "AGENT_VALIDATION_FAILED",
                    "Agent Graph is invalid",
                )
            })?;
            PublishedAgent::from_verified_graph(agent_id, agent_id, "Managed Graph Agent", graph)
                .map_err(|_| {
                    ManagementError::new(
                        StatusCode::UNPROCESSABLE_ENTITY,
                        "AGENT_VALIDATION_FAILED",
                        "Agent Graph is invalid",
                    )
                })?
        }
    };
    Ok(Arc::new(published))
}

fn draft_from_value(value: Value) -> Result<AgentDraftDocument, ManagementError> {
    serde_json::from_value(value).map_err(|_| ManagementError::internal())
}

fn published_from_definition(
    definition: &insight_durable::AgentDefinitionRevision,
) -> Result<Arc<PublishedAgent>, ManagementError> {
    let revision = DefinitionRevisionId::new(definition.definition_revision_id.clone())
        .map_err(|_| ManagementError::internal())?;
    let published = match definition
        .author_document
        .get("authoring_mode")
        .and_then(Value::as_str)
    {
        Some("structured") => {
            let source = definition
                .author_document
                .get("source")
                .and_then(Value::as_str)
                .ok_or_else(ManagementError::internal)?
                .to_owned();
            let prompt_files = definition
                .author_document
                .get("prompt_files")
                .cloned()
                .map(serde_json::from_value)
                .transpose()
                .map_err(|_| ManagementError::internal())?
                .unwrap_or_default();
            compile_managed_agent_package(&definition.agent_id, revision, source, prompt_files)
                .map_err(|_| ManagementError::internal())?
        }
        Some("graph") => {
            let graph = GraphAuthorDocument::decode_json(
                &serde_json::to_vec(&definition.author_document)
                    .map_err(|_| ManagementError::internal())?,
            )
            .map_err(|_| ManagementError::internal())?;
            PublishedAgent::from_verified_graph(
                &definition.agent_id,
                &definition.display_name,
                &definition.public_description,
                graph,
            )
            .map_err(|_| ManagementError::internal())?
        }
        _ => return Err(ManagementError::internal()),
    };
    Ok(Arc::new(published))
}

fn candidate_evidence(
    candidate: &ManagedDeploymentCandidate,
) -> Result<(Value, String, String), ManagementError> {
    let heads = serde_json::to_value(candidate.dependency_heads())
        .map_err(|_| ManagementError::internal())?;
    let catalog_snapshot_hash = canonical_hash(&heads)?;
    let resolution_hash = canonical_hash(&json!({
        "definition_revision_id":candidate.versioned_plan().definition_revision_id(),
        "catalog_snapshot_hash":catalog_snapshot_hash,
        "resolved_bindings":candidate.resolved_bindings(),
        "worker_contracts":candidate.worker_contracts(),
        "dependency_heads":heads,
    }))?;
    Ok((heads, catalog_snapshot_hash, resolution_hash))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorPageQuery {
    cursor: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentPageQuery {
    lifecycle: Option<AgentLifecycle>,
    cursor: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateAgentRequest {
    agent_id: String,
    authoring_mode: AgentAuthoringMode,
    #[serde(default = "empty_object")]
    labels: Value,
    draft: AgentDraftDocument,
}

fn empty_object() -> Value {
    json!({})
}

async fn create_agent(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    headers: HeaderMap,
    input: Result<Json<CreateAgentRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_WRITE)?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    if !valid_agent_id(&input.agent_id) || !input.labels.is_object() {
        return Err(ManagementError::invalid());
    }
    let draft = normalize_draft(input.draft, input.authoring_mode, &state.policy)?;
    let draft_value = serde_json::to_value(&draft).map_err(|_| ManagementError::invalid())?;
    let body = json!({"agent_id":input.agent_id,"authoring_mode":input.authoring_mode,"labels":input.labels,"draft":draft});
    let receipt = state
        .repository
        .create_agent(CreateAgentCommand {
            metadata: mutation_metadata(
                &principal,
                AGENT_WRITE,
                &headers,
                "POST",
                "/v1/admin/agents".to_owned(),
                &body,
            )?,
            agent_id: input.agent_id,
            authoring_mode: input.authoring_mode,
            labels: input.labels,
            author_hash: canonical_hash(&draft_value)?,
            draft_document: draft_value,
        })
        .await
        .map_err(ManagementError::from)?;
    let id = receipt
        .response
        .get("agent_id")
        .and_then(Value::as_str)
        .ok_or_else(ManagementError::internal)?;
    let mut response = receipt_response(receipt.clone())?;
    response.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(&format!("/v1/admin/agents/{id}"))
            .map_err(|_| ManagementError::internal())?,
    );
    Ok(response)
}

fn read_response<T: Serialize>(
    value: T,
    etag: Option<String>,
) -> Result<Response, ManagementError> {
    let mut response = private_no_store(Json(ApiResponse::ok(value)));
    if let Some(etag) = etag {
        response.headers_mut().insert(
            header::ETAG,
            HeaderValue::from_str(&etag).map_err(|_| ManagementError::internal())?,
        );
    }
    Ok(response)
}

async fn list_agents(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    query: Result<Query<AgentPageQuery>, QueryRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_READ)?;
    let Query(query) = query.map_err(|_| ManagementError::invalid())?;
    let cursor = opaque_decode(query.cursor.as_deref(), &state.policy.cursor_signing_key)?;
    let mut page = state
        .repository
        .list_agents(query.lifecycle, cursor.as_deref(), page_limit(query.limit)?)
        .await
        .map_err(|_| ManagementError::internal())?;
    page.next_cursor = page
        .next_cursor
        .map(|cursor| opaque_encode(&cursor, &state.policy.cursor_signing_key));
    read_response(page, None)
}

async fn get_agent(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(agent_id): Path<String>,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_READ)?;
    let agent = state
        .repository
        .get_agent(&agent_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let etag = format!("\"agent-{}\"", agent.entity_version);
    read_response(agent, Some(etag))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateAgentRequest {
    labels: Value,
}

async fn update_agent(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<UpdateAgentRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_WRITE)?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    if !input.labels.is_object() {
        return Err(ManagementError::invalid());
    }
    let body = json!({"labels":input.labels});
    let expected_entity_version = parse_etag(&headers, "agent")?;
    let receipt = state
        .repository
        .update_agent_labels(UpdateAgentLabelsCommand {
            metadata: mutation_metadata(
                &principal,
                AGENT_WRITE,
                &headers,
                "PATCH",
                format!("/v1/admin/agents/{agent_id}"),
                &body,
            )?,
            agent_id,
            expected_entity_version,
            labels: input.labels,
        })
        .await
        .map_err(ManagementError::from)?;
    receipt_response(receipt)
}

async fn delete_agent(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_WRITE)?;
    let expected_entity_version = parse_etag(&headers, "agent")?;
    let receipt = state
        .repository
        .delete_agent(DeleteAgentCommand {
            metadata: mutation_metadata(
                &principal,
                AGENT_WRITE,
                &headers,
                "DELETE",
                format!("/v1/admin/agents/{agent_id}"),
                &json!({}),
            )?,
            agent_id,
            expected_entity_version,
        })
        .await
        .map_err(ManagementError::from)?;
    receipt_response(receipt)
}

async fn get_draft(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(agent_id): Path<String>,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_READ)?;
    let draft = state
        .repository
        .get_agent_draft(&agent_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let etag = format!("\"draft-{}\"", draft.draft_version);
    read_response(draft, Some(etag))
}

async fn replace_draft(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<AgentDraftDocument>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_WRITE)?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    let agent = state
        .repository
        .get_agent(&agent_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let draft = normalize_draft(input, agent.authoring_mode, &state.policy)?;
    let document = serde_json::to_value(&draft).map_err(|_| ManagementError::invalid())?;
    let expected_draft_version = parse_etag(&headers, "draft")?;
    let receipt = state
        .repository
        .replace_agent_draft(ReplaceAgentDraftCommand {
            metadata: mutation_metadata(
                &principal,
                AGENT_WRITE,
                &headers,
                "PUT",
                format!("/v1/admin/agents/{agent_id}/draft"),
                &document,
            )?,
            agent_id,
            expected_draft_version,
            author_hash: canonical_hash(&document)?,
            draft_document: document,
        })
        .await
        .map_err(ManagementError::from)?;
    receipt_response(receipt)
}

fn graph_from_draft_value(value: Value) -> Result<GraphAuthorDocument, ManagementError> {
    let draft = draft_from_value(value)?;
    let AgentDraftSource::Graph { document } = draft.source else {
        return Err(ManagementError::new(
            StatusCode::CONFLICT,
            "AGENT_AUTHORING_MODE_CONFLICT",
            "ViewDocument requires a Graph Agent Draft",
        ));
    };
    GraphAuthorDocument::decode_json(
        &serde_json::to_vec(&document).map_err(|_| ManagementError::internal())?,
    )
    .map_err(|_| ManagementError::internal())
}

async fn get_draft_view(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(agent_id): Path<String>,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_READ)?;
    if let Some(view) = state
        .repository
        .get_agent_draft_view(&agent_id)
        .await
        .map_err(|_| ManagementError::internal())?
    {
        return read_response(
            view.clone(),
            Some(format!("\"view-{}\"", view.view_version)),
        );
    }
    let draft = state
        .repository
        .get_agent_draft(&agent_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let graph = graph_from_draft_value(draft.document)?;
    let document = ViewDocument::new(graph.document_id().clone());
    let document: Value = serde_json::from_slice(
        &document
            .encode_json()
            .map_err(|_| ManagementError::internal())?,
    )
    .map_err(|_| ManagementError::internal())?;
    read_response(
        json!({
            "agent_id":agent_id,
            "view_version":0,
            "document":document,
            "updated_at":draft.updated_at,
        }),
        Some("\"view-0\"".to_owned()),
    )
}

async fn replace_draft_view(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<Value>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_WRITE)?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    let bytes = serde_json::to_vec(&input).map_err(|_| ManagementError::invalid())?;
    if bytes.len() > 4 * 1_024 * 1_024 {
        return Err(ManagementError::invalid());
    }
    let view = ViewDocument::decode_json(&bytes).map_err(|_| ManagementError::invalid())?;
    let draft = state
        .repository
        .get_agent_draft(&agent_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let graph = graph_from_draft_value(draft.document)?;
    if graph.document_id() != view.graph_document_id() {
        return Err(ManagementError::new(
            StatusCode::CONFLICT,
            "AGENT_VIEW_DOCUMENT_CONFLICT",
            "ViewDocument does not belong to the current Graph Draft",
        ));
    }
    let expected_view_version = parse_etag(&headers, "view")?;
    let receipt = state
        .repository
        .replace_agent_draft_view(ReplaceAgentDraftViewCommand {
            metadata: mutation_metadata(
                &principal,
                AGENT_WRITE,
                &headers,
                "PUT",
                format!("/v1/admin/agents/{agent_id}/draft/view"),
                &input,
            )?,
            agent_id,
            expected_view_version,
            document: input,
        })
        .await
        .map_err(ManagementError::from)?;
    receipt_response(receipt)
}

async fn apply_semantic_edits(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<Value>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_WRITE)?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    let expected_draft_version = parse_etag(&headers, "draft")?;
    let stored = state
        .repository
        .get_agent_draft(&agent_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    if stored.draft_version != expected_draft_version {
        return Err(ManagementError::new(
            StatusCode::PRECONDITION_FAILED,
            "AGENT_MANAGEMENT_PRECONDITION_FAILED",
            "Agent management precondition failed",
        ));
    }
    let draft = draft_from_value(stored.document)?;
    let AgentDraftSource::Graph { document } = draft.source else {
        return Err(ManagementError::new(
            StatusCode::CONFLICT,
            "AGENT_AUTHORING_MODE_CONFLICT",
            "semantic edits require a Graph Agent Draft",
        ));
    };
    let mut graph = GraphAuthorDocument::decode_json(
        &serde_json::to_vec(&document).map_err(|_| ManagementError::invalid())?,
    )
    .map_err(|_| ManagementError::invalid())?;
    let batch = GraphSemanticEditBatch::decode_json(
        &serde_json::to_vec(&input).map_err(|_| ManagementError::invalid())?,
    )
    .map_err(|_| ManagementError::invalid())?;
    graph.apply_semantic_edit_batch(batch).map_err(|_| {
        ManagementError::new(
            StatusCode::CONFLICT,
            "AGENT_GRAPH_EDIT_CONFLICT",
            "Graph semantic edit could not be applied",
        )
    })?;
    let document: Value = serde_json::from_slice(
        &graph
            .encode_json()
            .map_err(|_| ManagementError::invalid())?,
    )
    .map_err(|_| ManagementError::invalid())?;
    let replacement = AgentDraftDocument {
        source: AgentDraftSource::Graph { document },
    };
    let replacement = serde_json::to_value(replacement).map_err(|_| ManagementError::invalid())?;
    let receipt = state
        .repository
        .replace_agent_draft(ReplaceAgentDraftCommand {
            metadata: mutation_metadata(
                &principal,
                AGENT_WRITE,
                &headers,
                "POST",
                format!("/v1/admin/agents/{agent_id}/draft/semantic-edits"),
                &input,
            )?,
            agent_id,
            expected_draft_version,
            author_hash: canonical_hash(&replacement)?,
            draft_document: replacement,
        })
        .await
        .map_err(ManagementError::from)?;
    receipt_response(receipt)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyRequest {}

async fn create_validation(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<EmptyRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_VALIDATE)?;
    let Json(_input) = input.map_err(|_| ManagementError::invalid())?;
    let expected_draft_version = parse_etag(&headers, "draft")?;
    let draft = state
        .repository
        .get_agent_draft(&agent_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    if draft.draft_version != expected_draft_version {
        return Err(ManagementError::new(
            StatusCode::PRECONDITION_FAILED,
            "AGENT_MANAGEMENT_PRECONDITION_FAILED",
            "Agent management precondition failed",
        ));
    }
    let document = draft_from_value(draft.document.clone())?;
    let compile_result = compile_draft(&agent_id, &draft.author_hash, &document);
    let (status, semantic_hash, report_document) = match compile_result {
        Ok(published) => (
            AgentOperationStatus::Succeeded,
            Some(published.plan().semantic_hash().as_str().to_owned()),
            json!({"valid":true,"diagnostics":[]}),
        ),
        Err(_) => (
            AgentOperationStatus::Failed,
            None,
            json!({
                "valid":false,
                "diagnostics":[{"code":"AGENT_DEFINITION_INVALID","severity":"error"}]
            }),
        ),
    };
    let now = Utc::now();
    let report = AgentValidationReport {
        validation_id: format!("agentval_{}", Uuid::new_v4().simple()),
        agent_id: agent_id.clone(),
        draft_version: draft.draft_version,
        author_hash: draft.author_hash.clone(),
        policy_digest: state.policy.validation_policy_digest.clone(),
        status,
        semantic_hash,
        report_hash: canonical_hash(&report_document)?,
        document: report_document,
        created_at: now,
        created_by: principal.identity().to_owned(),
    };
    let body = json!({});
    let receipt = state
        .repository
        .create_agent_validation(CreateAgentValidationCommand {
            metadata: mutation_metadata(
                &principal,
                AGENT_VALIDATE,
                &headers,
                "POST",
                format!("/v1/admin/agents/{agent_id}/validations"),
                &body,
            )?,
            report,
            expected_draft_version,
            expected_author_hash: draft.author_hash,
        })
        .await
        .map_err(ManagementError::from)?;
    let validation_id = receipt
        .response
        .get("validation_id")
        .and_then(Value::as_str)
        .ok_or_else(ManagementError::internal)?;
    let mut response = receipt_response(receipt.clone())?;
    response.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(&format!(
            "/v1/admin/agents/{agent_id}/validations/{validation_id}"
        ))
        .map_err(|_| ManagementError::internal())?,
    );
    Ok(response)
}

async fn get_validation(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path((agent_id, validation_id)): Path<(String, String)>,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_READ)?;
    let report = state
        .repository
        .get_agent_validation(&agent_id, &validation_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    read_response(report, None)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishRevisionRequest {
    draft_version: u64,
    validation_id: String,
}

async fn publish_revision(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<PublishRevisionRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_PUBLISH)?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    if input.validation_id.is_empty() || input.validation_id.len() > 128 {
        return Err(ManagementError::invalid());
    }
    let expected_draft_version = parse_etag(&headers, "draft")?;
    if input.draft_version != expected_draft_version {
        return Err(ManagementError::new(
            StatusCode::PRECONDITION_FAILED,
            "AGENT_MANAGEMENT_PRECONDITION_FAILED",
            "Agent management precondition failed",
        ));
    }
    let draft = state
        .repository
        .get_agent_draft(&agent_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    if draft.draft_version != expected_draft_version {
        return Err(ManagementError::new(
            StatusCode::PRECONDITION_FAILED,
            "AGENT_MANAGEMENT_PRECONDITION_FAILED",
            "Agent management precondition failed",
        ));
    }
    let published = compile_draft(
        &agent_id,
        &draft.author_hash,
        &draft_from_value(draft.document)?,
    )?;
    let plan = freeze_managed_agent_definition(&published).map_err(|_| {
        ManagementError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "AGENT_DEFINITION_INVALID",
            "Agent definition could not be frozen for publication",
        )
    })?;
    let body = json!({"draft_version":input.draft_version,"validation_id":input.validation_id});
    let receipt = state
        .repository
        .publish_agent_definition(PublishAgentDefinitionCommand {
            metadata: mutation_metadata(
                &principal,
                AGENT_PUBLISH,
                &headers,
                "POST",
                format!("/v1/admin/agents/{agent_id}/revisions"),
                &body,
            )?,
            expected_draft_version,
            validation_id: input.validation_id,
            validation_policy_digest: state.policy.validation_policy_digest.clone(),
            plan,
        })
        .await
        .map_err(ManagementError::from)?;
    let definition_revision_id = receipt
        .response
        .get("definition_revision_id")
        .and_then(Value::as_str)
        .ok_or_else(ManagementError::internal)?;
    let mut response = receipt_response(receipt.clone())?;
    response.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(&format!(
            "/v1/admin/agents/{agent_id}/revisions/{definition_revision_id}"
        ))
        .map_err(|_| ManagementError::internal())?,
    );
    Ok(response)
}

async fn list_revisions(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(agent_id): Path<String>,
    query: Result<Query<CursorPageQuery>, QueryRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_READ)?;
    let Query(query) = query.map_err(|_| ManagementError::invalid())?;
    let cursor = opaque_decode(query.cursor.as_deref(), &state.policy.cursor_signing_key)?;
    let mut page = state
        .repository
        .list_agent_definitions(&agent_id, cursor.as_deref(), page_limit(query.limit)?)
        .await
        .map_err(|_| ManagementError::internal())?;
    page.next_cursor = page
        .next_cursor
        .map(|cursor| opaque_encode(&cursor, &state.policy.cursor_signing_key));
    read_response(page, None)
}

async fn get_revision(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path((agent_id, definition_revision_id)): Path<(String, String)>,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_READ)?;
    let definition = state
        .repository
        .get_agent_definition(&agent_id, &definition_revision_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    read_response(definition, None)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateResolutionRequest {
    definition_revision_id: String,
}

async fn create_resolution(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<CreateResolutionRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_DEPLOY)?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    let definition = state
        .repository
        .get_agent_definition(&agent_id, &input.definition_revision_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let candidate = state
        .run_service
        .resolve_managed_deployment(published_from_definition(&definition)?)
        .await
        .map_err(|_| {
            ManagementError::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "AGENT_DEPLOYMENT_RESOLUTION_FAILED",
                "Agent dependencies could not be resolved",
            )
        })?;
    let (dependency_heads, catalog_snapshot_hash, resolution_hash) =
        candidate_evidence(&candidate)?;
    let now = Utc::now();
    let resolution = insight_durable::AgentDeploymentResolution {
        resolution_id: format!("agentres_{}", Uuid::new_v4().simple()),
        agent_id: agent_id.clone(),
        definition_revision_id: input.definition_revision_id,
        status: AgentOperationStatus::Succeeded,
        catalog_snapshot_hash,
        resolution_hash,
        resolved_bindings: candidate.resolved_bindings().clone(),
        worker_contracts: candidate.worker_contracts().clone(),
        dependency_heads,
        risks: serde_json::to_value(candidate.risks()).map_err(|_| ManagementError::internal())?,
        expires_at: now + state.policy.resolution_ttl,
        created_at: now,
        created_by: principal.identity().to_owned(),
    };
    let body = json!({"definition_revision_id":resolution.definition_revision_id});
    let receipt = state
        .repository
        .create_agent_deployment_resolution(CreateAgentResolutionCommand {
            metadata: mutation_metadata(
                &principal,
                AGENT_DEPLOY,
                &headers,
                "POST",
                format!("/v1/admin/agents/{agent_id}/deployment-resolutions"),
                &body,
            )?,
            resolution,
        })
        .await
        .map_err(ManagementError::from)?;
    let resolution_id = receipt
        .response
        .get("resolution_id")
        .and_then(Value::as_str)
        .ok_or_else(ManagementError::internal)?;
    let mut response = receipt_response(receipt.clone())?;
    response.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(&format!(
            "/v1/admin/agents/{agent_id}/deployment-resolutions/{resolution_id}"
        ))
        .map_err(|_| ManagementError::internal())?,
    );
    Ok(response)
}

async fn get_resolution(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path((agent_id, resolution_id)): Path<(String, String)>,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_READ)?;
    let resolution = state
        .repository
        .get_agent_deployment_resolution(&agent_id, &resolution_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    read_response(resolution, None)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateDeploymentRequest {
    definition_revision_id: String,
    resolution_id: String,
}

async fn create_deployment(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<CreateDeploymentRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_DEPLOY)?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    let resolution = state
        .repository
        .get_agent_deployment_resolution(&agent_id, &input.resolution_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    if resolution.definition_revision_id != input.definition_revision_id {
        return Err(ManagementError::new(
            StatusCode::CONFLICT,
            "AGENT_DEPLOYMENT_INVALID",
            "Deployment Resolution does not match the requested Definition",
        ));
    }
    if resolution.expires_at <= Utc::now() {
        return Err(ManagementError::new(
            StatusCode::CONFLICT,
            "AGENT_RESOLUTION_EXPIRED",
            "Agent Deployment Resolution expired",
        ));
    }
    let definition = state
        .repository
        .get_agent_definition(&agent_id, &resolution.definition_revision_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let candidate = state
        .run_service
        .resolve_managed_deployment(published_from_definition(&definition)?)
        .await
        .map_err(|_| {
            ManagementError::new(
                StatusCode::CONFLICT,
                "AGENT_DEPENDENCY_HEAD_CHANGED",
                "an exact dependency head changed",
            )
        })?;
    let (dependency_heads, _, resolution_hash) = candidate_evidence(&candidate)?;
    if resolution.resolution_hash != resolution_hash
        || resolution.dependency_heads != dependency_heads
    {
        return Err(ManagementError::new(
            StatusCode::CONFLICT,
            "AGENT_DEPENDENCY_HEAD_CHANGED",
            "an exact dependency head changed",
        ));
    }
    let plan = candidate.versioned_plan().clone();
    let body = json!({
        "definition_revision_id":input.definition_revision_id,
        "resolution_id":input.resolution_id
    });
    let receipt = state
        .repository
        .install_agent_deployment(InstallAgentDeploymentCommand {
            metadata: mutation_metadata(
                &principal,
                AGENT_DEPLOY,
                &headers,
                "POST",
                format!("/v1/admin/agents/{agent_id}/deployments"),
                &body,
            )?,
            resolution_id: input.resolution_id,
            expected_resolution_hash: resolution.resolution_hash,
            expected_dependency_heads: dependency_heads,
            plan,
        })
        .await
        .map_err(ManagementError::from)?;
    state
        .run_service
        .install_managed_deployment_archive(candidate)
        .map_err(|_| ManagementError::internal())?;
    let deployment_revision_id = receipt
        .response
        .get("deployment_revision_id")
        .and_then(Value::as_str)
        .ok_or_else(ManagementError::internal)?;
    let mut response = receipt_response(receipt.clone())?;
    response.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(&format!(
            "/v1/admin/agents/{agent_id}/deployments/{deployment_revision_id}"
        ))
        .map_err(|_| ManagementError::internal())?,
    );
    Ok(response)
}

async fn list_deployments(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(agent_id): Path<String>,
    query: Result<Query<CursorPageQuery>, QueryRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_READ)?;
    let Query(query) = query.map_err(|_| ManagementError::invalid())?;
    let cursor = opaque_decode(query.cursor.as_deref(), &state.policy.cursor_signing_key)?;
    let mut page = state
        .repository
        .list_agent_deployments(&agent_id, cursor.as_deref(), page_limit(query.limit)?)
        .await
        .map_err(|_| ManagementError::internal())?;
    page.next_cursor = page
        .next_cursor
        .map(|cursor| opaque_encode(&cursor, &state.policy.cursor_signing_key));
    read_response(page, None)
}

async fn get_deployment(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path((agent_id, deployment_revision_id)): Path<(String, String)>,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_READ)?;
    let deployment = state
        .repository
        .get_agent_deployment(&agent_id, &deployment_revision_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    read_response(deployment, None)
}

async fn get_active_deployment(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(agent_id): Path<String>,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_READ)?;
    let agent = state
        .repository
        .get_agent(&agent_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let value = json!({
        "agent_id":agent.agent_id,
        "definition_revision_id":agent.active_definition_revision_id,
        "deployment_revision_id":agent.active_deployment_revision_id,
        "entity_version":agent.entity_version,
    });
    read_response(value, Some(format!("\"agent-{}\"", agent.entity_version)))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivateDeploymentRequest {
    deployment_revision_id: String,
}

async fn activate_deployment(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<ActivateDeploymentRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_ACTIVATE)?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    let expected_entity_version = parse_etag(&headers, "agent")?;
    let body = json!({"deployment_revision_id":input.deployment_revision_id});
    let receipt = state
        .repository
        .activate_managed_agent_deployment(ActivateManagedAgentDeploymentCommand {
            metadata: mutation_metadata(
                &principal,
                AGENT_ACTIVATE,
                &headers,
                "PUT",
                format!("/v1/admin/agents/{agent_id}/active-deployment"),
                &body,
            )?,
            agent_id: agent_id.clone(),
            expected_entity_version,
            deployment_revision_id: input.deployment_revision_id,
        })
        .await
        .map_err(ManagementError::from)?;
    state
        .run_service
        .refresh_managed_agent_route(&agent_id)
        .await
        .map_err(|_| ManagementError::internal())?;
    receipt_response(receipt)
}

async fn deactivate_agent(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_ACTIVATE)?;
    let expected_entity_version = parse_etag(&headers, "agent")?;
    let receipt = state
        .repository
        .deactivate_managed_agent(DeactivateManagedAgentCommand {
            metadata: mutation_metadata(
                &principal,
                AGENT_ACTIVATE,
                &headers,
                "DELETE",
                format!("/v1/admin/agents/{agent_id}/active-deployment"),
                &json!({}),
            )?,
            agent_id: agent_id.clone(),
            expected_entity_version,
        })
        .await
        .map_err(ManagementError::from)?;
    state
        .run_service
        .refresh_managed_agent_route(&agent_id)
        .await
        .map_err(|_| ManagementError::internal())?;
    receipt_response(receipt)
}

async fn archive_agent(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<EmptyRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_ARCHIVE)?;
    let Json(_input) = input.map_err(|_| ManagementError::invalid())?;
    let expected_entity_version = parse_etag(&headers, "agent")?;
    let receipt = state
        .repository
        .archive_agent(ArchiveAgentCommand {
            metadata: mutation_metadata(
                &principal,
                AGENT_ARCHIVE,
                &headers,
                "POST",
                format!("/v1/admin/agents/{agent_id}/archive"),
                &json!({}),
            )?,
            agent_id: agent_id.clone(),
            expected_entity_version,
        })
        .await
        .map_err(ManagementError::from)?;
    state
        .run_service
        .refresh_managed_agent_route(&agent_id)
        .await
        .map_err(|_| ManagementError::internal())?;
    receipt_response(receipt)
}

async fn restore_agent(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<EmptyRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, AGENT_ARCHIVE)?;
    let Json(_input) = input.map_err(|_| ManagementError::invalid())?;
    let expected_entity_version = parse_etag(&headers, "agent")?;
    let receipt = state
        .repository
        .restore_agent(RestoreAgentCommand {
            metadata: mutation_metadata(
                &principal,
                AGENT_ARCHIVE,
                &headers,
                "POST",
                format!("/v1/admin/agents/{agent_id}/restore"),
                &json!({}),
            )?,
            agent_id: agent_id.clone(),
            expected_entity_version,
        })
        .await
        .map_err(ManagementError::from)?;
    state
        .run_service
        .refresh_managed_agent_route(&agent_id)
        .await
        .map_err(|_| ManagementError::internal())?;
    receipt_response(receipt)
}

fn require_debug_read(principal: &OperatorPrincipal) -> Result<(), ManagementError> {
    if principal.has_capability(AGENT_READ)
        || principal.has_capability(AGENT_DEBUG_SANDBOX)
        || principal.has_capability(AGENT_DEBUG_LIVE)
    {
        Ok(())
    } else {
        Err(ManagementError::forbidden())
    }
}

fn debug_summary(session: &AgentDebugSession) -> Value {
    json!({
        "debug_session_id":session.debug_session_id,
        "agent_id":session.agent_id,
        "source_hash":session.source_hash,
        "execution_profile_id":session.execution_profile_id,
        "profile_mode":session.profile_mode,
        "status":session.status,
        "definition_revision_id":session.definition_revision_id,
        "deployment_revision_id":session.deployment_revision_id,
        "run_id":session.run_id,
        "failure_code":session.failure_code,
        "expires_at":session.expires_at,
        "created_at":session.created_at,
        "finished_at":session.finished_at,
        "created_by":session.created_by
    })
}

fn validate_debug_source(source: &Value) -> Result<(), ManagementError> {
    let object = source.as_object().ok_or_else(ManagementError::invalid)?;
    let source_type = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(ManagementError::invalid)?;
    let valid = match source_type {
        "draft" => {
            object.len() == 2
                && object
                    .get("draft_version")
                    .and_then(Value::as_u64)
                    .is_some()
        }
        "definition" => {
            object.len() == 2
                && object
                    .get("definition_revision_id")
                    .and_then(Value::as_str)
                    .is_some()
        }
        "deployment" => {
            object.len() == 2
                && object
                    .get("deployment_revision_id")
                    .and_then(Value::as_str)
                    .is_some()
        }
        _ => false,
    };
    valid.then_some(()).ok_or_else(ManagementError::invalid)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateDebugSessionRequest {
    source: Value,
    execution_profile_id: String,
    input: Value,
    #[serde(default)]
    live_confirmation: bool,
    #[serde(default)]
    risk_preview_hash: Option<String>,
}

async fn create_debug_session(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(agent_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<CreateDebugSessionRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    validate_debug_source(&input.source)?;
    if input
        .risk_preview_hash
        .as_deref()
        .is_some_and(|hash| !valid_sha256(hash))
    {
        return Err(ManagementError::invalid());
    }
    let profile = state
        .policy
        .debug_profiles
        .get(&input.execution_profile_id)
        .copied()
        .ok_or_else(ManagementError::invalid)?;
    let debug_capability = match profile.mode {
        AgentDebugProfileMode::Sandbox => {
            require(&principal, AGENT_DEBUG_SANDBOX)?;
            AGENT_DEBUG_SANDBOX
        }
        AgentDebugProfileMode::Live => {
            require(&principal, AGENT_DEBUG_LIVE)?;
            AGENT_DEBUG_LIVE
        }
    };
    let request_body = json!({
        "source":input.source,
        "execution_profile_id":input.execution_profile_id,
        "input":input.input,
        "live_confirmation":input.live_confirmation,
        "risk_preview_hash":input.risk_preview_hash
    });
    let metadata = mutation_metadata(
        &principal,
        debug_capability,
        &headers,
        "POST",
        format!("/v1/admin/agents/{agent_id}/debug-sessions"),
        &request_body,
    )?;
    if let Some(receipt) = state
        .repository
        .replay_agent_mutation(&metadata)
        .await
        .map_err(ManagementError::from)?
    {
        return debug_created_response(&agent_id, receipt);
    }
    let mut source = pin_debug_source(&state, &agent_id, input.source, input.input).await?;
    let now = Utc::now();
    let debug_session_id = format!("agentdebug_{}", Uuid::new_v4().simple());
    if profile.mode == AgentDebugProfileMode::Live {
        let provisional = AgentDebugSession {
            debug_session_id: debug_session_id.clone(),
            agent_id: agent_id.clone(),
            source_hash: canonical_hash(&source)?,
            source: source.clone(),
            execution_profile_id: input.execution_profile_id.clone(),
            profile_mode: profile.mode.as_str().to_owned(),
            status: AgentDebugStatus::Queued,
            definition_revision_id: None,
            deployment_revision_id: None,
            run_id: None,
            failure_code: None,
            expires_at: now + profile.session_timeout,
            created_at: now,
            finished_at: None,
            created_by: principal.identity().to_owned(),
        };
        let prepared = prepare_debug_deployment(&state, &provisional).await?;
        let risk_preview = live_debug_risk_preview(&profile, &source, &prepared)?;
        let expected_hash = risk_preview
            .get("risk_preview_hash")
            .and_then(Value::as_str)
            .ok_or_else(ManagementError::internal)?;
        if !input.live_confirmation || input.risk_preview_hash.as_deref() != Some(expected_hash) {
            return Err(ManagementError::new(
                StatusCode::PRECONDITION_REQUIRED,
                "AGENT_DEBUG_LIVE_CONFIRMATION_REQUIRED",
                "live Debug requires confirmation of the exact reviewed risk preview",
            )
            .with_data(json!({"risk_preview":risk_preview})));
        }
        source
            .as_object_mut()
            .ok_or_else(ManagementError::internal)?
            .insert("reviewed_risk_preview".to_owned(), risk_preview);
    }
    let session = AgentDebugSession {
        debug_session_id,
        agent_id: agent_id.clone(),
        source_hash: canonical_hash(&source)?,
        source,
        execution_profile_id: input.execution_profile_id,
        profile_mode: profile.mode.as_str().to_owned(),
        status: AgentDebugStatus::Queued,
        definition_revision_id: None,
        deployment_revision_id: None,
        run_id: None,
        failure_code: None,
        expires_at: now + profile.session_timeout,
        created_at: now,
        finished_at: None,
        created_by: principal.identity().to_owned(),
    };
    let receipt = state
        .repository
        .create_agent_debug_session(CreateAgentDebugSessionCommand {
            metadata,
            session: session.clone(),
            max_active_sessions: profile.max_concurrent_sessions,
            retain_until: now + profile.content_retention,
        })
        .await
        .map_err(ManagementError::from)?;
    if !receipt.replayed {
        let worker_state = Arc::clone(&state);
        tokio::spawn(async move {
            drive_debug_session(worker_state, session).await;
        });
    }
    debug_created_response(&agent_id, receipt)
}

fn debug_created_response(
    agent_id: &str,
    receipt: AgentMutationReceipt,
) -> Result<Response, ManagementError> {
    let debug_session_id = receipt
        .response
        .get("debug_session_id")
        .and_then(Value::as_str)
        .ok_or_else(ManagementError::internal)?;
    let mut response = receipt_response(receipt.clone())?;
    response.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(&format!(
            "/v1/admin/agents/{agent_id}/debug-sessions/{debug_session_id}"
        ))
        .map_err(|_| ManagementError::internal())?,
    );
    Ok(response)
}

async fn pin_debug_source(
    state: &AgentManagementApiState,
    agent_id: &str,
    selection: Value,
    input: Value,
) -> Result<Value, ManagementError> {
    let source_type = selection
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(ManagementError::invalid)?;
    match source_type {
        "draft" => {
            let expected_version = selection
                .get("draft_version")
                .and_then(Value::as_u64)
                .ok_or_else(ManagementError::invalid)?;
            let draft = state
                .repository
                .get_agent_draft(agent_id)
                .await
                .map_err(|_| ManagementError::internal())?
                .ok_or_else(ManagementError::not_found)?;
            if draft.draft_version != expected_version {
                return Err(ManagementError::new(
                    StatusCode::CONFLICT,
                    "AGENT_DEBUG_SOURCE_STALE",
                    "Debug Draft source changed",
                ));
            }
            Ok(json!({
                "selection":selection,
                "source_author_hash":draft.author_hash,
                "source_snapshot":draft.document,
                "input":input
            }))
        }
        "definition" => {
            let revision_id = selection
                .get("definition_revision_id")
                .and_then(Value::as_str)
                .ok_or_else(ManagementError::invalid)?;
            state
                .repository
                .get_agent_definition(agent_id, revision_id)
                .await
                .map_err(|_| ManagementError::internal())?
                .ok_or_else(ManagementError::not_found)?;
            Ok(json!({"selection":selection,"input":input}))
        }
        "deployment" => {
            let revision_id = selection
                .get("deployment_revision_id")
                .and_then(Value::as_str)
                .ok_or_else(ManagementError::invalid)?;
            state
                .repository
                .get_agent_deployment(agent_id, revision_id)
                .await
                .map_err(|_| ManagementError::internal())?
                .ok_or_else(ManagementError::not_found)?;
            Ok(json!({"selection":selection,"input":input}))
        }
        _ => Err(ManagementError::invalid()),
    }
}

fn has_external_debug_binding(value: &Value) -> bool {
    match value {
        Value::Array(values) => values.iter().any(has_external_debug_binding),
        Value::Object(values) => {
            values.keys().any(|key| {
                matches!(
                    key.as_str(),
                    "provider_id"
                        | "provider_revision_id"
                        | "action_id"
                        | "server_id"
                        | "retrieval_id"
                        | "subflow_agent_id"
                )
            }) || values.values().any(has_external_debug_binding)
        }
        _ => false,
    }
}

fn has_provider_debug_binding(value: &Value) -> bool {
    match value {
        Value::Array(values) => values.iter().any(has_provider_debug_binding),
        Value::Object(values) => {
            values.contains_key("provider_revision_id")
                || values.values().any(has_provider_debug_binding)
        }
        _ => false,
    }
}

fn has_non_provider_external_debug_binding(value: &Value) -> bool {
    match value {
        Value::Array(values) => values.iter().any(has_non_provider_external_debug_binding),
        Value::Object(values) => {
            values.keys().any(|key| {
                matches!(
                    key.as_str(),
                    "action_id" | "server_id" | "retrieval_id" | "subflow_agent_id"
                )
            }) || values.values().any(has_non_provider_external_debug_binding)
        }
        _ => false,
    }
}

fn has_mcp_debug_binding(value: &Value) -> bool {
    match value {
        Value::Array(values) => values.iter().any(has_mcp_debug_binding),
        Value::Object(values) => {
            values.iter().any(|(key, value)| {
                matches!(key.as_str(), "action_id" | "implementation")
                    && value
                        .as_str()
                        .is_some_and(|identity| identity.starts_with("mcp."))
            }) || values.values().any(has_mcp_debug_binding)
        }
        _ => false,
    }
}

fn published_requires_debug_interaction_authority(published: &PublishedAgent) -> bool {
    published.plan().nodes().iter().any(|node| {
        matches!(
            node.kind(),
            NodeKind::HumanTask(_) | NodeKind::WaitSignal(_)
        )
    })
}

struct PreparedDebugDeployment {
    plan: Option<VersionedPlan>,
    definition_revision_id: String,
    deployment_revision_id: String,
    binding_hash: String,
    bindings: Value,
    input: Value,
}

fn live_debug_risk_preview(
    profile: &AgentDebugProfilePolicy,
    source: &Value,
    prepared: &PreparedDebugDeployment,
) -> Result<Value, ManagementError> {
    let provider_calls_possible = has_provider_debug_binding(&prepared.bindings);
    let mcp_calls_possible = has_mcp_debug_binding(&prepared.bindings);
    let external_actions_possible = has_non_provider_external_debug_binding(&prepared.bindings);
    let mut preview = json!({
        "profile_mode":"live",
        "source_hash":canonical_hash(source)?,
        "definition_revision_id":prepared.definition_revision_id,
        "deployment_revision_id":prepared.deployment_revision_id,
        "binding_hash":prepared.binding_hash,
        "maximum_duration_seconds":profile.session_timeout.num_seconds().max(0),
        "cost_risk":{
            "provider_calls_possible":provider_calls_possible,
            "billing_possible":provider_calls_possible
        },
        "side_effect_risk":{
            "external_actions_possible":external_actions_possible,
            "mcp_calls_possible":mcp_calls_possible
        },
        "enforced_guards":{
            "provider_suspension":true,
            "action_approval":true,
            "mcp_interaction":true
        }
    });
    let preview_hash = canonical_hash(&preview)?;
    preview
        .as_object_mut()
        .ok_or_else(ManagementError::internal)?
        .insert("risk_preview_hash".to_owned(), Value::String(preview_hash));
    Ok(preview)
}

async fn prepare_debug_deployment(
    state: &AgentManagementApiState,
    session: &AgentDebugSession,
) -> Result<PreparedDebugDeployment, ManagementError> {
    let selection = session
        .source
        .get("selection")
        .ok_or_else(ManagementError::internal)?;
    let input = session
        .source
        .get("input")
        .cloned()
        .ok_or_else(ManagementError::internal)?;
    let source_type = selection
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(ManagementError::internal)?;
    let (
        plan,
        definition_revision_id,
        deployment_revision_id,
        binding_hash,
        bindings,
        needs_interaction,
    ) = match source_type {
        "draft" => {
            let author_hash = session
                .source
                .get("source_author_hash")
                .and_then(Value::as_str)
                .ok_or_else(ManagementError::internal)?;
            let snapshot = session
                .source
                .get("source_snapshot")
                .cloned()
                .ok_or_else(ManagementError::internal)?;
            let published =
                compile_draft(&session.agent_id, author_hash, &draft_from_value(snapshot)?)?;
            let candidate = state
                .run_service
                .resolve_managed_deployment(published)
                .await
                .map_err(|_| {
                    ManagementError::new(
                        StatusCode::UNPROCESSABLE_ENTITY,
                        "AGENT_DEBUG_DEPENDENCY_RESOLUTION_FAILED",
                        "Debug dependencies could not be resolved",
                    )
                })?;
            (
                Some(candidate.versioned_plan().clone()),
                candidate
                    .versioned_plan()
                    .definition_revision_id()
                    .as_str()
                    .to_owned(),
                candidate
                    .versioned_plan()
                    .deployment_revision_id()
                    .as_str()
                    .to_owned(),
                candidate
                    .versioned_plan()
                    .binding_hash()
                    .as_str()
                    .to_owned(),
                candidate.resolved_bindings().clone(),
                candidate.requires_interaction_authority(),
            )
        }
        "definition" => {
            let revision_id = selection
                .get("definition_revision_id")
                .and_then(Value::as_str)
                .ok_or_else(ManagementError::internal)?;
            let definition = state
                .repository
                .get_agent_definition(&session.agent_id, revision_id)
                .await
                .map_err(|_| ManagementError::internal())?
                .ok_or_else(ManagementError::not_found)?;
            let candidate = state
                .run_service
                .resolve_managed_deployment(published_from_definition(&definition)?)
                .await
                .map_err(|_| {
                    ManagementError::new(
                        StatusCode::UNPROCESSABLE_ENTITY,
                        "AGENT_DEBUG_DEPENDENCY_RESOLUTION_FAILED",
                        "Debug dependencies could not be resolved",
                    )
                })?;
            (
                Some(candidate.versioned_plan().clone()),
                candidate
                    .versioned_plan()
                    .definition_revision_id()
                    .as_str()
                    .to_owned(),
                candidate
                    .versioned_plan()
                    .deployment_revision_id()
                    .as_str()
                    .to_owned(),
                candidate
                    .versioned_plan()
                    .binding_hash()
                    .as_str()
                    .to_owned(),
                candidate.resolved_bindings().clone(),
                candidate.requires_interaction_authority(),
            )
        }
        "deployment" => {
            let revision_id = selection
                .get("deployment_revision_id")
                .and_then(Value::as_str)
                .ok_or_else(ManagementError::internal)?;
            let deployment = state
                .repository
                .get_agent_deployment(&session.agent_id, revision_id)
                .await
                .map_err(|_| ManagementError::internal())?
                .ok_or_else(ManagementError::not_found)?;
            let definition = state
                .repository
                .get_agent_definition(&session.agent_id, &deployment.definition_revision_id)
                .await
                .map_err(|_| ManagementError::internal())?
                .ok_or_else(ManagementError::not_found)?;
            let needs_interaction = published_requires_debug_interaction_authority(
                published_from_definition(&definition)?.as_ref(),
            );
            (
                None,
                deployment.definition_revision_id,
                deployment.deployment_revision_id,
                deployment.binding_hash,
                deployment.resolved_bindings,
                needs_interaction,
            )
        }
        _ => return Err(ManagementError::internal()),
    };
    if needs_interaction {
        return Err(ManagementError::new(
            StatusCode::FORBIDDEN,
            "AGENT_DEBUG_INTERACTION_AUTHORITY_UNAVAILABLE",
            "Debug Plan requires an administrator-only interaction authority",
        ));
    }
    let profile = state
        .policy
        .debug_profiles
        .get(&session.execution_profile_id)
        .ok_or_else(ManagementError::internal)?;
    if profile.mode == AgentDebugProfileMode::Sandbox && has_external_debug_binding(&bindings) {
        return Err(ManagementError::new(
            StatusCode::FORBIDDEN,
            "AGENT_DEBUG_SANDBOX_UNAVAILABLE",
            "sandbox profile cannot safely replace every external dependency",
        ));
    }
    if profile.mode == AgentDebugProfileMode::Live {
        if has_mcp_debug_binding(&bindings) {
            return Err(ManagementError::new(
                StatusCode::FORBIDDEN,
                "AGENT_DEBUG_MCP_PRINCIPAL_UNAVAILABLE",
                "live Debug cannot establish the configured user-scoped MCP principal",
            ));
        }
        if has_provider_debug_binding(&bindings) && !profile.allow_live_provider_credentials {
            return Err(ManagementError::new(
                StatusCode::FORBIDDEN,
                "AGENT_DEBUG_PROVIDER_CREDENTIALS_DENIED",
                "live Provider credentials are disabled for this Debug profile",
            ));
        }
        if has_non_provider_external_debug_binding(&bindings) && !profile.allow_external_actions {
            return Err(ManagementError::new(
                StatusCode::FORBIDDEN,
                "AGENT_DEBUG_EXTERNAL_ACTIONS_DENIED",
                "external actions are disabled for this Debug profile",
            ));
        }
        if let Some(reviewed) = session.source.get("reviewed_risk_preview") {
            let same_definition = reviewed
                .get("definition_revision_id")
                .and_then(Value::as_str)
                == Some(definition_revision_id.as_str());
            let same_deployment = reviewed
                .get("deployment_revision_id")
                .and_then(Value::as_str)
                == Some(deployment_revision_id.as_str());
            let same_binding =
                reviewed.get("binding_hash").and_then(Value::as_str) == Some(binding_hash.as_str());
            if !(same_definition && same_deployment && same_binding) {
                return Err(ManagementError::new(
                    StatusCode::CONFLICT,
                    "AGENT_DEBUG_LIVE_REVIEW_STALE",
                    "live Debug dependencies changed after risk review",
                ));
            }
        }
    }
    Ok(PreparedDebugDeployment {
        plan,
        definition_revision_id,
        deployment_revision_id,
        binding_hash,
        bindings,
        input,
    })
}

async fn finish_debug_session(
    state: &AgentManagementApiState,
    session: &AgentDebugSession,
    status: AgentDebugStatus,
    failure_code: Option<&str>,
) {
    let current = state
        .repository
        .get_agent_debug_session(&session.agent_id, &session.debug_session_id)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| session.clone());
    let _ = state
        .repository
        .complete_agent_debug_session(CompleteAgentDebugSessionCommand {
            debug_session_id: session.debug_session_id.clone(),
            status,
            plan: None,
            definition_revision_id: current.definition_revision_id,
            deployment_revision_id: current.deployment_revision_id,
            run_id: current.run_id,
            failure_code: failure_code.map(str::to_owned),
            now: Utc::now(),
        })
        .await;
}

async fn drive_debug_session(state: Arc<AgentManagementApiState>, session: AgentDebugSession) {
    let PreparedDebugDeployment {
        plan,
        definition_revision_id,
        deployment_revision_id,
        input,
        ..
    } = match prepare_debug_deployment(&state, &session).await {
        Ok(prepared) => prepared,
        Err(error) => {
            finish_debug_session(&state, &session, AgentDebugStatus::Failed, Some(error.code))
                .await;
            return;
        }
    };
    let debug_run_id = format!(
        "debugrun_{}",
        session
            .debug_session_id
            .strip_prefix("agentdebug_")
            .unwrap_or(&session.debug_session_id)
    );
    if state
        .repository
        .complete_agent_debug_session(CompleteAgentDebugSessionCommand {
            debug_session_id: session.debug_session_id.clone(),
            status: AgentDebugStatus::Running,
            plan,
            definition_revision_id: Some(definition_revision_id.clone()),
            deployment_revision_id: Some(deployment_revision_id.clone()),
            run_id: None,
            failure_code: None,
            now: Utc::now(),
        })
        .await
        .is_err()
    {
        return;
    }
    if state
        .run_service
        .create_admin_debug_detached_from_deployment(
            &session.agent_id,
            &deployment_revision_id,
            &debug_run_id,
            input,
            insight_runtime::run_service::RequestMetadata {
                request_id: Some(format!("debug-{}", session.debug_session_id)),
            },
        )
        .await
        .is_err()
    {
        finish_debug_session(
            &state,
            &session,
            AgentDebugStatus::Failed,
            Some("AGENT_DEBUG_RUN_ADMISSION_FAILED"),
        )
        .await;
        return;
    }
    if state
        .repository
        .complete_agent_debug_session(CompleteAgentDebugSessionCommand {
            debug_session_id: session.debug_session_id.clone(),
            status: AgentDebugStatus::Running,
            plan: None,
            definition_revision_id: Some(definition_revision_id),
            deployment_revision_id: Some(deployment_revision_id),
            run_id: Some(debug_run_id.clone()),
            failure_code: None,
            now: Utc::now(),
        })
        .await
        .is_err()
    {
        let _ = state
            .run_service
            .cancel_admin_debug_run(&debug_run_id)
            .await;
        return;
    }
    loop {
        let current = match state
            .repository
            .get_agent_debug_session(&session.agent_id, &session.debug_session_id)
            .await
        {
            Ok(Some(current)) => current,
            _ => return,
        };
        if matches!(
            current.status,
            AgentDebugStatus::Cancelled | AgentDebugStatus::Expired
        ) {
            let _ = state
                .run_service
                .cancel_admin_debug_run(&debug_run_id)
                .await;
            return;
        }
        if Utc::now() >= current.expires_at {
            let _ = state
                .run_service
                .cancel_admin_debug_run(&debug_run_id)
                .await;
            finish_debug_session(&state, &session, AgentDebugStatus::Expired, None).await;
            return;
        }
        match state.run_service.get_admin_debug_run(&debug_run_id).await {
            Ok(record) => match record.status() {
                RunStatus::Completed => {
                    finish_debug_session(&state, &session, AgentDebugStatus::Succeeded, None).await;
                    return;
                }
                RunStatus::Failed | RunStatus::Interrupted => {
                    finish_debug_session(
                        &state,
                        &session,
                        AgentDebugStatus::Failed,
                        Some("AGENT_DEBUG_RUN_FAILED"),
                    )
                    .await;
                    return;
                }
                RunStatus::Cancelled => {
                    finish_debug_session(&state, &session, AgentDebugStatus::Cancelled, None).await;
                    return;
                }
                RunStatus::Created | RunStatus::Running => {}
            },
            Err(_) => {
                finish_debug_session(
                    &state,
                    &session,
                    AgentDebugStatus::Failed,
                    Some("AGENT_DEBUG_RUN_UNAVAILABLE"),
                )
                .await;
                return;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn get_debug_session(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path((agent_id, debug_session_id)): Path<(String, String)>,
) -> Result<Response, ManagementError> {
    require_debug_read(&principal)?;
    let session = state
        .repository
        .get_agent_debug_session(&agent_id, &debug_session_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let can_read_content = principal.has_capability(if session.profile_mode == "live" {
        AGENT_DEBUG_LIVE
    } else {
        AGENT_DEBUG_SANDBOX
    });
    if can_read_content {
        read_response(session, None)
    } else {
        read_response(debug_summary(&session), None)
    }
}

async fn stream_debug_session(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path((agent_id, debug_session_id)): Path<(String, String)>,
) -> Result<Response, ManagementError> {
    let session = state
        .repository
        .get_agent_debug_session(&agent_id, &debug_session_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let debug_capability = if session.profile_mode == "live" {
        AGENT_DEBUG_LIVE
    } else {
        AGENT_DEBUG_SANDBOX
    };
    require(&principal, debug_capability)?;
    if session
        .source
        .get("content_deleted")
        .and_then(Value::as_bool)
        == Some(true)
    {
        return Err(ManagementError::new(
            StatusCode::GONE,
            "AGENT_DEBUG_CONTENT_DELETED",
            "Debug Session content retention has elapsed",
        ));
    }
    let run_id = session.run_id.ok_or_else(|| {
        ManagementError::new(
            StatusCode::CONFLICT,
            "AGENT_DEBUG_STREAM_NOT_READY",
            "Debug stream is not ready",
        )
    })?;
    let attached = state
        .run_service
        .open_admin_debug_stream(&run_id)
        .await
        .map_err(|_| ManagementError::not_found())?;
    Ok(match attached {
        AnyAttachedRun::Full(attached) => {
            run_stream(attached, state.debug_stream_keep_alive_interval).into_response()
        }
        AnyAttachedRun::TerminalOnly(attached) => {
            terminal_run_stream(attached, state.debug_stream_keep_alive_interval).into_response()
        }
    })
}

async fn list_debug_sessions(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(agent_id): Path<String>,
    query: Result<Query<CursorPageQuery>, QueryRejection>,
) -> Result<Response, ManagementError> {
    require_debug_read(&principal)?;
    let Query(query) = query.map_err(|_| ManagementError::invalid())?;
    let cursor = opaque_decode(query.cursor.as_deref(), &state.policy.cursor_signing_key)?;
    let mut page = state
        .repository
        .list_agent_debug_sessions(&agent_id, cursor.as_deref(), page_limit(query.limit)?)
        .await
        .map_err(|_| ManagementError::internal())?;
    page.next_cursor = page
        .next_cursor
        .map(|cursor| opaque_encode(&cursor, &state.policy.cursor_signing_key));
    read_response(
        json!({
            "items":page.items.iter().map(debug_summary).collect::<Vec<_>>(),
            "next_cursor":page.next_cursor
        }),
        None,
    )
}

async fn cancel_debug_session(
    State(state): State<Arc<AgentManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path((agent_id, debug_session_id)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, ManagementError> {
    let session = state
        .repository
        .get_agent_debug_session(&agent_id, &debug_session_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let debug_capability = if session.profile_mode == "live" {
        AGENT_DEBUG_LIVE
    } else {
        AGENT_DEBUG_SANDBOX
    };
    require(&principal, debug_capability)?;
    if let Some(run_id) = session.run_id.as_deref() {
        let _ = state.run_service.cancel_admin_debug_run(run_id).await;
    }
    let receipt = state
        .repository
        .cancel_agent_debug_session(CancelAgentDebugSessionCommand {
            metadata: mutation_metadata(
                &principal,
                debug_capability,
                &headers,
                "DELETE",
                format!("/v1/admin/agents/{agent_id}/debug-sessions/{debug_session_id}"),
                &json!({}),
            )?,
            agent_id,
            debug_session_id,
        })
        .await
        .map_err(ManagementError::from)?;
    receipt_response(receipt)
}

#[cfg(test)]
mod contract_tests {
    use super::*;

    const MANAGEMENT_SCHEMA: &str = workspace_asset_str!("schemas/agent-management-v1.json");
    const MANAGEMENT_SAMPLES: &str =
        workspace_asset_str!("schemas/agent-management-v1.samples.json");
    const MANAGEMENT_OPENAPI: &str =
        workspace_asset_str!("schemas/agent-management-v1.openapi.json");

    fn validator_for(definition: &str) -> jsonschema::Validator {
        let schema: Value = serde_json::from_str(MANAGEMENT_SCHEMA).unwrap();
        jsonschema::Validator::new(&json!({
            "$schema":"https://json-schema.org/draft/2020-12/schema",
            "$ref":format!("#/$defs/{definition}"),
            "$defs":schema["$defs"]
        }))
        .unwrap()
    }

    #[test]
    fn agent_schema_closes_authoring_and_debug_requests() {
        let samples: Value = serde_json::from_str(MANAGEMENT_SAMPLES).unwrap();
        assert!(validator_for("createAgentRequest").is_valid(&samples["valid"]["create_yaml"]));
        assert!(validator_for("publishRequest").is_valid(&samples["valid"]["publish"]));
        assert!(validator_for("createResolutionRequest").is_valid(&samples["valid"]["resolution"]));
        assert!(validator_for("createDeploymentRequest").is_valid(&samples["valid"]["deployment"]));
        assert!(validator_for("activateRequest").is_valid(&samples["valid"]["activate"]));
        assert!(validator_for("createDebugSessionRequest").is_valid(&samples["valid"]["debug"]));
        assert!(validator_for("createDebugSessionRequest")
            .is_valid(&samples["valid"]["debug_live_confirmed"]));
        assert!(
            !validator_for("publishRequest").is_valid(&json!({"validation_id":"agentval_example"}))
        );
        assert!(!validator_for("createDeploymentRequest")
            .is_valid(&json!({"resolution_id":"agentres_example"})));
        assert!(
            !validator_for("createDebugSessionRequest").is_valid(&json!({
                "source":{"type":"draft","draft_version":1},
                "execution_profile_id":"author-live","input":{},
                "live_confirmation":true,"risk_preview_hash":"not-a-hash"
            }))
        );
        assert!(!validator_for("createAgentRequest").is_valid(&samples["invalid"]["wildcard_tool"]));
        assert!(!validator_for("draft").is_valid(&samples["invalid"]["path_escape"]));
        assert!(!validator_for("createDebugSessionRequest")
            .is_valid(&samples["invalid"]["debug_endpoint"]));
    }

    #[test]
    fn agent_openapi_covers_all_routes_and_delete_has_no_body() {
        let openapi: Value = serde_json::from_str(MANAGEMENT_OPENAPI).unwrap();
        assert_eq!(openapi["openapi"], "3.1.0");
        let paths = openapi["paths"].as_object().unwrap();
        let expected = BTreeSet::from([
            ("get", "/v1/admin/agents"),
            ("post", "/v1/admin/agents"),
            ("get", "/v1/admin/agents/{agent_id}"),
            ("patch", "/v1/admin/agents/{agent_id}"),
            ("delete", "/v1/admin/agents/{agent_id}"),
            ("get", "/v1/admin/agents/{agent_id}/draft"),
            ("put", "/v1/admin/agents/{agent_id}/draft"),
            ("post", "/v1/admin/agents/{agent_id}/draft/semantic-edits"),
            ("get", "/v1/admin/agents/{agent_id}/draft/view"),
            ("put", "/v1/admin/agents/{agent_id}/draft/view"),
            ("post", "/v1/admin/agents/{agent_id}/validations"),
            (
                "get",
                "/v1/admin/agents/{agent_id}/validations/{validation_id}",
            ),
            ("get", "/v1/admin/agents/{agent_id}/revisions"),
            ("post", "/v1/admin/agents/{agent_id}/revisions"),
            (
                "get",
                "/v1/admin/agents/{agent_id}/revisions/{definition_revision_id}",
            ),
            ("post", "/v1/admin/agents/{agent_id}/deployment-resolutions"),
            (
                "get",
                "/v1/admin/agents/{agent_id}/deployment-resolutions/{resolution_id}",
            ),
            ("get", "/v1/admin/agents/{agent_id}/deployments"),
            ("post", "/v1/admin/agents/{agent_id}/deployments"),
            (
                "get",
                "/v1/admin/agents/{agent_id}/deployments/{deployment_revision_id}",
            ),
            ("get", "/v1/admin/agents/{agent_id}/active-deployment"),
            ("put", "/v1/admin/agents/{agent_id}/active-deployment"),
            ("delete", "/v1/admin/agents/{agent_id}/active-deployment"),
            ("post", "/v1/admin/agents/{agent_id}/archive"),
            ("post", "/v1/admin/agents/{agent_id}/restore"),
            ("get", "/v1/admin/agents/{agent_id}/debug-sessions"),
            ("post", "/v1/admin/agents/{agent_id}/debug-sessions"),
            (
                "get",
                "/v1/admin/agents/{agent_id}/debug-sessions/{debug_session_id}",
            ),
            (
                "delete",
                "/v1/admin/agents/{agent_id}/debug-sessions/{debug_session_id}",
            ),
            (
                "get",
                "/v1/admin/agents/{agent_id}/debug-sessions/{debug_session_id}/stream",
            ),
        ]);
        let mut actual = BTreeSet::new();
        for (path, item) in paths {
            for (method, operation) in item.as_object().unwrap() {
                if method == "parameters" {
                    continue;
                }
                actual.insert((method.as_str(), path.as_str()));
                assert_eq!(
                    operation["responses"]["default"]["$ref"],
                    "#/components/responses/Error"
                );
                if method == "delete" {
                    assert!(operation.get("requestBody").is_none());
                }
            }
        }
        assert_eq!(actual, expected);
        for response in openapi["components"]["responses"]
            .as_object()
            .unwrap()
            .values()
        {
            assert_eq!(
                response["headers"]["X-Content-Type-Options"]["$ref"],
                "#/components/headers/NoSniff"
            );
            assert_eq!(
                response["headers"]["Cache-Control"]["$ref"],
                "#/components/headers/CacheControl"
            );
        }
    }
}
