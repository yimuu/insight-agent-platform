//! Installation-scoped MCP management HTTP API.
//!
//! This module keeps remote discovery I/O out of request/database
//! transactions. Mutations only validate and canonicalize bounded input, then
//! delegate lifecycle, CAS and idempotency to the durable repository.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use axum::{
    extract::{
        rejection::{JsonRejection, QueryRejection},
        DefaultBodyLimit, Extension, Path, Query, State,
    },
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use insight_durable::{
    ActivateMcpRevisionCommand, CancelMcpDiscoveryCommand, CreateMcpDiscoveryCommand,
    CreateMcpManifestCommand, CreateMcpServerCommand, CreateMcpValidationCommand,
    DeleteMcpServerCommand, DisableMcpServerCommand, McpDiscoveryStatus, McpManagedServerState,
    McpManagementConflict, McpManagementDurableRepository, McpManagementWriteError,
    McpMutationMetadata, McpMutationReceipt, McpServerRevision, McpSignedManifest,
    McpValidationReport, PublishMcpRevisionCommand, RecordMcpManagementRejectionCommand,
    ReplaceMcpDraftCommand, RetireMcpServerCommand, MCP_MANAGEMENT_MAX_REQUEST_ID_BYTES,
};
use ring::{hmac, signature};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    auth::{
        OperatorAuth, OperatorPrincipal, MCP_SERVER_DISCOVER, MCP_SERVER_PUBLISH, MCP_SERVER_READ,
        MCP_SERVER_WRITE,
    },
    response::ApiResponse,
};

const MAX_MANAGEMENT_BODY_BYTES: usize = 1024 * 1024;
const DEFAULT_PAGE_SIZE: u32 = 50;
const MAX_PAGE_SIZE: u32 = 200;

#[derive(Debug, Clone)]
pub struct McpManifestSignerPolicy {
    pub public_key: Vec<u8>,
}

#[derive(Clone)]
pub struct McpManagementPolicy {
    pub preferred_protocol: String,
    pub legacy_protocols: BTreeSet<String>,
    pub allowed_secret_refs: BTreeSet<String>,
    pub resolvable_secret_refs: BTreeSet<String>,
    pub stdio_profile_fingerprints: BTreeMap<String, String>,
    pub stdio_profile_allowed_parameters: BTreeMap<String, BTreeSet<String>>,
    pub allow_loopback_development: bool,
    pub trusted_manifest_signers: BTreeMap<String, McpManifestSignerPolicy>,
    pub manifest_max_validity: Duration,
    pub max_pending_discoveries: u32,
    pub max_request_bytes: usize,
    pub max_response_bytes: usize,
    pub max_sse_line_bytes: usize,
    pub max_sse_event_bytes: usize,
    pub max_content_items: usize,
    pub max_catalog_items: usize,
    pub policy_fingerprint: String,
    pub cursor_signing_key: [u8; 32],
}

impl std::fmt::Debug for McpManagementPolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpManagementPolicy")
            .field("preferred_protocol", &self.preferred_protocol)
            .field("legacy_protocols", &self.legacy_protocols)
            .field("allowed_secret_refs", &self.allowed_secret_refs)
            .field(
                "stdio_profile_fingerprints",
                &self.stdio_profile_fingerprints,
            )
            .field(
                "allow_loopback_development",
                &self.allow_loopback_development,
            )
            .field("manifest_max_validity", &self.manifest_max_validity)
            .field("max_pending_discoveries", &self.max_pending_discoveries)
            .field("policy_fingerprint", &self.policy_fingerprint)
            .field("cursor_signing_key", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct McpManagementApiState {
    pub auth: OperatorAuth,
    pub repository: Arc<dyn McpManagementDurableRepository>,
    pub policy: Arc<McpManagementPolicy>,
    pub authoring: Arc<dyn McpRevisionAuthoring>,
    pub readiness: Arc<dyn McpRevisionReadiness>,
}

#[async_trait]
pub trait McpRevisionAuthoring: Send + Sync {
    async fn snapshot_definition_prompts(
        &self,
        server_id: &str,
        draft: &McpDraftDocument,
        snapshot: &insight_durable::McpDiscoverySnapshot,
    ) -> Result<BTreeMap<String, Value>, ()>;
}

#[async_trait]
pub trait McpRevisionReadiness: Send + Sync {
    async fn probe(&self, revision: &McpServerRevision) -> Result<String, ()>;
}

/// Safe baseline used by tests and deployments that have a separate transport
/// reconciler. It validates immutable evidence locally and never performs I/O.
#[derive(Debug, Default)]
pub struct LocalMcpRevisionReadiness;

#[async_trait]
impl McpRevisionAuthoring for LocalMcpRevisionReadiness {
    async fn snapshot_definition_prompts(
        &self,
        _server_id: &str,
        draft: &McpDraftDocument,
        _snapshot: &insight_durable::McpDiscoverySnapshot,
    ) -> Result<BTreeMap<String, Value>, ()> {
        draft
            .imports
            .prompts
            .iter()
            .all(|prompt| prompt.definition_arguments.is_none())
            .then(BTreeMap::new)
            .ok_or(())
    }
}

#[async_trait]
impl McpRevisionReadiness for LocalMcpRevisionReadiness {
    async fn probe(&self, revision: &McpServerRevision) -> Result<String, ()> {
        canonical_hash(
            &json!({"revision_id":revision.revision_id,"revision_hash":revision.revision_hash}),
        )
        .map_err(|_| ())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDraftDocument {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<McpDraftTransport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery: Option<McpDraftDiscovery>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization: Option<McpDraftAuthorization>,
    #[serde(default)]
    pub protocol: McpDraftProtocol,
    #[serde(default)]
    pub imports: McpDraftImports,
    #[serde(default)]
    pub limits: McpDraftLimits,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator_note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum McpDraftTransport {
    StreamableHttp {
        endpoint: String,
    },
    Stdio {
        launch_profile_id: String,
        #[serde(default)]
        parameters: BTreeMap<String, String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum McpDraftDiscovery {
    LiveServiceAccount,
    SignedManifest {
        manifest_id: String,
        content_hash: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum McpDraftAuthorization {
    None,
    BearerSecretRef {
        secret_ref: String,
    },
    OauthUser {
        client_id: String,
        redirect_uri: String,
        scopes: Vec<String>,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDraftProtocol {
    #[serde(default)]
    pub preferred: String,
    #[serde(default)]
    pub legacy_fallback: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDraftImports {
    #[serde(default)]
    pub tools: Vec<McpDraftToolImport>,
    #[serde(default)]
    pub resources: McpDraftResourceImports,
    #[serde(default)]
    pub prompts: Vec<McpDraftPromptImport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDraftToolImport {
    pub remote: String,
    pub candidate_schema_hash: String,
    #[serde(rename = "as")]
    pub alias: String,
    #[serde(default)]
    pub description: McpToolDescriptionPolicy,
    pub effect: String,
    pub idempotency: String,
    pub cancellation: String,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    pub approval: String,
    pub input_required: String,
    pub tasks: String,
    #[serde(default)]
    pub terminal_only_compatible: bool,
    #[serde(default)]
    pub public: McpToolPublicPolicy,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum McpToolDescriptionPolicy {
    Remote,
    #[default]
    Disabled,
    Override {
        text: String,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpToolPublicPolicy {
    #[serde(default)]
    pub call: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDraftResourceImports {
    #[serde(default)]
    pub allow: Vec<McpDraftResourceRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDraftResourceRule {
    pub uri_pattern: String,
    #[serde(default)]
    pub mime_allowlist: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_content_bytes: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDraftPromptImport {
    pub remote: String,
    #[serde(default = "default_true")]
    pub allow_user_invocation: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition_arguments: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpDraftLimits {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_request_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_response_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_sse_line_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_sse_event_bytes: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_content_items: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_catalog_items: Option<usize>,
}

#[derive(Debug)]
struct ManagementError {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
}

impl ManagementError {
    fn invalid() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "MCP_MANAGEMENT_INVALID_REQUEST",
            "MCP management request is invalid",
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
            "MCP_MANAGEMENT_FORBIDDEN",
            "Operator capability is required",
        )
    }
    fn not_found() -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "MCP_SERVER_NOT_FOUND",
            "MCP management object was not found",
        )
    }
    fn precondition_required() -> Self {
        Self::new(
            StatusCode::PRECONDITION_REQUIRED,
            "MCP_MANAGEMENT_PRECONDITION_REQUIRED",
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
    fn new(status: StatusCode, code: &'static str, message: &'static str) -> Self {
        Self {
            status,
            code,
            message,
        }
    }
}

impl IntoResponse for ManagementError {
    fn into_response(self) -> Response {
        private_no_store((
            self.status,
            Json(json!({"code":self.code,"message":self.message,"data":null})),
        ))
    }
}

impl From<McpManagementWriteError> for ManagementError {
    fn from(error: McpManagementWriteError) -> Self {
        let McpManagementWriteError::Conflict(conflict) = error else {
            return Self::internal();
        };
        match conflict {
            McpManagementConflict::NotFound => Self::not_found(),
            McpManagementConflict::ForbiddenState | McpManagementConflict::Referenced => Self::new(
                StatusCode::CONFLICT,
                "MCP_SERVER_STATE_CONFLICT",
                "MCP Server state does not allow this operation",
            ),
            McpManagementConflict::PreconditionFailed | McpManagementConflict::FenceLost => {
                Self::new(
                    StatusCode::PRECONDITION_FAILED,
                    "MCP_MANAGEMENT_PRECONDITION_FAILED",
                    "MCP management precondition failed",
                )
            }
            McpManagementConflict::IdempotencyKeyReused => Self::new(
                StatusCode::CONFLICT,
                "MCP_IDEMPOTENCY_KEY_REUSED",
                "request ID was already used for a different request",
            ),
            McpManagementConflict::DiscoveryStale => Self::new(
                StatusCode::CONFLICT,
                "MCP_DISCOVERY_STALE",
                "MCP discovery is stale",
            ),
            McpManagementConflict::CandidateMismatch => Self::new(
                StatusCode::CONFLICT,
                "MCP_IMPORT_CANDIDATE_MISMATCH",
                "MCP import does not match the discovery candidate",
            ),
            McpManagementConflict::ValidationFailed => Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "MCP_VALIDATION_FAILED",
                "MCP validation failed",
            ),
            McpManagementConflict::CapacityExceeded => Self::new(
                StatusCode::TOO_MANY_REQUESTS,
                "MCP_DISCOVERY_CAPACITY_EXCEEDED",
                "MCP discovery capacity was exceeded",
            ),
        }
    }
}

pub fn build_mcp_management_router(state: McpManagementApiState) -> Router {
    let auth = state.auth.clone();
    let audit_repository = state.repository.clone();
    Router::new()
        .route(
            "/v1/admin/mcp/servers",
            get(list_servers).post(create_server),
        )
        .route(
            "/v1/admin/mcp/servers/{server_id}",
            get(get_server).delete(delete_server),
        )
        .route(
            "/v1/admin/mcp/servers/{server_id}/draft",
            get(get_draft).put(replace_draft),
        )
        .route(
            "/v1/admin/mcp/servers/{server_id}/manifests",
            get(list_manifests).post(create_manifest),
        )
        .route(
            "/v1/admin/mcp/servers/{server_id}/manifests/{manifest_id}",
            get(get_manifest),
        )
        .route(
            "/v1/admin/mcp/servers/{server_id}/discoveries",
            get(list_discoveries).post(create_discovery),
        )
        .route(
            "/v1/admin/mcp/servers/{server_id}/discoveries/{discovery_id}",
            get(get_discovery).delete(cancel_discovery),
        )
        .route(
            "/v1/admin/mcp/servers/{server_id}/discoveries/{discovery_id}/tools",
            get(list_candidate_tools),
        )
        .route(
            "/v1/admin/mcp/servers/{server_id}/discoveries/{discovery_id}/resources",
            get(list_candidate_resources),
        )
        .route(
            "/v1/admin/mcp/servers/{server_id}/discoveries/{discovery_id}/prompts",
            get(list_candidate_prompts),
        )
        .route(
            "/v1/admin/mcp/servers/{server_id}/tool-import-previews",
            post(preview_tool_imports),
        )
        .route(
            "/v1/admin/mcp/servers/{server_id}/draft/imports/tools",
            put(replace_tool_imports),
        )
        .route(
            "/v1/admin/mcp/servers/{server_id}/draft/imports/resources",
            put(replace_resource_imports),
        )
        .route(
            "/v1/admin/mcp/servers/{server_id}/draft/imports/prompts",
            put(replace_prompt_imports),
        )
        .route(
            "/v1/admin/mcp/servers/{server_id}/validations",
            post(create_validation),
        )
        .route(
            "/v1/admin/mcp/servers/{server_id}/validations/{validation_id}",
            get(get_validation),
        )
        .route(
            "/v1/admin/mcp/servers/{server_id}/revisions",
            get(list_revisions).post(publish_revision),
        )
        .route(
            "/v1/admin/mcp/servers/{server_id}/revisions/{revision_id}",
            get(get_revision),
        )
        .route(
            "/v1/admin/mcp/servers/{server_id}/active-revision",
            put(activate_revision).delete(disable_server),
        )
        .route(
            "/v1/admin/mcp/servers/{server_id}/retirement",
            post(retire_server),
        )
        .route_layer(DefaultBodyLimit::max(MAX_MANAGEMENT_BODY_BYTES))
        .route_layer(middleware::from_fn(
            move |headers: HeaderMap,
                  mut request: axum::http::Request<axum::body::Body>,
                  next: Next| {
                let auth = auth.clone();
                let audit_repository = audit_repository.clone();
                async move {
                    let started = std::time::Instant::now();
                    let route = management_route_class(request.uri().path());
                    let method = request.method().clone();
                    let server_id = management_server_id(request.uri().path());
                    let request_id = headers
                        .get("x-request-id")
                        .and_then(|value| value.to_str().ok())
                        .filter(|value| value.len() <= MCP_MANAGEMENT_MAX_REQUEST_ID_BYTES)
                        .unwrap_or("missing")
                        .to_owned();
                    let hide_object = request.method() == Method::GET
                        && request.uri().path() != "/v1/admin/mcp/servers";
                    let principal = auth.resolve(&headers);
                    let response = match principal.clone() {
                        Some(principal) => {
                            request.extensions_mut().insert(principal);
                            next.run(request).await
                        }
                        None if hide_object => ManagementError::not_found().into_response(),
                        None => ManagementError::unauthorized().into_response(),
                    };
                    let result = if response.status().is_success() {
                        "success"
                    } else if response.status().is_client_error() {
                        "client_error"
                    } else {
                        "server_error"
                    };
                    let duration = started.elapsed();
                    insight_mcp::record_management_request(route, result, duration);
                    let replayed = response
                        .headers()
                        .get("idempotent-replayed")
                        .is_some_and(|value| value == "true");
                    if let Some(event) = (!replayed)
                        .then(|| {
                            management_lifecycle_event(
                                route,
                                &method,
                                response.status().is_success(),
                            )
                        })
                        .flatten()
                    {
                        insight_mcp::record_management_event(event, duration);
                    }
                    if !response.status().is_success() {
                        if let Some(principal) = principal {
                            let _ = audit_repository
                                .record_mcp_management_rejection(
                                    RecordMcpManagementRejectionCommand {
                                        actor_id: principal.identity().to_owned(),
                                        request_id,
                                        server_id,
                                        subject_id: route.to_owned(),
                                        result_code: format!("http_{}", response.status().as_u16()),
                                        now: Utc::now(),
                                    },
                                )
                                .await;
                        }
                    }
                    private_no_store(response)
                }
            },
        ))
        .with_state(Arc::new(state))
}

fn management_server_id(path: &str) -> Option<String> {
    path.strip_prefix("/v1/admin/mcp/servers/")
        .and_then(|suffix| suffix.split('/').next())
        .filter(|server_id| valid_server_id(server_id))
        .map(str::to_owned)
}

fn management_lifecycle_event(
    route: &str,
    method: &Method,
    succeeded: bool,
) -> Option<insight_mcp::McpManagementEvent> {
    use insight_mcp::McpManagementEvent;
    match (route, method, succeeded) {
        ("validation", &Method::POST, true) => Some(McpManagementEvent::ValidationSucceeded),
        ("validation", &Method::POST, false) => Some(McpManagementEvent::ValidationFailed),
        ("revision", &Method::POST, true) => Some(McpManagementEvent::Published),
        ("revision", &Method::POST, false) => Some(McpManagementEvent::PublishFailed),
        ("activation", &Method::PUT, true) => Some(McpManagementEvent::Activated),
        ("activation", &Method::PUT, false) => Some(McpManagementEvent::ActivateFailed),
        ("activation", &Method::DELETE, true) => Some(McpManagementEvent::Disabled),
        ("activation", &Method::DELETE, false) => Some(McpManagementEvent::DisableFailed),
        ("retirement", &Method::POST, true) => Some(McpManagementEvent::Retired),
        ("retirement", &Method::POST, false) => Some(McpManagementEvent::RetireFailed),
        _ => None,
    }
}

fn management_route_class(path: &str) -> &'static str {
    if path.ends_with("/draft") {
        "draft"
    } else if path.contains("/manifests") {
        "manifest"
    } else if path.contains("/discoveries/")
        && (path.ends_with("/tools") || path.ends_with("/resources") || path.ends_with("/prompts"))
    {
        "candidate"
    } else if path.contains("/discoveries") {
        "discovery"
    } else if path.contains("/imports/") || path.ends_with("/tool-import-previews") {
        "import"
    } else if path.contains("/validations") {
        "validation"
    } else if path.contains("/revisions") {
        "revision"
    } else if path.ends_with("/active-revision") {
        "activation"
    } else if path.ends_with("/retirement") {
        "retirement"
    } else {
        "servers"
    }
}

fn private_no_store(response: impl IntoResponse) -> Response {
    let mut response = response.into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    response
}

fn require(principal: &OperatorPrincipal, capability: &str) -> Result<(), ManagementError> {
    principal
        .has_capability(capability)
        .then_some(())
        .ok_or_else(ManagementError::forbidden)
}

fn valid_server_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && !value.starts_with('-')
        && !value.ends_with('-')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.is_ascii()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 71
        && value.starts_with("sha256:")
        && value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_text(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && !value
            .chars()
            .any(|c| c.is_control() && !matches!(c, '\n' | '\r' | '\t'))
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

fn opaque_encode(value: &str, signing_key: &[u8; 32]) -> String {
    let key = hmac::Key::new(hmac::HMAC_SHA256, signing_key);
    let signature = hmac::sign(&key, value.as_bytes());
    format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(value),
        URL_SAFE_NO_PAD.encode(signature.as_ref())
    )
}
fn opaque_decode(
    value: Option<&str>,
    signing_key: &[u8; 32],
) -> Result<Option<String>, ManagementError> {
    value
        .map(|value| {
            let (payload, signature) =
                value.split_once('.').ok_or_else(ManagementError::invalid)?;
            URL_SAFE_NO_PAD
                .decode(payload)
                .ok()
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .filter(|decoded| decoded.len() <= 256 && !decoded.chars().any(char::is_control))
                .ok_or_else(ManagementError::invalid)
                .and_then(|decoded| {
                    let signature = URL_SAFE_NO_PAD
                        .decode(signature)
                        .map_err(|_| ManagementError::invalid())?;
                    let key = hmac::Key::new(hmac::HMAC_SHA256, signing_key);
                    hmac::verify(&key, decoded.as_bytes(), &signature)
                        .map_err(|_| ManagementError::invalid())?;
                    Ok(decoded)
                })
        })
        .transpose()
}

fn request_id(headers: &HeaderMap) -> Result<String, ManagementError> {
    let value = headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(ManagementError::invalid)?;
    if value.is_empty()
        || value.len() > MCP_MANAGEMENT_MAX_REQUEST_ID_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(ManagementError::invalid());
    }
    Ok(value.to_owned())
}

fn parse_etag(headers: &HeaderMap, prefix: &str) -> Result<u64, ManagementError> {
    let value = headers
        .get(header::IF_MATCH)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(ManagementError::precondition_required)?;
    value
        .strip_prefix(&format!("\"{prefix}-"))
        .and_then(|v| v.strip_suffix('"'))
        .and_then(|v| v.parse().ok())
        .ok_or_else(ManagementError::invalid)
}

fn metadata(
    principal: &OperatorPrincipal,
    headers: &HeaderMap,
    method: &str,
    path: String,
    body: &Value,
) -> Result<McpMutationMetadata, ManagementError> {
    Ok(McpMutationMetadata {
        operator_id: principal.identity().to_owned(),
        method: method.to_owned(),
        canonical_path: path,
        request_id: request_id(headers)?,
        request_hash: canonical_hash(body)?,
        now: Utc::now(),
    })
}

fn receipt_response(receipt: McpMutationReceipt) -> Result<Response, ManagementError> {
    let status = StatusCode::from_u16(receipt.status).map_err(|_| ManagementError::internal())?;
    let mut response = (status, Json(ApiResponse::ok(receipt.response))).into_response();
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

fn revision_receipt_response(
    server_id: &str,
    receipt: McpMutationReceipt,
) -> Result<Response, ManagementError> {
    let revision_id = receipt
        .response
        .get("revision_id")
        .and_then(Value::as_str)
        .ok_or_else(ManagementError::internal)?
        .to_owned();
    let mut response = receipt_response(receipt)?;
    response.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(&format!(
            "/v1/admin/mcp/servers/{server_id}/revisions/{revision_id}"
        ))
        .map_err(|_| ManagementError::internal())?,
    );
    Ok(response)
}

fn page_limit(limit: Option<u32>) -> Result<u32, ManagementError> {
    let limit = limit.unwrap_or(DEFAULT_PAGE_SIZE);
    (limit > 0 && limit <= MAX_PAGE_SIZE)
        .then_some(limit)
        .ok_or_else(ManagementError::invalid)
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateServerRequest {
    server_id: String,
    display_name: String,
    #[serde(default)]
    draft: Option<McpDraftDocument>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PageQuery {
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerPageQuery {
    #[serde(default)]
    state: Option<McpManagedServerState>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DiscoveryPageQuery {
    #[serde(default)]
    status: Option<McpDiscoveryStatus>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    cursor: Option<String>,
}

fn normalize_draft(
    mut draft: McpDraftDocument,
    policy: &McpManagementPolicy,
) -> Result<Value, ManagementError> {
    if draft.protocol.preferred.is_empty() {
        draft.protocol.preferred = policy.preferred_protocol.clone();
    }
    if draft.protocol.preferred != policy.preferred_protocol
        || draft
            .protocol
            .legacy_fallback
            .iter()
            .any(|version| !policy.legacy_protocols.contains(version))
        || has_duplicates(&draft.protocol.legacy_fallback)
    {
        return Err(ManagementError::invalid());
    }
    if draft
        .description
        .as_deref()
        .is_some_and(|v| !valid_text(v, 4096))
        || draft
            .operator_note
            .as_deref()
            .is_some_and(|v| !valid_text(v, 4096))
    {
        return Err(ManagementError::invalid());
    }
    if let Some(transport) = &draft.transport {
        match transport {
            McpDraftTransport::StreamableHttp { endpoint } => {
                validate_endpoint(endpoint, policy.allow_loopback_development)?
            }
            McpDraftTransport::Stdio {
                launch_profile_id,
                parameters,
            } => {
                let Some(allowed_parameters) = policy
                    .stdio_profile_allowed_parameters
                    .get(launch_profile_id)
                else {
                    return Err(ManagementError::invalid());
                };
                if !policy
                    .stdio_profile_fingerprints
                    .contains_key(launch_profile_id)
                    || parameters.len() > 128
                    || parameters
                        .keys()
                        .any(|name| !allowed_parameters.contains(name))
                    || parameters
                        .iter()
                        .any(|(name, value)| !valid_name(name) || !valid_text(value, 16 * 1024))
                {
                    return Err(ManagementError::invalid());
                }
            }
        }
    }
    if let Some(authorization) = &mut draft.authorization {
        match authorization {
            McpDraftAuthorization::None => {}
            McpDraftAuthorization::BearerSecretRef { secret_ref } => {
                if !policy.allowed_secret_refs.contains(secret_ref) {
                    return Err(ManagementError::invalid());
                }
            }
            McpDraftAuthorization::OauthUser {
                client_id,
                redirect_uri,
                scopes,
            } => {
                let redirect =
                    reqwest::Url::parse(redirect_uri).map_err(|_| ManagementError::invalid())?;
                if !valid_text(client_id, 512)
                    || redirect.scheme() != "https"
                    || redirect.host_str().is_none()
                    || !redirect.username().is_empty()
                    || redirect.password().is_some()
                    || redirect.fragment().is_some()
                    || scopes.is_empty()
                    || has_duplicates(scopes)
                    || scopes.iter().any(|scope| !valid_text(scope, 256))
                {
                    return Err(ManagementError::invalid());
                }
                scopes.sort();
            }
        }
    }
    fill_and_validate_limits(&mut draft.limits, policy)?;
    if draft.imports.tools.len() > policy.max_catalog_items
        || draft.imports.resources.allow.len() > policy.max_catalog_items
        || draft.imports.prompts.len() > policy.max_catalog_items
    {
        return Err(ManagementError::invalid());
    }
    validate_and_sort_imports(&mut draft.imports, None)?;
    let response_limit = draft
        .limits
        .max_response_bytes
        .ok_or_else(ManagementError::invalid)?;
    if draft.imports.resources.allow.iter().any(|rule| {
        rule.max_content_bytes
            .is_some_and(|limit| limit == 0 || limit > response_limit)
    }) {
        return Err(ManagementError::invalid());
    }
    serde_json::to_value(draft).map_err(|_| ManagementError::invalid())
}

fn validate_endpoint(
    endpoint: &str,
    allow_loopback_development: bool,
) -> Result<(), ManagementError> {
    let url = reqwest::Url::parse(endpoint).map_err(|_| ManagementError::invalid())?;
    let secure = url.scheme() == "https";
    let loopback_development = allow_loopback_development
        && url.scheme() == "http"
        && url.host_str().is_some_and(|host| {
            host.eq_ignore_ascii_case("localhost")
                || host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
        });
    if (!secure && !loopback_development)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.host_str().is_none()
    {
        return Err(ManagementError::invalid());
    }
    Ok(())
}

fn fill_and_validate_limits(
    limits: &mut McpDraftLimits,
    policy: &McpManagementPolicy,
) -> Result<(), ManagementError> {
    let request = *limits
        .max_request_bytes
        .get_or_insert(policy.max_request_bytes);
    let response = *limits
        .max_response_bytes
        .get_or_insert(policy.max_response_bytes);
    let line = *limits
        .max_sse_line_bytes
        .get_or_insert(policy.max_sse_line_bytes);
    let event = *limits
        .max_sse_event_bytes
        .get_or_insert(policy.max_sse_event_bytes);
    let contents = *limits
        .max_content_items
        .get_or_insert(policy.max_content_items);
    let catalog = *limits
        .max_catalog_items
        .get_or_insert(policy.max_catalog_items);
    if request == 0
        || request > policy.max_request_bytes
        || response == 0
        || response > policy.max_response_bytes
        || line == 0
        || line > policy.max_sse_line_bytes
        || event == 0
        || event > policy.max_sse_event_bytes
        || contents == 0
        || contents > policy.max_content_items
        || catalog == 0
        || catalog > policy.max_catalog_items
        || line > event
        || event > response
    {
        return Err(ManagementError::invalid());
    }
    Ok(())
}

fn validate_and_sort_imports(
    imports: &mut McpDraftImports,
    candidates: Option<&BTreeMap<String, String>>,
) -> Result<(), ManagementError> {
    imports.tools.sort_by(|a, b| a.remote.cmp(&b.remote));
    imports.prompts.sort_by(|a, b| a.remote.cmp(&b.remote));
    imports
        .resources
        .allow
        .sort_by(|a, b| a.uri_pattern.cmp(&b.uri_pattern));
    let mut remotes = BTreeSet::new();
    let mut aliases = BTreeSet::new();
    let mut normalized_aliases = BTreeSet::new();
    for tool in &mut imports.tools {
        tool.required_capabilities.sort();
        tool.required_capabilities.dedup();
        if !valid_name(&tool.remote)
            || !valid_name(&tool.alias)
            || !valid_sha256(&tool.candidate_schema_hash)
            || tool.remote == "*"
            || tool.alias == "*"
            || !remotes.insert(tool.remote.clone())
            || !aliases.insert(tool.alias.clone())
            || !normalized_aliases.insert(tool.alias.replace('.', "_"))
            || tool
                .required_capabilities
                .iter()
                .any(|capability| !valid_name(capability))
            || !matches!(tool.effect.as_str(), "pure" | "read_only" | "mutating")
            || !matches!(
                tool.idempotency.as_str(),
                "idempotent" | "non_idempotent" | "unknown"
            )
            || !matches!(
                tool.cancellation.as_str(),
                "not_supported" | "cooperative" | "immediate"
            )
            || !matches!(
                tool.approval.as_str(),
                "never" | "model_tool_only" | "mutating" | "always"
            )
            || !matches!(tool.input_required.as_str(), "denied" | "allowed")
            || !matches!(tool.tasks.as_str(), "denied" | "allowed")
            || tool.terminal_only_compatible
                && (tool.input_required == "allowed"
                    || tool.tasks == "allowed"
                    || tool.approval != "never")
            || matches!(&tool.description, McpToolDescriptionPolicy::Override { text } if !valid_text(text, 4096))
        {
            return Err(ManagementError::invalid());
        }
        if let Some(candidates) = candidates {
            if candidates.get(&tool.remote) != Some(&tool.candidate_schema_hash) {
                return Err(ManagementError::new(
                    StatusCode::CONFLICT,
                    "MCP_IMPORT_CANDIDATE_MISMATCH",
                    "MCP import does not match the discovery candidate",
                ));
            }
        }
    }
    for resource in &mut imports.resources.allow {
        resource.mime_allowlist.sort();
        resource.mime_allowlist.dedup();
    }
    if imports
        .prompts
        .windows(2)
        .any(|v| v[0].remote == v[1].remote)
        || imports
            .resources
            .allow
            .windows(2)
            .any(|v| v[0].uri_pattern == v[1].uri_pattern)
        || imports.prompts.iter().any(|p| {
            !valid_name(&p.remote)
                || p.definition_arguments.as_ref().is_some_and(|arguments| {
                    arguments.len() > 128
                        || arguments
                            .iter()
                            .any(|(name, value)| !valid_name(name) || !valid_text(value, 16 * 1024))
                })
        })
        || imports.resources.allow.iter().any(|r| {
            r.uri_pattern.is_empty()
                || r.uri_pattern.len() > 2048
                || r.uri_pattern.chars().any(char::is_control)
                || r.uri_pattern.matches('*').count() > 1
                || r.mime_allowlist.len() > 128
                || r.mime_allowlist.iter().any(|mime| {
                    mime.is_empty() || mime.len() > 256 || mime.chars().any(char::is_control)
                })
        })
    {
        return Err(ManagementError::invalid());
    }
    Ok(())
}

fn wildcard_matches(pattern: &str, value: &str) -> bool {
    match pattern.split_once('*') {
        None => pattern == value,
        Some((prefix, suffix)) if !suffix.contains('*') => {
            value.starts_with(prefix)
                && value.ends_with(suffix)
                && value.len() >= prefix.len() + suffix.len()
        }
        Some(_) => false,
    }
}

fn snapshot_prompt_names(document: &Value) -> BTreeSet<String> {
    document
        .get("prompts")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| item.get("name").and_then(Value::as_str).map(str::to_owned))
        .collect()
}

fn snapshot_resource_identities(document: &Value) -> Vec<String> {
    let mut identities = document
        .get("resources")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| item.get("uri").and_then(Value::as_str).map(str::to_owned))
        .collect::<Vec<_>>();
    identities.extend(
        document
            .get("resource_templates")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|item| {
                item.get("uriTemplate")
                    .or_else(|| item.get("uri_template"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            }),
    );
    identities.sort();
    identities.dedup();
    identities
}

fn discovery_input_hash(
    document: &Value,
    policy: &McpManagementPolicy,
) -> Result<String, ManagementError> {
    canonical_hash(&json!({
        "transport": document.get("transport"),
        "discovery": document.get("discovery"),
        "authorization": document.get("authorization"),
        "protocol": document.get("protocol"),
        "limits": document.get("limits"),
        "policy_fingerprint": policy.policy_fingerprint,
    }))
}

fn revision_matches_policy(revision: &McpServerRevision, policy: &McpManagementPolicy) -> bool {
    if revision
        .document
        .get("policy_fingerprint")
        .and_then(Value::as_str)
        != Some(policy.policy_fingerprint.as_str())
    {
        return false;
    }
    match revision
        .document
        .pointer("/draft/transport/type")
        .and_then(Value::as_str)
    {
        Some("streamable_http") => true,
        Some("stdio") => {
            let Some(profile_id) = revision
                .document
                .pointer("/draft/transport/launch_profile_id")
                .and_then(Value::as_str)
            else {
                return false;
            };
            revision
                .document
                .get("stdio_profile_fingerprint")
                .and_then(Value::as_str)
                .zip(
                    policy
                        .stdio_profile_fingerprints
                        .get(profile_id)
                        .map(String::as_str),
                )
                .is_some_and(|(published, current)| published == current)
        }
        _ => false,
    }
}

fn has_duplicates(values: &[String]) -> bool {
    values.iter().collect::<BTreeSet<_>>().len() != values.len()
}

async fn create_server(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    headers: HeaderMap,
    input: Result<Json<CreateServerRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, MCP_SERVER_WRITE)?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    if !valid_server_id(&input.server_id) || !valid_text(&input.display_name, 512) {
        return Err(ManagementError::invalid());
    }
    let draft = normalize_draft(
        input.draft.unwrap_or(McpDraftDocument {
            transport: None,
            discovery: None,
            authorization: None,
            protocol: McpDraftProtocol::default(),
            imports: McpDraftImports::default(),
            limits: McpDraftLimits::default(),
            description: None,
            operator_note: None,
        }),
        &state.policy,
    )?;
    let body = json!({"server_id":input.server_id,"display_name":input.display_name,"draft":draft});
    let command = CreateMcpServerCommand {
        metadata: metadata(
            &principal,
            &headers,
            "POST",
            "/v1/admin/mcp/servers".to_owned(),
            &body,
        )?,
        server_id: input.server_id,
        display_name: input.display_name,
        discovery_input_hash: discovery_input_hash(&draft, &state.policy)?,
        draft_document: draft,
    };
    receipt_response(
        state
            .repository
            .create_mcp_server(command)
            .await
            .map_err(ManagementError::from)?,
    )
}

async fn list_servers(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    query: Result<Query<ServerPageQuery>, QueryRejection>,
) -> Result<Json<ApiResponse<Value>>, ManagementError> {
    require(&principal, MCP_SERVER_READ)?;
    let Query(query) = query.map_err(|_| ManagementError::invalid())?;
    let cursor = opaque_decode(query.cursor.as_deref(), &state.policy.cursor_signing_key)?;
    let mut page = state
        .repository
        .list_mcp_servers(query.state, cursor.as_deref(), page_limit(query.limit)?)
        .await
        .map_err(|_| ManagementError::internal())?;
    page.next_cursor = page
        .next_cursor
        .map(|value| opaque_encode(&value, &state.policy.cursor_signing_key));
    Ok(Json(ApiResponse::ok(
        serde_json::to_value(page).map_err(|_| ManagementError::internal())?,
    )))
}

async fn get_server(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(server_id): Path<String>,
) -> Result<Response, ManagementError> {
    require(&principal, MCP_SERVER_READ)?;
    let server = state
        .repository
        .get_mcp_server(&server_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let draft = state
        .repository
        .get_mcp_draft(&server_id)
        .await
        .map_err(|_| ManagementError::internal())?;
    let active_revision = match server.active_revision_id.as_deref() {
        Some(revision_id) => state
            .repository
            .get_mcp_revision(&server_id, revision_id)
            .await
            .map_err(|_| ManagementError::internal())?,
        None => None,
    };
    let active_discovery = match active_revision.as_ref() {
        Some(revision) => state
            .repository
            .get_mcp_discovery(&server_id, &revision.discovery_id)
            .await
            .map_err(|_| ManagementError::internal())?,
        None => None,
    };
    let active_revision_policy_compatible = active_revision
        .as_ref()
        .is_none_or(|revision| revision_matches_policy(revision, &state.policy));
    let mut response = Json(ApiResponse::ok(json!({
        "server":server,
        "draft":draft,
        "active_revision":active_revision,
        "active_discovery_stale":active_discovery.as_ref().is_some_and(|operation| operation.stale),
        "active_revision_policy_compatible":active_revision_policy_compatible,
        "degraded_reason":(!active_revision_policy_compatible).then_some("policy_incompatible")
    })))
    .into_response();
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"server-{}\"", server.server_version))
            .map_err(|_| ManagementError::internal())?,
    );
    Ok(response)
}

async fn get_draft(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(server_id): Path<String>,
) -> Result<Response, ManagementError> {
    require(&principal, MCP_SERVER_READ)?;
    let draft = state
        .repository
        .get_mcp_draft(&server_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let mut response = Json(ApiResponse::ok(
        serde_json::to_value(&draft).map_err(|_| ManagementError::internal())?,
    ))
    .into_response();
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"draft-{}\"", draft.draft_version))
            .map_err(|_| ManagementError::internal())?,
    );
    Ok(response)
}

async fn replace_draft(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(server_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<McpDraftDocument>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, MCP_SERVER_WRITE)?;
    let expected = parse_etag(&headers, "draft")?;
    let document = normalize_draft(
        input.map_err(|_| ManagementError::invalid())?.0,
        &state.policy,
    )?;
    let command = ReplaceMcpDraftCommand {
        metadata: metadata(
            &principal,
            &headers,
            "PUT",
            format!("/v1/admin/mcp/servers/{server_id}/draft"),
            &document,
        )?,
        server_id: server_id.clone(),
        expected_draft_version: expected,
        discovery_input_hash: discovery_input_hash(&document, &state.policy)?,
        draft_document: document,
    };
    receipt_response(
        state
            .repository
            .replace_mcp_draft(command)
            .await
            .map_err(ManagementError::from)?,
    )
}

async fn delete_server(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(server_id): Path<String>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, ManagementError> {
    require(&principal, MCP_SERVER_WRITE)?;
    if !body.is_empty() {
        return Err(ManagementError::invalid());
    }
    let expected = parse_etag(&headers, "server")?;
    let body = json!({});
    let command = DeleteMcpServerCommand {
        metadata: metadata(
            &principal,
            &headers,
            "DELETE",
            format!("/v1/admin/mcp/servers/{server_id}"),
            &body,
        )?,
        server_id,
        expected_server_version: expected,
    };
    receipt_response(
        state
            .repository
            .delete_mcp_server(command)
            .await
            .map_err(ManagementError::from)?,
    )
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestUploadRequest {
    format: String,
    payload: String,
    key_id: String,
    signature: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManifestPayload {
    server_id: String,
    protocol: Value,
    discover: Value,
    tools: Vec<Value>,
    resources: Vec<Value>,
    resource_templates: Vec<Value>,
    prompts: Vec<Value>,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    content_hash: String,
}

fn manifest_summary(manifest: &McpSignedManifest) -> Value {
    json!({
        "manifest_id":manifest.manifest_id,"server_id":manifest.server_id,"format":manifest.format,
        "key_id":manifest.key_id,"content_hash":manifest.content_hash,"issued_at":manifest.issued_at,
        "expires_at":manifest.expires_at,"created_at":manifest.created_at,"created_by":manifest.created_by,
    })
}

async fn create_manifest(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(server_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<ManifestUploadRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, MCP_SERVER_DISCOVER)?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    if input.format != "jcs-ed25519-v1"
        || input.payload.len() > MAX_MANAGEMENT_BODY_BYTES * 2
        || input.signature.len() > 512
    {
        return Err(ManagementError::invalid());
    }
    let signer = state
        .policy
        .trusted_manifest_signers
        .get(&input.key_id)
        .ok_or_else(ManagementError::forbidden)?;
    let payload_bytes = URL_SAFE_NO_PAD
        .decode(&input.payload)
        .map_err(|_| ManagementError::invalid())?;
    if payload_bytes.len() > MAX_MANAGEMENT_BODY_BYTES {
        return Err(ManagementError::invalid());
    }
    let value: Value =
        serde_json::from_slice(&payload_bytes).map_err(|_| ManagementError::invalid())?;
    if serde_jcs::to_vec(&value).map_err(|_| ManagementError::invalid())? != payload_bytes {
        return Err(ManagementError::invalid());
    }
    let parsed: ManifestPayload =
        serde_json::from_value(value.clone()).map_err(|_| ManagementError::invalid())?;
    if parsed.server_id != server_id
        || parsed.tools.len() > state.policy.max_catalog_items
        || parsed.resources.len() > state.policy.max_catalog_items
        || parsed.resource_templates.len() > state.policy.max_catalog_items
        || parsed.prompts.len() > state.policy.max_catalog_items
        || parsed
            .protocol
            .get("preferred")
            .and_then(Value::as_str)
            .is_none_or(|version| {
                version != state.policy.preferred_protocol
                    && !state.policy.legacy_protocols.contains(version)
            })
        || !parsed
            .discover
            .get("capabilities")
            .is_some_and(Value::is_object)
        || !parsed
            .discover
            .get("serverInfo")
            .is_some_and(Value::is_object)
        || !manifest_catalog_is_structurally_valid(&parsed)
        || parsed.issued_at > Utc::now() + chrono::Duration::minutes(5)
        || parsed.expires_at <= Utc::now()
        || parsed.expires_at <= parsed.issued_at
        || (parsed.expires_at - parsed.issued_at)
            .to_std()
            .map_err(|_| ManagementError::invalid())?
            > state.policy.manifest_max_validity
    {
        return Err(ManagementError::invalid());
    }
    let signature_bytes = URL_SAFE_NO_PAD
        .decode(&input.signature)
        .map_err(|_| ManagementError::invalid())?;
    signature::UnparsedPublicKey::new(&signature::ED25519, &signer.public_key)
        .verify(&payload_bytes, &signature_bytes)
        .map_err(|_| ManagementError::forbidden())?;
    let mut hash_value = value;
    hash_value
        .as_object_mut()
        .ok_or_else(ManagementError::invalid)?
        .remove("content_hash");
    let content_hash = canonical_hash(&hash_value)?;
    if parsed.content_hash != content_hash {
        return Err(ManagementError::invalid());
    }
    let manifest = McpSignedManifest {
        manifest_id: format!("mmanifest_{}", Uuid::new_v4().simple()),
        server_id: server_id.clone(),
        format: input.format,
        key_id: input.key_id,
        payload: input.payload,
        signature: input.signature,
        content_hash,
        issued_at: parsed.issued_at,
        expires_at: parsed.expires_at,
        created_at: Utc::now(),
        created_by: principal.identity().to_owned(),
    };
    let body = serde_json::to_value(&manifest).map_err(|_| ManagementError::invalid())?;
    let command = CreateMcpManifestCommand {
        metadata: metadata(
            &principal,
            &headers,
            "POST",
            format!("/v1/admin/mcp/servers/{server_id}/manifests"),
            &body,
        )?,
        manifest,
    };
    let receipt = state
        .repository
        .create_mcp_manifest(command)
        .await
        .map_err(ManagementError::from)?;
    // Repository responses contain the immutable payload for replay authority;
    // the HTTP projection never echoes it.
    let stored: McpSignedManifest =
        serde_json::from_value(receipt.response).map_err(|_| ManagementError::internal())?;
    let safe = McpMutationReceipt {
        response: manifest_summary(&stored),
        ..receipt
    };
    receipt_response(safe)
}

fn manifest_catalog_is_structurally_valid(payload: &ManifestPayload) -> bool {
    let mut tool_names = BTreeSet::new();
    let tools_valid = payload.tools.iter().all(|tool| {
        let name = tool
            .get("remote")
            .or_else(|| tool.get("name"))
            .and_then(Value::as_str);
        name.is_some_and(|name| valid_name(name) && tool_names.insert(name.to_owned()))
            && tool
                .get("input_schema")
                .or_else(|| tool.get("inputSchema"))
                .is_some_and(Value::is_object)
    });
    let mut resource_uris = BTreeSet::new();
    let resources_valid = payload.resources.iter().all(|resource| {
        resource
            .get("uri")
            .and_then(Value::as_str)
            .is_some_and(|uri| valid_text(uri, 8 * 1024) && resource_uris.insert(uri.to_owned()))
    });
    let mut template_uris = BTreeSet::new();
    let templates_valid = payload.resource_templates.iter().all(|template| {
        template
            .get("uriTemplate")
            .or_else(|| template.get("uri_template"))
            .and_then(Value::as_str)
            .is_some_and(|uri| valid_text(uri, 8 * 1024) && template_uris.insert(uri.to_owned()))
    });
    let mut prompt_names = BTreeSet::new();
    let prompts_valid = payload.prompts.iter().all(|prompt| {
        prompt
            .get("name")
            .and_then(Value::as_str)
            .is_some_and(|name| valid_name(name) && prompt_names.insert(name.to_owned()))
    });
    tools_valid && resources_valid && templates_valid && prompts_valid
}

async fn list_manifests(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(server_id): Path<String>,
    query: Result<Query<PageQuery>, QueryRejection>,
) -> Result<Json<ApiResponse<Value>>, ManagementError> {
    require(&principal, MCP_SERVER_READ)?;
    let Query(query) = query.map_err(|_| ManagementError::invalid())?;
    let cursor = opaque_decode(query.cursor.as_deref(), &state.policy.cursor_signing_key)?;
    let page = state
        .repository
        .list_mcp_manifests(&server_id, cursor.as_deref(), page_limit(query.limit)?)
        .await
        .map_err(|_| ManagementError::internal())?;
    Ok(Json(ApiResponse::ok(
        json!({"items":page.items.iter().map(manifest_summary).collect::<Vec<_>>(),"next_cursor":page.next_cursor.map(|v| opaque_encode(&v, &state.policy.cursor_signing_key))}),
    )))
}

async fn get_manifest(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path((server_id, manifest_id)): Path<(String, String)>,
) -> Result<Json<ApiResponse<Value>>, ManagementError> {
    require(&principal, MCP_SERVER_READ)?;
    let manifest = state
        .repository
        .get_mcp_manifest(&server_id, &manifest_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    Ok(Json(ApiResponse::ok(manifest_summary(&manifest))))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyRequest {}

async fn create_discovery(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(server_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<EmptyRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, MCP_SERVER_DISCOVER)?;
    let _ = input.map_err(|_| ManagementError::invalid())?;
    let expected = parse_etag(&headers, "draft")?;
    let draft = state
        .repository
        .get_mcp_draft(&server_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    if draft.draft_version != expected {
        return Err(ManagementError::from(McpManagementWriteError::Conflict(
            McpManagementConflict::PreconditionFailed,
        )));
    }
    let body = json!({});
    let discovery_id = format!("mdisc_{}", Uuid::new_v4().simple());
    let command = CreateMcpDiscoveryCommand {
        metadata: metadata(
            &principal,
            &headers,
            "POST",
            format!("/v1/admin/mcp/servers/{server_id}/discoveries"),
            &body,
        )?,
        discovery_id: discovery_id.clone(),
        server_id: server_id.clone(),
        expected_draft_version: expected,
        discovery_input_hash: draft.discovery_input_hash,
        max_pending_discoveries: state.policy.max_pending_discoveries,
    };
    let receipt = state
        .repository
        .create_mcp_discovery(command)
        .await
        .map_err(ManagementError::from)?;
    let mut response = receipt_response(receipt)?;
    response.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(&format!(
            "/v1/admin/mcp/servers/{server_id}/discoveries/{discovery_id}"
        ))
        .map_err(|_| ManagementError::internal())?,
    );
    Ok(response)
}

async fn list_discoveries(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(server_id): Path<String>,
    query: Result<Query<DiscoveryPageQuery>, QueryRejection>,
) -> Result<Json<ApiResponse<Value>>, ManagementError> {
    require(&principal, MCP_SERVER_READ)?;
    let Query(query) = query.map_err(|_| ManagementError::invalid())?;
    let cursor = opaque_decode(query.cursor.as_deref(), &state.policy.cursor_signing_key)?;
    let mut page = state
        .repository
        .list_mcp_discoveries(
            &server_id,
            query.status,
            cursor.as_deref(),
            page_limit(query.limit)?,
        )
        .await
        .map_err(|_| ManagementError::internal())?;
    page.next_cursor = page
        .next_cursor
        .map(|v| opaque_encode(&v, &state.policy.cursor_signing_key));
    let draft = state
        .repository
        .get_mcp_draft(&server_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let current_hash = discovery_input_hash(&draft.document, &state.policy)?;
    Ok(Json(ApiResponse::ok(json!({
        "items":page.items.iter().map(|operation| discovery_operation_projection(operation, &current_hash)).collect::<Vec<_>>(),
        "next_cursor":page.next_cursor
    }))))
}

async fn get_discovery(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path((server_id, discovery_id)): Path<(String, String)>,
) -> Result<Json<ApiResponse<Value>>, ManagementError> {
    require(&principal, MCP_SERVER_READ)?;
    let operation = state
        .repository
        .get_mcp_discovery(&server_id, &discovery_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let snapshot = state
        .repository
        .get_mcp_discovery_snapshot(&server_id, &discovery_id)
        .await
        .map_err(|_| ManagementError::internal())?;
    let draft = state
        .repository
        .get_mcp_draft(&server_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let current_hash = discovery_input_hash(&draft.document, &state.policy)?;
    Ok(Json(ApiResponse::ok(
        json!({"operation":discovery_operation_projection(&operation, &current_hash),"snapshot":snapshot.as_ref().map(|s| json!({"catalog_fingerprint":s.catalog_fingerprint,"created_at":s.created_at}))}),
    )))
}

fn discovery_operation_projection(
    operation: &insight_durable::McpDiscoveryOperation,
    current_discovery_input_hash: &str,
) -> Value {
    let policy_stale = operation.discovery_input_hash != current_discovery_input_hash;
    let mut projection = serde_json::to_value(operation).unwrap_or_else(|_| json!({}));
    if let Some(object) = projection.as_object_mut() {
        object.insert(
            "effective_stale".to_owned(),
            Value::Bool(operation.stale || policy_stale),
        );
        object.insert(
            "effective_stale_reason".to_owned(),
            if operation.stale {
                operation
                    .stale_reason
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null)
            } else if policy_stale {
                Value::String("platform_policy_changed".to_owned())
            } else {
                Value::Null
            },
        );
    }
    projection
}

async fn cancel_discovery(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path((server_id, discovery_id)): Path<(String, String)>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, ManagementError> {
    require(&principal, MCP_SERVER_DISCOVER)?;
    if !body.is_empty() {
        return Err(ManagementError::invalid());
    }
    let body = json!({});
    let command = CancelMcpDiscoveryCommand {
        metadata: metadata(
            &principal,
            &headers,
            "DELETE",
            format!("/v1/admin/mcp/servers/{server_id}/discoveries/{discovery_id}"),
            &body,
        )?,
        server_id,
        discovery_id,
    };
    receipt_response(
        state
            .repository
            .cancel_mcp_discovery(command)
            .await
            .map_err(ManagementError::from)?,
    )
}

async fn list_candidate_tools(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path((server_id, discovery_id)): Path<(String, String)>,
    query: Result<Query<PageQuery>, QueryRejection>,
) -> Result<Json<ApiResponse<Value>>, ManagementError> {
    let Query(query) = query.map_err(|_| ManagementError::invalid())?;
    candidate_page(
        &state,
        &principal,
        &server_id,
        &discovery_id,
        "tools",
        query,
    )
    .await
}
async fn list_candidate_resources(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path((server_id, discovery_id)): Path<(String, String)>,
    query: Result<Query<PageQuery>, QueryRejection>,
) -> Result<Json<ApiResponse<Value>>, ManagementError> {
    let Query(query) = query.map_err(|_| ManagementError::invalid())?;
    candidate_page(
        &state,
        &principal,
        &server_id,
        &discovery_id,
        "resources",
        query,
    )
    .await
}
async fn list_candidate_prompts(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path((server_id, discovery_id)): Path<(String, String)>,
    query: Result<Query<PageQuery>, QueryRejection>,
) -> Result<Json<ApiResponse<Value>>, ManagementError> {
    let Query(query) = query.map_err(|_| ManagementError::invalid())?;
    candidate_page(
        &state,
        &principal,
        &server_id,
        &discovery_id,
        "prompts",
        query,
    )
    .await
}

async fn candidate_page(
    state: &McpManagementApiState,
    principal: &OperatorPrincipal,
    server_id: &str,
    discovery_id: &str,
    field: &str,
    query: PageQuery,
) -> Result<Json<ApiResponse<Value>>, ManagementError> {
    require(principal, MCP_SERVER_READ)?;
    let snapshot = state
        .repository
        .get_mcp_discovery_snapshot(server_id, discovery_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let mut items = if field == "resources" {
        let mut resources = snapshot
            .document
            .get("resources")
            .and_then(Value::as_array)
            .ok_or_else(ManagementError::internal)?
            .clone();
        resources.extend(
            snapshot
                .document
                .get("resource_templates")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .cloned(),
        );
        resources
    } else {
        snapshot
            .document
            .get(field)
            .and_then(Value::as_array)
            .ok_or_else(ManagementError::internal)?
            .clone()
    };
    let rejection_fields: &[(&str, &str)] = match field {
        "tools" => &[("tools", "tool")],
        "resources" => &[
            ("resources", "resource"),
            ("resource_templates", "resource_template"),
        ],
        "prompts" => &[("prompts", "prompt")],
        _ => return Err(ManagementError::internal()),
    };
    for (rejection_field, candidate_kind) in rejection_fields {
        items.extend(
            snapshot
                .document
                .pointer(&format!("/rejections/{rejection_field}"))
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .map(|rejection| {
                    json!({
                        "identity":rejection.get("identity"),
                        "candidate_kind":candidate_kind,
                        "importable":false,
                        "rejection_code":rejection.get("code")
                    })
                }),
        );
    }
    items.sort_by_key(candidate_projection_identity);
    let start = opaque_decode(query.cursor.as_deref(), &state.policy.cursor_signing_key)?
        .map(|v| v.parse::<usize>().map_err(|_| ManagementError::invalid()))
        .transpose()?
        .unwrap_or(0);
    let limit = page_limit(query.limit)? as usize;
    if start > items.len() {
        return Err(ManagementError::invalid());
    }
    let end = (start + limit).min(items.len());
    Ok(Json(ApiResponse::ok(
        json!({"items":&items[start..end],"next_cursor":(end<items.len()).then(||opaque_encode(&end.to_string(), &state.policy.cursor_signing_key)),"catalog_fingerprint":snapshot.catalog_fingerprint}),
    )))
}

fn candidate_projection_identity(item: &Value) -> String {
    item.get("remote")
        .or_else(|| item.get("name"))
        .or_else(|| item.get("uri"))
        .or_else(|| item.get("uriTemplate"))
        .or_else(|| item.get("uri_template"))
        .or_else(|| item.get("identity"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ToolPreviewRequest {
    discovery_id: String,
    selection: ToolSelection,
    #[serde(default)]
    alias_prefix: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum ToolSelection {
    All,
    Names { names: Vec<String> },
}

fn snapshot_tool_candidates(
    document: &Value,
) -> Result<BTreeMap<String, (String, Value)>, ManagementError> {
    let mut result = BTreeMap::new();
    for candidate in document
        .get("tools")
        .and_then(Value::as_array)
        .ok_or_else(ManagementError::internal)?
    {
        let remote = candidate
            .get("remote")
            .or_else(|| candidate.get("name"))
            .and_then(Value::as_str)
            .ok_or_else(ManagementError::internal)?
            .to_owned();
        let schema_hash = candidate
            .get("schema_hash")
            .or_else(|| candidate.get("candidate_schema_hash"))
            .and_then(Value::as_str)
            .ok_or_else(ManagementError::internal)?
            .to_owned();
        if result
            .insert(remote, (schema_hash, candidate.clone()))
            .is_some()
        {
            return Err(ManagementError::internal());
        }
    }
    Ok(result)
}

async fn preview_tool_imports(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(server_id): Path<String>,
    input: Result<Json<ToolPreviewRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<Value>>, ManagementError> {
    require(&principal, MCP_SERVER_WRITE)?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    if !input.alias_prefix.is_empty() && !valid_name(input.alias_prefix.trim_end_matches('_')) {
        return Err(ManagementError::invalid());
    }
    let snapshot = usable_snapshot(&state, &server_id, &input.discovery_id).await?;
    let candidates = snapshot_tool_candidates(&snapshot.document)?;
    let names = match input.selection {
        ToolSelection::All => candidates.keys().cloned().collect::<Vec<_>>(),
        ToolSelection::Names { mut names } => {
            names.sort();
            names.dedup();
            if names.iter().any(|v| v == "*" || !valid_name(v)) {
                return Err(ManagementError::invalid());
            }
            names
        }
    };
    if names.len() > state.policy.max_catalog_items {
        return Err(ManagementError::invalid());
    }
    let mut aliases = BTreeSet::new();
    let mut items = Vec::new();
    let mut rejected = Vec::new();
    for remote in names {
        let Some((schema_hash, _)) = candidates.get(&remote) else {
            rejected.push(json!({"remote":remote,"reason_code":"candidate_not_found"}));
            continue;
        };
        let alias = format!("{}{}", input.alias_prefix, remote.replace('.', "_"));
        if !valid_name(&alias) || !aliases.insert(alias.clone()) {
            rejected.push(json!({"remote":remote,"reason_code":"alias_conflict"}));
            continue;
        }
        items.push(json!({
            "remote":remote,"candidate_schema_hash":schema_hash,"as":alias,"description":{"mode":"disabled"},
            "effect":"mutating","idempotency":"unknown","cancellation":"not_supported","required_capabilities":[],
            "approval":"always","input_required":"denied","tasks":"denied","terminal_only_compatible":false,"public":{"call":false}
        }));
    }
    let preview_hash = canonical_hash(
        &json!({"discovery_id":input.discovery_id,"items":items,"rejected":rejected}),
    )?;
    Ok(Json(ApiResponse::ok(
        json!({"discovery_id":input.discovery_id,"items":items,"rejected":rejected,"preview_hash":preview_hash}),
    )))
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ToolImportsRequest {
    discovery_id: String,
    items: Vec<McpDraftToolImport>,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ResourceImportsRequest {
    discovery_id: String,
    items: Vec<McpDraftResourceRule>,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PromptImportsRequest {
    discovery_id: String,
    items: Vec<McpDraftPromptImport>,
}

async fn current_draft_typed(
    state: &McpManagementApiState,
    server_id: &str,
) -> Result<(insight_durable::McpStoredDraft, McpDraftDocument), ManagementError> {
    let stored = state
        .repository
        .get_mcp_draft(server_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let typed =
        serde_json::from_value(stored.document.clone()).map_err(|_| ManagementError::internal())?;
    Ok((stored, typed))
}

async fn usable_snapshot(
    state: &McpManagementApiState,
    server_id: &str,
    discovery_id: &str,
) -> Result<insight_durable::McpDiscoverySnapshot, ManagementError> {
    let operation = state
        .repository
        .get_mcp_discovery(server_id, discovery_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let draft = state
        .repository
        .get_mcp_draft(server_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let current_discovery_input_hash = discovery_input_hash(&draft.document, &state.policy)?;
    if operation.status != McpDiscoveryStatus::Succeeded
        || operation.stale
        || operation.discovery_input_hash != draft.discovery_input_hash
        || operation.discovery_input_hash != current_discovery_input_hash
    {
        return Err(ManagementError::new(
            StatusCode::CONFLICT,
            "MCP_DISCOVERY_STALE",
            "MCP discovery is stale",
        ));
    }
    state
        .repository
        .get_mcp_discovery_snapshot(server_id, discovery_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)
}

async fn replace_tool_imports(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(server_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<ToolImportsRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, MCP_SERVER_WRITE)?;
    let expected = parse_etag(&headers, "draft")?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    let snapshot = usable_snapshot(&state, &server_id, &input.discovery_id).await?;
    let candidates = snapshot_tool_candidates(&snapshot.document)?
        .into_iter()
        .map(|(name, (hash, _))| (name, hash))
        .collect();
    let (_, mut draft) = current_draft_typed(&state, &server_id).await?;
    draft.imports.tools = input.items;
    validate_and_sort_imports(&mut draft.imports, Some(&candidates))?;
    replace_import_document(
        &state,
        ImportMutationContext {
            principal: &principal,
            headers: &headers,
            server_id,
            expected,
            discovery_id: input.discovery_id,
            kind: "tools",
        },
        draft,
    )
    .await
}

async fn replace_resource_imports(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(server_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<ResourceImportsRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, MCP_SERVER_WRITE)?;
    let expected = parse_etag(&headers, "draft")?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    let snapshot = usable_snapshot(&state, &server_id, &input.discovery_id).await?;
    let (_, mut draft) = current_draft_typed(&state, &server_id).await?;
    draft.imports.resources.allow = input.items;
    validate_and_sort_imports(&mut draft.imports, None)?;
    let identities = snapshot_resource_identities(&snapshot.document);
    if draft.imports.resources.allow.iter().any(|rule| {
        !identities
            .iter()
            .any(|identity| wildcard_matches(&rule.uri_pattern, identity))
    }) {
        return Err(ManagementError::new(
            StatusCode::CONFLICT,
            "MCP_IMPORT_CANDIDATE_MISMATCH",
            "MCP import does not match the discovery candidate",
        ));
    }
    replace_import_document(
        &state,
        ImportMutationContext {
            principal: &principal,
            headers: &headers,
            server_id,
            expected,
            discovery_id: input.discovery_id,
            kind: "resources",
        },
        draft,
    )
    .await
}

async fn replace_prompt_imports(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(server_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<PromptImportsRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, MCP_SERVER_WRITE)?;
    let expected = parse_etag(&headers, "draft")?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    let snapshot = usable_snapshot(&state, &server_id, &input.discovery_id).await?;
    let (_, mut draft) = current_draft_typed(&state, &server_id).await?;
    draft.imports.prompts = input.items;
    validate_and_sort_imports(&mut draft.imports, None)?;
    let names = snapshot_prompt_names(&snapshot.document);
    if draft
        .imports
        .prompts
        .iter()
        .any(|prompt| !names.contains(&prompt.remote))
    {
        return Err(ManagementError::new(
            StatusCode::CONFLICT,
            "MCP_IMPORT_CANDIDATE_MISMATCH",
            "MCP import does not match the discovery candidate",
        ));
    }
    replace_import_document(
        &state,
        ImportMutationContext {
            principal: &principal,
            headers: &headers,
            server_id,
            expected,
            discovery_id: input.discovery_id,
            kind: "prompts",
        },
        draft,
    )
    .await
}

struct ImportMutationContext<'a> {
    principal: &'a OperatorPrincipal,
    headers: &'a HeaderMap,
    server_id: String,
    expected: u64,
    discovery_id: String,
    kind: &'static str,
}

async fn replace_import_document(
    state: &McpManagementApiState,
    context: ImportMutationContext<'_>,
    draft: McpDraftDocument,
) -> Result<Response, ManagementError> {
    let ImportMutationContext {
        principal,
        headers,
        server_id,
        expected,
        discovery_id,
        kind,
    } = context;
    let document = normalize_draft(draft, &state.policy)?;
    let body =
        json!({"discovery_id":discovery_id,"items":document.pointer(&format!("/imports/{kind}"))});
    let command = ReplaceMcpDraftCommand {
        metadata: metadata(
            principal,
            headers,
            "PUT",
            format!("/v1/admin/mcp/servers/{server_id}/draft/imports/{kind}"),
            &body,
        )?,
        server_id,
        expected_draft_version: expected,
        discovery_input_hash: discovery_input_hash(&document, &state.policy)?,
        draft_document: document,
    };
    receipt_response(
        state
            .repository
            .replace_mcp_draft(command)
            .await
            .map_err(ManagementError::from)?,
    )
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ValidationRequest {
    discovery_id: String,
}

fn validation_document(
    draft: &McpDraftDocument,
    snapshot: &insight_durable::McpDiscoverySnapshot,
    policy: &McpManagementPolicy,
) -> Value {
    let mut errors = Vec::<Value>::new();
    let candidates = snapshot_tool_candidates(&snapshot.document).unwrap_or_default();
    if draft.transport.is_none() {
        errors.push(json!({"code":"transport_required","path":"/transport"}));
    }
    if draft.discovery.is_none() {
        errors.push(json!({"code":"discovery_authority_required","path":"/discovery"}));
    }
    if draft.authorization.is_none() {
        errors.push(json!({"code":"authorization_required","path":"/authorization"}));
    }
    if let Some(McpDraftAuthorization::BearerSecretRef { secret_ref }) = &draft.authorization {
        if !policy.resolvable_secret_refs.contains(secret_ref) {
            errors
                .push(json!({"code":"secret_ref_unavailable","path":"/authorization/secret_ref"}));
        }
    }
    if matches!(
        draft.authorization,
        Some(McpDraftAuthorization::OauthUser { .. })
    ) && draft
        .imports
        .prompts
        .iter()
        .any(|prompt| prompt.definition_arguments.is_some())
    {
        errors.push(
            json!({"code":"prompt_snapshot_authority_unavailable","path":"/imports/prompts"}),
        );
    }
    let capabilities = snapshot
        .document
        .get("capabilities")
        .and_then(Value::as_object);
    if !draft.imports.tools.is_empty()
        && capabilities.is_none_or(|value| !value.contains_key("tools"))
    {
        errors.push(json!({"code":"tools_capability_missing","path":"/imports/tools"}));
    }
    if !draft.imports.resources.allow.is_empty()
        && capabilities.is_none_or(|value| !value.contains_key("resources"))
    {
        errors.push(json!({"code":"resources_capability_missing","path":"/imports/resources"}));
    }
    if !draft.imports.prompts.is_empty()
        && capabilities.is_none_or(|value| !value.contains_key("prompts"))
    {
        errors.push(json!({"code":"prompts_capability_missing","path":"/imports/prompts"}));
    }
    if draft
        .imports
        .tools
        .iter()
        .any(|tool| tool.tasks == "allowed")
        && snapshot
            .document
            .pointer("/capabilities/extensions")
            .and_then(Value::as_object)
            .and_then(|extensions| extensions.get(insight_mcp::MCP_TASKS_EXTENSION_ID))
            .is_none()
    {
        errors.push(json!({"code":"tasks_capability_missing","path":"/imports/tools"}));
    }
    for tool in &draft.imports.tools {
        if candidates
            .get(&tool.remote)
            .is_none_or(|(hash, _)| hash != &tool.candidate_schema_hash)
        {
            errors.push(json!({"code":"candidate_schema_mismatch","path":format!("/imports/tools/{}",tool.remote)}));
        }
    }
    let prompt_names = snapshot_prompt_names(&snapshot.document);
    for prompt in &draft.imports.prompts {
        if !prompt_names.contains(&prompt.remote) {
            errors.push(json!({"code":"prompt_candidate_missing","path":format!("/imports/prompts/{}",prompt.remote)}));
        }
    }
    let resource_identities = snapshot_resource_identities(&snapshot.document);
    for resource in &draft.imports.resources.allow {
        if !resource_identities
            .iter()
            .any(|identity| wildcard_matches(&resource.uri_pattern, identity))
        {
            errors.push(json!({"code":"resource_candidate_missing","path":format!("/imports/resources/{}",resource.uri_pattern)}));
        }
    }
    if let Some(McpDraftTransport::Stdio {
        launch_profile_id, ..
    }) = &draft.transport
    {
        if !policy
            .stdio_profile_fingerprints
            .contains_key(launch_profile_id)
        {
            errors.push(
                json!({"code":"stdio_profile_unavailable","path":"/transport/launch_profile_id"}),
            );
        }
    }
    errors.sort_by_key(|v| serde_jcs::to_string(v).unwrap_or_default());
    json!({"valid":errors.is_empty(),"errors":errors,"warnings":[]})
}

async fn create_validation(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(server_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<ValidationRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, MCP_SERVER_PUBLISH)?;
    let expected = parse_etag(&headers, "draft")?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    let (stored, draft) = current_draft_typed(&state, &server_id).await?;
    if stored.draft_version != expected {
        return Err(ManagementError::from(McpManagementWriteError::Conflict(
            McpManagementConflict::PreconditionFailed,
        )));
    }
    let snapshot = usable_snapshot(&state, &server_id, &input.discovery_id).await?;
    let document = validation_document(&draft, &snapshot, &state.policy);
    let report_hash = canonical_hash(&document)?;
    let report = McpValidationReport {
        validation_id: format!("mval_{}", Uuid::new_v4().simple()),
        server_id: server_id.clone(),
        draft_version: expected,
        discovery_id: input.discovery_id,
        report_hash,
        valid: document
            .get("valid")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        document,
        created_at: Utc::now(),
        created_by: principal.identity().to_owned(),
    };
    let body = json!({"discovery_id":report.discovery_id});
    let command = CreateMcpValidationCommand {
        metadata: metadata(
            &principal,
            &headers,
            "POST",
            format!("/v1/admin/mcp/servers/{server_id}/validations"),
            &body,
        )?,
        report,
        expected_draft_version: expected,
        expected_discovery_input_hash: stored.discovery_input_hash,
    };
    receipt_response(
        state
            .repository
            .create_mcp_validation(command)
            .await
            .map_err(ManagementError::from)?,
    )
}

async fn get_validation(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path((server_id, validation_id)): Path<(String, String)>,
) -> Result<Json<ApiResponse<Value>>, ManagementError> {
    require(&principal, MCP_SERVER_READ)?;
    let report = state
        .repository
        .get_mcp_validation(&server_id, &validation_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    Ok(Json(ApiResponse::ok(
        serde_json::to_value(report).map_err(|_| ManagementError::internal())?,
    )))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishRevisionRequest {
    draft_version: u64,
    discovery_id: String,
    validation_id: String,
}

fn generated_action_id(server_id: &str, alias: &str) -> String {
    let normalized = alias.replace('.', "_");
    let candidate = format!("mcp.{server_id}.tool_{normalized}");
    if candidate.len() <= 128 {
        return candidate;
    }
    let digest = Sha256::digest(candidate.as_bytes());
    let suffix = digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("{}_{}", &candidate[..128 - suffix.len() - 1], suffix)
}

fn revision_document(
    server_id: &str,
    draft: &McpDraftDocument,
    snapshot: &insight_durable::McpDiscoverySnapshot,
    validation: &McpValidationReport,
    definition_prompt_results: &BTreeMap<String, Value>,
    policy: &McpManagementPolicy,
) -> Result<Value, ManagementError> {
    let candidates = snapshot_tool_candidates(&snapshot.document)?;
    let mut tools = Vec::new();
    for tool in &draft.imports.tools {
        let (_, candidate) = candidates.get(&tool.remote).ok_or_else(|| {
            ManagementError::new(
                StatusCode::CONFLICT,
                "MCP_IMPORT_CANDIDATE_MISMATCH",
                "MCP import does not match the discovery candidate",
            )
        })?;
        let action_id = generated_action_id(server_id, &tool.alias);
        let binding_hash =
            canonical_hash(&json!({"action_id":action_id,"import":tool,"candidate":candidate}))?;
        tools.push(json!({"action_id":action_id,"tool_binding_hash":binding_hash,"import":tool,"candidate":candidate}));
    }
    tools.sort_by_key(|item| {
        item.get("action_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    });
    let resource_items = snapshot
        .document
        .get("resources")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|candidate| {
            candidate
                .get("uri")
                .and_then(Value::as_str)
                .is_some_and(|uri| {
                    draft
                        .imports
                        .resources
                        .allow
                        .iter()
                        .any(|rule| wildcard_matches(&rule.uri_pattern, uri))
                })
        })
        .cloned()
        .collect::<Vec<_>>();
    let resource_templates = snapshot
        .document
        .get("resource_templates")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|candidate| {
            candidate
                .get("uriTemplate")
                .or_else(|| candidate.get("uri_template"))
                .and_then(Value::as_str)
                .is_some_and(|uri| {
                    draft
                        .imports
                        .resources
                        .allow
                        .iter()
                        .any(|rule| wildcard_matches(&rule.uri_pattern, uri))
                })
        })
        .cloned()
        .collect::<Vec<_>>();
    let prompt_names = draft
        .imports
        .prompts
        .iter()
        .map(|prompt| prompt.remote.as_str())
        .collect::<BTreeSet<_>>();
    let prompt_items = snapshot
        .document
        .get("prompts")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|candidate| {
            candidate
                .get("name")
                .and_then(Value::as_str)
                .is_some_and(|name| prompt_names.contains(name))
        })
        .cloned()
        .collect::<Vec<_>>();
    let resource_catalog_fingerprint = canonical_hash(&json!({
        "resources":resource_items,
        "resource_templates":resource_templates,
    }))?;
    let prompt_catalog_fingerprint = canonical_hash(&json!(prompt_items))?;
    let mut draft_runtime = serde_json::to_value(draft).map_err(|_| ManagementError::invalid())?;
    draft_runtime
        .as_object_mut()
        .ok_or_else(ManagementError::invalid)?
        .remove("operator_note");
    let selected_protocol = snapshot
        .document
        .get("selected_protocol")
        .and_then(Value::as_str)
        .unwrap_or(&draft.protocol.preferred);
    if selected_protocol != draft.protocol.preferred
        && !draft
            .protocol
            .legacy_fallback
            .iter()
            .any(|version| version == selected_protocol)
    {
        return Err(ManagementError::invalid());
    }
    Ok(json!({
        "contract":"mcp-server-revision/v1","server_id":server_id,"draft":draft_runtime,
        "discovery":{
            "discovery_id":snapshot.discovery_id,
            "catalog_fingerprint":snapshot.catalog_fingerprint,
            "discovery_input_hash":snapshot.discovery_input_hash,
            "selected_protocol":selected_protocol,
            "capabilities":snapshot.document.get("capabilities"),
            "server_info":snapshot.document.get("server_info")
        },
        "validation":{"validation_id":validation.validation_id,"report_hash":validation.report_hash},
        "bindings":{
            "tools":tools,
            "resources":{
                "policies":draft.imports.resources.allow,
                "items":resource_items,
                "templates":resource_templates,
                "catalog_fingerprint":resource_catalog_fingerprint
            },
            "prompts":{
                "policies":draft.imports.prompts,
                "items":prompt_items,
                "definition_results":definition_prompt_results,
                "catalog_fingerprint":prompt_catalog_fingerprint
            }
        },
        "policy_fingerprint":policy.policy_fingerprint,
        "stdio_profile_fingerprint":match &draft.transport { Some(McpDraftTransport::Stdio{launch_profile_id,..}) => policy.stdio_profile_fingerprints.get(launch_profile_id), _ => None },
    }))
}

async fn publish_revision(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(server_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<PublishRevisionRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, MCP_SERVER_PUBLISH)?;
    let expected = parse_etag(&headers, "draft")?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    if expected != input.draft_version {
        return Err(ManagementError::invalid());
    }
    let body = json!({"draft_version":input.draft_version,"discovery_id":input.discovery_id,"validation_id":input.validation_id});
    let mutation_metadata = metadata(
        &principal,
        &headers,
        "POST",
        format!("/v1/admin/mcp/servers/{server_id}/revisions"),
        &body,
    )?;
    if let Some(receipt) = state
        .repository
        .replay_mcp_mutation(&mutation_metadata)
        .await
        .map_err(ManagementError::from)?
    {
        return revision_receipt_response(&server_id, receipt);
    }
    let (stored, draft) = current_draft_typed(&state, &server_id).await?;
    if stored.draft_version != expected {
        return Err(ManagementError::from(McpManagementWriteError::Conflict(
            McpManagementConflict::PreconditionFailed,
        )));
    }
    let snapshot = usable_snapshot(&state, &server_id, &input.discovery_id).await?;
    let validation = state
        .repository
        .get_mcp_validation(&server_id, &input.validation_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let current_validation = validation_document(&draft, &snapshot, &state.policy);
    if !validation.valid
        || current_validation.get("valid") != Some(&Value::Bool(true))
        || validation.report_hash != canonical_hash(&current_validation)?
    {
        return Err(ManagementError::from(McpManagementWriteError::Conflict(
            McpManagementConflict::ValidationFailed,
        )));
    }
    let definition_prompt_results = state
        .authoring
        .snapshot_definition_prompts(&server_id, &draft, &snapshot)
        .await
        .map_err(|_| {
            ManagementError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "MCP_PROMPT_AUTHORING_FAILED",
                "MCP definition Prompt snapshot failed",
            )
        })?;
    let expected_prompt_names = draft
        .imports
        .prompts
        .iter()
        .filter(|prompt| prompt.definition_arguments.is_some())
        .map(|prompt| prompt.remote.clone())
        .collect::<BTreeSet<_>>();
    if definition_prompt_results
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>()
        != expected_prompt_names
        || definition_prompt_results.values().any(|value| {
            serde_json::from_value::<insight_mcp::GetPromptResult>(value.clone()).is_err()
        })
    {
        return Err(ManagementError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "MCP_PROMPT_AUTHORING_FAILED",
            "MCP definition Prompt snapshot failed",
        ));
    }
    let document = revision_document(
        &server_id,
        &draft,
        &snapshot,
        &validation,
        &definition_prompt_results,
        &state.policy,
    )?;
    let revision_hash = canonical_hash(&document)?;
    let imported_tool_count = document
        .pointer("/bindings/tools")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    let imported_resource_count = document
        .pointer("/bindings/resources/items")
        .and_then(Value::as_array)
        .map_or(0, Vec::len)
        .saturating_add(
            document
                .pointer("/bindings/resources/templates")
                .and_then(Value::as_array)
                .map_or(0, Vec::len),
        );
    let imported_prompt_count = document
        .pointer("/bindings/prompts/items")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    let revision_id = format!("mrev_{}", Uuid::new_v4().simple());
    let command = PublishMcpRevisionCommand {
        metadata: mutation_metadata,
        revision_id,
        server_id: server_id.clone(),
        expected_draft_version: expected,
        discovery_id: input.discovery_id,
        validation_id: input.validation_id,
        revision_hash,
        document,
    };
    let receipt = state
        .repository
        .publish_mcp_revision(command)
        .await
        .map_err(ManagementError::from)?;
    if !receipt.replayed {
        insight_mcp::record_management_catalog_count(
            "tool",
            "imported",
            u64::try_from(imported_tool_count).unwrap_or(u64::MAX),
        );
        insight_mcp::record_management_catalog_count(
            "resource",
            "imported",
            u64::try_from(imported_resource_count).unwrap_or(u64::MAX),
        );
        insight_mcp::record_management_catalog_count(
            "prompt",
            "imported",
            u64::try_from(imported_prompt_count).unwrap_or(u64::MAX),
        );
    }
    revision_receipt_response(&server_id, receipt)
}

async fn list_revisions(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(server_id): Path<String>,
    query: Result<Query<PageQuery>, QueryRejection>,
) -> Result<Json<ApiResponse<Value>>, ManagementError> {
    require(&principal, MCP_SERVER_READ)?;
    let Query(query) = query.map_err(|_| ManagementError::invalid())?;
    let cursor = opaque_decode(query.cursor.as_deref(), &state.policy.cursor_signing_key)?;
    let mut page = state
        .repository
        .list_mcp_revisions(&server_id, cursor.as_deref(), page_limit(query.limit)?)
        .await
        .map_err(|_| ManagementError::internal())?;
    page.next_cursor = page
        .next_cursor
        .map(|v| opaque_encode(&v, &state.policy.cursor_signing_key));
    Ok(Json(ApiResponse::ok(
        serde_json::to_value(page).map_err(|_| ManagementError::internal())?,
    )))
}

async fn get_revision(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path((server_id, revision_id)): Path<(String, String)>,
) -> Result<Json<ApiResponse<Value>>, ManagementError> {
    require(&principal, MCP_SERVER_READ)?;
    let revision = state
        .repository
        .get_mcp_revision(&server_id, &revision_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    Ok(Json(ApiResponse::ok(
        serde_json::to_value(revision).map_err(|_| ManagementError::internal())?,
    )))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivateRevisionRequest {
    revision_id: String,
}

async fn activate_revision(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(server_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<ActivateRevisionRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, MCP_SERVER_PUBLISH)?;
    let expected = parse_etag(&headers, "server")?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    let body = json!({"revision_id":input.revision_id});
    let mutation_metadata = metadata(
        &principal,
        &headers,
        "PUT",
        format!("/v1/admin/mcp/servers/{server_id}/active-revision"),
        &body,
    )?;
    if let Some(receipt) = state
        .repository
        .replay_mcp_mutation(&mutation_metadata)
        .await
        .map_err(ManagementError::from)?
    {
        return receipt_response(receipt);
    }
    let revision = state
        .repository
        .get_mcp_revision(&server_id, &input.revision_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    if !revision_matches_policy(&revision, &state.policy) {
        return Err(ManagementError::new(
            StatusCode::CONFLICT,
            "MCP_SERVER_STATE_CONFLICT",
            "MCP Server policy changed after publication",
        ));
    }
    let readiness_hash = state.readiness.probe(&revision).await.map_err(|_| {
        ManagementError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "MCP_SERVER_READINESS_FAILED",
            "MCP Server readiness failed",
        )
    })?;
    let now = Utc::now();
    let command = ActivateMcpRevisionCommand {
        metadata: mutation_metadata,
        server_id,
        revision_id: input.revision_id,
        expected_server_version: expected,
        readiness_hash,
        readiness_expires_at: now + chrono::Duration::seconds(30),
    };
    receipt_response(
        state
            .repository
            .activate_mcp_revision(command)
            .await
            .map_err(ManagementError::from)?,
    )
}

async fn disable_server(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(server_id): Path<String>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, ManagementError> {
    require(&principal, MCP_SERVER_PUBLISH)?;
    if !body.is_empty() {
        return Err(ManagementError::invalid());
    }
    let expected = parse_etag(&headers, "server")?;
    let body = json!({});
    let command = DisableMcpServerCommand {
        metadata: metadata(
            &principal,
            &headers,
            "DELETE",
            format!("/v1/admin/mcp/servers/{server_id}/active-revision"),
            &body,
        )?,
        server_id,
        expected_server_version: expected,
    };
    receipt_response(
        state
            .repository
            .disable_mcp_server(command)
            .await
            .map_err(ManagementError::from)?,
    )
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetirementRequest {
    reason_code: String,
}

async fn retire_server(
    State(state): State<Arc<McpManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(server_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<RetirementRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, MCP_SERVER_PUBLISH)?;
    let expected = parse_etag(&headers, "server")?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    if !valid_name(&input.reason_code) {
        return Err(ManagementError::invalid());
    }
    let body = json!({"reason_code":input.reason_code});
    let command = RetireMcpServerCommand {
        metadata: metadata(
            &principal,
            &headers,
            "POST",
            format!("/v1/admin/mcp/servers/{server_id}/retirement"),
            &body,
        )?,
        server_id,
        expected_server_version: expected,
        reason_code: input.reason_code,
    };
    receipt_response(
        state
            .repository
            .retire_mcp_server(command)
            .await
            .map_err(ManagementError::from)?,
    )
}

#[cfg(test)]
mod contract_tests {
    use super::*;

    const MANAGEMENT_SCHEMA: &str = workspace_asset_str!("schemas/mcp-management-v1.json");
    const MANAGEMENT_SAMPLES: &str = workspace_asset_str!("schemas/mcp-management-v1.samples.json");
    const MANAGEMENT_OPENAPI: &str = workspace_asset_str!("schemas/mcp-management-v1.openapi.json");

    fn policy(fingerprint: &str) -> McpManagementPolicy {
        McpManagementPolicy {
            preferred_protocol: "2026-07-28".to_owned(),
            legacy_protocols: BTreeSet::new(),
            allowed_secret_refs: BTreeSet::new(),
            resolvable_secret_refs: BTreeSet::new(),
            stdio_profile_fingerprints: BTreeMap::new(),
            stdio_profile_allowed_parameters: BTreeMap::new(),
            allow_loopback_development: false,
            trusted_manifest_signers: BTreeMap::new(),
            manifest_max_validity: Duration::from_secs(3600),
            max_pending_discoveries: 8,
            max_request_bytes: 1024,
            max_response_bytes: 4096,
            max_sse_line_bytes: 512,
            max_sse_event_bytes: 1024,
            max_content_items: 16,
            max_catalog_items: 32,
            policy_fingerprint: sha256(fingerprint.as_bytes()),
            cursor_signing_key: [3; 32],
        }
    }

    #[test]
    fn public_schema_and_samples_are_closed_without_auto_import() {
        let schema: Value = serde_json::from_str(MANAGEMENT_SCHEMA).unwrap();
        let validator = jsonschema::Validator::new(&schema).unwrap();
        let samples: Value = serde_json::from_str(MANAGEMENT_SAMPLES).unwrap();
        for sample in samples["valid"].as_array().unwrap() {
            assert!(
                validator.is_valid(sample),
                "valid sample rejected: {sample}"
            );
        }
        for sample in samples["invalid"].as_array().unwrap() {
            assert!(
                !validator.is_valid(sample),
                "invalid sample accepted: {sample}"
            );
        }
        assert!(!validator.is_valid(&json!({
            "server_id":"engineering","display_name":"Engineering","auto_import":true
        })));
        assert!(!validator.is_valid(&json!({"discovery_id":"mdisc_one","items":"*"})));
    }

    #[test]
    fn openapi_covers_all_routes_and_keeps_delete_body_free() {
        let openapi: Value = serde_json::from_str(MANAGEMENT_OPENAPI).unwrap();
        assert_eq!(openapi["openapi"], "3.1.0");
        let paths = openapi["paths"].as_object().unwrap();
        let operations = paths
            .values()
            .flat_map(|item| item.as_object().unwrap().keys())
            .filter(|method| *method != "parameters")
            .count();
        assert_eq!(operations, 28);
        for item in paths.values() {
            if let Some(delete) = item.get("delete") {
                assert!(delete.get("requestBody").is_none());
            }
        }
    }

    #[test]
    fn discovery_evidence_tracks_policy_but_not_import_review_edits() {
        let mut current_policy = policy("policy-one");
        let mut draft = json!({
            "transport":{"type":"streamable_http","endpoint":"https://mcp.example.test/mcp"},
            "discovery":{"type":"live_service_account"},
            "authorization":{"type":"none"},
            "protocol":{"preferred":"2026-07-28","legacy_fallback":[]},
            "imports":{"tools":[],"resources":{"allow":[]},"prompts":[]},
            "limits":{}
        });
        let original = discovery_input_hash(&draft, &current_policy).unwrap();
        draft["imports"]["tools"] = json!([{"reviewed":true}]);
        assert_eq!(
            discovery_input_hash(&draft, &current_policy).unwrap(),
            original
        );
        current_policy.policy_fingerprint = sha256(b"policy-two");
        assert_ne!(
            discovery_input_hash(&draft, &current_policy).unwrap(),
            original
        );

        let operation = insight_durable::McpDiscoveryOperation {
            discovery_id: "mdisc_policy".to_owned(),
            server_id: "engineering".to_owned(),
            source_draft_version: 1,
            discovery_input_hash: original,
            status: McpDiscoveryStatus::Succeeded,
            cancel_requested: false,
            attempts: 1,
            claimed_by: None,
            claim_token: None,
            claim_expires_at: None,
            failure: None,
            stale: false,
            stale_reason: None,
            created_at: Utc::now(),
            started_at: Some(Utc::now()),
            finished_at: Some(Utc::now()),
        };
        let projection = discovery_operation_projection(
            &operation,
            &discovery_input_hash(&draft, &current_policy).unwrap(),
        );
        assert_eq!(projection["stale"], false);
        assert_eq!(projection["effective_stale"], true);
        assert_eq!(
            projection["effective_stale_reason"],
            "platform_policy_changed"
        );
    }
}
