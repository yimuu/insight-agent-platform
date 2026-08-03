//! Installation-scoped Provider management HTTP API.
//!
//! Provider Drafts never contain secret values. Remote discovery and
//! connection-test I/O are queued as durable operations and are performed by
//! leased workers outside request and database transactions.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    sync::Arc,
};

use async_trait::async_trait;
use axum::{
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
use chrono::Utc;
use insight_durable::{
    ActivateProviderRevisionCommand, CancelProviderOperationCommand, CreateProviderCommand,
    CreateProviderConnectionTestCommand, CreateProviderDiscoveryCommand,
    CreateProviderValidationCommand, DeactivateProviderRevisionCommand, DeleteProviderCommand,
    ProviderConnectionTestMode, ProviderManagementConflict, ProviderManagementDurableRepository,
    ProviderManagementWriteError, ProviderMutationMetadata, ProviderMutationReceipt,
    ProviderOperationalState, ProviderRevision, ProviderValidationReport,
    PublishProviderRevisionCommand, RecordProviderManagementRejectionCommand,
    ReplaceProviderDraftCommand, ResumeProviderCommand, RetireProviderCommand,
    SuspendProviderCommand, PROVIDER_MANAGEMENT_MAX_REQUEST_ID_BYTES,
};
use ring::hmac;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{
    auth::{
        OperatorAuth, OperatorPrincipal, PROVIDER_ACTIVATE, PROVIDER_DISCOVER, PROVIDER_PUBLISH,
        PROVIDER_READ, PROVIDER_RETIRE, PROVIDER_SUSPEND, PROVIDER_TEST, PROVIDER_WRITE,
    },
    response::ApiResponse,
};

const MAX_MANAGEMENT_BODY_BYTES: usize = 1024 * 1024;
const DEFAULT_PAGE_SIZE: u32 = 50;
const MAX_PAGE_SIZE: u32 = 200;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderTemplate {
    pub template_id: String,
    pub adapter_type: String,
    pub adapter_version: String,
    pub manifest_digest: String,
    pub document: Value,
}

#[derive(Clone)]
pub struct ProviderManagementPolicy {
    pub installed_adapters: BTreeMap<String, String>,
    pub adapter_worker_versions: BTreeMap<String, String>,
    pub adapter_manifest_digests: BTreeMap<String, String>,
    pub allowed_secret_refs: BTreeSet<String>,
    pub resolvable_secret_refs: BTreeSet<String>,
    pub templates: Vec<ProviderTemplate>,
    pub allow_loopback_development: bool,
    pub max_models: usize,
    pub max_connect_timeout_ms: u64,
    pub max_request_timeout_ms: u64,
    pub max_pending_operations: u32,
    pub require_connection_test_for_publish: bool,
    pub policy_fingerprint: String,
    pub cursor_signing_key: [u8; 32],
}

impl std::fmt::Debug for ProviderManagementPolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderManagementPolicy")
            .field("installed_adapters", &self.installed_adapters)
            .field("adapter_worker_versions", &self.adapter_worker_versions)
            .field("adapter_manifest_digests", &self.adapter_manifest_digests)
            .field("templates", &self.templates)
            .field(
                "allow_loopback_development",
                &self.allow_loopback_development,
            )
            .field("max_models", &self.max_models)
            .field("policy_fingerprint", &self.policy_fingerprint)
            .field("cursor_signing_key", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct ProviderManagementApiState {
    pub auth: OperatorAuth,
    pub repository: Arc<dyn ProviderManagementDurableRepository>,
    pub policy: Arc<ProviderManagementPolicy>,
    pub readiness: Arc<dyn ProviderRevisionReadiness>,
}

#[async_trait]
pub trait ProviderRevisionReadiness: Send + Sync {
    async fn probe(&self, revision: &ProviderRevision) -> Result<String, ()>;
}

#[derive(Debug, Default)]
pub struct LocalProviderRevisionReadiness;

#[async_trait]
impl ProviderRevisionReadiness for LocalProviderRevisionReadiness {
    async fn probe(&self, revision: &ProviderRevision) -> Result<String, ()> {
        canonical_hash(&json!({
            "revision_id": revision.revision_id,
            "revision_hash": revision.revision_hash,
        }))
        .map_err(|_| ())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDraftDocument {
    pub adapter: ProviderDraftAdapter,
    pub endpoint: String,
    pub credential: ProviderDraftCredential,
    #[serde(default)]
    pub transport: ProviderDraftTransport,
    #[serde(default)]
    pub models: Vec<ProviderDraftModel>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator_note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDraftAdapter {
    #[serde(rename = "type")]
    pub adapter_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProviderDraftCredential {
    Bearer { reference: String },
    ApiKey { reference: String },
    None,
}

impl ProviderDraftCredential {
    fn reference(&self) -> Option<&str> {
        match self {
            Self::Bearer { reference } | Self::ApiKey { reference } => Some(reference),
            Self::None => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDraftTransport {
    #[serde(default = "default_tls")]
    pub tls: String,
    #[serde(default = "default_redirects")]
    pub redirects: String,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_ms: u64,
    #[serde(default = "default_request_timeout")]
    pub request_timeout_ms: u64,
}

impl Default for ProviderDraftTransport {
    fn default() -> Self {
        Self {
            tls: default_tls(),
            redirects: default_redirects(),
            connect_timeout_ms: default_connect_timeout(),
            request_timeout_ms: default_request_timeout(),
        }
    }
}

fn default_tls() -> String {
    "required".to_owned()
}
fn default_redirects() -> String {
    "deny".to_owned()
}
const fn default_connect_timeout() -> u64 {
    5_000
}
const fn default_request_timeout() -> u64 {
    120_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDraftModel {
    pub id: String,
    #[serde(default)]
    pub input: Vec<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    pub provenance: ProviderCapabilityProvenance,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderCapabilityProvenance {
    #[serde(rename = "type")]
    pub provenance_type: String,
}

#[derive(Debug)]
struct ManagementError {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
}

impl ManagementError {
    fn new(status: StatusCode, code: &'static str, message: &'static str) -> Self {
        Self {
            status,
            code,
            message,
        }
    }
    fn invalid() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "PROVIDER_MANAGEMENT_INVALID_REQUEST",
            "Provider management request is invalid",
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
            "PROVIDER_MANAGEMENT_FORBIDDEN",
            "Operator capability is required",
        )
    }
    fn not_found() -> Self {
        Self::new(
            StatusCode::NOT_FOUND,
            "PROVIDER_NOT_FOUND",
            "Provider management object was not found",
        )
    }
    fn precondition_required() -> Self {
        Self::new(
            StatusCode::PRECONDITION_REQUIRED,
            "PROVIDER_MANAGEMENT_PRECONDITION_REQUIRED",
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
            Json(json!({"code":self.code,"message":self.message,"data":null})),
        ))
    }
}

impl From<ProviderManagementWriteError> for ManagementError {
    fn from(error: ProviderManagementWriteError) -> Self {
        let ProviderManagementWriteError::Conflict(conflict) = error else {
            return Self::internal();
        };
        match conflict {
            ProviderManagementConflict::NotFound => Self::not_found(),
            ProviderManagementConflict::ForbiddenState | ProviderManagementConflict::Referenced => {
                Self::new(
                    StatusCode::CONFLICT,
                    "PROVIDER_STATE_CONFLICT",
                    "Provider state does not allow this operation",
                )
            }
            ProviderManagementConflict::PreconditionFailed
            | ProviderManagementConflict::FenceLost => Self::new(
                StatusCode::PRECONDITION_FAILED,
                "PROVIDER_MANAGEMENT_PRECONDITION_FAILED",
                "Provider management precondition failed",
            ),
            ProviderManagementConflict::IdempotencyKeyReused => Self::new(
                StatusCode::CONFLICT,
                "PROVIDER_IDEMPOTENCY_KEY_REUSED",
                "request ID was already used for a different request",
            ),
            ProviderManagementConflict::OperationStale => Self::new(
                StatusCode::CONFLICT,
                "PROVIDER_OPERATION_STALE",
                "Provider operation evidence is stale",
            ),
            ProviderManagementConflict::CandidateMismatch => Self::new(
                StatusCode::CONFLICT,
                "PROVIDER_MODEL_CANDIDATE_MISMATCH",
                "Provider model import does not match discovery evidence",
            ),
            ProviderManagementConflict::ValidationFailed => Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "PROVIDER_VALIDATION_FAILED",
                "Provider validation failed",
            ),
            ProviderManagementConflict::CapacityExceeded => Self::new(
                StatusCode::TOO_MANY_REQUESTS,
                "PROVIDER_OPERATION_CAPACITY_EXCEEDED",
                "Provider operation capacity was exceeded",
            ),
        }
    }
}

pub fn build_provider_management_router(state: ProviderManagementApiState) -> Router {
    let auth = state.auth.clone();
    let audit_repository = Arc::clone(&state.repository);
    Router::new()
        .route("/v1/admin/provider-templates", get(list_templates))
        .route(
            "/v1/admin/providers",
            get(list_providers).post(create_provider),
        )
        .route(
            "/v1/admin/providers/{provider_id}",
            get(get_provider).delete(delete_provider),
        )
        .route(
            "/v1/admin/providers/{provider_id}/draft",
            get(get_draft).put(replace_draft),
        )
        .route(
            "/v1/admin/providers/{provider_id}/discoveries",
            post(create_discovery),
        )
        .route(
            "/v1/admin/providers/{provider_id}/discoveries/{discovery_id}",
            get(get_discovery).delete(cancel_discovery),
        )
        .route(
            "/v1/admin/providers/{provider_id}/discoveries/{discovery_id}/model-candidates",
            get(list_model_candidates),
        )
        .route(
            "/v1/admin/providers/{provider_id}/model-import-previews",
            post(preview_model_imports),
        )
        .route(
            "/v1/admin/providers/{provider_id}/draft/models",
            axum::routing::put(replace_models),
        )
        .route(
            "/v1/admin/providers/{provider_id}/validations",
            post(create_validation),
        )
        .route(
            "/v1/admin/providers/{provider_id}/validations/{validation_id}",
            get(get_validation),
        )
        .route(
            "/v1/admin/providers/{provider_id}/connection-tests",
            post(create_connection_test),
        )
        .route(
            "/v1/admin/providers/{provider_id}/connection-tests/{test_id}",
            get(get_connection_test).delete(cancel_connection_test),
        )
        .route(
            "/v1/admin/providers/{provider_id}/revisions",
            get(list_revisions).post(publish_revision),
        )
        .route(
            "/v1/admin/providers/{provider_id}/revisions/{revision_id}",
            get(get_revision),
        )
        .route(
            "/v1/admin/providers/{provider_id}/active-revision",
            get(get_active_revision)
                .put(activate_revision)
                .delete(deactivate_revision),
        )
        .route(
            "/v1/admin/providers/{provider_id}/suspension",
            post(suspend_provider).delete(resume_provider),
        )
        .route(
            "/v1/admin/providers/{provider_id}/retirement",
            post(retire_provider),
        )
        .route_layer(DefaultBodyLimit::max(MAX_MANAGEMENT_BODY_BYTES))
        .route_layer(middleware::from_fn(
            move |headers: HeaderMap,
                  request: axum::http::Request<axum::body::Body>,
                  next: Next| {
                let auth = auth.clone();
                let audit_repository = Arc::clone(&audit_repository);
                async move {
                    let method = request.method().clone();
                    let path = request.uri().path().to_owned();
                    let provider_id = management_provider_id(&path);
                    let request_id = headers
                        .get("x-request-id")
                        .and_then(|value| value.to_str().ok())
                        .filter(|value| value.len() <= PROVIDER_MANAGEMENT_MAX_REQUEST_ID_BYTES)
                        .unwrap_or("missing")
                        .to_owned();
                    let principal = auth.resolve(&headers);
                    let mut response = match principal.clone() {
                        Some(principal) => {
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
                        None => ManagementError::unauthorized().into_response(),
                    };
                    if !response.status().is_success() {
                        if let Some(principal) = principal {
                            let (capability, subject_id) = provider_rejection_class(&method, &path);
                            let _ = audit_repository
                                .record_provider_management_rejection(
                                    RecordProviderManagementRejectionCommand {
                                        actor_id: principal.identity().to_owned(),
                                        capability: capability.to_owned(),
                                        request_id,
                                        provider_id,
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

fn management_provider_id(path: &str) -> Option<String> {
    path.strip_prefix("/v1/admin/providers/")
        .and_then(|suffix| suffix.split('/').next())
        .filter(|value| !value.is_empty() && value.len() <= 256)
        .map(str::to_owned)
}

fn provider_rejection_class<'a>(method: &axum::http::Method, path: &str) -> (&'a str, &'a str) {
    if method == axum::http::Method::GET {
        return (PROVIDER_READ, "provider.read");
    }
    if path.contains("/discoveries")
        || path.contains("/model-import-previews")
        || path.contains("/draft/models")
    {
        return (PROVIDER_DISCOVER, "provider.discovery");
    }
    if path.contains("/connection-tests") {
        return (PROVIDER_TEST, "provider.connection_test");
    }
    if path.contains("/validations") || path.ends_with("/revisions") {
        return (PROVIDER_PUBLISH, "provider.revision");
    }
    if path.contains("/active-revision") {
        return (PROVIDER_ACTIVATE, "provider.active_revision");
    }
    if path.contains("/suspension") {
        return (PROVIDER_SUSPEND, "provider.suspension");
    }
    if path.contains("/retirement") {
        return (PROVIDER_RETIRE, "provider.retirement");
    }
    (PROVIDER_WRITE, "provider.write")
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

fn valid_provider_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && !value.starts_with('-')
        && !value.ends_with('-')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn valid_text(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && !value
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
}

fn valid_identity(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && !value.chars().any(char::is_control)
        && value != "*"
        && !value.contains('*')
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

fn metadata(
    principal: &OperatorPrincipal,
    capability: &str,
    headers: &HeaderMap,
    method: &str,
    path: String,
    body: &Value,
) -> Result<ProviderMutationMetadata, ManagementError> {
    let request_id = headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| {
            !value.is_empty()
                && value.len() <= PROVIDER_MANAGEMENT_MAX_REQUEST_ID_BYTES
                && !value.chars().any(char::is_control)
        })
        .ok_or_else(ManagementError::invalid)?;
    Ok(ProviderMutationMetadata {
        operator_id: principal.identity().to_owned(),
        capability: capability.to_owned(),
        method: method.to_owned(),
        canonical_path: path,
        request_id: request_id.to_owned(),
        request_hash: canonical_hash(body)?,
        now: Utc::now(),
    })
}

fn receipt_response(receipt: ProviderMutationReceipt) -> Result<Response, ManagementError> {
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

fn receipt_location_response(
    receipt: ProviderMutationReceipt,
    location: String,
) -> Result<Response, ManagementError> {
    let mut response = receipt_response(receipt)?;
    response.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(&location).map_err(|_| ManagementError::internal())?,
    );
    Ok(response)
}

fn page_limit(limit: Option<u32>) -> Result<u32, ManagementError> {
    match limit.unwrap_or(DEFAULT_PAGE_SIZE) {
        1..=MAX_PAGE_SIZE => Ok(limit.unwrap_or(DEFAULT_PAGE_SIZE)),
        _ => Err(ManagementError::invalid()),
    }
}

fn normalize_draft(
    mut draft: ProviderDraftDocument,
    policy: &ProviderManagementPolicy,
) -> Result<ProviderDraftDocument, ManagementError> {
    if !policy
        .installed_adapters
        .contains_key(&draft.adapter.adapter_type)
        || !matches!(draft.transport.tls.as_str(), "required")
        || !matches!(draft.transport.redirects.as_str(), "deny")
        || draft.transport.connect_timeout_ms == 0
        || draft.transport.connect_timeout_ms > policy.max_connect_timeout_ms
        || draft.transport.request_timeout_ms == 0
        || draft.transport.request_timeout_ms > policy.max_request_timeout_ms
        || draft.models.len() > policy.max_models
        || draft
            .description
            .as_ref()
            .is_some_and(|value| !valid_text(value, 4096))
        || draft
            .operator_note
            .as_ref()
            .is_some_and(|value| !valid_text(value, 4096))
    {
        return Err(ManagementError::invalid());
    }
    let endpoint = reqwest::Url::parse(&draft.endpoint).map_err(|_| ManagementError::invalid())?;
    let loopback = endpoint
        .host_str()
        .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "::1"));
    let transport_policy = if policy.allow_loopback_development && loopback {
        insight_resources::openai_chat::OpenAiTransportPolicy::AllowLoopbackHttp
    } else {
        insight_resources::openai_chat::OpenAiTransportPolicy::HttpsOnly
    };
    if endpoint.fragment().is_some()
        || insight_resources::openai_chat::validate_endpoint_transport(
            &endpoint,
            &draft.endpoint,
            transport_policy,
        )
        .is_err()
        || (loopback && draft.credential.reference().is_some())
    {
        return Err(ManagementError::invalid());
    }
    draft.endpoint = endpoint.to_string().trim_end_matches('/').to_owned();
    if draft.credential.reference().is_some_and(|reference| {
        !reference.starts_with("secret://") || !policy.allowed_secret_refs.contains(reference)
    }) {
        return Err(ManagementError::invalid());
    }
    let mut ids = BTreeSet::new();
    for model in &mut draft.models {
        model.input.sort();
        model.input.dedup();
        model.capabilities.sort();
        model.capabilities.dedup();
        if !valid_identity(&model.id, 512)
            || !ids.insert(model.id.clone())
            || model.input.is_empty()
            || model.input.len() > 8
            || model.capabilities.len() > 32
            || model
                .input
                .iter()
                .any(|value| !matches!(value.as_str(), "text" | "image" | "audio"))
            || model.capabilities.iter().any(|value| {
                !matches!(
                    value.as_str(),
                    "complete" | "streaming" | "tools" | "structured_output" | "reasoning"
                )
            })
            || !matches!(
                model.provenance.provenance_type.as_str(),
                "template_verified" | "adapter_verified" | "probe_verified" | "operator_asserted"
            )
        {
            return Err(ManagementError::invalid());
        }
    }
    draft.models.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(draft)
}

fn provider_input_hash(
    draft: &ProviderDraftDocument,
    policy: &ProviderManagementPolicy,
) -> Result<String, ManagementError> {
    canonical_hash(&json!({"draft":draft,"policy_fingerprint":policy.policy_fingerprint}))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderPageQuery {
    state: Option<ProviderOperationalState>,
    cursor: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorPageQuery {
    cursor: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateProviderRequest {
    provider_id: String,
    display_name: String,
    adapter_type: String,
    draft: ProviderDraftDocument,
}

async fn list_templates(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
) -> Result<Json<ApiResponse<Value>>, ManagementError> {
    require(&principal, PROVIDER_READ)?;
    Ok(Json(ApiResponse::ok(
        json!({"items":state.policy.templates}),
    )))
}

async fn create_provider(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    headers: HeaderMap,
    input: Result<Json<CreateProviderRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, PROVIDER_WRITE)?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    if !valid_provider_id(&input.provider_id)
        || !valid_text(&input.display_name, 256)
        || input.adapter_type != input.draft.adapter.adapter_type
    {
        return Err(ManagementError::invalid());
    }
    let draft = normalize_draft(input.draft, &state.policy)?;
    let body = json!({"provider_id":input.provider_id,"display_name":input.display_name,"adapter_type":input.adapter_type,"draft":draft});
    let command = CreateProviderCommand {
        metadata: metadata(
            &principal,
            PROVIDER_WRITE,
            &headers,
            "POST",
            "/v1/admin/providers".to_owned(),
            &body,
        )?,
        provider_id: input.provider_id,
        display_name: input.display_name,
        adapter_type: input.adapter_type,
        provider_input_hash: provider_input_hash(&draft, &state.policy)?,
        draft_document: serde_json::to_value(draft).map_err(|_| ManagementError::invalid())?,
    };
    let receipt = state
        .repository
        .create_provider(command)
        .await
        .map_err(ManagementError::from)?;
    let provider_id = receipt
        .response
        .get("provider_id")
        .and_then(Value::as_str)
        .ok_or_else(ManagementError::internal)?;
    receipt_location_response(
        receipt.clone(),
        format!("/v1/admin/providers/{provider_id}"),
    )
}

async fn list_providers(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    query: Result<Query<ProviderPageQuery>, QueryRejection>,
) -> Result<Json<ApiResponse<Value>>, ManagementError> {
    require(&principal, PROVIDER_READ)?;
    let Query(query) = query.map_err(|_| ManagementError::invalid())?;
    let cursor = opaque_decode(query.cursor.as_deref(), &state.policy.cursor_signing_key)?;
    let mut page = state
        .repository
        .list_providers(query.state, cursor.as_deref(), page_limit(query.limit)?)
        .await
        .map_err(|_| ManagementError::internal())?;
    page.next_cursor = page
        .next_cursor
        .map(|value| opaque_encode(&value, &state.policy.cursor_signing_key));
    Ok(Json(ApiResponse::ok(
        serde_json::to_value(page).map_err(|_| ManagementError::internal())?,
    )))
}

async fn get_provider(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(provider_id): Path<String>,
) -> Result<Response, ManagementError> {
    require(&principal, PROVIDER_READ)?;
    let provider = state
        .repository
        .get_provider(&provider_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let mut response = private_no_store(Json(ApiResponse::ok(
        serde_json::to_value(&provider).map_err(|_| ManagementError::internal())?,
    )));
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"provider-{}\"", provider.provider_version))
            .map_err(|_| ManagementError::internal())?,
    );
    Ok(response)
}

async fn get_draft(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(provider_id): Path<String>,
) -> Result<Response, ManagementError> {
    require(&principal, PROVIDER_READ)?;
    let draft = state
        .repository
        .get_provider_draft(&provider_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let mut response = private_no_store(Json(ApiResponse::ok(
        serde_json::to_value(&draft).map_err(|_| ManagementError::internal())?,
    )));
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"draft-{}\"", draft.draft_version))
            .map_err(|_| ManagementError::internal())?,
    );
    Ok(response)
}

async fn replace_draft(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(provider_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<ProviderDraftDocument>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, PROVIDER_WRITE)?;
    let expected = parse_etag(&headers, "draft")?;
    let draft = normalize_draft(
        input.map_err(|_| ManagementError::invalid())?.0,
        &state.policy,
    )?;
    let provider = state
        .repository
        .get_provider(&provider_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    if provider.adapter_type != draft.adapter.adapter_type {
        return Err(ManagementError::invalid());
    }
    let body = serde_json::to_value(&draft).map_err(|_| ManagementError::invalid())?;
    receipt_response(
        state
            .repository
            .replace_provider_draft(ReplaceProviderDraftCommand {
                metadata: metadata(
                    &principal,
                    PROVIDER_WRITE,
                    &headers,
                    "PUT",
                    format!("/v1/admin/providers/{provider_id}/draft"),
                    &body,
                )?,
                provider_id,
                expected_draft_version: expected,
                provider_input_hash: provider_input_hash(&draft, &state.policy)?,
                draft_document: body,
            })
            .await
            .map_err(ManagementError::from)?,
    )
}

async fn delete_provider(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(provider_id): Path<String>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, ManagementError> {
    require(&principal, PROVIDER_WRITE)?;
    if !body.is_empty() {
        return Err(ManagementError::invalid());
    }
    let expected = parse_etag(&headers, "provider")?;
    receipt_response(
        state
            .repository
            .delete_provider(DeleteProviderCommand {
                metadata: metadata(
                    &principal,
                    PROVIDER_WRITE,
                    &headers,
                    "DELETE",
                    format!("/v1/admin/providers/{provider_id}"),
                    &json!({}),
                )?,
                provider_id,
                expected_provider_version: expected,
            })
            .await
            .map_err(ManagementError::from)?,
    )
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateDiscoveryRequest {
    draft_version: u64,
}

async fn create_discovery(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(provider_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<CreateDiscoveryRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, PROVIDER_DISCOVER)?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    let draft = state
        .repository
        .get_provider_draft(&provider_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    if draft.draft_version != input.draft_version {
        return Err(ManagementError::new(
            StatusCode::PRECONDITION_FAILED,
            "PROVIDER_MANAGEMENT_PRECONDITION_FAILED",
            "Provider Draft version changed",
        ));
    }
    let discovery_id = format!("pdisc_{}", Uuid::new_v4().simple());
    let body = json!({"draft_version":input.draft_version});
    let receipt = state
        .repository
        .create_provider_discovery(CreateProviderDiscoveryCommand {
            metadata: metadata(
                &principal,
                PROVIDER_DISCOVER,
                &headers,
                "POST",
                format!("/v1/admin/providers/{provider_id}/discoveries"),
                &body,
            )?,
            discovery_id,
            provider_id: provider_id.clone(),
            expected_draft_version: input.draft_version,
            provider_input_hash: draft.provider_input_hash,
            max_pending_operations: state.policy.max_pending_operations,
        })
        .await
        .map_err(ManagementError::from)?;
    let discovery_id = receipt
        .response
        .get("discovery_id")
        .and_then(Value::as_str)
        .ok_or_else(ManagementError::internal)?;
    receipt_location_response(
        receipt.clone(),
        format!("/v1/admin/providers/{provider_id}/discoveries/{discovery_id}"),
    )
}

async fn get_discovery(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path((provider_id, discovery_id)): Path<(String, String)>,
) -> Result<Json<ApiResponse<Value>>, ManagementError> {
    require(&principal, PROVIDER_READ)?;
    let operation = state
        .repository
        .get_provider_discovery(&provider_id, &discovery_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let snapshot = state
        .repository
        .get_provider_discovery_snapshot(&provider_id, &discovery_id)
        .await
        .map_err(|_| ManagementError::internal())?;
    Ok(Json(ApiResponse::ok(
        json!({"operation":operation,"snapshot":snapshot}),
    )))
}

async fn cancel_discovery(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path((provider_id, discovery_id)): Path<(String, String)>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, ManagementError> {
    require(&principal, PROVIDER_DISCOVER)?;
    if !body.is_empty() {
        return Err(ManagementError::invalid());
    }
    receipt_response(
        state
            .repository
            .cancel_provider_discovery(CancelProviderOperationCommand {
                metadata: metadata(
                    &principal,
                    PROVIDER_DISCOVER,
                    &headers,
                    "DELETE",
                    format!("/v1/admin/providers/{provider_id}/discoveries/{discovery_id}"),
                    &json!({}),
                )?,
                provider_id: provider_id.clone(),
                operation_id: discovery_id,
            })
            .await
            .map_err(ManagementError::from)?,
    )
}

async fn list_model_candidates(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path((provider_id, discovery_id)): Path<(String, String)>,
    query: Result<Query<CursorPageQuery>, QueryRejection>,
) -> Result<Json<ApiResponse<Value>>, ManagementError> {
    require(&principal, PROVIDER_READ)?;
    let Query(query) = query.map_err(|_| ManagementError::invalid())?;
    let cursor = opaque_decode(query.cursor.as_deref(), &state.policy.cursor_signing_key)?;
    let mut page = state
        .repository
        .list_provider_model_candidates(
            &provider_id,
            &discovery_id,
            cursor.as_deref(),
            page_limit(query.limit)?,
        )
        .await
        .map_err(|_| ManagementError::internal())?;
    page.next_cursor = page
        .next_cursor
        .map(|value| opaque_encode(&value, &state.policy.cursor_signing_key));
    Ok(Json(ApiResponse::ok(
        serde_json::to_value(page).map_err(|_| ManagementError::internal())?,
    )))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelImportPreviewRequest {
    discovery_id: String,
    #[serde(default)]
    select_all: bool,
    #[serde(default)]
    model_ids: Vec<String>,
    #[serde(default)]
    policies: BTreeMap<String, ProviderDraftModel>,
}

async fn preview_model_imports(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(provider_id): Path<String>,
    input: Result<Json<ModelImportPreviewRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<Value>>, ManagementError> {
    require(&principal, PROVIDER_WRITE)?;
    let Json(mut input) = input.map_err(|_| ManagementError::invalid())?;
    input.model_ids.sort();
    input.model_ids.dedup();
    if input.select_all != input.model_ids.is_empty() {
        return Err(ManagementError::invalid());
    }
    let operation = state
        .repository
        .get_provider_discovery(&provider_id, &input.discovery_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    if operation.status != insight_durable::ProviderOperationStatus::Succeeded || operation.stale {
        return Err(ManagementError::new(
            StatusCode::CONFLICT,
            "PROVIDER_OPERATION_STALE",
            "Provider discovery is not usable",
        ));
    }
    let mut candidates = Vec::new();
    let mut cursor = None;
    loop {
        let page = state
            .repository
            .list_provider_model_candidates(
                &provider_id,
                &input.discovery_id,
                cursor.as_deref(),
                MAX_PAGE_SIZE,
            )
            .await
            .map_err(|_| ManagementError::internal())?;
        candidates.extend(page.items);
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
        if candidates.len() > state.policy.max_models {
            return Err(ManagementError::invalid());
        }
    }
    let selected = candidates
        .into_iter()
        .filter(|candidate| {
            input.select_all || input.model_ids.binary_search(&candidate.model_id).is_ok()
        })
        .collect::<Vec<_>>();
    if selected.is_empty() || (!input.select_all && selected.len() != input.model_ids.len()) {
        return Err(ManagementError::invalid());
    }
    let mut expanded = Vec::with_capacity(selected.len());
    for candidate in selected {
        let model = input
            .policies
            .remove(&candidate.model_id)
            .unwrap_or(ProviderDraftModel {
                id: candidate.model_id.clone(),
                input: vec!["text".to_owned()],
                capabilities: vec!["complete".to_owned()],
                provenance: ProviderCapabilityProvenance {
                    provenance_type: "operator_asserted".to_owned(),
                },
            });
        if model.id != candidate.model_id {
            return Err(ManagementError::invalid());
        }
        expanded
            .push(json!({"model":model,"candidate_fingerprint":candidate.candidate_fingerprint}));
    }
    if !input.policies.is_empty() {
        return Err(ManagementError::invalid());
    }
    Ok(Json(ApiResponse::ok(
        json!({"discovery_id":input.discovery_id,"models":expanded}),
    )))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplaceModelsRequest {
    models: Vec<ProviderDraftModel>,
}

async fn replace_models(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(provider_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<ReplaceModelsRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, PROVIDER_WRITE)?;
    let expected = parse_etag(&headers, "draft")?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    let stored = state
        .repository
        .get_provider_draft(&provider_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    if stored.draft_version != expected {
        return Err(ManagementError::new(
            StatusCode::PRECONDITION_FAILED,
            "PROVIDER_MANAGEMENT_PRECONDITION_FAILED",
            "Provider Draft version changed",
        ));
    }
    let mut draft: ProviderDraftDocument =
        serde_json::from_value(stored.document).map_err(|_| ManagementError::internal())?;
    draft.models = input.models;
    let draft = normalize_draft(draft, &state.policy)?;
    let body = json!({"models":draft.models});
    receipt_response(
        state
            .repository
            .replace_provider_draft(ReplaceProviderDraftCommand {
                metadata: metadata(
                    &principal,
                    PROVIDER_WRITE,
                    &headers,
                    "PUT",
                    format!("/v1/admin/providers/{provider_id}/draft/models"),
                    &body,
                )?,
                provider_id,
                expected_draft_version: expected,
                provider_input_hash: provider_input_hash(&draft, &state.policy)?,
                draft_document: serde_json::to_value(draft)
                    .map_err(|_| ManagementError::internal())?,
            })
            .await
            .map_err(ManagementError::from)?,
    )
}

fn validate_provider_draft(
    draft: &ProviderDraftDocument,
    policy: &ProviderManagementPolicy,
) -> Value {
    let mut diagnostics = Vec::new();
    if draft
        .credential
        .reference()
        .is_some_and(|reference| !policy.resolvable_secret_refs.contains(reference))
    {
        diagnostics.push(
            json!({"code":"credential_reference_unresolvable","path":"/credential/reference"}),
        );
    }
    for (index, model) in draft.models.iter().enumerate() {
        if model
            .capabilities
            .iter()
            .any(|capability| matches!(capability.as_str(), "tools" | "structured_output"))
            && model.provenance.provenance_type == "operator_asserted"
        {
            diagnostics.push(json!({"code":"capability_requires_verified_provenance","path":format!("/models/{index}/provenance")}));
        }
    }
    json!({"valid":diagnostics.is_empty(),"diagnostics":diagnostics,
        "policy_fingerprint":policy.policy_fingerprint,
        "adapter_version":policy.installed_adapters.get(&draft.adapter.adapter_type)})
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateValidationRequest {
    draft_version: u64,
}

async fn create_validation(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(provider_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<CreateValidationRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, PROVIDER_PUBLISH)?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    let stored = state
        .repository
        .get_provider_draft(&provider_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    if stored.draft_version != input.draft_version {
        return Err(ManagementError::new(
            StatusCode::PRECONDITION_FAILED,
            "PROVIDER_MANAGEMENT_PRECONDITION_FAILED",
            "Provider Draft version changed",
        ));
    }
    let draft: ProviderDraftDocument =
        serde_json::from_value(stored.document).map_err(|_| ManagementError::internal())?;
    let document = validate_provider_draft(&draft, &state.policy);
    let valid = document
        .get("valid")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let validation_id = format!("pval_{}", Uuid::new_v4().simple());
    let body = json!({"draft_version":input.draft_version});
    let receipt = state
        .repository
        .create_provider_validation(CreateProviderValidationCommand {
            metadata: metadata(
                &principal,
                PROVIDER_PUBLISH,
                &headers,
                "POST",
                format!("/v1/admin/providers/{provider_id}/validations"),
                &body,
            )?,
            report: ProviderValidationReport {
                validation_id,
                provider_id: provider_id.clone(),
                draft_version: stored.draft_version,
                provider_input_hash: stored.provider_input_hash.clone(),
                report_hash: canonical_hash(&document)?,
                valid,
                document,
                created_at: Utc::now(),
                created_by: principal.identity().to_owned(),
            },
            expected_draft_version: stored.draft_version,
            expected_provider_input_hash: stored.provider_input_hash,
        })
        .await
        .map_err(ManagementError::from)?;
    let validation_id = receipt
        .response
        .get("validation_id")
        .and_then(Value::as_str)
        .ok_or_else(ManagementError::internal)?;
    receipt_location_response(
        receipt.clone(),
        format!("/v1/admin/providers/{provider_id}/validations/{validation_id}"),
    )
}

async fn get_validation(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path((provider_id, validation_id)): Path<(String, String)>,
) -> Result<Json<ApiResponse<Value>>, ManagementError> {
    require(&principal, PROVIDER_READ)?;
    let value = state
        .repository
        .get_provider_validation(&provider_id, &validation_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    Ok(Json(ApiResponse::ok(
        serde_json::to_value(value).map_err(|_| ManagementError::internal())?,
    )))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateConnectionTestRequest {
    draft_version: u64,
    mode: ProviderConnectionTestMode,
}

async fn create_connection_test(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(provider_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<CreateConnectionTestRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, PROVIDER_TEST)?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    let draft = state
        .repository
        .get_provider_draft(&provider_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    if draft.draft_version != input.draft_version {
        return Err(ManagementError::new(
            StatusCode::PRECONDITION_FAILED,
            "PROVIDER_MANAGEMENT_PRECONDITION_FAILED",
            "Provider Draft version changed",
        ));
    }
    let test_id = format!("ptest_{}", Uuid::new_v4().simple());
    let body = json!({"draft_version":input.draft_version,"mode":input.mode});
    let receipt = state
        .repository
        .create_provider_connection_test(CreateProviderConnectionTestCommand {
            metadata: metadata(
                &principal,
                PROVIDER_TEST,
                &headers,
                "POST",
                format!("/v1/admin/providers/{provider_id}/connection-tests"),
                &body,
            )?,
            test_id,
            provider_id: provider_id.clone(),
            expected_draft_version: input.draft_version,
            provider_input_hash: draft.provider_input_hash,
            mode: input.mode,
            max_pending_operations: state.policy.max_pending_operations,
        })
        .await
        .map_err(ManagementError::from)?;
    let test_id = receipt
        .response
        .get("test_id")
        .and_then(Value::as_str)
        .ok_or_else(ManagementError::internal)?;
    receipt_location_response(
        receipt.clone(),
        format!("/v1/admin/providers/{provider_id}/connection-tests/{test_id}"),
    )
}

async fn get_connection_test(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path((provider_id, test_id)): Path<(String, String)>,
) -> Result<Json<ApiResponse<Value>>, ManagementError> {
    require(&principal, PROVIDER_READ)?;
    let value = state
        .repository
        .get_provider_connection_test(&provider_id, &test_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    Ok(Json(ApiResponse::ok(
        serde_json::to_value(value).map_err(|_| ManagementError::internal())?,
    )))
}

async fn cancel_connection_test(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path((provider_id, test_id)): Path<(String, String)>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, ManagementError> {
    require(&principal, PROVIDER_TEST)?;
    if !body.is_empty() {
        return Err(ManagementError::invalid());
    }
    receipt_response(
        state
            .repository
            .cancel_provider_connection_test(CancelProviderOperationCommand {
                metadata: metadata(
                    &principal,
                    PROVIDER_TEST,
                    &headers,
                    "DELETE",
                    format!("/v1/admin/providers/{provider_id}/connection-tests/{test_id}"),
                    &json!({}),
                )?,
                provider_id,
                operation_id: test_id,
            })
            .await
            .map_err(ManagementError::from)?,
    )
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishRevisionRequest {
    draft_version: u64,
    validation_id: String,
    #[serde(default)]
    discovery_id: Option<String>,
    #[serde(default)]
    connection_test_id: Option<String>,
}

async fn publish_revision(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(provider_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<PublishRevisionRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, PROVIDER_PUBLISH)?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    if state.policy.require_connection_test_for_publish && input.connection_test_id.is_none() {
        return Err(ManagementError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "PROVIDER_CONNECTION_TEST_REQUIRED",
            "Provider publish requires a successful connection test",
        ));
    }
    let stored = state
        .repository
        .get_provider_draft(&provider_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    if stored.draft_version != input.draft_version {
        return Err(ManagementError::new(
            StatusCode::PRECONDITION_FAILED,
            "PROVIDER_MANAGEMENT_PRECONDITION_FAILED",
            "Provider Draft version changed",
        ));
    }
    let provider = state
        .repository
        .get_provider(&provider_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let draft: ProviderDraftDocument =
        serde_json::from_value(stored.document).map_err(|_| ManagementError::internal())?;
    let adapter_version = state
        .policy
        .installed_adapters
        .get(&provider.adapter_type)
        .ok_or_else(ManagementError::internal)?;
    let adapter_manifest_digest = state
        .policy
        .adapter_manifest_digests
        .get(&provider.adapter_type)
        .ok_or_else(ManagementError::internal)?;
    let worker_version = state
        .policy
        .adapter_worker_versions
        .get(&provider.adapter_type)
        .ok_or_else(ManagementError::internal)?;
    let credential_reference = draft.credential.reference().map(str::to_owned);
    let credential_reference_hash = draft
        .credential
        .reference()
        .map(|reference| sha256(reference.as_bytes()));
    let document = json!({
        "provider_id":provider_id,
        "adapter":{"type":draft.adapter.adapter_type,"version":adapter_version,"worker_version":worker_version,"manifest_digest":adapter_manifest_digest},
        "endpoint":draft.endpoint,
        "credential":{"type":match draft.credential { ProviderDraftCredential::Bearer{..}=>"bearer", ProviderDraftCredential::ApiKey{..}=>"api_key", ProviderDraftCredential::None=>"none" },"reference":credential_reference,"reference_hash":credential_reference_hash},
        "transport":draft.transport,
        "models":draft.models,
        "policy_fingerprint":state.policy.policy_fingerprint,
        "source":{"draft_version":stored.draft_version,"provider_input_hash":stored.provider_input_hash,
                  "validation_id":input.validation_id,"discovery_id":input.discovery_id,"connection_test_id":input.connection_test_id}
    });
    let revision_id = format!("prev_{}", Uuid::new_v4().simple());
    let revision_hash = canonical_hash(&document)?;
    let body = json!({"draft_version":input.draft_version,"validation_id":input.validation_id,"discovery_id":input.discovery_id,"connection_test_id":input.connection_test_id});
    let receipt = state
        .repository
        .publish_provider_revision(PublishProviderRevisionCommand {
            metadata: metadata(
                &principal,
                PROVIDER_PUBLISH,
                &headers,
                "POST",
                format!("/v1/admin/providers/{provider_id}/revisions"),
                &body,
            )?,
            revision_id,
            provider_id: provider.provider_id.clone(),
            expected_draft_version: input.draft_version,
            validation_id: input.validation_id,
            discovery_id: input.discovery_id,
            connection_test_id: input.connection_test_id,
            revision_hash,
            document,
        })
        .await
        .map_err(ManagementError::from)?;
    let location_id = receipt
        .response
        .get("revision_id")
        .and_then(Value::as_str)
        .ok_or_else(ManagementError::internal)?
        .to_owned();
    let mut response = receipt_response(receipt)?;
    response.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(&format!(
            "/v1/admin/providers/{}/revisions/{location_id}",
            provider.provider_id
        ))
        .map_err(|_| ManagementError::internal())?,
    );
    Ok(response)
}

async fn get_revision(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path((provider_id, revision_id)): Path<(String, String)>,
) -> Result<Json<ApiResponse<Value>>, ManagementError> {
    require(&principal, PROVIDER_READ)?;
    let value = state
        .repository
        .get_provider_revision(&provider_id, &revision_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    Ok(Json(ApiResponse::ok(
        serde_json::to_value(value).map_err(|_| ManagementError::internal())?,
    )))
}

async fn list_revisions(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(provider_id): Path<String>,
    query: Result<Query<CursorPageQuery>, QueryRejection>,
) -> Result<Json<ApiResponse<Value>>, ManagementError> {
    require(&principal, PROVIDER_READ)?;
    let Query(query) = query.map_err(|_| ManagementError::invalid())?;
    let cursor = opaque_decode(query.cursor.as_deref(), &state.policy.cursor_signing_key)?;
    let mut page = state
        .repository
        .list_provider_revisions(&provider_id, cursor.as_deref(), page_limit(query.limit)?)
        .await
        .map_err(|_| ManagementError::internal())?;
    page.next_cursor = page
        .next_cursor
        .map(|value| opaque_encode(&value, &state.policy.cursor_signing_key));
    Ok(Json(ApiResponse::ok(
        serde_json::to_value(page).map_err(|_| ManagementError::internal())?,
    )))
}

async fn get_active_revision(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(provider_id): Path<String>,
) -> Result<Json<ApiResponse<Value>>, ManagementError> {
    require(&principal, PROVIDER_READ)?;
    let provider = state
        .repository
        .get_provider(&provider_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    let revision = match provider.active_revision_id.as_deref() {
        Some(revision_id) => state
            .repository
            .get_provider_revision(&provider_id, revision_id)
            .await
            .map_err(|_| ManagementError::internal())?,
        None => None,
    };
    Ok(Json(ApiResponse::ok(
        json!({"provider":provider,"revision":revision}),
    )))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivateRevisionRequest {
    revision_id: String,
}

async fn activate_revision(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(provider_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<ActivateRevisionRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, PROVIDER_ACTIVATE)?;
    let expected = parse_etag(&headers, "provider")?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    let revision = state
        .repository
        .get_provider_revision(&provider_id, &input.revision_id)
        .await
        .map_err(|_| ManagementError::internal())?
        .ok_or_else(ManagementError::not_found)?;
    state.readiness.probe(&revision).await.map_err(|_| {
        ManagementError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "PROVIDER_REVISION_NOT_READY",
            "Provider Revision readiness probe failed",
        )
    })?;
    let body = json!({"revision_id":input.revision_id});
    receipt_response(
        state
            .repository
            .activate_provider_revision(ActivateProviderRevisionCommand {
                metadata: metadata(
                    &principal,
                    PROVIDER_ACTIVATE,
                    &headers,
                    "PUT",
                    format!("/v1/admin/providers/{provider_id}/active-revision"),
                    &body,
                )?,
                provider_id,
                revision_id: input.revision_id,
                expected_provider_version: expected,
            })
            .await
            .map_err(ManagementError::from)?,
    )
}

async fn deactivate_revision(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(provider_id): Path<String>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, ManagementError> {
    require(&principal, PROVIDER_ACTIVATE)?;
    if !body.is_empty() {
        return Err(ManagementError::invalid());
    }
    let expected = parse_etag(&headers, "provider")?;
    receipt_response(
        state
            .repository
            .deactivate_provider_revision(DeactivateProviderRevisionCommand {
                metadata: metadata(
                    &principal,
                    PROVIDER_ACTIVATE,
                    &headers,
                    "DELETE",
                    format!("/v1/admin/providers/{provider_id}/active-revision"),
                    &json!({}),
                )?,
                provider_id,
                expected_provider_version: expected,
            })
            .await
            .map_err(ManagementError::from)?,
    )
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReasonRequest {
    reason_code: String,
}

async fn suspend_provider(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(provider_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<ReasonRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, PROVIDER_SUSPEND)?;
    let expected = parse_etag(&headers, "provider")?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    if !valid_identity(&input.reason_code, 128) {
        return Err(ManagementError::invalid());
    }
    let body = json!({"reason_code":input.reason_code});
    receipt_response(
        state
            .repository
            .suspend_provider(SuspendProviderCommand {
                metadata: metadata(
                    &principal,
                    PROVIDER_SUSPEND,
                    &headers,
                    "POST",
                    format!("/v1/admin/providers/{provider_id}/suspension"),
                    &body,
                )?,
                provider_id,
                expected_provider_version: expected,
                reason_code: input.reason_code,
            })
            .await
            .map_err(ManagementError::from)?,
    )
}

async fn resume_provider(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(provider_id): Path<String>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Response, ManagementError> {
    require(&principal, PROVIDER_SUSPEND)?;
    if !body.is_empty() {
        return Err(ManagementError::invalid());
    }
    let expected = parse_etag(&headers, "provider")?;
    receipt_response(
        state
            .repository
            .resume_provider(ResumeProviderCommand {
                metadata: metadata(
                    &principal,
                    PROVIDER_SUSPEND,
                    &headers,
                    "DELETE",
                    format!("/v1/admin/providers/{provider_id}/suspension"),
                    &json!({}),
                )?,
                provider_id,
                expected_provider_version: expected,
            })
            .await
            .map_err(ManagementError::from)?,
    )
}

async fn retire_provider(
    State(state): State<Arc<ProviderManagementApiState>>,
    Extension(principal): Extension<OperatorPrincipal>,
    Path(provider_id): Path<String>,
    headers: HeaderMap,
    input: Result<Json<ReasonRequest>, JsonRejection>,
) -> Result<Response, ManagementError> {
    require(&principal, PROVIDER_RETIRE)?;
    let expected = parse_etag(&headers, "provider")?;
    let Json(input) = input.map_err(|_| ManagementError::invalid())?;
    if !valid_identity(&input.reason_code, 128) {
        return Err(ManagementError::invalid());
    }
    let body = json!({"reason_code":input.reason_code});
    receipt_response(
        state
            .repository
            .retire_provider(RetireProviderCommand {
                metadata: metadata(
                    &principal,
                    PROVIDER_RETIRE,
                    &headers,
                    "POST",
                    format!("/v1/admin/providers/{provider_id}/retirement"),
                    &body,
                )?,
                provider_id,
                expected_provider_version: expected,
                reason_code: input.reason_code,
            })
            .await
            .map_err(ManagementError::from)?,
    )
}

#[cfg(test)]
mod contract_tests {
    use super::*;

    const MANAGEMENT_SCHEMA: &str = workspace_asset_str!("schemas/provider-management-v1.json");
    const MANAGEMENT_SAMPLES: &str =
        workspace_asset_str!("schemas/provider-management-v1.samples.json");
    const MANAGEMENT_OPENAPI: &str =
        workspace_asset_str!("schemas/provider-management-v1.openapi.json");

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
    fn provider_schema_is_closed_and_never_accepts_secret_values_or_wildcards() {
        let samples: Value = serde_json::from_str(MANAGEMENT_SAMPLES).unwrap();
        assert!(
            validator_for("createProviderRequest").is_valid(&samples["valid"]["create_provider"])
        );
        assert!(
            validator_for("connectionTestRequest").is_valid(&samples["valid"]["connection_test"])
        );
        assert!(validator_for("publishRequest").is_valid(&samples["valid"]["publish"]));
        assert!(validator_for("activateRequest").is_valid(&samples["valid"]["activate"]));
        assert!(!validator_for("draft").is_valid(&samples["invalid"]["secret_value"]));
        assert!(
            !validator_for("replaceModelsRequest").is_valid(&samples["invalid"]["wildcard_model"])
        );
        assert!(!validator_for("connectionTestRequest")
            .is_valid(&samples["invalid"]["arbitrary_probe"]));
        assert!(!validator_for("draft").is_valid(&json!({
            "adapter":{"type":"open_ai_compatible"},"endpoint":"https://example.test/v1",
            "credential":{"type":"none"},"models":[],"auto_import":true
        })));
    }

    #[test]
    fn provider_openapi_covers_every_route_and_delete_is_body_free() {
        let openapi: Value = serde_json::from_str(MANAGEMENT_OPENAPI).unwrap();
        assert_eq!(openapi["openapi"], "3.1.0");
        let paths = openapi["paths"].as_object().unwrap();
        let expected = BTreeSet::from([
            ("get", "/v1/admin/provider-templates"),
            ("get", "/v1/admin/providers"),
            ("post", "/v1/admin/providers"),
            ("get", "/v1/admin/providers/{provider_id}"),
            ("delete", "/v1/admin/providers/{provider_id}"),
            ("get", "/v1/admin/providers/{provider_id}/draft"),
            ("put", "/v1/admin/providers/{provider_id}/draft"),
            ("post", "/v1/admin/providers/{provider_id}/discoveries"),
            (
                "get",
                "/v1/admin/providers/{provider_id}/discoveries/{discovery_id}",
            ),
            (
                "delete",
                "/v1/admin/providers/{provider_id}/discoveries/{discovery_id}",
            ),
            (
                "get",
                "/v1/admin/providers/{provider_id}/discoveries/{discovery_id}/model-candidates",
            ),
            (
                "post",
                "/v1/admin/providers/{provider_id}/model-import-previews",
            ),
            ("put", "/v1/admin/providers/{provider_id}/draft/models"),
            ("post", "/v1/admin/providers/{provider_id}/validations"),
            (
                "get",
                "/v1/admin/providers/{provider_id}/validations/{validation_id}",
            ),
            ("post", "/v1/admin/providers/{provider_id}/connection-tests"),
            (
                "get",
                "/v1/admin/providers/{provider_id}/connection-tests/{test_id}",
            ),
            (
                "delete",
                "/v1/admin/providers/{provider_id}/connection-tests/{test_id}",
            ),
            ("get", "/v1/admin/providers/{provider_id}/revisions"),
            ("post", "/v1/admin/providers/{provider_id}/revisions"),
            (
                "get",
                "/v1/admin/providers/{provider_id}/revisions/{revision_id}",
            ),
            ("get", "/v1/admin/providers/{provider_id}/active-revision"),
            ("put", "/v1/admin/providers/{provider_id}/active-revision"),
            (
                "delete",
                "/v1/admin/providers/{provider_id}/active-revision",
            ),
            ("post", "/v1/admin/providers/{provider_id}/suspension"),
            ("delete", "/v1/admin/providers/{provider_id}/suspension"),
            ("post", "/v1/admin/providers/{provider_id}/retirement"),
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

    #[test]
    fn provider_draft_normalization_binds_policy_and_rejects_unlisted_secrets() {
        let policy = ProviderManagementPolicy {
            installed_adapters: BTreeMap::from([(
                "open_ai_compatible".to_owned(),
                "2.1.0".to_owned(),
            )]),
            adapter_worker_versions: BTreeMap::from([(
                "open_ai_compatible".to_owned(),
                "openai-chat-worker-test".to_owned(),
            )]),
            adapter_manifest_digests: BTreeMap::from([(
                "open_ai_compatible".to_owned(),
                "sha256:adapter-manifest".to_owned(),
            )]),
            allowed_secret_refs: BTreeSet::from(["secret://environment/LLM_TOKEN".to_owned()]),
            resolvable_secret_refs: BTreeSet::new(),
            templates: Vec::new(),
            allow_loopback_development: false,
            max_models: 4,
            max_connect_timeout_ms: 30_000,
            max_request_timeout_ms: 600_000,
            max_pending_operations: 2,
            require_connection_test_for_publish: false,
            policy_fingerprint: sha256(b"policy"),
            cursor_signing_key: [4; 32],
        };
        let draft = ProviderDraftDocument {
            adapter: ProviderDraftAdapter {
                adapter_type: "open_ai_compatible".to_owned(),
            },
            endpoint: "https://example.test/v1/".to_owned(),
            credential: ProviderDraftCredential::Bearer {
                reference: "secret://environment/LLM_TOKEN".to_owned(),
            },
            transport: ProviderDraftTransport::default(),
            models: vec![ProviderDraftModel {
                id: "vendor/model".to_owned(),
                input: vec!["text".to_owned()],
                capabilities: vec!["streaming".to_owned(), "complete".to_owned()],
                provenance: ProviderCapabilityProvenance {
                    provenance_type: "adapter_verified".to_owned(),
                },
            }],
            description: None,
            operator_note: None,
        };
        let normalized = normalize_draft(draft.clone(), &policy).unwrap();
        assert_eq!(normalized.endpoint, "https://example.test/v1");
        assert_ne!(
            provider_input_hash(&normalized, &policy).unwrap(),
            canonical_hash(&serde_json::to_value(&normalized).unwrap()).unwrap()
        );
        let mut denied = draft;
        denied.credential = ProviderDraftCredential::Bearer {
            reference: "secret://environment/OTHER".to_owned(),
        };
        assert!(normalize_draft(denied, &policy).is_err());

        let mut private_ip = normalized.clone();
        private_ip.endpoint = "https://169.254.169.254/latest/meta-data".to_owned();
        assert!(normalize_draft(private_ip, &policy).is_err());

        let mut development_policy = policy;
        development_policy.allow_loopback_development = true;
        let mut loopback_with_secret = normalized;
        loopback_with_secret.endpoint = "http://127.0.0.1:8080/v1".to_owned();
        assert!(normalize_draft(loopback_with_secret, &development_policy).is_err());
    }
}
