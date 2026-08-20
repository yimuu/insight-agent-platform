use crate::authentication::AuthenticatedPrincipal;
use async_trait::async_trait;
use axum::{
    body::Bytes,
    extract::{rejection::JsonRejection, DefaultBodyLimit, Extension, Path, State},
    http::{header::CACHE_CONTROL, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{
    canonical_digest, AdministrativeGate, ApiProblem, ApiProblemCode, DeploymentClosure,
    EntityLifecycle, OperationViewV1, PublishedVersionPayload, RegistryResourceKind,
    ResourceDocument, ResourceDraftPayload, ResourceId, ResourceKind, Sha256Digest, UtcTimestamp,
    MAX_FIELD_ERRORS, MAX_SAFE_TEXT_BYTES,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const IDEMPOTENCY_KEY: &str = "idempotency-key";
const IF_MATCH: &str = "if-match";
const RESOURCE_COMMAND_DEADLINE_MILLISECONDS: i64 = 5_000;
const MAX_RESOURCE_REQUEST_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateResourceRequestV1 {
    pub display_name: String,
    pub document: ResourceDocument,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateDeploymentRequestV1 {
    pub resource_version_id: ResourceId,
    pub environment: String,
    pub closure: DeploymentClosure,
}

impl CreateDeploymentRequestV1 {
    fn validate_for(&self, resource_kind: RegistryResourceKind) -> bool {
        resource_kind.deployment_kind().is_some()
            && resource_kind.allows_version_kind(self.resource_version_id.kind())
            && self.closure.resource_kind() == resource_kind
            && self.closure.validate().is_ok()
            && !self.environment.is_empty()
            && self.environment.len() <= 128
            && self
                .environment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PublishResourceDraftRequestV1 {
    Single {
        revision_no: u64,
        content_digest: Sha256Digest,
        artifact_id: Option<ResourceId>,
    },
    Agent {
        revision_no: u64,
        interface_content_digest: Sha256Digest,
        plan_content_digest: Sha256Digest,
        artifact_id: Option<ResourceId>,
    },
}

impl PublishResourceDraftRequestV1 {
    fn validate_for(&self, resource_kind: RegistryResourceKind) -> bool {
        let (revision_no, artifact_id, correct_shape) = match self {
            Self::Single {
                revision_no,
                artifact_id,
                ..
            } => (
                *revision_no,
                artifact_id,
                resource_kind != RegistryResourceKind::Agent,
            ),
            Self::Agent {
                revision_no,
                artifact_id,
                ..
            } => (
                *revision_no,
                artifact_id,
                resource_kind == RegistryResourceKind::Agent,
            ),
        };
        correct_shape
            && revision_no > 0
            && i64::try_from(revision_no).is_ok()
            && artifact_id
                .as_ref()
                .is_none_or(|artifact_id| artifact_id.kind() == ResourceKind::Artifact)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceViewV1 {
    pub schema_version: u32,
    pub resource_id: ResourceId,
    pub resource_kind: RegistryResourceKind,
    pub lifecycle_state: EntityLifecycle,
    pub gate_state: AdministrativeGate,
    pub draft_generation: u64,
    pub version: u64,
    pub draft: ResourceDraftPayload,
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceVersionViewV1 {
    pub schema_version: u32,
    pub resource_id: ResourceId,
    pub resource_kind: RegistryResourceKind,
    pub resource_version_id: ResourceId,
    pub revision_no: u64,
    pub content_digest: Sha256Digest,
    pub artifact_id: Option<ResourceId>,
    pub payload: PublishedVersionPayload,
    pub created_at: UtcTimestamp,
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentViewV1 {
    pub schema_version: u32,
    pub deployment_id: ResourceId,
    pub resource_id: ResourceId,
    pub resource_kind: RegistryResourceKind,
    pub resource_version_id: ResourceId,
    pub environment: String,
    pub closure_digest: Sha256Digest,
    pub closure: DeploymentClosure,
    pub created_at: UtcTimestamp,
    pub etag: String,
}

impl DeploymentViewV1 {
    pub fn validate(&self) -> Result<(), ResourceApplicationError> {
        if self.schema_version != 1
            || self.resource_id.kind() != self.resource_kind.id_kind()
            || self.resource_kind.deployment_kind() != Some(self.deployment_id.kind())
            || !self
                .resource_kind
                .allows_version_kind(self.resource_version_id.kind())
            || self.environment.is_empty()
            || self.closure.resource_kind() != self.resource_kind
            || self.closure.validate().is_err()
            || deployment_closure_digest(&self.closure).as_ref() != Ok(&self.closure_digest)
            || self.etag != deployment_etag(&self.deployment_id, &self.closure_digest)
        {
            return Err(ResourceApplicationError::Internal);
        }
        Ok(())
    }
}

impl ResourceVersionViewV1 {
    pub fn validate(&self) -> Result<(), ResourceApplicationError> {
        if self.schema_version != 1
            || self.resource_id.kind() != self.resource_kind.id_kind()
            || !self
                .resource_kind
                .allows_version_kind(self.resource_version_id.kind())
            || self.revision_no == 0
            || self
                .artifact_id
                .as_ref()
                .is_some_and(|artifact_id| artifact_id.kind() != ResourceKind::Artifact)
            || self
                .payload
                .validate_for(self.resource_kind, &self.resource_version_id)
                .is_err()
            || self.etag != resource_version_etag(&self.resource_version_id, &self.content_digest)
        {
            return Err(ResourceApplicationError::Internal);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedResourceVersionSummaryV1 {
    pub resource_version_id: ResourceId,
    pub revision_no: u64,
    pub content_digest: Sha256Digest,
    pub artifact_id: Option<ResourceId>,
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishResourceDraftResponseV1 {
    pub schema_version: u32,
    pub resource_id: ResourceId,
    pub resource_kind: RegistryResourceKind,
    pub draft_generation: u64,
    pub version: u64,
    pub published_versions: Vec<PublishedResourceVersionSummaryV1>,
    pub etag: String,
}

impl PublishResourceDraftResponseV1 {
    pub fn validate(&self) -> Result<(), ResourceApplicationError> {
        let mut kinds = std::collections::BTreeSet::new();
        let valid_versions = self.published_versions.iter().all(|version| {
            version.revision_no > 0
                && self
                    .resource_kind
                    .allows_version_kind(version.resource_version_id.kind())
                && kinds.insert(version.resource_version_id.kind())
                && version
                    .artifact_id
                    .as_ref()
                    .is_none_or(|artifact_id| artifact_id.kind() == ResourceKind::Artifact)
                && version.etag
                    == resource_version_etag(&version.resource_version_id, &version.content_digest)
        });
        let valid_count = if self.resource_kind == RegistryResourceKind::Agent {
            self.published_versions.len() == 2
                && kinds.contains(&ResourceKind::AgentInterfaceRevision)
                && kinds.contains(&ResourceKind::AgentPlanRevision)
        } else {
            self.published_versions.len() == 1
        };
        if self.schema_version != 1
            || self.resource_id.kind() != self.resource_kind.id_kind()
            || self.draft_generation == 0
            || self.version == 0
            || !valid_versions
            || !valid_count
            || self.etag != resource_etag(&self.resource_id, self.version)
        {
            return Err(ResourceApplicationError::Internal);
        }
        Ok(())
    }
}

impl ResourceViewV1 {
    pub fn validate(&self) -> Result<(), ResourceApplicationError> {
        if self.schema_version != 1
            || self.resource_id.kind() != self.resource_kind.id_kind()
            || self.draft_generation == 0
            || self.version == 0
            || self.draft.document.kind() != self.resource_kind
            || self.draft.validate().is_err()
            || self.etag != resource_etag(&self.resource_id, self.version)
        {
            return Err(ResourceApplicationError::Internal);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CreateResourceIntent {
    pub principal: AuthenticatedPrincipal,
    pub resource_kind: RegistryResourceKind,
    pub draft: ResourceDraftPayload,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ReadResourceIntent {
    pub principal: AuthenticatedPrincipal,
    pub resource_kind: RegistryResourceKind,
    pub resource_id: ResourceId,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct UpdateResourceDraftIntent {
    pub principal: AuthenticatedPrincipal,
    pub resource_kind: RegistryResourceKind,
    pub resource_id: ResourceId,
    pub expected_resource_version: u64,
    pub draft: ResourceDraftPayload,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ValidateResourceDraftIntent {
    pub principal: AuthenticatedPrincipal,
    pub resource_kind: RegistryResourceKind,
    pub resource_id: ResourceId,
    pub expected_resource_version: u64,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ReadResourceVersionIntent {
    pub principal: AuthenticatedPrincipal,
    pub resource_kind: RegistryResourceKind,
    pub resource_id: ResourceId,
    pub resource_version_id: ResourceId,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ReadDeploymentIntent {
    pub principal: AuthenticatedPrincipal,
    pub resource_kind: RegistryResourceKind,
    pub resource_id: ResourceId,
    pub deployment_id: ResourceId,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct CreateDeploymentIntent {
    pub principal: AuthenticatedPrincipal,
    pub resource_kind: RegistryResourceKind,
    pub resource_id: ResourceId,
    pub expected_resource_version: u64,
    pub request: CreateDeploymentRequestV1,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct PublishResourceDraftIntent {
    pub principal: AuthenticatedPrincipal,
    pub resource_kind: RegistryResourceKind,
    pub resource_id: ResourceId,
    pub expected_resource_version: u64,
    pub request: PublishResourceDraftRequestV1,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceApplicationError {
    Invalid,
    Unauthenticated,
    Denied,
    NotFound,
    Conflict,
    PreconditionRequired,
    IdempotencyConflict,
    Unavailable,
    Internal,
}

#[async_trait]
pub trait ResourceApplication: Send + Sync {
    async fn create_resource(
        &self,
        intent: CreateResourceIntent,
    ) -> Result<ResourceViewV1, ResourceApplicationError>;

    async fn read_resource(
        &self,
        intent: ReadResourceIntent,
    ) -> Result<ResourceViewV1, ResourceApplicationError>;

    async fn update_resource_draft(
        &self,
        intent: UpdateResourceDraftIntent,
    ) -> Result<ResourceViewV1, ResourceApplicationError>;

    async fn validate_resource_draft(
        &self,
        intent: ValidateResourceDraftIntent,
    ) -> Result<OperationViewV1, ResourceApplicationError>;

    async fn read_resource_version(
        &self,
        intent: ReadResourceVersionIntent,
    ) -> Result<ResourceVersionViewV1, ResourceApplicationError>;

    async fn read_deployment(
        &self,
        intent: ReadDeploymentIntent,
    ) -> Result<DeploymentViewV1, ResourceApplicationError>;

    async fn create_deployment(
        &self,
        intent: CreateDeploymentIntent,
    ) -> Result<DeploymentViewV1, ResourceApplicationError>;

    async fn publish_resource_draft(
        &self,
        intent: PublishResourceDraftIntent,
    ) -> Result<PublishResourceDraftResponseV1, ResourceApplicationError>;
}

pub trait ResourceClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Debug, Default)]
pub struct SystemResourceClock;

impl ResourceClock for SystemResourceClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Clone)]
pub struct ResourceHttpState {
    application: Arc<dyn ResourceApplication>,
    clock: Arc<dyn ResourceClock>,
}

impl ResourceHttpState {
    pub fn new(application: Arc<dyn ResourceApplication>, clock: Arc<dyn ResourceClock>) -> Self {
        Self { application, clock }
    }
}

pub fn build_resource_router(state: ResourceHttpState) -> Router {
    Router::new()
        .route("/v1/{resource_noun}", post(create_resource))
        .route("/v1/{resource_noun}/{resource_id}", get(read_resource))
        .route(
            "/v1/{resource_noun}/{resource_id}/draft",
            put(update_resource_draft),
        )
        .route(
            "/v1/{resource_noun}/{resource_id}/draft:validate",
            post(validate_resource_draft),
        )
        .route(
            "/v1/{resource_noun}/{resource_id}/draft:publish",
            post(publish_resource_draft),
        )
        .route(
            "/v1/{resource_noun}/{resource_id}/versions/{resource_version_id}",
            get(read_resource_version),
        )
        .route(
            "/v1/{resource_noun}/{resource_id}/deployments/{deployment_id}",
            get(read_deployment),
        )
        .route(
            "/v1/{resource_noun}/{resource_id}/deployments",
            post(create_deployment),
        )
        .layer(DefaultBodyLimit::max(MAX_RESOURCE_REQUEST_BYTES))
        .with_state(state)
}

async fn create_deployment(
    State(state): State<ResourceHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    Path((resource_noun, resource_id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Result<Json<CreateDeploymentRequestV1>, JsonRejection>,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(ResourceApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(ResourceApplicationError::Unauthenticated);
    }
    let Some(resource_kind) = resource_kind_for_noun(&resource_noun) else {
        return problem(ResourceApplicationError::NotFound);
    };
    let resource_id = match resource_id.parse::<ResourceId>() {
        Ok(resource_id) if resource_id.kind() == resource_kind.id_kind() => resource_id,
        _ => return problem(ResourceApplicationError::NotFound),
    };
    let Json(request) = match body {
        Ok(body) if body.0.validate_for(resource_kind) => body,
        _ => return problem(ResourceApplicationError::Invalid),
    };
    let expected_resource_version = match expected_resource_version(&headers, &resource_id) {
        Ok(version) => version,
        Err(error) => return problem(error),
    };
    let idempotency_key_digest = match idempotency_key_digest_for_operation(
        &headers,
        &principal,
        resource_kind,
        "resource.create_deployment",
        Some(&resource_id),
    ) {
        Ok(digest) => digest,
        Err(error) => return problem(error),
    };
    let request_digest = match digest(&serde_json::json!({
        "expected_resource_version": expected_resource_version,
        "idempotency_key_digest": idempotency_key_digest,
        "operation": "resource.create_deployment",
        "principal_id": principal.principal_id,
        "request": request,
        "resource_id": resource_id,
        "resource_kind": resource_kind,
        "schema_version": 1,
        "tenant_id": principal.tenant_id,
    })) {
        Ok(digest) => digest,
        Err(error) => return problem(error),
    };
    let intent = CreateDeploymentIntent {
        principal,
        resource_kind,
        resource_id,
        expected_resource_version,
        request,
        idempotency_key_digest,
        request_digest,
        deadline: state.clock.now()
            + Duration::milliseconds(RESOURCE_COMMAND_DEADLINE_MILLISECONDS),
    };
    match state.application.create_deployment(intent).await {
        Ok(view) if view.validate().is_ok() => create_deployment_response(resource_noun, view),
        Ok(_) => problem(ResourceApplicationError::Internal),
        Err(error) => problem(error),
    }
}

async fn read_deployment(
    State(state): State<ResourceHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    Path((resource_noun, resource_id, deployment_id)): Path<(String, String, String)>,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(ResourceApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(ResourceApplicationError::Unauthenticated);
    }
    let Some(resource_kind) = resource_kind_for_noun(&resource_noun) else {
        return problem(ResourceApplicationError::NotFound);
    };
    let resource_id = match resource_id.parse::<ResourceId>() {
        Ok(resource_id) if resource_id.kind() == resource_kind.id_kind() => resource_id,
        _ => return problem(ResourceApplicationError::NotFound),
    };
    let deployment_id = match deployment_id.parse::<ResourceId>() {
        Ok(deployment_id) if resource_kind.deployment_kind() == Some(deployment_id.kind()) => {
            deployment_id
        }
        _ => return problem(ResourceApplicationError::NotFound),
    };
    let intent = ReadDeploymentIntent {
        principal,
        resource_kind,
        resource_id,
        deployment_id,
        deadline: state.clock.now()
            + Duration::milliseconds(RESOURCE_COMMAND_DEADLINE_MILLISECONDS),
    };
    match state.application.read_deployment(intent).await {
        Ok(view) if view.validate().is_ok() => read_deployment_response(view),
        Ok(_) => problem(ResourceApplicationError::Internal),
        Err(error) => problem(error),
    }
}

async fn publish_resource_draft(
    State(state): State<ResourceHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    Path((resource_noun, resource_id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Result<Json<PublishResourceDraftRequestV1>, JsonRejection>,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(ResourceApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(ResourceApplicationError::Unauthenticated);
    }
    let Some(resource_kind) = resource_kind_for_noun(&resource_noun) else {
        return problem(ResourceApplicationError::NotFound);
    };
    let resource_id = match resource_id.parse::<ResourceId>() {
        Ok(resource_id) if resource_id.kind() == resource_kind.id_kind() => resource_id,
        _ => return problem(ResourceApplicationError::NotFound),
    };
    let Json(request) = match body {
        Ok(body) if body.0.validate_for(resource_kind) => body,
        _ => return problem(ResourceApplicationError::Invalid),
    };
    let expected_resource_version = match expected_resource_version(&headers, &resource_id) {
        Ok(version) => version,
        Err(error) => return problem(error),
    };
    let idempotency_key_digest = match idempotency_key_digest_for_operation(
        &headers,
        &principal,
        resource_kind,
        "resource.publish_draft",
        Some(&resource_id),
    ) {
        Ok(digest) => digest,
        Err(error) => return problem(error),
    };
    let request_digest = match digest(&serde_json::json!({
        "expected_resource_version": expected_resource_version,
        "idempotency_key_digest": idempotency_key_digest,
        "operation": "resource.publish_draft",
        "principal_id": principal.principal_id,
        "request": request,
        "resource_id": resource_id,
        "resource_kind": resource_kind,
        "schema_version": 1,
        "tenant_id": principal.tenant_id,
    })) {
        Ok(digest) => digest,
        Err(error) => return problem(error),
    };
    let intent = PublishResourceDraftIntent {
        principal,
        resource_kind,
        resource_id,
        expected_resource_version,
        request,
        idempotency_key_digest,
        request_digest,
        deadline: state.clock.now()
            + Duration::milliseconds(RESOURCE_COMMAND_DEADLINE_MILLISECONDS),
    };
    match state.application.publish_resource_draft(intent).await {
        Ok(view) if view.validate().is_ok() => publish_resource_response(view),
        Ok(_) => problem(ResourceApplicationError::Internal),
        Err(error) => problem(error),
    }
}

async fn read_resource_version(
    State(state): State<ResourceHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    Path((resource_noun, resource_id, resource_version_id)): Path<(String, String, String)>,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(ResourceApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(ResourceApplicationError::Unauthenticated);
    }
    let Some(resource_kind) = resource_kind_for_noun(&resource_noun) else {
        return problem(ResourceApplicationError::NotFound);
    };
    let resource_id = match resource_id.parse::<ResourceId>() {
        Ok(resource_id) if resource_id.kind() == resource_kind.id_kind() => resource_id,
        _ => return problem(ResourceApplicationError::NotFound),
    };
    let resource_version_id = match resource_version_id.parse::<ResourceId>() {
        Ok(version_id) if resource_kind.allows_version_kind(version_id.kind()) => version_id,
        _ => return problem(ResourceApplicationError::NotFound),
    };
    let intent = ReadResourceVersionIntent {
        principal,
        resource_kind,
        resource_id,
        resource_version_id,
        deadline: state.clock.now()
            + Duration::milliseconds(RESOURCE_COMMAND_DEADLINE_MILLISECONDS),
    };
    match state.application.read_resource_version(intent).await {
        Ok(view) if view.validate().is_ok() => read_resource_version_response(view),
        Ok(_) => problem(ResourceApplicationError::Internal),
        Err(error) => problem(error),
    }
}

async fn validate_resource_draft(
    State(state): State<ResourceHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    Path((resource_noun, resource_id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !body.is_empty() {
        return problem(ResourceApplicationError::Invalid);
    }
    let Some(Extension(principal)) = principal else {
        return problem(ResourceApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(ResourceApplicationError::Unauthenticated);
    }
    let Some(resource_kind) = resource_kind_for_noun(&resource_noun) else {
        return problem(ResourceApplicationError::NotFound);
    };
    let resource_id = match resource_id.parse::<ResourceId>() {
        Ok(resource_id) if resource_id.kind() == resource_kind.id_kind() => resource_id,
        _ => return problem(ResourceApplicationError::NotFound),
    };
    let expected_resource_version = match expected_resource_version(&headers, &resource_id) {
        Ok(version) => version,
        Err(error) => return problem(error),
    };
    let idempotency_key_digest = match idempotency_key_digest_for_operation(
        &headers,
        &principal,
        resource_kind,
        "resource.validate_draft",
        Some(&resource_id),
    ) {
        Ok(digest) => digest,
        Err(error) => return problem(error),
    };
    let request_digest = match digest(&serde_json::json!({
        "expected_resource_version": expected_resource_version,
        "idempotency_key_digest": idempotency_key_digest,
        "operation": "resource.validate_draft",
        "principal_id": principal.principal_id,
        "resource_id": resource_id,
        "resource_kind": resource_kind,
        "schema_version": 1,
        "tenant_id": principal.tenant_id,
    })) {
        Ok(digest) => digest,
        Err(error) => return problem(error),
    };
    let intent = ValidateResourceDraftIntent {
        principal,
        resource_kind,
        resource_id,
        expected_resource_version,
        idempotency_key_digest,
        request_digest,
        deadline: state.clock.now()
            + Duration::milliseconds(RESOURCE_COMMAND_DEADLINE_MILLISECONDS),
    };
    match state.application.validate_resource_draft(intent).await {
        Ok(view) if view.validate().is_ok() => operation_accepted_response(view),
        Ok(_) => problem(ResourceApplicationError::Internal),
        Err(error) => problem(error),
    }
}

async fn update_resource_draft(
    State(state): State<ResourceHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    Path((resource_noun, resource_id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Result<Json<CreateResourceRequestV1>, JsonRejection>,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(ResourceApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(ResourceApplicationError::Unauthenticated);
    }
    let Some(resource_kind) = resource_kind_for_noun(&resource_noun) else {
        return problem(ResourceApplicationError::NotFound);
    };
    let resource_id = match resource_id.parse::<ResourceId>() {
        Ok(resource_id) if resource_id.kind() == resource_kind.id_kind() => resource_id,
        _ => return problem(ResourceApplicationError::NotFound),
    };
    let Json(body) = match body {
        Ok(body) => body,
        Err(_) => return problem(ResourceApplicationError::Invalid),
    };
    if body.document.kind() != resource_kind {
        return problem(ResourceApplicationError::Invalid);
    }
    let expected_resource_version = match expected_resource_version(&headers, &resource_id) {
        Ok(version) => version,
        Err(error) => return problem(error),
    };
    let idempotency_key_digest = match idempotency_key_digest_for_operation(
        &headers,
        &principal,
        resource_kind,
        "resource.update_draft",
        Some(&resource_id),
    ) {
        Ok(digest) => digest,
        Err(error) => return problem(error),
    };
    let draft = ResourceDraftPayload {
        display_name: body.display_name,
        document: body.document,
        validation: None,
    };
    if draft.validate().is_err() {
        return problem(ResourceApplicationError::Invalid);
    }
    let request_digest = match digest(&serde_json::json!({
        "draft": draft,
        "expected_resource_version": expected_resource_version,
        "idempotency_key_digest": idempotency_key_digest,
        "operation": "resource.update_draft",
        "principal_id": principal.principal_id,
        "resource_id": resource_id,
        "resource_kind": resource_kind,
        "schema_version": 1,
        "tenant_id": principal.tenant_id,
    })) {
        Ok(digest) => digest,
        Err(error) => return problem(error),
    };
    let intent = UpdateResourceDraftIntent {
        principal,
        resource_kind,
        resource_id,
        expected_resource_version,
        draft,
        idempotency_key_digest,
        request_digest,
        deadline: state.clock.now()
            + Duration::milliseconds(RESOURCE_COMMAND_DEADLINE_MILLISECONDS),
    };
    match state.application.update_resource_draft(intent).await {
        Ok(view) if view.validate().is_ok() => read_resource_response(view),
        Ok(_) => problem(ResourceApplicationError::Internal),
        Err(error) => problem(error),
    }
}

async fn read_resource(
    State(state): State<ResourceHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    Path((resource_noun, resource_id)): Path<(String, String)>,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(ResourceApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(ResourceApplicationError::Unauthenticated);
    }
    let Some(resource_kind) = resource_kind_for_noun(&resource_noun) else {
        return problem(ResourceApplicationError::NotFound);
    };
    let resource_id: ResourceId = match resource_id.parse::<ResourceId>() {
        Ok(resource_id) if resource_id.kind() == resource_kind.id_kind() => resource_id,
        _ => return problem(ResourceApplicationError::NotFound),
    };
    let intent = ReadResourceIntent {
        principal,
        resource_kind,
        resource_id,
        deadline: state.clock.now()
            + Duration::milliseconds(RESOURCE_COMMAND_DEADLINE_MILLISECONDS),
    };
    match state.application.read_resource(intent).await {
        Ok(view) if view.validate().is_ok() => read_resource_response(view),
        Ok(_) => problem(ResourceApplicationError::Internal),
        Err(error) => problem(error),
    }
}

async fn create_resource(
    State(state): State<ResourceHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    Path(resource_noun): Path<String>,
    headers: HeaderMap,
    body: Result<Json<CreateResourceRequestV1>, JsonRejection>,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(ResourceApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(ResourceApplicationError::Unauthenticated);
    }
    let Some(resource_kind) = resource_kind_for_noun(&resource_noun) else {
        return problem(ResourceApplicationError::NotFound);
    };
    let Json(body) = match body {
        Ok(body) => body,
        Err(_) => return problem(ResourceApplicationError::Invalid),
    };
    if body.document.kind() != resource_kind {
        return problem(ResourceApplicationError::Invalid);
    }
    let idempotency_key_digest = match idempotency_key_digest(&headers, &principal, resource_kind) {
        Ok(digest) => digest,
        Err(error) => return problem(error),
    };
    let draft = ResourceDraftPayload {
        display_name: body.display_name,
        document: body.document,
        validation: None,
    };
    if draft.validate().is_err() {
        return problem(ResourceApplicationError::Invalid);
    }
    let request_digest =
        match create_request_digest(&principal, resource_kind, &draft, &idempotency_key_digest) {
            Ok(digest) => digest,
            Err(error) => return problem(error),
        };
    let intent = CreateResourceIntent {
        principal,
        resource_kind,
        draft,
        idempotency_key_digest,
        request_digest,
        deadline: state.clock.now()
            + Duration::milliseconds(RESOURCE_COMMAND_DEADLINE_MILLISECONDS),
    };
    match state.application.create_resource(intent).await {
        Ok(view) if view.validate().is_ok() => resource_response(resource_noun, view),
        Ok(_) => problem(ResourceApplicationError::Internal),
        Err(error) => problem(error),
    }
}

fn resource_response(resource_noun: String, view: ResourceViewV1) -> Response {
    let etag = match HeaderValue::from_str(&view.etag) {
        Ok(etag) => etag,
        Err(_) => return problem(ResourceApplicationError::Internal),
    };
    let location = match HeaderValue::from_str(&format!("/v1/{resource_noun}/{}", view.resource_id))
    {
        Ok(location) => location,
        Err(_) => return problem(ResourceApplicationError::Internal),
    };
    let mut response = (StatusCode::CREATED, Json(view)).into_response();
    response.headers_mut().insert("etag", etag);
    response.headers_mut().insert("location", location);
    no_store(response)
}

fn read_resource_response(view: ResourceViewV1) -> Response {
    let etag = match HeaderValue::from_str(&view.etag) {
        Ok(etag) => etag,
        Err(_) => return problem(ResourceApplicationError::Internal),
    };
    let mut response = (StatusCode::OK, Json(view)).into_response();
    response.headers_mut().insert("etag", etag);
    no_store(response)
}

fn read_resource_version_response(view: ResourceVersionViewV1) -> Response {
    let etag = match HeaderValue::from_str(&view.etag) {
        Ok(etag) => etag,
        Err(_) => return problem(ResourceApplicationError::Internal),
    };
    let mut response = (StatusCode::OK, Json(view)).into_response();
    response.headers_mut().insert("etag", etag);
    no_store(response)
}

fn read_deployment_response(view: DeploymentViewV1) -> Response {
    let etag = match HeaderValue::from_str(&view.etag) {
        Ok(etag) => etag,
        Err(_) => return problem(ResourceApplicationError::Internal),
    };
    let mut response = (StatusCode::OK, Json(view)).into_response();
    response.headers_mut().insert("etag", etag);
    no_store(response)
}

fn create_deployment_response(resource_noun: String, view: DeploymentViewV1) -> Response {
    let etag = match HeaderValue::from_str(&view.etag) {
        Ok(etag) => etag,
        Err(_) => return problem(ResourceApplicationError::Internal),
    };
    let location = match HeaderValue::from_str(&format!(
        "/v1/{resource_noun}/{}/deployments/{}",
        view.resource_id, view.deployment_id
    )) {
        Ok(location) => location,
        Err(_) => return problem(ResourceApplicationError::Internal),
    };
    let mut response = (StatusCode::CREATED, Json(view)).into_response();
    response.headers_mut().insert("etag", etag);
    response.headers_mut().insert("location", location);
    no_store(response)
}

fn publish_resource_response(view: PublishResourceDraftResponseV1) -> Response {
    let etag = match HeaderValue::from_str(&view.etag) {
        Ok(etag) => etag,
        Err(_) => return problem(ResourceApplicationError::Internal),
    };
    let mut response = (StatusCode::OK, Json(view)).into_response();
    response.headers_mut().insert("etag", etag);
    no_store(response)
}

fn operation_accepted_response(view: OperationViewV1) -> Response {
    let etag = match HeaderValue::from_str(&view.etag) {
        Ok(etag) => etag,
        Err(_) => return problem(ResourceApplicationError::Internal),
    };
    let location = match HeaderValue::from_str(&format!("/v1/operations/{}", view.operation_id)) {
        Ok(location) => location,
        Err(_) => return problem(ResourceApplicationError::Internal),
    };
    let mut response = (StatusCode::ACCEPTED, Json(view)).into_response();
    response.headers_mut().insert("etag", etag);
    response.headers_mut().insert("location", location);
    no_store(response)
}

fn idempotency_key_digest(
    headers: &HeaderMap,
    principal: &AuthenticatedPrincipal,
    resource_kind: RegistryResourceKind,
) -> Result<Sha256Digest, ResourceApplicationError> {
    idempotency_key_digest_for_operation(headers, principal, resource_kind, "resource.create", None)
}

fn idempotency_key_digest_for_operation(
    headers: &HeaderMap,
    principal: &AuthenticatedPrincipal,
    resource_kind: RegistryResourceKind,
    operation: &str,
    resource_id: Option<&ResourceId>,
) -> Result<Sha256Digest, ResourceApplicationError> {
    let mut values = headers.get_all(IDEMPOTENCY_KEY).iter();
    let key = values.next().ok_or(ResourceApplicationError::Invalid)?;
    if values.next().is_some() {
        return Err(ResourceApplicationError::Invalid);
    }
    let key = key
        .to_str()
        .map_err(|_| ResourceApplicationError::Invalid)?;
    if key.is_empty()
        || key.len() > 255
        || !key.is_ascii()
        || key.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(ResourceApplicationError::Invalid);
    }
    digest(&serde_json::json!({
        "key": key,
        "operation": operation,
        "principal_id": principal.principal_id,
        "resource_id": resource_id,
        "resource_kind": resource_kind,
        "schema_version": 1,
        "tenant_id": principal.tenant_id,
    }))
}

fn expected_resource_version(
    headers: &HeaderMap,
    resource_id: &ResourceId,
) -> Result<u64, ResourceApplicationError> {
    let mut values = headers.get_all(IF_MATCH).iter();
    let value = values
        .next()
        .ok_or(ResourceApplicationError::PreconditionRequired)?;
    if values.next().is_some() {
        return Err(ResourceApplicationError::Invalid);
    }
    let value = value
        .to_str()
        .map_err(|_| ResourceApplicationError::Invalid)?;
    let prefix = format!("\"{resource_id}-");
    let version = value
        .strip_prefix(&prefix)
        .and_then(|value| value.strip_suffix('"'))
        .ok_or(ResourceApplicationError::Invalid)?;
    if version.is_empty() || version.starts_with('0') {
        return Err(ResourceApplicationError::Invalid);
    }
    version
        .parse()
        .map_err(|_| ResourceApplicationError::Invalid)
}

fn create_request_digest(
    principal: &AuthenticatedPrincipal,
    resource_kind: RegistryResourceKind,
    draft: &ResourceDraftPayload,
    idempotency_key_digest: &Sha256Digest,
) -> Result<Sha256Digest, ResourceApplicationError> {
    digest(&serde_json::json!({
        "draft": draft,
        "idempotency_key_digest": idempotency_key_digest,
        "operation": "resource.create",
        "principal_id": principal.principal_id,
        "resource_kind": resource_kind,
        "schema_version": 1,
        "tenant_id": principal.tenant_id,
    }))
}

fn digest(value: &serde_json::Value) -> Result<Sha256Digest, ResourceApplicationError> {
    canonical_digest(value)
        .map_err(|_| ResourceApplicationError::Internal)?
        .parse()
        .map_err(|_| ResourceApplicationError::Internal)
}

pub fn resource_kind_for_noun(noun: &str) -> Option<RegistryResourceKind> {
    match noun {
        "agents" => Some(RegistryResourceKind::Agent),
        "skills" => Some(RegistryResourceKind::Skill),
        "capabilities" => Some(RegistryResourceKind::CapabilityInterface),
        "contexts" => Some(RegistryResourceKind::ContextSourceInterface),
        "models" => Some(RegistryResourceKind::ModelProfile),
        "mcp-servers" => Some(RegistryResourceKind::McpServer),
        "policies" => Some(RegistryResourceKind::Policy),
        "sandboxes" => Some(RegistryResourceKind::SandboxProfile),
        _ => None,
    }
}

pub fn resource_etag(resource_id: &ResourceId, version: u64) -> String {
    format!("\"{resource_id}-{version}\"")
}

pub fn resource_version_etag(
    resource_version_id: &ResourceId,
    content_digest: &Sha256Digest,
) -> String {
    format!(
        "\"{}-{}\"",
        resource_version_id,
        content_digest.as_str().trim_start_matches("sha256:")
    )
}

pub fn deployment_etag(deployment_id: &ResourceId, digest: &Sha256Digest) -> String {
    format!(
        "\"{}-{}\"",
        deployment_id,
        digest.as_str().trim_start_matches("sha256:")
    )
}

pub fn deployment_closure_digest(
    closure: &DeploymentClosure,
) -> Result<Sha256Digest, ResourceApplicationError> {
    let mut value =
        serde_json::to_value(closure).map_err(|_| ResourceApplicationError::Internal)?;
    value
        .as_object_mut()
        .ok_or(ResourceApplicationError::Internal)?
        .insert("schema_version".to_owned(), serde_json::json!(1));
    canonical_digest(&value)
        .map_err(|_| ResourceApplicationError::Internal)?
        .parse()
        .map_err(|_| ResourceApplicationError::Internal)
}

fn problem(error: ResourceApplicationError) -> Response {
    let (status, code, title, retryable) = match error {
        ResourceApplicationError::Invalid => (
            StatusCode::BAD_REQUEST,
            ApiProblemCode::InvalidRequest,
            "The resource request is invalid.",
            false,
        ),
        ResourceApplicationError::Unauthenticated => (
            StatusCode::UNAUTHORIZED,
            ApiProblemCode::Unauthenticated,
            "Authentication is required.",
            false,
        ),
        ResourceApplicationError::Denied => (
            StatusCode::FORBIDDEN,
            ApiProblemCode::PermissionDenied,
            "The resource operation is not permitted.",
            false,
        ),
        ResourceApplicationError::NotFound => (
            StatusCode::NOT_FOUND,
            ApiProblemCode::ResourceNotFound,
            "The resource route was not found.",
            false,
        ),
        ResourceApplicationError::Conflict => (
            StatusCode::PRECONDITION_FAILED,
            ApiProblemCode::PreconditionFailed,
            "The resource changed concurrently.",
            false,
        ),
        ResourceApplicationError::PreconditionRequired => (
            StatusCode::PRECONDITION_REQUIRED,
            ApiProblemCode::PreconditionRequired,
            "A strong If-Match precondition is required.",
            false,
        ),
        ResourceApplicationError::IdempotencyConflict => (
            StatusCode::CONFLICT,
            ApiProblemCode::IdempotencyConflict,
            "The idempotency key was used for a different request.",
            false,
        ),
        ResourceApplicationError::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            ApiProblemCode::TemporarilyUnavailable,
            "The resource service is temporarily unavailable.",
            true,
        ),
        ResourceApplicationError::Internal => (
            StatusCode::INTERNAL_SERVER_ERROR,
            ApiProblemCode::InternalError,
            "The resource response could not be projected safely.",
            false,
        ),
    };
    let request_id = ResourceId::from_uuid_v7(ResourceKind::ServerRequest, uuid::Uuid::now_v7())
        .expect("UUID v7 generator must produce a valid server request identity");
    let body = ApiProblem {
        type_uri: format!("https://insight.platform/problems/{}", code.as_str()),
        title: title.to_owned(),
        status: status.as_u16(),
        code,
        detail: None,
        request_id,
        retryable,
        retry_after_ms: retryable.then_some(1_000),
        field_errors: Vec::new(),
    };
    debug_assert!(body.validate(MAX_SAFE_TEXT_BYTES, MAX_FIELD_ERRORS).is_ok());
    let mut response = (status, Json(body)).into_response();
    if retryable {
        response
            .headers_mut()
            .insert("retry-after", HeaderValue::from_static("1"));
    }
    no_store(response)
}

fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::to_bytes, http::Request};
    use insight_platform_contracts::{
        operation_etag, AgentDeploymentClosure, ArtifactRef, AuthnStrength, AuthoringPackage,
        DataClassification, ExactVersionRef, Permission, PermissionSet, PolicyKind,
        PolicyResourceSpec, PrincipalKind, PublicJobKind, PublicJobState, PublicJobTarget,
        UtcTimestamp, ValidationSummary,
    };
    use std::sync::Mutex;
    use tower::ServiceExt;

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1cc-32e4-75e1-a9e8-d95ca0f8{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn fixed_digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn principal(now: DateTime<Utc>) -> AuthenticatedPrincipal {
        AuthenticatedPrincipal {
            tenant_id: id(ResourceKind::Tenant, 1),
            principal_id: id(ResourceKind::Principal, 2),
            principal_kind: PrincipalKind::TenantAdmin,
            permissions: PermissionSet::new(vec![Permission::PolicyWrite]).unwrap(),
            authn_strength: AuthnStrength::MultiFactor,
            principal_version: 1,
            binding_generation: 1,
            binding_version: 1,
            credential_digest: fixed_digest('a'),
            credential_expires_at: now + Duration::hours(1),
        }
    }

    fn request_body() -> CreateResourceRequestV1 {
        let artifact = ArtifactRef::new(
            id(ResourceKind::Artifact, 3),
            fixed_digest('b'),
            128,
            "application/json",
            DataClassification::Internal,
            Some("policy.json".to_owned()),
        )
        .unwrap();
        CreateResourceRequestV1 {
            display_name: "Runtime protocol policy".to_owned(),
            document: ResourceDocument::Policy(PolicyResourceSpec {
                authoring_package: AuthoringPackage {
                    artifact,
                    manifest_digest: fixed_digest('c'),
                },
                contract_digest: fixed_digest('d'),
                dependency_versions: Vec::new(),
                policy_versions: Vec::new(),
                policy_kind: PolicyKind::Protocol,
                rules_digest: fixed_digest('e'),
                scheduling: None,
                retention: None,
                mcp_protocol: None,
                mcp_auth: None,
                sandbox_isolation: None,
                sandbox_resource: None,
                sandbox_network: None,
                sandbox_artifact_io: None,
                sandbox_secret_resolution: None,
            }),
        }
    }

    struct FixedClock(DateTime<Utc>);

    impl ResourceClock for FixedClock {
        fn now(&self) -> DateTime<Utc> {
            self.0
        }
    }

    struct FixtureApplication {
        intents: Mutex<Vec<CreateResourceIntent>>,
    }

    #[async_trait]
    impl ResourceApplication for FixtureApplication {
        async fn create_resource(
            &self,
            intent: CreateResourceIntent,
        ) -> Result<ResourceViewV1, ResourceApplicationError> {
            self.intents.lock().unwrap().push(intent.clone());
            let resource_id = id(ResourceKind::Policy, 4);
            Ok(ResourceViewV1 {
                schema_version: 1,
                resource_id: resource_id.clone(),
                resource_kind: intent.resource_kind,
                lifecycle_state: EntityLifecycle::Active,
                gate_state: AdministrativeGate::Enabled,
                draft_generation: 1,
                version: 1,
                draft: intent.draft,
                etag: resource_etag(&resource_id, 1),
            })
        }

        async fn read_resource(
            &self,
            intent: ReadResourceIntent,
        ) -> Result<ResourceViewV1, ResourceApplicationError> {
            let draft = ResourceDraftPayload {
                display_name: request_body().display_name,
                document: request_body().document,
                validation: None,
            };
            Ok(ResourceViewV1 {
                schema_version: 1,
                resource_id: intent.resource_id.clone(),
                resource_kind: intent.resource_kind,
                lifecycle_state: EntityLifecycle::Active,
                gate_state: AdministrativeGate::Enabled,
                draft_generation: 1,
                version: 3,
                draft,
                etag: resource_etag(&intent.resource_id, 3),
            })
        }

        async fn update_resource_draft(
            &self,
            intent: UpdateResourceDraftIntent,
        ) -> Result<ResourceViewV1, ResourceApplicationError> {
            self.intents.lock().unwrap().push(CreateResourceIntent {
                principal: intent.principal,
                resource_kind: intent.resource_kind,
                draft: intent.draft.clone(),
                idempotency_key_digest: intent.idempotency_key_digest,
                request_digest: intent.request_digest,
                deadline: intent.deadline,
            });
            Ok(ResourceViewV1 {
                schema_version: 1,
                resource_id: intent.resource_id.clone(),
                resource_kind: intent.resource_kind,
                lifecycle_state: EntityLifecycle::Active,
                gate_state: AdministrativeGate::Enabled,
                draft_generation: 2,
                version: intent.expected_resource_version + 1,
                draft: intent.draft,
                etag: resource_etag(&intent.resource_id, intent.expected_resource_version + 1),
            })
        }

        async fn validate_resource_draft(
            &self,
            intent: ValidateResourceDraftIntent,
        ) -> Result<OperationViewV1, ResourceApplicationError> {
            let operation_id = id(ResourceKind::Job, 8);
            let now = UtcTimestamp::from_datetime(Utc::now());
            Ok(OperationViewV1 {
                operation_id: operation_id.clone(),
                tenant_id: intent.principal.tenant_id,
                kind: PublicJobKind::ResourceValidation,
                target: PublicJobTarget::ResourceVersion {
                    resource_id: intent.resource_id,
                    resource_version: intent.expected_resource_version,
                },
                state: PublicJobState::Queued,
                progress: None,
                result: None,
                error: None,
                created_at: now.clone(),
                updated_at: now,
                etag: operation_etag(&operation_id.to_string(), 1),
            })
        }

        async fn read_resource_version(
            &self,
            intent: ReadResourceVersionIntent,
        ) -> Result<ResourceVersionViewV1, ResourceApplicationError> {
            let content_digest = fixed_digest('9');
            Ok(ResourceVersionViewV1 {
                schema_version: 1,
                resource_id: intent.resource_id,
                resource_kind: intent.resource_kind,
                resource_version_id: intent.resource_version_id.clone(),
                revision_no: 2,
                content_digest: content_digest.clone(),
                artifact_id: None,
                payload: PublishedVersionPayload {
                    document: request_body().document,
                    validation: ValidationSummary {
                        validator_digest: fixed_digest('4'),
                        validated_draft_digest: fixed_digest('5'),
                        dependency_closure_digest: fixed_digest('6'),
                        security_evidence_digest: fixed_digest('7'),
                        warnings: Vec::new(),
                    },
                },
                created_at: UtcTimestamp::from_datetime(Utc::now()),
                etag: resource_version_etag(&intent.resource_version_id, &content_digest),
            })
        }

        async fn read_deployment(
            &self,
            intent: ReadDeploymentIntent,
        ) -> Result<DeploymentViewV1, ResourceApplicationError> {
            if intent.resource_kind != RegistryResourceKind::Agent {
                return Err(ResourceApplicationError::NotFound);
            }
            let closure = DeploymentClosure::Agent(AgentDeploymentClosure {
                interface: ExactVersionRef::new(
                    id(ResourceKind::AgentInterfaceRevision, 21),
                    fixed_digest('1'),
                )
                .unwrap(),
                plan: ExactVersionRef::new(
                    id(ResourceKind::AgentPlanRevision, 22),
                    fixed_digest('2'),
                )
                .unwrap(),
                slots: Vec::new(),
                policies: Vec::new(),
                execution_profile: ExactVersionRef::new(
                    id(ResourceKind::PolicyRevision, 23),
                    fixed_digest('3'),
                )
                .unwrap(),
            });
            let closure_digest = deployment_closure_digest(&closure).unwrap();
            Ok(DeploymentViewV1 {
                schema_version: 1,
                deployment_id: intent.deployment_id.clone(),
                resource_id: intent.resource_id,
                resource_kind: intent.resource_kind,
                resource_version_id: id(ResourceKind::AgentPlanRevision, 22),
                environment: "test".to_owned(),
                closure_digest: closure_digest.clone(),
                closure,
                created_at: UtcTimestamp::from_datetime(Utc::now()),
                etag: deployment_etag(&intent.deployment_id, &closure_digest),
            })
        }

        async fn create_deployment(
            &self,
            intent: CreateDeploymentIntent,
        ) -> Result<DeploymentViewV1, ResourceApplicationError> {
            let deployment_id = id(ResourceKind::AgentDeployment, 24);
            let closure_digest = deployment_closure_digest(&intent.request.closure)?;
            Ok(DeploymentViewV1 {
                schema_version: 1,
                deployment_id: deployment_id.clone(),
                resource_id: intent.resource_id,
                resource_kind: intent.resource_kind,
                resource_version_id: intent.request.resource_version_id,
                environment: intent.request.environment,
                closure_digest: closure_digest.clone(),
                closure: intent.request.closure,
                created_at: UtcTimestamp::from_datetime(Utc::now()),
                etag: deployment_etag(&deployment_id, &closure_digest),
            })
        }

        async fn publish_resource_draft(
            &self,
            intent: PublishResourceDraftIntent,
        ) -> Result<PublishResourceDraftResponseV1, ResourceApplicationError> {
            let (revision_no, content_digest, artifact_id) = match intent.request {
                PublishResourceDraftRequestV1::Single {
                    revision_no,
                    content_digest,
                    artifact_id,
                } => (revision_no, content_digest, artifact_id),
                PublishResourceDraftRequestV1::Agent { .. } => {
                    return Err(ResourceApplicationError::Invalid)
                }
            };
            let resource_version_id = id(ResourceKind::PolicyRevision, 11);
            Ok(PublishResourceDraftResponseV1 {
                schema_version: 1,
                resource_id: intent.resource_id.clone(),
                resource_kind: intent.resource_kind,
                draft_generation: 1,
                version: intent.expected_resource_version + 1,
                published_versions: vec![PublishedResourceVersionSummaryV1 {
                    resource_version_id: resource_version_id.clone(),
                    revision_no,
                    content_digest: content_digest.clone(),
                    artifact_id,
                    etag: resource_version_etag(&resource_version_id, &content_digest),
                }],
                etag: resource_etag(&intent.resource_id, intent.expected_resource_version + 1),
            })
        }
    }

    #[tokio::test]
    async fn create_requires_idempotency_and_returns_stable_location_and_etag() {
        let now = Utc::now();
        let application = Arc::new(FixtureApplication {
            intents: Mutex::new(Vec::new()),
        });
        let router = build_resource_router(ResourceHttpState::new(
            application.clone(),
            Arc::new(FixedClock(now)),
        ));
        let body = serde_json::to_vec(&request_body()).unwrap();
        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/policies")
                    .header("content-type", "application/json")
                    .header(IDEMPOTENCY_KEY, "create-policy-1")
                    .extension(principal(now))
                    .body(axum::body::Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
        assert_eq!(
            response.headers()["location"],
            "/v1/policies/pol_0198f1cc-32e4-75e1-a9e8-d95ca0f80004"
        );
        assert_eq!(
            response.headers()["etag"],
            "\"pol_0198f1cc-32e4-75e1-a9e8-d95ca0f80004-1\""
        );
        let intents = application.intents.lock().unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].resource_kind, RegistryResourceKind::Policy);
    }

    #[tokio::test]
    async fn wrong_route_kind_and_missing_idempotency_fail_before_application() {
        let now = Utc::now();
        let application = Arc::new(FixtureApplication {
            intents: Mutex::new(Vec::new()),
        });
        let router = build_resource_router(ResourceHttpState::new(
            application.clone(),
            Arc::new(FixedClock(now)),
        ));
        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/agents")
                    .header("content-type", "application/json")
                    .extension(principal(now))
                    .body(axum::body::Body::from(
                        serde_json::to_vec(&request_body()).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), 65_536).await.unwrap();
        assert!(!String::from_utf8_lossy(&body).contains("policy.json"));
        assert!(application.intents.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn read_checks_nominal_route_identity_and_returns_current_etag() {
        let now = Utc::now();
        let application = Arc::new(FixtureApplication {
            intents: Mutex::new(Vec::new()),
        });
        let router = build_resource_router(ResourceHttpState::new(
            application,
            Arc::new(FixedClock(now)),
        ));
        let resource_id = id(ResourceKind::Policy, 4);
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/policies/{resource_id}"))
                    .extension(principal(now))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
        assert_eq!(response.headers()["etag"], format!("\"{resource_id}-3\""));

        let wrong_kind = router
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/agents/{resource_id}"))
                    .extension(principal(now))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(wrong_kind.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn draft_update_requires_exact_strong_etag_and_scoped_idempotency() {
        let now = Utc::now();
        let application = Arc::new(FixtureApplication {
            intents: Mutex::new(Vec::new()),
        });
        let router = build_resource_router(ResourceHttpState::new(
            application.clone(),
            Arc::new(FixedClock(now)),
        ));
        let resource_id = id(ResourceKind::Policy, 4);
        let body = serde_json::to_vec(&request_body()).unwrap();
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/policies/{resource_id}/draft"))
                    .header("content-type", "application/json")
                    .header(IDEMPOTENCY_KEY, "update-policy-1")
                    .header(IF_MATCH, resource_etag(&resource_id, 3))
                    .extension(principal(now))
                    .body(axum::body::Body::from(body.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["etag"], resource_etag(&resource_id, 4));
        assert_eq!(application.intents.lock().unwrap().len(), 1);

        let weak = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/policies/{resource_id}/draft"))
                    .header("content-type", "application/json")
                    .header(IDEMPOTENCY_KEY, "update-policy-2")
                    .header(IF_MATCH, format!("W/{}", resource_etag(&resource_id, 4)))
                    .extension(principal(now))
                    .body(axum::body::Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(weak.status(), StatusCode::BAD_REQUEST);
        assert_eq!(application.intents.lock().unwrap().len(), 1);

        let missing = router
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/policies/{resource_id}/draft"))
                    .header("content-type", "application/json")
                    .header(IDEMPOTENCY_KEY, "update-policy-3")
                    .extension(principal(now))
                    .body(axum::body::Body::from(
                        serde_json::to_vec(&request_body()).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::PRECONDITION_REQUIRED);
        assert_eq!(application.intents.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn draft_validation_returns_job_operation_and_rejects_a_body() {
        let now = Utc::now();
        let application = Arc::new(FixtureApplication {
            intents: Mutex::new(Vec::new()),
        });
        let router = build_resource_router(ResourceHttpState::new(
            application,
            Arc::new(FixedClock(now)),
        ));
        let resource_id = id(ResourceKind::Policy, 4);
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/policies/{resource_id}/draft:validate"))
                    .header(IDEMPOTENCY_KEY, "validate-policy-1")
                    .header(IF_MATCH, resource_etag(&resource_id, 3))
                    .extension(principal(now))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(
            response.headers()["location"],
            "/v1/operations/job_0198f1cc-32e4-75e1-a9e8-d95ca0f80008"
        );

        let with_body = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/policies/{resource_id}/draft:validate"))
                    .header(IDEMPOTENCY_KEY, "validate-policy-2")
                    .header(IF_MATCH, resource_etag(&resource_id, 3))
                    .extension(principal(now))
                    .body(axum::body::Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(with_body.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn published_version_read_is_nominal_and_immutable() {
        let now = Utc::now();
        let application = Arc::new(FixtureApplication {
            intents: Mutex::new(Vec::new()),
        });
        let router = build_resource_router(ResourceHttpState::new(
            application,
            Arc::new(FixedClock(now)),
        ));
        let resource_id = id(ResourceKind::Policy, 4);
        let version_id = id(ResourceKind::PolicyRevision, 9);
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/policies/{resource_id}/versions/{version_id}"))
                    .extension(principal(now))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
        assert_eq!(
            response.headers()["etag"],
            resource_version_etag(&version_id, &fixed_digest('9'))
        );

        let wrong_version_kind = router
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/policies/{resource_id}/versions/{}",
                        id(ResourceKind::AgentPlanRevision, 10)
                    ))
                    .extension(principal(now))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(wrong_version_kind.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn deployment_read_is_parent_scoped_nominal_and_immutable() {
        let now = Utc::now();
        let application = Arc::new(FixtureApplication {
            intents: Mutex::new(Vec::new()),
        });
        let router = build_resource_router(ResourceHttpState::new(
            application,
            Arc::new(FixedClock(now)),
        ));
        let resource_id = id(ResourceKind::Agent, 20);
        let deployment_id = id(ResourceKind::AgentDeployment, 24);
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/agents/{resource_id}/deployments/{deployment_id}"
                    ))
                    .extension(principal(now))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
        let body = to_bytes(response.into_body(), MAX_RESOURCE_REQUEST_BYTES)
            .await
            .unwrap();
        let view: DeploymentViewV1 = serde_json::from_slice(&body).unwrap();
        assert_eq!(view.deployment_id, deployment_id);
        assert_eq!(
            view.etag,
            deployment_etag(&deployment_id, &view.closure_digest)
        );

        let wrong_deployment_kind = router
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/v1/agents/{resource_id}/deployments/{}",
                        id(ResourceKind::ModelDeployment, 25)
                    ))
                    .extension(principal(now))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(wrong_deployment_kind.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn deployment_create_is_closed_fenced_and_returns_immutable_location() {
        let now = Utc::now();
        let application = Arc::new(FixtureApplication {
            intents: Mutex::new(Vec::new()),
        });
        let router = build_resource_router(ResourceHttpState::new(
            application,
            Arc::new(FixedClock(now)),
        ));
        let resource_id = id(ResourceKind::Agent, 20);
        let request = CreateDeploymentRequestV1 {
            resource_version_id: id(ResourceKind::AgentPlanRevision, 22),
            environment: "test".to_owned(),
            closure: DeploymentClosure::Agent(AgentDeploymentClosure {
                interface: ExactVersionRef::new(
                    id(ResourceKind::AgentInterfaceRevision, 21),
                    fixed_digest('1'),
                )
                .unwrap(),
                plan: ExactVersionRef::new(
                    id(ResourceKind::AgentPlanRevision, 22),
                    fixed_digest('2'),
                )
                .unwrap(),
                slots: Vec::new(),
                policies: Vec::new(),
                execution_profile: ExactVersionRef::new(
                    id(ResourceKind::PolicyRevision, 23),
                    fixed_digest('3'),
                )
                .unwrap(),
            }),
        };
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/agents/{resource_id}/deployments"))
                    .header("content-type", "application/json")
                    .header(IDEMPOTENCY_KEY, "deploy-agent-1")
                    .header(IF_MATCH, resource_etag(&resource_id, 3))
                    .extension(principal(now))
                    .body(axum::body::Body::from(
                        serde_json::to_vec(&request).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            response.headers()["location"],
            format!(
                "/v1/agents/{resource_id}/deployments/{}",
                id(ResourceKind::AgentDeployment, 24)
            )
        );

        let missing_fence = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/agents/{resource_id}/deployments"))
                    .header("content-type", "application/json")
                    .header(IDEMPOTENCY_KEY, "deploy-agent-2")
                    .extension(principal(now))
                    .body(axum::body::Body::from(
                        serde_json::to_vec(&request).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_fence.status(), StatusCode::PRECONDITION_REQUIRED);
    }

    #[tokio::test]
    async fn draft_publish_is_closed_fenced_and_returns_version_summaries() {
        let now = Utc::now();
        let application = Arc::new(FixtureApplication {
            intents: Mutex::new(Vec::new()),
        });
        let router = build_resource_router(ResourceHttpState::new(
            application,
            Arc::new(FixedClock(now)),
        ));
        let resource_id = id(ResourceKind::Policy, 4);
        let request = PublishResourceDraftRequestV1::Single {
            revision_no: 2,
            content_digest: fixed_digest('9'),
            artifact_id: None,
        };
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/policies/{resource_id}/draft:publish"))
                    .header("content-type", "application/json")
                    .header(IDEMPOTENCY_KEY, "publish-policy-1")
                    .header(IF_MATCH, resource_etag(&resource_id, 3))
                    .extension(principal(now))
                    .body(axum::body::Body::from(
                        serde_json::to_vec(&request).unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["etag"], resource_etag(&resource_id, 4));

        let wrong_shape = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/policies/{resource_id}/draft:publish"))
                    .header("content-type", "application/json")
                    .header(IDEMPOTENCY_KEY, "publish-policy-2")
                    .header(IF_MATCH, resource_etag(&resource_id, 3))
                    .extension(principal(now))
                    .body(axum::body::Body::from(
                        serde_json::to_vec(&PublishResourceDraftRequestV1::Agent {
                            revision_no: 2,
                            interface_content_digest: fixed_digest('8'),
                            plan_content_digest: fixed_digest('9'),
                            artifact_id: None,
                        })
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(wrong_shape.status(), StatusCode::BAD_REQUEST);
    }
}
