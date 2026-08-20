use crate::authentication::AuthenticatedPrincipal;
use async_trait::async_trait;
use axum::{
    extract::{rejection::JsonRejection, Extension, Path, State},
    http::{header::CACHE_CONTROL, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{
    canonical_digest, AdministrativeGate, ApiProblem, ApiProblemCode, EntityLifecycle,
    RegistryResourceKind, ResourceDocument, ResourceDraftPayload, ResourceId, ResourceKind,
    Sha256Digest, MAX_FIELD_ERRORS, MAX_SAFE_TEXT_BYTES,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const IDEMPOTENCY_KEY: &str = "idempotency-key";
const RESOURCE_COMMAND_DEADLINE_MILLISECONDS: i64 = 5_000;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CreateResourceRequestV1 {
    pub display_name: String,
    pub document: ResourceDocument,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceApplicationError {
    Invalid,
    Unauthenticated,
    Denied,
    NotFound,
    Conflict,
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
        .with_state(state)
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

fn idempotency_key_digest(
    headers: &HeaderMap,
    principal: &AuthenticatedPrincipal,
    resource_kind: RegistryResourceKind,
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
        "operation": "resource.create",
        "principal_id": principal.principal_id,
        "resource_kind": resource_kind,
        "schema_version": 1,
        "tenant_id": principal.tenant_id,
    }))
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
            StatusCode::CONFLICT,
            ApiProblemCode::EtagMismatch,
            "The resource changed concurrently.",
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
        ArtifactRef, AuthnStrength, AuthoringPackage, DataClassification, Permission,
        PermissionSet, PolicyKind, PolicyResourceSpec, PrincipalKind,
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
}
