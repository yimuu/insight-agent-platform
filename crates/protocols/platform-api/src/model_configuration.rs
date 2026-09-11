//! Bounded previews of ordinary Registry documents; this API allocates no business identities.
use crate::authentication::{AuthenticatedPrincipal, AuthenticationClock};
use axum::{
    body::Bytes,
    extract::{RawQuery, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Extension, Json, Router,
};
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::*;
use insight_platform_registry::model_configuration::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelDestinationChoiceV1 {
    pub destination_digest: Sha256Digest,
    pub endpoint_identity_digest: Sha256Digest,
    pub base_url: String,
    pub protocol: ModelProviderWireProtocol,
    pub region: DataRegion,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConfigurationCatalogViewV1 {
    pub schema_version: u16,
    pub installation_digest: Sha256Digest,
    pub environment: String,
    pub secret_provider_id: ResourceId,
    pub destinations: Vec<ModelDestinationChoiceV1>,
    pub maximum_classification: DataClassification,
}
impl ModelConfigurationCatalogViewV1 {
    pub fn from_catalog(
        catalog: &ModelInstallationCatalogV1,
    ) -> Result<Self, ModelConfigurationApplicationError> {
        let invalid = || ModelConfigurationApplicationError::Unavailable;
        Ok(Self {
            schema_version: 1,
            installation_digest: catalog.canonical_digest().map_err(|_| invalid())?,
            environment: catalog.environment.clone(),
            secret_provider_id: catalog.secret_provider_id.clone(),
            destinations: catalog
                .destinations
                .iter()
                .map(|item| {
                    Ok(ModelDestinationChoiceV1 {
                        destination_digest: item.canonical_digest().map_err(|_| invalid())?,
                        endpoint_identity_digest: item.grant.endpoint_identity_digest.clone(),
                        base_url: format!(
                            "{}://{}:{}{}",
                            match item.grant.endpoint.scheme {
                                CapabilityEndpointScheme::Https => "https",
                                CapabilityEndpointScheme::Http => "http",
                            },
                            item.grant.endpoint.host,
                            item.grant.endpoint.port,
                            item.grant.endpoint.base_path
                        ),
                        protocol: item.grant.protocol,
                        region: item.grant.region.clone(),
                    })
                })
                .collect::<Result<_, ModelConfigurationApplicationError>>()?,
            maximum_classification: DataClassification::Internal,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclareModelConfigurationRequestV1 {
    pub schema_version: u16,
    pub installation_digest: Sha256Digest,
    pub input: ModelConfigurationInputV1,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompileModelConfigurationRequestV1 {
    pub schema_version: u16,
    pub installation_digest: Sha256Digest,
    pub input: ModelConfigurationInputV1,
    pub artifact: ArtifactRef,
}
#[derive(Debug, Clone)]
pub struct ModelConfigurationIntent {
    pub principal: AuthenticatedPrincipal,
    pub deadline: DateTime<Utc>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelConfigurationApplicationError {
    Invalid,
    Unauthenticated,
    Forbidden,
    Conflict,
    NotFound,
    Unavailable,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConfigurationResourceSummaryV1 {
    pub resource_id: ResourceId,
    pub resource_kind: RegistryResourceKind,
    pub alias: Option<ResourceAlias>,
    pub display_name: String,
    pub version: u64,
    pub etag: String,
    pub lifecycle_state: EntityLifecycle,
    pub gate_state: AdministrativeGate,
    pub active_deployment: Option<ExactDeploymentRef>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConfigurationResourcePageV1 {
    pub schema_version: u16,
    pub items: Vec<ModelConfigurationResourceSummaryV1>,
    pub next_after: Option<ResourceId>,
}
#[derive(Debug, Clone)]
pub struct ModelConfigurationResourceQueryV1 {
    pub kind: RegistryResourceKind,
    pub after: Option<ResourceId>,
}
#[async_trait::async_trait]
pub trait ModelConfigurationApplication: Send + Sync {
    async fn resources(
        &self,
        intent: ModelConfigurationIntent,
        query: ModelConfigurationResourceQueryV1,
    ) -> Result<ModelConfigurationResourcePageV1, ModelConfigurationApplicationError>;
    async fn catalog(
        &self,
        intent: ModelConfigurationIntent,
    ) -> Result<ModelConfigurationCatalogViewV1, ModelConfigurationApplicationError>;
    async fn declare(
        &self,
        intent: ModelConfigurationIntent,
        request: DeclareModelConfigurationRequestV1,
    ) -> Result<ModelConfigurationDeclarationV1, ModelConfigurationApplicationError>;
    async fn compile(
        &self,
        intent: ModelConfigurationIntent,
        request: CompileModelConfigurationRequestV1,
    ) -> Result<CompiledModelConfigurationV1, ModelConfigurationApplicationError>;
}
#[derive(Clone)]
pub struct ModelConfigurationHttpState {
    application: Arc<dyn ModelConfigurationApplication>,
    clock: Arc<dyn AuthenticationClock>,
}
impl ModelConfigurationHttpState {
    pub fn new(
        application: Arc<dyn ModelConfigurationApplication>,
        clock: Arc<dyn AuthenticationClock>,
    ) -> Self {
        Self { application, clock }
    }
    fn intent(
        &self,
        principal: Option<Extension<AuthenticatedPrincipal>>,
        write: bool,
    ) -> Result<ModelConfigurationIntent, ModelConfigurationApplicationError> {
        let principal = principal
            .ok_or(ModelConfigurationApplicationError::Unauthenticated)?
            .0;
        let now = self.clock.now();
        if principal.validate().is_err() || principal.credential_expires_at <= now {
            return Err(ModelConfigurationApplicationError::Unauthenticated);
        }
        if !principal.permissions.contains(if write {
            Permission::ModelWrite
        } else {
            Permission::ModelRead
        }) {
            return Err(ModelConfigurationApplicationError::Forbidden);
        }
        Ok(ModelConfigurationIntent {
            deadline: (now + Duration::seconds(5)).min(principal.credential_expires_at),
            principal,
        })
    }
}
pub fn build_model_configuration_router(state: ModelConfigurationHttpState) -> Router {
    Router::new()
        .route("/v1/model-configuration", get(catalog))
        .route("/v1/model-configuration/resources", get(resources))
        .route("/v1/model-configuration:declare", post(declare))
        .route("/v1/model-configuration:compile", post(compile))
        .layer(axum::extract::DefaultBodyLimit::max(16_384))
        .with_state(state)
}
fn resource_query(
    query: Option<String>,
) -> Result<ModelConfigurationResourceQueryV1, ModelConfigurationApplicationError> {
    let query = query.ok_or(ModelConfigurationApplicationError::Invalid)?;
    if query.len() > 512 {
        return Err(ModelConfigurationApplicationError::Invalid);
    }
    let mut fields = std::collections::BTreeMap::new();
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        if !matches!(key.as_ref(), "kind" | "after")
            || fields
                .insert(key.into_owned(), value.into_owned())
                .is_some()
        {
            return Err(ModelConfigurationApplicationError::Invalid);
        }
    }
    let kind = match fields.get("kind").map(String::as_str) {
        Some("model_provider") => RegistryResourceKind::ModelProvider,
        Some("model_profile") => RegistryResourceKind::ModelProfile,
        _ => return Err(ModelConfigurationApplicationError::Invalid),
    };
    let after = fields
        .get("after")
        .map(|value| {
            ResourceId::parse_expected(value, kind.id_kind())
                .map_err(|_| ModelConfigurationApplicationError::Invalid)
        })
        .transpose()?;
    Ok(ModelConfigurationResourceQueryV1 { kind, after })
}
async fn resources(
    State(state): State<ModelConfigurationHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    RawQuery(query): RawQuery,
) -> Response {
    let intent = match state.intent(principal, false) {
        Ok(v) => v,
        Err(e) => return problem(e),
    };
    let query = match resource_query(query) {
        Ok(v) => v,
        Err(e) => return problem(e),
    };
    respond(state.application.resources(intent, query).await)
}
async fn catalog(
    State(state): State<ModelConfigurationHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
) -> Response {
    let intent = match state.intent(principal, false) {
        Ok(v) => v,
        Err(e) => return problem(e),
    };
    respond(state.application.catalog(intent).await)
}
fn decode<T: serde::de::DeserializeOwned>(
    body: Bytes,
) -> Result<T, ModelConfigurationApplicationError> {
    let value = parse_strict_json(
        &body,
        JsonLimits {
            max_bytes: 16_384,
            max_depth: 10,
            max_items_per_array: 16,
            max_properties_per_object: 20,
            max_string_bytes: 4096,
        },
    )
    .map_err(|_| ModelConfigurationApplicationError::Invalid)?;
    serde_json::from_value(value).map_err(|_| ModelConfigurationApplicationError::Invalid)
}
async fn declare(
    State(state): State<ModelConfigurationHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    body: Bytes,
) -> Response {
    let intent = match state.intent(principal, true) {
        Ok(v) => v,
        Err(e) => return problem(e),
    };
    let request: DeclareModelConfigurationRequestV1 = match decode(body) {
        Ok(v) => v,
        Err(e) => return problem(e),
    };
    if request.schema_version != 1 {
        return problem(ModelConfigurationApplicationError::Invalid);
    }
    respond(state.application.declare(intent, request).await)
}
async fn compile(
    State(state): State<ModelConfigurationHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    body: Bytes,
) -> Response {
    let intent = match state.intent(principal, true) {
        Ok(v) => v,
        Err(e) => return problem(e),
    };
    let request: CompileModelConfigurationRequestV1 = match decode(body) {
        Ok(v) => v,
        Err(e) => return problem(e),
    };
    if request.schema_version != 1 || request.artifact.validate().is_err() {
        return problem(ModelConfigurationApplicationError::Invalid);
    }
    respond(state.application.compile(intent, request).await)
}
fn no_store(mut response: Response) -> Response {
    let value = "no-store, private, max-age=0";
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static(value));
    response
}
fn respond<T: Serialize>(value: Result<T, ModelConfigurationApplicationError>) -> Response {
    match value {
        Ok(value) => no_store(Json(value).into_response()),
        Err(error) => problem(error),
    }
}
fn problem(error: ModelConfigurationApplicationError) -> Response {
    use ModelConfigurationApplicationError::*;
    let (status, code, title) = match error {
        Invalid => (
            StatusCode::BAD_REQUEST,
            ApiProblemCode::InvalidRequest,
            "The model configuration request is invalid.",
        ),
        Unauthenticated => (
            StatusCode::UNAUTHORIZED,
            ApiProblemCode::Unauthenticated,
            "Authentication is required.",
        ),
        Forbidden => (
            StatusCode::FORBIDDEN,
            ApiProblemCode::PermissionDenied,
            "Current authority does not permit this model configuration.",
        ),
        Conflict => (
            StatusCode::CONFLICT,
            ApiProblemCode::InvalidStateTransition,
            "Model configuration facts have changed; reload current state.",
        ),
        NotFound => (
            StatusCode::NOT_FOUND,
            ApiProblemCode::ResourceNotFound,
            "The exact configuration dependency was not found.",
        ),
        Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            ApiProblemCode::TemporarilyUnavailable,
            "Model configuration is unavailable.",
        ),
    };
    let retryable = error == Unavailable;
    let value = ApiProblem {
        type_uri: format!("https://insight.platform/problems/{}", code.as_str()),
        title: title.to_owned(),
        status: status.as_u16(),
        code,
        detail: None,
        request_id: ResourceId::from_uuid_v7(ResourceKind::ServerRequest, uuid::Uuid::now_v7())
            .expect("request identity"),
        trace_id: crate::trace::current_trace_id(),
        retryable,
        retry_after_ms: retryable.then_some(1000),
        field_errors: vec![],
    };
    no_store((status, Json(value)).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tower::ServiceExt;
    struct Clock(DateTime<Utc>);
    impl AuthenticationClock for Clock {
        fn now(&self) -> DateTime<Utc> {
            self.0
        }
    }
    struct Application(AtomicUsize);
    #[async_trait::async_trait]
    impl ModelConfigurationApplication for Application {
        async fn resources(
            &self,
            _: ModelConfigurationIntent,
            _: ModelConfigurationResourceQueryV1,
        ) -> Result<ModelConfigurationResourcePageV1, ModelConfigurationApplicationError> {
            panic!("invalid list query reached application")
        }
        async fn catalog(
            &self,
            _: ModelConfigurationIntent,
        ) -> Result<ModelConfigurationCatalogViewV1, ModelConfigurationApplicationError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(ModelConfigurationApplicationError::Unavailable)
        }
        async fn declare(
            &self,
            intent: ModelConfigurationIntent,
            request: DeclareModelConfigurationRequestV1,
        ) -> Result<ModelConfigurationDeclarationV1, ModelConfigurationApplicationError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            assert!(intent.deadline <= intent.principal.credential_expires_at);
            assert!(matches!(request.input, ModelConfigurationInputV1::Model(_)));
            Err(ModelConfigurationApplicationError::Conflict)
        }
        async fn compile(
            &self,
            _: ModelConfigurationIntent,
            _: CompileModelConfigurationRequestV1,
        ) -> Result<CompiledModelConfigurationV1, ModelConfigurationApplicationError> {
            panic!("invalid compile input reached application")
        }
    }
    fn digest() -> Sha256Digest {
        format!("sha256:{}", "a".repeat(64)).parse().unwrap()
    }
    fn principal(now: DateTime<Utc>) -> AuthenticatedPrincipal {
        AuthenticatedPrincipal {
            tenant_id: "ten_0198f1c3-8f49-7c3e-b1f3-773c28367d10".parse().unwrap(),
            principal_id: "prn_0198f1c3-8f49-7c3e-b1f3-773c28367d12".parse().unwrap(),
            principal_kind: PrincipalKind::TenantAdmin,
            permissions: PermissionSet::new(vec![Permission::ModelRead, Permission::ModelWrite])
                .unwrap(),
            authn_strength: AuthnStrength::MultiFactor,
            principal_version: 1,
            binding_generation: 1,
            binding_version: 1,
            credential_digest: digest(),
            credential_expires_at: now + Duration::seconds(2),
            trace: TraceIdentityV1::generate(),
        }
    }
    fn request(now: DateTime<Utc>) -> serde_json::Value {
        serde_json::json!({"schema_version":1,"installation_digest":digest(),"input":{"kind":"model","configuration":{
            "schema_version":1,"alias":"work.model","display_name":"Work model","source":{"deployment_id":"mpdep_0198f1c3-8f49-7c3e-b1f3-773c28367d15","resource_kind":"model_provider_deployment","deployment_digest":digest()},
            "model":"example-model","maximum_input_tokens":8192,"maximum_output_tokens":1024,"declared_at":now}}})
    }
    #[test]
    fn model_resource_query_is_closed_and_kind_bound() {
        assert!(resource_query(Some("kind=model_provider".to_owned())).is_ok());
        for query in [
            "",
            "kind=agent",
            "kind=model_provider&kind=model_profile",
            "kind=model_provider&limit=1000",
            "kind=model_provider&after=",
            "kind=model_provider&after=mod_0198f1c3-8f49-7c3e-b1f3-773c28367d15",
        ] {
            assert!(resource_query(Some(query.to_owned())).is_err(), "{query}");
        }
    }
    #[test]
    fn all_success_queries_preserve_the_public_client_private_cache_contract() {
        let response = respond(Ok::<_, ModelConfigurationApplicationError>(
            ModelConfigurationResourcePageV1 {
                schema_version: 1,
                items: vec![],
                next_after: None,
            },
        ));
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CACHE_CONTROL],
            "no-store, private, max-age=0"
        );
    }
    #[tokio::test]
    async fn configuration_transport_is_closed_authenticated_and_non_cacheable() {
        let now = Utc::now();
        let app = Arc::new(Application(AtomicUsize::new(0)));
        let router = build_model_configuration_router(ModelConfigurationHttpState::new(
            app.clone(),
            Arc::new(Clock(now)),
        ));
        let response = router
            .clone()
            .oneshot(
                Request::get("/v1/model-configuration")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response.headers()[header::CACHE_CONTROL],
            "no-store, private, max-age=0"
        );
        let authenticated = router.clone().layer(Extension(principal(now)));
        let body = request(now).to_string();
        for invalid in [
            body.replace(
                "\"schema_version\":1",
                "\"schema_version\":1,\"schema_version\":1",
            ),
            body.replace("\"alias\":\"work.model\"", "\"alias\":\"Work model\""),
            body.replace("\"kind\":\"model\"", "\"kind\":\"provider\""),
            body.replace("\"schema_version\":1", "\"schema_version\":2"),
            "{\"api_key\":\"must-never-be-accepted\"}".to_owned(),
        ] {
            let response = authenticated
                .clone()
                .oneshot(
                    Request::post("/v1/model-configuration:declare")
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(invalid))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            assert_eq!(
                response.headers()[header::CACHE_CONTROL],
                "no-store, private, max-age=0"
            );
        }
        assert_eq!(app.0.load(Ordering::SeqCst), 0);
        let response = authenticated
            .oneshot(
                Request::post("/v1/model-configuration:declare")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
        assert_eq!(
            response.headers()[header::CACHE_CONTROL],
            "no-store, private, max-age=0"
        );
        let bytes = to_bytes(response.into_body(), 4096).await.unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(!text.contains("example-model"));
        assert!(!text.contains("configuration\":"));
        assert_eq!(app.0.load(Ordering::SeqCst), 1);
        let mut expired = principal(now);
        expired.credential_expires_at = now;
        let response = router
            .layer(Extension(expired))
            .oneshot(
                Request::get("/v1/model-configuration")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(app.0.load(Ordering::SeqCst), 1);
    }
}
