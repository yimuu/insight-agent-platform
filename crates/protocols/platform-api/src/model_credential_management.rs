//! Safe metadata and current CAS revocation for model credentials.
use insight_platform_contracts::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelCredentialMetadataViewV1 {
    pub schema_version: u16,
    pub tenant_id: ResourceId,
    pub secret_binding_id: ResourceId,
    pub provider_id: ResourceId,
    pub purpose: SecretPurpose,
    pub state: SecretBindingState,
    pub generation: u64,
    pub version: u64,
    pub etag: String,
}
impl ModelCredentialMetadataViewV1 {
    pub fn validate(&self) -> bool {
        self.schema_version == 1
            && self.tenant_id.kind() == ResourceKind::Tenant
            && self.secret_binding_id.kind() == ResourceKind::SecretBinding
            && self.provider_id.kind() == ResourceKind::SecretProvider
            && self.purpose.as_str() == MODEL_API_KEY_PURPOSE
            && self.generation > 0
            && self.generation <= i64::MAX as u64
            && self.version > 0
            && self.version <= i64::MAX as u64
            && self.etag == crate::resource::resource_etag(&self.secret_binding_id, self.version)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RevokeModelCredentialRequestV1 {
    pub schema_version: u16,
    pub expected_generation: u64,
}
impl RevokeModelCredentialRequestV1 {
    pub fn validate(&self) -> bool {
        self.schema_version == 1
            && self.expected_generation > 0
            && self.expected_generation <= i64::MAX as u64
    }
}

use crate::{
    authentication::{AuthenticatedPrincipal, AuthenticationClock},
    resource::{
        expected_resource_version, idempotency_key_digest_for_operation, problem,
        ResourceApplicationError as Failure,
    },
};
use axum::{
    extract::{Path, Request, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Extension, Json, Router,
};
use std::sync::Arc;
#[derive(Debug, Clone)]
pub struct ModelCredentialReadIntent {
    pub principal: AuthenticatedPrincipal,
    pub secret_binding_id: ResourceId,
    pub deadline: chrono::DateTime<chrono::Utc>,
}
#[derive(Debug, Clone)]
pub struct ModelCredentialRevokeIntent {
    pub read: ModelCredentialReadIntent,
    pub expected_generation: u64,
    pub expected_version: u64,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
}
#[async_trait::async_trait]
pub trait ModelCredentialManagementApplication: Send + Sync {
    async fn read(
        &self,
        intent: ModelCredentialReadIntent,
    ) -> Result<ModelCredentialMetadataViewV1, Failure>;
    async fn revoke(
        &self,
        intent: ModelCredentialRevokeIntent,
    ) -> Result<ModelCredentialMetadataViewV1, Failure>;
}
#[derive(Clone)]
pub struct ModelCredentialManagementHttpState {
    application: Arc<dyn ModelCredentialManagementApplication>,
    clock: Arc<dyn AuthenticationClock>,
}
impl ModelCredentialManagementHttpState {
    pub fn new(
        application: Arc<dyn ModelCredentialManagementApplication>,
        clock: Arc<dyn AuthenticationClock>,
    ) -> Self {
        Self { application, clock }
    }
}
pub fn build_model_credential_management_router(
    state: ModelCredentialManagementHttpState,
) -> Router {
    // One path parameter owns both forms; suffix parsing remains exact and rejects all others.
    Router::new()
        .route("/v1/model-credentials/{binding}", get(read).post(revoke))
        .with_state(state)
}
fn parse_id(text: &str) -> Result<ResourceId, Failure> {
    ResourceId::parse_expected(text, ResourceKind::SecretBinding).map_err(|_| Failure::Invalid)
}
fn response(
    result: Result<ModelCredentialMetadataViewV1, Failure>,
    binding: &ResourceId,
    tenant: &ResourceId,
) -> Response {
    match result {
        Ok(v) if v.validate() && &v.secret_binding_id == binding && &v.tenant_id == tenant => {
            let Ok(etag) = HeaderValue::from_str(&v.etag) else {
                return problem(Failure::Internal);
            };
            let mut r = (StatusCode::OK, Json(v)).into_response();
            r.headers_mut().insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static("no-store, private, max-age=0"),
            );
            r.headers_mut().insert(header::ETAG, etag);
            r
        }
        Ok(_) => problem(Failure::Internal),
        Err(e) => problem(e),
    }
}
async fn read(
    State(state): State<ModelCredentialManagementHttpState>,
    Path(binding): Path<String>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(Failure::Unauthenticated);
    };
    let tenant = principal.tenant_id.clone();
    let now = state.clock.now();
    if principal.validate().is_err() || principal.credential_expires_at <= now {
        return problem(Failure::Unauthenticated);
    }
    if !principal.permissions.contains(Permission::SecretBind)
        && !principal.permissions.contains(Permission::SecretRevoke)
    {
        return problem(Failure::Denied);
    }
    let Ok(binding) = parse_id(&binding) else {
        return problem(Failure::Invalid);
    };
    let deadline = (now + chrono::Duration::seconds(30)).min(principal.credential_expires_at);
    let result = tokio::time::timeout(
        (deadline - now).to_std().unwrap(),
        state.application.read(ModelCredentialReadIntent {
            principal,
            secret_binding_id: binding.clone(),
            deadline,
        }),
    )
    .await
    .unwrap_or(Err(Failure::Unavailable));
    response(result, &binding, &tenant)
}
async fn revoke(
    State(state): State<ModelCredentialManagementHttpState>,
    Path(binding): Path<String>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    request: Request,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(Failure::Unauthenticated);
    };
    let tenant = principal.tenant_id.clone();
    let now = state.clock.now();
    if principal.validate().is_err() || principal.credential_expires_at <= now {
        return problem(Failure::Unauthenticated);
    }
    if !principal.permissions.contains(Permission::SecretRevoke) {
        return problem(Failure::Denied);
    }
    let Some(binding) = binding.strip_suffix(":revoke") else {
        return problem(Failure::Invalid);
    };
    let Ok(binding) = parse_id(binding) else {
        return problem(Failure::Invalid);
    };
    let deadline = (now + chrono::Duration::seconds(30)).min(principal.credential_expires_at);
    let operation = async {
        if request.headers().contains_key(header::CONTENT_ENCODING) {
            return Err(Failure::Invalid);
        }
        let version = expected_resource_version(request.headers(), &binding)?;
        if version > i64::MAX as u64 {
            return Err(Failure::Invalid);
        }
        let key = idempotency_key_digest_for_operation(
            request.headers(),
            &principal,
            RegistryResourceKind::ModelProvider,
            "model.credential.revoke",
            Some(&binding),
        )?;
        let body = axum::body::to_bytes(request.into_body(), 1024)
            .await
            .map_err(|_| Failure::Invalid)?;
        let value = parse_strict_json(
            &body,
            JsonLimits {
                max_bytes: 1024,
                max_depth: 2,
                max_properties_per_object: 2,
                max_items_per_array: 1,
                max_string_bytes: 64,
            },
        )
        .map_err(|_| Failure::Invalid)?;
        let body: RevokeModelCredentialRequestV1 =
            serde_json::from_value(value).map_err(|_| Failure::Invalid)?;
        if !body.validate() {
            return Err(Failure::Invalid);
        }
        let digest=canonical_digest(&serde_json::json!({"schema_version":1,"operation":"model.credential.revoke","tenant_id":principal.tenant_id,"principal_id":principal.principal_id,"secret_binding_id":binding,"expected_generation":body.expected_generation,"expected_version":version,"idempotency_key_digest":key})).map_err(|_|Failure::Invalid)?.parse().map_err(|_|Failure::Invalid)?;
        state
            .application
            .revoke(ModelCredentialRevokeIntent {
                read: ModelCredentialReadIntent {
                    principal,
                    secret_binding_id: binding.clone(),
                    deadline,
                },
                expected_generation: body.expected_generation,
                expected_version: version,
                idempotency_key_digest: key,
                request_digest: digest,
            })
            .await
    };
    let result = tokio::time::timeout((deadline - now).to_std().unwrap(), operation)
        .await
        .unwrap_or(Err(Failure::Unavailable));
    response(result, &binding, &tenant)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authentication::SystemAuthenticationClock;
    use axum::{body::Body, http::Request};
    use std::sync::Mutex;
    use tower::ServiceExt;
    struct Application(Mutex<Vec<ModelCredentialRevokeIntent>>);
    fn id(kind: ResourceKind) -> ResourceId {
        ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
    }
    fn digest() -> Sha256Digest {
        format!("sha256:{}", "a".repeat(64)).parse().unwrap()
    }
    fn principal(permissions: Vec<Permission>) -> AuthenticatedPrincipal {
        AuthenticatedPrincipal {
            tenant_id: id(ResourceKind::Tenant),
            principal_id: id(ResourceKind::Principal),
            principal_kind: PrincipalKind::TenantAdmin,
            permissions: PermissionSet::new(permissions).unwrap(),
            authn_strength: AuthnStrength::MultiFactor,
            principal_version: 1,
            binding_generation: 1,
            binding_version: 1,
            credential_digest: digest(),
            credential_expires_at: chrono::Utc::now() + chrono::Duration::minutes(5),
            trace: TraceIdentityV1::generate(),
        }
    }
    fn view(i: &ModelCredentialReadIntent) -> ModelCredentialMetadataViewV1 {
        ModelCredentialMetadataViewV1 {
            schema_version: 1,
            tenant_id: i.principal.tenant_id.clone(),
            secret_binding_id: i.secret_binding_id.clone(),
            provider_id: id(ResourceKind::SecretProvider),
            purpose: MODEL_API_KEY_PURPOSE.parse().unwrap(),
            state: SecretBindingState::Active,
            generation: 3,
            version: 4,
            etag: crate::resource::resource_etag(&i.secret_binding_id, 4),
        }
    }
    #[async_trait::async_trait]
    impl ModelCredentialManagementApplication for Application {
        async fn read(
            &self,
            i: ModelCredentialReadIntent,
        ) -> Result<ModelCredentialMetadataViewV1, Failure> {
            Ok(view(&i))
        }
        async fn revoke(
            &self,
            i: ModelCredentialRevokeIntent,
        ) -> Result<ModelCredentialMetadataViewV1, Failure> {
            let mut v = view(&i.read);
            v.state = SecretBindingState::Revoked;
            self.0.lock().unwrap().push(i);
            Ok(v)
        }
    }
    #[tokio::test]
    async fn credential_route_preserves_tenant_permissions_etag_generation_and_receipt_binding() {
        let app = Arc::new(Application(Mutex::new(vec![])));
        let router =
            build_model_credential_management_router(ModelCredentialManagementHttpState::new(
                app.clone(),
                Arc::new(SystemAuthenticationClock),
            ));
        let binding = id(ResourceKind::SecretBinding);
        for permission in [Permission::SecretBind, Permission::SecretRevoke] {
            let p = principal(vec![permission]);
            let r = router
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/v1/model-credentials/{binding}"))
                        .extension(p.clone())
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(r.status(), StatusCode::OK);
            assert_eq!(r.headers()["cache-control"], "no-store, private, max-age=0");
            assert_eq!(
                r.headers()["etag"],
                crate::resource::resource_etag(&binding, 4)
            );
            let body: ModelCredentialMetadataViewV1 =
                serde_json::from_slice(&axum::body::to_bytes(r.into_body(), 4096).await.unwrap())
                    .unwrap();
            assert_eq!(body.tenant_id, p.tenant_id);
            assert!(body.validate());
        }
        let p = principal(vec![Permission::SecretRevoke]);
        let body = r#"{"schema_version":1,"expected_generation":3}"#;
        for (input, etag, status) in [
            (
                body,
                Some(crate::resource::resource_etag(&binding, 4)),
                StatusCode::OK,
            ),
            (body, None, StatusCode::PRECONDITION_REQUIRED),
            (
                r#"{"schema_version":1,"expected_generation":3,"expected_generation":3}"#,
                Some(crate::resource::resource_etag(&binding, 4)),
                StatusCode::BAD_REQUEST,
            ),
        ] {
            let mut req = Request::builder()
                .method("POST")
                .uri(format!("/v1/model-credentials/{binding}:revoke"))
                .extension(p.clone())
                .header("Idempotency-Key", "stable-revoke");
            if let Some(etag) = etag {
                req = req.header("If-Match", etag)
            }
            let r = router
                .clone()
                .oneshot(req.body(Body::from(input)).unwrap())
                .await
                .unwrap();
            assert_eq!(r.status(), status);
            if status == StatusCode::OK {
                assert_eq!(r.headers()["cache-control"], "no-store, private, max-age=0");
            }
        }
        let p = principal(vec![Permission::SecretBind]);
        let r = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/model-credentials/{binding}:revoke"))
                    .extension(p)
                    .header("If-Match", crate::resource::resource_etag(&binding, 4))
                    .header("Idempotency-Key", "stable-revoke")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::FORBIDDEN);
        let calls = app.0.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].expected_generation, 3);
        assert_eq!(calls[0].expected_version, 4);
    }
}
