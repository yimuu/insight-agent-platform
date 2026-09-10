//! One bounded, no-store credential import. Caller identity comes only from authentication.
use crate::authentication::{AuthenticatedPrincipal, AuthenticationClock};
use axum::{
    body::Bytes,
    extract::{Request, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Extension, Json, Router,
};
use chrono::Duration;
use insight_platform_contracts::{
    ApiProblem, ApiProblemCode, ExactSecretBindingRef, ModelCredentialImportAuthorizationV1,
    ModelCredentialImportError, ModelCredentialImportIdentityV1, ModelCredentialOperationId,
    Permission, ResourceId, ResourceKind, SensitiveModelApiKey, MODEL_API_KEY_PURPOSE,
    MODEL_CREDENTIAL_IMPORT_AUTHORIZATION_SECONDS,
};
use insight_platform_security::ModelCredentialImporter;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub const MAX_MODEL_CREDENTIAL_IMPORT_BODY_BYTES: usize = 8192;
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportModelCredentialRequestV1 {
    pub schema_version: u16,
    pub operation_id: ModelCredentialOperationId,
    pub provider_id: ResourceId,
    pub api_key: SensitiveModelApiKey,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportModelCredentialResponseV1 {
    pub schema_version: u16,
    pub binding: ExactSecretBindingRef,
}
#[derive(Clone)]
pub struct ModelCredentialHttpState {
    importer: Arc<dyn ModelCredentialImporter>,
    clock: Arc<dyn AuthenticationClock>,
}
impl ModelCredentialHttpState {
    pub fn new(
        importer: Arc<dyn ModelCredentialImporter>,
        clock: Arc<dyn AuthenticationClock>,
    ) -> Self {
        Self { importer, clock }
    }
}
pub fn build_model_credential_router(state: ModelCredentialHttpState) -> Router {
    Router::new()
        .route("/v1/model-credentials", post(import))
        .with_state(state)
}
struct PrivateBody(Vec<u8>);
impl Drop for PrivateBody {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}
fn decode(body: Bytes) -> Result<ImportModelCredentialRequestV1, ()> {
    if body.len() > MAX_MODEL_CREDENTIAL_IMPORT_BODY_BYTES {
        return Err(());
    }
    let private = PrivateBody(body.to_vec());
    // Serde's closed struct rejects duplicate/unknown fields, wrong types, and trailing input.
    // Sensitive field construction takes ownership of the String allocation and zeroes on drop.
    let request: ImportModelCredentialRequestV1 =
        serde_json::from_slice(&private.0).map_err(|_| ())?;
    if request.schema_version != 1 || request.provider_id.kind() != ResourceKind::SecretProvider {
        return Err(());
    }
    Ok(request)
}
async fn import(
    State(state): State<ModelCredentialHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    request: Request,
) -> Response {
    let started = state.clock.now();
    let Some(Extension(principal)) = principal else {
        return problem(
            StatusCode::UNAUTHORIZED,
            ApiProblemCode::Unauthenticated,
            "Authentication is required.",
            false,
        );
    };
    if principal.validate().is_err() {
        return problem(
            StatusCode::UNAUTHORIZED,
            ApiProblemCode::Unauthenticated,
            "Authentication is required.",
            false,
        );
    }
    if !principal.permissions.contains(Permission::SecretBind) {
        return problem(
            StatusCode::FORBIDDEN,
            ApiProblemCode::PermissionDenied,
            "Credential import is not permitted.",
            false,
        );
    }
    if request.headers().contains_key(header::CONTENT_ENCODING) {
        return problem(
            StatusCode::BAD_REQUEST,
            ApiProblemCode::InvalidRequest,
            "The credential import request is invalid.",
            false,
        );
    }
    let body = match tokio::time::timeout(
        std::time::Duration::from_secs(30),
        axum::body::to_bytes(request.into_body(), MAX_MODEL_CREDENTIAL_IMPORT_BODY_BYTES),
    )
    .await
    {
        Ok(Ok(body)) => body,
        _ => {
            return problem(
                StatusCode::BAD_REQUEST,
                ApiProblemCode::InvalidRequest,
                "The credential import request is invalid.",
                false,
            )
        }
    };
    let request = match decode(body) {
        Ok(value) => value,
        Err(()) => {
            return problem(
                StatusCode::BAD_REQUEST,
                ApiProblemCode::InvalidRequest,
                "The credential import request is invalid.",
                false,
            )
        }
    };
    let now = state.clock.now();
    let identity = ModelCredentialImportIdentityV1 {
        schema_version: 1,
        operation_id: request.operation_id,
        tenant_id: principal.tenant_id,
        principal_id: principal.principal_id,
        principal_kind: principal.principal_kind,
        provider_id: request.provider_id,
        purpose: MODEL_API_KEY_PURPOSE.parse().expect("owning purpose"),
    };
    let authorization = ModelCredentialImportAuthorizationV1 {
        schema_version: 1,
        identity: identity.clone(),
        deadline: (started + Duration::seconds(MODEL_CREDENTIAL_IMPORT_AUTHORIZATION_SECONDS))
            .min(principal.credential_expires_at),
    };
    if !authorization.validate_at(now) {
        return problem(
            StatusCode::UNAUTHORIZED,
            ApiProblemCode::Unauthenticated,
            "Authentication is required.",
            false,
        );
    }
    match state
        .importer
        .import_model_credential(authorization, request.api_key)
        .await
    {
        Ok(binding)
            if binding.validate().is_ok()
                && identity.secret_binding_id().ok().as_ref()
                    == Some(&binding.secret_binding_id)
                && binding.provider_id == identity.provider_id
                && binding.purpose == identity.purpose
                && binding.binding_generation == 1
                && matches!(
                    binding.resolution_policy,
                    insight_platform_contracts::SecretResolutionPolicy::Pinned { .. }
                ) =>
        {
            private_response(
                (
                    StatusCode::OK,
                    Json(ImportModelCredentialResponseV1 {
                        schema_version: 1,
                        binding,
                    }),
                )
                    .into_response(),
            )
        }
        Err(ModelCredentialImportError::Rejected) => problem(
            StatusCode::FORBIDDEN,
            ApiProblemCode::PermissionDenied,
            "Credential import was rejected by current authority or preparation identity.",
            false,
        ),
        Err(ModelCredentialImportError::TemporarilyUnavailable) => problem(
            StatusCode::SERVICE_UNAVAILABLE,
            ApiProblemCode::TemporarilyUnavailable,
            "Credential import is temporarily unavailable; retain the operation identity.",
            true,
        ),
        Ok(_) | Err(ModelCredentialImportError::OutcomeUnknown) => problem(
            StatusCode::SERVICE_UNAVAILABLE,
            ApiProblemCode::CredentialImportOutcomeUnknown,
            "Credential import outcome is unknown; retry only the same operation and input.",
            true,
        ),
    }
}
fn private_response(mut response: Response) -> Response {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, private, max-age=0"),
    );
    response
}
fn problem(status: StatusCode, code: ApiProblemCode, title: &str, retryable: bool) -> Response {
    let request_id = ResourceId::from_uuid_v7(ResourceKind::ServerRequest, uuid::Uuid::now_v7())
        .expect("request UUID");
    let problem = ApiProblem {
        type_uri: format!("https://insight.platform/problems/{}", code.as_str()),
        title: title.to_owned(),
        status: status.as_u16(),
        code,
        detail: None,
        request_id,
        trace_id: crate::trace::current_trace_id(),
        retryable,
        retry_after_ms: retryable.then_some(1000),
        field_errors: vec![],
    };
    let mut response = private_response((status, Json(problem)).into_response());
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };
    use chrono::{DateTime, Utc};
    use insight_platform_contracts::{
        AuthnStrength, PermissionSet, PrincipalKind, SecretResolutionPolicy, Sha256Digest,
    };
    use std::sync::Mutex;
    use tower::ServiceExt;
    struct Clock(DateTime<Utc>);
    impl AuthenticationClock for Clock {
        fn now(&self) -> DateTime<Utc> {
            self.0
        }
    }
    struct Importer {
        calls: Mutex<Vec<ModelCredentialImportAuthorizationV1>>,
        failure: Option<ModelCredentialImportError>,
    }
    #[async_trait::async_trait]
    impl ModelCredentialImporter for Importer {
        async fn import_model_credential(
            &self,
            request: ModelCredentialImportAuthorizationV1,
            key: SensitiveModelApiKey,
        ) -> Result<ExactSecretBindingRef, ModelCredentialImportError> {
            assert_eq!(key.expose(), b"api-key-canary");
            self.calls.lock().unwrap().push(request.clone());
            if let Some(failure) = self.failure {
                return Err(failure);
            }
            ExactSecretBindingRef::build(
                request.identity.secret_binding_id()?,
                1,
                request.identity.provider_id,
                request.identity.purpose,
                SecretResolutionPolicy::Pinned {
                    opaque_version_identity_digest: digest(),
                },
            )
            .map_err(|_| ModelCredentialImportError::Rejected)
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
            permissions: PermissionSet::new(vec![Permission::SecretBind]).unwrap(),
            authn_strength: AuthnStrength::MultiFactor,
            principal_version: 1,
            binding_generation: 1,
            binding_version: 1,
            credential_digest: digest(),
            credential_expires_at: now + Duration::minutes(5),
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
        }
    }
    fn body() -> String {
        r#"{"schema_version":1,"operation_id":"a8376371-3d45-4ef6-8c8c-eb1a895fa99c","provider_id":"spr_0198f1c3-8f49-7c3e-b1f3-773c28367d15","api_key":"api-key-canary"}"#.to_owned()
    }
    #[tokio::test]
    async fn closed_import_body_and_authentication_precede_private_calls() {
        let now = Utc::now();
        let importer = Arc::new(Importer {
            calls: Mutex::new(vec![]),
            failure: None,
        });
        let router = build_model_credential_router(ModelCredentialHttpState::new(
            importer.clone(),
            Arc::new(Clock(now)),
        ));
        for invalid in [
            body().replace("\"schema_version\":1", "\"schema_version\":1.0"),
            body().replace("\"api_key\":", "\"api_key\":\"duplicate\",\"api_key\":"),
            body().replace("api-key-canary", "with space"),
            body().replace("spr_", "ten_"),
            body().replace(
                "\"schema_version\":1",
                "\"schema_version\":1,\"tenant_id\":\"forged\"",
            ),
            body().replace("api-key-canary", &"a".repeat(4097)),
            "x".repeat(8193),
        ] {
            let response = router
                .clone()
                .oneshot(
                    Request::post("/v1/model-credentials")
                        .extension(principal(now))
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
            assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
            let bytes = to_bytes(response.into_body(), 8192).await.unwrap();
            assert!(!String::from_utf8_lossy(&bytes).contains("api-key-canary"));
        }
        let response = router
            .clone()
            .oneshot(
                Request::post("/v1/model-credentials")
                    .body(Body::from(body()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let mut denied = principal(now);
        denied.permissions = PermissionSet::new(vec![]).unwrap();
        let response = router
            .clone()
            .oneshot(
                Request::post("/v1/model-credentials")
                    .extension(denied)
                    .body(Body::from(body()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(importer.calls.lock().unwrap().is_empty());
        let response = router
            .oneshot(
                Request::post("/v1/model-credentials")
                    .extension(principal(now))
                    .body(Body::from(body()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CACHE_CONTROL],
            "no-store, private, max-age=0"
        );
        let bytes = to_bytes(response.into_body(), 8192).await.unwrap();
        assert!(!String::from_utf8_lossy(&bytes).contains("api-key-canary"));
        let response: ImportModelCredentialResponseV1 = serde_json::from_slice(&bytes).unwrap();
        let calls = importer.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].identity.principal_id, principal(now).principal_id);
        assert_eq!(calls[0].deadline, now + Duration::seconds(30));
        assert_eq!(
            response.binding.secret_binding_id,
            calls[0].identity.secret_binding_id().unwrap()
        );
    }
    #[tokio::test]
    async fn uncertain_import_is_distinct_and_never_returns_input() {
        for (failure, code) in [
            (
                ModelCredentialImportError::TemporarilyUnavailable,
                "temporarily_unavailable",
            ),
            (
                ModelCredentialImportError::OutcomeUnknown,
                "credential_import_outcome_unknown",
            ),
        ] {
            let now = Utc::now();
            let router = build_model_credential_router(ModelCredentialHttpState::new(
                Arc::new(Importer {
                    calls: Mutex::new(vec![]),
                    failure: Some(failure),
                }),
                Arc::new(Clock(now)),
            ));
            let response = router
                .oneshot(
                    Request::post("/v1/model-credentials")
                        .extension(principal(now))
                        .body(Body::from(body()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            let bytes = to_bytes(response.into_body(), 8192).await.unwrap();
            let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(value["code"], code);
            assert!(!String::from_utf8_lossy(&bytes).contains("api-key-canary"));
        }
    }
}
