//! One bounded protocol observation against an already deployed exact Model.
use crate::authentication::{AuthenticatedPrincipal, AuthenticationClock};
use crate::resource::{problem, ResourceApplicationError as Failure};
use axum::{
    extract::{Request, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Extension, Json, Router,
};
use insight_platform_contracts::*;
use std::sync::Arc;
#[derive(Debug, Clone)]
pub struct ModelConnectionIntent {
    pub principal: AuthenticatedPrincipal,
    pub request: ModelConnectionProbeRequestV1,
    pub deadline: chrono::DateTime<chrono::Utc>,
}
#[async_trait::async_trait]
pub trait ModelConnectionApplication: Send + Sync {
    async fn probe(
        &self,
        intent: ModelConnectionIntent,
    ) -> Result<ModelConnectionObservationV1, Failure>;
}
#[derive(Clone)]
pub struct ModelConnectionHttpState {
    application: Arc<dyn ModelConnectionApplication>,
    clock: Arc<dyn AuthenticationClock>,
}
impl ModelConnectionHttpState {
    pub fn new(
        application: Arc<dyn ModelConnectionApplication>,
        clock: Arc<dyn AuthenticationClock>,
    ) -> Self {
        Self { application, clock }
    }
}
pub fn build_model_connection_router(state: ModelConnectionHttpState) -> Router {
    Router::new()
        .route("/v1/model-configuration:probe", post(probe))
        .with_state(state)
}
async fn probe(
    State(state): State<ModelConnectionHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    request: Request,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(Failure::Unauthenticated);
    };
    let now = state.clock.now();
    if principal.validate().is_err() || principal.credential_expires_at <= now {
        return problem(Failure::Unauthenticated);
    }
    if !principal.permissions.contains(Permission::ModelRead)
        || !principal.permissions.contains(Permission::SecretBind)
    {
        return problem(Failure::Denied);
    }
    let deadline = (now + chrono::Duration::seconds(MODEL_PROBE_DEADLINE_SECONDS))
        .min(principal.credential_expires_at);
    let Ok(duration) = (deadline - now).to_std() else {
        return problem(Failure::Unauthenticated);
    };
    let operation = async {
        if request.headers().contains_key(header::CONTENT_ENCODING) {
            return Err(Failure::Invalid);
        }
        let body = axum::body::to_bytes(request.into_body(), 2048)
            .await
            .map_err(|_| Failure::Invalid)?;
        let value = parse_strict_json(
            &body,
            JsonLimits {
                max_bytes: 2048,
                max_depth: 3,
                max_properties_per_object: 4,
                max_items_per_array: 1,
                max_string_bytes: 255,
            },
        )
        .map_err(|_| Failure::Invalid)?;
        let request: ModelConnectionProbeRequestV1 =
            serde_json::from_value(value).map_err(|_| Failure::Invalid)?;
        if !request.validate() {
            return Err(Failure::Invalid);
        }
        let expected = request.model_deployment.clone();
        let observation = state
            .application
            .probe(ModelConnectionIntent {
                principal,
                request,
                deadline,
            })
            .await?;
        if !observation.validate() || observation.model_deployment != expected {
            return Err(Failure::Internal);
        }
        Ok(observation)
    };
    match tokio::time::timeout(duration, operation).await {
        Ok(Ok(v)) => {
            let mut r = (StatusCode::OK, Json(v)).into_response();
            r.headers_mut().insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static("no-store, private, max-age=0"),
            );
            r
        }
        Ok(Err(e)) => problem(e),
        Err(_) => problem(Failure::Unavailable),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::authentication::SystemAuthenticationClock;
    use axum::{body::Body, http::Request};
    use std::sync::Mutex;
    use tower::ServiceExt;
    fn id(kind: ResourceKind) -> ResourceId {
        ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
    }
    fn digest() -> Sha256Digest {
        format!("sha256:{}", "a".repeat(64)).parse().unwrap()
    }
    struct App(Mutex<Vec<ModelConnectionIntent>>);
    #[async_trait::async_trait]
    impl ModelConnectionApplication for App {
        async fn probe(
            &self,
            i: ModelConnectionIntent,
        ) -> Result<ModelConnectionObservationV1, Failure> {
            assert!(i.deadline <= chrono::Utc::now() + chrono::Duration::seconds(30));
            let observation = ModelConnectionObservationV1 {
                schema_version: 1,
                model_deployment: i.request.model_deployment.clone(),
                provider_deployment: ExactDeploymentRef::new(
                    id(ResourceKind::ModelProviderDeployment),
                    digest(),
                )
                .unwrap(),
                model_identity: ProviderModelIdentity {
                    value: "configured-model".to_owned(),
                    stability: ModelIdentityStability::ExternallyMutable,
                },
                protocol: ModelProviderWireProtocol::OpenAiResponses,
                observed_at: UtcTimestamp::from_datetime(chrono::Utc::now()),
                outcome: ModelConnectionOutcome::CredentialsRejected,
            };
            self.0.lock().unwrap().push(i);
            Ok(observation)
        }
    }
    #[tokio::test]
    async fn probe_public_route_is_authenticated_closed_and_only_an_observation() {
        let p = AuthenticatedPrincipal {
            tenant_id: id(ResourceKind::Tenant),
            principal_id: id(ResourceKind::Principal),
            principal_kind: PrincipalKind::TenantAdmin,
            permissions: PermissionSet::new(vec![Permission::ModelRead, Permission::SecretBind])
                .unwrap(),
            authn_strength: AuthnStrength::MultiFactor,
            principal_version: 1,
            binding_generation: 1,
            binding_version: 1,
            credential_digest: digest(),
            credential_expires_at: chrono::Utc::now() + chrono::Duration::minutes(5),
            trace: TraceIdentityV1::generate(),
        };
        let app = Arc::new(App(Mutex::new(vec![])));
        let router = build_model_connection_router(ModelConnectionHttpState::new(
            app.clone(),
            Arc::new(SystemAuthenticationClock),
        ));
        let request = ModelConnectionProbeRequestV1 {
            schema_version: 1,
            installation_digest: digest(),
            model_deployment: ExactDeploymentRef::new(id(ResourceKind::ModelDeployment), digest())
                .unwrap(),
        };
        let body = serde_json::to_string(&request).unwrap();
        for (input, status) in [
            (body.clone(), StatusCode::OK),
            (
                body.replace("\"schema_version\":1", "\"schema_version\":1.0"),
                StatusCode::BAD_REQUEST,
            ),
            (
                body.replace(
                    "\"schema_version\":1",
                    "\"schema_version\":1,\"schema_version\":1",
                ),
                StatusCode::BAD_REQUEST,
            ),
            (body.replace("mdep_", "mpdep_"), StatusCode::BAD_REQUEST),
        ] {
            let r = router
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/model-configuration:probe")
                        .extension(p.clone())
                        .body(Body::from(input))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(r.status(), status);
            if status == StatusCode::OK {
                assert_eq!(r.headers()["cache-control"], "no-store, private, max-age=0");
                let result: ModelConnectionObservationV1 = serde_json::from_slice(
                    &axum::body::to_bytes(r.into_body(), 4096).await.unwrap(),
                )
                .unwrap();
                assert_eq!(result.model_deployment, request.model_deployment);
                assert_eq!(result.outcome, ModelConnectionOutcome::CredentialsRejected);
            }
        }
        let mut denied = p;
        denied.permissions = PermissionSet::new(vec![Permission::ModelRead]).unwrap();
        let r = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/model-configuration:probe")
                    .extension(denied)
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::FORBIDDEN);
        assert_eq!(app.0.lock().unwrap().len(), 1);
    }
}
