//! Read-only authoring transport proof. Registry current authorization and resolution remain
//! behind the typed application; this fixture proves parsing, cursor scope and response binding.
use async_trait::async_trait;
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Router,
};
use chrono::{DateTime, Duration, Utc};
use insight_platform_api::{
    authentication::AuthenticatedPrincipal,
    authoring::*,
    product::{AuthorityListPage, HmacListCursorCodec, ListKeysetBoundary},
    task::TaskClock,
};
use insight_platform_contracts::{
    AgentSlotBindingInputV1, AgentSlotTargetInputV1, AuthnStrength, DependencySlotKind,
    ExactDeploymentRef, ExactPolicyBinding, ExactVersionRef, Permission, PermissionSet,
    PrincipalKind, ResourceId, ResourceKind, Sha256Digest, TraceIdentityV1, UtcTimestamp,
};
use insight_platform_registry::authoring::*;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tower::ServiceExt;
fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
    format!(
        "{}_0198f1cc-32e4-75e1-a9e8-d95ca0f8{suffix:04x}",
        kind.descriptor().prefix
    )
    .parse()
    .unwrap()
}
fn digest(c: char) -> Sha256Digest {
    format!("sha256:{}", c.to_string().repeat(64))
        .parse()
        .unwrap()
}
fn principal(now: DateTime<Utc>) -> AuthenticatedPrincipal {
    AuthenticatedPrincipal {
        tenant_id: id(ResourceKind::Tenant, 1),
        principal_id: id(ResourceKind::Principal, 2),
        principal_kind: PrincipalKind::AgentRunner,
        permissions: PermissionSet::new(vec![Permission::AgentRead]).unwrap(),
        authn_strength: AuthnStrength::MultiFactor,
        principal_version: 1,
        binding_generation: 1,
        binding_version: 1,
        credential_digest: digest('a'),
        credential_expires_at: now + Duration::hours(1),
        trace: TraceIdentityV1::generate(),
    }
}
struct Clock(DateTime<Utc>);
impl TaskClock for Clock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}
fn policy() -> ExactPolicyBinding {
    ExactPolicyBinding {
        deployment: ExactDeploymentRef::new(id(ResourceKind::PolicyDeployment, 3), digest('b'))
            .unwrap(),
        revision: ExactVersionRef::new(id(ResourceKind::PolicyRevision, 4), digest('c')).unwrap(),
    }
}
fn selection(slot_id: &str) -> AuthoringSlotSelectionV1 {
    AuthoringSlotSelectionV1 {
        slot_id: slot_id.into(),
        requirement_digest: digest('d'),
        interface_contract_digest: None,
        target: AuthoringSlotTargetV1::Model {
            candidates: vec![AuthoringDeploymentSelectorV1::Active {
                resource_id: id(ResourceKind::ModelProfile, 5),
                environment: "dev".into(),
            }],
            selection_policy: policy(),
        },
    }
}
fn request() -> ResolveAgentBindingsRequestV1 {
    ResolveAgentBindingsRequestV1 {
        schema_version: 1,
        slots: vec![selection("first"), selection("second")],
    }
}
struct Fixture {
    now: DateTime<Utc>,
    discover_calls: Mutex<Vec<DiscoverAuthoringIntent>>,
    resolve_calls: Mutex<Vec<ResolveAuthoringIntent>>,
    reorder: bool,
}
#[async_trait]
impl AuthoringApplication for Fixture {
    async fn discover(
        &self,
        intent: DiscoverAuthoringIntent,
    ) -> Result<AuthorityListPage<AuthoringDependencyV1>, AuthoringQueryError> {
        self.discover_calls.lock().unwrap().push(intent.clone());
        let deployment =
            ExactDeploymentRef::new(id(ResourceKind::ModelDeployment, 6), digest('e')).unwrap();
        Ok(AuthorityListPage {
            snapshot_at: intent.snapshot_at.unwrap_or(self.now),
            items: if intent.boundary.is_none() {
                vec![]
            } else {
                vec![AuthoringDependencyV1 {
                    schema_version: 1,
                    kind: DependencySlotKind::Model,
                    resource_id: id(ResourceKind::ModelProfile, 5),
                    environment: "dev".into(),
                    deployment: deployment.clone(),
                    interface_contract_digest: digest('f'),
                    contract_match: intent
                        .filters
                        .interface_contract_digest
                        .map(|wanted| wanted == digest('f')),
                    call_authorized: false,
                }]
            },
            next_boundary: intent.boundary.is_none().then(|| {
                ListKeysetBoundary::AuthoringDependency {
                    created_at: UtcTimestamp::from_datetime(self.now - Duration::minutes(1)),
                    deployment_id: deployment.deployment_id,
                }
            }),
        })
    }
    async fn resolve(
        &self,
        intent: ResolveAuthoringIntent,
    ) -> Result<ResolveAgentBindingsResponseV1, AuthoringQueryError> {
        self.resolve_calls.lock().unwrap().push(intent.clone());
        let mut slots: Vec<_> = intent
            .request
            .slots
            .iter()
            .map(|slot| AuthoringSlotResolutionV1 {
                slot_id: slot.slot_id.clone(),
                resolution: AuthoringResolutionV1::Resolved {
                    deployment_features: vec![],
                    binding: Box::new(AgentSlotBindingInputV1 {
                        slot_id: slot.slot_id.clone(),
                        requirement_digest: slot.requirement_digest.clone(),
                        target: AgentSlotTargetInputV1::Model {
                            candidates: vec![ExactDeploymentRef::new(
                                id(ResourceKind::ModelDeployment, 6),
                                digest('e'),
                            )
                            .unwrap()],
                            selection_policy: policy(),
                        },
                    }),
                    observed_contract_digests: vec![digest('f')],
                    contract_match: None,
                    call_authorized: false,
                },
            })
            .collect();
        if self.reorder {
            slots.reverse()
        }
        Ok(ResolveAgentBindingsResponseV1 {
            schema_version: 1,
            slots,
        })
    }
}
fn setup(reorder: bool) -> (Router, Arc<Fixture>, AuthenticatedPrincipal) {
    let now = Utc::now();
    let fixture = Arc::new(Fixture {
        // The durable owner and the Gateway do not share a clock.
        now: now + Duration::milliseconds(25),
        discover_calls: Mutex::new(vec![]),
        resolve_calls: Mutex::new(vec![]),
        reorder,
    });
    let router = build_authoring_router(
        AuthoringHttpState::new(
            fixture.clone(),
            Arc::new(HmacListCursorCodec::install(&[7; 32]).unwrap()),
        )
        .with_clock(Arc::new(Clock(now))),
    );
    (router, fixture, principal(now))
}
async fn call(
    router: &Router,
    method: &str,
    uri: &str,
    body: String,
    actor: Option<&AuthenticatedPrincipal>,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(actor) = actor {
        request = request.extension(actor.clone())
    };
    let response = router
        .clone()
        .oneshot(request.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    assert_eq!(
        response.headers()["cache-control"],
        "no-store, private, max-age=0"
    );
    let status = response.status();
    let body = to_bytes(response.into_body(), 1_048_576).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}
#[tokio::test]
async fn resolve_post_is_read_only_without_receipt_and_preserves_order_and_independent_call_permission(
) {
    let (router, fixture, actor) = setup(false);
    let request = request();
    request.validate().unwrap();
    let (status, body) = call(
        &router,
        "POST",
        "/v1/agent-authoring-bindings:resolve",
        serde_json::to_string(&request).unwrap(),
        Some(&actor),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let response: ResolveAgentBindingsResponseV1 = serde_json::from_value(body).unwrap();
    response.validate_for(&request).unwrap();
    assert_eq!(
        response
            .slots
            .iter()
            .map(|slot| slot.slot_id.as_str())
            .collect::<Vec<_>>(),
        vec!["first", "second"]
    );
    assert!(matches!(
        &response.slots[0].resolution,
        AuthoringResolutionV1::Resolved {
            contract_match: None,
            call_authorized: false,
            ..
        }
    ));
    assert_eq!(fixture.resolve_calls.lock().unwrap().len(), 1);
    assert!(fixture.discover_calls.lock().unwrap().is_empty());
    assert_eq!(
        call(
            &router,
            "POST",
            "/v1/agent-authoring-bindings:resolve",
            serde_json::to_string(&request).unwrap(),
            None
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(fixture.resolve_calls.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn authoring_json_is_strict_and_misordered_authority_output_is_rejected() {
    let (router, fixture, actor) = setup(false);
    let valid = serde_json::to_string(&request()).unwrap();
    for body in [
        valid.replacen(
            "\"schema_version\":1",
            "\"schema_version\":1,\"schema_version\":1",
            1,
        ),
        valid.replacen(
            "\"schema_version\":1",
            "\"schema_version\":1,\"mutation\":false",
            1,
        ),
        json!({"schema_version":1,"slots":[]}).to_string(),
    ] {
        assert_eq!(
            call(
                &router,
                "POST",
                "/v1/agent-authoring-bindings:resolve",
                body,
                Some(&actor)
            )
            .await
            .0,
            StatusCode::BAD_REQUEST
        );
    }
    assert!(fixture.resolve_calls.lock().unwrap().is_empty());
    let (router, fixture, actor) = setup(true);
    assert_eq!(
        call(
            &router,
            "POST",
            "/v1/agent-authoring-bindings:resolve",
            valid,
            Some(&actor)
        )
        .await
        .0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(fixture.resolve_calls.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn discovery_empty_page_keeps_scoped_cursor_and_rejects_changed_query_before_authority() {
    let (router, fixture, actor) = setup(false);
    let uri = "/v1/agent-authoring-dependencies?kind=model&environment=dev&page_size=1";
    let (status, first) = call(&router, "GET", uri, String::new(), Some(&actor)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["items"], json!([]));
    let cursor = first["next_cursor"].as_str().unwrap();
    let (_, second) = call(
        &router,
        "GET",
        &format!("{uri}&cursor={cursor}"),
        String::new(),
        Some(&actor),
    )
    .await;
    assert_eq!(second["items"][0]["contract_match"], Value::Null);
    assert_eq!(second["items"][0]["call_authorized"], false);
    assert_eq!(fixture.discover_calls.lock().unwrap().len(), 2);
    for changed in [
        format!("{uri}&cursor={cursor}").replace("environment=dev", "environment=prod"),
        format!("{uri}&cursor={cursor}").replace("kind=model", "kind=skill"),
        format!("{uri}&unknown=false"),
        format!("{uri}&kind=model"),
    ] {
        assert_eq!(
            call(&router, "GET", &changed, String::new(), Some(&actor))
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    let mut other = actor.clone();
    other.principal_id = id(ResourceKind::Principal, 9);
    assert_eq!(
        call(
            &router,
            "GET",
            &format!("{uri}&cursor={cursor}"),
            String::new(),
            Some(&other)
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(fixture.discover_calls.lock().unwrap().len(), 2);
}
