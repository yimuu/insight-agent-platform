//! Actual HTTP read projection tests with typed authority fixtures. The tests do not claim
//! PostgreSQL execution, child admission or content disclosure authorization implementation.
use async_trait::async_trait;
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Router,
};
use chrono::{DateTime, Duration, Utc};
use insight_platform_api::{
    authentication::AuthenticatedPrincipal,
    product::{AuthorityListPage, HmacListCursorCodec, ListKeysetBoundary},
    run::*,
};
use insight_platform_contracts::{
    ArtifactRef, AuthnStrength, DataClassification, ExactDeploymentRef, Permission, PermissionSet,
    PrincipalKind, ResourceId, ResourceKind, RunState, RunValueStorageKind, Sha256Digest,
    TraceIdentityV1, UtcTimestamp, ValueRef,
};
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
fn actor(now: DateTime<Utc>) -> AuthenticatedPrincipal {
    AuthenticatedPrincipal {
        tenant_id: id(ResourceKind::Tenant, 1),
        principal_id: id(ResourceKind::Principal, 2),
        principal_kind: PrincipalKind::AgentRunner,
        permissions: PermissionSet::new(vec![Permission::RuntimeRead]).unwrap(),
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
impl RunClock for Clock {
    fn now(&self) -> DateTime<Utc> {
        self.0
    }
}
struct Fixture {
    now: DateTime<Utc>,
    calls: Mutex<Vec<&'static str>>,
    wrong_scope: bool,
    content: Mutex<Option<ValueRef>>,
}
impl Fixture {
    fn result(
        &self,
        run_id: ResourceId,
        value_id: ResourceId,
    ) -> Result<RunResultViewV1, RunApplicationError> {
        let value = self
            .content
            .lock()
            .unwrap()
            .clone()
            .ok_or(RunApplicationError::Denied)?;
        Ok(RunResultViewV1 {
            schema_version: 1,
            run_id,
            value_id,
            classification: DataClassification::Confidential,
            schema_digest: digest('b'),
            content_digest: digest('c'),
            value,
        })
    }
}
#[async_trait]
impl RunApplication for Fixture {
    async fn read_run(&self, _: ReadRunIntent) -> Result<RunViewV1, RunApplicationError> {
        Err(RunApplicationError::Internal)
    }
    async fn list_run_values(
        &self,
        intent: ListRunValuesIntent,
    ) -> Result<AuthorityListPage<RunValueMetadataV1>, RunApplicationError> {
        self.calls.lock().unwrap().push("values");
        Ok(AuthorityListPage {
            snapshot_at: intent.snapshot_at.unwrap_or(self.now),
            items: if intent.boundary.is_none() {
                vec![]
            } else {
                vec![RunValueMetadataV1 {
                    schema_version: 1,
                    run_id: if self.wrong_scope {
                        id(ResourceKind::Run, 99)
                    } else {
                        intent.run_id
                    },
                    node_id: intent.node_id,
                    value_id: id(ResourceKind::RunValue, 5),
                    classification: DataClassification::Confidential,
                    schema_digest: digest('b'),
                    content_digest: digest('c'),
                    storage_kind: RunValueStorageKind::Inline,
                }]
            },
            next_boundary: intent
                .boundary
                .is_none()
                .then(|| ListKeysetBoundary::RunValue {
                    created_at: UtcTimestamp::from_datetime(self.now - Duration::minutes(1)),
                    value_id: id(ResourceKind::RunValue, 5),
                }),
        })
    }
    async fn list_child_runs(
        &self,
        intent: ListChildRunsIntent,
    ) -> Result<AuthorityListPage<ChildRunViewV1>, RunApplicationError> {
        self.calls.lock().unwrap().push("children");
        Ok(AuthorityListPage {
            snapshot_at: intent.snapshot_at.unwrap_or(self.now),
            items: if intent.boundary.is_none() {
                vec![]
            } else {
                vec![ChildRunViewV1 {
                    schema_version: 1,
                    parent_run_id: if self.wrong_scope {
                        id(ResourceKind::Run, 99)
                    } else {
                        intent.run_id
                    },
                    parent_node_id: intent
                        .node_id
                        .unwrap_or_else(|| id(ResourceKind::NodeExecution, 4)),
                    parent_plan_node_key: serde_json::from_value(json!("invoke")).unwrap(),
                    child_run_id: id(ResourceKind::Run, 6),
                    child_agent_deployment: ExactDeploymentRef::new(
                        id(ResourceKind::AgentDeployment, 7),
                        digest('d'),
                    )
                    .unwrap(),
                    child_state: RunState::Running,
                    child_version: 2,
                    input_value_id: id(ResourceKind::RunValue, 8),
                    output_value_id: None,
                    created_at: UtcTimestamp::from_datetime(self.now - Duration::minutes(1)),
                }]
            },
            next_boundary: intent
                .boundary
                .is_none()
                .then(|| ListKeysetBoundary::ChildRun {
                    created_at: UtcTimestamp::from_datetime(self.now - Duration::minutes(1)),
                    run_id: id(ResourceKind::Run, 6),
                }),
        })
    }
    async fn read_run_value_content(
        &self,
        intent: ReadRunValueIntent,
    ) -> Result<RunResultViewV1, RunApplicationError> {
        self.calls.lock().unwrap().push("content");
        self.result(intent.run_id, intent.value_id)
    }
    async fn read_run_result(
        &self,
        intent: ReadRunIntent,
    ) -> Result<RunResultViewV1, RunApplicationError> {
        self.calls.lock().unwrap().push("result");
        self.result(intent.run_id, id(ResourceKind::RunValue, 5))
    }
}
fn setup(wrong_scope: bool) -> (Router, Arc<Fixture>, AuthenticatedPrincipal) {
    let now = Utc::now();
    let fixture = Arc::new(Fixture {
        // Exercise both metadata routes with the database clock ahead of the Gateway.
        now: now + Duration::milliseconds(25),
        calls: Mutex::new(vec![]),
        wrong_scope,
        content: Mutex::new(None),
    });
    (
        build_run_router(
            RunHttpState::new(fixture.clone(), Arc::new(Clock(now)))
                .with_list_cursor_codec(Arc::new(HmacListCursorCodec::install(&[7; 32]).unwrap())),
        ),
        fixture,
        actor(now),
    )
}
async fn get(
    router: &Router,
    uri: &str,
    actor: Option<&AuthenticatedPrincipal>,
) -> (StatusCode, Value) {
    let mut request = Request::builder().uri(uri);
    if let Some(actor) = actor {
        request = request.extension(actor.clone())
    };
    let response = router
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(
        response.headers()["cache-control"],
        "no-store, private, max-age=0"
    );
    let status = response.status();
    let body = to_bytes(response.into_body(), 65_536).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}
#[tokio::test]
async fn values_metadata_continues_empty_pages_without_reading_content_and_content_denial_is_explicit(
) {
    let (router, fixture, actor) = setup(false);
    let run = id(ResourceKind::Run, 3);
    let node = id(ResourceKind::NodeExecution, 4);
    let uri = format!("/v1/runs/{run}/values?node_id={node}&page_size=1");
    let (status, page) = get(&router, &uri, Some(&actor)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["items"], json!([]));
    let cursor = page["next_cursor"].as_str().unwrap();
    let (status, page) = get(&router, &format!("{uri}&cursor={cursor}"), Some(&actor)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["items"][0]["node_id"], node.to_string());
    assert!(page["items"][0].get("value").is_none());
    assert_eq!(*fixture.calls.lock().unwrap(), vec!["values", "values"]);
    let uri = format!(
        "/v1/runs/{run}/values/{}/content",
        id(ResourceKind::RunValue, 5)
    );
    let (status, body) = get(&router, &uri, Some(&actor)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "permission_denied");
    assert_eq!(body["retryable"], false);
    assert!(body.get("value").is_none());
    let (status, body) = get(&router, &format!("/v1/runs/{run}/result"), Some(&actor)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "permission_denied");
    assert_eq!(body["retryable"], false);
    assert!(body.get("value").is_none());
    assert_eq!(get(&router, &uri, None).await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(
        *fixture.calls.lock().unwrap(),
        vec!["values", "values", "content", "result"]
    );
}
#[tokio::test]
async fn value_and_child_cursors_bind_route_parent_node_and_current_subject() {
    let (router, fixture, actor) = setup(false);
    let run = id(ResourceKind::Run, 3);
    let node = id(ResourceKind::NodeExecution, 4);
    for route in ["values", "children"] {
        let uri = format!("/v1/runs/{run}/{route}?node_id={node}&page_size=1");
        let (_, page) = get(&router, &uri, Some(&actor)).await;
        let cursor = page["next_cursor"].as_str().unwrap();
        let next = format!("{uri}&cursor={cursor}");
        let (status, body) = get(&router, &next, Some(&actor)).await;
        assert_eq!(status, StatusCode::OK);
        if route == "children" {
            let timestamp = body["items"][0]["created_at"].as_str().unwrap();
            assert_eq!(timestamp.len(), 27);
            assert_eq!(&timestamp[19..20], ".");
            assert_eq!(&timestamp[26..], "Z");
            assert!(timestamp[20..26].bytes().all(|b| b.is_ascii_digit()));
        }
        let calls = fixture.calls.lock().unwrap().len();
        let other_route = if route == "values" {
            "children"
        } else {
            "values"
        };
        for changed in [
            next.replace(&format!("/{route}?"), &format!("/{other_route}?")),
            next.replace(&run.to_string(), &id(ResourceKind::Run, 10).to_string()),
            next.replace(
                &node.to_string(),
                &id(ResourceKind::NodeExecution, 11).to_string(),
            ),
            format!("{uri}&include_body=true"),
            format!("{uri}&page_size=1"),
        ] {
            assert_eq!(
                get(&router, &changed, Some(&actor)).await.0,
                StatusCode::BAD_REQUEST
            );
        }
        let mut other = actor.clone();
        other.principal_id = id(ResourceKind::Principal, 12);
        assert_eq!(
            get(&router, &next, Some(&other)).await.0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(fixture.calls.lock().unwrap().len(), calls);
    }
}
#[tokio::test]
async fn current_run_lists_reject_foreign_parent_authority_projections() {
    let (router, fixture, actor) = setup(true);
    let run = id(ResourceKind::Run, 3);
    for route in ["values", "children"] {
        let uri = format!("/v1/runs/{run}/{route}?page_size=1");
        let (_, page) = get(&router, &uri, Some(&actor)).await;
        let (status, body) = get(
            &router,
            &format!("{uri}&cursor={}", page["next_cursor"].as_str().unwrap()),
            Some(&actor),
        )
        .await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body.get("items").is_none());
    }
    assert_eq!(fixture.calls.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn both_body_routes_preserve_exact_inline_and_artifact_authority_variants() {
    let (router, fixture, actor) = setup(false);
    let run = id(ResourceKind::Run, 3);
    let value = id(ResourceKind::RunValue, 5);
    let artifact = ArtifactRef::new(
        id(ResourceKind::Artifact, 13),
        digest('c'),
        7,
        "application/json",
        DataClassification::Confidential,
        None,
    )
    .unwrap();
    for content in [
        ValueRef::Inline {
            value: json!({"approved":false}),
        },
        ValueRef::Artifact { artifact },
    ] {
        *fixture.content.lock().unwrap() = Some(content.clone());
        for path in [
            format!("/v1/runs/{run}/result"),
            format!("/v1/runs/{run}/values/{value}/content"),
        ] {
            let (status, body) = get(&router, &path, Some(&actor)).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body["value"], serde_json::to_value(&content).unwrap());
            assert_eq!(body["value_id"], value.to_string());
            assert_eq!(body["run_id"], run.to_string());
        }
    }
    assert_eq!(
        *fixture.calls.lock().unwrap(),
        vec!["result", "content", "result", "content"]
    );
}
