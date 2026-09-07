//! HTTP boundary evidence with a deterministic Task application fixture. Current membership and
//! PostgreSQL eligibility are separately exercised by product_task_reads_pg.
use async_trait::async_trait;
use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Router,
};
use chrono::{DateTime, Duration, Utc};
use insight_platform_api::{
    authentication::AuthenticatedPrincipal,
    product::{
        AuthorityListPage, HmacListCursorCodec, ListKeysetBoundary, PRODUCT_LIST_CURSOR_TTL_SECONDS,
    },
    task::{
        build_task_router, task_etag, ListTasksIntent, ReadTaskIntent, ResolveTaskIntent,
        TaskApplication, TaskApplicationError, TaskClock, TaskFormV2, TaskHttpState,
        TaskOwnerLinkV2, TaskViewV2,
    },
};
use insight_platform_contracts::{
    AuthnStrength, InteractionSchemaDocument, Permission, PermissionSet, PrincipalKind, ResourceId,
    ResourceKind, TraceIdentityV1, UtcTimestamp,
};
use insight_platform_tasks::{TaskKind, TaskState};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use tower::ServiceExt;

fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
    format!(
        "{}_0198f1cc-32e4-75e1-a9e8-d95ca0f8{suffix:04x}",
        kind.descriptor().prefix
    )
    .parse()
    .unwrap()
}

fn principal(now: DateTime<Utc>) -> AuthenticatedPrincipal {
    AuthenticatedPrincipal {
        tenant_id: id(ResourceKind::Tenant, 1),
        principal_id: id(ResourceKind::Principal, 2),
        principal_kind: PrincipalKind::AgentRunner,
        permissions: PermissionSet::new(vec![
            Permission::InteractionRespond,
            Permission::ArtifactRead,
        ])
        .unwrap(),
        authn_strength: AuthnStrength::MultiFactor,
        principal_version: 1,
        binding_generation: 1,
        binding_version: 1,
        credential_digest: format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
        credential_expires_at: now + Duration::hours(1),
        trace: TraceIdentityV1::generate(),
    }
}

struct Clock(Mutex<DateTime<Utc>>);
impl TaskClock for Clock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().unwrap()
    }
}

#[derive(Clone, Copy)]
enum FormMode {
    Available,
    OAuthUnavailable,
    Denied,
    InvalidDigest,
    InvalidEtag,
}
struct Fixture {
    now: DateTime<Utc>,
    mode: FormMode,
    list_calls: Mutex<Vec<ListTasksIntent>>,
    form_calls: AtomicUsize,
    mutations: AtomicUsize,
    drift_snapshot: AtomicBool,
}
impl Fixture {
    fn schema(&self) -> InteractionSchemaDocument {
        InteractionSchemaDocument::build(json!({
            "$schema":"https://json-schema.org/draft/2020-12/schema","type":"object",
            "properties":{"approved":{"type":"boolean"}},"required":["approved"],"additionalProperties":false
        })).unwrap()
    }
    fn task(&self) -> TaskViewV2 {
        let task_id = id(ResourceKind::Interaction, 3);
        TaskViewV2 {
            schema_version: 2,
            allowed_actions: Vec::new(),
            task_id: task_id.clone(),
            task_kind: if matches!(self.mode, FormMode::OAuthUnavailable) {
                TaskKind::ExternalAuthorization
            } else {
                TaskKind::HumanWork
            },
            state: TaskState::Pending,
            generation: 2,
            version: 3,
            safe_prompt_key: "test_approval".to_owned(),
            response_schema_digest: (!matches!(self.mode, FormMode::OAuthUnavailable))
                .then(|| self.schema().canonical_digest),
            owner: TaskOwnerLinkV2::Run {
                run_id: id(ResourceKind::Run, 4),
            },
            deadline: UtcTimestamp::from_datetime(self.now + Duration::hours(1)),
            responded_at: None,
            created_at: UtcTimestamp::from_datetime(self.now - Duration::minutes(1)),
            updated_at: UtcTimestamp::from_datetime(self.now),
            etag: task_etag(&task_id, 3),
        }
    }
    fn boundary(&self) -> ListKeysetBoundary {
        ListKeysetBoundary::Task {
            created_at: UtcTimestamp::from_datetime(self.now - Duration::minutes(1)),
            task_id: id(ResourceKind::Interaction, 5),
        }
    }
}

#[async_trait]
impl TaskApplication for Fixture {
    async fn list_tasks(
        &self,
        intent: ListTasksIntent,
    ) -> Result<AuthorityListPage<TaskViewV2>, TaskApplicationError> {
        self.list_calls.lock().unwrap().push(intent.clone());
        Ok(AuthorityListPage {
            snapshot_at: intent.snapshot_at.unwrap_or(self.now)
                + if self.drift_snapshot.load(Ordering::Relaxed) {
                    Duration::milliseconds(1)
                } else {
                    Duration::zero()
                },
            items: if intent.boundary.is_none() {
                Vec::new()
            } else {
                vec![self.task()]
            },
            next_boundary: intent.boundary.is_none().then(|| self.boundary()),
        })
    }
    async fn read_task(&self, _intent: ReadTaskIntent) -> Result<TaskViewV2, TaskApplicationError> {
        Ok(self.task())
    }
    async fn read_task_form(
        &self,
        intent: ReadTaskIntent,
    ) -> Result<TaskFormV2, TaskApplicationError> {
        self.form_calls.fetch_add(1, Ordering::Relaxed);
        match self.mode {
            FormMode::OAuthUnavailable => return Err(TaskApplicationError::FormUnavailable),
            FormMode::Denied => return Err(TaskApplicationError::Denied),
            _ => {}
        }
        let schema = self.schema();
        Ok(TaskFormV2 {
            schema_version: 2,
            allowed_actions: self.task().allowed_actions,
            etag: if matches!(self.mode, FormMode::InvalidEtag) {
                task_etag(&intent.task_id, 4)
            } else {
                self.task().etag
            },
            safe_prompt_key: self.task().safe_prompt_key,
            task_id: intent.task_id,
            generation: 2,
            version: 3,
            response_schema_digest: if matches!(self.mode, FormMode::InvalidDigest) {
                format!("sha256:{}", "b".repeat(64)).parse().unwrap()
            } else {
                schema.canonical_digest.clone()
            },
            response_schema: schema,
        })
    }
    async fn resolve_task(
        &self,
        _intent: ResolveTaskIntent,
    ) -> Result<TaskViewV2, TaskApplicationError> {
        self.mutations.fetch_add(1, Ordering::Relaxed);
        Err(TaskApplicationError::InvalidTaskInput)
    }
}

fn setup(mode: FormMode) -> (Router, Arc<Fixture>, Arc<Clock>, AuthenticatedPrincipal) {
    let now = Utc::now();
    let fixture = Arc::new(Fixture {
        now,
        mode,
        list_calls: Mutex::new(Vec::new()),
        form_calls: AtomicUsize::new(0),
        mutations: AtomicUsize::new(0),
        drift_snapshot: AtomicBool::new(false),
    });
    let clock = Arc::new(Clock(Mutex::new(now)));
    let state = TaskHttpState::new(fixture.clone(), clock.clone())
        .with_list_cursor_codec(Arc::new(HmacListCursorCodec::install(&[7; 32]).unwrap()));
    (build_task_router(state), fixture, clock, principal(now))
}

async fn get(
    router: &Router,
    uri: &str,
    principal: Option<&AuthenticatedPrincipal>,
) -> (StatusCode, Value) {
    let mut request = Request::builder().uri(uri);
    if let Some(principal) = principal {
        request = request.extension(principal.clone());
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(
        response.headers().get("cache-control").unwrap(),
        "no-store, private, max-age=0"
    );
    let status = response.status();
    let etag = response.headers().get("etag").cloned();
    let body = to_bytes(response.into_body(), 262_144).await.unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    if status == StatusCode::OK && uri.starts_with("/v1/tasks/") {
        let expected = task_etag(
            &body["task_id"].as_str().unwrap().parse().unwrap(),
            body["version"].as_u64().unwrap(),
        );
        assert_eq!(body["etag"], expected);
        assert_eq!(etag.unwrap().to_str().unwrap(), expected);
    } else {
        assert!(etag.is_none());
    }
    (status, body)
}

#[tokio::test]
async fn empty_authority_page_keeps_signed_task_continuation_and_exact_filters() {
    let (router, fixture, _clock, principal) = setup(FormMode::Available);
    let filters = format!(
        "state=pending&kind=human_work&run_id={}&page_size=1",
        id(ResourceKind::Run, 4)
    );
    let (status, first) = get(&router, &format!("/v1/tasks?{filters}"), Some(&principal)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["items"], json!([]));
    let cursor = first["next_cursor"].as_str().unwrap();
    assert!(!cursor.is_empty());
    let (status, second) = get(
        &router,
        &format!("/v1/tasks?{filters}&cursor={cursor}"),
        Some(&principal),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        second["items"][0]["task_id"],
        fixture.task().task_id.to_string()
    );
    assert!(second["next_cursor"].is_null());
    let calls = fixture.list_calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls[0].filters.purpose,
        insight_platform_tasks::TaskQueryPurpose::Respondable
    );
    assert_eq!(calls[0].filters.state, Some(TaskState::Pending));
    assert_eq!(calls[0].filters.kind, Some(TaskKind::HumanWork));
    assert_eq!(calls[0].filters.run_id, Some(id(ResourceKind::Run, 4)));
    assert_eq!(calls[1].boundary, Some(fixture.boundary()));
    assert!(calls[0].snapshot_at.is_none());
    assert!(calls[1].snapshot_at.is_some());
    assert_eq!(fixture.mutations.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn task_pages_keep_the_database_snapshot_when_gateway_clock_lags() {
    let (router, fixture, clock, principal) = setup(FormMode::Available);
    *clock.0.lock().unwrap() -= Duration::milliseconds(25);
    let (status, first) = get(&router, "/v1/tasks?page_size=1", Some(&principal)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first["items"], json!([]));
    let cursor = first["next_cursor"].as_str().unwrap();
    let (status, second) = get(
        &router,
        &format!("/v1/tasks?page_size=1&cursor={cursor}"),
        Some(&principal),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(second["items"].as_array().unwrap().len(), 1);
    let database_snapshot =
        DateTime::parse_from_rfc3339(UtcTimestamp::from_datetime(fixture.now).as_str())
            .unwrap()
            .with_timezone(&Utc);
    assert_eq!(
        fixture.list_calls.lock().unwrap()[1].snapshot_at,
        Some(database_snapshot)
    );
    assert_eq!(fixture.mutations.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn task_continuation_rejects_a_changed_database_snapshot() {
    let (router, fixture, _, principal) = setup(FormMode::Available);
    let (status, first) = get(&router, "/v1/tasks?page_size=1", Some(&principal)).await;
    assert_eq!(status, StatusCode::OK);
    let cursor = first["next_cursor"].as_str().unwrap();
    fixture.drift_snapshot.store(true, Ordering::Relaxed);
    let (status, body) = get(
        &router,
        &format!("/v1/tasks?page_size=1&cursor={cursor}"),
        Some(&principal),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body["code"], "internal_error");
    assert!(body.get("items").is_none());
    assert_eq!(fixture.mutations.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn cursor_cannot_change_subject_filters_page_size_or_survive_expiry() {
    let (router, fixture, clock, principal) = setup(FormMode::Available);
    let (_, first) = get(
        &router,
        "/v1/tasks?state=pending&page_size=1",
        Some(&principal),
    )
    .await;
    let cursor = first["next_cursor"].as_str().unwrap();
    for query in [
        format!("state=responded&page_size=1&cursor={cursor}"),
        format!("state=pending&page_size=2&cursor={cursor}"),
        format!("state=pending&page_size=1&purpose=viewable&cursor={cursor}"),
    ] {
        let (status, error) = get(&router, &format!("/v1/tasks?{query}"), Some(&principal)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(error["code"], "cursor_invalid");
    }
    let mut other = principal.clone();
    other.principal_id = id(ResourceKind::Principal, 9);
    let uri = format!("/v1/tasks?state=pending&page_size=1&cursor={cursor}");
    assert_eq!(
        get(&router, &uri, Some(&other)).await.1["code"],
        "cursor_invalid"
    );
    *clock.0.lock().unwrap() += Duration::seconds(PRODUCT_LIST_CURSOR_TTL_SECONDS + 1);
    let (status, expired) = get(&router, &uri, Some(&principal)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(expired["code"], "cursor_expired");
    assert_eq!(fixture.list_calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn task_list_rejects_unknown_fields_false_states_invalid_ids_and_unbounded_page_sizes() {
    let (router, fixture, _clock, principal) = setup(FormMode::Available);
    for query in [
        "include_hidden=true".to_owned(),
        "state=false".to_owned(),
        "kind=unknown".to_owned(),
        "page_size=0".to_owned(),
        "page_size=51".to_owned(),
        "page_size=65536".to_owned(),
        "state=pending&state=responded".to_owned(),
        format!("run_id={}", id(ResourceKind::Interaction, 3)),
    ] {
        let (status, error) = get(&router, &format!("/v1/tasks?{query}"), Some(&principal)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}");
        assert_eq!(error["code"], "invalid_request", "{query}");
        assert_eq!(error["retryable"], false);
    }
    assert!(fixture.list_calls.lock().unwrap().is_empty());
    let (status, _) = get(&router, "/v1/tasks", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(fixture.mutations.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn task_form_is_exact_or_explicitly_unavailable_and_never_creates_a_response() {
    for (mode, expected, code) in [
        (FormMode::Available, StatusCode::OK, None),
        (
            FormMode::OAuthUnavailable,
            StatusCode::CONFLICT,
            Some("task_form_unavailable"),
        ),
        (
            FormMode::Denied,
            StatusCode::FORBIDDEN,
            Some("permission_denied"),
        ),
        (
            FormMode::InvalidDigest,
            StatusCode::INTERNAL_SERVER_ERROR,
            Some("internal_error"),
        ),
        (
            FormMode::InvalidEtag,
            StatusCode::INTERNAL_SERVER_ERROR,
            Some("internal_error"),
        ),
    ] {
        let (router, fixture, _clock, principal) = setup(mode);
        let uri = format!("/v1/tasks/{}/form", id(ResourceKind::Interaction, 3));
        let (status, body) = get(&router, &uri, Some(&principal)).await;
        assert_eq!(status, expected);
        if let Some(code) = code {
            assert_eq!(body["code"], code);
            assert!(body.get("response_schema").is_none());
        } else {
            assert_eq!(body["generation"], 2);
            assert_eq!(body["version"], 3);
            assert_eq!(
                body["response_schema"],
                serde_json::to_value(fixture.schema()).unwrap()
            );
            assert_eq!(
                body["response_schema_digest"],
                body["response_schema"]["canonical_digest"]
            );
        }
        assert_eq!(fixture.form_calls.load(Ordering::Relaxed), 1);
        assert_eq!(get(&router, &uri, None).await.0, StatusCode::UNAUTHORIZED);
        assert_eq!(fixture.form_calls.load(Ordering::Relaxed), 1);
        assert_eq!(fixture.mutations.load(Ordering::Relaxed), 0);
    }
}

#[tokio::test]
async fn query_purpose_is_closed_and_forms_cannot_use_viewable() {
    let (router, fixture, _, principal) = setup(FormMode::Available);
    let task_id = fixture.task().task_id;
    for uri in [
        "/v1/tasks?purpose=all".to_owned(),
        format!("/v1/tasks/{task_id}?purpose=all"),
        format!("/v1/tasks/{task_id}?include_body=true"),
        format!("/v1/tasks/{task_id}/form?purpose=viewable"),
    ] {
        assert_eq!(
            get(&router, &uri, Some(&principal)).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(fixture.form_calls.load(Ordering::Relaxed), 0);
    assert_eq!(
        get(
            &router,
            &format!("/v1/tasks/{task_id}?purpose=viewable"),
            Some(&principal)
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        get(
            &router,
            &format!("/v1/tasks/{task_id}/form"),
            Some(&principal)
        )
        .await
        .0,
        StatusCode::OK
    );
}

#[test]
fn current_task_dtos_reject_missing_or_forged_actions_and_form_identity() {
    let (_, fixture, _, _) = setup(FormMode::Available);
    let original = serde_json::to_value(fixture.task()).unwrap();
    let mut missing = original.clone();
    missing.as_object_mut().unwrap().remove("allowed_actions");
    assert!(serde_json::from_value::<TaskViewV2>(missing).is_err());
    let mut old = original.clone();
    old["schema_version"] = json!(1);
    assert!(serde_json::from_value::<TaskViewV2>(old)
        .unwrap()
        .validate()
        .is_err());
    let mut unknown = original.clone();
    unknown["allowed_actions"] = json!(["delete"]);
    assert!(serde_json::from_value::<TaskViewV2>(unknown).is_err());
    let mut duplicate = fixture.task();
    duplicate.allowed_actions = vec![insight_platform_tasks::TaskAction::Cancel; 2];
    assert!(duplicate.validate().is_err());
    let mut terminal = fixture.task();
    terminal.state = TaskState::Cancelled;
    terminal.allowed_actions = vec![insight_platform_tasks::TaskAction::Cancel];
    assert!(terminal.validate().is_err());
    let view = fixture.task();
    let form = TaskFormV2 {
        schema_version: 2,
        task_id: view.task_id,
        generation: view.generation,
        version: view.version,
        allowed_actions: vec![insight_platform_tasks::TaskAction::SubmitInput],
        etag: view.etag,
        safe_prompt_key: view.safe_prompt_key,
        response_schema: fixture.schema(),
        response_schema_digest: fixture.schema().canonical_digest,
    };
    form.validate().unwrap();
    for required in ["allowed_actions", "etag", "safe_prompt_key"] {
        let mut document = serde_json::to_value(&form).unwrap();
        document.as_object_mut().unwrap().remove(required);
        assert!(serde_json::from_value::<TaskFormV2>(document).is_err());
    }
    let mut stale = form;
    stale.etag = "\"wrong\"".into();
    assert!(stale.validate().is_err());
}
