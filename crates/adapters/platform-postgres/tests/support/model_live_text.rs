//! Real PostgreSQL + Core NATS + production Gateway SSE, using an explicitly fresh test database.
use super::*;
use axum::{body::Body, http::Request, Extension};
use futures::StreamExt;
use insight_platform_api::{
    authentication::AuthenticatedPrincipal, run_live::build_run_live_router,
};
use insight_platform_gateway::live_text::{LiveTextConfig, PgLiveText};
use insight_platform_models::{model_live_delta_subject, ModelLiveTextDelta};
use tower::ServiceExt;

pub(super) async fn verify() {
    let database_url = std::env::var("PLATFORM_LIVE_TEST_DATABASE_URL")
        .expect("fresh live fixture database required");
    assert!(database_url
        .rsplit('/')
        .next()
        .unwrap()
        .starts_with("insight_live_fixture_"));
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await
        .unwrap();
    insight_platform_postgres::provision_schema(&pool)
        .await
        .unwrap();
    let repository = Arc::new(PgRepository::new(pool.clone()));
    let fixture = seed_fixture(&pool, &repository).await;
    let command = command_for_node(&fixture, &fixture.primary_node_id, 0x100);
    let admitted = match execute_create(&repository, command.clone()).await.unwrap() {
        CommandOutcome::Applied(v) => v,
        _ => panic!("fresh admission"),
    };
    execute_prepare(
        &repository,
        PrepareModelDispatch {
            audit: audit(&fixture.tenant_id, &fixture.principal_id, 0x121, '9', 'a'),
            model_turn_id: command.model_turn_id.clone(),
            expected_turn_version: admitted.version,
            job_id: id(ResourceKind::Job, 0x120),
            scheduled_at: Utc::now() - Duration::seconds(1),
        },
    )
    .await
    .unwrap();
    let claim = claim_one(&repository, 0x130).await;
    let reader_id = id(ResourceKind::Principal, 0x901);
    repository
        .create_principal(NewPrincipal {
            principal_id: reader_id.clone(),
            authentication_authority_digest: named_digest("live_reader_authority"),
            subject_digest: named_digest("live_reader"),
            installation_bindings: PrincipalBindingsPayload {
                installation_bindings: vec![],
            },
        })
        .await
        .unwrap();
    let permissions =
        PermissionSet::new(vec![Permission::RuntimeRead, Permission::ArtifactRead]).unwrap();
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: fixture.tenant_id.clone(),
            principal_id: reader_id.clone(),
            principal_kind: PrincipalKind::AgentRunner,
            payload: TenantPrincipalPayload {
                permissions: permissions.clone(),
            },
        })
        .await
        .unwrap();
    let principal = AuthenticatedPrincipal {
        tenant_id: fixture.tenant_id.clone(),
        principal_id: reader_id.clone(),
        principal_kind: PrincipalKind::AgentRunner,
        permissions,
        authn_strength: insight_platform_contracts::AuthnStrength::MultiFactor,
        principal_version: 1,
        binding_generation: 1,
        binding_version: 1,
        credential_digest: named_digest("live_reader_credential"),
        credential_expires_at: Utc::now() + Duration::minutes(5),
        trace: insight_platform_contracts::TraceIdentityV1::generate(),
    };
    let server = std::env::var("PLATFORM_LIVE_NATS_URL").unwrap();
    let ca = PathBuf::from(std::env::var("PLATFORM_LIVE_NATS_CA").unwrap());
    let certificate = PathBuf::from(std::env::var("PLATFORM_LIVE_NATS_CERT").unwrap());
    let key = PathBuf::from(std::env::var("PLATFORM_LIVE_NATS_KEY").unwrap());
    let gateway = PgLiveText::from_tls_files(
        repository.clone(),
        LiveTextConfig {
            servers: vec![server.clone()],
            namespace: "fixture".into(),
            connect_timeout_milliseconds: 3000,
        },
        ca.clone(),
        certificate.clone(),
        key.clone(),
    )
    .unwrap();
    let router = build_run_live_router(Arc::new(gateway)).layer(Extension(principal));
    let response = router
        .oneshot(
            Request::builder()
                .uri(format!("/v1/runs/{}/live-text", fixture.run_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let mut body = response.into_body().into_data_stream();
    let opened = tokio::time::timeout(StdDuration::from_secs(3), body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(std::str::from_utf8(&opened).unwrap().contains("opened"));
    let publisher = async_nats::ConnectOptions::new()
        .require_tls(true)
        .add_root_certificates(ca)
        .add_client_certificate(certificate, key)
        .connect(server)
        .await
        .unwrap();
    let subject = model_live_delta_subject("fixture", &fixture.tenant_id, &fixture.run_id).unwrap();
    let mut delta = ModelLiveTextDelta {
        schema_version: 2,
        tenant_id: fixture.tenant_id.clone(),
        run_id: fixture.run_id.clone(),
        model_turn_id: claim.claimed.turn.model_turn_id.clone(),
        job_id: claim.claimed.job.job_id.parse().unwrap(),
        worker_process_generation_id: claim.worker_id.clone(),
        attempt_no: claim.claimed.job.attempt_no as u32,
        lease_generation: claim.claimed.job.lease_epoch as u64,
        transport_sequence: 1,
        text_sequence: 1,
        request_digest: claim.claimed.turn.payload.admission.request_digest.clone(),
        classification: claim.claimed.turn.payload.admission.request.classification,
        text: "你好，真实流式".into(),
    };
    let mut stale = delta.clone();
    stale.lease_generation += 1;
    stale.text = "STALE_FENCE_MUST_NOT_LEAK".into();
    publisher
        .publish(subject.clone(), serde_json::to_vec(&stale).unwrap().into())
        .await
        .unwrap();
    publisher
        .publish(subject.clone(), serde_json::to_vec(&delta).unwrap().into())
        .await
        .unwrap();
    publisher.flush().await.unwrap();
    let mut received = String::new();
    tokio::time::timeout(StdDuration::from_secs(5), async {
        while !received.contains("你好，真实流式") {
            received.push_str(std::str::from_utf8(&body.next().await.unwrap().unwrap()).unwrap());
        }
    })
    .await
    .unwrap();
    assert!(!received.contains("STALE_FENCE"));
    let state: String = sqlx::query_scalar(
        "SELECT state FROM insight_platform.invocations WHERE tenant_id=$1 AND invocation_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(delta.model_turn_id.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        state, "in_flight",
        "first live text must arrive before durable terminal"
    );
    delta.transport_sequence = 3;
    delta.text_sequence = 2;
    delta.text = "第二段".into();
    publisher
        .publish(subject, serde_json::to_vec(&delta).unwrap().into())
        .await
        .unwrap();
    publisher.flush().await.unwrap();
    let mut second = String::new();
    tokio::time::timeout(StdDuration::from_secs(5), async {
        while !second.contains("第二段") {
            second.push_str(std::str::from_utf8(&body.next().await.unwrap().unwrap()).unwrap());
        }
    })
    .await
    .unwrap();
    assert!(
        !second.contains("gap"),
        "private metadata transport gap is not text loss"
    );
    sqlx::query("UPDATE insight_platform.tenant_principals SET state='revoked',version=version+1,updated_at=clock_timestamp() WHERE tenant_id=$1 AND principal_id=$2").bind(fixture.tenant_id.to_string()).bind(reader_id.to_string()).execute(&pool).await.unwrap();
    let closed = tokio::time::timeout(StdDuration::from_secs(3), body.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(std::str::from_utf8(&closed)
        .unwrap()
        .contains("authorization_changed"));
    assert!(body.next().await.is_none());
}
