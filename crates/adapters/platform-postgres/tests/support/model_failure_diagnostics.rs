//! Real failure settlement and authorized event projection; no runtime schema changes.
use super::*;
use insight_platform_orchestrator::history::PublicRunReadPosition;

async fn summary(repository: &PgRepository, fixture: &Fixture) -> Option<String> {
    let page = repository
        .read_public_run_events_for_principal(
            &fixture.tenant_id,
            &fixture.principal_id,
            PrincipalKind::AgentRunner,
            &fixture.run_id,
            PublicRunReadPosition::Initial,
            100,
        )
        .await
        .unwrap();
    page.events
        .into_iter()
        .find(|event| {
            event.event_type == insight_platform_contracts::PublicRunEventType::ModelFailed
        })
        .expect("committed model.failed")
        .safe_summary
}

pub(super) async fn verify() {
    let url = std::env::var("PLATFORM_LIVE_TEST_DATABASE_URL").expect("fresh fixture required");
    assert!(url
        .rsplit('/')
        .next()
        .unwrap()
        .starts_with("insight_live_fixture_"));
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .unwrap();
    insight_platform_postgres::provision_schema(&pool)
        .await
        .unwrap();
    let repository = PgRepository::new(pool.clone());
    let fixture = seed_fixture(&pool, &repository).await;
    let command = command_for_node(&fixture, &fixture.primary_node_id, 0x100);
    let admitted = match execute_create(&repository, command.clone()).await.unwrap() {
        CommandOutcome::Applied(value) => value,
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
    let mut failure = model_failure(
        insight_platform_contracts::FailureClass::External,
        insight_platform_contracts::Retryability::Never,
    );
    let message = insight_platform_models::model_failure_safe_message(
        "model_structured_output_schema_mismatch",
    );
    failure.safe_code = "model_structured_output_schema_mismatch".into();
    failure.failure.safe_message = Some(message.into());
    execute_outcome(
        &repository,
        CommitModelOutcome {
            audit: worker_audit(&fixture.tenant_id, &claim.worker_id, 0x190, '7', '8'),
            model_turn_id: command.model_turn_id.clone(),
            job_id: id(ResourceKind::Job, 0x120),
            expected_turn_version: claim.claimed.turn.version,
            fence: claim.fence(),
            usage_reservation_id: claim.usage_reservation_id,
            quota_entry_ids: (0..4)
                .map(|offset| id(ResourceKind::QuotaLedgerEntry, 0x193 + offset))
                .collect(),
            request: command.request.request,
            outcome: ModelDispatchOutcome::PermanentFailure {
                failure,
                measurement: measurement(1, 0, 1),
            },
            resume_mutations: None,
            failure_mutations: None,
            tool_continuation_mutations: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        summary(&repository, &fixture).await.as_deref(),
        Some(message)
    );
    // Exact source version is mandatory even when the current message remains a recognized constant.
    sqlx::query("UPDATE insight_platform.invocations SET version=version+1 WHERE tenant_id=$1 AND invocation_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(command.model_turn_id.to_string()).execute(&pool).await.unwrap();
    assert_eq!(summary(&repository, &fixture).await, None);
    sqlx::query("UPDATE insight_platform.invocations SET version=version-1 WHERE tenant_id=$1 AND invocation_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(command.model_turn_id.to_string()).execute(&pool).await.unwrap();
    // A source belonging to a different Run cannot annotate this event.
    sqlx::query("UPDATE insight_platform.invocations SET run_id=NULL WHERE tenant_id=$1 AND invocation_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(command.model_turn_id.to_string()).execute(&pool).await.unwrap();
    assert_eq!(summary(&repository, &fixture).await, None);
    sqlx::query(
        "UPDATE insight_platform.invocations SET run_id=$3 WHERE tenant_id=$1 AND invocation_id=$2",
    )
    .bind(fixture.tenant_id.to_string())
    .bind(command.model_turn_id.to_string())
    .bind(fixture.run_id.to_string())
    .execute(&pool)
    .await
    .unwrap();
    for message in ["secret-provider-canary", "Model Provider request failed"] {
        sqlx::query("UPDATE insight_platform.invocations SET payload=jsonb_set(payload,'{failure,safe_message}',to_jsonb($3::text)) WHERE tenant_id=$1 AND invocation_id=$2")
            .bind(fixture.tenant_id.to_string()).bind(command.model_turn_id.to_string()).bind(message).execute(&pool).await.unwrap();
        assert_eq!(summary(&repository, &fixture).await, None);
    }
    assert!(repository
        .read_public_run_events_for_principal(
            &id(ResourceKind::Tenant, 0x999),
            &fixture.principal_id,
            PrincipalKind::AgentRunner,
            &fixture.run_id,
            PublicRunReadPosition::Initial,
            100
        )
        .await
        .is_err());
}
