//! Public projections are checked through current-principal reads after real owner transactions.
use super::*;
use insight_platform_orchestrator::history::{PublicRunEventRecord, PublicRunReadPosition};

pub(super) async fn snapshot(pool: &PgPool, fixture: &Fixture) -> serde_json::Value {
    sqlx::query_scalar(
        r#"SELECT jsonb_build_object(
          'sequence', (SELECT public_sequence FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2),
          'events', (SELECT count(*) FROM insight_platform.events WHERE tenant_id=$1),
          'outbox', (SELECT count(*) FROM insight_platform.outbox_events WHERE tenant_id=$1),
          'receipts', (SELECT count(*) FROM insight_platform.receipts WHERE tenant_id=$1),
          'ledger', (SELECT count(*) FROM insight_platform.quota_ledger WHERE tenant_id=$1),
          'quota', (SELECT jsonb_agg(jsonb_build_array(quota_account_id,version,reserved_value,used_value) ORDER BY quota_account_id)
                    FROM insight_platform.quota_accounts WHERE tenant_id=$1))"#,
    )
    .bind(fixture.tenant_id.to_string())
    .bind(fixture.run_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap()
}

pub(super) async fn assert_projection(
    pool: &PgPool,
    repository: &PgRepository,
    fixture: &Fixture,
    required: &[&str],
) {
    let mut records: Vec<PublicRunEventRecord> = Vec::new();
    let mut position = PublicRunReadPosition::Initial;
    // Small pages prove continuation under the actual Run-local sequence authority.
    for page_no in 0..64 {
        let page = repository
            .read_public_run_events_for_principal(
                &fixture.tenant_id,
                &fixture.principal_id,
                PrincipalKind::AgentRunner,
                &fixture.run_id,
                position,
                2,
            )
            .await
            .unwrap();
        let full = page.events.len() == 2;
        for event in page.events {
            assert_eq!(
                event.sequence,
                records
                    .last()
                    .map_or(page.replay_floor + 1, |previous| previous.sequence + 1)
            );
            assert!(event.source_projection_version > 0);
            let safe = serde_json::to_value(&event).unwrap();
            let keys: std::collections::BTreeSet<_> = safe
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect();
            assert_eq!(
                keys,
                std::collections::BTreeSet::from([
                    "event_id",
                    "safe_summary",
                    "trace_id",
                    "sequence",
                    "event_type",
                    "source_id",
                    "source_projection_version",
                    "occurred_at",
                ])
            );
            assert!(!records.iter().any(|old| old.event_id == event.event_id));
            position = PublicRunReadPosition::AfterSequence {
                sequence: event.sequence,
            };
            records.push(event);
        }
        if !full {
            assert_eq!(
                records
                    .last()
                    .map_or(page.replay_floor, |last| last.sequence),
                page.high_water_sequence
            );
            break;
        }
        assert!(page_no < 63, "bounded public page traversal exhausted");
    }
    for name in required {
        assert!(
            records
                .iter()
                .any(|event| event.event_type.as_str() == *name),
            "missing durable public event {name}"
        );
    }
    // Compare all six durable Model rows, including the explicit command path, independently
    // of their stored visibility. A missing run_id cannot silently disappear from this proof.
    let expected: Vec<(String, String, i64, String, Option<String>, String)> = sqlx::query_as(
        r#"SELECT event.event_id,event.aggregate_id,event.aggregate_version,event.event_type,event.run_id,event.visibility
           FROM insight_platform.events event JOIN insight_platform.invocations turn
             ON turn.tenant_id=event.tenant_id AND turn.invocation_id=event.aggregate_id
           WHERE event.tenant_id=$1 AND turn.run_id=$2 AND event.aggregate_kind='model_turn'
             AND event.event_type IN ('model.started','model.tool_intent','model.completed','model.failed','model.cancelled','model.timed_out')"#,
    ).bind(fixture.tenant_id.to_string()).bind(fixture.run_id.to_string()).fetch_all(pool).await.unwrap();
    for (event_id, turn_id, version, event_type, run_id, visibility) in expected {
        assert_eq!(run_id.as_deref(), Some(fixture.run_id.to_string().as_str()));
        assert_eq!(visibility, "public");
        let actual = records
            .iter()
            .find(|record| record.event_id.to_string() == event_id)
            .expect("durable Model row is publicly readable");
        assert_eq!(actual.event_type.as_str(), event_type);
        assert_eq!(actual.source_id.to_string(), turn_id);
        assert_eq!(
            actual.source_projection_version,
            u64::try_from(version).unwrap()
        );
        assert_eq!(actual.source_id.kind(), ResourceKind::ModelTurn);
    }
    let internal: i64 = sqlx::query_scalar("SELECT count(*) FROM insight_platform.events WHERE tenant_id=$1 AND run_id=$2 AND event_type IN ('model.delta','model.retry_scheduled','model.cancelling') AND (visibility <> 'internal' OR public_sequence IS NOT NULL)")
        .bind(fixture.tenant_id.to_string()).bind(fixture.run_id.to_string()).fetch_one(pool).await.unwrap();
    assert_eq!(internal, 0);
    assert!(
        repository
            .read_public_run_events_for_principal(
                &fixture.tenant_id,
                &id(ResourceKind::Principal, 0xffe0),
                PrincipalKind::AgentRunner,
                &fixture.run_id,
                PublicRunReadPosition::Initial,
                2,
            )
            .await
            .is_err(),
        "unrelated principal cannot read the stream"
    );
}

pub(super) async fn assert_read_denied(repository: &PgRepository, fixture: &Fixture) {
    assert!(matches!(
        repository
            .read_public_run_events_for_principal(
                &fixture.tenant_id,
                &fixture.principal_id,
                PrincipalKind::AgentRunner,
                &fixture.run_id,
                PublicRunReadPosition::Initial,
                2,
            )
            .await,
        Err(RepositoryError::PermissionDenied)
    ));
}

pub(super) async fn verify_timeout(pool: &PgPool, repository: &PgRepository, original: &Fixture) {
    let mut fixture = model_recovery_isolation::isolated_fixture(pool, original).await;
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(pool)
        .await
        .unwrap();
    fixture.deadline = now + Duration::seconds(3);
    let leaf = model_recovery_isolation::admit(pool, repository, &fixture, 0xe000).await;
    let claim = claim_one(repository, 0xe100).await;
    assert_eq!(claim.claimed.turn.model_turn_id, leaf.turn.model_turn_id);
    let command = ControlModelTurn {
        audit: audit(&fixture.tenant_id, &fixture.principal_id, 0xe200, 'a', 'b'),
        model_turn_id: claim.claimed.turn.model_turn_id.clone(),
        expected_turn_version: claim.claimed.turn.version,
        quota_entry_ids: (0xe210..=0xe213)
            .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
            .collect(),
        kind: ModelControlKind::Timeout,
    };
    let before = snapshot(pool, &fixture).await;
    assert!(
        execute_control(repository, command.clone()).await.is_err(),
        "timeout before the actual deadline must roll back"
    );
    assert_eq!(snapshot(pool, &fixture).await, before);
    tokio::time::timeout(StdDuration::from_secs(5), async {
        loop {
            let expired: bool = sqlx::query_scalar("SELECT clock_timestamp() >= $1")
                .bind(fixture.deadline)
                .fetch_one(pool)
                .await
                .unwrap();
            if expired {
                break;
            }
            tokio::time::sleep(StdDuration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let CommandOutcome::Applied(controlled) =
        execute_control(repository, command.clone()).await.unwrap()
    else {
        panic!("fresh timeout");
    };
    assert_eq!(controlled.turn.state, ModelTurnState::TimedOut);
    assert_eq!(controlled.job.unwrap().state, "timed_out");
    assert_projection(
        pool,
        repository,
        &fixture,
        &["model.started", "model.timed_out"],
    )
    .await;
    let after = snapshot(pool, &fixture).await;
    assert!(matches!(
        execute_control(repository, command).await.unwrap(),
        CommandOutcome::Replayed(_)
    ));
    assert_eq!(snapshot(pool, &fixture).await, after);
    let reserved: i64 = sqlx::query_scalar("SELECT coalesce(sum(reserved_value),0)::bigint FROM insight_platform.quota_accounts WHERE tenant_id=$1 AND work_class='model'")
        .bind(fixture.tenant_id.to_string()).fetch_one(pool).await.unwrap();
    assert_eq!(reserved, 0);
}

pub(super) async fn verify_permanent_failure(
    pool: &PgPool,
    repository: &PgRepository,
    original: &Fixture,
) {
    let fixture = model_recovery_isolation::isolated_fixture(pool, original).await;
    let leaf = model_recovery_isolation::admit(pool, repository, &fixture, 0xe400).await;
    let claim = claim_one(repository, 0xe500).await;
    assert_eq!(claim.claimed.turn.model_turn_id, leaf.turn.model_turn_id);
    let ModelExecutionInputMaterial::Inline { value } = &claim.claimed.request_input.material;
    let command = CommitModelOutcome {
        audit: worker_audit(&fixture.tenant_id, &claim.worker_id, 0xe600, 'c', 'd'),
        model_turn_id: claim.claimed.turn.model_turn_id.clone(),
        job_id: claim.claimed.job.job_id.parse().unwrap(),
        expected_turn_version: claim.claimed.turn.version,
        fence: claim.fence(),
        usage_reservation_id: claim.usage_reservation_id.clone(),
        quota_entry_ids: (0xe610..=0xe613)
            .map(|suffix| id(ResourceKind::QuotaLedgerEntry, suffix))
            .collect(),
        request: serde_json::from_value(value.clone()).unwrap(),
        outcome: ModelDispatchOutcome::PermanentFailure {
            failure: model_failure(
                insight_platform_contracts::FailureClass::External,
                insight_platform_contracts::Retryability::Never,
            ),
            measurement: measurement(10, 0, 10),
        },
        resume_mutations: None,
        failure_mutations: Some(failure_mutations(0xe620)),
        tool_continuation_mutations: None,
    };
    let CommandOutcome::Applied(result) =
        execute_outcome(repository, command.clone()).await.unwrap()
    else {
        panic!("fresh permanent failure");
    };
    assert_eq!(result.turn.state, ModelTurnState::Failed);
    assert_projection(
        pool,
        repository,
        &fixture,
        &["model.started", "model.failed"],
    )
    .await;
    let after = snapshot(pool, &fixture).await;
    assert!(matches!(
        execute_outcome(repository, command).await.unwrap(),
        CommandOutcome::Replayed(_)
    ));
    assert_eq!(snapshot(pool, &fixture).await, after);
}
