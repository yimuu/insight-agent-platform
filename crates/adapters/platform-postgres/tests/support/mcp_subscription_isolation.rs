//! One independently admitted subscription supplies invalid heads for both recovery pages.
use super::*;
use insight_platform_jobs::store::{JobRecord, SafetyScanCursor, SafetyScanPage, SafetyScanPhase};
use insight_platform_mcp_host::DueMcpSubscriptionRecovery;

pub(super) struct IsolationCase {
    fixture: Fixture,
    context_job_id: ResourceId,
    context_reservation_id: ResourceId,
    context_payload_digest: String,
    retained: serde_json::Value,
}

async fn snapshot(pool: &PgPool, case: &IsolationCase) -> serde_json::Value {
    sqlx::query_scalar(r#"
        SELECT jsonb_build_object(
            'subscription',(SELECT to_jsonb(i) FROM insight_platform.invocations i WHERE tenant_id=$1 AND invocation_id=$2),
            'jobs',(SELECT jsonb_agg(to_jsonb(j) ORDER BY job_id) FROM insight_platform.jobs j WHERE tenant_id=$1 AND job_id=ANY($3)),
            'receipts',(SELECT jsonb_agg(to_jsonb(r) ORDER BY receipt_id) FROM insight_platform.receipts r WHERE tenant_id=$1 AND scope_id=ANY($4)),
            'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY event_id) FROM insight_platform.events e WHERE tenant_id=$1 AND aggregate_id=ANY($4)),
            'outbox',(SELECT jsonb_agg(to_jsonb(o) ORDER BY outbox_id) FROM insight_platform.outbox_events o WHERE tenant_id=$1 AND event_id IN(SELECT event_id FROM insight_platform.events WHERE tenant_id=$1 AND aggregate_id=ANY($4))),
            'quota',(SELECT jsonb_agg(to_jsonb(q) ORDER BY quota_entry_id) FROM insight_platform.quota_ledger q WHERE tenant_id=$1 AND correlation_id=$5))
        "#)
        .bind(case.fixture.tenant_id.to_string())
        .bind(case.fixture.subscription_id.to_string())
        .bind(vec![case.fixture.job_id.to_string(), case.context_job_id.to_string()])
        .bind(vec![case.fixture.subscription_id.to_string(), case.fixture.job_id.to_string(), case.context_job_id.to_string()])
        .bind(case.context_reservation_id.to_string())
        .fetch_one(pool).await.unwrap()
}

fn refresh_recovery(
    after: Option<SafetyScanCursor>,
    base: u16,
) -> DriveExpiredContextSubscriptionRefreshJobs {
    DriveExpiredContextSubscriptionRefreshJobs {
        after,
        shard_index: 0,
        shard_count: 1,
        limit: 1,
        retry_backoff_milliseconds: 60_000,
        slots: vec![ContextSubscriptionRefreshRecoverySlot {
            quota_settlement_entry_id: id(ResourceKind::QuotaLedgerEntry, base),
            event_id: id(ResourceKind::Event, base + 1),
            outbox_id: id(ResourceKind::OutboxEvent, base + 2),
        }],
    }
}

pub(super) async fn seed_invalid_refresh(
    pool: &PgPool,
    repo: &PgRepository,
    original: &Fixture,
) -> IsolationCase {
    let now = Utc::now();
    let mut fixture = original.clone();
    fixture.subscription_id = id(ResourceKind::McpOperation, 0x8000);
    fixture.job_id = id(ResourceKind::Job, 0x8001);
    let mut command = create_command(&fixture, now);
    command.audit = command_audit(&fixture.tenant_id, &fixture.principal_id, 0x8010, now);
    command.logical_key = "recovery-isolation-subscription".to_owned();
    command.audit.request_digest = command.request_digest().unwrap();
    let CommandOutcome::Applied(mut record) = create_subscription(repo, command).await.unwrap()
    else {
        panic!("fresh isolation subscription must apply");
    };
    let worker = id(ResourceKind::WorkerProcessGeneration, 0x8002);
    let mut fence = claim_and_start(repo, &fixture, &worker, "isolation-session").await;
    for (target, base) in [
        (McpSessionState::Connecting, 0x8020),
        (McpSessionState::Initializing, 0x8030),
        (McpSessionState::Ready, 0x8040),
    ] {
        let ready = target == McpSessionState::Ready;
        let command = session_command(
            &fixture,
            &worker,
            fence.clone(),
            &record,
            SessionCommandInput {
                target,
                opaque: ready.then(|| EncryptedMcpState {
                    scheme: "aes256_gcm_v1".to_owned(),
                    ciphertext: b"isolation-encrypted-session".to_vec(),
                    key_id: "isolation-session-key".to_owned(),
                    key_reference_digest: named_digest("isolation-session-key"),
                    plaintext_digest: named_digest("isolation-session"),
                }),
                expires_at: ready.then_some(now + Duration::minutes(30)),
                base,
                now,
            },
        );
        let CommandOutcome::Applied(next) =
            repo.save_mcp_subscription_session(command).await.unwrap()
        else {
            panic!("fresh isolation session transition");
        };
        record = next;
        fence.expected_version += 1;
    }
    assert_eq!(record.payload.session.state, McpSessionState::Ready);
    let accepted = repo
        .admit_context_subscription_refresh(context_reconcile_admission(&record, 0x8050))
        .await
        .unwrap();
    let claims = repo
        .claim_refresh_fixture(ClaimContextSubscriptionRefreshJobs {
            worker_manifest: context_worker_manifest(),
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 0x8051),
            worker_manifest_digest: context_worker_manifest_digest(),
            slots: vec![ContextSubscriptionRefreshClaimSlot {
                lease_token_digest: named_digest("isolation-refresh-lease"),
                quota_reservation_id: id(ResourceKind::UsageReservation, 0x8052),
                quota_reserve_entry_id: id(ResourceKind::QuotaLedgerEntry, 0x8053),
                event_id: id(ResourceKind::Event, 0x8054),
                outbox_id: id(ResourceKind::OutboxEvent, 0x8055),
            }],
            lease_policy: LeasePolicy {
                requested_milliseconds: 30_000,
                hard_maximum_milliseconds: 30_000,
            },
        })
        .await
        .unwrap();
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].job.job_id, accepted.job_id.to_string());
    assert_eq!(claims[0].attempt.attempt_number, 1);
    let context_payload_digest = claims[0].job.payload.digest.clone();
    sqlx::query("UPDATE insight_platform.jobs SET heartbeat_at=clock_timestamp()-interval '4 seconds',lease_expires_at=clock_timestamp()-interval '3 seconds',payload_digest=$3 WHERE tenant_id=$1 AND job_id=$2")
        .bind(fixture.tenant_id.to_string()).bind(accepted.job_id.to_string())
        .bind(named_digest("invalid owning refresh payload").to_string()).execute(pool).await.unwrap();
    let mut case = IsolationCase {
        fixture,
        context_job_id: accepted.job_id,
        context_reservation_id: id(ResourceKind::UsageReservation, 0x8052),
        context_payload_digest,
        retained: serde_json::Value::Null,
    };
    case.retained = snapshot(pool, &case).await;
    case
}

pub(super) async fn recover_context_after_bad_head(
    pool: &PgPool,
    repo: &PgRepository,
    case: &IsolationCase,
    good_job: &JobRecord,
    mut command: DriveExpiredContextSubscriptionRefreshJobs,
) -> SafetyScanPage<JobRecord> {
    let first = repo
        .drive_expired_context_subscription_refresh_jobs(refresh_recovery(None, 0x8060))
        .await
        .unwrap();
    assert!(first.records.is_empty() && !first.exhausted);
    assert_eq!(first.diagnostics.len(), 1);
    assert_eq!(first.diagnostics[0].item_id, case.context_job_id);
    assert_eq!(first.diagnostics[0].phase, SafetyScanPhase::JobDecode);
    let cursor = first.next_cursor.unwrap();
    assert_eq!(cursor.item_id, case.context_job_id);
    assert_eq!(snapshot(pool, case).await, case.retained);
    command.after = Some(cursor);
    let recovered = repo
        .drive_expired_context_subscription_refresh_jobs(command)
        .await
        .unwrap();
    assert_eq!(recovered.records.len(), 1);
    assert_eq!(recovered.records[0].job_id, good_job.job_id);
    assert_eq!(recovered.records[0].state, "retry_scheduled");
    assert!(recovered.diagnostics.is_empty());
    assert_eq!(
        recovered.next_cursor.as_ref().unwrap().item_id.to_string(),
        good_job.job_id
    );
    assert_eq!(snapshot(pool, case).await, case.retained);
    let settlement_count: i64 = sqlx::query_scalar("SELECT count(*) FROM insight_platform.quota_ledger WHERE tenant_id=$1 AND correlation_id=$2 AND entry_kind='settle'")
        .bind(&good_job.tenant_id).bind(good_job.quota_reservation_id.as_ref().unwrap()).fetch_one(pool).await.unwrap();
    assert_eq!(settlement_count, 1);
    let end = repo
        .drive_expired_context_subscription_refresh_jobs(refresh_recovery(
            recovered.next_cursor.clone(),
            0x8070,
        ))
        .await
        .unwrap();
    assert!(end.records.is_empty() && end.diagnostics.is_empty() && end.exhausted);
    let repeat = repo
        .drive_expired_context_subscription_refresh_jobs(refresh_recovery(None, 0x8080))
        .await
        .unwrap();
    assert!(repeat.records.is_empty());
    assert_eq!(repeat.diagnostics.len(), 1);
    assert_eq!(snapshot(pool, case).await, case.retained);
    recovered
}

pub(super) async fn recover_subscription(
    pool: &PgPool,
    repo: &PgRepository,
    fixture: &Fixture,
    connecting: &McpSubscriptionRecord,
    now: DateTime<Utc>,
    case: Option<IsolationCase>,
) -> McpSubscriptionRecord {
    let mut original_payload = None;
    let mut original_created_at = None;
    let mut bad_snapshot = None;
    let mut after = None;
    if let Some(case) = &case {
        assert_eq!(snapshot(pool, case).await, case.retained);
        let row = sqlx::query("SELECT payload_schema_version,payload,payload_digest,created_at FROM insight_platform.invocations WHERE tenant_id=$1 AND invocation_id=$2")
            .bind(case.fixture.tenant_id.to_string()).bind(case.fixture.subscription_id.to_string()).fetch_one(pool).await.unwrap();
        let payload = TypedPayload {
            schema_version: row.try_get("payload_schema_version").unwrap(),
            value: row.try_get("payload").unwrap(),
            digest: row.try_get("payload_digest").unwrap(),
        };
        original_created_at = Some(row.try_get::<DateTime<Utc>, _>("created_at").unwrap());
        serde_json::from_value::<insight_platform_mcp_host::McpSubscriptionPayload>(
            payload.value.clone(),
        )
        .unwrap();
        let mut invalid = payload.value.clone();
        invalid["session"]["expires_at"] = serde_json::json!("invalid-timestamp");
        let invalid =
            TypedPayload::from_versioned(payload.schema_version, &invalid, 1_048_576).unwrap();
        assert!(
            serde_json::from_value::<insight_platform_mcp_host::McpSubscriptionPayload>(
                invalid.value.clone()
            )
            .is_err()
        );
        sqlx::query("UPDATE insight_platform.invocations SET created_at=(SELECT created_at-interval '1 second' FROM insight_platform.invocations WHERE tenant_id=$1 AND invocation_id=$3),payload=$4,payload_digest=$5 WHERE tenant_id=$1 AND invocation_id=$2")
            .bind(case.fixture.tenant_id.to_string()).bind(case.fixture.subscription_id.to_string())
            .bind(fixture.subscription_id.to_string()).bind(&invalid.value).bind(&invalid.digest).execute(pool).await.unwrap();
        original_payload = Some(payload);
        let before = snapshot(pool, case).await;
        let first = repo
            .list_due_mcp_subscription_recoveries_global(1, None)
            .await
            .unwrap();
        assert!(first.records.is_empty() && !first.exhausted);
        assert_eq!(first.diagnostics.len(), 1);
        assert_eq!(first.diagnostics[0].item_id, case.fixture.subscription_id);
        assert_eq!(first.diagnostics[0].phase, SafetyScanPhase::OwnerDecode);
        assert_eq!(
            first.next_cursor.as_ref().unwrap().item_id,
            case.fixture.subscription_id
        );
        after = first.next_cursor;
        assert_eq!(snapshot(pool, case).await, before);
        bad_snapshot = Some(before);
    }
    let recoveries: SafetyScanPage<DueMcpSubscriptionRecovery> = if case.is_some() {
        repo.list_due_mcp_subscription_recoveries_global(1, after)
            .await
            .unwrap()
    } else {
        repo.list_due_mcp_subscription_recoveries(McpSubscriptionRecoveryScan {
            after: None,
            tenant_id: fixture.tenant_id.clone(),
            limit: 8,
        })
        .await
        .unwrap()
    };
    assert_eq!(recoveries.records.len(), 1);
    assert!(recoveries.diagnostics.is_empty());
    assert_eq!(
        recoveries.records[0].subscription_id,
        fixture.subscription_id
    );
    assert_eq!(recoveries.records[0].job_id, fixture.job_id);
    assert_eq!(
        recoveries.records[0].cause,
        McpSubscriptionRecoveryCause::ExpiredLease
    );
    assert_eq!(
        recoveries.records[0].subscription_version,
        connecting.version
    );
    let recovery_worker = id(ResourceKind::WorkerProcessGeneration, 0x330);
    let mut recovery = RecoverDueMcpSubscription {
        audit: worker_audit(&fixture.tenant_id, &recovery_worker, 0x340, now),
        candidate: recoveries.records[0].clone(),
    };
    recovery.audit.request_digest = recovery.request_digest().unwrap();
    let CommandOutcome::Applied(recovered) = repo
        .recover_due_mcp_subscription(recovery.clone())
        .await
        .unwrap()
    else {
        panic!("expired lease recovery must apply");
    };
    let main_before_replay: serde_json::Value = sqlx::query_scalar("SELECT jsonb_build_object('subscription',(SELECT to_jsonb(i) FROM insight_platform.invocations i WHERE tenant_id=$1 AND invocation_id=$2),'job',(SELECT to_jsonb(j) FROM insight_platform.jobs j WHERE tenant_id=$1 AND job_id=$3))")
        .bind(fixture.tenant_id.to_string()).bind(fixture.subscription_id.to_string()).bind(fixture.job_id.to_string()).fetch_one(pool).await.unwrap();
    assert!(matches!(
        repo.recover_due_mcp_subscription(recovery).await.unwrap(),
        CommandOutcome::Replayed(_)
    ));
    let main_after_replay: serde_json::Value = sqlx::query_scalar("SELECT jsonb_build_object('subscription',(SELECT to_jsonb(i) FROM insight_platform.invocations i WHERE tenant_id=$1 AND invocation_id=$2),'job',(SELECT to_jsonb(j) FROM insight_platform.jobs j WHERE tenant_id=$1 AND job_id=$3))")
        .bind(fixture.tenant_id.to_string()).bind(fixture.subscription_id.to_string()).bind(fixture.job_id.to_string()).fetch_one(pool).await.unwrap();
    assert_eq!(main_before_replay, main_after_replay);
    if let Some(case) = case {
        assert_eq!(
            recoveries.next_cursor.as_ref().unwrap().item_id,
            fixture.subscription_id
        );
        let end = repo
            .list_due_mcp_subscription_recoveries_global(1, recoveries.next_cursor)
            .await
            .unwrap();
        assert!(end.records.is_empty() && end.diagnostics.is_empty() && end.exhausted);
        let repeat = repo
            .list_due_mcp_subscription_recoveries_global(2, None)
            .await
            .unwrap();
        assert!(repeat.records.is_empty() && repeat.exhausted);
        assert_eq!(repeat.diagnostics.len(), 1);
        assert_eq!(snapshot(pool, &case).await, bad_snapshot.unwrap());
        let payload = original_payload.unwrap();
        sqlx::query("UPDATE insight_platform.invocations SET payload_schema_version=$3,payload=$4,payload_digest=$5,created_at=$6 WHERE tenant_id=$1 AND invocation_id=$2")
            .bind(case.fixture.tenant_id.to_string()).bind(case.fixture.subscription_id.to_string())
            .bind(payload.schema_version).bind(&payload.value).bind(&payload.digest).bind(original_created_at.unwrap()).execute(pool).await.unwrap();
        sqlx::query(
            "UPDATE insight_platform.jobs SET payload_digest=$3 WHERE tenant_id=$1 AND job_id=$2",
        )
        .bind(case.fixture.tenant_id.to_string())
        .bind(case.context_job_id.to_string())
        .bind(&case.context_payload_digest)
        .execute(pool)
        .await
        .unwrap();
        let cleanup = repo
            .drive_expired_context_subscription_refresh_jobs(refresh_recovery(None, 0x8090))
            .await
            .unwrap();
        assert_eq!(cleanup.records.len(), 1);
        assert_eq!(cleanup.records[0].job_id, case.context_job_id.to_string());
        assert_eq!(cleanup.records[0].state, "retry_scheduled");
        assert!(cleanup.diagnostics.is_empty());
        let quota: (i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM insight_platform.quota_ledger WHERE tenant_id=$1 AND correlation_id=$2 AND entry_kind='settle'),reserved_value FROM insight_platform.quota_accounts WHERE tenant_id=$1 AND quota_account_id=$3")
            .bind(case.fixture.tenant_id.to_string()).bind(case.context_reservation_id.to_string()).bind(id(ResourceKind::QuotaAccount,0x5f0).to_string()).fetch_one(pool).await.unwrap();
        assert_eq!(quota, (1, 0));
        let repeat = repo
            .drive_expired_context_subscription_refresh_jobs(refresh_recovery(None, 0x80a0))
            .await
            .unwrap();
        assert!(repeat.records.is_empty() && repeat.diagnostics.is_empty() && repeat.exhausted);
    }
    recovered
}
