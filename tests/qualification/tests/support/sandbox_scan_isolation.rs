//! Global scan behavior uses fully admitted Sandbox jobs and their real owner/quota closures.
use super::*;
use insight_platform_jobs::store::SafetyScanCursor;

async fn before(fixture: &Fixture) -> SafetyScanCursor {
    SafetyScanCursor {
        sort_at: sqlx::query_scalar::<_, DateTime<Utc>>(
            "SELECT created_at FROM insight_platform.jobs WHERE tenant_id=$1 AND job_id=$2",
        )
        .bind(fixture.request.tenant_id.to_string())
        .bind(fixture.request.job_id.to_string())
        .fetch_one(&fixture.pool)
        .await
        .unwrap()
            - Duration::microseconds(1),
        tenant_id: fixture.request.tenant_id.clone(),
        item_id: fixture.request.job_id.clone(),
    }
}
async fn at(fixture: &Fixture) -> SafetyScanCursor {
    let mut cursor = before(fixture).await;
    cursor.sort_at += Duration::microseconds(1);
    cursor
}
async fn payload(fixture: &Fixture) -> Value {
    sqlx::query_scalar("SELECT payload FROM insight_platform.jobs WHERE tenant_id=$1 AND job_id=$2")
        .bind(fixture.request.tenant_id.to_string())
        .bind(fixture.request.job_id.to_string())
        .fetch_one(&fixture.pool)
        .await
        .unwrap()
}
async fn replace_payload(fixture: &Fixture, payload: &Value) {
    sqlx::query("UPDATE insight_platform.jobs SET payload=$3,payload_digest=$4 WHERE tenant_id=$1 AND job_id=$2")
        .bind(fixture.request.tenant_id.to_string()).bind(fixture.request.job_id.to_string())
        .bind(payload).bind(canonical_digest(payload).unwrap()).execute(&fixture.pool).await.unwrap();
}
async fn facts(fixture: &Fixture) -> Value {
    // These are task-owned fixture data. Diagnostics never contain or log these bodies.
    sqlx::query_scalar("SELECT jsonb_build_object('job',(SELECT to_jsonb(j) FROM insight_platform.jobs j WHERE tenant_id=$1 AND job_id=$2),'invocation',(SELECT to_jsonb(i) FROM insight_platform.invocations i WHERE tenant_id=$1 AND invocation_id=$3),'accounts',(SELECT jsonb_agg(to_jsonb(a) ORDER BY quota_account_id) FROM insight_platform.quota_accounts a WHERE tenant_id=$1),'ledger',(SELECT count(*) FROM insight_platform.quota_ledger WHERE tenant_id=$1),'receipts',(SELECT count(*) FROM insight_platform.receipts WHERE tenant_id=$1),'events',(SELECT count(*) FROM insight_platform.events WHERE tenant_id=$1),'outbox',(SELECT count(*) FROM insight_platform.outbox_events WHERE tenant_id=$1))")
        .bind(fixture.request.tenant_id.to_string()).bind(fixture.request.job_id.to_string()).bind(fixture.invocation.invocation_id.to_string()).fetch_one(&fixture.pool).await.unwrap()
}
fn cleanup_request(
    after: Option<SafetyScanCursor>,
    process: &ResourceId,
    limit: u16,
) -> SandboxCleanupClaimV1 {
    SandboxCleanupClaimV1 {
        after,
        worker_manifest: sandbox_fixture_manifest(),
        process_generation_id: process.clone(),
        limit,
        lease_milliseconds: 30_000,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sandbox_scans_isolate_bad_objects_and_advance_past_ineligible_prefixes() {
    let _fixture_guard = SANDBOX_RECOVERY_FIXTURE_LOCK.lock().await;
    let database = std::env::var("PLATFORM_TEST_DATABASE_URL")
        .expect("Sandbox scan isolation requires its real PostgreSQL fixture");
    let pool = PgPoolOptions::new()
        .max_connections(16)
        .connect(&database)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();

    let (completed, _) =
        prepare_boot_rollover_unknown_outcome(pool.clone(), "scan-completed").await;
    let completion_owner = id(ResourceKind::WorkerProcessGeneration);
    let completed_claim = claim_expected_cleanup(
        &completed.repository,
        &completion_owner,
        &completed.request.tenant_id,
        &completed.request.job_id,
    )
    .await;
    complete_expected_cleanup(
        &completed.repository,
        &completed.request.tenant_id,
        &completed.request.job_id,
        completed_claim,
    )
    .await;
    let completed_before = facts(&completed).await;
    let (bad_bool, _) = prepare_boot_rollover_unknown_outcome(pool.clone(), "scan-bad-bool").await;
    let (bad_time, _) = prepare_boot_rollover_unknown_outcome(pool.clone(), "scan-bad-time").await;
    let (busy, _) = prepare_boot_rollover_unknown_outcome(pool.clone(), "scan-live-owner").await;
    let busy_owner = id(ResourceKind::WorkerProcessGeneration);
    let busy_claim = claim_expected_cleanup(
        &busy.repository,
        &busy_owner,
        &busy.request.tenant_id,
        &busy.request.job_id,
    )
    .await;
    let busy_before = facts(&busy).await;
    let (good, _) = prepare_boot_rollover_unknown_outcome(pool.clone(), "scan-good-later").await;

    let original_bool = payload(&bad_bool).await;
    let original_time = payload(&bad_time).await;
    let mut malformed = original_bool.clone();
    malformed["cleanup"]["required"] = json!("invalid-boolean");
    replace_payload(&bad_bool, &malformed).await;
    let mut malformed = original_time.clone();
    malformed["cleanup"]["expires_at"] = json!("invalid-timestamp");
    replace_payload(&bad_time, &malformed).await;
    let bad_before = [facts(&bad_bool).await, facts(&bad_time).await];
    let process = id(ResourceKind::WorkerProcessGeneration);
    let first = SandboxJobRepository::claim_cleanup(
        &good.repository,
        cleanup_request(Some(before(&completed).await), &process, 3),
    )
    .await
    .unwrap();
    assert!(first.records.is_empty());
    assert_eq!(
        first
            .diagnostics
            .iter()
            .map(|diagnostic| diagnostic.item_id.clone())
            .collect::<Vec<_>>(),
        vec![
            bad_bool.request.job_id.clone(),
            bad_time.request.job_id.clone()
        ]
    );
    assert!(first
        .diagnostics
        .iter()
        .all(|diagnostic| diagnostic.validate().is_ok()));
    assert_eq!(
        first.next_cursor.as_ref().unwrap().item_id,
        bad_time.request.job_id
    );
    let second = SandboxJobRepository::claim_cleanup(
        &good.repository,
        cleanup_request(first.next_cursor, &process, 2),
    )
    .await
    .unwrap();
    assert!(second.diagnostics.is_empty());
    assert_eq!(second.records.len(), 1);
    let good_claim = second.records.into_iter().next().unwrap();
    assert_eq!(good_claim.job.job_id, good.request.job_id);
    assert_eq!(
        good_claim.job.state,
        JobState::ReconciliationRequired,
        "physical cleanup cannot invent a successful business result"
    );
    assert_eq!(facts(&completed).await, completed_before);
    assert_eq!(facts(&busy).await, busy_before);
    assert_eq!(facts(&bad_bool).await, bad_before[0]);
    assert_eq!(facts(&bad_time).await, bad_before[1]);

    let good_before = facts(&good).await;
    let replay = SandboxJobRepository::claim_cleanup(
        &good.repository,
        cleanup_request(Some(at(&busy).await), &process, 1),
    )
    .await
    .unwrap();
    assert_eq!(replay.records[0].fence, good_claim.fence);
    assert_eq!(
        facts(&good).await,
        good_before,
        "same build and process replay never renews the fence"
    );
    let mut different_build = cleanup_request(Some(at(&busy).await), &process, 1);
    different_build.worker_manifest.worker_build_digest = digest('f');
    let skipped = SandboxJobRepository::claim_cleanup(&good.repository, different_build)
        .await
        .unwrap();
    assert!(skipped.records.is_empty());
    assert!(skipped.diagnostics.is_empty());
    assert_eq!(
        skipped.next_cursor.as_ref().unwrap().item_id,
        good.request.job_id
    );
    assert_eq!(facts(&good).await, good_before);
    let after = SandboxJobRepository::claim_cleanup(
        &good.repository,
        cleanup_request(skipped.next_cursor, &process, 1),
    )
    .await
    .unwrap();
    assert!(after.exhausted && after.records.is_empty() && after.diagnostics.is_empty());

    // Restore only fixture corruption, then finish the original physical obligations by their ports.
    replace_payload(&bad_bool, &original_bool).await;
    replace_payload(&bad_time, &original_time).await;
    for fixture in [&bad_bool, &bad_time] {
        let claim = claim_expected_cleanup(
            &fixture.repository,
            &busy_owner,
            &fixture.request.tenant_id,
            &fixture.request.job_id,
        )
        .await;
        complete_expected_cleanup(
            &fixture.repository,
            &fixture.request.tenant_id,
            &fixture.request.job_id,
            claim,
        )
        .await;
    }
    complete_expected_cleanup(
        &busy.repository,
        &busy.request.tenant_id,
        &busy.request.job_id,
        busy_claim,
    )
    .await;
    complete_expected_cleanup(
        &good.repository,
        &good.request.tenant_id,
        &good.request.job_id,
        good_claim,
    )
    .await;

    // Expire during seed construction so default INSERT clocks deterministically
    // regress the fixture instead of requiring a slow CI runner to expose the race.
    let bad = seed_fixture_with(
        pool.clone(),
        FixtureOptions {
            deadline_after: Duration::microseconds(1),
            ..FixtureOptions::default()
        },
    )
    .await;
    let good = seed_fixture_with(
        pool,
        FixtureOptions {
            deadline_after: Duration::microseconds(1),
            ..FixtureOptions::default()
        },
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let original = payload(&bad).await;
    let mut malformed = original.clone();
    malformed["unexpected_control_field"] = json!(true);
    replace_payload(&bad, &malformed).await;
    let bad_before = facts(&bad).await;
    let first = SandboxJobRepository::reconcile_controls(
        &bad.repository,
        ReconcileSandboxControlsV1 {
            after: Some(before(&bad).await),
            limit: 1,
        },
    )
    .await
    .unwrap();
    assert!(first.records.is_empty());
    assert_eq!(first.diagnostics.len(), 1);
    assert_eq!(first.diagnostics[0].item_id, bad.request.job_id);
    assert_eq!(
        first.next_cursor.as_ref().unwrap().item_id,
        bad.request.job_id
    );
    let second = SandboxJobRepository::reconcile_controls(
        &bad.repository,
        ReconcileSandboxControlsV1 {
            after: first.next_cursor,
            limit: 1,
        },
    )
    .await
    .unwrap();
    assert!(second.diagnostics.is_empty());
    assert_eq!(second.records.len(), 1);
    assert_eq!(second.records[0].job.job_id, good.request.job_id);
    assert_eq!(second.records[0].job.state, JobState::TimedOut);
    assert_sandbox_control_settled(&good, "timed_out").await;
    assert_eq!(
        facts(&bad).await,
        bad_before,
        "invalid control rolls back Job, Invocation, quota and evidence together"
    );
    replace_payload(&bad, &original).await;
    assert_eq!(
        reconcile_expected_control(&bad).await.job.state,
        JobState::TimedOut
    );
    assert_sandbox_control_settled(&bad, "timed_out").await;
}
