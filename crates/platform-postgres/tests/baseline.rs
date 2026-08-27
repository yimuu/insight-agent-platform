use chrono::{Duration, Utc};
use insight_platform_contracts::{JobKind, SchedulerPriority, TenantConfig, WorkClass};
use insight_platform_postgres::{
    operational_metrics::{
        observe_durable_job_queue, observe_durable_job_queue_for_kinds, observe_durable_outbox,
    },
    repository::{
        ClaimJobs, CommitJob, HeartbeatJob, JobCommitOutcome, JobFence, JobTerminalState, NewJob,
        NewQuotaAccount, NewTenant, PgRepository, QuotaMutationOutcome, RepositoryError,
        ReserveQuota, SettleQuota, TypedPayload,
    },
    verify_schema, BASELINE_TABLE_COUNT, EXPECTED_TABLES, MIGRATIONS,
};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;

const TENANT_ID: &str = "ten_018f22bb-3c21-7c65-b5f8-67e2452f8a9b";
const WORKER_ID: &str = "wrk_018f22bb-3c21-7c65-b5f8-67e2452f8a9c";
const JOB_ID: &str = "job_018f22bb-3c21-7c65-b5f8-67e2452f8a9d";
const RECEIPT_ID: &str = "rcp_018f22bb-3c21-7c65-b5f8-67e2452f8a9e";
const EVENT_ID: &str = "evt_018f22bb-3c21-7c65-b5f8-67e2452f8a9f";
const OUTBOX_ID: &str = "obx_018f22bb-3c21-7c65-b5f8-67e2452f8aa0";
const QUOTA_ACCOUNT_ID: &str = "qac_018f22bb-3c21-7c65-b5f8-67e2452f8aa1";
const QUOTA_ENTRY_1: &str = "qle_018f22bb-3c21-7c65-b5f8-67e2452f8aa2";
const JOB_OWNER_ID: &str = "int_018f22bb-3c21-7c65-b5f8-67e2452f8aac";
const CORRELATION_1: &str = "inv_018f22bb-3c21-7c65-b5f8-67e2452f8aa3";
const QUOTA_ENTRY_2: &str = "qle_018f22bb-3c21-7c65-b5f8-67e2452f8aa4";
const STALE_RECEIPT_ID: &str = "rcp_018f22bb-3c21-7c65-b5f8-67e2452f8aa5";
const STALE_EVENT_ID: &str = "evt_018f22bb-3c21-7c65-b5f8-67e2452f8aa6";
const STALE_OUTBOX_ID: &str = "obx_018f22bb-3c21-7c65-b5f8-67e2452f8aa7";
const QUOTA_ENTRY_3: &str = "qle_018f22bb-3c21-7c65-b5f8-67e2452f8aa8";
const CORRELATION_2: &str = "inv_018f22bb-3c21-7c65-b5f8-67e2452f8aa9";
const QUOTA_ENTRY_4: &str = "qle_018f22bb-3c21-7c65-b5f8-67e2452f8aaa";
const CORRELATION_3: &str = "inv_018f22bb-3c21-7c65-b5f8-67e2452f8aab";

fn digest(byte: char) -> String {
    format!("sha256:{}", byte.to_string().repeat(64))
}

#[test]
fn migration_is_a_single_twenty_three_table_baseline() {
    assert_eq!(MIGRATIONS.len(), 1);
    assert_eq!(EXPECTED_TABLES.len(), BASELINE_TABLE_COUNT);
    assert_eq!(BASELINE_TABLE_COUNT, 23);
    assert_eq!(
        MIGRATIONS[0]
            .sql
            .matches("CREATE TABLE insight_platform.")
            .count(),
        22
    );
    for rejected in [
        "execution_attempts",
        "continuations",
        "command_receipts",
        "public_run_stream_heads",
        "registry_exact_resources",
        "CREATE TRIGGER",
    ] {
        assert!(!MIGRATIONS[0].sql.contains(rejected), "found {rejected}");
    }
}

#[tokio::test]
async fn real_postgres_baseline_job_receipt_outbox_and_quota() {
    let Ok(database_url) = std::env::var("PLATFORM_TEST_DATABASE_URL") else {
        eprintln!("PLATFORM_TEST_DATABASE_URL is unset; real PostgreSQL fixture skipped");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await
        .unwrap();
    let first = verify_schema(&pool).await.unwrap();
    let replay = verify_schema(&pool).await.unwrap();
    assert_eq!(first, replay);
    assert_eq!(first.table_count, 23);

    let repository = PgRepository::new(pool.clone());
    repository
        .create_tenant(NewTenant {
            tenant_id: TENANT_ID.to_owned(),
            state: "active".to_owned(),
            config: TenantConfig::default(),
        })
        .await
        .unwrap();

    let now: chrono::DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let trace_id = insight_platform_contracts::TraceIdentityV1::generate().trace_id;
    repository
        .create_job(NewJob {
            tenant_id: TENANT_ID.to_owned(),
            job_id: JOB_ID.to_owned(),
            job_kind: "interaction".to_owned(),
            work_class: "interaction".to_owned(),
            owner_kind: "interaction".to_owned(),
            owner_id: JOB_OWNER_ID.to_owned(),
            trace_id,
            invocation_id: None,
            run_id: None,
            node_id: None,
            attempt_limit: 2,
            scheduled_at: now,
            deadline: now + Duration::minutes(2),
            priority: SchedulerPriority::Normal,
            request_digest: digest('a'),
            effect_key_digest: None,
            payload: TypedPayload::new(1, &json!({"request": "bounded"})).unwrap(),
        })
        .await
        .unwrap();

    let queue = observe_durable_job_queue(&pool, WorkClass::Interaction)
        .await
        .unwrap();
    assert_eq!(queue.due_jobs, 1);
    assert!(queue.due_oldest_age_seconds >= 0.0);
    assert_eq!(queue.expired_leases, 0);
    assert_eq!(queue.expired_oldest_lag_seconds, 0.0);
    let filtered_queue =
        observe_durable_job_queue_for_kinds(&pool, WorkClass::Interaction, &[JobKind::Interaction])
            .await
            .unwrap();
    assert_eq!(filtered_queue.due_jobs, queue.due_jobs);
    assert!(filtered_queue.due_oldest_age_seconds >= queue.due_oldest_age_seconds);
    assert_eq!(filtered_queue.expired_leases, queue.expired_leases);
    assert_eq!(
        filtered_queue.expired_oldest_lag_seconds,
        queue.expired_oldest_lag_seconds
    );
    assert_eq!(
        observe_durable_job_queue(&pool, WorkClass::Orchestration)
            .await
            .unwrap(),
        Default::default()
    );

    let claimed = repository
        .claim_jobs(ClaimJobs {
            work_class: "interaction".to_owned(),
            worker_id: WORKER_ID.parse().unwrap(),
            limit: 1,
            lease_milliseconds: 60_000,
            lease_token_digests: vec![digest('f').parse().unwrap()],
        })
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].attempt_no, 0);
    assert_eq!(claimed[0].lease_epoch, 1);
    let start_fence = JobFence {
        tenant_id: TENANT_ID.to_owned(),
        job_id: JOB_ID.to_owned(),
        worker_id: WORKER_ID.parse().unwrap(),
        lease_epoch: 1,
        expected_job_version: claimed[0].version,
        lease_token_digest: digest('f').parse().unwrap(),
    };
    let mut wrong_token = start_fence.clone();
    wrong_token.lease_token_digest = digest('0').parse().unwrap();
    assert!(matches!(
        repository.start_job(wrong_token).await,
        Err(RepositoryError::StaleFence)
    ));
    let started = repository.start_job(start_fence).await.unwrap();
    assert_eq!(started.attempt_no, 1);
    let heartbeat_fence = JobFence {
        tenant_id: TENANT_ID.to_owned(),
        job_id: JOB_ID.to_owned(),
        worker_id: WORKER_ID.parse().unwrap(),
        lease_epoch: 1,
        expected_job_version: started.version,
        lease_token_digest: digest('f').parse().unwrap(),
    };
    let heartbeat = repository
        .heartbeat_job(HeartbeatJob {
            fence: heartbeat_fence,
            lease_milliseconds: 60_000,
        })
        .await
        .unwrap();
    let fence = JobFence {
        tenant_id: TENANT_ID.to_owned(),
        job_id: JOB_ID.to_owned(),
        worker_id: WORKER_ID.parse().unwrap(),
        lease_epoch: 1,
        expected_job_version: heartbeat.version,
        lease_token_digest: digest('f').parse().unwrap(),
    };

    let commit = CommitJob {
        fence: fence.clone(),
        terminal_state: JobTerminalState::Succeeded,
        result_digest: digest('b'),
        result_payload: TypedPayload::new(1, &json!({"result": "ok"})).unwrap(),
        receipt_id: RECEIPT_ID.to_owned(),
        idempotency_key_digest: digest('c'),
        request_digest: digest('d'),
        receipt_payload: TypedPayload::new(1, &json!({"source": "worker"})).unwrap(),
        receipt_expires_at: now + Duration::hours(1),
        event_id: EVENT_ID.to_owned(),
        event_type: "job.succeeded".to_owned(),
        event_payload: TypedPayload::new(1, &json!({"outcome": "succeeded"})).unwrap(),
        outbox_id: OUTBOX_ID.to_owned(),
    };
    let outcome = repository.commit_job(commit.clone()).await.unwrap();
    assert!(matches!(outcome, JobCommitOutcome::Committed(_)));
    let persisted_trace: (String, String) = sqlx::query_as(
        r#"
        SELECT event.trace_id, outbox.trace_id
        FROM insight_platform.events AS event
        JOIN insight_platform.outbox_events AS outbox
          ON outbox.tenant_id = event.tenant_id
         AND outbox.event_id = event.event_id
         AND outbox.trace_id = event.trace_id
        WHERE event.tenant_id = $1 AND event.event_id = $2
        "#,
    )
    .bind(TENANT_ID)
    .bind(EVENT_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(persisted_trace.0, trace_id.to_string());
    assert_eq!(persisted_trace.1, persisted_trace.0);
    let replay = repository.commit_job(commit.clone()).await.unwrap();
    assert!(matches!(replay, JobCommitOutcome::Replayed { .. }));

    let mut conflict = commit;
    conflict.request_digest = digest('e');
    assert!(matches!(
        repository.commit_job(conflict).await,
        Err(RepositoryError::IdempotencyConflict)
    ));
    let stale_commit = CommitJob {
        fence,
        terminal_state: JobTerminalState::Succeeded,
        result_digest: digest('b'),
        result_payload: TypedPayload::new(1, &json!({"result": "late"})).unwrap(),
        receipt_id: STALE_RECEIPT_ID.to_owned(),
        idempotency_key_digest: digest('2'),
        request_digest: digest('3'),
        receipt_payload: TypedPayload::new(1, &json!({"source": "late-worker"})).unwrap(),
        receipt_expires_at: now + Duration::hours(1),
        event_id: STALE_EVENT_ID.to_owned(),
        event_type: "job.succeeded".to_owned(),
        event_payload: TypedPayload::new(1, &json!({"outcome": "late"})).unwrap(),
        outbox_id: STALE_OUTBOX_ID.to_owned(),
    };
    assert!(matches!(
        repository.commit_job(stale_commit).await,
        Err(RepositoryError::StaleFence)
    ));
    let outbox_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.outbox_events WHERE tenant_id = $1",
    )
    .bind(TENANT_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(outbox_count, 1);
    let outbox = observe_durable_outbox(&pool).await.unwrap();
    assert_eq!(outbox.due_events, 1);
    assert!(outbox.due_oldest_age_seconds >= 0.0);
    assert_eq!(outbox.expired_claims, 0);
    assert_eq!(outbox.expired_oldest_lag_seconds, 0.0);
    assert_eq!(outbox.dead_events, 0);

    repository
        .create_quota_account(NewQuotaAccount {
            tenant_id: TENANT_ID.to_owned(),
            quota_account_id: QUOTA_ACCOUNT_ID.to_owned(),
            scope_kind: "tenant".to_owned(),
            scope_id: TENANT_ID.to_owned(),
            work_class: "artifact".to_owned(),
            metric: "concurrency".to_owned(),
            limit_value: 10,
            payload: TypedPayload::new(1, &json!({"window": "current"})).unwrap(),
        })
        .await
        .unwrap();
    let reserve = ReserveQuota {
        tenant_id: TENANT_ID.to_owned(),
        quota_account_id: QUOTA_ACCOUNT_ID.to_owned(),
        quota_entry_id: QUOTA_ENTRY_1.to_owned(),
        correlation_id: CORRELATION_1.to_owned(),
        amount: 7,
        request_digest: digest('f'),
    };
    assert!(matches!(
        repository.reserve_quota(reserve.clone()).await.unwrap(),
        QuotaMutationOutcome::Applied(_)
    ));
    assert!(matches!(
        repository.reserve_quota(reserve).await.unwrap(),
        QuotaMutationOutcome::Replayed(_)
    ));
    let settled = repository
        .settle_quota(SettleQuota {
            tenant_id: TENANT_ID.to_owned(),
            quota_account_id: QUOTA_ACCOUNT_ID.to_owned(),
            quota_entry_id: QUOTA_ENTRY_2.to_owned(),
            correlation_id: CORRELATION_1.to_owned(),
            used_amount: 5,
            request_digest: digest('1'),
        })
        .await
        .unwrap();
    let QuotaMutationOutcome::Applied(account) = settled else {
        panic!("first settlement cannot be a replay")
    };
    assert_eq!(account.reserved_value, 0);
    assert_eq!(account.used_value, 5);

    let reserve_a = ReserveQuota {
        tenant_id: TENANT_ID.to_owned(),
        quota_account_id: QUOTA_ACCOUNT_ID.to_owned(),
        quota_entry_id: QUOTA_ENTRY_3.to_owned(),
        correlation_id: CORRELATION_2.to_owned(),
        amount: 4,
        request_digest: digest('4'),
    };
    let reserve_b = ReserveQuota {
        tenant_id: TENANT_ID.to_owned(),
        quota_account_id: QUOTA_ACCOUNT_ID.to_owned(),
        quota_entry_id: QUOTA_ENTRY_4.to_owned(),
        correlation_id: CORRELATION_3.to_owned(),
        amount: 4,
        request_digest: digest('5'),
    };
    let (outcome_a, outcome_b) = tokio::join!(
        repository.reserve_quota(reserve_a),
        repository.reserve_quota(reserve_b)
    );
    let outcomes = [outcome_a, outcome_b];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Ok(QuotaMutationOutcome::Applied(_))))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Err(RepositoryError::QuotaExceeded)))
            .count(),
        1
    );
}
