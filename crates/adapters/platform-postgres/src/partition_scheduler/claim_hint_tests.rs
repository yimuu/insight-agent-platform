//! Actual PostgreSQL tests for the physical hint/authority transaction boundary.
use super::*;
use crate::repository::{NewTenant, PgRepository};
use insight_platform_contracts::{ResourceKind, Sha256Digest, TenantConfig, WorkerManifest};
use insight_platform_orchestrator::store::{
    ClaimOrchestrationJobs, OrchestrationClaimSlot, MAX_ORCHESTRATION_QUOTA_LINES,
};
use insight_platform_scheduler::partitioned::{
    select_admissible_partition_batch, AdmissionCandidate, LockedTenantVisit,
    PartitionSchedulerLimits,
};
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::{collections::BTreeSet, time::Duration};

fn fresh(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
}

fn claim() -> ClaimOrchestrationJobs {
    let digest: Sha256Digest = insight_platform_contracts::canonical_digest(
        &serde_json::json!({"fixture": "claim-hint-boundary"}),
    )
    .unwrap()
    .parse()
    .unwrap();
    ClaimOrchestrationJobs {
        worker_manifest: WorkerManifest {
            manifest_version: insight_platform_contracts::WORKER_MANIFEST_VERSION,
            worker_build_digest: digest.clone(),
            worker_role: "orchestration.hint-test".into(),
            work_class: WorkClass::Orchestration,
            adapter_runtime_digest: digest.clone(),
            protocol_version: 1,
            max_concurrency: 1,
            critical_control_reserved_slots: 1,
            execution_capabilities:
                insight_platform_plan::execution::program_execution_capabilities(),
        },
        worker_id: fresh(ResourceKind::WorkerProcessGeneration),
        limit: 1,
        lease_milliseconds: 30_000,
        slots: vec![OrchestrationClaimSlot {
            lease_token_digest: digest,
            quota_reservation_id: fresh(ResourceKind::UsageReservation),
            quota_entry_ids: (0..MAX_ORCHESTRATION_QUOTA_LINES)
                .map(|_| fresh(ResourceKind::QuotaLedgerEntry))
                .collect(),
            run_event_id: fresh(ResourceKind::Event),
            run_outbox_id: fresh(ResourceKind::OutboxEvent),
            node_event_id: fresh(ResourceKind::Event),
            node_outbox_id: fresh(ResourceKind::OutboxEvent),
            job_event_id: fresh(ResourceKind::Event),
            job_outbox_id: fresh(ResourceKind::OutboxEvent),
        }],
    }
}

async fn fixture() -> (String, PgPool) {
    let url = std::env::var("PLATFORM_TEST_CONTROLLER_TRANSACTION_DATABASE_URL")
        .expect("claim hint tests require the current-schema controller transaction fixture");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .unwrap();
    crate::verify_schema(&pool).await.unwrap();
    (url, pool)
}

async fn enroll_distinct_partition(pool: &PgPool) -> (ResourceId, SchedulerPartitionId) {
    let used: BTreeSet<i16> = sqlx::query_scalar(
        "SELECT DISTINCT partition_id FROM insight_platform.scheduler_tenant_state WHERE work_class='orchestration'",
    )
    .fetch_all(pool)
    .await
    .unwrap()
    .into_iter()
    .collect();
    let (tenant, partition) = (0..4096)
        .find_map(|_| {
            let tenant = fresh(ResourceKind::Tenant);
            let partition = SchedulerPartitionId::for_tenant(&tenant).unwrap();
            (!used.contains(&i16::from(partition.0))).then_some((tenant, partition))
        })
        .expect("bounded fixture search needs an unused physical partition");
    PgRepository::new(pool.clone())
        .create_tenant(NewTenant {
            tenant_id: tenant.to_string(),
            state: "active".into(),
            config: TenantConfig::default(),
        })
        .await
        .unwrap();
    (tenant, partition)
}

#[tokio::test]
async fn prepared_claim_releases_single_connection_and_rejects_generic_or_empty_hints() {
    let _fixture_lock = super::CONTROLLER_FIXTURE_LOCK.lock().await;
    let (url, admin) = fixture().await;
    let single = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(1))
        .connect(&url)
        .await
        .unwrap();
    let repository = PgRepository::new(single.clone());
    let mut generic = repository.begin_scheduler_transaction().await.unwrap();
    assert!(matches!(
        generic.claim_orchestration_jobs(claim()).await,
        Err(RepositoryError::InvalidInput(reason)) if reason == "orchestration claim requires its dedicated transaction"
    ));
    generic.rollback().await.unwrap();

    // A correct preflight releases its sole connection before acquiring the
    // SERIALIZABLE transaction; retaining it would time out this actual pool.
    tokio::time::timeout(Duration::from_secs(3), async {
        let prepared = repository
            .begin_orchestration_claim_transaction()
            .await
            .unwrap();
        prepared.rollback().await.unwrap();
    })
    .await
    .expect("preflight must release the single pool connection");

    let mut locks = admin.begin().await.unwrap();
    sqlx::query("SELECT partition_id FROM insight_platform.scheduler_state WHERE work_class='orchestration' ORDER BY partition_id FOR UPDATE")
        .fetch_all(&mut *locks).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut prepared = repository
            .begin_orchestration_claim_transaction()
            .await
            .unwrap();
        assert!(prepared
            .claim_orchestration_jobs(claim())
            .await
            .unwrap()
            .is_empty());
        prepared.commit().await.unwrap();
    })
    .await
    .expect("busy preflight is an empty hint set, never a blocking fallback");
    locks.rollback().await.unwrap();
    single.close().await;
}

#[tokio::test]
async fn observed_hint_rechecks_current_enrollment_and_busy_partition() {
    let _fixture_lock = super::CONTROLLER_FIXTURE_LOCK.lock().await;
    let (_, pool) = fixture().await;
    let (tenant, hint) = enroll_distinct_partition(&pool).await;
    assert!(available_partition_hints(&pool, WorkClass::Orchestration)
        .await
        .unwrap()
        .contains(&hint));
    let mut holder = pool.begin().await.unwrap();
    sqlx::query("SELECT partition_id FROM insight_platform.scheduler_state WHERE work_class='orchestration' AND partition_id=$1 FOR UPDATE")
        .bind(i16::from(hint.0)).fetch_one(&mut *holder).await.unwrap();
    let mut contender = pool.begin().await.unwrap();
    sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .execute(&mut *contender)
        .await
        .unwrap();
    assert!(tokio::time::timeout(
        Duration::from_secs(3),
        lock_exact_partition(&mut contender, WorkClass::Orchestration, hint)
    )
    .await
    .unwrap()
    .unwrap()
    .is_none());
    contender.rollback().await.unwrap();

    // Simulate vanished enrollment within this rolled-back authority fixture.
    // The previously observed hint cannot recreate fairness or authorize work.
    sqlx::query("DELETE FROM insight_platform.scheduler_tenant_state WHERE tenant_id=$1 AND work_class='orchestration'")
        .bind(tenant.to_string()).execute(&mut *holder).await.unwrap();
    assert!(
        lock_exact_partition(&mut holder, WorkClass::Orchestration, hint)
            .await
            .unwrap()
            .is_none()
    );
    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM insight_platform.scheduler_tenant_state WHERE tenant_id=$1 AND work_class='orchestration'")
        .bind(tenant.to_string()).fetch_one(&mut *holder).await.unwrap();
    assert_eq!(remaining, 0);
    holder.rollback().await.unwrap();
}

async fn empty_partition_visit(
    tx: &mut Transaction<'_, Postgres>,
    hint: SchedulerPartitionId,
) -> (u64, u64) {
    let partition = lock_exact_partition(tx, WorkClass::Orchestration, hint)
        .await
        .unwrap()
        .unwrap();
    let window = lock_tenant_window(tx, &partition, 128, 4096).await.unwrap();
    let visits = window
        .tenants
        .iter()
        .map(|tenant| LockedTenantVisit {
            state: tenant.state.clone(),
            business_policy: None,
            candidates: Vec::new(),
            next_job_sweep: None,
        })
        .collect::<Vec<_>>();
    let decision = select_admissible_partition_batch(
        &partition.state,
        &visits,
        &Default::default(),
        PartitionSchedulerLimits {
            maximum_deficit: 4096,
            maximum_tenant_window: 128,
            maximum_candidates_per_tenant: 1,
            maximum_claims: 1,
            maximum_control_claims_per_tenant: 1,
            maximum_quota_lines_per_candidate: MAX_ORCHESTRATION_QUOTA_LINES,
        },
        1,
        window.range_exhausted,
    )
    .unwrap();
    assert!(decision.admitted_job_ids.is_empty());
    let rounds = (
        partition.state.current_round,
        decision.next_partition.current_round,
    );
    persist_admission(tx, &partition, &window.tenants, &decision)
        .await
        .unwrap();
    rounds
}

#[tokio::test]
async fn exact_partition_visits_commit_independently_without_cross_partition_credit() {
    let _fixture_lock = super::CONTROLLER_FIXTURE_LOCK.lock().await;
    let (_, pool) = fixture().await;
    let (left_tenant, left) = enroll_distinct_partition(&pool).await;
    let (right_tenant, right) = enroll_distinct_partition(&pool).await;
    assert_ne!(left, right);
    // Enrollment joins the following round. Complete that opening round via
    // the same pure decision before testing concurrent fairness-row writes.
    for hint in [left, right] {
        let mut opening = pool.begin().await.unwrap();
        empty_partition_visit(&mut opening, hint).await;
        opening.commit().await.unwrap();
    }
    let mut first = pool.begin().await.unwrap();
    let mut second = pool.begin().await.unwrap();
    for tx in [&mut first, &mut second] {
        sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .execute(&mut **tx)
            .await
            .unwrap();
    }
    let (left_rounds, right_rounds) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(
            empty_partition_visit(&mut first, left),
            empty_partition_visit(&mut second, right)
        )
    })
    .await
    .expect("independent exact partitions must not acquire each other's row locks");
    // Both transactions have read and written before either commits. The old
    // all-partition predicate would make one an SSI pivot under this schedule.
    first.commit().await.unwrap();
    second.commit().await.unwrap();
    assert_eq!(left_rounds.1, left_rounds.0 + 1);
    assert_eq!(right_rounds.1, right_rounds.0 + 1);
    let states: Vec<(i64,i64)> = sqlx::query_as("SELECT deficit,successful_claims FROM insight_platform.scheduler_tenant_state WHERE tenant_id=ANY($1) AND work_class='orchestration'")
        .bind(vec![left_tenant.to_string(), right_tenant.to_string()]).fetch_all(&pool).await.unwrap();
    assert_eq!(
        states,
        vec![(0, 0), (0, 0)],
        "empty visits never mint business credit or claims"
    );
}

async fn persistence_visit(
    tx: &mut Transaction<'_, Postgres>,
    hint: SchedulerPartitionId,
    next_sweep: Option<JobSweepContinuation>,
    control_claim: bool,
) -> (
    LockedPartition,
    TenantFairnessWindow,
    PartitionAdmissionDecision,
) {
    // Exercise the legal coarse fairness predicate independently of fixture size.
    // Partition lookup and writes remain exact indexed operations. These planner
    // settings belong only to this mechanism test, never to the actual Q1 claim.
    sqlx::query("SET LOCAL enable_seqscan=off")
        .execute(&mut **tx)
        .await
        .unwrap();
    let partition = lock_exact_partition(tx, WorkClass::Orchestration, hint)
        .await
        .unwrap()
        .unwrap();
    for setting in [
        "SET LOCAL enable_seqscan=on",
        "SET LOCAL enable_indexscan=off",
        "SET LOCAL enable_indexonlyscan=off",
        "SET LOCAL enable_bitmapscan=off",
    ] {
        sqlx::query(setting).execute(&mut **tx).await.unwrap();
    }
    let window = lock_tenant_window(tx, &partition, 128, 4096).await.unwrap();
    for setting in [
        "SET LOCAL enable_seqscan=off",
        "SET LOCAL enable_indexscan=on",
        "SET LOCAL enable_indexonlyscan=on",
        "SET LOCAL enable_bitmapscan=on",
    ] {
        sqlx::query(setting).execute(&mut **tx).await.unwrap();
    }
    assert_eq!(window.tenants.len(), 1);
    let candidates = if control_claim {
        vec![AdmissionCandidate {
            job_id: fresh(ResourceKind::Job),
            mode: insight_platform_contracts::ClaimMode::NewAttempt,
            lane: insight_platform_contracts::SchedulingLane::RestrictedControl,
            currently_eligible: true,
            quota_costs: Vec::new(),
        }]
    } else {
        Vec::new()
    };
    // This tests only owning fairness persistence: the selector supplies a real
    // sweep/counter decision, but no domain Job, lease or quota is manufactured.
    let visits = vec![LockedTenantVisit {
        state: window.tenants[0].state.clone(),
        business_policy: None,
        candidates,
        next_job_sweep: next_sweep,
    }];
    let decision = select_admissible_partition_batch(
        &partition.state,
        &visits,
        &Default::default(),
        PartitionSchedulerLimits {
            maximum_deficit: 4096,
            maximum_tenant_window: 128,
            maximum_candidates_per_tenant: 1,
            maximum_claims: 1,
            maximum_control_claims_per_tenant: 1,
            maximum_quota_lines_per_candidate: MAX_ORCHESTRATION_QUOTA_LINES,
        },
        1,
        window.range_exhausted,
    )
    .unwrap();
    (partition, window, decision)
}

async fn fairness_row(pool: &PgPool, tenant: &ResourceId) -> String {
    sqlx::query_scalar("SELECT row_to_json(fairness)::text FROM insight_platform.scheduler_tenant_state AS fairness WHERE tenant_id=$1 AND work_class='orchestration'")
        .bind(tenant.to_string()).fetch_one(pool).await.unwrap()
}

#[tokio::test]
async fn empty_fairness_visit_does_not_abort_a_changed_partition() {
    let _fixture_lock = super::CONTROLLER_FIXTURE_LOCK.lock().await;
    let (url, admin) = fixture().await;
    for plan_mode in [
        "SET LOCAL plan_cache_mode=force_custom_plan",
        "SET LOCAL plan_cache_mode=force_generic_plan",
    ] {
        let (left_tenant, left_hint) = enroll_distinct_partition(&admin).await;
        let (empty_tenant, empty_hint) = enroll_distinct_partition(&admin).await;
        for hint in [left_hint, empty_hint] {
            let mut opening = admin.begin().await.unwrap();
            empty_partition_visit(&mut opening, hint).await;
            opening.commit().await.unwrap();
        }
        // Fresh sessions keep the tested prepared statements' planning settings
        // independent of whichever other fixture previously used the admin pool.
        let pool = PgPoolOptions::new()
            .max_connections(3)
            .connect(&url)
            .await
            .unwrap();
        let mut left = pool.begin().await.unwrap();
        let mut empty = pool.begin().await.unwrap();
        for tx in [&mut left, &mut empty] {
            sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                .execute(&mut **tx)
                .await
                .unwrap();
            sqlx::query(plan_mode).execute(&mut **tx).await.unwrap();
        }
        let cutoff: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *left)
            .await
            .unwrap();
        let sweep = JobSweepContinuation {
            creation_cutoff: cutoff,
            upper_bound: JobCreationKey {
                created_at: cutoff - chrono::Duration::seconds(1),
                job_id: fresh(ResourceKind::Job),
            },
            after: None,
        };
        let (left_partition, left_window, left_decision) =
            persistence_visit(&mut left, left_hint, Some(sweep), false).await;
        let (empty_partition, empty_window, empty_decision) =
            persistence_visit(&mut empty, empty_hint, None, false).await;
        assert_ne!(left_window.tenants[0].state, left_decision.next_tenants[0]);
        assert_eq!(
            empty_window.tenants[0].state,
            empty_decision.next_tenants[0]
        );
        let mut backend_ids = Vec::new();
        for tx in [&mut left, &mut empty] {
            backend_ids.push(
                sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
                    .fetch_one(&mut **tx)
                    .await
                    .unwrap(),
            );
        }
        let predicate_readers: i64 = sqlx::query_scalar("SELECT count(DISTINCT pid) FROM pg_locks WHERE pid=ANY($1) AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND relation='insight_platform.scheduler_tenant_state'::regclass AND locktype='relation' AND mode='SIReadLock'")
            .bind(backend_ids).fetch_one(&pool).await.unwrap();
        assert_eq!(
            predicate_readers, 2,
            "both owning windows must hold the coarse predicate"
        );
        let empty_before = fairness_row(&pool, &empty_tenant).await;
        persist_admission(
            &mut empty,
            &empty_partition,
            &empty_window.tenants,
            &empty_decision,
        )
        .await
        .unwrap();
        empty.commit().await.unwrap();
        // Under the old unconditional fairness UPDATE, the committed empty
        // visit creates the opposite rw edge and this actual write gets 40001.
        persist_admission(
            &mut left,
            &left_partition,
            &left_window.tenants,
            &left_decision,
        )
        .await
        .expect("an unchanged empty visit must not abort a real sweep update");
        left.commit().await.unwrap();
        assert_eq!(fairness_row(&pool, &empty_tenant).await, empty_before,
            "an empty visit must preserve the entire fairness row, including version and updated_at");
        let persisted = tenant_from_row(sqlx::query("SELECT * FROM insight_platform.scheduler_tenant_state WHERE tenant_id=$1 AND work_class='orchestration'")
            .bind(left_tenant.to_string()).fetch_one(&pool).await.unwrap(), WorkClass::Orchestration, 4096).unwrap();
        assert_eq!(persisted.state, left_decision.next_tenants[0]);
        assert_eq!(persisted.version, left_window.tenants[0].version + 1);
        for (locked, decision) in [
            (&left_partition, &left_decision),
            (&empty_partition, &empty_decision),
        ] {
            let persisted: (i64, i64, Option<String>, Option<String>) = sqlx::query_as("SELECT version,current_round,cursor_tenant_id,tenant_upper_bound FROM insight_platform.scheduler_state WHERE work_class='orchestration' AND partition_id=$1")
                .bind(i16::from(locked.state.partition_id.0)).fetch_one(&pool).await.unwrap();
            assert_eq!(
                persisted,
                (
                    locked.version + 1,
                    decision.next_partition.current_round as i64,
                    decision
                        .next_partition
                        .cursor_tenant_id
                        .as_ref()
                        .map(ToString::to_string),
                    decision
                        .next_partition
                        .tenant_upper_bound
                        .as_ref()
                        .map(ToString::to_string)
                )
            );
            assert_eq!(
                decision.next_partition.current_round,
                locked.state.current_round + 1
            );
        }
        pool.close().await;
    }
}

#[tokio::test]
async fn changed_fairness_still_persists_counters_and_rejects_a_stale_version() {
    let _fixture_lock = super::CONTROLLER_FIXTURE_LOCK.lock().await;
    let (_, pool) = fixture().await;
    let (tenant, hint) = enroll_distinct_partition(&pool).await;
    let mut opening = pool.begin().await.unwrap();
    empty_partition_visit(&mut opening, hint).await;
    opening.commit().await.unwrap();
    let mut tx = pool.begin().await.unwrap();
    let (partition, window, decision) = persistence_visit(&mut tx, hint, None, true).await;
    assert_eq!(decision.admitted_job_ids.len(), 1);
    assert_eq!(
        decision.next_tenants[0].successful_claims,
        window.tenants[0].state.successful_claims + 1
    );
    assert_eq!(
        decision.next_tenants[0].last_served_round,
        Some(partition.state.current_round)
    );
    persist_admission(&mut tx, &partition, &window.tenants, &decision)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let persisted = tenant_from_row(sqlx::query("SELECT * FROM insight_platform.scheduler_tenant_state WHERE tenant_id=$1 AND work_class='orchestration'")
        .bind(tenant.to_string()).fetch_one(&pool).await.unwrap(), WorkClass::Orchestration, 4096).unwrap();
    assert_eq!(persisted.state, decision.next_tenants[0]);
    assert_eq!(persisted.version, window.tenants[0].version + 1);
    let before = fairness_row(&pool, &tenant).await;
    let mut stale = pool.begin().await.unwrap();
    assert!(matches!(
        persist_admission(&mut stale, &partition, &window.tenants, &decision).await,
        Err(RepositoryError::Conflict("tenant scheduler state"))
    ));
    stale.rollback().await.unwrap();
    assert_eq!(fairness_row(&pool, &tenant).await, before);
}
