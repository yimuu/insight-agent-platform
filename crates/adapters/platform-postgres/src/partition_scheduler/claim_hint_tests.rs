//! Actual PostgreSQL tests for the physical hint/authority transaction boundary.
use super::*;
use crate::repository::{NewTenant, PgRepository};
use insight_platform_contracts::{ResourceKind, Sha256Digest, TenantConfig, WorkerManifest};
use insight_platform_orchestrator::store::{
    ClaimOrchestrationJobs, OrchestrationClaimSlot, MAX_ORCHESTRATION_QUOTA_LINES,
};
use insight_platform_scheduler::partitioned::{
    select_admissible_partition_batch, LockedTenantVisit, PartitionSchedulerLimits,
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
