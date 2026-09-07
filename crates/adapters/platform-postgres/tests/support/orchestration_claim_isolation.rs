//! Uses actual public admission and the existing current Plan fixture. No damaged state is
//! installed in the shared coordinator fixture or in another test's authority database.
use super::*;
use insight_platform_orchestrator::store::{ClaimOrchestrationJobs, OrchestrationClaimSlot};

fn command() -> ClaimOrchestrationJobs {
    ClaimOrchestrationJobs {
        worker_id: fresh_id(ResourceKind::WorkerProcessGeneration),
        worker_manifest: WorkerManifest {
            manifest_version: insight_platform_contracts::WORKER_MANIFEST_VERSION,
            worker_build_digest: fresh_digest(),
            execution_capabilities:
                insight_platform_plan::execution::program_execution_capabilities(),
            worker_role: "orchestration.isolation-fixture".into(),
            work_class: WorkClass::Orchestration,
            adapter_runtime_digest: fresh_digest(),
            protocol_version: insight_platform_contracts::WORKER_PROTOCOL_VERSION,
            max_concurrency: 8,
            critical_control_reserved_slots: 1,
        },
        limit: 8,
        lease_milliseconds: 30_000,
        slots: (0..8)
            .map(|_| OrchestrationClaimSlot {
                quota_reservation_id: fresh_id(ResourceKind::UsageReservation),
                lease_token_digest: fresh_digest(),
                job_event_id: fresh_id(ResourceKind::Event),
                job_outbox_id: fresh_id(ResourceKind::OutboxEvent),
                run_event_id: fresh_id(ResourceKind::Event),
                run_outbox_id: fresh_id(ResourceKind::OutboxEvent),
                node_event_id: fresh_id(ResourceKind::Event),
                node_outbox_id: fresh_id(ResourceKind::OutboxEvent),
                quota_entry_ids: (0..MAX_ORCHESTRATION_QUOTA_LINES)
                    .map(|_| fresh_id(ResourceKind::QuotaLedgerEntry))
                    .collect(),
            })
            .collect(),
    }
}
async fn snapshot(repo: &PgRepository, run: &ResourceId) -> serde_json::Value {
    sqlx::query_scalar("SELECT jsonb_build_object('run',(SELECT to_jsonb(r) FROM insight_platform.runs r WHERE run_id=$1),'nodes',(SELECT jsonb_agg(to_jsonb(n) ORDER BY node_id) FROM insight_platform.run_nodes n WHERE run_id=$1),'jobs',(SELECT jsonb_agg(to_jsonb(j) ORDER BY job_id) FROM insight_platform.jobs j WHERE run_id=$1))")
        .bind(run.to_string()).fetch_one(repo.pool()).await.unwrap()
}
#[tokio::test]
async fn corrupted_owners_and_partition_do_not_block_valid_orchestration_claims() {
    let url=std::env::var("PLATFORM_TEST_ORCHESTRATION_ISOLATION_DATABASE_URL").expect("PLATFORM_TEST_ORCHESTRATION_ISOLATION_DATABASE_URL is required for the real claim isolation fixture");
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let repo = PgRepository::new(pool.clone());
    let bindings = seed_authorities(&repo).await;
    admit_run(&repo, bindings.clone()).await;
    let mut broken = Vec::new();
    for damage in ["job", "node", "scope", "run"] {
        let mut admission = admission_command(bindings.clone());
        admission.audit = fresh_audit(&id(TENANT_ID), &id(PRINCIPAL_ID));
        admission.run_id = fresh_id(ResourceKind::Run);
        admission.root_scope_id = fresh_id(ResourceKind::ScopeInstance);
        admission.entry_node_execution_id = fresh_id(ResourceKind::NodeExecution);
        admission.orchestration_job_id = fresh_id(ResourceKind::Job);
        admission.input.value_id = fresh_id(ResourceKind::RunValue);
        let mut tx = repo.begin_run_transaction().await.unwrap();
        tx.admit_run(admission.clone()).await.unwrap();
        tx.commit().await.unwrap();
        // Force the bad objects ahead of the valid original Job in the immutable sweep.
        sqlx::query("UPDATE insight_platform.jobs SET created_at=created_at-interval '1 minute' WHERE job_id=$1").bind(admission.orchestration_job_id.to_string()).execute(&pool).await.unwrap();
        match damage {
            "job" => {
                sqlx::query("UPDATE insight_platform.jobs SET payload_digest=$2 WHERE job_id=$1")
                    .bind(admission.orchestration_job_id.to_string())
                    .bind(fresh_digest().to_string())
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            "node" | "scope" => {
                sqlx::query(
                    "UPDATE insight_platform.run_nodes SET payload_digest=$2 WHERE node_id=$1",
                )
                .bind(if damage == "node" {
                    admission.entry_node_execution_id.to_string()
                } else {
                    admission.root_scope_id.to_string()
                })
                .bind(fresh_digest().to_string())
                .execute(&pool)
                .await
                .unwrap();
            }
            "run" => {
                sqlx::query(
                    "UPDATE insight_platform.runs SET current_payload_digest=$2 WHERE run_id=$1",
                )
                .bind(admission.run_id.to_string())
                .bind(fresh_digest().to_string())
                .execute(&pool)
                .await
                .unwrap();
            }
            _ => unreachable!(),
        }
        broken.push((
            admission.run_id.clone(),
            snapshot(&repo, &admission.run_id).await,
        ));
    }
    let before:(i64,i64)=sqlx::query_as("SELECT successful_claims,deficit FROM insight_platform.scheduler_tenant_state WHERE tenant_id=$1 AND work_class='orchestration'").bind(TENANT_ID).fetch_one(&pool).await.unwrap();
    let mut claimed = Vec::new();
    for _ in 0..8 {
        let mut tx = repo.begin_orchestration_claim_transaction().await.unwrap();
        claimed = tx.claim_orchestration_jobs(command()).await.unwrap();
        tx.commit().await.unwrap();
        if !claimed.is_empty() {
            break;
        }
    }
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].job.job_id, JOB_ID);
    for (run, original) in &broken {
        assert_eq!(
            snapshot(&repo, run).await,
            *original,
            "isolated owning object must not mutate"
        );
    }
    let quota: i64 = sqlx::query_scalar(
        "SELECT reserved_value FROM insight_platform.quota_accounts WHERE quota_account_id=$1",
    )
    .bind(QUOTA_ACCOUNT_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(quota, 1);
    let after:i64=sqlx::query_scalar("SELECT successful_claims FROM insight_platform.scheduler_tenant_state WHERE tenant_id=$1 AND work_class='orchestration'").bind(TENANT_ID).fetch_one(&pool).await.unwrap();
    assert_eq!(after, before.0 + 1);
    let events:i64=sqlx::query_scalar("SELECT count(*) FROM insight_platform.events WHERE aggregate_id=ANY($1) AND event_type LIKE '%claimed%'").bind(broken.iter().map(|(id,_)|id.to_string()).collect::<Vec<_>>()).fetch_one(&pool).await.unwrap();
    assert_eq!(events, 0);
    // The authority row is structurally legal SQL, but its nominal upper bound is not a
    // Tenant. Its untouched failure must not pin the preflight at the same physical hint.
    let bad_partition: i16 = sqlx::query_scalar(
        "SELECT scheduler_partition_id FROM insight_platform.tenants WHERE tenant_id=$1",
    )
    .bind(TENANT_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    let (tenant, other_bindings) = loop {
        let pair = seed_capacity_tenant(&repo).await;
        let partition: i16 = sqlx::query_scalar(
            "SELECT scheduler_partition_id FROM insight_platform.tenants WHERE tenant_id=$1",
        )
        .bind(pair.0.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        if partition != bad_partition {
            break pair;
        }
    };
    admit_capacity_run(&repo, &tenant, other_bindings).await;
    sqlx::query("UPDATE insight_platform.scheduler_state SET tenant_upper_bound=$2,cursor_tenant_id=NULL,updated_at='2000-01-01' WHERE work_class='orchestration' AND partition_id=$1").bind(bad_partition).bind(fresh_id(ResourceKind::Run).to_string()).execute(&pool).await.unwrap();
    let corrupt:serde_json::Value=sqlx::query_scalar("SELECT to_jsonb(s) FROM insight_platform.scheduler_state s WHERE work_class='orchestration' AND partition_id=$1").bind(bad_partition).fetch_one(&pool).await.unwrap();
    let mut delivered = false;
    for _ in 0..8 {
        let mut tx = repo.begin_orchestration_claim_transaction().await.unwrap();
        let page = tx.claim_orchestration_jobs(command()).await.unwrap();
        tx.commit().await.unwrap();
        if page
            .iter()
            .any(|item| item.job.tenant_id == tenant.to_string())
        {
            delivered = true;
            break;
        }
    }
    assert!(
        delivered,
        "bad first partition cannot starve a valid later hint"
    );
    let retained:serde_json::Value=sqlx::query_scalar("SELECT to_jsonb(s) FROM insight_platform.scheduler_state s WHERE work_class='orchestration' AND partition_id=$1").bind(bad_partition).fetch_one(&pool).await.unwrap();
    assert_eq!(retained, corrupt);
}
