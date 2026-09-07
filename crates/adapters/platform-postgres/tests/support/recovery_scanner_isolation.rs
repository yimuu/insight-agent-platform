//! Recovery-only fixtures derived from an actually admitted owning payload in the parent test.
//! Fresh Job and preallocation IDs are never used to claim Artifact/admission qualification.
use chrono::{DateTime, Duration, Utc};
use insight_platform_context::ContextDatasetBuildJobPayload;
use insight_platform_contracts::{
    canonical_digest, ResourceId, ResourceKind, Sha256Digest, TypedPayload,
};
use insight_platform_jobs::store::{JobRecord, SafetyScanCursor, SafetyScanPage};
use insight_platform_postgres::repository::{NewJob, PgRepository};
use insight_platform_registry::RegistryValidationJobPayload;
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

fn fresh(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
}
fn digest(value: &str) -> Sha256Digest {
    canonical_digest(&json!({"recovery_fixture": value}))
        .unwrap()
        .parse()
        .unwrap()
}

struct Lane {
    source: Option<Sha256Digest>,
}
impl Lane {
    async fn scan(
        &self,
        repo: &PgRepository,
        after: Option<SafetyScanCursor>,
        limit: u16,
        backoff: i64,
    ) -> SafetyScanPage<ResourceId> {
        match &self.source {
            Some(source) => repo
                .recover_expired_context_dataset_build_jobs_for_sources(
                    after,
                    limit,
                    backoff,
                    std::slice::from_ref(source),
                )
                .await
                .unwrap(),
            None => repo
                .recover_expired_registry_validation_jobs(after, limit, backoff)
                .await
                .unwrap(),
        }
    }
}

async fn copy_job(
    repo: &PgRepository,
    pool: &PgPool,
    template: &JobRecord,
    base: DateTime<Utc>,
    order: i64,
) -> ResourceId {
    let job_id = fresh(ResourceKind::Job);
    let (owner, payload, requirement) = if template.work_class == "registry_validation" {
        let mut payload: RegistryValidationJobPayload =
            serde_json::from_value(template.payload.value.clone()).unwrap();
        payload.job_id = job_id.clone();
        payload.validate_for_owner(&job_id).unwrap();
        (
            job_id.clone(),
            TypedPayload::from_versioned(2, &payload, 262_144).unwrap(),
            payload.execution_requirement,
        )
    } else {
        let mut payload: ContextDatasetBuildJobPayload =
            serde_json::from_value(template.payload.value.clone()).unwrap();
        payload.job_id = job_id.clone();
        payload.dataset_id = fresh(ResourceKind::ContextDataset);
        payload.artifact_preallocations.generation_id = fresh(ResourceKind::DatasetGeneration);
        for (allocation, stage) in [
            (
                &mut payload.artifact_preallocations.index_manifest,
                &mut payload.artifact_stages.index_manifest,
            ),
            (
                &mut payload.artifact_preallocations.validation_evidence,
                &mut payload.artifact_stages.validation_evidence,
            ),
        ] {
            allocation.artifact_id = fresh(ResourceKind::Artifact);
            allocation.blob_id = fresh(ResourceKind::InternalBlob);
            allocation.quota_entry_id = fresh(ResourceKind::QuotaLedgerEntry);
            allocation.verification_job_id = fresh(ResourceKind::Job);
            stage.producer_job_id = job_id.clone();
            stage.artifact_id = allocation.artifact_id.clone();
            stage.blob_id = allocation.blob_id.clone();
            stage.quota_entry_id = allocation.quota_entry_id.clone();
        }
        payload.validate_for_owner(&payload.dataset_id).unwrap();
        let requirement = payload.execution_requirement().unwrap();
        (
            payload.dataset_id.clone(),
            TypedPayload::from_versioned(1, &payload, 262_144).unwrap(),
            requirement,
        )
    };
    repo.create_job(NewJob {
        tenant_id: template.tenant_id.clone(),
        job_id: job_id.to_string(),
        job_kind: template.job_kind.clone(),
        work_class: template.work_class.clone(),
        owner_kind: owner.kind().descriptor().name.into(),
        owner_id: owner.to_string(),
        trace_id: template.trace.trace_id,
        invocation_id: None,
        run_id: None,
        node_id: None,
        attempt_limit: 3,
        scheduled_at: base,
        deadline: template.deadline,
        priority: template.priority,
        request_digest: payload.digest.clone(),
        effect_key_digest: None,
        payload,
        execution_requirement: requirement,
    })
    .await
    .unwrap();
    // Age this newly created fixture as one abandoned running attempt. No constraint is removed.
    let lease_expiry = base - Duration::seconds(40) + Duration::seconds(order);
    sqlx::query("UPDATE insight_platform.jobs SET state='running',attempt_no=1,lease_epoch=1,worker_id=$3,lease_token_digest=$4,created_at=$5,scheduled_at=$6,started_at=$7,heartbeat_at=$8,lease_expires_at=$9,updated_at=$10 WHERE tenant_id=$1 AND job_id=$2")
        .bind(&template.tenant_id).bind(job_id.to_string()).bind(fresh(ResourceKind::WorkerProcessGeneration).to_string()).bind(digest("lease").to_string())
        .bind(base-Duration::minutes(10)).bind(base-Duration::minutes(9)).bind(base-Duration::minutes(8)).bind(lease_expiry-Duration::seconds(1)).bind(lease_expiry).bind(base).execute(pool).await.unwrap();
    job_id
}
async fn snapshot(pool: &PgPool, tenant: &str, id: &ResourceId) -> Value {
    sqlx::query_scalar(
        "SELECT to_jsonb(job) FROM insight_platform.jobs job WHERE tenant_id=$1 AND job_id=$2",
    )
    .bind(tenant)
    .bind(id.to_string())
    .fetch_one(pool)
    .await
    .unwrap()
}
async fn evidence_counts(pool: &PgPool, tenant: &str, ids: &[ResourceId]) -> (i64, i64, i64) {
    let ids: Vec<_> = ids.iter().map(ToString::to_string).collect();
    sqlx::query_as("SELECT (SELECT count(*) FROM insight_platform.receipts WHERE tenant_id=$1 AND (scope_id=ANY($2) OR response_reference_id=ANY($2))), (SELECT count(*) FROM insight_platform.events WHERE tenant_id=$1 AND aggregate_id=ANY($2)), (SELECT count(*) FROM insight_platform.outbox_events ob JOIN insight_platform.events e ON e.event_id=ob.event_id WHERE e.tenant_id=$1 AND e.aggregate_id=ANY($2))")
        .bind(tenant).bind(ids).fetch_one(pool).await.unwrap()
}

pub(super) async fn verify(repo: &PgRepository, pool: &PgPool, template: &JobRecord) {
    let lane = Lane {
        source: if template.work_class == "context" {
            Some(
                serde_json::from_value::<ContextDatasetBuildJobPayload>(
                    template.payload.value.clone(),
                )
                .unwrap()
                .source_binding
                .canonical_digest,
            )
        } else {
            None
        },
    };
    let base: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(pool)
        .await
        .unwrap();
    let mut ids = Vec::new();
    for order in 0..4 {
        ids.push(copy_job(repo, pool, template, base, order).await);
    }
    let mut bad = snapshot(pool, &template.tenant_id, &ids[0]).await;
    bad["payload"]["unexpected_recovery_field"] = json!(true);
    let payload_digest = canonical_digest(&bad["payload"]).unwrap();
    sqlx::query("UPDATE insight_platform.jobs SET payload=$3,payload_digest=$4 WHERE tenant_id=$1 AND job_id=$2")
        .bind(&template.tenant_id).bind(ids[0].to_string()).bind(&bad["payload"]).bind(payload_digest).execute(pool).await.unwrap();
    // Valid SQL shape, invalid owning wake-to-payload identity; projection validation must isolate it.
    sqlx::query("UPDATE insight_platform.jobs SET wake_kind='timer',wake_state='pending',wake_generation=1 WHERE tenant_id=$1 AND job_id=$2")
        .bind(&template.tenant_id).bind(ids[1].to_string()).execute(pool).await.unwrap();
    let before = [
        snapshot(pool, &template.tenant_id, &ids[0]).await,
        snapshot(pool, &template.tenant_id, &ids[1]).await,
    ];
    let evidence_before = evidence_counts(pool, &template.tenant_id, &ids[..2]).await;
    let first = lane.scan(repo, None, 3, 1000).await;
    assert_eq!(first.records, vec![ids[2].clone()]);
    assert_eq!(first.diagnostics.len(), 2);
    assert_eq!(
        first
            .diagnostics
            .iter()
            .map(|diagnostic| diagnostic.item_id.clone())
            .collect::<Vec<_>>(),
        ids[..2]
    );
    assert!(first
        .diagnostics
        .iter()
        .all(|diagnostic| diagnostic.validate().is_ok()));
    assert_eq!(first.next_cursor.as_ref().unwrap().item_id, ids[2]);
    assert!(!first.exhausted);
    let second = lane.scan(repo, first.next_cursor, 1, 1000).await;
    assert_eq!(second.records, vec![ids[3].clone()]);
    assert!(second.diagnostics.is_empty());
    assert!(lane.scan(repo, second.next_cursor, 1, 1000).await.exhausted);
    for id in &ids[2..] {
        let good = snapshot(pool, &template.tenant_id, id).await;
        assert_eq!(good["state"], "retry_scheduled");
        assert_eq!(good["attempt_no"], 1);
        assert_eq!(good["version"], 2);
        assert!(good["worker_id"].is_null());
    }
    // Restarting a finite sweep still progresses through retained bad rows with a limit of one.
    let only_bad = lane.scan(repo, None, 1, 1000).await;
    assert!(only_bad.records.is_empty());
    assert_eq!(only_bad.diagnostics.len(), 1);
    assert_eq!(only_bad.next_cursor.as_ref().unwrap().item_id, ids[0]);
    let next_bad = lane.scan(repo, only_bad.next_cursor, 1, 1000).await;
    assert_eq!(next_bad.diagnostics[0].item_id, ids[1]);
    assert!(
        lane.scan(repo, next_bad.next_cursor, 1, 1000)
            .await
            .exhausted
    );
    for (id, expected) in ids[..2].iter().zip(before) {
        assert_eq!(snapshot(pool, &template.tenant_id, id).await, expected);
    }
    assert_eq!(
        evidence_counts(pool, &template.tenant_id, &ids[..2]).await,
        evidence_before
    );

    if lane.source.is_none() {
        let tight = copy_job(repo, pool, template, base, 4).await;
        ids.push(tight.clone());
        sqlx::query("UPDATE insight_platform.jobs SET deadline=clock_timestamp()+interval '60 seconds' WHERE tenant_id=$1 AND job_id=$2").bind(&template.tenant_id).bind(tight.to_string()).execute(pool).await.unwrap();
        let expected = snapshot(pool, &template.tenant_id, &tight).await;
        let not_ready = lane
            .scan(
                repo,
                Some(SafetyScanCursor {
                    sort_at: base - Duration::seconds(37),
                    tenant_id: template.tenant_id.parse().unwrap(),
                    item_id: ids[3].clone(),
                }),
                1,
                60_000,
            )
            .await;
        assert!(not_ready.records.is_empty());
        assert!(not_ready.diagnostics.is_empty());
        assert_eq!(not_ready.next_cursor.as_ref().unwrap().item_id, tight);
        assert_eq!(snapshot(pool, &template.tenant_id, &tight).await, expected);
        // Explicit fixture aging models passage of the immutable deadline; the configured
        // backoff remains 60 seconds and production never shortens it to force a retry.
        sqlx::query("UPDATE insight_platform.jobs SET deadline=clock_timestamp()-interval '1 millisecond' WHERE tenant_id=$1 AND job_id=$2").bind(&template.tenant_id).bind(tight.to_string()).execute(pool).await.unwrap();
        let timed_out = lane
            .scan(
                repo,
                Some(SafetyScanCursor {
                    sort_at: base - Duration::seconds(37),
                    tenant_id: template.tenant_id.parse().unwrap(),
                    item_id: ids[3].clone(),
                }),
                1,
                60_000,
            )
            .await;
        assert_eq!(timed_out.records, vec![tight.clone()]);
        assert_eq!(
            snapshot(pool, &template.tenant_id, &tight).await["state"],
            "timed_out"
        );
    }
    // Delete only rows created by this fixture; preserve all admitted owners and source closures.
    let ids: Vec<_> = ids.iter().map(ToString::to_string).collect();
    let mut tx = pool.begin().await.unwrap();
    sqlx::query("DELETE FROM insight_platform.outbox_events WHERE event_id IN (SELECT event_id FROM insight_platform.events WHERE tenant_id=$1 AND aggregate_kind='job' AND aggregate_id=ANY($2))").bind(&template.tenant_id).bind(&ids).execute(&mut *tx).await.unwrap();
    sqlx::query("DELETE FROM insight_platform.events WHERE tenant_id=$1 AND aggregate_kind='job' AND aggregate_id=ANY($2)").bind(&template.tenant_id).bind(&ids).execute(&mut *tx).await.unwrap();
    sqlx::query("DELETE FROM insight_platform.jobs WHERE tenant_id=$1 AND job_id=ANY($2)")
        .bind(&template.tenant_id)
        .bind(&ids)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}
