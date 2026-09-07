//! Existing domain fixtures supply admitted leaves; this helper only injects reversible faults.
pub use super::control_convergence::command;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{ResourceId, TypedPayload};
use insight_platform_jobs::store::SafetyScanCursor;
use insight_platform_orchestrator::store::OrchestrationConvergenceStep;
use insight_platform_postgres::repository::{PgRepository, RepositoryError};
use serde_json::{json, Value};
use sqlx::PgPool;

pub struct Leaf {
    pub tenant: ResourceId,
    pub run: ResourceId,
    pub job: ResourceId,
    pub expected_state: &'static str,
    pub expected_settlements: i64,
}

async fn snapshot(pool: &PgPool, leaf: &Leaf) -> Value {
    sqlx::query_scalar(r#"SELECT jsonb_build_object(
        'run',to_jsonb(run),
        'nodes',(SELECT jsonb_agg(to_jsonb(n) ORDER BY n.node_id) FROM insight_platform.run_nodes n WHERE n.tenant_id=$1 AND n.run_id=$2),
        'owners',(SELECT jsonb_agg(to_jsonb(i) ORDER BY i.invocation_id) FROM insight_platform.invocations i WHERE i.tenant_id=$1 AND i.run_id=$2),
        'jobs',(SELECT jsonb_agg(to_jsonb(j) ORDER BY j.job_id) FROM insight_platform.jobs j WHERE j.tenant_id=$1 AND j.run_id=$2),
        'values',(SELECT jsonb_agg(to_jsonb(v) ORDER BY v.value_id) FROM insight_platform.run_values v WHERE v.tenant_id=$1 AND v.run_id=$2),
        'receipts',(SELECT jsonb_agg(to_jsonb(r) ORDER BY r.receipt_id) FROM insight_platform.receipts r WHERE r.tenant_id=$1 AND r.scope_id IN (
            SELECT $2 UNION SELECT j.job_id FROM insight_platform.jobs j WHERE j.tenant_id=$1 AND j.run_id=$2
            UNION SELECT n.node_id FROM insight_platform.run_nodes n WHERE n.tenant_id=$1 AND n.run_id=$2
            UNION SELECT i.invocation_id FROM insight_platform.invocations i WHERE i.tenant_id=$1 AND i.run_id=$2)),
        'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY e.event_id) FROM insight_platform.events e WHERE e.tenant_id=$1 AND e.run_id=$2),
        'outbox',(SELECT jsonb_agg(to_jsonb(o) ORDER BY o.outbox_id) FROM insight_platform.outbox_events o JOIN insight_platform.events e ON e.tenant_id=o.tenant_id AND e.event_id=o.event_id WHERE e.tenant_id=$1 AND e.run_id=$2),
        'quota',(SELECT jsonb_agg(to_jsonb(q) ORDER BY q.quota_entry_id) FROM insight_platform.quota_ledger q WHERE q.tenant_id=$1 AND q.correlation_id IN (
            SELECT j.quota_reservation_id FROM insight_platform.jobs j WHERE j.tenant_id=$1 AND j.run_id=$2)))
        FROM insight_platform.runs run WHERE run.tenant_id=$1 AND run.run_id=$2"#)
        .bind(leaf.tenant.to_string()).bind(leaf.run.to_string()).fetch_one(pool).await.unwrap()
}

async fn job_facts(pool: &PgPool, leaf: &Leaf) -> (String, i32, i64, i32) {
    sqlx::query_as(r#"SELECT job.state,job.attempt_no,
        (SELECT count(*) FROM insight_platform.quota_ledger q WHERE q.tenant_id=job.tenant_id AND q.correlation_id=job.quota_reservation_id AND q.entry_kind='settle'),
        run.active_work_count FROM insight_platform.jobs job JOIN insight_platform.runs run ON run.tenant_id=job.tenant_id AND run.run_id=job.run_id
        WHERE job.tenant_id=$1 AND job.job_id=$2"#)
        .bind(leaf.tenant.to_string()).bind(leaf.job.to_string()).fetch_one(pool).await.unwrap()
}

async fn quota_snapshot(pool: &PgPool, leaf: &Leaf) -> Value {
    sqlx::query_scalar("SELECT COALESCE(jsonb_agg(to_jsonb(account) ORDER BY account.quota_account_id),'[]'::jsonb) FROM insight_platform.quota_accounts account WHERE account.tenant_id=$1 AND account.quota_account_id IN (SELECT reserve.quota_account_id FROM insight_platform.quota_ledger reserve JOIN insight_platform.jobs job ON job.tenant_id=reserve.tenant_id AND job.quota_reservation_id=reserve.correlation_id WHERE job.tenant_id=$1 AND job.job_id=$2 AND reserve.entry_kind='reserve')")
        .bind(leaf.tenant.to_string()).bind(leaf.job.to_string()).fetch_one(pool).await.unwrap()
}

async fn write_payload(pool: &PgPool, leaf: &Leaf, payload: &TypedPayload) {
    sqlx::query("UPDATE insight_platform.jobs SET payload_schema_version=$3,payload=$4,payload_digest=$5 WHERE tenant_id=$1 AND job_id=$2")
        .bind(leaf.tenant.to_string()).bind(leaf.job.to_string()).bind(payload.schema_version).bind(&payload.value).bind(&payload.digest).execute(pool).await.unwrap();
}

pub async fn verify(pool: &PgPool, repository: &PgRepository, leaves: [Leaf; 2]) {
    let mut ordered = Vec::new();
    for leaf in leaves {
        let deadline: DateTime<Utc> = sqlx::query_scalar(
            "SELECT deadline FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2",
        )
        .bind(leaf.tenant.to_string())
        .bind(leaf.run.to_string())
        .fetch_one(pool)
        .await
        .unwrap();
        ordered.push((
            (deadline, leaf.tenant.to_string(), leaf.run.to_string()),
            leaf,
        ));
    }
    ordered.sort_by(|a, b| a.0.cmp(&b.0));
    let ((deadline, tenant, run), bad) = ordered.remove(0);
    let (_, good) = ordered.remove(0);
    // Resume from an actual preceding persisted Run key, so unrelated fixture Runs are untouched.
    let previous: Option<(DateTime<Utc>,String,String)> = sqlx::query_as("SELECT deadline,tenant_id,run_id FROM insight_platform.runs WHERE (deadline,tenant_id,run_id)<($1,$2,$3) ORDER BY deadline DESC,tenant_id DESC,run_id DESC LIMIT 1")
        .bind(deadline).bind(&tenant).bind(&run).fetch_optional(pool).await.unwrap();
    let after = previous.map(|(sort_at, tenant, item)| SafetyScanCursor {
        sort_at,
        tenant_id: tenant.parse().unwrap(),
        item_id: item.parse().unwrap(),
    });
    let page_command = || {
        let mut command = command();
        command.limit = 1;
        command.slots.truncate(1);
        command.after = after.clone();
        command
    };
    // A reused Run can contain earlier lawful owners. Let their owning commands advance
    // before taking the fault snapshot, until the target leaf is the actual first candidate.
    for leaf in [&bad, &good] {
        let previous:Option<(DateTime<Utc>,String,String)>=sqlx::query_as("SELECT prior.deadline,prior.tenant_id,prior.run_id FROM insight_platform.runs prior JOIN insight_platform.runs target ON target.tenant_id=$1 AND target.run_id=$2 WHERE (prior.deadline,prior.tenant_id,prior.run_id)<(target.deadline,target.tenant_id,target.run_id) ORDER BY prior.deadline DESC,prior.tenant_id DESC,prior.run_id DESC LIMIT 1")
            .bind(leaf.tenant.to_string()).bind(leaf.run.to_string()).fetch_optional(pool).await.unwrap();
        let position = previous.map(|(sort_at, tenant, item)| SafetyScanCursor {
            sort_at,
            tenant_id: tenant.parse().unwrap(),
            item_id: item.parse().unwrap(),
        });
        let expected_owner: String = sqlx::query_scalar(
            "SELECT owner_id FROM insight_platform.jobs WHERE tenant_id=$1 AND job_id=$2",
        )
        .bind(leaf.tenant.to_string())
        .bind(leaf.job.to_string())
        .fetch_one(pool)
        .await
        .unwrap();
        let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let first_owner:Option<String>=sqlx::query_scalar("SELECT invocation_id FROM insight_platform.invocations WHERE tenant_id=$1 AND run_id=$2 AND terminal_at IS NULL AND invocation_kind IN ('context','model','capability') AND state NOT IN ('cancelling','reconciliation_required') ORDER BY created_at,invocation_id LIMIT 1")
                .bind(leaf.tenant.to_string()).bind(leaf.run.to_string()).fetch_optional(pool).await.unwrap();
            if first_owner.as_deref() == Some(&expected_owner) {
                break;
            }
            assert!(
                first_owner.is_some() && std::time::Instant::now() < until,
                "target leaf did not become the first owning candidate"
            );
            let mut command = page_command();
            command.after = position.clone();
            let page = repository
                .drive_orchestration_convergence(command)
                .await
                .unwrap();
            assert!(page.diagnostics.is_empty());
            assert_eq!(page.records.len(), 1);
            assert_eq!(page.records[0].run.run_id, leaf.run.to_string());
        }
    }
    let good_owner: String = sqlx::query_scalar(
        "SELECT owner_id FROM insight_platform.jobs WHERE tenant_id=$1 AND job_id=$2",
    )
    .bind(good.tenant.to_string())
    .bind(good.job.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    let original: (i32,Value,String) = sqlx::query_as("SELECT payload_schema_version,payload,payload_digest FROM insight_platform.jobs WHERE tenant_id=$1 AND job_id=$2")
        .bind(bad.tenant.to_string()).bind(bad.job.to_string()).fetch_one(pool).await.unwrap();
    let original = TypedPayload::from_versioned(original.0, &original.1, 1_048_576).unwrap();
    let original_bad = snapshot(pool, &bad).await;
    let good_before = job_facts(pool, &good).await;
    let good_reservation: Option<String> = sqlx::query_scalar(
        "SELECT quota_reservation_id FROM insight_platform.jobs WHERE tenant_id=$1 AND job_id=$2",
    )
    .bind(good.tenant.to_string())
    .bind(good.job.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    let mut bad_schema = original.value.clone();
    bad_schema["schema_version"] = json!(2);
    let invalids = [
        TypedPayload::new(1, &json!({"invalid_domain_job":true})).unwrap(),
        TypedPayload::from_versioned(2, &bad_schema, 1_048_576).unwrap(),
    ];
    for (index, invalid) in invalids.iter().enumerate() {
        write_payload(pool, &bad, invalid).await;
        let retained = snapshot(pool, &bad).await;
        let quota_before = quota_snapshot(pool, &bad).await;
        let page = repository
            .drive_orchestration_convergence(page_command())
            .await
            .unwrap();
        assert!(
            page.records.is_empty() && !page.exhausted,
            "bad Job page: {page:?}"
        );
        assert_eq!(page.diagnostics.len(), 1);
        assert_eq!(page.diagnostics[0].item_id, bad.job);
        let cursor = page.next_cursor.unwrap();
        assert_eq!(cursor.item_id, bad.run);
        assert_eq!(snapshot(pool, &bad).await, retained);
        assert_eq!(quota_snapshot(pool, &bad).await, quota_before);
        if index == 0 {
            let mut next = page_command();
            next.after = Some(cursor);
            let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
            loop {
                assert!(
                    std::time::Instant::now() < until,
                    "healthy Run not reached through the scanned cursor"
                );
                let page = repository
                    .drive_orchestration_convergence(next)
                    .await
                    .unwrap();
                if let Some(record) = page
                    .records
                    .iter()
                    .find(|record| record.run.run_id == good.run.to_string())
                {
                    assert!(
                        matches!(&record.step,OrchestrationConvergenceStep::Domain{owner_id,state} if owner_id==&good_owner && state==good.expected_state)
                    );
                    break;
                }
                assert!(
                    !page.exhausted,
                    "healthy Run absent after the bad Run cursor"
                );
                next = page_command();
                next.after = page.next_cursor;
            }
            let actual = job_facts(pool, &good).await;
            assert_eq!(actual.0, good.expected_state);
            assert_eq!(actual.1, good_before.1);
            assert_eq!(
                actual.3,
                good_before.3 - i32::from(good.expected_settlements > 0)
            );
            let settlements:i64=sqlx::query_scalar("SELECT count(*) FROM insight_platform.quota_ledger WHERE tenant_id=$1 AND correlation_id=$2 AND entry_kind='settle'")
                .bind(good.tenant.to_string()).bind(&good_reservation).fetch_one(pool).await.unwrap();
            assert_eq!(settlements, good.expected_settlements);
        }
        let good_retained = snapshot(pool, &good).await;
        let quota_before = quota_snapshot(pool, &bad).await;
        let again = repository
            .drive_orchestration_convergence(page_command())
            .await
            .unwrap();
        assert_eq!(again.diagnostics.len(), 1);
        assert!(again.records.is_empty());
        assert_eq!(snapshot(pool, &bad).await, retained);
        assert_eq!(snapshot(pool, &good).await, good_retained);
        assert_eq!(quota_snapshot(pool, &bad).await, quota_before);
    }
    write_payload(pool, &bad, &original).await;
    assert_eq!(snapshot(pool, &bad).await, original_bad);
    // Both columns admit structurally legal SQL text outside their owning closed sets.
    let (node_id,node_state,node_kind):(String,String,String)=sqlx::query_as("SELECT node.node_id,node.state,node.node_kind FROM insight_platform.run_nodes node JOIN insight_platform.jobs job ON job.tenant_id=node.tenant_id AND job.node_id=node.node_id WHERE job.tenant_id=$1 AND job.job_id=$2")
        .bind(bad.tenant.to_string()).bind(bad.job.to_string()).fetch_one(pool).await.unwrap();
    for (column, invalid, original_value) in [
        ("state", "invalid_owner_state", node_state),
        ("node_kind", "invalid_owner_kind", node_kind),
    ] {
        let update = match column { "state" => "UPDATE insight_platform.run_nodes SET state=$3 WHERE tenant_id=$1 AND node_id=$2", "node_kind" => "UPDATE insight_platform.run_nodes SET node_kind=$3 WHERE tenant_id=$1 AND node_id=$2", _ => unreachable!("closed test fault column"), };
        sqlx::query(update)
            .bind(bad.tenant.to_string())
            .bind(&node_id)
            .bind(invalid)
            .execute(pool)
            .await
            .unwrap();
        let retained = snapshot(pool, &bad).await;
        let good_retained = snapshot(pool, &good).await;
        for _ in 0..2 {
            let quota_before = quota_snapshot(pool, &bad).await;
            let page = repository
                .drive_orchestration_convergence(page_command())
                .await
                .unwrap();
            assert!(page.records.is_empty() && !page.exhausted);
            assert_eq!(page.diagnostics.len(), 1);
            assert_eq!(page.diagnostics[0].item_id.to_string(), node_id);
            assert_eq!(page.next_cursor.unwrap().item_id, bad.run);
            assert_eq!(snapshot(pool, &bad).await, retained);
            assert_eq!(snapshot(pool, &good).await, good_retained);
            assert_eq!(quota_snapshot(pool, &bad).await, quota_before);
        }
        sqlx::query(update)
            .bind(bad.tenant.to_string())
            .bind(&node_id)
            .bind(original_value)
            .execute(pool)
            .await
            .unwrap();
        assert_eq!(snapshot(pool, &bad).await, original_bad);
    }
    // Shared accounting failures are not damaged-object diagnostics, even on this same path.
    let quota:Option<(String,i64)>=sqlx::query_as("SELECT account.quota_account_id,account.reserved_value FROM insight_platform.quota_accounts account JOIN insight_platform.quota_ledger reserve ON reserve.tenant_id=account.tenant_id AND reserve.quota_account_id=account.quota_account_id JOIN insight_platform.jobs job ON job.tenant_id=reserve.tenant_id AND job.quota_reservation_id=reserve.correlation_id WHERE job.tenant_id=$1 AND job.job_id=$2 AND reserve.entry_kind='reserve' AND account.reserved_value>0 ORDER BY account.quota_account_id LIMIT 1")
        .bind(bad.tenant.to_string()).bind(bad.job.to_string()).fetch_optional(pool).await.unwrap();
    if let Some((account, reserved)) = quota {
        let original_quota = quota_snapshot(pool, &bad).await;
        sqlx::query("UPDATE insight_platform.quota_accounts SET reserved_value=0 WHERE tenant_id=$1 AND quota_account_id=$2")
            .bind(bad.tenant.to_string()).bind(&account).execute(pool).await.unwrap();
        let damaged_quota = quota_snapshot(pool, &bad).await;
        assert!(matches!(
            repository
                .drive_orchestration_convergence(page_command())
                .await,
            Err(RepositoryError::CorruptRow(_))
        ));
        assert_eq!(snapshot(pool, &bad).await, original_bad);
        assert_eq!(quota_snapshot(pool, &bad).await, damaged_quota);
        sqlx::query("UPDATE insight_platform.quota_accounts SET reserved_value=$3 WHERE tenant_id=$1 AND quota_account_id=$2")
            .bind(bad.tenant.to_string()).bind(account).bind(reserved).execute(pool).await.unwrap();
        assert_eq!(quota_snapshot(pool, &bad).await, original_quota);
    }
}
