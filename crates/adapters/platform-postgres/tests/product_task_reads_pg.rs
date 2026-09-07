//! Direct-authority product read evidence. SQL seeds below satisfy the owning Task projection;
//! they do not qualify Run admission, Task creation, response mutation, or successful Run content.

use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{
    canonical_digest, ClosedJsonSchema, Permission, PermissionSet, PrincipalBindingsPayload,
    PrincipalKind, PrincipalSnapshot, ResourceId, ResourceKind, Sha256Digest, TaskEligibilityRule,
    TenantConfig, TenantPrincipalPayload, TraceIdentityV1, TypedPayload,
};
use insight_platform_postgres::{
    repository::{NewPrincipal, NewTenant, NewTenantPrincipal, PgRepository, RepositoryError},
    verify_schema,
};
use insight_platform_registry::LookupRegistryValidationReceipt;
use insight_platform_tasks::{
    store::{TaskInboxQuery, TASK_INBOX_MAX_SCAN},
    TaskDefinition, TaskKind, TaskPayload, TaskProjection, TaskState,
};
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::collections::BTreeSet;
use uuid::Uuid;

fn fresh_id(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
}

fn digest(value: &str) -> Sha256Digest {
    canonical_digest(&json!({"fixture": value}))
        .unwrap()
        .parse()
        .unwrap()
}

async fn add_principal(repository: &PgRepository, tenant: &ResourceId) -> ResourceId {
    let principal = fresh_id(ResourceKind::Principal);
    repository
        .create_principal(NewPrincipal {
            principal_id: principal.clone(),
            authentication_authority_digest: digest(&Uuid::new_v4().to_string()),
            subject_digest: digest(&principal.to_string()),
            installation_bindings: PrincipalBindingsPayload {
                installation_bindings: Vec::new(),
            },
        })
        .await
        .unwrap();
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: tenant.clone(),
            principal_id: principal.clone(),
            principal_kind: PrincipalKind::AgentRunner,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(vec![Permission::InteractionRespond]).unwrap(),
            },
        })
        .await
        .unwrap();
    principal
}

async fn change_membership(
    pool: &PgPool,
    tenant: &ResourceId,
    principal: &ResourceId,
    permissions: Vec<Permission>,
    state: &str,
) {
    let payload = TypedPayload::with_limit(
        1,
        &TenantPrincipalPayload {
            permissions: PermissionSet::new(permissions).unwrap(),
        },
        65_536,
    )
    .unwrap();
    let changed = sqlx::query(
        r#"UPDATE insight_platform.tenant_principals
           SET state = $4, permissions_schema_version = $5, permissions = $6,
               permissions_digest = $7, version = version + 1, updated_at = clock_timestamp()
           WHERE tenant_id = $1 AND principal_id = $2 AND principal_kind = $3"#,
    )
    .bind(tenant.to_string())
    .bind(principal.to_string())
    .bind(PrincipalKind::AgentRunner.as_str())
    .bind(state)
    .bind(payload.schema_version)
    .bind(payload.value)
    .bind(payload.digest)
    .execute(pool)
    .await
    .unwrap();
    assert_eq!(changed.rows_affected(), 1);
}

fn query(tenant: &ResourceId, principal: &ResourceId) -> TaskInboxQuery {
    TaskInboxQuery {
        purpose: insight_platform_tasks::TaskQueryPurpose::Respondable,
        tenant_id: tenant.clone(),
        principal_id: principal.clone(),
        principal_kind: PrincipalKind::AgentRunner,
        state: Some(TaskState::Pending),
        kind: Some(TaskKind::HumanWork),
        run_id: None,
        snapshot_at: None,
        boundary: None,
        page_size: 3,
    }
}

async fn seed_task(
    pool: &PgPool,
    tenant: &ResourceId,
    task_id: &ResourceId,
    creator: &PrincipalSnapshot,
    rule: TaskEligibilityRule,
    schema: &ClosedJsonSchema,
    created_at: DateTime<Utc>,
) {
    let payload = TaskPayload {
        response_schema: Some(schema.clone()),
        eligibility_rule: Some(rule.clone()),
        definition: TaskDefinition::HumanWork {
            eligible_principal_rule_digest: rule.canonical_digest().unwrap(),
            safe_prompt_key: "product_read_fixture".to_owned(),
        },
        created_by: creator.clone(),
        resolution: None,
    };
    let projection = TaskProjection {
        payload_schema_version: 2,
        tenant_id: tenant.clone(),
        task_id: task_id.clone(),
        kind: TaskKind::HumanWork,
        state: TaskState::Pending,
        generation: 1,
        version: 1,
        response_schema_digest: Some(schema.canonical_digest.clone()),
        payload,
        response_value_id: None,
        deadline: created_at + Duration::hours(1),
        resolved_at: None,
    };
    projection.validate().unwrap();
    let payload = TypedPayload::with_limit(2, &projection.payload, 262_144).unwrap();
    sqlx::query(
        r#"INSERT INTO insight_platform.tasks (
            tenant_id, task_id, task_kind, owner_kind, owner_id, state, generation, version,
            response_schema_digest, principal_snapshot_schema_version,
            payload_schema_version, payload, payload_digest, deadline, trace_id,
            created_at, updated_at
        ) VALUES ($1, $2, 'human_work', 'node_execution', $3, 'pending', 1, 1,
                  $4, 1, $5, $6, $7, $8, $9, $10, $10)"#,
    )
    .bind(tenant.to_string())
    .bind(task_id.to_string())
    .bind(fresh_id(ResourceKind::NodeExecution).to_string())
    .bind(schema.canonical_digest.to_string())
    .bind(payload.schema_version)
    .bind(payload.value)
    .bind(payload.digest)
    .bind(projection.deadline)
    .bind(TraceIdentityV1::generate().trace_id.to_string())
    .bind(created_at)
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn bounded_task_scan_preserves_filtered_continuation_and_current_read_authority() {
    let database_url = std::env::var("PLATFORM_TEST_DATABASE_URL")
        .expect("explicit PostgreSQL integration test requires PLATFORM_TEST_DATABASE_URL");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let repository = PgRepository::new(pool.clone());
    let tenant = fresh_id(ResourceKind::Tenant);
    repository
        .create_tenant(NewTenant {
            tenant_id: tenant.to_string(),
            state: "active".to_owned(),
            config: TenantConfig::default(),
        })
        .await
        .unwrap();
    let reader = add_principal(&repository, &tenant).await;
    let other = add_principal(&repository, &tenant).await;
    let creator = PrincipalSnapshot::build(
        tenant.clone(),
        reader.clone(),
        PrincipalKind::AgentRunner,
        PermissionSet::new(vec![Permission::InteractionRespond]).unwrap(),
        1,
        1,
        1,
    )
    .unwrap();
    let schema = ClosedJsonSchema::build(json!({
        "$schema":"https://json-schema.org/draft/2020-12/schema",
        "type":"object","properties":{"answer":{"type":"boolean"}},
        "required":["answer"],"additionalProperties":false
    }))
    .unwrap();
    let created_at: DateTime<Utc> =
        sqlx::query_scalar("SELECT clock_timestamp() - interval '5 minutes'")
            .fetch_one(&pool)
            .await
            .unwrap();
    // Equal timestamps also exercise the ID tie-break. Separate unique tenants make this rerunnable
    // without deleting any existing fixtures or depending on another test's identifiers.
    let mut ids: Vec<_> = (0..205)
        .map(|_| fresh_id(ResourceKind::Interaction))
        .collect();
    ids.sort_by_key(|id| std::cmp::Reverse(id.to_string()));
    for (index, task_id) in ids.iter().enumerate() {
        let rule = if index < 200 {
            TaskEligibilityRule::ExactPrincipal {
                principal_id: other.clone(),
            }
        } else if index % 2 == 0 {
            TaskEligibilityRule::Creator
        } else {
            TaskEligibilityRule::ExactPrincipal {
                principal_id: reader.clone(),
            }
        };
        seed_task(&pool, &tenant, task_id, &creator, rule, &schema, created_at).await;
    }
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM insight_platform.tasks WHERE tenant_id = $1")
            .bind(tenant.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 205);

    let first = repository
        .list_task_inbox(query(&tenant, &reader))
        .await
        .unwrap();
    assert!(first.records.is_empty());
    let mut future_snapshot = query(&tenant, &reader);
    future_snapshot.snapshot_at = Some(first.snapshot_at + Duration::hours(1));
    assert!(matches!(
        repository.list_task_inbox(future_snapshot).await,
        Err(RepositoryError::InvalidInput(_))
    ));
    assert_eq!(TASK_INBOX_MAX_SCAN, 200);
    assert_eq!(
        first.next_scanned_boundary,
        Some((created_at, ids[199].clone()))
    );
    let mut continuation = query(&tenant, &reader);
    continuation.snapshot_at = Some(first.snapshot_at);
    continuation.boundary = first.next_scanned_boundary;
    let second = repository
        .list_task_inbox(continuation.clone())
        .await
        .unwrap();
    assert_eq!(second.snapshot_at, first.snapshot_at);
    assert_eq!(
        second
            .records
            .iter()
            .map(|record| record.task.task_id.clone())
            .collect::<Vec<_>>(),
        ids[200..203]
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        second.next_scanned_boundary,
        Some((created_at, ids[202].clone()))
    );
    continuation.boundary = second.next_scanned_boundary;
    let third = repository.list_task_inbox(continuation).await.unwrap();
    assert_eq!(third.snapshot_at, first.snapshot_at);
    assert!(third.next_scanned_boundary.is_none());
    let visible: Vec<_> = second
        .records
        .into_iter()
        .chain(third.records)
        .map(|record| record.task.task_id)
        .collect();
    assert_eq!(
        visible,
        ids[200..]
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    );
    assert_eq!(visible.iter().collect::<BTreeSet<_>>().len(), 5);

    for id in &ids[200..] {
        let safe = repository
            .read_task_for_principal(
                &tenant,
                &reader,
                PrincipalKind::AgentRunner,
                id,
                insight_platform_tasks::TaskQueryPurpose::Respondable,
            )
            .await
            .unwrap();
        assert_eq!(safe.task.task_id, id.to_string());
        assert!(matches!(
            repository
                .read_task_form_for_principal(&tenant, &reader, PrincipalKind::AgentRunner, id)
                .await,
            Err(RepositoryError::PermissionDenied)
        ));
    }
    assert!(matches!(
        repository
            .read_task_for_principal(
                &tenant,
                &reader,
                PrincipalKind::AgentRunner,
                &ids[0],
                insight_platform_tasks::TaskQueryPurpose::Respondable
            )
            .await,
        Err(RepositoryError::PermissionDenied)
    ));

    // RuntimeRead without ArtifactRead is insufficient even before looking up the addressed Run.
    // The nominal IDs intentionally have no Run/RunValue rows: this proves only the permission gate.
    change_membership(
        &pool,
        &tenant,
        &reader,
        vec![Permission::InteractionRespond, Permission::RuntimeRead],
        "active",
    )
    .await;
    let absent_run = fresh_id(ResourceKind::Run);
    let absent_value = fresh_id(ResourceKind::RunValue);
    assert!(matches!(
        repository
            .read_run_result_for_principal(
                &tenant,
                &reader,
                PrincipalKind::AgentRunner,
                &absent_run
            )
            .await,
        Err(RepositoryError::PermissionDenied)
    ));
    assert!(matches!(
        repository
            .read_run_value_content_for_principal(
                &tenant,
                &reader,
                PrincipalKind::AgentRunner,
                &absent_run,
                &absent_value
            )
            .await,
        Err(RepositoryError::PermissionDenied)
    ));

    change_membership(
        &pool,
        &tenant,
        &reader,
        vec![Permission::InteractionRespond, Permission::ArtifactRead],
        "active",
    )
    .await;
    let authorized = repository
        .read_task_form_for_principal(&tenant, &reader, PrincipalKind::AgentRunner, &ids[200])
        .await
        .unwrap();
    assert_eq!(authorized.task.task_id, ids[200].to_string());
    assert_eq!(
        authorized.task.response_schema_digest,
        Some(schema.canonical_digest.to_string())
    );
    assert_eq!(
        authorized.task.payload.value["response_schema"],
        serde_json::to_value(&schema).unwrap()
    );
    assert!(matches!(
        repository
            .read_task_form_for_principal(&tenant, &reader, PrincipalKind::AgentRunner, &ids[0])
            .await,
        Err(RepositoryError::PermissionDenied)
    ));

    assert_eq!(
        authorized.allowed_actions,
        vec![
            insight_platform_tasks::TaskAction::SubmitInput,
            insight_platform_tasks::TaskAction::Reject,
            insight_platform_tasks::TaskAction::Cancel
        ]
    );
    let observer = add_principal(&repository, &tenant).await;
    change_membership(
        &pool,
        &tenant,
        &observer,
        vec![Permission::InteractionRead, Permission::ArtifactRead],
        "active",
    )
    .await;
    let mut viewable = query(&tenant, &observer);
    viewable.purpose = insight_platform_tasks::TaskQueryPurpose::Viewable;
    let visible_page = repository.list_task_inbox(viewable.clone()).await.unwrap();
    assert_eq!(visible_page.records.len(), 3);
    assert_eq!(visible_page.records[0].task.task_id, ids[0].to_string());
    assert!(visible_page
        .records
        .iter()
        .all(|record| record.allowed_actions.is_empty()));
    let metadata = repository
        .read_task_for_principal(
            &tenant,
            &observer,
            PrincipalKind::AgentRunner,
            &ids[0],
            insight_platform_tasks::TaskQueryPurpose::Viewable,
        )
        .await
        .unwrap();
    assert!(metadata.allowed_actions.is_empty());
    assert!(matches!(
        repository
            .read_task_for_principal(
                &tenant,
                &observer,
                PrincipalKind::AgentRunner,
                &ids[0],
                insight_platform_tasks::TaskQueryPurpose::Respondable
            )
            .await,
        Err(RepositoryError::PermissionDenied)
    ));
    assert!(matches!(
        repository
            .read_task_form_for_principal(&tenant, &observer, PrincipalKind::AgentRunner, &ids[0])
            .await,
        Err(RepositoryError::PermissionDenied)
    ));
    // Reusing a cursor never freezes a previously valid permission snapshot.
    viewable.snapshot_at = Some(visible_page.snapshot_at);
    viewable.boundary = visible_page.next_scanned_boundary;
    change_membership(
        &pool,
        &tenant,
        &observer,
        vec![Permission::ArtifactRead],
        "active",
    )
    .await;
    assert!(matches!(
        repository.list_task_inbox(viewable).await,
        Err(RepositoryError::PermissionDenied)
    ));
    assert!(matches!(
        repository
            .read_task_for_principal(
                &tenant,
                &observer,
                PrincipalKind::AgentRunner,
                &ids[0],
                insight_platform_tasks::TaskQueryPurpose::Viewable
            )
            .await,
        Err(RepositoryError::PermissionDenied)
    ));

    // Frozen creator permissions never replace the current responder permission.
    change_membership(
        &pool,
        &tenant,
        &reader,
        vec![Permission::ArtifactRead],
        "active",
    )
    .await;
    assert!(matches!(
        repository.list_task_inbox(query(&tenant, &reader)).await,
        Err(RepositoryError::PermissionDenied)
    ));
    assert!(matches!(
        repository
            .read_task_for_principal(
                &tenant,
                &reader,
                PrincipalKind::AgentRunner,
                &ids[200],
                insight_platform_tasks::TaskQueryPurpose::Respondable
            )
            .await,
        Err(RepositoryError::PermissionDenied)
    ));
    assert!(matches!(
        repository
            .read_task_form_for_principal(&tenant, &reader, PrincipalKind::AgentRunner, &ids[200])
            .await,
        Err(RepositoryError::PermissionDenied)
    ));

    // Restore the permission payload while revoking membership to isolate membership authority.
    change_membership(
        &pool,
        &tenant,
        &reader,
        vec![Permission::InteractionRespond, Permission::ArtifactRead],
        "revoked",
    )
    .await;
    assert!(matches!(
        repository.list_task_inbox(query(&tenant, &reader)).await,
        Err(RepositoryError::PermissionDenied)
    ));
    assert!(matches!(
        repository
            .read_task_for_principal(
                &tenant,
                &reader,
                PrincipalKind::AgentRunner,
                &ids[200],
                insight_platform_tasks::TaskQueryPurpose::Respondable
            )
            .await,
        Err(RepositoryError::PermissionDenied)
    ));
    assert!(matches!(
        repository
            .read_task_form_for_principal(&tenant, &reader, PrincipalKind::AgentRunner, &ids[200])
            .await,
        Err(RepositoryError::PermissionDenied)
    ));
    pool.close().await;
}

async fn lookup_side_effect_counts(pool: &PgPool, tenant: &ResourceId) -> (i64, i64, i64, i64) {
    sqlx::query_as(
        r#"SELECT
          (SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1),
          (SELECT count(*) FROM insight_platform.jobs WHERE tenant_id = $1),
          (SELECT count(*) FROM insight_platform.outbox_events WHERE tenant_id = $1),
          (SELECT count(*) FROM insight_platform.resources WHERE tenant_id = $1)"#,
    )
    .bind(tenant.to_string())
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn validation_receipt_lookup_uses_current_operation_permission_and_has_no_creation_effects() {
    let database_url = std::env::var("PLATFORM_TEST_DATABASE_URL")
        .expect("explicit PostgreSQL integration test requires PLATFORM_TEST_DATABASE_URL");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let repository = PgRepository::new(pool.clone());
    let tenant = fresh_id(ResourceKind::Tenant);
    repository
        .create_tenant(NewTenant {
            tenant_id: tenant.to_string(),
            state: "active".to_owned(),
            config: TenantConfig::default(),
        })
        .await
        .unwrap();
    let reader = add_principal(&repository, &tenant).await;
    let query = LookupRegistryValidationReceipt {
        tenant_id: tenant.clone(),
        principal_id: reader.clone(),
        principal_kind: PrincipalKind::AgentRunner,
        resource_id: fresh_id(ResourceKind::Agent),
        idempotency_key_digest: digest(&Uuid::new_v4().to_string()),
        request_digest: digest("validation_request"),
        deadline: Utc::now() + Duration::seconds(20),
    };
    let before_miss = lookup_side_effect_counts(&pool, &tenant).await;
    assert_eq!(
        repository
            .lookup_registry_validation_receipt(&query)
            .await
            .unwrap(),
        None
    );
    assert_eq!(lookup_side_effect_counts(&pool, &tenant).await, before_miss);

    // Only a safe committed Receipt is seeded. No Resource source or Job is created: returning its
    // nominal result ID proves lookup scope, not a later Operation read or compilation outcome.
    let job_id = fresh_id(ResourceKind::Job);
    let receipt_id = fresh_id(ResourceKind::Receipt);
    let payload = TypedPayload::empty(1).unwrap();
    sqlx::query(
        r#"INSERT INTO insight_platform.receipts (
          tenant_id, receipt_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
          operation, idempotency_key_digest, request_digest, state, response_reference_id,
          payload_schema_version, payload, payload_digest, created_at, completed_at, expires_at
        ) VALUES ($1,$2,'command','resource',$3,$4,'resource.validate',$5,$6,'succeeded',$7,
                  $8,$9,$10,clock_timestamp()-interval '2 hours',
                  clock_timestamp()-interval '1 hour',clock_timestamp()+interval '1 hour')"#,
    )
    .bind(tenant.to_string())
    .bind(receipt_id.to_string())
    .bind(query.resource_id.to_string())
    .bind(reader.to_string())
    .bind(query.idempotency_key_digest.to_string())
    .bind(query.request_digest.to_string())
    .bind(job_id.to_string())
    .bind(payload.schema_version)
    .bind(payload.value)
    .bind(payload.digest)
    .execute(&pool)
    .await
    .unwrap();
    let before_hit = lookup_side_effect_counts(&pool, &tenant).await;
    assert!(matches!(
        repository.lookup_registry_validation_receipt(&query).await,
        Err(RepositoryError::PermissionDenied)
    ));
    let mut conflicting = query.clone();
    conflicting.request_digest = digest("conflicting_validation_request");
    assert!(matches!(
        repository
            .lookup_registry_validation_receipt(&conflicting)
            .await,
        Err(RepositoryError::PermissionDenied)
    ));

    change_membership(
        &pool,
        &tenant,
        &reader,
        vec![Permission::OperationRead],
        "active",
    )
    .await;
    assert_eq!(
        repository
            .lookup_registry_validation_receipt(&query)
            .await
            .unwrap(),
        Some(job_id)
    );
    assert!(matches!(
        repository
            .lookup_registry_validation_receipt(&conflicting)
            .await,
        Err(RepositoryError::IdempotencyConflict)
    ));
    assert_eq!(lookup_side_effect_counts(&pool, &tenant).await, before_hit);
    let mut missed = query.clone();
    missed.idempotency_key_digest = digest("uncommitted_key");
    assert_eq!(
        repository
            .lookup_registry_validation_receipt(&missed)
            .await
            .unwrap(),
        None
    );
    assert_eq!(lookup_side_effect_counts(&pool, &tenant).await, before_hit);

    sqlx::query("UPDATE insight_platform.receipts SET expires_at = clock_timestamp() - interval '1 minute' WHERE tenant_id=$1 AND receipt_id=$2")
        .bind(tenant.to_string()).bind(receipt_id.to_string()).execute(&pool).await.unwrap();
    assert!(matches!(
        repository.lookup_registry_validation_receipt(&query).await,
        Err(RepositoryError::Conflict("validation Receipt unavailable"))
    ));
    let mut deadline = query.clone();
    deadline.deadline = Utc::now() + Duration::seconds(31);
    assert!(matches!(
        repository
            .lookup_registry_validation_receipt(&deadline)
            .await,
        Err(RepositoryError::InvalidInput(_))
    ));
    deadline.deadline = Utc::now() - Duration::seconds(1);
    assert!(matches!(
        repository
            .lookup_registry_validation_receipt(&deadline)
            .await,
        Err(RepositoryError::InvalidInput(_))
    ));

    change_membership(
        &pool,
        &tenant,
        &reader,
        vec![Permission::OperationRead],
        "revoked",
    )
    .await;
    assert!(matches!(
        repository.lookup_registry_validation_receipt(&query).await,
        Err(RepositoryError::PermissionDenied)
    ));
    assert!(matches!(
        repository.lookup_registry_validation_receipt(&missed).await,
        Err(RepositoryError::PermissionDenied)
    ));
    assert_eq!(lookup_side_effect_counts(&pool, &tenant).await, before_hit);
    pool.close().await;
}
