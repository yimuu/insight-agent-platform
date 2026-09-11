//! Real PostgreSQL process roles; physical scan evidence remains this target's explicit fixture.
use super::*;

#[path = "artifact_roles.rs"]
mod artifact_roles;

pub(super) async fn verify(pool: &PgPool, template: &PrepareArtifact) {
    let roles = artifact_roles::ArtifactRoles::create(pool).await;
    let fixture_pools = roles.pools.clone();
    let owner = pool.clone();
    let template = template.clone();
    let result =
        tokio::spawn(async move { exercise(&owner, &fixture_pools, &template).await }).await;
    roles.close().await;
    result.unwrap();
}

fn applied<T>(outcome: CommandOutcome<T>) -> T {
    match outcome {
        CommandOutcome::Applied(value) => value,
        _ => panic!("fresh command must apply"),
    }
}

async fn denied(pool: &PgPool, sql: String) {
    let error = sqlx::query(sqlx::AssertSqlSafe(sql))
        .execute(pool)
        .await
        .unwrap_err();
    assert!(
        matches!(error, sqlx::Error::Database(ref error) if error.code().as_deref() == Some("42501"))
    );
}

async fn exercise(owner: &PgPool, pools: &[PgPool], template: &PrepareArtifact) {
    let gateway = PgRepository::new(pools[0].clone());
    let worker = PgRepository::new(pools[2].clone());
    let tenant = &template.audit.tenant_id;
    let principal = &template.audit.principal_id;
    let mut prepare = command(
        tenant.clone(),
        principal.clone(),
        template.retention_policy_revision_id.clone(),
        template.scan_policy_revision.clone(),
        template.quota_account_id.clone(),
        0xe000,
        127,
        digest('e'),
    );
    // Distinct physical content avoids unrelated dedup aliases in the parent regression.
    prepare.expected_digest = Some(digest('e'));
    let prepared = applied(execute_prepare(&gateway, prepare.clone()).await.unwrap());
    assert_eq!(prepared.artifact.state, ArtifactState::Staging);
    let complete = complete_command(&prepare, 0xe010, 'e', 'e');
    let mut scan =
        schedule_initial_scan_command(&prepare, prepare.scan_policy_revision.clone(), 0xe020);
    scan.deadline = prepare.operation_deadline;
    // This is the actual public outer transaction: complete and schedule commit together.
    let mut transaction = gateway.begin_artifact_transaction().await.unwrap();
    applied(transaction.complete_upload(complete.clone()).await.unwrap());
    let scheduled = applied(
        transaction
            .schedule_initial_scan(scan.clone())
            .await
            .unwrap(),
    );
    transaction.commit().await.unwrap();
    let worker_id = id(ResourceKind::WorkerProcessGeneration, 0xe030);
    let fence =
        claim_and_start_artifact_job(&worker, tenant, &prepare.operation_id, &worker_id).await;
    let commit = commit_scan_command(
        &prepare,
        &scheduled,
        &worker_id,
        &fence,
        0xe040,
        ScanFinding::verified(digest('e')),
        Utc::now(),
    );
    let verified = applied(execute_commit_scan(&worker, commit.clone()).await.unwrap());
    assert_eq!(verified.artifact.state, ArtifactState::Verified);
    assert!(matches!(
        execute_commit_scan(&worker, commit).await.unwrap(),
        CommandOutcome::Replayed(_)
    ));
    let candidates = gateway
        .scan_public_artifact_finalize_candidates(256)
        .await
        .unwrap();
    assert!(candidates
        .iter()
        .any(|candidate| candidate.artifact.artifact_id == prepare.artifact_id));
    let version: i64 = sqlx::query_scalar("SELECT version FROM insight_platform.quota_accounts WHERE tenant_id=$1 AND quota_account_id=$2")
        .bind(tenant.to_string()).bind(prepare.quota_account_id.to_string()).fetch_one(owner).await.unwrap();
    let mut finalize = finalize_command(
        &prepare,
        prepare.quota_account_id.clone(),
        verified.operation.version,
        0xe050,
    );
    finalize.expected_quota_account_version = u64::try_from(version).unwrap();
    finalize.content_digest = digest('e');
    let ready = applied(execute_finalize(&gateway, finalize.clone()).await.unwrap());
    assert_eq!(ready.artifact.state, ArtifactState::Ready);
    let accounting: (i64, i64) = sqlx::query_as("SELECT reserved_value, version FROM insight_platform.quota_accounts WHERE tenant_id=$1 AND quota_account_id=$2")
        .bind(tenant.to_string()).bind(prepare.quota_account_id.to_string()).fetch_one(owner).await.unwrap();
    assert_eq!(accounting, (0, version + 1));
    assert!(matches!(
        execute_finalize(&gateway, finalize.clone()).await.unwrap(),
        CommandOutcome::Replayed(_)
    ));
    let replay = execute_complete(&gateway, complete).await.unwrap();
    let CommandOutcome::Replayed(replay) = replay else {
        panic!("complete must replay")
    };
    assert_eq!(replay.artifact, ready.artifact);
    assert_eq!(replay.operation.state, JobState::Succeeded);
    assert!(matches!(
        execute_schedule_initial_scan(&gateway, scan).await.unwrap(),
        CommandOutcome::Replayed(_)
    ));

    for restricted in pools {
        for table in ["resources", "resource_versions", "deployments"] {
            for sql in [
                format!("UPDATE insight_platform.{table} SET tenant_id=tenant_id WHERE FALSE"),
                format!("DELETE FROM insight_platform.{table} WHERE FALSE"),
                format!("INSERT INTO insight_platform.{table}(tenant_id) SELECT NULL WHERE FALSE"),
            ] {
                denied(restricted, sql).await;
            }
        }
    }
    for restricted in [&pools[1], &pools[3], &pools[4]] {
        denied(
            restricted,
            format!(
                "SELECT insight_platform.artifact_lock_scan_policy('{}','{}')",
                tenant, prepare.scan_policy_revision.revision_id
            ),
        )
        .await;
    }
    for table in ["quota_ledger", "tasks"] {
        denied(
            &pools[0],
            format!("UPDATE insight_platform.{table} SET tenant_id=tenant_id WHERE FALSE"),
        )
        .await;
        denied(
            &pools[0],
            format!("DELETE FROM insight_platform.{table} WHERE FALSE"),
        )
        .await;
    }
    for column in ["state", "payload", "owner_id", "tenant_id"] {
        denied(
            &pools[2],
            format!("UPDATE insight_platform.invocations SET {column}={column} WHERE FALSE"),
        )
        .await;
    }
    denied(
        &pools[2],
        "INSERT INTO insight_platform.invocations(tenant_id) SELECT NULL WHERE FALSE".to_owned(),
    )
    .await;
    denied(
        &pools[2],
        "DELETE FROM insight_platform.invocations WHERE FALSE".to_owned(),
    )
    .await;
    denied(
        &pools[2],
        "UPDATE insight_platform.tenants SET state=state WHERE FALSE".to_owned(),
    )
    .await;
    denied(
        &pools[2],
        "UPDATE insight_platform.quota_ledger SET reserved_amount=reserved_amount WHERE FALSE"
            .to_owned(),
    )
    .await;
    verify_policy_lock(owner, &pools[0], &prepare).await;
    verify_deletion(owner, &gateway, &prepare, &ready, &finalize).await;
}

async fn verify_policy_lock(owner: &PgPool, gateway: &PgPool, prepare: &PrepareArtifact) {
    use insight_platform_contracts::AdministrativeGate;
    use insight_platform_registry::SetResourceGate;
    let tenant = prepare.audit.tenant_id.to_string();
    let revision = prepare.scan_policy_revision.revision_id.to_string();
    let resource: String = sqlx::query_scalar("SELECT resource_id FROM insight_platform.resource_versions WHERE tenant_id=$1 AND resource_version_id=$2")
        .bind(&tenant).bind(&revision).fetch_one(owner).await.unwrap();
    let mut upload = gateway.begin().await.unwrap();
    let present: bool =
        sqlx::query_scalar("SELECT insight_platform.artifact_lock_scan_policy($1,$2)")
            .bind(&tenant)
            .bind(&revision)
            .fetch_one(&mut *upload)
            .await
            .unwrap();
    assert!(present);
    let owner_repository = PgRepository::new(owner.clone());
    let gate_principal = id(ResourceKind::Principal, 0xef20);
    owner_repository
        .create_principal(NewPrincipal {
            principal_id: gate_principal.clone(),
            authentication_authority_digest: digest('e'),
            subject_digest: digest('e'),
            installation_bindings: PrincipalBindingsPayload {
                installation_bindings: vec![],
            },
        })
        .await
        .unwrap();
    owner_repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: prepare.audit.tenant_id.clone(),
            principal_id: gate_principal.clone(),
            principal_kind: PrincipalKind::TenantAdmin,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(vec![Permission::PolicyActivate]).unwrap(),
            },
        })
        .await
        .unwrap();
    let version: i64 = sqlx::query_scalar(
        "SELECT version FROM insight_platform.resources WHERE tenant_id=$1 AND resource_id=$2",
    )
    .bind(&tenant)
    .bind(&resource)
    .fetch_one(owner)
    .await
    .unwrap();
    let suspend = SetResourceGate {
        audit: audit(&prepare.audit.tenant_id, &gate_principal, 0xef30, 'e', 'e'),
        resource_id: resource.parse().unwrap(),
        expected_resource_version: version,
        target: AdministrativeGate::Suspended,
    };
    // The real Registry command must remain pending until the upload releases its row locks.
    let mut writer = Box::pin(async {
        let mut transaction = owner_repository.begin_registry_transaction().await.unwrap();
        let result = applied(transaction.set_resource_gate(suspend).await.unwrap());
        transaction.commit().await.unwrap();
        result
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut writer)
            .await
            .is_err()
    );
    upload.rollback().await.unwrap();
    let suspended = tokio::time::timeout(std::time::Duration::from_secs(5), writer)
        .await
        .unwrap();
    // Conversely, a gate that committed first must reject scheduling, rolling complete back too.
    let blocked_prepare = command(
        prepare.audit.tenant_id.clone(),
        prepare.audit.principal_id.clone(),
        prepare.retention_policy_revision_id.clone(),
        prepare.scan_policy_revision.clone(),
        prepare.quota_account_id.clone(),
        0xef50,
        3,
        digest('d'),
    );
    let repository = PgRepository::new(gateway.clone());
    let mut transaction = repository.begin_artifact_transaction().await.unwrap();
    applied(
        transaction
            .prepare_artifact(blocked_prepare.clone())
            .await
            .unwrap(),
    );
    applied(
        transaction
            .complete_upload(complete_command(&blocked_prepare, 0xef60, 'c', 'd'))
            .await
            .unwrap(),
    );
    let mut scan = schedule_initial_scan_command(
        &blocked_prepare,
        prepare.scan_policy_revision.clone(),
        0xef70,
    );
    scan.deadline = blocked_prepare.operation_deadline;
    assert!(matches!(
        transaction.schedule_initial_scan(scan).await,
        Err(RepositoryError::NotFound(_))
    ));
    transaction.rollback().await.unwrap();
    assert_eq!(
        artifact_count(
            owner,
            &prepare.audit.tenant_id,
            &blocked_prepare.artifact_id
        )
        .await,
        0
    );
    let mut restore = owner_repository.begin_registry_transaction().await.unwrap();
    applied(
        restore
            .set_resource_gate(SetResourceGate {
                audit: audit(&prepare.audit.tenant_id, &gate_principal, 0xef40, 'f', 'f'),
                resource_id: resource.parse().unwrap(),
                expected_resource_version: suspended.version,
                target: AdministrativeGate::Enabled,
            })
            .await
            .unwrap(),
    );
    restore.commit().await.unwrap();
    let absent: bool =
        sqlx::query_scalar("SELECT insight_platform.artifact_lock_scan_policy($1,$2)")
            .bind(id(ResourceKind::Tenant, 0xef00).to_string())
            .bind(&revision)
            .fetch_one(gateway)
            .await
            .unwrap();
    assert!(!absent);
    for (tenant, revision) in [
        (Some("bad".to_owned()), Some(revision.clone())),
        (None, Some(revision.clone())),
        (Some(tenant.clone()), Some(prepare.artifact_id.to_string())),
        (Some(tenant), None),
    ] {
        let error = sqlx::query("SELECT insight_platform.artifact_lock_scan_policy($1,$2)")
            .bind(tenant)
            .bind(revision)
            .execute(gateway)
            .await
            .unwrap_err();
        assert!(
            matches!(error, sqlx::Error::Database(ref e) if e.code().as_deref()==Some("22023"))
        );
    }
}

async fn verify_deletion(
    owner: &PgPool,
    gateway: &PgRepository,
    prepare: &PrepareArtifact,
    ready: &FinalizedArtifact,
    finalize: &FinalizeArtifact,
) {
    let tenant = &prepare.audit.tenant_id;
    let principal = &prepare.audit.principal_id;
    execute_release_reference(
        gateway,
        ReleaseArtifactReference {
            audit: audit(tenant, principal, 0xe060, 'e', 'e'),
            artifact_reference_id: finalize.artifact_reference_id.clone(),
            artifact_id: prepare.artifact_id.clone(),
            expected_reference_version: 1,
            reason_class: "retention_elapsed".to_owned(),
        },
    )
    .await
    .unwrap();
    // Fixture time setup only; deletion/approval and replay themselves use owning commands.
    sqlx::query("UPDATE insight_platform.artifacts SET created_at=clock_timestamp()-interval '3 days', retain_until=clock_timestamp()-interval '2 days' WHERE tenant_id=$1 AND artifact_id=$2")
        .bind(tenant.to_string()).bind(prepare.artifact_id.to_string()).execute(owner).await.unwrap();
    let mark = MarkArtifactDeletion {
        audit: audit(tenant, principal, 0xe070, 'e', 'e'),
        deletion_operation_id: id(ResourceKind::Job, 0xe073),
        deletion_job_id: id(ResourceKind::Job, 0xe073),
        artifact_id: prepare.artifact_id.clone(),
        blob_id: ready.blob.blob_id.clone(),
        expected_artifact_version: ready.artifact.version,
        expected_blob_version: ready.blob.version,
        approval_task_id: Some(id(ResourceKind::ApprovalTask, 0xe074)),
        retry_backoff_milliseconds: 100,
        deadline: Utc::now() + Duration::hours(1),
    };
    let mut already_approved = mark.clone();
    already_approved.approval_task_id = Some(id(ResourceKind::ApprovalTask, 0xe075));
    seed_approved_deletion_task(
        owner,
        &already_approved,
        &prepare.retention_policy_revision_id,
    )
    .await;
    let mut wrong_binding = already_approved.clone();
    wrong_binding.audit.request_digest = digest('0');
    assert!(matches!(
        execute_mark_deletion(gateway, wrong_binding).await,
        Err(RepositoryError::InvalidInput(_))
    ));
    let mut transaction = gateway.begin_artifact_transaction().await.unwrap();
    let authorized = applied(transaction.mark_deletion(already_approved).await.unwrap());
    assert_eq!(authorized.artifact.state, ArtifactState::Deleting);
    transaction.rollback().await.unwrap();
    let pending = applied(execute_mark_deletion(gateway, mark.clone()).await.unwrap());
    assert_eq!(pending.deletion.operation_state, JobState::Waiting);
    let approve = ResolveArtifactDeletionApproval {
        audit: audit(tenant, principal, 0xe080, 'e', 'e'),
        artifact_id: prepare.artifact_id.clone(),
        operation_id: mark.deletion_operation_id.clone(),
        approval_task_id: mark.approval_task_id.clone().unwrap(),
        expected_artifact_version: ready.artifact.version,
        expected_task_generation: 1,
        expected_task_version: 1,
        decision: ArtifactDeletionApprovalDecision::Approve,
    };
    let owner_repository = PgRepository::new(owner.clone());
    owner_repository
        .resolve_artifact_deletion_approval(approve.clone())
        .await
        .unwrap();
    assert!(matches!(
        execute_mark_deletion(gateway, mark).await.unwrap(),
        CommandOutcome::Replayed(_)
    ));
    let mut resolve_again = approve;
    resolve_again.audit = audit(tenant, principal, 0xe090, 'f', 'f');
    assert!(owner_repository
        .resolve_artifact_deletion_approval(resolve_again)
        .await
        .is_err());
}
