//! Real Scheduler object authorization under the actual Artifact DataReader grants.
use super::*;
use futures::FutureExt;
use std::panic::AssertUnwindSafe;

pub(super) async fn verify(
    pool: &PgPool,
    running_for_recovery: &insight_platform_jobs::store::JobRecord,
    artifact_read_deadline: DateTime<Utc>,
) {
    let roles = artifact_roles::ArtifactRoles::create(pool).await;
    let reader = PgRepository::new(roles.pools[1].clone());
    let result = AssertUnwindSafe(async {
        assert_read_boundary(reader.pool()).await;
        verify_inner(pool, &reader, running_for_recovery, artifact_read_deadline).await;
    })
    .catch_unwind()
    .await;
    roles.close().await;
    result.unwrap();
}

async fn assert_read_boundary(pool: &PgPool) {
    for table in ["runs", "resource_versions", "deployments", "resources"] {
        for sql in [
            format!("SELECT * FROM insight_platform.{table} LIMIT 0"),
            format!("SELECT tenant_id FROM insight_platform.{table} WHERE false FOR SHARE"),
            format!("UPDATE insight_platform.{table} SET tenant_id=tenant_id WHERE false"),
            format!("INSERT INTO insight_platform.{table} (tenant_id) SELECT tenant_id FROM insight_platform.jobs WHERE false"),
            format!("DELETE FROM insight_platform.{table} WHERE false"),
        ] {
            let error=sqlx::query(sqlx::AssertSqlSafe(sql.clone())).execute(pool).await.unwrap_err();
            assert_eq!(error.as_database_error().unwrap().code().as_deref(),Some("42501"),"{sql}");
        }
    }
    for sql in [
        "SELECT current_payload FROM insight_platform.runs LIMIT 0",
        "SELECT payload FROM insight_platform.resources LIMIT 0",
        "SELECT created_by FROM insight_platform.resource_versions LIMIT 0",
        "SELECT environment FROM insight_platform.deployments LIMIT 0",
    ] {
        let error = sqlx::query(sql).execute(pool).await.unwrap_err();
        assert_eq!(
            error.as_database_error().unwrap().code().as_deref(),
            Some("42501"),
            "{sql}"
        );
    }
}

async fn verify_inner(
    pool: &PgPool,
    reader: &PgRepository,
    running_for_recovery: &insight_platform_jobs::store::JobRecord,
    artifact_read_deadline: DateTime<Utc>,
) {
    let typed_plan_bytes = canonical_json(&serde_json::to_value(runtime_plan()).unwrap()).unwrap();
    let expected_typed_plan_artifact = ArtifactRef::new(
        id(TYPED_PLAN_ARTIFACT_ID),
        runtime_plan().canonical_digest(plan_limits()).unwrap(),
        u64::try_from(typed_plan_bytes.len()).unwrap(),
        "application/json",
        DataClassification::Internal,
        Some("typed-plan.json".to_owned()),
    )
    .unwrap();
    let typed_plan_read = reader
        .resolve_typed_plan_read(SchedulerTypedPlanLease {
            tenant_id: id(TENANT_ID),
            run_id: id(running_for_recovery.run_id.as_deref().unwrap()),
            orchestration_job_id: id(&running_for_recovery.job_id),
            worker_process_generation_id: id(WORKER_D_ID),
            lease_generation: u64::try_from(running_for_recovery.lease_epoch).unwrap(),
            lease_token_digest: digest('0'),
            request_digest: digest('3'),
            maximum_bytes: typed_plan_bytes.len(),
            deadline: artifact_read_deadline,
        })
        .await
        .unwrap();
    assert_eq!(typed_plan_read.plan_revision_id, id(AGENT_PLAN_ID));
    assert_eq!(typed_plan_read.artifact, expected_typed_plan_artifact);
    let authorized = reader
        .authorize_object_read(&typed_plan_read)
        .await
        .unwrap();
    assert_eq!(authorized.blob_id, id(TYPED_PLAN_BLOB_ID));
    let mut wrong_fence = typed_plan_read.clone();
    wrong_fence.lease_token_digest = digest('4');
    assert!(matches!(
        reader.authorize_object_read(&wrong_fence).await,
        Err(ArtifactObjectReadAuthorityError::Denied)
    ));
    let mut foreign = typed_plan_read.clone();
    foreign.tenant_id = id("ten_0198f1c3-9a00-7c3e-b1f3-773c2836aeff");
    assert!(matches!(
        reader.authorize_object_read(&foreign).await,
        Err(ArtifactObjectReadAuthorityError::Denied)
    ));
    let mut wrong_digest = typed_plan_read.clone();
    wrong_digest.artifact = ArtifactRef::new(
        typed_plan_read.artifact.artifact_id().clone(),
        digest('f'),
        typed_plan_read.artifact.byte_length(),
        typed_plan_read.artifact.media_type().to_owned(),
        typed_plan_read.artifact.classification(),
        typed_plan_read.artifact.display_name().map(str::to_owned),
    )
    .unwrap();
    assert!(matches!(
        reader.authorize_object_read(&wrong_digest).await,
        Err(ArtifactObjectReadAuthorityError::InvalidEvidence)
    ));
    let skill_package_lease = SchedulerSkillPackageLease {
        tenant_id: id(TENANT_ID),
        run_id: id(running_for_recovery.run_id.as_deref().unwrap()),
        orchestration_job_id: id(&running_for_recovery.job_id),
        worker_process_generation_id: id(WORKER_D_ID),
        lease_generation: u64::try_from(running_for_recovery.lease_epoch).unwrap(),
        lease_token_digest: digest('0'),
        skill_slot_id: "review_skill".to_owned(),
        skill_deployment_id: id(SKILL_DEPLOYMENT_ID),
        request_digest: digest('8'),
        maximum_bytes: MAX_SCHEDULER_SKILL_PACKAGE_BYTES,
        deadline: artifact_read_deadline,
    };
    let skill_package_read = reader
        .resolve_skill_package_read(skill_package_lease.clone())
        .await
        .unwrap();
    assert_eq!(skill_package_read.skill_revision_id, id(SKILL_REVISION_ID));
    assert_eq!(
        skill_package_read.artifact.artifact_id(),
        &id(SKILL_PACKAGE_ARTIFACT_ID)
    );
    assert_eq!(
        reader
            .authorize_object_read(&skill_package_read)
            .await
            .unwrap()
            .blob_id,
        id(SKILL_PACKAGE_BLOB_ID)
    );
    assert_eq!(sqlx::query("UPDATE insight_platform.resources SET gate_state='disabled' WHERE tenant_id=$1 AND resource_id=$2").bind(TENANT_ID).bind(SELECTION_POLICY_ID).execute(pool).await.unwrap().rows_affected(), 1);
    let disabled_selection = reader.authorize_object_read(&skill_package_read).await;
    assert_eq!(sqlx::query("UPDATE insight_platform.resources SET gate_state='enabled' WHERE tenant_id=$1 AND resource_id=$2").bind(TENANT_ID).bind(SELECTION_POLICY_ID).execute(pool).await.unwrap().rows_affected(), 1);
    assert!(matches!(
        disabled_selection,
        Err(ArtifactObjectReadAuthorityError::NotFound)
    ));
    assert!(reader
        .authorize_object_read(&skill_package_read)
        .await
        .is_ok());
    let mut wrong_skill_slot = skill_package_lease.clone();
    wrong_skill_slot.skill_slot_id = "missing_skill".to_owned();
    assert!(matches!(
        reader.resolve_skill_package_read(wrong_skill_slot).await,
        Err(ArtifactObjectReadAuthorityError::Denied)
    ));
    let mut unbound_skill = skill_package_lease;
    unbound_skill.skill_deployment_id = id("skdep_0198f1c3-9a00-7c3e-b1f3-773c28367204");
    assert!(matches!(
        reader.resolve_skill_package_read(unbound_skill).await,
        Err(ArtifactObjectReadAuthorityError::Denied)
    ));
    let artifact_value_body = json!({"question": "artifact terminal"});
    let artifact_value_bytes = canonical_json(&artifact_value_body).unwrap();
    let artifact_value_digest: Sha256Digest = canonical_digest(&artifact_value_body)
        .unwrap()
        .parse()
        .unwrap();
    let artifact_value_metadata = fixture_artifact_metadata("terminal.json");
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_blobs (
            tenant_id, blob_id, backend, storage_binding_digest,
            security_domain_digest, object_reference_ciphertext, object_generation, key_id,
            encryption_domain_id, content_digest, size_bytes, state, verified_at,
            created_at, updated_at
        ) VALUES ($1, $2, 'fixture', $3, $4, $5, 'run-value-generation-1',
                  'fixture-key', $6, $7, $8, 'verified', statement_timestamp(),
                  statement_timestamp(), statement_timestamp())
        "#,
    )
    .bind(TENANT_ID)
    .bind("blb_0198f1c3-9a00-7c3e-b1f3-773c2836ae00")
    .bind(digest('5').to_string())
    .bind(digest('6').to_string())
    .bind(vec![10_u8, 11, 12])
    .bind("enc_0198f1c3-9a00-7c3e-b1f3-773c2836ae03")
    .bind(artifact_value_digest.to_string())
    .bind(i64::try_from(artifact_value_bytes.len()).unwrap())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifacts (
            tenant_id, artifact_id, blob_id, purpose, classification,
            expected_size_bytes, expected_digest, declared_media_type,
            verified_media_type, state, metadata_schema_version, metadata,
            metadata_digest, retention_policy_revision_id, retain_until, created_by
        ) VALUES ($1, $2, $3, 'run_output', 'internal', $4, $5,
                  'application/json', 'application/json', 'ready', $6, $7, $8,
                  $9, $10, $11)
        "#,
    )
    .bind(TENANT_ID)
    .bind("art_0198f1c3-9a00-7c3e-b1f3-773c2836ae01")
    .bind("blb_0198f1c3-9a00-7c3e-b1f3-773c2836ae00")
    .bind(i64::try_from(artifact_value_bytes.len()).unwrap())
    .bind(artifact_value_digest.to_string())
    .bind(artifact_value_metadata.schema_version)
    .bind(&artifact_value_metadata.value)
    .bind(&artifact_value_metadata.digest)
    .bind(POLICY_REVISION_ID)
    .bind(Utc::now() + Duration::days(30))
    .bind(PRINCIPAL_ID)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_values (
            tenant_id, value_id, run_id, node_id, value_kind, classification,
            schema_digest, content_digest, inline_value, artifact_id
        ) VALUES ($1, $2, $3, NULL, 'terminal_fixture', 'internal', $4, $5, NULL, $6)
        "#,
    )
    .bind(TENANT_ID)
    .bind("val_0198f1c3-9a00-7c3e-b1f3-773c2836ae02")
    .bind(running_for_recovery.run_id.as_deref().unwrap())
    .bind(agent_schema().canonical_digest.to_string())
    .bind(artifact_value_digest.to_string())
    .bind("art_0198f1c3-9a00-7c3e-b1f3-773c2836ae01")
    .execute(pool)
    .await
    .unwrap();
    let run_value_lease = SchedulerRunValueLease {
        tenant_id: id(TENANT_ID),
        run_id: id(running_for_recovery.run_id.as_deref().unwrap()),
        orchestration_job_id: id(&running_for_recovery.job_id),
        worker_process_generation_id: id(WORKER_D_ID),
        lease_generation: u64::try_from(running_for_recovery.lease_epoch).unwrap(),
        lease_token_digest: digest('0'),
        run_value_id: id("val_0198f1c3-9a00-7c3e-b1f3-773c2836ae02"),
        request_digest: digest('7'),
        maximum_bytes: artifact_value_bytes.len(),
        deadline: artifact_read_deadline,
    };
    let run_value_read = reader
        .resolve_run_value_read(run_value_lease.clone())
        .await
        .unwrap();
    assert_eq!(
        run_value_read.schema_digest,
        agent_schema().canonical_digest
    );
    assert_eq!(run_value_read.classification, DataClassification::Internal);
    assert_eq!(
        run_value_read.artifact.artifact_id(),
        &id("art_0198f1c3-9a00-7c3e-b1f3-773c2836ae01")
    );
    let authorized_value = reader.authorize_object_read(&run_value_read).await.unwrap();
    assert_eq!(
        authorized_value.blob_id,
        id("blb_0198f1c3-9a00-7c3e-b1f3-773c2836ae00")
    );
    let mut wrong_value = run_value_lease;
    wrong_value.run_value_id = id("val_0198f1c3-9a00-7c3e-b1f3-773c2836ae04");
    assert!(matches!(
        reader.resolve_run_value_read(wrong_value).await,
        Err(ArtifactObjectReadAuthorityError::Denied)
    ));
    let expired_running_job = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET lease_expires_at = clock_timestamp() - interval '1 millisecond'
        WHERE tenant_id = $1 AND job_id = $2 AND state = 'running'
        "#,
    )
    .bind(TENANT_ID)
    .bind(&running_for_recovery.job_id)
    .execute(pool)
    .await
    .unwrap();
    assert_eq!(expired_running_job.rows_affected(), 1);
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT $1 > clock_timestamp()")
            .bind(artifact_read_deadline)
            .fetch_one(pool)
            .await
            .unwrap()
    );
    assert!(matches!(
        reader.authorize_object_read(&typed_plan_read).await,
        Err(ArtifactObjectReadAuthorityError::Denied)
    ));
    assert!(matches!(
        reader.authorize_object_read(&run_value_read).await,
        Err(ArtifactObjectReadAuthorityError::Denied)
    ));
}
