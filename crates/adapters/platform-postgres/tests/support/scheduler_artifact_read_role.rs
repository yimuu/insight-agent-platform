//! Real Scheduler object authorization under the actual Artifact DataReader grants.
use super::*;
use futures::FutureExt;
use sqlx::Row;
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
    verify_conversation_history_artifact(pool, reader, &run_value_lease).await;
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

/// The same actual Artifact reader role must authorize only the frozen successful prefix.
async fn verify_conversation_history_artifact(
    pool: &PgPool,
    reader: &PgRepository,
    lease: &SchedulerRunValueLease,
) {
    let cid = "cnv_0198f1c3-9a00-7c3e-b1f3-773c2836ae10";
    let other_cid = "cnv_0198f1c3-9a00-7c3e-b1f3-773c2836ae11";
    let previous = "run_0198f1c3-9a00-7c3e-b1f3-773c2836ae12";
    let original=sqlx::query("SELECT permissions,permissions_digest FROM insight_platform.tenant_principals WHERE tenant_id=$1 AND principal_id=$2").bind(TENANT_ID).bind(PRINCIPAL_ID).fetch_one(pool).await.unwrap();
    let old_payload: serde_json::Value = original.get("permissions");
    let old_digest: String = original.get("permissions_digest");
    let mut raw = old_payload.clone();
    raw.as_object_mut().unwrap().remove("schema_version");
    let mut payload: TenantPrincipalPayload = serde_json::from_value(raw).unwrap();
    let mut permissions = payload.permissions.iter().collect::<Vec<_>>();
    if !permissions.contains(&Permission::RuntimeRead) {
        permissions.push(Permission::RuntimeRead)
    }
    payload.permissions = PermissionSet::new(permissions).unwrap();
    let granted = TypedPayload::new(1, &payload).unwrap();
    sqlx::query("UPDATE insight_platform.tenant_principals SET permissions=$3,permissions_digest=$4 WHERE tenant_id=$1 AND principal_id=$2").bind(TENANT_ID).bind(PRINCIPAL_ID).bind(granted.value.clone()).bind(granted.digest.clone()).execute(pool).await.unwrap();
    // Separate immutable historical Run, with only its exact output selected by the new relation.
    sqlx::query("INSERT INTO insight_platform.runs SELECT (jsonb_populate_record(NULL::insight_platform.runs,to_jsonb(r)||jsonb_build_object('run_id',$3::text,'root_run_id',$3::text,'parent_run_id',NULL,'parent_node_id',NULL,'depth',0,'state','succeeded','terminal_at',clock_timestamp(),'output_value_id',$4::text))).* FROM insight_platform.runs r WHERE tenant_id=$1 AND run_id=$2")
        .bind(TENANT_ID).bind(lease.run_id.to_string()).bind(previous).bind(lease.run_value_id.to_string()).execute(pool).await.unwrap();
    for conversation in [cid, other_cid] {
        sqlx::query("INSERT INTO insight_platform.conversations(tenant_id,conversation_id,agent_id,agent_deployment_id,deployment_digest,input_field,input_schema_digest,title,created_by,version,turn_count) SELECT r.tenant_id,$2,d.resource_id,r.agent_deployment_id,d.bindings_digest,'question',$4,'History authorization',$5,3,2 FROM insight_platform.runs r JOIN insight_platform.deployments d ON d.tenant_id=r.tenant_id AND d.deployment_id=r.agent_deployment_id WHERE r.tenant_id=$1 AND r.run_id=$3")
            .bind(TENANT_ID).bind(conversation).bind(lease.run_id.to_string()).bind(agent_schema().canonical_digest.to_string()).bind(PRINCIPAL_ID).execute(pool).await.unwrap();
    }
    sqlx::query("INSERT INTO insight_platform.conversation_turns(tenant_id,conversation_id,ordinal,run_id,history_through,conversation_version) VALUES($1,$2,1,$3,0,2),($1,$2,2,$4,1,3)").bind(TENANT_ID).bind(cid).bind(previous).bind(lease.run_id.to_string()).execute(pool).await.unwrap();
    sqlx::query(
        "UPDATE insight_platform.run_values SET run_id=$3 WHERE tenant_id=$1 AND value_id=$2",
    )
    .bind(TENANT_ID)
    .bind(lease.run_value_id.to_string())
    .bind(previous)
    .execute(pool)
    .await
    .unwrap();
    let read = reader.resolve_run_value_read(lease.clone()).await.unwrap();
    assert!(
        reader.authorize_object_read(&read).await.is_ok(),
        "retained exact successful history is readable through real limited role"
    );
    // Failure, future order, unrelated conversation, and a non-output value each deny BOTH passes.
    for (mutate,restore) in [
        ("UPDATE insight_platform.runs SET state='failed' WHERE tenant_id=$1 AND run_id=$2","UPDATE insight_platform.runs SET state='succeeded' WHERE tenant_id=$1 AND run_id=$2"),
        ("UPDATE insight_platform.runs SET output_value_id=NULL WHERE tenant_id=$1 AND run_id=$2","UPDATE insight_platform.runs SET output_value_id='val_0198f1c3-9a00-7c3e-b1f3-773c2836ae02' WHERE tenant_id=$1 AND run_id=$2"),
    ] {
        sqlx::query(mutate).bind(TENANT_ID).bind(previous).execute(pool).await.unwrap();
        assert!(reader.resolve_run_value_read(lease.clone()).await.is_err());assert!(reader.authorize_object_read(&read).await.is_err());
        sqlx::query(restore).bind(TENANT_ID).bind(previous).execute(pool).await.unwrap();
    }
    sqlx::query("UPDATE insight_platform.conversation_turns SET ordinal=3,history_through=2 WHERE tenant_id=$1 AND run_id=$2").bind(TENANT_ID).bind(previous).execute(pool).await.unwrap();
    assert!(reader.resolve_run_value_read(lease.clone()).await.is_err());
    assert!(reader.authorize_object_read(&read).await.is_err());
    sqlx::query("UPDATE insight_platform.conversation_turns SET ordinal=1,history_through=0,conversation_id=$3 WHERE tenant_id=$1 AND run_id=$2").bind(TENANT_ID).bind(previous).bind(other_cid).execute(pool).await.unwrap();
    assert!(reader.resolve_run_value_read(lease.clone()).await.is_err());
    assert!(reader.authorize_object_read(&read).await.is_err());
    sqlx::query("UPDATE insight_platform.conversation_turns SET conversation_id=$3 WHERE tenant_id=$1 AND run_id=$2").bind(TENANT_ID).bind(previous).bind(cid).execute(pool).await.unwrap();
    // A once-resolved request cannot survive current content permission revocation.
    payload.permissions = PermissionSet::new(
        payload
            .permissions
            .iter()
            .filter(|p| *p != Permission::ArtifactRead)
            .collect(),
    )
    .unwrap();
    let revoked = TypedPayload::new(1, &payload).unwrap();
    sqlx::query("UPDATE insight_platform.tenant_principals SET permissions=$3,permissions_digest=$4 WHERE tenant_id=$1 AND principal_id=$2").bind(TENANT_ID).bind(PRINCIPAL_ID).bind(revoked.value).bind(revoked.digest).execute(pool).await.unwrap();
    assert!(reader.resolve_run_value_read(lease.clone()).await.is_err());
    assert!(reader.authorize_object_read(&read).await.is_err());
    // Restore this shared qualification fixture before the existing fence-expiry assertions.
    sqlx::query("UPDATE insight_platform.tenant_principals SET permissions=$3,permissions_digest=$4 WHERE tenant_id=$1 AND principal_id=$2").bind(TENANT_ID).bind(PRINCIPAL_ID).bind(old_payload).bind(old_digest).execute(pool).await.unwrap();
    sqlx::query(
        "UPDATE insight_platform.run_values SET run_id=$3 WHERE tenant_id=$1 AND value_id=$2",
    )
    .bind(TENANT_ID)
    .bind(lease.run_value_id.to_string())
    .bind(lease.run_id.to_string())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("DELETE FROM insight_platform.conversation_turns WHERE tenant_id=$1 AND conversation_id IN ($2,$3)").bind(TENANT_ID).bind(cid).bind(other_cid).execute(pool).await.unwrap();
    sqlx::query("DELETE FROM insight_platform.conversations WHERE tenant_id=$1 AND conversation_id IN ($2,$3)").bind(TENANT_ID).bind(cid).bind(other_cid).execute(pool).await.unwrap();
    sqlx::query("DELETE FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2")
        .bind(TENANT_ID)
        .bind(previous)
        .execute(pool)
        .await
        .unwrap();
}
