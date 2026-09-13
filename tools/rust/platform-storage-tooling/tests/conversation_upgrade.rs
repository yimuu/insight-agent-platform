//! Run only against a newly created, explicitly designated disposable database.
use insight_platform_deployment_contracts::installation_release::*;
use insight_platform_storage_tooling::conversation_upgrade::{target_inventory_digest, upgrade};

#[tokio::test]
#[ignore = "requires PLATFORM_UPGRADE_TEST_DATABASE_URL naming an empty insight_upgrade_test_* database"]
async fn upgrade_preserves_owner_and_recovers_exact_commit() {
    let url =
        std::env::var("PLATFORM_UPGRADE_TEST_DATABASE_URL").expect("explicit disposable database");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    let name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(name.starts_with("insight_upgrade_test_"));
    // These are cluster roles, created only for this explicitly designated disposable fixture.
    sqlx::raw_sql("DO $roles$ DECLARE role_name text; BEGIN FOREACH role_name IN ARRAY ARRAY['insight_runtime_dev','insight_artifact_data_reader_dev','insight_artifact_gateway_dev'] LOOP IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=role_name) THEN EXECUTE format('CREATE ROLE %I NOLOGIN',role_name); END IF; END LOOP; END $roles$;")
        .execute(&pool).await.unwrap();
    sqlx::raw_sql(include_str!("fixtures/conversation-source16.sql"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::raw_sql("INSERT INTO insight_platform.principals (principal_id,state,authentication_authority_digest,subject_digest,payload_schema_version,payload,payload_digest) VALUES ('prn_00000000-0000-7000-8000-000000000001','active','sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',1,'{}','sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc');
        INSERT INTO insight_platform.local_console_owner (principal_id,email,display_name,password_salt,password_hash) VALUES ('prn_00000000-0000-7000-8000-000000000001','owner@example.test','Keep this owner',decode(repeat('ab',32),'hex'),decode(repeat('cd',64),'hex'));")
        .execute(&pool).await.unwrap();
    let before: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(o) FROM insight_platform.local_console_owner o")
            .fetch_one(&pool)
            .await
            .unwrap();
    let digest = |c: char| {
        format!("sha256:{}", c.to_string().repeat(64))
            .parse()
            .unwrap()
    };
    let release = InstallationReleaseV1 {
        schema_version: 1,
        installation_id: "svc_00000000-0000-7000-8000-000000000001".parse().unwrap(),
        bootstrap_input_digest: digest('a'),
        bootstrap_identity_digest: digest('b'),
        from_package_digest: digest('c'),
        to_package_digest: digest('d'),
        from_schema_version: 16,
        to_schema_version: 17,
        from_inventory_digest: SOURCE_INVENTORY_DIGEST.parse().unwrap(),
        to_inventory_digest: target_inventory_digest().parse().unwrap(),
    };
    let mut wrong = release.clone();
    wrong.to_inventory_digest = digest('f');
    assert!(upgrade(&pool, &wrong).await.is_err());
    let mut tx = pool.begin().await.unwrap();
    let privileges = insight_platform_storage_tooling::privileges::effective_privileges(
        &mut tx,
        "insight_runtime_dev",
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let old_evidence=insight_platform_deployment_contracts::installation::InstallationDatabaseEvidenceV1 {
        schema_version:1,input_digest:release.bootstrap_input_digest.clone(),identity_digest:release.bootstrap_identity_digest.clone(),
        purpose:insight_platform_deployment_contracts::installation::InstallationDatabasePurpose::Runtime,
        roles:vec![insight_platform_deployment_contracts::installation::InstallationDatabaseRoleEvidenceV1 { role_name:"insight_runtime_dev".into(),effective_privileges_digest:insight_platform_contracts::canonical_digest(&privileges).unwrap().parse().unwrap() }],
    };
    insight_platform_storage_tooling::conversation_upgrade::refresh_role_evidence(
        &pool,
        &old_evidence,
    )
    .await
    .unwrap();
    upgrade(&pool, &release).await.unwrap();
    let current_evidence =
        insight_platform_storage_tooling::conversation_upgrade::refresh_role_evidence(
            &pool,
            &old_evidence,
        )
        .await
        .unwrap();
    insight_platform_storage_tooling::conversation_upgrade::refresh_role_evidence(
        &pool,
        &current_evidence,
    )
    .await
    .unwrap();
    // Simulate loss of the response after PostgreSQL committed, before filesystem release publication.
    upgrade(&pool, &release).await.unwrap();
    let after: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(o) FROM insight_platform.local_console_owner o")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(before, after);
    wrong = release.clone();
    wrong.to_package_digest = digest('e');
    assert!(upgrade(&pool, &wrong).await.is_err());
    let access:bool=sqlx::query_scalar("SELECT has_table_privilege('insight_runtime_dev','public.insight_installation_upgrade_receipt','SELECT')").fetch_one(&pool).await.unwrap();
    assert!(!access);
    use insight_platform_deployment_contracts::installation_release::PackageRolloutIntentV1;
    use insight_platform_storage_tooling::conversation_upgrade::rollout_package;
    let mut previous = release;
    for package in ['e', 'f'] {
        let mut target = previous.clone();
        target.to_package_digest = digest(package);
        let intent = PackageRolloutIntentV1 {
            schema_version: 1,
            expected_previous_release_digest: previous.canonical_digest().unwrap(),
            previous_release: previous.clone(),
            target_release: target.clone(),
        };
        let mut bad = intent.clone();
        bad.expected_previous_release_digest = digest('0');
        assert!(rollout_package(&pool, &bad).await.is_err());
        bad = intent.clone();
        bad.previous_release.to_package_digest = digest('1');
        bad.expected_previous_release_digest = bad.previous_release.canonical_digest().unwrap();
        assert!(rollout_package(&pool, &bad).await.is_err());
        rollout_package(&pool, &intent).await.unwrap();
        // PostgreSQL committed but output publication has not happened: exact replay succeeds.
        rollout_package(&pool, &intent).await.unwrap();
        previous = target;
    }
    let after_rollout: serde_json::Value =
        sqlx::query_scalar("SELECT to_jsonb(o) FROM insight_platform.local_console_owner o")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(before, after_rollout);
    pool.close().await;
}
