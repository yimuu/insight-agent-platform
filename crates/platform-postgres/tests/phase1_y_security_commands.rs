use chrono::{Duration, Utc};
use insight_platform_contracts::{
    CommandAudit, CommandOutcome, Permission, PermissionSet, PrincipalBindingsPayload,
    PrincipalKind, ResourceId, SecretBindingPayload, SecretPurpose, SecretResolutionPolicy,
    Sha256Digest, TenantConfig, TenantPrincipalPayload,
};
use insight_platform_postgres::{
    repository::{NewPrincipal, NewTenant, NewTenantPrincipal, PgRepository, RepositoryError},
    verify_schema,
};
use insight_platform_security::{
    BindTenantPrincipal, CreateSecretBinding, EncryptedOpaqueReference,
    PreparedSecretBindingAuthority, PreparedSecretBindingRegistrationDisposition,
    PreparedSecretBindingRegistrationError, RegisterPreparedSecretBinding, RevokeSecretBinding,
    RevokeTenantPrincipal, RotateSecretBinding, SecretBindingResolutionAuthority,
    SecretBindingResolutionError, UpdateTenantPrincipalPermissions,
};
use sqlx::postgres::PgPoolOptions;

const TENANT_ID: &str = "ten_0198f1c3-8f49-7c3e-b1f3-773c28367d10";
const OTHER_TENANT_ID: &str = "ten_0198f1c3-8f49-7c3e-b1f3-773c28367d11";
const ADMIN_ID: &str = "prn_0198f1c3-8f49-7c3e-b1f3-773c28367d12";
const DENIED_ID: &str = "prn_0198f1c3-8f49-7c3e-b1f3-773c28367d13";
const MEMBER_ID: &str = "prn_0198f1c3-8f49-7c3e-b1f3-773c28367d14";
const BROKER_ID: &str = "prn_0198f1c3-8f49-7c3e-b1f3-773c28367d17";
const PROVIDER_ID: &str = "spr_0198f1c3-8f49-7c3e-b1f3-773c28367d15";
const SECRET_ID: &str = "sbd_0198f1c3-8f49-7c3e-b1f3-773c28367d16";
const PREPARED_SECRET_ID: &str = "sbd_0198f1c3-8f49-7c3e-b1f3-773c28367d18";
const RESTRICTED_PREPARED_SECRET_ID: &str = "sbd_0198f1c3-8f49-7c3e-b1f3-773c28367d19";
const SECURITY_AUTHORITY_TEST_ROLE: &str = "platform_security_authority_qualification";

fn id(value: &str) -> ResourceId {
    value.parse().unwrap()
}

fn digest(character: char) -> Sha256Digest {
    format!("sha256:{}", character.to_string().repeat(64))
        .parse()
        .unwrap()
}

fn audit(
    tenant_id: &str,
    principal_id: &str,
    suffix: &str,
    idempotency: char,
    request: char,
) -> CommandAudit {
    CommandAudit {
        tenant_id: id(tenant_id),
        principal_id: id(principal_id),
        principal_kind: PrincipalKind::TenantAdmin,
        receipt_id: id(&format!("rcp_0198f1c3-8f49-7c3e-b1f3-773c2836{suffix}")),
        event_id: id(&format!("evt_0198f1c3-8f49-7c3e-b1f3-773c2836{suffix}")),
        outbox_id: id(&format!("obx_0198f1c3-8f49-7c3e-b1f3-773c2836{suffix}")),
        idempotency_key_digest: digest(idempotency),
        request_digest: digest(request),
        receipt_expires_at: Utc::now() + Duration::hours(1),
    }
}

async fn seed_principal(repository: &PgRepository, principal_id: &str, digest_char: char) {
    repository
        .create_principal(NewPrincipal {
            principal_id: id(principal_id),
            authentication_authority_digest: digest(digest_char),
            subject_digest: digest(
                char::from_digit(digest_char.to_digit(16).unwrap() ^ 1, 16).unwrap(),
            ),
            installation_bindings: PrincipalBindingsPayload {
                installation_bindings: Vec::new(),
            },
        })
        .await
        .unwrap();
}

async fn seed_binding(
    repository: &PgRepository,
    tenant_id: &str,
    principal_id: &str,
    permissions: Vec<Permission>,
) {
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: id(tenant_id),
            principal_id: id(principal_id),
            principal_kind: PrincipalKind::TenantAdmin,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(permissions).unwrap(),
            },
        })
        .await
        .unwrap();
}

async fn seed_service_binding(
    repository: &PgRepository,
    tenant_id: &str,
    principal_id: &str,
    permissions: Vec<Permission>,
) {
    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: id(tenant_id),
            principal_id: id(principal_id),
            principal_kind: PrincipalKind::ServiceIdentity,
            payload: TenantPrincipalPayload {
                permissions: PermissionSet::new(permissions).unwrap(),
            },
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn security_commands_are_fenced_atomic_and_secret_safe() {
    let Ok(database_url) = std::env::var("PLATFORM_TEST_DATABASE_URL") else {
        eprintln!("PLATFORM_TEST_DATABASE_URL is unset; real PostgreSQL fixture skipped");
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let repository = PgRepository::new(pool.clone());

    for tenant_id in [TENANT_ID, OTHER_TENANT_ID] {
        repository
            .create_tenant(NewTenant {
                tenant_id: tenant_id.to_owned(),
                state: "active".to_owned(),
                config: TenantConfig::default(),
            })
            .await
            .unwrap();
    }
    seed_principal(&repository, ADMIN_ID, '1').await;
    seed_principal(&repository, DENIED_ID, '3').await;
    seed_principal(&repository, MEMBER_ID, '5').await;
    seed_principal(&repository, BROKER_ID, '7').await;
    let administrative_permissions = vec![
        Permission::TenantManage,
        Permission::SecretBind,
        Permission::SecretRotate,
        Permission::SecretRevoke,
    ];
    seed_binding(
        &repository,
        TENANT_ID,
        ADMIN_ID,
        administrative_permissions.clone(),
    )
    .await;
    seed_binding(
        &repository,
        OTHER_TENANT_ID,
        ADMIN_ID,
        administrative_permissions,
    )
    .await;
    seed_binding(&repository, TENANT_ID, DENIED_ID, Vec::new()).await;
    seed_service_binding(
        &repository,
        TENANT_ID,
        BROKER_ID,
        vec![Permission::SecretBind],
    )
    .await;

    let bind = BindTenantPrincipal {
        audit: audit(TENANT_ID, ADMIN_ID, "7d01", '1', '2'),
        principal_id: id(MEMBER_ID),
        principal_kind: PrincipalKind::AgentRunner,
        permissions: PermissionSet::new(vec![Permission::AgentRead]).unwrap(),
    };
    let mut transaction = repository.begin_security_transaction().await.unwrap();
    assert!(matches!(
        transaction
            .bind_tenant_principal(bind.clone())
            .await
            .unwrap(),
        CommandOutcome::Applied(_)
    ));
    transaction.rollback().await.unwrap();
    let rolled_back: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.tenant_principals WHERE tenant_id = $1 AND principal_id = $2 AND principal_kind = 'agent_runner'",
    )
    .bind(TENANT_ID)
    .bind(MEMBER_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(rolled_back, 0);

    let mut transaction = repository.begin_security_transaction().await.unwrap();
    let member = match transaction
        .bind_tenant_principal(bind.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("first committed bind unexpectedly replayed"),
    };
    transaction.commit().await.unwrap();
    assert_eq!((member.generation, member.version), (1, 1));

    let mut transaction = repository.begin_security_transaction().await.unwrap();
    assert!(matches!(
        transaction
            .bind_tenant_principal(bind.clone())
            .await
            .unwrap(),
        CommandOutcome::Replayed(_)
    ));
    transaction.commit().await.unwrap();
    let mut conflicting_bind = bind;
    conflicting_bind.audit.request_digest = digest('3');
    let mut transaction = repository.begin_security_transaction().await.unwrap();
    assert!(matches!(
        transaction.bind_tenant_principal(conflicting_bind).await,
        Err(RepositoryError::IdempotencyConflict)
    ));
    transaction.rollback().await.unwrap();

    let mut transaction = repository.begin_security_transaction().await.unwrap();
    let updated = match transaction
        .update_tenant_principal_permissions(UpdateTenantPrincipalPermissions {
            audit: audit(TENANT_ID, ADMIN_ID, "7d02", '2', '4'),
            principal_id: id(MEMBER_ID),
            principal_kind: PrincipalKind::AgentRunner,
            expected_generation: 1,
            expected_version: 1,
            permissions: PermissionSet::new(vec![Permission::AgentRead, Permission::AgentRun])
                .unwrap(),
        })
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("permission update unexpectedly replayed"),
    };
    transaction.commit().await.unwrap();
    assert_eq!((updated.generation, updated.version), (2, 2));

    let secret_payload = |version: char| SecretBindingPayload {
        provider_id: id(PROVIDER_ID),
        resolution_policy: SecretResolutionPolicy::Pinned {
            opaque_version_identity_digest: digest(version),
        },
    };
    let denied_create = CreateSecretBinding {
        audit: audit(TENANT_ID, DENIED_ID, "7d03", '3', '5'),
        secret_binding_id: id(SECRET_ID),
        purpose: "model.provider".parse::<SecretPurpose>().unwrap(),
        encrypted_reference: EncryptedOpaqueReference::new(vec![0x81, 0x15, 0x44]).unwrap(),
        key_id: "kms-test-key".to_owned(),
        reference_digest: digest('6'),
        payload: secret_payload('7'),
    };
    let mut transaction = repository.begin_security_transaction().await.unwrap();
    assert!(matches!(
        transaction.create_secret_binding(denied_create).await,
        Err(RepositoryError::PermissionDenied)
    ));
    transaction.rollback().await.unwrap();

    let create_secret = CreateSecretBinding {
        audit: audit(TENANT_ID, ADMIN_ID, "7d04", '4', '6'),
        secret_binding_id: id(SECRET_ID),
        purpose: "model.provider".parse::<SecretPurpose>().unwrap(),
        encrypted_reference: EncryptedOpaqueReference::new(vec![0x82, 0x16, 0x45]).unwrap(),
        key_id: "kms-test-key".to_owned(),
        reference_digest: digest('8'),
        payload: secret_payload('9'),
    };
    let mut transaction = repository.begin_security_transaction().await.unwrap();
    let secret = match transaction
        .create_secret_binding(create_secret.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("secret create unexpectedly replayed"),
    };
    transaction.commit().await.unwrap();
    assert_eq!((secret.generation, secret.version), (1, 1));
    assert!(!format!("{secret:?}").contains("kms-test-key"));
    let trusted_resolution = repository
        .load_for_resolution(&id(TENANT_ID), &id(SECRET_ID))
        .await
        .unwrap();
    assert_eq!(trusted_resolution.generation, 1);
    assert_eq!(
        trusted_resolution.encrypted_reference.as_bytes(),
        &[0x82, 0x16, 0x45]
    );
    assert!(!format!("{trusted_resolution:?}").contains("kms-test-key"));
    assert_eq!(
        repository
            .load_for_resolution(&id(OTHER_TENANT_ID), &id(SECRET_ID))
            .await
            .unwrap_err(),
        SecretBindingResolutionError::NotFound
    );

    let preparation_digest = digest('c');
    let mut prepared = RegisterPreparedSecretBinding {
        audit: CommandAudit {
            tenant_id: id(TENANT_ID),
            principal_id: id(BROKER_ID),
            principal_kind: PrincipalKind::ServiceIdentity,
            receipt_id: id("rcp_0198f1c3-8f49-7c3e-b1f3-773c28367d0a"),
            event_id: id("evt_0198f1c3-8f49-7c3e-b1f3-773c28367d0a"),
            outbox_id: id("obx_0198f1c3-8f49-7c3e-b1f3-773c28367d0a"),
            idempotency_key_digest: preparation_digest.clone(),
            request_digest: digest('0'),
            receipt_expires_at: Utc::now() + Duration::hours(1),
        },
        preparation_digest,
        secret_binding_id: id(PREPARED_SECRET_ID),
        purpose: "mcp.oauth.pkce".parse().unwrap(),
        provider_id: id(PROVIDER_ID),
        encrypted_reference: EncryptedOpaqueReference::new(
            b"prepared-secret-ciphertext-canary".to_vec(),
        )
        .unwrap(),
        key_id: "prepared-kms-key-canary".to_owned(),
        reference_digest: digest('d'),
        opaque_version_identity_digest: digest('e'),
        provider_storage_evidence_digest: digest('f'),
    };
    prepared.audit.request_digest = prepared.semantic_request_digest().unwrap();
    let applied = repository
        .register_prepared(prepared.clone())
        .await
        .unwrap();
    assert_eq!(
        applied.disposition,
        PreparedSecretBindingRegistrationDisposition::Applied
    );
    let replayed = repository
        .register_prepared(prepared.clone())
        .await
        .unwrap();
    assert_eq!(
        replayed.disposition,
        PreparedSecretBindingRegistrationDisposition::Replayed
    );
    assert_eq!(applied.exact_binding, replayed.exact_binding);
    let prepared_resolution = repository
        .load_for_resolution(&id(TENANT_ID), &id(PREPARED_SECRET_ID))
        .await
        .unwrap();
    assert_eq!(prepared_resolution.generation, 1);
    assert_eq!(
        prepared_resolution.payload.resolution_policy,
        applied.exact_binding.resolution_policy
    );

    let restricted_registration_count = if let Ok(configured_role) =
        std::env::var("PLATFORM_SECURITY_AUTHORITY_TEST_ROLE")
    {
        assert_eq!(configured_role, SECURITY_AUTHORITY_TEST_ROLE);
        let restricted_pool = PgPoolOptions::new()
            .max_connections(2)
            .after_connect(|connection, _metadata| {
                Box::pin(async move {
                    sqlx::query("SET ROLE platform_security_authority_qualification")
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect(&database_url)
            .await
            .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT current_user")
                .fetch_one(&restricted_pool)
                .await
                .unwrap(),
            SECURITY_AUTHORITY_TEST_ROLE
        );
        verify_schema(&restricted_pool).await.unwrap();
        let restricted_repository = PgRepository::new(restricted_pool.clone());
        let resolved = restricted_repository
            .load_for_resolution(&id(TENANT_ID), &id(SECRET_ID))
            .await
            .unwrap();
        assert_eq!(resolved.reference_digest, digest('8'));

        let mut restricted_prepared = prepared.clone();
        restricted_prepared.secret_binding_id = id(RESTRICTED_PREPARED_SECRET_ID);
        restricted_prepared.audit.receipt_id = id("rcp_0198f1c3-8f49-7c3e-b1f3-773c28367d0b");
        restricted_prepared.audit.event_id = id("evt_0198f1c3-8f49-7c3e-b1f3-773c28367d0b");
        restricted_prepared.audit.outbox_id = id("obx_0198f1c3-8f49-7c3e-b1f3-773c28367d0b");
        restricted_prepared.audit.idempotency_key_digest = digest('b');
        restricted_prepared.preparation_digest = digest('b');
        restricted_prepared.encrypted_reference =
            EncryptedOpaqueReference::new(b"restricted-role-ciphertext-canary".to_vec()).unwrap();
        restricted_prepared.reference_digest = digest('a');
        restricted_prepared.opaque_version_identity_digest = digest('9');
        restricted_prepared.provider_storage_evidence_digest = digest('8');
        restricted_prepared.audit.request_digest =
            restricted_prepared.semantic_request_digest().unwrap();
        let restricted_outcome = restricted_repository
            .register_prepared(restricted_prepared)
            .await
            .unwrap();
        assert_eq!(
            restricted_outcome.disposition,
            PreparedSecretBindingRegistrationDisposition::Applied
        );

        let unauthorized_read =
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM insight_platform.runs")
                .fetch_one(&restricted_pool)
                .await
                .unwrap_err();
        assert_eq!(
            unauthorized_read
                .as_database_error()
                .and_then(|failure| failure.code().map(|code| code.into_owned()))
                .as_deref(),
            Some("42501")
        );
        let unauthorized_mutation = sqlx::query(
            "UPDATE insight_platform.secret_bindings SET state = state WHERE tenant_id = $1 AND secret_binding_id = $2",
        )
        .bind(TENANT_ID)
        .bind(SECRET_ID)
        .execute(&restricted_pool)
        .await
        .unwrap_err();
        assert_eq!(
            unauthorized_mutation
                .as_database_error()
                .and_then(|failure| failure.code().map(|code| code.into_owned()))
                .as_deref(),
            Some("42501")
        );
        restricted_pool.close().await;
        1
    } else {
        eprintln!(
            "PLATFORM_SECURITY_AUTHORITY_TEST_ROLE is unset; least-privilege role fixture skipped"
        );
        0
    };

    let mut drifted = prepared;
    drifted.provider_storage_evidence_digest = digest('1');
    drifted.audit.request_digest = drifted.semantic_request_digest().unwrap();
    assert_eq!(
        repository.register_prepared(drifted).await.unwrap_err(),
        PreparedSecretBindingRegistrationError::Rejected
    );

    let mut transaction = repository.begin_security_transaction().await.unwrap();
    let rotated = match transaction
        .rotate_secret_binding(RotateSecretBinding {
            audit: audit(TENANT_ID, ADMIN_ID, "7d05", '5', '7'),
            secret_binding_id: id(SECRET_ID),
            expected_generation: 1,
            expected_version: 1,
            encrypted_reference: EncryptedOpaqueReference::new(vec![0x83, 0x17, 0x46]).unwrap(),
            key_id: "kms-test-key-rotated".to_owned(),
            reference_digest: digest('a'),
            payload: secret_payload('b'),
            provider_evidence_digest: digest('c'),
        })
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("secret rotation unexpectedly replayed"),
    };
    transaction.commit().await.unwrap();
    assert_eq!((rotated.generation, rotated.version), (2, 2));

    let mut transaction = repository.begin_security_transaction().await.unwrap();
    assert!(matches!(
        transaction
            .rotate_secret_binding(RotateSecretBinding {
                audit: audit(TENANT_ID, ADMIN_ID, "7d06", '6', '8'),
                secret_binding_id: id(SECRET_ID),
                expected_generation: 1,
                expected_version: 1,
                encrypted_reference: EncryptedOpaqueReference::new(vec![0x84]).unwrap(),
                key_id: "stale-key".to_owned(),
                reference_digest: digest('d'),
                payload: secret_payload('e'),
                provider_evidence_digest: digest('f'),
            })
            .await,
        Err(RepositoryError::Conflict("secret binding"))
    ));
    transaction.rollback().await.unwrap();

    let revoke = RevokeSecretBinding {
        audit: audit(TENANT_ID, ADMIN_ID, "7d07", '7', '9'),
        secret_binding_id: id(SECRET_ID),
        expected_generation: 2,
        expected_version: 2,
    };
    let mut transaction = repository.begin_security_transaction().await.unwrap();
    let revoked = match transaction
        .revoke_secret_binding(revoke.clone())
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("secret revoke unexpectedly replayed"),
    };
    transaction.commit().await.unwrap();
    assert_eq!(
        (revoked.state.as_str(), revoked.generation, revoked.version),
        ("revoked", 3, 3)
    );
    let revoked_resolution = repository
        .load_for_resolution(&id(TENANT_ID), &id(SECRET_ID))
        .await
        .unwrap();
    assert_eq!(revoked_resolution.state.as_str(), "revoked");
    assert_eq!(revoked_resolution.generation, 3);

    let mut transaction = repository.begin_security_transaction().await.unwrap();
    assert!(matches!(
        transaction.revoke_secret_binding(revoke).await.unwrap(),
        CommandOutcome::Replayed(_)
    ));
    transaction.commit().await.unwrap();

    let mut transaction = repository.begin_security_transaction().await.unwrap();
    assert!(matches!(
        transaction
            .revoke_secret_binding(RevokeSecretBinding {
                audit: audit(OTHER_TENANT_ID, ADMIN_ID, "7d08", '8', 'a'),
                secret_binding_id: id(SECRET_ID),
                expected_generation: 3,
                expected_version: 3,
            })
            .await,
        Err(RepositoryError::NotFound("secret binding"))
    ));
    transaction.rollback().await.unwrap();

    let mut transaction = repository.begin_security_transaction().await.unwrap();
    let revoked_member = match transaction
        .revoke_tenant_principal(RevokeTenantPrincipal {
            audit: audit(TENANT_ID, ADMIN_ID, "7d09", '9', 'b'),
            principal_id: id(MEMBER_ID),
            principal_kind: PrincipalKind::AgentRunner,
            expected_generation: 2,
            expected_version: 2,
        })
        .await
        .unwrap()
    {
        CommandOutcome::Applied(record) => record,
        CommandOutcome::Replayed(_) => panic!("membership revoke unexpectedly replayed"),
    };
    transaction.commit().await.unwrap();
    assert_eq!(revoked_member.state, "revoked");

    let command_receipts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.receipts WHERE tenant_id = $1 AND state = 'succeeded'",
    )
    .bind(TENANT_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    let command_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.events WHERE tenant_id = $1 AND event_type LIKE 'security.%'",
    )
    .bind(TENANT_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    let outbox_events: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM insight_platform.outbox_events WHERE tenant_id = $1",
    )
    .bind(TENANT_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    let expected_security_records = 7 + restricted_registration_count;
    assert_eq!(
        (command_receipts, command_events, outbox_events),
        (
            expected_security_records,
            expected_security_records,
            expected_security_records
        )
    );

    let leaked: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM insight_platform.events
            WHERE tenant_id = $1 AND payload::text LIKE '%kms-test-key%'
            UNION ALL
            SELECT 1 FROM insight_platform.receipts
            WHERE tenant_id = $1 AND payload::text LIKE '%kms-test-key%'
            UNION ALL
            SELECT 1 FROM insight_platform.events
            WHERE tenant_id = $1 AND payload::text LIKE '%prepared-secret-ciphertext-canary%'
            UNION ALL
            SELECT 1 FROM insight_platform.receipts
            WHERE tenant_id = $1 AND payload::text LIKE '%prepared-kms-key-canary%'
        )
        "#,
    )
    .bind(TENANT_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!leaked);
}
