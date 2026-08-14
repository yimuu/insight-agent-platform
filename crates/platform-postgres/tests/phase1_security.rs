use insight_platform_contracts::{
    PrincipalBindingsPayload, PrincipalKind, ResourceId, SecretBindingPayload, SecretPurpose,
    SecretResolutionPolicy, Sha256Digest, TenantConfig, TenantPrincipalPayload,
};
use insight_platform_postgres::{
    repository::{
        BootstrapInstallationOperator, BootstrapOutcome, NewPrincipal, NewSecretBinding, NewTenant,
        NewTenantPrincipal, PgRepository, RepositoryError,
    },
    verify_schema,
};
use sqlx::postgres::PgPoolOptions;

const OPERATOR_ID: &str = "prn_0198f1c3-8f49-7c3e-b1f3-773c28367b90";
const REQUEST_ID: &str = "req_0198f1c3-8f49-7c3e-b1f3-773c28367b91";
const TENANT_ID: &str = "ten_0198f1c3-8f49-7c3e-b1f3-773c28367b92";
const TENANT_B_ID: &str = "ten_0198f1c3-8f49-7c3e-b1f3-773c28367b93";
const PRINCIPAL_ID: &str = "prn_0198f1c3-8f49-7c3e-b1f3-773c28367b94";
const SECRET_PROVIDER_ID: &str = "spr_0198f1c3-8f49-7c3e-b1f3-773c28367b95";
const SECRET_BINDING_ID: &str = "sbd_0198f1c3-8f49-7c3e-b1f3-773c28367b96";

fn digest(character: char) -> Sha256Digest {
    format!("sha256:{}", character.to_string().repeat(64))
        .parse()
        .unwrap()
}

fn id(value: &str) -> ResourceId {
    value.parse().unwrap()
}

#[tokio::test]
async fn phase1_bootstrap_membership_and_secret_contract() {
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

    let bootstrap = BootstrapInstallationOperator {
        principal_id: id(OPERATOR_ID),
        request_id: id(REQUEST_ID),
        authentication_authority_digest: digest('a'),
        subject_digest: digest('b'),
        evidence_digest: digest('c'),
    };
    assert_eq!(
        repository
            .bootstrap_installation_operator(bootstrap.clone())
            .await
            .unwrap(),
        BootstrapOutcome::Created
    );
    assert_eq!(
        repository
            .bootstrap_installation_operator(bootstrap.clone())
            .await
            .unwrap(),
        BootstrapOutcome::Replayed
    );
    let mut conflicting_bootstrap = bootstrap;
    conflicting_bootstrap.evidence_digest = digest('d');
    assert!(matches!(
        repository
            .bootstrap_installation_operator(conflicting_bootstrap)
            .await,
        Err(RepositoryError::Conflict("installation bootstrap"))
    ));

    repository
        .create_tenant(NewTenant {
            tenant_id: TENANT_ID.to_owned(),
            state: "active".to_owned(),
            config: TenantConfig {
                scheduling_policy: None,
            },
        })
        .await
        .unwrap();
    repository
        .create_tenant(NewTenant {
            tenant_id: TENANT_B_ID.to_owned(),
            state: "active".to_owned(),
            config: TenantConfig {
                scheduling_policy: None,
            },
        })
        .await
        .unwrap();
    repository
        .create_principal(NewPrincipal {
            principal_id: id(PRINCIPAL_ID),
            authentication_authority_digest: digest('e'),
            subject_digest: digest('f'),
            installation_bindings: PrincipalBindingsPayload {
                installation_bindings: Vec::new(),
            },
        })
        .await
        .unwrap();

    let runner = repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: id(TENANT_ID),
            principal_id: id(PRINCIPAL_ID),
            principal_kind: PrincipalKind::AgentRunner,
            payload: TenantPrincipalPayload {
                permissions: insight_platform_contracts::PermissionSet::new(vec![
                    insight_platform_contracts::Permission::AgentRead,
                    insight_platform_contracts::Permission::AgentRun,
                ])
                .unwrap(),
            },
        })
        .await
        .unwrap();
    assert_eq!(runner.principal_kind, "agent_runner");

    repository
        .bind_tenant_principal(NewTenantPrincipal {
            tenant_id: id(TENANT_ID),
            principal_id: id(PRINCIPAL_ID),
            principal_kind: PrincipalKind::AgentAuthor,
            payload: TenantPrincipalPayload {
                permissions: insight_platform_contracts::PermissionSet::new(vec![
                    insight_platform_contracts::Permission::AgentRead,
                    insight_platform_contracts::Permission::AgentWrite,
                ])
                .unwrap(),
            },
        })
        .await
        .unwrap();
    assert!(matches!(
        repository
            .bind_tenant_principal(NewTenantPrincipal {
                tenant_id: id(TENANT_ID),
                principal_id: id(PRINCIPAL_ID),
                principal_kind: PrincipalKind::InstallationOperator,
                payload: TenantPrincipalPayload {
                    permissions: insight_platform_contracts::PermissionSet::new(vec![]).unwrap(),
                },
            })
            .await,
        Err(RepositoryError::InvalidInput(_))
    ));

    let secret_payload = SecretBindingPayload {
        provider_id: id(SECRET_PROVIDER_ID),
        resolution_policy: SecretResolutionPolicy::Pinned {
            opaque_version_identity_digest: digest('1'),
        },
    };
    repository
        .create_secret_binding(NewSecretBinding {
            tenant_id: id(TENANT_ID),
            secret_binding_id: id(SECRET_BINDING_ID),
            purpose: "model.provider".parse::<SecretPurpose>().unwrap(),
            provider_id: id(SECRET_PROVIDER_ID),
            opaque_reference_ciphertext: vec![0x83, 0x15, 0xc7, 0x44],
            key_id: "kms-key-1".to_owned(),
            reference_digest: digest('2'),
            payload: secret_payload,
        })
        .await
        .unwrap();

    let persisted_text: String = sqlx::query_scalar(
        r#"
        SELECT concat_ws('|', purpose, provider, key_id, reference_digest, payload::text)
        FROM insight_platform.secret_bindings
        WHERE tenant_id = $1 AND secret_binding_id = $2
        "#,
    )
    .bind(TENANT_ID)
    .bind(SECRET_BINDING_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!persisted_text.contains("phase1-secret-canary"));

    let cross_tenant_rows: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*) FROM insight_platform.tenant_principals
        WHERE tenant_id = $1 AND principal_id = $2
        "#,
    )
    .bind(TENANT_B_ID)
    .bind(PRINCIPAL_ID)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(cross_tenant_rows, 0);
}
