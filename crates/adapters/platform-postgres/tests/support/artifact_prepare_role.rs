//! Exercise the public upload authority through the actual restricted Artifact Gateway role.
use insight_platform_contracts::{PrincipalKind, ResourceId};
use insight_platform_postgres::repository::{
    BootstrapDevelopmentProfile, PgRepository, RepositoryError,
};
use sqlx::{postgres::PgPoolOptions, PgPool};

pub(super) async fn verify(pool: &PgPool, url: &str, profile: &BootstrapDevelopmentProfile) {
    let tenant: ResourceId = profile.tenant.tenant_id.parse().unwrap();
    let seed = profile.artifact_authority.as_ref().unwrap();
    let expected = PgRepository::new(pool.clone())
        .resolve_public_artifact_prepare_authority(
            tenant.clone(),
            profile.developer.principal_id.clone(),
            PrincipalKind::AgentAuthor,
        )
        .await
        .unwrap();
    assert_eq!(
        expected.artifact_io_policy_revision.revision_id,
        seed.artifact_io_policy_revision_id
    );
    assert_eq!(
        expected.retention_policy_revision.revision_id,
        seed.retention_policy_revision_id
    );
    assert_eq!(expected.quota_account_id, seed.staging_quota_account_id);

    let nonce = uuid::Uuid::now_v7().simple().to_string();
    let roles = ["gateway", "reader", "worker", "maintenance"]
        .map(|purpose| format!("insight_prepare_{purpose}_{nonce}"));
    let gateway = roles[0].clone();
    let mut transaction = pool.begin().await.unwrap();
    for role in &roles {
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "CREATE ROLE {role} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT"
        )))
        .execute(&mut *transaction)
        .await
        .unwrap();
    }
    let mut grants = insight_platform_postgres::artifact_repository::artifact_role_grants_sql()
        .replace("\\set ON_ERROR_STOP on", "")
        .replace("BEGIN;", "")
        .replace("COMMIT;", "");
    for (variable, role) in [
        "artifact_gateway_role",
        "artifact_data_reader_role",
        "artifact_data_worker_role",
        "artifact_maintenance_role",
    ]
    .into_iter()
    .zip(&roles)
    {
        grants = grants.replace(&format!(":'{variable}'"), &format!("'{role}'"));
    }
    sqlx::raw_sql(sqlx::AssertSqlSafe(grants))
        .execute(&mut *transaction)
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    let after_connect_role = gateway.clone();
    let restricted = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |connection, _| {
            let role = after_connect_role.clone();
            Box::pin(async move {
                sqlx::query(sqlx::AssertSqlSafe(format!("SET ROLE {role}")))
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect(url)
        .await
        .unwrap();
    let current: String = sqlx::query_scalar("SELECT current_user")
        .fetch_one(&restricted)
        .await
        .unwrap();
    assert_eq!(current, gateway);
    let repository = PgRepository::new(restricted.clone());
    let observed = repository
        .resolve_public_artifact_prepare_authority(
            tenant.clone(),
            profile.developer.principal_id.clone(),
            PrincipalKind::AgentAuthor,
        )
        .await
        .unwrap();
    assert_eq!(observed, expected);
    let error =
        sqlx::query("UPDATE insight_platform.deployments SET tenant_id=tenant_id WHERE FALSE")
            .execute(&restricted)
            .await
            .unwrap_err();
    assert!(
        matches!(error, sqlx::Error::Database(ref error) if error.code().as_deref() == Some("42501"))
    );

    // Reproduce the original missing grant against the same real owner read, not only a table list.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "REVOKE SELECT ON insight_platform.deployments FROM {gateway}"
    )))
    .execute(pool)
    .await
    .unwrap();
    let error = repository
        .resolve_public_artifact_prepare_authority(
            tenant,
            profile.developer.principal_id.clone(),
            PrincipalKind::AgentAuthor,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, RepositoryError::Database(sqlx::Error::Database(ref error)) if error.code().as_deref() == Some("42501"))
    );
    restricted.close().await;
    // These fresh NOLOGIN identities own no objects; remove only this fixture's grants and roles.
    for role in roles {
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "DROP OWNED BY {role}; DROP ROLE {role}"
        )))
        .execute(pool)
        .await
        .unwrap();
    }
}
