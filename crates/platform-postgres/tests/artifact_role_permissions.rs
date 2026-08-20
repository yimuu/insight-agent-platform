use sqlx::{postgres::PgPoolOptions, PgPool};

async fn privilege(pool: &PgPool, role: &str, table: &str, privilege: &str) -> bool {
    sqlx::query_scalar::<_, bool>("SELECT has_table_privilege($1, $2, $3)")
        .bind(role)
        .bind(format!("insight_platform.{table}"))
        .bind(privilege)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn artifact_roles_have_closed_mutually_denied_table_permissions() {
    let Some(database_url) = std::env::var("PLATFORM_DATABASE_URL").ok() else {
        return;
    };
    let variables = [
        "PLATFORM_ARTIFACT_GATEWAY_TEST_ROLE",
        "PLATFORM_ARTIFACT_DATA_READER_TEST_ROLE",
        "PLATFORM_ARTIFACT_DATA_WORKER_TEST_ROLE",
        "PLATFORM_ARTIFACT_MAINTENANCE_TEST_ROLE",
    ];
    let Some(roles) = variables
        .iter()
        .map(|name| std::env::var(name).ok())
        .collect::<Option<Vec<_>>>()
    else {
        return;
    };
    assert_eq!(
        roles
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        4
    );
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let gateway = &roles[0];
    let reader = &roles[1];
    let worker = &roles[2];
    let maintenance = &roles[3];

    assert!(privilege(&pool, reader, "artifact_blobs", "SELECT").await);
    assert!(!privilege(&pool, reader, "artifact_blobs", "UPDATE").await);
    assert!(!privilege(&pool, reader, "receipts", "SELECT").await);

    assert!(privilege(&pool, gateway, "artifacts", "INSERT").await);
    assert!(privilege(&pool, gateway, "quota_accounts", "UPDATE").await);
    assert!(!privilege(&pool, gateway, "artifacts", "DELETE").await);
    assert!(!privilege(&pool, gateway, "secret_bindings", "SELECT").await);

    assert!(privilege(&pool, worker, "artifacts", "UPDATE").await);
    assert!(privilege(&pool, worker, "jobs", "INSERT").await);
    assert!(!privilege(&pool, worker, "artifacts", "INSERT").await);
    assert!(!privilege(&pool, worker, "artifact_links", "SELECT").await);
    assert!(!privilege(&pool, worker, "tenant_principals", "SELECT").await);

    assert!(privilege(&pool, maintenance, "artifact_blobs", "UPDATE").await);
    assert!(privilege(&pool, maintenance, "events", "INSERT").await);
    assert!(!privilege(&pool, maintenance, "jobs", "INSERT").await);
    assert!(!privilege(&pool, maintenance, "artifact_links", "SELECT").await);
    assert!(!privilege(&pool, maintenance, "tenant_principals", "SELECT").await);
}
