use sqlx::{postgres::PgPoolOptions, PgPool};

async fn privilege(pool: &PgPool, role: &str, table: &str, privilege: &str) -> bool {
    assert!(
        role.len() <= 63
            && role
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    );
    assert!(table
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte == b'_'));
    // Execute as the actual database role. Catalog metadata alone does not prove that inherited
    // or role-specific privileges match the process boundary. Every query is mutation-free.
    let mut transaction = pool.begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("SET LOCAL ROLE \"{role}\"")))
        .execute(&mut *transaction)
        .await
        .unwrap();
    let sql = match privilege {
        "SELECT" => format!("SELECT * FROM insight_platform.{table} LIMIT 0"),
        "INSERT" => {
            let column = if table == "scheduler_state" {
                "work_class"
            } else {
                "tenant_id"
            };
            format!("INSERT INTO insight_platform.{table} ({column}) SELECT {column} FROM insight_platform.{table} WHERE FALSE")
        }
        "UPDATE" => {
            let column = if table == "scheduler_state" {
                "work_class"
            } else {
                "tenant_id"
            };
            format!("UPDATE insight_platform.{table} SET {column} = {column} WHERE FALSE")
        }
        "DELETE" => format!("DELETE FROM insight_platform.{table} WHERE FALSE"),
        _ => panic!("unregistered privilege test"),
    };
    let result = sqlx::query(sqlx::AssertSqlSafe(sql))
        .execute(&mut *transaction)
        .await;
    transaction.rollback().await.unwrap();
    match result {
        Ok(_) => true,
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("42501") => false,
        Err(error) => panic!("role permission fixture failed for {table}/{privilege}: {error}"),
    }
}

fn fixture_variable(name: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| panic!("{name} is required for the real PostgreSQL role fixture"))
}

#[tokio::test]
async fn artifact_roles_have_closed_mutually_denied_table_permissions() {
    let database_url = fixture_variable("PLATFORM_TEST_DATABASE_URL");
    let variables = [
        "PLATFORM_ARTIFACT_GATEWAY_TEST_ROLE",
        "PLATFORM_ARTIFACT_DATA_READER_TEST_ROLE",
        "PLATFORM_ARTIFACT_DATA_WORKER_TEST_ROLE",
        "PLATFORM_ARTIFACT_MAINTENANCE_TEST_ROLE",
    ];
    let roles = variables
        .iter()
        .map(|name| fixture_variable(name))
        .collect::<Vec<_>>();
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

    for role in [worker, maintenance] {
        for table in ["scheduler_state", "scheduler_tenant_state"] {
            assert!(privilege(&pool, role, table, "SELECT").await);
            assert!(privilege(&pool, role, table, "UPDATE").await);
            assert!(!privilege(&pool, role, table, "INSERT").await);
            assert!(!privilege(&pool, role, table, "DELETE").await);
        }
        assert!(privilege(&pool, role, "tenants", "SELECT").await);
        assert!(!privilege(&pool, role, "tenants", "UPDATE").await);
    }
    assert!(privilege(&pool, maintenance, "artifact_blobs", "UPDATE").await);
    assert!(privilege(&pool, maintenance, "events", "INSERT").await);
    assert!(!privilege(&pool, maintenance, "jobs", "INSERT").await);
    assert!(!privilege(&pool, maintenance, "artifact_links", "SELECT").await);
    assert!(!privilege(&pool, maintenance, "tenant_principals", "SELECT").await);
}
