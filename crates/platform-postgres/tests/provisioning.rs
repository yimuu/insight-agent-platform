use insight_platform_postgres::{provision_schema, verify_schema, AuthoritySchemaError};
use sqlx::{postgres::PgPoolOptions, Row};

/// This fixture intentionally receives an empty, separately-created database. Provisioning is
/// destructive only in the sense that it refuses an already-owned authority; it never attempts
/// to repair, alter, or replace an existing schema.
#[tokio::test]
async fn fresh_authority_is_provisioned_once_and_then_verified() {
    let Ok(database_url) = std::env::var("PLATFORM_TEST_PROVISION_DATABASE_URL") else {
        eprintln!(
            "PLATFORM_TEST_PROVISION_DATABASE_URL is unset; fresh provisioning fixture skipped"
        );
        return;
    };
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();

    let provisioned = provision_schema(&pool).await.unwrap();
    let verified = verify_schema(&pool).await.unwrap();
    assert_eq!(provisioned, verified);
    assert_eq!(provisioned.table_count, 23);

    assert!(matches!(
        provision_schema(&pool).await,
        Err(AuthoritySchemaError::SchemaAlreadyProvisioned)
    ));

    let rows = sqlx::query(
        "SELECT (SELECT count(*) FROM insight_platform.schema_migrations) AS migrations, (SELECT count(*) FROM pg_catalog.pg_tables WHERE schemaname = 'insight_platform') AS tables",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(rows.get::<i64, _>("migrations"), 1);
    assert_eq!(rows.get::<i64, _>("tables"), 23);
}
