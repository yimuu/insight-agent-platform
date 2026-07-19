use sqlx::postgres::PgPoolOptions;

/// PostgreSQL integration tests may remain opt-in for local development, but
/// CI must never silently convert the durable-kernel gates into no-ops.
#[tokio::test]
async fn ci_requires_the_shared_real_postgres_16_gate() {
    if std::env::var_os("CI").is_none() {
        return;
    }
    let database_url = std::env::var("V3_TEST_POSTGRES_URL")
        .expect("CI must set V3_TEST_POSTGRES_URL for durable-v3 gates");
    let artifact_url = std::env::var("V3_ARTIFACT_TEST_POSTGRES_URL")
        .expect("CI must set V3_ARTIFACT_TEST_POSTGRES_URL for artifact gates");
    let repository_url = std::env::var("TEST_POSTGRES_URL")
        .expect("CI must set TEST_POSTGRES_URL for repository-level PostgreSQL gates");
    assert_eq!(
        artifact_url, database_url,
        "all v3 PostgreSQL gates must use the same authoritative CI service"
    );
    assert_eq!(
        repository_url, database_url,
        "repository-level PostgreSQL tests must use the shared authoritative CI service"
    );

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("CI PostgreSQL service must be reachable");
    let version: String = sqlx::query_scalar("SHOW server_version_num")
        .fetch_one(&pool)
        .await
        .expect("CI PostgreSQL version must be readable");
    let version = version
        .parse::<u32>()
        .expect("PostgreSQL server_version_num must be numeric");
    assert!(
        (160_000..170_000).contains(&version),
        "durable-v3 CI gate requires PostgreSQL 16, found {version}"
    );
}
