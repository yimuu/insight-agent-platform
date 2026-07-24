use chrono::{DateTime, Utc};
use insight_engine::repository::REPOSITORY_MIGRATION_FAILED;
use insight_storage::{
    repository::migration_manifest::DURABLE_V3_MIGRATIONS, PostgresDurableRepository,
};
use sqlx::{postgres::PgPoolOptions, AssertSqlSafe, PgPool};
use tokio::sync::Barrier;
use uuid::Uuid;

struct IsolatedPostgresSchema {
    admin: PgPool,
    control: PgPool,
    repository: PostgresDurableRepository,
    scoped_url: String,
    schema: String,
}

fn postgres_test_url() -> Option<String> {
    match std::env::var("V3_TEST_POSTGRES_URL") {
        Ok(value) => Some(value),
        Err(error) if std::env::var_os("CI").is_some() => {
            panic!("CI must set V3_TEST_POSTGRES_URL for migration coordinator tests: {error}")
        }
        Err(_) => None,
    }
}

async fn isolated_schema(label: &str) -> Option<IsolatedPostgresSchema> {
    let database_url = postgres_test_url()?;
    let schema = format!("migration_v3_{label}_{}", Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    let control = PgPoolOptions::new()
        .max_connections(8)
        .connect(&scoped_url)
        .await
        .unwrap();
    Some(IsolatedPostgresSchema {
        admin,
        control,
        repository,
        scoped_url,
        schema,
    })
}

async fn cleanup(schema: IsolatedPostgresSchema) {
    schema.control.close().await;
    sqlx::query(AssertSqlSafe(format!(
        "DROP SCHEMA {} CASCADE",
        schema.schema
    )))
    .execute(&schema.admin)
    .await
    .unwrap();
    schema.admin.close().await;
}

async fn assert_migration_failure(repository: &PostgresDurableRepository) {
    let error = repository.migrate_schema().await.unwrap_err();
    assert_eq!(error.code(), REPOSITORY_MIGRATION_FAILED);
}

#[tokio::test]
async fn postgres_migration_coordinator_is_concurrent_idempotent_and_exactly_once() {
    let Some(schema) = isolated_schema("concurrent").await else {
        return;
    };
    let left = PostgresDurableRepository::connect(&schema.scoped_url)
        .await
        .unwrap();
    let right = PostgresDurableRepository::connect(&schema.scoped_url)
        .await
        .unwrap();
    let barrier = std::sync::Arc::new(Barrier::new(3));
    let left_barrier = barrier.clone();
    let left = tokio::spawn(async move {
        left_barrier.wait().await;
        left.migrate_schema().await
    });
    let right_barrier = barrier.clone();
    let right = tokio::spawn(async move {
        right_barrier.wait().await;
        right.initialize_schema().await
    });
    barrier.wait().await;
    left.await.unwrap().unwrap();
    right.await.unwrap().unwrap();

    let rows = sqlx::query_as::<_, (i64, String, String, DateTime<Utc>)>(
        "SELECT version,name,checksum,applied_at
         FROM schema_migrations ORDER BY version",
    )
    .fetch_all(&schema.control)
    .await
    .unwrap();
    assert_eq!(rows.len(), DURABLE_V3_MIGRATIONS.len());
    for (row, migration) in rows.iter().zip(DURABLE_V3_MIGRATIONS.iter()) {
        assert_eq!(u64::try_from(row.0).unwrap(), migration.version);
        assert_eq!(row.1, migration.name);
        assert_eq!(row.2, migration.postgres_checksum());
    }
    let before = rows.iter().map(|row| (row.0, row.3)).collect::<Vec<_>>();
    schema.repository.migrate_schema().await.unwrap();
    let after = sqlx::query_as::<_, (i64, DateTime<Utc>)>(
        "SELECT version,applied_at
         FROM schema_migrations ORDER BY version",
    )
    .fetch_all(&schema.control)
    .await
    .unwrap();
    assert_eq!(
        after, before,
        "idempotent startup must not rewrite ledger rows"
    );
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT to_regclass('workflow_runs') IS NOT NULL")
            .fetch_one(&schema.control)
            .await
            .unwrap()
    );
    cleanup(schema).await;
}

#[tokio::test]
async fn postgres_migration_coordinator_rejects_checksum_name_hole_and_unknown_rows() {
    let Some(checksum) = isolated_schema("checksum").await else {
        return;
    };
    checksum.repository.migrate_schema().await.unwrap();
    sqlx::query(
        "UPDATE schema_migrations
         SET checksum=$1 WHERE version=$2",
    )
    .bind(format!("sha256:{}", "0".repeat(64)))
    .bind(i64::try_from(DURABLE_V3_MIGRATIONS[0].version).unwrap())
    .execute(&checksum.control)
    .await
    .unwrap();
    assert_migration_failure(&checksum.repository).await;
    cleanup(checksum).await;

    let Some(name) = isolated_schema("name").await else {
        return;
    };
    name.repository.migrate_schema().await.unwrap();
    sqlx::query(
        "UPDATE schema_migrations
         SET name='drifted_migration.sql' WHERE version=$1",
    )
    .bind(i64::try_from(DURABLE_V3_MIGRATIONS[0].version).unwrap())
    .execute(&name.control)
    .await
    .unwrap();
    assert_migration_failure(&name.repository).await;
    cleanup(name).await;

    let Some(hole) = isolated_schema("hole").await else {
        return;
    };
    hole.repository.migrate_schema().await.unwrap();
    let missing_version = DURABLE_V3_MIGRATIONS[5].version;
    sqlx::query("DELETE FROM schema_migrations WHERE version=$1")
        .bind(i64::try_from(missing_version).unwrap())
        .execute(&hole.control)
        .await
        .unwrap();
    assert_migration_failure(&hole.repository).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM schema_migrations WHERE version=$1",)
            .bind(i64::try_from(missing_version).unwrap())
            .fetch_one(&hole.control)
            .await
            .unwrap(),
        0,
        "a hole must not be silently replayed over later authority",
    );
    cleanup(hole).await;

    let Some(unknown) = isolated_schema("unknown").await else {
        return;
    };
    unknown.repository.migrate_schema().await.unwrap();
    let unknown_version = DURABLE_V3_MIGRATIONS.last().unwrap().version + 1;
    sqlx::query(
        "INSERT INTO schema_migrations(version,name,checksum)
         VALUES ($1,'unknown_newer.sql',$2)",
    )
    .bind(i64::try_from(unknown_version).unwrap())
    .bind(format!("sha256:{}", "f".repeat(64)))
    .execute(&unknown.control)
    .await
    .unwrap();
    assert_migration_failure(&unknown.repository).await;
    cleanup(unknown).await;
}

#[tokio::test]
async fn postgres_migration_coordinator_rejects_unledgered_schema_without_adoption() {
    let Some(schema) = isolated_schema("unledgered").await else {
        return;
    };
    sqlx::raw_sql(DURABLE_V3_MIGRATIONS[0].postgres_sql)
        .execute(&schema.control)
        .await
        .unwrap();
    assert_migration_failure(&schema.repository).await;
    assert!(
        !sqlx::query_scalar::<_, bool>("SELECT to_regclass('schema_migrations') IS NOT NULL",)
            .fetch_one(&schema.control)
            .await
            .unwrap()
    );
    cleanup(schema).await;
}
