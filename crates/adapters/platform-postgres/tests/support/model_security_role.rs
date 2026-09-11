//! Real Security Authority privileges, mandatory within the actual Model PostgreSQL fixture.
use insight_platform_postgres::{security_authority_role_grants_sql, verify_schema};
use sqlx::{postgres::PgPoolOptions, PgPool};

pub struct ModelSecurityRole {
    owner: PgPool,
    name: String,
    pub pool: PgPool,
}

impl ModelSecurityRole {
    pub async fn create(owner: &PgPool) -> Self {
        let name = format!("model_security_{}", uuid::Uuid::now_v7().simple());
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "CREATE ROLE {name} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT"
        )))
        .execute(owner)
        .await
        .unwrap();
        let grants = security_authority_role_grants_sql()
            .replace("\\set ON_ERROR_STOP on", "")
            .replace(":'security_authority_role'", &format!("'{name}'"));
        sqlx::raw_sql(sqlx::AssertSqlSafe(grants))
            .execute(owner)
            .await
            .unwrap();
        let connected_role = name.clone();
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .after_connect(move |connection, _| {
                let name = connected_role.clone();
                Box::pin(async move {
                    sqlx::query(sqlx::AssertSqlSafe(format!("SET ROLE {name}")))
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect_with((*owner.connect_options()).clone())
            .await
            .unwrap();
        verify_schema(&pool).await.unwrap();
        let actual: String = sqlx::query_scalar("SELECT current_user")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(actual, name);
        Self {
            owner: owner.clone(),
            name,
            pool,
        }
    }

    pub async fn assert_read_boundary(&self) {
        let can_create: bool = sqlx::query_scalar(
            "SELECT has_schema_privilege(current_user, 'insight_platform', 'CREATE')",
        )
        .fetch_one(&self.pool)
        .await
        .unwrap();
        assert!(!can_create);
        for sql in [
            "SELECT * FROM insight_platform.artifacts LIMIT 0",
            "SELECT metadata FROM insight_platform.artifacts LIMIT 0",
            "SELECT * FROM insight_platform.artifact_blobs LIMIT 0",
            "SELECT object_reference_ciphertext FROM insight_platform.artifact_blobs LIMIT 0",
            "SELECT object_generation FROM insight_platform.artifact_blobs LIMIT 0",
            "SELECT * FROM insight_platform.artifact_links LIMIT 0",
            "SELECT * FROM insight_platform.run_values LIMIT 0",
            "SELECT state FROM insight_platform.artifacts WHERE false FOR SHARE",
            "SELECT state FROM insight_platform.artifact_blobs WHERE false FOR SHARE",
            "UPDATE insight_platform.jobs SET priority=priority WHERE false",
            "UPDATE insight_platform.resources SET gate_state=gate_state WHERE false",
            "UPDATE insight_platform.artifacts SET state=state WHERE false",
            "UPDATE insight_platform.artifact_blobs SET state=state WHERE false",
            "INSERT INTO insight_platform.artifacts (tenant_id) SELECT tenant_id FROM insight_platform.tenants WHERE false",
            "INSERT INTO insight_platform.artifact_blobs (tenant_id) SELECT tenant_id FROM insight_platform.tenants WHERE false",
            "DELETE FROM insight_platform.artifacts WHERE false",
            "DELETE FROM insight_platform.artifact_blobs WHERE false",
        ] {
            let error = sqlx::query(sql).execute(&self.pool).await.unwrap_err();
            assert_eq!(
                error.as_database_error().unwrap().code().as_deref(),
                Some("42501"),
                "{sql}"
            );
        }
    }

    pub async fn close(self) {
        self.pool.close().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "DROP OWNED BY {}; DROP ROLE {}",
            self.name, self.name
        )))
        .execute(&self.owner)
        .await
        .unwrap();
    }
}
