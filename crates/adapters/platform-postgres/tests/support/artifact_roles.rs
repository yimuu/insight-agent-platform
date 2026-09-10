//! Exact owning process grants on private fixture-only NOLOGIN identities.
use sqlx::{postgres::PgPoolOptions, PgPool};

pub struct ArtifactRoles {
    owner: PgPool,
    roles: [String; 5],
    pub pools: Vec<PgPool>,
}

impl ArtifactRoles {
    pub async fn create(pool: &PgPool) -> Self {
        let nonce = uuid::Uuid::now_v7().simple().to_string();
        let roles = ["gateway", "reader", "worker", "maintenance", "public"]
            .map(|purpose| format!("art_pipeline_{purpose}_{nonce}"));
        for role in &roles {
            sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                "CREATE ROLE {role} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT"
            )))
            .execute(pool)
            .await
            .unwrap();
        }
        let mut grants = insight_platform_postgres::artifact_repository::artifact_role_grants_sql()
            .replace("\\set ON_ERROR_STOP on", "");
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
            .execute(pool)
            .await
            .unwrap();
        // Only schema USAGE: this identity exercises PUBLIC's inherited function privileges.
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "GRANT USAGE ON SCHEMA insight_platform TO {}",
            roles[4]
        )))
        .execute(pool)
        .await
        .unwrap();
        let mut pools = Vec::new();
        for role in &roles {
            let role = role.clone();
            let restricted = PgPoolOptions::new()
                .max_connections(2)
                .after_connect(move |conn, _| {
                    let role = role.clone();
                    Box::pin(async move {
                        sqlx::query(sqlx::AssertSqlSafe(format!("SET ROLE {role}")))
                            .execute(conn)
                            .await?;
                        Ok(())
                    })
                })
                .connect_with((*pool.connect_options()).clone())
                .await
                .unwrap();
            pools.push(restricted);
        }
        Self {
            owner: pool.clone(),
            roles,
            pools,
        }
    }

    pub async fn close(self) {
        for pool in self.pools {
            pool.close().await;
        }
        for role in self.roles {
            sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                "DROP OWNED BY {role}; DROP ROLE {role}"
            )))
            .execute(&self.owner)
            .await
            .unwrap();
        }
    }
}
