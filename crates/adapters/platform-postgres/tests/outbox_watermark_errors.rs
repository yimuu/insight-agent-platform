use insight_platform_contracts::{
    canonical_digest, ResourceId, ResourceKind, TenantConfig, TraceIdentityV1,
};
use insight_platform_postgres::{
    outbox_repository::observe_outbox_backlog_in_transaction,
    repository::{NewTenant, PgRepository, RepositoryError},
    verify_schema,
};
use insight_platform_worker::outbox::{OutboxDeliveryError, OutboxDeliveryStore};
use sqlx::postgres::PgPoolOptions;

fn id(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap()
}

fn sqlstate(error: &RepositoryError) -> Option<String> {
    match error {
        RepositoryError::Database(sqlx::Error::Database(error)) => {
            error.code().map(|code| code.into_owned())
        }
        _ => None,
    }
}

#[tokio::test]
async fn watermark_preserves_postgres_abort_and_permission_identity() {
    let url = std::env::var("PLATFORM_TEST_DATABASE_URL")
        .expect("PLATFORM_TEST_DATABASE_URL is required for the real watermark fixture");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let repository = PgRepository::new(pool.clone());
    let tenant = id(ResourceKind::Tenant);
    repository
        .create_tenant(NewTenant {
            tenant_id: tenant.to_string(),
            state: "active".into(),
            config: TenantConfig::default(),
        })
        .await
        .unwrap();

    let mut forbidden = pool.begin().await.unwrap();
    sqlx::query("SET LOCAL ROLE pg_read_all_stats")
        .execute(&mut *forbidden)
        .await
        .unwrap();
    let error = observe_outbox_backlog_in_transaction(&mut forbidden, 1)
        .await
        .unwrap_err();
    assert_eq!(sqlstate(&error).as_deref(), Some("42501"));
    forbidden.rollback().await.unwrap();

    let restricted = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET ROLE pg_read_all_stats")
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect(&url)
        .await
        .unwrap();
    assert_eq!(
        PgRepository::new(restricted)
            .observe_outbox_backlog(1)
            .await,
        Err(OutboxDeliveryError::Unavailable),
        "the external port does not expose database diagnostics"
    );
    assert_eq!(
        repository.observe_outbox_backlog(0).await,
        Err(OutboxDeliveryError::InvalidCommand)
    );

    let before: i64 =
        sqlx::query_scalar("SELECT version FROM insight_platform.tenants WHERE tenant_id=$1")
            .bind(tenant.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
    let mut first = pool.begin().await.unwrap();
    sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .execute(&mut *first)
        .await
        .unwrap();
    sqlx::query("UPDATE insight_platform.tenants SET version=version+1 WHERE tenant_id=$1")
        .bind(tenant.to_string())
        .execute(&mut *first)
        .await
        .unwrap();

    // T2 reads the pre-T1 tenant version, then commits a new Outbox obligation. T1's
    // subsequent watermark read closes a real SSI cycle; no synthetic error is injected.
    let mut second = pool.begin().await.unwrap();
    sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
        .execute(&mut *second)
        .await
        .unwrap();
    let observed: i64 =
        sqlx::query_scalar("SELECT version FROM insight_platform.tenants WHERE tenant_id=$1")
            .bind(tenant.to_string())
            .fetch_one(&mut *second)
            .await
            .unwrap();
    assert_eq!(observed, before);
    let event = id(ResourceKind::Event);
    let outbox = id(ResourceKind::OutboxEvent);
    let trace = TraceIdentityV1::generate().trace_id;
    let payload = serde_json::json!({"schema_version":1});
    sqlx::query("INSERT INTO insight_platform.events (tenant_id,event_id,aggregate_kind,aggregate_id,aggregate_version,trace_id,event_type,visibility,payload_schema_version,payload,payload_digest) VALUES ($1,$2,'tenant',$1,1,$3,'tenant.watermark_fixture','internal',1,$4,$5)")
        .bind(tenant.to_string()).bind(event.to_string()).bind(trace.to_string())
        .bind(&payload).bind(canonical_digest(&payload).unwrap())
        .execute(&mut *second).await.unwrap();
    sqlx::query("INSERT INTO insight_platform.outbox_events (tenant_id,outbox_id,event_id,trace_id) VALUES ($1,$2,$3,$4)")
        .bind(tenant.to_string()).bind(outbox.to_string()).bind(event.to_string()).bind(trace.to_string())
        .execute(&mut *second).await.unwrap();
    second.commit().await.unwrap();
    let error = observe_outbox_backlog_in_transaction(&mut first, 1_000_000)
        .await
        .unwrap_err();
    assert_eq!(sqlstate(&error).as_deref(), Some("40001"));
    first.rollback().await.unwrap();
    let after: i64 =
        sqlx::query_scalar("SELECT version FROM insight_platform.tenants WHERE tenant_id=$1")
            .bind(tenant.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        after, before,
        "aborted admission did not commit any mutation"
    );
    assert!(
        repository
            .observe_outbox_backlog(1)
            .await
            .unwrap()
            .limit_reached
    );

    // Remove only this test's committed diagnostic obligation; other fixtures remain intact.
    let mut cleanup = pool.begin().await.unwrap();
    sqlx::query("DELETE FROM insight_platform.outbox_events WHERE tenant_id=$1 AND outbox_id=$2")
        .bind(tenant.to_string())
        .bind(outbox.to_string())
        .execute(&mut *cleanup)
        .await
        .unwrap();
    sqlx::query("DELETE FROM insight_platform.events WHERE tenant_id=$1 AND event_id=$2")
        .bind(tenant.to_string())
        .bind(event.to_string())
        .execute(&mut *cleanup)
        .await
        .unwrap();
    cleanup.commit().await.unwrap();
}
