//! Actual executable qualification with a dedicated login and restricted retention primitives.
use insight_platform_contracts::{canonical_digest, ResourceId, ResourceKind, TenantConfig};
use insight_platform_orchestrator::history::{
    retirement::{HistoryRecordKey, HistoryRetirementLane},
    HistoryRetentionPolicy,
};
use insight_platform_postgres::repository::{NewTenant, PgRepository, RepositoryError};
use insight_platform_worker::execution::executable_digest;
use sqlx::{postgres::PgPoolOptions, Row};
use std::{
    io::{Read as _, Write as _},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn seed_receipt_scan(pool: &sqlx::PgPool) -> (ResourceId, ResourceId, Vec<ResourceId>) {
    let fresh = |kind| ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7()).unwrap();
    let tenant = fresh(ResourceKind::Tenant);
    PgRepository::new(pool.clone())
        .create_tenant(NewTenant {
            tenant_id: tenant.to_string(),
            state: "active".into(),
            config: TenantConfig::default(),
        })
        .await
        .unwrap();
    let mut receipts: Vec<ResourceId> = (0..3).map(|_| fresh(ResourceKind::Receipt)).collect();
    receipts.sort();
    // The physical ID shape is valid, but the owning nominal type rejects this
    // unknown prefix. No constraint, schema function or permission is disabled.
    let corrupt_scope = format!("zz_{}", uuid::Uuid::now_v7());
    assert!(corrupt_scope.parse::<ResourceId>().is_err());
    let payload = serde_json::json!({"private_body": "history-corrupt-scan-canary"});
    let digest = canonical_digest(&payload).unwrap();
    for (index, receipt) in receipts.iter().enumerate() {
        let scope = if index == 0 {
            corrupt_scope.clone()
        } else {
            tenant.to_string()
        };
        let key = canonical_digest(&serde_json::json!({"receipt": receipt})).unwrap();
        sqlx::query("INSERT INTO insight_platform.receipts(tenant_id,receipt_id,receipt_kind,scope_kind,scope_id,dedupe_owner_id,operation,idempotency_key_digest,request_digest,state,payload,payload_digest,created_at,completed_at,expires_at) VALUES($1,$2,'command','tenant',$3,$1,'tenant.update',$4,$4,'succeeded',$5,$6,clock_timestamp()-interval '3 days',clock_timestamp()-interval '2 days',clock_timestamp()-interval '2 days')")
            .bind(tenant.to_string()).bind(receipt.to_string()).bind(scope).bind(key)
            .bind(&payload).bind(&digest).execute(pool).await.unwrap();
    }
    (tenant, receipts[0].clone(), receipts[1..].to_vec())
}

#[tokio::test]
async fn isolated_history_process_checks_binary_config_and_runs_with_execute_only_role() {
    let database = std::env::var("PLATFORM_TEST_HISTORY_PROCESS_DATABASE_URL").expect(
        "PLATFORM_TEST_HISTORY_PROCESS_DATABASE_URL must name the dedicated real process fixture",
    );
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database)
        .await
        .unwrap();
    insight_platform_postgres::verify_schema(&pool)
        .await
        .unwrap();
    let role = format!("history_process_{}", uuid::Uuid::new_v4().simple());
    let password = uuid::Uuid::new_v4().simple().to_string();
    let create = format!("CREATE ROLE {role} LOGIN PASSWORD '{password}'");
    sqlx::raw_sql(sqlx::AssertSqlSafe(create))
        .execute(&pool)
        .await
        .unwrap();
    let grants = insight_platform_postgres::history_repository::history_role_grants_sql()
        .replace("\\set ON_ERROR_STOP on", "")
        .replace(":'history_maintenance_role'", &format!("'{role}'"));
    sqlx::raw_sql(sqlx::AssertSqlSafe(grants))
        .execute(&pool)
        .await
        .unwrap();
    let mut url = url::Url::parse(&database).unwrap();
    url.set_username(&role).unwrap();
    url.set_password(Some(&password)).unwrap();
    let isolated = PgPoolOptions::new()
        .max_connections(2)
        .connect(url.as_str())
        .await
        .unwrap();
    let current: String = sqlx::query("SELECT current_user AS role")
        .fetch_one(&isolated)
        .await
        .unwrap()
        .get("role");
    assert_eq!(role, current);
    for sql in [
        "SELECT payload FROM insight_platform.events LIMIT 0",
        "DELETE FROM insight_platform.events WHERE FALSE",
        "UPDATE insight_platform.runs SET public_replay_floor=public_replay_floor WHERE FALSE",
    ] {
        let error = sqlx::query(sql).execute(&isolated).await.unwrap_err();
        assert!(
            matches!(error,sqlx::Error::Database(ref error) if error.code().as_deref()==Some("42501"))
        );
    }
    let directory = tempfile::tempdir().unwrap();
    let binary = std::path::Path::new(env!("CARGO_BIN_EXE_platform-history-maintenance"));
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);
    let mut config = serde_json::json!({"schema_version":1,"component_role":"history_maintenance",
        "executable_digest":executable_digest(binary).unwrap(), "observability_listen_address":address.to_string(),
        "database_max_connections":2,"database_acquire_timeout_milliseconds":1000,"poll_interval_milliseconds":1000,
        "maximum_runs":2,"maximum_events_per_run":8,"retention_policy":{"schema_version":2,
        "public_event_minimum_seconds":86400,"audit_event_minimum_seconds":86400,"receipt_minimum_seconds":86400,"published_outbox_minimum_seconds":86400,"cleanup_minimum_seconds":86400}});
    let path = directory.path().join("history.json");
    let start = |digest: String| {
        let mut command = Command::new(binary);
        command
            .env("PLATFORM_HISTORY_MAINTENANCE_CONFIG", &path)
            .env("PLATFORM_HISTORY_MAINTENANCE_CONFIG_DIGEST", digest)
            .env("PLATFORM_HISTORY_MAINTENANCE_DATABASE_URL", url.as_str())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        command
    };
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let expected = canonical_digest(&config).unwrap();
    let invalid = format!("sha256:{}", "f".repeat(64));
    assert!(
        !start(invalid.clone()).status().unwrap().success(),
        "config digest mismatch must fail before readiness"
    );
    config["executable_digest"] = invalid.into();
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    assert!(
        !start(canonical_digest(&config).unwrap())
            .status()
            .unwrap()
            .success(),
        "replacement executable must fail even with internally consistent config digest"
    );
    config["executable_digest"] = serde_json::to_value(executable_digest(binary).unwrap()).unwrap();
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let (tenant, corrupt_receipt, valid_receipts) = seed_receipt_scan(&pool).await;
    let policy: HistoryRetentionPolicy =
        serde_json::from_value(config["retention_policy"].clone()).unwrap();
    assert!(matches!(
        PgRepository::new(isolated.clone())
            .retire_history_record(
                HistoryRetirementLane::Receipt,
                HistoryRecordKey {
                    tenant_id: tenant.clone(),
                    record_id: corrupt_receipt.clone()
                },
                &policy,
            )
            .await,
        Err(RepositoryError::CorruptRow(_))
    ));
    let log_path = directory.path().join("process.log");
    let mut process = Process(
        start(expected)
            .stderr(Stdio::from(std::fs::File::create(&log_path).unwrap()))
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            process.0.try_wait().unwrap().is_none(),
            "history process exited before readiness"
        );
        if let Ok(mut connection) =
            std::net::TcpStream::connect_timeout(&address, Duration::from_millis(100))
        {
            connection
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            connection
                .write_all(b"GET /readyz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .unwrap();
            let mut response = String::new();
            connection.read_to_string(&mut response).unwrap();
            let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM insight_platform.receipts WHERE tenant_id=$1 AND receipt_id=ANY($2)")
                .bind(tenant.to_string())
                .bind(valid_receipts.iter().map(ToString::to_string).collect::<Vec<_>>())
                .fetch_one(&pool).await.unwrap();
            if response.starts_with("HTTP/1.1 200") && remaining == 0 {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "history process did not advance past the corrupt object and process the next page"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    drop(process);
    let retained: serde_json::Value = sqlx::query_scalar(
        "SELECT payload FROM insight_platform.receipts WHERE tenant_id=$1 AND receipt_id=$2",
    )
    .bind(tenant.to_string())
    .bind(corrupt_receipt.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        retained,
        serde_json::json!({"private_body": "history-corrupt-scan-canary"})
    );
    let log = std::fs::read_to_string(&log_path).unwrap();
    assert!(log.contains("invalid durable evidence; retained for repair"));
    assert!(!log.contains("history-corrupt-scan-canary"));
    isolated.close().await;
    let revoke = format!("REVOKE ALL ON ALL FUNCTIONS IN SCHEMA insight_platform FROM {role}; REVOKE ALL ON SCHEMA insight_platform FROM {role}; DROP ROLE {role}");
    sqlx::raw_sql(sqlx::AssertSqlSafe(revoke))
        .execute(&pool)
        .await
        .unwrap();
}
