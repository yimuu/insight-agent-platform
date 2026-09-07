use chrono::Utc;
use insight_platform_contracts::{
    canonical_digest, ClaimDueCommittedEvents, CommittedEventNoticeV1, OutboxSettlement,
    ResourceId, ResourceKind, TenantConfig, TraceIdentityV1, UtcTimestamp,
};
use insight_platform_deployment_contracts::outbox::{
    OutboxJetStreamContractV1, COMMITTED_EVENT_STREAM,
};
use insight_platform_outbox_worker::{stream_configuration, JetStreamCommittedEventPublisher};
use insight_platform_postgres::{
    repository::{NewTenant, PgRepository},
    verify_schema,
};
use insight_platform_worker::outbox::{CommittedEventPublisher, OutboxDeliveryStore};
use sqlx::{postgres::PgPoolOptions, Row};
use std::time::Duration;
use uuid::Uuid;

fn environment(name: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| panic!("{name} is required for the real Outbox integration fixture"))
}
fn id(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
}
fn stream_contract() -> OutboxJetStreamContractV1 {
    OutboxJetStreamContractV1 {
        schema_version: 1,
        maximum_messages: 1,
        maximum_bytes: 65536,
        replicas: 1,
        maximum_age_seconds: 0,
        duplicate_window_seconds: 120,
    }
}

#[tokio::test]
async fn real_jetstream_replay_capacity_and_expired_postgres_fence() {
    let (database_url, nats_url) = (
        environment("PLATFORM_OUTBOX_TEST_DATABASE_URL"),
        environment("PLATFORM_OUTBOX_TEST_NATS_URL"),
    );
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&database_url)
        .await
        .unwrap();
    verify_schema(&pool).await.unwrap();
    let administrator = PgRepository::new(pool.clone());
    let tenant = id(ResourceKind::Tenant);
    administrator
        .create_tenant(NewTenant {
            tenant_id: tenant.to_string(),
            state: "active".to_owned(),
            config: TenantConfig::default(),
        })
        .await
        .unwrap();
    let role = environment("PLATFORM_OUTBOX_TEST_ROLE");
    assert!(
        !role.is_empty()
            && role.len() <= 63
            && role
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    );
    let role_pool = PgPoolOptions::new()
        .max_connections(4)
        .after_connect(move |connection, _| {
            let role = role.clone();
            Box::pin(async move {
                // The bounded identifier above is the only interpolated value; fixtures use NOLOGIN
                // roles so that the connection and every subsequent statement run as the real role.
                sqlx::query(sqlx::AssertSqlSafe(format!("SET ROLE \"{role}\"")))
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect(&database_url)
        .await
        .unwrap();
    verify_schema(&role_pool).await.unwrap();
    for forbidden in [
        "SELECT payload FROM insight_platform.events LIMIT 0",
        "SELECT payload_digest FROM insight_platform.events LIMIT 0",
        "SELECT * FROM insight_platform.secret_bindings LIMIT 0",
        "SELECT * FROM insight_platform.jobs LIMIT 0",
        "DELETE FROM insight_platform.outbox_events WHERE FALSE",
        "UPDATE insight_platform.outbox_events SET event_id=event_id WHERE FALSE",
    ] {
        let error = sqlx::query(forbidden)
            .execute(&role_pool)
            .await
            .unwrap_err();
        assert!(
            matches!(error, sqlx::Error::Database(ref e) if e.code().as_deref() == Some("42501")),
            "unexpected permission result: {error}"
        );
    }
    let repository = PgRepository::new(role_pool);
    let client = async_nats::connect(nats_url).await.unwrap();
    let context = async_nats::jetstream::new(client.clone());
    // This fixture requires a dedicated JetStream server; never attach it to a user stream.
    let mut invalid = stream_configuration(&stream_contract()).unwrap();
    invalid.storage = async_nats::jetstream::stream::StorageType::Memory;
    context.create_stream(invalid).await.unwrap();
    assert!(JetStreamCommittedEventPublisher::from_client(
        client.clone(),
        &stream_contract(),
        Duration::from_secs(1)
    )
    .await
    .is_err());
    context.delete_stream(COMMITTED_EVENT_STREAM).await.unwrap();
    let mut stream = context
        .create_stream(stream_configuration(&stream_contract()).unwrap())
        .await
        .unwrap();
    let publisher = JetStreamCommittedEventPublisher::from_client(
        client,
        &stream_contract(),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    for version in 1_i64..=2 {
        let event = id(ResourceKind::Event);
        let outbox = id(ResourceKind::OutboxEvent);
        let trace = TraceIdentityV1::generate().trace_id;
        let payload =
            serde_json::json!({"credential": "private-event-body-must-never-be-published"});
        sqlx::query("INSERT INTO insight_platform.events (tenant_id,event_id,aggregate_kind,aggregate_id,aggregate_version,trace_id,event_type,visibility,payload_schema_version,payload,payload_digest) VALUES ($1,$2,'tenant',$1,$6,$3,'tenant.test','internal',1,$4,$5)")
            .bind(tenant.to_string()).bind(event.to_string()).bind(trace.to_string()).bind(&payload).bind(canonical_digest(&payload).unwrap()).bind(version).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO insight_platform.outbox_events (tenant_id,outbox_id,event_id,trace_id) VALUES ($1,$2,$3,$4)")
            .bind(tenant.to_string()).bind(outbox.to_string()).bind(event.to_string()).bind(trace.to_string()).execute(&pool).await.unwrap();
    }
    let command = |generation| ClaimDueCommittedEvents {
        process_generation: generation,
        maximum_claims: 1,
        lease_milliseconds: 30000,
    };
    let old = repository
        .claim_due_committed_events(command(id(ResourceKind::WorkerProcessGeneration)))
        .await
        .unwrap()
        .pop()
        .unwrap();
    publisher.publish(&old.notice).await.unwrap();
    assert_eq!(stream.info().await.unwrap().state.messages, 1);
    // Crash after the durable ACK: publication isn't committed in PostgreSQL. Expire and reclaim.
    sqlx::query("UPDATE insight_platform.outbox_events SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND outbox_id=$2")
        .bind(tenant.to_string()).bind(old.fence.outbox_id.to_string()).execute(&pool).await.unwrap();
    let newer = repository
        .claim_due_committed_events(command(id(ResourceKind::WorkerProcessGeneration)))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(newer.notice.event_id, old.notice.event_id);
    assert!(newer.fence.epoch > old.fence.epoch);
    assert!(!repository
        .settle_committed_event(&old.fence, OutboxSettlement::Published)
        .await
        .unwrap());
    publisher.publish(&newer.notice).await.unwrap();
    assert_eq!(
        stream.info().await.unwrap().state.messages,
        1,
        "stable Event identity deduplicates the durable retry"
    );
    assert!(repository
        .settle_committed_event(&newer.fence, OutboxSettlement::Published)
        .await
        .unwrap());
    let remaining = repository
        .claim_due_committed_events(command(id(ResourceKind::WorkerProcessGeneration)))
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert!(
        publisher.publish(&remaining.notice).await.is_err(),
        "DiscardNew must refuse a full stream"
    );
    assert!(repository
        .settle_committed_event(&remaining.fence, OutboxSettlement::Incompatible)
        .await
        .unwrap());
    let water = repository.observe_outbox_backlog(1).await.unwrap();
    assert_eq!(water.observed_undelivered, 1);
    assert!(water.limit_reached);
    let observation = insight_platform_postgres::operational_metrics::observe_durable_outbox(&pool)
        .await
        .unwrap();
    assert_eq!(observation.incompatible_events, 1);
    assert_eq!(observation.due_events, 0);
    let row = sqlx::query("SELECT published_at FROM insight_platform.outbox_events WHERE tenant_id=$1 AND outbox_id=$2")
        .bind(tenant.to_string()).bind(remaining.fence.outbox_id.to_string()).fetch_one(&pool).await.unwrap();
    assert!(row
        .get::<Option<chrono::DateTime<Utc>>, _>("published_at")
        .is_none());
    let raw = stream.get_raw_message(1).await.unwrap();
    let encoded = String::from_utf8(raw.payload.to_vec()).unwrap();
    assert!(!encoded.contains("credential") && !encoded.contains("private-event-body"));
    let decoded: CommittedEventNoticeV1 = serde_json::from_str(&encoded).unwrap();
    decoded.validate().unwrap();
    // Keep the accepted message for the external restart qualification; no automatic stream purge.
    assert_eq!(
        stream.info().await.unwrap().config.name,
        COMMITTED_EVENT_STREAM
    );
}

#[test]
fn accepted_projection_stays_bounded_without_source_payload() {
    let notice = CommittedEventNoticeV1 {
        schema_version: 1,
        tenant_id: id(ResourceKind::Tenant),
        event_id: id(ResourceKind::Event),
        aggregate_id: id(ResourceKind::Job),
        aggregate_version: Some(1),
        run_id: None,
        public_sequence: None,
        trace_id: TraceIdentityV1::generate().trace_id,
        occurred_at: UtcTimestamp::from_datetime(Utc::now()),
    };
    assert!(
        serde_json::to_vec(&notice).unwrap().len()
            < insight_platform_contracts::MAX_COMMITTED_EVENT_NOTICE_BYTES
    );
}

#[tokio::test]
async fn real_mtls_roles_cannot_expand_subject_or_stream_permissions() {
    let (url, directory) = (
        environment("PLATFORM_OUTBOX_TLS_TEST_NATS_URL"),
        environment("PLATFORM_OUTBOX_TLS_TEST_DIRECTORY"),
    );
    let directory = std::path::PathBuf::from(directory);
    let connect = |identity: &'static str, prefix: &'static str| {
        async_nats::ConnectOptions::new()
            .require_tls(true)
            .add_root_certificates(directory.join("ca.pem"))
            .add_client_certificate(
                directory.join(format!("{identity}.pem")),
                directory.join(format!("{identity}-key.pem")),
            )
            .custom_inbox_prefix(prefix)
            .connection_timeout(Duration::from_secs(2))
            .connect(url.clone())
    };
    let provisioner = connect("provision", "_INBOX.fixture.provision")
        .await
        .unwrap();
    let context = async_nats::jetstream::new(provisioner);
    context
        .create_stream(stream_configuration(&stream_contract()).unwrap())
        .await
        .unwrap();
    let worker = connect("publisher", "_INBOX.insight.outbox").await.unwrap();
    let publisher = JetStreamCommittedEventPublisher::from_client(
        worker.clone(),
        &stream_contract(),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    let notice = CommittedEventNoticeV1 {
        schema_version: 1,
        tenant_id: id(ResourceKind::Tenant),
        event_id: id(ResourceKind::Event),
        aggregate_id: id(ResourceKind::Job),
        aggregate_version: Some(1),
        run_id: None,
        public_sequence: None,
        trace_id: TraceIdentityV1::generate().trace_id,
        occurred_at: UtcTimestamp::from_datetime(Utc::now()),
    };
    publisher.publish(&notice).await.unwrap();
    let restricted = async_nats::jetstream::new(worker);
    let deletion = tokio::time::timeout(
        Duration::from_secs(1),
        restricted.delete_stream(COMMITTED_EVENT_STREAM),
    )
    .await;
    assert!(
        !matches!(deletion, Ok(Ok(_))),
        "publisher cannot delete the stream"
    );
    let other_subject = tokio::time::timeout(Duration::from_secs(1), async {
        let ack = restricted
            .publish("insight.platform.v1.unapproved", "{}".into())
            .await
            .map_err(|_| ())?;
        ack.await.map_err(|_| ())
    })
    .await;
    assert!(
        !matches!(other_subject, Ok(Ok(_))),
        "publisher cannot expand the subject ACL"
    );
    let live = connect("live", "_INBOX.fixture.live").await.unwrap();
    assert!(
        JetStreamCommittedEventPublisher::from_client(
            live,
            &stream_contract(),
            Duration::from_secs(1)
        )
        .await
        .is_err(),
        "live hints identity cannot use durable publication privileges"
    );
    let missing_certificate = async_nats::ConnectOptions::new()
        .require_tls(true)
        .add_root_certificates(directory.join("ca.pem"))
        .connection_timeout(Duration::from_secs(1))
        .connect(url)
        .await;
    assert!(
        missing_certificate.is_err(),
        "NATS requires a mapped client certificate"
    );
    // The least-privilege provisioner may inspect the exact stream but cannot deliver or read bodies.
    assert_eq!(
        context
            .get_stream(COMMITTED_EVENT_STREAM)
            .await
            .unwrap()
            .cached_info()
            .state
            .messages,
        1
    );
}

#[tokio::test]
#[ignore = "run explicitly after restarting the dedicated JetStream fixture"]
async fn accepted_notice_survives_jetstream_process_restart() {
    let url = environment("PLATFORM_OUTBOX_TEST_NATS_URL");
    let client = async_nats::connect(url).await.unwrap();
    JetStreamCommittedEventPublisher::from_client(
        client.clone(),
        &stream_contract(),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    let context = async_nats::jetstream::new(client);
    let stream = context.get_stream(COMMITTED_EVENT_STREAM).await.unwrap();
    assert_eq!(stream.cached_info().state.messages, 1);
    let raw = stream.get_raw_message(1).await.unwrap();
    let notice: CommittedEventNoticeV1 = serde_json::from_slice(&raw.payload).unwrap();
    notice.validate().unwrap();
}
