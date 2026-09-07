//! Independently credentialed, bounded PostgreSQL-to-JetStream delivery process.
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, ClaimDueCommittedEvents, JsonLimits, ResourceId,
    ResourceKind, Sha256Digest,
};
use insight_platform_deployment_contracts::outbox::OutboxWorkerConfigV1;
use insight_platform_observability::{ProcessHttpMetrics, PROCESS_OBSERVABILITY_OPERATIONS};
use insight_platform_observability_http::process_observability_router;
use insight_platform_outbox_worker::JetStreamCommittedEventPublisher;
use insight_platform_postgres::{repository::PgRepository, verify_schema};
use insight_platform_worker::outbox::drain_committed_events;
use sqlx::postgres::PgPoolOptions;
use std::{io::Read as _, path::PathBuf, sync::Arc, time::Duration};
use uuid::Uuid;

fn required(name: &str) -> Result<String, &'static str> {
    std::env::var(name)
        .ok()
        .filter(|s| !s.is_empty())
        .ok_or("required configuration missing")
}
fn path(name: &str) -> Result<PathBuf, &'static str> {
    let path = PathBuf::from(required(name)?);
    if !path.is_absolute() {
        return Err("configuration path must be absolute");
    }
    Ok(path)
}
fn load() -> Result<OutboxWorkerConfigV1, &'static str> {
    let mut bytes = Vec::new();
    std::fs::File::open(path("PLATFORM_OUTBOX_CONFIG")?)
        .map_err(|_| "configuration unreadable")?
        .take(65_537)
        .read_to_end(&mut bytes)
        .map_err(|_| "configuration unreadable")?;
    let value = parse_strict_json(
        &bytes,
        JsonLimits {
            max_bytes: 65_536,
            max_depth: 8,
            max_items_per_array: 8,
            max_properties_per_object: 32,
            max_string_bytes: 2048,
        },
    )
    .map_err(|_| "configuration invalid")?;
    let expected: Sha256Digest = required("PLATFORM_OUTBOX_CONFIG_DIGEST")?
        .parse()
        .map_err(|_| "configuration digest invalid")?;
    if canonical_digest(&value).map_err(|_| "configuration invalid")? != expected.to_string() {
        return Err("configuration digest mismatch");
    }
    let config: OutboxWorkerConfigV1 =
        serde_json::from_value(value).map_err(|_| "configuration invalid")?;
    config.validate().map_err(|_| "configuration invalid")?;
    Ok(config)
}
#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("platform-outbox-worker failed: {error}");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), &'static str> {
    let config = load()?;
    let pool = PgPoolOptions::new()
        .max_connections(config.database_max_connections)
        .acquire_timeout(Duration::from_millis(
            config.database_acquire_timeout_milliseconds,
        ))
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET statement_timeout = '5s'")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("SET lock_timeout = '1s'")
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            })
        })
        .connect(&required("PLATFORM_OUTBOX_DATABASE_URL")?)
        .await
        .map_err(|_| "database unavailable")?;
    verify_schema(&pool).await.map_err(|_| "schema mismatch")?;
    let repository = PgRepository::new(pool);
    let servers = config
        .nats_servers
        .iter()
        .map(|s| s.parse::<async_nats::ServerAddr>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| "NATS endpoint invalid")?;
    let client = async_nats::ConnectOptions::new()
        .name("insight-platform-outbox-worker-v1")
        .require_tls(true)
        .add_root_certificates(path("PLATFORM_OUTBOX_NATS_CA_PATH")?)
        .add_client_certificate(
            path("PLATFORM_OUTBOX_NATS_CERT_PATH")?,
            path("PLATFORM_OUTBOX_NATS_KEY_PATH")?,
        )
        .custom_inbox_prefix("_INBOX.insight.outbox")
        .connection_timeout(Duration::from_millis(
            config.nats_connect_timeout_milliseconds,
        ))
        .client_capacity(config.maximum_pending_messages)
        .max_reconnects(None)
        .connect(servers)
        .await
        .map_err(|_| "NATS unavailable")?;
    let publisher = JetStreamCommittedEventPublisher::from_client(
        client.clone(),
        &config.stream,
        Duration::from_millis(config.nats_publish_timeout_milliseconds),
    )
    .await
    .map_err(|_| "installed JetStream contract unavailable")?;
    let metrics = Arc::new(
        ProcessHttpMetrics::install("outbox-worker", PROCESS_OBSERVABILITY_OPERATIONS)
            .map_err(|_| "observability invalid")?,
    );
    let listener = tokio::net::TcpListener::bind(&config.observability_listen_address)
        .await
        .map_err(|_| "observability unavailable")?;
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let router = process_observability_router(Arc::clone(&metrics));
    let mut server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    metrics.mark_ready();
    let process_generation =
        ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, Uuid::now_v7())
            .map_err(|_| "process identity invalid")?;
    let permits = tokio::sync::Semaphore::new(usize::from(config.claim_batch));
    let mut delay = config.retry_base_milliseconds;
    let mut interval =
        tokio::time::interval(Duration::from_millis(config.poll_interval_milliseconds));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => { let _ = stop.send(()); server.await.map_err(|_| "observability task failed")?.map_err(|_| "observability task failed")?; return Ok(()); },
            _ = &mut server => return Err("observability task stopped"),
            _ = interval.tick() => {
                // Readiness must remain truthful while idle, including loss or replacement of the
                // durable stream. INFO uses the publisher's same narrowly allowed subject.
                if JetStreamCommittedEventPublisher::from_client(client.clone(), &config.stream,
                    Duration::from_millis(config.nats_publish_timeout_milliseconds)).await.is_err() {
                    metrics.mark_not_ready();
                    continue;
                }
                let capacity = permits.acquire_many(u32::from(config.claim_batch)).await.map_err(|_| "capacity closed")?;
                let result = drain_committed_events(&repository, &publisher, ClaimDueCommittedEvents {
                    process_generation: process_generation.clone(), maximum_claims: config.claim_batch, lease_milliseconds: config.lease_milliseconds,
                }, delay).await;
                drop(capacity);
                match result {
                    Ok(report) if report.retry == 0 && report.incompatible == 0 => { delay = config.retry_base_milliseconds; metrics.mark_ready(); },
                    Ok(_) | Err(_) => { metrics.mark_not_ready(); delay = delay.saturating_mul(2).min(config.retry_maximum_milliseconds); eprintln!("Outbox delivery dependency unavailable; obligations retained"); }
                }
            }
        }
    }
}
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! { _ = terminate.recv() => {}, _ = tokio::signal::ctrl_c() => {} }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}
