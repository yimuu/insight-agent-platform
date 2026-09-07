//! Isolated maintenance process: safe Run metadata and narrow PostgreSQL retention primitives.
use insight_platform_contracts::{canonical_digest, parse_strict_json, Sha256Digest};
use insight_platform_deployment_contracts::history::{
    HistoryMaintenanceConfigV1, HISTORY_MAINTENANCE_CONFIG_JSON_LIMITS,
};
use insight_platform_observability::{ProcessHttpMetrics, PROCESS_OBSERVABILITY_OPERATIONS};
use insight_platform_observability_http::process_observability_router;
use insight_platform_orchestrator::history::{
    retirement::{HistoryRetirementCursor, HistoryRetirementLane, ScanHistoryRetirement},
    HistoryRetentionCursor, PurgePublicRunEventPrefix, ScanHistoryRetentionRuns,
};
use insight_platform_postgres::{
    repository::{PgRepository, RepositoryError},
    verify_schema,
};
use insight_platform_worker::execution::executable_digest;
use sqlx::postgres::PgPoolOptions;
use std::{io::Read as _, path::PathBuf, sync::Arc, time::Duration};

fn required(name: &str) -> Result<String, &'static str> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or("required history maintenance configuration missing")
}
fn load() -> Result<HistoryMaintenanceConfigV1, &'static str> {
    let path = PathBuf::from(required("PLATFORM_HISTORY_MAINTENANCE_CONFIG")?);
    if !path.is_absolute() {
        return Err("history config path must be absolute");
    }
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|_| "history config unreadable")?
        .take(HISTORY_MAINTENANCE_CONFIG_JSON_LIMITS.max_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "history config unreadable")?;
    let value = parse_strict_json(&bytes, HISTORY_MAINTENANCE_CONFIG_JSON_LIMITS)
        .map_err(|_| "history config invalid")?;
    let expected: Sha256Digest = required("PLATFORM_HISTORY_MAINTENANCE_CONFIG_DIGEST")?
        .parse()
        .map_err(|_| "history config digest invalid")?;
    if canonical_digest(&value).map_err(|_| "history config invalid")? != expected.to_string() {
        return Err("history config digest mismatch");
    }
    let config: HistoryMaintenanceConfigV1 =
        serde_json::from_value(value).map_err(|_| "history config invalid")?;
    config.validate()?;
    if executable_digest(&std::env::current_exe().map_err(|_| "history executable unavailable")?)
        .map_err(|_| "history executable unreadable")?
        != config.executable_digest
    {
        return Err("history executable differs from deployment configuration");
    }
    Ok(config)
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("platform-history-maintenance failed: {error}");
        std::process::exit(1);
    }
}

#[derive(Clone, Default)]
struct MaintenanceCursors {
    runs: Option<HistoryRetentionCursor>,
    records: [Option<HistoryRetirementCursor>; 3],
}
fn retained_candidate(error: &RepositoryError) -> bool {
    match error {
        RepositoryError::CorruptRow(_) => {
            eprintln!("History candidate has invalid durable evidence; retained for repair");
            true
        }
        RepositoryError::Database(sqlx::Error::Database(error))
            if error.code().as_deref() == Some("55P03") =>
        {
            true
        }
        _ => false,
    }
}
async fn cycle(
    repository: &PgRepository,
    config: &HistoryMaintenanceConfigV1,
    mut cursors: MaintenanceCursors,
) -> Result<MaintenanceCursors, &'static str> {
    let page = repository
        .scan_history_retention_runs(ScanHistoryRetentionRuns {
            cursor: cursors.runs.clone(),
            maximum_runs: config.maximum_runs,
        })
        .await
        .map_err(|_| "history scan unavailable")?;
    for candidate in page.candidates {
        if let Err(error) = repository
            .purge_public_run_event_prefix(
                PurgePublicRunEventPrefix {
                    tenant_id: candidate.tenant_id,
                    run_id: candidate.run_id,
                    through_sequence: candidate.through_sequence,
                    maximum_events: config.maximum_events_per_run,
                },
                &config.retention_policy,
            )
            .await
        {
            if !retained_candidate(&error) {
                return Err("history retention unavailable");
            }
        }
    }
    cursors.runs = page.next_cursor;
    for (index, lane) in HistoryRetirementLane::ALL.into_iter().enumerate() {
        let page = repository
            .scan_history_retirement(ScanHistoryRetirement {
                lane,
                cursor: cursors.records[index].clone(),
                maximum_records: config.maximum_runs,
            })
            .await
            .map_err(|_| "history retirement scan unavailable")?;
        for candidate in page.candidates {
            if let Err(error) = repository
                .retire_history_record(lane, candidate, &config.retention_policy)
                .await
            {
                if !retained_candidate(&error) {
                    return Err("history retirement unavailable");
                }
            }
        }
        cursors.records[index] = page.next_cursor;
    }
    // Every lane advances across retained or corrupt candidates in its finite
    // cohort. A transient database failure retries idempotent completed work.
    Ok(cursors)
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
        .connect(&required("PLATFORM_HISTORY_MAINTENANCE_DATABASE_URL")?)
        .await
        .map_err(|_| "history database unavailable")?;
    verify_schema(&pool)
        .await
        .map_err(|_| "history schema mismatch")?;
    let repository = PgRepository::new(pool);
    let metrics = Arc::new(
        ProcessHttpMetrics::install("history-maintenance", PROCESS_OBSERVABILITY_OPERATIONS)
            .map_err(|_| "history observability invalid")?,
    );
    let listener = tokio::net::TcpListener::bind(&config.observability_listen_address)
        .await
        .map_err(|_| "history observability unavailable")?;
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let router = process_observability_router(metrics.clone());
    let mut server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let mut interval =
        tokio::time::interval(Duration::from_millis(config.poll_interval_milliseconds));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut cursor = MaintenanceCursors::default();
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            _ = &mut server => return Err("history observability stopped"),
            _ = interval.tick() => {
                let result = tokio::select! { _ = &mut shutdown => break, result = cycle(&repository, &config, cursor.clone()) => result };
                match result { Ok(next) => { cursor = next; metrics.mark_ready(); }, Err(_) => { metrics.mark_not_ready(); eprintln!("History maintenance dependency unavailable; unprocessed obligations retained"); } }
            }
        }
    }
    metrics.mark_not_ready();
    let _ = stop.send(());
    server
        .await
        .map_err(|_| "history observability stopped")?
        .map_err(|_| "history observability stopped")?;
    Ok(())
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
