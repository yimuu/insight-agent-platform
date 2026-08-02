//! Fifty-Attached-Run database and terminal calibration qualification probe.
//!
//! This is a Rust-only internal harness. It holds every SSE response open,
//! samples PostgreSQL and platform metrics at peak concurrency, releases the
//! durable waits, then verifies terminal/EOF/GET/snapshot-hash convergence.

use std::{cmp, time::Duration};

use futures::{stream, StreamExt, TryStreamExt};
use insight_agent_platform::{engine::ContentHash, runtime::RUN_STREAM_PROTOCOL_VERSION};
use reqwest::{Client, Response};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use tokio::time::{self, Instant};

struct AttachedResponse {
    run_id: String,
    response: Response,
}

struct TerminalObservation {
    run_id: String,
    run: Value,
}

type DynError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let base_url = required_env("BASE_URL")?;
    let database_url = required_env("QUALIFICATION_DATABASE_URL")?;
    let agent_id = std::env::var("AGENT_ID").unwrap_or_else(|_| "benchmark_wait".to_owned());
    let run_count = env_usize("ATTACHED_RUNS", 50)?;
    let hold_seconds = env_u64("HOLD_SECONDS", 20)?;
    if !(1..=1_000).contains(&run_count) || hold_seconds == 0 {
        return Err("qualification bounds are invalid".into());
    }

    let client = Client::builder()
        .pool_max_idle_per_host(run_count)
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(120))
        .build()?;
    let database = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&database_url)
        .await?;
    let notify_before = pg_notify_calls(&database).await?;

    let attached = stream::iter(0..run_count)
        .map(|index| open_attached(&client, &base_url, &agent_id, index))
        .buffer_unordered(run_count)
        .try_collect::<Vec<_>>()
        .await?;
    if attached.len() != run_count {
        return Err("not all Attached Runs were admitted".into());
    }
    let run_ids = attached
        .iter()
        .map(|attached| attached.run_id.clone())
        .collect::<Vec<_>>();

    let metrics = fetch_text(&client, &format!("{base_url}/metrics")).await?;
    if let Ok(path) = std::env::var("PEAK_METRICS_PATH") {
        std::fs::write(path, &metrics)?;
    }
    let backend = if metrics.contains("backend=\"nats_core\"") {
        "nats_core"
    } else {
        "in_memory"
    };
    let active_subscriptions = metric_value(
        &metrics,
        &format!("run_stream_bus_active_subscriptions{{backend=\"{backend}\"}}"),
    )?;
    if active_subscriptions as usize != run_count {
        return Err(format!(
            "active subscription mismatch: expected {run_count}, observed {active_subscriptions}"
        )
        .into());
    }
    let connection_count = if backend == "nats_core" {
        metric_value(
            &metrics,
            "run_stream_bus_connections{backend=\"nats_core\",state=\"connected\"}",
        )?
    } else {
        0.0
    };
    let nats_server_connections = match std::env::var("NATS_MONITOR_URL") {
        Ok(monitor) => {
            let document = client
                .get(format!("{}/varz", monitor.trim_end_matches('/')))
                .send()
                .await?
                .error_for_status()?
                .json::<Value>()
                .await?;
            if let Ok(path) = std::env::var("PEAK_NATS_VARZ_PATH") {
                std::fs::write(path, serde_json::to_vec_pretty(&document)?)?;
            }
            let connections = document["connections"]
                .as_u64()
                .ok_or("NATS varz omitted connections")?;
            if backend == "nats_core" && connections != 1 {
                return Err(format!(
                    "expected one NATS data connection at peak, observed {connections}"
                )
                .into());
            }
            Some(connections)
        }
        Err(_) => None,
    };

    let activity = sample_postgres_activity(&database).await?;
    if activity.run_stream_listener_connections != 0 {
        return Err("legacy per-Run PostgreSQL listeners remain active".into());
    }
    if activity.runtime_connections > 10 {
        return Err(
            "50 Attached streams exceeded the configured 10-connection runtime pool".into(),
        );
    }
    let get_p95_ms = waiting_get_p95(&client, &base_url, &run_ids).await?;
    if get_p95_ms > 100.0 {
        return Err(format!("waiting Run GET p95 exceeded 100ms: {get_p95_ms:.3}ms").into());
    }

    time::sleep(Duration::from_secs(hold_seconds)).await;
    stream::iter(run_ids.iter().enumerate())
        .map(|(index, run_id)| signal_run(&client, &base_url, run_id, index))
        .buffer_unordered(run_count)
        .try_collect::<Vec<_>>()
        .await?;

    let terminals = stream::iter(attached)
        .map(|attached| consume_terminal(&client, &base_url, attached))
        .buffer_unordered(run_count)
        .try_collect::<Vec<_>>()
        .await?;
    verify_snapshots(&database, &terminals).await?;
    let notify_after = pg_notify_calls(&database).await?;

    println!(
        "{}",
        json!({
            "passed": true,
            "backend": backend,
            "attached_runs": run_count,
            "terminal_success": terminals.len(),
            "terminal_eof": terminals.len(),
            "terminal_get_consistent": terminals.len(),
            "snapshot_hashes_verified": terminals.len(),
            "active_subscriptions_at_peak": active_subscriptions as u64,
            "nats_connected_state": connection_count as u64,
            "nats_server_connections": nats_server_connections,
            "postgres": {
                "connections": activity.connections,
                "runtime_connections": activity.runtime_connections,
                "active_connections": activity.active_connections,
                "legacy_run_stream_listener_connections": activity.run_stream_listener_connections,
                "pg_notify_calls_during_profile": notify_after.saturating_sub(notify_before),
            },
            "waiting_run_get_p95_ms": get_p95_ms,
        })
    );
    Ok(())
}

async fn open_attached(
    client: &Client,
    base_url: &str,
    agent_id: &str,
    index: usize,
) -> Result<AttachedResponse, DynError> {
    let response = client
        .post(format!("{base_url}/v1/agents/{agent_id}/runs/stream"))
        .header("x-request-id", format!("nats-attached-{index}"))
        .json(&json!({}))
        .send()
        .await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|error| format!("<failed to read response body: {error}>"));
        return Err(format!("Attached admission failed with {status}: {body}").into());
    }
    let run_id = response
        .headers()
        .get("x-run-id")
        .and_then(|value| value.to_str().ok())
        .ok_or("Attached response omitted x-run-id")?
        .to_owned();
    Ok(AttachedResponse { run_id, response })
}

async fn signal_run(
    client: &Client,
    base_url: &str,
    run_id: &str,
    index: usize,
) -> Result<(), DynError> {
    let response = client
        .post(format!("{base_url}/v1/runs/{run_id}/signals/continue"))
        .json(&json!({
            "message_id": format!("nats-signal-{index}"),
            "value": format!("completed-{index}"),
        }))
        .send()
        .await?;
    if !response.status().is_success() {
        return Err(format!("signal failed with {}", response.status()).into());
    }
    Ok(())
}

async fn consume_terminal(
    client: &Client,
    base_url: &str,
    attached: AttachedResponse,
) -> Result<TerminalObservation, DynError> {
    let run_id = attached.run_id;
    let mut body = attached.response.bytes_stream();
    let bytes = time::timeout(Duration::from_secs(90), async {
        let mut bytes = Vec::new();
        while let Some(chunk) = body.next().await {
            bytes.extend_from_slice(&chunk?);
            if bytes.len() > 2 * 1_024 * 1_024 {
                return Err("Attached SSE exceeded the 2 MiB qualification bound".into());
            }
        }
        Ok::<_, DynError>(bytes)
    })
    .await
    .map_err(|_| "Attached SSE did not reach terminal EOF")??;
    let frames = parse_sse(&bytes)?;
    let terminals = frames
        .iter()
        .filter(|(event, _)| {
            matches!(
                event.as_str(),
                "run.lifecycle.completed"
                    | "run.lifecycle.failed"
                    | "run.lifecycle.timed_out"
                    | "run.lifecycle.stopped"
                    | "run.lifecycle.cancelled"
                    | "run.lifecycle.interrupted"
            )
        })
        .collect::<Vec<_>>();
    if terminals.len() != 1 || frames.last() != terminals.first().copied() {
        return Err("SSE must end with exactly one terminal frame followed by EOF".into());
    }
    if terminals[0].0 != "run.lifecycle.completed" {
        return Err("qualification Run did not complete successfully".into());
    }
    let terminal_run = terminals[0]
        .1
        .get("run")
        .cloned()
        .ok_or("terminal SSE event omitted run")?;
    let response = client
        .get(format!("{base_url}/v1/runs/{run_id}"))
        .send()
        .await?;
    if !response.status().is_success() {
        return Err(format!("GET Run failed with {}", response.status()).into());
    }
    let document = response.json::<Value>().await?;
    let durable_run = document
        .get("data")
        .cloned()
        .ok_or("GET Run response omitted data")?;
    let consistent = terminal_run.get("id") == durable_run.get("run_id")
        && terminal_run.get("status") == durable_run.get("status")
        && terminal_run.get("result") == durable_run.pointer("/output/data");
    if !consistent {
        return Err("terminal SSE snapshot is inconsistent with GET Run authority".into());
    }
    Ok(TerminalObservation {
        run_id,
        run: terminal_run,
    })
}

fn parse_sse(bytes: &[u8]) -> Result<Vec<(String, Value)>, DynError> {
    let text = std::str::from_utf8(bytes)?.replace("\r\n", "\n");
    let mut frames = Vec::new();
    for raw in text.split("\n\n") {
        let mut event = None;
        let mut data = Vec::new();
        for line in raw.lines() {
            if let Some(value) = line.strip_prefix("event:") {
                event = Some(value.trim().to_owned());
            } else if let Some(value) = line.strip_prefix("data:") {
                data.push(value.trim_start());
            }
        }
        if let (Some(event), false) = (event, data.is_empty()) {
            frames.push((event, serde_json::from_str(&data.join("\n"))?));
        }
    }
    Ok(frames)
}

async fn waiting_get_p95(
    client: &Client,
    base_url: &str,
    run_ids: &[String],
) -> Result<f64, DynError> {
    let mut samples = stream::iter(run_ids)
        .map(|run_id| async move {
            let started = Instant::now();
            let response = client
                .get(format!("{base_url}/v1/runs/{run_id}"))
                .send()
                .await?;
            if !response.status().is_success() {
                return Err(format!("waiting GET failed with {}", response.status()).into());
            }
            let _: Value = response.json().await?;
            Ok::<_, DynError>(started.elapsed().as_secs_f64() * 1_000.0)
        })
        .buffer_unordered(cmp::min(run_ids.len(), 50))
        .try_collect::<Vec<_>>()
        .await?;
    samples.sort_by(f64::total_cmp);
    let index = ((samples.len() as f64 * 0.95).ceil() as usize)
        .saturating_sub(1)
        .min(samples.len().saturating_sub(1));
    Ok(samples[index])
}

struct PostgresActivity {
    connections: i64,
    runtime_connections: i64,
    active_connections: i64,
    run_stream_listener_connections: i64,
}

async fn sample_postgres_activity(database: &PgPool) -> Result<PostgresActivity, DynError> {
    let row = sqlx::query(
        r#"SELECT COUNT(*) FILTER (WHERE datname=current_database())::bigint AS connections,
                  COUNT(*) FILTER (WHERE datname=current_database() AND application_name='insight-agent-platform-runtime')::bigint AS runtime_connections,
                  COUNT(*) FILTER (WHERE datname=current_database() AND state='active')::bigint AS active_connections,
                  COUNT(*) FILTER (WHERE datname=current_database() AND application_name='insight-agent-platform-runtime' AND query ILIKE '%insight_live_run_stream_%')::bigint AS run_stream_listener_connections
           FROM pg_stat_activity"#,
    )
    .fetch_one(database)
    .await?;
    Ok(PostgresActivity {
        connections: row.try_get("connections")?,
        runtime_connections: row.try_get("runtime_connections")?,
        active_connections: row.try_get("active_connections")?,
        run_stream_listener_connections: row.try_get("run_stream_listener_connections")?,
    })
}

async fn pg_notify_calls(database: &PgPool) -> Result<u64, DynError> {
    let value = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT SUM(calls)::bigint FROM pg_stat_statements WHERE query ILIKE '%pg_notify%'",
    )
    .fetch_one(database)
    .await?
    .unwrap_or_default();
    Ok(value.max(0) as u64)
}

async fn verify_snapshots(
    database: &PgPool,
    terminals: &[TerminalObservation],
) -> Result<(), DynError> {
    let run_ids = terminals
        .iter()
        .map(|terminal| terminal.run_id.clone())
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"SELECT run_id,terminal_kind,run_payload,public_item_manifest,snapshot_hash
           FROM run_stream_snapshots WHERE run_id = ANY($1)"#,
    )
    .bind(&run_ids)
    .fetch_all(database)
    .await?;
    if rows.len() != terminals.len() {
        return Err("terminal snapshot row count differs from Attached Run count".into());
    }
    for row in rows {
        let run_id: String = row.try_get("run_id")?;
        let terminal_kind: String = row.try_get("terminal_kind")?;
        let run: Value = row.try_get("run_payload")?;
        let manifest: Value = row.try_get("public_item_manifest")?;
        let stored_hash: String = row.try_get("snapshot_hash")?;
        let observed = terminals
            .iter()
            .find(|terminal| terminal.run_id == run_id)
            .ok_or("snapshot returned an unexpected Run")?;
        if observed.run != run {
            return Err(format!(
                "SSE terminal differs from durable run_payload: sse={}, stored={}",
                serde_json::to_string(&observed.run)?,
                serde_json::to_string(&run)?
            )
            .into());
        }
        let projection = json!({
            "protocol": RUN_STREAM_PROTOCOL_VERSION,
            "run_id": run_id,
            "terminal_kind": terminal_kind,
            "run": run,
            "public_item_manifest": manifest,
        });
        let canonical = serde_jcs::to_vec(&projection)?;
        if ContentHash::from_bytes(&canonical).as_str() != stored_hash {
            return Err(
                "stored terminal snapshot hash does not match its canonical payload".into(),
            );
        }
    }
    Ok(())
}

async fn fetch_text(client: &Client, url: &str) -> Result<String, DynError> {
    let response = client.get(url).send().await?;
    if !response.status().is_success() {
        return Err(format!("metrics request failed with {}", response.status()).into());
    }
    Ok(response.text().await?)
}

fn metric_value(metrics: &str, prefix: &str) -> Result<f64, DynError> {
    metrics
        .lines()
        .find_map(|line| {
            line.strip_prefix(prefix)
                .and_then(|tail| tail.trim().parse::<f64>().ok())
        })
        .ok_or_else(|| format!("missing metric {prefix}").into())
}

fn required_env(name: &str) -> Result<String, DynError> {
    std::env::var(name).map_err(|_| format!("{name} is required").into())
}

fn env_u64(name: &str, default: u64) -> Result<u64, DynError> {
    Ok(std::env::var(name)
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(default))
}

fn env_usize(name: &str, default: usize) -> Result<usize, DynError> {
    Ok(std::env::var(name)
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(default))
}
