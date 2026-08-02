//! Long-running Core NATS Run Stream qualification driver.
//!
//! This is intentionally an example binary rather than an ordinary test: the
//! release gate runs it for two hours and captures every JSON sample as raw
//! qualification evidence.

use std::{path::PathBuf, process::Command, time::Duration};

use insight_engine::{ActivationId, AttemptNo, RunId};
use insight_runtime::{
    LiveRunObservationIdentity, LiveRunStreamBroker, LiveRunStreamDelivery,
    LiveRunStreamItemIdentity, LiveRunStreamPayload, LiveRunStreamPublication,
    LiveRunStreamPublishOutcome, LiveRunStreamSeal, LiveRunStreamSealStatus,
    LiveRunStreamSubscriber, NatsCoreLiveRunStreamBroker, NatsCoreLiveRunStreamBrokerOptions,
    NatsCoreTlsOptions,
};
use serde_json::json;
use tokio::time::{self, Instant};

struct RunFixture {
    run_id: RunId,
    output: LiveRunStreamItemIdentity,
    tool: LiveRunObservationIdentity,
    retrieval: LiveRunObservationIdentity,
    output_sequence: u64,
    tool_sequence: u64,
    retrieval_sequence: u64,
}

#[derive(Default)]
struct DeliveryCounts {
    publication: u64,
    gap: u64,
    seal: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let duration = env_u64("SOAK_DURATION_SECONDS", 7_200)?;
    let run_count = env_usize("SOAK_RUNS", 50)?;
    let tick_millis = env_u64("SOAK_TICK_MILLIS", 1_000)?;
    if !(1..=1_000).contains(&run_count) || tick_millis == 0 || duration < 10 {
        return Err("invalid soak bounds".into());
    }
    let options = options_from_environment()?;
    let publisher = NatsCoreLiveRunStreamBroker::connect(options.clone()).await?;
    let mut subscriber = NatsCoreLiveRunStreamBroker::connect(options.clone()).await?;
    let mut fixtures = build_fixtures(run_count)?;
    let mut subscriptions = subscribe_all(&subscriber, &fixtures).await?;
    let started = Instant::now();
    let deadline = started + Duration::from_secs(duration);
    let restart_at = started + Duration::from_secs(duration / 2);
    let slow_start = started + Duration::from_secs(duration / 3);
    let slow_end = slow_start + Duration::from_secs(30.min(duration / 6));
    let mut restarted = false;
    let mut counts = DeliveryCounts::default();
    let mut lost_publications = 0_u64;
    let mut ticks = 0_u64;
    let mut next_sample = started;

    while Instant::now() < deadline {
        ticks = ticks.saturating_add(1);
        for fixture in &mut fixtures {
            let publication = match ticks % 3 {
                0 => {
                    let sequence = fixture.output_sequence;
                    fixture.output_sequence = fixture.output_sequence.saturating_add(1);
                    LiveRunStreamPublication::new(
                        fixture.output.clone(),
                        sequence,
                        LiveRunStreamPayload::OutputTextDelta {
                            content_index: 0,
                            delta: format!("soak-{ticks}"),
                        },
                    )?
                }
                1 => {
                    let sequence = fixture.tool_sequence;
                    fixture.tool_sequence = fixture.tool_sequence.saturating_add(1);
                    LiveRunStreamPublication::new_run_observation(
                        fixture.tool.clone(),
                        sequence,
                        LiveRunStreamPayload::ToolStarted {
                            call_id: "soak_call".to_owned(),
                            tool_name: "soak_tool".to_owned(),
                            arguments: Some(json!({"tick": ticks})),
                        },
                    )?
                }
                _ => {
                    let sequence = fixture.retrieval_sequence;
                    fixture.retrieval_sequence = fixture.retrieval_sequence.saturating_add(1);
                    LiveRunStreamPublication::new_run_observation(
                        fixture.retrieval.clone(),
                        sequence,
                        LiveRunStreamPayload::RetrievalCompleted {
                            retrieval_id: "soak_retrieval".to_owned(),
                            query: Some("qualification".to_owned()),
                            results: Vec::new(),
                        },
                    )?
                }
            };
            let outcome = publisher.publish(publication);
            if outcome == LiveRunStreamPublishOutcome::RunClosed {
                // The server-restart injection intentionally traverses this
                // path. Core NATS does not replay these frames; the next
                // accepted sequence must expose the loss as a gap.
                lost_publications = lost_publications.saturating_add(1);
            } else if matches!(
                outcome,
                LiveRunStreamPublishOutcome::RejectedOutOfOrder
                    | LiveRunStreamPublishOutcome::RejectedAfterSeal
                    | LiveRunStreamPublishOutcome::SealConflict
            ) {
                return Err(format!("unexpected publish outcome: {outcome:?}").into());
            }
        }

        let now = Instant::now();
        for (index, subscription) in subscriptions.iter_mut().enumerate() {
            if index == 0 && now >= slow_start && now < slow_end {
                continue;
            }
            drain_one(subscription.as_mut(), &mut counts).await?;
        }

        if !restarted && Instant::now() >= restart_at {
            for fixture in &mut fixtures {
                let sequence = fixture.output_sequence;
                fixture.output_sequence = fixture.output_sequence.saturating_add(1);
                let _ = publisher.publish(LiveRunStreamPublication::new(
                    fixture.output.clone(),
                    sequence,
                    LiveRunStreamPayload::OutputTextDelta {
                        content_index: 0,
                        delta: "subscriber-restart-gap".to_owned(),
                    },
                )?);
            }
            drop(subscriptions);
            subscriber.shutdown(Duration::from_secs(5)).await?;
            subscriber = NatsCoreLiveRunStreamBroker::connect(options.clone()).await?;
            subscriptions = subscribe_all(&subscriber, &fixtures).await?;
            restarted = true;
        }

        if Instant::now() >= next_sample {
            let publisher_metrics = publisher.prometheus_metrics();
            let subscriber_metrics = subscriber.prometheus_metrics();
            println!(
                "{}",
                json!({
                    "elapsed_seconds": started.elapsed().as_secs(),
                    "runs": run_count,
                    "ticks": ticks,
                    "lost_publications": lost_publications,
                    "deliveries": {
                        "publication": counts.publication,
                        "gap": counts.gap,
                        "seal": counts.seal,
                    },
                    "resources": {
                        "rss_bytes": process_rss_bytes()?,
                        "publisher_tasks": metric_value(&publisher_metrics, "run_stream_bus_tasks{backend=\"nats_core\",state=\"active\"}")?,
                        "subscriber_tasks": metric_value(&subscriber_metrics, "run_stream_bus_tasks{backend=\"nats_core\",state=\"active\"}")?,
                        "active_subscriptions": metric_value(&subscriber_metrics, "run_stream_bus_active_subscriptions{backend=\"nats_core\"}")?,
                        "publisher_pending_messages": metric_value(&publisher_metrics, "run_stream_bus_pending_messages{backend=\"nats_core\",queue_class=\"all\"}")?,
                        "publisher_pending_bytes": metric_value(&publisher_metrics, "run_stream_bus_pending_bytes{backend=\"nats_core\",queue_class=\"all\"}")?,
                    },
                    "publisher_metrics": publisher_metrics,
                    "subscriber_metrics": subscriber_metrics,
                })
            );
            next_sample = Instant::now() + Duration::from_secs(30);
        }
        time::sleep(Duration::from_millis(tick_millis)).await;
    }

    time::timeout(Duration::from_secs(30), async {
        loop {
            if publisher
                .check_readiness(Duration::from_secs(1))
                .await
                .is_ok()
                && subscriber
                    .check_readiness(Duration::from_secs(1))
                    .await
                    .is_ok()
            {
                break;
            }
            time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await?;

    for fixture in &fixtures {
        let last = fixture.output_sequence.checked_sub(1);
        let outcome = publisher.seal(LiveRunStreamSeal::new(
            fixture.output.clone(),
            last,
            LiveRunStreamSealStatus::Completed,
        ));
        if !matches!(
            outcome,
            LiveRunStreamPublishOutcome::SealEnqueued
                | LiveRunStreamPublishOutcome::SealExactReplay
        ) {
            return Err(format!("unexpected seal outcome: {outcome:?}").into());
        }
    }
    let seal_deadline = Instant::now() + Duration::from_secs(15);
    while counts.seal < run_count as u64 && Instant::now() < seal_deadline {
        for subscription in &mut subscriptions {
            drain_one(subscription.as_mut(), &mut counts).await?;
        }
        time::sleep(Duration::from_millis(10)).await;
    }
    if counts.seal != run_count as u64 {
        return Err(format!(
            "terminal seal convergence failed: expected {run_count}, observed {}",
            counts.seal
        )
        .into());
    }
    for fixture in &fixtures {
        let _ = publisher.close_run(&fixture.run_id);
        let _ = subscriber.close_run(&fixture.run_id);
    }
    publisher.shutdown(Duration::from_secs(10)).await?;
    subscriber.shutdown(Duration::from_secs(10)).await?;
    println!(
        "{}",
        json!({
            "result": "passed",
            "elapsed_seconds": started.elapsed().as_secs(),
            "runs": run_count,
            "ticks": ticks,
            "lost_publications": lost_publications,
            "subscriber_restarted": restarted,
            "slow_client_injected": true,
            "deliveries": {
                "publication": counts.publication,
                "gap": counts.gap,
                "seal": counts.seal,
            }
        })
    );
    Ok(())
}

fn options_from_environment(
) -> Result<NatsCoreLiveRunStreamBrokerOptions, Box<dyn std::error::Error>> {
    let server = std::env::var("TEST_NATS_URL")?;
    let credentials = std::env::var("TEST_NATS_CREDENTIALS_FILE")
        .ok()
        .map(std::fs::read_to_string)
        .transpose()?;
    let root_certificates = std::env::var("TEST_NATS_CA")
        .ok()
        .map(PathBuf::from)
        .into_iter()
        .collect::<Vec<_>>();
    let tls_required = !root_certificates.is_empty() || server.starts_with("tls://");
    Ok(NatsCoreLiveRunStreamBrokerOptions {
        servers: vec![server],
        namespace: std::env::var("TEST_NATS_NAMESPACE")
            .unwrap_or_else(|_| "qualification".to_owned()),
        credentials,
        tls: NatsCoreTlsOptions {
            required: tls_required,
            root_certificates,
            client_certificate: None,
            client_private_key: None,
        },
        connect_timeout: Duration::from_secs(10),
        subscription_ready_timeout: Duration::from_secs(5),
        reconnect_min_delay: Duration::from_millis(100),
        reconnect_max_delay: Duration::from_secs(5),
        max_pending_messages: 4_096,
        max_pending_bytes: 64 * 1_024 * 1_024,
        body_queue_capacity: 64,
        control_queue_capacity: 16,
        max_frame_bytes: 64 * 1_024,
        max_item_bytes: 8 * 1_024 * 1_024,
        max_run_bytes: 32 * 1_024 * 1_024,
        drain_timeout: Duration::from_secs(10),
        outbound_idle_timeout: Duration::from_secs(120),
    })
}

fn build_fixtures(count: usize) -> Result<Vec<RunFixture>, Box<dyn std::error::Error>> {
    (0..count)
        .map(|index| {
            let run_id = RunId::new(format!("run_nats_soak_{index}_{}", uuid::Uuid::new_v4()))?;
            let activation = ActivationId::new(format!("activation_nats_soak_{index}"))?;
            Ok(RunFixture {
                output: LiveRunStreamItemIdentity::new(
                    run_id.clone(),
                    activation.clone(),
                    AttemptNo::FIRST,
                    1,
                    format!("item_nats_soak_{index}"),
                    0,
                )?,
                tool: LiveRunObservationIdentity::new(
                    run_id.clone(),
                    activation.clone(),
                    AttemptNo::FIRST,
                    format!("tool_nats_soak_{index}"),
                )?,
                retrieval: LiveRunObservationIdentity::new(
                    run_id.clone(),
                    activation,
                    AttemptNo::FIRST,
                    format!("retrieval_nats_soak_{index}"),
                )?,
                run_id,
                output_sequence: 0,
                tool_sequence: 0,
                retrieval_sequence: 0,
            })
        })
        .collect()
}

async fn subscribe_all(
    broker: &NatsCoreLiveRunStreamBroker,
    fixtures: &[RunFixture],
) -> Result<Vec<Box<dyn LiveRunStreamSubscriber>>, Box<dyn std::error::Error>> {
    let mut subscriptions = Vec::with_capacity(fixtures.len());
    for fixture in fixtures {
        subscriptions.push(broker.subscribe(fixture.run_id.clone()).await?);
    }
    Ok(subscriptions)
}

async fn drain_one(
    subscriber: &mut dyn LiveRunStreamSubscriber,
    counts: &mut DeliveryCounts,
) -> Result<(), Box<dyn std::error::Error>> {
    match time::timeout(Duration::from_millis(2), subscriber.recv()).await {
        Ok(Ok(LiveRunStreamDelivery::Publication(_))) => {
            counts.publication = counts.publication.saturating_add(1)
        }
        Ok(Ok(LiveRunStreamDelivery::Gap(_))) => counts.gap = counts.gap.saturating_add(1),
        Ok(Ok(LiveRunStreamDelivery::Seal(_))) => counts.seal = counts.seal.saturating_add(1),
        Ok(Err(error)) => return Err(error.into()),
        Err(_) => {}
    }
    Ok(())
}

fn env_u64(name: &str, default: u64) -> Result<u64, Box<dyn std::error::Error>> {
    Ok(std::env::var(name)
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(default))
}

fn env_usize(name: &str, default: usize) -> Result<usize, Box<dyn std::error::Error>> {
    Ok(std::env::var(name)
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(default))
}

fn metric_value(metrics: &str, name: &str) -> Result<u64, Box<dyn std::error::Error>> {
    metrics
        .lines()
        .find_map(|line| {
            line.strip_prefix(name)
                .and_then(|tail| tail.trim().parse::<u64>().ok())
        })
        .ok_or_else(|| format!("missing qualification metric {name}").into())
}

fn process_rss_bytes() -> Result<u64, Box<dyn std::error::Error>> {
    let pid = std::process::id().to_string();
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()?;
    if !output.status.success() {
        return Err("failed to sample qualification process RSS".into());
    }
    let kibibytes = std::str::from_utf8(&output.stdout)?.trim().parse::<u64>()?;
    kibibytes
        .checked_mul(1_024)
        .ok_or_else(|| "qualification process RSS overflow".into())
}
