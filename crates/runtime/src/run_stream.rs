//! Public `run-stream/v1` facade and in-process broker adapter.
//!
//! The protocol, projection, ordering, and broker-port contracts are owned by
//! `insight-engine`; this module supplies the single-process runtime adapter.

use std::{
    collections::BTreeMap,
    fmt::{self, Write as _},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, Weak,
    },
};

use async_trait::async_trait;
use insight_engine::{run_stream::adapter::RunQueue, RunId};

pub use insight_engine::run_stream::{
    CompletedFunctionCallPublication, CompletedFunctionCallTailPublication,
    LiveRunObservationIdentity, LiveRunStreamBroker, LiveRunStreamBrokerCapability,
    LiveRunStreamBrokerError, LiveRunStreamByteLimits, LiveRunStreamCloseOutcome,
    LiveRunStreamDelivery, LiveRunStreamGap, LiveRunStreamItemIdentity, LiveRunStreamPayload,
    LiveRunStreamPublication, LiveRunStreamPublishOutcome, LiveRunStreamSeal,
    LiveRunStreamSealStatus, LiveRunStreamSourceIdentity, LiveRunStreamSubscriber,
    LiveRunStreamTerminalBarrierOutcome, RunCompletedSnapshot, RunFailedSnapshot,
    RunInitialSnapshot, RunInteractionClosedDetails, RunInteractionMode, RunInteractionOutcome,
    RunInteractionRequiredDetails, RunInteractionSourceKind, RunInteractionState,
    RunInteractionSummary, RunObjectKind, RunOutputContentPart, RunOutputItem, RunOutputItemStatus,
    RunOutputRole, RunPublicError, RunPublicResultError, RunRetrieval, RunRetrievalMetadata,
    RunRetrievalPublicProjection, RunRetrievalResult, RunStatus, RunStoppedSnapshot,
    RunStreamEvent, RunStreamEventType, RunStreamGapAction, RunToolCompletedArgumentsProjection,
    RunToolContent, RunToolPublicProjection, RunToolResult, RunUsage, RunUsageInputDetails,
    RunUsageOutputDetails, RunUsageStatus, MAX_FUNCTION_CALL_ARGUMENT_BYTES,
    RUN_STREAM_PROTOCOL_VERSION,
};

const LIVE_RUN_STREAM_CONFIG_INVALID: &str = "LIVE_RUN_STREAM_CONFIG_INVALID";
const LIVE_RUN_STREAM_NOT_READY: &str = "LIVE_RUN_STREAM_NOT_READY";
const LIVE_RUN_STREAM_SUBSCRIBER_EXISTS: &str = "LIVE_RUN_STREAM_SUBSCRIBER_EXISTS";
#[cfg(test)]
const LIVE_RUN_STREAM_STREAM_CLOSED: &str = "LIVE_RUN_STREAM_STREAM_CLOSED";

#[derive(Clone)]
pub struct InMemoryLiveRunStreamBroker {
    inner: Arc<InMemoryBrokerInner>,
}

struct InMemoryBrokerInner {
    body_capacity: usize,
    control_capacity: usize,
    byte_limits: LiveRunStreamByteLimits,
    accepting: AtomicBool,
    runs: Mutex<BTreeMap<RunId, Arc<RunQueue>>>,
    terminal_barriers: Mutex<BTreeMap<&'static str, (u64, u64)>>,
    counters: Mutex<BTreeMap<(&'static str, &'static str, &'static str), u64>>,
}

impl InMemoryLiveRunStreamBroker {
    pub fn new(
        body_capacity: usize,
        control_capacity: usize,
    ) -> Result<Self, LiveRunStreamBrokerError> {
        Self::new_with_limits(
            body_capacity,
            control_capacity,
            LiveRunStreamByteLimits::default(),
        )
    }

    pub fn new_with_limits(
        body_capacity: usize,
        control_capacity: usize,
        byte_limits: LiveRunStreamByteLimits,
    ) -> Result<Self, LiveRunStreamBrokerError> {
        if body_capacity == 0 || control_capacity == 0 {
            return Err(LiveRunStreamBrokerError::new(
                LIVE_RUN_STREAM_CONFIG_INVALID,
                "live Run stream broker capacities must be non-zero",
            ));
        }
        LiveRunStreamByteLimits::new(
            byte_limits.max_frame_bytes,
            byte_limits.max_item_bytes,
            byte_limits.max_run_bytes,
        )?;
        Ok(Self {
            inner: Arc::new(InMemoryBrokerInner {
                body_capacity,
                control_capacity,
                byte_limits,
                accepting: AtomicBool::new(true),
                runs: Mutex::new(BTreeMap::new()),
                terminal_barriers: Mutex::new(BTreeMap::new()),
                counters: Mutex::new(BTreeMap::new()),
            }),
        })
    }

    pub fn body_capacity(&self) -> usize {
        self.inner.body_capacity
    }

    pub fn control_capacity(&self) -> usize {
        self.inner.control_capacity
    }

    pub fn byte_limits(&self) -> LiveRunStreamByteLimits {
        self.inner.byte_limits
    }
}

impl fmt::Debug for InMemoryLiveRunStreamBroker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let run_count = lock(&self.inner.runs).len();
        formatter
            .debug_struct("InMemoryLiveRunStreamBroker")
            .field("body_capacity", &self.inner.body_capacity)
            .field("control_capacity", &self.inner.control_capacity)
            .field("byte_limits", &self.inner.byte_limits)
            .field("run_count", &run_count)
            .finish()
    }
}

#[async_trait]
impl LiveRunStreamBroker for InMemoryLiveRunStreamBroker {
    fn deployment_capability(&self) -> LiveRunStreamBrokerCapability {
        LiveRunStreamBrokerCapability::SingleProcess
    }

    fn prometheus_metrics(&self) -> String {
        let ready = self.inner.accepting.load(Ordering::Acquire);
        let active = lock(&self.inner.runs).len();
        let mut output = format!(
            concat!(
                "run_stream_bus_ready{{backend=\"in_memory\"}} {}\n",
                "run_stream_bus_connections{{backend=\"in_memory\",state=\"connected\"}} {}\n",
                "run_stream_bus_reconnect_total{{backend=\"in_memory\",outcome=\"connected\"}} 0\n",
                "run_stream_bus_active_subscriptions{{backend=\"in_memory\"}} {}\n",
                "run_stream_bus_tasks{{backend=\"in_memory\",state=\"active\"}} 0\n",
                "run_stream_bus_publish_latency_seconds_count{{backend=\"in_memory\"}} 0\n",
                "run_stream_bus_publish_latency_seconds_sum{{backend=\"in_memory\"}} 0\n",
                "run_stream_bus_pending_messages{{backend=\"in_memory\",queue_class=\"all\"}} 0\n",
                "run_stream_bus_pending_bytes{{backend=\"in_memory\",queue_class=\"all\"}} 0\n",
                "run_stream_bus_decode_error_total{{backend=\"in_memory\",reason=\"wire\"}} 0\n",
                "run_stream_bus_slow_consumer_total{{backend=\"in_memory\",scope=\"client\"}} 0\n",
                "run_stream_bus_subscription_ready_seconds_count{{backend=\"in_memory\"}} 0\n",
                "run_stream_bus_subscription_ready_seconds_sum{{backend=\"in_memory\"}} 0\n"
            ),
            u8::from(ready),
            u8::from(ready),
            active
        );
        let barriers = lock(&self.inner.terminal_barriers);
        for outcome in LiveRunStreamTerminalBarrierOutcome::ALL {
            let label = outcome.label();
            let (count, nanos) = barriers.get(label).copied().unwrap_or_default();
            let _ = writeln!(
                output,
                "run_stream_bus_terminal_barrier_seconds_count{{backend=\"in_memory\",outcome=\"{label}\"}} {count}"
            );
            let _ = writeln!(
                output,
                "run_stream_bus_terminal_barrier_seconds_sum{{backend=\"in_memory\",outcome=\"{label}\"}} {:.9}",
                nanos as f64 / 1_000_000_000.0
            );
        }
        let counters = lock(&self.inner.counters);
        for (family, zero_sample) in [
            (
                "publish",
                "run_stream_bus_publish_total{backend=\"in_memory\",event_class=\"output\",outcome=\"enqueued\"} 0\n",
            ),
            (
                "drop",
                "run_stream_bus_dropped_total{backend=\"in_memory\",event_class=\"output\",reason=\"producer_queue_full\"} 0\n",
            ),
            (
                "gap",
                "run_stream_bus_gap_total{backend=\"in_memory\",gap_kind=\"known\",reason=\"producer_queue\"} 0\n",
            ),
        ] {
            if !counters.keys().any(|(candidate, _, _)| *candidate == family) {
                output.push_str(zero_sample);
            }
        }
        for ((family, class, outcome), count) in counters.iter() {
            let name = match *family {
                "publish" => "run_stream_bus_publish_total",
                "drop" => "run_stream_bus_dropped_total",
                "gap" => "run_stream_bus_gap_total",
                _ => continue,
            };
            let labels = if *family == "gap" {
                format!("backend=\"in_memory\",gap_kind=\"{class}\",reason=\"{outcome}\"")
            } else if *family == "drop" {
                format!("backend=\"in_memory\",event_class=\"{class}\",reason=\"{outcome}\"")
            } else {
                format!("backend=\"in_memory\",event_class=\"{class}\",outcome=\"{outcome}\"")
            };
            let _ = writeln!(output, "{name}{{{labels}}} {count}");
        }
        output
    }

    fn record_terminal_barrier(
        &self,
        outcome: LiveRunStreamTerminalBarrierOutcome,
        duration: std::time::Duration,
    ) {
        let nanos = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
        let mut barriers = lock(&self.inner.terminal_barriers);
        let (count, total_nanos) = barriers.entry(outcome.label()).or_default();
        *count = count.saturating_add(1);
        *total_nanos = total_nanos.saturating_add(nanos);
    }

    async fn check_readiness(
        &self,
        _readiness_timeout: std::time::Duration,
    ) -> Result<(), LiveRunStreamBrokerError> {
        self.inner
            .accepting
            .load(Ordering::Acquire)
            .then_some(())
            .ok_or_else(|| {
                LiveRunStreamBrokerError::new(
                    LIVE_RUN_STREAM_NOT_READY,
                    "in-memory live Run stream broker is not accepting work",
                )
            })
    }

    async fn shutdown(&self, _grace: std::time::Duration) -> Result<(), LiveRunStreamBrokerError> {
        self.inner.accepting.store(false, Ordering::Release);
        let queues = {
            let mut runs = lock(&self.inner.runs);
            std::mem::take(&mut *runs)
        };
        for queue in queues.into_values() {
            queue.close();
        }
        Ok(())
    }

    async fn subscribe(
        &self,
        run_id: RunId,
    ) -> Result<Box<dyn LiveRunStreamSubscriber>, LiveRunStreamBrokerError> {
        if !self.inner.accepting.load(Ordering::Acquire) {
            return Err(LiveRunStreamBrokerError::new(
                LIVE_RUN_STREAM_NOT_READY,
                "in-memory live Run stream broker is not accepting work",
            ));
        }
        let mut runs = lock(&self.inner.runs);
        if !self.inner.accepting.load(Ordering::Acquire) {
            return Err(LiveRunStreamBrokerError::new(
                LIVE_RUN_STREAM_NOT_READY,
                "in-memory live Run stream broker is not accepting work",
            ));
        }
        if runs.contains_key(&run_id) {
            return Err(LiveRunStreamBrokerError::new(
                LIVE_RUN_STREAM_SUBSCRIBER_EXISTS,
                "a live Run stream subscriber is already registered for this Run",
            ));
        }
        let queue = Arc::new(RunQueue::new_with_limits(
            run_id.clone(),
            self.inner.body_capacity,
            self.inner.control_capacity,
            self.inner.byte_limits,
        ));
        runs.insert(run_id.clone(), Arc::clone(&queue));
        Ok(Box::new(InMemoryLiveRunStreamSubscriber {
            run_id,
            queue,
            owner: Arc::downgrade(&self.inner),
        }))
    }

    fn publish(&self, publication: LiveRunStreamPublication) -> LiveRunStreamPublishOutcome {
        if !self.inner.accepting.load(Ordering::Acquire) {
            return LiveRunStreamPublishOutcome::RunClosed;
        }
        let class = publication_class(&publication);
        let queue = lock(&self.inner.runs).get(publication.run_id()).cloned();
        let outcome = match queue {
            Some(queue) => queue.publish(publication),
            None => LiveRunStreamPublishOutcome::NoSubscriber,
        };
        record_in_memory_outcome(&self.inner, class, outcome);
        outcome
    }

    fn seal(&self, seal: LiveRunStreamSeal) -> LiveRunStreamPublishOutcome {
        if !self.inner.accepting.load(Ordering::Acquire) {
            return LiveRunStreamPublishOutcome::RunClosed;
        }
        let queue = lock(&self.inner.runs)
            .get(seal.identity().run_id())
            .cloned();
        let outcome = match queue {
            Some(queue) => queue.seal(seal),
            None => LiveRunStreamPublishOutcome::NoSubscriber,
        };
        record_in_memory_outcome(&self.inner, "seal", outcome);
        outcome
    }

    fn close_run(&self, run_id: &RunId) -> LiveRunStreamCloseOutcome {
        let queue = lock(&self.inner.runs).remove(run_id);
        let outcome = queue.map_or_else(LiveRunStreamCloseOutcome::default, |queue| queue.close());
        if outcome.unknown_tail_gaps() > 0 {
            *lock(&self.inner.counters)
                .entry(("gap", "unknown_tail", "close"))
                .or_default() += outcome.unknown_tail_gaps() as u64;
        }
        outcome
    }
}

fn publication_class(publication: &LiveRunStreamPublication) -> &'static str {
    if publication.output_item_identity().is_some() {
        "output"
    } else if publication.payload_type() == RunStreamEventType::RunRetrievalCompleted {
        "retrieval"
    } else {
        "tool"
    }
}

fn record_in_memory_outcome(
    inner: &InMemoryBrokerInner,
    class: &'static str,
    outcome: LiveRunStreamPublishOutcome,
) {
    let label = match outcome {
        LiveRunStreamPublishOutcome::Enqueued => "enqueued",
        LiveRunStreamPublishOutcome::EnqueuedAfterGap => "enqueued_after_gap",
        LiveRunStreamPublishOutcome::EnqueuedAfterBestEffortLoss => "enqueued_after_loss",
        LiveRunStreamPublishOutcome::DroppedWithGap => "dropped_with_gap",
        LiveRunStreamPublishOutcome::DroppedOversizeWithGap => "dropped_oversize_with_gap",
        LiveRunStreamPublishOutcome::DroppedBestEffort => "dropped_best_effort",
        LiveRunStreamPublishOutcome::SealEnqueued => "seal_enqueued",
        LiveRunStreamPublishOutcome::SealExactReplay => "seal_exact_replay",
        LiveRunStreamPublishOutcome::NoSubscriber => "no_subscriber",
        LiveRunStreamPublishOutcome::RunClosed => "run_closed",
        LiveRunStreamPublishOutcome::RejectedOutOfOrder => "rejected_out_of_order",
        LiveRunStreamPublishOutcome::RejectedAfterSeal => "rejected_after_seal",
        LiveRunStreamPublishOutcome::SealConflict => "seal_conflict",
        LiveRunStreamPublishOutcome::ControlQueueFull => "control_full",
    };
    let mut counters = lock(&inner.counters);
    *counters.entry(("publish", class, label)).or_default() += 1;
    let reason = match outcome {
        LiveRunStreamPublishOutcome::DroppedWithGap
        | LiveRunStreamPublishOutcome::DroppedBestEffort => Some("producer_queue_full"),
        LiveRunStreamPublishOutcome::DroppedOversizeWithGap => Some("oversize"),
        LiveRunStreamPublishOutcome::ControlQueueFull => Some("control_full"),
        _ => None,
    };
    if let Some(reason) = reason {
        *counters.entry(("drop", class, reason)).or_default() += 1;
    }
    if matches!(
        outcome,
        LiveRunStreamPublishOutcome::EnqueuedAfterGap
            | LiveRunStreamPublishOutcome::DroppedWithGap
            | LiveRunStreamPublishOutcome::DroppedOversizeWithGap
    ) {
        *counters
            .entry(("gap", "known", "producer_queue"))
            .or_default() += 1;
    }
}

struct InMemoryLiveRunStreamSubscriber {
    run_id: RunId,
    queue: Arc<RunQueue>,
    owner: Weak<InMemoryBrokerInner>,
}

#[async_trait]
impl LiveRunStreamSubscriber for InMemoryLiveRunStreamSubscriber {
    fn run_id(&self) -> &RunId {
        &self.run_id
    }

    async fn recv(&mut self) -> Result<LiveRunStreamDelivery, LiveRunStreamBrokerError> {
        self.queue.recv().await
    }
}

impl Drop for InMemoryLiveRunStreamSubscriber {
    fn drop(&mut self) {
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        let removed = {
            let mut runs = lock(&owner.runs);
            if runs
                .get(&self.run_id)
                .is_some_and(|queue| Arc::ptr_eq(queue, &self.queue))
            {
                runs.remove(&self.run_id)
            } else {
                None
            }
        };
        if let Some(queue) = removed {
            queue.close();
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_engine::{ActivationId, AttemptNo};
    use serde_json::json;

    fn run(value: &str) -> RunId {
        RunId::new(value).unwrap()
    }

    fn identity(run_id: &str) -> LiveRunStreamItemIdentity {
        LiveRunStreamItemIdentity::new(
            run(run_id),
            ActivationId::new("activation_answer").unwrap(),
            AttemptNo::FIRST,
            1,
            "msg_answer",
            0,
        )
        .unwrap()
    }

    fn workflow_identity(run_id: &str, source_id: &str) -> LiveRunObservationIdentity {
        LiveRunObservationIdentity::new(
            run(run_id),
            ActivationId::new("activation_run_observation").unwrap(),
            AttemptNo::FIRST,
            source_id,
        )
        .unwrap()
    }

    fn delta(
        identity: LiveRunStreamItemIdentity,
        sequence: u64,
        text: &str,
    ) -> LiveRunStreamPublication {
        LiveRunStreamPublication::new(
            identity,
            sequence,
            LiveRunStreamPayload::OutputTextDelta {
                content_index: 0,
                delta: text.to_owned(),
            },
        )
        .unwrap()
    }

    fn tool_started(
        identity: LiveRunObservationIdentity,
        sequence: u64,
        call_id: &str,
    ) -> LiveRunStreamPublication {
        LiveRunStreamPublication::new_run_observation(
            identity,
            sequence,
            LiveRunStreamPayload::ToolStarted {
                call_id: call_id.to_owned(),
                tool_name: "lookup".to_owned(),
                arguments: Some(json!({"published": true})),
            },
        )
        .unwrap()
    }

    fn public_wire_bytes(publication: &LiveRunStreamPublication) -> usize {
        serde_json::to_vec(&publication.clone().into_public_event(u64::MAX))
            .unwrap()
            .len()
    }

    #[tokio::test]
    async fn metrics_are_bounded_complete_and_readiness_tracks_shutdown() {
        let broker = InMemoryLiveRunStreamBroker::new(4, 4).unwrap();
        broker.record_terminal_barrier(
            LiveRunStreamTerminalBarrierOutcome::Complete,
            std::time::Duration::from_millis(5),
        );
        let metrics = broker.prometheus_metrics();
        for family in [
            "run_stream_bus_ready",
            "run_stream_bus_connections",
            "run_stream_bus_reconnect_total",
            "run_stream_bus_active_subscriptions",
            "run_stream_bus_tasks",
            "run_stream_bus_publish_total",
            "run_stream_bus_publish_latency_seconds",
            "run_stream_bus_pending_messages",
            "run_stream_bus_pending_bytes",
            "run_stream_bus_dropped_total",
            "run_stream_bus_gap_total",
            "run_stream_bus_decode_error_total",
            "run_stream_bus_slow_consumer_total",
            "run_stream_bus_subscription_ready_seconds",
            "run_stream_bus_terminal_barrier_seconds",
        ] {
            assert!(metrics.contains(family), "missing metric family {family}");
        }
        assert!(!metrics.contains("run_private_identity"));
        assert!(broker
            .check_readiness(std::time::Duration::from_secs(1))
            .await
            .is_ok());
        broker
            .shutdown(std::time::Duration::from_secs(1))
            .await
            .unwrap();
        assert!(broker
            .check_readiness(std::time::Duration::from_secs(1))
            .await
            .is_err());
        assert!(broker
            .prometheus_metrics()
            .contains("run_stream_bus_ready{backend=\"in_memory\"} 0"));
    }

    #[tokio::test]
    async fn subscription_is_installed_before_the_first_publication_and_is_live_only() {
        let broker = InMemoryLiveRunStreamBroker::new(4, 4).unwrap();
        let first_identity = identity("run_first");
        assert_eq!(
            broker.publish(delta(first_identity.clone(), 0, "before subscribe")),
            LiveRunStreamPublishOutcome::NoSubscriber
        );
        let mut subscriber = broker.subscribe(run("run_first")).await.unwrap();
        assert_eq!(
            broker.publish(delta(first_identity, 0, "first visible")),
            LiveRunStreamPublishOutcome::Enqueued
        );
        let LiveRunStreamDelivery::Publication(publication) = subscriber.recv().await.unwrap()
        else {
            panic!("first delivery must be the first post-subscribe publication")
        };
        assert_eq!(publication.local_sequence(), 0);
        assert_eq!(
            publication.payload_type(),
            RunStreamEventType::RunOutputTextDelta
        );
    }

    #[tokio::test]
    async fn output_items_and_run_observations_have_isolated_local_ordering() {
        let broker = InMemoryLiveRunStreamBroker::new(8, 4).unwrap();
        let run_id = "run_source_ordering";
        let mut subscriber = broker.subscribe(run(run_id)).await.unwrap();
        let item = identity(run_id);
        let first_observation = workflow_identity(run_id, "tool_source_a");
        let second_observation = workflow_identity(run_id, "tool_source_b");

        assert_eq!(
            broker.publish(delta(item, 0, "answer")),
            LiveRunStreamPublishOutcome::Enqueued
        );
        assert_eq!(
            broker.publish(tool_started(first_observation.clone(), 2, "call_a")),
            LiveRunStreamPublishOutcome::EnqueuedAfterBestEffortLoss
        );
        assert_eq!(
            broker.publish(tool_started(second_observation, 0, "call_b")),
            LiveRunStreamPublishOutcome::Enqueued
        );
        assert_eq!(
            broker.publish(tool_started(first_observation, 1, "call_a_replay")),
            LiveRunStreamPublishOutcome::RejectedOutOfOrder
        );

        for expected in [
            RunStreamEventType::RunOutputTextDelta,
            RunStreamEventType::RunToolStarted,
            RunStreamEventType::RunToolStarted,
        ] {
            let LiveRunStreamDelivery::Publication(publication) = subscriber.recv().await.unwrap()
            else {
                panic!("source-local loss must not synthesize an output-item control frame")
            };
            assert_eq!(publication.payload_type(), expected);
        }
    }

    #[tokio::test]
    async fn workflow_loss_does_not_create_gaps_or_block_an_output_item_seal() {
        let broker = InMemoryLiveRunStreamBroker::new(1, 4).unwrap();
        let run_id = "run_workflow_best_effort";
        let mut subscriber = broker.subscribe(run(run_id)).await.unwrap();
        let observation = workflow_identity(run_id, "tool_source");
        let item = identity(run_id);

        assert_eq!(
            broker.publish(tool_started(observation.clone(), 0, "call_kept")),
            LiveRunStreamPublishOutcome::Enqueued
        );
        assert_eq!(
            broker.publish(tool_started(observation, 1, "call_dropped")),
            LiveRunStreamPublishOutcome::DroppedBestEffort
        );
        let seal = LiveRunStreamSeal::new(item, None, LiveRunStreamSealStatus::Completed);
        assert_eq!(
            broker.seal(seal.clone()),
            LiveRunStreamPublishOutcome::SealEnqueued
        );

        let LiveRunStreamDelivery::Seal(delivered) = subscriber.recv().await.unwrap() else {
            panic!("workflow observations must not enter the output-item seal barrier")
        };
        assert_eq!(delivered, seal);

        let close = broker.close_run(&run(run_id));
        assert_eq!(close.unknown_tail_gaps(), 0);
        assert_eq!(close.omitted_unknown_tail_gaps(), 0);
        assert!(matches!(
            subscriber.recv().await.unwrap(),
            LiveRunStreamDelivery::Publication(_)
        ));
        assert_eq!(
            subscriber.recv().await.unwrap_err().code(),
            LIVE_RUN_STREAM_STREAM_CLOSED
        );
    }

    #[tokio::test]
    async fn missing_any_completed_function_call_frame_yields_a_gap_before_its_seal() {
        for missing_sequence in 0..=CompletedFunctionCallPublication::LAST_LOCAL_SEQUENCE {
            let run_id = format!("run_function_gap_{missing_sequence}");
            let broker = InMemoryLiveRunStreamBroker::new(8, 8).unwrap();
            let mut subscriber = broker.subscribe(run(&run_id)).await.unwrap();
            let plan = CompletedFunctionCallPublication::build(
                identity(&run_id),
                "call_gap",
                "lookup",
                r#"{"indicator":"WBC"}"#,
            )
            .unwrap();
            let (publications, seal) = plan.into_parts();
            for publication in publications {
                if publication.local_sequence() != missing_sequence {
                    let outcome = broker.publish(publication);
                    assert!(matches!(
                        outcome,
                        LiveRunStreamPublishOutcome::Enqueued
                            | LiveRunStreamPublishOutcome::EnqueuedAfterGap
                    ));
                }
            }
            assert_eq!(
                broker.seal(seal.clone()),
                LiveRunStreamPublishOutcome::SealEnqueued
            );

            for sequence in 0..=CompletedFunctionCallPublication::LAST_LOCAL_SEQUENCE {
                if sequence == missing_sequence {
                    let LiveRunStreamDelivery::Gap(gap) = subscriber.recv().await.unwrap() else {
                        panic!("the missing canonical frame must be explicit in item order")
                    };
                    assert_eq!(gap.missing_from(), missing_sequence);
                    assert_eq!(gap.missing_to(), Some(missing_sequence));
                } else {
                    let LiveRunStreamDelivery::Publication(publication) =
                        subscriber.recv().await.unwrap()
                    else {
                        panic!("retained canonical frames must preserve item order")
                    };
                    assert_eq!(publication.local_sequence(), sequence);
                }
            }
            let LiveRunStreamDelivery::Seal(delivered) = subscriber.recv().await.unwrap() else {
                panic!("the completed seal must wait for every retained item frame")
            };
            assert_eq!(delivered, seal);
        }
    }

    #[tokio::test]
    async fn bounded_body_lag_is_explicit_and_seal_waits_for_prior_body() {
        let broker = InMemoryLiveRunStreamBroker::new(1, 4).unwrap();
        let item = identity("run_lag");
        let mut subscriber = broker.subscribe(run("run_lag")).await.unwrap();
        assert_eq!(
            broker.publish(delta(item.clone(), 0, "kept")),
            LiveRunStreamPublishOutcome::Enqueued
        );
        assert_eq!(
            broker.publish(delta(item.clone(), 1, "dropped-one")),
            LiveRunStreamPublishOutcome::DroppedWithGap
        );
        assert_eq!(
            broker.publish(delta(item.clone(), 2, "dropped-two")),
            LiveRunStreamPublishOutcome::DroppedWithGap
        );
        let seal = LiveRunStreamSeal::new(item, Some(2), LiveRunStreamSealStatus::Completed);
        assert_eq!(
            broker.seal(seal.clone()),
            LiveRunStreamPublishOutcome::SealEnqueued
        );

        let LiveRunStreamDelivery::Publication(publication) = subscriber.recv().await.unwrap()
        else {
            panic!("retained body must precede a later gap for the same item")
        };
        assert_eq!(publication.local_sequence(), 0);
        let LiveRunStreamDelivery::Gap(gap) = subscriber.recv().await.unwrap() else {
            panic!("dropped body must remain explicit after retained predecessors")
        };
        assert_eq!((gap.missing_from(), gap.missing_to()), (1, Some(2)));
        let LiveRunStreamDelivery::Seal(delivered) = subscriber.recv().await.unwrap() else {
            panic!("seal must follow all retained body for its item")
        };
        assert_eq!(delivered, seal);
    }

    #[tokio::test]
    async fn frame_item_and_run_byte_limits_drop_with_explicit_gaps() {
        let frame_item = identity("run_frame_limit");
        let frame = delta(frame_item, 0, "oversize frame");
        let frame_bytes = public_wire_bytes(&frame);
        let frame_broker = InMemoryLiveRunStreamBroker::new_with_limits(
            4,
            4,
            LiveRunStreamByteLimits::new(frame_bytes - 1, frame_bytes * 4, frame_bytes * 8)
                .unwrap(),
        )
        .unwrap();
        let mut frame_subscriber = frame_broker
            .subscribe(run("run_frame_limit"))
            .await
            .unwrap();
        assert_eq!(
            frame_broker.publish(frame),
            LiveRunStreamPublishOutcome::DroppedWithGap
        );
        let LiveRunStreamDelivery::Gap(frame_gap) = frame_subscriber.recv().await.unwrap() else {
            panic!("an oversized frame must become a gap")
        };
        assert_eq!(
            (frame_gap.missing_from(), frame_gap.missing_to()),
            (0, Some(0))
        );

        let item_identity = identity("run_item_limit");
        let first = delta(item_identity.clone(), 0, "first");
        let second = delta(item_identity, 1, "second");
        let first_bytes = public_wire_bytes(&first);
        let second_bytes = public_wire_bytes(&second);
        let max_frame = first_bytes.max(second_bytes);
        let item_broker = InMemoryLiveRunStreamBroker::new_with_limits(
            4,
            4,
            LiveRunStreamByteLimits::new(
                max_frame,
                first_bytes + second_bytes - 1,
                (first_bytes + second_bytes) * 2,
            )
            .unwrap(),
        )
        .unwrap();
        let mut item_subscriber = item_broker.subscribe(run("run_item_limit")).await.unwrap();
        assert_eq!(
            item_broker.publish(first),
            LiveRunStreamPublishOutcome::Enqueued
        );
        assert_eq!(
            item_broker.publish(second),
            LiveRunStreamPublishOutcome::DroppedWithGap
        );
        let LiveRunStreamDelivery::Publication(first) = item_subscriber.recv().await.unwrap()
        else {
            panic!("retained item body must precede its later size gap")
        };
        assert_eq!(first.local_sequence(), 0);
        let LiveRunStreamDelivery::Gap(item_gap) = item_subscriber.recv().await.unwrap() else {
            panic!("an exhausted item budget must become a gap")
        };
        assert_eq!(
            (item_gap.missing_from(), item_gap.missing_to()),
            (1, Some(1))
        );

        let first_identity = identity("run_total_limit");
        let second_identity = LiveRunStreamItemIdentity::new(
            run("run_total_limit"),
            ActivationId::new("activation_second").unwrap(),
            AttemptNo::FIRST,
            1,
            "msg_second",
            1,
        )
        .unwrap();
        let first = delta(first_identity, 0, "first run item");
        let second = delta(second_identity, 0, "second run item");
        let first_bytes = public_wire_bytes(&first);
        let second_bytes = public_wire_bytes(&second);
        let max_frame = first_bytes.max(second_bytes);
        let run_broker = InMemoryLiveRunStreamBroker::new_with_limits(
            4,
            4,
            LiveRunStreamByteLimits::new(max_frame, max_frame, first_bytes + second_bytes - 1)
                .unwrap(),
        )
        .unwrap();
        let mut run_subscriber = run_broker.subscribe(run("run_total_limit")).await.unwrap();
        assert_eq!(
            run_broker.publish(first),
            LiveRunStreamPublishOutcome::Enqueued
        );
        assert_eq!(
            run_broker.publish(second),
            LiveRunStreamPublishOutcome::DroppedWithGap
        );
        let LiveRunStreamDelivery::Gap(run_gap) = run_subscriber.recv().await.unwrap() else {
            panic!("an exhausted Run budget must become a gap")
        };
        assert_eq!((run_gap.missing_from(), run_gap.missing_to()), (0, Some(0)));
    }

    #[tokio::test]
    async fn skipped_producer_index_creates_a_known_gap_without_dropping_the_later_body() {
        let broker = InMemoryLiveRunStreamBroker::new(4, 4).unwrap();
        let item = identity("run_skip");
        let mut subscriber = broker.subscribe(run("run_skip")).await.unwrap();
        assert_eq!(
            broker.publish(delta(item, 2, "after missing indices")),
            LiveRunStreamPublishOutcome::EnqueuedAfterGap
        );
        let LiveRunStreamDelivery::Gap(gap) = subscriber.recv().await.unwrap() else {
            panic!("skipped producer indices must be explicit")
        };
        assert_eq!((gap.missing_from(), gap.missing_to()), (0, Some(1)));
        let LiveRunStreamDelivery::Publication(publication) = subscriber.recv().await.unwrap()
        else {
            panic!("the later body remains observable after the gap")
        };
        assert_eq!(publication.local_sequence(), 2);
    }

    #[tokio::test]
    async fn disjoint_known_gaps_are_not_falsely_widened_across_received_indices() {
        let broker = InMemoryLiveRunStreamBroker::new(4, 4).unwrap();
        let item = identity("run_disjoint_gap");
        let mut subscriber = broker.subscribe(run("run_disjoint_gap")).await.unwrap();
        assert_eq!(
            broker.publish(delta(item.clone(), 2, "received-two")),
            LiveRunStreamPublishOutcome::EnqueuedAfterGap
        );
        assert_eq!(
            broker.publish(delta(item.clone(), 3, "received-three")),
            LiveRunStreamPublishOutcome::Enqueued
        );
        assert_eq!(
            broker.publish(delta(item, 5, "received-five")),
            LiveRunStreamPublishOutcome::EnqueuedAfterGap
        );

        let LiveRunStreamDelivery::Gap(first) = subscriber.recv().await.unwrap() else {
            panic!("the first known gap must be delivered")
        };
        assert_eq!((first.missing_from(), first.missing_to()), (0, Some(1)));
        for expected in [2, 3] {
            let LiveRunStreamDelivery::Publication(publication) = subscriber.recv().await.unwrap()
            else {
                panic!("retained bodies before the next gap must remain ordered")
            };
            assert_eq!(publication.local_sequence(), expected);
        }
        let LiveRunStreamDelivery::Gap(second) = subscriber.recv().await.unwrap() else {
            panic!("the disjoint known gap must remain separate")
        };
        assert_eq!((second.missing_from(), second.missing_to()), (4, Some(4)));
    }

    #[tokio::test]
    async fn close_marks_unsealed_items_with_an_unknown_tail_then_reaches_eof() {
        let broker = InMemoryLiveRunStreamBroker::new(2, 2).unwrap();
        let item = identity("run_unsealed");
        let mut subscriber = broker.subscribe(run("run_unsealed")).await.unwrap();
        assert_eq!(
            broker.publish(delta(item, 0, "partial")),
            LiveRunStreamPublishOutcome::Enqueued
        );
        let close = broker.close_run(&run("run_unsealed"));
        assert_eq!(close.unknown_tail_gaps(), 1);
        assert_eq!(close.omitted_unknown_tail_gaps(), 0);

        assert!(matches!(
            subscriber.recv().await.unwrap(),
            LiveRunStreamDelivery::Publication(_)
        ));
        let LiveRunStreamDelivery::Gap(gap) = subscriber.recv().await.unwrap() else {
            panic!("unknown tail gap must follow retained body for the same item")
        };
        assert!(gap.has_unknown_tail());
        assert_eq!(gap.missing_from(), 1);
        assert_eq!(
            subscriber.recv().await.unwrap_err().code(),
            LIVE_RUN_STREAM_STREAM_CLOSED
        );
    }

    #[tokio::test]
    async fn runs_are_isolated_and_late_publication_after_seal_is_rejected() {
        let broker = InMemoryLiveRunStreamBroker::new(2, 2).unwrap();
        let first = identity("run_a");
        let second = identity("run_b");
        let mut subscriber_a = broker.subscribe(run("run_a")).await.unwrap();
        let mut subscriber_b = broker.subscribe(run("run_b")).await.unwrap();
        assert_eq!(
            broker.publish(delta(second, 0, "only b")),
            LiveRunStreamPublishOutcome::Enqueued
        );
        let LiveRunStreamDelivery::Publication(delivered_b) = subscriber_b.recv().await.unwrap()
        else {
            panic!("run B must receive its own publication")
        };
        assert_eq!(delivered_b.run_id(), &run("run_b"));

        let seal = LiveRunStreamSeal::new(first.clone(), None, LiveRunStreamSealStatus::Completed);
        assert_eq!(
            broker.seal(seal.clone()),
            LiveRunStreamPublishOutcome::SealEnqueued
        );
        assert_eq!(
            broker.seal(seal),
            LiveRunStreamPublishOutcome::SealExactReplay
        );
        assert_eq!(
            broker.publish(delta(first, 0, "too late")),
            LiveRunStreamPublishOutcome::RejectedAfterSeal
        );
        assert!(matches!(
            subscriber_a.recv().await.unwrap(),
            LiveRunStreamDelivery::Seal(_)
        ));
    }
}
