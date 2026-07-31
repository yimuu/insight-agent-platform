//! Public `run-stream/v1` facade and in-process broker adapter.
//!
//! The protocol, projection, ordering, and broker-port contracts are owned by
//! `insight-engine`; this module supplies the single-process runtime adapter.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex, Weak},
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
    RunCompletedSnapshot, RunFailedSnapshot, RunInitialSnapshot, RunInteractionClosedDetails,
    RunInteractionMode, RunInteractionOutcome, RunInteractionRequiredDetails,
    RunInteractionSourceKind, RunInteractionState, RunInteractionSummary, RunObjectKind,
    RunOutputContentPart, RunOutputItem, RunOutputItemStatus, RunOutputRole, RunPublicError,
    RunPublicResultError, RunRetrieval, RunRetrievalMetadata, RunRetrievalPublicProjection,
    RunRetrievalResult, RunStatus, RunStoppedSnapshot, RunStreamEvent, RunStreamEventType,
    RunStreamGapAction, RunToolCompletedArgumentsProjection, RunToolContent,
    RunToolPublicProjection, RunToolResult, RunUsage, RunUsageInputDetails, RunUsageOutputDetails,
    RunUsageStatus, MAX_FUNCTION_CALL_ARGUMENT_BYTES, RUN_STREAM_PROTOCOL_VERSION,
};

const LIVE_RUN_STREAM_CONFIG_INVALID: &str = "LIVE_RUN_STREAM_CONFIG_INVALID";
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
    runs: Mutex<BTreeMap<RunId, Arc<RunQueue>>>,
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
                runs: Mutex::new(BTreeMap::new()),
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

    async fn shutdown(&self, _grace: std::time::Duration) -> Result<(), LiveRunStreamBrokerError> {
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
        let mut runs = lock(&self.inner.runs);
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
        let queue = lock(&self.inner.runs).get(publication.run_id()).cloned();
        match queue {
            Some(queue) => queue.publish(publication),
            None => LiveRunStreamPublishOutcome::NoSubscriber,
        }
    }

    fn seal(&self, seal: LiveRunStreamSeal) -> LiveRunStreamPublishOutcome {
        let queue = lock(&self.inner.runs)
            .get(seal.identity().run_id())
            .cloned();
        match queue {
            Some(queue) => queue.seal(seal),
            None => LiveRunStreamPublishOutcome::NoSubscriber,
        }
    }

    fn close_run(&self, run_id: &RunId) -> LiveRunStreamCloseOutcome {
        let queue = lock(&self.inner.runs).remove(run_id);
        queue.map_or_else(LiveRunStreamCloseOutcome::default, |queue| queue.close())
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

            let LiveRunStreamDelivery::Gap(gap) = subscriber.recv().await.unwrap() else {
                panic!("a missing canonical function-call frame must precede the item seal")
            };
            assert_eq!(gap.missing_from(), missing_sequence);
            assert_eq!(gap.missing_to(), Some(missing_sequence));

            for _ in 0..3 {
                assert!(matches!(
                    subscriber.recv().await.unwrap(),
                    LiveRunStreamDelivery::Publication(_)
                ));
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

        let LiveRunStreamDelivery::Gap(gap) = subscriber.recv().await.unwrap() else {
            panic!("control gap must have priority")
        };
        assert_eq!((gap.missing_from(), gap.missing_to()), (1, Some(2)));
        let LiveRunStreamDelivery::Publication(publication) = subscriber.recv().await.unwrap()
        else {
            panic!("the retained body must precede its seal")
        };
        assert_eq!(publication.local_sequence(), 0);
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
        let LiveRunStreamDelivery::Gap(second) = subscriber.recv().await.unwrap() else {
            panic!("the disjoint known gap must remain separate")
        };
        assert_eq!((first.missing_from(), first.missing_to()), (0, Some(1)));
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

        let LiveRunStreamDelivery::Gap(gap) = subscriber.recv().await.unwrap() else {
            panic!("unknown tail gap must be delivered before retained body")
        };
        assert!(gap.has_unknown_tail());
        assert_eq!(gap.missing_from(), 1);
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
