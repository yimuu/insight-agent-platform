//! Public response-stream compatibility facade and in-process broker adapter.
//!
//! The protocol, projection, ordering, and broker-port contracts are owned by
//! `insight-engine`. This module retains the original public paths while the
//! in-memory broker supplies the single-process runtime implementation.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex, Weak},
};

use async_trait::async_trait;
use insight_engine::{response::adapter::RunQueue, RunId};

pub use insight_engine::response::{
    CompletedFunctionCallPublication, CompletedFunctionCallTailPublication, LiveResponseBroker,
    LiveResponseBrokerCapability, LiveResponseBrokerError, LiveResponseByteLimits,
    LiveResponseCloseOutcome, LiveResponseDelivery, LiveResponseGap, LiveResponseItemIdentity,
    LiveResponsePayload, LiveResponsePublication, LiveResponsePublishOutcome, LiveResponseSeal,
    LiveResponseSealStatus, LiveResponseSourceIdentity, LiveResponseSubscriber,
    LiveWorkflowObservationIdentity, PublicResponse, PublicResponseError, ResponseContentPart,
    ResponseItemStatus, ResponseObjectKind, ResponseOutputItem, ResponseRole, ResponseStatus,
    ResponseStreamEvent, ResponseStreamEventType, ResponseUsage, ResponseUsageInputDetails,
    ResponseUsageOutputDetails, WorkflowCompleted, WorkflowFailure, WorkflowPublicError,
    WorkflowPublicResultError, WorkflowRetrieval, WorkflowRetrievalMetadata,
    WorkflowRetrievalPublicProjection, WorkflowRetrievalResult, WorkflowStopReason,
    WorkflowStopped, WorkflowStreamGapAction, WorkflowToolCompletedArgumentsProjection,
    WorkflowToolContent, WorkflowToolPublicProjection, WorkflowToolResult, WorkflowUsageStatus,
    MAX_FUNCTION_CALL_ARGUMENT_BYTES, RESPONSE_STREAM_PROTOCOL_VERSION,
};

const LIVE_RESPONSE_CONFIG_INVALID: &str = "LIVE_RESPONSE_CONFIG_INVALID";
const LIVE_RESPONSE_SUBSCRIBER_EXISTS: &str = "LIVE_RESPONSE_SUBSCRIBER_EXISTS";
#[cfg(test)]
const LIVE_RESPONSE_STREAM_CLOSED: &str = "LIVE_RESPONSE_STREAM_CLOSED";

#[derive(Clone)]
pub struct InMemoryLiveResponseBroker {
    inner: Arc<InMemoryBrokerInner>,
}

struct InMemoryBrokerInner {
    body_capacity: usize,
    control_capacity: usize,
    byte_limits: LiveResponseByteLimits,
    runs: Mutex<BTreeMap<RunId, Arc<RunQueue>>>,
}

impl InMemoryLiveResponseBroker {
    pub fn new(
        body_capacity: usize,
        control_capacity: usize,
    ) -> Result<Self, LiveResponseBrokerError> {
        Self::new_with_limits(
            body_capacity,
            control_capacity,
            LiveResponseByteLimits::default(),
        )
    }

    pub fn new_with_limits(
        body_capacity: usize,
        control_capacity: usize,
        byte_limits: LiveResponseByteLimits,
    ) -> Result<Self, LiveResponseBrokerError> {
        if body_capacity == 0 || control_capacity == 0 {
            return Err(LiveResponseBrokerError::new(
                LIVE_RESPONSE_CONFIG_INVALID,
                "live response broker capacities must be non-zero",
            ));
        }
        LiveResponseByteLimits::new(
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

    pub fn byte_limits(&self) -> LiveResponseByteLimits {
        self.inner.byte_limits
    }
}

impl fmt::Debug for InMemoryLiveResponseBroker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let run_count = lock(&self.inner.runs).len();
        formatter
            .debug_struct("InMemoryLiveResponseBroker")
            .field("body_capacity", &self.inner.body_capacity)
            .field("control_capacity", &self.inner.control_capacity)
            .field("byte_limits", &self.inner.byte_limits)
            .field("run_count", &run_count)
            .finish()
    }
}

#[async_trait]
impl LiveResponseBroker for InMemoryLiveResponseBroker {
    fn deployment_capability(&self) -> LiveResponseBrokerCapability {
        LiveResponseBrokerCapability::SingleProcess
    }

    async fn shutdown(&self, _grace: std::time::Duration) -> Result<(), LiveResponseBrokerError> {
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
    ) -> Result<Box<dyn LiveResponseSubscriber>, LiveResponseBrokerError> {
        let mut runs = lock(&self.inner.runs);
        if runs.contains_key(&run_id) {
            return Err(LiveResponseBrokerError::new(
                LIVE_RESPONSE_SUBSCRIBER_EXISTS,
                "a live response subscriber is already registered for this Run",
            ));
        }
        let queue = Arc::new(RunQueue::new_with_limits(
            run_id.clone(),
            self.inner.body_capacity,
            self.inner.control_capacity,
            self.inner.byte_limits,
        ));
        runs.insert(run_id.clone(), Arc::clone(&queue));
        Ok(Box::new(InMemoryLiveResponseSubscriber {
            run_id,
            queue,
            owner: Arc::downgrade(&self.inner),
        }))
    }

    fn publish(&self, publication: LiveResponsePublication) -> LiveResponsePublishOutcome {
        let queue = lock(&self.inner.runs).get(publication.run_id()).cloned();
        match queue {
            Some(queue) => queue.publish(publication),
            None => LiveResponsePublishOutcome::NoSubscriber,
        }
    }

    fn seal(&self, seal: LiveResponseSeal) -> LiveResponsePublishOutcome {
        let queue = lock(&self.inner.runs)
            .get(seal.identity().run_id())
            .cloned();
        match queue {
            Some(queue) => queue.seal(seal),
            None => LiveResponsePublishOutcome::NoSubscriber,
        }
    }

    fn close_run(&self, run_id: &RunId) -> LiveResponseCloseOutcome {
        let queue = lock(&self.inner.runs).remove(run_id);
        queue.map_or_else(LiveResponseCloseOutcome::default, |queue| queue.close())
    }
}

struct InMemoryLiveResponseSubscriber {
    run_id: RunId,
    queue: Arc<RunQueue>,
    owner: Weak<InMemoryBrokerInner>,
}

#[async_trait]
impl LiveResponseSubscriber for InMemoryLiveResponseSubscriber {
    fn run_id(&self) -> &RunId {
        &self.run_id
    }

    async fn recv(&mut self) -> Result<LiveResponseDelivery, LiveResponseBrokerError> {
        self.queue.recv().await
    }
}

impl Drop for InMemoryLiveResponseSubscriber {
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

    fn identity(run_id: &str) -> LiveResponseItemIdentity {
        LiveResponseItemIdentity::new(
            run(run_id),
            ActivationId::new("activation_answer").unwrap(),
            AttemptNo::FIRST,
            1,
            "msg_answer",
            0,
        )
        .unwrap()
    }

    fn workflow_identity(run_id: &str, source_id: &str) -> LiveWorkflowObservationIdentity {
        LiveWorkflowObservationIdentity::new(
            run(run_id),
            ActivationId::new("activation_workflow_observation").unwrap(),
            AttemptNo::FIRST,
            source_id,
        )
        .unwrap()
    }

    fn delta(
        identity: LiveResponseItemIdentity,
        sequence: u64,
        text: &str,
    ) -> LiveResponsePublication {
        LiveResponsePublication::new(
            identity,
            sequence,
            LiveResponsePayload::OutputTextDelta {
                content_index: 0,
                delta: text.to_owned(),
            },
        )
        .unwrap()
    }

    fn tool_started(
        identity: LiveWorkflowObservationIdentity,
        sequence: u64,
        call_id: &str,
    ) -> LiveResponsePublication {
        LiveResponsePublication::new_workflow_observation(
            identity,
            sequence,
            LiveResponsePayload::ToolStarted {
                call_id: call_id.to_owned(),
                tool_name: "lookup".to_owned(),
                arguments: Some(json!({"published": true})),
            },
        )
        .unwrap()
    }

    fn public_wire_bytes(publication: &LiveResponsePublication) -> usize {
        serde_json::to_vec(&publication.clone().into_public_event(u64::MAX))
            .unwrap()
            .len()
    }

    #[tokio::test]
    async fn subscription_is_installed_before_the_first_publication_and_is_live_only() {
        let broker = InMemoryLiveResponseBroker::new(4, 4).unwrap();
        let first_identity = identity("run_first");
        assert_eq!(
            broker.publish(delta(first_identity.clone(), 0, "before subscribe")),
            LiveResponsePublishOutcome::NoSubscriber
        );
        let mut subscriber = broker.subscribe(run("run_first")).await.unwrap();
        assert_eq!(
            broker.publish(delta(first_identity, 0, "first visible")),
            LiveResponsePublishOutcome::Enqueued
        );
        let LiveResponseDelivery::Publication(publication) = subscriber.recv().await.unwrap()
        else {
            panic!("first delivery must be the first post-subscribe publication")
        };
        assert_eq!(publication.local_sequence(), 0);
        assert_eq!(
            publication.payload_type(),
            ResponseStreamEventType::ResponseOutputTextDelta
        );
    }

    #[tokio::test]
    async fn output_items_and_workflow_observations_have_isolated_local_ordering() {
        let broker = InMemoryLiveResponseBroker::new(8, 4).unwrap();
        let run_id = "run_source_ordering";
        let mut subscriber = broker.subscribe(run(run_id)).await.unwrap();
        let item = identity(run_id);
        let first_observation = workflow_identity(run_id, "tool_source_a");
        let second_observation = workflow_identity(run_id, "tool_source_b");

        assert_eq!(
            broker.publish(delta(item, 0, "answer")),
            LiveResponsePublishOutcome::Enqueued
        );
        assert_eq!(
            broker.publish(tool_started(first_observation.clone(), 2, "call_a")),
            LiveResponsePublishOutcome::EnqueuedAfterBestEffortLoss
        );
        assert_eq!(
            broker.publish(tool_started(second_observation, 0, "call_b")),
            LiveResponsePublishOutcome::Enqueued
        );
        assert_eq!(
            broker.publish(tool_started(first_observation, 1, "call_a_replay")),
            LiveResponsePublishOutcome::RejectedOutOfOrder
        );

        for expected in [
            ResponseStreamEventType::ResponseOutputTextDelta,
            ResponseStreamEventType::WorkflowToolStarted,
            ResponseStreamEventType::WorkflowToolStarted,
        ] {
            let LiveResponseDelivery::Publication(publication) = subscriber.recv().await.unwrap()
            else {
                panic!("source-local loss must not synthesize an output-item control frame")
            };
            assert_eq!(publication.payload_type(), expected);
        }
    }

    #[tokio::test]
    async fn workflow_loss_does_not_create_gaps_or_block_an_output_item_seal() {
        let broker = InMemoryLiveResponseBroker::new(1, 4).unwrap();
        let run_id = "run_workflow_best_effort";
        let mut subscriber = broker.subscribe(run(run_id)).await.unwrap();
        let observation = workflow_identity(run_id, "tool_source");
        let item = identity(run_id);

        assert_eq!(
            broker.publish(tool_started(observation.clone(), 0, "call_kept")),
            LiveResponsePublishOutcome::Enqueued
        );
        assert_eq!(
            broker.publish(tool_started(observation, 1, "call_dropped")),
            LiveResponsePublishOutcome::DroppedBestEffort
        );
        let seal = LiveResponseSeal::new(item, None, LiveResponseSealStatus::Completed);
        assert_eq!(
            broker.seal(seal.clone()),
            LiveResponsePublishOutcome::SealEnqueued
        );

        let LiveResponseDelivery::Seal(delivered) = subscriber.recv().await.unwrap() else {
            panic!("workflow observations must not enter the output-item seal barrier")
        };
        assert_eq!(delivered, seal);

        let close = broker.close_run(&run(run_id));
        assert_eq!(close.unknown_tail_gaps(), 0);
        assert_eq!(close.omitted_unknown_tail_gaps(), 0);
        assert!(matches!(
            subscriber.recv().await.unwrap(),
            LiveResponseDelivery::Publication(_)
        ));
        assert_eq!(
            subscriber.recv().await.unwrap_err().code(),
            LIVE_RESPONSE_STREAM_CLOSED
        );
    }

    #[tokio::test]
    async fn missing_any_completed_function_call_frame_yields_a_gap_before_its_seal() {
        for missing_sequence in 0..=CompletedFunctionCallPublication::LAST_LOCAL_SEQUENCE {
            let run_id = format!("run_function_gap_{missing_sequence}");
            let broker = InMemoryLiveResponseBroker::new(8, 8).unwrap();
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
                        LiveResponsePublishOutcome::Enqueued
                            | LiveResponsePublishOutcome::EnqueuedAfterGap
                    ));
                }
            }
            assert_eq!(
                broker.seal(seal.clone()),
                LiveResponsePublishOutcome::SealEnqueued
            );

            let LiveResponseDelivery::Gap(gap) = subscriber.recv().await.unwrap() else {
                panic!("a missing canonical function-call frame must precede the item seal")
            };
            assert_eq!(gap.missing_from(), missing_sequence);
            assert_eq!(gap.missing_to(), Some(missing_sequence));

            for _ in 0..3 {
                assert!(matches!(
                    subscriber.recv().await.unwrap(),
                    LiveResponseDelivery::Publication(_)
                ));
            }
            let LiveResponseDelivery::Seal(delivered) = subscriber.recv().await.unwrap() else {
                panic!("the completed seal must wait for every retained item frame")
            };
            assert_eq!(delivered, seal);
        }
    }

    #[tokio::test]
    async fn bounded_body_lag_is_explicit_and_seal_waits_for_prior_body() {
        let broker = InMemoryLiveResponseBroker::new(1, 4).unwrap();
        let item = identity("run_lag");
        let mut subscriber = broker.subscribe(run("run_lag")).await.unwrap();
        assert_eq!(
            broker.publish(delta(item.clone(), 0, "kept")),
            LiveResponsePublishOutcome::Enqueued
        );
        assert_eq!(
            broker.publish(delta(item.clone(), 1, "dropped-one")),
            LiveResponsePublishOutcome::DroppedWithGap
        );
        assert_eq!(
            broker.publish(delta(item.clone(), 2, "dropped-two")),
            LiveResponsePublishOutcome::DroppedWithGap
        );
        let seal = LiveResponseSeal::new(item, Some(2), LiveResponseSealStatus::Completed);
        assert_eq!(
            broker.seal(seal.clone()),
            LiveResponsePublishOutcome::SealEnqueued
        );

        let LiveResponseDelivery::Gap(gap) = subscriber.recv().await.unwrap() else {
            panic!("control gap must have priority")
        };
        assert_eq!((gap.missing_from(), gap.missing_to()), (1, Some(2)));
        let LiveResponseDelivery::Publication(publication) = subscriber.recv().await.unwrap()
        else {
            panic!("the retained body must precede its seal")
        };
        assert_eq!(publication.local_sequence(), 0);
        let LiveResponseDelivery::Seal(delivered) = subscriber.recv().await.unwrap() else {
            panic!("seal must follow all retained body for its item")
        };
        assert_eq!(delivered, seal);
    }

    #[tokio::test]
    async fn frame_item_and_run_byte_limits_drop_with_explicit_gaps() {
        let frame_item = identity("run_frame_limit");
        let frame = delta(frame_item, 0, "oversize frame");
        let frame_bytes = public_wire_bytes(&frame);
        let frame_broker = InMemoryLiveResponseBroker::new_with_limits(
            4,
            4,
            LiveResponseByteLimits::new(frame_bytes - 1, frame_bytes * 4, frame_bytes * 8).unwrap(),
        )
        .unwrap();
        let mut frame_subscriber = frame_broker
            .subscribe(run("run_frame_limit"))
            .await
            .unwrap();
        assert_eq!(
            frame_broker.publish(frame),
            LiveResponsePublishOutcome::DroppedWithGap
        );
        let LiveResponseDelivery::Gap(frame_gap) = frame_subscriber.recv().await.unwrap() else {
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
        let item_broker = InMemoryLiveResponseBroker::new_with_limits(
            4,
            4,
            LiveResponseByteLimits::new(
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
            LiveResponsePublishOutcome::Enqueued
        );
        assert_eq!(
            item_broker.publish(second),
            LiveResponsePublishOutcome::DroppedWithGap
        );
        let LiveResponseDelivery::Gap(item_gap) = item_subscriber.recv().await.unwrap() else {
            panic!("an exhausted item budget must become a gap")
        };
        assert_eq!(
            (item_gap.missing_from(), item_gap.missing_to()),
            (1, Some(1))
        );

        let first_identity = identity("run_total_limit");
        let second_identity = LiveResponseItemIdentity::new(
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
        let run_broker = InMemoryLiveResponseBroker::new_with_limits(
            4,
            4,
            LiveResponseByteLimits::new(max_frame, max_frame, first_bytes + second_bytes - 1)
                .unwrap(),
        )
        .unwrap();
        let mut run_subscriber = run_broker.subscribe(run("run_total_limit")).await.unwrap();
        assert_eq!(
            run_broker.publish(first),
            LiveResponsePublishOutcome::Enqueued
        );
        assert_eq!(
            run_broker.publish(second),
            LiveResponsePublishOutcome::DroppedWithGap
        );
        let LiveResponseDelivery::Gap(run_gap) = run_subscriber.recv().await.unwrap() else {
            panic!("an exhausted Run budget must become a gap")
        };
        assert_eq!((run_gap.missing_from(), run_gap.missing_to()), (0, Some(0)));
    }

    #[tokio::test]
    async fn skipped_producer_index_creates_a_known_gap_without_dropping_the_later_body() {
        let broker = InMemoryLiveResponseBroker::new(4, 4).unwrap();
        let item = identity("run_skip");
        let mut subscriber = broker.subscribe(run("run_skip")).await.unwrap();
        assert_eq!(
            broker.publish(delta(item, 2, "after missing indices")),
            LiveResponsePublishOutcome::EnqueuedAfterGap
        );
        let LiveResponseDelivery::Gap(gap) = subscriber.recv().await.unwrap() else {
            panic!("skipped producer indices must be explicit")
        };
        assert_eq!((gap.missing_from(), gap.missing_to()), (0, Some(1)));
        let LiveResponseDelivery::Publication(publication) = subscriber.recv().await.unwrap()
        else {
            panic!("the later body remains observable after the gap")
        };
        assert_eq!(publication.local_sequence(), 2);
    }

    #[tokio::test]
    async fn disjoint_known_gaps_are_not_falsely_widened_across_received_indices() {
        let broker = InMemoryLiveResponseBroker::new(4, 4).unwrap();
        let item = identity("run_disjoint_gap");
        let mut subscriber = broker.subscribe(run("run_disjoint_gap")).await.unwrap();
        assert_eq!(
            broker.publish(delta(item.clone(), 2, "received-two")),
            LiveResponsePublishOutcome::EnqueuedAfterGap
        );
        assert_eq!(
            broker.publish(delta(item.clone(), 3, "received-three")),
            LiveResponsePublishOutcome::Enqueued
        );
        assert_eq!(
            broker.publish(delta(item, 5, "received-five")),
            LiveResponsePublishOutcome::EnqueuedAfterGap
        );

        let LiveResponseDelivery::Gap(first) = subscriber.recv().await.unwrap() else {
            panic!("the first known gap must be delivered")
        };
        let LiveResponseDelivery::Gap(second) = subscriber.recv().await.unwrap() else {
            panic!("the disjoint known gap must remain separate")
        };
        assert_eq!((first.missing_from(), first.missing_to()), (0, Some(1)));
        assert_eq!((second.missing_from(), second.missing_to()), (4, Some(4)));
    }

    #[tokio::test]
    async fn close_marks_unsealed_items_with_an_unknown_tail_then_reaches_eof() {
        let broker = InMemoryLiveResponseBroker::new(2, 2).unwrap();
        let item = identity("run_unsealed");
        let mut subscriber = broker.subscribe(run("run_unsealed")).await.unwrap();
        assert_eq!(
            broker.publish(delta(item, 0, "partial")),
            LiveResponsePublishOutcome::Enqueued
        );
        let close = broker.close_run(&run("run_unsealed"));
        assert_eq!(close.unknown_tail_gaps(), 1);
        assert_eq!(close.omitted_unknown_tail_gaps(), 0);

        let LiveResponseDelivery::Gap(gap) = subscriber.recv().await.unwrap() else {
            panic!("unknown tail gap must be delivered before retained body")
        };
        assert!(gap.has_unknown_tail());
        assert_eq!(gap.missing_from(), 1);
        assert!(matches!(
            subscriber.recv().await.unwrap(),
            LiveResponseDelivery::Publication(_)
        ));
        assert_eq!(
            subscriber.recv().await.unwrap_err().code(),
            LIVE_RESPONSE_STREAM_CLOSED
        );
    }

    #[tokio::test]
    async fn runs_are_isolated_and_late_publication_after_seal_is_rejected() {
        let broker = InMemoryLiveResponseBroker::new(2, 2).unwrap();
        let first = identity("run_a");
        let second = identity("run_b");
        let mut subscriber_a = broker.subscribe(run("run_a")).await.unwrap();
        let mut subscriber_b = broker.subscribe(run("run_b")).await.unwrap();
        assert_eq!(
            broker.publish(delta(second, 0, "only b")),
            LiveResponsePublishOutcome::Enqueued
        );
        let LiveResponseDelivery::Publication(delivered_b) = subscriber_b.recv().await.unwrap()
        else {
            panic!("run B must receive its own publication")
        };
        assert_eq!(delivered_b.run_id(), &run("run_b"));

        let seal = LiveResponseSeal::new(first.clone(), None, LiveResponseSealStatus::Completed);
        assert_eq!(
            broker.seal(seal.clone()),
            LiveResponsePublishOutcome::SealEnqueued
        );
        assert_eq!(
            broker.seal(seal),
            LiveResponsePublishOutcome::SealExactReplay
        );
        assert_eq!(
            broker.publish(delta(first, 0, "too late")),
            LiveResponsePublishOutcome::RejectedAfterSeal
        );
        assert!(matches!(
            subscriber_a.recv().await.unwrap(),
            LiveResponseDelivery::Seal(_)
        ));
    }
}
