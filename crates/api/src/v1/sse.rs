//! Server-sent event transport for the v1 HTTP API.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    convert::Infallible,
    time::Duration,
};

use axum::response::sse::{Event, KeepAlive, Sse};
use futures::Stream;
use insight_engine::{
    events::protocol::{RunEvent, RunEventType},
    response::{
        DurableResponseSnapshot, LiveResponseDelivery, PublicResponse, ResponseObjectKind,
        ResponseStatus, ResponseStreamEvent, ResponseTerminalKind, WorkflowCompleted,
        WorkflowFailure, WorkflowStopReason, WorkflowStopped, WorkflowStreamGapAction,
    },
    RunId,
};
use insight_runtime::AttachedRun;
use serde::de::DeserializeOwned;
use tokio::{sync::mpsc, time::Instant};
use tokio_stream::wrappers::ReceiverStream;

const OUTBOUND_EVENT_CAPACITY: usize = 32;

pub(crate) fn response_stream(
    attached: AttachedRun,
    keep_alive_interval: Duration,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let outbound_write_timeout = attached.outbound_write_timeout;
    let (sender, receiver) = mpsc::channel(OUTBOUND_EVENT_CAPACITY);
    tokio::spawn(async move {
        let mut dispatcher = ResponseDispatcher::new(attached);
        loop {
            let public_event = tokio::select! {
                _ = sender.closed() => {
                    tracing::debug!(
                        run_id = dispatcher.run_id(),
                        code = "SSE_OUTBOUND_CLOSED",
                        "response-stream client closed the bounded output"
                    );
                    break;
                }
                public_event = dispatcher.next_event() => match public_event {
                    Some(public_event) => public_event,
                    None => break,
                },
            };
            let terminal = public_event.is_terminal();
            let encoded = match encode_event(&public_event) {
                Ok(encoded) => encoded,
                Err(error) => {
                    tracing::error!(
                        run_id = dispatcher.run_id(),
                        code = "SSE_ENCODE_FAILED",
                        error = %error,
                        "response-stream event encoding failed"
                    );
                    break;
                }
            };
            match tokio::time::timeout(outbound_write_timeout, sender.send(Ok(encoded))).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) | Err(_) => {
                    tracing::debug!(
                        run_id = dispatcher.run_id(),
                        code = "SSE_OUTBOUND_UNWRITABLE",
                        "response-stream client stopped accepting bounded output"
                    );
                    break;
                }
            }
            if terminal {
                break;
            }
        }
    });

    Sse::new(ReceiverStream::new(receiver)).keep_alive(
        KeepAlive::new()
            .interval(keep_alive_interval)
            .text("keep-alive"),
    )
}

struct ResponseDispatcher {
    attached: AttachedRun,
    pending: VecDeque<ResponseStreamEvent>,
    next_sequence: u64,
    terminal_snapshot: Option<DurableResponseSnapshot>,
    terminal_barrier_deadline: Option<Instant>,
    live_open: bool,
    seen_sealed_items: BTreeSet<String>,
    seen_unknown_tail_items: BTreeSet<String>,
    seen_item_watermarks: BTreeMap<String, u64>,
    done: bool,
}

impl ResponseDispatcher {
    fn new(attached: AttachedRun) -> Self {
        let response = PublicResponse {
            id: attached.response_id.clone(),
            object: ResponseObjectKind::Response,
            status: ResponseStatus::InProgress,
            output: Vec::new(),
            usage: None,
            error: None,
        };
        Self {
            attached,
            pending: VecDeque::from([
                ResponseStreamEvent::ResponseCreated {
                    sequence_number: 0,
                    response: response.clone(),
                },
                ResponseStreamEvent::ResponseInProgress {
                    sequence_number: 1,
                    response,
                },
            ]),
            next_sequence: 2,
            terminal_snapshot: None,
            terminal_barrier_deadline: None,
            live_open: true,
            seen_sealed_items: BTreeSet::new(),
            seen_unknown_tail_items: BTreeSet::new(),
            seen_item_watermarks: BTreeMap::new(),
            done: false,
        }
    }

    fn run_id(&self) -> &str {
        &self.attached.run_id
    }

    fn allocate_sequence(&mut self) -> u64 {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        sequence
    }

    async fn next_event(&mut self) -> Option<ResponseStreamEvent> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Some(event);
            }
            if self.done {
                return None;
            }
            if self.terminal_snapshot.is_some() {
                if self.live_open {
                    let deadline = self
                        .terminal_barrier_deadline
                        .expect("terminal barrier has a deadline");
                    match tokio::time::timeout_at(deadline, self.attached.live_response.recv())
                        .await
                    {
                        Ok(Ok(delivery)) => {
                            if let Some(event) = self.project_live_delivery(delivery) {
                                return Some(event);
                            }
                            continue;
                        }
                        Ok(Err(_)) | Err(_) => self.live_open = false,
                    }
                }
                self.enqueue_manifest_unknown_tail_gaps();
                if let Some(event) = self.pending.pop_front() {
                    return Some(event);
                }
                let snapshot = self
                    .terminal_snapshot
                    .take()
                    .expect("terminal snapshot is present");
                let sequence = self.allocate_sequence();
                self.done = true;
                return Some(match terminal_event(snapshot, sequence) {
                    Ok(event) => event,
                    Err(()) => protocol_error(
                        sequence,
                        "RESPONSE_SNAPSHOT_INVALID",
                        "terminal response snapshot is invalid",
                    ),
                });
            }

            if self.live_open {
                let durable = &mut self.attached.subscription;
                let live = &mut self.attached.live_response;
                tokio::select! {
                    durable_event = durable.recv() => {
                        match durable_event {
                            Ok(event) if run_event_is_terminal(&event) => {
                                if !self.begin_terminal_barrier().await {
                                    let sequence = self.allocate_sequence();
                                    self.done = true;
                                    return Some(protocol_error(
                                        sequence,
                                        "RESPONSE_SNAPSHOT_UNAVAILABLE",
                                        "terminal response snapshot is unavailable",
                                    ));
                                }
                            }
                            Ok(_) => {}
                            Err(error) => {
                                let sequence = self.allocate_sequence();
                                self.done = true;
                                return Some(protocol_error(
                                    sequence,
                                    error.code(),
                                    "response stream closed before terminal calibration",
                                ));
                            }
                        }
                    }
                    live_event = live.recv() => {
                        match live_event {
                            Ok(delivery) => {
                                if let Some(event) = self.project_live_delivery(delivery) {
                                    return Some(event);
                                }
                            }
                            Err(_) => self.live_open = false,
                        }
                    }
                }
            } else {
                match self.attached.subscription.recv().await {
                    Ok(event) if run_event_is_terminal(&event) => {
                        if !self.begin_terminal_barrier().await {
                            let sequence = self.allocate_sequence();
                            self.done = true;
                            return Some(protocol_error(
                                sequence,
                                "RESPONSE_SNAPSHOT_UNAVAILABLE",
                                "terminal response snapshot is unavailable",
                            ));
                        }
                    }
                    Ok(_) => {}
                    Err(error) => {
                        let sequence = self.allocate_sequence();
                        self.done = true;
                        return Some(protocol_error(
                            sequence,
                            error.code(),
                            "response stream closed before terminal calibration",
                        ));
                    }
                }
            }
        }
    }

    async fn begin_terminal_barrier(&mut self) -> bool {
        let snapshot = match self.attached.subscription.load_response_snapshot().await {
            Ok(snapshot) => snapshot,
            Err(_) => return false,
        };
        if let Ok(run_id) = RunId::new(self.attached.run_id.clone()) {
            let _ = self.attached.live_response_broker.close_run(&run_id);
        }
        self.terminal_snapshot = Some(snapshot);
        self.terminal_barrier_deadline =
            Some(Instant::now() + self.attached.terminal_barrier_timeout);
        true
    }

    fn project_live_delivery(
        &mut self,
        delivery: LiveResponseDelivery,
    ) -> Option<ResponseStreamEvent> {
        match delivery {
            LiveResponseDelivery::Publication(publication) => {
                if let Some(identity) = publication.output_item_identity() {
                    self.seen_item_watermarks
                        .entry(identity.item_id().to_owned())
                        .and_modify(|watermark| {
                            *watermark = (*watermark).max(publication.local_sequence())
                        })
                        .or_insert(publication.local_sequence());
                }
                let sequence = self.allocate_sequence();
                Some(publication.into_public_event(sequence))
            }
            LiveResponseDelivery::Gap(gap) => {
                if gap.has_unknown_tail() {
                    self.seen_unknown_tail_items
                        .insert(gap.identity().item_id().to_owned());
                } else if let Some(missing_to) = gap.missing_to() {
                    self.seen_item_watermarks
                        .entry(gap.identity().item_id().to_owned())
                        .and_modify(|watermark| *watermark = (*watermark).max(missing_to))
                        .or_insert(missing_to);
                }
                let sequence = self.allocate_sequence();
                Some(gap.into_public_event(sequence))
            }
            LiveResponseDelivery::Seal(seal) => {
                self.seen_sealed_items
                    .insert(seal.identity().item_id().to_owned());
                None
            }
        }
    }

    fn enqueue_manifest_unknown_tail_gaps(&mut self) {
        let Some(snapshot) = self.terminal_snapshot.as_ref() else {
            return;
        };
        let Some(items) = snapshot.public_item_manifest().as_array() else {
            return;
        };
        let pending = manifest_items_without_terminal_evidence(
            items,
            &self.seen_sealed_items,
            &self.seen_unknown_tail_items,
            &self.seen_item_watermarks,
        );
        for (item_id, attempt_no, missing_from) in pending {
            self.seen_unknown_tail_items.insert(item_id.clone());
            let sequence_number = self.allocate_sequence();
            self.pending
                .push_back(ResponseStreamEvent::WorkflowStreamGap {
                    sequence_number,
                    item_id,
                    attempt_no,
                    missing_from,
                    missing_to: None,
                    unknown_tail: true,
                    action: WorkflowStreamGapAction::DiscardProvisionalItem,
                });
        }
    }
}

/// A durable item status proves the final snapshot, not delivery of its
/// transient live tail. Before terminal calibration the dispatcher therefore
/// needs either the broker seal or an already delivered unknown-tail gap for
/// every manifest item. A finite gap alone is not terminal evidence.
fn manifest_items_without_terminal_evidence(
    items: &[serde_json::Value],
    sealed_items: &BTreeSet<String>,
    unknown_tail_items: &BTreeSet<String>,
    item_watermarks: &BTreeMap<String, u64>,
) -> Vec<(String, u32, u64)> {
    items
        .iter()
        .filter_map(|item| {
            let item = item.as_object()?;
            let item_id = item.get("item_id")?.as_str()?.to_owned();
            if sealed_items.contains(&item_id) || unknown_tail_items.contains(&item_id) {
                return None;
            }
            let attempt_no = u32::try_from(item.get("attempt_no")?.as_u64()?).ok()?;
            let missing_from = item_watermarks
                .get(&item_id)
                .map_or(0, |watermark| watermark.saturating_add(1));
            Some((item_id, attempt_no, missing_from))
        })
        .collect()
}

fn terminal_event(
    snapshot: DurableResponseSnapshot,
    sequence_number: u64,
) -> Result<ResponseStreamEvent, ()> {
    let response: PublicResponse = decode(snapshot.response())?;
    Ok(match snapshot.terminal_kind() {
        ResponseTerminalKind::Completed => ResponseStreamEvent::ResponseCompleted {
            sequence_number,
            response,
            workflow: decode::<WorkflowCompleted>(snapshot.workflow())?,
        },
        ResponseTerminalKind::Failed => ResponseStreamEvent::ResponseFailed {
            sequence_number,
            response,
            workflow: decode::<WorkflowFailure>(snapshot.workflow())?,
        },
        ResponseTerminalKind::TimedOut => ResponseStreamEvent::WorkflowResponseTimedOut {
            sequence_number,
            response,
            workflow: decode::<WorkflowFailure>(snapshot.workflow())?,
        },
        ResponseTerminalKind::Cancelled => ResponseStreamEvent::WorkflowResponseCancelled {
            sequence_number,
            response,
            workflow: decode_stopped(snapshot.workflow(), WorkflowStopReason::Cancelled)?,
        },
        ResponseTerminalKind::Interrupted => ResponseStreamEvent::WorkflowResponseInterrupted {
            sequence_number,
            response,
            workflow: decode_stopped(snapshot.workflow(), WorkflowStopReason::Interrupted)?,
        },
    })
}

fn decode<T: DeserializeOwned>(value: &serde_json::Value) -> Result<T, ()> {
    serde_json::from_value(value.clone()).map_err(|_| ())
}

fn decode_stopped(
    value: &serde_json::Value,
    expected: WorkflowStopReason,
) -> Result<WorkflowStopped, ()> {
    let stopped: WorkflowStopped = decode(value)?;
    (stopped.reason == expected).then_some(stopped).ok_or(())
}

fn protocol_error(
    sequence_number: u64,
    code: impl Into<String>,
    message: impl Into<String>,
) -> ResponseStreamEvent {
    ResponseStreamEvent::Error {
        sequence_number,
        code: code.into(),
        message: message.into(),
        param: None,
    }
}

fn run_event_is_terminal(event: &RunEvent) -> bool {
    matches!(
        event.event_type,
        RunEventType::RunCompleted
            | RunEventType::RunFailed
            | RunEventType::RunCancelled
            | RunEventType::RunInterrupted
    )
}

fn encode_event(event: &ResponseStreamEvent) -> Result<Event, axum::Error> {
    Event::default()
        .event(event.event_type().as_str())
        .json_data(event)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use serde_json::json;

    use super::{manifest_items_without_terminal_evidence, protocol_error};

    #[test]
    fn response_stream_envelope_has_no_replay_id() {
        let event = protocol_error(7, "BROKER_LOST", "stream observation was lost");
        let encoded = serde_json::to_value(event).unwrap();
        assert_eq!(encoded["type"], "error");
        assert_eq!(encoded["sequence_number"], 7);
        assert!(!encoded.to_string().contains("output_bytes"));
    }

    #[test]
    fn terminal_barrier_requires_live_evidence_even_for_completed_manifest_items() {
        let items = vec![
            json!({"item_id": "msg_completed", "attempt_no": 1, "status": "completed"}),
            json!({"item_id": "msg_incomplete", "attempt_no": 2, "status": "incomplete"}),
            json!({
                "item_id": "msg_unsealed",
                "attempt_no": 3,
                "status": "incomplete_unsealed"
            }),
        ];
        let sealed = BTreeSet::from(["msg_incomplete".to_owned()]);
        let unknown_tail = BTreeSet::from(["msg_unsealed".to_owned()]);

        assert_eq!(
            manifest_items_without_terminal_evidence(
                &items,
                &sealed,
                &unknown_tail,
                &BTreeMap::from([("msg_completed".to_owned(), 7)]),
            ),
            vec![("msg_completed".to_owned(), 1, 8)]
        );
    }
}
