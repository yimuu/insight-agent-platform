use std::{convert::Infallible, time::Duration};

use axum::response::sse::{Event, KeepAlive, Sse};
use futures::{stream, Stream};

use crate::{
    events::protocol::{RunEvent, RunEventType},
    runtime::RunSubscription,
};

pub(crate) fn response_stream(
    subscription: RunSubscription,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = stream::unfold(Some(subscription), |state| async move {
        let mut subscription = state?;
        match subscription.recv().await {
            Ok(event) => {
                let terminal = is_terminal(event.event_type);
                match encode_event(&event) {
                    Ok(encoded) => Some((Ok(encoded), (!terminal).then_some(subscription))),
                    Err(error) => {
                        tracing::error!(
                            run_id = subscription.run_id,
                            code = "SSE_ENCODE_FAILED",
                            error = %error,
                            "formal SSE event encoding failed"
                        );
                        None
                    }
                }
            }
            Err(crate::events::hub::EventError::ReplayFinished) => None,
            Err(error) => {
                tracing::debug!(
                    run_id = subscription.run_id,
                    code = error.code(),
                    last_seq = subscription.last_seq(),
                    "formal SSE subscription closed"
                );
                match transport_error(error.code(), subscription.last_seq()) {
                    Ok(event) => Some((Ok(event), None)),
                    Err(encoding_error) => {
                        tracing::error!(
                            run_id = subscription.run_id,
                            code = "SSE_ENCODE_FAILED",
                            error = %encoding_error,
                            "formal SSE transport error encoding failed"
                        );
                        None
                    }
                }
            }
        }
    });
    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

fn transport_error(code: &'static str, after_seq: u64) -> Result<Event, axum::Error> {
    Event::default()
        .event("transport.error")
        .json_data(serde_json::json!({
            "code": code,
            "message": "event stream closed; reconnect with after_seq",
            "data": {"after_seq": after_seq},
        }))
}

fn encode_event(event: &RunEvent) -> Result<Event, axum::Error> {
    Event::default()
        .id(event.seq.to_string())
        .event(event.event_type.as_str())
        .json_data(event)
}

fn is_terminal(event_type: RunEventType) -> bool {
    matches!(
        event_type,
        RunEventType::RunCompleted
            | RunEventType::RunFailed
            | RunEventType::RunCancelled
            | RunEventType::RunInterrupted
    )
}
