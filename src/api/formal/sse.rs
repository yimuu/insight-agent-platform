use std::{convert::Infallible, time::Duration};

use axum::response::sse::{Event, KeepAlive, Sse};
use futures::{stream, Stream};

use crate::{
    events::protocol::{RunEvent, RunEventType},
    runtime::RunSubscription,
};

pub(crate) fn response_stream(
    subscription: RunSubscription,
    keep_alive_interval: Duration,
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
            Err(error) => {
                tracing::debug!(
                    run_id = subscription.run_id,
                    code = error.code(),
                    last_seq = subscription.last_seq(),
                    "formal SSE subscription closed"
                );
                match transport_error(error.code()) {
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
            .interval(keep_alive_interval)
            .text("keep-alive"),
    )
}

fn transport_error(code: &'static str) -> Result<Event, axum::Error> {
    Event::default()
        .event("transport.error")
        .json_data(transport_error_payload(code))
}

fn transport_error_payload(code: &'static str) -> serde_json::Value {
    serde_json::json!({
        "code": code,
        "message": "event stream closed",
        "data": {},
    })
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

#[cfg(test)]
mod tests {
    use super::transport_error_payload;

    #[test]
    fn transport_error_does_not_advertise_recovery() {
        let payload = transport_error_payload("SUBSCRIBER_LAGGED");
        assert_eq!(payload["code"], "SUBSCRIBER_LAGGED");
        assert_eq!(payload["message"], "event stream closed");
        assert_eq!(payload["data"], serde_json::json!({}));
        assert!(payload.get("after_seq").is_none());
        assert!(!payload.to_string().contains("reconnect"));
    }
}
