use axum::response::sse::Event;
use serde_json::json;

use crate::{
    engine::event::{RunEvent, RunEventKind},
    error::AppError,
};

pub fn encode_event(event: RunEvent) -> Result<Event, AppError> {
    let name = event.kind.as_sse_name();
    let data = serde_json::to_string(&event)
        .map_err(|err| AppError::Run(format!("failed to encode sse event: {err}")))?;
    Ok(Event::default().event(name).data(data))
}

pub fn encode_event_or_sanitized_error(event: RunEvent, result: Result<Event, AppError>) -> Event {
    match result {
        Ok(encoded) => encoded,
        Err(_) => {
            let fallback = RunEvent {
                kind: RunEventKind::Error,
                run_id: event.run_id,
                agent_id: event.agent_id,
                step_id: event.step_id,
                timestamp: event.timestamp,
                payload: json!({ "message": "stream encoding failed" }),
            };
            let data = serde_json::to_string(&fallback).unwrap_or_else(|_| {
                "{\"kind\":\"error\",\"payload\":{\"message\":\"stream encoding failed\"}}"
                    .to_string()
            });
            Event::default().event("error").data(data)
        }
    }
}
