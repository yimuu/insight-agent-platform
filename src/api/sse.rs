use axum::response::sse::Event;

use crate::{
    engine::event::{RunEvent, RunEventScope, RunEventType},
    error::AppError,
    response::CODE_RUN_ERROR,
};

pub fn encode_event(event: RunEvent) -> Result<Event, AppError> {
    let name = event.event_type.as_str();
    let data = serde_json::to_string(&event)
        .map_err(|err| AppError::Run(format!("failed to encode sse event: {err}")))?;
    Ok(Event::default().event(name).data(data))
}

pub fn encode_event_or_sanitized_error(event: RunEvent, result: Result<Event, AppError>) -> Event {
    match result {
        Ok(encoded) => encoded,
        Err(_) => {
            let fallback = RunEvent::failed(
                RunEventType::RunFailed,
                event.seq,
                RunEventScope {
                    request_id: event.request_id,
                    run_id: event.run_id,
                    agent_id: event.agent_id,
                    step_id: event.step_id,
                },
                CODE_RUN_ERROR,
                "stream encoding failed",
            );
            let data = serde_json::to_string(&fallback)
            .unwrap_or_else(|_| {
                r#"{"type":"run.failed","seq":0,"request_id":"","run_id":"","agent_id":"","time":"1970-01-01T00:00:00Z","code":20000,"message":"stream encoding failed","data":{"status":"failed"}}"#.to_string()
            });
            Event::default()
                .event(RunEventType::RunFailed.as_str())
                .data(data)
        }
    }
}
