use axum::response::sse::Event;

use crate::{
    engine::event::RunEvent,
    error::AppError,
    response::{ApiResponse, CODE_RUN_ERROR},
};

pub fn encode_event(event: RunEvent) -> Result<Event, AppError> {
    let name = event.event.as_sse_name();
    let data = serde_json::to_string(&ApiResponse::new(event.code, event.message.clone(), event))
        .map_err(|err| AppError::Run(format!("failed to encode sse event: {err}")))?;
    Ok(Event::default().event(name).data(data))
}

pub fn encode_event_or_sanitized_error(event: RunEvent, result: Result<Event, AppError>) -> Event {
    match result {
        Ok(encoded) => encoded,
        Err(_) => {
            let fallback = RunEvent::error(
                event.request_id,
                event.run_id,
                event.agent_id,
                event.step_id,
                CODE_RUN_ERROR,
                "stream encoding failed",
            );
            let data = serde_json::to_string(&ApiResponse::new(
                fallback.code,
                fallback.message.clone(),
                fallback,
            ))
            .unwrap_or_else(|_| {
                r#"{"code":20000,"message":"stream encoding failed","data":null}"#.to_string()
            });
            Event::default().event("error").data(data)
        }
    }
}
