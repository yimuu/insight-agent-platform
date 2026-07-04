use axum::response::sse::Event;

use crate::{engine::event::RunEvent, error::AppError};

pub fn encode_event(event: RunEvent) -> Result<Event, AppError> {
    let name = event.kind.as_sse_name();
    let data = serde_json::to_string(&event)
        .map_err(|err| AppError::Run(format!("failed to encode sse event: {err}")))?;
    Ok(Event::default().event(name).data(data))
}
