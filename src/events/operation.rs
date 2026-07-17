use serde_json::json;

use crate::runtime::{RunError, RunErrorKind};

use super::{
    hub::{EventError, EventHub},
    protocol::{RunEvent, RunEventScope, RunEventType},
};

/// Stable identity shared by every public event for one operation attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationEventScope {
    pub run: RunEventScope,
    pub operation_id: String,
    pub operation_type: String,
    pub attempt: u32,
}

impl OperationEventScope {
    pub fn new(
        run: RunEventScope,
        operation_id: impl Into<String>,
        operation_type: impl Into<String>,
        attempt: u32,
    ) -> Self {
        Self {
            run,
            operation_id: operation_id.into(),
            operation_type: operation_type.into(),
            attempt,
        }
    }
}

/// Durable public event publisher for vNext leaf-operation lifecycle metadata.
///
/// Operation content and outputs never enter this publisher. Intermediate
/// values stay runtime-local and can enter another operation only through an
/// explicit typed projection; only the workflow root result is durable.
#[derive(Clone)]
pub struct OperationEventPublisher {
    events: EventHub,
}

impl OperationEventPublisher {
    pub fn new(events: EventHub) -> Self {
        Self { events }
    }

    pub async fn started(&self, scope: &OperationEventScope) -> Result<RunEvent, EventError> {
        self.events
            .publish(
                run_scope(scope),
                RunEventType::OperationStarted,
                json!({
                    "operation_id": scope.operation_id,
                    "operation_type": scope.operation_type,
                    "attempt": scope.attempt,
                }),
            )
            .await
    }

    pub async fn completed(
        &self,
        scope: &OperationEventScope,
        elapsed_ms: u64,
        output_bytes: usize,
    ) -> Result<RunEvent, EventError> {
        self.events
            .publish(
                run_scope(scope),
                RunEventType::OperationCompleted,
                json!({
                    "operation_id": scope.operation_id,
                    "operation_type": scope.operation_type,
                    "attempt": scope.attempt,
                    "elapsed_ms": elapsed_ms,
                    "output_bytes": output_bytes,
                }),
            )
            .await
    }

    pub async fn failed(
        &self,
        scope: &OperationEventScope,
        elapsed_ms: u64,
        error: &RunError,
    ) -> Result<RunEvent, EventError> {
        self.events
            .publish_error(
                run_scope(scope),
                RunEventType::OperationFailed,
                error.code(),
                "operation failed",
                json!({
                    "operation_id": scope.operation_id,
                    "operation_type": scope.operation_type,
                    "attempt": scope.attempt,
                    "elapsed_ms": elapsed_ms,
                    "error_kind": error_kind(error.kind()),
                }),
            )
            .await
    }
}

fn run_scope(scope: &OperationEventScope) -> RunEventScope {
    scope.run.clone()
}

fn error_kind(kind: RunErrorKind) -> &'static str {
    match kind {
        RunErrorKind::Operation => "operation",
        RunErrorKind::Timeout => "timeout",
        RunErrorKind::Stop => "stop",
        RunErrorKind::Infrastructure => "infrastructure",
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::protocol::{RunEvent, RunEventScope, RunEventType};

    #[test]
    fn operation_event_names_round_trip_and_completion_has_no_output_value() {
        for (event_type, name) in [
            (RunEventType::OperationStarted, "operation.started"),
            (RunEventType::OperationCompleted, "operation.completed"),
            (RunEventType::OperationFailed, "operation.failed"),
        ] {
            assert_eq!(event_type.as_str(), name);
            assert_eq!(RunEventType::parse(name), Some(event_type));
        }

        let event = RunEvent::ok(
            RunEventType::OperationCompleted,
            4,
            RunEventScope::for_run("request", "run", "agent", "version"),
            json!({
                "operation_id":"/workflow/analyze#Authored",
                "operation_type":"ai.chat",
                "attempt":1,
                "elapsed_ms":12,
                "output_bytes":128
            }),
        );
        let value = serde_json::to_value(event).unwrap();
        assert!(value["data"].get("output").is_none());
        assert!(value["data"].get("message").is_none());
    }
}
