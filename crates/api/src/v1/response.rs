//! Response envelopes and error mapping for the v1 HTTP API.

use axum::{
    extract::rejection::JsonRejection,
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

use insight_runtime::ServiceError;

#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub code: &'static str,
    pub message: &'static str,
    pub data: T,
}

impl<T> ApiResponse<T> {
    pub fn ok(data: T) -> Self {
        Self {
            code: "OK",
            message: "ok",
            data,
        }
    }
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
}

impl ApiError {
    pub fn input_invalid() -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "INPUT_INVALID",
            "request input is invalid",
        )
    }

    pub fn unauthorized() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "authorization is required",
        )
    }

    pub fn internal() -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL",
            "internal server error",
        )
    }

    pub(crate) fn new(status: StatusCode, code: &'static str, message: &'static str) -> Self {
        Self {
            status,
            code,
            message,
        }
    }
}

impl From<JsonRejection> for ApiError {
    fn from(_rejection: JsonRejection) -> Self {
        Self::input_invalid()
    }
}

impl From<ServiceError> for ApiError {
    fn from(error: ServiceError) -> Self {
        match error.code() {
            "INPUT_INVALID" => Self::input_invalid(),
            "GRAPH_DOCUMENT_INVALID"
            | "GRAPH_PUBLICATION_INVALID"
            | "GRAPH_VIEW_INVALID"
            | "GRAPH_EDIT_INVALID" => Self::new(
                StatusCode::BAD_REQUEST,
                error.code(),
                "graph author request is invalid",
            ),
            "RECOVERY_REQUEST_INVALID" => Self::new(
                StatusCode::BAD_REQUEST,
                "RECOVERY_REQUEST_INVALID",
                "recovery request is invalid",
            ),
            "SIGNAL_INVALID" => Self::new(
                StatusCode::BAD_REQUEST,
                "SIGNAL_INVALID",
                "signal request is invalid",
            ),
            "SIGNAL_TYPE_MISMATCH" => Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "SIGNAL_TYPE_MISMATCH",
                "signal payload does not match the wait contract",
            ),
            "CONVERSATION_REQUEST_INVALID" => Self::new(
                StatusCode::BAD_REQUEST,
                "CONVERSATION_REQUEST_INVALID",
                "conversation request is invalid",
            ),
            "CONVERSATION_INPUT_INVALID" => Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "CONVERSATION_INPUT_INVALID",
                "conversation message does not match the Agent input contract",
            ),
            "AGENT_NOT_FOUND" => {
                Self::new(StatusCode::NOT_FOUND, "AGENT_NOT_FOUND", "agent not found")
            }
            "GRAPH_DOCUMENT_NOT_FOUND"
            | "GRAPH_VIEW_NOT_FOUND"
            | "GRAPH_TRACE_NOT_FOUND"
            | "GRAPH_DEPLOYMENT_NOT_FOUND" => Self::new(
                StatusCode::NOT_FOUND,
                error.code(),
                "graph author resource was not found",
            ),
            "RUN_NOT_FOUND" => Self::new(StatusCode::NOT_FOUND, "RUN_NOT_FOUND", "run not found"),
            "CONVERSATION_NOT_FOUND" | "CONVERSATION_OWNERSHIP_MISMATCH" => Self::new(
                StatusCode::NOT_FOUND,
                "CONVERSATION_NOT_FOUND",
                "conversation not found",
            ),
            "ARTIFACT_NOT_FOUND" => Self::new(
                StatusCode::NOT_FOUND,
                "ARTIFACT_NOT_FOUND",
                "artifact not found",
            ),
            "ARTIFACT_TOO_LARGE" => Self::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "ARTIFACT_TOO_LARGE",
                "artifact exceeds the authorized read size",
            ),
            "RUN_CAPACITY_EXCEEDED" => Self::new(
                StatusCode::TOO_MANY_REQUESTS,
                "RUN_CAPACITY_EXCEEDED",
                "active Run capacity is exhausted",
            ),
            "RUN_CAPABILITY_UNAVAILABLE" => Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "RUN_CAPABILITY_UNAVAILABLE",
                "this run does not persist recovery checkpoints",
            ),
            "RUN_FULL_CONVERSATION_CAPABILITY_UNAVAILABLE" => Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "RUN_CAPABILITY_UNAVAILABLE",
                "Conversation-bound full Runs do not support recovery lineage",
            ),
            "RUN_CONFLICT" => Self::new(
                StatusCode::CONFLICT,
                "RUN_CONFLICT",
                "run request conflicts with current runtime state",
            ),
            "CONVERSATION_ARCHIVED" | "CONVERSATION_CONFLICT" => Self::new(
                StatusCode::CONFLICT,
                error.code(),
                "conversation conflicts with its current state",
            ),
            "GRAPH_PUBLICATION_CONFLICT" | "GRAPH_VIEW_CONFLICT" | "GRAPH_EDIT_CONFLICT" => {
                Self::new(
                    StatusCode::CONFLICT,
                    error.code(),
                    "graph author request conflicts with current state",
                )
            }
            "RECOVERY_CONFLICT" => Self::new(
                StatusCode::CONFLICT,
                "RECOVERY_CONFLICT",
                "recovery request conflicts with the durable source state",
            ),
            "RECOVERY_NOT_READY" => Self::new(
                StatusCode::CONFLICT,
                "RECOVERY_NOT_READY",
                "recovery source is not paused and fully drained",
            ),
            "REDRIVE_REQUIRES_FORK" => Self::new(
                StatusCode::CONFLICT,
                "REDRIVE_REQUIRES_FORK",
                "redrive requires a fork",
            ),
            "RECOVERY_CHECKPOINT_UNAVAILABLE" => Self::new(
                StatusCode::CONFLICT,
                "RECOVERY_CHECKPOINT_UNAVAILABLE",
                "no authoritative scheduler checkpoint is available",
            ),
            "FORK_REVISION_UNAVAILABLE" => Self::new(
                StatusCode::CONFLICT,
                "FORK_REVISION_UNAVAILABLE",
                "selected fork revision is unavailable",
            ),
            "FORK_REVISION_INCOMPATIBLE" => Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "FORK_REVISION_INCOMPATIBLE",
                "selected fork revision has an incompatible interface",
            ),
            "MIGRATION_MAPPING_INCOMPATIBLE" => Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "MIGRATION_MAPPING_INCOMPATIBLE",
                "migration node mapping is incomplete or incompatible",
            ),
            "SIGNAL_NOT_WAITING" => Self::new(
                StatusCode::CONFLICT,
                "SIGNAL_NOT_WAITING",
                "run is not waiting for this signal",
            ),
            "SIGNAL_AMBIGUOUS" => Self::new(
                StatusCode::CONFLICT,
                "SIGNAL_AMBIGUOUS",
                "signal name addresses more than one open wait",
            ),
            "SIGNAL_CONFLICT" => Self::new(
                StatusCode::CONFLICT,
                "SIGNAL_CONFLICT",
                "signal conflicts with the durable wait state",
            ),
            "HUMAN_TASK_REQUEST_INVALID" => Self::new(
                StatusCode::BAD_REQUEST,
                "HUMAN_TASK_REQUEST_INVALID",
                "human task request is invalid",
            ),
            "HUMAN_TASK_IDENTITY_INVALID" => Self::new(
                StatusCode::BAD_REQUEST,
                "HUMAN_TASK_IDENTITY_INVALID",
                "human task identity is invalid",
            ),
            "HUMAN_TASK_IDENTITY_UNAVAILABLE" => Self::new(
                StatusCode::UNAUTHORIZED,
                "HUMAN_TASK_IDENTITY_UNAVAILABLE",
                "human task identity is unavailable",
            ),
            "HUMAN_TASK_NOT_FOUND" => Self::new(
                StatusCode::NOT_FOUND,
                "HUMAN_TASK_NOT_FOUND",
                "human work item was not found",
            ),
            "HUMAN_TASK_CLAIM_CONFLICT" => Self::new(
                StatusCode::CONFLICT,
                "HUMAN_TASK_CLAIM_CONFLICT",
                "human work item cannot be claimed",
            ),
            "HUMAN_TASK_COMPLETION_CONFLICT" => Self::new(
                StatusCode::CONFLICT,
                "HUMAN_TASK_COMPLETION_CONFLICT",
                "human task completion conflicts with its claim",
            ),
            "MCP_INTERACTION_RESPONSE_INVALID" => Self::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "MCP_INTERACTION_RESPONSE_INVALID",
                "interaction response does not match the requested schema",
            ),
            "MCP_INTERACTION_NOT_FOUND" => Self::new(
                StatusCode::NOT_FOUND,
                "MCP_INTERACTION_NOT_FOUND",
                "MCP interaction not found",
            ),
            "MCP_INTERACTION_CONFLICT" => Self::new(
                StatusCode::CONFLICT,
                "MCP_INTERACTION_CONFLICT",
                "MCP interaction conflicts with its current state",
            ),
            "MCP_INTERACTION_UNAVAILABLE" => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "MCP_INTERACTION_UNAVAILABLE",
                "MCP interaction service is unavailable",
            ),
            "MCP_SERVER_DISABLED" => Self::new(
                StatusCode::CONFLICT,
                "MCP_SERVER_DISABLED",
                "an MCP Server required by this Agent is disabled",
            ),
            "RUN_SERVICE_STOPPING"
            | "RUN_SERVICE_UNAVAILABLE"
            | "RUN_DEPLOYMENT_UNAVAILABLE"
            | "TERMINAL_ONLY_UNAVAILABLE" => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "RUN_SERVICE_UNAVAILABLE",
                "run service is unavailable",
            ),
            "CONVERSATION_UNAVAILABLE" => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "CONVERSATION_UNAVAILABLE",
                "conversation service is unavailable",
            ),
            "GRAPH_PUBLICATION_UNAVAILABLE" | "GRAPH_SURFACE_UNAVAILABLE" => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                error.code(),
                "graph author service is unavailable",
            ),
            "UPSTREAM_FAILURE" => Self::new(
                StatusCode::BAD_GATEWAY,
                "UPSTREAM_FAILURE",
                "upstream service failed",
            ),
            _ => Self::internal(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let retry_after = self.code == "RUN_CAPACITY_EXCEEDED";
        let mut response = if self.code == "RUN_CAPABILITY_UNAVAILABLE" {
            (
                self.status,
                Json(serde_json::json!({
                    "error": {
                        "code": self.code,
                        "message": self.message,
                    }
                })),
            )
                .into_response()
        } else {
            (
                self.status,
                Json(ApiResponse {
                    code: self.code,
                    message: self.message,
                    data: serde_json::json!({}),
                }),
            )
                .into_response()
        };
        if retry_after {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use axum::{body::to_bytes, http::StatusCode, response::IntoResponse};
    use insight_runtime::ServiceError;

    use super::ApiError;

    #[tokio::test]
    async fn capacity_exhaustion_is_retryable_429_without_collapsing_conflicts() {
        let capacity =
            ApiError::from(ServiceError::new("RUN_CAPACITY_EXCEEDED", "capacity")).into_response();
        assert_eq!(capacity.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(capacity.headers()["retry-after"], "1");
        let capacity_body = to_bytes(capacity.into_body(), 4096).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&capacity_body).unwrap()["code"],
            "RUN_CAPACITY_EXCEEDED"
        );

        let conflict =
            ApiError::from(ServiceError::new("RUN_CONFLICT", "conflict")).into_response();
        assert_eq!(conflict.status(), StatusCode::CONFLICT);
        assert!(!conflict.headers().contains_key("retry-after"));
        let conflict_body = to_bytes(conflict.into_body(), 4096).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&conflict_body).unwrap()["code"],
            "RUN_CONFLICT"
        );
    }

    #[tokio::test]
    async fn unavailable_terminal_only_capability_is_a_typed_422() {
        let response = ApiError::from(ServiceError::new(
            "RUN_CAPABILITY_UNAVAILABLE",
            "runtime detail must not leak",
        ))
        .into_response();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert!(!response.headers().contains_key("retry-after"));
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::json!({
                "error": {
                    "code": "RUN_CAPABILITY_UNAVAILABLE",
                    "message": "this run does not persist recovery checkpoints",
                }
            })
        );
    }

    #[tokio::test]
    async fn unavailable_full_conversation_capability_is_a_reason_specific_typed_422() {
        let response = ApiError::from(ServiceError::new(
            "RUN_FULL_CONVERSATION_CAPABILITY_UNAVAILABLE",
            "runtime detail must not leak",
        ))
        .into_response();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert!(!response.headers().contains_key("retry-after"));
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::json!({
                "error": {
                    "code": "RUN_CAPABILITY_UNAVAILABLE",
                    "message": "Conversation-bound full Runs do not support recovery lineage",
                }
            })
        );
    }

    #[tokio::test]
    async fn conversation_ownership_mismatch_is_indistinguishable_from_not_found() {
        let response = ApiError::from(ServiceError::new(
            "CONVERSATION_OWNERSHIP_MISMATCH",
            "private ownership detail",
        ))
        .into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = to_bytes(response.into_body(), 4096).await.unwrap();
        let body = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
        assert_eq!(body["code"], "CONVERSATION_NOT_FOUND");
        assert!(!body.to_string().contains("ownership"));
    }
}
