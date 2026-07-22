use axum::{
    extract::rejection::JsonRejection,
    http::StatusCode,
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

    fn new(status: StatusCode, code: &'static str, message: &'static str) -> Self {
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
            "RUN_CONFLICT" | "RUN_CAPACITY_EXCEEDED" => Self::new(
                StatusCode::CONFLICT,
                "RUN_CONFLICT",
                "run request conflicts with current runtime state",
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
            "RUN_SERVICE_STOPPING" | "RUN_SERVICE_UNAVAILABLE" | "RUN_DEPLOYMENT_UNAVAILABLE" => {
                Self::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "RUN_SERVICE_UNAVAILABLE",
                    "run service is unavailable",
                )
            }
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
        (
            self.status,
            Json(ApiResponse {
                code: self.code,
                message: self.message,
                data: serde_json::json!({}),
            }),
        )
            .into_response()
    }
}
