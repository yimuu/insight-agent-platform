use axum::{
    extract::rejection::JsonRejection,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;

use crate::runtime::ServiceError;

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
            "AGENT_NOT_FOUND" => {
                Self::new(StatusCode::NOT_FOUND, "AGENT_NOT_FOUND", "agent not found")
            }
            "RUN_NOT_FOUND" => Self::new(StatusCode::NOT_FOUND, "RUN_NOT_FOUND", "run not found"),
            "RUN_CONFLICT" | "RUN_CAPACITY_EXCEEDED" | "RUN_SERVICE_STOPPING" => Self::new(
                StatusCode::CONFLICT,
                "RUN_CONFLICT",
                "run request conflicts with current runtime state",
            ),
            "RUN_SERVICE_UNAVAILABLE" => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "RUN_SERVICE_UNAVAILABLE",
                "run service is unavailable",
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
