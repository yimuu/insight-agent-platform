use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::Value;
use thiserror::Error;

use crate::response::{
    ApiResponse, CODE_CONFIG_ERROR, CODE_INPUT_ERROR, CODE_NOT_FOUND, CODE_RUN_ERROR,
    CODE_UNAUTHORIZED, CODE_UPSTREAM_ERROR,
};

#[derive(Debug, Error)]
pub enum AppError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("input error: {0}")]
    Input(String),
    #[error("run error: {0}")]
    Run(String),
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("upstream error: {0}")]
    Upstream(String),
}

impl AppError {
    pub fn api_code(&self) -> i32 {
        match self {
            AppError::Config(_) => CODE_CONFIG_ERROR,
            AppError::Input(_) => CODE_INPUT_ERROR,
            AppError::Run(_) => CODE_RUN_ERROR,
            AppError::Unauthorized(_) => CODE_UNAUTHORIZED,
            AppError::NotFound(_) => CODE_NOT_FOUND,
            AppError::Upstream(_) => CODE_UPSTREAM_ERROR,
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            AppError::Config(message) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                CODE_CONFIG_ERROR,
                message,
            ),
            AppError::Input(message) => (StatusCode::BAD_REQUEST, CODE_INPUT_ERROR, message),
            AppError::Run(message) => (StatusCode::INTERNAL_SERVER_ERROR, CODE_RUN_ERROR, message),
            AppError::Unauthorized(message) => {
                (StatusCode::UNAUTHORIZED, CODE_UNAUTHORIZED, message)
            }
            AppError::NotFound(message) => (StatusCode::NOT_FOUND, CODE_NOT_FOUND, message),
            AppError::Upstream(message) => (StatusCode::BAD_GATEWAY, CODE_UPSTREAM_ERROR, message),
        };
        if status.is_server_error() {
            tracing::error!(status = %status, code, error_message = %message, "request failed");
        } else {
            tracing::warn!(status = %status, code, error_message = %message, "request rejected");
        }
        (status, Json(ApiResponse::new(code, message, Value::Null))).into_response()
    }
}
