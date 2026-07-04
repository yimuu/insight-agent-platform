use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("input error: {0}")]
    Input(String),
    #[error("run error: {0}")]
    Run(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("upstream error: {0}")]
    Upstream(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            AppError::Config(message) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "config_error", message)
            }
            AppError::Input(message) => (StatusCode::BAD_REQUEST, "input_error", message),
            AppError::Run(message) => (StatusCode::INTERNAL_SERVER_ERROR, "run_error", message),
            AppError::NotFound(message) => (StatusCode::NOT_FOUND, "not_found", message),
            AppError::Upstream(message) => (StatusCode::BAD_GATEWAY, "upstream_error", message),
        };
        (
            status,
            Json(json!({ "error": { "code": code, "message": message } })),
        )
            .into_response()
    }
}
