//! Principal-scoped File Service HTTP surface.

use std::sync::Arc;

use axum::{
    body::Body,
    extract::{rejection::JsonRejection, Path, State},
    http::{header, HeaderMap, HeaderName, HeaderValue, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use insight_engine::file_store::{
    CreateFileRequest, FilePrincipal, FileServiceApi, FileServiceError,
};

use super::{
    auth::ApiAuth,
    response::{ApiError, ApiResponse},
};

const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");
const X_TENANT_ID: HeaderName = HeaderName::from_static("x-tenant-id");
const X_USER_ID: HeaderName = HeaderName::from_static("x-user-id");
const X_FILE_ID: HeaderName = HeaderName::from_static("x-file-id");
const IDEMPOTENCY_KEY: HeaderName = HeaderName::from_static("idempotency-key");

#[derive(Clone)]
pub struct FileApiState {
    pub auth: ApiAuth,
    pub service: Arc<dyn FileServiceApi>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyRequest {}

pub fn build_file_router(state: FileApiState) -> Router {
    let state = Arc::new(state);
    let auth = state.auth.clone();
    Router::new()
        .route("/v1/files", post(create_file))
        .route("/v1/files/{file_id}", get(get_file).delete(delete_file))
        .route("/v1/files/{file_id}/complete", post(complete_file))
        .route(
            "/v1/files/{file_id}/download-url",
            post(create_download_url),
        )
        .route_layer(middleware::from_fn(
            move |headers: HeaderMap, request: Request<Body>, next: Next| {
                let auth = auth.clone();
                async move {
                    if !auth.accepts(&headers) {
                        return Err(ApiError::unauthorized());
                    }
                    if ambiguous_header(&headers, &X_TENANT_ID)
                        || ambiguous_header(&headers, &X_USER_ID)
                        || ambiguous_header(&headers, &X_REQUEST_ID)
                        || ambiguous_header(&headers, &IDEMPOTENCY_KEY)
                    {
                        return Err(ApiError::input_invalid());
                    }
                    Ok::<Response, ApiError>(next.run(request).await)
                }
            },
        ))
        .with_state(state)
}

async fn create_file(
    State(state): State<Arc<FileApiState>>,
    headers: HeaderMap,
    request: Result<Json<CreateFileRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(request) = request.map_err(|_| file_request_invalid())?;
    let principal = principal(&headers)?;
    let request_id = request_id(&headers)?;
    let idempotency_key = optional_header(&headers, &IDEMPOTENCY_KEY)?;
    let outcome = state
        .service
        .create_file(&principal, request, idempotency_key.as_deref())
        .await
        .map_err(file_error)?;
    let status = if outcome.replayed {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    file_response(status, outcome, &request_id, None)
}

async fn complete_file(
    State(state): State<Arc<FileApiState>>,
    Path(file_id): Path<String>,
    headers: HeaderMap,
    request: Result<Json<EmptyRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(_request) = request.map_err(|_| file_request_invalid())?;
    let principal = principal(&headers)?;
    let request_id = request_id(&headers)?;
    let file = state
        .service
        .complete_file(&principal, &file_id)
        .await
        .map_err(file_error)?;
    file_response(StatusCode::OK, file, &request_id, Some(&file_id))
}

async fn get_file(
    State(state): State<Arc<FileApiState>>,
    Path(file_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let principal = principal(&headers)?;
    let request_id = request_id(&headers)?;
    let file = state
        .service
        .get_file(&principal, &file_id)
        .await
        .map_err(file_error)?;
    file_response(StatusCode::OK, file, &request_id, Some(&file_id))
}

async fn create_download_url(
    State(state): State<Arc<FileApiState>>,
    Path(file_id): Path<String>,
    headers: HeaderMap,
    request: Result<Json<EmptyRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(_request) = request.map_err(|_| file_request_invalid())?;
    let principal = principal(&headers)?;
    let request_id = request_id(&headers)?;
    let capability = state
        .service
        .create_download(&principal, &file_id)
        .await
        .map_err(file_error)?;
    file_response(StatusCode::OK, capability, &request_id, Some(&file_id))
}

async fn delete_file(
    State(state): State<Arc<FileApiState>>,
    Path(file_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let principal = principal(&headers)?;
    let request_id = request_id(&headers)?;
    state
        .service
        .delete_file(&principal, &file_id)
        .await
        .map_err(file_error)?;
    file_response(
        StatusCode::OK,
        json!({"file_id": file_id, "deleting": true}),
        &request_id,
        None,
    )
}

fn file_response<T: serde::Serialize>(
    status: StatusCode,
    data: T,
    request_id: &str,
    file_id: Option<&str>,
) -> Result<Response, ApiError> {
    let mut response = (status, Json(ApiResponse::ok(data))).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    insert_header(&mut response, &X_REQUEST_ID, request_id)?;
    if let Some(file_id) = file_id {
        insert_header(&mut response, &X_FILE_ID, file_id)?;
    }
    Ok(response)
}

fn principal(headers: &HeaderMap) -> Result<FilePrincipal, ApiError> {
    FilePrincipal::new(
        required_header(headers, &X_TENANT_ID)?,
        required_header(headers, &X_USER_ID)?,
    )
    .map_err(file_error)
}

fn request_id(headers: &HeaderMap) -> Result<String, ApiError> {
    Ok(optional_header(headers, &X_REQUEST_ID)?
        .unwrap_or_else(|| format!("req_{}", Uuid::new_v4().simple())))
}

fn required_header(headers: &HeaderMap, name: &HeaderName) -> Result<String, ApiError> {
    optional_header(headers, name)?.ok_or_else(ApiError::input_invalid)
}

fn optional_header(headers: &HeaderMap, name: &HeaderName) -> Result<Option<String>, ApiError> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(ApiError::input_invalid());
    }
    let value = value
        .to_str()
        .map_err(|_| ApiError::input_invalid())?
        .trim();
    if value.is_empty()
        || value.len() > 256
        || value.contains(',')
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(ApiError::input_invalid());
    }
    Ok(Some(value.to_owned()))
}

fn ambiguous_header(headers: &HeaderMap, name: &HeaderName) -> bool {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return false;
    };
    values.next().is_some() || value.to_str().map_or(true, |value| value.contains(','))
}

fn insert_header(response: &mut Response, name: &HeaderName, value: &str) -> Result<(), ApiError> {
    let value = HeaderValue::from_str(value).map_err(|_| ApiError::internal())?;
    response.headers_mut().insert(name.clone(), value);
    Ok(())
}

fn file_error(error: FileServiceError) -> ApiError {
    let status = match error.code() {
        "FILE_REQUEST_INVALID" => StatusCode::BAD_REQUEST,
        "FILE_NOT_FOUND" => StatusCode::NOT_FOUND,
        "FILE_TOO_LARGE" => StatusCode::PAYLOAD_TOO_LARGE,
        "FILE_NOT_READY"
        | "FILE_UPLOAD_EXPIRED"
        | "FILE_UPLOAD_FAILED"
        | "FILE_UPLOAD_INCOMPLETE"
        | "FILE_CONTENT_MISMATCH"
        | "FILE_IDENTITY_CHANGED" => StatusCode::CONFLICT,
        "FILE_LIMIT_EXCEEDED" => StatusCode::UNPROCESSABLE_ENTITY,
        "IDEMPOTENCY_KEY_REUSED" => StatusCode::CONFLICT,
        "FILE_SERVICE_UNAVAILABLE" | "OBJECT_STORAGE_UNAVAILABLE" => {
            StatusCode::SERVICE_UNAVAILABLE
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    ApiError::new(status, error.code(), error.message())
}

fn file_request_invalid() -> ApiError {
    ApiError::new(
        StatusCode::BAD_REQUEST,
        "FILE_REQUEST_INVALID",
        "file request is invalid",
    )
}
