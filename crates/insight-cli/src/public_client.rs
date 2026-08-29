//! Bounded client for the public Platform `/v1` HTTP contract.
//!
//! This module intentionally knows nothing about PostgreSQL or internal RPC. It validates the
//! public response envelope before returning authority state to the command layer.

use insight_platform_contracts::{
    ApiProblem, OperationViewV1, ResourceId, ResourceKind, Sha256Digest, SpanId, TraceId,
    MAX_FIELD_ERRORS, MAX_SAFE_TEXT_BYTES,
};
use reqwest::{
    blocking::{Client, Response},
    header::{
        HeaderValue, ACCEPT, AUTHORIZATION, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, ETAG,
        IF_MATCH, LOCATION,
    },
    redirect::Policy,
    Method, StatusCode,
};
use serde::{de::DeserializeOwned, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    fmt,
    io::{Read, Write},
    time::{Duration, Instant},
};

const MAX_PUBLIC_REQUEST_BYTES: usize = 1_048_576;
const MAX_PUBLIC_RESPONSE_BYTES: usize = 131_072;
const MAX_PUBLIC_SSE_BYTES: usize = 8_388_608;
const MAX_PUBLIC_SSE_FRAMES: usize = 128;
const JSON_CONTENT_TYPE: &str = "application/json";
const OPERATION_CACHE_CONTROL: &str = "no-store, private, max-age=0";
const IDEMPOTENCY_KEY: &str = "idempotency-key";
const TRACEPARENT: &str = "traceparent";

#[derive(Debug)]
pub enum PublicClientError {
    InvalidConfiguration(&'static str),
    Transport(String),
    InvalidResponse(String),
    OperationTimeout {
        operation_id: String,
        timeout_seconds: u64,
    },
    Problem(Box<ApiProblem>),
}

impl fmt::Display for PublicClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(detail) => formatter.write_str(detail),
            Self::Transport(detail) => write!(formatter, "public API transport failed: {detail}"),
            Self::InvalidResponse(detail) => {
                write!(formatter, "public API returned an invalid response: {detail}")
            }
            Self::OperationTimeout {
                operation_id,
                timeout_seconds,
            } => write!(
                formatter,
                "operation {operation_id} did not become terminal within {timeout_seconds} seconds"
            ),
            Self::Problem(problem) => write!(
                formatter,
                "public API problem status={} code={} request_id={} trace_id={} retryable={} retry_after_ms={} detail={}",
                problem.status,
                problem.code,
                problem.request_id,
                problem.trace_id,
                problem.retryable,
                problem
                    .retry_after_ms
                    .map_or_else(|| "none".to_owned(), |value| value.to_string()),
                problem.detail.as_deref().unwrap_or("none"),
            ),
        }
    }
}

impl std::error::Error for PublicClientError {}

#[derive(Debug)]
pub struct PublicHttpClient {
    client: Client,
    base_url: String,
    bearer_token: String,
}

#[derive(Debug)]
pub struct PublicJsonResponse<T> {
    pub body: T,
    pub etag: String,
    pub location: Option<String>,
    pub trace_id: TraceId,
}

#[derive(Debug)]
pub struct PublicBodyResponse<T> {
    pub body: T,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicBinaryResponse {
    pub content_length: u64,
    pub content_type: String,
    pub etag: String,
    pub content_digest: Sha256Digest,
    pub trace_id: TraceId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicSseFrame<T> {
    pub id: String,
    pub event: String,
    pub data: T,
}

impl PublicHttpClient {
    pub fn new(
        base_url: String,
        bearer_token: String,
        request_timeout: Duration,
    ) -> Result<Self, PublicClientError> {
        if !is_loopback_http_base(&base_url) {
            return Err(PublicClientError::InvalidConfiguration(
                "the local public API endpoint must be an HTTP loopback address",
            ));
        }
        if bearer_token.is_empty()
            || bearer_token.len() > 65_536
            || bearer_token.chars().any(char::is_whitespace)
        {
            return Err(PublicClientError::InvalidConfiguration(
                "the local OIDC access token is invalid",
            ));
        }
        if request_timeout.is_zero() || request_timeout > Duration::from_secs(60) {
            return Err(PublicClientError::InvalidConfiguration(
                "the public API request timeout is outside its closed bounds",
            ));
        }
        let client = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .timeout(request_timeout)
            .user_agent("insight-cli/0.1")
            .build()
            .map_err(|error| PublicClientError::Transport(error.to_string()))?;
        Ok(Self {
            client,
            base_url,
            bearer_token,
        })
    }

    pub fn read_operation(
        &self,
        operation_id: &ResourceId,
    ) -> Result<OperationViewV1, PublicClientError> {
        if operation_id.kind() != ResourceKind::Job {
            return Err(PublicClientError::InvalidConfiguration(
                "operation ID must be a Job ID",
            ));
        }
        let response = self.get_json(&format!("/v1/operations/{operation_id}"), StatusCode::OK)?;
        let operation: OperationViewV1 = response.body;
        operation.validate().map_err(|_| {
            PublicClientError::InvalidResponse("Operation body violates its contract".to_owned())
        })?;
        if &operation.operation_id != operation_id || operation.etag != response.etag {
            return Err(PublicClientError::InvalidResponse(
                "Operation identity or ETag does not match the request".to_owned(),
            ));
        }
        Ok(operation)
    }

    pub fn wait_operation(
        &self,
        operation_id: &ResourceId,
        expected_tenant_id: &ResourceId,
        timeout: Duration,
    ) -> Result<OperationViewV1, PublicClientError> {
        if expected_tenant_id.kind() != ResourceKind::Tenant
            || timeout.is_zero()
            || timeout > Duration::from_secs(3_600)
        {
            return Err(PublicClientError::InvalidConfiguration(
                "Operation wait arguments are outside their closed bounds",
            ));
        }
        let started = Instant::now();
        loop {
            let operation = self.read_operation(operation_id)?;
            if &operation.tenant_id != expected_tenant_id {
                return Err(PublicClientError::InvalidResponse(
                    "Operation tenant does not match the local project".to_owned(),
                ));
            }
            if matches!(
                operation.state,
                insight_platform_contracts::PublicJobState::Succeeded
                    | insight_platform_contracts::PublicJobState::Failed
                    | insight_platform_contracts::PublicJobState::Cancelled
                    | insight_platform_contracts::PublicJobState::TimedOut
                    | insight_platform_contracts::PublicJobState::ReconciliationRequired
            ) {
                return Ok(operation);
            }
            if started.elapsed() >= timeout {
                return Err(PublicClientError::OperationTimeout {
                    operation_id: operation_id.to_string(),
                    timeout_seconds: timeout.as_secs(),
                });
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn get_json<T: DeserializeOwned>(
        &self,
        path: &str,
        expected_status: StatusCode,
    ) -> Result<PublicJsonResponse<T>, PublicClientError> {
        validate_public_path(path)?;
        let response = self
            .client
            .get(format!("{}{}", self.base_url, path))
            .header(ACCEPT, JSON_CONTENT_TYPE)
            .header(AUTHORIZATION, format!("Bearer {}", self.bearer_token))
            .send()
            .map_err(|error| PublicClientError::Transport(error.to_string()))?;
        decode_json_response(response, expected_status, None)
    }

    pub fn get_body_json<T: DeserializeOwned>(
        &self,
        path: &str,
        expected_status: StatusCode,
    ) -> Result<PublicBodyResponse<T>, PublicClientError> {
        validate_public_path(path)?;
        let response = self
            .client
            .get(format!("{}{}", self.base_url, path))
            .header(ACCEPT, JSON_CONTENT_TYPE)
            .header(AUTHORIZATION, format!("Bearer {}", self.bearer_token))
            .send()
            .map_err(|error| PublicClientError::Transport(error.to_string()))?;
        decode_body_response(response, expected_status)
    }

    pub fn get_binary_to_writer<W: Write>(
        &self,
        path: &str,
        maximum_bytes: u64,
        writer: &mut W,
    ) -> Result<PublicBinaryResponse, PublicClientError> {
        if maximum_bytes == 0 || maximum_bytes > 1_073_741_824 {
            return Err(PublicClientError::InvalidConfiguration(
                "binary response limit is outside its closed bounds",
            ));
        }
        validate_public_path(path)?;
        let mut response = self
            .client
            .get(format!("{}{}", self.base_url, path))
            .header(ACCEPT, "application/octet-stream")
            .header(AUTHORIZATION, format!("Bearer {}", self.bearer_token))
            .send()
            .map_err(|error| PublicClientError::Transport(error.to_string()))?;
        let status = response.status();
        let trace_id = required_header(&response, "trace-id")?
            .parse::<TraceId>()
            .map_err(|_| PublicClientError::InvalidResponse("trace-id is invalid".to_owned()))?;
        if status != StatusCode::OK {
            if !status.is_client_error() && !status.is_server_error() {
                return Err(PublicClientError::InvalidResponse(format!(
                    "unexpected HTTP status {status}"
                )));
            }
            require_json_content_type(&response)?;
            return decode_problem(response, status, trace_id);
        }
        if required_header(&response, CACHE_CONTROL.as_str())? != OPERATION_CACHE_CONTROL
            || required_header(&response, "content-disposition")? != "attachment"
        {
            return Err(PublicClientError::InvalidResponse(
                "binary response cache or disposition is invalid".to_owned(),
            ));
        }
        let content_length = required_header(&response, CONTENT_LENGTH.as_str())?
            .parse::<u64>()
            .ok()
            .filter(|length| *length > 0 && *length <= maximum_bytes)
            .ok_or_else(|| {
                PublicClientError::InvalidResponse(
                    "binary response Content-Length is outside its bound".to_owned(),
                )
            })?;
        let content_type = required_header(&response, CONTENT_TYPE.as_str())?;
        if content_type.is_empty()
            || content_type.len() > 255
            || !content_type.is_ascii()
            || content_type.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(PublicClientError::InvalidResponse(
                "binary response Content-Type is invalid".to_owned(),
            ));
        }
        let content_type = content_type.to_owned();
        let etag = required_header(&response, ETAG.as_str())?;
        if etag.len() < 3
            || etag.len() > 128
            || !etag.starts_with('"')
            || !etag.ends_with('"')
            || etag.starts_with("W/")
        {
            return Err(PublicClientError::InvalidResponse(
                "binary response ETag is invalid".to_owned(),
            ));
        }
        let etag = etag.to_owned();
        let mut hasher = Sha256::new();
        let mut written = 0_u64;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = response
                .read(&mut buffer)
                .map_err(|error| PublicClientError::Transport(error.to_string()))?;
            if read == 0 {
                break;
            }
            written = written.checked_add(read as u64).ok_or_else(|| {
                PublicClientError::InvalidResponse("binary response length overflowed".to_owned())
            })?;
            if written > content_length || written > maximum_bytes {
                return Err(PublicClientError::InvalidResponse(
                    "binary response exceeded its declared length".to_owned(),
                ));
            }
            hasher.update(&buffer[..read]);
            writer
                .write_all(&buffer[..read])
                .map_err(|error| PublicClientError::Transport(error.to_string()))?;
        }
        if written != content_length {
            return Err(PublicClientError::InvalidResponse(
                "binary response ended before its declared length".to_owned(),
            ));
        }
        let mut encoded_digest = String::with_capacity(71);
        encoded_digest.push_str("sha256:");
        for byte in hasher.finalize() {
            use std::fmt::Write as _;
            write!(&mut encoded_digest, "{byte:02x}").map_err(|_| {
                PublicClientError::InvalidResponse(
                    "binary response digest could not be represented".to_owned(),
                )
            })?;
        }
        let content_digest = encoded_digest.parse::<Sha256Digest>().map_err(|_| {
            PublicClientError::InvalidResponse(
                "binary response digest could not be represented".to_owned(),
            )
        })?;
        Ok(PublicBinaryResponse {
            content_length,
            content_type,
            etag,
            content_digest,
            trace_id,
        })
    }

    pub fn get_sse_json<T: DeserializeOwned>(
        &self,
        path: &str,
        last_event_id: Option<&str>,
    ) -> Result<Vec<PublicSseFrame<T>>, PublicClientError> {
        validate_public_path(path)?;
        let mut request = self
            .client
            .get(format!("{}{}", self.base_url, path))
            .header(ACCEPT, "text/event-stream")
            .header(AUTHORIZATION, format!("Bearer {}", self.bearer_token));
        if let Some(cursor) = last_event_id {
            request = request.header("last-event-id", bounded_header(cursor, "Last-Event-ID")?);
        }
        let response = request
            .send()
            .map_err(|error| PublicClientError::Transport(error.to_string()))?;
        decode_sse_response(response)
    }

    pub fn post_json<Request: Serialize, ResponseBody: DeserializeOwned>(
        &self,
        path: &str,
        body: &Request,
        expected_status: StatusCode,
        idempotency_key: &str,
        if_match: Option<&str>,
    ) -> Result<PublicJsonResponse<ResponseBody>, PublicClientError> {
        let body = serde_json::to_vec(body).map_err(|_| {
            PublicClientError::InvalidConfiguration("public request body is not serializable")
        })?;
        if body.len() > MAX_PUBLIC_REQUEST_BYTES {
            return Err(PublicClientError::InvalidConfiguration(
                "public request body exceeds the client limit",
            ));
        }
        self.send_mutation(path, Some(body), expected_status, idempotency_key, if_match)
    }

    pub fn post_empty<ResponseBody: DeserializeOwned>(
        &self,
        path: &str,
        expected_status: StatusCode,
        idempotency_key: &str,
        if_match: &str,
    ) -> Result<PublicJsonResponse<ResponseBody>, PublicClientError> {
        self.send_mutation(path, None, expected_status, idempotency_key, Some(if_match))
    }

    fn send_mutation<ResponseBody: DeserializeOwned>(
        &self,
        path: &str,
        body: Option<Vec<u8>>,
        expected_status: StatusCode,
        idempotency_key: &str,
        if_match: Option<&str>,
    ) -> Result<PublicJsonResponse<ResponseBody>, PublicClientError> {
        validate_public_path(path)?;
        let idempotency_key = bounded_header(idempotency_key, "idempotency key")?;
        let expected_trace_id = TraceId::new();
        let span_id = SpanId::new();
        let traceparent = format!("00-{expected_trace_id}-{span_id}-01");
        let mut request = self
            .client
            .request(Method::POST, format!("{}{}", self.base_url, path))
            .header(ACCEPT, JSON_CONTENT_TYPE)
            .header(AUTHORIZATION, format!("Bearer {}", self.bearer_token))
            .header(IDEMPOTENCY_KEY, idempotency_key)
            .header(TRACEPARENT, traceparent);
        if let Some(if_match) = if_match {
            request = request.header(IF_MATCH, bounded_header(if_match, "If-Match")?);
        }
        if let Some(body) = body {
            request = request.header(CONTENT_TYPE, JSON_CONTENT_TYPE).body(body);
        } else {
            request = request.body(Vec::new());
        }
        let response = request
            .send()
            .map_err(|error| PublicClientError::Transport(error.to_string()))?;
        decode_json_response(response, expected_status, Some(expected_trace_id))
    }
}

fn decode_body_response<T: DeserializeOwned>(
    response: Response,
    expected_status: StatusCode,
) -> Result<PublicBodyResponse<T>, PublicClientError> {
    let status = response.status();
    let trace_id = required_header(&response, "trace-id")?
        .parse::<TraceId>()
        .map_err(|_| PublicClientError::InvalidResponse("trace-id is invalid".to_owned()))?;
    require_json_content_type(&response)?;
    if status != expected_status {
        if !status.is_client_error() && !status.is_server_error() {
            return Err(PublicClientError::InvalidResponse(format!(
                "unexpected HTTP status {status}"
            )));
        }
        return decode_problem(response, status, trace_id);
    }
    if required_header(&response, CACHE_CONTROL.as_str())? != OPERATION_CACHE_CONTROL {
        return Err(PublicClientError::InvalidResponse(
            "success cache-control is not the closed private no-store value".to_owned(),
        ));
    }
    let body = read_bounded_body(response)?;
    let body = serde_json::from_slice::<T>(&body).map_err(|_| {
        PublicClientError::InvalidResponse("response body is not closed JSON".to_owned())
    })?;
    Ok(PublicBodyResponse { body })
}

fn decode_sse_response<T: DeserializeOwned>(
    mut response: Response,
) -> Result<Vec<PublicSseFrame<T>>, PublicClientError> {
    let status = response.status();
    let trace_id = required_header(&response, "trace-id")?
        .parse::<TraceId>()
        .map_err(|_| PublicClientError::InvalidResponse("trace-id is invalid".to_owned()))?;
    if status != StatusCode::OK {
        if !status.is_client_error() && !status.is_server_error() {
            return Err(PublicClientError::InvalidResponse(format!(
                "unexpected HTTP status {status}"
            )));
        }
        require_json_content_type(&response)?;
        return decode_problem(response, status, trace_id);
    }
    if required_header(&response, CACHE_CONTROL.as_str())? != OPERATION_CACHE_CONTROL {
        return Err(PublicClientError::InvalidResponse(
            "SSE cache-control is not the closed private no-store value".to_owned(),
        ));
    }
    let content_type = required_header(&response, CONTENT_TYPE.as_str())?;
    if content_type
        .split(';')
        .next()
        .is_none_or(|value| value.trim() != "text/event-stream")
    {
        return Err(PublicClientError::InvalidResponse(
            "SSE content-type is invalid".to_owned(),
        ));
    }
    if response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length > MAX_PUBLIC_SSE_BYTES as u64)
    {
        return Err(PublicClientError::InvalidResponse(
            "SSE page exceeds the client limit".to_owned(),
        ));
    }
    let mut bytes = Vec::new();
    response
        .by_ref()
        .take((MAX_PUBLIC_SSE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| PublicClientError::Transport(error.to_string()))?;
    if bytes.len() > MAX_PUBLIC_SSE_BYTES {
        return Err(PublicClientError::InvalidResponse(
            "SSE page exceeds the client limit".to_owned(),
        ));
    }
    parse_sse_frames(&bytes)
}

fn parse_sse_frames<T: DeserializeOwned>(
    bytes: &[u8],
) -> Result<Vec<PublicSseFrame<T>>, PublicClientError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| PublicClientError::InvalidResponse("SSE page is not UTF-8".to_owned()))?;
    if text.contains('\0') {
        return Err(PublicClientError::InvalidResponse(
            "SSE page contains a null byte".to_owned(),
        ));
    }
    let mut frames = Vec::new();
    let mut id = None;
    let mut event = None;
    let mut data = None;
    for raw_line in text.split('\n').chain(std::iter::once("")) {
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
        if line.is_empty() {
            if id.is_none() && event.is_none() && data.is_none() {
                continue;
            }
            let id = id.take().ok_or_else(|| {
                PublicClientError::InvalidResponse("SSE frame omitted id".to_owned())
            })?;
            let event = event.take().ok_or_else(|| {
                PublicClientError::InvalidResponse("SSE frame omitted event".to_owned())
            })?;
            let data = data.take().ok_or_else(|| {
                PublicClientError::InvalidResponse("SSE frame omitted data".to_owned())
            })?;
            frames.push(PublicSseFrame { id, event, data });
            if frames.len() > MAX_PUBLIC_SSE_FRAMES {
                return Err(PublicClientError::InvalidResponse(
                    "SSE page contains too many frames".to_owned(),
                ));
            }
            continue;
        }
        let (field, value) = line.split_once(':').ok_or_else(|| {
            PublicClientError::InvalidResponse("SSE line has no field separator".to_owned())
        })?;
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "id" if id.is_none() && !value.is_empty() && value.len() <= 4_096 => {
                id = Some(value.to_owned())
            }
            "event" if event.is_none() && !value.is_empty() && value.len() <= 128 => {
                event = Some(value.to_owned())
            }
            "data" if data.is_none() && value.len() <= 262_144 => {
                data = Some(serde_json::from_str::<T>(value).map_err(|_| {
                    PublicClientError::InvalidResponse(
                        "SSE data is not closed event JSON".to_owned(),
                    )
                })?)
            }
            _ => {
                return Err(PublicClientError::InvalidResponse(
                    "SSE frame contains an unknown, duplicate, or oversized field".to_owned(),
                ))
            }
        }
    }
    Ok(frames)
}

fn is_loopback_http_base(value: &str) -> bool {
    let Some(authority) = value.strip_prefix("http://") else {
        return false;
    };
    let Some((host, port)) = authority.rsplit_once(':') else {
        return false;
    };
    matches!(host, "127.0.0.1" | "localhost")
        && !port.is_empty()
        && port.parse::<u16>().is_ok_and(|port| port > 0)
}

fn decode_json_response<T: DeserializeOwned>(
    response: Response,
    expected_status: StatusCode,
    expected_trace_id: Option<TraceId>,
) -> Result<PublicJsonResponse<T>, PublicClientError> {
    let status = response.status();
    let response_trace_id = required_header(&response, "trace-id")?
        .parse::<TraceId>()
        .map_err(|_| PublicClientError::InvalidResponse("trace-id is invalid".to_owned()))?;
    require_json_content_type(&response)?;
    if status != expected_status {
        if !status.is_client_error() && !status.is_server_error() {
            return Err(PublicClientError::InvalidResponse(format!(
                "unexpected HTTP status {status}"
            )));
        }
        return decode_problem(response, status, response_trace_id);
    }
    if expected_trace_id.is_some_and(|expected| expected != response_trace_id) {
        return Err(PublicClientError::InvalidResponse(
            "response trace identity does not match the request traceparent".to_owned(),
        ));
    }
    if required_header(&response, CACHE_CONTROL.as_str())? != OPERATION_CACHE_CONTROL {
        return Err(PublicClientError::InvalidResponse(
            "success cache-control is not the closed private no-store value".to_owned(),
        ));
    }
    let response_etag = required_header(&response, ETAG.as_str())?.to_owned();
    let location = response
        .headers()
        .get(LOCATION)
        .map(|value| {
            value.to_str().map(str::to_owned).map_err(|_| {
                PublicClientError::InvalidResponse("Location header is invalid".to_owned())
            })
        })
        .transpose()?;
    let body = read_bounded_body(response)?;
    let body = serde_json::from_slice::<T>(&body).map_err(|_| {
        PublicClientError::InvalidResponse("response body is not closed JSON".to_owned())
    })?;
    Ok(PublicJsonResponse {
        body,
        etag: response_etag,
        location,
        trace_id: response_trace_id,
    })
}

fn validate_public_path(path: &str) -> Result<(), PublicClientError> {
    if path.len() > 2_048
        || !path.starts_with("/v1/")
        || path.contains("..")
        || path.contains(['?', '#'])
        || !path.is_ascii()
    {
        return Err(PublicClientError::InvalidConfiguration(
            "public API path is outside its closed bounds",
        ));
    }
    Ok(())
}

fn bounded_header(value: &str, purpose: &'static str) -> Result<HeaderValue, PublicClientError> {
    if value.is_empty() || value.len() > 255 || !value.is_ascii() {
        return Err(PublicClientError::InvalidConfiguration(purpose));
    }
    HeaderValue::from_str(value).map_err(|_| PublicClientError::InvalidConfiguration(purpose))
}

fn decode_problem<T>(
    response: Response,
    status: StatusCode,
    response_trace_id: TraceId,
) -> Result<T, PublicClientError> {
    if required_header(&response, CACHE_CONTROL.as_str())? != "no-store" {
        return Err(PublicClientError::InvalidResponse(
            "Problem cache-control is not no-store".to_owned(),
        ));
    }
    let body = read_bounded_body(response)?;
    let problem = serde_json::from_slice::<ApiProblem>(&body).map_err(|_| {
        PublicClientError::InvalidResponse("Problem body is not closed JSON".to_owned())
    })?;
    problem
        .validate(MAX_SAFE_TEXT_BYTES, MAX_FIELD_ERRORS)
        .map_err(|_| {
            PublicClientError::InvalidResponse("Problem body violates its contract".to_owned())
        })?;
    if problem.status != status.as_u16() || problem.trace_id != response_trace_id {
        return Err(PublicClientError::InvalidResponse(
            "Problem status or trace identity does not match its HTTP envelope".to_owned(),
        ));
    }
    Err(PublicClientError::Problem(Box::new(problem)))
}

fn require_json_content_type(response: &Response) -> Result<(), PublicClientError> {
    let content_type = required_header(response, CONTENT_TYPE.as_str())?;
    if content_type
        .split(';')
        .next()
        .is_none_or(|value| value.trim() != JSON_CONTENT_TYPE)
    {
        return Err(PublicClientError::InvalidResponse(
            "content-type is not application/json".to_owned(),
        ));
    }
    Ok(())
}

fn required_header<'a>(response: &'a Response, name: &str) -> Result<&'a str, PublicClientError> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            PublicClientError::InvalidResponse(format!("required {name} header is absent"))
        })
}

fn read_bounded_body(mut response: Response) -> Result<Vec<u8>, PublicClientError> {
    if response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length > MAX_PUBLIC_RESPONSE_BYTES as u64)
    {
        return Err(PublicClientError::InvalidResponse(
            "response body exceeds the public client limit".to_owned(),
        ));
    }
    let mut body = Vec::new();
    response
        .by_ref()
        .take((MAX_PUBLIC_RESPONSE_BYTES + 1) as u64)
        .read_to_end(&mut body)
        .map_err(|error| PublicClientError::Transport(error.to_string()))?;
    if body.len() > MAX_PUBLIC_RESPONSE_BYTES {
        return Err(PublicClientError::InvalidResponse(
            "response body exceeds the public client limit".to_owned(),
        ));
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{
        operation_etag, ApiProblemCode, PublicJobKind, PublicJobState, PublicJobTarget,
        SafeJobResult, Sha256Digest, UtcTimestamp,
    };
    use std::{net::TcpListener, thread};
    use uuid::Uuid;

    fn id(kind: ResourceKind) -> ResourceId {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
    }

    #[test]
    fn operation_response_requires_matching_public_envelope() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let operation_id = id(ResourceKind::Job);
        let resource_id = id(ResourceKind::Policy);
        let operation = OperationViewV1 {
            operation_id: operation_id.clone(),
            tenant_id: id(ResourceKind::Tenant),
            kind: PublicJobKind::ResourceValidation,
            target: PublicJobTarget::ResourceVersion {
                resource_id,
                resource_version: 2,
            },
            state: PublicJobState::Succeeded,
            progress: None,
            result: Some(SafeJobResult {
                result_digest:
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .parse::<Sha256Digest>()
                        .unwrap(),
            }),
            error: None,
            created_at: "2026-08-29T00:00:00.000000Z"
                .parse::<UtcTimestamp>()
                .unwrap(),
            updated_at: "2026-08-29T00:00:01.000000Z"
                .parse::<UtcTimestamp>()
                .unwrap(),
            etag: operation_etag(&operation_id.to_string(), 2),
        };
        let body = serde_json::to_vec(&operation).unwrap();
        let expected_authorization = "authorization: bearer test-token";
        let etag = operation.etag.clone();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 4096];
            let read = stream.read(&mut request).unwrap();
            let request = String::from_utf8_lossy(&request[..read]);
            assert!(request.starts_with(&format!("GET /v1/operations/{operation_id} HTTP/1.1")));
            assert!(request
                .lines()
                .any(|line| line.to_ascii_lowercase() == expected_authorization));
            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncache-control: {OPERATION_CACHE_CONTROL}\r\ntrace-id: 11111111111111111111111111111111\r\netag: {etag}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            )
            .unwrap();
            stream.write_all(&body).unwrap();
        });
        let client = PublicHttpClient::new(
            format!("http://127.0.0.1:{port}"),
            "test-token".to_owned(),
            Duration::from_secs(2),
        )
        .unwrap();
        let response = client.read_operation(&operation.operation_id).unwrap();
        assert_eq!(response, operation);
        server.join().unwrap();
    }

    #[test]
    fn client_rejects_non_loopback_and_whitespace_token() {
        assert!(matches!(
            PublicHttpClient::new(
                "https://example.invalid".to_owned(),
                "token".to_owned(),
                Duration::from_secs(1),
            ),
            Err(PublicClientError::InvalidConfiguration(_))
        ));
        assert!(matches!(
            PublicHttpClient::new(
                "http://127.0.0.1:8080".to_owned(),
                "bad token".to_owned(),
                Duration::from_secs(1),
            ),
            Err(PublicClientError::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn public_problem_preserves_retry_and_trace_identity() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let operation_id = id(ResourceKind::Job);
        let trace_id = "22222222222222222222222222222222"
            .parse::<TraceId>()
            .unwrap();
        let problem = ApiProblem {
            type_uri: "urn:insight:problem:rate_limited".to_owned(),
            title: "Rate limited".to_owned(),
            status: 429,
            code: ApiProblemCode::RateLimited,
            detail: Some("retry the safe read after the bounded delay".to_owned()),
            request_id: id(ResourceKind::ServerRequest),
            trace_id,
            retryable: true,
            retry_after_ms: Some(250),
            field_errors: Vec::new(),
        };
        let body = serde_json::to_vec(&problem).unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 4096];
            let _ = stream.read(&mut request).unwrap();
            write!(
                stream,
                "HTTP/1.1 429 Too Many Requests\r\ncontent-type: application/json\r\ncache-control: no-store\r\ntrace-id: {trace_id}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                body.len()
            )
            .unwrap();
            stream.write_all(&body).unwrap();
        });
        let client = PublicHttpClient::new(
            format!("http://127.0.0.1:{port}"),
            "test-token".to_owned(),
            Duration::from_secs(2),
        )
        .unwrap();
        let error = client.read_operation(&operation_id).unwrap_err();
        match error {
            PublicClientError::Problem(actual) => {
                assert_eq!(*actual, problem);
                assert!(actual.retryable);
                assert_eq!(actual.retry_after_ms, Some(250));
            }
            other => panic!("expected public Problem, got {other:?}"),
        }
        server.join().unwrap();
    }
}
