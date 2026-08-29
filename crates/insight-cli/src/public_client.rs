//! Bounded client for the public Platform `/v1` HTTP contract.
//!
//! This module intentionally knows nothing about PostgreSQL or internal RPC. It validates the
//! public response envelope before returning authority state to the command layer.

use insight_platform_contracts::{
    ApiProblem, OperationViewV1, ResourceId, ResourceKind, TraceId, MAX_FIELD_ERRORS,
    MAX_SAFE_TEXT_BYTES,
};
use reqwest::{
    blocking::{Client, Response},
    header::{ACCEPT, AUTHORIZATION, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, ETAG},
    redirect::Policy,
    StatusCode,
};
use std::{fmt, io::Read, time::Duration};

const MAX_PUBLIC_RESPONSE_BYTES: usize = 131_072;
const JSON_CONTENT_TYPE: &str = "application/json";
const OPERATION_CACHE_CONTROL: &str = "no-store, private, max-age=0";

#[derive(Debug)]
pub enum PublicClientError {
    InvalidConfiguration(&'static str),
    Transport(String),
    InvalidResponse(String),
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
        let response = self
            .client
            .get(format!("{}/v1/operations/{operation_id}", self.base_url))
            .header(ACCEPT, JSON_CONTENT_TYPE)
            .header(AUTHORIZATION, format!("Bearer {}", self.bearer_token))
            .send()
            .map_err(|error| PublicClientError::Transport(error.to_string()))?;
        decode_operation_response(response, operation_id)
    }
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

fn decode_operation_response(
    response: Response,
    expected_operation_id: &ResourceId,
) -> Result<OperationViewV1, PublicClientError> {
    let status = response.status();
    let response_trace_id = required_header(&response, "trace-id")?
        .parse::<TraceId>()
        .map_err(|_| PublicClientError::InvalidResponse("trace-id is invalid".to_owned()))?;
    require_json_content_type(&response)?;
    if status != StatusCode::OK {
        return decode_problem(response, status, response_trace_id);
    }
    if required_header(&response, CACHE_CONTROL.as_str())? != OPERATION_CACHE_CONTROL {
        return Err(PublicClientError::InvalidResponse(
            "Operation cache-control is not the closed private no-store value".to_owned(),
        ));
    }
    let response_etag = required_header(&response, ETAG.as_str())?.to_owned();
    let body = read_bounded_body(response)?;
    let operation = serde_json::from_slice::<OperationViewV1>(&body).map_err(|_| {
        PublicClientError::InvalidResponse("Operation body is not closed JSON".to_owned())
    })?;
    operation.validate().map_err(|_| {
        PublicClientError::InvalidResponse("Operation body violates its contract".to_owned())
    })?;
    if &operation.operation_id != expected_operation_id || operation.etag != response_etag {
        return Err(PublicClientError::InvalidResponse(
            "Operation identity or ETag does not match the request".to_owned(),
        ));
    }
    Ok(operation)
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
    use std::{io::Write as _, net::TcpListener, thread};
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
