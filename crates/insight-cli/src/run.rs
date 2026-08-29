//! Closed public Run lifecycle client for the local Runtime Gateway.

use crate::public_client::{PublicClientError, PublicHttpClient, PublicJsonResponse};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, DataClassification, JsonLimits, ResourceId, ResourceKind,
    RunState, Sha256Digest, UtcTimestamp, ValueRef,
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr as _};

const MAX_RUN_REQUEST_BYTES: usize = 1_048_576;

#[derive(Debug)]
pub enum RunClientError {
    InvalidRequest(String),
    InvalidResponse(String),
    Public(PublicClientError),
}

impl fmt::Display for RunClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest(detail) => write!(formatter, "Run request is invalid: {detail}"),
            Self::InvalidResponse(detail) => {
                write!(formatter, "Run authority response is invalid: {detail}")
            }
            Self::Public(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for RunClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Public(error) => Some(error),
            _ => None,
        }
    }
}

impl From<PublicClientError> for RunClientError {
    fn from(value: PublicClientError) -> Self {
        Self::Public(value)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateRunInputV1 {
    pub classification: DataClassification,
    pub schema_digest: Sha256Digest,
    pub value: ValueRef,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateRunRequestV1 {
    pub agent_id: ResourceId,
    pub input: CreateRunInputV1,
    pub deadline: UtcTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunViewV1 {
    pub schema_version: u32,
    pub run_id: ResourceId,
    pub agent_deployment_id: ResourceId,
    pub state: RunState,
    pub version: u64,
    pub input_value_id: ResourceId,
    pub output_value_id: Option<ResourceId>,
    pub pause_generation: u64,
    pub cancel_generation: u64,
    pub deadline: UtcTimestamp,
    pub started_at: Option<UtcTimestamp>,
    pub terminal_at: Option<UtcTimestamp>,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunResultViewV1 {
    pub schema_version: u32,
    pub run_id: ResourceId,
    pub value_id: ResourceId,
    pub classification: DataClassification,
    pub schema_digest: Sha256Digest,
    pub content_digest: Sha256Digest,
    pub value: ValueRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunControlAction {
    Pause,
    Resume,
    Cancel,
}

impl RunControlAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pause => "pause",
            Self::Resume => "resume",
            Self::Cancel => "cancel",
        }
    }
}

pub fn create_run(
    client: &PublicHttpClient,
    request_bytes: &[u8],
) -> Result<RunViewV1, RunClientError> {
    let (request, digest) = parse_create_request(request_bytes)?;
    let receipt = format!(
        "insight-run-v1-{}-create",
        digest
            .to_string()
            .strip_prefix("sha256:")
            .unwrap_or("invalid-digest")
    );
    let response: PublicJsonResponse<RunViewV1> =
        client.post_json("/v1/runs", &request, StatusCode::CREATED, &receipt, None)?;
    validate_run_response(&response, None)?;
    let expected_location = format!("/v1/runs/{}", response.body.run_id);
    if response.location.as_deref() != Some(expected_location.as_str()) {
        return Err(RunClientError::InvalidResponse(
            "Location does not identify the created Run".to_owned(),
        ));
    }
    Ok(response.body)
}

pub fn read_run(
    client: &PublicHttpClient,
    run_id: &ResourceId,
) -> Result<RunViewV1, RunClientError> {
    require_run_id(run_id)?;
    let response: PublicJsonResponse<RunViewV1> =
        client.get_json(&format!("/v1/runs/{run_id}"), StatusCode::OK)?;
    validate_run_response(&response, Some(run_id))?;
    Ok(response.body)
}

pub fn control_run(
    client: &PublicHttpClient,
    run_id: &ResourceId,
    action: RunControlAction,
) -> Result<RunViewV1, RunClientError> {
    let current = read_run(client, run_id)?;
    let receipt_digest = canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "operation": format!("run.{}", action.as_str()),
        "run_id": run_id,
        "if_match": current.etag,
    }))
    .map_err(|error| RunClientError::InvalidRequest(error.to_string()))?;
    let receipt = format!(
        "insight-run-v1-{}-{}",
        receipt_digest
            .strip_prefix("sha256:")
            .unwrap_or(&receipt_digest),
        action.as_str()
    );
    let response: PublicJsonResponse<RunViewV1> = client.post_empty(
        &format!("/v1/runs/{run_id}:{}", action.as_str()),
        StatusCode::OK,
        &receipt,
        &current.etag,
    )?;
    validate_run_response(&response, Some(run_id))?;
    Ok(response.body)
}

pub fn read_run_result(
    client: &PublicHttpClient,
    run_id: &ResourceId,
) -> Result<RunResultViewV1, RunClientError> {
    require_run_id(run_id)?;
    let response = client
        .get_body_json::<RunResultViewV1>(&format!("/v1/runs/{run_id}/result"), StatusCode::OK)?;
    let result = response.body;
    validate_run_result(&result, run_id)?;
    Ok(result)
}

fn parse_create_request(
    bytes: &[u8],
) -> Result<(CreateRunRequestV1, Sha256Digest), RunClientError> {
    if bytes.is_empty() || bytes.len() > MAX_RUN_REQUEST_BYTES {
        return Err(RunClientError::InvalidRequest(
            "size is outside 1..=1048576 bytes".to_owned(),
        ));
    }
    let limits = run_json_limits();
    let value = parse_strict_json(bytes, limits)
        .map_err(|error| RunClientError::InvalidRequest(error.to_string()))?;
    let digest = Sha256Digest::from_str(
        &canonical_digest(&value)
            .map_err(|error| RunClientError::InvalidRequest(error.to_string()))?,
    )
    .map_err(|error| RunClientError::InvalidRequest(error.to_string()))?;
    let request = serde_json::from_value::<CreateRunRequestV1>(value)
        .map_err(|error| RunClientError::InvalidRequest(error.to_string()))?;
    if request.agent_id.kind() != ResourceKind::Agent {
        return Err(RunClientError::InvalidRequest(
            "agent_id is not an Agent Resource ID".to_owned(),
        ));
    }
    request
        .input
        .value
        .validate(limits)
        .map_err(|error| RunClientError::InvalidRequest(error.to_string()))?;
    if let ValueRef::Artifact { artifact } = &request.input.value {
        if artifact.classification() != request.input.classification {
            return Err(RunClientError::InvalidRequest(
                "input classification differs from its ArtifactRef".to_owned(),
            ));
        }
    }
    Ok((request, digest))
}

fn validate_run_response(
    response: &PublicJsonResponse<RunViewV1>,
    expected_run_id: Option<&ResourceId>,
) -> Result<(), RunClientError> {
    let run = &response.body;
    if run.schema_version != 1
        || run.run_id.kind() != ResourceKind::Run
        || expected_run_id.is_some_and(|expected| expected != &run.run_id)
        || run.agent_deployment_id.kind() != ResourceKind::AgentDeployment
        || run.input_value_id.kind() != ResourceKind::RunValue
        || run
            .output_value_id
            .as_ref()
            .is_some_and(|id| id.kind() != ResourceKind::RunValue)
        || run.version == 0
        || run.etag != response.etag
        || run.etag != format!("\"{}-{}\"", run.run_id, run.version)
        || run.updated_at.as_str() < run.created_at.as_str()
        || run
            .terminal_at
            .as_ref()
            .is_some_and(|terminal| terminal.as_str() < run.created_at.as_str())
    {
        return Err(RunClientError::InvalidResponse(
            "Run identity, version, ETag, or timestamps violate the public contract".to_owned(),
        ));
    }
    Ok(())
}

fn validate_run_result(
    result: &RunResultViewV1,
    expected_run_id: &ResourceId,
) -> Result<(), RunClientError> {
    if result.schema_version != 1
        || &result.run_id != expected_run_id
        || result.value_id.kind() != ResourceKind::RunValue
        || result.value.validate(run_json_limits()).is_err()
    {
        return Err(RunClientError::InvalidResponse(
            "Run result identity or value violates the public contract".to_owned(),
        ));
    }
    let content_matches = match &result.value {
        ValueRef::Inline { value } => {
            canonical_digest(value).is_ok_and(|digest| digest == result.content_digest.to_string())
        }
        ValueRef::Artifact { artifact } => {
            artifact.content_digest() == &result.content_digest
                && artifact.classification() == result.classification
        }
    };
    if !content_matches {
        return Err(RunClientError::InvalidResponse(
            "Run result content digest or classification does not match its value".to_owned(),
        ));
    }
    Ok(())
}

fn require_run_id(run_id: &ResourceId) -> Result<(), RunClientError> {
    if run_id.kind() != ResourceKind::Run {
        return Err(RunClientError::InvalidRequest(
            "run_id is not a Run ID".to_owned(),
        ));
    }
    Ok(())
}

const fn run_json_limits() -> JsonLimits {
    JsonLimits {
        max_bytes: MAX_RUN_REQUEST_BYTES,
        max_depth: 64,
        max_properties_per_object: 256,
        max_items_per_array: 4_096,
        max_string_bytes: 262_144,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{
        io::{Read as _, Write as _},
        net::{TcpListener, TcpStream},
        thread,
        time::Duration,
    };
    use uuid::Uuid;

    fn id(kind: ResourceKind) -> ResourceId {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
    }

    fn digest(character: char) -> String {
        format!("sha256:{}", character.to_string().repeat(64))
    }

    #[test]
    fn create_request_is_closed_and_agent_bound() {
        let request = json!({
            "agent_id": id(ResourceKind::Agent),
            "input": {
                "classification": "internal",
                "schema_digest": digest('a'),
                "value": {"kind": "inline", "value": {"message": "hello"}}
            },
            "deadline": "2026-08-30T00:00:00.000000Z"
        });
        assert!(parse_create_request(&serde_json::to_vec(&request).unwrap()).is_ok());

        let mut open = request;
        open.as_object_mut().unwrap().insert(
            "deployment_id".to_owned(),
            json!(id(ResourceKind::AgentDeployment)),
        );
        assert!(parse_create_request(&serde_json::to_vec(&open).unwrap()).is_err());
    }

    #[test]
    fn inline_result_requires_exact_canonical_digest() {
        let run_id = id(ResourceKind::Run);
        let value = json!({"answer": 42});
        let result = RunResultViewV1 {
            schema_version: 1,
            run_id: run_id.clone(),
            value_id: id(ResourceKind::RunValue),
            classification: DataClassification::Internal,
            schema_digest: digest('b').parse().unwrap(),
            content_digest: canonical_digest(&value).unwrap().parse().unwrap(),
            value: ValueRef::Inline { value },
        };
        assert!(validate_run_result(&result, &run_id).is_ok());
        let mut mismatched = result;
        mismatched.content_digest = digest('c').parse().unwrap();
        assert!(validate_run_result(&mismatched, &run_id).is_err());
    }

    #[test]
    fn public_run_create_control_and_result_preserve_contract_headers() {
        let agent_id = id(ResourceKind::Agent);
        let run_id = id(ResourceKind::Run);
        let deployment_id = id(ResourceKind::AgentDeployment);
        let input_value_id = id(ResourceKind::RunValue);
        let output_value_id = id(ResourceKind::RunValue);
        let request = json!({
            "agent_id": agent_id,
            "input": {
                "classification": "internal",
                "schema_digest": digest('a'),
                "value": {"kind": "inline", "value": {"message": "hello"}}
            },
            "deadline": "2026-08-30T00:00:00.000000Z"
        });
        let initial = RunViewV1 {
            schema_version: 1,
            run_id: run_id.clone(),
            agent_deployment_id: deployment_id.clone(),
            state: RunState::Running,
            version: 1,
            input_value_id: input_value_id.clone(),
            output_value_id: None,
            pause_generation: 0,
            cancel_generation: 0,
            deadline: "2026-08-30T00:00:00.000000Z".parse().unwrap(),
            started_at: Some("2026-08-29T00:00:01.000000Z".parse().unwrap()),
            terminal_at: None,
            created_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
            updated_at: "2026-08-29T00:00:01.000000Z".parse().unwrap(),
            etag: format!("\"{run_id}-1\""),
        };
        let paused = RunViewV1 {
            state: RunState::Waiting,
            version: 2,
            pause_generation: 1,
            updated_at: "2026-08-29T00:00:02.000000Z".parse().unwrap(),
            etag: format!("\"{run_id}-2\""),
            ..initial.clone()
        };
        let result_value = json!({"answer": 42});
        let result = RunResultViewV1 {
            schema_version: 1,
            run_id: run_id.clone(),
            value_id: output_value_id,
            classification: DataClassification::Internal,
            schema_digest: digest('b').parse().unwrap(),
            content_digest: canonical_digest(&result_value).unwrap().parse().unwrap(),
            value: ValueRef::Inline {
                value: result_value,
            },
        };

        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server_run_id = run_id.clone();
        let server_initial = initial.clone();
        let server_request = request.clone();
        let server_paused = paused.clone();
        let server_result = result.clone();
        let server = thread::spawn(move || {
            for step in 0..4 {
                let (mut stream, _) = listener.accept().unwrap();
                let (head, body) = read_request(&mut stream);
                assert_eq!(header_value(&head, "authorization"), Some("Bearer token"));
                match step {
                    0 => {
                        assert!(head.starts_with("POST /v1/runs HTTP/1.1"));
                        assert!(header_value(&head, "idempotency-key")
                            .is_some_and(|value| value.ends_with("-create")));
                        assert_eq!(
                            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
                            server_request
                        );
                        let trace = request_trace_id(&head);
                        write_json_response(
                            &mut stream,
                            "201 Created",
                            &trace,
                            Some(&server_initial.etag),
                            Some(&format!("/v1/runs/{server_run_id}")),
                            &server_initial,
                        );
                    }
                    1 => {
                        assert!(head.starts_with(&format!("GET /v1/runs/{server_run_id} HTTP/1.1")));
                        write_json_response(
                            &mut stream,
                            "200 OK",
                            "11111111111111111111111111111111",
                            Some(&server_initial.etag),
                            None,
                            &server_initial,
                        );
                    }
                    2 => {
                        assert!(head
                            .starts_with(&format!("POST /v1/runs/{server_run_id}:pause HTTP/1.1")));
                        assert_eq!(
                            header_value(&head, "if-match"),
                            Some(server_initial.etag.as_str())
                        );
                        assert!(header_value(&head, "idempotency-key")
                            .is_some_and(|value| value.ends_with("-pause")));
                        let trace = request_trace_id(&head);
                        write_json_response(
                            &mut stream,
                            "200 OK",
                            &trace,
                            Some(&server_paused.etag),
                            None,
                            &server_paused,
                        );
                    }
                    3 => {
                        assert!(head
                            .starts_with(&format!("GET /v1/runs/{server_run_id}/result HTTP/1.1")));
                        write_json_response(
                            &mut stream,
                            "200 OK",
                            "22222222222222222222222222222222",
                            None,
                            None,
                            &server_result,
                        );
                    }
                    _ => unreachable!(),
                }
            }
        });
        let client = PublicHttpClient::new(
            format!("http://127.0.0.1:{port}"),
            "token".to_owned(),
            Duration::from_secs(2),
        )
        .unwrap();
        let created = create_run(&client, &serde_json::to_vec(&request).unwrap()).unwrap();
        assert_eq!(created, initial);
        let controlled = control_run(&client, &run_id, RunControlAction::Pause).unwrap();
        assert_eq!(controlled, paused);
        assert_eq!(read_run_result(&client, &run_id).unwrap(), result);
        server.join().unwrap();
    }

    fn read_request(stream: &mut TcpStream) -> (String, Vec<u8>) {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 4096];
        let header_end = loop {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0);
            bytes.extend_from_slice(&buffer[..read]);
            if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
        };
        let head = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
        let content_length = header_value(&head, "content-length")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        while bytes.len() < header_end + content_length {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0);
            bytes.extend_from_slice(&buffer[..read]);
        }
        (
            head,
            bytes[header_end..header_end + content_length].to_vec(),
        )
    }

    fn header_value<'a>(head: &'a str, name: &str) -> Option<&'a str> {
        head.lines().find_map(|line| {
            let (candidate, value) = line.split_once(':')?;
            candidate.eq_ignore_ascii_case(name).then_some(value.trim())
        })
    }

    fn request_trace_id(head: &str) -> String {
        header_value(head, "traceparent")
            .unwrap()
            .split('-')
            .nth(1)
            .unwrap()
            .to_owned()
    }

    fn write_json_response<T: Serialize>(
        stream: &mut TcpStream,
        status: &str,
        trace_id: &str,
        etag: Option<&str>,
        location: Option<&str>,
        body: &T,
    ) {
        let body = serde_json::to_vec(body).unwrap();
        write!(
            stream,
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncache-control: no-store, private, max-age=0\r\ntrace-id: {trace_id}\r\n"
        )
        .unwrap();
        if let Some(etag) = etag {
            write!(stream, "etag: {etag}\r\n").unwrap();
        }
        if let Some(location) = location {
            write!(stream, "location: {location}\r\n").unwrap();
        }
        write!(
            stream,
            "content-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        )
        .unwrap();
        stream.write_all(&body).unwrap();
    }
}
