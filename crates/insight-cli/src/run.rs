//! Closed public Run lifecycle client for the local Runtime Gateway.

use crate::public_client::{
    PublicClientError, PublicHttpClient, PublicJsonResponse, PublicSseFrame,
};
use crate::run_journal::{self, RunControlJournalV1, RunJournalError};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, DataClassification, EventDurability, JsonLimits,
    PublicRunEvent, ResourceId, ResourceKind, RunState, Sha256Digest, UtcTimestamp, ValueRef,
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    io::Write,
    path::Path,
    str::FromStr as _,
    thread,
    time::{Duration, Instant},
};

const MAX_RUN_REQUEST_BYTES: usize = 1_048_576;

#[derive(Debug)]
pub enum RunClientError {
    InvalidRequest(String),
    InvalidResponse(String),
    Public(PublicClientError),
    WatchTimeout {
        run_id: String,
        timeout_seconds: u64,
    },
    Output(String),
    Journal(RunJournalError),
}

impl fmt::Display for RunClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest(detail) => write!(formatter, "Run request is invalid: {detail}"),
            Self::InvalidResponse(detail) => {
                write!(formatter, "Run authority response is invalid: {detail}")
            }
            Self::Public(error) => write!(formatter, "{error}"),
            Self::WatchTimeout {
                run_id,
                timeout_seconds,
            } => write!(
                formatter,
                "Run {run_id} did not become terminal within {timeout_seconds} seconds"
            ),
            Self::Output(detail) => write!(formatter, "cannot write Run watch output: {detail}"),
            Self::Journal(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for RunClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Public(error) => Some(error),
            Self::Journal(error) => Some(error),
            _ => None,
        }
    }
}

impl From<PublicClientError> for RunClientError {
    fn from(value: PublicClientError) -> Self {
        Self::Public(value)
    }
}

impl From<RunJournalError> for RunClientError {
    fn from(value: RunJournalError) -> Self {
        Self::Journal(value)
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

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunWatchRecordV1 {
    Event {
        schema_version: u16,
        event: PublicRunEvent,
    },
    Terminal {
        schema_version: u16,
        run: RunViewV1,
    },
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
    journal_directory: &Path,
) -> Result<RunViewV1, RunClientError> {
    require_run_id(run_id)?;
    let path = run_journal::journal_path(journal_directory, run_id, action.as_str());
    let mut journal = match run_journal::load(&path)? {
        Some(journal) => {
            validate_control_journal(&journal, run_id, action)?;
            if let Some(result) = &journal.result {
                validate_run_view(result, Some(run_id), &result.etag)?;
                let current = read_run(client, run_id)?;
                if control_effect_is_current(action, result, &current) {
                    return Ok(current);
                }
                new_control_journal(run_id, action, &current.etag)?
            } else {
                journal
            }
        }
        None => {
            let current = read_run(client, run_id)?;
            new_control_journal(run_id, action, &current.etag)?
        }
    };
    run_journal::save(&path, &journal)?;
    let response: PublicJsonResponse<RunViewV1> = client.post_empty(
        &format!("/v1/runs/{run_id}:{}", action.as_str()),
        StatusCode::OK,
        &journal.receipt,
        &journal.if_match,
    )?;
    validate_run_response(&response, Some(run_id))?;
    journal.result = Some(response.body.clone());
    run_journal::save(&path, &journal)?;
    Ok(response.body)
}

fn new_control_journal(
    run_id: &ResourceId,
    action: RunControlAction,
    if_match: &str,
) -> Result<RunControlJournalV1, RunClientError> {
    let receipt = control_receipt(run_id, action, if_match)?;
    Ok(RunControlJournalV1::new(
        run_id.clone(),
        action.as_str().to_owned(),
        receipt,
        if_match.to_owned(),
    ))
}

fn control_receipt(
    run_id: &ResourceId,
    action: RunControlAction,
    if_match: &str,
) -> Result<String, RunClientError> {
    let receipt_digest = canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "operation": format!("run.{}", action.as_str()),
        "run_id": run_id,
        "if_match": if_match,
    }))
    .map_err(|error| RunClientError::InvalidRequest(error.to_string()))?;
    Ok(format!(
        "insight-run-v1-{}-{}",
        receipt_digest
            .strip_prefix("sha256:")
            .unwrap_or(&receipt_digest),
        action.as_str()
    ))
}

fn validate_control_journal(
    journal: &RunControlJournalV1,
    run_id: &ResourceId,
    action: RunControlAction,
) -> Result<(), RunClientError> {
    if &journal.run_id != run_id
        || journal.action != action.as_str()
        || journal.receipt != control_receipt(run_id, action, &journal.if_match)?
    {
        return Err(RunClientError::InvalidResponse(
            "Run control journal differs from the deterministic command".to_owned(),
        ));
    }
    Ok(())
}

fn control_effect_is_current(
    action: RunControlAction,
    result: &RunViewV1,
    current: &RunViewV1,
) -> bool {
    match action {
        RunControlAction::Pause | RunControlAction::Resume => {
            current.pause_generation == result.pause_generation
        }
        RunControlAction::Cancel => current.cancel_generation >= result.cancel_generation,
    }
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

pub fn watch_run<W: Write>(
    client: &PublicHttpClient,
    run_id: &ResourceId,
    timeout: Duration,
    writer: &mut W,
) -> Result<RunViewV1, RunClientError> {
    require_run_id(run_id)?;
    if timeout.is_zero() || timeout > Duration::from_secs(3_600) {
        return Err(RunClientError::InvalidRequest(
            "watch timeout is outside 1..=3600 seconds".to_owned(),
        ));
    }
    let started = Instant::now();
    let mut cursor = None::<String>;
    let mut last_sequence = 0_u64;
    loop {
        let frames = client.get_sse_json::<PublicRunEvent>(
            &format!("/v1/runs/{run_id}/events"),
            cursor.as_deref(),
        )?;
        let page_is_full = frames.len() == 128;
        for frame in frames {
            let (next_cursor, sequence) = validate_event_frame(&frame, run_id, last_sequence)?;
            write_watch_record(
                writer,
                &RunWatchRecordV1::Event {
                    schema_version: 1,
                    event: frame.data,
                },
            )?;
            cursor = Some(next_cursor);
            last_sequence = sequence;
        }
        let run = read_run(client, run_id)?;
        if terminal_run_state(run.state) && !page_is_full {
            write_watch_record(
                writer,
                &RunWatchRecordV1::Terminal {
                    schema_version: 1,
                    run: run.clone(),
                },
            )?;
            return Ok(run);
        }
        if started.elapsed() >= timeout {
            return Err(RunClientError::WatchTimeout {
                run_id: run_id.to_string(),
                timeout_seconds: timeout.as_secs(),
            });
        }
        if cursor.is_none() || !page_is_full {
            thread::sleep(Duration::from_millis(100));
        }
    }
}

fn validate_event_frame(
    frame: &PublicSseFrame<PublicRunEvent>,
    expected_run_id: &ResourceId,
    last_sequence: u64,
) -> Result<(String, u64), RunClientError> {
    let event = &frame.data;
    event.validate().map_err(|_| {
        RunClientError::InvalidResponse("SSE event violates its public contract".to_owned())
    })?;
    let cursor = event.cursor.as_ref().ok_or_else(|| {
        RunClientError::InvalidResponse("durable SSE event omitted its cursor".to_owned())
    })?;
    let sequence = event.sequence.ok_or_else(|| {
        RunClientError::InvalidResponse("durable SSE event omitted its sequence".to_owned())
    })?;
    if &event.run_id != expected_run_id
        || event.durability != EventDurability::Durable
        || cursor.as_str() != frame.id
        || event.event_type.as_str() != frame.event
        || sequence <= last_sequence
    {
        return Err(RunClientError::InvalidResponse(
            "SSE id, type, Run, durability, or sequence is inconsistent".to_owned(),
        ));
    }
    Ok((cursor.as_str().to_owned(), sequence))
}

fn write_watch_record<W: Write>(
    writer: &mut W,
    record: &RunWatchRecordV1,
) -> Result<(), RunClientError> {
    serde_json::to_writer(&mut *writer, record)
        .map_err(|error| RunClientError::Output(error.to_string()))?;
    writer
        .write_all(b"\n")
        .and_then(|_| writer.flush())
        .map_err(|error| RunClientError::Output(error.to_string()))
}

const fn terminal_run_state(state: RunState) -> bool {
    matches!(
        state,
        RunState::Succeeded | RunState::Failed | RunState::Cancelled | RunState::TimedOut
    )
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
    validate_run_view(&response.body, expected_run_id, &response.etag)
}

fn validate_run_view(
    run: &RunViewV1,
    expected_run_id: Option<&ResourceId>,
    envelope_etag: &str,
) -> Result<(), RunClientError> {
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
        || run.etag != envelope_etag
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
    use insight_platform_contracts::{
        ApiProblem, ApiProblemCode, DurablePublicRunEventData, OpaqueRunEventCursor,
        PublicRunEventSourceKind, PublicRunEventType, TraceId,
    };
    use serde_json::json;
    use std::{
        fs,
        io::Read as _,
        net::{TcpListener, TcpStream},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread,
        time::Duration,
    };
    use tempfile::TempDir;
    use uuid::Uuid;

    fn id(kind: ResourceKind) -> ResourceId {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
    }

    fn digest(character: char) -> String {
        format!("sha256:{}", character.to_string().repeat(64))
    }

    fn running_run(run_id: ResourceId) -> RunViewV1 {
        RunViewV1 {
            schema_version: 1,
            run_id: run_id.clone(),
            agent_deployment_id: id(ResourceKind::AgentDeployment),
            state: RunState::Running,
            version: 1,
            input_value_id: id(ResourceKind::RunValue),
            output_value_id: None,
            pause_generation: 0,
            cancel_generation: 0,
            deadline: "2026-08-30T00:00:00.000000Z".parse().unwrap(),
            started_at: Some("2026-08-29T00:00:01.000000Z".parse().unwrap()),
            terminal_at: None,
            created_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
            updated_at: "2026-08-29T00:00:01.000000Z".parse().unwrap(),
            etag: format!("\"{run_id}-1\""),
        }
    }

    fn problem(
        status: u16,
        code: ApiProblemCode,
        retryable: bool,
        retry_after_ms: Option<u64>,
        trace_id: TraceId,
    ) -> ApiProblem {
        ApiProblem {
            type_uri: format!("urn:insight:problem:{}", code.as_str()),
            title: "Run request rejected".to_owned(),
            status,
            code,
            detail: Some("safe public diagnostic".to_owned()),
            request_id: id(ResourceKind::ServerRequest),
            trace_id,
            retryable,
            retry_after_ms,
            field_errors: Vec::new(),
        }
    }

    #[derive(Default)]
    struct FlushWriter {
        bytes: Vec<u8>,
        flushes: usize,
    }

    impl std::io::Write for FlushWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.flushes += 1;
            Ok(())
        }
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
    fn control_preserves_closed_cas_and_backpressure_problems() {
        let cases = [
            (
                "409 Conflict",
                409,
                ApiProblemCode::InvalidStateTransition,
                false,
                None,
            ),
            (
                "412 Precondition Failed",
                412,
                ApiProblemCode::PreconditionFailed,
                false,
                None,
            ),
            (
                "429 Too Many Requests",
                429,
                ApiProblemCode::RateLimited,
                true,
                Some(250),
            ),
            (
                "503 Service Unavailable",
                503,
                ApiProblemCode::TemporarilyUnavailable,
                true,
                Some(500),
            ),
        ];

        for (status_line, status, code, retryable, retry_after_ms) in cases {
            let run_id = id(ResourceKind::Run);
            let current = running_run(run_id.clone());
            let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let port = listener.local_addr().unwrap().port();
            let server_run_id = run_id.clone();
            let server_current = current.clone();
            let server_code = code;
            let server = thread::spawn(move || {
                let (mut read, _) = listener.accept().unwrap();
                let (head, _) = read_request(&mut read);
                assert!(head.starts_with(&format!("GET /v1/runs/{server_run_id} HTTP/1.1")));
                write_json_response(
                    &mut read,
                    "200 OK",
                    "11111111111111111111111111111111",
                    Some(&server_current.etag),
                    None,
                    &server_current,
                );

                let (mut mutation, _) = listener.accept().unwrap();
                let (head, _) = read_request(&mut mutation);
                assert!(head.starts_with(&format!("POST /v1/runs/{server_run_id}:pause HTTP/1.1")));
                assert_eq!(
                    header_value(&head, "if-match"),
                    Some(server_current.etag.as_str())
                );
                assert!(header_value(&head, "idempotency-key")
                    .is_some_and(|value| value.ends_with("-pause")));
                let trace_id = request_trace_id(&head).parse().unwrap();
                let problem = problem(status, server_code, retryable, retry_after_ms, trace_id);
                write_problem_response(&mut mutation, status_line, &problem);
            });
            let client = PublicHttpClient::new(
                format!("http://127.0.0.1:{port}"),
                "token".to_owned(),
                Duration::from_secs(2),
            )
            .unwrap();
            let journals = TempDir::new().unwrap();
            let error = control_run(&client, &run_id, RunControlAction::Pause, journals.path())
                .unwrap_err();
            match error {
                RunClientError::Public(PublicClientError::Problem(actual)) => {
                    assert_eq!(actual.status, status);
                    assert_eq!(actual.code, code);
                    assert_eq!(actual.retryable, retryable);
                    assert_eq!(actual.retry_after_ms, retry_after_ms);
                }
                other => panic!("expected closed Run Problem, got {other:?}"),
            }
            server.join().unwrap();
        }
    }

    #[test]
    fn result_not_ready_preserves_run_not_terminal_problem() {
        let run_id = id(ResourceKind::Run);
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server_run_id = run_id.clone();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let (head, _) = read_request(&mut stream);
            assert!(head.starts_with(&format!("GET /v1/runs/{server_run_id}/result HTTP/1.1")));
            let trace_id = "22222222222222222222222222222222".parse().unwrap();
            let problem = problem(
                409,
                ApiProblemCode::RunNotTerminal,
                true,
                Some(100),
                trace_id,
            );
            write_problem_response(&mut stream, "409 Conflict", &problem);
        });
        let client = PublicHttpClient::new(
            format!("http://127.0.0.1:{port}"),
            "token".to_owned(),
            Duration::from_secs(2),
        )
        .unwrap();
        match read_run_result(&client, &run_id).unwrap_err() {
            RunClientError::Public(PublicClientError::Problem(actual)) => {
                assert_eq!(actual.code, ApiProblemCode::RunNotTerminal);
                assert!(actual.retryable);
                assert_eq!(actual.retry_after_ms, Some(100));
            }
            other => panic!("expected result-not-ready Problem, got {other:?}"),
        }
        server.join().unwrap();
    }

    #[test]
    fn watch_reports_a_failed_terminal_run() {
        let run_id = id(ResourceKind::Run);
        let failed = RunViewV1 {
            state: RunState::Failed,
            version: 2,
            terminal_at: Some("2026-08-29T00:00:02.000000Z".parse().unwrap()),
            updated_at: "2026-08-29T00:00:02.000000Z".parse().unwrap(),
            etag: format!("\"{run_id}-2\""),
            ..running_run(run_id.clone())
        };
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server_run_id = run_id.clone();
        let server_failed = failed.clone();
        let server = thread::spawn(move || {
            let (mut events, _) = listener.accept().unwrap();
            let (head, _) = read_request(&mut events);
            assert!(head.starts_with(&format!("GET /v1/runs/{server_run_id}/events HTTP/1.1")));
            write_empty_sse_response(&mut events);

            let (mut read, _) = listener.accept().unwrap();
            let (head, _) = read_request(&mut read);
            assert!(head.starts_with(&format!("GET /v1/runs/{server_run_id} HTTP/1.1")));
            write_json_response(
                &mut read,
                "200 OK",
                "11111111111111111111111111111111",
                Some(&server_failed.etag),
                None,
                &server_failed,
            );
        });
        let client = PublicHttpClient::new(
            format!("http://127.0.0.1:{port}"),
            "token".to_owned(),
            Duration::from_secs(2),
        )
        .unwrap();
        let mut output = FlushWriter::default();
        assert_eq!(
            watch_run(&client, &run_id, Duration::from_secs(2), &mut output).unwrap(),
            failed
        );
        assert_eq!(output.flushes, 1);
        let record: serde_json::Value = serde_json::from_slice(&output.bytes).unwrap();
        assert_eq!(record["kind"], "terminal");
        assert_eq!(record["run"]["state"], "failed");
        server.join().unwrap();
    }

    #[test]
    fn watch_preserves_cursor_and_backpressure_problems() {
        let cases = [
            (
                "400 Bad Request",
                400,
                ApiProblemCode::CursorInvalid,
                false,
                None,
            ),
            ("410 Gone", 410, ApiProblemCode::CursorExpired, false, None),
            (
                "429 Too Many Requests",
                429,
                ApiProblemCode::RateLimited,
                true,
                Some(250),
            ),
            (
                "503 Service Unavailable",
                503,
                ApiProblemCode::TemporarilyUnavailable,
                true,
                Some(500),
            ),
        ];
        for (status_line, status, code, retryable, retry_after_ms) in cases {
            let run_id = id(ResourceKind::Run);
            let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let port = listener.local_addr().unwrap().port();
            let server_run_id = run_id.clone();
            let server_code = code;
            let server = thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let (head, _) = read_request(&mut stream);
                assert!(head.starts_with(&format!("GET /v1/runs/{server_run_id}/events HTTP/1.1")));
                let trace_id = "44444444444444444444444444444444".parse().unwrap();
                let problem = problem(status, server_code, retryable, retry_after_ms, trace_id);
                write_problem_response(&mut stream, status_line, &problem);
            });
            let client = PublicHttpClient::new(
                format!("http://127.0.0.1:{port}"),
                "token".to_owned(),
                Duration::from_secs(2),
            )
            .unwrap();
            let mut output = FlushWriter::default();
            match watch_run(&client, &run_id, Duration::from_secs(2), &mut output).unwrap_err() {
                RunClientError::Public(PublicClientError::Problem(actual)) => {
                    assert_eq!(actual.status, status);
                    assert_eq!(actual.code, code);
                    assert_eq!(actual.retryable, retryable);
                    assert_eq!(actual.retry_after_ms, retry_after_ms);
                }
                other => panic!("expected closed SSE Problem, got {other:?}"),
            }
            assert!(output.bytes.is_empty());
            server.join().unwrap();
        }
    }

    #[test]
    fn watch_times_out_without_mutating_a_nonterminal_run() {
        let run_id = id(ResourceKind::Run);
        let running = running_run(run_id.clone());
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let done = Arc::new(AtomicBool::new(false));
        let server_done = Arc::clone(&done);
        let server_run_id = run_id.clone();
        let server_running = running.clone();
        let server = thread::spawn(move || {
            while !server_done.load(Ordering::Acquire) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(error) => panic!("accept watch timeout fixture: {error}"),
                };
                stream.set_nonblocking(false).unwrap();
                let (head, _) = read_request(&mut stream);
                if head.starts_with(&format!("GET /v1/runs/{server_run_id}/events HTTP/1.1")) {
                    write_empty_sse_response(&mut stream);
                } else {
                    assert!(head.starts_with(&format!("GET /v1/runs/{server_run_id} HTTP/1.1")));
                    write_json_response(
                        &mut stream,
                        "200 OK",
                        "11111111111111111111111111111111",
                        Some(&server_running.etag),
                        None,
                        &server_running,
                    );
                }
            }
        });
        let client = PublicHttpClient::new(
            format!("http://127.0.0.1:{port}"),
            "token".to_owned(),
            Duration::from_secs(2),
        )
        .unwrap();
        let mut output = FlushWriter::default();
        assert!(matches!(
            watch_run(&client, &run_id, Duration::from_secs(1), &mut output),
            Err(RunClientError::WatchTimeout {
                timeout_seconds: 1,
                ..
            })
        ));
        done.store(true, Ordering::Release);
        server.join().unwrap();
        assert!(output.bytes.is_empty());
        assert_eq!(running.state, RunState::Running);
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
        let journals = TempDir::new().unwrap();
        let controlled =
            control_run(&client, &run_id, RunControlAction::Pause, journals.path()).unwrap();
        assert_eq!(controlled, paused);
        assert_eq!(read_run_result(&client, &run_id).unwrap(), result);
        server.join().unwrap();
    }

    #[test]
    fn control_replays_exact_intent_after_response_loss() {
        let run_id = id(ResourceKind::Run);
        let current = RunViewV1 {
            schema_version: 1,
            run_id: run_id.clone(),
            agent_deployment_id: id(ResourceKind::AgentDeployment),
            state: RunState::Running,
            version: 1,
            input_value_id: id(ResourceKind::RunValue),
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
            ..current.clone()
        };
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server_run_id = run_id.clone();
        let server_current = current.clone();
        let server_paused = paused.clone();
        let server =
            thread::spawn(move || {
                let (mut first_read, _) = listener.accept().unwrap();
                let (head, _) = read_request(&mut first_read);
                assert!(head.starts_with(&format!("GET /v1/runs/{server_run_id} HTTP/1.1")));
                write_json_response(
                    &mut first_read,
                    "200 OK",
                    "11111111111111111111111111111111",
                    Some(&server_current.etag),
                    None,
                    &server_current,
                );

                let (mut dropped, _) = listener.accept().unwrap();
                let (dropped_head, _) = read_request(&mut dropped);
                assert!(dropped_head
                    .starts_with(&format!("POST /v1/runs/{server_run_id}:pause HTTP/1.1")));
                let receipt = header_value(&dropped_head, "idempotency-key")
                    .unwrap()
                    .to_owned();
                let if_match = header_value(&dropped_head, "if-match").unwrap().to_owned();
                drop(dropped);

                let (mut replay, _) = listener.accept().unwrap();
                let (replay_head, _) = read_request(&mut replay);
                assert!(replay_head
                    .starts_with(&format!("POST /v1/runs/{server_run_id}:pause HTTP/1.1")));
                assert_eq!(
                    header_value(&replay_head, "idempotency-key"),
                    Some(receipt.as_str())
                );
                assert_eq!(
                    header_value(&replay_head, "if-match"),
                    Some(if_match.as_str())
                );
                let trace = request_trace_id(&replay_head);
                write_json_response(
                    &mut replay,
                    "200 OK",
                    &trace,
                    Some(&server_paused.etag),
                    None,
                    &server_paused,
                );

                let (mut final_read, _) = listener.accept().unwrap();
                let (head, _) = read_request(&mut final_read);
                assert!(head.starts_with(&format!("GET /v1/runs/{server_run_id} HTTP/1.1")));
                write_json_response(
                    &mut final_read,
                    "200 OK",
                    "22222222222222222222222222222222",
                    Some(&server_paused.etag),
                    None,
                    &server_paused,
                );
            });
        let client = PublicHttpClient::new(
            format!("http://127.0.0.1:{port}"),
            "token".to_owned(),
            Duration::from_secs(2),
        )
        .unwrap();
        let journals = TempDir::new().unwrap();
        assert!(matches!(
            control_run(&client, &run_id, RunControlAction::Pause, journals.path()),
            Err(RunClientError::Public(PublicClientError::Transport(_)))
        ));
        assert_eq!(
            control_run(&client, &run_id, RunControlAction::Pause, journals.path()).unwrap(),
            paused
        );
        assert_eq!(
            control_run(&client, &run_id, RunControlAction::Pause, journals.path()).unwrap(),
            paused
        );
        let journal =
            fs::read_to_string(run_journal::journal_path(journals.path(), &run_id, "pause"))
                .unwrap();
        assert!(!journal.contains("Bearer"));
        server.join().unwrap();
    }

    #[test]
    fn watch_reconnects_with_opaque_cursor_and_flushes_terminal_json_lines() {
        let run_id = id(ResourceKind::Run);
        let deployment_id = id(ResourceKind::AgentDeployment);
        let input_value_id = id(ResourceKind::RunValue);
        let cursor_one = OpaqueRunEventCursor::new(format!("cursor-{}", "a".repeat(512))).unwrap();
        let cursor_two = OpaqueRunEventCursor::new("cursor-two").unwrap();
        let event =
            |sequence: u64, event_type: PublicRunEventType, cursor: OpaqueRunEventCursor| {
                PublicRunEvent {
                    event_id: Some(id(ResourceKind::Event)),
                    run_id: run_id.clone(),
                    cursor: Some(cursor),
                    sequence: Some(sequence),
                    schema_version: 1,
                    trace_id: TraceId::new(),
                    event_type,
                    durability: EventDurability::Durable,
                    occurred_at: format!("2026-08-29T00:00:0{sequence}.000000Z")
                        .parse()
                        .unwrap(),
                    data: serde_json::to_value(DurablePublicRunEventData {
                        source_kind: PublicRunEventSourceKind::Run,
                        source_id: run_id.clone(),
                        source_projection_version: sequence,
                        safe_summary: None,
                    })
                    .unwrap(),
                }
            };
        let queued = event(1, PublicRunEventType::RunQueued, cursor_one.clone());
        let completed = event(2, PublicRunEventType::RunCompleted, cursor_two.clone());
        let running = RunViewV1 {
            schema_version: 1,
            run_id: run_id.clone(),
            agent_deployment_id: deployment_id.clone(),
            state: RunState::Running,
            version: 2,
            input_value_id: input_value_id.clone(),
            output_value_id: None,
            pause_generation: 0,
            cancel_generation: 0,
            deadline: "2026-08-30T00:00:00.000000Z".parse().unwrap(),
            started_at: Some("2026-08-29T00:00:01.000000Z".parse().unwrap()),
            terminal_at: None,
            created_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
            updated_at: "2026-08-29T00:00:01.000000Z".parse().unwrap(),
            etag: format!("\"{run_id}-2\""),
        };
        let terminal = RunViewV1 {
            state: RunState::Succeeded,
            version: 3,
            output_value_id: Some(id(ResourceKind::RunValue)),
            terminal_at: Some("2026-08-29T00:00:02.000000Z".parse().unwrap()),
            updated_at: "2026-08-29T00:00:02.000000Z".parse().unwrap(),
            etag: format!("\"{run_id}-3\""),
            ..running.clone()
        };
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server_run_id = run_id.clone();
        let server_cursor_one = cursor_one.clone();
        let server_terminal = terminal.clone();
        let server = thread::spawn(move || {
            for step in 0..4 {
                let (mut stream, _) = listener.accept().unwrap();
                let (head, _) = read_request(&mut stream);
                assert_eq!(header_value(&head, "authorization"), Some("Bearer token"));
                match step {
                    0 => {
                        assert!(head
                            .starts_with(&format!("GET /v1/runs/{server_run_id}/events HTTP/1.1")));
                        assert_eq!(header_value(&head, "last-event-id"), None);
                        write_sse_response(&mut stream, &queued);
                    }
                    1 => {
                        assert!(head.starts_with(&format!("GET /v1/runs/{server_run_id} HTTP/1.1")));
                        write_json_response(
                            &mut stream,
                            "200 OK",
                            "11111111111111111111111111111111",
                            Some(&running.etag),
                            None,
                            &running,
                        );
                    }
                    2 => {
                        assert!(head
                            .starts_with(&format!("GET /v1/runs/{server_run_id}/events HTTP/1.1")));
                        assert_eq!(
                            header_value(&head, "last-event-id"),
                            Some(server_cursor_one.as_str())
                        );
                        write_sse_response(&mut stream, &completed);
                    }
                    3 => {
                        assert!(head.starts_with(&format!("GET /v1/runs/{server_run_id} HTTP/1.1")));
                        write_json_response(
                            &mut stream,
                            "200 OK",
                            "22222222222222222222222222222222",
                            Some(&server_terminal.etag),
                            None,
                            &server_terminal,
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
        let mut output = FlushWriter::default();
        assert_eq!(
            watch_run(&client, &run_id, Duration::from_secs(2), &mut output).unwrap(),
            terminal
        );
        assert_eq!(output.flushes, 3);
        let records = String::from_utf8(output.bytes)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0]["kind"], "event");
        assert_eq!(records[0]["event"]["sequence"], 1);
        assert_eq!(records[1]["event"]["sequence"], 2);
        assert_eq!(records[2]["kind"], "terminal");
        assert_eq!(records[2]["run"]["state"], "succeeded");
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

    fn write_sse_response(stream: &mut TcpStream, event: &PublicRunEvent) {
        let cursor = event.cursor.as_ref().unwrap();
        let body = format!(
            "id:{}\nevent:{}\ndata:{}\n\n",
            cursor.as_str(),
            event.event_type.as_str(),
            serde_json::to_string(event).unwrap()
        );
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncache-control: no-store, private, max-age=0\r\ntrace-id: 33333333333333333333333333333333\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
    }

    fn write_empty_sse_response(stream: &mut TcpStream) {
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncache-control: no-store, private, max-age=0\r\ntrace-id: 33333333333333333333333333333333\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
        )
        .unwrap();
    }

    fn write_problem_response(stream: &mut TcpStream, status: &str, problem: &ApiProblem) {
        let body = serde_json::to_vec(problem).unwrap();
        write!(
            stream,
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncache-control: no-store, private, max-age=0\r\ntrace-id: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            problem.trace_id,
            body.len()
        )
        .unwrap();
        stream.write_all(&body).unwrap();
    }
}
