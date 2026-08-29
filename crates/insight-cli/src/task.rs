//! Closed public Task lifecycle client for the Runtime Gateway.

use crate::public_client::{PublicClientError, PublicHttpClient, PublicJsonResponse};
use crate::task_journal::{self, TaskControlJournalV1, TaskJournalError};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, DataClassification, JsonLimits, ResourceId, ResourceKind,
    Sha256Digest, UtcTimestamp, ValueRef,
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::{fmt, path::Path};

const MAX_TASK_INPUT_BYTES: usize = 65_536;

#[derive(Debug)]
pub enum TaskClientError {
    InvalidRequest(String),
    InvalidResponse(String),
    Public(PublicClientError),
    Journal(TaskJournalError),
}

impl fmt::Display for TaskClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest(detail) => write!(formatter, "Task request is invalid: {detail}"),
            Self::InvalidResponse(detail) => {
                write!(formatter, "Task authority response is invalid: {detail}")
            }
            Self::Public(error) => write!(formatter, "{error}"),
            Self::Journal(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for TaskClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Public(error) => Some(error),
            Self::Journal(error) => Some(error),
            _ => None,
        }
    }
}

impl From<PublicClientError> for TaskClientError {
    fn from(value: PublicClientError) -> Self {
        Self::Public(value)
    }
}

impl From<TaskJournalError> for TaskClientError {
    fn from(value: TaskJournalError) -> Self {
        Self::Journal(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKindV1 {
    Approval,
    InteractionForm,
    InteractionUrlConsent,
    InteractionBusinessInput,
    ExternalAuthorization,
    HumanWork,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStateV1 {
    Pending,
    Responded,
    Declined,
    Approved,
    Rejected,
    Cancelled,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskOwnerLinkV1 {
    Run { run_id: ResourceId },
    Invocation { invocation_id: ResourceId },
    Artifact { artifact_id: ResourceId },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskViewV1 {
    pub schema_version: u32,
    pub task_id: ResourceId,
    pub task_kind: TaskKindV1,
    pub state: TaskStateV1,
    pub generation: u64,
    pub version: u64,
    pub safe_prompt_key: String,
    pub response_schema_digest: Option<Sha256Digest>,
    pub owner: TaskOwnerLinkV1,
    pub deadline: UtcTimestamp,
    pub responded_at: Option<UtcTimestamp>,
    pub created_at: UtcTimestamp,
    pub updated_at: UtcTimestamp,
    pub etag: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitTaskInputV1 {
    pub classification: DataClassification,
    pub schema_digest: Sha256Digest,
    pub value: ValueRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskAction {
    SubmitInput,
    Approve,
    Reject,
    Cancel,
}

impl TaskAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SubmitInput => "submit-input",
            Self::Approve => "approve",
            Self::Reject => "reject",
            Self::Cancel => "cancel",
        }
    }
}

pub fn parse_submit_input(bytes: &[u8]) -> Result<SubmitTaskInputV1, TaskClientError> {
    if bytes.is_empty() || bytes.len() > MAX_TASK_INPUT_BYTES {
        return Err(TaskClientError::InvalidRequest(
            "input size must be within 1..=65536 bytes".to_owned(),
        ));
    }
    let limits = task_json_limits();
    let value = parse_strict_json(bytes, limits)
        .map_err(|error| TaskClientError::InvalidRequest(error.to_string()))?;
    let input = serde_json::from_value::<SubmitTaskInputV1>(value)
        .map_err(|error| TaskClientError::InvalidRequest(error.to_string()))?;
    input
        .value
        .validate(limits)
        .map_err(|error| TaskClientError::InvalidRequest(error.to_string()))?;
    if let ValueRef::Artifact { artifact } = &input.value {
        if artifact.classification() != input.classification {
            return Err(TaskClientError::InvalidRequest(
                "input classification differs from its ArtifactRef".to_owned(),
            ));
        }
    }
    Ok(input)
}

pub fn read_task(
    client: &PublicHttpClient,
    task_id: &ResourceId,
) -> Result<TaskViewV1, TaskClientError> {
    require_task_id(task_id)?;
    let response: PublicJsonResponse<TaskViewV1> =
        client.get_json(&format!("/v1/tasks/{task_id}"), StatusCode::OK)?;
    validate_task_response(&response, task_id)?;
    Ok(response.body)
}

pub fn resolve_task(
    client: &PublicHttpClient,
    task_id: &ResourceId,
    action: TaskAction,
    input: Option<&SubmitTaskInputV1>,
    journal_directory: &Path,
) -> Result<TaskViewV1, TaskClientError> {
    require_task_id(task_id)?;
    if (action == TaskAction::SubmitInput) != input.is_some() {
        return Err(TaskClientError::InvalidRequest(
            "submit-input requires exactly one closed input document".to_owned(),
        ));
    }
    let path = task_journal::journal_path(journal_directory, task_id, action.as_str());
    let mut journal = match task_journal::load(&path)? {
        Some(journal) => {
            validate_journal(&journal, task_id, action, input)?;
            if let Some(result) = &journal.result {
                if result.state != terminal_state(action) {
                    return Err(TaskClientError::InvalidResponse(
                        "Task journal result does not match its action".to_owned(),
                    ));
                }
                return read_task(client, task_id);
            }
            journal
        }
        None => {
            let current = read_task(client, task_id)?;
            let receipt = mutation_receipt(task_id, action, &current.etag, input)?;
            TaskControlJournalV1::new(
                task_id.clone(),
                action.as_str().to_owned(),
                receipt,
                current.etag,
                input.cloned(),
            )
        }
    };
    task_journal::save(&path, &journal)?;
    let path = format!("/v1/tasks/{task_id}:{}", action.as_str());
    let response: PublicJsonResponse<TaskViewV1> = match journal.input.as_ref() {
        Some(input) => client.post_json(
            &path,
            input,
            StatusCode::OK,
            &journal.receipt,
            Some(&journal.if_match),
        )?,
        None => client.post_empty(&path, StatusCode::OK, &journal.receipt, &journal.if_match)?,
    };
    validate_task_response(&response, task_id)?;
    if response.body.state != terminal_state(action) {
        return Err(TaskClientError::InvalidResponse(
            "Task mutation did not return its exact terminal state".to_owned(),
        ));
    }
    journal.result = Some(response.body.clone());
    task_journal::save(
        &task_journal::journal_path(journal_directory, task_id, action.as_str()),
        &journal,
    )?;
    Ok(response.body)
}

fn validate_journal(
    journal: &TaskControlJournalV1,
    task_id: &ResourceId,
    action: TaskAction,
    input: Option<&SubmitTaskInputV1>,
) -> Result<(), TaskClientError> {
    if &journal.task_id != task_id
        || journal.action != action.as_str()
        || journal.input.as_ref() != input
        || journal.receipt
            != mutation_receipt(task_id, action, &journal.if_match, journal.input.as_ref())?
    {
        return Err(TaskClientError::InvalidResponse(
            "Task journal differs from the deterministic command".to_owned(),
        ));
    }
    Ok(())
}

const fn terminal_state(action: TaskAction) -> TaskStateV1 {
    match action {
        TaskAction::SubmitInput => TaskStateV1::Responded,
        TaskAction::Approve => TaskStateV1::Approved,
        TaskAction::Reject => TaskStateV1::Rejected,
        TaskAction::Cancel => TaskStateV1::Cancelled,
    }
}

fn mutation_receipt(
    task_id: &ResourceId,
    action: TaskAction,
    if_match: &str,
    input: Option<&SubmitTaskInputV1>,
) -> Result<String, TaskClientError> {
    let digest = canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "operation": format!("task.{}", action.as_str().replace('-', "_")),
        "task_id": task_id,
        "if_match": if_match,
        "input": input,
    }))
    .map_err(|error| TaskClientError::InvalidRequest(error.to_string()))?;
    Ok(format!(
        "insight-task-v1-{}-{}",
        digest.strip_prefix("sha256:").unwrap_or(&digest),
        action.as_str()
    ))
}

fn require_task_id(task_id: &ResourceId) -> Result<(), TaskClientError> {
    if matches!(
        task_id.kind(),
        ResourceKind::Interaction | ResourceKind::ApprovalTask
    ) {
        Ok(())
    } else {
        Err(TaskClientError::InvalidRequest(
            "task ID must identify an Interaction or Approval Task".to_owned(),
        ))
    }
}

fn validate_task_response(
    response: &PublicJsonResponse<TaskViewV1>,
    expected_task_id: &ResourceId,
) -> Result<(), TaskClientError> {
    let view = &response.body;
    let expected_kind = match view.task_kind {
        TaskKindV1::Approval => ResourceKind::ApprovalTask,
        TaskKindV1::InteractionForm
        | TaskKindV1::InteractionUrlConsent
        | TaskKindV1::InteractionBusinessInput
        | TaskKindV1::ExternalAuthorization
        | TaskKindV1::HumanWork => ResourceKind::Interaction,
    };
    if view.schema_version != 1
        || &view.task_id != expected_task_id
        || view.task_id.kind() != expected_kind
        || view.generation == 0
        || view.version == 0
        || view.safe_prompt_key.is_empty()
        || view.safe_prompt_key.len() > 128
        || view.etag != format!("\"{}-{}\"", view.task_id, view.version)
        || response.etag != view.etag
    {
        return Err(TaskClientError::InvalidResponse(
            "Task identity, state metadata, or ETag violates the closed contract".to_owned(),
        ));
    }
    match &view.owner {
        TaskOwnerLinkV1::Run { run_id } if run_id.kind() == ResourceKind::Run => {}
        TaskOwnerLinkV1::Invocation { invocation_id }
            if invocation_id.kind() == ResourceKind::CapabilityInvocation => {}
        TaskOwnerLinkV1::Artifact { artifact_id }
            if artifact_id.kind() == ResourceKind::Artifact => {}
        _ => {
            return Err(TaskClientError::InvalidResponse(
                "Task owner link has the wrong resource kind".to_owned(),
            ))
        }
    }
    Ok(())
}

const fn task_json_limits() -> JsonLimits {
    JsonLimits {
        max_bytes: MAX_TASK_INPUT_BYTES,
        max_depth: 32,
        max_properties_per_object: 128,
        max_items_per_array: 1_024,
        max_string_bytes: 32_768,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::TraceId;
    use serde_json::json;
    use std::{
        io::{Read as _, Write as _},
        net::{TcpListener, TcpStream},
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

    #[test]
    fn submit_input_is_closed_bounded_and_classification_bound() {
        let input = json!({
            "classification": "internal",
            "schema_digest": digest('a'),
            "value": {"kind": "inline", "value": {"answer": 42}}
        });
        assert!(parse_submit_input(&serde_json::to_vec(&input).unwrap()).is_ok());

        let mut open = input;
        open.as_object_mut()
            .unwrap()
            .insert("backend".to_owned(), json!("forbidden"));
        assert!(parse_submit_input(&serde_json::to_vec(&open).unwrap()).is_err());
    }

    #[test]
    fn task_view_requires_exact_identity_owner_kind_and_etag() {
        let task_id = id(ResourceKind::Interaction);
        let view = TaskViewV1 {
            schema_version: 1,
            task_id: task_id.clone(),
            task_kind: TaskKindV1::InteractionForm,
            state: TaskStateV1::Pending,
            generation: 1,
            version: 2,
            safe_prompt_key: "interaction.collect_details".to_owned(),
            response_schema_digest: Some(digest('b').parse().unwrap()),
            owner: TaskOwnerLinkV1::Run {
                run_id: id(ResourceKind::Run),
            },
            deadline: "2026-08-30T00:00:00.000000Z".parse().unwrap(),
            responded_at: None,
            created_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
            updated_at: "2026-08-29T00:00:01.000000Z".parse().unwrap(),
            etag: format!("\"{task_id}-2\""),
        };
        let response = PublicJsonResponse {
            etag: view.etag.clone(),
            body: view.clone(),
            location: None,
            trace_id: TraceId::new(),
        };
        assert!(validate_task_response(&response, &task_id).is_ok());

        let mut invalid = view;
        invalid.owner = TaskOwnerLinkV1::Artifact {
            artifact_id: id(ResourceKind::Run),
        };
        let response = PublicJsonResponse {
            etag: invalid.etag.clone(),
            body: invalid,
            location: None,
            trace_id: TraceId::new(),
        };
        assert!(validate_task_response(&response, &task_id).is_err());
    }

    #[test]
    fn mutation_replays_exact_receipt_and_etag_after_response_loss() {
        let task_id = id(ResourceKind::Interaction);
        let pending = task_view(task_id.clone(), TaskStateV1::Pending, 1);
        let responded = task_view(task_id.clone(), TaskStateV1::Responded, 2);
        let input = parse_submit_input(
            &serde_json::to_vec(&json!({
                "classification": "internal",
                "schema_digest": digest('a'),
                "value": {"kind": "inline", "value": {"answer": 42}}
            }))
            .unwrap(),
        )
        .unwrap();
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server_task_id = task_id.clone();
        let server_pending = pending.clone();
        let server_responded = responded.clone();
        let server = thread::spawn(move || {
            let (mut read, _) = listener.accept().unwrap();
            let (head, _) = read_request(&mut read);
            assert!(head.starts_with(&format!("GET /v1/tasks/{server_task_id} HTTP/1.1")));
            write_json_response(
                &mut read,
                &server_pending,
                "11111111111111111111111111111111",
            );

            let (mut dropped, _) = listener.accept().unwrap();
            let (dropped_head, _) = read_request(&mut dropped);
            assert!(dropped_head.starts_with(&format!(
                "POST /v1/tasks/{server_task_id}:submit-input HTTP/1.1"
            )));
            let receipt = header_value(&dropped_head, "idempotency-key")
                .unwrap()
                .to_owned();
            let if_match = header_value(&dropped_head, "if-match").unwrap().to_owned();
            drop(dropped);

            let (mut replay, _) = listener.accept().unwrap();
            let (replay_head, _) = read_request(&mut replay);
            assert_eq!(
                header_value(&replay_head, "idempotency-key"),
                Some(receipt.as_str())
            );
            assert_eq!(
                header_value(&replay_head, "if-match"),
                Some(if_match.as_str())
            );
            let trace = request_trace_id(&replay_head);
            write_json_response(&mut replay, &server_responded, &trace);

            let (mut final_read, _) = listener.accept().unwrap();
            let (head, _) = read_request(&mut final_read);
            assert!(head.starts_with(&format!("GET /v1/tasks/{server_task_id} HTTP/1.1")));
            write_json_response(
                &mut final_read,
                &server_responded,
                "22222222222222222222222222222222",
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
            resolve_task(
                &client,
                &task_id,
                TaskAction::SubmitInput,
                Some(&input),
                journals.path(),
            ),
            Err(TaskClientError::Public(PublicClientError::Transport(_)))
        ));
        assert_eq!(
            resolve_task(
                &client,
                &task_id,
                TaskAction::SubmitInput,
                Some(&input),
                journals.path(),
            )
            .unwrap(),
            responded
        );
        assert_eq!(
            resolve_task(
                &client,
                &task_id,
                TaskAction::SubmitInput,
                Some(&input),
                journals.path(),
            )
            .unwrap(),
            responded
        );
        let journal = std::fs::read_to_string(task_journal::journal_path(
            journals.path(),
            &task_id,
            "submit-input",
        ))
        .unwrap();
        assert!(!journal.contains("Bearer"));
        server.join().unwrap();
    }

    fn task_view(task_id: ResourceId, state: TaskStateV1, version: u64) -> TaskViewV1 {
        TaskViewV1 {
            schema_version: 1,
            task_id: task_id.clone(),
            task_kind: TaskKindV1::InteractionForm,
            state,
            generation: 1,
            version,
            safe_prompt_key: "interaction.collect_details".to_owned(),
            response_schema_digest: Some(digest('b').parse().unwrap()),
            owner: TaskOwnerLinkV1::Run {
                run_id: id(ResourceKind::Run),
            },
            deadline: "2026-08-30T00:00:00.000000Z".parse().unwrap(),
            responded_at: (state != TaskStateV1::Pending)
                .then(|| "2026-08-29T00:00:02.000000Z".parse().unwrap()),
            created_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
            updated_at: "2026-08-29T00:00:01.000000Z".parse().unwrap(),
            etag: format!("\"{task_id}-{version}\""),
        }
    }

    fn read_request(stream: &mut TcpStream) -> (String, Vec<u8>) {
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 4_096];
        loop {
            let read = stream.read(&mut buffer).unwrap();
            assert!(read > 0);
            bytes.extend_from_slice(&buffer[..read]);
            if let Some(split) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                let body_start = split + 4;
                let head = String::from_utf8(bytes[..body_start].to_vec()).unwrap();
                let length = header_value(&head, "content-length")
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(0);
                while bytes.len() < body_start + length {
                    let read = stream.read(&mut buffer).unwrap();
                    bytes.extend_from_slice(&buffer[..read]);
                }
                return (head, bytes[body_start..body_start + length].to_vec());
            }
        }
    }

    fn header_value<'a>(head: &'a str, name: &str) -> Option<&'a str> {
        head.lines().skip(1).find_map(|line| {
            let (header, value) = line.split_once(':')?;
            header.eq_ignore_ascii_case(name).then_some(value.trim())
        })
    }

    fn request_trace_id(head: &str) -> String {
        header_value(head, "traceparent")
            .and_then(|value| value.split('-').nth(1))
            .unwrap()
            .to_owned()
    }

    fn write_json_response(stream: &mut TcpStream, view: &TaskViewV1, trace: &str) {
        let body = serde_json::to_vec(view).unwrap();
        write!(
            stream,
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncache-control: no-store, private, max-age=0\r\ntrace-id: {trace}\r\netag: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            view.etag,
            body.len()
        )
        .unwrap();
        stream.write_all(&body).unwrap();
    }
}
