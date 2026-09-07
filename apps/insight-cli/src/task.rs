//! Closed public Task lifecycle client for the Runtime Gateway.

use crate::public_client::{PublicClientError, PublicHttpClient, PublicJsonResponse};
use crate::task_journal::{self, TaskControlJournalV1, TaskJournalError};
#[cfg(test)]
use insight_platform_api::task::TaskOwnerLinkV2;
use insight_platform_api::{
    product::ListPageV1,
    task::{SubmitTaskInputV1, TaskInboxFiltersV1, TaskViewV2},
};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, JsonLimits, ResourceId, ResourceKind, ValueRef,
};
use insight_platform_tasks::{TaskAction, TaskQueryPurpose};
#[cfg(test)]
use insight_platform_tasks::{TaskKind, TaskState};
use reqwest::StatusCode;
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

fn action_route(action: TaskAction) -> &'static str {
    match action {
        TaskAction::SubmitInput => "submit-input",
        TaskAction::Approve => "approve",
        TaskAction::Reject => "reject",
        TaskAction::Cancel => "cancel",
    }
}
fn purpose_string(purpose: TaskQueryPurpose) -> &'static str {
    match purpose {
        TaskQueryPurpose::Respondable => "respondable",
        TaskQueryPurpose::Viewable => "viewable",
    }
}

pub fn list_tasks(
    client: &PublicHttpClient,
    filters: &TaskInboxFiltersV1,
    page_size: u16,
    cursor: Option<&str>,
) -> Result<ListPageV1<TaskViewV2>, TaskClientError> {
    if page_size == 0
        || page_size > 50
        || filters
            .run_id
            .as_ref()
            .is_some_and(|id| id.kind() != ResourceKind::Run)
    {
        return Err(TaskClientError::InvalidRequest(
            "invalid Task list bounds".into(),
        ));
    }
    let mut query = vec![
        ("purpose".into(), purpose_string(filters.purpose).into()),
        ("page_size".into(), page_size.to_string()),
    ];
    if let Some(state) = filters.state {
        query.push(("state".into(), state.to_string()));
    }
    if let Some(kind) = filters.kind {
        query.push(("kind".into(), kind.to_string()));
    }
    if let Some(run_id) = &filters.run_id {
        query.push(("run_id".into(), run_id.to_string()));
    }
    if let Some(cursor) = cursor {
        query.push(("cursor".into(), cursor.into()));
    }
    let response = client.get_body_json_query::<ListPageV1<TaskViewV2>>(
        "/v1/tasks",
        &query,
        StatusCode::OK,
    )?;
    let page = response.body;
    if page.schema_version != 1
        || page.items.len() > usize::from(page_size)
        || page.items.iter().any(|view| view.validate().is_err())
    {
        return Err(TaskClientError::InvalidResponse("invalid Task page".into()));
    }
    Ok(page)
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
    purpose: TaskQueryPurpose,
) -> Result<TaskViewV2, TaskClientError> {
    require_task_id(task_id)?;
    let response: PublicJsonResponse<TaskViewV2> = client.get_json_query(
        &format!("/v1/tasks/{task_id}"),
        &[("purpose".into(), purpose_string(purpose).into())],
        StatusCode::OK,
    )?;
    validate_task_response(&response, task_id)?;
    Ok(response.body)
}

pub fn resolve_task(
    client: &PublicHttpClient,
    task_id: &ResourceId,
    action: TaskAction,
    input: Option<&SubmitTaskInputV1>,
    journal_directory: &Path,
) -> Result<TaskViewV2, TaskClientError> {
    require_task_id(task_id)?;
    if (action == TaskAction::SubmitInput) != input.is_some() {
        return Err(TaskClientError::InvalidRequest(
            "submit-input requires exactly one closed input document".to_owned(),
        ));
    }
    let path = task_journal::journal_path(journal_directory, task_id, action_route(action));
    let mut journal = match task_journal::load(&path)? {
        Some(journal) => {
            validate_journal(&journal, task_id, action, input)?;
            if let Some(result) = &journal.result {
                if Some(result.state) != action.target(result.task_kind) {
                    return Err(TaskClientError::InvalidResponse(
                        "Task journal result does not match its action".to_owned(),
                    ));
                }
                return read_task(client, task_id, TaskQueryPurpose::Respondable);
            }
            journal
        }
        None => {
            let current = read_task(client, task_id, TaskQueryPurpose::Respondable)?;
            let receipt = mutation_receipt(task_id, action, &current.etag, input)?;
            TaskControlJournalV1::new(
                task_id.clone(),
                action_route(action).to_owned(),
                receipt,
                current.etag,
                input.cloned(),
            )
        }
    };
    task_journal::save(&path, &journal)?;
    let path = format!("/v1/tasks/{task_id}:{}", action_route(action));
    let response: PublicJsonResponse<TaskViewV2> = match journal.input.as_ref() {
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
    if Some(response.body.state) != action.target(response.body.task_kind) {
        return Err(TaskClientError::InvalidResponse(
            "Task mutation did not return its exact terminal state".to_owned(),
        ));
    }
    journal.result = Some(response.body.clone());
    task_journal::save(
        &task_journal::journal_path(journal_directory, task_id, action_route(action)),
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
        || journal.action != action_route(action)
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

fn mutation_receipt(
    task_id: &ResourceId,
    action: TaskAction,
    if_match: &str,
    input: Option<&SubmitTaskInputV1>,
) -> Result<String, TaskClientError> {
    let digest = canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "operation": format!("task.{}", action_route(action).replace('-', "_")),
        "task_id": task_id,
        "if_match": if_match,
        "input": input,
    }))
    .map_err(|error| TaskClientError::InvalidRequest(error.to_string()))?;
    Ok(format!(
        "insight-task-v1-{}-{}",
        digest.strip_prefix("sha256:").unwrap_or(&digest),
        action_route(action)
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
    response: &PublicJsonResponse<TaskViewV2>,
    expected_task_id: &ResourceId,
) -> Result<(), TaskClientError> {
    let view = &response.body;
    if view.validate().is_err() || &view.task_id != expected_task_id || response.etag != view.etag {
        return Err(TaskClientError::InvalidResponse(
            "Task view or ETag violates its closed contract".to_owned(),
        ));
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
    use insight_platform_contracts::{ApiProblem, ApiProblemCode, TraceId};
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
        let view = TaskViewV2 {
            schema_version: 2,
            allowed_actions: Vec::new(),
            task_id: task_id.clone(),
            task_kind: TaskKind::InteractionForm,
            state: TaskState::Pending,
            generation: 1,
            version: 2,
            safe_prompt_key: "interaction.collect_details".to_owned(),
            response_schema_digest: Some(digest('b').parse().unwrap()),
            owner: TaskOwnerLinkV2::Run {
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
        invalid.owner = TaskOwnerLinkV2::Artifact {
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
        let pending = task_view(task_id.clone(), TaskState::Pending, 1);
        let responded = task_view(task_id.clone(), TaskState::Responded, 2);
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
            assert!(head.starts_with(&format!(
                "GET /v1/tasks/{server_task_id}?purpose=respondable HTTP/1.1"
            )));
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
            assert!(head.starts_with(&format!(
                "GET /v1/tasks/{server_task_id}?purpose=respondable HTTP/1.1"
            )));
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

    #[test]
    fn task_mutations_preserve_closed_failure_matrix() {
        let submit_input = parse_submit_input(
            &serde_json::to_vec(&json!({
                "classification": "internal",
                "schema_digest": digest('a'),
                "value": {"kind": "inline", "value": {"answer": 42}}
            }))
            .unwrap(),
        )
        .unwrap();
        let cases = [
            (
                TaskAction::SubmitInput,
                TaskState::Expired,
                "409 Conflict",
                409,
                ApiProblemCode::InvalidStateTransition,
                false,
                None,
            ),
            (
                TaskAction::Approve,
                TaskState::Pending,
                "403 Forbidden",
                403,
                ApiProblemCode::PermissionDenied,
                false,
                None,
            ),
            (
                TaskAction::Reject,
                TaskState::Pending,
                "412 Precondition Failed",
                412,
                ApiProblemCode::PreconditionFailed,
                false,
                None,
            ),
            (
                TaskAction::Cancel,
                TaskState::Pending,
                "429 Too Many Requests",
                429,
                ApiProblemCode::RateLimited,
                true,
                Some(250),
            ),
            (
                TaskAction::Approve,
                TaskState::Pending,
                "503 Service Unavailable",
                503,
                ApiProblemCode::TemporarilyUnavailable,
                true,
                Some(500),
            ),
            (
                TaskAction::Approve,
                TaskState::Rejected,
                "409 Conflict",
                409,
                ApiProblemCode::InvalidStateTransition,
                false,
                None,
            ),
            (
                TaskAction::Reject,
                TaskState::Approved,
                "409 Conflict",
                409,
                ApiProblemCode::InvalidStateTransition,
                false,
                None,
            ),
            (
                TaskAction::Cancel,
                TaskState::Approved,
                "409 Conflict",
                409,
                ApiProblemCode::InvalidStateTransition,
                false,
                None,
            ),
        ];

        for (action, state, status_line, status, code, retryable, retry_after_ms) in cases {
            let task_id = id(if action == TaskAction::SubmitInput {
                ResourceKind::Interaction
            } else {
                ResourceKind::ApprovalTask
            });
            let current = if action == TaskAction::SubmitInput {
                task_view(task_id.clone(), state, 1)
            } else {
                approval_task_view(task_id.clone(), state, 1)
            };
            let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let port = listener.local_addr().unwrap().port();
            let server_task_id = task_id.clone();
            let server_current = current.clone();
            let server_code = code;
            let server = thread::spawn(move || {
                let (mut read, _) = listener.accept().unwrap();
                let (head, _) = read_request(&mut read);
                assert!(head.starts_with(&format!(
                    "GET /v1/tasks/{server_task_id}?purpose=respondable HTTP/1.1"
                )));
                write_json_response(
                    &mut read,
                    &server_current,
                    "11111111111111111111111111111111",
                );

                let (mut mutation, _) = listener.accept().unwrap();
                let (head, body) = read_request(&mut mutation);
                assert!(head.starts_with(&format!(
                    "POST /v1/tasks/{server_task_id}:{} HTTP/1.1",
                    action_route(action)
                )));
                assert_eq!(
                    header_value(&head, "if-match"),
                    Some(server_current.etag.as_str())
                );
                assert!(header_value(&head, "idempotency-key")
                    .is_some_and(|value| value.ends_with(action_route(action))));
                if action == TaskAction::SubmitInput {
                    assert!(!body.is_empty());
                } else {
                    assert!(body.is_empty());
                }
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
            let input = (action == TaskAction::SubmitInput).then_some(&submit_input);
            match resolve_task(&client, &task_id, action, input, journals.path()).unwrap_err() {
                TaskClientError::Public(PublicClientError::Problem(actual)) => {
                    assert_eq!(actual.status, status);
                    assert_eq!(actual.code, code);
                    assert_eq!(actual.retryable, retryable);
                    assert_eq!(actual.retry_after_ms, retry_after_ms);
                }
                other => panic!("expected closed Task Problem, got {other:?}"),
            }
            server.join().unwrap();
        }
    }

    #[test]
    fn each_kind_reject_and_cancel_require_their_exact_terminal_state() {
        for (kind, action, terminal) in [
            (
                ResourceKind::ApprovalTask,
                TaskAction::Approve,
                TaskState::Approved,
            ),
            (
                ResourceKind::ApprovalTask,
                TaskAction::Reject,
                TaskState::Rejected,
            ),
            (
                ResourceKind::ApprovalTask,
                TaskAction::Cancel,
                TaskState::Cancelled,
            ),
            (
                ResourceKind::Interaction,
                TaskAction::Reject,
                TaskState::Declined,
            ),
            (
                ResourceKind::Interaction,
                TaskAction::Cancel,
                TaskState::Cancelled,
            ),
        ] {
            let task_id = id(kind);
            let view = if kind == ResourceKind::ApprovalTask {
                approval_task_view
            } else {
                task_view
            };
            let pending = view(task_id.clone(), TaskState::Pending, 1);
            let resolved = view(task_id.clone(), terminal, 2);
            let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let port = listener.local_addr().unwrap().port();
            let server_task_id = task_id.clone();
            let server_pending = pending.clone();
            let server_resolved = resolved.clone();
            let server = thread::spawn(move || {
                let (mut read, _) = listener.accept().unwrap();
                let (head, _) = read_request(&mut read);
                assert!(head.starts_with(&format!(
                    "GET /v1/tasks/{server_task_id}?purpose=respondable HTTP/1.1"
                )));
                write_json_response(
                    &mut read,
                    &server_pending,
                    "11111111111111111111111111111111",
                );

                let (mut mutation, _) = listener.accept().unwrap();
                let (head, body) = read_request(&mut mutation);
                assert!(head.starts_with(&format!(
                    "POST /v1/tasks/{server_task_id}:{} HTTP/1.1",
                    action_route(action)
                )));
                assert!(body.is_empty());
                assert_eq!(
                    header_value(&head, "if-match"),
                    Some(server_pending.etag.as_str())
                );
                let trace_id = request_trace_id(&head);
                write_json_response(&mut mutation, &server_resolved, &trace_id);
            });
            let client = PublicHttpClient::new(
                format!("http://127.0.0.1:{port}"),
                "token".to_owned(),
                Duration::from_secs(2),
            )
            .unwrap();
            let journals = TempDir::new().unwrap();
            assert_eq!(
                resolve_task(&client, &task_id, action, None, journals.path()).unwrap(),
                resolved
            );
            server.join().unwrap();
        }
    }

    fn task_view(task_id: ResourceId, state: TaskState, version: u64) -> TaskViewV2 {
        TaskViewV2 {
            schema_version: 2,
            allowed_actions: Vec::new(),
            task_id: task_id.clone(),
            task_kind: TaskKind::InteractionForm,
            state,
            generation: 1,
            version,
            safe_prompt_key: "interaction.collect_details".to_owned(),
            response_schema_digest: Some(digest('b').parse().unwrap()),
            owner: TaskOwnerLinkV2::Run {
                run_id: id(ResourceKind::Run),
            },
            deadline: "2026-08-30T00:00:00.000000Z".parse().unwrap(),
            responded_at: (state != TaskState::Pending)
                .then(|| "2026-08-29T00:00:02.000000Z".parse().unwrap()),
            created_at: "2026-08-29T00:00:00.000000Z".parse().unwrap(),
            updated_at: "2026-08-29T00:00:01.000000Z".parse().unwrap(),
            etag: format!("\"{task_id}-{version}\""),
        }
    }

    fn approval_task_view(task_id: ResourceId, state: TaskState, version: u64) -> TaskViewV2 {
        TaskViewV2 {
            task_kind: TaskKind::Approval,
            response_schema_digest: None,
            safe_prompt_key: "approval.review_effect".to_owned(),
            ..task_view(task_id, state, version)
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
            title: "Task request rejected".to_owned(),
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

    fn write_json_response(stream: &mut TcpStream, view: &TaskViewV2, trace: &str) {
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
