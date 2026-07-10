use chrono::{TimeZone, Utc};
use insight_agent_platform::{
    dsl::compiled::RunOutput,
    events::protocol::{RunEvent, RunEventScope, RunEventType},
    history::types::{
        NewRun, NodeOutputRecord, RunAttachment, RunRecord, RunStatus, RunSummary, TerminalUpdate,
    },
};
use serde_json::json;

fn at(second: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 10, 0, 0, second).unwrap()
}

fn scope(node_id: Option<&str>) -> RunEventScope {
    RunEventScope {
        request_id: "req_1".to_string(),
        run_id: "run_1".to_string(),
        agent_id: "researcher".to_string(),
        agent_version: "sha256:abc".to_string(),
        node_id: node_id.map(str::to_string),
    }
}

#[test]
fn node_event_serializes_the_exact_formal_v1_envelope() {
    let event = RunEvent::ok_at(
        RunEventType::NodeCompleted,
        4,
        scope(Some("plan")),
        at(4),
        json!({"output":{"text":"done"}}),
    );

    let value = serde_json::to_value(event).unwrap();

    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["type"], "node.completed");
    assert_eq!(value["seq"], 4);
    assert_eq!(value["request_id"], "req_1");
    assert_eq!(value["run_id"], "run_1");
    assert_eq!(value["agent_id"], "researcher");
    assert_eq!(value["agent_version"], "sha256:abc");
    assert_eq!(value["node_id"], "plan");
    assert_eq!(value["time"], "2026-07-10T00:00:04Z");
    assert_eq!(value["code"], "OK");
    assert_eq!(value["message"], "ok");
    assert_eq!(value["data"], json!({"output":{"text":"done"}}));
    assert_eq!(value.as_object().unwrap().len(), 12);
}

#[test]
fn run_events_omit_node_id_and_error_events_keep_stable_string_codes() {
    let run_event = RunEvent::ok_at(
        RunEventType::RunStarted,
        2,
        scope(Some("must_be_ignored")),
        at(2),
        json!({}),
    );
    let value = serde_json::to_value(run_event).unwrap();
    assert!(value.get("node_id").is_none());

    let failed = RunEvent::error_at(
        RunEventType::NodeFailed,
        3,
        scope(Some("answer")),
        at(3),
        "UPSTREAM_FAILURE",
        "model request failed",
        json!({}),
    );
    let value = serde_json::to_value(failed).unwrap();
    assert_eq!(value["code"], "UPSTREAM_FAILURE");
    assert_eq!(value["message"], "model request failed");
    assert_eq!(value["node_id"], "answer");
}

#[test]
fn formal_event_type_set_is_exact_and_uses_dotted_names() {
    let types = [
        RunEventType::RunCreated,
        RunEventType::RunStarted,
        RunEventType::NodeStarted,
        RunEventType::ContentDelta,
        RunEventType::NodeCompleted,
        RunEventType::NodeFailed,
        RunEventType::RunCompleted,
        RunEventType::RunFailed,
        RunEventType::RunCancelled,
        RunEventType::RunInterrupted,
    ];
    let serialized = types
        .into_iter()
        .map(|event_type| serde_json::to_value(event_type).unwrap())
        .collect::<Vec<_>>();

    assert_eq!(
        serialized,
        vec![
            json!("run.created"),
            json!("run.started"),
            json!("node.started"),
            json!("content.delta"),
            json!("node.completed"),
            json!("node.failed"),
            json!("run.completed"),
            json!("run.failed"),
            json!("run.cancelled"),
            json!("run.interrupted"),
        ]
    );
}

#[test]
fn statuses_and_attachments_serialize_as_snake_case() {
    let statuses = [
        RunStatus::Created,
        RunStatus::Running,
        RunStatus::Completed,
        RunStatus::Failed,
        RunStatus::Cancelled,
        RunStatus::Interrupted,
    ];
    let values = statuses
        .into_iter()
        .map(|status| serde_json::to_value(status).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        vec![
            json!("created"),
            json!("running"),
            json!("completed"),
            json!("failed"),
            json!("cancelled"),
            json!("interrupted"),
        ]
    );
    assert_eq!(
        serde_json::to_value(RunAttachment::Attached).unwrap(),
        json!("attached")
    );
    assert_eq!(
        serde_json::to_value(RunAttachment::Detached).unwrap(),
        json!("detached")
    );
}

#[test]
fn only_completed_failed_cancelled_and_interrupted_are_terminal() {
    assert!(!RunStatus::Created.is_terminal());
    assert!(!RunStatus::Running.is_terminal());
    assert!(RunStatus::Completed.is_terminal());
    assert!(RunStatus::Failed.is_terminal());
    assert!(RunStatus::Cancelled.is_terminal());
    assert!(RunStatus::Interrupted.is_terminal());
}

#[test]
fn terminal_update_rejects_nonterminal_statuses() {
    let error =
        TerminalUpdate::new("run_1", RunStatus::Running, at(10), None, None, None).unwrap_err();
    assert_eq!(error.code(), "TERMINAL_STATUS_REQUIRED");

    let output = RunOutput {
        content: Some("answer".to_string()),
        format: Some("text".to_string()),
        data: json!({"answer":"answer"}),
    };
    let update = TerminalUpdate::new(
        "run_1",
        RunStatus::Completed,
        at(10),
        Some(output.clone()),
        None,
        None,
    )
    .unwrap();
    assert_eq!(update.run_id, "run_1");
    assert_eq!(update.status, RunStatus::Completed);
    assert_eq!(update.output, Some(output));
}

#[test]
fn formal_history_records_preserve_version_attachment_and_sanitized_terminal_data() {
    let new_run = NewRun {
        run_id: "run_1".to_string(),
        request_id: "req_1".to_string(),
        agent_id: "researcher".to_string(),
        agent_version: "sha256:abc".to_string(),
        attachment: RunAttachment::Detached,
        created_at: at(0),
        input_summary: json!({"keys":["question"], "serialized_bytes":24}),
    };
    let record = RunRecord {
        run_id: new_run.run_id.clone(),
        request_id: new_run.request_id.clone(),
        agent_id: new_run.agent_id.clone(),
        agent_version: new_run.agent_version.clone(),
        attachment: new_run.attachment,
        status: RunStatus::Failed,
        started_at: Some(at(1)),
        ended_at: Some(at(2)),
        updated_at: at(2),
        input_summary: new_run.input_summary.clone(),
        output: None,
        error_code: Some("UPSTREAM_FAILURE".to_string()),
        error_message: Some("model request failed".to_string()),
    };
    let summary = RunSummary::from(&record);
    let node_output = NodeOutputRecord {
        run_id: "run_1".to_string(),
        node_id: "plan".to_string(),
        output: json!({"text":"done"}),
        completed_at: at(2),
    };

    assert_eq!(summary.agent_version, "sha256:abc");
    assert_eq!(summary.attachment, RunAttachment::Detached);
    assert_eq!(summary.error_code.as_deref(), Some("UPSTREAM_FAILURE"));
    assert_eq!(node_output.node_id, "plan");
    assert_eq!(node_output.output, json!({"text":"done"}));
}
