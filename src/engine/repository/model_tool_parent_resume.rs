#[allow(unused_imports)]
pub(crate) use insight_durable::model_tool_parent_resume::adapter::*;
pub use insight_durable::ModelToolParentResume;

use crate::engine::EffectEvidence;

pub(crate) type LatestParentModelCallView = (
    (u32, String, u64, String),
    (String, Option<String>, Option<String>, Option<String>),
);

#[allow(clippy::too_many_arguments)]
pub(crate) fn latest_parent_model_call_view(
    model_call_no: u32,
    task_id: String,
    lease_epoch: u64,
    fencing_token: String,
    call_status: String,
    finish_reason: Option<String>,
    execution_status: Option<String>,
    continuation_status: Option<String>,
) -> LatestParentModelCallView {
    (
        (model_call_no, task_id, lease_epoch, fencing_token),
        (
            call_status,
            finish_reason,
            execution_status,
            continuation_status,
        ),
    )
}

pub(crate) fn latest_model_call_no(latest: &LatestParentModelCallView) -> u32 {
    latest.0 .0
}

pub(crate) fn latest_task_id(latest: &LatestParentModelCallView) -> &str {
    &latest.0 .1
}

pub(crate) fn latest_lease_epoch(latest: &LatestParentModelCallView) -> u64 {
    latest.0 .2
}

pub(crate) fn latest_fencing_token(latest: &LatestParentModelCallView) -> &str {
    &latest.0 .3
}

pub(crate) fn latest_execution_status(latest: &LatestParentModelCallView) -> Option<&str> {
    latest.1 .2.as_deref()
}

pub(crate) fn latest_continuation_status(latest: &LatestParentModelCallView) -> Option<&str> {
    latest.1 .3.as_deref()
}

fn latest_statuses(latest: &LatestParentModelCallView) -> LatestParentModelCallStatuses<'_> {
    (
        &latest.1 .0,
        latest.1 .1.as_deref(),
        latest.1 .2.as_deref(),
        latest.1 .3.as_deref(),
    )
}

pub(crate) fn classify_parent_task_claim(
    task_state: &str,
    attempt_lifecycle: &str,
    effect_evidence: EffectEvidence,
    latest: Option<&LatestParentModelCallView>,
) -> &'static str {
    insight_durable::model_tool_parent_resume::adapter::classify_parent_task_claim_statuses(
        task_state,
        attempt_lifecycle,
        effect_evidence,
        latest.map(latest_statuses),
    )
}

pub(crate) fn latest_is_waiting_tools(latest: &LatestParentModelCallView) -> bool {
    let (call_status, finish_reason, execution_status, continuation_status) =
        latest_statuses(latest);
    insight_durable::model_tool_parent_resume::adapter::latest_parent_model_call_is_waiting_tools(
        call_status,
        finish_reason,
        execution_status,
        continuation_status,
    )
}

pub(crate) fn latest_is_checkpointed(latest: &LatestParentModelCallView) -> bool {
    let (call_status, finish_reason, execution_status, continuation_status) =
        latest_statuses(latest);
    insight_durable::model_tool_parent_resume::adapter::latest_parent_model_call_is_checkpointed(
        call_status,
        finish_reason,
        execution_status,
        continuation_status,
    )
}

pub(crate) fn latest_is_ready(latest: &LatestParentModelCallView) -> bool {
    let (call_status, finish_reason, execution_status, continuation_status) =
        latest_statuses(latest);
    insight_durable::model_tool_parent_resume::adapter::latest_parent_model_call_is_ready(
        call_status,
        finish_reason,
        execution_status,
        continuation_status,
    )
}
