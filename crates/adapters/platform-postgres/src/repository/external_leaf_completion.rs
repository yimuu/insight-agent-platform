//! Reconstruct completion from the terminal domain owner and the original frozen leaf wait.
//! The Job is a continuation reference; it never substitutes for those committed facts.
use super::*;
use insight_platform_orchestrator::{ExternalLeafCompletion, ExternalLeafCompletionOwner};

#[derive(Clone, Copy)]
pub(super) struct CompletionValidation<'a> {
    pub plan: &'a RuntimePlan,
    pub context: ContextQueryLimits,
    pub model: ModelTurnLimits,
    pub scope: ScopeEnvironmentLimits,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn require_committed_external_leaf_completion(
    transaction: &mut Transaction<'_, Postgres>,
    current_job: &JobRecord,
    run: &RunRecord,
    source_node: &ControllerSourceNode,
    runtime_node: &RuntimeNode,
    limits: CompletionValidation<'_>,
) -> Result<(), RepositoryError> {
    let job_payload = decode_orchestration_job_payload(&current_job.payload)?;
    let completion =
        job_payload
            .external_leaf_completion
            .as_ref()
            .ok_or(RepositoryError::Conflict(
                "external leaf completion reference",
            ))?;
    let tenant_id: ResourceId =
        run.tenant_id
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?;
    let run_id: ResourceId =
        run.run_id
            .parse()
            .map_err(|failure: insight_platform_contracts::ResourceIdError| {
                RepositoryError::CorruptRow(failure.to_string())
            })?;
    let node_id = &job_payload.node_execution_id;
    if current_job.tenant_id != run.tenant_id
        || current_job.owner_id != node_id.to_string()
        || current_job.node_id.as_deref() != Some(current_job.owner_id.as_str())
        || current_job.run_id.as_deref() != Some(run.run_id.as_str())
        || job_payload.bindings_digest != run.bindings.canonical_digest
        || completion.source_orchestration_job_id.to_string() == current_job.job_id
    {
        return Err(RepositoryError::Conflict(
            "external leaf continuation owner",
        ));
    }
    let row = sqlx::query(
        "SELECT payload_schema_version, payload, payload_digest FROM insight_platform.run_nodes WHERE tenant_id=$1 AND run_id=$2 AND node_id=$3 AND record_kind='node_execution' AND state='running' AND terminal_at IS NULL AND scope_id=$4 AND plan_node_key=$5 AND version=$6"
    ).bind(&run.tenant_id).bind(&run.run_id).bind(node_id.to_string())
        .bind(&source_node.scope_id).bind(source_node.plan_node_key.as_str()).bind(source_node.version)
        .fetch_optional(&mut **transaction).await?
        .ok_or(RepositoryError::Conflict("external leaf current Node"))?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let (source_job_id, root_scope_id, output_port, wait_plan_key) = match (
        &completion.owner,
        runtime_node,
    ) {
        (
            ExternalLeafCompletionOwner::Child {
                child_run_id,
                child_link_id,
            },
            RuntimeNode::ChildAgentCall { output, resume, .. },
        ) => {
            let wait: StoredChildRunWaitPayload =
                decode_typed_payload(&payload, "Child completed wait")?;
            if &wait.child_run_id != child_run_id
                || &wait.child_link_id != child_link_id
                || &wait.output_port != output
                || &wait.resume_plan_node_key != resume
            {
                return Err(RepositoryError::Conflict("Child completion frozen wait"));
            }
            let link =
                load_child_run_link_by_id(transaction, &run.tenant_id, &child_link_id.to_string())
                    .await?;
            let child = load_run(transaction, &tenant_id, child_run_id).await?;
            if link.parent_run_id != run.run_id
                || link.parent_node_execution_id != node_id.to_string()
                || link.child_run_id != child_run_id.to_string()
                || link.state != ChildLinkState::Succeeded
                || link.terminal_at.is_none()
                || child.state != RunState::Succeeded.as_str()
                || child.terminal_at.is_none()
                || child.parent_run_id.as_deref() != Some(run.run_id.as_str())
            {
                return Err(RepositoryError::Conflict("Child completion terminal owner"));
            }
            let child_output = child
                .output_value_id
                .as_ref()
                .ok_or(RepositoryError::Conflict("Child completion output"))?;
            let exact: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM insight_platform.run_values child JOIN insight_platform.run_values parent ON parent.tenant_id=child.tenant_id AND parent.run_id=$6 AND parent.value_id=$7 WHERE child.tenant_id=$1 AND child.run_id=$2 AND child.value_id=$3 AND child.schema_digest=$4 AND child.content_digest=$5 AND parent.classification=child.classification AND parent.artifact_id IS NOT DISTINCT FROM child.artifact_id AND (parent.inline_value IS NOT NULL)=(child.inline_value IS NOT NULL))")
                .bind(&run.tenant_id).bind(&child.run_id).bind(child_output)
                .bind(completion.output.schema_digest.to_string()).bind(completion.output.content_digest.to_string())
                .bind(&run.run_id).bind(completion.output.value_id.to_string())
                .fetch_one(&mut **transaction).await?;
            if !exact {
                return Err(RepositoryError::Conflict("Child completion copied output"));
            }
            (
                wait.source_orchestration_job_id,
                wait.root_scope_id,
                wait.output_port,
                wait.plan_node_key,
            )
        }
        (
            ExternalLeafCompletionOwner::Context {
                context_query_id,
                context_job_id,
            },
            RuntimeNode::ContextQuery { result, resume, .. },
        ) => {
            let wait: StoredContextQueryWaitPayload =
                decode_typed_payload(&payload, "Context completed wait")?;
            if &wait.context_query_id != context_query_id
                || &wait.context_job_id != context_job_id
                || &wait.result_port != result
                || &wait.resume_plan_node_key != resume
            {
                return Err(RepositoryError::Conflict("Context completion frozen wait"));
            }
            let query = crate::context_query_repository::load_context_query(
                transaction,
                &tenant_id,
                context_query_id,
                false,
                limits.context,
            )
            .await?;
            if query.run_id != run_id
                || &query.node_execution_id != node_id
                || query.state != insight_platform_contracts::ContextQueryState::Succeeded
                || query.terminal_at.is_none()
                || query.payload.current_job_id.as_ref() != Some(context_job_id)
                || query.output_value_id.as_ref() != Some(&completion.output.value_id)
            {
                return Err(RepositoryError::Conflict(
                    "Context completion terminal owner",
                ));
            }
            require_exact_result(
                transaction,
                current_job,
                &query
                    .payload
                    .result
                    .as_ref()
                    .ok_or(RepositoryError::Conflict("Context completion result"))?
                    .output,
                completion,
            )
            .await?;
            require_terminal_owner_job(transaction, current_job, context_job_id, context_query_id)
                .await?;
            (
                wait.source_orchestration_job_id,
                wait.root_scope_id,
                wait.result_port,
                wait.plan_node_key,
            )
        }
        (
            ExternalLeafCompletionOwner::Model {
                model_turn_id,
                model_job_id,
            },
            RuntimeNode::ModelLoop { output, resume, .. },
        ) => {
            let wait: StoredModelTurnWaitPayload =
                decode_typed_payload(&payload, "Model completed wait")?;
            if &wait.model_turn_id != model_turn_id
                || &wait.model_job_id != model_job_id
                || &wait.output_port != output
                || &wait.resume_plan_node_key != resume
            {
                return Err(RepositoryError::Conflict("Model completion frozen wait"));
            }
            let turn = crate::model_turn_repository::load_model_turn_for_replay(
                transaction,
                &tenant_id,
                model_turn_id,
                limits.model,
            )
            .await?;
            if turn.run_id != run_id
                || &turn.node_execution_id != node_id
                || turn.state != insight_platform_contracts::ModelTurnState::Succeeded
                || turn.terminal_at.is_none()
                || turn.payload.current_job_id.as_ref() != Some(model_job_id)
            {
                return Err(RepositoryError::Conflict("Model completion terminal owner"));
            }
            let result = turn
                .payload
                .result
                .as_ref()
                .ok_or(RepositoryError::Conflict("Model completion result"))?;
            if result.finish_reason != insight_platform_models::CanonicalFinishReason::Completed
                || result.tool_intent_count != 0
            {
                return Err(RepositoryError::Conflict(
                    "Model completion final structured result",
                ));
            }
            require_model_structured_result(
                transaction,
                current_job,
                &turn,
                completion,
                limits.plan,
            )
            .await?;
            require_terminal_owner_job(transaction, current_job, model_job_id, model_turn_id)
                .await?;
            (
                wait.source_orchestration_job_id,
                wait.root_scope_id,
                wait.output_port,
                wait.plan_node_key,
            )
        }
        (
            ExternalLeafCompletionOwner::Capability {
                invocation_id,
                invocation_job_id,
            },
            RuntimeNode::CapabilityCall { output, resume, .. },
        ) => {
            let wait: StoredCapabilityInvocationWaitPayload =
                decode_typed_payload(&payload, "Capability completed wait")?;
            if &wait.invocation_id != invocation_id
                || wait.capability_job_id.as_ref() != Some(invocation_job_id)
                || &wait.output_port != output
                || &wait.resume_plan_node_key != resume
            {
                return Err(RepositoryError::Conflict(
                    "Capability completion frozen wait",
                ));
            }
            let invocation = crate::invocation_repository::load_capability_invocation(
                transaction,
                &tenant_id,
                invocation_id,
                false,
            )
            .await?;
            if invocation.run_id != run_id
                || &invocation.node_execution_id != node_id
                || invocation.state != InvocationState::Succeeded
                || invocation.terminal_at.is_none()
                || invocation.payload.current_job_id.as_ref() != Some(invocation_job_id)
                || invocation.output_value_id.as_ref() != Some(&completion.output.value_id)
            {
                return Err(RepositoryError::Conflict(
                    "Capability completion terminal owner",
                ));
            }
            require_exact_result(
                transaction,
                current_job,
                &invocation
                    .payload
                    .result
                    .as_ref()
                    .ok_or(RepositoryError::Conflict("Capability completion result"))?
                    .output,
                completion,
            )
            .await?;
            require_terminal_owner_job(transaction, current_job, invocation_job_id, invocation_id)
                .await?;
            (
                wait.source_orchestration_job_id,
                wait.root_scope_id,
                wait.output_port,
                wait.plan_node_key,
            )
        }
        _ => {
            return Err(RepositoryError::Conflict(
                "external completion RuntimeNode family",
            ))
        }
    };
    // Resume comes from the actual RuntimeNode above; no continuation-supplied target is used.
    if source_job_id != completion.source_orchestration_job_id
        || root_scope_id != job_payload.root_scope_id
        || wait_plan_key != source_node.plan_node_key
        || output_port.schema_digest() != &completion.output.schema_digest
    {
        return Err(RepositoryError::Conflict(
            "external completion frozen contract",
        ));
    }
    let source = load_job_by_text(transaction, &run.tenant_id, &source_job_id.to_string()).await?;
    require_orchestration_job(&source)?;
    let source_payload = decode_orchestration_job_payload(&source.payload)?;
    if source.state != JobState::Succeeded.as_str()
        || source.terminal_at.is_none()
        || source.owner_id != current_job.owner_id
        || source.node_id != current_job.node_id
        || source.run_id != current_job.run_id
        || source_payload.node_execution_id != *node_id
        || source_payload.root_scope_id != job_payload.root_scope_id
        || source_payload.bindings_digest != job_payload.bindings_digest
        || source_payload.external_leaf_completion.is_some()
        || source.execution_requirement != current_job.execution_requirement
    {
        return Err(RepositoryError::Conflict(
            "external completion source orchestration Job",
        ));
    }
    let exact: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM insight_platform.run_values WHERE tenant_id=$1 AND run_id=$2 AND node_id=$3 AND value_id=$4 AND schema_digest=$5 AND content_digest=$6)")
        .bind(&run.tenant_id).bind(&run.run_id).bind(node_id.to_string()).bind(completion.output.value_id.to_string())
        .bind(completion.output.schema_digest.to_string()).bind(completion.output.content_digest.to_string())
        .fetch_one(&mut **transaction).await?;
    if !exact {
        return Err(RepositoryError::Conflict(
            "external completion exact RunValue",
        ));
    }
    let scope_id: ResourceId = source_node.scope_id.parse().map_err(
        |failure: insight_platform_contracts::ResourceIdError| {
            RepositoryError::CorruptRow(failure.to_string())
        },
    )?;
    let environments =
        load_scope_environment_chain(transaction, &tenant_id, &run_id, &scope_id, limits.scope)
            .await?;
    let outputs = insight_platform_orchestrator::resolve_scope_inputs(
        &[output_port],
        &environments,
        limits.scope,
    )?;
    if outputs.as_slice() != [completion.output.clone()] {
        return Err(RepositoryError::Conflict(
            "external completion exact Scope output binding",
        ));
    }
    Ok(())
}

/// A ModelTurn owns the complete response. Its Plan leaf returns the distinct structured value
/// committed with that response; the continuation must prove the derivation from the owner.
async fn require_model_structured_result(
    transaction: &mut Transaction<'_, Postgres>,
    current: &JobRecord,
    turn: &insight_platform_models::ModelTurnRecord,
    completion: &ExternalLeafCompletion,
    plan: &RuntimePlan,
) -> Result<(), RepositoryError> {
    let result = turn
        .payload
        .result
        .as_ref()
        .ok_or(RepositoryError::Conflict("Model completion result"))?;
    let response_ref = &result.output;
    response_ref
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    if turn.output_value_id.as_ref() != Some(&response_ref.value_id)
        || response_ref.value_id == completion.output.value_id
        || response_ref.run_id != turn.run_id
        || response_ref.producing_node_id.as_ref() != Some(&turn.node_execution_id)
        || response_ref.value_kind != "model_response"
        || response_ref.storage != InvocationValueStorage::Inline
        || response_ref.content_digest != result.response_digest
    {
        return Err(RepositoryError::Conflict(
            "Model completion response reference",
        ));
    }
    let response: serde_json::Value = sqlx::query_scalar(
        "SELECT inline_value FROM insight_platform.run_values WHERE tenant_id=$1 AND run_id=$2 AND node_id=$3 AND value_id=$4 AND value_kind='model_response' AND classification=$5 AND schema_digest=$6 AND content_digest=$7 AND artifact_id IS NULL AND inline_value IS NOT NULL",
    )
    .bind(&current.tenant_id)
    .bind(turn.run_id.to_string())
    .bind(turn.node_execution_id.to_string())
    .bind(response_ref.value_id.to_string())
    .bind(response_ref.classification.as_str())
    .bind(response_ref.schema_digest.to_string())
    .bind(response_ref.content_digest.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Model completion response value"))?;
    if canonical_digest(&response)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?
        != result.response_digest.as_str()
    {
        return Err(RepositoryError::Conflict(
            "Model completion response digest",
        ));
    }
    let response: insight_platform_models::CanonicalModelResponse =
        serde_json::from_value(response)
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    if response.schema_version != 1
        || response.finish_reason != result.finish_reason
        || response.finish_reason != insight_platform_models::CanonicalFinishReason::Completed
        || !response.tool_intents.is_empty()
        || result.tool_intent_count != 0
    {
        return Err(RepositoryError::Conflict(
            "Model completion response outcome",
        ));
    }
    let structured = response.structured_output.ok_or(RepositoryError::Conflict(
        "Model completion structured response",
    ))?;
    structured
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    if structured.schema_digest != response_ref.schema_digest
        || structured.schema_digest != completion.output.schema_digest
        || structured.canonical_digest != completion.output.content_digest
    {
        return Err(RepositoryError::Conflict(
            "Model completion structured contract",
        ));
    }
    plan.validate_value_instance(&structured.schema_digest, &structured.value)?;
    let exact = ExactInvocationValueRef {
        schema_version: 1,
        value_id: completion.output.value_id.clone(),
        run_id: turn.run_id.clone(),
        producing_node_id: Some(turn.node_execution_id.clone()),
        value_kind: "model_structured_output".to_owned(),
        classification: response_ref.classification,
        schema_digest: structured.schema_digest,
        content_digest: structured.canonical_digest,
        storage: InvocationValueStorage::Inline,
    };
    require_exact_result(transaction, current, &exact, completion).await?;
    let same_body: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM insight_platform.run_values WHERE tenant_id=$1 AND run_id=$2 AND node_id=$3 AND value_id=$4 AND inline_value=$5)",
    )
    .bind(&current.tenant_id)
    .bind(turn.run_id.to_string())
    .bind(turn.node_execution_id.to_string())
    .bind(exact.value_id.to_string())
    .bind(structured.value)
    .fetch_one(&mut **transaction)
    .await?;
    if !same_body {
        return Err(RepositoryError::Conflict(
            "Model completion structured value",
        ));
    }
    Ok(())
}

async fn require_exact_result(
    transaction: &mut Transaction<'_, Postgres>,
    current: &JobRecord,
    result: &ExactInvocationValueRef,
    completion: &ExternalLeafCompletion,
) -> Result<(), RepositoryError> {
    result
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    if result.value_id != completion.output.value_id
        || result.schema_digest != completion.output.schema_digest
        || result.content_digest != completion.output.content_digest
        || current.run_id.as_deref() != Some(result.run_id.to_string().as_str())
        || result.producing_node_id.as_ref().map(ToString::to_string) != current.node_id
    {
        return Err(RepositoryError::Conflict(
            "external completion exact domain result",
        ));
    }
    let artifact_id = match &result.storage {
        InvocationValueStorage::Inline => None,
        InvocationValueStorage::Artifact { artifact } => Some(artifact.artifact_id().to_string()),
    };
    let exact: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM insight_platform.run_values WHERE tenant_id=$1 AND run_id=$2 AND value_id=$3 AND classification=$4 AND value_kind=$5 AND artifact_id IS NOT DISTINCT FROM $6 AND (inline_value IS NOT NULL)=$7)")
        .bind(&current.tenant_id).bind(result.run_id.to_string()).bind(result.value_id.to_string())
        .bind(result.classification.as_str()).bind(&result.value_kind).bind(&artifact_id).bind(artifact_id.is_none())
        .fetch_one(&mut **transaction).await?;
    if !exact {
        return Err(RepositoryError::Conflict(
            "external completion result classification and storage",
        ));
    }
    Ok(())
}

async fn require_terminal_owner_job(
    transaction: &mut Transaction<'_, Postgres>,
    current: &JobRecord,
    job_id: &ResourceId,
    owner_id: &ResourceId,
) -> Result<(), RepositoryError> {
    let job = load_job_by_text(transaction, &current.tenant_id, &job_id.to_string()).await?;
    let expected_kind = match owner_id.kind() {
        ResourceKind::ContextQuery => matches!(
            job.job_kind.as_str(),
            "context_query_native" | "context_query_remote"
        ),
        ResourceKind::ModelTurn => job.job_kind == JobKind::ModelTurn.as_str(),
        ResourceKind::CapabilityInvocation => {
            job.job_kind == JobKind::CapabilityInvocation.as_str()
        }
        _ => {
            return Err(RepositoryError::Conflict(
                "external completion Job owner kind",
            ))
        }
    };
    if !expected_kind
        || job.owner_kind != owner_id.kind().descriptor().name
        || job.state != JobState::Succeeded.as_str()
        || job.terminal_at.is_none()
        || job.owner_id != owner_id.to_string()
        || job.run_id != current.run_id
        || job.node_id != current.node_id
    {
        return Err(RepositoryError::Conflict(
            "external completion terminal domain Job",
        ));
    }
    Ok(())
}
