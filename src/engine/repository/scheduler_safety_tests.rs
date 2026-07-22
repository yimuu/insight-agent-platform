use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex as StdMutex,
    },
};

use crate::engine::worker::{
    LeafTaskExecutor, ModelCallAuthority, ModelCallCompletion, ModelFinishReason,
    ModelFunctionCallPublication, ModelIncompleteFunctionCallPublication, ModelTokenUsage,
    ModelToolCall, ModelToolCallBatch, ResponseItemAuthority, TaskExecutionRequest,
    WorkerExecutionContext, WorkerExecutorRegistry, WorkerFailure, WorkerFailureClass,
};
use crate::{
    dsl::v3::{compile_source, CompileOptions},
    engine::{
        plan::{
            DescriptorConfigurationContract, DescriptorContract, DescriptorContractRegistry,
            DescriptorFieldContract, DescriptorValueSchema, LeafTaskKind, LinkedPlan, NodeKind,
            Plan, PortDirection, SubflowContractRegistry, VersionTag, WorkerContract,
            WorkerInputPortContract,
        },
        DefinitionRevisionId, DeploymentRevisionId, EffectEvidence, EffectIdempotency, RunId,
        RunLifecycle, RuntimeValue, SchedulerQuiescence, SchedulerTaskKind, TaskExecutionResult,
        TerminationReason, TransitionKey, TransitionOutcome, WorkerCancellation, WorkerEffectClass,
        WorkerEffectPolicy,
    },
};
use chrono::{DateTime, Duration, Utc};
use insight_durable::scheduler_repository::adapter::SchedulerTaskFailureAdapter as _;
use serde_json::{json, Value};
use sqlx::{
    postgres::PgPoolOptions, sqlite::SqliteConnectOptions, AssertSqlSafe, PgPool, SqlitePool,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    consume_model_tool_task_once, drive_scheduler_until_quiescent, ClaimSchedulerRunCommand,
    CreateRunCommand, DurableRepository, FailOnceSchedulerCrash, FencedSchedulerRunCommand,
    ModelToolBatchActivationOutcome, ModelToolCallCheckpoint, ModelToolContinuationStatus,
    ModelToolFailureClass, ModelToolParentResume, ModelToolTaskDisposition,
    ModelToolTaskHeartbeatOutcome, ModelToolTaskOutcome, ModelToolTaskTransitionOutcome,
    ModelToolWorkerPumpOutcome, NoSchedulerCrash, PlanInstallOutcome, PostgresDurableRepository,
    ResponseUsageStatus, SchedulerCrashPoint, SchedulerDurableRepository,
    SchedulerFailureDisposition, SchedulerLeaseRepository, SchedulerRecoveryOutcome,
    SchedulerTaskClaim, SchedulerTaskClaimMode, SchedulerTaskCommitOutcome, SchedulerTaskFailure,
    SchedulerTaskHeartbeatOutcome, SchedulerTaskOutcome, SchedulerTaskSuccess,
    SqliteDurableRepository, VersionedPlan,
};

const DEADLINE_AGENT: &str = r#"api_version: insight.agent/v3
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - id: answer
      type: action
      call: fixture.deadline
      response: string
    - return: $answer
"#;

const MODEL_CALL_AGENT: &str = r#"api_version: insight.agent/v3
kind: agent
inputs: {}
output: string
workflow:
  steps:
    - id: answer
      type: llm
      model: general_chat
      publish: true
      messages:
        - role: user
          content:
            - text: answer the question
      response: string
    - return: $answer
"#;

fn key(label: &str, run_id: &RunId) -> TransitionKey {
    TransitionKey::derive(
        "scheduler.deadline.authority.test.v1",
        &[label, run_id.as_str()],
    )
    .unwrap()
}

fn deadline_fixture() -> (Plan, DescriptorContractRegistry, VersionedPlan) {
    let policy = WorkerEffectPolicy::frozen(
        WorkerEffectClass::ReadOnly,
        EffectIdempotency::Idempotent,
        2,
        0,
        0,
        2_000,
        WorkerCancellation::Cooperative,
    )
    .unwrap();
    let plan = compile_source(
        DEADLINE_AGENT,
        CompileOptions::new(
            DefinitionRevisionId::new("scheduler_deadline_authority_v1").unwrap(),
            "scheduler-deadline-authority.yaml",
            DEADLINE_AGENT,
        ),
    )
    .unwrap();
    let mut descriptors = DescriptorContractRegistry::new();
    for node in plan.nodes() {
        let NodeKind::ActionTask(descriptor) = node.kind() else {
            continue;
        };
        let inputs = plan
            .data_ports()
            .iter()
            .filter(|port| port.owner() == node.id() && port.direction() == PortDirection::Input)
            .map(|port| {
                (
                    port.name().clone(),
                    WorkerInputPortContract::new(port.value_type().clone(), port.required()),
                )
            })
            .collect();
        let outputs = plan
            .data_ports()
            .iter()
            .filter(|port| port.owner() == node.id() && port.direction() == PortDirection::Output)
            .map(|port| (port.name().clone(), port.value_type().clone()))
            .collect();
        let configuration = DescriptorConfigurationContract::closed(
            descriptor
                .public_configuration
                .keys()
                .map(|field| {
                    (
                        field.clone(),
                        DescriptorFieldContract::required(DescriptorValueSchema::Any),
                    )
                })
                .collect(),
            descriptor
                .secret_configuration
                .keys()
                .map(|field| (field.clone(), true))
                .collect(),
        );
        descriptors
            .register(DescriptorContract::new(
                descriptor.implementation.clone(),
                descriptor.descriptor_version.clone(),
                configuration,
                WorkerContract::new(
                    LeafTaskKind::Action,
                    VersionTag::new("worker-1").unwrap(),
                    inputs,
                    outputs,
                )
                .with_effect_policy(policy.clone()),
            ))
            .unwrap();
    }
    let versioned = VersionedPlan::from_verified_plan(
        "scheduler-deadline-authority",
        "scheduler-deadline-agent",
        "Scheduler deadline authority fixture",
        DeploymentRevisionId::new("scheduler_deadline_authority_deployment_v1").unwrap(),
        "expression-3.0.0",
        json!({"format": "structured-v3"}),
        &plan,
        json!({"fixture": "descriptor-v1"}),
        json!({}),
        json!({"fixture": "worker-1"}),
    )
    .unwrap();
    (plan, descriptors, versioned)
}

fn model_call_fixture() -> (Plan, DescriptorContractRegistry, VersionedPlan) {
    model_call_fixture_with_binding(model_tool_queue_binding())
}

fn model_call_fixture_with_binding(
    deployment_binding: serde_json::Value,
) -> (Plan, DescriptorContractRegistry, VersionedPlan) {
    model_call_fixture_from_source(
        MODEL_CALL_AGENT,
        deployment_binding,
        "scheduler_model_call_authority_v1",
        "scheduler_model_call_authority_deployment_v1",
    )
}

fn model_call_fixture_from_source(
    source: &str,
    deployment_binding: serde_json::Value,
    definition_revision_id: &str,
    deployment_revision_id: &str,
) -> (Plan, DescriptorContractRegistry, VersionedPlan) {
    model_call_fixture_from_source_with_effect_policy(
        source,
        deployment_binding,
        definition_revision_id,
        deployment_revision_id,
        None,
    )
}

fn model_call_fixture_from_source_with_effect_policy(
    source: &str,
    deployment_binding: serde_json::Value,
    definition_revision_id: &str,
    deployment_revision_id: &str,
    effect_policy: Option<WorkerEffectPolicy>,
) -> (Plan, DescriptorContractRegistry, VersionedPlan) {
    let plan = compile_source(
        source,
        CompileOptions::new(
            DefinitionRevisionId::new(definition_revision_id).unwrap(),
            "scheduler-model-call-authority.yaml",
            source,
        ),
    )
    .unwrap();
    let mut descriptors = DescriptorContractRegistry::new();
    for node in plan.nodes() {
        let NodeKind::LlmTask(descriptor) = node.kind() else {
            continue;
        };
        let inputs = plan
            .data_ports()
            .iter()
            .filter(|port| port.owner() == node.id() && port.direction() == PortDirection::Input)
            .map(|port| {
                (
                    port.name().clone(),
                    WorkerInputPortContract::new(port.value_type().clone(), port.required()),
                )
            })
            .collect();
        let outputs = plan
            .data_ports()
            .iter()
            .filter(|port| port.owner() == node.id() && port.direction() == PortDirection::Output)
            .map(|port| (port.name().clone(), port.value_type().clone()))
            .collect();
        let configuration = DescriptorConfigurationContract::closed(
            descriptor
                .public_configuration
                .keys()
                .map(|field| {
                    (
                        field.clone(),
                        DescriptorFieldContract::required(DescriptorValueSchema::Any),
                    )
                })
                .collect(),
            BTreeMap::new(),
        );
        let worker = WorkerContract::new(
            LeafTaskKind::Llm,
            VersionTag::new("model-call-worker-v1").unwrap(),
            inputs,
            outputs,
        );
        let worker = match &effect_policy {
            Some(policy) => worker.with_effect_policy(policy.clone()),
            None => worker,
        };
        descriptors
            .register(
                DescriptorContract::new(
                    descriptor.implementation.clone(),
                    descriptor.descriptor_version.clone(),
                    configuration,
                    worker,
                )
                .with_deployment_binding(deployment_binding.clone())
                .unwrap(),
            )
            .unwrap();
    }
    let versioned = VersionedPlan::from_verified_plan(
        "scheduler-model-call-authority",
        "scheduler-model-call-agent",
        "Scheduler model-call authority fixture",
        DeploymentRevisionId::new(deployment_revision_id).unwrap(),
        "expression-3.0.0",
        json!({"format": "structured-v3"}),
        &plan,
        json!({"fixture": "descriptor-v2"}),
        json!({}),
        json!({"fixture": "model-call-worker-v1"}),
    )
    .unwrap();
    (plan, descriptors, versioned)
}

fn model_tool_queue_binding() -> serde_json::Value {
    let policy = WorkerEffectPolicy::frozen(
        WorkerEffectClass::ReadOnly,
        EffectIdempotency::Idempotent,
        2,
        5_000,
        5_000,
        10_000,
        WorkerCancellation::Cooperative,
    )
    .unwrap();
    let non_idempotent = WorkerEffectPolicy::frozen(
        WorkerEffectClass::Mutating,
        EffectIdempotency::NonIdempotent,
        2,
        50,
        100,
        2_000,
        WorkerCancellation::LeaseOnly,
    )
    .unwrap();
    json!({
        "adapter": "core.llm",
        "model_alias": "fixture",
        "model_binding_hash": "sha256:fixture",
        "model_binding": {},
        "request_mode": "streaming_request",
        "request_capabilities": ["streaming_request"],
        "tool_choice": "auto",
        "tool_limits": {"max_rounds": 8, "max_calls": 32},
        "runtime_capabilities": ["runtime.llm_tool_continuation.v1"],
        "tools": [
            {
                "name": "weather",
                "action_id": "weather",
                "action_version": "1.0.0",
                "descriptor_hash": "a".repeat(64),
                "input_schema": {
                    "type": "object",
                    "properties": {"city": {"type": "string"}},
                    "required": ["city"],
                    "additionalProperties": false
                },
                "output_schema": {
                    "type": "object",
                    "properties": {"value": {"type": "string"}},
                    "required": ["value"],
                    "additionalProperties": false
                },
                "effect": "read_only",
                "idempotency": "idempotent",
                "cancellation": "cooperative",
                "required_capabilities": [],
                "effect_policy": policy,
                "public_policy": {"call": false, "arguments": "private", "result": null},
                "effective_public_policy": {"call": false, "arguments": "private", "result": null}
            },
            {
                "name": "clock",
                "action_id": "clock",
                "action_version": "1.0.0",
                "descriptor_hash": "b".repeat(64),
                "input_schema": {
                    "type": "object",
                    "properties": {"zone": {"type": "string"}},
                    "required": ["zone"],
                    "additionalProperties": false
                },
                "output_schema": {
                    "type": "object",
                    "properties": {"value": {"type": "string"}},
                    "required": ["value"],
                    "additionalProperties": false
                },
                "effect": "mutating",
                "idempotency": "non_idempotent",
                "cancellation": "not_supported",
                "required_capabilities": [],
                "effect_policy": non_idempotent,
                "public_policy": {"call": false, "arguments": "private", "result": null},
                "effective_public_policy": {"call": false, "arguments": "private", "result": null}
            }
        ]
    })
}

fn public_model_tool_queue_binding() -> serde_json::Value {
    let mut binding = model_tool_queue_binding();
    let public_policy = json!({
        "call": true,
        "arguments": "all",
        "result": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": {},
            "type": "object",
            "properties": {
                "value": {"type": "string", "enum": ["safe"]}
            },
            "required": ["value"],
            "additionalProperties": false
        }
    });
    binding["tools"][0]["public_policy"] = public_policy.clone();
    binding["tools"][0]["effective_public_policy"] = public_policy;
    binding
}

fn all_public_model_tool_queue_binding() -> serde_json::Value {
    let mut binding = public_model_tool_queue_binding();
    let policy = binding["tools"][0]["effective_public_policy"].clone();
    binding["tools"][1]["public_policy"] = policy.clone();
    binding["tools"][1]["effective_public_policy"] = policy;
    binding
}

fn retryable_public_model_call_fixture() -> (Plan, DescriptorContractRegistry, VersionedPlan) {
    let policy = WorkerEffectPolicy::frozen(
        WorkerEffectClass::ReadOnly,
        EffectIdempotency::Idempotent,
        2,
        0,
        0,
        60_000,
        WorkerCancellation::Cooperative,
    )
    .unwrap();
    model_call_fixture_from_source_with_effect_policy(
        MODEL_CALL_AGENT,
        all_public_model_tool_queue_binding(),
        "scheduler_retry_publication_v1",
        "scheduler_retry_publication_deployment_v1",
        Some(policy),
    )
}

fn field_public_model_tool_queue_binding() -> serde_json::Value {
    let mut binding = model_tool_queue_binding();
    let policy = json!({"call": true, "arguments": ["city"], "result": null});
    binding["tools"][0]["public_policy"] = policy.clone();
    binding["tools"][0]["effective_public_policy"] = policy;
    binding
}

fn artifact_public_model_tool_queue_binding() -> serde_json::Value {
    let mut binding = model_tool_queue_binding();
    let result_schema = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$defs": {},
        "type": "object",
        "properties": {
            "type": {"const": "output_image"},
            "artifact": {
                "type": "object",
                "properties": {
                    "artifact_id": {"type": "string"},
                    "content_hash": {"type": "string"},
                    "size_bytes": {"type": "integer"},
                    "media_type": {"type": "string"}
                },
                "required": ["artifact_id", "content_hash", "size_bytes", "media_type"],
                "additionalProperties": false
            }
        },
        "required": ["type", "artifact"],
        "additionalProperties": false
    });
    let public_policy = json!({
        "call": true,
        "arguments": "private",
        "result": result_schema.clone()
    });
    binding["tools"][0]["output_schema"] = result_schema;
    binding["tools"][0]["public_policy"] = public_policy.clone();
    binding["tools"][0]["effective_public_policy"] = public_policy;
    binding
}

fn success_for(claim: &SchedulerTaskClaim) -> SchedulerTaskOutcome {
    let output = claim
        .envelope()
        .request()
        .outputs()
        .first()
        .expect("deadline fixture output");
    SchedulerTaskOutcome::Succeeded(
        SchedulerTaskSuccess::inline(TaskExecutionResult::new(
            BTreeMap::from([(
                output.port_id().clone(),
                RuntimeValue::new(json!("late-success")).unwrap(),
            )]),
            EffectEvidence::Committed,
        ))
        .unwrap(),
    )
}

fn model_usage(base: u64) -> ModelTokenUsage {
    ModelTokenUsage {
        input_tokens: Some(base),
        cached_tokens: Some(base + 1),
        output_tokens: Some(base + 2),
        reasoning_tokens: Some(base + 3),
        total_tokens: Some(base + base + 2),
    }
}

fn tool_call_completion(model_call_no: u32, base: u64) -> ModelCallCompletion {
    ModelCallCompletion::new(
        model_call_no,
        ModelFinishReason::ToolCalls,
        Some(model_usage(base)),
        None,
        None,
    )
    .unwrap()
}

fn tool_call_checkpoint(model_call_no: u32, base: u64, argument: &str) -> ModelToolCallCheckpoint {
    tool_call_checkpoint_with_public_item(model_call_no, base, argument, None)
}

fn single_weather_tool_call_checkpoint(
    model_call_no: u32,
    base: u64,
    argument: &str,
) -> ModelToolCallCheckpoint {
    let completion = tool_call_completion(model_call_no, base);
    let batch = ModelToolCallBatch::new(
        model_call_no,
        None,
        vec![
            ModelToolCall::new(0, "call_weather_once", "weather", json!({"city": argument}))
                .unwrap(),
        ],
    )
    .unwrap();
    ModelToolCallCheckpoint::new(completion, batch).unwrap()
}

fn immediate_retry_model_tool_queue_binding() -> Value {
    let mut binding = model_tool_queue_binding();
    binding["tools"][0]["effect_policy"] = serde_json::to_value(
        WorkerEffectPolicy::frozen(
            WorkerEffectClass::ReadOnly,
            EffectIdempotency::Idempotent,
            2,
            0,
            0,
            10_000,
            WorkerCancellation::Cooperative,
        )
        .unwrap(),
    )
    .unwrap();
    binding
}

// This object models the external Action service and its idempotency ledger,
// so it intentionally survives a platform repository/process restart.
struct EffectIdempotentWeatherAction {
    invocations: AtomicUsize,
    external_applications: AtomicUsize,
    results_by_effect: StdMutex<BTreeMap<String, Value>>,
}

impl EffectIdempotentWeatherAction {
    fn new() -> Self {
        Self {
            invocations: AtomicUsize::new(0),
            external_applications: AtomicUsize::new(0),
            results_by_effect: StdMutex::new(BTreeMap::new()),
        }
    }

    fn invocations(&self) -> usize {
        self.invocations.load(Ordering::SeqCst)
    }

    fn external_applications(&self) -> usize {
        self.external_applications.load(Ordering::SeqCst)
    }

    fn effect_ids(&self) -> Vec<String> {
        self.results_by_effect
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect()
    }
}

#[async_trait::async_trait]
impl LeafTaskExecutor for EffectIdempotentWeatherAction {
    async fn execute(
        &self,
        _context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        _cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        let effect_id = request.effect_id().as_str().to_owned();
        let result = {
            let mut results = self.results_by_effect.lock().unwrap();
            results
                .entry(effect_id)
                .or_insert_with(|| {
                    self.external_applications.fetch_add(1, Ordering::SeqCst);
                    json!({"value": "effect-applied-once"})
                })
                .clone()
        };
        let output = request
            .outputs()
            .first()
            .expect("model-tool request has exactly one result port");
        Ok(TaskExecutionResult::new(
            BTreeMap::from([(output.port_id().clone(), RuntimeValue::new(result).unwrap())]),
            EffectEvidence::Committed,
        ))
    }
}

fn weather_action_registry(action: Arc<EffectIdempotentWeatherAction>) -> WorkerExecutorRegistry {
    let mut registry = WorkerExecutorRegistry::new();
    registry
        .register(
            SchedulerTaskKind::Action,
            "weather",
            VersionTag::new("1").unwrap(),
            VersionTag::new("1.0.0").unwrap(),
            action,
        )
        .unwrap();
    registry
}

fn tool_call_checkpoint_with_public_item(
    model_call_no: u32,
    base: u64,
    argument: &str,
    public_item: Option<ResponseItemAuthority>,
) -> ModelToolCallCheckpoint {
    let completion = tool_call_completion(model_call_no, base);
    let batch = ModelToolCallBatch::new(
        model_call_no,
        None,
        vec![
            ModelToolCall::new(0, "call_weather", "weather", json!({"city": argument})).unwrap(),
            ModelToolCall::new(1, "call_clock", "clock", json!({"zone": "UTC"})).unwrap(),
        ],
    )
    .unwrap();
    let batch = match public_item {
        Some(item) => batch
            .with_public_function_calls(
                vec![ModelFunctionCallPublication::new(0, item, 1).unwrap()],
            )
            .unwrap(),
        None => batch,
    };
    ModelToolCallCheckpoint::new(completion, batch).unwrap()
}

fn stop_completion(
    authority: &ModelCallAuthority,
    item: &ResponseItemAuthority,
    base: u64,
    text: &str,
) -> ModelCallCompletion {
    ModelCallCompletion::new(
        authority.model_call_no(),
        ModelFinishReason::Stop,
        Some(model_usage(base)),
        Some(8),
        Some(json!({
            "id": item.item_id(),
            "type": "message",
            "status": "completed",
            "role": "assistant",
            "content": [{
                "type": "output_text",
                "text": text,
                "annotations": [],
            }],
        })),
    )
    .unwrap()
}

fn failed_function_completion(
    model_call_no: u32,
    base: u64,
    functions: Vec<(u32, ResponseItemAuthority, &str, &str, u64)>,
) -> ModelCallCompletion {
    let functions = functions
        .into_iter()
        .map(|(call_index, item, call_id, tool_name, seal_index)| {
            ModelIncompleteFunctionCallPublication::new(
                call_index, item, call_id, tool_name, seal_index,
            )
            .unwrap()
        })
        .collect();
    ModelCallCompletion::new(
        model_call_no,
        ModelFinishReason::Invalid,
        Some(model_usage(base)),
        None,
        None,
    )
    .unwrap()
    .with_incomplete_function_calls(functions)
    .unwrap()
}

async fn exercise_failed_function_call_checkpoint<R>(
    repository: &R,
    claim: &SchedulerTaskClaim,
) -> (
    ResponseItemAuthority,
    ResponseItemAuthority,
    ModelCallCompletion,
)
where
    R: SchedulerDurableRepository + ?Sized,
{
    assert!(matches!(
        repository.reserve_model_call(claim, 1, true).await.unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    let weather = match repository
        .reserve_model_call_public_function_item(claim, 1, 0, "call_weather", "weather")
        .await
        .unwrap()
    {
        SchedulerTaskCommitOutcome::Committed { result } => result,
        other => panic!("weather function item was not reserved: {other:?}"),
    };
    let clock = match repository
        .reserve_model_call_public_function_item(claim, 1, 1, "call_clock", "clock")
        .await
        .unwrap()
    {
        SchedulerTaskCommitOutcome::Committed { result } => result,
        other => panic!("clock function item was not reserved: {other:?}"),
    };
    let completion = failed_function_completion(
        1,
        10,
        vec![
            (0, weather.clone(), "call_weather", "weather", 3),
            (1, clock.clone(), "call_clock", "clock", 7),
        ],
    );
    assert!(matches!(
        repository
            .checkpoint_model_call_completion(claim, &completion)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { result: () }
    ));
    assert_eq!(
        repository
            .checkpoint_model_call_completion(claim, &completion)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::ExactReplay { authoritative: () },
    );
    let conflict = failed_function_completion(
        1,
        10,
        vec![
            (0, weather.clone(), "call_weather", "weather", 4),
            (1, clock.clone(), "call_clock", "clock", 7),
        ],
    );
    assert_eq!(
        repository
            .checkpoint_model_call_completion(claim, &conflict)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::StateConflict,
    );
    (weather, clock, completion)
}

async fn exercise_public_retry_appends_new_item<R>(
    repository: &R,
    first_claim: &SchedulerTaskClaim,
) -> (ResponseItemAuthority, ResponseItemAuthority)
where
    R: SchedulerDurableRepository + ?Sized,
{
    assert_eq!(first_claim.envelope().attempt_no().get(), 1);
    assert!(matches!(
        repository
            .reserve_model_call(first_claim, 1, true)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    let failed_item = match repository
        .reserve_model_call_public_function_item(first_claim, 1, 0, "call_weather", "weather")
        .await
        .unwrap()
    {
        SchedulerTaskCommitOutcome::Committed { result } => result,
        other => panic!("retry fixture did not reserve the failed item: {other:?}"),
    };
    let completion = failed_function_completion(
        1,
        10,
        vec![(0, failed_item.clone(), "call_weather", "weather", 2)],
    );
    assert!(matches!(
        repository
            .checkpoint_model_call_completion(first_claim, &completion)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { result: () }
    ));
    let worker_failure = WorkerFailure::new(
        WorkerFailureClass::InfrastructureFailure,
        "RETRYABLE_PROVIDER_FAILURE",
        true,
    )
    .unwrap();
    let retry = SchedulerTaskOutcome::Failed(
        SchedulerTaskFailure::from_worker_failure(
            first_claim,
            &worker_failure,
            EffectEvidence::Started,
            SchedulerFailureDisposition::Retry {
                retry_at: Utc::now() - Duration::seconds(1),
                remaining_attempts: 1,
            },
        )
        .unwrap(),
    );
    assert!(matches!(
        repository
            .commit_scheduler_task_outcome(first_claim, &retry)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));

    let second_claim = repository
        .claim_scheduler_tasks("retry-publication-worker", 60, 32)
        .await
        .unwrap()
        .into_iter()
        .find(|claim| claim.run_id() == first_claim.run_id())
        .expect("retry Attempt must become claimable");
    assert_eq!(second_claim.activation_id(), first_claim.activation_id());
    assert_eq!(second_claim.envelope().attempt_no().get(), 2);
    assert!(matches!(
        repository
            .mark_scheduler_task_started(&second_claim)
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let authority = match repository
        .reserve_model_call(&second_claim, 1, true)
        .await
        .unwrap()
    {
        SchedulerTaskCommitOutcome::Committed { result } => result,
        other => panic!("retry model call was not reserved: {other:?}"),
    };
    let completed_item = match repository
        .reserve_model_call_public_item(&second_claim, 1)
        .await
        .unwrap()
    {
        SchedulerTaskCommitOutcome::Committed { result } => result,
        other => panic!("retry response item was not reserved: {other:?}"),
    };
    assert_ne!(failed_item.item_id(), completed_item.item_id());
    assert!(completed_item.output_index() > failed_item.output_index());
    let completion = stop_completion(&authority, &completed_item, 20, "retry succeeded");
    assert!(matches!(
        repository
            .checkpoint_model_call_completion(&second_claim, &completion)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { result: () }
    ));
    assert!(matches!(
        repository
            .commit_scheduler_task_outcome(&second_claim, &success_for(&second_claim))
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    assert!(repository
        .acknowledge_scheduler_task(&second_claim)
        .await
        .unwrap());
    (failed_item, completed_item)
}

async fn checkpoint_failed_model_call_with_usage<R>(
    repository: &R,
    claim: &SchedulerTaskClaim,
    base: u64,
) where
    R: SchedulerDurableRepository + ?Sized,
{
    assert!(matches!(
        repository.reserve_model_call(claim, 1, true).await.unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    let completion = ModelCallCompletion::new(
        1,
        ModelFinishReason::Invalid,
        Some(model_usage(base)),
        None,
        None,
    )
    .unwrap();
    assert!(matches!(
        repository
            .checkpoint_model_call_completion(claim, &completion)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { result: () }
    ));
}

async fn exercise_model_call_authority<R>(
    repository: &R,
    claim: &SchedulerTaskClaim,
    run_id: &RunId,
) where
    R: SchedulerDurableRepository + ?Sized,
{
    let first = match repository.reserve_model_call(claim, 1, true).await.unwrap() {
        SchedulerTaskCommitOutcome::Committed { result } => result,
        other => panic!("first model call was not reserved: {other:?}"),
    };
    assert_eq!(first.response_id(), format!("resp_{}", run_id.as_str()));
    assert_eq!(first.model_call_no(), 1);
    assert!(first.publication_enabled());
    assert!(first.public_item().is_none());
    assert_eq!(
        repository.reserve_model_call(claim, 1, true).await.unwrap(),
        SchedulerTaskCommitOutcome::ExactReplay {
            authoritative: first.clone(),
        },
        "reservation replay must preserve identity and append-only output index",
    );
    assert!(repository
        .reserve_model_call(claim, 1, false)
        .await
        .is_err());
    assert_eq!(
        repository.reserve_model_call(claim, 2, true).await.unwrap(),
        SchedulerTaskCommitOutcome::StateConflict,
        "continuation cannot start before the prior tool-call checkpoint",
    );

    let first_checkpoint = tool_call_checkpoint(1, 10, "Shanghai");
    assert!(matches!(
        repository
            .checkpoint_model_tool_call_batch(claim, &first_checkpoint)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { result: () }
    ));
    assert_eq!(
        repository
            .checkpoint_model_tool_call_batch(claim, &first_checkpoint)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::ExactReplay { authoritative: () },
        "tool-call checkpoint replay must not append a second batch",
    );
    let conflicting_checkpoint = tool_call_checkpoint(1, 10, "Beijing");
    assert_eq!(
        repository
            .checkpoint_model_tool_call_batch(claim, &conflicting_checkpoint)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::StateConflict,
        "same model-call identity cannot be overwritten with a different tool batch",
    );

    assert!(matches!(
        repository
            .activate_model_tool_call_batch(claim, 1)
            .await
            .unwrap(),
        ModelToolBatchActivationOutcome::Activated(_)
    ));
    let tool_claims = repository
        .claim_model_tool_calls("model-call-test-tool-worker", 60, 8, 8)
        .await
        .unwrap();
    assert_eq!(tool_claims.len(), 2);
    for tool_claim in tool_claims {
        assert!(matches!(
            repository
                .mark_model_tool_call_started(&tool_claim)
                .await
                .unwrap(),
            ModelToolTaskTransitionOutcome::Committed(())
        ));
        assert!(matches!(
            repository
                .commit_model_tool_call_outcome(
                    &tool_claim,
                    &ModelToolTaskOutcome::succeeded(json!({"value": "ok"})).unwrap(),
                )
                .await
                .unwrap(),
            ModelToolTaskTransitionOutcome::Committed(_)
        ));
    }
    let continuation_claim = repository
        .claim_scheduler_tasks("model-call-test-continuation-worker", 60, 1)
        .await
        .unwrap()
        .pop()
        .expect("ready tool barrier must wake exactly one parent task");
    assert_eq!(continuation_claim.mode(), SchedulerTaskClaimMode::Execute);
    assert!(matches!(
        repository
            .load_model_tool_parent_resume(&continuation_claim)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed {
            result: Some(ModelToolParentResume::ReadyContinue {
                completed_model_call_no: 1,
                next_model_call_no: 2,
                ..
            })
        }
    ));

    let second = match repository
        .reserve_model_call(&continuation_claim, 2, true)
        .await
        .unwrap()
    {
        SchedulerTaskCommitOutcome::Committed { result } => result,
        other => panic!("continuation model call was not reserved: {other:?}"),
    };
    assert_eq!(second.model_call_no(), 2);
    assert!(second.publication_enabled());
    assert!(second.public_item().is_none());
    assert_eq!(
        repository
            .reserve_model_call(&continuation_claim, 2, true)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::ExactReplay {
            authoritative: second.clone(),
        },
    );
    let second_item = match repository
        .reserve_model_call_public_item(&continuation_claim, 2)
        .await
        .unwrap()
    {
        SchedulerTaskCommitOutcome::Committed { result } => result,
        other => panic!("continuation public item was not allocated: {other:?}"),
    };
    assert_eq!(second_item.output_index(), 0);
    assert_eq!(
        repository
            .reserve_model_call_public_item(&continuation_claim, 2)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::ExactReplay {
            authoritative: second_item.clone(),
        },
        "lazy item replay must preserve its append-only index",
    );
    let second_completion = stop_completion(&second, &second_item, 100, "durable answer");
    assert!(matches!(
        repository
            .checkpoint_model_call_completion(&continuation_claim, &second_completion)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { result: () }
    ));
    assert_eq!(
        repository
            .reserve_model_call(&continuation_claim, 3, true)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::StateConflict,
        "a stop finish cannot create a continuation call",
    );

    assert!(matches!(
        repository
            .commit_scheduler_task_outcome(&continuation_claim, &success_for(&continuation_claim),)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    assert_eq!(
        repository
            .checkpoint_model_call_completion(&continuation_claim, &second_completion)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::StaleLease,
        "terminal/outcome cutoff must reject late telemetry even when values match",
    );
    assert_eq!(
        repository
            .reserve_model_call(&continuation_claim, 3, true)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::StaleLease,
    );
    assert!(repository
        .acknowledge_scheduler_task(&continuation_claim)
        .await
        .unwrap());
}

async fn exercise_concurrent_function_call_public_item_authority<R>(
    repository: &R,
    claim: &SchedulerTaskClaim,
) -> (ResponseItemAuthority, ResponseItemAuthority)
where
    R: SchedulerDurableRepository + ?Sized,
{
    assert!(matches!(
        repository.reserve_model_call(claim, 1, true).await.unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    let (weather, clock) = tokio::join!(
        repository.reserve_model_call_public_function_item(claim, 1, 0, "call_weather", "weather",),
        repository.reserve_model_call_public_function_item(claim, 1, 1, "call_clock", "clock",),
    );
    let weather = match weather.unwrap() {
        SchedulerTaskCommitOutcome::Committed { result } => result,
        other => panic!("weather function item was not allocated: {other:?}"),
    };
    let clock = match clock.unwrap() {
        SchedulerTaskCommitOutcome::Committed { result } => result,
        other => panic!("clock function item was not allocated: {other:?}"),
    };
    let mut output_indices = [weather.output_index(), clock.output_index()];
    output_indices.sort_unstable();
    assert_eq!(output_indices, [0, 1]);
    assert_ne!(weather.item_id(), clock.item_id());
    assert_eq!(
        repository
            .reserve_model_call_public_function_item(claim, 1, 0, "call_weather", "weather",)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::ExactReplay {
            authoritative: weather.clone(),
        },
    );
    (weather, clock)
}

async fn exercise_dynamic_function_call_checkpoint_and_activation<R>(
    repository: &R,
    claim: &SchedulerTaskClaim,
    weather_item: ResponseItemAuthority,
    clock_item: ResponseItemAuthority,
) where
    R: SchedulerDurableRepository + ?Sized,
{
    let batch = ModelToolCallBatch::new(
        1,
        None,
        vec![
            ModelToolCall::new(0, "call_weather", "weather", json!({"city": "Shanghai"})).unwrap(),
            ModelToolCall::new(1, "call_clock", "clock", json!({"zone": "UTC"})).unwrap(),
        ],
    )
    .unwrap()
    .with_public_function_calls(vec![
        ModelFunctionCallPublication::new(0, weather_item, 3).unwrap(),
        ModelFunctionCallPublication::new(1, clock_item, 1).unwrap(),
    ])
    .unwrap();
    let checkpoint = ModelToolCallCheckpoint::new(tool_call_completion(1, 10), batch).unwrap();
    assert!(matches!(
        repository
            .checkpoint_model_tool_call_batch(claim, &checkpoint)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { result: () }
    ));
    assert_eq!(
        repository
            .checkpoint_model_tool_call_batch(claim, &checkpoint)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::ExactReplay { authoritative: () },
    );
    let activation = match repository
        .activate_model_tool_call_batch(claim, 1)
        .await
        .unwrap()
    {
        ModelToolBatchActivationOutcome::Activated(activation) => activation,
        _ => panic!("public function batch was not activated"),
    };
    assert_eq!(activation.tasks().len(), 2);
    assert_eq!(activation.tasks()[0].public_seal_index(), Some(5));
    assert_eq!(activation.tasks()[1].public_seal_index(), Some(3));
    assert_eq!(
        activation.tasks()[0].public_arguments_jcs(),
        Some(r#"{"city":"Shanghai"}"#),
    );
    assert_eq!(
        activation.tasks()[1].public_arguments_jcs(),
        Some(r#"{"zone":"UTC"}"#),
    );
    match repository
        .activate_model_tool_call_batch(claim, 1)
        .await
        .unwrap()
    {
        ModelToolBatchActivationOutcome::ExactReplay(replay) => {
            assert_eq!(replay.tasks(), activation.tasks());
        }
        _ => panic!("public activation did not exact-replay"),
    }
}

async fn assert_model_call_snapshot<R>(repository: &R, run_id: &RunId)
where
    R: SchedulerDurableRepository + ?Sized,
{
    let snapshot = repository
        .load_response_snapshot(run_id)
        .await
        .unwrap()
        .expect("terminal response snapshot");
    assert_eq!(snapshot.usage_status(), ResponseUsageStatus::Complete);
    assert_eq!(
        snapshot.usage(),
        Some(&json!({
            "input_tokens": 110,
            "input_tokens_details": {"cached_tokens": 112},
            "output_tokens": 114,
            "output_tokens_details": {"reasoning_tokens": 116},
            "total_tokens": 224,
        })),
        "exact checkpoint replay must be aggregated once per model-call identity",
    );
    let manifest = snapshot.public_item_manifest().as_array().unwrap();
    assert_eq!(manifest.len(), 1);
    assert_eq!(manifest[0]["output_index"], 0);
    assert_eq!(manifest[0]["status"], "completed");
}

async fn drive_until_response_snapshot<R>(
    repository: &R,
    linked: &LinkedPlan<'_>,
    fence: &FencedSchedulerRunCommand,
    run_id: &RunId,
) where
    R: SchedulerDurableRepository + ?Sized,
{
    for _ in 0..8 {
        if repository
            .load_response_snapshot(run_id)
            .await
            .unwrap()
            .is_some()
        {
            return;
        }
        assert!(matches!(
            super::drive_scheduler_once(repository, linked, fence, &NoSchedulerCrash)
                .await
                .unwrap(),
            super::SchedulerDriveOutcome::Applied(_)
        ));
    }
    panic!("scheduler did not commit a terminal response snapshot");
}

async fn prepare_sqlite_task(
    repository: &SqliteDurableRepository,
    control: &SqlitePool,
    versioned: &VersionedPlan,
    linked: &LinkedPlan<'_>,
    run_id: &RunId,
) -> (SchedulerTaskClaim, FencedSchedulerRunCommand) {
    assert!(matches!(
        repository
            .create_run(
                key("create", run_id),
                CreateRunCommand::new(run_id.clone(), versioned, json!({})).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let owner = format!("scheduler-{}", run_id.as_str());
    let fencing_token = format!("fence-{}", run_id.as_str());
    assert_eq!(
        sqlx::query(
            "UPDATE workflow_runs SET lifecycle='active',started_at=CURRENT_TIMESTAMP,
                    scheduler_lease_epoch=1,scheduler_lease_owner=?,
                    scheduler_fencing_token=?,
                    scheduler_lease_expires_at=datetime('now','+1 hour'),
                    scheduler_heartbeat_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP
             WHERE run_id=? AND lifecycle='created'",
        )
        .bind(&owner)
        .bind(&fencing_token)
        .bind(run_id.as_str())
        .execute(control)
        .await
        .unwrap()
        .rows_affected(),
        1
    );
    let fence = FencedSchedulerRunCommand::new(run_id.clone(), owner, 1, fencing_token).unwrap();
    assert!(matches!(
        drive_scheduler_until_quiescent(repository, linked, &fence, &NoSchedulerCrash, 32)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
    ));
    let claim = repository
        .claim_scheduler_tasks(&format!("worker-{}", run_id.as_str()), 60, 16)
        .await
        .unwrap()
        .into_iter()
        .find(|claim| claim.run_id() == run_id)
        .unwrap();
    assert!(matches!(
        repository
            .mark_scheduler_task_started(&claim)
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let claim = match repository
        .heartbeat_scheduler_task(&claim, 60)
        .await
        .unwrap()
    {
        SchedulerTaskHeartbeatOutcome::Renewed(renewed) => renewed,
        other => panic!("fresh SQLite deadline claim did not renew: {other:?}"),
    };
    (claim, fence)
}

async fn expire_sqlite_scheduler_task_claim(control: &SqlitePool, run_id: &RunId) {
    let expired_at = sqlx::query_scalar::<_, String>("SELECT datetime('now','-1 second')")
        .fetch_one(control)
        .await
        .unwrap();
    sqlx::query("UPDATE task_outbox SET claim_expires_at=? WHERE run_id=?")
        .bind(&expired_at)
        .bind(run_id.as_str())
        .execute(control)
        .await
        .unwrap();
    sqlx::query("UPDATE node_attempts SET lease_expires_at=? WHERE run_id=?")
        .bind(&expired_at)
        .bind(run_id.as_str())
        .execute(control)
        .await
        .unwrap();
}

async fn prepare_postgres_task(
    repository: &PostgresDurableRepository,
    control: &PgPool,
    versioned: &VersionedPlan,
    linked: &LinkedPlan<'_>,
    run_id: &RunId,
) -> (SchedulerTaskClaim, FencedSchedulerRunCommand) {
    assert!(matches!(
        repository
            .create_run(
                key("create", run_id),
                CreateRunCommand::new(run_id.clone(), versioned, json!({})).unwrap(),
            )
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    assert_eq!(
        sqlx::query(
            "UPDATE workflow_runs SET lifecycle='active',started_at=CURRENT_TIMESTAMP,
                    updated_at=CURRENT_TIMESTAMP
             WHERE run_id=$1 AND lifecycle='created'",
        )
        .bind(run_id.as_str())
        .execute(control)
        .await
        .unwrap()
        .rows_affected(),
        1
    );
    let lease = repository
        .claim_scheduler_run(
            key("scheduler-claim", run_id),
            ClaimSchedulerRunCommand::new(
                run_id.clone(),
                format!("scheduler-{}", run_id.as_str()),
                60,
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .committed_result()
        .cloned()
        .unwrap();
    let fence = lease.fence().unwrap();
    assert!(matches!(
        drive_scheduler_until_quiescent(repository, linked, &fence, &NoSchedulerCrash, 32)
            .await
            .unwrap(),
        SchedulerRecoveryOutcome::Quiescent(SchedulerQuiescence::WaitingForTask { .. })
    ));
    let claim = repository
        .claim_scheduler_tasks(&format!("worker-{}", run_id.as_str()), 60, 16)
        .await
        .unwrap()
        .into_iter()
        .find(|claim| claim.run_id() == run_id)
        .unwrap();
    assert!(matches!(
        repository
            .mark_scheduler_task_started(&claim)
            .await
            .unwrap(),
        TransitionOutcome::Committed { .. }
    ));
    let claim = match repository
        .heartbeat_scheduler_task(&claim, 60)
        .await
        .unwrap()
    {
        SchedulerTaskHeartbeatOutcome::Renewed(renewed) => renewed,
        other => panic!("fresh PostgreSQL deadline claim did not renew: {other:?}"),
    };
    (claim, fence)
}

async fn prepare_sqlite_model_tool_batch(
    run_name: &str,
    deployment_binding: serde_json::Value,
) -> (
    tempfile::TempDir,
    SqliteDurableRepository,
    SqlitePool,
    RunId,
    SchedulerTaskClaim,
) {
    let publishes_raw_weather_arguments = deployment_binding["tools"][0]["effective_public_policy"]
        ["call"]
        == json!(true)
        && deployment_binding["tools"][0]["effective_public_policy"]["arguments"] == json!("all");
    let (plan, descriptors, versioned) = model_call_fixture_with_binding(deployment_binding);
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join(format!("{run_name}.sqlite"));
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .foreign_keys(true),
    )
    .await
    .unwrap();
    let run_id = RunId::new(run_name).unwrap();
    let (parent, _) =
        prepare_sqlite_task(&repository, &control, &versioned, &linked, &run_id).await;
    assert!(matches!(
        repository
            .reserve_model_call(&parent, 1, true)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    let public_item = if publishes_raw_weather_arguments {
        Some(
            match repository
                .reserve_model_call_public_function_item(&parent, 1, 0, "call_weather", "weather")
                .await
                .unwrap()
            {
                SchedulerTaskCommitOutcome::Committed { result } => result,
                other => panic!("function-call item was not reserved: {other:?}"),
            },
        )
    } else {
        None
    };
    assert!(matches!(
        repository
            .checkpoint_model_tool_call_batch(
                &parent,
                &tool_call_checkpoint_with_public_item(1, 10, "Shanghai", public_item),
            )
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    assert!(matches!(
        repository
            .activate_model_tool_call_batch(&parent, 1)
            .await
            .unwrap(),
        ModelToolBatchActivationOutcome::Activated(_)
    ));
    (directory, repository, control, run_id, parent)
}

async fn prepare_sqlite_single_weather_batch(
    run_name: &str,
) -> (
    tempfile::TempDir,
    PathBuf,
    SqliteDurableRepository,
    SqlitePool,
    RunId,
) {
    let (plan, descriptors, versioned) =
        model_call_fixture_with_binding(immediate_retry_model_tool_queue_binding());
    let linked = LinkedPlan::link(&plan, &descriptors, &SubflowContractRegistry::new()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join(format!("{run_name}.sqlite"));
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .foreign_keys(true),
    )
    .await
    .unwrap();
    let run_id = RunId::new(run_name).unwrap();
    let (parent, _) =
        prepare_sqlite_task(&repository, &control, &versioned, &linked, &run_id).await;
    assert!(matches!(
        repository
            .reserve_model_call(&parent, 1, true)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    assert!(matches!(
        repository
            .checkpoint_model_tool_call_batch(
                &parent,
                &single_weather_tool_call_checkpoint(1, 10, "Shanghai"),
            )
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    assert!(matches!(
        repository
            .activate_model_tool_call_batch(&parent, 1)
            .await
            .unwrap(),
        ModelToolBatchActivationOutcome::Activated(_)
    ));
    (directory, database, repository, control, run_id)
}

async fn drive_terminal_scheduler_action<R>(
    repository: &R,
    linked: &LinkedPlan<'_>,
    fence: &FencedSchedulerRunCommand,
    expected: SchedulerQuiescence,
) where
    R: SchedulerDurableRepository + ?Sized,
{
    for _ in 0..16 {
        match super::drive_scheduler_once(repository, linked, fence, &NoSchedulerCrash)
            .await
            .unwrap()
        {
            super::SchedulerDriveOutcome::Applied(_) => {}
            super::SchedulerDriveOutcome::Quiescent(actual) if actual == expected => return,
            other => panic!("terminal scheduler action diverged: {other:?}"),
        }
    }
    panic!("terminal scheduler action did not quiesce");
}

async fn sqlite_checkpoint_intent_snapshot(
    control: &SqlitePool,
    run_id: &RunId,
) -> (String, Vec<String>) {
    let batch = sqlx::query_scalar::<_, String>(
        "SELECT json_object(
            'execution_status',execution_status,
            'continuation_status',continuation_status,
            'parent_task_id',parent_task_id,
            'parent_lease_epoch',parent_lease_epoch,
            'parent_fencing_token',parent_fencing_token,
            'parent_claimed_by',parent_claimed_by,
            'parent_claim_token',parent_claim_token,
            'parent_claim_expires_at',parent_claim_expires_at,
            'parent_task_projection_version',parent_task_projection_version,
            'parent_operation_deadline',parent_operation_deadline,
            'activated_at',activated_at,
            'completed_at',completed_at,
            'updated_at',updated_at)
         FROM model_tool_call_batches WHERE run_id=?",
    )
    .bind(run_id.as_str())
    .fetch_one(control)
    .await
    .unwrap();
    let calls = sqlx::query_scalar::<_, String>(
        "SELECT json_object(
            'call_index',call_index,
            'call_id',call_id,
            'tool_name',tool_name,
            'arguments',json(arguments),
            'call_status',call_status,
            'tool_task_id',tool_task_id,
            'effect_id',effect_id,
            'action_id',action_id,
            'tool_attempt_no',tool_attempt_no,
            'lease_epoch',lease_epoch,
            'fencing_token',fencing_token,
            'effect_evidence',effect_evidence,
            'available_at',available_at,
            'claim_owner',claim_owner,
            'claim_token',claim_token,
            'claim_expires_at',claim_expires_at,
            'projection_version',projection_version,
            'started_at',started_at,
            'completed_at',completed_at,
            'updated_at',updated_at)
         FROM model_tool_calls WHERE run_id=? ORDER BY call_index",
    )
    .bind(run_id.as_str())
    .fetch_all(control)
    .await
    .unwrap();
    (batch, calls)
}

async fn postgres_checkpoint_intent_snapshot(
    control: &PgPool,
    run_id: &RunId,
) -> (Value, Vec<Value>) {
    let batch = sqlx::query_scalar::<_, Value>(
        "SELECT jsonb_build_object(
            'execution_status',execution_status,
            'continuation_status',continuation_status,
            'parent_task_id',parent_task_id,
            'parent_lease_epoch',parent_lease_epoch,
            'parent_fencing_token',parent_fencing_token,
            'parent_claimed_by',parent_claimed_by,
            'parent_claim_token',parent_claim_token,
            'parent_claim_expires_at',parent_claim_expires_at,
            'parent_task_projection_version',parent_task_projection_version,
            'parent_operation_deadline',parent_operation_deadline,
            'activated_at',activated_at,
            'completed_at',completed_at,
            'updated_at',updated_at)
         FROM model_tool_call_batches WHERE run_id=$1",
    )
    .bind(run_id.as_str())
    .fetch_one(control)
    .await
    .unwrap();
    let calls = sqlx::query_scalar::<_, Value>(
        "SELECT jsonb_build_object(
            'call_index',call_index,
            'call_id',call_id,
            'tool_name',tool_name,
            'arguments',arguments,
            'call_status',call_status,
            'tool_task_id',tool_task_id,
            'effect_id',effect_id,
            'action_id',action_id,
            'tool_attempt_no',tool_attempt_no,
            'lease_epoch',lease_epoch,
            'fencing_token',fencing_token,
            'effect_evidence',effect_evidence,
            'available_at',available_at,
            'claim_owner',claim_owner,
            'claim_token',claim_token,
            'claim_expires_at',claim_expires_at,
            'projection_version',projection_version,
            'started_at',started_at,
            'completed_at',completed_at,
            'updated_at',updated_at)
         FROM model_tool_calls WHERE run_id=$1 ORDER BY call_index",
    )
    .bind(run_id.as_str())
    .fetch_all(control)
    .await
    .unwrap();
    (batch, calls)
}

async fn sqlite_snapshot(
    control: &SqlitePool,
    run_id: &RunId,
) -> (i64, i64, i64, i64, i64, i64, i64, i64) {
    sqlx::query_as(
        "SELECT o.projection_version,a.projection_version,v.projection_version,
                r.projection_version,
                (SELECT COUNT(*) FROM execution_events WHERE run_id=?),
                (SELECT COUNT(*) FROM scheduler_checkpoints WHERE run_id=?),
                (SELECT COUNT(*) FROM scheduler_values WHERE run_id=?),
                (SELECT COUNT(*) FROM public_event_outbox WHERE run_id=?)
         FROM task_outbox o
         JOIN node_attempts a ON a.run_id=o.run_id AND a.activation_id=o.activation_id
           AND a.attempt_no=o.attempt_no AND a.lease_epoch=o.lease_epoch
         JOIN node_activations v ON v.run_id=o.run_id AND v.activation_id=o.activation_id
         JOIN workflow_runs r ON r.run_id=o.run_id
         WHERE o.run_id=?",
    )
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .bind(run_id.as_str())
    .fetch_one(control)
    .await
    .unwrap()
}

async fn postgres_snapshot(
    control: &PgPool,
    run_id: &RunId,
) -> (i64, i64, i64, i64, i64, i64, i64, i64) {
    sqlx::query_as(
        "SELECT o.projection_version,a.projection_version,v.projection_version,
                r.projection_version,
                (SELECT COUNT(*) FROM execution_events WHERE run_id=$1),
                (SELECT COUNT(*) FROM scheduler_checkpoints WHERE run_id=$1),
                (SELECT COUNT(*) FROM scheduler_values WHERE run_id=$1),
                (SELECT COUNT(*) FROM public_event_outbox WHERE run_id=$1)
         FROM task_outbox o
         JOIN node_attempts a ON a.run_id=o.run_id AND a.activation_id=o.activation_id
           AND a.attempt_no=o.attempt_no AND a.lease_epoch=o.lease_epoch
         JOIN node_activations v ON v.run_id=o.run_id AND v.activation_id=o.activation_id
         JOIN workflow_runs r ON r.run_id=o.run_id
         WHERE o.run_id=$1",
    )
    .bind(run_id.as_str())
    .fetch_one(control)
    .await
    .unwrap()
}

#[tokio::test]
async fn sqlite_deadline_authority_rejects_premature_and_lost_leases_and_commits_only_db_authorized_timeout(
) {
    let (plan, descriptors, versioned) = deadline_fixture();
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("scheduler-deadline-authority.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .foreign_keys(true),
    )
    .await
    .unwrap();

    let premature_run = RunId::new("run_sqlite_deadline_premature").unwrap();
    let (premature, _) =
        prepare_sqlite_task(&repository, &control, &versioned, &linked, &premature_run).await;
    let premature_snapshot = sqlite_snapshot(&control, &premature_run).await;
    let premature_timeout = SchedulerTaskOutcome::Failed(
        SchedulerTaskFailure::from_runtime_deadline(&premature).unwrap(),
    );
    assert!(repository
        .commit_scheduler_task_outcome(&premature, &premature_timeout)
        .await
        .is_err());
    assert_eq!(
        sqlite_snapshot(&control, &premature_run).await,
        premature_snapshot,
        "a private timeout before the database deadline must change no authority",
    );

    let late_run = RunId::new("run_sqlite_deadline_late_success").unwrap();
    let (late, _) =
        prepare_sqlite_task(&repository, &control, &versioned, &linked, &late_run).await;
    let lost_run = RunId::new("run_sqlite_deadline_lease_lost").unwrap();
    let (lost, _) =
        prepare_sqlite_task(&repository, &control, &versioned, &linked, &lost_run).await;
    tokio::time::sleep(std::time::Duration::from_millis(2_100)).await;

    let before_late = sqlite_snapshot(&control, &late_run).await;
    assert_eq!(
        repository
            .commit_scheduler_task_outcome(&late, &success_for(&late))
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::OperationDeadlineElapsed,
    );
    assert_eq!(
        sqlite_snapshot(&control, &late_run).await,
        before_late,
        "ordinary late success must roll back event, checkpoint, value, public event, and projections",
    );
    let authorized = match repository
        .heartbeat_scheduler_task(&late, 60)
        .await
        .unwrap()
    {
        SchedulerTaskHeartbeatOutcome::OperationDeadlineElapsed(renewed) => renewed,
        other => panic!("database did not authorize the elapsed deadline: {other:?}"),
    };
    let authorized_timeout = SchedulerTaskOutcome::Failed(
        SchedulerTaskFailure::from_runtime_deadline(&authorized).unwrap(),
    );
    assert!(matches!(
        repository
            .commit_scheduler_task_outcome(&authorized, &authorized_timeout)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT lifecycle FROM node_attempts WHERE run_id=? AND attempt_no=1",
        )
        .bind(late_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "timed_out",
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM scheduler_values WHERE run_id=?")
            .bind(late_run.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        0,
    );

    expire_sqlite_scheduler_task_claim(&control, &lost_run).await;
    assert_eq!(
        repository
            .heartbeat_scheduler_task(&lost, 60)
            .await
            .unwrap(),
        SchedulerTaskHeartbeatOutcome::LeaseLost,
    );
    let lost_snapshot = sqlite_snapshot(&control, &lost_run).await;
    let unauthorized_timeout =
        SchedulerTaskOutcome::Failed(SchedulerTaskFailure::from_runtime_deadline(&lost).unwrap());
    assert!(matches!(
        repository
            .commit_scheduler_task_outcome(&lost, &unauthorized_timeout)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::StateConflict | SchedulerTaskCommitOutcome::StaleLease
    ));
    assert_eq!(sqlite_snapshot(&control, &lost_run).await, lost_snapshot);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT lifecycle FROM node_attempts WHERE run_id=? AND attempt_no=1",
        )
        .bind(lost_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "running",
    );
}

#[tokio::test]
async fn postgres_deadline_authority_matches_sqlite_contract() {
    let Ok(database_url) = std::env::var("V3_TEST_POSTGRES_URL") else {
        return;
    };
    let schema = format!("scheduler_deadline_v3_{}", Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let control = PgPoolOptions::new()
        .max_connections(4)
        .connect(&scoped_url)
        .await
        .unwrap();
    let repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    repository.initialize_schema().await.unwrap();
    let (plan, descriptors, versioned) = deadline_fixture();
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );

    let premature_run = RunId::new("run_pg_deadline_premature").unwrap();
    let (premature, _) =
        prepare_postgres_task(&repository, &control, &versioned, &linked, &premature_run).await;
    let premature_snapshot = postgres_snapshot(&control, &premature_run).await;
    let premature_timeout = SchedulerTaskOutcome::Failed(
        SchedulerTaskFailure::from_runtime_deadline(&premature).unwrap(),
    );
    assert!(repository
        .commit_scheduler_task_outcome(&premature, &premature_timeout)
        .await
        .is_err());
    assert_eq!(
        postgres_snapshot(&control, &premature_run).await,
        premature_snapshot,
    );

    let late_run = RunId::new("run_pg_deadline_late_success").unwrap();
    let (late, _) =
        prepare_postgres_task(&repository, &control, &versioned, &linked, &late_run).await;
    let lost_run = RunId::new("run_pg_deadline_lease_lost").unwrap();
    let (lost, _) =
        prepare_postgres_task(&repository, &control, &versioned, &linked, &lost_run).await;
    tokio::time::sleep(std::time::Duration::from_millis(2_100)).await;

    let before_late = postgres_snapshot(&control, &late_run).await;
    assert_eq!(
        repository
            .commit_scheduler_task_outcome(&late, &success_for(&late))
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::OperationDeadlineElapsed,
    );
    assert_eq!(postgres_snapshot(&control, &late_run).await, before_late,);
    let authorized = match repository
        .heartbeat_scheduler_task(&late, 60)
        .await
        .unwrap()
    {
        SchedulerTaskHeartbeatOutcome::OperationDeadlineElapsed(renewed) => renewed,
        other => panic!("database did not authorize the elapsed deadline: {other:?}"),
    };
    let authorized_timeout = SchedulerTaskOutcome::Failed(
        SchedulerTaskFailure::from_runtime_deadline(&authorized).unwrap(),
    );
    assert!(matches!(
        repository
            .commit_scheduler_task_outcome(&authorized, &authorized_timeout)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT lifecycle FROM node_attempts WHERE run_id=$1 AND attempt_no=1",
        )
        .bind(late_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "timed_out",
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM scheduler_values WHERE run_id=$1")
            .bind(late_run.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        0,
    );

    let expired_at = sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
        "SELECT clock_timestamp()-INTERVAL '1 second'",
    )
    .fetch_one(&control)
    .await
    .unwrap();
    sqlx::query("UPDATE task_outbox SET claim_expires_at=$1 WHERE run_id=$2")
        .bind(expired_at)
        .bind(lost_run.as_str())
        .execute(&control)
        .await
        .unwrap();
    sqlx::query("UPDATE node_attempts SET lease_expires_at=$1 WHERE run_id=$2")
        .bind(expired_at)
        .bind(lost_run.as_str())
        .execute(&control)
        .await
        .unwrap();
    assert_eq!(
        repository
            .heartbeat_scheduler_task(&lost, 60)
            .await
            .unwrap(),
        SchedulerTaskHeartbeatOutcome::LeaseLost,
    );
    let lost_snapshot = postgres_snapshot(&control, &lost_run).await;
    let unauthorized_timeout =
        SchedulerTaskOutcome::Failed(SchedulerTaskFailure::from_runtime_deadline(&lost).unwrap());
    assert!(matches!(
        repository
            .commit_scheduler_task_outcome(&lost, &unauthorized_timeout)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::StateConflict | SchedulerTaskCommitOutcome::StaleLease
    ));
    assert_eq!(postgres_snapshot(&control, &lost_run).await, lost_snapshot);
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT lifecycle FROM node_attempts WHERE run_id=$1 AND attempt_no=1",
        )
        .bind(lost_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "running",
    );

    drop(repository);
    drop(control);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
}

#[tokio::test]
async fn sqlite_model_call_reservation_and_checkpoint_are_fenced_append_only_and_idempotent() {
    let (plan, descriptors, versioned) = model_call_fixture();
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let database = directory
        .path()
        .join("scheduler-model-call-authority.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .foreign_keys(true),
    )
    .await
    .unwrap();

    let run_id = RunId::new("run_sqlite_model_call_authority").unwrap();
    let (claim, fence) =
        prepare_sqlite_task(&repository, &control, &versioned, &linked, &run_id).await;
    exercise_model_call_authority(&repository, &claim, &run_id).await;
    drive_until_response_snapshot(&repository, &linked, &fence, &run_id).await;
    assert_model_call_snapshot(&repository, &run_id).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM model_call_usage WHERE run_id=?")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        2,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM response_public_items WHERE run_id=?")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        1,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM model_tool_call_batches WHERE run_id=?")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        1,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM model_tool_calls WHERE run_id=?")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        2,
    );

    let stale_run_id = RunId::new("run_sqlite_model_call_stale").unwrap();
    let (stale, _) =
        prepare_sqlite_task(&repository, &control, &versioned, &linked, &stale_run_id).await;
    expire_sqlite_scheduler_task_claim(&control, &stale_run_id).await;
    assert_eq!(
        repository
            .reserve_model_call(&stale, 1, true)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::StaleLease,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM model_call_usage WHERE run_id=?")
            .bind(stale_run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        0,
        "a stale worker must not leave telemetry or item reservations",
    );

    let stale_item_run_id = RunId::new("run_sqlite_lazy_item_stale").unwrap();
    let (stale_item, _) = prepare_sqlite_task(
        &repository,
        &control,
        &versioned,
        &linked,
        &stale_item_run_id,
    )
    .await;
    assert!(matches!(
        repository
            .reserve_model_call(&stale_item, 1, true)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    expire_sqlite_scheduler_task_claim(&control, &stale_item_run_id).await;
    assert_eq!(
        repository
            .reserve_model_call_public_item(&stale_item, 1)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::StaleLease,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM response_public_items WHERE run_id=?",)
            .bind(stale_item_run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        0,
    );

    let deadline_run_id = RunId::new("run_sqlite_lazy_item_deadline").unwrap();
    let (deadline_claim, _) =
        prepare_sqlite_task(&repository, &control, &versioned, &linked, &deadline_run_id).await;
    assert!(matches!(
        repository
            .reserve_model_call(&deadline_claim, 1, true)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    sqlx::query("UPDATE node_attempts SET started_at=datetime('now','-1 day') WHERE run_id=?")
        .bind(deadline_run_id.as_str())
        .execute(&control)
        .await
        .unwrap();
    assert_eq!(
        repository
            .reserve_model_call_public_item(&deadline_claim, 1)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::OperationDeadlineElapsed,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM response_public_items WHERE run_id=?",)
            .bind(deadline_run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        0,
    );
}

#[tokio::test]
async fn sqlite_function_call_public_item_allocation_is_concurrent_fenced_and_deadline_bounded() {
    let (plan, descriptors, versioned) =
        model_call_fixture_with_binding(all_public_model_tool_queue_binding());
    let linked = LinkedPlan::link(&plan, &descriptors, &SubflowContractRegistry::new()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let database = directory
        .path()
        .join("function-call-public-authority.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed,
    );
    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .foreign_keys(true),
    )
    .await
    .unwrap();

    let concurrent_run = RunId::new("run_sqlite_function_public_concurrent").unwrap();
    let (concurrent_claim, _) =
        prepare_sqlite_task(&repository, &control, &versioned, &linked, &concurrent_run).await;
    let (weather_item, clock_item) =
        exercise_concurrent_function_call_public_item_authority(&repository, &concurrent_claim)
            .await;
    let reserved = sqlx::query_as::<_, (i64, String, String, Option<i64>, String)>(
        "SELECT output_index,item_kind,item_status,seal_index,safe_item
         FROM response_public_items WHERE run_id=? ORDER BY output_index",
    )
    .bind(concurrent_run.as_str())
    .fetch_all(&control)
    .await
    .unwrap();
    assert_eq!(reserved.len(), 2);
    for (expected_index, row) in reserved.iter().enumerate() {
        assert_eq!(row.0, expected_index as i64);
        assert_eq!(row.1, "function_call");
        assert_eq!(row.2, "reserved");
        assert_eq!(row.3, None);
        let safe_item: serde_json::Value = serde_json::from_str(&row.4).unwrap();
        assert_eq!(safe_item["status"], "incomplete");
        assert_eq!(safe_item["arguments"], "");
    }
    exercise_dynamic_function_call_checkpoint_and_activation(
        &repository,
        &concurrent_claim,
        weather_item,
        clock_item,
    )
    .await;

    let stale_run = RunId::new("run_sqlite_function_public_stale").unwrap();
    let (stale_claim, _) =
        prepare_sqlite_task(&repository, &control, &versioned, &linked, &stale_run).await;
    assert!(matches!(
        repository
            .reserve_model_call(&stale_claim, 1, true)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    expire_sqlite_scheduler_task_claim(&control, &stale_run).await;
    assert_eq!(
        repository
            .reserve_model_call_public_function_item(&stale_claim, 1, 0, "call_weather", "weather",)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::StaleLease,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM response_public_items WHERE run_id=?",)
            .bind(stale_run.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        0,
    );

    let deadline_run = RunId::new("run_sqlite_function_public_deadline").unwrap();
    let (deadline_claim, _) =
        prepare_sqlite_task(&repository, &control, &versioned, &linked, &deadline_run).await;
    assert!(matches!(
        repository
            .reserve_model_call(&deadline_claim, 1, true)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    sqlx::query("UPDATE node_attempts SET started_at=datetime('now','-1 day') WHERE run_id=?")
        .bind(deadline_run.as_str())
        .execute(&control)
        .await
        .unwrap();
    assert_eq!(
        repository
            .reserve_model_call_public_function_item(
                &deadline_claim,
                1,
                0,
                "call_weather",
                "weather",
            )
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::OperationDeadlineElapsed,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM response_public_items WHERE run_id=?",)
            .bind(deadline_run.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        0,
    );
}

#[tokio::test]
async fn sqlite_failed_function_items_checkpoint_exactly_or_remain_unsealed_without_authority() {
    let (plan, descriptors, versioned) =
        model_call_fixture_with_binding(all_public_model_tool_queue_binding());
    let linked = LinkedPlan::link(&plan, &descriptors, &SubflowContractRegistry::new()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("failed-function-checkpoint.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    repository.install_versioned_plan(&versioned).await.unwrap();
    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .foreign_keys(true),
    )
    .await
    .unwrap();

    let checkpoint_run = RunId::new("run_sqlite_failed_function_checkpoint").unwrap();
    let (checkpoint_claim, _) =
        prepare_sqlite_task(&repository, &control, &versioned, &linked, &checkpoint_run).await;
    exercise_failed_function_call_checkpoint(&repository, &checkpoint_claim).await;
    let rows = sqlx::query_as::<_, (i64, String, Option<i64>, String)>(
        "SELECT item_ordinal,item_status,seal_index,safe_item
         FROM response_public_items WHERE run_id=? ORDER BY item_ordinal",
    )
    .bind(checkpoint_run.as_str())
    .fetch_all(&control)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[0].1, "incomplete");
    assert_eq!(rows[0].2, Some(3));
    assert_eq!(rows[1].0, 2);
    assert_eq!(rows[1].1, "incomplete");
    assert_eq!(rows[1].2, Some(7));
    for row in &rows {
        let safe_item: Value = serde_json::from_str(&row.3).unwrap();
        assert_eq!(safe_item["status"], "incomplete");
        assert_eq!(safe_item["arguments"], "");
    }

    for (suffix, expire_lease, expected) in [
        ("stale", true, SchedulerTaskCommitOutcome::StaleLease),
        (
            "deadline",
            false,
            SchedulerTaskCommitOutcome::OperationDeadlineElapsed,
        ),
    ] {
        let run_id = RunId::new(format!("run_sqlite_failed_function_{suffix}")).unwrap();
        let (claim, _) =
            prepare_sqlite_task(&repository, &control, &versioned, &linked, &run_id).await;
        assert!(matches!(
            repository
                .reserve_model_call(&claim, 1, true)
                .await
                .unwrap(),
            SchedulerTaskCommitOutcome::Committed { .. }
        ));
        let item = match repository
            .reserve_model_call_public_function_item(&claim, 1, 0, "call_weather", "weather")
            .await
            .unwrap()
        {
            SchedulerTaskCommitOutcome::Committed { result } => result,
            other => panic!("function item was not reserved: {other:?}"),
        };
        if expire_lease {
            expire_sqlite_scheduler_task_claim(&control, &run_id).await;
        } else {
            sqlx::query(
                "UPDATE node_attempts SET started_at=datetime('now','-1 day') WHERE run_id=?",
            )
            .bind(run_id.as_str())
            .execute(&control)
            .await
            .unwrap();
        }
        let completion =
            failed_function_completion(1, 20, vec![(0, item, "call_weather", "weather", 2)]);
        assert_eq!(
            repository
                .checkpoint_model_call_completion(&claim, &completion)
                .await
                .unwrap(),
            expected,
        );
        let row = sqlx::query_as::<_, (String, Option<i64>, String)>(
            "SELECT item_status,seal_index,safe_item FROM response_public_items
             WHERE run_id=? AND item_ordinal=1",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap();
        assert_eq!(row.0, "reserved");
        assert_eq!(row.1, None);
        let safe_item: Value = serde_json::from_str(&row.2).unwrap();
        assert_eq!(safe_item["status"], "incomplete");
        assert_eq!(safe_item["arguments"], "");
    }
}

#[tokio::test]
async fn sqlite_public_retry_appends_identity_and_preserves_failed_incomplete_item() {
    let (plan, descriptors, versioned) = retryable_public_model_call_fixture();
    let linked = LinkedPlan::link(&plan, &descriptors, &SubflowContractRegistry::new()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("public-retry.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    repository.install_versioned_plan(&versioned).await.unwrap();
    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .foreign_keys(true),
    )
    .await
    .unwrap();
    let run_id = RunId::new("run_sqlite_public_retry").unwrap();
    let (first_claim, fence) =
        prepare_sqlite_task(&repository, &control, &versioned, &linked, &run_id).await;
    let (failed_item, completed_item) =
        exercise_public_retry_appends_new_item(&repository, &first_claim).await;
    assert_eq!(failed_item.output_index(), 0);
    assert_eq!(completed_item.output_index(), 1);

    drop(repository);
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    drive_until_response_snapshot(&repository, &linked, &fence, &run_id).await;

    let snapshot = repository
        .load_response_snapshot(&run_id)
        .await
        .unwrap()
        .unwrap();
    let output = snapshot.response()["output"].as_array().unwrap();
    assert_eq!(output.len(), 2);
    assert_eq!(output[0]["id"], failed_item.item_id());
    assert_eq!(output[0]["type"], "function_call");
    assert_eq!(output[0]["status"], "incomplete");
    assert_eq!(output[0]["arguments"], "");
    assert_eq!(output[1]["id"], completed_item.item_id());
    assert_eq!(output[1]["type"], "message");
    assert_eq!(output[1]["status"], "completed");
    assert_eq!(snapshot.workflow()["result"], json!("late-success"));
    let manifest = snapshot.public_item_manifest().as_array().unwrap();
    assert_eq!(manifest[0]["attempt_no"], 1);
    assert_eq!(manifest[0]["status"], "incomplete");
    assert_eq!(manifest[0]["seal_index"], 2);
    assert_eq!(manifest[1]["attempt_no"], 2);
    assert_eq!(manifest[1]["status"], "completed");
    assert_eq!(snapshot.usage_status(), ResponseUsageStatus::Complete);
    assert_eq!(
        snapshot.usage(),
        Some(&json!({
            "input_tokens": 30,
            "input_tokens_details": {"cached_tokens": 32},
            "output_tokens": 34,
            "output_tokens_details": {"reasoning_tokens": 36},
            "total_tokens": 64,
        })),
        "restart aggregation must include both the failed first Attempt and successful retry exactly once",
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, String, i64)>(
            "SELECT attempt_no,call_status,usage_complete FROM model_call_usage
             WHERE run_id=? ORDER BY attempt_no,model_call_no",
        )
        .bind(run_id.as_str())
        .fetch_all(&control)
        .await
        .unwrap(),
        vec![(1, "failed".into(), 1), (2, "completed".into(), 1)],
        "the failed Attempt's Provider usage must remain durable across retry and restart",
    );
}

#[tokio::test]
async fn sqlite_failed_and_cancelled_runs_retain_fenced_reported_usage() {
    let (plan, descriptors, versioned) = model_call_fixture();
    let linked = LinkedPlan::link(&plan, &descriptors, &SubflowContractRegistry::new()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("terminal-reported-usage.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    repository.install_versioned_plan(&versioned).await.unwrap();
    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .foreign_keys(true),
    )
    .await
    .unwrap();

    let failed_run = RunId::new("run_sqlite_failed_reported_usage").unwrap();
    let (failed_claim, failed_fence) =
        prepare_sqlite_task(&repository, &control, &versioned, &linked, &failed_run).await;
    checkpoint_failed_model_call_with_usage(&repository, &failed_claim, 10).await;
    let worker_failure = WorkerFailure::new(
        WorkerFailureClass::InfrastructureFailure,
        "PROVIDER_FAILED",
        false,
    )
    .unwrap();
    let failure = SchedulerTaskOutcome::Failed(
        SchedulerTaskFailure::from_worker_failure(
            &failed_claim,
            &worker_failure,
            EffectEvidence::Started,
            SchedulerFailureDisposition::Terminal,
        )
        .unwrap(),
    );
    assert!(matches!(
        repository
            .commit_scheduler_task_outcome(&failed_claim, &failure)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    drive_until_response_snapshot(&repository, &linked, &failed_fence, &failed_run).await;

    let cancelled_run = RunId::new("run_sqlite_cancelled_reported_usage").unwrap();
    let (cancelled_claim, cancelled_fence) =
        prepare_sqlite_task(&repository, &control, &versioned, &linked, &cancelled_run).await;
    checkpoint_failed_model_call_with_usage(&repository, &cancelled_claim, 20).await;
    assert_eq!(
        sqlx::query(
            "UPDATE workflow_runs
             SET lifecycle='terminating',admission_state='draining',
                 termination_intent_reason='cancelled',termination_intent_transition_key=?,
                 termination_intent_at=CURRENT_TIMESTAMP,
                 projection_version=projection_version+1,updated_at=CURRENT_TIMESTAMP
             WHERE run_id=? AND lifecycle='active'",
        )
        .bind(key("cancel-with-reported-usage", &cancelled_run).as_str())
        .bind(cancelled_run.as_str())
        .execute(&control)
        .await
        .unwrap()
        .rows_affected(),
        1,
    );
    drive_until_response_snapshot(&repository, &linked, &cancelled_fence, &cancelled_run).await;

    for (run_id, lifecycle, base) in [
        (&failed_run, "failed", 10_u64),
        (&cancelled_run, "cancelled", 20_u64),
    ] {
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT lifecycle FROM workflow_runs WHERE run_id=?",)
                .bind(run_id.as_str())
                .fetch_one(&control)
                .await
                .unwrap(),
            lifecycle,
        );
        let snapshot = repository
            .load_response_snapshot(run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.usage_status(), ResponseUsageStatus::Complete);
        assert_eq!(
            snapshot.usage(),
            Some(&json!({
                "input_tokens": base,
                "input_tokens_details": {"cached_tokens": base + 1},
                "output_tokens": base + 2,
                "output_tokens_details": {"reasoning_tokens": base + 3},
                "total_tokens": base + base + 2,
            })),
            "an accepted Provider report must survive the Run's terminal lifecycle",
        );
    }
}

#[tokio::test]
async fn postgres_function_call_public_item_allocation_is_concurrent_fenced_and_deadline_bounded() {
    let Ok(database_url) = std::env::var("V3_TEST_POSTGRES_URL") else {
        return;
    };
    let schema = format!("function_call_public_v3_{}", Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let control = PgPoolOptions::new()
        .max_connections(4)
        .connect(&scoped_url)
        .await
        .unwrap();
    let repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    repository.initialize_schema().await.unwrap();
    let (plan, descriptors, versioned) =
        model_call_fixture_with_binding(all_public_model_tool_queue_binding());
    let linked = LinkedPlan::link(&plan, &descriptors, &SubflowContractRegistry::new()).unwrap();
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed,
    );

    let concurrent_run = RunId::new("run_pg_function_public_concurrent").unwrap();
    let (concurrent_claim, _) =
        prepare_postgres_task(&repository, &control, &versioned, &linked, &concurrent_run).await;
    let (weather_item, clock_item) =
        exercise_concurrent_function_call_public_item_authority(&repository, &concurrent_claim)
            .await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM response_public_items WHERE run_id=$1",
        )
        .bind(concurrent_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        2,
    );
    exercise_dynamic_function_call_checkpoint_and_activation(
        &repository,
        &concurrent_claim,
        weather_item,
        clock_item,
    )
    .await;

    let stale_run = RunId::new("run_pg_function_public_stale").unwrap();
    let (stale_claim, _) =
        prepare_postgres_task(&repository, &control, &versioned, &linked, &stale_run).await;
    assert!(matches!(
        repository
            .reserve_model_call(&stale_claim, 1, true)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    let expired_at =
        sqlx::query_scalar::<_, DateTime<Utc>>("SELECT clock_timestamp()-INTERVAL '1 second'")
            .fetch_one(&control)
            .await
            .unwrap();
    sqlx::query("UPDATE task_outbox SET claim_expires_at=$1 WHERE run_id=$2")
        .bind(expired_at)
        .bind(stale_run.as_str())
        .execute(&control)
        .await
        .unwrap();
    sqlx::query("UPDATE node_attempts SET lease_expires_at=$1 WHERE run_id=$2")
        .bind(expired_at)
        .bind(stale_run.as_str())
        .execute(&control)
        .await
        .unwrap();
    assert_eq!(
        repository
            .reserve_model_call_public_function_item(&stale_claim, 1, 0, "call_weather", "weather",)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::StaleLease,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM response_public_items WHERE run_id=$1",
        )
        .bind(stale_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        0,
    );

    let deadline_run = RunId::new("run_pg_function_public_deadline").unwrap();
    let (deadline_claim, _) =
        prepare_postgres_task(&repository, &control, &versioned, &linked, &deadline_run).await;
    assert!(matches!(
        repository
            .reserve_model_call(&deadline_claim, 1, true)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    sqlx::query(
        "UPDATE node_attempts SET started_at=clock_timestamp()-INTERVAL '1 day' WHERE run_id=$1",
    )
    .bind(deadline_run.as_str())
    .execute(&control)
    .await
    .unwrap();
    assert_eq!(
        repository
            .reserve_model_call_public_function_item(
                &deadline_claim,
                1,
                0,
                "call_weather",
                "weather",
            )
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::OperationDeadlineElapsed,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM response_public_items WHERE run_id=$1",
        )
        .bind(deadline_run.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        0,
    );

    drop(repository);
    drop(control);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
}

#[tokio::test]
async fn postgres_failed_function_items_checkpoint_exactly_or_remain_unsealed_without_authority() {
    let Ok(database_url) = std::env::var("V3_TEST_POSTGRES_URL") else {
        return;
    };
    let schema = format!("failed_function_checkpoint_v3_{}", Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let control = PgPoolOptions::new()
        .max_connections(4)
        .connect(&scoped_url)
        .await
        .unwrap();
    let repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    repository.initialize_schema().await.unwrap();
    let (plan, descriptors, versioned) =
        model_call_fixture_with_binding(all_public_model_tool_queue_binding());
    let linked = LinkedPlan::link(&plan, &descriptors, &SubflowContractRegistry::new()).unwrap();
    repository.install_versioned_plan(&versioned).await.unwrap();

    let checkpoint_run = RunId::new("run_pg_failed_function_checkpoint").unwrap();
    let (checkpoint_claim, _) =
        prepare_postgres_task(&repository, &control, &versioned, &linked, &checkpoint_run).await;
    exercise_failed_function_call_checkpoint(&repository, &checkpoint_claim).await;
    let rows = sqlx::query_as::<_, (i32, String, Option<i64>, Value)>(
        "SELECT item_ordinal,item_status,seal_index,safe_item
         FROM response_public_items WHERE run_id=$1 ORDER BY item_ordinal",
    )
    .bind(checkpoint_run.as_str())
    .fetch_all(&control)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        (rows[0].0, rows[0].1.as_str(), rows[0].2),
        (1, "incomplete", Some(3))
    );
    assert_eq!(
        (rows[1].0, rows[1].1.as_str(), rows[1].2),
        (2, "incomplete", Some(7))
    );
    for row in &rows {
        assert_eq!(row.3["status"], "incomplete");
        assert_eq!(row.3["arguments"], "");
    }

    for (suffix, expire_lease, expected) in [
        ("stale", true, SchedulerTaskCommitOutcome::StaleLease),
        (
            "deadline",
            false,
            SchedulerTaskCommitOutcome::OperationDeadlineElapsed,
        ),
    ] {
        let run_id = RunId::new(format!("run_pg_failed_function_{suffix}")).unwrap();
        let (claim, _) =
            prepare_postgres_task(&repository, &control, &versioned, &linked, &run_id).await;
        assert!(matches!(
            repository
                .reserve_model_call(&claim, 1, true)
                .await
                .unwrap(),
            SchedulerTaskCommitOutcome::Committed { .. }
        ));
        let item = match repository
            .reserve_model_call_public_function_item(&claim, 1, 0, "call_weather", "weather")
            .await
            .unwrap()
        {
            SchedulerTaskCommitOutcome::Committed { result } => result,
            other => panic!("function item was not reserved: {other:?}"),
        };
        if expire_lease {
            let expired_at = sqlx::query_scalar::<_, DateTime<Utc>>(
                "SELECT clock_timestamp()-INTERVAL '1 second'",
            )
            .fetch_one(&control)
            .await
            .unwrap();
            sqlx::query("UPDATE task_outbox SET claim_expires_at=$1 WHERE run_id=$2")
                .bind(expired_at)
                .bind(run_id.as_str())
                .execute(&control)
                .await
                .unwrap();
            sqlx::query("UPDATE node_attempts SET lease_expires_at=$1 WHERE run_id=$2")
                .bind(expired_at)
                .bind(run_id.as_str())
                .execute(&control)
                .await
                .unwrap();
        } else {
            sqlx::query(
                "UPDATE node_attempts SET started_at=clock_timestamp()-INTERVAL '1 day'
                 WHERE run_id=$1",
            )
            .bind(run_id.as_str())
            .execute(&control)
            .await
            .unwrap();
        }
        let completion =
            failed_function_completion(1, 20, vec![(0, item, "call_weather", "weather", 2)]);
        assert_eq!(
            repository
                .checkpoint_model_call_completion(&claim, &completion)
                .await
                .unwrap(),
            expected,
        );
        let row = sqlx::query_as::<_, (String, Option<i64>, Value)>(
            "SELECT item_status,seal_index,safe_item FROM response_public_items
             WHERE run_id=$1 AND item_ordinal=1",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap();
        assert_eq!(row.0, "reserved");
        assert_eq!(row.1, None);
        assert_eq!(row.2["status"], "incomplete");
        assert_eq!(row.2["arguments"], "");
    }

    drop(repository);
    drop(control);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
}

#[tokio::test]
async fn postgres_public_retry_appends_identity_and_preserves_failed_incomplete_item() {
    let Ok(database_url) = std::env::var("V3_TEST_POSTGRES_URL") else {
        return;
    };
    let schema = format!("public_retry_v3_{}", Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let control = PgPoolOptions::new()
        .max_connections(4)
        .connect(&scoped_url)
        .await
        .unwrap();
    let repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    repository.initialize_schema().await.unwrap();
    let (plan, descriptors, versioned) = retryable_public_model_call_fixture();
    let linked = LinkedPlan::link(&plan, &descriptors, &SubflowContractRegistry::new()).unwrap();
    repository.install_versioned_plan(&versioned).await.unwrap();
    let run_id = RunId::new("run_pg_public_retry").unwrap();
    let (first_claim, fence) =
        prepare_postgres_task(&repository, &control, &versioned, &linked, &run_id).await;
    let (failed_item, completed_item) =
        exercise_public_retry_appends_new_item(&repository, &first_claim).await;
    assert_eq!(failed_item.output_index(), 0);
    assert_eq!(completed_item.output_index(), 1);
    drive_until_response_snapshot(&repository, &linked, &fence, &run_id).await;

    let snapshot = repository
        .load_response_snapshot(&run_id)
        .await
        .unwrap()
        .unwrap();
    let output = snapshot.response()["output"].as_array().unwrap();
    assert_eq!(output.len(), 2);
    assert_eq!(output[0]["id"], failed_item.item_id());
    assert_eq!(output[0]["status"], "incomplete");
    assert_eq!(output[0]["arguments"], "");
    assert_eq!(output[1]["id"], completed_item.item_id());
    assert_eq!(output[1]["status"], "completed");
    let manifest = snapshot.public_item_manifest().as_array().unwrap();
    assert_eq!(manifest[0]["attempt_no"], 1);
    assert_eq!(manifest[0]["status"], "incomplete");
    assert_eq!(manifest[0]["seal_index"], 2);
    assert_eq!(manifest[1]["attempt_no"], 2);
    assert_eq!(manifest[1]["status"], "completed");
    assert_eq!(snapshot.usage_status(), ResponseUsageStatus::Complete);
    assert_eq!(
        snapshot.usage(),
        Some(&json!({
            "input_tokens": 30,
            "input_tokens_details": {"cached_tokens": 32},
            "output_tokens": 34,
            "output_tokens_details": {"reasoning_tokens": 36},
            "total_tokens": 64,
        })),
        "PostgreSQL must aggregate failed and successful retry Attempts exactly like SQLite",
    );
    assert_eq!(
        sqlx::query_as::<_, (i32, String, bool)>(
            "SELECT attempt_no,call_status,usage_complete FROM model_call_usage
             WHERE run_id=$1 ORDER BY attempt_no,model_call_no",
        )
        .bind(run_id.as_str())
        .fetch_all(&control)
        .await
        .unwrap(),
        vec![(1, "failed".into(), true), (2, "completed".into(), true)],
    );

    drop(repository);
    drop(control);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
}

#[tokio::test]
async fn sqlite_private_model_call_never_allocates_a_public_item() {
    let source = MODEL_CALL_AGENT.replace("publish: true", "publish: false");
    let (plan, descriptors, versioned) = model_call_fixture_from_source(
        &source,
        model_tool_queue_binding(),
        "scheduler_private_model_call_v1",
        "scheduler_private_model_call_deployment_v1",
    );
    let linked = LinkedPlan::link(&plan, &descriptors, &SubflowContractRegistry::new()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("scheduler-private-model-call.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .foreign_keys(true),
    )
    .await
    .unwrap();
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let run_id = RunId::new("run_sqlite_private_model_call").unwrap();
    let (claim, _) = prepare_sqlite_task(&repository, &control, &versioned, &linked, &run_id).await;
    let authority = match repository
        .reserve_model_call(&claim, 1, false)
        .await
        .unwrap()
    {
        SchedulerTaskCommitOutcome::Committed { result } => result,
        other => panic!("private model call was not reserved: {other:?}"),
    };
    assert!(!authority.publication_enabled());
    assert!(authority.public_item().is_none());
    assert!(repository
        .reserve_model_call_public_item(&claim, 1)
        .await
        .is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM response_public_items WHERE run_id=?",)
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        0,
    );
}

#[tokio::test]
async fn sqlite_terminal_run_closes_active_model_tool_work_and_fences_recovery() {
    for (suffix, reason, expected_lifecycle, expected_quiescence) in [
        (
            "cancelled",
            TerminationReason::Cancelled,
            RunLifecycle::Cancelled,
            SchedulerQuiescence::RunCancelled,
        ),
        (
            "timed_out",
            TerminationReason::TimedOut,
            RunLifecycle::TimedOut,
            SchedulerQuiescence::RunFailed,
        ),
    ] {
        let run_name = format!("run_sqlite_model_tool_terminal_{suffix}");
        let (_directory, repository, control, run_id, _parent) =
            prepare_sqlite_model_tool_batch(&run_name, model_tool_queue_binding()).await;
        let (plan, descriptors, _) = model_call_fixture();
        let subflows = SubflowContractRegistry::new();
        let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
        let fence = FencedSchedulerRunCommand::new(
            run_id.clone(),
            format!("scheduler-{}", run_id.as_str()),
            1,
            format!("fence-{}", run_id.as_str()),
        )
        .unwrap();

        let running = repository
            .claim_model_tool_calls("terminal-tool-worker", 60, 1, 2)
            .await
            .unwrap()
            .pop()
            .expect("one tool member must be claimed");
        assert_eq!(running.identity().call_index(), 0);
        assert!(matches!(
            repository
                .mark_model_tool_call_started(&running)
                .await
                .unwrap(),
            ModelToolTaskTransitionOutcome::Committed(())
        ));
        assert_eq!(
            sqlx::query_as::<_, (i64, String)>(
                "SELECT call_index,call_status FROM model_tool_calls
                 WHERE run_id=? ORDER BY call_index",
            )
            .bind(run_id.as_str())
            .fetch_all(&control)
            .await
            .unwrap(),
            vec![(0, "running".into()), (1, "pending".into())],
        );

        let termination_key = key(&format!("terminal-{suffix}"), &run_id);
        assert_eq!(
            sqlx::query(
                "UPDATE workflow_runs SET lifecycle='terminating',admission_state='draining',
                    termination_intent_reason=?,termination_intent_transition_key=?,
                    termination_intent_at=CURRENT_TIMESTAMP,
                    projection_version=projection_version+1,updated_at=CURRENT_TIMESTAMP
                 WHERE run_id=? AND lifecycle='active' AND admission_state='open'",
            )
            .bind(match reason {
                TerminationReason::Cancelled => "cancelled",
                TerminationReason::TimedOut => "timed_out",
                _ => unreachable!(),
            })
            .bind(termination_key.as_str())
            .bind(run_id.as_str())
            .execute(&control)
            .await
            .unwrap()
            .rows_affected(),
            1
        );
        drive_terminal_scheduler_action(&repository, &linked, &fence, expected_quiescence).await;
        assert_eq!(
            repository
                .load_run(&run_id)
                .await
                .unwrap()
                .unwrap()
                .lifecycle(),
            expected_lifecycle
        );

        let rows = sqlx::query_as::<
            _,
            (
                i64,
                String,
                String,
                String,
                Option<String>,
                Option<String>,
                Option<String>,
                i64,
                String,
            ),
        >(
            "SELECT call_index,call_status,effect_evidence,failure_class,
                    claim_owner,claim_token,claim_expires_at,lease_epoch,fencing_token
             FROM model_tool_calls WHERE run_id=? ORDER BY call_index",
        )
        .bind(run_id.as_str())
        .fetch_all(&control)
        .await
        .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            (rows[0].1.as_str(), rows[0].2.as_str(), rows[0].3.as_str()),
            ("failed", "unknown", "effect_outcome_unknown")
        );
        assert_eq!(
            (rows[1].1.as_str(), rows[1].2.as_str(), rows[1].3.as_str()),
            ("cancelled", "not_started", "safe")
        );
        assert!(rows.iter().all(|row| {
            row.4.is_none()
                && row.5.is_none()
                && row.6.is_none()
                && row.7 == 2
                && row.8.contains(":run-terminal:")
        }));
        assert_eq!(
            sqlx::query_as::<_, (String, String, i64)>(
                "SELECT execution_status,continuation_status,completed_at IS NOT NULL
                 FROM model_tool_call_batches WHERE run_id=?",
            )
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
            ("failed".into(), "ready_failed".into(), 1),
        );

        // Even if the old parent deadline is made overdue after Run terminal,
        // claim-time recovery must neither rewrite rows nor reopen continuation.
        sqlx::query(
            "UPDATE model_tool_call_batches
             SET parent_operation_deadline=datetime('now','-1 second') WHERE run_id=?",
        )
        .bind(run_id.as_str())
        .execute(&control)
        .await
        .unwrap();
        let before_recovery = sqlx::query_as::<_, (String, String, i64, String)>(
            "SELECT call_status,effect_evidence,lease_epoch,fencing_token
             FROM model_tool_calls WHERE run_id=? ORDER BY call_index",
        )
        .bind(run_id.as_str())
        .fetch_all(&control)
        .await
        .unwrap();
        assert!(repository
            .claim_model_tool_calls("terminal-recovery-worker", 60, 2, 2)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            sqlx::query_as::<_, (String, String, i64, String)>(
                "SELECT call_status,effect_evidence,lease_epoch,fencing_token
                 FROM model_tool_calls WHERE run_id=? ORDER BY call_index",
            )
            .bind(run_id.as_str())
            .fetch_all(&control)
            .await
            .unwrap(),
            before_recovery
        );
        assert!(repository
            .claim_scheduler_tasks("terminal-parent-worker", 60, 16)
            .await
            .unwrap()
            .iter()
            .all(|claim| claim.run_id() != &run_id));
        assert!(matches!(
            repository
                .mark_model_tool_call_started(&running)
                .await
                .unwrap(),
            ModelToolTaskTransitionOutcome::StaleLease
        ));
        assert!(matches!(
            repository
                .heartbeat_model_tool_call(&running, 60)
                .await
                .unwrap(),
            ModelToolTaskHeartbeatOutcome::StaleLease
        ));
        assert!(matches!(
            repository
                .commit_model_tool_call_outcome(
                    &running,
                    &ModelToolTaskOutcome::succeeded(json!({"value": "late"})).unwrap(),
                )
                .await
                .unwrap(),
            ModelToolTaskTransitionOutcome::StaleLease
        ));
    }
}

#[tokio::test]
async fn sqlite_terminal_run_preserves_checkpointed_model_tool_intent_and_fences_activation() {
    for (suffix, reason, expected_lifecycle, expected_quiescence) in [
        (
            "cancelled",
            TerminationReason::Cancelled,
            RunLifecycle::Cancelled,
            SchedulerQuiescence::RunCancelled,
        ),
        (
            "timed_out",
            TerminationReason::TimedOut,
            RunLifecycle::TimedOut,
            SchedulerQuiescence::RunFailed,
        ),
    ] {
        let (plan, descriptors, versioned) = model_call_fixture();
        let linked =
            LinkedPlan::link(&plan, &descriptors, &SubflowContractRegistry::new()).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let database = directory
            .path()
            .join(format!("checkpoint-before-{suffix}.sqlite"));
        let repository = SqliteDurableRepository::connect_path(&database)
            .await
            .unwrap();
        assert_eq!(
            repository.install_versioned_plan(&versioned).await.unwrap(),
            PlanInstallOutcome::Installed
        );
        let control = SqlitePool::connect_with(
            SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
        let run_id = RunId::new(format!("run_sqlite_checkpoint_terminal_{suffix}")).unwrap();
        let (parent, fence) =
            prepare_sqlite_task(&repository, &control, &versioned, &linked, &run_id).await;
        assert!(matches!(
            repository
                .reserve_model_call(&parent, 1, true)
                .await
                .unwrap(),
            SchedulerTaskCommitOutcome::Committed { .. }
        ));
        let checkpoint = tool_call_checkpoint(1, 10, "Shanghai");
        assert!(matches!(
            repository
                .checkpoint_model_tool_call_batch(&parent, &checkpoint)
                .await
                .unwrap(),
            SchedulerTaskCommitOutcome::Committed { .. }
        ));

        let (batch_before, calls_before) =
            sqlite_checkpoint_intent_snapshot(&control, &run_id).await;
        assert_eq!(calls_before.len(), 2);
        assert_eq!(
            sqlx::query_as::<_, (String, String, i64, i64)>(
                "SELECT execution_status,continuation_status,
                        (SELECT COUNT(*) FROM model_tool_calls c
                         WHERE c.run_id=b.run_id AND c.call_status='pending'),
                        (SELECT COUNT(*) FROM model_tool_calls c
                         WHERE c.run_id=b.run_id AND (
                            c.tool_task_id IS NOT NULL OR c.effect_id IS NOT NULL
                            OR c.action_id IS NOT NULL OR c.tool_attempt_no IS NOT NULL
                            OR c.lease_epoch IS NOT NULL OR c.fencing_token IS NOT NULL
                            OR c.effect_evidence IS NOT NULL OR c.available_at IS NOT NULL
                            OR c.projection_version<>0))
                 FROM model_tool_call_batches b WHERE run_id=?",
            )
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
            ("checkpointed".into(), "checkpointed".into(), 2, 0),
            "T1 is immutable intent and has no child execution authority",
        );
        assert!(repository
            .claim_model_tool_calls("checkpoint-terminal-must-not-claim", 60, 8, 8)
            .await
            .unwrap()
            .is_empty());

        // Make the parent claim recoverable before cancellation wins. A
        // terminal Run must fence both that reclaim path and direct activation.
        expire_sqlite_scheduler_task_claim(&control, &run_id).await;
        let termination_key = key(&format!("checkpoint-terminal-{suffix}"), &run_id);
        assert_eq!(
            sqlx::query(
                "UPDATE workflow_runs SET lifecycle='terminating',admission_state='draining',
                    termination_intent_reason=?,termination_intent_transition_key=?,
                    termination_intent_at=CURRENT_TIMESTAMP,
                    projection_version=projection_version+1,updated_at=CURRENT_TIMESTAMP
                 WHERE run_id=? AND lifecycle='active' AND admission_state='open'",
            )
            .bind(match reason {
                TerminationReason::Cancelled => "cancelled",
                TerminationReason::TimedOut => "timed_out",
                _ => unreachable!(),
            })
            .bind(termination_key.as_str())
            .bind(run_id.as_str())
            .execute(&control)
            .await
            .unwrap()
            .rows_affected(),
            1
        );
        drive_terminal_scheduler_action(&repository, &linked, &fence, expected_quiescence).await;
        assert_eq!(
            repository
                .load_run(&run_id)
                .await
                .unwrap()
                .unwrap()
                .lifecycle(),
            expected_lifecycle
        );

        assert_eq!(
            sqlite_checkpoint_intent_snapshot(&control, &run_id).await,
            (batch_before.clone(), calls_before.clone()),
            "global terminalization must preserve checkpoint intent byte-for-byte",
        );
        assert!(matches!(
            repository
                .activate_model_tool_call_batch(&parent, 1)
                .await
                .unwrap(),
            ModelToolBatchActivationOutcome::RunTerminal
        ));
        assert!(matches!(
            repository
                .checkpoint_model_tool_call_batch(&parent, &checkpoint)
                .await
                .unwrap(),
            SchedulerTaskCommitOutcome::StaleLease
        ));
        assert!(matches!(
            repository
                .load_model_tool_parent_resume(&parent)
                .await
                .unwrap(),
            SchedulerTaskCommitOutcome::StaleLease
        ));
        assert!(repository
            .claim_scheduler_tasks("checkpoint-terminal-recovery-parent", 60, 16)
            .await
            .unwrap()
            .iter()
            .all(|claim| claim.run_id() != &run_id));
        assert!(repository
            .claim_model_tool_calls("checkpoint-terminal-recovery-tool", 60, 8, 8)
            .await
            .unwrap()
            .is_empty());

        // Claim-time expiration recovery is a second line of defense. It must
        // leave the non-executable checkpoint history untouched.
        assert_eq!(
            sqlite_checkpoint_intent_snapshot(&control, &run_id).await,
            (batch_before, calls_before),
        );
    }
}

#[tokio::test]
async fn sqlite_model_tool_checkpoint_transaction_failure_leaves_no_partial_batch() {
    let (plan, descriptors, versioned) = model_call_fixture();
    let linked = LinkedPlan::link(&plan, &descriptors, &SubflowContractRegistry::new()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let database = directory
        .path()
        .join("checkpoint-transaction-failure.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .foreign_keys(true),
    )
    .await
    .unwrap();
    let run_id = RunId::new("run_sqlite_checkpoint_transaction_failure").unwrap();
    let (parent, _) =
        prepare_sqlite_task(&repository, &control, &versioned, &linked, &run_id).await;
    assert!(matches!(
        repository
            .reserve_model_call(&parent, 1, true)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    sqlx::query(
        "CREATE TRIGGER inject_second_tool_call_checkpoint_failure
         BEFORE INSERT ON model_tool_calls WHEN NEW.call_index=1
         BEGIN
             SELECT RAISE(ABORT,'injected checkpoint transaction failure');
         END",
    )
    .execute(&control)
    .await
    .unwrap();

    let checkpoint = tool_call_checkpoint(1, 10, "Shanghai");
    assert!(repository
        .checkpoint_model_tool_call_batch(&parent, &checkpoint)
        .await
        .is_err());
    assert_eq!(
        sqlx::query_as::<_, (String, Option<String>, Option<String>, i64)>(
            "SELECT call_status,finish_reason,usage,usage_complete
             FROM model_call_usage WHERE run_id=?",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        ("started".into(), None, None, 0),
        "checkpoint failure must roll model usage back to the pre-checkpoint state",
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, i64)>(
            "SELECT
                (SELECT COUNT(*) FROM model_tool_call_batches WHERE run_id=?),
                (SELECT COUNT(*) FROM model_tool_calls WHERE run_id=?)",
        )
        .bind(run_id.as_str())
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        (0, 0),
        "neither the batch nor its first child may leak from the aborted transaction",
    );

    sqlx::query("DROP TRIGGER inject_second_tool_call_checkpoint_failure")
        .execute(&control)
        .await
        .unwrap();
    assert!(matches!(
        repository
            .checkpoint_model_tool_call_batch(&parent, &checkpoint)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    assert_eq!(
        sqlx::query_as::<_, (i64, i64)>(
            "SELECT
                (SELECT COUNT(*) FROM model_tool_call_batches WHERE run_id=?),
                (SELECT COUNT(*) FROM model_tool_calls WHERE run_id=?)",
        )
        .bind(run_id.as_str())
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        (1, 2),
        "the exact checkpoint remains retryable after a full rollback",
    );
}

#[tokio::test]
async fn sqlite_model_tool_materialization_commit_survives_restart_as_complete_waiting_batch() {
    let run_name = "run_sqlite_materialization_restart";
    let (directory, repository, control, run_id, _parent) =
        prepare_sqlite_model_tool_batch(run_name, model_tool_queue_binding()).await;
    let database = directory.path().join(format!("{run_name}.sqlite"));
    let expected = ("active".to_owned(), "waiting_tools".to_owned(), 2, 2);
    assert_eq!(
        sqlx::query_as::<_, (String, String, i64, i64)>(
            "SELECT execution_status,continuation_status,
                    (SELECT COUNT(*) FROM model_tool_calls c
                     WHERE c.run_id=b.run_id AND c.call_status='pending'),
                    (SELECT COUNT(*) FROM model_tool_calls c
                     WHERE c.run_id=b.run_id AND c.tool_task_id IS NOT NULL
                       AND c.effect_id IS NOT NULL AND c.action_id IS NOT NULL
                       AND c.tool_attempt_no=1 AND c.lease_epoch=1
                       AND c.fencing_token IS NOT NULL AND c.effect_evidence='not_started'
                       AND c.available_at IS NOT NULL AND c.projection_version=1)
             FROM model_tool_call_batches b WHERE run_id=?",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        expected,
    );

    // The materialization transaction has returned successfully. Dropping
    // every repository connection here is the process-crash cut.
    drop(repository);
    drop(control);
    let restarted = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    let restarted_control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .foreign_keys(true),
    )
    .await
    .unwrap();
    assert_eq!(
        sqlx::query_as::<_, (String, String, i64, i64)>(
            "SELECT execution_status,continuation_status,
                    (SELECT COUNT(*) FROM model_tool_calls c
                     WHERE c.run_id=b.run_id AND c.call_status='pending'),
                    (SELECT COUNT(*) FROM model_tool_calls c
                     WHERE c.run_id=b.run_id AND c.tool_task_id IS NOT NULL
                       AND c.effect_id IS NOT NULL AND c.action_id IS NOT NULL
                       AND c.tool_attempt_no=1 AND c.lease_epoch=1
                       AND c.fencing_token IS NOT NULL AND c.effect_evidence='not_started'
                       AND c.available_at IS NOT NULL AND c.projection_version=1)
             FROM model_tool_call_batches b WHERE run_id=?",
        )
        .bind(run_id.as_str())
        .fetch_one(&restarted_control)
        .await
        .unwrap(),
        expected,
        "restart must observe all children and the waiting parent barrier together",
    );
    expire_sqlite_scheduler_task_claim(&restarted_control, &run_id).await;
    assert!(restarted
        .claim_scheduler_tasks("materialization-restart-parent", 60, 16)
        .await
        .unwrap()
        .iter()
        .all(|claim| claim.run_id() != &run_id));
    let mut children = restarted
        .claim_model_tool_calls("materialization-restart-tools", 60, 2, 2)
        .await
        .unwrap();
    children.sort_by_key(|claim| claim.identity().call_index());
    assert_eq!(children.len(), 2);
    assert_eq!(children[0].identity().call_index(), 0);
    assert_eq!(children[1].identity().call_index(), 1);
}

#[tokio::test]
async fn sqlite_model_tool_result_commit_crash_cuts_converge_with_stable_effect_identity() {
    for (suffix, crash_point, expects_retry) in [
        (
            "before_result_commit",
            SchedulerCrashPoint::BeforeResultCommit,
            true,
        ),
        (
            "after_result_commit",
            SchedulerCrashPoint::AfterResultCommit,
            false,
        ),
    ] {
        let run_name = format!("run_sqlite_tool_crash_{suffix}");
        let (_directory, database, repository, control, run_id) =
            prepare_sqlite_single_weather_batch(&run_name).await;
        let action = Arc::new(EffectIdempotentWeatherAction::new());
        let registry = weather_action_registry(Arc::clone(&action));
        let crash = FailOnceSchedulerCrash::new(crash_point);

        let error = consume_model_tool_task_once(
            &repository,
            &registry,
            "tool-crash-worker",
            30,
            4,
            CancellationToken::new(),
            &crash,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), "ENGINE_REPOSITORY_SCHEDULER_CRASH_INJECTED");
        assert!(crash.fired());
        assert_eq!(action.invocations(), 1);
        assert_eq!(action.external_applications(), 1);
        let effect_id = sqlx::query_scalar::<_, String>(
            "SELECT effect_id FROM model_tool_calls WHERE run_id=?",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap();
        assert_eq!(action.effect_ids(), vec![effect_id.clone()]);

        if expects_retry {
            assert_eq!(
                sqlx::query_as::<_, (String, String, Option<String>, String, String)>(
                    "SELECT c.call_status,c.effect_evidence,c.result_json,
                            b.execution_status,b.continuation_status
                     FROM model_tool_calls c
                     JOIN model_tool_call_batches b ON b.run_id=c.run_id
                       AND b.activation_id=c.activation_id AND b.attempt_no=c.attempt_no
                       AND b.model_call_no=c.model_call_no
                     WHERE c.run_id=?",
                )
                .bind(run_id.as_str())
                .fetch_one(&control)
                .await
                .unwrap(),
                (
                    "running".into(),
                    "started".into(),
                    None,
                    "active".into(),
                    "waiting_tools".into(),
                ),
                "BeforeResultCommit leaves only started/unknown recovery authority",
            );
        } else {
            assert_eq!(
                sqlx::query_as::<_, (String, String, String, String, String)>(
                    "SELECT c.call_status,c.effect_evidence,c.result_json,
                            b.execution_status,b.continuation_status
                     FROM model_tool_calls c
                     JOIN model_tool_call_batches b ON b.run_id=c.run_id
                       AND b.activation_id=c.activation_id AND b.attempt_no=c.attempt_no
                       AND b.model_call_no=c.model_call_no
                     WHERE c.run_id=?",
                )
                .bind(run_id.as_str())
                .fetch_one(&control)
                .await
                .unwrap(),
                (
                    "succeeded".into(),
                    "committed".into(),
                    r#"{"value":"effect-applied-once"}"#.into(),
                    "succeeded".into(),
                    "ready_continue".into(),
                ),
                "AfterResultCommit crash is after result and barrier commit",
            );
        }

        drop(repository);
        drop(control);
        let restarted = SqliteDurableRepository::connect_path(&database)
            .await
            .unwrap();
        let restarted_control = SqlitePool::connect_with(
            SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
        let recovered = if expects_retry {
            sqlx::query(
                "UPDATE model_tool_calls
                 SET claim_expires_at=datetime('now','-1 second') WHERE run_id=?",
            )
            .bind(run_id.as_str())
            .execute(&restarted_control)
            .await
            .unwrap();
            consume_model_tool_task_once(
                &restarted,
                &registry,
                "tool-recovery-worker",
                30,
                4,
                CancellationToken::new(),
                &NoSchedulerCrash,
            )
            .await
            .unwrap()
        } else {
            consume_model_tool_task_once(
                &restarted,
                &registry,
                "tool-recovery-worker",
                30,
                4,
                CancellationToken::new(),
                &NoSchedulerCrash,
            )
            .await
            .unwrap()
        };
        if expects_retry {
            assert!(matches!(
                recovered,
                ModelToolWorkerPumpOutcome::Succeeded {
                    exact_replay: false,
                    ..
                }
            ));
            assert_eq!(action.invocations(), 2);
        } else {
            assert!(matches!(recovered, ModelToolWorkerPumpOutcome::NoTask));
            assert_eq!(action.invocations(), 1);
        }
        assert_eq!(action.external_applications(), 1);
        assert_eq!(action.effect_ids(), vec![effect_id.clone()]);
        assert_eq!(
            sqlx::query_as::<
                _,
                (
                    String,
                    String,
                    String,
                    i64,
                    i64,
                    Option<String>,
                    String,
                    String
                ),
            >(
                "SELECT c.call_status,c.effect_evidence,c.result_json,
                        c.tool_attempt_no,c.lease_loss_count,c.last_lease_loss_evidence,
                        b.execution_status,b.continuation_status
                 FROM model_tool_calls c
                 JOIN model_tool_call_batches b ON b.run_id=c.run_id
                   AND b.activation_id=c.activation_id AND b.attempt_no=c.attempt_no
                   AND b.model_call_no=c.model_call_no
                 WHERE c.run_id=? AND c.effect_id=?",
            )
            .bind(run_id.as_str())
            .bind(&effect_id)
            .fetch_one(&restarted_control)
            .await
            .unwrap(),
            (
                "succeeded".into(),
                "committed".into(),
                r#"{"value":"effect-applied-once"}"#.into(),
                if expects_retry { 2 } else { 1 },
                if expects_retry { 1 } else { 0 },
                expects_retry.then(|| "unknown".to_owned()),
                "succeeded".into(),
                "ready_continue".into(),
            ),
            "result, committed evidence, and the parent barrier must converge",
        );
        let continuation = restarted
            .claim_scheduler_tasks("tool-result-parent-recovery", 60, 16)
            .await
            .unwrap()
            .into_iter()
            .find(|claim| claim.run_id() == &run_id)
            .expect("the committed final child must wake exactly one parent continuation");
        assert!(matches!(
            restarted
                .load_model_tool_parent_resume(&continuation)
                .await
                .unwrap(),
            SchedulerTaskCommitOutcome::Committed {
                result: Some(ModelToolParentResume::ReadyContinue { .. })
            }
        ));
    }
}

#[tokio::test]
async fn postgres_cancelled_run_closes_active_model_tool_work_and_fences_recovery() {
    let Ok(database_url) = std::env::var("V3_TEST_POSTGRES_URL") else {
        return;
    };
    let schema = format!("terminal_model_tool_v3_{}", Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let control = PgPoolOptions::new()
        .max_connections(4)
        .connect(&scoped_url)
        .await
        .unwrap();
    let repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    repository.initialize_schema().await.unwrap();
    let (plan, descriptors, versioned) = model_call_fixture();
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let run_id = RunId::new("run_pg_model_tool_terminal_cancelled").unwrap();
    let (parent, fence) =
        prepare_postgres_task(&repository, &control, &versioned, &linked, &run_id).await;
    assert!(matches!(
        repository
            .reserve_model_call(&parent, 1, true)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    assert!(matches!(
        repository
            .checkpoint_model_tool_call_batch(&parent, &tool_call_checkpoint(1, 10, "Shanghai"),)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    assert!(matches!(
        repository
            .activate_model_tool_call_batch(&parent, 1)
            .await
            .unwrap(),
        ModelToolBatchActivationOutcome::Activated(_)
    ));
    let running = repository
        .claim_model_tool_calls("pg-terminal-tool-worker", 60, 1, 2)
        .await
        .unwrap()
        .pop()
        .expect("one PostgreSQL tool member must be claimed");
    assert_eq!(running.identity().call_index(), 0);
    assert!(matches!(
        repository
            .mark_model_tool_call_started(&running)
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::Committed(())
    ));
    assert_eq!(
        sqlx::query_as::<_, (i32, String)>(
            "SELECT call_index,call_status FROM model_tool_calls
             WHERE run_id=$1 ORDER BY call_index",
        )
        .bind(run_id.as_str())
        .fetch_all(&control)
        .await
        .unwrap(),
        vec![(0, "running".into()), (1, "pending".into())],
    );

    let termination_key = key("pg-terminal-cancelled", &run_id);
    assert_eq!(
        sqlx::query(
            "UPDATE workflow_runs SET lifecycle='terminating',admission_state='draining',
                termination_intent_reason='cancelled',termination_intent_transition_key=$1,
                termination_intent_at=clock_timestamp(),
                projection_version=projection_version+1,updated_at=clock_timestamp()
             WHERE run_id=$2 AND lifecycle='active' AND admission_state='open'",
        )
        .bind(termination_key.as_str())
        .bind(run_id.as_str())
        .execute(&control)
        .await
        .unwrap()
        .rows_affected(),
        1
    );
    drive_terminal_scheduler_action(
        &repository,
        &linked,
        &fence,
        SchedulerQuiescence::RunCancelled,
    )
    .await;
    assert_eq!(
        repository
            .load_run(&run_id)
            .await
            .unwrap()
            .unwrap()
            .lifecycle(),
        RunLifecycle::Cancelled
    );

    let rows = sqlx::query_as::<_, (i32, String, String, String, bool, i64, String)>(
        "SELECT call_index,call_status,effect_evidence,failure_class,
                claim_owner IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL,
                lease_epoch,fencing_token
         FROM model_tool_calls WHERE run_id=$1 ORDER BY call_index",
    )
    .bind(run_id.as_str())
    .fetch_all(&control)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        (rows[0].1.as_str(), rows[0].2.as_str(), rows[0].3.as_str()),
        ("failed", "unknown", "effect_outcome_unknown")
    );
    assert_eq!(
        (rows[1].1.as_str(), rows[1].2.as_str(), rows[1].3.as_str()),
        ("cancelled", "not_started", "safe")
    );
    assert!(rows
        .iter()
        .all(|row| row.4 && row.5 == 2 && row.6.contains(":run-terminal:")));
    assert_eq!(
        sqlx::query_as::<_, (String, String, bool)>(
            "SELECT execution_status,continuation_status,completed_at IS NOT NULL
             FROM model_tool_call_batches WHERE run_id=$1",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        ("failed".into(), "ready_failed".into(), true),
    );

    sqlx::query(
        "UPDATE model_tool_call_batches
         SET parent_operation_deadline=clock_timestamp()-INTERVAL '1 second' WHERE run_id=$1",
    )
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .unwrap();
    let before_recovery = sqlx::query_as::<_, (String, String, i64, String)>(
        "SELECT call_status,effect_evidence,lease_epoch,fencing_token
         FROM model_tool_calls WHERE run_id=$1 ORDER BY call_index",
    )
    .bind(run_id.as_str())
    .fetch_all(&control)
    .await
    .unwrap();
    assert!(repository
        .claim_model_tool_calls("pg-terminal-recovery-worker", 60, 2, 2)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        sqlx::query_as::<_, (String, String, i64, String)>(
            "SELECT call_status,effect_evidence,lease_epoch,fencing_token
             FROM model_tool_calls WHERE run_id=$1 ORDER BY call_index",
        )
        .bind(run_id.as_str())
        .fetch_all(&control)
        .await
        .unwrap(),
        before_recovery
    );
    assert!(repository
        .claim_scheduler_tasks("pg-terminal-parent-worker", 60, 16)
        .await
        .unwrap()
        .iter()
        .all(|claim| claim.run_id() != &run_id));
    assert!(matches!(
        repository
            .mark_model_tool_call_started(&running)
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::StaleLease
    ));
    assert!(matches!(
        repository
            .heartbeat_model_tool_call(&running, 60)
            .await
            .unwrap(),
        ModelToolTaskHeartbeatOutcome::StaleLease
    ));
    assert!(matches!(
        repository
            .commit_model_tool_call_outcome(
                &running,
                &ModelToolTaskOutcome::succeeded(json!({"value": "late"})).unwrap(),
            )
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::StaleLease
    ));

    drop(repository);
    drop(control);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
}

#[tokio::test]
async fn postgres_terminal_run_preserves_checkpointed_model_tool_intent_and_fences_activation() {
    let Ok(database_url) = std::env::var("V3_TEST_POSTGRES_URL") else {
        return;
    };
    let schema = format!("checkpoint_terminal_tool_v3_{}", Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let control = PgPoolOptions::new()
        .max_connections(4)
        .connect(&scoped_url)
        .await
        .unwrap();
    let repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    repository.initialize_schema().await.unwrap();
    let (plan, descriptors, versioned) = model_call_fixture();
    let linked = LinkedPlan::link(&plan, &descriptors, &SubflowContractRegistry::new()).unwrap();
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );

    for (suffix, reason, expected_lifecycle, expected_quiescence) in [
        (
            "cancelled",
            "cancelled",
            RunLifecycle::Cancelled,
            SchedulerQuiescence::RunCancelled,
        ),
        (
            "timed_out",
            "timed_out",
            RunLifecycle::TimedOut,
            SchedulerQuiescence::RunFailed,
        ),
    ] {
        let run_id = RunId::new(format!("run_pg_checkpoint_terminal_{suffix}")).unwrap();
        let (parent, fence) =
            prepare_postgres_task(&repository, &control, &versioned, &linked, &run_id).await;
        assert!(matches!(
            repository
                .reserve_model_call(&parent, 1, true)
                .await
                .unwrap(),
            SchedulerTaskCommitOutcome::Committed { .. }
        ));
        let checkpoint = tool_call_checkpoint(1, 10, "Shanghai");
        assert!(matches!(
            repository
                .checkpoint_model_tool_call_batch(&parent, &checkpoint)
                .await
                .unwrap(),
            SchedulerTaskCommitOutcome::Committed { .. }
        ));
        let (batch_before, calls_before) =
            postgres_checkpoint_intent_snapshot(&control, &run_id).await;
        assert_eq!(calls_before.len(), 2);
        assert_eq!(
            sqlx::query_as::<_, (String, String, i64, i64)>(
                "SELECT execution_status,continuation_status,
                        (SELECT COUNT(*) FROM model_tool_calls c
                         WHERE c.run_id=b.run_id AND c.call_status='pending'),
                        (SELECT COUNT(*) FROM model_tool_calls c
                         WHERE c.run_id=b.run_id AND (
                            c.tool_task_id IS NOT NULL OR c.effect_id IS NOT NULL
                            OR c.action_id IS NOT NULL OR c.tool_attempt_no IS NOT NULL
                            OR c.lease_epoch IS NOT NULL OR c.fencing_token IS NOT NULL
                            OR c.effect_evidence IS NOT NULL OR c.available_at IS NOT NULL
                            OR c.projection_version<>0))
                 FROM model_tool_call_batches b WHERE run_id=$1",
            )
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
            ("checkpointed".into(), "checkpointed".into(), 2, 0),
            "PostgreSQL T1 has no child execution authority",
        );
        assert!(repository
            .claim_model_tool_calls("pg-checkpoint-terminal-must-not-claim", 60, 8, 8)
            .await
            .unwrap()
            .is_empty());

        sqlx::query(
            "UPDATE task_outbox
             SET claim_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE run_id=$1",
        )
        .bind(run_id.as_str())
        .execute(&control)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE node_attempts
             SET lease_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE run_id=$1",
        )
        .bind(run_id.as_str())
        .execute(&control)
        .await
        .unwrap();
        let termination_key = key(&format!("pg-checkpoint-terminal-{suffix}"), &run_id);
        assert_eq!(
            sqlx::query(
                "UPDATE workflow_runs SET lifecycle='terminating',admission_state='draining',
                    termination_intent_reason=$1,termination_intent_transition_key=$2,
                    termination_intent_at=clock_timestamp(),
                    projection_version=projection_version+1,updated_at=clock_timestamp()
                 WHERE run_id=$3 AND lifecycle='active' AND admission_state='open'",
            )
            .bind(reason)
            .bind(termination_key.as_str())
            .bind(run_id.as_str())
            .execute(&control)
            .await
            .unwrap()
            .rows_affected(),
            1
        );
        drive_terminal_scheduler_action(&repository, &linked, &fence, expected_quiescence).await;
        assert_eq!(
            repository
                .load_run(&run_id)
                .await
                .unwrap()
                .unwrap()
                .lifecycle(),
            expected_lifecycle
        );
        assert_eq!(
            postgres_checkpoint_intent_snapshot(&control, &run_id).await,
            (batch_before.clone(), calls_before.clone()),
            "PostgreSQL global terminalization must preserve checkpoint intent byte-for-byte",
        );
        assert!(matches!(
            repository
                .activate_model_tool_call_batch(&parent, 1)
                .await
                .unwrap(),
            ModelToolBatchActivationOutcome::RunTerminal
        ));
        assert!(matches!(
            repository
                .checkpoint_model_tool_call_batch(&parent, &checkpoint)
                .await
                .unwrap(),
            SchedulerTaskCommitOutcome::StaleLease
        ));
        assert!(matches!(
            repository
                .load_model_tool_parent_resume(&parent)
                .await
                .unwrap(),
            SchedulerTaskCommitOutcome::StaleLease
        ));
        assert!(repository
            .claim_scheduler_tasks("pg-checkpoint-terminal-recovery-parent", 60, 16)
            .await
            .unwrap()
            .iter()
            .all(|claim| claim.run_id() != &run_id));
        assert!(repository
            .claim_model_tool_calls("pg-checkpoint-terminal-recovery-tool", 60, 8, 8)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            postgres_checkpoint_intent_snapshot(&control, &run_id).await,
            (batch_before, calls_before),
            "PostgreSQL recovery must not materialize terminal checkpoint history",
        );
    }

    drop(repository);
    drop(control);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
}

#[tokio::test]
async fn sqlite_model_tool_queue_activates_claims_parallel_calls_and_opens_barrier_once() {
    let (plan, descriptors, versioned) = model_call_fixture();
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("scheduler-model-tool-queue.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .foreign_keys(true),
    )
    .await
    .unwrap();
    let run_id = RunId::new("run_sqlite_model_tool_queue").unwrap();
    let (parent, _) =
        prepare_sqlite_task(&repository, &control, &versioned, &linked, &run_id).await;
    assert!(matches!(
        repository
            .reserve_model_call(&parent, 1, true)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    let checkpoint = tool_call_checkpoint(1, 10, "Shanghai");
    assert!(matches!(
        repository
            .checkpoint_model_tool_call_batch(&parent, &checkpoint)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    let activation = match repository
        .activate_model_tool_call_batch(&parent, 1)
        .await
        .unwrap()
    {
        ModelToolBatchActivationOutcome::Activated(activation) => activation,
        _ => panic!("tool batch did not activate"),
    };
    assert_eq!(activation.tasks().len(), 2);
    assert!(activation
        .tasks()
        .iter()
        .all(|task| task.public_item().is_none() && task.public_arguments_jcs().is_none()));
    assert!(matches!(
        repository
            .activate_model_tool_call_batch(&parent, 1)
            .await
            .unwrap(),
        ModelToolBatchActivationOutcome::ExactReplay(replay) if replay == activation
    ));

    let left_repository = repository.clone();
    let right_repository = repository.clone();
    let (left, right) = tokio::join!(
        left_repository.claim_model_tool_calls("tool-worker-a", 60, 1, 2),
        right_repository.claim_model_tool_calls("tool-worker-b", 60, 1, 2),
    );
    let mut claims = left
        .unwrap()
        .into_iter()
        .chain(right.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        claims.len(),
        2,
        "parallel claims must not duplicate or omit calls"
    );
    claims.sort_by_key(|claim| claim.identity().call_index());
    assert_ne!(
        claims[0].identity().tool_task_id(),
        claims[1].identity().tool_task_id()
    );

    let weather = claims.remove(0);
    assert!(matches!(
        repository
            .mark_model_tool_call_started(&weather)
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::Committed(())
    ));
    assert!(matches!(
        repository
            .mark_model_tool_call_started(&weather)
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::ExactReplay(())
    ));
    let weather_renewed = match repository
        .heartbeat_model_tool_call(&weather, 60)
        .await
        .unwrap()
    {
        ModelToolTaskHeartbeatOutcome::Renewed(claim) => claim,
        _ => panic!("tool heartbeat did not renew"),
    };
    let weather_outcome = ModelToolTaskOutcome::succeeded(json!({"value": "sunny"})).unwrap();
    assert!(matches!(
        repository
            .commit_model_tool_call_outcome(&weather, &weather_outcome)
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::StaleLease
    ));
    let weather_receipt = match repository
        .commit_model_tool_call_outcome(&weather_renewed, &weather_outcome)
        .await
        .unwrap()
    {
        ModelToolTaskTransitionOutcome::Committed(receipt) => receipt,
        _ => panic!("weather result was not committed"),
    };
    assert_eq!(
        weather_receipt.continuation_status(),
        ModelToolContinuationStatus::WaitingTools
    );

    let clock = claims.remove(0);
    assert!(matches!(
        repository
            .mark_model_tool_call_started(&clock)
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::Committed(())
    ));
    let clock_outcome = ModelToolTaskOutcome::succeeded(json!({"value": "12:00"})).unwrap();
    let clock_receipt = match repository
        .commit_model_tool_call_outcome(&clock, &clock_outcome)
        .await
        .unwrap()
    {
        ModelToolTaskTransitionOutcome::Committed(receipt) => receipt,
        _ => panic!("clock result was not committed"),
    };
    assert_eq!(
        clock_receipt.continuation_status(),
        ModelToolContinuationStatus::ReadyContinue
    );
    assert!(matches!(
        repository
            .commit_model_tool_call_outcome(&clock, &clock_outcome)
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::ExactReplay(receipt)
            if receipt.continuation_status() == ModelToolContinuationStatus::ReadyContinue
    ));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT continuation_status FROM model_tool_call_batches WHERE run_id=?",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "ready_continue"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT task_state FROM task_outbox WHERE run_id=?")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        "pending",
        "the barrier wakeup is committed with the final tool outcome"
    );
    let stored_results = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM model_tool_calls
         WHERE run_id=? AND call_status='succeeded' AND result_json IS NOT NULL",
    )
    .bind(run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(stored_results, 2);
}

#[tokio::test]
async fn sqlite_parent_claim_excludes_waiting_tools_even_after_parent_lease_expiry() {
    let (_directory, repository, control, run_id, _parent) = prepare_sqlite_model_tool_batch(
        "run_sqlite_parent_waiting_tools",
        model_tool_queue_binding(),
    )
    .await;
    expire_sqlite_scheduler_task_claim(&control, &run_id).await;

    assert!(repository
        .claim_scheduler_tasks("must-not-steal-waiting-parent", 60, 1)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT continuation_status FROM model_tool_call_batches WHERE run_id=?",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "waiting_tools"
    );
}

#[tokio::test]
async fn sqlite_checkpointed_parent_reclaim_renews_running_attempt_without_resetting_identity() {
    let (plan, descriptors, versioned) = model_call_fixture();
    let linked = LinkedPlan::link(&plan, &descriptors, &SubflowContractRegistry::new()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("checkpointed-parent-reclaim.sqlite");
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );
    let control = SqlitePool::connect_with(
        SqliteConnectOptions::new()
            .filename(&database)
            .foreign_keys(true),
    )
    .await
    .unwrap();
    let run_id = RunId::new("run_sqlite_checkpointed_parent_reclaim").unwrap();
    let (parent, _) =
        prepare_sqlite_task(&repository, &control, &versioned, &linked, &run_id).await;
    assert!(matches!(
        repository
            .reserve_model_call(&parent, 1, true)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    assert!(matches!(
        repository
            .checkpoint_model_tool_call_batch(&parent, &tool_call_checkpoint(1, 10, "Shanghai"))
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    assert_eq!(
        sqlx::query_as::<_, (String, String, i64, i64)>(
            "SELECT execution_status,continuation_status,
                    (SELECT COUNT(*) FROM model_tool_calls c
                     WHERE c.run_id=b.run_id AND c.call_status='pending'),
                    (SELECT COUNT(*) FROM model_tool_calls c
                     WHERE c.run_id=b.run_id
                       AND (c.tool_task_id IS NOT NULL OR c.effect_id IS NOT NULL
                            OR c.projection_version<>0))
             FROM model_tool_call_batches b WHERE run_id=?",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        ("checkpointed".into(), "checkpointed".into(), 2, 0),
        "the first durable transition is a complete but non-executable batch intent",
    );
    assert!(
        repository
            .claim_model_tool_calls("must-not-claim-checkpoint-intent", 60, 8, 8)
            .await
            .unwrap()
            .is_empty(),
        "checkpoint rows cannot be mistaken for executable child tasks",
    );
    let before = sqlx::query_as::<_, (i64, i64, String, String)>(
        "SELECT attempt_no,lease_epoch,fencing_token,started_at
         FROM node_attempts WHERE run_id=?",
    )
    .bind(run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    expire_sqlite_scheduler_task_claim(&control, &run_id).await;

    let recovered = repository
        .claim_scheduler_tasks("checkpoint-activation-worker", 60, 1)
        .await
        .unwrap()
        .pop()
        .expect("latest checkpointed batch must be recoverable");
    assert_eq!(recovered.mode(), SchedulerTaskClaimMode::Execute);
    assert!(matches!(
        repository
            .load_model_tool_parent_resume(&recovered)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed {
            result: Some(ModelToolParentResume::ActivateCheckpointed {
                model_call_no: 1,
                ..
            })
        }
    ));
    let activation = match repository
        .activate_model_tool_call_batch(&recovered, 1)
        .await
        .unwrap()
    {
        ModelToolBatchActivationOutcome::Activated(activation) => activation,
        _ => panic!("recovered checkpoint did not atomically activate"),
    };
    assert_eq!(activation.tasks().len(), 2);
    assert_eq!(
        sqlx::query_as::<_, (String, String, i64)>(
            "SELECT execution_status,continuation_status,
                    (SELECT COUNT(*) FROM model_tool_calls c
                     WHERE c.run_id=b.run_id AND c.call_status='pending'
                       AND c.tool_task_id IS NOT NULL AND c.effect_id IS NOT NULL
                       AND c.projection_version=1)
             FROM model_tool_call_batches b WHERE run_id=?",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        ("active".into(), "waiting_tools".into(), 2),
        "one recovery transaction must materialize every child and the parent barrier together",
    );
    assert!(matches!(
        repository
            .activate_model_tool_call_batch(&recovered, 1)
            .await
            .unwrap(),
        ModelToolBatchActivationOutcome::ExactReplay(replay) if replay == activation
    ));
    let after = sqlx::query_as::<_, (i64, i64, String, String, String)>(
        "SELECT attempt_no,lease_epoch,fencing_token,started_at,worker_id
         FROM node_attempts WHERE run_id=?",
    )
    .bind(run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(
        (after.0, after.1, &after.2, &after.3),
        (before.0, before.1, &before.2, &before.3)
    );
    assert_eq!(after.4, "checkpoint-activation-worker");
    repository
        .load_scheduler_facts(&run_id)
        .await
        .expect("a legitimate continuation owner change must preserve scheduler fact recovery");

    sqlx::query(
        "UPDATE scheduler_checkpoints
         SET fact_payload=json_set(fact_payload,'$.claimed_by','forged-history-owner')
         WHERE run_id=? AND checkpoint_kind='task_started'",
    )
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .unwrap();
    assert!(
        repository.load_scheduler_facts(&run_id).await.is_err(),
        "mutable owner changes are allowed, but immutable task-start history remains hash-bound"
    );
}

#[tokio::test]
async fn sqlite_ready_parent_rebuilds_ordered_transcript_but_newer_started_call_finalizes_unknown()
{
    let (_directory, repository, control, run_id, parent) = prepare_sqlite_model_tool_batch(
        "run_sqlite_parent_ready_resume",
        model_tool_queue_binding(),
    )
    .await;
    let mut tools = repository
        .claim_model_tool_calls("ready-tool-worker", 60, 2, 2)
        .await
        .unwrap();
    tools.sort_by_key(|claim| claim.identity().call_index());
    for (index, tool) in tools.iter().enumerate() {
        assert!(matches!(
            repository.mark_model_tool_call_started(tool).await.unwrap(),
            ModelToolTaskTransitionOutcome::Committed(())
        ));
        assert!(matches!(
            repository
                .commit_model_tool_call_outcome(
                    tool,
                    &ModelToolTaskOutcome::succeeded(json!({"value": index.to_string()})).unwrap(),
                )
                .await
                .unwrap(),
            ModelToolTaskTransitionOutcome::Committed(_)
        ));
    }
    assert_eq!(
        repository
            .load_model_tool_parent_resume(&parent)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::StaleLease,
        "the barrier wakeup must revoke the activation claim captured by the old worker",
    );
    let continuation = repository
        .claim_scheduler_tasks("ready-parent-worker", 60, 1)
        .await
        .unwrap()
        .pop()
        .expect("ready barrier must wake parent");
    let resume = match repository
        .load_model_tool_parent_resume(&continuation)
        .await
        .unwrap()
    {
        SchedulerTaskCommitOutcome::Committed {
            result:
                Some(ModelToolParentResume::ReadyContinue {
                    completed_model_call_no,
                    next_model_call_no,
                    turns,
                    ..
                }),
        } => (completed_model_call_no, next_model_call_no, turns),
        other => panic!("ready transcript was not reconstructed: {other:?}"),
    };
    assert_eq!((resume.0, resume.1), (1, 2));
    assert_eq!(
        resume.2[0]
            .calls()
            .iter()
            .map(|call| call.call_id())
            .collect::<Vec<_>>(),
        vec!["call_weather", "call_clock"]
    );
    assert_eq!(
        resume.2[0]
            .tool_results()
            .iter()
            .map(|result| result.call_id())
            .collect::<Vec<_>>(),
        vec!["call_weather", "call_clock"]
    );
    assert!(matches!(
        repository
            .reserve_model_call(&continuation, 2, true)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    expire_sqlite_scheduler_task_claim(&control, &run_id).await;
    let lost = repository
        .claim_scheduler_tasks("must-not-repeat-provider", 60, 1)
        .await
        .unwrap()
        .pop()
        .expect("started provider call must be finalized, not resumed");
    assert_eq!(lost.mode(), SchedulerTaskClaimMode::FinalizeLeaseLoss);
    assert_eq!(lost.lease_loss_evidence(), Some(EffectEvidence::Unknown));
}

#[tokio::test]
async fn sqlite_model_tool_failure_terminalizes_and_fences_every_sibling_before_parent_wakeup() {
    let (_directory, repository, control, run_id, _parent) = prepare_sqlite_model_tool_batch(
        "run_sqlite_model_tool_all_settled",
        model_tool_queue_binding(),
    )
    .await;
    let mut claims = repository
        .claim_model_tool_calls("tool-worker", 60, 2, 2)
        .await
        .unwrap();
    claims.sort_by_key(|claim| claim.identity().call_index());
    assert_eq!(claims.len(), 2);
    let failing = claims.remove(0);
    let sibling = claims.remove(0);
    for claim in [&failing, &sibling] {
        assert!(matches!(
            repository
                .mark_model_tool_call_started(claim)
                .await
                .unwrap(),
            ModelToolTaskTransitionOutcome::Committed(())
        ));
    }
    let failure = ModelToolTaskOutcome::failed(
        ModelToolFailureClass::Safe,
        "MODEL_TOOL_EXECUTION_FAILED",
        false,
        EffectEvidence::Started,
    )
    .unwrap();
    let receipt = match repository
        .commit_model_tool_call_outcome(&failing, &failure)
        .await
        .unwrap()
    {
        ModelToolTaskTransitionOutcome::Committed(receipt) => receipt,
        _ => panic!("failure did not commit"),
    };
    assert_eq!(
        receipt.continuation_status(),
        ModelToolContinuationStatus::ReadyFailed
    );
    assert!(matches!(
        repository
            .commit_model_tool_call_outcome(&failing, &failure)
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::ExactReplay(replay)
            if replay.continuation_status() == ModelToolContinuationStatus::ReadyFailed
    ));
    assert!(matches!(
        repository
            .commit_model_tool_call_outcome(
                &sibling,
                &ModelToolTaskOutcome::succeeded(json!({"value": "late"})).unwrap(),
            )
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::StaleLease
    ));
    let rows = sqlx::query_as::<_, (String, String, String)>(
        "SELECT call_status,effect_evidence,failure_class
         FROM model_tool_calls WHERE run_id=? ORDER BY call_index",
    )
    .bind(run_id.as_str())
    .fetch_all(&control)
    .await
    .unwrap();
    assert_eq!(
        rows,
        vec![
            ("failed".into(), "started".into(), "safe".into()),
            (
                "failed".into(),
                "unknown".into(),
                "effect_outcome_unknown".into(),
            ),
        ],
        "the batch must be all-terminal before its parent is runnable",
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT continuation_status FROM model_tool_call_batches WHERE run_id=?",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "ready_failed"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT task_state FROM task_outbox WHERE run_id=?")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        "pending"
    );
    assert!(repository
        .claim_model_tool_calls("tool-worker", 60, 2, 2)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn sqlite_model_tool_retry_uses_database_time_and_non_idempotent_lease_loss_fails_closed() {
    let (_directory, repository, control, run_id, _parent) =
        prepare_sqlite_model_tool_batch("run_sqlite_model_tool_retry", model_tool_queue_binding())
            .await;
    let weather = repository
        .claim_model_tool_calls("tool-worker", 60, 1, 2)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(weather.identity().call_index(), 0);
    assert!(matches!(
        repository
            .mark_model_tool_call_started(&weather)
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::Committed(())
    ));
    let retryable = ModelToolTaskOutcome::failed(
        ModelToolFailureClass::Infrastructure,
        "MODEL_TOOL_UPSTREAM_UNAVAILABLE",
        true,
        EffectEvidence::Started,
    )
    .unwrap();
    let retry_receipt = match repository
        .commit_model_tool_call_outcome(&weather, &retryable)
        .await
        .unwrap()
    {
        ModelToolTaskTransitionOutcome::Committed(receipt) => receipt,
        _ => panic!("retry was not scheduled"),
    };
    assert_eq!(
        retry_receipt.disposition(),
        ModelToolTaskDisposition::RetryScheduled
    );
    assert_eq!(
        retry_receipt.continuation_status(),
        ModelToolContinuationStatus::WaitingTools
    );
    assert!(retry_receipt.next_available_at().is_some());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT julianday(available_at)>julianday('now')
             FROM model_tool_calls WHERE run_id=? AND call_index=0",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1,
        "retry time must be an absolute timestamp allocated by the database clock",
    );
    assert!(matches!(
        repository
            .commit_model_tool_call_outcome(&weather, &retryable)
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::ExactReplay(receipt)
            if receipt.disposition() == ModelToolTaskDisposition::RetryScheduled
    ));

    let clock = repository
        .claim_model_tool_calls("tool-worker", 60, 1, 2)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(clock.identity().call_index(), 1);
    assert!(matches!(
        repository
            .mark_model_tool_call_started(&clock)
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::Committed(())
    ));
    sqlx::query(
        "UPDATE model_tool_calls SET claim_expires_at=datetime('now','-1 second')
         WHERE run_id=? AND call_index=1",
    )
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .unwrap();
    assert!(repository
        .claim_model_tool_calls("recovery-worker", 60, 2, 2)
        .await
        .unwrap()
        .is_empty());
    let rows = sqlx::query_as::<_, (i64, String, String, String)>(
        "SELECT call_index,call_status,effect_evidence,failure_class
         FROM model_tool_calls WHERE run_id=? ORDER BY call_index",
    )
    .bind(run_id.as_str())
    .fetch_all(&control)
    .await
    .unwrap();
    assert_eq!(
        rows,
        vec![
            (0, "cancelled".into(), "not_started".into(), "safe".into()),
            (
                1,
                "failed".into(),
                "unknown".into(),
                "effect_outcome_unknown".into(),
            ),
        ]
    );
    assert!(matches!(
        repository
            .commit_model_tool_call_outcome(
                &clock,
                &ModelToolTaskOutcome::succeeded(json!({"value": "late"})).unwrap(),
            )
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::StaleLease
    ));
}

#[tokio::test]
async fn sqlite_model_tool_success_validates_frozen_public_projection_inside_the_commit_fence() {
    let (_directory, repository, control, run_id, _parent) = prepare_sqlite_model_tool_batch(
        "run_sqlite_model_tool_public_result",
        public_model_tool_queue_binding(),
    )
    .await;
    let mut claims = repository
        .claim_model_tool_calls("tool-worker", 60, 2, 2)
        .await
        .unwrap();
    claims.sort_by_key(|claim| claim.identity().call_index());
    let weather = claims.remove(0);
    let clock = claims.remove(0);
    assert_eq!(
        weather.identity().public_arguments_jcs(),
        Some(r#"{"city":"Shanghai"}"#)
    );
    assert!(weather.identity().public_item().is_some());
    assert!(clock.identity().public_item().is_none());
    assert!(matches!(
        repository
            .mark_model_tool_call_started(&weather)
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::Committed(())
    ));
    let unauthorized_public_result =
        ModelToolTaskOutcome::succeeded(json!({"value": "executor-private"})).unwrap();
    assert!(repository
        .commit_model_tool_call_outcome(&weather, &unauthorized_public_result)
        .await
        .is_err());
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT call_status FROM model_tool_calls WHERE run_id=? AND call_index=0",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "running",
        "a result that cannot form bounded typed public content must not be marked succeeded",
    );
    assert!(matches!(
        repository
            .commit_model_tool_call_outcome(
                &weather,
                &ModelToolTaskOutcome::succeeded(json!({"value": "safe"})).unwrap(),
            )
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::Committed(receipt)
            if receipt.continuation_status() == ModelToolContinuationStatus::WaitingTools
    ));
    assert!(matches!(
        repository
            .mark_model_tool_call_started(&clock)
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::Committed(())
    ));
    assert!(matches!(
        repository
            .commit_model_tool_call_outcome(
                &clock,
                &ModelToolTaskOutcome::succeeded(json!({"value": "12:00"})).unwrap(),
            )
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::Committed(receipt)
            if receipt.continuation_status() == ModelToolContinuationStatus::ReadyContinue
    ));
    let public_call = sqlx::query_as::<_, (i64, String, String)>(
        "SELECT seal_index,item_status,safe_item FROM response_public_items
         WHERE run_id=? AND item_kind='function_call'",
    )
    .bind(run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(
        public_call.0,
        super::FUNCTION_CALL_COMPLETE_SEAL_INDEX as i64
    );
    assert_eq!(public_call.1, "completed");
    let public_call: serde_json::Value = serde_json::from_str(&public_call.2).unwrap();
    assert_eq!(public_call["type"], "function_call");
    assert_eq!(public_call["arguments"], r#"{"city":"Shanghai"}"#);
    assert!(public_call.get("result").is_none());
}

#[tokio::test]
async fn sqlite_private_and_field_projected_arguments_allocate_no_standard_function_item() {
    for (run_name, binding) in [
        (
            "run_sqlite_private_function_item_absent",
            model_tool_queue_binding(),
        ),
        (
            "run_sqlite_field_function_item_absent",
            field_public_model_tool_queue_binding(),
        ),
    ] {
        let (_directory, _repository, control, run_id, _parent) =
            prepare_sqlite_model_tool_batch(run_name, binding).await;
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM response_public_items
                 WHERE run_id=? AND item_kind='function_call'",
            )
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
            0,
            "private and field-list arguments use workflow projection only",
        );
    }
}

#[tokio::test]
async fn sqlite_model_tool_public_artifact_ref_requires_exact_referenced_row_before_success() {
    let (_directory, repository, control, run_id, _parent) = prepare_sqlite_model_tool_batch(
        "run_sqlite_model_tool_public_artifact_ref",
        artifact_public_model_tool_queue_binding(),
    )
    .await;
    let weather = repository
        .claim_model_tool_calls("tool-worker", 60, 1, 2)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(weather.identity().call_index(), 0);
    assert!(matches!(
        repository
            .mark_model_tool_call_started(&weather)
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::Committed(())
    ));
    let artifact_id = "artifact_public_image";
    let content_hash = format!("sha256:{}", "c".repeat(64));
    let outcome = ModelToolTaskOutcome::succeeded(json!({
        "type": "output_image",
        "artifact": {
            "artifact_id": artifact_id,
            "content_hash": content_hash,
            "size_bytes": 12,
            "media_type": "image/png"
        }
    }))
    .unwrap();
    assert!(repository
        .commit_model_tool_call_outcome(&weather, &outcome)
        .await
        .is_err());
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT call_status FROM model_tool_calls WHERE run_id=? AND call_index=0",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "running",
        "an unproven public ArtifactRef must not cross the durable success fence",
    );

    sqlx::query(
        "INSERT INTO artifacts(
            run_id,artifact_id,content_hash,size_bytes,media_type,storage_uri,
            artifact_state,verified_at,referenced_at,created_at
         ) VALUES(?,?,?,?,?,'artifact://fixture','referenced',
                  CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
    )
    .bind(run_id.as_str())
    .bind(artifact_id)
    .bind(&content_hash)
    .bind(12_i64)
    .bind("image/png")
    .execute(&control)
    .await
    .unwrap();
    assert!(matches!(
        repository
            .commit_model_tool_call_outcome(&weather, &outcome)
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::Committed(receipt)
            if receipt.continuation_status() == ModelToolContinuationStatus::WaitingTools
    ));
}

#[tokio::test]
async fn sqlite_model_tool_activation_freezes_parent_operation_deadline_exactly_once() {
    let (_directory, repository, control, run_id, parent) = prepare_sqlite_model_tool_batch(
        "run_sqlite_model_tool_parent_deadline_freeze",
        model_tool_queue_binding(),
    )
    .await;
    let (started_at, deadline) = sqlx::query_as::<_, (String, String)>(
        "SELECT a.started_at,b.parent_operation_deadline
         FROM node_attempts a
         JOIN model_tool_call_batches b ON b.run_id=a.run_id
           AND b.activation_id=a.activation_id AND b.attempt_no=a.attempt_no
         WHERE b.run_id=? AND b.model_call_no=1",
    )
    .bind(run_id.as_str())
    .fetch_one(&control)
    .await
    .unwrap();
    let parse_timestamp = |value: &str| {
        DateTime::parse_from_rfc3339(value)
            .map(|value| value.with_timezone(&Utc))
            .or_else(|_| {
                chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
                    .map(|value| value.and_utc())
            })
            .unwrap()
    };
    let expected = parse_timestamp(&started_at)
        .checked_add_signed(Duration::milliseconds(
            i64::try_from(parent.envelope().request().effect_policy().timeout_ms()).unwrap(),
        ))
        .unwrap();
    assert_eq!(
        parse_timestamp(&deadline),
        expected,
        "the tool batch inherits the original LLM attempt budget",
    );
    assert!(matches!(
        repository
            .activate_model_tool_call_batch(&parent, 1)
            .await
            .unwrap(),
        ModelToolBatchActivationOutcome::ExactReplay(_)
    ));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT parent_operation_deadline FROM model_tool_call_batches WHERE run_id=?",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        deadline,
        "activation replay must never refresh the frozen deadline",
    );
}

#[tokio::test]
async fn sqlite_model_tool_activation_replay_converges_elapsed_and_legacy_null_deadlines() {
    for (run_name, legacy_null) in [
        ("run_sqlite_model_tool_replay_elapsed_deadline", false),
        ("run_sqlite_model_tool_replay_null_deadline", true),
    ] {
        let (_directory, repository, control, run_id, parent) =
            prepare_sqlite_model_tool_batch(run_name, model_tool_queue_binding()).await;
        if legacy_null {
            sqlx::query(
                "UPDATE model_tool_call_batches SET parent_operation_deadline=NULL WHERE run_id=?",
            )
            .bind(run_id.as_str())
            .execute(&control)
            .await
            .unwrap();
        } else {
            sqlx::query(
                "UPDATE model_tool_call_batches
                 SET parent_operation_deadline=datetime('now','-1 second') WHERE run_id=?",
            )
            .bind(run_id.as_str())
            .execute(&control)
            .await
            .unwrap();
        }
        assert!(matches!(
            repository
                .activate_model_tool_call_batch(&parent, 1)
                .await
                .unwrap(),
            ModelToolBatchActivationOutcome::ExactReplay(_)
        ));
        assert_eq!(
            sqlx::query_as::<_, (String, String)>(
                "SELECT execution_status,continuation_status
                 FROM model_tool_call_batches WHERE run_id=?",
            )
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
            ("cancelled".into(), "ready_cancelled".into()),
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM model_tool_calls
                 WHERE run_id=? AND call_status='cancelled'
                   AND effect_evidence='not_started' AND failure_class='safe'
                   AND failure_code='MODEL_TOOL_PARENT_DEADLINE_EXCEEDED'",
            )
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
            2,
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT task_state FROM task_outbox WHERE run_id=?",)
                .bind(run_id.as_str())
                .fetch_one(&control)
                .await
                .unwrap(),
            "pending",
            "deadline convergence must wake the suspended parent atomically",
        );
    }
}

#[tokio::test]
async fn sqlite_model_tool_claim_converges_elapsed_and_legacy_null_parent_deadlines() {
    for (run_name, legacy_null) in [
        ("run_sqlite_model_tool_claim_elapsed_deadline", false),
        ("run_sqlite_model_tool_claim_null_deadline", true),
    ] {
        let (_directory, repository, control, run_id, _parent) =
            prepare_sqlite_model_tool_batch(run_name, model_tool_queue_binding()).await;
        if legacy_null {
            sqlx::query(
                "UPDATE model_tool_call_batches SET parent_operation_deadline=NULL WHERE run_id=?",
            )
            .bind(run_id.as_str())
            .execute(&control)
            .await
            .unwrap();
        } else {
            sqlx::query(
                "UPDATE model_tool_call_batches
                 SET parent_operation_deadline=datetime('now','-1 second') WHERE run_id=?",
            )
            .bind(run_id.as_str())
            .execute(&control)
            .await
            .unwrap();
        }
        assert!(repository
            .claim_model_tool_calls("tool-worker", 60, 2, 2)
            .await
            .unwrap()
            .is_empty());
        let rows = sqlx::query_as::<_, (String, String, String, i64)>(
            "SELECT call_status,effect_evidence,failure_class,lease_epoch
             FROM model_tool_calls WHERE run_id=? ORDER BY call_index",
        )
        .bind(run_id.as_str())
        .fetch_all(&control)
        .await
        .unwrap();
        assert_eq!(
            rows,
            vec![
                ("cancelled".into(), "not_started".into(), "safe".into(), 2),
                ("cancelled".into(), "not_started".into(), "safe".into(), 2),
            ],
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT task_state FROM task_outbox WHERE run_id=?",)
                .bind(run_id.as_str())
                .fetch_one(&control)
                .await
                .unwrap(),
            "pending",
        );
    }
}

#[tokio::test]
async fn sqlite_model_tool_mark_and_heartbeat_enforce_parent_deadline_before_action() {
    let (_directory, repository, control, run_id, _parent) = prepare_sqlite_model_tool_batch(
        "run_sqlite_model_tool_mark_parent_deadline",
        model_tool_queue_binding(),
    )
    .await;
    let claim = repository
        .claim_model_tool_calls("tool-worker", 60, 1, 2)
        .await
        .unwrap()
        .pop()
        .unwrap();
    sqlx::query(
        "UPDATE model_tool_call_batches
         SET parent_operation_deadline=datetime('now','-1 second') WHERE run_id=?",
    )
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .unwrap();
    assert!(matches!(
        repository
            .mark_model_tool_call_started(&claim)
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::StaleLease
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM model_tool_calls
             WHERE run_id=? AND call_status='cancelled' AND effect_evidence='not_started'",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        2,
    );

    let (_directory, repository, control, run_id, _parent) = prepare_sqlite_model_tool_batch(
        "run_sqlite_model_tool_heartbeat_parent_deadline",
        model_tool_queue_binding(),
    )
    .await;
    let mut claims = repository
        .claim_model_tool_calls("tool-worker", 60, 2, 2)
        .await
        .unwrap();
    claims.sort_by_key(|claim| claim.identity().call_index());
    let running = claims.remove(0);
    assert!(matches!(
        repository
            .mark_model_tool_call_started(&running)
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::Committed(())
    ));
    sqlx::query(
        "UPDATE model_tool_call_batches
         SET parent_operation_deadline=datetime('now','-1 second') WHERE run_id=?",
    )
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .unwrap();
    assert!(matches!(
        repository
            .heartbeat_model_tool_call(&running, 60)
            .await
            .unwrap(),
        ModelToolTaskHeartbeatOutcome::StaleLease
    ));
    assert_eq!(
        sqlx::query_as::<_, (String, String, String)>(
            "SELECT call_status,effect_evidence,failure_class
             FROM model_tool_calls WHERE run_id=? AND call_index=0",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        (
            "failed".into(),
            "unknown".into(),
            "effect_outcome_unknown".into()
        ),
    );
    assert_eq!(
        sqlx::query_as::<_, (String, String, String)>(
            "SELECT call_status,effect_evidence,failure_class
             FROM model_tool_calls WHERE run_id=? AND call_index=1",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        ("cancelled".into(), "not_started".into(), "safe".into()),
    );
}

#[tokio::test]
async fn sqlite_model_tool_deadline_preserves_success_and_fences_late_commit() {
    let (_directory, repository, control, run_id, _parent) = prepare_sqlite_model_tool_batch(
        "run_sqlite_model_tool_late_commit_parent_deadline",
        model_tool_queue_binding(),
    )
    .await;
    let mut claims = repository
        .claim_model_tool_calls("tool-worker", 60, 2, 2)
        .await
        .unwrap();
    claims.sort_by_key(|claim| claim.identity().call_index());
    let weather = claims.remove(0);
    let clock = claims.remove(0);
    for claim in [&weather, &clock] {
        assert!(matches!(
            repository
                .mark_model_tool_call_started(claim)
                .await
                .unwrap(),
            ModelToolTaskTransitionOutcome::Committed(())
        ));
    }
    let weather_outcome = ModelToolTaskOutcome::succeeded(json!({"value": "sunny"})).unwrap();
    assert!(matches!(
        repository
            .commit_model_tool_call_outcome(&weather, &weather_outcome)
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::Committed(receipt)
            if receipt.continuation_status() == ModelToolContinuationStatus::WaitingTools
    ));
    sqlx::query(
        "UPDATE model_tool_call_batches
         SET parent_operation_deadline=datetime('now','-1 second') WHERE run_id=?",
    )
    .bind(run_id.as_str())
    .execute(&control)
    .await
    .unwrap();
    assert!(matches!(
        repository
            .commit_model_tool_call_outcome(
                &clock,
                &ModelToolTaskOutcome::succeeded(json!({"value": "late"})).unwrap(),
            )
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::StaleLease
    ));
    assert!(matches!(
        repository
            .heartbeat_model_tool_call(&clock, 60)
            .await
            .unwrap(),
        ModelToolTaskHeartbeatOutcome::StaleLease
    ));
    assert!(matches!(
        repository
            .commit_model_tool_call_outcome(&weather, &weather_outcome)
            .await
            .unwrap(),
        ModelToolTaskTransitionOutcome::ExactReplay(receipt)
            if receipt.continuation_status() == ModelToolContinuationStatus::ReadyFailed
    ));
    let rows = sqlx::query_as::<_, (i64, String, String, String, Option<String>)>(
        "SELECT call_index,call_status,effect_evidence,
                COALESCE(failure_class,''),result_json
         FROM model_tool_calls WHERE run_id=? ORDER BY call_index",
    )
    .bind(run_id.as_str())
    .fetch_all(&control)
    .await
    .unwrap();
    assert_eq!(
        rows,
        vec![
            (
                0,
                "succeeded".into(),
                "committed".into(),
                "".into(),
                Some(r#"{"value":"sunny"}"#.into()),
            ),
            (
                1,
                "failed".into(),
                "unknown".into(),
                "effect_outcome_unknown".into(),
                None,
            ),
        ],
    );
    assert_eq!(
        sqlx::query_as::<_, (String, String)>(
            "SELECT continuation_status,execution_status
             FROM model_tool_call_batches WHERE run_id=?",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        ("ready_failed".into(), "failed".into()),
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT task_state FROM task_outbox WHERE run_id=?")
            .bind(run_id.as_str())
            .fetch_one(&control)
            .await
            .unwrap(),
        "pending",
    );
}

#[tokio::test]
async fn postgres_model_call_authority_matches_sqlite_contract() {
    let Ok(database_url) = std::env::var("V3_TEST_POSTGRES_URL") else {
        return;
    };
    let schema = format!("scheduler_model_call_v3_{}", Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let separator = if database_url.contains('?') { '&' } else { '?' };
    let scoped_url = format!("{database_url}{separator}options=-csearch_path%3D{schema}");
    let control = PgPoolOptions::new()
        .max_connections(4)
        .connect(&scoped_url)
        .await
        .unwrap();
    let repository = PostgresDurableRepository::connect(&scoped_url)
        .await
        .unwrap();
    repository.initialize_schema().await.unwrap();
    let (plan, descriptors, versioned) = model_call_fixture();
    let subflows = SubflowContractRegistry::new();
    let linked = LinkedPlan::link(&plan, &descriptors, &subflows).unwrap();
    assert_eq!(
        repository.install_versioned_plan(&versioned).await.unwrap(),
        PlanInstallOutcome::Installed
    );

    let run_id = RunId::new("run_pg_model_call_authority").unwrap();
    let (claim, fence) =
        prepare_postgres_task(&repository, &control, &versioned, &linked, &run_id).await;
    exercise_model_call_authority(&repository, &claim, &run_id).await;
    drive_until_response_snapshot(&repository, &linked, &fence, &run_id).await;
    assert_model_call_snapshot(&repository, &run_id).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM model_call_usage WHERE run_id=$1",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        2,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM response_public_items WHERE run_id=$1",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM model_tool_call_batches WHERE run_id=$1",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        1,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM model_tool_calls WHERE run_id=$1",
        )
        .bind(run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        2,
    );

    let stale_run_id = RunId::new("run_pg_model_call_stale").unwrap();
    let (stale, _) =
        prepare_postgres_task(&repository, &control, &versioned, &linked, &stale_run_id).await;
    let expired_at =
        sqlx::query_scalar::<_, DateTime<Utc>>("SELECT clock_timestamp()-INTERVAL '1 second'")
            .fetch_one(&control)
            .await
            .unwrap();
    sqlx::query("UPDATE task_outbox SET claim_expires_at=$1 WHERE run_id=$2")
        .bind(expired_at)
        .bind(stale_run_id.as_str())
        .execute(&control)
        .await
        .unwrap();
    sqlx::query("UPDATE node_attempts SET lease_expires_at=$1 WHERE run_id=$2")
        .bind(expired_at)
        .bind(stale_run_id.as_str())
        .execute(&control)
        .await
        .unwrap();
    assert_eq!(
        repository
            .reserve_model_call(&stale, 1, true)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::StaleLease,
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT FROM model_call_usage WHERE run_id=$1",
        )
        .bind(stale_run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        0,
    );

    let queue_run_id = RunId::new("run_pg_model_tool_queue").unwrap();
    let (parent, _) =
        prepare_postgres_task(&repository, &control, &versioned, &linked, &queue_run_id).await;
    assert!(matches!(
        repository
            .reserve_model_call(&parent, 1, true)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    assert!(matches!(
        repository
            .checkpoint_model_tool_call_batch(&parent, &tool_call_checkpoint(1, 10, "Shanghai"),)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    assert!(matches!(
        repository
            .activate_model_tool_call_batch(&parent, 1)
            .await
            .unwrap(),
        ModelToolBatchActivationOutcome::Activated(activation)
            if activation.tasks().len() == 2
    ));
    let expired_at =
        sqlx::query_scalar::<_, DateTime<Utc>>("SELECT clock_timestamp()-INTERVAL '1 second'")
            .fetch_one(&control)
            .await
            .unwrap();
    sqlx::query("UPDATE task_outbox SET claim_expires_at=$1 WHERE run_id=$2")
        .bind(expired_at)
        .bind(queue_run_id.as_str())
        .execute(&control)
        .await
        .unwrap();
    sqlx::query("UPDATE node_attempts SET lease_expires_at=$1 WHERE run_id=$2")
        .bind(expired_at)
        .bind(queue_run_id.as_str())
        .execute(&control)
        .await
        .unwrap();
    assert!(repository
        .claim_scheduler_tasks("pg-must-not-steal-waiting-parent", 60, 16)
        .await
        .unwrap()
        .iter()
        .all(|claim| claim.run_id() != &queue_run_id));
    let mut tool_claims = repository
        .claim_model_tool_calls("pg-tool-worker", 60, 2, 2)
        .await
        .unwrap();
    tool_claims.sort_by_key(|claim| claim.identity().call_index());
    assert_eq!(tool_claims.len(), 2);
    for (index, claim) in tool_claims.iter().enumerate() {
        assert!(matches!(
            repository
                .mark_model_tool_call_started(claim)
                .await
                .unwrap(),
            ModelToolTaskTransitionOutcome::Committed(())
        ));
        let result = ModelToolTaskOutcome::succeeded(json!({
            "value": if index == 0 { "sunny" } else { "12:00" }
        }))
        .unwrap();
        let expected = if index == 0 {
            ModelToolContinuationStatus::WaitingTools
        } else {
            ModelToolContinuationStatus::ReadyContinue
        };
        assert!(matches!(
            repository
                .commit_model_tool_call_outcome(claim, &result)
                .await
                .unwrap(),
            ModelToolTaskTransitionOutcome::Committed(receipt)
                if receipt.continuation_status() == expected
        ));
    }
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT continuation_status FROM model_tool_call_batches WHERE run_id=$1",
        )
        .bind(queue_run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        "ready_continue"
    );

    let checkpoint_run_id = RunId::new("run_pg_checkpointed_parent_reclaim").unwrap();
    let (checkpoint_parent, _) = prepare_postgres_task(
        &repository,
        &control,
        &versioned,
        &linked,
        &checkpoint_run_id,
    )
    .await;
    assert!(matches!(
        repository
            .reserve_model_call(&checkpoint_parent, 1, true)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    assert!(matches!(
        repository
            .checkpoint_model_tool_call_batch(
                &checkpoint_parent,
                &tool_call_checkpoint(1, 10, "Shanghai"),
            )
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed { .. }
    ));
    assert_eq!(
        sqlx::query_as::<_, (String, String, i64, i64)>(
            "SELECT execution_status,continuation_status,
                    (SELECT COUNT(*)::BIGINT FROM model_tool_calls c
                     WHERE c.run_id=b.run_id AND c.call_status='pending'),
                    (SELECT COUNT(*)::BIGINT FROM model_tool_calls c
                     WHERE c.run_id=b.run_id
                       AND (c.tool_task_id IS NOT NULL OR c.effect_id IS NOT NULL
                            OR c.projection_version<>0))
             FROM model_tool_call_batches b WHERE run_id=$1",
        )
        .bind(checkpoint_run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        ("checkpointed".into(), "checkpointed".into(), 2, 0),
        "PostgreSQL must persist the same non-executable handoff intent as SQLite",
    );
    assert!(
        repository
            .claim_model_tool_calls("pg-must-not-claim-checkpoint-intent", 60, 8, 8)
            .await
            .unwrap()
            .is_empty(),
        "PostgreSQL checkpoint rows cannot become executable before materialization",
    );
    let expired_at =
        sqlx::query_scalar::<_, DateTime<Utc>>("SELECT clock_timestamp()-INTERVAL '1 second'")
            .fetch_one(&control)
            .await
            .unwrap();
    sqlx::query("UPDATE task_outbox SET claim_expires_at=$1 WHERE run_id=$2")
        .bind(expired_at)
        .bind(checkpoint_run_id.as_str())
        .execute(&control)
        .await
        .unwrap();
    sqlx::query("UPDATE node_attempts SET lease_expires_at=$1 WHERE run_id=$2")
        .bind(expired_at)
        .bind(checkpoint_run_id.as_str())
        .execute(&control)
        .await
        .unwrap();
    let checkpoint_recovered = repository
        .claim_scheduler_tasks("pg-checkpoint-activation-worker", 60, 16)
        .await
        .unwrap()
        .into_iter()
        .find(|claim| claim.run_id() == &checkpoint_run_id)
        .expect("PostgreSQL must reclaim the latest checkpointed parent");
    assert_eq!(checkpoint_recovered.mode(), SchedulerTaskClaimMode::Execute);
    assert!(matches!(
        repository
            .load_model_tool_parent_resume(&checkpoint_recovered)
            .await
            .unwrap(),
        SchedulerTaskCommitOutcome::Committed {
            result: Some(ModelToolParentResume::ActivateCheckpointed {
                model_call_no: 1,
                ..
            })
        }
    ));
    let checkpoint_activation = match repository
        .activate_model_tool_call_batch(&checkpoint_recovered, 1)
        .await
        .unwrap()
    {
        ModelToolBatchActivationOutcome::Activated(activation) => activation,
        _ => panic!("PostgreSQL recovered checkpoint did not activate"),
    };
    assert_eq!(checkpoint_activation.tasks().len(), 2);
    assert_eq!(
        sqlx::query_as::<_, (String, String, i64)>(
            "SELECT execution_status,continuation_status,
                    (SELECT COUNT(*)::BIGINT FROM model_tool_calls c
                     WHERE c.run_id=b.run_id AND c.call_status='pending'
                       AND c.tool_task_id IS NOT NULL AND c.effect_id IS NOT NULL
                       AND c.projection_version=1)
             FROM model_tool_call_batches b WHERE run_id=$1",
        )
        .bind(checkpoint_run_id.as_str())
        .fetch_one(&control)
        .await
        .unwrap(),
        ("active".into(), "waiting_tools".into(), 2),
        "PostgreSQL must materialize the complete child set and parent barrier in one transaction",
    );
    assert!(matches!(
        repository
            .activate_model_tool_call_batch(&checkpoint_recovered, 1)
            .await
            .unwrap(),
        ModelToolBatchActivationOutcome::ExactReplay(replay) if replay == checkpoint_activation
    ));
    repository
        .load_scheduler_facts(&checkpoint_run_id)
        .await
        .expect("PostgreSQL recovery must accept a legitimate continuation owner change");

    sqlx::query(
        "UPDATE scheduler_checkpoints
         SET fact_payload=jsonb_set(
             fact_payload,'{claimed_by}',to_jsonb('forged-history-owner'::TEXT)
         )
         WHERE run_id=$1 AND checkpoint_kind='task_started'",
    )
    .bind(checkpoint_run_id.as_str())
    .execute(&control)
    .await
    .unwrap();
    assert!(
        repository
            .load_scheduler_facts(&checkpoint_run_id)
            .await
            .is_err(),
        "PostgreSQL must retain the immutable task-start content-hash boundary"
    );

    drop(repository);
    drop(control);
    sqlx::query(AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
}
