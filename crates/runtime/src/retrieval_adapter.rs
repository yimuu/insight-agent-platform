//! Production worker adapter for first-class Retrieval leaves.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use serde_json::{Map, Value};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use insight_dsl::CompileError;
use insight_engine::{
    execution::{stop_pair, ExecutionControl, RunError, RunErrorKind, StopReason},
    plan::{DescriptorValue, VersionTag},
    retrieval::{deterministic_retrieval_id, FrozenRetrievalTarget, RetrievalCompletion},
    run_stream::LiveRunStreamBroker,
    worker::{
        adapter as worker_adapter, LeafTaskExecutor, TaskExecutionRequest, TaskExecutionResult,
        WorkerExecutionContext, WorkerExecutorRegistry, WorkerFailure, WorkerFailureClass,
        WorkerRuntimeServices,
    },
    EffectEvidence, RuntimeValue, SchedulerTaskKind, TaskOutputContract, WorkerEffectClass,
    WorkerEffectPolicy,
};
use insight_resources::{
    actions::ActionRegistry,
    models::ModelRegistry,
    retrievals::{RetrievalContext, RetrievalRegistry},
};

use crate::leaf_adapters::{
    production_worker_registry, production_worker_registry_with_live_run_stream,
};

const RETRIEVAL_DESCRIPTOR_VERSION: &str = "1";
const RETRIEVAL_DESCRIPTOR_INVALID: &str = "RETRIEVAL_DESCRIPTOR_INVALID";
const RETRIEVAL_BINDING_INVALID: &str = "RETRIEVAL_BINDING_INVALID";
const RETRIEVAL_EXECUTION_FAILED: &str = "RETRIEVAL_EXECUTION_FAILED";
const RETRIEVAL_PUBLIC_RESULT_INVALID: &str = "RETRIEVAL_PUBLIC_RESULT_INVALID";
const WORKER_CANCELLED: &str = "WORKER_CANCELLED";
const WORKER_DEADLINE_EXCEEDED: &str = "WORKER_DEADLINE_EXCEEDED";

#[derive(Clone)]
pub struct RetrievalTaskExecutor {
    retrievals: RetrievalRegistry,
}

impl RetrievalTaskExecutor {
    pub fn new(retrievals: RetrievalRegistry) -> Self {
        Self { retrievals }
    }
}

#[async_trait]
impl LeafTaskExecutor for RetrievalTaskExecutor {
    async fn execute(
        &self,
        context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        self.execute_with_runtime_services(
            context,
            request,
            &WorkerRuntimeServices::default(),
            cancellation,
        )
        .await
    }

    async fn execute_with_runtime_services(
        &self,
        context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        services: &WorkerRuntimeServices,
        cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        if request.task_kind() != SchedulerTaskKind::Retrieval
            || request.descriptor_version().as_str() != RETRIEVAL_DESCRIPTOR_VERSION
        {
            return Err(invariant(RETRIEVAL_DESCRIPTOR_INVALID));
        }
        let target = FrozenRetrievalTarget::from_deployment_binding(request.deployment_binding())
            .map_err(|_| invariant(RETRIEVAL_BINDING_INVALID))?;
        let registered = self
            .retrievals
            .resolve_frozen(
                request.implementation(),
                &target.resource_version().to_string(),
                target.descriptor_hash(),
            )
            .map_err(|_| invariant(RETRIEVAL_DESCRIPTOR_INVALID))?;
        target
            .validate_registered(registered.as_ref())
            .map_err(|_| invariant(RETRIEVAL_DESCRIPTOR_INVALID))?;
        target
            .validate_effect_policy(request.effect_policy())
            .map_err(|_| invariant(RETRIEVAL_DESCRIPTOR_INVALID))?;
        if request.implementation() != target.resource_id()
            || request.worker_version().as_str() != target.resource_version().to_string()
            || request.public_configuration().get("publish")
                != Some(&DescriptorValue::Boolean(target.publish()))
        {
            return Err(invariant(RETRIEVAL_BINDING_INVALID));
        }

        let bindings = RetrievalRuntimeBindings::new(request.public_configuration(), request)?;
        let input = request
            .public_configuration()
            .get("inputs")
            .map(|value| substitute_descriptor_value(value, &bindings))
            .transpose()?
            .unwrap_or_else(|| Value::Object(Map::new()));
        if !input.is_object() {
            return Err(invariant(RETRIEVAL_BINDING_INVALID));
        }
        registered
            .validate_input(&input)
            .map_err(|_| invariant(RETRIEVAL_BINDING_INVALID))?;
        require_live(context, &cancellation)?;

        let timeout = remaining(context)?;
        let (stop, signal) = stop_pair();
        let control = ExecutionControl::new(signal, timeout);
        let cancellation_for_retrieval = cancellation.clone();
        let stop_for_retrieval = stop.clone();
        let cancellation_bridge = tokio::spawn(async move {
            cancellation_for_retrieval.cancelled().await;
            stop_for_retrieval.request(StopReason::Cancelled);
        });
        let mut retrieval_context = RetrievalContext::for_durable_effect(
            request.run_id().as_str(),
            request.activation_id().as_str(),
            context.attempt_no().get(),
            request.effect_id().as_str(),
            control,
        );
        if let Some(permit) = worker_adapter::services_operation_permit(services) {
            retrieval_context = retrieval_context.with_operation_permit(permit.clone());
        }
        let call = registered.retrieve(input.clone(), retrieval_context);
        tokio::pin!(call);
        let execution = tokio::select! {
            value = &mut call => value.map_err(|error| map_retrieval_error(error, request.effect_policy())),
            _ = cancellation.cancelled() => {
                stop.request(StopReason::Cancelled);
                Err(cancelled_failure(request.effect_policy(), false))
            },
            _ = sleep(timeout) => {
                stop.request(StopReason::TimedOut);
                Err(cancelled_failure(request.effect_policy(), true))
            },
        };
        cancellation_bridge.abort();
        let execution = execution?;

        let retrieval_id = deterministic_retrieval_id(request.run_id(), request.activation_id());
        let (model_output, public_candidate, artifact_payloads) = execution.into_parts();
        let public = target
            .public_projection()
            .map_err(|_| invariant(RETRIEVAL_BINDING_INVALID))?
            .project_validated_completed(retrieval_id, &input, public_candidate.as_ref())
            .map_err(|_| invariant(RETRIEVAL_PUBLIC_RESULT_INVALID))?;
        let completion = RetrievalCompletion::new(public)
            .map_err(|_| invariant(RETRIEVAL_PUBLIC_RESULT_INVALID))?;
        let value =
            RuntimeValue::new(model_output).map_err(|_| invariant(RETRIEVAL_EXECUTION_FAILED))?;
        let output = only_output(request)?;
        if !value.matches(output.value_type()) {
            return Err(invariant(RETRIEVAL_EXECUTION_FAILED));
        }
        TaskExecutionResult::new(
            BTreeMap::from([(output.port_id().clone(), value)]),
            EffectEvidence::Committed,
        )
        .with_retrieval_completion(completion)
        .with_artifact_payloads(artifact_payloads)
        .map_err(|_| invariant(RETRIEVAL_EXECUTION_FAILED))
    }
}

pub fn production_worker_registry_with_retrievals(
    models: &ModelRegistry,
    actions: &ActionRegistry,
    retrievals: &RetrievalRegistry,
) -> Result<WorkerExecutorRegistry, CompileError> {
    let mut registry = production_worker_registry(models, actions)?;
    install_retrieval_workers(&mut registry, retrievals)?;
    Ok(registry)
}

pub fn production_worker_registry_with_live_run_stream_and_retrievals(
    models: &ModelRegistry,
    actions: &ActionRegistry,
    retrievals: &RetrievalRegistry,
    live_run_stream_broker: Arc<dyn LiveRunStreamBroker>,
) -> Result<WorkerExecutorRegistry, CompileError> {
    let mut registry =
        production_worker_registry_with_live_run_stream(models, actions, live_run_stream_broker)?;
    install_retrieval_workers(&mut registry, retrievals)?;
    Ok(registry)
}

pub fn install_retrieval_workers(
    registry: &mut WorkerExecutorRegistry,
    retrievals: &RetrievalRegistry,
) -> Result<(), CompileError> {
    let descriptor_version = VersionTag::new(RETRIEVAL_DESCRIPTOR_VERSION)
        .map_err(|error| CompileError::new("WORKER_REGISTRY_INVALID", error.to_string()))?;
    for retrieval_id in retrievals.names() {
        let retrieval = retrievals.resolve(&retrieval_id)?;
        registry
            .register(
                SchedulerTaskKind::Retrieval,
                retrieval_id,
                descriptor_version.clone(),
                VersionTag::new(retrieval.identity().version.to_string()).map_err(|error| {
                    CompileError::new("WORKER_REGISTRY_INVALID", error.to_string())
                })?,
                Arc::new(RetrievalTaskExecutor::new(retrievals.clone())),
            )
            .map_err(|code| CompileError::new(code, "failed to register Retrieval worker"))?;
    }
    registry
        .register_dynamic_retrieval_executor(Arc::new(RetrievalTaskExecutor::new(
            retrievals.clone(),
        )))
        .map_err(|code| CompileError::new(code, "failed to register dynamic Retrieval worker"))?;
    Ok(())
}

struct RetrievalRuntimeBindings {
    by_reference: BTreeMap<String, Value>,
}

impl RetrievalRuntimeBindings {
    fn new(
        configuration: &BTreeMap<String, DescriptorValue>,
        request: &TaskExecutionRequest,
    ) -> Result<Self, WorkerFailure> {
        let ports = request
            .inputs()
            .iter()
            .map(|input| (input.port_id().as_str(), input.value().value().clone()))
            .collect::<BTreeMap<_, _>>();
        let mappings = match configuration.get("runtime_bindings") {
            None => None,
            Some(DescriptorValue::Object(values)) => Some(values),
            Some(_) => return Err(invariant(RETRIEVAL_BINDING_INVALID)),
        };
        let mut optional_references = BTreeSet::new();
        match configuration.get("optional_runtime_bindings") {
            None => {}
            Some(DescriptorValue::Array(values)) => {
                for value in values {
                    let DescriptorValue::String(reference) = value else {
                        return Err(invariant(RETRIEVAL_BINDING_INVALID));
                    };
                    if !optional_references.insert(reference.clone()) {
                        return Err(invariant(RETRIEVAL_BINDING_INVALID));
                    }
                }
            }
            Some(_) => return Err(invariant(RETRIEVAL_BINDING_INVALID)),
        }
        if optional_references
            .iter()
            .any(|reference| mappings.is_none_or(|mappings| !mappings.contains_key(reference)))
        {
            return Err(invariant(RETRIEVAL_BINDING_INVALID));
        }
        let mut by_reference = BTreeMap::new();
        for (reference, port) in mappings.into_iter().flatten() {
            let DescriptorValue::String(port) = port else {
                return Err(invariant(RETRIEVAL_BINDING_INVALID));
            };
            let Some(value) = ports.get(port.as_str()).cloned() else {
                if optional_references.contains(reference) {
                    continue;
                }
                return Err(invariant(RETRIEVAL_BINDING_INVALID));
            };
            by_reference.insert(reference.clone(), value);
        }
        Ok(Self { by_reference })
    }

    fn resolve(&self, reference: &str) -> Result<Value, WorkerFailure> {
        if let Some(value) = self.by_reference.get(reference) {
            return Ok(value.clone());
        }
        let mut parts = reference.split('.');
        let root = parts
            .next()
            .ok_or_else(|| invariant(RETRIEVAL_BINDING_INVALID))?;
        let Some(mut value) = self.by_reference.get(root) else {
            return Err(invariant(RETRIEVAL_BINDING_INVALID));
        };
        for field in parts {
            value = value
                .as_object()
                .and_then(|object| object.get(field))
                .ok_or_else(|| invariant(RETRIEVAL_BINDING_INVALID))?;
        }
        Ok(value.clone())
    }
}

fn substitute_descriptor_value(
    value: &DescriptorValue,
    bindings: &RetrievalRuntimeBindings,
) -> Result<Value, WorkerFailure> {
    match value {
        DescriptorValue::String(value) if value.starts_with('$') => bindings.resolve(&value[1..]),
        DescriptorValue::Array(values) => values
            .iter()
            .map(|value| substitute_descriptor_value(value, bindings))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        DescriptorValue::Object(values) => values
            .iter()
            .map(|(name, value)| Ok((name.clone(), substitute_descriptor_value(value, bindings)?)))
            .collect::<Result<Map<_, _>, WorkerFailure>>()
            .map(Value::Object),
        value => descriptor_json(value),
    }
}

fn descriptor_json(value: &DescriptorValue) -> Result<Value, WorkerFailure> {
    Ok(match value {
        DescriptorValue::Null => Value::Null,
        DescriptorValue::Boolean(value) => Value::Bool(*value),
        DescriptorValue::Integer(value) => Value::Number((*value).into()),
        DescriptorValue::Number(value) => Value::Number(value.clone()),
        DescriptorValue::String(value) => Value::String(value.clone()),
        DescriptorValue::Array(values) => Value::Array(
            values
                .iter()
                .map(descriptor_json)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        DescriptorValue::Object(values) => Value::Object(
            values
                .iter()
                .map(|(name, value)| Ok((name.clone(), descriptor_json(value)?)))
                .collect::<Result<Map<_, _>, WorkerFailure>>()?,
        ),
    })
}

fn only_output(request: &TaskExecutionRequest) -> Result<&TaskOutputContract, WorkerFailure> {
    if request.outputs().len() != 1 || request.outputs()[0].name().as_str() != "result" {
        return Err(invariant(RETRIEVAL_DESCRIPTOR_INVALID));
    }
    Ok(&request.outputs()[0])
}

fn remaining(context: &WorkerExecutionContext) -> Result<Duration, WorkerFailure> {
    (context.deadline() - chrono::Utc::now())
        .to_std()
        .ok()
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| control(WORKER_DEADLINE_EXCEEDED))
}

fn require_live(
    context: &WorkerExecutionContext,
    cancellation: &CancellationToken,
) -> Result<(), WorkerFailure> {
    if cancellation.is_cancelled() {
        return Err(control(WORKER_CANCELLED));
    }
    remaining(context).map(|_| ())
}

fn map_retrieval_error(error: RunError, policy: &WorkerEffectPolicy) -> WorkerFailure {
    match error.kind() {
        RunErrorKind::Stop => cancelled_failure(policy, false),
        RunErrorKind::Timeout => cancelled_failure(policy, true),
        RunErrorKind::Operation | RunErrorKind::Infrastructure => {
            infrastructure(RETRIEVAL_EXECUTION_FAILED, true)
        }
    }
}

fn cancelled_failure(policy: &WorkerEffectPolicy, timed_out: bool) -> WorkerFailure {
    if policy.effect_class() == WorkerEffectClass::Mutating {
        WorkerFailure::new(
            WorkerFailureClass::EffectOutcomeUnknown,
            "RETRIEVAL_EFFECT_OUTCOME_UNKNOWN",
            true,
        )
        .expect("constant failure is valid")
    } else if timed_out {
        control(WORKER_DEADLINE_EXCEEDED)
    } else {
        control(WORKER_CANCELLED)
    }
}

fn control(code: &'static str) -> WorkerFailure {
    WorkerFailure::new(WorkerFailureClass::ControlTermination, code, false)
        .expect("constant failure is valid")
}

fn infrastructure(code: &'static str, retryable: bool) -> WorkerFailure {
    WorkerFailure::new(WorkerFailureClass::InfrastructureFailure, code, retryable)
        .expect("constant failure is valid")
}

fn invariant(code: &'static str) -> WorkerFailure {
    WorkerFailure::new(WorkerFailureClass::InvariantCorruption, code, false)
        .expect("constant failure is valid")
}
