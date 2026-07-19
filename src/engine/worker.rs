//! Closed worker-adapter boundary for durable leaf tasks.
//!
//! Workers execute one already-leased leaf effect. They never choose control
//! edges, mint Activations, or commit Run state. The durable repository owns
//! fencing and result publication; this module only resolves an immutable
//! implementation/version tuple and validates its typed output.

use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::{
    plan::{DataPortId, DescriptorValue, SecretRef, VersionTag},
    scheduler::{
        BoundTaskInput, RuntimeValue, SafeError, SchedulerAction, SchedulerIntent, SchedulerTaskId,
        SchedulerTaskKind, TaskAdmissionClass, TaskOutputContract,
    },
    ActivationId, AttemptNo, EffectEvidence, EffectId, LeaseEpoch, NodeId, RunId,
    WorkerEffectPolicy,
};

pub const WORKER_TASK_KIND_MISMATCH: &str = "ENGINE_WORKER_TASK_KIND_MISMATCH";
pub const WORKER_IMPLEMENTATION_NOT_FOUND: &str = "ENGINE_WORKER_IMPLEMENTATION_NOT_FOUND";
pub const WORKER_OUTPUT_INVALID: &str = "ENGINE_WORKER_OUTPUT_INVALID";
pub const WORKER_FAILURE_INVALID: &str = "ENGINE_WORKER_FAILURE_INVALID";
pub const WORKER_EXECUTION_CONTEXT_INVALID: &str = "ENGINE_WORKER_EXECUTION_CONTEXT_INVALID";

const MAX_FAILURE_CODE_BYTES: usize = 128;

/// Repository-minted authority that must accompany the immutable request all
/// the way into the exact worker/provider implementation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerExecutionContext {
    attempt_no: AttemptNo,
    lease_epoch: LeaseEpoch,
    fencing_token: String,
    deadline: DateTime<Utc>,
}

impl WorkerExecutionContext {
    pub fn new(
        attempt_no: AttemptNo,
        lease_epoch: LeaseEpoch,
        fencing_token: impl Into<String>,
        deadline: DateTime<Utc>,
    ) -> Result<Self, &'static str> {
        let fencing_token = fencing_token.into();
        if lease_epoch.get() < u64::from(attempt_no.get())
            || fencing_token.is_empty()
            || fencing_token.len() > 256
            || fencing_token.chars().any(char::is_control)
        {
            return Err(WORKER_EXECUTION_CONTEXT_INVALID);
        }
        Ok(Self {
            attempt_no,
            lease_epoch,
            fencing_token,
            deadline,
        })
    }

    pub fn attempt_no(&self) -> AttemptNo {
        self.attempt_no
    }

    pub fn lease_epoch(&self) -> LeaseEpoch {
        self.lease_epoch
    }

    pub fn fencing_token(&self) -> &str {
        &self.fencing_token
    }

    pub fn deadline(&self) -> DateTime<Utc> {
        self.deadline
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerFailureClass {
    SafeBusinessFailure,
    InfrastructureFailure,
    EffectOutcomeUnknown,
    ControlTermination,
    InvariantCorruption,
}

/// Body-free internal failure. Provider bodies, prompts, secrets and arbitrary
/// user values cannot be carried across this boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerFailure {
    class: WorkerFailureClass,
    code: String,
    retryable: bool,
    safe_error: Option<Box<SafeError>>,
}

impl WorkerFailure {
    pub fn new(
        class: WorkerFailureClass,
        code: impl Into<String>,
        retryable: bool,
    ) -> Result<Self, &'static str> {
        Self::build(class, code.into(), retryable, None)
    }

    pub fn safe_business(
        code: impl Into<String>,
        retryable: bool,
        safe_error: RuntimeValue,
    ) -> Result<Self, &'static str> {
        let code = code.into();
        let safe_error = SafeError::try_from(safe_error).map_err(|_| WORKER_FAILURE_INVALID)?;
        if code != safe_error.code() {
            return Err(WORKER_FAILURE_INVALID);
        }
        Self::build(
            WorkerFailureClass::SafeBusinessFailure,
            code,
            retryable,
            Some(Box::new(safe_error)),
        )
    }

    fn build(
        class: WorkerFailureClass,
        code: String,
        retryable: bool,
        safe_error: Option<Box<SafeError>>,
    ) -> Result<Self, &'static str> {
        if code.is_empty()
            || code.len() > MAX_FAILURE_CODE_BYTES
            || !code
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
            || code.as_bytes()[0].is_ascii_digit()
        {
            return Err(WORKER_FAILURE_INVALID);
        }
        if matches!(
            class,
            WorkerFailureClass::ControlTermination | WorkerFailureClass::InvariantCorruption
        ) && retryable
        {
            return Err(WORKER_FAILURE_INVALID);
        }
        if (class == WorkerFailureClass::SafeBusinessFailure) != safe_error.is_some() {
            return Err(WORKER_FAILURE_INVALID);
        }
        Ok(Self {
            class,
            code,
            retryable,
            safe_error,
        })
    }

    pub fn class(&self) -> WorkerFailureClass {
        self.class
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn retryable(&self) -> bool {
        self.retryable
    }

    pub fn safe_error(&self) -> Option<&RuntimeValue> {
        self.safe_error.as_deref().map(SafeError::runtime_value)
    }

    pub(crate) fn typed_safe_error(&self) -> Option<&SafeError> {
        self.safe_error.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ExecutorKey {
    task_kind: SchedulerTaskKind,
    implementation: String,
    descriptor_version: VersionTag,
    worker_version: VersionTag,
}

/// Immutable worker request extracted from a scheduler-owned DispatchTask
/// intent. `effect_id` is the stable provider idempotency key.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskExecutionRequest {
    task_id: SchedulerTaskId,
    run_id: RunId,
    activation_id: ActivationId,
    node_id: NodeId,
    effect_id: EffectId,
    admission_class: TaskAdmissionClass,
    task_kind: SchedulerTaskKind,
    implementation: String,
    descriptor_version: VersionTag,
    worker_version: VersionTag,
    effect_policy: WorkerEffectPolicy,
    public_configuration: BTreeMap<String, DescriptorValue>,
    secret_configuration: BTreeMap<String, SecretRef>,
    inputs: Vec<BoundTaskInput>,
    outputs: Vec<TaskOutputContract>,
}

impl TaskExecutionRequest {
    pub fn from_scheduler_intent(intent: &SchedulerIntent) -> Result<Self, &'static str> {
        let action = intent.action();
        let SchedulerAction::DispatchTask {
            task_id,
            effect_id,
            admission_class,
            activation_id,
            node_id,
            task_kind,
            implementation,
            descriptor_version,
            worker_version,
            effect_policy,
            public_configuration,
            secret_configuration,
            inputs,
            outputs,
        } = action
        else {
            return Err(WORKER_TASK_KIND_MISMATCH);
        };
        Ok(Self {
            task_id: task_id.clone(),
            run_id: intent.run_id().clone(),
            activation_id: activation_id.clone(),
            node_id: node_id.clone(),
            effect_id: effect_id.clone(),
            admission_class: *admission_class,
            task_kind: *task_kind,
            implementation: implementation.clone(),
            descriptor_version: descriptor_version.clone(),
            worker_version: worker_version.clone(),
            effect_policy: effect_policy.clone(),
            public_configuration: public_configuration.clone(),
            secret_configuration: secret_configuration.clone(),
            inputs: inputs.clone(),
            outputs: outputs.clone(),
        })
    }

    fn executor_key(&self) -> ExecutorKey {
        ExecutorKey {
            task_kind: self.task_kind,
            implementation: self.implementation.clone(),
            descriptor_version: self.descriptor_version.clone(),
            worker_version: self.worker_version.clone(),
        }
    }

    pub fn task_id(&self) -> &SchedulerTaskId {
        &self.task_id
    }
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }
    pub fn effect_id(&self) -> &EffectId {
        &self.effect_id
    }
    pub fn admission_class(&self) -> TaskAdmissionClass {
        self.admission_class
    }
    pub fn task_kind(&self) -> SchedulerTaskKind {
        self.task_kind
    }
    pub fn implementation(&self) -> &str {
        &self.implementation
    }
    pub fn descriptor_version(&self) -> &VersionTag {
        &self.descriptor_version
    }
    pub fn worker_version(&self) -> &VersionTag {
        &self.worker_version
    }
    pub fn effect_policy(&self) -> &WorkerEffectPolicy {
        &self.effect_policy
    }
    pub fn public_configuration(&self) -> &BTreeMap<String, DescriptorValue> {
        &self.public_configuration
    }
    pub fn secret_configuration(&self) -> &BTreeMap<String, SecretRef> {
        &self.secret_configuration
    }
    pub fn inputs(&self) -> &[BoundTaskInput] {
        &self.inputs
    }
    pub fn outputs(&self) -> &[TaskOutputContract] {
        &self.outputs
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskExecutionResult {
    outputs: BTreeMap<DataPortId, RuntimeValue>,
    effect_evidence: EffectEvidence,
}

impl TaskExecutionResult {
    pub fn new(
        outputs: BTreeMap<DataPortId, RuntimeValue>,
        effect_evidence: EffectEvidence,
    ) -> Self {
        Self {
            outputs,
            effect_evidence,
        }
    }

    pub fn outputs(&self) -> &BTreeMap<DataPortId, RuntimeValue> {
        &self.outputs
    }

    pub fn effect_evidence(&self) -> EffectEvidence {
        self.effect_evidence
    }
}

#[async_trait]
pub trait LeafTaskExecutor: Send + Sync {
    async fn execute(
        &self,
        context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure>;
}

#[derive(Default)]
pub struct WorkerExecutorRegistry {
    executors: BTreeMap<ExecutorKey, Arc<dyn LeafTaskExecutor>>,
}

impl WorkerExecutorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        task_kind: SchedulerTaskKind,
        implementation: impl Into<String>,
        descriptor_version: VersionTag,
        worker_version: VersionTag,
        executor: Arc<dyn LeafTaskExecutor>,
    ) -> Result<(), &'static str> {
        let implementation = implementation.into();
        if implementation.is_empty()
            || implementation
                .chars()
                .any(|character| character.is_control() || character.is_whitespace())
        {
            return Err(WORKER_IMPLEMENTATION_NOT_FOUND);
        }
        let key = ExecutorKey {
            task_kind,
            implementation,
            descriptor_version,
            worker_version,
        };
        match self.executors.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(executor);
            }
            Entry::Occupied(_) => return Err(WORKER_IMPLEMENTATION_NOT_FOUND),
        }
        Ok(())
    }

    /// Read-only startup capability check for an exact frozen deployment
    /// tuple. Recovery must not discover this by executing user work.
    pub fn contains(
        &self,
        task_kind: SchedulerTaskKind,
        implementation: &str,
        descriptor_version: &VersionTag,
        worker_version: &VersionTag,
    ) -> bool {
        self.executors.contains_key(&ExecutorKey {
            task_kind,
            implementation: implementation.to_owned(),
            descriptor_version: descriptor_version.clone(),
            worker_version: worker_version.clone(),
        })
    }

    pub async fn execute(
        &self,
        context: &WorkerExecutionContext,
        request: &TaskExecutionRequest,
        cancellation: CancellationToken,
    ) -> Result<TaskExecutionResult, WorkerFailure> {
        let executor = self.executors.get(&request.executor_key()).ok_or_else(|| {
            WorkerFailure::new(
                WorkerFailureClass::InvariantCorruption,
                "WORKER_IMPLEMENTATION_NOT_FOUND",
                false,
            )
            .expect("constant failure is valid")
        })?;
        let result = executor.execute(context, request, cancellation).await?;
        validate_outputs(request.outputs(), &result)?;
        Ok(result)
    }
}

fn validate_outputs(
    contracts: &[TaskOutputContract],
    result: &TaskExecutionResult,
) -> Result<(), WorkerFailure> {
    let contracts_by_id = contracts
        .iter()
        .map(|contract| (contract.port_id(), contract))
        .collect::<BTreeMap<_, _>>();
    let invalid = result.effect_evidence != EffectEvidence::Committed
        || result.outputs.iter().any(|(port_id, value)| {
            contracts_by_id
                .get(port_id)
                .is_none_or(|contract| !value.matches(contract.value_type()))
        })
        || contracts.iter().any(|contract| {
            contract.required() && !result.outputs.contains_key(contract.port_id())
        });
    if invalid {
        return Err(WorkerFailure::new(
            WorkerFailureClass::InvariantCorruption,
            "WORKER_OUTPUT_INVALID",
            false,
        )
        .expect("constant failure is valid"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{
        plan::{PlanType, PortName},
        scheduler::TaskOutputContract,
    };
    use serde_json::json;

    struct FixedExecutor {
        output: TaskExecutionResult,
    }

    #[async_trait]
    impl LeafTaskExecutor for FixedExecutor {
        async fn execute(
            &self,
            _context: &WorkerExecutionContext,
            _request: &TaskExecutionRequest,
            _cancellation: CancellationToken,
        ) -> Result<TaskExecutionResult, WorkerFailure> {
            Ok(self.output.clone())
        }
    }

    fn output_contract() -> TaskOutputContract {
        TaskOutputContract::new(
            DataPortId::new("answer_port").unwrap(),
            PortName::new("answer").unwrap(),
            PlanType::String,
            true,
        )
    }

    #[test]
    fn closed_failure_taxonomy_defers_unknown_retry_to_frozen_effect_policy() {
        for class in [
            WorkerFailureClass::ControlTermination,
            WorkerFailureClass::InvariantCorruption,
        ] {
            assert_eq!(
                WorkerFailure::new(class, "SAFE_CODE", true),
                Err(WORKER_FAILURE_INVALID)
            );
        }
        assert!(WorkerFailure::new(
            WorkerFailureClass::EffectOutcomeUnknown,
            "EFFECT_STATUS_UNKNOWN",
            true,
        )
        .is_ok());
        assert!(WorkerFailure::new(
            WorkerFailureClass::InfrastructureFailure,
            "PROVIDER_UNAVAILABLE",
            true,
        )
        .is_ok());
    }

    #[test]
    fn safe_business_failure_validates_payload_and_has_one_code_authority() {
        let safe_error = RuntimeValue::new(json!({
            "kind": "safe_error",
            "code": "RISK_REJECTED",
            "message": "risk policy rejected the request"
        }))
        .unwrap();
        assert!(WorkerFailure::safe_business("RISK_REJECTED", false, safe_error.clone(),).is_ok());
        assert_eq!(
            WorkerFailure::safe_business("DIFFERENT_CODE", false, safe_error),
            Err(WORKER_FAILURE_INVALID),
        );
        assert_eq!(
            WorkerFailure::safe_business(
                "RISK_REJECTED",
                false,
                RuntimeValue::new(json!({
                    "kind": "safe_error",
                    "code": "RISK_REJECTED",
                    "message": "rejected",
                    "provider_body": "must not be exposed"
                }))
                .unwrap(),
            ),
            Err(WORKER_FAILURE_INVALID),
        );
    }

    #[tokio::test]
    async fn executor_registry_is_version_exact_and_validates_required_typed_outputs() {
        let contract = output_contract();
        let request = TaskExecutionRequest {
            task_id: SchedulerTaskId::parse(format!("task_{}", "1".repeat(64))).unwrap(),
            run_id: RunId::new("run_worker_test").unwrap(),
            activation_id: ActivationId::new("activation_worker_test").unwrap(),
            node_id: NodeId::new("node_worker_test").unwrap(),
            effect_id: EffectId::new("effect_worker_test").unwrap(),
            admission_class: TaskAdmissionClass::Normal,
            task_kind: SchedulerTaskKind::Action,
            implementation: "test.action".to_owned(),
            descriptor_version: VersionTag::new("descriptor-1").unwrap(),
            worker_version: VersionTag::new("worker-1").unwrap(),
            effect_policy: WorkerEffectPolicy::new(
                crate::engine::EffectIdempotency::Idempotent,
                1,
                crate::engine::WorkerCancellation::Cooperative,
            )
            .unwrap(),
            public_configuration: BTreeMap::new(),
            secret_configuration: BTreeMap::new(),
            inputs: vec![],
            outputs: vec![contract.clone()],
        };
        let output_id = contract.port_id().clone();
        let context = WorkerExecutionContext::new(
            AttemptNo::FIRST,
            LeaseEpoch::FIRST,
            "worker-test-fence",
            Utc::now() + chrono::Duration::minutes(1),
        )
        .unwrap();
        let mut registry = WorkerExecutorRegistry::new();
        registry
            .register(
                SchedulerTaskKind::Action,
                "test.action",
                VersionTag::new("descriptor-1").unwrap(),
                VersionTag::new("worker-1").unwrap(),
                Arc::new(FixedExecutor {
                    output: TaskExecutionResult::new(
                        BTreeMap::from([(
                            output_id.clone(),
                            RuntimeValue::new(json!("ok")).unwrap(),
                        )]),
                        EffectEvidence::Committed,
                    ),
                }),
            )
            .unwrap();
        assert!(registry
            .execute(&context, &request, CancellationToken::new())
            .await
            .is_ok());

        let missing = WorkerExecutorRegistry::new()
            .execute(&context, &request, CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(missing.code(), "WORKER_IMPLEMENTATION_NOT_FOUND");

        let invalid = validate_outputs(
            &[contract],
            &TaskExecutionResult::new(
                BTreeMap::from([(output_id, RuntimeValue::new(json!(7)).unwrap())]),
                EffectEvidence::Committed,
            ),
        )
        .unwrap_err();
        assert_eq!(invalid.code(), "WORKER_OUTPUT_INVALID");
    }
}
