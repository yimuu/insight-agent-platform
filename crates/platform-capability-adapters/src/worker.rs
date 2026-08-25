use super::{
    adapter_failure_outcome, CapabilityAdapterFailure, CapabilityAdapterFailureClass,
    CapabilityAdapterRequest, CapabilityDispatchError, CapabilityDispatcher,
    CapabilityTransportCancelOutcome, CapabilityTransportRequestIdentity,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    CommandOutcome, ExternalLeafResumeMutationIds, ResourceId, ResourceKind,
};
use insight_platform_invocations::{
    CapabilityWorkerAudit, CommitCapabilityCancellationOutcome, CommitCapabilityOutcome,
    DispatchOutcome, CAPABILITY_QUOTA_LINES,
};
use insight_platform_jobs::JobFence;
use std::{collections::BTreeSet, error::Error, fmt, sync::Arc};

/// Exact command handed to one Capability Worker after a PostgreSQL claim transaction commits.
///
/// The command contains no plaintext Secret and grants no mutation access to Run or Invocation
/// state. The worker may execute the exact adapter request and submit one fenced outcome through
/// [`CapabilityExecutionAuthority`].
#[derive(Debug, Clone)]
pub struct ExecuteCapabilityAdapterJob {
    pub execution: CapabilityAdapterRequest,
    pub audit: CapabilityWorkerAudit,
    pub expected_invocation_version: u64,
    pub fence: JobFence,
    pub quota_entry_ids: Vec<ResourceId>,
    /// Retry time already intersected with the frozen policy and remaining Run/Invocation budget.
    /// It is consumed only when the adapter returns a safely retryable failure.
    pub retry_at: Option<DateTime<Utc>>,
    pub resume_mutations: Option<ExternalLeafResumeMutationIds>,
}

impl ExecuteCapabilityAdapterJob {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
    ) -> Result<(), CapabilityAdapterWorkerContractError> {
        self.audit
            .validate_at(now)
            .map_err(|_| CapabilityAdapterWorkerContractError::InvalidCommand)?;
        self.execution
            .validate_at(now)
            .map_err(|_| CapabilityAdapterWorkerContractError::InvalidCommand)?;
        if self.expected_invocation_version == 0
            || self.audit.tenant_id != self.execution.tenant_id
            || self.audit.worker_process_generation_id
                != self.execution.worker_process_generation_id
            || self.fence.expected_version == 0
            || self.fence.worker_process_generation_id
                != self.execution.worker_process_generation_id
            || self.fence.lease_generation != self.execution.lease_generation
            || self.quota_entry_ids.len() != CAPABILITY_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || self.quota_entry_ids.iter().collect::<BTreeSet<_>>().len()
                != self.quota_entry_ids.len()
            || self.retry_at.is_some_and(|retry_at| {
                self.execution.physical_attempt >= self.execution.attempt_limit
                    || retry_at <= now
                    || retry_at >= self.execution.deadline
            })
        {
            return Err(CapabilityAdapterWorkerContractError::InvalidCommand);
        }
        Ok(())
    }
}

#[async_trait]
pub trait CapabilityExecutionAuthority: Send + Sync {
    type Error;
    type Record;

    async fn commit_capability_outcome(
        &self,
        command: CommitCapabilityOutcome,
    ) -> Result<CommandOutcome<Self::Record>, Self::Error>;

    async fn commit_capability_cancellation_outcome(
        &self,
        command: CommitCapabilityCancellationOutcome,
    ) -> Result<CommandOutcome<Self::Record>, Self::Error>;
}

#[derive(Debug, Clone)]
pub struct CancelCapabilityAdapterJob {
    pub execution: CapabilityAdapterRequest,
    pub audit: CapabilityWorkerAudit,
    pub expected_invocation_version: u64,
    pub fence: JobFence,
    pub quota_entry_ids: Vec<ResourceId>,
    pub cancel_deadline: DateTime<Utc>,
}

impl CancelCapabilityAdapterJob {
    pub fn validate_at(
        &self,
        now: DateTime<Utc>,
    ) -> Result<(), CapabilityAdapterWorkerContractError> {
        self.audit
            .validate_at(now)
            .map_err(|_| CapabilityAdapterWorkerContractError::InvalidCommand)?;
        self.execution
            .validate_shape()
            .map_err(|_| CapabilityAdapterWorkerContractError::InvalidCommand)?;
        if self.expected_invocation_version == 0
            || self.audit.tenant_id != self.execution.tenant_id
            || self.audit.worker_process_generation_id
                != self.execution.worker_process_generation_id
            || self.fence.expected_version == 0
            || self.fence.worker_process_generation_id
                != self.execution.worker_process_generation_id
            || self.fence.lease_generation != self.execution.lease_generation
            || self.quota_entry_ids.len() != CAPABILITY_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || self.quota_entry_ids.iter().collect::<BTreeSet<_>>().len()
                != self.quota_entry_ids.len()
            || self.cancel_deadline <= now
            || self.cancel_deadline
                > self
                    .cleanup_deadline()
                    .ok_or(CapabilityAdapterWorkerContractError::InvalidCommand)?
            || !self
                .execution
                .execution
                .implementation
                .features
                .cancellation
            || !matches!(
                self.execution.execution.implementation.backend_kind,
                insight_platform_contracts::CapabilityBackendKind::Native
                    | insight_platform_contracts::CapabilityBackendKind::Http
                    | insight_platform_contracts::CapabilityBackendKind::Grpc
                    | insight_platform_contracts::CapabilityBackendKind::Mcp
            )
        {
            return Err(CapabilityAdapterWorkerContractError::InvalidCommand);
        }
        Ok(())
    }

    /// Bounded authority window for persisting the cancellation observation. It is derived from
    /// the frozen execution deadline and backend total timeout rather than accepted from a caller.
    pub fn cleanup_deadline(&self) -> Option<DateTime<Utc>> {
        let milliseconds = i64::try_from(
            self.execution
                .execution
                .implementation
                .backend_limits
                .total_timeout_milliseconds,
        )
        .ok()?;
        self.execution
            .deadline
            .checked_add_signed(chrono::Duration::milliseconds(milliseconds))
    }
}

pub struct CapabilityAdapterWorker<A> {
    dispatcher: Arc<CapabilityDispatcher>,
    authority: A,
}

impl<A> CapabilityAdapterWorker<A>
where
    A: CapabilityExecutionAuthority,
{
    pub fn new(dispatcher: Arc<CapabilityDispatcher>, authority: A) -> Self {
        Self {
            dispatcher,
            authority,
        }
    }

    pub async fn execute(
        &self,
        command: ExecuteCapabilityAdapterJob,
    ) -> Result<CommandOutcome<A::Record>, CapabilityAdapterWorkerError<A::Error>> {
        command
            .validate_at(Utc::now())
            .map_err(CapabilityAdapterWorkerError::Contract)?;
        let outcome = match self.dispatcher.dispatch(&command.execution).await {
            Ok(response) => response.outcome,
            Err(failure) => failure_outcome(&command, failure)
                .map_err(CapabilityAdapterWorkerError::Contract)?,
        };
        self.authority
            .commit_capability_outcome(CommitCapabilityOutcome {
                audit: command.audit,
                invocation_id: command.execution.invocation_id,
                job_id: command.execution.job_id,
                expected_invocation_version: command.expected_invocation_version,
                fence: command.fence,
                quota_entry_ids: command.quota_entry_ids,
                outcome,
                resume_mutations: command.resume_mutations,
            })
            .await
            .map_err(CapabilityAdapterWorkerError::Authority)
    }

    pub async fn cancel(
        &self,
        command: CancelCapabilityAdapterJob,
    ) -> Result<CommandOutcome<A::Record>, CapabilityAdapterWorkerError<A::Error>> {
        let cleanup_deadline =
            command
                .cleanup_deadline()
                .ok_or(CapabilityAdapterWorkerError::Contract(
                    CapabilityAdapterWorkerContractError::InvalidCommand,
                ))?;
        command
            .validate_at(Utc::now())
            .map_err(CapabilityAdapterWorkerError::Contract)?;
        let identity = CapabilityTransportRequestIdentity::from_adapter_request(
            &command.execution,
            command.execution.execution.implementation.backend_kind,
        );
        let (observation, failure_evidence_digest) = match self
            .dispatcher
            .cancel_execution(&command.execution, command.cancel_deadline)
            .await
        {
            Ok(CapabilityTransportCancelOutcome::Accepted) => ("accepted".to_owned(), None),
            Ok(CapabilityTransportCancelOutcome::AlreadyTerminal) => {
                ("already_terminal".to_owned(), None)
            }
            Ok(CapabilityTransportCancelOutcome::Unsupported) => ("unsupported".to_owned(), None),
            Err(failure) => (failure.safe_code, Some(failure.evidence_digest)),
        };
        let cancellation_observation_digest: insight_platform_contracts::Sha256Digest =
            insight_platform_contracts::canonical_digest(&serde_json::json!({
                "failure_evidence_digest": failure_evidence_digest,
                "identity": identity,
                "observation": observation,
                "schema_version": 1,
            }))
            .map_err(|_| {
                CapabilityAdapterWorkerError::Contract(
                    CapabilityAdapterWorkerContractError::InvalidCommand,
                )
            })?
            .parse()
            .map_err(|_| {
                CapabilityAdapterWorkerError::Contract(
                    CapabilityAdapterWorkerContractError::InvalidCommand,
                )
            })?;
        self.authority
            .commit_capability_cancellation_outcome(CommitCapabilityCancellationOutcome {
                audit: command.audit,
                invocation_id: command.execution.invocation_id,
                job_id: command.execution.job_id,
                expected_invocation_version: command.expected_invocation_version,
                fence: Some(command.fence),
                quota_entry_ids: command.quota_entry_ids,
                cancellation_observation_digest,
                external_identity_digest: Some(identity.evidence_digest()),
                // Transport cancellation is never proof that a prior Effect did not occur.
                no_effect_proof_digest: None,
                cleanup_deadline: Some(cleanup_deadline),
            })
            .await
            .map_err(CapabilityAdapterWorkerError::Authority)
    }
}

fn failure_outcome(
    command: &ExecuteCapabilityAdapterJob,
    mut failure: CapabilityAdapterFailure,
) -> Result<DispatchOutcome, CapabilityAdapterWorkerContractError> {
    let retryable = matches!(
        failure.class,
        CapabilityAdapterFailureClass::RetryableBeforeDispatch
            | CapabilityAdapterFailureClass::RetryableAfterDispatch
    );
    if retryable && command.retry_at.is_none() {
        // The physical attempt is exhausted or the trusted caller found no remaining policy/deadline
        // window. Preserve the safe external failure, but do not leave a Running Job waiting for a
        // retry time that can never be scheduled.
        failure.class = CapabilityAdapterFailureClass::Permanent;
        failure.external_identity_digest = None;
    }
    adapter_failure_outcome(&command.execution, failure, command.retry_at)
        .map_err(CapabilityAdapterWorkerContractError::InvalidAdapterFailure)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityAdapterWorkerContractError {
    InvalidCommand,
    InvalidAdapterFailure(CapabilityDispatchError),
}

impl fmt::Display for CapabilityAdapterWorkerContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCommand => formatter.write_str("Capability worker command is invalid"),
            Self::InvalidAdapterFailure(failure) => {
                write!(
                    formatter,
                    "Capability adapter failure is invalid: {failure}"
                )
            }
        }
    }
}

impl Error for CapabilityAdapterWorkerContractError {}

#[derive(Debug)]
pub enum CapabilityAdapterWorkerError<E> {
    Contract(CapabilityAdapterWorkerContractError),
    Authority(E),
}

impl<E: fmt::Display> fmt::Display for CapabilityAdapterWorkerError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(failure) => failure.fmt(formatter),
            Self::Authority(failure) => write!(formatter, "Capability authority failed: {failure}"),
        }
    }
}

impl<E: Error + 'static> Error for CapabilityAdapterWorkerError<E> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CapabilityAdapterFailure, CapabilityAdapterResponse, CapabilityBackendPort,
        CapabilityTransportCancelOutcome, CapabilityTransportCancelRequest, InstalledNativeAdapter,
        InstalledNativeRegistry, NativeCapabilityAdapter,
    };
    use async_trait::async_trait;
    use chrono::Duration;
    use insight_platform_contracts::{
        canonical_digest, ArtifactRef, CapabilityBackendBinding, CapabilityBackendContract,
        CapabilityBackendFeatures, CapabilityBackendLimits, CapabilityDeploymentClosure,
        CapabilityIdempotencyKind, CommandOutcome, DataClassification, Effect, ExactDeploymentRef,
        ExactVersionRef, FailureClass, NativeCapabilityContract, ResourceId, ResourceKind,
        Retryability, Sha256Digest, ValueRef, WORKER_PROTOCOL_VERSION,
    };
    use insight_platform_invocations::{
        CapabilityExecutionContract, CapabilityExecutionInput, CapabilityExecutionInputMaterial,
        CapabilityImplementationContract, CapabilityOutputValue, ExactInvocationValueRef,
        InvocationValueStorage,
    };
    use std::sync::Mutex;

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1c9-32e4-75e1-a9e8-d95ca0f6{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn exact(kind: ResourceKind, suffix: u16, character: char) -> ExactVersionRef {
        ExactVersionRef::new(id(kind, suffix), digest(character)).unwrap()
    }

    fn native_execution(worker_manifest_digest: Sha256Digest) -> CapabilityExecutionContract {
        let interface = exact(ResourceKind::CapabilityInterfaceRevision, 1, '1');
        let implementation_revision = exact(ResourceKind::CapabilityImplementationRevision, 2, '2');
        let backend_contract = CapabilityBackendContract::Native(NativeCapabilityContract {
            adapter_id: "builtin.worker_fixture".to_owned(),
            adapter_version: "1.0.0".to_owned(),
            module_digest: digest('3'),
            entrypoint_id: "fixture.invoke".to_owned(),
            worker_protocol_version: WORKER_PROTOCOL_VERSION,
        });
        let implementation = CapabilityImplementationContract {
            revision: implementation_revision.clone(),
            interface_revision: interface.clone(),
            backend_kind: insight_platform_contracts::CapabilityBackendKind::Native,
            backend_contract_digest: backend_contract.canonical_digest().unwrap(),
            backend_contract,
            credential_requirements: vec![],
            backend_limits: CapabilityBackendLimits {
                maximum_request_bytes: 4_096,
                maximum_response_bytes: 4_096,
                maximum_diagnostic_bytes: 1_024,
                connect_timeout_milliseconds: 10,
                first_byte_timeout_milliseconds: 20,
                idle_timeout_milliseconds: 30,
                total_timeout_milliseconds: 1_000,
            },
            features: CapabilityBackendFeatures {
                deferred: false,
                input_required: false,
                callback: false,
                poll: false,
                progress: false,
                cancellation: false,
                max_remote_state_bytes: 0,
                max_poll_count: 0,
            },
        };
        CapabilityExecutionContract::build(
            ExactDeploymentRef::new(id(ResourceKind::CapabilityDeployment, 3), digest('4'))
                .unwrap(),
            CapabilityDeploymentClosure {
                implementation: implementation_revision,
                interface,
                backend: CapabilityBackendBinding::Native {
                    worker_manifest_digest,
                    adapter_module_digest: digest('3'),
                },
                secret_bindings: vec![],
                policies: vec![],
                conformance_evidence: ArtifactRef::new(
                    id(ResourceKind::Artifact, 4),
                    digest('5'),
                    8,
                    "application/json",
                    DataClassification::Internal,
                    None,
                )
                .unwrap(),
            },
            implementation,
        )
        .unwrap()
    }

    fn request(
        worker_manifest_digest: Sha256Digest,
        effect: Effect,
        idempotency: CapabilityIdempotencyKind,
        physical_attempt: u32,
        attempt_limit: u32,
    ) -> CapabilityAdapterRequest {
        let value = serde_json::json!({"query": "status"});
        CapabilityAdapterRequest {
            tenant_id: id(ResourceKind::Tenant, 10),
            invocation_id: id(ResourceKind::CapabilityInvocation, 11),
            job_id: id(ResourceKind::Job, 12),
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 13),
            worker_manifest_digest: worker_manifest_digest.clone(),
            lease_generation: 1,
            physical_attempt,
            attempt_limit,
            admission_digest: digest('6'),
            idempotency_key_digest: digest('7'),
            effect,
            idempotency,
            deadline: Utc::now() + Duration::seconds(30),
            execution: native_execution(worker_manifest_digest),
            input: CapabilityExecutionInput {
                exact: ExactInvocationValueRef {
                    schema_version: 1,
                    value_id: id(ResourceKind::RunValue, 14),
                    run_id: id(ResourceKind::Run, 15),
                    producing_node_id: Some(id(ResourceKind::NodeExecution, 16)),
                    value_kind: "capability_input".to_owned(),
                    classification: DataClassification::Internal,
                    schema_digest: digest('8'),
                    content_digest: canonical_digest(&value).unwrap().parse().unwrap(),
                    storage: InvocationValueStorage::Inline,
                },
                material: CapabilityExecutionInputMaterial::Inline { value },
            },
            continuation: None,
            mcp_runtime: None,
        }
    }

    fn audit(request: &CapabilityAdapterRequest) -> CapabilityWorkerAudit {
        CapabilityWorkerAudit {
            tenant_id: request.tenant_id.clone(),
            worker_process_generation_id: request.worker_process_generation_id.clone(),
            receipt_id: id(ResourceKind::Receipt, 20),
            event_id: id(ResourceKind::Event, 21),
            outbox_id: id(ResourceKind::OutboxEvent, 22),
            idempotency_key_digest: digest('9'),
            request_digest: digest('a'),
            receipt_expires_at: Utc::now() + Duration::hours(1),
        }
    }

    fn command(execution: CapabilityAdapterRequest) -> ExecuteCapabilityAdapterJob {
        ExecuteCapabilityAdapterJob {
            audit: audit(&execution),
            expected_invocation_version: 3,
            fence: JobFence {
                expected_version: 4,
                worker_process_generation_id: execution.worker_process_generation_id.clone(),
                lease_generation: execution.lease_generation,
                token_digest: digest('b'),
            },
            quota_entry_ids: vec![
                id(ResourceKind::QuotaLedgerEntry, 23),
                id(ResourceKind::QuotaLedgerEntry, 24),
            ],
            retry_at: Some(Utc::now() + Duration::seconds(1)),
            resume_mutations: None,
            execution,
        }
    }

    struct FixtureNative {
        descriptor: InstalledNativeAdapter,
        result: Result<CapabilityAdapterResponse, CapabilityAdapterFailure>,
    }

    #[async_trait]
    impl NativeCapabilityAdapter for FixtureNative {
        fn descriptor(&self) -> InstalledNativeAdapter {
            self.descriptor.clone()
        }

        async fn invoke(
            &self,
            _request: &CapabilityAdapterRequest,
        ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure> {
            self.result.clone()
        }
    }

    #[derive(Default)]
    struct CapturingAuthority {
        commands: Mutex<Vec<CommitCapabilityOutcome>>,
        cancellations: Mutex<Vec<CommitCapabilityCancellationOutcome>>,
    }

    #[async_trait]
    impl CapabilityExecutionAuthority for Arc<CapturingAuthority> {
        type Error = std::convert::Infallible;
        type Record = CommitCapabilityOutcome;

        async fn commit_capability_outcome(
            &self,
            command: CommitCapabilityOutcome,
        ) -> Result<CommandOutcome<Self::Record>, Self::Error> {
            self.commands.lock().unwrap().push(command.clone());
            Ok(CommandOutcome::Applied(command))
        }

        async fn commit_capability_cancellation_outcome(
            &self,
            command: CommitCapabilityCancellationOutcome,
        ) -> Result<CommandOutcome<Self::Record>, Self::Error> {
            self.cancellations.lock().unwrap().push(command.clone());
            Ok(CommandOutcome::Applied(CommitCapabilityOutcome {
                audit: command.audit.clone(),
                invocation_id: command.invocation_id.clone(),
                job_id: command.job_id.clone(),
                expected_invocation_version: command.expected_invocation_version,
                fence: command.fence.clone().unwrap(),
                quota_entry_ids: command.quota_entry_ids.clone(),
                outcome: DispatchOutcome::Uncertain(
                    insight_platform_invocations::CapabilityUncertainty {
                        observation_digest: command.cancellation_observation_digest,
                        policy_path_digest: digest('e'),
                        external_identity_digest: command.external_identity_digest.unwrap(),
                        manual: true,
                    },
                ),
                resume_mutations: None,
            }))
        }
    }

    struct CancellingHttpPort;

    #[async_trait]
    impl CapabilityBackendPort for CancellingHttpPort {
        fn kind(&self) -> insight_platform_contracts::CapabilityBackendKind {
            insight_platform_contracts::CapabilityBackendKind::Http
        }

        async fn invoke(
            &self,
            _request: &CapabilityAdapterRequest,
        ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure> {
            unreachable!("cancel fixture never invokes the transport")
        }

        async fn cancel(
            &self,
            _request: CapabilityTransportCancelRequest,
        ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
            Ok(CapabilityTransportCancelOutcome::Accepted)
        }
    }

    fn worker(
        execution: &CapabilityAdapterRequest,
        result: Result<CapabilityAdapterResponse, CapabilityAdapterFailure>,
    ) -> (
        CapabilityAdapterWorker<Arc<CapturingAuthority>>,
        Arc<CapturingAuthority>,
    ) {
        let CapabilityBackendContract::Native(contract) =
            &execution.execution.implementation.backend_contract
        else {
            unreachable!();
        };
        let descriptor = InstalledNativeAdapter {
            adapter_id: contract.adapter_id.clone(),
            adapter_version: contract.adapter_version.clone(),
            module_digest: contract.module_digest.clone(),
            entrypoint_id: contract.entrypoint_id.clone(),
        };
        let mut registry = InstalledNativeRegistry::default();
        registry
            .install(Arc::new(FixtureNative { descriptor, result }))
            .unwrap();
        let authority = Arc::new(CapturingAuthority::default());
        (
            CapabilityAdapterWorker::new(
                Arc::new(CapabilityDispatcher::new(registry)),
                authority.clone(),
            ),
            authority,
        )
    }

    #[tokio::test]
    async fn successful_dispatch_submits_the_exact_fenced_outcome() {
        let manifest = digest('c');
        let execution = request(
            manifest,
            Effect::ReadOnly,
            CapabilityIdempotencyKind::Intrinsic,
            1,
            2,
        );
        let output_json = serde_json::json!({"accepted": true});
        let response = CapabilityAdapterResponse {
            outcome: DispatchOutcome::Completed(CapabilityOutputValue {
                value_id: id(ResourceKind::RunValue, 30),
                classification: DataClassification::Internal,
                schema_digest: digest('d'),
                content_digest: canonical_digest(&output_json).unwrap().parse().unwrap(),
                value: ValueRef::Inline { value: output_json },
                artifact_link_id: None,
                validation_evidence_digest: digest('e'),
            }),
        };
        let (worker, authority) = worker(&execution, Ok(response.clone()));
        let command = command(execution.clone());
        worker.execute(command.clone()).await.unwrap();
        let committed = authority.commands.lock().unwrap();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].invocation_id, execution.invocation_id);
        assert_eq!(committed[0].job_id, execution.job_id);
        assert_eq!(committed[0].fence, command.fence);
        assert_eq!(committed[0].outcome, response.outcome);
    }

    #[tokio::test]
    async fn unsafe_after_dispatch_write_is_never_committed_as_retryable() {
        let manifest = digest('c');
        let execution = request(
            manifest,
            Effect::NonIdempotentWrite,
            CapabilityIdempotencyKind::None,
            1,
            2,
        );
        let failure = CapabilityAdapterFailure {
            class: CapabilityAdapterFailureClass::RetryableAfterDispatch,
            safe_code: "remote_connection_lost".to_owned(),
            safe_message: "Remote completion was not observed".to_owned(),
            evidence_digest: digest('e'),
            external_identity_digest: Some(digest('f')),
        };
        let (worker, authority) = worker(&execution, Err(failure));
        worker.execute(command(execution)).await.unwrap();
        let committed = authority.commands.lock().unwrap();
        assert!(matches!(
            &committed[0].outcome,
            DispatchOutcome::Uncertain(uncertain)
                if uncertain.external_identity_digest == digest('f')
        ));
    }

    #[tokio::test]
    async fn exhausted_attempt_converts_a_retryable_failure_to_terminal_failure() {
        let manifest = digest('c');
        let execution = request(
            manifest,
            Effect::ReadOnly,
            CapabilityIdempotencyKind::Intrinsic,
            1,
            1,
        );
        let failure = CapabilityAdapterFailure {
            class: CapabilityAdapterFailureClass::RetryableBeforeDispatch,
            safe_code: "adapter_capacity".to_owned(),
            safe_message: "Adapter capacity is unavailable".to_owned(),
            evidence_digest: digest('e'),
            external_identity_digest: None,
        };
        let (worker, authority) = worker(&execution, Err(failure));
        let mut command = command(execution);
        command.retry_at = None;
        worker.execute(command).await.unwrap();
        let committed = authority.commands.lock().unwrap();
        assert!(matches!(
            &committed[0].outcome,
            DispatchOutcome::PermanentFailure(failure)
                if failure.failure.class == FailureClass::External
                    && failure.failure.retryability == Retryability::Never
        ));
    }

    #[tokio::test]
    async fn stale_worker_identity_is_rejected_before_dispatch_or_commit() {
        let manifest = digest('c');
        let execution = request(
            manifest,
            Effect::ReadOnly,
            CapabilityIdempotencyKind::Intrinsic,
            1,
            2,
        );
        let response = CapabilityAdapterResponse {
            outcome: DispatchOutcome::PermanentFailure(
                insight_platform_invocations::SafeBackendFailure {
                    failure: insight_platform_contracts::Failure {
                        code: insight_platform_contracts::FailureCode::Platform {
                            code: insight_platform_contracts::PlatformFailureCode::CapabilityFailed,
                        },
                        class: FailureClass::External,
                        retryability: Retryability::Never,
                        safe_message: None,
                        details_ref: None,
                        source: insight_platform_contracts::FailureSource::Capability,
                    },
                    evidence_digest: digest('d'),
                },
            ),
        };
        let (worker, authority) = worker(&execution, Ok(response));
        let mut command = command(execution);
        command.fence.worker_process_generation_id = id(ResourceKind::WorkerProcessGeneration, 99);
        assert!(matches!(
            worker.execute(command).await,
            Err(CapabilityAdapterWorkerError::Contract(
                CapabilityAdapterWorkerContractError::InvalidCommand
            ))
        ));
        assert!(authority.commands.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn transport_cancel_after_execution_deadline_submits_no_no_effect_proof() {
        let mut execution = request(
            digest('c'),
            Effect::NonIdempotentWrite,
            CapabilityIdempotencyKind::None,
            1,
            2,
        );
        let http = crate::tests::http_execution(1_000);
        let mut implementation = http.implementation;
        implementation.features.cancellation = true;
        let http = CapabilityExecutionContract::build(
            http.deployment,
            http.deployment_closure,
            implementation,
        )
        .unwrap();
        execution.worker_manifest_digest = digest('f');
        execution.execution = http;
        execution.deadline = Utc::now() - Duration::milliseconds(100);
        let mut dispatcher = CapabilityDispatcher::new(InstalledNativeRegistry::default());
        dispatcher
            .install_port(Arc::new(CancellingHttpPort))
            .unwrap();
        let authority = Arc::new(CapturingAuthority::default());
        let worker = CapabilityAdapterWorker::new(Arc::new(dispatcher), authority.clone());
        let execute = command(execution.clone());
        let cancel = CancelCapabilityAdapterJob {
            execution,
            audit: execute.audit,
            expected_invocation_version: execute.expected_invocation_version + 1,
            fence: JobFence {
                expected_version: execute.fence.expected_version + 1,
                ..execute.fence
            },
            quota_entry_ids: execute.quota_entry_ids,
            cancel_deadline: Utc::now() + Duration::milliseconds(500),
        };
        worker.cancel(cancel).await.unwrap();
        let cancellations = authority.cancellations.lock().unwrap();
        assert_eq!(cancellations.len(), 1);
        assert!(cancellations[0].no_effect_proof_digest.is_none());
        assert!(cancellations[0].external_identity_digest.is_some());
        assert!(cancellations[0].fence.is_some());
    }
}
