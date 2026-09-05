use super::{
    static_digest, ModelAdapterExecutionOutcome, ModelAdapterExecutionRequest, ModelAdapterFailure,
    ModelAdapterFailureClass, ModelAdapterHost, ModelAdapterHostError, ModelAdapterSuccess,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    CommandOutcome, DecimalMoney, ExternalLeafFailureMutationIds, ExternalLeafResumeMutationIds,
    FailureClass, ResourceId, ResourceKind, Retryability,
};
use insight_platform_jobs::JobFence;
use insight_platform_models::{
    model_failure, AccountingQuality, CommitModelOutcome, ModelAttemptMeasurement,
    ModelDispatchOutcome, ModelObservation, ModelOutputValue, ModelToolContinuationMutationIds,
    ModelUsage, ModelWorkerAudit, SafeModelFailure, MODEL_QUOTA_LINES,
};
use std::{collections::BTreeSet, error::Error, fmt, sync::Arc};

#[derive(Debug, Clone)]
pub struct ExecuteModelAdapterJob {
    pub execution: ModelAdapterExecutionRequest,
    pub audit: ModelWorkerAudit,
    pub expected_turn_version: u64,
    pub fence: JobFence,
    pub usage_reservation_id: ResourceId,
    pub quota_entry_ids: Vec<ResourceId>,
    pub resume_mutations: Option<ExternalLeafResumeMutationIds>,
    pub failure_mutations: Option<ExternalLeafFailureMutationIds>,
    pub tool_continuation_mutations: Option<ModelToolContinuationMutationIds>,
}

impl ExecuteModelAdapterJob {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ModelAdapterWorkerContractError> {
        self.audit
            .validate_at(now)
            .map_err(|_| ModelAdapterWorkerContractError::InvalidCommand)?;
        if self.expected_turn_version == 0
            || self.audit.tenant_id != self.execution.tenant_id
            || self.audit.worker_process_generation_id
                != self.execution.worker_process_generation_id
            || self.fence.expected_version == 0
            || self.fence.worker_process_generation_id
                != self.execution.worker_process_generation_id
            || self.fence.lease_generation != self.execution.lease_generation
            || self.usage_reservation_id.kind() != ResourceKind::UsageReservation
            || self.quota_entry_ids.len() != MODEL_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || self.quota_entry_ids.iter().collect::<BTreeSet<_>>().len()
                != self.quota_entry_ids.len()
            || self.resume_mutations.is_some() != self.failure_mutations.is_some()
            || self.resume_mutations.is_some() != self.tool_continuation_mutations.is_some()
            || self
                .resume_mutations
                .as_ref()
                .is_some_and(|mutations| mutations.validate().is_err())
            || self
                .failure_mutations
                .as_ref()
                .is_some_and(|mutations| mutations.validate().is_err())
            || self
                .tool_continuation_mutations
                .as_ref()
                .is_some_and(|mutations| mutations.validate().is_err())
        {
            return Err(ModelAdapterWorkerContractError::InvalidCommand);
        }
        Ok(())
    }
}

/// Converts a validated normalized terminal response into the shared RunValue/Artifact shape.
///
/// The implementation owns value-ID allocation and inline-versus-Artifact materialization. It
/// receives no Provider SDK value and cannot advance durable state.
#[async_trait]
pub trait ModelOutputMaterializer: Send + Sync {
    /// Fails before Provider dispatch when this materializer cannot represent every response
    /// allowed by the exact frozen execution contract.
    fn validate_execution(
        &self,
        execution: &ModelAdapterExecutionRequest,
    ) -> Result<(), ModelAdapterFailure>;

    async fn materialize(
        &self,
        execution: &ModelAdapterExecutionRequest,
        success: ModelAdapterSuccess,
    ) -> Result<ModelOutputValue, ModelAdapterFailure>;
}

#[async_trait]
pub trait ModelExecutionAuthority: Send + Sync {
    type Error;
    type Record;

    async fn commit_model_outcome(
        &self,
        command: CommitModelOutcome,
    ) -> Result<CommandOutcome<Self::Record>, Self::Error>;
}

pub struct ModelAdapterWorker<M, A> {
    host: Arc<ModelAdapterHost>,
    materializer: M,
    authority: A,
}

pub struct PreparedModelAdapterCommit {
    command: CommitModelOutcome,
}

impl PreparedModelAdapterCommit {
    pub fn refresh_fence(
        &mut self,
        fence: JobFence,
    ) -> Result<(), ModelAdapterWorkerContractError> {
        if fence.worker_process_generation_id != self.command.fence.worker_process_generation_id
            || fence.lease_generation != self.command.fence.lease_generation
            || fence.token_digest != self.command.fence.token_digest
            || fence.expected_version < self.command.fence.expected_version
        {
            return Err(ModelAdapterWorkerContractError::InvalidCommand);
        }
        self.command.fence = fence;
        Ok(())
    }

    pub fn fence(&self) -> &JobFence {
        &self.command.fence
    }
}

impl<M, A> ModelAdapterWorker<M, A>
where
    M: ModelOutputMaterializer,
    A: ModelExecutionAuthority,
{
    pub fn new(host: Arc<ModelAdapterHost>, materializer: M, authority: A) -> Self {
        Self {
            host,
            materializer,
            authority,
        }
    }

    pub async fn execute(
        &self,
        command: ExecuteModelAdapterJob,
    ) -> Result<CommandOutcome<A::Record>, ModelAdapterWorkerError<A::Error>> {
        let prepared = self
            .prepare(command)
            .await
            .map_err(ModelAdapterWorkerError::Contract)?;
        self.commit(prepared).await
    }

    /// Executes Provider I/O and local materialization without mutating durable authority.
    ///
    /// A production supervisor may heartbeat the exact lease while this future is pending, then
    /// rotate only the optimistic Job version through [`PreparedModelAdapterCommit::refresh_fence`]
    /// before committing the already-normalized outcome. This avoids replaying paid Provider I/O
    /// merely because a heartbeat advanced the Job row version.
    pub async fn prepare(
        &self,
        command: ExecuteModelAdapterJob,
    ) -> Result<PreparedModelAdapterCommit, ModelAdapterWorkerContractError> {
        let now = Utc::now();
        command
            .validate_at(now)
            .map_err(|_| ModelAdapterWorkerContractError::InvalidCommand)?;
        let adapter_outcome = match self.materializer.validate_execution(&command.execution) {
            Err(failure) => ModelAdapterExecutionOutcome::Failed(failure),
            Ok(()) => match self.host.execute(command.execution.clone()).await {
                Ok(outcome) => outcome,
                Err(failure) => ModelAdapterExecutionOutcome::Failed(host_failure(
                    failure,
                    &command.execution,
                    now,
                )),
            },
        };
        let outcome = match adapter_outcome {
            ModelAdapterExecutionOutcome::Succeeded(success) => {
                match self
                    .materializer
                    .materialize(&command.execution, success)
                    .await
                {
                    Ok(output) => ModelDispatchOutcome::Succeeded(Box::new(output)),
                    Err(mut failure) => {
                        // A normalized Provider response already exists. Replaying the Provider
                        // cannot repair local Value/Artifact projection and may duplicate cost.
                        failure.class = ModelAdapterFailureClass::Permanent;
                        failure.request_sent = true;
                        failure.retry_at = None;
                        failure_outcome(&command.execution, failure, Utc::now())?
                    }
                }
            }
            ModelAdapterExecutionOutcome::Failed(failure) => {
                eprintln!(
                    "Model adapter execution failed: class={:?} safe_code={} request_sent={}",
                    failure.class, failure.safe_code, failure.request_sent
                );
                failure_outcome(&command.execution, failure, Utc::now())?
            }
        };
        let resume_mutations = matches!(
            &outcome,
            ModelDispatchOutcome::Succeeded(output) if output.response.tool_intents.is_empty()
        )
        .then_some(command.resume_mutations)
        .flatten();
        let failure_mutations = matches!(&outcome, ModelDispatchOutcome::PermanentFailure { .. })
            .then_some(command.failure_mutations)
            .flatten();
        let tool_continuation_mutations = matches!(
            &outcome,
            ModelDispatchOutcome::Succeeded(output) if !output.response.tool_intents.is_empty()
        )
        .then_some(command.tool_continuation_mutations)
        .flatten();
        Ok(PreparedModelAdapterCommit {
            command: CommitModelOutcome {
                audit: command.audit,
                model_turn_id: command.execution.model_turn_id.clone(),
                job_id: command.execution.job_id.clone(),
                expected_turn_version: command.expected_turn_version,
                fence: command.fence,
                usage_reservation_id: command.usage_reservation_id,
                quota_entry_ids: command.quota_entry_ids,
                request: *command.execution.request,
                outcome,
                resume_mutations,
                failure_mutations,
                tool_continuation_mutations,
            },
        })
    }

    pub async fn commit(
        &self,
        prepared: PreparedModelAdapterCommit,
    ) -> Result<CommandOutcome<A::Record>, ModelAdapterWorkerError<A::Error>> {
        self.authority
            .commit_model_outcome(prepared.command)
            .await
            .map_err(ModelAdapterWorkerError::Authority)
    }
}

fn failure_outcome(
    execution: &ModelAdapterExecutionRequest,
    mut failure: ModelAdapterFailure,
    now: DateTime<Utc>,
) -> Result<ModelDispatchOutcome, ModelAdapterWorkerContractError> {
    failure
        .validate_for(execution, now)
        .map_err(|_| ModelAdapterWorkerContractError::InvalidFailure)?;
    if execution.attempt_no >= execution.attempt_limit
        && matches!(
            failure.class,
            ModelAdapterFailureClass::RetryableBeforeDispatch
                | ModelAdapterFailureClass::RetryableAfterDispatch
        )
    {
        failure.class = ModelAdapterFailureClass::Permanent;
        failure.retry_at = None;
    }
    let measurement = failure_measurement(execution, &failure)?;
    let retryable = matches!(
        failure.class,
        ModelAdapterFailureClass::RetryableBeforeDispatch
            | ModelAdapterFailureClass::RetryableAfterDispatch
    ) && execution.attempt_no < execution.attempt_limit;
    let safe_failure = safe_model_failure(&failure, retryable);
    if retryable {
        Ok(ModelDispatchOutcome::RetryableFailure {
            failure: safe_failure,
            retry_at: failure
                .retry_at
                .ok_or(ModelAdapterWorkerContractError::InvalidFailure)?,
            measurement,
        })
    } else {
        Ok(ModelDispatchOutcome::PermanentFailure {
            failure: safe_failure,
            measurement,
        })
    }
}

fn safe_model_failure(failure: &ModelAdapterFailure, retryable: bool) -> SafeModelFailure {
    let mut safe = model_failure(
        FailureClass::External,
        if retryable {
            Retryability::SafeWithinPolicy
        } else {
            Retryability::Never
        },
    );
    safe.failure.safe_message = Some(failure.safe_message.clone());
    safe.safe_code.clone_from(&failure.safe_code);
    safe.evidence_digest.clone_from(&failure.evidence_digest);
    safe
}

fn failure_measurement(
    execution: &ModelAdapterExecutionRequest,
    failure: &ModelAdapterFailure,
) -> Result<ModelAttemptMeasurement, ModelAdapterWorkerContractError> {
    let usage = if failure.request_sent {
        Some(conservative_usage(execution)?)
    } else {
        None
    };
    Ok(ModelAttemptMeasurement {
        usage,
        observation: ModelObservation {
            request_sent: failure.request_sent,
            provider_response_digest: None,
            actual_model_identity: None,
            model_fingerprint: None,
            possible_duplicate_charge: failure.request_sent,
            stream_delta_count: 0,
            stream_bytes: 0,
        },
    })
}

fn conservative_usage(
    execution: &ModelAdapterExecutionRequest,
) -> Result<ModelUsage, ModelAdapterWorkerContractError> {
    let profile = &execution.profile;
    let provider_reported_cost = profile
        .usage
        .cost_currency
        .as_ref()
        .map(|currency| {
            let microunits = i64::try_from(execution.quota_ceiling.cost_microunits)
                .map_err(|_| ModelAdapterWorkerContractError::InvalidExecutionContract)?;
            DecimalMoney::new(currency.clone(), microunits, 6)
                .map_err(|_| ModelAdapterWorkerContractError::InvalidExecutionContract)
        })
        .transpose()?;
    Ok(ModelUsage {
        input_tokens: Some(execution.request.input_token_estimate),
        output_tokens: Some(u64::from(execution.request.max_output_tokens)),
        cached_input_tokens: profile.usage.reports_cached_input_tokens.then_some(0),
        reasoning_tokens: profile.usage.reports_reasoning_tokens.then_some(0),
        provider_reported_cost,
        accounting_quality: AccountingQuality::Reconciled,
    })
}

fn host_failure(
    failure: ModelAdapterHostError,
    execution: &ModelAdapterExecutionRequest,
    now: DateTime<Utc>,
) -> ModelAdapterFailure {
    let retry_at = (failure == ModelAdapterHostError::AdapterNotInstalled
        && execution.attempt_no < execution.attempt_limit)
        .then(|| now + chrono::Duration::milliseconds(250))
        .filter(|retry_at| *retry_at < execution.request.deadline);
    let (class, request_sent) = match failure {
        ModelAdapterHostError::AdapterNotInstalled if retry_at.is_some() => {
            (ModelAdapterFailureClass::RetryableBeforeDispatch, false)
        }
        ModelAdapterHostError::AdapterNotInstalled => (ModelAdapterFailureClass::Permanent, false),
        ModelAdapterHostError::InvalidNormalizedStream
        | ModelAdapterHostError::InvalidNormalizedResponse
        | ModelAdapterHostError::InvalidAdapterFailure => {
            (ModelAdapterFailureClass::Permanent, true)
        }
        ModelAdapterHostError::InvalidInstalledAdapter
        | ModelAdapterHostError::DuplicateInstalledAdapter
        | ModelAdapterHostError::InvalidExecutionContract
        | ModelAdapterHostError::InvalidCancelRequest => {
            (ModelAdapterFailureClass::RejectedBeforeDispatch, false)
        }
    };
    let code = match failure {
        ModelAdapterHostError::InvalidInstalledAdapter => "model_invalid_installed_adapter",
        ModelAdapterHostError::DuplicateInstalledAdapter => "model_duplicate_installed_adapter",
        ModelAdapterHostError::AdapterNotInstalled => "model_adapter_not_installed",
        ModelAdapterHostError::InvalidExecutionContract => "model_invalid_execution_contract",
        ModelAdapterHostError::InvalidCancelRequest => "model_invalid_cancel_request",
        ModelAdapterHostError::InvalidAdapterFailure => "model_invalid_adapter_failure",
        ModelAdapterHostError::InvalidNormalizedStream => "model_invalid_normalized_stream",
        ModelAdapterHostError::InvalidNormalizedResponse => "model_invalid_normalized_response",
    };
    ModelAdapterFailure {
        class,
        safe_code: code.to_owned(),
        safe_message: failure.to_string(),
        evidence_digest: static_digest(code),
        request_sent,
        retry_at,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelAdapterWorkerContractError {
    InvalidCommand,
    InvalidExecutionContract,
    InvalidFailure,
}

impl fmt::Display for ModelAdapterWorkerContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidCommand => "Model adapter worker command is invalid",
            Self::InvalidExecutionContract => "Model adapter worker execution contract is invalid",
            Self::InvalidFailure => "Model adapter worker failure evidence is invalid",
        })
    }
}

impl Error for ModelAdapterWorkerContractError {}

#[derive(Debug)]
pub enum ModelAdapterWorkerError<E> {
    Contract(ModelAdapterWorkerContractError),
    Authority(E),
}

impl<E: fmt::Display> fmt::Display for ModelAdapterWorkerError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(failure) => failure.fmt(formatter),
            Self::Authority(failure) => write!(formatter, "Model authority failed: {failure}"),
        }
    }
}

impl<E: Error + 'static> Error for ModelAdapterWorkerError<E> {}
