//! Fail-closed Provider adapter host for Platform v1 Model Workers.
//!
//! The host resolves only process-installed adapters by their complete signed descriptor. It
//! consumes normalized frames, enforces request/stream deadlines and never owns durable
//! ModelTurn, Job, quota or Run state. Provider SDK and wire types remain behind
//! [`ModelProviderAdapter`].

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{stream::BoxStream, FutureExt, StreamExt};
use insight_platform_contracts::{
    canonical_digest, ExactDeploymentRef, ResourceId, ResourceKind, Sha256Digest,
};
use insight_platform_models::execution::ModelAdapterExecutionRequest;
use insight_platform_models::{
    CanonicalModelResponse, ModelStreamAcceptance, ModelStreamAccumulator, ModelStreamEvidence,
    ModelTurnLimits, NormalizedModelFrame,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{btree_map::Entry, BTreeMap},
    error::Error,
    fmt,
    panic::AssertUnwindSafe,
    sync::Arc,
    time::Duration,
};

pub const MAX_MODEL_ADAPTER_SAFE_CODE_BYTES: usize = 128;
pub const MAX_MODEL_ADAPTER_SAFE_MESSAGE_BYTES: usize = 512;

mod anthropic_messages;
mod openai_responses;
mod provider_broker;
mod provider_sse;
mod provider_wire;
mod worker;
pub use anthropic_messages::*;
pub use openai_responses::*;
pub use provider_broker::*;
pub use provider_sse::*;
pub use provider_wire::*;
pub use worker::*;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct InstalledModelAdapterDescriptor {
    pub qualified_name: String,
    pub worker_manifest_digest: Sha256Digest,
    pub adapter_contract_digest: Sha256Digest,
}

impl From<&insight_platform_contracts::InstalledModelAdapter> for InstalledModelAdapterDescriptor {
    fn from(value: &insight_platform_contracts::InstalledModelAdapter) -> Self {
        Self {
            qualified_name: value.qualified_name.clone(),
            worker_manifest_digest: value.worker_manifest_digest.clone(),
            adapter_contract_digest: value.adapter_contract_digest.clone(),
        }
    }
}

/// A safe failure is the only adapter error allowed across the Provider boundary.
///
/// `request_sent` is explicit because retries after dispatch can duplicate cost and data transfer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelAdapterFailure {
    pub class: ModelAdapterFailureClass,
    pub safe_code: String,
    pub safe_message: String,
    pub evidence_digest: Sha256Digest,
    pub request_sent: bool,
    pub retry_at: Option<DateTime<Utc>>,
}

impl ModelAdapterFailure {
    /// Validates the provider-boundary failure without requiring the full execution contract.
    ///
    /// This is used by an independently deployed Worker when it decodes an Egress response. The
    /// Worker still validates the request against the complete execution contract before dispatch;
    /// this second check prevents a compromised or stale Egress process from returning malformed
    /// retry instructions or unsafe text.
    pub fn validate_wire_shape(
        &self,
        deadline: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<(), ModelAdapterHostError> {
        let retryable = matches!(
            self.class,
            ModelAdapterFailureClass::RetryableBeforeDispatch
                | ModelAdapterFailureClass::RetryableAfterDispatch
        );
        if !stable_code(&self.safe_code)
            || self.safe_message.is_empty()
            || self.safe_message.len() > MAX_MODEL_ADAPTER_SAFE_MESSAGE_BYTES
            || self.safe_message.chars().any(char::is_control)
            || matches!(
                self.class,
                ModelAdapterFailureClass::RejectedBeforeDispatch
                    | ModelAdapterFailureClass::RetryableBeforeDispatch
            ) && self.request_sent
            || (self.class == ModelAdapterFailureClass::RetryableAfterDispatch
                && !self.request_sent)
            || self.retry_at.is_some() != retryable
            || self
                .retry_at
                .is_some_and(|retry_at| retry_at <= now || retry_at >= deadline)
        {
            return Err(ModelAdapterHostError::InvalidAdapterFailure);
        }
        Ok(())
    }

    pub fn validate_for(
        &self,
        request: &ModelAdapterExecutionRequest,
        now: DateTime<Utc>,
    ) -> Result<(), ModelAdapterHostError> {
        self.validate_wire_shape(request.request.deadline, now)
    }

    fn retryable_after_dispatch(code: &str, request: &ModelAdapterExecutionRequest) -> Self {
        let now = Utc::now();
        // Keep the bounded retry intent valid across the immediate Host-to-Worker validation;
        // durable lease recovery handles a later lost commit.
        let retry_at = now + chrono::Duration::milliseconds(250);
        let can_retry =
            request.attempt_no < request.attempt_limit && retry_at < request.request.deadline;
        Self {
            class: if can_retry {
                ModelAdapterFailureClass::RetryableAfterDispatch
            } else {
                ModelAdapterFailureClass::Permanent
            },
            safe_code: code.to_owned(),
            safe_message: "Model Provider completion could not be observed".to_owned(),
            evidence_digest: static_digest(code),
            request_sent: true,
            retry_at: can_retry.then_some(retry_at),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelAdapterFailureClass {
    RejectedBeforeDispatch,
    RetryableBeforeDispatch,
    RetryableAfterDispatch,
    Permanent,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModelAdapterSuccess {
    pub response: Box<CanonicalModelResponse>,
    pub stream_evidence: ModelStreamEvidence,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ModelAdapterExecutionOutcome {
    Succeeded(ModelAdapterSuccess),
    Failed(ModelAdapterFailure),
}

pub type NormalizedModelStream =
    BoxStream<'static, Result<NormalizedModelFrame, ModelAdapterFailure>>;

#[async_trait]
pub trait ModelProviderAdapter: Send + Sync {
    fn descriptor(&self) -> InstalledModelAdapterDescriptor;

    async fn invoke(
        &self,
        request: ModelAdapterExecutionRequest,
    ) -> Result<NormalizedModelStream, ModelAdapterFailure>;

    async fn cancel(
        &self,
        request: ModelAdapterCancelRequest,
    ) -> Result<ModelAdapterCancelOutcome, ModelAdapterFailure>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelAdapterCancelRequest {
    pub tenant_id: ResourceId,
    pub model_turn_id: ResourceId,
    pub job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub provider_deployment: ExactDeploymentRef,
    pub attempt_no: u32,
    pub lease_generation: u64,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelAdapterCancelOutcome {
    Accepted,
    AlreadyTerminal,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelAdapterCancelExecutionOutcome {
    Completed(ModelAdapterCancelOutcome),
    Failed(ModelAdapterFailure),
}

impl ModelAdapterCancelRequest {
    pub fn validate_shape_at(&self, now: DateTime<Utc>) -> Result<(), ModelAdapterHostError> {
        self.provider_deployment
            .validate()
            .map_err(|_| ModelAdapterHostError::InvalidCancelRequest)?;
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.model_turn_id.kind() != ResourceKind::ModelTurn
            || self.job_id.kind() != ResourceKind::Job
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.provider_deployment.resource_kind != ResourceKind::ModelProviderDeployment
            || self.attempt_no == 0
            || self.lease_generation == 0
            || self.deadline <= now
        {
            return Err(ModelAdapterHostError::InvalidCancelRequest);
        }
        Ok(())
    }

    pub fn identity(&self, protocol: ModelProviderWireProtocol) -> ModelProviderRequestIdentity {
        ModelProviderRequestIdentity {
            protocol,
            tenant_id: self.tenant_id.clone(),
            model_turn_id: self.model_turn_id.clone(),
            job_id: self.job_id.clone(),
            worker_process_generation_id: self.worker_process_generation_id.clone(),
            provider_deployment_id: self.provider_deployment.deployment_id.clone(),
            provider_deployment_digest: self.provider_deployment.deployment_digest.clone(),
            attempt_no: self.attempt_no,
            lease_generation: self.lease_generation,
        }
    }

    fn validate_for(
        &self,
        execution: &ModelAdapterExecutionRequest,
        now: DateTime<Utc>,
    ) -> Result<(), ModelAdapterHostError> {
        self.validate_shape_at(now)?;
        if self.tenant_id != execution.tenant_id
            || self.model_turn_id != execution.model_turn_id
            || self.job_id != execution.job_id
            || self.worker_process_generation_id != execution.worker_process_generation_id
            || self.provider_deployment != execution.provider_deployment
            || self.attempt_no != execution.attempt_no
            || self.lease_generation != execution.lease_generation
            || self.deadline <= now
            || self.deadline > execution.request.deadline
        {
            return Err(ModelAdapterHostError::InvalidCancelRequest);
        }
        Ok(())
    }
}

#[async_trait]
pub trait ModelLiveDeltaSink: Send + Sync {
    async fn publish(&self, execution: &ModelAdapterExecutionRequest, frame: &NormalizedModelFrame);
}

#[derive(Default)]
pub struct DropModelLiveDeltas;

#[async_trait]
impl ModelLiveDeltaSink for DropModelLiveDeltas {
    async fn publish(
        &self,
        _execution: &ModelAdapterExecutionRequest,
        _frame: &NormalizedModelFrame,
    ) {
    }
}

#[derive(Default, Clone)]
pub struct InstalledModelAdapterRegistry {
    adapters: BTreeMap<(String, Sha256Digest), Arc<dyn ModelProviderAdapter>>,
}

impl InstalledModelAdapterRegistry {
    pub fn install(
        &mut self,
        adapter: Arc<dyn ModelProviderAdapter>,
    ) -> Result<(), ModelAdapterHostError> {
        let descriptor = adapter.descriptor();
        let contract = insight_platform_contracts::InstalledModelAdapter {
            qualified_name: descriptor.qualified_name.clone(),
            worker_manifest_digest: descriptor.worker_manifest_digest.clone(),
            adapter_contract_digest: descriptor.adapter_contract_digest.clone(),
        };
        contract
            .validate()
            .map_err(|_| ModelAdapterHostError::InvalidInstalledAdapter)?;
        match self.adapters.entry((
            descriptor.qualified_name,
            descriptor.adapter_contract_digest,
        )) {
            Entry::Vacant(entry) => {
                entry.insert(adapter);
                Ok(())
            }
            Entry::Occupied(_) => Err(ModelAdapterHostError::DuplicateInstalledAdapter),
        }
    }

    fn resolve(
        &self,
        descriptor: &InstalledModelAdapterDescriptor,
    ) -> Result<Arc<dyn ModelProviderAdapter>, ModelAdapterHostError> {
        self.adapters
            .get(&(
                descriptor.qualified_name.clone(),
                descriptor.adapter_contract_digest.clone(),
            ))
            .cloned()
            .ok_or(ModelAdapterHostError::AdapterNotInstalled)
    }
}

pub struct ModelAdapterHost {
    registry: InstalledModelAdapterRegistry,
    live_sink: Arc<dyn ModelLiveDeltaSink>,
    limits: ModelTurnLimits,
}

impl ModelAdapterHost {
    pub fn new(
        registry: InstalledModelAdapterRegistry,
        live_sink: Arc<dyn ModelLiveDeltaSink>,
        limits: ModelTurnLimits,
    ) -> Self {
        Self {
            registry,
            live_sink,
            limits,
        }
    }

    pub async fn execute(
        &self,
        request: ModelAdapterExecutionRequest,
    ) -> Result<ModelAdapterExecutionOutcome, ModelAdapterHostError> {
        let now = Utc::now();
        request.validate_at(now, self.limits)?;
        let descriptor = InstalledModelAdapterDescriptor::from(&request.provider.installed_adapter);
        let adapter = self.registry.resolve(&descriptor)?;
        let total_timeout = bounded_timeout(
            now,
            request.request.deadline,
            request.provider.request_limits.total_timeout_milliseconds,
        )?;
        let future = AssertUnwindSafe(self.execute_inner(adapter, request.clone())).catch_unwind();
        match tokio::time::timeout(total_timeout, future).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_)) => Ok(ModelAdapterExecutionOutcome::Failed(
                ModelAdapterFailure::retryable_after_dispatch("model_adapter_panic", &request),
            )),
            Err(_) => Ok(ModelAdapterExecutionOutcome::Failed(
                ModelAdapterFailure::retryable_after_dispatch("model_total_timeout", &request),
            )),
        }
    }

    pub async fn cancel(
        &self,
        execution: &ModelAdapterExecutionRequest,
        request: ModelAdapterCancelRequest,
    ) -> Result<ModelAdapterCancelExecutionOutcome, ModelAdapterHostError> {
        let now = Utc::now();
        execution.validate_at(now, self.limits)?;
        request.validate_for(execution, now)?;
        let descriptor =
            InstalledModelAdapterDescriptor::from(&execution.provider.installed_adapter);
        let adapter = self.registry.resolve(&descriptor)?;
        let timeout = bounded_timeout(
            now,
            request.deadline,
            execution.provider.request_limits.total_timeout_milliseconds,
        )?;
        let future = AssertUnwindSafe(adapter.cancel(request)).catch_unwind();
        match tokio::time::timeout(timeout, future).await {
            Ok(Ok(Ok(outcome))) => Ok(ModelAdapterCancelExecutionOutcome::Completed(outcome)),
            Ok(Ok(Err(failure))) => {
                failure.validate_for(execution, Utc::now())?;
                Ok(ModelAdapterCancelExecutionOutcome::Failed(failure))
            }
            Ok(Err(_)) => Ok(ModelAdapterCancelExecutionOutcome::Failed(
                ModelAdapterFailure::retryable_after_dispatch(
                    "model_cancel_adapter_panic",
                    execution,
                ),
            )),
            Err(_) => Ok(ModelAdapterCancelExecutionOutcome::Failed(
                ModelAdapterFailure::retryable_after_dispatch("model_cancel_timeout", execution),
            )),
        }
    }

    async fn execute_inner(
        &self,
        adapter: Arc<dyn ModelProviderAdapter>,
        request: ModelAdapterExecutionRequest,
    ) -> Result<ModelAdapterExecutionOutcome, ModelAdapterHostError> {
        let connect_timeout =
            Duration::from_millis(request.provider.request_limits.connect_timeout_milliseconds);
        let stream =
            match tokio::time::timeout(connect_timeout, adapter.invoke(request.clone())).await {
                Err(_) => {
                    return Ok(ModelAdapterExecutionOutcome::Failed(
                        ModelAdapterFailure::retryable_after_dispatch(
                            "model_connect_timeout",
                            &request,
                        ),
                    ));
                }
                Ok(result) => match result {
                    Ok(stream) => stream,
                    Err(failure) => return checked_failure(failure, &request),
                },
            };
        self.consume_stream(stream, &request).await
    }

    async fn consume_stream(
        &self,
        mut stream: NormalizedModelStream,
        request: &ModelAdapterExecutionRequest,
    ) -> Result<ModelAdapterExecutionOutcome, ModelAdapterHostError> {
        let mut accumulator = ModelStreamAccumulator::new(
            request.model_turn_id.clone(),
            request.attempt_no,
            request.lease_generation,
            self.limits,
        )
        .map_err(|_| ModelAdapterHostError::InvalidExecutionContract)?;
        let first_byte = Duration::from_millis(
            request
                .provider
                .request_limits
                .first_byte_timeout_milliseconds,
        );
        let idle = Duration::from_millis(request.provider.request_limits.idle_timeout_milliseconds);
        let mut next_timeout = first_byte;
        loop {
            let item = match tokio::time::timeout(next_timeout, stream.next()).await {
                Ok(Some(item)) => item,
                Ok(None) => {
                    return Ok(ModelAdapterExecutionOutcome::Failed(
                        ModelAdapterFailure::retryable_after_dispatch(
                            "model_stream_missing_terminal",
                            request,
                        ),
                    ));
                }
                Err(_) => {
                    return Ok(ModelAdapterExecutionOutcome::Failed(
                        ModelAdapterFailure::retryable_after_dispatch(
                            "model_stream_idle_timeout",
                            request,
                        ),
                    ));
                }
            };
            next_timeout = idle;
            let frame = match item {
                Ok(frame) => frame,
                Err(failure) => return checked_failure(failure, request),
            };
            let delta_bytes = serde_json::to_vec(&frame.delta)
                .map_err(|_| ModelAdapterHostError::InvalidNormalizedStream)?
                .len();
            if delta_bytes
                > usize::try_from(request.provider.request_limits.maximum_stream_delta_bytes)
                    .map_err(|_| ModelAdapterHostError::InvalidExecutionContract)?
            {
                return Err(ModelAdapterHostError::InvalidNormalizedStream);
            }
            let live_frame = frame.clone();
            match accumulator
                .accept(frame)
                .map_err(|_| ModelAdapterHostError::InvalidNormalizedStream)?
            {
                ModelStreamAcceptance::Live { .. } => {
                    self.live_sink.publish(request, &live_frame).await
                }
                ModelStreamAcceptance::Terminal { response, evidence } => {
                    response
                        .validate_for(
                            &request.request,
                            &request.provider,
                            &request.profile,
                            self.limits,
                        )
                        .map_err(|_| ModelAdapterHostError::InvalidNormalizedResponse)?;
                    return Ok(ModelAdapterExecutionOutcome::Succeeded(
                        ModelAdapterSuccess {
                            response,
                            stream_evidence: evidence,
                        },
                    ));
                }
            }
        }
    }
}

fn checked_failure(
    mut failure: ModelAdapterFailure,
    request: &ModelAdapterExecutionRequest,
) -> Result<ModelAdapterExecutionOutcome, ModelAdapterHostError> {
    failure.validate_for(request, Utc::now())?;
    if request.attempt_no >= request.attempt_limit
        && matches!(
            failure.class,
            ModelAdapterFailureClass::RetryableBeforeDispatch
                | ModelAdapterFailureClass::RetryableAfterDispatch
        )
    {
        failure.class = ModelAdapterFailureClass::Permanent;
        failure.retry_at = None;
    }
    Ok(ModelAdapterExecutionOutcome::Failed(failure))
}

fn bounded_timeout(
    now: DateTime<Utc>,
    deadline: DateTime<Utc>,
    configured_milliseconds: u64,
) -> Result<Duration, ModelAdapterHostError> {
    let remaining = u64::try_from((deadline - now).num_milliseconds())
        .map_err(|_| ModelAdapterHostError::InvalidExecutionContract)?;
    Ok(Duration::from_millis(
        remaining.min(configured_milliseconds),
    ))
}

fn stable_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_MODEL_ADAPTER_SAFE_CODE_BYTES
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'.' | b'-')
        })
}

fn static_digest(domain: &str) -> Sha256Digest {
    canonical_digest(&serde_json::json!({"domain": domain, "schema_version": 1}))
        .expect("static Model adapter evidence is canonical")
        .parse()
        .expect("canonical digest is SHA-256")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelAdapterHostError {
    InvalidInstalledAdapter,
    DuplicateInstalledAdapter,
    AdapterNotInstalled,
    InvalidExecutionContract,
    InvalidCancelRequest,
    InvalidAdapterFailure,
    InvalidNormalizedStream,
    InvalidNormalizedResponse,
}

impl fmt::Display for ModelAdapterHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidInstalledAdapter => "installed Model adapter descriptor is invalid",
            Self::DuplicateInstalledAdapter => "Model adapter descriptor is already installed",
            Self::AdapterNotInstalled => "exact Model adapter is not installed",
            Self::InvalidExecutionContract => "Model adapter execution contract is invalid",
            Self::InvalidCancelRequest => "Model adapter cancel request is invalid",
            Self::InvalidAdapterFailure => "Model adapter failure is invalid",
            Self::InvalidNormalizedStream => "normalized Model stream is invalid",
            Self::InvalidNormalizedResponse => "normalized Model response is invalid",
        })
    }
}

impl Error for ModelAdapterHostError {}

#[cfg(test)]
mod tests;

impl From<insight_platform_models::ModelTurnError> for ModelAdapterHostError {
    fn from(_: insight_platform_models::ModelTurnError) -> Self {
        Self::InvalidExecutionContract
    }
}
