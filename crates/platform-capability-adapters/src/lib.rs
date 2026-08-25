//! Fail-closed dispatch boundary for Platform v1 Capability Workers.
//!
//! This crate performs no registry discovery and stores no durable state. A caller must supply the
//! exact execution contract returned by the PostgreSQL claim transaction. Native implementations
//! are selected only from the process-installed registry; remote implementations are delegated to
//! role-scoped transport ports that consume the same closed contract and Deployment binding.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::FutureExt;
use insight_platform_contracts::{
    canonical_digest, CapabilityBackendBinding, CapabilityBackendContract, CapabilityBackendKind,
    Effect, Failure, FailureClass, FailureCode, FailureSource, PlatformFailureCode, ResourceId,
    ResourceKind, Retryability, Sha256Digest,
};
use insight_platform_invocations::{
    CapabilityExecutionContract, CapabilityExecutionInput, CapabilityExecutionInputMaterial,
    CapabilityInputAction, CapabilityUncertainty, DispatchOutcome, EncryptedRemoteState,
    InvocationValueStorage, SafeBackendFailure,
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

mod http;
pub use http::*;
mod grpc;
pub use grpc::*;
mod mcp;
pub use mcp::*;
mod worker;
pub use worker::*;

pub const MAX_ADAPTER_FAILURE_CODE_BYTES: usize = 128;
pub const MAX_ADAPTER_SAFE_MESSAGE_BYTES: usize = 512;

/// Exact process-local ownership key for one physical Capability request.
///
/// PostgreSQL remains the durable Invocation/Job authority. This identity exists only so an
/// adapter process can reject duplicate live requests and so a stale control signal cannot cancel
/// a replacement worker's execution or connection.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityTransportRequestIdentity {
    pub backend_kind: CapabilityBackendKind,
    pub tenant_id: ResourceId,
    pub invocation_id: ResourceId,
    pub job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub capability_deployment_id: ResourceId,
    pub capability_deployment_digest: Sha256Digest,
    pub physical_attempt: u32,
    pub lease_generation: u64,
}

impl CapabilityTransportRequestIdentity {
    pub fn from_adapter_request(
        request: &CapabilityAdapterRequest,
        backend_kind: CapabilityBackendKind,
    ) -> Self {
        Self {
            backend_kind,
            tenant_id: request.tenant_id.clone(),
            invocation_id: request.invocation_id.clone(),
            job_id: request.job_id.clone(),
            worker_process_generation_id: request.worker_process_generation_id.clone(),
            capability_deployment_id: request.execution.deployment.deployment_id.clone(),
            capability_deployment_digest: request.execution.deployment.deployment_digest.clone(),
            physical_attempt: request.physical_attempt,
            lease_generation: request.lease_generation,
        }
    }

    pub fn validate(&self) -> Result<(), CapabilityDispatchError> {
        if !matches!(
            self.backend_kind,
            CapabilityBackendKind::Native
                | CapabilityBackendKind::Http
                | CapabilityBackendKind::Grpc
                | CapabilityBackendKind::Mcp
        ) || self.tenant_id.kind() != ResourceKind::Tenant
            || self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.job_id.kind() != ResourceKind::Job
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.capability_deployment_id.kind() != ResourceKind::CapabilityDeployment
            || self.physical_attempt == 0
            || self.lease_generation == 0
        {
            return Err(CapabilityDispatchError::InvalidRequest);
        }
        Ok(())
    }

    pub fn evidence_digest(&self) -> Sha256Digest {
        let value = serde_json::to_value(self).expect("transport identity is serializable");
        canonical_digest(&value)
            .expect("validated transport identity is canonical")
            .parse()
            .expect("canonical identity digest is SHA-256")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityTransportCancelRequest {
    pub identity: CapabilityTransportRequestIdentity,
    pub deadline: DateTime<Utc>,
}

impl CapabilityTransportCancelRequest {
    pub fn from_adapter_request(
        request: &CapabilityAdapterRequest,
        backend_kind: CapabilityBackendKind,
        deadline: DateTime<Utc>,
    ) -> Self {
        Self {
            identity: CapabilityTransportRequestIdentity::from_adapter_request(
                request,
                backend_kind,
            ),
            deadline,
        }
    }

    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), CapabilityDispatchError> {
        self.identity.validate()?;
        if self.deadline <= now {
            return Err(CapabilityDispatchError::InvalidCancelRequest);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityTransportCancelOutcome {
    Accepted,
    AlreadyTerminal,
    Unsupported,
}

/// Durable backend continuation recovered from the claimed Capability Job.
///
/// The state remains encrypted and opaque at this boundary. Only the exact backend codec may
/// unseal it, while PostgreSQL remains authoritative for the physical attempt and the current Job
/// fence. `resume_input` contains bounded RunValue material only after an InputRequired winner has
/// committed it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityAdapterContinuation {
    pub encrypted_remote_state: EncryptedRemoteState,
    pub external_identity_digest: Option<Sha256Digest>,
    pub resume_input: Option<CapabilityExecutionInput>,
    pub resume_input_action: Option<CapabilityInputAction>,
    pub poll_count: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityAdapterRequest {
    pub tenant_id: ResourceId,
    pub invocation_id: ResourceId,
    pub job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub worker_manifest_digest: Sha256Digest,
    pub lease_generation: u64,
    pub physical_attempt: u32,
    pub attempt_limit: u32,
    pub admission_digest: Sha256Digest,
    pub output_schema_digest: Sha256Digest,
    pub idempotency_key_digest: Sha256Digest,
    pub effect: Effect,
    pub idempotency: insight_platform_contracts::CapabilityIdempotencyKind,
    pub deadline: DateTime<Utc>,
    pub execution: CapabilityExecutionContract,
    pub input: CapabilityExecutionInput,
    pub continuation: Option<CapabilityAdapterContinuation>,
    pub mcp_runtime: Option<insight_platform_invocations::McpCapabilityRuntimeBinding>,
}

impl CapabilityAdapterRequest {
    /// Validates the immutable identity, execution closure and bounded input independently of the
    /// dispatch clock. Cancellation and cleanup must still be able to address the exact physical
    /// request after its execution deadline has elapsed.
    pub fn validate_shape(&self) -> Result<(), CapabilityDispatchError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.invocation_id.kind() != ResourceKind::CapabilityInvocation
            || self.job_id.kind() != ResourceKind::Job
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.lease_generation == 0
            || self.physical_attempt == 0
            || self.attempt_limit == 0
            || self.physical_attempt > self.attempt_limit
            || self.execution.validate().is_err()
            || (self.execution.implementation.backend_kind == CapabilityBackendKind::Mcp)
                != self.mcp_runtime.is_some()
            || self
                .mcp_runtime
                .as_ref()
                .is_some_and(|binding| binding.validate().is_err())
            || self.continuation.as_ref().is_some_and(|continuation| {
                let features = &self.execution.implementation.features;
                (!features.deferred && !features.input_required)
                    || continuation
                        .encrypted_remote_state
                        .validate(features.max_remote_state_bytes)
                        .is_err()
                    || continuation.poll_count > features.max_poll_count
                    || continuation.resume_input.as_ref().is_some_and(|input| {
                        !features.input_required
                            || validate_execution_input(
                                input,
                                self.execution
                                    .implementation
                                    .backend_limits
                                    .maximum_request_bytes,
                            )
                            .is_err()
                    })
                    || match continuation.resume_input_action {
                        None => continuation.resume_input.is_some(),
                        Some(CapabilityInputAction::Accept) => continuation.resume_input.is_none(),
                        Some(CapabilityInputAction::Decline | CapabilityInputAction::Cancel) => {
                            continuation.resume_input.is_some()
                        }
                    }
            })
        {
            return Err(CapabilityDispatchError::InvalidRequest);
        }
        validate_execution_input(
            &self.input,
            self.execution
                .implementation
                .backend_limits
                .maximum_request_bytes,
        )
    }

    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), CapabilityDispatchError> {
        self.validate_shape()?;
        if self.deadline <= now {
            return Err(CapabilityDispatchError::InvalidRequest);
        }
        Ok(())
    }
}

fn validate_execution_input(
    input: &CapabilityExecutionInput,
    maximum_request_bytes: u32,
) -> Result<(), CapabilityDispatchError> {
    input
        .validate()
        .map_err(|_| CapabilityDispatchError::InvalidRequest)?;
    match (&input.exact.storage, &input.material) {
        (InvocationValueStorage::Inline, CapabilityExecutionInputMaterial::Inline { value }) => {
            let encoded =
                serde_json::to_vec(value).map_err(|_| CapabilityDispatchError::InvalidRequest)?;
            if encoded.len()
                > usize::try_from(maximum_request_bytes)
                    .map_err(|_| CapabilityDispatchError::InvalidRequest)?
            {
                return Err(CapabilityDispatchError::InvalidRequest);
            }
        }
        (
            InvocationValueStorage::Artifact { artifact },
            CapabilityExecutionInputMaterial::LinkedArtifact { .. },
        ) if artifact.byte_length() <= u64::from(maximum_request_bytes) => {}
        _ => return Err(CapabilityDispatchError::InvalidRequest),
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityAdapterFailureClass {
    RejectedBeforeDispatch,
    RetryableBeforeDispatch,
    RetryableAfterDispatch,
    Permanent,
    Uncertain,
    TimedOutUncertain,
    ContainedPanic,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityAdapterFailure {
    pub class: CapabilityAdapterFailureClass,
    pub safe_code: String,
    pub safe_message: String,
    pub evidence_digest: Sha256Digest,
    pub external_identity_digest: Option<Sha256Digest>,
}

impl CapabilityAdapterFailure {
    pub fn validate(&self) -> Result<(), CapabilityDispatchError> {
        if !stable_code(&self.safe_code)
            || self.safe_message.is_empty()
            || self.safe_message.len() > MAX_ADAPTER_SAFE_MESSAGE_BYTES
            || self.safe_message.chars().any(char::is_control)
            || matches!(
                self.class,
                CapabilityAdapterFailureClass::Uncertain
                    | CapabilityAdapterFailureClass::TimedOutUncertain
                    | CapabilityAdapterFailureClass::RetryableAfterDispatch
            ) != self.external_identity_digest.is_some()
        {
            return Err(CapabilityDispatchError::MalformedAdapterFailure);
        }
        Ok(())
    }

    fn contained_panic() -> Self {
        Self {
            class: CapabilityAdapterFailureClass::ContainedPanic,
            safe_code: "adapter_panic".to_owned(),
            safe_message: "Capability adapter terminated unexpectedly".to_owned(),
            evidence_digest: digest_domain("adapter_panic"),
            external_identity_digest: None,
        }
    }

    fn remote_timeout(request: &CapabilityAdapterRequest) -> Self {
        Self {
            class: CapabilityAdapterFailureClass::TimedOutUncertain,
            safe_code: "remote_timeout_uncertain".to_owned(),
            safe_message: "Remote Capability completion could not be observed before timeout"
                .to_owned(),
            evidence_digest: digest_domain("remote_timeout_uncertain"),
            external_identity_digest: Some(request.admission_digest.clone()),
        }
    }
}

pub fn adapter_failure_outcome(
    request: &CapabilityAdapterRequest,
    mut failure: CapabilityAdapterFailure,
    retry_at: Option<DateTime<Utc>>,
) -> Result<DispatchOutcome, CapabilityDispatchError> {
    failure.validate()?;
    if failure.class == CapabilityAdapterFailureClass::RetryableAfterDispatch
        && request.effect.risk_rank() >= Effect::IdempotentWrite.risk_rank()
        && request.idempotency == insight_platform_contracts::CapabilityIdempotencyKind::None
    {
        failure.class = CapabilityAdapterFailureClass::Uncertain;
    }
    match failure.class {
        CapabilityAdapterFailureClass::Uncertain
        | CapabilityAdapterFailureClass::TimedOutUncertain => {
            Ok(DispatchOutcome::Uncertain(CapabilityUncertainty {
                observation_digest: failure.evidence_digest,
                policy_path_digest: digest_domain("adapter_uncertain_effect_policy"),
                external_identity_digest: failure
                    .external_identity_digest
                    .ok_or(CapabilityDispatchError::MalformedAdapterFailure)?,
                manual: true,
            }))
        }
        CapabilityAdapterFailureClass::RetryableBeforeDispatch
        | CapabilityAdapterFailureClass::RetryableAfterDispatch => {
            let retry_at = retry_at.ok_or(CapabilityDispatchError::MissingRetryDeadline)?;
            Ok(DispatchOutcome::RetryableFailure {
                failure: SafeBackendFailure {
                    failure: platform_failure(&failure, Retryability::SafeWithinPolicy),
                    evidence_digest: failure.evidence_digest,
                },
                retry_at,
            })
        }
        CapabilityAdapterFailureClass::RejectedBeforeDispatch
        | CapabilityAdapterFailureClass::Permanent
        | CapabilityAdapterFailureClass::ContainedPanic => {
            Ok(DispatchOutcome::PermanentFailure(SafeBackendFailure {
                failure: platform_failure(&failure, Retryability::Never),
                evidence_digest: failure.evidence_digest,
            }))
        }
    }
}

fn platform_failure(failure: &CapabilityAdapterFailure, retryability: Retryability) -> Failure {
    Failure {
        code: FailureCode::Platform {
            code: PlatformFailureCode::CapabilityFailed,
        },
        class: if failure.class == CapabilityAdapterFailureClass::ContainedPanic {
            FailureClass::Platform
        } else {
            FailureClass::External
        },
        retryability,
        safe_message: Some(failure.safe_message.clone()),
        details_ref: None,
        source: FailureSource::Capability,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CapabilityAdapterResponse {
    pub outcome: DispatchOutcome,
}

#[async_trait]
pub trait NativeCapabilityAdapter: Send + Sync {
    fn descriptor(&self) -> InstalledNativeAdapter;

    async fn invoke(
        &self,
        request: &CapabilityAdapterRequest,
    ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure>;

    async fn cancel(
        &self,
        _request: CapabilityTransportCancelRequest,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
        Ok(CapabilityTransportCancelOutcome::Unsupported)
    }
}

#[async_trait]
pub trait CapabilityBackendPort: Send + Sync {
    fn kind(&self) -> CapabilityBackendKind;

    async fn invoke(
        &self,
        request: &CapabilityAdapterRequest,
    ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure>;

    async fn cancel(
        &self,
        _request: CapabilityTransportCancelRequest,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
        Ok(CapabilityTransportCancelOutcome::Unsupported)
    }

    async fn cancel_execution(
        &self,
        request: &CapabilityAdapterRequest,
        deadline: DateTime<Utc>,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
        self.cancel(CapabilityTransportCancelRequest::from_adapter_request(
            request,
            self.kind(),
            deadline,
        ))
        .await
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct InstalledNativeAdapter {
    pub adapter_id: String,
    pub adapter_version: String,
    pub module_digest: Sha256Digest,
    pub entrypoint_id: String,
}

#[derive(Default, Clone)]
pub struct InstalledNativeRegistry {
    adapters: BTreeMap<InstalledNativeAdapter, Arc<dyn NativeCapabilityAdapter>>,
}

impl InstalledNativeRegistry {
    pub fn install(
        &mut self,
        adapter: Arc<dyn NativeCapabilityAdapter>,
    ) -> Result<(), CapabilityDispatchError> {
        let descriptor = adapter.descriptor();
        if !stable_name(&descriptor.adapter_id)
            || !stable_name(&descriptor.adapter_version)
            || !stable_name(&descriptor.entrypoint_id)
        {
            return Err(CapabilityDispatchError::InvalidInstalledAdapter);
        }
        match self.adapters.entry(descriptor) {
            Entry::Vacant(entry) => {
                entry.insert(adapter);
                Ok(())
            }
            Entry::Occupied(_) => Err(CapabilityDispatchError::InvalidInstalledAdapter),
        }
    }

    fn resolve(
        &self,
        descriptor: &InstalledNativeAdapter,
    ) -> Result<&Arc<dyn NativeCapabilityAdapter>, CapabilityDispatchError> {
        self.adapters
            .get(descriptor)
            .ok_or(CapabilityDispatchError::NativeAdapterNotInstalled)
    }
}

pub struct CapabilityDispatcher {
    native: InstalledNativeRegistry,
    ports: BTreeMap<CapabilityBackendKind, Arc<dyn CapabilityBackendPort>>,
}

impl CapabilityDispatcher {
    pub fn new(native: InstalledNativeRegistry) -> Self {
        Self {
            native,
            ports: BTreeMap::new(),
        }
    }

    pub fn install_port(
        &mut self,
        port: Arc<dyn CapabilityBackendPort>,
    ) -> Result<(), CapabilityDispatchError> {
        let kind = port.kind();
        if kind == CapabilityBackendKind::Native {
            return Err(CapabilityDispatchError::InvalidBackendPort);
        }
        match self.ports.entry(kind) {
            Entry::Vacant(entry) => {
                entry.insert(port);
                Ok(())
            }
            Entry::Occupied(_) => Err(CapabilityDispatchError::InvalidBackendPort),
        }
    }

    pub async fn dispatch(
        &self,
        request: &CapabilityAdapterRequest,
    ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure> {
        let now = Utc::now();
        request.validate_at(now).map_err(contract_failure)?;
        let contract = &request.execution.implementation.backend_contract;
        let binding = &request.execution.deployment_closure.backend;
        binding
            .validate_for(contract)
            .map_err(|_| contract_failure(CapabilityDispatchError::BackendContractMismatch))?;
        let timeout = dispatch_timeout(request, now).map_err(contract_failure)?;
        let future =
            AssertUnwindSafe(self.dispatch_inner(request, contract, binding)).catch_unwind();
        match tokio::time::timeout(timeout, future).await {
            Ok(Ok(result)) => match result {
                Ok(response) => {
                    validate_response_size(request, &response).map_err(contract_failure)?;
                    Ok(response)
                }
                Err(failure) => {
                    failure.validate().map_err(contract_failure)?;
                    Err(failure)
                }
            },
            Ok(Err(_)) => Err(CapabilityAdapterFailure::contained_panic()),
            Err(_) if contract.kind() == CapabilityBackendKind::Native => {
                Err(CapabilityAdapterFailure {
                    class: CapabilityAdapterFailureClass::ContainedPanic,
                    safe_code: "native_timeout".to_owned(),
                    safe_message: "Native Capability exceeded its execution timeout".to_owned(),
                    evidence_digest: digest_domain("native_timeout"),
                    external_identity_digest: None,
                })
            }
            Err(_) => Err(CapabilityAdapterFailure::remote_timeout(request)),
        }
    }

    pub async fn cancel(
        &self,
        request: CapabilityTransportCancelRequest,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
        let now = Utc::now();
        request.validate_at(now).map_err(contract_failure)?;
        let kind = request.identity.backend_kind;
        let port = self
            .ports
            .get(&kind)
            .ok_or_else(|| contract_failure(CapabilityDispatchError::BackendPortNotInstalled))?;
        let timeout = (request.deadline - now)
            .to_std()
            .map_err(|_| contract_failure(CapabilityDispatchError::InvalidCancelRequest))?;
        let identity_digest = request.identity.evidence_digest();
        let future = AssertUnwindSafe(port.cancel(request)).catch_unwind();
        match tokio::time::timeout(timeout, future).await {
            Ok(Ok(Ok(outcome))) => Ok(outcome),
            Ok(Ok(Err(failure))) => {
                failure.validate().map_err(contract_failure)?;
                Err(failure)
            }
            Ok(Err(_)) => Err(CapabilityAdapterFailure::contained_panic()),
            Err(_) => Err(CapabilityAdapterFailure {
                class: CapabilityAdapterFailureClass::TimedOutUncertain,
                safe_code: "remote_cancel_timeout_uncertain".to_owned(),
                safe_message: "Remote Capability cancellation could not be observed before timeout"
                    .to_owned(),
                evidence_digest: digest_domain("remote_cancel_timeout_uncertain"),
                external_identity_digest: Some(identity_digest),
            }),
        }
    }

    /// Cancels the exact physical request selected by the immutable execution closure.
    ///
    /// Native adapters are selected from the process-installed manifest registry; HTTP/gRPC are
    /// delegated to their role-scoped transport ports. MCP delegates the exact encrypted remote
    /// Task continuation to its Host. Sandbox retains its own executor cancellation protocol.
    pub async fn cancel_execution(
        &self,
        execution: &CapabilityAdapterRequest,
        deadline: DateTime<Utc>,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
        execution.validate_shape().map_err(contract_failure)?;
        let kind = execution.execution.implementation.backend_kind;
        let request =
            CapabilityTransportCancelRequest::from_adapter_request(execution, kind, deadline);
        match (
            &execution.execution.implementation.backend_contract,
            &execution.execution.deployment_closure.backend,
        ) {
            (
                CapabilityBackendContract::Native(contract),
                CapabilityBackendBinding::Native {
                    worker_manifest_digest,
                    ..
                },
            ) => {
                if worker_manifest_digest != &execution.worker_manifest_digest {
                    return Err(contract_failure(
                        CapabilityDispatchError::WorkerManifestMismatch,
                    ));
                }
                request.validate_at(Utc::now()).map_err(contract_failure)?;
                let descriptor = InstalledNativeAdapter {
                    adapter_id: contract.adapter_id.clone(),
                    adapter_version: contract.adapter_version.clone(),
                    module_digest: contract.module_digest.clone(),
                    entrypoint_id: contract.entrypoint_id.clone(),
                };
                let adapter = self.native.resolve(&descriptor).map_err(contract_failure)?;
                let timeout = (deadline - Utc::now())
                    .to_std()
                    .map_err(|_| contract_failure(CapabilityDispatchError::InvalidCancelRequest))?;
                let future = AssertUnwindSafe(adapter.cancel(request)).catch_unwind();
                match tokio::time::timeout(timeout, future).await {
                    Ok(Ok(Ok(outcome))) => Ok(outcome),
                    Ok(Ok(Err(failure))) => {
                        failure.validate().map_err(contract_failure)?;
                        Err(failure)
                    }
                    Ok(Err(_)) => Err(CapabilityAdapterFailure::contained_panic()),
                    Err(_) => Err(CapabilityAdapterFailure {
                        class: CapabilityAdapterFailureClass::ContainedPanic,
                        safe_code: "native_cancel_timeout".to_owned(),
                        safe_message: "Native Capability cancellation exceeded its timeout"
                            .to_owned(),
                        evidence_digest: digest_domain("native_cancel_timeout"),
                        external_identity_digest: None,
                    }),
                }
            }
            (contract, binding)
                if contract.kind() == binding.kind()
                    && matches!(
                        kind,
                        CapabilityBackendKind::Http | CapabilityBackendKind::Grpc
                    ) =>
            {
                self.cancel(request).await
            }
            (contract, binding)
                if contract.kind() == binding.kind() && kind == CapabilityBackendKind::Mcp =>
            {
                request.validate_at(Utc::now()).map_err(contract_failure)?;
                let timeout = (deadline - Utc::now())
                    .to_std()
                    .map_err(|_| contract_failure(CapabilityDispatchError::InvalidCancelRequest))?;
                let port = self.ports.get(&kind).ok_or_else(|| {
                    contract_failure(CapabilityDispatchError::BackendPortNotInstalled)
                })?;
                let identity_digest = request.identity.evidence_digest();
                let future =
                    AssertUnwindSafe(port.cancel_execution(execution, deadline)).catch_unwind();
                match tokio::time::timeout(timeout, future).await {
                    Ok(Ok(Ok(outcome))) => Ok(outcome),
                    Ok(Ok(Err(failure))) => {
                        failure.validate().map_err(contract_failure)?;
                        Err(failure)
                    }
                    Ok(Err(_)) => Err(CapabilityAdapterFailure::contained_panic()),
                    Err(_) => Err(CapabilityAdapterFailure {
                        class: CapabilityAdapterFailureClass::TimedOutUncertain,
                        safe_code: "remote_cancel_timeout_uncertain".to_owned(),
                        safe_message:
                            "Remote Capability cancellation could not be observed before timeout"
                                .to_owned(),
                        evidence_digest: digest_domain("remote_cancel_timeout_uncertain"),
                        external_identity_digest: Some(identity_digest),
                    }),
                }
            }
            _ => Err(contract_failure(
                CapabilityDispatchError::BackendContractMismatch,
            )),
        }
    }

    async fn dispatch_inner(
        &self,
        request: &CapabilityAdapterRequest,
        contract: &CapabilityBackendContract,
        binding: &CapabilityBackendBinding,
    ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure> {
        match (contract, binding) {
            (
                CapabilityBackendContract::Native(contract),
                CapabilityBackendBinding::Native {
                    worker_manifest_digest,
                    ..
                },
            ) => {
                if worker_manifest_digest != &request.worker_manifest_digest {
                    return Err(contract_failure(
                        CapabilityDispatchError::WorkerManifestMismatch,
                    ));
                }
                let descriptor = InstalledNativeAdapter {
                    adapter_id: contract.adapter_id.clone(),
                    adapter_version: contract.adapter_version.clone(),
                    module_digest: contract.module_digest.clone(),
                    entrypoint_id: contract.entrypoint_id.clone(),
                };
                self.native
                    .resolve(&descriptor)
                    .map_err(contract_failure)?
                    .invoke(request)
                    .await
            }
            (contract, binding) if contract.kind() == binding.kind() => {
                self.ports
                    .get(&contract.kind())
                    .ok_or_else(|| {
                        contract_failure(CapabilityDispatchError::BackendPortNotInstalled)
                    })?
                    .invoke(request)
                    .await
            }
            _ => Err(contract_failure(
                CapabilityDispatchError::BackendContractMismatch,
            )),
        }
    }
}

fn dispatch_timeout(
    request: &CapabilityAdapterRequest,
    now: DateTime<Utc>,
) -> Result<Duration, CapabilityDispatchError> {
    let contract_milliseconds = request
        .execution
        .implementation
        .backend_limits
        .total_timeout_milliseconds;
    let remaining_milliseconds = (request.deadline - now).num_milliseconds();
    let remaining_milliseconds = u64::try_from(remaining_milliseconds)
        .map_err(|_| CapabilityDispatchError::InvalidRequest)?;
    Ok(Duration::from_millis(
        contract_milliseconds.min(remaining_milliseconds),
    ))
}

fn validate_response_size(
    request: &CapabilityAdapterRequest,
    response: &CapabilityAdapterResponse,
) -> Result<(), CapabilityDispatchError> {
    let bytes = serde_json::to_vec(&response.outcome)
        .map_err(|_| CapabilityDispatchError::MalformedAdapterResponse)?;
    if bytes.len()
        > usize::try_from(
            request
                .execution
                .implementation
                .backend_limits
                .maximum_response_bytes,
        )
        .map_err(|_| CapabilityDispatchError::MalformedAdapterResponse)?
    {
        return Err(CapabilityDispatchError::MalformedAdapterResponse);
    }
    Ok(())
}

/// Constructs the stable fail-closed adapter error used by trusted composition crates.
pub fn contract_failure(error: CapabilityDispatchError) -> CapabilityAdapterFailure {
    CapabilityAdapterFailure {
        class: CapabilityAdapterFailureClass::Permanent,
        safe_code: error.as_code().to_owned(),
        safe_message: error.to_string(),
        evidence_digest: digest_domain(error.as_code()),
        external_identity_digest: None,
    }
}

fn digest_domain(domain: &str) -> Sha256Digest {
    canonical_digest(&serde_json::json!({"domain": domain, "schema_version": 1}))
        .expect("static adapter evidence is canonical")
        .parse()
        .expect("canonical digest is a SHA-256 digest")
}

fn stable_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_ADAPTER_FAILURE_CODE_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
}

fn stable_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value.is_ascii()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'/' | b'+')
        })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityDispatchError {
    InvalidRequest,
    InvalidCancelRequest,
    InvalidInstalledAdapter,
    NativeAdapterNotInstalled,
    ProtocolCodecNotInstalled,
    InvalidBackendPort,
    BackendPortNotInstalled,
    BackendContractMismatch,
    WorkerManifestMismatch,
    MalformedAdapterResponse,
    MalformedAdapterFailure,
    MissingRetryDeadline,
}

impl CapabilityDispatchError {
    const fn as_code(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid_request",
            Self::InvalidCancelRequest => "invalid_cancel_request",
            Self::InvalidInstalledAdapter => "invalid_installed_adapter",
            Self::NativeAdapterNotInstalled => "native_adapter_not_installed",
            Self::ProtocolCodecNotInstalled => "protocol_codec_not_installed",
            Self::InvalidBackendPort => "invalid_backend_port",
            Self::BackendPortNotInstalled => "backend_port_not_installed",
            Self::BackendContractMismatch => "backend_contract_mismatch",
            Self::WorkerManifestMismatch => "worker_manifest_mismatch",
            Self::MalformedAdapterResponse => "malformed_adapter_response",
            Self::MalformedAdapterFailure => "malformed_adapter_failure",
            Self::MissingRetryDeadline => "missing_retry_deadline",
        }
    }
}

impl fmt::Display for CapabilityDispatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidRequest => "Capability adapter request is invalid",
            Self::InvalidCancelRequest => "Capability cancellation request is invalid",
            Self::InvalidInstalledAdapter => "installed Native adapter descriptor is invalid",
            Self::NativeAdapterNotInstalled => "exact Native adapter is not installed",
            Self::ProtocolCodecNotInstalled => "exact protocol codec is not installed",
            Self::InvalidBackendPort => "Capability backend port registration is invalid",
            Self::BackendPortNotInstalled => "Capability backend port is not installed",
            Self::BackendContractMismatch => "Capability backend contract and binding do not match",
            Self::WorkerManifestMismatch => {
                "Capability claim WorkerManifest does not match the Deployment"
            }
            Self::MalformedAdapterResponse => {
                "Capability adapter response is malformed or oversized"
            }
            Self::MalformedAdapterFailure => "Capability adapter failure is malformed",
            Self::MissingRetryDeadline => {
                "retryable Capability adapter failure has no retry deadline"
            }
        })
    }
}

impl Error for CapabilityDispatchError {}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use chrono::Duration as ChronoDuration;
    use insight_platform_contracts::{
        AllowedMcpServerCapabilities, ArtifactRef, CanonicalHttpEndpoint,
        CapabilityBackendFeatures, CapabilityBackendLimits, CapabilityDeploymentClosure,
        CapabilityEndpointScheme, DataClassification, ExactDeploymentRef, ExactSecretBindingRef,
        ExactVersionRef, GrpcCapabilityContract, HttpCapabilityContract, HttpCapabilityMethod,
        InstalledCapabilityCodecRef, InteractionSchemaDocument, McpAuthorizationPrincipalKind,
        McpClientCapabilities, McpDeploymentClosure, McpDiscoverySnapshot, McpExperimentalFeature,
        McpMetadataPolicy, McpMethodLimits, McpNegotiatedCapabilities, McpProtocolPolicyDocument,
        McpServerExecutionContract, McpServerLimits, McpToolCapabilityContract,
        McpTransportBinding, McpTransportFeatures, McpTransportKind, NativeCapabilityContract,
        PrincipalKind, PublishedMcpMethod, SecretPurpose, SecretResolutionPolicy,
        INSTALLED_CAPABILITY_CODEC_MANIFEST_VERSION, MCP_PROTOCOL_BASELINE,
        WORKER_PROTOCOL_VERSION,
    };
    use insight_platform_invocations::{
        CapabilityImplementationContract, CapabilityUncertainty, ExactInvocationValueRef,
        McpCapabilityRuntimeBinding,
    };
    use insight_platform_mcp_host::{
        EncryptedMcpState, McpAuthorizationContext, McpExecutionContractQuery,
        McpExecutionContractResolutionError, McpExecutionContractResolver, McpHostClient,
        McpHostError, McpHostExecutionContract, McpOperationOutcome, McpOperationRequest,
        McpRemoteTaskCancelOutcome, NewMcpAuthorizationContext, NewMcpHostExecutionContract,
    };
    use std::{collections::BTreeMap, str::FromStr, sync::Mutex};

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1c9-32e4-75e1-a9e8-d95ca0f5{suffix:04x}",
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

    fn installed_codec(
        contract: &CapabilityBackendContract,
        codec_id: &str,
    ) -> InstalledCapabilityCodecRef {
        InstalledCapabilityCodecRef {
            schema_version: INSTALLED_CAPABILITY_CODEC_MANIFEST_VERSION,
            backend_kind: contract.kind(),
            codec_id: codec_id.to_owned(),
            codec_version: "1.0.0".to_owned(),
            module_digest: digest('e'),
            worker_protocol_version: WORKER_PROTOCOL_VERSION,
            descriptor_digest: contract.descriptor_digest().unwrap(),
        }
    }

    fn exact(kind: ResourceKind, suffix: u16, character: char) -> ExactVersionRef {
        ExactVersionRef::new(id(kind, suffix), digest(character)).unwrap()
    }

    fn exact_secret_binding(secret_binding_id: ResourceId) -> ExactSecretBindingRef {
        ExactSecretBindingRef::build(
            secret_binding_id,
            1,
            id(ResourceKind::SecretProvider, 0x77),
            SecretPurpose::from_str("mcp.oauth").unwrap(),
            SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: digest('f'),
            },
        )
        .unwrap()
    }

    fn limits(total_timeout_milliseconds: u64) -> CapabilityBackendLimits {
        CapabilityBackendLimits {
            maximum_request_bytes: 1_048_576,
            maximum_response_bytes: 1_048_576,
            maximum_diagnostic_bytes: 65_536,
            connect_timeout_milliseconds: 1,
            first_byte_timeout_milliseconds: 2,
            idle_timeout_milliseconds: 3,
            total_timeout_milliseconds,
        }
    }

    fn features() -> CapabilityBackendFeatures {
        CapabilityBackendFeatures {
            deferred: false,
            input_required: false,
            callback: false,
            poll: false,
            progress: false,
            cancellation: false,
            max_remote_state_bytes: 0,
            max_poll_count: 0,
        }
    }

    fn evidence() -> ArtifactRef {
        ArtifactRef::new(
            id(ResourceKind::Artifact, 40),
            digest('e'),
            8,
            "application/json",
            DataClassification::Internal,
            Some("conformance.json".to_owned()),
        )
        .unwrap()
    }

    fn native_execution(
        worker_manifest_digest: Sha256Digest,
        total_timeout_milliseconds: u64,
    ) -> CapabilityExecutionContract {
        let interface = exact(ResourceKind::CapabilityInterfaceRevision, 1, '1');
        let revision = exact(ResourceKind::CapabilityImplementationRevision, 2, '2');
        let backend_contract = CapabilityBackendContract::Native(NativeCapabilityContract {
            adapter_id: "builtin.fixture".to_owned(),
            adapter_version: "1.0.0".to_owned(),
            module_digest: digest('3'),
            entrypoint_id: "fixture.invoke".to_owned(),
            worker_protocol_version: WORKER_PROTOCOL_VERSION,
        });
        let implementation = CapabilityImplementationContract {
            revision: revision.clone(),
            interface_revision: interface.clone(),
            backend_kind: CapabilityBackendKind::Native,
            backend_contract_digest: backend_contract.canonical_digest().unwrap(),
            backend_contract,
            credential_requirements: vec![],
            backend_limits: limits(total_timeout_milliseconds),
            features: features(),
        };
        CapabilityExecutionContract::build(
            ExactDeploymentRef::new(id(ResourceKind::CapabilityDeployment, 3), digest('4'))
                .unwrap(),
            CapabilityDeploymentClosure {
                implementation: revision,
                interface,
                backend: CapabilityBackendBinding::Native {
                    worker_manifest_digest,
                    adapter_module_digest: digest('3'),
                },
                secret_bindings: vec![],
                policies: vec![exact(ResourceKind::PolicyRevision, 4, '5')],
                conformance_evidence: evidence(),
            },
            implementation,
        )
        .unwrap()
    }

    pub(super) fn http_execution(total_timeout_milliseconds: u64) -> CapabilityExecutionContract {
        let interface = exact(ResourceKind::CapabilityInterfaceRevision, 11, '1');
        let revision = exact(ResourceKind::CapabilityImplementationRevision, 12, '2');
        let backend_contract = CapabilityBackendContract::Http(HttpCapabilityContract {
            method: HttpCapabilityMethod::Post,
            protocol_contract_digest: digest('3'),
            request_mapping_digest: digest('4'),
            response_mapping_digest: digest('5'),
            error_mapping_digest: digest('6'),
            idempotency_header: Some("idempotency-key".to_owned()),
        });
        let endpoint = CanonicalHttpEndpoint {
            scheme: CapabilityEndpointScheme::Https,
            host: "api.example.test".to_owned(),
            port: 443,
            base_path: "/v1/invoke".to_owned(),
        };
        let codec = installed_codec(&backend_contract, "fixture.http");
        let implementation = CapabilityImplementationContract {
            revision: revision.clone(),
            interface_revision: interface.clone(),
            backend_kind: CapabilityBackendKind::Http,
            backend_contract_digest: backend_contract.canonical_digest().unwrap(),
            backend_contract,
            credential_requirements: vec![],
            backend_limits: limits(total_timeout_milliseconds),
            features: features(),
        };
        CapabilityExecutionContract::build(
            ExactDeploymentRef::new(id(ResourceKind::CapabilityDeployment, 13), digest('7'))
                .unwrap(),
            CapabilityDeploymentClosure {
                implementation: revision,
                interface,
                backend: CapabilityBackendBinding::Http {
                    codec,
                    worker_manifest_digest: digest('a'),
                    endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
                    endpoint,
                    network_policy: exact(ResourceKind::PolicyRevision, 14, '8'),
                    tls_policy: exact(ResourceKind::PolicyRevision, 15, '9'),
                    trust_policy: exact(ResourceKind::PolicyRevision, 16, 'a'),
                },
                secret_bindings: vec![],
                policies: vec![exact(ResourceKind::PolicyRevision, 17, 'b')],
                conformance_evidence: evidence(),
            },
            implementation,
        )
        .unwrap()
    }

    fn grpc_execution(total_timeout_milliseconds: u64) -> CapabilityExecutionContract {
        let interface = exact(ResourceKind::CapabilityInterfaceRevision, 51, '1');
        let revision = exact(ResourceKind::CapabilityImplementationRevision, 52, '2');
        let backend_contract = CapabilityBackendContract::Grpc(GrpcCapabilityContract {
            protobuf_contract_digest: digest('3'),
            service_name: "fixture.v1.LookupService".to_owned(),
            method_name: "Lookup".to_owned(),
            request_mapping_digest: digest('4'),
            response_mapping_digest: digest('5'),
            error_mapping_digest: digest('6'),
            idempotency_metadata_key: Some("idempotency-key".to_owned()),
        });
        let endpoint = CanonicalHttpEndpoint {
            scheme: CapabilityEndpointScheme::Https,
            host: "grpc.example.test".to_owned(),
            port: 443,
            base_path: "/".to_owned(),
        };
        let codec = installed_codec(&backend_contract, "fixture.grpc");
        let implementation = CapabilityImplementationContract {
            revision: revision.clone(),
            interface_revision: interface.clone(),
            backend_kind: CapabilityBackendKind::Grpc,
            backend_contract_digest: backend_contract.canonical_digest().unwrap(),
            backend_contract,
            credential_requirements: vec![],
            backend_limits: limits(total_timeout_milliseconds),
            features: features(),
        };
        CapabilityExecutionContract::build(
            ExactDeploymentRef::new(id(ResourceKind::CapabilityDeployment, 53), digest('7'))
                .unwrap(),
            CapabilityDeploymentClosure {
                implementation: revision,
                interface,
                backend: CapabilityBackendBinding::Grpc {
                    codec,
                    worker_manifest_digest: digest('a'),
                    endpoint_identity_digest: endpoint.canonical_digest().unwrap(),
                    endpoint,
                    network_policy: exact(ResourceKind::PolicyRevision, 54, '8'),
                    tls_policy: exact(ResourceKind::PolicyRevision, 55, '9'),
                    trust_policy: exact(ResourceKind::PolicyRevision, 56, 'a'),
                },
                secret_bindings: vec![],
                policies: vec![exact(ResourceKind::PolicyRevision, 57, 'b')],
                conformance_evidence: evidence(),
            },
            implementation,
        )
        .unwrap()
    }

    struct McpFixture {
        execution: CapabilityExecutionContract,
        host: McpHostExecutionContract,
        runtime: McpCapabilityRuntimeBinding,
        descriptor: InstalledMcpToolCodecDescriptor,
    }

    fn mcp_fixture(tasks: bool) -> McpFixture {
        let now = Utc::now();
        let mut method_limits = BTreeMap::from([(
            PublishedMcpMethod::ToolsCall,
            McpMethodLimits {
                maximum_request_bytes: 4_096,
                maximum_response_bytes: 4_096,
                maximum_metadata_entries: 16,
                maximum_progress_events: 16,
                maximum_pages: 8,
                minimum_poll_milliseconds: 100,
                maximum_poll_milliseconds: 1_000,
            },
        )]);
        if tasks {
            for method in [
                PublishedMcpMethod::TasksGet,
                PublishedMcpMethod::TasksResult,
                PublishedMcpMethod::TasksCancel,
            ] {
                method_limits.insert(
                    method,
                    McpMethodLimits {
                        maximum_request_bytes: 4_096,
                        maximum_response_bytes: 4_096,
                        maximum_metadata_entries: 16,
                        maximum_progress_events: 16,
                        maximum_pages: 8,
                        minimum_poll_milliseconds: 100,
                        maximum_poll_milliseconds: 1_000,
                    },
                );
            }
        }
        let protocol = McpProtocolPolicyDocument {
            schema_version: 1,
            offered_versions: vec![MCP_PROTOCOL_BASELINE.to_owned()],
            transport_features: McpTransportFeatures {
                streamable_http_get: true,
                streamable_http_sse: true,
                resumable_stream: true,
                session_affinity: true,
            },
            client_capabilities: McpClientCapabilities {
                elicitation_form: tasks,
                elicitation_url: false,
                tasks_elicitation_create: tasks,
                sampling: false,
                roots: false,
            },
            allowed_server_capabilities: AllowedMcpServerCapabilities {
                tools: true,
                resources: false,
                prompts: false,
                logging: false,
                tasks,
                subscriptions: false,
            },
            experimental_features: tasks
                .then_some(McpExperimentalFeature::Tasks)
                .into_iter()
                .collect(),
            method_limits,
            metadata_policy: McpMetadataPolicy {
                maximum_server_name_bytes: 128,
                maximum_server_version_bytes: 64,
                maximum_instruction_bytes: 4_096,
                maximum_object_name_bytes: 128,
                maximum_description_bytes: 8_192,
                maximum_icon_bytes: 1_048_576,
            },
        };
        let protocol_profile = ExactVersionRef::new(
            id(ResourceKind::PolicyRevision, 70),
            protocol.canonical_digest().unwrap(),
        )
        .unwrap();
        let mcp_deployment =
            ExactDeploymentRef::new(id(ResourceKind::McpDeployment, 71), digest('1')).unwrap();
        let server_revision = exact(ResourceKind::McpServerRevision, 72, '2');
        let network_policy = exact(ResourceKind::PolicyRevision, 73, '3');
        let tls_policy = exact(ResourceKind::PolicyRevision, 74, '4');
        let trust_policy = exact(ResourceKind::PolicyRevision, 75, '5');
        let auth_policy = exact(ResourceKind::PolicyRevision, 76, '6');
        let endpoint = CanonicalHttpEndpoint {
            scheme: CapabilityEndpointScheme::Https,
            host: "mcp.example.test".to_owned(),
            port: 443,
            base_path: "/mcp".to_owned(),
        };
        let identity = endpoint.canonical_digest().unwrap();
        let secret_binding_id = id(ResourceKind::SecretBinding, 77);
        let mcp_closure = McpDeploymentClosure {
            server_revision: server_revision.clone(),
            server_identity_digest: identity.clone(),
            transport: McpTransportBinding::StreamableHttp {
                endpoint,
                endpoint_identity_digest: identity.clone(),
                network_policy,
                tls_policy,
            },
            protocol_policy: protocol_profile.clone(),
            trust_policy,
            auth_policy: Some(auth_policy.clone()),
            secret_bindings: vec![],
            conformance_evidence: evidence(),
        };
        let server = McpServerExecutionContract::build(
            server_revision.clone(),
            insight_platform_contracts::McpTransportKind::StreamableHttp,
            protocol_profile.clone(),
            vec![],
            Some(SecretPurpose::from_str("mcp.oauth").unwrap()),
            McpServerLimits {
                maximum_message_bytes: 8_192,
                maximum_response_bytes: 8_192,
                maximum_headers: 32,
                maximum_sse_event_bytes: 4_096,
                maximum_in_flight: 8,
                maximum_connections: 4,
                maximum_sessions: 4,
                maximum_session_milliseconds: 3_600_000,
                idle_timeout_milliseconds: 1_000,
                initialize_timeout_milliseconds: 1_000,
                request_timeout_milliseconds: 5_000,
                total_timeout_milliseconds: 10_000,
            },
        )
        .unwrap();
        let authorization = McpAuthorizationContext::build(NewMcpAuthorizationContext {
            tenant_id: id(ResourceKind::Tenant, 20),
            authorization_binding_id: id(ResourceKind::McpAuthorizationBinding, 78),
            mcp_deployment: mcp_deployment.clone(),
            principal_kind: McpAuthorizationPrincipalKind::PerUser,
            principal_id: id(ResourceKind::Principal, 79),
            principal_identity_kind: PrincipalKind::AgentRunner,
            principal_binding_generation: 1,
            audience_identity_digest: identity,
            granted_scopes: vec!["tools.call".to_owned()],
            token_secret_binding: exact_secret_binding(secret_binding_id),
            generation: 1,
            expires_at: now + ChronoDuration::hours(1),
        })
        .unwrap();
        let objects = ArtifactRef::new(
            id(ResourceKind::Artifact, 80),
            digest('7'),
            128,
            "application/json",
            DataClassification::Internal,
            Some("mcp-discovery.json".to_owned()),
        )
        .unwrap();
        let discovery = McpDiscoverySnapshot::build(
            id(ResourceKind::McpDiscoverySnapshot, 81),
            mcp_deployment.clone(),
            server_revision,
            protocol_profile.clone(),
            authorization.canonical_digest.clone(),
            MCP_PROTOCOL_BASELINE.to_owned(),
            McpNegotiatedCapabilities {
                tools: true,
                resources: false,
                prompts: false,
                logging: false,
                tasks,
                tasks_list: tasks,
                tasks_cancel: tasks,
                tasks_tools_call: tasks,
                elicitation: tasks,
                sampling: false,
                roots: false,
                subscriptions: false,
            },
            objects,
            now - ChronoDuration::seconds(1),
            now + ChronoDuration::minutes(30),
        )
        .unwrap();
        let host = McpHostExecutionContract::build(NewMcpHostExecutionContract {
            deployment: mcp_deployment.clone(),
            deployment_closure: mcp_closure,
            server,
            protocol_profile: protocol,
            authorization: authorization.clone(),
            discovery: discovery.clone(),
        })
        .unwrap();
        let interface = exact(ResourceKind::CapabilityInterfaceRevision, 82, '8');
        let implementation_revision =
            exact(ResourceKind::CapabilityImplementationRevision, 83, '9');
        let tool_contract = McpToolCapabilityContract {
            remote_tool_name: "fixture.lookup".to_owned(),
            remote_input_schema_digest: digest('a'),
            output_mapping_digest: digest('b'),
            protocol_profile,
            discovery_semantic_evidence_digest: discovery.objects_digest.clone(),
            supports_task: tasks,
            supports_progress: false,
        };
        let backend_contract = CapabilityBackendContract::Mcp(tool_contract);
        let codec = installed_codec(&backend_contract, "fixture.mcp");
        let CapabilityBackendContract::Mcp(tool_contract) = &backend_contract else {
            unreachable!();
        };
        let descriptor = InstalledMcpToolCodecDescriptor::exact(&codec, tool_contract);
        let implementation = CapabilityImplementationContract {
            revision: implementation_revision.clone(),
            interface_revision: interface.clone(),
            backend_kind: CapabilityBackendKind::Mcp,
            backend_contract_digest: backend_contract.canonical_digest().unwrap(),
            backend_contract,
            credential_requirements: vec![],
            backend_limits: limits(10_000),
            features: if tasks {
                CapabilityBackendFeatures {
                    deferred: true,
                    input_required: true,
                    callback: false,
                    poll: true,
                    progress: false,
                    cancellation: true,
                    max_remote_state_bytes: 4_096,
                    max_poll_count: 2,
                }
            } else {
                features()
            },
        };
        let execution = CapabilityExecutionContract::build(
            ExactDeploymentRef::new(id(ResourceKind::CapabilityDeployment, 84), digest('c'))
                .unwrap(),
            CapabilityDeploymentClosure {
                implementation: implementation_revision,
                interface,
                backend: CapabilityBackendBinding::Mcp {
                    codec,
                    worker_manifest_digest: digest('0'),
                    mcp_deployment: mcp_deployment.clone(),
                    discovery_snapshot_id: discovery.snapshot_id.clone(),
                    discovery_snapshot_digest: discovery.canonical_digest.clone(),
                    authorization_policy: auth_policy,
                },
                secret_bindings: vec![],
                policies: vec![exact(ResourceKind::PolicyRevision, 85, 'd')],
                conformance_evidence: evidence(),
            },
            implementation,
        )
        .unwrap();
        let runtime = McpCapabilityRuntimeBinding {
            schema_version: 1,
            mcp_operation_id: id(ResourceKind::McpOperation, 86),
            mcp_deployment,
            discovery_snapshot_id: discovery.snapshot_id,
            discovery_snapshot_digest: discovery.canonical_digest,
            authorization_binding_id: authorization.authorization_binding_id,
            authorization_generation: authorization.generation,
            authorization_context_digest: authorization.canonical_digest,
            principal_id: authorization.principal_id,
        };
        McpFixture {
            execution,
            host,
            runtime,
            descriptor,
        }
    }

    fn request(
        execution: CapabilityExecutionContract,
        worker_manifest_digest: Sha256Digest,
    ) -> CapabilityAdapterRequest {
        let value = serde_json::json!({"query": "status"});
        CapabilityAdapterRequest {
            tenant_id: id(ResourceKind::Tenant, 20),
            invocation_id: id(ResourceKind::CapabilityInvocation, 21),
            job_id: id(ResourceKind::Job, 22),
            worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 23),
            worker_manifest_digest,
            lease_generation: 1,
            physical_attempt: 1,
            attempt_limit: 3,
            admission_digest: digest('c'),
            output_schema_digest: digest('d'),
            idempotency_key_digest: digest('b'),
            effect: Effect::ReadOnly,
            idempotency: insight_platform_contracts::CapabilityIdempotencyKind::Intrinsic,
            deadline: Utc::now() + ChronoDuration::seconds(30),
            execution,
            input: CapabilityExecutionInput {
                exact: ExactInvocationValueRef {
                    schema_version: 1,
                    value_id: id(ResourceKind::RunValue, 24),
                    run_id: id(ResourceKind::Run, 25),
                    producing_node_id: Some(id(ResourceKind::NodeExecution, 26)),
                    value_kind: "capability_input".to_owned(),
                    classification: DataClassification::Internal,
                    schema_digest: digest('d'),
                    content_digest: canonical_digest(&value).unwrap().parse().unwrap(),
                    storage: InvocationValueStorage::Inline,
                },
                material: CapabilityExecutionInputMaterial::Inline { value },
            },
            continuation: None,
            mcp_runtime: None,
        }
    }

    fn response() -> CapabilityAdapterResponse {
        CapabilityAdapterResponse {
            outcome: DispatchOutcome::Uncertain(CapabilityUncertainty {
                observation_digest: digest('d'),
                policy_path_digest: digest('e'),
                external_identity_digest: digest('f'),
                manual: true,
            }),
        }
    }

    struct FixtureNative {
        descriptor: InstalledNativeAdapter,
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
            Ok(response())
        }
    }

    struct DelayedPort {
        delay: Duration,
    }

    struct FixtureHttpCodec {
        descriptor: InstalledHttpCodecDescriptor,
    }

    #[async_trait]
    impl HttpCapabilityCodec for FixtureHttpCodec {
        fn descriptor(&self) -> InstalledHttpCodecDescriptor {
            self.descriptor.clone()
        }

        fn encode(
            &self,
            _request: &CapabilityAdapterRequest,
        ) -> Result<EncodedHttpCapabilityRequest, CapabilityAdapterFailure> {
            Ok(EncodedHttpCapabilityRequest {
                headers: vec![SafeHttpHeader {
                    name: "content-type".to_owned(),
                    value: "application/json".to_owned(),
                }],
                body: b"{}".to_vec(),
            })
        }

        fn decode(
            &self,
            _request: &CapabilityAdapterRequest,
            _response: HttpTransportResponse,
        ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure> {
            Ok(response())
        }
    }

    #[derive(Default)]
    struct FixtureHttpTransport {
        observed: Mutex<Option<HttpTransportRequest>>,
        cancelled: Mutex<Option<CapabilityTransportCancelRequest>>,
    }

    struct FixtureGrpcCodec {
        descriptor: InstalledGrpcCodecDescriptor,
    }

    #[async_trait]
    impl GrpcCapabilityCodec for FixtureGrpcCodec {
        fn descriptor(&self) -> InstalledGrpcCodecDescriptor {
            self.descriptor.clone()
        }

        fn encode(
            &self,
            _request: &CapabilityAdapterRequest,
        ) -> Result<EncodedGrpcCapabilityRequest, CapabilityAdapterFailure> {
            Ok(EncodedGrpcCapabilityRequest {
                metadata: vec![],
                message: vec![0x0a, 0x00],
            })
        }

        fn decode(
            &self,
            _request: &CapabilityAdapterRequest,
            _response: GrpcTransportResponse,
        ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure> {
            Ok(response())
        }
    }

    #[derive(Default)]
    struct FixtureGrpcTransport {
        observed: Mutex<Option<GrpcTransportRequest>>,
        cancelled: Mutex<Option<CapabilityTransportCancelRequest>>,
    }

    struct FixtureMcpResolver {
        contract: McpHostExecutionContract,
    }

    #[async_trait]
    impl McpExecutionContractResolver for FixtureMcpResolver {
        async fn resolve_mcp_execution_contract(
            &self,
            _query: &McpExecutionContractQuery,
        ) -> Result<McpHostExecutionContract, McpExecutionContractResolutionError> {
            Ok(self.contract.clone())
        }
    }

    #[derive(Default)]
    struct FixtureMcpHost {
        observed: Mutex<Option<McpOperationRequest>>,
        cancelled: Mutex<Option<(McpOperationRequest, DateTime<Utc>)>>,
        outcome: Mutex<Option<McpOperationOutcome>>,
    }

    #[async_trait]
    impl McpHostClient for FixtureMcpHost {
        async fn execute(
            &self,
            _contract: &McpHostExecutionContract,
            request: &McpOperationRequest,
        ) -> Result<McpOperationOutcome, McpHostError> {
            *self.observed.lock().unwrap() = Some(request.clone());
            if let Some(outcome) = self.outcome.lock().unwrap().take() {
                return Ok(outcome);
            }
            Ok(McpOperationOutcome::Completed {
                result: insight_platform_contracts::ClosedJsonValue::build(
                    digest('e'),
                    serde_json::json!({"answer": 42}),
                )
                .unwrap(),
                evidence_digest: digest('f'),
            })
        }

        async fn cancel_remote_task(
            &self,
            _contract: &McpHostExecutionContract,
            request: &McpOperationRequest,
            deadline: DateTime<Utc>,
        ) -> Result<McpRemoteTaskCancelOutcome, McpHostError> {
            *self.cancelled.lock().unwrap() = Some((request.clone(), deadline));
            Ok(McpRemoteTaskCancelOutcome::Accepted)
        }
    }

    struct FixtureMcpCodec {
        descriptor: InstalledMcpToolCodecDescriptor,
    }

    impl McpToolCapabilityCodec for FixtureMcpCodec {
        fn descriptor(&self) -> InstalledMcpToolCodecDescriptor {
            self.descriptor.clone()
        }

        fn encode(
            &self,
            request: &CapabilityAdapterRequest,
        ) -> Result<insight_platform_contracts::ClosedJsonValue, CapabilityAdapterFailure> {
            let CapabilityExecutionInputMaterial::Inline { value } = &request.input.material else {
                return Err(contract_failure(CapabilityDispatchError::InvalidRequest));
            };
            insight_platform_contracts::ClosedJsonValue::build(
                self.descriptor.remote_input_schema_digest.clone(),
                serde_json::json!({
                    "arguments": value,
                    "name": self.descriptor.remote_tool_name,
                }),
            )
            .map_err(|_| contract_failure(CapabilityDispatchError::InvalidRequest))
        }

        fn decode(
            &self,
            _request: &CapabilityAdapterRequest,
            outcome: McpOperationOutcome,
        ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure> {
            if !matches!(outcome, McpOperationOutcome::Completed { .. }) {
                return Err(contract_failure(
                    CapabilityDispatchError::MalformedAdapterResponse,
                ));
            }
            Ok(response())
        }
    }

    #[async_trait]
    impl GrpcNetworkTransport for FixtureGrpcTransport {
        async fn unary(
            &self,
            request: GrpcTransportRequest,
        ) -> Result<GrpcTransportResponse, CapabilityAdapterFailure> {
            *self.observed.lock().unwrap() = Some(request);
            Ok(GrpcTransportResponse {
                status_code: 0,
                trailing_metadata: vec![],
                message: vec![0x0a, 0x00],
                transport_evidence_digest: digest('f'),
            })
        }

        async fn cancel(
            &self,
            request: CapabilityTransportCancelRequest,
        ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
            *self.cancelled.lock().unwrap() = Some(request);
            Ok(CapabilityTransportCancelOutcome::Accepted)
        }
    }

    #[async_trait]
    impl HttpNetworkTransport for FixtureHttpTransport {
        async fn round_trip(
            &self,
            request: HttpTransportRequest,
        ) -> Result<HttpTransportResponse, CapabilityAdapterFailure> {
            *self.observed.lock().unwrap() = Some(request);
            Ok(HttpTransportResponse {
                status: 200,
                headers: vec![],
                body: b"{}".to_vec(),
                transport_evidence_digest: digest('f'),
            })
        }

        async fn cancel(
            &self,
            request: CapabilityTransportCancelRequest,
        ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
            *self.cancelled.lock().unwrap() = Some(request);
            Ok(CapabilityTransportCancelOutcome::Accepted)
        }
    }

    #[async_trait]
    impl CapabilityBackendPort for DelayedPort {
        fn kind(&self) -> CapabilityBackendKind {
            CapabilityBackendKind::Http
        }

        async fn invoke(
            &self,
            _request: &CapabilityAdapterRequest,
        ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure> {
            tokio::time::sleep(self.delay).await;
            Ok(response())
        }
    }

    #[tokio::test]
    async fn native_dispatch_requires_exact_installed_adapter_and_worker_manifest() {
        let worker_manifest = digest('a');
        let execution = native_execution(worker_manifest.clone(), 1_000);
        let descriptor = InstalledNativeAdapter {
            adapter_id: "builtin.fixture".to_owned(),
            adapter_version: "1.0.0".to_owned(),
            module_digest: digest('3'),
            entrypoint_id: "fixture.invoke".to_owned(),
        };
        let mut registry = InstalledNativeRegistry::default();
        registry
            .install(Arc::new(FixtureNative { descriptor }))
            .unwrap();
        let dispatcher = CapabilityDispatcher::new(registry);
        assert_eq!(
            dispatcher
                .dispatch(&request(execution.clone(), worker_manifest))
                .await
                .unwrap(),
            response()
        );

        let failure = dispatcher
            .dispatch(&request(execution, digest('b')))
            .await
            .unwrap_err();
        assert_eq!(failure.safe_code, "worker_manifest_mismatch");
    }

    #[tokio::test]
    async fn missing_native_module_and_remote_port_fail_closed() {
        let worker_manifest = digest('a');
        let dispatcher = CapabilityDispatcher::new(InstalledNativeRegistry::default());
        let native_failure = dispatcher
            .dispatch(&request(
                native_execution(worker_manifest.clone(), 1_000),
                worker_manifest.clone(),
            ))
            .await
            .unwrap_err();
        assert_eq!(native_failure.safe_code, "native_adapter_not_installed");

        let remote_failure = dispatcher
            .dispatch(&request(http_execution(1_000), worker_manifest))
            .await
            .unwrap_err();
        assert_eq!(remote_failure.safe_code, "backend_port_not_installed");
    }

    #[tokio::test]
    async fn remote_timeout_is_uncertain_and_never_retried_by_the_dispatcher() {
        let mut dispatcher = CapabilityDispatcher::new(InstalledNativeRegistry::default());
        dispatcher
            .install_port(Arc::new(DelayedPort {
                delay: Duration::from_millis(100),
            }))
            .unwrap();
        let failure = dispatcher
            .dispatch(&request(http_execution(10), digest('a')))
            .await
            .unwrap_err();
        assert_eq!(
            failure.class,
            CapabilityAdapterFailureClass::TimedOutUncertain
        );
        assert!(failure.external_identity_digest.is_some());
    }

    #[tokio::test]
    async fn http_adapter_passes_only_exact_endpoint_policies_and_secret_bindings() {
        let execution = http_execution(1_000);
        let CapabilityBackendContract::Http(contract) = &execution.implementation.backend_contract
        else {
            unreachable!();
        };
        let mut codecs = InstalledHttpCodecRegistry::default();
        codecs
            .install(Arc::new(FixtureHttpCodec {
                descriptor: {
                    let CapabilityBackendBinding::Http { codec, .. } =
                        &execution.deployment_closure.backend
                    else {
                        unreachable!();
                    };
                    InstalledHttpCodecDescriptor::exact(codec, contract)
                },
            }))
            .unwrap();
        let transport = Arc::new(FixtureHttpTransport::default());
        let adapter = Arc::new(HttpCapabilityAdapter::new(codecs, transport.clone()));
        let mut dispatcher = CapabilityDispatcher::new(InstalledNativeRegistry::default());
        dispatcher.install_port(adapter).unwrap();

        let request = request(execution, digest('a'));
        let expected_identity = CapabilityTransportRequestIdentity::from_adapter_request(
            &request,
            CapabilityBackendKind::Http,
        );
        assert_eq!(dispatcher.dispatch(&request).await.unwrap(), response());
        {
            let observed = transport.observed.lock().unwrap();
            let observed = observed.as_ref().unwrap();
            assert_eq!(observed.identity, expected_identity);
            assert_eq!(observed.admission_digest, request.admission_digest);
            assert_eq!(observed.deadline, request.deadline);
            assert_eq!(observed.endpoint.host, "api.example.test");
            assert_eq!(observed.endpoint.base_path, "/v1/invoke");
            assert!(observed.secret_bindings.is_empty());
            assert_eq!(
                observed.idempotency.as_ref().unwrap().header_name,
                "idempotency-key"
            );
            assert!(observed
                .headers
                .iter()
                .all(|header| header.name != "authorization"));
        }
        let cancel = CapabilityTransportCancelRequest::from_adapter_request(
            &request,
            CapabilityBackendKind::Http,
            Utc::now() + ChronoDuration::seconds(1),
        );
        assert_eq!(
            dispatcher.cancel(cancel.clone()).await.unwrap(),
            CapabilityTransportCancelOutcome::Accepted
        );
        assert_eq!(*transport.cancelled.lock().unwrap(), Some(cancel));
    }

    #[tokio::test]
    async fn remote_manifest_and_codec_drift_fail_before_transport_io() {
        let execution = http_execution(1_000);
        let CapabilityBackendContract::Http(contract) = &execution.implementation.backend_contract
        else {
            unreachable!();
        };
        let CapabilityBackendBinding::Http { codec, .. } = &execution.deployment_closure.backend
        else {
            unreachable!();
        };
        let mut codecs = InstalledHttpCodecRegistry::default();
        codecs
            .install(Arc::new(FixtureHttpCodec {
                descriptor: InstalledHttpCodecDescriptor::exact(codec, contract),
            }))
            .unwrap();
        let transport = Arc::new(FixtureHttpTransport::default());
        let adapter = Arc::new(HttpCapabilityAdapter::new(codecs, transport.clone()));
        let mut dispatcher = CapabilityDispatcher::new(InstalledNativeRegistry::default());
        dispatcher.install_port(adapter).unwrap();

        let wrong_manifest = request(execution.clone(), digest('f'));
        assert_eq!(
            dispatcher
                .dispatch(&wrong_manifest)
                .await
                .unwrap_err()
                .safe_code,
            "backend_contract_mismatch"
        );

        let mut wrong_module = execution.clone();
        let CapabilityBackendBinding::Http { codec, .. } =
            &mut wrong_module.deployment_closure.backend
        else {
            unreachable!();
        };
        codec.module_digest = digest('0');
        let wrong_module = CapabilityExecutionContract::build(
            wrong_module.deployment,
            wrong_module.deployment_closure,
            wrong_module.implementation,
        )
        .unwrap();
        assert_eq!(
            dispatcher
                .dispatch(&request(wrong_module, digest('a')))
                .await
                .unwrap_err()
                .safe_code,
            "protocol_codec_not_installed"
        );

        let mut wrong_descriptor = execution;
        let CapabilityBackendBinding::Http { codec, .. } =
            &mut wrong_descriptor.deployment_closure.backend
        else {
            unreachable!();
        };
        codec.descriptor_digest = digest('0');
        assert_eq!(
            dispatcher
                .dispatch(&request(wrong_descriptor, digest('a')))
                .await
                .unwrap_err()
                .safe_code,
            "invalid_request"
        );
        assert!(transport.observed.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn grpc_adapter_freezes_service_method_endpoint_and_safe_metadata() {
        let execution = grpc_execution(1_000);
        let CapabilityBackendContract::Grpc(contract) = &execution.implementation.backend_contract
        else {
            unreachable!();
        };
        let mut codecs = InstalledGrpcCodecRegistry::default();
        codecs
            .install(Arc::new(FixtureGrpcCodec {
                descriptor: {
                    let CapabilityBackendBinding::Grpc { codec, .. } =
                        &execution.deployment_closure.backend
                    else {
                        unreachable!();
                    };
                    InstalledGrpcCodecDescriptor::exact(codec, contract)
                },
            }))
            .unwrap();
        let transport = Arc::new(FixtureGrpcTransport::default());
        let adapter = Arc::new(GrpcCapabilityAdapter::new(codecs, transport.clone()));
        let mut dispatcher = CapabilityDispatcher::new(InstalledNativeRegistry::default());
        dispatcher.install_port(adapter).unwrap();

        let request = request(execution, digest('a'));
        let expected_identity = CapabilityTransportRequestIdentity::from_adapter_request(
            &request,
            CapabilityBackendKind::Grpc,
        );
        assert_eq!(dispatcher.dispatch(&request).await.unwrap(), response());
        {
            let observed = transport.observed.lock().unwrap();
            let observed = observed.as_ref().unwrap();
            assert_eq!(observed.identity, expected_identity);
            assert_eq!(observed.admission_digest, request.admission_digest);
            assert_eq!(observed.deadline, request.deadline);
            assert_eq!(observed.endpoint.host, "grpc.example.test");
            assert_eq!(observed.service_name, "fixture.v1.LookupService");
            assert_eq!(observed.method_name, "Lookup");
            assert_eq!(
                observed.idempotency.as_ref().unwrap().metadata_key,
                "idempotency-key"
            );
            assert!(observed
                .metadata
                .iter()
                .all(|metadata| metadata.key != "authorization"));
        }
        let cancel = CapabilityTransportCancelRequest::from_adapter_request(
            &request,
            CapabilityBackendKind::Grpc,
            Utc::now() + ChronoDuration::seconds(1),
        );
        assert_eq!(
            dispatcher.cancel(cancel.clone()).await.unwrap(),
            CapabilityTransportCancelOutcome::Accepted
        );
        assert_eq!(*transport.cancelled.lock().unwrap(), Some(cancel));
    }

    #[tokio::test]
    async fn mcp_adapter_uses_the_admitted_auth_generation_and_discovery_snapshot() {
        let fixture = mcp_fixture(false);
        let mut codecs = InstalledMcpToolCodecRegistry::default();
        codecs
            .install(Arc::new(FixtureMcpCodec {
                descriptor: fixture.descriptor,
            }))
            .unwrap();
        let host = Arc::new(FixtureMcpHost::default());
        let mut adapter = McpCapabilityAdapter::new(
            codecs,
            Arc::new(FixtureMcpResolver {
                contract: fixture.host,
            }),
        );
        adapter
            .install_host(
                insight_platform_contracts::McpTransportKind::StreamableHttp,
                host.clone(),
            )
            .unwrap();
        let mut dispatcher = CapabilityDispatcher::new(InstalledNativeRegistry::default());
        dispatcher.install_port(Arc::new(adapter)).unwrap();
        let mut request = request(fixture.execution, digest('0'));
        request.mcp_runtime = Some(fixture.runtime.clone());
        request.idempotency = insight_platform_contracts::CapabilityIdempotencyKind::CallerKey;
        let expected_key = request.idempotency_key_digest.clone();
        assert_eq!(dispatcher.dispatch(&request).await.unwrap(), response());
        {
            let observed = host.observed.lock().unwrap();
            let observed = observed.as_ref().unwrap();
            assert_eq!(observed.mcp_operation_id, fixture.runtime.mcp_operation_id);
            assert_eq!(
                observed.authorization_binding_id,
                fixture.runtime.authorization_binding_id
            );
            assert_eq!(observed.idempotency_key_digest, expected_key);
        }

        request
            .mcp_runtime
            .as_mut()
            .unwrap()
            .authorization_generation += 1;
        let failure = dispatcher.dispatch(&request).await.unwrap_err();
        assert_eq!(failure.safe_code, "backend_contract_mismatch");
    }

    #[tokio::test]
    async fn mcp_remote_task_round_trips_opaque_continuation_and_enforces_poll_limit() {
        let fixture = mcp_fixture(true);
        let mut codecs = InstalledMcpToolCodecRegistry::default();
        codecs
            .install(Arc::new(FixtureMcpCodec {
                descriptor: fixture.descriptor,
            }))
            .unwrap();
        let state = EncryptedMcpState {
            scheme: "aes256_gcm_v1".to_owned(),
            ciphertext: vec![1, 2, 3],
            key_id: "mcp-task-key".to_owned(),
            key_reference_digest: digest('1'),
            plaintext_digest: digest('2'),
        };
        let external_identity = digest('3');
        let host = Arc::new(FixtureMcpHost {
            observed: Mutex::new(None),
            cancelled: Mutex::new(None),
            outcome: Mutex::new(Some(McpOperationOutcome::RemoteTask {
                encrypted_state: state.clone(),
                external_identity_digest: external_identity.clone(),
                next_poll_at: Utc::now() + ChronoDuration::milliseconds(100),
            })),
        });
        let mut adapter = McpCapabilityAdapter::new(
            codecs,
            Arc::new(FixtureMcpResolver {
                contract: fixture.host,
            }),
        );
        adapter
            .install_host(McpTransportKind::StreamableHttp, host.clone())
            .unwrap();
        let mut request = request(fixture.execution, digest('0'));
        request.mcp_runtime = Some(fixture.runtime);

        let CapabilityAdapterResponse {
            outcome: DispatchOutcome::Deferred(first_wait),
        } = adapter.invoke(&request).await.unwrap()
        else {
            panic!("task-aware tools/call must defer")
        };
        assert!(
            host.observed
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .task_requested
        );

        request.continuation = Some(CapabilityAdapterContinuation {
            encrypted_remote_state: first_wait.encrypted_state.clone(),
            external_identity_digest: Some(first_wait.external_identity_digest.clone()),
            resume_input: None,
            resume_input_action: None,
            poll_count: 1,
        });
        *host.outcome.lock().unwrap() = Some(McpOperationOutcome::RemoteTask {
            encrypted_state: state.clone(),
            external_identity_digest: external_identity.clone(),
            next_poll_at: Utc::now() + ChronoDuration::milliseconds(100),
        });
        assert!(matches!(
            adapter.invoke(&request).await.unwrap().outcome,
            DispatchOutcome::Deferred(_)
        ));
        let observed = host.observed.lock().unwrap().clone().unwrap();
        assert!(!observed.task_requested);
        assert_eq!(observed.continuation.unwrap().encrypted_state, state);

        request.continuation.as_mut().unwrap().poll_count = 2;
        *host.outcome.lock().unwrap() = Some(McpOperationOutcome::RemoteTask {
            encrypted_state: EncryptedMcpState {
                scheme: "aes256_gcm_v1".to_owned(),
                ciphertext: vec![1, 2, 3],
                key_id: "mcp-task-key".to_owned(),
                key_reference_digest: digest('1'),
                plaintext_digest: digest('2'),
            },
            external_identity_digest: external_identity,
            next_poll_at: Utc::now() + ChronoDuration::milliseconds(100),
        });
        assert!(matches!(
            adapter.invoke(&request).await.unwrap().outcome,
            DispatchOutcome::PermanentFailure(_)
        ));

        let cancel_deadline = Utc::now() + ChronoDuration::seconds(1);
        assert_eq!(
            adapter
                .cancel_execution(&request, cancel_deadline)
                .await
                .unwrap(),
            CapabilityTransportCancelOutcome::Accepted
        );
        let cancelled = host.cancelled.lock().unwrap().clone().unwrap();
        assert_eq!(cancelled.1, cancel_deadline);
        assert!(!cancelled.0.task_requested);
        assert_eq!(
            cancelled.0.continuation.unwrap().external_identity_digest,
            request
                .continuation
                .as_ref()
                .unwrap()
                .external_identity_digest
                .clone()
                .unwrap()
        );
    }

    #[tokio::test]
    async fn mcp_input_required_maps_exact_task_schema_principal_and_all_resume_actions() {
        let fixture = mcp_fixture(true);
        let mut codecs = InstalledMcpToolCodecRegistry::default();
        codecs
            .install(Arc::new(FixtureMcpCodec {
                descriptor: fixture.descriptor,
            }))
            .unwrap();
        let state = EncryptedMcpState {
            scheme: "aes256_gcm_v1".to_owned(),
            ciphertext: vec![1, 2, 3],
            key_id: "mcp-task-key".to_owned(),
            key_reference_digest: digest('1'),
            plaintext_digest: digest('2'),
        };
        let external_identity = digest('3');
        let response_schema = InteractionSchemaDocument::build_mcp_form(serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "city": {"type": "string", "minLength": 1, "maxLength": 64}
            },
            "required": ["city"]
        }))
        .unwrap();
        let expected_principal = fixture.runtime.principal_id.clone();
        let host = Arc::new(FixtureMcpHost {
            observed: Mutex::new(None),
            cancelled: Mutex::new(None),
            outcome: Mutex::new(Some(McpOperationOutcome::InputRequired {
                encrypted_state: state.clone(),
                external_identity_digest: external_identity.clone(),
                safe_prompt_key: "mcp_task_input_required".to_owned(),
                response_schema: response_schema.clone(),
                response_schema_digest: response_schema.canonical_digest.clone(),
                deadline: Utc::now() + ChronoDuration::seconds(10),
            })),
        });
        let mut adapter = McpCapabilityAdapter::new(
            codecs,
            Arc::new(FixtureMcpResolver {
                contract: fixture.host,
            }),
        );
        adapter
            .install_host(McpTransportKind::StreamableHttp, host.clone())
            .unwrap();
        let mut request = request(fixture.execution, digest('0'));
        request.mcp_runtime = Some(fixture.runtime);
        request.continuation = Some(CapabilityAdapterContinuation {
            encrypted_remote_state: EncryptedRemoteState {
                scheme: state.scheme.clone(),
                key_id: state.key_id.clone(),
                key_reference_digest: state.key_reference_digest.clone(),
                ciphertext: base64::engine::general_purpose::STANDARD.encode(&state.ciphertext),
                plaintext_digest: state.plaintext_digest.clone(),
            },
            external_identity_digest: Some(external_identity.clone()),
            resume_input: None,
            resume_input_action: None,
            poll_count: 1,
        });

        let CapabilityAdapterResponse {
            outcome: DispatchOutcome::InputRequired(input_request),
        } = adapter.invoke(&request).await.unwrap()
        else {
            panic!("MCP elicitation must become the shared input-required outcome")
        };
        assert_eq!(
            input_request.interaction_kind,
            insight_platform_contracts::InteractionKind::Form
        );
        assert_eq!(input_request.response_schema, response_schema);
        assert_eq!(
            input_request.exact_eligible_principal_id,
            Some(expected_principal)
        );
        assert_eq!(input_request.opaque_state_digest, state.plaintext_digest);
        assert_eq!(input_request.external_identity_digest, external_identity);

        for action in [
            CapabilityInputAction::Accept,
            CapabilityInputAction::Decline,
            CapabilityInputAction::Cancel,
        ] {
            let continuation = request.continuation.as_mut().unwrap();
            continuation.resume_input_action = Some(action);
            continuation.resume_input = if action == CapabilityInputAction::Accept {
                let value = serde_json::json!({"city": "Shanghai"});
                let mut input = request.input.clone();
                input.exact.schema_digest = response_schema.canonical_digest.clone();
                input.exact.content_digest = canonical_digest(&value).unwrap().parse().unwrap();
                input.material = CapabilityExecutionInputMaterial::Inline { value };
                Some(input)
            } else {
                None
            };
            *host.outcome.lock().unwrap() = Some(McpOperationOutcome::RemoteTask {
                encrypted_state: state.clone(),
                external_identity_digest: input_request.external_identity_digest.clone(),
                next_poll_at: Utc::now() + ChronoDuration::milliseconds(100),
            });
            assert!(matches!(
                adapter.invoke(&request).await.unwrap().outcome,
                DispatchOutcome::Deferred(_)
            ));
            let observed = host.observed.lock().unwrap().clone().unwrap();
            let response = observed.continuation.unwrap().elicitation_response.unwrap();
            assert_eq!(
                response.action,
                match action {
                    CapabilityInputAction::Accept =>
                        insight_platform_mcp_host::McpElicitationAction::Accept,
                    CapabilityInputAction::Decline =>
                        insight_platform_mcp_host::McpElicitationAction::Decline,
                    CapabilityInputAction::Cancel =>
                        insight_platform_mcp_host::McpElicitationAction::Cancel,
                }
            );
            assert_eq!(
                response.content.is_some(),
                action == CapabilityInputAction::Accept
            );
        }
    }

    #[tokio::test]
    async fn malformed_input_is_rejected_before_any_adapter_call() {
        let worker_manifest = digest('a');
        let execution = native_execution(worker_manifest.clone(), 1_000);
        let mut malformed = request(execution, worker_manifest);
        malformed.input.exact.content_digest = digest('0');
        let dispatcher = CapabilityDispatcher::new(InstalledNativeRegistry::default());
        let failure = dispatcher.dispatch(&malformed).await.unwrap_err();
        assert_eq!(failure.safe_code, "invalid_request");
    }
}
