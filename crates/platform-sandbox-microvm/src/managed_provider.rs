use crate::{provider_contract_failure, MicroVmProviderFailure};
use async_trait::async_trait;
use insight_platform_contracts::{ResourceKind, SandboxIsolationClass, Sha256Digest};
use insight_platform_jobs::JobFence;
use insight_platform_sandbox::{
    ActivatedManagedMcpSandboxSession, InstalledSandboxBackendDescriptor,
    ManagedMcpSandboxSessionProvider, ManagedMcpSandboxSessionRequest,
    PreparedManagedMcpSandboxSession, PreparedManagedMcpSandboxSessionActivation,
    SandboxCommandLimits, SandboxIsolationBackendKind,
};
use std::sync::Arc;

/// Physical long-lived microVM lifecycle. Its implementation owns deterministic instance
/// uniqueness, the live guest channel, Secret injection, state sealing and exact destruction.
/// PostgreSQL Job/session authority remains outside this port.
#[async_trait]
pub trait ManagedMcpMicroVmSessionLifecyclePort: Send + Sync {
    async fn prepare_exact(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        executor_identity_digest: &Sha256Digest,
    ) -> Result<PreparedManagedMcpSandboxSession, MicroVmProviderFailure>;

    async fn initialize_exact(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: &PreparedManagedMcpSandboxSession,
    ) -> Result<PreparedManagedMcpSandboxSessionActivation, MicroVmProviderFailure>;

    async fn activate_exact(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        activation: &PreparedManagedMcpSandboxSessionActivation,
    ) -> Result<ActivatedManagedMcpSandboxSession, MicroVmProviderFailure>;

    async fn destroy_exact(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: Option<&PreparedManagedMcpSandboxSession>,
    ) -> Result<(), MicroVmProviderFailure>;
}

pub trait ManagedMcpProviderClock: Send + Sync {
    fn now(&self) -> chrono::DateTime<chrono::Utc>;
}

pub struct SystemManagedMcpProviderClock;

impl ManagedMcpProviderClock for SystemManagedMcpProviderClock {
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

/// Contract-validating adapter exposed through the dedicated Managed MCP Provider RPC. It never
/// translates a session into `SandboxExecutionRequest`; both request identity and phase fence stay
/// intact through the physical lifecycle.
pub struct ManagedMcpMicroVmSessionProvider<V, C> {
    descriptor: InstalledSandboxBackendDescriptor,
    limits: SandboxCommandLimits,
    lifecycle: Arc<V>,
    clock: Arc<C>,
}

impl<V, C> ManagedMcpMicroVmSessionProvider<V, C>
where
    V: ManagedMcpMicroVmSessionLifecyclePort,
    C: ManagedMcpProviderClock,
{
    pub fn new(
        descriptor: InstalledSandboxBackendDescriptor,
        limits: SandboxCommandLimits,
        lifecycle: Arc<V>,
        clock: Arc<C>,
    ) -> Result<Self, MicroVmProviderFailure> {
        if descriptor.backend_kind != SandboxIsolationBackendKind::MicroVm
            || descriptor.isolation_class != SandboxIsolationClass::MicroVm
        {
            return Err(provider_contract_failure(
                "managed_mcp_microvm_provider_configuration",
            ));
        }
        Ok(Self {
            descriptor,
            limits,
            lifecycle,
            clock,
        })
    }

    fn validate_live(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
    ) -> Result<(), MicroVmProviderFailure> {
        request
            .validate_at(self.clock.now(), self.limits)
            .map_err(|_| provider_contract_failure("managed_mcp_microvm_request_invalid"))?;
        self.validate_contract(request, fence)
    }

    fn validate_cleanup(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
    ) -> Result<(), MicroVmProviderFailure> {
        let sealed = request
            .clone()
            .seal()
            .map_err(|_| provider_contract_failure("managed_mcp_microvm_request_invalid"))?;
        if &sealed != request {
            return Err(provider_contract_failure(
                "managed_mcp_microvm_request_digest_invalid",
            ));
        }
        self.validate_contract(request, fence)
    }

    fn validate_contract(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
    ) -> Result<(), MicroVmProviderFailure> {
        if request.identity.sandbox_job_id.kind() != ResourceKind::SandboxJob
            || fence.expected_version == 0
            || fence.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || fence.lease_generation == 0
            || request.isolation_class != SandboxIsolationClass::MicroVm
            || request.executor_worker_manifest_digest != self.descriptor.worker_manifest_digest
            || request.isolation_backend_contract_digest != self.descriptor.backend_contract_digest
        {
            return Err(provider_contract_failure(
                "managed_mcp_microvm_provider_contract_mismatch",
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl<V, C> ManagedMcpSandboxSessionProvider for ManagedMcpMicroVmSessionProvider<V, C>
where
    V: ManagedMcpMicroVmSessionLifecyclePort + 'static,
    C: ManagedMcpProviderClock + 'static,
{
    type Error = MicroVmProviderFailure;

    async fn prepare(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        executor_identity_digest: &Sha256Digest,
    ) -> Result<PreparedManagedMcpSandboxSession, Self::Error> {
        self.validate_live(request, fence)?;
        let prepared = self
            .lifecycle
            .prepare_exact(request, fence, executor_identity_digest)
            .await?;
        if prepared
            .validate_for(request, fence, executor_identity_digest)
            .is_err()
        {
            self.lifecycle
                .destroy_exact(request, fence, Some(&prepared))
                .await?;
            return Err(provider_contract_failure(
                "managed_mcp_microvm_prepared_evidence_invalid",
            ));
        }
        Ok(prepared)
    }

    async fn initialize(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: &PreparedManagedMcpSandboxSession,
    ) -> Result<PreparedManagedMcpSandboxSessionActivation, Self::Error> {
        self.validate_live(request, fence)?;
        prepared
            .validate_for(request, fence, &prepared.executor_identity_digest)
            .map_err(|_| {
                provider_contract_failure("managed_mcp_microvm_prepared_identity_invalid")
            })?;
        let activation = self
            .lifecycle
            .initialize_exact(request, fence, prepared)
            .await?;
        if activation
            .validate_for(
                request,
                fence,
                &prepared.executor_identity_digest,
                self.clock.now(),
            )
            .is_err()
        {
            self.lifecycle
                .destroy_exact(request, fence, Some(prepared))
                .await?;
            return Err(provider_contract_failure(
                "managed_mcp_microvm_activation_binding_invalid",
            ));
        }
        Ok(activation)
    }

    async fn activate(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        activation: &PreparedManagedMcpSandboxSessionActivation,
    ) -> Result<ActivatedManagedMcpSandboxSession, Self::Error> {
        self.validate_live(request, fence)?;
        activation
            .validate_for(
                request,
                fence,
                &activation.prepared.executor_identity_digest,
                self.clock.now(),
            )
            .map_err(|_| {
                provider_contract_failure("managed_mcp_microvm_activation_identity_invalid")
            })?;
        let activated = self
            .lifecycle
            .activate_exact(request, fence, activation)
            .await?;
        if activated
            .validate_for(activation, self.clock.now())
            .is_err()
        {
            self.lifecycle
                .destroy_exact(request, fence, Some(&activation.prepared))
                .await?;
            return Err(provider_contract_failure(
                "managed_mcp_microvm_activated_evidence_invalid",
            ));
        }
        Ok(activated)
    }

    async fn destroy_exact(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: Option<&PreparedManagedMcpSandboxSession>,
    ) -> Result<(), Self::Error> {
        self.validate_cleanup(request, fence)?;
        if let Some(prepared) = prepared {
            prepared
                .validate_for(request, fence, &prepared.executor_identity_digest)
                .map_err(|_| {
                    provider_contract_failure("managed_mcp_microvm_cleanup_identity_invalid")
                })?;
        }
        self.lifecycle.destroy_exact(request, fence, prepared).await
    }
}
