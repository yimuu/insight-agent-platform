//! Versioned internal gRPC adapter for the Sandbox Executor authority.
//!
//! Transport security is mandatory in production composition: callers construct a tonic Channel
//! with client identity and private CA, while the Controller server requires the corresponding
//! client CA. This crate owns only bounded wire conversion and authority delegation.

mod control;

pub use control::*;

use async_trait::async_trait;
#[cfg(test)]
use insight_platform_contracts::ArtifactGrantOperation;
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, ArtifactRef, CommandOutcome, HardLimitProfile, JsonLimits,
    Retryability, Sha256Digest,
};
use insight_platform_jobs::JobFence;
use insight_platform_sandbox::{
    AbortSandboxExecution, ActivatedManagedMcpSandboxSession,
    AuthorizedManagedMcpSandboxSecretDelivery, ClaimSandboxJobs, ClaimedManagedMcpSandboxSession,
    ClaimedSandboxJob, CollectedSandbox, CommitManagedMcpSandboxSessionLost,
    CommitManagedMcpSandboxSessionPhase, CommitManagedMcpSandboxSessionReady, CommitSandboxOutcome,
    CommitSandboxPhase, DestroySandbox, ExpiredManagedMcpSandboxSessionLease,
    ExpiredManagedMcpSandboxSessionLeasePage, ExpiredSandboxLease, HeartbeatSandboxExecution,
    InstalledSandboxBackendDescriptor, ManagedMcpSandboxSecretCommitOutcome,
    ManagedMcpSandboxSecretDeliveryAuthority, ManagedMcpSandboxSecretDeliveryError,
    ManagedMcpSandboxSecretDeliveryRequest, ManagedMcpSandboxSecretReservationOutcome,
    ManagedMcpSandboxSessionClaimAuthority, ManagedMcpSandboxSessionCleanupOutcome,
    ManagedMcpSandboxSessionExecutionAuthority, ManagedMcpSandboxSessionLeaseRecoveryResult,
    ManagedMcpSandboxSessionLivenessEvidence, ManagedMcpSandboxSessionPhaseDecision,
    ManagedMcpSandboxSessionProvider, ManagedMcpSandboxSessionRecoveryAuthority,
    ManagedMcpSandboxSessionRecoveryFailure, ManagedMcpSandboxSessionRequest,
    MicroVmArtifactBroker, MicroVmArtifactBrokerError, MicroVmArtifactReadRequest,
    MicroVmGrantRevocationError, MicroVmGrantRevocationEvidence, MicroVmGrantRevoker,
    MicroVmIsolationProviderBackend, MicroVmProviderExecutionFence,
    PreparedManagedMcpSandboxSession, PreparedManagedMcpSandboxSessionActivation, PreparedSandbox,
    ProveSandboxProcessGenerationAbsent, RecoverExpiredManagedMcpSandboxSessionLease,
    RegisterWasiExecutorProcessGeneration, RevokeMicroVmSandboxGrants, RevokeWasiSandboxGrants,
    RunningSandbox, SandboxBackendFailure, SandboxBackendFailureStage, SandboxClaimAuthority,
    SandboxClaimFailure, SandboxCleanupEvidence, SandboxCommandLimits, SandboxExecutionAuthority,
    SandboxExecutionRequest, SandboxExecutorBackend, SandboxIsolationBackendKind,
    SandboxLeaseRecoveryEvidence, SandboxPhaseDecision, SandboxProcessGenerationAbsenceEvidence,
    SandboxProcessGenerationIsolation, SandboxProcessGenerationIsolationError,
    SandboxTerminationEvidence, ScanExpiredManagedMcpSandboxSessionLeases, TerminateSandbox,
    VerifyWasiExecutorProcessGeneration, WasiArtifactBroker, WasiArtifactBrokerError,
    WasiArtifactReadPurpose, WasiArtifactReadRequest, WasiExecutorProcessAttestationAuthority,
    WasiExecutorProcessIdentityEvidence, WasiExecutorProcessRegistrar,
    WasiExecutorProcessRegistrationError, WasiExecutorProcessRegistrationVerifier,
    WasiExecutorRegistrationPeer, WasiGrantRevocationError, WasiGrantRevocationEvidence,
    WasiGrantRevoker, WasiValueValidationError, WasiValueValidationRequest, WasiValueValidator,
};
#[cfg(test)]
use insight_platform_sandbox::{
    ManagedMcpSandboxSessionRecoveryExecutor, ManagedMcpSandboxSessionRecoveryShard,
    MicroVmArtifactReadPurpose, MicroVmSandboxWorkloadKind, ScopedArtifactGrant,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    error::Error,
    fmt,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::transport::server::{TlsConnectInfo, UdsConnectInfo};
use tonic::{Request, Response, Status};
use x509_parser::{extensions::GeneralName, parse_x509_certificate};

pub mod proto {
    tonic::include_proto!("insight.platform.v1");
}

use proto::{
    sandbox_executor_authority_service_client::SandboxExecutorAuthorityServiceClient,
    sandbox_executor_authority_service_server::SandboxExecutorAuthorityService,
    sandbox_executor_broker_service_client::SandboxExecutorBrokerServiceClient,
    sandbox_executor_broker_service_server::SandboxExecutorBrokerService,
    sandbox_executor_process_registration_service_client::SandboxExecutorProcessRegistrationServiceClient,
    sandbox_executor_process_registration_service_server::SandboxExecutorProcessRegistrationService,
    sandbox_isolation_provider_service_client::SandboxIsolationProviderServiceClient,
    sandbox_isolation_provider_service_server::SandboxIsolationProviderService,
    sandbox_managed_mcp_session_authority_service_client::SandboxManagedMcpSessionAuthorityServiceClient,
    sandbox_managed_mcp_session_authority_service_server::SandboxManagedMcpSessionAuthorityService,
    sandbox_managed_mcp_session_provider_service_client::SandboxManagedMcpSessionProviderServiceClient,
    sandbox_managed_mcp_session_provider_service_server::SandboxManagedMcpSessionProviderService,
    sandbox_micro_vm_broker_service_client::SandboxMicroVmBrokerServiceClient,
    sandbox_micro_vm_broker_service_server::SandboxMicroVmBrokerService,
    sandbox_micro_vm_executor_process_registration_service_client::SandboxMicroVmExecutorProcessRegistrationServiceClient,
    sandbox_micro_vm_executor_process_registration_service_server::SandboxMicroVmExecutorProcessRegistrationService,
    sandbox_process_isolation_attestor_service_client::SandboxProcessIsolationAttestorServiceClient,
    sandbox_process_isolation_attestor_service_server::SandboxProcessIsolationAttestorService,
    sandbox_secret_delivery_authority_service_client::SandboxSecretDeliveryAuthorityServiceClient,
    sandbox_secret_delivery_authority_service_server::SandboxSecretDeliveryAuthorityService,
    ClosedSandboxEnvelope, SandboxArtifactChunkEnvelope,
};

pub const SANDBOX_INTERNAL_RPC_SCHEMA_VERSION: u32 = 1;
pub const WASI_EXECUTOR_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/sandbox-executor.wasi";
pub const SANDBOX_CONTROLLER_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/sandbox-controller";
pub const MICROVM_EXECUTOR_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/sandbox-executor.microvm";
pub const MICROVM_PROVIDER_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/sandbox-provider.microvm";
pub const EGRESS_BROKER_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/egress-broker";
const SANDBOX_ARTIFACT_CHUNK_BYTES: usize = 1024 * 1024;
const SANDBOX_ARTIFACT_MESSAGE_OVERHEAD_BYTES: usize = 1024;
const MAX_IN_FLIGHT_ARTIFACT_RESPONSES_HARD: usize = 4;

/// Authorizes the mTLS-authenticated peer before any Sandbox request body is decoded.
///
/// TLS chain and validity verification remain the responsibility of tonic/rustls configured with
/// the private client CA. This gate owns only endpoint-role authorization from the verified leaf
/// certificate and deliberately ignores CN, DNS SAN and request metadata.
#[derive(Debug, Clone, Copy, Default)]
pub struct WasiExecutorWorkloadIdentity;

impl tonic::service::Interceptor for WasiExecutorWorkloadIdentity {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let certificates = request
            .peer_certs()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        let leaf = certificates
            .first()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        require_exact_workload_uri(leaf.as_ref(), WASI_EXECUTOR_WORKLOAD_IDENTITY)?;
        Ok(request)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SandboxControllerWorkloadIdentity;

impl tonic::service::Interceptor for SandboxControllerWorkloadIdentity {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let certificates = request
            .peer_certs()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        let leaf = certificates
            .first()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        require_exact_workload_uri(leaf.as_ref(), SANDBOX_CONTROLLER_WORKLOAD_IDENTITY)?;
        Ok(request)
    }
}

/// Authorizes only the dedicated microVM Executor at the provider lifecycle boundary.
#[derive(Debug, Clone, Copy, Default)]
pub struct MicroVmExecutorWorkloadIdentity;

impl tonic::service::Interceptor for MicroVmExecutorWorkloadIdentity {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let certificates = request
            .peer_certs()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        let leaf = certificates
            .first()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        require_exact_workload_uri(leaf.as_ref(), MICROVM_EXECUTOR_WORKLOAD_IDENTITY)?;
        Ok(request)
    }
}

/// Authorizes only the privileged node-local microVM Provider at Controller broker methods.
#[derive(Debug, Clone, Copy, Default)]
pub struct MicroVmProviderWorkloadIdentity;

impl tonic::service::Interceptor for MicroVmProviderWorkloadIdentity {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let certificates = request
            .peer_certs()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        let leaf = certificates
            .first()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        require_exact_workload_uri(leaf.as_ref(), MICROVM_PROVIDER_WORKLOAD_IDENTITY)?;
        Ok(request)
    }
}

/// Authorizes only the independently deployed Egress Broker at the credential-free Secret
/// delivery authority boundary.
#[derive(Debug, Clone, Copy, Default)]
pub struct EgressBrokerWorkloadIdentity;

impl tonic::service::Interceptor for EgressBrokerWorkloadIdentity {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let certificates = request
            .peer_certs()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        let leaf = certificates
            .first()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        require_exact_workload_uri(leaf.as_ref(), EGRESS_BROKER_WORKLOAD_IDENTITY)?;
        Ok(request)
    }
}

/// Shared durable-authority endpoint for the two closed Sandbox Executor roles. Backend-specific
/// broker and provider endpoints retain their stricter one-role interceptors.
#[derive(Debug, Clone, Copy, Default)]
pub struct SandboxExecutorAuthorityWorkloadIdentity;

impl tonic::service::Interceptor for SandboxExecutorAuthorityWorkloadIdentity {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let certificates = request
            .peer_certs()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        let leaf = certificates
            .first()
            .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
        require_allowed_workload_uri(
            leaf.as_ref(),
            &[
                WASI_EXECUTOR_WORKLOAD_IDENTITY,
                MICROVM_EXECUTOR_WORKLOAD_IDENTITY,
            ],
        )?;
        Ok(request)
    }
}

/// Authorizes the Executor registration role and binds the request to kernel-authenticated Unix
/// peer credentials. A TCP request cannot satisfy this interceptor even with a CA-valid
/// certificate.
#[derive(Debug, Clone, Copy, Default)]
pub struct WasiExecutorNodeRegistrationIdentity;

impl tonic::service::Interceptor for WasiExecutorNodeRegistrationIdentity {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        authorize_node_registration(request, WASI_EXECUTOR_WORKLOAD_IDENTITY)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MicroVmExecutorNodeRegistrationIdentity;

impl tonic::service::Interceptor for MicroVmExecutorNodeRegistrationIdentity {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        authorize_node_registration(request, MICROVM_EXECUTOR_WORKLOAD_IDENTITY)
    }
}

fn authorize_node_registration(
    mut request: Request<()>,
    expected_workload_identity: &str,
) -> Result<Request<()>, Status> {
    let connection = request
        .extensions()
        .get::<TlsConnectInfo<UdsConnectInfo>>()
        .ok_or_else(|| Status::permission_denied("node-local registration is required"))?;
    let certificates = connection
        .peer_certs()
        .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
    let leaf = certificates
        .first()
        .ok_or_else(|| Status::unauthenticated("client certificate is required"))?;
    require_exact_workload_uri(leaf.as_ref(), expected_workload_identity)?;
    let credentials = connection
        .get_ref()
        .peer_cred
        .ok_or_else(|| Status::permission_denied("Unix peer credentials are required"))?;
    let host_process_id = credentials
        .pid()
        .and_then(|value| u32::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| Status::permission_denied("Unix peer process identity is required"))?;
    let peer = WasiExecutorRegistrationPeer {
        host_process_id,
        host_user_id: credentials.uid(),
        host_group_id: credentials.gid(),
    };
    peer.validate()
        .map_err(|_| Status::permission_denied("Unix peer identity is invalid"))?;
    request.extensions_mut().insert(peer);
    Ok(request)
}

fn require_exact_workload_uri(certificate: &[u8], expected: &str) -> Result<(), Status> {
    require_allowed_workload_uri(certificate, &[expected])
}

fn require_allowed_workload_uri(certificate: &[u8], allowed: &[&str]) -> Result<(), Status> {
    let (remainder, certificate) = parse_x509_certificate(certificate)
        .map_err(|_| Status::unauthenticated("client certificate is invalid"))?;
    if !remainder.is_empty() {
        return Err(Status::unauthenticated("client certificate is invalid"));
    }
    let alternative_names = certificate
        .subject_alternative_name()
        .map_err(|_| Status::unauthenticated("client certificate identity is invalid"))?
        .ok_or_else(|| Status::permission_denied("workload identity is not authorized"))?;
    let mut uris = alternative_names
        .value
        .general_names
        .iter()
        .filter_map(|name| match name {
            GeneralName::URI(uri) => Some(*uri),
            _ => None,
        });
    if uris
        .next()
        .is_none_or(|identity| !allowed.contains(&identity))
        || uris.next().is_some()
    {
        return Err(Status::permission_denied(
            "workload identity is not authorized",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SandboxInternalRpcLimits {
    maximum_message_bytes: usize,
    maximum_claim_batch: u16,
    maximum_lease_milliseconds: u64,
    maximum_recovery_batch: u16,
    maximum_recovery_shards: u16,
    sandbox_command_limits: SandboxCommandLimits,
}

impl SandboxInternalRpcLimits {
    pub fn from_profile(profile: &HardLimitProfile) -> Result<Self, SandboxRpcError> {
        profile
            .validate()
            .map_err(|_| SandboxRpcError::InvalidConfiguration)?;
        let maximum_message_bytes = usize::try_from(profile.api.decoded_body_bytes.hard_max)
            .map_err(|_| SandboxRpcError::InvalidConfiguration)?;
        let maximum_claim_batch = u16::try_from(profile.run_scheduler.claim_batch.hard_max)
            .map_err(|_| SandboxRpcError::InvalidConfiguration)?;
        let maximum_recovery_batch = u16::try_from(profile.control_data.recovery_batch.q1_default)
            .map_err(|_| SandboxRpcError::InvalidConfiguration)?;
        let maximum_recovery_shards = u16::try_from(profile.control_data.recovery_shards.hard_max)
            .map_err(|_| SandboxRpcError::InvalidConfiguration)?;
        let limits = Self {
            maximum_message_bytes,
            maximum_claim_batch,
            maximum_lease_milliseconds: profile.run_scheduler.lease_milliseconds.hard_max,
            maximum_recovery_batch,
            maximum_recovery_shards,
            sandbox_command_limits: SandboxCommandLimits::from_profile(profile)
                .map_err(|_| SandboxRpcError::InvalidConfiguration)?,
        };
        if maximum_message_bytes == 0
            || maximum_claim_batch == 0
            || limits.maximum_lease_milliseconds == 0
            || maximum_recovery_batch == 0
            || maximum_recovery_shards == 0
        {
            return Err(SandboxRpcError::InvalidConfiguration);
        }
        Ok(limits)
    }

    pub const fn maximum_message_bytes(self) -> usize {
        self.maximum_message_bytes
    }
}

#[derive(Clone)]
pub struct SandboxAuthorityGrpcClient {
    client: SandboxExecutorAuthorityServiceClient<tonic::transport::Channel>,
    limits: SandboxInternalRpcLimits,
}

impl SandboxAuthorityGrpcClient {
    pub fn new(channel: tonic::transport::Channel, limits: SandboxInternalRpcLimits) -> Self {
        let maximum = limits.maximum_message_bytes();
        Self {
            client: SandboxExecutorAuthorityServiceClient::new(channel)
                .max_encoding_message_size(maximum)
                .max_decoding_message_size(maximum),
            limits,
        }
    }

    async fn unary<Req, Res>(
        &self,
        request: &Req,
        invoke: impl for<'a> FnOnce(
            &'a mut SandboxExecutorAuthorityServiceClient<tonic::transport::Channel>,
            Request<ClosedSandboxEnvelope>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<Response<ClosedSandboxEnvelope>, Status>>
                    + Send
                    + 'a,
            >,
        >,
    ) -> Result<Res, SandboxRpcError>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        let envelope = encode(request, self.limits)?;
        let mut client = self.client.clone();
        let response = invoke(&mut client, Request::new(envelope))
            .await
            .map_err(classify_status)?;
        decode(response.into_inner(), self.limits)
    }
}

#[derive(Clone)]
pub struct SandboxManagedMcpSessionAuthorityGrpcClient {
    client: SandboxManagedMcpSessionAuthorityServiceClient<tonic::transport::Channel>,
    limits: SandboxInternalRpcLimits,
}

#[derive(Clone)]
pub struct SandboxSecretDeliveryAuthorityGrpcClient {
    client: SandboxSecretDeliveryAuthorityServiceClient<tonic::transport::Channel>,
    limits: SandboxInternalRpcLimits,
}

impl SandboxSecretDeliveryAuthorityGrpcClient {
    pub fn new(channel: tonic::transport::Channel, limits: SandboxInternalRpcLimits) -> Self {
        let maximum = limits.maximum_message_bytes();
        Self {
            client: SandboxSecretDeliveryAuthorityServiceClient::new(channel)
                .max_encoding_message_size(maximum)
                .max_decoding_message_size(maximum),
            limits,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommitManagedMcpSandboxSecretDeliveryWire {
    request: ManagedMcpSandboxSecretDeliveryRequest,
    authorization: AuthorizedManagedMcpSandboxSecretDelivery,
    resolution_evidence_digest: Sha256Digest,
}

impl SandboxManagedMcpSessionAuthorityGrpcClient {
    pub fn new(channel: tonic::transport::Channel, limits: SandboxInternalRpcLimits) -> Self {
        let maximum = limits.maximum_message_bytes();
        Self {
            client: SandboxManagedMcpSessionAuthorityServiceClient::new(channel)
                .max_encoding_message_size(maximum)
                .max_decoding_message_size(maximum),
            limits,
        }
    }

    async fn unary<Req, Res>(
        &self,
        request: &Req,
        invoke: impl for<'a> FnOnce(
            &'a mut SandboxManagedMcpSessionAuthorityServiceClient<tonic::transport::Channel>,
            Request<ClosedSandboxEnvelope>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<Response<ClosedSandboxEnvelope>, Status>>
                    + Send
                    + 'a,
            >,
        >,
    ) -> Result<Res, SandboxRpcError>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        let envelope = encode(request, self.limits)?;
        let mut client = self.client.clone();
        let response = invoke(&mut client, Request::new(envelope))
            .await
            .map_err(classify_status)?;
        decode(response.into_inner(), self.limits)
    }
}

#[derive(Clone)]
pub struct SandboxBrokerGrpcClient {
    client: SandboxExecutorBrokerServiceClient<tonic::transport::Channel>,
    limits: SandboxInternalRpcLimits,
}

#[derive(Clone)]
pub struct SandboxMicroVmBrokerGrpcClient {
    client: SandboxMicroVmBrokerServiceClient<tonic::transport::Channel>,
    limits: SandboxInternalRpcLimits,
}

#[derive(Clone)]
pub struct SandboxProcessIsolationAttestorGrpcClient {
    client: SandboxProcessIsolationAttestorServiceClient<tonic::transport::Channel>,
    limits: SandboxInternalRpcLimits,
    attestor_identity_digest: Sha256Digest,
}

#[derive(Clone)]
pub struct SandboxExecutorProcessRegistrationGrpcClient {
    client: SandboxExecutorProcessRegistrationServiceClient<tonic::transport::Channel>,
    limits: SandboxInternalRpcLimits,
    attestor_identity_digest: Sha256Digest,
}

#[derive(Clone)]
pub struct SandboxMicroVmExecutorProcessRegistrationGrpcClient {
    client: SandboxMicroVmExecutorProcessRegistrationServiceClient<tonic::transport::Channel>,
    limits: SandboxInternalRpcLimits,
    attestor_identity_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
#[serde(
    tag = "disposition",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum SandboxIsolationProviderReply<T> {
    Completed(T),
    Failed(SandboxBackendFailure),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrepareManagedMcpSandboxSessionWire {
    request: ManagedMcpSandboxSessionRequest,
    fence: JobFence,
    executor_identity_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InitializeManagedMcpSandboxSessionWire {
    request: ManagedMcpSandboxSessionRequest,
    fence: JobFence,
    prepared: PreparedManagedMcpSandboxSession,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivateManagedMcpSandboxSessionWire {
    request: ManagedMcpSandboxSessionRequest,
    fence: JobFence,
    activation: PreparedManagedMcpSandboxSessionActivation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObserveExactManagedMcpSandboxSessionWire {
    request: ManagedMcpSandboxSessionRequest,
    fence: JobFence,
    prepared: PreparedManagedMcpSandboxSession,
    activated: ActivatedManagedMcpSandboxSession,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DestroyExactManagedMcpSandboxSessionWire {
    request: ManagedMcpSandboxSessionRequest,
    fence: JobFence,
    prepared: Option<PreparedManagedMcpSandboxSession>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "disposition",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum ManagedMcpSandboxSessionProviderReply<T> {
    Completed(T),
    Failed,
}

/// Executor-side client for the long-lived Managed MCP microVM lifecycle. A lost prepare reply is
/// followed by exact keyed destruction before the method returns, because the worker has no
/// prepared identity with which to perform its normal cleanup path.
#[derive(Clone)]
pub struct SandboxManagedMcpSessionProviderGrpcClient {
    client: SandboxManagedMcpSessionProviderServiceClient<tonic::transport::Channel>,
    limits: SandboxInternalRpcLimits,
}

impl SandboxManagedMcpSessionProviderGrpcClient {
    pub fn new(channel: tonic::transport::Channel, limits: SandboxInternalRpcLimits) -> Self {
        let maximum = limits.maximum_message_bytes();
        Self {
            client: SandboxManagedMcpSessionProviderServiceClient::new(channel)
                .max_encoding_message_size(maximum)
                .max_decoding_message_size(maximum),
            limits,
        }
    }

    async fn unary<Req, Res>(
        &self,
        request: &Req,
        invoke: impl for<'a> FnOnce(
            &'a mut SandboxManagedMcpSessionProviderServiceClient<tonic::transport::Channel>,
            Request<ClosedSandboxEnvelope>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<Response<ClosedSandboxEnvelope>, Status>>
                    + Send
                    + 'a,
            >,
        >,
    ) -> Result<Res, SandboxRpcError>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        let envelope = encode(request, self.limits)?;
        let mut client = self.client.clone();
        let response = invoke(&mut client, Request::new(envelope))
            .await
            .map_err(classify_status)?;
        match decode::<ManagedMcpSandboxSessionProviderReply<Res>>(
            response.into_inner(),
            self.limits,
        )? {
            ManagedMcpSandboxSessionProviderReply::Completed(value) => Ok(value),
            ManagedMcpSandboxSessionProviderReply::Failed => Err(SandboxRpcError::Rejected),
        }
    }

    async fn destroy_remote(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: Option<&PreparedManagedMcpSandboxSession>,
    ) -> Result<ManagedMcpSandboxSessionCleanupOutcome, SandboxRpcError> {
        let wire = DestroyExactManagedMcpSandboxSessionWire {
            request: request.clone(),
            fence: fence.clone(),
            prepared: prepared.cloned(),
        };
        self.unary(&wire, |client, request| {
            Box::pin(client.destroy_exact_managed_mcp_sandbox_session(request))
        })
        .await
    }
}

#[derive(Clone)]
pub struct SandboxIsolationProviderGrpcClient {
    client: SandboxIsolationProviderServiceClient<tonic::transport::Channel>,
    limits: SandboxInternalRpcLimits,
    descriptor: InstalledSandboxBackendDescriptor,
    fence: MicroVmProviderExecutionFence,
}

impl SandboxIsolationProviderGrpcClient {
    pub fn new(
        channel: tonic::transport::Channel,
        limits: SandboxInternalRpcLimits,
        descriptor: InstalledSandboxBackendDescriptor,
        worker_process_generation_id: insight_platform_contracts::ResourceId,
    ) -> Result<Self, SandboxRpcError> {
        let fence = MicroVmProviderExecutionFence {
            worker_process_generation_id,
        };
        if descriptor.backend_kind != SandboxIsolationBackendKind::MicroVm
            || descriptor.isolation_class
                != insight_platform_contracts::SandboxIsolationClass::MicroVm
            || fence.validate().is_err()
        {
            return Err(SandboxRpcError::InvalidConfiguration);
        }
        let maximum = limits.maximum_message_bytes();
        Ok(Self {
            client: SandboxIsolationProviderServiceClient::new(channel)
                .max_encoding_message_size(maximum)
                .max_decoding_message_size(maximum),
            limits,
            descriptor,
            fence,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn unary<Req, Res>(
        &self,
        request: &Req,
        stage: SandboxBackendFailureStage,
        execution_may_have_started: bool,
        external_effect_possible: bool,
        request_digest: &Sha256Digest,
        sandbox_identity_digest: Option<&Sha256Digest>,
        invoke: impl for<'a> FnOnce(
            &'a mut SandboxIsolationProviderServiceClient<tonic::transport::Channel>,
            Request<ClosedSandboxEnvelope>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<Response<ClosedSandboxEnvelope>, Status>>
                    + Send
                    + 'a,
            >,
        >,
    ) -> Result<Res, SandboxBackendFailure>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        let envelope = encode(request, self.limits).map_err(|error| {
            isolation_provider_failure(
                stage,
                execution_may_have_started,
                external_effect_possible,
                request_digest,
                sandbox_identity_digest,
                error,
            )
        })?;
        let mut client = self.client.clone();
        let response = invoke(&mut client, Request::new(envelope))
            .await
            .map_err(classify_status)
            .and_then(|response| {
                decode::<SandboxIsolationProviderReply<Res>>(response.into_inner(), self.limits)
            })
            .map_err(|error| {
                isolation_provider_failure(
                    stage,
                    execution_may_have_started,
                    external_effect_possible,
                    request_digest,
                    sandbox_identity_digest,
                    error,
                )
            })?;
        match response {
            SandboxIsolationProviderReply::Completed(value) => Ok(value),
            SandboxIsolationProviderReply::Failed(failure)
                if failure.stage == stage
                    && failure.execution_may_have_started == execution_may_have_started
                    && failure.external_effect_possible == external_effect_possible =>
            {
                Err(failure)
            }
            SandboxIsolationProviderReply::Failed(_) => Err(isolation_provider_failure(
                stage,
                execution_may_have_started,
                external_effect_possible,
                request_digest,
                sandbox_identity_digest,
                SandboxRpcError::Rejected,
            )),
        }
    }
}

impl SandboxProcessIsolationAttestorGrpcClient {
    pub fn new(
        channel: tonic::transport::Channel,
        limits: SandboxInternalRpcLimits,
        attestor_identity_digest: Sha256Digest,
    ) -> Self {
        let maximum = limits.maximum_message_bytes();
        Self {
            client: SandboxProcessIsolationAttestorServiceClient::new(channel)
                .max_encoding_message_size(maximum)
                .max_decoding_message_size(maximum),
            limits,
            attestor_identity_digest,
        }
    }
}

impl SandboxExecutorProcessRegistrationGrpcClient {
    pub fn new(
        channel: tonic::transport::Channel,
        limits: SandboxInternalRpcLimits,
        attestor_identity_digest: Sha256Digest,
    ) -> Self {
        let maximum = limits.maximum_message_bytes();
        Self {
            client: SandboxExecutorProcessRegistrationServiceClient::new(channel)
                .max_encoding_message_size(maximum)
                .max_decoding_message_size(maximum),
            limits,
            attestor_identity_digest,
        }
    }
}

impl SandboxMicroVmExecutorProcessRegistrationGrpcClient {
    pub fn new(
        channel: tonic::transport::Channel,
        limits: SandboxInternalRpcLimits,
        attestor_identity_digest: Sha256Digest,
    ) -> Self {
        let maximum = limits.maximum_message_bytes();
        Self {
            client: SandboxMicroVmExecutorProcessRegistrationServiceClient::new(channel)
                .max_encoding_message_size(maximum)
                .max_decoding_message_size(maximum),
            limits,
            attestor_identity_digest,
        }
    }
}

#[async_trait]
impl WasiExecutorProcessRegistrar for SandboxExecutorProcessRegistrationGrpcClient {
    async fn register(
        &self,
        request: RegisterWasiExecutorProcessGeneration,
    ) -> Result<WasiExecutorProcessIdentityEvidence, WasiExecutorProcessRegistrationError> {
        request.validate()?;
        let envelope = encode(&request, self.limits)
            .map_err(|_| WasiExecutorProcessRegistrationError::Rejected)?;
        let mut client = self.client.clone();
        let evidence: WasiExecutorProcessIdentityEvidence = client
            .register_wasi_executor_process_generation(Request::new(envelope))
            .await
            .map_err(|status| match status.code() {
                tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
                    WasiExecutorProcessRegistrationError::Unavailable
                }
                _ => WasiExecutorProcessRegistrationError::Rejected,
            })
            .and_then(|response| {
                decode(response.into_inner(), self.limits)
                    .map_err(|_| WasiExecutorProcessRegistrationError::Rejected)
            })?;
        evidence.validate_for(&request, &self.attestor_identity_digest, chrono::Utc::now())?;
        Ok(evidence)
    }
}

#[async_trait]
impl WasiExecutorProcessRegistrar for SandboxMicroVmExecutorProcessRegistrationGrpcClient {
    async fn register(
        &self,
        request: RegisterWasiExecutorProcessGeneration,
    ) -> Result<WasiExecutorProcessIdentityEvidence, WasiExecutorProcessRegistrationError> {
        request.validate()?;
        let envelope = encode(&request, self.limits)
            .map_err(|_| WasiExecutorProcessRegistrationError::Rejected)?;
        let mut client = self.client.clone();
        let evidence: WasiExecutorProcessIdentityEvidence = client
            .register_micro_vm_executor_process_generation(Request::new(envelope))
            .await
            .map_err(|status| match status.code() {
                tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
                    WasiExecutorProcessRegistrationError::Unavailable
                }
                _ => WasiExecutorProcessRegistrationError::Rejected,
            })
            .and_then(|response| {
                decode(response.into_inner(), self.limits)
                    .map_err(|_| WasiExecutorProcessRegistrationError::Rejected)
            })?;
        evidence.validate_for(&request, &self.attestor_identity_digest, chrono::Utc::now())?;
        Ok(evidence)
    }
}

#[async_trait]
impl WasiExecutorProcessRegistrationVerifier for SandboxProcessIsolationAttestorGrpcClient {
    async fn verify_registered(
        &self,
        request: VerifyWasiExecutorProcessGeneration,
    ) -> Result<WasiExecutorProcessIdentityEvidence, WasiExecutorProcessRegistrationError> {
        request.validate()?;
        let envelope = encode(&request, self.limits)
            .map_err(|_| WasiExecutorProcessRegistrationError::Rejected)?;
        let mut client = self.client.clone();
        let evidence: WasiExecutorProcessIdentityEvidence = client
            .verify_wasi_executor_process_generation(Request::new(envelope))
            .await
            .map_err(|status| match status.code() {
                tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
                    WasiExecutorProcessRegistrationError::Unavailable
                }
                _ => WasiExecutorProcessRegistrationError::Rejected,
            })
            .and_then(|response| {
                decode(response.into_inner(), self.limits)
                    .map_err(|_| WasiExecutorProcessRegistrationError::Rejected)
            })?;
        let registration = RegisterWasiExecutorProcessGeneration {
            worker_process_generation_id: request.worker_process_generation_id,
            worker_manifest_digest: request.worker_manifest_digest,
            isolation_backend_contract_digest: request.isolation_backend_contract_digest,
        };
        evidence.validate_for(
            &registration,
            &self.attestor_identity_digest,
            chrono::Utc::now(),
        )?;
        if evidence.executor_identity_digest != request.executor_identity_digest {
            return Err(WasiExecutorProcessRegistrationError::Rejected);
        }
        if evidence.attestor_route != request.attestor_route {
            return Err(WasiExecutorProcessRegistrationError::Rejected);
        }
        Ok(evidence)
    }
}

#[async_trait]
impl SandboxProcessGenerationIsolation for SandboxProcessIsolationAttestorGrpcClient {
    async fn prove_absent(
        &self,
        request: ProveSandboxProcessGenerationAbsent,
    ) -> Result<SandboxProcessGenerationAbsenceEvidence, SandboxProcessGenerationIsolationError>
    {
        request.validate()?;
        let envelope = encode(&request, self.limits)
            .map_err(|_| SandboxProcessGenerationIsolationError::Rejected)?;
        let mut client = self.client.clone();
        let evidence: SandboxProcessGenerationAbsenceEvidence = client
            .prove_sandbox_process_generation_absent(Request::new(envelope))
            .await
            .map_err(|status| match status.code() {
                tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
                    SandboxProcessGenerationIsolationError::Unavailable
                }
                tonic::Code::Aborted => SandboxProcessGenerationIsolationError::StillLive,
                _ => SandboxProcessGenerationIsolationError::Rejected,
            })
            .and_then(|response| {
                decode(response.into_inner(), self.limits)
                    .map_err(|_| SandboxProcessGenerationIsolationError::Rejected)
            })?;
        if evidence.attestor_identity_digest != self.attestor_identity_digest {
            return Err(SandboxProcessGenerationIsolationError::Rejected);
        }
        evidence.validate_for(&request, chrono::Utc::now())?;
        Ok(evidence)
    }
}

impl SandboxBrokerGrpcClient {
    pub fn new(channel: tonic::transport::Channel, limits: SandboxInternalRpcLimits) -> Self {
        let maximum = limits.maximum_message_bytes();
        Self {
            client: SandboxExecutorBrokerServiceClient::new(channel)
                .max_encoding_message_size(maximum)
                .max_decoding_message_size(maximum),
            limits,
        }
    }

    async fn closed_unary<Req, Res>(
        &self,
        request: &Req,
        invoke: impl for<'a> FnOnce(
            &'a mut SandboxExecutorBrokerServiceClient<tonic::transport::Channel>,
            Request<ClosedSandboxEnvelope>,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = Result<Response<ClosedSandboxEnvelope>, Status>>
                    + Send
                    + 'a,
            >,
        >,
    ) -> Result<Res, SandboxRpcError>
    where
        Req: Serialize,
        Res: DeserializeOwned,
    {
        let envelope = encode(request, self.limits)?;
        let mut client = self.client.clone();
        let response = invoke(&mut client, Request::new(envelope))
            .await
            .map_err(classify_status)?;
        decode(response.into_inner(), self.limits)
    }
}

impl SandboxMicroVmBrokerGrpcClient {
    pub fn new(channel: tonic::transport::Channel, limits: SandboxInternalRpcLimits) -> Self {
        let maximum = limits.maximum_message_bytes();
        Self {
            client: SandboxMicroVmBrokerServiceClient::new(channel)
                .max_encoding_message_size(maximum)
                .max_decoding_message_size(maximum),
            limits,
        }
    }
}

#[async_trait]
impl MicroVmGrantRevoker for SandboxMicroVmBrokerGrpcClient {
    async fn revoke_exact(
        &self,
        request: RevokeMicroVmSandboxGrants,
    ) -> Result<MicroVmGrantRevocationEvidence, MicroVmGrantRevocationError> {
        request.validate()?;
        let envelope =
            encode(&request, self.limits).map_err(|_| MicroVmGrantRevocationError::Rejected)?;
        let mut client = self.client.clone();
        client
            .revoke_micro_vm_grants(Request::new(envelope))
            .await
            .map_err(|status| match status.code() {
                tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
                    MicroVmGrantRevocationError::Unavailable
                }
                _ => MicroVmGrantRevocationError::Rejected,
            })
            .and_then(|response| {
                decode(response.into_inner(), self.limits)
                    .map_err(|_| MicroVmGrantRevocationError::Rejected)
            })
    }
}

#[async_trait]
impl MicroVmArtifactBroker for SandboxMicroVmBrokerGrpcClient {
    async fn read_exact(
        &self,
        request: MicroVmArtifactReadRequest,
    ) -> Result<Vec<u8>, MicroVmArtifactBrokerError> {
        request.validate()?;
        let maximum_bytes = request.maximum_bytes;
        let artifact = request.artifact.clone();
        let deadline = request.deadline;
        let envelope =
            encode(&request, self.limits).map_err(|_| MicroVmArtifactBrokerError::Integrity)?;
        let (rpc_request, deadline_at) = artifact_request_with_deadline(envelope, deadline)
            .map_err(map_micro_vm_artifact_client_read_error)?;
        let mut client = self.client.clone();
        let mut stream = tokio::time::timeout_at(
            deadline_at,
            client.read_exact_micro_vm_artifact(rpc_request),
        )
        .await
        .map_err(|_| MicroVmArtifactBrokerError::Unavailable)?
        .map_err(micro_vm_artifact_client_error)?
        .into_inner();
        collect_sandbox_artifact_stream(
            &mut stream,
            &artifact,
            maximum_bytes,
            self.limits,
            deadline_at,
        )
        .await
        .map_err(map_micro_vm_artifact_client_read_error)
    }
}

#[async_trait]
impl WasiArtifactBroker for SandboxBrokerGrpcClient {
    async fn read_exact(
        &self,
        request: WasiArtifactReadRequest,
    ) -> Result<Vec<u8>, WasiArtifactBrokerError> {
        let maximum_bytes = request.maximum_bytes;
        let artifact = request.artifact.clone();
        let deadline = request.deadline;
        let envelope =
            encode(&request, self.limits).map_err(|_| WasiArtifactBrokerError::Integrity)?;
        let (rpc_request, deadline_at) = artifact_request_with_deadline(envelope, deadline)
            .map_err(map_wasi_artifact_client_read_error)?;
        let mut client = self.client.clone();
        let mut stream =
            tokio::time::timeout_at(deadline_at, client.read_exact_artifact(rpc_request))
                .await
                .map_err(|_| WasiArtifactBrokerError::Unavailable)?
                .map_err(wasi_artifact_client_error)?
                .into_inner();
        collect_sandbox_artifact_stream(
            &mut stream,
            &artifact,
            maximum_bytes,
            self.limits,
            deadline_at,
        )
        .await
        .map_err(map_wasi_artifact_client_read_error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SandboxArtifactClientReadError {
    Unavailable,
    TooLarge,
    Integrity,
}

struct CollectedSandboxArtifact(Vec<u8>);

impl CollectedSandboxArtifact {
    fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }

    fn into_bytes(mut self) -> Vec<u8> {
        std::mem::take(&mut self.0)
    }
}

impl Drop for CollectedSandboxArtifact {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

fn artifact_deadline_budget(
    deadline: chrono::DateTime<chrono::Utc>,
) -> Result<std::time::Duration, SandboxArtifactClientReadError> {
    deadline
        .signed_duration_since(chrono::Utc::now())
        .to_std()
        .ok()
        .filter(|budget| !budget.is_zero())
        .ok_or(SandboxArtifactClientReadError::Unavailable)
}

fn artifact_request_with_deadline<T>(
    message: T,
    deadline: chrono::DateTime<chrono::Utc>,
) -> Result<(Request<T>, tokio::time::Instant), SandboxArtifactClientReadError> {
    let budget = artifact_deadline_budget(deadline)?;
    let mut request = Request::new(message);
    request.set_timeout(budget);
    Ok((request, tokio::time::Instant::now() + budget))
}

async fn collect_sandbox_artifact_stream(
    stream: &mut tonic::Streaming<SandboxArtifactChunkEnvelope>,
    artifact: &ArtifactRef,
    maximum_bytes: usize,
    limits: SandboxInternalRpcLimits,
    deadline_at: tokio::time::Instant,
) -> Result<Vec<u8>, SandboxArtifactClientReadError> {
    let expected_bytes = usize::try_from(artifact.byte_length())
        .map_err(|_| SandboxArtifactClientReadError::TooLarge)?;
    if expected_bytes > maximum_bytes {
        return Err(SandboxArtifactClientReadError::TooLarge);
    }
    let chunk_bytes = sandbox_artifact_chunk_bytes(limits)?;
    let mut value = CollectedSandboxArtifact::with_capacity(expected_bytes);
    let mut expected_sequence = 0_u64;
    loop {
        let chunk = tokio::time::timeout_at(deadline_at, stream.message())
            .await
            .map_err(|_| SandboxArtifactClientReadError::Unavailable)?
            .map_err(classify_sandbox_artifact_status)?;
        let Some(chunk) = chunk else {
            break;
        };
        let content_digest: Sha256Digest = chunk
            .content_digest
            .parse()
            .map_err(|_| SandboxArtifactClientReadError::Integrity)?;
        let chunk_digest: Sha256Digest = chunk
            .chunk_digest
            .parse()
            .map_err(|_| SandboxArtifactClientReadError::Integrity)?;
        let next_length = value
            .0
            .len()
            .checked_add(chunk.value.len())
            .ok_or(SandboxArtifactClientReadError::TooLarge)?;
        if chunk.schema_version != SANDBOX_INTERNAL_RPC_SCHEMA_VERSION
            || chunk.sequence != expected_sequence
            || chunk.value.is_empty()
            || chunk.value.len() > chunk_bytes
            || chunk.total_bytes != artifact.byte_length()
            || content_digest != *artifact.content_digest()
            || bytes_digest(&chunk.value).map_err(|_| SandboxArtifactClientReadError::Integrity)?
                != chunk_digest
            || next_length > expected_bytes
            || next_length > maximum_bytes
            || (next_length < expected_bytes && chunk.value.len() != chunk_bytes)
        {
            return Err(SandboxArtifactClientReadError::Integrity);
        }
        value.0.extend_from_slice(&chunk.value);
        expected_sequence = expected_sequence
            .checked_add(1)
            .ok_or(SandboxArtifactClientReadError::Integrity)?;
    }
    if value.0.len() != expected_bytes
        || bytes_digest(&value.0).map_err(|_| SandboxArtifactClientReadError::Integrity)?
            != *artifact.content_digest()
    {
        return Err(SandboxArtifactClientReadError::Integrity);
    }
    Ok(value.into_bytes())
}

fn classify_sandbox_artifact_status(status: Status) -> SandboxArtifactClientReadError {
    match status.code() {
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
            SandboxArtifactClientReadError::Unavailable
        }
        tonic::Code::ResourceExhausted => SandboxArtifactClientReadError::TooLarge,
        _ => SandboxArtifactClientReadError::Integrity,
    }
}

fn map_wasi_artifact_client_read_error(
    error: SandboxArtifactClientReadError,
) -> WasiArtifactBrokerError {
    match error {
        SandboxArtifactClientReadError::Unavailable => WasiArtifactBrokerError::Unavailable,
        SandboxArtifactClientReadError::TooLarge => WasiArtifactBrokerError::TooLarge,
        SandboxArtifactClientReadError::Integrity => WasiArtifactBrokerError::Integrity,
    }
}

fn map_micro_vm_artifact_client_read_error(
    error: SandboxArtifactClientReadError,
) -> MicroVmArtifactBrokerError {
    match error {
        SandboxArtifactClientReadError::Unavailable => MicroVmArtifactBrokerError::Unavailable,
        SandboxArtifactClientReadError::TooLarge => MicroVmArtifactBrokerError::TooLarge,
        SandboxArtifactClientReadError::Integrity => MicroVmArtifactBrokerError::Integrity,
    }
}

#[async_trait]
impl WasiValueValidator for SandboxBrokerGrpcClient {
    async fn validate(
        &self,
        request: WasiValueValidationRequest,
    ) -> Result<Sha256Digest, WasiValueValidationError> {
        self.closed_unary(&request, |client, request| {
            Box::pin(client.validate_wasi_value(request))
        })
        .await
        .map_err(|error| match error {
            SandboxRpcError::Unavailable => WasiValueValidationError::Unavailable,
            _ => WasiValueValidationError::Invalid,
        })
    }
}

#[async_trait]
impl WasiGrantRevoker for SandboxBrokerGrpcClient {
    async fn revoke_exact(
        &self,
        request: RevokeWasiSandboxGrants,
    ) -> Result<WasiGrantRevocationEvidence, WasiGrantRevocationError> {
        self.closed_unary(&request, |client, request| {
            Box::pin(client.revoke_wasi_grants(request))
        })
        .await
        .map_err(|error| match error {
            SandboxRpcError::Unavailable => WasiGrantRevocationError::Unavailable,
            _ => WasiGrantRevocationError::Rejected,
        })
    }
}

#[async_trait]
impl SandboxProcessGenerationIsolation for SandboxBrokerGrpcClient {
    async fn prove_absent(
        &self,
        request: ProveSandboxProcessGenerationAbsent,
    ) -> Result<SandboxProcessGenerationAbsenceEvidence, SandboxProcessGenerationIsolationError>
    {
        self.closed_unary(&request, |client, request| {
            Box::pin(client.prove_sandbox_process_generation_absent(request))
        })
        .await
        .map_err(|error| match error {
            SandboxRpcError::Unavailable => SandboxProcessGenerationIsolationError::Unavailable,
            SandboxRpcError::FirstWinnerLost => SandboxProcessGenerationIsolationError::StillLive,
            _ => SandboxProcessGenerationIsolationError::Rejected,
        })
    }
}

#[async_trait]
impl SandboxClaimAuthority for SandboxAuthorityGrpcClient {
    async fn claim_sandbox_jobs(
        &self,
        command: ClaimSandboxJobs,
    ) -> Result<Vec<ClaimedSandboxJob>, SandboxClaimFailure> {
        command
            .validate(
                self.limits.maximum_claim_batch,
                self.limits.maximum_lease_milliseconds,
            )
            .map_err(|_| SandboxClaimFailure::InvariantViolation)?;
        self.unary(&command, |client, request| {
            Box::pin(client.claim_sandbox_jobs(request))
        })
        .await
        .map_err(|error| match error {
            SandboxRpcError::Unavailable => SandboxClaimFailure::Unavailable,
            SandboxRpcError::FirstWinnerLost => SandboxClaimFailure::FirstWinnerLost,
            _ => SandboxClaimFailure::InvariantViolation,
        })
    }
}

#[async_trait]
impl SandboxExecutionAuthority for SandboxAuthorityGrpcClient {
    type Error = SandboxRpcError;

    async fn commit_sandbox_phase(
        &self,
        command: CommitSandboxPhase,
    ) -> Result<CommandOutcome<SandboxPhaseDecision>, Self::Error> {
        self.unary(&command, |client, request| {
            Box::pin(client.commit_sandbox_phase(request))
        })
        .await
    }

    async fn commit_sandbox_outcome(
        &self,
        command: CommitSandboxOutcome,
    ) -> Result<CommandOutcome<SandboxPhaseDecision>, Self::Error> {
        self.unary(&command, |client, request| {
            Box::pin(client.commit_sandbox_outcome(request))
        })
        .await
    }

    async fn heartbeat_sandbox_execution(
        &self,
        command: HeartbeatSandboxExecution,
    ) -> Result<SandboxPhaseDecision, Self::Error> {
        command
            .validate(self.limits.maximum_lease_milliseconds)
            .map_err(|_| SandboxRpcError::InvalidEnvelope)?;
        self.unary(&command, |client, request| {
            Box::pin(client.heartbeat_sandbox_execution(request))
        })
        .await
    }
}

#[async_trait]
impl ManagedMcpSandboxSessionClaimAuthority for SandboxManagedMcpSessionAuthorityGrpcClient {
    async fn claim_managed_mcp_sandbox_sessions(
        &self,
        command: ClaimSandboxJobs,
    ) -> Result<Vec<ClaimedManagedMcpSandboxSession>, SandboxClaimFailure> {
        command
            .validate(
                self.limits.maximum_claim_batch,
                self.limits.maximum_lease_milliseconds,
            )
            .map_err(|_| SandboxClaimFailure::InvariantViolation)?;
        self.unary(&command, |client, request| {
            Box::pin(client.claim_managed_mcp_sandbox_sessions(request))
        })
        .await
        .map_err(|error| match error {
            SandboxRpcError::Unavailable => SandboxClaimFailure::Unavailable,
            SandboxRpcError::FirstWinnerLost => SandboxClaimFailure::FirstWinnerLost,
            _ => SandboxClaimFailure::InvariantViolation,
        })
    }
}

#[async_trait]
impl ManagedMcpSandboxSessionExecutionAuthority for SandboxManagedMcpSessionAuthorityGrpcClient {
    type Error = SandboxRpcError;

    async fn commit_managed_mcp_sandbox_session_phase(
        &self,
        command: CommitManagedMcpSandboxSessionPhase,
    ) -> Result<CommandOutcome<ManagedMcpSandboxSessionPhaseDecision>, Self::Error> {
        self.unary(&command, |client, request| {
            Box::pin(client.commit_managed_mcp_sandbox_session_phase(request))
        })
        .await
    }

    async fn commit_managed_mcp_sandbox_session_ready(
        &self,
        command: CommitManagedMcpSandboxSessionReady,
    ) -> Result<CommandOutcome<ManagedMcpSandboxSessionPhaseDecision>, Self::Error> {
        self.unary(&command, |client, request| {
            Box::pin(client.commit_managed_mcp_sandbox_session_ready(request))
        })
        .await
    }

    async fn heartbeat_managed_mcp_sandbox_session(
        &self,
        command: HeartbeatSandboxExecution,
    ) -> Result<ManagedMcpSandboxSessionPhaseDecision, Self::Error> {
        command
            .validate(self.limits.maximum_lease_milliseconds)
            .map_err(|_| SandboxRpcError::InvalidEnvelope)?;
        self.unary(&command, |client, request| {
            Box::pin(client.heartbeat_managed_mcp_sandbox_session(request))
        })
        .await
    }

    async fn commit_managed_mcp_sandbox_session_lost(
        &self,
        command: CommitManagedMcpSandboxSessionLost,
    ) -> Result<CommandOutcome<ManagedMcpSandboxSessionPhaseDecision>, Self::Error> {
        self.unary(&command, |client, request| {
            Box::pin(client.commit_managed_mcp_sandbox_session_lost(request))
        })
        .await
    }
}

#[async_trait]
impl ManagedMcpSandboxSessionRecoveryAuthority for SandboxManagedMcpSessionAuthorityGrpcClient {
    async fn scan_expired_managed_mcp_sandbox_session_leases(
        &self,
        command: ScanExpiredManagedMcpSandboxSessionLeases,
    ) -> Result<ExpiredManagedMcpSandboxSessionLeasePage, ManagedMcpSandboxSessionRecoveryFailure>
    {
        command
            .validate(
                self.limits.maximum_recovery_batch,
                self.limits.maximum_recovery_shards,
            )
            .map_err(|_| ManagedMcpSandboxSessionRecoveryFailure::InvariantViolation)?;
        let page: ExpiredManagedMcpSandboxSessionLeasePage = self
            .unary(&command, |client, request| {
                Box::pin(client.scan_expired_managed_mcp_sandbox_session_leases(request))
            })
            .await
            .map_err(map_managed_recovery_client_error)?;
        page.validate_for(&command, self.limits.sandbox_command_limits)
            .map_err(|_| ManagedMcpSandboxSessionRecoveryFailure::InvariantViolation)?;
        Ok(page)
    }

    async fn recover_expired_managed_mcp_sandbox_session_lease(
        &self,
        command: RecoverExpiredManagedMcpSandboxSessionLease,
    ) -> Result<
        CommandOutcome<ManagedMcpSandboxSessionLeaseRecoveryResult>,
        ManagedMcpSandboxSessionRecoveryFailure,
    > {
        command
            .executor
            .validate()
            .map_err(|_| ManagedMcpSandboxSessionRecoveryFailure::InvariantViolation)?;
        self.unary(&command, |client, request| {
            Box::pin(client.recover_expired_managed_mcp_sandbox_session_lease(request))
        })
        .await
        .map_err(map_managed_recovery_client_error)
    }
}

#[async_trait]
impl SandboxProcessGenerationIsolation for SandboxManagedMcpSessionAuthorityGrpcClient {
    async fn prove_absent(
        &self,
        request: ProveSandboxProcessGenerationAbsent,
    ) -> Result<SandboxProcessGenerationAbsenceEvidence, SandboxProcessGenerationIsolationError>
    {
        request
            .validate()
            .map_err(|_| SandboxProcessGenerationIsolationError::Rejected)?;
        self.unary(&request, |client, request| {
            Box::pin(client.prove_managed_mcp_sandbox_session_process_absent(request))
        })
        .await
        .map_err(|error| match error {
            SandboxRpcError::Unavailable => SandboxProcessGenerationIsolationError::Unavailable,
            SandboxRpcError::FirstWinnerLost => SandboxProcessGenerationIsolationError::StillLive,
            _ => SandboxProcessGenerationIsolationError::Rejected,
        })
    }
}

#[async_trait]
impl ManagedMcpSandboxSecretDeliveryAuthority for SandboxSecretDeliveryAuthorityGrpcClient {
    async fn reserve_managed_mcp_sandbox_secret_delivery(
        &self,
        request: &ManagedMcpSandboxSecretDeliveryRequest,
    ) -> Result<ManagedMcpSandboxSecretReservationOutcome, ManagedMcpSandboxSecretDeliveryError>
    {
        request
            .validate_shape()
            .map_err(|_| ManagedMcpSandboxSecretDeliveryError::Denied)?;
        let envelope = encode(request, self.limits)
            .map_err(|_| ManagedMcpSandboxSecretDeliveryError::Denied)?;
        let mut client = self.client.clone();
        let response = client
            .reserve_managed_mcp_sandbox_secret_delivery(Request::new(envelope))
            .await
            .map_err(secret_delivery_client_error)?;
        decode(response.into_inner(), self.limits)
            .map_err(|_| ManagedMcpSandboxSecretDeliveryError::OutcomeUncertain)
    }

    async fn commit_managed_mcp_sandbox_secret_delivery(
        &self,
        request: &ManagedMcpSandboxSecretDeliveryRequest,
        authorization: &AuthorizedManagedMcpSandboxSecretDelivery,
        resolution_evidence_digest: &Sha256Digest,
    ) -> Result<ManagedMcpSandboxSecretCommitOutcome, ManagedMcpSandboxSecretDeliveryError> {
        request
            .validate_shape()
            .map_err(|_| ManagedMcpSandboxSecretDeliveryError::Denied)?;
        authorization
            .validate_for(request)
            .map_err(|_| ManagedMcpSandboxSecretDeliveryError::Denied)?;
        let wire = CommitManagedMcpSandboxSecretDeliveryWire {
            request: request.clone(),
            authorization: authorization.clone(),
            resolution_evidence_digest: resolution_evidence_digest.clone(),
        };
        let envelope =
            encode(&wire, self.limits).map_err(|_| ManagedMcpSandboxSecretDeliveryError::Denied)?;
        let mut client = self.client.clone();
        let response = client
            .commit_managed_mcp_sandbox_secret_delivery(Request::new(envelope))
            .await
            .map_err(secret_delivery_client_error)?;
        decode(response.into_inner(), self.limits)
            .map_err(|_| ManagedMcpSandboxSecretDeliveryError::OutcomeUncertain)
    }
}

fn request_external_effect_possible(request: &SandboxExecutionRequest) -> bool {
    request.effect.risk_rank() >= insight_platform_contracts::Effect::IdempotentWrite.risk_rank()
        || request.network_mode != insight_platform_sandbox::SandboxNetworkMode::None
}

#[async_trait]
impl ManagedMcpSandboxSessionProvider for SandboxManagedMcpSessionProviderGrpcClient {
    type Error = SandboxRpcError;

    async fn prepare(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        executor_identity_digest: &Sha256Digest,
    ) -> Result<PreparedManagedMcpSandboxSession, Self::Error> {
        let wire = PrepareManagedMcpSandboxSessionWire {
            request: request.clone(),
            fence: fence.clone(),
            executor_identity_digest: executor_identity_digest.clone(),
        };
        match self
            .unary(&wire, |client, request| {
                Box::pin(client.prepare_managed_mcp_sandbox_session(request))
            })
            .await
        {
            Ok(prepared) => Ok(prepared),
            Err(error) => match self.destroy_remote(request, fence, None).await {
                Ok(outcome)
                    if outcome
                        .validate_for(request, fence, None, chrono::Utc::now())
                        .is_ok() =>
                {
                    Err(error)
                }
                Ok(_) => Err(SandboxRpcError::InvalidEnvelope),
                Err(cleanup) => Err(cleanup),
            },
        }
    }

    async fn initialize(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: &PreparedManagedMcpSandboxSession,
    ) -> Result<PreparedManagedMcpSandboxSessionActivation, Self::Error> {
        let wire = InitializeManagedMcpSandboxSessionWire {
            request: request.clone(),
            fence: fence.clone(),
            prepared: prepared.clone(),
        };
        self.unary(&wire, |client, request| {
            Box::pin(client.initialize_managed_mcp_sandbox_session(request))
        })
        .await
    }

    async fn activate(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        activation: &PreparedManagedMcpSandboxSessionActivation,
    ) -> Result<ActivatedManagedMcpSandboxSession, Self::Error> {
        let wire = ActivateManagedMcpSandboxSessionWire {
            request: request.clone(),
            fence: fence.clone(),
            activation: activation.clone(),
        };
        self.unary(&wire, |client, request| {
            Box::pin(client.activate_managed_mcp_sandbox_session(request))
        })
        .await
    }

    async fn observe_exact(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: &PreparedManagedMcpSandboxSession,
        activated: &ActivatedManagedMcpSandboxSession,
    ) -> Result<ManagedMcpSandboxSessionLivenessEvidence, Self::Error> {
        let wire = ObserveExactManagedMcpSandboxSessionWire {
            request: request.clone(),
            fence: fence.clone(),
            prepared: prepared.clone(),
            activated: activated.clone(),
        };
        let evidence: ManagedMcpSandboxSessionLivenessEvidence = self
            .unary(&wire, |client, request| {
                Box::pin(client.observe_exact_managed_mcp_sandbox_session(request))
            })
            .await?;
        evidence
            .validate_for(request, fence, prepared, activated, chrono::Utc::now())
            .map_err(|_| SandboxRpcError::InvalidEnvelope)?;
        Ok(evidence)
    }

    async fn destroy_exact(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
        prepared: Option<&PreparedManagedMcpSandboxSession>,
    ) -> Result<ManagedMcpSandboxSessionCleanupOutcome, Self::Error> {
        let outcome = self.destroy_remote(request, fence, prepared).await?;
        outcome
            .validate_for(request, fence, prepared, chrono::Utc::now())
            .map_err(|_| SandboxRpcError::InvalidEnvelope)?;
        Ok(outcome)
    }

    async fn recover_expired_exact(
        &self,
        expired: &ExpiredManagedMcpSandboxSessionLease,
    ) -> Result<ManagedMcpSandboxSessionCleanupOutcome, Self::Error> {
        let outcome: ManagedMcpSandboxSessionCleanupOutcome = self
            .unary(expired, |client, request| {
                Box::pin(client.recover_expired_managed_mcp_sandbox_session(request))
            })
            .await?;
        outcome
            .validate_for_expired(expired, chrono::Utc::now())
            .map_err(|_| SandboxRpcError::InvalidEnvelope)?;
        Ok(outcome)
    }
}

#[async_trait]
impl SandboxExecutorBackend for SandboxIsolationProviderGrpcClient {
    fn descriptor(&self) -> InstalledSandboxBackendDescriptor {
        self.descriptor.clone()
    }

    async fn prepare(
        &self,
        request: SandboxExecutionRequest,
    ) -> Result<PreparedSandbox, SandboxBackendFailure> {
        let input = (self.fence.clone(), request.clone());
        self.unary(
            &input,
            SandboxBackendFailureStage::Preparing,
            false,
            false,
            &request.request_digest,
            None,
            |client, request| Box::pin(client.prepare_sandbox(request)),
        )
        .await
    }

    async fn start(
        &self,
        request: SandboxExecutionRequest,
        prepared: PreparedSandbox,
    ) -> Result<RunningSandbox, SandboxBackendFailure> {
        let identity = prepared.sandbox_identity_digest.clone();
        let input = (self.fence.clone(), request.clone(), prepared);
        self.unary(
            &input,
            SandboxBackendFailureStage::Starting,
            true,
            request_external_effect_possible(&request),
            &request.request_digest,
            Some(&identity),
            |client, request| Box::pin(client.start_sandbox(request)),
        )
        .await
    }

    async fn collect(
        &self,
        request: SandboxExecutionRequest,
        running: RunningSandbox,
    ) -> Result<CollectedSandbox, SandboxBackendFailure> {
        let identity = running.prepared.sandbox_identity_digest.clone();
        let input = (self.fence.clone(), request.clone(), running);
        self.unary(
            &input,
            SandboxBackendFailureStage::Collecting,
            true,
            request_external_effect_possible(&request),
            &request.request_digest,
            Some(&identity),
            |client, request| Box::pin(client.collect_sandbox(request)),
        )
        .await
    }

    async fn terminate(
        &self,
        command: TerminateSandbox,
    ) -> Result<SandboxTerminationEvidence, SandboxBackendFailure> {
        let external = command.effect.risk_rank()
            >= insight_platform_contracts::Effect::IdempotentWrite.risk_rank()
            || command.network_mode != insight_platform_sandbox::SandboxNetworkMode::None;
        self.unary(
            &(self.fence.clone(), command.clone()),
            SandboxBackendFailureStage::Terminating,
            true,
            external,
            &command.request_digest,
            Some(&command.sandbox_identity_digest),
            |client, request| Box::pin(client.terminate_sandbox(request)),
        )
        .await
    }

    async fn destroy(
        &self,
        command: DestroySandbox,
    ) -> Result<SandboxCleanupEvidence, SandboxBackendFailure> {
        let external = command.effect.risk_rank()
            >= insight_platform_contracts::Effect::IdempotentWrite.risk_rank()
            || command.network_mode != insight_platform_sandbox::SandboxNetworkMode::None;
        self.unary(
            &(self.fence.clone(), command.clone()),
            SandboxBackendFailureStage::Destroying,
            true,
            external,
            &command.request_digest,
            Some(&command.sandbox_identity_digest),
            |client, request| Box::pin(client.destroy_sandbox(request)),
        )
        .await
    }

    async fn abort(
        &self,
        command: AbortSandboxExecution,
    ) -> Result<insight_platform_sandbox::SandboxAbortEvidence, SandboxBackendFailure> {
        let external = command.effect.risk_rank()
            >= insight_platform_contracts::Effect::IdempotentWrite.risk_rank()
            || command.network_mode != insight_platform_sandbox::SandboxNetworkMode::None;
        self.unary(
            &(self.fence.clone(), command.clone()),
            SandboxBackendFailureStage::Aborting,
            true,
            external,
            &command.request_digest,
            None,
            |client, request| Box::pin(client.abort_sandbox(request)),
        )
        .await
    }

    async fn recover_expired_lease(
        &self,
        expired: ExpiredSandboxLease,
    ) -> Result<SandboxLeaseRecoveryEvidence, SandboxBackendFailure> {
        let may_have_started =
            expired.physical_state != insight_platform_contracts::SandboxJobState::Preparing;
        let external = may_have_started && request_external_effect_possible(&expired.request);
        self.unary(
            &(self.fence.clone(), expired.clone()),
            SandboxBackendFailureStage::Recovering,
            may_have_started,
            external,
            &expired.request.request_digest,
            None,
            |client, request| Box::pin(client.recover_expired_sandbox_lease(request)),
        )
        .await
    }
}

pub struct SandboxAuthorityGrpcService<A, V> {
    authority: Arc<A>,
    process_registration: Arc<V>,
    limits: SandboxInternalRpcLimits,
}

pub struct SandboxManagedMcpSessionAuthorityGrpcService<A, V> {
    authority: Arc<A>,
    process_registration: Arc<V>,
    limits: SandboxInternalRpcLimits,
}

pub struct SandboxSecretDeliveryAuthorityGrpcService<A> {
    authority: Arc<A>,
    limits: SandboxInternalRpcLimits,
}

impl<A> SandboxSecretDeliveryAuthorityGrpcService<A> {
    pub fn new(authority: Arc<A>, limits: SandboxInternalRpcLimits) -> Self {
        Self { authority, limits }
    }
}

impl<A, V> SandboxManagedMcpSessionAuthorityGrpcService<A, V> {
    pub fn new(
        authority: Arc<A>,
        process_registration: Arc<V>,
        limits: SandboxInternalRpcLimits,
    ) -> Self {
        Self {
            authority,
            process_registration,
            limits,
        }
    }
}

impl<A, V> SandboxAuthorityGrpcService<A, V> {
    pub fn new(
        authority: Arc<A>,
        process_registration: Arc<V>,
        limits: SandboxInternalRpcLimits,
    ) -> Self {
        Self {
            authority,
            process_registration,
            limits,
        }
    }
}

pub struct SandboxBrokerGrpcService<B, V, G, P> {
    artifacts: Arc<B>,
    value_validator: Arc<V>,
    grant_revoker: Arc<G>,
    process_isolation: Arc<P>,
    limits: SandboxInternalRpcLimits,
    artifact_response_capacity: SandboxArtifactResponseCapacity,
}

pub struct SandboxMicroVmBrokerGrpcService<B, G> {
    artifacts: Arc<B>,
    grant_revoker: Arc<G>,
    limits: SandboxInternalRpcLimits,
    artifact_response_capacity: SandboxArtifactResponseCapacity,
}

/// One process-local bulkhead shared by every Controller Artifact response lane.
///
/// A permit is acquired before the Controller calls the upstream Sandbox Artifact Broker and is
/// moved into the outward response stream. This bounds both the upstream aggregation and the
/// downstream response body for WASI and microVM callers together.
#[derive(Clone)]
pub struct SandboxArtifactResponseCapacity {
    permits: Arc<Semaphore>,
}

impl SandboxArtifactResponseCapacity {
    pub fn new(maximum_in_flight: usize) -> Result<Self, SandboxRpcError> {
        if !(1..=MAX_IN_FLIGHT_ARTIFACT_RESPONSES_HARD).contains(&maximum_in_flight) {
            return Err(SandboxRpcError::InvalidConfiguration);
        }
        Ok(Self {
            permits: Arc::new(Semaphore::new(maximum_in_flight)),
        })
    }

    fn try_acquire(&self) -> Result<OwnedSemaphorePermit, Status> {
        Arc::clone(&self.permits).try_acquire_owned().map_err(|_| {
            Status::resource_exhausted("Sandbox Artifact response capacity is saturated")
        })
    }

    #[cfg(test)]
    fn available_permits(&self) -> usize {
        self.permits.available_permits()
    }
}

impl<B, G> SandboxMicroVmBrokerGrpcService<B, G> {
    pub fn new(
        artifacts: Arc<B>,
        grant_revoker: Arc<G>,
        limits: SandboxInternalRpcLimits,
        artifact_response_capacity: SandboxArtifactResponseCapacity,
    ) -> Self {
        Self {
            artifacts,
            grant_revoker,
            limits,
            artifact_response_capacity,
        }
    }
}

#[tonic::async_trait]
impl<B, G> SandboxMicroVmBrokerService for SandboxMicroVmBrokerGrpcService<B, G>
where
    B: MicroVmArtifactBroker + 'static,
    G: MicroVmGrantRevoker + 'static,
{
    type ReadExactMicroVmArtifactStream = Pin<
        Box<
            dyn futures::Stream<Item = Result<SandboxArtifactChunkEnvelope, Status>>
                + Send
                + 'static,
        >,
    >;

    async fn read_exact_micro_vm_artifact(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<Self::ReadExactMicroVmArtifactStream>, Status> {
        let request: MicroVmArtifactReadRequest = decode(request.into_inner(), self.limits)?;
        request
            .validate()
            .map_err(|_| Status::invalid_argument("invalid microVM Artifact read fence"))?;
        let permit = self.artifact_response_capacity.try_acquire()?;
        let artifact = request.artifact.clone();
        let maximum_bytes = request.maximum_bytes;
        let deadline = request.deadline;
        let read_budget = sandbox_artifact_server_deadline_budget(deadline)?;
        let value = tokio::time::timeout(read_budget, self.artifacts.read_exact(request))
            .await
            .map_err(|_| Status::deadline_exceeded("microVM Artifact read deadline elapsed"))?
            .map_err(micro_vm_artifact_status)?;
        if value.len() > maximum_bytes
            || u64::try_from(value.len()).ok() != Some(artifact.byte_length())
            || bytes_digest(&value)? != *artifact.content_digest()
        {
            return Err(Status::failed_precondition(
                "microVM Artifact integrity failed",
            ));
        }
        let chunk_bytes = sandbox_artifact_chunk_bytes(self.limits)
            .map_err(|_| Status::resource_exhausted("Sandbox RPC message bound is too small"))?;
        let total_bytes = artifact.byte_length();
        let content_digest = artifact.content_digest().to_string();
        let stream_budget = sandbox_artifact_server_deadline_budget(deadline)?;
        Ok(Response::new(Box::pin(SandboxArtifactChunkStream::new(
            value,
            permit,
            chunk_bytes,
            total_bytes,
            content_digest,
            stream_budget,
        ))))
    }

    async fn revoke_micro_vm_grants(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let request: RevokeMicroVmSandboxGrants = decode(request.into_inner(), self.limits)?;
        request
            .validate()
            .map_err(|_| Status::invalid_argument("invalid microVM grant revocation fence"))?;
        let evidence =
            self.grant_revoker
                .revoke_exact(request)
                .await
                .map_err(|error| match error {
                    MicroVmGrantRevocationError::Unavailable => {
                        Status::unavailable("microVM grant authority unavailable")
                    }
                    MicroVmGrantRevocationError::Rejected => {
                        Status::failed_precondition("microVM grant revocation rejected")
                    }
                })?;
        Ok(Response::new(encode(&evidence, self.limits)?))
    }
}

fn sandbox_artifact_server_deadline_budget(
    deadline: chrono::DateTime<chrono::Utc>,
) -> Result<std::time::Duration, Status> {
    deadline
        .signed_duration_since(chrono::Utc::now())
        .to_std()
        .ok()
        .filter(|budget| !budget.is_zero())
        .ok_or_else(|| Status::deadline_exceeded("Sandbox Artifact read deadline elapsed"))
}

fn sandbox_artifact_chunk_bytes(
    limits: SandboxInternalRpcLimits,
) -> Result<usize, SandboxArtifactClientReadError> {
    let chunk_bytes = SANDBOX_ARTIFACT_CHUNK_BYTES.min(
        limits
            .maximum_message_bytes()
            .saturating_sub(SANDBOX_ARTIFACT_MESSAGE_OVERHEAD_BYTES),
    );
    if chunk_bytes == 0 {
        return Err(SandboxArtifactClientReadError::TooLarge);
    }
    Ok(chunk_bytes)
}

struct SandboxArtifactResponseState {
    value: Vec<u8>,
    permit: Option<OwnedSemaphorePermit>,
    deadline_expired: bool,
}

impl SandboxArtifactResponseState {
    fn release(&mut self) {
        let mut value = std::mem::take(&mut self.value);
        value.fill(0);
        drop(value);
        self.permit.take();
    }

    fn expire(&mut self) {
        self.deadline_expired = true;
        self.release();
    }
}

struct SandboxArtifactChunkStream {
    response: Arc<Mutex<SandboxArtifactResponseState>>,
    offset: usize,
    sequence: u64,
    chunk_bytes: usize,
    total_bytes: u64,
    content_digest: String,
    deadline_at: tokio::time::Instant,
    deadline_waker: Arc<futures::task::AtomicWaker>,
    deadline_task: Option<tokio::task::JoinHandle<()>>,
    completed: bool,
}

impl SandboxArtifactChunkStream {
    fn new(
        value: Vec<u8>,
        permit: OwnedSemaphorePermit,
        chunk_bytes: usize,
        total_bytes: u64,
        content_digest: String,
        deadline_budget: std::time::Duration,
    ) -> Self {
        let response = Arc::new(Mutex::new(SandboxArtifactResponseState {
            value,
            permit: Some(permit),
            deadline_expired: false,
        }));
        let deadline_at = tokio::time::Instant::now() + deadline_budget;
        let deadline_waker = Arc::new(futures::task::AtomicWaker::new());
        let deadline_response = Arc::clone(&response);
        let task_waker = Arc::clone(&deadline_waker);
        let deadline_task = tokio::spawn(async move {
            tokio::time::sleep_until(deadline_at).await;
            let mut response = deadline_response
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            response.expire();
            drop(response);
            task_waker.wake();
        });
        Self {
            response,
            offset: 0,
            sequence: 0,
            chunk_bytes,
            total_bytes,
            content_digest,
            deadline_at,
            deadline_waker,
            deadline_task: Some(deadline_task),
            completed: false,
        }
    }

    fn release(&mut self) {
        let mut response = self
            .response
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        response.release();
        drop(response);
        if let Some(task) = self.deadline_task.take() {
            task.abort();
        }
    }
}

impl futures::Stream for SandboxArtifactChunkStream {
    type Item = Result<SandboxArtifactChunkEnvelope, Status>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.completed {
            return Poll::Ready(None);
        }
        self.deadline_waker.register(context.waker());
        let (value, reached_end, deadline_expired) = {
            let response = self
                .response
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if response.deadline_expired || tokio::time::Instant::now() >= self.deadline_at {
                (Vec::new(), false, true)
            } else if self.offset >= response.value.len() {
                (Vec::new(), true, false)
            } else {
                let end = self
                    .offset
                    .saturating_add(self.chunk_bytes)
                    .min(response.value.len());
                (response.value[self.offset..end].to_vec(), false, false)
            }
        };
        if deadline_expired {
            self.release();
            self.completed = true;
            return Poll::Ready(Some(Err(Status::deadline_exceeded(
                "Sandbox Artifact read deadline elapsed",
            ))));
        }
        if reached_end {
            self.release();
            self.completed = true;
            return Poll::Ready(None);
        }

        let chunk_digest = match bytes_digest(&value) {
            Ok(digest) => digest,
            Err(error) => {
                self.release();
                self.completed = true;
                return Poll::Ready(Some(Err(Status::from(error))));
            }
        };

        let deadline_expired = {
            let response = self
                .response
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            response.deadline_expired || tokio::time::Instant::now() >= self.deadline_at
        };
        if deadline_expired {
            let mut value = value;
            value.fill(0);
            self.release();
            self.completed = true;
            return Poll::Ready(Some(Err(Status::deadline_exceeded(
                "Sandbox Artifact read deadline elapsed",
            ))));
        }

        let sequence = self.sequence;
        let Some(next_sequence) = sequence.checked_add(1) else {
            self.release();
            self.completed = true;
            return Poll::Ready(Some(Err(Status::resource_exhausted(
                "too many Artifact chunks",
            ))));
        };
        self.offset = self.offset.saturating_add(value.len());
        self.sequence = next_sequence;
        let chunk = SandboxArtifactChunkEnvelope {
            schema_version: SANDBOX_INTERNAL_RPC_SCHEMA_VERSION,
            sequence,
            value,
            chunk_digest: chunk_digest.to_string(),
            total_bytes: self.total_bytes,
            content_digest: self.content_digest.clone(),
        };
        Poll::Ready(Some(Ok(chunk)))
    }
}

impl Drop for SandboxArtifactChunkStream {
    fn drop(&mut self) {
        self.release();
    }
}

pub struct SandboxProcessIsolationAttestorGrpcService<P> {
    process_isolation: Arc<P>,
    limits: SandboxInternalRpcLimits,
}

pub struct SandboxExecutorProcessRegistrationGrpcService<P> {
    process_registration: Arc<P>,
    limits: SandboxInternalRpcLimits,
}

pub struct SandboxIsolationProviderGrpcService<B> {
    backend: Arc<B>,
    descriptor: InstalledSandboxBackendDescriptor,
    limits: SandboxInternalRpcLimits,
    sandbox_limits: SandboxCommandLimits,
}

pub struct SandboxManagedMcpSessionProviderGrpcService<P> {
    provider: Arc<P>,
    descriptor: InstalledSandboxBackendDescriptor,
    limits: SandboxInternalRpcLimits,
    sandbox_limits: SandboxCommandLimits,
}

impl<P> SandboxManagedMcpSessionProviderGrpcService<P>
where
    P: ManagedMcpSandboxSessionProvider,
{
    pub fn new(
        provider: Arc<P>,
        descriptor: InstalledSandboxBackendDescriptor,
        limits: SandboxInternalRpcLimits,
        sandbox_limits: SandboxCommandLimits,
    ) -> Result<Self, SandboxRpcError> {
        if descriptor.backend_kind != SandboxIsolationBackendKind::MicroVm
            || descriptor.isolation_class
                != insight_platform_contracts::SandboxIsolationClass::MicroVm
        {
            return Err(SandboxRpcError::InvalidConfiguration);
        }
        Ok(Self {
            provider,
            descriptor,
            limits,
            sandbox_limits,
        })
    }

    fn validate_live_request(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
    ) -> Result<(), Status> {
        request
            .validate_at(chrono::Utc::now(), self.sandbox_limits)
            .map_err(|_| Status::invalid_argument("invalid Managed MCP Sandbox request"))?;
        self.validate_contract(request, fence)
    }

    fn validate_cleanup_request(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
    ) -> Result<(), Status> {
        let sealed = request
            .clone()
            .seal()
            .map_err(|_| Status::invalid_argument("invalid Managed MCP Sandbox request"))?;
        if &sealed != request {
            return Err(Status::invalid_argument(
                "invalid Managed MCP Sandbox request digest",
            ));
        }
        self.validate_contract(request, fence)
    }

    fn validate_contract(
        &self,
        request: &ManagedMcpSandboxSessionRequest,
        fence: &JobFence,
    ) -> Result<(), Status> {
        if request.identity.sandbox_job_id.kind() != insight_platform_contracts::ResourceKind::Job
            || fence.expected_version == 0
            || fence.worker_process_generation_id.kind()
                != insight_platform_contracts::ResourceKind::WorkerProcessGeneration
            || fence.lease_generation == 0
            || request.executor_worker_manifest_digest != self.descriptor.worker_manifest_digest
            || request.isolation_backend_contract_digest != self.descriptor.backend_contract_digest
            || request.isolation_class != insight_platform_contracts::SandboxIsolationClass::MicroVm
        {
            return Err(Status::failed_precondition(
                "Managed MCP microVM provider contract does not match the request",
            ));
        }
        Ok(())
    }

    fn response<T: Serialize>(
        &self,
        result: Result<T, P::Error>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let reply = match result {
            Ok(value) => ManagedMcpSandboxSessionProviderReply::Completed(value),
            Err(_) => ManagedMcpSandboxSessionProviderReply::Failed,
        };
        Ok(Response::new(encode(&reply, self.limits)?))
    }
}

impl<B> SandboxIsolationProviderGrpcService<B>
where
    B: MicroVmIsolationProviderBackend,
{
    pub fn new(
        backend: Arc<B>,
        limits: SandboxInternalRpcLimits,
        sandbox_limits: SandboxCommandLimits,
    ) -> Result<Self, SandboxRpcError> {
        let descriptor = backend.descriptor();
        if descriptor.backend_kind != SandboxIsolationBackendKind::MicroVm
            || descriptor.isolation_class
                != insight_platform_contracts::SandboxIsolationClass::MicroVm
        {
            return Err(SandboxRpcError::InvalidConfiguration);
        }
        Ok(Self {
            backend,
            descriptor,
            limits,
            sandbox_limits,
        })
    }

    fn validate_request(&self, request: &SandboxExecutionRequest) -> Result<(), Status> {
        request
            .validate_at(chrono::Utc::now(), self.sandbox_limits)
            .map_err(|_| Status::invalid_argument("invalid microVM Sandbox request"))?;
        if request.isolation_class != insight_platform_contracts::SandboxIsolationClass::MicroVm
            || request.executor_worker_manifest_digest != self.descriptor.worker_manifest_digest
            || request.isolation_backend_contract_digest != self.descriptor.backend_contract_digest
        {
            return Err(Status::failed_precondition(
                "microVM provider contract does not match the request",
            ));
        }
        Ok(())
    }

    fn response<T: Serialize>(
        &self,
        result: Result<T, SandboxBackendFailure>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let reply = match result {
            Ok(value) => SandboxIsolationProviderReply::Completed(value),
            Err(failure) => SandboxIsolationProviderReply::Failed(failure),
        };
        Ok(Response::new(encode(&reply, self.limits)?))
    }
}

impl<P> SandboxProcessIsolationAttestorGrpcService<P> {
    pub fn new(process_isolation: Arc<P>, limits: SandboxInternalRpcLimits) -> Self {
        Self {
            process_isolation,
            limits,
        }
    }
}

impl<P> SandboxExecutorProcessRegistrationGrpcService<P> {
    pub fn new(process_registration: Arc<P>, limits: SandboxInternalRpcLimits) -> Self {
        Self {
            process_registration,
            limits,
        }
    }
}

#[tonic::async_trait]
impl<B> SandboxIsolationProviderService for SandboxIsolationProviderGrpcService<B>
where
    B: MicroVmIsolationProviderBackend + 'static,
{
    async fn prepare_sandbox(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let (fence, request): (MicroVmProviderExecutionFence, SandboxExecutionRequest) =
            decode(request.into_inner(), self.limits)?;
        fence
            .validate()
            .map_err(|_| Status::invalid_argument("invalid microVM Executor fence"))?;
        self.validate_request(&request)?;
        self.response(self.backend.prepare(fence, request).await)
    }

    async fn start_sandbox(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let (fence, request, prepared): (
            MicroVmProviderExecutionFence,
            SandboxExecutionRequest,
            PreparedSandbox,
        ) = decode(request.into_inner(), self.limits)?;
        fence
            .validate()
            .map_err(|_| Status::invalid_argument("invalid microVM Executor fence"))?;
        self.validate_request(&request)?;
        if prepared.request_digest != request.request_digest
            || prepared.attempt_no != request.attempt_no
            || prepared.lease_generation != request.lease_generation
        {
            return Err(Status::failed_precondition(
                "prepared microVM identity does not match the request",
            ));
        }
        self.response(self.backend.start(fence, request, prepared).await)
    }

    async fn collect_sandbox(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let (fence, request, running): (
            MicroVmProviderExecutionFence,
            SandboxExecutionRequest,
            RunningSandbox,
        ) = decode(request.into_inner(), self.limits)?;
        fence
            .validate()
            .map_err(|_| Status::invalid_argument("invalid microVM Executor fence"))?;
        self.validate_request(&request)?;
        if running.prepared.request_digest != request.request_digest
            || running.prepared.attempt_no != request.attempt_no
            || running.prepared.lease_generation != request.lease_generation
        {
            return Err(Status::failed_precondition(
                "running microVM identity does not match the request",
            ));
        }
        self.response(self.backend.collect(fence, request, running).await)
    }

    async fn terminate_sandbox(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let (fence, command): (MicroVmProviderExecutionFence, TerminateSandbox) =
            decode(request.into_inner(), self.limits)?;
        fence
            .validate()
            .map_err(|_| Status::invalid_argument("invalid microVM Executor fence"))?;
        command
            .validate()
            .map_err(|_| Status::invalid_argument("invalid microVM terminate command"))?;
        self.response(self.backend.terminate(fence, command).await)
    }

    async fn destroy_sandbox(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let (fence, command): (MicroVmProviderExecutionFence, DestroySandbox) =
            decode(request.into_inner(), self.limits)?;
        fence
            .validate()
            .map_err(|_| Status::invalid_argument("invalid microVM Executor fence"))?;
        command
            .validate()
            .map_err(|_| Status::invalid_argument("invalid microVM destroy command"))?;
        self.response(self.backend.destroy(fence, command).await)
    }

    async fn abort_sandbox(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let (fence, command): (MicroVmProviderExecutionFence, AbortSandboxExecution) =
            decode(request.into_inner(), self.limits)?;
        fence
            .validate()
            .map_err(|_| Status::invalid_argument("invalid microVM Executor fence"))?;
        command
            .validate()
            .map_err(|_| Status::invalid_argument("invalid microVM abort command"))?;
        self.response(self.backend.abort(fence, command).await)
    }

    async fn recover_expired_sandbox_lease(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let (fence, expired): (MicroVmProviderExecutionFence, ExpiredSandboxLease) =
            decode(request.into_inner(), self.limits)?;
        fence
            .validate()
            .map_err(|_| Status::invalid_argument("invalid microVM Executor fence"))?;
        if fence.worker_process_generation_id != expired.previous_worker_process_generation_id {
            return Err(Status::failed_precondition(
                "expired microVM lease owner does not match the Executor fence",
            ));
        }
        expired
            .validate(self.sandbox_limits)
            .map_err(|_| Status::invalid_argument("invalid expired microVM lease"))?;
        if expired.request.executor_worker_manifest_digest != self.descriptor.worker_manifest_digest
            || expired.request.isolation_backend_contract_digest
                != self.descriptor.backend_contract_digest
            || expired.request.isolation_class
                != insight_platform_contracts::SandboxIsolationClass::MicroVm
        {
            return Err(Status::failed_precondition(
                "expired microVM lease targets a different provider contract",
            ));
        }
        self.response(self.backend.recover_expired_lease(fence, expired).await)
    }
}

#[tonic::async_trait]
impl<P> SandboxManagedMcpSessionProviderService for SandboxManagedMcpSessionProviderGrpcService<P>
where
    P: ManagedMcpSandboxSessionProvider + 'static,
{
    async fn prepare_managed_mcp_sandbox_session(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let wire: PrepareManagedMcpSandboxSessionWire = decode(request.into_inner(), self.limits)?;
        self.validate_live_request(&wire.request, &wire.fence)?;
        self.response(
            self.provider
                .prepare(&wire.request, &wire.fence, &wire.executor_identity_digest)
                .await,
        )
    }

    async fn initialize_managed_mcp_sandbox_session(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let wire: InitializeManagedMcpSandboxSessionWire =
            decode(request.into_inner(), self.limits)?;
        self.validate_live_request(&wire.request, &wire.fence)?;
        wire.prepared
            .validate_for(
                &wire.request,
                &wire.fence,
                &wire.prepared.executor_identity_digest,
            )
            .map_err(|_| {
                Status::failed_precondition(
                    "prepared Managed MCP microVM does not match the request",
                )
            })?;
        self.response(
            self.provider
                .initialize(&wire.request, &wire.fence, &wire.prepared)
                .await,
        )
    }

    async fn activate_managed_mcp_sandbox_session(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let wire: ActivateManagedMcpSandboxSessionWire = decode(request.into_inner(), self.limits)?;
        self.validate_live_request(&wire.request, &wire.fence)?;
        wire.activation
            .validate_for(
                &wire.request,
                &wire.fence,
                &wire.activation.prepared.executor_identity_digest,
                chrono::Utc::now(),
            )
            .map_err(|_| {
                Status::failed_precondition(
                    "Managed MCP microVM activation does not match the request",
                )
            })?;
        self.response(
            self.provider
                .activate(&wire.request, &wire.fence, &wire.activation)
                .await,
        )
    }

    async fn observe_exact_managed_mcp_sandbox_session(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let wire: ObserveExactManagedMcpSandboxSessionWire =
            decode(request.into_inner(), self.limits)?;
        self.validate_cleanup_request(&wire.request, &wire.fence)?;
        wire.prepared
            .validate_for(
                &wire.request,
                &wire.fence,
                &wire.prepared.executor_identity_digest,
            )
            .map_err(|_| {
                Status::failed_precondition(
                    "prepared Managed MCP microVM does not match the observation fence",
                )
            })?;
        if wire.activated.identity != wire.request.identity
            || wire.activated.request_digest != wire.request.request_digest
            || wire.activated.worker_process_generation_id
                != wire.fence.worker_process_generation_id
            || wire.activated.lease_generation != wire.fence.lease_generation
            || wire.activated.sandbox_identity_digest != wire.prepared.sandbox_identity_digest
        {
            return Err(Status::failed_precondition(
                "activated Managed MCP microVM does not match the observation fence",
            ));
        }
        self.response(
            self.provider
                .observe_exact(&wire.request, &wire.fence, &wire.prepared, &wire.activated)
                .await,
        )
    }

    async fn destroy_exact_managed_mcp_sandbox_session(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let wire: DestroyExactManagedMcpSandboxSessionWire =
            decode(request.into_inner(), self.limits)?;
        self.validate_cleanup_request(&wire.request, &wire.fence)?;
        if let Some(prepared) = &wire.prepared {
            prepared
                .validate_for(
                    &wire.request,
                    &wire.fence,
                    &prepared.executor_identity_digest,
                )
                .map_err(|_| {
                    Status::failed_precondition(
                        "prepared Managed MCP microVM does not match the cleanup fence",
                    )
                })?;
        }
        self.response(
            self.provider
                .destroy_exact(&wire.request, &wire.fence, wire.prepared.as_ref())
                .await,
        )
    }

    async fn recover_expired_managed_mcp_sandbox_session(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let expired: ExpiredManagedMcpSandboxSessionLease =
            decode(request.into_inner(), self.limits)?;
        expired
            .validate(self.sandbox_limits)
            .map_err(|_| Status::invalid_argument("invalid expired Managed MCP lease"))?;
        if expired.request.executor_worker_manifest_digest != self.descriptor.worker_manifest_digest
            || expired.request.isolation_backend_contract_digest
                != self.descriptor.backend_contract_digest
            || expired.request.isolation_class
                != insight_platform_contracts::SandboxIsolationClass::MicroVm
            || expired.physical_state == insight_platform_contracts::SandboxJobState::Accepted
        {
            return Err(Status::failed_precondition(
                "expired Managed MCP lease targets a different provider contract",
            ));
        }
        self.response(self.provider.recover_expired_exact(&expired).await)
    }
}

#[tonic::async_trait]
impl<P> SandboxExecutorProcessRegistrationService
    for SandboxExecutorProcessRegistrationGrpcService<P>
where
    P: WasiExecutorProcessAttestationAuthority + 'static,
{
    async fn register_wasi_executor_process_generation(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let peer = *request
            .extensions()
            .get::<WasiExecutorRegistrationPeer>()
            .ok_or_else(|| Status::permission_denied("node-local peer identity is required"))?;
        let request: RegisterWasiExecutorProcessGeneration =
            decode(request.into_inner(), self.limits)?;
        let evidence = self
            .process_registration
            .register_observed(request, peer)
            .await
            .map_err(registration_status)?;
        Ok(Response::new(encode(&evidence, self.limits)?))
    }
}

#[tonic::async_trait]
impl<P> SandboxMicroVmExecutorProcessRegistrationService
    for SandboxExecutorProcessRegistrationGrpcService<P>
where
    P: WasiExecutorProcessAttestationAuthority + 'static,
{
    async fn register_micro_vm_executor_process_generation(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let peer = *request
            .extensions()
            .get::<WasiExecutorRegistrationPeer>()
            .ok_or_else(|| Status::permission_denied("node-local peer identity is required"))?;
        let request: RegisterWasiExecutorProcessGeneration =
            decode(request.into_inner(), self.limits)?;
        let evidence = self
            .process_registration
            .register_observed(request, peer)
            .await
            .map_err(registration_status)?;
        Ok(Response::new(encode(&evidence, self.limits)?))
    }
}

#[tonic::async_trait]
impl<P> SandboxProcessIsolationAttestorService for SandboxProcessIsolationAttestorGrpcService<P>
where
    P: SandboxProcessGenerationIsolation + WasiExecutorProcessRegistrationVerifier + 'static,
{
    async fn verify_wasi_executor_process_generation(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let request: VerifyWasiExecutorProcessGeneration =
            decode(request.into_inner(), self.limits)?;
        let evidence = self
            .process_isolation
            .verify_registered(request)
            .await
            .map_err(registration_status)?;
        Ok(Response::new(encode(&evidence, self.limits)?))
    }

    async fn prove_sandbox_process_generation_absent(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let request: ProveSandboxProcessGenerationAbsent =
            decode(request.into_inner(), self.limits)?;
        request
            .validate()
            .map_err(|_| Status::invalid_argument("invalid process-generation proof request"))?;
        let evidence = self
            .process_isolation
            .prove_absent(request.clone())
            .await
            .map_err(|error| match error {
                SandboxProcessGenerationIsolationError::StillLive => {
                    Status::aborted("process generation is still live")
                }
                SandboxProcessGenerationIsolationError::Unavailable => {
                    Status::unavailable("process isolation attestor unavailable")
                }
                SandboxProcessGenerationIsolationError::Rejected => {
                    Status::failed_precondition("process absence proof rejected")
                }
            })?;
        evidence
            .validate_for(&request, chrono::Utc::now())
            .map_err(|_| Status::failed_precondition("process absence evidence is invalid"))?;
        Ok(Response::new(encode(&evidence, self.limits)?))
    }
}

fn registration_status(error: WasiExecutorProcessRegistrationError) -> Status {
    match error {
        WasiExecutorProcessRegistrationError::Unavailable => {
            Status::unavailable("process registration attestor unavailable")
        }
        WasiExecutorProcessRegistrationError::Rejected => {
            Status::failed_precondition("process registration rejected")
        }
    }
}

fn process_isolation_status(error: SandboxProcessGenerationIsolationError) -> Status {
    match error {
        SandboxProcessGenerationIsolationError::StillLive => {
            Status::aborted("process generation is still live")
        }
        SandboxProcessGenerationIsolationError::Unavailable => {
            Status::unavailable("process isolation attestor unavailable")
        }
        SandboxProcessGenerationIsolationError::Rejected => {
            Status::failed_precondition("process absence proof rejected")
        }
    }
}

impl<B, V, G, P> SandboxBrokerGrpcService<B, V, G, P> {
    pub fn new(
        artifacts: Arc<B>,
        value_validator: Arc<V>,
        grant_revoker: Arc<G>,
        process_isolation: Arc<P>,
        limits: SandboxInternalRpcLimits,
        artifact_response_capacity: SandboxArtifactResponseCapacity,
    ) -> Self {
        Self {
            artifacts,
            value_validator,
            grant_revoker,
            process_isolation,
            limits,
            artifact_response_capacity,
        }
    }
}

#[tonic::async_trait]
impl<B, V, G, P> SandboxExecutorBrokerService for SandboxBrokerGrpcService<B, V, G, P>
where
    B: WasiArtifactBroker + 'static,
    V: WasiValueValidator + 'static,
    G: WasiGrantRevoker + 'static,
    P: SandboxProcessGenerationIsolation + 'static,
{
    type ReadExactArtifactStream =
        Pin<Box<dyn futures::Stream<Item = Result<SandboxArtifactChunkEnvelope, Status>> + Send>>;

    async fn read_exact_artifact(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<Self::ReadExactArtifactStream>, Status> {
        let request: WasiArtifactReadRequest = decode(request.into_inner(), self.limits)?;
        let grant_shape_valid = match request.purpose {
            WasiArtifactReadPurpose::RuntimeBundle => request.read_grant.is_none(),
            WasiArtifactReadPurpose::InputValue => request.read_grant.is_some(),
        };
        if request.worker_process_generation_id.kind()
            != insight_platform_contracts::ResourceKind::WorkerProcessGeneration
            || request.tenant_id.kind() != insight_platform_contracts::ResourceKind::Tenant
            || request.sandbox_job_id.kind() != insight_platform_contracts::ResourceKind::Job
            || request.lease_generation == 0
            || request.maximum_bytes == 0
            || request.maximum_bytes > self.limits.maximum_message_bytes
            || request.artifact.validate().is_err()
            || u64::try_from(request.maximum_bytes)
                .ok()
                .is_none_or(|maximum| maximum < request.artifact.byte_length())
            || !grant_shape_valid
        {
            return Err(Status::invalid_argument("invalid Artifact read bound"));
        }
        let permit = self.artifact_response_capacity.try_acquire()?;
        let maximum = request.maximum_bytes;
        let artifact = request.artifact.clone();
        let deadline = request.deadline;
        let read_budget = sandbox_artifact_server_deadline_budget(deadline)?;
        let value = tokio::time::timeout(read_budget, self.artifacts.read_exact(request))
            .await
            .map_err(|_| Status::deadline_exceeded("WASI Artifact read deadline elapsed"))?
            .map_err(artifact_status)?;
        if value.len() > maximum
            || u64::try_from(value.len()).ok() != Some(artifact.byte_length())
            || bytes_digest(&value)? != *artifact.content_digest()
        {
            return Err(Status::failed_precondition(
                "WASI Artifact integrity failed",
            ));
        }
        let chunk_bytes = sandbox_artifact_chunk_bytes(self.limits)
            .map_err(|_| Status::resource_exhausted("Sandbox RPC message bound is too small"))?;
        let stream_budget = sandbox_artifact_server_deadline_budget(deadline)?;
        Ok(Response::new(Box::pin(SandboxArtifactChunkStream::new(
            value,
            permit,
            chunk_bytes,
            artifact.byte_length(),
            artifact.content_digest().to_string(),
            stream_budget,
        ))))
    }

    async fn validate_wasi_value(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let request: WasiValueValidationRequest = decode(request.into_inner(), self.limits)?;
        if request.worker_process_generation_id.kind()
            != insight_platform_contracts::ResourceKind::WorkerProcessGeneration
            || request.lease_generation == 0
        {
            return Err(Status::invalid_argument("invalid Sandbox value fence"));
        }
        let digest = self
            .value_validator
            .validate(request)
            .await
            .map_err(|error| match error {
                WasiValueValidationError::Invalid => {
                    Status::invalid_argument("Sandbox value is invalid")
                }
                WasiValueValidationError::Unavailable => {
                    Status::unavailable("value validator unavailable")
                }
            })?;
        Ok(Response::new(encode(&digest, self.limits)?))
    }

    async fn revoke_wasi_grants(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let request: RevokeWasiSandboxGrants = decode(request.into_inner(), self.limits)?;
        if request.worker_process_generation_id.kind()
            != insight_platform_contracts::ResourceKind::WorkerProcessGeneration
            || request.lease_generation == 0
        {
            return Err(Status::invalid_argument("invalid Sandbox revoke fence"));
        }
        let evidence =
            self.grant_revoker
                .revoke_exact(request)
                .await
                .map_err(|error| match error {
                    WasiGrantRevocationError::Unavailable => {
                        Status::unavailable("grant authority unavailable")
                    }
                    WasiGrantRevocationError::Rejected => {
                        Status::failed_precondition("grant revocation rejected")
                    }
                })?;
        Ok(Response::new(encode(&evidence, self.limits)?))
    }

    async fn prove_sandbox_process_generation_absent(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let request: ProveSandboxProcessGenerationAbsent =
            decode(request.into_inner(), self.limits)?;
        let evidence = self
            .process_isolation
            .prove_absent(request)
            .await
            .map_err(|error| match error {
                SandboxProcessGenerationIsolationError::StillLive => {
                    Status::aborted("process generation is still live")
                }
                SandboxProcessGenerationIsolationError::Unavailable => {
                    Status::unavailable("process isolation authority unavailable")
                }
                SandboxProcessGenerationIsolationError::Rejected => {
                    Status::failed_precondition("process absence proof rejected")
                }
            })?;
        Ok(Response::new(encode(&evidence, self.limits)?))
    }
}

#[tonic::async_trait]
impl<A, V> SandboxExecutorAuthorityService for SandboxAuthorityGrpcService<A, V>
where
    A: SandboxClaimAuthority + SandboxExecutionAuthority + 'static,
    A::Error: fmt::Display + Send + Sync,
    V: WasiExecutorProcessRegistrationVerifier + 'static,
{
    async fn claim_sandbox_jobs(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let command: ClaimSandboxJobs = decode(request.into_inner(), self.limits)?;
        command
            .validate(
                self.limits.maximum_claim_batch,
                self.limits.maximum_lease_milliseconds,
            )
            .map_err(|_| Status::invalid_argument("invalid Sandbox claim"))?;
        self.process_registration
            .verify_registered(VerifyWasiExecutorProcessGeneration {
                worker_process_generation_id: command.worker_process_generation_id.clone(),
                worker_manifest_digest: command.worker_manifest_digest.clone(),
                isolation_backend_contract_digest: command
                    .isolation_backend_contract_digest
                    .clone(),
                executor_identity_digest: command.executor_identity_digest.clone(),
                attestor_route: command.attestor_route.clone(),
            })
            .await
            .map_err(registration_status)?;
        let result = self
            .authority
            .claim_sandbox_jobs(command)
            .await
            .map_err(claim_status)?;
        Ok(Response::new(encode(&result, self.limits)?))
    }

    async fn commit_sandbox_phase(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let command: CommitSandboxPhase = decode(request.into_inner(), self.limits)?;
        let result = self
            .authority
            .commit_sandbox_phase(command)
            .await
            .map_err(authority_status)?;
        Ok(Response::new(encode(&result, self.limits)?))
    }

    async fn commit_sandbox_outcome(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let command: CommitSandboxOutcome = decode(request.into_inner(), self.limits)?;
        let result = self
            .authority
            .commit_sandbox_outcome(command)
            .await
            .map_err(authority_status)?;
        Ok(Response::new(encode(&result, self.limits)?))
    }

    async fn heartbeat_sandbox_execution(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let command: HeartbeatSandboxExecution = decode(request.into_inner(), self.limits)?;
        command
            .validate(self.limits.maximum_lease_milliseconds)
            .map_err(|_| Status::invalid_argument("invalid Sandbox heartbeat"))?;
        let result = self
            .authority
            .heartbeat_sandbox_execution(command)
            .await
            .map_err(authority_status)?;
        Ok(Response::new(encode(&result, self.limits)?))
    }
}

#[tonic::async_trait]
impl<A, V> SandboxManagedMcpSessionAuthorityService
    for SandboxManagedMcpSessionAuthorityGrpcService<A, V>
where
    A: ManagedMcpSandboxSessionClaimAuthority
        + ManagedMcpSandboxSessionExecutionAuthority
        + ManagedMcpSandboxSessionRecoveryAuthority
        + 'static,
    A::Error: fmt::Display + Send + Sync,
    V: WasiExecutorProcessRegistrationVerifier + SandboxProcessGenerationIsolation + 'static,
{
    async fn claim_managed_mcp_sandbox_sessions(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let command: ClaimSandboxJobs = decode(request.into_inner(), self.limits)?;
        command
            .validate(
                self.limits.maximum_claim_batch,
                self.limits.maximum_lease_milliseconds,
            )
            .map_err(|_| Status::invalid_argument("invalid Managed MCP Sandbox claim"))?;
        self.process_registration
            .verify_registered(VerifyWasiExecutorProcessGeneration {
                worker_process_generation_id: command.worker_process_generation_id.clone(),
                worker_manifest_digest: command.worker_manifest_digest.clone(),
                isolation_backend_contract_digest: command
                    .isolation_backend_contract_digest
                    .clone(),
                executor_identity_digest: command.executor_identity_digest.clone(),
                attestor_route: command.attestor_route.clone(),
            })
            .await
            .map_err(registration_status)?;
        let result = self
            .authority
            .claim_managed_mcp_sandbox_sessions(command)
            .await
            .map_err(claim_status)?;
        Ok(Response::new(encode(&result, self.limits)?))
    }

    async fn commit_managed_mcp_sandbox_session_phase(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let command: CommitManagedMcpSandboxSessionPhase =
            decode(request.into_inner(), self.limits)?;
        let result = self
            .authority
            .commit_managed_mcp_sandbox_session_phase(command)
            .await
            .map_err(authority_status)?;
        Ok(Response::new(encode(&result, self.limits)?))
    }

    async fn commit_managed_mcp_sandbox_session_ready(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let command: CommitManagedMcpSandboxSessionReady =
            decode(request.into_inner(), self.limits)?;
        let result = self
            .authority
            .commit_managed_mcp_sandbox_session_ready(command)
            .await
            .map_err(authority_status)?;
        Ok(Response::new(encode(&result, self.limits)?))
    }

    async fn heartbeat_managed_mcp_sandbox_session(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let command: HeartbeatSandboxExecution = decode(request.into_inner(), self.limits)?;
        command
            .validate(self.limits.maximum_lease_milliseconds)
            .map_err(|_| Status::invalid_argument("invalid Managed MCP Sandbox heartbeat"))?;
        let result = self
            .authority
            .heartbeat_managed_mcp_sandbox_session(command)
            .await
            .map_err(authority_status)?;
        Ok(Response::new(encode(&result, self.limits)?))
    }

    async fn commit_managed_mcp_sandbox_session_lost(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let command: CommitManagedMcpSandboxSessionLost =
            decode(request.into_inner(), self.limits)?;
        let result = self
            .authority
            .commit_managed_mcp_sandbox_session_lost(command)
            .await
            .map_err(authority_status)?;
        Ok(Response::new(encode(&result, self.limits)?))
    }

    async fn scan_expired_managed_mcp_sandbox_session_leases(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let command: ScanExpiredManagedMcpSandboxSessionLeases =
            decode(request.into_inner(), self.limits)?;
        command
            .validate(
                self.limits.maximum_recovery_batch,
                self.limits.maximum_recovery_shards,
            )
            .map_err(|_| Status::invalid_argument("invalid Managed MCP recovery scan"))?;
        self.process_registration
            .verify_registered(VerifyWasiExecutorProcessGeneration {
                worker_process_generation_id: command.executor.worker_process_generation_id.clone(),
                worker_manifest_digest: command.executor.worker_manifest_digest.clone(),
                isolation_backend_contract_digest: command
                    .executor
                    .isolation_backend_contract_digest
                    .clone(),
                executor_identity_digest: command.executor.executor_identity_digest.clone(),
                attestor_route: command.executor.attestor_route.clone(),
            })
            .await
            .map_err(registration_status)?;
        let result = self
            .authority
            .scan_expired_managed_mcp_sandbox_session_leases(command.clone())
            .await
            .map_err(managed_recovery_status)?;
        result
            .validate_for(&command, self.limits.sandbox_command_limits)
            .map_err(|_| Status::failed_precondition("Managed MCP recovery page is invalid"))?;
        Ok(Response::new(encode(&result, self.limits)?))
    }

    async fn prove_managed_mcp_sandbox_session_process_absent(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let request: ProveSandboxProcessGenerationAbsent =
            decode(request.into_inner(), self.limits)?;
        request
            .validate()
            .map_err(|_| Status::invalid_argument("invalid process absence request"))?;
        let evidence = self
            .process_registration
            .prove_absent(request.clone())
            .await
            .map_err(process_isolation_status)?;
        evidence
            .validate_for(&request, chrono::Utc::now())
            .map_err(|_| Status::failed_precondition("process absence evidence is invalid"))?;
        Ok(Response::new(encode(&evidence, self.limits)?))
    }

    async fn recover_expired_managed_mcp_sandbox_session_lease(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let command: RecoverExpiredManagedMcpSandboxSessionLease =
            decode(request.into_inner(), self.limits)?;
        command
            .executor
            .validate()
            .map_err(|_| Status::invalid_argument("invalid Managed MCP recovery Executor"))?;
        self.process_registration
            .verify_registered(VerifyWasiExecutorProcessGeneration {
                worker_process_generation_id: command.executor.worker_process_generation_id.clone(),
                worker_manifest_digest: command.executor.worker_manifest_digest.clone(),
                isolation_backend_contract_digest: command
                    .executor
                    .isolation_backend_contract_digest
                    .clone(),
                executor_identity_digest: command.executor.executor_identity_digest.clone(),
                attestor_route: command.executor.attestor_route.clone(),
            })
            .await
            .map_err(registration_status)?;
        let result = self
            .authority
            .recover_expired_managed_mcp_sandbox_session_lease(command)
            .await
            .map_err(managed_recovery_status)?;
        Ok(Response::new(encode(&result, self.limits)?))
    }
}

#[tonic::async_trait]
impl<A> SandboxSecretDeliveryAuthorityService for SandboxSecretDeliveryAuthorityGrpcService<A>
where
    A: ManagedMcpSandboxSecretDeliveryAuthority + 'static,
{
    async fn reserve_managed_mcp_sandbox_secret_delivery(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let request: ManagedMcpSandboxSecretDeliveryRequest =
            decode(request.into_inner(), self.limits)?;
        request
            .validate_shape()
            .map_err(|_| Status::invalid_argument("invalid Managed MCP Secret delivery"))?;
        let result = self
            .authority
            .reserve_managed_mcp_sandbox_secret_delivery(&request)
            .await
            .map_err(secret_delivery_status)?;
        Ok(Response::new(encode(&result, self.limits)?))
    }

    async fn commit_managed_mcp_sandbox_secret_delivery(
        &self,
        request: Request<ClosedSandboxEnvelope>,
    ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
        let wire: CommitManagedMcpSandboxSecretDeliveryWire =
            decode(request.into_inner(), self.limits)?;
        wire.request
            .validate_shape()
            .map_err(|_| Status::invalid_argument("invalid Managed MCP Secret delivery"))?;
        wire.authorization
            .validate_for(&wire.request)
            .map_err(|_| Status::invalid_argument("invalid Managed MCP Secret authorization"))?;
        let result = self
            .authority
            .commit_managed_mcp_sandbox_secret_delivery(
                &wire.request,
                &wire.authorization,
                &wire.resolution_evidence_digest,
            )
            .await
            .map_err(secret_delivery_status)?;
        Ok(Response::new(encode(&result, self.limits)?))
    }
}

fn encode<T: Serialize>(
    value: &T,
    limits: SandboxInternalRpcLimits,
) -> Result<ClosedSandboxEnvelope, SandboxRpcError> {
    let canonical_json = serde_jcs::to_vec(value).map_err(|_| SandboxRpcError::InvalidEnvelope)?;
    if canonical_json.is_empty() || canonical_json.len() > limits.maximum_message_bytes {
        return Err(SandboxRpcError::InvalidEnvelope);
    }
    let parsed: serde_json::Value =
        serde_json::from_slice(&canonical_json).map_err(|_| SandboxRpcError::InvalidEnvelope)?;
    let payload_digest: Sha256Digest = canonical_digest(&parsed)
        .map_err(|_| SandboxRpcError::InvalidEnvelope)?
        .parse()
        .map_err(|_| SandboxRpcError::InvalidEnvelope)?;
    Ok(ClosedSandboxEnvelope {
        schema_version: SANDBOX_INTERNAL_RPC_SCHEMA_VERSION,
        canonical_json,
        payload_digest: payload_digest.to_string(),
    })
}

fn decode<T: DeserializeOwned>(
    envelope: ClosedSandboxEnvelope,
    limits: SandboxInternalRpcLimits,
) -> Result<T, SandboxRpcError> {
    if envelope.schema_version != SANDBOX_INTERNAL_RPC_SCHEMA_VERSION
        || envelope.canonical_json.is_empty()
        || envelope.canonical_json.len() > limits.maximum_message_bytes
    {
        return Err(SandboxRpcError::InvalidEnvelope);
    }
    let expected: Sha256Digest = envelope
        .payload_digest
        .parse()
        .map_err(|_| SandboxRpcError::InvalidEnvelope)?;
    let parsed = parse_strict_json(
        &envelope.canonical_json,
        JsonLimits {
            max_bytes: limits.maximum_message_bytes,
            max_depth: 128,
            max_items_per_array: 100_000,
            max_properties_per_object: 100_000,
            max_string_bytes: limits.maximum_message_bytes,
        },
    )
    .map_err(|_| SandboxRpcError::InvalidEnvelope)?;
    let actual: Sha256Digest = canonical_digest(&parsed)
        .map_err(|_| SandboxRpcError::InvalidEnvelope)?
        .parse()
        .map_err(|_| SandboxRpcError::InvalidEnvelope)?;
    if actual != expected
        || serde_jcs::to_vec(&parsed).map_err(|_| SandboxRpcError::InvalidEnvelope)?
            != envelope.canonical_json
    {
        return Err(SandboxRpcError::InvalidEnvelope);
    }
    serde_json::from_value(parsed).map_err(|_| SandboxRpcError::InvalidEnvelope)
}

fn bytes_digest(value: &[u8]) -> Result<Sha256Digest, SandboxRpcError> {
    let digest = Sha256::digest(value);
    let mut encoded = String::with_capacity(71);
    encoded.push_str("sha256:");
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
        .parse()
        .map_err(|_| SandboxRpcError::InvalidEnvelope)
}

fn artifact_status(error: WasiArtifactBrokerError) -> Status {
    match error {
        WasiArtifactBrokerError::Unavailable => Status::unavailable("Artifact broker unavailable"),
        WasiArtifactBrokerError::Denied => Status::permission_denied("Artifact read denied"),
        WasiArtifactBrokerError::NotFound => Status::not_found("Artifact not found"),
        WasiArtifactBrokerError::Integrity => {
            Status::failed_precondition("Artifact integrity failed")
        }
        WasiArtifactBrokerError::TooLarge => Status::resource_exhausted("Artifact too large"),
    }
}

fn micro_vm_artifact_status(error: MicroVmArtifactBrokerError) -> Status {
    match error {
        MicroVmArtifactBrokerError::Unavailable => {
            Status::unavailable("microVM Artifact broker unavailable")
        }
        MicroVmArtifactBrokerError::Denied => {
            Status::permission_denied("microVM Artifact read denied")
        }
        MicroVmArtifactBrokerError::NotFound => Status::not_found("microVM Artifact not found"),
        MicroVmArtifactBrokerError::Integrity => {
            Status::failed_precondition("microVM Artifact integrity failed")
        }
        MicroVmArtifactBrokerError::TooLarge => {
            Status::resource_exhausted("microVM Artifact too large")
        }
    }
}

fn micro_vm_artifact_client_error(status: Status) -> MicroVmArtifactBrokerError {
    match status.code() {
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
            MicroVmArtifactBrokerError::Unavailable
        }
        tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => {
            MicroVmArtifactBrokerError::Denied
        }
        tonic::Code::NotFound => MicroVmArtifactBrokerError::NotFound,
        tonic::Code::ResourceExhausted => MicroVmArtifactBrokerError::TooLarge,
        _ => MicroVmArtifactBrokerError::Integrity,
    }
}

fn wasi_artifact_client_error(status: Status) -> WasiArtifactBrokerError {
    match status.code() {
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
            WasiArtifactBrokerError::Unavailable
        }
        tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => {
            WasiArtifactBrokerError::Denied
        }
        tonic::Code::NotFound => WasiArtifactBrokerError::NotFound,
        tonic::Code::ResourceExhausted => WasiArtifactBrokerError::TooLarge,
        _ => WasiArtifactBrokerError::Integrity,
    }
}

fn secret_delivery_status(error: ManagedMcpSandboxSecretDeliveryError) -> Status {
    match error {
        ManagedMcpSandboxSecretDeliveryError::Unavailable => {
            Status::unavailable("Sandbox Secret delivery authority unavailable")
        }
        ManagedMcpSandboxSecretDeliveryError::Denied => {
            Status::permission_denied("Sandbox Secret delivery denied")
        }
        ManagedMcpSandboxSecretDeliveryError::OutcomeUncertain => {
            Status::aborted("Sandbox Secret delivery outcome is uncertain")
        }
    }
}

fn secret_delivery_client_error(status: Status) -> ManagedMcpSandboxSecretDeliveryError {
    match status.code() {
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
            ManagedMcpSandboxSecretDeliveryError::Unavailable
        }
        tonic::Code::Aborted | tonic::Code::AlreadyExists => {
            ManagedMcpSandboxSecretDeliveryError::OutcomeUncertain
        }
        _ => ManagedMcpSandboxSecretDeliveryError::Denied,
    }
}

fn claim_status(error: SandboxClaimFailure) -> Status {
    match error {
        SandboxClaimFailure::Unavailable => Status::unavailable("Sandbox authority unavailable"),
        SandboxClaimFailure::FirstWinnerLost => Status::aborted("Sandbox claim lost"),
        SandboxClaimFailure::InvariantViolation => {
            Status::failed_precondition("Sandbox authority invariant failed")
        }
    }
}

fn managed_recovery_status(error: ManagedMcpSandboxSessionRecoveryFailure) -> Status {
    match error {
        ManagedMcpSandboxSessionRecoveryFailure::Unavailable => {
            Status::unavailable("Managed MCP recovery authority unavailable")
        }
        ManagedMcpSandboxSessionRecoveryFailure::FirstWinnerLost => {
            Status::aborted("Managed MCP recovery lost the first-winner race")
        }
        ManagedMcpSandboxSessionRecoveryFailure::InvariantViolation => {
            Status::failed_precondition("Managed MCP recovery invariant failed")
        }
    }
}

fn map_managed_recovery_client_error(
    error: SandboxRpcError,
) -> ManagedMcpSandboxSessionRecoveryFailure {
    match error {
        SandboxRpcError::Unavailable => ManagedMcpSandboxSessionRecoveryFailure::Unavailable,
        SandboxRpcError::FirstWinnerLost => {
            ManagedMcpSandboxSessionRecoveryFailure::FirstWinnerLost
        }
        SandboxRpcError::InvalidConfiguration
        | SandboxRpcError::InvalidEnvelope
        | SandboxRpcError::Rejected => ManagedMcpSandboxSessionRecoveryFailure::InvariantViolation,
    }
}

fn authority_status(error: impl fmt::Display) -> Status {
    let _ = error;
    Status::failed_precondition("Sandbox authority rejected command")
}

fn classify_status(status: Status) -> SandboxRpcError {
    match status.code() {
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => SandboxRpcError::Unavailable,
        tonic::Code::Aborted | tonic::Code::AlreadyExists => SandboxRpcError::FirstWinnerLost,
        _ => SandboxRpcError::Rejected,
    }
}

fn isolation_provider_failure(
    stage: SandboxBackendFailureStage,
    execution_may_have_started: bool,
    external_effect_possible: bool,
    request_digest: &Sha256Digest,
    sandbox_identity_digest: Option<&Sha256Digest>,
    transport_error: SandboxRpcError,
) -> SandboxBackendFailure {
    let transport_class = match transport_error {
        SandboxRpcError::InvalidConfiguration => "invalid_configuration",
        SandboxRpcError::InvalidEnvelope => "invalid_envelope",
        SandboxRpcError::Unavailable => "unavailable",
        SandboxRpcError::FirstWinnerLost => "first_winner_lost",
        SandboxRpcError::Rejected => "rejected",
    };
    let evidence_digest = canonical_digest(&serde_json::json!({
        "schema_version": 1,
        "stage": stage,
        "request_digest": request_digest,
        "sandbox_identity_digest": sandbox_identity_digest,
        "transport_class": transport_class,
    }))
    .expect("closed provider failure evidence must canonicalize")
    .parse()
    .expect("canonical provider failure digest must parse");
    SandboxBackendFailure {
        stage,
        safe_code: "sandbox_isolation_provider_unavailable".to_owned(),
        safe_message: "sandbox isolation provider unavailable".to_owned(),
        retryability: if external_effect_possible {
            Retryability::Never
        } else {
            Retryability::SafeWithinPolicy
        },
        evidence_digest,
        sandbox_identity_digest: sandbox_identity_digest.cloned(),
        execution_may_have_started,
        external_effect_possible,
    }
}

impl From<SandboxRpcError> for Status {
    fn from(error: SandboxRpcError) -> Self {
        match error {
            SandboxRpcError::Unavailable => Status::unavailable("Sandbox RPC unavailable"),
            SandboxRpcError::FirstWinnerLost => Status::aborted("Sandbox first winner lost"),
            SandboxRpcError::InvalidConfiguration
            | SandboxRpcError::InvalidEnvelope
            | SandboxRpcError::Rejected => Status::invalid_argument("invalid Sandbox RPC envelope"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxRpcError {
    InvalidConfiguration,
    InvalidEnvelope,
    Unavailable,
    FirstWinnerLost,
    Rejected,
}

impl fmt::Display for SandboxRpcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "Sandbox RPC configuration is invalid",
            Self::InvalidEnvelope => "Sandbox RPC envelope is invalid",
            Self::Unavailable => "Sandbox RPC is unavailable",
            Self::FirstWinnerLost => "Sandbox RPC first-winner race was lost",
            Self::Rejected => "Sandbox RPC command was rejected",
        })
    }
}

impl Error for SandboxRpcError {}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use futures::StreamExt;
    use hyper_util::rt::TokioIo;
    use insight_platform_contracts::{
        checked_in_hard_limit_profile, ArtifactRef, DataClassification, ResourceId, ResourceKind,
        MAX_SANDBOX_RUNTIME_BUNDLE_BYTES,
    };
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa,
        KeyPair, KeyUsagePurpose, SanType,
    };
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering},
        sync::Mutex,
    };
    use tokio::net::{UnixListener, UnixStream};
    use tokio::sync::oneshot;
    use tokio_stream::wrappers::UnixListenerStream;
    use tonic::transport::{
        server::TcpIncoming, Certificate, ClientTlsConfig, Endpoint, Identity, Server,
        ServerTlsConfig,
    };

    fn attestor_route() -> insight_platform_sandbox::NodeAttestorRoute {
        "https://10.0.0.7:9443".parse().unwrap()
    }

    fn certificate_with_sans(subject_alt_names: Vec<SanType>) -> Vec<u8> {
        let key = KeyPair::generate().unwrap();
        let mut parameters = CertificateParams::default();
        parameters.subject_alt_names = subject_alt_names;
        parameters.self_signed(&key).unwrap().der().to_vec()
    }

    struct MtlsFixture {
        ca_pem: String,
        server_certificate_pem: String,
        server_key_pem: String,
        provider_server_certificate_pem: String,
        provider_server_key_pem: String,
        wasi_certificate_pem: String,
        wasi_key_pem: String,
        microvm_certificate_pem: String,
        microvm_key_pem: String,
        microvm_provider_certificate_pem: String,
        microvm_provider_key_pem: String,
        controller_certificate_pem: String,
        controller_key_pem: String,
        wrong_certificate_pem: String,
        wrong_key_pem: String,
    }

    fn mtls_fixture() -> MtlsFixture {
        let mut ca_parameters = CertificateParams::default();
        ca_parameters.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_parameters.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let ca = CertifiedIssuer::self_signed(ca_parameters, KeyPair::generate().unwrap()).unwrap();

        let issue = |subject_alt_names, extended_key_usage| {
            let mut parameters = CertificateParams::default();
            parameters.subject_alt_names = subject_alt_names;
            parameters.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            parameters.extended_key_usages = vec![extended_key_usage];
            let key = KeyPair::generate().unwrap();
            let certificate = parameters.signed_by(&key, &ca).unwrap();
            (certificate.pem(), key.serialize_pem())
        };
        let (server_certificate_pem, server_key_pem) = issue(
            vec![SanType::DnsName("localhost".try_into().unwrap())],
            ExtendedKeyUsagePurpose::ServerAuth,
        );
        let (provider_server_certificate_pem, provider_server_key_pem) = issue(
            vec![
                SanType::DnsName("localhost".try_into().unwrap()),
                SanType::URI(MICROVM_PROVIDER_WORKLOAD_IDENTITY.try_into().unwrap()),
            ],
            ExtendedKeyUsagePurpose::ServerAuth,
        );
        let (wasi_certificate_pem, wasi_key_pem) = issue(
            vec![SanType::URI(
                WASI_EXECUTOR_WORKLOAD_IDENTITY.try_into().unwrap(),
            )],
            ExtendedKeyUsagePurpose::ClientAuth,
        );
        let (controller_certificate_pem, controller_key_pem) = issue(
            vec![SanType::URI(
                SANDBOX_CONTROLLER_WORKLOAD_IDENTITY.try_into().unwrap(),
            )],
            ExtendedKeyUsagePurpose::ClientAuth,
        );
        let (microvm_certificate_pem, microvm_key_pem) = issue(
            vec![SanType::URI(
                MICROVM_EXECUTOR_WORKLOAD_IDENTITY.try_into().unwrap(),
            )],
            ExtendedKeyUsagePurpose::ClientAuth,
        );
        let (microvm_provider_certificate_pem, microvm_provider_key_pem) = issue(
            vec![SanType::URI(
                MICROVM_PROVIDER_WORKLOAD_IDENTITY.try_into().unwrap(),
            )],
            ExtendedKeyUsagePurpose::ClientAuth,
        );
        let (wrong_certificate_pem, wrong_key_pem) = issue(
            vec![SanType::URI(
                "spiffe://insight.platform/workload/model-worker"
                    .try_into()
                    .unwrap(),
            )],
            ExtendedKeyUsagePurpose::ClientAuth,
        );
        MtlsFixture {
            ca_pem: ca.pem(),
            server_certificate_pem,
            server_key_pem,
            provider_server_certificate_pem,
            provider_server_key_pem,
            wasi_certificate_pem,
            wasi_key_pem,
            microvm_certificate_pem,
            microvm_key_pem,
            microvm_provider_certificate_pem,
            microvm_provider_key_pem,
            controller_certificate_pem,
            controller_key_pem,
            wrong_certificate_pem,
            wrong_key_pem,
        }
    }

    #[derive(Default)]
    struct RecordingAuthority {
        claims: AtomicUsize,
        managed_heartbeats: AtomicUsize,
        managed_recovery_scans: AtomicUsize,
    }

    #[async_trait]
    impl SandboxClaimAuthority for RecordingAuthority {
        async fn claim_sandbox_jobs(
            &self,
            _command: ClaimSandboxJobs,
        ) -> Result<Vec<ClaimedSandboxJob>, SandboxClaimFailure> {
            self.claims.fetch_add(1, Ordering::AcqRel);
            Ok(vec![])
        }
    }

    #[async_trait]
    impl SandboxExecutionAuthority for RecordingAuthority {
        type Error = SandboxRpcError;

        async fn commit_sandbox_phase(
            &self,
            _command: CommitSandboxPhase,
        ) -> Result<CommandOutcome<SandboxPhaseDecision>, Self::Error> {
            Err(SandboxRpcError::Rejected)
        }

        async fn commit_sandbox_outcome(
            &self,
            _command: CommitSandboxOutcome,
        ) -> Result<CommandOutcome<SandboxPhaseDecision>, Self::Error> {
            Err(SandboxRpcError::Rejected)
        }

        async fn heartbeat_sandbox_execution(
            &self,
            _command: HeartbeatSandboxExecution,
        ) -> Result<SandboxPhaseDecision, Self::Error> {
            Err(SandboxRpcError::Rejected)
        }
    }

    #[async_trait]
    impl ManagedMcpSandboxSessionClaimAuthority for RecordingAuthority {
        async fn claim_managed_mcp_sandbox_sessions(
            &self,
            _command: ClaimSandboxJobs,
        ) -> Result<Vec<ClaimedManagedMcpSandboxSession>, SandboxClaimFailure> {
            self.claims.fetch_add(1, Ordering::AcqRel);
            Ok(vec![])
        }
    }

    #[async_trait]
    impl ManagedMcpSandboxSessionExecutionAuthority for RecordingAuthority {
        type Error = SandboxRpcError;

        async fn commit_managed_mcp_sandbox_session_phase(
            &self,
            _command: CommitManagedMcpSandboxSessionPhase,
        ) -> Result<CommandOutcome<ManagedMcpSandboxSessionPhaseDecision>, Self::Error> {
            Err(SandboxRpcError::Rejected)
        }

        async fn commit_managed_mcp_sandbox_session_ready(
            &self,
            _command: CommitManagedMcpSandboxSessionReady,
        ) -> Result<CommandOutcome<ManagedMcpSandboxSessionPhaseDecision>, Self::Error> {
            Err(SandboxRpcError::Rejected)
        }

        async fn heartbeat_managed_mcp_sandbox_session(
            &self,
            _command: HeartbeatSandboxExecution,
        ) -> Result<ManagedMcpSandboxSessionPhaseDecision, Self::Error> {
            self.managed_heartbeats.fetch_add(1, Ordering::AcqRel);
            Err(SandboxRpcError::Rejected)
        }

        async fn commit_managed_mcp_sandbox_session_lost(
            &self,
            _command: CommitManagedMcpSandboxSessionLost,
        ) -> Result<CommandOutcome<ManagedMcpSandboxSessionPhaseDecision>, Self::Error> {
            Err(SandboxRpcError::Rejected)
        }
    }

    #[async_trait]
    impl ManagedMcpSandboxSessionRecoveryAuthority for RecordingAuthority {
        async fn scan_expired_managed_mcp_sandbox_session_leases(
            &self,
            _command: ScanExpiredManagedMcpSandboxSessionLeases,
        ) -> Result<ExpiredManagedMcpSandboxSessionLeasePage, ManagedMcpSandboxSessionRecoveryFailure>
        {
            self.managed_recovery_scans.fetch_add(1, Ordering::AcqRel);
            Ok(ExpiredManagedMcpSandboxSessionLeasePage {
                records: Vec::new(),
                next_cursor: None,
                exhausted: true,
            })
        }

        async fn recover_expired_managed_mcp_sandbox_session_lease(
            &self,
            _command: RecoverExpiredManagedMcpSandboxSessionLease,
        ) -> Result<
            CommandOutcome<ManagedMcpSandboxSessionLeaseRecoveryResult>,
            ManagedMcpSandboxSessionRecoveryFailure,
        > {
            Err(ManagedMcpSandboxSessionRecoveryFailure::FirstWinnerLost)
        }
    }

    struct RecordingMicroVmBackend {
        destroys: AtomicUsize,
        executor_generations: Mutex<Vec<ResourceId>>,
        descriptor: InstalledSandboxBackendDescriptor,
    }

    #[derive(Default)]
    struct RecordingManagedMcpSessionProvider {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl ManagedMcpSandboxSessionProvider for RecordingManagedMcpSessionProvider {
        type Error = SandboxRpcError;

        async fn prepare(
            &self,
            _request: &ManagedMcpSandboxSessionRequest,
            _fence: &JobFence,
            _executor_identity_digest: &Sha256Digest,
        ) -> Result<PreparedManagedMcpSandboxSession, Self::Error> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            Err(SandboxRpcError::Rejected)
        }

        async fn initialize(
            &self,
            _request: &ManagedMcpSandboxSessionRequest,
            _fence: &JobFence,
            _prepared: &PreparedManagedMcpSandboxSession,
        ) -> Result<PreparedManagedMcpSandboxSessionActivation, Self::Error> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            Err(SandboxRpcError::Rejected)
        }

        async fn activate(
            &self,
            _request: &ManagedMcpSandboxSessionRequest,
            _fence: &JobFence,
            _activation: &PreparedManagedMcpSandboxSessionActivation,
        ) -> Result<ActivatedManagedMcpSandboxSession, Self::Error> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            Err(SandboxRpcError::Rejected)
        }

        async fn observe_exact(
            &self,
            _request: &ManagedMcpSandboxSessionRequest,
            _fence: &JobFence,
            _prepared: &PreparedManagedMcpSandboxSession,
            _activated: &ActivatedManagedMcpSandboxSession,
        ) -> Result<ManagedMcpSandboxSessionLivenessEvidence, Self::Error> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            Err(SandboxRpcError::Rejected)
        }

        async fn destroy_exact(
            &self,
            _request: &ManagedMcpSandboxSessionRequest,
            _fence: &JobFence,
            _prepared: Option<&PreparedManagedMcpSandboxSession>,
        ) -> Result<ManagedMcpSandboxSessionCleanupOutcome, Self::Error> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            Err(SandboxRpcError::Rejected)
        }

        async fn recover_expired_exact(
            &self,
            _expired: &ExpiredManagedMcpSandboxSessionLease,
        ) -> Result<ManagedMcpSandboxSessionCleanupOutcome, Self::Error> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            Err(SandboxRpcError::Rejected)
        }
    }

    #[derive(Default)]
    struct RecordingMicroVmGrantRevoker {
        calls: AtomicUsize,
    }

    struct RecordingArtifactBroker {
        calls: AtomicUsize,
        value: Vec<u8>,
        requests: Mutex<Vec<MicroVmArtifactReadRequest>>,
    }

    #[async_trait]
    impl MicroVmArtifactBroker for RecordingArtifactBroker {
        async fn read_exact(
            &self,
            request: MicroVmArtifactReadRequest,
        ) -> Result<Vec<u8>, MicroVmArtifactBrokerError> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.requests.lock().unwrap().push(request);
            Ok(self.value.clone())
        }
    }

    #[async_trait]
    impl WasiArtifactBroker for RecordingArtifactBroker {
        async fn read_exact(
            &self,
            _request: WasiArtifactReadRequest,
        ) -> Result<Vec<u8>, WasiArtifactBrokerError> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            Ok(self.value.clone())
        }
    }

    #[derive(Default)]
    struct RecordingWasiBrokerDependencies;

    #[async_trait]
    impl WasiValueValidator for RecordingWasiBrokerDependencies {
        async fn validate(
            &self,
            _request: WasiValueValidationRequest,
        ) -> Result<Sha256Digest, WasiValueValidationError> {
            Err(WasiValueValidationError::Invalid)
        }
    }

    #[async_trait]
    impl WasiGrantRevoker for RecordingWasiBrokerDependencies {
        async fn revoke_exact(
            &self,
            _request: RevokeWasiSandboxGrants,
        ) -> Result<WasiGrantRevocationEvidence, WasiGrantRevocationError> {
            Err(WasiGrantRevocationError::Rejected)
        }
    }

    struct StallingWasiArtifactService {
        observed_timeout: Arc<AtomicBool>,
    }

    #[tonic::async_trait]
    impl SandboxExecutorBrokerService for StallingWasiArtifactService {
        type ReadExactArtifactStream = Pin<
            Box<
                dyn futures::Stream<Item = Result<SandboxArtifactChunkEnvelope, Status>>
                    + Send
                    + 'static,
            >,
        >;

        async fn read_exact_artifact(
            &self,
            request: Request<ClosedSandboxEnvelope>,
        ) -> Result<Response<Self::ReadExactArtifactStream>, Status> {
            if request.metadata().get("grpc-timeout").is_none() {
                return Err(Status::invalid_argument("missing gRPC timeout"));
            }
            self.observed_timeout.store(true, Ordering::Release);
            Ok(Response::new(Box::pin(futures::stream::pending())))
        }

        async fn validate_wasi_value(
            &self,
            _request: Request<ClosedSandboxEnvelope>,
        ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
            Err(Status::unimplemented("not used"))
        }

        async fn revoke_wasi_grants(
            &self,
            _request: Request<ClosedSandboxEnvelope>,
        ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
            Err(Status::unimplemented("not used"))
        }

        async fn prove_sandbox_process_generation_absent(
            &self,
            _request: Request<ClosedSandboxEnvelope>,
        ) -> Result<Response<ClosedSandboxEnvelope>, Status> {
            Err(Status::unimplemented("not used"))
        }
    }

    struct FailOnceArtifactBroker {
        calls: AtomicUsize,
        value: Vec<u8>,
    }

    #[async_trait]
    impl MicroVmArtifactBroker for FailOnceArtifactBroker {
        async fn read_exact(
            &self,
            _request: MicroVmArtifactReadRequest,
        ) -> Result<Vec<u8>, MicroVmArtifactBrokerError> {
            if self.calls.fetch_add(1, Ordering::AcqRel) == 0 {
                Err(MicroVmArtifactBrokerError::Unavailable)
            } else {
                Ok(self.value.clone())
            }
        }
    }

    fn micro_vm_artifact_request(
        value: &[u8],
        deadline: chrono::DateTime<Utc>,
    ) -> MicroVmArtifactReadRequest {
        let artifact = ArtifactRef::new(
            ResourceId::from_uuid_v7(ResourceKind::Artifact, uuid::Uuid::now_v7()).unwrap(),
            bytes_digest(value).unwrap(),
            u64::try_from(value.len()).unwrap(),
            "application/vnd.insight.sandbox-bundle",
            DataClassification::Internal,
            None,
        )
        .unwrap();
        MicroVmArtifactReadRequest {
            workload_kind: MicroVmSandboxWorkloadKind::CapabilityExecution,
            tenant_id: ResourceId::from_uuid_v7(ResourceKind::Tenant, uuid::Uuid::now_v7())
                .unwrap(),
            sandbox_job_id: ResourceId::from_uuid_v7(ResourceKind::Job, uuid::Uuid::now_v7())
                .unwrap(),
            request_digest: format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
            executor_worker_process_generation_id: ResourceId::from_uuid_v7(
                ResourceKind::WorkerProcessGeneration,
                uuid::Uuid::now_v7(),
            )
            .unwrap(),
            provider_process_generation_id: ResourceId::from_uuid_v7(
                ResourceKind::WorkerProcessGeneration,
                uuid::Uuid::now_v7(),
            )
            .unwrap(),
            sandbox_identity_digest: format!("sha256:{}", "b".repeat(64)).parse().unwrap(),
            lease_generation: 1,
            artifact,
            purpose: MicroVmArtifactReadPurpose::RuntimeBundle,
            read_grant: None,
            maximum_bytes: value.len(),
            deadline,
        }
    }

    fn wasi_artifact_request(
        value: &[u8],
        deadline: chrono::DateTime<Utc>,
    ) -> WasiArtifactReadRequest {
        let tenant_id =
            ResourceId::from_uuid_v7(ResourceKind::Tenant, uuid::Uuid::now_v7()).unwrap();
        let sandbox_job_id =
            ResourceId::from_uuid_v7(ResourceKind::Job, uuid::Uuid::now_v7()).unwrap();
        let artifact = ArtifactRef::new(
            ResourceId::from_uuid_v7(ResourceKind::Artifact, uuid::Uuid::now_v7()).unwrap(),
            bytes_digest(value).unwrap(),
            u64::try_from(value.len()).unwrap(),
            "application/octet-stream",
            DataClassification::Internal,
            None,
        )
        .unwrap();
        let maximum_bytes = value.len().max(1);
        let read_grant = ScopedArtifactGrant {
            schema_version: 1,
            grant_id: ResourceId::from_uuid_v7(ResourceKind::ArtifactGrant, uuid::Uuid::now_v7())
                .unwrap(),
            tenant_id: tenant_id.clone(),
            sandbox_job_id: sandbox_job_id.clone(),
            operation: ArtifactGrantOperation::ReadWhole,
            port: "input".to_owned(),
            artifact: Some(artifact.clone()),
            staging_artifact_id: None,
            byte_range: None,
            maximum_bytes: u64::try_from(maximum_bytes).unwrap(),
            generation: 1,
            expires_at: deadline,
            grant_digest: format!("sha256:{}", "0".repeat(64)).parse().unwrap(),
        }
        .seal()
        .unwrap();
        WasiArtifactReadRequest {
            tenant_id,
            sandbox_job_id,
            request_digest: format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
            worker_process_generation_id: ResourceId::from_uuid_v7(
                ResourceKind::WorkerProcessGeneration,
                uuid::Uuid::now_v7(),
            )
            .unwrap(),
            lease_generation: 1,
            artifact,
            purpose: WasiArtifactReadPurpose::InputValue,
            read_grant: Some(read_grant),
            maximum_bytes,
            deadline,
        }
    }

    #[async_trait]
    impl MicroVmGrantRevoker for RecordingMicroVmGrantRevoker {
        async fn revoke_exact(
            &self,
            request: RevokeMicroVmSandboxGrants,
        ) -> Result<MicroVmGrantRevocationEvidence, MicroVmGrantRevocationError> {
            request.validate()?;
            self.calls.fetch_add(1, Ordering::AcqRel);
            Ok(MicroVmGrantRevocationEvidence {
                evidence_digest: format!("sha256:{}", "e".repeat(64)).parse().unwrap(),
            })
        }
    }

    fn recording_backend_failure(stage: SandboxBackendFailureStage) -> SandboxBackendFailure {
        SandboxBackendFailure {
            stage,
            safe_code: "unexpected_provider_call".to_owned(),
            safe_message: "unexpected provider call".to_owned(),
            retryability: Retryability::Never,
            evidence_digest: format!("sha256:{}", "f".repeat(64)).parse().unwrap(),
            sandbox_identity_digest: None,
            execution_may_have_started: !matches!(
                stage,
                SandboxBackendFailureStage::Admission | SandboxBackendFailureStage::Preparing
            ),
            external_effect_possible: false,
        }
    }

    #[async_trait]
    impl MicroVmIsolationProviderBackend for RecordingMicroVmBackend {
        fn descriptor(&self) -> InstalledSandboxBackendDescriptor {
            self.descriptor.clone()
        }

        async fn prepare(
            &self,
            _fence: MicroVmProviderExecutionFence,
            _request: SandboxExecutionRequest,
        ) -> Result<PreparedSandbox, SandboxBackendFailure> {
            Err(recording_backend_failure(
                SandboxBackendFailureStage::Preparing,
            ))
        }

        async fn start(
            &self,
            _fence: MicroVmProviderExecutionFence,
            _request: SandboxExecutionRequest,
            _prepared: PreparedSandbox,
        ) -> Result<RunningSandbox, SandboxBackendFailure> {
            Err(recording_backend_failure(
                SandboxBackendFailureStage::Starting,
            ))
        }

        async fn collect(
            &self,
            _fence: MicroVmProviderExecutionFence,
            _request: SandboxExecutionRequest,
            _running: RunningSandbox,
        ) -> Result<CollectedSandbox, SandboxBackendFailure> {
            Err(recording_backend_failure(
                SandboxBackendFailureStage::Collecting,
            ))
        }

        async fn terminate(
            &self,
            _fence: MicroVmProviderExecutionFence,
            _command: TerminateSandbox,
        ) -> Result<SandboxTerminationEvidence, SandboxBackendFailure> {
            Err(recording_backend_failure(
                SandboxBackendFailureStage::Terminating,
            ))
        }

        async fn destroy(
            &self,
            fence: MicroVmProviderExecutionFence,
            command: DestroySandbox,
        ) -> Result<SandboxCleanupEvidence, SandboxBackendFailure> {
            self.destroys.fetch_add(1, Ordering::AcqRel);
            self.executor_generations
                .lock()
                .unwrap()
                .push(fence.worker_process_generation_id);
            Ok(SandboxCleanupEvidence {
                disposition: insight_platform_sandbox::SandboxCleanupDisposition::Destroyed,
                sandbox_identity_digest: command.sandbox_identity_digest,
                grants_revoked: true,
                ephemeral_storage_destroyed: true,
                observed_at: chrono::Utc::now(),
                evidence_digest: format!("sha256:{}", "e".repeat(64)).parse().unwrap(),
            })
        }

        async fn abort(
            &self,
            _fence: MicroVmProviderExecutionFence,
            _command: AbortSandboxExecution,
        ) -> Result<insight_platform_sandbox::SandboxAbortEvidence, SandboxBackendFailure> {
            Err(recording_backend_failure(
                SandboxBackendFailureStage::Aborting,
            ))
        }

        async fn recover_expired_lease(
            &self,
            _fence: MicroVmProviderExecutionFence,
            _expired: ExpiredSandboxLease,
        ) -> Result<SandboxLeaseRecoveryEvidence, SandboxBackendFailure> {
            Err(recording_backend_failure(
                SandboxBackendFailureStage::Recovering,
            ))
        }
    }

    struct RecordingProcessIsolation {
        calls: AtomicUsize,
        registrations: AtomicUsize,
        registered_host_process_id: AtomicU32,
        attestor_identity_digest: Sha256Digest,
    }

    #[async_trait]
    impl SandboxProcessGenerationIsolation for RecordingProcessIsolation {
        async fn prove_absent(
            &self,
            request: ProveSandboxProcessGenerationAbsent,
        ) -> Result<SandboxProcessGenerationAbsenceEvidence, SandboxProcessGenerationIsolationError>
        {
            self.calls.fetch_add(1, Ordering::AcqRel);
            SandboxProcessGenerationAbsenceEvidence {
                schema_version: 1,
                tenant_id: request.tenant_id,
                sandbox_job_id: request.sandbox_job_id,
                request_digest: request.request_digest,
                previous_worker_process_generation_id: request
                    .previous_worker_process_generation_id,
                executor_identity_digest: request.executor_identity_digest,
                attestor_identity_digest: self.attestor_identity_digest.clone(),
                attestor_route: request.attestor_route,
                disposition:
                    insight_platform_sandbox::SandboxProcessGenerationIsolationDisposition::ProcessAbsent,
                observed_at: chrono::Utc::now(),
                evidence_digest: format!("sha256:{}", "0".repeat(64)).parse().unwrap(),
            }
            .seal()
        }
    }

    #[async_trait]
    impl WasiExecutorProcessAttestationAuthority for RecordingProcessIsolation {
        async fn register_observed(
            &self,
            request: RegisterWasiExecutorProcessGeneration,
            peer: WasiExecutorRegistrationPeer,
        ) -> Result<WasiExecutorProcessIdentityEvidence, WasiExecutorProcessRegistrationError>
        {
            peer.validate()?;
            self.registrations.fetch_add(1, Ordering::AcqRel);
            self.registered_host_process_id
                .store(peer.host_process_id, Ordering::Release);
            WasiExecutorProcessIdentityEvidence {
                schema_version: 1,
                worker_process_generation_id: request.worker_process_generation_id,
                worker_manifest_digest: request.worker_manifest_digest,
                isolation_backend_contract_digest: request.isolation_backend_contract_digest,
                executor_instance_binding_digest: canonical_digest(&serde_json::json!({
                    "host_group_id": peer.host_group_id,
                    "host_process_id": peer.host_process_id,
                    "host_user_id": peer.host_user_id,
                }))
                .unwrap()
                .parse()
                .unwrap(),
                executor_identity_digest: format!("sha256:{}", "0".repeat(64)).parse().unwrap(),
                attestor_identity_digest: self.attestor_identity_digest.clone(),
                attestor_route: attestor_route(),
                observed_at: chrono::Utc::now(),
                evidence_digest: format!("sha256:{}", "0".repeat(64)).parse().unwrap(),
            }
            .seal()
        }
    }

    #[async_trait]
    impl WasiExecutorProcessRegistrationVerifier for RecordingProcessIsolation {
        async fn verify_registered(
            &self,
            request: VerifyWasiExecutorProcessGeneration,
        ) -> Result<WasiExecutorProcessIdentityEvidence, WasiExecutorProcessRegistrationError>
        {
            Ok(WasiExecutorProcessIdentityEvidence {
                schema_version: 1,
                worker_process_generation_id: request.worker_process_generation_id,
                worker_manifest_digest: request.worker_manifest_digest,
                isolation_backend_contract_digest: request.isolation_backend_contract_digest,
                executor_instance_binding_digest: format!("sha256:{}", "9".repeat(64))
                    .parse()
                    .unwrap(),
                executor_identity_digest: request.executor_identity_digest,
                attestor_identity_digest: self.attestor_identity_digest.clone(),
                attestor_route: request.attestor_route,
                observed_at: chrono::Utc::now(),
                evidence_digest: format!("sha256:{}", "0".repeat(64)).parse().unwrap(),
            })
        }
    }

    async fn mtls_channel(
        endpoint: &str,
        fixture: &MtlsFixture,
        certificate: &str,
        key: &str,
    ) -> tonic::transport::Channel {
        Endpoint::from_shared(endpoint.to_owned())
            .unwrap()
            .tls_config(
                ClientTlsConfig::new()
                    .domain_name("localhost")
                    .ca_certificate(Certificate::from_pem(&fixture.ca_pem))
                    .identity(Identity::from_pem(certificate, key)),
            )
            .unwrap()
            .connect()
            .await
            .unwrap()
    }

    async fn mtls_uds_channel(
        socket_path: PathBuf,
        fixture: &MtlsFixture,
        certificate: &str,
        key: &str,
    ) -> tonic::transport::Channel {
        Endpoint::from_shared("https://localhost".to_owned())
            .unwrap()
            .tls_config(
                ClientTlsConfig::new()
                    .domain_name("localhost")
                    .ca_certificate(Certificate::from_pem(&fixture.ca_pem))
                    .identity(Identity::from_pem(certificate, key)),
            )
            .unwrap()
            .connect_with_connector(tower::service_fn(move |_| {
                let socket_path = socket_path.clone();
                async move { UnixStream::connect(socket_path).await.map(TokioIo::new) }
            }))
            .await
            .unwrap()
    }

    #[test]
    fn envelope_is_canonical_bounded_and_digest_bound() {
        let limits =
            SandboxInternalRpcLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
        let command = ClaimSandboxJobs {
            worker_process_generation_id: ResourceId::from_uuid_v7(
                ResourceKind::WorkerProcessGeneration,
                uuid::Uuid::now_v7(),
            )
            .unwrap(),
            worker_manifest_digest: format!("sha256:{}", "b".repeat(64)).parse().unwrap(),
            isolation_backend_contract_digest: format!("sha256:{}", "c".repeat(64))
                .parse()
                .unwrap(),
            executor_identity_digest: format!("sha256:{}", "d".repeat(64)).parse().unwrap(),
            attestor_route: attestor_route(),
            limit: 1,
            lease_milliseconds: 30_000,
            lease_token_digests: vec![format!("sha256:{}", "a".repeat(64)).parse().unwrap()],
        };
        let envelope = encode(&command, limits).unwrap();
        assert_eq!(
            decode::<ClaimSandboxJobs>(envelope.clone(), limits).unwrap(),
            command
        );

        let mut tampered = envelope;
        tampered.canonical_json.push(b' ');
        assert_eq!(
            decode::<ClaimSandboxJobs>(tampered, limits),
            Err(SandboxRpcError::InvalidEnvelope)
        );
    }

    #[test]
    fn workload_identity_uses_one_exact_uri_san_only() {
        let expected = SanType::URI(WASI_EXECUTOR_WORKLOAD_IDENTITY.try_into().unwrap());
        let exact = certificate_with_sans(vec![expected.clone()]);
        require_exact_workload_uri(&exact, WASI_EXECUTOR_WORKLOAD_IDENTITY).unwrap();

        let micro_vm = certificate_with_sans(vec![SanType::URI(
            MICROVM_EXECUTOR_WORKLOAD_IDENTITY.try_into().unwrap(),
        )]);
        require_allowed_workload_uri(
            &micro_vm,
            &[
                WASI_EXECUTOR_WORKLOAD_IDENTITY,
                MICROVM_EXECUTOR_WORKLOAD_IDENTITY,
            ],
        )
        .unwrap();
        assert_eq!(
            require_exact_workload_uri(&micro_vm, WASI_EXECUTOR_WORKLOAD_IDENTITY)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );

        let wrong = certificate_with_sans(vec![SanType::URI(
            "spiffe://insight.platform/workload/model-worker"
                .try_into()
                .unwrap(),
        )]);
        assert_eq!(
            require_exact_workload_uri(&wrong, WASI_EXECUTOR_WORKLOAD_IDENTITY)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );

        let ambiguous = certificate_with_sans(vec![
            expected,
            SanType::URI(
                "spiffe://insight.platform/workload/sandbox-executor.gvisor"
                    .try_into()
                    .unwrap(),
            ),
        ]);
        assert_eq!(
            require_exact_workload_uri(&ambiguous, WASI_EXECUTOR_WORKLOAD_IDENTITY)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );

        let dns_only = certificate_with_sans(vec![SanType::DnsName(
            "sandbox-executor.platform.svc".try_into().unwrap(),
        )]);
        assert_eq!(
            require_exact_workload_uri(&dns_only, WASI_EXECUTOR_WORKLOAD_IDENTITY)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );

        let microvm = certificate_with_sans(vec![SanType::URI(
            MICROVM_EXECUTOR_WORKLOAD_IDENTITY.try_into().unwrap(),
        )]);
        require_exact_workload_uri(&microvm, MICROVM_EXECUTOR_WORKLOAD_IDENTITY).unwrap();
        assert_eq!(
            require_exact_workload_uri(&microvm, WASI_EXECUTOR_WORKLOAD_IDENTITY)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );

        let egress = certificate_with_sans(vec![SanType::URI(
            EGRESS_BROKER_WORKLOAD_IDENTITY.try_into().unwrap(),
        )]);
        require_exact_workload_uri(&egress, EGRESS_BROKER_WORKLOAD_IDENTITY).unwrap();
        assert_eq!(
            require_exact_workload_uri(&egress, MICROVM_PROVIDER_WORKLOAD_IDENTITY)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }

    #[tokio::test]
    async fn real_mtls_rejects_a_wrong_ca_valid_workload_before_authority() {
        let fixture = mtls_fixture();
        let authority = Arc::new(RecordingAuthority::default());
        let process_registration = Arc::new(RecordingProcessIsolation {
            calls: AtomicUsize::new(0),
            registrations: AtomicUsize::new(0),
            registered_host_process_id: AtomicU32::new(0),
            attestor_identity_digest: format!("sha256:{}", "e".repeat(64)).parse().unwrap(),
        });
        let limits =
            SandboxInternalRpcLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = incoming.local_addr().unwrap();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
        let service = proto::sandbox_executor_authority_service_server::SandboxExecutorAuthorityServiceServer::new(
            SandboxAuthorityGrpcService::new(
                Arc::clone(&authority),
                process_registration,
                limits,
            ),
        )
        .max_encoding_message_size(limits.maximum_message_bytes())
        .max_decoding_message_size(limits.maximum_message_bytes());
        let service = tonic::service::interceptor::InterceptedService::new(
            service,
            WasiExecutorWorkloadIdentity,
        );
        let tls = ServerTlsConfig::new()
            .identity(Identity::from_pem(
                &fixture.server_certificate_pem,
                &fixture.server_key_pem,
            ))
            .client_ca_root(Certificate::from_pem(&fixture.ca_pem));
        let server = tokio::spawn(async move {
            Server::builder()
                .tls_config(tls)
                .unwrap()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_receiver.await;
                })
                .await
                .unwrap();
        });
        let endpoint = format!("https://localhost:{}", address.port());
        let command = ClaimSandboxJobs {
            worker_process_generation_id: ResourceId::from_uuid_v7(
                ResourceKind::WorkerProcessGeneration,
                uuid::Uuid::now_v7(),
            )
            .unwrap(),
            worker_manifest_digest: format!("sha256:{}", "b".repeat(64)).parse().unwrap(),
            isolation_backend_contract_digest: format!("sha256:{}", "c".repeat(64))
                .parse()
                .unwrap(),
            executor_identity_digest: format!("sha256:{}", "d".repeat(64)).parse().unwrap(),
            attestor_route: attestor_route(),
            limit: 1,
            lease_milliseconds: 30_000,
            lease_token_digests: vec![format!("sha256:{}", "a".repeat(64)).parse().unwrap()],
        };

        let channel = mtls_channel(
            &endpoint,
            &fixture,
            &fixture.wasi_certificate_pem,
            &fixture.wasi_key_pem,
        )
        .await;
        let mut client = SandboxExecutorAuthorityServiceClient::new(channel);
        let accepted = client
            .claim_sandbox_jobs(Request::new(encode(&command, limits).unwrap()))
            .await
            .unwrap();
        assert!(
            decode::<Vec<ClaimedSandboxJob>>(accepted.into_inner(), limits)
                .unwrap()
                .is_empty()
        );
        assert_eq!(authority.claims.load(Ordering::Acquire), 1);

        let channel = mtls_channel(
            &endpoint,
            &fixture,
            &fixture.wrong_certificate_pem,
            &fixture.wrong_key_pem,
        )
        .await;
        let mut client = SandboxExecutorAuthorityServiceClient::new(channel);
        let rejected = client
            .claim_sandbox_jobs(Request::new(encode(&command, limits).unwrap()))
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::PermissionDenied);
        assert_eq!(authority.claims.load(Ordering::Acquire), 1);

        shutdown_sender.send(()).unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn managed_session_authority_rpc_accepts_only_the_microvm_executor_lane() {
        let fixture = mtls_fixture();
        let authority = Arc::new(RecordingAuthority::default());
        let process_registration = Arc::new(RecordingProcessIsolation {
            calls: AtomicUsize::new(0),
            registrations: AtomicUsize::new(0),
            registered_host_process_id: AtomicU32::new(0),
            attestor_identity_digest: format!("sha256:{}", "e".repeat(64)).parse().unwrap(),
        });
        let limits =
            SandboxInternalRpcLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = incoming.local_addr().unwrap();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
        let service = proto::sandbox_managed_mcp_session_authority_service_server::SandboxManagedMcpSessionAuthorityServiceServer::new(
            SandboxManagedMcpSessionAuthorityGrpcService::new(
                Arc::clone(&authority),
                Arc::clone(&process_registration),
                limits,
            ),
        )
        .max_encoding_message_size(limits.maximum_message_bytes())
        .max_decoding_message_size(limits.maximum_message_bytes());
        let service = tonic::service::interceptor::InterceptedService::new(
            service,
            MicroVmExecutorWorkloadIdentity,
        );
        let tls = ServerTlsConfig::new()
            .identity(Identity::from_pem(
                &fixture.server_certificate_pem,
                &fixture.server_key_pem,
            ))
            .client_ca_root(Certificate::from_pem(&fixture.ca_pem));
        let server = tokio::spawn(async move {
            Server::builder()
                .tls_config(tls)
                .unwrap()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_receiver.await;
                })
                .await
                .unwrap();
        });
        let endpoint = format!("https://localhost:{}", address.port());
        let command = ClaimSandboxJobs {
            worker_process_generation_id: ResourceId::from_uuid_v7(
                ResourceKind::WorkerProcessGeneration,
                uuid::Uuid::now_v7(),
            )
            .unwrap(),
            worker_manifest_digest: format!("sha256:{}", "b".repeat(64)).parse().unwrap(),
            isolation_backend_contract_digest: format!("sha256:{}", "c".repeat(64))
                .parse()
                .unwrap(),
            executor_identity_digest: format!("sha256:{}", "d".repeat(64)).parse().unwrap(),
            attestor_route: attestor_route(),
            limit: 1,
            lease_milliseconds: 30_000,
            lease_token_digests: vec![format!("sha256:{}", "a".repeat(64)).parse().unwrap()],
        };

        let channel = mtls_channel(
            &endpoint,
            &fixture,
            &fixture.microvm_certificate_pem,
            &fixture.microvm_key_pem,
        )
        .await;
        let client = SandboxManagedMcpSessionAuthorityGrpcClient::new(channel, limits);
        assert!(client
            .claim_managed_mcp_sandbox_sessions(command.clone())
            .await
            .unwrap()
            .is_empty());
        assert_eq!(authority.claims.load(Ordering::Acquire), 1);

        let recovery_scan = ScanExpiredManagedMcpSandboxSessionLeases {
            executor: ManagedMcpSandboxSessionRecoveryExecutor {
                worker_process_generation_id: command.worker_process_generation_id.clone(),
                worker_manifest_digest: command.worker_manifest_digest.clone(),
                isolation_backend_contract_digest: command
                    .isolation_backend_contract_digest
                    .clone(),
                executor_identity_digest: command.executor_identity_digest.clone(),
                attestor_route: command.attestor_route.clone(),
            },
            shard: ManagedMcpSandboxSessionRecoveryShard { index: 0, count: 1 },
            after: None,
            limit: 1,
        };
        let page = client
            .scan_expired_managed_mcp_sandbox_session_leases(recovery_scan.clone())
            .await
            .unwrap();
        assert!(page.exhausted);
        assert!(page.records.is_empty());
        assert_eq!(authority.managed_recovery_scans.load(Ordering::Acquire), 1);

        let absence_request = ProveSandboxProcessGenerationAbsent {
            tenant_id: ResourceId::from_uuid_v7(ResourceKind::Tenant, uuid::Uuid::now_v7())
                .unwrap(),
            sandbox_job_id: ResourceId::from_uuid_v7(ResourceKind::Job, uuid::Uuid::now_v7())
                .unwrap(),
            request_digest: format!("sha256:{}", "f".repeat(64)).parse().unwrap(),
            previous_worker_process_generation_id: ResourceId::from_uuid_v7(
                ResourceKind::WorkerProcessGeneration,
                uuid::Uuid::now_v7(),
            )
            .unwrap(),
            executor_identity_digest: format!("sha256:{}", "1".repeat(64)).parse().unwrap(),
            attestor_route: attestor_route(),
        };
        let absence = client.prove_absent(absence_request.clone()).await.unwrap();
        absence
            .validate_for(&absence_request, chrono::Utc::now())
            .unwrap();
        assert_eq!(process_registration.calls.load(Ordering::Acquire), 1);

        let heartbeat = HeartbeatSandboxExecution {
            tenant_id: ResourceId::from_uuid_v7(ResourceKind::Tenant, uuid::Uuid::now_v7())
                .unwrap(),
            sandbox_job_id: ResourceId::from_uuid_v7(ResourceKind::Job, uuid::Uuid::now_v7())
                .unwrap(),
            job_id: ResourceId::from_uuid_v7(ResourceKind::Job, uuid::Uuid::now_v7()).unwrap(),
            fence: JobFence {
                expected_version: 1,
                worker_process_generation_id: command.worker_process_generation_id.clone(),
                lease_generation: 1,
                token_digest: command.lease_token_digests[0].clone(),
            },
            lease_milliseconds: 30_000,
        };
        assert_eq!(
            client
                .heartbeat_managed_mcp_sandbox_session(heartbeat.clone())
                .await
                .unwrap_err(),
            SandboxRpcError::Rejected
        );
        assert_eq!(authority.managed_heartbeats.load(Ordering::Acquire), 1);

        let channel = mtls_channel(
            &endpoint,
            &fixture,
            &fixture.wasi_certificate_pem,
            &fixture.wasi_key_pem,
        )
        .await;
        let mut unauthorized = SandboxManagedMcpSessionAuthorityServiceClient::new(channel);
        let rejected = unauthorized
            .claim_managed_mcp_sandbox_sessions(Request::new(encode(&command, limits).unwrap()))
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::PermissionDenied);
        assert_eq!(authority.claims.load(Ordering::Acquire), 1);
        let rejected = unauthorized
            .heartbeat_managed_mcp_sandbox_session(Request::new(
                encode(&heartbeat, limits).unwrap(),
            ))
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::PermissionDenied);
        assert_eq!(authority.managed_heartbeats.load(Ordering::Acquire), 1);
        let rejected = unauthorized
            .scan_expired_managed_mcp_sandbox_session_leases(Request::new(
                encode(&recovery_scan, limits).unwrap(),
            ))
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::PermissionDenied);
        assert_eq!(authority.managed_recovery_scans.load(Ordering::Acquire), 1);

        shutdown_sender.send(()).unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn microvm_provider_rpc_is_exact_role_gated_and_lifecycle_only() {
        let fixture = mtls_fixture();
        let profile = checked_in_hard_limit_profile();
        let limits = SandboxInternalRpcLimits::from_profile(&profile).unwrap();
        let sandbox_limits = SandboxCommandLimits::from_profile(&profile).unwrap();
        let descriptor = InstalledSandboxBackendDescriptor {
            backend_kind: SandboxIsolationBackendKind::MicroVm,
            isolation_class: insight_platform_contracts::SandboxIsolationClass::MicroVm,
            worker_manifest_digest: format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
            backend_contract_digest: format!("sha256:{}", "b".repeat(64)).parse().unwrap(),
        };
        let backend = Arc::new(RecordingMicroVmBackend {
            destroys: AtomicUsize::new(0),
            executor_generations: Mutex::new(Vec::new()),
            descriptor: descriptor.clone(),
        });
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = incoming.local_addr().unwrap();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
        let service = proto::sandbox_isolation_provider_service_server::SandboxIsolationProviderServiceServer::new(
            SandboxIsolationProviderGrpcService::new(
                Arc::clone(&backend),
                limits,
                sandbox_limits,
            )
            .unwrap(),
        )
        .max_encoding_message_size(limits.maximum_message_bytes())
        .max_decoding_message_size(limits.maximum_message_bytes());
        let service = tonic::service::interceptor::InterceptedService::new(
            service,
            MicroVmExecutorWorkloadIdentity,
        );
        let tls = ServerTlsConfig::new()
            .identity(Identity::from_pem(
                &fixture.provider_server_certificate_pem,
                &fixture.provider_server_key_pem,
            ))
            .client_ca_root(Certificate::from_pem(&fixture.ca_pem));
        let server = tokio::spawn(async move {
            Server::builder()
                .tls_config(tls)
                .unwrap()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_receiver.await;
                })
                .await
                .unwrap();
        });
        let endpoint = format!("https://localhost:{}", address.port());
        let command = DestroySandbox {
            tenant_id: ResourceId::from_uuid_v7(ResourceKind::Tenant, uuid::Uuid::now_v7())
                .unwrap(),
            sandbox_job_id: ResourceId::from_uuid_v7(ResourceKind::Job, uuid::Uuid::now_v7())
                .unwrap(),
            request_digest: format!("sha256:{}", "c".repeat(64)).parse().unwrap(),
            sandbox_identity_digest: format!("sha256:{}", "d".repeat(64)).parse().unwrap(),
            attempt_no: 1,
            lease_generation: 1,
            effect: insight_platform_contracts::Effect::Pure,
            network_mode: insight_platform_sandbox::SandboxNetworkMode::None,
        };

        let channel = mtls_channel(
            &endpoint,
            &fixture,
            &fixture.microvm_certificate_pem,
            &fixture.microvm_key_pem,
        )
        .await;
        let worker_process_generation_id =
            ResourceId::from_uuid_v7(ResourceKind::WorkerProcessGeneration, uuid::Uuid::now_v7())
                .unwrap();
        let client = SandboxIsolationProviderGrpcClient::new(
            channel,
            limits,
            descriptor,
            worker_process_generation_id.clone(),
        )
        .unwrap();
        let cleanup = client.destroy(command.clone()).await.unwrap();
        assert_eq!(
            cleanup.sandbox_identity_digest,
            command.sandbox_identity_digest
        );
        assert_eq!(backend.destroys.load(Ordering::Acquire), 1);
        assert_eq!(
            *backend.executor_generations.lock().unwrap(),
            vec![worker_process_generation_id]
        );

        let channel = mtls_channel(
            &endpoint,
            &fixture,
            &fixture.wrong_certificate_pem,
            &fixture.wrong_key_pem,
        )
        .await;
        let mut unauthorized = SandboxIsolationProviderServiceClient::new(channel);
        let rejected = unauthorized
            .destroy_sandbox(Request::new(encode(&command, limits).unwrap()))
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::PermissionDenied);
        assert_eq!(backend.destroys.load(Ordering::Acquire), 1);

        shutdown_sender.send(()).unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn managed_mcp_provider_rpc_is_exact_role_gated_and_closed() {
        let fixture = mtls_fixture();
        let profile = checked_in_hard_limit_profile();
        let limits = SandboxInternalRpcLimits::from_profile(&profile).unwrap();
        let sandbox_limits = SandboxCommandLimits::from_profile(&profile).unwrap();
        let descriptor = InstalledSandboxBackendDescriptor {
            backend_kind: SandboxIsolationBackendKind::MicroVm,
            isolation_class: insight_platform_contracts::SandboxIsolationClass::MicroVm,
            worker_manifest_digest: format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
            backend_contract_digest: format!("sha256:{}", "b".repeat(64)).parse().unwrap(),
        };
        let provider = Arc::new(RecordingManagedMcpSessionProvider::default());
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = incoming.local_addr().unwrap();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
        let service = proto::sandbox_managed_mcp_session_provider_service_server::SandboxManagedMcpSessionProviderServiceServer::new(
            SandboxManagedMcpSessionProviderGrpcService::new(
                Arc::clone(&provider),
                descriptor,
                limits,
                sandbox_limits,
            )
            .unwrap(),
        )
        .max_encoding_message_size(limits.maximum_message_bytes())
        .max_decoding_message_size(limits.maximum_message_bytes());
        let service = tonic::service::interceptor::InterceptedService::new(
            service,
            MicroVmExecutorWorkloadIdentity,
        );
        let tls = ServerTlsConfig::new()
            .identity(Identity::from_pem(
                &fixture.provider_server_certificate_pem,
                &fixture.provider_server_key_pem,
            ))
            .client_ca_root(Certificate::from_pem(&fixture.ca_pem));
        let server = tokio::spawn(async move {
            Server::builder()
                .tls_config(tls)
                .unwrap()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_receiver.await;
                })
                .await
                .unwrap();
        });
        let endpoint = format!("https://localhost:{}", address.port());
        let invalid = encode(&serde_json::json!({}), limits).unwrap();

        let channel = mtls_channel(
            &endpoint,
            &fixture,
            &fixture.wrong_certificate_pem,
            &fixture.wrong_key_pem,
        )
        .await;
        let mut unauthorized = SandboxManagedMcpSessionProviderServiceClient::new(channel);
        let rejected = unauthorized
            .prepare_managed_mcp_sandbox_session(Request::new(invalid.clone()))
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::PermissionDenied);

        let channel = mtls_channel(
            &endpoint,
            &fixture,
            &fixture.microvm_certificate_pem,
            &fixture.microvm_key_pem,
        )
        .await;
        let mut authorized = SandboxManagedMcpSessionProviderServiceClient::new(channel);
        let rejected = authorized
            .prepare_managed_mcp_sandbox_session(Request::new(invalid))
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::InvalidArgument);
        assert_eq!(provider.calls.load(Ordering::Acquire), 0);

        shutdown_sender.send(()).unwrap();
        server.await.unwrap();
    }

    fn recording_process_isolation() -> Arc<RecordingProcessIsolation> {
        Arc::new(RecordingProcessIsolation {
            calls: AtomicUsize::new(0),
            registrations: AtomicUsize::new(0),
            registered_host_process_id: AtomicU32::new(0),
            attestor_identity_digest: format!("sha256:{}", "e".repeat(64)).parse().unwrap(),
        })
    }

    #[test]
    fn artifact_rpc_request_carries_a_positive_domain_bounded_timeout() {
        let domain_budget = std::time::Duration::from_secs(5);
        let deadline = Utc::now() + chrono::Duration::from_std(domain_budget).unwrap();
        let (request, deadline_at) = artifact_request_with_deadline((), deadline).unwrap();
        assert!(request.metadata().contains_key("grpc-timeout"));
        assert!(deadline_at > tokio::time::Instant::now());
        assert!(deadline_at <= tokio::time::Instant::now() + domain_budget);
        assert!(matches!(
            artifact_request_with_deadline((), Utc::now()),
            Err(SandboxArtifactClientReadError::Unavailable)
        ));
    }

    #[tokio::test]
    async fn wasi_client_enforces_the_absolute_deadline_after_response_headers() {
        let limits =
            SandboxInternalRpcLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
        let observed_timeout = Arc::new(AtomicBool::new(false));
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = incoming.local_addr().unwrap();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
        let service =
            proto::sandbox_executor_broker_service_server::SandboxExecutorBrokerServiceServer::new(
                StallingWasiArtifactService {
                    observed_timeout: Arc::clone(&observed_timeout),
                },
            )
            .max_encoding_message_size(limits.maximum_message_bytes())
            .max_decoding_message_size(limits.maximum_message_bytes());
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_receiver.await;
                })
                .await
                .unwrap();
        });
        let channel = Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let client = SandboxBrokerGrpcClient::new(channel, limits);
        let request =
            wasi_artifact_request(b"deadline", Utc::now() + chrono::Duration::milliseconds(50));

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            WasiArtifactBroker::read_exact(&client, request),
        )
        .await
        .unwrap();
        assert_eq!(result, Err(WasiArtifactBrokerError::Unavailable));
        assert!(observed_timeout.load(Ordering::Acquire));

        shutdown_sender.send(()).unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn wasi_and_micro_vm_share_capacity_until_stream_completion() {
        let limits =
            SandboxInternalRpcLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
        let artifact_bytes = vec![0x4d; SANDBOX_ARTIFACT_CHUNK_BYTES + 17];
        let artifacts = Arc::new(RecordingArtifactBroker {
            calls: AtomicUsize::new(0),
            value: artifact_bytes.clone(),
            requests: Mutex::new(Vec::new()),
        });
        let capacity = SandboxArtifactResponseCapacity::new(1).unwrap();
        let dependencies = Arc::new(RecordingWasiBrokerDependencies);
        let wasi_service = SandboxBrokerGrpcService::new(
            Arc::clone(&artifacts),
            Arc::clone(&dependencies),
            dependencies,
            recording_process_isolation(),
            limits,
            capacity.clone(),
        );
        let micro_vm_service = SandboxMicroVmBrokerGrpcService::new(
            Arc::clone(&artifacts),
            Arc::new(RecordingMicroVmGrantRevoker::default()),
            limits,
            capacity.clone(),
        );
        let deadline = Utc::now() + chrono::Duration::minutes(2);
        let wasi_request = wasi_artifact_request(&artifact_bytes, deadline);
        let micro_vm_request = micro_vm_artifact_request(&artifact_bytes, deadline);

        let mut wasi_stream = wasi_service
            .read_exact_artifact(Request::new(encode(&wasi_request, limits).unwrap()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(capacity.available_permits(), 0);
        let saturated = micro_vm_service
            .read_exact_micro_vm_artifact(Request::new(encode(&micro_vm_request, limits).unwrap()))
            .await
            .err()
            .unwrap();
        assert_eq!(saturated.code(), tonic::Code::ResourceExhausted);
        assert_eq!(artifacts.calls.load(Ordering::Acquire), 1);

        let first = wasi_stream.next().await.unwrap().unwrap();
        assert_eq!(first.sequence, 0);
        let last = wasi_stream.next().await.unwrap().unwrap();
        assert_eq!(last.sequence, 1);
        assert_eq!(capacity.available_permits(), 0);
        assert!(wasi_stream.next().await.is_none());
        assert_eq!(capacity.available_permits(), 1);

        let released = micro_vm_service
            .read_exact_micro_vm_artifact(Request::new(encode(&micro_vm_request, limits).unwrap()))
            .await
            .unwrap();
        drop(released);
        assert_eq!(capacity.available_permits(), 1);
    }

    #[tokio::test]
    async fn wasi_stream_supports_an_exact_zero_byte_input() {
        let limits =
            SandboxInternalRpcLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
        let artifacts = Arc::new(RecordingArtifactBroker {
            calls: AtomicUsize::new(0),
            value: Vec::new(),
            requests: Mutex::new(Vec::new()),
        });
        let capacity = SandboxArtifactResponseCapacity::new(1).unwrap();
        let dependencies = Arc::new(RecordingWasiBrokerDependencies);
        let service = SandboxBrokerGrpcService::new(
            artifacts,
            Arc::clone(&dependencies),
            dependencies,
            recording_process_isolation(),
            limits,
            capacity.clone(),
        );
        let request = wasi_artifact_request(&[], Utc::now() + chrono::Duration::seconds(30));

        let mut stream = service
            .read_exact_artifact(Request::new(encode(&request, limits).unwrap()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(capacity.available_permits(), 0);
        assert!(stream.next().await.is_none());
        assert_eq!(capacity.available_permits(), 1);
    }

    #[tokio::test]
    async fn slow_maximum_artifact_response_holds_capacity_without_full_chunk_precopy() {
        let limits =
            SandboxInternalRpcLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
        let artifact_bytes = vec![0x5a; usize::try_from(MAX_SANDBOX_RUNTIME_BUNDLE_BYTES).unwrap()];
        let request =
            micro_vm_artifact_request(&artifact_bytes, Utc::now() + chrono::Duration::minutes(2));
        let artifacts = Arc::new(RecordingArtifactBroker {
            calls: AtomicUsize::new(0),
            value: artifact_bytes,
            requests: Mutex::new(Vec::new()),
        });
        assert!(matches!(
            SandboxArtifactResponseCapacity::new(MAX_IN_FLIGHT_ARTIFACT_RESPONSES_HARD + 1),
            Err(SandboxRpcError::InvalidConfiguration)
        ));
        let capacity = SandboxArtifactResponseCapacity::new(1).unwrap();
        let service = SandboxMicroVmBrokerGrpcService::new(
            Arc::clone(&artifacts),
            Arc::new(RecordingMicroVmGrantRevoker::default()),
            limits,
            capacity.clone(),
        );

        let mut first = service
            .read_exact_micro_vm_artifact(Request::new(encode(&request, limits).unwrap()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(artifacts.calls.load(Ordering::Acquire), 1);
        assert_eq!(capacity.available_permits(), 0);

        let saturated = service
            .read_exact_micro_vm_artifact(Request::new(encode(&request, limits).unwrap()))
            .await;
        assert!(matches!(
            saturated,
            Err(status) if status.code() == tonic::Code::ResourceExhausted
        ));
        assert_eq!(artifacts.calls.load(Ordering::Acquire), 1);

        let first_chunk = first.next().await.unwrap().unwrap();
        assert_eq!(first_chunk.sequence, 0);
        assert_eq!(first_chunk.value.len(), SANDBOX_ARTIFACT_CHUNK_BYTES);
        let still_saturated = service
            .read_exact_micro_vm_artifact(Request::new(encode(&request, limits).unwrap()))
            .await;
        assert!(matches!(
            still_saturated,
            Err(status) if status.code() == tonic::Code::ResourceExhausted
        ));
        assert_eq!(artifacts.calls.load(Ordering::Acquire), 1);

        drop(first);
        assert_eq!(capacity.available_permits(), 1);

        let mut completed = service
            .read_exact_micro_vm_artifact(Request::new(encode(&request, limits).unwrap()))
            .await
            .unwrap()
            .into_inner();
        let mut expected_sequence = 0_u64;
        let mut observed_bytes = 0_u64;
        loop {
            let chunk = completed.next().await.unwrap().unwrap();
            assert_eq!(chunk.sequence, expected_sequence);
            assert_eq!(chunk.total_bytes, MAX_SANDBOX_RUNTIME_BUNDLE_BYTES);
            observed_bytes += u64::try_from(chunk.value.len()).unwrap();
            expected_sequence += 1;
            if observed_bytes == MAX_SANDBOX_RUNTIME_BUNDLE_BYTES {
                break;
            }
        }
        assert_eq!(observed_bytes, MAX_SANDBOX_RUNTIME_BUNDLE_BYTES);
        assert_eq!(capacity.available_permits(), 0);
        assert!(completed.next().await.is_none());
        assert_eq!(capacity.available_permits(), 1);
        assert_eq!(artifacts.calls.load(Ordering::Acquire), 2);
    }

    #[tokio::test]
    async fn abandoned_artifact_response_releases_capacity_at_absolute_deadline() {
        let limits =
            SandboxInternalRpcLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
        let artifact_bytes = vec![0x6b; SANDBOX_ARTIFACT_CHUNK_BYTES + 17];
        let expiring_request = micro_vm_artifact_request(
            &artifact_bytes,
            Utc::now() + chrono::Duration::milliseconds(50),
        );
        let fresh_request = MicroVmArtifactReadRequest {
            deadline: Utc::now() + chrono::Duration::seconds(30),
            ..expiring_request.clone()
        };
        let artifacts = Arc::new(RecordingArtifactBroker {
            calls: AtomicUsize::new(0),
            value: artifact_bytes,
            requests: Mutex::new(Vec::new()),
        });
        let capacity = SandboxArtifactResponseCapacity::new(1).unwrap();
        let service = SandboxMicroVmBrokerGrpcService::new(
            Arc::clone(&artifacts),
            Arc::new(RecordingMicroVmGrantRevoker::default()),
            limits,
            capacity.clone(),
        );

        let mut abandoned = service
            .read_exact_micro_vm_artifact(Request::new(encode(&expiring_request, limits).unwrap()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(capacity.available_permits(), 0);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while capacity.available_permits() == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();

        let replacement = service
            .read_exact_micro_vm_artifact(Request::new(encode(&fresh_request, limits).unwrap()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(artifacts.calls.load(Ordering::Acquire), 2);
        let expired = abandoned.next().await.unwrap().unwrap_err();
        assert_eq!(expired.code(), tonic::Code::DeadlineExceeded);
        assert!(abandoned.next().await.is_none());
        drop(replacement);
        assert_eq!(capacity.available_permits(), 1);
    }

    #[tokio::test]
    async fn upstream_artifact_failure_does_not_leak_response_capacity() {
        let limits =
            SandboxInternalRpcLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
        let artifact_bytes = vec![0x7c; 4_097];
        let request =
            micro_vm_artifact_request(&artifact_bytes, Utc::now() + chrono::Duration::seconds(30));
        let artifacts = Arc::new(FailOnceArtifactBroker {
            calls: AtomicUsize::new(0),
            value: artifact_bytes,
        });
        let capacity = SandboxArtifactResponseCapacity::new(1).unwrap();
        let service = SandboxMicroVmBrokerGrpcService::new(
            Arc::clone(&artifacts),
            Arc::new(RecordingMicroVmGrantRevoker::default()),
            limits,
            capacity.clone(),
        );

        let failed = service
            .read_exact_micro_vm_artifact(Request::new(encode(&request, limits).unwrap()))
            .await;
        assert!(matches!(
            failed,
            Err(status) if status.code() == tonic::Code::Unavailable
        ));
        assert_eq!(capacity.available_permits(), 1);

        let recovered = service
            .read_exact_micro_vm_artifact(Request::new(encode(&request, limits).unwrap()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(artifacts.calls.load(Ordering::Acquire), 2);
        drop(recovered);
        assert_eq!(capacity.available_permits(), 1);
    }

    #[tokio::test]
    async fn microvm_broker_rpc_accepts_only_provider_and_preserves_exact_cleanup_fence() {
        let fixture = mtls_fixture();
        let limits =
            SandboxInternalRpcLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
        let revoker = Arc::new(RecordingMicroVmGrantRevoker::default());
        let artifact_bytes = vec![0x5a; SANDBOX_ARTIFACT_CHUNK_BYTES + 173];
        let artifact = ArtifactRef::new(
            ResourceId::from_uuid_v7(ResourceKind::Artifact, uuid::Uuid::now_v7()).unwrap(),
            bytes_digest(&artifact_bytes).unwrap(),
            u64::try_from(artifact_bytes.len()).unwrap(),
            "application/vnd.insight.sandbox-bundle",
            DataClassification::Internal,
            None,
        )
        .unwrap();
        let artifacts = Arc::new(RecordingArtifactBroker {
            calls: AtomicUsize::new(0),
            value: artifact_bytes.clone(),
            requests: Mutex::new(Vec::new()),
        });
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = incoming.local_addr().unwrap();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
        let service =
            proto::sandbox_micro_vm_broker_service_server::SandboxMicroVmBrokerServiceServer::new(
                SandboxMicroVmBrokerGrpcService::new(
                    Arc::clone(&artifacts),
                    Arc::clone(&revoker),
                    limits,
                    SandboxArtifactResponseCapacity::new(1).unwrap(),
                ),
            )
            .max_encoding_message_size(limits.maximum_message_bytes())
            .max_decoding_message_size(limits.maximum_message_bytes());
        let service = tonic::service::interceptor::InterceptedService::new(
            service,
            MicroVmProviderWorkloadIdentity,
        );
        let tls = ServerTlsConfig::new()
            .identity(Identity::from_pem(
                &fixture.server_certificate_pem,
                &fixture.server_key_pem,
            ))
            .client_ca_root(Certificate::from_pem(&fixture.ca_pem));
        let server = tokio::spawn(async move {
            Server::builder()
                .tls_config(tls)
                .unwrap()
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_receiver.await;
                })
                .await
                .unwrap();
        });
        let endpoint = format!("https://localhost:{}", address.port());
        let request = RevokeMicroVmSandboxGrants {
            workload_kind: MicroVmSandboxWorkloadKind::CapabilityExecution,
            tenant_id: ResourceId::from_uuid_v7(ResourceKind::Tenant, uuid::Uuid::now_v7())
                .unwrap(),
            sandbox_job_id: ResourceId::from_uuid_v7(ResourceKind::Job, uuid::Uuid::now_v7())
                .unwrap(),
            request_digest: format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
            executor_worker_process_generation_id: ResourceId::from_uuid_v7(
                ResourceKind::WorkerProcessGeneration,
                uuid::Uuid::now_v7(),
            )
            .unwrap(),
            provider_process_generation_id: ResourceId::from_uuid_v7(
                ResourceKind::WorkerProcessGeneration,
                uuid::Uuid::now_v7(),
            )
            .unwrap(),
            sandbox_identity_digest: format!("sha256:{}", "b".repeat(64)).parse().unwrap(),
            attempt_no: 1,
            lease_generation: 1,
        };

        let channel = mtls_channel(
            &endpoint,
            &fixture,
            &fixture.microvm_provider_certificate_pem,
            &fixture.microvm_provider_key_pem,
        )
        .await;
        let client = SandboxMicroVmBrokerGrpcClient::new(channel, limits);
        let evidence = client.revoke_exact(request.clone()).await.unwrap();
        assert_eq!(
            evidence.evidence_digest,
            format!("sha256:{}", "e".repeat(64)).parse().unwrap()
        );
        assert_eq!(revoker.calls.load(Ordering::Acquire), 1);
        let artifact_request = MicroVmArtifactReadRequest {
            workload_kind: MicroVmSandboxWorkloadKind::CapabilityExecution,
            tenant_id: request.tenant_id.clone(),
            sandbox_job_id: request.sandbox_job_id.clone(),
            request_digest: request.request_digest.clone(),
            executor_worker_process_generation_id: request
                .executor_worker_process_generation_id
                .clone(),
            provider_process_generation_id: request.provider_process_generation_id.clone(),
            sandbox_identity_digest: request.sandbox_identity_digest.clone(),
            lease_generation: request.lease_generation,
            artifact: artifact.clone(),
            purpose: MicroVmArtifactReadPurpose::RuntimeBundle,
            read_grant: None,
            maximum_bytes: artifact_bytes.len(),
            deadline: Utc::now() + chrono::Duration::seconds(30),
        };
        assert_eq!(
            client.read_exact(artifact_request.clone()).await.unwrap(),
            artifact_bytes
        );
        assert_eq!(artifacts.calls.load(Ordering::Acquire), 1);
        {
            let translated = artifacts.requests.lock().unwrap();
            assert_eq!(translated.len(), 1);
            assert_eq!(translated[0], artifact_request);
            assert_eq!(translated[0].artifact, artifact);
        }

        let channel = mtls_channel(
            &endpoint,
            &fixture,
            &fixture.microvm_certificate_pem,
            &fixture.microvm_key_pem,
        )
        .await;
        let mut unauthorized = SandboxMicroVmBrokerServiceClient::new(channel);
        let rejected = unauthorized
            .revoke_micro_vm_grants(Request::new(encode(&request, limits).unwrap()))
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::PermissionDenied);
        assert_eq!(revoker.calls.load(Ordering::Acquire), 1);
        let rejected = unauthorized
            .read_exact_micro_vm_artifact(Request::new(encode(&artifact_request, limits).unwrap()))
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::PermissionDenied);
        assert_eq!(artifacts.calls.load(Ordering::Acquire), 1);

        shutdown_sender.send(()).unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn real_mtls_attestor_accepts_only_controller_and_binds_installed_identity() {
        let fixture = mtls_fixture();
        let limits =
            SandboxInternalRpcLimits::from_profile(&checked_in_hard_limit_profile()).unwrap();
        let attestor_identity_digest: Sha256Digest =
            format!("sha256:{}", "d".repeat(64)).parse().unwrap();
        let isolation = Arc::new(RecordingProcessIsolation {
            calls: AtomicUsize::new(0),
            registrations: AtomicUsize::new(0),
            registered_host_process_id: AtomicU32::new(0),
            attestor_identity_digest: attestor_identity_digest.clone(),
        });
        let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let address = incoming.local_addr().unwrap();
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
        let service = proto::sandbox_process_isolation_attestor_service_server::SandboxProcessIsolationAttestorServiceServer::new(
            SandboxProcessIsolationAttestorGrpcService::new(Arc::clone(&isolation), limits),
        )
        .max_encoding_message_size(limits.maximum_message_bytes())
        .max_decoding_message_size(limits.maximum_message_bytes());
        let service = tonic::service::interceptor::InterceptedService::new(
            service,
            SandboxControllerWorkloadIdentity,
        );
        let registration_service = proto::sandbox_executor_process_registration_service_server::SandboxExecutorProcessRegistrationServiceServer::new(
            SandboxExecutorProcessRegistrationGrpcService::new(Arc::clone(&isolation), limits),
        )
        .max_encoding_message_size(limits.maximum_message_bytes())
        .max_decoding_message_size(limits.maximum_message_bytes());
        let registration_service = tonic::service::interceptor::InterceptedService::new(
            registration_service,
            WasiExecutorNodeRegistrationIdentity,
        );
        let micro_vm_registration_service = proto::sandbox_micro_vm_executor_process_registration_service_server::SandboxMicroVmExecutorProcessRegistrationServiceServer::new(
            SandboxExecutorProcessRegistrationGrpcService::new(Arc::clone(&isolation), limits),
        )
        .max_encoding_message_size(limits.maximum_message_bytes())
        .max_decoding_message_size(limits.maximum_message_bytes());
        let micro_vm_registration_service = tonic::service::interceptor::InterceptedService::new(
            micro_vm_registration_service,
            MicroVmExecutorNodeRegistrationIdentity,
        );
        let tls = ServerTlsConfig::new()
            .identity(Identity::from_pem(
                &fixture.server_certificate_pem,
                &fixture.server_key_pem,
            ))
            .client_ca_root(Certificate::from_pem(&fixture.ca_pem));
        let server = tokio::spawn(async move {
            Server::builder()
                .tls_config(tls)
                .unwrap()
                .add_service(service)
                .add_service(registration_service)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_receiver.await;
                })
                .await
                .unwrap();
        });
        let endpoint = format!("https://localhost:{}", address.port());
        let registration_request = RegisterWasiExecutorProcessGeneration {
            worker_process_generation_id: ResourceId::from_uuid_v7(
                ResourceKind::WorkerProcessGeneration,
                uuid::Uuid::now_v7(),
            )
            .unwrap(),
            worker_manifest_digest: format!("sha256:{}", "6".repeat(64)).parse().unwrap(),
            isolation_backend_contract_digest: format!("sha256:{}", "7".repeat(64))
                .parse()
                .unwrap(),
        };
        let executor_channel = mtls_channel(
            &endpoint,
            &fixture,
            &fixture.wasi_certificate_pem,
            &fixture.wasi_key_pem,
        )
        .await;
        let tcp_registration = SandboxExecutorProcessRegistrationGrpcClient::new(
            executor_channel.clone(),
            limits,
            attestor_identity_digest.clone(),
        )
        .register(registration_request.clone())
        .await;
        assert_eq!(
            tcp_registration,
            Err(WasiExecutorProcessRegistrationError::Rejected)
        );
        assert_eq!(isolation.registrations.load(Ordering::Acquire), 0);

        let socket_directory = tempfile::tempdir_in("/tmp").unwrap();
        let socket_path = socket_directory.path().join("registration.sock");
        let listener = UnixListener::bind(&socket_path).unwrap();
        let registration_incoming = UnixListenerStream::new(listener);
        let (registration_shutdown_sender, registration_shutdown_receiver) = oneshot::channel();
        let registration_service = proto::sandbox_executor_process_registration_service_server::SandboxExecutorProcessRegistrationServiceServer::new(
            SandboxExecutorProcessRegistrationGrpcService::new(Arc::clone(&isolation), limits),
        )
        .max_encoding_message_size(limits.maximum_message_bytes())
        .max_decoding_message_size(limits.maximum_message_bytes());
        let registration_service = tonic::service::interceptor::InterceptedService::new(
            registration_service,
            WasiExecutorNodeRegistrationIdentity,
        );
        let registration_tls = ServerTlsConfig::new()
            .identity(Identity::from_pem(
                &fixture.server_certificate_pem,
                &fixture.server_key_pem,
            ))
            .client_ca_root(Certificate::from_pem(&fixture.ca_pem));
        let registration_server = tokio::spawn(async move {
            Server::builder()
                .tls_config(registration_tls)
                .unwrap()
                .add_service(registration_service)
                .add_service(micro_vm_registration_service)
                .serve_with_incoming_shutdown(registration_incoming, async {
                    let _ = registration_shutdown_receiver.await;
                })
                .await
                .unwrap();
        });
        let registration_channel = mtls_uds_channel(
            socket_path.clone(),
            &fixture,
            &fixture.wasi_certificate_pem,
            &fixture.wasi_key_pem,
        )
        .await;
        let registration = SandboxExecutorProcessRegistrationGrpcClient::new(
            registration_channel,
            limits,
            attestor_identity_digest.clone(),
        )
        .register(registration_request.clone())
        .await
        .unwrap();
        assert_eq!(isolation.registrations.load(Ordering::Acquire), 1);
        assert_eq!(
            isolation.registered_host_process_id.load(Ordering::Acquire),
            std::process::id()
        );

        let micro_vm_registration_channel = mtls_uds_channel(
            socket_path.clone(),
            &fixture,
            &fixture.microvm_certificate_pem,
            &fixture.microvm_key_pem,
        )
        .await;
        SandboxMicroVmExecutorProcessRegistrationGrpcClient::new(
            micro_vm_registration_channel,
            limits,
            attestor_identity_digest.clone(),
        )
        .register(registration_request.clone())
        .await
        .unwrap();
        assert_eq!(isolation.registrations.load(Ordering::Acquire), 2);

        let wasi_to_micro_vm_channel = mtls_uds_channel(
            socket_path,
            &fixture,
            &fixture.wasi_certificate_pem,
            &fixture.wasi_key_pem,
        )
        .await;
        let mut wasi_to_micro_vm =
            SandboxMicroVmExecutorProcessRegistrationServiceClient::new(wasi_to_micro_vm_channel);
        let rejected = wasi_to_micro_vm
            .register_micro_vm_executor_process_generation(Request::new(
                encode(&registration_request, limits).unwrap(),
            ))
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::PermissionDenied);
        assert_eq!(isolation.registrations.load(Ordering::Acquire), 2);

        let mut unauthorized_proof =
            SandboxProcessIsolationAttestorServiceClient::new(executor_channel);
        let rejected = unauthorized_proof
            .verify_wasi_executor_process_generation(Request::new(
                encode(
                    &VerifyWasiExecutorProcessGeneration {
                        worker_process_generation_id: registration_request
                            .worker_process_generation_id
                            .clone(),
                        worker_manifest_digest: registration_request.worker_manifest_digest.clone(),
                        isolation_backend_contract_digest: registration_request
                            .isolation_backend_contract_digest
                            .clone(),
                        executor_identity_digest: registration.executor_identity_digest.clone(),
                        attestor_route: registration.attestor_route.clone(),
                    },
                    limits,
                )
                .unwrap(),
            ))
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::PermissionDenied);

        let request = ProveSandboxProcessGenerationAbsent {
            tenant_id: ResourceId::from_uuid_v7(ResourceKind::Tenant, uuid::Uuid::now_v7())
                .unwrap(),
            sandbox_job_id: ResourceId::from_uuid_v7(ResourceKind::Job, uuid::Uuid::now_v7())
                .unwrap(),
            request_digest: format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
            previous_worker_process_generation_id: ResourceId::from_uuid_v7(
                ResourceKind::WorkerProcessGeneration,
                uuid::Uuid::now_v7(),
            )
            .unwrap(),
            executor_identity_digest: format!("sha256:{}", "b".repeat(64)).parse().unwrap(),
            attestor_route: attestor_route(),
        };

        let channel = mtls_channel(
            &endpoint,
            &fixture,
            &fixture.controller_certificate_pem,
            &fixture.controller_key_pem,
        )
        .await;
        let client = SandboxProcessIsolationAttestorGrpcClient::new(
            channel.clone(),
            limits,
            attestor_identity_digest,
        );
        client.prove_absent(request.clone()).await.unwrap();
        assert_eq!(isolation.calls.load(Ordering::Acquire), 1);

        let wrong_identity = SandboxProcessIsolationAttestorGrpcClient::new(
            channel.clone(),
            limits,
            format!("sha256:{}", "e".repeat(64)).parse().unwrap(),
        );
        assert_eq!(
            wrong_identity.prove_absent(request.clone()).await,
            Err(SandboxProcessGenerationIsolationError::Rejected)
        );
        assert_eq!(isolation.calls.load(Ordering::Acquire), 2);

        let mut unauthorized_registration =
            SandboxExecutorProcessRegistrationServiceClient::new(channel);
        let rejected = unauthorized_registration
            .register_wasi_executor_process_generation(Request::new(
                encode(&registration_request, limits).unwrap(),
            ))
            .await
            .unwrap_err();
        assert_eq!(rejected.code(), tonic::Code::PermissionDenied);
        assert_eq!(isolation.registrations.load(Ordering::Acquire), 2);

        let channel = mtls_channel(
            &endpoint,
            &fixture,
            &fixture.wasi_certificate_pem,
            &fixture.wasi_key_pem,
        )
        .await;
        let mut unauthorized = SandboxProcessIsolationAttestorServiceClient::new(channel);
        let error = unauthorized
            .prove_sandbox_process_generation_absent(Request::new(
                encode(&request, limits).unwrap(),
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        assert_eq!(isolation.calls.load(Ordering::Acquire), 2);

        registration_shutdown_sender.send(()).unwrap();
        registration_server.await.unwrap();
        shutdown_sender.send(()).unwrap();
        server.await.unwrap();
    }
}
