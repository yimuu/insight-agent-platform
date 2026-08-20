use crate::ScopedArtifactGrant;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, ArtifactRef, DataClassification, ResourceId, ResourceKind, Sha256Digest,
};
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use std::{fmt, net::SocketAddr, str::FromStr};
use url::{Host, Url};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct NodeAttestorRoute(String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WasiExecutorProcessBinding {
    pub executor_identity_digest: Sha256Digest,
    pub attestor_route: NodeAttestorRoute,
}

impl<'de> Deserialize<'de> for NodeAttestorRoute {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(|_| D::Error::custom("invalid node attestor route"))
    }
}

impl NodeAttestorRoute {
    pub fn from_ip_port(
        address: std::net::IpAddr,
        port: u16,
    ) -> Result<Self, WasiExecutorProcessRegistrationError> {
        if port == 0 {
            return Err(WasiExecutorProcessRegistrationError::Rejected);
        }
        match address {
            std::net::IpAddr::V4(address) => format!("https://{address}:{port}").parse(),
            std::net::IpAddr::V6(address) => format!("https://[{address}]:{port}").parse(),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn socket_address(&self) -> SocketAddr {
        let route = Url::parse(&self.0).expect("validated node attestor route");
        SocketAddr::new(
            route
                .host()
                .and_then(|host| match host {
                    Host::Ipv4(address) => Some(address.into()),
                    Host::Ipv6(address) => Some(address.into()),
                    Host::Domain(_) => None,
                })
                .expect("validated node attestor IP address"),
            route.port().expect("validated node attestor port"),
        )
    }
}

impl fmt::Display for NodeAttestorRoute {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for NodeAttestorRoute {
    type Err = WasiExecutorProcessRegistrationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let route = Url::parse(value).map_err(|_| Self::Err::Rejected)?;
        let private_node_address = match route.host() {
            Some(Host::Ipv4(address)) => {
                let octets = address.octets();
                octets[0] == 10
                    || (octets[0] == 172 && (16..=31).contains(&octets[1]))
                    || (octets[0] == 192 && octets[1] == 168)
            }
            Some(Host::Ipv6(address)) => {
                let first = address.segments()[0];
                first & 0xfe00 == 0xfc00
            }
            _ => false,
        };
        if value.len() > 128
            || route.scheme() != "https"
            || !private_node_address
            || route.port().is_none()
            || !route.username().is_empty()
            || route.password().is_some()
            || route.query().is_some()
            || route.fragment().is_some()
            || !matches!(route.path(), "" | "/")
        {
            return Err(Self::Err::Rejected);
        }
        let normalized = match route.host() {
            Some(Host::Ipv4(address)) => format!("https://{address}:{}", route.port().unwrap()),
            Some(Host::Ipv6(address)) => format!("https://[{address}]:{}", route.port().unwrap()),
            _ => return Err(Self::Err::Rejected),
        };
        if value != normalized {
            return Err(Self::Err::Rejected);
        }
        Ok(Self(normalized))
    }
}
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WasiArtifactReadPurpose {
    RuntimeBundle,
    InputValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WasiArtifactReadRequest {
    pub tenant_id: ResourceId,
    pub sandbox_job_id: ResourceId,
    pub request_digest: Sha256Digest,
    pub worker_process_generation_id: ResourceId,
    pub lease_generation: u64,
    pub artifact: ArtifactRef,
    pub purpose: WasiArtifactReadPurpose,
    pub read_grant: Option<ScopedArtifactGrant>,
    pub maximum_bytes: usize,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WasiArtifactBrokerError {
    Unavailable,
    Denied,
    NotFound,
    Integrity,
    TooLarge,
}

#[async_trait]
pub trait WasiArtifactBroker: Send + Sync {
    async fn read_exact(
        &self,
        request: WasiArtifactReadRequest,
    ) -> Result<Vec<u8>, WasiArtifactBrokerError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WasiValueDirection {
    Input,
    Output,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WasiValueValidationRequest {
    pub tenant_id: ResourceId,
    pub sandbox_job_id: ResourceId,
    pub request_digest: Sha256Digest,
    pub worker_process_generation_id: ResourceId,
    pub lease_generation: u64,
    pub direction: WasiValueDirection,
    pub schema_digest: Sha256Digest,
    pub classification: DataClassification,
    pub value: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WasiValueValidationError {
    Invalid,
    Unavailable,
}

#[async_trait]
pub trait WasiValueValidator: Send + Sync {
    async fn validate(
        &self,
        request: WasiValueValidationRequest,
    ) -> Result<Sha256Digest, WasiValueValidationError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RevokeWasiSandboxGrants {
    pub tenant_id: ResourceId,
    pub sandbox_job_id: ResourceId,
    pub request_digest: Sha256Digest,
    pub worker_process_generation_id: ResourceId,
    pub attempt_no: u32,
    pub lease_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WasiGrantRevocationEvidence {
    pub evidence_digest: Sha256Digest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WasiGrantRevocationError {
    Unavailable,
    Rejected,
}

#[async_trait]
pub trait WasiGrantRevoker: Send + Sync {
    /// Repeated calls for one physical attempt must be idempotent and return equivalent evidence.
    async fn revoke_exact(
        &self,
        request: RevokeWasiSandboxGrants,
    ) -> Result<WasiGrantRevocationEvidence, WasiGrantRevocationError>;
}

/// Provider-side revocation command for one exact microVM physical attempt. The Executor
/// generation is the PostgreSQL lease owner; the Provider generation and sandbox identity bind
/// the independently deployed VMM process and jail that performed cleanup.
#[cfg(any())]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MicroVmSandboxWorkloadKind {
    CapabilityExecution,
    ManagedMcpSubscriptionSession,
}

#[cfg(any())]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RevokeMicroVmSandboxGrants {
    pub workload_kind: MicroVmSandboxWorkloadKind,
    pub tenant_id: ResourceId,
    pub sandbox_job_id: ResourceId,
    pub request_digest: Sha256Digest,
    pub executor_worker_process_generation_id: ResourceId,
    pub provider_process_generation_id: ResourceId,
    pub sandbox_identity_digest: Sha256Digest,
    pub attempt_no: u32,
    pub lease_generation: u64,
}

#[cfg(any())]
impl RevokeMicroVmSandboxGrants {
    pub fn validate(&self) -> Result<(), MicroVmGrantRevocationError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.sandbox_job_id.kind() != ResourceKind::Job
            || self.executor_worker_process_generation_id.kind()
                != ResourceKind::WorkerProcessGeneration
            || self.provider_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.attempt_no == 0
            || self.lease_generation == 0
        {
            return Err(MicroVmGrantRevocationError::Rejected);
        }
        Ok(())
    }
}

#[cfg(any())]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MicroVmGrantRevocationEvidence {
    pub evidence_digest: Sha256Digest,
}

#[cfg(any())]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MicroVmGrantRevocationError {
    Unavailable,
    Rejected,
}

#[cfg(any())]
#[async_trait]
pub trait MicroVmGrantRevoker: Send + Sync {
    /// Repeated calls for the exact physical attempt are idempotent. The authority must revalidate
    /// the current Job lease and release every active Artifact grant before returning evidence;
    /// Secret and Egress brokers reject future use once that same lease is no longer active.
    async fn revoke_exact(
        &self,
        request: RevokeMicroVmSandboxGrants,
    ) -> Result<MicroVmGrantRevocationEvidence, MicroVmGrantRevocationError>;
}

#[cfg(any())]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MicroVmArtifactReadPurpose {
    RuntimeBundle,
    InputValue,
}

#[cfg(any())]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MicroVmArtifactReadRequest {
    pub workload_kind: MicroVmSandboxWorkloadKind,
    pub tenant_id: ResourceId,
    pub sandbox_job_id: ResourceId,
    pub request_digest: Sha256Digest,
    pub executor_worker_process_generation_id: ResourceId,
    pub provider_process_generation_id: ResourceId,
    pub sandbox_identity_digest: Sha256Digest,
    pub lease_generation: u64,
    pub artifact: ArtifactRef,
    pub purpose: MicroVmArtifactReadPurpose,
    pub read_grant: Option<ScopedArtifactGrant>,
    pub maximum_bytes: usize,
    pub deadline: DateTime<Utc>,
}

#[cfg(any())]
impl MicroVmArtifactReadRequest {
    pub fn validate(&self) -> Result<(), MicroVmArtifactBrokerError> {
        let grant_shape_valid = match (self.workload_kind, self.purpose) {
            (
                MicroVmSandboxWorkloadKind::CapabilityExecution,
                MicroVmArtifactReadPurpose::RuntimeBundle,
            ) => self.read_grant.is_none(),
            (
                MicroVmSandboxWorkloadKind::CapabilityExecution,
                MicroVmArtifactReadPurpose::InputValue,
            )
            | (
                MicroVmSandboxWorkloadKind::ManagedMcpSubscriptionSession,
                MicroVmArtifactReadPurpose::RuntimeBundle,
            ) => self.read_grant.is_some(),
            (
                MicroVmSandboxWorkloadKind::ManagedMcpSubscriptionSession,
                MicroVmArtifactReadPurpose::InputValue,
            ) => false,
        };
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.sandbox_job_id.kind() != ResourceKind::Job
            || self.executor_worker_process_generation_id.kind()
                != ResourceKind::WorkerProcessGeneration
            || self.provider_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.lease_generation == 0
            || self.maximum_bytes == 0
            || u64::try_from(self.maximum_bytes)
                .ok()
                .is_none_or(|maximum| maximum < self.artifact.byte_length())
            || !grant_shape_valid
        {
            return Err(MicroVmArtifactBrokerError::Denied);
        }
        Ok(())
    }
}

#[cfg(any())]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MicroVmArtifactBrokerError {
    Unavailable,
    Denied,
    NotFound,
    Integrity,
    TooLarge,
}

#[cfg(any())]
#[async_trait]
pub trait MicroVmArtifactBroker: Send + Sync {
    /// Returns bytes for one exact published package or active input grant. Implementations may
    /// stream on the wire, but the Provider-facing result remains bounded by `maximum_bytes` and
    /// must be revalidated against the complete `ArtifactRef` before return.
    async fn read_exact(
        &self,
        request: MicroVmArtifactReadRequest,
    ) -> Result<Vec<u8>, MicroVmArtifactBrokerError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisterWasiExecutorProcessGeneration {
    pub worker_process_generation_id: ResourceId,
    pub worker_manifest_digest: Sha256Digest,
    pub isolation_backend_contract_digest: Sha256Digest,
}

/// Kernel-authenticated identity of the process connected to the node-local registration socket.
///
/// This value is deliberately not serializable. It must be constructed from Unix peer
/// credentials by the transport and must never be accepted from the registration envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasiExecutorRegistrationPeer {
    pub host_process_id: u32,
    pub host_user_id: u32,
    pub host_group_id: u32,
}

impl WasiExecutorRegistrationPeer {
    pub fn validate(&self) -> Result<(), WasiExecutorProcessRegistrationError> {
        if self.host_process_id == 0 {
            return Err(WasiExecutorProcessRegistrationError::Rejected);
        }
        Ok(())
    }
}

impl RegisterWasiExecutorProcessGeneration {
    pub fn validate(&self) -> Result<(), WasiExecutorProcessRegistrationError> {
        if self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration {
            return Err(WasiExecutorProcessRegistrationError::Rejected);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WasiExecutorProcessIdentityEvidence {
    pub schema_version: u32,
    pub worker_process_generation_id: ResourceId,
    pub worker_manifest_digest: Sha256Digest,
    pub isolation_backend_contract_digest: Sha256Digest,
    pub executor_instance_binding_digest: Sha256Digest,
    pub executor_identity_digest: Sha256Digest,
    pub attestor_identity_digest: Sha256Digest,
    pub attestor_route: NodeAttestorRoute,
    pub observed_at: DateTime<Utc>,
    pub evidence_digest: Sha256Digest,
}

impl WasiExecutorProcessIdentityEvidence {
    pub fn seal(mut self) -> Result<Self, WasiExecutorProcessRegistrationError> {
        self.executor_identity_digest = self.canonical_executor_identity_digest()?;
        self.evidence_digest = self.canonical_evidence_digest()?;
        Ok(self)
    }

    pub fn validate_for(
        &self,
        request: &RegisterWasiExecutorProcessGeneration,
        expected_attestor_identity_digest: &Sha256Digest,
        now: DateTime<Utc>,
    ) -> Result<(), WasiExecutorProcessRegistrationError> {
        request.validate()?;
        if self.schema_version != 1
            || self.worker_process_generation_id != request.worker_process_generation_id
            || self.worker_manifest_digest != request.worker_manifest_digest
            || self.isolation_backend_contract_digest != request.isolation_backend_contract_digest
            || &self.attestor_identity_digest != expected_attestor_identity_digest
            || self.observed_at > now
            || self.executor_identity_digest != self.canonical_executor_identity_digest()?
            || self.evidence_digest != self.canonical_evidence_digest()?
        {
            return Err(WasiExecutorProcessRegistrationError::Rejected);
        }
        Ok(())
    }

    fn canonical_executor_identity_digest(
        &self,
    ) -> Result<Sha256Digest, WasiExecutorProcessRegistrationError> {
        canonical_digest(&serde_json::json!({
            "attestor_identity_digest": self.attestor_identity_digest,
            "attestor_route": self.attestor_route,
            "executor_instance_binding_digest": self.executor_instance_binding_digest,
            "isolation_backend_contract_digest": self.isolation_backend_contract_digest,
            "schema_version": self.schema_version,
            "worker_manifest_digest": self.worker_manifest_digest,
            "worker_process_generation_id": self.worker_process_generation_id,
        }))
        .map_err(|_| WasiExecutorProcessRegistrationError::Rejected)?
        .parse()
        .map_err(|_| WasiExecutorProcessRegistrationError::Rejected)
    }

    fn canonical_evidence_digest(
        &self,
    ) -> Result<Sha256Digest, WasiExecutorProcessRegistrationError> {
        canonical_digest(&serde_json::json!({
            "attestor_identity_digest": self.attestor_identity_digest,
            "attestor_route": self.attestor_route,
            "executor_identity_digest": self.executor_identity_digest,
            "executor_instance_binding_digest": self.executor_instance_binding_digest,
            "isolation_backend_contract_digest": self.isolation_backend_contract_digest,
            "observed_at": self.observed_at,
            "schema_version": self.schema_version,
            "worker_manifest_digest": self.worker_manifest_digest,
            "worker_process_generation_id": self.worker_process_generation_id,
        }))
        .map_err(|_| WasiExecutorProcessRegistrationError::Rejected)?
        .parse()
        .map_err(|_| WasiExecutorProcessRegistrationError::Rejected)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifyWasiExecutorProcessGeneration {
    pub worker_process_generation_id: ResourceId,
    pub worker_manifest_digest: Sha256Digest,
    pub isolation_backend_contract_digest: Sha256Digest,
    pub executor_identity_digest: Sha256Digest,
    pub attestor_route: NodeAttestorRoute,
}

impl VerifyWasiExecutorProcessGeneration {
    pub fn validate(&self) -> Result<(), WasiExecutorProcessRegistrationError> {
        if self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration {
            return Err(WasiExecutorProcessRegistrationError::Rejected);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WasiExecutorProcessRegistrationError {
    Unavailable,
    Rejected,
}

#[async_trait]
pub trait WasiExecutorProcessRegistrar: Send + Sync {
    async fn register(
        &self,
        request: RegisterWasiExecutorProcessGeneration,
    ) -> Result<WasiExecutorProcessIdentityEvidence, WasiExecutorProcessRegistrationError>;
}

/// Node-side registration authority. Unlike [`WasiExecutorProcessRegistrar`], this port receives
/// kernel peer credentials out of band and is implemented only by the node/runtime attestor.
#[async_trait]
pub trait WasiExecutorProcessAttestationAuthority: Send + Sync {
    async fn register_observed(
        &self,
        request: RegisterWasiExecutorProcessGeneration,
        peer: WasiExecutorRegistrationPeer,
    ) -> Result<WasiExecutorProcessIdentityEvidence, WasiExecutorProcessRegistrationError>;
}

#[async_trait]
pub trait WasiExecutorProcessRegistrationVerifier: Send + Sync {
    async fn verify_registered(
        &self,
        request: VerifyWasiExecutorProcessGeneration,
    ) -> Result<WasiExecutorProcessIdentityEvidence, WasiExecutorProcessRegistrationError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProveSandboxProcessGenerationAbsent {
    pub tenant_id: ResourceId,
    pub sandbox_job_id: ResourceId,
    pub request_digest: Sha256Digest,
    pub previous_worker_process_generation_id: ResourceId,
    pub executor_identity_digest: Sha256Digest,
    pub attestor_route: NodeAttestorRoute,
}

impl ProveSandboxProcessGenerationAbsent {
    pub fn validate(&self) -> Result<(), SandboxProcessGenerationIsolationError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.sandbox_job_id.kind() != ResourceKind::Job
            || self.previous_worker_process_generation_id.kind()
                != ResourceKind::WorkerProcessGeneration
        {
            return Err(SandboxProcessGenerationIsolationError::Rejected);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxProcessGenerationIsolationDisposition {
    ProcessAbsent,
    NodeQuarantined,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxProcessGenerationAbsenceEvidence {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub sandbox_job_id: ResourceId,
    pub request_digest: Sha256Digest,
    pub previous_worker_process_generation_id: ResourceId,
    pub executor_identity_digest: Sha256Digest,
    pub attestor_identity_digest: Sha256Digest,
    pub attestor_route: NodeAttestorRoute,
    pub disposition: SandboxProcessGenerationIsolationDisposition,
    pub observed_at: DateTime<Utc>,
    pub evidence_digest: Sha256Digest,
}

impl SandboxProcessGenerationAbsenceEvidence {
    pub fn seal(mut self) -> Result<Self, SandboxProcessGenerationIsolationError> {
        self.evidence_digest = self
            .canonical_evidence_digest()
            .map_err(|_| SandboxProcessGenerationIsolationError::Rejected)?;
        Ok(self)
    }

    pub fn validate_for(
        &self,
        request: &ProveSandboxProcessGenerationAbsent,
        now: DateTime<Utc>,
    ) -> Result<(), SandboxProcessGenerationIsolationError> {
        request.validate()?;
        if self.schema_version != 1
            || self.tenant_id != request.tenant_id
            || self.sandbox_job_id != request.sandbox_job_id
            || self.request_digest != request.request_digest
            || self.previous_worker_process_generation_id
                != request.previous_worker_process_generation_id
            || self.executor_identity_digest != request.executor_identity_digest
            || self.attestor_route != request.attestor_route
            || self.observed_at > now
            || self.evidence_digest != self.canonical_evidence_digest()?
        {
            return Err(SandboxProcessGenerationIsolationError::Rejected);
        }
        Ok(())
    }

    fn canonical_evidence_digest(
        &self,
    ) -> Result<Sha256Digest, SandboxProcessGenerationIsolationError> {
        canonical_digest(&serde_json::json!({
            "attestor_identity_digest": self.attestor_identity_digest,
            "attestor_route": self.attestor_route,
            "disposition": self.disposition,
            "executor_identity_digest": self.executor_identity_digest,
            "observed_at": self.observed_at,
            "previous_worker_process_generation_id": self.previous_worker_process_generation_id,
            "request_digest": self.request_digest,
            "sandbox_job_id": self.sandbox_job_id,
            "schema_version": self.schema_version,
            "tenant_id": self.tenant_id,
        }))
        .map_err(|_| SandboxProcessGenerationIsolationError::Rejected)?
        .parse()
        .map_err(|_| SandboxProcessGenerationIsolationError::Rejected)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxProcessGenerationIsolationError {
    StillLive,
    Unavailable,
    Rejected,
}

#[async_trait]
pub trait SandboxProcessGenerationIsolation: Send + Sync {
    async fn prove_absent(
        &self,
        request: ProveSandboxProcessGenerationAbsent,
    ) -> Result<SandboxProcessGenerationAbsenceEvidence, SandboxProcessGenerationIsolationError>;
}
