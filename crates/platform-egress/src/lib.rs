//! Trusted, role-scoped outbound network boundary for Platform v1 execution workers.
//!
//! The broker owns endpoint selection, DNS pinning, SSRF/TLS/redirect enforcement and late
//! credential resolution. It deliberately owns no durable Run, Invocation, Job or Attempt state.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{stream, StreamExt};
use insight_platform_contracts::{
    canonical_digest, canonical_json, CanonicalHttpEndpoint, CapabilityEndpointScheme, DataRegion,
    ExactDeploymentRef, ExactSecretBindingRef, ExactVersionRef, ResourceId, ResourceKind,
    SecretPurpose, SecretResolutionPolicy, Sha256Digest,
};
use insight_platform_model_adapters::{
    ModelAdapterCancelOutcome, ModelAdapterCancelRequest, ModelAdapterFailure,
    ModelAdapterFailureClass, ModelProviderByteStream, ModelProviderEgressBroker,
    ModelProviderEgressResponse, ModelProviderRequestIdentity, ModelProviderWireProtocol,
    ModelProviderWireRequest,
};
#[cfg(any())]
use insight_platform_sandbox::{
    ManagedMcpSandboxSecretCommitOutcome, ManagedMcpSandboxSecretDeliveryAuthority,
    ManagedMcpSandboxSecretDeliveryError, ManagedMcpSandboxSecretDeliveryEvidence,
    ManagedMcpSandboxSecretDeliveryRequest, ManagedMcpSandboxSecretReservationOutcome,
};
use reqwest::header::{
    HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use url::Url;

mod capability_grpc;
mod capability_http;
mod mcp_oauth;
mod mcp_oauth_start;
mod mcp_streamable_http;
mod remote_context;

pub use capability_grpc::*;
pub use capability_http::*;
pub use mcp_oauth::*;
pub use mcp_oauth_start::*;
pub use mcp_streamable_http::*;
pub use remote_context::*;

pub const MAX_INSTALLED_MODEL_ENDPOINTS: usize = 1_024;
pub const MAX_EGRESS_IN_FLIGHT_HARD: usize = 4_096;
pub const MAX_DNS_ANSWERS_HARD: usize = 64;
pub const MAX_SECRET_MATERIAL_BYTES_HARD: usize = 16_384;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EgressCapacitySnapshot {
    pub maximum_in_flight: usize,
    pub available: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelProviderEgressLimits {
    pub maximum_in_flight: usize,
    pub maximum_dns_answers: usize,
    pub maximum_secret_material_bytes: usize,
}

impl ModelProviderEgressLimits {
    pub fn validate(self) -> Result<(), EgressConfigurationError> {
        if self.maximum_in_flight == 0
            || self.maximum_in_flight > MAX_EGRESS_IN_FLIGHT_HARD
            || self.maximum_dns_answers == 0
            || self.maximum_dns_answers > MAX_DNS_ANSWERS_HARD
            || self.maximum_secret_material_bytes == 0
            || self.maximum_secret_material_bytes > MAX_SECRET_MATERIAL_BYTES_HARD
        {
            return Err(EgressConfigurationError::InvalidLimits);
        }
        Ok(())
    }
}

impl Default for ModelProviderEgressLimits {
    fn default() -> Self {
        Self {
            maximum_in_flight: 128,
            maximum_dns_answers: 16,
            maximum_secret_material_bytes: 8_192,
        }
    }
}

/// Process-installed endpoint and policy closure. Callers submit only its exact identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledModelProviderEndpoint {
    pub schema_version: u32,
    pub protocol: ModelProviderWireProtocol,
    pub provider_deployment: ExactDeploymentRef,
    pub provider_revision: ExactVersionRef,
    pub endpoint: CanonicalHttpEndpoint,
    pub endpoint_identity_digest: Sha256Digest,
    pub credential_purpose: SecretPurpose,
    pub network_policy: ExactVersionRef,
    pub tls_policy: ExactVersionRef,
    pub trust_policy: ExactVersionRef,
    pub data_policy: ExactVersionRef,
    pub region: DataRegion,
    #[serde(default)]
    pub development_loopback: bool,
    #[serde(default)]
    pub development_anonymous: bool,
    #[serde(default)]
    pub trusted_root_pem: Option<String>,
}

impl InstalledModelProviderEndpoint {
    pub fn validate(&self) -> Result<(), EgressConfigurationError> {
        self.provider_deployment
            .validate()
            .map_err(|_| EgressConfigurationError::InvalidEndpoint)?;
        self.provider_revision
            .validate()
            .map_err(|_| EgressConfigurationError::InvalidEndpoint)?;
        self.endpoint
            .validate()
            .map_err(|_| EgressConfigurationError::InvalidEndpoint)?;
        if self.schema_version != 1
            || self.provider_deployment.resource_kind != ResourceKind::ModelProviderDeployment
            || self.provider_revision.resource_kind != ResourceKind::ModelProviderRevision
            || self.endpoint.scheme != CapabilityEndpointScheme::Https
            || self.endpoint.canonical_digest().as_ref() != Ok(&self.endpoint_identity_digest)
            || !valid_broker_base_path(&self.endpoint.base_path)
            || parse_endpoint_host(&self.endpoint.host).is_err()
            || self
                .trusted_root_pem
                .as_ref()
                .is_some_and(|pem| !valid_model_trust_roots(pem))
            || (self.development_loopback
                && (self.endpoint.host != "localhost" || self.trusted_root_pem.is_none()))
            || (self.development_anonymous && !self.development_loopback)
        {
            return Err(EgressConfigurationError::InvalidEndpoint);
        }
        let policies = [
            &self.network_policy,
            &self.tls_policy,
            &self.trust_policy,
            &self.data_policy,
        ];
        let mut ids = BTreeSet::new();
        for policy in policies {
            policy
                .validate()
                .map_err(|_| EgressConfigurationError::InvalidEndpoint)?;
            if policy.resource_kind != ResourceKind::PolicyRevision
                || !ids.insert(policy.revision_id.clone())
            {
                return Err(EgressConfigurationError::InvalidEndpoint);
            }
        }
        endpoint_url(self)?;
        Ok(())
    }

    fn matches(&self, request: &ModelProviderWireRequest) -> bool {
        self.protocol == request.protocol
            && self.provider_deployment == request.provider_deployment
            && self.provider_revision == request.provider_revision
            && self.endpoint_identity_digest == request.endpoint_identity_digest
            && self.network_policy == request.network_policy
            && self.tls_policy == request.tls_policy
            && self.trust_policy == request.trust_policy
            && self.data_policy == request.data_policy
            && self.region == request.region
            && request
                .secret_bindings
                .iter()
                .filter(|binding| binding.purpose == self.credential_purpose)
                .count()
                == 1
    }
}

fn valid_model_trust_roots(pem: &str) -> bool {
    pem.len() <= 65_536
        && reqwest::Certificate::from_pem_bundle(pem.as_bytes())
            .is_ok_and(|roots| !roots.is_empty())
}

#[derive(Debug, Clone)]
pub struct InstalledModelProviderEndpointCatalog {
    entries: BTreeMap<(ResourceId, Sha256Digest), InstalledModelProviderEndpoint>,
}

impl InstalledModelProviderEndpointCatalog {
    pub fn new(
        entries: Vec<InstalledModelProviderEndpoint>,
    ) -> Result<Self, EgressConfigurationError> {
        // An empty installed closure is a valid deny-all state. This lets deployments enable the
        // broker before a Model endpoint is admitted without inventing a permissive placeholder.
        if entries.len() > MAX_INSTALLED_MODEL_ENDPOINTS {
            return Err(EgressConfigurationError::InvalidEndpointCatalog);
        }
        let mut catalog = BTreeMap::new();
        for entry in entries {
            entry.validate()?;
            let key = (
                entry.provider_deployment.deployment_id.clone(),
                entry.provider_deployment.deployment_digest.clone(),
            );
            if catalog.insert(key, entry).is_some() {
                return Err(EgressConfigurationError::DuplicateEndpoint);
            }
        }
        Ok(Self { entries: catalog })
    }

    fn resolve(
        &self,
        request: &ModelProviderWireRequest,
    ) -> Result<InstalledModelProviderEndpoint, ModelAdapterFailure> {
        let key = (
            request.provider_deployment.deployment_id.clone(),
            request.provider_deployment.deployment_digest.clone(),
        );
        self.entries
            .get(&key)
            .filter(|entry| entry.matches(request))
            .cloned()
            .ok_or_else(|| rejected_before_dispatch("model_egress_endpoint_not_installed"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EgressConfigurationError {
    InvalidLimits,
    InvalidEndpointCatalog,
    InvalidEndpoint,
    DuplicateEndpoint,
    InvalidSecretMaterial,
    InvalidCryptographicKey,
}

impl fmt::Display for EgressConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidLimits => "egress limits are invalid",
            Self::InvalidEndpointCatalog => "egress endpoint catalog is invalid",
            Self::InvalidEndpoint => "egress endpoint is invalid",
            Self::DuplicateEndpoint => "egress endpoint identity is duplicated",
            Self::InvalidSecretMaterial => "resolved secret material is invalid",
            Self::InvalidCryptographicKey => "egress cryptographic key configuration is invalid",
        })
    }
}

impl Error for EgressConfigurationError {}

struct SecretMaterial(Vec<u8>);

impl SecretMaterial {
    fn new(mut value: Vec<u8>) -> Result<Self, EgressConfigurationError> {
        if value.is_empty() || value.len() > MAX_SECRET_MATERIAL_BYTES_HARD {
            value.fill(0);
            return Err(EgressConfigurationError::InvalidSecretMaterial);
        }
        Ok(Self(value))
    }

    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Drop for SecretMaterial {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

/// Resolver output contains exact non-secret evidence plus non-clone, zeroed-on-drop material.
pub struct ResolvedSecretMaterial {
    pub secret_binding_id: ResourceId,
    pub provider_id: ResourceId,
    pub purpose: SecretPurpose,
    pub binding_generation: u64,
    pub opaque_version_identity_digest: Sha256Digest,
    material: SecretMaterial,
}

impl ResolvedSecretMaterial {
    pub fn new(
        secret_binding_id: ResourceId,
        provider_id: ResourceId,
        purpose: SecretPurpose,
        binding_generation: u64,
        opaque_version_identity_digest: Sha256Digest,
        mut material: Vec<u8>,
    ) -> Result<Self, EgressConfigurationError> {
        if secret_binding_id.kind() != ResourceKind::SecretBinding
            || provider_id.kind() != ResourceKind::SecretProvider
            || binding_generation == 0
        {
            material.fill(0);
            return Err(EgressConfigurationError::InvalidSecretMaterial);
        }
        Ok(Self {
            secret_binding_id,
            provider_id,
            purpose,
            binding_generation,
            opaque_version_identity_digest,
            material: SecretMaterial::new(material)?,
        })
    }

    pub(crate) fn validate_for(
        &self,
        binding: &ExactSecretBindingRef,
        maximum_material_bytes: usize,
    ) -> bool {
        binding.validate().is_ok()
            && binding.provider_id == self.provider_id
            && binding.permits_resolved_generation(
                &self.secret_binding_id,
                &self.purpose,
                self.binding_generation,
            )
            && self.material.0.len() <= maximum_material_bytes
            && match &binding.resolution_policy {
                SecretResolutionPolicy::Pinned {
                    opaque_version_identity_digest,
                } => opaque_version_identity_digest == &self.opaque_version_identity_digest,
                SecretResolutionPolicy::FollowProviderRotation { .. } => true,
            }
    }
}

impl fmt::Debug for ResolvedSecretMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedSecretMaterial")
            .field("secret_binding_id", &self.secret_binding_id)
            .field("provider_id", &self.provider_id)
            .field("purpose", &self.purpose)
            .field("binding_generation", &self.binding_generation)
            .field(
                "opaque_version_identity_digest",
                &self.opaque_version_identity_digest,
            )
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretMaterialResolutionError {
    Unavailable,
    NotFound,
    Revoked,
    InvalidEvidence,
}

#[async_trait]
pub trait SecretMaterialResolver: Send + Sync {
    async fn resolve(
        &self,
        tenant_id: &ResourceId,
        binding: &ExactSecretBindingRef,
    ) -> Result<ResolvedSecretMaterial, SecretMaterialResolutionError>;
}

/// Plaintext returned only across the Egress-to-Provider mTLS boundary. It cannot be cloned or
/// formatted and is zeroed if the RPC adapter drops it before transfer.
#[cfg(any())]
mod deferred_managed_mcp_secret_delivery {
    use super::*;

    pub struct DeliveredManagedMcpSandboxSecret {
        pub authorization: insight_platform_sandbox::AuthorizedManagedMcpSandboxSecretDelivery,
        pub evidence: ManagedMcpSandboxSecretDeliveryEvidence,
        material: Vec<u8>,
    }

    impl DeliveredManagedMcpSandboxSecret {
        pub fn from_committed(
            request: &ManagedMcpSandboxSecretDeliveryRequest,
            authorization: insight_platform_sandbox::AuthorizedManagedMcpSandboxSecretDelivery,
            evidence: ManagedMcpSandboxSecretDeliveryEvidence,
            mut material: Vec<u8>,
        ) -> Result<Self, ManagedMcpSandboxSecretBrokerError> {
            if material.is_empty()
                || material.len() > MAX_SECRET_MATERIAL_BYTES_HARD
                || authorization.validate_for(request).is_err()
                || evidence.validate_for(request, &authorization).is_err()
            {
                material.fill(0);
                return Err(ManagedMcpSandboxSecretBrokerError::Denied);
            }
            Ok(Self {
                authorization,
                evidence,
                material,
            })
        }

        pub fn into_material(mut self) -> Vec<u8> {
            std::mem::take(&mut self.material)
        }
    }

    impl fmt::Debug for DeliveredManagedMcpSandboxSecret {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter
                .debug_struct("DeliveredManagedMcpSandboxSecret")
                .field("authorization", &self.authorization)
                .field("evidence", &self.evidence)
                .field("byte_length", &self.material.len())
                .finish_non_exhaustive()
        }
    }

    impl Drop for DeliveredManagedMcpSandboxSecret {
        fn drop(&mut self) {
            self.material.fill(0);
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ManagedMcpSandboxSecretBrokerError {
        Unavailable,
        Denied,
        OutcomeUncertain,
    }

    #[async_trait]
    pub trait ManagedMcpSandboxSecretBroker: Send + Sync {
        async fn deliver(
            &self,
            request: ManagedMcpSandboxSecretDeliveryRequest,
        ) -> Result<DeliveredManagedMcpSandboxSecret, ManagedMcpSandboxSecretBrokerError>;
    }

    /// Two-plane Secret delivery composition. The Controller reserves and revalidates an exact read;
    /// the existing Egress resolver alone sees KMS/Provider material. Only a fresh reserve followed by
    /// a fresh durable commit can release bytes to the microVM Provider.
    pub struct BrokeredManagedMcpSandboxSecretDelivery<A> {
        authority: Arc<A>,
        resolver: Arc<dyn SecretMaterialResolver>,
        maximum_material_bytes: usize,
    }

    impl<A> BrokeredManagedMcpSandboxSecretDelivery<A> {
        pub fn new(
            authority: Arc<A>,
            resolver: Arc<dyn SecretMaterialResolver>,
            maximum_material_bytes: usize,
        ) -> Result<Self, EgressConfigurationError> {
            if maximum_material_bytes == 0
                || maximum_material_bytes > MAX_SECRET_MATERIAL_BYTES_HARD
            {
                return Err(EgressConfigurationError::InvalidLimits);
            }
            Ok(Self {
                authority,
                resolver,
                maximum_material_bytes,
            })
        }
    }

    #[async_trait]
    impl<A> ManagedMcpSandboxSecretBroker for BrokeredManagedMcpSandboxSecretDelivery<A>
    where
        A: ManagedMcpSandboxSecretDeliveryAuthority,
    {
        async fn deliver(
            &self,
            request: ManagedMcpSandboxSecretDeliveryRequest,
        ) -> Result<DeliveredManagedMcpSandboxSecret, ManagedMcpSandboxSecretBrokerError> {
            request
                .validate_shape()
                .map_err(|_| ManagedMcpSandboxSecretBrokerError::Denied)?;
            if request.secret_grant.expires_at <= Utc::now() {
                return Err(ManagedMcpSandboxSecretBrokerError::Denied);
            }
            let authorization = match self
                .authority
                .reserve_managed_mcp_sandbox_secret_delivery(&request)
                .await
                .map_err(map_sandbox_secret_authority_error)?
            {
                ManagedMcpSandboxSecretReservationOutcome::Authorized(authorization) => {
                    *authorization
                }
                ManagedMcpSandboxSecretReservationOutcome::AlreadyReserved
                | ManagedMcpSandboxSecretReservationOutcome::AlreadyDelivered => {
                    return Err(ManagedMcpSandboxSecretBrokerError::OutcomeUncertain);
                }
            };
            authorization
                .validate_for(&request)
                .map_err(|_| ManagedMcpSandboxSecretBrokerError::Denied)?;
            let mut resolved = self
                .resolver
                .resolve(&authorization.tenant_id, &authorization.secret_binding)
                .await
                .map_err(map_sandbox_secret_resolution_error)?;
            if !resolved.validate_for(&authorization.secret_binding, self.maximum_material_bytes)
                || resolved.binding_generation != authorization.resolved_binding_generation
            {
                return Err(ManagedMcpSandboxSecretBrokerError::Denied);
            }
            let resolution_evidence_digest: Sha256Digest = canonical_digest(&serde_json::json!({
                "authorization_digest": authorization.authorization_digest,
                "binding_generation": resolved.binding_generation,
                "domain": "managed_mcp_sandbox_secret_resolution",
                "opaque_version_identity_digest": resolved.opaque_version_identity_digest,
                "provider_id": resolved.provider_id,
                "purpose": resolved.purpose,
                "schema_version": 1,
                "secret_binding_id": resolved.secret_binding_id,
            }))
            .map_err(|_| ManagedMcpSandboxSecretBrokerError::Denied)?
            .parse()
            .map_err(|_| ManagedMcpSandboxSecretBrokerError::Denied)?;
            let evidence = match self
                .authority
                .commit_managed_mcp_sandbox_secret_delivery(
                    &request,
                    &authorization,
                    &resolution_evidence_digest,
                )
                .await
                .map_err(map_sandbox_secret_authority_error)?
            {
                ManagedMcpSandboxSecretCommitOutcome::Delivered(evidence) => evidence,
                ManagedMcpSandboxSecretCommitOutcome::Replayed(_) => {
                    return Err(ManagedMcpSandboxSecretBrokerError::OutcomeUncertain);
                }
            };
            evidence
                .validate_for(&request, &authorization)
                .map_err(|_| ManagedMcpSandboxSecretBrokerError::Denied)?;
            DeliveredManagedMcpSandboxSecret::from_committed(
                &request,
                authorization,
                evidence,
                std::mem::take(&mut resolved.material.0),
            )
        }
    }

    fn map_sandbox_secret_authority_error(
        error: ManagedMcpSandboxSecretDeliveryError,
    ) -> ManagedMcpSandboxSecretBrokerError {
        match error {
            ManagedMcpSandboxSecretDeliveryError::Unavailable => {
                ManagedMcpSandboxSecretBrokerError::Unavailable
            }
            ManagedMcpSandboxSecretDeliveryError::Denied => {
                ManagedMcpSandboxSecretBrokerError::Denied
            }
            ManagedMcpSandboxSecretDeliveryError::OutcomeUncertain => {
                ManagedMcpSandboxSecretBrokerError::OutcomeUncertain
            }
        }
    }

    fn map_sandbox_secret_resolution_error(
        error: SecretMaterialResolutionError,
    ) -> ManagedMcpSandboxSecretBrokerError {
        match error {
            SecretMaterialResolutionError::Unavailable => {
                ManagedMcpSandboxSecretBrokerError::Unavailable
            }
            SecretMaterialResolutionError::NotFound
            | SecretMaterialResolutionError::Revoked
            | SecretMaterialResolutionError::InvalidEvidence => {
                ManagedMcpSandboxSecretBrokerError::Denied
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DnsResolutionError {
    Unavailable,
    NoAddresses,
    TooManyAddresses,
}

#[async_trait]
pub trait EgressDnsResolver: Send + Sync {
    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, DnsResolutionError>;
}

#[derive(Default)]
pub struct TokioEgressDnsResolver;

#[async_trait]
impl EgressDnsResolver for TokioEgressDnsResolver {
    async fn resolve(&self, host: &str, port: u16) -> Result<Vec<SocketAddr>, DnsResolutionError> {
        let mut resolved = tokio::net::lookup_host((host, port))
            .await
            .map_err(|_| DnsResolutionError::Unavailable)?;
        let mut addresses = Vec::new();
        for address in resolved.by_ref() {
            if addresses.len() == MAX_DNS_ANSWERS_HARD {
                return Err(DnsResolutionError::TooManyAddresses);
            }
            addresses.push(address);
        }
        if addresses.is_empty() {
            return Err(DnsResolutionError::NoAddresses);
        }
        Ok(addresses)
    }
}

/// Conservative public-Internet predicate used after every resolution and before pinning.
pub fn is_public_destination_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_public_ipv4(address),
        IpAddr::V6(address) => is_public_ipv6(address),
    }
}

fn is_public_ipv4(address: Ipv4Addr) -> bool {
    let [a, b, c, _] = address.octets();
    if a == 0
        || a == 10
        || a == 127
        || a >= 224
        || (a == 100 && (64..=127).contains(&b))
        || (a == 169 && b == 254)
        || (a == 172 && (16..=31).contains(&b))
        || (a == 192 && b == 168)
        || (a == 198 && matches!(b, 18 | 19))
    {
        return false;
    }
    !matches!(
        (a, b, c),
        (192, 0, _) | (192, 88, 99) | (198, 51, 100) | (203, 0, 113)
    )
}

fn is_public_ipv6(address: Ipv6Addr) -> bool {
    if let Some(mapped) = address.to_ipv4_mapped() {
        return is_public_ipv4(mapped);
    }
    let segments = address.segments();
    if segments[0] & 0xe000 != 0x2000 {
        return false;
    }
    // Documentation, benchmarking, ORCHID and 6to4 ranges are never Provider destinations.
    !(segments[0] == 0x2001
        && (segments[1] == 0x0db8
            || segments[1] == 0x0002
            || (0x0010..=0x003f).contains(&segments[1])))
        && segments[0] != 0x2002
}

enum ParsedEndpointHost {
    Name(String),
    Address(IpAddr),
}

fn parse_endpoint_host(value: &str) -> Result<ParsedEndpointHost, EgressConfigurationError> {
    if let Some(inner) = value
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
    {
        return inner
            .parse::<Ipv6Addr>()
            .map(|address| ParsedEndpointHost::Address(IpAddr::V6(address)))
            .map_err(|_| EgressConfigurationError::InvalidEndpoint);
    }
    if let Ok(address) = value.parse::<IpAddr>() {
        return Ok(ParsedEndpointHost::Address(address));
    }
    if value.len() > 253 || value.contains(':') {
        return Err(EgressConfigurationError::InvalidEndpoint);
    }
    for label in value.split('.') {
        if label.is_empty()
            || label.len() > 63
            || label.starts_with('-')
            || label.ends_with('-')
            || !label
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(EgressConfigurationError::InvalidEndpoint);
        }
    }
    Ok(ParsedEndpointHost::Name(value.to_owned()))
}

fn valid_broker_base_path(path: &str) -> bool {
    path.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.' | b'~')
    })
}

fn endpoint_url(entry: &InstalledModelProviderEndpoint) -> Result<Url, EgressConfigurationError> {
    let base = entry.endpoint.base_path.trim_end_matches('/');
    let path = if base.is_empty() {
        entry.protocol.endpoint_path().to_owned()
    } else {
        format!("{base}{}", entry.protocol.endpoint_path())
    };
    let raw = format!(
        "https://{}:{}{}",
        entry.endpoint.host, entry.endpoint.port, path
    );
    let url = Url::parse(&raw).map_err(|_| EgressConfigurationError::InvalidEndpoint)?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.port_or_known_default() != Some(entry.endpoint.port)
    {
        return Err(EgressConfigurationError::InvalidEndpoint);
    }
    Ok(url)
}

struct PinnedHttpRequest {
    url: Url,
    dns_host: String,
    addresses: Vec<SocketAddr>,
    headers: HeaderMap,
    body: Vec<u8>,
    connect_timeout: Duration,
    total_timeout: Duration,
    maximum_response_bytes: u64,
    deadline: DateTime<Utc>,
    cancellation: CancellationToken,
    trusted_root_pem: Option<String>,
}

struct PinnedHttpResponse {
    status_code: u16,
    content_type: String,
    body: ModelProviderByteStream,
}

#[async_trait]
trait PinnedModelProviderHttpTransport: Send + Sync {
    async fn open(
        &self,
        request: PinnedHttpRequest,
    ) -> Result<PinnedHttpResponse, ModelAdapterFailure>;
}

#[derive(Default)]
struct ReqwestPinnedModelProviderHttpTransport;

#[async_trait]
impl PinnedModelProviderHttpTransport for ReqwestPinnedModelProviderHttpTransport {
    async fn open(
        &self,
        request: PinnedHttpRequest,
    ) -> Result<PinnedHttpResponse, ModelAdapterFailure> {
        let mut client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .referer(false)
            .no_proxy()
            .https_only(true)
            .connect_timeout(request.connect_timeout)
            .timeout(request.total_timeout)
            .pool_max_idle_per_host(0)
            .resolve_to_addrs(&request.dns_host, &request.addresses);
        if let Some(pem) = &request.trusted_root_pem {
            for root in reqwest::Certificate::from_pem_bundle(pem.as_bytes())
                .map_err(|_| rejected_before_dispatch("model_egress_trust_root_rejected"))?
            {
                client = client.add_root_certificate(root);
            }
        }
        let client = client
            .build()
            .map_err(|_| rejected_before_dispatch("model_egress_client_build_failed"))?;
        let outbound = client
            .post(request.url)
            .headers(request.headers)
            .body(request.body)
            .build()
            .map_err(|_| rejected_before_dispatch("model_egress_request_build_failed"))?;
        let response = tokio::select! {
            biased;
            _ = request.cancellation.cancelled() => {
                return Err(permanent_failure("model_egress_cancelled", false));
            }
            response = client.execute(outbound) => response
                .map_err(|_| retryable_after_dispatch("model_egress_transport_failed", request.deadline))?,
        };
        if response.headers().get_all(CONTENT_TYPE).iter().count() > 1
            || response.headers().get_all(CONTENT_LENGTH).iter().count() > 1
            || response
                .headers()
                .get(CONTENT_ENCODING)
                .is_some_and(|value| value.as_bytes() != b"identity")
            || response
                .content_length()
                .is_some_and(|length| length > request.maximum_response_bytes)
        {
            return Err(permanent_failure(
                "model_egress_invalid_response_metadata",
                true,
            ));
        }
        let content_type = match response.headers().get(CONTENT_TYPE) {
            None => String::new(),
            Some(value) => value
                .to_str()
                .ok()
                .filter(|value| value.len() <= 128 && !value.chars().any(char::is_control))
                .map(str::to_owned)
                .ok_or_else(|| permanent_failure("model_egress_invalid_response_metadata", true))?,
        };
        let status_code = response.status().as_u16();
        let response_deadline = request.deadline;
        let body = response
            .bytes_stream()
            .map(move |item| {
                item.map(|bytes| bytes.to_vec()).map_err(|_| {
                    retryable_after_dispatch(
                        "model_egress_response_stream_failed",
                        response_deadline,
                    )
                })
            })
            .boxed();
        Ok(PinnedHttpResponse {
            status_code,
            content_type,
            body,
        })
    }
}

struct InFlightRegistration {
    identity: ModelProviderRequestIdentity,
    active: Arc<Mutex<BTreeMap<ModelProviderRequestIdentity, CancellationToken>>>,
    cancellation: CancellationToken,
    _permit: OwnedSemaphorePermit,
}

impl Drop for InFlightRegistration {
    fn drop(&mut self) {
        if let Ok(mut active) = self.active.lock() {
            active.remove(&self.identity);
        }
    }
}

/// Production HTTPS implementation of the Model Provider broker port.
pub struct ReqwestModelProviderEgressBroker {
    catalog: InstalledModelProviderEndpointCatalog,
    secrets: Arc<dyn SecretMaterialResolver>,
    dns: Arc<dyn EgressDnsResolver>,
    transport: Arc<dyn PinnedModelProviderHttpTransport>,
    limits: ModelProviderEgressLimits,
    permits: Arc<Semaphore>,
    active: Arc<Mutex<BTreeMap<ModelProviderRequestIdentity, CancellationToken>>>,
}

impl ReqwestModelProviderEgressBroker {
    pub fn new(
        catalog: InstalledModelProviderEndpointCatalog,
        secrets: Arc<dyn SecretMaterialResolver>,
        dns: Arc<dyn EgressDnsResolver>,
        limits: ModelProviderEgressLimits,
    ) -> Result<Self, EgressConfigurationError> {
        Self::with_transport(
            catalog,
            secrets,
            dns,
            Arc::new(ReqwestPinnedModelProviderHttpTransport),
            limits,
        )
    }

    fn with_transport(
        catalog: InstalledModelProviderEndpointCatalog,
        secrets: Arc<dyn SecretMaterialResolver>,
        dns: Arc<dyn EgressDnsResolver>,
        transport: Arc<dyn PinnedModelProviderHttpTransport>,
        limits: ModelProviderEgressLimits,
    ) -> Result<Self, EgressConfigurationError> {
        limits.validate()?;
        Ok(Self {
            catalog,
            secrets,
            dns,
            transport,
            limits,
            permits: Arc::new(Semaphore::new(limits.maximum_in_flight)),
            active: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    pub fn capacity_snapshot(&self) -> EgressCapacitySnapshot {
        EgressCapacitySnapshot {
            maximum_in_flight: self.limits.maximum_in_flight,
            available: self.permits.available_permits(),
        }
    }

    fn register(
        &self,
        identity: ModelProviderRequestIdentity,
        deadline: DateTime<Utc>,
    ) -> Result<InFlightRegistration, ModelAdapterFailure> {
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| retryable_before_dispatch("model_egress_capacity", deadline))?;
        let cancellation = CancellationToken::new();
        let mut active = self
            .active
            .lock()
            .map_err(|_| permanent_failure("model_egress_registry_unavailable", false))?;
        if active.contains_key(&identity) {
            return Err(rejected_before_dispatch("model_egress_duplicate_request"));
        }
        active.insert(identity.clone(), cancellation.clone());
        drop(active);
        Ok(InFlightRegistration {
            identity,
            active: self.active.clone(),
            cancellation,
            _permit: permit,
        })
    }

    async fn resolve_addresses(
        &self,
        entry: &InstalledModelProviderEndpoint,
        cancellation: &CancellationToken,
        deadline: DateTime<Utc>,
    ) -> Result<(String, Vec<SocketAddr>), ModelAdapterFailure> {
        let endpoint = &entry.endpoint;
        let host = parse_endpoint_host(&endpoint.host)
            .map_err(|_| rejected_before_dispatch("model_egress_invalid_endpoint"))?;
        let (dns_host, mut addresses) = match host {
            ParsedEndpointHost::Address(address) => (
                match address {
                    IpAddr::V4(address) => address.to_string(),
                    IpAddr::V6(address) => address.to_string(),
                },
                vec![SocketAddr::new(address, endpoint.port)],
            ),
            ParsedEndpointHost::Name(host) => {
                let resolved = tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => {
                        return Err(permanent_failure("model_egress_cancelled", false));
                    }
                    result = self.dns.resolve(&host, endpoint.port) => result,
                }
                .map_err(|failure| match failure {
                    DnsResolutionError::Unavailable => {
                        retryable_before_dispatch("model_egress_dns_unavailable", deadline)
                    }
                    DnsResolutionError::NoAddresses | DnsResolutionError::TooManyAddresses => {
                        rejected_before_dispatch("model_egress_dns_rejected")
                    }
                })?;
                (host, resolved)
            }
        };
        addresses.sort_unstable();
        addresses.dedup();
        if addresses.is_empty()
            || addresses.len() > self.limits.maximum_dns_answers
            || addresses.iter().any(|address| {
                address.port() != endpoint.port
                    || !(is_public_destination_ip(address.ip())
                        || entry.development_loopback && address.ip().is_loopback())
            })
        {
            return Err(rejected_before_dispatch("model_egress_destination_denied"));
        }
        Ok((dns_host, addresses))
    }

    async fn resolve_credential(
        &self,
        request: &ModelProviderWireRequest,
        entry: &InstalledModelProviderEndpoint,
        cancellation: &CancellationToken,
    ) -> Result<ResolvedSecretMaterial, ModelAdapterFailure> {
        let binding = request
            .secret_bindings
            .iter()
            .find(|binding| binding.purpose == entry.credential_purpose)
            .ok_or_else(|| rejected_before_dispatch("model_egress_secret_binding_missing"))?;
        let resolved = tokio::select! {
            biased;
            _ = cancellation.cancelled() => {
                return Err(permanent_failure("model_egress_cancelled", false));
            }
            result = self.secrets.resolve(&request.tenant_id, binding) => result,
        }
        .map_err(|failure| match failure {
            SecretMaterialResolutionError::Unavailable => {
                retryable_before_dispatch("model_egress_secret_unavailable", request.deadline)
            }
            SecretMaterialResolutionError::NotFound
            | SecretMaterialResolutionError::Revoked
            | SecretMaterialResolutionError::InvalidEvidence => {
                rejected_before_dispatch("model_egress_secret_rejected")
            }
        })?;
        if !resolved.validate_for(binding, self.limits.maximum_secret_material_bytes) {
            return Err(rejected_before_dispatch("model_egress_secret_rejected"));
        }
        Ok(resolved)
    }
}

#[async_trait]
impl ModelProviderEgressBroker for ReqwestModelProviderEgressBroker {
    async fn open(
        &self,
        request: ModelProviderWireRequest,
    ) -> Result<ModelProviderEgressResponse, ModelAdapterFailure> {
        request.validate_at(Utc::now())?;
        let entry = self.catalog.resolve(&request)?;
        let registration = self.register(request.identity(), request.deadline)?;
        let (dns_host, addresses) = self
            .resolve_addresses(&entry, &registration.cancellation, request.deadline)
            .await?;
        let credential = if entry.development_anonymous {
            None
        } else {
            Some(
                self.resolve_credential(&request, &entry, &registration.cancellation)
                    .await?,
            )
        };
        let headers = provider_headers(request.protocol, credential.as_ref())?;
        let body = canonical_json(&request.request_body)
            .map_err(|_| rejected_before_dispatch("model_egress_request_not_canonical"))?;
        if body.len() > request.maximum_request_bytes as usize
            || canonical_digest(&request.request_body).ok().as_deref()
                != Some(request.request_body_digest.as_str())
        {
            return Err(rejected_before_dispatch(
                "model_egress_request_digest_mismatch",
            ));
        }
        let now = Utc::now();
        let remaining = (request.deadline - now)
            .to_std()
            .map_err(|_| rejected_before_dispatch("model_egress_deadline_elapsed"))?;
        let contract_total = Duration::from_millis(request.total_timeout_milliseconds);
        let total_timeout = remaining.min(contract_total);
        let connect_timeout =
            Duration::from_millis(request.connect_timeout_milliseconds).min(total_timeout);
        if connect_timeout.is_zero() || total_timeout.is_zero() {
            return Err(rejected_before_dispatch("model_egress_deadline_elapsed"));
        }
        let outbound = PinnedHttpRequest {
            url: endpoint_url(&entry)
                .map_err(|_| rejected_before_dispatch("model_egress_invalid_endpoint"))?,
            dns_host,
            addresses,
            headers,
            body,
            connect_timeout,
            total_timeout,
            maximum_response_bytes: u64::from(request.maximum_response_bytes),
            deadline: request.deadline,
            cancellation: registration.cancellation.clone(),
            trusted_root_pem: entry.trusted_root_pem.clone(),
        };
        let response = self.transport.open(outbound).await?;
        let body = guarded_response_body(
            response.body,
            u64::from(request.maximum_response_bytes),
            registration,
        );
        Ok(ModelProviderEgressResponse {
            status_code: response.status_code,
            content_type: response.content_type,
            body,
        })
    }

    async fn cancel(
        &self,
        protocol: ModelProviderWireProtocol,
        request: ModelAdapterCancelRequest,
    ) -> Result<ModelAdapterCancelOutcome, ModelAdapterFailure> {
        request
            .validate_shape_at(Utc::now())
            .map_err(|_| rejected_before_dispatch("model_egress_invalid_cancel"))?;
        let identity = request.identity(protocol);
        let cancellation = self
            .active
            .lock()
            .map_err(|_| permanent_failure("model_egress_registry_unavailable", false))?
            .get(&identity)
            .cloned();
        let Some(cancellation) = cancellation else {
            return Ok(ModelAdapterCancelOutcome::AlreadyTerminal);
        };
        cancellation.cancel();
        Ok(ModelAdapterCancelOutcome::Accepted)
    }
}

fn provider_headers(
    protocol: ModelProviderWireProtocol,
    credential: Option<&ResolvedSecretMaterial>,
) -> Result<HeaderMap, ModelAdapterFailure> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
    let Some(credential) = credential else {
        return Ok(headers);
    };
    match protocol {
        ModelProviderWireProtocol::OpenAiResponses => {
            let mut bearer = Vec::with_capacity(7 + credential.material.as_bytes().len());
            bearer.extend_from_slice(b"Bearer ");
            bearer.extend_from_slice(credential.material.as_bytes());
            let parsed = HeaderValue::from_bytes(&bearer);
            bearer.fill(0);
            let mut value =
                parsed.map_err(|_| rejected_before_dispatch("model_egress_secret_rejected"))?;
            value.set_sensitive(true);
            headers.insert(AUTHORIZATION, value);
        }
        ModelProviderWireProtocol::AnthropicMessages => {
            let mut value = HeaderValue::from_bytes(credential.material.as_bytes())
                .map_err(|_| rejected_before_dispatch("model_egress_secret_rejected"))?;
            value.set_sensitive(true);
            headers.insert("x-api-key", value);
            headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        }
    }
    Ok(headers)
}

fn guarded_response_body(
    upstream: ModelProviderByteStream,
    maximum_response_bytes: u64,
    registration: InFlightRegistration,
) -> ModelProviderByteStream {
    struct State {
        upstream: ModelProviderByteStream,
        observed_bytes: u64,
        maximum_response_bytes: u64,
        registration: Option<InFlightRegistration>,
        done: bool,
    }

    stream::unfold(
        State {
            upstream,
            observed_bytes: 0,
            maximum_response_bytes,
            registration: Some(registration),
            done: false,
        },
        |mut state| async move {
            if state.done {
                return None;
            }
            let cancellation = state
                .registration
                .as_ref()
                .expect("active response retains its registration")
                .cancellation
                .clone();
            let item = tokio::select! {
                biased;
                _ = cancellation.cancelled() => {
                    state.done = true;
                    Err(permanent_failure("model_egress_cancelled", true))
                }
                item = state.upstream.next() => match item {
                    Some(item) => item,
                    None => {
                        state.registration.take();
                        return None;
                    }
                },
            };
            match item {
                Ok(bytes) => {
                    state.observed_bytes =
                        match state.observed_bytes.checked_add(bytes.len() as u64) {
                            Some(total) if total <= state.maximum_response_bytes => total,
                            _ => {
                                state.done = true;
                                state.registration.take();
                                return Some((
                                    Err(permanent_failure("model_egress_response_too_large", true)),
                                    state,
                                ));
                            }
                        };
                    Some((Ok(bytes), state))
                }
                Err(failure) => {
                    state.done = true;
                    state.registration.take();
                    Some((Err(failure), state))
                }
            }
        },
    )
    .boxed()
}

fn rejected_before_dispatch(code: &str) -> ModelAdapterFailure {
    failure(
        ModelAdapterFailureClass::RejectedBeforeDispatch,
        code,
        false,
        None,
    )
}

fn retryable_before_dispatch(code: &str, deadline: DateTime<Utc>) -> ModelAdapterFailure {
    let retry_at = Utc::now() + chrono::Duration::milliseconds(250);
    if retry_at >= deadline {
        return permanent_failure(code, false);
    }
    failure(
        ModelAdapterFailureClass::RetryableBeforeDispatch,
        code,
        false,
        Some(retry_at),
    )
}

fn retryable_after_dispatch(code: &str, deadline: DateTime<Utc>) -> ModelAdapterFailure {
    let retry_at = Utc::now() + chrono::Duration::milliseconds(250);
    if retry_at >= deadline {
        return permanent_failure(code, true);
    }
    failure(
        ModelAdapterFailureClass::RetryableAfterDispatch,
        code,
        true,
        Some(retry_at),
    )
}

fn permanent_failure(code: &str, request_sent: bool) -> ModelAdapterFailure {
    failure(
        ModelAdapterFailureClass::Permanent,
        code,
        request_sent,
        None,
    )
}

fn failure(
    class: ModelAdapterFailureClass,
    code: &str,
    request_sent: bool,
    retry_at: Option<DateTime<Utc>>,
) -> ModelAdapterFailure {
    let evidence_digest: Sha256Digest = canonical_digest(&serde_json::json!({
        "domain": "model_egress",
        "safe_code": code,
        "schema_version": 1,
    }))
    .expect("static egress evidence is canonical")
    .parse()
    .expect("canonical digest is SHA-256");
    ModelAdapterFailure {
        class,
        safe_code: code.to_owned(),
        safe_message: "Model Provider egress contract failed".to_owned(),
        evidence_digest,
        request_sent,
        retry_at,
    }
}

#[cfg(test)]
mod tests;
