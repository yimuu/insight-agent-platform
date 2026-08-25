use super::{
    contract_failure, stable_code, CapabilityAdapterFailure, CapabilityAdapterRequest,
    CapabilityAdapterResponse, CapabilityBackendPort, CapabilityDispatchError,
    CapabilityTransportCancelOutcome, CapabilityTransportCancelRequest,
    CapabilityTransportRequestIdentity,
};
use async_trait::async_trait;
use insight_platform_contracts::{
    CanonicalHttpEndpoint, CapabilityBackendBinding, CapabilityBackendContract,
    CapabilityBackendKind, CapabilityBackendLimits, CapabilityIdempotencyKind, Effect,
    ExactSecretBindingRef, ExactVersionRef, HttpCapabilityContract, HttpCapabilityMethod,
    InstalledCapabilityCodecRef, Sha256Digest,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

pub const MAX_HTTP_ADAPTER_HEADERS: usize = 64;
pub const MAX_HTTP_ADAPTER_HEADER_NAME_BYTES: usize = 128;
pub const MAX_HTTP_ADAPTER_HEADER_VALUE_BYTES: usize = 4_096;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct InstalledHttpCodecDescriptor {
    pub codec_id: String,
    pub codec_version: String,
    pub module_digest: Sha256Digest,
    pub worker_protocol_version: u32,
    pub descriptor_digest: Sha256Digest,
    pub protocol_contract_digest: Sha256Digest,
    pub request_mapping_digest: Sha256Digest,
    pub response_mapping_digest: Sha256Digest,
    pub error_mapping_digest: Sha256Digest,
}

impl InstalledHttpCodecDescriptor {
    pub fn exact(codec: &InstalledCapabilityCodecRef, contract: &HttpCapabilityContract) -> Self {
        Self {
            codec_id: codec.codec_id.clone(),
            codec_version: codec.codec_version.clone(),
            module_digest: codec.module_digest.clone(),
            worker_protocol_version: codec.worker_protocol_version,
            descriptor_digest: codec.descriptor_digest.clone(),
            protocol_contract_digest: contract.protocol_contract_digest.clone(),
            request_mapping_digest: contract.request_mapping_digest.clone(),
            response_mapping_digest: contract.response_mapping_digest.clone(),
            error_mapping_digest: contract.error_mapping_digest.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SafeHttpHeader {
    pub name: String,
    pub value: String,
}

impl SafeHttpHeader {
    pub fn validate(&self) -> Result<(), CapabilityDispatchError> {
        if !valid_header_name(&self.name)
            || forbidden_adapter_header(&self.name)
            || self.value.len() > MAX_HTTP_ADAPTER_HEADER_VALUE_BYTES
            || self
                .value
                .bytes()
                .any(|byte| byte.is_ascii_control() && byte != b'\t')
        {
            return Err(CapabilityDispatchError::MalformedAdapterResponse);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedHttpCapabilityRequest {
    pub headers: Vec<SafeHttpHeader>,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpIdempotencyBinding {
    pub header_name: String,
    pub value_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpTransportRequest {
    pub identity: CapabilityTransportRequestIdentity,
    pub admission_digest: Sha256Digest,
    pub deadline: chrono::DateTime<chrono::Utc>,
    pub effect: Effect,
    pub idempotency_kind: CapabilityIdempotencyKind,
    pub backend_contract_digest: Sha256Digest,
    pub method: HttpCapabilityMethod,
    pub endpoint: CanonicalHttpEndpoint,
    pub endpoint_identity_digest: Sha256Digest,
    pub network_policy: ExactVersionRef,
    pub tls_policy: ExactVersionRef,
    pub trust_policy: ExactVersionRef,
    pub secret_bindings: Vec<ExactSecretBindingRef>,
    pub idempotency: Option<HttpIdempotencyBinding>,
    pub limits: CapabilityBackendLimits,
    pub headers: Vec<SafeHttpHeader>,
    pub body: Vec<u8>,
}

impl HttpTransportRequest {
    pub fn validate_at(
        &self,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), CapabilityDispatchError> {
        self.identity.validate()?;
        self.endpoint
            .validate()
            .map_err(|_| CapabilityDispatchError::BackendContractMismatch)?;
        if self.identity.backend_kind != CapabilityBackendKind::Http
            || self.deadline <= now
            || self.endpoint.canonical_digest().as_ref() != Ok(&self.endpoint_identity_digest)
            || self.limits.validate().is_err()
            || self.headers.len() > MAX_HTTP_ADAPTER_HEADERS
            || self.body.len()
                > usize::try_from(self.limits.maximum_request_bytes)
                    .map_err(|_| CapabilityDispatchError::InvalidRequest)?
        {
            return Err(CapabilityDispatchError::InvalidRequest);
        }
        validate_transport_dependencies(
            &self.network_policy,
            &self.tls_policy,
            &self.trust_policy,
            &self.secret_bindings,
        )?;
        let mut header_names = BTreeMap::new();
        for header in &self.headers {
            header.validate()?;
            if header_names
                .insert(header.name.to_ascii_lowercase(), ())
                .is_some()
            {
                return Err(CapabilityDispatchError::InvalidRequest);
            }
        }
        if let Some(idempotency) = &self.idempotency {
            if !valid_header_name(&idempotency.header_name)
                || forbidden_idempotency_header(&idempotency.header_name)
                || header_names.contains_key(&idempotency.header_name.to_ascii_lowercase())
            {
                return Err(CapabilityDispatchError::BackendContractMismatch);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpTransportResponse {
    pub status: u16,
    pub headers: Vec<SafeHttpHeader>,
    pub body: Vec<u8>,
    pub transport_evidence_digest: Sha256Digest,
}

impl HttpTransportResponse {
    pub fn validate(&self, limits: CapabilityBackendLimits) -> Result<(), CapabilityDispatchError> {
        if !(200..=599).contains(&self.status)
            || self.headers.len() > MAX_HTTP_ADAPTER_HEADERS
            || self.body.len()
                > usize::try_from(limits.maximum_response_bytes)
                    .map_err(|_| CapabilityDispatchError::MalformedAdapterResponse)?
        {
            return Err(CapabilityDispatchError::MalformedAdapterResponse);
        }
        let mut header_names = BTreeMap::new();
        for header in &self.headers {
            header.validate()?;
            if header_names
                .insert(header.name.to_ascii_lowercase(), ())
                .is_some()
            {
                return Err(CapabilityDispatchError::MalformedAdapterResponse);
            }
        }
        Ok(())
    }
}

#[async_trait]
pub trait HttpCapabilityCodec: Send + Sync {
    fn descriptor(&self) -> InstalledHttpCodecDescriptor;

    fn encode(
        &self,
        request: &CapabilityAdapterRequest,
    ) -> Result<EncodedHttpCapabilityRequest, CapabilityAdapterFailure>;

    fn decode(
        &self,
        request: &CapabilityAdapterRequest,
        response: HttpTransportResponse,
    ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure>;
}

#[async_trait]
pub trait HttpNetworkTransport: Send + Sync {
    async fn round_trip(
        &self,
        request: HttpTransportRequest,
    ) -> Result<HttpTransportResponse, CapabilityAdapterFailure>;

    async fn cancel(
        &self,
        request: CapabilityTransportCancelRequest,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure>;
}

#[derive(Default, Clone)]
pub struct InstalledHttpCodecRegistry {
    codecs: BTreeMap<InstalledHttpCodecDescriptor, Arc<dyn HttpCapabilityCodec>>,
}

impl InstalledHttpCodecRegistry {
    pub fn install(
        &mut self,
        codec: Arc<dyn HttpCapabilityCodec>,
    ) -> Result<(), CapabilityDispatchError> {
        let descriptor = codec.descriptor();
        match self.codecs.entry(descriptor) {
            Entry::Vacant(entry) => {
                entry.insert(codec);
                Ok(())
            }
            Entry::Occupied(_) => Err(CapabilityDispatchError::InvalidInstalledAdapter),
        }
    }

    fn resolve(
        &self,
        codec: &InstalledCapabilityCodecRef,
        contract: &HttpCapabilityContract,
    ) -> Result<&Arc<dyn HttpCapabilityCodec>, CapabilityDispatchError> {
        codec
            .validate_for(&CapabilityBackendContract::Http(contract.clone()))
            .map_err(|_| CapabilityDispatchError::BackendContractMismatch)?;
        self.codecs
            .get(&InstalledHttpCodecDescriptor::exact(codec, contract))
            .ok_or(CapabilityDispatchError::ProtocolCodecNotInstalled)
    }
}

pub struct HttpCapabilityAdapter {
    codecs: InstalledHttpCodecRegistry,
    transport: Arc<dyn HttpNetworkTransport>,
}

impl HttpCapabilityAdapter {
    pub fn new(
        codecs: InstalledHttpCodecRegistry,
        transport: Arc<dyn HttpNetworkTransport>,
    ) -> Self {
        Self { codecs, transport }
    }
}

#[async_trait]
impl CapabilityBackendPort for HttpCapabilityAdapter {
    fn kind(&self) -> CapabilityBackendKind {
        CapabilityBackendKind::Http
    }

    async fn invoke(
        &self,
        request: &CapabilityAdapterRequest,
    ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure> {
        let CapabilityBackendContract::Http(contract) =
            &request.execution.implementation.backend_contract
        else {
            return Err(contract_failure(
                CapabilityDispatchError::BackendContractMismatch,
            ));
        };
        let CapabilityBackendBinding::Http {
            codec,
            worker_manifest_digest,
            endpoint,
            endpoint_identity_digest,
            network_policy,
            tls_policy,
            trust_policy,
        } = &request.execution.deployment_closure.backend
        else {
            return Err(contract_failure(
                CapabilityDispatchError::BackendContractMismatch,
            ));
        };
        if worker_manifest_digest != &request.worker_manifest_digest {
            return Err(contract_failure(
                CapabilityDispatchError::BackendContractMismatch,
            ));
        }
        let codec = self
            .codecs
            .resolve(codec, contract)
            .map_err(contract_failure)?;
        let encoded = codec.encode(request)?;
        let transport_request = HttpTransportRequest {
            identity: CapabilityTransportRequestIdentity::from_adapter_request(
                request,
                CapabilityBackendKind::Http,
            ),
            admission_digest: request.admission_digest.clone(),
            deadline: request.deadline,
            effect: request.effect,
            idempotency_kind: request.idempotency,
            backend_contract_digest: request
                .execution
                .implementation
                .backend_contract_digest
                .clone(),
            method: contract.method,
            endpoint: endpoint.clone(),
            endpoint_identity_digest: endpoint_identity_digest.clone(),
            network_policy: network_policy.clone(),
            tls_policy: tls_policy.clone(),
            trust_policy: trust_policy.clone(),
            secret_bindings: request.execution.deployment_closure.secret_bindings.clone(),
            idempotency: contract.idempotency_header.as_ref().map(|header_name| {
                HttpIdempotencyBinding {
                    header_name: header_name.clone(),
                    value_digest: request.idempotency_key_digest.clone(),
                }
            }),
            limits: request.execution.implementation.backend_limits,
            headers: encoded.headers,
            body: encoded.body,
        };
        transport_request
            .validate_at(chrono::Utc::now())
            .map_err(contract_failure)?;
        let response = self.transport.round_trip(transport_request).await?;
        response
            .validate(request.execution.implementation.backend_limits)
            .map_err(contract_failure)?;
        codec.decode(request, response)
    }

    async fn cancel(
        &self,
        request: CapabilityTransportCancelRequest,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
        if request.identity.backend_kind != CapabilityBackendKind::Http {
            return Err(contract_failure(
                CapabilityDispatchError::InvalidCancelRequest,
            ));
        }
        self.transport.cancel(request).await
    }
}

fn valid_header_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_HTTP_ADAPTER_HEADER_NAME_BYTES
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        ..=b'\'' | b'*' | b'+' | b'-' | b'.' | b'^' | b'_' | b'`' | b'|' | b'~'
                )
        })
}

fn forbidden_adapter_header(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "authorization"
            | "connection"
            | "content-length"
            | "cookie"
            | "host"
            | "proxy-authorization"
            | "set-cookie"
            | "transfer-encoding"
    )
}

fn forbidden_idempotency_header(value: &str) -> bool {
    forbidden_adapter_header(value) || !stable_code(&value.to_ascii_lowercase())
}

fn validate_transport_dependencies(
    network_policy: &ExactVersionRef,
    tls_policy: &ExactVersionRef,
    trust_policy: &ExactVersionRef,
    secret_bindings: &[ExactSecretBindingRef],
) -> Result<(), CapabilityDispatchError> {
    let policies = [network_policy, tls_policy, trust_policy];
    let mut policy_ids = std::collections::BTreeSet::new();
    for policy in policies {
        policy
            .validate()
            .map_err(|_| CapabilityDispatchError::InvalidRequest)?;
        if policy.resource_kind != insight_platform_contracts::ResourceKind::PolicyRevision
            || !policy_ids.insert(policy.revision_id.clone())
        {
            return Err(CapabilityDispatchError::InvalidRequest);
        }
    }
    let mut prior = None;
    for binding in secret_bindings {
        binding
            .validate()
            .map_err(|_| CapabilityDispatchError::InvalidRequest)?;
        let key = (&binding.purpose, &binding.secret_binding_id);
        if prior.is_some_and(|value| value >= key) {
            return Err(CapabilityDispatchError::InvalidRequest);
        }
        prior = Some(key);
    }
    Ok(())
}
