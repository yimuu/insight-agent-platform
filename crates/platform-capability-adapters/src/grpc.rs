use super::{
    contract_failure, CapabilityAdapterFailure, CapabilityAdapterRequest,
    CapabilityAdapterResponse, CapabilityBackendPort, CapabilityDispatchError,
    CapabilityTransportCancelOutcome, CapabilityTransportCancelRequest,
    CapabilityTransportRequestIdentity,
};
use async_trait::async_trait;
use insight_platform_contracts::{
    CanonicalHttpEndpoint, CapabilityBackendBinding, CapabilityBackendContract,
    CapabilityBackendKind, CapabilityBackendLimits, CapabilityIdempotencyKind, Effect,
    ExactSecretBindingRef, ExactVersionRef, GrpcCapabilityContract, InstalledCapabilityCodecRef,
    Sha256Digest,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

pub const MAX_GRPC_ADAPTER_METADATA: usize = 64;
pub const MAX_GRPC_METADATA_KEY_BYTES: usize = 128;
pub const MAX_GRPC_METADATA_VALUE_BYTES: usize = 4_096;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct InstalledGrpcCodecDescriptor {
    pub codec_id: String,
    pub codec_version: String,
    pub module_digest: Sha256Digest,
    pub worker_protocol_version: u32,
    pub descriptor_digest: Sha256Digest,
    pub protobuf_contract_digest: Sha256Digest,
    pub service_name: String,
    pub method_name: String,
    pub request_mapping_digest: Sha256Digest,
    pub response_mapping_digest: Sha256Digest,
    pub error_mapping_digest: Sha256Digest,
}

impl InstalledGrpcCodecDescriptor {
    pub fn exact(codec: &InstalledCapabilityCodecRef, contract: &GrpcCapabilityContract) -> Self {
        Self {
            codec_id: codec.codec_id.clone(),
            codec_version: codec.codec_version.clone(),
            module_digest: codec.module_digest.clone(),
            worker_protocol_version: codec.worker_protocol_version,
            descriptor_digest: codec.descriptor_digest.clone(),
            protobuf_contract_digest: contract.protobuf_contract_digest.clone(),
            service_name: contract.service_name.clone(),
            method_name: contract.method_name.clone(),
            request_mapping_digest: contract.request_mapping_digest.clone(),
            response_mapping_digest: contract.response_mapping_digest.clone(),
            error_mapping_digest: contract.error_mapping_digest.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SafeGrpcMetadata {
    pub key: String,
    pub value: String,
}

impl SafeGrpcMetadata {
    pub fn validate(&self) -> Result<(), CapabilityDispatchError> {
        if !valid_metadata_key(&self.key)
            || forbidden_metadata_key(&self.key)
            || self.value.len() > MAX_GRPC_METADATA_VALUE_BYTES
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
pub struct EncodedGrpcCapabilityRequest {
    pub metadata: Vec<SafeGrpcMetadata>,
    pub message: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrpcIdempotencyBinding {
    pub metadata_key: String,
    pub value_digest: Sha256Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrpcTransportRequest {
    pub identity: CapabilityTransportRequestIdentity,
    pub admission_digest: Sha256Digest,
    pub deadline: chrono::DateTime<chrono::Utc>,
    pub effect: Effect,
    pub idempotency_kind: CapabilityIdempotencyKind,
    pub backend_contract_digest: Sha256Digest,
    pub endpoint: CanonicalHttpEndpoint,
    pub endpoint_identity_digest: Sha256Digest,
    pub service_name: String,
    pub method_name: String,
    pub network_policy: ExactVersionRef,
    pub tls_policy: ExactVersionRef,
    pub trust_policy: ExactVersionRef,
    pub secret_bindings: Vec<ExactSecretBindingRef>,
    pub idempotency: Option<GrpcIdempotencyBinding>,
    pub limits: CapabilityBackendLimits,
    pub metadata: Vec<SafeGrpcMetadata>,
    pub message: Vec<u8>,
}

impl GrpcTransportRequest {
    pub fn validate_at(
        &self,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), CapabilityDispatchError> {
        self.identity.validate()?;
        self.endpoint
            .validate()
            .map_err(|_| CapabilityDispatchError::BackendContractMismatch)?;
        if self.identity.backend_kind != CapabilityBackendKind::Grpc
            || self.deadline <= now
            || self.endpoint.canonical_digest().as_ref() != Ok(&self.endpoint_identity_digest)
            || self.service_name.is_empty()
            || self.method_name.is_empty()
            || self.metadata.len() > MAX_GRPC_ADAPTER_METADATA
            || self.message.len()
                > usize::try_from(self.limits.maximum_request_bytes)
                    .map_err(|_| CapabilityDispatchError::InvalidRequest)?
            || self.limits.validate().is_err()
        {
            return Err(CapabilityDispatchError::InvalidRequest);
        }
        validate_transport_dependencies(
            &self.network_policy,
            &self.tls_policy,
            &self.trust_policy,
            &self.secret_bindings,
        )?;
        let mut metadata_keys = BTreeMap::new();
        for metadata in &self.metadata {
            metadata.validate()?;
            if metadata_keys.insert(metadata.key.clone(), ()).is_some() {
                return Err(CapabilityDispatchError::InvalidRequest);
            }
        }
        if let Some(idempotency) = &self.idempotency {
            if !valid_metadata_key(&idempotency.metadata_key)
                || forbidden_metadata_key(&idempotency.metadata_key)
                || metadata_keys.contains_key(&idempotency.metadata_key)
            {
                return Err(CapabilityDispatchError::BackendContractMismatch);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrpcTransportResponse {
    pub status_code: u16,
    pub trailing_metadata: Vec<SafeGrpcMetadata>,
    pub message: Vec<u8>,
    pub transport_evidence_digest: Sha256Digest,
}

impl GrpcTransportResponse {
    pub fn validate(&self, limits: CapabilityBackendLimits) -> Result<(), CapabilityDispatchError> {
        if self.status_code > 16
            || self.trailing_metadata.len() > MAX_GRPC_ADAPTER_METADATA
            || self.message.len()
                > usize::try_from(limits.maximum_response_bytes)
                    .map_err(|_| CapabilityDispatchError::MalformedAdapterResponse)?
        {
            return Err(CapabilityDispatchError::MalformedAdapterResponse);
        }
        let mut metadata_keys = BTreeMap::new();
        for metadata in &self.trailing_metadata {
            metadata.validate()?;
            if metadata_keys.insert(metadata.key.clone(), ()).is_some() {
                return Err(CapabilityDispatchError::MalformedAdapterResponse);
            }
        }
        Ok(())
    }
}

#[async_trait]
pub trait GrpcCapabilityCodec: Send + Sync {
    fn descriptor(&self) -> InstalledGrpcCodecDescriptor;

    fn encode(
        &self,
        request: &CapabilityAdapterRequest,
    ) -> Result<EncodedGrpcCapabilityRequest, CapabilityAdapterFailure>;

    fn decode(
        &self,
        request: &CapabilityAdapterRequest,
        response: GrpcTransportResponse,
    ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure>;
}

#[async_trait]
pub trait GrpcNetworkTransport: Send + Sync {
    async fn unary(
        &self,
        request: GrpcTransportRequest,
    ) -> Result<GrpcTransportResponse, CapabilityAdapterFailure>;

    async fn cancel(
        &self,
        request: CapabilityTransportCancelRequest,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure>;
}

#[derive(Default, Clone)]
pub struct InstalledGrpcCodecRegistry {
    codecs: BTreeMap<InstalledGrpcCodecDescriptor, Arc<dyn GrpcCapabilityCodec>>,
}

impl InstalledGrpcCodecRegistry {
    pub fn install(
        &mut self,
        codec: Arc<dyn GrpcCapabilityCodec>,
    ) -> Result<(), CapabilityDispatchError> {
        match self.codecs.entry(codec.descriptor()) {
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
        contract: &GrpcCapabilityContract,
    ) -> Result<&Arc<dyn GrpcCapabilityCodec>, CapabilityDispatchError> {
        codec
            .validate_for(&CapabilityBackendContract::Grpc(contract.clone()))
            .map_err(|_| CapabilityDispatchError::BackendContractMismatch)?;
        self.codecs
            .get(&InstalledGrpcCodecDescriptor::exact(codec, contract))
            .ok_or(CapabilityDispatchError::ProtocolCodecNotInstalled)
    }
}

pub struct GrpcCapabilityAdapter {
    codecs: InstalledGrpcCodecRegistry,
    transport: Arc<dyn GrpcNetworkTransport>,
}

impl GrpcCapabilityAdapter {
    pub fn new(
        codecs: InstalledGrpcCodecRegistry,
        transport: Arc<dyn GrpcNetworkTransport>,
    ) -> Self {
        Self { codecs, transport }
    }
}

#[async_trait]
impl CapabilityBackendPort for GrpcCapabilityAdapter {
    fn kind(&self) -> CapabilityBackendKind {
        CapabilityBackendKind::Grpc
    }

    async fn invoke(
        &self,
        request: &CapabilityAdapterRequest,
    ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure> {
        let CapabilityBackendContract::Grpc(contract) =
            &request.execution.implementation.backend_contract
        else {
            return Err(contract_failure(
                CapabilityDispatchError::BackendContractMismatch,
            ));
        };
        let CapabilityBackendBinding::Grpc {
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
        let transport_request = GrpcTransportRequest {
            identity: CapabilityTransportRequestIdentity::from_adapter_request(
                request,
                CapabilityBackendKind::Grpc,
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
            endpoint: endpoint.clone(),
            endpoint_identity_digest: endpoint_identity_digest.clone(),
            service_name: contract.service_name.clone(),
            method_name: contract.method_name.clone(),
            network_policy: network_policy.clone(),
            tls_policy: tls_policy.clone(),
            trust_policy: trust_policy.clone(),
            secret_bindings: request.execution.deployment_closure.secret_bindings.clone(),
            idempotency: contract
                .idempotency_metadata_key
                .as_ref()
                .map(|metadata_key| GrpcIdempotencyBinding {
                    metadata_key: metadata_key.clone(),
                    value_digest: request.idempotency_key_digest.clone(),
                }),
            limits: request.execution.implementation.backend_limits,
            metadata: encoded.metadata,
            message: encoded.message,
        };
        transport_request
            .validate_at(chrono::Utc::now())
            .map_err(contract_failure)?;
        let response = self.transport.unary(transport_request).await?;
        response
            .validate(request.execution.implementation.backend_limits)
            .map_err(contract_failure)?;
        codec.decode(request, response)
    }

    async fn cancel(
        &self,
        request: CapabilityTransportCancelRequest,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
        if request.identity.backend_kind != CapabilityBackendKind::Grpc {
            return Err(contract_failure(
                CapabilityDispatchError::InvalidCancelRequest,
            ));
        }
        self.transport.cancel(request).await
    }
}

fn valid_metadata_key(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_GRPC_METADATA_KEY_BYTES
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-' | b'.')
        })
        && !value.ends_with("-bin")
}

fn forbidden_metadata_key(value: &str) -> bool {
    matches!(
        value,
        "authorization"
            | "cookie"
            | "grpc-encoding"
            | "grpc-message"
            | "grpc-status"
            | "grpc-timeout"
            | "proxy-authorization"
            | "te"
    )
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
