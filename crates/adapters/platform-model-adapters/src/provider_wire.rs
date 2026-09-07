use super::{
    ModelAdapterCancelOutcome, ModelAdapterExecutionRequest, ModelAdapterFailure,
    ModelAdapterFailureClass, NormalizedModelStream,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{stream, stream::BoxStream, StreamExt};
use insight_platform_contracts::{
    canonical_digest, canonical_json, ExactDeploymentRef, ExactSecretBindingRef, ExactVersionRef,
    ResourceId, ResourceKind, Sha256Digest, MAX_MODEL_REQUEST_BYTES, MAX_MODEL_RESPONSE_BYTES,
};
use insight_platform_models::{NormalizedModelDelta, NormalizedModelFrame};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeSet, fmt};

pub const OPENAI_RESPONSES_ADAPTER_NAME: &str = "openai.responses/v1";
pub const ANTHROPIC_MESSAGES_ADAPTER_NAME: &str = "anthropic.messages/2023-06-01";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelProviderWireProtocol {
    OpenAiResponses,
    AnthropicMessages,
}

/// Exact physical request identity used only for bounded in-flight connection ownership.
///
/// Durable Job/Attempt state remains in PostgreSQL. Including the Worker generation and lease
/// prevents a stale cancel from terminating a replacement worker's Provider connection.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelProviderRequestIdentity {
    pub protocol: ModelProviderWireProtocol,
    pub tenant_id: ResourceId,
    pub model_turn_id: ResourceId,
    pub job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub provider_deployment_id: ResourceId,
    pub provider_deployment_digest: Sha256Digest,
    pub attempt_no: u32,
    pub lease_generation: u64,
}

impl ModelProviderWireProtocol {
    pub const fn endpoint_path(self) -> &'static str {
        match self {
            Self::OpenAiResponses => "/v1/responses",
            Self::AnthropicMessages => "/v1/messages",
        }
    }

    pub const fn protocol_version(self) -> &'static str {
        match self {
            Self::OpenAiResponses => "responses-v1",
            Self::AnthropicMessages => "2023-06-01",
        }
    }
}

/// Credential-free request for the role-scoped Provider HTTP/Secret/Egress connector.
///
/// The connector resolves the exact Secret bindings and endpoint identity internally. No API key,
/// bearer token, arbitrary URL, caller header, or redirect target can cross this interface.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelProviderWireRequest {
    pub schema_version: u32,
    pub protocol: ModelProviderWireProtocol,
    pub tenant_id: ResourceId,
    pub model_turn_id: ResourceId,
    pub job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub attempt_no: u32,
    pub lease_generation: u64,
    pub admission_digest: Sha256Digest,
    pub model_request_digest: Sha256Digest,
    pub provider_deployment: ExactDeploymentRef,
    pub provider_revision: ExactVersionRef,
    pub endpoint_identity_digest: Sha256Digest,
    pub secret_bindings: Vec<ExactSecretBindingRef>,
    pub network_policy: ExactVersionRef,
    pub tls_policy: ExactVersionRef,
    pub trust_policy: ExactVersionRef,
    pub data_policy: ExactVersionRef,
    pub region: insight_platform_contracts::DataRegion,
    pub request_body: Value,
    pub request_body_digest: Sha256Digest,
    pub maximum_request_bytes: u32,
    pub maximum_response_bytes: u32,
    pub connect_timeout_milliseconds: u64,
    pub total_timeout_milliseconds: u64,
    pub deadline: DateTime<Utc>,
}

impl fmt::Debug for ModelProviderWireRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ModelProviderWireRequest")
            .field("protocol", &self.protocol)
            .field("tenant_id", &self.tenant_id)
            .field("model_turn_id", &self.model_turn_id)
            .field("job_id", &self.job_id)
            .field(
                "worker_process_generation_id",
                &self.worker_process_generation_id,
            )
            .field("attempt_no", &self.attempt_no)
            .field("lease_generation", &self.lease_generation)
            .field("admission_digest", &self.admission_digest)
            .field("model_request_digest", &self.model_request_digest)
            .field("provider_deployment", &self.provider_deployment)
            .field("provider_revision", &self.provider_revision)
            .field("endpoint_identity_digest", &self.endpoint_identity_digest)
            .field("secret_binding_count", &self.secret_bindings.len())
            .field("request_body_digest", &self.request_body_digest)
            .field("maximum_request_bytes", &self.maximum_request_bytes)
            .field("maximum_response_bytes", &self.maximum_response_bytes)
            .field(
                "connect_timeout_milliseconds",
                &self.connect_timeout_milliseconds,
            )
            .field(
                "total_timeout_milliseconds",
                &self.total_timeout_milliseconds,
            )
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

impl ModelProviderWireRequest {
    pub fn build(
        protocol: ModelProviderWireProtocol,
        execution: &ModelAdapterExecutionRequest,
        request_body: Value,
    ) -> Result<Self, ModelAdapterFailure> {
        let request_body_digest: Sha256Digest = canonical_digest(&request_body)
            .map_err(|_| rejected("model_wire_request_not_canonical"))?
            .parse()
            .map_err(|_| rejected("model_wire_request_not_canonical"))?;
        let request_bytes = serde_json::to_vec(&request_body)
            .map_err(|_| rejected("model_wire_request_not_canonical"))?
            .len();
        if request_bytes
            > usize::try_from(execution.provider.request_limits.maximum_request_bytes)
                .map_err(|_| rejected("model_wire_request_too_large"))?
        {
            return Err(rejected("model_wire_request_too_large"));
        }
        let wire = Self {
            schema_version: 2,
            protocol,
            tenant_id: execution.tenant_id.clone(),
            model_turn_id: execution.model_turn_id.clone(),
            job_id: execution.job_id.clone(),
            worker_process_generation_id: execution.worker_process_generation_id.clone(),
            attempt_no: execution.attempt_no,
            lease_generation: execution.lease_generation,
            admission_digest: execution.admission_digest.clone(),
            model_request_digest: execution.request_digest.clone(),
            provider_deployment: execution.provider_deployment.clone(),
            provider_revision: execution.provider_revision.clone(),
            endpoint_identity_digest: execution.provider_closure.endpoint_identity_digest.clone(),
            secret_bindings: execution.provider_closure.secret_bindings.clone(),
            network_policy: execution.provider_closure.network_policy.clone(),
            tls_policy: execution.provider_closure.tls_policy.clone(),
            trust_policy: execution.provider_closure.trust_policy.clone(),
            data_policy: execution.provider_closure.data_policy.clone(),
            region: execution.provider_closure.region.clone(),
            request_body,
            request_body_digest,
            maximum_request_bytes: execution.provider.request_limits.maximum_request_bytes,
            maximum_response_bytes: execution.provider.request_limits.maximum_response_bytes,
            connect_timeout_milliseconds: execution
                .provider
                .request_limits
                .connect_timeout_milliseconds,
            total_timeout_milliseconds: execution
                .provider
                .request_limits
                .total_timeout_milliseconds,
            deadline: execution.request.deadline,
        };
        wire.validate_at(Utc::now())?;
        Ok(wire)
    }

    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), ModelAdapterFailure> {
        self.provider_deployment
            .validate()
            .map_err(|_| rejected("model_wire_invalid_deployment"))?;
        self.provider_revision
            .validate()
            .map_err(|_| rejected("model_wire_invalid_revision"))?;
        if self.schema_version != 2
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.model_turn_id.kind() != ResourceKind::ModelTurn
            || self.job_id.kind() != ResourceKind::Job
            || self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.attempt_no == 0
            || self.lease_generation == 0
            || self.provider_deployment.resource_kind != ResourceKind::ModelProviderDeployment
            || self.provider_revision.resource_kind != ResourceKind::ModelProviderRevision
            || self.maximum_request_bytes == 0
            || self.maximum_request_bytes > MAX_MODEL_REQUEST_BYTES
            || self.maximum_response_bytes == 0
            || self.maximum_response_bytes > MAX_MODEL_RESPONSE_BYTES
            || self.connect_timeout_milliseconds == 0
            || self.total_timeout_milliseconds == 0
            || self.connect_timeout_milliseconds >= self.total_timeout_milliseconds
            || self.deadline <= now
        {
            return Err(rejected("model_wire_invalid_request"));
        }
        let policies = [
            &self.network_policy,
            &self.tls_policy,
            &self.trust_policy,
            &self.data_policy,
        ];
        let mut policy_ids = BTreeSet::new();
        for policy in policies {
            policy
                .validate()
                .map_err(|_| rejected("model_wire_invalid_policy"))?;
            if policy.resource_kind != ResourceKind::PolicyRevision
                || !policy_ids.insert(policy.revision_id.clone())
            {
                return Err(rejected("model_wire_invalid_policy"));
            }
        }
        let mut prior = None;
        for binding in &self.secret_bindings {
            binding
                .validate()
                .map_err(|_| rejected("model_wire_invalid_secret_binding"))?;
            let key = (&binding.purpose, &binding.secret_binding_id);
            if prior.is_some_and(|value| value >= key) {
                return Err(rejected("model_wire_invalid_secret_binding"));
            }
            prior = Some(key);
        }
        let encoded = canonical_json(&self.request_body)
            .map_err(|_| rejected("model_wire_request_not_canonical"))?;
        let encoded_len =
            u32::try_from(encoded.len()).map_err(|_| rejected("model_wire_request_too_large"))?;
        if encoded_len > self.maximum_request_bytes
            || canonical_digest(&self.request_body).ok().as_deref()
                != Some(self.request_body_digest.as_str())
        {
            return Err(rejected("model_wire_request_not_canonical"));
        }
        Ok(())
    }

    pub fn identity(&self) -> ModelProviderRequestIdentity {
        ModelProviderRequestIdentity {
            protocol: self.protocol,
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

    pub const fn endpoint_path(&self) -> &'static str {
        self.protocol.endpoint_path()
    }

    pub const fn protocol_version(&self) -> &'static str {
        self.protocol.protocol_version()
    }
}

/// One decoded SSE event after the connector has enforced HTTP status, content type, body limits,
/// redirect/TLS/egress policy and credential isolation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelProviderWireEvent {
    pub event_name: String,
    pub data: Value,
}

pub type ModelProviderWireStream =
    BoxStream<'static, Result<ModelProviderWireEvent, ModelAdapterFailure>>;

#[async_trait]
pub trait ModelProviderWireConnector: Send + Sync {
    async fn open(
        &self,
        request: ModelProviderWireRequest,
    ) -> Result<ModelProviderWireStream, ModelAdapterFailure>;

    async fn cancel(
        &self,
        protocol: ModelProviderWireProtocol,
        request: super::ModelAdapterCancelRequest,
    ) -> Result<ModelAdapterCancelOutcome, ModelAdapterFailure>;
}

pub(crate) trait ProviderEventCodec: Send + 'static {
    fn accept(
        &mut self,
        event: ModelProviderWireEvent,
    ) -> Result<Option<NormalizedModelFrame>, ModelAdapterFailure>;

    fn missing_terminal(&self) -> ModelAdapterFailure;
}

pub(crate) fn normalize_provider_stream<C>(
    upstream: ModelProviderWireStream,
    codec: C,
    maximum_response_bytes: u32,
) -> NormalizedModelStream
where
    C: ProviderEventCodec,
{
    struct State<C> {
        upstream: ModelProviderWireStream,
        codec: C,
        observed_bytes: u64,
        maximum_response_bytes: u64,
        done: bool,
    }

    Box::pin(stream::unfold(
        State {
            upstream,
            codec,
            observed_bytes: 0,
            maximum_response_bytes: u64::from(maximum_response_bytes),
            done: false,
        },
        |mut state| async move {
            if state.done {
                return None;
            }
            loop {
                let item = match state.upstream.next().await {
                    Some(item) => item,
                    None => {
                        state.done = true;
                        return Some((Err(state.codec.missing_terminal()), state));
                    }
                };
                let event = match item {
                    Ok(event) => event,
                    Err(failure) => {
                        state.done = true;
                        return Some((Err(failure), state));
                    }
                };
                let event_bytes = match serde_json::to_vec(&event)
                    .ok()
                    .and_then(|encoded| u64::try_from(encoded.len()).ok())
                {
                    Some(bytes) => bytes,
                    None => {
                        state.done = true;
                        return Some((Err(permanent("model_wire_event_not_json")), state));
                    }
                };
                state.observed_bytes = match state.observed_bytes.checked_add(event_bytes) {
                    Some(total) if total <= state.maximum_response_bytes => total,
                    _ => {
                        state.done = true;
                        return Some((Err(permanent("model_wire_response_too_large")), state));
                    }
                };
                match state.codec.accept(event) {
                    Ok(Some(frame)) => return Some((Ok(frame), state)),
                    Ok(None) => {}
                    Err(failure) => {
                        state.done = true;
                        return Some((Err(failure), state));
                    }
                }
            }
        },
    ))
}

pub(crate) struct NormalizedFrameBuilder {
    model_turn_id: ResourceId,
    attempt_no: u32,
    lease_generation: u64,
    next_sequence: u64,
    delta_count: u32,
    delta_bytes: u64,
}

impl NormalizedFrameBuilder {
    pub(crate) fn new(request: &ModelAdapterExecutionRequest) -> Self {
        Self {
            model_turn_id: request.model_turn_id.clone(),
            attempt_no: request.attempt_no,
            lease_generation: request.lease_generation,
            next_sequence: 1,
            delta_count: 0,
            delta_bytes: 0,
        }
    }

    pub(crate) fn live(
        &mut self,
        delta: NormalizedModelDelta,
    ) -> Result<NormalizedModelFrame, ModelAdapterFailure> {
        let bytes = u64::try_from(
            serde_json::to_vec(&delta)
                .map_err(|_| permanent("model_normalized_delta_not_json"))?
                .len(),
        )
        .map_err(|_| permanent("model_normalized_delta_overflow"))?;
        self.delta_count = self
            .delta_count
            .checked_add(1)
            .ok_or_else(|| permanent("model_normalized_delta_overflow"))?;
        self.delta_bytes = self
            .delta_bytes
            .checked_add(bytes)
            .ok_or_else(|| permanent("model_normalized_delta_overflow"))?;
        self.frame(delta)
    }

    pub(crate) fn terminal(
        &mut self,
        response: insight_platform_models::CanonicalModelResponse,
    ) -> Result<NormalizedModelFrame, ModelAdapterFailure> {
        self.frame(NormalizedModelDelta::Terminal(Box::new(response)))
    }

    fn frame(
        &mut self,
        delta: NormalizedModelDelta,
    ) -> Result<NormalizedModelFrame, ModelAdapterFailure> {
        let sequence = self.next_sequence;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| permanent("model_normalized_sequence_overflow"))?;
        Ok(NormalizedModelFrame {
            model_turn_id: self.model_turn_id.clone(),
            attempt_no: self.attempt_no,
            lease_generation: self.lease_generation,
            transport_sequence: sequence,
            delta,
        })
    }

    pub(crate) const fn delta_count(&self) -> u32 {
        self.delta_count
    }

    pub(crate) const fn delta_bytes(&self) -> u64 {
        self.delta_bytes
    }
}

pub(crate) fn rejected(code: &str) -> ModelAdapterFailure {
    failure(
        ModelAdapterFailureClass::RejectedBeforeDispatch,
        code,
        false,
        None,
    )
}

pub(crate) fn permanent(code: &str) -> ModelAdapterFailure {
    failure(ModelAdapterFailureClass::Permanent, code, true, None)
}

pub(crate) fn retryable_after_dispatch(code: &str, deadline: DateTime<Utc>) -> ModelAdapterFailure {
    let retry_at = Utc::now() + chrono::Duration::milliseconds(250);
    if retry_at >= deadline {
        return permanent(code);
    }
    failure(
        ModelAdapterFailureClass::RetryableAfterDispatch,
        code,
        true,
        Some(retry_at),
    )
}

fn failure(
    class: ModelAdapterFailureClass,
    code: &str,
    request_sent: bool,
    retry_at: Option<DateTime<Utc>>,
) -> ModelAdapterFailure {
    let evidence_digest: Sha256Digest = canonical_digest(&serde_json::json!({
        "domain": code,
        "schema_version": 1,
    }))
    .expect("static Provider wire evidence is canonical")
    .parse()
    .expect("canonical digest is SHA-256");
    ModelAdapterFailure {
        class,
        safe_code: code.to_owned(),
        safe_message: "Model Provider wire contract failed".to_owned(),
        evidence_digest,
        request_sent,
        retry_at,
    }
}

pub(crate) fn validate_wire_descriptor(
    descriptor: &super::InstalledModelAdapterDescriptor,
    expected_name: &str,
) -> Result<(), super::ModelAdapterHostError> {
    if descriptor.qualified_name != expected_name {
        return Err(super::ModelAdapterHostError::InvalidInstalledAdapter);
    }
    insight_platform_contracts::InstalledModelAdapter {
        qualified_name: descriptor.qualified_name.clone(),
        worker_manifest_digest: descriptor.worker_manifest_digest.clone(),
        adapter_contract_digest: descriptor.adapter_contract_digest.clone(),
    }
    .validate()
    .map_err(|_| super::ModelAdapterHostError::InvalidInstalledAdapter)
}
