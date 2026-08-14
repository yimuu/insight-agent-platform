//! Trusted protocol and lifecycle composition for the Platform microVM Sandbox backend.
//!
//! This crate belongs exclusively to the independently deployed microVM provider. Ordinary API,
//! Capability Worker, MCP Host and Egress processes must not link it. The first slice maps a raw,
//! validated Managed MCP exchange into the single physical Sandbox Job outcome; Firecracker and
//! guest lifecycle ownership are layered behind the same provider boundary.

use base64::Engine as _;
use insight_platform_capability_adapters::{
    contract_failure, CapabilityAdapterFailure, CapabilityAdapterResponse, CapabilityDispatchError,
    InstalledMcpToolCodecRegistry, ManagedMcpToolDecodeContext,
};
use insight_platform_contracts::{
    canonical_digest, CapabilityBackendContract, InteractionKind, ResourceId, ResourceKind,
    Sha256Digest,
};
use insight_platform_invocations::{
    BackendInputRequest, DispatchOutcome, EncryptedRemoteState, RemoteWait,
};
use insight_platform_jobs::WakeSource;
use insight_platform_mcp_host::{EncryptedMcpState, McpOperationOutcome};
use insight_platform_sandbox::{
    SandboxCommandLimits, SandboxExecutionOutcome, SandboxExecutionRequest, SandboxExecutionSource,
    SandboxManagedMcpOutput, SandboxResourceUsage,
};
use sha2::{Digest as _, Sha256};

mod firecracker;
mod guest_protocol;
mod lifecycle;
mod managed_guest_protocol;
mod managed_provider;
mod provider;
mod system_host;

pub use firecracker::*;
pub use guest_protocol::*;
pub use lifecycle::*;
pub use managed_guest_protocol::*;
pub use managed_provider::*;
pub use provider::*;
pub use system_host::*;

/// Trusted protocol adapter installed only in the microVM provider composition.
///
/// It validates the raw MCP outcome against the exact post-claim operation, applies the exact
/// descriptor-selected declarative Capability codec and then revalidates the bounded physical
/// Sandbox result. It performs no durable write and receives no Secret value.
pub struct ManagedMcpSandboxProtocolAdapter {
    codecs: InstalledMcpToolCodecRegistry,
    limits: SandboxCommandLimits,
}

impl ManagedMcpSandboxProtocolAdapter {
    pub fn new(codecs: InstalledMcpToolCodecRegistry, limits: SandboxCommandLimits) -> Self {
        Self { codecs, limits }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn decode(
        &self,
        request: &SandboxExecutionRequest,
        worker_process_generation_id: ResourceId,
        outcome: McpOperationOutcome,
        observed_at: chrono::DateTime<chrono::Utc>,
        protocol_evidence_digest: Sha256Digest,
        usage: SandboxResourceUsage,
    ) -> Result<SandboxManagedMcpOutput, CapabilityAdapterFailure> {
        request
            .validate_at(observed_at, self.limits)
            .map_err(|_| contract_failure(CapabilityDispatchError::InvalidRequest))?;
        let SandboxExecutionSource::ManagedMcp {
            capability_deployment_closure,
            capability_interface,
            capability_implementation,
            mcp_contract,
            operation,
            ..
        } = &request.execution_source
        else {
            return Err(contract_failure(CapabilityDispatchError::InvalidRequest));
        };
        let CapabilityBackendContract::Mcp(tool_contract) =
            &capability_implementation.backend_contract
        else {
            return Err(contract_failure(
                CapabilityDispatchError::BackendContractMismatch,
            ));
        };
        let physical = operation.bind_physical(
            worker_process_generation_id.clone(),
            request.lease_generation,
        );
        outcome
            .validate_for(&physical, mcp_contract, observed_at)
            .map_err(|_| contract_failure(CapabilityDispatchError::MalformedAdapterResponse))?;
        let logical_poll_count = operation
            .continuation
            .as_ref()
            .map_or(0, |continuation| continuation.poll_count);
        let response = match outcome {
            McpOperationOutcome::RemoteTask {
                encrypted_state,
                external_identity_digest,
                next_poll_at,
            } => CapabilityAdapterResponse {
                outcome: DispatchOutcome::Deferred(RemoteWait {
                    encrypted_state: encode_remote_state(encrypted_state),
                    external_identity_digest,
                    accepted_sources: vec![WakeSource::Poll],
                    next_poll_at: Some(next_poll_at),
                    callback_binding_digest: None,
                }),
            },
            McpOperationOutcome::InputRequired {
                encrypted_state,
                external_identity_digest,
                safe_prompt_key,
                response_schema,
                response_schema_digest,
                deadline,
            } => {
                let state = encode_remote_state(encrypted_state);
                CapabilityAdapterResponse {
                    outcome: DispatchOutcome::InputRequired(BackendInputRequest {
                        input_task_id: deterministic_sandbox_input_task_id(
                            request,
                            &state.plaintext_digest,
                        )?,
                        interaction_kind: InteractionKind::Form,
                        safe_prompt_key,
                        response_schema,
                        response_schema_digest,
                        eligible_principal_rule_digest: sandbox_exact_principal_rule_digest(
                            request,
                        )?,
                        exact_eligible_principal_id: Some(
                            mcp_contract.authorization.principal_id.clone(),
                        ),
                        opaque_state_digest: state.plaintext_digest.clone(),
                        encrypted_state: state,
                        external_identity_digest,
                        deadline,
                    }),
                }
            }
            terminal => self.codecs.decode_managed_sandbox(
                tool_contract,
                &ManagedMcpToolDecodeContext {
                    tenant_id: &request.tenant_id,
                    invocation_id: &request.invocation_id,
                    job_id: &request.job_id,
                    capability_deployment: &request.capability_deployment,
                    capability_deployment_closure,
                    capability_interface,
                    capability_implementation,
                    input_value_id: &request.input_value_id,
                    input_schema_digest: &request.input_schema_digest,
                    input_ref: &request.input_ref,
                    output_value_id: &request.output_value_id,
                    output_schema_digest: &request.output_schema_digest,
                    classification: request.classification,
                    effect: request.effect,
                    deadline: request.deadline,
                    logical_poll_count,
                },
                terminal,
            )?,
        };
        let mapped = SandboxManagedMcpOutput {
            worker_process_generation_id,
            logical_poll_count,
            observed_at,
            outcome: response.outcome,
            protocol_evidence_digest,
            usage,
        };
        SandboxExecutionOutcome::ManagedMcp(Box::new(mapped.clone()))
            .validate_for(request, self.limits)
            .map_err(|_| contract_failure(CapabilityDispatchError::MalformedAdapterResponse))?;
        Ok(mapped)
    }
}

fn encode_remote_state(state: EncryptedMcpState) -> EncryptedRemoteState {
    EncryptedRemoteState {
        scheme: state.scheme,
        key_id: state.key_id,
        key_reference_digest: state.key_reference_digest,
        ciphertext: base64::engine::general_purpose::STANDARD.encode(state.ciphertext),
        plaintext_digest: state.plaintext_digest,
    }
}

fn deterministic_sandbox_input_task_id(
    request: &SandboxExecutionRequest,
    opaque_state_digest: &Sha256Digest,
) -> Result<ResourceId, CapabilityAdapterFailure> {
    let material = serde_json::json!({
        "domain": "managed_mcp_sandbox_input_task_id",
        "schema_version": 1,
        "tenant_id": request.tenant_id,
        "invocation_id": request.invocation_id,
        "job_id": request.job_id,
        "physical_attempt": request.attempt_no,
        "opaque_state_digest": opaque_state_digest,
    });
    let canonical = insight_platform_contracts::canonical_json(&material)
        .map_err(|_| contract_failure(CapabilityDispatchError::MalformedAdapterResponse))?;
    let digest = Sha256::digest(canonical);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[..6].copy_from_slice(&request.invocation_id.uuid().as_bytes()[..6]);
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    ResourceId::from_uuid_v7(ResourceKind::Interaction, uuid::Uuid::from_bytes(bytes))
        .map_err(|_| contract_failure(CapabilityDispatchError::MalformedAdapterResponse))
}

fn sandbox_exact_principal_rule_digest(
    request: &SandboxExecutionRequest,
) -> Result<Sha256Digest, CapabilityAdapterFailure> {
    let SandboxExecutionSource::ManagedMcp { mcp_contract, .. } = &request.execution_source else {
        return Err(contract_failure(CapabilityDispatchError::InvalidRequest));
    };
    canonical_digest(&serde_json::json!({
        "domain": "managed_mcp_sandbox_exact_eligible_principal",
        "schema_version": 1,
        "tenant_id": request.tenant_id,
        "principal_id": mcp_contract.authorization.principal_id,
        "authorization_binding_id": mcp_contract.authorization.authorization_binding_id,
        "authorization_generation": mcp_contract.authorization.generation,
    }))
    .map_err(|_| contract_failure(CapabilityDispatchError::MalformedAdapterResponse))?
    .parse()
    .map_err(|_| contract_failure(CapabilityDispatchError::MalformedAdapterResponse))
}
