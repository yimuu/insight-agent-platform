use super::{
    contract_failure, CapabilityAdapterFailure, CapabilityAdapterRequest,
    CapabilityAdapterResponse, CapabilityBackendPort, CapabilityDispatchError,
    CapabilityTransportCancelOutcome,
};
use async_trait::async_trait;
use base64::Engine as _;
use insight_platform_contracts::{
    canonical_digest, CapabilityBackendBinding, CapabilityBackendContract, CapabilityBackendKind,
    CapabilityDeploymentClosure, CapabilityImplementationResourceSpec,
    CapabilityInterfaceResourceSpec, ClosedJsonValue, DataClassification, Effect,
    ExactDeploymentRef, InstalledCapabilityCodecRef, InteractionKind, McpToolCapabilityContract,
    McpTransportKind, PublishedMcpMethod, ResourceId, ResourceKind, Sha256Digest, ValueRef,
};
use insight_platform_invocations::{
    BackendInputRequest, CapabilityExecutionInputMaterial, CapabilityInputAction, DispatchOutcome,
    EncryptedRemoteState, RemoteWait,
};
use insight_platform_jobs::WakeSource;
use insight_platform_mcp_host::{
    EncryptedMcpState, McpElicitationAction, McpElicitationResponse, McpExecutionContractQuery,
    McpExecutionContractResolutionError, McpExecutionContractResolver, McpHostClient,
    McpHostExecutionContract, McpOperationContinuation, McpOperationOutcome, McpOperationRequest,
    McpRemoteTaskCancelOutcome,
};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct InstalledMcpToolCodecDescriptor {
    pub codec_id: String,
    pub codec_version: String,
    pub module_digest: Sha256Digest,
    pub worker_protocol_version: u32,
    pub descriptor_digest: Sha256Digest,
    pub remote_tool_name: String,
    pub remote_input_schema_digest: insight_platform_contracts::Sha256Digest,
    pub output_mapping_digest: insight_platform_contracts::Sha256Digest,
    pub protocol_profile_id: insight_platform_contracts::ResourceId,
    pub protocol_profile_digest: insight_platform_contracts::Sha256Digest,
    pub discovery_semantic_evidence_digest: insight_platform_contracts::Sha256Digest,
}

impl InstalledMcpToolCodecDescriptor {
    pub fn exact(
        codec: &InstalledCapabilityCodecRef,
        contract: &McpToolCapabilityContract,
    ) -> Self {
        Self {
            codec_id: codec.codec_id.clone(),
            codec_version: codec.codec_version.clone(),
            module_digest: codec.module_digest.clone(),
            worker_protocol_version: codec.worker_protocol_version,
            descriptor_digest: codec.descriptor_digest.clone(),
            remote_tool_name: contract.remote_tool_name.clone(),
            remote_input_schema_digest: contract.remote_input_schema_digest.clone(),
            output_mapping_digest: contract.output_mapping_digest.clone(),
            protocol_profile_id: contract.protocol_profile.revision_id.clone(),
            protocol_profile_digest: contract.protocol_profile.semantic_digest.clone(),
            discovery_semantic_evidence_digest: contract.discovery_semantic_evidence_digest.clone(),
        }
    }
}

/// Sandbox-independent input for the exact declarative MCP Tool output mapping.
///
/// The microVM provider constructs this view only after validating a leased Sandbox request and
/// the raw MCP exchange. Keeping Sandbox transport types out of this interface prevents ordinary
/// Capability/Egress compositions from depending on the untrusted execution plane.
pub struct ManagedMcpToolDecodeContext<'a> {
    pub tenant_id: &'a ResourceId,
    pub invocation_id: &'a ResourceId,
    pub job_id: &'a ResourceId,
    pub capability_deployment: &'a ExactDeploymentRef,
    pub capability_deployment_closure: &'a CapabilityDeploymentClosure,
    pub capability_interface: &'a CapabilityInterfaceResourceSpec,
    pub capability_implementation: &'a CapabilityImplementationResourceSpec,
    pub input_value_id: &'a ResourceId,
    pub input_schema_digest: &'a Sha256Digest,
    pub input_ref: &'a ValueRef,
    pub output_value_id: &'a ResourceId,
    pub output_schema_digest: &'a Sha256Digest,
    pub classification: DataClassification,
    pub effect: Effect,
    pub deadline: chrono::DateTime<chrono::Utc>,
    pub logical_poll_count: u32,
}

pub trait McpToolCapabilityCodec: Send + Sync {
    fn descriptor(&self) -> InstalledMcpToolCodecDescriptor;

    fn encode(
        &self,
        request: &CapabilityAdapterRequest,
    ) -> Result<insight_platform_contracts::ClosedJsonValue, CapabilityAdapterFailure>;

    fn decode(
        &self,
        request: &CapabilityAdapterRequest,
        outcome: McpOperationOutcome,
    ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure>;

    /// Maps a validated Managed stdio protocol outcome inside the Sandbox execution plane.
    /// Implementations are installed by exact descriptor and must remain declarative/bounded.
    /// Ordinary Capability Workers never call this method.
    fn decode_managed_sandbox(
        &self,
        _context: &ManagedMcpToolDecodeContext<'_>,
        _outcome: McpOperationOutcome,
    ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure> {
        Err(contract_failure(
            CapabilityDispatchError::ProtocolCodecNotInstalled,
        ))
    }
}

#[derive(Default, Clone)]
pub struct InstalledMcpToolCodecRegistry {
    codecs: BTreeMap<InstalledMcpToolCodecDescriptor, Arc<dyn McpToolCapabilityCodec>>,
}

impl InstalledMcpToolCodecRegistry {
    pub fn install(
        &mut self,
        codec: Arc<dyn McpToolCapabilityCodec>,
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
        contract: &McpToolCapabilityContract,
    ) -> Result<&Arc<dyn McpToolCapabilityCodec>, CapabilityDispatchError> {
        codec
            .validate_for(&CapabilityBackendContract::Mcp(contract.clone()))
            .map_err(|_| CapabilityDispatchError::BackendContractMismatch)?;
        self.codecs
            .get(&InstalledMcpToolCodecDescriptor::exact(codec, contract))
            .ok_or(CapabilityDispatchError::ProtocolCodecNotInstalled)
    }

    pub fn decode_managed_sandbox(
        &self,
        codec: &InstalledCapabilityCodecRef,
        contract: &McpToolCapabilityContract,
        context: &ManagedMcpToolDecodeContext<'_>,
        outcome: McpOperationOutcome,
    ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure> {
        self.resolve(codec, contract)
            .map_err(contract_failure)?
            .decode_managed_sandbox(context, outcome)
    }
}

pub struct McpCapabilityAdapter {
    codecs: InstalledMcpToolCodecRegistry,
    resolver: Arc<dyn McpExecutionContractResolver>,
    hosts: BTreeMap<McpTransportKind, Arc<dyn McpHostClient>>,
}

impl McpCapabilityAdapter {
    pub fn new(
        codecs: InstalledMcpToolCodecRegistry,
        resolver: Arc<dyn McpExecutionContractResolver>,
    ) -> Self {
        Self {
            codecs,
            resolver,
            hosts: BTreeMap::new(),
        }
    }

    pub fn install_host(
        &mut self,
        transport: McpTransportKind,
        host: Arc<dyn McpHostClient>,
    ) -> Result<(), CapabilityDispatchError> {
        match self.hosts.entry(transport) {
            Entry::Vacant(entry) => {
                entry.insert(host);
                Ok(())
            }
            Entry::Occupied(_) => Err(CapabilityDispatchError::InvalidBackendPort),
        }
    }

    async fn prepare<'a>(
        &'a self,
        request: &'a CapabilityAdapterRequest,
    ) -> Result<PreparedMcpExecution<'a>, CapabilityAdapterFailure> {
        let CapabilityBackendContract::Mcp(tool_contract) =
            &request.execution.implementation.backend_contract
        else {
            return Err(contract_failure(
                CapabilityDispatchError::BackendContractMismatch,
            ));
        };
        let CapabilityBackendBinding::Mcp {
            codec,
            worker_manifest_digest,
            mcp_deployment,
            discovery_snapshot_id,
            discovery_snapshot_digest,
            authorization_policy,
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
        let runtime = request
            .mcp_runtime
            .as_ref()
            .ok_or_else(|| contract_failure(CapabilityDispatchError::BackendContractMismatch))?;
        let host_contract = self
            .resolver
            .resolve_mcp_execution_contract(&McpExecutionContractQuery {
                schema_version: 1,
                tenant_id: request.tenant_id.clone(),
                mcp_deployment: runtime.mcp_deployment.clone(),
                discovery_snapshot_id: runtime.discovery_snapshot_id.clone(),
                discovery_snapshot_digest: runtime.discovery_snapshot_digest.clone(),
                authorization_binding_id: runtime.authorization_binding_id.clone(),
                authorization_generation: runtime.authorization_generation,
                authorization_context_digest: runtime.authorization_context_digest.clone(),
                principal_id: runtime.principal_id.clone(),
            })
            .await
            .map_err(resolution_failure)?;
        host_contract
            .validate_canonical_at(chrono::Utc::now())
            .map_err(|_| contract_failure(CapabilityDispatchError::BackendContractMismatch))?;
        if &runtime.mcp_deployment != mcp_deployment
            || &runtime.discovery_snapshot_id != discovery_snapshot_id
            || &runtime.discovery_snapshot_digest != discovery_snapshot_digest
            || runtime.authorization_binding_id
                != host_contract.authorization.authorization_binding_id
            || runtime.authorization_generation != host_contract.authorization.generation
            || runtime.authorization_context_digest != host_contract.authorization.canonical_digest
            || runtime.principal_id != host_contract.authorization.principal_id
            || host_contract.deployment != *mcp_deployment
            || host_contract.discovery.snapshot_id != *discovery_snapshot_id
            || host_contract.discovery.canonical_digest != *discovery_snapshot_digest
            || host_contract.server.protocol_policy != tool_contract.protocol_profile
            || host_contract.discovery.objects_digest
                != tool_contract.discovery_semantic_evidence_digest
            || host_contract.deployment_closure.auth_policy.as_ref() != Some(authorization_policy)
        {
            return Err(contract_failure(
                CapabilityDispatchError::BackendContractMismatch,
            ));
        }
        let codec = self
            .codecs
            .resolve(codec, tool_contract)
            .map_err(contract_failure)?;
        let params = codec.encode(request)?;
        params
            .validate()
            .map_err(|_| contract_failure(CapabilityDispatchError::MalformedAdapterResponse))?;
        let continuation = mcp_continuation(request)?;
        let operation = McpOperationRequest {
            schema_version: 1,
            mcp_operation_id: runtime.mcp_operation_id.clone(),
            tenant_id: request.tenant_id.clone(),
            invocation_id: request.invocation_id.clone(),
            job_id: request.job_id.clone(),
            worker_process_generation_id: request.worker_process_generation_id.clone(),
            lease_generation: request.lease_generation,
            physical_attempt: request.physical_attempt,
            snapshot_id: runtime.discovery_snapshot_id.clone(),
            authorization_binding_id: runtime.authorization_binding_id.clone(),
            method: PublishedMcpMethod::ToolsCall,
            params,
            task_requested: continuation.is_none()
                && tool_contract.supports_task
                && request.execution.implementation.features.deferred
                && request.execution.implementation.features.poll,
            continuation,
            idempotency_key_digest: request.idempotency_key_digest.clone(),
            idempotency: request.idempotency,
            effect: request.effect,
            deadline: request.deadline,
        };
        let host = self
            .hosts
            .get(&host_contract.transport_kind())
            .ok_or_else(|| contract_failure(CapabilityDispatchError::BackendPortNotInstalled))?;
        Ok(PreparedMcpExecution {
            tool_contract,
            codec,
            host_contract,
            host,
            operation,
        })
    }
}

struct PreparedMcpExecution<'a> {
    tool_contract: &'a McpToolCapabilityContract,
    codec: &'a Arc<dyn McpToolCapabilityCodec>,
    host_contract: McpHostExecutionContract,
    host: &'a Arc<dyn McpHostClient>,
    operation: McpOperationRequest,
}

fn mcp_continuation(
    request: &CapabilityAdapterRequest,
) -> Result<Option<McpOperationContinuation>, CapabilityAdapterFailure> {
    request
        .continuation
        .as_ref()
        .map(|continuation| {
            let ciphertext = base64::engine::general_purpose::STANDARD
                .decode(&continuation.encrypted_remote_state.ciphertext)
                .map_err(|_| contract_failure(CapabilityDispatchError::BackendContractMismatch))?;
            let encrypted_state = EncryptedMcpState {
                scheme: continuation.encrypted_remote_state.scheme.clone(),
                ciphertext,
                key_id: continuation.encrypted_remote_state.key_id.clone(),
                key_reference_digest: continuation
                    .encrypted_remote_state
                    .key_reference_digest
                    .clone(),
                plaintext_digest: continuation.encrypted_remote_state.plaintext_digest.clone(),
            };
            encrypted_state
                .validate()
                .map_err(|_| contract_failure(CapabilityDispatchError::BackendContractMismatch))?;
            let elicitation_response = match (
                continuation.resume_input_action,
                continuation.resume_input.as_ref(),
            ) {
                (None, None) => None,
                (Some(CapabilityInputAction::Accept), Some(input)) => {
                    let CapabilityExecutionInputMaterial::Inline { value } = &input.material else {
                        return Err(contract_failure(
                            CapabilityDispatchError::BackendContractMismatch,
                        ));
                    };
                    Some(McpElicitationResponse {
                        action: McpElicitationAction::Accept,
                        content: Some(
                            ClosedJsonValue::build(
                                input.exact.schema_digest.clone(),
                                value.clone(),
                            )
                            .map_err(|_| {
                                contract_failure(CapabilityDispatchError::BackendContractMismatch)
                            })?,
                        ),
                    })
                }
                (Some(CapabilityInputAction::Decline), None) => Some(McpElicitationResponse {
                    action: McpElicitationAction::Decline,
                    content: None,
                }),
                (Some(CapabilityInputAction::Cancel), None) => Some(McpElicitationResponse {
                    action: McpElicitationAction::Cancel,
                    content: None,
                }),
                _ => {
                    return Err(contract_failure(
                        CapabilityDispatchError::BackendContractMismatch,
                    ))
                }
            };
            Ok(McpOperationContinuation {
                encrypted_state,
                external_identity_digest: continuation
                    .external_identity_digest
                    .clone()
                    .ok_or_else(|| {
                        contract_failure(CapabilityDispatchError::BackendContractMismatch)
                    })?,
                poll_count: continuation.poll_count,
                elicitation_response,
            })
        })
        .transpose()
}

#[async_trait]
impl CapabilityBackendPort for McpCapabilityAdapter {
    fn kind(&self) -> CapabilityBackendKind {
        CapabilityBackendKind::Mcp
    }

    async fn invoke(
        &self,
        request: &CapabilityAdapterRequest,
    ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure> {
        let prepared = self.prepare(request).await?;
        let outcome = prepared
            .host
            .execute(&prepared.host_contract, &prepared.operation)
            .await
            .map_err(|_| contract_failure(CapabilityDispatchError::MalformedAdapterResponse))?;
        if matches!(outcome, McpOperationOutcome::RemoteTask { .. })
            && !prepared.tool_contract.supports_task
        {
            return Err(contract_failure(
                CapabilityDispatchError::MalformedAdapterResponse,
            ));
        }
        if let McpOperationOutcome::RemoteTask {
            encrypted_state,
            external_identity_digest,
            next_poll_at,
        } = outcome
        {
            if request.continuation.as_ref().is_some_and(|continuation| {
                continuation.poll_count >= request.execution.implementation.features.max_poll_count
            }) {
                let is_write = request.effect.risk_rank() >= Effect::IdempotentWrite.risk_rank();
                return Ok(CapabilityAdapterResponse {
                    outcome: super::adapter_failure_outcome(
                        request,
                        CapabilityAdapterFailure {
                            class: if is_write {
                                super::CapabilityAdapterFailureClass::Uncertain
                            } else {
                                super::CapabilityAdapterFailureClass::Permanent
                            },
                            safe_code: "mcp_remote_task_poll_limit_exhausted".to_owned(),
                            safe_message:
                                "MCP remote Task remained active after its bounded poll policy"
                                    .to_owned(),
                            evidence_digest: insight_platform_contracts::canonical_digest(
                                &serde_json::json!({
                                    "domain": "mcp_remote_task_poll_limit_exhausted",
                                    "schema_version": 1,
                                }),
                            )
                            .expect("static MCP adapter evidence is canonical")
                            .parse()
                            .expect("canonical digest is SHA-256"),
                            external_identity_digest: is_write.then_some(external_identity_digest),
                        },
                        None,
                    )
                    .map_err(contract_failure)?,
                });
            }
            let state = EncryptedRemoteState {
                scheme: encrypted_state.scheme,
                key_id: encrypted_state.key_id,
                key_reference_digest: encrypted_state.key_reference_digest,
                ciphertext: base64::engine::general_purpose::STANDARD
                    .encode(encrypted_state.ciphertext),
                plaintext_digest: encrypted_state.plaintext_digest,
            };
            state
                .validate(
                    request
                        .execution
                        .implementation
                        .features
                        .max_remote_state_bytes,
                )
                .map_err(|_| contract_failure(CapabilityDispatchError::MalformedAdapterResponse))?;
            return Ok(CapabilityAdapterResponse {
                outcome: DispatchOutcome::Deferred(RemoteWait {
                    encrypted_state: state,
                    external_identity_digest,
                    accepted_sources: vec![WakeSource::Poll],
                    next_poll_at: Some(next_poll_at),
                    callback_binding_digest: None,
                }),
            });
        }
        if let McpOperationOutcome::InputRequired {
            encrypted_state,
            external_identity_digest,
            safe_prompt_key,
            response_schema,
            response_schema_digest,
            deadline,
        } = outcome
        {
            if !prepared.tool_contract.supports_task
                || !request.execution.implementation.features.input_required
                || request
                    .continuation
                    .as_ref()
                    .is_some_and(|continuation| continuation.resume_input_action.is_some())
            {
                return Err(contract_failure(
                    CapabilityDispatchError::MalformedAdapterResponse,
                ));
            }
            let state = EncryptedRemoteState {
                scheme: encrypted_state.scheme,
                key_id: encrypted_state.key_id,
                key_reference_digest: encrypted_state.key_reference_digest,
                ciphertext: base64::engine::general_purpose::STANDARD
                    .encode(encrypted_state.ciphertext),
                plaintext_digest: encrypted_state.plaintext_digest,
            };
            state
                .validate(
                    request
                        .execution
                        .implementation
                        .features
                        .max_remote_state_bytes,
                )
                .map_err(|_| contract_failure(CapabilityDispatchError::MalformedAdapterResponse))?;
            return Ok(CapabilityAdapterResponse {
                outcome: DispatchOutcome::InputRequired(BackendInputRequest {
                    input_task_id: deterministic_input_task_id(request, &state.plaintext_digest)?,
                    interaction_kind: InteractionKind::Form,
                    safe_prompt_key,
                    response_schema,
                    response_schema_digest,
                    eligible_principal_rule_digest: exact_principal_rule_digest(request)?,
                    exact_eligible_principal_id: request
                        .mcp_runtime
                        .as_ref()
                        .map(|runtime| runtime.principal_id.clone()),
                    opaque_state_digest: state.plaintext_digest.clone(),
                    encrypted_state: state,
                    external_identity_digest,
                    deadline,
                }),
            });
        }
        prepared.codec.decode(request, outcome)
    }

    async fn cancel_execution(
        &self,
        request: &CapabilityAdapterRequest,
        deadline: chrono::DateTime<chrono::Utc>,
    ) -> Result<CapabilityTransportCancelOutcome, CapabilityAdapterFailure> {
        let mut prepared = self.prepare(request).await?;
        if prepared.operation.continuation.is_none()
            || !prepared.tool_contract.supports_task
            || !prepared
                .host_contract
                .discovery
                .negotiated_capabilities
                .tasks_cancel
        {
            return Ok(CapabilityTransportCancelOutcome::Unsupported);
        }
        prepared.operation.task_requested = false;
        prepared.operation.deadline = deadline;
        match prepared
            .host
            .cancel_remote_task(&prepared.host_contract, &prepared.operation, deadline)
            .await
        {
            Ok(McpRemoteTaskCancelOutcome::Accepted) => {
                Ok(CapabilityTransportCancelOutcome::Accepted)
            }
            Err(_) => Err(CapabilityAdapterFailure {
                class: super::CapabilityAdapterFailureClass::Permanent,
                safe_code: "mcp_remote_task_cancel_failed".to_owned(),
                safe_message: "MCP remote Task cancellation could not be confirmed".to_owned(),
                evidence_digest: insight_platform_contracts::canonical_digest(&serde_json::json!({
                    "domain": "mcp_remote_task_cancel_failed",
                    "schema_version": 1,
                }))
                .expect("static MCP adapter evidence is canonical")
                .parse()
                .expect("canonical digest is SHA-256"),
                external_identity_digest: None,
            }),
        }
    }
}

fn deterministic_input_task_id(
    request: &CapabilityAdapterRequest,
    opaque_state_digest: &Sha256Digest,
) -> Result<ResourceId, CapabilityAdapterFailure> {
    let material = serde_json::json!({
        "domain": "mcp_input_task_id",
        "schema_version": 1,
        "tenant_id": request.tenant_id,
        "invocation_id": request.invocation_id,
        "job_id": request.job_id,
        "physical_attempt": request.physical_attempt,
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

fn exact_principal_rule_digest(
    request: &CapabilityAdapterRequest,
) -> Result<Sha256Digest, CapabilityAdapterFailure> {
    let runtime = request
        .mcp_runtime
        .as_ref()
        .ok_or_else(|| contract_failure(CapabilityDispatchError::BackendContractMismatch))?;
    canonical_digest(&serde_json::json!({
        "domain": "mcp_exact_eligible_principal",
        "schema_version": 1,
        "tenant_id": request.tenant_id,
        "principal_id": runtime.principal_id,
        "authorization_binding_id": runtime.authorization_binding_id,
        "authorization_generation": runtime.authorization_generation,
    }))
    .map_err(|_| contract_failure(CapabilityDispatchError::MalformedAdapterResponse))?
    .parse()
    .map_err(|_| contract_failure(CapabilityDispatchError::MalformedAdapterResponse))
}

fn resolution_failure(failure: McpExecutionContractResolutionError) -> CapabilityAdapterFailure {
    match failure {
        McpExecutionContractResolutionError::AuthorityUnavailable => CapabilityAdapterFailure {
            class: super::CapabilityAdapterFailureClass::RetryableBeforeDispatch,
            safe_code: "mcp_contract_authority_unavailable".to_owned(),
            safe_message: "MCP execution contract authority is temporarily unavailable".to_owned(),
            evidence_digest: insight_platform_contracts::canonical_digest(&serde_json::json!({
                "domain": "mcp_contract_authority_unavailable",
                "schema_version": 1,
            }))
            .expect("static MCP adapter evidence is canonical")
            .parse()
            .expect("canonical digest is SHA-256"),
            external_identity_digest: None,
        },
        McpExecutionContractResolutionError::InvalidQuery
        | McpExecutionContractResolutionError::NotFoundOrChanged => {
            contract_failure(CapabilityDispatchError::BackendContractMismatch)
        }
    }
}
