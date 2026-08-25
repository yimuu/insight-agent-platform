//! Bounded production driver for native and remote Capability Worker lanes.
//!
//! PostgreSQL owns every durable Invocation, Job, lease and quota transition. This crate reserves
//! process-local capacity before claim, executes only the exact immutable adapter contract returned
//! by that claim, heartbeats long dispatches, and submits one fenced outcome.

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use insight_platform_capability_adapters::{
    CapabilityAdapterFailure, CapabilityAdapterFailureClass, CapabilityAdapterResponse,
    CapabilityAdapterWorker, CapabilityExecutionAuthority, EncodedGrpcCapabilityRequest,
    EncodedHttpCapabilityRequest, ExecuteCapabilityAdapterJob, GrpcCapabilityCodec,
    GrpcTransportResponse, HttpCapabilityCodec, HttpTransportResponse,
    InstalledGrpcCodecDescriptor, InstalledGrpcCodecRegistry, InstalledHttpCodecDescriptor,
    InstalledHttpCodecRegistry, InstalledMcpToolCodecDescriptor, InstalledMcpToolCodecRegistry,
    InstalledNativeAdapter, InstalledNativeRegistry, McpToolCapabilityCodec,
    NativeCapabilityAdapter, SafeHttpHeader,
};
use insight_platform_contracts::{
    canonical_digest, checked_in_hard_limit_profile, parse_strict_json, ClosedJsonValue,
    HardLimitProfile, JsonLimits, ResourceId, ResourceIdError, ResourceKind, Sha256Digest,
    ValueRef, WorkClass, WORKER_PROTOCOL_VERSION,
};
use insight_platform_invocations::{
    CapabilityClaimSlot, CapabilityExecutionInputMaterial, CapabilityOutputValue,
    CapabilityWorkerAudit, ClaimCapabilityJobs, DispatchOutcome, CAPABILITY_QUOTA_LINES,
};
use insight_platform_jobs::JobFence;
use insight_platform_mcp_host::McpOperationOutcome;
use insight_platform_postgres::{
    capability_execution_repository::{
        ClaimedCapabilityExecution, DriveExpiredCapabilityJobs, ExpiredCapabilityRecoverySlot,
    },
    repository::{
        HeartbeatJob, JobFence as RepositoryJobFence, PgRepository, RepositoryError,
        SafetyScanShard,
    },
};
use insight_platform_worker::{
    ClaimBatchHardLimit, ClaimedJobIdentity, LocalWorkerPoolError, LocalWorkerPools,
};
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeSet, error::Error, fmt, sync::Arc, time::Duration};
use tokio::{task::JoinSet, time::Instant};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub const CAPABILITY_NATIVE_WORKER_ROLE: &str = "capability.native";
pub const CAPABILITY_REMOTE_WORKER_ROLE: &str = "capability.remote";
pub const BUILTIN_ECHO_ADAPTER_ID: &str = "builtin.echo";
pub const BUILTIN_ECHO_ADAPTER_VERSION: &str = "1.0.0";
pub const BUILTIN_ECHO_ENTRYPOINT_ID: &str = "echo.inline";
pub const BUILTIN_JSON_CODEC_ID: &str = "platform.json";
pub const BUILTIN_JSON_CODEC_VERSION: &str = "1.0.0";

pub fn builtin_json_codec_module_digest() -> Sha256Digest {
    domain_digest("builtin-json-codec-module")
}

pub fn builtin_json_http_protocol_contract_digest() -> Sha256Digest {
    domain_digest("builtin-json-http-protocol-contract")
}

pub fn builtin_json_http_request_mapping_digest() -> Sha256Digest {
    domain_digest("builtin-json-http-request-mapping")
}

pub fn builtin_json_http_response_mapping_digest() -> Sha256Digest {
    domain_digest("builtin-json-http-response-mapping")
}

pub fn builtin_json_http_error_mapping_digest() -> Sha256Digest {
    domain_digest("builtin-json-http-error-mapping")
}

pub fn builtin_json_grpc_protobuf_contract_digest() -> Sha256Digest {
    domain_digest("builtin-json-grpc-protobuf-contract")
}

pub fn builtin_json_grpc_request_mapping_digest() -> Sha256Digest {
    domain_digest("builtin-json-grpc-request-mapping")
}

pub fn builtin_json_grpc_response_mapping_digest() -> Sha256Digest {
    domain_digest("builtin-json-grpc-response-mapping")
}

pub fn builtin_json_grpc_error_mapping_digest() -> Sha256Digest {
    domain_digest("builtin-json-grpc-error-mapping")
}

pub fn builtin_json_mcp_output_mapping_digest() -> Sha256Digest {
    domain_digest("builtin-json-mcp-output-mapping")
}

#[derive(Debug, Clone)]
pub struct BuiltinJsonHttpCapabilityCodec {
    descriptor: InstalledHttpCodecDescriptor,
}

impl BuiltinJsonHttpCapabilityCodec {
    pub fn new(descriptor: InstalledHttpCodecDescriptor) -> Result<Self, CapabilityAdapterFailure> {
        validate_json_codec_identity(
            &descriptor.codec_id,
            &descriptor.codec_version,
            &descriptor.module_digest,
            descriptor.worker_protocol_version,
        )?;
        if descriptor.protocol_contract_digest != builtin_json_http_protocol_contract_digest()
            || descriptor.request_mapping_digest != builtin_json_http_request_mapping_digest()
            || descriptor.response_mapping_digest != builtin_json_http_response_mapping_digest()
            || descriptor.error_mapping_digest != builtin_json_http_error_mapping_digest()
        {
            return Err(permanent_adapter_failure(
                "builtin_json_http_descriptor",
                "Built-in HTTP JSON codec descriptor is not the compiled contract",
            ));
        }
        Ok(Self { descriptor })
    }
}

impl HttpCapabilityCodec for BuiltinJsonHttpCapabilityCodec {
    fn descriptor(&self) -> InstalledHttpCodecDescriptor {
        self.descriptor.clone()
    }

    fn encode(
        &self,
        request: &insight_platform_capability_adapters::CapabilityAdapterRequest,
    ) -> Result<EncodedHttpCapabilityRequest, CapabilityAdapterFailure> {
        let CapabilityExecutionInputMaterial::Inline { value } = &request.input.material else {
            return Err(permanent_adapter_failure(
                "builtin_json_inline_only",
                "Built-in JSON codec accepts Inline input only",
            ));
        };
        Ok(EncodedHttpCapabilityRequest {
            headers: vec![SafeHttpHeader {
                name: "content-type".to_owned(),
                value: "application/json".to_owned(),
            }],
            body: serde_jcs::to_vec(value).map_err(|_| {
                permanent_adapter_failure(
                    "builtin_json_encode",
                    "Built-in JSON request encoding failed",
                )
            })?,
        })
    }

    fn decode(
        &self,
        request: &insight_platform_capability_adapters::CapabilityAdapterRequest,
        response: HttpTransportResponse,
    ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure> {
        if (200..=299).contains(&response.status) {
            decode_json_success(
                request,
                &response.body,
                response.transport_evidence_digest,
                "http",
            )
        } else {
            let class = http_failure_class(response.status);
            failure_result(
                request,
                class,
                "builtin_json_http_status",
                response.transport_evidence_digest,
            )
        }
    }
}

#[derive(Debug, Clone)]
pub struct BuiltinJsonGrpcCapabilityCodec {
    descriptor: InstalledGrpcCodecDescriptor,
}

#[derive(Debug, Clone)]
pub struct BuiltinJsonMcpToolCapabilityCodec {
    descriptor: InstalledMcpToolCodecDescriptor,
}

impl BuiltinJsonMcpToolCapabilityCodec {
    pub fn new(
        descriptor: InstalledMcpToolCodecDescriptor,
    ) -> Result<Self, CapabilityAdapterFailure> {
        validate_json_codec_identity(
            &descriptor.codec_id,
            &descriptor.codec_version,
            &descriptor.module_digest,
            descriptor.worker_protocol_version,
        )?;
        if descriptor.output_mapping_digest != builtin_json_mcp_output_mapping_digest() {
            return Err(permanent_adapter_failure(
                "builtin_json_mcp_descriptor",
                "Built-in MCP JSON codec descriptor is not the compiled mapping",
            ));
        }
        Ok(Self { descriptor })
    }
}

impl McpToolCapabilityCodec for BuiltinJsonMcpToolCapabilityCodec {
    fn descriptor(&self) -> InstalledMcpToolCodecDescriptor {
        self.descriptor.clone()
    }

    fn encode(
        &self,
        request: &insight_platform_capability_adapters::CapabilityAdapterRequest,
    ) -> Result<ClosedJsonValue, CapabilityAdapterFailure> {
        let CapabilityExecutionInputMaterial::Inline { value } = &request.input.material else {
            return Err(permanent_adapter_failure(
                "builtin_json_inline_only",
                "Built-in JSON codec accepts Inline input only",
            ));
        };
        ClosedJsonValue::build(
            self.descriptor.remote_input_schema_digest.clone(),
            serde_json::json!({
                "arguments": value,
                "name": self.descriptor.remote_tool_name,
            }),
        )
        .map_err(|_| {
            permanent_adapter_failure(
                "builtin_json_encode",
                "Built-in MCP JSON request encoding failed",
            )
        })
    }

    fn decode(
        &self,
        request: &insight_platform_capability_adapters::CapabilityAdapterRequest,
        outcome: McpOperationOutcome,
    ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure> {
        match outcome {
            McpOperationOutcome::Completed {
                result,
                evidence_digest,
            } => {
                if result.schema_digest != request.output_schema_digest {
                    return Err(permanent_adapter_failure(
                        "builtin_json_output_schema",
                        "MCP output schema does not match the admitted Capability interface",
                    ));
                }
                let content_digest: Sha256Digest = canonical_digest(&result.value)
                    .map_err(|_| {
                        permanent_adapter_failure("builtin_json_digest", "MCP output digest failed")
                    })?
                    .parse()
                    .map_err(|_| {
                        permanent_adapter_failure("builtin_json_digest", "MCP output digest failed")
                    })?;
                let value_id = ResourceId::from_uuid_v7(ResourceKind::RunValue, Uuid::now_v7())
                    .map_err(|_| {
                        permanent_adapter_failure(
                            "builtin_json_identity",
                            "Output identity generation failed",
                        )
                    })?;
                let validation_evidence_digest: Sha256Digest =
                    canonical_digest(&serde_json::json!({
                        "content_digest": content_digest,
                        "host_evidence_digest": evidence_digest,
                        "output_mapping_digest": self.descriptor.output_mapping_digest,
                        "output_schema_digest": request.output_schema_digest,
                        "schema_version": 1,
                        "transport": "mcp",
                    }))
                    .map_err(|_| {
                        permanent_adapter_failure(
                            "builtin_json_evidence",
                            "MCP output evidence failed",
                        )
                    })?
                    .parse()
                    .map_err(|_| {
                        permanent_adapter_failure(
                            "builtin_json_evidence",
                            "MCP output evidence failed",
                        )
                    })?;
                Ok(CapabilityAdapterResponse {
                    outcome: DispatchOutcome::Completed(CapabilityOutputValue {
                        value_id,
                        classification: request.input.exact.classification,
                        schema_digest: request.output_schema_digest.clone(),
                        content_digest,
                        value: ValueRef::Inline {
                            value: result.value,
                        },
                        artifact_link_id: None,
                        validation_evidence_digest,
                    }),
                })
            }
            McpOperationOutcome::RetryableFailure(failure) => Err(CapabilityAdapterFailure {
                class: CapabilityAdapterFailureClass::RetryableAfterDispatch,
                safe_code: failure.safe_code,
                safe_message: failure.safe_message,
                evidence_digest: failure.evidence_digest,
                external_identity_digest: Some(request.idempotency_key_digest.clone()),
            }),
            McpOperationOutcome::PermanentFailure(failure) => Err(CapabilityAdapterFailure {
                class: CapabilityAdapterFailureClass::Permanent,
                safe_code: failure.safe_code,
                safe_message: failure.safe_message,
                evidence_digest: failure.evidence_digest,
                external_identity_digest: None,
            }),
            McpOperationOutcome::Uncertain {
                observation_digest,
                external_identity_digest,
            } => Ok(CapabilityAdapterResponse {
                outcome: DispatchOutcome::Uncertain(
                    insight_platform_invocations::CapabilityUncertainty {
                        observation_digest,
                        policy_path_digest: domain_digest("builtin-json-mcp-uncertain"),
                        external_identity_digest,
                        manual: true,
                    },
                ),
            }),
            McpOperationOutcome::ReauthorizationRequired { challenge_digest } => {
                Err(CapabilityAdapterFailure {
                    class: CapabilityAdapterFailureClass::Permanent,
                    safe_code: "mcp_reauthorization_required".to_owned(),
                    safe_message: "MCP authorization must be renewed".to_owned(),
                    evidence_digest: challenge_digest,
                    external_identity_digest: None,
                })
            }
            McpOperationOutcome::RemoteTask { .. } | McpOperationOutcome::InputRequired { .. } => {
                Err(permanent_adapter_failure(
                    "builtin_json_mcp_unexpected_outcome",
                    "MCP continuation outcome reached the terminal codec unexpectedly",
                ))
            }
        }
    }
}

impl BuiltinJsonGrpcCapabilityCodec {
    pub fn new(descriptor: InstalledGrpcCodecDescriptor) -> Result<Self, CapabilityAdapterFailure> {
        validate_json_codec_identity(
            &descriptor.codec_id,
            &descriptor.codec_version,
            &descriptor.module_digest,
            descriptor.worker_protocol_version,
        )?;
        if descriptor.protobuf_contract_digest != builtin_json_grpc_protobuf_contract_digest()
            || descriptor.request_mapping_digest != builtin_json_grpc_request_mapping_digest()
            || descriptor.response_mapping_digest != builtin_json_grpc_response_mapping_digest()
            || descriptor.error_mapping_digest != builtin_json_grpc_error_mapping_digest()
        {
            return Err(permanent_adapter_failure(
                "builtin_json_grpc_descriptor",
                "Built-in gRPC JSON codec descriptor is not the compiled contract",
            ));
        }
        Ok(Self { descriptor })
    }
}

impl GrpcCapabilityCodec for BuiltinJsonGrpcCapabilityCodec {
    fn descriptor(&self) -> InstalledGrpcCodecDescriptor {
        self.descriptor.clone()
    }

    fn encode(
        &self,
        request: &insight_platform_capability_adapters::CapabilityAdapterRequest,
    ) -> Result<EncodedGrpcCapabilityRequest, CapabilityAdapterFailure> {
        let CapabilityExecutionInputMaterial::Inline { value } = &request.input.material else {
            return Err(permanent_adapter_failure(
                "builtin_json_inline_only",
                "Built-in JSON codec accepts Inline input only",
            ));
        };
        Ok(EncodedGrpcCapabilityRequest {
            metadata: Vec::new(),
            message: serde_jcs::to_vec(value).map_err(|_| {
                permanent_adapter_failure(
                    "builtin_json_encode",
                    "Built-in JSON request encoding failed",
                )
            })?,
        })
    }

    fn decode(
        &self,
        request: &insight_platform_capability_adapters::CapabilityAdapterRequest,
        response: GrpcTransportResponse,
    ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure> {
        if response.status_code == 0 {
            decode_json_success(
                request,
                &response.message,
                response.transport_evidence_digest,
                "grpc",
            )
        } else {
            let class = grpc_failure_class(response.status_code);
            failure_result(
                request,
                class,
                "builtin_json_grpc_status",
                response.transport_evidence_digest,
            )
        }
    }
}

fn validate_json_codec_identity(
    codec_id: &str,
    codec_version: &str,
    module_digest: &Sha256Digest,
    worker_protocol_version: u32,
) -> Result<(), CapabilityAdapterFailure> {
    if codec_id != BUILTIN_JSON_CODEC_ID
        || codec_version != BUILTIN_JSON_CODEC_VERSION
        || module_digest != &builtin_json_codec_module_digest()
        || worker_protocol_version != WORKER_PROTOCOL_VERSION
    {
        return Err(permanent_adapter_failure(
            "builtin_json_codec_identity",
            "Built-in JSON codec identity is invalid",
        ));
    }
    Ok(())
}

pub fn install_builtin_http_json_codecs(
    registry: &mut InstalledHttpCodecRegistry,
    descriptors: impl IntoIterator<Item = InstalledHttpCodecDescriptor>,
) -> Result<(), insight_platform_capability_adapters::CapabilityDispatchError> {
    for descriptor in descriptors {
        let codec = BuiltinJsonHttpCapabilityCodec::new(descriptor).map_err(|_| {
            insight_platform_capability_adapters::CapabilityDispatchError::InvalidInstalledAdapter
        })?;
        registry.install(Arc::new(codec))?;
    }
    Ok(())
}

pub fn install_builtin_grpc_json_codecs(
    registry: &mut InstalledGrpcCodecRegistry,
    descriptors: impl IntoIterator<Item = InstalledGrpcCodecDescriptor>,
) -> Result<(), insight_platform_capability_adapters::CapabilityDispatchError> {
    for descriptor in descriptors {
        let codec = BuiltinJsonGrpcCapabilityCodec::new(descriptor).map_err(|_| {
            insight_platform_capability_adapters::CapabilityDispatchError::InvalidInstalledAdapter
        })?;
        registry.install(Arc::new(codec))?;
    }
    Ok(())
}

pub fn install_builtin_mcp_json_codecs(
    registry: &mut InstalledMcpToolCodecRegistry,
    descriptors: impl IntoIterator<Item = InstalledMcpToolCodecDescriptor>,
) -> Result<(), insight_platform_capability_adapters::CapabilityDispatchError> {
    for descriptor in descriptors {
        let codec = BuiltinJsonMcpToolCapabilityCodec::new(descriptor).map_err(|_| {
            insight_platform_capability_adapters::CapabilityDispatchError::InvalidInstalledAdapter
        })?;
        registry.install(Arc::new(codec))?;
    }
    Ok(())
}

fn http_failure_class(status: u16) -> CapabilityAdapterFailureClass {
    if status == 408 || status == 429 || status >= 500 {
        CapabilityAdapterFailureClass::RetryableAfterDispatch
    } else {
        CapabilityAdapterFailureClass::Permanent
    }
}

fn grpc_failure_class(status_code: u16) -> CapabilityAdapterFailureClass {
    match status_code {
        4 => CapabilityAdapterFailureClass::TimedOutUncertain,
        8 | 10 | 14 => CapabilityAdapterFailureClass::RetryableAfterDispatch,
        _ => CapabilityAdapterFailureClass::Permanent,
    }
}

fn decode_json_success(
    request: &insight_platform_capability_adapters::CapabilityAdapterRequest,
    bytes: &[u8],
    transport_evidence_digest: Sha256Digest,
    transport: &str,
) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure> {
    let maximum = usize::try_from(
        request
            .execution
            .implementation
            .backend_limits
            .maximum_response_bytes,
    )
    .map_err(|_| permanent_adapter_failure("builtin_json_limits", "JSON limit is invalid"))?;
    let value = parse_bounded_remote_json(bytes, maximum)?;
    let content_digest: Sha256Digest = canonical_digest(&value)
        .map_err(|_| permanent_adapter_failure("builtin_json_digest", "JSON digest failed"))?
        .parse()
        .map_err(|_| permanent_adapter_failure("builtin_json_digest", "JSON digest failed"))?;
    let value_id =
        ResourceId::from_uuid_v7(ResourceKind::RunValue, Uuid::now_v7()).map_err(|_| {
            permanent_adapter_failure("builtin_json_identity", "Output identity generation failed")
        })?;
    let validation_evidence_digest: Sha256Digest = canonical_digest(&serde_json::json!({
        "content_digest": content_digest,
        "output_schema_digest": request.output_schema_digest,
        "schema_version": 1,
        "transport": transport,
        "transport_evidence_digest": transport_evidence_digest,
    }))
    .map_err(|_| permanent_adapter_failure("builtin_json_evidence", "Evidence digest failed"))?
    .parse()
    .map_err(|_| permanent_adapter_failure("builtin_json_evidence", "Evidence digest failed"))?;
    Ok(CapabilityAdapterResponse {
        outcome: DispatchOutcome::Completed(CapabilityOutputValue {
            value_id,
            classification: request.input.exact.classification,
            schema_digest: request.output_schema_digest.clone(),
            content_digest,
            value: ValueRef::Inline { value },
            artifact_link_id: None,
            validation_evidence_digest,
        }),
    })
}

fn parse_bounded_remote_json(
    bytes: &[u8],
    maximum: usize,
) -> Result<serde_json::Value, CapabilityAdapterFailure> {
    let hard_limits = checked_in_hard_limit_profile();
    parse_strict_json(
        bytes,
        JsonLimits {
            max_bytes: maximum,
            max_depth: usize::try_from(hard_limits.api.json_depth.hard_max).map_err(|_| {
                permanent_adapter_failure("builtin_json_limits", "JSON limit is invalid")
            })?,
            max_properties_per_object: usize::try_from(hard_limits.api.json_properties.hard_max)
                .map_err(|_| {
                    permanent_adapter_failure("builtin_json_limits", "JSON limit is invalid")
                })?,
            max_items_per_array: usize::try_from(hard_limits.api.json_items.hard_max).map_err(
                |_| permanent_adapter_failure("builtin_json_limits", "JSON limit is invalid"),
            )?,
            max_string_bytes: maximum,
        },
    )
    .map_err(|_| {
        permanent_adapter_failure(
            "builtin_json_response",
            "Remote response is not bounded strict JSON",
        )
    })
}

fn failure_result(
    request: &insight_platform_capability_adapters::CapabilityAdapterRequest,
    class: CapabilityAdapterFailureClass,
    code: &str,
    transport_evidence_digest: Sha256Digest,
) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure> {
    let requires_external_identity = matches!(
        class,
        CapabilityAdapterFailureClass::RetryableAfterDispatch
            | CapabilityAdapterFailureClass::Uncertain
            | CapabilityAdapterFailureClass::TimedOutUncertain
    );
    let failure = CapabilityAdapterFailure {
        class,
        safe_code: code.to_owned(),
        safe_message: "Remote JSON Capability returned a non-success status".to_owned(),
        evidence_digest: transport_evidence_digest,
        external_identity_digest: requires_external_identity.then(|| {
            insight_platform_capability_adapters::CapabilityTransportRequestIdentity::from_adapter_request(
                    request,
                    request.execution.implementation.backend_kind,
                )
                .evidence_digest()
        }),
    };
    Err(failure)
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BuiltinEchoCapabilityAdapter;

impl BuiltinEchoCapabilityAdapter {
    pub fn installed_descriptor() -> InstalledNativeAdapter {
        InstalledNativeAdapter {
            adapter_id: BUILTIN_ECHO_ADAPTER_ID.to_owned(),
            adapter_version: BUILTIN_ECHO_ADAPTER_VERSION.to_owned(),
            module_digest: domain_digest("builtin-echo-module"),
            entrypoint_id: BUILTIN_ECHO_ENTRYPOINT_ID.to_owned(),
        }
    }
}

#[async_trait]
impl NativeCapabilityAdapter for BuiltinEchoCapabilityAdapter {
    fn descriptor(&self) -> InstalledNativeAdapter {
        Self::installed_descriptor()
    }

    async fn invoke(
        &self,
        request: &insight_platform_capability_adapters::CapabilityAdapterRequest,
    ) -> Result<CapabilityAdapterResponse, CapabilityAdapterFailure> {
        let CapabilityExecutionInputMaterial::Inline { value } = &request.input.material else {
            return Err(permanent_adapter_failure(
                "builtin_echo_inline_only",
                "Built-in echo accepts Inline input only",
            ));
        };
        let value_id =
            ResourceId::from_uuid_v7(ResourceKind::RunValue, Uuid::now_v7()).map_err(|_| {
                permanent_adapter_failure(
                    "builtin_echo_identity",
                    "Output identity generation failed",
                )
            })?;
        let validation_evidence_digest: Sha256Digest = canonical_digest(&serde_json::json!({
            "adapter": BUILTIN_ECHO_ADAPTER_ID,
            "content_digest": request.input.exact.content_digest,
            "output_schema_digest": request.output_schema_digest,
            "schema_version": 1,
        }))
        .map_err(|_| {
            permanent_adapter_failure("builtin_echo_evidence", "Output evidence generation failed")
        })?
        .parse()
        .map_err(|_| {
            permanent_adapter_failure("builtin_echo_evidence", "Output evidence generation failed")
        })?;
        Ok(CapabilityAdapterResponse {
            outcome: DispatchOutcome::Completed(CapabilityOutputValue {
                value_id,
                classification: request.input.exact.classification,
                schema_digest: request.output_schema_digest.clone(),
                content_digest: request.input.exact.content_digest.clone(),
                value: ValueRef::Inline {
                    value: value.clone(),
                },
                artifact_link_id: None,
                validation_evidence_digest,
            }),
        })
    }
}

pub fn install_builtin_native_adapters(
    registry: &mut InstalledNativeRegistry,
) -> Result<(), insight_platform_capability_adapters::CapabilityDispatchError> {
    registry.install(Arc::new(BuiltinEchoCapabilityAdapter))
}

fn domain_digest(domain: &str) -> Sha256Digest {
    let mut hasher = Sha256::new();
    hasher.update(b"insight.platform/v1/capability-worker/builtin\0");
    hasher.update(domain.as_bytes());
    format!("sha256:{}", lower_hex(&hasher.finalize()))
        .parse()
        .expect("SHA-256 digest constructed from fixed bytes is valid")
}

fn permanent_adapter_failure(code: &str, message: &str) -> CapabilityAdapterFailure {
    CapabilityAdapterFailure {
        class: CapabilityAdapterFailureClass::Permanent,
        safe_code: code.to_owned(),
        safe_message: message.to_owned(),
        evidence_digest: domain_digest(code),
        external_identity_digest: None,
    }
}

fn expected_role(work_class: WorkClass) -> Option<&'static str> {
    match work_class {
        WorkClass::CapabilityNative => Some(CAPABILITY_NATIVE_WORKER_ROLE),
        WorkClass::CapabilityRemote => Some(CAPABILITY_REMOTE_WORKER_ROLE),
        _ => None,
    }
}

pub trait CapabilityWorkerIdentityFactory: Send + Sync {
    fn new_resource_id(
        &self,
        kind: ResourceKind,
    ) -> Result<ResourceId, CapabilityWorkerIdentityError>;
    fn new_opaque_digest(&self) -> Result<Sha256Digest, CapabilityWorkerIdentityError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct UuidCapabilityWorkerIdentityFactory;

impl CapabilityWorkerIdentityFactory for UuidCapabilityWorkerIdentityFactory {
    fn new_resource_id(
        &self,
        kind: ResourceKind,
    ) -> Result<ResourceId, CapabilityWorkerIdentityError> {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7())
            .map_err(CapabilityWorkerIdentityError::ResourceId)
    }

    fn new_opaque_digest(&self) -> Result<Sha256Digest, CapabilityWorkerIdentityError> {
        let mut hasher = Sha256::new();
        hasher.update(b"insight.platform/v1/capability-worker/opaque-identity\0");
        hasher.update(Uuid::new_v4().as_bytes());
        format!("sha256:{}", lower_hex(&hasher.finalize()))
            .parse()
            .map_err(|_| CapabilityWorkerIdentityError::Digest)
    }
}

fn lower_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[derive(Debug)]
pub enum CapabilityWorkerIdentityError {
    ResourceId(ResourceIdError),
    Digest,
}

impl fmt::Display for CapabilityWorkerIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ResourceId(failure) => failure.fmt(formatter),
            Self::Digest => formatter.write_str("generated Capability Worker digest is invalid"),
        }
    }
}

impl Error for CapabilityWorkerIdentityError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityClaimFailure {
    Unavailable,
    FirstWinnerLost,
    Invariant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityClaimBinding {
    pub tenant_id: ResourceId,
    pub job_id: ResourceId,
    pub worker_process_generation_id: ResourceId,
    pub expected_job_version: u64,
    pub lease_generation: u64,
    pub lease_token_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub deadline: DateTime<Utc>,
    pub physical_attempt: u32,
    pub attempt_limit: u32,
    pub retry_backoff_milliseconds: u64,
    pub quota_reservation_entry_ids: Vec<ResourceId>,
}

pub trait ClaimedCapabilityJob: Send + 'static {
    fn claim_binding(
        &self,
        worker_manifest_digest: Sha256Digest,
    ) -> Result<CapabilityClaimBinding, CapabilityClaimFailure>;

    fn adapter_job(
        &self,
        worker_manifest_digest: Sha256Digest,
        audit: CapabilityWorkerAudit,
        quota_settlement_entry_ids: Vec<ResourceId>,
        retry_at: Option<DateTime<Utc>>,
    ) -> Result<ExecuteCapabilityAdapterJob, CapabilityClaimFailure>;
}

impl ClaimedCapabilityJob for ClaimedCapabilityExecution {
    fn claim_binding(
        &self,
        worker_manifest_digest: Sha256Digest,
    ) -> Result<CapabilityClaimBinding, CapabilityClaimFailure> {
        let execution = self
            .adapter_execution(worker_manifest_digest)
            .map_err(|_| CapabilityClaimFailure::Invariant)?;
        Ok(CapabilityClaimBinding {
            tenant_id: execution.tenant_id,
            job_id: execution.job_id,
            worker_process_generation_id: execution.worker_process_generation_id,
            expected_job_version: self.fence.expected_version,
            lease_generation: execution.lease_generation,
            lease_token_digest: self.fence.token_digest.clone(),
            request_digest: execution.admission_digest,
            deadline: execution.deadline,
            physical_attempt: execution.physical_attempt,
            attempt_limit: execution.attempt_limit,
            retry_backoff_milliseconds: self
                .invocation
                .payload
                .admission
                .retry_backoff_milliseconds,
            quota_reservation_entry_ids: self.quota_reservation_entry_ids.clone(),
        })
    }

    fn adapter_job(
        &self,
        worker_manifest_digest: Sha256Digest,
        audit: CapabilityWorkerAudit,
        quota_settlement_entry_ids: Vec<ResourceId>,
        retry_at: Option<DateTime<Utc>>,
    ) -> Result<ExecuteCapabilityAdapterJob, CapabilityClaimFailure> {
        ClaimedCapabilityExecution::adapter_job(
            self,
            worker_manifest_digest,
            audit,
            quota_settlement_entry_ids,
            retry_at,
        )
        .map_err(|_| CapabilityClaimFailure::Invariant)
    }
}

#[async_trait]
pub trait CapabilityClaimAuthority: Send + Sync + 'static {
    type Claim: ClaimedCapabilityJob;

    async fn claim_capability_jobs(
        &self,
        command: ClaimCapabilityJobs,
    ) -> Result<Vec<Self::Claim>, CapabilityClaimFailure>;

    async fn recover_expired_capability_jobs(
        &self,
        command: DriveExpiredCapabilityJobs,
    ) -> Result<usize, CapabilityClaimFailure>;
}

#[async_trait]
impl CapabilityClaimAuthority for PgRepository {
    type Claim = ClaimedCapabilityExecution;

    async fn claim_capability_jobs(
        &self,
        command: ClaimCapabilityJobs,
    ) -> Result<Vec<Self::Claim>, CapabilityClaimFailure> {
        PgRepository::claim_capability_jobs(self, command)
            .await
            .map_err(map_repository_failure)
    }

    async fn recover_expired_capability_jobs(
        &self,
        command: DriveExpiredCapabilityJobs,
    ) -> Result<usize, CapabilityClaimFailure> {
        PgRepository::drive_expired_capability_jobs(self, command)
            .await
            .map(|page| page.records.len())
            .map_err(map_repository_failure)
    }
}

fn map_repository_failure(failure: RepositoryError) -> CapabilityClaimFailure {
    match failure {
        RepositoryError::Database(_) => CapabilityClaimFailure::Unavailable,
        RepositoryError::Conflict(_)
        | RepositoryError::StaleFence
        | RepositoryError::LeaseExpired
        | RepositoryError::NotFound(_) => CapabilityClaimFailure::FirstWinnerLost,
        _ => CapabilityClaimFailure::Invariant,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityHeartbeatFailure {
    Unavailable,
    FenceLost,
    Invariant,
}

#[async_trait]
pub trait CapabilityHeartbeatAuthority: Send + Sync + 'static {
    async fn heartbeat_capability_execution(
        &self,
        tenant_id: &ResourceId,
        job_id: &ResourceId,
        fence: &JobFence,
        lease_milliseconds: u64,
    ) -> Result<JobFence, CapabilityHeartbeatFailure>;
}

#[async_trait]
impl CapabilityHeartbeatAuthority for PgRepository {
    async fn heartbeat_capability_execution(
        &self,
        tenant_id: &ResourceId,
        job_id: &ResourceId,
        fence: &JobFence,
        lease_milliseconds: u64,
    ) -> Result<JobFence, CapabilityHeartbeatFailure> {
        let record = self
            .heartbeat_job(HeartbeatJob {
                fence: RepositoryJobFence {
                    tenant_id: tenant_id.to_string(),
                    job_id: job_id.to_string(),
                    worker_id: fence.worker_process_generation_id.clone(),
                    lease_epoch: i64::try_from(fence.lease_generation)
                        .map_err(|_| CapabilityHeartbeatFailure::Invariant)?,
                    expected_job_version: i64::try_from(fence.expected_version)
                        .map_err(|_| CapabilityHeartbeatFailure::Invariant)?,
                    lease_token_digest: fence.token_digest.clone(),
                },
                lease_milliseconds: i64::try_from(lease_milliseconds)
                    .map_err(|_| CapabilityHeartbeatFailure::Invariant)?,
            })
            .await
            .map_err(|failure| match map_repository_failure(failure) {
                CapabilityClaimFailure::Unavailable => CapabilityHeartbeatFailure::Unavailable,
                CapabilityClaimFailure::FirstWinnerLost => CapabilityHeartbeatFailure::FenceLost,
                CapabilityClaimFailure::Invariant => CapabilityHeartbeatFailure::Invariant,
            })?;
        let worker_id = fence.worker_process_generation_id.to_string();
        if record.tenant_id != tenant_id.to_string()
            || record.job_id != job_id.to_string()
            || record.worker_id.as_deref() != Some(worker_id.as_str())
            || record.lease_token_digest.as_deref() != Some(fence.token_digest.as_str())
            || u64::try_from(record.lease_epoch).ok() != Some(fence.lease_generation)
            || record.state != "running"
            || u64::try_from(record.version)
                .ok()
                .is_none_or(|version| version <= fence.expected_version)
            || record.heartbeat_at.is_none()
            || record.lease_expires_at.is_none()
        {
            return Err(CapabilityHeartbeatFailure::Invariant);
        }
        Ok(JobFence {
            expected_version: u64::try_from(record.version)
                .map_err(|_| CapabilityHeartbeatFailure::Invariant)?,
            worker_process_generation_id: fence.worker_process_generation_id.clone(),
            lease_generation: fence.lease_generation,
            token_digest: fence.token_digest.clone(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapabilityExecutionDisposition {
    Settled,
    Abandoned,
}

pub struct ExecuteClaimedCapabilityJob<C> {
    pub claim: C,
    pub audit: CapabilityWorkerAudit,
    pub quota_settlement_entry_ids: Vec<ResourceId>,
    pub worker_manifest_digest: Sha256Digest,
    pub heartbeat_interval: Duration,
    pub lease_milliseconds: u64,
}

#[async_trait]
pub trait CapabilityJobCommandExecutor<C>: Send + Sync + 'static {
    async fn execute(
        &self,
        command: ExecuteClaimedCapabilityJob<C>,
    ) -> CapabilityExecutionDisposition;
}

pub struct RegisteredCapabilityJobExecutor<A, H> {
    worker: Arc<CapabilityAdapterWorker<A>>,
    heartbeat_authority: Arc<H>,
}

impl<A, H> RegisteredCapabilityJobExecutor<A, H> {
    pub fn new(worker: Arc<CapabilityAdapterWorker<A>>, heartbeat_authority: Arc<H>) -> Self {
        Self {
            worker,
            heartbeat_authority,
        }
    }
}

#[async_trait]
impl<C, A, H> CapabilityJobCommandExecutor<C> for RegisteredCapabilityJobExecutor<A, H>
where
    C: ClaimedCapabilityJob,
    A: CapabilityExecutionAuthority + Send + Sync + 'static,
    A::Error: Send + std::fmt::Debug,
    H: CapabilityHeartbeatAuthority,
{
    async fn execute(
        &self,
        command: ExecuteClaimedCapabilityJob<C>,
    ) -> CapabilityExecutionDisposition {
        let binding = match command
            .claim
            .claim_binding(command.worker_manifest_digest.clone())
        {
            Ok(binding) => binding,
            Err(_) => return CapabilityExecutionDisposition::Abandoned,
        };
        let retry_at = retry_at(&binding);
        let adapter_job = match command.claim.adapter_job(
            command.worker_manifest_digest,
            command.audit,
            command.quota_settlement_entry_ids,
            retry_at,
        ) {
            Ok(command) => command,
            Err(_) => return CapabilityExecutionDisposition::Abandoned,
        };
        let mut fence = adapter_job.fence.clone();
        let initial_fence_version = fence.expected_version;
        let prepare = self.worker.prepare(adapter_job);
        tokio::pin!(prepare);
        let mut heartbeat = tokio::time::interval_at(
            Instant::now()
                .checked_add(command.heartbeat_interval)
                .unwrap_or_else(Instant::now),
            command.heartbeat_interval,
        );
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut prepared = loop {
            tokio::select! {
                result = &mut prepare => match result {
                    Ok(prepared) => break prepared,
                    Err(failure) => {
                        eprintln!("Capability execution prepare abandoned: {failure:?}");
                        return CapabilityExecutionDisposition::Abandoned;
                    }
                },
                _ = heartbeat.tick() => match self.heartbeat_authority
                    .heartbeat_capability_execution(
                        &binding.tenant_id,
                        &binding.job_id,
                        &fence,
                        command.lease_milliseconds,
                    ).await {
                        Ok(next) => fence = next,
                        Err(CapabilityHeartbeatFailure::Unavailable) => {}
                        Err(CapabilityHeartbeatFailure::FenceLost | CapabilityHeartbeatFailure::Invariant) => {
                            return CapabilityExecutionDisposition::Abandoned;
                        }
                    }
            }
        };
        if fence.expected_version > initial_fence_version && prepared.refresh_fence(fence).is_err()
        {
            return CapabilityExecutionDisposition::Abandoned;
        }
        match self.worker.commit(prepared).await {
            Ok(_) => CapabilityExecutionDisposition::Settled,
            Err(failure) => {
                eprintln!("Capability execution commit abandoned: {failure:?}");
                CapabilityExecutionDisposition::Abandoned
            }
        }
    }
}

fn retry_at(binding: &CapabilityClaimBinding) -> Option<DateTime<Utc>> {
    if binding.physical_attempt >= binding.attempt_limit {
        return None;
    }
    let delay =
        ChronoDuration::milliseconds(i64::try_from(binding.retry_backoff_milliseconds).ok()?);
    let retry_at = Utc::now().checked_add_signed(delay)?;
    (retry_at < binding.deadline).then_some(retry_at)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapabilityWorkerDriverTiming {
    pub initial_scan_delay: Duration,
    pub safety_scan_interval: Duration,
    pub claim_failure_backoff: Duration,
    pub drain_grace: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapabilityWorkerDriverConfig {
    requested_claim_size: usize,
    recovery_batch_size: u16,
    claim_batch_hard_limit: ClaimBatchHardLimit,
    lease_milliseconds: u64,
    heartbeat_interval: Duration,
    receipt_ttl: Duration,
    timing: CapabilityWorkerDriverTiming,
}

impl CapabilityWorkerDriverConfig {
    pub fn from_profile(
        profile: &HardLimitProfile,
        receipt_ttl: Duration,
        timing: CapabilityWorkerDriverTiming,
    ) -> Result<Self, CapabilityWorkerDriverError> {
        profile
            .validate()
            .map_err(|failure| CapabilityWorkerDriverError::InvalidConfig(failure.to_string()))?;
        let config = Self {
            requested_claim_size: usize::try_from(profile.run_scheduler.claim_batch.q1_default)
                .map_err(|_| {
                    CapabilityWorkerDriverError::InvalidConfig(
                        "claim batch exceeds address space".to_owned(),
                    )
                })?,
            recovery_batch_size: u16::try_from(profile.control_data.recovery_batch.q1_default)
                .map_err(|_| {
                    CapabilityWorkerDriverError::InvalidConfig(
                        "recovery batch exceeds protocol limit".to_owned(),
                    )
                })?,
            claim_batch_hard_limit: ClaimBatchHardLimit::from_profile(profile)
                .map_err(CapabilityWorkerDriverError::LocalPool)?,
            lease_milliseconds: profile.run_scheduler.lease_milliseconds.q1_default,
            heartbeat_interval: Duration::from_millis(
                profile.run_scheduler.heartbeat_milliseconds.q1_default,
            ),
            receipt_ttl,
            timing,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(self) -> Result<(), CapabilityWorkerDriverError> {
        if self.requested_claim_size == 0
            || self.recovery_batch_size == 0
            || self.timing.initial_scan_delay > Duration::from_secs(60)
            || self.lease_milliseconds < 3
            || self.heartbeat_interval.is_zero()
            || self.heartbeat_interval >= Duration::from_millis(self.lease_milliseconds / 3)
            || self.receipt_ttl.is_zero()
            || self.receipt_ttl <= Duration::from_millis(self.lease_milliseconds)
            || ChronoDuration::from_std(self.receipt_ttl).is_err()
            || self.timing.safety_scan_interval.is_zero()
            || self.timing.claim_failure_backoff.is_zero()
            || self.timing.claim_failure_backoff > self.timing.safety_scan_interval
            || self.timing.drain_grace.is_zero()
        {
            return Err(CapabilityWorkerDriverError::InvalidConfig(
                "Capability Worker timing is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}

pub struct CapabilityWorkerDriver<S, E, I> {
    claim_authority: Arc<S>,
    executor: Arc<E>,
    identities: Arc<I>,
    pools: LocalWorkerPools,
    config: CapabilityWorkerDriverConfig,
    work_class: WorkClass,
}

impl<S, E, I> CapabilityWorkerDriver<S, E, I>
where
    S: CapabilityClaimAuthority,
    E: CapabilityJobCommandExecutor<S::Claim>,
    I: CapabilityWorkerIdentityFactory + 'static,
{
    pub fn new(
        claim_authority: Arc<S>,
        executor: Arc<E>,
        identities: Arc<I>,
        pools: LocalWorkerPools,
        config: CapabilityWorkerDriverConfig,
    ) -> Result<Self, CapabilityWorkerDriverError> {
        config.validate()?;
        let pool = pools.snapshot();
        let Some(role) = expected_role(pool.work_class) else {
            return Err(CapabilityWorkerDriverError::WrongWorkerManifest);
        };
        if pool.worker_role != role {
            return Err(CapabilityWorkerDriverError::WrongWorkerManifest);
        }
        Ok(Self {
            claim_authority,
            executor,
            identities,
            pools,
            config,
            work_class: pool.work_class,
        })
    }

    pub async fn drive_once(
        &self,
        active: &mut JoinSet<CapabilityExecutionDisposition>,
    ) -> Result<usize, CapabilityWorkerDriverError> {
        let Some(reservation) = self
            .pools
            .reserve_claim_capacity(
                self.work_class,
                self.config.requested_claim_size,
                self.config.claim_batch_hard_limit,
            )
            .map_err(CapabilityWorkerDriverError::LocalPool)?
        else {
            return Ok(0);
        };
        let claim_limit = reservation.claim_limit();
        let slots = (0..claim_limit)
            .map(|_| self.claim_slot())
            .collect::<Result<Vec<_>, _>>()?;
        let expected_tokens = slots
            .iter()
            .map(|slot| slot.lease_token_digest.clone())
            .collect::<BTreeSet<_>>();
        let claimed = self
            .claim_authority
            .claim_capability_jobs(ClaimCapabilityJobs {
                work_class: self.work_class,
                worker_process_generation_id: self.pools.snapshot().worker_process_generation_id,
                worker_manifest_digest: self.pools.snapshot().worker_manifest_digest,
                limit: u16::try_from(claim_limit)
                    .map_err(|_| CapabilityWorkerDriverError::InvalidGeneratedCommand)?,
                lease_milliseconds: self.config.lease_milliseconds,
                slots,
            })
            .await
            .map_err(CapabilityWorkerDriverError::Claim)?;
        if claimed.is_empty() {
            return Ok(0);
        }
        let pool = self.pools.snapshot();
        let bindings = validate_claims(&claimed, &pool, &expected_tokens)?;
        let permits = reservation
            .bind_claimed_jobs(
                bindings
                    .iter()
                    .map(|binding| ClaimedJobIdentity {
                        job_id: binding.job_id.clone(),
                        lease_generation: binding.lease_generation,
                    })
                    .collect(),
            )
            .map_err(CapabilityWorkerDriverError::LocalPool)?;
        if permits.len() != claimed.len() {
            return Err(CapabilityWorkerDriverError::CorruptClaim);
        }
        let count = claimed.len();
        for ((claim, binding), permit) in claimed.into_iter().zip(bindings).zip(permits) {
            let command = self.execution_command(claim, &binding)?;
            let executor = Arc::clone(&self.executor);
            active.spawn(async move {
                let _permit = permit;
                executor.execute(command).await
            });
        }
        Ok(count)
    }

    pub async fn recover_once(&self) -> Result<usize, CapabilityWorkerDriverError> {
        let slots = (0..self.config.recovery_batch_size)
            .map(|_| self.recovery_slot())
            .collect::<Result<Vec<_>, _>>()?;
        self.claim_authority
            .recover_expired_capability_jobs(DriveExpiredCapabilityJobs {
                work_class: self.work_class,
                shard: SafetyScanShard::whole(),
                after: None,
                limit: self.config.recovery_batch_size,
                slots,
            })
            .await
            .map_err(CapabilityWorkerDriverError::Claim)
    }

    fn recovery_slot(&self) -> Result<ExpiredCapabilityRecoverySlot, CapabilityWorkerDriverError> {
        Ok(ExpiredCapabilityRecoverySlot {
            quota_entry_ids: self.quota_entry_ids()?,
            event_id: self.new_id(ResourceKind::Event)?,
            outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
            failure_mutations: insight_platform_contracts::ExternalLeafFailureMutationIds {
                convergence_job_id: self.new_id(ResourceKind::Job)?,
                run_event_id: self.new_id(ResourceKind::Event)?,
                run_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
                leaf_node_event_id: self.new_id(ResourceKind::Event)?,
                leaf_node_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
                convergence_job_event_id: self.new_id(ResourceKind::Event)?,
                convergence_job_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
            },
        })
    }

    fn claim_slot(&self) -> Result<CapabilityClaimSlot, CapabilityWorkerDriverError> {
        Ok(CapabilityClaimSlot {
            lease_token_digest: self.new_digest()?,
            quota_reservation_id: self.new_id(ResourceKind::UsageReservation)?,
            quota_entry_ids: self.quota_entry_ids()?,
            event_id: self.new_id(ResourceKind::Event)?,
            outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
            resume_mutations: insight_platform_contracts::ExternalLeafResumeMutationIds {
                continuation_node_execution_id: self.new_id(ResourceKind::NodeExecution)?,
                continuation_job_id: self.new_id(ResourceKind::Job)?,
                run_event_id: self.new_id(ResourceKind::Event)?,
                run_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
                leaf_node_event_id: self.new_id(ResourceKind::Event)?,
                leaf_node_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
                continuation_node_event_id: self.new_id(ResourceKind::Event)?,
                continuation_node_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
                continuation_job_event_id: self.new_id(ResourceKind::Event)?,
                continuation_job_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
            },
            failure_mutations: insight_platform_contracts::ExternalLeafFailureMutationIds {
                convergence_job_id: self.new_id(ResourceKind::Job)?,
                run_event_id: self.new_id(ResourceKind::Event)?,
                run_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
                leaf_node_event_id: self.new_id(ResourceKind::Event)?,
                leaf_node_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
                convergence_job_event_id: self.new_id(ResourceKind::Event)?,
                convergence_job_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
            },
        })
    }

    fn execution_command(
        &self,
        claim: S::Claim,
        binding: &CapabilityClaimBinding,
    ) -> Result<ExecuteClaimedCapabilityJob<S::Claim>, CapabilityWorkerDriverError> {
        let receipt_expires_at = Utc::now()
            .checked_add_signed(
                ChronoDuration::from_std(self.config.receipt_ttl)
                    .map_err(|_| CapabilityWorkerDriverError::InvalidGeneratedCommand)?,
            )
            .ok_or(CapabilityWorkerDriverError::InvalidGeneratedCommand)?;
        let quota_settlement_entry_ids = self.quota_entry_ids()?;
        if quota_settlement_entry_ids
            .iter()
            .any(|id| binding.quota_reservation_entry_ids.contains(id))
        {
            return Err(CapabilityWorkerDriverError::InvalidGeneratedCommand);
        }
        Ok(ExecuteClaimedCapabilityJob {
            claim,
            audit: CapabilityWorkerAudit {
                tenant_id: binding.tenant_id.clone(),
                worker_process_generation_id: binding.worker_process_generation_id.clone(),
                receipt_id: self.new_id(ResourceKind::Receipt)?,
                event_id: self.new_id(ResourceKind::Event)?,
                outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
                idempotency_key_digest: self.new_digest()?,
                request_digest: binding.request_digest.clone(),
                receipt_expires_at,
            },
            quota_settlement_entry_ids,
            worker_manifest_digest: self.pools.snapshot().worker_manifest_digest,
            heartbeat_interval: self.config.heartbeat_interval,
            lease_milliseconds: self.config.lease_milliseconds,
        })
    }

    fn quota_entry_ids(&self) -> Result<Vec<ResourceId>, CapabilityWorkerDriverError> {
        let mut ids = (0..CAPABILITY_QUOTA_LINES)
            .map(|_| self.new_id(ResourceKind::QuotaLedgerEntry))
            .collect::<Result<Vec<_>, _>>()?;
        ids.sort();
        if ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(CapabilityWorkerDriverError::InvalidGeneratedCommand);
        }
        Ok(ids)
    }

    fn new_id(&self, kind: ResourceKind) -> Result<ResourceId, CapabilityWorkerDriverError> {
        self.identities
            .new_resource_id(kind)
            .map_err(CapabilityWorkerDriverError::Identity)
    }

    fn new_digest(&self) -> Result<Sha256Digest, CapabilityWorkerDriverError> {
        self.identities
            .new_opaque_digest()
            .map_err(CapabilityWorkerDriverError::Identity)
    }

    pub async fn run(
        self,
        cancellation: CancellationToken,
    ) -> Result<CapabilityWorkerCycleReport, CapabilityWorkerDriverError> {
        let mut active = JoinSet::new();
        let mut report = CapabilityWorkerCycleReport::default();
        let mut scan = tokio::time::interval_at(
            Instant::now()
                .checked_add(self.config.timing.initial_scan_delay)
                .unwrap_or_else(Instant::now),
            self.config.timing.safety_scan_interval,
        );
        scan.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                joined = active.join_next(), if !active.is_empty() => observe_join(joined, &mut report),
                _ = self.pools.wait_for_business_capacity(), if self.pools.snapshot().business_available == 0 => {}
                _ = scan.tick() => match self.recover_once().await {
                    Ok(recovered) => {
                        report.recovered = report.recovered.saturating_add(recovered as u64);
                        match self.drive_once(&mut active).await {
                            Ok(claimed) => report.claimed = report.claimed.saturating_add(claimed as u64),
                            Err(CapabilityWorkerDriverError::Claim(
                                CapabilityClaimFailure::Unavailable | CapabilityClaimFailure::FirstWinnerLost,
                            )) => {}
                            Err(failure) => return Err(failure),
                        }
                    }
                    Err(CapabilityWorkerDriverError::Claim(
                        CapabilityClaimFailure::Unavailable | CapabilityClaimFailure::FirstWinnerLost,
                    )) => tokio::select! {
                        _ = cancellation.cancelled() => break,
                        _ = tokio::time::sleep(self.config.timing.claim_failure_backoff) => {}
                    },
                    Err(failure) => return Err(failure),
                }
            }
        }
        let drain = async {
            while let Some(joined) = active.join_next().await {
                observe_join(Some(joined), &mut report);
            }
        };
        if tokio::time::timeout(self.config.timing.drain_grace, drain)
            .await
            .is_err()
        {
            return Err(CapabilityWorkerDriverError::DrainRequiresProcessTermination);
        }
        Ok(report)
    }
}

fn validate_claims<C: ClaimedCapabilityJob>(
    claimed: &[C],
    pool: &insight_platform_worker::LocalWorkerPoolSnapshot,
    expected_tokens: &BTreeSet<Sha256Digest>,
) -> Result<Vec<CapabilityClaimBinding>, CapabilityWorkerDriverError> {
    if claimed.len() > expected_tokens.len() {
        return Err(CapabilityWorkerDriverError::CorruptClaim);
    }
    let mut jobs = BTreeSet::new();
    let mut tokens = BTreeSet::new();
    let mut bindings = Vec::with_capacity(claimed.len());
    for claim in claimed {
        let binding = claim
            .claim_binding(pool.worker_manifest_digest.clone())
            .map_err(|_| CapabilityWorkerDriverError::CorruptClaim)?;
        if binding.worker_process_generation_id != pool.worker_process_generation_id
            || !expected_tokens.contains(&binding.lease_token_digest)
            || !jobs.insert(binding.job_id.clone())
            || !tokens.insert(binding.lease_token_digest.clone())
            || binding.quota_reservation_entry_ids.len() != CAPABILITY_QUOTA_LINES
            || binding
                .quota_reservation_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
        {
            return Err(CapabilityWorkerDriverError::CorruptClaim);
        }
        bindings.push(binding);
    }
    Ok(bindings)
}

fn observe_join(
    joined: Option<Result<CapabilityExecutionDisposition, tokio::task::JoinError>>,
    report: &mut CapabilityWorkerCycleReport,
) {
    match joined {
        Some(Ok(CapabilityExecutionDisposition::Settled)) => {
            report.settled = report.settled.saturating_add(1)
        }
        Some(Ok(CapabilityExecutionDisposition::Abandoned)) | Some(Err(_)) | None => {
            report.abandoned = report.abandoned.saturating_add(1)
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CapabilityWorkerCycleReport {
    pub claimed: u64,
    pub recovered: u64,
    pub settled: u64,
    pub abandoned: u64,
}

#[derive(Debug)]
pub enum CapabilityWorkerDriverError {
    InvalidConfig(String),
    WrongWorkerManifest,
    InvalidGeneratedCommand,
    CorruptClaim,
    DrainRequiresProcessTermination,
    Identity(CapabilityWorkerIdentityError),
    LocalPool(LocalWorkerPoolError),
    Claim(CapabilityClaimFailure),
}

impl fmt::Display for CapabilityWorkerDriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfig(_) => "Capability Worker driver configuration is invalid",
            Self::WrongWorkerManifest => "Capability Worker manifest is invalid",
            Self::InvalidGeneratedCommand => "Capability Worker generated an invalid command",
            Self::CorruptClaim => "Capability claim response is inconsistent",
            Self::DrainRequiresProcessTermination => {
                "Capability Worker drain grace expired; process-generation termination is required"
            }
            Self::Identity(_) => "Capability Worker identity generation failed",
            Self::LocalPool(_) => "Capability Worker local pool failed",
            Self::Claim(_) => "Capability claim authority failed",
        })
    }
}

impl Error for CapabilityWorkerDriverError {}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{
        checked_in_hard_limit_profile, WorkerManifest, WORKER_MANIFEST_VERSION,
        WORKER_PROTOCOL_VERSION,
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    fn id(kind: ResourceKind) -> ResourceId {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7()).unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn manifest(role: &str, work_class: WorkClass) -> WorkerManifest {
        WorkerManifest {
            manifest_version: WORKER_MANIFEST_VERSION,
            worker_role: role.to_owned(),
            work_class,
            adapter_runtime_digest: digest('a'),
            protocol_version: WORKER_PROTOCOL_VERSION,
            max_concurrency: 2,
            critical_control_reserved_slots: 1,
        }
    }

    fn config() -> CapabilityWorkerDriverConfig {
        CapabilityWorkerDriverConfig::from_profile(
            &checked_in_hard_limit_profile(),
            Duration::from_secs(60),
            CapabilityWorkerDriverTiming {
                initial_scan_delay: Duration::ZERO,
                safety_scan_interval: Duration::from_millis(10),
                claim_failure_backoff: Duration::from_millis(1),
                drain_grace: Duration::from_secs(1),
            },
        )
        .unwrap()
    }

    struct FakeClaim {
        binding: CapabilityClaimBinding,
    }

    impl ClaimedCapabilityJob for FakeClaim {
        fn claim_binding(
            &self,
            _worker_manifest_digest: Sha256Digest,
        ) -> Result<CapabilityClaimBinding, CapabilityClaimFailure> {
            Ok(self.binding.clone())
        }

        fn adapter_job(
            &self,
            _worker_manifest_digest: Sha256Digest,
            _audit: CapabilityWorkerAudit,
            _quota_settlement_entry_ids: Vec<ResourceId>,
            _retry_at: Option<DateTime<Utc>>,
        ) -> Result<ExecuteCapabilityAdapterJob, CapabilityClaimFailure> {
            Err(CapabilityClaimFailure::Invariant)
        }
    }

    struct FakeClaims {
        observed: Mutex<Option<ClaimCapabilityJobs>>,
        corrupt_token: bool,
    }

    #[async_trait]
    impl CapabilityClaimAuthority for FakeClaims {
        type Claim = FakeClaim;

        async fn claim_capability_jobs(
            &self,
            command: ClaimCapabilityJobs,
        ) -> Result<Vec<Self::Claim>, CapabilityClaimFailure> {
            let slot = command.slots[0].clone();
            let token = if self.corrupt_token {
                digest('f')
            } else {
                slot.lease_token_digest.clone()
            };
            let claim = FakeClaim {
                binding: CapabilityClaimBinding {
                    tenant_id: id(ResourceKind::Tenant),
                    job_id: id(ResourceKind::Job),
                    worker_process_generation_id: command.worker_process_generation_id.clone(),
                    expected_job_version: 2,
                    lease_generation: 1,
                    lease_token_digest: token,
                    request_digest: digest('b'),
                    deadline: Utc::now() + ChronoDuration::minutes(1),
                    physical_attempt: 1,
                    attempt_limit: 2,
                    retry_backoff_milliseconds: 10,
                    quota_reservation_entry_ids: slot.quota_entry_ids.clone(),
                },
            };
            *self.observed.lock().unwrap() = Some(command);
            Ok(vec![claim])
        }

        async fn recover_expired_capability_jobs(
            &self,
            _command: DriveExpiredCapabilityJobs,
        ) -> Result<usize, CapabilityClaimFailure> {
            Ok(0)
        }
    }

    #[derive(Default)]
    struct FakeExecutor {
        executions: AtomicUsize,
    }

    #[async_trait]
    impl CapabilityJobCommandExecutor<FakeClaim> for FakeExecutor {
        async fn execute(
            &self,
            command: ExecuteClaimedCapabilityJob<FakeClaim>,
        ) -> CapabilityExecutionDisposition {
            assert_eq!(command.audit.tenant_id, command.claim.binding.tenant_id);
            assert_eq!(
                command.quota_settlement_entry_ids.len(),
                CAPABILITY_QUOTA_LINES
            );
            self.executions.fetch_add(1, Ordering::SeqCst);
            CapabilityExecutionDisposition::Settled
        }
    }

    #[tokio::test]
    async fn reserves_capacity_before_claim_and_binds_exact_claim_identity() {
        let process_generation = id(ResourceKind::WorkerProcessGeneration);
        let pools = LocalWorkerPools::new(
            manifest(CAPABILITY_NATIVE_WORKER_ROLE, WorkClass::CapabilityNative),
            process_generation.clone(),
        )
        .unwrap();
        let claims = Arc::new(FakeClaims {
            observed: Mutex::new(None),
            corrupt_token: false,
        });
        let executor = Arc::new(FakeExecutor::default());
        let driver = CapabilityWorkerDriver::new(
            Arc::clone(&claims),
            Arc::clone(&executor),
            Arc::new(UuidCapabilityWorkerIdentityFactory),
            pools.clone(),
            config(),
        )
        .unwrap();
        let mut active = JoinSet::new();

        assert_eq!(driver.drive_once(&mut active).await.unwrap(), 1);
        assert_eq!(pools.snapshot().business_available, 1);
        assert_eq!(
            active.join_next().await.unwrap().unwrap(),
            CapabilityExecutionDisposition::Settled
        );
        assert_eq!(pools.snapshot().business_available, 2);
        assert_eq!(executor.executions.load(Ordering::SeqCst), 1);
        let observed = claims.observed.lock().unwrap();
        let command = observed.as_ref().unwrap();
        assert_eq!(command.work_class, WorkClass::CapabilityNative);
        assert_eq!(command.worker_process_generation_id, process_generation);
        assert_eq!(command.limit, 2);
        assert_eq!(command.slots.len(), 2);
    }

    #[tokio::test]
    async fn corrupt_claim_token_is_rejected_without_starting_execution() {
        let pools = LocalWorkerPools::new(
            manifest(CAPABILITY_REMOTE_WORKER_ROLE, WorkClass::CapabilityRemote),
            id(ResourceKind::WorkerProcessGeneration),
        )
        .unwrap();
        let executor = Arc::new(FakeExecutor::default());
        let driver = CapabilityWorkerDriver::new(
            Arc::new(FakeClaims {
                observed: Mutex::new(None),
                corrupt_token: true,
            }),
            Arc::clone(&executor),
            Arc::new(UuidCapabilityWorkerIdentityFactory),
            pools,
            config(),
        )
        .unwrap();
        let mut active = JoinSet::new();

        assert!(matches!(
            driver.drive_once(&mut active).await,
            Err(CapabilityWorkerDriverError::CorruptClaim)
        ));
        assert!(active.is_empty());
        assert_eq!(executor.executions.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn rejects_cross_lane_or_misnamed_worker_manifest() {
        let pools = LocalWorkerPools::new(
            manifest(CAPABILITY_REMOTE_WORKER_ROLE, WorkClass::CapabilityNative),
            id(ResourceKind::WorkerProcessGeneration),
        )
        .unwrap();
        let result = CapabilityWorkerDriver::new(
            Arc::new(FakeClaims {
                observed: Mutex::new(None),
                corrupt_token: false,
            }),
            Arc::new(FakeExecutor::default()),
            Arc::new(UuidCapabilityWorkerIdentityFactory),
            pools,
            config(),
        );
        assert!(matches!(
            result,
            Err(CapabilityWorkerDriverError::WrongWorkerManifest)
        ));
    }

    #[test]
    fn builtin_echo_has_one_deterministic_installed_descriptor() {
        let first = BuiltinEchoCapabilityAdapter::installed_descriptor();
        let second = BuiltinEchoCapabilityAdapter::installed_descriptor();
        assert_eq!(first, second);
        assert_eq!(first.adapter_id, BUILTIN_ECHO_ADAPTER_ID);
        assert_eq!(first.adapter_version, BUILTIN_ECHO_ADAPTER_VERSION);
        assert_eq!(first.entrypoint_id, BUILTIN_ECHO_ENTRYPOINT_ID);
        let mut registry = InstalledNativeRegistry::default();
        install_builtin_native_adapters(&mut registry).unwrap();
        assert!(install_builtin_native_adapters(&mut registry).is_err());
    }

    fn http_json_descriptor() -> InstalledHttpCodecDescriptor {
        InstalledHttpCodecDescriptor {
            codec_id: BUILTIN_JSON_CODEC_ID.to_owned(),
            codec_version: BUILTIN_JSON_CODEC_VERSION.to_owned(),
            module_digest: builtin_json_codec_module_digest(),
            worker_protocol_version: WORKER_PROTOCOL_VERSION,
            descriptor_digest: digest('1'),
            protocol_contract_digest: builtin_json_http_protocol_contract_digest(),
            request_mapping_digest: builtin_json_http_request_mapping_digest(),
            response_mapping_digest: builtin_json_http_response_mapping_digest(),
            error_mapping_digest: builtin_json_http_error_mapping_digest(),
        }
    }

    fn grpc_json_descriptor() -> InstalledGrpcCodecDescriptor {
        InstalledGrpcCodecDescriptor {
            codec_id: BUILTIN_JSON_CODEC_ID.to_owned(),
            codec_version: BUILTIN_JSON_CODEC_VERSION.to_owned(),
            module_digest: builtin_json_codec_module_digest(),
            worker_protocol_version: WORKER_PROTOCOL_VERSION,
            descriptor_digest: digest('6'),
            protobuf_contract_digest: builtin_json_grpc_protobuf_contract_digest(),
            service_name: "insight.fixture.v1.Lookup".to_owned(),
            method_name: "Get".to_owned(),
            request_mapping_digest: builtin_json_grpc_request_mapping_digest(),
            response_mapping_digest: builtin_json_grpc_response_mapping_digest(),
            error_mapping_digest: builtin_json_grpc_error_mapping_digest(),
        }
    }

    fn mcp_json_descriptor() -> InstalledMcpToolCodecDescriptor {
        InstalledMcpToolCodecDescriptor {
            codec_id: BUILTIN_JSON_CODEC_ID.to_owned(),
            codec_version: BUILTIN_JSON_CODEC_VERSION.to_owned(),
            module_digest: builtin_json_codec_module_digest(),
            worker_protocol_version: WORKER_PROTOCOL_VERSION,
            descriptor_digest: digest('b'),
            remote_tool_name: "fixture_lookup".to_owned(),
            remote_input_schema_digest: digest('c'),
            output_mapping_digest: builtin_json_mcp_output_mapping_digest(),
            protocol_profile_id: id(ResourceKind::PolicyRevision),
            protocol_profile_digest: digest('e'),
            discovery_semantic_evidence_digest: digest('f'),
        }
    }

    #[test]
    fn builtin_json_codecs_require_exact_installed_identity_and_reject_duplicates() {
        let mut wrong = http_json_descriptor();
        wrong.module_digest = digest('f');
        assert!(BuiltinJsonHttpCapabilityCodec::new(wrong).is_err());

        let mut wrong_mapping = http_json_descriptor();
        wrong_mapping.response_mapping_digest = digest('f');
        assert!(BuiltinJsonHttpCapabilityCodec::new(wrong_mapping).is_err());

        let mut wrong_protocol = grpc_json_descriptor();
        wrong_protocol.protobuf_contract_digest = digest('f');
        assert!(BuiltinJsonGrpcCapabilityCodec::new(wrong_protocol).is_err());

        let mut wrong_mcp_mapping = mcp_json_descriptor();
        wrong_mcp_mapping.output_mapping_digest = digest('f');
        assert!(BuiltinJsonMcpToolCapabilityCodec::new(wrong_mcp_mapping).is_err());

        let mut http = InstalledHttpCodecRegistry::default();
        install_builtin_http_json_codecs(&mut http, [http_json_descriptor()]).unwrap();
        assert!(install_builtin_http_json_codecs(&mut http, [http_json_descriptor()]).is_err());

        let mut grpc = InstalledGrpcCodecRegistry::default();
        install_builtin_grpc_json_codecs(&mut grpc, [grpc_json_descriptor()]).unwrap();
        assert!(install_builtin_grpc_json_codecs(&mut grpc, [grpc_json_descriptor()]).is_err());

        let mut mcp = InstalledMcpToolCodecRegistry::default();
        let descriptor = mcp_json_descriptor();
        install_builtin_mcp_json_codecs(&mut mcp, [descriptor.clone()]).unwrap();
        assert!(install_builtin_mcp_json_codecs(&mut mcp, [descriptor]).is_err());
    }

    #[test]
    fn builtin_json_parser_is_strict_and_bounded_by_reviewed_limits() {
        assert_eq!(
            parse_bounded_remote_json(br#"{"b":2,"a":1}"#, 64).unwrap(),
            serde_json::json!({"a": 1, "b": 2})
        );
        assert!(parse_bounded_remote_json(br#"{} trailing"#, 64).is_err());
        assert!(parse_bounded_remote_json(br#"{"too_large":true}"#, 4).is_err());

        let profile = checked_in_hard_limit_profile();
        let excessive_depth = format!(
            "{}0{}",
            "[".repeat(usize::try_from(profile.api.json_depth.hard_max).unwrap() + 1),
            "]".repeat(usize::try_from(profile.api.json_depth.hard_max).unwrap() + 1)
        );
        assert!(parse_bounded_remote_json(excessive_depth.as_bytes(), 1_024).is_err());
    }

    #[test]
    fn builtin_json_status_mapping_preserves_uncertainty_classes_for_shared_policy() {
        assert_eq!(
            http_failure_class(503),
            CapabilityAdapterFailureClass::RetryableAfterDispatch
        );
        assert_eq!(
            http_failure_class(404),
            CapabilityAdapterFailureClass::Permanent
        );
        assert_eq!(
            grpc_failure_class(4),
            CapabilityAdapterFailureClass::TimedOutUncertain
        );
        assert_eq!(
            grpc_failure_class(14),
            CapabilityAdapterFailureClass::RetryableAfterDispatch
        );
    }
}
