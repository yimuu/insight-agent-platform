//! Closed configuration fragments for the additive local `full` development profile.
//!
//! This module deliberately contains only product-facing process configuration. It does not
//! connect to an authority or execute worker logic; each generated document is consumed and
//! revalidated by the corresponding independent Platform process.

use super::DevProfile;
use insight_platform_contracts::limits::MAX_EGRESS_METADATA_BYTES_HARD;
use insight_platform_contracts::{
    builtin_json_codec_module_digest, builtin_json_grpc_error_mapping_digest,
    builtin_json_grpc_protobuf_contract_digest, builtin_json_grpc_request_mapping_digest,
    builtin_json_grpc_response_mapping_digest, builtin_json_http_error_mapping_digest,
    builtin_json_http_protocol_contract_digest, builtin_json_http_request_mapping_digest,
    builtin_json_http_response_mapping_digest, builtin_json_mcp_output_mapping_digest,
    canonical_digest, CapabilityBackendContract, GrpcCapabilityContract, HttpCapabilityContract,
    HttpCapabilityMethod, McpToolCapabilityContract, ResourceId, BUILTIN_JSON_CODEC_ID,
    BUILTIN_JSON_CODEC_VERSION,
};
pub use insight_platform_contracts::{
    CAPABILITY_WORKER_WORKLOAD_IDENTITY, CONTEXT_DATASET_WORKER_WORKLOAD_IDENTITY,
    CONTEXT_WORKER_WORKLOAD_IDENTITY, EGRESS_BROKER_WORKLOAD_IDENTITY,
    MCP_CALLBACK_WORKLOAD_IDENTITY, MCP_CLEANUP_WORKER_WORKLOAD_IDENTITY,
    MCP_DISCOVERY_WORKER_WORKLOAD_IDENTITY, MCP_HOST_WORKLOAD_IDENTITY,
    MCP_SUBSCRIPTION_WORKER_WORKLOAD_IDENTITY, MODEL_WORKER_WORKLOAD_IDENTITY,
};
use insight_platform_deployment_contracts::installation::{
    DatabaseEndpointV1, InstallationError, InstallationProcess as Process, InstallationTopology,
    NetworkTopologyV1, ProcessNetworkV1, ServiceOrigin,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

pub const GATEWAY_EGRESS_CLIENT_CERTIFICATE_FILE: &str = "gateway-egress-client.pem";
pub const GATEWAY_EGRESS_CLIENT_PRIVATE_KEY_FILE: &str = "gateway-egress-client-key.pem";
pub const CONTEXT_NATIVE_CONFIG_FILE: &str = "context-native.json";
pub const CONTEXT_DATASET_CONFIG_FILE: &str = "context-dataset-worker.json";
pub const ARTIFACT_MAINTENANCE_CONFIG_FILE: &str = "artifact-maintenance.json";
pub const SECURITY_AUTHORITY_CONFIG_FILE: &str = "security-authority.json";
pub const EGRESS_BROKER_CONFIG_FILE: &str = "egress-broker.json";
pub const MODEL_WORKER_CONFIG_FILE: &str = "model-worker.json";
pub const CONTEXT_REMOTE_CONFIG_FILE: &str = "context-remote.json";
pub const MCP_HOST_CONFIG_FILE: &str = "mcp-host.json";
pub const MCP_RESOURCE_HOST_CONFIG_FILE: &str = "mcp-resource-host.json";
pub const CAPABILITY_REMOTE_CONFIG_FILE: &str = "capability-remote.json";
pub const MCP_DISCOVERY_CONFIG_FILE: &str = "mcp-discovery-worker.json";
pub const MCP_SUBSCRIPTION_CONFIG_FILE: &str = "mcp-subscription-worker.json";
pub const MCP_CLEANUP_CONFIG_FILE: &str = "mcp-cleanup-worker.json";
pub const CONTEXT_SUBSCRIPTION_CONFIG_FILE: &str = "subscription-context-worker.json";
pub const CALLBACK_API_CONFIG_FILE: &str = "callback-api.json";
pub const SECURITY_AUTHORITY_CERTIFICATE_FILE: &str = "security-authority.pem";
pub const SECURITY_AUTHORITY_PRIVATE_KEY_FILE: &str = "security-authority-key.pem";
pub const EGRESS_BROKER_CLIENT_CERTIFICATE_FILE: &str = "egress-broker-client.pem";
pub const EGRESS_BROKER_CLIENT_PRIVATE_KEY_FILE: &str = "egress-broker-client-key.pem";
pub const EGRESS_BROKER_CERTIFICATE_FILE: &str = "egress-broker.pem";
pub const EGRESS_BROKER_PRIVATE_KEY_FILE: &str = "egress-broker-key.pem";
pub const MCP_STATE_KEY_DIRECTORY: &str = "mcp-state-keys";
pub const MCP_STATE_KEY_FILE: &str = "current";
pub const MCP_OAUTH_STATE_KEY_DIRECTORY: &str = "mcp-oauth-state-keys";
pub const MCP_OAUTH_STATE_KEY_FILE: &str = "current";
pub const MODEL_WORKER_CLIENT_CERTIFICATE_FILE: &str = "model-worker-client.pem";
pub const MODEL_WORKER_CLIENT_PRIVATE_KEY_FILE: &str = "model-worker-client-key.pem";
pub const CONTEXT_WORKER_CLIENT_CERTIFICATE_FILE: &str = "context-worker-client.pem";
pub const CONTEXT_WORKER_CLIENT_PRIVATE_KEY_FILE: &str = "context-worker-client-key.pem";
pub const CONTEXT_DATASET_CLIENT_CERTIFICATE_FILE: &str = "context-dataset-client.pem";
pub const CONTEXT_DATASET_CLIENT_PRIVATE_KEY_FILE: &str = "context-dataset-client-key.pem";
pub const MCP_HOST_CERTIFICATE_FILE: &str = "mcp-host.pem";
pub const MCP_HOST_PRIVATE_KEY_FILE: &str = "mcp-host-key.pem";
pub const MCP_RESOURCE_HOST_CERTIFICATE_FILE: &str = "mcp-resource-host.pem";
pub const MCP_RESOURCE_HOST_PRIVATE_KEY_FILE: &str = "mcp-resource-host-key.pem";
pub const MCP_HOST_EGRESS_CLIENT_CERTIFICATE_FILE: &str = "mcp-host-egress-client.pem";
pub const MCP_HOST_EGRESS_CLIENT_PRIVATE_KEY_FILE: &str = "mcp-host-egress-client-key.pem";
pub const MCP_RESOURCE_EGRESS_CLIENT_CERTIFICATE_FILE: &str = "mcp-resource-egress-client.pem";
pub const MCP_RESOURCE_EGRESS_CLIENT_PRIVATE_KEY_FILE: &str = "mcp-resource-egress-client-key.pem";
pub const CAPABILITY_REMOTE_CLIENT_CERTIFICATE_FILE: &str = "capability-remote-client.pem";
pub const CAPABILITY_REMOTE_CLIENT_PRIVATE_KEY_FILE: &str = "capability-remote-client-key.pem";
pub const MCP_DISCOVERY_CLIENT_CERTIFICATE_FILE: &str = "mcp-discovery-client.pem";
pub const MCP_DISCOVERY_CLIENT_PRIVATE_KEY_FILE: &str = "mcp-discovery-client-key.pem";
pub const MCP_SUBSCRIPTION_CLIENT_CERTIFICATE_FILE: &str = "mcp-subscription-client.pem";
pub const MCP_SUBSCRIPTION_CLIENT_PRIVATE_KEY_FILE: &str = "mcp-subscription-client-key.pem";
pub const MCP_CLEANUP_CLIENT_CERTIFICATE_FILE: &str = "mcp-cleanup-client.pem";
pub const MCP_CLEANUP_CLIENT_PRIVATE_KEY_FILE: &str = "mcp-cleanup-client-key.pem";
pub const CONTEXT_SUBSCRIPTION_CLIENT_CERTIFICATE_FILE: &str = "context-subscription-client.pem";
pub const CONTEXT_SUBSCRIPTION_CLIENT_PRIVATE_KEY_FILE: &str =
    "context-subscription-client-key.pem";
pub const CALLBACK_CLIENT_CERTIFICATE_FILE: &str = "callback-client.pem";
pub const CALLBACK_CLIENT_PRIVATE_KEY_FILE: &str = "callback-client-key.pem";
pub const INITIAL_BINARY_NAMES: [&str; 15] = [
    "platform-context-worker",
    "platform-artifact-maintenance",
    "platform-security-authority",
    "platform-egress-broker",
    "platform-model-worker",
    "platform-remote-context-worker",
    "platform-mcp-host",
    "platform-mcp-resource-host",
    "platform-capability-remote-worker",
    "platform-mcp-discovery-worker",
    "platform-mcp-subscription-worker",
    "platform-mcp-cleanup-worker",
    "platform-subscription-context-worker",
    "platform-callback-api",
    "platform-context-dataset-worker",
];

pub struct ProcessLaunch {
    pub role: &'static str,
    pub binary: PathBuf,
    pub ready_address: String,
    pub environment: Vec<(&'static str, String)>,
    pub extra_environment: Vec<(String, String)>,
}

pub struct ProcessPaths<'a> {
    pub release: &'a Path,
    pub configuration: &'a Path,
    pub tls: &'a Path,
    pub ca_certificate_file: &'a str,
    pub nats_client_certificate_file: &'a str,
    pub nats_client_private_key_file: &'a str,
}

pub struct EgressConfigInputs<'a> {
    pub model_installation: Option<&'a insight_platform_contracts::ModelInstallationCatalogV2>,
    pub remote_context_destinations:
        &'a [insight_platform_contracts::InstalledRemoteContextDestinationV1],
    pub service_principal_id: &'a str,
    pub secret_provider_catalog: &'a Value,
    pub mcp_state_key_root: &'a Path,
    pub mcp_state_key_path: &'a Path,
    pub mcp_state_key_reference_digest: &'a str,
    pub mcp_oauth_state_key_root: &'a Path,
    pub mcp_oauth_state_key_path: &'a Path,
    pub mcp_oauth_state_key_reference_digest: &'a str,
}

pub struct WorkerDigests<'a> {
    pub context_adapter: &'a str,
    pub context_contract: &'a str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PortBindings {
    pub context_native_observability: u16,
    pub artifact_maintenance_observability: u16,
    pub security_authority: u16,
    pub security_authority_observability: u16,
    pub egress_broker: u16,
    pub egress_broker_observability: u16,
    pub model_worker_observability: u16,
    pub remote_context_worker_observability: u16,
    pub mcp_host: u16,
    pub mcp_host_observability: u16,
    pub mcp_resource_host: u16,
    pub mcp_resource_host_observability: u16,
    pub capability_remote_observability: u16,
    pub mcp_discovery_observability: u16,
    pub mcp_subscription_observability: u16,
    pub mcp_cleanup_observability: u16,
    pub context_subscription_observability: u16,
    pub callback_api: u16,
    pub context_dataset_observability: u16,
    pub outbox_observability: u16,
    pub history_observability: u16,
}

impl PortBindings {
    pub fn allocate<E>(next: &mut impl FnMut() -> Result<u16, E>) -> Result<Self, E> {
        Ok(Self {
            context_native_observability: next()?,
            artifact_maintenance_observability: next()?,
            security_authority: next()?,
            security_authority_observability: next()?,
            egress_broker: next()?,
            egress_broker_observability: next()?,
            model_worker_observability: next()?,
            remote_context_worker_observability: next()?,
            mcp_host: next()?,
            mcp_host_observability: next()?,
            mcp_resource_host: next()?,
            mcp_resource_host_observability: next()?,
            capability_remote_observability: next()?,
            mcp_discovery_observability: next()?,
            mcp_subscription_observability: next()?,
            mcp_cleanup_observability: next()?,
            context_subscription_observability: next()?,
            callback_api: next()?,
            context_dataset_observability: next()?,
            outbox_observability: next()?,
            history_observability: next()?,
        })
    }

    #[cfg(test)]
    pub const fn static_test_ports() -> Self {
        Self {
            context_native_observability: 19_095,
            artifact_maintenance_observability: 19_096,
            security_authority: 19_097,
            security_authority_observability: 19_098,
            egress_broker: 19_099,
            egress_broker_observability: 19_100,
            model_worker_observability: 19_101,
            remote_context_worker_observability: 19_102,
            mcp_host: 19_103,
            mcp_host_observability: 19_104,
            mcp_resource_host: 19_105,
            mcp_resource_host_observability: 19_106,
            capability_remote_observability: 19_107,
            mcp_discovery_observability: 19_108,
            mcp_subscription_observability: 19_109,
            mcp_cleanup_observability: 19_110,
            context_subscription_observability: 19_111,
            callback_api: 19_112,
            context_dataset_observability: 19_113,
            outbox_observability: 19114,
            history_observability: 19115,
        }
    }
}

pub fn initial_configs(
    worker_builds: &crate::worker_profile::WorkerBuilds,
    network: &NetworkTopologyV1,
    artifact_provider_catalog: &Value,
    capability_protocol_profile_revision_id: Option<&ResourceId>,
    digests: WorkerDigests<'_>,
    egress: EgressConfigInputs<'_>,
) -> Result<BTreeMap<String, (&'static str, Value)>, InstallationError> {
    network.validate()?;
    if egress
        .model_installation
        .is_some_and(|catalog| !catalog.validate())
    {
        return Err(InstallationError::InvalidInput);
    }
    let model_manifest = crate::worker_profile::model_manifest(worker_builds);
    let model_manifest_digest = model_manifest
        .as_ref()
        .map(canonical_digest)
        .transpose()
        .expect("the closed local Model worker manifest is canonical JSON");
    let capability_http_contract = CapabilityBackendContract::Http(HttpCapabilityContract {
        method: HttpCapabilityMethod::Post,
        protocol_contract_digest: builtin_json_http_protocol_contract_digest(),
        request_mapping_digest: builtin_json_http_request_mapping_digest(),
        response_mapping_digest: builtin_json_http_response_mapping_digest(),
        error_mapping_digest: builtin_json_http_error_mapping_digest(),
        idempotency_header: None,
    });
    let capability_http_codecs = vec![json!({
        "codec_id": BUILTIN_JSON_CODEC_ID,
        "codec_version": BUILTIN_JSON_CODEC_VERSION,
        "module_digest": builtin_json_codec_module_digest(),
        "worker_protocol_version": 1,
        "descriptor_digest": capability_http_contract.descriptor_digest()
            .expect("the built-in HTTP Capability descriptor is canonical"),
        "protocol_contract_digest": builtin_json_http_protocol_contract_digest(),
        "request_mapping_digest": builtin_json_http_request_mapping_digest(),
        "response_mapping_digest": builtin_json_http_response_mapping_digest(),
        "error_mapping_digest": builtin_json_http_error_mapping_digest(),
    })];
    let capability_grpc_contract = CapabilityBackendContract::Grpc(GrpcCapabilityContract {
        protobuf_contract_digest: builtin_json_grpc_protobuf_contract_digest(),
        service_name: "insight.fixture.v1.Lookup".to_owned(),
        method_name: "Get".to_owned(),
        request_mapping_digest: builtin_json_grpc_request_mapping_digest(),
        response_mapping_digest: builtin_json_grpc_response_mapping_digest(),
        error_mapping_digest: builtin_json_grpc_error_mapping_digest(),
        idempotency_metadata_key: None,
    });
    let capability_grpc_codecs = vec![json!({
        "codec_id": BUILTIN_JSON_CODEC_ID,
        "codec_version": BUILTIN_JSON_CODEC_VERSION,
        "module_digest": builtin_json_codec_module_digest(),
        "worker_protocol_version": 1,
        "descriptor_digest": capability_grpc_contract.descriptor_digest()
            .expect("the built-in gRPC Capability descriptor is canonical"),
        "protobuf_contract_digest": builtin_json_grpc_protobuf_contract_digest(),
        "service_name": "insight.fixture.v1.Lookup",
        "method_name": "Get",
        "request_mapping_digest": builtin_json_grpc_request_mapping_digest(),
        "response_mapping_digest": builtin_json_grpc_response_mapping_digest(),
        "error_mapping_digest": builtin_json_grpc_error_mapping_digest(),
    })];
    let capability_mcp_codecs = if capability_protocol_profile_revision_id.is_some() {
        let capability_mcp_contract = McpToolCapabilityContract {
            remote_tool_name: "fixture_lookup".to_owned(),
            remote_input_schema_digest: closed_local_digest("capability-mcp-input-schema")
                .parse()
                .expect("the built-in MCP input schema digest is valid"),
            output_mapping_digest: builtin_json_mcp_output_mapping_digest(),
            protocol_profile: insight_platform_contracts::ExactVersionRef::new(
                capability_protocol_profile_revision_id
                    .ok_or(InstallationError::InvalidInput)?
                    .clone(),
                closed_local_digest("capability-mcp-protocol-profile")
                    .parse()
                    .expect("the built-in MCP protocol profile digest is valid"),
            )
            .expect("the built-in MCP protocol profile is exact"),
            discovery_semantic_evidence_digest: closed_local_digest("capability-mcp-discovery")
                .parse()
                .expect("the built-in MCP discovery digest is valid"),
            supports_task: false,
            supports_progress: true,
        };
        let capability_mcp_backend =
            CapabilityBackendContract::Mcp(capability_mcp_contract.clone());
        vec![json!({
            "codec_id": BUILTIN_JSON_CODEC_ID,
            "codec_version": BUILTIN_JSON_CODEC_VERSION,
            "module_digest": builtin_json_codec_module_digest(),
            "worker_protocol_version": 1,
            "descriptor_digest": capability_mcp_backend.descriptor_digest()
                .expect("the built-in MCP Capability descriptor is canonical"),
            "remote_tool_name": capability_mcp_contract.remote_tool_name,
            "remote_input_schema_digest": capability_mcp_contract.remote_input_schema_digest,
            "output_mapping_digest": capability_mcp_contract.output_mapping_digest,
            "protocol_profile_id": capability_mcp_contract.protocol_profile.revision_id,
            "protocol_profile_digest": capability_mcp_contract.protocol_profile.semantic_digest,
            "discovery_semantic_evidence_digest": capability_mcp_contract.discovery_semantic_evidence_digest,
        })]
    } else {
        Vec::new()
    };
    let capability_closure = json!({
        "schema_version": 1,
        "http": capability_http_codecs,
        "grpc": capability_grpc_codecs,
        "mcp": capability_mcp_codecs,
    });
    let capability_adapter_digest = canonical_digest(&capability_closure)
        .expect("the closed local remote Capability codec closure is canonical JSON");
    let dataset_content = json!("local deterministic context item");
    let dataset_index = json!({
        "items": [{"content": dataset_content.clone(), "ordinal": 0}],
        "schema_version": 1,
    });
    let dataset_source_manifest_digest = canonical_digest(&dataset_index)
        .expect("the closed local Context Dataset source is canonical JSON");
    let context_worker_manifest = worker_builds.manifest(
        "platform-context-worker",
        "context-worker",
        insight_platform_contracts::WorkClass::Context,
        digests.context_adapter,
        (4, 1),
        crate::worker_profile::native_context(digests.context_contract, digests.context_adapter),
    );
    let context_worker_manifest_digest = context_worker_manifest
        .as_ref()
        .map(canonical_digest)
        .transpose()
        .expect("the closed local Context Worker manifest is canonical JSON");
    let dataset_source_binding_value = json!({
        "adapter_contract_digest": digests.context_contract,
        "installed_adapter_digest": digests.context_adapter,
        "required_worker_manifest_digest": context_worker_manifest_digest,
        "schema_version": 1,
    });
    let dataset_source_binding_digest = canonical_digest(&dataset_source_binding_value)
        .expect("the closed local Context Dataset source binding is canonical JSON");
    let mut configurations = BTreeMap::new();
    if network.process(Process::ContextNative).is_ok() {
        configurations.insert(
            "context-native".to_owned(),
            (
                CONTEXT_NATIVE_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "observability_listen_address": network.observability(Process::ContextNative)?,
                    "worker_manifest": context_worker_manifest,
                    "native_catalog": {
                        "schema_version": 1,
                        "installed_adapter_digest": digests.context_adapter,
                        "adapter_contract_digest": digests.context_contract,
                        "source_item_identity_digest": digests.context_contract,
                        "content": "local deterministic context item",
                        "structured_fields_schema_digest": digests.context_contract,
                        "score_millionths": 500000,
                        "locator_digest": digests.context_contract,
                        "authorization_evidence_digest": digests.context_contract,
                        "ranking_evidence_digest": digests.context_contract,
                        "display_label": "Local deterministic context",
                        "classification": "internal",
                    },
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "receipt_ttl_seconds": 3600,
                    "scan_interval_milliseconds": 250,
                    "failure_backoff_milliseconds": 100,
                    "drain_grace_milliseconds": 30000,
                }),
            ),
        );
    }
    if network.process(Process::ArtifactMaintenance).is_ok() {
        configurations.insert(
            "artifact-maintenance".to_owned(),
            (
                ARTIFACT_MAINTENANCE_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "listen_address": network.observability(Process::ArtifactMaintenance)?,
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "artifact_provider_catalog": artifact_provider_catalog,
                    "broker": {
                        "maximum_in_flight": 8,
                        "maximum_read_bytes": 67108864,
                        "operation_timeout_milliseconds": 5000,
                    },
                    "worker": {
                        "worker_manifest": worker_builds.manifest("platform-artifact-maintenance", "artifact-maintenance", insight_platform_contracts::WorkClass::Artifact, &closed_local_digest("artifact-maintenance-runtime"), (4, 1), insight_platform_artifacts::execution::maintenance_execution_capabilities()),
                        "claim_batch": 4,
                        "lease_milliseconds": 120000,
                        "receipt_ttl_milliseconds": 3600000,
                        "poll_milliseconds": 250,
                    },
                    "shutdown_grace_milliseconds": 30000,
                }),
            ),
        );
    }
    if network.process(Process::SecurityAuthority).is_ok() {
        configurations.insert(
            "security-authority".to_owned(),
            (
                SECURITY_AUTHORITY_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "listen_address": network.listen(Process::SecurityAuthority)?,
                    "observability_listen_address": network.observability(Process::SecurityAuthority)?,
                    "service_principal_id": egress.service_principal_id,
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "maximum_rpc_message_bytes": 65536,
                    "tls_handshake_timeout_milliseconds": 5000,
                    "shutdown_grace_milliseconds": 30000,
                }),
            ),
        );
    }
    if network.process(Process::EgressBroker).is_ok() {
        configurations.insert(
            "egress-broker".to_owned(),
            (
                EGRESS_BROKER_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "listen_address": network.listen(Process::EgressBroker)?,
                    "observability_listen_address": network.observability(Process::EgressBroker)?,
                    "security_authority_endpoint": network.origin(Process::SecurityAuthority)?.as_str(),
                    "security_authority_tls_server_name": network.tls_server_name(Process::SecurityAuthority)?,
                    "maximum_rpc_metadata_bytes": MAX_EGRESS_METADATA_BYTES_HARD,
                    "maximum_rpc_payload_bytes": 1048576,
                    "security_authority_maximum_rpc_message_bytes": 65536,
                    "connect_timeout_milliseconds": 5000,
                    "request_timeout_milliseconds": 30000,
                    "tls_handshake_timeout_milliseconds": 5000,
                    "shutdown_grace_milliseconds": 30000,
                    "secret_broker": {
                        "maximum_in_flight": 16,
                        "maximum_material_bytes": 16384,
                        "resolution_timeout_milliseconds": 5000,
                    },
                    "model_limits": common_egress_limits(16),
                    "capability_http_limits": common_egress_limits(16),
                    "capability_grpc_limits": common_egress_limits(16),
                    "remote_context_limits": {
                        "maximum_in_flight": 16,
                        "maximum_dns_answers": 16,
                        "maximum_secret_material_bytes": 8192,
                        "connect_timeout_milliseconds": 5000,
                        "first_byte_timeout_milliseconds": 15000,
                        "idle_timeout_milliseconds": 10000,
                    },
                    "mcp_oauth_limits": {
                        "maximum_in_flight": 16,
                        "maximum_dns_answers": 16,
                        "maximum_secret_material_bytes": 8192,
                        "maximum_token_lifetime_seconds": 86400,
                    },
                    "mcp_oauth_service_principal_id": egress.service_principal_id,
                    "mcp_oauth_verification_bindings": [],
                    "mcp_streamable_http_limits": {
                        "maximum_in_flight": 16,
                        "maximum_dns_answers": 16,
                        "maximum_secret_material_bytes": 8192,
                        "maximum_subscription_reconnects": 16,
                        "maximum_subscription_events_per_session": 10000,
                    },
                    "mcp_streamable_http_endpoints": [],
                    "mcp_state_keys": {
                        "active_key_id": "local-current",
                        "projected_secret_root": egress.mcp_state_key_root.display().to_string(),
                        "keys": [{
                            "key_id": "local-current",
                            "key_reference_digest": egress.mcp_state_key_reference_digest,
                            "key_material_path": egress.mcp_state_key_path.display().to_string(),
                        }],
                    },
                    "mcp_subscription_bridge": {
                        "maximum_pending": 16,
                        "maximum_active": 64,
                        "event_buffer_capacity": 16,
                    },
                    "secret_provider_catalog": egress.secret_provider_catalog,
                    "model_egress": egress.model_installation.map(|catalog| insight_platform_contracts::ModelEgressRoutingV1::PublicHttps { grant: Box::new(catalog.public_egress()) }).unwrap_or(insight_platform_contracts::ModelEgressRoutingV1::Fixed { destinations: vec![] }),
                    "capability_http_endpoints": [],
                    "capability_grpc_endpoints": [],
                    "remote_context_destinations": egress.remote_context_destinations,
                }),
            ),
        );
    }
    if network.process(Process::ModelWorker).is_ok() {
        configurations.insert(
            "model-worker".to_owned(),
            (
                MODEL_WORKER_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "observability_listen_address": network.observability(Process::ModelWorker)?,
                    "worker_manifest": model_manifest,
                    "installed_adapters": [
                        {
                            "qualified_name": "anthropic.messages/2023-06-01",
                            "worker_manifest_digest": model_manifest_digest,
                            "adapter_contract_digest": insight_platform_contracts::ModelProviderWireProtocol::AnthropicMessages.adapter_contract_digest(),
                        },
                        {
                            "qualified_name": "openai.responses/v1",
                            "worker_manifest_digest": model_manifest_digest,
                            "adapter_contract_digest": insight_platform_contracts::ModelProviderWireProtocol::OpenAiResponses.adapter_contract_digest(),
                        }
                    ],
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "egress_endpoint": network.endpoint(Process::EgressBroker)?,
                    "egress_tls_server_name": network.tls_server_name(Process::EgressBroker)?,
                    "egress_connect_timeout_milliseconds": 5000,
                    "egress_request_timeout_milliseconds": 30000,
                    "maximum_rpc_metadata_bytes": MAX_EGRESS_METADATA_BYTES_HARD,
                    "maximum_rpc_payload_bytes": 1048576,
                    "live_delta": {
                        "servers": [format!("tls://{}:{}", network.nats_host, network.nats_port)],
                        "namespace": "local",
                        "connect_timeout_milliseconds": 5000,
                        "publish_timeout_milliseconds": 1000,
                        "reconnect_backoff_milliseconds": 250,
                        "drain_timeout_milliseconds": 5000,
                        "maximum_pending_messages": 1024,
                        "maximum_pending_bytes": 16777216,
                    },
                    "receipt_ttl_seconds": 3600,
                    "claim_scan_milliseconds": 250,
                    "claim_failure_backoff_milliseconds": 100,
                    "drain_grace_milliseconds": 30000,
                }),
            ),
        );
    }
    if network.process(Process::ContextRemote).is_ok() {
        configurations.insert(
            "context-remote".to_owned(),
            (
                CONTEXT_REMOTE_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "observability_listen_address": network.observability(Process::ContextRemote)?,
                    "worker_manifest": worker_builds.manifest("platform-remote-context-worker", "context-worker", insight_platform_contracts::WorkClass::Context, digests.context_adapter, (4, 1), crate::worker_profile::remote_context()),
                    "installed_adapter_digest": digests.context_adapter,
                    "protocol_contract_digest": insight_platform_context::remote_context_protocol_contract_digest(),
                    "result_mapping_digest": insight_platform_context::remote_context_result_mapping_digest(),
                    "egress_endpoint": network.endpoint(Process::EgressBroker)?,
                    "egress_tls_server_name": network.tls_server_name(Process::EgressBroker)?,
                    "maximum_rpc_metadata_bytes": MAX_EGRESS_METADATA_BYTES_HARD,
                    "maximum_rpc_payload_bytes": 1048576,
                    "connect_timeout_milliseconds": 5000,
                    "request_timeout_milliseconds": 30000,
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "receipt_ttl_seconds": 3600,
                    "scan_interval_milliseconds": 250,
                    "failure_backoff_milliseconds": 100,
                    "drain_grace_milliseconds": 30000,
                }),
            ),
        );
    }
    if network.process(Process::McpHost).is_ok() {
        configurations.insert(
            "mcp-host".to_owned(),
            (
                MCP_HOST_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "listen_address": network.listen(Process::McpHost)?,
                    "observability_listen_address": network.observability(Process::McpHost)?,
                    "tls_server_name": network.tls_server_name(Process::McpHost)?,
                    "maximum_rpc_message_bytes": 1048576,
                    "maximum_in_flight_requests": 8,
                    "egress": {
                        "endpoint": network.endpoint(Process::EgressBroker)?,
                        "tls_server_name": network.tls_server_name(Process::EgressBroker)?,
                        "connect_timeout_milliseconds": 5000,
                        "request_timeout_milliseconds": 30000,
                        "maximum_rpc_metadata_bytes": 65536,
                        "maximum_rpc_payload_bytes": 1048576,
                    },
                    "drain_grace_milliseconds": 30000,
                }),
            ),
        );
    }
    if network.process(Process::McpResourceHost).is_ok() {
        configurations.insert(
            "mcp-resource-host".to_owned(),
            (
                MCP_RESOURCE_HOST_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "listen_address": network.listen(Process::McpResourceHost)?,
                    "observability_listen_address": network.observability(Process::McpResourceHost)?,
                    "maximum_rpc_message_bytes": 1048576,
                    "maximum_in_flight_requests": 8,
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "egress": {
                        "endpoint": network.endpoint(Process::EgressBroker)?,
                        "tls_server_name": network.tls_server_name(Process::EgressBroker)?,
                        "connect_timeout_milliseconds": 5000,
                        "request_timeout_milliseconds": 30000,
                        "maximum_rpc_metadata_bytes": 65536,
                        "maximum_rpc_payload_bytes": 1048576,
                    },
                    "drain_grace_milliseconds": 30000,
                }),
            ),
        );
    }
    if network.process(Process::CapabilityRemote).is_ok() {
        configurations.insert(
            "capability-remote".to_owned(),
            (
                CAPABILITY_REMOTE_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "observability_listen_address": network.observability(Process::CapabilityRemote)?,
                    "worker_manifest": worker_builds.manifest("platform-capability-remote-worker", "capability.remote", insight_platform_contracts::WorkClass::CapabilityRemote, &capability_adapter_digest, (4, 1), crate::worker_profile::remote_capability(&capability_closure)),
                    "installed_http_codecs": capability_closure["http"],
                    "installed_grpc_codecs": capability_closure["grpc"],
                    "installed_mcp_codecs": capability_closure["mcp"],
                    "database": {
                        "business_max_connections": 4,
                        "critical_control_max_connections": 2,
                        "process_connection_budget": 6,
                        "acquire_timeout_milliseconds": 5000,
                    },
                    "egress": {
                        "endpoint": network.endpoint(Process::EgressBroker)?,
                        "tls_server_name": network.tls_server_name(Process::EgressBroker)?,
                        "connect_timeout_milliseconds": 5000,
                        "request_timeout_milliseconds": 30000,
                        "maximum_rpc_metadata_bytes": 65536,
                        "maximum_rpc_payload_bytes": 1048576,
                    },
                    "mcp_host": {
                        "endpoint": network.endpoint(Process::McpHost)?,
                        "tls_server_name": network.tls_server_name(Process::McpHost)?,
                        "connect_timeout_milliseconds": 5000,
                        "request_timeout_milliseconds": 30000,
                        "maximum_rpc_message_bytes": 1048576,
                    },
                    "timing": {
                        "initial_scan_delay_milliseconds": 0,
                        "receipt_ttl_milliseconds": 60000,
                        "safety_scan_milliseconds": 250,
                        "claim_failure_backoff_milliseconds": 100,
                        "drain_grace_milliseconds": 30000,
                    },
                }),
            ),
        );
    }
    if network.process(Process::McpDiscovery).is_ok() {
        configurations.insert(
            "mcp-discovery".to_owned(),
            (
                MCP_DISCOVERY_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "worker_manifest": worker_builds.manifest("platform-mcp-discovery-worker", "mcp-discovery-worker", insight_platform_contracts::WorkClass::Mcp, &closed_local_digest("mcp-discovery-runtime"), (4, 1), insight_platform_mcp_host::execution::discovery_execution_capabilities()),
                    "observability_listen_address": network.observability(Process::McpDiscovery)?,
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "claim_batch_size": 4,
                    "recovery_batch_size": 4,
                    "maximum_concurrency": 4,
                    "lease_milliseconds": 30000,
                    "scan_interval_milliseconds": 500,
                    "failure_backoff_milliseconds": 500,
                    "heartbeat_interval_milliseconds": 5000,
                    "retry_backoff_milliseconds": 1000,
                    "receipt_ttl_milliseconds": 60000,
                    "drain_grace_milliseconds": 30000,
                    "egress": {
                        "endpoint": network.endpoint(Process::EgressBroker)?,
                        "tls_server_name": network.tls_server_name(Process::EgressBroker)?,
                        "connect_timeout_milliseconds": 5000,
                        "request_timeout_milliseconds": 30000,
                        "maximum_rpc_metadata_bytes": 65536,
                        "maximum_rpc_payload_bytes": 1048576,
                    },
                    "artifact_data_worker": {
                        "endpoint": network.endpoint(Process::ArtifactData)?,
                        "tls_server_name": network.tls_server_name(Process::ArtifactData)?,
                        "connect_timeout_milliseconds": 5000,
                        "request_timeout_milliseconds": 30000,
                        "maximum_read_request_bytes": 1048576,
                        "maximum_chunk_bytes": 262144,
                        "maximum_write_request_bytes": 67108864,
                    },
                }),
            ),
        );
    }
    if network.process(Process::McpSubscription).is_ok() {
        configurations.insert(
            "mcp-subscription".to_owned(),
            (
                MCP_SUBSCRIPTION_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "worker_manifest": worker_builds.manifest("platform-mcp-subscription-worker", "mcp-subscription-worker", insight_platform_contracts::WorkClass::Mcp, &closed_local_digest("mcp-subscription-runtime"), (4, 1), insight_platform_mcp_host::execution::subscription_execution_capabilities()),
                    "observability_listen_address": network.observability(Process::McpSubscription)?,
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "claim_batch_size": 4,
                    "recovery_batch_size": 4,
                    "reconcile_batch_size": 4,
                    "reconcile_minimum_idle_milliseconds": 60000,
                    "maximum_concurrency": 4,
                    "lease_milliseconds": 30000,
                    "scan_interval_milliseconds": 500,
                    "failure_backoff_milliseconds": 500,
                    "heartbeat_interval_milliseconds": 5000,
                    "receipt_ttl_milliseconds": 60000,
                    "drain_grace_milliseconds": 30000,
                    "notification": {
                        "maximum_in_flight": 32,
                        "maximum_wire_bytes": 1048576,
                        "maximum_tracked_bindings": 4096,
                        "maximum_events_per_window": 1000,
                        "window_milliseconds": 60000,
                    },
                    "egress": {
                        "endpoint": network.endpoint(Process::EgressBroker)?,
                        "tls_server_name": network.tls_server_name(Process::EgressBroker)?,
                        "connect_timeout_milliseconds": 5000,
                        "request_timeout_milliseconds": 30000,
                        "maximum_rpc_metadata_bytes": 65536,
                        "maximum_rpc_payload_bytes": 1048576,
                    },
                }),
            ),
        );
    }
    if network.process(Process::McpCleanup).is_ok() {
        configurations.insert(
            "mcp-cleanup".to_owned(),
            (
                MCP_CLEANUP_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "worker_manifest": worker_builds.manifest("platform-mcp-cleanup-worker", "mcp-cleanup-worker", insight_platform_contracts::WorkClass::Recovery, &closed_local_digest("mcp-cleanup-runtime"), (16, 1), insight_platform_contracts::WorkerExecutionCapabilities { schema_version: 1, capabilities: vec![insight_platform_mcp_host::mcp_oauth_cleanup_execution_capability()] }),
                    "observability_listen_address": network.observability(Process::McpCleanup)?,
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "egress_endpoint": network.endpoint(Process::EgressBroker)?,
                    "egress_tls_server_name": network.tls_server_name(Process::EgressBroker)?,
                    "egress_connect_timeout_milliseconds": 5000,
                    "egress_request_timeout_milliseconds": 30000,
                    "maximum_rpc_metadata_bytes": 65536,
                    "maximum_rpc_payload_bytes": 1048576,
                    "poll_interval_milliseconds": 1000,
                    "maximum_batch": 64,
                    "maximum_lease_milliseconds": 120000,
                    "claim_batch": 16,
                    "lease_milliseconds": 30000,
                    "retry_base_milliseconds": 1000,
                    "retry_maximum_milliseconds": 60000,
                }),
            ),
        );
    }
    if network.process(Process::ContextSubscription).is_ok() {
        configurations.insert(
            "context-subscription".to_owned(),
            (
                CONTEXT_SUBSCRIPTION_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "observability_listen_address": network.observability(Process::ContextSubscription)?,
                    "worker_manifest": worker_builds.manifest("platform-subscription-context-worker", "context-worker", insight_platform_contracts::WorkClass::Context, digests.context_adapter, (4, 1), crate::worker_profile::subscription()),
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "receipt_ttl_seconds": 3600,
                    "scan_interval_milliseconds": 500,
                    "failure_backoff_milliseconds": 100,
                    "drain_grace_milliseconds": 30000,
                    "host": {
                        "endpoint": network.endpoint(Process::McpResourceHost)?,
                        "tls_server_name": network.tls_server_name(Process::McpResourceHost)?,
                        "connect_timeout_milliseconds": 5000,
                        "request_timeout_milliseconds": 30000,
                        "maximum_rpc_message_bytes": 1048576,
                    },
                }),
            ),
        );
    }
    if network.process(Process::CallbackApi).is_ok() {
        configurations.insert(
            "callback-api".to_owned(),
            (
                CALLBACK_API_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "listen_address": network.listen(Process::CallbackApi)?,
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "egress_endpoint": network.endpoint(Process::EgressBroker)?,
                    "egress_tls_server_name": network.tls_server_name(Process::EgressBroker)?,
                    "egress_connect_timeout_milliseconds": 5000,
                    "egress_request_timeout_milliseconds": 30000,
                    "maximum_rpc_metadata_bytes": 65536,
                    "maximum_rpc_payload_bytes": 1048576,
                    "callback_binding_digest": closed_local_digest("mcp-oauth-callback-binding"),
                    "callback_receipt_ttl_seconds": 3600,
                    "oauth_state": {
                        "active_key_id": "local-current",
                        "maximum_lifetime_seconds": 600,
                        "clock_skew_seconds": 30,
                        "key_directory": egress.mcp_oauth_state_key_root.display().to_string(),
                        "keys": [{
                            "key_id": "local-current",
                            "key_material_digest": egress.mcp_oauth_state_key_reference_digest,
                            "key_material_path": egress.mcp_oauth_state_key_path.display().to_string(),
                        }],
                    },
                    "shutdown_grace_milliseconds": 30000,
                }),
            ),
        );
    }
    if network.process(Process::ContextDataset).is_ok() {
        configurations.insert(
            "context-dataset".to_owned(),
            (
                CONTEXT_DATASET_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "worker_manifest": worker_builds.manifest("platform-context-dataset-worker", "context-dataset-worker", insight_platform_contracts::WorkClass::Context, digests.context_adapter, (4, 1), crate::worker_profile::dataset(digests.context_contract)),
                    "observability_listen_address": network.observability(Process::ContextDataset)?,
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "claim_batch_size": 4,
                    "recovery_batch_size": 4,
                    "maximum_concurrency": 4,
                    "lease_milliseconds": 30000,
                    "scan_interval_milliseconds": 500,
                    "failure_backoff_milliseconds": 250,
                    "heartbeat_interval_milliseconds": 5000,
                    "retry_backoff_milliseconds": 1000,
                    "drain_grace_milliseconds": 30000,
                    "sources": [{
                        "schema_version": 1,
                        "binding": {
                            "schema_version": 1,
                            "required_worker_manifest_digest": context_worker_manifest_digest,
                            "adapter_contract_digest": digests.context_contract,
                            "installed_adapter_digest": digests.context_adapter,
                            "canonical_digest": dataset_source_binding_digest,
                        },
                        "source_manifest_digest": dataset_source_manifest_digest,
                        "items": [dataset_content],
                    }],
                    "artifact_data_worker": {
                        "endpoint": network.endpoint(Process::ArtifactData)?,
                        "tls_server_name": network.tls_server_name(Process::ArtifactData)?,
                        "connect_timeout_milliseconds": 5000,
                        "request_timeout_milliseconds": 30000,
                        "maximum_read_request_bytes": 1048576,
                        "maximum_chunk_bytes": 262144,
                        "maximum_write_request_bytes": 67108864,
                    },
                }),
            ),
        );
    }
    Ok(configurations)
}

fn closed_local_digest(kind: &str) -> String {
    canonical_digest(&json!({"schema_version": 1, "kind": kind}))
        .expect("the closed local digest input is canonical JSON")
}

fn common_egress_limits(maximum_in_flight: usize) -> Value {
    json!({
        "maximum_in_flight": maximum_in_flight,
        "maximum_dns_answers": 16,
        "maximum_secret_material_bytes": 8192,
    })
}

pub fn initial_process_launches(
    paths: ProcessPaths<'_>,
    ports: &PortBindings,
    config_digests: &BTreeMap<String, String>,
    selected_profile: DevProfile,
    database_url: &str,
) -> Result<Vec<ProcessLaunch>, String> {
    let mut network = native_feature_network(ports);
    network
        .processes
        .retain(|entry| selected_profile.includes_role(entry.process.name()));
    network
        .processes
        .iter()
        .map(|entry| {
            crate::process_environment::process_launch(
                crate::process_environment::ProcessEnvironmentInputs {
                    process: entry.process,
                    paths: ProcessPaths {
                        release: paths.release,
                        configuration: paths.configuration,
                        tls: paths.tls,
                        ca_certificate_file: paths.ca_certificate_file,
                        nats_client_certificate_file: paths.nats_client_certificate_file,
                        nats_client_private_key_file: paths.nats_client_private_key_file,
                    },
                    network: &network,
                    configuration_digest: config_digests.get(entry.process.name()).ok_or_else(
                        || {
                            format!(
                                "selected runtime role {} has no exact configuration digest",
                                entry.process.name()
                            )
                        },
                    )?,
                    database: crate::process_environment::ProcessDatabaseUrls {
                        primary: database_url,
                        read: None,
                        work: None,
                    },
                    cursor_key_path: None,
                    cursor_key_digest: None,
                    aws_credentials_path: None,
                },
            )
            .map_err(|error| error.to_string())
        })
        .collect()
}

/// Native CLI adapter: endpoints are explicit output of its reserved port inputs.
pub fn aws_qualification_network(
    ports: &PortBindings,
    artifact_data_port: u16,
    artifact_observability_port: u16,
) -> NetworkTopologyV1 {
    let mut network = native_feature_network(ports);
    let loopback = |port| std::net::SocketAddr::from(([127, 0, 0, 1], port));
    network.processes.push(ProcessNetworkV1 {
        process: Process::ArtifactData,
        listen_address: Some(loopback(artifact_data_port)),
        observability_address: loopback(artifact_observability_port),
        service_origin: Some(
            ServiceOrigin::parse(&format!("https://localhost:{artifact_data_port}"))
                .expect("reserved native port"),
        ),
    });
    network
}
fn native_feature_network(ports: &PortBindings) -> NetworkTopologyV1 {
    let loopback = |port| std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let mut processes = Vec::new();
    processes.push(ProcessNetworkV1 {
        process: Process::ContextNative,
        listen_address: None,
        observability_address: loopback(ports.context_native_observability),
        service_origin: None,
    });
    processes.push(ProcessNetworkV1 {
        process: Process::ArtifactMaintenance,
        listen_address: None,
        observability_address: loopback(ports.artifact_maintenance_observability),
        service_origin: None,
    });
    processes.push(ProcessNetworkV1 {
        process: Process::SecurityAuthority,
        listen_address: Some(loopback(ports.security_authority)),
        observability_address: loopback(ports.security_authority_observability),
        service_origin: Some(
            ServiceOrigin::parse(&format!("https://localhost:{}", ports.security_authority))
                .expect("reserved native port"),
        ),
    });
    processes.push(ProcessNetworkV1 {
        process: Process::EgressBroker,
        listen_address: Some(loopback(ports.egress_broker)),
        observability_address: loopback(ports.egress_broker_observability),
        service_origin: Some(
            ServiceOrigin::parse(&format!("https://localhost:{}", ports.egress_broker))
                .expect("reserved native port"),
        ),
    });
    processes.push(ProcessNetworkV1 {
        process: Process::ModelWorker,
        listen_address: None,
        observability_address: loopback(ports.model_worker_observability),
        service_origin: None,
    });
    processes.push(ProcessNetworkV1 {
        process: Process::ContextRemote,
        listen_address: None,
        observability_address: loopback(ports.remote_context_worker_observability),
        service_origin: None,
    });
    processes.push(ProcessNetworkV1 {
        process: Process::McpHost,
        listen_address: Some(loopback(ports.mcp_host)),
        observability_address: loopback(ports.mcp_host_observability),
        service_origin: Some(
            ServiceOrigin::parse(&format!("https://localhost:{}", ports.mcp_host))
                .expect("reserved native port"),
        ),
    });
    processes.push(ProcessNetworkV1 {
        process: Process::McpResourceHost,
        listen_address: Some(loopback(ports.mcp_resource_host)),
        observability_address: loopback(ports.mcp_resource_host_observability),
        service_origin: Some(
            ServiceOrigin::parse(&format!("https://localhost:{}", ports.mcp_resource_host))
                .expect("reserved native port"),
        ),
    });
    processes.push(ProcessNetworkV1 {
        process: Process::CapabilityRemote,
        listen_address: None,
        observability_address: loopback(ports.capability_remote_observability),
        service_origin: None,
    });
    processes.push(ProcessNetworkV1 {
        process: Process::McpDiscovery,
        listen_address: None,
        observability_address: loopback(ports.mcp_discovery_observability),
        service_origin: None,
    });
    processes.push(ProcessNetworkV1 {
        process: Process::McpSubscription,
        listen_address: None,
        observability_address: loopback(ports.mcp_subscription_observability),
        service_origin: None,
    });
    processes.push(ProcessNetworkV1 {
        process: Process::McpCleanup,
        listen_address: None,
        observability_address: loopback(ports.mcp_cleanup_observability),
        service_origin: None,
    });
    processes.push(ProcessNetworkV1 {
        process: Process::ContextSubscription,
        listen_address: None,
        observability_address: loopback(ports.context_subscription_observability),
        service_origin: None,
    });
    processes.push(ProcessNetworkV1 {
        process: Process::CallbackApi,
        listen_address: Some(loopback(ports.callback_api)),
        observability_address: loopback(ports.callback_api),
        service_origin: Some(
            ServiceOrigin::parse(&format!("http://localhost:{}", ports.callback_api))
                .expect("reserved native port"),
        ),
    });
    processes.push(ProcessNetworkV1 {
        process: Process::ContextDataset,
        listen_address: None,
        observability_address: loopback(ports.context_dataset_observability),
        service_origin: None,
    });
    NetworkTopologyV1 {
        topology: InstallationTopology::Native,
        processes,
        database: DatabaseEndpointV1 {
            host: "localhost".into(),
            port: 5432,
            database: "insight_platform".into(),
        },
        nats_host: "localhost".into(),
        nats_port: 4222,
        console_origin: ServiceOrigin::parse("http://localhost:4173")
            .expect("native Console origin"),
        providers: insight_platform_deployment_contracts::installation::ProviderNetworkV1::Aws {
            artifact: ServiceOrigin::parse("https://localhost.localstack.cloud:4566")
                .expect("native AWS origin"),
            kms: ServiceOrigin::parse("https://localhost.localstack.cloud:4566")
                .expect("native AWS origin"),
            secrets: ServiceOrigin::parse("https://localhost.localstack.cloud:4566")
                .expect("native AWS origin"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(character: char) -> String {
        format!("sha256:{}", character.to_string().repeat(64))
    }

    #[test]
    fn initial_documents_bind_exact_ports_and_adapter_digest() {
        let ports = PortBindings {
            context_native_observability: 31_001,
            artifact_maintenance_observability: 31_002,
            security_authority: 31_003,
            security_authority_observability: 31_004,
            egress_broker: 31_005,
            egress_broker_observability: 31_006,
            model_worker_observability: 31_007,
            remote_context_worker_observability: 31_008,
            mcp_host: 31_009,
            mcp_host_observability: 31_010,
            mcp_resource_host: 31_011,
            mcp_resource_host_observability: 31_012,
            capability_remote_observability: 31_013,
            mcp_discovery_observability: 31_014,
            mcp_subscription_observability: 31_015,
            mcp_cleanup_observability: 31_016,
            context_subscription_observability: 31_017,
            callback_api: 31_018,
            context_dataset_observability: 31_019,
            outbox_observability: 31020,
            history_observability: 31021,
        };
        let catalog = json!({"schema_version": 1});
        let adapter = digest('a');
        let contract = digest('b');
        let principal = "prn_0198f1c3-8f49-7c3e-b1f3-773c28367b90";
        let directory = tempfile::tempdir().unwrap();
        let builds = crate::worker_profile::WorkerBuilds::read(
            &crate::worker_profile::fixture_binaries(directory.path()),
            DevProfile::parse(Some("all"), false, true).unwrap(),
        )
        .unwrap();
        let configs = initial_configs(
            &builds,
            &aws_qualification_network(&ports, 30999, 31022),
            &catalog,
            Some(&insight_platform_contracts::ResourceId::from_uuid_v7(insight_platform_contracts::ResourceKind::PolicyRevision, uuid::Uuid::now_v7()).unwrap()),
            WorkerDigests {
                context_adapter: &adapter,
                context_contract: &contract,
            },
            EgressConfigInputs {
                model_installation: None,
                remote_context_destinations: &[],
                service_principal_id: principal,
                secret_provider_catalog: &json!({"schema_version": 1, "providers": [{"provider": "closed"}]}),
                mcp_state_key_root: Path::new("/project/runtime/mcp-state-keys"),
                mcp_state_key_path: Path::new("/project/runtime/mcp-state-keys/current"),
                mcp_state_key_reference_digest: &digest('c'),
                mcp_oauth_state_key_root: Path::new("/project/runtime/mcp-oauth-state-keys"),
                mcp_oauth_state_key_path: Path::new(
                    "/project/runtime/mcp-oauth-state-keys/current",
                ),
                mcp_oauth_state_key_reference_digest: &digest('9'),
            },
        ).unwrap();

        let context = &configs["context-native"].1;
        assert_eq!(context["observability_listen_address"], "127.0.0.1:31001");
        assert_eq!(
            context["worker_manifest"]["adapter_runtime_digest"],
            context["native_catalog"]["installed_adapter_digest"]
        );
        assert_eq!(context["native_catalog"]["classification"], "internal");

        let maintenance = &configs["artifact-maintenance"].1;
        assert_eq!(maintenance["listen_address"], "127.0.0.1:31002");
        assert_eq!(maintenance["artifact_provider_catalog"], catalog);

        let security = &configs["security-authority"].1;
        assert_eq!(security["listen_address"], "127.0.0.1:31003");
        assert_eq!(security["observability_listen_address"], "127.0.0.1:31004");
        assert_eq!(security["service_principal_id"], principal);

        let egress = &configs["egress-broker"].1;
        assert_eq!(egress["listen_address"], "127.0.0.1:31005");
        assert_eq!(
            egress["security_authority_endpoint"],
            "https://localhost:31003"
        );
        assert_eq!(egress["mcp_state_keys"]["key_material_path"], Value::Null);
        // Model terminal SSE data travels as metadata; the payload allowance cannot carry it.
        // Both sides must accept the supported basic structured-output frame size.
        let model = &configs["model-worker"].1;
        for role in [egress, model] {
            assert_eq!(
                role["maximum_rpc_metadata_bytes"],
                MAX_EGRESS_METADATA_BYTES_HARD
            );
            assert_eq!(role["maximum_rpc_payload_bytes"], 1_048_576);
        }
        assert!(model["maximum_rpc_metadata_bytes"].as_u64().unwrap() > 262_144);
        assert_eq!(egress["model_limits"]["maximum_in_flight"], 16);
        assert_eq!(model["live_delta"]["maximum_pending_messages"], 1_024);
        assert_eq!(model["live_delta"]["maximum_pending_bytes"], 16_777_216);
        assert_eq!(security["maximum_rpc_message_bytes"], 65_536);
        assert_eq!(
            configs["mcp-host"].1["egress"]["maximum_rpc_metadata_bytes"],
            65_536
        );
        assert_eq!(
            configs["mcp-cleanup"].1["maximum_rpc_metadata_bytes"],
            65_536
        );
        assert_eq!(
            egress["mcp_state_keys"]["keys"][0]["key_material_path"],
            "/project/runtime/mcp-state-keys/current"
        );
        assert_eq!(
            configs["mcp-discovery"].1["artifact_data_worker"]["endpoint"],
            "https://localhost:30999/"
        );
        assert_eq!(
            configs["context-subscription"].1["host"]["endpoint"],
            "https://localhost:31011/"
        );
        assert_eq!(
            configs["callback-api"].1["oauth_state"]["key_directory"],
            "/project/runtime/mcp-oauth-state-keys"
        );
        assert_eq!(
            configs["callback-api"].1["listen_address"],
            "127.0.0.1:31018"
        );
        assert_eq!(
            configs["context-dataset"].1["observability_listen_address"],
            "127.0.0.1:31019"
        );
        assert_eq!(
            configs["context-dataset"].1["sources"][0]["binding"]["adapter_contract_digest"],
            contract
        );
    }

    #[test]
    fn initial_processes_are_profile_scoped_and_digest_bound() {
        let ports = PortBindings {
            context_native_observability: 31_001,
            artifact_maintenance_observability: 31_002,
            security_authority: 31_003,
            security_authority_observability: 31_004,
            egress_broker: 31_005,
            egress_broker_observability: 31_006,
            model_worker_observability: 31_007,
            remote_context_worker_observability: 31_008,
            mcp_host: 31_009,
            mcp_host_observability: 31_010,
            mcp_resource_host: 31_011,
            mcp_resource_host_observability: 31_012,
            capability_remote_observability: 31_013,
            mcp_discovery_observability: 31_014,
            mcp_subscription_observability: 31_015,
            mcp_cleanup_observability: 31_016,
            context_subscription_observability: 31_017,
            callback_api: 31_018,
            context_dataset_observability: 31_019,
            outbox_observability: 31020,
            history_observability: 31021,
        };
        let digests = BTreeMap::from([
            ("context-native".to_owned(), digest('a')),
            ("artifact-maintenance".to_owned(), digest('b')),
            ("security-authority".to_owned(), digest('c')),
            ("egress-broker".to_owned(), digest('d')),
            ("model-worker".to_owned(), digest('e')),
            ("context-remote".to_owned(), digest('f')),
            ("mcp-host".to_owned(), digest('1')),
            ("mcp-resource-host".to_owned(), digest('2')),
            ("capability-remote".to_owned(), digest('3')),
            ("mcp-discovery".to_owned(), digest('4')),
            ("mcp-subscription".to_owned(), digest('5')),
            ("mcp-cleanup".to_owned(), digest('6')),
            ("context-subscription".to_owned(), digest('7')),
            ("callback-api".to_owned(), digest('8')),
            ("context-dataset".to_owned(), digest('c')),
        ]);
        let launches = initial_process_launches(
            ProcessPaths {
                release: Path::new("/workspace/target/release"),
                configuration: Path::new("/project/runtime/config"),
                tls: Path::new("/project/runtime/tls"),
                ca_certificate_file: "ca.pem",
                nats_client_certificate_file: "nats-client.pem",
                nats_client_private_key_file: "nats-client-key.pem",
            },
            &ports,
            &digests,
            DevProfile::parse(Some("all"), false, true).unwrap(),
            "postgres://local-authority",
        )
        .unwrap();
        assert_eq!(
            launches
                .iter()
                .map(|launch| launch.role)
                .collect::<Vec<_>>(),
            vec![
                "context-native",
                "security-authority",
                "egress-broker",
                "model-worker",
                "context-remote",
                "mcp-host",
                "mcp-resource-host",
                "capability-remote",
                "mcp-discovery",
                "mcp-subscription",
                "mcp-cleanup",
                "context-subscription",
                "callback-api",
                "context-dataset"
            ]
        );
        let remote_only = initial_process_launches(
            ProcessPaths {
                release: Path::new("/workspace/target/release"),
                configuration: Path::new("/project/runtime/config"),
                tls: Path::new("/project/runtime/tls"),
                ca_certificate_file: "ca.pem",
                nats_client_certificate_file: "nats-client.pem",
                nats_client_private_key_file: "nats-client-key.pem",
            },
            &ports,
            &digests,
            DevProfile::parse(Some("remote-capability"), false, true).unwrap(),
            "postgres://local-authority",
        )
        .unwrap();
        assert!(!remote_only.iter().any(|launch| launch.role == "mcp-host"));
        let remote = remote_only
            .iter()
            .find(|launch| launch.role == "capability-remote")
            .unwrap();
        assert!(!remote
            .environment
            .iter()
            .any(|(name, _)| name.starts_with("PLATFORM_CAPABILITY_REMOTE_WORKER_MCP_HOST_")));
        assert!(remote
            .environment
            .iter()
            .any(|(name, _)| *name == "PLATFORM_CAPABILITY_REMOTE_WORKER_EGRESS_CA_PATH"));
        assert_eq!(launches[0].ready_address, "127.0.0.1:31001");
        assert_eq!(launches[1].ready_address, "127.0.0.1:31004");
        assert_eq!(launches[2].ready_address, "127.0.0.1:31006");
        assert_eq!(launches[3].ready_address, "127.0.0.1:31007");
        assert_eq!(launches[4].ready_address, "127.0.0.1:31008");
        assert_eq!(launches[5].ready_address, "127.0.0.1:31010");
        assert_eq!(launches[6].ready_address, "127.0.0.1:31012");
        assert_eq!(launches[7].ready_address, "127.0.0.1:31013");
        assert_eq!(launches[8].ready_address, "127.0.0.1:31014");
        assert_eq!(launches[9].ready_address, "127.0.0.1:31015");
        assert_eq!(launches[10].ready_address, "127.0.0.1:31016");
        assert_eq!(launches[11].ready_address, "127.0.0.1:31017");
        assert_eq!(launches[12].ready_address, "127.0.0.1:31018");
        assert_eq!(launches[13].ready_address, "127.0.0.1:31019");
        assert!(launches
            .iter()
            .all(|launch| launch.environment.iter().any(|(name, value)| name
                .ends_with("CONFIG_DIGEST")
                && value.starts_with("sha256:"))));
        assert!(launches[1].environment.iter().any(|(name, value)| *name
            == "PLATFORM_SECURITY_AUTHORITY_CLIENT_CA_PATH"
            && value == "/project/runtime/tls/ca.pem"));
        assert_eq!(
            launches[2].extra_environment,
            vec![
                ("AWS_ACCESS_KEY_ID".to_owned(), "test".to_owned()),
                ("AWS_SECRET_ACCESS_KEY".to_owned(), "test".to_owned()),
                ("AWS_EC2_METADATA_DISABLED".to_owned(), "true".to_owned())
            ]
        );
        assert!(launches[2].environment.iter().any(|(name, value)| *name
            == "PLATFORM_EGRESS_BROKER_AUTHORITY_CERT_PATH"
            && value == "/project/runtime/tls/egress-broker-client.pem"));
        assert!(launches[3].environment.iter().any(|(name, value)| *name
            == "PLATFORM_MODEL_WORKER_EGRESS_CERT_PATH"
            && value == "/project/runtime/tls/model-worker-client.pem"));
        assert!(launches[4].environment.iter().any(|(name, value)| *name
            == "PLATFORM_REMOTE_CONTEXT_WORKER_EGRESS_CERT_PATH"
            && value == "/project/runtime/tls/context-worker-client.pem"));
        assert!(launches[5].environment.iter().any(|(name, value)| *name
            == "PLATFORM_MCP_HOST_EGRESS_CERT_PATH"
            && value == "/project/runtime/tls/mcp-host-egress-client.pem"));
        assert!(launches[6].environment.iter().any(|(name, value)| *name
            == "PLATFORM_MCP_RESOURCE_HOST_SERVER_CERT_PATH"
            && value == "/project/runtime/tls/mcp-resource-host.pem"));
        assert!(launches[7].environment.iter().any(|(name, value)| *name
            == "PLATFORM_CAPABILITY_REMOTE_WORKER_MCP_HOST_CERT_PATH"
            && value == "/project/runtime/tls/capability-remote-client.pem"));
        assert!(launches[8].environment.iter().any(|(name, value)| *name
            == "PLATFORM_MCP_DISCOVERY_WORKER_CLIENT_CERT_PATH"
            && value == "/project/runtime/tls/mcp-discovery-client.pem"));
        assert!(launches[9].environment.iter().any(|(name, value)| *name
            == "PLATFORM_MCP_SUBSCRIPTION_WORKER_CLIENT_CERT_PATH"
            && value == "/project/runtime/tls/mcp-subscription-client.pem"));
        assert!(launches[10].environment.iter().any(|(name, value)| *name
            == "PLATFORM_MCP_CLEANUP_EGRESS_CERT_PATH"
            && value == "/project/runtime/tls/mcp-cleanup-client.pem"));
        assert!(launches[11].environment.iter().any(|(name, value)| *name
            == "PLATFORM_SUBSCRIPTION_CONTEXT_WORKER_HOST_CERT_PATH"
            && value == "/project/runtime/tls/context-subscription-client.pem"));
        assert!(launches[12].environment.iter().any(|(name, value)| *name
            == "PLATFORM_CALLBACK_API_EGRESS_CERT_PATH"
            && value == "/project/runtime/tls/callback-client.pem"));
        assert!(launches[13].environment.iter().any(|(name, value)| *name
            == "PLATFORM_CONTEXT_DATASET_WORKER_CLIENT_CERT_PATH"
            && value == "/project/runtime/tls/context-dataset-client.pem"));

        let scoped_launches = initial_process_launches(
            ProcessPaths {
                release: Path::new("/workspace/target/release"),
                configuration: Path::new("/project/runtime/config"),
                tls: Path::new("/project/runtime/tls"),
                ca_certificate_file: "ca.pem",
                nats_client_certificate_file: "nats-client.pem",
                nats_client_private_key_file: "nats-client-key.pem",
            },
            &ports,
            &BTreeMap::from([("model-worker".to_owned(), digest('e'))]),
            DevProfile::parse(Some("model"), false, true).unwrap(),
            "postgres://local-authority",
        );
        assert!(matches!(
            scoped_launches,
            Err(detail) if detail.contains("security-authority")
        ));

        let scoped_launches = initial_process_launches(
            ProcessPaths {
                release: Path::new("/workspace/target/release"),
                configuration: Path::new("/project/runtime/config"),
                tls: Path::new("/project/runtime/tls"),
                ca_certificate_file: "ca.pem",
                nats_client_certificate_file: "nats-client.pem",
                nats_client_private_key_file: "nats-client-key.pem",
            },
            &ports,
            &digests,
            DevProfile::parse(Some("model"), false, true).unwrap(),
            "postgres://local-authority",
        )
        .unwrap();
        assert_eq!(
            scoped_launches
                .iter()
                .map(|launch| launch.role)
                .collect::<Vec<_>>(),
            vec!["security-authority", "egress-broker", "model-worker"]
        );
    }
}
