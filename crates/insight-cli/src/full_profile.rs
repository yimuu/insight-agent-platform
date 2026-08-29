//! Closed configuration fragments for the additive local `full` development profile.
//!
//! This module deliberately contains only product-facing process configuration. It does not
//! connect to an authority or execute worker logic; each generated document is consumed and
//! revalidated by the corresponding independent Platform process.

use super::fresh_resource_id;
use insight_platform_contracts::{
    builtin_json_codec_module_digest, builtin_json_grpc_error_mapping_digest,
    builtin_json_grpc_protobuf_contract_digest, builtin_json_grpc_request_mapping_digest,
    builtin_json_grpc_response_mapping_digest, builtin_json_http_error_mapping_digest,
    builtin_json_http_protocol_contract_digest, builtin_json_http_request_mapping_digest,
    builtin_json_http_response_mapping_digest, builtin_json_mcp_output_mapping_digest,
    canonical_digest, ResourceKind, BUILTIN_JSON_CODEC_ID, BUILTIN_JSON_CODEC_VERSION,
    WASI_ABI_V1_RUNTIME_VERSION,
};
pub(crate) use insight_platform_contracts::{
    CAPABILITY_WORKER_WORKLOAD_IDENTITY, CONTEXT_WORKER_WORKLOAD_IDENTITY,
    EGRESS_BROKER_WORKLOAD_IDENTITY, MCP_CALLBACK_WORKLOAD_IDENTITY,
    MCP_CLEANUP_WORKER_WORKLOAD_IDENTITY, MCP_DISCOVERY_WORKER_WORKLOAD_IDENTITY,
    MCP_HOST_WORKLOAD_IDENTITY, MCP_SUBSCRIPTION_WORKER_WORKLOAD_IDENTITY,
    MODEL_WORKER_WORKLOAD_IDENTITY, SANDBOX_CONTROLLER_WORKLOAD_IDENTITY,
    WASI_EXECUTOR_WORKLOAD_IDENTITY,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

pub(crate) const CONTEXT_NATIVE_CONFIG_FILE: &str = "context-native.json";
pub(crate) const ARTIFACT_MAINTENANCE_CONFIG_FILE: &str = "artifact-maintenance.json";
pub(crate) const SECURITY_AUTHORITY_CONFIG_FILE: &str = "security-authority.json";
pub(crate) const EGRESS_BROKER_CONFIG_FILE: &str = "egress-broker.json";
pub(crate) const MODEL_WORKER_CONFIG_FILE: &str = "model-worker.json";
pub(crate) const CONTEXT_REMOTE_CONFIG_FILE: &str = "context-remote.json";
pub(crate) const MCP_HOST_CONFIG_FILE: &str = "mcp-host.json";
pub(crate) const MCP_RESOURCE_HOST_CONFIG_FILE: &str = "mcp-resource-host.json";
pub(crate) const CAPABILITY_REMOTE_CONFIG_FILE: &str = "capability-remote.json";
pub(crate) const MCP_DISCOVERY_CONFIG_FILE: &str = "mcp-discovery-worker.json";
pub(crate) const MCP_SUBSCRIPTION_CONFIG_FILE: &str = "mcp-subscription-worker.json";
pub(crate) const MCP_CLEANUP_CONFIG_FILE: &str = "mcp-cleanup-worker.json";
pub(crate) const CONTEXT_SUBSCRIPTION_CONFIG_FILE: &str = "subscription-context-worker.json";
pub(crate) const CALLBACK_API_CONFIG_FILE: &str = "callback-api.json";
pub(crate) const SANDBOX_ATTESTOR_CONFIG_FILE: &str = "sandbox-attestor.json";
pub(crate) const SANDBOX_CONTROLLER_CONFIG_FILE: &str = "sandbox-controller.json";
pub(crate) const SANDBOX_EXECUTOR_CONFIG_FILE: &str = "sandbox-executor-wasi.json";
pub(crate) const SECURITY_AUTHORITY_CERTIFICATE_FILE: &str = "security-authority.pem";
pub(crate) const SECURITY_AUTHORITY_PRIVATE_KEY_FILE: &str = "security-authority-key.pem";
pub(crate) const EGRESS_BROKER_CLIENT_CERTIFICATE_FILE: &str = "egress-broker-client.pem";
pub(crate) const EGRESS_BROKER_CLIENT_PRIVATE_KEY_FILE: &str = "egress-broker-client-key.pem";
pub(crate) const EGRESS_BROKER_CERTIFICATE_FILE: &str = "egress-broker.pem";
pub(crate) const EGRESS_BROKER_PRIVATE_KEY_FILE: &str = "egress-broker-key.pem";
pub(crate) const MCP_STATE_KEY_DIRECTORY: &str = "mcp-state-keys";
pub(crate) const MCP_STATE_KEY_FILE: &str = "current";
pub(crate) const MCP_OAUTH_STATE_KEY_DIRECTORY: &str = "mcp-oauth-state-keys";
pub(crate) const MCP_OAUTH_STATE_KEY_FILE: &str = "current";
pub(crate) const MODEL_WORKER_CLIENT_CERTIFICATE_FILE: &str = "model-worker-client.pem";
pub(crate) const MODEL_WORKER_CLIENT_PRIVATE_KEY_FILE: &str = "model-worker-client-key.pem";
pub(crate) const CONTEXT_WORKER_CLIENT_CERTIFICATE_FILE: &str = "context-worker-client.pem";
pub(crate) const CONTEXT_WORKER_CLIENT_PRIVATE_KEY_FILE: &str = "context-worker-client-key.pem";
pub(crate) const MCP_HOST_CERTIFICATE_FILE: &str = "mcp-host.pem";
pub(crate) const MCP_HOST_PRIVATE_KEY_FILE: &str = "mcp-host-key.pem";
pub(crate) const MCP_RESOURCE_HOST_CERTIFICATE_FILE: &str = "mcp-resource-host.pem";
pub(crate) const MCP_RESOURCE_HOST_PRIVATE_KEY_FILE: &str = "mcp-resource-host-key.pem";
pub(crate) const MCP_HOST_EGRESS_CLIENT_CERTIFICATE_FILE: &str = "mcp-host-egress-client.pem";
pub(crate) const MCP_HOST_EGRESS_CLIENT_PRIVATE_KEY_FILE: &str = "mcp-host-egress-client-key.pem";
pub(crate) const MCP_RESOURCE_EGRESS_CLIENT_CERTIFICATE_FILE: &str =
    "mcp-resource-egress-client.pem";
pub(crate) const MCP_RESOURCE_EGRESS_CLIENT_PRIVATE_KEY_FILE: &str =
    "mcp-resource-egress-client-key.pem";
pub(crate) const CAPABILITY_REMOTE_CLIENT_CERTIFICATE_FILE: &str = "capability-remote-client.pem";
pub(crate) const CAPABILITY_REMOTE_CLIENT_PRIVATE_KEY_FILE: &str =
    "capability-remote-client-key.pem";
pub(crate) const MCP_DISCOVERY_CLIENT_CERTIFICATE_FILE: &str = "mcp-discovery-client.pem";
pub(crate) const MCP_DISCOVERY_CLIENT_PRIVATE_KEY_FILE: &str = "mcp-discovery-client-key.pem";
pub(crate) const MCP_SUBSCRIPTION_CLIENT_CERTIFICATE_FILE: &str = "mcp-subscription-client.pem";
pub(crate) const MCP_SUBSCRIPTION_CLIENT_PRIVATE_KEY_FILE: &str = "mcp-subscription-client-key.pem";
pub(crate) const MCP_CLEANUP_CLIENT_CERTIFICATE_FILE: &str = "mcp-cleanup-client.pem";
pub(crate) const MCP_CLEANUP_CLIENT_PRIVATE_KEY_FILE: &str = "mcp-cleanup-client-key.pem";
pub(crate) const CONTEXT_SUBSCRIPTION_CLIENT_CERTIFICATE_FILE: &str =
    "context-subscription-client.pem";
pub(crate) const CONTEXT_SUBSCRIPTION_CLIENT_PRIVATE_KEY_FILE: &str =
    "context-subscription-client-key.pem";
pub(crate) const CALLBACK_CLIENT_CERTIFICATE_FILE: &str = "callback-client.pem";
pub(crate) const CALLBACK_CLIENT_PRIVATE_KEY_FILE: &str = "callback-client-key.pem";
pub(crate) const SANDBOX_ATTESTOR_CERTIFICATE_FILE: &str = "sandbox-attestor.pem";
pub(crate) const SANDBOX_ATTESTOR_PRIVATE_KEY_FILE: &str = "sandbox-attestor-key.pem";
pub(crate) const SANDBOX_CONTROLLER_CERTIFICATE_FILE: &str = "sandbox-controller.pem";
pub(crate) const SANDBOX_CONTROLLER_PRIVATE_KEY_FILE: &str = "sandbox-controller-key.pem";
pub(crate) const SANDBOX_CONTROLLER_CLIENT_CERTIFICATE_FILE: &str = "sandbox-controller-client.pem";
pub(crate) const SANDBOX_CONTROLLER_CLIENT_PRIVATE_KEY_FILE: &str =
    "sandbox-controller-client-key.pem";
pub(crate) const SANDBOX_EXECUTOR_CLIENT_CERTIFICATE_FILE: &str = "sandbox-executor-client.pem";
pub(crate) const SANDBOX_EXECUTOR_CLIENT_PRIVATE_KEY_FILE: &str = "sandbox-executor-client-key.pem";
pub(crate) const SANDBOX_STATE_DIRECTORY: &str = "sandbox-attestor";
pub(crate) const SANDBOX_REGISTRATION_SOCKET_FILE: &str = "registration.sock";
pub(crate) const SANDBOX_REGISTRY_FILE: &str = "registrations.json";
pub(crate) const SANDBOX_LOCAL_INSTANCE_UID_FILE: &str = "sandbox-local-instance-uid";
pub(crate) const INITIAL_BINARY_NAMES: [&str; 17] = [
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
    "platform-sandbox-attestor",
    "platform-sandbox-controller",
    "platform-sandbox-executor",
];

pub(crate) struct ProcessLaunch {
    pub(crate) role: &'static str,
    pub(crate) binary: PathBuf,
    pub(crate) ready_address: String,
    pub(crate) environment: Vec<(&'static str, String)>,
    pub(crate) extra_environment: Vec<(String, String)>,
}

pub(crate) struct ProcessPaths<'a> {
    pub(crate) release: &'a Path,
    pub(crate) configuration: &'a Path,
    pub(crate) tls: &'a Path,
    pub(crate) ca_certificate_file: &'a str,
    pub(crate) nats_client_certificate_file: &'a str,
    pub(crate) nats_client_private_key_file: &'a str,
}

pub(crate) struct EgressConfigInputs<'a> {
    pub(crate) service_principal_id: &'a str,
    pub(crate) secret_provider_catalog: &'a Value,
    pub(crate) mcp_state_key_root: &'a Path,
    pub(crate) mcp_state_key_path: &'a Path,
    pub(crate) mcp_state_key_reference_digest: &'a str,
    pub(crate) mcp_oauth_state_key_root: &'a Path,
    pub(crate) mcp_oauth_state_key_path: &'a Path,
    pub(crate) mcp_oauth_state_key_reference_digest: &'a str,
    pub(crate) artifact_data_worker_port: u16,
}

pub(crate) struct WorkerDigests<'a> {
    pub(crate) context_adapter: &'a str,
    pub(crate) context_contract: &'a str,
    pub(crate) model_adapter: &'a str,
    pub(crate) anthropic_contract: &'a str,
    pub(crate) openai_contract: &'a str,
}

pub(crate) struct SandboxConfigInputs<'a> {
    pub(crate) runtime: &'a Path,
    pub(crate) local_instance_uid_path: &'a Path,
    pub(crate) artifact_data_worker_port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PortBindings {
    pub(crate) context_native_observability: u16,
    pub(crate) artifact_maintenance_observability: u16,
    pub(crate) security_authority: u16,
    pub(crate) security_authority_observability: u16,
    #[serde(default = "default_egress_broker_port")]
    pub(crate) egress_broker: u16,
    #[serde(default = "default_egress_broker_observability_port")]
    pub(crate) egress_broker_observability: u16,
    #[serde(default = "default_model_worker_observability_port")]
    pub(crate) model_worker_observability: u16,
    #[serde(default = "default_remote_context_worker_observability_port")]
    pub(crate) remote_context_worker_observability: u16,
    #[serde(default = "default_mcp_host_port")]
    pub(crate) mcp_host: u16,
    #[serde(default = "default_mcp_host_observability_port")]
    pub(crate) mcp_host_observability: u16,
    #[serde(default = "default_mcp_resource_host_port")]
    pub(crate) mcp_resource_host: u16,
    #[serde(default = "default_mcp_resource_host_observability_port")]
    pub(crate) mcp_resource_host_observability: u16,
    #[serde(default = "default_capability_remote_observability_port")]
    pub(crate) capability_remote_observability: u16,
    #[serde(default = "default_mcp_discovery_observability_port")]
    pub(crate) mcp_discovery_observability: u16,
    #[serde(default = "default_mcp_subscription_observability_port")]
    pub(crate) mcp_subscription_observability: u16,
    #[serde(default = "default_mcp_cleanup_observability_port")]
    pub(crate) mcp_cleanup_observability: u16,
    #[serde(default = "default_context_subscription_observability_port")]
    pub(crate) context_subscription_observability: u16,
    #[serde(default = "default_callback_api_port")]
    pub(crate) callback_api: u16,
    #[serde(default = "default_sandbox_attestor_port")]
    pub(crate) sandbox_attestor: u16,
    #[serde(default = "default_sandbox_attestor_observability_port")]
    pub(crate) sandbox_attestor_observability: u16,
    #[serde(default = "default_sandbox_controller_port")]
    pub(crate) sandbox_controller: u16,
    #[serde(default = "default_sandbox_controller_observability_port")]
    pub(crate) sandbox_controller_observability: u16,
    #[serde(default = "default_sandbox_executor_observability_port")]
    pub(crate) sandbox_executor_observability: u16,
}

impl PortBindings {
    pub(crate) fn allocate<E>(next: &mut impl FnMut() -> Result<u16, E>) -> Result<Self, E> {
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
            sandbox_attestor: next()?,
            sandbox_attestor_observability: next()?,
            sandbox_controller: next()?,
            sandbox_controller_observability: next()?,
            sandbox_executor_observability: next()?,
        })
    }

    pub(crate) const fn legacy_defaults() -> Self {
        Self {
            context_native_observability: 19_095,
            artifact_maintenance_observability: 19_096,
            security_authority: 19_097,
            security_authority_observability: 19_098,
            egress_broker: default_egress_broker_port(),
            egress_broker_observability: default_egress_broker_observability_port(),
            model_worker_observability: default_model_worker_observability_port(),
            remote_context_worker_observability: default_remote_context_worker_observability_port(),
            mcp_host: default_mcp_host_port(),
            mcp_host_observability: default_mcp_host_observability_port(),
            mcp_resource_host: default_mcp_resource_host_port(),
            mcp_resource_host_observability: default_mcp_resource_host_observability_port(),
            capability_remote_observability: default_capability_remote_observability_port(),
            mcp_discovery_observability: default_mcp_discovery_observability_port(),
            mcp_subscription_observability: default_mcp_subscription_observability_port(),
            mcp_cleanup_observability: default_mcp_cleanup_observability_port(),
            context_subscription_observability: default_context_subscription_observability_port(),
            callback_api: default_callback_api_port(),
            sandbox_attestor: default_sandbox_attestor_port(),
            sandbox_attestor_observability: default_sandbox_attestor_observability_port(),
            sandbox_controller: default_sandbox_controller_port(),
            sandbox_controller_observability: default_sandbox_controller_observability_port(),
            sandbox_executor_observability: default_sandbox_executor_observability_port(),
        }
    }
}

const fn default_egress_broker_port() -> u16 {
    19_099
}

const fn default_egress_broker_observability_port() -> u16 {
    19_100
}

const fn default_model_worker_observability_port() -> u16 {
    19_101
}

const fn default_remote_context_worker_observability_port() -> u16 {
    19_102
}

const fn default_mcp_host_port() -> u16 {
    19_103
}

const fn default_mcp_host_observability_port() -> u16 {
    19_104
}

const fn default_mcp_resource_host_port() -> u16 {
    19_105
}

const fn default_mcp_resource_host_observability_port() -> u16 {
    19_106
}

const fn default_capability_remote_observability_port() -> u16 {
    19_107
}

const fn default_mcp_discovery_observability_port() -> u16 {
    19_108
}

const fn default_mcp_subscription_observability_port() -> u16 {
    19_109
}

const fn default_mcp_cleanup_observability_port() -> u16 {
    19_110
}

const fn default_context_subscription_observability_port() -> u16 {
    19_111
}

const fn default_callback_api_port() -> u16 {
    19_112
}

const fn default_sandbox_attestor_port() -> u16 {
    19_113
}

const fn default_sandbox_attestor_observability_port() -> u16 {
    19_114
}

const fn default_sandbox_controller_port() -> u16 {
    19_115
}

const fn default_sandbox_controller_observability_port() -> u16 {
    19_116
}

const fn default_sandbox_executor_observability_port() -> u16 {
    19_117
}

impl Default for PortBindings {
    fn default() -> Self {
        Self::legacy_defaults()
    }
}

pub(crate) fn initial_configs(
    ports: &PortBindings,
    artifact_provider_catalog: &Value,
    digests: WorkerDigests<'_>,
    egress: EgressConfigInputs<'_>,
    sandbox: SandboxConfigInputs<'_>,
) -> BTreeMap<String, (&'static str, Value)> {
    let model_manifest = json!({
        "manifest_version": 1,
        "worker_role": "model-worker",
        "work_class": "model",
        "adapter_runtime_digest": digests.model_adapter,
        "protocol_version": 1,
        "max_concurrency": 4,
        "critical_control_reserved_slots": 1,
    });
    let model_manifest_digest = canonical_digest(&model_manifest)
        .expect("the closed local Model worker manifest is canonical JSON");
    let capability_http_codecs = vec![json!({
        "codec_id": BUILTIN_JSON_CODEC_ID,
        "codec_version": BUILTIN_JSON_CODEC_VERSION,
        "module_digest": builtin_json_codec_module_digest(),
        "worker_protocol_version": 1,
        "descriptor_digest": closed_local_digest("capability-http-descriptor"),
        "protocol_contract_digest": builtin_json_http_protocol_contract_digest(),
        "request_mapping_digest": builtin_json_http_request_mapping_digest(),
        "response_mapping_digest": builtin_json_http_response_mapping_digest(),
        "error_mapping_digest": builtin_json_http_error_mapping_digest(),
    })];
    let capability_grpc_codecs = vec![json!({
        "codec_id": BUILTIN_JSON_CODEC_ID,
        "codec_version": BUILTIN_JSON_CODEC_VERSION,
        "module_digest": builtin_json_codec_module_digest(),
        "worker_protocol_version": 1,
        "descriptor_digest": closed_local_digest("capability-grpc-descriptor"),
        "protobuf_contract_digest": builtin_json_grpc_protobuf_contract_digest(),
        "service_name": "insight.fixture.v1.Lookup",
        "method_name": "Get",
        "request_mapping_digest": builtin_json_grpc_request_mapping_digest(),
        "response_mapping_digest": builtin_json_grpc_response_mapping_digest(),
        "error_mapping_digest": builtin_json_grpc_error_mapping_digest(),
    })];
    let capability_mcp_codecs = vec![json!({
        "codec_id": BUILTIN_JSON_CODEC_ID,
        "codec_version": BUILTIN_JSON_CODEC_VERSION,
        "module_digest": builtin_json_codec_module_digest(),
        "worker_protocol_version": 1,
        "descriptor_digest": closed_local_digest("capability-mcp-descriptor"),
        "remote_tool_name": "fixture_lookup",
        "remote_input_schema_digest": closed_local_digest("capability-mcp-input-schema"),
        "output_mapping_digest": builtin_json_mcp_output_mapping_digest(),
        "protocol_profile_id": fresh_resource_id(ResourceKind::PolicyRevision),
        "protocol_profile_digest": closed_local_digest("capability-mcp-protocol-profile"),
        "discovery_semantic_evidence_digest": closed_local_digest("capability-mcp-discovery"),
    })];
    let capability_closure = json!({
        "schema_version": 1,
        "http": capability_http_codecs,
        "grpc": capability_grpc_codecs,
        "mcp": capability_mcp_codecs,
    });
    let capability_adapter_digest = canonical_digest(&capability_closure)
        .expect("the closed local remote Capability codec closure is canonical JSON");
    BTreeMap::from([
        (
            "context-native".to_owned(),
            (
                CONTEXT_NATIVE_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "observability_listen_address": loopback_address(ports.context_native_observability),
                    "worker_manifest": {
                        "manifest_version": 1,
                        "worker_role": "context-worker",
                        "work_class": "context",
                        "adapter_runtime_digest": digests.context_adapter,
                        "protocol_version": 1,
                        "max_concurrency": 4,
                        "critical_control_reserved_slots": 1,
                    },
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
        ),
        (
            "artifact-maintenance".to_owned(),
            (
                ARTIFACT_MAINTENANCE_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "listen_address": loopback_address(ports.artifact_maintenance_observability),
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "artifact_provider_catalog": artifact_provider_catalog,
                    "broker": {
                        "maximum_in_flight": 8,
                        "maximum_read_bytes": 67108864,
                        "operation_timeout_milliseconds": 5000,
                    },
                    "worker": {
                        "claim_batch": 4,
                        "lease_milliseconds": 120000,
                        "receipt_ttl_milliseconds": 3600000,
                        "poll_milliseconds": 250,
                    },
                    "shutdown_grace_milliseconds": 30000,
                }),
            ),
        ),
        (
            "security-authority".to_owned(),
            (
                SECURITY_AUTHORITY_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "listen_address": loopback_address(ports.security_authority),
                    "observability_listen_address": loopback_address(ports.security_authority_observability),
                    "service_principal_id": egress.service_principal_id,
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "maximum_rpc_message_bytes": 65536,
                    "tls_handshake_timeout_milliseconds": 5000,
                    "shutdown_grace_milliseconds": 30000,
                }),
            ),
        ),
        (
            "egress-broker".to_owned(),
            (
                EGRESS_BROKER_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "listen_address": loopback_address(ports.egress_broker),
                    "observability_listen_address": loopback_address(ports.egress_broker_observability),
                    "security_authority_endpoint": format!("https://localhost:{}", ports.security_authority),
                    "security_authority_tls_server_name": "localhost",
                    "maximum_rpc_metadata_bytes": 65536,
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
                    "model_endpoints": [],
                    "capability_http_endpoints": [],
                    "capability_grpc_endpoints": [],
                    "remote_context_endpoints": [],
                }),
            ),
        ),
        (
            "model-worker".to_owned(),
            (
                MODEL_WORKER_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "observability_listen_address": loopback_address(ports.model_worker_observability),
                    "worker_manifest": model_manifest,
                    "installed_adapters": [
                        {
                            "qualified_name": "anthropic.messages/2023-06-01",
                            "worker_manifest_digest": model_manifest_digest,
                            "adapter_contract_digest": digests.anthropic_contract,
                        },
                        {
                            "qualified_name": "openai.responses/v1",
                            "worker_manifest_digest": model_manifest_digest,
                            "adapter_contract_digest": digests.openai_contract,
                        }
                    ],
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "egress_endpoint": format!("https://localhost:{}/", ports.egress_broker),
                    "egress_tls_server_name": "localhost",
                    "egress_connect_timeout_milliseconds": 5000,
                    "egress_request_timeout_milliseconds": 30000,
                    "maximum_rpc_metadata_bytes": 65536,
                    "maximum_rpc_payload_bytes": 1048576,
                    "live_delta": {
                        "servers": ["tls://localhost:4222"],
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
        ),
        (
            "context-remote".to_owned(),
            (
                CONTEXT_REMOTE_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "observability_listen_address": loopback_address(ports.remote_context_worker_observability),
                    "worker_manifest": {
                        "manifest_version": 1,
                        "worker_role": "context-worker",
                        "work_class": "context",
                        "adapter_runtime_digest": digests.context_adapter,
                        "protocol_version": 1,
                        "max_concurrency": 4,
                        "critical_control_reserved_slots": 1,
                    },
                    "installed_adapter_digest": digests.context_adapter,
                    "egress_endpoint": format!("https://localhost:{}/", ports.egress_broker),
                    "egress_tls_server_name": "localhost",
                    "maximum_rpc_metadata_bytes": 65536,
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
        ),
        (
            "mcp-host".to_owned(),
            (
                MCP_HOST_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "listen_address": loopback_address(ports.mcp_host),
                    "observability_listen_address": loopback_address(ports.mcp_host_observability),
                    "tls_server_name": "localhost",
                    "maximum_rpc_message_bytes": 1048576,
                    "maximum_in_flight_requests": 8,
                    "egress": {
                        "endpoint": format!("https://localhost:{}/", ports.egress_broker),
                        "tls_server_name": "localhost",
                        "connect_timeout_milliseconds": 5000,
                        "request_timeout_milliseconds": 30000,
                        "maximum_rpc_metadata_bytes": 65536,
                        "maximum_rpc_payload_bytes": 1048576,
                    },
                    "drain_grace_milliseconds": 30000,
                }),
            ),
        ),
        (
            "mcp-resource-host".to_owned(),
            (
                MCP_RESOURCE_HOST_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "listen_address": loopback_address(ports.mcp_resource_host),
                    "observability_listen_address": loopback_address(ports.mcp_resource_host_observability),
                    "maximum_rpc_message_bytes": 1048576,
                    "maximum_in_flight_requests": 8,
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "egress": {
                        "endpoint": format!("https://localhost:{}/", ports.egress_broker),
                        "tls_server_name": "localhost",
                        "connect_timeout_milliseconds": 5000,
                        "request_timeout_milliseconds": 30000,
                        "maximum_rpc_metadata_bytes": 65536,
                        "maximum_rpc_payload_bytes": 1048576,
                    },
                    "drain_grace_milliseconds": 30000,
                }),
            ),
        ),
        (
            "capability-remote".to_owned(),
            (
                CAPABILITY_REMOTE_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "observability_listen_address": loopback_address(ports.capability_remote_observability),
                    "worker_manifest": {
                        "manifest_version": 1,
                        "worker_role": "capability.remote",
                        "work_class": "capability_remote",
                        "adapter_runtime_digest": capability_adapter_digest,
                        "protocol_version": 1,
                        "max_concurrency": 4,
                        "critical_control_reserved_slots": 1,
                    },
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
                        "endpoint": format!("https://localhost:{}/", ports.egress_broker),
                        "tls_server_name": "localhost",
                        "connect_timeout_milliseconds": 5000,
                        "request_timeout_milliseconds": 30000,
                        "maximum_rpc_metadata_bytes": 65536,
                        "maximum_rpc_payload_bytes": 1048576,
                    },
                    "mcp_host": {
                        "endpoint": format!("https://localhost:{}/", ports.mcp_host),
                        "tls_server_name": "localhost",
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
        ),
        (
            "mcp-discovery".to_owned(),
            (
                MCP_DISCOVERY_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "observability_listen_address": loopback_address(ports.mcp_discovery_observability),
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
                        "endpoint": format!("https://localhost:{}/", ports.egress_broker),
                        "tls_server_name": "localhost",
                        "connect_timeout_milliseconds": 5000,
                        "request_timeout_milliseconds": 30000,
                        "maximum_rpc_metadata_bytes": 65536,
                        "maximum_rpc_payload_bytes": 1048576,
                    },
                    "artifact_data_worker": {
                        "endpoint": format!("https://localhost:{}/", egress.artifact_data_worker_port),
                        "tls_server_name": "localhost",
                        "connect_timeout_milliseconds": 5000,
                        "request_timeout_milliseconds": 30000,
                        "maximum_read_request_bytes": 1048576,
                        "maximum_chunk_bytes": 262144,
                        "maximum_write_request_bytes": 67108864,
                    },
                }),
            ),
        ),
        (
            "mcp-subscription".to_owned(),
            (
                MCP_SUBSCRIPTION_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "observability_listen_address": loopback_address(ports.mcp_subscription_observability),
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
                        "endpoint": format!("https://localhost:{}/", ports.egress_broker),
                        "tls_server_name": "localhost",
                        "connect_timeout_milliseconds": 5000,
                        "request_timeout_milliseconds": 30000,
                        "maximum_rpc_metadata_bytes": 65536,
                        "maximum_rpc_payload_bytes": 1048576,
                    },
                }),
            ),
        ),
        (
            "mcp-cleanup".to_owned(),
            (
                MCP_CLEANUP_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "observability_listen_address": loopback_address(ports.mcp_cleanup_observability),
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "egress_endpoint": format!("https://localhost:{}/", ports.egress_broker),
                    "egress_tls_server_name": "localhost",
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
        ),
        (
            "context-subscription".to_owned(),
            (
                CONTEXT_SUBSCRIPTION_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "observability_listen_address": loopback_address(ports.context_subscription_observability),
                    "worker_manifest": {
                        "manifest_version": 1,
                        "worker_role": "context-worker",
                        "work_class": "context",
                        "adapter_runtime_digest": digests.context_adapter,
                        "protocol_version": 1,
                        "max_concurrency": 4,
                        "critical_control_reserved_slots": 1,
                    },
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "receipt_ttl_seconds": 3600,
                    "scan_interval_milliseconds": 500,
                    "failure_backoff_milliseconds": 100,
                    "drain_grace_milliseconds": 30000,
                    "host": {
                        "endpoint": format!("https://localhost:{}/", ports.mcp_resource_host),
                        "tls_server_name": "localhost",
                        "connect_timeout_milliseconds": 5000,
                        "request_timeout_milliseconds": 30000,
                        "maximum_rpc_message_bytes": 1048576,
                    },
                }),
            ),
        ),
        (
            "callback-api".to_owned(),
            (
                CALLBACK_API_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "listen_address": loopback_address(ports.callback_api),
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "egress_endpoint": format!("https://localhost:{}/", ports.egress_broker),
                    "egress_tls_server_name": "localhost",
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
        ),
        (
            "sandbox-attestor".to_owned(),
            (
                SANDBOX_ATTESTOR_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "process_observer": "local_unix",
                    "registration_socket_path": sandbox.runtime
                        .join(SANDBOX_STATE_DIRECTORY)
                        .join(SANDBOX_REGISTRATION_SOCKET_FILE)
                        .display().to_string(),
                    "controller_listen_address": loopback_address(ports.sandbox_attestor),
                    "observability_listen_address": loopback_address(ports.sandbox_attestor_observability),
                    "proc_root": "/",
                    "node_uid_authority_path": sandbox.local_instance_uid_path.display().to_string(),
                    "registry_path": sandbox.runtime
                        .join(SANDBOX_STATE_DIRECTORY)
                        .join(SANDBOX_REGISTRY_FILE)
                        .display().to_string(),
                    "maximum_registrations": 1024,
                    "absent_retention_seconds": 3900,
                    "attestor_identity_digest": closed_local_digest("sandbox-attestor-identity"),
                    "tls_handshake_timeout_milliseconds": 5000,
                    "shutdown_grace_milliseconds": 30000,
                    "allow_loopback_advertised_route": true,
                }),
            ),
        ),
        (
            "sandbox-controller".to_owned(),
            (
                SANDBOX_CONTROLLER_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "listen_address": loopback_address(ports.sandbox_controller),
                    "observability_listen_address": loopback_address(ports.sandbox_controller_observability),
                    "database_business_max_connections": 6,
                    "database_critical_control_max_connections": 2,
                    "outcome_convergence": {
                        "maximum_in_flight": 1,
                        "scan_interval_milliseconds": 1000,
                        "scan_jitter_milliseconds": 100,
                        "failure_backoff_milliseconds": 100,
                        "receipt_ttl_seconds": 7200,
                    },
                    "artifact_broker": {
                        "endpoint": format!("https://localhost:{}/", sandbox.artifact_data_worker_port),
                        "tls_server_name": "localhost",
                        "maximum_request_bytes": 1048576,
                        "maximum_chunk_bytes": 262144,
                        "maximum_in_flight_responses": 1,
                    },
                    "process_isolation_attestor": {
                        "tls_server_name": "sandbox-attestor.local",
                        "attestor_identity_digest": closed_local_digest("sandbox-attestor-identity"),
                        "maximum_cached_routes": 128,
                        "controller_port": ports.sandbox_attestor,
                        "allow_loopback_routes": true,
                        "allowed_node_cidrs": ["127.0.0.1/32"],
                    },
                    "connect_timeout_milliseconds": 5000,
                    "request_timeout_milliseconds": 30000,
                    "shutdown_grace_milliseconds": 30000,
                }),
            ),
        ),
        (
            "sandbox-executor".to_owned(),
            (
                SANDBOX_EXECUTOR_CONFIG_FILE,
                json!({
                    "schema_version": 1,
                    "worker_manifest": {
                        "manifest_version": 1,
                        "worker_role": "sandbox-executor.wasi",
                        "work_class": "sandbox",
                        "adapter_runtime_digest": closed_local_digest("sandbox-wasi-adapter"),
                        "protocol_version": 1,
                        "max_concurrency": 4,
                        "critical_control_reserved_slots": 1,
                    },
                    "backend": {
                        "kind": "wasi",
                        "runtime_version": WASI_ABI_V1_RUNTIME_VERSION,
                    },
                    "backend_contract_digest": closed_local_digest("sandbox-wasi-backend-contract"),
                    "authority_endpoint": format!("https://localhost:{}/", ports.sandbox_controller),
                    "authority_tls_server_name": "localhost",
                    "process_registration_attestor_socket_path": sandbox.runtime
                        .join(SANDBOX_STATE_DIRECTORY)
                        .join(SANDBOX_REGISTRATION_SOCKET_FILE)
                        .display().to_string(),
                    "process_registration_attestor_tls_server_name": "sandbox-attestor.local",
                    "process_registration_attestor_identity_digest": closed_local_digest("sandbox-attestor-identity"),
                    "nats_endpoint": "tls://localhost:4222",
                    "observability_listen_address": loopback_address(ports.sandbox_executor_observability),
                    "receipt_ttl_seconds": 7200,
                    "claim_scan_milliseconds": 100,
                    "claim_failure_backoff_milliseconds": 25,
                    "drain_grace_milliseconds": 30000,
                    "control_request_timeout_milliseconds": 5000,
                    "connect_timeout_milliseconds": 5000,
                    "request_timeout_milliseconds": 30000,
                }),
            ),
        ),
    ])
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

pub(crate) fn initial_process_launches(
    paths: ProcessPaths<'_>,
    ports: &PortBindings,
    config_digests: &BTreeMap<String, String>,
    database_url: &str,
    common_aws: &[(&str, &str)],
) -> Vec<ProcessLaunch> {
    let binary = |name: &str| {
        paths
            .release
            .join(format!("{name}{}", std::env::consts::EXE_SUFFIX))
    };
    vec![
        ProcessLaunch {
            role: "context-native",
            binary: binary(INITIAL_BINARY_NAMES[0]),
            ready_address: loopback_address(ports.context_native_observability),
            environment: vec![
                (
                    "PLATFORM_CONTEXT_WORKER_CONFIG",
                    paths
                        .configuration
                        .join(CONTEXT_NATIVE_CONFIG_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_CONTEXT_WORKER_CONFIG_DIGEST",
                    config_digests["context-native"].clone(),
                ),
                (
                    "PLATFORM_CONTEXT_WORKER_DATABASE_URL",
                    database_url.to_owned(),
                ),
            ],
            extra_environment: Vec::new(),
        },
        ProcessLaunch {
            role: "artifact-maintenance",
            binary: binary(INITIAL_BINARY_NAMES[1]),
            ready_address: loopback_address(ports.artifact_maintenance_observability),
            environment: vec![
                (
                    "PLATFORM_ARTIFACT_MAINTENANCE_CONFIG",
                    paths
                        .configuration
                        .join(ARTIFACT_MAINTENANCE_CONFIG_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_ARTIFACT_MAINTENANCE_CONFIG_DIGEST",
                    config_digests["artifact-maintenance"].clone(),
                ),
                (
                    "PLATFORM_ARTIFACT_MAINTENANCE_DATABASE_URL",
                    database_url.to_owned(),
                ),
            ],
            extra_environment: common_aws
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect(),
        },
        ProcessLaunch {
            role: "security-authority",
            binary: binary(INITIAL_BINARY_NAMES[2]),
            ready_address: loopback_address(ports.security_authority_observability),
            environment: vec![
                (
                    "PLATFORM_SECURITY_AUTHORITY_CONFIG",
                    paths
                        .configuration
                        .join(SECURITY_AUTHORITY_CONFIG_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SECURITY_AUTHORITY_CONFIG_DIGEST",
                    config_digests["security-authority"].clone(),
                ),
                (
                    "PLATFORM_SECURITY_AUTHORITY_DATABASE_URL",
                    database_url.to_owned(),
                ),
                (
                    "PLATFORM_SECURITY_AUTHORITY_CLIENT_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SECURITY_AUTHORITY_CERT_PATH",
                    paths
                        .tls
                        .join(SECURITY_AUTHORITY_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SECURITY_AUTHORITY_KEY_PATH",
                    paths
                        .tls
                        .join(SECURITY_AUTHORITY_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
            ],
            extra_environment: Vec::new(),
        },
        ProcessLaunch {
            role: "egress-broker",
            binary: binary(INITIAL_BINARY_NAMES[3]),
            ready_address: loopback_address(ports.egress_broker_observability),
            environment: vec![
                (
                    "PLATFORM_EGRESS_BROKER_CONFIG",
                    paths
                        .configuration
                        .join(EGRESS_BROKER_CONFIG_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_EGRESS_BROKER_CONFIG_DIGEST",
                    config_digests["egress-broker"].clone(),
                ),
                (
                    "PLATFORM_EGRESS_BROKER_AUTHORITY_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_EGRESS_BROKER_AUTHORITY_CERT_PATH",
                    paths
                        .tls
                        .join(EGRESS_BROKER_CLIENT_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_EGRESS_BROKER_AUTHORITY_KEY_PATH",
                    paths
                        .tls
                        .join(EGRESS_BROKER_CLIENT_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_EGRESS_BROKER_CLIENT_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_EGRESS_BROKER_CERT_PATH",
                    paths
                        .tls
                        .join(EGRESS_BROKER_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_EGRESS_BROKER_KEY_PATH",
                    paths
                        .tls
                        .join(EGRESS_BROKER_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
            ],
            extra_environment: common_aws
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                .collect(),
        },
        ProcessLaunch {
            role: "model-worker",
            binary: binary(INITIAL_BINARY_NAMES[4]),
            ready_address: loopback_address(ports.model_worker_observability),
            environment: vec![
                (
                    "PLATFORM_MODEL_WORKER_CONFIG",
                    paths
                        .configuration
                        .join(MODEL_WORKER_CONFIG_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MODEL_WORKER_CONFIG_DIGEST",
                    config_digests["model-worker"].clone(),
                ),
                (
                    "PLATFORM_MODEL_WORKER_DATABASE_URL",
                    database_url.to_owned(),
                ),
                (
                    "PLATFORM_MODEL_WORKER_EGRESS_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MODEL_WORKER_EGRESS_CERT_PATH",
                    paths
                        .tls
                        .join(MODEL_WORKER_CLIENT_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MODEL_WORKER_EGRESS_KEY_PATH",
                    paths
                        .tls
                        .join(MODEL_WORKER_CLIENT_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MODEL_WORKER_NATS_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MODEL_WORKER_NATS_CERT_PATH",
                    paths
                        .tls
                        .join(paths.nats_client_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MODEL_WORKER_NATS_KEY_PATH",
                    paths
                        .tls
                        .join(paths.nats_client_private_key_file)
                        .display()
                        .to_string(),
                ),
            ],
            extra_environment: Vec::new(),
        },
        ProcessLaunch {
            role: "context-remote",
            binary: binary(INITIAL_BINARY_NAMES[5]),
            ready_address: loopback_address(ports.remote_context_worker_observability),
            environment: vec![
                (
                    "PLATFORM_REMOTE_CONTEXT_WORKER_CONFIG",
                    paths
                        .configuration
                        .join(CONTEXT_REMOTE_CONFIG_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_REMOTE_CONTEXT_WORKER_CONFIG_DIGEST",
                    config_digests["context-remote"].clone(),
                ),
                (
                    "PLATFORM_REMOTE_CONTEXT_WORKER_DATABASE_URL",
                    database_url.to_owned(),
                ),
                (
                    "PLATFORM_REMOTE_CONTEXT_WORKER_EGRESS_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_REMOTE_CONTEXT_WORKER_EGRESS_CERT_PATH",
                    paths
                        .tls
                        .join(CONTEXT_WORKER_CLIENT_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_REMOTE_CONTEXT_WORKER_EGRESS_KEY_PATH",
                    paths
                        .tls
                        .join(CONTEXT_WORKER_CLIENT_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
            ],
            extra_environment: Vec::new(),
        },
        ProcessLaunch {
            role: "mcp-host",
            binary: binary(INITIAL_BINARY_NAMES[6]),
            ready_address: loopback_address(ports.mcp_host_observability),
            environment: vec![
                (
                    "PLATFORM_MCP_HOST_CONFIG",
                    paths
                        .configuration
                        .join(MCP_HOST_CONFIG_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MCP_HOST_CONFIG_DIGEST",
                    config_digests["mcp-host"].clone(),
                ),
                (
                    "PLATFORM_MCP_HOST_SERVER_CLIENT_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MCP_HOST_SERVER_CERT_PATH",
                    paths
                        .tls
                        .join(MCP_HOST_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MCP_HOST_SERVER_KEY_PATH",
                    paths
                        .tls
                        .join(MCP_HOST_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MCP_HOST_EGRESS_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MCP_HOST_EGRESS_CERT_PATH",
                    paths
                        .tls
                        .join(MCP_HOST_EGRESS_CLIENT_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MCP_HOST_EGRESS_KEY_PATH",
                    paths
                        .tls
                        .join(MCP_HOST_EGRESS_CLIENT_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
            ],
            extra_environment: Vec::new(),
        },
        ProcessLaunch {
            role: "mcp-resource-host",
            binary: binary(INITIAL_BINARY_NAMES[7]),
            ready_address: loopback_address(ports.mcp_resource_host_observability),
            environment: vec![
                (
                    "PLATFORM_MCP_RESOURCE_HOST_CONFIG",
                    paths
                        .configuration
                        .join(MCP_RESOURCE_HOST_CONFIG_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MCP_RESOURCE_HOST_CONFIG_DIGEST",
                    config_digests["mcp-resource-host"].clone(),
                ),
                (
                    "PLATFORM_MCP_RESOURCE_HOST_DATABASE_URL",
                    database_url.to_owned(),
                ),
                (
                    "PLATFORM_MCP_RESOURCE_HOST_SERVER_CLIENT_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MCP_RESOURCE_HOST_SERVER_CERT_PATH",
                    paths
                        .tls
                        .join(MCP_RESOURCE_HOST_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MCP_RESOURCE_HOST_SERVER_KEY_PATH",
                    paths
                        .tls
                        .join(MCP_RESOURCE_HOST_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MCP_RESOURCE_HOST_EGRESS_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MCP_RESOURCE_HOST_EGRESS_CERT_PATH",
                    paths
                        .tls
                        .join(MCP_RESOURCE_EGRESS_CLIENT_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MCP_RESOURCE_HOST_EGRESS_KEY_PATH",
                    paths
                        .tls
                        .join(MCP_RESOURCE_EGRESS_CLIENT_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
            ],
            extra_environment: Vec::new(),
        },
        ProcessLaunch {
            role: "capability-remote",
            binary: binary(INITIAL_BINARY_NAMES[8]),
            ready_address: loopback_address(ports.capability_remote_observability),
            environment: vec![
                (
                    "PLATFORM_CAPABILITY_REMOTE_WORKER_CONFIG",
                    paths
                        .configuration
                        .join(CAPABILITY_REMOTE_CONFIG_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_CAPABILITY_REMOTE_WORKER_CONFIG_DIGEST",
                    config_digests["capability-remote"].clone(),
                ),
                (
                    "PLATFORM_CAPABILITY_REMOTE_WORKER_DATABASE_URL",
                    database_url.to_owned(),
                ),
                (
                    "PLATFORM_CAPABILITY_REMOTE_WORKER_EGRESS_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_CAPABILITY_REMOTE_WORKER_EGRESS_CERT_PATH",
                    paths
                        .tls
                        .join(CAPABILITY_REMOTE_CLIENT_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_CAPABILITY_REMOTE_WORKER_EGRESS_KEY_PATH",
                    paths
                        .tls
                        .join(CAPABILITY_REMOTE_CLIENT_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_CAPABILITY_REMOTE_WORKER_MCP_HOST_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_CAPABILITY_REMOTE_WORKER_MCP_HOST_CERT_PATH",
                    paths
                        .tls
                        .join(CAPABILITY_REMOTE_CLIENT_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_CAPABILITY_REMOTE_WORKER_MCP_HOST_KEY_PATH",
                    paths
                        .tls
                        .join(CAPABILITY_REMOTE_CLIENT_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
            ],
            extra_environment: Vec::new(),
        },
        ProcessLaunch {
            role: "mcp-discovery",
            binary: binary(INITIAL_BINARY_NAMES[9]),
            ready_address: loopback_address(ports.mcp_discovery_observability),
            environment: vec![
                (
                    "PLATFORM_MCP_DISCOVERY_WORKER_CONFIG",
                    paths
                        .configuration
                        .join(MCP_DISCOVERY_CONFIG_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MCP_DISCOVERY_WORKER_CONFIG_DIGEST",
                    config_digests["mcp-discovery"].clone(),
                ),
                (
                    "PLATFORM_MCP_DISCOVERY_WORKER_DATABASE_URL",
                    database_url.to_owned(),
                ),
                (
                    "PLATFORM_MCP_DISCOVERY_WORKER_CLIENT_CERT_PATH",
                    paths
                        .tls
                        .join(MCP_DISCOVERY_CLIENT_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MCP_DISCOVERY_WORKER_CLIENT_KEY_PATH",
                    paths
                        .tls
                        .join(MCP_DISCOVERY_CLIENT_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MCP_DISCOVERY_WORKER_EGRESS_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MCP_DISCOVERY_WORKER_ARTIFACT_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
            ],
            extra_environment: Vec::new(),
        },
        ProcessLaunch {
            role: "mcp-subscription",
            binary: binary(INITIAL_BINARY_NAMES[10]),
            ready_address: loopback_address(ports.mcp_subscription_observability),
            environment: vec![
                (
                    "PLATFORM_MCP_SUBSCRIPTION_WORKER_CONFIG",
                    paths
                        .configuration
                        .join(MCP_SUBSCRIPTION_CONFIG_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MCP_SUBSCRIPTION_WORKER_CONFIG_DIGEST",
                    config_digests["mcp-subscription"].clone(),
                ),
                (
                    "PLATFORM_MCP_SUBSCRIPTION_WORKER_DATABASE_URL",
                    database_url.to_owned(),
                ),
                (
                    "PLATFORM_MCP_SUBSCRIPTION_WORKER_CLIENT_CERT_PATH",
                    paths
                        .tls
                        .join(MCP_SUBSCRIPTION_CLIENT_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MCP_SUBSCRIPTION_WORKER_CLIENT_KEY_PATH",
                    paths
                        .tls
                        .join(MCP_SUBSCRIPTION_CLIENT_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MCP_SUBSCRIPTION_WORKER_EGRESS_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
            ],
            extra_environment: Vec::new(),
        },
        ProcessLaunch {
            role: "mcp-cleanup",
            binary: binary(INITIAL_BINARY_NAMES[11]),
            ready_address: loopback_address(ports.mcp_cleanup_observability),
            environment: vec![
                (
                    "PLATFORM_MCP_CLEANUP_CONFIG",
                    paths
                        .configuration
                        .join(MCP_CLEANUP_CONFIG_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MCP_CLEANUP_CONFIG_DIGEST",
                    config_digests["mcp-cleanup"].clone(),
                ),
                ("PLATFORM_MCP_CLEANUP_DATABASE_URL", database_url.to_owned()),
                (
                    "PLATFORM_MCP_CLEANUP_EGRESS_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MCP_CLEANUP_EGRESS_CERT_PATH",
                    paths
                        .tls
                        .join(MCP_CLEANUP_CLIENT_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_MCP_CLEANUP_EGRESS_KEY_PATH",
                    paths
                        .tls
                        .join(MCP_CLEANUP_CLIENT_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
            ],
            extra_environment: Vec::new(),
        },
        ProcessLaunch {
            role: "context-subscription",
            binary: binary(INITIAL_BINARY_NAMES[12]),
            ready_address: loopback_address(ports.context_subscription_observability),
            environment: vec![
                (
                    "PLATFORM_SUBSCRIPTION_CONTEXT_WORKER_CONFIG",
                    paths
                        .configuration
                        .join(CONTEXT_SUBSCRIPTION_CONFIG_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SUBSCRIPTION_CONTEXT_WORKER_CONFIG_DIGEST",
                    config_digests["context-subscription"].clone(),
                ),
                (
                    "PLATFORM_SUBSCRIPTION_CONTEXT_WORKER_DATABASE_URL",
                    database_url.to_owned(),
                ),
                (
                    "PLATFORM_SUBSCRIPTION_CONTEXT_WORKER_HOST_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SUBSCRIPTION_CONTEXT_WORKER_HOST_CERT_PATH",
                    paths
                        .tls
                        .join(CONTEXT_SUBSCRIPTION_CLIENT_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SUBSCRIPTION_CONTEXT_WORKER_HOST_KEY_PATH",
                    paths
                        .tls
                        .join(CONTEXT_SUBSCRIPTION_CLIENT_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
            ],
            extra_environment: Vec::new(),
        },
        ProcessLaunch {
            role: "callback-api",
            binary: binary(INITIAL_BINARY_NAMES[13]),
            ready_address: loopback_address(ports.callback_api),
            environment: vec![
                (
                    "PLATFORM_CALLBACK_API_CONFIG",
                    paths
                        .configuration
                        .join(CALLBACK_API_CONFIG_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_CALLBACK_API_CONFIG_DIGEST",
                    config_digests["callback-api"].clone(),
                ),
                (
                    "PLATFORM_CALLBACK_API_DATABASE_URL",
                    database_url.to_owned(),
                ),
                (
                    "PLATFORM_CALLBACK_API_EGRESS_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_CALLBACK_API_EGRESS_CERT_PATH",
                    paths
                        .tls
                        .join(CALLBACK_CLIENT_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_CALLBACK_API_EGRESS_KEY_PATH",
                    paths
                        .tls
                        .join(CALLBACK_CLIENT_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
            ],
            extra_environment: Vec::new(),
        },
        ProcessLaunch {
            role: "sandbox-attestor",
            binary: binary(INITIAL_BINARY_NAMES[14]),
            ready_address: loopback_address(ports.sandbox_attestor_observability),
            environment: vec![
                (
                    "PLATFORM_SANDBOX_ATTESTOR_CONFIG",
                    paths
                        .configuration
                        .join(SANDBOX_ATTESTOR_CONFIG_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SANDBOX_ATTESTOR_CONFIG_DIGEST",
                    config_digests["sandbox-attestor"].clone(),
                ),
                (
                    "PLATFORM_SANDBOX_ATTESTOR_EXECUTOR_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SANDBOX_ATTESTOR_REGISTRATION_CERT_PATH",
                    paths
                        .tls
                        .join(SANDBOX_ATTESTOR_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SANDBOX_ATTESTOR_REGISTRATION_KEY_PATH",
                    paths
                        .tls
                        .join(SANDBOX_ATTESTOR_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SANDBOX_ATTESTOR_CONTROLLER_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SANDBOX_ATTESTOR_CONTROLLER_CERT_PATH",
                    paths
                        .tls
                        .join(SANDBOX_ATTESTOR_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SANDBOX_ATTESTOR_CONTROLLER_KEY_PATH",
                    paths
                        .tls
                        .join(SANDBOX_ATTESTOR_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SANDBOX_ATTESTOR_ADVERTISED_IP",
                    "127.0.0.1".to_owned(),
                ),
            ],
            extra_environment: Vec::new(),
        },
        ProcessLaunch {
            role: "sandbox-controller",
            binary: binary(INITIAL_BINARY_NAMES[15]),
            ready_address: loopback_address(ports.sandbox_controller_observability),
            environment: vec![
                (
                    "PLATFORM_SANDBOX_CONTROLLER_CONFIG",
                    paths
                        .configuration
                        .join(SANDBOX_CONTROLLER_CONFIG_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SANDBOX_CONTROLLER_CONFIG_DIGEST",
                    config_digests["sandbox-controller"].clone(),
                ),
                ("PLATFORM_DATABASE_URL", database_url.to_owned()),
                (
                    "PLATFORM_SANDBOX_GRPC_CLIENT_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SANDBOX_GRPC_CERT_PATH",
                    paths
                        .tls
                        .join(SANDBOX_CONTROLLER_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SANDBOX_GRPC_KEY_PATH",
                    paths
                        .tls
                        .join(SANDBOX_CONTROLLER_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SANDBOX_ATTESTOR_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SANDBOX_ATTESTOR_CERT_PATH",
                    paths
                        .tls
                        .join(SANDBOX_CONTROLLER_CLIENT_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SANDBOX_ATTESTOR_KEY_PATH",
                    paths
                        .tls
                        .join(SANDBOX_CONTROLLER_CLIENT_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SANDBOX_ARTIFACT_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SANDBOX_ARTIFACT_CERT_PATH",
                    paths
                        .tls
                        .join(SANDBOX_CONTROLLER_CLIENT_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SANDBOX_ARTIFACT_KEY_PATH",
                    paths
                        .tls
                        .join(SANDBOX_CONTROLLER_CLIENT_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
            ],
            extra_environment: Vec::new(),
        },
        ProcessLaunch {
            role: "sandbox-executor",
            binary: binary(INITIAL_BINARY_NAMES[16]),
            ready_address: loopback_address(ports.sandbox_executor_observability),
            environment: vec![
                (
                    "PLATFORM_SANDBOX_EXECUTOR_CONFIG",
                    paths
                        .configuration
                        .join(SANDBOX_EXECUTOR_CONFIG_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SANDBOX_EXECUTOR_CONFIG_DIGEST",
                    config_digests["sandbox-executor"].clone(),
                ),
                (
                    "PLATFORM_SANDBOX_GRPC_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SANDBOX_GRPC_CERT_PATH",
                    paths
                        .tls
                        .join(SANDBOX_EXECUTOR_CLIENT_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SANDBOX_GRPC_KEY_PATH",
                    paths
                        .tls
                        .join(SANDBOX_EXECUTOR_CLIENT_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SANDBOX_NATS_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SANDBOX_NATS_CERT_PATH",
                    paths
                        .tls
                        .join(SANDBOX_EXECUTOR_CLIENT_CERTIFICATE_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SANDBOX_NATS_KEY_PATH",
                    paths
                        .tls
                        .join(SANDBOX_EXECUTOR_CLIENT_PRIVATE_KEY_FILE)
                        .display()
                        .to_string(),
                ),
                (
                    "PLATFORM_SANDBOX_ATTESTOR_CA_PATH",
                    paths
                        .tls
                        .join(paths.ca_certificate_file)
                        .display()
                        .to_string(),
                ),
            ],
            extra_environment: Vec::new(),
        },
    ]
}

fn loopback_address(port: u16) -> String {
    format!("127.0.0.1:{port}")
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
            sandbox_attestor: 31_019,
            sandbox_attestor_observability: 31_020,
            sandbox_controller: 31_021,
            sandbox_controller_observability: 31_022,
            sandbox_executor_observability: 31_023,
        };
        let catalog = json!({"schema_version": 1});
        let adapter = digest('a');
        let contract = digest('b');
        let principal = "prn_0198f1c3-8f49-7c3e-b1f3-773c28367b90";
        let configs = initial_configs(
            &ports,
            &catalog,
            WorkerDigests {
                context_adapter: &adapter,
                context_contract: &contract,
                model_adapter: &digest('d'),
                anthropic_contract: &digest('e'),
                openai_contract: &digest('f'),
            },
            EgressConfigInputs {
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
                artifact_data_worker_port: 30_999,
            },
            SandboxConfigInputs {
                runtime: Path::new("/project/runtime"),
                local_instance_uid_path: Path::new("/project/runtime/sandbox-local-instance-uid"),
                artifact_data_worker_port: 30_999,
            },
        );

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
            configs["sandbox-attestor"].1["process_observer"],
            "local_unix"
        );
        assert_eq!(
            configs["sandbox-controller"].1["process_isolation_attestor"]["allowed_node_cidrs"][0],
            "127.0.0.1/32"
        );
        assert_eq!(
            configs["sandbox-executor"].1["backend"]["runtime_version"],
            WASI_ABI_V1_RUNTIME_VERSION
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
            sandbox_attestor: 31_019,
            sandbox_attestor_observability: 31_020,
            sandbox_controller: 31_021,
            sandbox_controller_observability: 31_022,
            sandbox_executor_observability: 31_023,
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
            ("sandbox-attestor".to_owned(), digest('9')),
            ("sandbox-controller".to_owned(), digest('a')),
            ("sandbox-executor".to_owned(), digest('b')),
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
            "postgres://local-authority",
            &[("AWS_ACCESS_KEY_ID", "test")],
        );
        assert_eq!(
            launches
                .iter()
                .map(|launch| launch.role)
                .collect::<Vec<_>>(),
            vec![
                "context-native",
                "artifact-maintenance",
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
                "sandbox-attestor",
                "sandbox-controller",
                "sandbox-executor"
            ]
        );
        assert_eq!(launches[0].ready_address, "127.0.0.1:31001");
        assert_eq!(launches[1].ready_address, "127.0.0.1:31002");
        assert_eq!(launches[2].ready_address, "127.0.0.1:31004");
        assert_eq!(launches[3].ready_address, "127.0.0.1:31006");
        assert_eq!(launches[4].ready_address, "127.0.0.1:31007");
        assert_eq!(launches[5].ready_address, "127.0.0.1:31008");
        assert_eq!(launches[6].ready_address, "127.0.0.1:31010");
        assert_eq!(launches[7].ready_address, "127.0.0.1:31012");
        assert_eq!(launches[8].ready_address, "127.0.0.1:31013");
        assert_eq!(launches[9].ready_address, "127.0.0.1:31014");
        assert_eq!(launches[10].ready_address, "127.0.0.1:31015");
        assert_eq!(launches[11].ready_address, "127.0.0.1:31016");
        assert_eq!(launches[12].ready_address, "127.0.0.1:31017");
        assert_eq!(launches[13].ready_address, "127.0.0.1:31018");
        assert_eq!(launches[14].ready_address, "127.0.0.1:31020");
        assert_eq!(launches[15].ready_address, "127.0.0.1:31022");
        assert_eq!(launches[16].ready_address, "127.0.0.1:31023");
        assert!(launches
            .iter()
            .all(|launch| launch.environment.iter().any(|(name, value)| name
                .ends_with("CONFIG_DIGEST")
                && value.starts_with("sha256:"))));
        assert_eq!(
            launches[1].extra_environment,
            vec![("AWS_ACCESS_KEY_ID".to_owned(), "test".to_owned())]
        );
        assert!(launches[2].environment.iter().any(|(name, value)| *name
            == "PLATFORM_SECURITY_AUTHORITY_CLIENT_CA_PATH"
            && value == "/project/runtime/tls/ca.pem"));
        assert_eq!(
            launches[3].extra_environment,
            vec![("AWS_ACCESS_KEY_ID".to_owned(), "test".to_owned())]
        );
        assert!(launches[3].environment.iter().any(|(name, value)| *name
            == "PLATFORM_EGRESS_BROKER_AUTHORITY_CERT_PATH"
            && value == "/project/runtime/tls/egress-broker-client.pem"));
        assert!(launches[4].environment.iter().any(|(name, value)| *name
            == "PLATFORM_MODEL_WORKER_EGRESS_CERT_PATH"
            && value == "/project/runtime/tls/model-worker-client.pem"));
        assert!(launches[5].environment.iter().any(|(name, value)| *name
            == "PLATFORM_REMOTE_CONTEXT_WORKER_EGRESS_CERT_PATH"
            && value == "/project/runtime/tls/context-worker-client.pem"));
        assert!(launches[6].environment.iter().any(|(name, value)| *name
            == "PLATFORM_MCP_HOST_EGRESS_CERT_PATH"
            && value == "/project/runtime/tls/mcp-host-egress-client.pem"));
        assert!(launches[7].environment.iter().any(|(name, value)| *name
            == "PLATFORM_MCP_RESOURCE_HOST_SERVER_CERT_PATH"
            && value == "/project/runtime/tls/mcp-resource-host.pem"));
        assert!(launches[8].environment.iter().any(|(name, value)| *name
            == "PLATFORM_CAPABILITY_REMOTE_WORKER_MCP_HOST_CERT_PATH"
            && value == "/project/runtime/tls/capability-remote-client.pem"));
        assert!(launches[9].environment.iter().any(|(name, value)| *name
            == "PLATFORM_MCP_DISCOVERY_WORKER_CLIENT_CERT_PATH"
            && value == "/project/runtime/tls/mcp-discovery-client.pem"));
        assert!(launches[10].environment.iter().any(|(name, value)| *name
            == "PLATFORM_MCP_SUBSCRIPTION_WORKER_CLIENT_CERT_PATH"
            && value == "/project/runtime/tls/mcp-subscription-client.pem"));
        assert!(launches[11].environment.iter().any(|(name, value)| *name
            == "PLATFORM_MCP_CLEANUP_EGRESS_CERT_PATH"
            && value == "/project/runtime/tls/mcp-cleanup-client.pem"));
        assert!(launches[12].environment.iter().any(|(name, value)| *name
            == "PLATFORM_SUBSCRIPTION_CONTEXT_WORKER_HOST_CERT_PATH"
            && value == "/project/runtime/tls/context-subscription-client.pem"));
        assert!(launches[13].environment.iter().any(|(name, value)| *name
            == "PLATFORM_CALLBACK_API_EGRESS_CERT_PATH"
            && value == "/project/runtime/tls/callback-client.pem"));
        assert!(launches[14].environment.iter().any(|(name, value)| *name
            == "PLATFORM_SANDBOX_ATTESTOR_ADVERTISED_IP"
            && value == "127.0.0.1"));
        assert!(launches[15].environment.iter().any(|(name, value)| *name
            == "PLATFORM_SANDBOX_ARTIFACT_CERT_PATH"
            && value == "/project/runtime/tls/sandbox-controller-client.pem"));
        assert!(launches[16]
            .environment
            .iter()
            .any(|(name, value)| *name == "PLATFORM_SANDBOX_NATS_CERT_PATH"
                && value == "/project/runtime/tls/sandbox-executor-client.pem"));
    }

    #[test]
    fn persisted_pre_egress_ports_replay_with_fixed_additive_defaults() {
        let ports: PortBindings = serde_json::from_value(json!({
            "context_native_observability": 31_001,
            "artifact_maintenance_observability": 31_002,
            "security_authority": 31_003,
            "security_authority_observability": 31_004
        }))
        .unwrap();
        assert_eq!(ports.egress_broker, 19_099);
        assert_eq!(ports.egress_broker_observability, 19_100);
        assert_eq!(ports.model_worker_observability, 19_101);
        assert_eq!(ports.remote_context_worker_observability, 19_102);
        assert_eq!(ports.mcp_host, 19_103);
        assert_eq!(ports.mcp_host_observability, 19_104);
        assert_eq!(ports.mcp_resource_host, 19_105);
        assert_eq!(ports.mcp_resource_host_observability, 19_106);
        assert_eq!(ports.capability_remote_observability, 19_107);
        assert_eq!(ports.mcp_discovery_observability, 19_108);
        assert_eq!(ports.mcp_subscription_observability, 19_109);
        assert_eq!(ports.mcp_cleanup_observability, 19_110);
        assert_eq!(ports.context_subscription_observability, 19_111);
        assert_eq!(ports.callback_api, 19_112);
        assert_eq!(ports.sandbox_attestor, 19_113);
        assert_eq!(ports.sandbox_attestor_observability, 19_114);
        assert_eq!(ports.sandbox_controller, 19_115);
        assert_eq!(ports.sandbox_controller_observability, 19_116);
        assert_eq!(ports.sandbox_executor_observability, 19_117);
    }
}
