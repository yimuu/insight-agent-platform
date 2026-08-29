//! Closed configuration fragments for the additive local `full` development profile.
//!
//! This module deliberately contains only product-facing process configuration. It does not
//! connect to an authority or execute worker logic; each generated document is consumed and
//! revalidated by the corresponding independent Platform process.

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
pub(crate) const SECURITY_AUTHORITY_CERTIFICATE_FILE: &str = "security-authority.pem";
pub(crate) const SECURITY_AUTHORITY_PRIVATE_KEY_FILE: &str = "security-authority-key.pem";
pub(crate) const EGRESS_BROKER_CLIENT_CERTIFICATE_FILE: &str = "egress-broker-client.pem";
pub(crate) const EGRESS_BROKER_CLIENT_PRIVATE_KEY_FILE: &str = "egress-broker-client-key.pem";
pub(crate) const EGRESS_BROKER_CERTIFICATE_FILE: &str = "egress-broker.pem";
pub(crate) const EGRESS_BROKER_PRIVATE_KEY_FILE: &str = "egress-broker-key.pem";
pub(crate) const MCP_STATE_KEY_DIRECTORY: &str = "mcp-state-keys";
pub(crate) const MCP_STATE_KEY_FILE: &str = "current";
pub(crate) const EGRESS_BROKER_WORKLOAD_IDENTITY: &str =
    "spiffe://insight.platform/workload/egress-broker";

pub(crate) const INITIAL_BINARY_NAMES: [&str; 4] = [
    "platform-context-worker",
    "platform-artifact-maintenance",
    "platform-security-authority",
    "platform-egress-broker",
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
}

pub(crate) struct EgressConfigInputs<'a> {
    pub(crate) service_principal_id: &'a str,
    pub(crate) secret_provider_catalog: &'a Value,
    pub(crate) mcp_state_key_root: &'a Path,
    pub(crate) mcp_state_key_path: &'a Path,
    pub(crate) mcp_state_key_reference_digest: &'a str,
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
        }
    }
}

const fn default_egress_broker_port() -> u16 {
    19_099
}

const fn default_egress_broker_observability_port() -> u16 {
    19_100
}

impl Default for PortBindings {
    fn default() -> Self {
        Self::legacy_defaults()
    }
}

pub(crate) fn initial_configs(
    ports: &PortBindings,
    artifact_provider_catalog: &Value,
    context_adapter_digest: &str,
    context_contract_digest: &str,
    egress: EgressConfigInputs<'_>,
) -> BTreeMap<String, (&'static str, Value)> {
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
                        "adapter_runtime_digest": context_adapter_digest,
                        "protocol_version": 1,
                        "max_concurrency": 4,
                        "critical_control_reserved_slots": 1,
                    },
                    "native_catalog": {
                        "schema_version": 1,
                        "installed_adapter_digest": context_adapter_digest,
                        "adapter_contract_digest": context_contract_digest,
                        "source_item_identity_digest": context_contract_digest,
                        "content": "local deterministic context item",
                        "structured_fields_schema_digest": context_contract_digest,
                        "score_millionths": 500000,
                        "locator_digest": context_contract_digest,
                        "authorization_evidence_digest": context_contract_digest,
                        "ranking_evidence_digest": context_contract_digest,
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
    ])
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
        };
        let catalog = json!({"schema_version": 1});
        let adapter = digest('a');
        let contract = digest('b');
        let principal = "prn_0198f1c3-8f49-7c3e-b1f3-773c28367b90";
        let configs = initial_configs(
            &ports,
            &catalog,
            &adapter,
            &contract,
            EgressConfigInputs {
                service_principal_id: principal,
                secret_provider_catalog: &json!({"schema_version": 1, "providers": [{"provider": "closed"}]}),
                mcp_state_key_root: Path::new("/project/runtime/mcp-state-keys"),
                mcp_state_key_path: Path::new("/project/runtime/mcp-state-keys/current"),
                mcp_state_key_reference_digest: &digest('c'),
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
        };
        let digests = BTreeMap::from([
            ("context-native".to_owned(), digest('a')),
            ("artifact-maintenance".to_owned(), digest('b')),
            ("security-authority".to_owned(), digest('c')),
            ("egress-broker".to_owned(), digest('d')),
        ]);
        let launches = initial_process_launches(
            ProcessPaths {
                release: Path::new("/workspace/target/release"),
                configuration: Path::new("/project/runtime/config"),
                tls: Path::new("/project/runtime/tls"),
                ca_certificate_file: "ca.pem",
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
                "egress-broker"
            ]
        );
        assert_eq!(launches[0].ready_address, "127.0.0.1:31001");
        assert_eq!(launches[1].ready_address, "127.0.0.1:31002");
        assert_eq!(launches[2].ready_address, "127.0.0.1:31004");
        assert_eq!(launches[3].ready_address, "127.0.0.1:31006");
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
    }
}
