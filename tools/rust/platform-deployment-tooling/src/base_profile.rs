//! Base process configuration shared by native CLI and container installation.
use crate::{
    worker_profile::{self, WorkerBuilds},
    DeploymentError,
};
use insight_platform_contracts::{canonical_digest, ResourceId};
use insight_platform_deployment_contracts::{
    development::DevelopmentArtifactAuthorityConfigV1,
    installation::{InstallationProcess as Process, NetworkTopologyV1},
};
use serde_json::Value;
use std::collections::BTreeMap;

pub const OUTBOX_CONFIG_FILE: &str = "outbox-worker.json";
pub const HISTORY_CONFIG_FILE: &str = "history-maintenance.json";
pub const RUNTIME_GATEWAY_MANAGEMENT_CONFIG_FILE: &str = "gateway-management.json";
pub const RUNTIME_GATEWAY_RUNTIME_CONFIG_FILE: &str = "gateway-runtime.json";
pub const RUNTIME_ARTIFACT_GATEWAY_CONFIG_FILE: &str = "artifact-gateway.json";
pub const RUNTIME_ARTIFACT_BOOTSTRAP_CONFIG_FILE: &str = "artifact-bootstrap.json";
pub const RUNTIME_ARTIFACT_DATA_CONFIG_FILE: &str = "artifact-data.json";
pub const RUNTIME_ORCHESTRATION_CONFIG_FILE: &str = "orchestration.json";
pub const RUNTIME_CAPABILITY_NATIVE_CONFIG_FILE: &str = "capability-native.json";
pub const RUNTIME_REGISTRY_VALIDATION_CONFIG_FILE: &str = "registry-validation.json";

pub struct BaseIdentity<'a> {
    pub encryption_domain_id: &'a ResourceId,
    pub registry_validator_principal_id: &'a ResourceId,
}
pub struct BaseConfigInputs<'a> {
    pub network: &'a NetworkTopologyV1,
    pub worker_builds: &'a WorkerBuilds,
    pub identity: BaseIdentity<'a>,
    pub oidc: &'a Value,
    pub artifact_provider_catalog: &'a Value,
    pub artifact_bootstrap: &'a DevelopmentArtifactAuthorityConfigV1,
    pub model_installation: Option<&'a insight_platform_contracts::ModelInstallationCatalogV2>,
}
pub fn configurations(
    inputs: BaseConfigInputs<'_>,
) -> Result<BTreeMap<String, (&'static str, Value)>, DeploymentError> {
    let BaseConfigInputs {
        network,
        worker_builds,
        identity,
        oidc,
        artifact_provider_catalog: catalog,
        artifact_bootstrap,
        model_installation,
    } = inputs;
    network.validate()?;
    if model_installation.is_some_and(|catalog| !catalog.validate()) {
        return Err(DeploymentError::Configuration(
            "Model installation catalog invalid".into(),
        ));
    }
    artifact_bootstrap
        .validate()
        .map_err(|_| DeploymentError::Configuration("Artifact bootstrap invalid".into()))?;
    let scanner_contract_digest = artifact_bootstrap
        .artifact_io_policy
        .scanner_contract_digest
        .to_string();
    let scanner_ruleset_digest = artifact_bootstrap
        .artifact_io_policy
        .canonical_digest()
        .map_err(|_| DeploymentError::Configuration("Artifact scanner rules invalid".into()))?
        .to_string();
    let artifact_bootstrap = serde_json::to_value(artifact_bootstrap)
        .map_err(|_| DeploymentError::Configuration("Artifact bootstrap invalid".into()))?;
    let orchestration_adapter_digest = local_digest("orchestration-worker")?;
    let registry_validator_digest = local_digest("registry-validator")?;
    let registry_validation_profile_digest = local_digest("registry-validation-profile")?;
    let configurations = BTreeMap::from([
        (
            "outbox".to_owned(),
            (OUTBOX_CONFIG_FILE, outbox_config(network)?),
        ),
        (
            "history-maintenance".to_owned(),
            (HISTORY_CONFIG_FILE, history_config(network, worker_builds)?),
        ),
        (
            "artifact-bootstrap".to_owned(),
            (RUNTIME_ARTIFACT_BOOTSTRAP_CONFIG_FILE, artifact_bootstrap),
        ),
        (
            "gateway-management".to_owned(),
            (
                RUNTIME_GATEWAY_MANAGEMENT_CONFIG_FILE,
                serde_json::json!({
                    "schema_version": 1,
                    "role": "management_api",
                    "listen_address": network.observability(Process::GatewayManagement)?,
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "shutdown_grace_milliseconds": 30000,
                    "registry_validator_digest": registry_validator_digest,
                    "registry_validation_profile_digest": registry_validation_profile_digest,
                    "oidc": oidc,
                    "artifact_gateway": {"endpoint": network.endpoint(Process::ArtifactGateway)?},
                    "model_installation": model_installation,
                    "model_credential_egress": {
                        "endpoint":network.endpoint(Process::EgressBroker)?,
                        "tls_server_name":network.tls_server_name(Process::EgressBroker)?,
                        "connect_timeout_milliseconds":5000,
                        "request_timeout_milliseconds":30000,
                        "maximum_rpc_metadata_bytes":4096,
                        "maximum_rpc_payload_bytes":4096,
                    },
                }),
            ),
        ),
        (
            "gateway-runtime".to_owned(),
            (
                RUNTIME_GATEWAY_RUNTIME_CONFIG_FILE,
                serde_json::json!({
                    "schema_version": 1,
                    "role": "runtime_api",
                    "listen_address": network.observability(Process::GatewayRuntime)?,
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "shutdown_grace_milliseconds": 30000,
                    "registry_validator_digest": registry_validator_digest,
                    "registry_validation_profile_digest": registry_validation_profile_digest,
                    "oidc": oidc,
                    "artifact_gateway": {"endpoint": network.endpoint(Process::ArtifactGateway)?},
                    "model_credential_egress": null,
                    "model_installation": null,
                    "live_text": {"servers":[format!("tls://{}:{}", network.nats_host, network.nats_port)], "namespace":"local", "connect_timeout_milliseconds":3000},
                }),
            ),
        ),
        (
            "artifact-gateway".to_owned(),
            (
                RUNTIME_ARTIFACT_GATEWAY_CONFIG_FILE,
                serde_json::json!({
                    "schema_version": 1,
                    "listen_address": network.listen(Process::ArtifactGateway)?,
                    "observability_listen_address": network.observability(Process::ArtifactGateway)?,
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "artifact_provider_catalog": catalog,
                    "write_encryption_domain_id": identity.encryption_domain_id,
                    "scanner_contract_digest": scanner_contract_digest,
                    "scan_evidence_ttl_milliseconds": 3600000,
                    "scan_retry_backoff_milliseconds": 250,
                    "finalize_batch_size": 32,
                    "finalize_poll_milliseconds": 1000,
                    "maximum_upload_target_seconds": 300,
                    "maximum_download_bytes": 16777216,
                    "maximum_download_in_flight": 16,
                    "download_timeout_milliseconds": 5000,
                    "shutdown_grace_milliseconds": 30000,
                }),
            ),
        ),
        (
            "artifact-data".to_owned(),
            (
                RUNTIME_ARTIFACT_DATA_CONFIG_FILE,
                serde_json::json!({
                    "schema_version": 1,
                    "audience": "data_worker",
                    "controller_listen_address": network.listen(Process::ArtifactData)?,
                    "observability_listen_address": network.observability(Process::ArtifactData)?,
                    "read_database_max_connections": 4,
                    "work_database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "artifact_provider_catalog": catalog,
                    "broker": {
                        "maximum_in_flight": 16,
                        "maximum_read_bytes": 67108864,
                        "operation_timeout_milliseconds": 5000,
                    },
                    "rpc": {
                        "maximum_request_bytes": 1048576,
                        "maximum_write_request_bytes": 16777216,
                        "maximum_chunk_bytes": 262144,
                    },
                    "scan_worker": {
                        "worker_manifest": worker_builds.manifest("platform-artifact-data-worker", "artifact-data-worker", insight_platform_contracts::WorkClass::Artifact, &local_digest("artifact-data-runtime")?, (4, 1), insight_platform_artifacts::execution::data_worker_execution_capabilities(&scanner_contract_digest.parse().map_err(|_| DeploymentError::Configuration("Artifact scanner digest invalid".to_owned()))?)),
                        "scanner_contract_digest": scanner_contract_digest,
                        "ruleset_digest": scanner_ruleset_digest,
                        "claim_batch": 4,
                        "lease_milliseconds": 120000,
                        "receipt_ttl_milliseconds": 3600000,
                        "poll_milliseconds": 1000,
                    },
                    "tls_handshake_timeout_milliseconds": 5000,
                    "shutdown_grace_milliseconds": 30000,
                }),
            ),
        ),
        (
            "orchestration".to_owned(),
            (
                RUNTIME_ORCHESTRATION_CONFIG_FILE,
                serde_json::json!({
                    "schema_version": 1,
                    "observability_listen_address": network.observability(Process::Orchestration)?,
                    "worker_manifest": worker_builds.manifest("platform-orchestration-worker", "orchestration-worker", insight_platform_contracts::WorkClass::Orchestration, &orchestration_adapter_digest, (4, 1), worker_profile::programs()),
                    "database": {
                        "business_max_connections": 4,
                        "critical_control_reserved_connections": 2,
                        "process_connection_budget": 6,
                        "acquire_timeout_milliseconds": 5000,
                        "statement_timeout_milliseconds": 30000,
                        "idle_timeout_milliseconds": 60000,
                        "max_lifetime_milliseconds": 600000,
                    },
                    "artifact": {
                        "endpoint": network.endpoint(Process::ArtifactData)?,
                        "tls_server_name": network.tls_server_name(Process::ArtifactData)?,
                        "connect_timeout_milliseconds": 5000,
                        "request_timeout_milliseconds": 5000,
                        "maximum_request_bytes": 1048576,
                        "maximum_chunk_bytes": 262144,
                    },
                    "timing": {
                        "coordinator_coalesce_milliseconds": 5,
                        "coordinator_scan_milliseconds": 500,
                        "coordinator_scan_jitter_milliseconds": 50,
                        "claim_failure_backoff_milliseconds": 100,
                        "drain_grace_milliseconds": 30000,
                        "heartbeat_jitter_milliseconds": 100,
                        "store_retry_backoff_milliseconds": 100,
                        "safety_scan_milliseconds": 5000,
                        "safety_scan_jitter_milliseconds": 50,
                        "safety_failure_backoff_milliseconds": 100,
                        "handoff_retry_milliseconds": 100,
                    },
                    "plan_maximum_bytes": 1048576,
                    "safety_shard": {"index": 0, "count": 1},
                }),
            ),
        ),
        (
            "capability-native".to_owned(),
            (
                RUNTIME_CAPABILITY_NATIVE_CONFIG_FILE,
                native_capability_config(network, worker_builds)?,
            ),
        ),
        (
            "registry-validation".to_owned(),
            (
                RUNTIME_REGISTRY_VALIDATION_CONFIG_FILE,
                serde_json::json!({
                    "schema_version": 1,
                    "observability_listen_address": network.observability(Process::RegistryValidation)?,
                    "worker_manifest": worker_builds.manifest("platform-registry-validation-worker", "registry-validation-worker", insight_platform_contracts::WorkClass::RegistryValidation, &registry_validator_digest, (2, 1), worker_profile::registry()),
                    "validator_principal_id": identity.registry_validator_principal_id,
                    "validator_digest": registry_validator_digest,
                    "validation_profile_digest": registry_validation_profile_digest,
                    "database_max_connections": 4,
                    "database_acquire_timeout_milliseconds": 5000,
                    "claim_batch": 2,
                    "lease_milliseconds": 30000,
                    "receipt_ttl_seconds": 300,
                    "scan_interval_milliseconds": 1000,
                    "failure_backoff_milliseconds": 50,
                    "drain_grace_milliseconds": 5000,
                }),
            ),
        ),
    ]);
    Ok(configurations)
}
pub fn native_capability_config(
    network: &NetworkTopologyV1,
    worker_builds: &WorkerBuilds,
) -> Result<serde_json::Value, DeploymentError> {
    let module_digest = insight_platform_contracts::builtin_echo_module_digest().to_string();
    let adapters = serde_json::json!([{
        "adapter_id": insight_platform_contracts::BUILTIN_ECHO_ADAPTER_ID,
        "adapter_version": insight_platform_contracts::BUILTIN_ECHO_ADAPTER_VERSION,
        "module_digest": module_digest,
        "entrypoint_id": insight_platform_contracts::BUILTIN_ECHO_ENTRYPOINT_ID,
    }]);
    let adapter_runtime_digest = canonical_digest(&adapters).map_err(|_| {
        DeploymentError::Configuration("local native capability configuration invalid".into())
    })?;
    Ok(serde_json::json!({
        "schema_version": 1,
        "observability_listen_address": network.observability(Process::CapabilityNative)?,
        "worker_manifest": worker_builds.manifest("platform-capability-native-worker", "capability.native", insight_platform_contracts::WorkClass::CapabilityNative, &adapter_runtime_digest, (4, 1), worker_profile::native_capability(&module_digest)),
        "installed_adapters": adapters,
        "database": {
            "business_max_connections": 4,
            "critical_control_max_connections": 2,
            "process_connection_budget": 6,
            "acquire_timeout_milliseconds": 5000,
        },
        "timing": {
            "initial_scan_delay_milliseconds": 0,
            "receipt_ttl_milliseconds": 60000,
            "safety_scan_milliseconds": 1000,
            "claim_failure_backoff_milliseconds": 50,
            "drain_grace_milliseconds": 5000,
        },
    }))
}

pub fn local_digest(kind: &str) -> Result<String, DeploymentError> {
    canonical_digest(&serde_json::json!({"schema_version": 1, "kind": kind})).map_err(|_| {
        DeploymentError::Configuration("local development configuration invalid".into())
    })
}

pub fn outbox_stream_contract(
) -> insight_platform_deployment_contracts::outbox::OutboxJetStreamContractV1 {
    serde_json::from_slice(include_bytes!(
        "../../../../deploy/jetstream/committed-events-v1.json"
    ))
    .expect("embedded current transport contract")
}

pub fn outbox_config(network: &NetworkTopologyV1) -> Result<Value, DeploymentError> {
    Ok(serde_json::json!({
        "schema_version": 1, "observability_listen_address": network.observability(Process::Outbox)?,
        "database_max_connections": 4, "database_acquire_timeout_milliseconds": 5000,
        "nats_servers": [format!("tls://{}:{}", network.nats_host, network.nats_port)], "nats_connect_timeout_milliseconds": 5000,
        "nats_publish_timeout_milliseconds": 3000, "maximum_pending_messages": 64,
        "poll_interval_milliseconds": 1000, "claim_batch": 4, "lease_milliseconds": 60000,
        "retry_base_milliseconds": 1000, "retry_maximum_milliseconds": 60000,
        "stream": outbox_stream_contract()
    }))
}

pub fn history_config(
    network: &NetworkTopologyV1,
    builds: &WorkerBuilds,
) -> Result<serde_json::Value, DeploymentError> {
    let executable = builds
        .executable("platform-history-maintenance")
        .ok_or_else(|| {
            DeploymentError::Configuration("History maintenance executable is missing".to_owned())
        })?;
    let value = serde_json::json!({
        "schema_version":1,"component_role":"history_maintenance","executable_digest":executable,
        "observability_listen_address":network.observability(Process::HistoryMaintenance)?,"database_max_connections":2,"database_acquire_timeout_milliseconds":5000,
        "poll_interval_milliseconds":30000,"maximum_runs":16,"maximum_events_per_run":128,
        "retention_policy":{"schema_version":2,"public_event_minimum_seconds":604800,"audit_event_minimum_seconds":604800,
            "cleanup_minimum_seconds":604800,"receipt_minimum_seconds":604800,"published_outbox_minimum_seconds":604800}
    });
    let config: insight_platform_deployment_contracts::history::HistoryMaintenanceConfigV1 =
        serde_json::from_value(value.clone())
            .map_err(|_| DeploymentError::Configuration("History config is invalid".to_owned()))?;
    config
        .validate()
        .map_err(|error| DeploymentError::Configuration(error.to_owned()))?;
    Ok(value)
}
