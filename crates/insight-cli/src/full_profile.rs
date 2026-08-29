//! Closed configuration fragments for the additive local `full` development profile.
//!
//! This module deliberately contains only product-facing process configuration. It does not
//! connect to an authority or execute worker logic; each generated document is consumed and
//! revalidated by the corresponding independent Platform process.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

pub(crate) const CONTEXT_NATIVE_CONFIG_FILE: &str = "context-native.json";
pub(crate) const ARTIFACT_MAINTENANCE_CONFIG_FILE: &str = "artifact-maintenance.json";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct PortBindings {
    pub(crate) context_native_observability: u16,
    pub(crate) artifact_maintenance_observability: u16,
}

impl PortBindings {
    pub(crate) fn allocate<E>(next: &mut impl FnMut() -> Result<u16, E>) -> Result<Self, E> {
        Ok(Self {
            context_native_observability: next()?,
            artifact_maintenance_observability: next()?,
        })
    }

    pub(crate) const fn legacy_defaults() -> Self {
        Self {
            context_native_observability: 19_095,
            artifact_maintenance_observability: 19_096,
        }
    }
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
    ])
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
        };
        let catalog = json!({"schema_version": 1});
        let adapter = digest('a');
        let contract = digest('b');
        let configs = initial_configs(&ports, &catalog, &adapter, &contract);

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
    }
}
