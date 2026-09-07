//! Deployment-verified configuration for isolated history maintenance.
use insight_platform_contracts::{ComponentRole, Sha256Digest};
use insight_platform_orchestrator::history::{
    HistoryRetentionPolicy, MAX_HISTORY_RETENTION_RUN_SCAN, MAX_PUBLIC_RUN_EVENT_PURGE_BATCH,
};
use serde::{Deserialize, Serialize};

pub const HISTORY_MAINTENANCE_BINARY: &str = "platform-history-maintenance";
pub const HISTORY_MAINTENANCE_CONFIG_JSON_LIMITS: insight_platform_contracts::JsonLimits =
    insight_platform_contracts::JsonLimits {
        max_bytes: 65_536,
        max_depth: 8,
        max_properties_per_object: 32,
        max_items_per_array: 8,
        max_string_bytes: 1024,
    };

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryMaintenanceConfigV1 {
    pub schema_version: u32,
    pub component_role: ComponentRole,
    pub executable_digest: Sha256Digest,
    pub observability_listen_address: String,
    pub database_max_connections: u32,
    pub database_acquire_timeout_milliseconds: u64,
    pub poll_interval_milliseconds: u64,
    pub maximum_runs: u16,
    pub maximum_events_per_run: u16,
    pub retention_policy: HistoryRetentionPolicy,
}
impl HistoryMaintenanceConfigV1 {
    pub fn validate(&self) -> Result<(), &'static str> {
        self.retention_policy
            .validate()
            .map_err(|_| "invalid history retention policy")?;
        let address: std::net::SocketAddr = self
            .observability_listen_address
            .parse()
            .map_err(|_| "invalid history observability address")?;
        if self.schema_version != 1
            || self.component_role != ComponentRole::HistoryMaintenance
            || address.port() == 0
            || !(2..=8).contains(&self.database_max_connections)
            || !(1..=5000).contains(&self.database_acquire_timeout_milliseconds)
            || !(1000..=60000).contains(&self.poll_interval_milliseconds)
            || !(1..=MAX_HISTORY_RETENTION_RUN_SCAN).contains(&self.maximum_runs)
            || !(1..=MAX_PUBLIC_RUN_EVENT_PURGE_BATCH).contains(&self.maximum_events_per_run)
        {
            return Err("invalid history maintenance configuration");
        }
        Ok(())
    }
}

/// Release evidence for the non-Job maintenance process. No execution capability is invented.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryMaintenanceExecutableEvidenceV1 {
    pub schema_version: u32,
    pub component_role: ComponentRole,
    pub runtime_image_digest: Sha256Digest,
    pub executable_digest: Sha256Digest,
    pub process_config_digest: Sha256Digest,
}

impl HistoryMaintenanceExecutableEvidenceV1 {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != 1 || self.component_role != ComponentRole::HistoryMaintenance {
            return Err("unsupported history maintenance executable evidence");
        }
        Ok(())
    }
}
