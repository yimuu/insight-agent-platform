//! Installed transport and process capacity contract; provisioning owns stream mutations.
use insight_platform_contracts::{
    MAX_COMMITTED_EVENT_NOTICE_BYTES, MAX_OUTBOX_CLAIM_BATCH, MAX_OUTBOX_LEASE_MILLISECONDS,
    MAX_OUTBOX_RETRY_MILLISECONDS,
};
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt};

pub const COMMITTED_EVENT_STREAM: &str = "INSIGHT_COMMITTED_V1";
pub const COMMITTED_EVENT_SUBJECT: &str = "insight.platform.v1.committed";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxJetStreamContractV1 {
    pub schema_version: u16,
    pub maximum_messages: u64,
    pub maximum_bytes: u64,
    pub replicas: u8,
    /// Zero means no timer expiry. A deployment performs explicit, reviewed consumer-watermark
    /// retention before removing accepted data; capacity uses DiscardNew, never implicit eviction.
    pub maximum_age_seconds: u64,
    pub duplicate_window_seconds: u32,
}
impl OutboxJetStreamContractV1 {
    pub fn validate(&self) -> Result<(), OutboxDeploymentError> {
        if self.schema_version != 1
            || self.maximum_messages == 0
            || self.maximum_messages > 100_000_000
            || self.maximum_bytes < MAX_COMMITTED_EVENT_NOTICE_BYTES as u64
            || self.maximum_bytes > 1_099_511_627_776
            || self.replicas == 0
            || self.replicas > 5
            || self.maximum_age_seconds != 0
            || self.duplicate_window_seconds == 0
            || self.duplicate_window_seconds > 3600
        {
            return Err(OutboxDeploymentError);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutboxWorkerConfigV1 {
    pub schema_version: u16,
    pub observability_listen_address: String,
    pub database_max_connections: u32,
    pub database_acquire_timeout_milliseconds: u64,
    pub nats_servers: Vec<String>,
    pub nats_connect_timeout_milliseconds: u64,
    pub nats_publish_timeout_milliseconds: u64,
    pub maximum_pending_messages: usize,
    pub poll_interval_milliseconds: u64,
    pub claim_batch: u16,
    pub lease_milliseconds: u64,
    pub retry_base_milliseconds: u64,
    pub retry_maximum_milliseconds: u64,
    pub stream: OutboxJetStreamContractV1,
}
impl OutboxWorkerConfigV1 {
    pub fn validate(&self) -> Result<(), OutboxDeploymentError> {
        self.stream.validate()?;
        let address: std::net::SocketAddr = self
            .observability_listen_address
            .parse()
            .map_err(|_| OutboxDeploymentError)?;
        if self.schema_version != 1 || address.port() == 0
            || !(2..=32).contains(&self.database_max_connections)
            || self.database_acquire_timeout_milliseconds == 0 || self.database_acquire_timeout_milliseconds > 30_000
            || self.nats_servers.is_empty() || self.nats_servers.len() > 8
            || self.nats_servers.iter().any(|server| server.len() > 2048 || !server.starts_with("tls://") || server.contains('@') || server.contains(['\n','\r']))
            || self.nats_connect_timeout_milliseconds == 0 || self.nats_connect_timeout_milliseconds > 30_000
            || self.nats_publish_timeout_milliseconds == 0 || self.nats_publish_timeout_milliseconds > 30_000
            || self.maximum_pending_messages == 0 || self.maximum_pending_messages > 256
            || self.poll_interval_milliseconds == 0 || self.poll_interval_milliseconds > 60_000
            || self.claim_batch == 0 || self.claim_batch > MAX_OUTBOX_CLAIM_BATCH
            || self.lease_milliseconds == 0 || self.lease_milliseconds > MAX_OUTBOX_LEASE_MILLISECONDS
            // A slow or unavailable transport must not consume the entire claim lease.
            || u64::from(self.claim_batch) * (self.nats_publish_timeout_milliseconds + 5_000) >= self.lease_milliseconds
            || self.retry_base_milliseconds == 0 || self.retry_base_milliseconds > self.retry_maximum_milliseconds
            || self.retry_maximum_milliseconds > MAX_OUTBOX_RETRY_MILLISECONDS
        {
            return Err(OutboxDeploymentError);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboxDeploymentError;
impl fmt::Display for OutboxDeploymentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid Outbox deployment contract")
    }
}
impl Error for OutboxDeploymentError {}
