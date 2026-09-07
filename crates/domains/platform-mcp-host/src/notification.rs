use super::{
    digest, McpHostError, McpNotificationApplyDisposition, McpNotificationAudit,
    McpNotificationCommit, McpStreamableHttpSubscriptionSinkError, McpSubscriptionWorkerAudit,
    MAX_MCP_NOTIFICATION_BYTES,
};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{ResourceId, ResourceKind, Sha256Digest};
use std::{error::Error, fmt};

const MAX_NOTIFICATION_RATE_KEYS: usize = 65_536;
const MAX_NOTIFICATION_WINDOW_MILLISECONDS: u64 = 60_000;
const MAX_NOTIFICATION_EVENTS_PER_WINDOW: u32 = 10_000;

#[derive(Clone, PartialEq, Eq)]
pub struct SensitiveMcpNotificationWire(Vec<u8>);

impl SensitiveMcpNotificationWire {
    pub fn new(bytes: Vec<u8>) -> Result<Self, McpNotificationIngressError> {
        if bytes.is_empty() || bytes.len() > usize::try_from(MAX_MCP_NOTIFICATION_BYTES).unwrap() {
            return Err(McpNotificationIngressError::InvalidEnvelope);
        }
        Ok(Self(bytes))
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Transfers the bounded wire body across an authenticated internal transport boundary.
    /// Callers must keep the bytes out of logs, errors and durable event metadata.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl fmt::Debug for SensitiveMcpNotificationWire {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SensitiveMcpNotificationWire([REDACTED])")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestMcpNotification {
    pub audit: McpNotificationAudit,
    pub tenant_id: ResourceId,
    pub subscription_id: ResourceId,
    pub authorization_generation: u64,
    pub session_generation: u64,
    pub event_key_digest: Sha256Digest,
    pub event_generation: u64,
    pub wire: SensitiveMcpNotificationWire,
    pub received_at: DateTime<Utc>,
}

impl IngestMcpNotification {
    pub fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpNotificationIngressError> {
        self.audit
            .validate_at(now)
            .map_err(|_| McpNotificationIngressError::InvalidEnvelope)?;
        if self.tenant_id != self.audit.tenant_id
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.subscription_id.kind() != ResourceKind::McpOperation
            || self.authorization_generation == 0
            || self.session_generation == 0
            || self.event_generation == 0
            || self.received_at > now + Duration::seconds(60)
        {
            return Err(McpNotificationIngressError::InvalidEnvelope);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpNotificationIngressLimits {
    pub maximum_in_flight: usize,
    pub maximum_wire_bytes: u32,
}

impl McpNotificationIngressLimits {
    pub fn validate(self) -> Result<(), McpNotificationIngressError> {
        if self.maximum_in_flight == 0
            || self.maximum_in_flight > 4_096
            || self.maximum_wire_bytes == 0
            || self.maximum_wire_bytes > MAX_MCP_NOTIFICATION_BYTES
        {
            return Err(McpNotificationIngressError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpNotificationRateLimits {
    pub maximum_tracked_bindings: usize,
    pub maximum_events_per_window: u32,
    pub window_milliseconds: u64,
}

impl McpNotificationRateLimits {
    pub fn validate(self) -> Result<(), McpNotificationIngressError> {
        if self.maximum_tracked_bindings == 0
            || self.maximum_tracked_bindings > MAX_NOTIFICATION_RATE_KEYS
            || self.maximum_events_per_window == 0
            || self.maximum_events_per_window > MAX_NOTIFICATION_EVENTS_PER_WINDOW
            || self.window_milliseconds == 0
            || self.window_milliseconds > MAX_NOTIFICATION_WINDOW_MILLISECONDS
        {
            return Err(McpNotificationIngressError::InvalidConfiguration);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpNotificationRateKey {
    pub tenant_id: ResourceId,
    pub subscription_id: ResourceId,
    pub authorization_generation: u64,
    pub session_generation: u64,
}

impl McpNotificationRateKey {
    pub fn digest(&self) -> Result<String, McpNotificationIngressError> {
        digest(&serde_json::json!({
            "authorization_generation": self.authorization_generation,
            "schema_version": 1,
            "session_generation": self.session_generation,
            "subscription_id": self.subscription_id,
            "tenant_id": self.tenant_id,
        }))
        .map(|value| value.to_string())
        .map_err(|_| McpNotificationIngressError::InvalidEnvelope)
    }
}

pub trait McpNotificationRateAuthority: Send + Sync {
    fn admit(
        &self,
        key: &McpNotificationRateKey,
        now: DateTime<Utc>,
    ) -> Result<(), McpNotificationIngressError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpNotificationPersistenceError {
    Conflict,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpNotificationReceipt {
    pub disposition: McpNotificationApplyDisposition,
    pub replayed: bool,
}

#[async_trait]
pub trait McpNotificationCommitAuthority: Send + Sync {
    async fn commit(
        &self,
        command: McpNotificationCommit,
    ) -> Result<McpNotificationReceipt, McpNotificationPersistenceError>;
}

pub trait McpSubscriptionIngressIdentityFactory: Send + Sync {
    fn notification_audit(
        &self,
        tenant_id: &ResourceId,
        subscription_id: &ResourceId,
        event_key_digest: &Sha256Digest,
        now: DateTime<Utc>,
    ) -> Result<McpNotificationAudit, McpStreamableHttpSubscriptionSinkError>;

    fn termination_audit(
        &self,
        tenant_id: &ResourceId,
        subscription_id: &ResourceId,
        worker_process_generation_id: &ResourceId,
        session_generation: u64,
        now: DateTime<Utc>,
    ) -> Result<McpSubscriptionWorkerAudit, McpStreamableHttpSubscriptionSinkError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpNotificationIngressError {
    InvalidConfiguration,
    InvalidEnvelope,
    InvalidWire,
    RateLimited,
    Saturated,
    Rejected,
    Unavailable,
}

impl fmt::Display for McpNotificationIngressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "MCP notification ingress configuration is invalid",
            Self::InvalidEnvelope => "MCP notification envelope is invalid",
            Self::InvalidWire => "MCP notification wire message is invalid",
            Self::RateLimited => "MCP notification rate limit was reached",
            Self::Saturated => "MCP notification ingress is saturated",
            Self::Rejected => "MCP notification was rejected",
            Self::Unavailable => "MCP notification authority is unavailable",
        })
    }
}

impl Error for McpNotificationIngressError {}

impl From<McpHostError> for McpNotificationIngressError {
    fn from(_: McpHostError) -> Self {
        Self::InvalidWire
    }
}
