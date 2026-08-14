use super::{
    digest, McpHostError, McpNotificationApplyDisposition, McpNotificationAudit,
    McpNotificationClass, McpNotificationCommit, McpStreamableHttpSubscriptionNotification,
    McpStreamableHttpSubscriptionSink, McpStreamableHttpSubscriptionSinkError,
    McpStreamableHttpSubscriptionTermination, McpSubscriptionPersistenceError,
    McpSubscriptionTransportTerminationAuthority, McpSubscriptionWorkerAudit,
    ReportMcpSubscriptionTransportTermination, MAX_MCP_NOTIFICATION_BYTES,
};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{ResourceId, ResourceKind, Sha256Digest};
use serde::de::{self, Deserialize, Deserializer, IgnoredAny, MapAccess, Visitor};
use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    sync::{Arc, Mutex},
};
use tokio::sync::Semaphore;
use uuid::Uuid;

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

    fn as_slice(&self) -> &[u8] {
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
    fn validate_at(&self, now: DateTime<Utc>) -> Result<(), McpNotificationIngressError> {
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
    fn validate(self) -> Result<(), McpNotificationIngressError> {
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
    fn validate(self) -> Result<(), McpNotificationIngressError> {
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
    fn digest(&self) -> Result<String, McpNotificationIngressError> {
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

#[derive(Debug, Clone, Copy)]
struct RateWindow {
    started_at: DateTime<Utc>,
    accepted: u32,
}

pub struct FixedWindowMcpNotificationRateAuthority {
    limits: McpNotificationRateLimits,
    windows: Mutex<BTreeMap<String, RateWindow>>,
}

impl FixedWindowMcpNotificationRateAuthority {
    pub fn new(limits: McpNotificationRateLimits) -> Result<Self, McpNotificationIngressError> {
        limits.validate()?;
        Ok(Self {
            limits,
            windows: Mutex::new(BTreeMap::new()),
        })
    }
}

impl McpNotificationRateAuthority for FixedWindowMcpNotificationRateAuthority {
    fn admit(
        &self,
        key: &McpNotificationRateKey,
        now: DateTime<Utc>,
    ) -> Result<(), McpNotificationIngressError> {
        let digest = key.digest()?;
        let window = Duration::milliseconds(
            i64::try_from(self.limits.window_milliseconds)
                .map_err(|_| McpNotificationIngressError::InvalidConfiguration)?,
        );
        let mut windows = self
            .windows
            .lock()
            .map_err(|_| McpNotificationIngressError::Unavailable)?;
        windows.retain(|_, value| value.started_at + window > now);
        if let Some(current) = windows.get_mut(&digest) {
            if current.accepted >= self.limits.maximum_events_per_window {
                return Err(McpNotificationIngressError::RateLimited);
            }
            current.accepted = current
                .accepted
                .checked_add(1)
                .ok_or(McpNotificationIngressError::RateLimited)?;
            return Ok(());
        }
        if windows.len() >= self.limits.maximum_tracked_bindings {
            return Err(McpNotificationIngressError::Saturated);
        }
        windows.insert(
            digest,
            RateWindow {
                started_at: now,
                accepted: 1,
            },
        );
        Ok(())
    }
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

pub struct McpNotificationIngressService {
    limits: McpNotificationIngressLimits,
    permits: Arc<Semaphore>,
    rate_authority: Arc<dyn McpNotificationRateAuthority>,
    commit_authority: Arc<dyn McpNotificationCommitAuthority>,
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

/// Production identity source for the trusted Egress-to-Host subscription ingress boundary.
///
/// Notification deduplication remains keyed by the server event identity carried in the command;
/// these UUIDv7 values only identify the individual Receipt/Event/Outbox rows. Transport loss uses
/// one stable idempotency scope per exact subscription session and worker generation so a retried
/// signal cannot schedule two rebuilds.
#[derive(Debug, Clone, Copy)]
pub struct UuidMcpSubscriptionIngressIdentityFactory {
    receipt_ttl_milliseconds: u64,
}

impl UuidMcpSubscriptionIngressIdentityFactory {
    pub fn new(
        receipt_ttl_milliseconds: u64,
    ) -> Result<Self, McpStreamableHttpSubscriptionSinkError> {
        if receipt_ttl_milliseconds == 0 || receipt_ttl_milliseconds > 86_400_000 {
            return Err(McpStreamableHttpSubscriptionSinkError::Rejected);
        }
        Ok(Self {
            receipt_ttl_milliseconds,
        })
    }

    fn new_id(kind: ResourceKind) -> Result<ResourceId, McpStreamableHttpSubscriptionSinkError> {
        ResourceId::from_uuid_v7(kind, Uuid::now_v7())
            .map_err(|_| McpStreamableHttpSubscriptionSinkError::Unavailable)
    }

    fn receipt_expires_at(
        &self,
        now: DateTime<Utc>,
    ) -> Result<DateTime<Utc>, McpStreamableHttpSubscriptionSinkError> {
        let milliseconds = i64::try_from(self.receipt_ttl_milliseconds)
            .map_err(|_| McpStreamableHttpSubscriptionSinkError::Rejected)?;
        now.checked_add_signed(Duration::milliseconds(milliseconds))
            .ok_or(McpStreamableHttpSubscriptionSinkError::Rejected)
    }
}

impl McpSubscriptionIngressIdentityFactory for UuidMcpSubscriptionIngressIdentityFactory {
    fn notification_audit(
        &self,
        tenant_id: &ResourceId,
        _subscription_id: &ResourceId,
        _event_key_digest: &Sha256Digest,
        now: DateTime<Utc>,
    ) -> Result<McpNotificationAudit, McpStreamableHttpSubscriptionSinkError> {
        Ok(McpNotificationAudit {
            tenant_id: tenant_id.clone(),
            receipt_id: Self::new_id(ResourceKind::Receipt)?,
            event_id: Self::new_id(ResourceKind::Event)?,
            outbox_id: Self::new_id(ResourceKind::OutboxEvent)?,
            receipt_expires_at: self.receipt_expires_at(now)?,
        })
    }

    fn termination_audit(
        &self,
        tenant_id: &ResourceId,
        subscription_id: &ResourceId,
        worker_process_generation_id: &ResourceId,
        session_generation: u64,
        now: DateTime<Utc>,
    ) -> Result<McpSubscriptionWorkerAudit, McpStreamableHttpSubscriptionSinkError> {
        let idempotency_key_digest = digest(&serde_json::json!({
            "schema_version": 1,
            "session_generation": session_generation,
            "subscription_id": subscription_id,
            "tenant_id": tenant_id,
            "worker_process_generation_id": worker_process_generation_id,
        }))
        .map_err(|_| McpStreamableHttpSubscriptionSinkError::Rejected)?;
        Ok(McpSubscriptionWorkerAudit {
            tenant_id: tenant_id.clone(),
            worker_process_generation_id: worker_process_generation_id.clone(),
            receipt_id: Self::new_id(ResourceKind::Receipt)?,
            event_id: Self::new_id(ResourceKind::Event)?,
            outbox_id: Self::new_id(ResourceKind::OutboxEvent)?,
            idempotency_key_digest: idempotency_key_digest.clone(),
            request_digest: idempotency_key_digest,
            receipt_expires_at: self.receipt_expires_at(now)?,
        })
    }
}

pub struct McpStreamableHttpSubscriptionIngress {
    notifications: Arc<McpNotificationIngressService>,
    terminations: Arc<dyn McpSubscriptionTransportTerminationAuthority>,
    identities: Arc<dyn McpSubscriptionIngressIdentityFactory>,
}

impl McpStreamableHttpSubscriptionIngress {
    pub fn new(
        notifications: Arc<McpNotificationIngressService>,
        terminations: Arc<dyn McpSubscriptionTransportTerminationAuthority>,
        identities: Arc<dyn McpSubscriptionIngressIdentityFactory>,
    ) -> Self {
        Self {
            notifications,
            terminations,
            identities,
        }
    }
}

#[async_trait]
impl McpStreamableHttpSubscriptionSink for McpStreamableHttpSubscriptionIngress {
    async fn ingest_notification(
        &self,
        notification: McpStreamableHttpSubscriptionNotification,
    ) -> Result<(), McpStreamableHttpSubscriptionSinkError> {
        let now = Utc::now();
        let audit = self.identities.notification_audit(
            &notification.tenant_id,
            &notification.subscription_id,
            &notification.event_key_digest,
            now,
        )?;
        self.notifications
            .ingest(
                IngestMcpNotification {
                    audit,
                    tenant_id: notification.tenant_id,
                    subscription_id: notification.subscription_id,
                    authorization_generation: notification.authorization_generation,
                    session_generation: notification.session_generation,
                    event_key_digest: notification.event_key_digest,
                    event_generation: notification.event_generation,
                    wire: notification.wire,
                    received_at: notification.received_at,
                },
                now,
            )
            .await
            .map(|_| ())
            .map_err(map_ingress_sink_error)
    }

    async fn report_termination(
        &self,
        termination: McpStreamableHttpSubscriptionTermination,
    ) -> Result<(), McpStreamableHttpSubscriptionSinkError> {
        let now = Utc::now();
        let evidence_digest = termination_evidence_digest(&termination)?;
        let mut audit = self.identities.termination_audit(
            &termination.tenant_id,
            &termination.subscription_id,
            &termination.worker_process_generation_id,
            termination.session_generation,
            now,
        )?;
        let mut command = ReportMcpSubscriptionTransportTermination {
            audit: audit.clone(),
            subscription_id: termination.subscription_id,
            expected_authorization_generation: termination.authorization_generation,
            expected_session_generation: termination.session_generation,
            reported_at: termination.observed_at,
            session_loss_evidence_digest: evidence_digest,
        };
        audit.request_digest = command
            .request_digest()
            .map_err(|_| McpStreamableHttpSubscriptionSinkError::Rejected)?;
        command.audit = audit;
        self.terminations
            .report_transport_termination(command)
            .await
            .map(|_| ())
            .map_err(map_termination_sink_error)
    }
}

fn termination_evidence_digest(
    termination: &McpStreamableHttpSubscriptionTermination,
) -> Result<Sha256Digest, McpStreamableHttpSubscriptionSinkError> {
    let failure = match &termination.failure {
        super::McpTransportFailure::RejectedBeforeDispatch(failure)
        | super::McpTransportFailure::RetryableBeforeDispatch(failure)
        | super::McpTransportFailure::Permanent(failure) => serde_json::json!({
            "class": "closed",
            "evidence_digest": failure.evidence_digest,
            "safe_code": failure.safe_code,
        }),
        super::McpTransportFailure::ReauthorizationRequired { challenge_digest } => {
            serde_json::json!({"challenge_digest": challenge_digest, "class": "reauthorization"})
        }
        super::McpTransportFailure::PostDispatchUncertain {
            failure,
            external_identity_digest,
        } => serde_json::json!({
            "class": "post_dispatch_uncertain",
            "evidence_digest": failure.evidence_digest,
            "external_identity_digest": external_identity_digest,
            "safe_code": failure.safe_code,
        }),
    };
    digest(&serde_json::json!({
        "authorization_generation": termination.authorization_generation,
        "failure": failure,
        "schema_version": 1,
        "session_generation": termination.session_generation,
        "subscription_id": termination.subscription_id,
        "tenant_id": termination.tenant_id,
    }))
    .map_err(|_| McpStreamableHttpSubscriptionSinkError::Rejected)
}

fn map_ingress_sink_error(
    failure: McpNotificationIngressError,
) -> McpStreamableHttpSubscriptionSinkError {
    match failure {
        McpNotificationIngressError::Saturated | McpNotificationIngressError::RateLimited => {
            McpStreamableHttpSubscriptionSinkError::Saturated
        }
        McpNotificationIngressError::Unavailable => {
            McpStreamableHttpSubscriptionSinkError::Unavailable
        }
        McpNotificationIngressError::InvalidConfiguration
        | McpNotificationIngressError::InvalidEnvelope
        | McpNotificationIngressError::InvalidWire
        | McpNotificationIngressError::Rejected => McpStreamableHttpSubscriptionSinkError::Rejected,
    }
}

fn map_termination_sink_error(
    failure: McpSubscriptionPersistenceError,
) -> McpStreamableHttpSubscriptionSinkError {
    match failure {
        McpSubscriptionPersistenceError::AuthorityUnavailable
        | McpSubscriptionPersistenceError::CommitUncertain => {
            McpStreamableHttpSubscriptionSinkError::Unavailable
        }
        McpSubscriptionPersistenceError::InvalidCommand
        | McpSubscriptionPersistenceError::Conflict => {
            McpStreamableHttpSubscriptionSinkError::Rejected
        }
    }
}

impl McpNotificationIngressService {
    pub fn new(
        limits: McpNotificationIngressLimits,
        rate_authority: Arc<dyn McpNotificationRateAuthority>,
        commit_authority: Arc<dyn McpNotificationCommitAuthority>,
    ) -> Result<Self, McpNotificationIngressError> {
        limits.validate()?;
        Ok(Self {
            limits,
            permits: Arc::new(Semaphore::new(limits.maximum_in_flight)),
            rate_authority,
            commit_authority,
        })
    }

    pub async fn ingest(
        &self,
        request: IngestMcpNotification,
        now: DateTime<Utc>,
    ) -> Result<McpNotificationReceipt, McpNotificationIngressError> {
        let _permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| McpNotificationIngressError::Saturated)?;
        request.validate_at(now)?;
        let wire_bytes = u32::try_from(request.wire.as_slice().len())
            .map_err(|_| McpNotificationIngressError::InvalidWire)?;
        if wire_bytes > self.limits.maximum_wire_bytes {
            return Err(McpNotificationIngressError::InvalidWire);
        }
        let parsed = StrictMcpNotification::parse(request.wire.as_slice())?;
        self.rate_authority.admit(
            &McpNotificationRateKey {
                tenant_id: request.tenant_id.clone(),
                subscription_id: request.subscription_id.clone(),
                authorization_generation: request.authorization_generation,
                session_generation: request.session_generation,
            },
            now,
        )?;
        let command = McpNotificationCommit {
            audit: request.audit,
            tenant_id: request.tenant_id,
            subscription_id: request.subscription_id,
            authorization_generation: request.authorization_generation,
            session_generation: request.session_generation,
            event_key_digest: request.event_key_digest,
            event_generation: request.event_generation,
            class: parsed.class,
            resource_uri_digest: parsed.resource_uri_digest,
            body_digest: parsed.body_digest,
            wire_bytes,
            received_at: request.received_at,
        };
        command
            .validate_at(now)
            .map_err(|_| McpNotificationIngressError::InvalidWire)?;
        self.commit_authority
            .commit(command)
            .await
            .map_err(|failure| match failure {
                McpNotificationPersistenceError::Conflict => McpNotificationIngressError::Rejected,
                McpNotificationPersistenceError::Unavailable => {
                    McpNotificationIngressError::Unavailable
                }
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StrictMcpNotification {
    class: McpNotificationClass,
    resource_uri_digest: Option<Sha256Digest>,
    body_digest: Sha256Digest,
}

impl StrictMcpNotification {
    fn parse(bytes: &[u8]) -> Result<Self, McpNotificationIngressError> {
        let mut deserializer = serde_json::Deserializer::from_slice(bytes);
        let wire = StrictNotificationWire::deserialize(&mut deserializer)
            .map_err(|_| McpNotificationIngressError::InvalidWire)?;
        deserializer
            .end()
            .map_err(|_| McpNotificationIngressError::InvalidWire)?;
        if wire.jsonrpc != "2.0" {
            return Err(McpNotificationIngressError::InvalidWire);
        }
        let body_digest = digest(&serde_json::json!({
            "jsonrpc": wire.jsonrpc,
            "method": wire.method,
            "params": wire.params,
        }))
        .map_err(|_| McpNotificationIngressError::InvalidWire)?;
        let (class, resource_uri_digest) = match wire.method.as_str() {
            "notifications/resources/updated" => {
                let uri = wire
                    .params
                    .as_ref()
                    .and_then(|params| params.uri.as_ref())
                    .ok_or(McpNotificationIngressError::InvalidWire)?;
                let digest = super::canonical_mcp_resource_uri_digest(uri)
                    .map_err(|_| McpNotificationIngressError::InvalidWire)?;
                (McpNotificationClass::ResourceUpdated, Some(digest))
            }
            "notifications/resources/list_changed"
                if wire.params.as_ref().is_none_or(|p| p.uri.is_none()) =>
            {
                (McpNotificationClass::ResourceListChanged, None)
            }
            "notifications/tools/list_changed"
                if wire.params.as_ref().is_none_or(|p| p.uri.is_none()) =>
            {
                (McpNotificationClass::ToolListChanged, None)
            }
            "notifications/prompts/list_changed"
                if wire.params.as_ref().is_none_or(|p| p.uri.is_none()) =>
            {
                (McpNotificationClass::PromptListChanged, None)
            }
            _ => return Err(McpNotificationIngressError::InvalidWire),
        };
        Ok(Self {
            class,
            resource_uri_digest,
            body_digest,
        })
    }
}

#[derive(Debug, serde::Serialize)]
struct StrictNotificationWire {
    jsonrpc: String,
    method: String,
    params: Option<StrictNotificationParams>,
}

impl<'de> Deserialize<'de> for StrictNotificationWire {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct WireVisitor;

        impl<'de> Visitor<'de> for WireVisitor {
            type Value = StrictNotificationWire;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a closed MCP JSON-RPC notification")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut jsonrpc = None;
                let mut method = None;
                let mut params = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "jsonrpc" if jsonrpc.is_none() => jsonrpc = Some(map.next_value()?),
                        "method" if method.is_none() => method = Some(map.next_value()?),
                        "params" if params.is_none() => params = Some(map.next_value()?),
                        "jsonrpc" => {
                            let _: IgnoredAny = map.next_value()?;
                            return Err(de::Error::duplicate_field("jsonrpc"));
                        }
                        "method" => {
                            let _: IgnoredAny = map.next_value()?;
                            return Err(de::Error::duplicate_field("method"));
                        }
                        "params" => {
                            let _: IgnoredAny = map.next_value()?;
                            return Err(de::Error::duplicate_field("params"));
                        }
                        _ => {
                            let _: IgnoredAny = map.next_value()?;
                            return Err(de::Error::unknown_field(
                                &key,
                                &["jsonrpc", "method", "params"],
                            ));
                        }
                    }
                }
                Ok(StrictNotificationWire {
                    jsonrpc: jsonrpc.ok_or_else(|| de::Error::missing_field("jsonrpc"))?,
                    method: method.ok_or_else(|| de::Error::missing_field("method"))?,
                    params: params.unwrap_or(None),
                })
            }
        }

        deserializer.deserialize_map(WireVisitor)
    }
}

#[derive(Debug, serde::Serialize)]
struct StrictNotificationParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    uri: Option<String>,
}

impl<'de> Deserialize<'de> for StrictNotificationParams {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ParamsVisitor;

        impl<'de> Visitor<'de> for ParamsVisitor {
            type Value = StrictNotificationParams;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("closed MCP notification params")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut uri = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "uri" if uri.is_none() => uri = Some(map.next_value()?),
                        "uri" => {
                            let _: IgnoredAny = map.next_value()?;
                            return Err(de::Error::duplicate_field("uri"));
                        }
                        _ => {
                            let _: IgnoredAny = map.next_value()?;
                            return Err(de::Error::unknown_field(&key, &["uri"]));
                        }
                    }
                }
                Ok(StrictNotificationParams { uri })
            }
        }

        deserializer.deserialize_map(ParamsVisitor)
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{McpSubscriptionRecord, McpTransportFailure};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct RecordingAuthority {
        calls: AtomicUsize,
        last: Mutex<Option<McpNotificationCommit>>,
    }

    struct RecordingTerminationAuthority {
        last: Mutex<Option<ReportMcpSubscriptionTransportTermination>>,
    }

    #[async_trait]
    impl McpNotificationCommitAuthority for RecordingAuthority {
        async fn commit(
            &self,
            command: McpNotificationCommit,
        ) -> Result<McpNotificationReceipt, McpNotificationPersistenceError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.last.lock().unwrap() = Some(command);
            Ok(McpNotificationReceipt {
                disposition: McpNotificationApplyDisposition::Wake,
                replayed: false,
            })
        }
    }

    #[async_trait]
    impl McpSubscriptionTransportTerminationAuthority for RecordingTerminationAuthority {
        async fn report_transport_termination(
            &self,
            command: ReportMcpSubscriptionTransportTermination,
        ) -> Result<
            insight_platform_contracts::CommandOutcome<McpSubscriptionRecord>,
            McpSubscriptionPersistenceError,
        > {
            *self.last.lock().unwrap() = Some(command);
            Err(McpSubscriptionPersistenceError::AuthorityUnavailable)
        }
    }

    fn id(prefix: &str, suffix: &str) -> ResourceId {
        format!("{prefix}_0198f1cf-32e4-75e1-a9e8-d95ca0f6{suffix:0>4}")
            .parse()
            .unwrap()
    }

    fn sha(name: &str) -> Sha256Digest {
        digest(&serde_json::json!({"fixture": name})).unwrap()
    }

    fn request(now: DateTime<Utc>, wire: &[u8]) -> IngestMcpNotification {
        IngestMcpNotification {
            audit: McpNotificationAudit {
                tenant_id: id("ten", "1"),
                receipt_id: id("rcp", "2"),
                event_id: id("evt", "3"),
                outbox_id: id("out", "4"),
                receipt_expires_at: now + Duration::minutes(5),
            },
            tenant_id: id("ten", "1"),
            subscription_id: id("mop", "5"),
            authorization_generation: 2,
            session_generation: 3,
            event_key_digest: sha("event-key"),
            event_generation: 4,
            wire: SensitiveMcpNotificationWire::new(wire.to_vec()).unwrap(),
            received_at: now,
        }
    }

    fn service(
        authority: Arc<RecordingAuthority>,
        maximum_events_per_window: u32,
    ) -> McpNotificationIngressService {
        McpNotificationIngressService::new(
            McpNotificationIngressLimits {
                maximum_in_flight: 1,
                maximum_wire_bytes: 4_096,
            },
            Arc::new(
                FixedWindowMcpNotificationRateAuthority::new(McpNotificationRateLimits {
                    maximum_tracked_bindings: 8,
                    maximum_events_per_window,
                    window_milliseconds: 1_000,
                })
                .unwrap(),
            ),
            authority,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn ingress_strictly_parses_and_commits_only_a_digest() {
        let now = Utc::now();
        let authority = Arc::new(RecordingAuthority {
            calls: AtomicUsize::new(0),
            last: Mutex::new(None),
        });
        let service = service(authority.clone(), 2);
        let wire = br#"{"jsonrpc":"2.0","method":"notifications/resources/updated","params":{"uri":"mcp://catalog.example/items/42"}}"#;
        let outcome = service.ingest(request(now, wire), now).await.unwrap();
        assert_eq!(outcome.disposition, McpNotificationApplyDisposition::Wake);
        let command = authority.last.lock().unwrap().clone().unwrap();
        assert_eq!(command.class, McpNotificationClass::ResourceUpdated);
        assert!(command.resource_uri_digest.is_some());
        assert!(!format!("{command:?}").contains("catalog.example"));
    }

    #[tokio::test]
    async fn duplicate_unknown_and_credentialed_fields_fail_before_commit() {
        let now = Utc::now();
        for wire in [
            br#"{"jsonrpc":"2.0","jsonrpc":"2.0","method":"notifications/tools/list_changed"}"#.as_slice(),
            br#"{"jsonrpc":"2.0","method":"notifications/tools/list_changed","extra":1}"#.as_slice(),
            br#"{"jsonrpc":"2.0","method":"notifications/resources/updated","params":{"uri":"mcp://user@catalog.example/items/42"}}"#.as_slice(),
        ] {
            let authority = Arc::new(RecordingAuthority {
                calls: AtomicUsize::new(0),
                last: Mutex::new(None),
            });
            let service = service(authority.clone(), 2);
            assert_eq!(
                service.ingest(request(now, wire), now).await.unwrap_err(),
                McpNotificationIngressError::InvalidWire
            );
            assert_eq!(authority.calls.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn rate_limit_rejects_before_the_durable_authority() {
        let now = Utc::now();
        let authority = Arc::new(RecordingAuthority {
            calls: AtomicUsize::new(0),
            last: Mutex::new(None),
        });
        let service = service(authority.clone(), 1);
        let wire = br#"{"jsonrpc":"2.0","method":"notifications/tools/list_changed"}"#;
        service.ingest(request(now, wire), now).await.unwrap();
        let mut second = request(now, wire);
        second.audit.receipt_id = id("rcp", "6");
        assert_eq!(
            service.ingest(second, now).await.unwrap_err(),
            McpNotificationIngressError::RateLimited
        );
        assert_eq!(authority.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn production_identity_factory_emits_typed_ids_and_stable_termination_scope() {
        let now = Utc::now();
        let factory = UuidMcpSubscriptionIngressIdentityFactory::new(60_000).unwrap();
        let tenant_id = id("ten", "1");
        let subscription_id = id("mop", "5");
        let worker_id = id("wrk", "6");
        let notification = factory
            .notification_audit(&tenant_id, &subscription_id, &sha("event"), now)
            .unwrap();
        assert_eq!(notification.receipt_id.kind(), ResourceKind::Receipt);
        assert_eq!(notification.event_id.kind(), ResourceKind::Event);
        assert_eq!(notification.outbox_id.kind(), ResourceKind::OutboxEvent);
        assert_eq!(notification.receipt_expires_at, now + Duration::minutes(1));

        let first = factory
            .termination_audit(&tenant_id, &subscription_id, &worker_id, 7, now)
            .unwrap();
        let retry = factory
            .termination_audit(&tenant_id, &subscription_id, &worker_id, 7, now)
            .unwrap();
        assert_eq!(first.idempotency_key_digest, retry.idempotency_key_digest);
        assert_ne!(first.receipt_id, retry.receipt_id);
        assert_ne!(first.event_id, retry.event_id);
        assert_ne!(first.outbox_id, retry.outbox_id);
        assert!(UuidMcpSubscriptionIngressIdentityFactory::new(0).is_err());
        assert!(UuidMcpSubscriptionIngressIdentityFactory::new(86_400_001).is_err());
    }

    #[tokio::test]
    async fn streamable_http_sink_binds_notification_and_loss_to_exact_generation() {
        let now = Utc::now();
        let notification_authority = Arc::new(RecordingAuthority {
            calls: AtomicUsize::new(0),
            last: Mutex::new(None),
        });
        let termination_authority = Arc::new(RecordingTerminationAuthority {
            last: Mutex::new(None),
        });
        let ingress = McpStreamableHttpSubscriptionIngress::new(
            Arc::new(service(notification_authority.clone(), 2)),
            termination_authority.clone(),
            Arc::new(UuidMcpSubscriptionIngressIdentityFactory::new(60_000).unwrap()),
        );
        let tenant_id = id("ten", "1");
        let subscription_id = id("mop", "5");
        ingress
            .ingest_notification(McpStreamableHttpSubscriptionNotification {
                tenant_id: tenant_id.clone(),
                subscription_id: subscription_id.clone(),
                authorization_generation: 2,
                session_generation: 3,
                event_generation: 4,
                event_key_digest: sha("event-key"),
                wire: SensitiveMcpNotificationWire::new(
                    br#"{"jsonrpc":"2.0","method":"notifications/resources/list_changed"}"#
                        .to_vec(),
                )
                .unwrap(),
                received_at: now,
            })
            .await
            .unwrap();
        let committed = notification_authority.last.lock().unwrap().clone().unwrap();
        assert_eq!(committed.tenant_id, tenant_id);
        assert_eq!(committed.subscription_id, subscription_id);
        assert_eq!(committed.session_generation, 3);
        assert_eq!(committed.event_generation, 4);

        let worker_id = id("wrk", "6");
        let failure = McpTransportFailure::PostDispatchUncertain {
            failure: super::super::SafeMcpFailure {
                safe_code: "mcp_subscription_stream_closed".to_owned(),
                safe_message: "MCP subscription stream closed".to_owned(),
                evidence_digest: sha("failure"),
            },
            external_identity_digest: sha("external"),
        };
        assert_eq!(
            ingress
                .report_termination(McpStreamableHttpSubscriptionTermination {
                    tenant_id: tenant_id.clone(),
                    subscription_id: subscription_id.clone(),
                    authorization_generation: 2,
                    session_generation: 3,
                    worker_process_generation_id: worker_id.clone(),
                    observed_at: now,
                    failure,
                })
                .await,
            Err(McpStreamableHttpSubscriptionSinkError::Unavailable)
        );
        let reported = termination_authority.last.lock().unwrap().clone().unwrap();
        assert_eq!(reported.audit.tenant_id, tenant_id);
        assert_eq!(reported.audit.worker_process_generation_id, worker_id);
        assert_eq!(reported.subscription_id, subscription_id);
        assert_eq!(reported.expected_authorization_generation, 2);
        assert_eq!(reported.expected_session_generation, 3);
        reported.validate_at(Utc::now()).unwrap();
    }
}
