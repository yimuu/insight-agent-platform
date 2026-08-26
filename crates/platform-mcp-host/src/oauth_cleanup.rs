use async_trait::async_trait;
use insight_platform_contracts::{
    ExactSecretBindingRef, ResourceId, ResourceKind, SecretResolutionPolicy, TraceFlags,
    TraceIdentityV1,
};
use insight_platform_rpc_trace::{scope_trace, RpcTraceContext};
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpOAuthPkceCleanupCause {
    Authorized,
    Declined,
    Expired,
}

/// Secret-free durable hint carried by the terminal OAuth Event/Outbox projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpOAuthPkceCleanupHint {
    pub schema_version: u32,
    pub secret_binding_id: ResourceId,
    pub binding_generation: u64,
}

impl McpOAuthPkceCleanupHint {
    pub fn validate(&self) -> Result<(), McpOAuthPkceCleanupError> {
        if self.schema_version != 1
            || self.secret_binding_id.kind() != ResourceKind::SecretBinding
            || self.binding_generation == 0
        {
            return Err(McpOAuthPkceCleanupError::Rejected(
                "mcp_oauth_pkce_cleanup_hint_invalid",
            ));
        }
        Ok(())
    }
}

/// Trusted delivery metadata is taken from the committed Event envelope, not duplicated inside
/// the durable cleanup hint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpOAuthPkceCleanupRequest {
    pub tenant_id: ResourceId,
    pub task_id: ResourceId,
    pub cause: McpOAuthPkceCleanupCause,
    pub hint: McpOAuthPkceCleanupHint,
}

impl McpOAuthPkceCleanupRequest {
    pub fn validate(&self) -> Result<(), McpOAuthPkceCleanupError> {
        self.hint.validate()?;
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.task_id.kind() != ResourceKind::Interaction
        {
            return Err(McpOAuthPkceCleanupError::Rejected(
                "mcp_oauth_pkce_cleanup_envelope_invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizedMcpOAuthPkceCleanup {
    pub tenant_id: ResourceId,
    pub task_id: ResourceId,
    pub secret_binding: ExactSecretBindingRef,
}

impl AuthorizedMcpOAuthPkceCleanup {
    pub fn validate_for(
        &self,
        request: &McpOAuthPkceCleanupRequest,
    ) -> Result<(), McpOAuthPkceCleanupError> {
        request.validate()?;
        if self.tenant_id != request.tenant_id
            || self.task_id != request.task_id
            || self.secret_binding.secret_binding_id != request.hint.secret_binding_id
            || self.secret_binding.binding_generation != request.hint.binding_generation
            || self.secret_binding.purpose.as_str() != super::MCP_OAUTH_PKCE_SECRET_PURPOSE
            || self.secret_binding.validate().is_err()
            || !matches!(
                self.secret_binding.resolution_policy,
                SecretResolutionPolicy::Pinned { .. }
            )
        {
            return Err(McpOAuthPkceCleanupError::Rejected(
                "mcp_oauth_pkce_cleanup_authority_invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthPkceCleanupAuthorityError {
    StaleOrNotFound,
    Unavailable,
}

#[async_trait]
pub trait McpOAuthPkceCleanupAuthority: Send + Sync {
    async fn authorize_cleanup(
        &self,
        request: &McpOAuthPkceCleanupRequest,
    ) -> Result<AuthorizedMcpOAuthPkceCleanup, McpOAuthPkceCleanupAuthorityError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpOAuthPkceSecretCleanupDisposition {
    Deleted,
    AlreadyAbsent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthPkceSecretCleanupError {
    Rejected,
    TemporarilyUnavailable,
    OutcomeUncertain,
}

/// Trusted Secret Manager port. Implementations must delete only the exact pinned generation.
#[async_trait]
pub trait McpOAuthPkceSecretCleaner: Send + Sync {
    async fn delete_exact(
        &self,
        authorization: &AuthorizedMcpOAuthPkceCleanup,
    ) -> Result<McpOAuthPkceSecretCleanupDisposition, McpOAuthPkceSecretCleanupError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthPkceCleanupOutcome {
    Deleted,
    AlreadyAbsent,
    IgnoredStale,
}

pub struct McpOAuthPkceCleanupConsumer {
    authority: Arc<dyn McpOAuthPkceCleanupAuthority>,
    cleaner: Arc<dyn McpOAuthPkceSecretCleaner>,
}

impl McpOAuthPkceCleanupConsumer {
    pub fn new(
        authority: Arc<dyn McpOAuthPkceCleanupAuthority>,
        cleaner: Arc<dyn McpOAuthPkceSecretCleaner>,
    ) -> Self {
        Self { authority, cleaner }
    }

    pub async fn consume(
        &self,
        request: McpOAuthPkceCleanupRequest,
    ) -> Result<McpOAuthPkceCleanupOutcome, McpOAuthPkceCleanupError> {
        request.validate()?;
        let authorization = match self.authority.authorize_cleanup(&request).await {
            Ok(authorization) => authorization,
            Err(McpOAuthPkceCleanupAuthorityError::StaleOrNotFound) => {
                return Ok(McpOAuthPkceCleanupOutcome::IgnoredStale);
            }
            Err(McpOAuthPkceCleanupAuthorityError::Unavailable) => {
                return Err(McpOAuthPkceCleanupError::TemporarilyUnavailable(
                    "mcp_oauth_pkce_cleanup_authority_unavailable",
                ));
            }
        };
        authorization.validate_for(&request)?;
        match self.cleaner.delete_exact(&authorization).await {
            Ok(McpOAuthPkceSecretCleanupDisposition::Deleted) => {
                Ok(McpOAuthPkceCleanupOutcome::Deleted)
            }
            Ok(McpOAuthPkceSecretCleanupDisposition::AlreadyAbsent) => {
                Ok(McpOAuthPkceCleanupOutcome::AlreadyAbsent)
            }
            Err(McpOAuthPkceSecretCleanupError::Rejected) => Err(
                McpOAuthPkceCleanupError::Rejected("mcp_oauth_pkce_cleanup_secret_rejected"),
            ),
            Err(McpOAuthPkceSecretCleanupError::TemporarilyUnavailable) => {
                Err(McpOAuthPkceCleanupError::TemporarilyUnavailable(
                    "mcp_oauth_pkce_cleanup_secret_unavailable",
                ))
            }
            Err(McpOAuthPkceSecretCleanupError::OutcomeUncertain) => {
                Err(McpOAuthPkceCleanupError::OutcomeUncertain(
                    "mcp_oauth_pkce_cleanup_outcome_uncertain",
                ))
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthPkceCleanupError {
    Rejected(&'static str),
    TemporarilyUnavailable(&'static str),
    OutcomeUncertain(&'static str),
}

impl McpOAuthPkceCleanupError {
    pub const fn safe_code(self) -> &'static str {
        match self {
            Self::Rejected(code)
            | Self::TemporarilyUnavailable(code)
            | Self::OutcomeUncertain(code) => code,
        }
    }
}

impl fmt::Display for McpOAuthPkceCleanupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Rejected(_) => "MCP OAuth PKCE cleanup was rejected",
            Self::TemporarilyUnavailable(_) => "MCP OAuth PKCE cleanup dependency is unavailable",
            Self::OutcomeUncertain(_) => "MCP OAuth PKCE cleanup outcome is uncertain",
        })
    }
}

impl Error for McpOAuthPkceCleanupError {}

/// Exact lease-fenced delivery of one committed OAuth terminal outbox event.
///
/// `publish_attempts` is observation only. The claim owner and epoch are the mutation fence, so a
/// worker that resumes after its lease was reclaimed cannot acknowledge another worker's cleanup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedMcpOAuthPkceCleanup {
    pub outbox_id: ResourceId,
    pub event_id: ResourceId,
    pub claim_owner: ResourceId,
    pub claim_epoch: u64,
    pub publish_attempts: u32,
    pub trace: TraceIdentityV1,
    pub request: McpOAuthPkceCleanupRequest,
}

impl ClaimedMcpOAuthPkceCleanup {
    pub fn validate(&self) -> Result<(), McpOAuthPkceCleanupDeliveryError> {
        self.request
            .validate()
            .map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptEvent)?;
        if self.outbox_id.kind() != ResourceKind::OutboxEvent
            || self.event_id.kind() != ResourceKind::Event
            || self.claim_owner.kind() != ResourceKind::WorkerProcessGeneration
            || self.claim_epoch == 0
            || self.trace.validate().is_err()
        {
            return Err(McpOAuthPkceCleanupDeliveryError::CorruptEvent);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimDueMcpOAuthPkceCleanups {
    pub claim_owner: ResourceId,
    pub maximum_claims: u16,
    pub lease_milliseconds: u64,
}

impl ClaimDueMcpOAuthPkceCleanups {
    pub fn validate(
        &self,
        maximum_batch: u16,
        maximum_lease_milliseconds: u64,
    ) -> Result<(), McpOAuthPkceCleanupDeliveryError> {
        if self.claim_owner.kind() != ResourceKind::WorkerProcessGeneration
            || self.maximum_claims == 0
            || self.maximum_claims > maximum_batch
            || self.lease_milliseconds == 0
            || self.lease_milliseconds > maximum_lease_milliseconds
        {
            return Err(McpOAuthPkceCleanupDeliveryError::InvalidCommand);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthPkceCleanupSettlement {
    Completed,
    Retry {
        failure_code: &'static str,
        delay_milliseconds: u64,
    },
    DeadLetter {
        failure_code: &'static str,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpOAuthPkceCleanupDeliveryError {
    InvalidCommand,
    CorruptEvent,
    Unavailable,
}

impl fmt::Display for McpOAuthPkceCleanupDeliveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidCommand => "MCP OAuth cleanup delivery command is invalid",
            Self::CorruptEvent => "MCP OAuth cleanup outbox event is invalid",
            Self::Unavailable => "MCP OAuth cleanup outbox authority is unavailable",
        })
    }
}

impl Error for McpOAuthPkceCleanupDeliveryError {}

#[async_trait]
pub trait McpOAuthPkceCleanupOutbox: Send + Sync {
    async fn claim_due_mcp_oauth_pkce_cleanups(
        &self,
        command: ClaimDueMcpOAuthPkceCleanups,
    ) -> Result<Vec<ClaimedMcpOAuthPkceCleanup>, McpOAuthPkceCleanupDeliveryError>;

    /// Returns `false` when the exact claim fence was already lost. This is a normal first-winner
    /// outcome and must never be retried as an unfenced update.
    async fn settle_mcp_oauth_pkce_cleanup(
        &self,
        claim: &ClaimedMcpOAuthPkceCleanup,
        settlement: McpOAuthPkceCleanupSettlement,
    ) -> Result<bool, McpOAuthPkceCleanupDeliveryError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpOAuthPkceCleanupWorkerConfig {
    pub maximum_batch: u16,
    pub maximum_lease_milliseconds: u64,
    pub claim_batch: u16,
    pub lease_milliseconds: u64,
    pub retry_base_milliseconds: u64,
    pub retry_maximum_milliseconds: u64,
}

impl McpOAuthPkceCleanupWorkerConfig {
    pub fn validate(self) -> Result<(), McpOAuthPkceCleanupDeliveryError> {
        if self.maximum_batch == 0
            || self.maximum_lease_milliseconds == 0
            || self.claim_batch == 0
            || self.claim_batch > self.maximum_batch
            || self.lease_milliseconds == 0
            || self.lease_milliseconds > self.maximum_lease_milliseconds
            || self.retry_base_milliseconds == 0
            || self.retry_maximum_milliseconds < self.retry_base_milliseconds
            || self.retry_maximum_milliseconds > 3_600_000
        {
            return Err(McpOAuthPkceCleanupDeliveryError::InvalidCommand);
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct McpOAuthPkceCleanupWorkerSummary {
    pub claimed: u16,
    pub completed: u16,
    pub deferred: u16,
    pub dead_lettered: u16,
    pub lost_claims: u16,
}

pub struct McpOAuthPkceCleanupWorker {
    worker_process_generation_id: ResourceId,
    outbox: Arc<dyn McpOAuthPkceCleanupOutbox>,
    consumer: Arc<McpOAuthPkceCleanupConsumer>,
    config: McpOAuthPkceCleanupWorkerConfig,
}

impl McpOAuthPkceCleanupWorker {
    pub fn new(
        worker_process_generation_id: ResourceId,
        outbox: Arc<dyn McpOAuthPkceCleanupOutbox>,
        consumer: Arc<McpOAuthPkceCleanupConsumer>,
        config: McpOAuthPkceCleanupWorkerConfig,
    ) -> Result<Self, McpOAuthPkceCleanupDeliveryError> {
        config.validate()?;
        if worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration {
            return Err(McpOAuthPkceCleanupDeliveryError::InvalidCommand);
        }
        Ok(Self {
            worker_process_generation_id,
            outbox,
            consumer,
            config,
        })
    }

    pub async fn run_once(
        &self,
    ) -> Result<McpOAuthPkceCleanupWorkerSummary, McpOAuthPkceCleanupDeliveryError> {
        let claims = self
            .outbox
            .claim_due_mcp_oauth_pkce_cleanups(ClaimDueMcpOAuthPkceCleanups {
                claim_owner: self.worker_process_generation_id.clone(),
                maximum_claims: self.config.claim_batch,
                lease_milliseconds: self.config.lease_milliseconds,
            })
            .await?;
        let mut summary = McpOAuthPkceCleanupWorkerSummary {
            claimed: u16::try_from(claims.len())
                .map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptEvent)?,
            ..McpOAuthPkceCleanupWorkerSummary::default()
        };
        for claim in claims {
            claim.validate()?;
            let trace = RpcTraceContext::start(claim.trace, TraceFlags::NotSampled)
                .map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptEvent)?;
            let settlement =
                match scope_trace(trace, self.consumer.consume(claim.request.clone())).await {
                    Ok(_) => McpOAuthPkceCleanupSettlement::Completed,
                    Err(McpOAuthPkceCleanupError::Rejected(code)) => {
                        McpOAuthPkceCleanupSettlement::DeadLetter { failure_code: code }
                    }
                    Err(McpOAuthPkceCleanupError::TemporarilyUnavailable(code))
                    | Err(McpOAuthPkceCleanupError::OutcomeUncertain(code)) => {
                        McpOAuthPkceCleanupSettlement::Retry {
                            failure_code: code,
                            delay_milliseconds: retry_delay(
                                claim.publish_attempts,
                                self.config.retry_base_milliseconds,
                                self.config.retry_maximum_milliseconds,
                            ),
                        }
                    }
                };
            let settled = self
                .outbox
                .settle_mcp_oauth_pkce_cleanup(&claim, settlement)
                .await?;
            if !settled {
                summary.lost_claims = summary.lost_claims.saturating_add(1);
                continue;
            }
            match settlement {
                McpOAuthPkceCleanupSettlement::Completed => {
                    summary.completed = summary.completed.saturating_add(1);
                }
                McpOAuthPkceCleanupSettlement::Retry { .. } => {
                    summary.deferred = summary.deferred.saturating_add(1);
                }
                McpOAuthPkceCleanupSettlement::DeadLetter { .. } => {
                    summary.dead_lettered = summary.dead_lettered.saturating_add(1);
                }
            }
        }
        Ok(summary)
    }
}

fn retry_delay(attempts: u32, base: u64, maximum: u64) -> u64 {
    let multiplier = 1_u64.checked_shl(attempts.min(20)).unwrap_or(u64::MAX);
    base.saturating_mul(multiplier).min(maximum)
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{SecretPurpose, Sha256Digest};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1c9-32e4-75e1-a9e8-d95ca0f5{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn request() -> McpOAuthPkceCleanupRequest {
        McpOAuthPkceCleanupRequest {
            tenant_id: id(ResourceKind::Tenant, 1),
            task_id: id(ResourceKind::Interaction, 2),
            cause: McpOAuthPkceCleanupCause::Expired,
            hint: McpOAuthPkceCleanupHint {
                schema_version: 1,
                secret_binding_id: id(ResourceKind::SecretBinding, 3),
                binding_generation: 4,
            },
        }
    }

    struct FixtureAuthority {
        stale: bool,
    }

    #[async_trait]
    impl McpOAuthPkceCleanupAuthority for FixtureAuthority {
        async fn authorize_cleanup(
            &self,
            request: &McpOAuthPkceCleanupRequest,
        ) -> Result<AuthorizedMcpOAuthPkceCleanup, McpOAuthPkceCleanupAuthorityError> {
            if self.stale {
                return Err(McpOAuthPkceCleanupAuthorityError::StaleOrNotFound);
            }
            Ok(AuthorizedMcpOAuthPkceCleanup {
                tenant_id: request.tenant_id.clone(),
                task_id: request.task_id.clone(),
                secret_binding: ExactSecretBindingRef::build(
                    request.hint.secret_binding_id.clone(),
                    request.hint.binding_generation,
                    id(ResourceKind::SecretProvider, 5),
                    crate::MCP_OAUTH_PKCE_SECRET_PURPOSE
                        .parse::<SecretPurpose>()
                        .unwrap(),
                    SecretResolutionPolicy::Pinned {
                        opaque_version_identity_digest: digest('a'),
                    },
                )
                .unwrap(),
            })
        }
    }

    struct FixtureCleaner {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl McpOAuthPkceSecretCleaner for FixtureCleaner {
        async fn delete_exact(
            &self,
            _authorization: &AuthorizedMcpOAuthPkceCleanup,
        ) -> Result<McpOAuthPkceSecretCleanupDisposition, McpOAuthPkceSecretCleanupError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(McpOAuthPkceSecretCleanupDisposition::Deleted)
        }
    }

    struct FailingCleaner(McpOAuthPkceSecretCleanupError);

    #[async_trait]
    impl McpOAuthPkceSecretCleaner for FailingCleaner {
        async fn delete_exact(
            &self,
            _authorization: &AuthorizedMcpOAuthPkceCleanup,
        ) -> Result<McpOAuthPkceSecretCleanupDisposition, McpOAuthPkceSecretCleanupError> {
            Err(self.0)
        }
    }

    struct FixtureOutbox {
        claims: Mutex<Vec<ClaimedMcpOAuthPkceCleanup>>,
        settlements: Mutex<Vec<McpOAuthPkceCleanupSettlement>>,
        wins_settlement: bool,
    }

    #[async_trait]
    impl McpOAuthPkceCleanupOutbox for FixtureOutbox {
        async fn claim_due_mcp_oauth_pkce_cleanups(
            &self,
            _command: ClaimDueMcpOAuthPkceCleanups,
        ) -> Result<Vec<ClaimedMcpOAuthPkceCleanup>, McpOAuthPkceCleanupDeliveryError> {
            Ok(std::mem::take(&mut *self.claims.lock().unwrap()))
        }

        async fn settle_mcp_oauth_pkce_cleanup(
            &self,
            _claim: &ClaimedMcpOAuthPkceCleanup,
            settlement: McpOAuthPkceCleanupSettlement,
        ) -> Result<bool, McpOAuthPkceCleanupDeliveryError> {
            self.settlements.lock().unwrap().push(settlement);
            Ok(self.wins_settlement)
        }
    }

    fn cleanup_claim(attempts: u32) -> ClaimedMcpOAuthPkceCleanup {
        ClaimedMcpOAuthPkceCleanup {
            outbox_id: id(ResourceKind::OutboxEvent, 6),
            event_id: id(ResourceKind::Event, 7),
            claim_owner: id(ResourceKind::WorkerProcessGeneration, 8),
            claim_epoch: 1,
            publish_attempts: attempts,
            trace: TraceIdentityV1::generate(),
            request: request(),
        }
    }

    fn worker_config() -> McpOAuthPkceCleanupWorkerConfig {
        McpOAuthPkceCleanupWorkerConfig {
            maximum_batch: 16,
            maximum_lease_milliseconds: 60_000,
            claim_batch: 4,
            lease_milliseconds: 30_000,
            retry_base_milliseconds: 1_000,
            retry_maximum_milliseconds: 60_000,
        }
    }

    #[tokio::test]
    async fn cleanup_revalidates_exact_binding_before_secret_manager_delete() {
        let cleaner = Arc::new(FixtureCleaner {
            calls: AtomicUsize::new(0),
        });
        let consumer = McpOAuthPkceCleanupConsumer::new(
            Arc::new(FixtureAuthority { stale: false }),
            cleaner.clone(),
        );
        assert_eq!(
            consumer.consume(request()).await.unwrap(),
            McpOAuthPkceCleanupOutcome::Deleted
        );
        assert_eq!(cleaner.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn stale_cleanup_hint_never_reaches_secret_manager() {
        let cleaner = Arc::new(FixtureCleaner {
            calls: AtomicUsize::new(0),
        });
        let consumer = McpOAuthPkceCleanupConsumer::new(
            Arc::new(FixtureAuthority { stale: true }),
            cleaner.clone(),
        );
        assert_eq!(
            consumer.consume(request()).await.unwrap(),
            McpOAuthPkceCleanupOutcome::IgnoredStale
        );
        assert_eq!(cleaner.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn durable_cleanup_hint_contains_only_the_exact_secret_identity() {
        let request = request();
        assert_eq!(
            serde_json::to_value(&request.hint).unwrap(),
            serde_json::json!({
                "binding_generation": 4,
                "schema_version": 1,
                "secret_binding_id": request.hint.secret_binding_id,
            })
        );
    }

    #[tokio::test]
    async fn delivery_worker_completes_only_through_the_exact_outbox_fence() {
        let outbox = Arc::new(FixtureOutbox {
            claims: Mutex::new(vec![cleanup_claim(0)]),
            settlements: Mutex::new(Vec::new()),
            wins_settlement: true,
        });
        let consumer = Arc::new(McpOAuthPkceCleanupConsumer::new(
            Arc::new(FixtureAuthority { stale: false }),
            Arc::new(FixtureCleaner {
                calls: AtomicUsize::new(0),
            }),
        ));
        let worker = McpOAuthPkceCleanupWorker::new(
            id(ResourceKind::WorkerProcessGeneration, 8),
            outbox.clone(),
            consumer,
            worker_config(),
        )
        .unwrap();

        assert_eq!(
            worker.run_once().await.unwrap(),
            McpOAuthPkceCleanupWorkerSummary {
                claimed: 1,
                completed: 1,
                ..McpOAuthPkceCleanupWorkerSummary::default()
            }
        );
        assert_eq!(
            *outbox.settlements.lock().unwrap(),
            vec![McpOAuthPkceCleanupSettlement::Completed]
        );
    }

    #[tokio::test]
    async fn uncertain_delete_is_deferred_with_bounded_backoff() {
        let outbox = Arc::new(FixtureOutbox {
            claims: Mutex::new(vec![cleanup_claim(3)]),
            settlements: Mutex::new(Vec::new()),
            wins_settlement: true,
        });
        let consumer = Arc::new(McpOAuthPkceCleanupConsumer::new(
            Arc::new(FixtureAuthority { stale: false }),
            Arc::new(FailingCleaner(
                McpOAuthPkceSecretCleanupError::OutcomeUncertain,
            )),
        ));
        let worker = McpOAuthPkceCleanupWorker::new(
            id(ResourceKind::WorkerProcessGeneration, 8),
            outbox.clone(),
            consumer,
            worker_config(),
        )
        .unwrap();

        assert_eq!(worker.run_once().await.unwrap().deferred, 1);
        assert_eq!(
            *outbox.settlements.lock().unwrap(),
            vec![McpOAuthPkceCleanupSettlement::Retry {
                failure_code: "mcp_oauth_pkce_cleanup_outcome_uncertain",
                delay_milliseconds: 8_000,
            }]
        );
    }
}
