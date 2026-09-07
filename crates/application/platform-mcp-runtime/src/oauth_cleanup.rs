use insight_platform_contracts::{ResourceId, ResourceKind, TraceFlags};
use insight_platform_execution_context::{scope_trace, ExecutionTraceContext};
use insight_platform_jobs::store::SafetyScanCursor;

use std::sync::Arc;

use insight_platform_mcp_host::*;

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

pub struct McpOAuthPkceCleanupWorker {
    worker_process_generation_id: ResourceId,
    jobs: Arc<dyn McpOAuthPkceCleanupJobs>,
    pools: insight_platform_worker::LocalWorkerPools,
    expiry_cursor: tokio::sync::Mutex<Option<SafetyScanCursor>>,
    consumer: Arc<McpOAuthPkceCleanupConsumer>,
    config: McpOAuthPkceCleanupWorkerConfig,
}

impl McpOAuthPkceCleanupWorker {
    pub fn new(
        worker_process_generation_id: ResourceId,
        jobs: Arc<dyn McpOAuthPkceCleanupJobs>,
        worker_manifest: insight_platform_contracts::WorkerManifest,
        consumer: Arc<McpOAuthPkceCleanupConsumer>,
        config: McpOAuthPkceCleanupWorkerConfig,
    ) -> Result<Self, McpOAuthPkceCleanupDeliveryError> {
        config.validate()?;
        worker_manifest
            .validate()
            .map_err(|_| McpOAuthPkceCleanupDeliveryError::InvalidCommand)?;
        if worker_manifest.work_class != insight_platform_contracts::WorkClass::Recovery {
            return Err(McpOAuthPkceCleanupDeliveryError::InvalidCommand);
        }
        if worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration {
            return Err(McpOAuthPkceCleanupDeliveryError::InvalidCommand);
        }
        let pools = insight_platform_worker::LocalWorkerPools::new(
            worker_manifest,
            worker_process_generation_id.clone(),
        )
        .map_err(|_| McpOAuthPkceCleanupDeliveryError::InvalidCommand)?;
        Ok(Self {
            worker_process_generation_id,
            jobs,
            pools,
            expiry_cursor: tokio::sync::Mutex::new(None),
            consumer,
            config,
        })
    }

    async fn expire_due_tasks(&self) -> Result<(), McpOAuthPkceCleanupDeliveryError> {
        let Some(_permit) = self
            .pools
            .try_acquire_critical_control()
            .map_err(|_| McpOAuthPkceCleanupDeliveryError::InvalidCommand)?
        else {
            return Ok(());
        };
        // One instance advances one finite global sweep. This is a scheduling hint;
        // PostgreSQL time, current Task state and row locks remain the authority.
        let mut cursor = self.expiry_cursor.lock().await;
        let limit = self.config.claim_batch.min(MAX_MCP_OAUTH_EXPIRY_BATCH);
        let page = self
            .jobs
            .expire_due_mcp_oauth_tasks(DriveExpiredMcpOAuthTasks {
                after: cursor.clone(),
                scheduler_generation_id: self.worker_process_generation_id.clone(),
                limit,
                slots: (0..limit)
                    .map(|_| McpOAuthExpirySlot {
                        event_id: ResourceId::from_uuid_v7(
                            ResourceKind::Event,
                            uuid::Uuid::now_v7(),
                        )
                        .expect("generated event identity"),
                        outbox_id: ResourceId::from_uuid_v7(
                            ResourceKind::OutboxEvent,
                            uuid::Uuid::now_v7(),
                        )
                        .expect("generated outbox identity"),
                    })
                    .collect(),
            })
            .await?;
        if page.records.len() + page.diagnostics.len() > usize::from(limit)
            || page
                .records
                .iter()
                .any(|id| id.kind() != ResourceKind::Interaction)
            || page.diagnostics.iter().any(|diagnostic| {
                diagnostic.validate().is_err()
                    || diagnostic.item_id.kind() != ResourceKind::Interaction
            })
            || (!page.exhausted && page.next_cursor.is_none())
        {
            return Err(McpOAuthPkceCleanupDeliveryError::CorruptJob);
        }
        if let Some(next) = &page.next_cursor {
            next.validate(ResourceKind::Interaction)
                .map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptJob)?;
            if cursor.as_ref().is_some_and(|previous| {
                (next.sort_at, &next.tenant_id, &next.item_id)
                    <= (previous.sort_at, &previous.tenant_id, &previous.item_id)
            }) {
                return Err(McpOAuthPkceCleanupDeliveryError::CorruptJob);
            }
        }
        for diagnostic in &page.diagnostics {
            eprintln!(
                "mcp_oauth_expiry_retained_invalid_object phase={:?} code={:?}",
                diagnostic.phase, diagnostic.code
            );
        }
        *cursor = if page.exhausted {
            None
        } else {
            page.next_cursor
        };
        // Both cursor and control permit are released before business claims reserve capacity.
        Ok(())
    }

    pub async fn run_once(
        &self,
    ) -> Result<McpOAuthPkceCleanupWorkerSummary, McpOAuthPkceCleanupDeliveryError> {
        self.expire_due_tasks().await?;
        let hard_limit = insight_platform_worker::ClaimBatchHardLimit::from_profile(
            &insight_platform_contracts::checked_in_hard_limit_profile(),
        )
        .map_err(|_| McpOAuthPkceCleanupDeliveryError::InvalidCommand)?;
        let Some(reservation) = self
            .pools
            .reserve_claim_capacity(
                insight_platform_contracts::WorkClass::Recovery,
                usize::from(self.config.claim_batch),
                hard_limit,
            )
            .map_err(|_| McpOAuthPkceCleanupDeliveryError::InvalidCommand)?
        else {
            return Ok(McpOAuthPkceCleanupWorkerSummary::default());
        };
        let claim_limit = u16::try_from(reservation.claim_limit())
            .map_err(|_| McpOAuthPkceCleanupDeliveryError::InvalidCommand)?;
        let claims = self
            .jobs
            .claim_due_mcp_oauth_pkce_cleanups(ClaimDueMcpOAuthPkceCleanups {
                worker_manifest: self.pools.manifest().clone(),
                claim_owner: self.worker_process_generation_id.clone(),
                lease_token_digests: (0..claim_limit)
                    .map(|_| {
                        insight_platform_contracts::canonical_digest(
                            &serde_json::json!({"lease_entropy":uuid::Uuid::new_v4().to_string()}),
                        )
                        .expect("finite lease entropy")
                        .parse()
                        .expect("canonical digest")
                    })
                    .collect(),
                maximum_claims: claim_limit,
                lease_milliseconds: self.config.lease_milliseconds,
            })
            .await?;
        let mut summary = McpOAuthPkceCleanupWorkerSummary {
            claimed: u16::try_from(claims.len())
                .map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptJob)?,
            ..McpOAuthPkceCleanupWorkerSummary::default()
        };
        let permits = reservation
            .bind_claimed_jobs(
                claims
                    .iter()
                    .map(|claim| insight_platform_worker::ClaimedJobIdentity {
                        job_id: claim.request.cleanup_job_id.clone(),
                        lease_generation: claim.request.fence.lease_generation,
                    })
                    .collect(),
            )
            .map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptJob)?;
        // Every returned lease already owns a physical slot. Poll the bounded
        // batch concurrently so later claims do not spend their lease in a queue.
        let outcomes = futures::future::join_all(claims.into_iter().zip(permits).map(
            |(claim, permit)| async move {
                let _permit = permit;
                claim.validate()?;
                let trace = ExecutionTraceContext::start(claim.trace, TraceFlags::NotSampled)
                    .map_err(|_| McpOAuthPkceCleanupDeliveryError::CorruptJob)?;
                let outcome = tokio::time::timeout(
                    std::time::Duration::from_millis(self.config.lease_milliseconds / 2),
                    scope_trace(trace, self.consumer.consume(claim.request.clone())),
                )
                .await
                .unwrap_or(Err(McpOAuthPkceCleanupError::OutcomeUncertain(
                    "mcp_oauth_pkce_cleanup_outcome_uncertain",
                )));
                let settlement = match outcome {
                    Ok(McpOAuthPkceCleanupOutcome::Deleted) => {
                        McpOAuthPkceCleanupSettlement::Completed {
                            proof: McpOAuthPkceSecretCleanupDisposition::Deleted,
                        }
                    }
                    Ok(McpOAuthPkceCleanupOutcome::AlreadyAbsent) => {
                        McpOAuthPkceCleanupSettlement::Completed {
                            proof: McpOAuthPkceSecretCleanupDisposition::AlreadyAbsent,
                        }
                    }
                    Ok(McpOAuthPkceCleanupOutcome::IgnoredStale) => {
                        McpOAuthPkceCleanupSettlement::Stale
                    }
                    Err(McpOAuthPkceCleanupError::Rejected(code)) => {
                        McpOAuthPkceCleanupSettlement::DeadLetter { failure_code: code }
                    }
                    Err(McpOAuthPkceCleanupError::TemporarilyUnavailable(code))
                    | Err(McpOAuthPkceCleanupError::OutcomeUncertain(code)) => {
                        McpOAuthPkceCleanupSettlement::Retry {
                            failure_code: code,
                            delay_milliseconds: retry_delay(
                                claim.attempt_no,
                                self.config.retry_base_milliseconds,
                                self.config.retry_maximum_milliseconds,
                            ),
                        }
                    }
                };
                let settled = self
                    .jobs
                    .settle_mcp_oauth_pkce_cleanup(&claim, settlement)
                    .await?;
                Ok::<_, McpOAuthPkceCleanupDeliveryError>((settlement, settled))
            },
        ))
        .await;
        for outcome in outcomes {
            let (settlement, settled) = outcome?;
            if !settled {
                summary.lost_claims = summary.lost_claims.saturating_add(1);
                continue;
            }
            match settlement {
                McpOAuthPkceCleanupSettlement::Stale => {
                    summary.lost_claims = summary.lost_claims.saturating_add(1);
                }
                McpOAuthPkceCleanupSettlement::Completed { .. } => {
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
    use async_trait::async_trait;
    use insight_platform_contracts::{
        ExactSecretBindingRef, SecretResolutionPolicy, TraceIdentityV1,
    };
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
        let hint = McpOAuthPkceCleanupHint {
            schema_version: 1,
            secret_binding_id: id(ResourceKind::SecretBinding, 3),
            binding_generation: 4,
        };
        McpOAuthPkceCleanupRequest {
            cleanup_job_id: id(ResourceKind::Job, 6),
            task_generation: 1,
            deletion_effect_identity: McpOAuthPkceCleanupJobPayload::effect_identity(
                &id(ResourceKind::Tenant, 1),
                &id(ResourceKind::Interaction, 2),
                1,
                &hint,
            )
            .unwrap(),
            fence: insight_platform_jobs::JobFence {
                expected_version: 3,
                worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, 8),
                lease_generation: 1,
                token_digest: digest('b'),
            },
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
                    insight_platform_mcp_host::MCP_OAUTH_PKCE_SECRET_PURPOSE
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
    impl McpOAuthPkceCleanupJobs for FixtureOutbox {
        async fn expire_due_mcp_oauth_tasks(
            &self,
            command: DriveExpiredMcpOAuthTasks,
        ) -> Result<
            insight_platform_jobs::store::SafetyScanPage<ResourceId>,
            McpOAuthPkceCleanupDeliveryError,
        > {
            command.validate().unwrap();
            Ok(insight_platform_jobs::store::SafetyScanPage {
                records: Vec::new(),
                diagnostics: Vec::new(),
                next_cursor: None,
                exhausted: true,
            })
        }
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
            event_id: id(ResourceKind::Event, 7),
            attempt_no: attempts.max(1),
            trace: TraceIdentityV1::generate(),
            request: request(),
        }
    }

    fn worker_manifest() -> insight_platform_contracts::WorkerManifest {
        insight_platform_contracts::WorkerManifest {
            manifest_version: 2,
            worker_role: "cleanup_test".into(),
            work_class: insight_platform_contracts::WorkClass::Recovery,
            adapter_runtime_digest: digest('1'),
            worker_build_digest: digest('2'),
            execution_capabilities: insight_platform_contracts::WorkerExecutionCapabilities {
                schema_version: 1,
                capabilities: vec![mcp_oauth_cleanup_execution_capability()],
            },
            protocol_version: 1,
            max_concurrency: 4,
            critical_control_reserved_slots: 1,
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
    async fn delivery_worker_completes_only_through_the_exact_job_fence() {
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
            worker_manifest(),
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
            vec![McpOAuthPkceCleanupSettlement::Completed {
                proof: McpOAuthPkceSecretCleanupDisposition::Deleted
            }]
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
            worker_manifest(),
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
    struct ExpiryJobs {
        pages: Mutex<
            std::collections::VecDeque<
                Result<
                    insight_platform_jobs::store::SafetyScanPage<ResourceId>,
                    McpOAuthPkceCleanupDeliveryError,
                >,
            >,
        >,
        commands: Mutex<Vec<DriveExpiredMcpOAuthTasks>>,
        claimed: AtomicUsize,
    }
    #[async_trait]
    impl McpOAuthPkceCleanupJobs for ExpiryJobs {
        async fn expire_due_mcp_oauth_tasks(
            &self,
            command: DriveExpiredMcpOAuthTasks,
        ) -> Result<
            insight_platform_jobs::store::SafetyScanPage<ResourceId>,
            McpOAuthPkceCleanupDeliveryError,
        > {
            command.validate().unwrap();
            self.commands.lock().unwrap().push(command);
            self.pages
                .lock()
                .unwrap()
                .pop_front()
                .expect("declared expiry result")
        }
        async fn claim_due_mcp_oauth_pkce_cleanups(
            &self,
            command: ClaimDueMcpOAuthPkceCleanups,
        ) -> Result<Vec<ClaimedMcpOAuthPkceCleanup>, McpOAuthPkceCleanupDeliveryError> {
            command.validate(16, 60_000).unwrap();
            self.claimed.fetch_add(1, Ordering::SeqCst);
            Ok(Vec::new())
        }
        async fn settle_mcp_oauth_pkce_cleanup(
            &self,
            _claim: &ClaimedMcpOAuthPkceCleanup,
            _settlement: McpOAuthPkceCleanupSettlement,
        ) -> Result<bool, McpOAuthPkceCleanupDeliveryError> {
            panic!("no cleanup claims")
        }
    }
    fn expiry_worker(jobs: Arc<ExpiryJobs>) -> McpOAuthPkceCleanupWorker {
        McpOAuthPkceCleanupWorker::new(
            id(ResourceKind::WorkerProcessGeneration, 8),
            jobs,
            worker_manifest(),
            Arc::new(McpOAuthPkceCleanupConsumer::new(
                Arc::new(FixtureAuthority { stale: false }),
                Arc::new(FixtureCleaner {
                    calls: AtomicUsize::new(0),
                }),
            )),
            worker_config(),
        )
        .unwrap()
    }
    fn expiry_cursor(suffix: u16) -> SafetyScanCursor {
        SafetyScanCursor {
            sort_at: "2026-09-06T00:00:00Z".parse().unwrap(),
            tenant_id: id(ResourceKind::Tenant, 1),
            item_id: id(ResourceKind::Interaction, suffix),
        }
    }
    fn expiry_page(
        cursor: Option<SafetyScanCursor>,
        exhausted: bool,
    ) -> insight_platform_jobs::store::SafetyScanPage<ResourceId> {
        insight_platform_jobs::store::SafetyScanPage {
            records: Vec::new(),
            diagnostics: Vec::new(),
            next_cursor: cursor,
            exhausted,
        }
    }
    #[tokio::test]
    async fn expiry_retains_bad_task_and_advances_then_restarts_finite_sweep() {
        use insight_platform_jobs::store::{
            SafeScanDiagnostic, SafetyScanDiagnosticCode, SafetyScanPhase,
        };
        let cursor = expiry_cursor(30);
        let mut bad_page = expiry_page(Some(cursor.clone()), false);
        bad_page.diagnostics.push(SafeScanDiagnostic {
            schema_version: 1,
            tenant_id: cursor.tenant_id.clone(),
            item_id: cursor.item_id.clone(),
            phase: SafetyScanPhase::OwnerDecode,
            code: SafetyScanDiagnosticCode::InvalidPersistedObject,
        });
        let jobs = Arc::new(ExpiryJobs {
            pages: Mutex::new(
                [
                    Ok(bad_page),
                    Ok(expiry_page(None, true)),
                    Ok(expiry_page(None, true)),
                ]
                .into(),
            ),
            commands: Mutex::new(Vec::new()),
            claimed: AtomicUsize::new(0),
        });
        let worker = expiry_worker(jobs.clone());
        for _ in 0..3 {
            worker.run_once().await.unwrap();
        }
        assert_eq!(
            jobs.claimed.load(Ordering::SeqCst),
            3,
            "isolated corruption does not suppress cleanup claims"
        );
        let commands = jobs.commands.lock().unwrap();
        assert_eq!(
            commands
                .iter()
                .map(|command| command.after.clone())
                .collect::<Vec<_>>(),
            vec![None, Some(cursor), None]
        );
        let allocated: std::collections::BTreeSet<_> = commands
            .iter()
            .flat_map(|command| command.slots.iter())
            .flat_map(|slot| [&slot.event_id, &slot.outbox_id])
            .collect();
        assert_eq!(
            allocated.len(),
            commands.len() * usize::from(worker.config.claim_batch) * 2
        );
        assert_eq!(worker.pools.snapshot().critical_control_available, 1);
    }
    #[tokio::test]
    async fn expiry_failure_preserves_cursor_and_releases_control_without_claiming() {
        let cursor = expiry_cursor(30);
        let jobs = Arc::new(ExpiryJobs {
            pages: Mutex::new(
                [
                    Ok(expiry_page(Some(cursor.clone()), false)),
                    Err(McpOAuthPkceCleanupDeliveryError::Unavailable),
                    Ok(expiry_page(None, true)),
                ]
                .into(),
            ),
            commands: Mutex::new(Vec::new()),
            claimed: AtomicUsize::new(0),
        });
        let worker = expiry_worker(jobs.clone());
        worker.run_once().await.unwrap();
        assert_eq!(
            worker.run_once().await.unwrap_err(),
            McpOAuthPkceCleanupDeliveryError::Unavailable
        );
        assert_eq!(jobs.claimed.load(Ordering::SeqCst), 1);
        assert_eq!(*worker.expiry_cursor.lock().await, Some(cursor.clone()));
        assert_eq!(worker.pools.snapshot().critical_control_available, 1);
        worker.run_once().await.unwrap();
        assert_eq!(jobs.commands.lock().unwrap()[2].after, Some(cursor));
        assert_eq!(jobs.claimed.load(Ordering::SeqCst), 2);
    }
    #[tokio::test]
    async fn expiry_and_cleanup_obey_independent_physical_capacity() {
        let jobs = Arc::new(ExpiryJobs {
            pages: Mutex::new([Ok(expiry_page(None, true))].into()),
            commands: Mutex::new(Vec::new()),
            claimed: AtomicUsize::new(0),
        });
        let worker = expiry_worker(jobs.clone());
        let control = worker
            .pools
            .try_acquire_critical_control()
            .unwrap()
            .unwrap();
        worker.run_once().await.unwrap();
        assert!(
            jobs.commands.lock().unwrap().is_empty(),
            "expiry never runs without a control permit"
        );
        assert_eq!(jobs.claimed.load(Ordering::SeqCst), 1);
        drop(control);
        let limit = insight_platform_worker::ClaimBatchHardLimit::from_profile(
            &insight_platform_contracts::checked_in_hard_limit_profile(),
        )
        .unwrap();
        let business = worker
            .pools
            .reserve_claim_capacity(insight_platform_contracts::WorkClass::Recovery, 4, limit)
            .unwrap()
            .unwrap();
        worker.run_once().await.unwrap();
        assert_eq!(
            jobs.commands.lock().unwrap().len(),
            1,
            "expiry remains available while business slots are occupied"
        );
        assert_eq!(
            jobs.claimed.load(Ordering::SeqCst),
            1,
            "no durable claim without business capacity"
        );
        assert_eq!(worker.pools.snapshot().critical_control_available, 1);
        drop(business);
    }
}
