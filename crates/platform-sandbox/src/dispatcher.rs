//! Restart-safe CR-216 Sandbox Dispatcher orchestration.
//!
//! The Dispatcher owns no second business state. Every external action is preceded by a shared
//! Job fence transition, and every observation is committed back through the repository port.

use crate::opensandbox::{
    parse_result_frame, AuthorizeCandidateCreateV1, AuthorizeSandboxActivationV1,
    BoundedCandidatePageV1, CandidateCursorV1, ClaimedSandboxCleanupV1, CommitSandboxTerminalV1,
    LeasedSandboxJobV1, OpaqueActivationToken, OpenSandboxCreateV1, OpenSandboxProvider,
    PhysicalDecision, RecordProvisioningIntentV1, RecordSandboxCleanupObservationV1,
    RecordSandboxObservationV1, SandboxCandidateV1, SandboxCleanupObservationV1,
    SandboxContractError, SandboxDurableObservationV1, SandboxFencedIdentityV1,
    SandboxJobRepository, SandboxPhysicalPhaseV1, SandboxProviderError,
    SandboxRepositoryDecisionV1, SandboxRunnerOutcomeV1, SandboxRunnerPhaseV1,
    SandboxTerminalOutcomeV1, SelectSandboxCandidateV1, SANDBOX_CONTRACT_SCHEMA_VERSION,
};
use insight_platform_contracts::{canonical_digest, JobState, Sha256Digest};
use std::{collections::BTreeSet, error::Error, fmt, sync::Arc};

const MAX_DISPATCH_TRANSITIONS: usize = 32;

#[derive(Debug, Clone, PartialEq)]
pub enum SandboxDispatchProgressV1 {
    AwaitingCandidate(Box<LeasedSandboxJobV1>),
    AwaitingRunner(Box<LeasedSandboxJobV1>),
    TerminalCommitted(JobState),
}

#[derive(Debug)]
pub enum SandboxDispatchError<E> {
    Repository(E),
    Provider(SandboxProviderError),
    Contract(SandboxContractError),
    TransitionLimit,
}

impl<E: fmt::Display> fmt::Display for SandboxDispatchError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Repository(error) => write!(formatter, "sandbox repository failed: {error}"),
            Self::Provider(error) => write!(formatter, "{error}"),
            Self::Contract(error) => write!(formatter, "{error}"),
            Self::TransitionLimit => {
                formatter.write_str("sandbox dispatch transition limit reached")
            }
        }
    }
}

impl<E> Error for SandboxDispatchError<E> where E: Error + 'static {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxCleanupProgressV1 {
    CandidateAbsent,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxOrphanSweepProgressV1 {
    pub examined: u16,
    pub deleted: u16,
    pub next: Option<CandidateCursorV1>,
}

pub struct OpenSandboxDispatcher<R, P> {
    repository: Arc<R>,
    provider: Arc<P>,
}

impl<R, P> OpenSandboxDispatcher<R, P>
where
    R: SandboxJobRepository,
    P: OpenSandboxProvider,
{
    pub fn new(repository: Arc<R>, provider: Arc<P>) -> Self {
        Self {
            repository,
            provider,
        }
    }

    pub async fn drive_job(
        &self,
        mut leased: LeasedSandboxJobV1,
    ) -> Result<SandboxDispatchProgressV1, SandboxDispatchError<R::Error>> {
        leased.validate().map_err(SandboxDispatchError::Contract)?;
        for _ in 0..MAX_DISPATCH_TRANSITIONS {
            let Some(physical) = leased.payload.physical.as_deref() else {
                let decision = self
                    .repository
                    .record_provisioning_intent(RecordProvisioningIntentV1 {
                        identity: leased_identity(&leased),
                        activation_token: OpaqueActivationToken::generate()
                            .map_err(SandboxDispatchError::Contract)?,
                    })
                    .await
                    .map_err(SandboxDispatchError::Repository)?;
                apply_decision(&mut leased, decision).map_err(SandboxDispatchError::Contract)?;
                continue;
            };

            match physical.phase {
                SandboxPhysicalPhaseV1::Provisioning => {
                    let candidates = self.discover_candidates(&leased).await?;
                    if let Some(candidate) = candidates.first().cloned() {
                        for candidate in candidates {
                            let decision = self
                                .repository
                                .record_physical_observation(RecordSandboxObservationV1 {
                                    identity: leased_identity(&leased),
                                    observation: SandboxDurableObservationV1::Candidate {
                                        candidate,
                                        limits: leased.request.provisioning_limits.clone(),
                                    },
                                })
                                .await
                                .map_err(SandboxDispatchError::Repository)?;
                            apply_decision(&mut leased, decision)
                                .map_err(SandboxDispatchError::Contract)?;
                        }
                        let selected = self
                            .repository
                            .select_candidate(SelectSandboxCandidateV1 {
                                identity: leased_identity(&leased),
                                candidate,
                            })
                            .await
                            .map_err(SandboxDispatchError::Repository)?
                            .into_inner();
                        apply_decision(&mut leased, selected)
                            .map_err(SandboxDispatchError::Contract)?;
                        continue;
                    }

                    if physical.candidate_count != 0 {
                        return Err(SandboxDispatchError::Contract(
                            SandboxContractError::InvalidCandidate,
                        ));
                    }
                    let create_ordinal = physical.create_authorization_count.checked_add(1).ok_or(
                        SandboxDispatchError::Contract(
                            SandboxContractError::CandidateCreateConflict,
                        ),
                    )?;
                    let authorization = self
                        .repository
                        .authorize_candidate_create(AuthorizeCandidateCreateV1 {
                            identity: leased_identity(&leased),
                            create_ordinal,
                            limits: leased.request.provisioning_limits.clone(),
                        })
                        .await
                        .map_err(SandboxDispatchError::Repository)?;
                    let authorization = match authorization {
                        PhysicalDecision::Applied(authorization) => authorization,
                        PhysicalDecision::Replayed(authorization) => {
                            // A replayed authorization is deliberately burned. Calling the
                            // provider here would make an ambiguous response restart-unsafe.
                            apply_decision(&mut leased, authorization.decision)
                                .map_err(SandboxDispatchError::Contract)?;
                            return Ok(SandboxDispatchProgressV1::AwaitingCandidate(Box::new(
                                leased,
                            )));
                        }
                    };
                    apply_decision(&mut leased, authorization.decision)
                        .map_err(SandboxDispatchError::Contract)?;
                    let physical = leased.payload.physical.as_deref().ok_or(
                        SandboxDispatchError::Contract(
                            SandboxContractError::InvalidPhysicalTransition,
                        ),
                    )?;
                    let create = OpenSandboxCreateV1::from_authorization(
                        &leased.request,
                        physical,
                        authorization.create_ordinal,
                        sandbox_ttl_seconds(&leased),
                    )
                    .map_err(SandboxDispatchError::Contract)?;
                    let candidate = self
                        .provider
                        .create_candidate(create)
                        .await
                        .map_err(SandboxDispatchError::Provider)?;
                    let decision = self
                        .repository
                        .record_physical_observation(RecordSandboxObservationV1 {
                            identity: leased_identity(&leased),
                            observation: SandboxDurableObservationV1::Candidate {
                                candidate: candidate.clone(),
                                limits: leased.request.provisioning_limits.clone(),
                            },
                        })
                        .await
                        .map_err(SandboxDispatchError::Repository)?;
                    apply_decision(&mut leased, decision)
                        .map_err(SandboxDispatchError::Contract)?;
                    let selected = self
                        .repository
                        .select_candidate(SelectSandboxCandidateV1 {
                            identity: leased_identity(&leased),
                            candidate,
                        })
                        .await
                        .map_err(SandboxDispatchError::Repository)?
                        .into_inner();
                    apply_decision(&mut leased, selected)
                        .map_err(SandboxDispatchError::Contract)?;
                }
                SandboxPhysicalPhaseV1::CandidateSelected => {
                    let sandbox_id = selected_sandbox_id(&leased)?;
                    let state = self
                        .provider
                        .runner_state(&sandbox_id)
                        .await
                        .map_err(SandboxDispatchError::Provider)?;
                    if state.phase != SandboxRunnerPhaseV1::Armed {
                        return Err(SandboxDispatchError::Contract(
                            SandboxContractError::InvalidPhysicalTransition,
                        ));
                    }
                    let decision = self
                        .repository
                        .authorize_activation(AuthorizeSandboxActivationV1 {
                            identity: leased_identity(&leased),
                            sandbox_id,
                            boot_id: state.boot_id,
                        })
                        .await
                        .map_err(SandboxDispatchError::Repository)?
                        .into_inner();
                    apply_decision(&mut leased, decision)
                        .map_err(SandboxDispatchError::Contract)?;
                }
                SandboxPhysicalPhaseV1::ActivationAuthorized => {
                    let sandbox_id = selected_sandbox_id(&leased)?;
                    let state = self
                        .provider
                        .runner_state(&sandbox_id)
                        .await
                        .map_err(SandboxDispatchError::Provider)?;
                    let state = if state.phase == SandboxRunnerPhaseV1::Armed {
                        let frame = crate::opensandbox::SandboxActivationFrameV1::from_authorized(
                            &leased.request,
                            leased.payload.physical.as_deref().ok_or(
                                SandboxDispatchError::Contract(
                                    SandboxContractError::InvalidPhysicalTransition,
                                ),
                            )?,
                        )
                        .map_err(SandboxDispatchError::Contract)?;
                        self.provider
                            .activate(&sandbox_id, frame)
                            .await
                            .map_err(SandboxDispatchError::Provider)?
                    } else {
                        state
                    };
                    let decision = self
                        .repository
                        .record_physical_observation(RecordSandboxObservationV1 {
                            identity: leased_identity(&leased),
                            observation: SandboxDurableObservationV1::RunnerState { frame: state },
                        })
                        .await
                        .map_err(SandboxDispatchError::Repository)?;
                    apply_decision(&mut leased, decision)
                        .map_err(SandboxDispatchError::Contract)?;
                }
                SandboxPhysicalPhaseV1::Started => {
                    let sandbox_id = selected_sandbox_id(&leased)?;
                    let state = self
                        .provider
                        .runner_state(&sandbox_id)
                        .await
                        .map_err(SandboxDispatchError::Provider)?;
                    if matches!(
                        state.phase,
                        SandboxRunnerPhaseV1::ActivationLatched | SandboxRunnerPhaseV1::Started
                    ) {
                        return Ok(SandboxDispatchProgressV1::AwaitingRunner(Box::new(leased)));
                    }
                    let decision = self
                        .repository
                        .record_physical_observation(RecordSandboxObservationV1 {
                            identity: leased_identity(&leased),
                            observation: SandboxDurableObservationV1::RunnerState { frame: state },
                        })
                        .await
                        .map_err(SandboxDispatchError::Repository)?;
                    apply_decision(&mut leased, decision)
                        .map_err(SandboxDispatchError::Contract)?;
                }
                SandboxPhysicalPhaseV1::Succeeded | SandboxPhysicalPhaseV1::Failed => {
                    return self.commit_runner_result(&mut leased).await;
                }
                SandboxPhysicalPhaseV1::UnknownOutcome => {
                    let evidence_digest =
                        unknown_outcome_digest(&leased).map_err(SandboxDispatchError::Contract)?;
                    let committed = self
                        .repository
                        .commit_terminal(CommitSandboxTerminalV1 {
                            identity: leased_identity(&leased),
                            outcome: SandboxTerminalOutcomeV1::UnknownOutcome { evidence_digest },
                        })
                        .await
                        .map_err(SandboxDispatchError::Repository)?
                        .into_inner();
                    return Ok(SandboxDispatchProgressV1::TerminalCommitted(
                        committed.job.state,
                    ));
                }
                SandboxPhysicalPhaseV1::Cleaning | SandboxPhysicalPhaseV1::Absent => {
                    return Err(SandboxDispatchError::Contract(
                        SandboxContractError::InvalidPhysicalTransition,
                    ));
                }
            }
        }
        Err(SandboxDispatchError::TransitionLimit)
    }

    pub async fn cleanup_once(
        &self,
        claimed: ClaimedSandboxCleanupV1,
    ) -> Result<SandboxCleanupProgressV1, SandboxDispatchError<R::Error>> {
        claimed.validate().map_err(SandboxDispatchError::Contract)?;
        let physical =
            claimed
                .payload
                .physical
                .as_deref()
                .ok_or(SandboxDispatchError::Contract(
                    SandboxContractError::InvalidCleanup,
                ))?;
        let Some(sandbox_id) = physical
            .candidate_ids
            .iter()
            .find(|candidate| !physical.absent_candidate_ids.contains(candidate))
            .cloned()
        else {
            return Ok(SandboxCleanupProgressV1::Complete);
        };
        let terminated = self
            .provider
            .terminate(&sandbox_id)
            .await
            .map_err(SandboxDispatchError::Provider)?;
        terminated
            .validate()
            .map_err(SandboxDispatchError::Contract)?;
        let absence = self
            .provider
            .prove_absent(&sandbox_id)
            .await
            .map_err(SandboxDispatchError::Provider)?;
        absence.validate().map_err(SandboxDispatchError::Contract)?;
        if terminated.sandbox_id != sandbox_id
            || terminated.present
            || absence.sandbox_id != sandbox_id
            || absence.present
        {
            return Err(SandboxDispatchError::Contract(
                SandboxContractError::InvalidCleanup,
            ));
        }
        let next = self
            .repository
            .record_cleanup_observation(RecordSandboxCleanupObservationV1 {
                fence: claimed.fence,
                observation: SandboxCleanupObservationV1::Absent {
                    sandbox_id,
                    evidence_digest: absence.observation_digest,
                },
            })
            .await
            .map_err(SandboxDispatchError::Repository)?;
        Ok(if next.payload.cleanup.required {
            SandboxCleanupProgressV1::CandidateAbsent
        } else {
            SandboxCleanupProgressV1::Complete
        })
    }

    pub async fn sweep_orphan_page(
        &self,
        cursor: CandidateCursorV1,
    ) -> Result<SandboxOrphanSweepProgressV1, SandboxDispatchError<R::Error>> {
        let page = self
            .provider
            .list_operator_candidates(cursor)
            .await
            .map_err(SandboxDispatchError::Provider)?;
        validate_page_shape(&page).map_err(SandboxDispatchError::Contract)?;
        let mut deleted = 0_u16;
        for candidate in &page.items {
            let decision = self
                .repository
                .decide_orphan(candidate.clone())
                .await
                .map_err(SandboxDispatchError::Repository)?;
            if decision.disposition.may_delete() {
                let terminated = self
                    .provider
                    .terminate(&candidate.sandbox_id)
                    .await
                    .map_err(SandboxDispatchError::Provider)?;
                let absent = self
                    .provider
                    .prove_absent(&candidate.sandbox_id)
                    .await
                    .map_err(SandboxDispatchError::Provider)?;
                terminated
                    .validate()
                    .map_err(SandboxDispatchError::Contract)?;
                absent.validate().map_err(SandboxDispatchError::Contract)?;
                if terminated.present
                    || absent.present
                    || terminated.sandbox_id != candidate.sandbox_id
                    || absent.sandbox_id != candidate.sandbox_id
                {
                    return Err(SandboxDispatchError::Contract(
                        SandboxContractError::InvalidCleanup,
                    ));
                }
                deleted = deleted.saturating_add(1);
            }
        }
        Ok(SandboxOrphanSweepProgressV1 {
            examined: u16::try_from(page.items.len()).map_err(|_| {
                SandboxDispatchError::Contract(SandboxContractError::InvalidCandidate)
            })?,
            deleted,
            next: page.next,
        })
    }

    async fn discover_candidates(
        &self,
        leased: &LeasedSandboxJobV1,
    ) -> Result<Vec<SandboxCandidateV1>, SandboxDispatchError<R::Error>> {
        let physical = leased
            .payload
            .physical
            .as_deref()
            .ok_or(SandboxDispatchError::Contract(
                SandboxContractError::InvalidPhysicalTransition,
            ))?;
        let mut cursor = CandidateCursorV1 {
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            opaque: None,
        };
        let mut seen_cursors = BTreeSet::new();
        let mut seen_candidates = BTreeSet::new();
        let mut candidates = Vec::new();
        loop {
            if !seen_cursors.insert(cursor.opaque.clone()) {
                return Err(SandboxDispatchError::Contract(
                    SandboxContractError::InvalidCandidate,
                ));
            }
            let page = self
                .provider
                .list_candidates(physical.provisioning_token_digest.clone(), cursor)
                .await
                .map_err(SandboxDispatchError::Provider)?;
            validate_page_shape(&page).map_err(SandboxDispatchError::Contract)?;
            if page.items.len()
                > usize::from(leased.request.provisioning_limits.candidate_page_items)
            {
                return Err(SandboxDispatchError::Contract(
                    SandboxContractError::InvalidCandidate,
                ));
            }
            for candidate in page.items {
                candidate
                    .metadata
                    .validate_for(&leased.request, &physical.provisioning_token)
                    .map_err(SandboxDispatchError::Contract)?;
                if !seen_candidates.insert(candidate.sandbox_id.clone())
                    || candidates.len()
                        >= usize::from(leased.request.provisioning_limits.maximum_candidates)
                {
                    return Err(SandboxDispatchError::Contract(
                        SandboxContractError::CandidateLimitOrLateCandidate,
                    ));
                }
                candidates.push(candidate);
            }
            let Some(next) = page.next else {
                break;
            };
            if candidates.len()
                >= usize::from(leased.request.provisioning_limits.maximum_candidates)
            {
                return Err(SandboxDispatchError::Contract(
                    SandboxContractError::CandidateLimitOrLateCandidate,
                ));
            }
            cursor = next;
        }
        candidates.sort_by(|left, right| {
            left.metadata
                .create_ordinal
                .cmp(&right.metadata.create_ordinal)
                .then_with(|| left.sandbox_id.cmp(&right.sandbox_id))
        });
        Ok(candidates)
    }

    async fn commit_runner_result(
        &self,
        leased: &mut LeasedSandboxJobV1,
    ) -> Result<SandboxDispatchProgressV1, SandboxDispatchError<R::Error>> {
        let sandbox_id = selected_sandbox_id(leased)?;
        let boot_id = leased
            .payload
            .physical
            .as_deref()
            .and_then(|physical| physical.runner_boot_id.clone())
            .ok_or(SandboxDispatchError::Contract(
                SandboxContractError::InvalidResult,
            ))?;
        let maximum = leased
            .request
            .limits
            .maximum_output_bytes
            .saturating_add(u64::from(
                leased.request.provisioning_limits.runner_header_bytes,
            ));
        let bytes = self
            .provider
            .read_result(&sandbox_id, maximum)
            .await
            .map_err(SandboxDispatchError::Provider)?;
        let frame = parse_result_frame(&bytes, &leased.request, &boot_id)
            .map_err(SandboxDispatchError::Contract)?;
        if leased
            .payload
            .physical
            .as_deref()
            .is_some_and(|physical| physical.result_evidence.is_none())
        {
            let decision = self
                .repository
                .record_physical_observation(RecordSandboxObservationV1 {
                    identity: leased_identity(leased),
                    observation: SandboxDurableObservationV1::Result {
                        frame: frame.clone(),
                    },
                })
                .await
                .map_err(SandboxDispatchError::Repository)?;
            apply_decision(leased, decision).map_err(SandboxDispatchError::Contract)?;
        }
        let outcome = match &frame.result {
            SandboxRunnerOutcomeV1::Succeeded { .. } => SandboxTerminalOutcomeV1::Succeeded {
                result: Box::new(frame),
            },
            SandboxRunnerOutcomeV1::Failed { failure_class, .. } => {
                SandboxTerminalOutcomeV1::Failed {
                    failure_class: *failure_class,
                    evidence_digest: frame.frame_digest.clone(),
                    result: Some(Box::new(frame)),
                }
            }
        };
        let committed = self
            .repository
            .commit_terminal(CommitSandboxTerminalV1 {
                identity: leased_identity(leased),
                outcome,
            })
            .await
            .map_err(SandboxDispatchError::Repository)?
            .into_inner();
        Ok(SandboxDispatchProgressV1::TerminalCommitted(
            committed.job.state,
        ))
    }
}

fn apply_decision(
    leased: &mut LeasedSandboxJobV1,
    decision: SandboxRepositoryDecisionV1,
) -> Result<(), SandboxContractError> {
    decision.validate()?;
    let fence = decision.fence.ok_or(SandboxContractError::InvalidJob)?;
    leased.job = decision.job;
    leased.payload = decision.payload;
    leased.fence = fence;
    leased.validate()
}

fn leased_identity(leased: &LeasedSandboxJobV1) -> SandboxFencedIdentityV1 {
    SandboxFencedIdentityV1 {
        tenant_id: leased.job.tenant_id.clone(),
        job_id: leased.job.job_id.clone(),
        fence: leased.fence.clone(),
    }
}

fn selected_sandbox_id<E>(
    leased: &LeasedSandboxJobV1,
) -> Result<crate::opensandbox::OpenSandboxId, SandboxDispatchError<E>> {
    leased
        .payload
        .physical
        .as_deref()
        .and_then(|physical| physical.selected_sandbox_id.clone())
        .ok_or(SandboxDispatchError::Contract(
            SandboxContractError::CandidateSelectionConflict,
        ))
}

fn sandbox_ttl_seconds(leased: &LeasedSandboxJobV1) -> u32 {
    let milliseconds = leased
        .request
        .limits
        .wall_milliseconds
        .saturating_add(leased.request.limits.cleanup_milliseconds);
    let seconds = milliseconds.saturating_add(999) / 1_000;
    u32::try_from(seconds).unwrap_or(u32::MAX).clamp(60, 3_600)
}

fn validate_page_shape(page: &BoundedCandidatePageV1) -> Result<(), SandboxContractError> {
    if page.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
        || page.next.as_ref().is_some_and(|next| {
            next.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION || next.opaque.is_none()
        })
        || page
            .items
            .iter()
            .any(|candidate| candidate.validate_shape().is_err())
    {
        return Err(SandboxContractError::InvalidCandidate);
    }
    Ok(())
}

fn unknown_outcome_digest(
    leased: &LeasedSandboxJobV1,
) -> Result<Sha256Digest, SandboxContractError> {
    let physical = leased
        .payload
        .physical
        .as_deref()
        .ok_or(SandboxContractError::InvalidPhysicalTransition)?;
    canonical_digest(&serde_json::json!({
        "domain": "insight.sandbox.unknown-outcome/v1",
        "job_id": leased.job.job_id,
        "physical_attempt": physical.provisioning_token.physical_attempt,
        "request_digest": leased.request.request_digest,
        "sandbox_id": physical.selected_sandbox_id,
        "runner_boot_id": physical.runner_boot_id,
    }))
    .map_err(|_| SandboxContractError::Canonical)?
    .parse()
    .map_err(|_| SandboxContractError::Canonical)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::opensandbox::{
        AuthorizeSandboxActivationV1, CandidateCreateAuthorizationV1, CommitSandboxTerminalV1,
        HeartbeatSandboxJobV1, OpenSandboxId, OpenSandboxObservationV1,
        RecordSandboxCleanupObservationV1, SandboxClaimV1, SandboxCleanupClaimV1,
        SandboxDispatcherJobPayloadV1, SandboxExecutionPlanV1, SandboxExecutionRequestV1,
        SandboxNetworkMode, SandboxOrphanDecisionV1, SandboxProvisioningLimitsV1,
        SandboxResourceLimitsV1, SandboxRunnerStateFrameV1,
    };
    use chrono::{Duration, TimeZone, Utc};
    use insight_platform_contracts::{
        DataClassification, ResourceId, ResourceKind, TraceIdentityV1, WorkClass,
    };
    use insight_platform_jobs::{
        decide_claim, decide_observation_update, decide_start, JobFence, JobOwnerRef,
        JobProjection, LeasePolicy,
    };
    use std::{
        convert::Infallible,
        sync::atomic::{AtomicUsize, Ordering},
    };
    use uuid::Uuid;

    struct AuthorizationRepository {
        authorization: PhysicalDecision<CandidateCreateAuthorizationV1>,
    }

    #[async_trait::async_trait]
    impl SandboxJobRepository for AuthorizationRepository {
        type Error = Infallible;

        async fn claim(
            &self,
            _request: SandboxClaimV1,
        ) -> Result<Vec<LeasedSandboxJobV1>, Self::Error> {
            panic!("claim is not used by this driver fixture")
        }

        async fn heartbeat(
            &self,
            _command: HeartbeatSandboxJobV1,
        ) -> Result<SandboxRepositoryDecisionV1, Self::Error> {
            panic!("heartbeat is not used by this driver fixture")
        }

        async fn record_provisioning_intent(
            &self,
            _command: RecordProvisioningIntentV1,
        ) -> Result<SandboxRepositoryDecisionV1, Self::Error> {
            panic!("provisioning intent is already present")
        }

        async fn authorize_candidate_create(
            &self,
            _command: AuthorizeCandidateCreateV1,
        ) -> Result<PhysicalDecision<CandidateCreateAuthorizationV1>, Self::Error> {
            Ok(self.authorization.clone())
        }

        async fn select_candidate(
            &self,
            _command: SelectSandboxCandidateV1,
        ) -> Result<PhysicalDecision<SandboxRepositoryDecisionV1>, Self::Error> {
            panic!("no candidate is returned")
        }

        async fn authorize_activation(
            &self,
            _command: AuthorizeSandboxActivationV1,
        ) -> Result<PhysicalDecision<SandboxRepositoryDecisionV1>, Self::Error> {
            panic!("activation is not reached")
        }

        async fn record_physical_observation(
            &self,
            _command: RecordSandboxObservationV1,
        ) -> Result<SandboxRepositoryDecisionV1, Self::Error> {
            panic!("the provider create response is intentionally lost")
        }

        async fn commit_terminal(
            &self,
            _command: CommitSandboxTerminalV1,
        ) -> Result<PhysicalDecision<SandboxRepositoryDecisionV1>, Self::Error> {
            panic!("terminal is not reached")
        }

        async fn claim_cleanup(
            &self,
            _request: SandboxCleanupClaimV1,
        ) -> Result<Vec<ClaimedSandboxCleanupV1>, Self::Error> {
            panic!("cleanup is not reached")
        }

        async fn record_cleanup_observation(
            &self,
            _command: RecordSandboxCleanupObservationV1,
        ) -> Result<ClaimedSandboxCleanupV1, Self::Error> {
            panic!("cleanup is not reached")
        }

        async fn decide_orphan(
            &self,
            _candidate: SandboxCandidateV1,
        ) -> Result<SandboxOrphanDecisionV1, Self::Error> {
            panic!("orphan cleanup is not reached")
        }

        async fn recover(
            &self,
            _tenant_id: &ResourceId,
            _job_id: &ResourceId,
        ) -> Result<SandboxRepositoryDecisionV1, Self::Error> {
            panic!("recovery is not reached")
        }
    }

    struct CountingProvider {
        create_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl OpenSandboxProvider for CountingProvider {
        async fn create_candidate(
            &self,
            _request: OpenSandboxCreateV1,
        ) -> Result<SandboxCandidateV1, SandboxProviderError> {
            self.create_calls.fetch_add(1, Ordering::SeqCst);
            Err(SandboxProviderError::Timeout)
        }

        async fn list_candidates(
            &self,
            _token_digest: Sha256Digest,
            _cursor: CandidateCursorV1,
        ) -> Result<BoundedCandidatePageV1, SandboxProviderError> {
            Ok(BoundedCandidatePageV1 {
                schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
                items: Vec::new(),
                next: None,
            })
        }

        async fn list_operator_candidates(
            &self,
            _cursor: CandidateCursorV1,
        ) -> Result<BoundedCandidatePageV1, SandboxProviderError> {
            panic!("orphan listing is not reached")
        }

        async fn observe(
            &self,
            _sandbox_id: &OpenSandboxId,
        ) -> Result<OpenSandboxObservationV1, SandboxProviderError> {
            panic!("observation is not reached")
        }

        async fn runner_state(
            &self,
            _sandbox_id: &OpenSandboxId,
        ) -> Result<SandboxRunnerStateFrameV1, SandboxProviderError> {
            panic!("runner state is not reached")
        }

        async fn activate(
            &self,
            _sandbox_id: &OpenSandboxId,
            _frame: crate::opensandbox::SandboxActivationFrameV1,
        ) -> Result<SandboxRunnerStateFrameV1, SandboxProviderError> {
            panic!("activation is not reached")
        }

        async fn read_result(
            &self,
            _sandbox_id: &OpenSandboxId,
            _maximum_bytes: u64,
        ) -> Result<Vec<u8>, SandboxProviderError> {
            panic!("result is not reached")
        }

        async fn terminate(
            &self,
            _sandbox_id: &OpenSandboxId,
        ) -> Result<OpenSandboxObservationV1, SandboxProviderError> {
            panic!("cleanup is not reached")
        }

        async fn prove_absent(
            &self,
            _sandbox_id: &OpenSandboxId,
        ) -> Result<OpenSandboxObservationV1, SandboxProviderError> {
            panic!("cleanup is not reached")
        }
    }

    #[tokio::test]
    async fn replayed_create_authorization_never_calls_provider() {
        let (leased, authorization) = fixture();
        let repository = Arc::new(AuthorizationRepository {
            authorization: PhysicalDecision::Replayed(authorization),
        });
        let provider = Arc::new(CountingProvider {
            create_calls: AtomicUsize::new(0),
        });
        let dispatcher = OpenSandboxDispatcher::new(repository, Arc::clone(&provider));
        assert!(matches!(
            dispatcher.drive_job(leased).await.unwrap(),
            SandboxDispatchProgressV1::AwaitingCandidate(_)
        ));
        assert_eq!(provider.create_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn applied_create_authorization_calls_provider_once_without_retry() {
        let (leased, authorization) = fixture();
        let repository = Arc::new(AuthorizationRepository {
            authorization: PhysicalDecision::Applied(authorization),
        });
        let provider = Arc::new(CountingProvider {
            create_calls: AtomicUsize::new(0),
        });
        let dispatcher = OpenSandboxDispatcher::new(repository, Arc::clone(&provider));
        assert!(matches!(
            dispatcher.drive_job(leased).await,
            Err(SandboxDispatchError::Provider(
                SandboxProviderError::Timeout
            ))
        ));
        assert_eq!(provider.create_calls.load(Ordering::SeqCst), 1);
    }

    fn fixture() -> (LeasedSandboxJobV1, CandidateCreateAuthorizationV1) {
        let deadline = Utc.timestamp_opt(2_000_000_000, 0).unwrap();
        let now = deadline - Duration::minutes(5);
        let worker = id(ResourceKind::WorkerProcessGeneration, 7);
        let request = SandboxExecutionRequestV1 {
            schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
            tenant_id: id(ResourceKind::Tenant, 1),
            invocation_id: id(ResourceKind::CapabilityInvocation, 2),
            job_id: id(ResourceKind::Job, 3),
            lease_generation: 1,
            physical_attempt: 1,
            worker_process_generation_id: worker.clone(),
            package_version_id: id(ResourceKind::SandboxPackageRevision, 4),
            image_uri: format!("registry.invalid/package@sha256:{}", "a".repeat(64)),
            runtime_version_id: id(ResourceKind::SandboxRuntimeRevision, 5),
            runtime_contract_digest: digest('d'),
            sandbox_profile_deployment_id: id(ResourceKind::SandboxProfileDeployment, 6),
            profile_deployment_digest: digest('e'),
            runner_argv: vec!["/usr/local/bin/platform-sandbox-runner".to_owned()],
            package_argv: vec!["/opt/insight/package".to_owned()],
            input_value_id: id(ResourceKind::RunValue, 8),
            output_value_id: id(ResourceKind::RunValue, 9),
            classification: DataClassification::Internal,
            input: serde_json::json!({"question":"answer"}),
            input_schema_digest: digest('b'),
            input_digest: digest('0'),
            output_schema_digest: digest('c'),
            network_mode: SandboxNetworkMode::Direct,
            limits: SandboxResourceLimitsV1 {
                maximum_input_bytes: 65_536,
                maximum_output_bytes: 65_536,
                cpu_millicores: 500,
                memory_mebibytes: 512,
                pids: 64,
                ephemeral_storage_bytes: 67_108_864,
                wall_milliseconds: 30_000,
                cleanup_milliseconds: 10_000,
            },
            provisioning_limits: SandboxProvisioningLimitsV1 {
                maximum_candidates: 2,
                candidate_page_items: 4,
                candidate_quiescence_milliseconds: 500,
                provisioning_timeout_milliseconds: 10_000,
                orphan_page_items: 20,
                runner_header_bytes: 8_192,
                diagnostic_bytes: 8_192,
            },
            deadline_at: deadline,
            trace: TraceIdentityV1::generate(),
            request_digest: digest('0'),
        }
        .seal()
        .unwrap();
        let plan = SandboxExecutionPlanV1::from_request(&request).unwrap();
        let ready = JobProjection {
            trace: request.trace,
            tenant_id: request.tenant_id.clone(),
            job_id: request.job_id.clone(),
            work_class: WorkClass::Sandbox,
            owner: JobOwnerRef {
                owner_id: request.job_id.clone(),
                owner_kind: ResourceKind::Job,
            },
            state: JobState::Ready,
            version: 1,
            attempt_count: 0,
            attempt_limit: 1,
            lease_generation: 0,
            lease: None,
            scheduled_at: now,
            retry_at: None,
            wake: None,
            deadline,
        };
        let leased_job = decide_claim(
            &ready,
            now,
            worker.clone(),
            digest('7'),
            LeasePolicy {
                requested_milliseconds: 30_000,
                hard_maximum_milliseconds: 60_000,
            },
        )
        .unwrap();
        let running = decide_start(&leased_job, &job_fence(&leased_job), now).unwrap();
        let bound_request = plan
            .bind_request(
                running.lease_generation,
                1,
                worker,
                running.trace,
                request.input,
            )
            .unwrap();
        let payload = SandboxDispatcherJobPayloadV1::accepted(plan)
            .unwrap()
            .begin(
                &bound_request,
                OpaqueActivationToken::parse("1".repeat(64)).unwrap(),
                now,
            )
            .unwrap();
        let leased = LeasedSandboxJobV1 {
            job: running.clone(),
            payload: payload.clone(),
            request: bound_request.clone(),
            fence: job_fence(&running),
            usage_reservation_id: id(ResourceKind::UsageReservation, 10),
        };
        leased.validate().unwrap();

        let authorized_payload = payload
            .authorize_candidate_create(&bound_request, &bound_request.provisioning_limits, now, 1)
            .unwrap()
            .into_inner();
        let authorized_job =
            decide_observation_update(&running, &job_fence(&running), now).unwrap();
        let authorization = CandidateCreateAuthorizationV1 {
            decision: SandboxRepositoryDecisionV1 {
                job: authorized_job.clone(),
                payload: authorized_payload,
                fence: Some(job_fence(&authorized_job)),
            },
            create_ordinal: 1,
        };
        authorization.validate().unwrap();
        (leased, authorization)
    }

    fn job_fence(job: &JobProjection) -> JobFence {
        let lease = job.lease.as_ref().unwrap();
        JobFence {
            expected_version: job.version,
            worker_process_generation_id: lease.worker_process_generation_id.clone(),
            lease_generation: lease.lease_generation,
            token_digest: lease.token_digest.clone(),
        }
    }

    fn id(kind: ResourceKind, sequence: u128) -> ResourceId {
        let raw = (sequence & ((1_u128 << 74) - 1)) | (7_u128 << 76) | (2_u128 << 62);
        ResourceId::from_uuid_v7(kind, Uuid::from_u128(raw)).unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }
}
