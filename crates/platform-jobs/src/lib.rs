//! Pure state decisions for the shared durable Job authority.
//!
//! This crate performs no I/O and never reads a wall clock. Repositories inject PostgreSQL time
//! and persist an accepted decision with an optimistic version and lease fence.

use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{
    is_execution_work_owner_pair, JobState, ResourceId, ResourceKind, Sha256Digest,
    TraceIdentityV1, WorkClass,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, error::Error, fmt};

pub const MAX_WAKE_SOURCES: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobOwnerRef {
    pub owner_id: ResourceId,
    pub owner_kind: ResourceKind,
}

impl JobOwnerRef {
    pub fn validate_for(&self, work_class: WorkClass) -> Result<(), JobError> {
        if self.owner_id.kind() != self.owner_kind
            || !is_execution_work_owner_pair(work_class, self.owner_kind)
        {
            return Err(JobError::InvalidOwner);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WakeSource {
    Callback,
    Poll,
    Signal,
    Timer,
    ChildRun,
    Cancel,
    Timeout,
}

impl WakeSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Callback => "callback",
            Self::Poll => "poll",
            Self::Signal => "signal",
            Self::Timer => "timer",
            Self::ChildRun => "child_run",
            Self::Cancel => "cancel",
            Self::Timeout => "timeout",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WakeKind {
    Timer,
    Signal,
    RemoteInvocation,
    ChildRun,
    RetryDeadline,
}

impl WakeKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Timer => "timer",
            Self::Signal => "signal",
            Self::RemoteInvocation => "remote_invocation",
            Self::ChildRun => "child_run",
            Self::RetryDeadline => "retry_deadline",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WakeContract {
    pub kind: WakeKind,
    pub generation: u64,
    pub accepted_sources: Vec<WakeSource>,
    pub expected_response_schema_digest: Option<Sha256Digest>,
    pub opaque_state_digest: Option<Sha256Digest>,
    pub next_poll_at: Option<DateTime<Utc>>,
    pub poll_count: u32,
    pub poll_limit: u32,
    pub callback_binding_digest: Option<Sha256Digest>,
    pub deadline: DateTime<Utc>,
}

impl WakeContract {
    pub fn validate(&self, job_deadline: DateTime<Utc>) -> Result<(), JobError> {
        let unique = self
            .accepted_sources
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if self.generation == 0
            || self.accepted_sources.is_empty()
            || self.accepted_sources.len() > MAX_WAKE_SOURCES
            || unique.len() != self.accepted_sources.len()
            || !self
                .accepted_sources
                .windows(2)
                .all(|pair| pair[0] < pair[1])
            || self.poll_count > self.poll_limit
            || self.deadline > job_deadline
            || self.next_poll_at.is_some_and(|next| next > self.deadline)
            || (self.accepted_sources.contains(&WakeSource::Poll) && self.poll_limit == 0)
            || (!self.accepted_sources.contains(&WakeSource::Poll)
                && (self.next_poll_at.is_some() || self.poll_count != 0 || self.poll_limit != 0))
        {
            return Err(JobError::InvalidWakeContract);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobLease {
    pub worker_process_generation_id: ResourceId,
    pub lease_generation: u64,
    pub token_digest: Sha256Digest,
    pub heartbeat_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl JobLease {
    fn validate(&self, deadline: DateTime<Utc>) -> Result<(), JobError> {
        if self.worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || self.lease_generation == 0
            || self.expires_at <= self.heartbeat_at
            || self.expires_at > deadline
        {
            return Err(JobError::InvalidLease);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobProjection {
    pub trace: TraceIdentityV1,
    pub tenant_id: ResourceId,
    pub job_id: ResourceId,
    pub work_class: WorkClass,
    pub owner: JobOwnerRef,
    pub state: JobState,
    pub version: u64,
    pub attempt_count: u32,
    pub attempt_limit: u32,
    pub lease_generation: u64,
    pub lease: Option<JobLease>,
    pub scheduled_at: DateTime<Utc>,
    pub retry_at: Option<DateTime<Utc>>,
    pub wake: Option<WakeContract>,
    pub deadline: DateTime<Utc>,
}

impl JobProjection {
    pub fn validate(&self) -> Result<(), JobError> {
        self.owner.validate_for(self.work_class)?;
        self.trace
            .validate()
            .map_err(|_| JobError::InvalidProjection)?;
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.job_id.kind() != ResourceKind::Job
            || self.version == 0
            || self.attempt_limit == 0
            || self.attempt_count > self.attempt_limit
            || self.scheduled_at > self.deadline
        {
            return Err(JobError::InvalidProjection);
        }
        if let Some(lease) = &self.lease {
            lease.validate(self.deadline)?;
            if lease.lease_generation != self.lease_generation
                || !matches!(
                    self.state,
                    JobState::Leased | JobState::Running | JobState::Cancelling
                )
            {
                return Err(JobError::InvalidProjection);
            }
        } else if matches!(self.state, JobState::Leased | JobState::Running) {
            return Err(JobError::InvalidProjection);
        }
        match self.state {
            JobState::Waiting => self
                .wake
                .as_ref()
                .ok_or(JobError::InvalidProjection)?
                .validate(self.deadline)?,
            _ if self.wake.is_some() => return Err(JobError::InvalidProjection),
            _ => {}
        }
        if self.state == JobState::RetryScheduled {
            let retry_at = self.retry_at.ok_or(JobError::InvalidProjection)?;
            if retry_at >= self.deadline {
                return Err(JobError::InvalidProjection);
            }
        } else if self.retry_at.is_some() {
            return Err(JobError::InvalidProjection);
        }
        if is_terminal(self.state) && self.lease.is_some() {
            return Err(JobError::InvalidProjection);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobFence {
    pub expected_version: u64,
    pub worker_process_generation_id: ResourceId,
    pub lease_generation: u64,
    pub token_digest: Sha256Digest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeasePolicy {
    pub requested_milliseconds: u64,
    pub hard_maximum_milliseconds: u64,
}

impl LeasePolicy {
    fn duration(self) -> Result<Duration, JobError> {
        if self.requested_milliseconds == 0
            || self.hard_maximum_milliseconds == 0
            || self.requested_milliseconds > self.hard_maximum_milliseconds
            || self.requested_milliseconds > i64::MAX as u64
        {
            return Err(JobError::InvalidLeasePolicy);
        }
        Ok(Duration::milliseconds(
            i64::try_from(self.requested_milliseconds).map_err(|_| JobError::InvalidLeasePolicy)?,
        ))
    }
}

pub fn decide_claim(
    current: &JobProjection,
    database_now: DateTime<Utc>,
    worker_process_generation_id: ResourceId,
    token_digest: Sha256Digest,
    policy: LeasePolicy,
) -> Result<JobProjection, JobError> {
    current.validate()?;
    if worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
        || !matches!(current.state, JobState::Ready | JobState::RetryScheduled)
        || current.attempt_count >= current.attempt_limit
        || current.scheduled_at > database_now
        || current
            .retry_at
            .is_some_and(|retry_at| retry_at > database_now)
        || database_now >= current.deadline
    {
        return Err(JobError::NotClaimable);
    }
    let expires_at = database_now
        .checked_add_signed(policy.duration()?)
        .ok_or(JobError::CounterOverflow)?
        .min(current.deadline);
    if expires_at <= database_now {
        return Err(JobError::NotClaimable);
    }
    let mut next = current.clone();
    next.state = JobState::Leased;
    next.version = increment(next.version)?;
    next.lease_generation = next
        .lease_generation
        .checked_add(1)
        .ok_or(JobError::CounterOverflow)?;
    next.retry_at = None;
    next.lease = Some(JobLease {
        worker_process_generation_id,
        lease_generation: next.lease_generation,
        token_digest,
        heartbeat_at: database_now,
        expires_at,
    });
    next.validate()?;
    Ok(next)
}

/// Claims a durable continuation of an already-started logical attempt.
///
/// A callback, poll, or human response resumes remote state; it does not redispatch the original
/// effect and therefore must not consume another attempt. The owner payload is responsible for
/// proving that this is a continuation before calling this decision.
pub fn decide_claim_continuation(
    current: &JobProjection,
    database_now: DateTime<Utc>,
    worker_process_generation_id: ResourceId,
    token_digest: Sha256Digest,
    policy: LeasePolicy,
) -> Result<JobProjection, JobError> {
    current.validate()?;
    if worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
        || current.state != JobState::Ready
        || current.attempt_count == 0
        || current.attempt_count > current.attempt_limit
        || current.scheduled_at > database_now
        || current.retry_at.is_some()
        || database_now >= current.deadline
    {
        return Err(JobError::NotClaimable);
    }
    let expires_at = database_now
        .checked_add_signed(policy.duration()?)
        .ok_or(JobError::CounterOverflow)?
        .min(current.deadline);
    if expires_at <= database_now {
        return Err(JobError::NotClaimable);
    }
    let mut next = current.clone();
    next.state = JobState::Leased;
    next.version = increment(next.version)?;
    next.lease_generation = increment(next.lease_generation)?;
    next.lease = Some(JobLease {
        worker_process_generation_id,
        lease_generation: next.lease_generation,
        token_digest,
        heartbeat_at: database_now,
        expires_at,
    });
    next.validate()?;
    Ok(next)
}

pub fn decide_start(
    current: &JobProjection,
    fence: &JobFence,
    database_now: DateTime<Utc>,
) -> Result<JobProjection, JobError> {
    validate_fence(current, fence, database_now)?;
    if current.state != JobState::Leased {
        return Err(JobError::InvalidTransition);
    }
    if current.attempt_count >= current.attempt_limit {
        return Err(JobError::InvalidTransition);
    }
    let mut next = transition(current, JobState::Running, true)?;
    next.attempt_count = next
        .attempt_count
        .checked_add(1)
        .ok_or(JobError::CounterOverflow)?;
    next.validate()?;
    Ok(next)
}

/// Starts a claimed continuation without incrementing the logical dispatch attempt counter.
pub fn decide_resume(
    current: &JobProjection,
    fence: &JobFence,
    database_now: DateTime<Utc>,
) -> Result<JobProjection, JobError> {
    validate_fence(current, fence, database_now)?;
    if current.state != JobState::Leased
        || current.attempt_count == 0
        || current.attempt_count > current.attempt_limit
    {
        return Err(JobError::InvalidTransition);
    }
    transition(current, JobState::Running, true)
}

pub fn decide_heartbeat(
    current: &JobProjection,
    fence: &JobFence,
    database_now: DateTime<Utc>,
    policy: LeasePolicy,
) -> Result<JobProjection, JobError> {
    validate_fence(current, fence, database_now)?;
    if !matches!(current.state, JobState::Leased | JobState::Running) {
        return Err(JobError::InvalidTransition);
    }
    let expires_at = database_now
        .checked_add_signed(policy.duration()?)
        .ok_or(JobError::CounterOverflow)?
        .min(current.deadline);
    if expires_at <= database_now {
        return Err(JobError::LeaseExpired);
    }
    let mut next = current.clone();
    next.version = increment(next.version)?;
    let lease = next.lease.as_mut().ok_or(JobError::InvalidProjection)?;
    lease.heartbeat_at = database_now;
    lease.expires_at = expires_at;
    next.validate()?;
    Ok(next)
}

/// Advances the optimistic version for a fenced, non-state-changing owner observation.
///
/// This is used for bounded durable progress or other typed Job payload evidence. It deliberately
/// does not renew the lease or alter scheduling/deadline state.
pub fn decide_observation_update(
    current: &JobProjection,
    fence: &JobFence,
    database_now: DateTime<Utc>,
) -> Result<JobProjection, JobError> {
    validate_fence(current, fence, database_now)?;
    if current.state != JobState::Running {
        return Err(JobError::InvalidTransition);
    }
    let mut next = current.clone();
    next.version = increment(next.version)?;
    next.validate()?;
    Ok(next)
}

pub fn decide_terminal(
    current: &JobProjection,
    fence: &JobFence,
    database_now: DateTime<Utc>,
    terminal: JobState,
) -> Result<JobProjection, JobError> {
    validate_fence(current, fence, database_now)?;
    if !is_terminal(terminal) || !current.state.can_transition_to(terminal) {
        return Err(JobError::InvalidTransition);
    }
    transition(current, terminal, false)
}

/// Advances only the optimistic Job version for terminal physical cleanup evidence.
///
/// A terminal Job no longer has a business lease. The owning repository must separately validate
/// its closed cleanup-generation fence before using this decision; this function deliberately
/// cannot change state, result, scheduling, or lease fields.
pub fn decide_terminal_physical_update(
    current: &JobProjection,
    expected_version: u64,
) -> Result<JobProjection, JobError> {
    current.validate()?;
    if current.version != expected_version
        || current.lease.is_some()
        || !matches!(
            current.state,
            JobState::Succeeded
                | JobState::Failed
                | JobState::Cancelled
                | JobState::TimedOut
                | JobState::ReconciliationRequired
        )
    {
        return Err(JobError::StaleFence);
    }
    let mut next = current.clone();
    next.version = increment(next.version)?;
    next.validate()?;
    Ok(next)
}

/// Commits a terminal result during an explicitly bounded post-deadline cleanup window.
///
/// The worker identity, lease generation, token and optimistic version remain exact. Only the
/// ordinary lease-expiry time check is replaced by `cleanup_deadline`, allowing an executor to
/// terminate a workload and persist cleanup evidence after the execution deadline without opening
/// an unfenced owner-control path.
pub fn decide_cleanup_terminal(
    current: &JobProjection,
    fence: &JobFence,
    database_now: DateTime<Utc>,
    cleanup_deadline: DateTime<Utc>,
    terminal: JobState,
) -> Result<JobProjection, JobError> {
    validate_cleanup_fence(current, fence, database_now, cleanup_deadline)?;
    if !is_terminal(terminal) || !current.state.can_transition_to(terminal) {
        return Err(JobError::InvalidTransition);
    }
    transition(current, terminal, false)
}

pub fn decide_reconciliation(
    current: &JobProjection,
    fence: &JobFence,
    database_now: DateTime<Utc>,
) -> Result<JobProjection, JobError> {
    validate_fence(current, fence, database_now)?;
    if !matches!(current.state, JobState::Running | JobState::Cancelling)
        || !current
            .state
            .can_transition_to(JobState::ReconciliationRequired)
    {
        return Err(JobError::InvalidTransition);
    }
    transition(current, JobState::ReconciliationRequired, false)
}

pub fn decide_cleanup_reconciliation(
    current: &JobProjection,
    fence: &JobFence,
    database_now: DateTime<Utc>,
    cleanup_deadline: DateTime<Utc>,
) -> Result<JobProjection, JobError> {
    validate_cleanup_fence(current, fence, database_now, cleanup_deadline)?;
    if !matches!(current.state, JobState::Running | JobState::Cancelling)
        || !current
            .state
            .can_transition_to(JobState::ReconciliationRequired)
    {
        return Err(JobError::InvalidTransition);
    }
    transition(current, JobState::ReconciliationRequired, false)
}

/// Applies a terminal decision made by the logical owner's recovery/control authority.
///
/// Unlike a Worker terminal commit, this path is deliberately not lease-fenced: the owner has
/// already won the parent aggregate's cancel, timeout, or recovery transition. Repositories must
/// lock and revalidate that parent authority before persisting this decision.
pub fn decide_owner_terminal(
    current: &JobProjection,
    terminal: JobState,
) -> Result<JobProjection, JobError> {
    current.validate()?;
    if !is_terminal(terminal) || !current.state.can_transition_to(terminal) {
        return Err(JobError::InvalidRecoveryDecision);
    }
    transition(current, terminal, false)
}

pub fn decide_owner_reconciliation(current: &JobProjection) -> Result<JobProjection, JobError> {
    current.validate()?;
    if !current
        .state
        .can_transition_to(JobState::ReconciliationRequired)
    {
        return Err(JobError::InvalidRecoveryDecision);
    }
    transition(current, JobState::ReconciliationRequired, false)
}

pub fn decide_owner_cancelling(current: &JobProjection) -> Result<JobProjection, JobError> {
    current.validate()?;
    if !current.state.can_transition_to(JobState::Cancelling) {
        return Err(JobError::InvalidRecoveryDecision);
    }
    let retain_lease = current.lease.is_some();
    transition(current, JobState::Cancelling, retain_lease)
}

pub fn decide_wait(
    current: &JobProjection,
    fence: &JobFence,
    database_now: DateTime<Utc>,
    wake: WakeContract,
) -> Result<JobProjection, JobError> {
    validate_fence(current, fence, database_now)?;
    wake.validate(current.deadline)?;
    if current.state != JobState::Running || wake.generation != current.lease_generation {
        return Err(JobError::InvalidTransition);
    }
    if !current.state.can_transition_to(JobState::Waiting) {
        return Err(JobError::InvalidTransition);
    }
    let mut next = current.clone();
    next.state = JobState::Waiting;
    next.version = increment(next.version)?;
    next.lease = None;
    next.retry_at = None;
    next.wake = Some(wake);
    next.validate()?;
    Ok(next)
}

pub fn decide_retry(
    current: &JobProjection,
    fence: &JobFence,
    database_now: DateTime<Utc>,
    retry_at: DateTime<Utc>,
) -> Result<JobProjection, JobError> {
    validate_fence(current, fence, database_now)?;
    if current.state != JobState::Running
        || current.attempt_count >= current.attempt_limit
        || retry_at <= database_now
        || retry_at >= current.deadline
    {
        return Err(JobError::InvalidRetry);
    }
    if !current.state.can_transition_to(JobState::RetryScheduled) {
        return Err(JobError::InvalidTransition);
    }
    let mut next = current.clone();
    next.state = JobState::RetryScheduled;
    next.version = increment(next.version)?;
    next.lease = None;
    next.wake = None;
    next.retry_at = Some(retry_at);
    next.validate()?;
    Ok(next)
}

pub fn decide_retry_due(
    current: &JobProjection,
    database_now: DateTime<Utc>,
) -> Result<JobProjection, JobError> {
    current.validate()?;
    if current.state != JobState::RetryScheduled
        || current
            .retry_at
            .is_none_or(|retry_at| retry_at > database_now)
        || database_now >= current.deadline
    {
        return Err(JobError::InvalidRetry);
    }
    let mut next = transition(current, JobState::Ready, false)?;
    next.scheduled_at = database_now;
    next.validate()?;
    Ok(next)
}

pub fn decide_wake(
    current: &JobProjection,
    expected_generation: u64,
    source: WakeSource,
    database_now: DateTime<Utc>,
) -> Result<JobProjection, JobError> {
    current.validate()?;
    let wake = current.wake.as_ref().ok_or(JobError::InvalidWakeContract)?;
    let outside_source_window = if source == WakeSource::Timeout {
        database_now < wake.deadline
    } else {
        database_now >= wake.deadline
    };
    if current.state != JobState::Waiting
        || wake.generation != expected_generation
        || !wake.accepted_sources.contains(&source)
        || outside_source_window
        || (source == WakeSource::Poll
            && (wake.poll_count >= wake.poll_limit
                || wake
                    .next_poll_at
                    .is_some_and(|next_poll_at| database_now < next_poll_at)))
    {
        return Err(JobError::WakeLostFirstWinner);
    }
    let mut next = transition(current, JobState::Ready, false)?;
    next.wake = None;
    next.scheduled_at = database_now;
    next.validate()?;
    Ok(next)
}

pub fn decide_expired_lease(
    current: &JobProjection,
    observed_version: u64,
    observed_lease_generation: u64,
    database_now: DateTime<Utc>,
    target: JobState,
    retry_at: Option<DateTime<Utc>>,
) -> Result<JobProjection, JobError> {
    current.validate()?;
    let lease = current.lease.as_ref().ok_or(JobError::InvalidLease)?;
    if current.version != observed_version
        || lease.lease_generation != observed_lease_generation
        || database_now < lease.expires_at
        || !matches!(
            current.state,
            JobState::Leased | JobState::Running | JobState::Cancelling
        )
    {
        return Err(JobError::StaleFence);
    }
    match target {
        JobState::Ready if current.state == JobState::Leased && database_now < current.deadline => {
            let mut next = transition(current, JobState::Ready, false)?;
            next.scheduled_at = database_now;
            next.validate()?;
            Ok(next)
        }
        JobState::RetryScheduled
            if current.state == JobState::Running
                && current.attempt_count < current.attempt_limit
                && retry_at
                    .is_some_and(|value| value > database_now && value < current.deadline) =>
        {
            let mut next = current.clone();
            next.state = JobState::RetryScheduled;
            next.version = increment(next.version)?;
            next.lease = None;
            next.wake = None;
            next.retry_at = retry_at;
            next.validate()?;
            Ok(next)
        }
        JobState::Failed
            if current.state == JobState::Running
                && current.attempt_count >= current.attempt_limit
                && database_now < current.deadline =>
        {
            transition(current, JobState::Failed, false)
        }
        JobState::ReconciliationRequired
            if current
                .state
                .can_transition_to(JobState::ReconciliationRequired) =>
        {
            transition(current, JobState::ReconciliationRequired, false)
        }
        JobState::TimedOut if database_now >= current.deadline => {
            transition(current, JobState::TimedOut, false)
        }
        _ => Err(JobError::InvalidRecoveryDecision),
    }
}

/// Releases an expired lease for a durable external continuation without consuming a new attempt.
///
/// The owner payload must prove that the same physical attempt can only be observed/replayed and
/// cannot be redispatched before a repository calls this decision.
pub fn decide_expired_continuation(
    current: &JobProjection,
    observed_version: u64,
    observed_lease_generation: u64,
    database_now: DateTime<Utc>,
) -> Result<JobProjection, JobError> {
    current.validate()?;
    let lease = current.lease.as_ref().ok_or(JobError::InvalidLease)?;
    if current.version != observed_version
        || lease.lease_generation != observed_lease_generation
        || database_now < lease.expires_at
        || current.state != JobState::Running
        || current.attempt_count == 0
        || current.attempt_count > current.attempt_limit
        || database_now >= current.deadline
    {
        return Err(JobError::StaleFence);
    }
    let mut next = transition(current, JobState::Ready, false)?;
    next.scheduled_at = database_now;
    next.validate()?;
    Ok(next)
}

fn validate_fence(
    current: &JobProjection,
    fence: &JobFence,
    database_now: DateTime<Utc>,
) -> Result<(), JobError> {
    current.validate()?;
    let lease = current.lease.as_ref().ok_or(JobError::StaleFence)?;
    if current.version != fence.expected_version
        || lease.worker_process_generation_id != fence.worker_process_generation_id
        || lease.lease_generation != fence.lease_generation
        || lease.token_digest != fence.token_digest
    {
        return Err(JobError::StaleFence);
    }
    if database_now >= lease.expires_at {
        return Err(JobError::LeaseExpired);
    }
    Ok(())
}

fn validate_cleanup_fence(
    current: &JobProjection,
    fence: &JobFence,
    database_now: DateTime<Utc>,
    cleanup_deadline: DateTime<Utc>,
) -> Result<(), JobError> {
    current.validate()?;
    let lease = current.lease.as_ref().ok_or(JobError::StaleFence)?;
    if current.version != fence.expected_version
        || lease.worker_process_generation_id != fence.worker_process_generation_id
        || lease.lease_generation != fence.lease_generation
        || lease.token_digest != fence.token_digest
    {
        return Err(JobError::StaleFence);
    }
    if cleanup_deadline < current.deadline || database_now > cleanup_deadline {
        return Err(JobError::LeaseExpired);
    }
    Ok(())
}

fn transition(
    current: &JobProjection,
    target: JobState,
    retain_lease: bool,
) -> Result<JobProjection, JobError> {
    if !current.state.can_transition_to(target) {
        return Err(JobError::InvalidTransition);
    }
    let mut next = current.clone();
    next.state = target;
    next.version = increment(next.version)?;
    if !retain_lease {
        next.lease = None;
    }
    next.retry_at = None;
    next.wake = None;
    next.validate()?;
    Ok(next)
}

const fn is_terminal(state: JobState) -> bool {
    matches!(
        state,
        JobState::Succeeded | JobState::Failed | JobState::Cancelled | JobState::TimedOut
    )
}

fn increment(value: u64) -> Result<u64, JobError> {
    value.checked_add(1).ok_or(JobError::CounterOverflow)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobError {
    InvalidOwner,
    InvalidProjection,
    InvalidLease,
    InvalidLeasePolicy,
    InvalidWakeContract,
    InvalidTransition,
    InvalidRetry,
    InvalidRecoveryDecision,
    NotClaimable,
    StaleFence,
    LeaseExpired,
    WakeLostFirstWinner,
    CounterOverflow,
}

impl fmt::Display for JobError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidOwner => "Job owner/work-class pair is invalid",
            Self::InvalidProjection => "Job projection is invalid",
            Self::InvalidLease => "Job lease is invalid",
            Self::InvalidLeasePolicy => "Job lease policy is invalid",
            Self::InvalidWakeContract => "Job wake contract is invalid",
            Self::InvalidTransition => "Job state transition is invalid",
            Self::InvalidRetry => "Job retry schedule is invalid",
            Self::InvalidRecoveryDecision => "expired Job recovery decision is invalid",
            Self::NotClaimable => "Job is not claimable",
            Self::StaleFence => "Job lease fence is stale",
            Self::LeaseExpired => "Job lease has expired",
            Self::WakeLostFirstWinner => "Job wake lost the first-winner race",
            Self::CounterOverflow => "Job counter overflowed",
        })
    }
}

impl Error for JobError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(prefix: &str, suffix: &str) -> ResourceId {
        format!("{prefix}_0198f1c5-0787-75e1-a9e8-d95ca0f3{suffix}")
            .parse()
            .unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn ready(now: DateTime<Utc>) -> JobProjection {
        JobProjection {
            trace: insight_platform_contracts::TraceIdentityV1::generate(),
            tenant_id: id("ten", "6001"),
            job_id: id("job", "6001"),
            work_class: WorkClass::Orchestration,
            owner: JobOwnerRef {
                owner_id: id("nod", "6001"),
                owner_kind: ResourceKind::NodeExecution,
            },
            state: JobState::Ready,
            version: 1,
            attempt_count: 0,
            attempt_limit: 3,
            lease_generation: 0,
            lease: None,
            scheduled_at: now,
            retry_at: None,
            wake: None,
            deadline: now + Duration::minutes(5),
        }
    }

    fn claim(now: DateTime<Utc>) -> (JobProjection, JobFence) {
        let claimed = decide_claim(
            &ready(now),
            now,
            id("wrk", "6001"),
            digest('a'),
            LeasePolicy {
                requested_milliseconds: 30_000,
                hard_maximum_milliseconds: 60_000,
            },
        )
        .unwrap();
        let fence = JobFence {
            expected_version: claimed.version,
            worker_process_generation_id: id("wrk", "6001"),
            lease_generation: claimed.lease_generation,
            token_digest: digest('a'),
        };
        (claimed, fence)
    }

    #[test]
    fn lease_fence_checks_version_generation_worker_token_and_database_time() {
        let now = DateTime::parse_from_rfc3339("2026-08-09T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let (claimed, fence) = claim(now);
        assert_eq!(claimed.attempt_count, 0);
        let running = decide_start(&claimed, &fence, now + Duration::seconds(1)).unwrap();
        assert_eq!(running.attempt_count, 1);
        let mut stale = JobFence {
            expected_version: running.version,
            ..fence
        };
        stale.token_digest = digest('b');
        assert_eq!(
            decide_heartbeat(
                &running,
                &stale,
                now + Duration::seconds(2),
                LeasePolicy {
                    requested_milliseconds: 30_000,
                    hard_maximum_milliseconds: 60_000,
                }
            ),
            Err(JobError::StaleFence)
        );
    }

    #[test]
    fn cleanup_terminal_keeps_exact_fence_during_bounded_post_deadline_grace() {
        let now = DateTime::parse_from_rfc3339("2026-08-09T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let (claimed, fence) = claim(now);
        let running = decide_start(&claimed, &fence, now + Duration::seconds(1)).unwrap();
        let running_fence = JobFence {
            expected_version: running.version,
            ..fence
        };
        let cleanup_deadline = running.deadline + Duration::seconds(10);
        let terminal = decide_cleanup_terminal(
            &running,
            &running_fence,
            running.deadline + Duration::seconds(1),
            cleanup_deadline,
            JobState::TimedOut,
        )
        .unwrap();
        assert_eq!(terminal.state, JobState::TimedOut);
        assert!(terminal.lease.is_none());

        let mut stale = running_fence.clone();
        stale.token_digest = digest('b');
        assert_eq!(
            decide_cleanup_terminal(
                &running,
                &stale,
                running.deadline + Duration::seconds(1),
                cleanup_deadline,
                JobState::TimedOut,
            ),
            Err(JobError::StaleFence)
        );
        assert_eq!(
            decide_cleanup_terminal(
                &running,
                &running_fence,
                cleanup_deadline + Duration::milliseconds(1),
                cleanup_deadline,
                JobState::TimedOut,
            ),
            Err(JobError::LeaseExpired)
        );
    }

    #[test]
    fn terminal_physical_update_only_advances_version() {
        let now = DateTime::parse_from_rfc3339("2026-08-09T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let (claimed, fence) = claim(now);
        let running = decide_start(&claimed, &fence, now + Duration::seconds(1)).unwrap();
        let running_fence = JobFence {
            expected_version: running.version,
            ..fence
        };
        let terminal = decide_terminal(
            &running,
            &running_fence,
            now + Duration::seconds(2),
            JobState::Succeeded,
        )
        .unwrap();
        let updated = decide_terminal_physical_update(&terminal, terminal.version).unwrap();
        assert_eq!(updated.state, terminal.state);
        assert_eq!(updated.version, terminal.version + 1);
        assert_eq!(updated.attempt_count, terminal.attempt_count);
        assert_eq!(updated.lease_generation, terminal.lease_generation);
        assert_eq!(
            decide_terminal_physical_update(&updated, terminal.version),
            Err(JobError::StaleFence)
        );
    }

    #[test]
    fn wait_and_wake_are_generation_first_winner() {
        let now = DateTime::parse_from_rfc3339("2026-08-09T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let (claimed, fence) = claim(now);
        let running = decide_start(&claimed, &fence, now + Duration::seconds(1)).unwrap();
        let running_fence = JobFence {
            expected_version: running.version,
            ..fence
        };
        let waiting = decide_wait(
            &running,
            &running_fence,
            now + Duration::seconds(2),
            WakeContract {
                kind: WakeKind::RemoteInvocation,
                generation: running.lease_generation,
                accepted_sources: vec![WakeSource::Callback, WakeSource::Poll],
                expected_response_schema_digest: Some(digest('c')),
                opaque_state_digest: Some(digest('d')),
                next_poll_at: Some(now + Duration::seconds(10)),
                poll_count: 0,
                poll_limit: 3,
                callback_binding_digest: Some(digest('e')),
                deadline: now + Duration::minutes(2),
            },
        )
        .unwrap();
        let ready = decide_wake(
            &waiting,
            waiting.lease_generation,
            WakeSource::Callback,
            now + Duration::seconds(3),
        )
        .unwrap();
        assert_eq!(ready.state, JobState::Ready);
        assert_eq!(
            decide_wake(
                &ready,
                waiting.lease_generation,
                WakeSource::Poll,
                now + Duration::seconds(4)
            ),
            Err(JobError::InvalidWakeContract)
        );
        let mut at_limit = waiting.clone();
        at_limit.attempt_count = at_limit.attempt_limit;
        let continuation_ready = decide_wake(
            &at_limit,
            at_limit.lease_generation,
            WakeSource::Callback,
            now + Duration::seconds(3),
        )
        .unwrap();
        assert_eq!(
            decide_claim(
                &continuation_ready,
                now + Duration::seconds(4),
                id("wrk", "6002"),
                digest('f'),
                LeasePolicy {
                    requested_milliseconds: 30_000,
                    hard_maximum_milliseconds: 60_000,
                },
            ),
            Err(JobError::NotClaimable)
        );
        let continuation = decide_claim_continuation(
            &continuation_ready,
            now + Duration::seconds(4),
            id("wrk", "6002"),
            digest('f'),
            LeasePolicy {
                requested_milliseconds: 30_000,
                hard_maximum_milliseconds: 60_000,
            },
        )
        .unwrap();
        let resumed = decide_resume(
            &continuation,
            &JobFence {
                expected_version: continuation.version,
                worker_process_generation_id: id("wrk", "6002"),
                lease_generation: continuation.lease_generation,
                token_digest: digest('f'),
            },
            now + Duration::seconds(5),
        )
        .unwrap();
        assert_eq!(resumed.attempt_count, resumed.attempt_limit);

        let mut poll_exhausted = waiting.clone();
        poll_exhausted.wake.as_mut().unwrap().poll_count = 3;
        assert_eq!(
            decide_wake(
                &poll_exhausted,
                poll_exhausted.lease_generation,
                WakeSource::Poll,
                now + Duration::seconds(10),
            ),
            Err(JobError::WakeLostFirstWinner)
        );

        let mut timeout_waiting = waiting;
        timeout_waiting.wake.as_mut().unwrap().accepted_sources =
            vec![WakeSource::Callback, WakeSource::Poll, WakeSource::Timeout];
        assert_eq!(
            decide_wake(
                &timeout_waiting,
                timeout_waiting.lease_generation,
                WakeSource::Timeout,
                now + Duration::seconds(30),
            ),
            Err(JobError::WakeLostFirstWinner)
        );
        assert_eq!(
            decide_wake(
                &timeout_waiting,
                timeout_waiting.lease_generation,
                WakeSource::Timeout,
                now + Duration::minutes(2),
            )
            .unwrap()
            .state,
            JobState::Ready
        );
    }

    #[test]
    fn retry_becomes_ready_only_after_the_persisted_database_deadline() {
        let now = DateTime::parse_from_rfc3339("2026-08-09T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let (claimed, fence) = claim(now);
        let running = decide_start(&claimed, &fence, now + Duration::seconds(1)).unwrap();
        let retry_at = now + Duration::seconds(10);
        let retry = decide_retry(
            &running,
            &JobFence {
                expected_version: running.version,
                ..fence
            },
            now + Duration::seconds(2),
            retry_at,
        )
        .unwrap();
        assert_eq!(retry.state, JobState::RetryScheduled);
        assert_eq!(
            decide_retry_due(&retry, retry_at - Duration::milliseconds(1)),
            Err(JobError::InvalidRetry)
        );
        let ready = decide_retry_due(&retry, retry_at).unwrap();
        assert_eq!(ready.state, JobState::Ready);
        assert_eq!(ready.scheduled_at, retry_at);
        assert!(ready.retry_at.is_none());
        let second_worker = id("wrk", "6003");
        let second_token = digest('7');
        let second_claim = decide_claim(
            &ready,
            retry_at,
            second_worker.clone(),
            second_token.clone(),
            LeasePolicy {
                requested_milliseconds: 30_000,
                hard_maximum_milliseconds: 60_000,
            },
        )
        .unwrap();
        let second_started = decide_start(
            &second_claim,
            &JobFence {
                expected_version: second_claim.version,
                worker_process_generation_id: second_worker,
                lease_generation: second_claim.lease_generation,
                token_digest: second_token,
            },
            retry_at + Duration::seconds(1),
        )
        .unwrap();
        assert_eq!(second_started.attempt_count, 2);
    }

    #[test]
    fn expired_running_effect_requires_an_explicit_safe_target() {
        let now = DateTime::parse_from_rfc3339("2026-08-09T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let (claimed, fence) = claim(now);
        let running = decide_start(&claimed, &fence, now + Duration::seconds(1)).unwrap();
        let expired_at = running.lease.as_ref().unwrap().expires_at;
        assert_eq!(
            decide_expired_lease(
                &running,
                running.version,
                running.lease_generation,
                expired_at,
                JobState::Ready,
                None
            ),
            Err(JobError::InvalidRecoveryDecision)
        );
        let reconciliation = decide_expired_lease(
            &running,
            running.version,
            running.lease_generation,
            expired_at,
            JobState::ReconciliationRequired,
            None,
        )
        .unwrap();
        assert_eq!(reconciliation.state, JobState::ReconciliationRequired);
        assert!(reconciliation.lease.is_none());

        let cancelled = decide_owner_terminal(&running, JobState::Cancelled).unwrap();
        assert_eq!(cancelled.state, JobState::Cancelled);
        assert!(cancelled.lease.is_none());
    }

    #[test]
    fn expired_external_continuation_reclaims_the_same_attempt() {
        let now = DateTime::parse_from_rfc3339("2026-08-09T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let (claimed, fence) = claim(now);
        let running = decide_start(&claimed, &fence, now + Duration::seconds(1)).unwrap();
        let expired_at = running.lease.as_ref().unwrap().expires_at;

        let ready = decide_expired_continuation(
            &running,
            running.version,
            running.lease_generation,
            expired_at,
        )
        .unwrap();
        assert_eq!(ready.state, JobState::Ready);
        assert_eq!(ready.attempt_count, running.attempt_count);
        assert!(ready.lease.is_none());

        let reclaimed = decide_claim_continuation(
            &ready,
            expired_at,
            id("wrk", "6004"),
            digest('8'),
            LeasePolicy {
                requested_milliseconds: 30_000,
                hard_maximum_milliseconds: 60_000,
            },
        )
        .unwrap();
        assert_eq!(reclaimed.attempt_count, running.attempt_count);
        assert_eq!(reclaimed.lease_generation, running.lease_generation + 1);
        let resumed = decide_resume(
            &reclaimed,
            &JobFence {
                expected_version: reclaimed.version,
                worker_process_generation_id: reclaimed
                    .lease
                    .as_ref()
                    .unwrap()
                    .worker_process_generation_id
                    .clone(),
                lease_generation: reclaimed.lease_generation,
                token_digest: reclaimed.lease.as_ref().unwrap().token_digest.clone(),
            },
            expired_at,
        )
        .unwrap();
        assert_eq!(resumed.state, JobState::Running);
        assert_eq!(resumed.attempt_count, running.attempt_count);
        assert_eq!(resumed.lease_generation, running.lease_generation + 1);

        assert_eq!(
            decide_expired_continuation(
                &running,
                running.version,
                running.lease_generation,
                expired_at - Duration::milliseconds(1),
            ),
            Err(JobError::StaleFence)
        );
    }

    #[test]
    fn expired_running_job_at_attempt_limit_can_fail_closed() {
        let now = DateTime::parse_from_rfc3339("2026-08-09T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let (claimed, fence) = claim(now);
        let mut running = decide_start(&claimed, &fence, now + Duration::seconds(1)).unwrap();
        running.attempt_count = running.attempt_limit;
        running.validate().unwrap();
        let expired_at = running.lease.as_ref().unwrap().expires_at;
        let failed = decide_expired_lease(
            &running,
            running.version,
            running.lease_generation,
            expired_at,
            JobState::Failed,
            None,
        )
        .unwrap();
        assert_eq!(failed.state, JobState::Failed);
        assert!(failed.lease.is_none());
    }
}
