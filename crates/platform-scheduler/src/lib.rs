//! Deterministic tenant-first scheduling decisions for the shared Job authority.
//!
//! Candidate enumeration remains a repository responsibility. This crate consumes a bounded,
//! trusted window and returns a pure WDRR decision plus the exact next scheduler-state snapshot.

use insight_platform_contracts::{
    canonical_digest, ResourceId, ResourceKind, SchedulerPriority, Sha256Digest, WorkClass,
};
use serde::{Deserialize, Serialize};
use std::{cmp::Reverse, collections::BTreeSet, error::Error, fmt};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerLane {
    Business,
    CriticalControl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerHardLimits {
    pub maximum_deficit: u64,
    pub maximum_tenants: u16,
    pub maximum_state_tenants: u16,
    pub maximum_window_per_tenant: u16,
    pub maximum_batch: u16,
}

impl SchedulerHardLimits {
    pub fn validate(&self) -> Result<(), ScheduleError> {
        if self.maximum_deficit == 0
            || self.maximum_tenants == 0
            || self.maximum_state_tenants < self.maximum_tenants
            || self.maximum_window_per_tenant == 0
            || self.maximum_batch == 0
        {
            return Err(ScheduleError::InvalidPolicy);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantSchedulingPolicyBinding {
    pub tenant_id: ResourceId,
    pub policy_version_id: ResourceId,
    pub policy_version_digest: Sha256Digest,
    pub rules_digest: Sha256Digest,
    pub weight: u16,
    pub burst: u16,
    pub aging_rounds: u16,
}

impl TenantSchedulingPolicyBinding {
    pub fn validate(&self) -> Result<(), ScheduleError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.policy_version_id.kind() != ResourceKind::PolicyRevision
            || self.weight == 0
            || self.burst == 0
            || self.aging_rounds == 0
        {
            return Err(ScheduleError::InvalidPolicy);
        }
        let document = serde_json::json!({
            "version": 1,
            "weight": self.weight,
            "burst": self.burst,
            "aging_rounds": self.aging_rounds,
        });
        let digest: Sha256Digest = canonical_digest(&document)
            .map_err(|_| ScheduleError::Canonicalization)?
            .parse()
            .map_err(|_| ScheduleError::Canonicalization)?;
        if digest != self.rules_digest {
            return Err(ScheduleError::PolicyDigestMismatch);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadyWork {
    pub job_id: ResourceId,
    pub priority: SchedulerPriority,
    pub lane: SchedulerLane,
    pub enqueue_round: u64,
    pub cost: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantWindow {
    pub tenant_id: ResourceId,
    pub work: Vec<ReadyWork>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantDeficitState {
    pub tenant_id: ResourceId,
    pub policy_version_id: ResourceId,
    pub policy_version_digest: Sha256Digest,
    pub rules_digest: Sha256Digest,
    pub deficit: u64,
    pub last_served_round: Option<u64>,
    pub successful_claims: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulerStateSnapshot {
    pub work_class: WorkClass,
    pub current_round: u64,
    pub cursor_tenant_id: Option<ResourceId>,
    pub tenants: Vec<TenantDeficitState>,
}

impl SchedulerStateSnapshot {
    pub fn canonical_digest(&self) -> Result<Sha256Digest, ScheduleError> {
        let value = serde_json::to_value(self).map_err(|_| ScheduleError::Canonicalization)?;
        canonical_digest(&value)
            .map_err(|_| ScheduleError::Canonicalization)?
            .parse()
            .map_err(|_| ScheduleError::Canonicalization)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleDecision {
    pub selected_job_ids: Vec<ResourceId>,
    pub next_state: SchedulerStateSnapshot,
}

pub fn select_weighted_deficit_round_robin(
    state: &SchedulerStateSnapshot,
    limits: &SchedulerHardLimits,
    policy_bindings: &[TenantSchedulingPolicyBinding],
    windows: &[TenantWindow],
    requested_batch: u16,
) -> Result<ScheduleDecision, ScheduleError> {
    limits.validate()?;
    validate_state(state, limits)?;
    if requested_batch == 0 || requested_batch > limits.maximum_batch {
        return Err(ScheduleError::InvalidBatch);
    }
    validate_policy_bindings(limits, policy_bindings, windows)?;
    validate_windows(state.current_round, limits, windows)?;

    let next_round = state
        .current_round
        .checked_add(1)
        .ok_or(ScheduleError::CounterOverflow)?;
    let mut tenant_states = state.tenants.clone();
    tenant_states.sort_by(|left, right| left.tenant_id.cmp(&right.tenant_id));
    let mut windows = windows.to_vec();
    windows.sort_by(|left, right| left.tenant_id.cmp(&right.tenant_id));
    for window in &windows {
        let policy = policy_bindings
            .iter()
            .find(|binding| binding.tenant_id == window.tenant_id)
            .ok_or(ScheduleError::InvalidPolicy)?;
        if tenant_states
            .binary_search_by(|candidate| candidate.tenant_id.cmp(&window.tenant_id))
            .is_err()
        {
            tenant_states.push(TenantDeficitState {
                tenant_id: window.tenant_id.clone(),
                policy_version_id: policy.policy_version_id.clone(),
                policy_version_digest: policy.policy_version_digest.clone(),
                rules_digest: policy.rules_digest.clone(),
                deficit: 0,
                last_served_round: None,
                successful_claims: 0,
            });
            tenant_states.sort_by(|left, right| left.tenant_id.cmp(&right.tenant_id));
        }
        let tenant = tenant_states
            .iter_mut()
            .find(|candidate| candidate.tenant_id == window.tenant_id)
            .ok_or(ScheduleError::InvalidState)?;
        if tenant.policy_version_id == policy.policy_version_id
            && (tenant.policy_version_digest != policy.policy_version_digest
                || tenant.rules_digest != policy.rules_digest)
        {
            return Err(ScheduleError::PolicyDigestMismatch);
        }
        if tenant.policy_version_id != policy.policy_version_id {
            tenant.deficit = 0;
            tenant.policy_version_id = policy.policy_version_id.clone();
            tenant.policy_version_digest = policy.policy_version_digest.clone();
            tenant.rules_digest = policy.rules_digest.clone();
        }
    }
    prune_inactive_tenant_state(&mut tenant_states, &windows, limits.maximum_state_tenants)?;

    let start = state
        .cursor_tenant_id
        .as_ref()
        .and_then(|cursor| windows.iter().position(|window| &window.tenant_id > cursor))
        .unwrap_or(0);
    let mut selected = Vec::with_capacity(usize::from(requested_batch));
    let mut last_visited = state.cursor_tenant_id.clone();
    for offset in 0..windows.len() {
        if selected.len() >= usize::from(requested_batch) {
            break;
        }
        let window = &windows[(start + offset) % windows.len()];
        let policy = policy_bindings
            .iter()
            .find(|binding| binding.tenant_id == window.tenant_id)
            .ok_or(ScheduleError::InvalidPolicy)?;
        last_visited = Some(window.tenant_id.clone());
        let tenant = tenant_states
            .iter_mut()
            .find(|candidate| candidate.tenant_id == window.tenant_id)
            .ok_or(ScheduleError::InvalidState)?;
        tenant.deficit = tenant
            .deficit
            .checked_add(u64::from(policy.weight))
            .ok_or(ScheduleError::CounterOverflow)?
            .min(limits.maximum_deficit);

        let mut work = window.work.clone();
        work.sort_by_key(|candidate| {
            (
                Reverse(effective_priority(
                    candidate,
                    state.current_round,
                    u64::from(policy.aging_rounds),
                )),
                candidate.enqueue_round,
                candidate.job_id.clone(),
            )
        });
        let mut selected_for_tenant = 0_u16;
        for candidate in work {
            if selected.len() >= usize::from(requested_batch)
                || selected_for_tenant >= policy.burst
                || u64::from(candidate.cost) > tenant.deficit
            {
                continue;
            }
            tenant.deficit -= u64::from(candidate.cost);
            tenant.last_served_round = Some(next_round);
            tenant.successful_claims = tenant
                .successful_claims
                .checked_add(1)
                .ok_or(ScheduleError::CounterOverflow)?;
            selected.push(candidate.job_id);
            selected_for_tenant += 1;
        }
    }

    Ok(ScheduleDecision {
        selected_job_ids: selected,
        next_state: SchedulerStateSnapshot {
            work_class: state.work_class,
            current_round: next_round,
            cursor_tenant_id: last_visited,
            tenants: tenant_states,
        },
    })
}

fn effective_priority(candidate: &ReadyWork, current_round: u64, aging_rounds: u64) -> u8 {
    if candidate.lane == SchedulerLane::CriticalControl {
        return 3;
    }
    let base: u64 = match candidate.priority {
        SchedulerPriority::Low => 0,
        SchedulerPriority::Normal => 1,
        SchedulerPriority::High => 2,
        SchedulerPriority::CriticalControl => 3,
    };
    let promotions = current_round
        .saturating_sub(candidate.enqueue_round)
        .checked_div(aging_rounds)
        .unwrap_or(0)
        .min(2);
    u8::try_from((base + promotions).min(2)).unwrap_or(2)
}

fn validate_state(
    state: &SchedulerStateSnapshot,
    limits: &SchedulerHardLimits,
) -> Result<(), ScheduleError> {
    if state
        .cursor_tenant_id
        .as_ref()
        .is_some_and(|tenant| tenant.kind() != ResourceKind::Tenant)
        || state.tenants.len() > usize::from(limits.maximum_state_tenants)
    {
        return Err(ScheduleError::InvalidState);
    }
    let mut tenants = BTreeSet::new();
    for tenant in &state.tenants {
        if tenant.tenant_id.kind() != ResourceKind::Tenant
            || tenant.policy_version_id.kind() != ResourceKind::PolicyRevision
            || tenant.deficit > limits.maximum_deficit
            || tenant
                .last_served_round
                .is_some_and(|round| round > state.current_round)
            || !tenants.insert(tenant.tenant_id.clone())
        {
            return Err(ScheduleError::InvalidState);
        }
    }
    Ok(())
}

fn prune_inactive_tenant_state(
    tenant_states: &mut Vec<TenantDeficitState>,
    windows: &[TenantWindow],
    maximum_state_tenants: u16,
) -> Result<(), ScheduleError> {
    let maximum = usize::from(maximum_state_tenants);
    if tenant_states.len() <= maximum {
        return Ok(());
    }
    let active = windows
        .iter()
        .map(|window| window.tenant_id.clone())
        .collect::<BTreeSet<_>>();
    if active.len() > maximum {
        return Err(ScheduleError::InvalidState);
    }
    let mut removable = tenant_states
        .iter()
        .filter(|tenant| !active.contains(&tenant.tenant_id))
        .map(|tenant| {
            (
                tenant.last_served_round.unwrap_or(0),
                tenant.successful_claims,
                tenant.tenant_id.clone(),
            )
        })
        .collect::<Vec<_>>();
    removable.sort();
    let remove = tenant_states.len() - maximum;
    let evicted = removable
        .into_iter()
        .take(remove)
        .map(|(_, _, tenant_id)| tenant_id)
        .collect::<BTreeSet<_>>();
    if evicted.len() != remove {
        return Err(ScheduleError::InvalidState);
    }
    tenant_states.retain(|tenant| !evicted.contains(&tenant.tenant_id));
    Ok(())
}

fn validate_policy_bindings(
    limits: &SchedulerHardLimits,
    policy_bindings: &[TenantSchedulingPolicyBinding],
    windows: &[TenantWindow],
) -> Result<(), ScheduleError> {
    if policy_bindings.len() != windows.len()
        || policy_bindings.len() > usize::from(limits.maximum_tenants)
    {
        return Err(ScheduleError::InvalidPolicy);
    }
    let mut tenants = BTreeSet::new();
    for binding in policy_bindings {
        binding.validate()?;
        if !tenants.insert(binding.tenant_id.clone())
            || !windows
                .iter()
                .any(|window| window.tenant_id == binding.tenant_id)
        {
            return Err(ScheduleError::InvalidPolicy);
        }
    }
    Ok(())
}

fn validate_windows(
    current_round: u64,
    limits: &SchedulerHardLimits,
    windows: &[TenantWindow],
) -> Result<(), ScheduleError> {
    if windows.len() > usize::from(limits.maximum_tenants) {
        return Err(ScheduleError::InvalidWindow);
    }
    let mut tenants = BTreeSet::new();
    let mut jobs = BTreeSet::new();
    for window in windows {
        if window.tenant_id.kind() != ResourceKind::Tenant
            || window.work.len() > usize::from(limits.maximum_window_per_tenant)
            || !tenants.insert(window.tenant_id.clone())
        {
            return Err(ScheduleError::InvalidWindow);
        }
        for candidate in &window.work {
            if candidate.job_id.kind() != ResourceKind::Job
                || candidate.cost == 0
                || candidate.enqueue_round > current_round
                || (candidate.priority == SchedulerPriority::CriticalControl
                    && candidate.lane != SchedulerLane::CriticalControl)
                || (candidate.lane == SchedulerLane::CriticalControl
                    && candidate.priority != SchedulerPriority::CriticalControl)
                || !jobs.insert(candidate.job_id.clone())
            {
                return Err(ScheduleError::InvalidWindow);
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleError {
    InvalidPolicy,
    InvalidState,
    InvalidWindow,
    InvalidBatch,
    PolicyDigestMismatch,
    CounterOverflow,
    Canonicalization,
}

impl fmt::Display for ScheduleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidPolicy => "scheduling policy is invalid",
            Self::InvalidState => "scheduler state is invalid",
            Self::InvalidWindow => "trusted candidate window is invalid",
            Self::InvalidBatch => "requested scheduler batch is invalid",
            Self::PolicyDigestMismatch => "same scheduling policy version changed digest",
            Self::CounterOverflow => "scheduler counter overflowed",
            Self::Canonicalization => "scheduler state cannot be canonicalized",
        })
    }
}

impl Error for ScheduleError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(prefix: &str, suffix: &str) -> ResourceId {
        format!("{prefix}_0198f1c5-0787-75e1-a9e8-d95ca0f3{suffix}")
            .parse()
            .unwrap()
    }

    fn limits() -> SchedulerHardLimits {
        SchedulerHardLimits {
            maximum_deficit: 100,
            maximum_tenants: 8,
            maximum_state_tenants: 16,
            maximum_window_per_tenant: 8,
            maximum_batch: 8,
        }
    }

    fn policy(
        tenant_id: ResourceId,
        policy_suffix: &str,
        weight: u16,
        burst: u16,
        aging_rounds: u16,
    ) -> TenantSchedulingPolicyBinding {
        let document = serde_json::json!({
            "version": 1,
            "weight": weight,
            "burst": burst,
            "aging_rounds": aging_rounds,
        });
        TenantSchedulingPolicyBinding {
            tenant_id,
            policy_version_id: id("prev", policy_suffix),
            policy_version_digest: format!(
                "sha256:{}",
                policy_suffix
                    .chars()
                    .last()
                    .unwrap_or('a')
                    .to_string()
                    .repeat(64)
            )
            .parse()
            .unwrap(),
            rules_digest: canonical_digest(&document).unwrap().parse().unwrap(),
            weight,
            burst,
            aging_rounds,
        }
    }

    fn state() -> SchedulerStateSnapshot {
        SchedulerStateSnapshot {
            work_class: WorkClass::Orchestration,
            current_round: 10,
            cursor_tenant_id: None,
            tenants: Vec::new(),
        }
    }

    fn work(suffix: &str, priority: SchedulerPriority, enqueue_round: u64) -> ReadyWork {
        ReadyWork {
            job_id: id("job", suffix),
            priority,
            lane: SchedulerLane::Business,
            enqueue_round,
            cost: 1,
        }
    }

    #[test]
    fn same_state_and_windows_are_byte_stable() {
        let windows = vec![
            TenantWindow {
                tenant_id: id("ten", "6102"),
                work: vec![
                    work("6112", SchedulerPriority::Normal, 10),
                    work("6111", SchedulerPriority::Low, 1),
                ],
            },
            TenantWindow {
                tenant_id: id("ten", "6101"),
                work: vec![work("6110", SchedulerPriority::Normal, 10)],
            },
        ];
        let policies = vec![
            policy(id("ten", "6102"), "6102", 1, 2, 2),
            policy(id("ten", "6101"), "6101", 1, 1, 2),
        ];
        let left = select_weighted_deficit_round_robin(&state(), &limits(), &policies, &windows, 3)
            .unwrap();
        let right =
            select_weighted_deficit_round_robin(&state(), &limits(), &policies, &windows, 3)
                .unwrap();
        assert_eq!(left, right);
        assert_eq!(
            left.next_state.canonical_digest(),
            right.next_state.canonical_digest()
        );
        assert_eq!(left.selected_job_ids[0], id("job", "6110"));
    }

    #[test]
    fn tenant_cursor_prevents_batch_one_starvation() {
        let mut current = state();
        let tenant_a = id("ten", "6101");
        let tenant_b = id("ten", "6102");
        let mut served = Vec::new();
        for round in 0..6_u16 {
            let windows = vec![
                TenantWindow {
                    tenant_id: tenant_a.clone(),
                    work: vec![work(
                        &format!("62{round:02}"),
                        SchedulerPriority::Normal,
                        current.current_round,
                    )],
                },
                TenantWindow {
                    tenant_id: tenant_b.clone(),
                    work: vec![work(
                        &format!("63{round:02}"),
                        SchedulerPriority::Normal,
                        current.current_round,
                    )],
                },
            ];
            let policies = vec![
                policy(tenant_a.clone(), "6101", 1, 1, 2),
                policy(tenant_b.clone(), "6102", 1, 1, 2),
            ];
            let decision =
                select_weighted_deficit_round_robin(&current, &limits(), &policies, &windows, 1)
                    .unwrap();
            served.push(decision.next_state.cursor_tenant_id.clone().unwrap());
            current = decision.next_state;
        }
        assert_eq!(
            served,
            vec![
                tenant_a.clone(),
                tenant_b.clone(),
                tenant_a,
                tenant_b.clone(),
                id("ten", "6101"),
                tenant_b
            ]
        );
    }

    #[test]
    fn persistent_mixed_cost_backlogs_have_a_bounded_starvation_window() {
        let tenants = [id("ten", "6401"), id("ten", "6402"), id("ten", "6403")];
        let jobs = [id("job", "6411"), id("job", "6412"), id("job", "6413")];
        let policies = vec![
            policy(tenants[0].clone(), "6401", 1, 1, 2),
            policy(tenants[1].clone(), "6402", 8, 1, 2),
            policy(tenants[2].clone(), "6403", 2, 1, 2),
        ];
        let costs = [8_u32, 1, 3];
        let mut current = state();
        let mut last_served = [None; 3];
        let mut maximum_gap = [0_u64; 3];
        let mut served = [0_u64; 3];

        for drive in 0..10_000_u64 {
            let windows = tenants
                .iter()
                .zip(jobs.iter())
                .zip(costs)
                .map(|((tenant_id, job_id), cost)| TenantWindow {
                    tenant_id: tenant_id.clone(),
                    work: vec![ReadyWork {
                        job_id: job_id.clone(),
                        priority: SchedulerPriority::Normal,
                        lane: SchedulerLane::Business,
                        enqueue_round: 1,
                        cost,
                    }],
                })
                .collect::<Vec<_>>();
            let decision =
                select_weighted_deficit_round_robin(&current, &limits(), &policies, &windows, 1)
                    .unwrap();
            let selected = decision.selected_job_ids[0].clone();
            let index = jobs.iter().position(|job_id| job_id == &selected).unwrap();
            if let Some(previous) = last_served[index] {
                maximum_gap[index] = maximum_gap[index].max(drive - previous);
            }
            last_served[index] = Some(drive);
            served[index] += 1;
            current = decision.next_state;
        }

        assert!(served.iter().all(|count| *count > 0));
        assert!(maximum_gap.iter().all(|gap| *gap <= 32));
    }

    #[test]
    fn business_aging_never_reaches_critical_control() {
        let old_low = work("6110", SchedulerPriority::Low, 0);
        assert_eq!(effective_priority(&old_low, 100, 1), 2);
        let forged = ReadyWork {
            priority: SchedulerPriority::CriticalControl,
            ..old_low
        };
        let windows = vec![TenantWindow {
            tenant_id: id("ten", "6101"),
            work: vec![forged],
        }];
        let policies = vec![policy(id("ten", "6101"), "6101", 1, 1, 2)];
        assert_eq!(
            select_weighted_deficit_round_robin(&state(), &limits(), &policies, &windows, 1,),
            Err(ScheduleError::InvalidWindow)
        );
    }

    #[test]
    fn policy_change_resets_deficit_but_not_service_history() {
        let mut current = state();
        current.cursor_tenant_id = Some(id("ten", "6101"));
        current.tenants.push(TenantDeficitState {
            tenant_id: id("ten", "6101"),
            policy_version_id: id("prev", "6101"),
            policy_version_digest: policy(id("ten", "6101"), "6101", 1, 1, 2).policy_version_digest,
            rules_digest: policy(id("ten", "6101"), "6101", 1, 1, 2).rules_digest,
            deficit: 99,
            last_served_round: Some(9),
            successful_claims: 7,
        });
        let replacement = policy(id("ten", "6101"), "6102", 2, 1, 2);
        let decision = select_weighted_deficit_round_robin(
            &current,
            &limits(),
            &[replacement],
            &[TenantWindow {
                tenant_id: id("ten", "6101"),
                work: Vec::new(),
            }],
            1,
        )
        .unwrap();
        assert_eq!(decision.next_state.tenants[0].deficit, 2);
        assert_eq!(decision.next_state.tenants[0].last_served_round, Some(9));
        assert_eq!(decision.next_state.tenants[0].successful_claims, 7);
    }
}
