//! Pure accounting for a locked, bounded partition visit. Enumeration and database
//! locks are owned by PostgreSQL; this function cannot acquire work or spend quota.

use crate::TenantSchedulingPolicyBinding;
use insight_platform_contracts::{
    ClaimMode, JobSweepContinuation, ResourceId, ResourceKind, SchedulerPartitionState,
    SchedulingLane, SchedulingPolicyBinding, TenantSchedulerState,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy)]
pub struct PartitionSchedulerLimits {
    pub maximum_deficit: u64,
    pub maximum_tenant_window: usize,
    pub maximum_candidates_per_tenant: usize,
    pub maximum_claims: usize,
    pub maximum_control_claims_per_tenant: usize,
    pub maximum_quota_lines_per_candidate: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaCost {
    pub account_id: ResourceId,
    pub amount: u64,
}

#[derive(Debug, Clone)]
pub struct AdmissionCandidate {
    pub job_id: ResourceId,
    pub mode: ClaimMode,
    pub lane: SchedulingLane,
    /// The repository has revalidated owner, due time, control and runtime compatibility.
    pub currently_eligible: bool,
    /// Exact accounts for this domain and purpose; control never borrows business permits.
    pub quota_costs: Vec<QuotaCost>,
}

#[derive(Debug, Clone)]
pub struct LockedTenantVisit {
    pub state: TenantSchedulerState,
    pub business_policy: Option<TenantSchedulingPolicyBinding>,
    /// Main-sweep and independent head-probe candidates, deduplicated before calling.
    pub candidates: Vec<AdmissionCandidate>,
    /// Advances over all enumerated rows, including skipped and quota-blocked work.
    pub next_job_sweep: Option<JobSweepContinuation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionSkipReason {
    Ineligible,
    PolicyUnbound,
    Deficit,
    Burst,
    QuotaSaturated,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedAdmission {
    pub job_id: ResourceId,
    pub reason: AdmissionSkipReason,
}

#[derive(Debug, Clone)]
pub struct PartitionAdmissionDecision {
    pub admitted_job_ids: Vec<ResourceId>,
    pub skipped: Vec<SkippedAdmission>,
    pub next_partition: SchedulerPartitionState,
    /// Only visited rows. The repository must never delete unvisited fairness rows.
    pub next_tenants: Vec<TenantSchedulerState>,
    pub quota_reservations: BTreeMap<ResourceId, u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionScheduleError {
    InvalidInput,
    PolicyMismatch,
    MissingQuotaAccount,
    CounterOverflow,
}

pub fn select_admissible_partition_batch(
    partition: &SchedulerPartitionState,
    visits: &[LockedTenantVisit],
    available_quota: &BTreeMap<ResourceId, u64>,
    limits: PartitionSchedulerLimits,
    requested_claims: usize,
    tenant_range_exhausted: bool,
) -> Result<PartitionAdmissionDecision, PartitionScheduleError> {
    use PartitionScheduleError as Error;
    partition.validate().map_err(|_| Error::InvalidInput)?;
    if limits.maximum_deficit == 0
        || limits.maximum_tenant_window == 0
        || limits.maximum_candidates_per_tenant == 0
        || limits.maximum_control_claims_per_tenant == 0
        || limits.maximum_quota_lines_per_candidate == 0
        || requested_claims == 0
        || requested_claims > limits.maximum_claims
        || visits.len() > limits.maximum_tenant_window
    {
        return Err(Error::InvalidInput);
    }
    let mut remaining = available_quota.clone();
    let mut reservations = BTreeMap::<ResourceId, u64>::new();
    let mut admitted = Vec::new();
    let mut skipped = Vec::new();
    let mut next_tenants = Vec::new();
    let mut previous_tenant = partition.cursor_tenant_id.clone();
    let mut seen_jobs = BTreeSet::new();
    for visit in visits {
        let mut state = visit.state.clone();
        state
            .validate(limits.maximum_deficit)
            .map_err(|_| Error::InvalidInput)?;
        if state.work_class != partition.work_class
            || state.partition_id != partition.partition_id
            || state.earliest_eligible_round > partition.current_round
            || previous_tenant
                .as_ref()
                .is_some_and(|previous| previous >= &state.tenant_id)
            || partition
                .tenant_upper_bound
                .as_ref()
                .is_none_or(|upper| &state.tenant_id > upper)
            || visit.candidates.len() > limits.maximum_candidates_per_tenant
        {
            return Err(Error::InvalidInput);
        }
        previous_tenant = Some(state.tenant_id.clone());
        if let Some(policy) = &visit.business_policy {
            policy.validate().map_err(|_| Error::PolicyMismatch)?;
            if policy.tenant_id != state.tenant_id
                || state.policy
                    != (SchedulingPolicyBinding::Bound {
                        policy_version_id: policy.policy_version_id.clone(),
                        policy_version_digest: policy.policy_version_digest.clone(),
                        rules_digest: policy.rules_digest.clone(),
                    })
            {
                return Err(Error::PolicyMismatch);
            }
            if state.credited_round != Some(partition.current_round)
                && visit.candidates.iter().any(|candidate| {
                    candidate.currently_eligible
                        && candidate.lane == SchedulingLane::Business
                        && candidate.mode == ClaimMode::NewAttempt
                })
            {
                state.deficit = state
                    .deficit
                    .saturating_add(u64::from(policy.weight))
                    .min(limits.maximum_deficit);
                state.credited_round = Some(partition.current_round);
            }
        }
        let mut business_claims = 0_usize;
        let mut control_claims = 0_usize;
        for candidate in &visit.candidates {
            if candidate.job_id.kind() != ResourceKind::Job
                || !seen_jobs.insert(candidate.job_id.clone())
                || candidate.quota_costs.len() > limits.maximum_quota_lines_per_candidate
            {
                return Err(Error::InvalidInput);
            }
            let mut accounts = BTreeSet::new();
            for cost in &candidate.quota_costs {
                if cost.account_id.kind() != ResourceKind::QuotaAccount
                    || cost.amount == 0
                    || !accounts.insert(&cost.account_id)
                {
                    return Err(Error::InvalidInput);
                }
                if !remaining.contains_key(&cost.account_id) {
                    return Err(Error::MissingQuotaAccount);
                }
            }
            let skip = if !candidate.currently_eligible {
                Some(AdmissionSkipReason::Ineligible)
            } else if candidate.lane == SchedulingLane::Business && visit.business_policy.is_none()
            {
                Some(AdmissionSkipReason::PolicyUnbound)
            } else if admitted.len() >= requested_claims
                || match candidate.lane {
                    SchedulingLane::Business => {
                        business_claims
                            >= visit
                                .business_policy
                                .as_ref()
                                .map_or(0, |policy| usize::from(policy.burst))
                    }
                    SchedulingLane::RestrictedControl => {
                        control_claims >= limits.maximum_control_claims_per_tenant
                    }
                }
            {
                Some(AdmissionSkipReason::Burst)
            } else if candidate.lane == SchedulingLane::Business
                && candidate.mode.admitted_attempt_cost() > state.deficit
            {
                Some(AdmissionSkipReason::Deficit)
            } else if candidate
                .quota_costs
                .iter()
                .any(|cost| remaining[&cost.account_id] < cost.amount)
            {
                Some(AdmissionSkipReason::QuotaSaturated)
            } else {
                None
            };
            if let Some(reason) = skip {
                skipped.push(SkippedAdmission {
                    job_id: candidate.job_id.clone(),
                    reason,
                });
                continue;
            }
            for cost in &candidate.quota_costs {
                *remaining
                    .get_mut(&cost.account_id)
                    .ok_or(Error::MissingQuotaAccount)? -= cost.amount;
                let reserved = reservations.entry(cost.account_id.clone()).or_default();
                *reserved = reserved
                    .checked_add(cost.amount)
                    .ok_or(Error::CounterOverflow)?;
            }
            match candidate.lane {
                SchedulingLane::Business => {
                    state.deficit -= candidate.mode.admitted_attempt_cost();
                    business_claims += 1;
                }
                SchedulingLane::RestrictedControl => control_claims += 1,
            }
            state.successful_claims = state
                .successful_claims
                .checked_add(1)
                .filter(|value| *value <= i64::MAX as u64)
                .ok_or(Error::CounterOverflow)?;
            state.last_served_round = Some(partition.current_round);
            admitted.push(candidate.job_id.clone());
        }
        state.job_sweep = visit.next_job_sweep.clone();
        state
            .validate(limits.maximum_deficit)
            .map_err(|_| Error::InvalidInput)?;
        next_tenants.push(state);
    }
    let mut next_partition = partition.clone();
    next_partition.cursor_tenant_id = previous_tenant;
    if tenant_range_exhausted {
        next_partition.current_round = next_partition
            .current_round
            .checked_add(1)
            .filter(|round| *round <= i64::MAX as u64)
            .ok_or(Error::CounterOverflow)?;
        next_partition.cursor_tenant_id = None;
        next_partition.tenant_upper_bound = None;
    }
    Ok(PartitionAdmissionDecision {
        admitted_job_ids: admitted,
        skipped,
        next_partition,
        next_tenants,
        quota_reservations: reservations,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{
        canonical_digest, SchedulerPartitionId, Sha256Digest, WorkClass,
    };
    fn id(kind: ResourceKind, n: u8) -> ResourceId {
        let tenant: ResourceId = format!("ten_01951f3d-7b80-7b81-8d22-841bcc458f{n:02x}")
            .parse()
            .unwrap();
        ResourceId::from_uuid_v7(kind, tenant.uuid()).unwrap()
    }
    fn digest() -> Sha256Digest {
        format!("sha256:{}", "1".repeat(64)).parse().unwrap()
    }
    fn fixture() -> (
        SchedulerPartitionState,
        LockedTenantVisit,
        PartitionSchedulerLimits,
    ) {
        let tenant = id(ResourceKind::Tenant, 1);
        let partition_id = SchedulerPartitionId::for_tenant(&tenant).unwrap();
        let policy = TenantSchedulingPolicyBinding {
            tenant_id: tenant.clone(),
            policy_version_id: id(ResourceKind::PolicyRevision, 2),
            policy_version_digest: digest(),
            rules_digest: canonical_digest(
                &serde_json::json!({"version":1,"weight":4,"burst":4,"aging_rounds":1}),
            )
            .unwrap()
            .parse()
            .unwrap(),
            weight: 4,
            burst: 4,
            aging_rounds: 1,
        };
        let state = TenantSchedulerState {
            schema_version: 1,
            tenant_id: tenant.clone(),
            work_class: WorkClass::Orchestration,
            partition_id,
            policy: SchedulingPolicyBinding::Bound {
                policy_version_id: policy.policy_version_id.clone(),
                policy_version_digest: policy.policy_version_digest.clone(),
                rules_digest: policy.rules_digest.clone(),
            },
            deficit: 0,
            earliest_eligible_round: 0,
            credited_round: None,
            last_served_round: None,
            successful_claims: 0,
            job_sweep: None,
        };
        (
            SchedulerPartitionState {
                schema_version: 1,
                work_class: WorkClass::Orchestration,
                partition_id,
                current_round: 1,
                tenant_upper_bound: Some(tenant),
                cursor_tenant_id: None,
            },
            LockedTenantVisit {
                state,
                business_policy: Some(policy),
                candidates: Vec::new(),
                next_job_sweep: None,
            },
            PartitionSchedulerLimits {
                maximum_deficit: 100,
                maximum_tenant_window: 4,
                maximum_candidates_per_tenant: 8,
                maximum_claims: 4,
                maximum_control_claims_per_tenant: 2,
                maximum_quota_lines_per_candidate: 4,
            },
        )
    }
    fn candidate(n: u8, cost: u64) -> AdmissionCandidate {
        AdmissionCandidate {
            job_id: id(ResourceKind::Job, n),
            mode: ClaimMode::NewAttempt,
            lane: SchedulingLane::Business,
            currently_eligible: true,
            quota_costs: vec![QuotaCost {
                account_id: id(ResourceKind::QuotaAccount, 9),
                amount: cost,
            }],
        }
    }
    #[test]
    fn saturated_head_does_not_poison_later_work_or_spend_credit() {
        let (partition, mut visit, limits) = fixture();
        visit.candidates = vec![candidate(3, 3), candidate(4, 1), candidate(5, 1)];
        let decision = select_admissible_partition_batch(
            &partition,
            &[visit],
            &BTreeMap::from([(id(ResourceKind::QuotaAccount, 9), 1)]),
            limits,
            4,
            true,
        )
        .unwrap();
        assert_eq!(decision.admitted_job_ids, vec![id(ResourceKind::Job, 4)]);
        assert_eq!(decision.next_tenants[0].deficit, 3);
        assert_eq!(
            decision.quota_reservations[&id(ResourceKind::QuotaAccount, 9)],
            1
        );
        assert_eq!(decision.next_partition.current_round, 2);
    }
    #[test]
    fn persisted_credit_is_not_granted_twice_after_restart() {
        let (partition, mut visit, limits) = fixture();
        visit.state.deficit = 7;
        visit.state.credited_round = Some(1);
        visit.candidates = vec![candidate(3, 1)];
        let decision = select_admissible_partition_batch(
            &partition,
            &[visit],
            &BTreeMap::from([(id(ResourceKind::QuotaAccount, 9), 0)]),
            limits,
            1,
            false,
        )
        .unwrap();
        assert_eq!(decision.next_tenants[0].deficit, 7);
        assert!(decision.admitted_job_ids.is_empty());
        assert_eq!(
            decision.next_partition.cursor_tenant_id,
            partition.tenant_upper_bound
        );
    }
    #[test]
    fn restricted_cleanup_does_not_require_a_business_policy() {
        let (partition, mut visit, limits) = fixture();
        visit.state.policy = SchedulingPolicyBinding::Unbound;
        visit.business_policy = None;
        let mut control = candidate(3, 1);
        control.lane = SchedulingLane::RestrictedControl;
        control.quota_costs.clear();
        visit.candidates = vec![control];
        let decision = select_admissible_partition_batch(
            &partition,
            &[visit],
            &BTreeMap::new(),
            limits,
            1,
            true,
        )
        .unwrap();
        assert_eq!(decision.admitted_job_ids.len(), 1);
        assert_eq!(decision.next_tenants[0].deficit, 0);
    }
}
