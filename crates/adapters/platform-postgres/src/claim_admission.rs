//! Shared admission transaction prefix. Domain claims supply eligibility and
//! exact quota costs, then settle this decision with their own owner mutations.
//! This module never creates a queue or acquires a lease.
use crate::{
    partition_scheduler::{self, JobCohortPage, LockedPartition, TenantFairnessWindow},
    repository::{load_tenant_scheduling_policy, RepositoryError},
};
use insight_platform_contracts::{
    ClaimMode, ResourceId, SchedulingLane, SchedulingPolicyBinding, WorkClass,
};
use insight_platform_scheduler::partitioned::{
    AdmissionCandidate, LockedTenantVisit, PartitionAdmissionDecision, PartitionSchedulerLimits,
    QuotaCost,
};
use insight_platform_scheduler::{SchedulerHardLimits, TenantSchedulingPolicyBinding};
use sqlx::{Postgres, Transaction};
use std::collections::BTreeMap;

pub(crate) struct PreparedClaimAdmission {
    partition: LockedPartition,
    window: TenantFairnessWindow,
    pages: BTreeMap<ResourceId, JobCohortPage>,
    policies: BTreeMap<ResourceId, TenantSchedulingPolicyBinding>,
    limits: SchedulerHardLimits,
}
#[derive(Clone)]
pub(crate) struct EligibleClaim {
    pub lane: SchedulingLane,
    pub mode: ClaimMode,
    pub quota_costs: Vec<QuotaCost>,
}
impl PreparedClaimAdmission {
    /// All shared scheduling and tenant locks precede quota and domain locks.
    pub async fn prepare(
        tx: &mut Transaction<'_, Postgres>,
        class: WorkClass,
        limits: SchedulerHardLimits,
    ) -> Result<Option<Self>, RepositoryError> {
        let Some(partition) = partition_scheduler::lock_partition(tx, class).await? else {
            return Ok(None);
        };
        let window = partition_scheduler::lock_tenant_window(
            tx,
            &partition,
            limits.maximum_tenants,
            limits.maximum_deficit,
        )
        .await?;
        let mut pages = BTreeMap::new();
        let mut policies = BTreeMap::new();
        for tenant in &window.tenants {
            pages.insert(
                tenant.state.tenant_id.clone(),
                partition_scheduler::scan_job_cohort(tx, tenant, limits.maximum_window_per_tenant)
                    .await?,
            );
        }
        for tenant in &window.tenants {
            if matches!(tenant.state.policy, SchedulingPolicyBinding::Bound { .. }) {
                match load_tenant_scheduling_policy(tx, &tenant.state.tenant_id).await {
                    Ok(policy) => {
                        policies.insert(tenant.state.tenant_id.clone(), policy);
                    }
                    // A disabled tenant or deployment prevents business dispatch,
                    // but must not prevent cursor progress or restricted cleanup.
                    Err(RepositoryError::NotFound(_)) => {}
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(Some(Self {
            partition,
            window,
            pages,
            policies,
            limits,
        }))
    }
    pub fn candidate_ids(&self) -> Vec<String> {
        self.pages
            .values()
            .flat_map(|page| page.jobs.iter().map(|job| job.job_id.clone()))
            .collect()
    }
    pub fn candidate_jobs(&self) -> impl Iterator<Item = &insight_platform_jobs::store::JobRecord> {
        self.pages.values().flat_map(|page| &page.jobs)
    }
    pub fn observe_diagnostics(&self) {
        for diagnostic in self.pages.values().flat_map(|page| &page.diagnostics) {
            crate::recovery_isolation::observe(diagnostic);
        }
    }

    /// Re-run the same selector from the original locked inputs. This is accounting only:
    /// no owner, lease, quota or journal mutation occurs here and no replacement is admitted.
    pub fn settle_actual(
        &self,
        eligible: &BTreeMap<String, EligibleClaim>,
        original_quota: &BTreeMap<ResourceId, u64>,
        limit: u16,
        actual_ids: impl IntoIterator<Item = String>,
    ) -> Result<PartitionAdmissionDecision, RepositoryError> {
        let actual = actual_ids
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        let filtered = eligible
            .iter()
            .filter(|(id, _)| actual.contains(*id))
            .map(|(id, candidate)| (id.clone(), candidate.clone()))
            .collect();
        let decision = self.select(&filtered, original_quota, limit)?;
        let selected = decision
            .admitted_job_ids
            .iter()
            .map(ToString::to_string)
            .collect::<std::collections::BTreeSet<_>>();
        if selected != actual {
            return Err(RepositoryError::CorruptRow(
                "actual claim accounting differs from selected subset".into(),
            ));
        }
        Ok(decision)
    }
    pub fn select(
        &self,
        eligible: &BTreeMap<String, EligibleClaim>,
        quota: &BTreeMap<ResourceId, u64>,
        limit: u16,
    ) -> Result<PartitionAdmissionDecision, RepositoryError> {
        let visits = self
            .window
            .tenants
            .iter()
            .map(|tenant| {
                let page = &self.pages[&tenant.state.tenant_id];
                let candidates = page
                    .jobs
                    .iter()
                    .map(|job| {
                        let entry = eligible.get(&job.job_id);
                        Ok(AdmissionCandidate {
                            job_id: job.job_id.parse().map_err(
                                |error: insight_platform_contracts::ResourceIdError| {
                                    RepositoryError::CorruptRow(error.to_string())
                                },
                            )?,
                            mode: entry.map_or(ClaimMode::NewAttempt, |entry| entry.mode),
                            lane: entry.map_or(SchedulingLane::Business, |entry| entry.lane),
                            currently_eligible: entry.is_some(),
                            quota_costs: entry
                                .map_or_else(Vec::new, |entry| entry.quota_costs.clone()),
                        })
                    })
                    .collect::<Result<Vec<_>, RepositoryError>>()?;
                Ok(LockedTenantVisit {
                    state: tenant.state.clone(),
                    business_policy: self.policies.get(&tenant.state.tenant_id).cloned(),
                    candidates,
                    next_job_sweep: page.next_sweep.clone(),
                })
            })
            .collect::<Result<Vec<_>, RepositoryError>>()?;
        insight_platform_scheduler::partitioned::select_admissible_partition_batch(
            &self.partition.state,
            &visits,
            quota,
            PartitionSchedulerLimits {
                maximum_deficit: self.limits.maximum_deficit,
                maximum_tenant_window: usize::from(self.limits.maximum_tenants),
                maximum_candidates_per_tenant: usize::from(self.limits.maximum_window_per_tenant),
                maximum_claims: 256,
                maximum_control_claims_per_tenant: 1,
                maximum_quota_lines_per_candidate: 16,
            },
            usize::from(limit),
            self.window.range_exhausted,
        )
        .map_err(|error| RepositoryError::InvalidInput(format!("partition admission: {error:?}")))
    }
    pub async fn persist(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        decision: &PartitionAdmissionDecision,
    ) -> Result<(), RepositoryError> {
        partition_scheduler::persist_admission(tx, &self.partition, &self.window.tenants, decision)
            .await
    }
}
