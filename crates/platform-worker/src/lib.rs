//! Process-local physical capacity for Platform v1 worker roles.
//!
//! This crate deliberately owns no durable Job, lease, or tenant quota state. A caller reserves
//! local capacity before a PostgreSQL claim and binds only the Jobs actually claimed.

use insight_platform_contracts::{
    HardLimitProfile, ResourceId, ResourceKind, Sha256Digest, WorkClass, WorkerManifest,
    WorkerManifestError,
};
use std::{collections::BTreeSet, error::Error, fmt, sync::Arc};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClaimBatchHardLimit(usize);

impl ClaimBatchHardLimit {
    pub fn from_profile(profile: &HardLimitProfile) -> Result<Self, LocalWorkerPoolError> {
        profile.validate().map_err(|failure| {
            LocalWorkerPoolError::InvalidHardLimitProfile(failure.to_string())
        })?;
        let value = usize::try_from(profile.run_scheduler.claim_batch.hard_max).map_err(|_| {
            LocalWorkerPoolError::InvalidHardLimitProfile(
                "run_scheduler.claim_batch exceeds local address space".to_owned(),
            )
        })?;
        Ok(Self(value))
    }

    pub const fn get(self) -> usize {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalWorkerPoolSnapshot {
    pub worker_role: String,
    pub work_class: WorkClass,
    pub worker_process_generation_id: ResourceId,
    pub worker_manifest_digest: Sha256Digest,
    pub business_capacity: usize,
    pub business_available: usize,
    pub critical_control_capacity: usize,
    pub critical_control_available: usize,
}

#[derive(Clone)]
pub struct LocalWorkerPools {
    manifest: Arc<WorkerManifest>,
    worker_manifest_digest: Sha256Digest,
    worker_process_generation_id: ResourceId,
    business: Arc<Semaphore>,
    business_capacity_available: Arc<Notify>,
    critical_control: Arc<Semaphore>,
}

impl LocalWorkerPools {
    pub fn new(
        manifest: WorkerManifest,
        worker_process_generation_id: ResourceId,
    ) -> Result<Self, LocalWorkerPoolError> {
        manifest
            .validate()
            .map_err(LocalWorkerPoolError::InvalidManifest)?;
        if worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration {
            return Err(LocalWorkerPoolError::InvalidWorkerProcessGeneration);
        }
        let worker_manifest_digest = manifest
            .canonical_digest()
            .map_err(LocalWorkerPoolError::InvalidManifest)?;
        let business = Arc::new(Semaphore::new(usize::from(manifest.max_concurrency)));
        let critical_control = Arc::new(Semaphore::new(usize::from(
            manifest.critical_control_reserved_slots,
        )));
        Ok(Self {
            manifest: Arc::new(manifest),
            worker_manifest_digest,
            worker_process_generation_id,
            business,
            business_capacity_available: Arc::new(Notify::new()),
            critical_control,
        })
    }

    pub fn snapshot(&self) -> LocalWorkerPoolSnapshot {
        LocalWorkerPoolSnapshot {
            worker_role: self.manifest.worker_role.clone(),
            work_class: self.manifest.work_class,
            worker_process_generation_id: self.worker_process_generation_id.clone(),
            worker_manifest_digest: self.worker_manifest_digest.clone(),
            business_capacity: usize::from(self.manifest.max_concurrency),
            business_available: self.business.available_permits(),
            critical_control_capacity: usize::from(self.manifest.critical_control_reserved_slots),
            critical_control_available: self.critical_control.available_permits(),
        }
    }

    pub fn reserve_claim_capacity(
        &self,
        work_class: WorkClass,
        requested: usize,
        hard_limit: ClaimBatchHardLimit,
    ) -> Result<Option<ClaimCapacityReservation>, LocalWorkerPoolError> {
        if work_class != self.manifest.work_class {
            return Err(LocalWorkerPoolError::WorkClassMismatch {
                manifest: self.manifest.work_class,
                requested: work_class,
            });
        }
        if requested == 0 || hard_limit.get() == 0 {
            return Err(LocalWorkerPoolError::InvalidClaimCapacity);
        }
        let target = requested
            .min(hard_limit.get())
            .min(self.business.available_permits());
        let mut permits = Vec::with_capacity(target);
        for _ in 0..target {
            match self.business.clone().try_acquire_owned() {
                Ok(permit) => permits.push(permit),
                Err(TryAcquireError::NoPermits) => break,
                Err(TryAcquireError::Closed) => return Err(LocalWorkerPoolError::PoolClosed),
            }
        }
        if permits.is_empty() {
            return Ok(None);
        }
        Ok(Some(ClaimCapacityReservation {
            worker_role: self.manifest.worker_role.clone(),
            work_class,
            worker_process_generation_id: self.worker_process_generation_id.clone(),
            worker_manifest_digest: self.worker_manifest_digest.clone(),
            business_capacity_available: Arc::clone(&self.business_capacity_available),
            permits,
        }))
    }

    pub fn try_acquire_critical_control(
        &self,
    ) -> Result<Option<ActiveCriticalControlPermit>, LocalWorkerPoolError> {
        match self.critical_control.clone().try_acquire_owned() {
            Ok(permit) => Ok(Some(ActiveCriticalControlPermit {
                worker_role: self.manifest.worker_role.clone(),
                worker_process_generation_id: self.worker_process_generation_id.clone(),
                worker_manifest_digest: self.worker_manifest_digest.clone(),
                _permit: permit,
            })),
            Err(TryAcquireError::NoPermits) => Ok(None),
            Err(TryAcquireError::Closed) => Err(LocalWorkerPoolError::PoolClosed),
        }
    }

    pub async fn wait_for_business_capacity(&self) {
        loop {
            if self.business.available_permits() > 0 {
                return;
            }
            let notified = self.business_capacity_available.notified();
            if self.business.available_permits() > 0 {
                return;
            }
            notified.await;
        }
    }
}

pub struct ClaimCapacityReservation {
    worker_role: String,
    work_class: WorkClass,
    worker_process_generation_id: ResourceId,
    worker_manifest_digest: Sha256Digest,
    business_capacity_available: Arc<Notify>,
    permits: Vec<OwnedSemaphorePermit>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClaimedJobIdentity {
    pub job_id: ResourceId,
    pub lease_generation: u64,
}

impl ClaimCapacityReservation {
    pub fn claim_limit(&self) -> usize {
        self.permits.len()
    }

    pub fn bind_claimed_jobs(
        mut self,
        jobs: Vec<ClaimedJobIdentity>,
    ) -> Result<Vec<ActiveLocalJobPermit>, LocalWorkerPoolError> {
        if jobs.len() > self.permits.len() {
            return Err(LocalWorkerPoolError::ClaimedMoreThanReserved {
                claimed: jobs.len(),
                reserved: self.permits.len(),
            });
        }
        let mut unique = BTreeSet::new();
        for job in &jobs {
            if job.job_id.kind() != ResourceKind::Job || job.lease_generation == 0 {
                return Err(LocalWorkerPoolError::InvalidJobId);
            }
            if !unique.insert(job.job_id.clone()) {
                return Err(LocalWorkerPoolError::DuplicateJobId);
            }
        }
        Ok(jobs
            .into_iter()
            .zip(self.permits.drain(..))
            .map(|(job, permit)| ActiveLocalJobPermit {
                worker_role: self.worker_role.clone(),
                work_class: self.work_class,
                worker_process_generation_id: self.worker_process_generation_id.clone(),
                worker_manifest_digest: self.worker_manifest_digest.clone(),
                job,
                permit: Some(permit),
                capacity_available: Arc::clone(&self.business_capacity_available),
            })
            .collect())
    }
}

pub struct ActiveLocalJobPermit {
    worker_role: String,
    work_class: WorkClass,
    worker_process_generation_id: ResourceId,
    worker_manifest_digest: Sha256Digest,
    job: ClaimedJobIdentity,
    permit: Option<OwnedSemaphorePermit>,
    capacity_available: Arc<Notify>,
}

impl ActiveLocalJobPermit {
    pub fn worker_role(&self) -> &str {
        &self.worker_role
    }

    pub const fn work_class(&self) -> WorkClass {
        self.work_class
    }

    pub fn worker_process_generation_id(&self) -> &ResourceId {
        &self.worker_process_generation_id
    }

    pub fn worker_manifest_digest(&self) -> &Sha256Digest {
        &self.worker_manifest_digest
    }

    pub fn job_id(&self) -> &ResourceId {
        &self.job.job_id
    }

    pub const fn lease_generation(&self) -> u64 {
        self.job.lease_generation
    }
}

impl Drop for ActiveLocalJobPermit {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.capacity_available.notify_one();
    }
}

pub struct ActiveCriticalControlPermit {
    worker_role: String,
    worker_process_generation_id: ResourceId,
    worker_manifest_digest: Sha256Digest,
    _permit: OwnedSemaphorePermit,
}

impl ActiveCriticalControlPermit {
    pub fn worker_role(&self) -> &str {
        &self.worker_role
    }

    pub fn worker_process_generation_id(&self) -> &ResourceId {
        &self.worker_process_generation_id
    }

    pub fn worker_manifest_digest(&self) -> &Sha256Digest {
        &self.worker_manifest_digest
    }
}

#[derive(Debug)]
pub enum LocalWorkerPoolError {
    InvalidManifest(WorkerManifestError),
    InvalidHardLimitProfile(String),
    InvalidWorkerProcessGeneration,
    WorkClassMismatch {
        manifest: WorkClass,
        requested: WorkClass,
    },
    InvalidClaimCapacity,
    ClaimedMoreThanReserved {
        claimed: usize,
        reserved: usize,
    },
    InvalidJobId,
    DuplicateJobId,
    PoolClosed,
}

impl fmt::Display for LocalWorkerPoolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidManifest(failure) => failure.fmt(formatter),
            Self::InvalidHardLimitProfile(message) => {
                write!(formatter, "invalid claim-batch hard limit: {message}")
            }
            Self::InvalidWorkerProcessGeneration => {
                formatter.write_str("worker process generation ID has the wrong resource kind")
            }
            Self::WorkClassMismatch {
                manifest,
                requested,
            } => write!(
                formatter,
                "worker manifest class {manifest} cannot reserve {requested} work"
            ),
            Self::InvalidClaimCapacity => {
                formatter.write_str("claim capacity and its hard limit must be positive")
            }
            Self::ClaimedMoreThanReserved { claimed, reserved } => write!(
                formatter,
                "claim returned {claimed} Jobs after only {reserved} local slots were reserved"
            ),
            Self::InvalidJobId => formatter.write_str("local Job permit requires a Job ID"),
            Self::DuplicateJobId => {
                formatter.write_str("one claim batch cannot bind the same Job twice")
            }
            Self::PoolClosed => formatter.write_str("local worker pool is closed"),
        }
    }
}

impl Error for LocalWorkerPoolError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidManifest(failure) => Some(failure),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn id(prefix: &str, suffix: &str) -> ResourceId {
        format!("{prefix}_0198f1c5-0787-75e1-a9e8-d95ca0f3{suffix}")
            .parse()
            .unwrap()
    }

    fn pools(work_class: WorkClass, business: u16, control: u16) -> LocalWorkerPools {
        LocalWorkerPools::new(
            WorkerManifest {
                manifest_version: 1,
                worker_role: format!("{}.primary", work_class.as_str()),
                work_class,
                adapter_runtime_digest: digest('a'),
                protocol_version: 1,
                max_concurrency: business,
                critical_control_reserved_slots: control,
            },
            id("wrk", "5401"),
        )
        .unwrap()
    }

    #[test]
    fn business_saturation_cannot_consume_reserved_control_capacity() {
        let pools = pools(WorkClass::Sandbox, 1, 1);
        let reservation = pools
            .reserve_claim_capacity(WorkClass::Sandbox, 1, ClaimBatchHardLimit(1))
            .unwrap()
            .unwrap();
        let active = reservation
            .bind_claimed_jobs(vec![ClaimedJobIdentity {
                job_id: id("job", "5401"),
                lease_generation: 1,
            }])
            .unwrap();
        assert!(pools
            .reserve_claim_capacity(WorkClass::Sandbox, 1, ClaimBatchHardLimit(1))
            .unwrap()
            .is_none());

        let control = pools.try_acquire_critical_control().unwrap().unwrap();
        assert!(pools.try_acquire_critical_control().unwrap().is_none());
        assert_eq!(pools.snapshot().business_available, 0);
        assert_eq!(pools.snapshot().critical_control_available, 0);

        drop(active);
        drop(control);
        assert_eq!(pools.snapshot().business_available, 1);
        assert_eq!(pools.snapshot().critical_control_available, 1);
    }

    #[test]
    fn work_classes_have_disjoint_physical_capacity() {
        let sandbox = pools(WorkClass::Sandbox, 1, 1);
        let model = pools(WorkClass::Model, 1, 1);
        let sandbox_reservation = sandbox
            .reserve_claim_capacity(WorkClass::Sandbox, 1, ClaimBatchHardLimit(1))
            .unwrap()
            .unwrap();
        assert!(sandbox
            .reserve_claim_capacity(WorkClass::Sandbox, 1, ClaimBatchHardLimit(1))
            .unwrap()
            .is_none());
        assert_eq!(
            model
                .reserve_claim_capacity(WorkClass::Model, 1, ClaimBatchHardLimit(1))
                .unwrap()
                .unwrap()
                .claim_limit(),
            1
        );
        drop(sandbox_reservation);
    }

    #[test]
    fn claim_batch_is_capped_before_database_claim_and_unused_slots_release() {
        let pools = pools(WorkClass::Orchestration, 4, 1);
        let reservation = pools
            .reserve_claim_capacity(WorkClass::Orchestration, 4, ClaimBatchHardLimit(2))
            .unwrap()
            .unwrap();
        assert_eq!(reservation.claim_limit(), 2);
        assert_eq!(pools.snapshot().business_available, 2);

        let active = reservation
            .bind_claimed_jobs(vec![ClaimedJobIdentity {
                job_id: id("job", "5402"),
                lease_generation: 1,
            }])
            .unwrap();
        assert_eq!(pools.snapshot().business_available, 3);
        assert_eq!(active[0].work_class(), WorkClass::Orchestration);
        drop(active);
        assert_eq!(pools.snapshot().business_available, 4);
    }

    #[test]
    fn class_or_job_mismatch_fails_closed_without_leaking_slots() {
        let pools = pools(WorkClass::Context, 2, 1);
        assert!(matches!(
            pools.reserve_claim_capacity(WorkClass::Mcp, 1, ClaimBatchHardLimit(1)),
            Err(LocalWorkerPoolError::WorkClassMismatch { .. })
        ));
        let reservation = pools
            .reserve_claim_capacity(WorkClass::Context, 2, ClaimBatchHardLimit(2))
            .unwrap()
            .unwrap();
        assert!(matches!(
            reservation.bind_claimed_jobs(vec![ClaimedJobIdentity {
                job_id: id("run", "5401"),
                lease_generation: 1,
            }]),
            Err(LocalWorkerPoolError::InvalidJobId)
        ));
        assert_eq!(pools.snapshot().business_available, 2);
    }

    #[tokio::test]
    async fn active_job_release_wakes_capacity_waiter() {
        let pools = pools(WorkClass::Orchestration, 1, 1);
        let active = pools
            .reserve_claim_capacity(WorkClass::Orchestration, 1, ClaimBatchHardLimit(1))
            .unwrap()
            .unwrap()
            .bind_claimed_jobs(vec![ClaimedJobIdentity {
                job_id: id("job", "5403"),
                lease_generation: 1,
            }])
            .unwrap();
        let waiting_pools = pools.clone();
        let waiter = tokio::spawn(async move {
            waiting_pools.wait_for_business_capacity().await;
            waiting_pools.snapshot().business_available
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        drop(active);
        assert_eq!(waiter.await.unwrap(), 1);
    }
}
