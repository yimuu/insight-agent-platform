//! PostgreSQL cursor and fairness authority for partitioned Job admission.
//!
//! Acquire a partition and the complete fairness window before taking tenant,
//! quota, domain-owner or Job locks. Job creation must never lazily create these
//! rows. The caller commits admission, quota, events and these cursors together.

use crate::repository::{persisted_job_from_row, RepositoryError};
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    JobCreationKey, JobSweepContinuation, ResourceId, SchedulerPartitionId,
    SchedulerPartitionState, SchedulingPolicyBinding, TenantSchedulerState, WorkClass,
    SCHEDULER_STATE_VERSION,
};
use insight_platform_jobs::store::JobRecord;
use insight_platform_scheduler::partitioned::PartitionAdmissionDecision;
use sqlx::{postgres::PgRow, Postgres, Row, Transaction};

/// Physical placement hints observed before a serializable claim transaction starts.
/// The short READ COMMITTED statement releases its locks before returning; these IDs
/// authorize nothing and are always re-read under the actual claim transaction.
pub(crate) async fn available_partition_hints(
    pool: &sqlx::PgPool,
    work_class: WorkClass,
) -> Result<std::collections::VecDeque<SchedulerPartitionId>, RepositoryError> {
    let ids: Vec<i16> = sqlx::query_scalar("SELECT partition.partition_id FROM insight_platform.scheduler_state AS partition WHERE partition.work_class=$1 AND EXISTS (SELECT 1 FROM insight_platform.scheduler_tenant_state AS tenant WHERE tenant.work_class=partition.work_class AND tenant.partition_id=partition.partition_id) ORDER BY partition.updated_at,partition.partition_id LIMIT $2 FOR UPDATE OF partition SKIP LOCKED")
        .bind(work_class.as_str())
        .bind(i64::from(insight_platform_contracts::SCHEDULER_PARTITION_COUNT))
        .fetch_all(pool).await?;
    ids.into_iter()
        .map(|id| u8::try_from(id).map(SchedulerPartitionId).map_err(corrupt))
        .collect()
}

/// Exact placement for SERIALIZABLE claims. The broad dispatch scan must never
/// enter this transaction's predicate-read set and couple otherwise independent partitions.
pub(crate) async fn lock_exact_partition(
    tx: &mut Transaction<'_, Postgres>,
    work_class: WorkClass,
    hint: SchedulerPartitionId,
) -> Result<Option<LockedPartition>, RepositoryError> {
    let row = sqlx::query("SELECT partition.* FROM insight_platform.scheduler_state AS partition WHERE partition.work_class=$1 AND partition.partition_id=$2 AND EXISTS (SELECT 1 FROM insight_platform.scheduler_tenant_state AS tenant WHERE tenant.work_class=$1 AND tenant.partition_id=$2) FOR UPDATE OF partition SKIP LOCKED")
        .bind(work_class.as_str()).bind(i16::from(hint.0)).fetch_optional(&mut **tx).await?;
    decode_locked_partition(tx, work_class, row).await
}

pub(crate) struct LockedPartition {
    version: i64,
    pub state: SchedulerPartitionState,
}

pub(crate) struct LockedTenantFairness {
    version: i64,
    pub state: TenantSchedulerState,
}

pub(crate) struct TenantFairnessWindow {
    pub tenants: Vec<LockedTenantFairness>,
    pub range_exhausted: bool,
}

pub(crate) struct JobCohortPage {
    /// Enumerated work is not yet eligible, leased or authorized.
    pub jobs: Vec<JobRecord>,
    pub diagnostics: Vec<insight_platform_jobs::store::SafeScanDiagnostic>,
    pub next_sweep: Option<JobSweepContinuation>,
}

fn corrupt(message: impl ToString) -> RepositoryError {
    RepositoryError::CorruptRow(message.to_string())
}
fn nominal(value: String) -> Result<ResourceId, RepositoryError> {
    value.parse().map_err(corrupt)
}
fn counter(value: i64) -> Result<u64, RepositoryError> {
    u64::try_from(value).map_err(corrupt)
}
fn optional_counter(value: Option<i64>) -> Result<Option<u64>, RepositoryError> {
    value.map(counter).transpose()
}

/// Skip only partitions without any enrolled tenant fairness row. Existing tenants
/// remain visible regardless of policy, due work, quota or worker compatibility;
/// least-recently-visited selection is physical distribution, not tenant fairness.
pub(crate) async fn lock_partition(
    tx: &mut Transaction<'_, Postgres>,
    work_class: WorkClass,
) -> Result<Option<LockedPartition>, RepositoryError> {
    let row = sqlx::query("SELECT partition.* FROM insight_platform.scheduler_state AS partition WHERE partition.work_class=$1 AND EXISTS (SELECT 1 FROM insight_platform.scheduler_tenant_state AS tenant WHERE tenant.work_class=partition.work_class AND tenant.partition_id=partition.partition_id) ORDER BY partition.updated_at, partition.partition_id FOR UPDATE OF partition SKIP LOCKED LIMIT 1")
        .bind(work_class.as_str()).fetch_optional(&mut **tx).await?;
    decode_locked_partition(tx, work_class, row).await
}

async fn decode_locked_partition(
    tx: &mut Transaction<'_, Postgres>,
    work_class: WorkClass,
    row: Option<PgRow>,
) -> Result<Option<LockedPartition>, RepositoryError> {
    let Some(row) = row else {
        return Ok(None);
    };
    let partition: i16 = row.try_get("partition_id")?;
    let mut locked = LockedPartition {
        version: row.try_get("version")?,
        state: SchedulerPartitionState {
            schema_version: SCHEDULER_STATE_VERSION,
            work_class,
            partition_id: SchedulerPartitionId(u8::try_from(partition).map_err(corrupt)?),
            current_round: counter(row.try_get("current_round")?)?,
            tenant_upper_bound: row
                .try_get::<Option<String>, _>("tenant_upper_bound")?
                .map(nominal)
                .transpose()?,
            cursor_tenant_id: row
                .try_get::<Option<String>, _>("cursor_tenant_id")?
                .map(nominal)
                .transpose()?,
        },
    };
    locked.state.validate().map_err(corrupt)?;
    if locked.state.tenant_upper_bound.is_none() {
        let upper: Option<String> = sqlx::query_scalar("SELECT max(tenant_id) FROM insight_platform.scheduler_tenant_state WHERE work_class=$1 AND partition_id=$2 AND earliest_eligible_round <= $3")
            .bind(work_class.as_str()).bind(partition).bind(locked.state.current_round as i64)
            .fetch_one(&mut **tx).await?;
        locked.state.tenant_upper_bound = upper.map(nominal).transpose()?;
    }
    Ok(Some(locked))
}

pub(crate) async fn lock_tenant_window(
    tx: &mut Transaction<'_, Postgres>,
    partition: &LockedPartition,
    maximum_tenants: u16,
    maximum_deficit: u64,
) -> Result<TenantFairnessWindow, RepositoryError> {
    if maximum_tenants == 0 {
        return Err(RepositoryError::InvalidInput("empty tenant window".into()));
    }
    let rows = sqlx::query("SELECT * FROM insight_platform.scheduler_tenant_state WHERE work_class=$1 AND partition_id=$2 AND earliest_eligible_round <= $3 AND ($4::text IS NULL OR tenant_id > $4) AND tenant_id <= $5 ORDER BY tenant_id LIMIT $6 FOR UPDATE")
        .bind(partition.state.work_class.as_str()).bind(i16::from(partition.state.partition_id.0))
        .bind(partition.state.current_round as i64)
        .bind(partition.state.cursor_tenant_id.as_ref().map(ToString::to_string))
        .bind(partition.state.tenant_upper_bound.as_ref().map(ToString::to_string))
        .bind(i64::from(maximum_tenants)+1).fetch_all(&mut **tx).await?;
    let range_exhausted = rows.len() <= usize::from(maximum_tenants);
    let tenants = rows
        .into_iter()
        .take(usize::from(maximum_tenants))
        .map(|row| tenant_from_row(row, partition.state.work_class, maximum_deficit))
        .collect::<Result<_, _>>()?;
    Ok(TenantFairnessWindow {
        tenants,
        range_exhausted,
    })
}

fn tenant_from_row(
    row: PgRow,
    work_class: WorkClass,
    maximum_deficit: u64,
) -> Result<LockedTenantFairness, RepositoryError> {
    let policy = match row.try_get::<Option<String>, _>("policy_version_id")? {
        None => SchedulingPolicyBinding::Unbound,
        Some(id) => SchedulingPolicyBinding::Bound {
            policy_version_id: nominal(id)?,
            policy_version_digest: row
                .try_get::<String, _>("policy_version_digest")?
                .parse()
                .map_err(corrupt)?,
            rules_digest: row
                .try_get::<String, _>("rules_digest")?
                .parse()
                .map_err(corrupt)?,
        },
    };
    let job_sweep = match row.try_get::<Option<DateTime<Utc>>, _>("job_creation_cutoff")? {
        None => None,
        Some(creation_cutoff) => Some(JobSweepContinuation {
            creation_cutoff,
            upper_bound: JobCreationKey {
                created_at: row.try_get("job_upper_created_at")?,
                job_id: nominal(row.try_get("job_upper_id")?)?,
            },
            after: match row.try_get::<Option<DateTime<Utc>>, _>("job_cursor_created_at")? {
                None => None,
                Some(created_at) => Some(JobCreationKey {
                    created_at,
                    job_id: nominal(row.try_get("job_cursor_id")?)?,
                }),
            },
        }),
    };
    let state = TenantSchedulerState {
        schema_version: SCHEDULER_STATE_VERSION,
        tenant_id: nominal(row.try_get("tenant_id")?)?,
        work_class,
        partition_id: SchedulerPartitionId(
            u8::try_from(row.try_get::<i16, _>("partition_id")?).map_err(corrupt)?,
        ),
        policy,
        deficit: counter(row.try_get("deficit")?)?,
        earliest_eligible_round: counter(row.try_get("earliest_eligible_round")?)?,
        credited_round: optional_counter(row.try_get("credited_round")?)?,
        last_served_round: optional_counter(row.try_get("last_served_round")?)?,
        successful_claims: counter(row.try_get("successful_claims")?)?,
        job_sweep,
    };
    state.validate(maximum_deficit).map_err(corrupt)?;
    Ok(LockedTenantFairness {
        version: row.try_get("version")?,
        state,
    })
}

/// The main sweep includes ineligible rows: due/priority/quota changes never move
/// its key. A statement clock cutoff excludes later-created work. A late commit
/// behind the persisted cursor is revisited on the next finite sweep.
pub(crate) async fn scan_job_cohort(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &LockedTenantFairness,
    maximum_rows: u16,
) -> Result<JobCohortPage, RepositoryError> {
    if maximum_rows == 0 {
        return Err(RepositoryError::InvalidInput(
            "empty Job scan window".into(),
        ));
    }
    let state = &tenant.state;
    let sweep = match &state.job_sweep {
        Some(sweep) => sweep.clone(),
        None => {
            let cutoff: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
                .fetch_one(&mut **tx)
                .await?;
            let row = sqlx::query("SELECT created_at, job_id FROM insight_platform.jobs WHERE tenant_id=$1 AND work_class=$2 AND scheduler_partition_id=$3 AND terminal_at IS NULL AND created_at < $4 ORDER BY created_at DESC, job_id DESC LIMIT 1")
                .bind(state.tenant_id.to_string()).bind(state.work_class.as_str())
                .bind(i16::from(state.partition_id.0)).bind(cutoff).fetch_optional(&mut **tx).await?;
            let Some(row) = row else {
                return Ok(JobCohortPage {
                    jobs: Vec::new(),
                    diagnostics: Vec::new(),
                    next_sweep: None,
                });
            };
            JobSweepContinuation {
                creation_cutoff: cutoff,
                upper_bound: JobCreationKey {
                    created_at: row.try_get("created_at")?,
                    job_id: nominal(row.try_get("job_id")?)?,
                },
                after: None,
            }
        }
    };
    let rows = sqlx::query("SELECT * FROM insight_platform.jobs WHERE tenant_id=$1 AND work_class=$2 AND scheduler_partition_id=$3 AND terminal_at IS NULL AND created_at < $4 AND (created_at,job_id) <= ($5,$6) AND ($7::timestamptz IS NULL OR (created_at,job_id) > ($7,$8)) ORDER BY created_at,job_id LIMIT $9")
        .bind(state.tenant_id.to_string()).bind(state.work_class.as_str()).bind(i16::from(state.partition_id.0))
        .bind(sweep.creation_cutoff).bind(sweep.upper_bound.created_at).bind(sweep.upper_bound.job_id.to_string())
        .bind(sweep.after.as_ref().map(|key|key.created_at)).bind(sweep.after.as_ref().map(|key|key.job_id.to_string()))
        .bind(i64::from(maximum_rows)+1).fetch_all(&mut **tx).await?;
    let exhausted = rows.len() <= usize::from(maximum_rows);
    let mut jobs = Vec::new();
    let mut diagnostics = Vec::new();
    let mut after = sweep.after.clone();
    for row in rows.into_iter().take(usize::from(maximum_rows)) {
        after = Some(JobCreationKey {
            created_at: row.try_get("created_at")?,
            job_id: nominal(row.try_get("job_id")?)?,
        });
        if let Some(job) =
            crate::recovery_isolation::collect(persisted_job_from_row(row), &mut diagnostics)?
        {
            jobs.push(job);
        }
    }
    let next_sweep = if exhausted {
        None
    } else {
        Some(JobSweepContinuation { after, ..sweep })
    };
    Ok(JobCohortPage {
        jobs,
        diagnostics,
        next_sweep,
    })
}

pub(crate) async fn persist_admission(
    tx: &mut Transaction<'_, Postgres>,
    partition: &LockedPartition,
    tenants: &[LockedTenantFairness],
    decision: &PartitionAdmissionDecision,
) -> Result<(), RepositoryError> {
    if tenants.len() != decision.next_tenants.len() {
        return Err(corrupt("fairness decision omitted a visited tenant"));
    }
    for (locked, next) in tenants.iter().zip(&decision.next_tenants) {
        if locked.state.tenant_id != next.tenant_id
            || locked.state.work_class != next.work_class
            || locked.state.policy != next.policy
        {
            return Err(corrupt(
                "admission cannot change fairness ownership or policy",
            ));
        }
        // The partition records the visit. Rewriting unchanged tenant state can
        // turn a short empty scan into an SSI writer against another partition.
        if locked.state == *next {
            continue;
        }
        let sweep = next.job_sweep.as_ref();
        let affected = sqlx::query("UPDATE insight_platform.scheduler_tenant_state SET version=version+1,deficit=$4,credited_round=$5,last_served_round=$6,successful_claims=$7,job_creation_cutoff=$8,job_upper_created_at=$9,job_upper_id=$10,job_cursor_created_at=$11,job_cursor_id=$12,updated_at=clock_timestamp() WHERE tenant_id=$1 AND work_class=$2 AND version=$3")
            .bind(next.tenant_id.to_string()).bind(next.work_class.as_str()).bind(locked.version)
            .bind(next.deficit as i64).bind(next.credited_round.map(|round|round as i64)).bind(next.last_served_round.map(|round|round as i64))
            .bind(next.successful_claims as i64).bind(sweep.map(|s|s.creation_cutoff)).bind(sweep.map(|s|s.upper_bound.created_at))
            .bind(sweep.map(|s|s.upper_bound.job_id.to_string())).bind(sweep.and_then(|s|s.after.as_ref()).map(|k|k.created_at))
            .bind(sweep.and_then(|s|s.after.as_ref()).map(|k|k.job_id.to_string())).execute(&mut **tx).await?.rows_affected();
        if affected != 1 {
            return Err(RepositoryError::Conflict("tenant scheduler state"));
        }
    }
    let next = &decision.next_partition;
    if next.work_class != partition.state.work_class
        || next.partition_id != partition.state.partition_id
    {
        return Err(corrupt("partition decision changed ownership"));
    }
    let affected = sqlx::query("UPDATE insight_platform.scheduler_state SET version=version+1,current_round=$4,tenant_upper_bound=$5,cursor_tenant_id=$6,updated_at=clock_timestamp() WHERE work_class=$1 AND partition_id=$2 AND version=$3")
        .bind(next.work_class.as_str()).bind(i16::from(next.partition_id.0)).bind(partition.version).bind(next.current_round as i64)
        .bind(next.tenant_upper_bound.as_ref().map(ToString::to_string)).bind(next.cursor_tenant_id.as_ref().map(ToString::to_string))
        .execute(&mut **tx).await?.rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict("partition scheduler state"));
    }
    Ok(())
}

/// Call before inserting the Tenant, or acquiring any existing Tenant/owner lock.
/// The returned rounds freeze enrollment into the following round.
pub(crate) async fn lock_tenant_enrollment(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
) -> Result<Vec<(WorkClass, u64)>, RepositoryError> {
    let partition = SchedulerPartitionId::for_tenant(tenant_id).map_err(corrupt)?;
    let rows = sqlx::query("SELECT work_class,current_round FROM insight_platform.scheduler_state WHERE partition_id=$1 ORDER BY work_class FOR UPDATE")
        .bind(i16::from(partition.0)).fetch_all(&mut **tx).await?;
    if rows.len() != WorkClass::ALL.len() {
        return Err(corrupt("scheduler work classes were not provisioned"));
    }
    rows.into_iter()
        .map(|row| {
            let work_class = row
                .try_get::<String, _>("work_class")?
                .parse()
                .map_err(corrupt)?;
            let round = counter(row.try_get("current_round")?)?
                .checked_add(1)
                .filter(|v| *v <= i64::MAX as u64)
                .ok_or_else(|| corrupt("scheduler round exhausted"))?;
            Ok((work_class, round))
        })
        .collect()
}

pub(crate) async fn provision_tenant_fairness(
    tx: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    enrollment: &[(WorkClass, u64)],
) -> Result<(), RepositoryError> {
    if enrollment.len() != WorkClass::ALL.len() {
        return Err(corrupt("incomplete tenant enrollment"));
    }
    let partition = SchedulerPartitionId::for_tenant(tenant_id).map_err(corrupt)?;
    for (class, round) in enrollment {
        sqlx::query("INSERT INTO insight_platform.scheduler_tenant_state (tenant_id,work_class,partition_id,earliest_eligible_round) VALUES ($1,$2,$3,$4)")
            .bind(tenant_id.to_string()).bind(class.as_str()).bind(i16::from(partition.0)).bind(*round as i64)
            .execute(&mut **tx).await?;
    }
    Ok(())
}

#[cfg(test)]
static CONTROLLER_FIXTURE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[cfg(test)]
#[path = "partition_scheduler/claim_hint_tests.rs"]
mod claim_hint_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{ResourceKind, TypedPayload};
    use insight_platform_scheduler::partitioned::{
        select_admissible_partition_batch, LockedTenantVisit, PartitionSchedulerLimits,
    };
    use sqlx::Acquire;
    use std::collections::{BTreeMap, BTreeSet};

    #[tokio::test]
    async fn job_creation_is_independent_of_scheduler_accounting_and_work_classes_are_closed() {
        let _fixture_lock = CONTROLLER_FIXTURE_LOCK.lock().await;
        use crate::repository::{NewTenant, PgRepository};
        use insight_platform_contracts::TenantConfig;
        let database_url = std::env::var("PLATFORM_TEST_CONTROLLER_TRANSACTION_DATABASE_URL")
            .expect("real controller transaction fixture requires PLATFORM_TEST_CONTROLLER_TRANSACTION_DATABASE_URL");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&database_url)
            .await
            .unwrap();
        crate::verify_schema(&pool).await.unwrap();
        let repository = PgRepository::new(pool.clone());
        let tenant = ResourceId::from_uuid_v7(ResourceKind::Tenant, uuid::Uuid::now_v7()).unwrap();
        repository
            .create_tenant(NewTenant {
                tenant_id: tenant.to_string(),
                state: "active".into(),
                config: TenantConfig::default(),
            })
            .await
            .unwrap();
        let enrolled: Vec<String> = sqlx::query_scalar(
            "SELECT work_class FROM insight_platform.scheduler_tenant_state WHERE tenant_id=$1",
        )
        .bind(tenant.to_string())
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            enrolled.into_iter().collect::<BTreeSet<_>>(),
            WorkClass::ALL.iter().map(ToString::to_string).collect()
        );

        // The controller has a real SERIALIZABLE snapshot while the scheduler observes
        // Jobs and commits a credit update. A Job never references that mutable credit row.
        let mut controller = pool.begin().await.unwrap();
        sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .execute(&mut *controller)
            .await
            .unwrap();
        let _: i64 =
            sqlx::query_scalar("SELECT count(*) FROM insight_platform.tenants WHERE tenant_id=$1")
                .bind(tenant.to_string())
                .fetch_one(&mut *controller)
                .await
                .unwrap();
        let mut scheduler = pool.begin().await.unwrap();
        sqlx::query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .execute(&mut *scheduler)
            .await
            .unwrap();
        let _: i64 = sqlx::query_scalar("SELECT count(*) FROM insight_platform.jobs WHERE tenant_id=$1 AND work_class='interaction'")
            .bind(tenant.to_string()).fetch_one(&mut *scheduler).await.unwrap();
        sqlx::query("UPDATE insight_platform.scheduler_tenant_state SET version=version+1 WHERE tenant_id=$1 AND work_class='interaction'")
            .bind(tenant.to_string()).execute(&mut *scheduler).await.unwrap();
        scheduler.commit().await.unwrap();
        let payload = TypedPayload::empty(1).unwrap();
        let execution = crate::execution_requirements::StoredExecutionRequirement::new(
            &insight_platform_contracts::ExecutionRequirement::DomainOperation {
                operation_abi_identity: payload.digest.parse().unwrap(),
                requirements: insight_platform_contracts::DomainOperationRequirements::Control {
                    control_policy_digest: payload.digest.parse().unwrap(),
                },
            },
        )
        .unwrap();
        let job = ResourceId::from_uuid_v7(ResourceKind::Job, uuid::Uuid::now_v7()).unwrap();
        let partition = SchedulerPartitionId::for_tenant(&tenant).unwrap();
        sqlx::query("INSERT INTO insight_platform.jobs (tenant_id,job_id,job_kind,work_class,owner_kind,owner_id,trace_id,state,attempt_limit,scheduled_at,deadline,request_digest,payload_schema_version,payload,payload_digest,scheduler_partition_id,execution_requirement_version,execution_requirement,execution_requirement_digest) VALUES ($1,$2,'interaction','interaction','interaction',$3,'0123456789abcdef0123456789abcdef','ready',3,clock_timestamp(),clock_timestamp()+interval '1 day',$4,1,$5,$4,$6,1,$7,$8)")
            .bind(tenant.to_string()).bind(job.to_string())
            .bind(ResourceId::from_uuid_v7(ResourceKind::Interaction, uuid::Uuid::now_v7()).unwrap().to_string())
            .bind(&payload.digest).bind(&payload.value).bind(i16::from(partition.0))
            .bind(&execution.value).bind(&execution.digest).execute(&mut *controller).await.unwrap();
        controller.commit().await.unwrap();

        for class in WorkClass::ALL {
            let mut tx = pool.begin().await.unwrap();
            sqlx::query(
                "UPDATE insight_platform.jobs SET work_class=$3 WHERE tenant_id=$1 AND job_id=$2",
            )
            .bind(tenant.to_string())
            .bind(job.to_string())
            .bind(class.as_str())
            .execute(&mut *tx)
            .await
            .unwrap();
            tx.rollback().await.unwrap();
        }
        for (column, value, expected_code, expected_constraint) in [
            ("work_class", "unregistered", "23514", "jobs_work_class_ck"),
            (
                "scheduler_partition_id",
                "-1",
                "23503",
                "jobs_scheduler_route_fk",
            ),
        ] {
            let mut tx = pool.begin().await.unwrap();
            let sql = match column {
                "work_class" => "UPDATE insight_platform.jobs SET work_class=$3 WHERE tenant_id=$1 AND job_id=$2",
                _ => "UPDATE insight_platform.jobs SET scheduler_partition_id=$3::text::smallint WHERE tenant_id=$1 AND job_id=$2",
            };
            let failure = sqlx::query(sql)
                .bind(tenant.to_string())
                .bind(job.to_string())
                .bind(value)
                .execute(&mut *tx)
                .await
                .unwrap_err();
            assert!(
                matches!(failure, sqlx::Error::Database(ref error) if error.code().as_deref()==Some(expected_code) && error.constraint()==Some(expected_constraint))
            );
            tx.rollback().await.unwrap();
        }
    }

    #[tokio::test]
    async fn empty_partitions_are_read_only_and_new_enrollment_joins_without_starvation() {
        let url = std::env::var("PLATFORM_TEST_PARTITION_SCHEDULER_DATABASE_URL")
            .expect("PLATFORM_TEST_PARTITION_SCHEDULER_DATABASE_URL is required");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        let mut tx = pool.begin().await.unwrap();
        sqlx::raw_sql(crate::CURRENT_SCHEMA_SQL)
            .execute(&mut *tx)
            .await
            .unwrap();
        assert!(lock_partition(&mut tx, WorkClass::Interaction)
            .await
            .unwrap()
            .is_none());
        let changed: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM insight_platform.scheduler_state WHERE version <> 1",
        )
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        assert_eq!(changed, 0);
        let payload = TypedPayload::empty(1).unwrap();
        let first = ResourceId::from_uuid_v7(ResourceKind::Tenant, uuid::Uuid::now_v7()).unwrap();
        let first_partition = SchedulerPartitionId::for_tenant(&first).unwrap();
        let second = loop {
            let next =
                ResourceId::from_uuid_v7(ResourceKind::Tenant, uuid::Uuid::now_v7()).unwrap();
            if SchedulerPartitionId::for_tenant(&next).unwrap() != first_partition {
                break next;
            }
        };
        let mut expected = BTreeSet::new();
        for tenant in [first, second] {
            let enrollment = lock_tenant_enrollment(&mut tx, &tenant).await.unwrap();
            let partition = SchedulerPartitionId::for_tenant(&tenant).unwrap();
            sqlx::query("INSERT INTO insight_platform.tenants (tenant_id,state,config_digest,scheduler_partition_id) VALUES ($1,'active',$2,$3)").bind(tenant.to_string()).bind(&payload.digest).bind(i16::from(partition.0)).execute(&mut *tx).await.unwrap();
            provision_tenant_fairness(&mut tx, &tenant, &enrollment)
                .await
                .unwrap();
            expected.insert(tenant);
            let mut visited = BTreeSet::new();
            for _ in 0..8 {
                let locked = lock_partition(&mut tx, WorkClass::Interaction)
                    .await
                    .unwrap()
                    .unwrap();
                let window = lock_tenant_window(&mut tx, &locked, 8, 100).await.unwrap();
                let visits = window
                    .tenants
                    .iter()
                    .map(|tenant| {
                        visited.insert(tenant.state.tenant_id.clone());
                        assert_eq!(tenant.state.policy, SchedulingPolicyBinding::Unbound);
                        LockedTenantVisit {
                            state: tenant.state.clone(),
                            business_policy: None,
                            candidates: vec![],
                            next_job_sweep: None,
                        }
                    })
                    .collect::<Vec<_>>();
                let decision = select_admissible_partition_batch(
                    &locked.state,
                    &visits,
                    &BTreeMap::new(),
                    PartitionSchedulerLimits {
                        maximum_deficit: 100,
                        maximum_tenant_window: 8,
                        maximum_candidates_per_tenant: 1,
                        maximum_claims: 1,
                        maximum_control_claims_per_tenant: 1,
                        maximum_quota_lines_per_candidate: 1,
                    },
                    1,
                    window.range_exhausted,
                )
                .unwrap();
                assert!(decision.admitted_job_ids.is_empty());
                persist_admission(&mut tx, &locked, &window.tenants, &decision)
                    .await
                    .unwrap();
            }
            assert_eq!(
                visited, expected,
                "an unbound existing tenant must not hide newly enrolled tenants"
            );
        }
        let untouched: i64 = sqlx::query_scalar("SELECT count(*) FROM insight_platform.scheduler_state AS partition WHERE partition.version <> 1 AND NOT EXISTS (SELECT 1 FROM insight_platform.scheduler_tenant_state AS tenant WHERE tenant.partition_id=partition.partition_id AND tenant.work_class=partition.work_class)").fetch_one(&mut *tx).await.unwrap();
        assert_eq!(untouched, 0);
        tx.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn job_sweep_yields_across_transactions_and_excludes_new_creation() {
        let url = std::env::var("PLATFORM_TEST_PARTITION_SCHEDULER_DATABASE_URL")
            .expect("PLATFORM_TEST_PARTITION_SCHEDULER_DATABASE_URL is required");
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        let mut tx = pool.begin().await.unwrap();
        sqlx::raw_sql(crate::CURRENT_SCHEMA_SQL)
            .execute(&mut *tx)
            .await
            .unwrap();
        let tenant: ResourceId = "ten_01951f3d-7b80-7b81-8d22-841bcc458faa".parse().unwrap();
        let payload = TypedPayload::new(1, &serde_json::json!({})).unwrap();
        let partition = SchedulerPartitionId::for_tenant(&tenant).unwrap();
        sqlx::query("INSERT INTO insight_platform.tenants (tenant_id,state,config_digest,scheduler_partition_id) VALUES ($1,'active',$2,$3)")
            .bind(tenant.to_string()).bind(&payload.digest).bind(i16::from(partition.0)).execute(&mut *tx).await.unwrap();
        provision_tenant_fairness(
            &mut tx,
            &tenant,
            &WorkClass::ALL
                .iter()
                .map(|class| (*class, 0))
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        sqlx::query("UPDATE insight_platform.scheduler_state SET updated_at=clock_timestamp()+interval '1 day' WHERE partition_id<>$1")
            .bind(i16::from(partition.0)).execute(&mut *tx).await.unwrap();
        let execution = crate::execution_requirements::StoredExecutionRequirement::new(
            &insight_platform_contracts::ExecutionRequirement::DomainOperation {
                operation_abi_identity: payload.digest.parse().unwrap(),
                requirements: insight_platform_contracts::DomainOperationRequirements::Control {
                    control_policy_digest: payload.digest.parse().unwrap(),
                },
            },
        )
        .unwrap();
        let insert="INSERT INTO insight_platform.jobs (tenant_id,job_id,job_kind,work_class,owner_kind,owner_id,trace_id,state,attempt_limit,scheduled_at,deadline,request_digest,payload_schema_version,payload,payload_digest,scheduler_partition_id,execution_requirement_version,execution_requirement,execution_requirement_digest) VALUES ($1,$2,'interaction','interaction','interaction',$3,'0123456789abcdef0123456789abcdef','ready',3,clock_timestamp(),clock_timestamp()+interval '1 day',$4,1,$5,$4,$6,1,$7,$8)";
        let mut original = BTreeSet::new();
        for _ in 0..23 {
            let job = ResourceId::from_uuid_v7(ResourceKind::Job, uuid::Uuid::now_v7()).unwrap();
            original.insert(job.to_string());
            sqlx::query(insert)
                .bind(tenant.to_string())
                .bind(job.to_string())
                .bind(
                    ResourceId::from_uuid_v7(ResourceKind::Interaction, uuid::Uuid::now_v7())
                        .unwrap()
                        .to_string(),
                )
                .bind(&payload.digest)
                .bind(&payload.value)
                .bind(i16::from(partition.0))
                .bind(&execution.value)
                .bind(&execution.digest)
                .execute(&mut *tx)
                .await
                .unwrap();
        }
        let mut observed = BTreeSet::new();
        let mut late = None;
        let limits = PartitionSchedulerLimits {
            maximum_deficit: 100,
            maximum_tenant_window: 1,
            maximum_candidates_per_tenant: 3,
            maximum_claims: 1,
            maximum_control_claims_per_tenant: 1,
            maximum_quota_lines_per_candidate: 1,
        };
        for page_index in 0..8 {
            // New handles read only persisted cursor/accounting. Nested savepoint
            // commits exercise the same transaction-visible reload as a restart.
            let mut visit_tx = tx.begin().await.unwrap();
            let locked = lock_partition(&mut visit_tx, WorkClass::Interaction)
                .await
                .unwrap()
                .unwrap();
            let window = lock_tenant_window(&mut visit_tx, &locked, 1, 100)
                .await
                .unwrap();
            assert_eq!(window.tenants.len(), 1);
            let page = scan_job_cohort(&mut visit_tx, &window.tenants[0], 3)
                .await
                .unwrap();
            for job in &page.jobs {
                assert!(
                    observed.insert(job.job_id.clone()),
                    "due-key changes must not duplicate a sweep row"
                );
            }
            let exhausted = page.next_sweep.is_none();
            let decision = select_admissible_partition_batch(
                &locked.state,
                &[LockedTenantVisit {
                    state: window.tenants[0].state.clone(),
                    business_policy: None,
                    candidates: Vec::new(),
                    next_job_sweep: page.next_sweep,
                }],
                &BTreeMap::new(),
                limits,
                1,
                window.range_exhausted,
            )
            .unwrap();
            persist_admission(&mut visit_tx, &locked, &window.tenants, &decision)
                .await
                .unwrap();
            visit_tx.commit().await.unwrap();
            if page_index == 0 {
                let job =
                    ResourceId::from_uuid_v7(ResourceKind::Job, uuid::Uuid::now_v7()).unwrap();
                late = Some(job.to_string());
                sqlx::query(insert)
                    .bind(tenant.to_string())
                    .bind(job.to_string())
                    .bind(
                        ResourceId::from_uuid_v7(ResourceKind::Interaction, uuid::Uuid::now_v7())
                            .unwrap()
                            .to_string(),
                    )
                    .bind(&payload.digest)
                    .bind(&payload.value)
                    .bind(i16::from(partition.0))
                    .bind(&execution.value)
                    .bind(&execution.digest)
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                sqlx::query("UPDATE insight_platform.jobs SET scheduled_at=clock_timestamp()+interval '1 hour' WHERE job_id=$1")
                    .bind(original.first().unwrap()).execute(&mut *tx).await.unwrap();
            }
            assert_eq!(
                exhausted,
                page_index == 7,
                "per-page budget must not reset the sweep"
            );
        }
        assert_eq!(observed, original);
        assert!(!observed.contains(late.as_ref().unwrap()));
        let persisted:Option<DateTime<Utc>>=sqlx::query_scalar("SELECT job_creation_cutoff FROM insight_platform.scheduler_tenant_state WHERE tenant_id=$1 AND work_class='interaction'")
            .bind(tenant.to_string()).fetch_one(&mut *tx).await.unwrap();
        assert!(persisted.is_none());
        // Simulate authority corruption only inside this rolled-back fixture. A missing
        // enrollment must not make admission invent a default credit row or lease Jobs.
        sqlx::query("DELETE FROM insight_platform.scheduler_tenant_state WHERE tenant_id=$1 AND work_class='interaction'")
            .bind(tenant.to_string()).execute(&mut *tx).await.unwrap();
        assert!(lock_partition(&mut tx, WorkClass::Interaction)
            .await
            .unwrap()
            .is_none());
        let ready: i64 = sqlx::query_scalar("SELECT count(*) FROM insight_platform.jobs WHERE tenant_id=$1 AND state='ready' AND worker_id IS NULL")
            .bind(tenant.to_string()).fetch_one(&mut *tx).await.unwrap();
        assert_eq!(ready, i64::try_from(original.len() + 1).unwrap());
        tx.rollback().await.unwrap();
    }
}
