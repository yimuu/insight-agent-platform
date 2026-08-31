//! Direct-authority product list queries.
//!
//! These projections intentionally read the shared Resource, Deployment, Run, Job, and Task
//! aggregates in one read-only repeatable-read transaction. They do not persist a list head or
//! rebuild current state from Events.

use crate::repository::{
    begin_read_only_repeatable, load_current_principal_snapshot, payload_from_row,
    DeploymentRecord, PgRepository, RepositoryError, ResourceRecord, TypedPayload,
};
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    AgentProductState, Permission, PrincipalKind, ResourceId, ResourceKind, RunState,
};
use sqlx::Row;

pub const PRODUCT_AUTHORITY_MAX_FETCH: u16 = 51;

#[derive(Debug, Clone)]
pub struct AgentProductListQuery {
    pub tenant_id: ResourceId,
    pub principal_id: ResourceId,
    pub principal_kind: PrincipalKind,
    pub state: Option<AgentProductState>,
    pub environment: Option<String>,
    pub snapshot_at: Option<DateTime<Utc>>,
    pub boundary: Option<(DateTime<Utc>, ResourceId)>,
    pub fetch_limit: u16,
}

#[derive(Debug, Clone)]
pub struct RunProductListQuery {
    pub tenant_id: ResourceId,
    pub principal_id: ResourceId,
    pub principal_kind: PrincipalKind,
    pub agent_id: Option<ResourceId>,
    pub state: Option<RunState>,
    pub created_after: Option<DateTime<Utc>>,
    pub created_before: Option<DateTime<Utc>>,
    pub snapshot_at: Option<DateTime<Utc>>,
    pub boundary: Option<(DateTime<Utc>, ResourceId)>,
    pub fetch_limit: u16,
}

#[derive(Debug, Clone)]
pub struct AuthorityRecordPage<T> {
    pub snapshot_at: DateTime<Utc>,
    pub records: Vec<T>,
}

#[derive(Debug, Clone)]
pub struct AgentProductRecord {
    pub resource: ResourceRecord,
    pub active_deployment: Option<DeploymentRecord>,
    pub product_state: AgentProductState,
    pub latest_run_state: Option<RunState>,
}

#[derive(Debug, Clone)]
pub struct RunProductRecord {
    pub run_id: ResourceId,
    pub agent_id: ResourceId,
    pub agent_resource_payload: TypedPayload,
    pub state: RunState,
    pub started_at: Option<DateTime<Utc>>,
    pub terminal_at: Option<DateTime<Utc>>,
    pub waiting_task_count: u32,
    pub result_available: bool,
    pub created_at: DateTime<Utc>,
}

impl PgRepository {
    pub async fn list_agent_products(
        &self,
        query: AgentProductListQuery,
    ) -> Result<AuthorityRecordPage<AgentProductRecord>, RepositoryError> {
        validate_agent_query(&query)?;
        let mut transaction = begin_read_only_repeatable(self.pool()).await?;
        let principal = load_current_principal_snapshot(
            &mut transaction,
            &query.tenant_id,
            &query.principal_id,
            query.principal_kind,
        )
        .await?;
        if !principal.permissions.contains(Permission::AgentRead) {
            return Err(RepositoryError::PermissionDenied);
        }
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT transaction_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let snapshot_at = query.snapshot_at.unwrap_or(database_now);
        if snapshot_at > database_now {
            return Err(RepositoryError::InvalidInput(
                "Agent list snapshot is in the future".to_owned(),
            ));
        }
        let (boundary_at, boundary_id) = match query.boundary {
            Some((at, id)) => (Some(at), Some(id.to_string())),
            None => (None, None),
        };
        let rows = sqlx::query(
            r#"
            WITH agent_authority AS (
                SELECT resource.tenant_id, resource.resource_id, resource.resource_kind,
                       resource.lifecycle_state, resource.gate_state,
                       resource.draft_generation, resource.active_version_id,
                       resource.active_deployment_id, resource.version,
                       resource.payload_schema_version, resource.payload,
                       resource.payload_digest, resource.created_at, resource.updated_at,
                       deployment.deployment_id AS active_deployment_id_value,
                       deployment.resource_version_id AS active_resource_version_id,
                       deployment.environment AS active_environment,
                       deployment.payload_schema_version AS active_bindings_schema_version,
                       deployment.bindings AS active_bindings,
                       deployment.bindings_digest AS active_bindings_digest,
                       deployment.created_by AS active_created_by,
                       deployment.created_at AS active_created_at,
                       latest_run.state AS latest_run_state,
                       CASE
                           WHEN resource.lifecycle_state <> 'active'
                             OR resource.gate_state <> 'enabled' THEN 'blocked'
                           WHEN validation_job.state IN ('failed', 'cancelled', 'timed_out')
                             THEN 'blocked'
                           WHEN resource.active_deployment_id IS NOT NULL THEN 'ready'
                           WHEN validation_job.state IN (
                               'ready', 'leased', 'running', 'waiting', 'retry_scheduled',
                               'cancelling', 'reconciliation_required'
                           ) THEN 'validating'
                           WHEN resource.payload -> 'validation' <> 'null'::jsonb
                             THEN 'publishing'
                           ELSE 'draft'
                       END AS product_state
                FROM insight_platform.resources AS resource
                LEFT JOIN insight_platform.deployments AS deployment
                  ON deployment.tenant_id = resource.tenant_id
                 AND deployment.resource_id = resource.resource_id
                 AND deployment.deployment_id = resource.active_deployment_id
                LEFT JOIN LATERAL (
                    SELECT job.state
                    FROM insight_platform.jobs AS job
                    WHERE job.tenant_id = resource.tenant_id
                      AND job.job_kind = 'registry_validation'
                      AND job.work_class = 'registry_validation'
                      AND job.payload ->> 'resource_id' = resource.resource_id
                      AND job.payload ->> 'expected_resource_version' = resource.version::text
                    ORDER BY job.created_at DESC, job.job_id DESC
                    LIMIT 1
                ) AS validation_job ON true
                LEFT JOIN LATERAL (
                    SELECT run.state
                    FROM insight_platform.deployments AS run_deployment
                    JOIN insight_platform.runs AS run
                      ON run.tenant_id = run_deployment.tenant_id
                     AND run.agent_deployment_id = run_deployment.deployment_id
                    WHERE run_deployment.tenant_id = resource.tenant_id
                      AND run_deployment.resource_id = resource.resource_id
                    ORDER BY run.created_at DESC, run.run_id DESC
                    LIMIT 1
                ) AS latest_run ON true
                WHERE resource.tenant_id = $1
                  AND resource.resource_kind = 'agent'
                  AND resource.updated_at <= $2
            )
            SELECT *
            FROM agent_authority
            WHERE ($3::timestamptz IS NULL
                   OR updated_at < $3
                   OR (updated_at = $3 AND resource_id < $4))
              AND ($5::text IS NULL OR product_state = $5)
              AND ($6::text IS NULL OR active_environment = $6)
            ORDER BY updated_at DESC, resource_id DESC
            LIMIT $7
            "#,
        )
        .bind(query.tenant_id.to_string())
        .bind(snapshot_at)
        .bind(boundary_at)
        .bind(boundary_id)
        .bind(query.state.map(|state| state.as_str().to_owned()))
        .bind(query.environment)
        .bind(i64::from(query.fetch_limit))
        .fetch_all(&mut *transaction)
        .await?;

        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let resource = ResourceRecord {
                tenant_id: row.try_get("tenant_id")?,
                resource_id: row.try_get("resource_id")?,
                resource_kind: row.try_get("resource_kind")?,
                lifecycle_state: row.try_get("lifecycle_state")?,
                gate_state: row.try_get("gate_state")?,
                draft_generation: row.try_get("draft_generation")?,
                active_version_id: row.try_get("active_version_id")?,
                active_deployment_id: row.try_get("active_deployment_id")?,
                version: row.try_get("version")?,
                payload: payload_from_row(
                    &row,
                    "payload_schema_version",
                    "payload",
                    "payload_digest",
                )?,
                created_at: row.try_get("created_at")?,
                updated_at: row.try_get("updated_at")?,
            };
            let active_deployment =
                match row.try_get::<Option<String>, _>("active_deployment_id_value")? {
                    Some(deployment_id) => Some(DeploymentRecord {
                        tenant_id: resource.tenant_id.clone(),
                        deployment_id,
                        resource_id: resource.resource_id.clone(),
                        resource_version_id: row.try_get("active_resource_version_id")?,
                        environment: row.try_get("active_environment")?,
                        bindings: payload_from_row(
                            &row,
                            "active_bindings_schema_version",
                            "active_bindings",
                            "active_bindings_digest",
                        )?,
                        created_by: row.try_get("active_created_by")?,
                        created_at: row.try_get("active_created_at")?,
                    }),
                    None => None,
                };
            let product_state = row
                .try_get::<String, _>("product_state")?
                .parse::<AgentProductState>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            let latest_run_state = row
                .try_get::<Option<String>, _>("latest_run_state")?
                .map(|state| state.parse::<RunState>())
                .transpose()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            records.push(AgentProductRecord {
                resource,
                active_deployment,
                product_state,
                latest_run_state,
            });
        }
        transaction.commit().await?;
        Ok(AuthorityRecordPage {
            snapshot_at,
            records,
        })
    }

    pub async fn list_run_products(
        &self,
        query: RunProductListQuery,
    ) -> Result<AuthorityRecordPage<RunProductRecord>, RepositoryError> {
        validate_run_query(&query)?;
        let mut transaction = begin_read_only_repeatable(self.pool()).await?;
        let principal = load_current_principal_snapshot(
            &mut transaction,
            &query.tenant_id,
            &query.principal_id,
            query.principal_kind,
        )
        .await?;
        if !principal.permissions.contains(Permission::RuntimeRead) {
            return Err(RepositoryError::PermissionDenied);
        }
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT transaction_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        let snapshot_at = query.snapshot_at.unwrap_or(database_now);
        if snapshot_at > database_now
            || query
                .created_before
                .is_some_and(|created_before| created_before > snapshot_at)
        {
            return Err(RepositoryError::InvalidInput(
                "Run list snapshot or time window is invalid".to_owned(),
            ));
        }
        let (boundary_at, boundary_id) = match query.boundary {
            Some((at, id)) => (Some(at), Some(id.to_string())),
            None => (None, None),
        };
        let rows = sqlx::query(
            r#"
            SELECT run.run_id, run.state, run.started_at, run.terminal_at,
                   run.output_value_id, run.created_at,
                   resource.resource_id AS agent_id,
                   resource.payload_schema_version AS agent_payload_schema_version,
                   resource.payload AS agent_payload,
                   resource.payload_digest AS agent_payload_digest,
                   COALESCE(waiting_tasks.waiting_task_count, 0) AS waiting_task_count
            FROM insight_platform.runs AS run
            JOIN insight_platform.deployments AS deployment
              ON deployment.tenant_id = run.tenant_id
             AND deployment.deployment_id = run.agent_deployment_id
            JOIN insight_platform.resources AS resource
              ON resource.tenant_id = deployment.tenant_id
             AND resource.resource_id = deployment.resource_id
             AND resource.resource_kind = 'agent'
            LEFT JOIN LATERAL (
                SELECT count(*) AS waiting_task_count
                FROM insight_platform.tasks AS task
                WHERE task.tenant_id = run.tenant_id
                  AND task.run_id = run.run_id
                  AND task.state = 'pending'
                  AND task.responded_at IS NULL
            ) AS waiting_tasks ON true
            WHERE run.tenant_id = $1
              AND run.created_at <= $2
              AND ($3::timestamptz IS NULL
                   OR run.created_at < $3
                   OR (run.created_at = $3 AND run.run_id < $4))
              AND ($5::text IS NULL OR resource.resource_id = $5)
              AND ($6::text IS NULL OR run.state = $6)
              AND ($7::timestamptz IS NULL OR run.created_at > $7)
              AND ($8::timestamptz IS NULL OR run.created_at < $8)
            ORDER BY run.created_at DESC, run.run_id DESC
            LIMIT $9
            "#,
        )
        .bind(query.tenant_id.to_string())
        .bind(snapshot_at)
        .bind(boundary_at)
        .bind(boundary_id)
        .bind(query.agent_id.map(|id| id.to_string()))
        .bind(query.state.map(|state| state.as_str().to_owned()))
        .bind(query.created_after)
        .bind(query.created_before)
        .bind(i64::from(query.fetch_limit))
        .fetch_all(&mut *transaction)
        .await?;

        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let run_id = row
                .try_get::<String, _>("run_id")?
                .parse::<ResourceId>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            let agent_id = row
                .try_get::<String, _>("agent_id")?
                .parse::<ResourceId>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            let state = row
                .try_get::<String, _>("state")?
                .parse::<RunState>()
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            let waiting_task_count = u32::try_from(row.try_get::<i64, _>("waiting_task_count")?)
                .map_err(|_| RepositoryError::CorruptRow("Task count overflow".to_owned()))?;
            let terminal_at: Option<DateTime<Utc>> = row.try_get("terminal_at")?;
            let output_value_id: Option<String> = row.try_get("output_value_id")?;
            records.push(RunProductRecord {
                run_id,
                agent_id,
                agent_resource_payload: payload_from_row(
                    &row,
                    "agent_payload_schema_version",
                    "agent_payload",
                    "agent_payload_digest",
                )?,
                state,
                started_at: row.try_get("started_at")?,
                terminal_at,
                waiting_task_count,
                result_available: terminal_at.is_some() && output_value_id.is_some(),
                created_at: row.try_get("created_at")?,
            });
        }
        transaction.commit().await?;
        Ok(AuthorityRecordPage {
            snapshot_at,
            records,
        })
    }
}

fn validate_agent_query(query: &AgentProductListQuery) -> Result<(), RepositoryError> {
    if query.tenant_id.kind() != ResourceKind::Tenant
        || query.principal_id.kind() != ResourceKind::Principal
        || query.fetch_limit == 0
        || query.fetch_limit > PRODUCT_AUTHORITY_MAX_FETCH
        || query
            .environment
            .as_deref()
            .is_some_and(|value| value.is_empty() || value.len() > 64)
        || query
            .boundary
            .as_ref()
            .is_some_and(|(_, id)| id.kind() != ResourceKind::Agent || query.snapshot_at.is_none())
    {
        return Err(RepositoryError::InvalidInput(
            "Agent product list query is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn validate_run_query(query: &RunProductListQuery) -> Result<(), RepositoryError> {
    if query.tenant_id.kind() != ResourceKind::Tenant
        || query.principal_id.kind() != ResourceKind::Principal
        || query.fetch_limit == 0
        || query.fetch_limit > PRODUCT_AUTHORITY_MAX_FETCH
        || query
            .agent_id
            .as_ref()
            .is_some_and(|id| id.kind() != ResourceKind::Agent)
        || matches!((query.created_after, query.created_before), (Some(after), Some(before)) if after >= before)
        || query
            .boundary
            .as_ref()
            .is_some_and(|(_, id)| id.kind() != ResourceKind::Run || query.snapshot_at.is_none())
    {
        return Err(RepositoryError::InvalidInput(
            "Run product list query is invalid".to_owned(),
        ));
    }
    Ok(())
}
