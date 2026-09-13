//! Execution details come from current rows and exact value references, never events.
use super::*;
use insight_platform_orchestrator::store::{
    RunExecutionDetailRecord, RunExecutionState, RunValueMetadataRecord,
};
impl PgRepository {
    pub async fn read_run_execution_for_principal(
        &self,
        tenant: &ResourceId,
        principal: &ResourceId,
        principal_kind: PrincipalKind,
        run: &ResourceId,
        source_kind: insight_platform_contracts::PublicRunEventSourceKind,
        source: &ResourceId,
    ) -> Result<RunExecutionDetailRecord, RepositoryError> {
        use insight_platform_contracts::PublicRunEventSourceKind as K;
        let model = match source_kind {
            K::NodeExecution if source.kind() == ResourceKind::NodeExecution => false,
            K::ModelTurn if source.kind() == ResourceKind::ModelTurn => true,
            _ => return Err(RepositoryError::NotFound("execution")),
        };
        if run.kind() != ResourceKind::Run {
            return Err(RepositoryError::NotFound("Run"));
        }
        let mut tx = begin_read_only_repeatable(&self.pool).await?;
        sqlx::query("SET LOCAL statement_timeout='8s'")
            .execute(&mut *tx)
            .await?;
        let principal =
            load_current_principal_snapshot(&mut tx, tenant, principal, principal_kind).await?;
        if !principal.permissions.contains(Permission::RuntimeRead) {
            return Err(RepositoryError::PermissionDenied);
        }
        load_run(&mut tx, tenant, run).await?;
        let row=if model {
   sqlx::query("SELECT i.node_id,n.plan_node_key,n.node_kind,i.state,i.version,i.started_at,i.terminal_at,i.input_value_id,i.output_value_id FROM insight_platform.invocations i JOIN insight_platform.run_nodes n ON n.tenant_id=i.tenant_id AND n.run_id=i.run_id AND n.node_id=i.node_id AND n.record_kind='node_execution' WHERE i.tenant_id=$1 AND i.run_id=$2 AND i.invocation_id=$3 AND i.invocation_kind='model'")
  }else{
   sqlx::query("SELECT node_id,plan_node_key,node_kind,state,version,started_at,terminal_at,NULL::text AS input_value_id,NULL::text AS output_value_id FROM insight_platform.run_nodes WHERE tenant_id=$1 AND run_id=$2 AND node_id=$3 AND record_kind='node_execution'")
  }.bind(tenant.to_string()).bind(run.to_string()).bind(source.to_string()).fetch_optional(&mut *tx).await?.ok_or(RepositoryError::NotFound("execution"))?;
        let corrupt = || RepositoryError::CorruptRow("execution identity or state".into());
        let node: ResourceId = ResourceId::parse_expected(
            &row.try_get::<String, _>("node_id")?,
            ResourceKind::NodeExecution,
        )
        .map_err(|_| corrupt())?;
        let parse_value = |field: &str| -> Result<Option<ResourceId>, RepositoryError> {
            row.try_get::<Option<String>, _>(field)?
                .map(|s| {
                    ResourceId::parse_expected(&s, ResourceKind::RunValue).map_err(|_| corrupt())
                })
                .transpose()
        };
        let input = parse_value("input_value_id")?;
        let output = parse_value("output_value_id")?;
        let mut rows=sqlx::query("SELECT value_id,node_id,classification,schema_digest,content_digest,(artifact_id IS NOT NULL) AS has_artifact FROM insight_platform.run_values WHERE tenant_id=$1 AND run_id=$2 AND (($3::boolean AND value_id=ANY($4::text[])) OR (NOT $3 AND node_id=$5)) ORDER BY created_at DESC,value_id DESC LIMIT 65")
   .bind(tenant.to_string()).bind(run.to_string()).bind(model).bind([input.as_ref(),output.as_ref()].into_iter().flatten().map(ToString::to_string).collect::<Vec<_>>()).bind(node.to_string()).fetch_all(&mut *tx).await?;
        let truncated = rows.len() > 64;
        if truncated {
            rows.pop();
        }
        let mut values = Vec::new();
        for v in rows {
            values.push(RunValueMetadataRecord {
                node_id: v
                    .try_get::<Option<String>, _>("node_id")?
                    .map(|s| {
                        ResourceId::parse_expected(&s, ResourceKind::NodeExecution)
                            .map_err(|_| corrupt())
                    })
                    .transpose()?,
                run_id: run.clone(),
                value_id: ResourceId::parse_expected(
                    &v.try_get::<String, _>("value_id")?,
                    ResourceKind::RunValue,
                )
                .map_err(|_| corrupt())?,
                classification: v
                    .try_get::<String, _>("classification")?
                    .parse()
                    .map_err(|_| corrupt())?,
                schema_digest: v
                    .try_get::<String, _>("schema_digest")?
                    .parse()
                    .map_err(|_| corrupt())?,
                content_digest: v
                    .try_get::<String, _>("content_digest")?
                    .parse()
                    .map_err(|_| corrupt())?,
                storage_kind: if v.try_get("has_artifact")? {
                    insight_platform_contracts::RunValueStorageKind::Artifact
                } else {
                    insight_platform_contracts::RunValueStorageKind::Inline
                },
            });
        }
        if [input.as_ref(), output.as_ref()]
            .into_iter()
            .flatten()
            .any(|id| !values.iter().any(|v| v.value_id == *id))
        {
            return Err(corrupt());
        }
        let state: String = row.try_get("state")?;
        let result = RunExecutionDetailRecord {
            run_id: run.clone(),
            source_kind,
            source_id: source.clone(),
            version: u64::try_from(row.try_get::<i64, _>("version")?)
                .ok()
                .filter(|v| *v > 0)
                .ok_or_else(corrupt)?,
            state: if model {
                RunExecutionState::Model(state.parse().map_err(|_| corrupt())?)
            } else {
                RunExecutionState::Node(state.parse().map_err(|_| corrupt())?)
            },
            node_execution_id: node,
            plan_node_key: row.try_get("plan_node_key")?,
            node_kind: row.try_get("node_kind")?,
            started_at: row.try_get("started_at")?,
            terminal_at: row.try_get("terminal_at")?,
            input_value_id: input,
            output_value_id: output,
            values,
            values_truncated: truncated,
        };
        tx.commit().await?;
        Ok(result)
    }
}
