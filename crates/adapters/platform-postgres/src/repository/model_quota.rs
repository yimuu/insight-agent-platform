//! Public allocation commands operate on the same accounts locked by Model claim/settlement.
use super::*;
use insight_platform_contracts::{
    validate_model_quota_target, ModelQuotaAllocationV1, ModelQuotaCounterV1, ModelQuotaLimitsV1,
    ModelQuotaViewV1, QuotaDimension,
};
use insight_platform_registry::model_quota::SetModelQuota;
const METRICS: [QuotaDimension; 3] = [
    QuotaDimension::ModelRequests,
    QuotaDimension::ModelTokens,
    QuotaDimension::ModelCostMicrounits,
];
fn invalid() -> RepositoryError {
    RepositoryError::InvalidInput("model quota is invalid".into())
}
fn corrupt() -> RepositoryError {
    RepositoryError::CorruptRow("model quota account closure is invalid".into())
}

impl PgRepository {
    pub async fn read_model_quota_for_principal(
        &self,
        tenant: &ResourceId,
        principal: &ResourceId,
        kind: PrincipalKind,
        deployment: &ResourceId,
    ) -> Result<ModelQuotaViewV1, RepositoryError> {
        if tenant.kind() != ResourceKind::Tenant
            || deployment.kind() != ResourceKind::ModelDeployment
        {
            return Err(invalid());
        }
        let mut tx = begin_read_only_repeatable(self.pool()).await?;
        let current = load_current_principal_snapshot(&mut tx, tenant, principal, kind).await?;
        if !current.permissions.contains(Permission::ModelRead)
            || load_tenant(&mut tx, tenant).await?.state != "active"
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let target = model_target(&mut tx, tenant, deployment).await?;
        let accounts = accounts(&mut tx, tenant, deployment, false).await?;
        let view = quota_view(tenant, &target, &accounts)?;
        tx.commit().await?;
        Ok(view)
    }
}
impl PgRegistryTransaction {
    pub async fn set_model_quota(
        &mut self,
        command: SetModelQuota,
    ) -> Result<CommandOutcome<ModelQuotaViewV1>, RepositoryError> {
        command.validate_at(Utc::now()).map_err(|_| invalid())?;
        let mut tx = self.transaction.begin().await?;
        require_tenant_permission(&mut tx, &command.audit, Permission::TenantManage).await?;
        let tenant = &command.audit.tenant_id;
        // Serialize absent-set creation, without changing Tenant version or configuration.
        if load_tenant_for_update(&mut tx, tenant).await?.state != "active" {
            return Err(RepositoryError::PermissionDenied);
        }
        let target = model_target(
            &mut tx,
            tenant,
            &command.request.model_deployment.deployment_id,
        )
        .await?;
        if target != command.request.model_deployment {
            return Err(RepositoryError::Conflict("model quota deployment"));
        }
        let replay = claim_command_receipt(
            &mut tx,
            &command.audit,
            "model_deployment",
            &target.deployment_id.to_string(),
            "model.quota.set",
        )
        .await?;
        let old = accounts(&mut tx, tenant, &target.deployment_id, true).await?;
        let observed = quota_view(tenant, &target, &old)?;
        if replay {
            tx.commit().await?;
            return Ok(CommandOutcome::Replayed(observed));
        }
        if observed.etag != command.expected_etag {
            return Err(RepositoryError::Conflict("model quota"));
        }
        let payload = TypedPayload::new(1, &serde_json::json!({"accounting_mode":"consumable"}))?;
        for (index, (metric, limit)) in METRICS
            .iter()
            .zip(command.request.limits.values())
            .enumerate()
        {
            let old = old.iter().find(|account| {
                account.scope_kind == "model_deployment" && account.metric == metric.as_str()
            });
            let (account_id, version) = if let Some(old) = old {
                if (limit as i64)
                    < old
                        .used_value
                        .checked_add(old.reserved_value)
                        .ok_or_else(corrupt)?
                {
                    return Err(RepositoryError::Conflict("model quota usage"));
                }
                let version:i64=sqlx::query_scalar("UPDATE insight_platform.quota_accounts SET limit_value=$4,version=version+1,updated_at=clock_timestamp() WHERE tenant_id=$1 AND quota_account_id=$2 AND version=$3 AND reserved_value+used_value<=$4 RETURNING version")
                    .bind(tenant.to_string()).bind(&old.quota_account_id).bind(old.version).bind(limit as i64).fetch_optional(&mut *tx).await?.ok_or(RepositoryError::Conflict("model quota"))?;
                (old.quota_account_id.clone(), version)
            } else {
                let id = command.account_ids[index].to_string();
                sqlx::query("INSERT INTO insight_platform.quota_accounts(tenant_id,quota_account_id,scope_kind,scope_id,work_class,metric,limit_value,payload_schema_version,payload,payload_digest) VALUES($1,$2,'model_deployment',$3,'model',$4,$5,$6,$7,$8)")
                    .bind(tenant.to_string()).bind(&id).bind(target.deployment_id.to_string()).bind(metric.as_str()).bind(limit as i64)
                    .bind(payload.schema_version).bind(&payload.value).bind(&payload.digest).execute(&mut *tx).await?;
                (id, 1)
            };
            let mut audit = command.audit.clone();
            audit.event_id = command.event_ids[index].clone();
            audit.outbox_id = command.outbox_ids[index].clone();
            append_command_event(
                &mut tx,
                &audit,
                "quota_account",
                &account_id,
                version,
                "model.quota_allocated",
                &TypedPayload::new(
                    1,
                    &serde_json::json!({"model_deployment":target,"metric":metric,"limit":limit}),
                )?,
            )
            .await?;
        }
        terminalize_command_receipt(
            &mut tx,
            &command.audit,
            &target.deployment_id.to_string(),
            "allocated",
        )
        .await?;
        let result = quota_view(
            tenant,
            &target,
            &accounts(&mut tx, tenant, &target.deployment_id, false).await?,
        )?;
        tx.commit().await?;
        Ok(CommandOutcome::Applied(result))
    }
}
async fn model_target(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &ResourceId,
    id: &ResourceId,
) -> Result<ExactDeploymentRef, RepositoryError> {
    let record = load_deployment(tx, tenant, id).await?;
    let closure = decode_deployment_closure(&record.bindings)?;
    let DeploymentClosure::ModelProfile(ref model) = closure else {
        return Err(corrupt());
    };
    if ResourceId::parse_expected(&record.resource_id, ResourceKind::ModelProfile).is_err()
        || record.resource_version_id != model.profile_revision.revision_id.to_string()
    {
        return Err(corrupt());
    }
    let target = ExactDeploymentRef {
        resource_kind: ResourceKind::ModelDeployment,
        deployment_id: id.clone(),
        deployment_digest: record.bindings.digest.parse().map_err(|_| corrupt())?,
    };
    validate_model_quota_target(&target).map_err(|_| corrupt())?;
    Ok(target)
}
async fn accounts(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &ResourceId,
    deployment: &ResourceId,
    lock: bool,
) -> Result<Vec<QuotaAccountRecord>, RepositoryError> {
    let query = "SELECT * FROM insight_platform.quota_accounts WHERE tenant_id=$1 AND work_class='model' AND ((scope_kind='tenant' AND scope_id=$1 AND metric=$3) OR (scope_kind='model_deployment' AND scope_id=$2 AND metric=ANY($4))) ORDER BY tenant_id,quota_account_id";
    let query = if lock {
        "SELECT * FROM insight_platform.quota_accounts WHERE tenant_id=$1 AND work_class='model' AND ((scope_kind='tenant' AND scope_id=$1 AND metric=$3) OR (scope_kind='model_deployment' AND scope_id=$2 AND metric=ANY($4))) ORDER BY tenant_id,quota_account_id FOR UPDATE"
    } else {
        query
    };
    sqlx::query(query)
        .bind(tenant.to_string())
        .bind(deployment.to_string())
        .bind(QuotaDimension::WorkClassConcurrentOperations.as_str())
        .bind(METRICS.map(|m| m.as_str()).as_slice())
        .fetch_all(&mut **tx)
        .await?
        .into_iter()
        .map(quota_account_from_row)
        .collect()
}
fn counter(account: &QuotaAccountRecord) -> Result<ModelQuotaCounterV1, RepositoryError> {
    let id: ResourceId = account.quota_account_id.parse().map_err(|_| corrupt())?;
    if id.kind() != ResourceKind::QuotaAccount || account.version <= 0 {
        return Err(corrupt());
    }
    let result = ModelQuotaCounterV1 {
        limit: u64::try_from(account.limit_value).map_err(|_| corrupt())?,
        reserved: u64::try_from(account.reserved_value).map_err(|_| corrupt())?,
        used: u64::try_from(account.used_value).map_err(|_| corrupt())?,
    };
    result.validate().map_err(|_| corrupt())?;
    Ok(result)
}
fn quota_view(
    tenant: &ResourceId,
    target: &ExactDeploymentRef,
    accounts: &[QuotaAccountRecord],
) -> Result<ModelQuotaViewV1, RepositoryError> {
    let concurrency: Vec<_> = accounts
        .iter()
        .filter(|a| a.scope_kind == "tenant")
        .collect();
    if concurrency.len() != 1 {
        return Err(corrupt());
    }
    let tenant_concurrency = counter(concurrency[0])?;
    let deployed: Vec<_> = accounts
        .iter()
        .filter(|a| a.scope_kind == "model_deployment")
        .collect();
    let allocation = if deployed.is_empty() {
        None
    } else {
        if deployed.len() != 3 {
            return Err(corrupt());
        }
        let mut limits = [0; 3];
        let mut reserved = [0; 3];
        let mut used = [0; 3];
        for (index, metric) in METRICS.iter().enumerate() {
            let matching: Vec<_> = deployed
                .iter()
                .filter(|a| a.metric == metric.as_str())
                .collect();
            if matching.len() != 1 {
                return Err(corrupt());
            }
            let current = counter(matching[0])?;
            limits[index] = current.limit;
            reserved[index] = current.reserved;
            used[index] = current.used;
        }
        Some(ModelQuotaAllocationV1 {
            limits: ModelQuotaLimitsV1::from_values(limits).map_err(|_| corrupt())?,
            reserved: ModelQuotaLimitsV1::from_values(reserved).map_err(|_| corrupt())?,
            used: ModelQuotaLimitsV1::from_values(used).map_err(|_| corrupt())?,
        })
    };
    let identities: Vec<_> = deployed
        .iter()
        .map(|a| serde_json::json!({"id":a.quota_account_id,"version":a.version,"metric":a.metric}))
        .collect();
    let hash=canonical_digest(&serde_json::json!({"schema_version":1,"tenant_id":tenant,"model_deployment":target,"accounts":identities})).map_err(|_|corrupt())?;
    let hash = hash.strip_prefix("sha256:").ok_or_else(corrupt)?;
    let view = ModelQuotaViewV1 {
        schema_version: 1,
        tenant_id: tenant.clone(),
        model_deployment: target.clone(),
        allocation,
        tenant_concurrency,
        etag: format!("\"model-quota-{hash}\""),
    };
    view.validate().map_err(|_| corrupt())?;
    Ok(view)
}
