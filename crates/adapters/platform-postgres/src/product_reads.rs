//! Bounded product reads against existing durable owners; no list projection state.
use crate::repository::{
    begin_read_only_repeatable, load_current_principal_snapshot, task_from_row, task_projection,
    PgRepository, RepositoryError,
};
use insight_platform_contracts::{Permission, ResourceKind};
use insight_platform_tasks::{
    is_eligible_responder,
    store::{TaskInboxPage, TaskInboxQuery, TASK_INBOX_MAX_SCAN},
    TaskQueryPurpose,
};

impl PgRepository {
    pub async fn list_task_inbox(
        &self,
        query: TaskInboxQuery,
    ) -> Result<TaskInboxPage, RepositoryError> {
        if query.tenant_id.kind() != ResourceKind::Tenant
            || query.principal_id.kind() != ResourceKind::Principal
            || query.page_size == 0
            || query.page_size > 50
            || query
                .run_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::Run)
            || query.boundary.as_ref().is_some_and(|(_, id)| {
                !matches!(
                    id.kind(),
                    ResourceKind::Interaction | ResourceKind::ApprovalTask
                )
            })
        {
            return Err(RepositoryError::InvalidInput(
                "Task inbox query is invalid".to_owned(),
            ));
        }
        let mut tx = begin_read_only_repeatable(self.pool()).await?;
        let principal = load_current_principal_snapshot(
            &mut tx,
            &query.tenant_id,
            &query.principal_id,
            query.principal_kind,
        )
        .await?;
        let permissions = match query.purpose {
            TaskQueryPurpose::Respondable => [
                Permission::InteractionRespond,
                Permission::ApprovalRespond,
                Permission::McpWrite,
            ],
            TaskQueryPurpose::Viewable => [
                Permission::InteractionRead,
                Permission::ApprovalRead,
                Permission::McpRead,
            ],
        };
        if !permissions
            .iter()
            .any(|permission| principal.permissions.contains(*permission))
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let now =
            sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>("SELECT clock_timestamp()")
                .fetch_one(&mut *tx)
                .await?;
        let snapshot_at = query.snapshot_at.unwrap_or(now);
        if snapshot_at > now {
            return Err(RepositoryError::InvalidInput(
                "Task inbox snapshot is invalid".to_owned(),
            ));
        }
        let rows = sqlx::query(
            r#"
            SELECT * FROM insight_platform.tasks
            WHERE tenant_id = $1 AND created_at <= $2
              AND ($3::text IS NULL OR state = $3)
              AND ($4::text IS NULL OR task_kind = $4)
              AND ($5::text IS NULL OR run_id = $5)
              AND ($6::timestamptz IS NULL OR (created_at,task_id) < ($6,$7))
            ORDER BY created_at DESC, task_id DESC LIMIT $8
        "#,
        )
        .bind(query.tenant_id.to_string())
        .bind(snapshot_at)
        .bind(query.state.map(|v| v.as_str()))
        .bind(query.kind.map(|v| v.as_str()))
        .bind(query.run_id.as_ref().map(ToString::to_string))
        .bind(query.boundary.as_ref().map(|v| v.0))
        .bind(query.boundary.as_ref().map(|v| v.1.to_string()))
        .bind(TASK_INBOX_MAX_SCAN)
        .fetch_all(&mut *tx)
        .await?;
        let fetched = rows.len();
        let mut records = Vec::new();
        let mut scanned = 0;
        let mut last_scanned = None;
        for row in rows {
            let record = task_from_row(row)?;
            scanned += 1;
            last_scanned = Some((
                record.created_at,
                record.task_id.parse().map_err(|_| {
                    RepositoryError::CorruptRow("Task inbox identity is invalid".to_owned())
                })?,
            ));
            let projection = task_projection(&record)?;
            if insight_platform_tasks::can_query(&projection, &principal, query.purpose)? {
                records.push(insight_platform_tasks::store::TaskAccessRecord {
                    allowed_actions: insight_platform_tasks::allowed_actions(
                        &projection,
                        &principal,
                        now,
                    )?,
                    task: record,
                });
            }
            if records.len() == usize::from(query.page_size) {
                break;
            }
        }
        let next_scanned_boundary = if scanned < fetched || fetched == TASK_INBOX_MAX_SCAN as usize
        {
            last_scanned
        } else {
            None
        };
        tx.commit().await?;
        Ok(TaskInboxPage {
            snapshot_at,
            records,
            next_scanned_boundary,
        })
    }
}

use insight_platform_contracts::{
    AgentSlotBindingInputV1, AgentSlotTargetInputV1, ContextBindingInputV1, DependencySlotKind,
    DeploymentClosure, ExactDeploymentRef, ExactPolicyBinding, ExactVersionRef, PolicyKind,
    PrincipalKind, PrincipalSnapshot, ResourceDocument, ResourceId, Sha256Digest,
};
use insight_platform_registry::authoring::*;
use sqlx::{Postgres, Row, Transaction};

fn authoring_read_permission(kind: DependencySlotKind) -> Permission {
    match kind {
        DependencySlotKind::Model => Permission::ModelRead,
        DependencySlotKind::Capability => Permission::CapabilityRead,
        DependencySlotKind::Context => Permission::ContextRead,
        DependencySlotKind::ChildAgent => Permission::AgentRead,
        DependencySlotKind::Skill => Permission::SkillRead,
    }
}
fn authoring_call_permission(kind: DependencySlotKind) -> Permission {
    match kind {
        DependencySlotKind::Model => Permission::ModelInvoke,
        DependencySlotKind::Capability => Permission::CapabilityInvoke,
        DependencySlotKind::Context => Permission::ContextQuery,
        DependencySlotKind::ChildAgent => Permission::AgentRun,
        DependencySlotKind::Skill => Permission::SkillBind,
    }
}
fn contract_digest(closure: &DeploymentClosure) -> Result<Sha256Digest, AuthoringQueryError> {
    Ok(match closure {
        DeploymentClosure::Agent(c) => c.interface.semantic_digest.clone(),
        DeploymentClosure::ModelProfile(c) => c.profile_revision.semantic_digest.clone(),
        DeploymentClosure::CapabilityInterface(c) => c.interface.semantic_digest.clone(),
        DeploymentClosure::ContextSourceInterface(c) => c.interface.semantic_digest.clone(),
        DeploymentClosure::Skill(c) => c.skill_revision.semantic_digest.clone(),
        _ => return Err(AuthoringQueryError::Invalid),
    })
}
async fn authoring_exact_deployment(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &ResourceId,
    kind: DependencySlotKind,
    selector: &AuthoringDeploymentSelectorV1,
) -> Result<(ExactDeploymentRef, Sha256Digest), AuthoringQueryError> {
    let mut expected_default = None;
    let (exact_id, resource_id, environment, alias) = match selector {
        AuthoringDeploymentSelectorV1::Exact { deployment } => {
            (Some(deployment.deployment_id.to_string()), None, None, None)
        }
        AuthoringDeploymentSelectorV1::Active {
            resource_id,
            environment,
        } => (
            None,
            Some(resource_id.to_string()),
            Some(environment.as_str()),
            None,
        ),
        AuthoringDeploymentSelectorV1::Alias { alias, environment } => {
            (None, None, Some(environment.as_str()), Some(alias.as_str()))
        }
        AuthoringDeploymentSelectorV1::DefaultModel { environment } => {
            if kind != DependencySlotKind::Model {
                return Err(AuthoringQueryError::Invalid);
            }
            let config = crate::repository::load_tenant(tx, tenant)
                .await
                .map_err(authoring_repository_error)?
                .config;
            let default = config
                .default_model
                .ok_or(AuthoringQueryError::DefaultNotConfigured)?;
            let id = default.deployment_id.to_string();
            expected_default = Some(default);
            (Some(id), None, Some(environment.as_str()), None)
        }
    };
    let row=sqlx::query("SELECT d.deployment_id,d.bindings_digest,d.payload_schema_version,d.bindings,r.gate_state,r.lifecycle_state FROM insight_platform.deployments d JOIN insight_platform.resources r ON r.tenant_id=d.tenant_id AND r.resource_id=d.resource_id WHERE d.tenant_id=$1 AND r.resource_kind=$2 AND ($5::text IS NULL OR d.environment=$5) AND (NOT $7::boolean OR r.active_deployment_id=d.deployment_id) AND (($3::text IS NOT NULL AND d.deployment_id=$3) OR ($3::text IS NULL AND (($4::text IS NOT NULL AND r.resource_id=$4) OR ($6::text IS NOT NULL AND r.payload->>'alias'=$6)) AND r.active_deployment_id=d.deployment_id))")
        .bind(tenant.to_string()).bind(match kind {DependencySlotKind::Model=>"model_profile",DependencySlotKind::Capability=>"capability_interface",DependencySlotKind::Context=>"context_source_interface",DependencySlotKind::ChildAgent=>"agent",DependencySlotKind::Skill=>"skill"})
        .bind(exact_id).bind(resource_id).bind(environment).bind(alias).bind(expected_default.is_some()).fetch_optional(&mut **tx).await.map_err(|_|AuthoringQueryError::Unavailable)?.ok_or(AuthoringQueryError::NotFound)?;
    if row
        .try_get::<String, _>("gate_state")
        .map_err(|_| AuthoringQueryError::Unavailable)?
        != "enabled"
        || row
            .try_get::<String, _>("lifecycle_state")
            .map_err(|_| AuthoringQueryError::Unavailable)?
            == "retired"
    {
        return Err(AuthoringQueryError::Disabled);
    }
    let digest: Sha256Digest = row
        .try_get::<String, _>("bindings_digest")
        .map_err(|_| AuthoringQueryError::Unavailable)?
        .parse()
        .map_err(|_| AuthoringQueryError::Unavailable)?;
    let id = row
        .try_get::<String, _>("deployment_id")
        .map_err(|_| AuthoringQueryError::Unavailable)?
        .parse()
        .map_err(|_| AuthoringQueryError::Unavailable)?;
    let exact =
        ExactDeploymentRef::new(id, digest).map_err(|_| AuthoringQueryError::Unavailable)?;
    if matches!(selector,AuthoringDeploymentSelectorV1::Exact{deployment} if deployment!=&exact) {
        return Err(AuthoringQueryError::ContractMismatch);
    }
    if expected_default
        .as_ref()
        .is_some_and(|default| default != &exact)
    {
        return Err(AuthoringQueryError::ContractMismatch);
    }
    if expected_default.is_some()
        && row
            .try_get::<String, _>("lifecycle_state")
            .map_err(|_| AuthoringQueryError::Unavailable)?
            != "active"
    {
        return Err(AuthoringQueryError::Disabled);
    }
    let payload = insight_platform_contracts::TypedPayload {
        schema_version: row
            .try_get("payload_schema_version")
            .map_err(|_| AuthoringQueryError::Unavailable)?,
        value: row
            .try_get("bindings")
            .map_err(|_| AuthoringQueryError::Unavailable)?,
        digest: exact.deployment_digest.to_string(),
    };
    let closure = crate::repository::decode_deployment_closure(&payload)
        .map_err(|_| AuthoringQueryError::Unavailable)?;
    if kind == DependencySlotKind::Model
        && matches!(
            selector,
            AuthoringDeploymentSelectorV1::DefaultModel { .. }
                | AuthoringDeploymentSelectorV1::Alias { .. }
        )
    {
        crate::repository::validate_default_model_closure(tx, tenant, &exact, false)
            .await
            .map_err(|error| match error {
                RepositoryError::PermissionDenied => AuthoringQueryError::Denied,
                RepositoryError::NotFound(_) => AuthoringQueryError::NotFound,
                RepositoryError::Conflict(_) => AuthoringQueryError::ContractMismatch,
                RepositoryError::InvalidInput(_) => AuthoringQueryError::Invalid,
                _ => AuthoringQueryError::Unavailable,
            })?;
    }
    Ok((exact, contract_digest(&closure)?))
}
async fn authoring_policy(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &ResourceId,
    revision: &ExactVersionRef,
    expected_kind: PolicyKind,
    deployment: Option<&ExactDeploymentRef>,
) -> Result<(), AuthoringQueryError> {
    let row=sqlx::query("SELECT v.content_digest,v.payload_schema_version,v.payload,v.payload_digest,r.gate_state,r.lifecycle_state FROM insight_platform.resource_versions v JOIN insight_platform.resources r ON r.tenant_id=v.tenant_id AND r.resource_id=v.resource_id WHERE v.tenant_id=$1 AND v.resource_version_id=$2 AND r.resource_kind='policy'")
        .bind(tenant.to_string()).bind(revision.revision_id.to_string()).fetch_optional(&mut **tx).await.map_err(|_|AuthoringQueryError::Unavailable)?.ok_or(AuthoringQueryError::NotFound)?;
    if row
        .try_get::<String, _>("gate_state")
        .map_err(|_| AuthoringQueryError::Unavailable)?
        != "enabled"
        || row
            .try_get::<String, _>("lifecycle_state")
            .map_err(|_| AuthoringQueryError::Unavailable)?
            == "retired"
    {
        return Err(AuthoringQueryError::Disabled);
    }
    if row
        .try_get::<String, _>("content_digest")
        .map_err(|_| AuthoringQueryError::Unavailable)?
        != revision.semantic_digest.as_str()
    {
        return Err(AuthoringQueryError::ContractMismatch);
    }
    let payload = crate::repository::payload_from_row(
        &row,
        "payload_schema_version",
        "payload",
        "payload_digest",
    )
    .map_err(|_| AuthoringQueryError::Unavailable)?;
    let published = crate::repository::decode_published_version_payload(&payload)
        .map_err(|_| AuthoringQueryError::Unavailable)?;
    if !matches!(&published.document,ResourceDocument::Policy(policy) if policy.policy_kind==expected_kind)
    {
        return Err(AuthoringQueryError::ContractMismatch);
    }
    if let Some(expected) = deployment {
        let row=sqlx::query("SELECT payload_schema_version,bindings,bindings_digest FROM insight_platform.deployments WHERE tenant_id=$1 AND deployment_id=$2 AND resource_version_id=$3")
            .bind(tenant.to_string()).bind(expected.deployment_id.to_string()).bind(revision.revision_id.to_string()).fetch_optional(&mut **tx).await.map_err(|_|AuthoringQueryError::Unavailable)?.ok_or(AuthoringQueryError::NotFound)?;
        if row
            .try_get::<String, _>("bindings_digest")
            .map_err(|_| AuthoringQueryError::Unavailable)?
            != expected.deployment_digest.as_str()
        {
            return Err(AuthoringQueryError::ContractMismatch);
        }
    }
    Ok(())
}
async fn resolve_authoring_slot(
    tx: &mut Transaction<'_, Postgres>,
    tenant: &ResourceId,
    principal: &PrincipalSnapshot,
    slot: &AuthoringSlotSelectionV1,
) -> Result<AuthoringResolutionV1, AuthoringQueryError> {
    if !principal
        .permissions
        .contains(authoring_read_permission(slot.target.kind()))
        || !principal.permissions.contains(Permission::PolicyRead)
    {
        return Err(AuthoringQueryError::Denied);
    }
    let mut candidates = Vec::new();
    let mut contracts = Vec::new();
    let mut deployment_features = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for selector in slot.target.selectors() {
        let (exact, contract) =
            authoring_exact_deployment(tx, tenant, slot.target.kind(), selector).await?;
        if !seen.insert(exact.deployment_id.clone()) {
            return Err(AuthoringQueryError::Invalid);
        }
        if matches!(
            slot.target.kind(),
            DependencySlotKind::Capability
                | DependencySlotKind::Context
                | DependencySlotKind::ChildAgent
        ) {
            let features = crate::agent_feature_repository::derive_agent_deployment_features(
                tx, tenant, &exact,
            )
            .await
            .map_err(authoring_repository_error)?;
            if features.interface_contract_digest != contract {
                return Err(AuthoringQueryError::ContractMismatch);
            }
            deployment_features.push(features);
        }
        candidates.push(exact);
        contracts.push(contract);
    }
    let target = match &slot.target {
        AuthoringSlotTargetV1::Context {
            consistency,
            allowed_projection,
            authorization_policy,
            ranking_policy,
            ..
        } => {
            authoring_policy(
                tx,
                tenant,
                authorization_policy,
                PolicyKind::Authorization,
                None,
            )
            .await?;
            authoring_policy(tx, tenant, ranking_policy, PolicyKind::Ranking, None).await?;
            AgentSlotTargetInputV1::Context {
                binding: Box::new(ContextBindingInputV1 {
                    context_deployment: candidates.remove(0),
                    consistency: consistency.clone(),
                    allowed_projection: allowed_projection.clone(),
                    authorization_policy: authorization_policy.clone(),
                    ranking_policy: ranking_policy.clone(),
                }),
            }
        }
        other => {
            let policy: &ExactPolicyBinding = match other {
                AuthoringSlotTargetV1::Model {
                    selection_policy, ..
                }
                | AuthoringSlotTargetV1::Capability {
                    selection_policy, ..
                }
                | AuthoringSlotTargetV1::ChildAgent {
                    selection_policy, ..
                }
                | AuthoringSlotTargetV1::Skill {
                    selection_policy, ..
                } => selection_policy,
                _ => unreachable!(),
            };
            authoring_policy(
                tx,
                tenant,
                &policy.revision,
                PolicyKind::Selection,
                Some(&policy.deployment),
            )
            .await?;
            match other {
                AuthoringSlotTargetV1::Model { .. } => AgentSlotTargetInputV1::Model {
                    candidates,
                    selection_policy: policy.clone(),
                },
                AuthoringSlotTargetV1::Capability { tool_alias, .. } => {
                    AgentSlotTargetInputV1::Capability {
                        candidates,
                        selection_policy: policy.clone(),
                        tool_alias: tool_alias.clone(),
                    }
                }
                AuthoringSlotTargetV1::ChildAgent { .. } => AgentSlotTargetInputV1::ChildAgent {
                    candidates,
                    selection_policy: policy.clone(),
                },
                AuthoringSlotTargetV1::Skill { .. } => AgentSlotTargetInputV1::Skill {
                    candidates,
                    selection_policy: policy.clone(),
                },
                _ => unreachable!(),
            }
        }
    };
    let binding = AgentSlotBindingInputV1 {
        slot_id: slot.slot_id.clone(),
        requirement_digest: slot.requirement_digest.clone(),
        target,
    };
    binding
        .validate()
        .map_err(|_| AuthoringQueryError::Invalid)?;
    let contract_match = slot
        .interface_contract_digest
        .as_ref()
        .map(|wanted| contracts.iter().all(|observed| observed == wanted));
    if contract_match == Some(false) {
        return Err(AuthoringQueryError::ContractMismatch);
    }
    Ok(AuthoringResolutionV1::Resolved {
        binding: Box::new(binding),
        deployment_features,
        observed_contract_digests: contracts,
        contract_match,
        call_authorized: principal
            .permissions
            .contains(authoring_call_permission(slot.target.kind())),
    })
}
impl PgRepository {
    pub async fn resolve_agent_authoring_bindings(
        &self,
        tenant: &ResourceId,
        principal_id: &ResourceId,
        principal_kind: PrincipalKind,
        request: &ResolveAgentBindingsRequestV1,
    ) -> Result<ResolveAgentBindingsResponseV1, AuthoringQueryError> {
        request.validate()?;
        let mut tx = begin_read_only_repeatable(self.pool())
            .await
            .map_err(authoring_repository_error)?;
        let principal =
            load_current_principal_snapshot(&mut tx, tenant, principal_id, principal_kind)
                .await
                .map_err(authoring_repository_error)?;
        let mut slots = Vec::new();
        for slot in &request.slots {
            let resolution = match resolve_authoring_slot(&mut tx, tenant, &principal, slot).await {
                Ok(value) => value,
                Err(AuthoringQueryError::Unavailable) => {
                    return Err(AuthoringQueryError::Unavailable)
                }
                Err(code) => AuthoringResolutionV1::Rejected { code },
            };
            slots.push(AuthoringSlotResolutionV1 {
                slot_id: slot.slot_id.clone(),
                resolution,
            });
        }
        let response = ResolveAgentBindingsResponseV1 {
            schema_version: 1,
            slots,
        };
        response.validate_for(request)?;
        tx.commit()
            .await
            .map_err(|_| AuthoringQueryError::Unavailable)?;
        Ok(response)
    }
    pub async fn discover_agent_authoring_dependencies(
        &self,
        query: DiscoverAuthoringDependencies,
    ) -> Result<AuthoringDependencyPage, AuthoringQueryError> {
        query.filters.validate()?;
        if query.tenant_id.kind() != ResourceKind::Tenant
            || query.principal_id.kind() != ResourceKind::Principal
            || query.page_size == 0
            || query.page_size > 50
            || query
                .boundary
                .as_ref()
                .is_some_and(|(_, id)| id.kind() != deployment_kind(query.filters.kind))
        {
            return Err(AuthoringQueryError::Invalid);
        }
        let mut tx = begin_read_only_repeatable(self.pool())
            .await
            .map_err(authoring_repository_error)?;
        let principal = load_current_principal_snapshot(
            &mut tx,
            &query.tenant_id,
            &query.principal_id,
            query.principal_kind,
        )
        .await
        .map_err(authoring_repository_error)?;
        if !principal
            .permissions
            .contains(authoring_read_permission(query.filters.kind))
            || !principal.permissions.contains(Permission::PolicyRead)
        {
            return Err(AuthoringQueryError::Denied);
        }
        let now =
            sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>("SELECT clock_timestamp()")
                .fetch_one(&mut *tx)
                .await
                .map_err(|_| AuthoringQueryError::Unavailable)?;
        let snapshot_at = query.snapshot_at.unwrap_or(now);
        if snapshot_at > now {
            return Err(AuthoringQueryError::Invalid);
        }
        let mut rows=sqlx::query("SELECT d.deployment_id,d.created_at,d.environment,r.resource_id FROM insight_platform.deployments d JOIN insight_platform.resources r ON r.tenant_id=d.tenant_id AND r.resource_id=d.resource_id WHERE d.tenant_id=$1 AND r.resource_kind=$2 AND r.gate_state='enabled' AND r.lifecycle_state<>'retired' AND r.active_deployment_id=d.deployment_id AND d.created_at<=$3 AND ($4::text IS NULL OR d.environment=$4) AND ($5::timestamptz IS NULL OR (d.created_at,d.deployment_id)<($5,$6)) ORDER BY d.created_at DESC,d.deployment_id DESC LIMIT $7")
            .bind(query.tenant_id.to_string()).bind(resource_kind(query.filters.kind).descriptor().name).bind(snapshot_at).bind(&query.filters.environment)
            .bind(query.boundary.as_ref().map(|v|v.0)).bind(query.boundary.as_ref().map(|v|v.1.to_string())).bind(i64::from(query.page_size)+1).fetch_all(&mut *tx).await.map_err(|_|AuthoringQueryError::Unavailable)?;
        let has_more = rows.len() > usize::from(query.page_size);
        if has_more {
            rows.pop();
        }
        let mut items = Vec::new();
        let mut boundary = None;
        for row in rows {
            let resource_id: ResourceId = row
                .try_get::<String, _>("resource_id")
                .map_err(|_| AuthoringQueryError::Unavailable)?
                .parse()
                .map_err(|_| AuthoringQueryError::Unavailable)?;
            let environment: String = row
                .try_get("environment")
                .map_err(|_| AuthoringQueryError::Unavailable)?;
            let selector = AuthoringDeploymentSelectorV1::Active {
                resource_id: resource_id.clone(),
                environment: environment.clone(),
            };
            let (deployment, interface_contract_digest) = authoring_exact_deployment(
                &mut tx,
                &query.tenant_id,
                query.filters.kind,
                &selector,
            )
            .await?;
            boundary = Some((
                row.try_get("created_at")
                    .map_err(|_| AuthoringQueryError::Unavailable)?,
                deployment.deployment_id.clone(),
            ));
            let item = AuthoringDependencyV1 {
                schema_version: 1,
                kind: query.filters.kind,
                resource_id,
                environment,
                deployment,
                contract_match: query
                    .filters
                    .interface_contract_digest
                    .as_ref()
                    .map(|value| value == &interface_contract_digest),
                interface_contract_digest,
                call_authorized: principal
                    .permissions
                    .contains(authoring_call_permission(query.filters.kind)),
            };
            item.validate_for(&query.filters)?;
            items.push(item);
        }
        tx.commit()
            .await
            .map_err(|_| AuthoringQueryError::Unavailable)?;
        Ok(AuthoringDependencyPage {
            items,
            snapshot_at,
            next_boundary: if has_more { boundary } else { None },
        })
    }
}
fn authoring_repository_error(error: RepositoryError) -> AuthoringQueryError {
    match error {
        RepositoryError::PermissionDenied => AuthoringQueryError::Denied,
        _ => AuthoringQueryError::Unavailable,
    }
}

impl PgRepository {
    pub async fn read_capability_task_control(
        &self,
        tenant: &ResourceId,
        principal_id: &ResourceId,
        principal_kind: PrincipalKind,
        task_id: &ResourceId,
    ) -> Result<insight_platform_tasks::store::CapabilityTaskControl, RepositoryError> {
        let mut tx = begin_read_only_repeatable(self.pool()).await?;
        let principal =
            load_current_principal_snapshot(&mut tx, tenant, principal_id, principal_kind).await?;
        let row =
            sqlx::query("SELECT * FROM insight_platform.tasks WHERE tenant_id=$1 AND task_id=$2")
                .bind(tenant.to_string())
                .bind(task_id.to_string())
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(RepositoryError::NotFound("Task"))?;
        let task = task_from_row(row)?;
        let projection = task_projection(&task)?;
        if !is_eligible_responder(&projection, &principal)? {
            return Err(RepositoryError::PermissionDenied);
        }
        let insight_platform_tasks::TaskDefinition::CapabilityInput { job_id, .. } =
            &projection.payload.definition
        else {
            return Err(RepositoryError::InvalidInput(
                "Task is not a Capability input".into(),
            ));
        };
        let row=sqlx::query("SELECT i.version AS invocation_version,j.version AS job_version FROM insight_platform.invocations i JOIN insight_platform.jobs j ON j.tenant_id=i.tenant_id AND j.invocation_id=i.invocation_id WHERE i.tenant_id=$1 AND i.invocation_id=$2 AND j.job_id=$3").bind(tenant.to_string()).bind(&task.owner_id).bind(job_id.to_string()).fetch_optional(&mut *tx).await?.ok_or(RepositoryError::NotFound("Task owner"))?;
        let version = |name: &str| -> Result<u64, RepositoryError> {
            u64::try_from(row.try_get::<i64, _>(name)?)
                .map_err(|_| RepositoryError::CorruptRow("Task owner version".into()))
        };
        let result = insight_platform_tasks::store::CapabilityTaskControl {
            task,
            invocation_version: version("invocation_version")?,
            job_version: version("job_version")?,
        };
        tx.commit().await?;
        Ok(result)
    }
}

impl PgRepository {
    pub async fn read_run_value_metadata_for_principal(
        &self,
        tenant: &ResourceId,
        principal: &ResourceId,
        kind: PrincipalKind,
        run: &ResourceId,
        value: &ResourceId,
    ) -> Result<insight_platform_orchestrator::store::RunValueMetadataRecord, RepositoryError> {
        if value.kind() != ResourceKind::RunValue {
            return Err(RepositoryError::NotFound("Run value"));
        }
        self.read_run_value_metadata_page(
            insight_platform_orchestrator::store::RunValuesQuery {
                tenant_id: tenant.clone(),
                principal_id: principal.clone(),
                principal_kind: kind,
                run_id: run.clone(),
                node_id: None,
                page_size: 1,
                snapshot_at: None,
                boundary: None,
            },
            Some(value),
        )
        .await?
        .items
        .into_iter()
        .next()
        .ok_or(RepositoryError::NotFound("Run value"))
    }
    pub async fn list_run_values_for_principal(
        &self,
        query: insight_platform_orchestrator::store::RunValuesQuery,
    ) -> Result<insight_platform_orchestrator::store::RunValuesPage, RepositoryError> {
        self.read_run_value_metadata_page(query, None).await
    }
    async fn read_run_value_metadata_page(
        &self,
        query: insight_platform_orchestrator::store::RunValuesQuery,
        value: Option<&ResourceId>,
    ) -> Result<insight_platform_orchestrator::store::RunValuesPage, RepositoryError> {
        use insight_platform_orchestrator::store::{RunValueMetadataRecord, RunValuesPage};
        if query.run_id.kind() != ResourceKind::Run
            || query
                .node_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::NodeExecution)
            || query.page_size == 0
            || query.page_size > 50
            || query
                .boundary
                .as_ref()
                .is_some_and(|(_, id)| id.kind() != ResourceKind::RunValue)
        {
            return Err(RepositoryError::InvalidInput("Run values query".into()));
        }
        let mut tx = begin_read_only_repeatable(self.pool()).await?;
        let principal = load_current_principal_snapshot(
            &mut tx,
            &query.tenant_id,
            &query.principal_id,
            query.principal_kind,
        )
        .await?;
        if !principal.permissions.contains(Permission::RuntimeRead) {
            return Err(RepositoryError::PermissionDenied);
        }
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2)",
        )
        .bind(query.tenant_id.to_string())
        .bind(query.run_id.to_string())
        .fetch_one(&mut *tx)
        .await?;
        if !exists {
            return Err(RepositoryError::NotFound("Run"));
        }
        let now =
            sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>("SELECT clock_timestamp()")
                .fetch_one(&mut *tx)
                .await?;
        let snapshot_at = query.snapshot_at.unwrap_or(now);
        if snapshot_at > now {
            return Err(RepositoryError::InvalidInput("Run values snapshot".into()));
        }
        let mut rows=sqlx::query("SELECT value_id,node_id,classification,schema_digest,content_digest,(artifact_id IS NOT NULL) AS has_artifact,created_at FROM insight_platform.run_values WHERE tenant_id=$1 AND run_id=$2 AND created_at<=$3 AND ($4::text IS NULL OR node_id=$4) AND ($5::text IS NULL OR value_id=$5) AND ($6::timestamptz IS NULL OR (created_at,value_id)<($6,$7)) ORDER BY created_at DESC,value_id DESC LIMIT $8")
            .bind(query.tenant_id.to_string()).bind(query.run_id.to_string()).bind(snapshot_at).bind(query.node_id.as_ref().map(ToString::to_string)).bind(value.map(ToString::to_string)).bind(query.boundary.as_ref().map(|v|v.0)).bind(query.boundary.as_ref().map(|v|v.1.to_string())).bind(i64::from(query.page_size)+1).fetch_all(&mut *tx).await?;
        let more = rows.len() > usize::from(query.page_size);
        if more {
            rows.pop();
        }
        let mut items = Vec::new();
        let mut boundary = None;
        for row in rows {
            let parse = |field: &str| -> Result<ResourceId, RepositoryError> {
                row.try_get::<String, _>(field)?
                    .parse()
                    .map_err(|_| RepositoryError::CorruptRow("Run value identity".into()))
            };
            let digest = |field: &str| -> Result<Sha256Digest, RepositoryError> {
                row.try_get::<String, _>(field)?
                    .parse()
                    .map_err(|_| RepositoryError::CorruptRow("Run value digest".into()))
            };
            let value_id = parse("value_id")?;
            let node_id = row
                .try_get::<Option<String>, _>("node_id")?
                .map(|v| {
                    ResourceId::parse_expected(&v, ResourceKind::NodeExecution)
                        .map_err(|_| RepositoryError::CorruptRow("Run value node".into()))
                })
                .transpose()?;
            boundary = Some((row.try_get("created_at")?, value_id.clone()));
            items.push(RunValueMetadataRecord {
                run_id: query.run_id.clone(),
                value_id,
                node_id,
                classification: row
                    .try_get::<String, _>("classification")?
                    .parse()
                    .map_err(|_| RepositoryError::CorruptRow("Run value classification".into()))?,
                schema_digest: digest("schema_digest")?,
                content_digest: digest("content_digest")?,
                storage_kind: if row.try_get("has_artifact")? {
                    insight_platform_contracts::RunValueStorageKind::Artifact
                } else {
                    insight_platform_contracts::RunValueStorageKind::Inline
                },
            });
        }
        tx.commit().await?;
        Ok(RunValuesPage {
            items,
            snapshot_at,
            next_boundary: if more { boundary } else { None },
        })
    }
}

impl PgRepository {
    pub async fn list_child_runs_for_principal(
        &self,
        query: insight_platform_orchestrator::store::ChildRunLinksQuery,
    ) -> Result<insight_platform_orchestrator::store::ChildRunLinksPage, RepositoryError> {
        use insight_platform_orchestrator::store::{ChildRunLinksPage, PublicChildRunLinkRecord};
        if query.parent_run_id.kind() != ResourceKind::Run
            || query
                .parent_node_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::NodeExecution)
            || query.page_size == 0
            || query.page_size > 50
            || query
                .boundary
                .as_ref()
                .is_some_and(|(_, id)| id.kind() != ResourceKind::Run)
        {
            return Err(RepositoryError::InvalidInput("child Run query".into()));
        }
        let mut tx = begin_read_only_repeatable(self.pool()).await?;
        let principal = load_current_principal_snapshot(
            &mut tx,
            &query.tenant_id,
            &query.principal_id,
            query.principal_kind,
        )
        .await?;
        if !principal.permissions.contains(Permission::RuntimeRead) {
            return Err(RepositoryError::PermissionDenied);
        }
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM insight_platform.runs WHERE tenant_id=$1 AND run_id=$2)",
        )
        .bind(query.tenant_id.to_string())
        .bind(query.parent_run_id.to_string())
        .fetch_one(&mut *tx)
        .await?;
        if !exists {
            return Err(RepositoryError::NotFound("parent Run"));
        }
        let now =
            sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>("SELECT clock_timestamp()")
                .fetch_one(&mut *tx)
                .await?;
        let snapshot_at = query.snapshot_at.unwrap_or(now);
        if snapshot_at > now {
            return Err(RepositoryError::InvalidInput("child Run snapshot".into()));
        }
        let mut rows=sqlx::query(r#"
            SELECT child.run_id AS child_run_id,child.parent_node_id,child.agent_deployment_id,
                   child.state,child.version,child.input_value_id,child.output_value_id,child.created_at,
                   node.plan_node_key,deployment.bindings_digest
            FROM insight_platform.runs child
            JOIN insight_platform.run_nodes link ON link.tenant_id=child.tenant_id AND link.related_run_id=child.run_id
                AND link.record_kind='child_run_link' AND link.run_id=child.parent_run_id AND link.parent_node_id=child.parent_node_id
            JOIN insight_platform.run_nodes node ON node.tenant_id=child.tenant_id AND node.run_id=child.parent_run_id
                AND node.node_id=child.parent_node_id AND node.record_kind='node_execution' AND node.node_kind='child_agent_call'
            JOIN insight_platform.deployments deployment ON deployment.tenant_id=child.tenant_id AND deployment.deployment_id=child.agent_deployment_id
            WHERE child.tenant_id=$1 AND child.parent_run_id=$2 AND child.created_at<=$3
                AND ($4::text IS NULL OR child.parent_node_id=$4)
                AND ($5::timestamptz IS NULL OR (child.created_at,child.run_id)<($5,$6))
            ORDER BY child.created_at DESC,child.run_id DESC LIMIT $7
        "#).bind(query.tenant_id.to_string()).bind(query.parent_run_id.to_string()).bind(snapshot_at)
            .bind(query.parent_node_id.as_ref().map(ToString::to_string)).bind(query.boundary.as_ref().map(|v|v.0)).bind(query.boundary.as_ref().map(|v|v.1.to_string())).bind(i64::from(query.page_size)+1).fetch_all(&mut *tx).await?;
        let more = rows.len() > usize::from(query.page_size);
        rows.truncate(usize::from(query.page_size));
        let mut items = Vec::new();
        let mut boundary = None;
        for row in rows {
            let id = |key: &str| -> Result<ResourceId, RepositoryError> {
                row.try_get::<String, _>(key)?
                    .parse()
                    .map_err(|_| RepositoryError::CorruptRow("child Run identity".into()))
            };
            let record = PublicChildRunLinkRecord {
                schema_version: 1,
                parent_run_id: query.parent_run_id.clone(),
                parent_node_id: id("parent_node_id")?,
                parent_plan_node_key: insight_platform_plan::PlanNodeKey::new(
                    row.try_get("plan_node_key")?,
                )
                .map_err(|_| RepositoryError::CorruptRow("child Run Plan key".into()))?,
                child_run_id: id("child_run_id")?,
                child_agent_deployment: ExactDeploymentRef::new(
                    id("agent_deployment_id")?,
                    row.try_get::<String, _>("bindings_digest")?
                        .parse()
                        .map_err(|_| {
                            RepositoryError::CorruptRow("child Run deployment digest".into())
                        })?,
                )
                .map_err(|_| RepositoryError::CorruptRow("child Run deployment".into()))?,
                child_state: row
                    .try_get::<String, _>("state")?
                    .parse()
                    .map_err(|_| RepositoryError::CorruptRow("child Run state".into()))?,
                child_version: u64::try_from(row.try_get::<i64, _>("version")?)
                    .map_err(|_| RepositoryError::CorruptRow("child Run version".into()))?,
                input_value_id: id("input_value_id")?,
                output_value_id: row
                    .try_get::<Option<String>, _>("output_value_id")?
                    .map(|v| {
                        v.parse()
                            .map_err(|_| RepositoryError::CorruptRow("child Run output".into()))
                    })
                    .transpose()?,
                created_at: row.try_get("created_at")?,
            };
            record
                .validate_for(&query.parent_run_id, query.parent_node_id.as_ref())
                .map_err(|_| RepositoryError::CorruptRow("child Run projection".into()))?;
            boundary = Some((record.created_at, record.child_run_id.clone()));
            items.push(record);
        }
        tx.commit().await?;
        Ok(ChildRunLinksPage {
            items,
            snapshot_at,
            next_boundary: if more { boundary } else { None },
        })
    }
}
