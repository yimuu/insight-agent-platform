//! CR-216 Sandbox Capability admission.
//!
//! This module admits exactly one OpenSandbox Dispatcher Job into the shared Job authority. It
//! deliberately contains no provider, guest, attestor, callback, Artifact grant, Secret grant, or
//! backend-selection behavior.

use crate::{
    capability_execution_repository::update_capability_invocation,
    invocation_repository::{
        load_capability_execution_input, load_enabled_exact_published_version,
    },
    repository::{
        append_command_event, claim_command_receipt, decode_deployment_closure, job_projection,
        load_deployment, load_job_for_update_by_text, load_resource, require_tenant_permission,
        terminalize_command_receipt, RepositoryError, TypedPayload,
    },
};
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_json, CapabilityBackendBinding, CapabilityBackendContract, CommandAudit,
    CommandOutcome, DeploymentClosure, EntityLifecycle, InvocationState, JobState, Permission,
    QuotaDimension, RegistryResourceKind, ResourceDocument, ResourceId, ResourceKind,
    SandboxCapabilityContract, SandboxNetworkMode as RegistrySandboxNetworkMode,
    SandboxResourceLimitsV1 as RegistryLimits, Sha256Digest, WorkClass,
};
use insight_platform_invocations::{
    decide_defer_to_sandbox, CapabilityExecutionInputMaterial, CapabilityInvocationRecord,
    DetachedSandboxSourceKind,
};
use insight_platform_jobs::{JobOwnerRef, JobProjection};
use insight_platform_sandbox::opensandbox::{
    SandboxDispatcherJobPayloadV1, SandboxExecutionPlanV1,
    SandboxNetworkMode as ExecutionNetworkMode,
    SandboxProvisioningLimitsV1 as ExecutionProvisioningLimits, SandboxRepositoryDecisionV1,
    SandboxResourceLimitsV1 as ExecutionLimits, SANDBOX_CONTRACT_SCHEMA_VERSION,
};
use sqlx::{Postgres, Row, Transaction};
use std::collections::BTreeSet;

pub const SANDBOX_QUOTA_LINES: usize = 4;

#[derive(Debug, Clone)]
pub struct SandboxCapabilitySubmission {
    pub output_value_id: ResourceId,
    pub receipt_id: ResourceId,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
    pub usage_reservation_id: ResourceId,
    pub quota_entry_ids: Vec<ResourceId>,
}

impl SandboxCapabilitySubmission {
    pub fn validate(&self) -> Result<(), RepositoryError> {
        let fixed = [
            (&self.output_value_id, ResourceKind::RunValue),
            (&self.receipt_id, ResourceKind::Receipt),
            (&self.event_id, ResourceKind::Event),
            (&self.outbox_id, ResourceKind::OutboxEvent),
            (&self.usage_reservation_id, ResourceKind::UsageReservation),
        ];
        if fixed.iter().any(|(id, kind)| id.kind() != *kind)
            || self.quota_entry_ids.len() != SANDBOX_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
        {
            return Err(RepositoryError::InvalidInput(
                "OpenSandbox submission identities are invalid".to_owned(),
            ));
        }
        let identities = fixed
            .iter()
            .map(|(id, _)| id.to_string())
            .chain(self.quota_entry_ids.iter().map(ToString::to_string))
            .collect::<BTreeSet<_>>();
        if identities.len() != fixed.len() + self.quota_entry_ids.len() {
            return Err(RepositoryError::InvalidInput(
                "OpenSandbox submission identities must be unique".to_owned(),
            ));
        }
        Ok(())
    }
}

pub(crate) async fn accept_sandbox_capability_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &CapabilityInvocationRecord,
    job_id: &ResourceId,
    submission: &SandboxCapabilitySubmission,
    database_now: DateTime<Utc>,
) -> Result<CommandOutcome<SandboxRepositoryDecisionV1>, RepositoryError> {
    submission.validate()?;
    if job_id.kind() != ResourceKind::Job
        || invocation.state != InvocationState::Ready
        || invocation.payload.current_job_id.is_some()
        || invocation.payload.admission.backend_kind
            != insight_platform_contracts::CapabilityBackendKind::Sandbox
        || invocation.payload.admission.mcp_runtime.is_some()
        || !invocation
            .payload
            .admission
            .artifact_contract
            .ports
            .is_empty()
    {
        return Err(RepositoryError::Conflict(
            "OpenSandbox Capability admission shape",
        ));
    }

    let input = load_capability_execution_input(transaction, invocation).await?;
    let CapabilityExecutionInputMaterial::Inline { value: input_value } = input.material else {
        return Err(RepositoryError::InvalidInput(
            "OpenSandbox Artifact input port is inactive in CR-216".to_owned(),
        ));
    };

    let deployment = load_deployment(
        transaction,
        &invocation.tenant_id,
        &invocation.payload.admission.deployment.deployment_id,
    )
    .await?;
    if deployment.bindings.digest
        != invocation
            .payload
            .admission
            .deployment
            .deployment_digest
            .to_string()
    {
        return Err(RepositoryError::Conflict(
            "OpenSandbox exact Capability Deployment",
        ));
    }
    let capability = match decode_deployment_closure(&deployment.bindings)? {
        DeploymentClosure::CapabilityInterface(closure) => closure,
        _ => {
            return Err(RepositoryError::Conflict(
                "OpenSandbox Capability Deployment closure",
            ));
        }
    };
    if !capability.secret_bindings.is_empty() {
        return Err(RepositoryError::Conflict(
            "OpenSandbox secret injection is disabled",
        ));
    }
    let CapabilityBackendBinding::Sandbox {
        runtime: runtime_revision,
        package: package_revision,
        profile: profile_binding,
    } = &capability.backend
    else {
        return Err(RepositoryError::Conflict("OpenSandbox Capability backend"));
    };

    let implementation = load_enabled_exact_published_version(
        transaction,
        &invocation.tenant_id,
        &capability.implementation,
        RegistryResourceKind::CapabilityImplementation,
    )
    .await?;
    let ResourceDocument::CapabilityImplementation(implementation) = implementation.document else {
        return Err(RepositoryError::CorruptRow(
            "OpenSandbox Capability Implementation document".to_owned(),
        ));
    };
    let CapabilityBackendContract::Sandbox(sandbox_contract) = &implementation.backend_contract
    else {
        return Err(RepositoryError::Conflict(
            "OpenSandbox Capability Implementation contract",
        ));
    };

    let runtime = load_enabled_exact_published_version(
        transaction,
        &invocation.tenant_id,
        runtime_revision,
        RegistryResourceKind::SandboxRuntime,
    )
    .await?;
    let ResourceDocument::SandboxRuntime(runtime) = runtime.document else {
        return Err(RepositoryError::CorruptRow(
            "OpenSandbox Runtime Revision document".to_owned(),
        ));
    };
    let runtime_contract_digest = runtime
        .runtime_contract
        .canonical_digest()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;

    let package = load_enabled_exact_published_version(
        transaction,
        &invocation.tenant_id,
        package_revision,
        RegistryResourceKind::SandboxPackage,
    )
    .await?;
    let ResourceDocument::SandboxPackage(package) = package.document else {
        return Err(RepositoryError::CorruptRow(
            "OpenSandbox Package Revision document".to_owned(),
        ));
    };

    let profile_deployment = load_deployment(
        transaction,
        &invocation.tenant_id,
        &profile_binding.deployment.deployment_id,
    )
    .await?;
    if profile_deployment.bindings.digest
        != profile_binding.deployment.deployment_digest.to_string()
        || profile_deployment.resource_version_id
            != profile_binding.revision.revision_id.to_string()
    {
        return Err(RepositoryError::Conflict(
            "OpenSandbox Profile Deployment binding",
        ));
    }
    let profile_closure = match decode_deployment_closure(&profile_deployment.bindings)? {
        DeploymentClosure::SandboxProfile(closure) => closure,
        _ => {
            return Err(RepositoryError::Conflict(
                "OpenSandbox Profile Deployment closure",
            ));
        }
    };
    let profile = load_enabled_exact_published_version(
        transaction,
        &invocation.tenant_id,
        &profile_binding.revision,
        RegistryResourceKind::SandboxProfile,
    )
    .await?;
    let ResourceDocument::SandboxProfile(profile) = profile.document else {
        return Err(RepositoryError::CorruptRow(
            "OpenSandbox Profile Revision document".to_owned(),
        ));
    };
    let profile_resource_id = profile_deployment
        .resource_id
        .parse::<ResourceId>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let profile_resource =
        load_resource(transaction, &invocation.tenant_id, &profile_resource_id).await?;
    let active_profile_deployment = profile_binding.deployment.deployment_id.to_string();
    if profile_resource.resource_kind != RegistryResourceKind::SandboxProfile.as_str()
        || profile_resource.lifecycle_state != EntityLifecycle::Active.as_str()
        || profile_resource.gate_state != "enabled"
        || profile_resource.active_deployment_id.as_deref()
            != Some(active_profile_deployment.as_str())
    {
        return Err(RepositoryError::Conflict(
            "OpenSandbox Profile Deployment gate",
        ));
    }

    validate_exact_closure(
        sandbox_contract,
        runtime_revision,
        &runtime_contract_digest,
        package_revision,
        &package,
        profile_binding,
        &profile_closure,
        &profile,
    )?;

    let mut limits = profile_closure.limits.clone();
    limits.maximum_input_bytes = limits.maximum_input_bytes.min(u64::from(
        invocation
            .payload
            .admission
            .interface_limits
            .maximum_input_bytes,
    ));
    limits.maximum_output_bytes = limits.maximum_output_bytes.min(u64::from(
        invocation
            .payload
            .admission
            .interface_limits
            .maximum_output_bytes,
    ));
    limits.wall_milliseconds = limits.wall_milliseconds.min(
        invocation
            .payload
            .admission
            .interface_limits
            .maximum_execution_milliseconds,
    );
    limits
        .validate()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let input_bytes = u64::try_from(
        canonical_json(&input_value)
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
            .len(),
    )
    .map_err(|_| RepositoryError::InvalidInput("OpenSandbox input size".to_owned()))?;
    if input_bytes > limits.maximum_input_bytes {
        return Err(RepositoryError::InvalidInput(
            "OpenSandbox inline input exceeds the effective Profile ceiling".to_owned(),
        ));
    }
    drop(input_value);

    let mut plan = SandboxExecutionPlanV1 {
        schema_version: SANDBOX_CONTRACT_SCHEMA_VERSION,
        tenant_id: invocation.tenant_id.clone(),
        invocation_id: invocation.invocation_id.clone(),
        job_id: job_id.clone(),
        package_version_id: package_revision.revision_id.clone(),
        image_uri: package.image_uri.clone(),
        runtime_version_id: runtime_revision.revision_id.clone(),
        runtime_contract_digest,
        sandbox_profile_deployment_id: profile_binding.deployment.deployment_id.clone(),
        profile_deployment_digest: profile_binding.deployment.deployment_digest.clone(),
        runner_argv: runtime.fixed_runner_argv.clone(),
        package_argv: package.package_argv.clone(),
        input_value_id: invocation.input_value_id.clone(),
        output_value_id: submission.output_value_id.clone(),
        classification: input.exact.classification,
        input_schema_digest: invocation.payload.admission.input_schema_digest.clone(),
        input_digest: input.exact.content_digest.clone(),
        output_schema_digest: invocation.payload.admission.output_schema_digest.clone(),
        network_mode: execution_network_mode(profile_closure.network_mode),
        limits: execution_limits(&limits),
        provisioning_limits: ExecutionProvisioningLimits {
            maximum_candidates: profile_closure.provisioning_limits.maximum_candidates,
            candidate_page_items: profile_closure.provisioning_limits.candidate_page_items,
            candidate_quiescence_milliseconds: profile_closure
                .provisioning_limits
                .candidate_quiescence_milliseconds,
            provisioning_timeout_milliseconds: profile_closure
                .provisioning_limits
                .provisioning_timeout_milliseconds,
            orphan_page_items: profile_closure.provisioning_limits.orphan_page_items,
            runner_header_bytes: profile_closure.provisioning_limits.runner_header_bytes,
            diagnostic_bytes: profile_closure.provisioning_limits.diagnostic_bytes,
        },
        deadline_at: invocation.deadline,
        request_digest: zero_digest()?,
    };
    plan.request_digest = plan.semantic_digest()?;
    plan.validate()?;
    let payload = SandboxDispatcherJobPayloadV1::accepted(plan)?;

    let job = JobProjection {
        trace: invocation.trace,
        tenant_id: invocation.tenant_id.clone(),
        job_id: job_id.clone(),
        work_class: WorkClass::Sandbox,
        owner: JobOwnerRef {
            owner_kind: ResourceKind::Job,
            owner_id: job_id.clone(),
        },
        state: JobState::Ready,
        version: 1,
        attempt_count: 0,
        attempt_limit: 1,
        lease_generation: 0,
        lease: None,
        scheduled_at: database_now,
        retry_at: None,
        wake: None,
        deadline: invocation.deadline,
    };
    job.validate()?;
    payload.validate_for(&job)?;

    let audit = CommandAudit {
        trace: invocation.trace,
        tenant_id: invocation.tenant_id.clone(),
        principal_id: invocation.payload.admission.principal.principal_id.clone(),
        principal_kind: invocation.payload.admission.principal.principal_kind,
        receipt_id: submission.receipt_id.clone(),
        event_id: submission.event_id.clone(),
        outbox_id: submission.outbox_id.clone(),
        idempotency_key_digest: invocation.payload.admission.idempotency_key_digest.clone(),
        request_digest: payload.plan.request_digest.clone(),
        receipt_expires_at: invocation.deadline,
    };
    audit
        .validate_at(database_now)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    if claim_command_receipt(
        transaction,
        &audit,
        "job",
        &job_id.to_string(),
        "sandbox.execute",
    )
    .await?
    {
        let existing = load_job_for_update_by_text(
            transaction,
            &invocation.tenant_id.to_string(),
            &job_id.to_string(),
        )
        .await?;
        let current_job = job_projection(&existing)?;
        let current_payload: SandboxDispatcherJobPayloadV1 =
            serde_json::from_value(existing.payload.value.clone())
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        current_payload.validate_for(&current_job)?;
        if current_payload != payload {
            return Err(RepositoryError::Conflict("OpenSandbox admission replay"));
        }
        return Ok(CommandOutcome::Replayed(SandboxRepositoryDecisionV1 {
            fence: None,
            job: current_job,
            payload: current_payload,
        }));
    }

    require_tenant_permission(transaction, &audit, Permission::SandboxExecute).await?;
    reserve_sandbox_quota(
        transaction,
        &invocation.tenant_id,
        &submission.usage_reservation_id,
        &submission.quota_entry_ids,
        &payload.plan.request_digest,
        &payload.plan.limits,
        database_now,
    )
    .await?;
    let deferred = decide_defer_to_sandbox(
        invocation,
        DetachedSandboxSourceKind::SandboxCapability,
        invocation.version,
        job_id,
        1,
        None,
        database_now,
    )?;
    update_capability_invocation(transaction, invocation, &deferred).await?;

    let stored = TypedPayload::from_versioned(1, &payload, 1_048_576)?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.jobs (
            tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, invocation_id,
            run_id, node_id, state, version, attempt_no, attempt_limit, lease_epoch,
            scheduled_at, deadline, priority, request_digest, quota_reservation_id,
            payload_schema_version, payload, payload_digest, created_at, updated_at, trace_id
        ) VALUES ($1, $2, 'sandbox_capability_execution', 'sandbox', 'job', $2, $3,
                  $4, $5, 'ready', 1, 0, 1, 0, $6, $7, 0, $8, $9,
                  $10, $11, $12, $6, $6, $13)
        "#,
    )
    .bind(invocation.tenant_id.to_string())
    .bind(job_id.to_string())
    .bind(invocation.invocation_id.to_string())
    .bind(invocation.run_id.to_string())
    .bind(invocation.node_execution_id.to_string())
    .bind(database_now)
    .bind(invocation.deadline)
    .bind(payload.plan.request_digest.to_string())
    .bind(submission.usage_reservation_id.to_string())
    .bind(stored.schema_version)
    .bind(&stored.value)
    .bind(&stored.digest)
    .bind(invocation.trace.trace_id.to_string())
    .execute(&mut **transaction)
    .await?;
    append_command_event(
        transaction,
        &audit,
        "capability_invocation",
        &invocation.invocation_id.to_string(),
        to_i64(deferred.version, "Invocation version")?,
        "capability.waiting",
        &TypedPayload::new(
            1,
            &serde_json::json!({
                "invocation_id": invocation.invocation_id,
                "invocation_state": deferred.state,
                "job_id": job_id,
                "job_state": job.state,
                "provider": "opensandbox_kubernetes",
            }),
        )?,
    )
    .await?;
    terminalize_command_receipt(transaction, &audit, &job_id.to_string(), "accepted").await?;
    Ok(CommandOutcome::Applied(SandboxRepositoryDecisionV1 {
        fence: None,
        job,
        payload,
    }))
}

#[allow(clippy::too_many_arguments)]
fn validate_exact_closure(
    implementation: &SandboxCapabilityContract,
    runtime_revision: &insight_platform_contracts::ExactVersionRef,
    runtime_contract_digest: &Sha256Digest,
    package_revision: &insight_platform_contracts::ExactVersionRef,
    package: &insight_platform_contracts::SandboxPackageResourceSpec,
    profile_binding: &insight_platform_contracts::ExactSandboxProfileBinding,
    profile_closure: &insight_platform_contracts::SandboxProfileDeploymentClosure,
    profile: &insight_platform_contracts::SandboxProfileResourceSpec,
) -> Result<(), RepositoryError> {
    if package.runtime_revision != *runtime_revision
        || package.dependency_versions != [runtime_revision.clone()]
        || profile_closure.schema_version != SANDBOX_CONTRACT_SCHEMA_VERSION
        || profile_closure.profile_revision != profile_binding.revision
        || profile_closure.runtime_revision != *runtime_revision
        || !profile_closure.secret_injection_disabled
        || !profile.secret_injection_disabled
        || !profile.allowed_trust_classes.contains(&package.trust_class)
        || !profile
            .allowed_network_modes
            .contains(&profile_closure.network_mode)
        || !profile_closure.limits.bounded_by(&profile.maximum_limits)
        || !profile_closure
            .provisioning_limits
            .bounded_by(&profile.maximum_provisioning_limits)
        || implementation.package_contract_digest != package.package_digest
        || implementation.image_uri != package.image_uri
        || implementation.package_argv != package.package_argv
        || implementation.dependency_lock_digest != package.dependency_lock_digest
        || implementation.runtime_contract_digest != *runtime_contract_digest
        || package_revision.revision_id.kind() != ResourceKind::SandboxPackageRevision
    {
        return Err(RepositoryError::Conflict(
            "OpenSandbox exact Runtime/Package/Profile closure",
        ));
    }
    Ok(())
}

fn execution_network_mode(mode: RegistrySandboxNetworkMode) -> ExecutionNetworkMode {
    match mode {
        RegistrySandboxNetworkMode::Disabled => ExecutionNetworkMode::Disabled,
        RegistrySandboxNetworkMode::Direct => ExecutionNetworkMode::Direct,
    }
}

fn execution_limits(limits: &RegistryLimits) -> ExecutionLimits {
    ExecutionLimits {
        maximum_input_bytes: limits.maximum_input_bytes,
        maximum_output_bytes: limits.maximum_output_bytes,
        cpu_millicores: limits.cpu_millicores,
        memory_mebibytes: limits.memory_mebibytes,
        pids: limits.pids,
        ephemeral_storage_bytes: limits.ephemeral_storage_bytes,
        wall_milliseconds: limits.wall_milliseconds,
        cleanup_milliseconds: limits.cleanup_milliseconds,
    }
}

async fn reserve_sandbox_quota(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    reservation_id: &ResourceId,
    entry_ids: &[ResourceId],
    request_digest: &Sha256Digest,
    limits: &ExecutionLimits,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    if entry_ids.len() != SANDBOX_QUOTA_LINES
        || entry_ids
            .iter()
            .any(|entry_id| entry_id.kind() != ResourceKind::QuotaLedgerEntry)
        || entry_ids.iter().collect::<BTreeSet<_>>().len() != entry_ids.len()
    {
        return Err(RepositoryError::InvalidInput(
            "OpenSandbox quota identities are invalid".to_owned(),
        ));
    }
    let expected_metrics = sandbox_quota_metrics();
    let rows = sqlx::query(
        r#"
        SELECT tenant_id, quota_account_id, metric, version
        FROM insight_platform.quota_accounts
        WHERE tenant_id = $1 AND scope_kind = 'tenant' AND scope_id = $1
          AND work_class = 'sandbox' AND metric = ANY($2)
        ORDER BY tenant_id, quota_account_id
        FOR UPDATE
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(expected_metrics.iter().cloned().collect::<Vec<_>>())
    .fetch_all(&mut **transaction)
    .await?;
    if rows.len() != SANDBOX_QUOTA_LINES {
        return Err(RepositoryError::QuotaExceeded);
    }
    let mut observed = BTreeSet::new();
    for (row, entry_id) in rows.iter().zip(entry_ids) {
        let metric: String = row.try_get("metric")?;
        if !observed.insert(metric.clone()) {
            return Err(RepositoryError::CorruptRow(
                "duplicate OpenSandbox quota metric".to_owned(),
            ));
        }
        let amount = quota_amount(&metric, limits)?;
        let version: i64 = row.try_get("version")?;
        let next_version: i64 = sqlx::query_scalar(
            r#"
            UPDATE insight_platform.quota_accounts
            SET reserved_value = reserved_value + $4,
                version = version + 1, updated_at = $5
            WHERE tenant_id = $1 AND quota_account_id = $2 AND version = $3
              AND reserved_value + used_value + $4 <= limit_value
            RETURNING version
            "#,
        )
        .bind(row.try_get::<String, _>("tenant_id")?)
        .bind(row.try_get::<String, _>("quota_account_id")?)
        .bind(version)
        .bind(amount)
        .bind(database_now)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::QuotaExceeded)?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.quota_ledger (
                tenant_id, quota_entry_id, quota_account_id, correlation_id,
                entry_kind, reserved_amount, used_amount, account_version,
                request_digest, created_at
            ) VALUES ($1, $2, $3, $4, 'reserve', $5, 0, $6, $7, $8)
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(entry_id.to_string())
        .bind(row.try_get::<String, _>("quota_account_id")?)
        .bind(reservation_id.to_string())
        .bind(amount)
        .bind(next_version)
        .bind(request_digest.to_string())
        .bind(database_now)
        .execute(&mut **transaction)
        .await?;
    }
    if observed != expected_metrics {
        return Err(RepositoryError::CorruptRow(
            "OpenSandbox quota bundle is incomplete".to_owned(),
        ));
    }
    Ok(())
}

fn sandbox_quota_metrics() -> BTreeSet<String> {
    [
        QuotaDimension::SandboxConcurrentExecutions,
        QuotaDimension::SandboxCpuSeconds,
        QuotaDimension::SandboxMemoryMebibytes,
        QuotaDimension::SandboxOutputBytes,
    ]
    .into_iter()
    .map(|metric| metric.as_str().to_owned())
    .collect()
}

fn quota_amount(metric: &str, limits: &ExecutionLimits) -> Result<i64, RepositoryError> {
    let amount = if metric == QuotaDimension::SandboxConcurrentExecutions.as_str() {
        1
    } else if metric == QuotaDimension::SandboxCpuSeconds.as_str() {
        u64::from(limits.cpu_millicores)
            .checked_mul(limits.wall_milliseconds)
            .ok_or_else(|| {
                RepositoryError::InvalidInput("OpenSandbox CPU ceiling overflow".to_owned())
            })?
            .div_ceil(1_000_000)
    } else if metric == QuotaDimension::SandboxMemoryMebibytes.as_str() {
        u64::from(limits.memory_mebibytes)
    } else if metric == QuotaDimension::SandboxOutputBytes.as_str() {
        limits.maximum_output_bytes
    } else {
        return Err(RepositoryError::CorruptRow(
            "OpenSandbox quota metric is not registered".to_owned(),
        ));
    };
    i64::try_from(amount)
        .map_err(|_| RepositoryError::InvalidInput("OpenSandbox quota exceeds bigint".to_owned()))
}

fn zero_digest() -> Result<Sha256Digest, RepositoryError> {
    format!("sha256:{}", "0".repeat(64))
        .parse()
        .map_err(|_| RepositoryError::InvalidInput("OpenSandbox zero digest".to_owned()))
}

fn to_i64(value: u64, label: &str) -> Result<i64, RepositoryError> {
    i64::try_from(value)
        .map_err(|_| RepositoryError::InvalidInput(format!("{label} exceeds bigint")))
}
