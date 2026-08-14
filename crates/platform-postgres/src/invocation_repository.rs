use crate::context_query_repository::load_context_query;
use crate::repository::{
    append_command_event, claim_command_receipt, decode_deployment_closure,
    decode_published_version_payload, load_deployment, load_resource, load_run_for_update,
    load_task_for_update, payload_from_row, require_ready_run_artifact, require_tenant_permission,
    task_projection, terminalize_command_receipt, validate_deployment_closure_exists, PgRepository,
    RepositoryError, TypedPayload,
};
use chrono::{DateTime, Utc};
use insight_platform_artifacts::ArtifactReferenceSnapshot;
use insight_platform_context::{
    validate_text_to_sql_admission, ContextQueryLimits, ReadOnlySqlPlan, TextToSqlAdmissionFacts,
    TEXT2SQL_PLAN_VALUE_KIND,
};
use insight_platform_contracts::{
    canonical_digest, canonical_json, ArtifactPurpose, ArtifactRef, ArtifactReferenceKind,
    CapabilityBackendBinding, ClosedJsonSchema, CommandOutcome, DataClassification,
    DeploymentClosure, EntityLifecycle, InvocationState, McpTransportBinding, NodeExecutionState,
    Permission, PlanNodeKind, PublishedVersionPayload, RegistryResourceKind, ResourceDocument,
    ResourceId, ResourceKind, RunState, Sha256Digest,
};
use insight_platform_invocations::{
    decide_approval_transition, decide_capability_admission, AdmitCapabilityInvocation,
    CapabilityAdmissionFacts, CapabilityAdmissionSnapshot, CapabilityApprovalDecision,
    CapabilityExecutionContract, CapabilityExecutionInput, CapabilityExecutionInputMaterial,
    CapabilityImplementationContract, CapabilityInterfaceContract, CapabilityInvocationPayload,
    CapabilityInvocationRecord, ExactInvocationValueRef, InvocationCommandLimits, InvocationStore,
    InvocationTransaction, InvocationValueStorage, McpCapabilityRuntimeBinding,
    ResolveCapabilityApproval,
};
use insight_platform_sandbox::SandboxCommandLimits;
use insight_platform_tasks::{
    decide_resolution as decide_task_resolution, ResolveTask, TaskDefinition, TaskPayload,
    TaskState,
};
use sqlx::{postgres::PgRow, Acquire, Postgres, Row, Transaction};
use std::str::FromStr;

pub struct PgInvocationTransaction {
    pub(crate) transaction: Transaction<'static, Postgres>,
    pub(crate) limits: InvocationCommandLimits,
    pub(crate) sandbox_limits: SandboxCommandLimits,
    context_limits: ContextQueryLimits,
}

impl PgRepository {
    pub async fn begin_invocation_transaction(
        &self,
    ) -> Result<PgInvocationTransaction, RepositoryError> {
        Ok(PgInvocationTransaction {
            transaction: self.pool().begin().await?,
            limits: self.invocation_limits(),
            sandbox_limits: self.sandbox_limits(),
            context_limits: self.context_query_limits(),
        })
    }
}

impl InvocationStore for PgRepository {
    type Error = RepositoryError;
    type Transaction<'a>
        = PgInvocationTransaction
    where
        Self: 'a;

    async fn begin(&self) -> Result<Self::Transaction<'_>, Self::Error> {
        self.begin_invocation_transaction().await
    }
}

impl InvocationTransaction for PgInvocationTransaction {
    type Error = RepositoryError;
    type ExecutionRecord = crate::capability_execution_repository::PreparedCapabilityExecution;
    type JobRecord = crate::repository::JobRecord;
    type ControlRecord = crate::capability_execution_repository::ControlledCapabilityExecution;

    async fn admit_capability_invocation(
        &mut self,
        command: AdmitCapabilityInvocation,
    ) -> Result<CommandOutcome<CapabilityInvocationRecord>, Self::Error> {
        command.validate_at(Utc::now(), self.limits)?;
        let mut transaction = self.transaction.begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        command.validate_at(database_now, self.limits)?;

        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "capability_invocation",
            &command.invocation_id.to_string(),
            "capability.admit",
        )
        .await?
        {
            let record = load_capability_invocation(
                &mut transaction,
                &command.audit.tenant_id,
                &command.invocation_id,
                false,
            )
            .await?;
            require_admission_replay(&record, &command)?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }

        let run = load_run_for_update(&mut transaction, &command.audit.tenant_id, &command.run_id)
            .await?;
        let principal = require_tenant_permission(
            &mut transaction,
            &command.audit,
            Permission::CapabilityInvoke,
        )
        .await?;
        let node = load_capability_node_for_update(
            &mut transaction,
            &command.audit.tenant_id,
            &command.node_execution_id,
        )
        .await?;
        if node.run_id != run.run_id {
            return Err(RepositoryError::InvalidInput(
                "CapabilityInvocation Node does not belong to its Run".to_owned(),
            ));
        }

        let selected = selected_capability_deployment(&run.bindings, &command)?;
        let deployment = load_deployment(
            &mut transaction,
            &command.audit.tenant_id,
            &selected.deployment_id,
        )
        .await?;
        if deployment.bindings.digest != selected.deployment_digest.to_string() {
            return Err(RepositoryError::Conflict(
                "exact Capability Deployment binding",
            ));
        }
        let deployment_resource_id = parse_id(&deployment.resource_id, "Capability resource")?;
        let deployment_resource = load_resource(
            &mut transaction,
            &command.audit.tenant_id,
            &deployment_resource_id,
        )
        .await?;
        if deployment_resource.resource_kind != RegistryResourceKind::CapabilityInterface.as_str()
            || deployment_resource.lifecycle_state != EntityLifecycle::Active.as_str()
            || deployment_resource.gate_state != "enabled"
        {
            return Err(RepositoryError::Conflict("Capability Deployment gate"));
        }
        let closure = match decode_deployment_closure(&deployment.bindings)? {
            DeploymentClosure::CapabilityInterface(closure) => closure,
            _ => {
                return Err(RepositoryError::CorruptRow(
                    "Capability Deployment has a non-Capability closure".to_owned(),
                ));
            }
        };
        if deployment.resource_version_id != closure.interface.revision_id.to_string() {
            return Err(RepositoryError::CorruptRow(
                "Capability Deployment root revision differs from its Interface closure".to_owned(),
            ));
        }
        let interface_payload = load_enabled_exact_published_version(
            &mut transaction,
            &command.audit.tenant_id,
            &closure.interface,
            RegistryResourceKind::CapabilityInterface,
        )
        .await?;
        let implementation_payload = load_enabled_exact_published_version(
            &mut transaction,
            &command.audit.tenant_id,
            &closure.implementation,
            RegistryResourceKind::CapabilityImplementation,
        )
        .await?;
        let ResourceDocument::CapabilityInterface(interface_spec) = interface_payload.document
        else {
            return Err(RepositoryError::CorruptRow(
                "Capability Interface revision contains the wrong document".to_owned(),
            ));
        };
        let ResourceDocument::CapabilityImplementation(implementation_spec) =
            implementation_payload.document
        else {
            return Err(RepositoryError::CorruptRow(
                "Capability Implementation revision contains the wrong document".to_owned(),
            ));
        };
        let interface = CapabilityInterfaceContract {
            revision: closure.interface.clone(),
            qualified_name: interface_spec.qualified_name,
            input_schema_digest: interface_spec.input_schema.canonical_digest.clone(),
            output_schema_digest: interface_spec.output_schema.canonical_digest.clone(),
            error_schema_digest: interface_spec.error_schema.canonical_digest.clone(),
            artifacts: interface_spec.artifacts,
            data_policy: interface_spec.data_policy,
            execution_limits: interface_spec.execution_limits,
            effect: interface_spec.effect,
            idempotency: interface_spec.idempotency,
            cancellation: interface_spec.cancellation,
            progress: interface_spec.progress,
        };
        let implementation = CapabilityImplementationContract {
            revision: closure.implementation.clone(),
            interface_revision: implementation_spec.interface_revision,
            backend_kind: implementation_spec.backend_kind,
            backend_contract: implementation_spec.backend_contract,
            backend_contract_digest: implementation_spec.backend_contract_digest,
            credential_requirements: implementation_spec.credential_requirements,
            backend_limits: implementation_spec.backend_limits,
            features: implementation_spec.features,
        };
        require_capability_backend_closure(&closure, &implementation)?;
        validate_deployment_closure_exists(
            &mut transaction,
            &command.audit.tenant_id,
            &DeploymentClosure::CapabilityInterface(closure.clone()),
        )
        .await?;
        validate_policy_revisions(
            &mut transaction,
            &command.audit.tenant_id,
            &command.policy_decisions,
        )
        .await?;
        let mcp_runtime = resolve_mcp_capability_runtime(
            &mut transaction,
            &command,
            &closure,
            &principal,
            database_now,
        )
        .await?;
        let input = load_exact_input_for_update(
            &mut transaction,
            &command.audit.tenant_id,
            &command.input_value_id,
        )
        .await?;
        if matches!(input.storage, InvocationValueStorage::Inline) {
            let row = sqlx::query(
                "SELECT inline_value FROM insight_platform.run_values \
                     WHERE tenant_id = $1 AND value_id = $2 FOR SHARE",
            )
            .bind(command.audit.tenant_id.to_string())
            .bind(command.input_value_id.to_string())
            .fetch_one(&mut *transaction)
            .await?;
            let input_value = insight_platform_contracts::ValueRef::Inline {
                value: row.try_get::<serde_json::Value, _>("inline_value")?,
            };
            validate_capability_value_against_schema(
                &interface_spec.input_schema,
                &input_value,
                interface_spec.execution_limits.maximum_input_bytes,
            )?;
        }
        validate_text_to_sql_input_if_present(
            &mut transaction,
            &command.audit.tenant_id,
            &selected,
            &interface,
            &input,
            self.context_limits,
        )
        .await?;
        let run_state = RunState::from_str(&run.state)
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        let record = decide_capability_admission(
            &command,
            CapabilityAdmissionFacts {
                run_state,
                run_version: parse_u64(run.version, "Run version")?,
                run_pause_requested: run.current.control.pause_requested,
                run_cancel_requested: run.current.control.cancel_requested_at.is_some(),
                run_timeout_requested: run.current.control.timeout_requested_at.is_some(),
                run_deadline: run.deadline,
                run_bindings: run.bindings,
                node_state: node.state,
                node_version: node.version,
                node_kind: node.node_kind,
                node_deadline: node.deadline,
                deployment: selected,
                deployment_closure: closure,
                interface,
                implementation,
                input,
                principal,
                mcp_runtime,
                database_now,
            },
            self.limits,
        )?;
        insert_capability_invocation(&mut transaction, &record).await?;
        if let Some(link_id) = &command.input_artifact_link_id {
            insert_input_artifact_reference(&mut transaction, &record, link_id).await?;
        }
        if let Some(task_id) = &record.payload.approval_task_id {
            insert_approval_task(&mut transaction, &record, task_id).await?;
        }
        append_command_event(
            &mut transaction,
            &command.audit,
            "capability_invocation",
            &record.invocation_id.to_string(),
            1,
            "capability.admitted",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "admission_digest": record.payload.admission.canonical_digest,
                    "approval_task_id": record.payload.approval_task_id,
                    "deployment_id": record.deployment_id,
                    "state": record.state.as_str(),
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &record.invocation_id.to_string(),
            "admitted",
        )
        .await?;
        let persisted = load_capability_invocation(
            &mut transaction,
            &command.audit.tenant_id,
            &command.invocation_id,
            false,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(persisted))
    }

    async fn resolve_capability_approval(
        &mut self,
        command: ResolveCapabilityApproval,
    ) -> Result<CommandOutcome<CapabilityInvocationRecord>, Self::Error> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *transaction)
            .await?;
        command.validate_at(database_now)?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "capability_invocation",
            &command.invocation_id.to_string(),
            "capability.approval.resolve",
        )
        .await?
        {
            let record = load_capability_invocation(
                &mut transaction,
                &command.audit.tenant_id,
                &command.invocation_id,
                false,
            )
            .await?;
            require_approval_replay(&record, command.decision)?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }
        let current = load_capability_invocation(
            &mut transaction,
            &command.audit.tenant_id,
            &command.invocation_id,
            true,
        )
        .await?;
        let resolver = require_tenant_permission(
            &mut transaction,
            &command.audit,
            Permission::ApprovalRespond,
        )
        .await?;
        let task = load_task_for_update(
            &mut transaction,
            &command.audit.tenant_id,
            &command.approval_task_id,
        )
        .await?;
        let task_projection = task_projection(&task)?;
        require_exact_approval_task(&current, &task, &task_projection, &command)?;
        let task_state = match command.decision {
            CapabilityApprovalDecision::Approve => TaskState::Approved,
            CapabilityApprovalDecision::Reject => TaskState::Rejected,
        };
        let next_task = decide_task_resolution(
            &task_projection,
            ResolveTask {
                expected_generation: command.expected_task_generation,
                expected_version: command.expected_task_version,
                target: task_state,
                principal: Some(resolver),
                response_value_id: None,
                response_schema_digest: None,
            },
            database_now,
        )?;
        let next = decide_approval_transition(&current, &command, database_now)?;
        let task_payload = TypedPayload::new(1, &next_task.payload)?;
        let updated_task =
            sqlx::query(
                r#"
            UPDATE insight_platform.tasks
            SET state = $4, version = $5, payload_schema_version = $6,
                payload = $7, payload_digest = $8, responded_at = $9, updated_at = $9
            WHERE tenant_id = $1 AND task_id = $2 AND version = $3
              AND generation = $10 AND state = 'pending' AND responded_at IS NULL
            RETURNING task_id
            "#,
            )
            .bind(command.audit.tenant_id.to_string())
            .bind(command.approval_task_id.to_string())
            .bind(i64::try_from(command.expected_task_version).map_err(|_| {
                RepositoryError::InvalidInput("Task version exceeds bigint".to_owned())
            })?)
            .bind(task_state.as_str())
            .bind(i64::try_from(next_task.version).map_err(|_| {
                RepositoryError::InvalidInput("Task version exceeds bigint".to_owned())
            })?)
            .bind(task_payload.schema_version)
            .bind(&task_payload.value)
            .bind(&task_payload.digest)
            .bind(database_now)
            .bind(
                i64::try_from(command.expected_task_generation).map_err(|_| {
                    RepositoryError::InvalidInput("Task generation exceeds bigint".to_owned())
                })?,
            )
            .fetch_optional(&mut *transaction)
            .await?;
        if updated_task.is_none() {
            return Err(RepositoryError::Conflict("Capability approval Task"));
        }
        let payload = TypedPayload::from_versioned(1, &next.payload, 1_048_576)?;
        let updated_invocation = sqlx::query(
            r#"
            UPDATE insight_platform.invocations
            SET state = $4, version = $5, payload_schema_version = $6,
                payload = $7, payload_digest = $8, terminal_at = $9, updated_at = $10
            WHERE tenant_id = $1 AND invocation_id = $2 AND version = $3
              AND state = 'awaiting_approval' AND terminal_at IS NULL
            RETURNING invocation_id
            "#,
        )
        .bind(command.audit.tenant_id.to_string())
        .bind(command.invocation_id.to_string())
        .bind(
            i64::try_from(command.expected_invocation_version).map_err(|_| {
                RepositoryError::InvalidInput("Invocation version exceeds bigint".to_owned())
            })?,
        )
        .bind(next.state.as_str())
        .bind(i64::try_from(next.version).map_err(|_| {
            RepositoryError::InvalidInput("Invocation version exceeds bigint".to_owned())
        })?)
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .bind(next.terminal_at)
        .bind(database_now)
        .fetch_optional(&mut *transaction)
        .await?;
        if updated_invocation.is_none() {
            return Err(RepositoryError::Conflict("CapabilityInvocation approval"));
        }
        append_command_event(
            &mut transaction,
            &command.audit,
            "capability_invocation",
            &command.invocation_id.to_string(),
            i64::try_from(next.version).map_err(|_| {
                RepositoryError::InvalidInput("Invocation version exceeds bigint".to_owned())
            })?,
            "approval.resolved",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "approval_task_id": command.approval_task_id,
                    "decision": task_state.as_str(),
                    "state": next.state.as_str(),
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &command.invocation_id.to_string(),
            task_state.as_str(),
        )
        .await?;
        let persisted = load_capability_invocation(
            &mut transaction,
            &command.audit.tenant_id,
            &command.invocation_id,
            false,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(persisted))
    }

    async fn prepare_capability_dispatch(
        &mut self,
        command: insight_platform_invocations::PrepareCapabilityDispatch,
    ) -> Result<CommandOutcome<Self::ExecutionRecord>, Self::Error> {
        PgInvocationTransaction::prepare_capability_dispatch(self, command).await
    }

    async fn commit_capability_outcome(
        &mut self,
        command: insight_platform_invocations::CommitCapabilityOutcome,
    ) -> Result<CommandOutcome<Self::ExecutionRecord>, Self::Error> {
        PgInvocationTransaction::commit_capability_outcome(self, command).await
    }

    async fn commit_capability_cancellation_outcome(
        &mut self,
        command: insight_platform_invocations::CommitCapabilityCancellationOutcome,
    ) -> Result<CommandOutcome<Self::ExecutionRecord>, Self::Error> {
        PgInvocationTransaction::commit_capability_cancellation_outcome(self, command).await
    }

    async fn wake_capability_invocation(
        &mut self,
        command: insight_platform_invocations::WakeCapabilityInvocation,
    ) -> Result<CommandOutcome<Self::ExecutionRecord>, Self::Error> {
        PgInvocationTransaction::wake_capability_invocation(self, command).await
    }

    async fn resolve_capability_input(
        &mut self,
        command: insight_platform_invocations::ResolveCapabilityInput,
    ) -> Result<CommandOutcome<Self::ExecutionRecord>, Self::Error> {
        PgInvocationTransaction::resolve_capability_input(self, command).await
    }

    async fn record_capability_progress(
        &mut self,
        command: insight_platform_invocations::RecordCapabilityProgress,
    ) -> Result<CommandOutcome<Self::JobRecord>, Self::Error> {
        PgInvocationTransaction::record_capability_progress(self, command).await
    }

    async fn control_capability_invocation(
        &mut self,
        command: insight_platform_invocations::ControlCapabilityInvocation,
    ) -> Result<CommandOutcome<Self::ControlRecord>, Self::Error> {
        PgInvocationTransaction::control_capability_invocation(self, command).await
    }

    async fn resolve_capability_reconciliation(
        &mut self,
        command: insight_platform_invocations::ResolveCapabilityReconciliation,
    ) -> Result<CommandOutcome<Self::ExecutionRecord>, Self::Error> {
        PgInvocationTransaction::resolve_capability_reconciliation(self, command).await
    }

    async fn commit(self) -> Result<(), Self::Error> {
        self.transaction.commit().await?;
        Ok(())
    }

    async fn rollback(self) -> Result<(), Self::Error> {
        self.transaction.rollback().await?;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct LockedCapabilityNode {
    pub(crate) run_id: String,
    pub(crate) state: NodeExecutionState,
    pub(crate) version: u64,
    pub(crate) node_kind: PlanNodeKind,
    pub(crate) deadline: DateTime<Utc>,
}

pub(crate) async fn load_capability_node_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    node_id: &ResourceId,
) -> Result<LockedCapabilityNode, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT run_id, record_kind, node_kind, state, version, deadline
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id = $2
        FOR UPDATE
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(node_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Capability NodeExecution"))?;
    if row.try_get::<String, _>("record_kind")? != "node_execution" {
        return Err(RepositoryError::InvalidInput(
            "Capability owner is not a NodeExecution".to_owned(),
        ));
    }
    Ok(LockedCapabilityNode {
        run_id: row.try_get("run_id")?,
        state: row
            .try_get::<String, _>("state")?
            .parse::<NodeExecutionState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        version: parse_u64(row.try_get("version")?, "Node version")?,
        node_kind: row
            .try_get::<String, _>("node_kind")?
            .parse::<PlanNodeKind>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        deadline: row.try_get("deadline")?,
    })
}

fn selected_capability_deployment(
    bindings: &insight_platform_contracts::RunBindingsSnapshot,
    command: &AdmitCapabilityInvocation,
) -> Result<insight_platform_contracts::ExactDeploymentRef, RepositoryError> {
    let slot = bindings
        .slots
        .iter()
        .find(|slot| slot.slot_id == command.slot_id)
        .ok_or(RepositoryError::NotFound("frozen Capability slot"))?;
    let insight_platform_contracts::FrozenSlotTarget::Capability { candidates, .. } = &slot.target
    else {
        return Err(RepositoryError::InvalidInput(
            "selected Run slot is not a Capability".to_owned(),
        ));
    };
    candidates
        .get(usize::from(command.selected_candidate_ordinal))
        .cloned()
        .ok_or_else(|| {
            RepositoryError::InvalidInput(
                "Capability selector chose an out-of-range candidate".to_owned(),
            )
        })
}

pub(crate) async fn load_enabled_exact_published_version(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    exact: &insight_platform_contracts::ExactVersionRef,
    owner_kind: RegistryResourceKind,
) -> Result<PublishedVersionPayload, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT version.payload_schema_version, version.payload, version.payload_digest
        FROM insight_platform.resource_versions AS version
        JOIN insight_platform.resources AS resource
          ON resource.tenant_id = version.tenant_id
         AND resource.resource_id = version.resource_id
        WHERE version.tenant_id = $1 AND version.resource_version_id = $2
          AND version.resource_version_kind = $3 AND version.content_digest = $4
          AND resource.resource_kind = $5
          AND resource.lifecycle_state = 'active' AND resource.gate_state = 'enabled'
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(exact.revision_id.to_string())
    .bind(exact.resource_kind.descriptor().name)
    .bind(exact.semantic_digest.to_string())
    .bind(owner_kind.as_str())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("enabled exact ResourceVersion"))?;
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let published = decode_published_version_payload(&payload)?;
    published
        .validate_for(owner_kind, &exact.revision_id)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(published)
}

pub(crate) async fn load_exact_capability_interface_spec(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    admission: &CapabilityAdmissionSnapshot,
) -> Result<insight_platform_contracts::CapabilityInterfaceResourceSpec, RepositoryError> {
    let published = load_enabled_exact_published_version(
        transaction,
        tenant_id,
        &admission.interface,
        RegistryResourceKind::CapabilityInterface,
    )
    .await?;
    let ResourceDocument::CapabilityInterface(spec) = published.document else {
        return Err(RepositoryError::CorruptRow(
            "Capability Interface revision contains the wrong document".to_owned(),
        ));
    };
    let exact = spec.input_schema.canonical_digest == admission.input_schema_digest
        && spec.output_schema.canonical_digest == admission.output_schema_digest
        && spec.error_schema.canonical_digest == admission.error_schema_digest
        && spec.artifacts == admission.artifact_contract
        && spec.data_policy == admission.data_flow_policy
        && spec.execution_limits == admission.interface_limits
        && spec.effect == admission.effect
        && spec.idempotency == admission.idempotency
        && spec.cancellation == admission.cancellation
        && spec.progress == admission.progress;
    if !exact {
        return Err(RepositoryError::Conflict(
            "Capability Interface admission snapshot",
        ));
    }
    Ok(spec)
}

pub(crate) fn validate_capability_value_against_schema(
    schema: &ClosedJsonSchema,
    value: &insight_platform_contracts::ValueRef,
    maximum_bytes: u32,
) -> Result<(), RepositoryError> {
    let value = match value {
        insight_platform_contracts::ValueRef::Inline { value } => value.clone(),
        insight_platform_contracts::ValueRef::Artifact { .. } => {
            return Err(RepositoryError::InvalidInput(
                "Artifact-backed Capability value must be materialized before schema validation"
                    .to_owned(),
            ));
        }
    };
    let byte_length = canonical_json(&value)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
        .len();
    if byte_length
        > usize::try_from(maximum_bytes).map_err(|_| {
            RepositoryError::InvalidInput("Capability byte limit exceeds usize".to_owned())
        })?
    {
        return Err(RepositoryError::InvalidInput(
            "Capability value exceeds Interface byte limit".to_owned(),
        ));
    }
    schema.validate_instance(&value).map_err(|_| {
        RepositoryError::InvalidInput("Capability value fails exact Interface schema".to_owned())
    })
}

async fn validate_policy_revisions(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    bundle: &insight_platform_invocations::InvocationPolicyDecisionBundle,
) -> Result<(), RepositoryError> {
    for decision in &bundle.decisions {
        let published = load_enabled_exact_published_version(
            transaction,
            tenant_id,
            &decision.policy,
            RegistryResourceKind::Policy,
        )
        .await?;
        if !matches!(published.document, ResourceDocument::Policy(_)) {
            return Err(RepositoryError::CorruptRow(
                "Invocation Policy revision contains the wrong document".to_owned(),
            ));
        }
    }
    Ok(())
}

fn require_capability_backend_closure(
    closure: &insight_platform_contracts::CapabilityDeploymentClosure,
    implementation: &CapabilityImplementationContract,
) -> Result<(), RepositoryError> {
    closure
        .backend
        .validate_for(&implementation.backend_contract)
        .map_err(|_| {
            RepositoryError::InvalidInput(
                "Capability backend contract and Deployment closure disagree".to_owned(),
            )
        })
}

async fn resolve_mcp_capability_runtime(
    transaction: &mut Transaction<'_, Postgres>,
    command: &AdmitCapabilityInvocation,
    capability_closure: &insight_platform_contracts::CapabilityDeploymentClosure,
    principal: &insight_platform_contracts::PrincipalSnapshot,
    database_now: DateTime<Utc>,
) -> Result<Option<McpCapabilityRuntimeBinding>, RepositoryError> {
    let CapabilityBackendBinding::Mcp {
        mcp_deployment,
        discovery_snapshot_id,
        discovery_snapshot_digest,
        authorization_policy,
    } = &capability_closure.backend
    else {
        if command.mcp_runtime.is_some() {
            return Err(RepositoryError::InvalidInput(
                "non-MCP Capability supplied an MCP runtime binding".to_owned(),
            ));
        }
        return Ok(None);
    };
    let request = command.mcp_runtime.as_ref().ok_or_else(|| {
        RepositoryError::InvalidInput("MCP Capability requires an exact runtime binding".to_owned())
    })?;
    let record = crate::mcp_repository::load_mcp_authorization_binding(
        transaction,
        &command.audit.tenant_id,
        &request.authorization_binding_id,
        false,
    )
    .await?;
    let authorization = record
        .execution_context(database_now)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    if authorization.tenant_id != command.audit.tenant_id
        || authorization.principal_id != principal.principal_id
        || authorization.principal_identity_kind != principal.principal_kind
        || authorization.principal_binding_generation != principal.binding_generation
        || authorization.mcp_deployment != *mcp_deployment
    {
        return Err(RepositoryError::Conflict(
            "MCP Capability authorization binding",
        ));
    }
    crate::mcp_repository::validate_mcp_authorization_dependencies(
        transaction,
        &command.audit.tenant_id,
        &authorization.mcp_deployment,
        &authorization.audience_identity_digest,
        &authorization.token_secret_binding,
    )
    .await?;
    let deployment = load_deployment(
        transaction,
        &command.audit.tenant_id,
        &mcp_deployment.deployment_id,
    )
    .await?;
    if deployment.bindings.digest != mcp_deployment.deployment_digest.to_string() {
        return Err(RepositoryError::Conflict(
            "MCP Capability exact MCP Deployment",
        ));
    }
    let closure = match decode_deployment_closure(&deployment.bindings)? {
        DeploymentClosure::McpServer(closure) => closure,
        _ => {
            return Err(RepositoryError::CorruptRow(
                "MCP Capability references a non-MCP Deployment".to_owned(),
            ));
        }
    };
    if closure.auth_policy.as_ref() != Some(authorization_policy) {
        return Err(RepositoryError::Conflict(
            "MCP Capability authorization Policy",
        ));
    }
    let authorization_context_digest = authorization.canonical_digest;
    Ok(Some(McpCapabilityRuntimeBinding {
        schema_version: 1,
        mcp_operation_id: request.mcp_operation_id.clone(),
        mcp_deployment: mcp_deployment.clone(),
        discovery_snapshot_id: discovery_snapshot_id.clone(),
        discovery_snapshot_digest: discovery_snapshot_digest.clone(),
        authorization_binding_id: authorization.authorization_binding_id,
        authorization_generation: authorization.generation,
        authorization_context_digest,
        principal_id: principal.principal_id.clone(),
    }))
}

pub(crate) async fn load_capability_execution_contract(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    admission: &CapabilityAdmissionSnapshot,
) -> Result<CapabilityExecutionContract, RepositoryError> {
    let deployment =
        load_deployment(transaction, tenant_id, &admission.deployment.deployment_id).await?;
    if deployment.bindings.digest != admission.deployment.deployment_digest.to_string()
        || deployment.resource_version_id != admission.interface.revision_id.to_string()
    {
        return Err(RepositoryError::Conflict(
            "Capability execution exact Deployment",
        ));
    }
    let resource_id = parse_id(&deployment.resource_id, "Capability resource")?;
    let resource = load_resource(transaction, tenant_id, &resource_id).await?;
    if resource.resource_kind != RegistryResourceKind::CapabilityInterface.as_str()
        || resource.lifecycle_state != EntityLifecycle::Active.as_str()
        || resource.gate_state != "enabled"
    {
        return Err(RepositoryError::Conflict(
            "Capability execution Deployment gate",
        ));
    }
    let closure = match decode_deployment_closure(&deployment.bindings)? {
        DeploymentClosure::CapabilityInterface(closure) => closure,
        _ => {
            return Err(RepositoryError::CorruptRow(
                "Capability execution Deployment has a non-Capability closure".to_owned(),
            ));
        }
    };
    validate_deployment_closure_exists(
        transaction,
        tenant_id,
        &DeploymentClosure::CapabilityInterface(closure.clone()),
    )
    .await?;
    let implementation_payload = load_enabled_exact_published_version(
        transaction,
        tenant_id,
        &closure.implementation,
        RegistryResourceKind::CapabilityImplementation,
    )
    .await?;
    let ResourceDocument::CapabilityImplementation(spec) = implementation_payload.document else {
        return Err(RepositoryError::CorruptRow(
            "Capability execution Implementation revision has the wrong document".to_owned(),
        ));
    };
    let implementation = CapabilityImplementationContract {
        revision: closure.implementation.clone(),
        interface_revision: spec.interface_revision,
        backend_kind: spec.backend_kind,
        backend_contract: spec.backend_contract,
        backend_contract_digest: spec.backend_contract_digest,
        credential_requirements: spec.credential_requirements,
        backend_limits: spec.backend_limits,
        features: spec.features,
    };
    let contract =
        CapabilityExecutionContract::build(admission.deployment.clone(), closure, implementation)?;
    contract.validate_for(admission)?;
    validate_frozen_mcp_runtime_at_claim(transaction, tenant_id, admission, &contract).await?;
    Ok(contract)
}

async fn validate_frozen_mcp_runtime_at_claim(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    admission: &CapabilityAdmissionSnapshot,
    contract: &CapabilityExecutionContract,
) -> Result<(), RepositoryError> {
    let (
        Some(binding),
        CapabilityBackendBinding::Mcp {
            mcp_deployment,
            discovery_snapshot_id,
            discovery_snapshot_digest,
            authorization_policy,
        },
    ) = (&admission.mcp_runtime, &contract.deployment_closure.backend)
    else {
        if admission.backend_kind == insight_platform_contracts::CapabilityBackendKind::Mcp {
            return Err(RepositoryError::CorruptRow(
                "MCP Capability admission lost its runtime binding".to_owned(),
            ));
        }
        return Ok(());
    };
    if &binding.mcp_deployment != mcp_deployment
        || &binding.discovery_snapshot_id != discovery_snapshot_id
        || &binding.discovery_snapshot_digest != discovery_snapshot_digest
    {
        return Err(RepositoryError::Conflict(
            "MCP Capability frozen runtime binding",
        ));
    }
    let record = crate::mcp_repository::load_mcp_authorization_binding(
        transaction,
        tenant_id,
        &binding.authorization_binding_id,
        false,
    )
    .await?;
    let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut **transaction)
        .await?;
    let authorization = record
        .execution_context(database_now)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    if authorization.generation != binding.authorization_generation
        || authorization.canonical_digest != binding.authorization_context_digest
        || authorization.principal_id != binding.principal_id
        || authorization.mcp_deployment != binding.mcp_deployment
    {
        return Err(RepositoryError::Conflict(
            "MCP Capability authorization generation changed",
        ));
    }
    crate::mcp_repository::validate_mcp_authorization_dependencies(
        transaction,
        tenant_id,
        &authorization.mcp_deployment,
        &authorization.audience_identity_digest,
        &authorization.token_secret_binding,
    )
    .await?;
    let deployment = load_deployment(transaction, tenant_id, &mcp_deployment.deployment_id).await?;
    let closure = match decode_deployment_closure(&deployment.bindings)? {
        DeploymentClosure::McpServer(closure) => closure,
        _ => {
            return Err(RepositoryError::CorruptRow(
                "MCP Capability references a non-MCP Deployment".to_owned(),
            ));
        }
    };
    if closure.auth_policy.as_ref() != Some(authorization_policy) {
        return Err(RepositoryError::Conflict(
            "MCP Capability authorization Policy changed",
        ));
    }
    if matches!(closure.transport, McpTransportBinding::ManagedStdio { .. }) {
        return Err(RepositoryError::InvalidInput(
            "Managed stdio MCP must be admitted directly as Sandbox work".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) async fn load_capability_execution_input(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &CapabilityInvocationRecord,
) -> Result<CapabilityExecutionInput, RepositoryError> {
    let exact = &invocation.payload.admission.input;
    load_capability_value_input(
        transaction,
        invocation,
        exact,
        invocation.payload.admission.input_artifact_link_id.as_ref(),
        "Capability execution input RunValue",
    )
    .await
}

pub(crate) async fn load_capability_continuation_input(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &CapabilityInvocationRecord,
    exact: &ExactInvocationValueRef,
    artifact_link_id: Option<&ResourceId>,
) -> Result<CapabilityExecutionInput, RepositoryError> {
    load_capability_value_input(
        transaction,
        invocation,
        exact,
        artifact_link_id,
        "Capability continuation input RunValue",
    )
    .await
}

async fn load_capability_value_input(
    transaction: &mut Transaction<'_, Postgres>,
    invocation: &CapabilityInvocationRecord,
    exact: &ExactInvocationValueRef,
    artifact_link_id: Option<&ResourceId>,
    label: &'static str,
) -> Result<CapabilityExecutionInput, RepositoryError> {
    exact
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let row = sqlx::query(
        r#"
        SELECT inline_value, artifact_id
        FROM insight_platform.run_values
        WHERE tenant_id = $1 AND value_id = $2 AND run_id = $3
          AND node_id IS NOT DISTINCT FROM $4 AND value_kind = $5 AND classification = $6
          AND schema_digest = $7 AND content_digest = $8
        "#,
    )
    .bind(invocation.tenant_id.to_string())
    .bind(exact.value_id.to_string())
    .bind(exact.run_id.to_string())
    .bind(exact.producing_node_id.as_ref().map(ResourceId::to_string))
    .bind(&exact.value_kind)
    .bind(exact.classification.as_str())
    .bind(exact.schema_digest.to_string())
    .bind(exact.content_digest.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound(label))?;
    let inline_value: Option<serde_json::Value> = row.try_get("inline_value")?;
    let artifact_id: Option<String> = row.try_get("artifact_id")?;
    let material = match (&exact.storage, inline_value, artifact_id) {
        (InvocationValueStorage::Inline, Some(value), None) if artifact_link_id.is_none() => {
            CapabilityExecutionInputMaterial::Inline { value }
        }
        (InvocationValueStorage::Artifact { artifact }, None, Some(stored_artifact_id))
            if stored_artifact_id == artifact.artifact_id().to_string() =>
        {
            let link_id = artifact_link_id.ok_or_else(|| {
                RepositoryError::CorruptRow(
                    "Capability Artifact input has no frozen ArtifactLink".to_owned(),
                )
            })?;
            require_ready_run_artifact(transaction, &invocation.tenant_id, artifact).await?;
            let linked: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM insight_platform.artifact_links
                    WHERE tenant_id = $1 AND artifact_link_id = $2
                      AND link_kind = 'reference'
                      AND owner_kind = 'capability_invocation' AND owner_id = $3
                      AND target_artifact_id = $4 AND state = 'active'
                      AND released_at IS NULL
                      AND (expires_at IS NULL OR expires_at > clock_timestamp())
                )
                "#,
            )
            .bind(invocation.tenant_id.to_string())
            .bind(link_id.to_string())
            .bind(invocation.invocation_id.to_string())
            .bind(artifact.artifact_id().to_string())
            .fetch_one(&mut **transaction)
            .await?;
            if !linked {
                return Err(RepositoryError::NotFound(
                    "active Capability input ArtifactLink",
                ));
            }
            CapabilityExecutionInputMaterial::LinkedArtifact {
                artifact_link_id: link_id.clone(),
            }
        }
        _ => {
            return Err(RepositoryError::CorruptRow(
                "Capability execution input storage differs from frozen admission".to_owned(),
            ));
        }
    };
    let input = CapabilityExecutionInput {
        exact: exact.clone(),
        material,
    };
    input
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(input)
}

async fn validate_text_to_sql_input_if_present(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    selected: &insight_platform_contracts::ExactDeploymentRef,
    interface: &CapabilityInterfaceContract,
    input: &ExactInvocationValueRef,
    context_limits: ContextQueryLimits,
) -> Result<(), RepositoryError> {
    if input.value_kind != TEXT2SQL_PLAN_VALUE_KIND {
        return Ok(());
    }
    if !matches!(input.storage, InvocationValueStorage::Inline) {
        return Err(RepositoryError::InvalidInput(
            "Text2SQL plan input must be an inline closed value".to_owned(),
        ));
    }
    let plan_value: serde_json::Value = sqlx::query_scalar(
        r#"
        SELECT inline_value
        FROM insight_platform.run_values
        WHERE tenant_id = $1 AND value_id = $2
        FOR UPDATE
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(input.value_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    let plan: ReadOnlySqlPlan = serde_json::from_value(plan_value).map_err(|_| {
        RepositoryError::InvalidInput("Text2SQL plan is not a closed typed value".to_owned())
    })?;
    let catalog_query = load_context_query(
        transaction,
        tenant_id,
        &plan.catalog_context_query_id,
        true,
        context_limits,
    )
    .await?;
    let catalog_result = catalog_query.payload.result.as_ref().ok_or_else(|| {
        RepositoryError::InvalidInput(
            "Text2SQL catalog ContextQuery has no committed Observation".to_owned(),
        )
    })?;
    let insight_platform_contracts::ContextBackendBinding::SqlCatalog {
        database_identity_digest,
        dialect,
        ..
    } = &catalog_query.payload.admission.context_closure.backend
    else {
        return Err(RepositoryError::InvalidInput(
            "Text2SQL catalog source is not SqlCatalog".to_owned(),
        ));
    };
    let insight_platform_contracts::ContextBackendContract::SqlCatalog {
        catalog_projection_digest,
        ..
    } = &catalog_query
        .payload
        .admission
        .implementation
        .contract
        .backend
    else {
        return Err(RepositoryError::CorruptRow(
            "Text2SQL catalog implementation is not SqlCatalog".to_owned(),
        ));
    };
    validate_text_to_sql_admission(
        &plan,
        &TextToSqlAdmissionFacts {
            run_id: input.run_id.clone(),
            input_value_id: input.value_id.clone(),
            input_value_kind: input.value_kind.clone(),
            input_content_digest: input.content_digest.clone(),
            selected_capability_name: interface.qualified_name.clone(),
            selected_capability_deployment: selected.clone(),
            selected_interface_revision: interface.revision.clone(),
            selected_effect: interface.effect,
            catalog_query_id: catalog_query.context_query_id.clone(),
            catalog_run_id: catalog_query.run_id.clone(),
            catalog_context_deployment: catalog_query
                .payload
                .admission
                .binding
                .context_deployment
                .clone(),
            catalog_backend_database_identity_digest: database_identity_digest.clone(),
            catalog_backend_dialect: dialect.clone(),
            catalog_observation_id: catalog_result.observation.observation_id.clone(),
            catalog_observation_digest: catalog_result.observation.canonical_digest.clone(),
            catalog_projection_digest: catalog_projection_digest.clone(),
        },
    )?;
    Ok(())
}

async fn load_exact_input_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    value_id: &ResourceId,
) -> Result<ExactInvocationValueRef, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT value.value_id, value.run_id, value.node_id, value.value_kind,
               value.classification, value.schema_digest, value.content_digest,
               value.inline_value, value.artifact_id
        FROM insight_platform.run_values AS value
        WHERE value.tenant_id = $1 AND value.value_id = $2
        FOR UPDATE OF value
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(value_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Capability input RunValue"))?;
    let classification = row
        .try_get::<String, _>("classification")?
        .parse::<DataClassification>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let content_digest = parse_digest(row.try_get("content_digest")?, "RunValue content digest")?;
    let inline_value: Option<serde_json::Value> = row.try_get("inline_value")?;
    let artifact_id: Option<String> = row.try_get("artifact_id")?;
    let storage = match (inline_value, artifact_id) {
        (Some(value), None) => {
            let actual = canonical_digest(&value)
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            if actual != content_digest.to_string() {
                return Err(RepositoryError::CorruptRow(
                    "inline RunValue content digest is inconsistent".to_owned(),
                ));
            }
            InvocationValueStorage::Inline
        }
        (None, Some(artifact_id)) => {
            let artifact = lock_ready_input_artifact(
                transaction,
                tenant_id,
                &parse_id(&artifact_id, "Capability input Artifact")?,
                &content_digest,
                classification,
            )
            .await?;
            InvocationValueStorage::Artifact { artifact }
        }
        _ => {
            return Err(RepositoryError::CorruptRow(
                "RunValue storage shape is invalid".to_owned(),
            ));
        }
    };
    let input = ExactInvocationValueRef {
        schema_version: 1,
        value_id: parse_id(&row.try_get::<String, _>("value_id")?, "RunValue")?,
        run_id: parse_id(&row.try_get::<String, _>("run_id")?, "RunValue Run")?,
        producing_node_id: row
            .try_get::<Option<String>, _>("node_id")?
            .map(|value| parse_id(&value, "RunValue Node"))
            .transpose()?,
        value_kind: row.try_get("value_kind")?,
        classification,
        schema_digest: parse_digest(row.try_get("schema_digest")?, "RunValue schema digest")?,
        content_digest,
        storage,
    };
    input
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(input)
}

async fn lock_ready_input_artifact(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    artifact_id: &ResourceId,
    content_digest: &Sha256Digest,
    classification: DataClassification,
) -> Result<ArtifactRef, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT artifact.verified_media_type, artifact.classification,
               blob.content_digest, blob.size_bytes
        FROM insight_platform.artifacts AS artifact
        JOIN insight_platform.artifact_blobs AS blob
          ON blob.tenant_id = artifact.tenant_id AND blob.blob_id = artifact.blob_id
        WHERE artifact.tenant_id = $1 AND artifact.artifact_id = $2
          AND artifact.state = 'ready' AND artifact.terminal_at IS NULL
          AND blob.state = 'verified' AND blob.deleted_at IS NULL
        FOR UPDATE OF blob, artifact
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(artifact_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("ready Capability input Artifact"))?;
    let stored_classification = row
        .try_get::<String, _>("classification")?
        .parse::<DataClassification>()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let stored_digest = parse_digest(row.try_get("content_digest")?, "Artifact content digest")?;
    if stored_classification != classification || &stored_digest != content_digest {
        return Err(RepositoryError::Conflict(
            "Capability input Artifact identity",
        ));
    }
    ArtifactRef::new(
        artifact_id.clone(),
        stored_digest,
        u64::try_from(row.try_get::<i64, _>("size_bytes")?)
            .map_err(|_| RepositoryError::CorruptRow("negative Artifact size".to_owned()))?,
        row.try_get::<String, _>("verified_media_type")?,
        classification,
        None,
    )
    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))
}

async fn insert_capability_invocation(
    transaction: &mut Transaction<'_, Postgres>,
    record: &CapabilityInvocationRecord,
) -> Result<(), RepositoryError> {
    record
        .validate()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let payload = TypedPayload::from_versioned(1, &record.payload, 1_048_576)?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.invocations (
            tenant_id, invocation_id, invocation_kind, owner_kind, owner_id,
            logical_key, run_id, node_id, deployment_id, state, version,
            input_value_id, output_value_id, effect_key_digest,
            payload_schema_version, payload, payload_digest, deadline,
            retry_at, started_at, terminal_at, created_at, updated_at
        ) VALUES (
            $1, $2, 'capability', $3, $4,
            $5, $6, $7, $8, $9, $10,
            $11, NULL, $12, $13, $14, $15, $16,
            NULL, NULL, NULL, $17, $17
        )
        "#,
    )
    .bind(record.tenant_id.to_string())
    .bind(record.invocation_id.to_string())
    .bind(record.owner_kind.descriptor().name)
    .bind(record.owner_id.to_string())
    .bind(&record.logical_key)
    .bind(record.run_id.to_string())
    .bind(record.node_execution_id.to_string())
    .bind(record.deployment_id.to_string())
    .bind(record.state.as_str())
    .bind(i64::try_from(record.version).map_err(|_| {
        RepositoryError::InvalidInput("Invocation version exceeds bigint".to_owned())
    })?)
    .bind(record.input_value_id.to_string())
    .bind(record.effect_key_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(record.deadline)
    .bind(record.created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_approval_task(
    transaction: &mut Transaction<'_, Postgres>,
    record: &CapabilityInvocationRecord,
    task_id: &ResourceId,
) -> Result<(), RepositoryError> {
    let requirement = record
        .payload
        .admission
        .policies
        .approval
        .as_ref()
        .ok_or_else(|| {
            RepositoryError::InvalidInput("approval Task has no Policy requirement".to_owned())
        })?;
    let task = TaskPayload {
        definition: TaskDefinition::Approval {
            owner_version: record.version,
            owner_snapshot_digest: record.payload.admission.canonical_digest.clone(),
            effect: record.payload.admission.effect,
            input_digest: record.payload.admission.input.content_digest.clone(),
            policy_revision_id: requirement.policy_revision.revision_id.clone(),
            approver_rule_digest: requirement.eligible_principal_rule_digest.clone(),
            safe_prompt_key: requirement.safe_prompt_key.clone(),
        },
        created_by: record.payload.admission.principal.clone(),
        resolution: None,
    };
    task.validate()
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let payload = TypedPayload::new(1, &task)?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.tasks (
            tenant_id, task_id, task_kind, owner_kind, owner_id, run_id, node_id,
            invocation_id, state, generation, version, response_schema_digest,
            principal_snapshot_schema_version, payload_schema_version, payload,
            payload_digest, response_value_id, deadline, responded_at,
            created_at, updated_at
        ) VALUES (
            $1, $2, 'approval', 'capability_invocation', $3, $4, $5,
            $3, 'pending', 1, 1, NULL,
            1, $6, $7, $8, NULL, $9, NULL, $10, $10
        )
        "#,
    )
    .bind(record.tenant_id.to_string())
    .bind(task_id.to_string())
    .bind(record.invocation_id.to_string())
    .bind(record.run_id.to_string())
    .bind(record.node_execution_id.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(record.deadline)
    .bind(record.created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_input_artifact_reference(
    transaction: &mut Transaction<'_, Postgres>,
    record: &CapabilityInvocationRecord,
    link_id: &ResourceId,
) -> Result<(), RepositoryError> {
    let InvocationValueStorage::Artifact { artifact } = &record.payload.admission.input.storage
    else {
        return Err(RepositoryError::InvalidInput(
            "inline Capability input cannot create an ArtifactLink".to_owned(),
        ));
    };
    if record.payload.admission.input_artifact_link_id.as_ref() != Some(link_id) {
        return Err(RepositoryError::InvalidInput(
            "Capability input ArtifactLink differs from frozen admission".to_owned(),
        ));
    }
    let snapshot = ArtifactReferenceSnapshot {
        schema_version: 1,
        artifact_id: artifact.artifact_id().clone(),
        owner_id: record.invocation_id.clone(),
        reference_kind: ArtifactReferenceKind::Input,
        purpose: ArtifactPurpose::CapabilityInput,
        created_by: record.payload.admission.principal.principal_id.clone(),
    };
    let payload = TypedPayload::from_versioned(1, &snapshot, 262_144)?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_links (
            tenant_id, artifact_link_id, link_kind, owner_kind, owner_id,
            target_artifact_id, link_key_digest, state, payload_schema_version,
            payload, payload_digest, created_at, updated_at
        ) VALUES (
            $1, $2, 'reference', 'capability_invocation', $3,
            $4, $5, 'active', $6, $7, $8, $9, $9
        )
        "#,
    )
    .bind(record.tenant_id.to_string())
    .bind(link_id.to_string())
    .bind(record.invocation_id.to_string())
    .bind(artifact.artifact_id().to_string())
    .bind(
        snapshot
            .link_key_digest()
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
            .to_string(),
    )
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(record.created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

pub(crate) async fn load_capability_invocation(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    invocation_id: &ResourceId,
    for_update: bool,
) -> Result<CapabilityInvocationRecord, RepositoryError> {
    let query = if for_update {
        "SELECT * FROM insight_platform.invocations WHERE tenant_id = $1 AND invocation_id = $2 FOR UPDATE"
    } else {
        "SELECT * FROM insight_platform.invocations WHERE tenant_id = $1 AND invocation_id = $2"
    };
    let row = sqlx::query(query)
        .bind(tenant_id.to_string())
        .bind(invocation_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::NotFound("CapabilityInvocation"))?;
    capability_invocation_from_row(row)
}

fn capability_invocation_from_row(
    row: PgRow,
) -> Result<CapabilityInvocationRecord, RepositoryError> {
    let invocation_kind: String = row.try_get("invocation_kind")?;
    if invocation_kind != "capability" {
        return Err(RepositoryError::CorruptRow(
            "CapabilityInvocation row has the wrong invocation kind".to_owned(),
        ));
    }
    let payload = payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
    let payload: CapabilityInvocationPayload = serde_json::from_value(payload.value)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    let owner_kind_name: String = row.try_get("owner_kind")?;
    let owner_kind = ResourceKind::ALL
        .iter()
        .copied()
        .find(|kind| kind.descriptor().name == owner_kind_name)
        .ok_or_else(|| RepositoryError::CorruptRow("unknown Invocation owner kind".to_owned()))?;
    let record = CapabilityInvocationRecord {
        tenant_id: parse_id(&row.try_get::<String, _>("tenant_id")?, "Invocation tenant")?,
        invocation_id: parse_id(&row.try_get::<String, _>("invocation_id")?, "Invocation")?,
        run_id: parse_id(&row.try_get::<String, _>("run_id")?, "Invocation Run")?,
        node_execution_id: parse_id(&row.try_get::<String, _>("node_id")?, "Invocation Node")?,
        owner_kind,
        owner_id: parse_id(&row.try_get::<String, _>("owner_id")?, "Invocation owner")?,
        logical_key: row.try_get("logical_key")?,
        deployment_id: parse_id(
            &row.try_get::<String, _>("deployment_id")?,
            "Invocation Deployment",
        )?,
        input_value_id: parse_id(
            &row.try_get::<String, _>("input_value_id")?,
            "Invocation input",
        )?,
        output_value_id: row
            .try_get::<Option<String>, _>("output_value_id")?
            .map(|value| parse_id(&value, "Invocation output"))
            .transpose()?,
        effect_key_digest: parse_digest(
            row.try_get::<Option<String>, _>("effect_key_digest")?
                .ok_or_else(|| {
                    RepositoryError::CorruptRow("CapabilityInvocation has no effect key".to_owned())
                })?,
            "Invocation effect key",
        )?,
        state: row
            .try_get::<String, _>("state")?
            .parse::<InvocationState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        version: parse_u64(row.try_get("version")?, "Invocation version")?,
        payload,
        deadline: row.try_get("deadline")?,
        retry_at: row.try_get("retry_at")?,
        started_at: row.try_get("started_at")?,
        terminal_at: row.try_get("terminal_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    };
    record
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(record)
}

fn require_admission_replay(
    record: &CapabilityInvocationRecord,
    command: &AdmitCapabilityInvocation,
) -> Result<(), RepositoryError> {
    if record.invocation_id != command.invocation_id
        || record.run_id != command.run_id
        || record.node_execution_id != command.node_execution_id
        || record.input_value_id != command.input_value_id
        || record.payload.admission.input_artifact_link_id != command.input_artifact_link_id
        || record.payload.admission.slot_id != command.slot_id
        || record.payload.admission.origin_key != command.origin
        || record.payload.approval_task_id != command.approval_task_id
    {
        return Err(RepositoryError::Conflict("Capability admission replay"));
    }
    Ok(())
}

fn require_exact_approval_task(
    current: &CapabilityInvocationRecord,
    task: &crate::repository::TaskRecord,
    projection: &insight_platform_tasks::TaskProjection,
    command: &ResolveCapabilityApproval,
) -> Result<(), RepositoryError> {
    let requirement = current
        .payload
        .admission
        .policies
        .approval
        .as_ref()
        .ok_or(RepositoryError::Conflict("Capability approval requirement"))?;
    let TaskDefinition::Approval {
        owner_version,
        owner_snapshot_digest,
        effect,
        input_digest,
        policy_revision_id,
        approver_rule_digest,
        ..
    } = &projection.payload.definition
    else {
        return Err(RepositoryError::CorruptRow(
            "Capability approval Task has the wrong definition".to_owned(),
        ));
    };
    if task.owner_kind != "capability_invocation"
        || task.owner_id != current.invocation_id.to_string()
        || task.invocation_id.as_deref() != Some(current.invocation_id.to_string().as_str())
        || task.run_id.as_deref() != Some(current.run_id.to_string().as_str())
        || task.node_id.as_deref() != Some(current.node_execution_id.to_string().as_str())
        || projection.state != TaskState::Pending
        || projection.generation != command.expected_task_generation
        || projection.version != command.expected_task_version
        || projection.deadline != current.deadline
        || *owner_version != current.version
        || owner_snapshot_digest != &current.payload.admission.canonical_digest
        || *effect != current.payload.admission.effect
        || input_digest != &current.payload.admission.input.content_digest
        || policy_revision_id != &requirement.policy_revision.revision_id
        || approver_rule_digest != &command.eligible_principal_rule_digest
    {
        return Err(RepositoryError::Conflict(
            "Capability approval Task binding",
        ));
    }
    Ok(())
}

fn require_approval_replay(
    record: &CapabilityInvocationRecord,
    decision: CapabilityApprovalDecision,
) -> Result<(), RepositoryError> {
    let expected = match decision {
        CapabilityApprovalDecision::Approve => InvocationState::Ready,
        CapabilityApprovalDecision::Reject => InvocationState::Failed,
    };
    if record.state != expected {
        return Err(RepositoryError::Conflict("Capability approval replay"));
    }
    Ok(())
}

fn parse_id(value: &str, kind: &str) -> Result<ResourceId, RepositoryError> {
    value
        .parse()
        .map_err(|failure| RepositoryError::CorruptRow(format!("{kind}: {failure}")))
}

fn parse_digest(value: String, kind: &str) -> Result<Sha256Digest, RepositoryError> {
    value
        .parse()
        .map_err(|failure| RepositoryError::CorruptRow(format!("{kind}: {failure}")))
}

fn parse_u64(value: i64, kind: &str) -> Result<u64, RepositoryError> {
    u64::try_from(value).map_err(|_| RepositoryError::CorruptRow(format!("negative {kind}")))
}
