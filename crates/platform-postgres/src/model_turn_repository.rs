use crate::{
    invocation_repository::load_enabled_exact_published_version,
    repository::{
        append_command_event, append_scheduler_event, claim_command_receipt,
        decode_deployment_closure, decode_versioned_payload, job_from_row, job_projection,
        load_deployment, load_job_for_update_by_text, load_resource, load_run_for_update,
        require_tenant_permission, terminalize_command_receipt, JobRecord, PgRepository,
        RepositoryError, RunRecord, TypedPayload,
    },
};
use chrono::{DateTime, Utc};
use insight_platform_artifacts::ArtifactReferenceSnapshot;
use insight_platform_contracts::{
    canonical_digest, ArtifactPurpose, ArtifactRef, ArtifactReferenceKind, CommandOutcome,
    DeploymentClosure, EntityLifecycle, FrozenSlotTarget, JobState, ModelTurnState,
    NodeExecutionState, Permission, PlanNodeKind, QuotaDimension, RegistryResourceKind,
    ResourceDocument, ResourceId, ResourceKind, RunState, Sha256Digest, ValueRef, WorkClass,
};
use insight_platform_invocations::InvocationValueStorage;
use insight_platform_jobs::{JobFence, JobProjection, LeasePolicy};
use insight_platform_model_adapters::{ModelAdapterExecutionRequest, ModelExecutionAuthority};
use insight_platform_models::{
    decide_model_cancellation_outcome, decide_model_control, decide_model_outcome,
    decide_model_turn_admission, decide_prepare_model_dispatch, decide_start_model_dispatch,
    CanonicalMessagePart, ClaimModelJobs, CommitModelCancellationOutcome, CommitModelOutcome,
    ControlModelTurn, CreateModelTurn, ModelAdmissionFacts, ModelControlKind, ModelDispatchOutcome,
    ModelExecutionInput, ModelExecutionInputMaterial, ModelJobPayload, ModelOutputValue,
    ModelQuotaCeiling, ModelQuotaSettlement, ModelRequestValue, ModelTurnLimits, ModelTurnPayload,
    ModelTurnRecord, ModelTurnStore, ModelTurnTransaction, ModelWorkerAudit, PrepareModelDispatch,
    PreparedModelDispatch, MODEL_QUOTA_LINES,
};
use sqlx::{postgres::PgRow, Acquire, Postgres, Row, Transaction};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq)]
pub struct PreparedModelExecution {
    pub turn: ModelTurnRecord,
    pub job: JobRecord,
}

#[async_trait::async_trait]
impl ModelExecutionAuthority for PgRepository {
    type Error = RepositoryError;
    type Record = PreparedModelExecution;

    async fn commit_model_outcome(
        &self,
        command: CommitModelOutcome,
    ) -> Result<CommandOutcome<Self::Record>, Self::Error> {
        let mut transaction = self.begin_model_turn().await?;
        let result = ModelTurnTransaction::commit_model_outcome(&mut transaction, command).await?;
        ModelTurnTransaction::commit(transaction).await?;
        Ok(result)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimedModelExecution {
    pub turn: ModelTurnRecord,
    pub job: JobRecord,
    pub request_input: ModelExecutionInput,
    pub quota_account_ids: Vec<String>,
    pub fence: JobFence,
    pub usage_reservation_id: ResourceId,
    pub quota_entry_ids: Vec<ResourceId>,
}

impl ClaimedModelExecution {
    pub fn job_projection(&self) -> Result<JobProjection, RepositoryError> {
        job_projection(&self.job)
    }

    /// Builds the credential-free, exact Provider request after the canonical request bytes have
    /// been materialized from their inline value or Ready Artifact and digest-checked.
    pub fn adapter_execution(
        &self,
        request: insight_platform_models::CanonicalModelRequest,
        worker_manifest_digest: Sha256Digest,
        limits: ModelTurnLimits,
    ) -> Result<ModelAdapterExecutionRequest, RepositoryError> {
        self.turn
            .validate(limits)
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        let projection = job_projection(&self.job)?;
        let payload: ModelJobPayload = decode_versioned_payload(&self.job.payload, "Model Job")?;
        payload
            .validate_for(&self.turn, &projection, limits)
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        if projection.state != JobState::Running
            || self.turn.payload.current_job_id.as_ref() != Some(&projection.job_id)
            || projection.version != self.fence.expected_version
            || projection.lease_generation != self.fence.lease_generation
            || projection.attempt_count == 0
            || payload.active_usage_reservation_id.as_ref() != Some(&self.usage_reservation_id)
        {
            return Err(RepositoryError::Conflict("claimed Model execution fence"));
        }
        let admission = &self.turn.payload.admission;
        let execution = ModelAdapterExecutionRequest {
            schema_version: 1,
            tenant_id: self.turn.tenant_id.clone(),
            model_turn_id: self.turn.model_turn_id.clone(),
            job_id: projection.job_id,
            worker_process_generation_id: self.fence.worker_process_generation_id.clone(),
            worker_manifest_digest,
            attempt_no: projection.attempt_count,
            attempt_limit: projection.attempt_limit,
            lease_generation: projection.lease_generation,
            admission_digest: admission.canonical_digest.clone(),
            request_digest: admission.request_digest.clone(),
            quota_ceiling: admission.quota_ceiling,
            model_deployment: admission.model_deployment.clone(),
            model_closure: admission.model_closure.clone(),
            profile_revision: admission.profile_revision.clone(),
            provider_deployment: admission.provider_deployment.clone(),
            provider_closure: admission.provider_closure.clone(),
            provider_revision: admission.provider_revision.clone(),
            provider: admission.provider.clone(),
            profile: admission.profile.clone(),
            request: Box::new(request),
        };
        execution
            .validate_at(Utc::now(), limits)
            .map_err(|_| RepositoryError::Conflict("Model adapter execution contract"))?;
        Ok(execution)
    }

    pub fn adapter_job(
        &self,
        request: insight_platform_models::CanonicalModelRequest,
        worker_manifest_digest: Sha256Digest,
        audit: ModelWorkerAudit,
        quota_settlement_entry_ids: Vec<ResourceId>,
        limits: ModelTurnLimits,
    ) -> Result<insight_platform_model_adapters::ExecuteModelAdapterJob, RepositoryError> {
        let reservation_ids = self.quota_entry_ids.iter().collect::<BTreeSet<_>>();
        if quota_settlement_entry_ids
            .iter()
            .any(|identity| reservation_ids.contains(identity))
        {
            return Err(RepositoryError::InvalidInput(
                "Model quota settlement identities reuse reservation identities".to_owned(),
            ));
        }
        let execution = self.adapter_execution(request, worker_manifest_digest, limits)?;
        let command = insight_platform_model_adapters::ExecuteModelAdapterJob {
            audit,
            expected_turn_version: self.turn.version,
            fence: self.fence.clone(),
            usage_reservation_id: self.usage_reservation_id.clone(),
            quota_entry_ids: quota_settlement_entry_ids,
            execution,
        };
        command
            .validate_at(Utc::now())
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        Ok(command)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ControlledModelExecution {
    pub turn: ModelTurnRecord,
    pub job: Option<JobRecord>,
}

pub struct PgModelTurnTransaction {
    transaction: Transaction<'static, Postgres>,
    limits: ModelTurnLimits,
}

#[derive(Debug)]
struct LockedModelNode {
    run_id: String,
    scope_instance_id: ResourceId,
    state: NodeExecutionState,
    version: u64,
    node_kind: PlanNodeKind,
    deadline: DateTime<Utc>,
}

#[derive(Debug)]
struct ModelClaimCandidate<'a> {
    job: JobRecord,
    model_deployment_id: ResourceId,
    quota_ceiling: ModelQuotaCeiling,
    slot: &'a insight_platform_models::ModelClaimSlot,
}

#[derive(Debug, Clone)]
struct ModelQuotaAccount {
    tenant_id: String,
    quota_account_id: String,
    scope_kind: String,
    scope_id: String,
    work_class: String,
    metric: String,
    limit_value: i64,
    reserved_value: i64,
    used_value: i64,
    version: i64,
}

#[derive(Debug, Clone)]
struct ModelQuotaReservationLine {
    account: ModelQuotaAccount,
    reserved_amount: i64,
}

impl PgRepository {
    pub async fn begin_model_turn_transaction(
        &self,
    ) -> Result<PgModelTurnTransaction, RepositoryError> {
        Ok(PgModelTurnTransaction {
            transaction: self.pool().begin().await?,
            limits: self.model_turn_limits(),
        })
    }
}

impl ModelTurnStore for PgRepository {
    type Error = RepositoryError;
    type Transaction<'a>
        = PgModelTurnTransaction
    where
        Self: 'a;

    async fn begin_model_turn(&self) -> Result<Self::Transaction<'_>, Self::Error> {
        self.begin_model_turn_transaction().await
    }
}

impl ModelTurnTransaction for PgModelTurnTransaction {
    type Error = RepositoryError;
    type ExecutionRecord = PreparedModelExecution;
    type ControlRecord = ControlledModelExecution;

    async fn create_model_turn(
        &mut self,
        command: CreateModelTurn,
    ) -> Result<CommandOutcome<ModelTurnRecord>, Self::Error> {
        command.validate_at(Utc::now(), self.limits)?;
        let mut transaction = self.transaction.begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now, self.limits)?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "model_turn",
            &command.model_turn_id.to_string(),
            "model.create",
        )
        .await?
        {
            let record = load_model_turn(
                &mut transaction,
                &command.audit.tenant_id,
                &command.model_turn_id,
                false,
                self.limits,
            )
            .await?;
            require_model_create_replay(&record, &command)?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(record));
        }

        let run = load_run_for_update(&mut transaction, &command.audit.tenant_id, &command.run_id)
            .await?;
        let principal =
            require_tenant_permission(&mut transaction, &command.audit, Permission::ModelInvoke)
                .await?;
        let node = load_model_node_for_update(
            &mut transaction,
            &command.audit.tenant_id,
            &command.node_execution_id,
        )
        .await?;
        if node.run_id != run.run_id {
            return Err(RepositoryError::InvalidInput(
                "ModelTurn NodeExecution does not belong to its Run".to_owned(),
            ));
        }

        let selected = selected_model_deployment(&run, &command)?;
        let model_deployment = load_deployment(
            &mut transaction,
            &command.audit.tenant_id,
            &selected.deployment_id,
        )
        .await?;
        if model_deployment.bindings.digest != selected.deployment_digest.to_string() {
            return Err(RepositoryError::Conflict("exact Model Deployment binding"));
        }
        let model_resource_id = parse_id(&model_deployment.resource_id, "Model Profile resource")?;
        let model_resource = load_resource(
            &mut transaction,
            &command.audit.tenant_id,
            &model_resource_id,
        )
        .await?;
        require_enabled_resource(
            &model_resource,
            RegistryResourceKind::ModelProfile,
            "Model Deployment gate",
        )?;
        let model_closure = match decode_deployment_closure(&model_deployment.bindings)? {
            DeploymentClosure::ModelProfile(closure) => closure,
            _ => {
                return Err(RepositoryError::CorruptRow(
                    "Model Deployment has a non-Model closure".to_owned(),
                ));
            }
        };
        if model_deployment.resource_version_id
            != model_closure.profile_revision.revision_id.to_string()
        {
            return Err(RepositoryError::CorruptRow(
                "Model Deployment root revision differs from its Profile closure".to_owned(),
            ));
        }
        let profile_payload = load_enabled_exact_published_version(
            &mut transaction,
            &command.audit.tenant_id,
            &model_closure.profile_revision,
            RegistryResourceKind::ModelProfile,
        )
        .await?;
        let ResourceDocument::ModelProfile(profile) = profile_payload.document else {
            return Err(RepositoryError::CorruptRow(
                "Model Profile revision contains the wrong document".to_owned(),
            ));
        };

        let provider_deployment = load_deployment(
            &mut transaction,
            &command.audit.tenant_id,
            &model_closure.provider_deployment.deployment_id,
        )
        .await?;
        if provider_deployment.bindings.digest
            != model_closure
                .provider_deployment
                .deployment_digest
                .to_string()
        {
            return Err(RepositoryError::Conflict(
                "exact Model Provider Deployment binding",
            ));
        }
        let provider_resource_id =
            parse_id(&provider_deployment.resource_id, "Model Provider resource")?;
        let provider_resource = load_resource(
            &mut transaction,
            &command.audit.tenant_id,
            &provider_resource_id,
        )
        .await?;
        require_enabled_resource(
            &provider_resource,
            RegistryResourceKind::ModelProvider,
            "Model Provider Deployment gate",
        )?;
        let provider_closure = match decode_deployment_closure(&provider_deployment.bindings)? {
            DeploymentClosure::ModelProvider(closure) => closure,
            _ => {
                return Err(RepositoryError::CorruptRow(
                    "Model Provider Deployment has a non-Provider closure".to_owned(),
                ));
            }
        };
        if provider_deployment.resource_version_id
            != provider_closure.provider_revision.revision_id.to_string()
        {
            return Err(RepositoryError::CorruptRow(
                "Provider Deployment root revision differs from its Provider closure".to_owned(),
            ));
        }
        let provider_payload = load_enabled_exact_published_version(
            &mut transaction,
            &command.audit.tenant_id,
            &provider_closure.provider_revision,
            RegistryResourceKind::ModelProvider,
        )
        .await?;
        let ResourceDocument::ModelProvider(provider) = provider_payload.document else {
            return Err(RepositoryError::CorruptRow(
                "Model Provider revision contains the wrong document".to_owned(),
            ));
        };
        let profile_revision = model_closure.profile_revision.clone();
        let provider_revision = provider_closure.provider_revision.clone();

        validate_model_tool_projections(
            &mut transaction,
            &command.audit.tenant_id,
            &run,
            &command.request,
        )
        .await?;
        require_committed_tool_results(
            &mut transaction,
            &command.audit.tenant_id,
            &command.run_id,
            &command.request,
        )
        .await?;
        lock_model_request_artifacts(&mut transaction, &command.audit.tenant_id, &command.request)
            .await?;

        let record = decide_model_turn_admission(
            &command,
            ModelAdmissionFacts {
                run_state: run
                    .state
                    .parse::<RunState>()
                    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
                run_version: parse_u64(run.version, "Run version")?,
                run_pause_requested: run.current.control.pause_requested,
                run_cancel_requested: run.current.control.cancel_requested_at.is_some(),
                run_timeout_requested: run.current.control.timeout_requested_at.is_some(),
                run_deadline: run.deadline,
                run_bindings: run.bindings,
                node_state: node.state,
                node_version: node.version,
                node_kind: node.node_kind,
                node_scope_instance_id: node.scope_instance_id,
                node_deadline: node.deadline,
                model_deployment: selected,
                model_closure,
                model_gate_enabled: true,
                profile_revision,
                profile: *profile,
                provider_deployment: insight_platform_contracts::ExactDeploymentRef {
                    deployment_id: parse_id(
                        &provider_deployment.deployment_id,
                        "Model Provider Deployment",
                    )?,
                    deployment_digest: provider_deployment.bindings.digest.parse().map_err(
                        |failure| {
                            RepositoryError::CorruptRow(format!(
                                "Provider Deployment digest: {failure}"
                            ))
                        },
                    )?,
                    resource_kind: ResourceKind::ModelProviderDeployment,
                },
                provider_closure,
                provider_gate_enabled: true,
                provider_revision,
                provider,
                principal,
                database_now,
            },
            self.limits,
        )?;
        validate_exact_model_policies(
            &mut transaction,
            &record.tenant_id,
            &record.payload.admission.policies,
        )
        .await?;
        insert_model_request_value(&mut transaction, &record, &command.request).await?;
        insert_model_turn(&mut transaction, &record).await?;
        insert_model_input_artifact_references(&mut transaction, &record, &command.request).await?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "model_turn",
            &record.model_turn_id.to_string(),
            1,
            "model.admitted",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "admission_digest": record.payload.admission.canonical_digest,
                    "deployment_id": record.deployment_id,
                    "state": record.state,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &record.model_turn_id.to_string(),
            "admitted",
        )
        .await?;
        let persisted = load_model_turn(
            &mut transaction,
            &record.tenant_id,
            &record.model_turn_id,
            false,
            self.limits,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(persisted))
    }

    async fn prepare_model_dispatch(
        &mut self,
        command: PrepareModelDispatch,
    ) -> Result<CommandOutcome<Self::ExecutionRecord>, Self::Error> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now)?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "model_turn",
            &command.model_turn_id.to_string(),
            "model.dispatch.prepare",
        )
        .await?
        {
            let turn = load_model_turn(
                &mut transaction,
                &command.audit.tenant_id,
                &command.model_turn_id,
                false,
                self.limits,
            )
            .await?;
            if turn.payload.current_job_id.as_ref() != Some(&command.job_id) {
                return Err(RepositoryError::Conflict("Model dispatch replay"));
            }
            let job = load_model_job(
                &mut transaction,
                &command.audit.tenant_id,
                &command.job_id,
                false,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(PreparedModelExecution {
                turn,
                job,
            }));
        }
        let current = load_model_turn(
            &mut transaction,
            &command.audit.tenant_id,
            &command.model_turn_id,
            true,
            self.limits,
        )
        .await?;
        let principal =
            require_tenant_permission(&mut transaction, &command.audit, Permission::ModelInvoke)
                .await?;
        if principal != current.payload.admission.principal {
            return Err(RepositoryError::Conflict(
                "Model dispatch principal snapshot",
            ));
        }
        let prepared =
            decide_prepare_model_dispatch(&current, &command, database_now, self.limits)?;
        insert_model_job(&mut transaction, &prepared).await?;
        update_model_turn(&mut transaction, &current, &prepared.turn, self.limits).await?;
        append_command_event(
            &mut transaction,
            &command.audit,
            "model_turn",
            &command.model_turn_id.to_string(),
            as_i64(prepared.turn.version, "ModelTurn version")?,
            "model.dispatch_prepared",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "admission_digest": prepared.turn.payload.admission.canonical_digest,
                    "job_id": prepared.job.job_id,
                    "work_class": prepared.job.work_class,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &command.model_turn_id.to_string(),
            "prepared",
        )
        .await?;
        let turn = load_model_turn(
            &mut transaction,
            &command.audit.tenant_id,
            &command.model_turn_id,
            false,
            self.limits,
        )
        .await?;
        let job = load_model_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            false,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(PreparedModelExecution {
            turn,
            job,
        }))
    }

    async fn commit_model_outcome(
        &mut self,
        command: CommitModelOutcome,
    ) -> Result<CommandOutcome<Self::ExecutionRecord>, Self::Error> {
        self.commit_model_outcome_inner(command).await
    }

    async fn commit_model_cancellation_outcome(
        &mut self,
        command: CommitModelCancellationOutcome,
    ) -> Result<CommandOutcome<Self::ExecutionRecord>, Self::Error> {
        self.commit_model_cancellation_outcome_inner(command).await
    }

    async fn control_model_turn(
        &mut self,
        command: ControlModelTurn,
    ) -> Result<CommandOutcome<Self::ControlRecord>, Self::Error> {
        self.control_model_turn_inner(command).await
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

fn selected_model_deployment(
    run: &RunRecord,
    command: &CreateModelTurn,
) -> Result<insight_platform_contracts::ExactDeploymentRef, RepositoryError> {
    let slot = run
        .bindings
        .slots
        .iter()
        .find(|slot| slot.slot_id == command.slot_id)
        .ok_or(RepositoryError::NotFound("frozen Model slot"))?;
    let FrozenSlotTarget::Model { candidates, .. } = &slot.target else {
        return Err(RepositoryError::InvalidInput(
            "selected Run slot is not a Model".to_owned(),
        ));
    };
    candidates
        .get(usize::from(command.selected_candidate_ordinal))
        .cloned()
        .ok_or_else(|| {
            RepositoryError::InvalidInput(
                "Model selector chose an out-of-range candidate".to_owned(),
            )
        })
}

fn require_enabled_resource(
    resource: &crate::repository::ResourceRecord,
    expected: RegistryResourceKind,
    conflict: &'static str,
) -> Result<(), RepositoryError> {
    if resource.resource_kind != expected.as_str()
        || resource.lifecycle_state != EntityLifecycle::Active.as_str()
        || resource.gate_state != "enabled"
    {
        return Err(RepositoryError::Conflict(conflict));
    }
    Ok(())
}

async fn validate_exact_model_policies(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    policies: &[insight_platform_contracts::ExactVersionRef],
) -> Result<(), RepositoryError> {
    for policy in policies {
        let payload = load_enabled_exact_published_version(
            transaction,
            tenant_id,
            policy,
            RegistryResourceKind::Policy,
        )
        .await?;
        if !matches!(payload.document, ResourceDocument::Policy(_)) {
            return Err(RepositoryError::CorruptRow(
                "Model Policy revision contains the wrong document".to_owned(),
            ));
        }
    }
    Ok(())
}

async fn validate_model_tool_projections(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    run: &RunRecord,
    request: &ModelRequestValue,
) -> Result<(), RepositoryError> {
    for tool in &request.request.tools {
        let matches = run
            .bindings
            .slots
            .iter()
            .filter_map(|slot| match &slot.target {
                FrozenSlotTarget::Capability {
                    candidates,
                    tool_alias,
                    ..
                } if candidates.contains(&tool.capability_deployment)
                    && tool_alias
                        .as_deref()
                        .is_none_or(|alias| alias == tool.projected_name) =>
                {
                    Some(())
                }
                _ => None,
            })
            .count();
        if matches != 1 {
            return Err(RepositoryError::InvalidInput(
                "Model tool projection is not an unambiguous frozen Capability binding".to_owned(),
            ));
        }
        let deployment = load_deployment(
            transaction,
            tenant_id,
            &tool.capability_deployment.deployment_id,
        )
        .await?;
        if deployment.bindings.digest != tool.capability_deployment.deployment_digest.to_string() {
            return Err(RepositoryError::Conflict(
                "Model tool Capability Deployment binding",
            ));
        }
        let resource_id = parse_id(&deployment.resource_id, "Model tool Capability resource")?;
        let resource = load_resource(transaction, tenant_id, &resource_id).await?;
        require_enabled_resource(
            &resource,
            RegistryResourceKind::CapabilityInterface,
            "Model tool Capability gate",
        )?;
        let closure = match decode_deployment_closure(&deployment.bindings)? {
            DeploymentClosure::CapabilityInterface(closure) => closure,
            _ => {
                return Err(RepositoryError::CorruptRow(
                    "Model tool Capability has the wrong Deployment closure".to_owned(),
                ));
            }
        };
        if closure.interface != tool.interface_revision
            || deployment.resource_version_id != closure.interface.revision_id.to_string()
        {
            return Err(RepositoryError::Conflict(
                "Model tool Interface revision binding",
            ));
        }
        let published = load_enabled_exact_published_version(
            transaction,
            tenant_id,
            &tool.interface_revision,
            RegistryResourceKind::CapabilityInterface,
        )
        .await?;
        let ResourceDocument::CapabilityInterface(interface) = published.document else {
            return Err(RepositoryError::CorruptRow(
                "Model tool Interface revision contains the wrong document".to_owned(),
            ));
        };
        if interface.input_schema.canonical_digest != tool.input_schema.canonical_digest
            || interface.output_schema.canonical_digest != tool.output_schema_digest
            || interface.effect != tool.effect
        {
            return Err(RepositoryError::Conflict("Model tool projection contract"));
        }
    }
    Ok(())
}

async fn require_committed_tool_results(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    run_id: &ResourceId,
    request: &ModelRequestValue,
) -> Result<(), RepositoryError> {
    for result in request.request.messages.iter().flat_map(|message| {
        message.parts.iter().filter_map(|part| match part {
            CanonicalMessagePart::ToolResult(result) => Some(result),
            _ => None,
        })
    }) {
        let row = sqlx::query(
            r#"
            SELECT invocation.invocation_kind, invocation.run_id AS invocation_run_id,
                   invocation.node_id AS invocation_node_id, invocation.state,
                   invocation.output_value_id,
                   value.run_id AS value_run_id, value.node_id AS value_node_id,
                   value.value_kind, value.classification, value.schema_digest,
                   value.content_digest, value.inline_value, value.artifact_id
            FROM insight_platform.invocations AS invocation
            JOIN insight_platform.run_values AS value
              ON value.tenant_id = invocation.tenant_id
             AND value.value_id = invocation.output_value_id
            WHERE invocation.tenant_id = $1 AND invocation.invocation_id = $2
            FOR SHARE OF invocation, value
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(result.invocation_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::NotFound("committed Model tool result"))?;
        let invocation_node_id: String = row.try_get("invocation_node_id")?;
        if row.try_get::<String, _>("invocation_kind")? != "capability"
            || row.try_get::<String, _>("state")? != "succeeded"
            || row.try_get::<String, _>("invocation_run_id")? != run_id.to_string()
            || row.try_get::<String, _>("output_value_id")? != result.output_value_id.to_string()
            || row.try_get::<String, _>("value_run_id")? != run_id.to_string()
            || row
                .try_get::<Option<String>, _>("value_node_id")?
                .as_deref()
                != Some(invocation_node_id.as_str())
            || row.try_get::<String, _>("value_kind")? != "capability_output"
            || row.try_get::<String, _>("classification")? != result.classification.as_str()
            || row.try_get::<String, _>("schema_digest")? != result.output_schema_digest.to_string()
            || row.try_get::<String, _>("content_digest")? != result.content_digest.to_string()
        {
            return Err(RepositoryError::Conflict(
                "committed Model tool result binding",
            ));
        }
        let inline_value: Option<serde_json::Value> = row.try_get("inline_value")?;
        let artifact_id: Option<String> = row.try_get("artifact_id")?;
        match (&result.value, inline_value, artifact_id) {
            (ValueRef::Inline { value }, Some(stored), None) if value == &stored => {}
            (ValueRef::Artifact { artifact }, None, Some(stored_id))
                if stored_id == artifact.artifact_id().to_string() =>
            {
                lock_ready_model_artifact(transaction, tenant_id, artifact).await?;
            }
            _ => {
                return Err(RepositoryError::Conflict(
                    "committed Model tool result storage",
                ));
            }
        }
    }
    Ok(())
}

async fn lock_model_request_artifacts(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    request: &ModelRequestValue,
) -> Result<(), RepositoryError> {
    let mut link_ids = BTreeSet::new();
    let mut artifact_ids = BTreeSet::new();
    if let ValueRef::Artifact { artifact } = &request.value {
        let link_id = request.artifact_link_id.as_ref().ok_or_else(|| {
            RepositoryError::InvalidInput("Artifact-backed Model request has no link ID".to_owned())
        })?;
        link_ids.insert(link_id.clone());
        artifact_ids.insert(artifact.artifact_id().clone());
        lock_ready_model_artifact(transaction, tenant_id, artifact).await?;
    } else if request.artifact_link_id.is_some() {
        return Err(RepositoryError::InvalidInput(
            "inline Model request has an ArtifactLink".to_owned(),
        ));
    }
    for input in &request.request.artifact_inputs {
        if !link_ids.insert(input.artifact_link_id.clone())
            || !artifact_ids.insert(input.artifact.artifact_id().clone())
        {
            return Err(RepositoryError::InvalidInput(
                "Model request Artifact identities are not unique".to_owned(),
            ));
        }
        lock_ready_model_artifact(transaction, tenant_id, &input.artifact).await?;
    }
    Ok(())
}

async fn lock_ready_model_artifact(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    artifact: &ArtifactRef,
) -> Result<(), RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT artifact.classification, artifact.verified_media_type,
               blob.content_digest, blob.size_bytes
        FROM insight_platform.artifacts AS artifact
        JOIN insight_platform.artifact_blobs AS blob
          ON blob.tenant_id = artifact.tenant_id AND blob.blob_id = artifact.blob_id
        WHERE artifact.tenant_id = $1 AND artifact.artifact_id = $2
          AND artifact.state = 'ready' AND artifact.terminal_at IS NULL
          AND blob.state = 'verified' AND blob.deleted_at IS NULL
        FOR UPDATE OF artifact, blob
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(artifact.artifact_id().to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("ready Model Artifact"))?;
    if row.try_get::<String, _>("classification")? != artifact.classification().as_str()
        || row.try_get::<String, _>("verified_media_type")? != artifact.media_type()
        || row.try_get::<String, _>("content_digest")? != artifact.content_digest().to_string()
        || u64::try_from(row.try_get::<i64, _>("size_bytes")?).ok() != Some(artifact.byte_length())
    {
        return Err(RepositoryError::Conflict("Model Artifact identity"));
    }
    Ok(())
}

async fn insert_model_request_value(
    transaction: &mut Transaction<'_, Postgres>,
    record: &ModelTurnRecord,
    request: &ModelRequestValue,
) -> Result<(), RepositoryError> {
    let (inline_value, artifact_id) = match &request.value {
        ValueRef::Inline { value } => (Some(value), None),
        ValueRef::Artifact { artifact } => (None, Some(artifact.artifact_id().to_string())),
    };
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_values (
            tenant_id, value_id, run_id, node_id, value_kind, classification,
            schema_digest, content_digest, inline_value, artifact_id, created_at
        ) VALUES ($1, $2, $3, $4, 'model_request', $5, $6, $7, $8, $9, $10)
        "#,
    )
    .bind(record.tenant_id.to_string())
    .bind(request.value_id.to_string())
    .bind(record.run_id.to_string())
    .bind(record.node_execution_id.to_string())
    .bind(request.classification.as_str())
    .bind(request.schema_digest.to_string())
    .bind(request.content_digest.to_string())
    .bind(inline_value)
    .bind(artifact_id)
    .bind(record.created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_model_turn(
    transaction: &mut Transaction<'_, Postgres>,
    record: &ModelTurnRecord,
) -> Result<(), RepositoryError> {
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
            $1, $2, 'model', 'node_execution', $3,
            $4, $5, $3, $6, $7, $8,
            $9, NULL, NULL, $10, $11, $12, $13,
            NULL, NULL, NULL, $14, $14
        )
        "#,
    )
    .bind(record.tenant_id.to_string())
    .bind(record.model_turn_id.to_string())
    .bind(record.node_execution_id.to_string())
    .bind(&record.logical_key)
    .bind(record.run_id.to_string())
    .bind(record.deployment_id.to_string())
    .bind(record.state.as_str())
    .bind(as_i64(record.version, "ModelTurn version")?)
    .bind(record.request_value_id.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(record.deadline)
    .bind(record.created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn insert_model_input_artifact_references(
    transaction: &mut Transaction<'_, Postgres>,
    record: &ModelTurnRecord,
    request: &ModelRequestValue,
) -> Result<(), RepositoryError> {
    let mut references = Vec::new();
    if let ValueRef::Artifact { artifact } = &request.value {
        references.push((
            request.artifact_link_id.as_ref().ok_or_else(|| {
                RepositoryError::InvalidInput(
                    "Artifact-backed Model request has no link ID".to_owned(),
                )
            })?,
            artifact,
        ));
    }
    references.extend(
        request
            .request
            .artifact_inputs
            .iter()
            .map(|input| (&input.artifact_link_id, &input.artifact)),
    );
    for (link_id, artifact) in references {
        insert_model_artifact_reference(
            transaction,
            record,
            link_id,
            artifact,
            ArtifactReferenceKind::Input,
            ArtifactPurpose::RunInput,
            record.created_at,
        )
        .await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_model_artifact_reference(
    transaction: &mut Transaction<'_, Postgres>,
    record: &ModelTurnRecord,
    link_id: &ResourceId,
    artifact: &ArtifactRef,
    reference_kind: ArtifactReferenceKind,
    purpose: ArtifactPurpose,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let snapshot = ArtifactReferenceSnapshot {
        schema_version: 1,
        artifact_id: artifact.artifact_id().clone(),
        owner_id: record.model_turn_id.clone(),
        reference_kind,
        purpose,
        created_by: record.payload.admission.principal.principal_id.clone(),
    };
    let payload = TypedPayload::from_versioned(1, &snapshot, 262_144)?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.artifact_links (
            tenant_id, artifact_link_id, link_kind, owner_kind, owner_id,
            target_artifact_id, link_key_digest, state, payload_schema_version,
            payload, payload_digest, created_at, updated_at
        ) VALUES ($1, $2, 'reference', 'model_turn', $3, $4, $5,
                  'active', $6, $7, $8, $9, $9)
        "#,
    )
    .bind(record.tenant_id.to_string())
    .bind(link_id.to_string())
    .bind(record.model_turn_id.to_string())
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
    .bind(database_now)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn require_model_create_replay(
    record: &ModelTurnRecord,
    command: &CreateModelTurn,
) -> Result<(), RepositoryError> {
    if record.run_id != command.run_id
        || record.node_execution_id != command.node_execution_id
        || record.scope_instance_id != command.scope_instance_id
        || record.round_ordinal != command.round_ordinal
        || record.payload.admission.slot_id != command.slot_id
        || record
            .payload
            .admission
            .selection_evidence
            .selected_candidate_ordinal
            != command.selected_candidate_ordinal
        || record
            .payload
            .admission
            .selection_evidence
            .selector_input_digest
            != command.selector_input_digest
        || record.request_value_id != command.request.value_id
        || record.payload.admission.request_digest != command.request.content_digest
        || record.payload.admission.request_artifact_link_id != command.request.artifact_link_id
        || record.payload.admission.attempt_limit != command.requested_attempt_limit
        || record.payload.admission.quota_ceiling.cost_microunits != command.cost_ceiling_microunits
    {
        return Err(RepositoryError::Conflict("ModelTurn create replay"));
    }
    Ok(())
}

async fn load_model_node_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    node_id: &ResourceId,
) -> Result<LockedModelNode, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT run_id, record_kind, scope_id, node_kind, state, version, deadline
        FROM insight_platform.run_nodes
        WHERE tenant_id = $1 AND node_id = $2
        FOR UPDATE
        "#,
    )
    .bind(tenant_id.to_string())
    .bind(node_id.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Model NodeExecution"))?;
    if row.try_get::<String, _>("record_kind")? != "node_execution" {
        return Err(RepositoryError::InvalidInput(
            "Model owner is not a NodeExecution".to_owned(),
        ));
    }
    Ok(LockedModelNode {
        run_id: row.try_get("run_id")?,
        scope_instance_id: parse_required_id(
            Some(&row.try_get::<String, _>("scope_id")?),
            ResourceKind::ScopeInstance,
            "Model Node scope",
        )?,
        state: row
            .try_get::<String, _>("state")?
            .parse::<NodeExecutionState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        version: parse_u64(row.try_get("version")?, "Model Node version")?,
        node_kind: row
            .try_get::<String, _>("node_kind")?
            .parse::<PlanNodeKind>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        deadline: row.try_get("deadline")?,
    })
}

async fn insert_model_job(
    transaction: &mut Transaction<'_, Postgres>,
    prepared: &PreparedModelDispatch,
) -> Result<(), RepositoryError> {
    let payload = TypedPayload::from_versioned(1, &prepared.job_payload, 1_048_576)?;
    sqlx::query(
        r#"
        INSERT INTO insight_platform.jobs (
            tenant_id, job_id, work_class, owner_kind, owner_id, invocation_id,
            run_id, node_id, state, version, attempt_no, attempt_limit, lease_epoch,
            scheduled_at, deadline, priority, request_digest, effect_key_digest,
            payload_schema_version, payload, payload_digest, created_at, updated_at
        ) VALUES (
            $1, $2, 'model', 'model_turn', $3, $3,
            $4, $5, 'ready', 1, 0, $6, 0,
            $7, $8, 0, $9, NULL, $10, $11, $12, $13, $13
        )
        "#,
    )
    .bind(prepared.turn.tenant_id.to_string())
    .bind(prepared.job.job_id.to_string())
    .bind(prepared.turn.model_turn_id.to_string())
    .bind(prepared.turn.run_id.to_string())
    .bind(prepared.turn.node_execution_id.to_string())
    .bind(i32::try_from(prepared.job.attempt_limit).map_err(|_| {
        RepositoryError::InvalidInput("Model attempt limit exceeds integer".to_owned())
    })?)
    .bind(prepared.job.scheduled_at)
    .bind(prepared.job.deadline)
    .bind(prepared.job_payload.binding.request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(prepared.turn.updated_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn update_model_turn(
    transaction: &mut Transaction<'_, Postgres>,
    current: &ModelTurnRecord,
    next: &ModelTurnRecord,
    limits: ModelTurnLimits,
) -> Result<(), RepositoryError> {
    next.validate(limits)?;
    let payload = TypedPayload::from_versioned(1, &next.payload, 1_048_576)?;
    let updated = sqlx::query(
        r#"
        UPDATE insight_platform.invocations
        SET state = $4, version = $5, output_value_id = $6,
            payload_schema_version = $7, payload = $8, payload_digest = $9,
            retry_at = $10, started_at = $11, terminal_at = $12, updated_at = $13
        WHERE tenant_id = $1 AND invocation_id = $2 AND version = $3
          AND invocation_kind = 'model'
        RETURNING invocation_id
        "#,
    )
    .bind(next.tenant_id.to_string())
    .bind(next.model_turn_id.to_string())
    .bind(as_i64(current.version, "ModelTurn version")?)
    .bind(next.state.as_str())
    .bind(as_i64(next.version, "ModelTurn version")?)
    .bind(next.output_value_id.as_ref().map(ToString::to_string))
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(next.retry_at)
    .bind(next.started_at)
    .bind(next.terminal_at)
    .bind(next.updated_at)
    .fetch_optional(&mut **transaction)
    .await?;
    if updated.is_none() {
        return Err(RepositoryError::Conflict("ModelTurn"));
    }
    Ok(())
}

async fn load_model_turn(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    model_turn_id: &ResourceId,
    for_update: bool,
    limits: ModelTurnLimits,
) -> Result<ModelTurnRecord, RepositoryError> {
    let query = if for_update {
        "SELECT * FROM insight_platform.invocations WHERE tenant_id = $1 AND invocation_id = $2 FOR UPDATE"
    } else {
        "SELECT * FROM insight_platform.invocations WHERE tenant_id = $1 AND invocation_id = $2"
    };
    let row = sqlx::query(query)
        .bind(tenant_id.to_string())
        .bind(model_turn_id.to_string())
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::NotFound("ModelTurn"))?;
    model_turn_from_row(row, limits)
}

fn model_turn_from_row(
    row: PgRow,
    limits: ModelTurnLimits,
) -> Result<ModelTurnRecord, RepositoryError> {
    if row.try_get::<String, _>("invocation_kind")? != "model"
        || row.try_get::<String, _>("owner_kind")? != "node_execution"
    {
        return Err(RepositoryError::CorruptRow(
            "ModelTurn row has the wrong Invocation or owner kind".to_owned(),
        ));
    }
    let payload = crate::repository::payload_from_row(
        &row,
        "payload_schema_version",
        "payload",
        "payload_digest",
    )?;
    let payload: ModelTurnPayload = decode_versioned_payload(&payload, "ModelTurn")?;
    let node_execution_id = parse_id(
        &row.try_get::<String, _>("node_id")?,
        "ModelTurn NodeExecution",
    )?;
    if row.try_get::<String, _>("owner_id")? != node_execution_id.to_string() {
        return Err(RepositoryError::CorruptRow(
            "ModelTurn owner differs from its NodeExecution".to_owned(),
        ));
    }
    let record = ModelTurnRecord {
        tenant_id: parse_id(&row.try_get::<String, _>("tenant_id")?, "ModelTurn tenant")?,
        model_turn_id: parse_id(&row.try_get::<String, _>("invocation_id")?, "ModelTurn")?,
        run_id: parse_id(&row.try_get::<String, _>("run_id")?, "ModelTurn Run")?,
        node_execution_id,
        scope_instance_id: payload.admission.scope_instance_id.clone(),
        round_ordinal: payload.admission.round_ordinal,
        logical_key: row.try_get("logical_key")?,
        deployment_id: parse_id(
            &row.try_get::<String, _>("deployment_id")?,
            "ModelTurn Deployment",
        )?,
        request_value_id: parse_id(
            &row.try_get::<String, _>("input_value_id")?,
            "ModelTurn request",
        )?,
        output_value_id: row
            .try_get::<Option<String>, _>("output_value_id")?
            .map(|value| parse_id(&value, "ModelTurn output"))
            .transpose()?,
        state: row
            .try_get::<String, _>("state")?
            .parse::<ModelTurnState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?,
        version: parse_u64(row.try_get("version")?, "ModelTurn version")?,
        payload,
        deadline: row.try_get("deadline")?,
        retry_at: row.try_get("retry_at")?,
        started_at: row.try_get("started_at")?,
        terminal_at: row.try_get("terminal_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    };
    record
        .validate(limits)
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(record)
}

async fn load_model_job(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    job_id: &ResourceId,
    for_update: bool,
) -> Result<JobRecord, RepositoryError> {
    let record = if for_update {
        load_job_for_update_by_text(transaction, &tenant_id.to_string(), &job_id.to_string())
            .await?
    } else {
        let row =
            sqlx::query("SELECT * FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2")
                .bind(tenant_id.to_string())
                .bind(job_id.to_string())
                .fetch_optional(&mut **transaction)
                .await?
                .ok_or(RepositoryError::NotFound("Model Job"))?;
        job_from_row(row)?
    };
    if record.work_class != WorkClass::Model.as_str() || record.owner_kind != "model_turn" {
        return Err(RepositoryError::CorruptRow(
            "Job is not a Model Job".to_owned(),
        ));
    }
    Ok(record)
}

impl PgRepository {
    pub async fn claim_model_jobs(
        &self,
        command: ClaimModelJobs,
    ) -> Result<Vec<ClaimedModelExecution>, RepositoryError> {
        command.validate(self.model_turn_limits())?;
        let mut transaction = self.pool().begin().await?;
        let database_now = database_now(&mut transaction).await?;
        let rows = sqlx::query(
            r#"
            SELECT * FROM insight_platform.jobs
            WHERE work_class = 'model' AND state IN ('ready', 'retry_scheduled')
              AND terminal_at IS NULL AND worker_id IS NULL
              AND scheduled_at <= $1 AND (retry_at IS NULL OR retry_at <= $1)
              AND deadline > $1
            ORDER BY priority DESC, COALESCE(retry_at, scheduled_at), job_id
            LIMIT $2
            "#,
        )
        .bind(database_now)
        .bind(i64::from(command.limit))
        .fetch_all(&mut *transaction)
        .await?;
        let candidates = rows
            .into_iter()
            .zip(&command.slots)
            .map(|(row, slot)| model_claim_candidate(job_from_row(row)?, slot))
            .collect::<Result<Vec<_>, RepositoryError>>()?;
        // Quota is the first mutable authority. The complete, sorted batch is locked before
        // Run -> Node -> ModelTurn -> Job so overlapping tenants/deployments cannot invert locks.
        let mut quota_accounts = lock_model_quota_accounts(&mut transaction, &candidates).await?;
        let mut claimed = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let ModelClaimCandidate {
                job: observed_job,
                model_deployment_id,
                quota_ceiling,
                slot,
            } = candidate;
            let tenant_id = parse_required_id(
                Some(&observed_job.tenant_id),
                ResourceKind::Tenant,
                "Model Job tenant",
            )?;
            let run_id = parse_required_id(
                observed_job.run_id.as_deref(),
                ResourceKind::Run,
                "Model Job Run",
            )?;
            let node_id = parse_required_id(
                observed_job.node_id.as_deref(),
                ResourceKind::NodeExecution,
                "Model Job NodeExecution",
            )?;
            let model_turn_id = parse_required_id(
                Some(&observed_job.owner_id),
                ResourceKind::ModelTurn,
                "Model Job owner",
            )?;
            let invocation_id = parse_required_id(
                observed_job.invocation_id.as_deref(),
                ResourceKind::ModelTurn,
                "Model Job Invocation",
            )?;
            let job_id =
                parse_required_id(Some(&observed_job.job_id), ResourceKind::Job, "Model Job")?;
            if invocation_id != model_turn_id {
                return Err(RepositoryError::CorruptRow(
                    "Model Job owner and Invocation differ".to_owned(),
                ));
            }
            let run = load_run_for_update(&mut transaction, &tenant_id, &run_id).await?;
            let node = load_model_node_for_update(&mut transaction, &tenant_id, &node_id).await?;
            let current_turn = load_model_turn(
                &mut transaction,
                &tenant_id,
                &model_turn_id,
                true,
                self.model_turn_limits(),
            )
            .await?;
            require_model_claim_parents(&mut transaction, &current_turn, &run, &node, database_now)
                .await?;
            let request_input = load_model_execution_input(&mut transaction, &current_turn).await?;
            let current_job = load_model_job(&mut transaction, &tenant_id, &job_id, true).await?;
            if current_job.version != observed_job.version
                || current_job.state != observed_job.state
                || current_job.worker_id != observed_job.worker_id
                || current_job.lease_epoch != observed_job.lease_epoch
                || current_job.payload.digest != observed_job.payload.digest
            {
                continue;
            }
            let projection = job_projection(&current_job)?;
            let payload: ModelJobPayload =
                decode_versioned_payload(&current_job.payload, "Model Job")?;
            if payload.binding.quota_ceiling != quota_ceiling
                || payload.binding.model_deployment.deployment_id != model_deployment_id
            {
                return Err(RepositoryError::CorruptRow(
                    "Model Job discovery binding changed".to_owned(),
                ));
            }
            let decision = decide_start_model_dispatch(
                &current_turn,
                &projection,
                &payload,
                command.worker_process_generation_id.clone(),
                slot.lease_token_digest.clone(),
                slot.usage_reservation_id.clone(),
                LeasePolicy {
                    requested_milliseconds: command.lease_milliseconds,
                    hard_maximum_milliseconds: self
                        .model_turn_limits()
                        .maximum_lease_milliseconds(),
                },
                database_now,
                self.model_turn_limits(),
            )?;
            let quota_account_ids = reserve_model_quota(
                &mut transaction,
                &mut quota_accounts,
                &current_job,
                &model_deployment_id,
                quota_ceiling,
                slot,
                &decision.job,
            )
            .await?;
            let job = update_started_model_job(
                &mut transaction,
                &current_job,
                &decision.job,
                &decision.job_payload,
                &slot.usage_reservation_id,
                database_now,
            )
            .await?;
            update_model_turn(
                &mut transaction,
                &current_turn,
                &decision.turn,
                self.model_turn_limits(),
            )
            .await?;
            append_scheduler_event(
                &mut transaction,
                &current_job.tenant_id,
                &slot.event_id,
                &slot.outbox_id,
                "model_turn",
                &model_turn_id.to_string(),
                as_i64(decision.turn.version, "ModelTurn version")?,
                Some(&run_id.to_string()),
                "model.started",
                &TypedPayload::new(
                    1,
                    &serde_json::json!({
                        "attempt_count": decision.job.attempt_count,
                        "job_id": decision.job.job_id,
                        "lease_generation": decision.job.lease_generation,
                        "quota_account_ids": quota_account_ids,
                        "usage_reservation_id": slot.usage_reservation_id,
                        "worker_process_generation_id": command.worker_process_generation_id,
                    }),
                )?,
            )
            .await?;
            let turn = load_model_turn(
                &mut transaction,
                &tenant_id,
                &model_turn_id,
                false,
                self.model_turn_limits(),
            )
            .await?;
            claimed.push(ClaimedModelExecution {
                turn,
                job,
                request_input,
                quota_account_ids,
                fence: JobFence {
                    expected_version: decision.job.version,
                    worker_process_generation_id: command.worker_process_generation_id.clone(),
                    lease_generation: decision.job.lease_generation,
                    token_digest: slot.lease_token_digest.clone(),
                },
                usage_reservation_id: slot.usage_reservation_id.clone(),
                quota_entry_ids: slot.quota_entry_ids.clone(),
            });
        }
        transaction.commit().await?;
        Ok(claimed)
    }
}

async fn load_model_execution_input(
    transaction: &mut Transaction<'_, Postgres>,
    turn: &ModelTurnRecord,
) -> Result<ModelExecutionInput, RepositoryError> {
    let exact = &turn.payload.admission.request;
    exact
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    if exact.value_kind != "model_request"
        || exact.run_id != turn.run_id
        || exact.producing_node_id.as_ref() != Some(&turn.node_execution_id)
        || exact.value_id != turn.request_value_id
    {
        return Err(RepositoryError::CorruptRow(
            "Model request binding differs from its turn".to_owned(),
        ));
    }
    let row = sqlx::query(
        r#"
        SELECT inline_value, artifact_id
        FROM insight_platform.run_values
        WHERE tenant_id = $1 AND value_id = $2 AND run_id = $3
          AND node_id = $4 AND value_kind = 'model_request' AND classification = $5
          AND schema_digest = $6 AND content_digest = $7
        "#,
    )
    .bind(turn.tenant_id.to_string())
    .bind(exact.value_id.to_string())
    .bind(exact.run_id.to_string())
    .bind(turn.node_execution_id.to_string())
    .bind(exact.classification.as_str())
    .bind(exact.schema_digest.to_string())
    .bind(exact.content_digest.to_string())
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::NotFound("Model request RunValue"))?;
    let inline_value: Option<serde_json::Value> = row.try_get("inline_value")?;
    let artifact_id: Option<String> = row.try_get("artifact_id")?;
    let material = match (&exact.storage, inline_value, artifact_id) {
        (InvocationValueStorage::Inline, Some(value), None)
            if turn.payload.admission.request_artifact_link_id.is_none() =>
        {
            ModelExecutionInputMaterial::Inline { value }
        }
        (InvocationValueStorage::Artifact { artifact }, None, Some(stored_artifact_id))
            if stored_artifact_id == artifact.artifact_id().to_string() =>
        {
            let link_id = turn
                .payload
                .admission
                .request_artifact_link_id
                .as_ref()
                .ok_or_else(|| {
                    RepositoryError::CorruptRow(
                        "Artifact-backed Model request has no frozen ArtifactLink".to_owned(),
                    )
                })?;
            lock_ready_model_artifact(transaction, &turn.tenant_id, artifact).await?;
            let linked: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM insight_platform.artifact_links
                    WHERE tenant_id = $1 AND artifact_link_id = $2
                      AND link_kind = 'reference'
                      AND owner_kind = 'model_turn' AND owner_id = $3
                      AND target_artifact_id = $4 AND state = 'active'
                      AND released_at IS NULL
                      AND (expires_at IS NULL OR expires_at > clock_timestamp())
                )
                "#,
            )
            .bind(turn.tenant_id.to_string())
            .bind(link_id.to_string())
            .bind(turn.model_turn_id.to_string())
            .bind(artifact.artifact_id().to_string())
            .fetch_one(&mut **transaction)
            .await?;
            if !linked {
                return Err(RepositoryError::NotFound(
                    "active Model request ArtifactLink",
                ));
            }
            ModelExecutionInputMaterial::LinkedArtifact {
                artifact_link_id: link_id.clone(),
            }
        }
        _ => {
            return Err(RepositoryError::CorruptRow(
                "Model request storage differs from frozen admission".to_owned(),
            ));
        }
    };
    let input = ModelExecutionInput {
        exact: exact.clone(),
        material,
    };
    input
        .validate()
        .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
    Ok(input)
}

fn model_claim_candidate<'a>(
    job: JobRecord,
    slot: &'a insight_platform_models::ModelClaimSlot,
) -> Result<ModelClaimCandidate<'a>, RepositoryError> {
    let payload: ModelJobPayload = decode_versioned_payload(&job.payload, "Model Job")?;
    if job.work_class != WorkClass::Model.as_str()
        || job.owner_kind != "model_turn"
        || job.owner_id != payload.binding.model_turn_id.to_string()
        || job.invocation_id.as_deref() != Some(job.owner_id.as_str())
        || job.request_digest != payload.binding.request_digest.to_string()
    {
        return Err(RepositoryError::CorruptRow(
            "Model Job claim binding is invalid".to_owned(),
        ));
    }
    Ok(ModelClaimCandidate {
        job,
        model_deployment_id: payload.binding.model_deployment.deployment_id,
        quota_ceiling: payload.binding.quota_ceiling,
        slot,
    })
}

async fn lock_model_quota_accounts(
    transaction: &mut Transaction<'_, Postgres>,
    candidates: &[ModelClaimCandidate<'_>],
) -> Result<BTreeMap<String, ModelQuotaAccount>, RepositoryError> {
    let tenants = candidates
        .iter()
        .map(|candidate| candidate.job.tenant_id.clone())
        .collect::<Vec<_>>();
    let deployments = candidates
        .iter()
        .map(|candidate| candidate.model_deployment_id.to_string())
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT tenant_id, quota_account_id, scope_kind, scope_id, work_class, metric,
               limit_value, reserved_value, used_value, version
        FROM insight_platform.quota_accounts
        WHERE work_class = 'model' AND (
            (scope_kind = 'tenant' AND scope_id = ANY($1) AND metric = $3)
            OR
            (scope_kind = 'model_deployment' AND scope_id = ANY($2)
             AND metric = ANY($4))
        )
        ORDER BY tenant_id, quota_account_id
        FOR UPDATE
        "#,
    )
    .bind(tenants)
    .bind(deployments)
    .bind(QuotaDimension::WorkClassConcurrentOperations.as_str())
    .bind(vec![
        QuotaDimension::ModelRequests.as_str(),
        QuotaDimension::ModelTokens.as_str(),
        QuotaDimension::ModelCostMicrounits.as_str(),
    ])
    .fetch_all(&mut **transaction)
    .await?;
    let mut accounts = BTreeMap::new();
    for row in rows {
        let account = model_quota_account_from_row(&row)?;
        if accounts
            .insert(account.quota_account_id.clone(), account)
            .is_some()
        {
            return Err(RepositoryError::CorruptRow(
                "Model quota lock returned a duplicate".to_owned(),
            ));
        }
    }
    Ok(accounts)
}

#[allow(clippy::too_many_arguments)]
async fn reserve_model_quota(
    transaction: &mut Transaction<'_, Postgres>,
    accounts: &mut BTreeMap<String, ModelQuotaAccount>,
    job: &JobRecord,
    deployment_id: &ResourceId,
    ceiling: ModelQuotaCeiling,
    slot: &insight_platform_models::ModelClaimSlot,
    next: &JobProjection,
) -> Result<Vec<String>, RepositoryError> {
    let mut relevant = accounts
        .values()
        .filter(|account| model_quota_account_matches(account, job, deployment_id))
        .map(|account| account.quota_account_id.clone())
        .collect::<Vec<_>>();
    relevant.sort();
    if relevant.len() != MODEL_QUOTA_LINES {
        return Err(RepositoryError::QuotaExceeded);
    }
    let amounts = relevant
        .iter()
        .map(|account_id| {
            let account = accounts.get(account_id).ok_or_else(|| {
                RepositoryError::CorruptRow("Model quota account disappeared".to_owned())
            })?;
            model_reserved_amount(account, ceiling)
        })
        .collect::<Result<Vec<_>, RepositoryError>>()?;
    let request = TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "job_id": job.job_id,
            "lease_generation": next.lease_generation,
            "quota_account_ids": relevant,
            "reserved_amounts": amounts,
            "usage_reservation_id": slot.usage_reservation_id,
        }),
        65_536,
    )?;
    for ((account_id, amount), entry_id) in relevant.iter().zip(&amounts).zip(&slot.quota_entry_ids)
    {
        let account = accounts.get_mut(account_id).ok_or_else(|| {
            RepositoryError::CorruptRow("Model quota account disappeared".to_owned())
        })?;
        if account
            .reserved_value
            .checked_add(account.used_value)
            .and_then(|value| value.checked_add(*amount))
            .is_none_or(|value| value > account.limit_value)
        {
            return Err(RepositoryError::QuotaExceeded);
        }
        let version: i64 = sqlx::query_scalar(
            r#"
            UPDATE insight_platform.quota_accounts
            SET reserved_value = reserved_value + $4, version = version + 1,
                updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND quota_account_id = $2 AND version = $3
              AND reserved_value + used_value + $4 <= limit_value
            RETURNING version
            "#,
        )
        .bind(&account.tenant_id)
        .bind(&account.quota_account_id)
        .bind(account.version)
        .bind(*amount)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::QuotaExceeded)?;
        account.version = version;
        account.reserved_value += *amount;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.quota_ledger (
                tenant_id, quota_entry_id, quota_account_id, correlation_id,
                entry_kind, reserved_amount, used_amount, account_version, request_digest
            ) VALUES ($1, $2, $3, $4, 'reserve', $5, 0, $6, $7)
            "#,
        )
        .bind(&account.tenant_id)
        .bind(entry_id.to_string())
        .bind(&account.quota_account_id)
        .bind(slot.usage_reservation_id.to_string())
        .bind(*amount)
        .bind(version)
        .bind(&request.digest)
        .execute(&mut **transaction)
        .await?;
    }
    Ok(relevant)
}

fn model_quota_account_matches(
    account: &ModelQuotaAccount,
    job: &JobRecord,
    deployment_id: &ResourceId,
) -> bool {
    account.tenant_id == job.tenant_id
        && account.work_class == WorkClass::Model.as_str()
        && ((account.scope_kind == "tenant"
            && account.scope_id == job.tenant_id
            && account.metric == QuotaDimension::WorkClassConcurrentOperations.as_str())
            || (account.scope_kind == "model_deployment"
                && account.scope_id == deployment_id.to_string()
                && matches!(
                    account.metric.as_str(),
                    value if value == QuotaDimension::ModelRequests.as_str()
                        || value == QuotaDimension::ModelTokens.as_str()
                        || value == QuotaDimension::ModelCostMicrounits.as_str()
                )))
}

fn model_reserved_amount(
    account: &ModelQuotaAccount,
    ceiling: ModelQuotaCeiling,
) -> Result<i64, RepositoryError> {
    let amount = if account.metric == QuotaDimension::WorkClassConcurrentOperations.as_str() {
        ceiling.concurrent_units
    } else if account.metric == QuotaDimension::ModelRequests.as_str() {
        ceiling.requests
    } else if account.metric == QuotaDimension::ModelTokens.as_str() {
        ceiling.tokens
    } else if account.metric == QuotaDimension::ModelCostMicrounits.as_str() {
        ceiling.cost_microunits
    } else {
        return Err(RepositoryError::CorruptRow(
            "Model quota metric is not registered".to_owned(),
        ));
    };
    i64::try_from(amount)
        .map_err(|_| RepositoryError::InvalidInput("Model quota exceeds bigint".to_owned()))
}

fn model_quota_account_from_row(row: &PgRow) -> Result<ModelQuotaAccount, RepositoryError> {
    Ok(ModelQuotaAccount {
        tenant_id: row.try_get("tenant_id")?,
        quota_account_id: row.try_get("quota_account_id")?,
        scope_kind: row.try_get("scope_kind")?,
        scope_id: row.try_get("scope_id")?,
        work_class: row.try_get("work_class")?,
        metric: row.try_get("metric")?,
        limit_value: row.try_get("limit_value")?,
        reserved_value: row.try_get("reserved_value")?,
        used_value: row.try_get("used_value")?,
        version: row.try_get("version")?,
    })
}

async fn require_model_claim_parents(
    transaction: &mut Transaction<'_, Postgres>,
    turn: &ModelTurnRecord,
    run: &RunRecord,
    node: &LockedModelNode,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let model_deployment =
        load_deployment(transaction, &turn.tenant_id, &turn.deployment_id).await?;
    let model_resource_id = parse_id(&model_deployment.resource_id, "Model resource")?;
    let model_resource = load_resource(transaction, &turn.tenant_id, &model_resource_id).await?;
    let provider_deployment = load_deployment(
        transaction,
        &turn.tenant_id,
        &turn.payload.admission.provider_deployment.deployment_id,
    )
    .await?;
    let provider_resource_id =
        parse_id(&provider_deployment.resource_id, "Model Provider resource")?;
    let provider_resource =
        load_resource(transaction, &turn.tenant_id, &provider_resource_id).await?;
    if turn.run_id.to_string() != run.run_id
        || node.run_id != run.run_id
        || node.scope_instance_id != turn.scope_instance_id
        || run.state.parse::<RunState>().ok() != Some(RunState::Running)
        || run.current.control.pause_requested
        || run.current.control.cancel_requested_at.is_some()
        || run.current.control.timeout_requested_at.is_some()
        || node.state != NodeExecutionState::Running
        || node.node_kind != PlanNodeKind::ModelLoop
        || database_now >= run.deadline
        || database_now >= node.deadline
        || run.bindings.canonical_digest != turn.payload.admission.run_bindings_digest
        || model_deployment.bindings.digest
            != turn
                .payload
                .admission
                .model_deployment
                .deployment_digest
                .to_string()
        || provider_deployment.bindings.digest
            != turn
                .payload
                .admission
                .provider_deployment
                .deployment_digest
                .to_string()
        || model_resource.lifecycle_state != EntityLifecycle::Active.as_str()
        || model_resource.gate_state != "enabled"
        || provider_resource.lifecycle_state != EntityLifecycle::Active.as_str()
        || provider_resource.gate_state != "enabled"
    {
        return Err(RepositoryError::Conflict("Model dispatch parent"));
    }
    Ok(())
}

async fn update_started_model_job(
    transaction: &mut Transaction<'_, Postgres>,
    current: &JobRecord,
    next: &JobProjection,
    next_payload: &ModelJobPayload,
    usage_reservation_id: &ResourceId,
    database_now: DateTime<Utc>,
) -> Result<JobRecord, RepositoryError> {
    let lease = next
        .lease
        .as_ref()
        .ok_or_else(|| RepositoryError::CorruptRow("started Model Job has no lease".to_owned()))?;
    let payload = TypedPayload::from_versioned(1, next_payload, 1_048_576)?;
    let row = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = 'running', version = $4, attempt_no = $5, lease_epoch = $6,
            worker_id = $7, lease_token_digest = $8, lease_expires_at = $9,
            heartbeat_at = $10, retry_at = NULL, started_at = COALESCE(started_at, $10),
            quota_reservation_id = $11, payload_schema_version = $12, payload = $13,
            payload_digest = $14, updated_at = $10
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
          AND state IN ('ready', 'retry_scheduled') AND worker_id IS NULL
        RETURNING *
        "#,
    )
    .bind(&current.tenant_id)
    .bind(&current.job_id)
    .bind(current.version)
    .bind(as_i64(next.version, "Model Job version")?)
    .bind(i32::try_from(next.attempt_count).map_err(|_| {
        RepositoryError::InvalidInput("Model Job attempt count exceeds integer".to_owned())
    })?)
    .bind(as_i64(next.lease_generation, "Model Job lease generation")?)
    .bind(lease.worker_process_generation_id.to_string())
    .bind(lease.token_digest.to_string())
    .bind(lease.expires_at)
    .bind(database_now)
    .bind(usage_reservation_id.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Model Job claim"))?;
    job_from_row(row)
}

impl PgModelTurnTransaction {
    async fn commit_model_outcome_inner(
        &mut self,
        command: CommitModelOutcome,
    ) -> Result<CommandOutcome<PreparedModelExecution>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now)?;
        let receipt_payload = model_outcome_receipt_payload(&command)?;
        if claim_model_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            "model.outcome.commit",
            &receipt_payload,
        )
        .await?
        {
            let turn = load_model_turn(
                &mut transaction,
                &command.audit.tenant_id,
                &command.model_turn_id,
                false,
                self.limits,
            )
            .await?;
            let job = load_model_job(
                &mut transaction,
                &command.audit.tenant_id,
                &command.job_id,
                false,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(PreparedModelExecution {
                turn,
                job,
            }));
        }
        let observed_job = load_model_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            false,
        )
        .await?;
        require_active_usage_reservation(
            &observed_job,
            &command.usage_reservation_id,
            "Model outcome reservation",
        )?;
        let quota_lines = lock_model_quota_bundle(&mut transaction, &observed_job).await?;
        let current_turn = load_model_turn(
            &mut transaction,
            &command.audit.tenant_id,
            &command.model_turn_id,
            true,
            self.limits,
        )
        .await?;
        if current_turn.version != command.expected_turn_version
            || current_turn.payload.current_job_id.as_ref() != Some(&command.job_id)
        {
            return Err(RepositoryError::Conflict("ModelTurn outcome"));
        }
        let current_job = load_model_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            true,
        )
        .await?;
        require_same_observed_model_job(&observed_job, &current_job)?;
        let projection = job_projection(&current_job)?;
        let payload: ModelJobPayload = decode_versioned_payload(&current_job.payload, "Model Job")?;
        let decision = decide_model_outcome(
            &current_turn,
            &projection,
            &payload,
            &command.fence,
            &command.usage_reservation_id,
            &command.request,
            &command.outcome,
            database_now,
            self.limits,
        )?;
        settle_model_quota(
            &mut transaction,
            &current_job,
            &quota_lines,
            &command.quota_entry_ids,
            decision.settlement,
            &command.audit.request_digest,
        )
        .await?;
        if let Some(output) = decision.output.as_deref() {
            insert_model_output_value_and_references(
                &mut transaction,
                &decision.turn,
                output,
                database_now,
            )
            .await?;
        }
        let job = update_model_job(
            &mut transaction,
            &current_job,
            &decision.job,
            &decision.job_payload,
            database_now,
        )
        .await?;
        update_model_turn(&mut transaction, &current_turn, &decision.turn, self.limits).await?;
        let (event_type, disposition) = match &command.outcome {
            ModelDispatchOutcome::Succeeded(output) if output.response.tool_intents.is_empty() => {
                ("model.completed", "completed")
            }
            ModelDispatchOutcome::Succeeded(_) => ("model.tool_intent", "tool_intent"),
            ModelDispatchOutcome::RetryableFailure { .. } => {
                ("model.retry_scheduled", "retry_scheduled")
            }
            ModelDispatchOutcome::PermanentFailure { .. } => ("model.failed", "failed"),
        };
        append_scheduler_event(
            &mut transaction,
            &current_job.tenant_id,
            &command.audit.event_id,
            &command.audit.outbox_id,
            "model_turn",
            &command.model_turn_id.to_string(),
            as_i64(decision.turn.version, "ModelTurn version")?,
            Some(&decision.turn.run_id.to_string()),
            event_type,
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "attempt_count": decision.job.attempt_count,
                    "finish_reason": decision.turn.payload.result.as_ref().map(|result| result.finish_reason),
                    "job_id": decision.job.job_id,
                    "lease_generation": decision.job.lease_generation,
                    "state": decision.turn.state,
                    "tool_intent_count": decision.turn.payload.result.as_ref().map_or(0, |result| result.tool_intent_count),
                }),
            )?,
        )
        .await?;
        terminalize_model_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            disposition,
            &command.model_turn_id,
        )
        .await?;
        let turn = load_model_turn(
            &mut transaction,
            &command.audit.tenant_id,
            &command.model_turn_id,
            false,
            self.limits,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(PreparedModelExecution {
            turn,
            job,
        }))
    }

    async fn commit_model_cancellation_outcome_inner(
        &mut self,
        command: CommitModelCancellationOutcome,
    ) -> Result<CommandOutcome<PreparedModelExecution>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now)?;
        let receipt_payload = model_cancellation_receipt_payload(&command)?;
        if claim_model_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            "model.cancellation.commit",
            &receipt_payload,
        )
        .await?
        {
            let turn = load_model_turn(
                &mut transaction,
                &command.audit.tenant_id,
                &command.model_turn_id,
                false,
                self.limits,
            )
            .await?;
            let job = load_model_job(
                &mut transaction,
                &command.audit.tenant_id,
                &command.job_id,
                false,
            )
            .await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(PreparedModelExecution {
                turn,
                job,
            }));
        }
        let observed_job = load_model_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            false,
        )
        .await?;
        require_active_usage_reservation(
            &observed_job,
            &command.usage_reservation_id,
            "Model cancellation reservation",
        )?;
        let quota_lines = lock_model_quota_bundle(&mut transaction, &observed_job).await?;
        let current_turn = load_model_turn(
            &mut transaction,
            &command.audit.tenant_id,
            &command.model_turn_id,
            true,
            self.limits,
        )
        .await?;
        if current_turn.version != command.expected_turn_version
            || current_turn.payload.current_job_id.as_ref() != Some(&command.job_id)
        {
            return Err(RepositoryError::Conflict("ModelTurn cancellation outcome"));
        }
        let current_job = load_model_job(
            &mut transaction,
            &command.audit.tenant_id,
            &command.job_id,
            true,
        )
        .await?;
        require_same_observed_model_job(&observed_job, &current_job)?;
        let projection = job_projection(&current_job)?;
        let payload: ModelJobPayload = decode_versioned_payload(&current_job.payload, "Model Job")?;
        let decision = decide_model_cancellation_outcome(
            &current_turn,
            &projection,
            &payload,
            command.fence.as_ref(),
            &command.usage_reservation_id,
            &command.measurement,
            database_now,
            self.limits,
        )?;
        settle_model_quota(
            &mut transaction,
            &current_job,
            &quota_lines,
            &command.quota_entry_ids,
            decision.settlement,
            &command.audit.request_digest,
        )
        .await?;
        let job = update_model_job(
            &mut transaction,
            &current_job,
            &decision.job,
            &decision.job_payload,
            database_now,
        )
        .await?;
        update_model_turn(&mut transaction, &current_turn, &decision.turn, self.limits).await?;
        append_scheduler_event(
            &mut transaction,
            &current_job.tenant_id,
            &command.audit.event_id,
            &command.audit.outbox_id,
            "model_turn",
            &command.model_turn_id.to_string(),
            as_i64(decision.turn.version, "ModelTurn version")?,
            Some(&decision.turn.run_id.to_string()),
            "model.cancelled",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "attempt_count": decision.job.attempt_count,
                    "job_id": decision.job.job_id,
                    "lease_generation": decision.job.lease_generation,
                    "state": decision.turn.state,
                }),
            )?,
        )
        .await?;
        terminalize_model_worker_receipt(
            &mut transaction,
            &command.audit,
            &command.job_id,
            "cancelled",
            &command.model_turn_id,
        )
        .await?;
        let turn = load_model_turn(
            &mut transaction,
            &command.audit.tenant_id,
            &command.model_turn_id,
            false,
            self.limits,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(PreparedModelExecution {
            turn,
            job,
        }))
    }

    async fn control_model_turn_inner(
        &mut self,
        command: ControlModelTurn,
    ) -> Result<CommandOutcome<ControlledModelExecution>, RepositoryError> {
        command.validate_at(Utc::now())?;
        let mut transaction = self.transaction.begin().await?;
        let database_now = database_now(&mut transaction).await?;
        command.validate_at(database_now)?;
        if claim_command_receipt(
            &mut transaction,
            &command.audit,
            "model_turn",
            &command.model_turn_id.to_string(),
            match command.kind {
                ModelControlKind::Cancel => "model.control.cancel",
                ModelControlKind::Timeout => "model.control.timeout",
            },
        )
        .await?
        {
            let turn = load_model_turn(
                &mut transaction,
                &command.audit.tenant_id,
                &command.model_turn_id,
                false,
                self.limits,
            )
            .await?;
            let job = load_optional_current_model_job(&mut transaction, &turn, false).await?;
            transaction.commit().await?;
            return Ok(CommandOutcome::Replayed(ControlledModelExecution {
                turn,
                job,
            }));
        }
        require_tenant_permission(&mut transaction, &command.audit, Permission::RuntimeControl)
            .await?;
        let observed_turn = load_model_turn(
            &mut transaction,
            &command.audit.tenant_id,
            &command.model_turn_id,
            false,
            self.limits,
        )
        .await?;
        let observed_job =
            load_optional_current_model_job(&mut transaction, &observed_turn, false).await?;
        let quota_lines = match observed_job
            .as_ref()
            .and_then(|job| job.quota_reservation_id.as_ref().map(|_| job))
        {
            Some(job)
                if command.kind == ModelControlKind::Timeout
                    || observed_turn.state == ModelTurnState::Cancelling =>
            {
                Some(lock_model_quota_bundle(&mut transaction, job).await?)
            }
            _ => None,
        };
        let current_turn = load_model_turn(
            &mut transaction,
            &command.audit.tenant_id,
            &command.model_turn_id,
            true,
            self.limits,
        )
        .await?;
        if current_turn.version != command.expected_turn_version
            || current_turn.version != observed_turn.version
        {
            return Err(RepositoryError::Conflict("ModelTurn control"));
        }
        let current_job = match current_turn.payload.current_job_id.as_ref() {
            Some(job_id) => Some(
                load_model_job(&mut transaction, &command.audit.tenant_id, job_id, true).await?,
            ),
            None => None,
        };
        match (&observed_job, &current_job) {
            (Some(observed), Some(current)) => require_same_observed_model_job(observed, current)?,
            (None, None) => {}
            _ => return Err(RepositoryError::Conflict("Model control Job")),
        }
        let current_projection = current_job.as_ref().map(job_projection).transpose()?;
        let current_payload = current_job
            .as_ref()
            .map(|job| decode_versioned_payload::<ModelJobPayload>(&job.payload, "Model Job"))
            .transpose()?;
        let decision = decide_model_control(
            &current_turn,
            current_projection.as_ref(),
            current_payload.as_ref(),
            command.kind,
            database_now,
            self.limits,
        )?;
        match (quota_lines.as_deref(), decision.settlement) {
            (Some(lines), Some(settlement)) => {
                let job = current_job.as_ref().ok_or_else(|| {
                    RepositoryError::CorruptRow("Model control settlement lost its Job".to_owned())
                })?;
                settle_model_quota(
                    &mut transaction,
                    job,
                    lines,
                    &command.quota_entry_ids,
                    settlement,
                    &command.audit.request_digest,
                )
                .await?;
            }
            (None, None) if command.quota_entry_ids.is_empty() => {}
            (Some(_), None) if command.quota_entry_ids.is_empty() => {}
            _ => {
                return Err(RepositoryError::InvalidInput(
                    "Model control quota identities do not match its settlement".to_owned(),
                ));
            }
        }
        let job = match (
            current_job.as_ref(),
            decision.job.as_ref(),
            decision.job_payload.as_ref(),
        ) {
            (Some(current), Some(next), Some(payload)) => Some(
                update_model_job(&mut transaction, current, next, payload, database_now).await?,
            ),
            (None, None, None) => None,
            _ => {
                return Err(RepositoryError::CorruptRow(
                    "Model control Job decision is incomplete".to_owned(),
                ));
            }
        };
        update_model_turn(&mut transaction, &current_turn, &decision.turn, self.limits).await?;
        let (event_type, disposition) = match decision.turn.state {
            ModelTurnState::Cancelling => ("model.cancelling", "cancelling"),
            ModelTurnState::Cancelled => ("model.cancelled", "cancelled"),
            ModelTurnState::TimedOut => ("model.timed_out", "timed_out"),
            _ => {
                return Err(RepositoryError::CorruptRow(
                    "Model control produced an invalid state".to_owned(),
                ));
            }
        };
        append_command_event(
            &mut transaction,
            &command.audit,
            "model_turn",
            &command.model_turn_id.to_string(),
            as_i64(decision.turn.version, "ModelTurn version")?,
            event_type,
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "job_id": job.as_ref().map(|job| &job.job_id),
                    "state": decision.turn.state,
                }),
            )?,
        )
        .await?;
        terminalize_command_receipt(
            &mut transaction,
            &command.audit,
            &command.model_turn_id.to_string(),
            disposition,
        )
        .await?;
        let turn = load_model_turn(
            &mut transaction,
            &command.audit.tenant_id,
            &command.model_turn_id,
            false,
            self.limits,
        )
        .await?;
        transaction.commit().await?;
        Ok(CommandOutcome::Applied(ControlledModelExecution {
            turn,
            job,
        }))
    }
}

fn model_outcome_receipt_payload(
    command: &CommitModelOutcome,
) -> Result<TypedPayload, RepositoryError> {
    let outcome = serde_json::to_value(&command.outcome)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let outcome_digest = canonical_digest(&outcome)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "expected_turn_version": command.expected_turn_version,
            "fence": {
                "expected_version": command.fence.expected_version,
                "lease_generation": command.fence.lease_generation,
                "token_digest": command.fence.token_digest,
                "worker_process_generation_id": command.fence.worker_process_generation_id,
            },
            "job_id": command.job_id,
            "model_turn_id": command.model_turn_id,
            "outcome_digest": outcome_digest,
            "quota_entry_ids": command.quota_entry_ids,
            "request_digest": command.request.canonical_digest()?,
            "usage_reservation_id": command.usage_reservation_id,
        }),
        65_536,
    )
}

fn model_cancellation_receipt_payload(
    command: &CommitModelCancellationOutcome,
) -> Result<TypedPayload, RepositoryError> {
    let measurement = serde_json::to_value(&command.measurement)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let measurement_digest = canonical_digest(&measurement)
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
    let fence = command.fence.as_ref().map(|fence| {
        serde_json::json!({
            "expected_version": fence.expected_version,
            "lease_generation": fence.lease_generation,
            "token_digest": fence.token_digest,
            "worker_process_generation_id": fence.worker_process_generation_id,
        })
    });
    TypedPayload::with_limit(
        1,
        &serde_json::json!({
            "expected_turn_version": command.expected_turn_version,
            "fence": fence,
            "job_id": command.job_id,
            "measurement_digest": measurement_digest,
            "model_turn_id": command.model_turn_id,
            "quota_entry_ids": command.quota_entry_ids,
            "usage_reservation_id": command.usage_reservation_id,
        }),
        65_536,
    )
}

async fn load_optional_current_model_job(
    transaction: &mut Transaction<'_, Postgres>,
    turn: &ModelTurnRecord,
    for_update: bool,
) -> Result<Option<JobRecord>, RepositoryError> {
    match turn.payload.current_job_id.as_ref() {
        Some(job_id) => load_model_job(transaction, &turn.tenant_id, job_id, for_update)
            .await
            .map(Some),
        None => Ok(None),
    }
}

fn require_active_usage_reservation(
    job: &JobRecord,
    reservation_id: &ResourceId,
    conflict: &'static str,
) -> Result<(), RepositoryError> {
    if job.quota_reservation_id.as_deref() != Some(reservation_id.to_string().as_str()) {
        return Err(RepositoryError::Conflict(conflict));
    }
    let payload: ModelJobPayload = decode_versioned_payload(&job.payload, "Model Job")?;
    if payload.active_usage_reservation_id.as_ref() != Some(reservation_id) {
        return Err(RepositoryError::Conflict(conflict));
    }
    Ok(())
}

fn require_same_observed_model_job(
    observed: &JobRecord,
    current: &JobRecord,
) -> Result<(), RepositoryError> {
    if current.version != observed.version
        || current.state != observed.state
        || current.lease_epoch != observed.lease_epoch
        || current.worker_id != observed.worker_id
        || current.lease_token_digest != observed.lease_token_digest
        || current.quota_reservation_id != observed.quota_reservation_id
        || current.payload.digest != observed.payload.digest
    {
        return Err(RepositoryError::Conflict("Model Job"));
    }
    Ok(())
}

async fn lock_model_quota_bundle(
    transaction: &mut Transaction<'_, Postgres>,
    job: &JobRecord,
) -> Result<Vec<ModelQuotaReservationLine>, RepositoryError> {
    let reservation_id = job.quota_reservation_id.as_deref().ok_or_else(|| {
        RepositoryError::CorruptRow("active Model Job has no usage reservation".to_owned())
    })?;
    let payload: ModelJobPayload = decode_versioned_payload(&job.payload, "Model Job")?;
    if payload
        .active_usage_reservation_id
        .as_ref()
        .map(ToString::to_string)
        .as_deref()
        != Some(reservation_id)
    {
        return Err(RepositoryError::CorruptRow(
            "Model Job quota column and payload disagree".to_owned(),
        ));
    }
    let rows = sqlx::query(
        r#"
        SELECT account.tenant_id, account.quota_account_id, account.scope_kind,
               account.scope_id, account.work_class, account.metric, account.limit_value,
               account.reserved_value, account.used_value, account.version,
               reserve.reserved_amount, reserve.used_amount AS reservation_used_amount
        FROM insight_platform.quota_ledger AS reserve
        JOIN insight_platform.quota_accounts AS account
          ON account.tenant_id = reserve.tenant_id
         AND account.quota_account_id = reserve.quota_account_id
        WHERE reserve.tenant_id = $1 AND reserve.correlation_id = $2
          AND reserve.entry_kind = 'reserve'
        ORDER BY account.tenant_id, account.quota_account_id
        FOR UPDATE OF account
        "#,
    )
    .bind(&job.tenant_id)
    .bind(reservation_id)
    .fetch_all(&mut **transaction)
    .await?;
    if rows.len() != MODEL_QUOTA_LINES {
        return Err(RepositoryError::CorruptRow(
            "Model quota bundle does not contain exactly four lines".to_owned(),
        ));
    }
    let already_settled: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM insight_platform.quota_ledger
            WHERE tenant_id = $1 AND correlation_id = $2 AND entry_kind = 'settle'
        )
        "#,
    )
    .bind(&job.tenant_id)
    .bind(reservation_id)
    .fetch_one(&mut **transaction)
    .await?;
    if already_settled {
        return Err(RepositoryError::Conflict("Model quota settlement"));
    }
    let deployment_id = payload.binding.model_deployment.deployment_id;
    let ceiling = payload.binding.quota_ceiling;
    let mut metrics = BTreeSet::new();
    let mut lines = Vec::with_capacity(rows.len());
    for row in rows {
        let account = model_quota_account_from_row(&row)?;
        if !model_quota_account_matches(&account, job, &deployment_id)
            || !metrics.insert(account.metric.clone())
        {
            return Err(RepositoryError::CorruptRow(
                "Model quota bundle has an unexpected or duplicate line".to_owned(),
            ));
        }
        let reserved_amount: i64 = row.try_get("reserved_amount")?;
        if reserved_amount != model_reserved_amount(&account, ceiling)?
            || row.try_get::<i64, _>("reservation_used_amount")? != 0
            || account.reserved_value < reserved_amount
        {
            return Err(RepositoryError::CorruptRow(
                "Model quota reservation amount is invalid".to_owned(),
            ));
        }
        lines.push(ModelQuotaReservationLine {
            account,
            reserved_amount,
        });
    }
    let expected = BTreeSet::from([
        QuotaDimension::WorkClassConcurrentOperations
            .as_str()
            .to_owned(),
        QuotaDimension::ModelRequests.as_str().to_owned(),
        QuotaDimension::ModelTokens.as_str().to_owned(),
        QuotaDimension::ModelCostMicrounits.as_str().to_owned(),
    ]);
    if metrics != expected {
        return Err(RepositoryError::CorruptRow(
            "Model quota bundle is incomplete".to_owned(),
        ));
    }
    Ok(lines)
}

async fn settle_model_quota(
    transaction: &mut Transaction<'_, Postgres>,
    job: &JobRecord,
    lines: &[ModelQuotaReservationLine],
    settlement_entry_ids: &[ResourceId],
    settlement: ModelQuotaSettlement,
    request_digest: &Sha256Digest,
) -> Result<(), RepositoryError> {
    if lines.len() != MODEL_QUOTA_LINES || settlement_entry_ids.len() != MODEL_QUOTA_LINES {
        return Err(RepositoryError::InvalidInput(
            "Model quota settlement line count is invalid".to_owned(),
        ));
    }
    let reservation_id = job.quota_reservation_id.as_deref().ok_or_else(|| {
        RepositoryError::CorruptRow("active Model Job has no usage reservation".to_owned())
    })?;
    for (line, entry_id) in lines.iter().zip(settlement_entry_ids) {
        let used_amount = model_settlement_amount(&line.account, settlement)?;
        if used_amount < 0 || used_amount > line.reserved_amount {
            return Err(RepositoryError::QuotaExceeded);
        }
        let version: i64 = sqlx::query_scalar(
            r#"
            UPDATE insight_platform.quota_accounts
            SET reserved_value = reserved_value - $4,
                used_value = used_value + $5,
                version = version + 1, updated_at = clock_timestamp()
            WHERE tenant_id = $1 AND quota_account_id = $2 AND version = $3
              AND reserved_value >= $4
              AND reserved_value + used_value - $4 + $5 <= limit_value
            RETURNING version
            "#,
        )
        .bind(&line.account.tenant_id)
        .bind(&line.account.quota_account_id)
        .bind(line.account.version)
        .bind(line.reserved_amount)
        .bind(used_amount)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(RepositoryError::Conflict("Model quota account"))?;
        sqlx::query(
            r#"
            INSERT INTO insight_platform.quota_ledger (
                tenant_id, quota_entry_id, quota_account_id, correlation_id,
                entry_kind, reserved_amount, used_amount, account_version, request_digest
            ) VALUES ($1, $2, $3, $4, 'settle', $5, $6, $7, $8)
            "#,
        )
        .bind(&line.account.tenant_id)
        .bind(entry_id.to_string())
        .bind(&line.account.quota_account_id)
        .bind(reservation_id)
        .bind(line.reserved_amount)
        .bind(used_amount)
        .bind(version)
        .bind(request_digest.to_string())
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

fn model_settlement_amount(
    account: &ModelQuotaAccount,
    settlement: ModelQuotaSettlement,
) -> Result<i64, RepositoryError> {
    let amount = if account.metric == QuotaDimension::WorkClassConcurrentOperations.as_str() {
        settlement.concurrency_used
    } else if account.metric == QuotaDimension::ModelRequests.as_str() {
        settlement.requests_used
    } else if account.metric == QuotaDimension::ModelTokens.as_str() {
        settlement.tokens_used
    } else if account.metric == QuotaDimension::ModelCostMicrounits.as_str() {
        settlement.cost_microunits_used
    } else {
        return Err(RepositoryError::CorruptRow(
            "Model quota settlement metric is not registered".to_owned(),
        ));
    };
    i64::try_from(amount)
        .map_err(|_| RepositoryError::InvalidInput("Model quota usage exceeds bigint".to_owned()))
}

async fn update_model_job(
    transaction: &mut Transaction<'_, Postgres>,
    current: &JobRecord,
    next: &JobProjection,
    next_payload: &ModelJobPayload,
    database_now: DateTime<Utc>,
) -> Result<JobRecord, RepositoryError> {
    let payload = TypedPayload::from_versioned(1, next_payload, 1_048_576)?;
    let (worker_id, lease_token_digest, lease_expires_at, heartbeat_at) = next
        .lease
        .as_ref()
        .map(|lease| {
            (
                Some(lease.worker_process_generation_id.to_string()),
                Some(lease.token_digest.to_string()),
                Some(lease.expires_at),
                Some(lease.heartbeat_at),
            )
        })
        .unwrap_or((None, None, None, None));
    let terminal = matches!(
        next.state,
        JobState::Succeeded | JobState::Failed | JobState::Cancelled | JobState::TimedOut
    );
    let quota_reservation_id = next_payload
        .active_usage_reservation_id
        .as_ref()
        .map(ToString::to_string);
    if quota_reservation_id.is_some() != next.lease.is_some() {
        return Err(RepositoryError::CorruptRow(
            "Model Job lease and usage reservation shape disagree".to_owned(),
        ));
    }
    let row = sqlx::query(
        r#"
        UPDATE insight_platform.jobs
        SET state = $4, version = $5, attempt_no = $6, lease_epoch = $7,
            worker_id = $8, lease_token_digest = $9, lease_expires_at = $10,
            heartbeat_at = $11, scheduled_at = $12, retry_at = $13,
            wake_kind = NULL, wake_state = NULL, wake_generation = 0,
            result_digest = $14, payload_schema_version = $15, payload = $16,
            payload_digest = $17, terminal_at = $18, updated_at = $19,
            quota_reservation_id = $20
        WHERE tenant_id = $1 AND job_id = $2 AND version = $3
        RETURNING *
        "#,
    )
    .bind(&current.tenant_id)
    .bind(&current.job_id)
    .bind(current.version)
    .bind(next.state.as_str())
    .bind(as_i64(next.version, "Model Job version")?)
    .bind(i32::try_from(next.attempt_count).map_err(|_| {
        RepositoryError::InvalidInput("Model Job attempt count exceeds integer".to_owned())
    })?)
    .bind(as_i64(next.lease_generation, "Model Job lease generation")?)
    .bind(worker_id)
    .bind(lease_token_digest)
    .bind(lease_expires_at)
    .bind(heartbeat_at)
    .bind(next.scheduled_at)
    .bind(next.retry_at)
    .bind(terminal.then_some(payload.digest.clone()))
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(terminal.then_some(database_now))
    .bind(database_now)
    .bind(quota_reservation_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(RepositoryError::Conflict("Model Job"))?;
    job_from_row(row)
}

async fn insert_model_output_value_and_references(
    transaction: &mut Transaction<'_, Postgres>,
    turn: &ModelTurnRecord,
    output: &ModelOutputValue,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    if let ValueRef::Artifact { artifact } = &output.value {
        lock_ready_model_artifact(transaction, &turn.tenant_id, artifact).await?;
    }
    for artifact in &output.artifact_outputs {
        lock_ready_model_artifact(transaction, &turn.tenant_id, &artifact.artifact).await?;
    }
    let (inline_value, artifact_id) = match &output.value {
        ValueRef::Inline { value } => (Some(value), None),
        ValueRef::Artifact { artifact } => (None, Some(artifact.artifact_id().to_string())),
    };
    sqlx::query(
        r#"
        INSERT INTO insight_platform.run_values (
            tenant_id, value_id, run_id, node_id, value_kind, classification,
            schema_digest, content_digest, inline_value, artifact_id, created_at
        ) VALUES ($1, $2, $3, $4, 'model_response', $5, $6, $7, $8, $9, $10)
        "#,
    )
    .bind(turn.tenant_id.to_string())
    .bind(output.value_id.to_string())
    .bind(turn.run_id.to_string())
    .bind(turn.node_execution_id.to_string())
    .bind(output.classification.as_str())
    .bind(output.schema_digest.to_string())
    .bind(output.content_digest.to_string())
    .bind(inline_value)
    .bind(artifact_id)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?;
    if let ValueRef::Artifact { artifact } = &output.value {
        let link_id = output.artifact_link_id.as_ref().ok_or_else(|| {
            RepositoryError::InvalidInput(
                "Artifact-backed Model response has no link ID".to_owned(),
            )
        })?;
        insert_model_artifact_reference(
            transaction,
            turn,
            link_id,
            artifact,
            ArtifactReferenceKind::Output,
            ArtifactPurpose::RunOutput,
            database_now,
        )
        .await?;
    }
    for artifact in &output.artifact_outputs {
        insert_model_artifact_reference(
            transaction,
            turn,
            &artifact.artifact_link_id,
            &artifact.artifact,
            ArtifactReferenceKind::Output,
            ArtifactPurpose::RunOutput,
            database_now,
        )
        .await?;
    }
    Ok(())
}

async fn claim_model_worker_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &ModelWorkerAudit,
    job_id: &ResourceId,
    operation: &str,
    payload: &TypedPayload,
) -> Result<bool, RepositoryError> {
    let inserted = sqlx::query(
        r#"
        INSERT INTO insight_platform.receipts (
            tenant_id, receipt_id, receipt_kind, scope_kind, scope_id,
            dedupe_owner_id, operation, idempotency_key_digest, request_digest, state,
            payload_schema_version, payload, payload_digest, expires_at
        ) VALUES ($1, $2, 'job_commit', 'job', $3, $4, $5, $6, $7,
                  'processing', $8, $9, $10, $11)
        ON CONFLICT (
            tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
            operation, idempotency_key_digest
        ) DO NOTHING
        RETURNING receipt_id
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(audit.receipt_id.to_string())
    .bind(job_id.to_string())
    .bind(audit.worker_process_generation_id.to_string())
    .bind(operation)
    .bind(audit.idempotency_key_digest.to_string())
    .bind(audit.request_digest.to_string())
    .bind(payload.schema_version)
    .bind(&payload.value)
    .bind(&payload.digest)
    .bind(audit.receipt_expires_at)
    .fetch_optional(&mut **transaction)
    .await?;
    if inserted.is_some() {
        return Ok(false);
    }
    let row = sqlx::query(
        r#"
        SELECT request_digest, state, payload_digest
        FROM insight_platform.receipts
        WHERE tenant_id = $1 AND receipt_kind = 'job_commit'
          AND scope_kind = 'job' AND scope_id = $2 AND dedupe_owner_id = $3
          AND operation = $4 AND idempotency_key_digest = $5
        FOR UPDATE
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(job_id.to_string())
    .bind(audit.worker_process_generation_id.to_string())
    .bind(operation)
    .bind(audit.idempotency_key_digest.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if row.try_get::<String, _>("request_digest")? != audit.request_digest.to_string()
        || row.try_get::<String, _>("payload_digest")? != payload.digest
    {
        return Err(RepositoryError::IdempotencyConflict);
    }
    if row.try_get::<String, _>("state")? != "succeeded" {
        return Err(RepositoryError::Conflict("Model worker receipt"));
    }
    Ok(true)
}

async fn terminalize_model_worker_receipt(
    transaction: &mut Transaction<'_, Postgres>,
    audit: &ModelWorkerAudit,
    job_id: &ResourceId,
    disposition: &str,
    response_reference_id: &ResourceId,
) -> Result<(), RepositoryError> {
    let affected = sqlx::query(
        r#"
        UPDATE insight_platform.receipts
        SET state = 'succeeded', disposition = $4, response_reference_id = $5,
            completed_at = clock_timestamp()
        WHERE tenant_id = $1 AND receipt_id = $2 AND request_digest = $3
          AND scope_id = $6 AND state = 'processing'
        "#,
    )
    .bind(audit.tenant_id.to_string())
    .bind(audit.receipt_id.to_string())
    .bind(audit.request_digest.to_string())
    .bind(disposition)
    .bind(response_reference_id.to_string())
    .bind(job_id.to_string())
    .execute(&mut **transaction)
    .await?
    .rows_affected();
    if affected != 1 {
        return Err(RepositoryError::Conflict("Model worker receipt"));
    }
    Ok(())
}

async fn database_now(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<DateTime<Utc>, RepositoryError> {
    Ok(sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&mut **transaction)
        .await?)
}

fn parse_id(value: &str, kind: &str) -> Result<ResourceId, RepositoryError> {
    value
        .parse()
        .map_err(|failure| RepositoryError::CorruptRow(format!("{kind}: {failure}")))
}

fn parse_required_id(
    value: Option<&str>,
    expected: ResourceKind,
    kind: &str,
) -> Result<ResourceId, RepositoryError> {
    let value = value.ok_or_else(|| RepositoryError::CorruptRow(format!("{kind} is missing")))?;
    let parsed = parse_id(value, kind)?;
    if parsed.kind() != expected {
        return Err(RepositoryError::CorruptRow(format!(
            "{kind} has the wrong ID kind"
        )));
    }
    Ok(parsed)
}

fn parse_u64(value: i64, kind: &str) -> Result<u64, RepositoryError> {
    u64::try_from(value).map_err(|_| RepositoryError::CorruptRow(format!("negative {kind}")))
}

fn as_i64(value: u64, kind: &str) -> Result<i64, RepositoryError> {
    i64::try_from(value)
        .map_err(|_| RepositoryError::InvalidInput(format!("{kind} exceeds bigint")))
}
