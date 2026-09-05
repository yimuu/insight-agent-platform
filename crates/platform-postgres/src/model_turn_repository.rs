use crate::{
    invocation_repository::load_enabled_exact_published_version,
    repository::{
        append_command_event, append_scheduler_event, claim_command_receipt,
        decode_deployment_closure, decode_versioned_payload, job_from_row, job_projection,
        load_deployment, load_exact_frozen_selection_policy, load_job_for_update_by_text,
        load_resource, load_run_for_update, require_tenant_permission, safety_scan_cursor_from_row,
        safety_scan_page, terminalize_command_receipt, validate_safety_scan_request, JobRecord,
        PgRepository, RepositoryError, RunRecord, SafetyScanCursor, SafetyScanPage,
        SafetyScanShard, TypedPayload,
    },
};
use chrono::{DateTime, Duration, Utc};
use insight_platform_contracts::{
    canonical_digest, AgentResourceSpec, ArtifactRef, CommandOutcome, DeploymentClosure,
    EntityLifecycle, ExactDeploymentRef, ExactVersionRef, ExternalLeafFailureMutationIds,
    FrozenSlotBinding, FrozenSlotTarget, JobState, ModelBudgetPolicyDocument,
    ModelDeploymentClosure, ModelProfileResourceSpec, ModelProviderDeploymentClosure,
    ModelProviderResourceSpec, ModelPublicProjectionPolicyDocument, ModelSafetyPolicyDocument,
    ModelTurnState, NodeExecutionState, Permission, PlanNodeKind, PolicyKind, PolicyResourceSpec,
    QuotaDimension, RegistryResourceKind, ResourceDocument, ResourceId, ResourceKind,
    RunBindingsSnapshot, RunState, Sha256Digest, SkillResourceSpec, ValueRef, WorkClass,
};
use insight_platform_invocations::InvocationValueStorage;
use insight_platform_jobs::{JobFence, JobProjection, LeasePolicy};
use insight_platform_model_adapters::{ModelAdapterExecutionRequest, ModelExecutionAuthority};
use insight_platform_models::{
    decide_expired_model_lease, decide_model_cancellation_outcome, decide_model_control,
    decide_model_outcome, decide_model_turn_admission, decide_prepare_model_dispatch,
    decide_start_model_dispatch, CanonicalMessagePart, ClaimModelJobs,
    CommitModelCancellationOutcome, CommitModelOutcome, ControlModelTurn, CreateModelTurn,
    ExpiredModelLeaseObservation, ModelAdmissionFacts, ModelControlKind, ModelDispatchOutcome,
    ModelExecutionInput, ModelExecutionInputMaterial, ModelJobPayload, ModelOutputValue,
    ModelQuotaCeiling, ModelQuotaSettlement, ModelRequestValue, ModelToolProjection,
    ModelTurnLimits, ModelTurnPayload, ModelTurnRecord, ModelTurnStore, ModelTurnTransaction,
    ModelWorkerAudit, PrepareModelDispatch, PreparedModelDispatch, MODEL_QUOTA_LINES,
};
use insight_platform_orchestrator::derive_candidate_selection;
use sqlx::{postgres::PgRow, Acquire, Postgres, Row, Transaction};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq)]
pub struct PreparedModelExecution {
    pub turn: ModelTurnRecord,
    pub job: JobRecord,
}

#[derive(Debug, Clone)]
pub struct ExpiredModelRecoverySlot {
    pub quota_entry_ids: Vec<ResourceId>,
    pub event_id: ResourceId,
    pub outbox_id: ResourceId,
    pub failure_mutations: ExternalLeafFailureMutationIds,
}

impl ExpiredModelRecoverySlot {
    fn validate(&self) -> Result<(), RepositoryError> {
        let mut unique = self
            .quota_entry_ids
            .iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>();
        if self.quota_entry_ids.len() != MODEL_QUOTA_LINES
            || unique.len() != MODEL_QUOTA_LINES
            || self
                .quota_entry_ids
                .iter()
                .any(|id| id.kind() != ResourceKind::QuotaLedgerEntry)
            || self.event_id.kind() != ResourceKind::Event
            || self.outbox_id.kind() != ResourceKind::OutboxEvent
            || self.failure_mutations.validate().is_err()
        {
            return Err(RepositoryError::InvalidInput(
                "expired Model recovery identity is invalid".to_owned(),
            ));
        }
        for id in [
            &self.event_id,
            &self.outbox_id,
            &self.failure_mutations.convergence_job_id,
            &self.failure_mutations.run_event_id,
            &self.failure_mutations.run_outbox_id,
            &self.failure_mutations.leaf_node_event_id,
            &self.failure_mutations.leaf_node_outbox_id,
            &self.failure_mutations.convergence_job_event_id,
            &self.failure_mutations.convergence_job_outbox_id,
        ] {
            if !unique.insert(id.to_string()) {
                return Err(RepositoryError::InvalidInput(
                    "expired Model recovery identities must be globally unique".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct DriveExpiredModelJobs {
    pub shard: SafetyScanShard,
    pub after: Option<SafetyScanCursor>,
    pub limit: u16,
    pub retry_backoff_milliseconds: u64,
    pub slots: Vec<ExpiredModelRecoverySlot>,
}

impl DriveExpiredModelJobs {
    fn validate(&self, maximum_batch: u16, maximum_shards: u16) -> Result<(), RepositoryError> {
        validate_safety_scan_request(
            self.shard,
            self.after.as_ref(),
            ResourceKind::Job,
            self.limit,
            self.slots.len(),
            maximum_batch,
            maximum_shards,
        )?;
        if self.retry_backoff_milliseconds == 0 {
            return Err(RepositoryError::InvalidInput(
                "expired Model recovery request is invalid".to_owned(),
            ));
        }
        let mut unique = BTreeSet::new();
        for slot in &self.slots {
            slot.validate()?;
            for id in slot.quota_entry_ids.iter().chain([
                &slot.event_id,
                &slot.outbox_id,
                &slot.failure_mutations.convergence_job_id,
                &slot.failure_mutations.run_event_id,
                &slot.failure_mutations.run_outbox_id,
                &slot.failure_mutations.leaf_node_event_id,
                &slot.failure_mutations.leaf_node_outbox_id,
                &slot.failure_mutations.convergence_job_event_id,
                &slot.failure_mutations.convergence_job_outbox_id,
            ]) {
                if !unique.insert(id.to_string()) {
                    return Err(RepositoryError::InvalidInput(
                        "expired Model recovery identities must be globally unique".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecoveredModelExecution {
    pub turn: ModelTurnRecord,
    pub job: JobRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExactModelPolicyFacts {
    pub deployment: ModelDeploymentClosure,
    pub safety: ModelSafetyPolicyDocument,
    pub budget: ModelBudgetPolicyDocument,
    pub public_projection: ModelPublicProjectionPolicyDocument,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExactControllerSkillFacts {
    pub slot_id: String,
    pub deployment: ExactDeploymentRef,
    pub revision: ExactVersionRef,
    pub skill: SkillResourceSpec,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExactControllerModelAssemblyFacts {
    pub run_bindings: RunBindingsSnapshot,
    pub agent: AgentResourceSpec,
    pub model: ExactModelPolicyFacts,
    pub profile: ModelProfileResourceSpec,
    pub provider_deployment: ModelProviderDeploymentClosure,
    pub provider: ModelProviderResourceSpec,
    pub skills: Vec<ExactControllerSkillFacts>,
    pub tools: Vec<ModelToolProjection>,
}

impl PgRepository {
    pub async fn load_exact_model_policy_facts(
        &self,
        tenant_id: &ResourceId,
        selected_deployment: &insight_platform_contracts::ExactDeploymentRef,
    ) -> Result<ExactModelPolicyFacts, RepositoryError> {
        if tenant_id.kind() != ResourceKind::Tenant
            || selected_deployment.resource_kind != ResourceKind::ModelDeployment
        {
            return Err(RepositoryError::InvalidInput(
                "Model policy lookup identity".to_owned(),
            ));
        }
        let mut transaction = self.pool().begin().await?;
        let record = load_deployment(
            &mut transaction,
            tenant_id,
            &selected_deployment.deployment_id,
        )
        .await?;
        if record.tenant_id != tenant_id.to_string()
            || record.deployment_id != selected_deployment.deployment_id.to_string()
            || record.bindings.digest != selected_deployment.deployment_digest.to_string()
        {
            return Err(RepositoryError::Conflict("exact Model Deployment"));
        }
        let closure = decode_deployment_closure(&record.bindings)?;
        let DeploymentClosure::ModelProfile(deployment) = closure else {
            return Err(RepositoryError::CorruptRow(
                "Model Deployment contains the wrong closure".to_owned(),
            ));
        };
        if record.resource_version_id != deployment.profile_revision.revision_id.to_string() {
            return Err(RepositoryError::Conflict(
                "Model Deployment profile revision",
            ));
        }
        let safety = load_exact_model_policy_document(
            &mut transaction,
            tenant_id,
            &deployment.safety_policy,
            PolicyKind::ModelSafety,
            |policy| policy.model_safety.clone(),
        )
        .await?;
        let budget = load_exact_model_policy_document(
            &mut transaction,
            tenant_id,
            &deployment.budget_policy,
            PolicyKind::Budget,
            |policy| policy.model_budget.clone(),
        )
        .await?;
        let public_projection = load_exact_model_policy_document(
            &mut transaction,
            tenant_id,
            &deployment.public_projection_policy,
            PolicyKind::PublicProjection,
            |policy| policy.model_public_projection.clone(),
        )
        .await?;
        transaction.commit().await?;
        Ok(ExactModelPolicyFacts {
            deployment,
            safety,
            budget,
            public_projection,
        })
    }

    pub async fn load_exact_controller_model_assembly_facts(
        &self,
        tenant_id: &ResourceId,
        run_id: &ResourceId,
        model_slot_id: &str,
        selected_model: &ExactDeploymentRef,
        tool_slots: &[FrozenSlotBinding],
    ) -> Result<ExactControllerModelAssemblyFacts, RepositoryError> {
        let model = self
            .load_exact_model_policy_facts(tenant_id, selected_model)
            .await?;
        let mut transaction = self.pool().begin().await?;
        let run = load_run_for_update(&mut transaction, tenant_id, run_id).await?;
        let model_record =
            load_deployment(&mut transaction, tenant_id, &selected_model.deployment_id).await?;
        if model_record.bindings.digest != selected_model.deployment_digest.to_string()
            || model_record.resource_version_id
                != model.deployment.profile_revision.revision_id.to_string()
        {
            return Err(RepositoryError::Conflict("exact Model assembly Deployment"));
        }
        let model_resource_id = parse_id(&model_record.resource_id, "Model assembly Profile")?;
        let model_resource = load_resource(&mut transaction, tenant_id, &model_resource_id).await?;
        require_enabled_resource(
            &model_resource,
            RegistryResourceKind::ModelProfile,
            "Model assembly Deployment gate",
        )?;
        let model_slot = run
            .bindings
            .slots
            .iter()
            .find(|slot| slot.slot_id == model_slot_id)
            .ok_or(RepositoryError::NotFound("frozen Model slot"))?;
        let FrozenSlotTarget::Model { candidates, .. } = &model_slot.target else {
            return Err(RepositoryError::Conflict("frozen Model slot kind"));
        };
        if !candidates.contains(selected_model)
            || tool_slots.iter().any(|slot| {
                run.bindings
                    .slots
                    .iter()
                    .find(|frozen| frozen.slot_id == slot.slot_id)
                    != Some(slot)
            })
        {
            return Err(RepositoryError::Conflict("exact Model assembly slots"));
        }

        let agent_payload = load_enabled_exact_published_version(
            &mut transaction,
            tenant_id,
            &run.bindings.agent_interface,
            RegistryResourceKind::Agent,
        )
        .await?;
        let ResourceDocument::Agent(agent) = agent_payload.document else {
            return Err(RepositoryError::CorruptRow(
                "Agent interface revision contains the wrong document".to_owned(),
            ));
        };
        let profile_payload = load_enabled_exact_published_version(
            &mut transaction,
            tenant_id,
            &model.deployment.profile_revision,
            RegistryResourceKind::ModelProfile,
        )
        .await?;
        let ResourceDocument::ModelProfile(profile) = profile_payload.document else {
            return Err(RepositoryError::CorruptRow(
                "Model Profile revision contains the wrong document".to_owned(),
            ));
        };
        let provider_record = load_deployment(
            &mut transaction,
            tenant_id,
            &model.deployment.provider_deployment.deployment_id,
        )
        .await?;
        if provider_record.bindings.digest
            != model
                .deployment
                .provider_deployment
                .deployment_digest
                .to_string()
        {
            return Err(RepositoryError::Conflict(
                "exact Model Provider Deployment binding",
            ));
        }
        let DeploymentClosure::ModelProvider(provider_deployment) =
            decode_deployment_closure(&provider_record.bindings)?
        else {
            return Err(RepositoryError::CorruptRow(
                "Model Provider Deployment has the wrong closure".to_owned(),
            ));
        };
        let provider_resource_id =
            parse_id(&provider_record.resource_id, "Model assembly Provider")?;
        let provider_resource =
            load_resource(&mut transaction, tenant_id, &provider_resource_id).await?;
        require_enabled_resource(
            &provider_resource,
            RegistryResourceKind::ModelProvider,
            "Model assembly Provider gate",
        )?;
        if provider_record.resource_version_id
            != provider_deployment
                .provider_revision
                .revision_id
                .to_string()
            || profile.provider_revision != provider_deployment.provider_revision
        {
            return Err(RepositoryError::Conflict(
                "Model assembly Provider revision",
            ));
        }
        let provider_payload = load_enabled_exact_published_version(
            &mut transaction,
            tenant_id,
            &provider_deployment.provider_revision,
            RegistryResourceKind::ModelProvider,
        )
        .await?;
        let ResourceDocument::ModelProvider(provider) = provider_payload.document else {
            return Err(RepositoryError::CorruptRow(
                "Model Provider revision contains the wrong document".to_owned(),
            ));
        };

        let mut skills = Vec::new();
        let mut tools = Vec::new();
        for slot in tool_slots {
            let (candidates, selection_policy) = match &slot.target {
                FrozenSlotTarget::Skill {
                    candidates,
                    selection_policy,
                }
                | FrozenSlotTarget::Capability {
                    candidates,
                    selection_policy,
                    ..
                } => (candidates, selection_policy),
                _ => return Err(RepositoryError::Conflict("Model assembly slot kind")),
            };
            let selection_document =
                load_exact_frozen_selection_policy(&mut transaction, &run, selection_policy, false)
                    .await?;
            let selected = derive_candidate_selection(
                &slot.slot_id,
                selection_policy,
                &selection_document,
                candidates,
                None,
            )
            .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
            .selected_deployment;
            let deployment =
                load_deployment(&mut transaction, tenant_id, &selected.deployment_id).await?;
            if deployment.bindings.digest != selected.deployment_digest.to_string() {
                return Err(RepositoryError::Conflict("exact Model assembly Deployment"));
            }
            let resource_id = parse_id(&deployment.resource_id, "Model assembly resource")?;
            let resource = load_resource(&mut transaction, tenant_id, &resource_id).await?;
            match &slot.target {
                FrozenSlotTarget::Skill { .. } => {
                    require_enabled_resource(
                        &resource,
                        RegistryResourceKind::Skill,
                        "Model Skill Deployment gate",
                    )?;
                    let DeploymentClosure::Skill(closure) =
                        decode_deployment_closure(&deployment.bindings)?
                    else {
                        return Err(RepositoryError::Conflict("Model Skill Deployment closure"));
                    };
                    let published = load_enabled_exact_published_version(
                        &mut transaction,
                        tenant_id,
                        &closure.skill_revision,
                        RegistryResourceKind::Skill,
                    )
                    .await?;
                    let ResourceDocument::Skill(skill) = published.document else {
                        return Err(RepositoryError::Conflict("Model Skill revision document"));
                    };
                    skills.push(ExactControllerSkillFacts {
                        slot_id: slot.slot_id.clone(),
                        deployment: selected,
                        revision: closure.skill_revision,
                        skill,
                    });
                }
                FrozenSlotTarget::Capability { tool_alias, .. } => {
                    require_enabled_resource(
                        &resource,
                        RegistryResourceKind::CapabilityInterface,
                        "Model tool Capability gate",
                    )?;
                    let DeploymentClosure::CapabilityInterface(closure) =
                        decode_deployment_closure(&deployment.bindings)?
                    else {
                        return Err(RepositoryError::Conflict(
                            "Model tool Capability Deployment closure",
                        ));
                    };
                    let published = load_enabled_exact_published_version(
                        &mut transaction,
                        tenant_id,
                        &closure.interface,
                        RegistryResourceKind::CapabilityInterface,
                    )
                    .await?;
                    let ResourceDocument::CapabilityInterface(interface) = published.document
                    else {
                        return Err(RepositoryError::Conflict(
                            "Model tool Capability Interface document",
                        ));
                    };
                    tools.push(ModelToolProjection {
                        projected_name: tool_alias.clone().unwrap_or_else(|| slot.slot_id.clone()),
                        capability_deployment: selected,
                        interface_revision: closure.interface,
                        input_schema: interface.input_schema,
                        output_schema_digest: interface.output_schema.canonical_digest,
                        effect: interface.effect,
                    });
                }
                _ => unreachable!(),
            }
        }
        skills.sort_by(|left, right| left.slot_id.cmp(&right.slot_id));
        tools.sort_by(|left, right| left.projected_name.cmp(&right.projected_name));
        transaction.commit().await?;
        Ok(ExactControllerModelAssemblyFacts {
            run_bindings: run.bindings,
            agent,
            model,
            profile: *profile,
            provider_deployment,
            provider,
            skills,
            tools,
        })
    }
}

async fn load_exact_model_policy_document<T>(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    exact: &insight_platform_contracts::ExactVersionRef,
    expected_kind: PolicyKind,
    select: impl FnOnce(&PolicyResourceSpec) -> Option<T>,
) -> Result<T, RepositoryError> {
    let published = load_enabled_exact_published_version(
        transaction,
        tenant_id,
        exact,
        RegistryResourceKind::Policy,
    )
    .await?;
    let ResourceDocument::Policy(policy) = published.document else {
        return Err(RepositoryError::CorruptRow(
            "Model Policy revision contains the wrong document".to_owned(),
        ));
    };
    if policy.policy_kind != expected_kind {
        return Err(RepositoryError::Conflict("exact Model Policy kind"));
    }
    select(&policy).ok_or(RepositoryError::Conflict("exact Model Policy document"))
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
    pub resume_mutations: Option<insight_platform_contracts::ExternalLeafResumeMutationIds>,
    pub failure_mutations: Option<insight_platform_contracts::ExternalLeafFailureMutationIds>,
    pub tool_continuation_mutations:
        Option<insight_platform_models::ModelToolContinuationMutationIds>,
}

impl ClaimedModelExecution {
    pub fn job_projection(&self) -> Result<JobProjection, RepositoryError> {
        job_projection(&self.job)
    }

    /// Builds the credential-free Provider request after the frozen Inline value is digest-checked.
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
            run_id: self.turn.run_id.clone(),
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
            resume_mutations: self.resume_mutations.clone(),
            failure_mutations: self.failure_mutations.clone(),
            tool_continuation_mutations: self.tool_continuation_mutations.clone(),
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

impl ControlledModelExecution {
    pub fn job_projection(&self) -> Result<Option<JobProjection>, RepositoryError> {
        self.job.as_ref().map(job_projection).transpose()
    }
}

pub struct PgModelTurnTransaction<'a> {
    transaction: Transaction<'a, Postgres>,
    limits: ModelTurnLimits,
    scope_environment_limits: insight_platform_orchestrator::ScopeEnvironmentLimits,
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
    ) -> Result<PgModelTurnTransaction<'_>, RepositoryError> {
        Ok(PgModelTurnTransaction {
            transaction: self.pool().begin().await?,
            limits: self.model_turn_limits(),
            scope_environment_limits: self.scope_environment_limits(),
        })
    }
}

pub(crate) async fn load_model_turn_for_replay(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: &ResourceId,
    model_turn_id: &ResourceId,
    limits: ModelTurnLimits,
) -> Result<ModelTurnRecord, RepositoryError> {
    load_model_turn(transaction, tenant_id, model_turn_id, false, limits).await
}

impl<'a> PgModelTurnTransaction<'a> {
    pub(crate) fn from_transaction(
        transaction: Transaction<'a, Postgres>,
        limits: ModelTurnLimits,
        scope_environment_limits: insight_platform_orchestrator::ScopeEnvironmentLimits,
    ) -> Self {
        Self {
            transaction,
            limits,
            scope_environment_limits,
        }
    }
}

#[async_trait::async_trait]
impl ModelTurnStore for PgRepository {
    type Error = RepositoryError;
    type Transaction<'a>
        = PgModelTurnTransaction<'a>
    where
        Self: 'a;

    async fn begin_model_turn(&self) -> Result<Self::Transaction<'_>, Self::Error> {
        self.begin_model_turn_transaction().await
    }
}

#[async_trait::async_trait]
impl<'a> ModelTurnTransaction for PgModelTurnTransaction<'a> {
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
            &command.tool_slots,
        )
        .await?;
        require_committed_tool_results(
            &mut transaction,
            &command.audit.tenant_id,
            &command.run_id,
            &command.request,
        )
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
    tool_slots: &[insight_platform_contracts::FrozenSlotBinding],
) -> Result<(), RepositoryError> {
    for slot in tool_slots {
        let FrozenSlotTarget::Capability {
            candidates,
            selection_policy,
            tool_alias,
        } = &slot.target
        else {
            continue;
        };
        let policy =
            load_exact_frozen_selection_policy(transaction, run, selection_policy, false).await?;
        let selected =
            derive_candidate_selection(&slot.slot_id, selection_policy, &policy, candidates, None)
                .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
        let projected_name = tool_alias.as_deref().unwrap_or(slot.slot_id.as_str());
        let tool = request
            .request
            .tools
            .iter()
            .find(|tool| tool.projected_name == projected_name)
            .ok_or_else(|| {
                RepositoryError::InvalidInput(
                    "Model tool projection is missing its frozen Capability slot".to_owned(),
                )
            })?;
        if tool.capability_deployment != selected.selected_deployment {
            return Err(RepositoryError::Conflict(
                "exact Model tool candidate selection",
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
    let results = request
        .request
        .messages
        .iter()
        .flat_map(|message| &message.parts)
        .filter_map(|part| match part {
            CanonicalMessagePart::ToolResult(result) => Some(result.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    for result in results {
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
    let ValueRef::Inline { value } = &request.value else {
        return Err(RepositoryError::InvalidInput(
            "Model request must use Inline storage".to_owned(),
        ));
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
    .bind(value)
    .bind(Option::<String>::None)
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
            retry_at, started_at, terminal_at, created_at, updated_at, trace_id
        ) VALUES (
            $1, $2, 'model', 'node_execution', $3,
            $4, $5, $3, $6, $7, $8,
            $9, NULL, NULL, $10, $11, $12, $13,
            NULL, NULL, NULL, $14, $14,
            (SELECT trace_id FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $5)
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
        || record.payload.admission.tool_slots != command.tool_slots
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
            tenant_id, job_id, job_kind, work_class, owner_kind, owner_id, invocation_id,
            run_id, node_id, state, version, attempt_no, attempt_limit, lease_epoch,
            scheduled_at, deadline, priority, request_digest, effect_key_digest,
            payload_schema_version, payload, payload_digest, created_at, updated_at,
            trace_id
        ) VALUES (
            $1, $2, 'model_turn', 'model', 'model_turn', $3, $3,
            $4, $5, 'ready', 1, 0, $6, 0,
            $7, $8, 0, $9, NULL, $10, $11, $12, $13, $13,
            (SELECT trace_id FROM insight_platform.runs WHERE tenant_id = $1 AND run_id = $4)
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
    /// Returns the current cancellation winners leased by one exact Model Worker generation.
    ///
    /// Discovery is bounded and read-only. Egress cancellation and the fenced terminal commit
    /// remain separate operations, so this scan cannot become a second ModelTurn authority.
    pub async fn scan_cancelling_model_executions(
        &self,
        worker_process_generation_id: &ResourceId,
        limit: u16,
    ) -> Result<Vec<ControlledModelExecution>, RepositoryError> {
        if worker_process_generation_id.kind() != ResourceKind::WorkerProcessGeneration
            || limit == 0
            || limit > self.recovery_batch_limit()
        {
            return Err(RepositoryError::InvalidInput(
                "Model cancellation scan is outside the platform bound".to_owned(),
            ));
        }
        let mut transaction = self.pool().begin().await?;
        let rows = sqlx::query(
            r#"
            SELECT *
            FROM insight_platform.jobs
            WHERE work_class = 'model' AND owner_kind = 'model_turn'
              AND state = 'cancelling' AND terminal_at IS NULL
              AND worker_id = $1 AND lease_token_digest IS NOT NULL
              AND lease_expires_at > clock_timestamp()
              AND deadline > clock_timestamp()
            ORDER BY updated_at, tenant_id, job_id
            LIMIT $2
            "#,
        )
        .bind(worker_process_generation_id.to_string())
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await?;
        let mut executions = Vec::with_capacity(rows.len());
        for row in rows {
            let job = job_from_row(row)?;
            let tenant_id = parse_required_id(
                Some(&job.tenant_id),
                ResourceKind::Tenant,
                "Model cancellation tenant",
            )?;
            let model_turn_id = parse_required_id(
                Some(&job.owner_id),
                ResourceKind::ModelTurn,
                "Model cancellation owner",
            )?;
            let invocation_id = parse_required_id(
                job.invocation_id.as_deref(),
                ResourceKind::ModelTurn,
                "Model cancellation Invocation",
            )?;
            if invocation_id != model_turn_id {
                return Err(RepositoryError::CorruptRow(
                    "Model cancellation Job owner and Invocation differ".to_owned(),
                ));
            }
            let turn = load_model_turn(
                &mut transaction,
                &tenant_id,
                &model_turn_id,
                false,
                self.model_turn_limits(),
            )
            .await?;
            let projection = job_projection(&job)?;
            let payload: ModelJobPayload = decode_versioned_payload(&job.payload, "Model Job")?;
            payload
                .validate_for(&turn, &projection, self.model_turn_limits())
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
            if turn.state != ModelTurnState::Cancelling
                || turn.payload.current_job_id.as_ref() != Some(&projection.job_id)
                || projection.state != JobState::Cancelling
                || projection.lease.as_ref().is_none_or(|lease| {
                    lease.worker_process_generation_id != *worker_process_generation_id
                })
                || payload
                    .active_usage_reservation_id
                    .as_ref()
                    .map(ToString::to_string)
                    != job.quota_reservation_id
            {
                return Err(RepositoryError::CorruptRow(
                    "Model cancellation projection is inconsistent".to_owned(),
                ));
            }
            executions.push(ControlledModelExecution {
                turn,
                job: Some(job),
            });
        }
        transaction.commit().await?;
        Ok(executions)
    }

    pub async fn drive_expired_model_jobs(
        &self,
        command: DriveExpiredModelJobs,
    ) -> Result<SafetyScanPage<RecoveredModelExecution>, RepositoryError> {
        command.validate(self.recovery_batch_limit(), self.recovery_shard_limit())?;
        let scan_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(self.pool())
            .await?;
        let rows = sqlx::query(
            r#"
            SELECT job.*, job.lease_expires_at AS scan_sort_at
            FROM insight_platform.jobs AS job
            JOIN insight_platform.invocations AS turn
              ON turn.tenant_id = job.tenant_id AND turn.invocation_id = job.owner_id
            WHERE job.work_class = 'model' AND job.owner_kind = 'model_turn'
              AND job.state = 'running' AND job.terminal_at IS NULL
              AND job.lease_expires_at <= $1
              AND job.deadline > $1 AND turn.state = 'in_flight' AND turn.terminal_at IS NULL
              AND mod(('x' || right(job.job_id, 8))::bit(32)::bigint, $4) = $3
              AND ($5::timestamptz IS NULL OR
                   (job.lease_expires_at, job.tenant_id, job.job_id) >
                   ($5::timestamptz, $6::text, $7::text))
            ORDER BY job.lease_expires_at, job.tenant_id, job.job_id
            LIMIT $2
            "#,
        )
        .bind(scan_now)
        .bind(i64::from(command.limit))
        .bind(i64::from(command.shard.index))
        .bind(i64::from(command.shard.count))
        .bind(command.after.as_ref().map(|cursor| cursor.sort_at))
        .bind(
            command
                .after
                .as_ref()
                .map(|cursor| cursor.tenant_id.to_string()),
        )
        .bind(
            command
                .after
                .as_ref()
                .map(|cursor| cursor.item_id.to_string()),
        )
        .fetch_all(self.pool())
        .await?;
        let scanned_count = rows.len();
        let last_cursor = rows
            .last()
            .map(|row| safety_scan_cursor_from_row(row, "job_id", ResourceKind::Job))
            .transpose()?;
        let candidates = rows
            .into_iter()
            .map(job_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        let mut recovered = Vec::with_capacity(candidates.len());
        for (observed, slot) in candidates.into_iter().zip(&command.slots) {
            match self
                .recover_expired_model_job(
                    &observed,
                    slot,
                    scan_now,
                    command.retry_backoff_milliseconds,
                )
                .await
            {
                Ok(Some(record)) => recovered.push(record),
                Ok(None)
                | Err(RepositoryError::Conflict(_))
                | Err(RepositoryError::StaleFence)
                | Err(RepositoryError::LeaseExpired) => {}
                Err(failure) => return Err(failure),
            }
        }
        Ok(safety_scan_page(
            recovered,
            scanned_count,
            command.limit,
            last_cursor,
        ))
    }

    async fn recover_expired_model_job(
        &self,
        observed: &JobRecord,
        slot: &ExpiredModelRecoverySlot,
        scan_now: DateTime<Utc>,
        retry_backoff_milliseconds: u64,
    ) -> Result<Option<RecoveredModelExecution>, RepositoryError> {
        let mut transaction = self.pool().begin().await?;
        let quota_lines = lock_model_quota_bundle(&mut transaction, observed).await?;
        let tenant_id = parse_required_id(
            Some(&observed.tenant_id),
            ResourceKind::Tenant,
            "expired Model tenant",
        )?;
        let run_id = parse_required_id(
            observed.run_id.as_deref(),
            ResourceKind::Run,
            "expired Model Run",
        )?;
        let node_id = parse_required_id(
            observed.node_id.as_deref(),
            ResourceKind::NodeExecution,
            "expired Model NodeExecution",
        )?;
        let model_turn_id = parse_required_id(
            Some(&observed.owner_id),
            ResourceKind::ModelTurn,
            "expired ModelTurn",
        )?;
        let job_id = parse_required_id(
            Some(&observed.job_id),
            ResourceKind::Job,
            "expired Model Job",
        )?;
        // Preserve the required quota -> Run -> Node -> owner -> Job lock order. Exhausted
        // recovery may hand the Plan leaf back to orchestration in this transaction.
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
        let current_job = load_model_job(&mut transaction, &tenant_id, &job_id, true).await?;
        let database_now = database_now(&mut transaction).await?;
        let node_id_text = node_id.to_string();
        if database_now < scan_now
            || run.terminal_at.is_some()
            || node.run_id != run.run_id
            || node.node_kind != PlanNodeKind::ModelLoop
            || !matches!(
                node.state,
                NodeExecutionState::Running | NodeExecutionState::Waiting
            )
            || current_turn.run_id != run_id
            || current_turn.node_execution_id != node_id
            || current_job.run_id.as_deref() != Some(run.run_id.as_str())
            || current_job.node_id.as_deref() != Some(node_id_text.as_str())
            || current_job.version != observed.version
            || current_job.lease_epoch != observed.lease_epoch
            || current_job.lease_token_digest != observed.lease_token_digest
            || current_job.lease_expires_at != observed.lease_expires_at
            || current_job.payload.digest != observed.payload.digest
            || current_job.quota_reservation_id != observed.quota_reservation_id
            || current_job
                .lease_expires_at
                .is_none_or(|expires_at| expires_at > database_now)
        {
            transaction.rollback().await?;
            return Ok(None);
        }
        let projection = job_projection(&current_job)?;
        let payload: ModelJobPayload = decode_versioned_payload(&current_job.payload, "Model Job")?;
        let retry_at = if projection.attempt_count < projection.attempt_limit {
            Some(
                database_now
                    .checked_add_signed(Duration::milliseconds(
                        i64::try_from(retry_backoff_milliseconds).map_err(|_| {
                            RepositoryError::InvalidInput(
                                "Model retry backoff exceeds bigint".to_owned(),
                            )
                        })?,
                    ))
                    .filter(|retry_at| *retry_at < current_turn.deadline)
                    .ok_or_else(|| RepositoryError::Conflict("expired Model retry deadline"))?,
            )
        } else {
            None
        };
        let observation_digest: Sha256Digest = canonical_digest(&serde_json::json!({
            "job_id": job_id,
            "lease_generation": projection.lease_generation,
            "model_turn_id": model_turn_id,
            "observed_job_version": projection.version,
            "reason": "worker_lease_expired",
            "schema_version": 1,
        }))
        .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?
        .parse()
        .map_err(|failure: insight_platform_contracts::NominalTypeError| {
            RepositoryError::InvalidInput(failure.to_string())
        })?;
        let decision = decide_expired_model_lease(
            &current_turn,
            &projection,
            &payload,
            &ExpiredModelLeaseObservation {
                observed_job_version: projection.version,
                observed_lease_generation: projection.lease_generation,
                retry_at,
                observation_digest: observation_digest.clone(),
            },
            database_now,
            self.model_turn_limits(),
        )?;
        settle_model_quota(
            &mut transaction,
            &current_job,
            &quota_lines,
            &slot.quota_entry_ids,
            decision.settlement,
            &current_turn.payload.admission.request_digest,
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
        update_model_turn(
            &mut transaction,
            &current_turn,
            &decision.turn,
            self.model_turn_limits(),
        )
        .await?;
        let plan_leaf_waiting = node.state == NodeExecutionState::Waiting;
        if decision.turn.state == ModelTurnState::Failed && plan_leaf_waiting {
            let failure = decision.turn.payload.failure.as_ref().ok_or_else(|| {
                RepositoryError::CorruptRow("failed ModelTurn has no failure".to_owned())
            })?;
            crate::repository::settle_model_leaf_failure_in_transaction(
                &mut transaction,
                &decision.turn,
                &job_id,
                failure,
                &slot.failure_mutations,
                database_now,
            )
            .await?;
        } else {
            release_model_plan_leaf_permit(
                &mut transaction,
                &current_job,
                &current_turn,
                database_now,
            )
            .await?;
        }
        append_scheduler_event(
            &mut transaction,
            &current_job.tenant_id,
            &slot.event_id,
            &slot.outbox_id,
            "model_turn",
            &model_turn_id.to_string(),
            as_i64(decision.turn.version, "ModelTurn version")?,
            Some(&decision.turn.run_id.to_string()),
            "model.lease_recovered",
            &TypedPayload::new(
                1,
                &serde_json::json!({
                    "attempt_count": decision.job.attempt_count,
                    "job_id": job_id,
                    "lease_generation": decision.job.lease_generation,
                    "observation_digest": observation_digest,
                    "state": decision.turn.state,
                }),
            )?,
        )
        .await?;
        transaction.commit().await?;
        Ok(Some(RecoveredModelExecution {
            turn: decision.turn,
            job,
        }))
    }

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
            let plan_leaf = node.state == NodeExecutionState::Waiting;
            let current_turn = load_model_turn(
                &mut transaction,
                &tenant_id,
                &model_turn_id,
                true,
                self.model_turn_limits(),
            )
            .await?;
            if current_turn
                .payload
                .admission
                .provider
                .installed_adapter
                .worker_manifest_digest
                != command.worker_manifest_digest
            {
                continue;
            }
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
                resume_mutations: plan_leaf.then(|| slot.resume_mutations.clone()).flatten(),
                failure_mutations: plan_leaf.then(|| slot.failure_mutations.clone()).flatten(),
                tool_continuation_mutations: plan_leaf
                    .then(|| slot.tool_continuation_mutations.clone())
                    .flatten(),
            });
        }
        transaction.commit().await?;
        Ok(claimed)
    }
}

async fn release_model_plan_leaf_permit(
    transaction: &mut Transaction<'_, Postgres>,
    job: &JobRecord,
    turn: &ModelTurnRecord,
    database_now: DateTime<Utc>,
) -> Result<(), RepositoryError> {
    let plan_leaf_waiting: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM insight_platform.run_nodes
            WHERE tenant_id = $1 AND node_id = $2 AND run_id = $3
              AND record_kind = 'node_execution' AND node_kind = 'model_loop'
              AND state = 'waiting' AND terminal_at IS NULL
        )
        "#,
    )
    .bind(&job.tenant_id)
    .bind(turn.node_execution_id.to_string())
    .bind(turn.run_id.to_string())
    .fetch_one(&mut **transaction)
    .await?;
    if !plan_leaf_waiting {
        return Ok(());
    }
    let updated = sqlx::query(
        r#"
        UPDATE insight_platform.runs
        SET version = version + 1, active_work_count = active_work_count - 1,
            updated_at = $3
        WHERE tenant_id = $1 AND run_id = $2 AND terminal_at IS NULL
          AND state = 'running' AND active_work_count > 0
        "#,
    )
    .bind(&job.tenant_id)
    .bind(turn.run_id.to_string())
    .bind(database_now)
    .execute(&mut **transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(RepositoryError::Conflict("expired Model Plan leaf permit"));
    }
    Ok(())
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
        (InvocationValueStorage::Inline, Some(value), None) => {
            ModelExecutionInputMaterial::Inline { value }
        }
        _ => {
            return Err(RepositoryError::CorruptRow(
                "Model request is not the frozen Inline value".to_owned(),
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
    let plan_leaf = node.state == NodeExecutionState::Waiting;
    let run_state = run.state.parse::<RunState>().ok();
    if turn.run_id.to_string() != run.run_id
        || node.run_id != run.run_id
        || node.scope_instance_id != turn.scope_instance_id
        || if plan_leaf {
            !matches!(run_state, Some(RunState::Running | RunState::Waiting))
        } else {
            run_state != Some(RunState::Running)
        }
        || run.current.control.pause_requested
        || run.current.control.cancel_requested_at.is_some()
        || run.current.control.timeout_requested_at.is_some()
        || if plan_leaf {
            node.state != NodeExecutionState::Waiting
        } else {
            node.state != NodeExecutionState::Running
        }
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
    if plan_leaf {
        let mut current = run.current.clone();
        if run_state == Some(RunState::Waiting) {
            current.waiting_reason = None;
        }
        current
            .validate(&turn.run_id)
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        let payload = TypedPayload::from_versioned(1, &current, 1_048_576)?;
        let updated = sqlx::query(
            r#"
            UPDATE insight_platform.runs
            SET state = 'running', version = version + 1,
                active_work_count = active_work_count + 1,
                current_schema_version = $4, current_payload = $5,
                current_payload_digest = $6, updated_at = $7
            WHERE tenant_id = $1 AND run_id = $2 AND version = $3
              AND state IN ('running', 'waiting') AND terminal_at IS NULL
            "#,
        )
        .bind(&run.tenant_id)
        .bind(&run.run_id)
        .bind(run.version)
        .bind(payload.schema_version)
        .bind(&payload.value)
        .bind(&payload.digest)
        .bind(database_now)
        .execute(&mut **transaction)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(RepositoryError::Conflict("Model Plan leaf claim permit"));
        }
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

impl<'a> PgModelTurnTransaction<'a> {
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
        let plan_leaf_waiting: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM insight_platform.run_nodes
                WHERE tenant_id = $1 AND node_id = $2 AND run_id = $3
                  AND record_kind = 'node_execution' AND node_kind = 'model_loop'
                  AND state = 'waiting' AND terminal_at IS NULL
            )
            "#,
        )
        .bind(decision.turn.tenant_id.to_string())
        .bind(decision.turn.node_execution_id.to_string())
        .bind(decision.turn.run_id.to_string())
        .fetch_one(&mut *transaction)
        .await?;
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
            if output.response.tool_intents.is_empty() && plan_leaf_waiting {
                let mutations = command.resume_mutations.as_ref().ok_or_else(|| {
                    RepositoryError::InvalidInput(
                        "Model Plan leaf completion has no continuation identities".to_owned(),
                    )
                })?;
                let exact = output
                    .structured_output_exact_for(
                        &decision.turn.run_id,
                        &decision.turn.node_execution_id,
                        &command.request,
                        self.limits,
                    )
                    .map_err(|failure| RepositoryError::InvalidInput(failure.to_string()))?;
                crate::repository::settle_model_leaf_success_in_transaction(
                    &mut transaction,
                    &decision.turn,
                    &command.job_id,
                    &exact,
                    mutations,
                    self.scope_environment_limits,
                    database_now,
                )
                .await?;
            } else if !output.response.tool_intents.is_empty() && plan_leaf_waiting {
                let mutations = command
                    .tool_continuation_mutations
                    .as_ref()
                    .ok_or_else(|| {
                        RepositoryError::InvalidInput(
                            "Model Plan tool response has no continuation identities".to_owned(),
                        )
                    })?;
                crate::repository::handoff_model_tool_continuation_in_transaction(
                    &mut transaction,
                    &decision.turn,
                    &command.job_id,
                    output,
                    mutations,
                    database_now,
                )
                .await?;
            } else if command.resume_mutations.is_some()
                || command.tool_continuation_mutations.is_some()
            {
                return Err(RepositoryError::InvalidInput(
                    "non-Plan Model success has continuation identities".to_owned(),
                ));
            }
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
        if let ModelDispatchOutcome::PermanentFailure { failure, .. } = &command.outcome {
            if plan_leaf_waiting {
                let mutations = command.failure_mutations.as_ref().ok_or_else(|| {
                    RepositoryError::InvalidInput(
                        "Model Plan leaf failure has no convergence identities".to_owned(),
                    )
                })?;
                crate::repository::settle_model_leaf_failure_in_transaction(
                    &mut transaction,
                    &decision.turn,
                    &command.job_id,
                    &failure.failure,
                    mutations,
                    database_now,
                )
                .await?;
            } else if command.failure_mutations.is_some() {
                return Err(RepositoryError::InvalidInput(
                    "non-Plan Model failure has convergence identities".to_owned(),
                ));
            }
        }
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
    let ValueRef::Inline { value } = &output.value else {
        return Err(RepositoryError::InvalidInput(
            "Model response must use Inline storage".to_owned(),
        ));
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
    .bind(value)
    .bind(Option::<String>::None)
    .bind(database_now)
    .execute(&mut **transaction)
    .await?;
    if let (Some(value_id), Some(structured)) = (
        &output.structured_output_value_id,
        &output.response.structured_output,
    ) {
        sqlx::query(
            r#"
            INSERT INTO insight_platform.run_values (
                tenant_id, value_id, run_id, node_id, value_kind, classification,
                schema_digest, content_digest, inline_value, artifact_id, created_at
            ) VALUES ($1, $2, $3, $4, 'model_structured_output', $5, $6, $7, $8, $9, $10)
            "#,
        )
        .bind(turn.tenant_id.to_string())
        .bind(value_id.to_string())
        .bind(turn.run_id.to_string())
        .bind(turn.node_execution_id.to_string())
        .bind(output.classification.as_str())
        .bind(structured.schema_digest.to_string())
        .bind(structured.canonical_digest.to_string())
        .bind(&structured.value)
        .bind(Option::<String>::None)
        .bind(database_now)
        .execute(&mut **transaction)
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
