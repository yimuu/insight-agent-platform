//! PostgreSQL-backed durable Plan-generation store.

use crate::{
    allocate_controller_step_mutations, CoordinatorIdentityFactory, DerivedControllerCommit,
    DurableControllerFacts, DurableControllerPhase, DurablePlanDriverError,
    DurablePlanGenerationStore, GenerationHandoffReason, StartedOrchestrationJob,
};
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use insight_platform_contracts::{canonical_digest, ClosedJsonValue, ResourceKind, ValueRef};
use insight_platform_orchestrator::{CommittedExpressionInput, RuntimeNode, RuntimePlan};
use insight_platform_postgres::repository::{
    ApplyDerivedExpressionControllerStep, ApplyOrchestrationControllerStep, ControllerFacts,
    JobFence, OrchestrationYield, OrchestrationYieldMutationIds, PgRepository, RepositoryError,
    ResolvedExpressionInput, YieldOrchestrationJob, MAX_ORCHESTRATION_QUOTA_LINES,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunValueMaterializationError {
    Unavailable,
    Integrity,
}

#[async_trait]
pub trait ControllerRunValueMaterializer: Send + Sync + 'static {
    async fn materialize(
        &self,
        input: &ResolvedExpressionInput,
    ) -> Result<ClosedJsonValue, RunValueMaterializationError>;
}

/// Useful for installations whose effective Plan only permits Inline expression inputs.
pub struct InlineControllerRunValueMaterializer;

#[async_trait]
impl ControllerRunValueMaterializer for InlineControllerRunValueMaterializer {
    async fn materialize(
        &self,
        input: &ResolvedExpressionInput,
    ) -> Result<ClosedJsonValue, RunValueMaterializationError> {
        let ValueRef::Inline { value } = &input.value else {
            return Err(RunValueMaterializationError::Unavailable);
        };
        let materialized = ClosedJsonValue::build(input.schema_digest.clone(), value.clone())
            .map_err(|_| RunValueMaterializationError::Integrity)?;
        if materialized.canonical_digest != input.content_digest {
            return Err(RunValueMaterializationError::Integrity);
        }
        Ok(materialized)
    }
}

pub struct PostgresDurablePlanGenerationStore<M, I> {
    repository: PgRepository,
    materializer: Arc<M>,
    identities: Arc<I>,
    handoff_retry_delay: Duration,
}

impl<M, I> PostgresDurablePlanGenerationStore<M, I>
where
    M: ControllerRunValueMaterializer,
    I: CoordinatorIdentityFactory + 'static,
{
    pub fn new(
        repository: PgRepository,
        materializer: Arc<M>,
        identities: Arc<I>,
        handoff_retry_delay: Duration,
    ) -> Result<Self, DurablePlanDriverError> {
        if handoff_retry_delay.is_zero() || i64::try_from(handoff_retry_delay.as_millis()).is_err()
        {
            return Err(DurablePlanDriverError::InvariantViolation);
        }
        Ok(Self {
            repository,
            materializer,
            identities,
            handoff_retry_delay,
        })
    }

    fn new_id(
        &self,
        kind: ResourceKind,
    ) -> Result<insight_platform_contracts::ResourceId, DurablePlanDriverError> {
        self.identities
            .new_resource_id(kind)
            .map_err(|_| DurablePlanDriverError::InvariantViolation)
    }
}

#[async_trait]
impl<M, I> DurablePlanGenerationStore for PostgresDurablePlanGenerationStore<M, I>
where
    M: ControllerRunValueMaterializer,
    I: CoordinatorIdentityFactory + 'static,
{
    async fn load_controller_facts(
        &self,
        _job: &StartedOrchestrationJob,
        fence: &JobFence,
        plan: &RuntimePlan,
    ) -> Result<DurableControllerFacts, DurablePlanDriverError> {
        match self
            .repository
            .load_controller_facts(fence, plan)
            .await
            .map_err(classify_repository_failure)?
        {
            ControllerFacts::Expression(facts) => {
                let mut inputs = Vec::with_capacity(facts.inputs.len());
                for input in facts.inputs {
                    let value =
                        self.materializer.materialize(&input).await.map_err(
                            |failure| match failure {
                                RunValueMaterializationError::Unavailable => {
                                    DurablePlanDriverError::Unavailable
                                }
                                RunValueMaterializationError::Integrity => {
                                    DurablePlanDriverError::InvariantViolation
                                }
                            },
                        )?;
                    inputs.push(CommittedExpressionInput {
                        run_value_id: input.run_value_id,
                        port: input.port,
                        classification: input.classification,
                        value,
                    });
                }
                Ok(DurableControllerFacts {
                    node_execution_id: facts.node_execution_id,
                    node_execution_version: facts.node_execution_version,
                    plan_node_key: facts.plan_node_key,
                    phase: DurableControllerPhase::Expression {
                        inputs,
                        loop_iteration: facts.loop_iteration,
                    },
                })
            }
            ControllerFacts::Committed {
                identity,
                observation,
            } => Ok(DurableControllerFacts {
                node_execution_id: identity.node_execution_id,
                node_execution_version: identity.node_execution_version,
                plan_node_key: identity.plan_node_key,
                phase: DurableControllerPhase::CommittedFacts { observation },
            }),
        }
    }

    async fn commit_controller(
        &self,
        job: &StartedOrchestrationJob,
        command: DerivedControllerCommit,
    ) -> Result<(), DurablePlanDriverError> {
        let derived = match (command.facts.phase.clone(), command.evaluation.clone()) {
            (DurableControllerPhase::Expression { inputs, .. }, Some(evaluation)) => {
                Some((inputs, evaluation))
            }
            (DurableControllerPhase::CommittedFacts { .. }, None) => None,
            _ => return Err(DurablePlanDriverError::InvariantViolation),
        };
        let requirements = self
            .repository
            .load_controller_mutation_requirements(
                &command.fence,
                &command.materialized.plan,
                &command.observation,
            )
            .await
            .map_err(classify_repository_failure)?;
        let request_digest = canonical_digest(&json!({
            "evidence_digest": derived.as_ref().map(|(_, evaluation)| &evaluation.evidence.canonical_digest),
            "job_id": command.fence.job_id,
            "lease_generation": command.fence.lease_epoch,
            "operation": "orchestration.controller.derived.commit",
            "plan_digest": command.materialized.request.artifact.content_digest(),
        }))
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?
        .parse()
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let wake_request_digest = canonical_digest(&json!({
            "job_id": command.fence.job_id,
            "operation": "orchestration.controller.wake",
            "request_digest": request_digest,
        }))
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?
        .parse()
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let mutations = allocate_controller_step_mutations(
            self.identities.as_ref(),
            &requirements,
            wake_request_digest,
        )
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let node = command
            .materialized
            .plan
            .node(&command.facts.plan_node_key)
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let output_value_count = match node {
            RuntimeNode::Compute { .. } => derived
                .as_ref()
                .map(|(_, evaluation)| evaluation.outputs.len())
                .unwrap_or(0),
            RuntimeNode::Map { .. } => mutations.activations.len(),
            _ => 0,
        };
        let output_value_ids = (0..output_value_count)
            .map(|_| self.new_id(ResourceKind::RunValue))
            .collect::<Result<Vec<_>, _>>()?;
        let step = ApplyOrchestrationControllerStep {
            fence: command.fence,
            plan: command.materialized.plan,
            observation: command.observation,
            idempotency_key_digest: self
                .identities
                .new_lease_token_digest()
                .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
            request_digest,
            receipt_expires_at: job.started().deadline,
            mutations,
        };
        let mut transaction = self
            .repository
            .begin_scheduler_transaction()
            .await
            .map_err(classify_repository_failure)?;
        let applied = if let Some((inputs, evaluation)) = derived {
            transaction
                .apply_derived_expression_controller_step(ApplyDerivedExpressionControllerStep {
                    step,
                    materialized_inputs: inputs,
                    evaluation,
                    output_value_ids,
                })
                .await
        } else {
            transaction.apply_orchestration_controller_step(step).await
        };
        match applied {
            Ok(_) => transaction
                .commit()
                .await
                .map_err(classify_repository_failure),
            Err(failure) => {
                transaction
                    .rollback()
                    .await
                    .map_err(classify_repository_failure)?;
                Err(classify_repository_failure(failure))
            }
        }
    }

    async fn handoff_controller(
        &self,
        job: &StartedOrchestrationJob,
        fence: JobFence,
        reason: GenerationHandoffReason,
    ) -> Result<(), DurablePlanDriverError> {
        let delay = ChronoDuration::from_std(self.handoff_retry_delay)
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let request_digest = canonical_digest(&json!({
            "job_id": fence.job_id,
            "lease_generation": fence.lease_epoch,
            "operation": "orchestration.controller.handoff",
            "reason": format!("{reason:?}"),
        }))
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?
        .parse()
        .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let mutations = OrchestrationYieldMutationIds {
            receipt_id: self.new_id(ResourceKind::Receipt)?,
            quota_entry_ids: (0..MAX_ORCHESTRATION_QUOTA_LINES)
                .map(|_| self.new_id(ResourceKind::QuotaLedgerEntry))
                .collect::<Result<Vec<_>, _>>()?,
            run_event_id: self.new_id(ResourceKind::Event)?,
            run_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
            node_event_id: self.new_id(ResourceKind::Event)?,
            node_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
            job_event_id: self.new_id(ResourceKind::Event)?,
            job_outbox_id: self.new_id(ResourceKind::OutboxEvent)?,
        };
        let command = YieldOrchestrationJob {
            fence,
            outcome: OrchestrationYield::Retry {
                retry_at: Utc::now() + delay,
            },
            idempotency_key_digest: self
                .identities
                .new_lease_token_digest()
                .map_err(|_| DurablePlanDriverError::InvariantViolation)?,
            request_digest,
            receipt_expires_at: job.started().deadline,
            mutations,
        };
        let mut transaction = self
            .repository
            .begin_scheduler_transaction()
            .await
            .map_err(classify_repository_failure)?;
        match transaction.yield_orchestration_job(command).await {
            Ok(_) => transaction
                .commit()
                .await
                .map_err(classify_repository_failure),
            Err(failure) => {
                transaction
                    .rollback()
                    .await
                    .map_err(classify_repository_failure)?;
                Err(classify_repository_failure(failure))
            }
        }
    }
}

fn classify_repository_failure(failure: RepositoryError) -> DurablePlanDriverError {
    match failure {
        RepositoryError::Database(_) => DurablePlanDriverError::Unavailable,
        RepositoryError::NotFound(_)
        | RepositoryError::Conflict(_)
        | RepositoryError::StaleFence
        | RepositoryError::LeaseExpired => DurablePlanDriverError::FenceLost,
        RepositoryError::InvalidInput(_)
        | RepositoryError::QuotaExceeded
        | RepositoryError::PermissionDenied
        | RepositoryError::IdempotencyConflict
        | RepositoryError::CorruptRow(_) => DurablePlanDriverError::InvariantViolation,
    }
}
