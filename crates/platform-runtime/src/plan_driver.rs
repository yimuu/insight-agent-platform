//! Production Plan driver: derive controller commands only from exact Plan bytes and durable facts.

use crate::{
    DurableOrchestrationPlanDriver, DurablePlanDriverError, GenerationHandoffReason,
    MaterializedTypedPlan, StartedOrchestrationJob,
};
use async_trait::async_trait;
use insight_platform_contracts::ResourceId;
use insight_platform_orchestrator::{
    decide_controller, derive_expression_controller, CommittedExpressionInput, ControllerDecision,
    ControllerEvaluation, ControllerObservation, ExpressionLimits, PlanNodeKey, RuntimeNode,
    RuntimePlan,
};
use insight_platform_postgres::repository::JobFence;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DurableControllerPhase {
    Expression {
        inputs: Vec<CommittedExpressionInput>,
        loop_iteration: u32,
    },
    CommittedFacts {
        observation: ControllerObservation,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableControllerFacts {
    pub node_execution_id: ResourceId,
    pub node_execution_version: i64,
    pub plan_node_key: PlanNodeKey,
    pub phase: DurableControllerPhase,
}

#[derive(Debug, Clone)]
pub struct DerivedControllerCommit {
    pub fence: JobFence,
    pub materialized: MaterializedTypedPlan,
    pub facts: DurableControllerFacts,
    pub observation: ControllerObservation,
    pub evaluation: Option<ControllerEvaluation>,
    pub decision: ControllerDecision,
}

#[async_trait]
pub trait DurablePlanGenerationStore: Send + Sync + 'static {
    async fn load_controller_facts(
        &self,
        job: &StartedOrchestrationJob,
        fence: &JobFence,
        plan: &RuntimePlan,
    ) -> Result<DurableControllerFacts, DurablePlanDriverError>;

    async fn commit_controller(
        &self,
        job: &StartedOrchestrationJob,
        command: DerivedControllerCommit,
    ) -> Result<(), DurablePlanDriverError>;

    async fn handoff_controller(
        &self,
        job: &StartedOrchestrationJob,
        fence: JobFence,
        reason: GenerationHandoffReason,
    ) -> Result<(), DurablePlanDriverError>;
}

pub struct ExactPlanGenerationDriver<S> {
    store: Arc<S>,
    expression_limits: ExpressionLimits,
}

impl<S> ExactPlanGenerationDriver<S>
where
    S: DurablePlanGenerationStore,
{
    pub fn new(
        store: Arc<S>,
        expression_limits: ExpressionLimits,
    ) -> Result<Self, DurablePlanDriverError> {
        if !expression_limits.bounded_by_absolute() {
            return Err(DurablePlanDriverError::InvariantViolation);
        }
        Ok(Self {
            store,
            expression_limits,
        })
    }
}

#[async_trait]
impl<S> DurableOrchestrationPlanDriver for ExactPlanGenerationDriver<S>
where
    S: DurablePlanGenerationStore,
{
    async fn commit_generation(
        &self,
        job: &StartedOrchestrationJob,
        fence: JobFence,
        materialized: MaterializedTypedPlan,
    ) -> Result<(), DurablePlanDriverError> {
        let facts = self
            .store
            .load_controller_facts(job, &fence, &materialized.plan)
            .await?;
        let started = job.started();
        if started.owner_id != facts.node_execution_id.to_string()
            || started.node_id.as_deref() != Some(started.owner_id.as_str())
            || facts.node_execution_version <= 0
        {
            return Err(DurablePlanDriverError::InvariantViolation);
        }
        let node = materialized
            .plan
            .node(&facts.plan_node_key)
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        let (observation, evaluation) = match facts.phase.clone() {
            DurableControllerPhase::Expression {
                inputs,
                loop_iteration,
            } => {
                if !matches!(
                    node,
                    RuntimeNode::Compute { .. }
                        | RuntimeNode::Branch { .. }
                        | RuntimeNode::Map { .. }
                        | RuntimeNode::Loop { .. }
                ) {
                    return Err(DurablePlanDriverError::InvariantViolation);
                }
                let evaluation = derive_expression_controller(
                    node,
                    inputs,
                    facts.node_execution_id.clone(),
                    facts.node_execution_version,
                    loop_iteration,
                    self.expression_limits,
                )
                .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
                (evaluation.observation.clone(), Some(evaluation))
            }
            DurableControllerPhase::CommittedFacts { observation } => {
                if !committed_observation_is_allowed(node, &observation) {
                    return Err(DurablePlanDriverError::InvariantViolation);
                }
                (observation, None)
            }
        };
        let decision = decide_controller(node, &observation)
            .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
        self.store
            .commit_controller(
                job,
                DerivedControllerCommit {
                    fence,
                    materialized,
                    facts,
                    observation,
                    evaluation,
                    decision,
                },
            )
            .await
    }

    async fn handoff_generation(
        &self,
        job: &StartedOrchestrationJob,
        fence: JobFence,
        reason: GenerationHandoffReason,
    ) -> Result<(), DurablePlanDriverError> {
        self.store.handoff_controller(job, fence, reason).await
    }
}

fn committed_observation_is_allowed(
    node: &RuntimeNode,
    observation: &ControllerObservation,
) -> bool {
    matches!(
        (node, observation),
        (
            RuntimeNode::Map { .. },
            ControllerObservation::MapSettlement { .. }
        ) | (RuntimeNode::Join { .. }, ControllerObservation::Join { .. })
            | (
                RuntimeNode::ErrorBoundary { .. },
                ControllerObservation::ErrorBoundary { .. }
            )
            | (
                RuntimeNode::Start { .. }
                    | RuntimeNode::Fork { .. }
                    | RuntimeNode::ModelLoop { .. }
                    | RuntimeNode::CapabilityCall { .. }
                    | RuntimeNode::ContextQuery { .. }
                    | RuntimeNode::ChildAgentCall { .. }
                    | RuntimeNode::HumanTask { .. }
                    | RuntimeNode::TimerWait { .. }
                    | RuntimeNode::SignalWait { .. }
                    | RuntimeNode::Return
                    | RuntimeNode::Raise,
                ControllerObservation::None,
            )
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MaterializedTypedPlan;
    use async_trait::async_trait;
    use chrono::{Duration, Utc};
    use insight_platform_artifacts::SchedulerTypedPlanReadRequest;
    use insight_platform_contracts::{
        ArtifactRef, DataClassification, ResourceKind, SchedulerPriority, Sha256Digest, WorkClass,
    };
    use insight_platform_orchestrator::{
        BranchArm, DataPortKey, ExactDataPortRef, TypedExpressionProgram, TypedInstruction,
    };
    use insight_platform_postgres::repository::{ClaimedOrchestrationJob, JobRecord, TypedPayload};
    use serde_json::json;
    use std::{collections::BTreeMap, str::FromStr, sync::Mutex};

    fn id(kind: ResourceKind, suffix: &str) -> ResourceId {
        format!(
            "{}_0198f1c5-0787-75e1-a9e8-d95ca0f3{}",
            kind.descriptor().prefix,
            suffix
        )
        .parse()
        .unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        Sha256Digest::from_str(&format!("sha256:{}", character.to_string().repeat(64))).unwrap()
    }

    fn key(value: &str) -> PlanNodeKey {
        PlanNodeKey::new(value.to_owned()).unwrap()
    }

    fn started() -> StartedOrchestrationJob {
        let now = Utc::now();
        let node_id = id(ResourceKind::NodeExecution, "9903");
        let job = JobRecord {
            tenant_id: id(ResourceKind::Tenant, "9901").to_string(),
            job_id: id(ResourceKind::Job, "9902").to_string(),
            work_class: WorkClass::Orchestration.as_str().to_owned(),
            owner_kind: ResourceKind::NodeExecution.descriptor().name.to_owned(),
            owner_id: node_id.to_string(),
            invocation_id: None,
            run_id: Some(id(ResourceKind::Run, "9904").to_string()),
            node_id: Some(node_id.to_string()),
            state: "running".to_owned(),
            version: 4,
            attempt_no: 1,
            attempt_limit: 8,
            lease_epoch: 2,
            worker_id: Some(id(ResourceKind::WorkerProcessGeneration, "9905").to_string()),
            lease_token_digest: Some(digest('a').to_string()),
            lease_expires_at: Some(now + Duration::seconds(30)),
            heartbeat_at: Some(now),
            scheduled_at: now,
            retry_at: None,
            deadline: now + Duration::minutes(5),
            priority: SchedulerPriority::Normal,
            wake_kind: None,
            wake_state: None,
            wake_generation: 0,
            request_digest: digest('b').to_string(),
            result_digest: None,
            effect_key_digest: None,
            quota_reservation_id: Some(id(ResourceKind::UsageReservation, "9906").to_string()),
            payload: TypedPayload::new(1, &json!({"kind":"controller"})).unwrap(),
            started_at: Some(now),
            terminal_at: None,
            created_at: now,
            updated_at: now,
        };
        let mut claimed = job.clone();
        claimed.state = "leased".to_owned();
        claimed.version = 3;
        claimed.started_at = None;
        StartedOrchestrationJob::from_parts(
            ClaimedOrchestrationJob {
                job: claimed,
                run_version: 2,
                node_version: 2,
                quota_reservation_id: id(ResourceKind::UsageReservation, "9906").to_string(),
                quota_account_ids: vec![],
            },
            job,
        )
    }

    fn materialized() -> (MaterializedTypedPlan, ExactDataPortRef) {
        let port = ExactDataPortRef::NodeOutput {
            producer_node_id: key("source"),
            port_id: DataPortKey::new("predicate".to_owned()).unwrap(),
            schema_digest: digest('1'),
        };
        let expression = TypedExpressionProgram::build(
            vec![port.clone()],
            vec![TypedInstruction::LoadPort { port: port.clone() }],
            digest('1'),
            ExpressionLimits::ABSOLUTE,
        )
        .unwrap();
        let plan = RuntimePlan {
            plan_version: 2,
            interface_revision_id: id(ResourceKind::AgentInterfaceRevision, "9910"),
            entry_node_id: key("branch"),
            nodes: BTreeMap::from([
                (
                    key("branch"),
                    RuntimeNode::Branch {
                        ordered_arms: vec![BranchArm {
                            when: expression,
                            target: key("selected"),
                        }],
                        otherwise: key("otherwise"),
                    },
                ),
                (key("selected"), RuntimeNode::Return),
                (key("otherwise"), RuntimeNode::Return),
            ]),
        };
        let artifact = ArtifactRef::new(
            id(ResourceKind::Artifact, "9911"),
            plan.canonical_digest(
                insight_platform_orchestrator::PlanLimits::from_profile(
                    &insight_platform_contracts::checked_in_hard_limit_profile(),
                )
                .unwrap(),
            )
            .unwrap(),
            100,
            "application/json".to_owned(),
            DataClassification::Internal,
            None,
        )
        .unwrap();
        (
            MaterializedTypedPlan {
                request: SchedulerTypedPlanReadRequest {
                    tenant_id: id(ResourceKind::Tenant, "9901"),
                    run_id: id(ResourceKind::Run, "9904"),
                    orchestration_job_id: id(ResourceKind::Job, "9902"),
                    worker_process_generation_id: id(ResourceKind::WorkerProcessGeneration, "9905"),
                    lease_generation: 2,
                    lease_token_digest: digest('a'),
                    plan_revision_id: id(ResourceKind::AgentPlanRevision, "9912"),
                    artifact,
                    maximum_bytes: 1024,
                    deadline: Utc::now() + Duration::seconds(20),
                    request_digest: digest('f'),
                },
                plan,
            },
            port,
        )
    }

    struct Store {
        facts: DurableControllerFacts,
        committed: Mutex<Option<DerivedControllerCommit>>,
    }

    #[async_trait]
    impl DurablePlanGenerationStore for Store {
        async fn load_controller_facts(
            &self,
            _job: &StartedOrchestrationJob,
            _fence: &JobFence,
            _plan: &RuntimePlan,
        ) -> Result<DurableControllerFacts, DurablePlanDriverError> {
            Ok(self.facts.clone())
        }

        async fn commit_controller(
            &self,
            _job: &StartedOrchestrationJob,
            command: DerivedControllerCommit,
        ) -> Result<(), DurablePlanDriverError> {
            *self.committed.lock().unwrap() = Some(command);
            Ok(())
        }

        async fn handoff_controller(
            &self,
            _job: &StartedOrchestrationJob,
            _fence: JobFence,
            _reason: GenerationHandoffReason,
        ) -> Result<(), DurablePlanDriverError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn driver_derives_branch_from_exact_value_and_never_accepts_an_observation() {
        let started = started();
        let (materialized, port) = materialized();
        let store = Arc::new(Store {
            facts: DurableControllerFacts {
                node_execution_id: started.started().owner_id.parse().unwrap(),
                node_execution_version: 3,
                plan_node_key: key("branch"),
                phase: DurableControllerPhase::Expression {
                    inputs: vec![CommittedExpressionInput {
                        run_value_id: id(ResourceKind::RunValue, "9914"),
                        port,
                        classification: DataClassification::Restricted,
                        value: insight_platform_contracts::ClosedJsonValue::build(
                            digest('1'),
                            json!(true),
                        )
                        .unwrap(),
                    }],
                    loop_iteration: 0,
                },
            },
            committed: Mutex::new(None),
        });
        let driver =
            ExactPlanGenerationDriver::new(store.clone(), ExpressionLimits::ABSOLUTE).unwrap();
        let fence = JobFence {
            tenant_id: started.started().tenant_id.clone(),
            job_id: started.started().job_id.clone(),
            worker_id: started
                .started()
                .worker_id
                .as_deref()
                .unwrap()
                .parse()
                .unwrap(),
            lease_epoch: 2,
            expected_job_version: 4,
            lease_token_digest: digest('a'),
        };
        driver
            .commit_generation(&started, fence, materialized)
            .await
            .unwrap();
        let committed = store.committed.lock().unwrap();
        let command = committed.as_ref().unwrap();
        assert_eq!(
            command.observation,
            ControllerObservation::Branch {
                selected: key("selected")
            }
        );
        assert!(command.evaluation.as_ref().unwrap().validate().is_ok());
    }
}
