//! Production Plan driver: derive controller commands only from exact Plan bytes and durable facts.

use crate::{
    DurableOrchestrationPlanDriver, DurablePlanDriverError, GenerationHandoffReason,
    MaterializedTypedPlan, StartedOrchestrationJob,
};
use async_trait::async_trait;
use insight_platform_contracts::{
    CandidateSelectionPolicyDocument, ClosedJsonValue, ExactDeploymentRef, ExactPolicyBinding,
    Failure, ResourceId,
};
use insight_platform_orchestrator::{
    decide_controller, derive_expression_controller, CommittedExpressionInput, ControllerDecision,
    ControllerEvaluation, ControllerObservation, DurableWaitKind, ExpressionLimits,
    OrchestrationJobPayload, PlanNodeKey, RuntimeNode, RuntimePlan,
};
use insight_platform_postgres::repository::{JobFence, ResolvedExpressionInput};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum DurableControllerPhase {
    Expression {
        inputs: Vec<CommittedExpressionInput>,
        loop_iteration: u32,
    },
    CommittedFacts {
        observation: ControllerObservation,
    },
    ChildAgentDispatch(Box<DurableChildAgentDispatchFacts>),
    CapabilityDispatch(Box<DurableCapabilityDispatchFacts>),
    ContextDispatch(Box<DurableContextDispatchFacts>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct DurableChildAgentDispatchFacts {
    pub input: ResolvedExpressionInput,
    pub route: Option<(ResolvedExpressionInput, ClosedJsonValue)>,
    pub selection_policy: ExactPolicyBinding,
    pub selection_document: CandidateSelectionPolicyDocument,
    pub candidates: Vec<ExactDeploymentRef>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DurableContextDispatchFacts {
    pub input: ResolvedExpressionInput,
    pub value: ClosedJsonValue,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DurableCapabilityDispatchFacts {
    pub input: ResolvedExpressionInput,
    pub route: Option<(ResolvedExpressionInput, ClosedJsonValue)>,
    pub selection_policy: ExactPolicyBinding,
    pub selection_document: CandidateSelectionPolicyDocument,
    pub candidates: Vec<ExactDeploymentRef>,
}

#[derive(Debug, Clone, PartialEq)]
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

    async fn commit_convergence_failure(
        &self,
        job: &StartedOrchestrationJob,
        fence: JobFence,
        materialized: MaterializedTypedPlan,
        failure: Failure,
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
        let started_payload = &job.started().payload;
        if started_payload.value.get("convergence_failure").is_some() {
            if started_payload.schema_version != 1 {
                return Err(DurablePlanDriverError::InvariantViolation);
            }
            let mut payload_value = started_payload.value.clone();
            let schema_version = payload_value
                .as_object_mut()
                .and_then(|object| object.remove("schema_version"))
                .and_then(|value| value.as_i64());
            if schema_version != Some(i64::from(started_payload.schema_version)) {
                return Err(DurablePlanDriverError::InvariantViolation);
            }
            let job_payload: OrchestrationJobPayload = serde_json::from_value(payload_value)
                .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
            job_payload
                .validate()
                .map_err(|_| DurablePlanDriverError::InvariantViolation)?;
            let failure = job_payload
                .convergence_failure
                .ok_or(DurablePlanDriverError::InvariantViolation)?;
            return self
                .store
                .commit_convergence_failure(job, fence, materialized, failure)
                .await;
        }
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
            DurableControllerPhase::ChildAgentDispatch(_)
                if matches!(node, RuntimeNode::ChildAgentCall { .. }) =>
            {
                (ControllerObservation::None, None)
            }
            DurableControllerPhase::ChildAgentDispatch(_) => {
                return Err(DurablePlanDriverError::InvariantViolation)
            }
            DurableControllerPhase::CapabilityDispatch(_)
                if matches!(node, RuntimeNode::CapabilityCall { .. }) =>
            {
                (ControllerObservation::None, None)
            }
            DurableControllerPhase::CapabilityDispatch(_) => {
                return Err(DurablePlanDriverError::InvariantViolation)
            }
            DurableControllerPhase::ContextDispatch(_)
                if matches!(node, RuntimeNode::ContextQuery { .. }) =>
            {
                (ControllerObservation::None, None)
            }
            DurableControllerPhase::ContextDispatch(_) => {
                return Err(DurablePlanDriverError::InvariantViolation)
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
                    | RuntimeNode::Return { .. }
                    | RuntimeNode::Raise { .. },
                ControllerObservation::None,
            )
            | (
                RuntimeNode::HumanTask { .. },
                ControllerObservation::DurableWait {
                    wait_kind: DurableWaitKind::HumanTask,
                    ..
                },
            )
            | (
                RuntimeNode::TimerWait { .. },
                ControllerObservation::DurableWait {
                    wait_kind: DurableWaitKind::Timer,
                    ..
                },
            )
            | (
                RuntimeNode::SignalWait { .. },
                ControllerObservation::DurableWait {
                    wait_kind: DurableWaitKind::Signal,
                    ..
                },
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
        ArtifactRef, DataClassification, FailureClass, FailureCode, FailureSource,
        PlatformFailureCode, ResourceKind, Retryability, SchedulerPriority, Sha256Digest,
        WorkClass,
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
        started_with_failure(None)
    }

    fn started_with_failure(convergence_failure: Option<Failure>) -> StartedOrchestrationJob {
        let now = Utc::now();
        let node_id = id(ResourceKind::NodeExecution, "9903");
        let payload = OrchestrationJobPayload {
            bindings_digest: digest('c'),
            node_execution_id: node_id.clone(),
            root_scope_id: id(ResourceKind::ScopeInstance, "9907"),
            retry_backoff_milliseconds: 100,
            wake_contract: None,
            convergence_failure,
        };
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
            payload: TypedPayload::new(1, &payload).unwrap(),
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
            plan_version: 4,
            interface_revision_id: id(ResourceKind::AgentInterfaceRevision, "9910"),
            entry_node_id: key("branch"),
            dependency_slots: BTreeMap::new(),
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
                (
                    key("selected"),
                    RuntimeNode::Return {
                        value: ExactDataPortRef::RunInput {
                            schema_digest: digest('1'),
                        },
                    },
                ),
                (
                    key("otherwise"),
                    RuntimeNode::Return {
                        value: ExactDataPortRef::RunInput {
                            schema_digest: digest('1'),
                        },
                    },
                ),
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
        converged: Mutex<Option<Failure>>,
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

        async fn commit_convergence_failure(
            &self,
            _job: &StartedOrchestrationJob,
            _fence: JobFence,
            _materialized: MaterializedTypedPlan,
            failure: Failure,
        ) -> Result<(), DurablePlanDriverError> {
            *self.converged.lock().unwrap() = Some(failure);
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
            converged: Mutex::new(None),
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

    #[tokio::test]
    async fn committed_external_failure_bypasses_leaf_dispatch_and_uses_convergence_owner() {
        let failure = Failure {
            code: FailureCode::Platform {
                code: PlatformFailureCode::ContextQueryFailed,
            },
            class: FailureClass::External,
            retryability: Retryability::Never,
            safe_message: Some("context source rejected the query".to_owned()),
            details_ref: None,
            source: FailureSource::Context,
        };
        let started = started_with_failure(Some(failure.clone()));
        let (materialized, _) = materialized();
        let store = Arc::new(Store {
            facts: DurableControllerFacts {
                node_execution_id: id(ResourceKind::NodeExecution, "9991"),
                node_execution_version: 1,
                plan_node_key: key("never-loaded"),
                phase: DurableControllerPhase::CommittedFacts {
                    observation: ControllerObservation::None,
                },
            },
            committed: Mutex::new(None),
            converged: Mutex::new(None),
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
        assert_eq!(*store.converged.lock().unwrap(), Some(failure));
        assert!(store.committed.lock().unwrap().is_none());
    }

    #[test]
    fn durable_wait_observations_are_accepted_only_for_the_matching_wait_kind() {
        let timer = RuntimeNode::TimerWait {
            delay_milliseconds: 100,
            resume: key("finish"),
        };
        assert!(committed_observation_is_allowed(
            &timer,
            &ControllerObservation::DurableWait {
                wait_kind: DurableWaitKind::Timer,
                outcome: insight_platform_orchestrator::DurableWaitOutcome::Succeeded,
            },
        ));
        assert!(!committed_observation_is_allowed(
            &timer,
            &ControllerObservation::DurableWait {
                wait_kind: DurableWaitKind::Signal,
                outcome: insight_platform_orchestrator::DurableWaitOutcome::Succeeded,
            },
        ));
    }
}
